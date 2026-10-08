//! GitLab (gitlab.com or self-hosted) support: open merge requests and
//! issues per tracked project via the REST v4 API, mapped onto the same
//! `RepoResult`/`RepoItem` model the GitHub path produces.

use std::sync::OnceLock;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use futures::stream::{self, StreamExt};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use tokio::time::timeout;

use crate::github::{Forge, ItemKind, RepoError, RepoItem, RepoResult, RepoStatus, item_cmp};

const FETCH_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_CONCURRENT_FETCHES: usize = 4;
/// Safety cap on pagination: 50 pages x 100 items per endpoint.
const MAX_PAGES: u32 = 50;

pub struct GitlabClient {
    http: reqwest::Client,
    /// `https://host/api/v4`, no trailing slash.
    api: String,
    token: Option<String>,
}

#[derive(Debug)]
enum GitlabError {
    NotFound,
    Api(String),
}

impl GitlabClient {
    pub fn new(url: &str) -> Result<Self> {
        let base = url.trim().trim_end_matches('/');
        if base.is_empty() {
            bail!("GitLab URL is empty");
        }
        let base = if base.starts_with("http://") || base.starts_with("https://") {
            base.to_owned()
        } else {
            format!("https://{base}")
        };
        let http = reqwest::Client::builder()
            .user_agent(concat!("ghpending/", env!("CARGO_PKG_VERSION")))
            .timeout(FETCH_TIMEOUT)
            .build()
            .context("building GitLab HTTP client")?;
        let token = gitlab_token(&host_of(&base));
        Ok(GitlabClient {
            http,
            api: format!("{base}/api/v4"),
            token,
        })
    }

    pub fn has_token(&self) -> bool {
        self.token.is_some()
    }

    async fn get_page<T: DeserializeOwned>(
        &self,
        path: &str,
        page: u32,
    ) -> std::result::Result<(Vec<T>, Option<u32>), GitlabError> {
        let sep = if path.contains('?') { '&' } else { '?' };
        let url = format!("{}{path}{sep}per_page=100&page={page}", self.api);
        let response = self.send(&url).await?;
        let next = response
            .headers()
            .get("x-next-page")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.trim().parse::<u32>().ok());
        let items = response
            .json::<Vec<T>>()
            .await
            .map_err(|e| GitlabError::Api(format!("decoding response: {e}")))?;
        Ok((items, next))
    }

    async fn get_all<T: DeserializeOwned>(
        &self,
        path: &str,
    ) -> std::result::Result<Vec<T>, GitlabError> {
        let mut all = Vec::new();
        let mut page = 1;
        for _ in 0..MAX_PAGES {
            let (mut items, next) = self.get_page::<T>(path, page).await?;
            all.append(&mut items);
            match next {
                Some(n) if n > page => page = n,
                _ => break,
            }
        }
        Ok(all)
    }

    async fn get_one<T: DeserializeOwned>(
        &self,
        path: &str,
    ) -> std::result::Result<T, GitlabError> {
        let url = format!("{}{path}", self.api);
        self.send(&url)
            .await?
            .json::<T>()
            .await
            .map_err(|e| GitlabError::Api(format!("decoding response: {e}")))
    }

    async fn send(&self, url: &str) -> std::result::Result<reqwest::Response, GitlabError> {
        let mut request = self.http.get(url);
        if let Some(token) = &self.token {
            request = request.header("PRIVATE-TOKEN", token);
        }
        let response = request
            .send()
            .await
            .map_err(|e| GitlabError::Api(e.to_string()))?;
        let status = response.status();
        if status.as_u16() == 404 {
            return Err(GitlabError::NotFound);
        }
        if !status.is_success() {
            let mut message = format!("HTTP {status}");
            if status.as_u16() == 401 {
                message.push_str(" — set GITLAB_TOKEN or run `glab auth login`");
            }
            return Err(GitlabError::Api(message));
        }
        Ok(response)
    }
}

/// `https://gitlab.example.com:8443/x` -> `gitlab.example.com:8443`.
fn host_of(base: &str) -> String {
    let rest = base.split_once("://").map_or(base, |(_, rest)| rest);
    rest.split('/').next().unwrap_or(rest).to_owned()
}

/// Token resolution: `$GHPENDING_GITLAB_TOKEN`, then `$GITLAB_TOKEN`, then
/// `glab config get token --host <host>` (silent on failure), then none.
/// Cached for the process, like the GitHub token.
fn gitlab_token(host: &str) -> Option<String> {
    static TOKEN: OnceLock<Option<String>> = OnceLock::new();
    TOKEN
        .get_or_init(|| {
            ["GHPENDING_GITLAB_TOKEN", "GITLAB_TOKEN"]
                .iter()
                .find_map(|var| std::env::var(var).ok().filter(|v| !v.trim().is_empty()))
                .or_else(|| glab_token(host))
                .map(|t| t.trim().to_owned())
        })
        .clone()
}

fn glab_token(host: &str) -> Option<String> {
    let output = std::process::Command::new("glab")
        .args(["config", "get", "token", "--host", host])
        .stderr(std::process::Stdio::null())
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let token = String::from_utf8(output.stdout).ok()?.trim().to_owned();
    if token.is_empty() { None } else { Some(token) }
}

/// URL-encodes a project/namespace path for use as an `:id` segment.
/// GitLab paths are restricted to `[A-Za-z0-9_.-/]`, so only `/` needs it.
fn encode_path(path: &str) -> String {
    path.trim_matches('/').replace('/', "%2F")
}

#[derive(Debug, Deserialize)]
struct GlUser {
    username: String,
}

#[derive(Debug, Deserialize)]
struct GlMergeRequest {
    iid: u64,
    title: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    author: Option<GlUser>,
    #[serde(default)]
    draft: bool,
    /// Pre-14.0 instances only report the old name.
    #[serde(default)]
    work_in_progress: bool,
    user_notes_count: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct GlIssue {
    iid: u64,
    title: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    author: Option<GlUser>,
    user_notes_count: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct GlProject {
    path_with_namespace: String,
}

#[derive(Debug, Deserialize)]
struct GlNamespace {
    kind: String,
    full_path: String,
}

fn author_name(author: Option<GlUser>) -> String {
    author.map_or_else(|| "ghost".to_owned(), |u| u.username)
}

fn merge_request_item(mr: GlMergeRequest) -> RepoItem {
    RepoItem {
        kind: ItemKind::PullRequest,
        number: mr.iid,
        title: mr.title,
        created_at: mr.created_at,
        updated_at: mr.updated_at,
        author: author_name(mr.author),
        pr_draft: Some(mr.draft || mr.work_in_progress),
        comments: mr.user_notes_count,
        review_decision: None,
        mine: false,
        severity: None,
        source: None,
    }
}

fn issue_item(issue: GlIssue) -> RepoItem {
    RepoItem {
        kind: ItemKind::Issue,
        number: issue.iid,
        title: issue.title,
        created_at: issue.created_at,
        updated_at: issue.updated_at,
        author: author_name(issue.author),
        pr_draft: None,
        comments: issue.user_notes_count,
        review_decision: None,
        mine: false,
        severity: None,
        source: None,
    }
}

async fn fetch_project_items(
    client: &GitlabClient,
    project: &str,
) -> std::result::Result<Vec<RepoItem>, GitlabError> {
    let id = encode_path(project);
    let mrs_path = format!("/projects/{id}/merge_requests?state=opened");
    let issues_path = format!("/projects/{id}/issues?state=opened");
    let (mrs, issues) = futures::future::join(
        client.get_all::<GlMergeRequest>(&mrs_path),
        client.get_all::<GlIssue>(&issues_path),
    )
    .await;

    let mut items: Vec<RepoItem> = mrs?.into_iter().map(merge_request_item).collect();
    // A project with issues disabled answers 403/404 on the issues endpoint
    // while MRs still work; keep the MRs rather than failing the project.
    match issues {
        Ok(issues) => items.extend(issues.into_iter().map(issue_item)),
        Err(GitlabError::NotFound) => {}
        Err(GitlabError::Api(m)) if m.starts_with("HTTP 403") => {}
        Err(e) => return Err(e),
    }
    items.sort_by(item_cmp);
    Ok(items)
}

async fn fetch_project(client: &GitlabClient, project: String) -> RepoResult {
    let status = match fetch_project_items(client, &project).await {
        Ok(items) => RepoStatus::Items(items),
        Err(GitlabError::NotFound) => RepoStatus::NotFound,
        Err(GitlabError::Api(message)) => RepoStatus::Error(RepoError::Api(message)),
    };
    RepoResult::new(project, status).with_forge(Forge::GitLab)
}

/// Fetches every tracked project with bounded concurrency inside a single
/// 30s window; projects that haven't finished by then render as timeouts,
/// matching the GitHub REST path.
pub async fn fetch_projects(client: &GitlabClient, projects: &[String]) -> Vec<RepoResult> {
    let mut results: Vec<Option<RepoResult>> = vec![None; projects.len()];
    {
        let mut fetches = stream::iter(projects.iter().cloned().enumerate())
            .map(|(index, project)| async move { (index, fetch_project(client, project).await) })
            .buffer_unordered(MAX_CONCURRENT_FETCHES);
        let _ = timeout(FETCH_TIMEOUT, async {
            while let Some((index, result)) = fetches.next().await {
                results[index] = Some(result);
            }
        })
        .await;
    }
    results
        .into_iter()
        .zip(projects)
        .map(|(result, project)| {
            result.unwrap_or_else(|| {
                RepoResult::new(project.clone(), RepoStatus::Error(RepoError::Timeout))
                    .with_forge(Forge::GitLab)
            })
        })
        .collect()
}

/// The username the token authenticates as, or `None` without a token or
/// on any failure (the digest falls back to the configured user).
pub async fn authenticated_username(client: &GitlabClient) -> Option<String> {
    if !client.has_token() {
        return None;
    }
    client
        .get_one::<GlUser>("/user")
        .await
        .ok()
        .map(|u| u.username)
}

/// Every non-archived project the token is a member of.
pub async fn list_member_projects(client: &GitlabClient) -> Result<Vec<String>> {
    if !client.has_token() {
        bail!(
            "listing your GitLab projects needs a token: set GITLAB_TOKEN or run `glab auth login`"
        );
    }
    let projects = client
        .get_all::<GlProject>(
            "/projects?membership=true&archived=false&simple=true&order_by=path&sort=asc",
        )
        .await
        .map_err(describe)?;
    Ok(sorted_paths(projects))
}

/// Projects under a user or group namespace (subgroups included for groups).
pub async fn list_namespace_projects(
    client: &GitlabClient,
    namespace: &str,
) -> Result<Vec<String>> {
    let ns: GlNamespace = client
        .get_one(&format!("/namespaces/{}", encode_path(namespace)))
        .await
        .map_err(|e| match e {
            GitlabError::NotFound => anyhow::anyhow!("GitLab user or group not found: {namespace}"),
            other => describe(other),
        })?;
    let id = encode_path(&ns.full_path);
    let path = if ns.kind == "group" {
        format!("/groups/{id}/projects?include_subgroups=true&archived=false&simple=true")
    } else {
        format!("/users/{id}/projects?archived=false&simple=true")
    };
    let projects = client.get_all::<GlProject>(&path).await.map_err(describe)?;
    Ok(sorted_paths(projects))
}

fn sorted_paths(projects: Vec<GlProject>) -> Vec<String> {
    let mut paths: Vec<String> = projects
        .into_iter()
        .map(|p| p.path_with_namespace)
        .collect();
    paths.sort_by_key(|p| p.to_ascii_lowercase());
    paths.dedup();
    paths
}

fn describe(e: GitlabError) -> anyhow::Error {
    match e {
        GitlabError::NotFound => anyhow::anyhow!("GitLab returned 404"),
        GitlabError::Api(m) => anyhow::anyhow!("GitLab API error: {m}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_path_escapes_slashes_only() {
        assert_eq!(
            encode_path("group/sub/my-project.v2"),
            "group%2Fsub%2Fmy-project.v2"
        );
        assert_eq!(encode_path("/group/proj/"), "group%2Fproj");
    }

    #[test]
    fn host_of_strips_scheme_and_path() {
        assert_eq!(host_of("https://git.example.com"), "git.example.com");
        assert_eq!(
            host_of("https://git.example.com:8443/gitlab"),
            "git.example.com:8443"
        );
        assert_eq!(host_of("git.example.com"), "git.example.com");
    }

    #[test]
    fn client_normalizes_url() {
        let client = GitlabClient::new("git.example.com/").unwrap();
        assert_eq!(client.api, "https://git.example.com/api/v4");
        assert!(GitlabClient::new("  ").is_err());
    }

    #[test]
    fn merge_request_json_maps_to_pull_request_item() {
        let json = r#"[{
            "iid": 12, "title": "Add login", "state": "opened",
            "created_at": "2026-10-01T12:00:00.000Z",
            "updated_at": "2026-10-02T08:30:00.000Z",
            "author": {"username": "alice", "name": "Alice"},
            "draft": false, "work_in_progress": true,
            "user_notes_count": 3
        }]"#;
        let mrs: Vec<GlMergeRequest> = serde_json::from_str(json).unwrap();
        let item = merge_request_item(mrs.into_iter().next().unwrap());
        assert_eq!(item.kind, ItemKind::PullRequest);
        assert_eq!(item.number, 12);
        assert_eq!(item.title, "Add login");
        assert_eq!(item.author, "alice");
        assert_eq!(item.pr_draft, Some(true));
        assert_eq!(item.comments, Some(3));
    }

    #[test]
    fn issue_json_maps_to_issue_item_and_tolerates_missing_author() {
        let json = r#"[{
            "iid": 7, "title": "Crash on start",
            "created_at": "2026-09-01T00:00:00Z",
            "updated_at": "2026-09-03T00:00:00Z",
            "author": null
        }]"#;
        let issues: Vec<GlIssue> = serde_json::from_str(json).unwrap();
        let item = issue_item(issues.into_iter().next().unwrap());
        assert_eq!(item.kind, ItemKind::Issue);
        assert_eq!(item.number, 7);
        assert_eq!(item.author, "ghost");
        assert_eq!(item.pr_draft, None);
        assert_eq!(item.comments, None);
    }

    #[test]
    fn sorted_paths_sorts_case_insensitively_and_dedups() {
        let projects = vec![
            GlProject {
                path_with_namespace: "b/x".into(),
            },
            GlProject {
                path_with_namespace: "A/y".into(),
            },
            GlProject {
                path_with_namespace: "b/x".into(),
            },
        ];
        assert_eq!(sorted_paths(projects), vec!["A/y", "b/x"]);
    }
}
