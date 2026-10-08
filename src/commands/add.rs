use std::{future::Future, time::Duration};

use anyhow::{Context, Result, bail};
use inquire::{MultiSelect, Text};
use octocrab::Octocrab;
use tokio::time::timeout;

use crate::github::ListSource;
use crate::{config, github, gitlab};

const API_TIMEOUT: Duration = Duration::from_secs(30);

pub async fn run(crab: &Octocrab, user: Option<String>, all: bool) -> Result<()> {
    let mut cfg = config::load()?;

    let found = if all {
        with_api_timeout(
            github::list_authenticated_repos(crab),
            "listing repositories timed out after 30s",
        )
        .await?
    } else {
        let username = match resolve_user(user, cfg.user.clone()) {
            UserChoice::Override(u) => {
                cfg.user = Some(u.clone());
                config::save(&cfg)?;
                u
            }
            UserChoice::Saved(u) => u,
            UserChoice::Prompt => {
                let u = Text::new("GitHub username or org to list repos from:")
                    .prompt()?
                    .trim()
                    .to_owned();
                cfg.user = Some(u.clone());
                config::save(&cfg)?;
                u
            }
            UserChoice::Blank => bail!("--user cannot be empty"),
        };

        match with_api_timeout(
            github::resolve_source_for(crab, &username),
            "resolving repository source timed out after 30s",
        )
        .await?
        {
            ListSource::Authenticated => {
                with_api_timeout(
                    github::list_authenticated_repos(crab),
                    "listing repositories timed out after 30s",
                )
                .await?
            }
            ListSource::Org(org) => {
                with_api_timeout(
                    github::list_org_repos(crab, &org),
                    "listing repositories timed out after 30s",
                )
                .await?
            }
            ListSource::PublicUser(u) => {
                with_api_timeout(
                    github::list_user_repos(crab, &u),
                    "listing repositories timed out after 30s",
                )
                .await?
            }
        }
    };

    if found.is_empty() {
        if all {
            println!("No repos found for your account.");
        } else {
            println!("No repos found.");
        }
        return Ok(());
    }

    let already: std::collections::HashSet<&str> =
        cfg.repos.iter().map(std::string::String::as_str).collect();

    let defaults: Vec<usize> = found
        .iter()
        .enumerate()
        .filter_map(|(i, r)| {
            if already.contains(r.as_str()) {
                Some(i)
            } else {
                None
            }
        })
        .collect();

    let selected = MultiSelect::new("Select repos to track:", found)
        .with_default(&defaults)
        .prompt()?;

    for repo in selected {
        if !cfg.repos.contains(&repo) {
            cfg.repos.push(repo);
        }
    }
    cfg.repos.sort();
    config::save(&cfg)?;
    println!("Saved. Tracking {} repo(s) total.", cfg.repos.len());
    Ok(())
}

/// `add --gitlab`: pick projects from a GitLab user/group (or every project
/// you're a member of) and store them under `[gitlab].repos`. Without
/// `--user`, falls back to the saved GitLab user, then to membership.
pub async fn run_gitlab(user: Option<String>, all: bool, url: Option<String>) -> Result<()> {
    let mut cfg = config::load()?;
    let mut gl = cfg.gitlab.clone().unwrap_or_default();

    if let Some(url) = url {
        let url = url.trim().trim_end_matches('/').to_owned();
        if url.is_empty() {
            bail!("--url cannot be empty");
        }
        gl.url = url;
    }
    if gl.url.is_empty() {
        let url = Text::new("GitLab URL (e.g. https://gitlab.example.com):").prompt()?;
        gl.url = url.trim().trim_end_matches('/').to_owned();
        if gl.url.is_empty() {
            bail!("a GitLab URL is required");
        }
    }

    let client = gitlab::GitlabClient::new(&gl.url)?;

    let namespace = if all {
        None
    } else {
        match resolve_user(user, gl.user.clone()) {
            UserChoice::Override(u) => {
                gl.user = Some(u.clone());
                Some(u)
            }
            UserChoice::Saved(u) => Some(u),
            UserChoice::Prompt => None,
            UserChoice::Blank => bail!("--user cannot be empty"),
        }
    };

    let found = match &namespace {
        Some(ns) => {
            with_api_timeout(
                gitlab::list_namespace_projects(&client, ns),
                "listing GitLab projects timed out after 30s",
            )
            .await?
        }
        None => {
            with_api_timeout(
                gitlab::list_member_projects(&client),
                "listing GitLab projects timed out after 30s",
            )
            .await?
        }
    };

    if found.is_empty() {
        println!("No GitLab projects found.");
        cfg.gitlab = Some(gl);
        config::save(&cfg)?;
        return Ok(());
    }

    let defaults: Vec<usize> = found
        .iter()
        .enumerate()
        .filter_map(|(i, r)| gl.repos.contains(r).then_some(i))
        .collect();

    let selected = MultiSelect::new("Select GitLab projects to track:", found)
        .with_default(&defaults)
        .prompt()?;

    for repo in selected {
        if !gl.repos.contains(&repo) {
            gl.repos.push(repo);
        }
    }
    gl.repos.sort();
    let count = gl.repos.len();
    cfg.gitlab = Some(gl);
    config::save(&cfg)?;
    println!("Saved. Tracking {count} GitLab project(s).");
    Ok(())
}

async fn with_api_timeout<T>(
    future: impl Future<Output = Result<T>>,
    message: &'static str,
) -> Result<T> {
    timeout(API_TIMEOUT, future).await.context(message)?
}

/// Which GitHub user/org `add` should list repos from, decided from the
/// optional `--user` flag and whatever is already saved in config.
#[derive(Debug, PartialEq)]
enum UserChoice {
    /// `--user` was given: use it and persist it as the new saved default.
    Override(String),
    /// No flag, but config already holds a user: reuse it untouched.
    Saved(String),
    /// Neither flag nor saved user: prompt for one interactively.
    Prompt,
    /// `--user` was given but blank once trimmed.
    Blank,
}

fn resolve_user(flag: Option<String>, saved: Option<String>) -> UserChoice {
    match flag {
        Some(u) => {
            let u = u.trim();
            if u.is_empty() {
                UserChoice::Blank
            } else {
                UserChoice::Override(u.to_owned())
            }
        }
        None => match saved {
            Some(u) => UserChoice::Saved(u),
            None => UserChoice::Prompt,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flag_overrides_saved_user() {
        let choice = resolve_user(Some("octocat".into()), Some("akitaonrails".into()));
        assert_eq!(choice, UserChoice::Override("octocat".into()));
    }

    #[test]
    fn flag_is_trimmed() {
        let choice = resolve_user(Some("  octocat  ".into()), None);
        assert_eq!(choice, UserChoice::Override("octocat".into()));
    }

    #[test]
    fn blank_flag_is_rejected_over_saved_user() {
        let choice = resolve_user(Some("   ".into()), Some("akitaonrails".into()));
        assert_eq!(choice, UserChoice::Blank);
    }

    #[test]
    fn falls_back_to_saved_user_without_flag() {
        let choice = resolve_user(None, Some("akitaonrails".into()));
        assert_eq!(choice, UserChoice::Saved("akitaonrails".into()));
    }

    #[test]
    fn prompts_when_nothing_supplied_or_saved() {
        assert_eq!(resolve_user(None, None), UserChoice::Prompt);
    }
}
