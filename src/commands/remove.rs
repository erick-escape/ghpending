use anyhow::Result;
use inquire::MultiSelect;

use crate::config;

const GITLAB_SUFFIX: &str = "  (gitlab)";

pub fn run() -> Result<()> {
    let mut cfg = config::load()?;
    let gitlab_repos = cfg
        .gitlab
        .as_ref()
        .map(|gl| gl.repos.clone())
        .unwrap_or_default();
    if cfg.repos.is_empty() && gitlab_repos.is_empty() {
        println!("No repos tracked.");
        return Ok(());
    }

    // GitLab entries carry a suffix so a project path that happens to match
    // a GitHub `owner/name` stays distinguishable in the picker.
    let mut choices = cfg.repos.clone();
    choices.extend(gitlab_repos.iter().map(|r| format!("{r}{GITLAB_SUFFIX}")));

    let to_remove = MultiSelect::new("Select repos to remove:", choices).prompt()?;

    let remove_set: std::collections::HashSet<&str> =
        to_remove.iter().map(std::string::String::as_str).collect();
    cfg.repos.retain(|r| !remove_set.contains(r.as_str()));
    if let Some(gl) = cfg.gitlab.as_mut() {
        gl.repos
            .retain(|r| !remove_set.contains(format!("{r}{GITLAB_SUFFIX}").as_str()));
    }
    config::save(&cfg)?;

    if to_remove.is_empty() {
        println!("Nothing removed.");
    } else {
        println!("Removed {} repo(s).", to_remove.len());
    }
    Ok(())
}
