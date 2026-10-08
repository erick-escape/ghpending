use crate::config;
use anyhow::Result;

pub fn run() -> Result<()> {
    let cfg = config::load()?;
    let gitlab_repos = cfg
        .gitlab
        .as_ref()
        .map_or(&[][..], |gl| gl.repos.as_slice());
    if cfg.repos.is_empty() && gitlab_repos.is_empty() {
        println!("No repos tracked. Run `ghpending add` to get started.");
    } else {
        for repo in &cfg.repos {
            println!("{repo}");
        }
        for repo in gitlab_repos {
            println!("{repo}  (gitlab)");
        }
    }
    Ok(())
}
