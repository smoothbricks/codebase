//! Empirical git-identity probe for workspace mount paths.
//!
//! `includeIf gitdir:` rules follow the path Git actually sees. A rule anchored at a project
//! checkout does not automatically cover `<mount-root>/<owner>/<repo>/<ws>`. Cowshed does not
//! evaluate those patterns: it diffs `git config --list --show-origin` in the checkout against
//! the same listing inside a throwaway repository at the candidate path, then names every
//! origin file that appeared only in the checkout together with the `includeIf` condition
//! whose `.path` value pointed at it. The repository's own config files, and anything they
//! include by a relative path that stays inside the repository, travel with every clone and are
//! never a gap.
//!
//! The answer depends on the host's git configuration and the mount root, never on a workspace,
//! so `setup` and `doctor` run it and `new` does not.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use cowshed_core::api::{Finding, FindingSeverity};
use cowshed_core::fork_lock::Run as _;
use cowshed_core::metadata::WorkspaceName;
use cowshed_core::storage::StorageLayout;
use cowshed_core::{CowshedError, Result};

const HINT: &str = "add an includeIf gitdir: pattern covering the workspace mount root; cowshed setup --mount-root does not rewrite git identity";

/// One config file Git included in the checkout and not at the candidate workspace path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GitIdentityGap {
    pub config_file: PathBuf,
    pub include_if_condition: Option<String>,
}

impl GitIdentityGap {
    pub fn message(&self, mount_root: &Path) -> String {
        match &self.include_if_condition {
            Some(condition) => format!(
                "config file {} is included only in the checkout via includeIf {}; add an includeIf gitdir: pattern covering {}",
                self.config_file.display(),
                condition,
                mount_root.display()
            ),
            None => format!(
                "config file {} is included only in the checkout; add an includeIf gitdir: pattern covering {}",
                self.config_file.display(),
                mount_root.display()
            ),
        }
    }

    pub fn finding(&self, mount_root: &Path) -> Finding {
        Finding {
            code: "git-identity".into(),
            severity: FindingSeverity::Warning,
            message: self.message(mount_root),
            hint: HINT.into(),
            path: Some(self.config_file.clone()),
        }
    }
}

/// Probe one adopted project: its checkout against a throwaway repository where its workspaces
/// mount, which is always removed again. The gaps name `layout.project().host_mount_root` in
/// their remedy.
pub fn probe_project(checkout: &Path, layout: &StorageLayout) -> Result<Vec<GitIdentityGap>> {
    let workspace = WorkspaceName::session("identity-probe")
        .map_err(|error| CowshedError::internal(error.to_string()))?;
    let candidate = layout.workspace_mount(&workspace).map_err(|error| {
        CowshedError::environment_missing(error.to_string(), "cowshed setup --mount-root <dir>")
    })?;
    probe_git_identity_with_env(checkout, &candidate, &[])
}

fn probe_git_identity_with_env(
    checkout: &Path,
    candidate: &Path,
    extra_env: &[(&str, &OsStr)],
) -> Result<Vec<GitIdentityGap>> {
    let probe = ProbeRepo::create(candidate)?;
    let checkout_list = git_config_list(checkout, extra_env)?;
    let probe_list = git_config_list(&probe.path, extra_env)?;
    Ok(diff_identity(
        checkout,
        &probe.path,
        &checkout_list,
        &probe_list,
    ))
}

fn diff_identity(
    checkout: &Path,
    probe: &Path,
    checkout_list: &str,
    probe_list: &str,
) -> Vec<GitIdentityGap> {
    let checkout_cfg = parse_show_origin(checkout_list, checkout);
    let probe_cfg = parse_show_origin(probe_list, probe);
    let checkout_origins = origins_outside(checkout, &checkout_cfg.origins);
    let probe_origins = origins_outside(probe, &probe_cfg.origins);
    checkout_origins
        .difference(&probe_origins)
        .map(|file| GitIdentityGap {
            include_if_condition: checkout_cfg.condition_for(file),
            config_file: file.clone(),
        })
        .collect()
}

struct ParsedConfig {
    origins: BTreeSet<PathBuf>,
    /// includeIf condition → included path, as Git listed them.
    include_if: BTreeMap<PathBuf, String>,
}

impl ParsedConfig {
    fn condition_for(&self, file: &Path) -> Option<String> {
        self.include_if.get(file).cloned().or_else(|| {
            self.include_if.iter().find_map(|(included, condition)| {
                same_config_file(included, file).then(|| condition.clone())
            })
        })
    }
}

/// `root` is the repository the listing was taken in. Git names the repository's own config
/// files relative to it (`.git/config`, and `.git/../tooling/workspace.gitconfig` for a relative
/// include there), so every origin is resolved against it: a relative include inside the
/// repository is then the repository's own file in every clone, not a host file only the
/// checkout loads.
fn parse_show_origin(listing: &str, root: &Path) -> ParsedConfig {
    let mut origins = BTreeSet::new();
    let mut include_if = BTreeMap::new();
    for line in listing.lines() {
        let Some((origin, rest)) = line.split_once('\t') else {
            continue;
        };
        let Some(file) = origin_file(origin, root) else {
            continue;
        };
        origins.insert(file.clone());
        let Some((key, value)) = rest.split_once('=') else {
            continue;
        };
        if let Some(condition) = include_if_condition(key) {
            let included = resolve_include_path(&file, value);
            include_if.insert(included, condition);
        }
    }
    ParsedConfig {
        origins,
        include_if,
    }
}

fn include_if_condition(key: &str) -> Option<String> {
    // Git lowercases config section/variable names, so the listed key is
    // `includeif.gitdir:…/.path` even when the file was written `includeIf`.
    const PREFIX: &str = "includeif.";
    const SUFFIX: &str = ".path";
    if key.len() < PREFIX.len() + SUFFIX.len() {
        return None;
    }
    if !key[..PREFIX.len()].eq_ignore_ascii_case(PREFIX) {
        return None;
    }
    if !key[key.len() - SUFFIX.len()..].eq_ignore_ascii_case(SUFFIX) {
        return None;
    }
    let condition = &key[PREFIX.len()..key.len() - SUFFIX.len()];
    if condition.is_empty() {
        None
    } else {
        Some(condition.to_owned())
    }
}

fn origin_file(origin: &str, root: &Path) -> Option<PathBuf> {
    origin
        .strip_prefix("file:")
        .filter(|path| !path.is_empty())
        .map(|path| lexically_normal(&root.join(path)))
}

fn resolve_include_path(including_file: &Path, value: &str) -> PathBuf {
    let path = PathBuf::from(value);
    if path.is_absolute() {
        path
    } else {
        including_file
            .parent()
            .map(|parent| lexically_normal(&parent.join(path)))
            .unwrap_or_else(|| PathBuf::from(value))
    }
}

/// `path` with `.` dropped and each `..` taking back the component before it. An include that
/// climbs out of the repository is then seen where it lands, outside it. Lexical, so a symlinked
/// component is not followed; both listings are resolved the same way.
fn lexically_normal(path: &Path) -> PathBuf {
    let mut normal = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                if !normal.pop() {
                    normal.push(component);
                }
            }
            Component::Prefix(_) | Component::RootDir | Component::Normal(_) => {
                normal.push(component);
            }
        }
    }
    normal
}

fn origins_outside(repo: &Path, origins: &BTreeSet<PathBuf>) -> BTreeSet<PathBuf> {
    origins
        .iter()
        .filter(|origin| !origin.starts_with(repo))
        .cloned()
        .collect()
}

fn same_config_file(left: &Path, right: &Path) -> bool {
    if left == right {
        return true;
    }
    match (fs::canonicalize(left), fs::canonicalize(right)) {
        (Ok(left), Ok(right)) => left == right,
        _ => false,
    }
}

fn git_config_list(root: &Path, extra_env: &[(&str, &OsStr)]) -> Result<String> {
    git_at(root, extra_env, ["config", "--list", "--show-origin"])
}

fn git_at<const N: usize>(
    root: &Path,
    extra_env: &[(&str, &OsStr)],
    args: [&str; N],
) -> Result<String> {
    let mut command = cowshed_core::git::git_command_at(root);
    command.args(args);
    for (key, value) in extra_env {
        command.env(key, value);
    }
    let output = command
        .output_locked()
        .map_err(|error| cowshed_core::git::git_spawn_error(&error))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let detail = stderr.trim();
        return Err(CowshedError::environment_missing(
            if detail.is_empty() {
                format!(
                    "failed to run git {} in {} (status {})",
                    args.join(" "),
                    root.display(),
                    output.status
                )
            } else {
                format!(
                    "failed to run git {} in {}: {detail}",
                    args.join(" "),
                    root.display()
                )
            },
            "cowshed doctor",
        ));
    }
    String::from_utf8(output.stdout)
        .map_err(|_| CowshedError::internal("git config --list --show-origin is not valid UTF-8"))
}

struct ProbeRepo {
    path: PathBuf,
}

impl ProbeRepo {
    fn create(candidate: &Path) -> Result<Self> {
        let path = unused_probe_path(candidate)?;
        fs::create_dir_all(&path).map_err(|error| {
            CowshedError::environment_missing(
                format!(
                    "could not create git-identity probe at {}: {error}",
                    path.display()
                ),
                HINT,
            )
        })?;
        let probe = Self { path };
        git_at(&probe.path, &[], ["init", "--quiet"])?;
        Ok(probe)
    }
}

impl Drop for ProbeRepo {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn unused_probe_path(candidate: &Path) -> Result<PathBuf> {
    if !candidate.exists() {
        return Ok(candidate.to_path_buf());
    }
    let parent = candidate.parent().ok_or_else(|| {
        CowshedError::internal(format!(
            "git-identity probe path {} has no parent",
            candidate.display()
        ))
    })?;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    Ok(parent.join(format!(
        ".cowshed-identity-probe-{}-{nonce}",
        std::process::id()
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probe_detects_a_fake_conditional_include_and_always_cleans_up() {
        let root = fs::canonicalize(temp_dir("git-identity-probe")).unwrap();
        let checkout = root.join("checkout");
        let candidate = root.join("mnt/acme/widget/raven");
        let extra = root.join("dev.gitconfig");
        let global = root.join("global.gitconfig");
        fs::create_dir_all(&checkout).unwrap();
        fs::write(&extra, "[user]\n    email = cowshed-probe@example.com\n").unwrap();
        fs::write(
            &global,
            format!(
                "[includeIf \"gitdir:{}/\"]\n    path = {}\n",
                checkout.display(),
                extra.display()
            ),
        )
        .unwrap();
        git_at(&checkout, &[], ["init", "--quiet"]).unwrap();

        let env = [
            ("GIT_CONFIG_GLOBAL", global.as_os_str()),
            ("GIT_CONFIG_NOSYSTEM", OsStr::new("1")),
            ("GIT_CONFIG_SYSTEM", OsStr::new("/dev/null")),
        ];
        let listing = git_config_list(&checkout, &env).unwrap();
        assert!(
            listing.contains("cowshed-probe@example.com"),
            "checkout must load the conditional include:\n{listing}"
        );
        let gaps = probe_git_identity_with_env(&checkout, &candidate, &env).unwrap();

        assert!(
            !candidate.exists(),
            "throwaway probe repository must be removed"
        );
        assert_eq!(gaps.len(), 1, "{gaps:?}\ncheckout listing:\n{listing}");
        assert_eq!(gaps[0].config_file, extra);
        assert_eq!(
            gaps[0].include_if_condition.as_deref(),
            Some(format!("gitdir:{}/", checkout.display()).as_str())
        );
        let finding = gaps[0].finding(Path::new("/Users/dev/.cowshed/mnt"));
        assert_eq!(finding.code, "git-identity");
        assert_eq!(finding.severity, FindingSeverity::Warning);
        assert!(finding.message.contains(extra.to_str().unwrap()));
        assert!(finding.message.contains("includeIf gitdir:"));
        assert!(
            finding
                .message
                .contains("add an includeIf gitdir: pattern covering /Users/dev/.cowshed/mnt")
        );
        assert!(
            !finding.hint.starts_with("cowshed setup"),
            "the next step must not be a setup command that cannot add includeIf patterns; hint was {}",
            finding.hint
        );
        assert!(finding.hint.contains("does not rewrite git identity"));
        assert_eq!(finding.hint, HINT);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn matching_anchor_is_not_a_gap() {
        let root = fs::canonicalize(temp_dir("git-identity-shared-anchor")).unwrap();
        let checkout = root.join("Dev/widget");
        let candidate = root.join("Dev/.cowshed/acme/widget/raven");
        let extra = root.join("dev.gitconfig");
        let global = root.join("global.gitconfig");
        fs::create_dir_all(&checkout).unwrap();
        fs::write(&extra, "[user]\n    name = Shared\n").unwrap();
        fs::write(
            &global,
            format!(
                "[includeIf \"gitdir:{}/\"]\n    path = {}\n",
                root.join("Dev").display(),
                extra.display()
            ),
        )
        .unwrap();
        git_at(&checkout, &[], ["init", "--quiet"]).unwrap();

        let env = [
            ("GIT_CONFIG_GLOBAL", global.as_os_str()),
            ("GIT_CONFIG_NOSYSTEM", OsStr::new("1")),
            ("GIT_CONFIG_SYSTEM", OsStr::new("/dev/null")),
        ];
        let gaps = probe_git_identity_with_env(&checkout, &candidate, &env).unwrap();
        assert!(gaps.is_empty(), "{gaps:?}");
        assert!(!candidate.exists());
        let _ = fs::remove_dir_all(root);
    }

    /// A repository's own config is copied with every clone, so an include it names by a relative
    /// path resolves inside each clone: no identity gap, however Git spells its origin
    /// (`.git/../tooling/workspace.gitconfig`). A relative include that climbs out of the
    /// repository lands somewhere else for a clone mounted elsewhere, and still is one.
    #[test]
    fn a_relative_include_inside_the_repository_is_not_a_gap_and_one_outside_it_is() {
        let root = fs::canonicalize(temp_dir("git-identity-relative-include")).unwrap();
        let checkout = root.join("checkout");
        let candidate = root.join("mnt/acme/widget/raven");
        let global = root.join("global.gitconfig");
        let shared = root.join("shared.gitconfig");
        fs::create_dir_all(checkout.join("tooling")).unwrap();
        fs::write(
            checkout.join("tooling/workspace.gitconfig"),
            "[merge \"newer-pins\"]\n    name = keep the newer pins\n",
        )
        .unwrap();
        fs::write(&shared, "[user]\n    name = Shared\n").unwrap();
        fs::write(&global, "").unwrap();
        git_at(&checkout, &[], ["init", "--quiet"]).unwrap();
        let env = [
            ("GIT_CONFIG_GLOBAL", global.as_os_str()),
            ("GIT_CONFIG_NOSYSTEM", OsStr::new("1")),
            ("GIT_CONFIG_SYSTEM", OsStr::new("/dev/null")),
        ];
        git_at(
            &checkout,
            &env,
            ["config", "include.path", "../tooling/workspace.gitconfig"],
        )
        .unwrap();
        let gaps = probe_git_identity_with_env(&checkout, &candidate, &env).unwrap();
        assert!(gaps.is_empty(), "{gaps:?}");

        git_at(
            &checkout,
            &env,
            ["config", "--add", "include.path", "../../shared.gitconfig"],
        )
        .unwrap();
        let gaps = probe_git_identity_with_env(&checkout, &candidate, &env).unwrap();
        assert_eq!(
            gaps,
            vec![GitIdentityGap {
                config_file: shared,
                include_if_condition: None,
            }]
        );
        assert!(!candidate.exists());
        let _ = fs::remove_dir_all(root);
    }

    fn temp_dir(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("cowshed-{label}-{}-{nonce}", std::process::id()));
        fs::create_dir_all(&path).unwrap();
        path
    }
}
