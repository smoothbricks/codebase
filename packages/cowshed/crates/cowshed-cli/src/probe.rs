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
//! Two spellings of one file are told by the real path each resolves to. The decision is pure over
//! the two listings and over those paths, which the shell reads for the few spellings that need it.
//!
//! The answer depends on the host's git configuration and the mount root, never on a workspace,
//! so `setup` and `doctor` run it and `fork` does not.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use cowshed_core::api::{Finding, FindingSeverity};
use cowshed_core::metadata::WorkspaceName;
use cowshed_core::storage::StorageLayout;
use cowshed_core::{CowshedError, Result};
use cowshed_os::fork_lock::Run as _;

const HINT: &str = "add an includeIf gitdir: pattern covering the workspace mount root; cowshed setup does not rewrite git identity";

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
    let candidate = layout
        .workspace_mount(&workspace)
        .map_err(|error| CowshedError::environment_missing(error.to_string(), "cowshed setup"))?;
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
    let found = Found::between(checkout, &probe.path, &checkout_list, &probe_list);
    let observed = observe(&found.spellings_to_resolve())?;
    Ok(found.gaps(&observed))
}

/// What the two listings say before the disk is asked anything: the files only the checkout loads
/// from outside its repository, and the checkout's own parse, which names their conditions.
struct Found {
    files: BTreeSet<PathBuf>,
    config: ParsedConfig,
}

impl Found {
    fn between(checkout: &Path, probe: &Path, checkout_list: &str, probe_list: &str) -> Self {
        let config = parse_show_origin(checkout_list, checkout);
        let probe_config = parse_show_origin(probe_list, probe);
        let probe_origins = origins_outside(probe, &probe_config.origins);
        let files = origins_outside(checkout, &config.origins)
            .difference(&probe_origins)
            .cloned()
            .collect();
        Self { files, config }
    }

    /// The spellings whose real files the disk has to show before a condition can be named.
    fn spellings_to_resolve(&self) -> BTreeSet<PathBuf> {
        self.config.spellings_to_resolve(&self.files)
    }

    /// The gaps, each with the condition that named its file, given what the disk showed.
    fn gaps(self, observed: &Observed) -> Vec<GitIdentityGap> {
        self.files
            .into_iter()
            .map(|file| GitIdentityGap {
                include_if_condition: self.config.condition_for(&file, observed),
                config_file: file,
            })
            .collect()
    }
}

struct ParsedConfig {
    origins: BTreeSet<PathBuf>,
    /// includeIf condition → included path, as Git listed them.
    include_if: BTreeMap<PathBuf, String>,
}

impl ParsedConfig {
    /// The condition whose included path is this file: that path as Git listed it, else a spelling
    /// the disk showed resolving to the same real file.
    fn condition_for(&self, file: &Path, observed: &Observed) -> Option<String> {
        self.include_if.get(file).cloned().or_else(|| {
            self.include_if.iter().find_map(|(included, condition)| {
                same_config_file(included, file, observed).then(|| condition.clone())
            })
        })
    }

    /// The spellings condition_for compares by their real files for these files: none when every
    /// file is named by its own spelling, else each file that is not, and every included path.
    fn spellings_to_resolve(&self, files: &BTreeSet<PathBuf>) -> BTreeSet<PathBuf> {
        let unnamed: Vec<&PathBuf> = files
            .iter()
            .filter(|file| !self.include_if.contains_key(*file))
            .collect();
        if unnamed.is_empty() {
            return BTreeSet::new();
        }
        unnamed
            .into_iter()
            .chain(self.include_if.keys())
            .cloned()
            .collect()
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

/// What the disk says of one spelling of a config file's path.
#[derive(Clone, Debug, Eq, PartialEq)]
enum Canonical {
    /// The file the spelling resolves to once every symlink is followed.
    Real(PathBuf),
    /// Nothing is there: an include Git skips as missing, or a spelling that names no file.
    Absent,
}

/// What the disk showed of each spelling the decision asked about.
type Observed = BTreeMap<PathBuf, Canonical>;

/// Whether two spellings are one file: the same spelling, or two the disk showed resolving to the
/// same real file. A spelling with nothing at it is no file, and so no other spelling's alias.
fn same_config_file(left: &Path, right: &Path, observed: &Observed) -> bool {
    let shown = |spelling: &Path| {
        observed
            .get(spelling)
            .expect("the disk was asked about every spelling the decision compares")
    };
    left == right
        || matches!(
            (shown(left), shown(right)),
            (Canonical::Real(real_left), Canonical::Real(real_right)) if real_left == real_right
        )
}

/// The disk's answer for each spelling, the one place this probe follows symlinks. A spelling with
/// nothing at it is absent; a read that fails any other way cannot tell absent from different, so
/// it fails the probe naming the path, and is never taken for an answer.
fn observe(spellings: &BTreeSet<PathBuf>) -> Result<Observed> {
    spellings
        .iter()
        .map(|spelling| match fs::canonicalize(spelling) {
            Ok(real) => Ok((spelling.clone(), Canonical::Real(real))),
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::NotFound | io::ErrorKind::NotADirectory
                ) =>
            {
                Ok((spelling.clone(), Canonical::Absent))
            }
            Err(error) => Err(CowshedError::environment_missing(
                format!(
                    "could not resolve git config path {}: {error}",
                    spelling.display()
                ),
                "cowshed doctor",
            )),
        })
        .collect()
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
    // WHY unbounded: a git of this host's own repository with hooks, fsmonitor, pager, editors and
    // prompts off (`git_command_at`): it waits on no other process.
    #[allow(clippy::disallowed_methods)]
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

    /// One setting as git config --list --show-origin prints it: its origin, then the setting.
    type Line = (&'static str, &'static str);

    /// Git names a repository's own config relative to the repository.
    const LOCAL: Line = ("file:.git/config", "core.repositoryformatversion=0");

    fn listing(lines: &[Line]) -> String {
        lines
            .iter()
            .map(|(origin, setting)| format!("{origin}\t{setting}\n"))
            .collect()
    }

    /// Which origins are gaps is a decision over two listings, the checkout's and the candidate
    /// mount's, so it is a table over their lines and never a Git run: only what the checkout
    /// loads from outside its repository and the candidate does not is a gap, and it carries the
    /// includeIf condition that named it. A repository's own files travel with every clone,
    /// however Git spells their origin. The real-Git run above establishes that the listings are
    /// what these rows say, and that its include loading is the fixture's adversary. These rows
    /// plant nothing of the disk: what it shows of a spelling is a fact the next test plants.
    #[test]
    fn a_gap_is_an_origin_only_the_checkout_loads_from_outside_its_repository() {
        let gap = |file: &str, condition: Option<&str>| GitIdentityGap {
            config_file: PathBuf::from(file),
            include_if_condition: condition.map(str::to_owned),
        };
        let inside: [Line; 2] = [
            (
                "file:.git/config",
                "include.path=../tooling/workspace.gitconfig",
            ),
            (
                "file:.git/../tooling/workspace.gitconfig",
                "merge.newer-pins.name=keep the newer pins",
            ),
        ];
        let outside: [Line; 2] = [
            ("file:.git/config", "include.path=../../shared.gitconfig"),
            ("file:.git/../../shared.gitconfig", "user.name=Shared"),
        ];
        let under_dev: [Line; 3] = [
            (
                "file:/r/global.gitconfig",
                "includeif.gitdir:/r/Dev/.path=/r/dev.gitconfig",
            ),
            ("file:/r/dev.gitconfig", "user.name=Shared"),
            LOCAL,
        ];
        let at_checkout: [Line; 3] = [
            (
                "file:/r/global.gitconfig",
                "includeif.gitdir:/r/checkout/.path=/r/dev.gitconfig",
            ),
            (
                "file:/r/dev.gitconfig",
                "user.email=cowshed-probe@example.com",
            ),
            LOCAL,
        ];
        let cases = [
            (
                "an anchor above both the checkout and the candidate covers both",
                "/r/Dev/widget",
                "/r/Dev/.cowshed/acme/widget/raven",
                under_dev.to_vec(),
                under_dev.to_vec(),
                vec![],
            ),
            (
                "an include anchored at the checkout only is the one gap, naming its condition",
                "/r/checkout",
                "/r/mnt/acme/widget/raven",
                at_checkout.to_vec(),
                vec![at_checkout[0], LOCAL],
                vec![gap("/r/dev.gitconfig", Some("gitdir:/r/checkout/"))],
            ),
            (
                "a relative include inside the repository travels with every clone",
                "/r/checkout",
                "/r/mnt/acme/widget/raven",
                [vec![LOCAL], inside.to_vec()].concat(),
                vec![LOCAL],
                vec![],
            ),
            (
                "a relative include that climbs out of the repository is a gap with no condition",
                "/r/checkout",
                "/r/mnt/acme/widget/raven",
                [vec![LOCAL], inside.to_vec(), outside.to_vec()].concat(),
                vec![LOCAL],
                vec![gap("/r/shared.gitconfig", None)],
            ),
            (
                "a setting that came from no file names no file",
                "/r/checkout",
                "/r/mnt/acme/widget/raven",
                vec![("command line:", "user.name=Arg"), LOCAL],
                vec![LOCAL],
                vec![],
            ),
            (
                "nothing included anywhere",
                "/r/checkout",
                "/r/mnt/acme/widget/raven",
                vec![LOCAL],
                vec![LOCAL],
                vec![],
            ),
        ];
        for (label, checkout, candidate, checkout_lines, probe_lines, expected) in cases {
            assert_eq!(
                gaps_of(checkout, candidate, &checkout_lines, &probe_lines, &[]),
                expected,
                "{label}"
            );
        }
    }

    /// What the disk shows, planted: each spelling the decision asks about resolves where the row
    /// says, else nothing is there.
    fn shown(asked: BTreeSet<PathBuf>, shows: &[(&str, &str)]) -> Observed {
        asked
            .into_iter()
            .map(|spelling| {
                let form = shows
                    .iter()
                    .find(|(from, _)| Path::new(from) == spelling)
                    .map_or(Canonical::Absent, |(_, real)| {
                        Canonical::Real(PathBuf::from(real))
                    });
                (spelling, form)
            })
            .collect()
    }

    fn gaps_of(
        checkout: &str,
        candidate: &str,
        checkout_lines: &[Line],
        probe_lines: &[Line],
        shows: &[(&str, &str)],
    ) -> Vec<GitIdentityGap> {
        let found = Found::between(
            Path::new(checkout),
            Path::new(candidate),
            &listing(checkout_lines),
            &listing(probe_lines),
        );
        let observed = shown(found.spellings_to_resolve(), shows);
        found.gaps(&observed)
    }

    /// An include can name a file by another spelling than the one Git listed it under. What the
    /// disk shows of each spelling is a fact handed to the decision, so the table plants it: two
    /// spellings are one file only when both resolve to the same real file, and a spelling with
    /// nothing at it, or resolving elsewhere, is no alias.
    #[test]
    fn a_condition_follows_an_include_to_the_real_file_its_spelling_resolves_to() {
        let global: Line = (
            "file:/r/global.gitconfig",
            "includeif.gitdir:/r/checkout/.path=/r/link/dev.gitconfig",
        );
        let included: Line = (
            "file:/r/real/dev.gitconfig",
            "user.email=cowshed-probe@example.com",
        );
        let gap = |condition: Option<&str>| {
            vec![GitIdentityGap {
                config_file: PathBuf::from("/r/real/dev.gitconfig"),
                include_if_condition: condition.map(str::to_owned),
            }]
        };
        let real = ("/r/real/dev.gitconfig", "/r/real/dev.gitconfig");
        for (label, shows, expected) in [
            (
                "two spellings of one real file are one file",
                vec![("/r/link/dev.gitconfig", "/r/real/dev.gitconfig"), real],
                gap(Some("gitdir:/r/checkout/")),
            ),
            (
                "spellings that resolve to different files are two files",
                vec![("/r/link/dev.gitconfig", "/r/other/dev.gitconfig"), real],
                gap(None),
            ),
            (
                "a spelling with nothing at it is no alias, however absent both are",
                vec![],
                gap(None),
            ),
        ] {
            assert_eq!(
                gaps_of(
                    "/r/checkout",
                    "/r/mnt/acme/widget/raven",
                    &[global, included, LOCAL],
                    &[global, LOCAL],
                    &shows
                ),
                expected,
                "{label}"
            );
        }
    }

    /// The shell's answers: a spelling through a symlink resolves to the real file; a spelling with
    /// nothing at it, or under a file, is absent; a read that fails any other way (a symlink loop)
    /// fails the probe naming the path, never an answer.
    #[test]
    fn the_disk_is_asked_per_spelling_and_a_failure_to_tell_fails_the_probe() {
        let root = fs::canonicalize(temp_dir("git-identity-observe")).unwrap();
        let real = root.join("real");
        fs::create_dir_all(&real).unwrap();
        fs::write(real.join("dev.gitconfig"), "").unwrap();
        std::os::unix::fs::symlink(&real, root.join("link")).unwrap();
        let through_link = root.join("link/dev.gitconfig");
        let direct = real.join("dev.gitconfig");
        let under_a_file = direct.join("inside");
        let absent = root.join("absent.gitconfig");

        let observed = observe(&BTreeSet::from([
            through_link.clone(),
            direct.clone(),
            under_a_file.clone(),
            absent.clone(),
        ]))
        .expect("every spelling answers");
        assert_eq!(observed[&through_link], Canonical::Real(direct.clone()));
        assert_eq!(observed[&direct], Canonical::Real(direct.clone()));
        assert_eq!(observed[&under_a_file], Canonical::Absent);
        assert_eq!(observed[&absent], Canonical::Absent);

        std::os::unix::fs::symlink(root.join("loop_b"), root.join("loop_a")).unwrap();
        std::os::unix::fs::symlink(root.join("loop_a"), root.join("loop_b")).unwrap();
        let error = observe(&BTreeSet::from([root.join("loop_a")]))
            .expect_err("a symlink loop cannot be told from absence");
        assert!(error.to_string().contains("loop_a"), "{error}");

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
