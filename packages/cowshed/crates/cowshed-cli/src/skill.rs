//! Installing the bundled agent skill into whichever harnesses a host runs.
//!
//! The skill is `include_str!`d rather than read from disk so the standalone
//! binary and the npm bin — which share dispatch but not a working directory —
//! install byte-identical content with no path resolution to get wrong.

use std::io;
use std::path::{Path, PathBuf};

use cowshed_core::CowshedError;
use cowshed_core::api::{SkillInstall, SkillInstallReport, SkillInstallStatus};

use crate::args::{GlobalOptions, SkillArgs, SkillCommand};
use crate::output::Output;

/// The skill's directory name, and therefore how a harness refers to it.
pub const SKILL_NAME: &str = "cowshed";

/// The shipped skill. The same bytes are published in the npm package under
/// `skills/cowshed/SKILL.md`.
pub const SKILL_MD: &str = include_str!("../../../skills/cowshed/SKILL.md");

/// One agent harness and where it keeps skills.
///
/// `root` is the harness's configuration directory: its existence is what marks
/// the harness as present. Probing `root` rather than `skills` matters because a
/// harness that has never installed a skill still has no `skills` directory.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Harness {
    pub name: &'static str,
    root: &'static str,
    skills: &'static str,
}

impl Harness {
    /// The directory that receives this skill, under `base`.
    #[must_use]
    pub fn destination(&self, base: &Path) -> PathBuf {
        base.join(self.skills).join(SKILL_NAME)
    }

    /// The file this skill installs to, under `base`.
    #[must_use]
    pub fn skill_file(&self, base: &Path) -> PathBuf {
        self.destination(base).join("SKILL.md")
    }

    #[must_use]
    pub fn root(&self, base: &Path) -> PathBuf {
        base.join(self.root)
    }
}

/// Whether an install targets a home directory or a single repository.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Scope {
    Global,
    Project,
}

const CLAUDE: Harness = Harness {
    name: "claude",
    root: ".claude",
    skills: ".claude/skills",
};

/// Harnesses that keep skills in the user's home directory.
pub const GLOBAL_HARNESSES: &[Harness] = &[
    CLAUDE,
    Harness {
        name: "cursor",
        root: ".cursor",
        skills: ".cursor/skills",
    },
    Harness {
        name: "opencode",
        root: ".opencode",
        skills: ".opencode/skills",
    },
    Harness {
        name: "goose",
        root: ".config/goose",
        skills: ".config/goose/skills",
    },
    Harness {
        name: "amp",
        root: ".amp",
        skills: ".amp/skills",
    },
];

/// Harnesses that keep skills inside the repository. Copilot and VS Code read
/// `.github/skills`, which only ever exists per repository.
pub const PROJECT_HARNESSES: &[Harness] = &[
    CLAUDE,
    Harness {
        name: "cursor",
        root: ".cursor",
        skills: ".cursor/skills",
    },
    Harness {
        name: "opencode",
        root: ".opencode",
        skills: ".opencode/skills",
    },
    Harness {
        name: "copilot",
        root: ".github",
        skills: ".github/skills",
    },
];

#[must_use]
pub fn harnesses(scope: Scope) -> &'static [Harness] {
    match scope {
        Scope::Global => GLOBAL_HARNESSES,
        Scope::Project => PROJECT_HARNESSES,
    }
}

/// Resolve a `--harness` value within a scope. Returns `None` for an unknown
/// name so the parser can reject it as usage before anything runs.
#[must_use]
pub fn harness_named(scope: Scope, name: &str) -> Option<Harness> {
    harnesses(scope)
        .iter()
        .copied()
        .find(|harness| harness.name == name)
}

/// The accepted `--harness` values for a scope, for usage messages.
#[must_use]
pub fn harness_names(scope: Scope) -> String {
    harnesses(scope)
        .iter()
        .map(|harness| harness.name)
        .collect::<Vec<_>>()
        .join("|")
}

/// Choose install targets without touching the filesystem.
///
/// An explicit `selected` list is honoured verbatim — asking for a harness is
/// reason enough to create its directory. Otherwise only harnesses already
/// present on the host are written, so an install never fabricates config
/// directories for tools the user does not run. Claude Code is the fallback when
/// nothing is detected, because a skill that installs nowhere is a silent no-op.
#[must_use]
pub fn plan(
    base: &Path,
    scope: Scope,
    selected: &[Harness],
    exists: &dyn Fn(&Path) -> bool,
) -> Vec<Harness> {
    if !selected.is_empty() {
        return selected.to_vec();
    }
    let detected: Vec<Harness> = harnesses(scope)
        .iter()
        .copied()
        .filter(|harness| exists(&harness.root(base)))
        .collect();
    if detected.is_empty() {
        vec![CLAUDE]
    } else {
        detected
    }
}

/// Write the skill to each planned harness, skipping files that already hold the
/// shipped bytes. Reading before writing is what makes a repeat install report
/// `Unchanged` and leave mtimes alone.
pub fn install(base: &Path, planned: &[Harness]) -> io::Result<SkillInstallReport> {
    let mut installs = Vec::with_capacity(planned.len());
    for harness in planned {
        let path = harness.skill_file(base);
        let status = if std::fs::read_to_string(&path).is_ok_and(|current| current == SKILL_MD) {
            SkillInstallStatus::Unchanged
        } else {
            std::fs::create_dir_all(harness.destination(base))?;
            std::fs::write(&path, SKILL_MD)?;
            SkillInstallStatus::Written
        };
        installs.push(SkillInstall {
            harness: harness.name.to_owned(),
            path,
            status,
        });
    }
    Ok(SkillInstallReport {
        skill: SKILL_NAME.to_owned(),
        installs,
    })
}

/// Resolve the install base: the repository for `--project`, else `$HOME`.
///
/// `$HOME` is read explicitly rather than inferred so a daemon or sandbox with
/// no home fails as an environment error instead of writing to a surprising
/// path.
pub fn base_directory(project: Option<&Path>) -> Result<(PathBuf, Scope), CowshedError> {
    if let Some(project) = project {
        return Ok((project.to_path_buf(), Scope::Project));
    }
    let home = std::env::var_os("HOME").ok_or_else(|| {
        CowshedError::environment_missing(
            "HOME is not set, so the global skill directory cannot be resolved",
            "cowshed skill install --project <path>",
        )
    })?;
    Ok((PathBuf::from(home), Scope::Global))
}

/// Run `skill install`.
///
/// This never reaches the coordinator or host storage: installing a skill has to
/// work on a host where `adopt` has not run, which is exactly when an agent most
/// needs the skill telling it to run `adopt`.
pub fn dispatch<W, E>(
    args: &SkillArgs,
    global: &GlobalOptions,
    output: &mut Output<W, E>,
) -> Result<i32, CowshedError>
where
    W: io::Write,
    E: io::Write,
{
    let SkillCommand::Install = args.action;
    let (base, scope) = base_directory(global.project.as_deref())?;
    let selected: Vec<Harness> = args
        .harnesses
        .iter()
        .filter_map(|name| harness_named(scope, name))
        .collect();
    let planned = plan(&base, scope, &selected, &|path| path.exists());
    let report = install(&base, &planned).map_err(|error| {
        CowshedError::internal(format!("could not install the {SKILL_NAME} skill: {error}"))
    })?;

    if global.json {
        output.success(report).map_err(output_error)?;
        return Ok(0);
    }
    for entry in &report.installs {
        let status = match entry.status {
            SkillInstallStatus::Written => "written",
            SkillInstallStatus::Unchanged => "unchanged",
        };
        output
            .bare_line(format!("{}\t{status}\t{}", entry.harness, entry.path.display()).as_bytes())
            .map_err(output_error)?;
    }
    let written = report
        .installs
        .iter()
        .filter(|entry| entry.status == SkillInstallStatus::Written)
        .count();
    output
        .guidance(&format!(
            "installed the {SKILL_NAME} skill for {} harness(es); {written} written, {} unchanged",
            report.installs.len(),
            report.installs.len() - written
        ))
        .map_err(output_error)?;
    output
        .hint("cowshed adopt <git-root>")
        .map_err(output_error)?;
    Ok(0)
}

fn output_error(error: io::Error) -> CowshedError {
    CowshedError::internal(format!("could not write command output: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn never(_: &Path) -> bool {
        false
    }

    /// A fresh directory per test, following the crate's temp-dir convention.
    fn scratch(label: &str) -> PathBuf {
        let directory =
            std::env::temp_dir().join(format!("cowshed-cli-skill-{label}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).expect("scratch");
        directory
    }

    /// Fold the frontmatter into `(key, value)` pairs the way a YAML reader
    /// does. The repository's markdown formatter rewraps long plain scalars onto
    /// indented continuation lines, so asserting on line layout would test the
    /// formatter rather than the contract a harness actually reads.
    fn frontmatter(document: &str) -> Vec<(String, String)> {
        let body = document
            .strip_prefix("---\n")
            .expect("frontmatter opens on the first line");
        let (frontmatter, _) = body.split_once("\n---\n").expect("frontmatter closes");
        let mut pairs: Vec<(String, String)> = Vec::new();
        for line in frontmatter.lines() {
            let key_here = (!line.starts_with(char::is_whitespace))
                .then(|| {
                    line.split_once(": ")
                        .map(|(key, value)| (key, value.trim()))
                        .or_else(|| line.strip_suffix(':').map(|key| (key, "")))
                })
                .flatten();
            match key_here {
                Some((key, value)) => {
                    pairs.push((key.to_owned(), value.to_owned()));
                }
                None => {
                    let (_, value) = pairs.last_mut().expect("a continuation follows a key");
                    if !value.is_empty() {
                        value.push(' ');
                    }
                    value.push_str(line.trim());
                }
            }
        }
        pairs
    }

    #[test]
    fn shipped_skill_carries_the_frontmatter_a_harness_indexes() {
        let pairs = frontmatter(SKILL_MD);

        let keys: Vec<&str> = pairs.iter().map(|(key, _)| key.as_str()).collect();
        assert_eq!(
            keys,
            ["name", "description"],
            "a harness indexes exactly name and description"
        );
        assert_eq!(pairs[0].1, SKILL_NAME);
        assert!(
            pairs[1].1.len() > 40 && !pairs[1].1.contains('\n'),
            "the description folds to one non-trivial line: {:?}",
            pairs[1].1
        );
    }

    #[test]
    fn detection_selects_present_harnesses_and_never_creates_unused_ones() {
        let base = Path::new("/home/agent");
        let present = |path: &Path| {
            path == Path::new("/home/agent/.claude") || path == Path::new("/home/agent/.cursor")
        };

        let planned = plan(base, Scope::Global, &[], &present);

        let names: Vec<&str> = planned.iter().map(|harness| harness.name).collect();
        assert_eq!(names, ["claude", "cursor"]);
    }

    #[test]
    fn an_undetected_host_still_installs_for_claude_rather_than_nowhere() {
        let planned = plan(Path::new("/home/agent"), Scope::Global, &[], &never);

        let names: Vec<&str> = planned.iter().map(|harness| harness.name).collect();
        assert_eq!(names, ["claude"]);
    }

    #[test]
    fn an_explicit_harness_is_installed_even_when_absent() {
        let goose = harness_named(Scope::Global, "goose").expect("goose is a global harness");

        let planned = plan(Path::new("/home/agent"), Scope::Global, &[goose], &never);

        let names: Vec<&str> = planned.iter().map(|harness| harness.name).collect();
        assert_eq!(names, ["goose"]);
    }

    #[test]
    fn scopes_expose_only_the_harnesses_that_can_hold_skills_there() {
        assert!(
            harness_named(Scope::Project, "copilot").is_some(),
            "copilot reads .github/skills, which is per repository"
        );
        assert!(
            harness_named(Scope::Global, "copilot").is_none(),
            "copilot has no home-directory skill directory"
        );
        assert!(
            harness_named(Scope::Project, "goose").is_none(),
            "goose keeps skills under the home config directory only"
        );
        assert!(harness_named(Scope::Global, "nonesuch").is_none());
    }

    #[test]
    fn install_writes_once_then_reports_unchanged_without_rewriting() {
        let base = scratch("idempotent");
        let planned = vec![CLAUDE];

        let first = install(&base, &planned).expect("first install");
        assert_eq!(first.installs[0].status, SkillInstallStatus::Written);
        assert_eq!(first.skill, SKILL_NAME);
        assert_eq!(
            std::fs::read_to_string(&first.installs[0].path).expect("written"),
            SKILL_MD
        );
        let written_at = std::fs::metadata(&first.installs[0].path)
            .and_then(|metadata| metadata.modified())
            .expect("mtime");

        let second = install(&base, &planned).expect("second install");
        assert_eq!(second.installs[0].status, SkillInstallStatus::Unchanged);
        assert_eq!(
            std::fs::metadata(&first.installs[0].path)
                .and_then(|metadata| metadata.modified())
                .expect("mtime"),
            written_at,
            "an unchanged install must not touch the file"
        );
        std::fs::remove_dir_all(&base).expect("cleanup");
    }

    #[test]
    fn a_drifted_skill_is_restored_to_the_shipped_bytes() {
        let base = scratch("drift");
        let planned = vec![CLAUDE];
        let path = CLAUDE.skill_file(&base);
        std::fs::create_dir_all(CLAUDE.destination(&base)).expect("mkdir");
        std::fs::write(&path, "stale").expect("seed");

        let report = install(&base, &planned).expect("install");

        assert_eq!(report.installs[0].status, SkillInstallStatus::Written);
        assert_eq!(std::fs::read_to_string(&path).expect("read"), SKILL_MD);
        std::fs::remove_dir_all(&base).expect("cleanup");
    }
}
