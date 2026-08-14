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

mod generated;

pub use generated::GENERATED_HARNESSES;

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

/// One harness's directories, independent of install scope.
///
/// This is the shape the generated upstream snapshot emits and the shape a
/// verified override supplies, so the two compose by name without translation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HarnessEntry {
    pub name: &'static str,
    /// Configuration directory, relative to the home directory. Its existence
    /// is what marks the harness as installed.
    pub global_root: &'static str,
    /// Skills directory, relative to the home directory.
    pub global_skills: &'static str,
    /// Skills directory, relative to a repository root.
    pub project_skills: &'static str,
}

/// A harness whose directories were verified against the harness itself on a
/// real host, overriding the upstream snapshot by name.
///
/// `reason` is required: an override is a claim that upstream is wrong or silent
/// for this harness, and that claim has to carry its evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VerifiedHarness {
    pub entry: HarnessEntry,
    pub reason: &'static str,
}

/// Hand-verified harnesses. These OVERRIDE `GENERATED_HARNESSES` by name.
///
/// Deliberately minimal: an override that merely restates upstream is the drift
/// this snapshot exists to remove. `claude-code` and `codex` are absent here
/// because the snapshot already matches what this host shows.
pub const VERIFIED_HARNESSES: &[VerifiedHarness] = &[VerifiedHarness {
    entry: HarnessEntry {
        name: "omp",
        global_root: ".omp",
        global_skills: ".omp/agent/skills",
        project_skills: ".omp/skills",
    },
    reason: "absent upstream; omp's own binary states that user-authored skills \
             live under ~/.omp/agent/skills and .omp/skills, and reserves \
             ~/.omp/agent/managed-skills for skills its auto-learn may rewrite",
}];

/// The harness installed to when detection finds nothing, so that an install is
/// never a silent no-op.
const FALLBACK_HARNESS: &str = "claude-code";

/// Verified entries first, then every generated entry they do not override.
#[must_use]
pub fn harness_entries() -> Vec<&'static HarnessEntry> {
    let mut entries: Vec<&'static HarnessEntry> = VERIFIED_HARNESSES
        .iter()
        .map(|verified| &verified.entry)
        .collect();
    let overridden: Vec<&str> = entries.iter().map(|entry| entry.name).collect();
    entries.extend(
        GENERATED_HARNESSES
            .iter()
            .filter(|generated| !overridden.contains(&generated.name)),
    );
    entries
}

impl HarnessEntry {
    fn in_scope(&self, scope: Scope) -> Harness {
        let skills = match scope {
            Scope::Global => self.global_skills,
            Scope::Project => self.project_skills,
        };
        let root = match scope {
            Scope::Global => self.global_root,
            // A repository-relative skills directory always sits under the
            // harness's own dotted directory, so its first segment is the probe.
            Scope::Project => skills.split('/').next().unwrap_or(skills),
        };
        Harness {
            name: self.name,
            root,
            skills,
        }
    }
}

#[must_use]
pub fn harnesses(scope: Scope) -> Vec<Harness> {
    harness_entries()
        .into_iter()
        .map(|entry| entry.in_scope(scope))
        .collect()
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

/// A sample of accepted `--harness` values, for usage messages.
///
/// The snapshot carries dozens of harnesses, so a usage error names a few and
/// reports the total rather than printing an unreadable wall of names.
#[must_use]
pub fn harness_names(scope: Scope) -> String {
    const SAMPLE: usize = 8;
    let all = harnesses(scope);
    let sample = all
        .iter()
        .take(SAMPLE)
        .map(|harness| harness.name)
        .collect::<Vec<_>>()
        .join("|");
    if all.len() > SAMPLE {
        format!("{sample}| and {} more", all.len() - SAMPLE)
    } else {
        sample
    }
}

/// Harness names related to `name`, for correcting a near miss.
///
/// Substring matching in both directions is what turns the common rename
/// mistakes — `copilot` for `github-copilot`, `claude` for `claude-code` — into
/// a usable suggestion instead of an alphabetical sample of dozens of names.
#[must_use]
pub fn harness_suggestions(scope: Scope, name: &str) -> Vec<&'static str> {
    if name.is_empty() {
        return Vec::new();
    }
    let lowered = name.to_ascii_lowercase();
    harnesses(scope)
        .into_iter()
        .filter(|harness| {
            // Only a substantial harness name may match by being contained in
            // the input, or short names match everything: `pi` is inside
            // `copilot` and would drown the real suggestion.
            harness.name.contains(lowered.as_str())
                || (harness.name.len() >= 4 && lowered.contains(harness.name))
        })
        .map(|harness| harness.name)
        .collect()
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
        harness_named(scope, FALLBACK_HARNESS).into_iter().collect()
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

    /// The fallback harness, resolved through the composed table.
    fn fallback() -> Harness {
        harness_named(Scope::Global, FALLBACK_HARNESS).expect("the fallback harness exists")
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
        assert_eq!(names, ["claude-code", "cursor"]);
    }

    #[test]
    fn an_undetected_host_still_installs_for_the_fallback_rather_than_nowhere() {
        let planned = plan(Path::new("/home/agent"), Scope::Global, &[], &never);

        let names: Vec<&str> = planned.iter().map(|harness| harness.name).collect();
        assert_eq!(names, [FALLBACK_HARNESS]);
    }

    #[test]
    fn an_explicit_harness_is_installed_even_when_absent() {
        let goose = harness_named(Scope::Global, "goose").expect("goose is a global harness");

        let planned = plan(Path::new("/home/agent"), Scope::Global, &[goose], &never);

        let names: Vec<&str> = planned.iter().map(|harness| harness.name).collect();
        assert_eq!(names, ["goose"]);
    }

    /// Spot-checks, not the whole table: the snapshot is refreshed from
    /// upstream, so pinning every entry would turn a routine refresh into a test
    /// rewrite. These four cover the shapes that break silently.
    #[test]
    fn representative_harnesses_target_their_published_discovery_paths() {
        let base = Path::new("/home/agent");
        let global = |name: &str| {
            harness_named(Scope::Global, name)
                .unwrap_or_else(|| panic!("{name} is a global harness"))
                .skill_file(base)
                .display()
                .to_string()
        };

        assert_eq!(
            global("claude-code"),
            "/home/agent/.claude/skills/cowshed/SKILL.md"
        );
        assert_eq!(
            global("codex"),
            "/home/agent/.codex/skills/cowshed/SKILL.md"
        );
        // The verified override: not .omp/agent/managed-skills, which omp's
        // auto-learn owns and may rewrite.
        assert_eq!(
            global("omp"),
            "/home/agent/.omp/agent/skills/cowshed/SKILL.md"
        );
        // amp is the shape that breaks naive derivation: its probe directory is
        // not the parent of its skills directory.
        let amp = harness_named(Scope::Global, "amp").expect("amp is a global harness");
        assert_eq!(
            amp.skill_file(base).display().to_string(),
            "/home/agent/.config/agents/skills/cowshed/SKILL.md"
        );
        assert_eq!(
            amp.root(base).display().to_string(),
            "/home/agent/.config/amp"
        );

        let project = Path::new("/repo");
        let omp = harness_named(Scope::Project, "omp").expect("omp is a project harness");
        assert_eq!(
            omp.skill_file(project).display().to_string(),
            "/repo/.omp/skills/cowshed/SKILL.md",
            "omp nests user skills under agent/ only in the home directory"
        );
        assert_eq!(
            omp.root(project).display().to_string(),
            "/repo/.omp",
            "a project probe is the first segment of the skills directory"
        );
    }

    #[test]
    fn a_near_miss_harness_name_suggests_the_real_one() {
        assert_eq!(
            harness_suggestions(Scope::Global, "copilot"),
            ["github-copilot"],
            "a short name inside the input must not drown the real suggestion"
        );
        assert_eq!(
            harness_suggestions(Scope::Global, "claude"),
            ["claude-code"]
        );
        assert!(harness_suggestions(Scope::Global, "zzz").is_empty());
        assert!(harness_suggestions(Scope::Global, "").is_empty());
    }

    #[test]
    fn the_generated_snapshot_parses_into_usable_entries() {
        assert!(
            GENERATED_HARNESSES.len() >= 40,
            "a snapshot this small means the generator stopped parsing upstream"
        );

        let mut names: Vec<&str> = GENERATED_HARNESSES.iter().map(|entry| entry.name).collect();
        names.sort_unstable();
        let unique = names.len();
        names.dedup();
        assert_eq!(names.len(), unique, "snapshot names must be unique");

        for entry in GENERATED_HARNESSES {
            for path in [entry.global_root, entry.global_skills, entry.project_skills] {
                assert!(!path.is_empty(), "{} has an empty path", entry.name);
                assert!(
                    !path.starts_with('/') && !path.split('/').any(|part| part == ".."),
                    "{} escapes its install base via {path}",
                    entry.name
                );
            }
        }
    }

    #[test]
    fn verified_entries_override_the_snapshot_and_appear_exactly_once() {
        let entries = harness_entries();

        for verified in VERIFIED_HARNESSES {
            let matching: Vec<_> = entries
                .iter()
                .filter(|entry| entry.name == verified.entry.name)
                .collect();
            assert_eq!(
                matching.len(),
                1,
                "{} must appear once, not once per source",
                verified.entry.name
            );
            assert_eq!(
                **matching[0], verified.entry,
                "the verified entry must win over the snapshot"
            );
            assert!(
                !verified.reason.is_empty(),
                "an override must carry its evidence"
            );
        }

        // omp is currently the only override precisely because it is absent
        // upstream; if upstream adds it, this is the reminder to re-verify.
        assert!(
            !GENERATED_HARNESSES.iter().any(|entry| entry.name == "omp"),
            "upstream now ships omp: re-verify the override against its binary"
        );
    }

    /// Detection probes the configuration directory, which for omp is `.omp`
    /// even though its skills live two levels deeper.
    #[test]
    fn omp_and_codex_are_detected_from_their_configuration_directories() {
        let base = Path::new("/home/agent");
        let present = |path: &Path| {
            path == Path::new("/home/agent/.codex") || path == Path::new("/home/agent/.omp")
        };

        let planned = plan(base, Scope::Global, &[], &present);

        let names: Vec<&str> = planned.iter().map(|harness| harness.name).collect();
        assert_eq!(
            names,
            ["omp", "codex"],
            "verified entries lead the composed table, then the snapshot in name order"
        );
    }

    #[test]
    fn a_harness_is_addressable_in_both_scopes_and_unknown_names_are_rejected() {
        // Every harness in the snapshot carries both a home and a repository
        // directory, so a name is valid in either scope; only the resolved path
        // differs. Scope no longer gates which names exist.
        for name in ["claude-code", "codex", "omp", "github-copilot", "goose"] {
            assert!(
                harness_named(Scope::Global, name).is_some(),
                "{name} should resolve globally"
            );
            assert!(
                harness_named(Scope::Project, name).is_some(),
                "{name} should resolve per repository"
            );
        }
        assert!(harness_named(Scope::Global, "nonesuch").is_none());
        assert!(harness_named(Scope::Project, "nonesuch").is_none());

        // The usage sample stays readable even though the table is large.
        let names = harness_names(Scope::Global);
        assert!(names.contains("more"), "{names}");
        assert!(
            names.len() < 200,
            "usage sample is too long to read: {names}"
        );
    }

    #[test]
    fn install_writes_once_then_reports_unchanged_without_rewriting() {
        let base = scratch("idempotent");
        let planned = vec![fallback()];

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
        let planned = vec![fallback()];
        let path = fallback().skill_file(&base);
        std::fs::create_dir_all(fallback().destination(&base)).expect("mkdir");
        std::fs::write(&path, "stale").expect("seed");

        let report = install(&base, &planned).expect("install");

        assert_eq!(report.installs[0].status, SkillInstallStatus::Written);
        assert_eq!(std::fs::read_to_string(&path).expect("read"), SKILL_MD);
        std::fs::remove_dir_all(&base).expect("cleanup");
    }
}
