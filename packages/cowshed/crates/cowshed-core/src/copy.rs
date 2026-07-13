use std::ffi::OsString;
use std::path::{Path, PathBuf};

use crate::apfs::{
    CommandOutput, CommandRequest, CommandRunError, CommandRunner, SystemCommandRunner,
};
use crate::error::{CowshedError, Result};

const RSYNC: &str = "/usr/bin/rsync";
const DEFAULT_PASS_BUDGET: usize = 6;
const CHURN_SAMPLE_LIMIT: usize = 8;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CopyReport {
    pub passes: usize,
    pub changed_entries: usize,
}

#[derive(Clone, Debug)]
pub struct TreeCopier<R> {
    runner: R,
}

impl<R> TreeCopier<R>
where
    R: CommandRunner,
{
    pub const fn new(runner: R) -> Self {
        Self { runner }
    }

    pub fn copy_until_quiescent(&self, source: &Path, destination: &Path) -> Result<CopyReport> {
        self.copy_with_budget(source, destination, DEFAULT_PASS_BUDGET)
    }

    pub fn copy_with_budget(
        &self,
        source: &Path,
        destination: &Path,
        pass_budget: usize,
    ) -> Result<CopyReport> {
        if pass_budget == 0 {
            return Err(CowshedError::usage(
                "copy pass budget must be positive",
                "retry cowshed adopt without overriding the pass budget",
            ));
        }
        let (source, destination) = validate_copy_roots(source, destination)?;
        let source_contents = source.join(".");
        let destination_contents = destination.join(".");
        let mut changed_entries = 0usize;
        let mut last_changes = Vec::new();

        for pass in 1..=pass_budget {
            let request = CommandRequest::new(
                RSYNC,
                [
                    OsString::from("-aE"),
                    OsString::from("--delete"),
                    OsString::from("--itemize-changes"),
                    OsString::from("--out-format=%i"),
                    OsString::from("--"),
                    source_contents.as_os_str().to_owned(),
                    destination_contents.as_os_str().to_owned(),
                ],
            );
            let output = self.runner.run(&request).map_err(copy_spawn_error)?;
            if !output.succeeded() {
                return Err(copy_process_error(&output));
            }
            let changes = parse_changes(&output.stdout)?;
            if changes.is_empty() {
                return Ok(CopyReport {
                    passes: pass,
                    changed_entries,
                });
            }
            changed_entries = changed_entries.saturating_add(changes.len());
            last_changes = changes;
        }

        let sample = last_changes
            .iter()
            .take(CHURN_SAMPLE_LIMIT)
            .map(String::as_str)
            .collect::<Vec<_>>()
            .join(", ");
        Err(CowshedError::conflict(
            format!(
                "repository did not quiesce after {pass_budget} copy passes; recent change kinds: {sample}"
            ),
            "stop repository writers and retry cowshed adopt",
        ))
    }
}

pub fn copy_until_quiescent_blocking(source: &Path, destination: &Path) -> Result<CopyReport> {
    TreeCopier::new(SystemCommandRunner).copy_until_quiescent(source, destination)
}

/// Copy a live repository into an attached image until a complete delta pass observes no changes.
pub async fn copy_until_quiescent(source: &Path, destination: &Path) -> Result<CopyReport> {
    copy_with_budget(source, destination, DEFAULT_PASS_BUDGET).await
}

pub async fn copy_with_budget(
    source: &Path,
    destination: &Path,
    pass_budget: usize,
) -> Result<CopyReport> {
    let source = source.to_owned();
    let destination = destination.to_owned();
    tokio::task::spawn_blocking(move || {
        TreeCopier::new(SystemCommandRunner).copy_with_budget(&source, &destination, pass_budget)
    })
    .await
    .map_err(|error| CowshedError::internal(format!("repository copy worker failed: {error}")))?
}

fn validate_copy_roots(source: &Path, destination: &Path) -> Result<(PathBuf, PathBuf)> {
    let source = source.canonicalize().map_err(|error| {
        CowshedError::not_found(
            format!("cannot open source tree {}: {error}", source.display()),
            "cowshed adopt <existing-git-root>",
        )
    })?;
    let destination = destination.canonicalize().map_err(|error| {
        CowshedError::environment_missing(
            format!("cannot open image mount {}: {error}", destination.display()),
            "cowshed doctor --json",
        )
    })?;

    if !source.is_dir() || !destination.is_dir() {
        return Err(CowshedError::usage(
            "adopt source and image destination must both be directories",
            "cowshed adopt <git-root>",
        ));
    }
    if destination.starts_with(&source) || source.starts_with(&destination) {
        return Err(CowshedError::conflict(
            "adopt copy roots overlap",
            "choose a cowshed store outside the repository tree",
        ));
    }
    Ok((source, destination))
}

fn parse_changes(stdout: &[u8]) -> Result<Vec<String>> {
    let text = std::str::from_utf8(stdout)
        .map_err(|_| CowshedError::internal("rsync emitted a non-UTF-8 change report"))?;
    Ok(text
        .lines()
        .filter(|line| !line.is_empty())
        .map(sanitize_change_kind)
        .collect())
}

fn sanitize_change_kind(kind: &str) -> String {
    kind.bytes()
        .take(12)
        .map(|byte| match byte {
            b' '..=b'~' => char::from(byte),
            _ => '�',
        })
        .collect()
}
fn copy_spawn_error(error: CommandRunError) -> CowshedError {
    CowshedError::environment_missing(
        format!("cannot execute {RSYNC}: {error}"),
        "install the macOS base system tools, then retry cowshed adopt",
    )
}

fn copy_process_error(output: &CommandOutput) -> CowshedError {
    let reason = String::from_utf8_lossy(&output.stderr);
    let reason = reason.trim();
    let status = output.status.to_string();
    let message = if reason.is_empty() {
        format!("repository copy failed with status {status}")
    } else {
        format!("repository copy failed with status {status}: {reason}")
    };
    CowshedError::conflict(
        message,
        "resolve the filesystem error and retry cowshed adopt",
    )
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{copy_with_budget, parse_changes, validate_copy_roots};

    fn temp_root(label: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "cowshed-copy-{label}-{}-{suffix}",
            std::process::id()
        ));
        fs::create_dir_all(&root).expect("create fixture root");
        root
    }

    #[test]
    fn parses_and_sanitizes_change_kinds_without_paths() {
        let changes = parse_changes(b">f+++++++++\n.d..t......\n").expect("parse changes");
        assert_eq!(changes, [">f+++++++++", ".d..t......"]);
    }

    #[tokio::test]
    async fn copies_complete_tree_deletes_stale_entries_and_reaches_quiescence() {
        let source = temp_root("source");
        let destination = temp_root("destination");
        fs::create_dir_all(source.join(".git/objects")).expect("create source git directory");
        fs::create_dir(source.join("nested")).expect("create source directory");
        fs::write(source.join(".git/HEAD"), b"ref: refs/heads/main\n")
            .expect("write source git metadata");
        fs::write(source.join("nested/file"), b"warm state\n").expect("write source file");
        fs::write(destination.join("stale-secret"), b"remove me\n").expect("write stale file");

        let report = copy_with_budget(&source, &destination, 3)
            .await
            .expect("copy reaches quiescence");
        assert!(report.passes >= 2);
        assert_eq!(
            fs::read(destination.join("nested/file")).expect("read copied file"),
            b"warm state\n"
        );
        assert_eq!(
            fs::read(destination.join(".git/HEAD")).expect("read copied git metadata"),
            b"ref: refs/heads/main\n"
        );
        assert!(!destination.join("stale-secret").exists());

        fs::remove_dir_all(source).expect("remove source");
        fs::remove_dir_all(destination).expect("remove destination");
    }

    #[test]
    fn rejects_each_overlapping_root_boundary_and_accepts_disjoint_roots() {
        let root = temp_root("root-boundaries");
        let source = root.join("source");
        let source_child = source.join("child");
        let destination = root.join("destination");
        fs::create_dir(&source).expect("create source");
        fs::create_dir(&source_child).expect("create source child");
        fs::create_dir(&destination).expect("create destination");

        for (candidate_source, candidate_destination) in [
            (&source, &source),
            (&source, &source_child),
            (&source_child, &source),
        ] {
            let error = validate_copy_roots(candidate_source, candidate_destination)
                .expect_err("overlapping roots must fail");
            assert_eq!(error.code.as_str(), "conflict");
        }

        let (canonical_source, canonical_destination) =
            validate_copy_roots(&source, &destination).expect("siblings are disjoint");
        assert_eq!(
            canonical_source,
            source.canonicalize().expect("source root")
        );
        assert_eq!(
            canonical_destination,
            destination.canonicalize().expect("destination root")
        );
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[test]
    fn rejects_either_copy_root_when_it_is_not_a_directory() {
        let root = temp_root("file-boundaries");
        let directory = root.join("directory");
        let file = root.join("file");
        fs::create_dir(&directory).expect("create directory");
        fs::write(&file, b"not a directory").expect("create file");

        for (candidate_source, candidate_destination) in [(&file, &directory), (&directory, &file)]
        {
            let error = validate_copy_roots(candidate_source, candidate_destination)
                .expect_err("both roots must be directories");
            assert_eq!(error.code.as_str(), "usage");
        }
        fs::remove_dir_all(root).expect("remove fixture");
    }
}
