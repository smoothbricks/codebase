use std::ffi::OsString;
use std::path::{Path, PathBuf};

use crate::apfs::{
    CommandOutput, CommandRequest, CommandRunError, CommandRunner, SystemCommandRunner,
};
use crate::error::{CowshedError, Result};

const DEFAULT_PASS_BUDGET: usize = 6;
const CHURN_SAMPLE_LIMIT: usize = 8;
const APFS_ROOT_METADATA_EXCLUDES: [&str; 5] = [
    "--exclude=/.DocumentRevisions-V100",
    "--exclude=/.Spotlight-V100",
    "--exclude=/.TemporaryItems",
    "--exclude=/.Trashes",
    "--exclude=/.fseventsd",
];

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CopyReport {
    pub passes: usize,
    pub changed_entries: usize,
}

/// Which rsync implementation is on PATH.
///
/// The two differ in more than flag spelling. Apple's openrsync preserves
/// extended attributes only under `--extended-attributes`, and under that flag
/// it re-transfers every regular file on every pass — a tree that has not
/// changed still itemizes as `>f+++++++` forever. Its itemized output can
/// therefore never signal quiescence, so the variant decides how convergence is
/// measured, not just which flag is spelled.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RsyncVariant {
    /// Apple openrsync (macOS 15+), which reports protocol version 29.
    Openrsync,
    /// Upstream rsync 3.x, whose itemized output is authoritative.
    Upstream,
}

impl RsyncVariant {
    /// Classify from `rsync --version` output.
    ///
    /// Upstream is the default for unrecognized output: its single-pass
    /// convergence is the cheaper contract, and misclassifying openrsync as
    /// upstream surfaces loudly as a non-quiescing adopt rather than silently
    /// skipping extended attributes.
    #[must_use]
    pub fn classify(version_output: &[u8]) -> Self {
        if String::from_utf8_lossy(version_output).contains("openrsync") {
            Self::Openrsync
        } else {
            Self::Upstream
        }
    }

    /// The flag that preserves extended attributes for this variant.
    #[must_use]
    pub const fn xattr_flag(self) -> &'static str {
        match self {
            Self::Openrsync => "--extended-attributes",
            Self::Upstream => "--xattrs",
        }
    }

    /// Whether a copy pass's own itemized output can decide quiescence.
    ///
    /// False for openrsync, which needs a separate probe because copying with
    /// extended attributes always itemizes every regular file.
    #[must_use]
    pub const fn copy_pass_itemizes_truthfully(self) -> bool {
        matches!(self, Self::Upstream)
    }
}

impl<R> CommandRunner for &R
where
    R: CommandRunner + ?Sized,
{
    fn run(&self, request: &CommandRequest) -> std::result::Result<CommandOutput, CommandRunError> {
        (**self).run(request)
    }
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

    fn detect_variant(&self) -> Result<RsyncVariant> {
        let request = CommandRequest::new("rsync", [OsString::from("--version")]);
        let output = self.runner.run(&request).map_err(copy_spawn_error)?;
        if !output.succeeded() {
            return Err(copy_process_error(&output));
        }
        Ok(RsyncVariant::classify(&output.stdout))
    }

    fn run_rsync(&self, request: &CommandRequest) -> Result<CommandOutput> {
        let output = self.runner.run(request).map_err(copy_spawn_error)?;
        if output.succeeded() {
            Ok(output)
        } else {
            Err(copy_process_error(&output))
        }
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
        let variant = self.detect_variant()?;
        let mut changed_entries = 0usize;
        let mut last_changes = Vec::new();

        for pass in 1..=pass_budget {
            let output = self.run_rsync(&rsync_request(
                RsyncPass::Copy(variant),
                &source_contents,
                &destination_contents,
            ))?;

            let changes = if variant.copy_pass_itemizes_truthfully() {
                parse_changes(&output.stdout)?
            } else {
                // openrsync's copy pass always claims to have transferred every
                // regular file, so quiescence is measured by a probe that omits
                // the extended-attribute flag. The probe transfers nothing when
                // content and metadata already match, and it never strips the
                // attributes the copy pass just wrote.
                let probe = self.run_rsync(&rsync_request(
                    RsyncPass::Probe,
                    &source_contents,
                    &destination_contents,
                ))?;
                parse_changes(&probe.stdout)?
            };

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

/// Which of the two invocations in a pass is being built.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RsyncPass {
    /// Copies, preserving extended attributes with the variant's flag.
    Copy(RsyncVariant),
    /// Measures quiescence only; omits extended attributes so openrsync can
    /// report an unchanged tree as unchanged.
    Probe,
}

fn rsync_request(pass: RsyncPass, source: &Path, destination: &Path) -> CommandRequest {
    let mut args = vec![OsString::from("-a")];
    if let RsyncPass::Copy(variant) = pass {
        args.push(OsString::from(variant.xattr_flag()));
    }
    args.push(OsString::from("--delete"));
    args.push(OsString::from("--itemize-changes"));
    args.extend(
        APFS_ROOT_METADATA_EXCLUDES
            .iter()
            .map(|exclude| OsString::from(*exclude)),
    );
    args.push(OsString::from("--out-format=%i"));
    args.push(OsString::from("--"));
    args.push(source.as_os_str().to_owned());
    args.push(destination.as_os_str().to_owned());
    CommandRequest::new("rsync", args)
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
        format!(
            "cannot execute {}: {}",
            error.program.display(),
            error.source
        ),
        "install rsync, ensure it is available on PATH, then retry cowshed adopt",
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
    use std::sync::Mutex;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{
        CommandOutput, CommandRequest, CommandRunError, CommandRunner, RsyncVariant, TreeCopier,
        copy_with_budget, parse_changes, validate_copy_roots,
    };

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

    /// Records every rsync invocation and replays scripted stdout, so both
    /// variants are exercised on a host that only has one of them installed.
    struct ScriptedRsync {
        version: &'static str,
        /// stdout for each copy/probe invocation, in order.
        transcripts: Mutex<Vec<&'static str>>,
        calls: Mutex<Vec<Vec<String>>>,
    }

    impl ScriptedRsync {
        fn new(version: &'static str, transcripts: Vec<&'static str>) -> Self {
            Self {
                version,
                transcripts: Mutex::new(transcripts),
                calls: Mutex::new(Vec::new()),
            }
        }

        fn calls(&self) -> Vec<Vec<String>> {
            self.calls.lock().expect("calls").clone()
        }
    }

    impl CommandRunner for ScriptedRsync {
        fn run(
            &self,
            request: &CommandRequest,
        ) -> std::result::Result<CommandOutput, CommandRunError> {
            let args: Vec<String> = request
                .args
                .iter()
                .map(|arg| arg.to_string_lossy().into_owned())
                .collect();
            if args == ["--version"] {
                return Ok(CommandOutput::success(self.version));
            }
            self.calls.lock().expect("calls").push(args);
            let mut transcripts = self.transcripts.lock().expect("transcripts");
            assert!(!transcripts.is_empty(), "unexpected extra rsync invocation");
            Ok(CommandOutput::success(transcripts.remove(0)))
        }
    }

    #[cfg(target_os = "macos")]
    const TEST_XATTR: &str = "com.cowshed.test";

    #[cfg(target_os = "macos")]
    fn write_xattr(path: &std::path::Path, name: &str, value: &[u8]) {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt as _;
        let path = CString::new(path.as_os_str().as_bytes()).expect("path");
        let name = CString::new(name).expect("name");
        // SAFETY: both C strings outlive the call, and the value slice is passed
        // with its own length.
        let written = unsafe {
            libc::setxattr(
                path.as_ptr(),
                name.as_ptr(),
                value.as_ptr().cast(),
                value.len(),
                0,
                0,
            )
        };
        assert_eq!(written, 0, "setxattr: {}", std::io::Error::last_os_error());
    }

    #[cfg(target_os = "macos")]
    fn read_xattr(path: &std::path::Path, name: &str) -> Option<Vec<u8>> {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt as _;
        let path = CString::new(path.as_os_str().as_bytes()).expect("path");
        let name = CString::new(name).expect("name");
        let mut buffer = vec![0u8; 64];
        // SAFETY: the buffer is valid for `buffer.len()` bytes for the call.
        let read = unsafe {
            libc::getxattr(
                path.as_ptr(),
                name.as_ptr(),
                buffer.as_mut_ptr().cast(),
                buffer.len(),
                0,
                0,
            )
        };
        if read < 0 {
            return None;
        }
        buffer.truncate(usize::try_from(read).expect("non-negative length"));
        Some(buffer)
    }

    fn copy_roots(label: &str) -> (PathBuf, PathBuf, PathBuf) {
        let root = temp_root(label);
        let source = root.join("source");
        let destination = root.join("destination");
        fs::create_dir(&source).expect("create source");
        fs::create_dir(&destination).expect("create destination");
        (root, source, destination)
    }

    #[test]
    fn variant_classification_keys_off_openrsync_and_defaults_to_upstream() {
        assert_eq!(
            RsyncVariant::classify(
                b"openrsync: protocol version 29\nrsync version 2.6.9 compatible\n"
            ),
            RsyncVariant::Openrsync
        );
        assert_eq!(
            RsyncVariant::classify(b"rsync  version 3.4.1  protocol version 32\n"),
            RsyncVariant::Upstream
        );
        assert_eq!(
            RsyncVariant::classify(b""),
            RsyncVariant::Upstream,
            "unrecognized output defaults to the cheaper single-pass contract"
        );

        assert_eq!(
            RsyncVariant::Openrsync.xattr_flag(),
            "--extended-attributes"
        );
        assert_eq!(RsyncVariant::Upstream.xattr_flag(), "--xattrs");
        assert!(RsyncVariant::Upstream.copy_pass_itemizes_truthfully());
        assert!(!RsyncVariant::Openrsync.copy_pass_itemizes_truthfully());
    }

    #[test]
    fn upstream_rsync_decides_quiescence_from_the_copy_pass_alone() {
        let (root, source, destination) = copy_roots("upstream-arm");
        let runner = ScriptedRsync::new(
            "rsync  version 3.4.1  protocol version 32\n",
            vec![">f+++++++++\ncd+++++++++\n", ""],
        );

        let report = TreeCopier::new(&runner)
            .copy_with_budget(&source, &destination, 4)
            .expect("upstream converges");

        assert_eq!(report.passes, 2);
        assert_eq!(report.changed_entries, 2);
        let calls = runner.calls();
        assert_eq!(calls.len(), 2, "upstream runs one invocation per pass");
        for call in &calls {
            assert!(
                call.contains(&"--xattrs".to_owned()),
                "every upstream pass preserves extended attributes: {call:?}"
            );
        }
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[test]
    fn openrsync_measures_quiescence_with_a_probe_that_omits_extended_attributes() {
        let (root, source, destination) = copy_roots("openrsync-arm");
        // The copy pass keeps claiming every regular file was transferred; only
        // the probe reports the tree as settled.
        let runner = ScriptedRsync::new(
            "openrsync: protocol version 29\n",
            vec![">f+++++++\n>f+++++++\n", ">f+++++++\n", ">f+++++++\n", ""],
        );

        let report = TreeCopier::new(&runner)
            .copy_with_budget(&source, &destination, 4)
            .expect("openrsync converges on the probe");

        assert_eq!(report.passes, 2, "the second probe reports a settled tree");
        let calls = runner.calls();
        assert_eq!(calls.len(), 4, "each pass is one copy plus one probe");
        for (index, call) in calls.iter().enumerate() {
            let is_copy = index % 2 == 0;
            assert_eq!(
                call.contains(&"--extended-attributes".to_owned()),
                is_copy,
                "copy passes preserve attributes, probes must not: {call:?}"
            );
            assert!(call.contains(&"--itemize-changes".to_owned()));
            assert!(call.contains(&"--delete".to_owned()));
        }
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[test]
    fn a_tree_that_never_settles_is_a_conflict_naming_the_churn() {
        let (root, source, destination) = copy_roots("never-settles");
        let runner = ScriptedRsync::new(
            "openrsync: protocol version 29\n",
            vec![">f+++++++\n", ">f+++++++\n", ">f+++++++\n", ">f+++++++\n"],
        );

        let error = TreeCopier::new(&runner)
            .copy_with_budget(&source, &destination, 2)
            .expect_err("a churning tree cannot be adopted");

        assert_eq!(error.code.as_str(), "conflict");
        assert!(
            error
                .message
                .contains("did not quiesce after 2 copy passes")
        );
        assert!(error.message.contains(">f+++++++"));
        fs::remove_dir_all(root).expect("remove fixture");
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
        #[cfg(target_os = "macos")]
        write_xattr(&source.join("nested/file"), TEST_XATTR, b"warm");
        fs::write(destination.join("stale-secret"), b"remove me\n").expect("write stale file");
        let apfs_metadata = destination.join(".fseventsd");
        fs::create_dir(&apfs_metadata).expect("create destination APFS metadata");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&apfs_metadata, fs::Permissions::from_mode(0o000))
                .expect("make destination APFS metadata unreadable");
        }

        let report = copy_with_budget(&source, &destination, 3)
            .await
            .expect("copy reaches quiescence");
        assert!(report.passes >= 1 && report.passes <= 3);
        assert_eq!(
            fs::read(destination.join("nested/file")).expect("read copied file"),
            b"warm state\n"
        );
        assert_eq!(
            fs::read(destination.join(".git/HEAD")).expect("read copied git metadata"),
            b"ref: refs/heads/main\n"
        );
        assert!(!destination.join("stale-secret").exists());

        // Preserving extended attributes is the only reason the copy pass needs
        // a variant-specific flag, so the fix for quiescence must not quietly
        // stop carrying them.
        #[cfg(target_os = "macos")]
        assert_eq!(
            read_xattr(&destination.join("nested/file"), TEST_XATTR).as_deref(),
            Some(&b"warm"[..]),
            "the copy pass carries extended attributes"
        );

        // Quiescence must be a property of the tree, not of how many passes ran:
        // copying an already-synced pair converges immediately having moved
        // nothing. Under openrsync this is the assertion that fails when the
        // copy pass's own itemized output is trusted, because it reports every
        // regular file as freshly transferred forever.
        let repeat = copy_with_budget(&source, &destination, 3)
            .await
            .expect("an already-synced tree is quiescent");
        assert_eq!(repeat.passes, 1);
        assert_eq!(repeat.changed_entries, 0);

        assert!(apfs_metadata.is_dir());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&apfs_metadata, fs::Permissions::from_mode(0o700))
                .expect("restore destination APFS metadata permissions");
        }
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
