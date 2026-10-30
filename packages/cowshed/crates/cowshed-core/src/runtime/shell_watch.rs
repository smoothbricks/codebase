//! Freshness of an activated workspace shell, judged by the inputs direnv itself recorded.
//!
//! direnv has no watcher. At load it records every file the evaluation depended on — the
//! `.envrc`, its allow/deny files, every `source_up`/`watch_file` target, devenv's inputs — as
//! `DIRENV_WATCHES`: base64url(zlib(JSON `[{path, modtime, exists}]`)). Each later `direnv
//! export` stats that list and reloads on a mismatch. A pooled shell keeps the same contract
//! without paying a direnv process per command: the pool decodes the list once per activation,
//! subscribes those exact paths with the kernel, and compares exact stat identities in process.
//!
//! direnv's own `modtime` is whole seconds, so two writes inside one second are invisible to
//! it. It is never compared here; only its `exists` bit, which is exact, is.
//!
//! A file's timestamps come from the kernel's stamping clocks at its filesystem's granularity,
//! not from the process clock: Linux without multigrain timestamps stamps from a clock up to a
//! scheduler tick behind `SystemTime::now()`, and HFS+ keeps whole seconds. So a file written
//! after a `now()` reading can carry an earlier ctime, and nothing here compares a timestamp
//! with the process clock. Nor need one filesystem stamp every kind of change from one clock.
//! When an activation starts is read off the filesystem's own clocks ([`FsSeparation`]).

use std::collections::BTreeMap;
use std::io::{self, Write as _};
use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};
use std::time::Duration;

use base64::Engine as _;

/// More entries than any real activation records (a devenv project records ~60-70). A list
/// beyond it cannot be watched without exhausting descriptors, so it is refused rather than
/// partially watched: a shell whose inputs are only partially observed is never reused.
pub const MAX_WATCHED_PATHS: usize = 4096;

/// Bound on the inflated JSON, so a hostile `.envrc` cannot make the supervisor allocate
/// without limit. 4096 entries of a maximal path are far below it.
const MAX_WATCH_LIST_BYTES: usize = 16 * 1024 * 1024;

/// One input direnv recorded for an activation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WatchEntry {
    pub path: PathBuf,
    /// Whether the input existed when direnv recorded it.
    pub exists: bool,
}

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
pub enum WatchListError {
    #[error("DIRENV_WATCHES is not base64url: {0}")]
    Encoding(String),
    #[error("DIRENV_WATCHES does not inflate as zlib: {0}")]
    Compression(String),
    #[error("DIRENV_WATCHES is not direnv's watch-list JSON: {0}")]
    Json(String),
    #[error("DIRENV_WATCHES names {0} inputs, above the {MAX_WATCHED_PATHS} a shell can watch")]
    TooMany(usize),
    #[error("DIRENV_WATCHES names a relative path {0:?}")]
    RelativePath(PathBuf),
}

#[derive(serde::Deserialize)]
struct RecordedInput {
    path: PathBuf,
    exists: bool,
}

/// Decode direnv's `DIRENV_WATCHES` value into the inputs it names, in direnv's order with
/// duplicates removed.
pub fn decode_direnv_watches(encoded: &str) -> Result<Vec<WatchEntry>, WatchListError> {
    let engine = base64::engine::GeneralPurpose::new(
        &base64::alphabet::URL_SAFE,
        base64::engine::GeneralPurposeConfig::new()
            .with_decode_padding_mode(base64::engine::DecodePaddingMode::Indifferent),
    );
    let compressed = engine
        .decode(encoded.trim())
        .map_err(|error| WatchListError::Encoding(error.to_string()))?;
    let json =
        miniz_oxide::inflate::decompress_to_vec_zlib_with_limit(&compressed, MAX_WATCH_LIST_BYTES)
            .map_err(|error| WatchListError::Compression(error.to_string()))?;
    let recorded: Vec<RecordedInput> =
        serde_json::from_slice(&json).map_err(|error| WatchListError::Json(error.to_string()))?;
    if recorded.len() > MAX_WATCHED_PATHS {
        return Err(WatchListError::TooMany(recorded.len()));
    }
    let mut seen = std::collections::BTreeSet::new();
    let mut entries = Vec::with_capacity(recorded.len());
    for input in recorded {
        if !input.path.is_absolute() {
            return Err(WatchListError::RelativePath(input.path));
        }
        if seen.insert(input.path.clone()) {
            entries.push(WatchEntry {
                path: input.path,
                exists: input.exists,
            });
        }
    }
    Ok(entries)
}

/// Encode a watch list as direnv does for `DIRENV_WATCHES`: each input's whole-second
/// modification time and existence as `snapshot` read them (0 and absent for an input it does
/// not hold or could not stat), as a fresh `direnv export` would record them. The inverse of
/// [`decode_direnv_watches`]; an inherited shell sets it for its own checkout from the same
/// reading its identities come from.
pub fn encode_direnv_watches(entries: &[WatchEntry], snapshot: &Snapshot) -> String {
    #[derive(serde::Serialize)]
    struct Recorded<'a> {
        path: &'a Path,
        modtime: i64,
        exists: bool,
    }
    let recorded: Vec<Recorded<'_>> = entries
        .iter()
        .map(|entry| match snapshot.get(&entry.path) {
            Some(FileState::Present { mtime_ns, .. }) => Recorded {
                path: &entry.path,
                // Nanoseconds built from i64 seconds divide back into i64 seconds.
                modtime: i64::try_from(mtime_ns.div_euclid(1_000_000_000))
                    .expect("whole seconds of an i64-second timestamp fit in i64"),
                exists: true,
            },
            Some(FileState::Missing | FileState::Unreadable(_)) | None => Recorded {
                path: &entry.path,
                modtime: 0,
                exists: false,
            },
        })
        .collect();
    let json = serde_json::to_vec(&recorded).expect("a watch list serializes");
    base64::engine::general_purpose::URL_SAFE
        .encode(miniz_oxide::deflate::compress_to_vec_zlib(&json, 6))
}

/// direnv's `DIRENV_DIFF` for an environment moving from `before` to `after`: its `EnvDiff`
/// (`p`, the previous values of every changed variable; `n`, the new ones) as base64url(zlib
/// (JSON)), skipping the variables direnv lets change freely. An adopted shell sets it so a
/// direnv run inside one of its commands unloads it exactly as it would an evaluated one.
pub fn encode_direnv_diff(
    before: &BTreeMap<std::ffi::OsString, std::ffi::OsString>,
    after: &BTreeMap<std::ffi::OsString, std::ffi::OsString>,
) -> String {
    fn ignored(name: &str) -> bool {
        name.starts_with("__fish")
            || name.starts_with("BASH_FUNC_")
            || matches!(
                name,
                "COMP_WORDBREAKS"
                    | "PS1"
                    | "OLDPWD"
                    | "PWD"
                    | "SHELL"
                    | "SHELLOPTS"
                    | "SHLVL"
                    | "_"
                    | "DIRENV_DIFF"
            )
    }
    type Environment = BTreeMap<std::ffi::OsString, std::ffi::OsString>;
    fn changed<'a>(from: &'a Environment, to: &Environment) -> BTreeMap<&'a str, &'a str> {
        from.iter()
            .filter_map(|(name, value)| Some((name.to_str()?, value.to_str()?)))
            .filter(|(name, value)| {
                !ignored(name)
                    && to.get(std::ffi::OsStr::new(name)).and_then(|v| v.to_str()) != Some(*value)
            })
            .collect()
    }
    let diff = serde_json::json!({ "p": changed(before, after), "n": changed(after, before) });
    let json = serde_json::to_vec(&diff).expect("an environment diff serializes");
    base64::engine::general_purpose::URL_SAFE
        .encode(miniz_oxide::deflate::compress_to_vec_zlib(&json, 6))
}

/// The exact identity of one watched path, following symlinks as direnv does.
///
/// `(ino, size, mtime, ctime)` at the filesystem's timestamp resolution: an atomic replace
/// changes the inode, and ctime cannot be set by any user tool, so a content change that
/// restores mtime still shows. Two same-size writes inside one timestamp tick (a coarse Linux
/// clock tick, an HFS+ second) share an identity; the kernel subscription reports those.
///
/// `dev` is kept for ordering a ctime against a filesystem's clocks ([`FsSeparation`]), never
/// for identity ([`FileState::same_as`]): it numbers the mount, not the file. A clone of a
/// volume keeps every file's inode, size, mtime and ctime and mounts under another device, and
/// a file whose four fields all match is that same file state.
#[derive(Clone, Copy, Debug)]
pub enum FileState {
    Missing,
    Present {
        dev: u64,
        ino: u64,
        size: u64,
        mtime_ns: i128,
        ctime_ns: i128,
    },
    /// The path exists but cannot be inspected; the errno is part of the identity.
    Unreadable(i32),
}

impl FileState {
    pub fn of(path: &Path) -> Self {
        match std::fs::metadata(path) {
            Ok(metadata) => Self::Present {
                dev: metadata.dev(),
                ino: metadata.ino(),
                size: metadata.size(),
                mtime_ns: nanoseconds(metadata.mtime(), metadata.mtime_nsec()),
                ctime_ns: nanoseconds(metadata.ctime(), metadata.ctime_nsec()),
            },
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::NotFound | io::ErrorKind::NotADirectory
                ) =>
            {
                Self::Missing
            }
            Err(error) => Self::Unreadable(error.raw_os_error().unwrap_or(0)),
        }
    }

    /// [`Self::of`] of the entry at `path` itself, a link not followed: a directory moved away
    /// and replaced by a link to it keeps the identity [`Self::of`] reads, not this one.
    pub fn of_entry(path: &Path) -> Self {
        match std::fs::symlink_metadata(path) {
            Ok(metadata) => Self::Present {
                dev: metadata.dev(),
                ino: metadata.ino(),
                size: metadata.size(),
                mtime_ns: nanoseconds(metadata.mtime(), metadata.mtime_nsec()),
                ctime_ns: nanoseconds(metadata.ctime(), metadata.ctime_nsec()),
            },
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::NotFound | io::ErrorKind::NotADirectory
                ) =>
            {
                Self::Missing
            }
            Err(error) => Self::Unreadable(error.raw_os_error().unwrap_or(0)),
        }
    }

    fn exists(self) -> bool {
        !matches!(self, Self::Missing)
    }

    /// Whether `self` and `other` are one state of one file: equal inode, size, mtime and
    /// ctime, or both missing, or both unreadable with one errno. The device is not compared.
    pub fn same_as(self, other: Self) -> bool {
        match (self, other) {
            (Self::Missing, Self::Missing) => true,
            (
                Self::Present {
                    ino,
                    size,
                    mtime_ns,
                    ctime_ns,
                    dev: _,
                },
                Self::Present {
                    ino: other_ino,
                    size: other_size,
                    mtime_ns: other_mtime_ns,
                    ctime_ns: other_ctime_ns,
                    dev: _,
                },
            ) => {
                (ino, size, mtime_ns, ctime_ns)
                    == (other_ino, other_size, other_mtime_ns, other_ctime_ns)
            }
            (Self::Unreadable(errno), Self::Unreadable(other_errno)) => errno == other_errno,
            (Self::Missing | Self::Present { .. } | Self::Unreadable(_), _) => false,
        }
    }
}

fn nanoseconds(seconds: i64, nanoseconds: i64) -> i128 {
    i128::from(seconds) * 1_000_000_000 + i128::from(nanoseconds)
}

/// Longest wait for a filesystem's clocks to move past a probe: a stamping tick where
/// timestamps are fine or tick-grained, one second where they are whole seconds.
const CLOCK_ADVANCE_BOUND: Duration = Duration::from_secs(3);

const NANOS_PER_SECOND: i128 = 1_000_000_000;

/// The ctimes one file in a directory was stamped with by its creation, by a write to it and
/// by a change to its attributes.
///
/// Each kind of change takes its ctime from whichever clock its filesystem stamps that kind
/// with, and they need not be one clock. ZFS on Linux stamps writes from the kernel's coarse
/// clock, set once per scheduler tick, and attribute changes (and a creation, under POSIX
/// ACLs) from the VFS clock, which on a kernel with multigrain timestamps (6.13 and later) never
/// reads below the latest fine-grained stamp any multigrain filesystem on the host took. So a
/// creation can stamp most of a tick later than a write that follows it. The earliest of the
/// three is the slowest clock's reading, the latest the fastest's.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Probe {
    dev: u64,
    created_ns: i128,
    written_ns: i128,
    attributed_ns: i128,
}

impl Probe {
    /// Create a file in `directory`, write to it, change its mode, keeping the ctime after
    /// each, and remove it.
    fn take(directory: &Path) -> io::Result<Self> {
        let path = directory.join(format!(".clock-{}", uuid::Uuid::new_v4().simple()));
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)?;
        let probe = Self::stamp(&file);
        let removed = std::fs::remove_file(&path);
        let probe = probe?;
        removed?;
        Ok(probe)
    }

    fn stamp(mut file: &std::fs::File) -> io::Result<Self> {
        let created = file.metadata()?;
        file.write_all(b"c")?;
        let written = file.metadata()?;
        // Flipping owner-write changes the mode whatever the umask left at creation.
        file.set_permissions(std::fs::Permissions::from_mode(
            written.permissions().mode() ^ 0o200,
        ))?;
        let attributed = file.metadata()?;
        let ctime =
            |metadata: &std::fs::Metadata| nanoseconds(metadata.ctime(), metadata.ctime_nsec());
        Ok(Self {
            dev: created.dev(),
            created_ns: ctime(&created),
            written_ns: ctime(&written),
            attributed_ns: ctime(&attributed),
        })
    }

    fn earliest(self) -> i128 {
        self.created_ns.min(self.written_ns).min(self.attributed_ns)
    }

    fn latest(self) -> i128 {
        self.created_ns.max(self.written_ns).max(self.attributed_ns)
    }
}

/// A reading of one filesystem's slowest stamping clock.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FsInstant {
    dev: u64,
    ctime_ns: i128,
}

impl FsInstant {
    /// The slowest clock in `directory` now: the earliest stamp of one [`Probe`]. No later
    /// change stamps earlier unless that clock is stepped back.
    pub fn read(directory: &Path) -> io::Result<Self> {
        let probe = Probe::take(directory)?;
        Ok(Self {
            dev: probe.dev,
            ctime_ns: probe.earliest(),
        })
    }
}

/// One filesystem's clocks read across a call, separating every change completed there
/// before the call from every change after it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FsSeparation {
    dev: u64,
    /// The latest stamp of the call's first probe: no change completed before the call stamps
    /// later.
    last_before_ns: i128,
    /// The earliest stamp of the first later probe whose every stamp is later than
    /// `last_before_ns`: no change after the call stamps earlier unless a clock is stepped
    /// back.
    crossed_ns: i128,
}

impl FsSeparation {
    /// Probe `directory` until its slowest clock has passed its fastest clock's first reading.
    /// Takes at most one stamping tick of the filesystem: none where timestamps are
    /// fine-grained and one clock stamps everything.
    pub fn take(directory: &Path) -> io::Result<Self> {
        let first = Probe::take(directory)?;
        let deadline = std::time::Instant::now() + CLOCK_ADVANCE_BOUND;
        Self::across(
            first,
            || Probe::take(directory),
            || {
                if std::time::Instant::now() < deadline {
                    return Ok(());
                }
                Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!(
                        "the filesystem clock under {} did not advance in {CLOCK_ADVANCE_BOUND:?}",
                        directory.display()
                    ),
                ))
            },
        )
    }

    /// The separation `first` and the probes after it prove: the first later probe whose
    /// earliest stamp is later than `first`'s latest. `within_bound` is asked before each
    /// further probe.
    fn across(
        first: Probe,
        mut next: impl FnMut() -> io::Result<Probe>,
        mut within_bound: impl FnMut() -> io::Result<()>,
    ) -> io::Result<Self> {
        let last_before_ns = first.latest();
        loop {
            let probe = next()?;
            if probe.dev != first.dev {
                return Err(io::Error::other(format!(
                    "the clock directory moved from device {} to {} while it was probed",
                    first.dev, probe.dev
                )));
            }
            let crossed_ns = probe.earliest();
            if crossed_ns > last_before_ns {
                return Ok(Self {
                    dev: first.dev,
                    last_before_ns,
                    crossed_ns,
                });
            }
            within_bound()?;
        }
    }

    /// Whether a file on `dev` whose ctime is `ctime_ns` last changed before this separation.
    ///
    /// On the separation's own filesystem a ctime no later than `last_before_ns` is before it,
    /// and a ctime between `last_before_ns` and `crossed_ns` is not proven before. A change made
    /// while the call probed has no causal order against it. Another filesystem may stamp more
    /// coarsely, down to whole seconds, which truncates a later change to before the reading
    /// within its second; there only a ctime before the second of `last_before_ns` is earlier.
    fn precedes(self, dev: u64, ctime_ns: i128) -> bool {
        if dev == self.dev {
            ctime_ns <= self.last_before_ns
        } else {
            ctime_ns < self.last_before_ns - self.last_before_ns.rem_euclid(NANOS_PER_SECOND)
        }
    }
}

/// The workspace filesystem's clocks, read on both sides of one evaluation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EvaluationClock {
    /// An [`FsSeparation::take`] after approval and before `direnv export` ran.
    pub started: FsSeparation,
    /// An [`FsInstant::read`] taken once the evaluation's inputs were snapshotted.
    pub finished: FsInstant,
}

impl EvaluationClock {
    /// Whether a file on `dev` whose ctime is `ctime_ns` last changed before the evaluation
    /// began.
    ///
    /// Ordering a ctime against the start assumes the clock that stamped it did not run
    /// backwards meanwhile. A wall clock can be stepped back — a VM's time sync, an NTP step —
    /// and then a change made during the evaluation stamps earlier than the start. A slowest
    /// clock that reads earlier at the end than at the start's crossing proves the clock ran
    /// backwards, so nothing first seen in it is provably older than it: every such input
    /// counts as changed, which costs one more activation.
    ///
    /// An end reading at or after the crossing does not rule a step back out. Stamps are the
    /// only clock a filesystem offers, and a clock stepped back and then forward again past the
    /// crossing reads exactly like one that ran forward, was slewed, or stamps coarsely; a
    /// monotonic reading beside it cannot tell them apart either, because slewing alone moves
    /// the wall clock against it. So an input first listed by this activation and written
    /// while the clock was stepped back can still be judged older, and its shell reused until
    /// that input changes again.
    fn precedes_start(self, dev: u64, ctime_ns: i128) -> bool {
        self.ran_forward() && self.started.precedes(dev, ctime_ns)
    }

    fn ran_forward(self) -> bool {
        self.started.dev == self.finished.dev && self.started.crossed_ns <= self.finished.ctime_ns
    }
}

/// Stat identities of a set of paths at one moment.
#[derive(Clone, Debug, Default)]
pub struct Snapshot(BTreeMap<PathBuf, FileState>);

impl Snapshot {
    pub fn take<'a>(paths: impl IntoIterator<Item = &'a Path>) -> Self {
        Self(
            paths
                .into_iter()
                .map(|path| (path.to_path_buf(), FileState::of(path)))
                .collect(),
        )
    }

    /// Whether any path's identity now differs from this snapshot.
    pub fn changed_since(&self) -> bool {
        self.0
            .iter()
            .any(|(path, state)| !FileState::of(path).same_as(*state))
    }

    /// Whether `other` names the same paths, each in the same file state
    /// ([`FileState::same_as`]).
    pub fn same_as(&self, other: &Self) -> bool {
        self.0.len() == other.0.len()
            && self
                .0
                .iter()
                .zip(&other.0)
                .all(|((path, state), (other_path, other_state))| {
                    path == other_path && state.same_as(*other_state)
                })
    }

    pub fn paths(&self) -> impl Iterator<Item = &Path> + Clone {
        self.0.keys().map(PathBuf::as_path)
    }

    fn get(&self, path: &Path) -> Option<FileState> {
        self.0.get(path).copied()
    }

    /// Whether `path` existed when this snapshot read it; `false` for a path it does not hold.
    pub fn exists(&self, path: &Path) -> bool {
        self.get(path).is_some_and(FileState::exists)
    }
}

/// What one input holds, for deciding whether a shell's inputs really changed: an identity
/// moves on a rewrite of the same bytes (a checkout, a stash, an editor's save), and a byte that
/// did not change cannot change the shell.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Content {
    Missing,
    /// A directory direnv watches (`watch_dir` records each directory under the one named, and
    /// every file): SHA-256 of its sorted entry names. An entry added or removed is a change;
    /// the files' own bytes are inputs of their own.
    Directory([u8; 32]),
    File([u8; 32]),
}

impl Content {
    pub fn of(path: &Path) -> io::Result<Self> {
        use sha2::Digest as _;
        use std::io::Read as _;
        let mut file = match std::fs::File::open(path) {
            Ok(file) => file,
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::NotFound | io::ErrorKind::NotADirectory
                ) =>
            {
                return Ok(Self::Missing);
            }
            Err(error) => return Err(error),
        };
        if file.metadata()?.is_dir() {
            let mut names = std::fs::read_dir(path)?
                .map(|entry| entry.map(|entry| entry.file_name()))
                .collect::<io::Result<Vec<_>>>()?;
            names.sort();
            let mut hasher = sha2::Sha256::new();
            for name in names {
                hasher.update(name.as_encoded_bytes());
                hasher.update([0]);
            }
            return Ok(Self::Directory(hasher.finalize().into()));
        }
        let mut hasher = sha2::Sha256::new();
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
        }
        Ok(Self::File(hasher.finalize().into()))
    }
}

/// The contents of a set of inputs, certified by the identities taken on either side of reading
/// them ([`certify`]).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Contents(BTreeMap<PathBuf, Content>);

impl Contents {
    pub fn iter(&self) -> impl Iterator<Item = (&Path, Content)> {
        self.0
            .iter()
            .map(|(path, content)| (path.as_path(), *content))
    }
}

/// Why inputs could not be certified.
#[derive(Debug, thiserror::Error)]
pub enum CertifyError {
    #[error("cannot read shell input {path:?}: {source}")]
    Unreadable { path: PathBuf, source: io::Error },
    #[error("a shell input changed while it was being read")]
    Moved,
}

/// Identities, then bytes, then identities again: the bytes belong to those identities only if
/// nothing moved between the two readings, so a later identity check that finds them unchanged
/// also proves the bytes unchanged.
pub fn certify<'a>(
    paths: impl IntoIterator<Item = &'a Path> + Clone,
) -> Result<(Snapshot, Contents), CertifyError> {
    let before = Snapshot::take(paths.clone());
    let contents = paths
        .into_iter()
        .map(|path| {
            Content::of(path)
                .map(|content| (path.to_path_buf(), content))
                .map_err(|source| CertifyError::Unreadable {
                    path: path.to_path_buf(),
                    source,
                })
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    let after = Snapshot::take(before.paths());
    if !before.same_as(&after) {
        return Err(CertifyError::Moved);
    }
    Ok((after, Contents(contents)))
}

/// What one activation observed about its own inputs.
#[derive(Clone, Debug)]
pub struct ActivationEvidence {
    /// The inputs the activation's `DIRENV_WATCHES` names.
    pub entries: Vec<WatchEntry>,
    /// Identities of the previous generation's inputs, taken just before evaluation began.
    pub before: Snapshot,
    /// The workspace filesystem's clock read around the evaluation; `None` when no evaluation
    /// ran.
    pub clock: Option<EvaluationClock>,
    /// Identities of `entries` taken as soon as evaluation finished.
    pub after: Snapshot,
    /// The bytes of `entries`, certified by `after` ([`certify`]); `None` when an input moved
    /// while they were read, which leaves the activation unstable.
    pub contents: Option<Contents>,
}

impl ActivationEvidence {
    /// Identities and bytes of `entries` as they are now, for an activation that just finished.
    pub fn observe(entries: &[WatchEntry]) -> (Snapshot, Option<Contents>) {
        match certify(entries.iter().map(|entry| entry.path.as_path())) {
            Ok((after, contents)) => (after, Some(contents)),
            Err(_) => (
                Snapshot::take(entries.iter().map(|entry| entry.path.as_path())),
                None,
            ),
        }
    }

    /// Whether every input the activation read stayed put while it ran, and its bytes are known.
    ///
    /// An input the previous generation also watched must be identical before and after. An
    /// input first seen by this activation has no earlier identity, so it must have last
    /// changed before the start: ctime moves on every content, rename or metadata change and no
    /// tool can set it back — only a clock stepped backwards can, which is why the clock must
    /// also have run forward across the evaluation ([`EvaluationClock`]). direnv's existence
    /// bit, which is exact, must agree with what is there now.
    pub fn stable(&self) -> bool {
        self.contents.is_some()
            && self.entries.iter().all(|entry| {
                let Some(now) = self.after.get(&entry.path) else {
                    return false;
                };
                let unchanged = match self.before.get(&entry.path) {
                    Some(before) => before.same_as(now),
                    None => match now {
                        FileState::Present { dev, ctime_ns, .. } => self
                            .clock
                            .is_some_and(|clock| clock.precedes_start(dev, ctime_ns)),
                        FileState::Missing | FileState::Unreadable(_) => true,
                    },
                };
                unchanged && entry.exists == now.exists()
            })
    }
}

/// A kernel subscription to one generation's inputs.
///
/// An event on any input only reports "something changed"; how many and which does not
/// matter, because the answer is always the same: the generation is stale. Events queue in the
/// kernel until [`Watcher::drain`] reads them at the pool's decision points.
pub struct Watcher {
    platform: platform::Subscription,
}

impl Watcher {
    /// Subscribe every entry: the file itself when it exists, else the nearest existing
    /// ancestor directory, where its creation will appear.
    pub fn subscribe(entries: &[WatchEntry]) -> io::Result<Self> {
        let mut targets = std::collections::BTreeSet::new();
        for entry in entries {
            match FileState::of(&entry.path) {
                FileState::Present { .. } | FileState::Unreadable(_) => {
                    targets.insert((entry.path.clone(), platform::Target::File));
                }
                FileState::Missing => {
                    if let Some(ancestor) = entry
                        .path
                        .ancestors()
                        .skip(1)
                        .find(|ancestor| ancestor.is_dir())
                    {
                        targets.insert((ancestor.to_path_buf(), platform::Target::Directory));
                    }
                }
            }
        }
        Ok(Self {
            platform: platform::Subscription::new(&targets)?,
        })
    }

    /// Whether any subscribed input changed since the last drain.
    pub fn drain(&self) -> io::Result<bool> {
        self.platform.drain()
    }
}

#[cfg(target_os = "macos")]
mod platform {
    use std::collections::BTreeSet;
    use std::ffi::CString;
    use std::io;
    use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd};
    use std::os::unix::ffi::OsStrExt as _;
    use std::path::PathBuf;

    #[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
    pub enum Target {
        File,
        Directory,
    }

    /// One kqueue with an `EVFILT_VNODE` filter per watched vnode.
    pub struct Subscription {
        queue: OwnedFd,
        /// Held open for the subscription's lifetime: closing a vnode's descriptor removes its
        /// filter. `O_EVTONLY` never keeps a volume from unmounting.
        _vnodes: Vec<OwnedFd>,
    }

    const EVENTS: u32 = libc::NOTE_DELETE
        | libc::NOTE_WRITE
        | libc::NOTE_EXTEND
        | libc::NOTE_ATTRIB
        | libc::NOTE_LINK
        | libc::NOTE_RENAME
        | libc::NOTE_REVOKE;

    impl Subscription {
        pub fn new(targets: &BTreeSet<(PathBuf, Target)>) -> io::Result<Self> {
            // SAFETY: kqueue takes no arguments and returns a new descriptor or -1.
            let raw = unsafe { libc::kqueue() };
            if raw < 0 {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: `raw` is a freshly created descriptor this function owns.
            let queue = unsafe { OwnedFd::from_raw_fd(raw) };
            // SAFETY: plain fcntl on an owned descriptor.
            if unsafe { libc::fcntl(queue.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) } < 0 {
                return Err(io::Error::last_os_error());
            }
            let mut vnodes = Vec::with_capacity(targets.len());
            let mut changes = Vec::with_capacity(targets.len());
            for (path, _) in targets {
                let path = CString::new(path.as_os_str().as_bytes())
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
                // SAFETY: `path` is a NUL-terminated string that outlives the call.
                let raw = unsafe { libc::open(path.as_ptr(), libc::O_EVTONLY | libc::O_CLOEXEC) };
                if raw < 0 {
                    // Vanished between the stat and the open: that is itself a change, which
                    // the pool's stat backstop reports on the next decision.
                    continue;
                }
                // SAFETY: `raw` is a freshly opened descriptor this function owns.
                let vnode = unsafe { OwnedFd::from_raw_fd(raw) };
                changes.push(libc::kevent {
                    ident: usize::try_from(vnode.as_raw_fd()).map_err(io::Error::other)?,
                    filter: libc::EVFILT_VNODE,
                    flags: libc::EV_ADD | libc::EV_CLEAR,
                    fflags: EVENTS,
                    data: 0,
                    udata: std::ptr::null_mut(),
                });
                vnodes.push(vnode);
            }
            if !changes.is_empty() {
                let count = i32::try_from(changes.len()).map_err(io::Error::other)?;
                // SAFETY: `changes` holds `count` initialized kevents; no event list is read.
                let registered = unsafe {
                    libc::kevent(
                        queue.as_raw_fd(),
                        changes.as_ptr(),
                        count,
                        std::ptr::null_mut(),
                        0,
                        std::ptr::null(),
                    )
                };
                if registered < 0 {
                    return Err(io::Error::last_os_error());
                }
            }
            Ok(Self {
                queue,
                _vnodes: vnodes,
            })
        }

        pub fn drain(&self) -> io::Result<bool> {
            let mut changed = false;
            let immediately = libc::timespec {
                tv_sec: 0,
                tv_nsec: 0,
            };
            let mut events = [libc::kevent {
                ident: 0,
                filter: 0,
                flags: 0,
                fflags: 0,
                data: 0,
                udata: std::ptr::null_mut(),
            }; 64];
            loop {
                // SAFETY: `events` has room for 64 kevents and the zero timeout never blocks.
                let received = unsafe {
                    libc::kevent(
                        self.queue.as_raw_fd(),
                        std::ptr::null(),
                        0,
                        events.as_mut_ptr(),
                        64,
                        &immediately,
                    )
                };
                if received < 0 {
                    let error = io::Error::last_os_error();
                    if error.kind() == io::ErrorKind::Interrupted {
                        continue;
                    }
                    return Err(error);
                }
                if received > 0 {
                    changed = true;
                }
                if received < 64 {
                    return Ok(changed);
                }
            }
        }
    }
}

#[cfg(target_os = "linux")]
mod platform {
    use std::collections::BTreeSet;
    use std::ffi::CString;
    use std::io;
    use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd};
    use std::os::unix::ffi::OsStrExt as _;
    use std::path::PathBuf;

    #[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
    pub enum Target {
        File,
        Directory,
    }

    /// One non-blocking inotify instance with a watch per target.
    pub struct Subscription {
        inotify: OwnedFd,
    }

    impl Subscription {
        pub fn new(targets: &BTreeSet<(PathBuf, Target)>) -> io::Result<Self> {
            // SAFETY: inotify_init1 takes flags only and returns a new descriptor or -1.
            let raw = unsafe { libc::inotify_init1(libc::IN_NONBLOCK | libc::IN_CLOEXEC) };
            if raw < 0 {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: `raw` is a freshly created descriptor this function owns.
            let inotify = unsafe { OwnedFd::from_raw_fd(raw) };
            for (path, target) in targets {
                let mask = match target {
                    Target::File => {
                        libc::IN_MODIFY
                            | libc::IN_ATTRIB
                            | libc::IN_CLOSE_WRITE
                            | libc::IN_MOVE_SELF
                            | libc::IN_DELETE_SELF
                    }
                    Target::Directory => libc::IN_CREATE | libc::IN_MOVED_TO,
                };
                let path = CString::new(path.as_os_str().as_bytes())
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
                // SAFETY: `path` is NUL-terminated and outlives the call.
                // A path that vanished since the stat is a change the stat backstop reports.
                let _ =
                    unsafe { libc::inotify_add_watch(inotify.as_raw_fd(), path.as_ptr(), mask) };
            }
            Ok(Self { inotify })
        }

        pub fn drain(&self) -> io::Result<bool> {
            let mut changed = false;
            let mut buffer = [0_u8; 4096];
            loop {
                // SAFETY: `buffer` is writable for its full length; the descriptor is owned.
                let read = unsafe {
                    libc::read(
                        self.inotify.as_raw_fd(),
                        buffer.as_mut_ptr().cast(),
                        buffer.len(),
                    )
                };
                if read > 0 {
                    changed = true;
                    continue;
                }
                if read == 0 {
                    return Ok(changed);
                }
                let error = io::Error::last_os_error();
                match error.kind() {
                    io::ErrorKind::WouldBlock => return Ok(changed),
                    io::ErrorKind::Interrupted => {}
                    _ => return Err(error),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode(json: &str) -> String {
        let compressed = miniz_oxide::deflate::compress_to_vec_zlib(json.as_bytes(), 6);
        base64::engine::general_purpose::URL_SAFE.encode(compressed)
    }

    fn scratch(label: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "cowshed-shell-watch-{label}-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::canonicalize(root).unwrap()
    }

    #[test]
    fn direnv_watch_lists_decode_with_or_without_padding() {
        let json = r#"[{"path":"/w/.envrc","modtime":1789481595,"exists":true},
            {"path":"/w/missing","modtime":0,"exists":false},
            {"path":"/w/.envrc","modtime":1789481595,"exists":true}]"#;
        let padded = encode(json);
        let unpadded = padded.trim_end_matches('=').to_owned();
        for encoded in [padded, unpadded] {
            assert_eq!(
                decode_direnv_watches(&encoded).unwrap(),
                vec![
                    WatchEntry {
                        path: "/w/.envrc".into(),
                        exists: true
                    },
                    WatchEntry {
                        path: "/w/missing".into(),
                        exists: false
                    },
                ]
            );
        }
    }

    #[test]
    fn a_watch_list_that_is_not_direnvs_is_refused_by_stage() {
        assert!(matches!(
            decode_direnv_watches("!!!"),
            Err(WatchListError::Encoding(_))
        ));
        assert!(matches!(
            decode_direnv_watches(&base64::engine::general_purpose::URL_SAFE.encode(b"plain")),
            Err(WatchListError::Compression(_))
        ));
        assert!(matches!(
            decode_direnv_watches(&encode("{}")),
            Err(WatchListError::Json(_))
        ));
        assert_eq!(
            decode_direnv_watches(&encode(r#"[{"path":"rel","modtime":0,"exists":true}]"#)),
            Err(WatchListError::RelativePath("rel".into()))
        );
        let many = format!(
            "[{}]",
            (0..=MAX_WATCHED_PATHS)
                .map(|index| format!(r#"{{"path":"/w/{index}","modtime":0,"exists":false}}"#))
                .collect::<Vec<_>>()
                .join(",")
        );
        assert_eq!(
            decode_direnv_watches(&encode(&many)),
            Err(WatchListError::TooMany(MAX_WATCHED_PATHS + 1))
        );
    }

    #[test]
    fn a_same_size_rewrite_once_the_clock_moved_is_a_new_identity() {
        let root = scratch("same-size");
        let file = root.join("watched.lock");
        std::fs::write(&file, b"one").unwrap();
        let first = Snapshot::take([file.as_path()]);
        FsSeparation::take(&root).unwrap();
        // Same size, same inode: only the stamps tell these apart.
        std::fs::write(&file, b"two").unwrap();
        assert!(first.changed_since());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn a_reading_separates_the_changes_before_it_from_those_after() {
        let root = scratch("separating");
        let earlier = root.join("earlier");
        let later = root.join("later");
        for _ in 0..5 {
            std::fs::write(&earlier, b"e").unwrap();
            let reading = FsSeparation::take(&root).unwrap();
            std::fs::write(&later, b"l").unwrap();
            let stamp = |path: &Path| match FileState::of(path) {
                FileState::Present { dev, ctime_ns, .. } => (dev, ctime_ns),
                other => panic!("{} is {other:?}", path.display()),
            };
            let (dev, ctime_ns) = stamp(&earlier);
            assert!(
                reading.precedes(dev, ctime_ns),
                "a change before the reading"
            );
            let (dev, ctime_ns) = stamp(&later);
            assert!(
                !reading.precedes(dev, ctime_ns),
                "a change after the reading"
            );
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    /// ZFS on a Linux 6.13+ kernel with POSIX ACLs, the CI host's filesystem: a write takes the
    /// coarse clock, set at each 4 ms tick, and a creation or attribute change takes the VFS
    /// clock. That never reads below the multigrain floor, which a fine-grained stamp on any
    /// tmpfs or ext4 of a busy host raises to the present: here every one has.
    struct BusyHostZfs {
        now_ns: i128,
    }

    impl BusyHostZfs {
        const TICK_NS: i128 = 4_000_000;

        fn coarse(&self) -> i128 {
            self.now_ns - self.now_ns.rem_euclid(Self::TICK_NS)
        }

        fn floored(&self) -> i128 {
            self.now_ns
        }

        /// Each change and each probe takes half a millisecond.
        fn elapse(&mut self) {
            self.now_ns += 500_000;
        }

        fn probe(&mut self) -> Probe {
            let probe = Probe {
                dev: 1,
                created_ns: self.floored(),
                written_ns: self.coarse(),
                attributed_ns: self.floored(),
            };
            self.elapse();
            probe
        }

        fn write(&mut self) -> i128 {
            let stamp = self.coarse();
            self.elapse();
            stamp
        }

        fn chmod(&mut self) -> i128 {
            let stamp = self.floored();
            self.elapse();
            stamp
        }
    }

    /// CI (3488cf976) saw a write after the reading stamped before it. Here a separation made of
    /// creation stamps alone would be the first later probe: its VFS stamp passed the first's,
    /// and a write after it stamps the coarse tick both probes fell in, before the reading.
    #[test]
    fn a_separation_waits_for_the_slowest_of_a_filesystems_clocks() {
        let mut zfs = BusyHostZfs {
            now_ns: 10 * NANOS_PER_SECOND + 100_000,
        };
        let written_before = zfs.write();
        let changed_before = zfs.chmod();
        let first = zfs.probe();
        let mut probes = 0;
        let separation = FsSeparation::across(
            first,
            || {
                probes += 1;
                Ok(zfs.probe())
            },
            || Ok(()),
        )
        .unwrap();
        assert!(separation.precedes(1, written_before));
        assert!(separation.precedes(1, changed_before));
        let written_after = zfs.write();
        assert!(
            !separation.precedes(1, written_after),
            "a write after the separation, stamped {written_after} by the coarse clock"
        );
        assert!(!separation.precedes(1, zfs.chmod()));
        assert_eq!(
            probes, 6,
            "the coarse clock passes the first probe's VFS stamps at its next tick, 10.004 s"
        );
    }

    #[test]
    fn a_probe_of_another_device_separates_nothing() {
        let probe = |dev, ctime_ns| Probe {
            dev,
            created_ns: ctime_ns,
            written_ns: ctime_ns,
            attributed_ns: ctime_ns,
        };
        let moved = FsSeparation::across(probe(1, 1), || Ok(probe(2, 2)), || Ok(()));
        assert!(moved.is_err(), "{moved:?}");
    }

    #[test]
    fn another_filesystems_input_must_predate_the_separations_second() {
        let second = NANOS_PER_SECOND;
        let separation = FsSeparation {
            dev: 1,
            last_before_ns: 10 * second + 500,
            crossed_ns: 10 * second + 4_000_000,
        };
        assert!(
            separation.precedes(1, 10 * second + 500),
            "the last reading before the call ties nothing after it"
        );
        assert!(
            !separation.precedes(1, 10 * second + 501),
            "a stamp after the last reading before may be a change while the call probed"
        );
        assert!(
            !separation.precedes(2, 10 * second),
            "a whole-second stamp inside the separation's second may be a later change"
        );
        assert!(separation.precedes(2, 10 * second - 1));
    }

    #[test]
    fn an_input_that_appears_is_a_change() {
        let root = scratch("appears");
        let file = root.join("later");
        let before = Snapshot::take([file.as_path()]);
        assert!(!before.changed_since());
        std::fs::write(&file, b"now").unwrap();
        assert!(before.changed_since());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn activation_stability_uses_the_earlier_identity_or_the_start_time() {
        let root = scratch("stability");
        let known = root.join("known");
        let new = root.join("new");
        std::fs::write(&known, b"k").unwrap();
        std::fs::write(&new, b"n").unwrap();
        let entries = vec![
            WatchEntry {
                path: known.clone(),
                exists: true,
            },
            WatchEntry {
                path: new.clone(),
                exists: true,
            },
        ];
        let before = Snapshot::take([known.as_path()]);
        let started = FsSeparation::take(&root).unwrap();
        // The end reading follows the snapshot it closes, as an activation takes it.
        let around = |after: Snapshot| {
            let finished = FsInstant::read(&root).unwrap();
            (after, Some(EvaluationClock { started, finished }))
        };
        let (after, clock) = around(Snapshot::take([known.as_path(), new.as_path()]));
        let evidence = ActivationEvidence {
            entries: entries.clone(),
            before: before.clone(),
            clock,
            after,
            contents: Some(Contents::default()),
        };
        assert!(evidence.stable(), "nothing moved during evaluation");

        // An input first recorded by this activation that changed after it began.
        std::fs::OpenOptions::new()
            .append(true)
            .open(&new)
            .unwrap()
            .write_all(b"!")
            .unwrap();
        let (after, clock) = around(Snapshot::take([known.as_path(), new.as_path()]));
        let evidence = ActivationEvidence {
            entries: entries.clone(),
            before: before.clone(),
            clock,
            after,
            contents: Some(Contents::default()),
        };
        assert!(!evidence.stable(), "a new input written mid-evaluation");

        // A known input that changed between the earlier snapshot and the end.
        std::fs::write(&known, b"K").unwrap();
        let (after, clock) = around(Snapshot::take([known.as_path()]));
        let evidence = ActivationEvidence {
            entries: vec![entries[0].clone()],
            before,
            clock,
            after,
            contents: Some(Contents::default()),
        };
        assert!(!evidence.stable(), "a known input rewritten mid-evaluation");

        // direnv recorded the input as absent but it exists by the end, and did so before the
        // activation started.
        let started = FsSeparation::take(&root).unwrap();
        let after = Snapshot::take([known.as_path()]);
        let evidence = ActivationEvidence {
            entries: vec![WatchEntry {
                path: known.clone(),
                exists: false,
            }],
            before: Snapshot::default(),
            clock: Some(EvaluationClock {
                started,
                finished: FsInstant::read(&root).unwrap(),
            }),
            after,
            contents: Some(Contents::default()),
        };
        assert!(
            !evidence.stable(),
            "existence disagrees with direnv's record"
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    /// A wall clock stepped back while the evaluation ran — a VM's time sync, an NTP step —
    /// stamps an input written mid-evaluation earlier than the start. The end reading, earlier
    /// than the start's crossing, is what shows the clock went backwards.
    #[test]
    fn a_clock_stepped_back_mid_evaluation_proves_no_new_input_older() {
        let second = NANOS_PER_SECOND;
        let new = PathBuf::from("/workspace/bun.lock");
        let evidence = |input_ctime_ns: i128, finished_ns: i128| ActivationEvidence {
            entries: vec![WatchEntry {
                path: new.clone(),
                exists: true,
            }],
            before: Snapshot::default(),
            clock: Some(EvaluationClock {
                started: FsSeparation {
                    dev: 1,
                    last_before_ns: 10 * second + second / 2 - 4_000_000,
                    crossed_ns: 10 * second + second / 2,
                },
                finished: FsInstant {
                    dev: 1,
                    ctime_ns: finished_ns,
                },
            }),
            after: Snapshot(BTreeMap::from([(
                new.clone(),
                FileState::Present {
                    dev: 1,
                    ino: 7,
                    size: 3,
                    mtime_ns: input_ctime_ns,
                    ctime_ns: input_ctime_ns,
                },
            )])),
            contents: Some(Contents::default()),
        };
        // Started at 10.5s, stepped back a second: the mid-evaluation write stamps 9.7s and the
        // clock reads 9.8s at the end.
        assert!(
            !evidence(9 * second + 700_000_000, 9 * second + 800_000_000).stable(),
            "a clock that ran backwards orders nothing against the start"
        );
        // The same input stamp under a clock that ran forward is an input older than the start.
        assert!(evidence(9 * second + 700_000_000, 10 * second + 600_000_000).stable());
        // A clock that did not move at all across the evaluation still ran forward.
        assert!(evidence(9 * second + 700_000_000, 10 * second + second / 2).stable());
        // An end reading below the crossing ran backwards, even above the last reading before.
        assert!(
            !evidence(9 * second + 700_000_000, 10 * second + second / 2 - 1).stable(),
            "a slowest clock that fell back below the crossing orders nothing"
        );
    }

    #[test]
    fn the_kernel_reports_a_write_to_a_watched_file_and_nothing_else() {
        let root = scratch("kernel");
        let watched = root.join("watched");
        let unwatched = root.join("unwatched");
        let appears = root.join("sub/appears");
        std::fs::create_dir_all(root.join("sub")).unwrap();
        std::fs::write(&watched, b"w").unwrap();
        let watcher = Watcher::subscribe(&[
            WatchEntry {
                path: watched.clone(),
                exists: true,
            },
            WatchEntry {
                path: appears.clone(),
                exists: false,
            },
        ])
        .unwrap();
        assert!(!watcher.drain().unwrap());
        std::fs::write(&unwatched, b"u").unwrap();
        assert!(
            !watcher.drain().unwrap(),
            "an unwatched sibling is not an input"
        );
        std::fs::write(&watched, b"W").unwrap();
        assert!(watcher.drain().unwrap());
        assert!(
            !watcher.drain().unwrap(),
            "a drained change is reported once"
        );
        std::fs::write(&appears, b"a").unwrap();
        assert!(
            watcher.drain().unwrap(),
            "a recorded-absent input appearing"
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    /// A clone of a volume keeps each file's inode, size, mtime and ctime and mounts under
    /// another device: the same file state, which an activation's evidence must not read as an
    /// input that changed. Any one of the four fields differing is a change.
    #[test]
    fn the_device_number_is_not_part_of_an_inputs_identity() {
        let state = |dev, ino, size, mtime_ns, ctime_ns| FileState::Present {
            dev,
            ino,
            size,
            mtime_ns,
            ctime_ns,
        };
        let main = state(16_777_256, 1_398_438, 8_784, 5, 6);
        assert!(main.same_as(state(16_777_401, 1_398_438, 8_784, 5, 6)));
        for changed in [
            state(16_777_256, 1_398_439, 8_784, 5, 6),
            state(16_777_256, 1_398_438, 8_785, 5, 6),
            state(16_777_256, 1_398_438, 8_784, 7, 6),
            state(16_777_256, 1_398_438, 8_784, 5, 7),
            FileState::Missing,
            FileState::Unreadable(13),
        ] {
            assert!(!main.same_as(changed), "{changed:?} differs from {main:?}");
        }
        assert!(FileState::Unreadable(13).same_as(FileState::Unreadable(13)));
        assert!(!FileState::Unreadable(13).same_as(FileState::Unreadable(1)));

        let path = PathBuf::from("/workspace/tooling/direnv/devenv.nix");
        let evidence = ActivationEvidence {
            entries: vec![WatchEntry {
                path: path.clone(),
                exists: true,
            }],
            before: Snapshot(BTreeMap::from([(path.clone(), main)])),
            clock: None,
            after: Snapshot(BTreeMap::from([(
                path,
                state(16_777_401, 1_398_438, 8_784, 5, 6),
            )])),
            contents: Some(Contents::default()),
        };
        assert!(
            evidence.stable(),
            "an input remounted under another device stayed put"
        );
    }

    /// Bytes decide: a file's content is its bytes, a directory's its entry names, and a
    /// rewrite of the same bytes under a new identity is the same content.
    #[test]
    fn content_is_bytes_for_files_and_entry_names_for_directories() {
        let root =
            std::env::temp_dir().join(format!("cowshed-content-{}", uuid::Uuid::new_v4().simple()));
        let directory = root.join("recipe");
        std::fs::create_dir_all(&directory).unwrap();
        let file = directory.join("a.nix");
        std::fs::write(&file, b"a").unwrap();
        let (before, contents) = certify([file.as_path(), directory.as_path()]).unwrap();

        std::fs::remove_file(&file).unwrap();
        std::fs::write(&file, b"a").unwrap();
        let (after, same) = certify([file.as_path(), directory.as_path()]).unwrap();
        assert!(!before.same_as(&after), "a new file is a new identity");
        assert_eq!(contents, same, "and the same content");

        std::fs::write(directory.join("b.patch"), b"b").unwrap();
        let (_, added) = certify([file.as_path(), directory.as_path()]).unwrap();
        assert_ne!(
            contents, added,
            "an entry added to a watched directory is a change"
        );
        assert_eq!(Content::of(&root.join("absent")).unwrap(), Content::Missing);
        std::fs::remove_dir_all(root).unwrap();
    }
}
