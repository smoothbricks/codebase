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

use std::collections::BTreeMap;
use std::io;
use std::os::unix::fs::MetadataExt as _;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

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

/// The exact identity of one watched path, following symlinks as direnv does.
///
/// `(dev, ino, size, mtime, ctime)` at nanosecond resolution: a rewrite inside the same second
/// changes mtime or ctime, an atomic replace changes the inode, and ctime cannot be set by any
/// user tool, so a content change that restores mtime still shows.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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

    fn exists(self) -> bool {
        !matches!(self, Self::Missing)
    }
}

fn nanoseconds(seconds: i64, nanoseconds: i64) -> i128 {
    i128::from(seconds) * 1_000_000_000 + i128::from(nanoseconds)
}

fn system_time_ns(time: SystemTime) -> i128 {
    match time.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(after) => i128::try_from(after.as_nanos()).unwrap_or(i128::MAX),
        Err(before) => -i128::try_from(before.duration().as_nanos()).unwrap_or(i128::MAX),
    }
}

/// Stat identities of a set of paths at one moment.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
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
            .any(|(path, state)| FileState::of(path) != *state)
    }

    pub fn paths(&self) -> impl Iterator<Item = &Path> {
        self.0.keys().map(PathBuf::as_path)
    }

    fn get(&self, path: &Path) -> Option<FileState> {
        self.0.get(path).copied()
    }
}

/// What one activation observed about its own inputs.
#[derive(Clone, Debug)]
pub struct ActivationEvidence {
    /// The inputs the activation's `DIRENV_WATCHES` names.
    pub entries: Vec<WatchEntry>,
    /// Identities of the previous generation's inputs, taken just before evaluation began.
    pub before: Snapshot,
    /// When evaluation began, after approval and before `direnv export` ran.
    pub started: SystemTime,
    /// Identities of `entries` taken as soon as evaluation finished.
    pub after: Snapshot,
}

impl ActivationEvidence {
    /// Whether every input the activation read stayed put while it ran.
    ///
    /// An input the previous generation also watched must be identical before and after. An
    /// input first seen by this activation has no earlier identity, so its ctime must predate
    /// the start: ctime moves on every content, rename or metadata change and no tool can set
    /// it back. direnv's existence bit, which is exact, must agree with what is there now.
    pub fn stable(&self) -> bool {
        let started = system_time_ns(self.started);
        self.entries.iter().all(|entry| {
            let Some(now) = self.after.get(&entry.path) else {
                return false;
            };
            let unchanged = match self.before.get(&entry.path) {
                Some(before) => before == now,
                None => match now {
                    FileState::Present { ctime_ns, .. } => ctime_ns < started,
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
    use std::io::Write as _;

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
    fn two_writes_inside_one_second_are_two_identities() {
        let root = scratch("same-second");
        let file = root.join("watched.lock");
        std::fs::write(&file, b"one").unwrap();
        let first = Snapshot::take([file.as_path()]);
        // Same size, same second: only nanosecond mtime/ctime tell these apart.
        std::fs::write(&file, b"two").unwrap();
        assert!(first.changed_since());
        std::fs::remove_dir_all(root).unwrap();
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
        let started = SystemTime::now();
        let after = Snapshot::take([known.as_path(), new.as_path()]);
        let evidence = ActivationEvidence {
            entries: entries.clone(),
            before: before.clone(),
            started,
            after,
        };
        assert!(evidence.stable(), "nothing moved during evaluation");

        // An input first recorded by this activation that changed after it began.
        std::fs::OpenOptions::new()
            .append(true)
            .open(&new)
            .unwrap()
            .write_all(b"!")
            .unwrap();
        let evidence = ActivationEvidence {
            entries: entries.clone(),
            before: before.clone(),
            started,
            after: Snapshot::take([known.as_path(), new.as_path()]),
        };
        assert!(!evidence.stable(), "a new input written mid-evaluation");

        // A known input that changed between the earlier snapshot and the end.
        std::fs::write(&known, b"K").unwrap();
        let evidence = ActivationEvidence {
            entries: vec![entries[0].clone()],
            before,
            started: SystemTime::now(),
            after: Snapshot::take([known.as_path()]),
        };
        assert!(!evidence.stable(), "a known input rewritten mid-evaluation");

        // direnv recorded the input as absent but it exists by the end.
        let evidence = ActivationEvidence {
            entries: vec![WatchEntry {
                path: known.clone(),
                exists: false,
            }],
            before: Snapshot::default(),
            started: SystemTime::now(),
            after: Snapshot::take([known.as_path()]),
        };
        assert!(
            !evidence.stable(),
            "existence disagrees with direnv's record"
        );
        std::fs::remove_dir_all(root).unwrap();
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
}
