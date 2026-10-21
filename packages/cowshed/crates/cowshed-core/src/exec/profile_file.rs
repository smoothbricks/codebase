//! A rendered Seatbelt profile as the file `sandbox-exec -f` reads, and the parameters a launch
//! binds with `-D` (07_api.md, `SandboxSpec::wrap`; 04_sandbox.md, "Exec pipeline" step 4).
//!
//! A profile on the command line (`sandbox-exec -p <profile>`) is one argv element of the
//! launched process. Profiles grow with grants past the 128 KiB a command argument may hold
//! (`MAX_COMMAND_ARG_BYTES`), and a job's process observation reads that argv back. The file
//! keeps the profile out of every argv.
//!
//! A profile's text depends only on its workspace's authority: the one path that moves under a
//! workspace, its build volume, is the `BUILD_VOLUME` parameter (`crate::sandbox`), bound per
//! launch. So one file serves every job of a workspace profile, before and after a build volume
//! pivot. It is named by its SHA-256 in the [`profile_directory`] of the store the launcher
//! serves, inside the store every rendered profile denies, read and write, to every role, so no
//! sandboxed process can replace a profile a later launch reads. A fixture's launches use its
//! own store, never the host's. Every launch makes sure of its file: it is written, mode 0400, by
//! a rename that makes it whole or absent when missing, and its mtime is bumped when present.
//! `cowshed gc` ([`collect_profiles`]) removes a file only when its last launch predates the
//! start of every live supervisor of the store, an order of events and not a timeout: no live
//! supervisor can still be about to launch from it, and a launch that wants it writes it again.

use std::ffi::{OsStr, OsString};
use std::fs::{self, DirBuilder, OpenOptions};
use std::io::{self, Write as _};
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::{DirBuilderExt as _, OpenOptionsExt as _};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

use crate::api::dto::Sha256Digest;
use crate::sandbox::{BUILD_VOLUME_PARAM, SeatbeltProfile};

/// Where the profile files of `store_root`'s launches live: beside its supervisors' sockets in
/// its `run` directory.
pub fn profile_directory(store_root: &Path) -> PathBuf {
    store_root.join("run").join("profiles")
}

/// One rendered profile on disk, `<sha256>.sb`, whole once it exists.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProfileFile {
    path: PathBuf,
}

/// This process's next staging-file number, so concurrent first writes never share one.
static NEXT: AtomicU64 = AtomicU64::new(0);

impl ProfileFile {
    /// The file of `profile` in `directory`, made sure of for a launch now: its mtime bumped
    /// when present, written when absent. The content is staged beside it and renamed into
    /// place, so a reader finds the whole profile or none; a concurrent writer of the same
    /// profile renames identical bytes over it.
    pub fn write(directory: &Path, profile: &str) -> io::Result<Self> {
        let digest = Sha256Digest::compute(profile.as_bytes()).to_hex();
        let path = directory.join(format!("{digest}.sb"));
        match touch(&path) {
            Ok(()) => return Ok(Self { path }),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(directory)
            .map_err(|error| denied_as_present(directory, error))?;
        let staged = directory.join(format!(
            "{digest}.{}.{}.staged",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let written = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o400)
            .open(&staged)
            .and_then(|mut file| file.write_all(profile.as_bytes()))
            .and_then(|()| fs::rename(&staged, &path));
        if let Err(error) = written {
            let _ = fs::remove_file(&staged);
            return Err(error);
        }
        Ok(Self { path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// `error` from creating `directory`, with a sandbox's denial named as one. Seatbelt answers a
/// lookup of a path its profile denies as absent, so the profile's touch sees no file, the create
/// meets the directory and answers EEXIST, and a look at it finds nothing: the directory is there
/// and this process may not touch it. That is EPERM, the operation not permitted, which is what
/// its callers report as a sandbox's denial; every other error is the create's own.
fn denied_as_present(directory: &Path, error: io::Error) -> io::Error {
    if error.kind() == io::ErrorKind::AlreadyExists && fs::symlink_metadata(directory).is_err() {
        io::Error::from_raw_os_error(libc::EPERM)
    } else {
        error
    }
}
/// Set `path`'s access and modification times to now, without following a symlink.
fn touch(path: &Path) -> io::Result<()> {
    let path = std::ffi::CString::new(path.as_os_str().as_bytes())
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    // SAFETY: `path` is NUL-terminated; a null `times` means now for both.
    match unsafe {
        libc::utimensat(
            libc::AT_FDCWD,
            path.as_ptr(),
            std::ptr::null(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    } {
        0 => Ok(()),
        _ => Err(io::Error::last_os_error()),
    }
}

/// Remove the profile files of `store_root` whose last launch predates the start of its oldest
/// live supervisor; with no live supervisor, nothing is removed. Answers how many went.
pub fn collect_profiles(store_root: &Path) -> io::Result<usize> {
    let Some(oldest) = oldest_live_supervisor(&store_root.join("run"))? else {
        return Ok(0);
    };
    let directory = profile_directory(store_root);
    let entries = match fs::read_dir(&directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error),
    };
    let mut removed = 0;
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name();
        let Some(digest) = name.to_str().and_then(|name| name.strip_suffix(".sb")) else {
            continue;
        };
        if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            continue;
        }
        let launched = match entry.metadata().and_then(|metadata| metadata.modified()) {
            Ok(launched) => launched,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        if launched >= oldest {
            continue;
        }
        match fs::remove_file(entry.path()) {
            Ok(()) => removed += 1,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Ok(removed)
}

/// When the oldest supervisor still serving a socket in `run` started serving it.
fn oldest_live_supervisor(run: &Path) -> io::Result<Option<SystemTime>> {
    let entries = match fs::read_dir(run) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let mut oldest: Option<SystemTime> = None;
    for entry in entries {
        let path = entry?.path();
        if path.extension() != Some(OsStr::new("sock")) {
            continue;
        }
        if let Some(started) = crate::runtime::supervisor_socket::serving_since(&path)? {
            oldest = Some(oldest.map_or(started, |oldest| oldest.min(started)));
        }
    }
    Ok(oldest)
}

/// How `sandbox-exec` is told a profile: its file and the parameters its rules name.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProfileLaunch {
    file: ProfileFile,
    build_volume: Option<PathBuf>,
}

impl ProfileLaunch {
    /// A launch of `profile`: its file made sure of in `directory` now, bound to the build
    /// volume its text names.
    pub fn write(directory: &Path, profile: &SeatbeltProfile) -> io::Result<Self> {
        Ok(Self {
            file: ProfileFile::write(directory, profile.text())?,
            build_volume: profile.build_volume().map(Path::to_path_buf),
        })
    }

    pub fn file(&self) -> &ProfileFile {
        &self.file
    }

    /// `sandbox-exec`'s arguments before the command: each parameter's `-D NAME=value`, then
    /// `-f <file> --`. The profile travels in its file, never as an argument.
    pub fn args(&self) -> Vec<OsString> {
        let mut args = Vec::with_capacity(5);
        if let Some(volume) = &self.build_volume {
            let mut binding = OsString::from(BUILD_VOLUME_PARAM);
            binding.push("=");
            binding.push(volume);
            args.extend([OsString::from("-D"), binding]);
        }
        args.extend([
            OsString::from("-f"),
            self.file.path.clone().into_os_string(),
            OsString::from("--"),
        ]);
        args
    }

    /// What tells one launch's sandbox from another's: the profile and every binding.
    pub fn identity(&self) -> Vec<&OsStr> {
        let mut identity = vec![self.file.path.as_os_str()];
        identity.extend(self.build_volume.iter().map(|volume| volume.as_os_str()));
        identity
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

    /// A fresh directory under the scratch parent, short enough for a socket inside it to bind (`sun_path`
    /// holds 104 bytes), removed with its guard on every exit path.
    fn scratch() -> crate::temp_root::TempRoot {
        crate::temp_root::TempRoot::new_in(&crate::temp_parent::scratch_parent(), "cowshed-pf")
    }

    /// Set `path`'s modification time to `seconds` after the epoch, without following a symlink.
    fn set_mtime(path: &Path, seconds: i64) {
        let path = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
        let time = libc::timespec {
            tv_sec: seconds,
            tv_nsec: 0,
        };
        // SAFETY: `path` is NUL-terminated and `times` holds the two timespecs utimensat reads.
        let set = unsafe {
            libc::utimensat(
                libc::AT_FDCWD,
                path.as_ptr(),
                [time, time].as_ptr(),
                libc::AT_SYMLINK_NOFOLLOW,
            )
        };
        assert_eq!(set, 0, "{}", io::Error::last_os_error());
    }

    fn mtime(path: &Path) -> SystemTime {
        fs::symlink_metadata(path).unwrap().modified().unwrap()
    }

    #[test]
    fn a_profile_is_written_once_read_only_kept_and_its_launches_bump_it() {
        let scratch = scratch();
        let directory = scratch.join("profiles");
        let first = ProfileFile::write(&directory, "(version 1)(allow default)").unwrap();
        assert_eq!(
            fs::read_to_string(first.path()).unwrap(),
            "(version 1)(allow default)"
        );
        let metadata = fs::metadata(first.path()).unwrap();
        assert_eq!(metadata.permissions().mode() & 0o777, 0o400);
        set_mtime(first.path(), 1_000);
        let again = ProfileFile::write(&directory, "(version 1)(allow default)").unwrap();
        assert_eq!(again, first, "one file per profile");
        assert_eq!(
            fs::metadata(again.path()).unwrap().ino(),
            metadata.ino(),
            "an existing profile is not written again"
        );
        assert!(
            mtime(again.path()) > SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1_000),
            "a launch bumps its file"
        );
        let other = ProfileFile::write(&directory, "(version 1)(deny default)").unwrap();
        assert_ne!(other, first);
        let names: Vec<_> = fs::read_dir(&directory)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        assert_eq!(names.len(), 2, "no staging file is left: {names:?}");
        drop((first, again, other));
        assert_eq!(
            fs::read_dir(&directory).unwrap().count(),
            2,
            "dropping a handle removes nothing"
        );
    }

    /// gc removes a profile file exactly when its last launch predates the start of the store's
    /// oldest live supervisor: a supervisor older than the file may still launch from it, a
    /// stopped one is no supervisor, and with none live nothing goes. A launch after gc writes
    /// the file again.
    #[test]
    fn gc_removes_a_profile_only_when_every_live_supervisor_started_after_its_last_launch() {
        let scratch = scratch();
        let store = scratch.join("store");
        let run = store.join("run");
        let profile = "(version 1)(allow default)";
        let file = ProfileFile::write(&profile_directory(&store), profile).unwrap();
        set_mtime(file.path(), 2_000);
        assert_eq!(collect_profiles(&store).unwrap(), 0, "no live supervisor");

        let stopped = run.join("stopped.sock");
        drop(std::os::unix::net::UnixListener::bind(&stopped).unwrap());
        set_mtime(&stopped, 3_000);
        assert_eq!(
            collect_profiles(&store).unwrap(),
            0,
            "a socket nothing is attached to is no live supervisor"
        );

        let older = run.join("older.sock");
        let older_listener = std::os::unix::net::UnixListener::bind(&older).unwrap();
        set_mtime(&older, 1_000);
        let younger = run.join("younger.sock");
        let _younger_listener = std::os::unix::net::UnixListener::bind(&younger).unwrap();
        set_mtime(&younger, 3_000);
        assert_eq!(
            collect_profiles(&store).unwrap(),
            0,
            "a supervisor started before the file's last launch may launch from it again"
        );
        assert!(file.path().is_file());

        drop(older_listener);
        assert_eq!(
            collect_profiles(&store).unwrap(),
            1,
            "every live supervisor started after the file's last launch"
        );
        assert!(!file.path().exists());

        let relaunched = ProfileFile::write(&profile_directory(&store), profile).unwrap();
        assert_eq!(relaunched, file);
        assert_eq!(fs::read_to_string(relaunched.path()).unwrap(), profile);
        assert_eq!(
            collect_profiles(&store).unwrap(),
            0,
            "a launch after the youngest supervisor started keeps its file"
        );
    }

    #[test]
    fn a_launch_binds_the_build_volume_before_its_file() {
        let file = ProfileFile {
            path: PathBuf::from("/private/cowshed/store/run/profiles/a.sb"),
        };
        let launch = |volume: Option<&str>| ProfileLaunch {
            file: file.clone(),
            build_volume: volume.map(PathBuf::from),
        };
        assert_eq!(
            launch(None).args(),
            ["-f", "/private/cowshed/store/run/profiles/a.sb", "--"]
        );
        assert_eq!(
            launch(Some("/m/.build/o/r/0123")).args(),
            [
                "-D",
                "BUILD_VOLUME=/m/.build/o/r/0123",
                "-f",
                "/private/cowshed/store/run/profiles/a.sb",
                "--"
            ]
        );
        assert_ne!(
            launch(Some("/m/.build/o/r/0123")).identity(),
            launch(Some("/m/.build/o/r/4567")).identity(),
            "a pivot is another launch of the same file"
        );
    }
}
