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
//! pivot. It is named by its SHA-256 in [`profile_directory`], inside the machine-global cowshed
//! root that every rendered profile denies, read and write, to every role, so no sandboxed process
//! can replace a profile a later launch reads. It is written once, mode 0400, by a rename that
//! makes it whole or absent, and kept.

use std::ffi::{OsStr, OsString};
use std::fs::{self, DirBuilder, OpenOptions};
use std::io::{self, Write as _};
use std::os::unix::fs::{DirBuilderExt as _, OpenOptionsExt as _};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::api::dto::Sha256Digest;
use crate::sandbox::{BUILD_VOLUME_PARAM, SandboxConfig};

/// Where profile files live: beside the supervisors' sockets in the store's `run` directory.
pub fn profile_directory() -> PathBuf {
    Path::new(cowshed_gateway_types::STORE_ROOT)
        .join("run")
        .join("profiles")
}

/// One rendered profile on disk, `<sha256>.sb`, whole once it exists.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProfileFile {
    path: PathBuf,
}

/// This process's next staging-file number, so concurrent first writes never share one.
static NEXT: AtomicU64 = AtomicU64::new(0);

impl ProfileFile {
    /// The file of `profile` in [`profile_directory`], written now when it is absent.
    pub fn write(profile: &str) -> io::Result<Self> {
        Self::write_in(&profile_directory(), profile)
    }

    /// The file of `profile` in `directory`, written now when it is absent. The content is
    /// staged beside it and renamed into place, so a reader finds the whole profile or none; a
    /// concurrent writer of the same profile renames identical bytes over it.
    pub fn write_in(directory: &Path, profile: &str) -> io::Result<Self> {
        let digest = Sha256Digest::compute(profile.as_bytes()).to_hex();
        let path = directory.join(format!("{digest}.sb"));
        if fs::symlink_metadata(&path).is_ok_and(|metadata| metadata.is_file()) {
            return Ok(Self { path });
        }
        DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(directory)?;
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

/// How `sandbox-exec` is told a profile: its file and the parameters its rules name.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProfileLaunch {
    file: ProfileFile,
    build_volume: Option<PathBuf>,
}

impl ProfileLaunch {
    /// `file`, the profile rendered from `sandbox`, bound to `sandbox`'s build volume.
    pub fn new(file: ProfileFile, sandbox: &SandboxConfig) -> Self {
        Self::bound(file, sandbox.build_volume_mount.clone())
    }

    /// `file`, bound to `build_volume`, the build volume of the sandbox it was rendered from.
    pub fn bound(file: ProfileFile, build_volume: Option<PathBuf>) -> Self {
        Self { file, build_volume }
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

    fn scratch() -> PathBuf {
        std::env::temp_dir().join(format!(
            "cowshed-profile-file-{}",
            uuid::Uuid::new_v4().simple()
        ))
    }

    #[test]
    fn a_profile_is_written_once_read_only_and_kept() {
        let directory = scratch();
        let first = ProfileFile::write_in(&directory, "(version 1)(allow default)").unwrap();
        assert_eq!(
            fs::read_to_string(first.path()).unwrap(),
            "(version 1)(allow default)"
        );
        let metadata = fs::metadata(first.path()).unwrap();
        assert_eq!(metadata.permissions().mode() & 0o777, 0o400);
        let again = ProfileFile::write_in(&directory, "(version 1)(allow default)").unwrap();
        assert_eq!(again, first, "one file per profile");
        assert_eq!(
            fs::metadata(again.path()).unwrap().ino(),
            metadata.ino(),
            "an existing profile is not written again"
        );
        let other = ProfileFile::write_in(&directory, "(version 1)(deny default)").unwrap();
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
        fs::remove_dir_all(&directory).unwrap();
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
