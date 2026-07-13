use std::ffi::{CStr, CString};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::mem::MaybeUninit;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::{AsRawFd, FromRawFd};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use sha2::{Digest, Sha256};

use super::{ArtifactError, PublicationStage, reject_hardlink, verify_private_file_mode};
use crate::api::dto::{
    OutputPublication, ProtectedOutput, PublicationPolicy, Sha256Digest, StreamInfo,
};

const COPY_BUFFER_BYTES: usize = 64 * 1024;
static NONCE: AtomicU64 = AtomicU64::new(1);

pub(super) fn publish(
    workspace_root: &Path,
    stream: &StreamInfo,
    publication: &OutputPublication,
) -> Result<(), ArtifactError> {
    stream.validate()?;
    let mut parent = Parent::open(workspace_root, publication.path.as_path())?;
    if let Err(primary) = parent.materialize_and_publish(workspace_root, stream, publication.policy)
    {
        if let Err(cleanup) = parent.cleanup_temporary() {
            return Err(publication_error(
                &parent.temporary_path(),
                PublicationStage::Cleanup,
                format!("{primary}; temporary cleanup failed: {cleanup}"),
            ));
        }
        return Err(primary);
    }
    Ok(())
}

fn publication_error(
    path: &Path,
    stage: PublicationStage,
    error: impl std::fmt::Display,
) -> ArtifactError {
    ArtifactError::Publication {
        path: path.to_owned(),
        stage,
        message: error.to_string(),
    }
}

struct Parent {
    directory: File,
    display: PathBuf,
    destination_leaf: CString,
    temporary_leaf: CString,
    temporary_exists: bool,
}

impl Parent {
    fn open(workspace_root: &Path, relative: &Path) -> Result<Self, ArtifactError> {
        let components = relative.components().collect::<Vec<_>>();
        if components.is_empty()
            || components
                .first()
                .is_some_and(|component| component.as_os_str() == ".cowshed")
        {
            return Err(publication_error(
                relative,
                PublicationStage::ValidateDestination,
                "publication cannot target protected .cowshed storage",
            ));
        }
        let destination_leaf = CString::new(
            components
                .last()
                .expect("components is non-empty")
                .as_os_str()
                .as_bytes(),
        )
        .map_err(|error| {
            publication_error(relative, PublicationStage::ValidateDestination, error)
        })?;
        let mut options = OpenOptions::new();
        options.read(true);
        options.custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC);
        let mut directory = options.open(workspace_root).map_err(|error| {
            publication_error(workspace_root, PublicationStage::ValidateDestination, error)
        })?;
        let mut display = workspace_root.to_owned();
        for component in &components[..components.len() - 1] {
            let name = CString::new(component.as_os_str().as_bytes()).map_err(|error| {
                publication_error(relative, PublicationStage::ValidateDestination, error)
            })?;
            let fd = unsafe {
                libc::openat(
                    directory.as_raw_fd(),
                    name.as_ptr(),
                    libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                )
            };
            if fd < 0 {
                return Err(publication_error(
                    &display.join(component.as_os_str()),
                    PublicationStage::ValidateDestination,
                    io::Error::last_os_error(),
                ));
            }
            directory = unsafe { File::from_raw_fd(fd) };
            display.push(component.as_os_str());
        }
        validate_destination(&directory, &display, &destination_leaf)?;
        let nonce = NONCE.fetch_add(1, Ordering::Relaxed);
        let temporary_leaf = CString::new(format!(
            ".{}.cowshed-publish-{}-{nonce}",
            destination_leaf.to_string_lossy(),
            std::process::id()
        ))
        .expect("generated publication leaf contains no NUL");
        Ok(Self {
            directory,
            display,
            destination_leaf,
            temporary_leaf,
            temporary_exists: false,
        })
    }

    fn temporary_path(&self) -> PathBuf {
        self.display
            .join(self.temporary_leaf.to_string_lossy().as_ref())
    }

    fn destination_path(&self) -> PathBuf {
        self.display
            .join(self.destination_leaf.to_string_lossy().as_ref())
    }

    fn materialize_and_publish(
        &mut self,
        workspace_root: &Path,
        stream: &StreamInfo,
        policy: PublicationPolicy,
    ) -> Result<(), ArtifactError> {
        let temporary_path = self.temporary_path();
        let temporary = match stream.storage.artifact() {
            ProtectedOutput::Inline { data } => {
                let mut file = self.create_temporary()?;
                file.write_all(data.as_bytes()).map_err(|error| {
                    publication_error(&temporary_path, PublicationStage::Copy, error)
                })?;
                file
            }
            ProtectedOutput::File { path } => {
                let source_path = workspace_root.join(path.as_path());
                let source = open_authority_source(&source_path, stream)?;
                match self.try_fast_clone(&source)? {
                    Some(file) => file,
                    None => {
                        let mut file = self.create_temporary()?;
                        copy_file_descriptor(&source, &mut file, &source_path, &temporary_path)?;
                        file
                    }
                }
            }
        };
        verify_content(&temporary, &temporary_path, stream.bytes, stream.sha256)?;
        temporary
            .sync_all()
            .map_err(|error| publication_error(&temporary_path, PublicationStage::Sync, error))?;
        drop(temporary);
        publish_relative(
            &self.directory,
            &self.temporary_leaf,
            &self.destination_leaf,
            policy,
            &self.destination_path(),
        )?;
        self.temporary_exists = false;
        self.directory.sync_all().map_err(|error| {
            publication_error(&self.destination_path(), PublicationStage::Sync, error)
        })?;
        let metadata = metadata_at(&self.directory, &self.destination_leaf).map_err(|error| {
            publication_error(&self.destination_path(), PublicationStage::Publish, error)
        })?;
        if metadata.st_mode & libc::S_IFMT != libc::S_IFREG || metadata.st_nlink != 1 {
            return Err(publication_error(
                &self.destination_path(),
                PublicationStage::Publish,
                "published output is not an independent regular file",
            ));
        }
        Ok(())
    }

    fn create_temporary(&mut self) -> Result<File, ArtifactError> {
        let fd = unsafe {
            libc::openat(
                self.directory.as_raw_fd(),
                self.temporary_leaf.as_ptr(),
                libc::O_CREAT | libc::O_EXCL | libc::O_RDWR | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                0o600,
            )
        };
        if fd < 0 {
            return Err(publication_error(
                &self.temporary_path(),
                PublicationStage::CreateTemporary,
                io::Error::last_os_error(),
            ));
        }
        self.temporary_exists = true;
        Ok(unsafe { File::from_raw_fd(fd) })
    }

    #[cfg(target_os = "macos")]
    fn try_fast_clone(&mut self, source: &File) -> Result<Option<File>, ArtifactError> {
        let result = unsafe {
            libc::fclonefileat(
                source.as_raw_fd(),
                self.directory.as_raw_fd(),
                self.temporary_leaf.as_ptr(),
                0,
            )
        };
        if result != 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error().is_some_and(|code| {
                code == libc::EXDEV
                    || code == libc::ENOTSUP
                    || code == libc::EACCES
                    || code == libc::EPERM
            }) {
                return Ok(None);
            }
            return Err(publication_error(
                &self.temporary_path(),
                PublicationStage::Clone,
                error,
            ));
        }
        self.temporary_exists = true;
        let fd = unsafe {
            libc::openat(
                self.directory.as_raw_fd(),
                self.temporary_leaf.as_ptr(),
                libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        if fd < 0 {
            return Err(publication_error(
                &self.temporary_path(),
                PublicationStage::Clone,
                io::Error::last_os_error(),
            ));
        }
        if unsafe { libc::fchmod(fd, 0o600) } != 0 {
            let error = io::Error::last_os_error();
            unsafe { libc::close(fd) };
            return Err(publication_error(
                &self.temporary_path(),
                PublicationStage::Clone,
                error,
            ));
        }
        Ok(Some(unsafe { File::from_raw_fd(fd) }))
    }

    #[cfg(target_os = "linux")]
    fn try_fast_clone(&mut self, source: &File) -> Result<Option<File>, ArtifactError> {
        const FICLONE: libc::c_ulong = 0x4004_9409;
        let file = self.create_temporary()?;
        if unsafe { libc::ioctl(file.as_raw_fd(), FICLONE, source.as_raw_fd()) } == 0 {
            return Ok(Some(file));
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error().is_some_and(|code| {
            code == libc::EXDEV
                || code == libc::EOPNOTSUPP
                || code == libc::ENOTTY
                || code == libc::EINVAL
        }) {
            drop(file);
            self.cleanup_temporary().map_err(|cleanup| {
                publication_error(&self.temporary_path(), PublicationStage::Cleanup, cleanup)
            })?;
            return Ok(None);
        }
        Err(publication_error(
            &self.temporary_path(),
            PublicationStage::Clone,
            error,
        ))
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    fn try_fast_clone(&mut self, _source: &File) -> Result<Option<File>, ArtifactError> {
        Ok(None)
    }

    fn cleanup_temporary(&mut self) -> io::Result<()> {
        if self.temporary_exists {
            if unsafe {
                libc::unlinkat(self.directory.as_raw_fd(), self.temporary_leaf.as_ptr(), 0)
            } != 0
            {
                return Err(io::Error::last_os_error());
            }
            self.temporary_exists = false;
        }
        Ok(())
    }
}

fn validate_destination(
    directory: &File,
    display: &Path,
    leaf: &CStr,
) -> Result<(), ArtifactError> {
    match metadata_at(directory, leaf) {
        Ok(metadata) if metadata.st_mode & libc::S_IFMT != libc::S_IFREG => Err(publication_error(
            &display.join(leaf.to_string_lossy().as_ref()),
            PublicationStage::ValidateDestination,
            "existing publication target is not a regular file",
        )),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(publication_error(
            &display.join(leaf.to_string_lossy().as_ref()),
            PublicationStage::ValidateDestination,
            error,
        )),
    }
}

fn metadata_at(directory: &File, leaf: &CStr) -> io::Result<libc::stat> {
    let mut metadata = MaybeUninit::<libc::stat>::uninit();
    if unsafe {
        libc::fstatat(
            directory.as_raw_fd(),
            leaf.as_ptr(),
            metadata.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    } != 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { metadata.assume_init() })
}

fn open_authority_source(path: &Path, stream: &StreamInfo) -> Result<File, ArtifactError> {
    let mut options = OpenOptions::new();
    options.read(true);
    options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    let file = options.open(path).map_err(|error| ArtifactError::Io {
        path: path.to_owned(),
        message: error.to_string(),
    })?;
    let metadata = file.metadata().map_err(|error| ArtifactError::Io {
        path: path.to_owned(),
        message: error.to_string(),
    })?;
    reject_hardlink(path, &metadata)?;
    verify_private_file_mode(path, &metadata, true)?;
    verify_content(&file, path, stream.bytes, stream.sha256)?;
    Ok(file)
}

fn copy_file_descriptor(
    source: &File,
    destination: &mut File,
    source_path: &Path,
    destination_path: &Path,
) -> Result<(), ArtifactError> {
    let mut source = source
        .try_clone()
        .map_err(|error| publication_error(source_path, PublicationStage::Copy, error))?;
    source
        .seek(SeekFrom::Start(0))
        .map_err(|error| publication_error(source_path, PublicationStage::Copy, error))?;
    let mut buffer = [0_u8; COPY_BUFFER_BYTES];
    loop {
        let read = source
            .read(&mut buffer)
            .map_err(|error| publication_error(source_path, PublicationStage::Copy, error))?;
        if read == 0 {
            break;
        }
        destination
            .write_all(&buffer[..read])
            .map_err(|error| publication_error(destination_path, PublicationStage::Copy, error))?;
    }
    Ok(())
}

fn verify_content(
    file: &File,
    path: &Path,
    expected_bytes: u64,
    expected_sha256: Sha256Digest,
) -> Result<(), ArtifactError> {
    let mut file = file
        .try_clone()
        .map_err(|error| publication_error(path, PublicationStage::Sync, error))?;
    file.seek(SeekFrom::Start(0))
        .map_err(|error| publication_error(path, PublicationStage::Sync, error))?;
    let mut hasher = Sha256::new();
    let mut bytes = 0_u64;
    let mut buffer = [0_u8; COPY_BUFFER_BYTES];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| publication_error(path, PublicationStage::Sync, error))?;
        if read == 0 {
            break;
        }
        bytes = bytes.saturating_add(read as u64);
        hasher.update(&buffer[..read]);
    }
    if bytes != expected_bytes
        || Sha256Digest::from_bytes(hasher.finalize().into()) != expected_sha256
    {
        return Err(publication_error(
            path,
            PublicationStage::Sync,
            "materialized publication does not match sealed stream",
        ));
    }
    Ok(())
}

fn publish_relative(
    directory: &File,
    temporary: &CStr,
    destination: &CStr,
    policy: PublicationPolicy,
    destination_path: &Path,
) -> Result<(), ArtifactError> {
    let directory_fd = directory.as_raw_fd();
    let result = match policy {
        PublicationPolicy::Replace => unsafe {
            libc::renameat(
                directory_fd,
                temporary.as_ptr(),
                directory_fd,
                destination.as_ptr(),
            )
        },
        PublicationPolicy::CreateNew => rename_noreplace(directory_fd, temporary, destination)?,
    };
    if result != 0 {
        return Err(publication_error(
            destination_path,
            PublicationStage::Publish,
            io::Error::last_os_error(),
        ));
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn rename_noreplace(
    directory_fd: libc::c_int,
    temporary: &CStr,
    destination: &CStr,
) -> Result<libc::c_int, ArtifactError> {
    Ok(unsafe {
        libc::renameatx_np(
            directory_fd,
            temporary.as_ptr(),
            directory_fd,
            destination.as_ptr(),
            libc::RENAME_EXCL,
        )
    })
}

#[cfg(target_os = "linux")]
fn rename_noreplace(
    directory_fd: libc::c_int,
    temporary: &CStr,
    destination: &CStr,
) -> Result<libc::c_int, ArtifactError> {
    const RENAME_NOREPLACE: libc::c_uint = 1;
    Ok(unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            directory_fd,
            temporary.as_ptr(),
            directory_fd,
            destination.as_ptr(),
            RENAME_NOREPLACE,
        ) as libc::c_int
    })
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn rename_noreplace(
    _directory_fd: libc::c_int,
    _temporary: &CStr,
    destination: &CStr,
) -> Result<libc::c_int, ArtifactError> {
    Err(publication_error(
        Path::new(destination.to_string_lossy().as_ref()),
        PublicationStage::Publish,
        "atomic create-new publication is unsupported on this platform",
    ))
}
