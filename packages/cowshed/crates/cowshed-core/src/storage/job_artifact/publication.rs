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
const PROTECTED_DIRECTORY: &[u8] = b".cowshed";
static NONCE: AtomicU64 = AtomicU64::new(1);

pub(super) fn publish(
    workspace_root: &Path,
    stream: &StreamInfo,
    publication: &OutputPublication,
) -> Result<(), ArtifactError> {
    stream.validate()?;
    let parent = Parent::open(workspace_root, publication.path.as_path())?;
    materialize_and_publish(parent, workspace_root, stream, publication.policy)
}

fn materialize_and_publish(
    mut parent: Parent,
    workspace_root: &Path,
    stream: &StreamInfo,
    policy: PublicationPolicy,
) -> Result<(), ArtifactError> {
    if let Err(primary) = parent.materialize_and_publish(workspace_root, stream, policy) {
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
            || components.first().is_some_and(|component| {
                component
                    .as_os_str()
                    .as_bytes()
                    .eq_ignore_ascii_case(PROTECTED_DIRECTORY)
            })
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
        let protected = ProtectedDirectory::open(&directory, workspace_root)?;
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
            let next = unsafe { File::from_raw_fd(fd) };
            let component_path = display.join(component.as_os_str());
            if let Some(protected) = &protected {
                protected.reject_alias(&next, &component_path)?;
            }
            directory = next;
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FileIdentity {
    device: libc::dev_t,
    inode: libc::ino_t,
}

struct ProtectedDirectory {
    _directory: File,
    identity: FileIdentity,
}

impl ProtectedDirectory {
    fn open(workspace: &File, workspace_root: &Path) -> Result<Option<Self>, ArtifactError> {
        let name = c".cowshed";
        let fd = unsafe {
            libc::openat(
                workspace.as_raw_fd(),
                name.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        if fd < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::NotFound {
                return Ok(None);
            }
            return Err(publication_error(
                &workspace_root.join(".cowshed"),
                PublicationStage::ValidateDestination,
                error,
            ));
        }
        let directory = unsafe { File::from_raw_fd(fd) };
        let identity = file_identity(&directory).map_err(|error| {
            publication_error(
                &workspace_root.join(".cowshed"),
                PublicationStage::ValidateDestination,
                error,
            )
        })?;
        Ok(Some(Self {
            _directory: directory,
            identity,
        }))
    }

    fn reject_alias(&self, directory: &File, path: &Path) -> Result<(), ArtifactError> {
        let identity = file_identity(directory).map_err(|error| {
            publication_error(path, PublicationStage::ValidateDestination, error)
        })?;
        if identity == self.identity {
            return Err(publication_error(
                path,
                PublicationStage::ValidateDestination,
                "publication parent aliases protected .cowshed storage",
            ));
        }
        Ok(())
    }
}

fn file_identity(file: &File) -> io::Result<FileIdentity> {
    let mut metadata = MaybeUninit::<libc::stat>::uninit();
    if unsafe { libc::fstat(file.as_raw_fd(), metadata.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let metadata = unsafe { metadata.assume_init() };
    Ok(FileIdentity {
        device: metadata.st_dev,
        inode: metadata.st_ino,
    })
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

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;
    use crate::api::dto::{BinaryData, OutputStorage, OutputSummary, WorkspacePath};

    static TEST_NONCE: AtomicU64 = AtomicU64::new(1);

    struct TestWorkspace {
        root: PathBuf,
    }

    impl TestWorkspace {
        fn new(label: &str) -> Self {
            let nonce = TEST_NONCE.fetch_add(1, Ordering::Relaxed);
            let root = std::env::temp_dir().join(format!(
                "cowshed-publication-{label}-{}-{nonce}",
                std::process::id()
            ));
            fs::create_dir(&root).expect("create test workspace");
            Self { root }
        }

        fn path(&self) -> &Path {
            &self.root
        }
    }

    impl Drop for TestWorkspace {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    fn inline_stream(data: &[u8]) -> StreamInfo {
        StreamInfo {
            storage: OutputStorage::Captured {
                artifact: ProtectedOutput::Inline {
                    data: BinaryData::new(data.to_vec()).expect("bounded test data"),
                },
            },
            bytes: data.len() as u64,
            sha256: Sha256Digest::compute(data),
            summary: OutputSummary {
                version: 1,
                text: String::new(),
                truncated: false,
            },
        }
    }

    fn output(path: &str, policy: PublicationPolicy) -> OutputPublication {
        OutputPublication {
            path: WorkspacePath::new(path).expect("valid publication path"),
            policy,
        }
    }

    fn create_file(path: &Path, data: &[u8]) -> File {
        let mut options = OpenOptions::new();
        options.create_new(true).read(true).write(true).mode(0o600);
        let mut file = options.open(path).expect("create test file");
        file.write_all(data).expect("write test file");
        file
    }

    fn assert_publication_error(
        error: ArtifactError,
        expected_path: &Path,
        expected_stage: PublicationStage,
        expected_message: &str,
    ) {
        match error {
            ArtifactError::Publication {
                path,
                stage,
                message,
            } => {
                assert_eq!(path, expected_path);
                assert_eq!(stage, expected_stage);
                assert!(
                    message.contains(expected_message),
                    "expected {message:?} to contain {expected_message:?}"
                );
            }
            other => panic!("expected publication error, got {other:?}"),
        }
    }

    fn assert_no_temporary_files(directory: &Path) {
        let entries = fs::read_dir(directory).expect("read publication parent");
        for entry in entries {
            let name = entry
                .expect("read directory entry")
                .file_name()
                .to_string_lossy()
                .into_owned();
            assert!(
                !name.contains(".cowshed-publish-"),
                "temporary publication file leaked: {name}"
            );
        }
    }

    #[test]
    fn rejects_ascii_case_variants_of_protected_first_component_when_absent() {
        let workspace = TestWorkspace::new("reserved-case");
        for relative in [
            ".cowshed/result",
            ".COWSHED/result",
            ".CowShed/result",
            ".cOwShEd",
        ] {
            let error = Parent::open(workspace.path(), Path::new(relative))
                .err()
                .expect("reserved first component must be rejected");
            assert_publication_error(
                error,
                Path::new(relative),
                PublicationStage::ValidateDestination,
                "protected .cowshed storage",
            );
        }
    }

    #[test]
    fn rejects_actual_protected_inode_reached_during_component_walk() {
        let workspace = TestWorkspace::new("inode-alias");
        fs::create_dir(workspace.path().join(".cowshed")).expect("create protected directory");
        fs::create_dir(workspace.path().join("safe")).expect("create safe directory");

        let relative = Path::new("safe/../.cowshed/result");
        let error = Parent::open(workspace.path(), relative)
            .err()
            .expect("protected inode reached through another walk must be rejected");
        assert_publication_error(
            error,
            &workspace.path().join("safe/../.cowshed"),
            PublicationStage::ValidateDestination,
            "aliases protected .cowshed storage",
        );
        assert_no_temporary_files(workspace.path().join(".cowshed").as_path());
    }

    #[test]
    fn component_walk_rejects_symlink_and_non_directory_and_sets_cloexec() {
        let workspace = TestWorkspace::new("component-walk");
        fs::create_dir(workspace.path().join("real")).expect("create real parent");
        symlink("real", workspace.path().join("linked")).expect("create parent symlink");
        fs::write(workspace.path().join("plain"), b"not a directory")
            .expect("create non-directory parent");

        for component in ["linked", "plain"] {
            let relative = PathBuf::from(component).join("result");
            let error = Parent::open(workspace.path(), &relative)
                .err()
                .expect("unsafe parent component must be rejected");
            assert_publication_error(
                error,
                &workspace.path().join(component),
                PublicationStage::ValidateDestination,
                "",
            );
        }

        let parent = Parent::open(workspace.path(), Path::new("real/result"))
            .expect("open regular parent directory");
        let descriptor_flags = unsafe { libc::fcntl(parent.directory.as_raw_fd(), libc::F_GETFD) };
        assert!(descriptor_flags >= 0, "read parent descriptor flags");
        assert_ne!(descriptor_flags & libc::FD_CLOEXEC, 0);
        assert!(
            parent
                .directory
                .metadata()
                .expect("parent metadata")
                .is_dir()
        );
    }

    #[test]
    fn rejects_existing_non_regular_destination() {
        let workspace = TestWorkspace::new("non-regular-destination");
        fs::create_dir(workspace.path().join("parent")).expect("create parent");
        fs::create_dir(workspace.path().join("parent/result")).expect("create destination dir");

        let error = Parent::open(workspace.path(), Path::new("parent/result"))
            .err()
            .expect("non-regular destination must be rejected");
        assert_publication_error(
            error,
            &workspace.path().join("parent/result"),
            PublicationStage::ValidateDestination,
            "not a regular file",
        );
    }

    #[test]
    fn temporary_creation_is_exclusive_private_cloexec_and_cleanup_removes_it() {
        let workspace = TestWorkspace::new("temporary");
        fs::create_dir(workspace.path().join("parent")).expect("create parent");
        let mut parent = Parent::open(workspace.path(), Path::new("parent/result"))
            .expect("open publication parent");
        let temporary_path = parent.temporary_path();

        let temporary = parent.create_temporary().expect("create temporary");
        let metadata = temporary.metadata().expect("temporary metadata");
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
        assert_eq!(metadata.nlink(), 1);
        let descriptor_flags = unsafe { libc::fcntl(temporary.as_raw_fd(), libc::F_GETFD) };
        assert!(descriptor_flags >= 0, "read temporary descriptor flags");
        assert_ne!(descriptor_flags & libc::FD_CLOEXEC, 0);

        let error = parent
            .create_temporary()
            .expect_err("O_EXCL must reject an existing temporary");
        assert_publication_error(
            error,
            &temporary_path,
            PublicationStage::CreateTemporary,
            "",
        );
        drop(temporary);
        parent.cleanup_temporary().expect("cleanup temporary");
        assert!(!temporary_path.exists());
    }

    #[test]
    fn verify_content_rejects_length_and_digest_mismatches() {
        let workspace = TestWorkspace::new("verify-content");
        let path = workspace.path().join("materialized");
        let file = create_file(&path, b"sealed bytes");
        let digest = Sha256Digest::compute(b"sealed bytes");

        let error = verify_content(&file, &path, 13, digest)
            .expect_err("incorrect byte count must fail verification");
        assert_publication_error(
            error,
            &path,
            PublicationStage::Sync,
            "does not match sealed stream",
        );

        let error = verify_content(
            &file,
            &path,
            b"sealed bytes".len() as u64,
            Sha256Digest::compute(b"different"),
        )
        .expect_err("incorrect digest must fail verification");
        assert_publication_error(
            error,
            &path,
            PublicationStage::Sync,
            "does not match sealed stream",
        );
    }

    #[test]
    fn content_verification_failure_cleans_temporary_file() {
        let workspace = TestWorkspace::new("verification-cleanup");
        fs::create_dir(workspace.path().join("parent")).expect("create parent");
        let parent = Parent::open(workspace.path(), Path::new("parent/result"))
            .expect("open publication parent");
        let temporary_path = parent.temporary_path();
        let destination_path = parent.destination_path();
        let mut stream = inline_stream(b"sealed bytes");
        stream.sha256 = Sha256Digest::compute(b"different");

        let error = materialize_and_publish(
            parent,
            workspace.path(),
            &stream,
            PublicationPolicy::CreateNew,
        )
        .expect_err("verification mismatch must fail publication");
        assert_publication_error(
            error,
            &temporary_path,
            PublicationStage::Sync,
            "does not match sealed stream",
        );
        assert!(!temporary_path.exists());
        assert!(!destination_path.exists());
        assert_no_temporary_files(workspace.path().join("parent").as_path());
    }

    #[test]
    fn descriptor_copy_rewinds_and_copies_exact_bytes() {
        let workspace = TestWorkspace::new("descriptor-copy");
        let source_path = workspace.path().join("source");
        let destination_path = workspace.path().join("destination");
        let mut source = create_file(&source_path, b"copy me exactly");
        source.seek(SeekFrom::End(0)).expect("move source cursor");
        let mut destination = create_file(&destination_path, b"");

        copy_file_descriptor(&source, &mut destination, &source_path, &destination_path)
            .expect("copy file descriptor");
        drop(destination);
        assert_eq!(
            fs::read(&destination_path).expect("read copied file"),
            b"copy me exactly"
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn fast_clone_unsupported_error_falls_back_without_leaking_temporary() {
        let workspace = TestWorkspace::new("clone-fallback");
        let mut parent =
            Parent::open(workspace.path(), Path::new("result")).expect("open publication parent");
        let temporary_path = parent.temporary_path();
        let cross_volume_source =
            File::open("/bin/sh").expect("open sealed system-volume clone source");

        assert!(
            parent
                .try_fast_clone(&cross_volume_source)
                .expect("cross-volume clone must be a fallback")
                .is_none()
        );
        assert!(!temporary_path.exists());
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn fast_clone_fatal_error_is_typed_and_does_not_remove_existing_path() {
        let workspace = TestWorkspace::new("clone-fatal");
        let source_path = workspace.path().join("source");
        let source = create_file(&source_path, b"source");
        let mut parent =
            Parent::open(workspace.path(), Path::new("result")).expect("open publication parent");
        let temporary_path = parent.temporary_path();
        fs::write(&temporary_path, b"preexisting").expect("occupy clone destination");

        let error = parent
            .try_fast_clone(&source)
            .expect_err("unexpected clone failure must be fatal");
        assert_publication_error(error, &temporary_path, PublicationStage::Clone, "");
        assert_eq!(
            fs::read(&temporary_path).expect("read preexisting path"),
            b"preexisting"
        );
    }

    #[test]
    fn create_new_preserves_existing_destination_and_cleans_temporary() {
        let workspace = TestWorkspace::new("create-new");
        fs::write(workspace.path().join("result"), b"existing").expect("create destination");

        let error = publish(
            workspace.path(),
            &inline_stream(b"replacement"),
            &output("result", PublicationPolicy::CreateNew),
        )
        .expect_err("create-new must reject an existing destination");
        assert_publication_error(
            error,
            &workspace.path().join("result"),
            PublicationStage::Publish,
            "",
        );
        assert_eq!(
            fs::read(workspace.path().join("result")).expect("read existing destination"),
            b"existing"
        );
        assert_no_temporary_files(workspace.path());
    }

    #[test]
    fn replace_atomically_publishes_verified_independent_file() {
        let workspace = TestWorkspace::new("replace");
        fs::write(workspace.path().join("result"), b"existing").expect("create destination");

        publish(
            workspace.path(),
            &inline_stream(b"replacement"),
            &output("result", PublicationPolicy::Replace),
        )
        .expect("replace publication");

        let destination = workspace.path().join("result");
        assert_eq!(
            fs::read(&destination).expect("read destination"),
            b"replacement"
        );
        let metadata = fs::metadata(&destination).expect("destination metadata");
        assert!(metadata.is_file());
        assert_eq!(metadata.nlink(), 1);
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
        assert_no_temporary_files(workspace.path());
    }
}
