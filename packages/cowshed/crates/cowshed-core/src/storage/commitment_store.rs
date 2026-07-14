use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{CStr, CString, OsString};
use std::fmt;
use std::fs::{self, File};
use std::io::{self, Cursor, Read, Write};
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::MetadataExt;
use std::path::Path;

use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use thiserror::Error;
use uuid::Uuid;

use crate::api::dto::ControllerCommitment;
use crate::metadata::WorkspaceIncarnation;
use crate::repository::RepoId;
use crate::storage::job_artifact::{
    CommitmentPriorContext, controller_commitments_from_batch, controller_commitments_to_batch,
    validate_commitments,
};

const LOCK_NAME: &[u8] = b".commitment-store.lock";
const SEGMENT_PREFIX: &[u8] = b"commitment-";
const SEGMENT_SUFFIX: &[u8] = b".arrow";
const MAX_COMMITMENT_SEGMENT_BYTES: u64 = 1024 * 1024;

/// A validated UTC calendar date used as a telemetry partition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommitmentDate {
    year: u16,
    month: u8,
    day: u8,
}

impl CommitmentDate {
    pub fn new(year: u16, month: u8, day: u8) -> Result<Self, CommitmentStoreError> {
        if valid_calendar_date(year, month, day) {
            Ok(Self { year, month, day })
        } else {
            Err(CommitmentStoreError::Integrity {
                message: "invalid UTC commitment date".into(),
            })
        }
    }

    fn parse(value: &[u8]) -> Option<Self> {
        if value.len() != 10 || value[4] != b'-' || value[7] != b'-' {
            return None;
        }
        let year = parse_digits(&value[..4])? as u16;
        let month = parse_digits(&value[5..7])? as u8;
        let day = parse_digits(&value[8..])? as u8;
        valid_calendar_date(year, month, day).then_some(Self { year, month, day })
    }
}

impl fmt::Display for CommitmentDate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{:04}-{:02}-{:02}",
            self.year, self.month, self.day
        )
    }
}

/// Publication checkpoints exposed only to make crash behavior deterministic under test.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommitmentPublicationPoint {
    BeforeRename,
    AfterRenameAndDirectorySync,
}

/// The clock and durability operations used by [`CommitmentStore`].
///
/// Production callers use [`CommitmentStore::open`]. This seam lets focused tests inject a UTC
/// date and failures at the two crash-relevant publication boundaries.
pub trait CommitmentStoreEnvironment: Send {
    fn utc_date(&self) -> io::Result<CommitmentDate>;

    fn sync_directory(&self, directory: &File) -> io::Result<()> {
        directory.sync_all()
    }

    fn publication_point(&self, _point: CommitmentPublicationPoint) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum CommitmentStoreError {
    #[error("commitment store I/O failed during {operation}: {source}")]
    Io {
        operation: &'static str,
        #[source]
        source: io::Error,
    },
    #[error("commitment store integrity failure: {message}")]
    Integrity { message: String },
    #[error("commitment order {order} conflicts with an existing publication")]
    Conflict { order: u64 },
}

/// Actor-owned durable controller commitment state.
///
/// The store is deliberately not `Clone`: one runtime actor owns its validation context and writer
/// identity. Immutable segment publication is still serialized against other controller processes.
pub struct CommitmentStore {
    root: File,
    baseline: CommitmentPriorContext,
    context: CommitmentPriorContext,
    writer_id: Uuid,
    environment: Box<dyn CommitmentStoreEnvironment>,
}

impl CommitmentStore {
    pub fn open(
        telemetry_root: impl AsRef<Path>,
        repo_id: RepoId,
        known_incarnations: impl IntoIterator<Item = WorkspaceIncarnation>,
    ) -> Result<Self, CommitmentStoreError> {
        Self::open_with_environment(
            telemetry_root,
            repo_id,
            known_incarnations,
            Box::new(SystemEnvironment),
        )
    }

    #[doc(hidden)]
    pub fn open_with_environment(
        telemetry_root: impl AsRef<Path>,
        repo_id: RepoId,
        known_incarnations: impl IntoIterator<Item = WorkspaceIncarnation>,
        environment: Box<dyn CommitmentStoreEnvironment>,
    ) -> Result<Self, CommitmentStoreError> {
        let root = open_or_create_directory_chain(telemetry_root.as_ref())?;
        let baseline = CommitmentPriorContext::new(repo_id, known_incarnations);
        let lock = acquire_lock(&root)?;
        let context = recover_context(&root, &baseline)?;
        drop(lock);
        Ok(Self {
            root,
            baseline,
            context,
            writer_id: Uuid::new_v4(),
            environment,
        })
    }

    pub fn next_order(&self) -> Result<u64, CommitmentStoreError> {
        self.context
            .last_order()
            .checked_add(1)
            .ok_or_else(|| CommitmentStoreError::Integrity {
                message: "controller commitment order overflow".into(),
            })
    }

    pub fn writer_id(&self) -> Uuid {
        self.writer_id
    }

    pub(crate) fn workspace_is_introduced(
        &self,
        repo_id: &RepoId,
        incarnation: &WorkspaceIncarnation,
    ) -> bool {
        self.context.is_introduced(repo_id, incarnation)
    }

    pub(crate) fn workspace_is_retired(
        &self,
        repo_id: &RepoId,
        incarnation: &WorkspaceIncarnation,
    ) -> bool {
        self.context.is_retired(repo_id, incarnation)
    }

    pub(crate) fn admitted_lifecycle_incarnations(
        &self,
        repo_id: &RepoId,
    ) -> BTreeSet<WorkspaceIncarnation> {
        self.context.admitted_lifecycle_incarnations(repo_id)
    }

    pub fn refresh(&mut self) -> Result<(), CommitmentStoreError> {
        let lock = acquire_lock(&self.root)?;
        self.context = recover_context(&self.root, &self.baseline)?;
        drop(lock);
        Ok(())
    }

    pub fn publish(
        &mut self,
        commitment: ControllerCommitment,
    ) -> Result<(), CommitmentStoreError> {
        let order = commitment.order();
        let lock = acquire_lock(&self.root)?;
        let recovered = recover_context(&self.root, &self.baseline)?;
        if recovered.last_order() != self.context.last_order() {
            self.context = recovered;
            return Err(CommitmentStoreError::Conflict { order });
        }

        let next_context = validate_commitments(&recovered, std::slice::from_ref(&commitment))
            .map_err(|error| integrity(error.to_string()))?;
        let batch = controller_commitments_to_batch(std::slice::from_ref(&commitment))
            .map_err(|error| integrity(error.to_string()))?;
        let date = self
            .environment
            .utc_date()
            .map_err(|source| io_failure("reading UTC date", source))?;
        let (date_directory, created) =
            open_or_create_child_directory(&self.root, &date.to_string())?;
        if created {
            self.environment
                .sync_directory(&self.root)
                .map_err(|source| io_failure("syncing telemetry root", source))?;
        }

        let sealed_name = segment_name(order, self.writer_id);
        let temporary_name = format!(
            ".commitment-{order:020}-{}-{}.tmp",
            self.writer_id.hyphenated(),
            Uuid::new_v4().hyphenated()
        );
        let temporary = CString::new(temporary_name.as_bytes())
            .map_err(|_| integrity("temporary segment name contains NUL"))?;
        let sealed = CString::new(sealed_name.as_bytes())
            .map_err(|_| integrity("sealed segment name contains NUL"))?;
        let mut file = create_new_file_at(&date_directory, &temporary)?;
        let mut cleanup = TemporaryCleanup::new(date_directory.as_raw_fd(), temporary.clone());
        {
            let mut writer = StreamWriter::try_new(&mut file, &batch.schema())
                .map_err(|error| integrity(error.to_string()))?;
            writer
                .write(&batch)
                .map_err(|error| integrity(error.to_string()))?;
            writer
                .finish()
                .map_err(|error| integrity(error.to_string()))?;
        }
        file.flush()
            .map_err(|source| io_failure("flushing commitment segment", source))?;
        file.sync_all()
            .map_err(|source| io_failure("syncing commitment segment", source))?;
        drop(file);

        self.environment
            .publication_point(CommitmentPublicationPoint::BeforeRename)
            .map_err(|source| io_failure("before commitment rename", source))?;
        match rename_noreplace(
            date_directory.as_raw_fd(),
            temporary.as_c_str(),
            sealed.as_c_str(),
        ) {
            Ok(()) => cleanup.disarm(),
            Err(source) if source.kind() == io::ErrorKind::AlreadyExists => {
                self.context = recover_context(&self.root, &self.baseline)?;
                return Err(CommitmentStoreError::Conflict { order });
            }
            Err(source) => return Err(io_failure("publishing commitment segment", source)),
        }
        self.environment
            .sync_directory(&date_directory)
            .map_err(|source| io_failure("syncing commitment directory", source))?;
        self.environment
            .publication_point(CommitmentPublicationPoint::AfterRenameAndDirectorySync)
            .map_err(|source| io_failure("after commitment rename", source))?;

        self.context = next_context;
        drop(lock);
        Ok(())
    }
}

struct SystemEnvironment;

impl CommitmentStoreEnvironment for SystemEnvironment {
    fn utc_date(&self) -> io::Result<CommitmentDate> {
        let mut timestamp: libc::time_t = 0;
        if unsafe { libc::time(&mut timestamp) } == -1 {
            return Err(io::Error::last_os_error());
        }
        let mut broken_down = std::mem::MaybeUninit::<libc::tm>::uninit();
        if unsafe { libc::gmtime_r(&timestamp, broken_down.as_mut_ptr()) }.is_null() {
            return Err(io::Error::last_os_error());
        }
        let broken_down = unsafe { broken_down.assume_init() };
        let year = u16::try_from(broken_down.tm_year + 1900)
            .map_err(|_| io::Error::other("UTC year is outside the supported range"))?;
        let month = u8::try_from(broken_down.tm_mon + 1)
            .map_err(|_| io::Error::other("UTC month is outside the supported range"))?;
        let day = u8::try_from(broken_down.tm_mday)
            .map_err(|_| io::Error::other("UTC day is outside the supported range"))?;
        CommitmentDate::new(year, month, day).map_err(|error| io::Error::other(error.to_string()))
    }
}

fn recover_context(
    root: &File,
    baseline: &CommitmentPriorContext,
) -> Result<CommitmentPriorContext, CommitmentStoreError> {
    let mut segments = BTreeMap::<u64, (File, OsString)>::new();
    for date_name in directory_names(root)? {
        let Some(date) = CommitmentDate::parse(date_name.as_bytes()) else {
            continue;
        };
        if date.to_string().as_bytes() != date_name.as_bytes() {
            continue;
        }
        let date_directory = open_existing_child_directory(root, &date_name)
            .map_err(|error| integrity(format!("invalid commitment date directory: {error}")))?;
        for name in directory_names(&date_directory)? {
            let bytes = name.as_bytes();
            let Some((order, _writer)) = parse_segment_name(bytes) else {
                if bytes.starts_with(SEGMENT_PREFIX) {
                    return Err(integrity("malformed commitment segment name"));
                }
                continue;
            };
            let segment = open_existing_file_at(&date_directory, &name)
                .map_err(|error| integrity(format!("invalid commitment segment: {error}")))?;
            let metadata = segment
                .metadata()
                .map_err(|source| io_failure("reading commitment metadata", source))?;
            if !metadata.file_type().is_file() {
                return Err(integrity("commitment segment is not a regular file"));
            }
            if metadata.nlink() != 1 {
                return Err(integrity("commitment segment has more than one hard link"));
            }
            if metadata.len() > MAX_COMMITMENT_SEGMENT_BYTES {
                return Err(integrity("commitment segment exceeds the size limit"));
            }
            if segments.insert(order, (segment, name)).is_some() {
                return Err(integrity("duplicate controller commitment order"));
            }
        }
    }

    let mut context = CommitmentPriorContext::empty();
    let mut expected_order = 1_u64;
    for (order, (segment, _name)) in segments {
        if order != expected_order {
            return Err(integrity("controller commitment order is not contiguous"));
        }
        let mut bytes = Vec::new();
        segment
            .take(MAX_COMMITMENT_SEGMENT_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|source| io_failure("reading commitment segment", source))?;
        if bytes.len() as u64 > MAX_COMMITMENT_SEGMENT_BYTES {
            return Err(integrity("commitment segment exceeds the size limit"));
        }
        let batch = decode_one_batch(&bytes)?;
        if batch.num_rows() != 1 {
            return Err(integrity(
                "commitment segment must contain exactly one commitment row",
            ));
        }
        let commitments = controller_commitments_from_batch(&batch, &context)
            .map_err(|error| integrity(error.to_string()))?;
        if commitments.len() != 1 || commitments[0].order() != order {
            return Err(integrity(
                "commitment segment name does not match its single row",
            ));
        }
        context = validate_commitments(&context, &commitments)
            .map_err(|error| integrity(error.to_string()))?;
        expected_order = expected_order
            .checked_add(1)
            .ok_or_else(|| integrity("controller commitment order overflow"))?;
    }
    context.merge_verified_active(baseline);
    Ok(context)
}

fn decode_one_batch(bytes: &[u8]) -> Result<arrow_array::RecordBatch, CommitmentStoreError> {
    let mut cursor = Cursor::new(bytes);
    let batch = {
        let mut reader = StreamReader::try_new(&mut cursor, None)
            .map_err(|error| integrity(format!("invalid Arrow IPC stream: {error}")))?;
        let batch = reader
            .next()
            .ok_or_else(|| integrity("commitment segment contains no Arrow batch"))?
            .map_err(|error| integrity(format!("invalid Arrow IPC batch: {error}")))?;
        if reader.next().is_some() {
            return Err(integrity(
                "commitment segment contains more than one Arrow batch",
            ));
        }
        batch
    };
    if cursor.position() != bytes.len() as u64 {
        return Err(integrity("commitment segment has trailing bytes"));
    }
    Ok(batch)
}

fn segment_name(order: u64, writer: Uuid) -> String {
    format!("commitment-{order:020}-{}.arrow", writer.hyphenated())
}

fn parse_segment_name(name: &[u8]) -> Option<(u64, Uuid)> {
    let body = name
        .strip_prefix(SEGMENT_PREFIX)?
        .strip_suffix(SEGMENT_SUFFIX)?;
    if body.len() != 57 || body[20] != b'-' {
        return None;
    }
    let order = parse_digits(&body[..20])?;
    if order == 0 || format!("{order:020}").as_bytes() != &body[..20] {
        return None;
    }
    let writer_text = std::str::from_utf8(&body[21..]).ok()?;
    let writer = Uuid::parse_str(writer_text).ok()?;
    if writer.hyphenated().to_string() != writer_text {
        return None;
    }
    Some((order, writer))
}

fn parse_digits(bytes: &[u8]) -> Option<u64> {
    bytes.iter().try_fold(0_u64, |value, byte| {
        byte.is_ascii_digit()
            .then_some(())
            .and_then(|()| value.checked_mul(10))
            .and_then(|value| value.checked_add(u64::from(byte - b'0')))
    })
}

fn valid_calendar_date(year: u16, month: u8, day: u8) -> bool {
    if year == 0 || !(1..=12).contains(&month) {
        return false;
    }
    let leap = year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400));
    let days = match month {
        2 if leap => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    (1..=days).contains(&day)
}

fn open_or_create_directory_chain(path: &Path) -> Result<File, CommitmentStoreError> {
    if path.as_os_str().is_empty() {
        return Err(integrity("telemetry root is empty"));
    }
    fs::create_dir_all(path).map_err(|source| io_failure("creating telemetry root", source))?;
    let path = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| integrity("telemetry root contains NUL"))?;
    let fd = unsafe {
        libc::open(
            path.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        Err(io_failure(
            "opening telemetry root without following links",
            io::Error::last_os_error(),
        ))
    } else {
        Ok(unsafe { File::from_raw_fd(fd) })
    }
}

fn open_or_create_child_directory(
    parent: &File,
    name: &str,
) -> Result<(File, bool), CommitmentStoreError> {
    let name = CString::new(name).map_err(|_| integrity("date directory contains NUL"))?;
    let result = unsafe { libc::mkdirat(parent.as_raw_fd(), name.as_ptr(), 0o700) };
    let created = if result == 0 {
        true
    } else if io::Error::last_os_error().kind() == io::ErrorKind::AlreadyExists {
        false
    } else {
        return Err(io_failure(
            "creating commitment date directory",
            io::Error::last_os_error(),
        ));
    };
    let directory = open_directory_at(parent.as_raw_fd(), name.as_c_str()).map_err(|source| {
        io_failure(
            "opening commitment date directory without following links",
            source,
        )
    })?;
    Ok((directory, created))
}

fn open_existing_child_directory(parent: &File, name: &OsString) -> io::Result<File> {
    let name = CString::new(name.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "directory name contains NUL"))?;
    open_directory_at(parent.as_raw_fd(), name.as_c_str())
}

fn open_directory_at(parent: RawFd, name: &CStr) -> io::Result<File> {
    let fd = unsafe {
        libc::openat(
            parent,
            name.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(unsafe { File::from_raw_fd(fd) })
    }
}

fn acquire_lock(root: &File) -> Result<File, CommitmentStoreError> {
    let name = CString::new(LOCK_NAME).expect("static lock name has no NUL");
    let fd = unsafe {
        libc::openat(
            root.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDWR | libc::O_CREAT | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            0o600,
        )
    };
    if fd < 0 {
        return Err(io_failure(
            "opening commitment publication lock",
            io::Error::last_os_error(),
        ));
    }
    let lock = unsafe { File::from_raw_fd(fd) };
    let metadata = lock
        .metadata()
        .map_err(|source| io_failure("reading commitment lock metadata", source))?;
    if !metadata.file_type().is_file() || metadata.nlink() != 1 {
        return Err(integrity(
            "commitment publication lock is not a private regular file",
        ));
    }
    if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX) } != 0 {
        return Err(io_failure(
            "locking commitment publication",
            io::Error::last_os_error(),
        ));
    }
    Ok(lock)
}

fn create_new_file_at(directory: &File, name: &CStr) -> Result<File, CommitmentStoreError> {
    let fd = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            name.as_ptr(),
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            0o600,
        )
    };
    if fd < 0 {
        Err(io_failure(
            "creating temporary commitment segment",
            io::Error::last_os_error(),
        ))
    } else if unsafe { libc::fchmod(fd, 0o600) } != 0 {
        let source = io::Error::last_os_error();
        unsafe {
            libc::close(fd);
            libc::unlinkat(directory.as_raw_fd(), name.as_ptr(), 0);
        }
        Err(io_failure(
            "setting temporary commitment segment mode",
            source,
        ))
    } else {
        Ok(unsafe { File::from_raw_fd(fd) })
    }
}

fn open_existing_file_at(directory: &File, name: &OsString) -> io::Result<File> {
    let name = CString::new(name.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "segment name contains NUL"))?;
    let fd = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(unsafe { File::from_raw_fd(fd) })
    }
}

#[cfg(target_os = "macos")]
fn rename_noreplace(directory: RawFd, temporary: &CStr, sealed: &CStr) -> io::Result<()> {
    let result = unsafe {
        libc::renameatx_np(
            directory,
            temporary.as_ptr(),
            directory,
            sealed.as_ptr(),
            libc::RENAME_EXCL,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(target_os = "linux")]
fn rename_noreplace(directory: RawFd, temporary: &CStr, sealed: &CStr) -> io::Result<()> {
    const RENAME_NOREPLACE: libc::c_uint = 1;
    let result = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            directory,
            temporary.as_ptr(),
            directory,
            sealed.as_ptr(),
            RENAME_NOREPLACE,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn rename_noreplace(_directory: RawFd, _temporary: &CStr, _sealed: &CStr) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "atomic create-new rename is unsupported",
    ))
}

struct TemporaryCleanup {
    directory: RawFd,
    name: CString,
    armed: bool,
}

impl TemporaryCleanup {
    fn new(directory: RawFd, name: CString) -> Self {
        Self {
            directory,
            name,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for TemporaryCleanup {
    fn drop(&mut self) {
        if self.armed {
            unsafe {
                libc::unlinkat(self.directory, self.name.as_ptr(), 0);
            }
        }
    }
}

fn directory_names(directory: &File) -> Result<Vec<OsString>, CommitmentStoreError> {
    let current = CString::new(".").expect("static directory name has no NUL");
    let independent = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            current.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if independent < 0 {
        return Err(io_failure(
            "opening independent directory descriptor",
            io::Error::last_os_error(),
        ));
    }
    let stream = unsafe { libc::fdopendir(independent) };
    if stream.is_null() {
        unsafe {
            libc::close(independent);
        }
        return Err(io_failure(
            "opening directory stream",
            io::Error::last_os_error(),
        ));
    }
    let stream = DirectoryStream(stream);
    let mut names = Vec::new();
    loop {
        set_errno(0);
        let entry = unsafe { libc::readdir(stream.0) };
        if entry.is_null() {
            let error = current_errno();
            if error != 0 {
                return Err(io_failure(
                    "enumerating commitment directories",
                    io::Error::from_raw_os_error(error),
                ));
            }
            break;
        }
        let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) }.to_bytes();
        if name != b"." && name != b".." {
            names.push(OsString::from_vec(name.to_vec()));
        }
    }
    Ok(names)
}

struct DirectoryStream(*mut libc::DIR);

impl Drop for DirectoryStream {
    fn drop(&mut self) {
        unsafe {
            libc::closedir(self.0);
        }
    }
}

#[cfg(target_os = "macos")]
fn current_errno() -> libc::c_int {
    unsafe { *libc::__error() }
}

#[cfg(target_os = "macos")]
fn set_errno(value: libc::c_int) {
    unsafe {
        *libc::__error() = value;
    }
}

#[cfg(target_os = "linux")]
fn current_errno() -> libc::c_int {
    unsafe { *libc::__errno_location() }
}

#[cfg(target_os = "linux")]
fn set_errno(value: libc::c_int) {
    unsafe {
        *libc::__errno_location() = value;
    }
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn current_errno() -> libc::c_int {
    0
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn set_errno(_value: libc::c_int) {}

fn io_failure(operation: &'static str, source: io::Error) -> CommitmentStoreError {
    CommitmentStoreError::Io { operation, source }
}

fn integrity(message: impl Into<String>) -> CommitmentStoreError {
    CommitmentStoreError::Integrity {
        message: message.into(),
    }
}
