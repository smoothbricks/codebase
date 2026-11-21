//! Linux keeps scoped gateway credentials as record files in one directory of the user's.
//!
//! The store is `cowshed_core::host_dirs::credentials_directory`: owned by the user at mode
//! 0700, one record file per binding at mode 0600. The enrolment functions here write it, finding
//! it from `$HOME`. The gateway reads it through [`SystemdCredentialProvider`], whose unit names
//! the same directory in `$CREDENTIALS_DIRECTORY`, and looks again on every request, so an
//! enrolment or a removal is seen without a restart.
//!
//! The decisions are pure and tested as tables: a binding's file name ([`record_file_name`]),
//! which directory states are refused ([`directory_state`]) and where `$HOME` puts the store
//! ([`store_directory`]). The rest is the file I/O that carries them out, all of it blocking and
//! run through `spawn_blocking`.

use std::ffi::{CStr, CString, OsStr, OsString};
use std::fs::{File, Metadata};
use std::io;
use std::os::fd::AsRawFd as _;
use std::os::unix::ffi::OsStringExt as _;
use std::os::unix::fs::{DirBuilderExt as _, MetadataExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};

use zeroize::Zeroizing;

use cowshed_core::host_dirs::credentials_directory;
use cowshed_os::fsio::{
    TemporaryAt, create_private_file_at, open_directory_nofollow, sync_directory,
    sync_held_directory, temp_name,
};

use crate::interfaces::{
    CredentialError, CredentialProtocol, CredentialProvider, CredentialQuery, CredentialRecord,
};

use super::{
    CredentialPresence, ScopedCredential, account_for, account_key, decode_record, validate_origin,
    validate_query, validate_scope,
};

/// Reads scoped gateway credentials from the record directory its service unit names.
///
/// The directory is `$CREDENTIALS_DIRECTORY`, which the gateway's unit sets to the user's
/// credential store. Each lookup judges the directory and the record again, so what
/// `cowshed credential` wrote or removed since the last request is what this one sees. Lookup
/// occurs only after the gateway actor has admitted the exact request.
#[derive(Clone, Debug)]
pub struct SystemdCredentialProvider {
    directory: PathBuf,
    authorized_uid: u32,
}

impl SystemdCredentialProvider {
    pub fn from_environment() -> Result<Self, CredentialError> {
        let directory = std::env::var_os("CREDENTIALS_DIRECTORY")
            .map(PathBuf::from)
            .ok_or_else(|| {
                CredentialError::Unavailable(
                    "CREDENTIALS_DIRECTORY is required for Linux production credentials".to_owned(),
                )
            })?;
        Self::new(directory, effective_uid())
    }

    pub fn new(directory: PathBuf, authorized_uid: u32) -> Result<Self, CredentialError> {
        validate_directory(&directory, authorized_uid)?;
        Ok(Self {
            directory,
            authorized_uid,
        })
    }

    pub fn account_for(query: &CredentialQuery) -> String {
        account_for(query)
    }
}

#[async_trait::async_trait]
impl CredentialProvider for SystemdCredentialProvider {
    async fn lookup(
        &self,
        query: &CredentialQuery,
    ) -> Result<Option<CredentialRecord>, CredentialError> {
        validate_query(query)?;
        // Enrolment refuses a binding that has no file name, so no record of it can be there.
        let Ok(name) = record_file_name(&query.repo_id, query.protocol, &query.origin) else {
            return Ok(None);
        };
        let directory = self.directory.clone();
        let authorized_uid = self.authorized_uid;
        blocking(move || {
            validate_directory(&directory, authorized_uid)?;
            read_systemd_credential(&directory.join(name), authorized_uid)
        })
        .await
    }
}

/// Install or replace one scoped credential in the user's credential store.
///
/// Enrolment is a trusted-host operation: the secret arrives from the operator's environment,
/// travels in a `Zeroizing` buffer, and reaches the store without passing through argv or any
/// path outside the store directory. Replacing an existing binding is deliberate: rotation is the
/// common case, and the rename that publishes the new record leaves no moment with no credential.
pub async fn store_scoped_credential(credential: ScopedCredential) -> Result<(), CredentialError> {
    store_under(std::env::var_os("HOME"), effective_uid(), credential).await
}

/// Remove one scoped credential. An absent binding is already the requested state.
pub async fn remove_scoped_credential(
    repo_id: &str,
    protocol: CredentialProtocol,
    origin: &str,
) -> Result<CredentialPresence, CredentialError> {
    remove_under(
        std::env::var_os("HOME"),
        effective_uid(),
        repo_id,
        protocol,
        origin,
    )
    .await
}

/// The scope of the installed binding for this origin, or `None` when nothing is enrolled.
///
/// `status` reports what an operator enrolled, and the path prefixes are that answer: they are
/// the non-secret half of the record, and the half that decides which requests the credential
/// can ever ride. The decoded record's secret is dropped, and zeroed, right here rather than
/// travelling into a reporting path.
pub async fn scoped_credential_scope(
    repo_id: &str,
    protocol: CredentialProtocol,
    origin: &str,
) -> Result<Option<Vec<String>>, CredentialError> {
    scope_under(
        std::env::var_os("HOME"),
        effective_uid(),
        repo_id,
        protocol,
        origin,
    )
    .await
}

/// [`store_scoped_credential`] for the store of the user whose home is `home`.
async fn store_under(
    home: Option<OsString>,
    authorized_uid: u32,
    credential: ScopedCredential,
) -> Result<(), CredentialError> {
    validate_scope(&credential)?;
    let name = record_file_name(&credential.repo_id, credential.protocol, &credential.origin)?;
    let payload = credential.to_json()?;
    let directory = store_directory(home.as_deref())?;
    blocking(move || write_record(&directory, authorized_uid, name, payload.as_bytes())).await
}

/// [`remove_scoped_credential`] for the store of the user whose home is `home`.
async fn remove_under(
    home: Option<OsString>,
    authorized_uid: u32,
    repo_id: &str,
    protocol: CredentialProtocol,
    origin: &str,
) -> Result<CredentialPresence, CredentialError> {
    validate_origin(origin)?;
    // Enrolment refuses a binding that has no file name, so no record of it can be there.
    let Ok(name) = record_file_name(repo_id, protocol, origin) else {
        return Ok(CredentialPresence::Absent);
    };
    let directory = store_directory(home.as_deref())?;
    blocking(move || remove_record(&directory, authorized_uid, name)).await
}

/// [`scoped_credential_scope`] for the store of the user whose home is `home`.
async fn scope_under(
    home: Option<OsString>,
    authorized_uid: u32,
    repo_id: &str,
    protocol: CredentialProtocol,
    origin: &str,
) -> Result<Option<Vec<String>>, CredentialError> {
    validate_origin(origin)?;
    // Enrolment refuses a binding that has no file name, so no record of it can be there.
    let Ok(name) = record_file_name(repo_id, protocol, origin) else {
        return Ok(None);
    };
    let directory = store_directory(home.as_deref())?;
    blocking(move || read_record(&directory, authorized_uid, &name))
        .await
        .map(|record| record.map(|record| record.path_prefixes))
}

/// The longest file name the kernel takes (`NAME_MAX`), in bytes.
const MAX_FILE_NAME_BYTES: usize = 255;

/// A binding whose record name is longer than one file name can be.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error(
    "the record name of this binding is {bytes} bytes and a file name holds at most {max}: \
     the repository id and the origin's host are too long together",
    max = MAX_FILE_NAME_BYTES
)]
struct RecordNameTooLong {
    bytes: usize,
}

impl From<RecordNameTooLong> for CredentialError {
    fn from(refusal: RecordNameTooLong) -> Self {
        Self::Unavailable(refusal.to_string())
    }
}

/// The name of a binding's record file: its account key as one file-name component.
///
/// The account key is `v1|<repo_id>|<protocol>|<base64url origin>`. The protocol tag is ASCII
/// letters and the origin is base64url (`A-Z a-z 0-9 - _`), so neither can hold a `/` or a NUL,
/// and `|` is an ordinary file-name byte. The repository id can: cowshed's is `owner/repo`. So
/// `/` is written `%2F`, and `%` and NUL are written `%25` and `%00` so that two bindings never
/// share a name whatever text a caller passes. The key starts `v1|`, so the name is never `.` or
/// `..`.
///
/// Nothing bounds the key's length: a repository id runs to 257 bytes and a host name to 253,
/// which base64url makes a third longer. A name over [`MAX_FILE_NAME_BYTES`] is refused, never
/// truncated: a shortened name could be another binding's.
fn record_file_name(
    repo_id: &str,
    protocol: CredentialProtocol,
    origin: &str,
) -> Result<String, RecordNameTooLong> {
    const ESCAPED: [char; 3] = ['/', '%', '\0'];
    let account = account_key(repo_id, protocol, origin);
    let name = if account.contains(ESCAPED) {
        let mut name = String::with_capacity(account.len() + 2 * account.matches(ESCAPED).count());
        for character in account.chars() {
            match character {
                '/' => name.push_str("%2F"),
                '%' => name.push_str("%25"),
                '\0' => name.push_str("%00"),
                other => name.push(other),
            }
        }
        name
    } else {
        account
    };
    if name.len() > MAX_FILE_NAME_BYTES {
        return Err(RecordNameTooLong { bytes: name.len() });
    }
    Ok(name)
}

/// Where the store of the user whose `$HOME` is `home` lives.
fn store_directory(home: Option<&OsStr>) -> Result<PathBuf, CredentialError> {
    match home.map(Path::new) {
        Some(home) if home.is_absolute() => Ok(credentials_directory(home)),
        _ => Err(CredentialError::Unavailable(
            "HOME must be an absolute path: the gateway credential store is found beneath it"
                .to_owned(),
        )),
    }
}

/// What one look at the store directory's path found.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Observed {
    Absent,
    /// The look itself failed, for a reason other than nothing being there.
    Unreadable,
    Entry {
        kind: EntryKind,
        uid: u32,
        mode: u32,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EntryKind {
    Directory,
    Symlink,
    Other,
}

/// What the store directory is to a reader or a writer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DirectoryState {
    Usable,
    /// Nothing is at the path: nothing is enrolled, and enrolment may create the directory.
    Absent,
    Refused(DirectoryRefusal),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DirectoryRefusal {
    Relative,
    Unavailable,
    Insecure,
}

impl DirectoryRefusal {
    const fn message(self) -> &'static str {
        match self {
            Self::Relative => "systemd credential directory must be absolute",
            Self::Unavailable => "systemd credential directory is unavailable",
            Self::Insecure => "systemd credential directory has insecure ownership or mode",
        }
    }
}

impl From<DirectoryRefusal> for CredentialError {
    fn from(refusal: DirectoryRefusal) -> Self {
        Self::Unavailable(refusal.message().to_owned())
    }
}

/// Judge the store directory at `path` from what was `observed` there.
///
/// The store is usable only as a real directory that `authorized_uid` owns and that grants group
/// and other nothing. A symlink is refused whatever it points at, as is anything else in the
/// directory's place: records are secrets, and one written or read through a path somebody else
/// can redirect or list is not private.
fn directory_state(path: &Path, observed: Observed, authorized_uid: u32) -> DirectoryState {
    if !path.is_absolute() {
        return DirectoryState::Refused(DirectoryRefusal::Relative);
    }
    match observed {
        Observed::Absent => DirectoryState::Absent,
        Observed::Unreadable => DirectoryState::Refused(DirectoryRefusal::Unavailable),
        Observed::Entry {
            kind: EntryKind::Directory,
            uid,
            mode,
        } if uid == authorized_uid && mode & 0o077 == 0 => DirectoryState::Usable,
        Observed::Entry { .. } => DirectoryState::Refused(DirectoryRefusal::Insecure),
    }
}

/// The gateway reads only a store that is there: to it an absent directory is an unavailable one.
fn require_usable(state: DirectoryState) -> Result<(), CredentialError> {
    match state {
        DirectoryState::Usable => Ok(()),
        DirectoryState::Absent => Err(DirectoryRefusal::Unavailable.into()),
        DirectoryState::Refused(refusal) => Err(refusal.into()),
    }
}

fn observe(metadata: io::Result<Metadata>) -> Observed {
    match metadata {
        Ok(metadata) => {
            let file_type = metadata.file_type();
            let kind = if file_type.is_symlink() {
                EntryKind::Symlink
            } else if file_type.is_dir() {
                EntryKind::Directory
            } else {
                EntryKind::Other
            };
            Observed::Entry {
                kind,
                uid: metadata.uid(),
                mode: metadata.permissions().mode(),
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Observed::Absent,
        Err(_) => Observed::Unreadable,
    }
}

fn validate_directory(path: &Path, authorized_uid: u32) -> Result<(), CredentialError> {
    require_usable(directory_state(
        path,
        observe(std::fs::symlink_metadata(path)),
        authorized_uid,
    ))
}

/// Hold the store directory open once its state is accepted, or `None` when nothing is there.
///
/// `lstat` judges the path, and the descriptor is then judged as held: what a writer changes is
/// the directory this descriptor names, whatever the path resolves to afterwards.
fn hold_directory(directory: &Path, authorized_uid: u32) -> Result<Option<File>, CredentialError> {
    match directory_state(
        directory,
        observe(std::fs::symlink_metadata(directory)),
        authorized_uid,
    ) {
        DirectoryState::Usable => {}
        DirectoryState::Absent => return Ok(None),
        DirectoryState::Refused(refusal) => return Err(refusal.into()),
    }
    let held = open_directory_nofollow(directory)
        .map_err(|_| CredentialError::from(DirectoryRefusal::Unavailable))?;
    require_usable(directory_state(
        directory,
        observe(held.metadata()),
        authorized_uid,
    ))?;
    Ok(Some(held))
}

/// Make the store directory at mode 0700, and the directories above it as the umask has them.
fn create_directory(directory: &Path) -> io::Result<()> {
    let parent = directory.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "credential store directory has no parent",
        )
    })?;
    std::fs::create_dir_all(parent)?;
    match std::fs::DirBuilder::new().mode(0o700).create(directory) {
        Ok(()) => sync_directory(parent),
        // Another enrolment made it first. The caller judges whatever is there now.
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Ok(()),
        Err(error) => Err(error),
    }
}

fn write_record(
    directory: &Path,
    authorized_uid: u32,
    name: String,
    payload: &[u8],
) -> Result<(), CredentialError> {
    let name = leaf(name)?;
    let held = match hold_directory(directory, authorized_uid)? {
        Some(held) => held,
        None => {
            create_directory(directory).map_err(|error| {
                CredentialError::Unavailable(format!(
                    "credential store directory {} could not be created: {error}",
                    directory.display()
                ))
            })?;
            hold_directory(directory, authorized_uid)?
                .ok_or_else(|| CredentialError::from(DirectoryRefusal::Unavailable))?
        }
    };
    publish_record(&held, &name, payload).map_err(|error| {
        CredentialError::Unavailable(format!(
            "credential record could not be written in {}: {error}",
            directory.display()
        ))
    })
}

fn remove_record(
    directory: &Path,
    authorized_uid: u32,
    name: String,
) -> Result<CredentialPresence, CredentialError> {
    let name = leaf(name)?;
    let Some(held) = hold_directory(directory, authorized_uid)? else {
        return Ok(CredentialPresence::Absent);
    };
    unlink_record(&held, &name).map_err(|error| {
        CredentialError::Unavailable(format!(
            "credential record could not be removed from {}: {error}",
            directory.display()
        ))
    })
}

/// The record `name` of the store at `directory`, which holds none while it does not exist.
fn read_record(
    directory: &Path,
    authorized_uid: u32,
    name: &str,
) -> Result<Option<CredentialRecord>, CredentialError> {
    match directory_state(
        directory,
        observe(std::fs::symlink_metadata(directory)),
        authorized_uid,
    ) {
        DirectoryState::Usable => read_systemd_credential(&directory.join(name), authorized_uid),
        DirectoryState::Absent => Ok(None),
        DirectoryState::Refused(refusal) => Err(refusal.into()),
    }
}

/// A record file name as the directory-relative calls take it.
fn leaf(name: String) -> Result<CString, CredentialError> {
    CString::new(name).map_err(|_| {
        CredentialError::Unavailable("credential record name holds a NUL byte".to_owned())
    })
}

/// Publish `payload` as the record `name` beneath the held store directory.
///
/// The bytes go to an exclusively created, never-followed 0600 temporary in the same directory,
/// are synced, and are renamed over `name`; the directory is then synced. A reader sees the old
/// record or the new one. A failure before the rename unlinks the temporary. Its name does not
/// derive from `name`, which may already be as long as a file name can be.
fn publish_record(directory: &File, name: &CStr, payload: &[u8]) -> io::Result<()> {
    use std::io::Write as _;

    let temporary_name =
        CString::new(temp_name(OsStr::new("record"), uuid::Uuid::new_v4().simple()).into_vec())
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "temporary name holds a NUL byte",
                )
            })?;
    let mut file = create_private_file_at(directory, &temporary_name)?;
    let temporary = TemporaryAt::new(directory, &temporary_name);
    file.write_all(payload)?;
    file.sync_all()?;
    // SAFETY: the held directory descriptor and both NUL-terminated names outlive the call, and
    // each name is one entry beneath that directory.
    if unsafe {
        libc::renameat(
            directory.as_raw_fd(),
            temporary_name.as_ptr(),
            directory.as_raw_fd(),
            name.as_ptr(),
        )
    } != 0
    {
        return Err(io::Error::last_os_error());
    }
    temporary.disarm();
    sync_held_directory(directory)
}

/// Unlink the record `name` beneath the held store directory, saying whether it was there.
fn unlink_record(directory: &File, name: &CStr) -> io::Result<CredentialPresence> {
    // SAFETY: the held directory descriptor and the NUL-terminated name outlive the call.
    // unlinkat removes this directory's own entry, never a link's target.
    if unsafe { libc::unlinkat(directory.as_raw_fd(), name.as_ptr(), 0) } == 0 {
        sync_held_directory(directory)?;
        return Ok(CredentialPresence::Installed);
    }
    let error = io::Error::last_os_error();
    if error.kind() == io::ErrorKind::NotFound {
        Ok(CredentialPresence::Absent)
    } else {
        Err(error)
    }
}

fn read_systemd_credential(
    path: &Path,
    authorized_uid: u32,
) -> Result<Option<CredentialRecord>, CredentialError> {
    use std::{io::Read as _, os::unix::fs::OpenOptionsExt as _};

    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => {
            return Err(CredentialError::Unavailable(
                "systemd credential file is unavailable".to_owned(),
            ));
        }
    };
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.uid() != authorized_uid
        || !matches!(metadata.permissions().mode() & 0o777, 0o400 | 0o600)
        || metadata.len() > 1024 * 1024
    {
        return Err(CredentialError::Unavailable(
            "systemd credential file has invalid type, ownership, mode, or size".to_owned(),
        ));
    }
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(|_| {
            CredentialError::Unavailable("systemd credential file could not be opened".to_owned())
        })?;
    let opened = file.metadata().map_err(|_| {
        CredentialError::Unavailable("systemd credential file metadata failed".to_owned())
    })?;
    if opened.dev() != metadata.dev() || opened.ino() != metadata.ino() {
        return Err(CredentialError::Unavailable(
            "systemd credential file changed during lookup".to_owned(),
        ));
    }
    // The record is a secret from its first byte: the buffer is zeroed however this returns.
    let mut bytes = Zeroizing::new(Vec::with_capacity(
        usize::try_from(opened.len()).unwrap_or_default(),
    ));
    file.take(1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| {
            CredentialError::Unavailable("systemd credential file read failed".to_owned())
        })?;
    if bytes.len() > 1024 * 1024 {
        return Err(CredentialError::Unavailable(
            "systemd credential file exceeds 1 MiB".to_owned(),
        ));
    }
    decode_record(std::mem::take(&mut *bytes)).map(Some)
}

/// Run blocking store I/O off the async workers.
async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> Result<T, CredentialError> + Send + 'static,
) -> Result<T, CredentialError> {
    tokio::task::spawn_blocking(work).await.map_err(|error| {
        CredentialError::Unavailable(format!("systemd credential task failed: {error}"))
    })?
}

fn effective_uid() -> u32 {
    // SAFETY: geteuid has no preconditions and reads no caller-owned memory.
    unsafe { libc::geteuid() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::platform::tests::enrollable;

    const ORIGIN: &str = "https://registry.test:443";

    /// A scratch `$HOME` under the temporary directory, removed when the test ends.
    struct ScratchHome(PathBuf);

    impl ScratchHome {
        fn new() -> Self {
            let path = std::env::temp_dir()
                .join(format!("cowshed-credential-store-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir(&path).expect("scratch home");
            Self(path)
        }

        fn home(&self) -> Option<OsString> {
            Some(self.0.clone().into_os_string())
        }

        fn store(&self) -> PathBuf {
            credentials_directory(&self.0)
        }
    }

    impl Drop for ScratchHome {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn query_for(credential: &ScopedCredential) -> CredentialQuery {
        CredentialQuery {
            workspace_id: "ws".to_owned(),
            repo_id: credential.repo_id.clone(),
            protocol: credential.protocol,
            origin: credential.origin.clone(),
            method: http::Method::GET,
            path: "/api/packages/owner/npm/package".to_owned(),
        }
    }

    fn mode_of(path: &Path) -> u32 {
        std::fs::symlink_metadata(path)
            .expect("metadata")
            .permissions()
            .mode()
            & 0o7777
    }

    fn set_mode(path: &Path, mode: u32) {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).expect("mode");
    }

    fn entries(directory: &Path) -> Vec<OsString> {
        let mut names: Vec<OsString> = std::fs::read_dir(directory)
            .expect("read directory")
            .map(|entry| entry.expect("entry").file_name())
            .collect();
        names.sort();
        names
    }

    fn encoded(origin: &str) -> String {
        use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
        URL_SAFE_NO_PAD.encode(origin.as_bytes())
    }

    /// An origin `validate_scope` admits whose account key alone is longer than a file name.
    fn long_origin() -> String {
        let label = "a".repeat(60);
        format!("https://{label}.{label}.{label}.{label}.test:443")
    }

    #[test]
    fn a_record_is_named_by_its_account_key_as_one_file_name_component() {
        let origin = encoded(ORIGIN);
        for (repo_id, protocol, expected) in [
            // The repository id's `/` is the one byte of a real account key no file name holds.
            (
                "owner/repo",
                CredentialProtocol::Generic,
                format!("v1|owner%2Frepo|generic|{origin}"),
            ),
            (
                "owner/repo",
                CredentialProtocol::Npm,
                format!("v1|owner%2Frepo|npm|{origin}"),
            ),
            (
                "owner/repo",
                CredentialProtocol::Cargo,
                format!("v1|owner%2Frepo|cargo|{origin}"),
            ),
            (
                "owner/repo",
                CredentialProtocol::Go,
                format!("v1|owner%2Frepo|go|{origin}"),
            ),
            // A key that is already one component is the name itself.
            (
                "repo",
                CredentialProtocol::Generic,
                format!("v1|repo|generic|{origin}"),
            ),
            // The escape is itself escaped, so no id can spell another id's name.
            (
                "owner%2Frepo",
                CredentialProtocol::Generic,
                format!("v1|owner%252Frepo|generic|{origin}"),
            ),
            (
                "owner\0repo",
                CredentialProtocol::Generic,
                format!("v1|owner%00repo|generic|{origin}"),
            ),
            (
                "../..",
                CredentialProtocol::Generic,
                format!("v1|..%2F..|generic|{origin}"),
            ),
        ] {
            let name = record_file_name(repo_id, protocol, ORIGIN).expect("a storable binding");
            assert_eq!(name, expected, "{repo_id:?}");
            assert!(!name.contains(['/', '\0']), "{name:?}");
            assert!(name.len() <= MAX_FILE_NAME_BYTES, "{name:?}");
        }
    }

    #[test]
    fn a_record_name_longer_than_a_file_name_is_refused_with_its_length() {
        let overhead = record_file_name("", CredentialProtocol::Go, ORIGIN)
            .expect("the key without a repository id")
            .len();
        let room = MAX_FILE_NAME_BYTES - overhead;
        for (repo_id, expected) in [
            ("r".repeat(room), None),
            ("r".repeat(room + 1), Some(MAX_FILE_NAME_BYTES + 1)),
            // An escape counts as the three bytes it is written in.
            (format!("{}/", "r".repeat(room - 3)), None),
            (
                format!("{}/", "r".repeat(room - 2)),
                Some(MAX_FILE_NAME_BYTES + 1),
            ),
        ] {
            let named = record_file_name(&repo_id, CredentialProtocol::Go, ORIGIN);
            match expected {
                None => assert_eq!(named.map(|name| name.len()), Ok(MAX_FILE_NAME_BYTES)),
                Some(bytes) => assert_eq!(named, Err(RecordNameTooLong { bytes })),
            }
        }
        // A host name within DNS limits is enough on its own.
        let refused = record_file_name("owner/repo", CredentialProtocol::Generic, &long_origin())
            .expect_err("a key longer than a file name");
        assert!(refused.bytes > MAX_FILE_NAME_BYTES);
        let CredentialError::Unavailable(message) = CredentialError::from(refused) else {
            panic!("a name refusal is a store refusal");
        };
        assert!(message.contains(&refused.bytes.to_string()), "{message}");
        assert!(message.contains("255"), "{message}");
    }

    #[test]
    fn only_a_private_directory_of_the_authorized_user_is_a_usable_store() {
        const UID: u32 = 1000;
        let directory = |uid, mode| Observed::Entry {
            kind: EntryKind::Directory,
            uid,
            mode,
        };
        let insecure = DirectoryState::Refused(DirectoryRefusal::Insecure);
        for (path, observed, expected) in [
            ("/store", directory(UID, 0o040_700), DirectoryState::Usable),
            ("/store", directory(UID, 0o040_500), DirectoryState::Usable),
            ("/store", Observed::Absent, DirectoryState::Absent),
            (
                "/store",
                Observed::Unreadable,
                DirectoryState::Refused(DirectoryRefusal::Unavailable),
            ),
            // Any bit for group or other.
            ("/store", directory(UID, 0o040_750), insecure),
            ("/store", directory(UID, 0o040_710), insecure),
            ("/store", directory(UID, 0o040_705), insecure),
            ("/store", directory(UID, 0o040_701), insecure),
            ("/store", directory(UID, 0o040_777), insecure),
            // Another owner, root included.
            ("/store", directory(UID + 1, 0o040_700), insecure),
            ("/store", directory(0, 0o040_700), insecure),
            // Not a directory, whatever it points at and whatever its mode.
            (
                "/store",
                Observed::Entry {
                    kind: EntryKind::Symlink,
                    uid: UID,
                    mode: 0o120_700,
                },
                insecure,
            ),
            (
                "/store",
                Observed::Entry {
                    kind: EntryKind::Other,
                    uid: UID,
                    mode: 0o100_600,
                },
                insecure,
            ),
            // A relative path is refused before anything found there is considered.
            (
                "store",
                directory(UID, 0o040_700),
                DirectoryState::Refused(DirectoryRefusal::Relative),
            ),
            (
                "store",
                Observed::Absent,
                DirectoryState::Refused(DirectoryRefusal::Relative),
            ),
        ] {
            assert_eq!(
                directory_state(Path::new(path), observed, UID),
                expected,
                "{path} {observed:?}"
            );
        }
    }

    #[test]
    fn the_gateway_reads_only_a_store_that_is_there() {
        for (state, expected) in [
            (DirectoryState::Usable, None),
            (
                DirectoryState::Absent,
                Some("systemd credential directory is unavailable"),
            ),
            (
                DirectoryState::Refused(DirectoryRefusal::Unavailable),
                Some("systemd credential directory is unavailable"),
            ),
            (
                DirectoryState::Refused(DirectoryRefusal::Relative),
                Some("systemd credential directory must be absolute"),
            ),
            (
                DirectoryState::Refused(DirectoryRefusal::Insecure),
                Some("systemd credential directory has insecure ownership or mode"),
            ),
        ] {
            let refusal = require_usable(state).err().map(|error| match error {
                CredentialError::Unavailable(message) => message,
                other => panic!("{state:?} refused as {other:?}"),
            });
            assert_eq!(refusal.as_deref(), expected, "{state:?}");
        }
    }

    #[test]
    fn the_store_is_found_beneath_an_absolute_home_only() {
        let home = Path::new("/absolute/home");
        assert_eq!(
            store_directory(Some(home.as_os_str())).expect("an absolute home"),
            credentials_directory(home)
        );
        for home in [None, Some(""), Some("relative/home"), Some(".")] {
            let Err(CredentialError::Unavailable(message)) = store_directory(home.map(OsStr::new))
            else {
                panic!("{home:?} must be refused");
            };
            assert!(message.contains("HOME"), "{message}");
        }
    }

    #[tokio::test]
    async fn an_enrolment_is_what_the_gateway_reads_until_it_is_removed() {
        let scratch = ScratchHome::new();
        let uid = effective_uid();
        let credential = enrollable();
        let query = query_for(&credential);
        let (repo_id, protocol, origin) = (
            credential.repo_id.as_str(),
            credential.protocol,
            credential.origin.as_str(),
        );

        // Enrolment makes the store beneath a home that has none of cowshed's directories yet.
        store_under(scratch.home(), uid, enrollable())
            .await
            .expect("enrol");
        let store = scratch.store();
        let name = record_file_name(repo_id, protocol, origin).expect("name");
        assert_eq!(mode_of(&store), 0o700);
        assert_eq!(mode_of(&store.join(&name)), 0o600);
        assert_eq!(entries(&store), [OsString::from(&name)]);

        let provider = SystemdCredentialProvider::new(store.clone(), uid).expect("provider");
        let found = provider
            .lookup(&query)
            .await
            .expect("lookup")
            .expect("the enrolled record");
        assert!(found.validate_for(&query));
        assert_eq!(found.header_value.as_str(), "Bearer host-held");
        assert_eq!(
            scope_under(scratch.home(), uid, repo_id, protocol, origin)
                .await
                .expect("scope"),
            Some(credential.path_prefixes.clone())
        );

        // Rotation replaces the record under the same provider and leaves no temporary behind.
        let mut rotated = enrollable();
        rotated.header_value = Zeroizing::new("Bearer rotated".to_owned());
        store_under(scratch.home(), uid, rotated)
            .await
            .expect("rotate");
        let found = provider
            .lookup(&query)
            .await
            .expect("lookup")
            .expect("the rotated record");
        assert_eq!(found.header_value.as_str(), "Bearer rotated");
        assert_eq!(mode_of(&store.join(&name)), 0o600);
        assert_eq!(entries(&store), [OsString::from(&name)]);

        assert_eq!(
            remove_under(scratch.home(), uid, repo_id, protocol, origin)
                .await
                .expect("remove"),
            CredentialPresence::Installed
        );
        assert!(provider.lookup(&query).await.expect("lookup").is_none());
        assert_eq!(
            scope_under(scratch.home(), uid, repo_id, protocol, origin)
                .await
                .expect("scope"),
            None
        );
        assert!(entries(&store).is_empty());
    }

    #[tokio::test]
    async fn removal_says_whether_the_binding_was_there() {
        #[derive(Clone, Copy, Debug)]
        enum Before {
            NoStore,
            EmptyStore,
            OtherBinding,
            Enrolled,
        }
        let uid = effective_uid();
        for (before, expected) in [
            (Before::NoStore, CredentialPresence::Absent),
            (Before::EmptyStore, CredentialPresence::Absent),
            (Before::OtherBinding, CredentialPresence::Absent),
            (Before::Enrolled, CredentialPresence::Installed),
        ] {
            let scratch = ScratchHome::new();
            let store = scratch.store();
            match before {
                Before::NoStore => {}
                Before::EmptyStore => {
                    std::fs::create_dir_all(&store).expect("store");
                    set_mode(&store, 0o700);
                }
                Before::OtherBinding => {
                    let mut other = enrollable();
                    other.origin = "https://other.test:443".to_owned();
                    store_under(scratch.home(), uid, other)
                        .await
                        .expect("enrol");
                }
                Before::Enrolled => store_under(scratch.home(), uid, enrollable())
                    .await
                    .expect("enrol"),
            }
            let kept = if store.exists() {
                entries(&store).len()
            } else {
                0
            };
            let credential = enrollable();
            let removed = remove_under(
                scratch.home(),
                uid,
                &credential.repo_id,
                credential.protocol,
                &credential.origin,
            )
            .await
            .expect("remove");
            assert_eq!(removed, expected, "{before:?}");
            match (before, expected) {
                // Removal never makes the store it found missing.
                (Before::NoStore, _) => assert!(!store.exists()),
                (_, CredentialPresence::Installed) => assert_eq!(entries(&store).len(), kept - 1),
                (_, CredentialPresence::Absent) => assert_eq!(entries(&store).len(), kept),
            }
        }
    }

    #[tokio::test]
    async fn a_store_directory_in_a_refused_state_is_not_written_read_or_emptied() {
        #[derive(Clone, Copy, Debug)]
        enum Planted {
            Symlink,
            GroupReadable,
            OtherSearchable,
            RegularFile,
            ForeignOwner,
        }
        for planted in [
            Planted::Symlink,
            Planted::GroupReadable,
            Planted::OtherSearchable,
            Planted::RegularFile,
            Planted::ForeignOwner,
        ] {
            let scratch = ScratchHome::new();
            let store = scratch.store();
            let elsewhere = scratch.0.join("elsewhere");
            std::fs::create_dir_all(store.parent().expect("support directory"))
                .expect("support directory");
            let mut uid = effective_uid();
            match planted {
                Planted::Symlink => {
                    std::fs::create_dir(&elsewhere).expect("link target");
                    set_mode(&elsewhere, 0o700);
                    std::os::unix::fs::symlink(&elsewhere, &store).expect("symlink");
                }
                Planted::GroupReadable => {
                    std::fs::create_dir(&store).expect("store");
                    set_mode(&store, 0o750);
                }
                Planted::OtherSearchable => {
                    std::fs::create_dir(&store).expect("store");
                    set_mode(&store, 0o701);
                }
                Planted::RegularFile => {
                    std::fs::write(&store, b"").expect("file");
                    set_mode(&store, 0o600);
                }
                // The directory is this user's, so the store's user is made somebody else.
                Planted::ForeignOwner => {
                    std::fs::create_dir(&store).expect("store");
                    set_mode(&store, 0o700);
                    uid = uid.wrapping_add(1);
                }
            }
            let credential = enrollable();
            let (repo_id, protocol, origin) = (
                credential.repo_id.as_str(),
                credential.protocol,
                credential.origin.as_str(),
            );
            let insecure = DirectoryRefusal::Insecure.message();
            for refused in [
                store_under(scratch.home(), uid, enrollable())
                    .await
                    .map(|()| "stored"),
                remove_under(scratch.home(), uid, repo_id, protocol, origin)
                    .await
                    .map(|_| "removed"),
                scope_under(scratch.home(), uid, repo_id, protocol, origin)
                    .await
                    .map(|_| "read"),
            ] {
                match refused {
                    Err(CredentialError::Unavailable(message)) => {
                        assert_eq!(message, insecure, "{planted:?}");
                    }
                    other => panic!("{planted:?} must be refused, got {other:?}"),
                }
            }
            match planted {
                Planted::Symlink => assert!(entries(&elsewhere).is_empty()),
                Planted::RegularFile => {
                    assert!(std::fs::read(&store).expect("file").is_empty());
                }
                Planted::GroupReadable | Planted::OtherSearchable | Planted::ForeignOwner => {
                    assert!(entries(&store).is_empty(), "{planted:?}");
                }
            }
        }
    }

    #[tokio::test]
    async fn a_binding_with_no_file_name_is_refused_at_enrolment_and_absent_everywhere_else() {
        let scratch = ScratchHome::new();
        let uid = effective_uid();
        let mut credential = enrollable();
        credential.origin = long_origin();
        validate_scope(&credential).expect("the binding itself is usable");
        let query = query_for(&credential);
        let (repo_id, protocol, origin) = (
            credential.repo_id.clone(),
            credential.protocol,
            credential.origin.clone(),
        );

        let Err(CredentialError::Unavailable(message)) =
            store_under(scratch.home(), uid, credential).await
        else {
            panic!("a record name longer than a file name must be refused");
        };
        assert!(message.contains("255"), "{message}");
        // Refused before the store is touched.
        assert!(!scratch.store().exists());

        assert_eq!(
            remove_under(scratch.home(), uid, &repo_id, protocol, &origin)
                .await
                .expect("remove"),
            CredentialPresence::Absent
        );
        assert_eq!(
            scope_under(scratch.home(), uid, &repo_id, protocol, &origin)
                .await
                .expect("scope"),
            None
        );
        let store = scratch.store();
        std::fs::create_dir_all(&store).expect("store");
        set_mode(&store, 0o700);
        let provider = SystemdCredentialProvider::new(store, uid).expect("provider");
        assert!(provider.lookup(&query).await.expect("lookup").is_none());
    }

    #[tokio::test]
    async fn systemd_credentials_require_scoped_owned_strict_files() {
        let directory =
            std::env::temp_dir().join(format!("cowshed-systemd-creds-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).expect("directory");
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700))
            .expect("directory mode");
        let provider =
            SystemdCredentialProvider::new(directory.clone(), effective_uid()).expect("provider");
        let query = CredentialQuery {
            workspace_id: "ws".to_owned(),
            repo_id: "repo".to_owned(),
            protocol: CredentialProtocol::Generic,
            origin: "https://example.test:443".to_owned(),
            method: http::Method::GET,
            path: "/v1/resource".to_owned(),
        };
        let path = directory
            .join(record_file_name(&query.repo_id, query.protocol, &query.origin).expect("name"));
        std::fs::write(
            &path,
            br#"{"version":1,"repoId":"repo","protocol":"generic","origin":"https://example.test:443","methods":["GET"],"pathPrefixes":["/v1"],"headerName":"authorization","headerValue":"Bearer secret"}"#,
        )
        .expect("credential");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o400))
            .expect("credential mode");
        let record = provider
            .lookup(&query)
            .await
            .expect("lookup")
            .expect("record");
        assert!(record.validate_for(&query));
        assert!(!format!("{record:?}").contains("Bearer secret"));

        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).expect("bad mode");
        assert!(matches!(
            provider.lookup(&query).await,
            Err(CredentialError::Unavailable(_))
        ));
        let _ = std::fs::remove_dir_all(directory);
    }
}
