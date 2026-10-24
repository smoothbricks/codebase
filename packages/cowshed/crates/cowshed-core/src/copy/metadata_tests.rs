//! Behavioural proof for the held-file metadata warrant of copy_held_file and
//! verify_held_file: extended attributes and the ACL are part of what a
//! cross-filesystem recovery copy must carry before its source may be
//! released. Fixtures are real owned files; metadata is set and read back
//! through the operating system's descriptor interfaces, never through the
//! production helpers under test, so a copier that dropped an attribute
//! cannot also be the oracle that says it arrived.

use std::ffi::CString;
use std::fs;
use std::os::fd::AsRawFd as _;
use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::storage::directory::tests::Scratch;

#[cfg(target_os = "macos")]
use super::native::{NativeAcl, acl_free, acl_get_fd_np};
use super::{copy_held_file, verify_held_file};

const PAYLOAD: &[u8] = b"metadata custody payload\n";
const SOURCE_MODE: u32 = 0o640;

#[cfg(target_os = "macos")]
const TEST_XATTR: &str = "com.cowshed.test.metadata";
#[cfg(not(target_os = "macos"))]
const TEST_XATTR: &str = "user.cowshed.test.metadata";
#[cfg(target_os = "macos")]
const STRAY_XATTR: &str = "com.cowshed.test.stray";
#[cfg(not(target_os = "macos"))]
const STRAY_XATTR: &str = "user.cowshed.test.stray";

const XATTR_VALUE: &[u8] = b"carried by the copy";
const CHANGED_XATTR_VALUE: &[u8] = b"changed after the copy";

/// The one errno that means an attribute is absent. Every other failure -
/// permission, unsupported namespace, I/O - is an oracle failure and must
/// never be read as absence.
#[cfg(target_os = "macos")]
const XATTR_ABSENT: libc::c_int = libc::ENOATTR;
#[cfg(not(target_os = "macos"))]
const XATTR_ABSENT: libc::c_int = libc::ENODATA;

fn fixed_time() -> SystemTime {
    UNIX_EPOCH + Duration::new(1_600_000_000, 123_456_789)
}

fn xattr_set(file: &fs::File, name: &str, value: &[u8]) {
    let name = CString::new(name).expect("xattr name");
    // SAFETY: the name and value outlive the call; the descriptor is held by file.
    #[cfg(target_os = "macos")]
    let status = unsafe {
        libc::fsetxattr(
            file.as_raw_fd(),
            name.as_ptr(),
            value.as_ptr().cast(),
            value.len(),
            0,
            0,
        )
    };
    #[cfg(not(target_os = "macos"))]
    let status = unsafe {
        libc::fsetxattr(
            file.as_raw_fd(),
            name.as_ptr(),
            value.as_ptr().cast(),
            value.len(),
            0,
        )
    };
    assert_eq!(status, 0, "fsetxattr: {}", std::io::Error::last_os_error());
}

fn xattr_get(file: &fs::File, name: &str) -> Option<Vec<u8>> {
    let name = CString::new(name).expect("xattr name");
    // SAFETY: a null buffer with size zero asks for the value length only.
    #[cfg(target_os = "macos")]
    let size = unsafe {
        libc::fgetxattr(
            file.as_raw_fd(),
            name.as_ptr(),
            std::ptr::null_mut(),
            0,
            0,
            0,
        )
    };
    #[cfg(not(target_os = "macos"))]
    let size = unsafe { libc::fgetxattr(file.as_raw_fd(), name.as_ptr(), std::ptr::null_mut(), 0) };
    if size < 0 {
        let error = std::io::Error::last_os_error();
        assert_eq!(
            error.raw_os_error(),
            Some(XATTR_ABSENT),
            "fgetxattr probe: {error}"
        );
        return None;
    }
    let mut value = vec![0_u8; usize::try_from(size).expect("non-negative xattr size")];
    // SAFETY: value holds exactly the probed length for the second call.
    #[cfg(target_os = "macos")]
    let read = unsafe {
        libc::fgetxattr(
            file.as_raw_fd(),
            name.as_ptr(),
            value.as_mut_ptr().cast(),
            value.len(),
            0,
            0,
        )
    };
    #[cfg(not(target_os = "macos"))]
    let read = unsafe {
        libc::fgetxattr(
            file.as_raw_fd(),
            name.as_ptr(),
            value.as_mut_ptr().cast(),
            value.len(),
        )
    };
    assert_eq!(read, size, "fgetxattr: {}", std::io::Error::last_os_error());
    Some(value)
}

fn xattr_names(file: &fs::File) -> Vec<String> {
    // SAFETY: a null buffer with size zero asks for the name-list length only.
    #[cfg(target_os = "macos")]
    let size = unsafe { libc::flistxattr(file.as_raw_fd(), std::ptr::null_mut(), 0, 0) };
    #[cfg(not(target_os = "macos"))]
    let size = unsafe { libc::flistxattr(file.as_raw_fd(), std::ptr::null_mut(), 0) };
    assert!(size >= 0, "flistxattr: {}", std::io::Error::last_os_error());
    let mut list = vec![0_u8; usize::try_from(size).expect("non-negative xattr list size")];
    // SAFETY: list holds exactly the probed length for the second call.
    #[cfg(target_os = "macos")]
    let read =
        unsafe { libc::flistxattr(file.as_raw_fd(), list.as_mut_ptr().cast(), list.len(), 0) };
    #[cfg(not(target_os = "macos"))]
    let read = unsafe { libc::flistxattr(file.as_raw_fd(), list.as_mut_ptr().cast(), list.len()) };
    assert_eq!(
        read,
        size,
        "flistxattr: {}",
        std::io::Error::last_os_error()
    );
    list.split(|byte| *byte == 0)
        .filter(|name| !name.is_empty())
        .map(|name| String::from_utf8(name.to_vec()).expect("xattr name is utf8"))
        .collect()
}

/// The oracle calls the OS text APIs, not the production metadata comparison.
/// Shared raw declarations keep one C ABI for the opaque ACL object.
#[cfg(target_os = "macos")]
type AclT = *mut NativeAcl;

#[cfg(target_os = "macos")]
const ACL_TYPE_EXTENDED: libc::c_int = 0x0000_0100;

#[cfg(target_os = "macos")]
unsafe extern "C" {
    fn acl_from_text(buf: *const libc::c_char) -> AclT;
    fn acl_to_text(acl: AclT, len: *mut libc::ssize_t) -> *mut libc::c_char;
    fn acl_set_fd_np(fd: libc::c_int, acl: AclT, kind: libc::c_int) -> libc::c_int;
}

// The well-known everyone group (ABCDEFAB-CDEF-ABCD-EFAB-CDEF0000000C, gid 12)
// in the acl_from_text long form; acl_to_text renders it back byte-for-byte.
#[cfg(target_os = "macos")]
const SOURCE_ACL_TEXT: &str =
    "!#acl 1\ngroup:ABCDEFAB-CDEF-ABCD-EFAB-CDEF0000000C:everyone:12:allow:read\n";
#[cfg(target_os = "macos")]
const CHANGED_ACL_TEXT: &str =
    "!#acl 1\ngroup:ABCDEFAB-CDEF-ABCD-EFAB-CDEF0000000C:everyone:12:allow:read,write\n";

#[cfg(target_os = "macos")]
fn acl_set(file: &fs::File, text: &str) {
    let text = CString::new(text).expect("acl text");
    // SAFETY: the text outlives acl_from_text, which copies it.
    let acl = unsafe { acl_from_text(text.as_ptr()) };
    assert!(
        !acl.is_null(),
        "acl_from_text: {}",
        std::io::Error::last_os_error()
    );
    // SAFETY: acl is the live object just created; the descriptor is held by file.
    let status = unsafe { acl_set_fd_np(file.as_raw_fd(), acl, ACL_TYPE_EXTENDED) };
    let error = std::io::Error::last_os_error();
    // SAFETY: acl is freed exactly once, after its last use.
    unsafe { acl_free(acl.cast()) };
    assert_eq!(status, 0, "acl_set_fd_np: {error}");
}

#[cfg(target_os = "macos")]
fn acl_snapshot(file: &fs::File) -> Option<Vec<u8>> {
    // SAFETY: the descriptor is held by file for the call.
    let acl = unsafe { acl_get_fd_np(file.as_raw_fd(), ACL_TYPE_EXTENDED) };
    if acl.is_null() {
        // ENOENT is the kernel absent-ACL answer; anything else is an error,
        // never absence.
        let error = std::io::Error::last_os_error();
        assert_eq!(
            error.raw_os_error(),
            Some(libc::ENOENT),
            "acl_get_fd_np: {error}"
        );
        return None;
    }
    let mut len: libc::ssize_t = 0;
    // SAFETY: acl is live; len receives the text length.
    let text = unsafe { acl_to_text(acl, &raw mut len) };
    assert!(
        !text.is_null(),
        "acl_to_text: {}",
        std::io::Error::last_os_error()
    );
    // SAFETY: text holds len bytes of ACL text and is freed exactly once below.
    let bytes = unsafe {
        std::slice::from_raw_parts(
            text.cast::<u8>(),
            usize::try_from(len).expect("acl text length"),
        )
    }
    .to_vec();
    // SAFETY: both objects are freed exactly once, after their last use.
    unsafe { acl_free(text.cast()) };
    unsafe { acl_free(acl.cast()) };
    Some(bytes)
}

#[cfg(target_os = "macos")]
fn set_source_acl(file: &fs::File) {
    acl_set(file, SOURCE_ACL_TEXT);
}

#[cfg(target_os = "macos")]
fn set_changed_acl(file: &fs::File) {
    acl_set(file, CHANGED_ACL_TEXT);
}

#[cfg(not(target_os = "macos"))]
const POSIX_ACL_ACCESS: &str = "system.posix_acl_access";

/// The kernel posix_acl_xattr encoding (version 2): one named-user entry
/// beside the owning entries, with a mask that leaves SOURCE_MODE untouched,
/// so an ACL difference is never also a mode difference.
#[cfg(not(target_os = "macos"))]
fn posix_acl_bytes(named_user: u32) -> Vec<u8> {
    const UNDEFINED: u32 = 0xffff_ffff;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&2_u32.to_le_bytes());
    for (tag, perm, id) in [
        (1_u16, 6_u16, UNDEFINED),
        (2, 4, named_user),
        (4, 4, UNDEFINED),
        (16, 4, UNDEFINED),
        (32, 0, UNDEFINED),
    ] {
        bytes.extend_from_slice(&tag.to_le_bytes());
        bytes.extend_from_slice(&perm.to_le_bytes());
        bytes.extend_from_slice(&id.to_le_bytes());
    }
    bytes
}

#[cfg(not(target_os = "macos"))]
fn acl_snapshot(file: &fs::File) -> Option<Vec<u8>> {
    xattr_get(file, POSIX_ACL_ACCESS)
}

#[cfg(not(target_os = "macos"))]
fn set_source_acl(file: &fs::File) {
    xattr_set(file, POSIX_ACL_ACCESS, &posix_acl_bytes(65_534));
}

#[cfg(not(target_os = "macos"))]
fn set_changed_acl(file: &fs::File) {
    xattr_set(file, POSIX_ACL_ACCESS, &posix_acl_bytes(65_533));
}

struct Held {
    _scratch: Scratch,
    source_path: PathBuf,
    destination_path: PathBuf,
    source: fs::File,
    destination: fs::File,
}

/// A regular owned file carrying the payload and one extended attribute,
/// plus an extended ACL when asked, with timestamps pinned after the
/// metadata so the copy, not this fixture, must carry every one of them.
fn dress_source(path: &Path, extended_acl: bool) {
    fs::write(path, PAYLOAD).expect("write source payload");
    let setup = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .expect("open source for metadata");
    setup
        .set_permissions(fs::Permissions::from_mode(SOURCE_MODE))
        .expect("set source mode");
    xattr_set(&setup, TEST_XATTR, XATTR_VALUE);
    if extended_acl {
        set_source_acl(&setup);
    }
    setup
        .set_times(
            fs::FileTimes::new()
                .set_accessed(fixed_time())
                .set_modified(fixed_time()),
        )
        .expect("set source times");
}

fn held_fixture(label: &str) -> Held {
    held_fixture_dressed(label, true)
}

fn copied_fixture(label: &str) -> Held {
    let held = held_fixture(label);
    copy_held_file(&held.source, &held.destination).expect("copy held file");
    verify_held_file(&held.source, &held.destination).expect("an untouched copy verifies");
    held
}

#[derive(Debug, Eq, PartialEq)]
struct Observed {
    bytes: Vec<u8>,
    mode: u32,
    modified: (i64, i64),
    xattr: Option<Vec<u8>>,
    acl: Option<Vec<u8>>,
}

fn observe(path: &Path, file: &fs::File) -> Observed {
    let metadata = file.metadata().expect("held metadata");
    Observed {
        bytes: fs::read(path).expect("the file is still readable at its path"),
        mode: metadata.mode() & 0o7777,
        modified: (metadata.mtime(), metadata.mtime_nsec()),
        xattr: xattr_get(file, TEST_XATTR),
        acl: acl_snapshot(file),
    }
}

/// Everything verify_held_file could refuse on except extended metadata is
/// already equal, so a refusal after this point names the metadata alone.
fn assert_plain_facts_match(held: &Held) {
    assert_eq!(
        fs::read(&held.source_path).expect("read source"),
        fs::read(&held.destination_path).expect("read destination"),
        "bytes identical"
    );
    let source = held.source.metadata().expect("source metadata");
    let destination = held.destination.metadata().expect("destination metadata");
    assert_eq!(source.len(), destination.len(), "size identical");
    assert_eq!(
        source.mode() & 0o7777,
        destination.mode() & 0o7777,
        "mode identical"
    );
    assert_eq!(
        (source.mtime(), source.mtime_nsec()),
        (destination.mtime(), destination.mtime_nsec()),
        "mtime identical"
    );
}

#[test]
fn a_held_copy_carries_extended_attributes_and_the_acl() {
    let held = held_fixture("carries-metadata");
    assert_eq!(
        xattr_get(&held.source, TEST_XATTR).as_deref(),
        Some(XATTR_VALUE),
        "the fixture's attribute is really on the source"
    );
    let source_acl = acl_snapshot(&held.source);
    assert!(
        source_acl.is_some(),
        "the fixture's ACL is really on the source"
    );

    copy_held_file(&held.source, &held.destination).expect("copy held file");
    verify_held_file(&held.source, &held.destination).expect("the faithful copy verifies");

    assert_eq!(
        fs::read(&held.destination_path).expect("read destination"),
        PAYLOAD
    );
    let source = held.source.metadata().expect("source metadata");
    let destination = held.destination.metadata().expect("destination metadata");
    assert_eq!(destination.mode() & 0o7777, SOURCE_MODE);
    assert_eq!(
        (destination.mtime(), destination.mtime_nsec()),
        (source.mtime(), source.mtime_nsec())
    );
    assert_eq!(
        (destination.uid(), destination.gid()),
        (source.uid(), source.gid())
    );
    assert!(
        xattr_names(&held.destination)
            .iter()
            .any(|name| name == TEST_XATTR),
        "the copied attribute is listed on the destination"
    );
    assert_eq!(
        xattr_get(&held.destination, TEST_XATTR).as_deref(),
        Some(XATTR_VALUE),
        "the attribute value arrived, read back through the descriptor"
    );
    assert_eq!(
        acl_snapshot(&held.destination),
        source_acl,
        "the ACL arrived, read back through the descriptor"
    );
}

#[cfg(not(target_os = "macos"))]
#[test]
fn a_copy_drops_destination_only_extended_attributes() {
    let held = held_fixture("drops-stray");
    xattr_set(&held.destination, STRAY_XATTR, b"inherited stray");
    assert!(
        xattr_get(&held.destination, STRAY_XATTR).is_some(),
        "the stray attribute is really on the destination before the copy"
    );

    copy_held_file(&held.source, &held.destination).expect("copy held file");

    assert_eq!(
        xattr_get(&held.destination, STRAY_XATTR),
        None,
        "an attribute the source never had must not survive the copy"
    );
    verify_held_file(&held.source, &held.destination).expect("the exact copy verifies");
}

#[test]
fn verification_refuses_a_byte_identical_destination_with_a_changed_xattr() {
    let held = copied_fixture("changed-xattr");
    let before = observe(&held.source_path, &held.source);
    xattr_set(&held.destination, TEST_XATTR, CHANGED_XATTR_VALUE);
    assert_plain_facts_match(&held);

    let error = verify_held_file(&held.source, &held.destination)
        .expect_err("a changed attribute value must refuse verification");

    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData, "{error}");
    assert_eq!(
        observe(&held.source_path, &held.source),
        before,
        "a refused verification leaves the source present and untouched"
    );
}

#[test]
fn verification_refuses_a_byte_identical_destination_with_an_extra_xattr() {
    let held = copied_fixture("extra-xattr");
    let before = observe(&held.source_path, &held.source);
    xattr_set(&held.destination, STRAY_XATTR, b"not on the source");
    assert_plain_facts_match(&held);

    let error = verify_held_file(&held.source, &held.destination)
        .expect_err("an extra attribute must refuse verification");

    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData, "{error}");
    assert_eq!(
        observe(&held.source_path, &held.source),
        before,
        "a refused verification leaves the source present and untouched"
    );
}

#[test]
fn verification_refuses_a_byte_identical_destination_with_a_changed_acl() {
    let held = copied_fixture("changed-acl");
    let before = observe(&held.source_path, &held.source);
    set_changed_acl(&held.destination);
    assert_ne!(
        acl_snapshot(&held.destination),
        acl_snapshot(&held.source),
        "the destination ACL really differs before verification is asked"
    );
    assert_plain_facts_match(&held);

    let error = verify_held_file(&held.source, &held.destination)
        .expect_err("a changed ACL must refuse verification");

    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData, "{error}");
    assert_eq!(
        observe(&held.source_path, &held.source),
        before,
        "a refused verification leaves the source present and untouched"
    );
}

#[test]
fn a_copy_that_cannot_complete_leaves_the_source_intact() {
    let scratch = scratch("failed-transfer");
    let source_path = scratch.path().join("source");
    let destination_path = scratch.path().join("destination");
    dress_source(&source_path, true);
    let source = fs::File::open(&source_path).expect("hold source");
    let before = observe(&source_path, &source);
    fs::write(&destination_path, b"").expect("create destination");
    let destination = fs::File::open(&destination_path).expect("hold read-only destination");

    let error = copy_held_file(&source, &destination)
        .expect_err("a read-only destination cannot take the copy");

    assert_eq!(
        observe(&source_path, &source),
        before,
        "a failed transfer neither moves nor strips the source: {error}"
    );
}

/// The shared RAII scratch root, under the runtime directory where one is
/// offered: created before any fixture setup can panic and removed on drop,
/// assertion failures included.
fn scratch(label: &str) -> Scratch {
    let parent = std::env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    Scratch::new_in(&parent, &format!("copy-metadata-{label}"))
}

fn held_fixture_dressed(label: &str, extended_acl: bool) -> Held {
    let scratch = scratch(label);
    let source_path = scratch.path().join("source");
    let destination_path = scratch.path().join("destination");
    dress_source(&source_path, extended_acl);
    let source = fs::File::open(&source_path).expect("hold source");
    let destination = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&destination_path)
        .expect("hold newly created destination");
    Held {
        _scratch: scratch,
        source_path,
        destination_path,
        source,
        destination,
    }
}

fn held_fixture_without_acl(label: &str) -> Held {
    held_fixture_dressed(label, false)
}

fn copied_fixture_without_acl(label: &str) -> Held {
    let held = held_fixture_without_acl(label);
    copy_held_file(&held.source, &held.destination).expect("copy held file");
    verify_held_file(&held.source, &held.destination).expect("an untouched ACL-free copy verifies");
    held
}

#[test]
fn a_held_copy_without_an_acl_carries_extended_attributes_and_verifies() {
    let held = held_fixture_without_acl("acl-free");
    assert_eq!(
        acl_snapshot(&held.source),
        None,
        "the fixture source really has no extended ACL"
    );
    assert_eq!(
        xattr_get(&held.source, TEST_XATTR).as_deref(),
        Some(XATTR_VALUE),
        "the fixture's attribute is really on the source"
    );

    copy_held_file(&held.source, &held.destination).expect("copy held file");
    verify_held_file(&held.source, &held.destination)
        .expect("an ACL-free copy verifies: absence is a value, not an error");

    assert_eq!(
        fs::read(&held.destination_path).expect("read destination"),
        PAYLOAD
    );
    assert_eq!(
        xattr_get(&held.destination, TEST_XATTR).as_deref(),
        Some(XATTR_VALUE),
        "the attribute value arrived, read back through the descriptor"
    );
    assert_eq!(
        acl_snapshot(&held.destination),
        None,
        "the destination is exactly as ACL-free as the source"
    );
}

#[test]
fn verification_refuses_an_acl_where_the_source_has_none() {
    let held = copied_fixture_without_acl("acl-appears");
    let before = observe(&held.source_path, &held.source);
    assert_eq!(before.acl, None, "the source has no ACL to warrant one");
    set_source_acl(&held.destination);
    assert!(
        acl_snapshot(&held.destination).is_some(),
        "the destination ACL really appeared before verification is asked"
    );
    assert_plain_facts_match(&held);

    let error = verify_held_file(&held.source, &held.destination)
        .expect_err("an ACL the source never had must refuse verification");

    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData, "{error}");
    assert_eq!(
        observe(&held.source_path, &held.source),
        before,
        "a refused verification leaves the ACL-free source present and untouched"
    );
}
