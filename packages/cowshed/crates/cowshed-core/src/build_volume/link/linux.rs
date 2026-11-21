//! A path's own extended attributes on Linux: the `l` calls, which never follow a final link.

use std::ffi::CStr;

/// Set `name` to `value` on `path` itself; the raw result of `lsetxattr`.
pub(super) fn set_attribute(path: &CStr, name: &CStr, value: &[u8]) -> libc::c_int {
    // SAFETY: the path and name are NUL-terminated and the value is live across the call, which
    // only reads them.
    unsafe {
        libc::lsetxattr(
            path.as_ptr(),
            name.as_ptr(),
            value.as_ptr().cast(),
            value.len(),
            0,
        )
    }
}

/// The length of `name`'s value on `path` itself, negative when it carries none or cannot be read.
pub(super) fn attribute_length(path: &CStr, name: &CStr) -> isize {
    // SAFETY: the path and name are NUL-terminated and live across the call; a null buffer of
    // size zero asks only for the value's length.
    unsafe { libc::lgetxattr(path.as_ptr(), name.as_ptr(), std::ptr::null_mut(), 0) }
}
