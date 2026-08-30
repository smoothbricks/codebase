use std::os::fd::{AsRawFd, OwnedFd};

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;

#[cfg(target_os = "linux")]
use linux::peer_uid as platform_peer_uid;
#[cfg(target_os = "macos")]
use macos::peer_uid as platform_peer_uid;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PeerCredentialsError {
    SocketTypeSizeOverflow,
    SocketTypeQueryFailed,
    NotStream,
    PeerCredentialQueryFailed,
}

/// Reads the peer uid after requiring a stream socket. The uid is the authorization boundary;
/// platform credential APIs may also provide a gid, but it is intentionally not consulted.
/// On targets other than macOS and Linux, this returns `Err(PeerCredentialsError::PeerCredentialQueryFailed)`.
pub(crate) fn peer_uid(descriptor: &OwnedFd) -> Result<libc::uid_t, PeerCredentialsError> {
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = descriptor;
        return Err(PeerCredentialsError::PeerCredentialQueryFailed);
    }
    let fd = descriptor.as_raw_fd();
    let mut socket_type: libc::c_int = 0;
    let mut socket_type_len = libc::socklen_t::try_from(std::mem::size_of::<libc::c_int>())
        .map_err(|_| PeerCredentialsError::SocketTypeSizeOverflow)?;
    // SAFETY: `descriptor` owns a live fd and the output pointer/length describe one `c_int`.
    let result = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_TYPE,
            std::ptr::from_mut(&mut socket_type).cast(),
            &mut socket_type_len,
        )
    };
    if result != 0 {
        return Err(PeerCredentialsError::SocketTypeQueryFailed);
    }
    if socket_type_len as usize != std::mem::size_of::<libc::c_int>() {
        return Err(PeerCredentialsError::SocketTypeQueryFailed);
    }
    if socket_type != libc::SOCK_STREAM {
        return Err(PeerCredentialsError::NotStream);
    }

    platform_peer_uid(fd)
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn platform_peer_uid(_fd: libc::c_int) -> Result<libc::uid_t, PeerCredentialsError> {
    Err(PeerCredentialsError::PeerCredentialQueryFailed)
}

#[cfg(all(test, not(any(target_os = "macos", target_os = "linux"))))]
#[test]
fn unsupported_platform_fails_closed() {
    let (stream, _peer) = std::os::unix::net::UnixStream::pair().expect("unix socket pair");
    let descriptor: OwnedFd = stream.into();
    assert_eq!(
        peer_uid(&descriptor),
        Err(PeerCredentialsError::PeerCredentialQueryFailed)
    );
}
