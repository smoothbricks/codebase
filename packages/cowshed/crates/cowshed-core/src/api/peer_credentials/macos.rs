use super::PeerCredentialsError;

pub(super) fn peer_uid(fd: libc::c_int) -> Result<libc::uid_t, PeerCredentialsError> {
    let mut peer_uid: libc::uid_t = 0;
    let mut peer_gid: libc::gid_t = 0;
    // SAFETY: the caller supplies a live stream-socket fd and both output pointers are valid.
    let result = unsafe { libc::getpeereid(fd, &mut peer_uid, &mut peer_gid) };
    if result != 0 {
        return Err(PeerCredentialsError::PeerCredentialQueryFailed);
    }
    // macOS requires the gid out-parameter, but uid is the authorization boundary for this socket.
    Ok(peer_uid)
}
