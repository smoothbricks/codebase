use super::PeerCredentialsError;

pub(super) fn peer_uid(fd: libc::c_int) -> Result<libc::uid_t, PeerCredentialsError> {
    let mut credentials = libc::ucred {
        pid: 0,
        uid: 0,
        gid: 0,
    };
    let mut credentials_len = libc::socklen_t::try_from(std::mem::size_of::<libc::ucred>())
        .map_err(|_| PeerCredentialsError::PeerCredentialQueryFailed)?;
    let result = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            std::ptr::from_mut(&mut credentials).cast(),
            &mut credentials_len,
        )
    };
    if result != 0 {
        return Err(PeerCredentialsError::PeerCredentialQueryFailed);
    }
    // SO_PEERCRED includes gid, but uid is the authorization boundary for this socket.
    Ok(credentials.uid)
}
