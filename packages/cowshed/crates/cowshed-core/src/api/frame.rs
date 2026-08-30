use super::peer_credentials::{self, PeerCredentialsError};
use crate::error::Result;
use std::os::fd::OwnedFd;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

/// Length-prefixed unix-socket frame. Client handshake and server RPC share this codec.
pub(crate) async fn write_frame(
    stream: &mut UnixStream,
    bytes: &[u8],
    maximum: usize,
    map_invalid: impl Fn() -> crate::error::CowshedError,
    map_io: impl Fn(std::io::Error) -> crate::error::CowshedError,
) -> Result<()> {
    if bytes.is_empty() || bytes.len() > maximum {
        return Err(map_invalid());
    }
    let length = u32::try_from(bytes.len()).map_err(|_| map_invalid())?;
    stream
        .write_all(&length.to_be_bytes())
        .await
        .map_err(&map_io)?;
    stream.write_all(bytes).await.map_err(map_io)
}

pub(crate) async fn read_frame(
    stream: &mut UnixStream,
    maximum: usize,
    map_invalid: impl Fn() -> crate::error::CowshedError,
    map_io: impl Fn(std::io::Error) -> crate::error::CowshedError,
) -> Result<Vec<u8>> {
    let mut length_bytes = [0_u8; 4];
    stream
        .read_exact(&mut length_bytes)
        .await
        .map_err(&map_io)?;
    let length = usize::try_from(u32::from_be_bytes(length_bytes)).map_err(|_| map_invalid())?;
    if length == 0 || length > maximum {
        return Err(map_invalid());
    }
    let mut bytes = vec![0_u8; length];
    stream.read_exact(&mut bytes).await.map_err(map_io)?;
    Ok(bytes)
}

pub(crate) fn verify_peer(
    descriptor: &OwnedFd,
    map: impl Fn(PeerCredentialsError) -> crate::error::CowshedError,
) -> Result<()> {
    let peer_uid = peer_credentials::peer_uid(descriptor).map_err(&map)?;
    // SAFETY: geteuid has no preconditions and reads no caller-owned memory.
    let current_uid = unsafe { libc::geteuid() };
    if peer_uid != current_uid {
        return Err(map(PeerCredentialsError::PeerCredentialQueryFailed));
    }
    Ok(())
}
