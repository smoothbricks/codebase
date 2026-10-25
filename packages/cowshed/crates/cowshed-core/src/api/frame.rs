use super::peer_credentials::{self, PeerCredentialsError};
use crate::error::Result;
use std::os::fd::OwnedFd;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Length-prefixed unix-socket frame. Client handshake and server RPC share this codec.
pub(crate) async fn write_frame(
    stream: &mut (impl AsyncWrite + Unpin),
    bytes: &[u8],
    maximum: usize,
    map_invalid: impl Fn() -> crate::error::CowshedError,
    map_io: impl Fn(std::io::Error) -> crate::error::CowshedError,
) -> Result<()> {
    let length = frame_length(bytes, maximum).ok_or_else(map_invalid)?;
    write_frame_io(stream, length, bytes).await.map_err(map_io)
}

pub(crate) async fn read_frame(
    stream: &mut (impl AsyncRead + Unpin),
    maximum: usize,
    map_invalid: impl Fn() -> crate::error::CowshedError,
    map_io: impl Fn(std::io::Error) -> crate::error::CowshedError,
) -> Result<Vec<u8>> {
    read_frame_io(stream, maximum)
        .await
        .map_err(map_io)?
        .ok_or_else(map_invalid)
}

/// The length prefix of a frame holding `bytes`; `None` when `bytes` is empty or longer than
/// `maximum`.
pub(crate) fn frame_length(bytes: &[u8], maximum: usize) -> Option<[u8; 4]> {
    if bytes.is_empty() || bytes.len() > maximum {
        return None;
    }
    u32::try_from(bytes.len()).ok().map(u32::to_be_bytes)
}

/// Write one frame whose prefix [`frame_length`] gave, with the stream's own error: a bounded
/// stream's error carries the bound it ran past.
pub(crate) async fn write_frame_io(
    stream: &mut (impl AsyncWrite + Unpin),
    length: [u8; 4],
    bytes: &[u8],
) -> std::io::Result<()> {
    stream.write_all(&length).await?;
    stream.write_all(bytes).await
}

/// Read one frame with the stream's own error; `Ok(None)` when its announced length is zero or
/// past `maximum`.
pub(crate) async fn read_frame_io(
    stream: &mut (impl AsyncRead + Unpin),
    maximum: usize,
) -> std::io::Result<Option<Vec<u8>>> {
    let mut length_bytes = [0_u8; 4];
    stream.read_exact(&mut length_bytes).await?;
    let length = match usize::try_from(u32::from_be_bytes(length_bytes)) {
        Ok(length) if length != 0 && length <= maximum => length,
        Ok(_) | Err(_) => return Ok(None),
    };
    let mut bytes = vec![0_u8; length];
    stream.read_exact(&mut bytes).await?;
    Ok(Some(bytes))
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
