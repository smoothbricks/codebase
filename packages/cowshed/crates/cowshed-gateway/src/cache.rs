use std::{
    collections::HashMap,
    fmt, io,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use http_body::{Body, Frame, SizeHint};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio::{
    fs::{self, File, OpenOptions},
    io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWrite, AsyncWriteExt, ReadBuf, SeekFrom},
    sync::{mpsc, oneshot},
};
use uuid::Uuid;

pub const DEFAULT_HIGH_WATER_BYTES: u64 = 20 * 1024 * 1024 * 1024;
pub const DEFAULT_LOW_WATER_BYTES: u64 = 16 * 1024 * 1024 * 1024;
const CACHE_VERSION: u8 = 1;
const HEADER_REGION: u64 = 64 * 1024;
const MAX_HEADER_BYTES: usize = HEADER_REGION as usize - 4;
const STREAM_CHUNK_BYTES: usize = 64 * 1024;
const COMMAND_CAPACITY: usize = 128;
const FINAL_PREFIX: &str = "obj-";
const TEMP_PREFIX: &str = ".tmp-";

pub type CacheBodyError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Clone, Debug)]
pub struct CacheConfig {
    pub root: PathBuf,
    pub high_water_bytes: u64,
    pub low_water_bytes: u64,
    pub metadata_ttl: Duration,
}

impl CacheConfig {
    pub fn production(root: PathBuf) -> Self {
        Self {
            root,
            high_water_bytes: DEFAULT_HIGH_WATER_BYTES,
            low_water_bytes: DEFAULT_LOW_WATER_BYTES,
            metadata_ttl: Duration::from_secs(5 * 60),
        }
    }

    pub fn validate(&self) -> Result<(), CacheError> {
        if !self.root.is_absolute() {
            return Err(CacheError::InvalidRoot(
                "cache root must be absolute".to_owned(),
            ));
        }
        if self.low_water_bytes >= self.high_water_bytes {
            return Err(CacheError::InvalidLimits);
        }
        if self.metadata_ttl.is_zero() {
            return Err(CacheError::InvalidLimits);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum CacheNamespace {
    Anonymous,
    Project { repo_id: String },
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct CacheKey {
    namespace: CacheNamespace,
    protocol: &'static str,
    origin: String,
    path: String,
    immutable_digest: Option<[u8; 32]>,
}

impl CacheKey {
    pub fn new(
        namespace: CacheNamespace,
        protocol: &'static str,
        origin: String,
        path: String,
        immutable_digest: Option<[u8; 32]>,
    ) -> Result<Self, CacheError> {
        if protocol.is_empty() || origin.is_empty() || path.is_empty() {
            return Err(CacheError::InvalidKey);
        }
        if let CacheNamespace::Project { repo_id } = &namespace
            && repo_id.is_empty()
        {
            return Err(CacheError::InvalidKey);
        }
        Ok(Self {
            namespace,
            protocol,
            origin,
            path,
            immutable_digest,
        })
    }

    fn digest(&self) -> [u8; 32] {
        let mut digest = Sha256::new();
        match &self.namespace {
            CacheNamespace::Anonymous => digest.update([0]),
            CacheNamespace::Project { repo_id } => {
                digest.update([1]);
                update_component(&mut digest, repo_id.as_bytes());
            }
        }
        update_component(&mut digest, self.protocol.as_bytes());
        update_component(&mut digest, self.origin.as_bytes());
        update_component(&mut digest, self.path.as_bytes());
        if let Some(expected) = self.immutable_digest {
            digest.update([1]);
            digest.update(expected);
        } else {
            digest.update([0]);
        }
        digest.finalize().into()
    }
}

fn update_component(digest: &mut Sha256, value: &[u8]) {
    digest.update((value.len() as u64).to_be_bytes());
    digest.update(value);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObjectExpectation {
    pub length: u64,
    pub sha256: [u8; 32],
}

#[derive(Clone, Debug)]
pub struct CachedResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub content_length: u64,
    pub content_sha256: [u8; 32],
    pub expected: Option<ObjectExpectation>,
    pub stored_unix_ms: u64,
}

impl CachedResponse {
    pub fn etag(&self) -> Option<&HeaderValue> {
        self.headers.get(http::header::ETAG)
    }

    pub fn last_modified(&self) -> Option<&HeaderValue> {
        self.headers.get(http::header::LAST_MODIFIED)
    }
}

#[derive(Clone)]
pub struct Cache {
    commands: mpsc::Sender<Command>,
    config: Arc<CacheConfig>,
}

impl fmt::Debug for Cache {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Cache")
            .field("root", &self.config.root)
            .finish_non_exhaustive()
    }
}

impl Cache {
    pub async fn open(config: CacheConfig) -> Result<Self, CacheError> {
        config.validate()?;
        validate_root(&config.root).await?;
        cleanup_temps(&config.root).await?;
        let entries = load_entries(&config.root).await?;
        let config = Arc::new(config);
        let (commands, receiver) = mpsc::channel(COMMAND_CAPACITY);
        let actor = CacheActor::new(Arc::clone(&config), entries, commands.clone(), receiver);
        tokio::spawn(actor.run());
        Ok(Self { commands, config })
    }

    pub fn metadata_ttl(&self) -> Duration {
        self.config.metadata_ttl
    }

    pub async fn acquire(
        &self,
        key: CacheKey,
        allow_stale: bool,
    ) -> Result<CacheAcquire, CacheError> {
        let digest = key.digest();
        let (reply, receiver) = oneshot::channel();
        self.commands
            .send(Command::Acquire {
                digest,
                allow_stale,
                reply,
            })
            .await
            .map_err(|_| CacheError::ActorStopped)?;
        receiver.await.map_err(|_| CacheError::ActorStopped)?
    }

    pub async fn retry_after_wait(&self, wait: CacheWait) -> Result<(), CacheError> {
        wait.receiver.await.map_err(|_| CacheError::ActorStopped)?
    }

    pub async fn open_candidate(&self, candidate: CacheCandidate) -> Result<CacheHit, CacheError> {
        match open_and_validate(&candidate.path, &candidate.response).await {
            Ok(file) => {
                let content_length = candidate.response.content_length;
                Ok(CacheHit {
                    response: candidate.response,
                    body: CacheReadBody {
                        file,
                        remaining: content_length,
                        buffer: vec![0; STREAM_CHUNK_BYTES],
                        commands: self.commands.clone(),
                        digest: candidate.digest,
                        generation: candidate.generation,
                        released: false,
                    },
                })
            }
            Err(error) => {
                let (reply, receiver) = oneshot::channel();
                self.commands
                    .send(Command::Corrupt {
                        digest: candidate.digest,
                        generation: candidate.generation,
                        reply,
                    })
                    .await
                    .map_err(|_| CacheError::ActorStopped)?;
                receiver.await.map_err(|_| CacheError::ActorStopped)??;
                Err(error)
            }
        }
    }

    pub async fn validate_previous(
        &self,
        permit: &FillPermit,
    ) -> Result<Option<CachedResponse>, CacheError> {
        let Some(candidate) = permit.previous.as_ref() else {
            return Ok(None);
        };
        match open_and_validate(&candidate.path, &candidate.response).await {
            Ok(file) => {
                drop(file);
                Ok(Some(candidate.response.clone()))
            }
            Err(error) => {
                permit.mark_corrupt().await?;
                Err(error)
            }
        }
    }

    pub async fn start_fill<B>(
        &self,
        permit: FillPermit,
        response: CachedResponse,
        max_bytes: u64,
        source: B,
    ) -> Result<CacheFillBody<B>, CacheError>
    where
        B: Body<Data = Bytes, Error = CacheBodyError> + Send + Unpin + 'static,
    {
        let temp_name = format!("{TEMP_PREFIX}{}", Uuid::new_v4().simple());
        let temp_path = self.config.root.join(temp_name);
        let file = create_new_nofollow(&temp_path).await?;
        let mut writer = file;
        writer.seek(SeekFrom::Start(HEADER_REGION)).await?;
        Ok(CacheFillBody {
            source,
            writer: Some(writer),
            pending: None,
            pending_offset: 0,
            digest: Sha256::new(),
            bytes: 0,
            max_bytes,
            response: Some(response),
            permit: Some(permit),
            temp_path: Some(temp_path),
            state: FillState::Streaming,
        })
    }
}

pub enum CacheAcquire {
    Hit(CacheCandidate),
    Fill(FillPermit),
    Wait(CacheWait),
}

pub struct CacheWait {
    receiver: oneshot::Receiver<Result<(), CacheError>>,
}

#[derive(Clone)]
pub struct CacheCandidate {
    digest: [u8; 32],
    generation: u64,
    path: PathBuf,
    pub response: CachedResponse,
}

pub struct CacheHit {
    pub response: CachedResponse,
    pub body: CacheReadBody,
}

pub struct FillPermit {
    commands: mpsc::Sender<Command>,
    digest: [u8; 32],
    generation: u64,
    previous: Option<CacheCandidate>,
    completed: bool,
}

impl FillPermit {
    pub fn previous(&self) -> Option<&CachedResponse> {
        self.previous.as_ref().map(|candidate| &candidate.response)
    }

    pub async fn not_modified(mut self) -> Result<CacheCandidate, CacheError> {
        let (reply, receiver) = oneshot::channel();
        self.commands
            .send(Command::NotModified {
                digest: self.digest,
                generation: self.generation,
                refreshed_unix_ms: unix_ms(SystemTime::now())?,
                reply,
            })
            .await
            .map_err(|_| CacheError::ActorStopped)?;
        let candidate = receiver.await.map_err(|_| CacheError::ActorStopped)??;
        self.completed = true;
        Ok(candidate)
    }

    pub async fn bypass(mut self) -> Result<(), CacheError> {
        self.finish_without_commit().await?;
        self.completed = true;
        Ok(())
    }

    async fn mark_corrupt(&self) -> Result<(), CacheError> {
        let (reply, receiver) = oneshot::channel();
        self.commands
            .send(Command::CorruptDuringFill {
                digest: self.digest,
                generation: self.generation,
                reply,
            })
            .await
            .map_err(|_| CacheError::ActorStopped)?;
        receiver.await.map_err(|_| CacheError::ActorStopped)??;
        Ok(())
    }

    async fn finish_without_commit(&self) -> Result<(), CacheError> {
        let (reply, receiver) = oneshot::channel();
        self.commands
            .send(Command::AbortFill {
                digest: self.digest,
                generation: self.generation,
                reply: Some(reply),
            })
            .await
            .map_err(|_| CacheError::ActorStopped)?;
        receiver.await.map_err(|_| CacheError::ActorStopped)?;
        Ok(())
    }
}

impl Drop for FillPermit {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        let command = Command::AbortFill {
            digest: self.digest,
            generation: self.generation,
            reply: None,
        };
        match self.commands.try_send(command) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(command)) => {
                let commands = self.commands.clone();
                tokio::spawn(async move {
                    let _ = commands.send(command).await;
                });
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {}
        }
    }
}

pub struct CacheReadBody {
    file: File,
    remaining: u64,
    buffer: Vec<u8>,
    commands: mpsc::Sender<Command>,
    digest: [u8; 32],
    generation: u64,
    released: bool,
}

impl CacheReadBody {
    fn release(&mut self) {
        if self.released {
            return;
        }
        self.released = true;
        let command = Command::Release {
            digest: self.digest,
            generation: self.generation,
        };
        match self.commands.try_send(command) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(command)) => {
                let commands = self.commands.clone();
                tokio::spawn(async move {
                    let _ = commands.send(command).await;
                });
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {}
        }
    }
}

impl Drop for CacheReadBody {
    fn drop(&mut self) {
        self.release();
    }
}

impl Body for CacheReadBody {
    type Data = Bytes;
    type Error = CacheBodyError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        if self.remaining == 0 {
            self.release();
            return Poll::Ready(None);
        }
        let read_len = usize::try_from(self.remaining.min(self.buffer.len() as u64))
            .expect("read length is bounded by the buffer");
        let this = &mut *self;
        let mut read_buf = ReadBuf::new(&mut this.buffer[..read_len]);
        match Pin::new(&mut this.file).poll_read(cx, &mut read_buf) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => {
                this.release();
                Poll::Ready(Some(Err(Box::new(error))))
            }
            Poll::Ready(Ok(())) if read_buf.filled().is_empty() => {
                this.release();
                Poll::Ready(Some(Err("cache object truncated after validation".into())))
            }
            Poll::Ready(Ok(())) => {
                let length = read_buf.filled().len();
                this.remaining -= length as u64;
                let bytes = Bytes::copy_from_slice(read_buf.filled());
                Poll::Ready(Some(Ok(Frame::data(bytes))))
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.remaining == 0
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.remaining)
    }
}

pub struct CacheFillBody<B> {
    source: B,
    writer: Option<File>,
    pending: Option<Bytes>,
    pending_offset: usize,
    digest: Sha256,
    bytes: u64,
    max_bytes: u64,
    response: Option<CachedResponse>,
    permit: Option<FillPermit>,
    temp_path: Option<PathBuf>,
    state: FillState,
}

enum FillState {
    Streaming,
    Finalizing(Pin<Box<dyn Future<Output = Result<(), CacheError>> + Send + Sync>>),
    Done,
}

impl<B> Unpin for CacheFillBody<B> where B: Unpin {}

impl<B> Body for CacheFillBody<B>
where
    B: Body<Data = Bytes, Error = CacheBodyError> + Send + Unpin + 'static,
{
    type Data = Bytes;
    type Error = CacheBodyError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        loop {
            match &mut self.state {
                FillState::Done => return Poll::Ready(None),
                FillState::Finalizing(future) => match future.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(())) => {
                        self.state = FillState::Done;
                        return Poll::Ready(None);
                    }
                    Poll::Ready(Err(error)) => {
                        self.state = FillState::Done;
                        return Poll::Ready(Some(Err(Box::new(error))));
                    }
                },
                FillState::Streaming => {}
            }

            if self.pending.is_some() {
                let pending = self.pending.take().expect("pending bytes exist");
                if self
                    .bytes
                    .checked_add(pending.len() as u64)
                    .is_none_or(|length| length > self.max_bytes)
                {
                    self.state = FillState::Done;
                    return Poll::Ready(Some(Err(Box::new(CacheError::ObjectTooLarge))));
                }
                let offset = self.pending_offset;
                let writer = self.writer.as_mut().expect("writer exists while streaming");
                match Pin::new(writer).poll_write(cx, &pending[offset..]) {
                    Poll::Pending => {
                        self.pending = Some(pending);
                        return Poll::Pending;
                    }
                    Poll::Ready(Err(error)) => {
                        self.state = FillState::Done;
                        return Poll::Ready(Some(Err(Box::new(error))));
                    }
                    Poll::Ready(Ok(0)) => {
                        self.state = FillState::Done;
                        return Poll::Ready(Some(Err(
                            "cache temporary file stopped accepting bytes".into(),
                        )));
                    }
                    Poll::Ready(Ok(written)) => {
                        self.pending_offset += written;
                        if self.pending_offset < pending.len() {
                            self.pending = Some(pending);
                            continue;
                        }
                        self.pending_offset = 0;
                        self.digest.update(&pending);
                        self.bytes = self.bytes.saturating_add(pending.len() as u64);
                        return Poll::Ready(Some(Ok(Frame::data(pending))));
                    }
                }
            }

            match Pin::new(&mut self.source).poll_frame(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Err(error))) => {
                    self.state = FillState::Done;
                    return Poll::Ready(Some(Err(error)));
                }
                Poll::Ready(Some(Ok(frame))) => match frame.into_data() {
                    Ok(data) if data.is_empty() => continue,
                    Ok(data) => {
                        self.pending = Some(data);
                    }
                    Err(frame) => return Poll::Ready(Some(Ok(frame))),
                },
                Poll::Ready(None) => {
                    let writer = self.writer.take().expect("writer exists while streaming");
                    let permit = self.permit.take().expect("permit exists while streaming");
                    let mut response = self
                        .response
                        .take()
                        .expect("response exists while streaming");
                    let temp_path = self.temp_path.take().expect("temporary path exists");
                    response.content_length = self.bytes;
                    response.content_sha256 = self.digest.clone().finalize().into();
                    let cleanup = TempCleanup(temp_path.clone());
                    let future = Box::pin(async move {
                        let _cleanup = cleanup;
                        finalize_fill(writer, temp_path, response, permit).await
                    });
                    self.state = FillState::Finalizing(future);
                }
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        matches!(self.state, FillState::Done)
    }

    fn size_hint(&self) -> SizeHint {
        self.source.size_hint()
    }
}

impl<B> Drop for CacheFillBody<B> {
    fn drop(&mut self) {
        if let Some(path) = self.temp_path.take() {
            tokio::spawn(async move {
                let _ = fs::remove_file(path).await;
            });
        }
    }
}

struct TempCleanup(PathBuf);

impl Drop for TempCleanup {
    fn drop(&mut self) {
        let path = self.0.clone();
        tokio::spawn(async move {
            let _ = fs::remove_file(path).await;
        });
    }
}

async fn finalize_fill(
    mut writer: File,
    temp_path: PathBuf,
    response: CachedResponse,
    mut permit: FillPermit,
) -> Result<(), CacheError> {
    if let Some(expected) = response.expected
        && (response.content_length != expected.length
            || response.content_sha256 != expected.sha256)
    {
        drop(writer);
        let _ = fs::remove_file(&temp_path).await;
        permit.finish_without_commit().await?;
        permit.completed = true;
        return Err(CacheError::DigestMismatch);
    }
    let disk = DiskRecord::from_response(permit.digest, permit.generation, &response)?;
    let encoded = serde_json::to_vec(&disk).map_err(CacheError::MetadataEncode)?;
    if encoded.len() > MAX_HEADER_BYTES {
        let _ = fs::remove_file(&temp_path).await;
        permit.finish_without_commit().await?;
        permit.completed = true;
        return Err(CacheError::MetadataTooLarge);
    }
    writer.seek(SeekFrom::Start(0)).await?;
    writer
        .write_all(&(encoded.len() as u32).to_be_bytes())
        .await?;
    writer.write_all(&encoded).await?;
    writer.sync_all().await?;
    drop(writer);

    let final_path = permit.commands_path(&temp_path)?;
    fs::rename(&temp_path, &final_path).await?;
    sync_parent(final_path.parent().ok_or(CacheError::Containment)?).await?;
    let stored_bytes = fs::metadata(&final_path).await?.len();
    let (reply, receiver) = oneshot::channel();
    permit
        .commands
        .send(Command::Commit {
            digest: permit.digest,
            generation: permit.generation,
            path: final_path,
            response: Box::new(response),
            stored_bytes,
            reply,
        })
        .await
        .map_err(|_| CacheError::ActorStopped)?;
    receiver.await.map_err(|_| CacheError::ActorStopped)??;
    permit.completed = true;
    Ok(())
}

impl FillPermit {
    fn commands_path(&self, temp_path: &Path) -> Result<PathBuf, CacheError> {
        let root = temp_path.parent().ok_or(CacheError::Containment)?;
        Ok(root.join(final_name(&self.digest)))
    }
}

#[derive(Clone)]
struct Entry {
    generation: u64,
    path: PathBuf,
    response: CachedResponse,
    stored_bytes: u64,
    active_readers: usize,
    last_access: u64,
}

struct FillStateEntry {
    generation: u64,
    waiters: Vec<oneshot::Sender<Result<(), CacheError>>>,
}

enum Command {
    Acquire {
        digest: [u8; 32],
        allow_stale: bool,
        reply: oneshot::Sender<Result<CacheAcquire, CacheError>>,
    },
    Release {
        digest: [u8; 32],
        generation: u64,
    },
    Corrupt {
        digest: [u8; 32],
        generation: u64,
        reply: oneshot::Sender<Result<(), CacheError>>,
    },
    CorruptDuringFill {
        digest: [u8; 32],
        generation: u64,
        reply: oneshot::Sender<Result<(), CacheError>>,
    },
    AbortFill {
        digest: [u8; 32],
        generation: u64,
        reply: Option<oneshot::Sender<()>>,
    },
    Commit {
        digest: [u8; 32],
        generation: u64,
        path: PathBuf,
        response: Box<CachedResponse>,
        stored_bytes: u64,
        reply: oneshot::Sender<Result<(), CacheError>>,
    },
    NotModified {
        digest: [u8; 32],
        generation: u64,
        refreshed_unix_ms: u64,
        reply: oneshot::Sender<Result<CacheCandidate, CacheError>>,
    },
}

struct CacheActor {
    config: Arc<CacheConfig>,
    entries: HashMap<[u8; 32], Entry>,
    fills: HashMap<[u8; 32], FillStateEntry>,
    commands: mpsc::Sender<Command>,
    receiver: mpsc::Receiver<Command>,
    next_generation: u64,
    next_access: u64,
    total_bytes: u64,
}

impl CacheActor {
    fn new(
        config: Arc<CacheConfig>,
        entries: HashMap<[u8; 32], Entry>,
        commands: mpsc::Sender<Command>,
        receiver: mpsc::Receiver<Command>,
    ) -> Self {
        let total_bytes = entries.values().map(|entry| entry.stored_bytes).sum();
        let next_generation = entries
            .values()
            .map(|entry| entry.generation)
            .max()
            .unwrap_or(0)
            .saturating_add(1);
        let next_access = entries
            .values()
            .map(|entry| entry.last_access)
            .max()
            .unwrap_or(0)
            .saturating_add(1);
        Self {
            config,
            entries,
            fills: HashMap::new(),
            commands,
            receiver,
            next_generation,
            next_access,
            total_bytes,
        }
    }

    async fn run(mut self) {
        let _ = self.evict_if_needed().await;
        while let Some(command) = self.receiver.recv().await {
            match command {
                Command::Acquire {
                    digest,
                    allow_stale,
                    reply,
                } => {
                    let result = self.acquire(digest, allow_stale);
                    let _ = reply.send(result);
                }
                Command::Release { digest, generation } => {
                    if let Some(entry) = self.entries.get_mut(&digest)
                        && entry.generation == generation
                    {
                        entry.active_readers = entry.active_readers.saturating_sub(1);
                    }
                    let _ = self.evict_if_needed().await;
                }
                Command::Corrupt {
                    digest,
                    generation,
                    reply,
                } => {
                    let result = self.remove_entry(digest, generation).await;
                    let _ = reply.send(result);
                }
                Command::CorruptDuringFill {
                    digest,
                    generation,
                    reply,
                } => {
                    let result = if self
                        .fills
                        .get(&digest)
                        .is_some_and(|fill| fill.generation == generation)
                    {
                        if let Some(entry) = self.entries.remove(&digest) {
                            self.total_bytes = self.total_bytes.saturating_sub(entry.stored_bytes);
                            remove_generated_file(&entry.path).await
                        } else {
                            Ok(())
                        }
                    } else {
                        Err(CacheError::StalePermit)
                    };
                    let _ = reply.send(result);
                }
                Command::AbortFill {
                    digest,
                    generation,
                    reply,
                } => {
                    self.finish_fill(digest, generation, Err(CacheError::FillAborted));
                    if let Some(reply) = reply {
                        let _ = reply.send(());
                    }
                }
                Command::Commit {
                    digest,
                    generation,
                    path,
                    response,
                    stored_bytes,
                    reply,
                } => {
                    let result = self
                        .commit(digest, generation, path, *response, stored_bytes)
                        .await;
                    let notification = result
                        .as_ref()
                        .map(|_| ())
                        .map_err(|error| error.for_waiter());
                    self.finish_fill(digest, generation, notification);
                    let _ = reply.send(result);
                }
                Command::NotModified {
                    digest,
                    generation,
                    refreshed_unix_ms,
                    reply,
                } => {
                    let result = self.not_modified(digest, generation, refreshed_unix_ms);
                    let notification = result
                        .as_ref()
                        .map(|_| ())
                        .map_err(|error| error.for_waiter());
                    self.finish_fill(digest, generation, notification);
                    let _ = reply.send(result);
                }
            }
        }
    }

    fn acquire(&mut self, digest: [u8; 32], allow_stale: bool) -> Result<CacheAcquire, CacheError> {
        if let Some(fill) = self.fills.get_mut(&digest) {
            let (reply, receiver) = oneshot::channel();
            fill.waiters.push(reply);
            return Ok(CacheAcquire::Wait(CacheWait { receiver }));
        }
        let now = unix_ms(SystemTime::now())?;
        if let Some(entry) = self.entries.get_mut(&digest) {
            let age = Duration::from_millis(now.saturating_sub(entry.response.stored_unix_ms));
            if age <= self.config.metadata_ttl || entry.response.expected.is_some() || allow_stale {
                entry.active_readers = entry.active_readers.saturating_add(1);
                entry.last_access = self.next_access;
                self.next_access = self.next_access.saturating_add(1);
                return Ok(CacheAcquire::Hit(CacheCandidate {
                    digest,
                    generation: entry.generation,
                    path: entry.path.clone(),
                    response: entry.response.clone(),
                }));
            }
            let candidate = CacheCandidate {
                digest,
                generation: entry.generation,
                path: entry.path.clone(),
                response: entry.response.clone(),
            };
            let generation = self.allocate_generation();
            self.fills.insert(
                digest,
                FillStateEntry {
                    generation,
                    waiters: Vec::new(),
                },
            );
            return Ok(CacheAcquire::Fill(FillPermit {
                commands: self.commands.clone(),
                digest,
                generation,
                previous: Some(candidate),
                completed: false,
            }));
        }
        let generation = self.allocate_generation();
        self.fills.insert(
            digest,
            FillStateEntry {
                generation,
                waiters: Vec::new(),
            },
        );
        Ok(CacheAcquire::Fill(FillPermit {
            commands: self.commands.clone(),
            digest,
            generation,
            previous: None,
            completed: false,
        }))
    }

    fn allocate_generation(&mut self) -> u64 {
        let generation = self.next_generation;
        self.next_generation = self.next_generation.saturating_add(1);
        generation
    }

    async fn remove_entry(&mut self, digest: [u8; 32], generation: u64) -> Result<(), CacheError> {
        if self
            .entries
            .get(&digest)
            .is_some_and(|entry| entry.generation != generation)
        {
            return Err(CacheError::StalePermit);
        }
        if let Some(entry) = self.entries.remove(&digest) {
            self.total_bytes = self.total_bytes.saturating_sub(entry.stored_bytes);
            remove_generated_file(&entry.path).await?;
        }
        Ok(())
    }

    async fn commit(
        &mut self,
        digest: [u8; 32],
        generation: u64,
        path: PathBuf,
        response: CachedResponse,
        stored_bytes: u64,
    ) -> Result<(), CacheError> {
        if !self
            .fills
            .get(&digest)
            .is_some_and(|fill| fill.generation == generation)
        {
            return Err(CacheError::StalePermit);
        }
        if let Some(old) = self.entries.insert(
            digest,
            Entry {
                generation,
                path,
                response,
                stored_bytes,
                active_readers: 0,
                last_access: self.next_access,
            },
        ) {
            self.total_bytes = self.total_bytes.saturating_sub(old.stored_bytes);
        }
        self.next_access = self.next_access.saturating_add(1);
        self.total_bytes = self.total_bytes.saturating_add(stored_bytes);
        self.evict_if_needed().await
    }

    fn not_modified(
        &mut self,
        digest: [u8; 32],
        generation: u64,
        refreshed_unix_ms: u64,
    ) -> Result<CacheCandidate, CacheError> {
        if !self
            .fills
            .get(&digest)
            .is_some_and(|fill| fill.generation == generation)
        {
            return Err(CacheError::StalePermit);
        }
        let entry = self.entries.get_mut(&digest).ok_or(CacheError::CacheMiss)?;
        entry.response.stored_unix_ms = refreshed_unix_ms;
        entry.active_readers = entry.active_readers.saturating_add(1);
        entry.last_access = self.next_access;
        self.next_access = self.next_access.saturating_add(1);
        Ok(CacheCandidate {
            digest,
            generation: entry.generation,
            path: entry.path.clone(),
            response: entry.response.clone(),
        })
    }

    fn finish_fill(&mut self, digest: [u8; 32], generation: u64, result: Result<(), CacheError>) {
        if !self
            .fills
            .get(&digest)
            .is_some_and(|fill| fill.generation == generation)
        {
            return;
        }
        if let Some(fill) = self.fills.remove(&digest) {
            for waiter in fill.waiters {
                let value = result
                    .as_ref()
                    .map(|_| ())
                    .map_err(|error| error.for_waiter());
                let _ = waiter.send(value);
            }
        }
    }

    async fn evict_if_needed(&mut self) -> Result<(), CacheError> {
        if self.total_bytes <= self.config.high_water_bytes {
            return Ok(());
        }
        let mut candidates = self
            .entries
            .iter()
            .filter(|(digest, entry)| {
                entry.active_readers == 0 && !self.fills.contains_key(*digest)
            })
            .map(|(digest, entry)| (*digest, entry.last_access))
            .collect::<Vec<_>>();
        candidates.sort_unstable_by_key(|(_, access)| *access);
        for (digest, _) in candidates {
            if self.total_bytes <= self.config.low_water_bytes {
                break;
            }
            if let Some(entry) = self.entries.remove(&digest) {
                self.total_bytes = self.total_bytes.saturating_sub(entry.stored_bytes);
                remove_generated_file(&entry.path).await?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum CacheError {
    #[error("invalid cache root: {0}")]
    InvalidRoot(String),
    #[error("cache low-water limit must be below its non-zero high-water limit")]
    InvalidLimits,
    #[error("cache key components must be non-empty")]
    InvalidKey,
    #[error("cache actor stopped")]
    ActorStopped,
    #[error("cache entry is missing")]
    CacheMiss,
    #[error("cache fill was aborted")]
    FillAborted,
    #[error("cache object length or digest does not match protocol metadata")]
    DigestMismatch,
    #[error("cache fill exceeded its declared bounded length")]
    ObjectTooLarge,
    #[error("cache metadata exceeds its bounded header region")]
    MetadataTooLarge,
    #[error("cache path escaped its configured root")]
    Containment,
    #[error("cache operation used a stale fill or reader lease")]
    StalePermit,
    #[error("cache metadata is invalid")]
    InvalidMetadata,
    #[error("cache I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("cache metadata encoding failed: {0}")]
    MetadataEncode(serde_json::Error),
}

impl CacheError {
    fn for_waiter(&self) -> Self {
        match self {
            Self::DigestMismatch => Self::DigestMismatch,
            Self::FillAborted => Self::FillAborted,
            Self::StalePermit => Self::StalePermit,
            Self::CacheMiss => Self::CacheMiss,
            _ => Self::ActorStopped,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct DiskRecord {
    version: u8,
    key_sha256: String,
    generation: u64,
    status: u16,
    headers: Vec<(String, String)>,
    content_length: u64,
    content_sha256: String,
    expected_length: Option<u64>,
    expected_sha256: Option<String>,
    stored_unix_ms: u64,
}

impl DiskRecord {
    fn from_response(
        digest: [u8; 32],
        generation: u64,
        response: &CachedResponse,
    ) -> Result<Self, CacheError> {
        let mut headers = Vec::with_capacity(response.headers.len());
        for (name, value) in &response.headers {
            if is_sensitive_header(name) {
                continue;
            }
            let text = value.to_str().map_err(|_| CacheError::InvalidMetadata)?;
            headers.push((name.as_str().to_owned(), text.to_owned()));
        }
        Ok(Self {
            version: CACHE_VERSION,
            key_sha256: hex_encode(&digest),
            generation,
            status: response.status.as_u16(),
            headers,
            content_length: response.content_length,
            content_sha256: hex_encode(&response.content_sha256),
            expected_length: response.expected.map(|expected| expected.length),
            expected_sha256: response
                .expected
                .map(|expected| hex_encode(&expected.sha256)),
            stored_unix_ms: response.stored_unix_ms,
        })
    }

    fn into_entry(self, path: PathBuf, stored_bytes: u64) -> Result<([u8; 32], Entry), CacheError> {
        if self.version != CACHE_VERSION || stored_bytes < HEADER_REGION {
            return Err(CacheError::InvalidMetadata);
        }
        let digest = hex_decode_32(&self.key_sha256)?;
        let content_sha256 = hex_decode_32(&self.content_sha256)?;
        let expected = match (self.expected_length, self.expected_sha256) {
            (Some(length), Some(sha256)) => Some(ObjectExpectation {
                length,
                sha256: hex_decode_32(&sha256)?,
            }),
            (None, None) => None,
            _ => return Err(CacheError::InvalidMetadata),
        };
        let mut headers = HeaderMap::new();
        for (name, value) in self.headers {
            let name =
                HeaderName::from_bytes(name.as_bytes()).map_err(|_| CacheError::InvalidMetadata)?;
            if is_sensitive_header(&name) {
                return Err(CacheError::InvalidMetadata);
            }
            let value = HeaderValue::from_str(&value).map_err(|_| CacheError::InvalidMetadata)?;
            headers.append(name, value);
        }
        let status = StatusCode::from_u16(self.status).map_err(|_| CacheError::InvalidMetadata)?;
        Ok((
            digest,
            Entry {
                generation: self.generation,
                path,
                response: CachedResponse {
                    status,
                    headers,
                    content_length: self.content_length,
                    content_sha256,
                    expected,
                    stored_unix_ms: self.stored_unix_ms,
                },
                stored_bytes,
                active_readers: 0,
                last_access: self.stored_unix_ms,
            },
        ))
    }
}

fn is_sensitive_header(name: &HeaderName) -> bool {
    name == http::header::AUTHORIZATION
        || name == http::header::PROXY_AUTHORIZATION
        || name == http::header::COOKIE
        || name == http::header::SET_COOKIE
        || name.as_str().eq_ignore_ascii_case("npm-auth-type")
        || name.as_str().eq_ignore_ascii_case("npm-otp")
}

async fn validate_root(root: &Path) -> Result<(), CacheError> {
    let metadata = fs::symlink_metadata(root).await.map_err(|error| {
        CacheError::InvalidRoot(format!("existing cache root cannot be opened: {error}"))
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(CacheError::InvalidRoot(
            "cache root must be an existing real directory".to_owned(),
        ));
    }
    Ok(())
}

async fn cleanup_temps(root: &Path) -> Result<(), CacheError> {
    let mut entries = fs::read_dir(root).await?;
    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if name.starts_with(TEMP_PREFIX) {
            let metadata = fs::symlink_metadata(entry.path()).await?;
            if metadata.file_type().is_file() || metadata.file_type().is_symlink() {
                fs::remove_file(entry.path()).await?;
            }
        }
    }
    sync_parent(root).await
}

async fn load_entries(root: &Path) -> Result<HashMap<[u8; 32], Entry>, CacheError> {
    let mut loaded = HashMap::new();
    let mut entries = fs::read_dir(root).await?;
    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if !name.starts_with(FINAL_PREFIX) {
            continue;
        }
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path).await?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            remove_generated_file(&path).await?;
            continue;
        }
        match read_disk_record(&path).await.and_then(|record| {
            let expected_name = final_name(&hex_decode_32(&record.key_sha256)?);
            if name != expected_name {
                return Err(CacheError::InvalidMetadata);
            }
            record.into_entry(path.clone(), metadata.len())
        }) {
            Ok((digest, value)) => {
                loaded.insert(digest, value);
            }
            Err(_) => {
                remove_generated_file(&path).await?;
            }
        }
    }
    Ok(loaded)
}

async fn read_disk_record(path: &Path) -> Result<DiskRecord, CacheError> {
    let mut file = open_existing_nofollow(path).await?;
    let mut length = [0; 4];
    file.read_exact(&mut length).await?;
    let length = u32::from_be_bytes(length) as usize;
    if length == 0 || length > MAX_HEADER_BYTES {
        return Err(CacheError::InvalidMetadata);
    }
    let mut encoded = vec![0; length];
    file.read_exact(&mut encoded).await?;
    serde_json::from_slice(&encoded).map_err(|_| CacheError::InvalidMetadata)
}

async fn open_and_validate(path: &Path, response: &CachedResponse) -> Result<File, CacheError> {
    let mut file = open_existing_nofollow(path).await?;
    let metadata = file.metadata().await?;
    let expected_file_len = HEADER_REGION
        .checked_add(response.content_length)
        .ok_or(CacheError::InvalidMetadata)?;
    if metadata.len() != expected_file_len {
        return Err(CacheError::DigestMismatch);
    }
    file.seek(SeekFrom::Start(HEADER_REGION)).await?;
    let mut remaining = response.content_length;
    let mut digest = Sha256::new();
    let mut buffer = vec![0; STREAM_CHUNK_BYTES];
    while remaining > 0 {
        let length = usize::try_from(remaining.min(buffer.len() as u64))
            .expect("validation read is bounded by the buffer");
        let read = file.read(&mut buffer[..length]).await?;
        if read == 0 {
            return Err(CacheError::DigestMismatch);
        }
        digest.update(&buffer[..read]);
        remaining -= read as u64;
    }
    let actual: [u8; 32] = digest.finalize().into();
    if actual != response.content_sha256
        || response.expected.is_some_and(|expected| {
            expected.length != response.content_length || expected.sha256 != actual
        })
    {
        return Err(CacheError::DigestMismatch);
    }
    file.seek(SeekFrom::Start(HEADER_REGION)).await?;
    Ok(file)
}

#[cfg(unix)]
async fn create_new_nofollow(path: &Path) -> Result<File, CacheError> {
    let mut options = OpenOptions::new();
    options
        .write(true)
        .read(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW);
    Ok(options.open(path).await?)
}

#[cfg(not(unix))]
async fn create_new_nofollow(path: &Path) -> Result<File, CacheError> {
    let mut options = OpenOptions::new();
    options.write(true).read(true).create_new(true);
    Ok(options.open(path).await?)
}

#[cfg(unix)]
async fn open_existing_nofollow(path: &Path) -> Result<File, CacheError> {
    let mut options = OpenOptions::new();
    options.read(true).custom_flags(libc::O_NOFOLLOW);
    Ok(options.open(path).await?)
}

#[cfg(not(unix))]
async fn open_existing_nofollow(path: &Path) -> Result<File, CacheError> {
    Ok(OpenOptions::new().read(true).open(path).await?)
}

async fn sync_parent(root: &Path) -> Result<(), CacheError> {
    let directory = File::open(root).await?;
    directory.sync_all().await?;
    Ok(())
}

async fn remove_generated_file(path: &Path) -> Result<(), CacheError> {
    match fs::remove_file(path).await {
        Ok(()) => {
            sync_parent(path.parent().ok_or(CacheError::Containment)?).await?;
            Ok(())
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn final_name(digest: &[u8; 32]) -> String {
    format!("{FINAL_PREFIX}{}", hex_encode(digest))
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0xf) as usize] as char);
    }
    encoded
}

fn hex_decode_32(value: &str) -> Result<[u8; 32], CacheError> {
    if value.len() != 64 {
        return Err(CacheError::InvalidMetadata);
    }
    let mut decoded = [0; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        decoded[index] = (hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?;
    }
    Ok(decoded)
}

fn hex_nibble(value: u8) -> Result<u8, CacheError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err(CacheError::InvalidMetadata),
    }
}

fn unix_ms(time: SystemTime) -> Result<u64, CacheError> {
    let duration = time
        .duration_since(UNIX_EPOCH)
        .map_err(|_| CacheError::InvalidMetadata)?;
    u64::try_from(duration.as_millis()).map_err(|_| CacheError::InvalidMetadata)
}
