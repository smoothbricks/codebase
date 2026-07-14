use std::{
    collections::VecDeque,
    fs::{self, File, OpenOptions},
    io,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use arrow_array::{
    ArrayRef, RecordBatch, StringArray, TimestampNanosecondArray, UInt16Array, UInt64Array,
};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use tokio::{
    sync::{mpsc, oneshot},
    time::Instant,
};
use uuid::Uuid;

use crate::interfaces::{AuditError, AuditEvent, AuditSink, AuditStatus};

const DEFAULT_BATCH_CAPACITY: usize = 64;
const DEFAULT_TAIL_CAPACITY: usize = 1_024;
const DEFAULT_SUBSCRIBERS: usize = 16;
const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_millis(25);

/// Configuration for immutable, batched gateway trace segments.
#[derive(Clone, Debug)]
pub struct ArrowAuditConfig {
    pub root: PathBuf,
    pub channel_capacity: NonZeroUsize,
    pub batch_capacity: NonZeroUsize,
    pub tail_capacity: NonZeroUsize,
    pub max_subscribers: NonZeroUsize,
    pub flush_interval: Duration,
}

impl ArrowAuditConfig {
    pub fn new(root: PathBuf) -> Result<Self, AuditError> {
        if !root.is_absolute() {
            return Err(AuditError("telemetry root must be absolute".to_owned()));
        }
        Ok(Self {
            root,
            channel_capacity: NonZeroUsize::new(256).expect("256 is non-zero"),
            batch_capacity: NonZeroUsize::new(DEFAULT_BATCH_CAPACITY)
                .expect("default batch capacity is non-zero"),
            tail_capacity: NonZeroUsize::new(DEFAULT_TAIL_CAPACITY)
                .expect("default tail capacity is non-zero"),
            max_subscribers: NonZeroUsize::new(DEFAULT_SUBSCRIBERS)
                .expect("default subscriber capacity is non-zero"),
            flush_interval: DEFAULT_FLUSH_INTERVAL,
        })
    }
}

/// A bounded query used by the authenticated control-plane audit tail.
#[derive(Clone, Debug, Default)]
pub(crate) struct AuditTailQuery {
    pub workspace_id: Option<String>,
    pub after_sequence: Option<u64>,
    pub limit: usize,
}

/// Read-only access to the writer's bounded, durable audit tail.
#[derive(Clone)]
pub(crate) struct AuditTailHandle {
    commands: mpsc::Sender<WriterCommand>,
}

impl std::fmt::Debug for AuditTailHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AuditTailHandle")
            .finish_non_exhaustive()
    }
}

impl AuditTailHandle {
    pub(crate) async fn query(&self, query: AuditTailQuery) -> Result<Vec<AuditEvent>, AuditError> {
        let (reply, receive) = oneshot::channel();
        self.commands
            .send(WriterCommand::Query { query, reply })
            .await
            .map_err(|_| writer_stopped())?;
        receive.await.map_err(|_| writer_stopped())?
    }

    pub(crate) async fn subscribe(
        &self,
        query: AuditTailQuery,
    ) -> Result<mpsc::Receiver<AuditEvent>, AuditError> {
        let (reply, receive) = oneshot::channel();
        self.commands
            .send(WriterCommand::Subscribe { query, reply })
            .await
            .map_err(|_| writer_stopped())?;
        receive.await.map_err(|_| writer_stopped())?
    }
}

/// Durable production audit sink writing mode-0600 immutable Arrow IPC segments.
///
/// The writer has one bounded command queue, one bounded in-memory batch, and a
/// bounded durable tail. Records are acknowledged only after their containing
/// segment has been fsynced and atomically published. Denial/failure decisions
/// force an immediate batch boundary; ordinary completions share a short timer.
pub struct ArrowAuditSink {
    commands: mpsc::Sender<WriterCommand>,
    tail: AuditTailHandle,
}

impl std::fmt::Debug for ArrowAuditSink {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ArrowAuditSink")
            .finish_non_exhaustive()
    }
}

impl ArrowAuditSink {
    pub fn start(config: ArrowAuditConfig) -> Result<Self, AuditError> {
        if config.flush_interval.is_zero() {
            return Err(AuditError(
                "telemetry flush interval must be non-zero".to_owned(),
            ));
        }
        prepare_root(&config.root)?;
        let (commands, receiver) = mpsc::channel(config.channel_capacity.get());
        let tail = AuditTailHandle {
            commands: commands.clone(),
        };
        let writer = AuditWriter {
            root: config.root,
            writer_id: Uuid::new_v4(),
            last_sequence: 0,
            receiver,
            pending: Vec::with_capacity(config.batch_capacity.get()),
            batch_capacity: config.batch_capacity.get(),
            flush_interval: config.flush_interval,
            flush_deadline: None,
            tail: VecDeque::with_capacity(config.tail_capacity.get()),
            tail_capacity: config.tail_capacity.get(),
            subscribers: Vec::with_capacity(config.max_subscribers.get()),
            max_subscribers: config.max_subscribers.get(),
        };
        tokio::spawn(writer.run());
        Ok(Self { commands, tail })
    }

    pub(crate) fn tail_handle(&self) -> AuditTailHandle {
        self.tail.clone()
    }
}

#[async_trait::async_trait]
impl AuditSink for ArrowAuditSink {
    async fn record(&self, event: AuditEvent) -> Result<(), AuditError> {
        let (reply, receive) = oneshot::channel();
        self.commands
            .send(WriterCommand::Record { event, reply })
            .await
            .map_err(|_| writer_stopped())?;
        receive.await.map_err(|_| writer_stopped())?
    }

    async fn flush(&self) -> Result<(), AuditError> {
        let (reply, receive) = oneshot::channel();
        self.commands
            .send(WriterCommand::Flush { reply })
            .await
            .map_err(|_| writer_stopped())?;
        receive.await.map_err(|_| writer_stopped())?
    }
}

fn writer_stopped() -> AuditError {
    AuditError("Arrow audit writer stopped".to_owned())
}

#[allow(clippy::large_enum_variant)]
enum WriterCommand {
    Record {
        event: AuditEvent,
        reply: oneshot::Sender<Result<(), AuditError>>,
    },
    Flush {
        reply: oneshot::Sender<Result<(), AuditError>>,
    },
    Query {
        query: AuditTailQuery,
        reply: oneshot::Sender<Result<Vec<AuditEvent>, AuditError>>,
    },
    Subscribe {
        query: AuditTailQuery,
        reply: oneshot::Sender<Result<mpsc::Receiver<AuditEvent>, AuditError>>,
    },
}

struct PendingRecord {
    event: AuditEvent,
    reply: oneshot::Sender<Result<(), AuditError>>,
}

struct TailSubscriber {
    query: AuditTailQuery,
    sender: mpsc::Sender<AuditEvent>,
}

struct AuditWriter {
    root: PathBuf,
    writer_id: Uuid,
    last_sequence: u64,
    receiver: mpsc::Receiver<WriterCommand>,
    pending: Vec<PendingRecord>,
    batch_capacity: usize,
    flush_interval: Duration,
    flush_deadline: Option<Instant>,
    tail: VecDeque<AuditEvent>,
    tail_capacity: usize,
    subscribers: Vec<TailSubscriber>,
    max_subscribers: usize,
}

impl AuditWriter {
    async fn run(mut self) {
        loop {
            if self.pending.is_empty() {
                let Some(command) = self.receiver.recv().await else {
                    break;
                };
                if !self.handle(command).await {
                    break;
                }
                continue;
            }

            let deadline = self
                .flush_deadline
                .expect("a pending audit batch always has a deadline");
            tokio::select! {
                biased;
                command = self.receiver.recv() => {
                    let Some(command) = command else {
                        break;
                    };
                    if !self.handle(command).await {
                        break;
                    }
                }
                () = tokio::time::sleep_until(deadline) => {
                    if self.flush_pending().await.is_err() {
                        break;
                    }
                }
            }
        }
        if !self.pending.is_empty() {
            let _ = self.flush_pending().await;
        }
    }

    async fn handle(&mut self, command: WriterCommand) -> bool {
        match command {
            WriterCommand::Record { event, reply } => {
                if let Err(error) = validate_event(&event, self.next_expected_sequence()) {
                    let _ = reply.send(Err(error));
                    return false;
                }
                let decision_boundary = is_decision_boundary(event.status);
                self.pending.push(PendingRecord { event, reply });
                self.flush_deadline
                    .get_or_insert_with(|| Instant::now() + self.flush_interval);
                if (decision_boundary || self.pending.len() >= self.batch_capacity)
                    && self.flush_pending().await.is_err()
                {
                    return false;
                }
                true
            }
            WriterCommand::Flush { reply } => {
                let result = self
                    .flush_pending()
                    .await
                    .and_then(|()| sync_directory(&self.root));
                let success = result.is_ok();
                let _ = reply.send(result);
                success
            }
            WriterCommand::Query { query, reply } => {
                let result = self.query_tail(&query);
                let _ = reply.send(result);
                true
            }
            WriterCommand::Subscribe { query, reply } => {
                let result = self.add_subscriber(query);
                let _ = reply.send(result);
                true
            }
        }
    }

    fn next_expected_sequence(&self) -> u64 {
        self.pending
            .last()
            .map_or(self.last_sequence.saturating_add(1), |record| {
                record.event.sequence.saturating_add(1)
            })
    }

    async fn flush_pending(&mut self) -> Result<(), AuditError> {
        self.flush_deadline = None;
        if self.pending.is_empty() {
            return Ok(());
        }
        let events = self
            .pending
            .iter()
            .map(|record| record.event.clone())
            .collect::<Vec<_>>();
        let root = self.root.clone();
        let writer_id = self.writer_id;
        let write_events = events.clone();
        let result =
            tokio::task::spawn_blocking(move || write_segment(&root, writer_id, &write_events))
                .await
                .map_err(|error| AuditError(format!("Arrow audit writer task failed: {error}")))
                .and_then(|result| result);

        if let Err(error) = result {
            let message = error.to_string();
            for record in self.pending.drain(..) {
                let _ = record.reply.send(Err(AuditError(message.clone())));
            }
            return Err(error);
        }

        self.last_sequence = events
            .last()
            .expect("non-empty batch has a final event")
            .sequence;
        self.publish_tail(&events);
        for record in self.pending.drain(..) {
            let _ = record.reply.send(Ok(()));
        }
        Ok(())
    }

    fn query_tail(&self, query: &AuditTailQuery) -> Result<Vec<AuditEvent>, AuditError> {
        self.validate_query(query)?;
        Ok(self
            .tail
            .iter()
            .filter(|event| matches_query(event, query))
            .take(query.limit)
            .cloned()
            .collect())
    }

    fn add_subscriber(
        &mut self,
        query: AuditTailQuery,
    ) -> Result<mpsc::Receiver<AuditEvent>, AuditError> {
        self.validate_query(&query)?;
        self.subscribers
            .retain(|subscriber| !subscriber.sender.is_closed());
        if self.subscribers.len() >= self.max_subscribers {
            return Err(AuditError(
                "audit tail subscriber capacity is exhausted".to_owned(),
            ));
        }
        let (sender, receiver) = mpsc::channel(query.limit);
        for event in self
            .tail
            .iter()
            .filter(|event| matches_query(event, &query))
        {
            sender.try_send(event.clone()).map_err(|_| {
                AuditError("audit tail backlog exceeds its bounded receiver".to_owned())
            })?;
        }
        self.subscribers.push(TailSubscriber { query, sender });
        Ok(receiver)
    }

    fn validate_query(&self, query: &AuditTailQuery) -> Result<(), AuditError> {
        if query.limit == 0 || query.limit > self.tail_capacity {
            return Err(AuditError(format!(
                "audit tail limit must be between 1 and {}",
                self.tail_capacity
            )));
        }
        Ok(())
    }

    fn publish_tail(&mut self, events: &[AuditEvent]) {
        for event in events {
            if self.tail.len() == self.tail_capacity {
                self.tail.pop_front();
            }
            self.tail.push_back(event.clone());
        }

        // Tail consumers are read-only projections. A slow or disconnected
        // consumer loses its subscription; it cannot poison the authoritative
        // durable writer or stop gateway egress.
        self.subscribers.retain(|subscriber| {
            for event in events
                .iter()
                .filter(|event| matches_query(event, &subscriber.query))
            {
                match subscriber.sender.try_send(event.clone()) {
                    Ok(()) => {}
                    Err(
                        mpsc::error::TrySendError::Closed(_)
                        | mpsc::error::TrySendError::Full(_),
                    ) => return false,
                }
            }
            true
        });
    }
}

fn matches_query(event: &AuditEvent, query: &AuditTailQuery) -> bool {
    query
        .workspace_id
        .as_deref()
        .is_none_or(|workspace| workspace == event.workspace_id)
        && query
            .after_sequence
            .is_none_or(|sequence| event.sequence > sequence)
}

fn is_decision_boundary(status: AuditStatus) -> bool {
    !matches!(status, AuditStatus::Allowed | AuditStatus::Completed)
}

fn validate_event(event: &AuditEvent, expected_sequence: u64) -> Result<(), AuditError> {
    if event.sequence != expected_sequence {
        return Err(AuditError(format!(
            "audit sequence must be contiguous: expected {expected_sequence}, received {}",
            event.sequence
        )));
    }
    if event.workspace_id.is_empty() || event.repo_id.is_empty() || event.endpoint.is_empty() {
        return Err(AuditError(
            "audit workspace, repo, and endpoint are required".to_owned(),
        ));
    }
    if event.mirror_cache_status.is_some()
        && !matches!(
            event.kind,
            crate::interfaces::AuditKind::Npm
                | crate::interfaces::AuditKind::Cargo
                | crate::interfaces::AuditKind::Go
        )
    {
        return Err(AuditError(
            "non-mirror audit event cannot carry mirror cache status".to_owned(),
        ));
    }
    if let Some(trace_id) = event.trace_id.as_deref()
        && !is_nonzero_hex(trace_id, 32)
    {
        return Err(AuditError(
            "audit trace id must be 32 non-zero hexadecimal digits".to_owned(),
        ));
    }
    if event.span_id == 0
        || event.upstream_span_id == Some(0)
        || event.upstream_span_id == Some(event.span_id)
    {
        return Err(AuditError(
            "audit span identities must be distinct and non-zero".to_owned(),
        ));
    }
    if event.parent_span_id == Some(0) {
        return Err(AuditError(
            "audit parent span id must be non-zero when present".to_owned(),
        ));
    }
    if event
        .tracestate
        .as_deref()
        .is_some_and(|value| value.len() > 512 || value.contains(['\r', '\n']))
    {
        return Err(AuditError("audit tracestate is invalid".to_owned()));
    }
    Ok(())
}

fn is_nonzero_hex(value: &str, width: usize) -> bool {
    value.len() == width
        && value.bytes().all(|byte| byte.is_ascii_hexdigit())
        && value.bytes().any(|byte| byte != b'0')
}

fn prepare_root(root: &Path) -> Result<(), AuditError> {
    if !root.is_absolute() {
        return Err(AuditError("telemetry root must be absolute".to_owned()));
    }
    let metadata =
        fs::symlink_metadata(root).map_err(io_error("opening existing telemetry root"))?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(AuditError(
            "telemetry root must be an existing real directory".to_owned(),
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(AuditError(
                "telemetry root must deny group and other access".to_owned(),
            ));
        }
    }
    Ok(())
}

fn write_segment(root: &Path, writer_id: Uuid, events: &[AuditEvent]) -> Result<(), AuditError> {
    let first = events
        .first()
        .ok_or_else(|| AuditError("cannot write an empty audit batch".to_owned()))?;
    let last = events.last().expect("non-empty batch has a last event");
    let timestamp = time::OffsetDateTime::from_unix_timestamp_nanos(
        i128::from(first.timestamp_unix_ms) * 1_000_000,
    )
    .map_err(|_| AuditError("audit timestamp is outside the UTC calendar".to_owned()))?;
    let batch = event_batch(events)?;
    let date = format!(
        "{:04}-{:02}-{:02}",
        timestamp.year(),
        u8::from(timestamp.month()),
        timestamp.day()
    );
    let partition = root.join(date);
    if let Ok(metadata) = fs::symlink_metadata(&partition)
        && metadata.file_type().is_symlink()
    {
        return Err(AuditError(
            "telemetry partition cannot be a symlink".to_owned(),
        ));
    }
    fs::create_dir_all(&partition).map_err(io_error("creating telemetry partition"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(&partition, fs::Permissions::from_mode(0o700))
            .map_err(io_error("securing telemetry partition"))?;
    }
    sync_directory(root)?;
    let stem = format!(
        "gateway-{:020}-{:020}-{writer_id}",
        first.sequence, last.sequence
    );
    let temporary = partition.join(format!(".{stem}.tmp"));
    let final_path = partition.join(format!("{stem}.arrow"));
    if final_path.exists() {
        return Err(AuditError("audit segment already exists".to_owned()));
    }
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    let result = (|| {
        let mut file = options
            .open(&temporary)
            .map_err(io_error("creating audit segment"))?;
        {
            let mut writer = StreamWriter::try_new(&mut file, &batch.schema())
                .map_err(|error| AuditError(format!("creating Arrow stream: {error}")))?;
            writer
                .write(&batch)
                .map_err(|error| AuditError(format!("writing Arrow batch: {error}")))?;
            writer
                .finish()
                .map_err(|error| AuditError(format!("finishing Arrow stream: {error}")))?;
        }
        file.sync_all().map_err(io_error("syncing audit segment"))?;
        fs::rename(&temporary, &final_path).map_err(io_error("publishing audit segment"))?;
        sync_directory(&partition)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn event_batch(events: &[AuditEvent]) -> Result<RecordBatch, AuditError> {
    let row_capacity = events.len().saturating_mul(4);
    let mut timestamp = Vec::with_capacity(row_capacity);
    let mut trace_id = Vec::with_capacity(row_capacity);
    let mut thread_id = Vec::with_capacity(row_capacity);
    let mut span_id = Vec::with_capacity(row_capacity);
    let mut parent_thread_id = Vec::with_capacity(row_capacity);
    let mut parent_span_id = Vec::with_capacity(row_capacity);
    let mut entry_type = Vec::with_capacity(row_capacity);
    let mut message = Vec::with_capacity(row_capacity);
    let mut sequence = Vec::with_capacity(row_capacity);
    let mut workspace_id = Vec::with_capacity(row_capacity);
    let mut repo_id = Vec::with_capacity(row_capacity);
    let mut revision = Vec::with_capacity(row_capacity);
    let mut endpoint = Vec::with_capacity(row_capacity);
    let mut kind = Vec::with_capacity(row_capacity);
    let mut host = Vec::with_capacity(row_capacity);
    let mut method = Vec::with_capacity(row_capacity);
    let mut path = Vec::with_capacity(row_capacity);
    let mut decision = Vec::with_capacity(row_capacity);
    let mut http_status = Vec::with_capacity(row_capacity);
    let mut bytes = Vec::with_capacity(row_capacity);
    let mut grant_hint = Vec::with_capacity(row_capacity);
    let mut classification = Vec::with_capacity(row_capacity);
    let mut mirror_cache_status = Vec::with_capacity(row_capacity);
    let mut tracestate = Vec::with_capacity(row_capacity);

    for event in events {
        let trace = event
            .trace_id
            .clone()
            .unwrap_or_else(|| format!("{:032x}", event.sequence.max(1)));
        let request_span = event.span_id;
        let upstream_span = event.upstream_span_id;
        let thread = event.sequence.max(1);
        let completed_ns = event.timestamp_unix_ms.saturating_mul(1_000_000);
        let request_start_ns = completed_ns.saturating_sub(3);
        let request_end = end_entry_type(event.status);
        let kind_value = enum_name(&event.kind)?;
        let decision_value = enum_name(&event.status)?;
        let cache_value = event
            .mirror_cache_status
            .as_ref()
            .map(enum_name)
            .transpose()?;

        let mut push_row = |at: u64,
                            current_span: u64,
                            current_parent: Option<u64>,
                            entry: &'static str,
                            span_name: &'static str| {
            timestamp.push(i64::try_from(at).unwrap_or(i64::MAX));
            trace_id.push(trace.clone());
            thread_id.push(thread);
            span_id.push(current_span);
            parent_thread_id.push(current_parent.map(|_| thread));
            parent_span_id.push(current_parent);
            entry_type.push(entry.to_owned());
            message.push(span_name.to_owned());
            sequence.push(event.sequence);
            workspace_id.push(event.workspace_id.clone());
            repo_id.push(event.repo_id.clone());
            revision.push(event.revision);
            endpoint.push(event.endpoint.clone());
            kind.push(kind_value.clone());
            host.push(event.host.clone());
            method.push(event.method.clone());
            path.push(event.path.clone());
            decision.push(decision_value.clone());
            http_status.push(event.http_status);
            bytes.push(event.bytes);
            grant_hint.push(event.grant_hint.clone());
            classification.push(event.classification.clone());
            mirror_cache_status.push(cache_value.clone());
            tracestate.push(event.tracestate.clone());
        };

        push_row(
            request_start_ns,
            request_span,
            event.parent_span_id,
            "span-start",
            "gateway.request",
        );
        if let Some(upstream_span) = upstream_span {
            push_row(
                request_start_ns.saturating_add(1),
                upstream_span,
                Some(request_span),
                "span-start",
                "gateway.upstream",
            );
            push_row(
                request_start_ns.saturating_add(2),
                upstream_span,
                Some(request_span),
                request_end,
                "gateway.upstream",
            );
        }
        push_row(
            request_start_ns.saturating_add(3),
            request_span,
            event.parent_span_id,
            request_end,
            "gateway.request",
        );
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("thread_id", DataType::UInt64, false),
        Field::new("span_id", DataType::UInt64, false),
        Field::new("parent_thread_id", DataType::UInt64, true),
        Field::new("parent_span_id", DataType::UInt64, true),
        Field::new("entry_type", DataType::Utf8, false),
        Field::new("message", DataType::Utf8, false),
        Field::new("sequence", DataType::UInt64, false),
        Field::new("workspace_id", DataType::Utf8, false),
        Field::new("repo_id", DataType::Utf8, false),
        Field::new("revision", DataType::UInt64, false),
        Field::new("endpoint", DataType::Utf8, false),
        Field::new("kind", DataType::Utf8, false),
        Field::new("host", DataType::Utf8, true),
        Field::new("method", DataType::Utf8, true),
        Field::new("path", DataType::Utf8, true),
        Field::new("decision", DataType::Utf8, false),
        Field::new("http_status", DataType::UInt16, true),
        Field::new("bytes", DataType::UInt64, false),
        Field::new("grant_hint", DataType::Utf8, true),
        Field::new("classification", DataType::Utf8, true),
        Field::new("mirror_cache_status", DataType::Utf8, true),
        Field::new("tracestate", DataType::Utf8, true),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(TimestampNanosecondArray::from(timestamp)),
        Arc::new(StringArray::from(trace_id)),
        Arc::new(UInt64Array::from(thread_id)),
        Arc::new(UInt64Array::from(span_id)),
        Arc::new(UInt64Array::from(parent_thread_id)),
        Arc::new(UInt64Array::from(parent_span_id)),
        Arc::new(StringArray::from(entry_type)),
        Arc::new(StringArray::from(message)),
        Arc::new(UInt64Array::from(sequence)),
        Arc::new(StringArray::from(workspace_id)),
        Arc::new(StringArray::from(repo_id)),
        Arc::new(UInt64Array::from(revision)),
        Arc::new(StringArray::from(endpoint)),
        Arc::new(StringArray::from(kind)),
        Arc::new(StringArray::from(host)),
        Arc::new(StringArray::from(method)),
        Arc::new(StringArray::from(path)),
        Arc::new(StringArray::from(decision)),
        Arc::new(UInt16Array::from(http_status)),
        Arc::new(UInt64Array::from(bytes)),
        Arc::new(StringArray::from(grant_hint)),
        Arc::new(StringArray::from(classification)),
        Arc::new(StringArray::from(mirror_cache_status)),
        Arc::new(StringArray::from(tracestate)),
    ];
    RecordBatch::try_new(schema, columns)
        .map_err(|error| AuditError(format!("building audit batch: {error}")))
}

fn enum_name<T: serde::Serialize>(value: &T) -> Result<String, AuditError> {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .ok_or_else(|| AuditError("audit enum did not serialize as a string".to_owned()))
}

fn end_entry_type(status: AuditStatus) -> &'static str {
    if matches!(status, AuditStatus::Allowed | AuditStatus::Completed) {
        "span-ok"
    } else if matches!(status, AuditStatus::Failed) {
        "span-exception"
    } else {
        "span-err"
    }
}

fn sync_directory(path: &Path) -> Result<(), AuditError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(io_error("syncing telemetry directory"))
}

fn io_error(operation: &'static str) -> impl FnOnce(io::Error) -> AuditError {
    move |error| AuditError(format!("{operation}: {error}"))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Array as _, StringArray, UInt64Array};
    use arrow_ipc::reader::StreamReader;

    use super::*;
    use crate::{interfaces::AuditKind, mirror::MirrorCacheStatus};

    struct TestRoot(PathBuf);

    impl TestRoot {
        fn new(label: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "cowshed-gateway-{label}-{}-{}",
                std::process::id(),
                Uuid::new_v4()
            ));
            fs::create_dir(&path).expect("create audit test root");
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt as _;
                fs::set_permissions(&path, fs::Permissions::from_mode(0o700))
                    .expect("secure audit test root");
            }
            Self(path)
        }

        fn segments(&self) -> Vec<PathBuf> {
            let mut segments = Vec::new();
            for partition in fs::read_dir(&self.0).expect("list audit root") {
                let partition = partition.expect("valid partition entry").path();
                if !partition.is_dir() {
                    continue;
                }
                for entry in fs::read_dir(partition).expect("list audit partition") {
                    let path = entry.expect("valid segment entry").path();
                    if path.extension().and_then(|value| value.to_str()) == Some("arrow") {
                        segments.push(path);
                    }
                }
            }
            segments.sort();
            segments
        }
    }

    impl Drop for TestRoot {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
            let _ = fs::remove_file(&self.0);
        }
    }

    fn event(sequence: u64, workspace: &str, status: AuditStatus) -> AuditEvent {
        AuditEvent {
            sequence,
            timestamp_unix_ms: 1_700_000_000_000_u64.saturating_add(sequence),
            workspace_id: workspace.to_owned(),
            repo_id: format!("repo-{workspace}"),
            revision: 7,
            endpoint: "127.0.0.1:40960".to_owned(),
            kind: AuditKind::Npm,
            host: Some("registry.test:443".to_owned()),
            method: Some("GET".to_owned()),
            path: Some("/npm/pkg".to_owned()),
            status,
            http_status: Some(200),
            bytes: sequence,
            trace_id: Some(format!("{:032x}", sequence.max(1))),
            span_id: sequence.saturating_mul(2).saturating_sub(1).max(1),
            upstream_span_id: Some(sequence.saturating_mul(2).max(2)),
            parent_span_id: Some(0x00f0_67aa_0ba9_02b7),
            tracestate: Some("vendor=opaque".to_owned()),
            grant_hint: None,
            classification: None,
            mirror_cache_status: Some(MirrorCacheStatus::Hit),
        }
    }

    fn read_batch(path: &Path) -> RecordBatch {
        let mut reader = StreamReader::try_new(File::open(path).expect("open trace segment"), None)
            .expect("open Arrow stream");
        let batch = reader
            .next()
            .expect("one trace batch")
            .expect("valid trace batch");
        assert!(reader.next().is_none(), "segment contains one record batch");
        batch
    }

    #[tokio::test]
    async fn batches_spans_in_order_and_exposes_a_bounded_tail() {
        let root = TestRoot::new("trace-batch");
        let mut config = ArrowAuditConfig::new(root.0.clone()).expect("audit config");
        config.batch_capacity = NonZeroUsize::new(3).expect("non-zero batch");
        config.flush_interval = Duration::from_secs(60);
        let sink = Arc::new(ArrowAuditSink::start(config).expect("start trace sink"));
        let tail = sink.tail_handle();
        let mut subscription = tail
            .subscribe(AuditTailQuery {
                workspace_id: Some("alpha".to_owned()),
                after_sequence: None,
                limit: 8,
            })
            .await
            .expect("subscribe to audit tail");

        let (first, second, third) = tokio::join!(
            sink.record(event(1, "alpha", AuditStatus::Completed)),
            sink.record(event(2, "beta", AuditStatus::Completed)),
            sink.record(event(3, "alpha", AuditStatus::Completed)),
        );
        first.expect("first trace committed");
        second.expect("second trace committed");
        third.expect("third trace committed");

        let segments = root.segments();
        assert_eq!(segments.len(), 1, "one fsync segment per batch");
        let batch = read_batch(&segments[0]);
        assert_eq!(
            batch.num_rows(),
            12,
            "request and upstream span lifecycle rows"
        );
        let schema = batch.schema();
        let fields = schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            &fields[..8],
            [
                "timestamp",
                "trace_id",
                "thread_id",
                "span_id",
                "parent_thread_id",
                "parent_span_id",
                "entry_type",
                "message",
            ]
        );
        let sequences = batch
            .column_by_name("sequence")
            .expect("sequence column")
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("UInt64 sequence");
        assert_eq!(
            (0..sequences.len())
                .map(|row| sequences.value(row))
                .collect::<Vec<_>>(),
            [1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3]
        );
        let entries = batch
            .column_by_name("entry_type")
            .expect("entry type column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string entry type");
        assert_eq!(entries.value(0), "span-start");
        assert_eq!(entries.value(1), "span-start");
        assert_eq!(entries.value(2), "span-ok");
        assert_eq!(entries.value(3), "span-ok");

        let first_tail = subscription
            .recv()
            .await
            .expect("first matching tail event");
        let second_tail = subscription
            .recv()
            .await
            .expect("second matching tail event");
        assert_eq!((first_tail.sequence, second_tail.sequence), (1, 3));
        let queried = tail
            .query(AuditTailQuery {
                workspace_id: None,
                after_sequence: Some(1),
                limit: 8,
            })
            .await
            .expect("query durable tail");
        assert_eq!(
            queried
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            [2, 3]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn short_timer_flushes_an_ordinary_completion() {
        let root = TestRoot::new("trace-timer");
        let mut config = ArrowAuditConfig::new(root.0.clone()).expect("audit config");
        config.flush_interval = Duration::from_millis(25);
        let sink = Arc::new(ArrowAuditSink::start(config).expect("start trace sink"));
        let task_sink = Arc::clone(&sink);
        let record = tokio::spawn(async move {
            task_sink
                .record(event(1, "timer", AuditStatus::Completed))
                .await
        });
        tokio::task::yield_now().await;
        assert!(
            !record.is_finished(),
            "completion waits for durable timer flush"
        );
        tokio::time::advance(Duration::from_millis(25)).await;
        record
            .await
            .expect("record task joined")
            .expect("timer batch committed");
        assert_eq!(root.segments().len(), 1);
    }

    #[tokio::test]
    async fn lagging_tail_is_disconnected_and_storage_failure_stops_the_writer() {
        let backpressure_root = TestRoot::new("trace-backpressure");
        let sink = ArrowAuditSink::start(
            ArrowAuditConfig::new(backpressure_root.0.clone()).expect("audit config"),
        )
        .expect("start trace sink");
        let tail = sink.tail_handle();
        let mut fast = tail
            .subscribe(AuditTailQuery {
                workspace_id: None,
                after_sequence: None,
                limit: 4,
            })
            .await
            .expect("subscribe healthy audit tail");
        let mut slow = tail
            .subscribe(AuditTailQuery {
                workspace_id: None,
                after_sequence: None,
                limit: 1,
            })
            .await
            .expect("subscribe bounded tail");
        sink.record(event(1, "slow", AuditStatus::Denied))
            .await
            .expect("first decision fills tail receiver");
        sink.record(event(2, "slow", AuditStatus::Denied))
            .await
            .expect("lagging projection cannot stop the writer");
        assert_eq!(
            slow.recv().await.expect("buffered first event").sequence,
            1
        );
        assert!(
            slow.recv().await.is_none(),
            "lagging subscription is disconnected"
        );
        sink.record(event(3, "slow", AuditStatus::Denied))
            .await
            .expect("writer remains authoritative");
        assert_eq!(
            (
                fast.recv().await.expect("fast event one").sequence,
                fast.recv().await.expect("fast event two").sequence,
                fast.recv().await.expect("fast event three").sequence,
            ),
            (1, 2, 3),
            "one malicious subscriber cannot isolate a healthy subscriber"
        );
        let durable = tail
            .query(AuditTailQuery {
                workspace_id: None,
                after_sequence: None,
                limit: 4,
            })
            .await
            .expect("query durable tail after subscriber lag");
        assert_eq!(
            durable
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            [1, 2, 3]
        );
        let mut catch_up = tail
            .subscribe(AuditTailQuery {
                workspace_id: None,
                after_sequence: Some(1),
                limit: 4,
            })
            .await
            .expect("resubscribe from durable sequence");
        assert_eq!(
            (
                catch_up.recv().await.expect("catch-up event two").sequence,
                catch_up.recv().await.expect("catch-up event three").sequence,
            ),
            (2, 3),
            "resubscription catches up from the bounded durable tail"
        );

        let failed_root = TestRoot::new("trace-storage-failure");
        let failed_path = failed_root.0.clone();
        let failed = ArrowAuditSink::start(
            ArrowAuditConfig::new(failed_path.clone()).expect("audit config"),
        )
        .expect("start trace sink");
        fs::remove_dir(&failed_path).expect("remove empty audit root");
        fs::write(&failed_path, b"not a directory").expect("replace root with a file");
        let error = failed
            .record(event(1, "failure", AuditStatus::Denied))
            .await
            .expect_err("storage failure must be reported");
        assert!(
            error.0.contains("creating telemetry partition")
                || error.0.contains("syncing telemetry directory")
        );
        assert!(
            failed
                .record(event(2, "failure", AuditStatus::Denied))
                .await
                .expect_err("failed writer remains stopped")
                .0
                .contains("stopped")
        );
    }
}
