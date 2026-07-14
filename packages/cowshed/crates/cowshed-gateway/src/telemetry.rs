use std::{
    fs::{self, File, OpenOptions},
    io,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_array::{ArrayRef, RecordBatch, StringArray, UInt16Array, UInt64Array};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::interfaces::{AuditError, AuditEvent, AuditKind, AuditSink, AuditStatus};

/// Configuration for immutable one-batch gateway audit segments.
#[derive(Clone, Debug)]
pub struct ArrowAuditConfig {
    pub root: PathBuf,
    pub channel_capacity: NonZeroUsize,
}

impl ArrowAuditConfig {
    pub fn new(root: PathBuf) -> Result<Self, AuditError> {
        if !root.is_absolute() {
            return Err(AuditError("telemetry root must be absolute".to_owned()));
        }
        Ok(Self {
            root,
            channel_capacity: NonZeroUsize::new(256).expect("256 is non-zero"),
        })
    }
}

/// Durable production audit sink writing mode-0600 immutable Arrow IPC segments.
///
/// The gateway actor supplies total event order. This sink has a separate bounded
/// file-I/O actor, publishes each event with create-new + fsync + atomic rename,
/// and never appends to or shares a segment.
pub struct ArrowAuditSink {
    commands: mpsc::Sender<WriterCommand>,
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
        prepare_root(&config.root)?;
        let (commands, receiver) = mpsc::channel(config.channel_capacity.get());
        let writer = AuditWriter {
            root: config.root,
            writer_id: Uuid::new_v4(),
            last_sequence: 0,
            receiver,
        };
        tokio::spawn(writer.run());
        Ok(Self { commands })
    }
}

#[async_trait::async_trait]
impl AuditSink for ArrowAuditSink {
    async fn record(&self, event: AuditEvent) -> Result<(), AuditError> {
        let (reply, receive) = oneshot::channel();
        self.commands
            .send(WriterCommand::Record { event, reply })
            .await
            .map_err(|_| AuditError("Arrow audit writer stopped".to_owned()))?;
        receive
            .await
            .map_err(|_| AuditError("Arrow audit writer stopped".to_owned()))?
    }

    async fn flush(&self) -> Result<(), AuditError> {
        let (reply, receive) = oneshot::channel();
        self.commands
            .send(WriterCommand::Flush { reply })
            .await
            .map_err(|_| AuditError("Arrow audit writer stopped".to_owned()))?;
        receive
            .await
            .map_err(|_| AuditError("Arrow audit writer stopped".to_owned()))?
    }
}

// Keeping the event inline avoids one heap allocation on every audit decision;
// the channel is bounded, so its memory footprint is fixed by configuration.
#[allow(clippy::large_enum_variant)]
enum WriterCommand {
    Record {
        event: AuditEvent,
        reply: oneshot::Sender<Result<(), AuditError>>,
    },
    Flush {
        reply: oneshot::Sender<Result<(), AuditError>>,
    },
}

struct AuditWriter {
    root: PathBuf,
    writer_id: Uuid,
    last_sequence: u64,
    receiver: mpsc::Receiver<WriterCommand>,
}

impl AuditWriter {
    async fn run(mut self) {
        while let Some(command) = self.receiver.recv().await {
            match command {
                WriterCommand::Record { event, reply } => {
                    let result = if event.sequence <= self.last_sequence {
                        Err(AuditError(
                            "audit sequence is not strictly increasing".to_owned(),
                        ))
                    } else {
                        let root = self.root.clone();
                        let writer_id = self.writer_id;
                        let sequence = event.sequence;
                        match tokio::task::spawn_blocking(move || {
                            write_segment(&root, writer_id, event)
                        })
                        .await
                        {
                            Ok(result) => {
                                if result.is_ok() {
                                    self.last_sequence = sequence;
                                }
                                result
                            }
                            Err(error) => Err(AuditError(format!(
                                "Arrow audit writer task failed: {error}"
                            ))),
                        }
                    };
                    let _ = reply.send(result);
                }
                WriterCommand::Flush { reply } => {
                    let root = self.root.clone();
                    let result = tokio::task::spawn_blocking(move || sync_directory(&root))
                        .await
                        .map_err(|error| AuditError(format!("Arrow flush task failed: {error}")))
                        .and_then(|result| result);
                    let _ = reply.send(result);
                }
            }
        }
    }
}

fn prepare_root(root: &Path) -> Result<(), AuditError> {
    if !root.is_absolute() {
        return Err(AuditError("telemetry root must be absolute".to_owned()));
    }
    if let Ok(metadata) = fs::symlink_metadata(root)
        && metadata.file_type().is_symlink()
    {
        return Err(AuditError("telemetry root cannot be a symlink".to_owned()));
    }
    fs::create_dir_all(root).map_err(io_error("creating telemetry root"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(root, fs::Permissions::from_mode(0o700))
            .map_err(io_error("securing telemetry root"))?;
    }
    Ok(())
}

fn write_segment(root: &Path, writer_id: Uuid, event: AuditEvent) -> Result<(), AuditError> {
    let timestamp = time::OffsetDateTime::from_unix_timestamp_nanos(
        i128::from(event.timestamp_unix_ms) * 1_000_000,
    )
    .map_err(|_| AuditError("audit timestamp is outside the UTC calendar".to_owned()))?;
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
    let stem = format!("gateway-{:020}-{writer_id}", event.sequence);
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
    let mut file = options
        .open(&temporary)
        .map_err(io_error("creating audit segment"))?;
    let batch = event_batch(event)?;
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
}

fn event_batch(event: AuditEvent) -> Result<RecordBatch, AuditError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("sequence", DataType::UInt64, false),
        Field::new("timestamp_unix_ms", DataType::UInt64, false),
        Field::new("workspace_id", DataType::Utf8, false),
        Field::new("revision", DataType::UInt64, false),
        Field::new("endpoint", DataType::Utf8, false),
        Field::new("kind", DataType::Utf8, false),
        Field::new("host", DataType::Utf8, true),
        Field::new("method", DataType::Utf8, true),
        Field::new("path", DataType::Utf8, true),
        Field::new("status", DataType::Utf8, false),
        Field::new("http_status", DataType::UInt16, true),
        Field::new("bytes", DataType::UInt64, false),
        Field::new("trace_id", DataType::Utf8, true),
        Field::new("grant_hint", DataType::Utf8, true),
        Field::new("classification", DataType::Utf8, true),
    ]));
    let kind = match event.kind {
        AuditKind::Http => "http",
        AuditKind::Connect => "connect",
        AuditKind::Intercept => "intercept",
        AuditKind::Opaque => "opaque",
        AuditKind::Npm => "npm",
        AuditKind::Cargo => "cargo",
        AuditKind::Go => "go",
    };
    let status = match event.status {
        AuditStatus::Allowed => "allowed",
        AuditStatus::Denied => "denied",
        AuditStatus::Unauthorized => "unauthorized",
        AuditStatus::Limited => "limited",
        AuditStatus::Offline => "offline",
        AuditStatus::Failed => "failed",
        AuditStatus::Completed => "completed",
        AuditStatus::TimedOut => "timed-out",
        AuditStatus::Cancelled => "cancelled",
    };
    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(vec![event.sequence])),
        Arc::new(UInt64Array::from(vec![event.timestamp_unix_ms])),
        Arc::new(StringArray::from(vec![event.workspace_id])),
        Arc::new(UInt64Array::from(vec![event.revision])),
        Arc::new(StringArray::from(vec![event.endpoint])),
        Arc::new(StringArray::from(vec![kind])),
        Arc::new(StringArray::from(vec![event.host])),
        Arc::new(StringArray::from(vec![event.method])),
        Arc::new(StringArray::from(vec![event.path])),
        Arc::new(StringArray::from(vec![status])),
        Arc::new(UInt16Array::from(vec![event.http_status])),
        Arc::new(UInt64Array::from(vec![event.bytes])),
        Arc::new(StringArray::from(vec![event.trace_id])),
        Arc::new(StringArray::from(vec![event.grant_hint])),
        Arc::new(StringArray::from(vec![event.classification])),
    ];
    RecordBatch::try_new(schema, columns)
        .map_err(|error| AuditError(format!("building audit batch: {error}")))
}

fn sync_directory(path: &Path) -> Result<(), AuditError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(io_error("syncing telemetry directory"))
}

fn io_error(operation: &'static str) -> impl FnOnce(io::Error) -> AuditError {
    move |error| AuditError(format!("{operation}: {error}"))
}
