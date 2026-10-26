//! Span sealing cadence shared by every writer that gathers rows into a segment.

use std::time::Duration;

/// How long a writer gathers process rows before it seals them as one segment. A process fold
/// batch is not a durability event: sealing one is a full-drive flush of the segment and of its
/// directory (`Durability::PowerLoss`), and one seal per fold batch made 26 seals and about 52
/// `F_FULLFSYNC`s a second host-wide, 5-8 s of flushing per wall second at 0.10-0.16 s each,
/// which saturated the drive under every other process on it (measured 2026-10-21). So the writer
/// seals on this interval, and the durability of each seal is unchanged. Rows still waiting
/// when the process dies are lost, as the gateway's one-batch audit window is (13_telemetry.md,
/// "Process segments").
pub const PROCESS_SEAL_INTERVAL: Duration = Duration::from_secs(10);
