//! Bidirectional `gate-watch` service for served workspace supervisors (05_gateway.md "A land's
//! gate takes tokens back").
//!
//! Each served supervisor opens one persistent Unix connection to `<store>/gateway.sock` naming
//! its `.groups` ledger under `<store>/run/`. A supervisor that admits a land's gate job sends
//! `gate-start` at admission and `gate-end` at conclusion; the gateway broadcasts `GateWatchState`
//! on open, on every `active` transition, and on every heartbeat (`deadline::beats()`, 5 s) after
//! verifying the gateway actor is still turning (`handle.ping()`).
//!
//! Dropping a supervisor's connection drops all gate registrations it held and clears
//! `PRIO_DARWIN_BG` on every recorded live process in its validated `.groups` ledger at once.

use std::collections::BTreeSet;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};

use cowshed_core::gate_watch::{GateWatchEvent, GateWatchState};
use cowshed_os::deadline;
use tokio::io::{AsyncBufReadExt as _, AsyncRead, AsyncWrite, AsyncWriteExt as _, BufReader};
use tokio::sync::watch;

use crate::actor::GatewayHandle;

#[derive(Default)]
struct Inner {
    next_conn_id: u64,
    active_jobs: BTreeSet<(u64, u64)>,
    seq: u64,
}

/// Shared state of active land's gate jobs across all connected supervisors.
#[derive(Clone)]
pub(crate) struct GateWatch {
    inner: Arc<Mutex<Inner>>,
    state: watch::Sender<(bool, u64)>,
}

impl std::fmt::Debug for GateWatch {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("GateWatch").finish_non_exhaustive()
    }
}

impl GateWatch {
    pub(crate) fn new() -> Self {
        let (state, _) = watch::channel((false, 0_u64));
        Self {
            inner: Arc::new(Mutex::new(Inner::default())),
            state,
        }
    }

    fn connect(&self) -> (u64, watch::Receiver<(bool, u64)>) {
        let mut inner = self.inner.lock().expect("gate-watch lock poisoned");
        inner.next_conn_id = inner.next_conn_id.saturating_add(1);
        let id = inner.next_conn_id;
        let rx = self.state.subscribe();
        (id, rx)
    }

    fn register(&self, conn_id: u64, job_id: u64) {
        let transition = {
            let mut inner = self.inner.lock().expect("gate-watch lock poisoned");
            let before = !inner.active_jobs.is_empty();
            inner.active_jobs.insert((conn_id, job_id));
            let after = !inner.active_jobs.is_empty();
            if before != after {
                inner.seq = inner.seq.saturating_add(1);
                Some((after, inner.seq))
            } else {
                None
            }
        };
        if let Some(next) = transition {
            let _ = self.state.send(next);
        }
    }

    fn unregister(&self, conn_id: u64, job_id: u64) {
        let transition = {
            let mut inner = self.inner.lock().expect("gate-watch lock poisoned");
            let before = !inner.active_jobs.is_empty();
            inner.active_jobs.remove(&(conn_id, job_id));
            let after = !inner.active_jobs.is_empty();
            if before != after {
                inner.seq = inner.seq.saturating_add(1);
                Some((after, inner.seq))
            } else {
                None
            }
        };
        if let Some(next) = transition {
            let _ = self.state.send(next);
        }
    }

    fn disconnect(&self, conn_id: u64) {
        let transition = {
            let mut inner = self.inner.lock().expect("gate-watch lock poisoned");
            let before = !inner.active_jobs.is_empty();
            inner.active_jobs.retain(|(owner, _)| *owner != conn_id);
            let after = !inner.active_jobs.is_empty();
            if before != after {
                inner.seq = inner.seq.saturating_add(1);
                Some((after, inner.seq))
            } else {
                None
            }
        };
        if let Some(next) = transition {
            let _ = self.state.send(next);
        }
    }
}

/// Validate that a supervisor's claimed `.groups` ledger lies directly under `<store>/run/`
/// without traversal components.
pub(crate) fn validate_ledger_path(ledger: &Path, run_root: &Path) -> Result<PathBuf, String> {
    if !ledger.is_absolute() {
        return Err("gate-watch ledger path must be absolute".to_owned());
    }
    if ledger
        .components()
        .any(|component| matches!(component, Component::ParentDir | Component::CurDir))
    {
        return Err("gate-watch ledger path must not contain relative traversal".to_owned());
    }
    if ledger.extension().and_then(|ext| ext.to_str()) != Some("groups") {
        return Err("gate-watch ledger path must name a .groups file".to_owned());
    }
    let parent = ledger
        .parent()
        .ok_or_else(|| "gate-watch ledger path has no parent directory".to_owned())?;
    let expected = std::fs::canonicalize(run_root).unwrap_or_else(|_| run_root.to_path_buf());
    let actual = std::fs::canonicalize(parent).unwrap_or_else(|_| parent.to_path_buf());
    if actual != expected {
        return Err(format!(
            "gate-watch ledger {} must lie directly under {}",
            ledger.display(),
            expected.display()
        ));
    }
    let file_name = ledger
        .file_name()
        .ok_or_else(|| "gate-watch ledger path has no file name".to_owned())?;
    Ok(expected.join(file_name))
}

async fn write_state<W: AsyncWrite + Unpin>(
    writer: &mut W,
    active: bool,
    seq: u64,
) -> std::io::Result<()> {
    let frame = GateWatchState {
        ok: true,
        active,
        seq,
    };
    let mut bytes = serde_json::to_vec(&frame).map_err(std::io::Error::other)?;
    bytes.push(b'\n');
    writer.write_all(&bytes).await?;
    writer.flush().await
}

/// Serve one persistent supervisor `gate-watch` connection until EOF, error, or gateway shutdown.
pub(crate) async fn serve<R, W>(
    reader: R,
    mut writer: W,
    ledger: Option<PathBuf>,
    gate_watch: GateWatch,
    handle: GatewayHandle,
) where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let (conn_id, mut rx) = gate_watch.connect();
    let initial = *rx.borrow_and_update();
    if write_state(&mut writer, initial.0, initial.1)
        .await
        .is_err()
    {
        return;
    }
    let mut lines = BufReader::new(reader).lines();
    let mut beats = deadline::beats();
    loop {
        tokio::select! {
            line = lines.next_line() => {
                let Ok(Some(line)) = line else {
                    break;
                };
                match serde_json::from_str::<GateWatchEvent>(&line) {
                    Ok(GateWatchEvent::GateStart { job_id }) => {
                        gate_watch.register(conn_id, job_id);
                    }
                    Ok(GateWatchEvent::GateEnd { job_id }) => {
                        gate_watch.unregister(conn_id, job_id);
                    }
                    Err(_) => break,
                }
            }
            changed = rx.changed() => {
                if changed.is_err() {
                    break;
                }
                let (active, seq) = *rx.borrow_and_update();
                if write_state(&mut writer, active, seq).await.is_err() {
                    break;
                }
            }
            _ = beats.tick() => {
                if handle.ping().await.is_err() {
                    break;
                }
                let (active, seq) = *rx.borrow();
                if write_state(&mut writer, active, seq).await.is_err() {
                    break;
                }
            }
        }
    }
    if let Some(ledger) = ledger {
        let cleared = tokio::task::spawn_blocking(move || {
            cowshed_core::runtime::job_groups::clear_recorded_darwinbg(&ledger)
        })
        .await;
        if let Ok(Err(error)) = cleared {
            cowshed_os::eprintln!(
                "cowshed: clearing PRIO_DARWIN_BG after supervisor gate-watch disconnect failed: {error}"
            );
        }
    }
    gate_watch.disconnect(conn_id);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ledger_validation_requires_direct_groups_child_under_run_root() {
        let run = Path::new("/tmp/cowshed-store/run");
        assert!(validate_ledger_path(Path::new("/tmp/cowshed-store/run/ws.groups"), run).is_ok());
        assert!(validate_ledger_path(Path::new("ws.groups"), run).is_err());
        assert!(
            validate_ledger_path(Path::new("/tmp/cowshed-store/run/../ws.groups"), run).is_err()
        );
        assert!(
            validate_ledger_path(Path::new("/tmp/cowshed-store/run/sub/ws.groups"), run).is_err()
        );
        assert!(validate_ledger_path(Path::new("/tmp/cowshed-store/run/ws.sock"), run).is_err());
        assert!(validate_ledger_path(Path::new("/etc/passwd.groups"), run).is_err());
    }

    #[test]
    fn active_tracks_multiple_supervisors_and_disconnect_drops_registrations() {
        let watch = GateWatch::new();
        let (c1, rx1) = watch.connect();
        let (c2, _rx2) = watch.connect();
        assert_eq!(*rx1.borrow(), (false, 0));

        watch.register(c1, 1);
        assert_eq!(*rx1.borrow(), (true, 1));
        watch.register(c2, 2);
        assert_eq!(*rx1.borrow(), (true, 1));
        watch.unregister(c1, 1);
        assert_eq!(*rx1.borrow(), (true, 1), "c2 still holds job 2");
        watch.disconnect(c2);
        assert_eq!(*rx1.borrow(), (false, 2), "disconnecting c2 drops job 2");
    }
}
