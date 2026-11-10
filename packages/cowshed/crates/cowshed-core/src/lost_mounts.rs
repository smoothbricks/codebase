//! The gateway's watch on the volumes it holds attached (05_gateway.md, "Lost mounts").
//!
//! A volume can lose its mount while the kernel keeps its image attached: on 2026-11-10 a restart
//! of `fseventsd` left about 100 cowshed volumes so, every main and shed of the host, and each
//! stayed unmounted until an operator mounted main and attached every shed by hand. The kernel
//! says when any filesystem unmounts ([`MountTableEvents`]); the watch reads the mount table then,
//! names the mount points that went, and remounts every held workspace of the projects whose
//! mount roots held them, through the same settle and mount policy as every heal
//! ([`NativeGatewayInventory::remount_lost`]). A workspace whose image lock a live operation holds
//! is remounted once that holder lets go, by one wait per workspace the watch owns.

use std::collections::BTreeSet;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use cowshed_apfs::mounts::{MountTableEvents, kernel_mounts, vanished};
use tokio::sync::mpsc;

use crate::gateway_inventory::{LostMount, NativeGatewayInventory, RemountScope};
use crate::metadata::WorkspaceName;
use crate::repository::RepoId;
use crate::storage::apfs::Remount;

/// A running watch. Dropping it ends its passes; the kernel-event thread ends at its next event,
/// once nothing reads what it sends.
#[derive(Debug)]
pub struct LostMountWatch {
    passes: tokio::task::JoinHandle<()>,
}

impl Drop for LostMountWatch {
    fn drop(&mut self) {
        self.passes.abort();
    }
}

/// Start watching the volumes `inventory`'s projects hold: arm the kernel's filesystem events,
/// read the mount table they are compared with, then make one pass over every workspace, for a
/// mount lost while no gateway watched, and one over each burst of vanished mount points after
/// it. `report` hears every workspace found with its mount lost and what remounting it did, the
/// late answer of a remount left to an image lock's holder included, and every project the pass
/// could not read. It runs on the caller's tokio runtime.
pub fn watch(
    inventory: NativeGatewayInventory,
    report: impl Fn(LostMount) + Send + Sync + 'static,
) -> io::Result<LostMountWatch> {
    let events = MountTableEvents::new()?;
    let mut table = kernel_mounts()?;
    let (vanishing, vanished_mounts) = mpsc::unbounded_channel::<BTreeSet<PathBuf>>();
    std::thread::Builder::new()
        .name("lost-mounts".to_owned())
        .spawn(move || {
            loop {
                if let Err(error) = events.next() {
                    cowshed_os::eprintln!(
                        "cowshed: the watch on lost mounts stopped: the kernel's filesystem events failed: {error}; a volume that loses its mount stays unmounted until a verb or the next gateway start heals it"
                    );
                    return;
                }
                let now = match kernel_mounts() {
                    Ok(now) => now,
                    Err(error) => {
                        // The table held stays the comparison: the next event's read names
                        // whatever went meanwhile.
                        cowshed_os::eprintln!(
                            "cowshed: lost mounts: the kernel mount table could not be read after a filesystem event: {error}"
                        );
                        continue;
                    }
                };
                let gone = vanished(&table, &now);
                table = now;
                if !gone.is_empty() && vanishing.send(gone).is_err() {
                    return;
                }
            }
        })?;
    let passes = tokio::spawn(remount_passes(inventory, Arc::new(report), vanished_mounts));
    Ok(LostMountWatch { passes })
}

/// The one owner of the watch's state: which workspaces wait for their image lock. It runs a pass
/// at a time, folding every burst that arrived during one into the next.
async fn remount_passes(
    inventory: NativeGatewayInventory,
    report: Arc<dyn Fn(LostMount) + Send + Sync>,
    mut vanished_mounts: mpsc::UnboundedReceiver<BTreeSet<PathBuf>>,
) {
    let (released, mut released_answers) =
        mpsc::unbounded_channel::<((RepoId, WorkspaceName), LostMount)>();
    let mut waiting = BTreeSet::new();
    let mut scope = Some(RemountScope::Every);
    loop {
        if let Some(next) = scope.take() {
            let lost = match inventory.remount_lost(&next).await {
                Ok(lost) => lost,
                Err(error) => {
                    cowshed_os::eprintln!(
                        "cowshed: lost mounts: the recorded projects could not be read: {error}"
                    );
                    Vec::new()
                }
            };
            for found in lost {
                if let LostMount::Workspace {
                    workspace,
                    remount: Ok(Remount::Locked { .. }),
                } = &found
                {
                    let key = (workspace.repo().clone(), workspace.name().clone());
                    if waiting.insert(key.clone()) {
                        let inventory = inventory.clone();
                        let workspace = workspace.clone();
                        let released = released.clone();
                        tokio::spawn(async move {
                            let remount = inventory.remount_once_released(&workspace).await;
                            // The watch ended: nobody is left to hear it.
                            let _ =
                                released.send((key, LostMount::Workspace { workspace, remount }));
                        });
                    }
                }
                report(found);
            }
        }
        tokio::select! {
            gone = vanished_mounts.recv() => {
                let Some(mut gone) = gone else { return };
                while let Ok(more) = vanished_mounts.try_recv() {
                    gone.extend(more);
                }
                scope = Some(RemountScope::Vanished(gone));
            }
            Some((key, answer)) = released_answers.recv() => {
                waiting.remove(&key);
                report(answer);
            }
        }
    }
}
