# cowshed: gc and workspace retire judge the store by the store inventory

The project's unbinding judges every entry of its terminal trees (`.staging/`, `sessions/`, `tmp/`, `build/`, the root's
locks) by one declaration, `cowshed-core/src/storage/store_inventory.rs` (`StoreArtifact`, its `Class`), and discards
cowshed's derived state while refusing user work by name (`specs/cowshed/02_workspaces.md`, "Checkout layouts"). `gc`
and `rm <ws>` still decide file by file in the APFS host, and leave derived residue that only the unbinding now removes.
Each item names the residue, the code that leaves it, and the test that proves it gone.

## gc never sweeps derived crash residue

- Residue: `build/.compacting/<seed>.dmg` and `copy.dmg` from an interrupted compaction (only the next compaction hands
  them off, and it needs main); fsio publication temps `.<name>.tmp.<id>` in `sessions/` and `build/`
  (`cowshed_os::fsio::is_temp_artifact`); `sessions/.trash/.<name>.<id>.partial` and `.bundle-verify-<id>/` an abandon
  bundle's write left; an orphan `<bundle>.patch-ids`; an orphan `tmp/<name>/` whose image gc reclaimed.
- Code: `preview_gc_project` (`storage/apfs/native.rs`) enumerates sessions, trash images, `.staging` and the mount root
  only; its trash pass reads `*.bundle` files. `reclaim_image` reclaims an orphan session image without its temp dir.
- Work: drive gc's sweep of these from `StoreArtifact::of` and its class, not from a second set of name predicates; an
  entry is swept only when no lock, lease or live process owns it.
- Acceptance: a tempdir property test over the inventory: every derived crash-residue kind gc can prove unowned is gone
  after `gc`, every image among them is in `.deleting`, and nothing user work or foreign is touched.

## rm <ws> leaves its locks

- Residue: `sessions/<ws>.dmg.lock`, `<ws>.intent.lock`, `<ws>.land.lock`, one triple per name ever used, until the
  unbinding.
- Code: `release_workspace` → `reclaim_workspace_artifacts` (`storage/apfs/native.rs`) reclaims image, sidecar, key and
  temp dir only.
- Work: retire the name's locks once the retirement commits and no holder remains, by the inventory's kinds.
- Acceptance: after `rm <ws>` with no other holder, `sessions/` holds no entry naming `<ws>`.
