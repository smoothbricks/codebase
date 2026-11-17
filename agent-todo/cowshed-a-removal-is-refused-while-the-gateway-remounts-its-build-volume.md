# cowshed: a removal is refused while the gateway remounts its build volume

Found 2026-11-17: three removals in one day, in the teardown of a consumer workspace's host tests, each of a scratch
project's shed.

## What was seen

The removal unmounted the shed's build volume and then failed thirteen detaches of that build image, the forced one
included: `hdiutil detach -force /dev/diskNNN: Resource busy` (exit 16). The scratch project's destroy then refused
("resource owners have not proved retirement"). In the gateway's log (`~/Library/Logs/cowshed/daemon-stderr.log`, lines
about 9042249-9042360 for the 11:04 failure; the same pattern near 8993280 and 9022862) the same seconds hold a
`lost-mounts check <repo>` of that project that takes an image lease in 10 ms, pins a device (`apfs canonical/pin`),
waits 12.78 s on a second `apfs canonical/image-lease`, mounts a volume, and reports
`<ws> lost its mount while its image stayed attached as /dev/diskN; remounted at …` for the shed being removed.

A read descriptor open on `/dev/rdiskNs1` makes `hdiutil detach -force /dev/diskN` answer `Resource busy`; closing it
lets the eject through (reproduced on a throwaway image by the diagnosis, not by this unit).

## The cause, as read in the code (base `4d247dce1d`)

1. A removal releases the build volumes the workspace owns before its retirement takes the workspace's image lock.
   `cowshed-core/src/runtime/project.rs:9480-9489` calls `release_owned` (`runtime/build_volumes.rs:2041`), and only
   `finish_retirement` at `:9491` reaches `execute_retire_staged` (`storage/apfs.rs:1818`), whose `dispatch_with_locks`
   takes the lock. The unfinished-clone removal already does it the other way round: its `release_owned` runs inside
   `execute_pending_clone_retirement`'s lock (`project.rs:11490`, `apfs.rs:1742`).
2. `release_claimed` (`storage/apfs/native/build_volumes.rs:973-989`) unmounts the live volume (`unmount_verified`),
   then detaches (`backend.detach`, `cowshed-apfs/src/lib.rs:4407-4411`, which takes the image lease). Between the two
   the lease is free and the shed's own volume is still mounted.
3. That state is what the watch calls a lost mount. `lost_mount` (`storage/apfs/native.rs:4334-4347`) answers the
   workspace's device whenever `mount_settled` is false and its image is attached, and `mount_settled`
   (`native.rs:4275-4332`) is false when the build volume the checkout links is not mounted at its mount. This settles
   the point the diagnosis left open: the check fires because the removal itself unmounted the linked build volume, not
   because anything was lost.
4. `remount_lost` (`storage/apfs.rs:1964-1997`) only tries the image lock. The removal does not hold it yet, so the try
   succeeds and `settle_mount` runs (`apfs.rs:3356`); the shed is mounted, so it goes to `ensure_linked_build_volume`
   (`:3367`), `settle_build_link` (`native/build_volumes.rs:1032`, record still present, so `LinkResolution::Keep`) and
   `mount_build_volume`, which recovers the attachment with its raw pin (`existing_attachment_for_mount`,
   `cowshed-apfs/src/lib.rs:3681-3694`) and then, in `prepare_mount` (`lib.rs:2972-2986`), waits for the image lease
   with that pin held. `lib.rs:1268-1273` says the pin blocks a forced detach. The removal holds the lease for its
   detach and cannot eject; the gateway holds the pin and waits for the lease. The removal gives up after its thirteen
   attempts, the gateway gets the lease and mounts.

## What the fix makes true

A removal holds its workspace's image lock from before it unmounts the first build volume the workspace owns until its
image is released. The watch's try then answers `Remount::Locked`, it waits for the lock holding no lease, and finds the
lock gone with the workspace: this is what `specs/cowshed/05_gateway.md` "Lost mounts" already states for every
lifecycle holder ("A workspace whose image lock a live operation holds is that holder's until it lets go"), and what the
doc comment of `remount_lost` names ("a detach or a retirement among them").

Chosen over dropping the pin in `prepare_mount`: the pin is what proves a recovered attachment's device across the lease
reacquisition (`lib.rs:2970`), every other mounter of a build volume runs under its workspace's image lock, and without
the lock the watch would still act on a workspace under removal (once the record is gone, `settle_build_link` takes
`LinkResolution::Vanished` and reforks or unlinks).

## The seed

- Shed `detach-pin` of smoothbricks, branch `cowshed/detach-pin`, base `4d247dce1d2771be04576c2a9379906d0ed427bc`
  (smoothbricks main's tip when this was written, so no rebase was needed); tree clean, no stash.
- Red commit `647c26ee8937250a11ef7ff08468aaafd99eb37f`: the test
  `real_apfs_a_removal_under_the_lost_mount_watch_detaches_and_remounts_nothing` in
  `packages/cowshed/crates/cowshed-cli/tests/command_dispatch/macos.rs`. A `Project::Declared` fixture forks a shed with
  a build volume of its own, arms `cowshed_core::lost_mounts::watch` on the fixture's store, runs `rm shed --force`,
  then unmounts main with `diskutil` and reads the watch's reports until main is remounted (every pass the removal
  caused ran before that answer). It fails if the `rm` fails, if any report is a remount of anything but main or an
  error, or if the shed or its build volume is mounted or the build image exists afterwards.
- Tip `d134a4ab9054dd99c6166f819dbd899a3746f62f`: `ApfsSubstrate::lock_retirement` and `RetirementLock`
  (`storage/apfs.rs`), which `execute_retire_staged` now takes instead of locking itself (a lock for another workspace
  is `InvalidPlan`); `project.rs` takes it before `release_owned` in the removal and before `finish_retirement` in a
  rename's retirement of its source; the four fake-host callers in `cowshed-core/tests/apfs_storage.rs`; the sentence in
  `specs/cowshed/02_workspaces.md` (`rm` step 4) and in `specs/cowshed/05_gateway.md` ("Lost mounts"). No spec sentence
  is removed.

## What is proven, and what is not

- Proven: the red commit's test binary compiles. On `647c26ee89`,
  `cargo --frozen nextest run -p cowshed-cli --test command_dispatch -E 'test(real_apfs_a_removal_under_the_lost_mount_watch)'`
  on the host lane built `cowshed-cli` and `command_dispatch` (`Finished test profile in 2m 55s`) and exited 105 after 8
  m 41 s without running the test: nextest's `apfs-sweep` setup script needs `.cache/nextest/binaries-metadata.json`,
  which only `cargo-test-build` writes.
- Not run: the red test on the red commit; anything on the tip. The tip has not been compiled.
- Inferred, not observed: that the test is red on the red commit. The watch must reach the shed between the build
  volume's unmount and the retirement's lock; the removal spends an `hdiutil detach`, a release and a `git` in main in
  that window and the watch a few milliseconds, but a pass that runs after the retirement's lock would let the test pass
  on the base. If it does, state it and make the window certain rather than widen a bound.
- Inferred: nothing between the lock and the retirement takes the same lock again in the same process
  (`release_build_volume` claims the volume's hold and the image lease; `check_checkout_nx` reads a census;
  `unregister_workspace_in_main` runs git in main). A second `flock` of it there would hang the removal: a run of the
  red test on the tip that exceeds twice the base's wall is profiled (`sample <pid> 5`), not rerun.
- Inferred: `recover_pending` moving from just before the detach to the lock's acquisition, before `release_owned`,
  changes nothing a removal relies on.

## What remains, in order

Host-lane runs hold `/private/tmp/wide-run-host.lock` for one invocation each, start only when
`/bin/ps -axo command= | grep '[c]owshed land'` shows nothing, and run from the smoothbricks checkout. `S` is the shed's
path, `/private/var/folders/cowshed/502/mnt/smoothbricks/codebase/detach-pin`; `T` is the single-test command:

```
T='COWSHED_EXECUTABLE=packages/cowshed/bin/cowshed cargo --frozen nextest run \
  --binaries-metadata .cache/nextest/binaries-metadata.json --cargo-metadata .cache/nextest/cargo-metadata.json \
  --target-dir-remap "$(node_modules/@smoothbricks/nx-plugin/../nextest-extract/dist/bin/darwin-arm64/smoo-nx-nextest-binaries target-dir)" \
  --workspace-remap . -E "package(cowshed-cli) and test(real_apfs_a_removal_under_the_lost_mount_watch)" \
  --no-tests=fail --user-config-file none --tool-config-file "smoo:$PWD/node_modules/@smoothbricks/nx-plugin/nextest.toml"'
```

1. Red on the red commit (expected: exit non-zero, the panic at the `succeed(&mut service, ["rm", "shed", "--force"])`
   line with `Resource busy`, or at `the watch acted on a workspace under removal`):
   `lockf /private/tmp/wide-run-host.lock cowshed exec detach-pin --host --want 8 -- direnv exec $S sh -c 'git checkout -q --detach 647c26ee89 && bun nx run @smoothbricks/codebase:cargo-test-build && '"$T"'; rc=$?; git checkout -q cowshed/detach-pin; exit $rc'`
2. Green on the tip:
   `lockf /private/tmp/wide-run-host.lock cowshed exec detach-pin --host --want 8 -- direnv exec $S sh -c 'bun nx run @smoothbricks/codebase:cargo-test-build && '"$T"`
3. Lint and what the sandbox can run:
   `cowshed exec detach-pin --class interactive -- bun nx affected -t lint test build --base=4d247dce1d`
4. The cargo targets, host lane, one invocation:
   `lockf /private/tmp/wide-run-host.lock cowshed exec detach-pin --host --want 8 -- direnv exec $S bun nx run-many -t cargo-test cargo-test-group-real-apfs-host -p @smoothbricks/codebase cowshed`
   (`cargo-test-group-real-apfs-host` holds every `real_apfs_` test of `cowshed-core` and `cowshed-cli`, the new one and
   the removals of `real_apfs_build_volume_names_follow_checkout_ownership_and_unlinked_mounts_are_reclaimed` among
   them; `cowshed-core`'s `apfs_storage` fake-host tests run in `cargo-test`).
5. Hand the integrator the tip with each check's exit and wall. After the land a consumer runs `cowshed setup` from the
   landed build and the gateway restarts with it: the fix is in the removal, which runs in the project's controller, and
   takes effect for every `rm`, land retirement and project destroy served by the new build. [INFERENCE: the watch
   itself is unchanged, so an old gateway beside a new controller already behaves; not observed.]

## Done when

- `real_apfs_a_removal_under_the_lost_mount_watch_detaches_and_remounts_nothing`
  (`packages/cowshed/crates/cowshed-cli/tests/command_dispatch/macos.rs`) fails on `647c26ee89` and passes on the tip,
  both observed, with the failing line quoted.
- `@smoothbricks/codebase:cargo-test`, `cargo-test-group-real-apfs-host` and the affected lint, test and build targets
  exit 0 on the final tree.
- A removal under an armed watch reports `Locked` or nothing for the workspace, never `remounted`.
