# cowshed: every macOS census opens and reads every lease file of its store

Governing spec:
[02_workspaces.md §Holders cowshed does not run](../specs/cowshed/02_workspaces.md#holders-cowshed-does-not-run). The
rule the census implements: "A pid is passed over only while a lease file of the census's own store names it with the
unique id its process has now: a record that names no process, or whose pid another process has since taken, names
nothing." The holder writes the record "after the holder takes the lease's `flock(2)`" and empties it "before it lets
go". "The census's own process is always asked, whatever its leases say."

Read that section and the code below before designing. Cite the sections you follow in your report. Where the code
contradicts the spec, the code is the defect.

## What the code does

`build_volume::nx::census_pids` (`cowshed-core/src/build_volume/nx.rs:1305-1321`) starts every macOS census, one asked
lock included, with `lease_holder::running_lifecycle` (`cowshed-core/src/lease_holder.rs:122-150`). That function lists
every owner, every project and every `sessions/` directory of the store, then opens and reads every `*.intent.lock` and
`*.land.lock` in them (`leases`, `:177-192`). The spec rule needs only the leases whose record names a process. A lease
no lifecycle operation holds is empty, and the leases of every workspace name ever used stay in `sessions/` after the
workspace is gone.

One asked lock triggers the whole walk. `cowshed_apfs::store_lock::wait_for` (`cowshed-apfs/src/store_lock.rs:225-241`)
names the holders of the one image lock it waits on (`Taker::holders` → `held_lock::holders` → `held_lock::observe`). It
does that once when the wait begins and once per `DISK_CHILD_DEADLINE` (120 s) after, and each naming runs a full
census.

## Measured 2026-11-20, 08:47, host store

- **Profile.** dtrace on the gateway, at 60.3 % CPU: 470 of 747 on-CPU samples were under `census_pids`, 598 under
  `held_lock::observe` and 210 under `store_lock::acquire` → `wait_for`. The hot stack was `held_lock::holders` →
  `observe` → `census_pids` → `fs::read`.
- **File opens.** A 1.47 s `fs_usage` window had 12,370 `open` and 68 `openat` calls. Of them, 11,888 opened a `.lock`
  and there were 11,887 distinct `.lock` paths. 11,885 were session leases (7,308 `.intent.lock`, 4,577 `.land.lock`),
  each opened once, all on one gateway thread, between 08:47:17.966 and 08:47:18.486. That is one pass over every lease
  of the store in 520 ms, for one census.
- **Lease files.** The store holds 11,874 lease files; 12 of them are non-empty. By project: one holds 8,494, the
  smoothbricks codebase 2,703, one 464, one 160, and the rest 15 or fewer each.

## Not this brief

- `Cowshed.md` "A retirement leaves its name's lock files" and `cowshed-store-inventory-gc-retire.md` "rm <ws> leaves
  its locks" are about the residue itself. This brief removes the census's reads of it, and its fix neither depends on
  nor performs any unlinking of lock files.
- The consumer's queue item COWSHEDS-CENSUS-ANSWERS-WHILE-A-CONTROLLER-TEARS-DOWN is about which thread a census runs
  on. This brief is about what each census reads.

## Constraints

- Every rule of the governing section holds, unchanged:
  - a genuine holder still refuses and is still named;
  - a pid is passed over only while a record names it with the unique id its process has now;
  - a reused pid and a record without a unique id name nothing;
  - the census's own process is always asked;
  - a store, project or lease this user cannot read still names nothing, and the census asks those processes.
- No lock file is unlinked, renamed or truncated by the census. No ownership is cached across censuses: each census
  proves each passed-over pid against its live unique id. Nothing polls.
- How the census learns which leases hold a record, without opening the empty ones, is the design to choose and to write
  into §Holders first. Two directions to weigh, neither chosen:
  - one bulk directory read per `sessions/` directory that returns sizes, opening only non-empty leases;
  - a record of held leases that the holder keeps under its own `flock`, beside the lease record it already writes and
    clears.

## Done when

- §Holders states how the census finds the records, with the owner's ruling if the design adds a mechanism.
- Red first, through the existing seam `census_pids_reading(census, read)`. A fixture store holds N empty historical
  leases and one lease a live process holds with its record. Today the count of `read` calls grows with N. After the
  change it is bounded by the number of non-empty records and does not depend on N.
- The same fixture keeps every rule passing:
  - the live holder is passed over;
  - a record naming an ended process, or a reused pid, names nothing;
  - the census's own pid is asked;
  - an unreadable `sessions/` leaves its processes asked.
- After the change, an `fs_usage` window over one census on the host store opens no more lease files than it holds
  non-empty records. The baseline is the 11,885 measured above, so only the after is measured.
