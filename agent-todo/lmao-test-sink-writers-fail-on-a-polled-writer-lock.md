# Test processes sharing one trace sink fail on SQLite's polled writer lock

Every test process of a package writes `<package>/.cache/lmao/trace-results.db`
(`packages/lmao/src/lib/testing/bun-harness.ts:278-279`, `sqlite/trace-db-path.ts:45`); a consumer workspace runs nine
`bun test` shards of one package at once. A shard's test then fails with `SQLiteError: database is locked` from
`SQLiteTraceWriter`, after 10.3 to 13.3 s, with no defect in the change under test.

## What is wrong (base `4d247dce1d`)

- A test process is one trace. It commits: one live row when the run starts and two per test (start, completion), each
  its own autocommit (`sqlite/sqlite-writer.ts:155-166`, called from `tracers/SQLiteTracer.ts:44-65`), and at the end
  one `BEGIN IMMEDIATE` over every row of the run (`sqlite-writer.ts:181-193`).
- That closing flush holds the writer lock longest. In the consumer's sink (2,706,821,120 bytes, 9.48 million rows, 47
  columns) runs are up to 269,691 rows over 122,612 spans, 1,754 of them tests (p50 2,796 rows, p90 260,255). Measured
  alone on a clone of that sink, a run of 263,102 rows through the real writer flushes in 1.66 s (host load 39); the SQL
  alone is 1.06 s of inserts and 0.84 s of commit, the commit being the WAL checkpoint.
- The only wait between the processes is SQLite's busy handler, set at `sqlite-writer.ts:61`
  (`PRAGMA busy_timeout = 10000`): a sleep-and-retry poll that returns `SQLITE_BUSY` when the bound passes. Several
  siblings' flushes in a row pass it, and a writer that commits again at once takes the lock from every sleeper.

## What the fix makes true

`specs/lmao/01_trace_logging_system.md`, "Writers of one sink" (`smoo/lmao!n/sqlite-shared-writers`), added by the fix
commit, states it:

- A connection that `openWalDatabase` opened carries the sink's write lock: flock(2) on the persistent sibling
  `.<file>.write` (`sqlite/sqlite-flock.ts`, reached from Bun through `bun:ffi`; attached in `sqlite/sqlite-wal.ts`,
  carried to the Node adapter in `sqlite/sqlite-node.ts`, the runtime-neutral half in `sqlite/sqlite-write-lock.ts`).
  `SQLiteTraceWriter` holds it around every write transaction. A waiter blocks in the kernel until the holder unlocks or
  dies: no poll, no bound.
- A writer with a write lock flushes in turns of at most 4,096 rows (`SHARED_FLUSH_ROWS_PER_TURN`), so a sibling waits
  for turns and never for a trace. A writer without one (in-memory, a connection opened by hand, Node, Windows) is as
  before: one transaction, SQLite's wait.
- No caller changes. The harness, and a consumer's own `openWalDatabase` callers under Bun, get the lock through the
  connection. A consumer's Node writers of the sink (a Vitest global setup using `createNodeSQLiteDatabase`) get none
  and keep the 10 s wait against the others.

Not in this unit: the sink's growth. `lmao-test-sink-loses-killed-runs-and-never-prunes.md` section 2 holds that design;
a large sink no longer fails a writer here, it only makes each turn slower.

## Seed

- Shed `sink-wait` of smoothbricks, rebased onto main `4d247dce1d2771be04576c2a9379906d0ed427bc` (a no-op: it was forked
  there), tree clean, no stash.
- Red commit `5707acbcdee837c9607116cabc1a22bcd5ebc6d0`
  (`test(lmao): writer processes of one sink outlast a sibling's open write transaction`).
- Tip `d44c25437c4ef3d340fc67a5cb01a12780f2b875`
  (`fix(lmao): writers of one sink take turns at a kernel lock instead of SQLite's bounded poll`).

## Proven, and on which tree

All from this repository's main checkout, through `bash -l -c "cowshed exec sink-wait --class interactive -- …"`, unless
said.

| Tree                                   | Command                                                                                                   | Exit                                                                                                                                                                                                        | Wall                |
| -------------------------------------- | --------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------- |
| base + red test (`5707acbcde` content) | `sh -c 'cd packages/lmao && bun test src/lib/sqlite/__tests__/sqlite-writer-shared-sink.test.ts'`         | 1: the test fails at its line 73 (`expect(exitCode, output).toBe(0)`), each writer's output `SQLiteError: database is locked`, `code: "SQLITE_BUSY"`, from `sqlite-writer.ts:119` under `writeRow` (`:156`) | 44.6 s (test 2.6 s) |
| fix, turn size then 1,024              | the same file with `sqlite-writer-concurrent-open.test.ts`                                                | 0: 6 pass                                                                                                                                                                                                   | 50.8 s              |
| fix, before the turn-size edit         | `bun nx run-many -t lint typecheck -p lmao`                                                               | 0                                                                                                                                                                                                           | 39.8 s              |
| tip `d44c25437c` content               | `sh -c 'cd packages/lmao && bun test src/lib/sqlite src/lib/testing/__tests__/bun-harness-flush.test.ts'` | 0: 19 pass                                                                                                                                                                                                  | 25.2 s              |
| tip `d44c25437c`                       | `bun nx affected -t lint test build --base=4d247dce1d --outputStyle=static`                               | 1                                                                                                                                                                                                           | 9 min 8 s           |

The affected run (its log is handed over with the bench script, see step 3): 17 projects, 228 tasks. `lmao:test` passed
(1,203 tests, 108 files, 64 s), as did lmao's lint and build. Seven tasks failed, none of lmao: `nx-plugin:test-host`,
`nx-plugin:test-shard2`, `-shard3`, `-shard4`, `cli:test-shard-1`, `cli:test-shard-5`, `nx-cache-server:test`;
`cli:test` and `nx-plugin:test` did not run. The failures the log shows are sandbox refusals: `EPERM … link` (the cache
server answering 500), `EPERM … mkdir '/private/var/folders/cowshed/502/…'`,
`EPERM … open '/private/tmp/smoo-ram3-502.lock'`, `EPERM … posix_spawn 'sudo'`,
`sandbox-exec: sandbox_apply: Operation not permitted`, and daemon tests. None names the sink, flock or SQLite.

Measured, host bun outside the sandbox, on a clone of the 2.7 GB sink:

- One process, one run of 263,102 rows (1,754 tests), load 39: base flush 1,655 ms, longest live row 732 ms; fix with
  1,024-row turns flush 4,177 ms, longest live row 674 ms.
- Turn size, the same 269,691 rows by SQL, load 41: 1,024 rows per turn 3,553 ms total (longest turn 286 ms); 4,096:
  2,275 ms (132 ms); 16,384: 1,620 ms (186 ms); one transaction: 1,752 ms. The connection runs `synchronous = 1` and
  `wal_autocheckpoint = 1000`: a turn's commit pays the checkpoint, which is why small turns cost more.
- flock through `bun:ffi` on macOS arm64: a waiter is woken when the holder unlocks; an uncontended lock and unlock pair
  is 0.7 µs. The built `dist` creates `.traces.db.write` beside the sink and writes through it.

Inferred, not observed:

- That the seven failed tasks fail the same way on the base and pass on the host lane. The host-lane run was queued
  behind a land and ended before it started.
- That nine processes show no `SQLITE_BUSY` and short waits on the fix: the nine-process bench has not run on either
  tree with this script. The earlier measurement (a model of the writer, not the writer: 7 `SQLITE_BUSY` in 48,312
  commits and a 14.6 s longest wait on the large sink; 0 in 65,336 and 5.1 s on a fresh one) is the diagnosis's.
- Linux: `sqlite-flock.ts` loads flock from `libc.so.6`, then `libc.musl-<arch>.so.1`, and throws naming both if neither
  loads. Only macOS was run.
- That 4,096 is the right turn size at nine writers; it was chosen from the one-process table above.

## What remains, in order

1. The failed tasks on the host lane, when no `cowshed land` runs (`/bin/ps -axo command= | grep '[c]owshed land'`) and
   holding the host lock for the one invocation:

   ```sh
   S=/private/var/folders/cowshed/502/mnt/smoothbricks/codebase/sink-wait
   direnv allow $S && /usr/bin/lockf -k /private/tmp/wide-run-host.lock bash -l -c \
     "cowshed exec sink-wait --host --want 8 -- direnv exec $S bun nx run-many -t test -p nx-plugin cli nx-cache-server --outputStyle=static"
   ```

   A task that is red there is read from its package's `.cache/lmao/trace-results.db` and run on the base before it is
   called this unit's.

2. If anything changed in the shed since the tip above, again:
   `bash -l -c "cowshed exec sink-wait --class interactive -- bun nx affected -t lint test build --base=4d247dce1d --outputStyle=static"`.

3. The bench, `sink-wait-bench.ts` (nine processes, each lmao's real `SQLiteTracer` with `liveChildRows` over
   `openWalDatabase`, test runs of 1,754 tests and 263,102 rows back to back; it prints runs, live commits,
   `SQLITE_BUSY` on live rows and on flushes, the longest live-row wait and the longest flush). The script is not in
   this repository: the consumer's queue line for this brief names where it and the affected run's log are kept. Its
   first argument is the checkout whose lmao source runs: "before" is a checkout at the base (this repository's main
   while it is at `4d247dce1d`, else a worktree of it), "after" is the shed. `BIG` is a package's trace sink of a
   consumer workspace, about 2.7 GB, read only. Run each of the four only when `uptime`'s 1-minute load is under 12, and
   state the load beside each:

   ```sh
   B=/private/tmp/sink-wait-bench; mkdir -p $B; cd $B
   BENCH=<path of sink-wait-bench.ts>
   BIG=<a consumer package's .cache/lmao/trace-results.db of about 2.7 GB>
   BASE=<a checkout of this repository at 4d247dce1d>
   FIX=/private/var/folders/cowshed/502/mnt/smoothbricks/codebase/sink-wait
   run() { uptime; bun "$BENCH" "$1" "$2" main 30 1754 74; }
   rm -f large.db*; cp -c "$BIG" large.db; run $BASE large.db   # 1: before, large sink
   rm -f fresh.db*;                        run $BASE fresh.db   # 2: before, fresh sink
   rm -f large.db*; cp -c "$BIG" large.db; run $FIX  large.db   # 3: after, large sink
   rm -f fresh.db*;                        run $FIX  fresh.db   # 4: after, fresh sink
   ```

   If `$BIG` is gone, any package sink of that size serves; `/private/tmp/sink-wait-bench/big.db` is a clone of it with
   one 269,691-row trace added.

4. Add the four lines (load, runs, live commits, `SQLITE_BUSY`, longest live-row wait, longest flush) to the handoff. If
   the longest live-row wait after the fix is over a second, measure 16,384-row turns the same way before changing
   `SHARED_FLUSH_ROWS_PER_TURN`, and keep the constant's comment and the spec paragraph on the numbers.

5. Hand the tip to the land owner. After the land a consumer needs nothing beyond its linked package being rebuilt: test
   processes are short-lived and take the fix when they next start, and `.trace-results.db.write` is created in
   `.cache/lmao`, which the consumer already declares as build state.

## Acceptance

- `packages/lmao/src/lib/sqlite/__tests__/sqlite-writer-shared-sink.test.ts`, both tests: "writers wait out a sibling
  whose write transaction outlasts their SQLite lock wait" (red on the base with `SQLITE_BUSY`, green on the tip) and
  "flushes a trace at a write lock in turns of at most 4,096 rows".
- lmao's lint, typecheck and test targets green on the final tree; the seven tasks above green on the host lane, or
  shown red on the base too.
- The bench after the fix: zero `SQLITE_BUSY` on live rows and on flushes, on the large and on the fresh sink. No bound
  on the longest wait was set; the four runs' numbers are reported with their loads.
