# Nx cache server

A Bun implementation of Nx's self-hosted `GET`/`PUT /v1/cache/{hash}` protocol. Archives are opaque binary bytes in Nx's
native format; this package does not unpack them, modify Nx, introduce blob CAS, or share workspace databases. The
governing host contract is `specs/cowshed/03_caches.md` §Nx accepted-task artifact service. Gateway authorization and
exact-head/task-hash landing eligibility belong to `05_gateway.md` and `16_build_volumes.md`.

## Start and supervise

`nx-cache-server` runs the published `dist/ts/main.js` with Bun. Release platform targets also produce standalone
executables in `dist/bin/{darwin,linux}-{arm64,x64}/nx-cache-server`, using the same platform artifact convention as
cowshed. Supervision uses the installed artifact, not a workspace source path. One process owns one project store.
JavaScript targets own only `dist/ts`; each native target owns its `dist/bin` platform directory. Their output trees are
disjoint, so restoring a cached JavaScript target cannot replace a measured or installed native executable. Each project
must have its own parent directory: `<project>/entries` holds archives and `<project>/sieve.hand` holds the SIEVE
cursor. No other process may open that store or sweep it concurrently.

Invocation is `nx-cache-server [--exit-on-stdin-eof]`; the optional flag enables the parent-held stdin pipe lease
described below.

| Environment                    | Meaning                                                                                                                                                                                |
| ------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `NX_CACHE_SERVER_DIR`          | Required absolute project entries directory.                                                                                                                                           |
| `NX_CACHE_SERVER_HOST`         | Bind address; default `127.0.0.1`.                                                                                                                                                     |
| `NX_CACHE_SERVER_PORT`         | Port; default `8765`; `0` selects a free port.                                                                                                                                         |
| `NX_CACHE_SERVER_MAX_BYTES`    | Maximum one upload; default 1 GiB.                                                                                                                                                     |
| `NX_CACHE_SERVER_CAP_BYTES`    | Published-archive cap and independent staging-reservation budget, each a separate logical cap (not one combined physical disk cap); default 10 GiB each; must be at least `MAX_BYTES`. |
| `NX_CACHE_SERVER_WRITE_TOKENS` | Comma-separated `label:secret` writer principals.                                                                                                                                      |
| `NX_CACHE_SERVER_READ_TOKENS`  | Comma-separated `label:secret` read-only principals.                                                                                                                                   |

At least one principal is required. Labels are unique lowercase/digit/dash names of at most 32 characters; secrets must
be nonempty and unique. Configuration and backend tokens are host-owned, never committed, and never passed to sandbox
clients. Cowshed clients receive only gateway authority, not these backend credentials. Ordinary iteration reads
accepted project results. Only accepted landing transactions publish successful current hashes, including local hits
without rerunning their tasks. There is no WIP/scratch store.

stdout contains one JSON startup record:

- Ready: `{ "kind": "ready", "origin": "http://127.0.0.1:<port>", "port": <port>, "reclaimedStagingFiles": <count> }`.
- Configuration failure: `{ "kind": "failed", "stage": "config", "problems": [...] }`, exit 2.
- Operational failure:
  `{ "kind": "failed", "ok": false, "stage": "store" | "gc" | "listen", "directory": "...", "reason": "..." }`, exit 1.
- Store-locked refusal:
  `{ "kind": "failed", "ok": false, "stage": "store-locked", "directory": "...", "path": "<directory>/.owner.lock", "reason": "..." }`,
  exit 1. This is a distinct typed refusal; the generic `store`, `gc`, and `listen` failures keep their shape without
  `path`, and configuration failure remains `{ "kind": "failed", "stage": "config", "problems": [...] }` with no `ok`.
  Gateways act on `stage` and `path`, never on `reason` text.

stderr carries logfmt events, byte counts, timings, principal labels and refusal reasons. It never prints a bearer
secret. The supervisor captures those events; the package writes no permanent text log. SIGTERM/SIGINT drains requests
before exiting. SIGKILL can leave staged bytes, which the next startup reclaims and reports, never a partial hit.

With `--exit-on-stdin-eof`, the parent holds the write end of the process's stdin pipe for the child's lifetime. When
the parent dies, the kernel closes the pipe; EOF drains requests and the process exits. Without the flag, the process
keeps the SIGTERM/SIGINT behavior described above. The flag is argv-only and is not inherited through an environment
mode.

Each entries directory has a persistent `.owner.lock`, which is never unlinked. Before any staging reclamation, the
backend takes a nonblocking exclusive kernel `flock` (`LOCK_EX | LOCK_NB`) through a minimal Bun FFI binding to libc
(`libc.dylib` on Darwin, `libc.so.6` on Linux; see https://bun.com/docs/runtime/ffi) and holds it through drain and
stop. Hard death, including SIGKILL, releases it in the kernel. A busy lock fails startup as store-locked before
listening. Any other errno, or unavailable FFI, fails startup loudly with no fallback. Normal and supervised modes use
the same lease. Deployed evidence is currently macOS only; Linux release targets use the same syscall semantics, and no
Linux/ZFS runtime evidence is claimed.

A busy lock is the only `store-locked` refusal. The root lease is the sole store-exclusion mechanism; EOF changes
lifecycle only.

## Library API and retention

The root export provides `cacheServerConfigFromEnv`, `CacheServerConfig`, `startCacheServer`, `RunningCacheServer`,
`openCacheStore`, `stageEntry`, `publishEntry`, `readEntry`, `directoryStore`, `sweep`, and `sweepDirectory`.
`startCacheServer(config)` returns `{ ok: true, server }` or `{ ok: false, stage, directory, reason }`; a `store-locked`
failure also carries `path`. A running server provides `origin`, `port`, `reclaimedStagingFiles`, and asynchronous
`stop()`. `stageEntry` returns `ready` (a verified `CompleteUpload`), `incomplete` (received bytes), or `failed`
(reason). `publishEntry(store, upload)` atomically publishes that complete value and returns `stored`, `exists`, or
`failed`. Terminal outcomes carry `stagingRetained`: admission accounting may be released only after cleanup removed the
file. `readEntry` returns a pinned inode stream and byte count, `missing`, or `failed`; unlinking an archive cannot
truncate an already-opened GET. `sweepDirectory(store, capBytes, protectedKey?)` takes an owned `CacheStore`, not a bare
path, and returns `{ ok: true, outcome }` or `{ ok: false, reason }`. The standalone `gc` CLI acquires the store lease
and passes its store; callers must not run it beside a live server.

`openCacheStore(directory)` returns `{ ok: true, store }` or `{ ok: false, kind: 'store-locked', path }`. A successful
store has an async `close()`, and callers must close it after all use. Other low-level IO or errno errors propagate to
`startCacheServer`'s typed failure. The standalone `gc` CLI takes the same store lease; embedded server sweeps use the
already-held lease, and nothing sweeps the store concurrently.

Parallel streams produce complete staged values. One serialized publication section hard-links each value without
replacement and sweeps SIEVE before successful completion; no publication interleaves another commit's sweep. Startup
also sweeps. Each completed successful commit enforces the published archive-length cap before replying 200. SIEVE
orders by mtime, reads `atime > mtime` as the visited bit, and clears it by restoring atime to mtime. A real APFS GET
left atime unchanged, so GET explicitly marks the pinned inode when clear, retaining the modification-time cursor.

Authenticated writers reserve their declared upload lengths before streaming from an independent staging budget, also
`capBytes`. Full staging admission returns 403; there is no admission queue or retry service. Reservations cover
streaming and completed uploads waiting to commit and release after cleanup. A cleanup failure retains its reservation
and logs the path/reason until restart reclaims it; startup refuses if reclamation fails. Published archives and staging
each have their own logical cap, not one combined disk cap. Actual allocation and metadata are measured separately.
GET/PUT transfers and client archive extraction copy bytes; this is not zero-copy.

## Protocol and publication safety

| Operation                                      | Status                                                                                                                                    |
| ---------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| GET complete archive                           | 200, `application/octet-stream`, standard chunked complete binary body (pinned stream uses chunked HTTP framing; archive bytes unchanged) |
| GET missing/evicted archive                    | 404                                                                                                                                       |
| PUT complete new archive                       | 200                                                                                                                                       |
| PUT existing key                               | 409, original bytes untouched                                                                                                             |
| PUT read-only principal or over upload limit   | 403                                                                                                                                       |
| Missing/unknown bearer                         | 401, exactly `text/plain`, `WWW-Authenticate: Bearer`                                                                                     |
| Missing declared length                        | 411                                                                                                                                       |
| Invalid key/length or complete but short body  | 400                                                                                                                                       |
| Stream/write, link, read, or retention failure | 500, reason logged                                                                                                                        |
| Cleanup failure after complete publication     | 200, whole archive retained; staging reservation held and fault logged                                                                    |

The native Nx client and the current official Nx self-hosted caching OpenAPI
(https://nx.dev/docs/kb/self-hosted-caching) both use PUT 200, with 409 for existing keys and 403 for read-only or
refused admission. Server-side 403 prevents publication; it does not disable client-side tar/gzip creation or attempted
PUT. Do not claim zero overhead or no PUT for read-only iteration without a separately measured supported client
mechanism.

Staging, declared-length verification, and atomic `link(2)` publication preserve the existing never-replace design.
Concurrent same-key uploads have one winner; different keys are independent. Eviction makes future retrieval a miss.
Archives are not validated by extraction here: exact successful-task eligibility is the trusted gateway's
responsibility.

## Release and consumer cutover

The package is public via `npm:public`; conventional commits drive the normal independent Nx release. Source version
metadata is not proof of an available release. Existing external consumers keep their working source until a real
published dependency is available. Only then migrate the consumer and remove its obsolete source in the same cutover.
