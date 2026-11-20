# nx-cache-server: a land's publication is evicted before the next gate reads it

`specs/cowshed/03_caches.md` "Nx accepted-task artifact service" says a published result is replayed by the next gate of
the same tree instead of run again (:584-588). The same section's SIEVE (:622-634), as `packages/nx-cache-server`
implements it, evicts that result first once the store is at its cap, usually before any gate has asked for it.

## The mechanism

`sweep` (`packages/nx-cache-server/src/gc.ts:42-79`) orders entries by mtime and starts at the first entry whose mtime
is at or after the persisted hand. The hand is the mtime of the last entry it evicted (`gc.ts:73`). A store at cap
therefore keeps its hand just behind the newest entries, and every sweep (`server.ts:123`, one per stored `PUT`) reaches
the fresh, unvisited entries before it wraps to the old ones. A land's publish writes only unvisited entries (no `GET`
has read them yet), so they are the next victims. That includes entries of the same publish, which later `PUT`s of the
same run sweep away. Only the key just linked is protected (`gc.ts:62`). Old entries behind the hand, visited or not,
wait for a wrap, which does not come while fresh unvisited entries lie ahead of it. Scan resistance (`gc.ts:9-11`) is
working as written. What it was meant to protect is hashes every gate reads. What it removes in practice is the result
the very next gate asks for.

## Measured 2026-11-20 (one consumer repository, store `~/Library/Caches/dev.cowshed/nx/fdae06bc…/entries`)

- The store is at its cap: about 10.0 GiB held against `capBytes` 10737418240 (`config.ts:37`, the default).
- Churn from 05:45 to 07:46, read from `~/Library/Logs/cowshed/daemon-stderr.log`, which interleaves every backend the
  gateway runs: 4,063 stored `PUT`s totalling 5.80 GiB, and 3,993 entries evicted by 4,067 sweeps.
- What survives, by mtime, at 07:46: 2 entries written between 06:00 and 07:30, both visited. 191 entries from 00:0x
  survive, none visited, and 352 from 2026-11-17.
- One key, `14325990620516321786` (a `cargo-lint-cross` task): stored 9 times between 05:45 and 07:46, each `PUT` 200
  preceded by a `GET` 404, and never once answered 200. A land stored it (log line 9829539) before it landed at
  06:53:43. Sweeps moved the hand past 06:53:57 by line 9831738. The next land's gate asked at line 9833229, about
  07:17, got 404 `reason=miss`, and ran the task for 137 s.
- That gate's 367 tasks whose hash it shares with the previous gate's run, and which it executed rather than restored:
  - **253 confirmed evictions.** A land had stored the key (`PUT` 200 in the log), and that gate's `GET` reached this
    backend and got 404. Nothing deletes an entry but a sweep, and no `nx-cache-clear` ran in the window.
  - **112 admission refusals, by design.** The gateway answered `nx-queue-limited` before the backend
    (`/private/cowshed/store/telemetry/gateway/2026-11-20/*.arrow`, 07:15-07:36). 100 of them are in the store now. This
    is `05_gateway.md` "Busy gateway" and is not this defect.
  - **2 unexplained.** A generated-sources task and a runtime build task have no lookup in the backend log or in the
    gateway audit, and no earlier `PUT`. Why Nx asked nothing for them is open.
- The gate stored 533 entries after its first graph and 471 more after its second, which restored all 1,042 cacheable
  tasks locally. Most of the second batch had been stored minutes earlier by the first and evicted in between.

## Not this brief

- The consumer's queue item NX-CACHE-SERVER-SWEEP-RING is about when a sweep runs and what it lists: a re-list and
  `lstat` of the whole store per `PUT`. This brief is about which entry a sweep evicts. Both touch `gc.ts` and
  `server.ts:112-132`. Whichever lands second carries the other's rule; the in-memory ring of that item must keep this
  brief's ruling.
- Capacity, the staging budget's 403 (`03_caches.md:636-638`) and the gateway's queue limits stay as they are.

## Decided first by the owner

The two passages of `03_caches.md` conflict, and choosing how a published entry earns its first read is a policy
decision this brief does not make. The ruling is written into `03_caches.md` "Nx accepted-task artifact service" before
any code. Candidates for that decision, none chosen here:

- a land's publish counts as one visit;
- a fresh entry is not evicted before the hand has wrapped once past it;
- another rule the owner names.

Whatever is ruled, these hold:

- eviction stays bounded: each commit restores `used <= capBytes` before its 200 (`03_caches.md:628-630`);
- the cap, `maxBytes` and the staging budget do not change;
- a burst of archives no gate reads is still evicted eventually;
- nothing promises a result indefinite retention.

## Done when

- `03_caches.md` states the ruled rule, with the owner's ruling cited.
- Red first, in `packages/nx-cache-server`: a store at cap replays a representative churn sequence, the one measured
  above (the order and sizes of the `PUT`s and `GET`s between 05:45 and 07:46), or a generated sequence with the same
  bytes per land and the same lands per hour. On today's code, an entry one land publishes is evicted before the next
  land's lookups of the same key. After the change, those lookups are answered 200.
- The same test shows eviction still bounded: `used <= capBytes` after every commit, and entries no lookup reads are
  evicted by the end of the sequence.
- In the consumer repository, two consecutive gates of related trees are re-measured. The same-hash reruns are split
  into evicted (stored then answered 404), admission-refused and other, and the evicted count for keys the previous land
  stored is 0.
