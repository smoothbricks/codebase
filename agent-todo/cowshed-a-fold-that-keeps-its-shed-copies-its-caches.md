# cowshed: a fold that keeps its shed copies 18 GB of caches

`cowshed land <unit> --into <lane> --no-retire` folds a unit and keeps its shed for the agent that goes on working in
it. When the unit has built anything, that takes two to five minutes of disk and CPU under the controller, every time. A
plain fold of the same unit, which retires the shed, takes microseconds: it mints no seed (`build_volumes.rs`,
`Freeze::Unread`: "nothing would read a new seed, so none is minted"). Of nineteen folds on the measured day, the ten
with `--no-retire` each logged 64 `verified copy` lines and the nine plain ones none.

## Measured (2026-11-15, one repository, one day)

The `land freeze-seed done elapsed=…` line of each fold's own output:

| Fold of a unit that had built       | freeze-seed  |
| ----------------------------------- | ------------ |
| reader unit, first fold             | 326.7 s      |
| kernel field unit                   | 179.4 s      |
| native fixes, second fold           | 169.0 s      |
| reader unit, third fold             | 165.5 s      |
| writer unit                         | 162.1 s      |
| native host unit                    | 146.7 s      |
| reader unit, second fold            | 132.8 s      |
| native fixes, first fold            | 127.6 s      |
| reader unit                         | 124.9 s      |
| a unit that never built (six folds) | 28 to 284 µs |

- Nine folds of built units cost 25.6 minutes in total that day.
- During one, the watchdog attributed 8.0 cores to the controller's tree over 7.5 minutes, and a `gateway status` call
  got no answer in 60 s.

## Where the time goes (read from one fold's log, step by step)

It is not the image and not fragmentation.

- The 52 GB image is cloned with `clonefile` in 54 ms (`image clone after … elapsed_us=54057`).
- `fsck` of the clone: 38 to 197 ms. Attach: 134 to 425 ms. One mount: 15.3 s.
- Then about 65 lines of `verified copy <unit volume>/declared/<path> -> <seed volume>/…`, with no timing of their own,
  which hold the rest, about 160 s. The paths are the declared caches:
  - `packages/*/.cache/lmao` and `targets/*/.cache/lmao`, 11 GB in total, of which one package's test trace database is
    8.9 to 9.1 GB;
  - `.cache/ttsc`, 4.7 to 6.2 GB;
  - `codegraph`, 1.0 to 1.2 GB;
  - `.cache/linked-snapshots`, `.bun-pin`.
- So each fold reads, hashes and writes about 18 GB across two volumes, whatever the fold changed.

## What is wrong

- A kept shed is re-forked from the lane's new seed (`SeedReader::Refork`), and the re-fork carries the shed's declared
  caches into its new volume by a verified tree copy (`storage/directory.rs` `copy_tree`: copy until quiescent, then
  verify). The image was cloned a moment earlier; the caches cross two images, so they cannot be cloned.
- The copy is whole-cache work for a change of a few files. Its cost does not depend on the fold.
- Most of the bytes were a test trace database that only ever grows
  (`lmao-test-sink-loses-killed-runs-and-never-prunes.md`). It is history, not a build input.
- The kept shed already sits on a volume holding exactly those caches. Nothing it needs is elsewhere.
- The controller is busy throughout, so status calls time out and other commands queue behind it. A controller that does
  not answer is also what ends healthy jobs as SILENT (`cowshed-silent-controller-kills-a-healthy-job.md`).
- The way around it today is a plain fold followed by a fresh `cowshed fork <lane> <unit>`, which clones the adopted
  volume. That this is faster than keeping the shed is the defect.

## What to establish before changing it

- Why a kept shed is re-forked at all, instead of staying on its volume with the lane's seed cloned from it.
- Why the declared caches are copied between volumes: what the re-forked shed needs that a clone does not give.
- Whether a path can be declared as carried by clone only, or as not carried at all (a trace database).
- Whether the copy can skip a file whose size and content identity already match in the destination.
- Each `verified copy` line gets its own elapsed time and byte count, so the next reading needs no arithmetic.

## Done when

- A fold of a built unit into a lane returns in seconds, with the controller answering status during it.
- A fork from the lane after several folds starts warm, and its first build is measured against today's.
- The numbers above are re-measured on the same kind of day.
