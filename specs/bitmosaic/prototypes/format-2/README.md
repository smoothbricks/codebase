# Format 2: the experiments behind it

What was tried to arrive at [the format](../../01-format.md), with the numbers, and how to run it again. The data of a
consumer stays outside this repository; its shape is described here. Measured 2026-11-15 on an Apple M-series host under
load (load average 22 to 50): instructions are stable to a few percent, cycles are indicative.

## Files

| File             | What                                                                                                                                                                                                     |
| ---------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `model.py`       | every candidate form as a closed formula of an ascending member list; `evaluate` prints one block a row                                                                                                  |
| `rows.py`        | the rows: `python3 rows.py <data dir> [seen] [text] [parity] [frames] [synthetic]`                                                                                                                       |
| `corpus.py`      | a real text corpus with a tail from any git repository: `python3 corpus.py <repo> <out dir> repo '*.md' '*.rs'`                                                                                          |
| `ref.py`         | a reference writer and reader of format 2 from the spec alone; `check`, `vectors`, `corpus`                                                                                                              |
| `sizes_check.py` | the numbers `../../laws/sizes.bend` prints, from Python                                                                                                                                                  |
| `proto/`         | a standalone crate (its own workspace): `crate_len` (lengths and hex of the repository crate's images, which `model.py` and `ref.py` read) and `proto2`, the read, write, patch and intersection harness |
| `results/`       | the tables of the run of 2026-11-15                                                                                                                                                                      |

Build `proto` with `cargo build --release --manifest-path proto/Cargo.toml`. `proto2 <data dir> [filter]` checks every
image of every form against its member list before it measures anything. Its counters (instructions, cycles, branch
mispredicts) are a kpc window on macOS, programmed through
`sudo -n /usr/sbin/dtrace -q -n 'BEGIN{trace(0);}' -c '<proto2> <data dir>'`; started plainly it still checks every
image and prints sizes. Give every rebuilt copy its own `version` in `Cargo.toml`: a build cache keyed on the version
has served a stale binary before.

## Data

| File in the data dir                                     | Rows                                                                                                                                                                                                       | Real?                                |
| -------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------ |
| `repo-postings.txt`, `repo-ngrams.txt`                   | `corpus.py` over this repository's tracked `*.md` and `*.rs` at `9118ed07cc`: one row a non-empty line (723,750 rows); a list per token `[a-z0-9_]{2,}` (54,732 lists) and per byte trigram (54,930 lists) | real text, public, repeatable        |
| `seen-runs.txt`, `seen-families.txt` (and a second log)  | a consumer's deduplication set of ordinal identities, from two recorded fleet logs: per (family, admitting transaction) the ordinals admitted; per family their union                                      | real; not public                     |
| `postings-u32.txt`, `ngrams.txt`                         | a consumer's search parity corpus: 54,000 documents of 12 tokens over 256 Zipf tokens; its trigram plane                                                                                                   | a benchmark corpus: synthetic text   |
| `frames.txt`                                             | 812 real record shapes of a consumer (13 to 93 positions, the always-set ones first); which optional positions a row sets is drawn at p = 0.5, four draws a shape                                          | shapes real, draws synthetic         |
| `present-best.txt`, `actor-planes.txt`, `sparse-1e6.txt` | run-shaped sets; `0..n` less up to three holes; uniform draws of `2^32`                                                                                                                                    | the first real, the others synthetic |

What the real data said:

- **Deduplication set.** 3,155 runs in two logs: every run is one ordinal, every family is exactly `0..n-1` (size p50 1,
  p90 4, max 390), each run starts at the previous maximum plus one, no hole.
- **The parity corpus has no tail**: its shortest posting list has 357 documents and its shortest gram 132 rows.
- **Real text has one**: 26% of token lists hold one row and 54% at most three; a rare token's rows lie near each other,
  so a list relative to its first member is narrow.

## Sizes

Bytes a set, mean. "Format 1" is the repository's crate. "Small forms" is an unlanded implementation of six small forms
confined to members below 65,536. "Format 2" is the family the spec keeps: Bitmap, List, Elias-Fano made lean, the
directory unchanged. The bound is the information in a set of `k` members and `r` runs between a known first and last
member.

| Row                                                 |  bound | format 1 | small forms | format 2 | a candidate left out        |
| --------------------------------------------------- | -----: | -------: | ----------: | -------: | --------------------------- |
| deduplication set, one transaction (real)           |      0 |     12.0 |        2.11 |     2.11 |                             |
| frames (real shapes)                                |    2.0 |     20.6 |        4.31 |     4.31 |                             |
| postings, real text, lists of 1 to 3                |    0.2 |     14.4 |        10.4 |     4.94 | one collection layout: 4.80 |
| postings, real text, 4 to 32                        |   13.8 |     35.8 |        32.4 |     24.1 |                             |
| postings, real text, 33 to 256                      |    145 |      189 |         186 |      177 |                             |
| postings, real text, above 256                      |  1,698 |    2,149 |       2,146 |    2,136 | blocks: 1,872               |
| postings, real text, every list                     |   64.9 |     96.7 |        93.0 |     86.1 | blocks: 78.6                |
| n-grams, real text, 1 to 3                          |    0.3 |     15.6 |        11.3 |     5.08 | one collection layout: 4.58 |
| n-grams, real text, every list                      |    409 |      534 |         530 |      522 | blocks: 465                 |
| postings, parity corpus                             |  1,211 |    1,581 |       1,578 |    1,568 | blocks: 1,452               |
| n-grams, parity corpus                              |  1,581 |    2,078 |       2,075 |    2,066 | blocks: 1,921               |
| routing set, 1 to 64 of 10^6 (synthetic)            |   60.3 |     86.0 |        83.0 |     74.7 |                             |
| slot of capacity 16, members of 10^6 (synthetic)    |   14.4 |     39.5 |        36.4 |     25.4 |                             |
| one shard of 50,000 of a fan-out set (synthetic)    |  2,606 |    3,561 |       3,558 |    3,548 | blocks: 3,178               |
| routing set, 10^3 to 10^4 of 10^6 (synthetic)       |  5,905 |    7,100 |       7,097 |    7,087 | blocks: 6,610               |
| 30% of 2 x 10^5 (synthetic)                         | 22,030 |   27,849 |      27,846 |   27,846 |                             |
| one member anywhere in u32 (synthetic)              |      0 |       12 |           9 |        5 |                             |
| `0..n` less up to three holes, n to 300 (synthetic) |    1.1 |     22.2 |        9.55 |     15.0 | intervals with a base: 8.70 |
| one interval anywhere (synthetic)                   |      0 |     14.4 |        11.0 |     11.1 | an interval form: 5.34      |

`results/summary.md` has every row and every candidate family.

## Candidates, and why each stayed or went

| Candidate                                                                                                              | Verdict                  | Evidence                                                                                                                                                                                                                                              |
| ---------------------------------------------------------------------------------------------------------------------- | ------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Bitmap from zero, one tag byte, up to 127 bytes                                                                        | kept                     | the bare bitset: contains 30 instructions and 6 cycles, patch of one member 57 instructions                                                                                                                                                           |
| List of at most 8 members, the first and the others' distances from it, each in the bytes it needs                     | kept                     | real tails 10.4 B to 4.94 B; a member anywhere in u32 is 5 B; written in 128 instructions                                                                                                                                                             |
| the same list with absolute members                                                                                    | dropped                  | 5.65 B on the real tails: clustered rows make the distances narrow                                                                                                                                                                                    |
| a list of up to 31 members                                                                                             | dropped                  | within 0.5% of 8 on every row; Elias-Fano takes over from 9                                                                                                                                                                                           |
| Elias-Fano made lean (planes in bytes, no body length, the first zero-sample not stored)                               | kept, as the root itself | real lists of 4 to 32: 32.4 B to 24.1 B; 33 to 256: -5%; above: -0.4%; one reader                                                                                                                                                                     |
| a small Elias-Fano form of one block, its geometry derived per read                                                    | dropped                  | 1.5 times the root's instructions for the same bytes                                                                                                                                                                                                  |
| a bitmap at a base                                                                                                     | dropped                  | 0.02% to 0.5% on every row                                                                                                                                                                                                                            |
| a sparse (two-level) window                                                                                            | dropped                  | wins no row                                                                                                                                                                                                                                           |
| intervals with a base (boundaries as `u16` from the first)                                                             | dropped                  | rank with no stored counts is 114 instructions; only the synthetic plane row wins (15.0 B to 8.7 B) and no real row has the shape                                                                                                                     |
| Elias-Fano over run boundaries                                                                                         | dropped                  | 17% under the interval list, rank 655 instructions (every boundary is read)                                                                                                                                                                           |
| blocks: Elias-Fano in blocks of 128 members under a table of `last: u32, end: u32`                                     | not in format 2          | 3% to 12% smaller above 256 members on every row, but contains is 1.4 to 2.2 times the root's instructions and `and_len` 1.0 to 1.8 times, at two tuning passes. A candidate for a later format: its blocks are the rank ranges block-max bounds need |
| one collection layout for many tiny lists (offsets as one Elias-Fano sequence, members packed at the universe's width) | a consumer's own plane   | 4.80 B a list against 4.94 B standalone plus what locates each; needs nothing new: the offsets are an image read by `select`                                                                                                                          |

## Costs

Instructions / cycles per operation, the probe loop's 8 instructions included. 65,536 random (set, value) probes in
shuffled order over 3,000 to 20,000 images, so each probe misses the branch history and usually the cache. The crate's
views are opened outside the window; the prototype decodes its header inside it.

| Cell                                                                                 | prototype | small forms             | format 1 |
| ------------------------------------------------------------------------------------ | --------- | ----------------------- | -------- |
| frames: Bitmap contains                                                              | 30 / 6    | 57 / 20                 | 117 / 71 |
| frames: Bitmap rank                                                                  | 86 / 40   | 89 / 28                 | 116 / 69 |
| frames: write a set                                                                  | 246 / 47  | 767 / 137               |          |
| frames: add one member and remove it, with the witness                               | 57 / 27   | 3,552 / 703             |          |
| real tails of 1 to 3: List contains                                                  | 113 / 56  | 107 / 81                | 118 / 88 |
| real tails of 1 to 3: List rank                                                      | 104 / 47  | 125 / 86                | 139 / 90 |
| real tails of 1 to 3: write a set                                                    | 128 / 32  | 2,864 / 1,069           |          |
| real lists above 256: contains, blocks against the Elias-Fano root and the directory | 376 / 292 | 276 / 287 and 302 / 303 |          |
| real lists above 256: seek                                                           | 468 / 326 | 601 / 483 and 774 / 558 |          |
| real lists above 256: `and_len` of two, per member of the smaller                    | 293 / 91  | 234 / 96 and 34.5 / 11  |          |
| 10^6 of 2^32: contains                                                               | 456 / 290 | 208 / 174 and 179 / 141 |          |

`results/proto2-run1.txt` is the first cut and `results/proto2-run2.txt` the tuned one, every cell.

One thing the costs say that is no format matter: an intersection of two directories is 7 to 10 times cheaper per member
than anything over Elias-Fano. A consumer that intersects large lists wants the forest image, and the sealed rule picks
Elias-Fano by bytes alone.
