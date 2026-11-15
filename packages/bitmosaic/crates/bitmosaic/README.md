# bitmosaic

Compressed ordered sets over `u32` and `u64`, with owned builders, allocation-free borrowed views, rank/select, set
algebra, and witnessed updates to fixed-capacity serialized slots. The crate has no production dependencies.

## Container selection

The `u32` domain is split into chunks covering all 65,536 low-bit values. Each nonempty chunk chooses one in-memory arm
(the ladder). The selection price is the owned payload price, **not** its serialized size:

| Arm    | Shape                                               | Selection price          |
| ------ | --------------------------------------------------- | ------------------------ |
| Stride | Pure arithmetic progression                         | 6 bytes                  |
| Cone   | Fixed-point model plus signed-byte residuals        | `16 + cardinality` bytes |
| Array  | Sorted unique `u16` values                          | `2 * cardinality` bytes  |
| Words  | Dense plane, block summary, rank directory          | 8,720 bytes              |
| Runs   | Maximal inclusive intervals with exclusive prefixes | `6 * run_count` bytes    |

A singleton selects Array; a recognized progression selects Stride first. Cone is eligible through 1,024 members, Array
through 4,096. The remaining candidates keep the existing Words/Cone/Array tie order. Runs is considered last and must
be strictly cheaper. Consequently a single long interval, or an alternating singleton progression, remains Stride rather
than a large run list.

A set that is one chunk at key zero is written as a small image (a byte list, a value-aligned bitmap window, a `u16`
list or an interval list, behind one tag byte) whenever that is no longer than its directory. A directory holds only the
five arms, and the small forms have a reader of their own, so the directory reader's chunk types, loops and kernels are
untouched by them. Native v2 states the rule.

The forest-versus-Elias–Fano decision is separate: for at least two members, EF wins only when the **complete framed
image** is strictly smaller. `heap_bytes()` and `serialized_len()` describe different representations and must not be
compared as interchangeable measurements.

## Use

```rust
use bitmosaic::{Bitmosaic, BitmosaicView, Range};

let left = Bitmosaic::from_sorted((0..4096).chain(32768..36864));
let right = Bitmosaic::from_sorted(2048..6144);
let bytes = left.to_bytes();
assert_eq!(bytes.len(), 9);
let view = BitmosaicView::open_verified(&bytes).expect("valid native image");
assert_eq!(view.rank(32768), 4096);
assert_eq!(left.and_len(&right), 2048);
let mut cursor = view.range();
cursor.seek(33000);
assert_eq!(cursor.next(), Some(33000));
```

`BitmosaicBuilder` supports insertion, removal, membership, retained clearing, and freezing. Sorted `u64` input streams
into `Bitmosaic64`. Lazy intersection, union, difference, and symmetric-difference cursors compose without an
intermediate member buffer. Seeking into a long run is arithmetic; counting run intersections works on intervals rather
than enumerating their members.

## Native v2

This is bitmosaic's native format, not the standard Roaring format. There is no legacy-format reader and no
identification beyond the control byte: bytes of the first native format (`BMS` and a control byte) are not recognized
as such, and may read as a set of this format. This section is the whole format: a port needs nothing else.

Conventions. Multi-byte integers are little-endian. A _varint_ is ULEB128: seven bits a byte, low group first, the high
bit set on every byte but the last, in its shortest form (an encoding of two or more bytes whose last byte is zero is
refused), at most five bytes for a `u32`. Bit `j` of a byte or word is the one worth `2^j`. There is no alignment
padding anywhere in an image.

### Image and control byte

A set has exactly one sealed image. An image is self-delimiting in every form: its length follows from its own bytes, so
it needs no outer length, and bytes after it (a slot's spare capacity) are not part of it. The first byte is the
control:

| Control      | Image                                   | Layout                             |
| ------------ | --------------------------------------- | ---------------------------------- |
| `14`         | the empty `u32` set                     | the control byte alone             |
| `15`         | the empty `u64` set                     | the control byte alone             |
| `10`         | `u32` directory                         | control, `body_bytes` varint, body |
| `12`         | `u32` Elias–Fano                        | control, `body_bytes` varint, body |
| `11`         | `u64` directory                         | control, `body_bytes` varint, body |
| `20` to `C0` | `u32` small image: members below 65,536 | control (the tag), payload         |

Every other control is refused. `body_bytes` counts the body alone, and the whole image must fit the `u32` offset
domain. `IMAGE_ID_LEN` (one), `EMPTY_U32_IMAGE` and `EMPTY_U64_IMAGE` are public constants.

### Small images

A small image holds a set whose members are all below 65,536 (chunk key zero) with no directory. Its tag is `kk lllll`:
`kk` (bits 5 to 7, values 1 to 6) is the form and, for forms 1 to 5, `lllll + 1` (1 to 32) the count. The payload
follows; its length follows from the tag alone, except a Window wide's, which its third payload byte gives:

| `kk` | Tag          | Form        | Count is                | Payload                                                                                                                           | Payload bytes |
| ---- | ------------ | ----------- | ----------------------- | --------------------------------------------------------------------------------------------------------------------------------- | ------------- |
| 1    | `20` to `3F` | List8       | members                 | the members, each below 256, as ascending bytes                                                                                   | `count`       |
| 2    | `40` to `5F` | Window      | bitmap bytes            | a bitmap: bit `j` of byte `i` is the member `8i + j`                                                                              | `count`       |
| 3    | `60` to `7F` | Window at   | bitmap bytes            | `start` (one byte, at least 1), then a bitmap: bit `j` of byte `i` is the member `8(start + i) + j`                               | `1 + count`   |
| 4    | `80` to `9F` | List16      | members                 | the members as ascending `u16`                                                                                                    | `2 * count`   |
| 5    | `A0` to `BF` | Spans       | intervals               | `count` pairs `(start: u16, end: u16)`, both inclusive                                                                            | `4 * count`   |
| 6    | `C0`         | Window wide | (none: `lllll` is zero) | `start: u16`, `len - 1` (one byte), then a bitmap of `len` bytes (1 to 256): bit `j` of byte `i` is the member `8(start + i) + j` | `3 + len`     |

**List8, List16.** Strictly ascending.

**Window, Window at, Window wide.** A bitmap of whole bytes aligned in the value domain: a member `v` is bit `v mod 8`
of the byte for `v div 8`, whatever the window's start, so two windows AND byte for byte where they overlap. The last
bitmap byte is not zero. In a Window at, `start` is at least 1 and the first bitmap byte is not zero either; a window
that would start at byte zero is a Window. In a Window wide the first bitmap byte is not zero and `start + len <= 8192`.
The cardinality is the bitmap's popcount.

**Spans.** `start <= end`; intervals ascend and neither overlap nor touch (`end_i + 1 < start_{i+1}`). A singleton
interval has `start = end`. The cardinality is the sum of `end - start + 1`.

Examples: `{7}` is `40 80`; `{7, 200}` is `21 07 C8`; `{0, 1, 2, 10}` is `41 07 04`; `0..=82` is `A0 00 00 52 00`;
`{1000, 2000}` is `81 E8 03 D0 07`; the even numbers `3000, 3002, ..` up to `3398` with `3001` are `C0 77 01 31` and 50
bitmap bytes (`start` 375, `len` 50), 54 bytes in all.

Every form is read where it lies: `contains` on a window is one byte load, a shift and a mask; rank and the intersection
of two windows are popcounts over at most four unaligned 64-bit loads (a partial last word is two overlapping narrower
loads, so no read leaves the image); `select` on a list is one indexed load. A reader holds a pointer, a length and the
window's start, and copies no member.

### Chunks and container kinds

A `u32` member's high 16 bits are its chunk key and its low 16 bits its value in that chunk. In a directory a chunk
holds 1 to 65,536 values, ascending and distinct, in one container. The kind is three bits:

| Kind | Container | Payload                                                                | Payload bytes          |
| ---- | --------- | ---------------------------------------------------------------------- | ---------------------- |
| 0    | Words     | summary, window, bounds, bitmap words, rank directory                  | `24 + 8w + 2(w/4 - 1)` |
| 1    | Stride    | `first:u16, step:u16`; member `i` is `first + i*step`                  | 4                      |
| 2    | Cone      | `first:u16, eps:u8, scale:u64`, then one signed residual byte a member | `11 + n`               |
| 3    | Array     | the members as `u16`                                                   | `2n`                   |
| 4    | Runs      | run count, `(start:u16, end:u16)` records, interior prefixes           | below                  |

Kinds 5 to 7 are refused. `n` is the chunk's cardinality. It is never stored in a payload: the directory's prefix plane
gives it.

**Stride.** `step >= 1` and `first + (n - 1) * step <= 65535`.

**Cone.** Member `i` is `first + floor(i * scale / 2^32) + residual_i`, the residual a signed byte. `eps` is the largest
absolute residual. `n * scale` fits a `u64`. Members ascend strictly and stay within `0..=65535`.

**Array.** Strictly ascending.

**Runs.** A varint `r - 1` (`r` runs, at most 32,768), then `r` records of inclusive `start` and `end`, then one `u16`
for each interior run `1..=r-2`: the members before it. Runs ascend, and no two overlap or touch
(`end_i + 1 < start_{i+1}`). The payload is `varint_len(r - 1) + 4r + 2 * max(r - 2, 0)` bytes; two runs occupy nine.

**Words.** At offset 0 two `u64` summary words: bit `b` (of 128) is set exactly when a member lies in positions
`512b..512b+511`. At 16 `base_word:u16` and at 18 `w:u16`: the window of 64-bit words the bitmap holds, both multiples
of 8, `w >= 8`, `base_word + w <= 1024`, the smallest such window that holds every member. At 20 the smallest member and
at 22 the largest, as `u16`. At 24 the `w` words: bit `j` of word `i` is the value `(base_word + i) * 64 + j`. Then
`w/4 - 1` rank entries as `u16`: entry `g` (from 1) is the number of members in window words `0..4g`.

### u32 directory body

In order:

1. Cardinality minus one, varint. Decoding uses `u64`, permitting cardinality `2^32`.
2. Chunk count `c` minus one, varint, at most 65,535.
3. `c` ascending `u16` keys.
4. `c - 1` exclusive cardinality prefixes as `u32`: the members before chunks `1..c`. The first is implicitly zero and
   the header cardinality closes the last chunk.
5. `c - 1` payload offsets as `u32`, for chunks `1..c`, relative to the payload area; the first is implicitly zero and
   the body's end closes the last payload.
6. `c` three-bit kinds, packed from bit 0 of the first byte upward, `ceil(3c / 8)` bytes. Unused bits are zero. Each
   kind is 0 to 4.
7. The slot plane, present exactly when `c > 64` and the keys are not one unbroken run (`last - first != c - 1`): `2^s`
   slots of `u64` with `s = log2(next_power_of_two(floor(8c / 7) + 1))`. Chunk `i` with key `k` is stored as
   `k << 32 | (i + 1)` in the first empty slot at or after `(k * 0x9E3779B97F4A7C15 mod 2^64) >> (64 - s)`, probing
   upward and wrapping, chunks taken in key order. Every other slot is zero.
8. The container payloads, adjacent, in key order.

A directory of one chunk at key zero is 7 bytes and its payload while the set has fewer than 129 members (control,
`body_bytes`, cardinality, chunk count, key, kind).

### Elias–Fano body

Cardinality `n` minus one, `base` and `span` as varints, where `base` is the first member and `base + span` the last
(`n <= span + 1`). With `d = member - base`:

- `L`, the low width: 0 when `span <= n`; otherwise `floor(log2 span) - floor(log2 n)`, less one when that is not zero
  and `span >> L < n`.
- The high plane: `ceil((buckets + n) / 64)` `u64` words, `buckets = (span >> L) + 1`. Member `k` (from 0) sets bit
  `(d >> L) + k`. Bits past `buckets + n` are zero.
- The low plane: `ceil(n * L / 8)` bytes, member `k`'s field `d mod 2^L` at bits `k*L..(k+1)*L`. Unused bits are zero.
- `floor((n - 1) / 64)` one-samples as `u32`: entry `j` (from 1) is the high-plane bit of member `64j`.
- `ceil(buckets / 64)` zero-samples as `u32`: entry `j` (from 0) is the high-plane bit of zero number `64j`, which is
  `64j` plus the members whose `d >> L` is at most `64j`.

### u64 directory body

A `u64` member's high 32 bits are its key. In order: the child count `m` minus one as a varint; `m` ascending `u32`
keys; `m - 2` cardinality prefixes as `u64`, the members before children `1..m-1` (the first is zero and the last is
derived); `m - 1` child offsets as `u32` for children `1..m`, relative to the child area; then the children, adjacent. A
child is the sealed `u32` image of its low halves **without its `body_bytes` varint** (the offsets say where it ends): a
small image as it stands, or control `10` or `12` and the body. An empty child is refused.

### Writing: the one image of a set

1. **Arm, per chunk** of `n` values with `r` maximal runs:
   - `n = 1`: Array.
   - all gaps equal: Stride.
   - `n <= 4096`: the cheapest of Cone at `16 + n` (only when `n <= 1024` and, with
     `scale = floor(span * 2^32 / (n - 1))` over `span = last - first`, every residual is within `-63..=63`), Array at
     `2n`, and Runs at `6r`. A tie goes to Cone, then Array, then Runs.
   - `n > 4096`: Runs when `6r < 8720`, otherwise Words.
2. **Directory.** Every chunk as its arm.
3. **Small form**, when the set is one chunk at key zero. With `first` and `last` its end members, `n` its cardinality
   and `r` its runs, the candidates and their payload lengths are, in this order:
   1. Window, when `last div 8 <= 31`: `last div 8 + 1`.
   2. Window at, when `first div 8` is 1 to 255 and `last div 8 - first div 8 <= 31`: `2 + last div 8 - first div 8`.
   3. Spans, when `r <= 32`: `4r`.
   4. List8, when `last <= 255` and `n <= 32`: `n`.
   5. List16, when `n <= 32`: `2n`.
   6. Window wide, when `last div 8 - first div 8 <= 255`: `4 + last div 8 - first div 8`.

   The shortest candidate is the small form; the earlier one wins a tie. The small image (one byte more than its
   payload) is written when it is no longer than the directory image of step 2; otherwise the directory.

4. **Forest family** (`to_forest_bytes`, and what a patched slot holds). No member: the empty image. Otherwise step 3
   for one chunk at key zero, step 2 for anything else.
5. **Sealed image** (`to_bytes`). With two or more members whose Elias–Fano geometry fits the `u32` position domain, the
   Elias–Fano image when it is strictly shorter than the forest-family image; otherwise the forest-family image.
6. **u64.** Members grouped by key; each child is the sealed image of step 5, compared at its standalone length (as if
   it carried its `body_bytes`).

### Admission

Three entries, by what the caller needs:

| Entry            | Proves                                                                                                                                                                                                                       | Who calls it                                               |
| ---------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------- |
| `open`           | structure: control, framing, directory geometry, kinds, payload extents, counts                                                                                                                                              | a hot attach path over bytes it wrote or already admitted  |
| `open_verified`  | `open`, every member and derived plane, and that every form is the writer's: the image is byte-equal to the sealed or the forest-family image of its set, or is an Elias–Fano image of it (a patch keeps an Elias–Fano root) | a consumer that patches slots, or reads bytes from outside |
| `open_canonical` | `open_verified`, and the sealed root choice: the image is byte-equal to `to_bytes()`                                                                                                                                         | a consumer that hashes or compares images                  |

`open` returns `Option`; the other two return `Result<View, AdmissionError>`. None allocates. `open_verified` runs the
selection rules above over the members of every chunk: one more pass, with up to 16 KiB of stack. `image_header(bytes)`
returns `ImageHeader { width, encoded_len, .. }` in bounded constant work, from the control byte and, for a framed
image, its `body_bytes`, for a Window wide its length byte.

`AdmissionError` retains the typed `class`, the image-relative byte `offset`, and a static `what`. A child refusal in a
u64 directory is shifted to the complete image. `AdmissionClass::as_str()` names the rules:

| Class         | Refuses                                                                                                                                                                                                                                                                                                                                                                                                      |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `extent`      | a missing control byte; a truncated or non-shortest `body_bytes`; a body, plane or payload that is truncated or does not cover its extent exactly; a window's bitmap that ends, or a Window wide or Window at bitmap that starts, in a zero byte                                                                                                                                                             |
| `kind`        | an unassigned control; an image of the other key width; a Window at whose `start` is zero; a directory chunk of kind 5 to 7; a u64 child that is not a nonempty `u32` image                                                                                                                                                                                                                                  |
| `count`       | a non-shortest varint count; a chunk cardinality outside `1..=65536` or outside its kind's range; prefixes that do not ascend or disagree with their payloads (Runs, Words, u64 children); a Words window that is not whole blocks                                                                                                                                                                           |
| `map-order`   | chunk keys, u64 keys, Array, List8, List16 or Cone members, runs, intervals or Elias–Fano members that do not ascend strictly; intervals that overlap or touch                                                                                                                                                                                                                                               |
| `plane-range` | a value outside its domain: a zero Stride step, a member past 65,535, a Window wide whose `start + len` passes 8,192, a Cone model that overflows or whose window misses a member, Words bounds, summary or rank entries that disagree with the words, an Elias–Fano base plus span past `u32`                                                                                                               |
| `bit-pad`     | a nonzero unused bit in the kind plane or an Elias–Fano plane                                                                                                                                                                                                                                                                                                                                                |
| `placement`   | payload or child offsets that are not adjacent within their area; a slot-plane entry or an Elias–Fano sample that names the wrong position                                                                                                                                                                                                                                                                   |
| `canonical`   | a well-formed image that is not the writer's for its set: a chunk whose members select another kind (step 1); a Cone whose `scale` or `eps` is not step 1's or the largest residual; a directory of one chunk at key zero where a small form is no longer (step 3); a small image that is not the selected form; under `open_canonical`, a root that is not step 5's; a u64 child whose root is not step 5's |

### Reading

The operations a reader answers, all without allocating: admission, `contains(value)`, `rank(value)` (members strictly
below), `select(k)` (the `k`-th member ascending, from zero), the ascending walk (`for_each`, and the pull cursor
`range()` with `seek`), `len()`, `serialized_len()` (the image's extent), `and_len(other)` (the intersection's
cardinality) and `and_rank(other, k)` (members of the intersection below `k`).

All mapped reads are byte-backed. Words and balanced Array intersection retain their NEON byte-load kernels at odd
offsets; unsupported targets use the portable little-endian scalar implementation rather than a realignment copy. The
small forms need only shifts, AND, popcount and count-trailing-zeros, which wasm32 and Go (`math/bits`) have; `select`
in a word is six halvings by popcount, so no port needs a bit-deposit instruction.

### Sizes

| Set                                         |                         Complete image |
| ------------------------------------------- | -------------------------------------: |
| Empty u32 or u64                            |                                 1 byte |
| u32 singleton below 256                     |                                2 bytes |
| u32 singleton below 65,536                  |                                3 bytes |
| u32 singleton above                         |                                9 bytes |
| ordinals `0, 1, 2, 10`                      |                                3 bytes |
| 4 ordinals of 83                            |                              at most 5 |
| 13 ordinals of 70                           |                             at most 10 |
| 13 ordinals spread over 129                 |                               14 bytes |
| any set below 256                           |                             at most 33 |
| any set within 32 value-aligned bytes       | at most `3 + last div 8 - first div 8` |
| a dense set spanning up to 2,048 positions  | at most `5 + last div 8 - first div 8` |
| one interval below 65,536, such as `0..=99` |                                5 bytes |
| 4 members below 65,536                      |                              at most 9 |
| u32 `0..=4095` plus `32768..=36863`         |                                9 bytes |
| u64 singleton below 256                     |                                9 bytes |

## Fixed-capacity mutation

Initialize a slot with an actual native image, including the one-byte empty image. No outer length word or fixed header
reserve is needed.

<a id="smoo/bitmosaic!n/patch-small-forms"></a>A patched slot holds the forest-family image of its set: empty, small or
directory, whichever the surviving set selects, so a patch moves a slot between them, between every small form and
between every container kind, and the result is byte-equal to a fresh `to_forest_bytes()`. A slot that holds a small or
empty image, patched by a batch whose adds are all below 65,536, is one merge of its members with the batch and one
encode: no plan, and scratch the size of the image. Any other change of form rebuilds the image in retained scratch
before the slot is touched.

`patch` applies `(old union adds) minus removes`; both batches are sorted and unique. `patch_witnessed` also reports
additions absent from **old**, and removals present in **old union adds**. Witnesses are provisional on refusal.
Capacity, malformed-image, and unsorted-batch errors leave slot bytes unchanged.

Runs and contiguous Stride updates merge intervals with batch events in one sweep. A single surviving interval with at
least two members emits its Stride payload directly from retained bounds and cardinality; a singleton remains Array.
Retained interval scratch is bounded by the chooser: at most 1,453 records before spilling into the existing dense
plane. Array, Cone and noncontiguous Stride payloads merge their ordered members directly when old cardinality plus
additions is at most 4,096, without staging and extracting a dense plane. The same merge produces cardinality changes
and per-value witnesses. Sparse member scratch stays bounded by Array eligibility; larger results retain the dense path.
A Words in-place trial counts result runs with word masks/carry and proves that Words still wins the common chooser.

The patch keeps its root, except that an EF result with fewer than two members falls back to forest/empty. Geometry or
varint-width changes assemble the new image in retained scratch before touching the slot. Stable geometry retains the
touched-payload/rigid-tail path with byte offsets. Forest results agree with fresh `to_forest_bytes()` output under the
same chooser.

`prepare_patch_witnessed` retains that same validated plan without writing the image. For an embedded slot,
`prepare_patch_in_witnessed(state, slot_range, adds, removes, scratch, witness)` keeps the enclosing buffer borrowed as
well. The returned `PreparedPatch` exposes `state()` read-only, `old_serialized_len()` and `new_serialized_len()`, and
sorted, disjoint `mutation_ranges()` as slot-relative `(offset, length)` pairs. These cover framing, directory updates,
changed payload extents and intermediate tail moves; a sparse stable Words edit does not capture untouched chunks or
unused capacity. `cleared_tail_range()` separately identifies the old framed tail for callers that clear it.

A physical journal can coalesce those ranges with its metadata ranges, capture prior bytes through `state()`, and refuse
by dropping the plan if admission fails. `apply()` consumes the retained writer once and returns
`(PatchReport, original_state)`; `apply_with_cleared_tail()` also zeros only the old framed tail. Existing `patch` and
`patch_witnessed` use this same preparation/application path and preserve their tail behavior. The plan cannot be
redirected to a different image, and its batches, witness and scratch cannot change while it is retained. Independent
state/work lifetimes release scratch and witness access on application even when the returned state reference is kept.
`witness()` exposes complete per-value answers on successful preparation, allowing dependent metadata/index plans before
journal admission. Words retains the paid trial's fresh/held masks and publishes them only after its verdict; a failed
trial discards them before the general planner reports. Application does not publish those answers again. The final
counts remain in the application report; no provisional report is exposed. All preparation refusals and dropping an
admitted plan leave the complete enclosing buffer unchanged.

## Verification and measurements

From the smoothbricks workspace root:

```sh
nx run bitmosaic:cargo-test
nx run bitmosaic:bench
```

<a id="smoo/bitmosaic!n/small-set-sizes"></a>`tests/small_sets.rs` holds the size table above as a test: each row's
image is written, admitted and read back, and may not exceed its stated bytes, nor three bytes above a plain bitmap of
its span within 256 positions.

<a id="smoo/bitmosaic!n/conformance-vectors"></a>`tests/vectors/native-v2.json` is the conformance document a port is
checked against: for every case the member list and the canonical image as hex, and for every refusal the image and its
admission class. It covers every small form and every container kind, alone and in a directory, both sides of every
selection boundary, the slot plane, the Elias–Fano root and the u64 tier, and for every form a well-formed image that is
not the writer's, with the admission that refuses it. `tests/vectors.rs` holds the file to what the crate writes, admits
and refuses; regenerate it with the `conformance_vectors` example.

The existing wire, range, skew, and patch oracles compare observable behavior against BTreeSet and development-only
roaring-rs. They cover hostile framing, unaligned images, the full-u32 cardinality boundary without enumerating the
domain, all five-by-five container count/algebra pairings, patch witnesses, refusal atomicity, and retained-scratch
allocation behavior.

`bitmosaic::oracle` (present with `bench-internals` and in the library's own tests) holds two operation-list steps whose
inputs come from a recipe aimed at the container ladder. `patch_step` patches one image and checks, after every
operation, that its bytes are a fresh encoding and that the report, witnesses and reads match a BTreeSet; `and_step`
intersects two sets on every forced skew arm, owned, mapped and through the u64 tier. The default test lane runs both
from fixed seeds, and every container-tag pairing on every forced arm. `nx run bitmosaic:fuzz` drives the same steps
with cargo-fuzz (`fuzz/`, its own workspace and lockfile) for five minutes each; it is a dedicated proof outside the
ordinary gate.

Benchmarks use the bench profile and report workload-specific observations: attach separately from attached operations,
owned accounting separately from wire bytes, run-rich count/algebra cells, enumerable/restaged controls, and warm patch
allocations. No speed ratio from an earlier layout is a claim about native v2. Compare interleaved controls on the same
binary and machine before drawing performance conclusions.

Where the host is too loaded for wall time to compare two builds, the `format_gate` example runs one borrowed read, seal
or patch a fixed number of times on the large-view shapes of `benches/ops.rs`; `/usr/bin/time -l` then gives the
instructions retired, and the difference between a run of N iterations and one of zero, divided by N, is the operation's
cost in either build.

The sparse patch control is reproducible with:

```sh
cargo run --locked -p bitmosaic --profile bench --features bench-internals --example patch_sequence
```

It alternates the former Cone/Stride dense staging path and interval member expansion against direct extraction in one
binary, restores the same input outside timing, compares canonical output bytes and reports warm allocation calls/bytes.
Append `-- interval` to select interval shifts, growth and shrinkage, including both directions across 4,096 members.
Singleton and above-bound interval results are unchanged-path controls. These paths serve the VM's bitmap insert/remove
opcodes, not just owned set algebra. The native payload remains the source of truth: its existing order feeds the merge,
and counts/witnesses come from that same pass. No cache or second store is introduced.

Standard Roaring import/export is a possible later public-request interoperability feature. No Roaring codec or
production Roaring dependency is implemented.

## License and attribution

MIT OR Apache-2.0. The component was extracted from the minigraf fork; the original license notices and attribution are
retained in `LICENSE-MIT` and `LICENSE-APACHE`.
