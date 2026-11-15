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

A set that is one chunk at key zero is a lone image, and its chunk is written as Bytes or Window instead of its arm's
payload when that is strictly shorter and its members lie within 256 positions. A directory holds only the five arms, so
the directory reader's chunk types, loops and kernels are untouched by the small forms. Native v2 states the rule.

The forest-versus-Elias–Fano decision is separate: for at least two members, EF wins only when the **complete framed
image** is strictly smaller. `heap_bytes()` and `serialized_len()` describe different representations and must not be
compared as interchangeable measurements.

## Use

```rust
use bitmosaic::{Bitmosaic, BitmosaicView, Range};

let left = Bitmosaic::from_sorted((0..4096).chain(32768..36864));
let right = Bitmosaic::from_sorted(2048..6144);
let bytes = left.to_bytes();
assert_eq!(bytes.len(), 12);
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

This is bitmosaic's native format, not the standard Roaring format. There is no legacy-format reader: an image of the
first native format (`BMS` and a control byte) is refused. This section is the whole format: a port needs nothing else.

Conventions. Multi-byte integers are little-endian. A _varint_ is ULEB128: seven bits a byte, low group first, the high
bit set on every byte but the last, in its shortest form (an encoding of two or more bytes whose last byte is zero is
refused), at most five bytes for a `u32`. Bit `j` of a byte or word is the one worth `2^j`. There is no alignment
padding anywhere in an image.

### Image and control byte

A set has exactly one image. An image is self-delimiting in every form: its length follows from its own bytes, so it
needs no outer length, and bytes after it (a slot's spare capacity) are not part of it. The first byte is the control:

| Control      | Image                                                                     | Layout                             |
| ------------ | ------------------------------------------------------------------------- | ---------------------------------- |
| `14`         | the empty `u32` set                                                       | the control byte alone             |
| `15`         | the empty `u64` set                                                       | the control byte alone             |
| `10`         | `u32` directory of two or more chunks, or one chunk whose key is not zero | control, `body_bytes` varint, body |
| `12`         | `u32` Elias–Fano                                                          | control, `body_bytes` varint, body |
| `11`         | `u64` directory                                                           | control, `body_bytes` varint, body |
| `80` to `FF` | `u32` lone image: one chunk at key zero                                   | control, count extension, payload  |

Every other control is refused. `body_bytes` counts the body alone, and the whole image must fit the `u32` offset
domain. `IMAGE_ID_LEN` (one), `EMPTY_U32_IMAGE` and `EMPTY_U64_IMAGE` are public constants.

### Chunks and container kinds

A `u32` member's high 16 bits are its chunk key and its low 16 bits its value in that chunk. A chunk holds 1 to 65,536
values, ascending and distinct, in one container. The kind is three bits:

| Kind | Container | Payload                                                                        | Payload bytes                |
| ---- | --------- | ------------------------------------------------------------------------------ | ---------------------------- |
| 0    | Words     | summary, window, bounds, bitmap words, rank directory                          | `24 + 8w + 2(w/4 - 1)`       |
| 1    | Stride    | `first:u16, step:u16`; member `i` is `first + i*step`                          | 4                            |
| 2    | Cone      | `first:u16, eps:u8, scale:u64`, then one signed residual byte a member         | `11 + n`                     |
| 3    | Array     | the members as `u16`                                                           | `2n`                         |
| 4    | Runs      | run count, `(start:u16, end:u16)` records, interior prefixes                   | below                        |
| 5    | Bytes     | `first` as a varint, then one byte a further member: its distance from `first` | `varint_len(first) + n - 1`  |
| 6    | Window    | `first` as a varint, then a bitmap of 1 to 16 bytes                            | `varint_len(first) + bitmap` |
| 7    | Window    | the same with a bitmap of 17 to 32 bytes                                       | `varint_len(first) + bitmap` |

Kinds 5 to 7, the small containers, exist only as the chunk of a lone image; in a directory's kind plane they are
refused.

`n` is the chunk's cardinality. It is never stored in a payload: a directory's prefix plane or a lone image's control
byte gives it.

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

**Bytes.** `first` is at most 65,535 and in shortest varint form. The `n - 1` distances are `1..=255`, strictly
ascending, and `first + distance <= 65535`. `n` is 1 to 256: with no distance the payload is a singleton.

**Window.** Bit `p` of the bitmap (bit `p mod 8` of byte `p div 8`) is the value `first + 1 + p`. The last byte is not
zero, `n = 1 + popcount`, and the last member is at most 65,535. Kind 6 holds a bitmap of 1 to 16 bytes and kind 7 one
of 17 to 32; a length outside its kind's range is refused.

### Lone image

`control = 0x80 | folded << 3 | kind`, `folded` in `0..=15`. The count it carries is the one number the payload does not
say:

- kinds 6 and 7: the bitmap's length is `folded + 1` bytes for kind 6 and `folded + 17` for kind 7. The cardinality is
  one more than the bitmap's popcount. No extension follows.
- every other kind: the cardinality. `folded <= 13` is the cardinality `folded + 1` (1 to 14). `folded = 14` is followed
  by one byte `b`: cardinality `15 + b` (15 to 270). `folded = 15` is followed by a `u16` `c`: cardinality `c + 1`,
  which must be at least 271.

The payload follows, with the length the table above gives. Two lengths are read from the payload itself: Runs from its
run count, Words from `w`. A singleton below 128 is two bytes, `85` and the member.

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

One chunk at key zero is never a directory: it is the lone image, and a directory of it is refused.

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
lone image as it stands, or control `10` or `12` and the body. An empty child is refused.

### Writing: the one image of a set

1. **Arm, per chunk** of `n` values with `r` maximal runs:
   - `n = 1`: Array.
   - all gaps equal: Stride.
   - `n <= 4096`: the cheapest of Cone at `16 + n` (only when `n <= 1024` and, with
     `scale = floor(span * 2^32 / (n - 1))` over `span = last - first`, every residual is within `-63..=63`), Array at
     `2n`, and Runs at `6r`. A tie goes to Cone, then Array, then Runs.
   - `n > 4096`: Runs when `6r < 8720`, otherwise Words.
2. **Forest-family root.** No member: the empty image. One chunk at key zero: the lone image. Otherwise the directory,
   every chunk as its arm. This is the image a patched slot holds (`to_forest_bytes`).
3. **Small form, for the chunk of a lone image.** When `last - first <= 255`, Bytes costs `varint_len(first) + n - 1`
   and Window `varint_len(first) + ceil((last - first) / 8)`. The shortest of the arm's payload and these two is
   written; the arm wins a tie with either, and Bytes wins a tie with Window.
4. **Sealed root** (`to_bytes`). With two or more members whose Elias–Fano geometry fits the `u32` position domain, the
   Elias–Fano image when it is strictly shorter than the forest-family image; otherwise the forest-family image.
5. **u64.** Members grouped by key; each child is the sealed image of step 4, compared at its standalone length.

### Admission

`open` is a structural probe returning `Option`: control, framing, directory geometry, kinds, adjacent payload extents,
counts, Runs records and prefixes, Window structure. `open_verified` returns `Result<View, AdmissionError>` and
additionally verifies Array, Bytes and Cone ordering and query hints, Words metadata, and Elias–Fano samples and member
content, including u64 children. Neither allocates. `image_header(bytes)` returns
`ImageHeader { width, encoded_len, .. }` in bounded constant work, from the control byte and at most the 20 bytes after
a lone header.

`AdmissionError` retains the typed `class`, the image-relative byte `offset`, and a static `what`. A child refusal in a
u64 directory is shifted to the complete image. `AdmissionClass::as_str()` names the rules:

| Class         | Refuses                                                                                                                                                                                                                                                                              |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `extent`      | a missing control byte; a truncated or non-shortest `body_bytes`; a body, plane or payload that is truncated or does not cover its extent exactly; a truncated lone count; a Window bitmap outside its kind's range or ending in a zero byte                                         |
| `kind`        | an unassigned control; an image of the other key width; a directory of one chunk at key zero; a directory chunk of kind 5 to 7; a u64 child that is not a nonempty `u32` image                                                                                                       |
| `count`       | a non-shortest varint count; a two-byte lone count below 271; a chunk cardinality outside `1..=65536` or outside its kind's range; prefixes that do not ascend or disagree with their payloads (Runs, Words, Window popcount, u64 children); a Words window that is not whole blocks |
| `map-order`   | chunk keys, u64 keys, Array members, Bytes distances, Cone members, runs or Elias–Fano members that do not ascend strictly                                                                                                                                                           |
| `plane-range` | a value outside its domain: a zero Stride step, a member past 65,535, a first-member varint past 65,535, a Cone model that overflows or whose window misses a member, Words bounds, summary or rank entries that disagree with the words, an Elias–Fano base plus span past `u32`    |
| `bit-pad`     | a nonzero unused bit in the kind plane or an Elias–Fano plane                                                                                                                                                                                                                        |
| `placement`   | payload or child offsets that are not adjacent within their area; a slot-plane entry or an Elias–Fano sample that names the wrong position                                                                                                                                           |

Admission proves an image well formed. It does not re-run the writer's selection: a well-formed image whose kinds are
not the ones steps 1 and 3 choose is admitted. A consumer that hashes images hashes the writer's.

### Reading

The operations a reader answers, all without allocating: `open_verified` (admission), `contains(value)`, `rank(value)`
(members strictly below), `select(k)` (the `k`-th member ascending, from zero), the ascending walk (`for_each`, and the
pull cursor `range()` with `seek`), `len()` and `serialized_len()` (the image's extent).

All mapped reads are byte-backed. Words and balanced Array intersection retain their NEON byte-load kernels at odd
offsets; unsupported targets use the portable little-endian scalar implementation rather than a realignment copy.

### Sizes

| Set                                           |                         Complete image |
| --------------------------------------------- | -------------------------------------: |
| Empty u32 or u64                              |                                 1 byte |
| u32 singleton below 128                       |                                2 bytes |
| u32 singleton below 65,536                    |                                3 bytes |
| u32 singleton above                           |                                9 bytes |
| 4 ordinals of 83                              |                                5 bytes |
| 13 ordinals of 70                             |                             at most 11 |
| any set within 256 positions, first below 128 | at most `2 + ceil((last - first) / 8)` |
| u32 interval `0..=99`                         |                                6 bytes |
| u32 `0..=4095` plus `32768..=36863`           |                               12 bytes |
| u64 singleton below 128                       |                                9 bytes |

## Fixed-capacity mutation

Initialize a slot with an actual native image, including the one-byte empty image. No outer length word or fixed header
reserve is needed.

<a id="smoo/bitmosaic!n/patch-small-forms"></a>A patched slot holds the forest-family image of its set: empty, lone or
directory, whichever the surviving set selects, so a patch moves a slot between them and between every container kind,
the small ones included, and the result is byte-equal to a fresh `to_forest_bytes()`. A lone image whose header keeps
its length is edited where it lies (a dense lone chunk takes one member without the image being rebuilt); a header that
changes length, or a chunk set that changes, rebuilds the image in retained scratch before the slot is touched.

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
admission class. It covers every container kind alone and in a directory, both sides of every selection boundary, every
lone count width, the slot plane, the Elias–Fano root and the u64 tier. `tests/vectors.rs` holds the file to what the
crate writes, admits and refuses; regenerate it with the `conformance_vectors` example.

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
