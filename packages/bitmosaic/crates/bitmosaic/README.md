# bitmosaic

Compressed ordered sets over `u32` and `u64`, with owned builders, allocation-free borrowed views, rank/select, set
algebra, and witnessed updates to fixed-capacity serialized slots. The crate has no production dependencies.

## Container selection

The `u32` domain is split into chunks covering all 65,536 low-bit values. Each nonempty chunk chooses one
representation. The selection price is the owned payload price, **not** its serialized size:

| Container | Shape                                               | Selection price          |
| --------- | --------------------------------------------------- | ------------------------ |
| Stride    | Pure arithmetic progression                         | 6 bytes                  |
| Cone      | Fixed-point model plus signed-byte residuals        | `16 + cardinality` bytes |
| Array     | Sorted unique `u16` values                          | `2 * cardinality` bytes  |
| Words     | Dense plane, block summary, rank directory          | 8,720 bytes              |
| Runs      | Maximal inclusive intervals with exclusive prefixes | `6 * run_count` bytes    |

A singleton selects Array; a recognized progression selects Stride first. Cone is eligible through 1,024 members, Array
through 4,096. The remaining candidates keep the existing Words/Cone/Array tie order. Runs is considered last and must
be strictly cheaper. Consequently a single long interval, or an alternating singleton progression, remains Stride rather
than a large run list.

The forest-versus-Elias–Fano decision is separate: for at least two members, EF wins only when the **complete framed
image** is strictly smaller. `heap_bytes()` and `serialized_len()` describe different representations and must not be
compared as interchangeable measurements.

## Use

```rust
use bitmosaic::{Bitmosaic, BitmosaicView, Range};

let left = Bitmosaic::from_sorted((0..4096).chain(32768..36864));
let right = Bitmosaic::from_sorted(2048..6144);
let bytes = left.to_bytes();
assert_eq!(bytes.len(), 20);
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

## Native v1

This is bitmosaic's native format, not the standard Roaring format. There is no legacy-format reader.

Every image starts with ASCII `BMS` and one control byte:

`control = (1 << 3) | (empty << 2) | (ef << 1) | width`

`width` is zero for `u32` and one for `u64`. Legal controls are `08` (u32 forest), `09` (u64 directory), `0A` (u32 EF),
`0C` (empty u32), and `0D` (empty u64), in hexadecimal. Other modes and versions are rejected. Empty images consist of
exactly these four identification bytes.

A nonempty image is `[identification:4][body_bytes:ULEB128-u32][body]`. `body_bytes` excludes both the identification
and the length's own bytes. The whole image must fit the u32 byte-offset domain. Integers use canonical ULEB128;
searchable directory planes use fixed-width little-endian values. There is no alignment padding inside an image.

### u32 forest body

In order:

1. Cardinality minus one, ULEB128-u32. Decoding uses u64, permitting cardinality `2^32`.
2. Chunk count minus one, ULEB128-u16.
3. Ascending `u16` keys.
4. Interior exclusive cardinality prefixes, `u32[chunks - 1]`; the first prefix is implicitly zero and the final
   after-value is the header cardinality.
5. Interior payload byte offsets, `u32[chunks - 1]`, relative to the payload area; the first offset is implicitly zero.
6. Three-bit kinds, packed LSB-first: Words 0, Stride 1, Cone 2, Array 3, Runs 4. Unused bits are zero; kinds 5–7 are
   invalid.
7. The derived key-probe slot plane, only where the key count/span requires it.
8. Tightly adjacent container payloads.

Stride stores `first:u16, step:u16`. Cone stores `first:u16, eps:u8, scale:u64` and one residual byte per member. Array
stores only its u16 members. Words stores its summary, occupied word window, member bounds, words, and rank directory;
the always-zero first rank entry is omitted.

Runs stores a canonical ULEB128-u16 run count minus one, then packed `(start:u16, end:u16)` records. Runs are ascending,
nonempty, disjoint, and nonadjacent. Exclusive u16 prefixes are stored only for interior records: the first is zero; the
final prefix is cardinality minus the final interval length. Its exact payload size is

`uleb_len(run_count - 1) + 4 * run_count + 2 * max(run_count - 2, 0)`.

Two runs therefore occupy **9 payload bytes**.

### EF body and u64 directory

EF starts with cardinality minus one, base, and span as ULEB128-u32, followed by high words, packed low fields, and
sampled positions. Geometry is derived on attachment. There is no low-plane guard padding and the always-zero first ones
sample is omitted. Five-byte unaligned reads cover a low field without crossing the image extent.

The u64 body stores forest count minus one, ascending high32 keys, interior u64 cardinality prefixes, interior u32 child
offsets, and one root bit per child. Children are the exact u32 forest/EF **bodies**, with no repeated identification or
length prefix. The first prefix/offset is zero; the last prefix is derived from checked child cardinalities. Empty
children and overflowing totals are rejected.

### Framing and validation

`IMAGE_ID_LEN`, `EMPTY_U32_IMAGE`, and `EMPTY_U64_IMAGE` are public constants. `image_header(bytes)` returns
`ImageHeader { width, encoded_len, .. }` in bounded constant work. It validates identification, canonical framing, and
available extent, **not body topology**. Use it to find an embedded image's boundary, then attach the view of the
expected `KeyWidth`.

Both views expose `serialized_len()` separately from cardinality `len()`. Capacity after the encoded extent is allowed.
`open` checks directory geometry, legal kinds, adjacent payload extents, canonical counts, and Runs records and
prefixes. `open_verified` additionally verifies Array/Cone ordering, Words metadata, and EF samples/member content,
including u64 children. Neither attachment allocates or expands the bitmap.

All mapped reads are byte-backed. Words and balanced Array intersection retain their NEON byte-load kernels at odd
offsets; unsupported targets use the portable little-endian scalar implementation rather than a realignment copy.

Concrete native sizes covered by the wire oracle:

| Set                                 | Complete image |
| ----------------------------------- | -------------: |
| Empty u32 or u64                    |        4 bytes |
| u32 singleton                       |       12 bytes |
| u32 interval `0..=99`               |       14 bytes |
| u32 `0..=4095` plus `32768..=36863` |       20 bytes |
| u64 singleton                       |       18 bytes |

## Fixed-capacity mutation

Initialize a slot with an actual native image, including the four-byte empty image. No outer length word or fixed header
reserve is needed.

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
allocations. No speed ratio from an earlier layout is a claim about native v1. Compare interleaved controls on the same
binary and machine before drawing performance conclusions.

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
