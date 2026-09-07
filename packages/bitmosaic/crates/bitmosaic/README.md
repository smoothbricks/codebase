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

Runs and contiguous Stride updates merge intervals with batch events in one sweep. Retained interval scratch is bounded
by the chooser: at most 1,453 records before spilling into the existing dense plane. Sparse member scratch stays bounded
by Array eligibility. A Words in-place trial counts result runs with word masks/carry and proves that Words still wins
the common chooser.

The patch keeps its root, except that an EF result with fewer than two members falls back to forest/empty. Geometry or
varint-width changes assemble the new image in retained scratch before touching the slot. Stable geometry retains the
touched-payload/rigid-tail path with byte offsets. Forest results agree with fresh `to_forest_bytes()` output under the
same chooser.

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

Benchmarks use the bench profile and report workload-specific observations: attach separately from attached operations,
owned accounting separately from wire bytes, run-rich count/algebra cells, enumerable/restaged controls, and warm patch
allocations. No speed ratio from an earlier layout is a claim about native v1. Compare interleaved controls on the same
binary and machine before drawing performance conclusions.

Standard Roaring import/export is a possible later public-request interoperability feature. No Roaring codec or
production Roaring dependency is implemented.

## License and attribution

MIT OR Apache-2.0. The component was extracted from the minigraf fork; the original license notices and attribution are
retained in `LICENSE-MIT` and `LICENSE-APACHE`.
