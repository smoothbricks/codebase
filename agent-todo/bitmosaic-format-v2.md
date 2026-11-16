# bitmosaic: implement format 2

`packages/bitmosaic/crates/bitmosaic`. The format is specified:
[specs/bitmosaic/01-format.md](../specs/bitmosaic/01-format.md). Read it whole before designing, then the laws in
`specs/bitmosaic/laws/` (each names the shortcut it licenses) and the evidence in
[specs/bitmosaic/prototypes/format-2/README.md](../specs/bitmosaic/prototypes/format-2/README.md). Where this brief and
the spec differ, the spec is right. It supersedes `agent-todo/bitmosaic-tiny-sets.md`, whose measured defect it closes.
Nothing is in work on it since the Bitmap root landed (2026-11-15): every step below but the Bitmap's part is open.

## Delivered: the Bitmap root

The crate reads and writes format 2's Bitmap root (spec §4) and nothing else of format 2: `src/bitmap.rs` (`BitmapView`
with `open`, `open_admitted`, `extent`, `bytes`, `len`, `is_empty`, `word`, `contains`, `rank`, `iter`; the writer
`bitmap_len`, `bitmap_set`, `bitmap_seal`; `Image::open`, which tells the format by the first byte, admits format 1 as
before and refuses every other format-2 root by name). `tests/bitmap_root.rs` holds it to the corpus's Bitmap rows and
refusals and to the laws `one_image.bend` (a trimmed bitmap is the written one) and `bitmap_probe.bend` (rank by whole
words and one masked word; AND word by word; the `k`-th member has `k` below it). Its first caller is a storing format
whose field is always a Bitmap (§7.3).

So of the order of work below, step 1 exists for the Bitmap alone and step 2's Bitmap half is done. What remains: Single
and List; the lean Elias-Fano root; the sealed and forest rules (§7) and the writers that apply them; `select` and a
two-image `and_len` on the Bitmap (nothing calls them yet, and the wiring lint refuses an export with no caller); the
`u64` directory; patching; the law tests of the other forms; the ports. The uses waiting on those: a datom store's
set-typed values, n-gram and posting tails (List), a deduplication set's runs (Single). The rules that decide the
design, quoted from the spec:

- §1: "Format 2 is an additive version. An image of format 1 … begins with the bytes `42 4D 53`; no image of format 2
  begins with `42`. A reader of format 2 reads format 1 as well, and nothing written in format 1 is rewritten."
- §7.1: "The sealed image of a set is the shortest of its candidates; among candidates of the same length the earlier in
  this order" (Bitmap, Single or List, directory, Elias-Fano). "A writer or verifier computes lengths and writes one
  image; it never encodes a candidate to measure it."
- §10: "Canonical is per format." and "Format 2 is frozen."
- §12.2: "A reader never reads a byte past the image's extent."

## What exists to build from

1. **The crate on main** is format 1: the directory with its five containers, the Elias-Fano root, the `u64` directory,
   `open` / `open_verified`, the patch planner, `patched_len_bound`. Format 2's directory BODY is format 1's forest body
   byte for byte (spec §8), so the directory reader, its kernels and the patch planner carry over untouched. Do not give
   any function of the directory reader a second caller, add a variant to its root or cursor enums, or add a field to
   the view: each of those cost 2% to 47% on large sets when it was tried (below).
2. **An unlanded implementation of six small forms** (commits `0ed86b85b1` .. `65411f5e9d` on top of main `7e71e81a54`,
   in the shed `bitmosaic-small`, whose tip `722a1ee7ca` adds three law files for those forms). Its six forms were
   dropped by decision (2026-11-15): the case they were written for, the columns present in a stored row, is held as a
   Bitmap over the columns of the row's shape instead. Nothing of the shed is owed a land. The work below starts from
   it: the shed stays mounted for whoever takes this brief, who reads these parts there and removes it when step 4 is on
   main. Reusable from it:
   - the shape "a small image is a forest of no chunks whose answers are cold out-of-line functions over the image
     bytes", which keeps the directory loops as they are;
   - its Window reader (`small.rs`: `count_below`, `select_bit`, `select_in_word`, `window_and`, the clipped `word_at`):
     the Bitmap form is that reader with the byte count in the tag's low seven bits and no start;
   - its List readers' compare-all (`list_rank`, `list_holds`): a List reads the same after one subtraction of the first
     member, at field widths of one to four bytes;
   - `image_len` / `write_image_into` (the slice writer and its zero-allocation test), the three admissions with the
     class `canonical`, the tiny patch path, the oracle steps that reach small sets, the instruction harness
     (`examples/format_gate.rs`) and its measuring rules. Superseded by the spec: its tag space (`20..C0`), the forms
     List8, List16, Window at, Window wide and Spans, the restriction of small images to members below 65,536, its `u64`
     child rule (a child is now a whole sealed image), the `14` / `15` empty images (the empty set is `80`), its
     vectors, and its selection rule (six candidates and a ladder floor; now four candidates by closed formulas).

## Order of work

1. **Identification and format 1 beside format 2.** `image_header` and the three admissions take either format: first
   byte `42` is format 1 (the existing parser, `BMS` and all), anything else is §3. Views answer the same reads for
   both. Writers write format 2 only. A test opens every image of the existing format-1 oracle unchanged.
2. **Bitmap, Single, List**: readers in place (the direct entries of §4 and §5 included: for a Bitmap `bitmap_len(tag)`,
   `contains(bytes, i)`, `rank(bytes, i)` and `write_bitmap(out, positions)` over the bytes after the tag, which a kind
   that fixes the form calls with no dispatch), writers, admission with the classes of §11.
3. **Elias-Fano made lean** (§6): the root's geometry takes "planes in bytes, the first zero-sample not stored, no body
   length"; format 1's geometry stays for format-1 images. One reader, two geometries.
4. **The rule** (§7): `image_len` by closed formulas for candidates 1, 2 and 4, the directory priced only when no small
   candidate is at or under its floor; `to_bytes` (sealed), `to_forest_bytes` (forest), `write_image_into`.
5. **The `u64` directory** (§9) and §7.4 (a `u64` set below `2^32` is the `u32` image).
6. **Patch** (§12.3) and `patched_len_bound` over the new roots.
7. **Vectors and laws**: a test reads `specs/bitmosaic/vectors/format-2.json` (writes every sealed and forest image byte
   for byte, reads each back, refuses each reject with its class); one test per law of spec §13 over the crate's own
   bytes, each in a region that names the spec section; `image_len` printed over the universe of `laws/sizes.bend`
   equals its 36 numbers.
8. **Ports**: TypeScript and Go readers and writers of format 2 (verify, rank, select, count, end, size, write), held
   byte-identical by the same corpus. Per form a port needs: Bitmap, two popcount loops; List, a compare loop; lean
   Elias-Fano, the root's arithmetic it already has with the two geometry changes. No SIMD and no bit-deposit
   instruction is needed.
9. README: the format section becomes a pointer to the spec; the size table is spec §15.

## Gates

- **Sizes**: every row of spec §15 and every vector, exactly.
- **Large sets**: the 144-cell instruction gate of `format_gate` against main (shapes dense, sparse and runs at 10^4 and
  10^6; attach, verify, contains, rank, select, walk, cursor, seek, and_len, to_bytes, to_forest_bytes, patch; sealed
  and forest), no cell slower beyond noise. Measure each build as a standalone copy with its own version string, at the
  workspace profile (`lto = true`, `codegen-units = 1`), least of several runs. The cells that were still above main in
  the unlanded implementation, each to close or to name with its cause from the disassembly: `open_verified` 1.4 to 3.4
  times (the canonical check walks Array, Cone and Words chunks and ladders them: decide a chunk's container from its
  counts where the laws allow, as Runs, Stride and singletons already are); forest patch +3.4% to +5.6% (inside the
  planner; cause not named); Runs `select` +2%; about twenty cells at +0.5% to +1.5% (directory attach, forest walk,
  seek, Elias-Fano `and_len`).
- **Small sets**: Bitmap contains at most 30 instructions and rank at most 45 through the direct entry; a List's rank at
  most 50; writing a set of 4 and of 13 ordinals through `write_image_into` under 250 instructions with no allocation; a
  one-member patch of a Bitmap under 100.
- `bun nx run-many -t lint test -p bitmosaic columine` and `bun nx run @smoothbricks/codebase:lint` green; the fuzz
  targets 60 s each at the final tip.

## What each large regression was, so it is not made again

1. Directory attach +33% to +47%: functions of the directory reader that are inlined only because each has one call site
   gained a second caller.
2. Elias-Fano cursor +47%: any code added to the arms of the view cursor's `next` stops the compiler keeping the
   Elias-Fano cursor in registers.
3. Probe loops +1% to +7%: a probe loop over a view is compiled once per root; a third root, or a call on the miss path
   that is not cold, ends that.
4. Forest walk +8.5%: handing the caller's closure to a function that is not inlined makes its captures escape.

## Left open by the spec, for the implementer to measure

- Whether a verified view type that skips the bounds checks the laws discharge (§12.2) is worth its type.
- Blocks (Elias-Fano in blocks of 128 members): 3% to 12% smaller above 256 members, not faster. Not format 2; if a
  tuned reader and a block cursor reach the root's instruction counts it is a later format (§10), and its blocks are the
  rank ranges that block-max bounds need.
