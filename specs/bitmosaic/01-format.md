# bitmosaic format 2

## 1. Scope

A bitmosaic image is the byte form of one set of unsigned integers: `u32` members, or `u64` members through a directory
of `u32` images. This document is the whole of format 2: the bytes of every form, the rule that gives a set its one
image, what a reader may be handed and must refuse, and the laws the rule and the reads rest on. A port needs nothing
else.

Format 2 is an additive version. An image of format 1 (the format of the crate before this one) begins with the bytes
`42 4D 53`; no image of format 2 begins with `42`. A reader of format 2 reads format 1 as well (§10), and nothing
written in format 1 is rewritten.

The requirements it is held to:

- **Self-delimiting.** An image's length follows from its own bytes in bounded constant work. It is embedded with no
  outer length; bytes after it (a slot's spare capacity, the next field) are not part of it.
- **Read where it lies.** Every read is arithmetic over the image's bytes: no decode at open, no allocation, no
  alignment requirement. A view holds a pointer, a length and a few header fields.
- **One image per set.** The sealed image is a pure function of the set (§7), chosen by closed formulas, so images
  compare and hash as bytes.
- **Close to the raw structure.** A small dense set is a bare bitset behind one tag byte; a few scattered members are
  those members behind one tag byte.

## 2. Conventions

Multi-byte integers are little-endian. Bit `j` of a byte or word is the one worth `2^j`. A _varint_ is ULEB128: seven
bits a byte, low group first, the high bit set on every byte but the last, in its shortest form (an encoding of two or
more bytes whose last byte is zero is refused), at most five bytes for a `u32`. `V(x)` is the varint length of `x` (1
below 128, 2 below 16,384, 3 below 2,097,152, 4 below 268,435,456, else 5). `W(x)` is the bytes `x` needs, at least one
(1 below 256, 2 below 65,536, 3 below 16,777,216, else 4). There is no alignment padding anywhere in an image. Divisions
are integer divisions.

A set is written `{a, b, ..}`; `first` and `last` are its least and greatest member, `n` its cardinality.

## 3. The control byte

The first byte of an image is its control. It names the form and, for the three small forms, everything needed to find
the image's end.

| Control                             | Form            | Image                                                                           |
| ----------------------------------- | --------------- | ------------------------------------------------------------------------------- |
| `80` to `FF`                        | Bitmap          | control, then `control & 7F` bitmap bytes (§4)                                  |
| `20` to `23`                        | Single          | control, then the member in `(control & 3) + 1` bytes (§5)                      |
| `04` to `7F` with bits 2 to 4 not 0 | List            | control, the first member, the other members' distances (§5)                    |
| `02`                                | Elias-Fano      | control, three varints, the planes (§6)                                         |
| `01`                                | `u32` directory | control, `body_bytes` varint, body (§8)                                         |
| `03`                                | `u64` directory | control, body (§9)                                                              |
| `00`                                | refused         | an all-zero region is no image                                                  |
| `40`, `41`, `42`, `43`              | never format 2  | `42 4D 53` begins a format-1 image; the others are kept for later formats (§10) |
| `60` to `63`                        | unassigned      | refused                                                                         |

The low seven bits of a control below `80` are three fields, `dd kkk aa` (bits 5 to 6, 2 to 4, 0 to 1). `kkk = 0` is the
sixteen controls `00` to `03`, `20` to `23`, `40` to `43`, `60` to `63` of the table; every other value is a List.

The empty set is the Bitmap of no bytes: the single byte `80`, for `u32` and `u64` alike.

## 4. Bitmap

```text
[0]        control = 0x80 | n        n = bitmap bytes, 0..=127
[1..1+n)   the bitmap                bit j of byte i is the member 8 i + j
```

- The image is `1 + n` bytes: a bare little-endian bitset of `8 n` positions behind one byte. Loaded as 64-bit words,
  bit `v mod 64` of word `v div 64` is the member `v`.
- The last bitmap byte is not zero (trailing zero bytes are trimmed), so `n = last div 8 + 1` and the form holds members
  `0..=1015`.
- The cardinality is the population count of the `n` bytes.

A caller that knows the form reads it as a bitset with no dispatch: the direct entry takes the bytes after the control
and `n`.

## 5. Single and List

```text
Single   [0]  control = 0x20 | (wa - 1)
         [1..1+wa)                       the member

List     [0]  control = (wd - 1) << 5 | (k - 1) << 2 | (wa - 1)        k = members, 2..=8
         [1..1+wa)                       first, the least member
         then k - 1 fields of wd bytes   each other member's distance from first, ascending
```

- `wa = W(first)` and, in a List, `wd = W(last - first)`: the first member and the distances are each in the fewest
  bytes that hold them. So the top byte of the first member is not zero unless `wa = 1`, and the top byte of the last
  distance is not zero unless `wd = 1`.
- Distances are strictly ascending and at least 1; `first` plus the last distance is at most `2^32 - 1`.
- The image is `1 + wa + (k - 1) wd` bytes, from the control alone.

## 6. Elias-Fano

```text
[0]   control = 0x02
      n - 1     varint      n = members, at least 2
      first     varint      the least member
      span      varint      last - first;  n <= span + 1;  first + span <= 2^32 - 1
      high plane, low plane, one-samples, zero-samples
```

With `d = member - first` (so the first member has `d = 0` and the last `d = span`):

- `L`, the low width: 0 when `span <= n`; otherwise `floor(log2 span) - floor(log2 n)`, less one when that is not zero
  and `span >> L < n`. Then `span >> L < 2 n`.
- `buckets = (span >> L) + 1`.
- **High plane**: `ceil((buckets + n) / 8)` bytes. Member `k` (from 0) sets bit `(d >> L) + k`. Every other bit is zero,
  the bits past position `buckets + n - 1` included. The plane holds exactly `n` ones and `buckets` zeros within its
  `buckets + n` positions, and its last position is a one.
- **Low plane**: `ceil(n L / 8)` bytes. Member `k`'s field `d mod 2^L` is at bits `k L .. (k + 1) L`. Unused bits are
  zero.
- **One-samples**: `floor((n - 1) / 64)` values as `u32`. Entry `j` (from 1) is the high-plane position of member
  `64 j`.
- **Zero-samples**: `floor((buckets - 1) / 64)` values as `u32`. Entry `j` (from 1) is the high-plane position of zero
  number `64 j` (zeros counted from 0).

The image's length follows from `n` and `span`:
`1 + V(n - 1) + V(first) + V(span) + ceil((buckets + n) / 8) + ceil(n L / 8) + 4 floor((n - 1) / 64) + 4 floor((buckets - 1) / 64)`.

Members ascend strictly: the members of one bucket have strictly ascending low fields.

Against format 1's Elias-Fano body the differences are three, all in geometry: the high plane is whole bytes, not whole
64-bit words; the first zero-sample (zero number 0) is not stored; and there is no `body_bytes`.

## 7. The one image of a set

### 7.1 The sealed image

The sealed image of a set is the shortest of its candidates; among candidates of the same length the earlier in this
order:

| Order | Candidate    | Exists when                         | Length in bytes                          |
| ----- | ------------ | ----------------------------------- | ---------------------------------------- |
| 1     | Bitmap       | `last <= 1015`, or the set is empty | `2 + last div 8`; the empty set: 1       |
| 2     | Single, List | `1 <= n <= 8`                       | `1 + W(first) + (n - 1) W(last - first)` |
| 3     | Directory    | `n >= 1`                            | §8.4                                     |
| 4     | Elias-Fano   | `n >= 2`                            | §6                                       |

Every length is a formula of the set: of `n`, `first` and `last` for candidates 1, 2 and 4, and of each chunk's count,
runs and fit for the directory. A writer or verifier computes lengths and writes one image; it never encodes a candidate
to measure it.

Consequences worth naming: a Bitmap wins every tie; an Elias-Fano image is written only when it is strictly shorter than
the directory; `{7}` is `81 80` and `{8}` is `20 08`; an unbroken interval of 81 or more members from zero is a
directory of one Stride chunk (11 bytes), not a bitmap.

### 7.2 The forest image

The forest image of a set is chosen the same way without candidate 4. It is the image for a region that is patched in
place (§12): a patch of a Bitmap, a List or a directory touches the bytes of the members it changes, where an Elias-Fano
image is rewritten whole. The sealed and the forest image of a set are the same bytes unless the sealed image is
Elias-Fano.

Both are pure functions of the set. Which of the two a stored region holds is the storing format's rule; bytes that are
hashed or compared across writers are the sealed image.

### 7.3 A kind that fixes the form

A storing format may state that a field is always one named form of a bitmosaic image, whatever §7.1 would choose (a
frame of bits that is dense at its low positions by construction is always a Bitmap, so that its writer never chooses).
Such bytes are a well-formed image of that form and are admitted by `open` and by the form's direct entry. They are the
sealed image exactly when §7.1 picks that form; their canonical form is the storing format's rule, not this section's.

### 7.4 u64

A `u64` set whose members are all below `2^32` has the image of the `u32` set of the same members. Any other `u64` set
is a `u64` directory (§9). So a `u64` reader admits every control, and a `u32` reader refuses `03`.

### 7.5 Writing

One pass over the ascending members gives `n`, `first`, `last`, and for each chunk its count, its runs, whether it is an
arithmetic progression, and the Cone fit. Shortcuts the laws license (§13):

- A Bitmap of at most `n + 1` bytes (`last < 8 n`) is never beaten by a List: the List is not priced.
- A directory of one chunk is never shorter than `7 + min(11 + n, 2 n, 4 r)` bytes for `n` members in `r` runs (a single
  member is 9 bytes and an arithmetic progression 11, exactly). A small candidate at or under that floor is the image
  without the directory being priced, so no chunk is laddered.
- An Elias-Fano image is at least 5 bytes.

A writer that holds its members as an ascending slice writes the image into a caller's buffer with no allocation for a
Bitmap, a Single, a List or an Elias-Fano image (`image_len(members)`, `write_image_into(members, out)`).

## 8. The u32 directory

`01`, then `body_bytes` as a varint (the body's length; the whole image fits the `u32` offset domain), then the body.
The body is format 1's forest body, byte for byte.

### 8.1 Chunks

A member's high 16 bits are its chunk key and its low 16 bits its value in that chunk. A chunk holds 1 to 65,536 values,
ascending and distinct, in one container.

### 8.2 Body

In order:

1. Cardinality minus one, varint. Decoding uses `u64`, permitting cardinality `2^32`.
2. Chunk count `c` minus one, varint, at most 65,535.
3. `c` ascending `u16` keys.
4. `c - 1` exclusive cardinality prefixes as `u32`: the members before chunks `1..c`. The first is implicitly zero and
   the header cardinality closes the last chunk.
5. `c - 1` payload offsets as `u32`, for chunks `1..c`, relative to the payload area; the first is implicitly zero and
   the body's end closes the last payload.
6. `c` three-bit kinds, packed from bit 0 of the first byte upward, `ceil(3 c / 8)` bytes. Unused bits are zero. Each
   kind is 0 to 4.
7. The slot plane, present exactly when `c > 64` and the keys are not one unbroken run
   (`last key - first key != c - 1`): `2^s` slots of `u64` with `s = log2(next_power_of_two(floor(8 c / 7) + 1))`. Chunk
   `i` with key `k` is stored as `k << 32 | (i + 1)` in the first empty slot at or after
   `(k * 0x9E3779B97F4A7C15 mod 2^64) >> (64 - s)`, probing upward and wrapping, chunks taken in key order. Every other
   slot is zero.
8. The container payloads, adjacent, in key order.

### 8.3 Containers

| Kind | Container | Payload                                                                   | Payload bytes              |
| ---- | --------- | ------------------------------------------------------------------------- | -------------------------- |
| 0    | Words     | summary, window, bounds, bitmap words, rank directory                     | `24 + 8 w + 2 (w / 4 - 1)` |
| 1    | Stride    | `first: u16, step: u16`; member `i` is `first + i * step`                 | 4                          |
| 2    | Cone      | `first: u16, eps: u8, scale: u64`, then one signed residual byte a member | `11 + n`                   |
| 3    | Array     | the members as `u16`                                                      | `2 n`                      |
| 4    | Runs      | run count, `(start: u16, end: u16)` records, interior prefixes            | below                      |

Kinds 5 to 7 are refused. `n` is the chunk's cardinality; it is never stored in a payload: the prefix plane gives it.

- **Stride.** `step >= 1` and `first + (n - 1) * step <= 65535`.
- **Cone.** Member `i` is `first + floor(i * scale / 2^32) + residual_i`, the residual a signed byte. `eps` is the
  largest absolute residual. `n * scale` fits a `u64`. Members ascend strictly and stay within `0..=65535`.
- **Array.** Strictly ascending.
- **Runs.** A varint `r - 1` (`r` runs, at most 32,768), then `r` records of inclusive `start` and `end`, then one `u16`
  for each interior run `1..=r-2`: the members before it. Runs ascend, and no two overlap or touch
  (`end_i + 1 < start_{i+1}`). The payload is `V(r - 1) + 4 r + 2 max(r - 2, 0)` bytes.
- **Words.** At offset 0 two `u64` summary words: bit `b` (of 128) is set exactly when a member lies in positions
  `512 b .. 512 b + 511`. At 16 `base_word: u16` and at 18 `w: u16`: the window of 64-bit words the bitmap holds, both
  multiples of 8, `w >= 8`, `base_word + w <= 1024`, the smallest such window that holds every member. At 20 the
  smallest member and at 22 the largest, as `u16`. At 24 the `w` words: bit `j` of word `i` is the value
  `(base_word + i) * 64 + j`. Then `w / 4 - 1` rank entries as `u16`: entry `g` (from 1) is the number of members in
  window words `0 .. 4 g`.

### 8.4 The container of a chunk, and the directory's length

For a chunk of `n` values in `r` maximal runs:

- `n = 1`: Array.
- all gaps equal: Stride.
- `n <= 4096`: the cheapest of Cone at `16 + n` (only when `n <= 1024` and, with `scale = floor(span * 2^32 / (n - 1))`
  over `span = last - first`, every residual is within `-63..=63`), Array at `2 n`, and Runs at `6 r`. A tie goes to
  Cone, then Array, then Runs.
- `n > 4096`: Runs when `6 r < 8720`, otherwise Words.

These prices are format 1's and select the container; they are not payload lengths. The directory's length is
`1 + V(body_bytes) + body_bytes`, with `body_bytes` the sum of §8.2's parts over the chunks' payload lengths of §8.3.

## 9. The u64 directory

A `u64` member's high 32 bits are its key; a child is the set of the low 32 bits of the members that share a key.

```text
[0]   control = 0x03
      m - 1                 varint     m = children, at least 1
      m keys                u32 each   ascending
      max(m - 2, 0) prefixes  u64 each   the members before children 1 .. m - 2
      m - 1 offsets         u32 each   where children 1 .. m - 1 start, relative to the child area
      the children, adjacent, in key order
```

- A child is the sealed `u32` image of its low halves, whole, as §7.1 gives it. It is never empty.
- The members before child 0 are none; before the last child they are the last stored prefix (or zero) plus the
  cardinality of the child before it, which that child's own bytes give.
- The image ends where its last child ends; the last child is self-delimiting.
- A directory of one child at key zero is refused: that set's image is the child (§7.4).

## 10. Format 1, and what a later format may do

| First bytes                                               | Format | Image                                    |
| --------------------------------------------------------- | ------ | ---------------------------------------- |
| `42 4D 53`, then a control `08`, `09`, `0A`, `0C` or `0D` | 1      | the control's low bits are `(empty << 2) | (ef << 1) | width`; an empty image is these four bytes; any other is followed by `body_bytes` and the body |
| anything else of §3                                       | 2      | this document                            |

- A format-1 `u32` forest body is §8's body. A format-1 Elias-Fano body is §6's with three differences: the high plane
  is `ceil((buckets + n) / 64)` whole `u64` words; there are `ceil(buckets / 64)` zero-samples, entry `j` from 0; and
  the body is preceded by `body_bytes`. A format-1 `u64` body is `m - 1`, the keys, the prefixes, the offsets, then one
  root bit per child in `ceil(m / 8)` bytes (0 forest, 1 Elias-Fano), then the children as bare bodies.
- A reader of format 2 admits both formats through the same entry; a view answers the same reads for either.
- **Canonical is per format.** The sealed image of §7 is the canonical image of a set under format 2. A format-1 image
  is canonical under format 1's rule and is not a format-2 sealed image of any set. A storing format names the bitmosaic
  format its digests and equality are over; a reader never rewrites an image into another format.
- **Format 2 is frozen.** Its controls, its forms and §7's rule do not change: an image written under it stays readable
  and stays the sealed image of its set. A later format begins with `40`, `41` or `43` followed by its own image and is
  specified beside this document; it costs format 2's images nothing.

## 11. Admission

Three entries, by what the caller needs. None allocates.

| Entry            | Proves                                                                                                                                                                                                                                                 | For                                                                                      |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------- |
| `open`           | structure: the control, the image's extent, every count, width and offset, directory geometry and kinds, payload extents, and the one-spelling rules that cost no member pass (a Bitmap's last byte, a field's top byte, a List's ascending distances) | a hot attach over bytes the caller wrote or already admitted; a kind that fixes the form |
| `open_verified`  | `open`, every member and every derived plane (Array, Cone and Elias-Fano order; Words bounds, summary and rank entries; samples), and that the image is the sealed or the forest image of its set                                                      | bytes from outside; a region that is patched                                             |
| `open_canonical` | `open_verified`, and that the image is the sealed image                                                                                                                                                                                                | bytes that are hashed or compared                                                        |

Each form also has a direct entry that admits that form alone at `open`'s level and returns a reader with no dispatch
(for a Bitmap: the control's high bit, `1 + n` bytes present, the last byte not zero).

`image_header(bytes)` returns the image's format, form, key width and extent in bounded constant work: from the control
alone for a Bitmap, a Single and a List; from three varints for Elias-Fano; from `body_bytes` for a `u32` directory;
from the last offset and the last child's header for a `u64` directory.

A refusal carries its class, the image-relative byte offset and a static cause. A child refusal in a `u64` directory is
shifted to the complete image.

| Class         | Refuses                                                                                                                                                                                                                                                                                                                                                                                        |
| ------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `extent`      | a missing control; an image, plane, field or payload that is truncated or does not cover its extent exactly; a non-shortest `body_bytes`; a Bitmap whose last byte is zero; a Single or List field in more bytes than its value needs                                                                                                                                                          |
| `kind`        | the control `00`; an unassigned control; a control of §10's other formats where format 2 is required; a `u64` directory handed to a `u32` reader; a directory chunk of kind 5 to 7; a `u64` child that is not a nonempty `u32` image                                                                                                                                                           |
| `count`       | a non-shortest varint count; an Elias-Fano image of fewer than two members or of more members than `span + 1`; a high plane whose ones are not `n` or whose zeros are not `buckets`; a chunk cardinality outside `1..=65536` or outside its kind's range; prefixes that do not ascend or disagree with their payloads (Runs, Words, `u64` children); a Words window that is not whole blocks   |
| `map-order`   | chunk keys, `u64` keys, List distances, Array or Cone members, runs or Elias-Fano members that do not ascend strictly; runs that overlap or touch                                                                                                                                                                                                                                              |
| `plane-range` | a value outside its domain: a member past `2^32 - 1`, a zero Stride step, a chunk value past 65,535, a Cone model that overflows or whose window misses a member, Words bounds, summary or rank entries that disagree with the words, an Elias-Fano last member that is not `first + span`                                                                                                     |
| `bit-pad`     | a nonzero unused bit in the kind plane or in an Elias-Fano plane                                                                                                                                                                                                                                                                                                                               |
| `placement`   | payload or child offsets that are not adjacent within their area; a slot-plane entry or a sample that names the wrong position                                                                                                                                                                                                                                                                 |
| `canonical`   | a well-formed image that is not the writer's for its set: a chunk whose members select another container (§8.4); a Cone whose `scale` or `eps` is not §8.4's; an image that is neither the sealed nor the forest image of its set; under `open_canonical`, a forest image that is not the sealed one; a `u64` directory of one child at key zero, or with a child that is not its sealed image |

Format 1's class `magic` (a first byte that is not `42`) is, under a reader of both formats, an unassigned control:
`kind`.

## 12. Reads, the readable extent, and patching

### 12.1 Reads

Every image answers, without allocating: `len()` (the cardinality), `contains(v)`, `rank(v)` (members strictly below
`v`), `select(k)` (the `k`-th member ascending, from zero), the ascending walk and the pull cursor with `seek(v)` (the
least member at or above `v`), `serialized_len()` (the image's extent), `and_len(other)` (the cardinality of the
intersection) and `and_rank(other, k)` (members of the intersection below `k`).

| Form         | `contains(v)`                                                                                                                                                                                                                              | `rank(v)`                                                                                                        | `select(k)`                                                                 | `len`                             |
| ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------- | --------------------------------- |
| Bitmap       | `v < 8 n` and bit `v mod 8` of byte `v div 8`                                                                                                                                                                                              | population count of the whole words below `v`, plus one word masked to the bits below `v`; `len` when `v >= 8 n` | skip whole words by their counts, then the `k`-th set bit of one word       | population count of the `n` bytes |
| Single, List | `v = first`, or some distance equals `v - first` (the OR of every field's equality)                                                                                                                                                        | 0 when `v <= first`; else 1 plus the distances below `v - first` (every field compared, no branch on a member)   | `first`, or `first` plus field `k - 1`                                      | from the control                  |
| Elias-Fano   | the bucket `h = d >> L` starts at high-plane position `s` (0 for `h = 0`, else one past zero number `h - 1`); its members are the ones from `s` to the next zero, member `s - h` first; `d`'s low bits are searched among their low fields | `s - h` plus the bucket's members whose low field is below `d`'s                                                 | the one number `k` is at position `p`; the member is `first + ((p - k) << L | low_k)`                           | from the header |
| Directory    | the chunk by key (binary search, or the slot plane), then the container                                                                                                                                                                    | the chunk's prefix plus the container's rank                                                                     | the chunk by prefix, then the container's select                            | from the header                   |

`seek(v)` on any form is `select(rank(v))`, absent when the rank is the cardinality. `and_len` of two Bitmaps is the
population count of the byte-wise AND over the shorter; of a Single or List with any image, the count of its members the
other contains; of any two others, a leapfrog of seeks or the directory's chunk-wise kernels.

The operations need only shifts, AND, population count and count-trailing-zeros; a select in a word may be six halvings
by population count, so no port needs a bit-deposit instruction.

### 12.2 The readable extent

A reader never reads a byte past the image's extent. A word load that would cross the end is clipped (two overlapping
narrower loads, or a copy of the remaining bytes into a zeroed word). A container that guarantees eight readable bytes
after every image it holds may tell its reader so and take whole-word loads; that is the container's contract, never an
assumption of the format.

A view from `open_verified` or `open_canonical` may skip the per-read bounds checks the laws discharge (§13); a view
from `open` keeps them.

### 12.3 Patching a region

A region holds an image at its start and zero bytes after it. A patch adds and removes members and reports, per value,
whether an add was new and a remove was held. After a patch the region holds the forest image (§7.2) of the new set,
zero bytes after it.

- A Bitmap that stays a Bitmap is patched by setting and clearing bits and growing or trimming `n`.
- A List that stays a List is rewritten in its at most 33 bytes.
- A directory is patched chunk by chunk: only the chunks a batch touches are re-laddered, and the bytes after them move
  once.
- A patch that changes the form writes the new image from the merged members.

`patched_len_bound(image, adds, touches)` is an upper bound on the forest image's length after `adds` new members
landing in `touches` distinct chunks, in bounded constant work: the directory of the patched set bounds every forest
image, so the bound is format 1's directory bound over the image's member and chunk counts. The member count is in the
header of every form but a Bitmap, whose population count is at most 16 words.

## 13. Laws

Each law is proven in Bend2 (`bend2 <file> --verdict`: ALL PROOFS CHECK) over `Nat`s and bit lists and licenses one
shortcut of this document. An implementation binds each to a test of the same statement over its own bytes.

| File                                                                                                         | Law                                                                                                                                                                | Licenses                                                                                                                                                                      |
| ------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `laws/one_image.bend`                                                                                        | `a_bitmap_reads_back_as_its_set`, `an_admitted_bitmap_is_the_written_one`                                                                                          | a trimmed bitmap is the only bitmap of its set: Bitmap images compare and hash as bytes                                                                                       |
|                                                                                                              | `a_list_reads_back_as_its_members`, `distances_rank_like_members`                                                                                                  | a List is read on its stored distances after one subtraction; no member is rebuilt                                                                                            |
|                                                                                                              | `the_pick_is_no_longer_than_any`, `the_first_wins_its_ties`, `a_longer_first_loses`                                                                                | §7.1 has one answer, a function of the lengths: no candidate is encoded to try it                                                                                             |
|                                                                                                              | `a_list_is_longer_than_its_count`, `a_bitmap_within_its_count_beats_a_list`                                                                                        | §7.5: a Bitmap of at most `n + 1` bytes is taken without pricing the List                                                                                                     |
| `laws/form_choice.bend`                                                                                      | `a_cone_directory_is_not_under_the_floor`, `an_array_directory_is_not_under_the_floor`, `a_runs_directory_is_not_under_the_floor`, `ordinals_never_ask_the_ladder` | §7.5: a small candidate at or under the directory's floor is taken without laddering a chunk                                                                                  |
| `laws/bitmap_probe.bend`                                                                                     | `whole_words_count_whole`, `a_mask_counts_inside_its_word`                                                                                                         | Bitmap rank: whole words by population count, one masked word, no bit loop                                                                                                    |
|                                                                                                              | `select_skips_whole_words`, `select_stays_in_its_word`, `rank_of_select`, `select_is_a_member`                                                                     | Bitmap select, and any select-in-word routine held to the pair                                                                                                                |
|                                                                                                              | `and_holds_what_both_hold`, `and_goes_word_by_word`, `a_later_start_shifts_positions`                                                                              | `and_len` of two Bitmaps is the population count of the word-wise AND                                                                                                         |
| `laws/list_probe.bend`                                                                                       | `count_below_is_rank`, `or_of_equals_is_contains`                                                                                                                  | List rank and contains compare every field and never search                                                                                                                   |
| `laws/elias_fano.bend`                                                                                       | `the_high_plane_reads_back_as_its_buckets`, `a_value_is_its_low_bits_then_its_high_bits`                                                                           | the Elias-Fano round trip: the planes determine the set                                                                                                                       |
|                                                                                                              | `ones_and_zeros_fill_a_prefix`, `the_zeros_before_a_member_are_its_bucket`                                                                                         | rank is `s - h` plus a bucket's scan; select is `(p - k) << L                                                                                                                 | low_k`; no walk over earlier members |
|                                                                                                              | `a_zero_select_stays_in_the_plane`, `a_one_select_stays_in_the_plane`, `a_field_ends_inside_its_plane`                                                             | on a verified image no select, bucket scan or field read leaves its plane (§12.2), for Elias-Fano and for a List's fields                                                     |
| `laws/seek.bend`                                                                                             | `the_member_at_the_rank_is_not_below`, `members_before_the_rank_are_below`, `a_rank_is_inside_or_at_the_end`                                                       | seek is `select(rank(v))` on every form: leapfrog intersection needs no decode                                                                                                |
| `laws/sizes.bend` (evaluated, not proven; `prototypes/format-2/sizes_check.py` is its second implementation) | the totals over 4,095 subsets at seven placements; the bound `span >> L < 2 n` over 258,048 pairs                                                                  | §6's and §7.1's formulas have three implementations that agree (Bend2, the Python model, an implementation's `image_len`); a bucket scan is bounded by the plane's `3 n` bits |

Where a law is over bit lists and not 64-bit words (a select in a word, a clipped load), the word-level routine is held
to the bit-list statement by its test; Bend2 has no 64-bit word to state it over.

## 14. Vectors

The conformance corpus is `specs/bitmosaic/vectors/format-2.json`: every set below with its members and its sealed and
forest images whole, the `u64` cases, and the refusals with their class. An implementation writes every sealed and
forest image byte for byte, reads every one back as its members, and refuses every reject with the class named. The
tables show the short ones.

Sets and their images (`u32`):

| Set                                                                                                 | Form       | Sealed image                                                                              | Bytes | Forest image, where it differs                                               |
| --------------------------------------------------------------------------------------------------- | ---------- | ----------------------------------------------------------------------------------------- | ----: | ---------------------------------------------------------------------------- |
| the empty set                                                                                       | Bitmap     | `80`                                                                                      |     1 |                                                                              |
| `{0}`                                                                                               | Bitmap     | `81 01`                                                                                   |     2 |                                                                              |
| `{7}`: a tie of two bytes goes to the Bitmap                                                        | Bitmap     | `81 80`                                                                                   |     2 |                                                                              |
| `{0, 1, 2, 10}`                                                                                     | Bitmap     | `82 07 04`                                                                                |     3 |                                                                              |
| `{0, 1, 2, 9}`                                                                                      | Bitmap     | `82 07 02`                                                                                |     3 |                                                                              |
| `0..=12` and 70                                                                                     | Bitmap     | `89 FF 1F 00 00 00 00 00 00 40`                                                           |    10 |                                                                              |
| `0..=82`                                                                                            | Directory  | `01 09 52 00 00 00 01 00 00 01 00`                                                        |    11 |                                                                              |
| `{8}`: the Bitmap is three bytes, the Single two                                                    | Single     | `20 08`                                                                                   |     2 |                                                                              |
| `{255}`                                                                                             | Single     | `20 FF`                                                                                   |     2 |                                                                              |
| `{256}`                                                                                             | Single     | `21 00 01`                                                                                |     3 |                                                                              |
| `{65535}`                                                                                           | Single     | `21 FF FF`                                                                                |     3 |                                                                              |
| `{65536}`                                                                                           | Single     | `22 00 00 01`                                                                             |     4 |                                                                              |
| `{1000000}`                                                                                         | Single     | `22 40 42 0F`                                                                             |     4 |                                                                              |
| `{4294967295}`                                                                                      | Single     | `23 FF FF FF FF`                                                                          |     5 |                                                                              |
| `{7, 200}`                                                                                          | List       | `04 07 C1`                                                                                |     3 |                                                                              |
| `{1000, 2000}`                                                                                      | List       | `25 E8 03 E8 03`                                                                          |     5 |                                                                              |
| `{1000000, 1000003}`                                                                                | List       | `06 40 42 0F 03`                                                                          |     5 |                                                                              |
| `{5, 70000}`                                                                                        | List       | `44 05 6B 11 01`                                                                          |     5 |                                                                              |
| `{5, 70000, 140000}`                                                                                | List       | `48 05 6B 11 01 DB 22 02`                                                                 |     8 |                                                                              |
| `{0, 4294967295}`                                                                                   | List       | `64 00 FF FF FF FF`                                                                       |     6 |                                                                              |
| eight members: `{300, 301, 310, 400, 401, 450, 500, 555}`                                           | List       | `1D 2C 01 01 0A 64 65 96 C8 FF`                                                           |    10 |                                                                              |
| nine members below 1,016: `{100, 200, ..., 900}` (a Stride directory)                               | Directory  | `01 09 08 00 00 00 01 64 00 64 00`                                                        |    11 |                                                                              |
| nine members: `{1000, 1001, 1010, 1100, 1101, 1150, 1200, 1255, 1300}`                              | Elias-Fano | `02 08 E8 07 AC 02 C7 52 02 20 28 52 2C FA 0C`                                            |    15 | `01 17 08 00 00 00 03 E8 03 E9 03 F2 03 4C 04 4D 04 7E 04 B0 04 E7 04 14 05` |
| nine scattered: `{3, 70000, 140001, 210000, 280007, 350000, 420002, 490000, 560009}`                | Elias-Fano | `02 08 03 86 97 22 49 92 24 02 00 80 B6 88 B7 A8 89 46 5C 6C B9 7E A2 1B F4 86 0B`        |    27 | `01 6A 08 08 00 00 01 00 02 00 03 00 04 00 05 00 ... (108 bytes, Directory)` |
| `1000..=1007` (an interval: a Stride directory against a List)                                      | Elias-Fano | `02 07 E8 07 07 55 55`                                                                    |     7 | `1D E8 03 01 02 03 04 05 06 07`                                              |
| `1000..=1099` (an interval: a Stride directory)                                                     | Directory  | `01 09 63 00 00 00 01 E8 03 01 00`                                                        |    11 |                                                                              |
| `{1015}` and `0..=7`: the Bitmap at its last position                                               | Directory  | `01 0E 08 00 00 00 04 01 00 00 07 00 F7 03 F7 03`                                         |    16 |                                                                              |
| `0..=7` and 1016: past the Bitmap                                                                   | Directory  | `01 0E 08 00 00 00 04 01 00 00 07 00 F8 03 F8 03`                                         |    16 |                                                                              |
| `0..=99` and `70000..=70099` (two Stride chunks)                                                    | Directory  | `01 18 C7 01 01 00 00 01 00 64 00 00 00 04 00 00 00 09 00 00 01 00 70 11 01 00`           |    26 |                                                                              |
| 66 members: `1000 i + (i * i mod 7)` for `i` in 0..=64, and 70000 (two chunks: a Cone and an Array) | Directory  | `01 5D 41 01 00 00 01 00 41 00 00 00 4C 00 00 00 1A 00 00 04 00 00 00 04 ... (95 bytes)`  |    95 |                                                                              |
| 200 members: `5000 i + (i * i mod 11)` for `i` in 0..=199 (three one-samples, three zero-samples)   | Elias-Fano | `02 C7 01 00 B9 DD 3C 55 A9 4A 95 AA 54 A9 4A 95 AA 54 A9 4A 95 AA 54 A9 ... (387 bytes)` |   387 | `01 B1 04 C7 01 0F 00 00 01 00 02 00 03 00 04 00 ... (564 bytes, Directory)` |

`u64` sets:

| u64 set                                   | Image                                                                                                         | Bytes |
| ----------------------------------------- | ------------------------------------------------------------------------------------------------------------- | ----: |
| `{5}`                                     | `81 20`                                                                                                       |     2 |
| `{4294967296}`                            | `03 00 01 00 00 00 81 01`                                                                                     |     8 |
| `{5, 4294967303}`                         | `03 01 00 00 00 00 01 00 00 00 02 00 00 00 81 20 81 80`                                                       |    18 |
| `{5, 4294967303, 8589934592, 8589934593}` | `03 02 00 00 00 00 01 00 00 00 02 00 00 00 01 00 00 00 00 00 00 00 02 00 00 00 04 00 00 00 81 20 81 80 81 03` |    36 |

Refusals:

| Bytes                                                   | Class         | Refused by        | Why                                                                        |
| ------------------------------------------------------- | ------------- | ----------------- | -------------------------------------------------------------------------- |
| `00`                                                    | `kind`        | open              | the control `00` is no image (an all-zero slot)                            |
| `42 4D 53 0C`                                           | `kind`        | open, as format 2 | a format-1 image; a reader of both formats admits it as format 1           |
| `60 05`                                                 | `kind`        | open              | an unassigned control                                                      |
| `81`                                                    | `extent`      | open              | a Bitmap of one byte with no byte                                          |
| `81 00`                                                 | `extent`      | open              | a Bitmap whose last byte is zero                                           |
| `82 01 00`                                              | `extent`      | open              | a Bitmap whose last byte is zero                                           |
| `21 07`                                                 | `extent`      | open              | a Single of two bytes with one present                                     |
| `21 07 00`                                              | `extent`      | open              | a Single in more bytes than its member needs                               |
| `24 07 C1 00`                                           | `extent`      | open              | a List whose distances are in more bytes than the last one needs           |
| `08 07 05 05`                                           | `map-order`   | open              | a List with a member twice                                                 |
| `08 07 05 03`                                           | `map-order`   | open              | a List whose distances do not ascend                                       |
| `07 FF FF FF FF 01`                                     | `plane-range` | open              | a List whose last member passes 4,294,967,295                              |
| `04 07 01`                                              | `canonical`   | open_verified     | `{7, 8}` as a List: its Bitmap `82 80 01` is no longer                     |
| `20 05`                                                 | `canonical`   | open_verified     | `{5}` as a Single: its Bitmap `81 20` is no longer                         |
| `02 00 05 00 01`                                        | `count`       | open              | an Elias-Fano image of one member                                          |
| `02 01 05 00 03`                                        | `count`       | open              | an Elias-Fano image of two members in a span of zero                       |
| `02 02 05 DB C5 08 49 00 80 B5 C8 B6`                   | `extent`      | open              | an Elias-Fano image cut short of its low plane                             |
| `02 02 05 DB C5 08 C9 00 80 B5 C8 B6 08`                | `count`       | open_verified     | a high plane with four ones for three members                              |
| `02 02 05 DB C5 08 49 00 80 B5 C8 B6 00`                | `plane-range` | open_verified     | a last member that is not `first + span`                                   |
| `02 01 00 01 05`                                        | `canonical`   | open_verified     | `{0, 1}` as Elias-Fano: its Bitmap `81 03` is shorter                      |
| `01 07 00 00 00 00 03 05 00`                            | `canonical`   | open_verified     | `{5}` as a directory of one Array chunk: its Bitmap `81 20` is shorter     |
| `03 00 00 00 00 00 81 20`                               | `canonical`   | open_verified     | a u64 directory of one child at key zero: the set is the u32 image `81 20` |
| `03 01 01 00 00 00 01 00 00 00 02 00 00 00 81 20 81 80` | `map-order`   | open              | a u64 directory whose keys do not ascend                                   |

## 15. Sizes

| Set                                                 |                                                                                                                       Sealed image |
| --------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------: |
| the empty set, `u32` or `u64`                       |                                                                                                                             1 byte |
| one member below 8                                  |                                                                                                                            2 bytes |
| one member below 256 / 65,536 / 16,777,216 / `2^32` |                                                                                                                2 / 3 / 4 / 5 bytes |
| a set below 1,016                                   |                                                                                                           at most `2 + last div 8` |
| two members                                         |                                                                                         at most 9 bytes; 3 when both are below 256 |
| `k <= 8` members                                    |                                                                                   at most `1 + W(first) + (k - 1) W(last - first)` |
| `n >= 2` members anywhere                           | at most `1 + V(n - 1) + V(first) + V(span) + ceil(3 n / 8) + ceil(n L / 8)` plus 4 bytes for every 64 members and every 64 buckets |
| an arithmetic progression within one chunk          |                                                                                                                           11 bytes |
