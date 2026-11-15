"""A reference writer and reader of bitmosaic format 2, from specs/bitmosaic/01-format.md alone.

Bitmap, Single, List and Elias-Fano are written and read here. A directory's bytes come from the crate on main
(format 1's forest image with its four identification bytes replaced by the control `01`): the directory body is
the same in both formats. `python3 ref.py vectors` prints the spec's vector tables, `python3 ref.py corpus` the conformance corpus
(specs/bitmosaic/vectors/format-2.json); `python3 ref.py check` reads
back every image it writes over random and exhaustive small sets.
"""
import os
import random
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
V1 = os.environ.get('BITMOSAIC_CRATE_LEN', HERE + '/proto/target/release/crate_len')


def varint(x):
    out = bytearray()
    while x >= 128:
        out.append(x & 0x7F | 0x80)
        x >>= 7
    out.append(x)
    return bytes(out)


def read_varint(b, at):
    x = shift = 0
    while True:
        byte = b[at]
        at += 1
        x |= (byte & 0x7F) << shift
        if byte < 0x80:
            return x, at
        shift += 7


def width(x):
    return max(1, (x.bit_length() + 7) // 8)


def low_width(n, span):
    if span <= n:
        return 0
    low = span.bit_length() - n.bit_length()
    return low - 1 if low and (span >> low) < n else low


# --- lengths, closed forms -------------------------------------------------------------------
def bitmap_len(s):
    if not s:
        return 1
    return 2 + s[-1] // 8 if s[-1] <= 1015 else None


def list_len(s):
    k = len(s)
    return 1 + width(s[0]) + (k - 1) * width(s[-1] - s[0]) if 1 <= k <= 8 else None


def elias_fano_len(s):
    n = len(s)
    if n < 2:
        return None
    span = s[-1] - s[0]
    low = low_width(n, span)
    buckets = (span >> low) + 1
    return (1 + len(varint(n - 1)) + len(varint(s[0])) + len(varint(span)) + (buckets + n + 7) // 8
            + (n * low + 7) // 8 + 4 * ((n - 1) // 64) + 4 * ((buckets - 1) // 64))


# --- writers ---------------------------------------------------------------------------------
def write_bitmap(s):
    n = s[-1] // 8 + 1 if s else 0
    out = bytearray([0x80 | n]) + bytearray(n)
    for v in s:
        out[1 + v // 8] |= 1 << (v % 8)
    return bytes(out)


def write_list(s):
    k, wa, wd = len(s), width(s[0]), width(s[-1] - s[0])
    tag = 0x20 | (wa - 1) if k == 1 else (wd - 1) << 5 | (k - 1) << 2 | (wa - 1)
    out = bytearray([tag]) + s[0].to_bytes(wa, 'little')
    for v in s[1:]:
        out += (v - s[0]).to_bytes(wd, 'little')
    return bytes(out)


def write_elias_fano(s):
    n, first, span = len(s), s[0], s[-1] - s[0]
    low = low_width(n, span)
    buckets = (span >> low) + 1
    high = bytearray((buckets + n + 7) // 8)
    lows = bytearray((n * low + 7) // 8)
    ones = []
    for k, v in enumerate(s):
        d = v - first
        p = (d >> low) + k
        high[p // 8] |= 1 << (p % 8)
        if k and k % 64 == 0:
            ones.append(p)
        x = (d & ((1 << low) - 1)) << (k * low % 8)
        at = k * low // 8
        while x:
            lows[at] |= x & 0xFF
            x >>= 8
            at += 1
    zeros, seen = [], 0
    for p in range(buckets + n):
        if not (high[p // 8] >> (p % 8)) & 1:
            if seen and seen % 64 == 0:
                zeros.append(p)
            seen += 1
    out = bytearray([0x02]) + varint(n - 1) + varint(first) + varint(span) + high + lows
    for p in ones + zeros:
        out += p.to_bytes(4, 'little')
    return bytes(out)


def crate_forest(sets):
    """Format 1 forest images of the sets, from the crate on main."""
    text = '\n'.join(' '.join(map(str, s)) for s in sets) + '\n'
    out = subprocess.run([V1, 'hex'], input=text, capture_output=True, text=True, check=True)
    return [bytes.fromhex(line.split()[1]) for line in out.stdout.splitlines()]


def write_directory(v1_forest):
    return b'\x01' + v1_forest[4:]


def sealed(s, v1_forest=None, forest_family=False):
    """(form, image): the earliest of the shortest among Bitmap, List, directory, Elias-Fano."""
    if not s:
        return 'Bitmap', b'\x80'
    cands = []
    if bitmap_len(s) is not None:
        cands.append((bitmap_len(s), 0, 'Bitmap', lambda: write_bitmap(s)))
    if list_len(s) is not None:
        cands.append((list_len(s), 1, 'Single' if len(s) == 1 else 'List', lambda: write_list(s)))
    if v1_forest is not None:
        cands.append((len(v1_forest) - 3, 2, 'Directory', lambda: write_directory(v1_forest)))
    if elias_fano_len(s) is not None and not forest_family:
        cands.append((elias_fano_len(s), 3, 'Elias-Fano', lambda: write_elias_fano(s)))
    length, _, form, write = min(cands, key=lambda c: (c[0], c[1]))
    image = write()
    assert len(image) == length, (form, s, len(image), length)
    return form, image


# --- reader (Bitmap, Single, List, Elias-Fano) -----------------------------------------------
def read(b):
    """(members, image length) of the image at the start of `b`."""
    tag = b[0]
    if tag >= 0x80:
        n = tag & 0x7F
        return [8 * i + j for i in range(n) for j in range(8) if (b[1 + i] >> j) & 1], 1 + n
    if tag == 0x02:
        n, at = read_varint(b, 1)
        n += 1
        first, at = read_varint(b, at)
        span, at = read_varint(b, at)
        low = low_width(n, span)
        buckets = (span >> low) + 1
        hb = (buckets + n + 7) // 8
        lows_at = at + hb
        out, k = [], 0
        for p in range(buckets + n):
            if (b[at + p // 8] >> (p % 8)) & 1:
                bit = k * low
                field = int.from_bytes(b[lows_at + bit // 8:lows_at + bit // 8 + 5], 'little') >> (bit % 8) & ((1 << low) - 1)
                out.append(first + ((p - k) << low | field))
                k += 1
        return out, lows_at + (n * low + 7) // 8 + 4 * ((n - 1) // 64) + 4 * ((buckets - 1) // 64)
    k = ((tag >> 2) & 7) + 1
    wa, wd = (tag & 3) + 1, (tag >> 5) + 1
    first = int.from_bytes(b[1:1 + wa], 'little')
    out = [first] + [first + int.from_bytes(b[1 + wa + i * wd:1 + wa + (i + 1) * wd], 'little') for i in range(k - 1)]
    return out, 1 + wa + (k - 1) * wd


def hexs(b):
    return ' '.join(f'{x:02X}' for x in b)


ACCEPTS = [
    ('the empty set', []),
    ('`{0}`', [0]),
    ('`{7}`: a tie of two bytes goes to the Bitmap', [7]),
    ('`{0, 1, 2, 10}`', [0, 1, 2, 10]),
    ('`{0, 1, 2, 9}`', [0, 1, 2, 9]),
    ('`0..=12` and 70', list(range(13)) + [70]),
    ('`0..=82`', list(range(83))),
    ('`{8}`: the Bitmap is three bytes, the Single two', [8]),
    ('`{255}`', [255]),
    ('`{256}`', [256]),
    ('`{65535}`', [65535]),
    ('`{65536}`', [65536]),
    ('`{1000000}`', [1000000]),
    ('`{4294967295}`', [4294967295]),
    ('`{7, 200}`', [7, 200]),
    ('`{1000, 2000}`', [1000, 2000]),
    ('`{1000000, 1000003}`', [1000000, 1000003]),
    ('`{5, 70000}`', [5, 70000]),
    ('`{5, 70000, 140000}`', [5, 70000, 140000]),
    ('`{0, 4294967295}`', [0, 4294967295]),
    ('eight members: `{300, 301, 310, 400, 401, 450, 500, 555}`', [300, 301, 310, 400, 401, 450, 500, 555]),
    ('nine members below 1,016: `{100, 200, ..., 900}` (a Stride directory)', list(range(100, 1000, 100))),
    ('nine members: `{1000, 1001, 1010, 1100, 1101, 1150, 1200, 1255, 1300}`', [1000, 1001, 1010, 1100, 1101, 1150, 1200, 1255, 1300]),
    ('nine scattered: `{3, 70000, 140001, 210000, 280007, 350000, 420002, 490000, 560009}`', [3, 70000, 140001, 210000, 280007, 350000, 420002, 490000, 560009]),
    ('`1000..=1007` (an interval: a Stride directory against a List)', list(range(1000, 1008))),
    ('`1000..=1099` (an interval: a Stride directory)', list(range(1000, 1100))),
    ('`{1015}` and `0..=7`: the Bitmap at its last position', list(range(8)) + [1015]),
    ('`0..=7` and 1016: past the Bitmap', list(range(8)) + [1016]),
    ('`0..=99` and `70000..=70099` (two Stride chunks)', list(range(100)) + list(range(70000, 70100))),
    ('66 members: `1000 i + (i * i mod 7)` for `i` in 0..=64, and 70000 (two chunks: a Cone and an Array)', [1000 * i + i * i % 7 for i in range(65)] + [70000]),
    ('200 members: `5000 i + (i * i mod 11)` for `i` in 0..=199 (three one-samples, three zero-samples)', [5000 * i + i * i % 11 for i in range(200)]),
]

REJECTS = [
    ('00', 'kind', 'open', 'the control `00` is no image (an all-zero slot)'),
    ('42 4D 53 0C', 'kind', 'open, as format 2', 'a format-1 image; a reader of both formats admits it as format 1'),
    ('60 05', 'kind', 'open', 'an unassigned control'),
    ('81', 'extent', 'open', 'a Bitmap of one byte with no byte'),
    ('81 00', 'extent', 'open', 'a Bitmap whose last byte is zero'),
    ('82 01 00', 'extent', 'open', 'a Bitmap whose last byte is zero'),
    ('21 07', 'extent', 'open', 'a Single of two bytes with one present'),
    ('21 07 00', 'extent', 'open', 'a Single in more bytes than its member needs'),
    ('24 07 C1 00', 'extent', 'open', 'a List whose distances are in more bytes than the last one needs'),
    ('08 07 05 05', 'map-order', 'open', 'a List with a member twice'),
    ('08 07 05 03', 'map-order', 'open', 'a List whose distances do not ascend'),
    ('07 FF FF FF FF 01', 'plane-range', 'open', 'a List whose last member passes 4,294,967,295'),
    ('04 07 01', 'canonical', 'open_verified', '`{7, 8}` as a List: its Bitmap `82 80 01` is no longer'),
    ('20 05', 'canonical', 'open_verified', '`{5}` as a Single: its Bitmap `81 20` is no longer'),
    ('02 00 05 00 01', 'count', 'open', 'an Elias-Fano image of one member'),
    ('02 01 05 00 03', 'count', 'open', 'an Elias-Fano image of two members in a span of zero'),
    ('02 02 05 DB C5 08 49 00 80 B5 C8 B6', 'extent', 'open', 'an Elias-Fano image cut short of its low plane'),
    ('02 02 05 DB C5 08 C9 00 80 B5 C8 B6 08', 'count', 'open_verified', 'a high plane with four ones for three members'),
    ('02 02 05 DB C5 08 49 00 80 B5 C8 B6 00', 'plane-range', 'open_verified', 'a last member that is not `first + span`'),
    ('02 01 00 01 05', 'canonical', 'open_verified', '`{0, 1}` as Elias-Fano: its Bitmap `81 03` is shorter'),
    ('01 07 00 00 00 00 03 05 00', 'canonical', 'open_verified', '`{5}` as a directory of one Array chunk: its Bitmap `81 20` is shorter'),
    ('03 00 00 00 00 00 81 20', 'canonical', 'open_verified', 'a u64 directory of one child at key zero: the set is the u32 image `81 20`'),
    ('03 01 01 00 00 00 01 00 00 00 02 00 00 00 81 20 81 80', 'map-order', 'open', 'a u64 directory whose keys do not ascend'),
]


def write_u64(members):
    """The u64 directory: children are the sealed u32 images of the low halves, whole."""
    groups = {}
    for v in members:
        groups.setdefault(v >> 32, []).append(v & 0xFFFFFFFF)
    keys = sorted(groups)
    if keys == [0]:
        return sealed(groups[0], crate_forest([groups[0]])[0])[1]
    children = [sealed(groups[k], crate_forest([groups[k]])[0])[1] for k in keys]
    out = bytearray([0x03]) + varint(len(keys) - 1)
    for k in keys:
        out += k.to_bytes(4, 'little')
    before = 0
    for i, k in enumerate(keys[:-1]):
        before += len(groups[k])
        if i < len(keys) - 2:
            out += before.to_bytes(8, 'little')
    at = 0
    for c in children[:-1]:
        at += len(c)
        out += at.to_bytes(4, 'little')
    for c in children:
        out += c
    return bytes(out)


U64 = [
    ('`{5}`', [5]),
    ('`{4294967296}`', [1 << 32]),
    ('`{5, 4294967303}`', [5, (1 << 32) + 7]),
    ('`{5, 4294967303, 8589934592, 8589934593}`', [5, (1 << 32) + 7, 2 << 32, (2 << 32) + 1]),
]


def vectors():
    sets = [s for _, s in ACCEPTS]
    forests = crate_forest([s or [0] for s in sets])
    print('| Set | Form | Sealed image | Bytes | Forest image, where it differs |')
    print('| --- | --- | --- | ---: | --- |')
    for (name, s), v1 in zip(ACCEPTS, forests):
        form, image = sealed(s, v1 if s else None)
        fform, fimage = sealed(s, v1 if s else None, forest_family=True)
        if s:
            assert read(image)[0] == s or form == 'Directory', name
        shown = hexs(image) if len(image) <= 40 else hexs(image[:24]) + f' ... ({len(image)} bytes)'
        other = '' if fimage == image else (hexs(fimage) if len(fimage) <= 40 else hexs(fimage[:16]) + f' ... ({len(fimage)} bytes, {fform})')
        print(f'| {name} | {form} | `{shown}` | {len(image)} | {"`" + other + "`" if other else ""} |')
    print()
    print('| u64 set | Image | Bytes |')
    print('| --- | --- | ---: |')
    for name, s in U64:
        image = write_u64(s)
        print(f'| {name} | `{hexs(image)}` | {len(image)} |')
    print()
    print('| Bytes | Class | Refused by | Why |')
    print('| --- | --- | --- | --- |')
    for b, cls, by, why in REJECTS:
        print(f'| `{b}` | `{cls}` | {by} | {why} |')


def check():
    rng = random.Random(2)
    n = 0
    for m in range(1, 1 << 12):
        for base, scale in [(0, 1), (0, 61), (250, 1), (65530, 1), (1000000, 3), (0, 8000), (4000000000, 7)]:
            s = [base + scale * p for p in range(12) if (m >> p) & 1]
            for write, length in ((write_bitmap, bitmap_len), (write_list, list_len), (write_elias_fano, elias_fano_len)):
                if length(s) is not None:
                    image = write(s)
                    assert len(image) == length(s), (s, write.__name__)
                    assert read(image + b'\xAA' * 8) == (s, len(image)), (s, write.__name__)
                    n += 1
    for _ in range(3000):
        k = rng.choice([2, 3, 9, 33, 64, 65, 129, 200, 1000])
        u = rng.choice([k + 5, 4 * k, 1000, 70000, 10 ** 6, 1 << 32])
        s = sorted(rng.sample(range(u), min(k, u)))
        image = write_elias_fano(s)
        assert len(image) == elias_fano_len(s)
        assert read(image) == (s, len(image))
        n += 1
    print(f'{n} images read back as their sets, each at its closed-form length')


def corpus():
    """The conformance corpus as JSON: every image whole."""
    import json
    forests = crate_forest([s or [0] for _, s in ACCEPTS])
    accepts = []
    for (name, s), v1 in zip(ACCEPTS, forests):
        form, image = sealed(s, v1 if s else None)
        _, fimage = sealed(s, v1 if s else None, forest_family=True)
        accepts.append({'members': s, 'form': form, 'sealed': image.hex(), 'forest': fimage.hex()})
    print(json.dumps({
        'format': 2,
        'u32': accepts,
        'u64': [{'members': s, 'sealed': write_u64(s).hex()} for _, s in U64],
        'rejects': [{'bytes': b.replace(' ', '').lower(), 'class': c, 'refused_by': by.split(',')[0], 'why': why.replace('`', '')} for b, c, by, why in REJECTS],
    }, indent=1))


if __name__ == '__main__':
    {'vectors': vectors, 'check': check, 'corpus': corpus}[sys.argv[1]]()