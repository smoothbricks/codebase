"""Round two size model: every candidate form as a closed formula of an ascending member list.

Sizes are whole-image bytes, control byte included. vlen(x) is the ULEB128 length of x. The baseline and the
directory's length come from the repository's crate through `proto/target/release/crate_len` (BITMOSAIC_CRATE_LEN
names another build of it); BITMOSAIC_SMALL_LEN, when set, names a tool that prints one length a set for an
implementation of small forms, and adds its column.
"""
import math
import os
import subprocess

HERE = os.path.dirname(os.path.abspath(__file__))
CRATE_LEN = os.environ.get('BITMOSAIC_CRATE_LEN', HERE + '/proto/target/release/crate_len')
SMALL_LEN = os.environ.get('BITMOSAIC_SMALL_LEN')


def vlen(x):
    n = 1
    while x >= 128:
        x >>= 7
        n += 1
    return n


def lg2c(n, k):
    if k < 0 or k > n:
        return float('-inf')
    return (math.lgamma(n + 1) - math.lgamma(k + 1) - math.lgamma(n - k + 1)) / math.log(2)


def shape(s):
    """(k, first, last, runs as [(start, end)])."""
    rs = []
    st = p = s[0]
    for v in s[1:]:
        if v != p + 1:
            rs.append((st, p))
            st = v
        p = v
    rs.append((st, p))
    return len(s), s[0], s[-1], rs


def ef_low(n, span):
    """The crate's closed form (ef.rs): low width from cardinality and span = last - first."""
    if span <= n:
        return 0
    low = span.bit_length() - 1 - (n.bit_length() - 1)
    if low and (span >> low) < n:
        low -= 1
    return low


def ef_planes(n, span, samples=True):
    """Byte-granular Elias-Fano planes over n values in 0..=span; samples as u32 past the first of each kind."""
    low = ef_low(n, span)
    buckets = (span >> low) + 1
    z = (n * low + 7) // 8 + (buckets + n + 7) // 8
    if samples:
        z += 4 * ((n - 1) // 64) + 4 * ((buckets - 1) // 64)
    return z


def ef_best_planes(n, span):
    best = None
    for low in range(0, 32):
        z = (n * low + 7) // 8 + ((span >> low) + 1 + n + 7) // 8
        best = z if best is None else min(best, z)
    return best


def bound_bytes(s):
    """Information in a set of k members and r runs between a known first and last member, in bytes."""
    k, a, b, rs = shape(s)
    u = b - a + 1
    r = len(rs)
    scatter = lg2c(u - 2, k - 2) if k >= 2 else 0.0
    runs = lg2c(k - 1, r - 1) + lg2c(u - k + 1, r)
    return max(0.0, min(scatter, runs)) / 8


def forms(s):
    """Every candidate form that can hold `s`, with its bytes."""
    k, a, b, rs = shape(s)
    r = len(rs)
    sb, eb = a >> 3, b >> 3
    span = b - a
    f = {}
    # --- bitmaps, byte-aligned in the value domain
    n = eb - sb + 1
    if eb < 127:
        f['bm'] = 1 + eb + 1
    if n <= 32:
        f['wb'] = 1 + vlen(sb) + n
    if n <= 256:
        f['ww'] = 2 + vlen(sb) + n
    # --- lists relative to the first member (the first is the base, so k - 1 fields)
    if k <= 32:
        if span < 256:
            f['l8'] = 1 + vlen(a) + (k - 1)
        if span < 65536:
            f['l16'] = 1 + vlen(a) + 2 * (k - 1)
        f['l32'] = 1 + vlen(a) + 4 * (k - 1)
    # --- a plain list: k members, each in the bytes the last one needs; no base
    nbytes = lambda x: max(1, (x.bit_length() + 7) // 8)
    if k <= 31:
        f['lw'] = 1 + k * nbytes(b)
    if k <= 31:
        f['lr31'] = 1 + nbytes(a) + (k - 1) * nbytes(span)
    if k <= 8:
        # the List form: the first member in its own bytes, k - 1 deltas from it in the bytes the span needs
        f['lr'] = 1 + nbytes(a) + (k - 1) * nbytes(span)
    # --- interval lists: boundaries start_1 (the base), end_1 + 1, start_2, .. relative to the base
    if r == 1:
        f['iv'] = 1 + vlen(a) + vlen(k - 1)
    if r <= 32:
        if span + 1 < 256:
            f['s8'] = 1 + vlen(a) + (2 * r - 1)
        if span + 1 < 65536:
            f['s16'] = 1 + vlen(a) + 2 * (2 * r - 1)
        f['s32'] = 1 + vlen(a) + 4 * (2 * r - 1)
    gaps = [rs[0][1] - rs[0][0]]
    for (s0, e0), (s1, e1) in zip(rs, rs[1:]):
        gaps += [s1 - e0 - 2, e1 - s1]
    if r <= 32:
        f['sv'] = 1 + vlen(a) + sum(vlen(g) for g in gaps)
    # --- Elias-Fano, lean at every size: count in the tag up to 32, else a varint; base and span varints
    if k >= 2:
        head = 1 + vlen(k - 1) + vlen(a) + vlen(span)
        f['ef'] = head + ef_planes(k, span)
        f['ef*'] = head + ef_best_planes(k, span)
        nb = 2 * r
        headr = 1 + (0 if r <= 32 else vlen(r - 1)) + vlen(a) + vlen(span + 1)
        f['runef'] = headr + ef_planes(nb, span + 1)
        # today's root layout behind one control byte: word-granular high plane, every sample, a body length
        low = ef_low(k, span)
        buckets = (span >> low) + 1
        body = vlen(k - 1) + vlen(a) + vlen(span) + 8 * ((buckets + k + 63) // 64) + (k * low + 7) // 8 \
            + 4 * ((k - 1) // 64) + 4 * ((buckets + 63) // 64)
        f['ef1'] = 1 + vlen(body) + body
        # partitioned: blocks of 128 members, each its own planes (or nothing when it is one run); a block's
        # record in the upper level is its last member and its end offset, 4 + 2 bytes
        if k > 256:
            z = 1 + vlen(k - 1) + vlen(a)
            for i in range(0, k, 128):
                blk = s[i:i + 128]
                lo = s[i - 1] + 1 if i else a
                sp = blk[-1] - lo
                z += 8 + (0 if sp + 1 == len(blk) else min(ef_planes(len(blk), sp, samples=False), (sp + 8) // 8))
            f['pef'] = z
    # --- sparse window: a bitmap of the nonzero bytes, then the nonzero bytes
    nz = len({v >> 3 for v in s})
    if n <= 2048:
        f['sw8'] = 2 + vlen(sb) + (n + 7) // 8 + nz
    return f


FAMILIES = {
    'bitmap, else directory': ['bm'],
    "bitmap + today's Elias-Fano root": ['bm', 'ef1'],
    'bitmap + lean Elias-Fano': ['bm', 'ef'],
    'FOUR: bitmap + list + lean Elias-Fano + directory': ['bm', 'lr', 'ef'],
    'FOUR, list not relative': ['bm', 'lw', 'ef'],
    'FOUR, list up to 31 members': ['bm', 'lr31', 'ef'],
    'FOUR + interval': ['bm', 'lr', 'iv', 'ef'],
    'FOUR + spans with a base': ['bm', 'lr', 's16', 'iv', 'ef'],
    'FOUR + bitmap at a base': ['bm', 'lr', 'wb', 'ww', 'ef'],
    'FOUR + run Elias-Fano': ['bm', 'lr', 'ef', 'runef'],
    'FOUR + blocks (partitioned Elias-Fano)': ['bm', 'lr', 'ef', 'pef'],
    'every candidate': ['bm', 'wb', 'ww', 'lw', 'lr31', 'l8', 'l16', 'l32', 's8', 's16', 's32', 'sv', 'iv', 'ef', 'runef', 'sw8', 'pef'],
}


def crate_lens(binary, sets):
    text = '\n'.join(' '.join(map(str, s)) for s in sets) + '\n'
    out = subprocess.run([binary], input=text, capture_output=True, text=True, check=True)
    return [tuple(int(x) for x in line.split()) for line in out.stdout.splitlines()]


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(p * len(xs)))]


def evaluate(name, sets, families=None, out=print, raw_width=None):
    """One block: baselines, every family (mean / p50 / p90 / max, total, picks), the bound."""
    families = families or FAMILIES
    v1 = crate_lens(CRATE_LEN, sets)
    now = [x[0] for x in crate_lens(SMALL_LEN, sets)] if SMALL_LEN else None
    main = [x[0] for x in v1]
    large = [x[1] - 3 for x in v1]      # v1 directory with a one-byte identification
    n = len(sets)
    ks = [len(s) for s in sets]
    rr = [len(shape(s)[3]) for s in sets]
    bnd = [bound_bytes(s) for s in sets]

    def line(label, xs, extra=''):
        out(f"| {label} | {sum(xs)/n:.2f} | {pct(xs,.5)} | {pct(xs,.9)} | {max(xs)} | {sum(xs)} | {extra} |")

    out(f"\n### {name}\n")
    out(f"{n} sets; members mean {sum(ks)/n:.1f} p50 {pct(ks,.5)} p90 {pct(ks,.9)} max {max(ks)}; runs mean {sum(rr)/n:.1f}; "
        f"bound mean {sum(bnd)/n:.2f} B, total {sum(bnd):.0f} B\n")
    out("| family | mean | p50 | p90 | max | total | picks |")
    out("| --- | --- | --- | --- | --- | --- | --- |")
    line('format 1 (the crate)', main)
    if now:
        line('an implementation of small forms', now)
    if raw_width:
        line(f'raw u{raw_width * 8} array, no length', [raw_width * k for k in ks])
    fs = [forms(s) for s in sets]
    res = {}
    for fam, members in families.items():
        xs, picks = [], {}
        for f, lg in zip(fs, large):
            z, m = min(((f[m], m) for m in members if m in f), default=(1 << 60, None))
            if lg < z:
                z, m = lg, 'directory'
            xs.append(z)
            picks[m] = picks.get(m, 0) + 1
        res[fam] = xs
        line(fam, xs, ', '.join(f"{m} {100*c/n:.0f}%" for m, c in sorted(picks.items(), key=lambda t: -t[1])[:6]))
    return res