"""Every row that has data: bytes under format 1 and under each candidate family.

rows.py <data dir> [seen] [text] [parity] [frames] [synthetic]. The data files are described in README.md; the
synthetic rows need none.
"""
import math
import random
import sys
from model import evaluate, ef_planes, vlen, pct

D = sys.argv[1].rstrip('/') + '/'


def load(name, skip=0, cap=None):
    out = []
    for line in open(D + name):
        xs = [int(x) for x in line.split()[skip:]]
        if xs:
            out.append(xs)
        if cap and len(out) >= cap:
            break
    return out


def collection(name, lists, universe):
    """Many sets as ONE layout: bytes a list, framing included."""
    n = len(lists)
    total = sum(len(s) for s in lists)
    w = max(1, (universe - 1).bit_length())
    # (a) offsets as an Elias-Fano sequence of the cumulative ends; members packed at w bits
    csr = 8 + ef_planes(n, total) + (total * w + 7) // 8
    # (b) one Elias-Fano sequence over (list index, member) pairs: pair = index * 2^w + member
    pairs = 8 + ef_planes(total, (n << w) - 1)
    # (c) offsets as (a); each list's members as an Elias-Fano body with no header of its own:
    #     its count comes from the offsets, its span is the universe
    body = sum(ef_planes(len(s), universe - 1, samples=False) for s in lists)
    bits = sum(8 * ef_planes(len(s), universe - 1, samples=False) for s in lists)
    per = 8 + ef_planes(n, max(1, bits // 8)) + body
    print(f"\n{name}: {n} lists, {total} members, universe {universe} ({w} bits): as ONE collection: "
          f"offsets + packed members {csr/n:.2f} B a list; one pair sequence {pairs/n:.2f} B a list; "
          f"offsets + headerless Elias-Fano bodies {per/n:.2f} B a list")


if __name__ == '__main__':
    which = sys.argv[2:] or ['seen', 'text', 'parity', 'frames', 'synthetic']
    if 'seen' in which:
        evaluate('seen-set, one transaction (REAL, fleet log A: 2,545 runs of 887 families)', load('seen-runs.txt', 2))
        evaluate('seen-set, one transaction (REAL, fleet log B: 610 runs of 288 families)', load('seen-run3-runs.txt', 2))
        evaluate('seen-set, a family whole (REAL, fleet log A)', load('seen-families.txt', 1))
    if 'text' in which:
        for corpus in ('repo',):
            for kind in ('postings', 'ngrams'):
                lists = load(f'{corpus}-{kind}.txt')
                rows = 1 + max(s[-1] for s in lists)
                label = f'{kind}, text corpus "{corpus}" (REAL text, {rows} rows)'
                evaluate(label + ': every list', lists, raw_width=4 if rows > 65536 else 2)
                tail = [s for s in lists if len(s) <= 3]
                evaluate(label + ': lists of 1 to 3', tail, raw_width=4 if rows > 65536 else 2)
                evaluate(label + ': lists of 4 to 32', [s for s in lists if 4 <= len(s) <= 32])
                evaluate(label + ': lists of 33 to 256', [s for s in lists if 33 <= len(s) <= 256])
                evaluate(label + ': lists above 256', [s for s in lists if len(s) > 256])
                collection(label + ': lists of 1 to 3', tail, rows)
                collection(label + ': lists of at most 32', [s for s in lists if len(s) <= 32], rows)
                collection(label + ': every list', lists, rows)
    if 'parity' in which:
        evaluate('postings, a parity corpus (54,000 documents, 256 synthetic Zipf tokens)', load('postings-u32.txt', 1))
        evaluate('n-grams, a parity corpus (54,000 rows)', load('ngrams.txt', 1))
    if 'frames' in which:
        evaluate('frames: the 812 REAL shapes, optional positions drawn at p = 0.5 (4 draws a shape)', load('frames.txt'))
    if 'synthetic' in which:
        rng = random.Random(20261115)
        evaluate('SYNTHETIC routing set, 1..64 listeners of 10^5', [sorted(rng.sample(range(100_000), rng.randint(1, 64))) for _ in range(4000)])
        evaluate('SYNTHETIC routing set, 1..64 listeners of 10^6', [sorted(rng.sample(range(1_000_000), rng.randint(1, 64))) for _ in range(4000)])
        evaluate('SYNTHETIC slot of capacity 16: 1..16 customers of 10^6', [sorted(rng.sample(range(1_000_000), rng.randint(1, 16))) for _ in range(4000)])
        # 12k: a destination set split by range shards of 50,000 (23:225): each shard's part of 84,321 of 10^6
        whole = sorted(rng.sample(range(1_000_000), 84_321))
        shards = {}
        for v in whole:
            shards.setdefault(v // 50_000, []).append(v)
        evaluate('SYNTHETIC fan-out: 84,321 of 10^6 split into range shards of 50,000', list(shards.values()))
        sparse = sorted(rng.sample(range(1_000_000), 2_000))
        shards = {}
        for v in sparse:
            shards.setdefault(v // 50_000, []).append(v)
        evaluate('SYNTHETIC fan-out: 2,000 of 10^6 split into range shards of 50,000', list(shards.values()))
        evaluate('SYNTHETIC routing set, 10^3..10^4 of 10^6', [sorted(rng.sample(range(1_000_000), rng.randint(1000, 10000))) for _ in range(60)])
        evaluate('SYNTHETIC attribute E-set, 30% of 2x10^5', [sorted(rng.sample(range(200_000), 60_000)) for _ in range(10)])
        def plane(n, holes):
            s = set(range(n))
            for _ in range(holes):
                s.discard(rng.randrange(n))
            return sorted(s)
        evaluate('SYNTHETIC actor plane: 0..n less 0..3 holes, n 1..300', [s for s in (plane(rng.randint(1, 300), rng.randint(0, 3)) for _ in range(4000)) if s])
        evaluate('SYNTHETIC actor subset: each of n actors with p = 0.1, n 8..300', [s for s in ([v for v in range(rng.randint(8, 300)) if rng.random() < 0.1] for _ in range(4000)) if s])
        evaluate('SYNTHETIC one interval anywhere below 10^6, 1..200 long', [list(range(a, a + rng.randint(1, 200))) for a in (rng.randrange(1_000_000) for _ in range(4000))])
        evaluate('SYNTHETIC singleton anywhere below 2^32', [[rng.randrange(1 << 32)] for _ in range(4000)])