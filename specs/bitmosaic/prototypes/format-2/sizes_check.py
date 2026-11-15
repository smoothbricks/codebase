"""The numbers specs/bitmosaic/laws/sizes.bend prints, from the Python size model: the two must agree."""
NONE = 1_000_000


def width(x):
    return 1 + (x >= 256) + (x >= 65536) + (x >= 1 << 24)


def varint(x):
    return 1 + (x >= 128) + (x >= 16384) + (x >= 1 << 21) + (x >= 1 << 28)


def bitmap(last):
    return 2 + last // 8 if last <= 1015 else NONE


def list_len(k, first, span):
    return 1 + width(first) + (k - 1) * width(span) if k <= 8 else NONE


def low(k, span):
    if span <= k:
        return 0
    l = span.bit_length() - k.bit_length()
    return l - 1 if l and (span >> l) < k else l


def elias_fano(k, first, span):
    if k < 2:
        return NONE
    l = low(k, span)
    buckets = (span >> l) + 1
    return (1 + varint(k - 1) + varint(first) + varint(span) + (buckets + k + 7) // 8 + (k * l + 7) // 8
            + 4 * ((k - 1) // 64) + 4 * ((buckets - 1) // 64))


if __name__ == '__main__':
    for base, scale in [(0, 1), (0, 61), (250, 1), (65530, 1), (1000000, 3), (0, 8000), (4000000000, 7)]:
        total = nb = nl = ne = efs = 0
        for m in range(1, 4096):
            k = bin(m).count('1')
            first = base + scale * ((m & -m).bit_length() - 1)
            last = base + scale * (m.bit_length() - 1)
            b, l, e = bitmap(last), list_len(k, first, last - first), elias_fano(k, first, last - first)
            total += min(b, l, e)
            nb += b <= l and b <= e
            nl += l < b and l <= e
            ne += e < b and e < l
            efs += e if k >= 2 else 0
        print(total, nb, nl, ne, efs)
    print(sum(not ((span >> low(k, span)) < 2 * k and (low(k, span) == 0 or (span >> low(k, span)) >= k))
              for k in range(2, 65) for span in range(1, 4096)))