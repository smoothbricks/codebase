# The closed-form lengths of three forms of a bitmosaic image and the rule that picks among them
# (specs/bitmosaic/01-format.md "The one image of a set"), evaluated over every nonempty subset of twelve positions
# at seven placements: a position `p` is the member `base + scale * p`. The placements put the same 4,095 subsets
# below 256, across the Bitmap's end (1,015), across a chunk boundary (65,536), at the widths of one to four bytes
# and at the top of the u32 domain, so the forms separate: a dense universe at zero alone picks the Bitmap almost
# everywhere.
#
# This file is an evaluator, not a proof: `bend2 specs/bitmosaic/laws/sizes.bend` prints, for each placement,
#   <sum of the chosen lengths> <sets that pick Bitmap> <List> <Elias-Fano> <sum of Elias-Fano lengths alone>
# and then one line, the count of (k, span) pairs whose low width breaks its bound (below). The same numbers come from the Python model (packages/bitmosaic/experiments/format-2/sizes_check.py) and from an
# implementation's `image_len`; the three agree or one of them is wrong. The directory is not in it (its length
# needs each chunk's container): where it is the image, the chosen length here is an upper bound.
#
# A length that does not apply is `none` (1,000,000): a Bitmap past member 1,015, a List of more than eight
# members, an Elias-Fano image of one member.
#
# Check: bend2 specs/bitmosaic/laws/sizes.bend
import Base

def none() -> U32:
  1000000

def pick(+c: Bool, a: U32, b: U32) -> U32:
  match c:
    case True{}:
      a
    case False{}:
      b

def one(+c: Bool) -> U32:
  match c:
    case True{}:
      1
    case False{}:
      0

# the bytes a value needs, one to four
def width(+x: U32) -> U32:
  (1 + one(U32.is_ge(x, 256)) + one(U32.is_ge(x, 65536)) + one(U32.is_ge(x, 16777216)) : U32)

# the bytes of a varint
def varint(+x: U32) -> U32:
  (1 + one(U32.is_ge(x, 128)) + one(U32.is_ge(x, 16384)) + one(U32.is_ge(x, 2097152)) + one(U32.is_ge(x, 268435456)) : U32)

def bitmap(+last: U32) -> U32:
  pick(U32.is_le(last, 1015), (2 + last / 8 : U32), none())

def list(+k: U32, +first: U32, +span: U32) -> U32:
  pick(U32.is_le(k, 8), (1 + width(first) + (k - 1) * width(span) : U32), none())

# the low width: 0 when span <= k; else floor(log2 span) - floor(log2 k), less one when that leaves fewer than k buckets
def lower(+tight: Bool, +l: U32, +k: U32, +span: U32) -> U32:
  match tight:
    case True{}:
      (l - 1 : U32)
    case False{}:
      l

def low(+flat: Bool, +k: U32, +span: U32) -> U32:
  match flat:
    case True{}:
      0
    case False{}:
      +l = (U32.from_nat(U32.log2(span)) - U32.from_nat(U32.log2(k)) : U32)
      lower(U32.is_ne(l, 0) && U32.is_lt(U32.shrn(span, U32.to_nat(l)), k), l, k, span)

def elias_fano(+k: U32, +first: U32, +span: U32) -> U32:
  +l = low(U32.is_le(span, k), k, span)
  +buckets = (U32.shrn(span, U32.to_nat(l)) + 1 : U32)
  pick(U32.is_ge(k, 2), (1 + varint((k - 1 : U32)) + varint(first) + varint(span) + (buckets + k + 7) / 8 + (k * l + 7) / 8 + 4 * ((k - 1) / 64) + 4 * ((buckets - 1) / 64) : U32), none())

def go(+fuel: Nat, +m: U32, +base: U32, +scale: U32, +sum: U32, +nb: U32, +nl: U32, +ne: U32, +efs: U32) -> String:
  match fuel:
    case 0n:
      U32.show(sum) ++ " " ++ U32.show(nb) ++ " " ++ U32.show(nl) ++ " " ++ U32.show(ne) ++ " " ++ U32.show(efs)
    case 1n+f:
      +k = U32.from_nat(U32.popcount(m))
      +first = (base + scale * U32.from_nat(U32.ctz(m)) : U32)
      +last = (base + scale * U32.from_nat(U32.log2(m)) : U32)
      +span = (last - first : U32)
      +b = bitmap(last)
      +l = list(k, first, span)
      +e = elias_fano(k, first, span)
      +isb = U32.is_le(b, l) && U32.is_le(b, e)
      +isl = U32.is_lt(l, b) && U32.is_le(l, e)
      +ise = U32.is_lt(e, b) && U32.is_lt(e, l)
      go(f, (m + 1 : U32), base, scale, (sum + U32.min(b, U32.min(l, e)) : U32), (nb + one(isb) : U32), (nl + one(isl) : U32), (ne + one(ise) : U32), (efs + pick(U32.is_ge(k, 2), e, 0) : U32))

# The closed-form low width keeps the high plane short: with `L = low(k, span)`, `span >> L` is below `2 k` (so the
# plane holds at most `3 k` bits and a bucket's scan is bounded), and `L` is zero or leaves at least `k` buckets.
# Counted over every `k` in 2..=64 and every span in 1..=4095: the number of pairs that break either. Zero.
def not(+b: Bool) -> Bool:
  match b:
    case True{}:
      False{}
    case False{}:
      True{}

def spans(+fuel: Nat, +k: U32, +span: U32, +bad: U32) -> U32:
  match fuel:
    case 0n:
      bad
    case 1n+f:
      +l = low(U32.is_le(span, k), k, span)
      +h = U32.shrn(span, U32.to_nat(l))
      +ok = U32.is_lt(h, (2 * k : U32)) && (U32.is_eq(l, 0) || U32.is_ge(h, k))
      spans(f, k, (span + 1 : U32), (bad + one(not(ok)) : U32))

def counts(+fuel: Nat, +k: U32, +bad: U32) -> U32:
  match fuel:
    case 0n:
      bad
    case 1n+f:
      counts(f, (k + 1 : U32), spans(U32.to_nat(4095), k, 1, bad))

def place(+base: U32, +scale: U32) -> String:
  go(U32.to_nat(4095), 1, base, scale, 0, 0, 0, 0, 0)

def main() -> IO(Unit):
  do IO<Unit>:
    IO.print(place(0, 1))
    IO.print(place(0, 61))
    IO.print(place(250, 1))
    IO.print(place(65530, 1))
    IO.print(place(1000000, 3))
    IO.print(place(0, 8000))
    IO.print(place(4000000000, 7))
    IO.print(U32.show(counts(U32.to_nat(63), 2, 0)))