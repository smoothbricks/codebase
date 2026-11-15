# The sealed image of a set is its shortest form (specs/bitmosaic/01-format.md "The one image of a set"). Three of
# the four lengths are closed formulas of the member count and the two end members. The fourth, the directory's,
# needs each chunk's container, and choosing a container walks the chunk (the Cone's fit). These laws say when the
# directory need not be priced at all.
#
# The model, in bytes: a directory of one chunk is seven bytes of framing and its payload, and `x` stands for
# whatever more a real one carries (longer varints, a Runs payload's count and gaps, further chunks). `floor(n, r)`
# is seven and the least of a Cone's `11 + n`, an Array's `2 n` and a Runs' `4 r`, for `n` members in `r` runs.
# `shortest(cap, w, n, r)` is kept from the first form of these laws as a model of three small candidates of
# `w`, `4 r` and `n` bytes under a cap; format 2 uses its first and last only (a bitmap of `w` bytes, a list).
#
# Proven here, for all `n`, `r`, `x`:
# - a_cone_directory_is_not_under_the_floor, an_array_directory_is_not_under_the_floor,
#   a_runs_directory_is_not_under_the_floor: whichever of the three containers a chunk takes, the directory is at
#   least `floor(n, r)`.
#   LICENSE the writer's shortcut of 01-format.md "Writing": a Bitmap, List or Elias-Fano length at or under the
#   floor is the image without the directory being priced. (One member and an arithmetic progression are not on
#   this floor: they are priced exactly, at nine and eleven bytes.)
# - ordinals_never_ask_the_ladder: with a bitmap of `w <= cap` bytes on offer and `r <= n`, the least small
#   candidate is strictly under the floor.
# - a_wide_window_is_longer_than_one_at_its_start, a_wide_window_is_longer_than_one_from_zero,
#   a_wide_list_is_no_shorter: a candidate with more framing or wider fields over the same payload never wins.
#   LICENSE dropping the dominated candidates: format 2 has one bitmap form and one list form.
# The comparisons between the small forms themselves are in one_image.bend.
#
# Not modelled: the division of a member by eight that gives a bitmap's bytes (the laws take the byte counts), and
# the Cone's fit (the floor assumes it fits). Constants are unary `Nat`s; 32 appears only as `cap`.
#
# Check: bend2 specs/bitmosaic/laws/form_choice.bend --verdict
import Base

def add(+a: Nat, b: Nat) -> Nat:
  match a:
    case 0n:
      b
    case 1n+p:
      1n+add(p, b)

def le(+a: Nat, +b: Nat) -> Bool:
  match a b:
    case 0n b0:
      True{}
    case 1n+p 0n:
      False{}
    case 1n+p 1n+q:
      le(p, q)

def min(+a: Nat, +b: Nat) -> Nat:
  match a b:
    case 0n b0:
      0n
    case 1n+p 0n:
      0n
    case 1n+p 1n+q:
      1n+min(p, q)

def IsTrue(b: Bool) -> Data:
  match b:
    case True{}:
      Unit
    case False{}:
      Empty

def IsFalse(b: Bool) -> Data:
  match b:
    case True{}:
      Empty
    case False{}:
      Unit

def dbl(+n: Nat) -> Nat:
  add(n, n)

def four(+r: Nat) -> Nat:
  add(r, add(r, add(r, r)))

def least(+a: Nat, +b: Nat, +c: Nat) -> Nat:
  min(a, min(b, c))

def floor(+n: Nat, +r: Nat) -> Nat:
  add(7n, least(add(11n, n), dbl(n), four(r)))

def offer(+take: Bool, +len: Nat, +best: Nat) -> Nat:
  match take:
    case True{}:
      min(len, best)
    case False{}:
      best

def shortest(+cap: Nat, +w: Nat, +n: Nat, +r: Nat) -> Nat:
  offer(le(n, cap), n, offer(le(r, cap), four(r), w))

def le_refl(+a: Nat) -> IsTrue(le(a, a)):
  match a:
    case 0n:
      Unit{}
    case 1n+p:
      le_refl(p)

def le_trans(+a: Nat, +b: Nat, +c: Nat) -> IsTrue(le(a, b)) -> IsTrue(le(b, c)) -> IsTrue(le(a, c)):
  match a b c:
    case 0n b0 c0:
      i => j => Unit{}
    case 1n+p 0n c0:
      i => j => Empty.absurd(IsTrue(le(1n+p, c0)), i)
    case 1n+p 1n+q 0n:
      i => j => j
    case 1n+p 1n+q 1n+s:
      le_trans(p, q, s)

def le_succ(+a: Nat, +b: Nat) -> IsTrue(le(a, b)) -> IsTrue(le(a, 1n+b)):
  match a b:
    case 0n b0:
      i => Unit{}
    case 1n+p 0n:
      i => Empty.absurd(IsTrue(le(1n+p, 1n)), i)
    case 1n+p 1n+q:
      le_succ(p, q)

def le_addl(+k: Nat, +m: Nat) -> IsTrue(le(m, add(k, m))):
  match k:
    case 0n:
      le_refl(m)
    case 1n+p:
      le_succ(m, add(p, m), le_addl(p, m))

def le_addr(+m: Nat, +x: Nat) -> IsTrue(le(m, add(m, x))):
  match m:
    case 0n:
      Unit{}
    case 1n+p:
      le_addr(p, x)

def le_min_l(+a: Nat, +b: Nat) -> IsTrue(le(min(a, b), a)):
  match a b:
    case 0n b0:
      Unit{}
    case 1n+p 0n:
      Unit{}
    case 1n+p 1n+q:
      le_min_l(p, q)

def le_min_r(+a: Nat, +b: Nat) -> IsTrue(le(min(a, b), b)):
  match a b:
    case 0n b0:
      Unit{}
    case 1n+p 0n:
      Unit{}
    case 1n+p 1n+q:
      le_min_r(p, q)

def le_min_both(+x: Nat, +a: Nat, +b: Nat) -> IsTrue(le(x, a)) -> IsTrue(le(x, b)) -> IsTrue(le(x, min(a, b))):
  match x a b:
    case 0n a0 b0:
      i => j => Unit{}
    case 1n+p 0n b0:
      i => j => Empty.absurd(IsTrue(le(1n+p, min(0n, b0))), i)
    case 1n+p 1n+q 0n:
      i => j => j
    case 1n+p 1n+q 1n+s:
      le_min_both(p, q, s)

def not_le(+a: Nat, +b: Nat) -> IsFalse(le(a, b)) -> IsTrue(le(b, a)):
  match a b:
    case 0n b0:
      i => Empty.absurd(IsTrue(le(b0, 0n)), i)
    case 1n+p 0n:
      i => Unit{}
    case 1n+p 1n+q:
      not_le(p, q)

law a_cone_directory_is_not_under_the_floor:
  for +n: Nat
  for +r: Nat
  for +x: Nat
  IsTrue(le(floor(n, r), add(7n, add(add(11n, n), x))))

def a_cone_directory_is_not_under_the_floor(n, r, x):
  le_trans(least(add(11n, n), dbl(n), four(r)), add(11n, n), add(add(11n, n), x), le_min_l(add(11n, n), min(dbl(n), four(r))), le_addr(add(11n, n), x))

law an_array_directory_is_not_under_the_floor:
  for +n: Nat
  for +r: Nat
  for +x: Nat
  IsTrue(le(floor(n, r), add(7n, add(dbl(n), x))))

def an_array_directory_is_not_under_the_floor(n, r, x):
  le_trans(least(add(11n, n), dbl(n), four(r)), dbl(n), add(dbl(n), x), le_trans(least(add(11n, n), dbl(n), four(r)), min(dbl(n), four(r)), dbl(n), le_min_r(add(11n, n), min(dbl(n), four(r))), le_min_l(dbl(n), four(r))), le_addr(dbl(n), x))

law a_runs_directory_is_not_under_the_floor:
  for +n: Nat
  for +r: Nat
  for +x: Nat
  IsTrue(le(floor(n, r), add(7n, add(four(r), x))))

def a_runs_directory_is_not_under_the_floor(n, r, x):
  le_trans(least(add(11n, n), dbl(n), four(r)), four(r), add(four(r), x), le_trans(least(add(11n, n), dbl(n), four(r)), min(dbl(n), four(r)), four(r), le_min_r(add(11n, n), min(dbl(n), four(r))), le_min_r(dbl(n), four(r))), le_addr(four(r), x))

def lift(+s: Nat, +m: Nat) -> IsTrue(le(s, m)) -> IsTrue(le(1n+s, add(7n, m))):
  i => le_trans(s, m, add(6n, m), i, le_addl(6n, m))

def under_all(+s: Nat, +n: Nat, +r: Nat) -> IsTrue(le(s, n)) -> IsTrue(le(s, four(r))) -> IsTrue(le(s, least(add(11n, n), dbl(n), four(r)))):
  +i => j => le_min_both(s, add(11n, n), min(dbl(n), four(r)), le_trans(s, n, add(11n, n), i, le_addl(11n, n)), le_min_both(s, dbl(n), four(r), le_trans(s, n, dbl(n), i, le_addr(n, n)), j))

def pick(+ln: Bool, +lr: Bool, +w: Nat, +n: Nat, +r: Nat) -> (IsTrue(ln) -> IsTrue(lr)) -> (IsFalse(ln) -> IsTrue(le(w, n))) -> (IsFalse(lr) -> IsTrue(le(w, r))) -> IsTrue(le(offer(ln, n, offer(lr, four(r), w)), least(add(11n, n), dbl(n), four(r)))):
  match ln lr:
    case True{} True{}:
      a => b => c => under_all(min(n, min(four(r), w)), n, r, le_min_l(n, min(four(r), w)), le_trans(min(n, min(four(r), w)), min(four(r), w), four(r), le_min_r(n, min(four(r), w)), le_min_l(four(r), w)))
    case True{} False{}:
      a => b => c => Empty.absurd(IsTrue(le(offer(True{}, n, offer(False{}, four(r), w)), least(add(11n, n), dbl(n), four(r)))), a(Unit{}))
    case False{} True{}:
      a => b => c => under_all(min(four(r), w), n, r, le_trans(min(four(r), w), w, n, le_min_r(four(r), w), b(Unit{})), le_min_l(four(r), w))
    case False{} False{}:
      a => b => c => under_all(w, n, r, b(Unit{}), le_trans(w, r, four(r), c(Unit{}), le_addr(r, add(r, add(r, r)))))

law ordinals_never_ask_the_ladder:
  for +cap: Nat
  for +w: Nat
  for +n: Nat
  for +r: Nat
  for +a: IsTrue(le(w, cap))
  for +b: IsTrue(le(r, n))
  IsTrue(le(1n+shortest(cap, w, n, r), floor(n, r)))

def ordinals_never_ask_the_ladder(cap, w, n, r, a, b):
  lift(shortest(cap, w, n, r), least(add(11n, n), dbl(n), four(r)), pick(le(n, cap), le(r, cap), w, n, r, i => le_trans(r, n, cap, b, i), i => le_trans(w, cap, n, a, not_le(n, cap, i)), i => le_trans(w, cap, r, a, not_le(r, cap, i))))

law a_wide_window_is_longer_than_one_at_its_start:
  for +d: Nat
  IsTrue(le(add(3n, d), add(4n, d)))

def a_wide_window_is_longer_than_one_at_its_start(d):
  le_succ(d, d, le_refl(d))

law a_wide_window_is_longer_than_one_from_zero:
  for +e: Nat
  IsTrue(le(add(2n, e), add(4n, e)))

def a_wide_window_is_longer_than_one_from_zero(e):
  le_addl(2n, e)

law a_wide_list_is_no_shorter:
  for +n: Nat
  IsTrue(le(n, dbl(n)))

def a_wide_list_is_no_shorter(n):
  le_addr(n, n)
