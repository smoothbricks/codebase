# Seek: the least member at or above a value (specs/bitmosaic/01-format.md "Reads"). An intersection of two images
# leapfrogs: each side seeks the other's candidate. No form stores anything for it: a seek is a rank and a select.
#
# The model: a set is its members ascending, as `Nat`. `rank(xs, v)` is the search (the position of the first
# member not below `v`); `at(i, xs)` is the member at position `i` (select).
#
# Proven here, for every list and every value:
# - the_member_at_the_rank_is_not_below: when the rank is inside the list, the member there is at or above `v`.
# - members_before_the_rank_are_below: every member at a position under the rank is below `v`.
#   Together: `select(rank(v))` is the least member at or above `v`, and there is none when the rank is the
#   cardinality. LICENSE seek on every form as its rank followed by its select (a Bitmap's masked word and
#   count-trailing-zeros, a List's compare-all and indexed field, an Elias-Fano bucket and its scan, a directory's
#   chunk and container), and so the leapfrog intersection of any two forms with no decode of either.
# - a_rank_is_inside_or_at_the_end: a rank is at most the cardinality, so `select(rank(v))` is asked only inside
#   the set or answered "none" by one compare.
# The laws need no ascending order: they hold of the search on any list; on an ascending list the search is the
# count of members below (list_probe.bend), which is the rank every form answers.
#
# Not modelled: the forms' own ranks and selects (bitmap_probe.bend, list_probe.bend, elias_fano.bend).
#
# Check: bend2 specs/bitmosaic/laws/seek.bend --verdict
import Base

def IsTrue(b: Bool) -> Data:
  match b:
    case True{}:
      Unit
    case False{}:
      Empty

def not(+b: Bool) -> Bool:
  match b:
    case True{}:
      False{}
    case False{}:
      True{}

def le(+a: Nat, +b: Nat) -> Bool:
  match a b:
    case 0n b0:
      True{}
    case 1n+p 0n:
      False{}
    case 1n+p 1n+q:
      le(p, q)

def lt(+a: Nat, +b: Nat) -> Bool:
  match a b:
    case 0n 0n:
      False{}
    case 0n 1n+q:
      True{}
    case 1n+p 0n:
      False{}
    case 1n+p 1n+q:
      lt(p, q)

def len(+xs: +List<Nat>) -> Nat:
  match xs:
    case Nil{}:
      0n
    case Con{x, t}:
      1n+len(t)

def go(+b: Bool, r: Nat) -> Nat:
  match b:
    case True{}:
      1n+r
    case False{}:
      0n

def rank(+xs: +List<Nat>, +v: Nat) -> Nat:
  match xs:
    case Nil{}:
      0n
    case Con{x, t}:
      go(lt(x, v), rank(t, v))

def at(+i: Nat, +xs: +List<Nat>) -> Nat:
  match i xs:
    case 0n Nil{}:
      0n
    case 0n Con{x, t}:
      x
    case 1n+j Nil{}:
      0n
    case 1n+j Con{x, t}:
      at(j, t)

def not_lt(+x: Nat, +v: Nat) -> IsTrue(not(lt(x, v))) -> IsTrue(le(v, x)):
  match x v:
    case 0n 0n:
      i => Unit{}
    case 0n 1n+q:
      i => i
    case 1n+p 0n:
      i => Unit{}
    case 1n+p 1n+q:
      not_lt(p, q)

def found(+c: Bool, +x: Nat, +t: +List<Nat>, +v: Nat, +r: Nat) -> (IsTrue(not(c)) -> IsTrue(le(v, x))) -> (IsTrue(lt(r, len(t))) -> IsTrue(le(v, at(r, t)))) -> IsTrue(lt(go(c, r), 1n+len(t))) -> IsTrue(le(v, at(go(c, r), Con{x, t}))):
  match c:
    case True{}:
      h => ih => i => ih(i)
    case False{}:
      h => ih => i => h(Unit{})

law the_member_at_the_rank_is_not_below:
  for +xs: +List<Nat>
  for +v: Nat
  for +i: IsTrue(lt(rank(xs, v), len(xs)))
  IsTrue(le(v, at(rank(xs, v), xs)))

def the_member_at_the_rank_is_not_below(xs, v, i):
  match xs:
    case Nil{}:
      Empty.absurd(IsTrue(le(v, at(rank(Nil{}, v), Nil{}))), i)
    case Con{x, t}:
      found(lt(x, v), x, t, v, rank(t, v), not_lt(x, v), j => the_member_at_the_rank_is_not_below(t, v, j), i)

def passed(+c: Bool, +i: Nat, +x: Nat, +t: +List<Nat>, +v: Nat, +r: Nat) -> (IsTrue(c) -> IsTrue(lt(x, v))) -> (@j:Nat -> IsTrue(lt(j, r)) -> IsTrue(lt(at(j, t), v))) -> IsTrue(lt(i, go(c, r))) -> IsTrue(lt(at(i, Con{x, t}), v)):
  match c i:
    case True{} 0n:
      h => ih => q => h(Unit{})
    case True{} 1n+j:
      h => ih => q => ih(j, q)
    case False{} 0n:
      h => ih => q => Empty.absurd(IsTrue(lt(at(0n, Con{x, t}), v)), q)
    case False{} 1n+j:
      h => ih => q => Empty.absurd(IsTrue(lt(at(1n+j, Con{x, t}), v)), q)

law members_before_the_rank_are_below:
  for +xs: +List<Nat>
  for +v: Nat
  for +i: Nat
  for +q: IsTrue(lt(i, rank(xs, v)))
  IsTrue(lt(at(i, xs), v))

def members_before_the_rank_are_below(xs, v, i, q):
  match xs i:
    case Nil{} 0n:
      Empty.absurd(IsTrue(lt(at(0n, Nil{}), v)), q)
    case Nil{} 1n+j:
      Empty.absurd(IsTrue(lt(at(1n+j, Nil{}), v)), q)
    case Con{x, t} i0:
      passed(lt(x, v), i0, x, t, v, rank(t, v), c => c, j => qq => members_before_the_rank_are_below(t, v, j, qq), q)

def inside(+c: Bool, +r: Nat, +n: Nat) -> IsTrue(le(r, n)) -> IsTrue(le(go(c, r), 1n+n)):
  match c:
    case True{}:
      i => i
    case False{}:
      i => Unit{}

law a_rank_is_inside_or_at_the_end:
  for +xs: +List<Nat>
  for +v: Nat
  IsTrue(le(rank(xs, v), len(xs)))

def a_rank_is_inside_or_at_the_end(xs, v):
  match xs:
    case Nil{}:
      Unit{}
    case Con{x, t}:
      inside(lt(x, v), rank(t, v), len(t), a_rank_is_inside_or_at_the_end(t, v))

law the_example:
  {(rank([3n, 7n, 9n], 5n), at(rank([3n, 7n, 9n], 5n), [3n, 7n, 9n]), rank([3n, 7n, 9n], 10n)) == (1n, 7n, 3n) : Nat & Nat & Nat}

def the_example():
  {==}