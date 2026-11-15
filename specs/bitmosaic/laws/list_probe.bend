# The List form of a bitmosaic image (specs/bitmosaic/01-format.md "List") holds at most eight members ascending:
# the first, then the others as their distance from it. A reader of an ascending list would search it: stop at the
# first member not below the probe. These laws say the search is not needed.
#
# The model: a list is its members as `Nat`, `asc` says each is below the next. `rank` and `found` are the search
# (they stop at the first member not below `v`); `count_below` and `holds` look at every member and never branch on
# one.
#
# Proven here, for every ascending list and every probe:
# - count_below_is_rank: the number of members below `v` is the position the search stops at.
#   LICENSES the List rank of 01-format.md "Reads": compare all members and add the answers: no search, no early
#   exit, no branch that depends on a member.
# - or_of_equals_is_contains: some member equals `v` exactly when the search finds `v`.
#   LICENSES the List contains: membership is the OR of every member's equality.
# Both need `asc`: an unverified list answers the count and the OR, which is why those are the definition of a
# list's rank and membership, and the search is not.
# The distances are covered by one_image.bend (`a_list_reads_back_as_its_members`): a member is the first plus its
# distance, so comparing distances against `v - first` is comparing members against `v`.
#
# Not modelled: that a field is one to four bytes read little-endian, and that the compiler's vector compare equals
# the scalar one. Bend2 has `U32` as 32 `Bool`s and no byte arrays; the laws are over `Nat`, and the reads are held
# to them by the conformance vectors.
#
# Check: bend2 specs/bitmosaic/laws/list_probe.bend --verdict
import Base

def lt(+a: Nat, +b: Nat) -> Bool:
  match a b:
    case a0 0n:
      False{}
    case 0n 1n+q:
      True{}
    case 1n+p 1n+q:
      lt(p, q)

def bit(+b: Bool) -> Nat:
  match b:
    case True{}:
      1n
    case False{}:
      0n

def tally(+b: Bool, n: Nat) -> Nat:
  match b:
    case True{}:
      1n+n
    case False{}:
      n

def count_below(+xs: +List<Nat>, +v: Nat) -> Nat:
  match xs:
    case Nil{}:
      0n
    case Con{x, t}:
      tally(lt(x, v), count_below(t, v))

def rank.at(+below: Bool, rest: Nat) -> Nat:
  match below:
    case True{}:
      1n+rest
    case False{}:
      0n

def rank(+xs: +List<Nat>, +v: Nat) -> Nat:
  match xs:
    case Nil{}:
      0n
    case Con{x, t}:
      rank.at(lt(x, v), rank(t, v))

def IsTrue(b: Bool) -> Data:
  match b:
    case True{}:
      Unit
    case False{}:
      Empty

def both(+a: Bool, b: Bool) -> Bool:
  match a:
    case True{}:
      b
    case False{}:
      False{}

def asc(+xs: +List<Nat>) -> Bool:
  match xs:
    case Nil{}:
      True{}
    case Con{x, Nil{}}:
      True{}
    case Con{x, Con{y, t}}:
      both(lt(x, y), asc(Con{y, t}))

def IsFalse(b: Bool) -> Data:
  match b:
    case True{}:
      Empty
    case False{}:
      Unit

def lt_up(+x: Nat, +y: Nat, +v: Nat) -> IsTrue(lt(x, y)) -> IsFalse(lt(x, v)) -> IsFalse(lt(y, v)):
  match x y v:
    case x0 0n v0:
      a => b => Empty.absurd(IsFalse(lt(0n, v0)), a)
    case x0 1n+q 0n:
      a => b => Unit{}
    case 0n 1n+q 1n+w:
      a => b => Empty.absurd(IsFalse(lt(1n+q, 1n+w)), b)
    case 1n+p 1n+q 1n+w:
      lt_up(p, q, w)

def both_fst(+a: Bool, +b: Bool) -> IsTrue(both(a, b)) -> IsTrue(a):
  match a:
    case True{}:
      i => Unit{}
    case False{}:
      i => i

def both_snd(+a: Bool, +b: Bool) -> IsTrue(both(a, b)) -> IsTrue(b):
  match a:
    case True{}:
      i => i
    case False{}:
      i => Empty.absurd(IsTrue(b), i)

def tally_none(+b: Bool, +n: Nat) -> IsFalse(b) -> {n == 0n : Nat} -> {tally(b, n) == 0n : Nat}:
  match b:
    case True{}:
      i => e => Empty.absurd({tally(True{}, n) == 0n : Nat}, i)
    case False{}:
      i => e => e

def none_below(+t: +List<Nat>, +x: Nat, +v: Nat) -> IsTrue(asc(Con{x, t})) -> IsFalse(lt(x, v)) -> {count_below(t, v) == 0n : Nat}:
  match t:
    case Nil{}:
      a => b => {==}
    case Con{y, u}:
      +a => b =>
        +yv = lt_up(x, y, v, both_fst(lt(x, y), asc(Con{y, u}), a), b)
        tally_none(lt(y, v), count_below(u, v), yv, none_below(u, y, v, both_snd(lt(x, y), asc(Con{y, u}), a), yv))

def agree(+b: Bool, +c: Nat, +r: Nat) -> (IsFalse(b) -> {c == 0n : Nat}) -> {c == r : Nat} -> {tally(b, c) == rank.at(b, r) : Nat}:
  match b:
    case True{}:
      z => e =>
        %e : {1n+c == 1n+_ : Nat}
        {==}
    case False{}:
      z => e => z(Unit{})

law count_below_is_rank:
  for +xs: +List<Nat>
  for +v: Nat
  for +a: IsTrue(asc(xs))
  {count_below(xs, v) == rank(xs, v) : Nat}

def asc_tail(+x: Nat, +t: +List<Nat>) -> IsTrue(asc(Con{x, t})) -> IsTrue(asc(t)):
  match t:
    case Nil{}:
      a => Unit{}
    case Con{y, u}:
      both_snd(lt(x, y), asc(Con{y, u}))

def count_below_is_rank(xs, v, a):
  match xs:
    case Nil{}:
      {==}
    case Con{x, t}:
      agree(lt(x, v), count_below(t, v), rank(t, v), none_below(t, x, v, a), count_below_is_rank(t, v, asc_tail(x, t, a)))

def eq(+a: Nat, +b: Nat) -> Bool:
  match a b:
    case 0n 0n:
      True{}
    case 0n 1n+q:
      False{}
    case 1n+p 0n:
      False{}
    case 1n+p 1n+q:
      eq(p, q)

def either(+a: Bool, b: Bool) -> Bool:
  match a:
    case True{}:
      True{}
    case False{}:
      b

def holds(+xs: +List<Nat>, +v: Nat) -> Bool:
  match xs:
    case Nil{}:
      False{}
    case Con{x, t}:
      either(eq(x, v), holds(t, v))

def found.at(+below: Bool, same: Bool, later: Bool) -> Bool:
  match below:
    case True{}:
      later
    case False{}:
      same

def found(+xs: +List<Nat>, +v: Nat) -> Bool:
  match xs:
    case Nil{}:
      False{}
    case Con{x, t}:
      found.at(lt(x, v), eq(x, v), found(t, v))

def lt_ne(+x: Nat, +v: Nat) -> IsTrue(lt(x, v)) -> IsFalse(eq(x, v)):
  match x v:
    case 0n 0n:
      a => a
    case 0n 1n+w:
      a => Unit{}
    case 1n+p 0n:
      a => Unit{}
    case 1n+p 1n+w:
      lt_ne(p, w)

def past_ne(+x: Nat, +y: Nat, +v: Nat) -> IsTrue(lt(x, y)) -> IsFalse(lt(x, v)) -> IsFalse(eq(y, v)):
  match x y v:
    case x0 0n v0:
      a => b => Empty.absurd(IsFalse(eq(0n, v0)), a)
    case x0 1n+q 0n:
      a => b => Unit{}
    case 0n 1n+q 1n+w:
      a => b => Empty.absurd(IsFalse(eq(1n+q, 1n+w)), b)
    case 1n+p 1n+q 1n+w:
      past_ne(p, q, w)

def either_none(+a: Bool, +b: Bool) -> IsFalse(a) -> {b == False{} : Bool} -> {either(a, b) == False{} : Bool}:
  match a:
    case True{}:
      i => e => Empty.absurd({either(True{}, b) == False{} : Bool}, i)
    case False{}:
      i => e => e

def none_holds(+t: +List<Nat>, +x: Nat, +v: Nat) -> IsTrue(asc(Con{x, t})) -> IsFalse(lt(x, v)) -> {holds(t, v) == False{} : Bool}:
  match t:
    case Nil{}:
      a => b => {==}
    case Con{y, u}:
      +a => +b =>
        +xy = both_fst(lt(x, y), asc(Con{y, u}), a)
        either_none(eq(y, v), holds(u, v), past_ne(x, y, v, xy, b), none_holds(u, y, v, both_snd(lt(x, y), asc(Con{y, u}), a), lt_up(x, y, v, xy, b)))

def agree_holds(+below: Bool, +same: Bool, +h: Bool, +f: Bool) -> (IsTrue(below) -> IsFalse(same)) -> (IsFalse(below) -> {h == False{} : Bool}) -> {h == f : Bool} -> {either(same, h) == found.at(below, same, f) : Bool}:
  match below same:
    case True{} True{}:
      n => z => e => Empty.absurd({either(True{}, h) == found.at(True{}, True{}, f) : Bool}, n(Unit{}))
    case True{} False{}:
      n => z => e => e
    case False{} True{}:
      n => z => e => {==}
    case False{} False{}:
      n => z => e => z(Unit{})

law or_of_equals_is_contains:
  for +xs: +List<Nat>
  for +v: Nat
  for +a: IsTrue(asc(xs))
  {holds(xs, v) == found(xs, v) : Bool}

def or_of_equals_is_contains(xs, v, a):
  match xs:
    case Nil{}:
      {==}
    case Con{x, t}:
      agree_holds(lt(x, v), eq(x, v), holds(t, v), found(t, v), lt_ne(x, v), none_holds(t, x, v, a), or_of_equals_is_contains(t, v, asc_tail(x, t, a)))

law the_example:
  {(count_below([1n, 3n, 7n], 4n), rank([1n, 3n, 7n], 4n), holds([1n, 3n, 7n], 3n), holds([1n, 3n, 7n], 4n)) == (2n, 2n, True{}, False{}) : Nat & Nat & Bool & Bool}

def the_example():
  {==}
