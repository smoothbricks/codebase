# Fast-lane laws: L0 single-descent `rank_if_present`, and `AndNotRange` seek-skipping.
#
# 1. `rank_if_present` (L0 byproduct): one descent answers membership and rank together.
#    Proven here, for every list and every value:
#    - `rank_if_present_matches_contains_and_rank`: `rank_if_present(xs, v)` equals
#      `Hit{rank_asc(xs, v)}` when `contains_asc(xs, v)` holds and `Miss{}` otherwise
#      (no second descent is needed, and the two answers cannot disagree).
#
# 2. `AndNotRange::align()` seek skip: advancing the subtrahend `b` past every element
#    strictly below `x` in one `seek(x)` call produces the exact same difference as
#    stepping `b.pop_front()` one element at a time.
#    Proven here, for every value `x` and list `b`:
#    - `andnot_seek_matches_step`: `align_step(x, b)` equals
#      `align_after_seek(x, drop_below(b, x))`.
#
# Check: bend2 specs/bitmosaic/laws/fast_lanes.bend --verdict
import Base

type RankHit is Data:
  Miss{}
  Hit{rank: Nat}

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

def bump_hit(+m: RankHit) -> RankHit:
  match m:
    case Miss{}:
      Miss{}
    case Hit{r}:
      Hit{1n+r}

def when_true(+b: Bool, r: Nat) -> RankHit:
  match b:
    case True{}:
      Hit{r}
    case False{}:
      Miss{}

def go(+b: Bool, r: Nat) -> Nat:
  match b:
    case True{}:
      1n+r
    case False{}:
      0n

# Separate two-descent specification: `rank_asc` and `contains_asc`.
def rank_asc(+xs: +List<Nat>, +v: Nat) -> Nat:
  match xs:
    case Nil{}:
      0n
    case Con{x, t}:
      go(lt(x, v), rank_asc(t, v))

def contains_step(+e: Bool, +l: Bool, rest: Bool) -> Bool:
  match e:
    case True{}:
      True{}
    case False{}:
      match l:
        case True{}:
          rest
        case False{}:
          False{}

def contains_asc(+xs: +List<Nat>, +v: Nat) -> Bool:
  match xs:
    case Nil{}:
      False{}
    case Con{x, t}:
      contains_step(eq(x, v), lt(x, v), contains_asc(t, v))

# Single-descent L0 byproduct: `rank_if_present`.
def rip_step(+e: Bool, +l: Bool, rest: RankHit) -> RankHit:
  match e:
    case True{}:
      Hit{0n}
    case False{}:
      match l:
        case True{}:
          bump_hit(rest)
        case False{}:
          Miss{}

def rank_if_present(+xs: +List<Nat>, +v: Nat) -> RankHit:
  match xs:
    case Nil{}:
      Miss{}
    case Con{x, t}:
      rip_step(eq(x, v), lt(x, v), rank_if_present(t, v))

def eq_implies_not_lt(+x: Nat, +v: Nat) -> IsTrue(eq(x, v)) -> IsFalse(lt(x, v)):
  match x v:
    case 0n 0n:
      h => Unit{}
    case 0n 1n+q:
      h => Empty.absurd(IsFalse(lt(0n, 1n+q)), h)
    case 1n+p 0n:
      h => Empty.absurd(IsFalse(lt(1n+p, 0n)), h)
    case 1n+p 1n+q:
      eq_implies_not_lt(p, q)

def bump_when(+c: Bool, +r: Nat) -> {bump_hit(when_true(c, r)) == when_true(c, 1n+r) : RankHit}:
  match c:
    case True{}:
      {==}
    case False{}:
      {==}

def step_case(+e: Bool, +l: Bool, +c: Bool, +r: Nat, +m: RankHit)
  -> (IsTrue(e) -> IsFalse(l))
  -> {m == when_true(c, r) : RankHit}
  -> {rip_step(e, l, m) == when_true(contains_step(e, l, c), go(l, r)) : RankHit}:
  match e l:
    case True{} True{}:
      hl => hm => Empty.absurd({rip_step(True{}, True{}, m) == when_true(contains_step(True{}, True{}, c), go(True{}, r)) : RankHit}, hl(Unit{}))
    case True{} False{}:
      hl => hm => {==}
    case False{} True{}:
      hl => hm =>
        %Equal.sym(RankHit, m, when_true(c, r), hm) : {bump_hit(_) == when_true(c, 1n+r) : RankHit}
        bump_when(c, r)
    case False{} False{}:
      hl => hm => {==}

law rank_if_present_matches_contains_and_rank:
  for +xs: +List<Nat>
  for +v: Nat
  {rank_if_present(xs, v) == when_true(contains_asc(xs, v), rank_asc(xs, v)) : RankHit}

def rank_if_present_matches_contains_and_rank(xs, v):
  match xs:
    case Nil{}:
      {==}
    case Con{x, t}:
      step_case(
        eq(x, v),
        lt(x, v),
        contains_asc(t, v),
        rank_asc(t, v),
        rank_if_present(t, v),
        eq_implies_not_lt(x, v),
        rank_if_present_matches_contains_and_rank(t, v))

# ── AndNotRange seek-skip law ─────────────────────────────────────────────

def drop_below_step(+l: Bool, +y: Nat, +bs: +List<Nat>, rest: +List<Nat>) -> +List<Nat>:
  match l:
    case True{}:
      rest
    case False{}:
      Con{y, bs}

def drop_below(+b: +List<Nat>, +x: Nat) -> +List<Nat>:
  match b:
    case Nil{}:
      Nil{}
    case Con{y, bs}:
      drop_below_step(lt(y, x), y, bs, drop_below(bs, x))

def not_bool(+b: Bool) -> Bool:
  match b:
    case True{}:
      False{}
    case False{}:
      True{}

def align_decide(+l: Bool, +e: Bool, rest: Bool) -> Bool:
  match l:
    case True{}:
      rest
    case False{}:
      not_bool(e)

def align_step(+x: Nat, +b: +List<Nat>) -> Bool:
  match b:
    case Nil{}:
      True{}
    case Con{y, bs}:
      align_decide(lt(y, x), eq(y, x), align_step(x, bs))

def align_after_seek(+x: Nat, +b: +List<Nat>) -> Bool:
  match b:
    case Nil{}:
      True{}
    case Con{y, bs}:
      not_bool(eq(y, x))

def align_branch(+l: Bool, +x: Nat, +y: Nat, +bs: +List<Nat>, +rest_drop: +List<Nat>)
  -> {align_step(x, bs) == align_after_seek(x, rest_drop) : Bool}
  -> {align_decide(l, eq(y, x), align_step(x, bs)) == align_after_seek(x, drop_below_step(l, y, bs, rest_drop)) : Bool}:
  match l:
    case True{}:
      ih => ih
    case False{}:
      ih => {==}

law andnot_seek_matches_step:
  for +x: Nat
  for +b: +List<Nat>
  {align_step(x, b) == align_after_seek(x, drop_below(b, x)) : Bool}

def andnot_seek_matches_step(x, b):
  match b:
    case Nil{}:
      {==}
    case Con{y, bs}:
      align_branch(lt(y, x), x, y, bs, drop_below(bs, x), andnot_seek_matches_step(x, bs))

law fast_lanes_examples:
  {(rank_if_present([3n, 7n, 9n], 7n), rank_if_present([3n, 7n, 9n], 5n), align_step(7n, [1n, 3n, 7n, 9n]), align_step(7n, [1n, 3n, 8n])) == (Hit{1n}, Miss{}, False{}, True{}) : RankHit & RankHit & Bool & Bool}

def fast_lanes_examples():
  {==}
