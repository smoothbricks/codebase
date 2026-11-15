# A set has one sealed image (specs/bitmosaic/01-format.md "The one image of a set"): each form has one spelling of
# a set, and the rule that picks the form has one answer. These laws are the two halves.
#
# One spelling per form. A set is its members ascending; as data it is its gaps: the first member, then the count of
# non-members between each member and the next. `bits(gs)` is the bitmap of the set with gaps `gs`, lowest position
# first; `gaps(bs, 0)` reads a bitmap back. A bitmap is trimmed when its last bit is set (`ends(bs, False)`): the
# format trims a Bitmap's trailing zero bytes and refuses a last byte of zero.
# - a_bitmap_reads_back_as_its_set: `gaps(bits(gs), 0) == gs`. Two sets with the same bitmap are the same set.
# - an_admitted_bitmap_is_the_written_one: a trimmed bitmap `bs` is `bits(gaps(bs, 0))`. So the bitmap a verified
#   reader admits for a set is the one the writer writes: the Bitmap form spells a set one way.
#   Together they LICENSE comparing and hashing Bitmap images as bytes: equal sets, equal bytes, and the reverse.
#   (Bit granularity: the format trims whole bytes; the last byte's high zero bits are positions, not padding.)
# - a_list_reads_back_as_its_members: a List writes its first member and each other member's distance from it
#   (`dist`); reading adds the first back (`back`). For members not below the first, `back(f, dist(f, xs)) == xs`.
# - distances_rank_like_members: for a probe `v` not below the first, the distances below `v - first` are as many
#   as the members below `v`. LICENSES the List reads on the stored distances: one subtraction of the first from
#   the probe, then the compare-all of list_probe.bend, and no member is rebuilt.
#
# One answer of the rule. The candidates' lengths are tried in the order Bitmap, List, directory, Elias-Fano;
# `pick(a, xs)` is the position chosen among `a` and the later `xs`: a later candidate is taken only when it is
# strictly shorter than everything before it.
# - the_pick_is_no_longer_than_any: no candidate is shorter than the chosen one.
# - the_first_wins_its_ties: a first candidate no longer than every later one is the choice.
# - a_longer_first_loses: a first candidate longer than some later one is not the choice.
#   Together: the choice is the earliest of the shortest, a function of the lengths alone. LICENSES a writer and a
#   verifier that compare closed-form lengths and never encode a candidate to try it.
# - a_list_is_longer_than_its_count, a_bitmap_within_its_count_beats_a_list: a List of `k` members is at least
#   `k + 1` bytes, so a set whose bitmap is at most `k + 1` bytes (its last member below `8 k`) is a Bitmap and the
#   List is not priced.
#
# Not modelled: bytes (a bitmap is a list of bits; a field's width is a number of bytes given, not derived from a
# value), and the directory's and Elias-Fano's lengths (numbers here; their formulas are checked against two other
# implementations by sizes.bend).
#
# Check: bend2 specs/bitmosaic/laws/one_image.bend --verdict
import Base

def IsTrue(b: Bool) -> Data:
  match b:
    case True{}:
      Unit
    case False{}:
      Empty

def add(+a: Nat, b: Nat) -> Nat:
  match a:
    case 0n:
      b
    case 1n+p:
      1n+add(p, b)

def sub(+a: Nat, +b: Nat) -> Nat:
  match a b:
    case 0n b0:
      0n
    case 1n+p 0n:
      1n+p
    case 1n+p 1n+q:
      sub(p, q)

def mul(+a: Nat, +b: Nat) -> Nat:
  match a:
    case 0n:
      0n
    case 1n+p:
      add(b, mul(p, b))

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
    case a0 0n:
      False{}
    case 0n 1n+q:
      True{}
    case 1n+p 1n+q:
      lt(p, q)

def app(-A: Data, +a: +List<A>, b: +List<A>) -> +List<A>:
  match a:
    case Nil{}:
      b
    case Con{h, t}:
      h <> app(A, t, b)

def falses(+k: Nat) -> +List<Bool>:
  match k:
    case 0n:
      []
    case 1n+j:
      False{} <> falses(j)

# --- the Bitmap form: a set as its gaps, and back ------------------------------------------

def bits(+gs: +List<Nat>) -> +List<Bool>:
  match gs:
    case Nil{}:
      []
    case Con{g, t}:
      app(Bool, falses(g), True{} <> bits(t))

def gaps(+bs: +List<Bool>, +k: Nat) -> +List<Nat>:
  match bs:
    case Nil{}:
      []
    case Con{False{}, t}:
      gaps(t, 1n+k)
    case Con{True{}, t}:
      k <> gaps(t, 0n)

def ends(+bs: +List<Bool>, +d: Bool) -> Bool:
  match bs:
    case Nil{}:
      d
    case Con{b, t}:
      ends(t, b)

def plus(+k: Nat, p: Nat) -> Nat:
  match k:
    case 0n:
      p
    case 1n+j:
      plus(j, 1n+p)

def plus_succ(+k: Nat, +p: Nat) -> {plus(k, 1n+p) == 1n+plus(k, p) : Nat}:
  match k:
    case 0n:
      {==}
    case 1n+j:
      plus_succ(j, 1n+p)

def plus_zero(+k: Nat) -> {plus(k, 0n) == k : Nat}:
  match k:
    case 0n:
      {==}
    case 1n+j:
      %plus_zero(j) : {plus(j, 1n) == 1n+_ : Nat}
      plus_succ(j, 0n)

def gaps_skip(+g: Nat, +r: +List<Bool>, +k: Nat) -> {gaps(app(Bool, falses(g), r), k) == gaps(r, plus(g, k)) : +List<Nat>}:
  match g:
    case 0n:
      {==}
    case 1n+j:
      gaps_skip(j, r, 1n+k)

law a_bitmap_reads_back_as_its_set:
  for +gs: +List<Nat>
  {gaps(bits(gs), 0n) == gs : +List<Nat>}

def a_bitmap_reads_back_as_its_set(gs):
  match gs:
    case Nil{}:
      {==}
    case Con{g, t}:
      %Equal.sym(+List<Nat>, gaps(app(Bool, falses(g), True{} <> bits(t)), 0n), gaps(True{} <> bits(t), plus(g, 0n)), gaps_skip(g, True{} <> bits(t), 0n)) : {_ == g <> t : +List<Nat>}
      %Equal.sym(Nat, plus(g, 0n), g, plus_zero(g)) : {_ <> gaps(bits(t), 0n) == g <> t : +List<Nat>}
      %Equal.sym(+List<Nat>, gaps(bits(t), 0n), t, a_bitmap_reads_back_as_its_set(t)) : {g <> _ == g <> t : +List<Nat>}
      {==}

def falses_shift(+k: Nat, +r: +List<Bool>) -> {False{} <> app(Bool, falses(k), r) == app(Bool, falses(k), False{} <> r) : +List<Bool>}:
  match k:
    case 0n:
      {==}
    case 1n+j:
      %falses_shift(j, r) : {False{} <> False{} <> app(Bool, falses(j), r) == False{} <> _ : +List<Bool>}
      {==}

def written(+t: +List<Bool>, +b: Bool, +k: Nat) -> IsTrue(ends(t, b)) -> {bits(gaps(Con{b, t}, k)) == app(Bool, falses(k), Con{b, t}) : +List<Bool>}:
  match t b:
    case Nil{} True{}:
      i => {==}
    case Con{c, u} True{}:
      i =>
        %written(u, c, 0n, i) : {app(Bool, falses(k), True{} <> bits(gaps(Con{c, u}, 0n))) == app(Bool, falses(k), True{} <> _) : +List<Bool>}
        {==}
    case Nil{} False{}:
      i => Empty.absurd({bits(gaps(Con{False{}, Nil{}}, k)) == app(Bool, falses(k), Con{False{}, Nil{}}) : +List<Bool>}, i)
    case Con{c, u} False{}:
      i =>
        %falses_shift(k, Con{c, u}) : {bits(gaps(Con{c, u}, 1n+k)) == _ : +List<Bool>}
        written(u, c, 1n+k, i)

law an_admitted_bitmap_is_the_written_one:
  for +b: Bool
  for +t: +List<Bool>
  for +i: IsTrue(ends(t, b))
  {bits(gaps(Con{b, t}, 0n)) == Con{b, t} : +List<Bool>}

def an_admitted_bitmap_is_the_written_one(b, t, i):
  written(t, b, 0n, i)

# --- the List form: the first member and the others' distances from it --------------------

def dist(+f: Nat, +xs: +List<Nat>) -> +List<Nat>:
  match xs:
    case Nil{}:
      []
    case Con{x, t}:
      sub(x, f) <> dist(f, t)

def back(+f: Nat, +ds: +List<Nat>) -> +List<Nat>:
  match ds:
    case Nil{}:
      []
    case Con{d, t}:
      add(f, d) <> back(f, t)

def both(+b: Bool, r: Bool) -> Bool:
  match b:
    case True{}:
      r
    case False{}:
      False{}

def step(+b: Bool, n: Nat) -> Nat:
  match b:
    case True{}:
      1n+n
    case False{}:
      n

def none_below(+f: Nat, +xs: +List<Nat>) -> Bool:
  match xs:
    case Nil{}:
      True{}
    case Con{x, t}:
      both(le(f, x), none_below(f, t))

def count_below(+xs: +List<Nat>, +v: Nat) -> Nat:
  match xs:
    case Nil{}:
      0n
    case Con{x, t}:
      step(lt(x, v), count_below(t, v))

def add_sub(+f: Nat, +x: Nat) -> IsTrue(le(f, x)) -> {add(f, sub(x, f)) == x : Nat}:
  match f x:
    case 0n 0n:
      i => {==}
    case 0n 1n+q:
      i => {==}
    case 1n+p 0n:
      i => Empty.absurd({add(1n+p, sub(0n, 1n+p)) == 0n : Nat}, i)
    case 1n+p 1n+q:
      i =>
        %add_sub(p, q, i) : {1n+add(p, sub(q, p)) == 1n+_ : Nat}
        {==}

def head_ok(+b: Bool, +r: Bool) -> IsTrue(both(b, r)) -> IsTrue(b):
  match b:
    case True{}:
      i => Unit{}
    case False{}:
      i => i

def tail_ok(+b: Bool, +r: Bool) -> IsTrue(both(b, r)) -> IsTrue(r):
  match b:
    case True{}:
      i => i
    case False{}:
      i => Empty.absurd(IsTrue(r), i)

law a_list_reads_back_as_its_members:
  for +f: Nat
  for +xs: +List<Nat>
  for +i: IsTrue(none_below(f, xs))
  {back(f, dist(f, xs)) == xs : +List<Nat>}

def a_list_reads_back_as_its_members(f, xs, i):
  match xs:
    case Nil{}:
      {==}
    case Con{x, t}:
      %a_list_reads_back_as_its_members(f, t, tail_ok(le(f, x), none_below(f, t), i)) : {add(f, sub(x, f)) <> back(f, dist(f, t)) == x <> _ : +List<Nat>}
      %add_sub(f, x, head_ok(le(f, x), none_below(f, t), i)) : {add(f, sub(x, f)) <> back(f, dist(f, t)) == _ <> back(f, dist(f, t)) : +List<Nat>}
      {==}

def lt_sub(+f: Nat, +x: Nat, +v: Nat) -> IsTrue(le(f, x)) -> IsTrue(le(f, v)) -> {lt(sub(x, f), sub(v, f)) == lt(x, v) : Bool}:
  match f x v:
    case 0n 0n 0n:
      i => j => {==}
    case 0n 0n 1n+w:
      i => j => {==}
    case 0n 1n+q 0n:
      i => j => {==}
    case 0n 1n+q 1n+w:
      i => j => {==}
    case 1n+p 0n v0:
      i => j => Empty.absurd({lt(sub(0n, 1n+p), sub(v0, 1n+p)) == lt(0n, v0) : Bool}, i)
    case 1n+p 1n+q 0n:
      i => j => Empty.absurd({lt(sub(1n+q, 1n+p), sub(0n, 1n+p)) == lt(1n+q, 0n) : Bool}, j)
    case 1n+p 1n+q 1n+w:
      lt_sub(p, q, w)

law distances_rank_like_members:
  for +f: Nat
  for +xs: +List<Nat>
  for +v: Nat
  for +i: IsTrue(none_below(f, xs))
  for +j: IsTrue(le(f, v))
  {count_below(dist(f, xs), sub(v, f)) == count_below(xs, v) : Nat}

def distances_rank_like_members(f, xs, v, i, j):
  match xs:
    case Nil{}:
      {==}
    case Con{x, t}:
      %distances_rank_like_members(f, t, v, tail_ok(le(f, x), none_below(f, t), i), j) : {step(lt(sub(x, f), sub(v, f)), count_below(dist(f, t), sub(v, f))) == step(lt(x, v), _) : Nat}
      %lt_sub(f, x, v, head_ok(le(f, x), none_below(f, t), i), j) : {step(lt(sub(x, f), sub(v, f)), count_below(dist(f, t), sub(v, f))) == step(_, count_below(dist(f, t), sub(v, f))) : Nat}
      {==}

# --- the rule: the earliest of the shortest ---------------------------------------------------

def min(+a: Nat, +b: Nat) -> Nat:
  match a b:
    case 0n b0:
      0n
    case 1n+p 0n:
      0n
    case 1n+p 1n+q:
      1n+min(p, q)

def best(+xs: +List<Nat>, +a: Nat) -> Nat:
  match xs:
    case Nil{}:
      a
    case Con{x, t}:
      min(a, best(t, x))

def choose(+keep: Bool, later: Nat) -> Nat:
  match keep:
    case True{}:
      0n
    case False{}:
      1n+later

def pick(+xs: +List<Nat>, +a: Nat) -> Nat:
  match xs:
    case Nil{}:
      0n
    case Con{x, t}:
      choose(le(a, best(t, x)), pick(t, x))

def nth(+i: Nat, +xs: +List<Nat>, +a: Nat) -> Nat:
  match i xs:
    case 0n xs0:
      a
    case 1n+j Nil{}:
      a
    case 1n+j Con{x, t}:
      nth(j, t, x)

def sel(+c: Bool, a: Nat, m: Nat) -> Nat:
  match c:
    case True{}:
      a
    case False{}:
      m

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

def sel_succ(+c: Bool, +p: Nat, +q: Nat) -> {sel(c, 1n+p, 1n+q) == 1n+sel(c, p, q) : Nat}:
  match c:
    case True{}:
      {==}
    case False{}:
      {==}

def min_sel(+a: Nat, +m: Nat) -> {sel(le(a, m), a, m) == min(a, m) : Nat}:
  match a m:
    case 0n m0:
      {==}
    case 1n+p 0n:
      {==}
    case 1n+p 1n+q:
      %min_sel(p, q) : {sel(le(p, q), 1n+p, 1n+q) == 1n+_ : Nat}
      sel_succ(le(p, q), p, q)

def nth_choose(+c: Bool, +x: Nat, +t: +List<Nat>, +a: Nat, +p: Nat) -> {nth(choose(c, p), Con{x, t}, a) == sel(c, a, nth(p, t, x)) : Nat}:
  match c:
    case True{}:
      {==}
    case False{}:
      {==}

def pick_is_best(+xs: +List<Nat>, +a: Nat) -> {nth(pick(xs, a), xs, a) == best(xs, a) : Nat}:
  match xs:
    case Nil{}:
      {==}
    case Con{x, t}:
      %min_sel(a, best(t, x)) : {nth(choose(le(a, best(t, x)), pick(t, x)), Con{x, t}, a) == _ : Nat}
      %pick_is_best(t, x) : {nth(choose(le(a, best(t, x)), pick(t, x)), Con{x, t}, a) == sel(le(a, best(t, x)), a, _) : Nat}
      nth_choose(le(a, best(t, x)), x, t, a, pick(t, x))

def best_le(+i: Nat, +xs: +List<Nat>, +a: Nat) -> IsTrue(le(best(xs, a), nth(i, xs, a))):
  match i xs:
    case 0n Nil{}:
      le_refl(a)
    case 0n Con{x, t}:
      le_min_l(a, best(t, x))
    case 1n+j Nil{}:
      le_refl(a)
    case 1n+j Con{x, t}:
      le_trans(min(a, best(t, x)), best(t, x), nth(j, t, x), le_min_r(a, best(t, x)), best_le(j, t, x))

law the_pick_is_no_longer_than_any:
  for +xs: +List<Nat>
  for +a: Nat
  for +i: Nat
  IsTrue(le(nth(pick(xs, a), xs, a), nth(i, xs, a)))

def the_pick_is_no_longer_than_any(xs, a, i):
  %Equal.sym(Nat, nth(pick(xs, a), xs, a), best(xs, a), pick_is_best(xs, a)) : IsTrue(le(_, nth(i, xs, a)))
  best_le(i, xs, a)

def choose_kept(+c: Bool, +p: Nat) -> IsTrue(c) -> {choose(c, p) == 0n : Nat}:
  match c:
    case True{}:
      i => {==}
    case False{}:
      i => Empty.absurd({choose(False{}, p) == 0n : Nat}, i)

law the_first_wins_its_ties:
  for +x: Nat
  for +t: +List<Nat>
  for +a: Nat
  for +i: IsTrue(le(a, best(t, x)))
  {pick(Con{x, t}, a) == 0n : Nat}

def the_first_wins_its_ties(x, t, a, i):
  choose_kept(le(a, best(t, x)), pick(t, x), i)

def choose_passed(+a: Nat, +m: Nat, +p: Nat) -> IsTrue(lt(m, a)) -> {choose(le(a, m), p) == 1n+p : Nat}:
  match a m:
    case 0n 0n:
      i => Empty.absurd({choose(le(0n, 0n), p) == 1n+p : Nat}, i)
    case 0n 1n+s:
      i => Empty.absurd({choose(le(0n, 1n+s), p) == 1n+p : Nat}, i)
    case 1n+q 0n:
      i => {==}
    case 1n+q 1n+s:
      choose_passed(q, s, p)

law a_longer_first_loses:
  for +x: Nat
  for +t: +List<Nat>
  for +a: Nat
  for +i: IsTrue(lt(best(t, x), a))
  {pick(Con{x, t}, a) == 1n+pick(t, x) : Nat}

def a_longer_first_loses(x, t, a, i):
  choose_passed(a, best(t, x), pick(t, x), i)

# --- a Bitmap against a List ------------------------------------------------------------------

def le_mul(+j: Nat, +d: Nat) -> IsTrue(le(j, mul(j, 1n+d))):
  match j:
    case 0n:
      Unit{}
    case 1n+p:
      le_trans(p, mul(p, 1n+d), add(d, mul(p, 1n+d)), le_mul(p, d), le_addl(d, mul(p, 1n+d)))

law a_list_is_longer_than_its_count:
  for +a: Nat
  for +d: Nat
  for +j: Nat
  IsTrue(le(add(2n, j), add(1n, add(1n+a, mul(j, 1n+d)))))

def a_list_is_longer_than_its_count(a, d, j):
  le_trans(j, mul(j, 1n+d), add(a, mul(j, 1n+d)), le_mul(j, d), le_addl(a, mul(j, 1n+d)))

law a_bitmap_within_its_count_beats_a_list:
  for +e: Nat
  for +a: Nat
  for +d: Nat
  for +j: Nat
  for +i: IsTrue(le(e, j))
  IsTrue(le(add(2n, e), add(1n, add(1n+a, mul(j, 1n+d)))))

def a_bitmap_within_its_count_beats_a_list(e, a, d, j, i):
  le_trans(e, j, add(a, mul(j, 1n+d)), i, a_list_is_longer_than_its_count(a, d, j))

law the_examples:
  {(gaps(bits([0n, 0n, 0n, 7n]), 0n), pick([4n, 9n, 4n], 5n), pick([4n, 9n, 4n], 4n), back(7n, dist(7n, [7n, 200n]))) == ([0n, 0n, 0n, 7n], 1n, 0n, [7n, 200n]) : +List<Nat> & Nat & Nat & +List<Nat>}

def the_examples():
  {==}