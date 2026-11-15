# The Elias-Fano form of a bitmosaic image (specs/bitmosaic/01-format.md "Elias-Fano"). Each member is taken as its
# distance `d` from the first; `d` is split at `L` bits: the low `L` bits go to the low plane as a field, and the
# high part `h` (the member's bucket) to the high plane, where member `j` sets bit `h_j + j`. So the high plane
# holds, for each member in turn, as many zeros as its bucket lies past the one before, then a one.
#
# The model: the high plane is `bits(gs)` for the bucket steps `gs` (the first member's bucket, then each bucket
# less the one before), lowest position first. `ones(p, bs)` and `zeros(p, bs)` count below position `p`; `sel`
# is the position of the `k`-th one and `selz` of the `k`-th zero; `upto(j, gs)` is the bucket of member `j` (the
# steps up to it, added). A value's bits are a list, lowest first; `take` and `drop` split it at `L`.
#
# Proven here:
# - the_high_plane_reads_back_as_its_buckets: `gaps(bits(gs), 0) == gs`: the plane determines every bucket.
# - a_value_is_its_low_bits_then_its_high_bits: `app(take(l, bs), drop(l, bs)) == bs`: the split loses nothing.
#   With the first law: an Elias-Fano image reads back as its set (the round trip), stated over bit lists.
# - ones_and_zeros_fill_a_prefix: below any position inside the plane, the ones and the zeros add up to the
#   position. LICENSES the two subtractions of the reads: a member found at position `p` with `j` ones below it has
#   the bucket `p - j`; the bucket `h` that starts at position `s` has `s - h` members before it.
# - the_zeros_before_a_member_are_its_bucket: the zeros below the `j`-th one are `upto(j, gs)`. LICENSES select:
#   member `j` is `(sel(j) - j) << L | low_j`, with no walk over the members before it.
# - a_zero_select_stays_in_the_plane, a_one_select_stays_in_the_plane: the `k`-th zero (one) of a plane that holds
#   more than `k` zeros (ones) lies inside it. A verified plane holds exactly `buckets` zeros and `n` ones, so the
#   select of a bucket below `buckets` or a member below `n`, and the scan of a bucket up to its closing zero, read
#   inside the plane: the high-plane half of "verified, so every read is in bounds".
# - a_field_ends_inside_its_plane: field `j < c` of width `l` ends at or before `c * l` bits. The low-plane half,
#   and the List form's (field `j` of `k - 1` fields of `wd` bytes).
#
# Not modelled: `L`'s closed form (it is evaluated, with the lengths it gives, by sizes.bend against two other
# implementations), bytes and 64-bit words (planes are bit lists; a clipped word load is an implementation's), and
# the samples (positions of every 64th one and zero, verified against the plane at admission; they shorten a
# select's scan and change no answer).
#
# Check: bend2 specs/bitmosaic/laws/elias_fano.bend --verdict
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
    case 0n 0n:
      False{}
    case 0n 1n+q:
      True{}
    case 1n+p 0n:
      False{}
    case 1n+p 1n+q:
      lt(p, q)

def app(-A: Data, +a: +List<A>, b: +List<A>) -> +List<A>:
  match a:
    case Nil{}:
      b
    case Con{h, t}:
      h <> app(A, t, b)

def len(-A: Data, +a: +List<A>) -> Nat:
  match a:
    case Nil{}:
      0n
    case Con{h, t}:
      1n+len(A, t)

def falses(+k: Nat) -> +List<Bool>:
  match k:
    case 0n:
      []
    case 1n+j:
      False{} <> falses(j)

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

law the_high_plane_reads_back_as_its_buckets:
  for +gs: +List<Nat>
  {gaps(bits(gs), 0n) == gs : +List<Nat>}

def the_high_plane_reads_back_as_its_buckets(gs):
  match gs:
    case Nil{}:
      {==}
    case Con{g, t}:
      %Equal.sym(+List<Nat>, gaps(app(Bool, falses(g), True{} <> bits(t)), 0n), gaps(True{} <> bits(t), plus(g, 0n)), gaps_skip(g, True{} <> bits(t), 0n)) : {_ == g <> t : +List<Nat>}
      %Equal.sym(Nat, plus(g, 0n), g, plus_zero(g)) : {_ <> gaps(bits(t), 0n) == g <> t : +List<Nat>}
      %Equal.sym(+List<Nat>, gaps(bits(t), 0n), t, the_high_plane_reads_back_as_its_buckets(t)) : {g <> _ == g <> t : +List<Nat>}
      {==}

# --- the split of a value at L bits ---------------------------------------------------------

def take(+l: Nat, +bs: +List<Bool>) -> +List<Bool>:
  match l bs:
    case 0n bs0:
      []
    case 1n+k Nil{}:
      []
    case 1n+k Con{b, t}:
      b <> take(k, t)

def drop(+l: Nat, +bs: +List<Bool>) -> +List<Bool>:
  match l bs:
    case 0n bs0:
      bs0
    case 1n+k Nil{}:
      []
    case 1n+k Con{b, t}:
      drop(k, t)

law a_value_is_its_low_bits_then_its_high_bits:
  for +l: Nat
  for +bs: +List<Bool>
  {app(Bool, take(l, bs), drop(l, bs)) == bs : +List<Bool>}

def a_value_is_its_low_bits_then_its_high_bits(l, bs):
  match l bs:
    case 0n bs0:
      {==}
    case 1n+k Nil{}:
      {==}
    case 1n+k Con{b, t}:
      %a_value_is_its_low_bits_then_its_high_bits(k, t) : {b <> app(Bool, take(k, t), drop(k, t)) == b <> _ : +List<Bool>}
      {==}

# --- counting in the high plane -------------------------------------------------------------

def step(+b: Bool, n: Nat) -> Nat:
  match b:
    case True{}:
      1n+n
    case False{}:
      n

def not(+b: Bool) -> Bool:
  match b:
    case True{}:
      False{}
    case False{}:
      True{}

def ones(+p: Nat, +bs: +List<Bool>) -> Nat:
  match p bs:
    case 0n bs0:
      0n
    case 1n+q Nil{}:
      0n
    case 1n+q Con{b, t}:
      step(b, ones(q, t))

def zeros(+p: Nat, +bs: +List<Bool>) -> Nat:
  match p bs:
    case 0n bs0:
      0n
    case 1n+q Nil{}:
      0n
    case 1n+q Con{b, t}:
      step(not(b), zeros(q, t))

def add_succ(+a: Nat, +b: Nat) -> {add(a, 1n+b) == 1n+add(a, b) : Nat}:
  match a:
    case 0n:
      {==}
    case 1n+p:
      %add_succ(p, b) : {1n+add(p, 1n+b) == 1n+_ : Nat}
      {==}

def fill(+b: Bool, +x: Nat, +y: Nat, +q: Nat) -> {add(x, y) == q : Nat} -> {add(step(b, x), step(not(b), y)) == 1n+q : Nat}:
  match b:
    case True{}:
      e =>
        %e : {1n+add(x, y) == 1n+_ : Nat}
        {==}
    case False{}:
      e =>
        %e : {add(x, 1n+y) == 1n+_ : Nat}
        add_succ(x, y)

law ones_and_zeros_fill_a_prefix:
  for +p: Nat
  for +bs: +List<Bool>
  for +i: IsTrue(le(p, len(Bool, bs)))
  {add(ones(p, bs), zeros(p, bs)) == p : Nat}

def ones_and_zeros_fill_a_prefix(p, bs, i):
  match p bs:
    case 0n bs0:
      {==}
    case 1n+q Nil{}:
      Empty.absurd({add(ones(1n+q, Nil{}), zeros(1n+q, Nil{})) == 1n+q : Nat}, i)
    case 1n+q Con{b, t}:
      fill(b, ones(q, t), zeros(q, t), q, ones_and_zeros_fill_a_prefix(q, t, i))

# --- select in the high plane ---------------------------------------------------------------

def count(+bs: +List<Bool>) -> Nat:
  match bs:
    case Nil{}:
      0n
    case Con{b, t}:
      step(b, count(t))

def countz(+bs: +List<Bool>) -> Nat:
  match bs:
    case Nil{}:
      0n
    case Con{b, t}:
      step(not(b), countz(t))

def sel(+bs: +List<Bool>, +k: Nat) -> Nat:
  match bs k:
    case Nil{} k0:
      0n
    case Con{True{}, t} 0n:
      0n
    case Con{True{}, t} 1n+j:
      1n+sel(t, j)
    case Con{False{}, t} k0:
      1n+sel(t, k0)

def selz(+bs: +List<Bool>, +k: Nat) -> Nat:
  match bs k:
    case Nil{} k0:
      0n
    case Con{False{}, t} 0n:
      0n
    case Con{False{}, t} 1n+j:
      1n+selz(t, j)
    case Con{True{}, t} k0:
      1n+selz(t, k0)

law a_one_select_stays_in_the_plane:
  for +bs: +List<Bool>
  for +k: Nat
  for +i: IsTrue(lt(k, count(bs)))
  IsTrue(lt(sel(bs, k), len(Bool, bs)))

def a_one_select_stays_in_the_plane(bs, k, i):
  match bs k:
    case Nil{} 0n:
      Empty.absurd(IsTrue(lt(sel(Nil{}, 0n), len(Bool, Nil{}))), i)
    case Nil{} 1n+j:
      Empty.absurd(IsTrue(lt(sel(Nil{}, 1n+j), len(Bool, Nil{}))), i)
    case Con{True{}, t} 0n:
      Unit{}
    case Con{True{}, t} 1n+j:
      a_one_select_stays_in_the_plane(t, j, i)
    case Con{False{}, t} k0:
      a_one_select_stays_in_the_plane(t, k0, i)

law a_zero_select_stays_in_the_plane:
  for +bs: +List<Bool>
  for +k: Nat
  for +i: IsTrue(lt(k, countz(bs)))
  IsTrue(lt(selz(bs, k), len(Bool, bs)))

def a_zero_select_stays_in_the_plane(bs, k, i):
  match bs k:
    case Nil{} 0n:
      Empty.absurd(IsTrue(lt(selz(Nil{}, 0n), len(Bool, Nil{}))), i)
    case Nil{} 1n+j:
      Empty.absurd(IsTrue(lt(selz(Nil{}, 1n+j), len(Bool, Nil{}))), i)
    case Con{False{}, t} 0n:
      Unit{}
    case Con{False{}, t} 1n+j:
      a_zero_select_stays_in_the_plane(t, j, i)
    case Con{True{}, t} k0:
      a_zero_select_stays_in_the_plane(t, k0, i)

# --- a member's bucket is the zeros below its one -------------------------------------------

def upto(+j: Nat, +gs: +List<Nat>) -> Nat:
  match j gs:
    case j0 Nil{}:
      0n
    case 0n Con{g, t}:
      g
    case 1n+k Con{g, t}:
      add(g, upto(k, t))

# the zeros below the j-th one of a plane
def before(+bs: +List<Bool>, +j: Nat) -> Nat:
  match bs j:
    case Nil{} j0:
      0n
    case Con{True{}, t} 0n:
      0n
    case Con{True{}, t} 1n+k:
      before(t, k)
    case Con{False{}, t} j0:
      1n+before(t, j0)

def before_is_zeros(+bs: +List<Bool>, +j: Nat) -> {zeros(sel(bs, j), bs) == before(bs, j) : Nat}:
  match bs j:
    case Nil{} j0:
      {==}
    case Con{True{}, t} 0n:
      {==}
    case Con{True{}, t} 1n+k:
      before_is_zeros(t, k)
    case Con{False{}, t} j0:
      %before_is_zeros(t, j0) : {1n+zeros(sel(t, j0), t) == 1n+_ : Nat}
      {==}

def before_skip(+g: Nat, +r: +List<Bool>, +j: Nat) -> {before(app(Bool, falses(g), r), j) == add(g, before(r, j)) : Nat}:
  match g:
    case 0n:
      {==}
    case 1n+k:
      %before_skip(k, r, j) : {1n+before(app(Bool, falses(k), r), j) == 1n+_ : Nat}
      {==}

def add_zero(+a: Nat) -> {add(a, 0n) == a : Nat}:
  match a:
    case 0n:
      {==}
    case 1n+p:
      %add_zero(p) : {1n+add(p, 0n) == 1n+_ : Nat}
      {==}

def bucket(+j: Nat, +gs: +List<Nat>) -> IsTrue(lt(j, len(Nat, gs))) -> {before(bits(gs), j) == upto(j, gs) : Nat}:
  match j gs:
    case 0n Nil{}:
      i => {==}
    case 1n+k Nil{}:
      i => {==}
    case 0n Con{g, t}:
      i =>
        %Equal.sym(Nat, before(app(Bool, falses(g), True{} <> bits(t)), 0n), add(g, before(True{} <> bits(t), 0n)), before_skip(g, True{} <> bits(t), 0n)) : {_ == g : Nat}
        add_zero(g)
    case 1n+k Con{g, t}:
      i =>
        %Equal.sym(Nat, before(app(Bool, falses(g), True{} <> bits(t)), 1n+k), add(g, before(True{} <> bits(t), 1n+k)), before_skip(g, True{} <> bits(t), 1n+k)) : {_ == add(g, upto(k, t)) : Nat}
        %bucket(k, t, i) : {add(g, before(bits(t), k)) == add(g, _) : Nat}
        {==}

law the_zeros_before_a_member_are_its_bucket:
  for +j: Nat
  for +gs: +List<Nat>
  for +i: IsTrue(lt(j, len(Nat, gs)))
  {zeros(sel(bits(gs), j), bits(gs)) == upto(j, gs) : Nat}

def the_zeros_before_a_member_are_its_bucket(j, gs, i):
  %Equal.sym(Nat, zeros(sel(bits(gs), j), bits(gs)), before(bits(gs), j), before_is_zeros(bits(gs), j)) : {_ == upto(j, gs) : Nat}
  bucket(j, gs, i)

# --- a field ends inside its plane ----------------------------------------------------------

def le_refl(+a: Nat) -> IsTrue(le(a, a)):
  match a:
    case 0n:
      Unit{}
    case 1n+p:
      le_refl(p)

def le_addr(+m: Nat, +x: Nat) -> IsTrue(le(m, add(m, x))):
  match m:
    case 0n:
      Unit{}
    case 1n+p:
      le_addr(p, x)

def le_mono(+l: Nat, +x: Nat, +y: Nat) -> IsTrue(le(x, y)) -> IsTrue(le(add(l, x), add(l, y))):
  match l:
    case 0n:
      i => i
    case 1n+p:
      le_mono(p, x, y)

def add_assoc(+a: Nat, +b: Nat, +c: Nat) -> {add(add(a, b), c) == add(a, add(b, c)) : Nat}:
  match a:
    case 0n:
      {==}
    case 1n+p:
      %add_assoc(p, b, c) : {1n+add(add(p, b), c) == 1n+_ : Nat}
      {==}

law a_field_ends_inside_its_plane:
  for +j: Nat
  for +c: Nat
  for +l: Nat
  for +i: IsTrue(lt(j, c))
  IsTrue(le(add(mul(j, l), l), mul(c, l)))

def a_field_ends_inside_its_plane(j, c, l, i):
  match j c:
    case 0n 0n:
      Empty.absurd(IsTrue(le(add(mul(0n, l), l), mul(0n, l))), i)
    case 0n 1n+q:
      le_addr(l, mul(q, l))
    case 1n+p 0n:
      Empty.absurd(IsTrue(le(add(mul(1n+p, l), l), mul(0n, l))), i)
    case 1n+p 1n+q:
      %Equal.sym(Nat, add(add(l, mul(p, l)), l), add(l, add(mul(p, l), l)), add_assoc(l, mul(p, l), l)) : IsTrue(le(_, add(l, mul(q, l))))
      le_mono(l, add(mul(p, l), l), mul(q, l), a_field_ends_inside_its_plane(p, q, l, i))

law the_example:
  {(bits([1n, 0n, 2n]), zeros(sel(bits([1n, 0n, 2n]), 2n), bits([1n, 0n, 2n])), upto(2n, [1n, 0n, 2n]), selz(bits([1n, 0n, 2n]), 1n)) == ([False{}, True{}, True{}, False{}, False{}, True{}], 3n, 3n, 3n) : +List<Bool> & Nat & Nat & Nat}

def the_example():
  {==}