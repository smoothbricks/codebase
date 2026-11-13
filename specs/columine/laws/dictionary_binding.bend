# Dictionary binding and field identity of a columine schema (specs/columine/10-physical-vocabulary.md
# §3 "The field descriptor", §4 "Schema dictionaries" and §6 "Field identity"), as a model whose laws
# are the creation check's contract.
#
# A field is `Plain` or `Encoded{slot, id}`: its descriptor's dictionary slot (`DictionarySlot::wire`:
# 1 is the stream dictionary, 2 + k is entry k of the creation call's dictionary table; 0 names no
# dictionary) and the Arrow dictionary id the Schema message declares for it. `n` is the table's
# length. `bind` answers `Refused` or `Bound{t}`, where entry s of `t` is slot s's id set as a bitmap:
# bit i is set exactly when a field of slot s declares id i, so read low to high it is the ascending
# `arrow_ids` of that slot. An unbound slot's bitmap is empty. A binding is accepted when no id is
# set under two slots, slot 0 is unbound, and the slots 2 + k bound are exactly those with k < n.
#
# `bind` mirrors `bind_dictionaries` (packages/columine/crates/columine-arrow/src/schema.rs):
# `Refused` is its `DictionaryMismatch` or `InvalidDictionaryTable`, `Bound` its `Ok`. The table's
# values, the index plane's capacity, a field with no Arrow id and allocation failure are not
# modelled. Its laws:
# (1) an_id_names_one_slot: an accepted binding sets no id under two slots, so a dictionary message
#     written for an id reaches fields of exactly one slot.
# (2) every_encoded_field_is_bound: an accepted binding holds each encoded field's id under its slot.
# (3) binding_is_order_free: swapping two adjacent fields changes neither acceptance nor the bound
#     sets; adjacent swaps generate every permutation.
# (4) one_id_under_two_slots_is_refused: the check is not vacuous; one id under two slots is refused,
#     and the same fields with two ids are bound.
#
# A column carries the `sb.fields` identities of the declared fields it holds, each
# `Ident{lib, n, digest}`. `identities_ok` mirrors `decode_field_identities`
# (packages/columine/crates/columine-parsing/src/identity.rs): `False` is its `IdentityError::TwoTypes`
# or `IdentityError::Repeated`. A malformed value is not modelled. Its laws:
# (5) a_column_has_one_type_digest: an accepted schema's column carries one type digest.
# (6) an_identity_is_named_once: an accepted schema names each (lib, n) at one entry of one column.
# identity_check_decides is their definedness: the check accepts and refuses.
#
# Check: bun bend2/main.ts specs/columine/laws/dictionary_binding.bend --verdict
# Bound to the code by packages/columine/crates/columine-arrow/tests/dictionary_binding_laws.rs, which
# runs `bind` (emitted into tests/dictionary_binding_model.rs) against `DynamicSchemaConfig::from_parts`,
# and by packages/columine/crates/columine-parsing/tests/identity_laws.rs, which runs `identities_ok`
# (emitted into tests/identity_model.rs) against `decode_field_identities`, on generated schemas.
import Base

type Field is Data:
  Plain{}
  Encoded{slot: Nat, id: Nat}

type Binding is Data:
  Refused{}
  Bound{t: +List<+List<Bool>>}

type Ident is Data:
  Ident{lib: Nat, n: Nat, digest: Nat}

# --- helpers -------------------------------------------------------------------------

def same(a: Nat, b: Nat) -> Bool:
  match a b:
    case 0n 0n:
      True{}
    case 0n 1n+q:
      False{}
    case 1n+p 0n:
      False{}
    case 1n+p 1n+q:
      same(p, q)

def cat(xs: +List<Field>, ys: +List<Field>) -> +List<Field>:
  match xs:
    case Nil{}:
      ys
    case Con{h, t}:
      h <> cat(t, ys)

# --- id sets as bitmaps ----------------------------------------------------------------

def set(i: Nat, b: +List<Bool>) -> +List<Bool>:
  match i b:
    case 0n Nil{}:
      [True{}]
    case 0n Con{x, t}:
      True{} <> t
    case 1n+p Nil{}:
      False{} <> set(p, [])
    case 1n+p Con{x, t}:
      x <> set(p, t)

def bit(b: +List<Bool>, i: Nat) -> Bool:
  match b i:
    case Nil{} i0:
      False{}
    case Con{x, t} 0n:
      x
    case Con{x, t} 1n+p:
      bit(t, p)

def any(b: +List<Bool>) -> Bool:
  match b:
    case Nil{}:
      False{}
    case Con{x, t}:
      Bool.or(x, any(t))

def join(a: +List<Bool>, b: +List<Bool>) -> +List<Bool>:
  match a b:
    case Nil{} b0:
      b0
    case Con{x, s} Nil{}:
      x <> s
    case Con{x, s} Con{y, t}:
      Bool.or(x, y) <> join(s, t)

def meets(a: +List<Bool>, b: +List<Bool>) -> Bool:
  match a b:
    case Nil{} b0:
      False{}
    case Con{x, s} Nil{}:
      False{}
    case Con{x, s} Con{y, t}:
      Bool.or(Bool.and(x, y), meets(s, t))

# --- the binding -------------------------------------------------------------------------

# Slot s's set with id i added.
def put(s: Nat, i: Nat, t: +List<+List<Bool>>) -> +List<+List<Bool>>:
  match s t:
    case 0n Nil{}:
      [set(i, [])]
    case 0n Con{b, r}:
      set(i, b) <> r
    case 1n+p Nil{}:
      [] <> put(p, i, [])
    case 1n+p Con{b, r}:
      b <> put(p, i, r)

def has(t: +List<+List<Bool>>, s: Nat, i: Nat) -> Bool:
  match t s:
    case Nil{} s0:
      False{}
    case Con{b, r} 0n:
      bit(b, i)
    case Con{b, r} 1n+p:
      has(r, p, i)

def table(fs: +List<Field>) -> +List<+List<Bool>>:
  match fs:
    case Nil{}:
      []
    case Con{Plain{}, r}:
      table(r)
    case Con{Encoded{s, i}, r}:
      put(s, i, table(r))

# No id is set under two slots; `seen` is the union of the slots before.
def apart(t: +List<+List<Bool>>, +seen: +List<Bool>) -> Bool:
  match t:
    case Nil{}:
      True{}
    case Con{+b, r}:
      Bool.and(Bool.not(meets(b, seen)), apart(r, join(b, seen)))

# Slots 2 + k from k = 0 on: bound while k < n, unbound after.
def entries(t: +List<+List<Bool>>, n: Nat) -> Bool:
  match t n:
    case Nil{} n0:
      same(n0, 0n)
    case Con{b, r} 0n:
      Bool.and(Bool.not(any(b)), entries(r, 0n))
    case Con{b, r} 1n+m:
      Bool.and(any(b), entries(r, m))

# Slot 0 unbound, slot 1 free, the table's entries each bound.
def shape(t: +List<+List<Bool>>, +n: Nat) -> Bool:
  match t:
    case Nil{}:
      same(n, 0n)
    case Con{b0, Nil{}}:
      Bool.and(Bool.not(any(b0)), same(n, 0n))
    case Con{b0, Con{b1, r}}:
      Bool.and(Bool.not(any(b0)), entries(r, n))

def ok(+t: +List<+List<Bool>>, +n: Nat) -> Bool:
  Bool.and(apart(t, []), shape(t, n))

def decide(t: +List<+List<Bool>>, ok: Bool) -> Binding:
  match ok:
    case True{}:
      Bound{t}
    case False{}:
      Refused{}

def bind(+fs: +List<Field>, +n: Nat) -> Binding:
  decide(table(fs), ok(table(fs), n))

def accepted(r: Binding) -> Bool:
  match r:
    case Refused{}:
      False{}
    case Bound{t}:
      True{}

# The binding holds id i under slot s.
def holds(r: Binding, s: Nat, i: Nat) -> Bool:
  match r:
    case Refused{}:
      False{}
    case Bound{t}:
      has(t, s, i)

# --- field identity ----------------------------------------------------------------------

def has_key(+l: Nat, +k: Nat, col: +List<Ident>) -> Bool:
  match col:
    case Nil{}:
      False{}
    case Con{Ident{l0, k0, d}, r}:
      Bool.or(Bool.and(same(l, l0), same(k, k0)), has_key(l, k, r))

def has_digest(+d: Nat, col: +List<Ident>) -> Bool:
  match col:
    case Nil{}:
      False{}
    case Con{Ident{l, k, e}, r}:
      Bool.or(same(d, e), has_digest(d, r))

def all_digest(+d: Nat, col: +List<Ident>) -> Bool:
  match col:
    case Nil{}:
      True{}
    case Con{Ident{l, k, e}, r}:
      Bool.and(same(e, d), all_digest(d, r))

def one_digest(col: +List<Ident>) -> Bool:
  match col:
    case Nil{}:
      True{}
    case Con{Ident{l, k, d}, r}:
      all_digest(d, r)

# Some column of `cols` carries (l, k).
def keyed(+l: Nat, +k: Nat, cols: +List<+List<Ident>>) -> Bool:
  match cols:
    case Nil{}:
      False{}
    case Con{col, r}:
      Bool.or(has_key(l, k, col), keyed(l, k, r))

# An identity of `col` is on a column of `cols`.
def shares(col: +List<Ident>, +cols: +List<+List<Ident>>) -> Bool:
  match col:
    case Nil{}:
      False{}
    case Con{Ident{l, k, d}, r}:
      Bool.or(keyed(l, k, cols), shares(r, cols))

# Each (lib, n) of the column at one entry of it.
def distinct(col: +List<Ident>) -> Bool:
  match col:
    case Nil{}:
      True{}
    case Con{Ident{l, k, d}, +r}:
      Bool.and(Bool.not(has_key(l, k, r)), distinct(r))

# A column's checks: one type digest, each (lib, n) once in it and on no later column.
def column_ok(+col: +List<Ident>, +later: +List<+List<Ident>>) -> Bool:
  Bool.and(one_digest(col), Bool.and(distinct(col), Bool.not(shares(col, later))))

def identities_ok(cols: +List<+List<Ident>>) -> Bool:
  match cols:
    case Nil{}:
      True{}
    case Con{+col, +r}:
      Bool.and(column_ok(col, r), identities_ok(r))

# Entry p of the column names (l, k).
def at_entry(col: +List<Ident>, p: Nat, +l: Nat, +k: Nat) -> Bool:
  match col p:
    case Nil{} p0:
      False{}
    case Con{Ident{l0, k0, d}, r} 0n:
      Bool.and(same(l, l0), same(k, k0))
    case Con{Ident{l0, k0, d}, r} 1n+q:
      at_entry(r, q, l, k)

# Entry p of column c names (l, k).
def at(cols: +List<+List<Ident>>, c: Nat, +p: Nat, +l: Nat, +k: Nat) -> Bool:
  match cols c:
    case Nil{} c0:
      False{}
    case Con{col, r} 0n:
      at_entry(col, p, l, k)
    case Con{col, r} 1n+q:
      at(r, q, p, l, k)

# Column c carries type digest d.
def digest_on(cols: +List<+List<Ident>>, c: Nat, +d: Nat) -> Bool:
  match cols c:
    case Nil{} c0:
      False{}
    case Con{col, r} 0n:
      has_digest(d, col)
    case Con{col, r} 1n+p:
      digest_on(r, p, d)

# === laws ==========================================================================

# --- lemmas ------------------------------------------------------------------------

def disc(b: Bool) -> Data:
  match b:
    case True{}:
      Empty
    case False{}:
      Unit

def false_true(h: {False{} == True{} : Bool}) -> Empty:
  %h : disc(_)
  Unit{}

def contra(+x: Bool, +f: {x == False{} : Bool}, +t: {x == True{} : Bool}) -> Empty:
  match x:
    case True{}:
      false_true(Equal.sym(Bool, True{}, False{}, f))
    case False{}:
      false_true(t)

def or_elim(-G: Type, +a: Bool, +b: Bool, +h: {Bool.or(a, b) == True{} : Bool}, f: {a == True{} : Bool} -> G, g: {b == True{} : Bool} -> G) -> G:
  match a:
    case True{}:
      f({==})
    case False{}:
      g(h)

def or_l(+a: Bool, +b: Bool, +h: {a == True{} : Bool}) -> {Bool.or(a, b) == True{} : Bool}:
  match a:
    case True{}:
      {==}
    case False{}:
      Empty.absurd({Bool.or(False{}, b) == True{} : Bool}, false_true(h))

def or_r(+a: Bool, +b: Bool, +h: {b == True{} : Bool}) -> {Bool.or(a, b) == True{} : Bool}:
  match a:
    case True{}:
      {==}
    case False{}:
      h

def or_false_r(+a: Bool, +b: Bool, +h: {Bool.or(a, b) == False{} : Bool}) -> {b == False{} : Bool}:
  match a:
    case True{}:
      Empty.absurd({b == False{} : Bool}, false_true(Equal.sym(Bool, True{}, False{}, h)))
    case False{}:
      h

def and_l(+a: Bool, +b: Bool, +h: {Bool.and(a, b) == True{} : Bool}) -> {a == True{} : Bool}:
  match a:
    case False{}:
      Empty.absurd({False{} == True{} : Bool}, false_true(h))
    case True{}:
      {==}

def and_r(+a: Bool, +b: Bool, +h: {Bool.and(a, b) == True{} : Bool}) -> {b == True{} : Bool}:
  match a:
    case False{}:
      Empty.absurd({b == True{} : Bool}, false_true(h))
    case True{}:
      h

def not_true(+a: Bool, +hn: {Bool.not(a) == True{} : Bool}, +h: {a == True{} : Bool}) -> Empty:
  match a:
    case True{}:
      false_true(hn)
    case False{}:
      false_true(h)

def not_or_r(+a: Bool, +b: Bool, +h: {Bool.not(Bool.or(a, b)) == True{} : Bool}) -> {Bool.not(b) == True{} : Bool}:
  match a:
    case True{}:
      Empty.absurd({Bool.not(b) == True{} : Bool}, false_true(h))
    case False{}:
      h

def same_refl(+a: Nat) -> {same(a, a) == True{} : Bool}:
  match a:
    case 0n:
      {==}
    case 1n+p:
      same_refl(p)

def same_eq(+a: Nat, +b: Nat, +h: {same(a, b) == True{} : Bool}) -> {a == b : Nat}:
  match a b:
    case 0n 0n:
      {==}
    case 0n 1n+q:
      Empty.absurd({0n == 1n+q : Nat}, false_true(h))
    case 1n+p 0n:
      Empty.absurd({1n+p == 0n : Nat}, false_true(h))
    case 1n+p 1n+q:
      %same_eq(p, q, h) : {1n+p == 1n+_ : Nat}
      {==}

# --- bitmap lemmas ----------------------------------------------------------------------

def bit_set_same(+i: Nat, +b: +List<Bool>) -> {bit(set(i, b), i) == True{} : Bool}:
  match i b:
    case 0n Nil{}:
      {==}
    case 0n Con{x, t}:
      {==}
    case 1n+p Nil{}:
      bit_set_same(p, [])
    case 1n+p Con{x, t}:
      bit_set_same(p, t)

def bit_set_keep(+b: +List<Bool>, +i: Nat, +j: Nat, +h: {bit(b, j) == True{} : Bool}) -> {bit(set(i, b), j) == True{} : Bool}:
  match b i j:
    case Nil{} i0 j0:
      Empty.absurd({bit(set(i0, []), j0) == True{} : Bool}, false_true(h))
    case Con{x, t} 0n 0n:
      {==}
    case Con{x, t} 0n 1n+q:
      h
    case Con{x, t} 1n+p 0n:
      h
    case Con{x, t} 1n+p 1n+q:
      bit_set_keep(t, p, q, h)

def set_comm(+i: Nat, +j: Nat, +b: +List<Bool>) -> {set(i, set(j, b)) == set(j, set(i, b)) : +List<Bool>}:
  match i j b:
    case 0n 0n b0:
      {==}
    case 0n 1n+q Nil{}:
      {==}
    case 0n 1n+q Con{x, t}:
      {==}
    case 1n+p 0n Nil{}:
      {==}
    case 1n+p 0n Con{x, t}:
      {==}
    case 1n+p 1n+q Nil{}:
      %set_comm(p, q, []) : {False{} <> set(p, set(q, [])) == False{} <> _ : +List<Bool>}
      {==}
    case 1n+p 1n+q Con{x, t}:
      %set_comm(p, q, t) : {x <> set(p, set(q, t)) == x <> _ : +List<Bool>}
      {==}

def bit_join_l(+a: +List<Bool>, +c: +List<Bool>, +i: Nat, +h: {bit(a, i) == True{} : Bool}) -> {bit(join(a, c), i) == True{} : Bool}:
  match a c i:
    case Nil{} c0 i0:
      Empty.absurd({bit(join([], c0), i0) == True{} : Bool}, false_true(h))
    case Con{x, s} Nil{} i0:
      h
    case Con{x, s} Con{y, t} 0n:
      or_l(x, y, h)
    case Con{x, s} Con{y, t} 1n+p:
      bit_join_l(s, t, p, h)

def join_false_r(+a: +List<Bool>, +c: +List<Bool>, +i: Nat, +h: {bit(join(a, c), i) == False{} : Bool}) -> {bit(c, i) == False{} : Bool}:
  match a c i:
    case Nil{} c0 i0:
      h
    case Con{x, s} Nil{} i0:
      {==}
    case Con{x, s} Con{y, t} 0n:
      or_false_r(x, y, h)
    case Con{x, s} Con{y, t} 1n+p:
      join_false_r(s, t, p, h)

def meet_head(+x: Bool, +y: Bool, +m: Bool, +nm: {Bool.not(Bool.or(Bool.and(x, y), m)) == True{} : Bool}, +hx: {x == True{} : Bool}) -> {y == False{} : Bool}:
  match x y:
    case True{} True{}:
      Empty.absurd({True{} == False{} : Bool}, false_true(nm))
    case True{} False{}:
      {==}
    case False{} y0:
      Empty.absurd({y0 == False{} : Bool}, false_true(hx))

def meets_bit(+a: +List<Bool>, +c: +List<Bool>, +i: Nat, +nm: {Bool.not(meets(a, c)) == True{} : Bool}, +hb: {bit(a, i) == True{} : Bool}) -> {bit(c, i) == False{} : Bool}:
  match a c i:
    case Nil{} c0 i0:
      Empty.absurd({bit(c0, i0) == False{} : Bool}, false_true(hb))
    case Con{x, s} Nil{} i0:
      {==}
    case Con{x, s} Con{y, t} 0n:
      meet_head(x, y, meets(s, t), nm, hb)
    case Con{x, s} Con{y, t} 1n+p:
      meets_bit(s, t, p, not_or_r(Bool.and(x, y), meets(s, t), nm), hb)

# --- table lemmas -------------------------------------------------------------------------

def put_comm(+s1: Nat, +i1: Nat, +s2: Nat, +i2: Nat, +t: +List<+List<Bool>>) -> {put(s1, i1, put(s2, i2, t)) == put(s2, i2, put(s1, i1, t)) : +List<+List<Bool>>}:
  match s1 s2 t:
    case 0n 0n Nil{}:
      %set_comm(i1, i2, []) : {[set(i1, set(i2, []))] == [_] : +List<+List<Bool>>}
      {==}
    case 0n 0n Con{b, r}:
      %set_comm(i1, i2, b) : {set(i1, set(i2, b)) <> r == _ <> r : +List<+List<Bool>>}
      {==}
    case 0n 1n+q Nil{}:
      {==}
    case 0n 1n+q Con{b, r}:
      {==}
    case 1n+p 0n Nil{}:
      {==}
    case 1n+p 0n Con{b, r}:
      {==}
    case 1n+p 1n+q Nil{}:
      %put_comm(p, i1, q, i2, []) : {[] <> put(p, i1, put(q, i2, [])) == [] <> _ : +List<+List<Bool>>}
      {==}
    case 1n+p 1n+q Con{b, r}:
      %put_comm(p, i1, q, i2, r) : {b <> put(p, i1, put(q, i2, r)) == b <> _ : +List<+List<Bool>>}
      {==}

def swap_table(+a: Field, +b: Field, +r: +List<Field>) -> {table(a <> (b <> r)) == table(b <> (a <> r)) : +List<+List<Bool>>}:
  match a b:
    case Plain{} Plain{}:
      {==}
    case Plain{} Encoded{s, i}:
      {==}
    case Encoded{s, i} Plain{}:
      {==}
    case Encoded{s1, i1} Encoded{s2, i2}:
      put_comm(s1, i1, s2, i2, table(r))

def swap_at(+p: +List<Field>, +a: Field, +b: Field, +r: +List<Field>) -> {table(cat(p, a <> (b <> r))) == table(cat(p, b <> (a <> r))) : +List<+List<Bool>>}:
  match p:
    case Nil{}:
      swap_table(a, b, r)
    case Con{Plain{}, q}:
      swap_at(q, a, b, r)
    case Con{Encoded{s, i}, q}:
      %swap_at(q, a, b, r) : {put(s, i, table(cat(q, a <> (b <> r)))) == put(s, i, _) : +List<+List<Bool>>}
      {==}

def has_put_same(+s: Nat, +i: Nat, +t: +List<+List<Bool>>) -> {has(put(s, i, t), s, i) == True{} : Bool}:
  match s t:
    case 0n Nil{}:
      bit_set_same(i, [])
    case 0n Con{b, r}:
      bit_set_same(i, b)
    case 1n+p Nil{}:
      has_put_same(p, i, [])
    case 1n+p Con{b, r}:
      has_put_same(p, i, r)

def has_put_keep(+t: +List<+List<Bool>>, +s: Nat, +i: Nat, +s2: Nat, +i2: Nat, +h: {has(t, s2, i2) == True{} : Bool}) -> {has(put(s, i, t), s2, i2) == True{} : Bool}:
  match t s s2:
    case Nil{} s0 s3:
      Empty.absurd({has(put(s0, i, []), s3, i2) == True{} : Bool}, false_true(h))
    case Con{b, r} 0n 0n:
      bit_set_keep(b, i, i2, h)
    case Con{b, r} 0n 1n+q:
      h
    case Con{b, r} 1n+p 0n:
      h
    case Con{b, r} 1n+p 1n+q:
      has_put_keep(r, p, i, q, i2, h)

def has_at(+p: +List<Field>, +s: Nat, +i: Nat, +r: +List<Field>) -> {has(table(cat(p, Encoded{s, i} <> r)), s, i) == True{} : Bool}:
  match p:
    case Nil{}:
      has_put_same(s, i, table(r))
    case Con{Plain{}, q}:
      has_at(q, s, i, r)
    case Con{Encoded{s0, i0}, q}:
      has_put_keep(table(cat(q, Encoded{s, i} <> r)), s0, i0, s, i, has_at(q, s, i, r))

# An id set under a later slot is outside `seen`.
def apart_none(+t: +List<+List<Bool>>, +seen: +List<Bool>, +s: Nat, +i: Nat, +h: {apart(t, seen) == True{} : Bool}, +hs: {has(t, s, i) == True{} : Bool}) -> {bit(seen, i) == False{} : Bool}:
  match t s:
    case Nil{} s0:
      Empty.absurd({bit(seen, i) == False{} : Bool}, false_true(hs))
    case Con{b, r} 0n:
      meets_bit(b, seen, i, and_l(Bool.not(meets(b, seen)), apart(r, join(b, seen)), h), hs)
    case Con{b, r} 1n+p:
      join_false_r(b, seen, i, apart_none(r, join(b, seen), p, i, and_r(Bool.not(meets(b, seen)), apart(r, join(b, seen)), h), hs))

def apart_one(+t: +List<+List<Bool>>, +seen: +List<Bool>, +s1: Nat, +s2: Nat, +i: Nat, +h: {apart(t, seen) == True{} : Bool}, +h1: {has(t, s1, i) == True{} : Bool}, +h2: {has(t, s2, i) == True{} : Bool}) -> {same(s1, s2) == True{} : Bool}:
  match t s1 s2:
    case Nil{} a0 b0:
      Empty.absurd({same(a0, b0) == True{} : Bool}, false_true(h1))
    case Con{b, r} 0n 0n:
      {==}
    case Con{b, r} 0n 1n+q:
      Empty.absurd({same(0n, 1n+q) == True{} : Bool}, contra(bit(join(b, seen), i), apart_none(r, join(b, seen), q, i, and_r(Bool.not(meets(b, seen)), apart(r, join(b, seen)), h), h2), bit_join_l(b, seen, i, h1)))
    case Con{b, r} 1n+p 0n:
      Empty.absurd({same(1n+p, 0n) == True{} : Bool}, contra(bit(join(b, seen), i), apart_none(r, join(b, seen), p, i, and_r(Bool.not(meets(b, seen)), apart(r, join(b, seen)), h), h1), bit_join_l(b, seen, i, h2)))
    case Con{b, r} 1n+p 1n+q:
      apart_one(r, join(b, seen), p, q, i, and_r(Bool.not(meets(b, seen)), apart(r, join(b, seen)), h), h1, h2)

def one_slot(+t: +List<+List<Bool>>, +b: Bool, +s1: Nat, +s2: Nat, +i: Nat, ha: {b == True{} : Bool} -> {apart(t, []) == True{} : Bool}, +h1: {holds(decide(t, b), s1, i) == True{} : Bool}, +h2: {holds(decide(t, b), s2, i) == True{} : Bool}) -> {same(s1, s2) == True{} : Bool}:
  match b:
    case True{}:
      apart_one(t, [], s1, s2, i, ha({==}), h1, h2)
    case False{}:
      Empty.absurd({same(s1, s2) == True{} : Bool}, false_true(h1))

def covered(+t: +List<+List<Bool>>, +b: Bool, +s: Nat, +i: Nat, +ha: {accepted(decide(t, b)) == True{} : Bool}, +hh: {has(t, s, i) == True{} : Bool}) -> {holds(decide(t, b), s, i) == True{} : Bool}:
  match b:
    case True{}:
      hh
    case False{}:
      Empty.absurd({holds(decide(t, False{}), s, i) == True{} : Bool}, false_true(ha))

# --- (1) to (4): the binding -----------------------------------------------------------

# (1) An accepted binding sets no Arrow id under two slots.
law an_id_names_one_slot:
  for +fs: +List<Field>
  for +n: Nat
  for +s1: Nat
  for +s2: Nat
  for +i: Nat
  for h1: {holds(bind(fs, n), s1, i) == True{} : Bool}
  for h2: {holds(bind(fs, n), s2, i) == True{} : Bool}
  {same(s1, s2) == True{} : Bool}

def an_id_names_one_slot(fs, n, s1, s2, i, h1, h2):
  one_slot(table(fs), ok(table(fs), n), s1, s2, i, k => and_l(apart(table(fs), []), shape(table(fs), n), k), h1, h2)

# (2) An accepted binding holds every encoded field's id under its slot, which is therefore bound.
law every_encoded_field_is_bound:
  for +p: +List<Field>
  for +s: Nat
  for +i: Nat
  for +r: +List<Field>
  for +n: Nat
  for h: {accepted(bind(cat(p, Encoded{s, i} <> r), n)) == True{} : Bool}
  {holds(bind(cat(p, Encoded{s, i} <> r), n), s, i) == True{} : Bool}

def every_encoded_field_is_bound(p, s, i, r, n, h):
  covered(table(cat(p, Encoded{s, i} <> r)), ok(table(cat(p, Encoded{s, i} <> r)), n), s, i, h, has_at(p, s, i, r))

# (3) Swapping two adjacent fields anywhere changes neither acceptance nor the bound sets.
law binding_is_order_free:
  for +p: +List<Field>
  for +a: Field
  for +b: Field
  for +r: +List<Field>
  for +n: Nat
  {bind(cat(p, a <> (b <> r)), n) == bind(cat(p, b <> (a <> r)), n) : Binding}

def binding_is_order_free(p, a, b, r, n):
  %swap_at(p, a, b, r) : {bind(cat(p, a <> (b <> r)), n) == decide(_, ok(_, n)) : Binding}
  {==}

# (4) Arrow id 0 under the stream dictionary and under table entry 0 is refused; the same two
# fields with ids 0 and 1 are bound.
law one_id_under_two_slots_is_refused:
  Sigma<&2, &2,
    {bind([Encoded{1n, 0n}, Encoded{2n, 0n}], 1n) == Refused{} : Binding},
    u => {bind([Encoded{1n, 0n}, Encoded{2n, 1n}], 1n) == Bound{[[], [True{}], [False{}, True{}]]} : Binding}>

def one_id_under_two_slots_is_refused():
  ({==}, {==})

# --- (5), (6): field identity --------------------------------------------------------------

def same_via(+a: Nat, +b: Nat, +d: Nat, +ha: {same(a, d) == True{} : Bool}, +hb: {same(b, d) == True{} : Bool}) -> {same(a, b) == True{} : Bool}:
  %Equal.sym(Nat, a, d, same_eq(a, d, ha)) : {same(_, b) == True{} : Bool}
  %Equal.sym(Nat, b, d, same_eq(b, d, hb)) : {same(d, _) == True{} : Bool}
  same_refl(d)

def in_head(+x: Nat, +e: Nat, +d: Nat, +hx: {same(x, e) == True{} : Bool}, +he: {same(e, d) == True{} : Bool}) -> {same(x, d) == True{} : Bool}:
  %Equal.sym(Nat, x, e, same_eq(x, e, hx)) : {same(_, d) == True{} : Bool}
  he

def in_all(+d: Nat, +col: +List<Ident>, +x: Nat, +ha: {all_digest(d, col) == True{} : Bool}, +hx: {has_digest(x, col) == True{} : Bool}) -> {same(x, d) == True{} : Bool}:
  match col:
    case Nil{}:
      Empty.absurd({same(x, d) == True{} : Bool}, false_true(hx))
    case Con{Ident{l, k, e}, r}:
      or_elim({same(x, d) == True{} : Bool}, same(x, e), has_digest(x, r), hx, he => in_head(x, e, d, he, and_l(same(e, d), all_digest(d, r), ha)), hr => in_all(d, r, x, and_r(same(e, d), all_digest(d, r), ha), hr))

def same_refl_all(+d: Nat, +r: +List<Ident>, +ho: {all_digest(d, r) == True{} : Bool}) -> {Bool.and(same(d, d), all_digest(d, r)) == True{} : Bool}:
  %Equal.sym(Bool, same(d, d), True{}, same_refl(d)) : {Bool.and(_, all_digest(d, r)) == True{} : Bool}
  ho

def col_one(+col: +List<Ident>, +d1: Nat, +d2: Nat, +ho: {one_digest(col) == True{} : Bool}, +h1: {has_digest(d1, col) == True{} : Bool}, +h2: {has_digest(d2, col) == True{} : Bool}) -> {same(d1, d2) == True{} : Bool}:
  match col:
    case Nil{}:
      Empty.absurd({same(d1, d2) == True{} : Bool}, false_true(h1))
    case Con{Ident{l, k, d}, r}:
      same_via(d1, d2, d, in_all(d, Ident{l, k, d} <> r, d1, same_refl_all(d, r, ho), h1), in_all(d, Ident{l, k, d} <> r, d2, same_refl_all(d, r, ho), h2))

def ok_column(+col: +List<Ident>, +r: +List<+List<Ident>>, +h: {identities_ok(col <> r) == True{} : Bool}) -> {column_ok(col, r) == True{} : Bool}:
  and_l(column_ok(col, r), identities_ok(r), h)

def ok_rest(+col: +List<Ident>, +r: +List<+List<Ident>>, +h: {identities_ok(col <> r) == True{} : Bool}) -> {identities_ok(r) == True{} : Bool}:
  and_r(column_ok(col, r), identities_ok(r), h)

def col_digest(+col: +List<Ident>, +r: +List<+List<Ident>>, +h: {identities_ok(col <> r) == True{} : Bool}) -> {one_digest(col) == True{} : Bool}:
  and_l(one_digest(col), Bool.and(distinct(col), Bool.not(shares(col, r))), ok_column(col, r, h))

def col_distinct(+col: +List<Ident>, +r: +List<+List<Ident>>, +h: {identities_ok(col <> r) == True{} : Bool}) -> {distinct(col) == True{} : Bool}:
  and_l(distinct(col), Bool.not(shares(col, r)), and_r(one_digest(col), Bool.and(distinct(col), Bool.not(shares(col, r))), ok_column(col, r, h)))

def col_apart(+col: +List<Ident>, +r: +List<+List<Ident>>, +h: {identities_ok(col <> r) == True{} : Bool}) -> {Bool.not(shares(col, r)) == True{} : Bool}:
  and_r(distinct(col), Bool.not(shares(col, r)), and_r(one_digest(col), Bool.and(distinct(col), Bool.not(shares(col, r))), ok_column(col, r, h)))

def one_type(+cols: +List<+List<Ident>>, +c: Nat, +d1: Nat, +d2: Nat, +h: {identities_ok(cols) == True{} : Bool}, +h1: {digest_on(cols, c, d1) == True{} : Bool}, +h2: {digest_on(cols, c, d2) == True{} : Bool}) -> {same(d1, d2) == True{} : Bool}:
  match cols c:
    case Nil{} c0:
      Empty.absurd({same(d1, d2) == True{} : Bool}, false_true(h1))
    case Con{col, r} 0n:
      col_one(col, d1, d2, col_digest(col, r, h), h1, h2)
    case Con{col, r} 1n+p:
      one_type(r, p, d1, d2, ok_rest(col, r, h), h1, h2)

# (5) A column of an accepted schema carries one type digest.
law a_column_has_one_type_digest:
  for +cols: +List<+List<Ident>>
  for +c: Nat
  for +d1: Nat
  for +d2: Nat
  for h: {identities_ok(cols) == True{} : Bool}
  for h1: {digest_on(cols, c, d1) == True{} : Bool}
  for h2: {digest_on(cols, c, d2) == True{} : Bool}
  {same(d1, d2) == True{} : Bool}

def a_column_has_one_type_digest(cols, c, d1, d2, h, h1, h2):
  one_type(cols, c, d1, d2, h, h1, h2)

def shares_head(+l: Nat, +k: Nat, +l0: Nat, +k0: Nat, +r: +List<Ident>, +cols: +List<+List<Ident>>, +m: {Bool.and(same(l, l0), same(k, k0)) == True{} : Bool}, +hl: {keyed(l, k, cols) == True{} : Bool}) -> {Bool.or(keyed(l0, k0, cols), shares(r, cols)) == True{} : Bool}:
  %same_eq(l, l0, and_l(same(l, l0), same(k, k0), m)) : {Bool.or(keyed(_, k0, cols), shares(r, cols)) == True{} : Bool}
  %same_eq(k, k0, and_r(same(l, l0), same(k, k0), m)) : {Bool.or(keyed(l, _, cols), shares(r, cols)) == True{} : Bool}
  or_l(keyed(l, k, cols), shares(r, cols), hl)

def shares_of(+col: +List<Ident>, +cols: +List<+List<Ident>>, +l: Nat, +k: Nat, +hk: {has_key(l, k, col) == True{} : Bool}, +hl: {keyed(l, k, cols) == True{} : Bool}) -> {shares(col, cols) == True{} : Bool}:
  match col:
    case Nil{}:
      Empty.absurd({shares([], cols) == True{} : Bool}, false_true(hk))
    case Con{Ident{l0, k0, d}, r}:
      or_elim({Bool.or(keyed(l0, k0, cols), shares(r, cols)) == True{} : Bool}, Bool.and(same(l, l0), same(k, k0)), has_key(l, k, r), hk, m => shares_head(l, k, l0, k0, r, cols, m, hl), hr => or_r(keyed(l0, k0, cols), shares(r, cols), shares_of(r, cols, l, k, hr, hl)))

def at_entry_key(+col: +List<Ident>, +p: Nat, +l: Nat, +k: Nat, +h: {at_entry(col, p, l, k) == True{} : Bool}) -> {has_key(l, k, col) == True{} : Bool}:
  match col p:
    case Nil{} p0:
      Empty.absurd({has_key(l, k, []) == True{} : Bool}, false_true(h))
    case Con{Ident{l0, k0, d}, r} 0n:
      or_l(Bool.and(same(l, l0), same(k, k0)), has_key(l, k, r), h)
    case Con{Ident{l0, k0, d}, r} 1n+q:
      or_r(Bool.and(same(l, l0), same(k, k0)), has_key(l, k, r), at_entry_key(r, q, l, k, h))

def at_keyed(+cols: +List<+List<Ident>>, +c: Nat, +p: Nat, +l: Nat, +k: Nat, +h: {at(cols, c, p, l, k) == True{} : Bool}) -> {keyed(l, k, cols) == True{} : Bool}:
  match cols c:
    case Nil{} c0:
      Empty.absurd({keyed(l, k, []) == True{} : Bool}, false_true(h))
    case Con{col, r} 0n:
      or_l(has_key(l, k, col), keyed(l, k, r), at_entry_key(col, p, l, k, h))
    case Con{col, r} 1n+q:
      or_r(has_key(l, k, col), keyed(l, k, r), at_keyed(r, q, p, l, k, h))

def key_moves(+l: Nat, +k: Nat, +l0: Nat, +k0: Nat, +r: +List<Ident>, +m: {Bool.and(same(l, l0), same(k, k0)) == True{} : Bool}, +h: {has_key(l, k, r) == True{} : Bool}) -> {has_key(l0, k0, r) == True{} : Bool}:
  %same_eq(l, l0, and_l(same(l, l0), same(k, k0), m)) : {has_key(_, k0, r) == True{} : Bool}
  %same_eq(k, k0, and_r(same(l, l0), same(k, k0), m)) : {has_key(l, _, r) == True{} : Bool}
  h

def entry_once(+col: +List<Ident>, +p1: Nat, +p2: Nat, +l: Nat, +k: Nat, +hd: {distinct(col) == True{} : Bool}, +h1: {at_entry(col, p1, l, k) == True{} : Bool}, +h2: {at_entry(col, p2, l, k) == True{} : Bool}) -> {same(p1, p2) == True{} : Bool}:
  match col p1 p2:
    case Nil{} a0 b0:
      Empty.absurd({same(a0, b0) == True{} : Bool}, false_true(h1))
    case Con{Ident{l0, k0, d}, r} 0n 0n:
      {==}
    case Con{Ident{l0, k0, d}, r} 0n 1n+b:
      Empty.absurd({same(0n, 1n+b) == True{} : Bool}, not_true(has_key(l0, k0, r), and_l(Bool.not(has_key(l0, k0, r)), distinct(r), hd), key_moves(l, k, l0, k0, r, h1, at_entry_key(r, b, l, k, h2))))
    case Con{Ident{l0, k0, d}, r} 1n+a 0n:
      Empty.absurd({same(1n+a, 0n) == True{} : Bool}, not_true(has_key(l0, k0, r), and_l(Bool.not(has_key(l0, k0, r)), distinct(r), hd), key_moves(l, k, l0, k0, r, h2, at_entry_key(r, a, l, k, h1))))
    case Con{Ident{l0, k0, d}, r} 1n+a 1n+b:
      entry_once(r, a, b, l, k, and_r(Bool.not(has_key(l0, k0, r)), distinct(r), hd), h1, h2)

def named_once(+cols: +List<+List<Ident>>, +c1: Nat, +c2: Nat, +p1: Nat, +p2: Nat, +l: Nat, +k: Nat, +h: {identities_ok(cols) == True{} : Bool}, +h1: {at(cols, c1, p1, l, k) == True{} : Bool}, +h2: {at(cols, c2, p2, l, k) == True{} : Bool}) -> {Bool.and(same(c1, c2), same(p1, p2)) == True{} : Bool}:
  match cols c1 c2:
    case Nil{} a0 b0:
      Empty.absurd({Bool.and(same(a0, b0), same(p1, p2)) == True{} : Bool}, false_true(h1))
    case Con{col, r} 0n 0n:
      entry_once(col, p1, p2, l, k, col_distinct(col, r, h), h1, h2)
    case Con{col, r} 0n 1n+b:
      Empty.absurd({Bool.and(same(0n, 1n+b), same(p1, p2)) == True{} : Bool}, not_true(shares(col, r), col_apart(col, r, h), shares_of(col, r, l, k, at_entry_key(col, p1, l, k, h1), at_keyed(r, b, p2, l, k, h2))))
    case Con{col, r} 1n+a 0n:
      Empty.absurd({Bool.and(same(1n+a, 0n), same(p1, p2)) == True{} : Bool}, not_true(shares(col, r), col_apart(col, r, h), shares_of(col, r, l, k, at_entry_key(col, p2, l, k, h2), at_keyed(r, a, p1, l, k, h1))))
    case Con{col, r} 1n+a 1n+b:
      named_once(r, a, b, p1, p2, l, k, ok_rest(col, r, h), h1, h2)

# (6) An accepted schema names an identity (lib, n) at one entry of one column: twice in a column
# and on two columns are both refused.
law an_identity_is_named_once:
  for +cols: +List<+List<Ident>>
  for +c1: Nat
  for +p1: Nat
  for +c2: Nat
  for +p2: Nat
  for +l: Nat
  for +k: Nat
  for h: {identities_ok(cols) == True{} : Bool}
  for h1: {at(cols, c1, p1, l, k) == True{} : Bool}
  for h2: {at(cols, c2, p2, l, k) == True{} : Bool}
  {Bool.and(same(c1, c2), same(p1, p2)) == True{} : Bool}

def an_identity_is_named_once(cols, c1, p1, c2, p2, l, k, h, h1, h2):
  named_once(cols, c1, c2, p1, p2, l, k, h, h1, h2)

# Definedness: two columns of distinct identities are accepted; a column of two type digests, one
# identity twice in a column and one identity on two columns are refused.
law identity_check_decides:
  Sigma<&2, &2,
    {identities_ok([[Ident{1n, 1n, 7n}], [Ident{1n, 2n, 7n}]]) == True{} : Bool},
    u => Sigma<&2, &2,
      {identities_ok([[Ident{1n, 1n, 7n}, Ident{1n, 2n, 8n}]]) == False{} : Bool},
      v => Sigma<&2, &2,
        {identities_ok([[Ident{1n, 1n, 7n}, Ident{1n, 1n, 7n}]]) == False{} : Bool},
        w => {identities_ok([[Ident{1n, 1n, 7n}], [Ident{1n, 1n, 7n}]]) == False{} : Bool}>>>

def identity_check_decides():
  ({==}, ({==}, ({==}, {==})))
