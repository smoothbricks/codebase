# Maplet laws for bitmosaic::maplet (mutable live Maplet + sealed MapletView).
#
# The model:
# - A live maplet is a newest-first insertion list `+List<Entry>` of `Pair{key, val}` entries.
# - `insert(m, k, v)` prepends `Pair{k, v}` to `m`.
# - `lookup_all(m, k)` yields all values associated with `k` in newest-first order.
# - `seal(m)` orders entries by ascending `key` (matching Format 2 V2View sorted keys) while
#   preserving exact newest-first value order within each key group.
# - `promote(s)` hydrates a sealed maplet back into a live maplet via `insert`, inserting older
#   entries before newer entries so newest-first lookup order is preserved.
#
# Proven here, for every maplet, every key, and every value (unconditional, zero vacuity):
# - `maplet_insert_finds_value`: inserting `(k, v)` makes `v` present in `lookup_all(..., k)`.
# - `maplet_collision_preserves_older`: inserting a second value `v_new` for the same key `k`
#   never overwrites an older value `v_old` (both remain discoverable).
# - `seal_promote_preserves_lookup`: `lookup_all(promote(seal(m)), k) == lookup_all(m, k)`
#   structurally for every maplet `m` and every key `k`.
#
# All laws use the first-order bend-emit fragment (Nat, Bool, +List<Nat>, +List<Entry>, Entry).
import Base

type Entry is Data:
  Pair{key: Nat, val: Nat}

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

def le(+a: Nat, +b: Nat) -> Bool:
  match a b:
    case 0n b0:
      True{}
    case 1n+p 0n:
      False{}
    case 1n+p 1n+q:
      le(p, q)

def either(+a: Bool, b: Bool) -> Bool:
  match a:
    case True{}:
      True{}
    case False{}:
      b

def list_mem(+xs: +List<Nat>, +v: Nat) -> Bool:
  match xs:
    case Nil{}:
      False{}
    case Con{h, t}:
      either(eq(h, v), list_mem(t, v))

def insert(m: +List<Entry>, k: Nat, v: Nat) -> +List<Entry>:
  Con{Pair{k, v}, m}

def lookup_step(+b: Bool, v: Nat, rest: +List<Nat>) -> +List<Nat>:
  match b:
    case True{}:
      Con{v, rest}
    case False{}:
      rest

def lookup_all(+m: +List<Entry>, +k: Nat) -> +List<Nat>:
  match m:
    case Nil{}:
      Nil{}
    case Con{e, t}:
      match e:
        case Pair{ek, ev}:
          lookup_step(eq(ek, k), ev, lookup_all(t, k))

def insert_sorted_at(+b_le: Bool, ek: Nat, ev: Nat, hk: Nat, hv: Nat, t: +List<Entry>, rec: +List<Entry>) -> +List<Entry>:
  match b_le:
    case True{}:
      Con{Pair{ek, ev}, Con{Pair{hk, hv}, t}}
    case False{}:
      Con{Pair{hk, hv}, rec}

# Stable insertion by ascending key: places `Pair{ek, ev}` before the first entry with `ek <= hk`,
# preserving exact newest-first order among equal keys.
def insert_by_key(+s: +List<Entry>, +ek: Nat, +ev: Nat) -> +List<Entry>:
  match s:
    case Nil{}:
      Con{Pair{ek, ev}, Nil{}}
    case Con{h, t}:
      match h:
        case Pair{hk, hv}:
          insert_sorted_at(le(ek, hk), ek, ev, hk, hv, t, insert_by_key(t, ek, ev))

# Seal: sort entries by ascending key (matching Format 2 V2View sorted key plane), newest-first per key.
def seal(+m: +List<Entry>) -> +List<Entry>:
  match m:
    case Nil{}:
      Nil{}
    case Con{e, t}:
      match e:
        case Pair{ek, ev}:
          insert_by_key(seal(t), ek, ev)

# Promote: hydrate a sealed maplet back into a live maplet by inserting older tail entries first.
def promote(+s: +List<Entry>) -> +List<Entry>:
  match s:
    case Nil{}:
      Nil{}
    case Con{e, t}:
      match e:
        case Pair{ek, ev}:
          insert(promote(t), ek, ev)

# ---- Helpers ----------------------------------------------------------------

def Bool.is_true(b: Bool) -> Data:
  match b:
    case True{}:
      Unit
    case False{}:
      Empty

def false_ne_true(e: {False{} == True{} : Bool}) -> Empty:
  %Equal.sym(Bool, False{}, True{}, e) : Bool.is_true(_)
  Unit{}

def true_ne_false(e: {True{} == False{} : Bool}) -> Empty:
  %e : Bool.is_true(_)
  Unit{}

def eq_refl(+a: Nat) -> {eq(a, a) == True{} : Bool}:
  match a:
    case 0n:
      {==}
    case 1n+p:
      eq_refl(p)

def either_true_r(+a: Bool) -> {either(a, True{}) == True{} : Bool}:
  match a:
    case True{}:
      {==}
    case False{}:
      {==}

def not_le_and_eq_implies_ne(+ek: Nat, +hk: Nat, +k: Nat, h_le: {le(ek, hk) == False{} : Bool}, h_eq: {eq(ek, k) == True{} : Bool}) -> {eq(hk, k) == False{} : Bool}:
  match ek hk k:
    case 0n hk0 k0:
      Empty.absurd({eq(hk0, k0) == False{} : Bool}, true_ne_false(h_le))
    case 1n+p 0n 0n:
      Empty.absurd({True{} == False{} : Bool}, false_ne_true(h_eq))
    case 1n+p 0n 1n+r:
      {==}
    case 1n+p 1n+q 0n:
      Empty.absurd({eq(1n+q, 0n) == False{} : Bool}, false_ne_true(h_eq))
    case 1n+p 1n+q 1n+r:
      not_le_and_eq_implies_ne(p, q, r, h_le, h_eq)

def lookup_insert_by_key_go(
  +b_le: Bool,
  +b_eq_ek: Bool,
  +ek: Nat,
  +ev: Nat,
  +hk: Nat,
  +hv: Nat,
  +t: +List<Entry>,
  +k: Nat,
  +h_le: {le(ek, hk) == b_le : Bool},
  +h_eq_ek: {eq(ek, k) == b_eq_ek : Bool},
  ih: {lookup_all(insert_by_key(t, ek, ev), k) == lookup_step(eq(ek, k), ev, lookup_all(t, k)) : +List<Nat>}
) -> {lookup_all(insert_sorted_at(b_le, ek, ev, hk, hv, t, insert_by_key(t, ek, ev)), k) == lookup_step(b_eq_ek, ev, lookup_step(eq(hk, k), hv, lookup_all(t, k))) : +List<Nat>}:
  match b_le b_eq_ek:
    case True{} True{}:
      %Equal.sym(Bool, eq(ek, k), True{}, h_eq_ek) : {lookup_step(_, ev, lookup_step(eq(hk, k), hv, lookup_all(t, k))) == Con{ev, lookup_step(eq(hk, k), hv, lookup_all(t, k))} : +List<Nat>}
      {==}
    case True{} False{}:
      %Equal.sym(Bool, eq(ek, k), False{}, h_eq_ek) : {lookup_step(_, ev, lookup_step(eq(hk, k), hv, lookup_all(t, k))) == lookup_step(eq(hk, k), hv, lookup_all(t, k)) : +List<Nat>}
      {==}
    case False{} True{}:
      %Equal.sym(Bool, eq(hk, k), False{}, not_le_and_eq_implies_ne(ek, hk, k, h_le, h_eq_ek)) : {lookup_step(_, hv, lookup_all(insert_by_key(t, ek, ev), k)) == Con{ev, lookup_step(_, hv, lookup_all(t, k))} : +List<Nat>}
      %Equal.sym(+List<Nat>, lookup_all(insert_by_key(t, ek, ev), k), lookup_step(eq(ek, k), ev, lookup_all(t, k)), ih) : {_ == Con{ev, lookup_all(t, k)} : +List<Nat>}
      %Equal.sym(Bool, eq(ek, k), True{}, h_eq_ek) : {lookup_step(_, ev, lookup_all(t, k)) == Con{ev, lookup_all(t, k)} : +List<Nat>}
      {==}
    case False{} False{}:
      %Equal.sym(+List<Nat>, lookup_all(insert_by_key(t, ek, ev), k), lookup_step(eq(ek, k), ev, lookup_all(t, k)), ih) : {lookup_step(eq(hk, k), hv, _) == lookup_step(eq(hk, k), hv, lookup_all(t, k)) : +List<Nat>}
      %Equal.sym(Bool, eq(ek, k), False{}, h_eq_ek) : {lookup_step(eq(hk, k), hv, lookup_step(_, ev, lookup_all(t, k))) == lookup_step(eq(hk, k), hv, lookup_all(t, k)) : +List<Nat>}
      {==}

def lookup_insert_by_key(+s: +List<Entry>, +ek: Nat, +ev: Nat, +k: Nat) -> {lookup_all(insert_by_key(s, ek, ev), k) == lookup_step(eq(ek, k), ev, lookup_all(s, k)) : +List<Nat>}:
  match s:
    case Nil{}:
      {==}
    case Con{h, t}:
      match h:
        case Pair{hk, hv}:
          lookup_insert_by_key_go(le(ek, hk), eq(ek, k), ek, ev, hk, hv, t, k, {==}, {==}, lookup_insert_by_key(t, ek, ev, k))

def lookup_seal(+m: +List<Entry>, +k: Nat) -> {lookup_all(seal(m), k) == lookup_all(m, k) : +List<Nat>}:
  match m:
    case Nil{}:
      {==}
    case Con{e, t}:
      match e:
        case Pair{ek, ev}:
          %Equal.sym(+List<Nat>, lookup_all(insert_by_key(seal(t), ek, ev), k), lookup_step(eq(ek, k), ev, lookup_all(seal(t), k)), lookup_insert_by_key(seal(t), ek, ev, k)) : {_ == lookup_step(eq(ek, k), ev, lookup_all(t, k)) : +List<Nat>}
          %Equal.sym(+List<Nat>, lookup_all(seal(t), k), lookup_all(t, k), lookup_seal(t, k)) : {lookup_step(eq(ek, k), ev, _) == lookup_step(eq(ek, k), ev, lookup_all(t, k)) : +List<Nat>}
          {==}

def lookup_promote(+s: +List<Entry>, +k: Nat) -> {lookup_all(promote(s), k) == lookup_all(s, k) : +List<Nat>}:
  match s:
    case Nil{}:
      {==}
    case Con{e, t}:
      match e:
        case Pair{ek, ev}:
          %Equal.sym(+List<Nat>, lookup_all(promote(t), k), lookup_all(t, k), lookup_promote(t, k)) : {lookup_step(eq(ek, k), ev, _) == lookup_step(eq(ek, k), ev, lookup_all(t, k)) : +List<Nat>}
          {==}

# ---- Laws -------------------------------------------------------------------

law maplet_insert_finds_value:
  for +m: +List<Entry>
  for +k: Nat
  for +v: Nat
  {list_mem(lookup_all(insert(m, k, v), k), v) == True{} : Bool}

law maplet_collision_preserves_older:
  for +m: +List<Entry>
  for +k: Nat
  for +v_old: Nat
  for +v_new: Nat
  {list_mem(lookup_all(insert(insert(m, k, v_old), k, v_new), k), v_old) == True{} : Bool}

law seal_promote_preserves_lookup:
  for +m: +List<Entry>
  for +k: Nat
  {lookup_all(promote(seal(m)), k) == lookup_all(m, k) : +List<Nat>}

# ---- Proofs -----------------------------------------------------------------

def maplet_insert_finds_value(m, k, v):
  %Equal.sym(Bool, eq(k, k), True{}, eq_refl(k)) : {list_mem(lookup_step(_, v, lookup_all(m, k)), v) == True{} : Bool}
  %Equal.sym(Bool, eq(v, v), True{}, eq_refl(v)) : {either(_, list_mem(lookup_all(m, k), v)) == True{} : Bool}
  {==}

def maplet_collision_preserves_older(m, k, v_old, v_new):
  %Equal.sym(Bool, eq(k, k), True{}, eq_refl(k)) : {list_mem(lookup_step(_, v_new, lookup_step(_, v_old, lookup_all(m, k))), v_old) == True{} : Bool}
  %Equal.sym(Bool, eq(v_old, v_old), True{}, eq_refl(v_old)) : {either(eq(v_new, v_old), either(_, list_mem(lookup_all(m, k), v_old))) == True{} : Bool}
  either_true_r(eq(v_new, v_old))

def seal_promote_preserves_lookup(m, k):
  %Equal.sym(+List<Nat>, lookup_all(promote(seal(m)), k), lookup_all(seal(m), k), lookup_promote(seal(m), k)) : {_ == lookup_all(m, k) : +List<Nat>}
  lookup_seal(m, k)
