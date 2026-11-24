# Algebraic and patch laws for bitmosaic sets (specs/bitmosaic/01-format.md).
#
# The model: a bitmosaic set is represented as a bitmap `+List<Bool>` (lowest member first; positions past the end are
# absent), alongside a canonical member list `+List<Nat>` lowered to a bitmap by `from_list`.
#
# Proven here, for every bitmap, every member list, and every member:
# - and3_equals_pairwise_and: fused 3-way intersection `band3(a, b, c)` is structurally identical to `band(band(a, b), c)`;
# - and3_membership: `band3(a, b, c)` contains `x` iff all three inputs contain `x`;
# - and_associative: `band(band(a, b), c) == band(a, band(b, c))` structurally;
# - and_commutative: `band(a, b) == band(b, a)` structurally;
# - andnot_spec: `mem(bandnot(a, b), x) == both(mem(a, x), notb(mem(b, x)))`;
# - patch_add_preserves_canonical_membership: in-place bit set matches `{k} U bs` on every member `x`;
# - patch_remove_preserves_canonical_membership: in-place bit clear matches `bs \ {k}` on every member `x`;
# - patch_add_matches_rebuild: patching a bitmap built from `xs` has the exact same membership as rebuilding from
#   `insert_sorted(xs, k)`;
# - patch_remove_matches_rebuild: clearing a bit in a bitmap built from `xs` has the exact same membership as
#   rebuilding from `remove_all(xs, k)`.
#
# All laws use the first-order bend-emit fragment (Nat, Bool, +List<Nat>, +List<Bool>).
import Base

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
    case a0 0n:
      False{}
    case 0n 1n+q:
      True{}
    case 1n+p 1n+q:
      lt(p, q)

def notb(+a: Bool) -> Bool:
  match a:
    case True{}:
      False{}
    case False{}:
      True{}

def both(+a: Bool, b: Bool) -> Bool:
  match a:
    case True{}:
      b
    case False{}:
      False{}

def either(+a: Bool, b: Bool) -> Bool:
  match a:
    case True{}:
      True{}
    case False{}:
      b

def mem(+bs: +List<Bool>, +i: Nat) -> Bool:
  match bs i:
    case Nil{} i0:
      False{}
    case Con{b, t} 0n:
      b
    case Con{b, t} 1n+j:
      mem(t, j)

def band(+a: +List<Bool>, +b: +List<Bool>) -> +List<Bool>:
  match a b:
    case Nil{} b0:
      Nil{}
    case Con{x, t} Nil{}:
      Nil{}
    case Con{x, t} Con{y, u}:
      Con{both(x, y), band(t, u)}

# Fused single-pass 3-way intersection.
def band3(+a: +List<Bool>, +b: +List<Bool>, +c: +List<Bool>) -> +List<Bool>:
  match a b c:
    case Nil{} b0 c0:
      Nil{}
    case Con{x, t} Nil{} c0:
      Nil{}
    case Con{x, t} Con{y, u} Nil{}:
      Nil{}
    case Con{x, t} Con{y, u} Con{z, v}:
      Con{both(both(x, y), z), band3(t, u, v)}

# Bitwise difference: members of `a` not in `b`.
def bandnot(+a: +List<Bool>, +b: +List<Bool>) -> +List<Bool>:
  match a b:
    case Nil{} b0:
      Nil{}
    case Con{x, t} Nil{}:
      Con{x, t}
    case Con{x, t} Con{y, u}:
      Con{both(x, notb(y)), bandnot(t, u)}

# In-place patch: set bit `k`, extending with False if `k` lies past the end.
def patch_add(+bs: +List<Bool>, +k: Nat) -> +List<Bool>:
  match bs k:
    case Nil{} 0n:
      Con{True{}, Nil{}}
    case Nil{} 1n+j:
      Con{False{}, patch_add(Nil{}, j)}
    case Con{b, t} 0n:
      Con{True{}, t}
    case Con{b, t} 1n+j:
      Con{b, patch_add(t, j)}

# In-place patch: clear bit `k`.
def patch_remove(+bs: +List<Bool>, +k: Nat) -> +List<Bool>:
  match bs k:
    case Nil{} k0:
      Nil{}
    case Con{b, t} 0n:
      Con{False{}, t}
    case Con{b, t} 1n+j:
      Con{b, patch_remove(t, j)}

# Build a bitmap from a member list.
def from_list(+xs: +List<Nat>) -> +List<Bool>:
  match xs:
    case Nil{}:
      Nil{}
    case Con{h, t}:
      patch_add(from_list(t), h)

def list_contains(+xs: +List<Nat>, +v: Nat) -> Bool:
  match xs:
    case Nil{}:
      False{}
    case Con{h, t}:
      either(eq(h, v), list_contains(t, v))

def insert_sorted.at(+b_lt: Bool, +b_eq: Bool, k: Nat, h: Nat, t: +List<Nat>, rec: +List<Nat>) -> +List<Nat>:
  match b_lt:
    case True{}:
      Con{k, Con{h, t}}
    case False{}:
      match b_eq:
        case True{}:
          Con{h, t}
        case False{}:
          Con{h, rec}

# Sorted insertion into a member list (deduplicating equal keys).
def insert_sorted(+xs: +List<Nat>, +k: Nat) -> +List<Nat>:
  match xs:
    case Nil{}:
      Con{k, Nil{}}
    case Con{h, t}:
      insert_sorted.at(lt(k, h), eq(k, h), k, h, t, insert_sorted(t, k))

def remove_all.at(+b_eq: Bool, h: Nat, rec: +List<Nat>) -> +List<Nat>:
  match b_eq:
    case True{}:
      rec
    case False{}:
      Con{h, rec}

# Remove all occurrences of `k` from a member list (identical to sorted removal on ascending lists).
def remove_all(+xs: +List<Nat>, +k: Nat) -> +List<Nat>:
  match xs:
    case Nil{}:
      Nil{}
    case Con{h, t}:
      remove_all.at(eq(k, h), h, remove_all(t, k))

# ---- Boolean helpers --------------------------------------------------------

def both_false_r(+a: Bool) -> {both(a, False{}) == False{} : Bool}:
  match a:
    case True{}:
      {==}
    case False{}:
      {==}

def both_true_r(+a: Bool) -> {both(a, True{}) == a : Bool}:
  match a:
    case True{}:
      {==}
    case False{}:
      {==}

def both_comm(+a: Bool, +b: Bool) -> {both(a, b) == both(b, a) : Bool}:
  match a b:
    case True{} True{}:
      {==}
    case True{} False{}:
      {==}
    case False{} True{}:
      {==}
    case False{} False{}:
      {==}

def both_assoc(+a: Bool, +b: Bool, +c: Bool) -> {both(both(a, b), c) == both(a, both(b, c)) : Bool}:
  match a:
    case True{}:
      {==}
    case False{}:
      {==}

def either_false_r(+a: Bool) -> {either(a, False{}) == a : Bool}:
  match a:
    case True{}:
      {==}
    case False{}:
      {==}

def either_comm(+a: Bool, +b: Bool) -> {either(a, b) == either(b, a) : Bool}:
  match a b:
    case True{} True{}:
      {==}
    case True{} False{}:
      {==}
    case False{} True{}:
      {==}
    case False{} False{}:
      {==}

def either_assoc(+a: Bool, +b: Bool, +c: Bool) -> {either(either(a, b), c) == either(a, either(b, c)) : Bool}:
  match a:
    case True{}:
      {==}
    case False{}:
      {==}

def both_either_dist(+a: Bool, +b: Bool, +c: Bool) -> {both(a, either(b, c)) == either(both(a, b), both(a, c)) : Bool}:
  match a:
    case True{}:
      {==}
    case False{}:
      {==}

def eq_sym(+a: Nat, +b: Nat) -> {eq(a, b) == eq(b, a) : Bool}:
  match a b:
    case 0n 0n:
      {==}
    case 0n 1n+q:
      {==}
    case 1n+p 0n:
      {==}
    case 1n+p 1n+q:
      eq_sym(p, q)

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

def eq_trans_bool(+k: Nat, +h: Nat, +x: Nat, e: {eq(k, h) == True{} : Bool}) -> {eq(k, x) == eq(h, x) : Bool}:
  match k h x:
    case 0n 0n x0:
      {==}
    case 0n 1n+q x0:
      Empty.absurd({eq(0n, x0) == eq(1n+q, x0) : Bool}, false_ne_true(e))
    case 1n+p 0n x0:
      Empty.absurd({eq(1n+p, x0) == eq(0n, x0) : Bool}, false_ne_true(e))
    case 1n+p 1n+q 0n:
      {==}
    case 1n+p 1n+q 1n+r:
      eq_trans_bool(p, q, r, e)

def either_idem(+a: Bool) -> {either(a, a) == a : Bool}:
  match a:
    case True{}:
      {==}
    case False{}:
      {==}

def mem_band(+a: +List<Bool>, +b: +List<Bool>, +i: Nat) -> {mem(band(a, b), i) == both(mem(a, i), mem(b, i)) : Bool}:
  match a b i:
    case Nil{} b0 i0:
      {==}
    case Con{x, t} Nil{} 0n:
      Equal.sym(Bool, both(x, False{}), False{}, both_false_r(x))
    case Con{x, t} Nil{} 1n+j:
      Equal.sym(Bool, both(mem(t, j), False{}), False{}, both_false_r(mem(t, j)))
    case Con{x, t} Con{y, u} 0n:
      {==}
    case Con{x, t} Con{y, u} 1n+j:
      mem_band(t, u, j)

# ---- Laws -------------------------------------------------------------------

law and3_equals_pairwise_and:
  for +a: +List<Bool>
  for +b: +List<Bool>
  for +c: +List<Bool>
  {band3(a, b, c) == band(band(a, b), c) : +List<Bool>}

law and3_membership:
  for +a: +List<Bool>
  for +b: +List<Bool>
  for +c: +List<Bool>
  for +x: Nat
  {mem(band3(a, b, c), x) == both(both(mem(a, x), mem(b, x)), mem(c, x)) : Bool}

law and_associative:
  for +a: +List<Bool>
  for +b: +List<Bool>
  for +c: +List<Bool>
  {band(band(a, b), c) == band(a, band(b, c)) : +List<Bool>}

law and_commutative:
  for +a: +List<Bool>
  for +b: +List<Bool>
  {band(a, b) == band(b, a) : +List<Bool>}

law andnot_spec:
  for +a: +List<Bool>
  for +b: +List<Bool>
  for +x: Nat
  {mem(bandnot(a, b), x) == both(mem(a, x), notb(mem(b, x))) : Bool}

law patch_add_preserves_canonical_membership:
  for +bs: +List<Bool>
  for +k: Nat
  for +x: Nat
  {mem(patch_add(bs, k), x) == either(eq(k, x), mem(bs, x)) : Bool}

law patch_remove_preserves_canonical_membership:
  for +bs: +List<Bool>
  for +k: Nat
  for +x: Nat
  {mem(patch_remove(bs, k), x) == both(notb(eq(k, x)), mem(bs, x)) : Bool}

law patch_add_matches_rebuild:
  for +xs: +List<Nat>
  for +k: Nat
  for +x: Nat
  {mem(patch_add(from_list(xs), k), x) == mem(from_list(insert_sorted(xs, k)), x) : Bool}

law patch_remove_matches_rebuild:
  for +xs: +List<Nat>
  for +k: Nat
  for +x: Nat
  {mem(patch_remove(from_list(xs), k), x) == mem(from_list(remove_all(xs, k)), x) : Bool}

# ---- Proofs -----------------------------------------------------------------

def and3_equals_pairwise_and(a, b, c):
  match a b c:
    case Nil{} b0 c0:
      {==}
    case Con{x, t} Nil{} c0:
      {==}
    case Con{x, t} Con{y, u} Nil{}:
      {==}
    case Con{x, t} Con{y, u} Con{z, v}:
      %Equal.sym(+List<Bool>, band3(t, u, v), band(band(t, u), v), and3_equals_pairwise_and(t, u, v)) : {Con{both(both(x, y), z), _} == Con{both(both(x, y), z), band(band(t, u), v)} : +List<Bool>}
      {==}

def and3_membership(a, b, c, x):
  %Equal.sym(+List<Bool>, band3(a, b, c), band(band(a, b), c), and3_equals_pairwise_and(a, b, c)) : {mem(_, x) == both(both(mem(a, x), mem(b, x)), mem(c, x)) : Bool}
  %Equal.sym(Bool, mem(band(band(a, b), c), x), both(mem(band(a, b), x), mem(c, x)), mem_band(band(a, b), c, x)) : {_ == both(both(mem(a, x), mem(b, x)), mem(c, x)) : Bool}
  %Equal.sym(Bool, mem(band(a, b), x), both(mem(a, x), mem(b, x)), mem_band(a, b, x)) : {both(_, mem(c, x)) == both(both(mem(a, x), mem(b, x)), mem(c, x)) : Bool}
  {==}

def and_associative(a, b, c):
  match a b c:
    case Nil{} b0 c0:
      {==}
    case Con{x, t} Nil{} c0:
      {==}
    case Con{x, t} Con{y, u} Nil{}:
      {==}
    case Con{x, t} Con{y, u} Con{z, v}:
      %Equal.sym(Bool, both(both(x, y), z), both(x, both(y, z)), both_assoc(x, y, z)) : {Con{_, band(band(t, u), v)} == Con{both(x, both(y, z)), band(t, band(u, v))} : +List<Bool>}
      %Equal.sym(+List<Bool>, band(band(t, u), v), band(t, band(u, v)), and_associative(t, u, v)) : {Con{both(x, both(y, z)), _} == Con{both(x, both(y, z)), band(t, band(u, v))} : +List<Bool>}
      {==}

def and_commutative(a, b):
  match a b:
    case Nil{} Nil{}:
      {==}
    case Nil{} Con{y, u}:
      {==}
    case Con{x, t} Nil{}:
      {==}
    case Con{x, t} Con{y, u}:
      %Equal.sym(Bool, both(x, y), both(y, x), both_comm(x, y)) : {Con{_, band(t, u)} == Con{both(y, x), band(u, t)} : +List<Bool>}
      %Equal.sym(+List<Bool>, band(t, u), band(u, t), and_commutative(t, u)) : {Con{both(y, x), _} == Con{both(y, x), band(u, t)} : +List<Bool>}
      {==}

def andnot_spec(a, b, x):
  match a b x:
    case Nil{} b0 x0:
      {==}
    case Con{h, t} Nil{} 0n:
      Equal.sym(Bool, both(h, True{}), h, both_true_r(h))
    case Con{h, t} Nil{} 1n+j:
      Equal.sym(Bool, both(mem(t, j), True{}), mem(t, j), both_true_r(mem(t, j)))
    case Con{h, t} Con{y, u} 0n:
      {==}
    case Con{h, t} Con{y, u} 1n+j:
      andnot_spec(t, u, j)

def patch_add_preserves_canonical_membership(bs, k, x):
  match bs k x:
    case Nil{} 0n 0n:
      {==}
    case Nil{} 0n 1n+r:
      {==}
    case Nil{} 1n+j 0n:
      {==}
    case Nil{} 1n+j 1n+r:
      patch_add_preserves_canonical_membership(Nil{}, j, r)
    case Con{b, t} 0n 0n:
      {==}
    case Con{b, t} 0n 1n+r:
      {==}
    case Con{b, t} 1n+j 0n:
      {==}
    case Con{b, t} 1n+j 1n+r:
      patch_add_preserves_canonical_membership(t, j, r)

def patch_remove_preserves_canonical_membership(bs, k, x):
  match bs k x:
    case Nil{} 0n 0n:
      {==}
    case Nil{} 0n 1n+r:
      {==}
    case Nil{} 1n+j 0n:
      {==}
    case Nil{} 1n+j 1n+r:
      Equal.sym(Bool, both(notb(eq(j, r)), False{}), False{}, both_false_r(notb(eq(j, r))))
    case Con{b, t} 0n 0n:
      {==}
    case Con{b, t} 0n 1n+r:
      {==}
    case Con{b, t} 1n+j 0n:
      {==}
    case Con{b, t} 1n+j 1n+r:
      patch_remove_preserves_canonical_membership(t, j, r)

def from_list_spec(+xs: +List<Nat>, +x: Nat) -> {mem(from_list(xs), x) == list_contains(xs, x) : Bool}:
  match xs:
    case Nil{}:
      {==}
    case Con{h, t}:
      %Equal.sym(Bool, mem(patch_add(from_list(t), h), x), either(eq(h, x), mem(from_list(t), x)), patch_add_preserves_canonical_membership(from_list(t), h, x)) : {_ == either(eq(h, x), list_contains(t, x)) : Bool}
      %Equal.sym(Bool, mem(from_list(t), x), list_contains(t, x), from_list_spec(t, x)) : {either(eq(h, x), _) == either(eq(h, x), list_contains(t, x)) : Bool}
      {==}

def insert_sorted_contains_eq(+k: Nat, +h: Nat, +t: +List<Nat>, +x: Nat, e: {eq(k, h) == True{} : Bool}) -> {either(eq(k, x), either(eq(h, x), list_contains(t, x))) == either(eq(h, x), list_contains(t, x)) : Bool}:
  %Equal.sym(Bool, eq(k, x), eq(h, x), eq_trans_bool(k, h, x, e)) : {either(_, either(eq(h, x), list_contains(t, x))) == either(eq(h, x), list_contains(t, x)) : Bool}
  %either_assoc(eq(h, x), eq(h, x), list_contains(t, x)) : {_ == either(eq(h, x), list_contains(t, x)) : Bool}
  %Equal.sym(Bool, either(eq(h, x), eq(h, x)), eq(h, x), either_idem(eq(h, x))) : {either(_, list_contains(t, x)) == either(eq(h, x), list_contains(t, x)) : Bool}
  {==}

def insert_sorted_contains_go(+k: Nat, +h: Nat, +t: +List<Nat>, +x: Nat, +b_lt: Bool, +b_eq: Bool, h_eq: {eq(k, h) == b_eq : Bool}, ih: {list_contains(insert_sorted(t, k), x) == either(eq(k, x), list_contains(t, x)) : Bool}) -> {list_contains(insert_sorted.at(b_lt, b_eq, k, h, t, insert_sorted(t, k)), x) == either(eq(k, x), either(eq(h, x), list_contains(t, x))) : Bool}:
  match b_lt:
    case True{}:
      {==}
    case False{}:
      match b_eq:
        case True{}:
          Equal.sym(Bool, either(eq(k, x), either(eq(h, x), list_contains(t, x))), either(eq(h, x), list_contains(t, x)), insert_sorted_contains_eq(k, h, t, x, h_eq))
        case False{}:
          %Equal.sym(Bool, list_contains(insert_sorted(t, k), x), either(eq(k, x), list_contains(t, x)), ih) : {either(eq(h, x), _) == either(eq(k, x), either(eq(h, x), list_contains(t, x))) : Bool}
          %either_assoc(eq(h, x), eq(k, x), list_contains(t, x)) : {_ == either(eq(k, x), either(eq(h, x), list_contains(t, x))) : Bool}
          %Equal.sym(Bool, either(eq(h, x), eq(k, x)), either(eq(k, x), eq(h, x)), either_comm(eq(h, x), eq(k, x))) : {either(_, list_contains(t, x)) == either(eq(k, x), either(eq(h, x), list_contains(t, x))) : Bool}
          either_assoc(eq(k, x), eq(h, x), list_contains(t, x))

def insert_sorted_contains(+xs: +List<Nat>, +k: Nat, +x: Nat) -> {list_contains(insert_sorted(xs, k), x) == either(eq(k, x), list_contains(xs, x)) : Bool}:
  match xs:
    case Nil{}:
      {==}
    case Con{h, t}:
      insert_sorted_contains_go(k, h, t, x, lt(k, h), eq(k, h), {==}, insert_sorted_contains(t, k, x))

def both_not_self(+b: Bool) -> {both(notb(b), b) == False{} : Bool}:
  match b:
    case True{}:
      {==}
    case False{}:
      {==}

def remove_all_head_eq(+k: Nat, +h: Nat, +x: Nat, e: {eq(k, h) == True{} : Bool}) -> {both(notb(eq(k, x)), eq(h, x)) == False{} : Bool}:
  %Equal.sym(Bool, eq(k, x), eq(h, x), eq_trans_bool(k, h, x, e)) : {both(notb(_), eq(h, x)) == False{} : Bool}
  both_not_self(eq(h, x))

def eq_trans_ne(+k: Nat, +h: Nat, +x: Nat, e_kh: {eq(k, h) == False{} : Bool}, e_hx: {eq(h, x) == True{} : Bool}) -> {eq(k, x) == False{} : Bool}:
  match k h x:
    case 0n 0n x0:
      Empty.absurd({eq(0n, x0) == False{} : Bool}, true_ne_false(e_kh))
    case 0n 1n+q 0n:
      Empty.absurd({True{} == False{} : Bool}, false_ne_true(e_hx))
    case 0n 1n+q 1n+r:
      {==}
    case 1n+p 0n 0n:
      {==}
    case 1n+p 0n 1n+r:
      Empty.absurd({eq(1n+p, 1n+r) == False{} : Bool}, false_ne_true(e_hx))
    case 1n+p 1n+q 0n:
      {==}
    case 1n+p 1n+q 1n+r:
      eq_trans_ne(p, q, r, e_kh, e_hx)

def remove_all_head_ne_aux(+k: Nat, +h: Nat, +x: Nat, +b_hx: Bool, e_kh: {eq(k, h) == False{} : Bool}, e_hx: {eq(h, x) == b_hx : Bool}) -> {both(notb(eq(k, x)), b_hx) == b_hx : Bool}:
  match b_hx:
    case True{}:
      %Equal.sym(Bool, eq(k, x), False{}, eq_trans_ne(k, h, x, e_kh, e_hx)) : {both(notb(_), True{}) == True{} : Bool}
      {==}
    case False{}:
      both_false_r(notb(eq(k, x)))

def remove_all_contains_go(+k: Nat, +h: Nat, +t: +List<Nat>, +x: Nat, +b_eq: Bool, e_kh: {eq(k, h) == b_eq : Bool}, ih: {list_contains(remove_all(t, k), x) == both(notb(eq(k, x)), list_contains(t, x)) : Bool}) -> {list_contains(remove_all.at(b_eq, h, remove_all(t, k)), x) == both(notb(eq(k, x)), either(eq(h, x), list_contains(t, x))) : Bool}:
  match b_eq:
    case True{}:
      %Equal.sym(Bool, list_contains(remove_all(t, k), x), both(notb(eq(k, x)), list_contains(t, x)), ih) : {_ == both(notb(eq(k, x)), either(eq(h, x), list_contains(t, x))) : Bool}
      %Equal.sym(Bool, both(notb(eq(k, x)), either(eq(h, x), list_contains(t, x))), either(both(notb(eq(k, x)), eq(h, x)), both(notb(eq(k, x)), list_contains(t, x))), both_either_dist(notb(eq(k, x)), eq(h, x), list_contains(t, x))) : {both(notb(eq(k, x)), list_contains(t, x)) == _ : Bool}
      %Equal.sym(Bool, both(notb(eq(k, x)), eq(h, x)), False{}, remove_all_head_eq(k, h, x, e_kh)) : {both(notb(eq(k, x)), list_contains(t, x)) == either(_, both(notb(eq(k, x)), list_contains(t, x))) : Bool}
      {==}
    case False{}:
      %Equal.sym(Bool, list_contains(remove_all(t, k), x), both(notb(eq(k, x)), list_contains(t, x)), ih) : {either(eq(h, x), _) == both(notb(eq(k, x)), either(eq(h, x), list_contains(t, x))) : Bool}
      %Equal.sym(Bool, both(notb(eq(k, x)), either(eq(h, x), list_contains(t, x))), either(both(notb(eq(k, x)), eq(h, x)), both(notb(eq(k, x)), list_contains(t, x))), both_either_dist(notb(eq(k, x)), eq(h, x), list_contains(t, x))) : {either(eq(h, x), both(notb(eq(k, x)), list_contains(t, x))) == _ : Bool}
      %Equal.sym(Bool, both(notb(eq(k, x)), eq(h, x)), eq(h, x), remove_all_head_ne_aux(k, h, x, eq(h, x), e_kh, {==})) : {either(eq(h, x), both(notb(eq(k, x)), list_contains(t, x))) == either(_, both(notb(eq(k, x)), list_contains(t, x))) : Bool}
      {==}

def remove_all_contains(+xs: +List<Nat>, +k: Nat, +x: Nat) -> {list_contains(remove_all(xs, k), x) == both(notb(eq(k, x)), list_contains(xs, x)) : Bool}:
  match xs:
    case Nil{}:
      Equal.sym(Bool, both(notb(eq(k, x)), False{}), False{}, both_false_r(notb(eq(k, x))))
    case Con{h, t}:
      remove_all_contains_go(k, h, t, x, eq(k, h), {==}, remove_all_contains(t, k, x))

def patch_add_matches_rebuild(xs, k, x):
  %Equal.sym(Bool, mem(patch_add(from_list(xs), k), x), either(eq(k, x), mem(from_list(xs), x)), patch_add_preserves_canonical_membership(from_list(xs), k, x)) : {_ == mem(from_list(insert_sorted(xs, k)), x) : Bool}
  %Equal.sym(Bool, mem(from_list(xs), x), list_contains(xs, x), from_list_spec(xs, x)) : {either(eq(k, x), _) == mem(from_list(insert_sorted(xs, k)), x) : Bool}
  %Equal.sym(Bool, mem(from_list(insert_sorted(xs, k)), x), list_contains(insert_sorted(xs, k), x), from_list_spec(insert_sorted(xs, k), x)) : {either(eq(k, x), list_contains(xs, x)) == _ : Bool}
  Equal.sym(Bool, list_contains(insert_sorted(xs, k), x), either(eq(k, x), list_contains(xs, x)), insert_sorted_contains(xs, k, x))

def patch_remove_matches_rebuild(xs, k, x):
  %Equal.sym(Bool, mem(patch_remove(from_list(xs), k), x), both(notb(eq(k, x)), mem(from_list(xs), x)), patch_remove_preserves_canonical_membership(from_list(xs), k, x)) : {_ == mem(from_list(remove_all(xs, k)), x) : Bool}
  %Equal.sym(Bool, mem(from_list(xs), x), list_contains(xs, x), from_list_spec(xs, x)) : {both(notb(eq(k, x)), _) == mem(from_list(remove_all(xs, k)), x) : Bool}
  %Equal.sym(Bool, mem(from_list(remove_all(xs, k)), x), list_contains(remove_all(xs, k), x), from_list_spec(remove_all(xs, k), x)) : {both(notb(eq(k, x)), list_contains(xs, x)) == _ : Bool}
  Equal.sym(Bool, list_contains(remove_all(xs, k), x), both(notb(eq(k, x)), list_contains(xs, x)), remove_all_contains(xs, k, x))
