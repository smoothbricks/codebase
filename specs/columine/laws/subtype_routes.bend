# Subtype routing rows (specs/columine/10-physical-vocabulary.md §7, vocab-subtype-routes): "A routing row may also name
# a subtype ... Such a row places its member only on rows whose key member holds that text ...; a row of no subtype
# places the member on every row of its type that no subtype row places it on", and "a payload member no column and no
# routing row places is refused by name, never dropped".
#
# The model: a routing row is (event, subtype, member, field), its subtype `0n` for none and `1n+s` for subtype `s`. A
# row of an event whose key member holds `s` asks `place(rows, e, s, m)`: the field of the first row naming its subtype,
# else of the first row naming none, `1n+field`; `0n` is a member nothing places, which the lanes refuse
# (`UnplacedMember`). The model takes the row's subtype as already read from the row; reading it (nested or flat,
# before or after the member) is the lanes' and is tested there. Columns are left out: a member a column places never
# asks for a routing row.
#
# Proven here, for every table, event, subtype, member and field:
# - a_subtype_row_places_its_member: a row naming (event, subtype, member) places that member on rows of that subtype;
# - a_row_of_another_member_places_nothing: a row naming another member leaves every placement as it was;
# - an_empty_table_places_nothing: so a member no row names is refused (with the step above, for every table whose
#   rows name other members).
# Open control: subtype_routes_open.bend, the model before subtype rows were read (only rows of no subtype place).
#
# Check: bun bend2/main.ts specs/columine/laws/subtype_routes.bend --verdict
# Bound to the code by the tests of packages/columine/crates/columine-parsing/tests/record_routes.rs
# (a_subtype_routing_row_places_its_member_in_the_record_on_rows_of_its_subtype,
# a_payload_member_nothing_places_is_refused_by_name_where_the_schema_has_a_record): the driver emits TypeScript, and
# this code is Rust, so the binding is those tests over the model's cases.
import Base

type Row is Data:
  Row{event: Nat, subtype: Nat, member: Nat, field: Nat}

def same(+a: Nat, +b: Nat) -> Bool:
  match a b:
    case 0n 0n:
      True{}
    case 0n 1n+q:
      False{}
    case 1n+p 0n:
      False{}
    case 1n+p 1n+q:
      same(p, q)

def both(+a: Bool, b: Bool) -> Bool:
  match a:
    case True{}:
      b
    case False{}:
      False{}

def pick(+b: Bool, x: Nat, y: Nat) -> Nat:
  match b:
    case True{}:
      x
    case False{}:
      y

# Whether a row naming (re, rs, rm) names (e, s, m): its member first, so a row of another member is told at once.
def hit(+rm: Nat, +m: Nat, +re: Nat, +e: Nat, +rs: Nat, +s: Nat) -> Bool:
  both(same(rm, m), both(same(re, e), same(rs, s)))

# The first row naming exactly (e, s, m): `1n+field`, or `0n` when none does.
def named(+rows: +List<Row>, +e: Nat, +s: Nat, +m: Nat) -> Nat:
  match rows:
    case Nil{}:
      0n
    case Con{r, t}:
      match r:
        case Row{re, rs, rm, rf}:
          pick(hit(rm, m, re, e, rs, s), 1n+rf, named(t, e, s, m))

# A found place wins; else the other.
def first(+a: Nat, b: Nat) -> Nat:
  match a:
    case 0n:
      b
    case 1n+x:
      1n+x

def place(+rows: +List<Row>, +e: Nat, +s: Nat, +m: Nat) -> Nat:
  first(named(rows, e, 1n+s, m), named(rows, e, 0n, m))

law a_subtype_row_places_its_member:
  for +rows: +List<Row>
  for +e: Nat
  for +s: Nat
  for +m: Nat
  for +f: Nat
  {place(Con{Row{e, 1n+s, m, f}, rows}, e, s, m) == 1n+f : Nat}

law a_row_of_another_member_places_nothing:
  for +rows: +List<Row>
  for +e: Nat
  for +s: Nat
  for +m: Nat
  for +re: Nat
  for +rs: Nat
  for +rm: Nat
  for +f: Nat
  for h: {same(rm, m) == False{} : Bool}
  {place(Con{Row{re, rs, rm, f}, rows}, e, s, m) == place(rows, e, s, m) : Nat}

law an_empty_table_places_nothing:
  for +e: Nat
  for +s: Nat
  for +m: Nat
  {place(Nil{}, e, s, m) == 0n : Nat}

# Two subtypes of one member, each to its own field; a third subtype and another member are refused.
law the_example:
  {(place([Row{1n, 1n, 3n, 4n}, Row{1n, 2n, 3n, 5n}], 1n, 0n, 3n), place([Row{1n, 1n, 3n, 4n}, Row{1n, 2n, 3n, 5n}], 1n, 1n, 3n), place([Row{1n, 1n, 3n, 4n}, Row{1n, 2n, 3n, 5n}], 1n, 2n, 3n), place([Row{1n, 1n, 3n, 4n}, Row{1n, 2n, 3n, 5n}], 1n, 0n, 9n)) == (5n, 6n, 0n, 0n) : Nat & Nat & Nat & Nat}

# --- proofs -----------------------------------------------------------------------------------------------------------

def same_refl(+n: Nat) -> {same(n, n) == True{} : Bool}:
  match n:
    case 0n:
      {==}
    case 1n+p:
      same_refl(p)

def hit_self(+m: Nat, +e: Nat, +s: Nat) -> {hit(m, m, e, e, s, s) == True{} : Bool}:
  %Equal.sym(Bool, same(m, m), True{}, same_refl(m)) : {both(_, both(same(e, e), same(s, s))) == True{} : Bool}
  %Equal.sym(Bool, same(e, e), True{}, same_refl(e)) : {both(True{}, both(_, same(s, s))) == True{} : Bool}
  %Equal.sym(Bool, same(s, s), True{}, same_refl(s)) : {both(True{}, both(True{}, _)) == True{} : Bool}
  {==}

def a_subtype_row_places_its_member(rows, e, s, m, f):
  %Equal.sym(Bool, hit(m, m, e, e, 1n+s, 1n+s), True{}, hit_self(m, e, 1n+s)) : {first(pick(_, 1n+f, named(rows, e, 1n+s, m)), named(Con{Row{e, 1n+s, m, f}, rows}, e, 0n, m)) == 1n+f : Nat}
  {==}

def a_row_of_another_member_places_nothing(rows, e, s, m, re, rs, rm, f, h):
  %Equal.sym(Bool, same(rm, m), False{}, h) : {first(pick(both(_, both(same(re, e), same(rs, 1n+s))), 1n+f, named(rows, e, 1n+s, m)), pick(both(_, both(same(re, e), same(rs, 0n))), 1n+f, named(rows, e, 0n, m))) == place(rows, e, s, m) : Nat}
  {==}

def an_empty_table_places_nothing(e, s, m):
  {==}

def the_example():
  {==}