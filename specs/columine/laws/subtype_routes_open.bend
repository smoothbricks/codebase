# The subtype routing law refuted by the code before subtype rows were read (specs/columine/10-physical-vocabulary.md
# §7, vocab-subtype-routes), over the model in subtype_routes.bend: routing rows keyed by (event, member) alone, so a
# row naming a subtype never places its member. It has no proof of the law: it is a red. The counterexample is proven:
#   rows = [Row{1n, 2n, 3n, 4n}] (event 1, subtype 1, member 3, field 4), asked for (event 1, subtype 1, member 3):
#   nothing places it (0n), so the member was lost; the lanes stored a null record cell.
#
# Check: bun bend2/main.ts specs/columine/laws/subtype_routes_open.bend --verdict (SOME PROOFS FAIL is the expected verdict)
import Base
import ./subtype_routes.bend as R

def place_open(+rows: +List<R.Row>, +e: Nat, +s: Nat, +m: Nat) -> Nat:
  R.named(rows, e, 0n, m)

# Open, refuted by the code as it was.
law a_subtype_row_places_its_member_open:
  for +rows: +List<R.Row>
  for +e: Nat
  for +s: Nat
  for +m: Nat
  for +f: Nat
  {place_open(Con{R.Row{e, 1n+s, m, f}, rows}, e, s, m) == 1n+f : Nat}

law the_member_is_lost:
  {place_open([R.Row{1n, 2n, 3n, 4n}], 1n, 1n, 3n) == 0n : Nat}

def the_member_is_lost():
  {==}