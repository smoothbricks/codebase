# Exact-cover soundness of the per-crate nextest piece plan (packages/nx-plugin/README.md
# "Cargo Workspace Target Inference": sharded crates, exceptional overrides, timed slow tests,
# and macOS-only serial test groups).
#
# A test of a Cargo package is modelled by four orthogonal properties:
# - `exc`: whether `nextest.toml`'s exceptional override filter matches it
# - `dealt`: whether `.config/nextest-timings.json` names it (`ToShard{k}`) or not (`NotNamed{}`)
# - `ser`: whether a workspace serial test group filter matches it
# - `bucket`: nextest's hash partition bucket in `1..N` (nextest's own exact-partition contract,
#   taken as a hypothesis `in_range(bucket, shards) == True{}`; specs/cowshed/08_testing.md)
#
# Each `Piece` mirrors `StructuredCargoTestPiece` in `packages/nx-plugin/src/nextest-timings.ts`
# plus the workspace's `SerialRunner{}` when serial groups are active (macOS only):
# - `OrdUnsharded{excl_ser}`: single target of an unsharded crate (`shards == 1`)
# - `OrdShard{idx, excl_exc, excl_named, excl_ser}`: ordinary hash shard `idx` of a sharded crate
# - `ExcHash{idx, excl_ser}`: hash-partitioned exception shard `idx` (no timing manifest entries)
# - `ExcTimed{idx, take_unnamed_exc, excl_ser}`: time-dealt exception shard `idx` (the last shard
#   also takes unnamed exceptional tests when `has_exc` holds)
# - `SerialRunner{}`: the workspace's serial group runner (present iff `serial_active == True{}`)
#
# Laws proven:
# - `pieces_cover_once`: for every shard count `N >= 1`, override flag, timing flag, platform serial
#   flag, and test whose hash bucket and timed shard (if named) lie in `1..N`, `count_selected` over
#   `plan(...)` is exactly `1n`.
# - `unsharded_covers_once`: an unsharded crate (`shards == 1n`) covers every test once without
#   requiring bucket or timing hypotheses.
# - `non_macos_covers_once`: off macOS (`serial_active == False{}`), no serial runner is emitted and
#   no crate piece excludes serial tests, so every test — including `ser == True{}` — is covered once.
# - `dropping_unnamed_exc_loses_test` and `missing_serial_exclusion_double_runs`: definedness
#   witnesses showing broken plans yield `0n` (dropped test) or `2n` (double-run test).
#
# Check & emit:
#   bun packages/bend-emit/bin/bend-emit.ts specs/cowshed/laws/nextest_pieces.bend . \
#     --fns plan,selects,count_selected,valid_test \
#     --laws pieces_cover_once,unsharded_covers_once,non_macos_covers_once,dropping_unnamed_exc_loses_test,missing_serial_exclusion_double_runs \
#     --cap 400 --props 2000 --prop-len 8 --len 2 \
#     --ts-module packages/nx-plugin/src/__tests__/nextest-pieces-model.ts \
#     --model-name specs/cowshed/laws/nextest_pieces.bend
#
# Bound to the production TS piece planner (`cargoTestPieces` in `src/nextest-timings.ts`) by
# `packages/nx-plugin/src/nextest-pieces-laws.test.ts`.
import Base

type Dealt is Data:
  NotNamed{}
  ToShard{k: Nat}

type Test is Data:
  Test{exc: Bool, dealt: Dealt, ser: Bool, bucket: Nat}

type Piece is Data:
  OrdUnsharded{excl_ser: Bool}
  OrdShard{idx: Nat, excl_exc: Bool, excl_named: Bool, excl_ser: Bool}
  ExcHash{idx: Nat, excl_ser: Bool}
  ExcTimed{idx: Nat, take_unnamed_exc: Bool, excl_ser: Bool}
  SerialRunner{}

# --- helpers -------------------------------------------------------------------------

def and(a: Bool, b: Bool) -> Bool:
  match a:
    case True{}:
      b
    case False{}:
      False{}

def or(a: Bool, b: Bool) -> Bool:
  match a:
    case True{}:
      True{}
    case False{}:
      b

def not(a: Bool) -> Bool:
  match a:
    case True{}:
      False{}
    case False{}:
      True{}

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

def le(a: Nat, b: Nat) -> Bool:
  match a b:
    case 0n b0:
      True{}
    case 1n+a1 0n:
      False{}
    case 1n+a1 1n+b1:
      le(a1, b1)

def add(a: Nat, b: Nat) -> Nat:
  match a:
    case 0n:
      b
    case 1n+p:
      1n+add(p, b)

def add_bool(b: Bool, n: Nat) -> Nat:
  match b:
    case True{}:
      1n+n
    case False{}:
      n

def cat_p(xs: +List<Piece>, ys: +List<Piece>) -> +List<Piece>:
  match xs:
    case Nil{}:
      ys
    case Con{h, t}:
      h <> cat_p(t, ys)

# 1 <= x <= n
def in_range(x: Nat, n: Nat) -> Bool:
  match x:
    case 0n:
      False{}
    case 1n+p:
      le(1n+p, n)

def is_not_named(d: Dealt) -> Bool:
  match d:
    case NotNamed{}:
      True{}
    case ToShard{k}:
      False{}

def dealt_to(d: Dealt, idx: Nat) -> Bool:
  match d:
    case NotNamed{}:
      False{}
    case ToShard{k}:
      same(k, idx)

def dealt_in_range(d: Dealt, n: Nat) -> Bool:
  match d:
    case NotNamed{}:
      True{}
    case ToShard{k}:
      in_range(k, n)

# The dependency hypotheses: nextest hashes every test into 1..N, and timed dealing places every
# named test into exactly one shard k in 1..N.
def valid_test(t: Test, +shards: Nat) -> Bool:
  match t:
    case Test{exc, dealt, ser, bucket}:
      and(in_range(bucket, shards), dealt_in_range(dealt, shards))

# --- piece selection semantics -------------------------------------------------------

def passes_serial(excl_ser: Bool, ser: Bool) -> Bool:
  or(not(excl_ser), not(ser))

def ord_class_ok(excl_exc: Bool, excl_named: Bool, excl_ser: Bool, exc: Bool, dealt: Dealt, ser: Bool) -> Bool:
  and(passes_serial(excl_ser, ser), and(or(not(excl_exc), not(exc)), or(not(excl_named), is_not_named(dealt))))

def selects(p: Piece, +t: Test) -> Bool:
  match p t:
    case OrdUnsharded{excl_ser} Test{exc, dealt, ser, bucket}:
      passes_serial(excl_ser, ser)
    case OrdShard{idx, excl_exc, excl_named, excl_ser} Test{exc, dealt, ser, bucket}:
      and(ord_class_ok(excl_exc, excl_named, excl_ser, exc, dealt, ser), same(bucket, idx))
    case ExcHash{idx, excl_ser} Test{exc, dealt, ser, bucket}:
      and(and(passes_serial(excl_ser, ser), exc), same(bucket, idx))
    case ExcTimed{idx, take_unnamed_exc, excl_ser} Test{exc, +dealt, ser, bucket}:
      and(
        passes_serial(excl_ser, ser),
        or(dealt_to(dealt, idx), and(take_unnamed_exc, and(exc, is_not_named(dealt)))),
      )
    case SerialRunner{} Test{exc, dealt, ser, bucket}:
      ser

def count_selected(pieces: +List<Piece>, +t: Test) -> Nat:
  match pieces:
    case Nil{}:
      0n
    case Con{p, rest}:
      add_bool(selects(p, t), count_selected(rest, t))

# --- the piece plan ------------------------------------------------------------------

def ord_shards(+n: Nat, +excl_exc: Bool, +excl_named: Bool, +excl_ser: Bool) -> +List<Piece>:
  match n:
    case 0n:
      []
    case 1n+p:
      OrdShard{1n+p, excl_exc, excl_named, excl_ser} <> ord_shards(p, excl_exc, excl_named, excl_ser)

def exc_hash_shards(+n: Nat, +excl_ser: Bool) -> +List<Piece>:
  match n:
    case 0n:
      []
    case 1n+p:
      ExcHash{1n+p, excl_ser} <> exc_hash_shards(p, excl_ser)

def exc_timed_rest(+n: Nat, +excl_ser: Bool) -> +List<Piece>:
  match n:
    case 0n:
      []
    case 1n+p:
      ExcTimed{1n+p, False{}, excl_ser} <> exc_timed_rest(p, excl_ser)

def exc_timed_shards(+n: Nat, +has_exc: Bool, +excl_ser: Bool) -> +List<Piece>:
  match n:
    case 0n:
      []
    case 1n+p:
      ExcTimed{1n+p, has_exc, excl_ser} <> exc_timed_rest(p, excl_ser)

def exc_pieces(+n: Nat, +has_exc: Bool, +has_timed: Bool, +excl_ser: Bool) -> +List<Piece>:
  match has_exc has_timed:
    case True{} True{}:
      exc_timed_shards(n, True{}, excl_ser)
    case False{} True{}:
      exc_timed_shards(n, False{}, excl_ser)
    case True{} False{}:
      exc_hash_shards(n, excl_ser)
    case False{} False{}:
      []

def pkg_pieces(+shards: Nat, +has_exc: Bool, +has_timed: Bool, +excl_ser: Bool) -> +List<Piece>:
  match shards:
    case 0n:
      []
    case 1n:
      [OrdUnsharded{excl_ser}]
    case 2n+m:
      cat_p(
        ord_shards(2n+m, has_exc, has_timed, excl_ser),
        exc_pieces(2n+m, has_exc, has_timed, excl_ser),
      )

def plan(+shards: Nat, +has_exc: Bool, +has_timed: Bool, +serial_active: Bool) -> +List<Piece>:
  match serial_active:
    case True{}:
      SerialRunner{} <> pkg_pieces(shards, has_exc, has_timed, True{})
    case False{}:
      pkg_pieces(shards, has_exc, has_timed, False{})

# --- proof helpers -------------------------------------------------------------------

def disc(b: Bool) -> Data:
  match b:
    case True{}:
      Empty
    case False{}:
      Unit

def false_true(h: {False{} == True{} : Bool}) -> Empty:
  %h : disc(_)
  Unit{}

def sym_b(+a: Bool, +b: Bool, h: {a == b : Bool}) -> {b == a : Bool}:
  Equal.sym(Bool, a, b, h)

def sym_n(+a: Nat, +b: Nat, h: {a == b : Nat}) -> {b == a : Nat}:
  Equal.sym(Nat, a, b, h)

def and_l(a: Bool, b: Bool, h: {and(a, b) == True{} : Bool}) -> {a == True{} : Bool}:
  match a:
    case False{}:
      Empty.absurd({False{} == True{} : Bool}, false_true(h))
    case True{}:
      {==}

def and_r(a: Bool, b: Bool, h: {and(a, b) == True{} : Bool}) -> {b == True{} : Bool}:
  match a:
    case False{}:
      Empty.absurd({b == True{} : Bool}, false_true(h))
    case True{}:
      h

def and_false_r(+b: Bool) -> {False{} == and(b, False{}) : Bool}:
  match b:
    case True{}:
      {==}
    case False{}:
      {==}

def and_true_r(+b: Bool) -> {b == and(b, True{}) : Bool}:
  match b:
    case True{}:
      {==}
    case False{}:
      {==}

def add_bool_add(+b: Bool, +a: Nat, +c: Nat) -> {add_bool(b, add(a, c)) == add(add_bool(b, a), c) : Nat}:
  match b:
    case True{}:
      {==}
    case False{}:
      {==}

def count_cat(+xs: +List<Piece>, +ys: +List<Piece>, +t: Test) -> {add(count_selected(xs, t), count_selected(ys, t)) == count_selected(cat_p(xs, ys), t) : Nat}:
  match xs:
    case Nil{}:
      {==}
    case Con{+p, +rest}:
      %count_cat(rest, ys, t) : {add(add_bool(selects(p, t), count_selected(rest, t)), count_selected(ys, t)) == add_bool(selects(p, t), _) : Nat}
      sym_n(add_bool(selects(p, t), add(count_selected(rest, t), count_selected(ys, t))), add(add_bool(selects(p, t), count_selected(rest, t)), count_selected(ys, t)), add_bool_add(selects(p, t), count_selected(rest, t), count_selected(ys, t)))

def same_eq(+a: Nat, +b: Nat, h: {same(a, b) == True{} : Bool}) -> {b == a : Nat}:
  match a b:
    case 0n 0n:
      {==}
    case 0n 1n+q:
      Empty.absurd({1n+q == 0n : Nat}, false_true(h))
    case 1n+p 0n:
      Empty.absurd({0n == 1n+p : Nat}, false_true(h))
    case 1n+p 1n+q:
      %same_eq(p, q, h) : {1n+q == 1n+_ : Nat}
      {==}

def le_pred_l(+a: Nat, +b: Nat, h: {le(1n+a, b) == True{} : Bool}) -> {le(a, b) == True{} : Bool}:
  match a b:
    case 0n b0:
      {==}
    case 1n+a1 0n:
      Empty.absurd({False{} == True{} : Bool}, false_true(h))
    case 1n+a1 1n+b1:
      le_pred_l(a1, b1, h)

def le_refl(+n: Nat) -> {le(n, n) == True{} : Bool}:
  match n:
    case 0n:
      {==}
    case 1n+p:
      le_refl(p)

def gt_same_false(+x: Nat, +n: Nat, h: {le(1n+n, x) == True{} : Bool}) -> {False{} == same(x, n) : Bool}:
  match x n:
    case 0n 0n:
      Empty.absurd({False{} == True{} : Bool}, false_true(h))
    case 1n+x1 0n:
      {==}
    case 0n 1n+n1:
      Empty.absurd({False{} == False{} : Bool}, false_true(h))
    case 1n+x1 1n+n1:
      gt_same_false(x1, n1, h)

def le_neq_pred(+a: Nat, +b: Nat, h_le: {le(a, 1n+b) == True{} : Bool}, h_neq: {same(a, 1n+b) == False{} : Bool}) -> {le(a, b) == True{} : Bool}:
  match a b:
    case 0n b0:
      {==}
    case 1n 0n:
      Empty.absurd({False{} == True{} : Bool}, false_true(sym_b(True{}, False{}, h_neq)))
    case 2n+a2 0n:
      Empty.absurd({False{} == True{} : Bool}, false_true(h_le))
    case 1n+a1 1n+b1:
      le_neq_pred(a1, b1, h_le, h_neq)

def in_range_pred(+x: Nat, +p: Nat, h_in: {in_range(x, 1n+p) == True{} : Bool}, h_neq: {same(x, 1n+p) == False{} : Bool}) -> {in_range(x, p) == True{} : Bool}:
  match x:
    case 0n:
      Empty.absurd({False{} == True{} : Bool}, false_true(h_in))
    case 1n+x1:
      le_neq_pred(1n+x1, p, h_in, h_neq)

def in_range_zero(+x: Nat, h: {in_range(x, 0n) == True{} : Bool}) -> Empty:
  match x:
    case 0n:
      false_true(h)
    case 1n+x1:
      false_true(h)

# --- counting lemmas for each shard family -------------------------------------------

def count_ord_inactive(+n: Nat, +excl_exc: Bool, +excl_named: Bool, +excl_ser: Bool, +exc: Bool, +dealt: Dealt, +ser: Bool, +bucket: Nat, +h: {False{} == ord_class_ok(excl_exc, excl_named, excl_ser, exc, dealt, ser) : Bool}) -> {0n == count_selected(ord_shards(n, excl_exc, excl_named, excl_ser), Test{exc, dealt, ser, bucket}) : Nat}:
  match n:
    case 0n:
      {==}
    case 1n+p:
      %h : {0n == add_bool(and(_, same(bucket, 1n+p)), count_selected(ord_shards(p, excl_exc, excl_named, excl_ser), Test{exc, dealt, ser, bucket})) : Nat}
      count_ord_inactive(p, excl_exc, excl_named, excl_ser, exc, dealt, ser, bucket, h)

def count_ord_above(+n: Nat, +excl_exc: Bool, +excl_named: Bool, +excl_ser: Bool, +exc: Bool, +dealt: Dealt, +ser: Bool, +bucket: Nat, +h_ok: {True{} == ord_class_ok(excl_exc, excl_named, excl_ser, exc, dealt, ser) : Bool}, +h_gt: {le(1n+n, bucket) == True{} : Bool}) -> {0n == count_selected(ord_shards(n, excl_exc, excl_named, excl_ser), Test{exc, dealt, ser, bucket}) : Nat}:
  match n:
    case 0n:
      {==}
    case 1n+p:
      %h_ok : {0n == add_bool(and(_, same(bucket, 1n+p)), count_selected(ord_shards(p, excl_exc, excl_named, excl_ser), Test{exc, dealt, ser, bucket})) : Nat}
      %gt_same_false(bucket, 1n+p, h_gt) : {0n == add_bool(_, count_selected(ord_shards(p, excl_exc, excl_named, excl_ser), Test{exc, dealt, ser, bucket})) : Nat}
      count_ord_above(p, excl_exc, excl_named, excl_ser, exc, dealt, ser, bucket, h_ok, le_pred_l(1n+p, bucket, h_gt))

def ord_active_step(+p: Nat, +excl_exc: Bool, +excl_named: Bool, +excl_ser: Bool, +exc: Bool, +dealt: Dealt, +ser: Bool, +bucket: Nat, +hit: Bool, +h_hit: {same(bucket, 1n+p) == hit : Bool}, +h_ok: {True{} == ord_class_ok(excl_exc, excl_named, excl_ser, exc, dealt, ser) : Bool}, +h_in: {in_range(bucket, 1n+p) == True{} : Bool}, ih: {in_range(bucket, p) == True{} : Bool} -> {1n == count_selected(ord_shards(p, excl_exc, excl_named, excl_ser), Test{exc, dealt, ser, bucket}) : Nat}) -> {1n == add_bool(hit, count_selected(ord_shards(p, excl_exc, excl_named, excl_ser), Test{exc, dealt, ser, bucket})) : Nat}:
  match hit:
    case True{}:
      %same_eq(bucket, 1n+p, h_hit) : {1n == 1n+count_selected(ord_shards(p, excl_exc, excl_named, excl_ser), Test{exc, dealt, ser, _}) : Nat}
      %count_ord_above(p, excl_exc, excl_named, excl_ser, exc, dealt, ser, 1n+p, h_ok, le_refl(1n+p)) : {1n == 1n+_ : Nat}
      {==}
    case False{}:
      ih(in_range_pred(bucket, p, h_in, h_hit))

def count_ord_active(+n: Nat, +excl_exc: Bool, +excl_named: Bool, +excl_ser: Bool, +exc: Bool, +dealt: Dealt, +ser: Bool, +bucket: Nat, +h_ok: {True{} == ord_class_ok(excl_exc, excl_named, excl_ser, exc, dealt, ser) : Bool}, +h_in: {in_range(bucket, n) == True{} : Bool}) -> {1n == count_selected(ord_shards(n, excl_exc, excl_named, excl_ser), Test{exc, dealt, ser, bucket}) : Nat}:
  match n:
    case 0n:
      Empty.absurd({1n == 0n : Nat}, in_range_zero(bucket, h_in))
    case 1n+p:
      %h_ok : {1n == add_bool(and(_, same(bucket, 1n+p)), count_selected(ord_shards(p, excl_exc, excl_named, excl_ser), Test{exc, dealt, ser, bucket})) : Nat}
      ord_active_step(p, excl_exc, excl_named, excl_ser, exc, dealt, ser, bucket, same(bucket, 1n+p), {==}, h_ok, h_in, h_p => count_ord_active(p, excl_exc, excl_named, excl_ser, exc, dealt, ser, bucket, h_ok, h_p))

def count_eh_inactive(+n: Nat, +excl_ser: Bool, +exc: Bool, +dealt: Dealt, +ser: Bool, +bucket: Nat, +h: {False{} == and(passes_serial(excl_ser, ser), exc) : Bool}) -> {0n == count_selected(exc_hash_shards(n, excl_ser), Test{exc, dealt, ser, bucket}) : Nat}:
  match n:
    case 0n:
      {==}
    case 1n+p:
      %h : {0n == add_bool(and(_, same(bucket, 1n+p)), count_selected(exc_hash_shards(p, excl_ser), Test{exc, dealt, ser, bucket})) : Nat}
      count_eh_inactive(p, excl_ser, exc, dealt, ser, bucket, h)

def count_eh_above(+n: Nat, +excl_ser: Bool, +exc: Bool, +dealt: Dealt, +ser: Bool, +bucket: Nat, +h_ok: {True{} == and(passes_serial(excl_ser, ser), exc) : Bool}, +h_gt: {le(1n+n, bucket) == True{} : Bool}) -> {0n == count_selected(exc_hash_shards(n, excl_ser), Test{exc, dealt, ser, bucket}) : Nat}:
  match n:
    case 0n:
      {==}
    case 1n+p:
      %h_ok : {0n == add_bool(and(_, same(bucket, 1n+p)), count_selected(exc_hash_shards(p, excl_ser), Test{exc, dealt, ser, bucket})) : Nat}
      %gt_same_false(bucket, 1n+p, h_gt) : {0n == add_bool(_, count_selected(exc_hash_shards(p, excl_ser), Test{exc, dealt, ser, bucket})) : Nat}
      count_eh_above(p, excl_ser, exc, dealt, ser, bucket, h_ok, le_pred_l(1n+p, bucket, h_gt))

def eh_active_step(+p: Nat, +excl_ser: Bool, +exc: Bool, +dealt: Dealt, +ser: Bool, +bucket: Nat, +hit: Bool, +h_hit: {same(bucket, 1n+p) == hit : Bool}, +h_ok: {True{} == and(passes_serial(excl_ser, ser), exc) : Bool}, +h_in: {in_range(bucket, 1n+p) == True{} : Bool}, ih: {in_range(bucket, p) == True{} : Bool} -> {1n == count_selected(exc_hash_shards(p, excl_ser), Test{exc, dealt, ser, bucket}) : Nat}) -> {1n == add_bool(hit, count_selected(exc_hash_shards(p, excl_ser), Test{exc, dealt, ser, bucket})) : Nat}:
  match hit:
    case True{}:
      %same_eq(bucket, 1n+p, h_hit) : {1n == 1n+count_selected(exc_hash_shards(p, excl_ser), Test{exc, dealt, ser, _}) : Nat}
      %count_eh_above(p, excl_ser, exc, dealt, ser, 1n+p, h_ok, le_refl(1n+p)) : {1n == 1n+_ : Nat}
      {==}
    case False{}:
      ih(in_range_pred(bucket, p, h_in, h_hit))

def count_eh_active(+n: Nat, +excl_ser: Bool, +exc: Bool, +dealt: Dealt, +ser: Bool, +bucket: Nat, +h_ok: {True{} == and(passes_serial(excl_ser, ser), exc) : Bool}, +h_in: {in_range(bucket, n) == True{} : Bool}) -> {1n == count_selected(exc_hash_shards(n, excl_ser), Test{exc, dealt, ser, bucket}) : Nat}:
  match n:
    case 0n:
      Empty.absurd({1n == 0n : Nat}, in_range_zero(bucket, h_in))
    case 1n+p:
      %h_ok : {1n == add_bool(and(_, same(bucket, 1n+p)), count_selected(exc_hash_shards(p, excl_ser), Test{exc, dealt, ser, bucket})) : Nat}
      eh_active_step(p, excl_ser, exc, dealt, ser, bucket, same(bucket, 1n+p), {==}, h_ok, h_in, h_p => count_eh_active(p, excl_ser, exc, dealt, ser, bucket, h_ok, h_p))

def count_et_rest_ser_false(+n: Nat, +excl_ser: Bool, +exc: Bool, +dealt: Dealt, +ser: Bool, +bucket: Nat, +h_ser: {False{} == passes_serial(excl_ser, ser) : Bool}) -> {0n == count_selected(exc_timed_rest(n, excl_ser), Test{exc, dealt, ser, bucket}) : Nat}:
  match n:
    case 0n:
      {==}
    case 1n+p:
      %h_ser : {0n == add_bool(and(_, or(dealt_to(dealt, 1n+p), False{})), count_selected(exc_timed_rest(p, excl_ser), Test{exc, dealt, ser, bucket})) : Nat}
      count_et_rest_ser_false(p, excl_ser, exc, dealt, ser, bucket, h_ser)

def count_et_ser_false(+n: Nat, +has_exc: Bool, +excl_ser: Bool, +exc: Bool, +dealt: Dealt, +ser: Bool, +bucket: Nat, +h_ser: {False{} == passes_serial(excl_ser, ser) : Bool}) -> {0n == count_selected(exc_timed_shards(n, has_exc, excl_ser), Test{exc, dealt, ser, bucket}) : Nat}:
  match n:
    case 0n:
      {==}
    case 1n+p:
      %h_ser : {0n == add_bool(and(_, or(dealt_to(dealt, 1n+p), and(has_exc, and(exc, is_not_named(dealt))))), count_selected(exc_timed_rest(p, excl_ser), Test{exc, dealt, ser, bucket})) : Nat}
      count_et_rest_ser_false(p, excl_ser, exc, dealt, ser, bucket, h_ser)

def count_et_rest_not_named(+n: Nat, +excl_ser: Bool, +exc: Bool, +ser: Bool, +bucket: Nat) -> {0n == count_selected(exc_timed_rest(n, excl_ser), Test{exc, NotNamed{}, ser, bucket}) : Nat}:
  match n:
    case 0n:
      {==}
    case 1n+p:
      %and_false_r(passes_serial(excl_ser, ser)) : {0n == add_bool(_, count_selected(exc_timed_rest(p, excl_ser), Test{exc, NotNamed{}, ser, bucket})) : Nat}
      count_et_rest_not_named(p, excl_ser, exc, ser, bucket)

def count_et_rest_above(+n: Nat, +excl_ser: Bool, +exc: Bool, +k: Nat, +ser: Bool, +bucket: Nat, +h_ser: {True{} == passes_serial(excl_ser, ser) : Bool}, +h_gt: {le(1n+n, k) == True{} : Bool}) -> {0n == count_selected(exc_timed_rest(n, excl_ser), Test{exc, ToShard{k}, ser, bucket}) : Nat}:
  match n:
    case 0n:
      {==}
    case 1n+p:
      %h_ser : {0n == add_bool(and(_, or(same(k, 1n+p), False{})), count_selected(exc_timed_rest(p, excl_ser), Test{exc, ToShard{k}, ser, bucket})) : Nat}
      %gt_same_false(k, 1n+p, h_gt) : {0n == add_bool(or(_, False{}), count_selected(exc_timed_rest(p, excl_ser), Test{exc, ToShard{k}, ser, bucket})) : Nat}
      count_et_rest_above(p, excl_ser, exc, k, ser, bucket, h_ser, le_pred_l(1n+p, k, h_gt))

def et_rest_step(+p: Nat, +excl_ser: Bool, +exc: Bool, +k: Nat, +ser: Bool, +bucket: Nat, +hit: Bool, +h_hit: {same(k, 1n+p) == hit : Bool}, +h_ser: {True{} == passes_serial(excl_ser, ser) : Bool}, +h_in: {in_range(k, 1n+p) == True{} : Bool}, ih: {in_range(k, p) == True{} : Bool} -> {1n == count_selected(exc_timed_rest(p, excl_ser), Test{exc, ToShard{k}, ser, bucket}) : Nat}) -> {1n == add_bool(or(hit, False{}), count_selected(exc_timed_rest(p, excl_ser), Test{exc, ToShard{k}, ser, bucket})) : Nat}:
  match hit:
    case True{}:
      %same_eq(k, 1n+p, h_hit) : {1n == 1n+count_selected(exc_timed_rest(p, excl_ser), Test{exc, ToShard{_}, ser, bucket}) : Nat}
      %count_et_rest_above(p, excl_ser, exc, 1n+p, ser, bucket, h_ser, le_refl(1n+p)) : {1n == 1n+_ : Nat}
      {==}
    case False{}:
      ih(in_range_pred(k, p, h_in, h_hit))

def count_et_rest_dealt(+n: Nat, +excl_ser: Bool, +exc: Bool, +k: Nat, +ser: Bool, +bucket: Nat, +h_ser: {True{} == passes_serial(excl_ser, ser) : Bool}, +h_in: {in_range(k, n) == True{} : Bool}) -> {1n == count_selected(exc_timed_rest(n, excl_ser), Test{exc, ToShard{k}, ser, bucket}) : Nat}:
  match n:
    case 0n:
      Empty.absurd({1n == 0n : Nat}, in_range_zero(k, h_in))
    case 1n+p:
      %h_ser : {1n == add_bool(and(_, or(same(k, 1n+p), False{})), count_selected(exc_timed_rest(p, excl_ser), Test{exc, ToShard{k}, ser, bucket})) : Nat}
      et_rest_step(p, excl_ser, exc, k, ser, bucket, same(k, 1n+p), {==}, h_ser, h_in, h_p => count_et_rest_dealt(p, excl_ser, exc, k, ser, bucket, h_ser, h_p))

def count_et_dealt(+n: Nat, +has_exc: Bool, +excl_ser: Bool, +exc: Bool, +k: Nat, +ser: Bool, +bucket: Nat, +h_ser: {True{} == passes_serial(excl_ser, ser) : Bool}, +h_in: {in_range(k, n) == True{} : Bool}) -> {1n == count_selected(exc_timed_shards(n, has_exc, excl_ser), Test{exc, ToShard{k}, ser, bucket}) : Nat}:
  match n:
    case 0n:
      Empty.absurd({1n == 0n : Nat}, in_range_zero(k, h_in))
    case 1n+p:
      %h_ser : {1n == add_bool(and(_, or(same(k, 1n+p), and(has_exc, and(exc, False{})))), count_selected(exc_timed_rest(p, excl_ser), Test{exc, ToShard{k}, ser, bucket})) : Nat}
      %and_false_r(exc) : {1n == add_bool(or(same(k, 1n+p), and(has_exc, _)), count_selected(exc_timed_rest(p, excl_ser), Test{exc, ToShard{k}, ser, bucket})) : Nat}
      %and_false_r(has_exc) : {1n == add_bool(or(same(k, 1n+p), _), count_selected(exc_timed_rest(p, excl_ser), Test{exc, ToShard{k}, ser, bucket})) : Nat}
      et_rest_step(p, excl_ser, exc, k, ser, bucket, same(k, 1n+p), {==}, h_ser, h_in, h_p => count_et_rest_dealt(p, excl_ser, exc, k, ser, bucket, h_ser, h_p))

# --- main theorem assembly -----------------------------------------------------------

def sharded_ser_blocked(+m: Nat, +has_exc: Bool, +has_timed: Bool, +exc: Bool, +dealt: Dealt, +bucket: Nat) -> {0n == count_selected(pkg_pieces(2n+m, has_exc, has_timed, True{}), Test{exc, dealt, True{}, bucket}) : Nat}:
  match has_exc has_timed:
    case True{} True{}:
      %count_cat(ord_shards(2n+m, True{}, True{}, True{}), exc_timed_shards(2n+m, True{}, True{}), Test{exc, dealt, True{}, bucket}) : {0n == _ : Nat}
      %count_ord_inactive(2n+m, True{}, True{}, True{}, exc, dealt, True{}, bucket, {==}) : {0n == add(_, count_selected(exc_timed_shards(2n+m, True{}, True{}), Test{exc, dealt, True{}, bucket})) : Nat}
      count_et_ser_false(2n+m, True{}, True{}, exc, dealt, True{}, bucket, {==})
    case False{} True{}:
      %count_cat(ord_shards(2n+m, False{}, True{}, True{}), exc_timed_shards(2n+m, False{}, True{}), Test{exc, dealt, True{}, bucket}) : {0n == _ : Nat}
      %count_ord_inactive(2n+m, False{}, True{}, True{}, exc, dealt, True{}, bucket, {==}) : {0n == add(_, count_selected(exc_timed_shards(2n+m, False{}, True{}), Test{exc, dealt, True{}, bucket})) : Nat}
      count_et_ser_false(2n+m, False{}, True{}, exc, dealt, True{}, bucket, {==})
    case True{} False{}:
      %count_cat(ord_shards(2n+m, True{}, False{}, True{}), exc_hash_shards(2n+m, True{}), Test{exc, dealt, True{}, bucket}) : {0n == _ : Nat}
      %count_ord_inactive(2n+m, True{}, False{}, True{}, exc, dealt, True{}, bucket, {==}) : {0n == add(_, count_selected(exc_hash_shards(2n+m, True{}), Test{exc, dealt, True{}, bucket})) : Nat}
      count_eh_inactive(2n+m, True{}, exc, dealt, True{}, bucket, {==})
    case False{} False{}:
      %count_cat(ord_shards(2n+m, False{}, False{}, True{}), [], Test{exc, dealt, True{}, bucket}) : {0n == _ : Nat}
      %count_ord_inactive(2n+m, False{}, False{}, True{}, exc, dealt, True{}, bucket, {==}) : {0n == add(_, 0n) : Nat}
      {==}

def sharded_non_serial(+m: Nat, +has_exc: Bool, +has_timed: Bool, +excl_ser: Bool, +exc: Bool, +dealt: Dealt, +ser: Bool, +bucket: Nat, +h_ser: {True{} == passes_serial(excl_ser, ser) : Bool}, +h_b: {in_range(bucket, 2n+m) == True{} : Bool}, +h_d: {dealt_in_range(dealt, 2n+m) == True{} : Bool}) -> {count_selected(pkg_pieces(2n+m, has_exc, has_timed, excl_ser), Test{exc, dealt, ser, bucket}) == 1n : Nat}:
  match has_exc has_timed exc dealt:
    case False{} False{} exc0 dealt0:
      %count_cat(ord_shards(2n+m, False{}, False{}, excl_ser), [], Test{exc0, dealt0, ser, bucket}) : {_ == 1n : Nat}
      %count_ord_active(2n+m, False{}, False{}, excl_ser, exc0, dealt0, ser, bucket, Equal.trans(Bool, True{}, passes_serial(excl_ser, ser), and(passes_serial(excl_ser, ser), True{}), h_ser, and_true_r(passes_serial(excl_ser, ser))), h_b) : {add(_, 0n) == 1n : Nat}
      {==}
    case True{} False{} False{} dealt0:
      %count_cat(ord_shards(2n+m, True{}, False{}, excl_ser), exc_hash_shards(2n+m, excl_ser), Test{False{}, dealt0, ser, bucket}) : {_ == 1n : Nat}
      %count_ord_active(2n+m, True{}, False{}, excl_ser, False{}, dealt0, ser, bucket, Equal.trans(Bool, True{}, passes_serial(excl_ser, ser), and(passes_serial(excl_ser, ser), True{}), h_ser, and_true_r(passes_serial(excl_ser, ser))), h_b) : {add(_, count_selected(exc_hash_shards(2n+m, excl_ser), Test{False{}, dealt0, ser, bucket})) == 1n : Nat}
      %count_eh_inactive(2n+m, excl_ser, False{}, dealt0, ser, bucket, and_false_r(passes_serial(excl_ser, ser))) : {add(1n, _) == 1n : Nat}
      {==}
    case True{} False{} True{} dealt0:
      %count_cat(ord_shards(2n+m, True{}, False{}, excl_ser), exc_hash_shards(2n+m, excl_ser), Test{True{}, dealt0, ser, bucket}) : {_ == 1n : Nat}
      %count_ord_inactive(2n+m, True{}, False{}, excl_ser, True{}, dealt0, ser, bucket, and_false_r(passes_serial(excl_ser, ser))) : {add(_, count_selected(exc_hash_shards(2n+m, excl_ser), Test{True{}, dealt0, ser, bucket})) == 1n : Nat}
      %count_eh_active(2n+m, excl_ser, True{}, dealt0, ser, bucket, Equal.trans(Bool, True{}, passes_serial(excl_ser, ser), and(passes_serial(excl_ser, ser), True{}), h_ser, and_true_r(passes_serial(excl_ser, ser))), h_b) : {_ == 1n : Nat}
      {==}
    case False{} True{} exc0 NotNamed{}:
      %count_cat(ord_shards(2n+m, False{}, True{}, excl_ser), exc_timed_shards(2n+m, False{}, excl_ser), Test{exc0, NotNamed{}, ser, bucket}) : {_ == 1n : Nat}
      %count_ord_active(2n+m, False{}, True{}, excl_ser, exc0, NotNamed{}, ser, bucket, Equal.trans(Bool, True{}, passes_serial(excl_ser, ser), and(passes_serial(excl_ser, ser), True{}), h_ser, and_true_r(passes_serial(excl_ser, ser))), h_b) : {add(_, count_selected(exc_timed_shards(2n+m, False{}, excl_ser), Test{exc0, NotNamed{}, ser, bucket})) == 1n : Nat}
      %h_ser : {add(1n, add_bool(and(_, False{}), count_selected(exc_timed_rest(1n+m, excl_ser), Test{exc0, NotNamed{}, ser, bucket}))) == 1n : Nat}
      %count_et_rest_not_named(1n+m, excl_ser, exc0, ser, bucket) : {add(1n, _) == 1n : Nat}
      {==}
    case False{} True{} exc0 ToShard{k}:
      %count_cat(ord_shards(2n+m, False{}, True{}, excl_ser), exc_timed_shards(2n+m, False{}, excl_ser), Test{exc0, ToShard{k}, ser, bucket}) : {_ == 1n : Nat}
      %count_ord_inactive(2n+m, False{}, True{}, excl_ser, exc0, ToShard{k}, ser, bucket, and_false_r(passes_serial(excl_ser, ser))) : {add(_, count_selected(exc_timed_shards(2n+m, False{}, excl_ser), Test{exc0, ToShard{k}, ser, bucket})) == 1n : Nat}
      %count_et_dealt(2n+m, False{}, excl_ser, exc0, k, ser, bucket, h_ser, h_d) : {_ == 1n : Nat}
      {==}
    case True{} True{} False{} NotNamed{}:
      %count_cat(ord_shards(2n+m, True{}, True{}, excl_ser), exc_timed_shards(2n+m, True{}, excl_ser), Test{False{}, NotNamed{}, ser, bucket}) : {_ == 1n : Nat}
      %count_ord_active(2n+m, True{}, True{}, excl_ser, False{}, NotNamed{}, ser, bucket, Equal.trans(Bool, True{}, passes_serial(excl_ser, ser), and(passes_serial(excl_ser, ser), True{}), h_ser, and_true_r(passes_serial(excl_ser, ser))), h_b) : {add(_, count_selected(exc_timed_shards(2n+m, True{}, excl_ser), Test{False{}, NotNamed{}, ser, bucket})) == 1n : Nat}
      %h_ser : {add(1n, add_bool(and(_, False{}), count_selected(exc_timed_rest(1n+m, excl_ser), Test{False{}, NotNamed{}, ser, bucket}))) == 1n : Nat}
      %count_et_rest_not_named(1n+m, excl_ser, False{}, ser, bucket) : {add(1n, _) == 1n : Nat}
      {==}
    case True{} True{} False{} ToShard{k}:
      %count_cat(ord_shards(2n+m, True{}, True{}, excl_ser), exc_timed_shards(2n+m, True{}, excl_ser), Test{False{}, ToShard{k}, ser, bucket}) : {_ == 1n : Nat}
      %count_ord_inactive(2n+m, True{}, True{}, excl_ser, False{}, ToShard{k}, ser, bucket, and_false_r(passes_serial(excl_ser, ser))) : {add(_, count_selected(exc_timed_shards(2n+m, True{}, excl_ser), Test{False{}, ToShard{k}, ser, bucket})) == 1n : Nat}
      %count_et_dealt(2n+m, True{}, excl_ser, False{}, k, ser, bucket, h_ser, h_d) : {_ == 1n : Nat}
      {==}
    case True{} True{} True{} NotNamed{}:
      %count_cat(ord_shards(2n+m, True{}, True{}, excl_ser), exc_timed_shards(2n+m, True{}, excl_ser), Test{True{}, NotNamed{}, ser, bucket}) : {_ == 1n : Nat}
      %count_ord_inactive(2n+m, True{}, True{}, excl_ser, True{}, NotNamed{}, ser, bucket, and_false_r(passes_serial(excl_ser, ser))) : {add(_, count_selected(exc_timed_shards(2n+m, True{}, excl_ser), Test{True{}, NotNamed{}, ser, bucket})) == 1n : Nat}
      %h_ser : {add_bool(and(_, True{}), count_selected(exc_timed_rest(1n+m, excl_ser), Test{True{}, NotNamed{}, ser, bucket})) == 1n : Nat}
      %count_et_rest_not_named(1n+m, excl_ser, True{}, ser, bucket) : {1n+_ == 1n : Nat}
      {==}
    case True{} True{} True{} ToShard{k}:
      %count_cat(ord_shards(2n+m, True{}, True{}, excl_ser), exc_timed_shards(2n+m, True{}, excl_ser), Test{True{}, ToShard{k}, ser, bucket}) : {_ == 1n : Nat}
      %count_ord_inactive(2n+m, True{}, True{}, excl_ser, True{}, ToShard{k}, ser, bucket, and_false_r(passes_serial(excl_ser, ser))) : {add(_, count_selected(exc_timed_shards(2n+m, True{}, excl_ser), Test{True{}, ToShard{k}, ser, bucket})) == 1n : Nat}
      %count_et_dealt(2n+m, True{}, excl_ser, True{}, k, ser, bucket, h_ser, h_d) : {_ == 1n : Nat}
      {==}

# --- laws ----------------------------------------------------------------------------

# Every valid test of the package is selected by exactly one piece of the plan: ordinary shard,
# exceptions shard, or serial group runner.
law pieces_cover_once:
  for +shards: Nat
  for +has_exc: Bool
  for +has_timed: Bool
  for +serial_active: Bool
  for +t: Test
  for +h: {valid_test(t, shards) == True{} : Bool}
  {count_selected(plan(shards, has_exc, has_timed, serial_active), t) == 1n : Nat}

def pieces_cover_once(shards, has_exc, has_timed, serial_active, t, h):
  match shards serial_active t:
    case 0n sa Test{+exc, +dealt, +ser, +bucket}:
      Empty.absurd({count_selected(plan(0n, has_exc, has_timed, sa), Test{exc, dealt, ser, bucket}) == 1n : Nat}, in_range_zero(bucket, and_l(in_range(bucket, 0n), dealt_in_range(dealt, 0n), h)))
    case 1n True{} Test{exc, dealt, True{}, bucket}:
      {==}
    case 1n True{} Test{exc, dealt, False{}, bucket}:
      {==}
    case 1n False{} Test{exc, dealt, ser, bucket}:
      {==}
    case 2n+m True{} Test{+exc, +dealt, True{}, +bucket}:
      %sharded_ser_blocked(m, has_exc, has_timed, exc, dealt, bucket) : {1n+_ == 1n : Nat}
      {==}
    case 2n+m True{} Test{+exc, +dealt, False{}, +bucket}:
      sharded_non_serial(
        m,
        has_exc,
        has_timed,
        True{},
        exc,
        dealt,
        False{},
        bucket,
        {==},
        and_l(in_range(bucket, 2n+m), dealt_in_range(dealt, 2n+m), h),
        and_r(in_range(bucket, 2n+m), dealt_in_range(dealt, 2n+m), h),
      )
    case 2n+m False{} Test{+exc, +dealt, +ser, +bucket}:
      sharded_non_serial(
        m,
        has_exc,
        has_timed,
        False{},
        exc,
        dealt,
        ser,
        bucket,
        {==},
        and_l(in_range(bucket, 2n+m), dealt_in_range(dealt, 2n+m), h),
        and_r(in_range(bucket, 2n+m), dealt_in_range(dealt, 2n+m), h),
      )

# Unsharded crates (shards == 1) cover every test once without depending on hash buckets or timings.
law unsharded_covers_once:
  for +has_exc: Bool
  for +has_timed: Bool
  for +serial_active: Bool
  for +t: Test
  {count_selected(plan(1n, has_exc, has_timed, serial_active), t) == 1n : Nat}

def unsharded_covers_once(has_exc, has_timed, serial_active, t):
  match serial_active t:
    case True{} Test{exc, dealt, True{}, bucket}:
      {==}
    case True{} Test{exc, dealt, False{}, bucket}:
      {==}
    case False{} Test{exc, dealt, ser, bucket}:
      {==}

# Non-macOS hosts (serial_active == False) emit no serial runner and exclude no serial test from
# per-crate pieces, so even tests matching a serial filter are covered once.
law non_macos_covers_once:
  for +shards: Nat
  for +has_exc: Bool
  for +has_timed: Bool
  for +t: Test
  for +h: {valid_test(t, shards) == True{} : Bool}
  {count_selected(plan(shards, has_exc, has_timed, False{}), t) == 1n : Nat}

def non_macos_covers_once(shards, has_exc, has_timed, t, h):
  pieces_cover_once(shards, has_exc, has_timed, False{}, t, h)

# --- definedness witnesses: broken plans fail exact cover ----------------------------

# Omitting `take_unnamed_exc` on the last timed exception shard drops an unnamed exceptional test
# regardless of whether serial groups are active.
law dropping_unnamed_exc_loses_test:
  for +serial_active: Bool
  {count_selected(
    cat_p(ord_shards(2n, True{}, True{}, serial_active), exc_timed_rest(2n, serial_active)),
    Test{True{}, NotNamed{}, False{}, 1n},
  ) == 0n : Nat}

def dropping_unnamed_exc_loses_test(serial_active):
  match serial_active:
    case True{}:
      {==}
    case False{}:
      {==}

# Omitting serial exclusion from ordinary pieces while a serial runner is active runs serial tests
# twice, whether or not they are exceptional.
law missing_serial_exclusion_double_runs:
  for +exc: Bool
  {count_selected(
    SerialRunner{} <> ord_shards(2n, False{}, False{}, False{}),
    Test{exc, NotNamed{}, True{}, 1n},
  ) == 2n : Nat}

def missing_serial_exclusion_double_runs(exc):
  match exc:
    case True{}:
      {==}
    case False{}:
      {==}
