# The laws the deployed mbx stores do not satisfy (specs/cowshed/03_caches.md "mbx"). Each is an
# open claim with no proof, so this file's verdict is red, and each is refuted by a trace the
# checker evaluates in mbx_store.bend. mbx_store.bend proves the same law under the setting that
# holds it. Closing one means the code reaches that setting; then the claim gets its proof.
#
# Check: bun bend2/main.ts mbx_store_open.bend --verdict   (SOME PROOFS FAIL: 4 TODOs)
import Base
import ./mbx_store.bend as S


# Neither the pin (8cd8f6b) nor a build without record_use holds a producer's blob against the sweep.
# Refuted by S.in_flight_lost_today on the trace S.t_swept.
law in_flight_kept_today:
  for +tch: Bool
  for +os: +List<S.Op>
  {S.inflight_b(S.run(os, S.Pol{tch, False{}, False{}}, S.empty())) == True{} : Bool}

# Neither the pin (8cd8f6b) nor a build without record_use drops a result before it takes its blob.
# Refuted by S.window_today on the trace S.t_window.
law no_dangling_today:
  for +tch: Bool
  for +os: +List<S.Op>
  {S.dang_of(S.run(os, S.Pol{tch, False{}, False{}}, S.empty())) == 0n : Nat}

# A producer that stored the blobs a result names, and then published it, is refused when a sweep
# took one of them in between: the compile runs twice.
# Refuted by S.publish_refused_today on the trace S.t_inflight.
law publish_kept_today:
  for +tch: Bool
  for +os: +List<S.Op>
  for +i: Nat
  for +names: +List<Nat>
  for g: {S.all_in(names, S.fl_of(S.run(os, S.Pol{tch, False{}, False{}}, S.empty()))) == True{} : Bool}
  {S.redo_of(S.step(S.Pol{tch, False{}, False{}}, S.Publish{i, names}, S.run(os, S.Pol{tch, False{}, False{}}, S.empty()))) == S.redo_of(S.run(os, S.Pol{tch, False{}, False{}}, S.empty())) : Nat}