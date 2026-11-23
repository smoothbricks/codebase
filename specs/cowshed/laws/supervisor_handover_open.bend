# The handover law the embedder's relay refuted before its release (specs/cowshed/11_shell.md
# "Draining a supervisor of another build", smoo/cowshed!n/embedder-awaits-other-build), over the
# model in supervisor_handover.bend with fixed=False: a worker's relay holds the replaced child
# for as long as its worker lives. It has no proof: it is a red. supervisor_handover.bend proves
# its twin for the repair (handover_is_never_stuck) and computes the counterexample,
# todays_cycle_is_stuck, measured 2026-11-23:
#   replaced, serving, one resident, the child alive, one relay, nothing else:
#     the resident waits for its requester, the child, to exit; the child is held by the relay's
#     handle on that resident. No step can happen and the supervisor has not retired.
# Check: bend2 specs/cowshed/laws/supervisor_handover_open.bend --verdict (SOME PROOFS FAIL is the
# expected verdict)
import Base
import ./supervisor_handover.bend as H

law handover_is_never_stuck_without_the_release:
  for +rl: Nat
  for +j: Nat
  for +c: Bool
  for +rs: Nat
  for +ot: Nat
  for +ow: Nat
  for +sv: Bool
  {Bool.or(Bool.not(H.idle(False{}, H.St{True{}, sv, ow, ot, rs, c, j, rl})), H.retired(H.St{True{}, sv, ow, ot, rs, c, j, rl})) == True{} : Bool}