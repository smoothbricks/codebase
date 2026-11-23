# The close law the loop refuted before its drain (specs/cowshed/05_gateway.md "Deadlines",
# smoo/cowshed!n/controller-connection-own-thread), over the model in connection_close.bend with
# fixed=False: the reader arm returns Ok the moment the peer closes. It has no proof: it is a red.
# connection_close.bend proves its twin for the repair (close_reports_every_ended_answer) and
# computes the counterexample, the_old_loop_loses_an_ended_fatal:
#   queue [Fatal], [Poll{Read}]: the connection ends Ok with a fatal answer ended and unread.
# Check: bend2 specs/cowshed/laws/connection_close_open.bend --verdict (SOME PROOFS FAIL is the
# expected verdict)
import Base
import ./connection_close.bend as C

law close_reports_every_ended_answer_before_the_drain:
  for +steps: +List<C.Step>
  for +q: +List<C.Out>
  {C.honest(C.run(False{}, steps, q, C.any_fatal(q))) == True{} : Bool}
