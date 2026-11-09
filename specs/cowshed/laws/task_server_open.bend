# The task-server laws the code refutes (specs/cowshed/05_gateway.md "The task server schedules
# work units", "Classes"), over the model in task_server.bend with the code as it is. They have no
# proof: each is a red, and the emitted driver computes each counterexample:
#   lands_behind_older_aged       a land is ordered ahead of an aged unit that was admitted before it
#     The driver's first draw has 20 inputs; dropping inputs while the law stays refuted leaves
#       os = [OAdmit(2,CGate,true,2,DAt(0),Ls(false,true,false)),OTick(2),
#             OAdmit(0,CInter,true,2,DNone,Ls(true,true,true))], b = 0
#     The ready order is [0,2], the land (0) before the aged unit (2) that waited longer; 05:2060-2061
#     and 2063 say [2,0]. With a fresh land, by hand:
#       os = [OAdmit(0,CBack,false,0,DNone,Ls(false,false,false)),OTick(1),
#             OAdmit(1,CGate,false,0,DNone,Ls(false,false,true))], b = 1
#     The ready order is [1,0] and the spec's is [0,1].
#   omitted_class_is_background   an omitted class is admitted as Gate
#     w = WOmit: the code's class is CGate, 05:1416 says CBack.
# Driver: concatenate task_server.bend with these laws (the emitter reads one book, so drop the
# import and the `T.` prefix), emit with --len 3 --cap 400 inside cowshed under a memory ceiling.
import Base
import ./task_server.bend as T

# Open, refuted by the code: no land is ordered ahead of an aged unit older than it. `order_cmp`
# returns on a land before it reads age, so a land that waited no time goes ahead of a unit that
# waited the age bound, and an aged land ahead of an aged unit that waited longer.
law lands_behind_older_aged:
  for +os: +List<T.Op>
  for +b: Nat
  {T.lands_behind_b(T.run(os, T.empty(b))) == True{} : Bool}

# Open, refuted by the code: a request that omits its class is admitted as background
# (05:1416: ordinary commands default to background). TaskAdmitIn's `#[serde(default)]` takes
# TaskClass::default(), which is Gate. task_server.bend proves the repaired default,
# repaired_default_is_background.
law omitted_class_is_background:
  for +w: T.Wire
  {T.wire_class(T.CGate{}, w) == T.spec_class(w) : T.Cls}