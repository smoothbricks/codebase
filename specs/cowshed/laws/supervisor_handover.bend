# The handover of a workspace from a supervisor of another build to the embedder's next
# controller (specs/cowshed/11_shell.md "Draining a supervisor of another build",
# smoo/cowshed!n/embedder-awaits-other-build). One draining supervisor and the one controller
# child an embedder reached it through. The embedder's side is its own contract: a worker's relay
# holds a child only while the embedder's keeper sends new work there.
#
# replaced   the embedder's keeper has opened a fresh child and sends new work there
# serving    the supervisor has not retired; the embedder's ensure waits on its exit watch, so
#            the wait answers exactly when this turns false
# owed       owed jobs whose end nobody has read yet, running ones included
# others     finite jobs of other requesters, still running
# residents  residents whose requester is the child
# child      the replaced child is alive
# jobs       finite jobs the child admitted, still running: each holds the child
# relays     worker relays that still hold the child
#
# A step happens only as the specs allow it. A relay ends once the keeper has replaced the child
# (the release; without it, `fixed` False, no relay ever ends while its worker lives). A finite job
# ends on its own. The child exits once no relay and no job of its own holds it. The supervisor
# ends a resident once its requester, the child, has exited. An owed end is read, or its readers
# have all exited, or its 30-minute bound passes, once nothing runs. The supervisor retires once
# nothing runs and nothing is owed.
#
# What the laws assume:
# a. A finite job ends on its own; the model lets it end at any step and never forbids it.
# b. A resident whose requester is some other live process is outside the model: the supervisor
#    ends it at that requester's exit, which no handover causes.
# c. The hour's ceiling of the embedder's wait is time, not order: the laws prove the drain ends,
#    not that it ends inside the hour.
# d. One replaced child. Several in a row are this state once per child.
#
# handover_is_never_stuck and the seven `*_shrinks` laws together are the claim: with the child
# replaced, a state where no step can happen has retired, and every step that can happen lowers
# `left` by one, so every order of steps ends retired within `left` steps.
#
# Check: bend2 specs/cowshed/laws/supervisor_handover.bend --verdict
import Base

type St is Data:
  St{replaced: Bool, serving: Bool, owed: Nat, others: Nat, residents: Nat, child: Bool, jobs: Nat, relays: Nat}

type Op is Data:
  RelayEnds{}
  JobEnds{}
  ChildExits{}
  ResidentEnded{}
  OtherEnds{}
  EndRead{}
  Retires{}

def zero(n: Nat) -> Bool:
  match n:
    case 0n:
      True{}
    case 1n+p:
      False{}

def pred(n: Nat) -> Nat:
  match n:
    case 0n:
      0n
    case 1n+p:
      p

def add(a: Nat, b: Nat) -> Nat:
  match a:
    case 0n:
      b
    case 1n+p:
      1n+add(p, b)

def bit(b: Bool) -> Nat:
  match b:
    case True{}:
      1n
    case False{}:
      0n

# Nothing the supervisor serves is still running.
def quiet(+ot: Nat, +rs: Nat, +j: Nat) -> Bool:
  Bool.and(zero(ot), Bool.and(zero(rs), zero(j)))

def can(+fixed: Bool, +o: Op, +s: St) -> Bool:
  match o s:
    case RelayEnds{} St{rp, sv, ow, ot, rs, c, j, rl}:
      Bool.and(fixed, Bool.and(rp, Bool.not(zero(rl))))
    case JobEnds{} St{rp, sv, ow, ot, rs, c, j, rl}:
      Bool.not(zero(j))
    case ChildExits{} St{rp, sv, ow, ot, rs, c, j, rl}:
      Bool.and(c, Bool.and(zero(rl), zero(j)))
    case ResidentEnded{} St{rp, sv, ow, ot, rs, c, j, rl}:
      Bool.and(Bool.not(c), Bool.not(zero(rs)))
    case OtherEnds{} St{rp, sv, ow, ot, rs, c, j, rl}:
      Bool.not(zero(ot))
    case EndRead{} St{rp, sv, ow, ot, rs, c, j, rl}:
      Bool.and(Bool.not(zero(ow)), quiet(ot, rs, j))
    case Retires{} St{rp, sv, ow, ot, rs, c, j, rl}:
      Bool.and(sv, Bool.and(zero(ow), quiet(ot, rs, j)))

def step(+o: Op, +s: St) -> St:
  match o s:
    case RelayEnds{} St{rp, sv, ow, ot, rs, c, j, rl}:
      St{rp, sv, ow, ot, rs, c, j, pred(rl)}
    case JobEnds{} St{rp, sv, ow, ot, rs, c, j, rl}:
      St{rp, sv, ow, ot, rs, c, pred(j), rl}
    case ChildExits{} St{rp, sv, ow, ot, rs, c, j, rl}:
      St{rp, sv, ow, ot, rs, False{}, j, rl}
    case ResidentEnded{} St{rp, sv, ow, ot, rs, c, j, rl}:
      St{rp, sv, ow, ot, pred(rs), c, j, rl}
    case OtherEnds{} St{rp, sv, ow, ot, rs, c, j, rl}:
      St{rp, sv, ow, pred(ot), rs, c, j, rl}
    case EndRead{} St{rp, sv, ow, ot, rs, c, j, rl}:
      St{rp, sv, pred(ow), ot, rs, c, j, rl}
    case Retires{} St{rp, sv, ow, ot, rs, c, j, rl}:
      St{rp, False{}, ow, ot, rs, c, j, rl}

def idle(+fixed: Bool, +s: St) -> Bool:
  Bool.not(Bool.or(can(fixed, RelayEnds{}, s), Bool.or(can(fixed, JobEnds{}, s), Bool.or(can(fixed, ChildExits{}, s),
    Bool.or(can(fixed, ResidentEnded{}, s), Bool.or(can(fixed, OtherEnds{}, s), Bool.or(can(fixed, EndRead{}, s), can(fixed, Retires{}, s))))))))

def retired(+s: St) -> Bool:
  match s:
    case St{rp, sv, ow, ot, rs, c, j, rl}:
      Bool.not(sv)

def left(+s: St) -> Nat:
  match s:
    case St{rp, sv, ow, ot, rs, c, j, rl}:
      add(bit(sv), add(ow, add(ot, add(rs, add(bit(c), add(j, rl))))))

# Witness against a relay that holds a replaced child: today's cycle. No step can happen, and the
# supervisor serves on.
law todays_cycle_is_stuck:
  {(idle(False{}, St{True{}, True{}, 0n, 0n, 1n, True{}, 0n, 1n}), retired(St{True{}, True{}, 0n, 0n, 1n, True{}, 0n, 1n})) == (True{}, False{}) : Bool & Bool}

def todays_cycle_is_stuck():
  {==}

# In the same state, with the release, the relay can end.
law the_release_unsticks_it:
  {can(True{}, RelayEnds{}, St{True{}, True{}, 0n, 0n, 1n, True{}, 0n, 1n}) == True{} : Bool}

def the_release_unsticks_it():
  {==}

# With the child replaced, a state where no step can happen has retired.
law handover_is_never_stuck:
  for +rl: Nat
  for +j: Nat
  for +c: Bool
  for +rs: Nat
  for +ot: Nat
  for +ow: Nat
  for +sv: Bool
  {Bool.or(Bool.not(idle(True{}, St{True{}, sv, ow, ot, rs, c, j, rl})), retired(St{True{}, sv, ow, ot, rs, c, j, rl})) == True{} : Bool}

def handover_is_never_stuck(rl, j, c, rs, ot, ow, sv):
  match rl j c rs ot ow sv:
    case 1n+rl1 j0 c0 rs0 ot0 ow0 sv0:
      {==}
    case 0n 1n+j1 c0 rs0 ot0 ow0 sv0:
      {==}
    case 0n 0n True{} rs0 ot0 ow0 sv0:
      {==}
    case 0n 0n False{} 1n+rs1 ot0 ow0 sv0:
      {==}
    case 0n 0n False{} 0n 1n+ot1 ow0 sv0:
      {==}
    case 0n 0n False{} 0n 0n 1n+ow1 sv0:
      {==}
    case 0n 0n False{} 0n 0n 0n True{}:
      {==}
    case 0n 0n False{} 0n 0n 0n False{}:
      {==}

# `left` falls by one at each step that can happen. A step's state is written with what the step
# needs (a relay to end, the child alive and unheld), and its law shows both that the step can
# happen there and what it leaves.
def up(+a: Nat, +b: Nat) -> {add(a, 1n+b) == 1n+add(a, b) : Nat}:
  match a:
    case 0n:
      {==}
    case 1n+p:
      Equal.cong(Nat, Nat, k => 1n+k, add(p, 1n+b), 1n+add(p, b), up(p, b))

def lift(+a: Nat, +x: Nat, +y: Nat, e: {x == 1n+y : Nat}) -> {add(a, x) == 1n+add(a, y) : Nat}:
  Equal.trans(Nat, add(a, x), add(a, 1n+y), 1n+add(a, y),
    Equal.cong(Nat, Nat, k => add(a, k), x, 1n+y, e), up(a, y))

# A relay that ends is one thing fewer.
law a_relay_ending_shrinks:
  for +sv: Bool
  for +ow: Nat
  for +ot: Nat
  for +rs: Nat
  for +c: Bool
  for +j: Nat
  for +rl: Nat
  {(can(True{}, RelayEnds{}, St{True{}, sv, ow, ot, rs, c, j, 1n+rl}), left(St{True{}, sv, ow, ot, rs, c, j, 1n+rl})) == (True{}, 1n+left(step(RelayEnds{}, St{True{}, sv, ow, ot, rs, c, j, 1n+rl}))) : Bool & Nat}

def a_relay_ending_shrinks(sv, ow, ot, rs, c, j, rl):
  Equal.cong(Nat, Bool & Nat, k => (True{}, k), left(St{True{}, sv, ow, ot, rs, c, j, 1n+rl}), 1n+left(step(RelayEnds{}, St{True{}, sv, ow, ot, rs, c, j, 1n+rl})),
    lift(bit(sv), add(ow, add(ot, add(rs, add(bit(c), add(j, 1n+rl))))), add(ow, add(ot, add(rs, add(bit(c), add(j, rl))))),
      lift(ow, add(ot, add(rs, add(bit(c), add(j, 1n+rl)))), add(ot, add(rs, add(bit(c), add(j, rl)))),
      lift(ot, add(rs, add(bit(c), add(j, 1n+rl))), add(rs, add(bit(c), add(j, rl))),
      lift(rs, add(bit(c), add(j, 1n+rl)), add(bit(c), add(j, rl)),
      lift(bit(c), add(j, 1n+rl), add(j, rl),
      up(j, rl)))))))

# So is a job of the child's that ends,
law a_job_ending_shrinks:
  for +rp: Bool
  for +sv: Bool
  for +ow: Nat
  for +ot: Nat
  for +rs: Nat
  for +c: Bool
  for +j: Nat
  for +rl: Nat
  {(can(True{}, JobEnds{}, St{rp, sv, ow, ot, rs, c, 1n+j, rl}), left(St{rp, sv, ow, ot, rs, c, 1n+j, rl})) == (True{}, 1n+left(step(JobEnds{}, St{rp, sv, ow, ot, rs, c, 1n+j, rl}))) : Bool & Nat}

def a_job_ending_shrinks(rp, sv, ow, ot, rs, c, j, rl):
  Equal.cong(Nat, Bool & Nat, k => (True{}, k), left(St{rp, sv, ow, ot, rs, c, 1n+j, rl}), 1n+left(step(JobEnds{}, St{rp, sv, ow, ot, rs, c, 1n+j, rl})),
    lift(bit(sv), add(ow, add(ot, add(rs, add(bit(c), add(1n+j, rl))))), add(ow, add(ot, add(rs, add(bit(c), add(j, rl))))),
      lift(ow, add(ot, add(rs, add(bit(c), add(1n+j, rl)))), add(ot, add(rs, add(bit(c), add(j, rl)))),
      lift(ot, add(rs, add(bit(c), add(1n+j, rl))), add(rs, add(bit(c), add(j, rl))),
      lift(rs, add(bit(c), add(1n+j, rl)), add(bit(c), add(j, rl)),
      lift(bit(c), add(1n+j, rl), add(j, rl),
      {==}))))))

# the child's exit,
law the_child_exiting_shrinks:
  for +rp: Bool
  for +sv: Bool
  for +ow: Nat
  for +ot: Nat
  for +rs: Nat
  {(can(True{}, ChildExits{}, St{rp, sv, ow, ot, rs, True{}, 0n, 0n}), left(St{rp, sv, ow, ot, rs, True{}, 0n, 0n})) == (True{}, 1n+left(step(ChildExits{}, St{rp, sv, ow, ot, rs, True{}, 0n, 0n}))) : Bool & Nat}

def the_child_exiting_shrinks(rp, sv, ow, ot, rs):
  Equal.cong(Nat, Bool & Nat, k => (True{}, k), left(St{rp, sv, ow, ot, rs, True{}, 0n, 0n}), 1n+left(step(ChildExits{}, St{rp, sv, ow, ot, rs, True{}, 0n, 0n})),
    lift(bit(sv), add(ow, add(ot, add(rs, 1n))), add(ow, add(ot, add(rs, 0n))),
      lift(ow, add(ot, add(rs, 1n)), add(ot, add(rs, 0n)),
      lift(ot, add(rs, 1n), add(rs, 0n),
      lift(rs, 1n, 0n,
      {==})))))

# a resident the supervisor ends,
law a_resident_ended_shrinks:
  for +rp: Bool
  for +sv: Bool
  for +ow: Nat
  for +ot: Nat
  for +rs: Nat
  for +j: Nat
  for +rl: Nat
  {(can(True{}, ResidentEnded{}, St{rp, sv, ow, ot, 1n+rs, False{}, j, rl}), left(St{rp, sv, ow, ot, 1n+rs, False{}, j, rl})) == (True{}, 1n+left(step(ResidentEnded{}, St{rp, sv, ow, ot, 1n+rs, False{}, j, rl}))) : Bool & Nat}

def a_resident_ended_shrinks(rp, sv, ow, ot, rs, j, rl):
  Equal.cong(Nat, Bool & Nat, k => (True{}, k), left(St{rp, sv, ow, ot, 1n+rs, False{}, j, rl}), 1n+left(step(ResidentEnded{}, St{rp, sv, ow, ot, 1n+rs, False{}, j, rl})),
    lift(bit(sv), add(ow, add(ot, add(1n+rs, add(0n, add(j, rl))))), add(ow, add(ot, add(rs, add(0n, add(j, rl))))),
      lift(ow, add(ot, add(1n+rs, add(0n, add(j, rl)))), add(ot, add(rs, add(0n, add(j, rl)))),
      lift(ot, add(1n+rs, add(0n, add(j, rl))), add(rs, add(0n, add(j, rl))),
      {==}))))

# another requester's job that ends,
law another_job_ending_shrinks:
  for +rp: Bool
  for +sv: Bool
  for +ow: Nat
  for +ot: Nat
  for +rs: Nat
  for +c: Bool
  for +j: Nat
  for +rl: Nat
  {(can(True{}, OtherEnds{}, St{rp, sv, ow, 1n+ot, rs, c, j, rl}), left(St{rp, sv, ow, 1n+ot, rs, c, j, rl})) == (True{}, 1n+left(step(OtherEnds{}, St{rp, sv, ow, 1n+ot, rs, c, j, rl}))) : Bool & Nat}

def another_job_ending_shrinks(rp, sv, ow, ot, rs, c, j, rl):
  Equal.cong(Nat, Bool & Nat, k => (True{}, k), left(St{rp, sv, ow, 1n+ot, rs, c, j, rl}), 1n+left(step(OtherEnds{}, St{rp, sv, ow, 1n+ot, rs, c, j, rl})),
    lift(bit(sv), add(ow, add(1n+ot, add(rs, add(bit(c), add(j, rl))))), add(ow, add(ot, add(rs, add(bit(c), add(j, rl))))),
      lift(ow, add(1n+ot, add(rs, add(bit(c), add(j, rl)))), add(ot, add(rs, add(bit(c), add(j, rl)))),
      {==})))

# an owed end that is read,
law an_end_read_shrinks:
  for +rp: Bool
  for +sv: Bool
  for +ow: Nat
  for +c: Bool
  {(can(True{}, EndRead{}, St{rp, sv, 1n+ow, 0n, 0n, c, 0n, 0n}), left(St{rp, sv, 1n+ow, 0n, 0n, c, 0n, 0n})) == (True{}, 1n+left(step(EndRead{}, St{rp, sv, 1n+ow, 0n, 0n, c, 0n, 0n}))) : Bool & Nat}

def an_end_read_shrinks(rp, sv, ow, c):
  Equal.cong(Nat, Bool & Nat, k => (True{}, k), left(St{rp, sv, 1n+ow, 0n, 0n, c, 0n, 0n}), 1n+left(step(EndRead{}, St{rp, sv, 1n+ow, 0n, 0n, c, 0n, 0n})),
    lift(bit(sv), add(1n+ow, add(0n, add(0n, add(bit(c), add(0n, 0n))))), add(ow, add(0n, add(0n, add(bit(c), add(0n, 0n))))),
      {==}))

# and the retirement.
law retiring_shrinks:
  for +rp: Bool
  for +c: Bool
  {(can(True{}, Retires{}, St{rp, True{}, 0n, 0n, 0n, c, 0n, 0n}), left(St{rp, True{}, 0n, 0n, 0n, c, 0n, 0n})) == (True{}, 1n+left(step(Retires{}, St{rp, True{}, 0n, 0n, 0n, c, 0n, 0n}))) : Bool & Nat}

def retiring_shrinks(rp, c):
  Equal.cong(Nat, Bool & Nat, k => (True{}, k), left(St{rp, True{}, 0n, 0n, 0n, c, 0n, 0n}), 1n+left(step(Retires{}, St{rp, True{}, 0n, 0n, 0n, c, 0n, 0n})),
    {==})
