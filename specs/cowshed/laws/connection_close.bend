# How a controller connection ends when its peer closes (specs/cowshed/05_gateway.md "Deadlines",
# smoo/cowshed!n/controller-connection-own-thread; cowshed-core src/api/server.rs
# `serve_connection_with_waits`, its close in `ended_answers`). The loop selects between its reader
# and the next answer that ends. The connection serves on a thread of its own, so the peer's close
# and an answer's end may be ready together, and the select takes either.
#
# The model starts with the peer's close ready. `q` is the answers that have ended and are not yet
# joined, in the order they ended; an answer still running is in no queue, and no loop waits for
# it. A step is an answer ending, fine or fatal, or a poll of the loop taking one arm. The join arm
# reads the oldest ended answer: a fatal one ends the connection in its error, a fine one leaves
# the loop running. With nothing ended the join arm is not ready and the poll takes the reader.
# The reader arm ends the connection: Ok at once before the fix (`fixed` False), or after reading
# every ended answer (`drain`). `owed` is the ghost: a fatal answer has ended.
#
# Check: bend2 specs/cowshed/laws/connection_close.bend --verdict
import Base

type Out is Data:
  Fine{}
  Fatal{}

type Arm is Data:
  Read{}
  Join{}

type Step is Data:
  EndsFine{}
  EndsFatal{}
  Poll{a: Arm}

type Res is Data:
  Open{}
  Closed{got: Bool, owed: Bool}

def is_fatal(o: Out) -> Bool:
  match o:
    case Fine{}:
      False{}
    case Fatal{}:
      True{}

def any_fatal(q: +List<Out>) -> Bool:
  match q:
    case Nil{}:
      False{}
    case Con{o, t}:
      Bool.or(is_fatal(o), any_fatal(t))

def snoc(q: +List<Out>, +o: Out) -> +List<Out>:
  match q:
    case Nil{}:
      [o]
    case Con{h, t}:
      h <> snoc(t, o)

def drain(q: +List<Out>) -> Bool:
  match q:
    case Nil{}:
      False{}
    case Con{Fatal{}, t}:
      True{}
    case Con{Fine{}, t}:
      drain(t)

def close(fixed: Bool, q: +List<Out>) -> Bool:
  match fixed:
    case True{}:
      drain(q)
    case False{}:
      False{}

def run(fixed: Bool, steps: +List<Step>, q: +List<Out>, owed: Bool) -> Res:
  match steps q:
    case Nil{} q0:
      Open{}
    case Con{EndsFine{}, rest} q0:
      run(fixed, rest, snoc(q0, Fine{}), owed)
    case Con{EndsFatal{}, rest} q0:
      run(fixed, rest, snoc(q0, Fatal{}), True{})
    case Con{Poll{Read{}}, rest} q0:
      Closed{close(fixed, q0), owed}
    case Con{Poll{Join{}}, rest} Nil{}:
      Closed{close(fixed, []), owed}
    case Con{Poll{Join{}}, rest} Con{Fatal{}, t}:
      Closed{True{}, owed}
    case Con{Poll{Join{}}, rest} Con{Fine{}, t}:
      run(fixed, rest, t, owed)

def same(a: Bool, b: Bool) -> Bool:
  match a b:
    case True{} True{}:
      True{}
    case False{} False{}:
      True{}
    case True{} False{}:
      False{}
    case False{} True{}:
      False{}

def honest(r: Res) -> Bool:
  match r:
    case Open{}:
      True{}
    case Closed{got, owed}:
      same(got, owed)

# Witness against returning at the close without the drain: one fatal answer has ended, the poll
# takes the reader, and the connection ends Ok. connection_close_open.bend states the law this
# refutes.
law the_old_loop_loses_an_ended_fatal:
  {run(False{}, [Poll{Read{}}], [Fatal{}], True{}) == Closed{False{}, True{}} : Res}

def the_old_loop_loses_an_ended_fatal():
  {==}


def same_refl(+b: Bool) -> {same(b, b) == True{} : Bool}:
  match b:
    case True{}:
      {==}
    case False{}:
      {==}

def drain_is_any(+q: +List<Out>) -> {same(drain(q), any_fatal(q)) == True{} : Bool}:
  match q:
    case Nil{}:
      {==}
    case Con{Fatal{}, t}:
      {==}
    case Con{Fine{}, t}:
      drain_is_any(t)

def fine_adds_none(+q: +List<Out>) -> {any_fatal(snoc(q, Fine{})) == any_fatal(q) : Bool}:
  match q:
    case Nil{}:
      {==}
    case Con{Fatal{}, t}:
      {==}
    case Con{Fine{}, t}:
      fine_adds_none(t)

def fatal_adds_one(+q: +List<Out>) -> {any_fatal(snoc(q, Fatal{})) == True{} : Bool}:
  match q:
    case Nil{}:
      {==}
    case Con{Fatal{}, t}:
      {==}
    case Con{Fine{}, t}:
      fatal_adds_one(t)

# For every schedule and every queue: when the loop returns, it returns an error exactly when a
# fatal answer has ended. So an ended fatal answer is always the connection's result, and an Ok
# return has read every ended answer.
law close_reports_every_ended_answer:
  for +steps: +List<Step>
  for +q: +List<Out>
  {honest(run(True{}, steps, q, any_fatal(q))) == True{} : Bool}

def close_reports_every_ended_answer(steps, q):
  match steps q:
    case Nil{} q0:
      {==}
    case Con{EndsFine{}, rest} q0:
      Equal.trans(Bool,
        honest(run(True{}, rest, snoc(q0, Fine{}), any_fatal(q0))),
        honest(run(True{}, rest, snoc(q0, Fine{}), any_fatal(snoc(q0, Fine{})))),
        True{},
        Equal.sym(Bool,
          honest(run(True{}, rest, snoc(q0, Fine{}), any_fatal(snoc(q0, Fine{})))),
          honest(run(True{}, rest, snoc(q0, Fine{}), any_fatal(q0))),
          Equal.cong(Bool, Bool, owed => honest(run(True{}, rest, snoc(q0, Fine{}), owed)),
            any_fatal(snoc(q0, Fine{})), any_fatal(q0), fine_adds_none(q0))),
        close_reports_every_ended_answer(rest, snoc(q0, Fine{})))
    case Con{EndsFatal{}, rest} q0:
      Equal.trans(Bool,
        honest(run(True{}, rest, snoc(q0, Fatal{}), True{})),
        honest(run(True{}, rest, snoc(q0, Fatal{}), any_fatal(snoc(q0, Fatal{})))),
        True{},
        Equal.sym(Bool,
          honest(run(True{}, rest, snoc(q0, Fatal{}), any_fatal(snoc(q0, Fatal{})))),
          honest(run(True{}, rest, snoc(q0, Fatal{}), True{})),
          Equal.cong(Bool, Bool, owed => honest(run(True{}, rest, snoc(q0, Fatal{}), owed)),
            any_fatal(snoc(q0, Fatal{})), True{}, fatal_adds_one(q0))),
        close_reports_every_ended_answer(rest, snoc(q0, Fatal{})))
    case Con{Poll{Read{}}, rest} q0:
      drain_is_any(q0)
    case Con{Poll{Join{}}, rest} Nil{}:
      {==}
    case Con{Poll{Join{}}, rest} Con{Fatal{}, t}:
      {==}
    case Con{Poll{Join{}}, rest} Con{Fine{}, t}:
      close_reports_every_ended_answer(rest, t)
