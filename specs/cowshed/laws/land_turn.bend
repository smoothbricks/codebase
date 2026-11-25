# Land turns (specs/cowshed/05_gateway.md "Land turns", contract "Land-turn
# laws"): one repository's queue as the gateway's pure core `Turns` keeps it
# (cowshed-gateway src/land_turn.rs). A ticket is numbered as its ask arrives
# and takes its place at the end; the first ticket holds the turn; a turn is
# held by its asker and by every command bound to it, and passes on at once
# when the last of them has gone; only an unfenced held turn binds a command.
# Repositories' queues are independent (tickets are numbered globally and found
# by number), so one queue is the model.
#
# An install's read of the turns asked with the fence (05_gateway.md "The read
# fences the turns") fences every queue when every live ticket is fenced
# (including an empty queue): `TFence` is that read, `TUnfence` a fence's
# connection closing, and `fences` how many fences hold. An install may wait
# for the turns to fence instead (`TWait`): when every live ticket is already
# fenced (or the queue is empty) it is fenced at once; otherwise it waits
# (`waiting` counts such installs), every resumable asker acknowledges with
# `TFenced{num}` within one heartbeat, and the step that leaves every live
# ticket fenced (a `TFenced`, departure, or bound command's exit) fences every
# waiting install without waiting for fenced tickets to end. `TWaitLeave` is a
# waiting install's connection closing first. `TResume{num}` is a fenced asker
# resuming its ticket on the replacing gateway.
#
# Every ask names its asker, the process the kernel names as the connection's
# peer, and whether the ask is bounded (`TAsk{pid, bounded}`: True for
# `"class":"bounded"`, False for an ordinary land turn), and a ticket keeps
# both `bounded` and `fenced`. While any fence holds (`f > 0`), every ask from
# outside every unfenced held turn is parked (`parked` keeps `P{pid, bounded}`
# in arrival order), never numbered. While an install waits with no fence held
# (`w > 0, f = 0`), a non-bounded ask from outside every unfenced held turn is
# parked; a bounded ask from outside is numbered while any non-bounded ticket
# (unfenced or fenced) remains in the queue (`has_unbounded`), and parks once
# only bounded tickets remain so the install waits at most one bound past the
# last land ticket. Once no install waits and no fence holds without a restart,
# fenced tickets are unmarked in place and the parked asks are numbered in the
# order they arrived, behind every ticket already queued.
#
# An ask made inside an unfenced held turn, by the asker of an unfenced ticket
# that holds its turn before it fences or ends its hold or by a command bound
# to an unfenced turn (`inside`), is not an ask from outside: a land takes its
# ref turn inside its land turn ("A ref turn"), and the install waits for that
# turn to fence or end. So it is numbered whatever installs wait or fences
# hold. A fenced ticket asks from outside.
#
# Check: bun bend2/main.ts land_turn.bend --verdict
# Bound to the code by cowshed-gateway src/land_turn/laws.rs, which runs the
# emitted model (src/land_turn/laws/model.rs) and `Turns` on the same inputs.
import Base

type E is Data:
  E{num: Nat, pid: Nat, asker: Bool, bound: +List<Nat>, held: Bool, bounded: Bool, fenced: Bool}

type P is Data:
  P{pid: Nat, bounded: Bool}

type Op is Data:
  TAsk{pid: Nat, bounded: Bool}
  TBind{num: Nat, pid: Nat}
  TExit{num: Nat, pid: Nat}
  TLeave{num: Nat}
  TFence{}
  TUnfence{}
  TWait{}
  TWaitLeave{}
  TFenced{num: Nat}
  TResume{num: Nat}

type Bound is Data:
  BGranted{}
  BGone{}
  BWaiting{}
  BFenced{}

type St is Data:
  St{q: +List<E>, numbered: Nat, fences: Nat, waiting: Nat, parked: +List<P>}

# --- Nat and list helpers ---------------------------------------------------

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

def or(a: Bool, b: Bool) -> Bool:
  match a:
    case True{}:
      True{}
    case False{}:
      b

def and(a: Bool, b: Bool) -> Bool:
  match a:
    case True{}:
      b
    case False{}:
      False{}

def not(a: Bool) -> Bool:
  match a:
    case True{}:
      False{}
    case False{}:
      True{}

def mem(+x: Nat, xs: +List<Nat>) -> Bool:
  match xs:
    case Nil{}:
      False{}
    case Con{h, t}:
      or(same(x, h), mem(x, t))

def remove.if(h: Nat, r: +List<Nat>, b: Bool) -> +List<Nat>:
  match b:
    case True{}:
      r
    case False{}:
      h <> r

def remove(+x: Nat, xs: +List<Nat>) -> +List<Nat>:
  match xs:
    case Nil{}:
      []
    case Con{+h, t}:
      remove.if(h, remove(x, t), same(x, h))

def push(xs: +List<Nat>, +x: Nat) -> +List<Nat>:
  match xs:
    case Nil{}:
      [x]
    case Con{h, t}:
      h <> push(t, x)

def push_p(xs: +List<P>, +x: P) -> +List<P>:
  match xs:
    case Nil{}:
      [x]
    case Con{h, t}:
      h <> push_p(t, x)

def add.if(+x: Nat, xs: +List<Nat>, b: Bool) -> +List<Nat>:
  match b:
    case True{}:
      xs
    case False{}:
      push(xs, x)

# a bound pid is kept once, in bind order (land_turn.rs `bind` pushes it)
def add(+x: Nat, +xs: +List<Nat>) -> +List<Nat>:
  add.if(x, xs, mem(x, xs))

def is_nil(xs: +List<Nat>) -> Bool:
  match xs:
    case Nil{}:
      True{}
    case Con{h, t}:
      False{}

# --- the queue ----------------------------------------------------------------

def num(e: E) -> Nat:
  match e:
    case E{n, z, a, b, h, d, f}:
      n

def held(e: E) -> Bool:
  match e:
    case E{n, z, a, b, h, d, f}:
      h

def bounded(e: E) -> Bool:
  match e:
    case E{n, z, a, b, h, d, f}:
      d

# some queued ticket (unfenced or fenced) is non-bounded (land_turn.rs `has_unbounded`)
def has_unbounded(q: +List<E>) -> Bool:
  match q:
    case Nil{}:
      False{}
    case Con{e, t}:
      or(not(bounded(e)), has_unbounded(t))

def is_fenced(e: E) -> Bool:
  match e:
    case E{n, z, a, b, h, d, f}:
      f

# a ticket is fenced for restart when its asker has acknowledged the fence, or
# has already left (only bound commands remain, with no live asker to wait for)
def ticket_fenced(e: E) -> Bool:
  match e:
    case E{n, z, True{}, b, h, d, f}:
      f
    case E{n, z, False{}, b, h, d, f}:
      True{}

# every live ticket in the queue has fenced (vacuously true for an empty queue)
def all_fenced(q: +List<E>) -> Bool:
  match q:
    case Nil{}:
      True{}
    case Con{e, t}:
      and(ticket_fenced(e), all_fenced(t))

# no ticket in the queue carries the fenced mark
def none_fenced(q: +List<E>) -> Bool:
  match q:
    case Nil{}:
      True{}
    case Con{e, t}:
      and(not(is_fenced(e)), none_fenced(t))

# nothing holds the ticket: neither its asker nor a bound command
def done(e: E) -> Bool:
  match e:
    case E{n, z, a, b, h, d, f}:
      match a:
        case True{}:
          False{}
        case False{}:
          is_nil(b)

def snoc(q: +List<E>, +e: E) -> +List<E>:
  match q:
    case Nil{}:
      [e]
    case Con{h, t}:
      h <> snoc(t, e)

# the first ticket holds the turn (land_turn.rs `tell`)
def tell(q: +List<E>) -> +List<E>:
  match q:
    case Nil{}:
      []
    case Con{E{n, z, a, b, h, d, f}, t}:
      E{n, z, a, b, True{}, d, f} <> t

# clear the fenced mark on every ticket when the last fence lifts without restart
def unfence_all(q: +List<E>) -> +List<E>:
  match q:
    case Nil{}:
      []
    case Con{E{n, z, a, b, h, d, f}, t}:
      E{n, z, a, b, h, d, False{}} <> unfence_all(t)

# what one input does to the ticket it names
type Edit is Data:
  AddPid{pid: Nat}
  DropPid{pid: Nat}
  Depart{}
  MarkFenced{}
  UnfenceOne{}

def apply(k: Edit, e: E) -> E:
  match k e:
    case AddPid{p} E{n, z, a, +b, h, d, f}:
      E{n, z, a, add(p, b), h, d, f}
    case DropPid{p} E{n, z, a, +b, h, d, f}:
      E{n, z, a, remove(p, b), h, d, f}
    case Depart{} E{n, z, a, b, h, d, f}:
      E{n, z, False{}, b, h, d, f}
    case MarkFenced{} E{n, z, a, b, h, d, f}:
      E{n, z, a, b, h, d, True{}}
    case UnfenceOne{} E{n, z, a, b, h, d, f}:
      E{n, z, a, b, h, d, False{}}

def edit.if(+k: Edit, +e: E, t: +List<E>, r: +List<E>, b: Bool) -> +List<E>:
  match b:
    case True{}:
      apply(k, e) <> t
    case False{}:
      e <> r

def edit(+n: Nat, +k: Edit, q: +List<E>) -> +List<E>:
  match q:
    case Nil{}:
      []
    case Con{+e, +t}:
      edit.if(k, e, t, edit(n, k, t), same(n, num(e)))

# remove ticket n once nothing holds it (land_turn.rs `settle`)
def drop.if(+e: E, t: +List<E>, b: Bool) -> +List<E>:
  match b:
    case True{}:
      t
    case False{}:
      e <> t

def sweep.if(+e: E, t: +List<E>, r: +List<E>, b: Bool) -> +List<E>:
  match b:
    case True{}:
      drop.if(e, t, done(e))
    case False{}:
      e <> r

def sweep(+n: Nat, q: +List<E>) -> +List<E>:
  match q:
    case Nil{}:
      []
    case Con{+e, +t}:
      sweep.if(e, t, sweep(n, t), same(n, num(e)))

def settle(+n: Nat, +q: +List<E>) -> +List<E>:
  tell(sweep(n, q))

def find.held(h: Bool, f: Bool) -> Bound:
  match h f:
    case True{} False{}:
      BGranted{}
    case True{} True{}:
      BFenced{}
    case False{} f0:
      BWaiting{}

def find.if(+e: E, r: Bound, b: Bool) -> Bound:
  match b:
    case True{}:
      find.held(held(e), is_fenced(e))
    case False{}:
      r

def find(+n: Nat, q: +List<E>) -> Bound:
  match q:
    case Nil{}:
      BGone{}
    case Con{+e, t}:
      find.if(e, find(n, t), same(n, num(e)))

# what a bind is answered (land_turn.rs `bind`)
def verdict(o: Op, s: St) -> Bound:
  match o s:
    case TBind{n, p} St{+q, c, f, w, k}:
      find(n, q)
    case TAsk{x, d} St{q, c, f, w, k}:
      BGranted{}
    case TExit{n, p} St{q, c, f, w, k}:
      BGranted{}
    case TLeave{n} St{q, c, f, w, k}:
      BGranted{}
    case TFence{} St{q, c, f, w, k}:
      BGranted{}
    case TUnfence{} St{q, c, f, w, k}:
      BGranted{}
    case TWait{} St{q, c, f, w, k}:
      BGranted{}
    case TWaitLeave{} St{q, c, f, w, k}:
      BGranted{}
    case TFenced{n} St{q, c, f, w, k}:
      BGranted{}
    case TResume{n} St{q, c, f, w, k}:
      BGranted{}

def bind.if(+n: Nat, +p: Nat, +q: +List<E>, v: Bound) -> +List<E>:
  match v:
    case BGranted{}:
      tell(edit(n, AddPid{p}, q))
    case BGone{}:
      q
    case BWaiting{}:
      q
    case BFenced{}:
      q

# a bounded ask from outside under a waiting install (f = 0, w > 0): numbered while
# some non-bounded ticket (unfenced or fenced) remains in q, parked once only
# bounded tickets remain
def ask.wb(+q: +List<E>, +c: Nat, +v: Nat, p: +List<P>, +x: Nat, u: Bool) -> St:
  match u:
    case True{}:
      St{tell(snoc(q, E{1n+c, x, True{}, [], False{}, True{}, False{}})), 1n+c, 0n, 1n+v, p}
    case False{}:
      St{q, c, 0n, 1n+v, push_p(p, P{x, True{}})}

# an ask from outside every unfenced held turn, with no fence held (f = 0)
def ask.w(+q: +List<E>, +c: Nat, w: Nat, p: +List<P>, +x: Nat, +d: Bool) -> St:
  match w:
    case 0n:
      St{tell(snoc(q, E{1n+c, x, True{}, [], False{}, d, False{}})), 1n+c, 0n, 0n, p}
    case 1n+v:
      match d:
        case False{}:
          St{q, c, 0n, 1n+v, push_p(p, P{x, False{}})}
        case True{}:
          ask.wb(q, c, v, p, x, has_unbounded(q))

# ... and any ask from outside under a fence (f > 0) is parked
def ask(+q: +List<E>, +c: Nat, f: Nat, w: Nat, p: +List<P>, +x: Nat, +d: Bool) -> St:
  match f:
    case 0n:
      ask.w(q, c, w, p, x, d)
    case 1n+g:
      St{q, c, 1n+g, w, push_p(p, P{x, d})}

# the asker of an unfenced ticket holds its turn once the ticket holds it and
# until the asker fences or ends its hold
def asking(a: Bool, h: Bool) -> Bool:
  match h:
    case True{}:
      a
    case False{}:
      False{}

def mine.if(a: Bool, h: Bool, s: Bool) -> Bool:
  match s:
    case True{}:
      asking(a, h)
    case False{}:
      False{}

# the process x holds this unfenced ticket's turn: a command bound to it, or
# its asker; a fenced ticket asks from outside
def within(+x: Nat, e: E) -> Bool:
  match e:
    case E{n, z, a, +b, h, d, False{}}:
      or(mem(x, b), mine.if(a, h, same(x, z)))
    case E{n, z, a, b, h, d, True{}}:
      False{}

# the process x asks from inside an unfenced held turn (land_turn.rs `inside`)
def inside(+x: Nat, q: +List<E>) -> Bool:
  match q:
    case Nil{}:
      False{}
    case Con{e, t}:
      or(within(x, e), inside(x, t))

# an ask made inside an unfenced held turn is numbered and enters its queue
# whatever installs wait and whatever fences hold, and changes neither them nor
# the parked asks; any other is an ask from outside (land_turn.rs `ask`)
def ask.in(+q: +List<E>, +c: Nat, f: Nat, w: Nat, p: +List<P>, +x: Nat, +d: Bool, i: Bool) -> St:
  match i:
    case True{}:
      St{tell(snoc(q, E{1n+c, x, True{}, [], False{}, d, False{}})), 1n+c, f, w, p}
    case False{}:
      ask(q, c, f, w, p, x, d)

# a read fences when every live ticket is fenced (land_turn.rs `fence`)
def fence.if(f: Nat, ready: Bool) -> Nat:
  match ready:
    case True{}:
      1n+f
    case False{}:
      f

def fence(q: +List<E>, f: Nat) -> Nat:
  fence.if(f, all_fenced(q))

# one fewer fence or waiting install, never fewer than none (land_turn.rs
# `unfence`, `wait_left`)
def less(f: Nat) -> Nat:
  match f:
    case 0n:
      0n
    case 1n+g:
      g

def plus(+a: Nat, b: Nat) -> Nat:
  match a:
    case 0n:
      b
    case 1n+a1:
      1n+plus(a1, b)

# an install waits for the turns to fence: when every live ticket is already
# fenced (or the queue is empty) it is fenced at once, and otherwise it waits
# (land_turn.rs `wait_fence`)
def wait.if(q: +List<E>, c: Nat, f: Nat, w: Nat, p: +List<P>, ready: Bool) -> St:
  match ready:
    case True{}:
      St{q, c, 1n+f, w, p}
    case False{}:
      St{q, c, f, 1n+w, p}

def wait(+q: +List<E>, c: Nat, f: Nat, w: Nat, p: +List<P>) -> St:
  wait.if(q, c, f, w, p, all_fenced(q))

# the step that leaves every live ticket fenced fences every waiting install
# (land_turn.rs `settle`, `fenced`)
def wake.if(q: +List<E>, c: Nat, f: Nat, w: Nat, p: +List<P>, ready: Bool) -> St:
  match ready:
    case True{}:
      St{q, c, plus(w, f), 0n, p}
    case False{}:
      St{q, c, f, w, p}

def wake(+q: +List<E>, c: Nat, f: Nat, w: Nat, p: +List<P>) -> St:
  wake.if(q, c, f, w, p, all_fenced(q))

# every parked ask numbered in arrival order and put at the end of its queue
# (land_turn.rs `release`)
def admit(p: +List<P>, +q: +List<E>, +c: Nat) -> St:
  match p:
    case Nil{}:
      St{q, c, 0n, 0n, []}
    case Con{P{+x, +d}, r}:
      admit(r, tell(snoc(q, E{1n+c, x, True{}, [], False{}, d, False{}})), 1n+c)

def lift.w(+q: +List<E>, +c: Nat, w: Nat, p: +List<P>) -> St:
  match w:
    case 0n:
      admit(p, unfence_all(q), c)
    case 1n+v:
      St{q, c, 0n, 1n+v, p}

# once no install waits and no fence holds, fenced tickets unmark in place and
# the parked asks are admitted (land_turn.rs `release`)
def lift(+q: +List<E>, +c: Nat, f: Nat, w: Nat, p: +List<P>) -> St:
  match f:
    case 0n:
      lift.w(q, c, w, p)
    case 1n+g:
      St{q, c, 1n+g, w, p}

def step(o: Op, s: St) -> St:
  match o s:
    case TAsk{+x, +d} St{+q, +c, f, w, p}:
      ask.in(q, c, f, w, p, x, d, inside(x, q))
    case TBind{+n, +p} St{+q, c, f, w, k}:
      St{bind.if(n, p, q, find(n, q)), c, f, w, k}
    case TExit{+n, +p} St{+q, c, f, w, k}:
      wake(settle(n, edit(n, DropPid{p}, q)), c, f, w, k)
    case TLeave{+n} St{+q, c, f, w, p}:
      wake(settle(n, edit(n, Depart{}, q)), c, f, w, p)
    case TFence{} St{+q, c, f, w, p}:
      St{q, c, fence(q, f), w, p}
    case TUnfence{} St{q, c, f, w, p}:
      lift(q, c, less(f), w, p)
    case TWait{} St{+q, c, f, w, p}:
      wait(q, c, f, w, p)
    case TWaitLeave{} St{q, c, f, w, p}:
      lift(q, c, f, less(w), p)
    case TFenced{+n} St{+q, c, f, w, p}:
      wake(edit(n, MarkFenced{}, q), c, f, w, p)
    case TResume{+n} St{+q, c, f, w, p}:
      St{edit(n, UnfenceOne{}, q), c, f, w, p}

def run(os: +List<Op>, s: St) -> St:
  match os:
    case Nil{}:
      s
    case Con{o, t}:
      run(t, step(o, s))

def empty() -> St:
  St{[], 0n, 0n, 0n, []}

def queue(s: St) -> +List<E>:
  match s:
    case St{q, c, f, w, p}:
      q

def counter(s: St) -> Nat:
  match s:
    case St{q, c, f, w, p}:
      c

def fences(s: St) -> Nat:
  match s:
    case St{q, c, f, w, p}:
      f

def waiting(s: St) -> Nat:
  match s:
    case St{q, c, f, w, p}:
      w

def parked(s: St) -> +List<P>:
  match s:
    case St{q, c, f, w, p}:
      p

# --- order facts --------------------------------------------------------------

def LE(a: Nat, b: Nat) -> Data:
  match a b:
    case 0n b0:
      Unit
    case 1n+a1 0n:
      Empty
    case 1n+a1 1n+b1:
      LE(a1, b1)

def le_refl(+n: Nat) -> LE(n, n):
  match n:
    case 0n:
      Unit{}
    case 1n+p:
      le_refl(p)

def le_trans(+a: Nat, +b: Nat, +c: Nat, ab: LE(a, b), bc: LE(b, c)) -> LE(a, c):
  match a b c:
    case 0n b0 c0:
      Unit{}
    case 1n+a1 0n c0:
      match ab:
    case 1n+a1 1n+b1 0n:
      match bc:
    case 1n+a1 1n+b1 1n+c1:
      le_trans(a1, b1, c1, ab, bc)

# a < b gives a <= b
def le_of_lt(+a: Nat, +b: Nat, h: LE(1n+a, b)) -> LE(a, b):
  match a b:
    case 0n b0:
      Unit{}
    case 1n+a1 0n:
      match h:
    case 1n+a1 1n+b1:
      le_of_lt(a1, b1, h)

# --- the invariants -------------------------------------------------------------

# FIFO: ticket numbers rise strictly from the first ticket to the last, every
# one above `lo` and none above the last number given
def Chain(+lo: Nat, q: +List<E>, +top: Nat) -> Data:
  match q:
    case Nil{}:
      LE(lo, top)
    case Con{+e, t}:
      Sigma<&2, &2, LE(1n+lo, num(e)), u => Chain(num(e), t, top)>

def IsT(b: Bool) -> Data:
  match b:
    case True{}:
      Unit
    case False{}:
      Empty

def IsF(b: Bool) -> Data:
  match b:
    case True{}:
      Empty
    case False{}:
      Unit

def IsNil(xs: +List<Nat>) -> Data:
  match xs:
    case Nil{}:
      Unit
    case Con{h, t}:
      Empty

# a waiting ticket: it does not hold the turn and no command is bound to it
def Idle(e: E) -> Data:
  match e:
    case E{n, z, a, b, h, d, f}:
      Sigma<&2, &2, IsF(h), u => IsNil(b)>

def Waiters(q: +List<E>) -> Data:
  match q:
    case Nil{}:
      Unit
    case Con{e, t}:
      Sigma<&2, &2, Idle(e), u => Waiters(t)>

# the first ticket holds the turn and every other waits
def TurnOk(q: +List<E>) -> Data:
  match q:
    case Nil{}:
      Unit
    case Con{e, t}:
      Sigma<&2, &2, IsT(held(e)), u => Waiters(t)>

# between an input and `tell`: every ticket but the first waits
def Pre(q: +List<E>) -> Data:
  match q:
    case Nil{}:
      Unit
    case Con{e, t}:
      Waiters(t)

def Inv(s: St) -> Data:
  match s:
    case St{+q, +c, f, w, p}:
      Sigma<&2, &2, TurnOk(q), u => Chain(0n, q, c)>

# --- FIFO is kept ---------------------------------------------------------------

def chain_weaken(+lo: Nat, +n: Nat, +t: +List<E>, +top: Nat, le: LE(lo, n), ch: Chain(n, t, top)) -> Chain(lo, t, top):
  match t:
    case Nil{}:
      le_trans(lo, n, top, le, ch)
    case Con{e, r}:
      (lt, rest) = ch
      (le_trans(1n+lo, 1n+n, num(e), le, lt), rest)

def chain_snoc(+q: +List<E>, +lo: Nat, +top: Nat, +z: Nat, +a: Bool, +b: +List<Nat>, +h: Bool, +d: Bool, +f: Bool, ch: Chain(lo, q, top)) -> Chain(lo, snoc(q, E{1n+top, z, a, b, h, d, f}), 1n+top):
  match q:
    case Nil{}:
      (ch, le_refl(1n+top))
    case Con{e, t}:
      (lt, rest) = ch
      (lt, chain_snoc(t, num(e), top, z, a, b, h, d, f, rest))

def chain_tell(+lo: Nat, +q: +List<E>, +top: Nat, ch: Chain(lo, q, top)) -> Chain(lo, tell(q), top):
  match q:
    case Nil{}:
      ch
    case Con{E{n, z, a, b, h, d, f}, t}:
      ch

def chain_unfence_all(+q: +List<E>, +lo: Nat, +top: Nat, ch: Chain(lo, q, top)) -> Chain(lo, unfence_all(q), top):
  match q:
    case Nil{}:
      ch
    case Con{E{n, z, a, b, h, d, f}, t}:
      (lt, rest) = ch
      (lt, chain_unfence_all(t, n, top, rest))

def chain_apply(+lo: Nat, +k: Edit, +e: E, +t: +List<E>, +top: Nat, ch: Chain(lo, e <> t, top)) -> Chain(lo, apply(k, e) <> t, top):
  match k e:
    case AddPid{p} E{n, z, a, b, h, d, f}:
      ch
    case DropPid{p} E{n, z, a, b, h, d, f}:
      ch
    case Depart{} E{n, z, a, b, h, d, f}:
      ch
    case MarkFenced{} E{n, z, a, b, h, d, f}:
      ch
    case UnfenceOne{} E{n, z, a, b, h, d, f}:
      ch

def chain_edit.if(+lo: Nat, +k: Edit, +e: E, +t: +List<E>, +r: +List<E>, +top: Nat, b: Bool, ch: Chain(lo, e <> t, top), cr: Chain(num(e), r, top)) -> Chain(lo, edit.if(k, e, t, r, b), top):
  match b:
    case True{}:
      chain_apply(lo, k, e, t, top, ch)
    case False{}:
      (lt, rest) = ch
      (lt, cr)

def chain_edit(+q: +List<E>, +lo: Nat, +n: Nat, +k: Edit, +top: Nat, +ch: Chain(lo, q, top)) -> Chain(lo, edit(n, k, q), top):
  match q:
    case Nil{}:
      ch
    case Con{e, t}:
      (lt, +rest) = ch
      chain_edit.if(lo, k, e, t, edit(n, k, t), top, same(n, num(e)), ch, chain_edit(t, num(e), n, k, top, rest))

def chain_drop.if(+lo: Nat, +e: E, +t: +List<E>, +top: Nat, c: Bool, ch: Chain(lo, e <> t, top)) -> Chain(lo, drop.if(e, t, c), top):
  match c:
    case True{}:
      (lt, rest) = ch
      chain_weaken(lo, num(e), t, top, le_of_lt(lo, num(e), lt), rest)
    case False{}:
      ch

def chain_sweep.if(+lo: Nat, +e: E, +t: +List<E>, +r: +List<E>, +top: Nat, b: Bool, ch: Chain(lo, e <> t, top), cr: Chain(num(e), r, top)) -> Chain(lo, sweep.if(e, t, r, b), top):
  match b:
    case True{}:
      chain_drop.if(lo, e, t, top, done(e), ch)
    case False{}:
      (lt, rest) = ch
      (lt, cr)

def chain_sweep(+q: +List<E>, +lo: Nat, +n: Nat, +top: Nat, +ch: Chain(lo, q, top)) -> Chain(lo, sweep(n, q), top):
  match q:
    case Nil{}:
      ch
    case Con{e, t}:
      (lt, +rest) = ch
      chain_sweep.if(lo, e, t, sweep(n, t), top, same(n, num(e)), ch, chain_sweep(t, num(e), n, top, rest))

# --- one holder: the first ticket holds the turn, every other waits -------------

def pre_tell(+q: +List<E>, p: Pre(q)) -> TurnOk(tell(q)):
  match q:
    case Nil{}:
      Unit{}
    case Con{E{n, z, a, b, h, d, f}, t}:
      (Unit{}, p)

def waiters_unfence_all(+q: +List<E>, w: Waiters(q)) -> Waiters(unfence_all(q)):
  match q:
    case Nil{}:
      Unit{}
    case Con{E{n, z, a, b, h, d, f}, t}:
      (ie, wr) = w
      (ie, waiters_unfence_all(t, wr))

def turn_ok_unfence_all(+q: +List<E>, ok: TurnOk(q)) -> TurnOk(unfence_all(q)):
  match q:
    case Nil{}:
      Unit{}
    case Con{E{n, z, a, b, h, d, f}, t}:
      (he, w) = ok
      (he, waiters_unfence_all(t, w))

def waiters_pre(+t: +List<E>, w: Waiters(t)) -> Pre(t):
  match t:
    case Nil{}:
      Unit{}
    case Con{e, r}:
      (ie, wr) = w
      wr

def waiters_snoc(+t: +List<E>, +x: E, w: Waiters(t), ix: Idle(x)) -> Waiters(snoc(t, x)):
  match t:
    case Nil{}:
      (ix, Unit{})
    case Con{e, r}:
      (ie, wr) = w
      (ie, waiters_snoc(r, x, wr, ix))

def pre_snoc(+q: +List<E>, +x: E, ok: TurnOk(q), ix: Idle(x)) -> Pre(snoc(q, x)):
  match q:
    case Nil{}:
      Unit{}
    case Con{e, t}:
      (he, w) = ok
      waiters_snoc(t, x, w, ix)

# DropPid, Depart, MarkFenced and UnfenceOne never add a pid; AddPid is a bind
def NotAdd(k: Edit) -> Data:
  match k:
    case AddPid{p}:
      Empty
    case DropPid{p}:
      Unit
    case Depart{}:
      Unit
    case MarkFenced{}:
      Unit
    case UnfenceOne{}:
      Unit

def idle_apply(+k: Edit, +e: E, nk: NotAdd(k), ie: Idle(e)) -> Idle(apply(k, e)):
  match k e:
    case AddPid{p} E{n, z, a, b, h, d, f}:
      match nk:
    case DropPid{p} E{n, z, a, Nil{}, h, d, f}:
      ie
    case DropPid{p} E{n, z, a, Con{x, xs}, h, d, f}:
      (fh, nb) = ie
      match nb:
    case Depart{} E{n, z, a, b, h, d, f}:
      ie
    case MarkFenced{} E{n, z, a, b, h, d, f}:
      ie
    case UnfenceOne{} E{n, z, a, b, h, d, f}:
      ie

def waiters_edit.if(+k: Edit, +e: E, +t: +List<E>, +r: +List<E>, b: Bool, nk: NotAdd(k), ie: Idle(e), wt: Waiters(t), wr: Waiters(r)) -> Waiters(edit.if(k, e, t, r, b)):
  match b:
    case True{}:
      (idle_apply(k, e, nk, ie), wt)
    case False{}:
      (ie, wr)

def waiters_edit(+t: +List<E>, +n: Nat, +k: Edit, +nk: NotAdd(k), +w: Waiters(t)) -> Waiters(edit(n, k, t)):
  match t:
    case Nil{}:
      Unit{}
    case Con{e, r}:
      (ie, +wr) = w
      waiters_edit.if(k, e, r, edit(n, k, r), same(n, num(e)), nk, ie, wr, waiters_edit(r, n, k, nk, wr))

def pre_apply(+k: Edit, +e: E, +t: +List<E>, wt: Waiters(t)) -> Pre(apply(k, e) <> t):
  match k e:
    case AddPid{p} E{n, z, a, x, h, d, f}:
      wt
    case DropPid{p} E{n, z, a, x, h, d, f}:
      wt
    case Depart{} E{n, z, a, x, h, d, f}:
      wt
    case MarkFenced{} E{n, z, a, x, h, d, f}:
      wt
    case UnfenceOne{} E{n, z, a, x, h, d, f}:
      wt

def pre_edit.if(+k: Edit, +e: E, +t: +List<E>, +r: +List<E>, b: Bool, wt: Waiters(t), wr: Waiters(r)) -> Pre(edit.if(k, e, t, r, b)):
  match b:
    case True{}:
      pre_apply(k, e, t, wt)
    case False{}:
      wr

def pre_edit(+q: +List<E>, +n: Nat, +k: Edit, nk: NotAdd(k), ok: TurnOk(q)) -> Pre(edit(n, k, q)):
  match q:
    case Nil{}:
      Unit{}
    case Con{e, t}:
      (he, +w) = ok
      pre_edit.if(k, e, t, edit(n, k, t), same(n, num(e)), w, waiters_edit(t, n, k, nk, w))

def held_apply(+k: Edit, +e: E, he: IsT(held(e))) -> IsT(held(apply(k, e))):
  match k e:
    case AddPid{p} E{n, z, a, b, h, d, f}:
      he
    case DropPid{p} E{n, z, a, b, h, d, f}:
      he
    case Depart{} E{n, z, a, b, h, d, f}:
      he
    case MarkFenced{} E{n, z, a, b, h, d, f}:
      he
    case UnfenceOne{} E{n, z, a, b, h, d, f}:
      he

def turn_ok_edit.if(+k: Edit, +e: E, +t: +List<E>, +r: +List<E>, b: Bool, he: IsT(held(e)), wt: Waiters(t), wr: Waiters(r)) -> TurnOk(edit.if(k, e, t, r, b)):
  match b:
    case True{}:
      (held_apply(k, e, he), wt)
    case False{}:
      (he, wr)

def turn_ok_edit(+q: +List<E>, +n: Nat, +k: Edit, nk: NotAdd(k), ok: TurnOk(q)) -> TurnOk(edit(n, k, q)):
  match q:
    case Nil{}:
      Unit{}
    case Con{e, t}:
      (he, +w) = ok
      turn_ok_edit.if(k, e, t, edit(n, k, t), same(n, num(e)), he, w, waiters_edit(t, n, k, nk, w))

def waiters_drop.if(+e: E, +t: +List<E>, c: Bool, ie: Idle(e), wt: Waiters(t)) -> Waiters(drop.if(e, t, c)):
  match c:
    case True{}:
      wt
    case False{}:
      (ie, wt)

def waiters_sweep.if(+e: E, +t: +List<E>, +r: +List<E>, b: Bool, ie: Idle(e), wt: Waiters(t), wr: Waiters(r)) -> Waiters(sweep.if(e, t, r, b)):
  match b:
    case True{}:
      waiters_drop.if(e, t, done(e), ie, wt)
    case False{}:
      (ie, wr)

def waiters_sweep(+t: +List<E>, +n: Nat, +w: Waiters(t)) -> Waiters(sweep(n, t)):
  match t:
    case Nil{}:
      Unit{}
    case Con{e, r}:
      (ie, +wr) = w
      waiters_sweep.if(e, r, sweep(n, r), same(n, num(e)), ie, wr, waiters_sweep(r, n, wr))

def pre_drop.if(+e: E, +t: +List<E>, c: Bool, wt: Waiters(t)) -> Pre(drop.if(e, t, c)):
  match c:
    case True{}:
      waiters_pre(t, wt)
    case False{}:
      wt

def pre_sweep.if(+e: E, +t: +List<E>, +r: +List<E>, b: Bool, wt: Waiters(t), wr: Waiters(r)) -> Pre(sweep.if(e, t, r, b)):
  match b:
    case True{}:
      pre_drop.if(e, t, done(e), wt)
    case False{}:
      wr

def pre_sweep(+q: +List<E>, +n: Nat, +p: Pre(q)) -> Pre(sweep(n, q)):
  match q:
    case Nil{}:
      Unit{}
    case Con{e, t}:
      pre_sweep.if(e, t, sweep(n, t), same(n, num(e)), p, waiters_sweep(t, n, p))

def absurd(-A: Type, e: Empty) -> A:
  match e:

# --- a bind is granted only to an unfenced ticket that holds the turn -----------

def g_disc(v: Bound) -> Data:
  match v:
    case BGranted{}:
      Empty
    case BGone{}:
      Unit
    case BWaiting{}:
      Unit
    case BFenced{}:
      Unit

def gone_not_granted(g: {BGone{} == BGranted{} : Bound}) -> Empty:
  %g : g_disc(_)
  Unit{}

def waiting_not_granted(g: {BWaiting{} == BGranted{} : Bound}) -> Empty:
  %g : g_disc(_)
  Unit{}

def idle_not_granted(+e: E, ie: Idle(e), g: {find.held(held(e), is_fenced(e)) == BGranted{} : Bound}) -> Empty:
  match e:
    case E{n, z, a, b, True{}, d, f}:
      (fh, nb) = ie
      match fh:
    case E{n, z, a, b, False{}, d, f}:
      waiting_not_granted(g)

def wng.if(+e: E, +r: Bound, b: Bool, ie: Idle(e), ih: {r == BGranted{} : Bound} -> Empty, g: {find.if(e, r, b) == BGranted{} : Bound}) -> Empty:
  match b:
    case True{}:
      idle_not_granted(e, ie, g)
    case False{}:
      ih(g)

# no waiting ticket is ever granted a bind
def waiters_not_granted(+t: +List<E>, +n: Nat, w: Waiters(t), g: {find(n, t) == BGranted{} : Bound}) -> Empty:
  match t:
    case Nil{}:
      gone_not_granted(g)
    case Con{e, r}:
      (ie, wr) = w
      wng.if(e, find(n, r), same(n, num(e)), ie, h => waiters_not_granted(r, n, wr, h), g)

def add_head(+p: Nat, +e: E, +t: +List<E>, w: Waiters(t)) -> TurnOk(tell(apply(AddPid{p}, e) <> t)):
  match e:
    case E{m, z, a, x, h, d, f}:
      (Unit{}, w)

def add_ok.if(+n: Nat, +p: Nat, +e: E, +t: +List<E>, b: Bool, he: IsT(held(e)), w: Waiters(t), g: {find.if(e, find(n, t), b) == BGranted{} : Bound}) -> TurnOk(tell(edit.if(AddPid{p}, e, t, edit(n, AddPid{p}, t), b))):
  match b:
    case True{}:
      add_head(p, e, t, w)
    case False{}:
      absurd(TurnOk(tell(edit.if(AddPid{p}, e, t, edit(n, AddPid{p}, t), False{}))), waiters_not_granted(t, n, w, g))

def add_ok(+q: +List<E>, +n: Nat, +p: Nat, ok: TurnOk(q), g: {find(n, q) == BGranted{} : Bound}) -> TurnOk(tell(edit(n, AddPid{p}, q))):
  match q:
    case Nil{}:
      absurd(TurnOk(tell(edit(n, AddPid{p}, []))), gone_not_granted(g))
    case Con{e, t}:
      (he, w) = ok
      add_ok.if(n, p, e, t, same(n, num(e)), he, w, g)

def bind_ok(+q: +List<E>, +n: Nat, +p: Nat, +v: Bound, ok: TurnOk(q), g: {find(n, q) == v : Bound}) -> TurnOk(bind.if(n, p, q, v)):
  match v:
    case BGranted{}:
      add_ok(q, n, p, ok, g)
    case BGone{}:
      ok
    case BWaiting{}:
      ok
    case BFenced{}:
      ok

def bind_chain(+q: +List<E>, +n: Nat, +p: Nat, +v: Bound, +top: Nat, ch: Chain(0n, q, top)) -> Chain(0n, bind.if(n, p, q, v), top):
  match v:
    case BGranted{}:
      chain_tell(0n, edit(n, AddPid{p}, q), top, chain_edit(q, 0n, n, AddPid{p}, top, ch))
    case BGone{}:
      ch
    case BWaiting{}:
      ch
    case BFenced{}:
      ch

# --- every input keeps both invariants -------------------------------------------

def inv_ask.wb(+q: +List<E>, +c: Nat, +v: Nat, +p: +List<P>, +x: Nat, +u: Bool, ok: TurnOk(q), ch: Chain(0n, q, c)) -> Inv(ask.wb(q, c, v, p, x, u)):
  match u:
    case True{}:
      (pre_tell(snoc(q, E{1n+c, x, True{}, [], False{}, True{}, False{}}), pre_snoc(q, E{1n+c, x, True{}, [], False{}, True{}, False{}}, ok, (Unit{}, Unit{}))), chain_tell(0n, snoc(q, E{1n+c, x, True{}, [], False{}, True{}, False{}}), 1n+c, chain_snoc(q, 0n, c, x, True{}, [], False{}, True{}, False{}, ch)))
    case False{}:
      (ok, ch)

# an ask keeps them whether it is numbered or parked
def inv_ask.w(+q: +List<E>, +c: Nat, +w: Nat, +p: +List<P>, +x: Nat, +d: Bool, ok: TurnOk(q), ch: Chain(0n, q, c)) -> Inv(ask.w(q, c, w, p, x, d)):
  match w:
    case 0n:
      (pre_tell(snoc(q, E{1n+c, x, True{}, [], False{}, d, False{}}), pre_snoc(q, E{1n+c, x, True{}, [], False{}, d, False{}}, ok, (Unit{}, Unit{}))), chain_tell(0n, snoc(q, E{1n+c, x, True{}, [], False{}, d, False{}}), 1n+c, chain_snoc(q, 0n, c, x, True{}, [], False{}, d, False{}, ch)))
    case 1n+v:
      match d:
        case False{}:
          (ok, ch)
        case True{}:
          inv_ask.wb(q, c, v, p, x, has_unbounded(q), ok, ch)

def inv_ask(+q: +List<E>, +c: Nat, +f: Nat, +w: Nat, +p: +List<P>, +x: Nat, +d: Bool, ok: TurnOk(q), ch: Chain(0n, q, c)) -> Inv(ask(q, c, f, w, p, x, d)):
  match f:
    case 0n:
      inv_ask.w(q, c, w, p, x, d, ok, ch)
    case 1n+g:
      (ok, ch)

def inv_ask.in(+q: +List<E>, +c: Nat, +f: Nat, +w: Nat, +p: +List<P>, +x: Nat, +d: Bool, +i: Bool, ok: TurnOk(q), ch: Chain(0n, q, c)) -> Inv(ask.in(q, c, f, w, p, x, d, i)):
  match i:
    case True{}:
      (pre_tell(snoc(q, E{1n+c, x, True{}, [], False{}, d, False{}}), pre_snoc(q, E{1n+c, x, True{}, [], False{}, d, False{}}, ok, (Unit{}, Unit{}))), chain_tell(0n, snoc(q, E{1n+c, x, True{}, [], False{}, d, False{}}), 1n+c, chain_snoc(q, 0n, c, x, True{}, [], False{}, d, False{}, ch)))
    case False{}:
      inv_ask(q, c, f, w, p, x, d, ok, ch)

# a wait and a wake keep the queue as it is
def inv_wait.if(+q: +List<E>, +c: Nat, +f: Nat, +w: Nat, +p: +List<P>, +r: Bool, ok: TurnOk(q), ch: Chain(0n, q, c)) -> Inv(wait.if(q, c, f, w, p, r)):
  match r:
    case True{}:
      (ok, ch)
    case False{}:
      (ok, ch)

def inv_wake.if(+q: +List<E>, +c: Nat, +f: Nat, +w: Nat, +p: +List<P>, +r: Bool, ok: TurnOk(q), ch: Chain(0n, q, c)) -> Inv(wake.if(q, c, f, w, p, r)):
  match r:
    case True{}:
      (ok, ch)
    case False{}:
      (ok, ch)

# the parked asks enter the queue as numbered asks do
def inv_admit(+p: +List<P>, +q: +List<E>, +c: Nat, ok: TurnOk(q), ch: Chain(0n, q, c)) -> Inv(admit(p, q, c)):
  match p:
    case Nil{}:
      (ok, ch)
    case Con{P{+x, +d}, r}:
      inv_admit(r, tell(snoc(q, E{1n+c, x, True{}, [], False{}, d, False{}})), 1n+c, pre_tell(snoc(q, E{1n+c, x, True{}, [], False{}, d, False{}}), pre_snoc(q, E{1n+c, x, True{}, [], False{}, d, False{}}, ok, (Unit{}, Unit{}))), chain_tell(0n, snoc(q, E{1n+c, x, True{}, [], False{}, d, False{}}), 1n+c, chain_snoc(q, 0n, c, x, True{}, [], False{}, d, False{}, ch)))

def inv_lift.w(+q: +List<E>, +c: Nat, +w: Nat, +p: +List<P>, ok: TurnOk(q), ch: Chain(0n, q, c)) -> Inv(lift.w(q, c, w, p)):
  match w:
    case 0n:
      inv_admit(p, unfence_all(q), c, turn_ok_unfence_all(q, ok), chain_unfence_all(q, 0n, c, ch))
    case 1n+v:
      (ok, ch)

def inv_lift(+q: +List<E>, +c: Nat, +f: Nat, +w: Nat, +p: +List<P>, ok: TurnOk(q), ch: Chain(0n, q, c)) -> Inv(lift(q, c, f, w, p)):
  match f:
    case 0n:
      inv_lift.w(q, c, w, p, ok, ch)
    case 1n+g:
      (ok, ch)

def inv_step(+o: Op, +s: St, i: Inv(s)) -> Inv(step(o, s)):
  match o s:
    case TAsk{x, d} St{q, c, f, w, p}:
      (ok, ch) = i
      inv_ask.in(q, c, f, w, p, x, d, inside(x, q), ok, ch)
    case TBind{n, p} St{q, c, f, w, k}:
      (ok, ch) = i
      (bind_ok(q, n, p, find(n, q), ok, {==}), bind_chain(q, n, p, find(n, q), c, ch))
    case TExit{n, p} St{q, c, f, w, k}:
      (ok, ch) = i
      inv_wake.if(settle(n, edit(n, DropPid{p}, q)), c, f, w, k, all_fenced(settle(n, edit(n, DropPid{p}, q))), pre_tell(sweep(n, edit(n, DropPid{p}, q)), pre_sweep(edit(n, DropPid{p}, q), n, pre_edit(q, n, DropPid{p}, Unit{}, ok))), chain_tell(0n, sweep(n, edit(n, DropPid{p}, q)), c, chain_sweep(edit(n, DropPid{p}, q), 0n, n, c, chain_edit(q, 0n, n, DropPid{p}, c, ch))))
    case TLeave{n} St{q, c, f, w, p}:
      (ok, ch) = i
      inv_wake.if(settle(n, edit(n, Depart{}, q)), c, f, w, p, all_fenced(settle(n, edit(n, Depart{}, q))), pre_tell(sweep(n, edit(n, Depart{}, q)), pre_sweep(edit(n, Depart{}, q), n, pre_edit(q, n, Depart{}, Unit{}, ok))), chain_tell(0n, sweep(n, edit(n, Depart{}, q)), c, chain_sweep(edit(n, Depart{}, q), 0n, n, c, chain_edit(q, 0n, n, Depart{}, c, ch))))
    case TFence{} St{q, c, f, w, p}:
      (ok, ch) = i
      (ok, ch)
    case TUnfence{} St{q, c, f, w, p}:
      (ok, ch) = i
      inv_lift(q, c, less(f), w, p, ok, ch)
    case TWait{} St{q, c, f, w, p}:
      (ok, ch) = i
      inv_wait.if(q, c, f, w, p, all_fenced(q), ok, ch)
    case TWaitLeave{} St{q, c, f, w, p}:
      (ok, ch) = i
      inv_lift(q, c, f, less(w), p, ok, ch)
    case TFenced{n} St{q, c, f, w, p}:
      (ok, ch) = i
      inv_wake.if(edit(n, MarkFenced{}, q), c, f, w, p, all_fenced(edit(n, MarkFenced{}, q)), turn_ok_edit(q, n, MarkFenced{}, Unit{}, ok), chain_edit(q, 0n, n, MarkFenced{}, c, ch))
    case TResume{n} St{q, c, f, w, p}:
      (ok, ch) = i
      (turn_ok_edit(q, n, UnfenceOne{}, Unit{}, ok), chain_edit(q, 0n, n, UnfenceOne{}, c, ch))

def inv_run(+os: +List<Op>, +s: St, i: Inv(s)) -> Inv(run(os, s)):
  match os:
    case Nil{}:
      i
    case Con{o, t}:
      inv_run(t, step(o, s), inv_step(o, s, i))

def inv_empty() -> Inv(empty()):
  (Unit{}, Unit{})

def turn_of(+s: St, i: Inv(s)) -> TurnOk(queue(s)):
  match s:
    case St{q, c, f, w, p}:
      (ok, ch) = i
      ok

def chain_of(+s: St, i: Inv(s)) -> Chain(0n, queue(s), counter(s)):
  match s:
    case St{q, c, f, w, p}:
      (ok, ch) = i
      ch

def nums(q: +List<E>) -> +List<Nat>:
  match q:
    case Nil{}:
      []
    case Con{e, t}:
      num(e) <> nums(t)

def snocn(xs: +List<Nat>, +y: Nat) -> +List<Nat>:
  match xs:
    case Nil{}:
      [y]
    case Con{h, t}:
      h <> snocn(t, y)

def nums_tell(+q: +List<E>) -> {nums(tell(q)) == nums(q) : +List<Nat>}:
  match q:
    case Nil{}:
      {==}
    case Con{E{n, z, a, b, h, d, f}, t}:
      {==}

def nums_unfence_all(+q: +List<E>) -> {nums(unfence_all(q)) == nums(q) : +List<Nat>}:
  match q:
    case Nil{}:
      {==}
    case Con{E{n, z, a, b, h, d, f}, t}:
      %nums_unfence_all(t) : {n <> nums(unfence_all(t)) == n <> _ : +List<Nat>}
      {==}

def nums_snoc(+q: +List<E>, +x: E) -> {nums(snoc(q, x)) == snocn(nums(q), num(x)) : +List<Nat>}:
  match q:
    case Nil{}:
      {==}
    case Con{e, t}:
      %nums_snoc(t, x) : {num(e) <> nums(snoc(t, x)) == num(e) <> _ : +List<Nat>}
      {==}

# the numbers the parked asks `p` are given past the last number `c`, each put
# after those before it
def arrived(p: +List<P>, xs: +List<Nat>, +c: Nat) -> +List<Nat>:
  match p:
    case Nil{}:
      xs
    case Con{P{x, d}, r}:
      arrived(r, snocn(xs, 1n+c), 1n+c)

def nums_admit(+p: +List<P>, +q: +List<E>, +c: Nat) -> {nums(queue(admit(p, q, c))) == arrived(p, nums(q), c) : +List<Nat>}:
  match p:
    case Nil{}:
      {==}
    case Con{P{+x, +d}, r}:
      %nums_snoc(q, E{1n+c, x, True{}, [], False{}, d, False{}}) : {nums(queue(admit(r, tell(snoc(q, E{1n+c, x, True{}, [], False{}, d, False{}})), 1n+c))) == arrived(r, _, 1n+c) : +List<Nat>}
      %nums_tell(snoc(q, E{1n+c, x, True{}, [], False{}, d, False{}})) : {nums(queue(admit(r, tell(snoc(q, E{1n+c, x, True{}, [], False{}, d, False{}})), 1n+c))) == arrived(r, _, 1n+c) : +List<Nat>}
      nums_admit(r, tell(snoc(q, E{1n+c, x, True{}, [], False{}, d, False{}})), 1n+c)

def nf_unfence_all(+q: +List<E>) -> {none_fenced(unfence_all(q)) == True{} : Bool}:
  match q:
    case Nil{}:
      {==}
    case Con{E{n, z, a, b, h, d, f}, t}:
      nf_unfence_all(t)

def nf_tell(+q: +List<E>, h: {none_fenced(q) == True{} : Bool}) -> {none_fenced(tell(q)) == True{} : Bool}:
  match q:
    case Nil{}:
      {==}
    case Con{E{n, z, a, b, hd, d, f}, t}:
      h

def nf_snoc(+q: +List<E>, +n: Nat, +z: Nat, +a: Bool, +b: +List<Nat>, +hd: Bool, +d: Bool, h: {none_fenced(q) == True{} : Bool}) -> {none_fenced(snoc(q, E{n, z, a, b, hd, d, False{}})) == True{} : Bool}:
  match q:
    case Nil{}:
      {==}
    case Con{E{m, zz, aa, bb, hh, dd, False{}}, t}:
      nf_snoc(t, n, z, a, b, hd, d, h)
    case Con{E{m, zz, aa, bb, hh, dd, True{}}, t}:
      %h : {and(False{}, none_fenced(snoc(t, E{n, z, a, b, hd, d, False{}}))) == _ : Bool}
      {==}

def nf_admit(+p: +List<P>, +q: +List<E>, +c: Nat, h: {none_fenced(q) == True{} : Bool}) -> {none_fenced(queue(admit(p, q, c))) == True{} : Bool}:
  match p:
    case Nil{}:
      h
    case Con{P{+x, +d}, r}:
      nf_admit(r, tell(snoc(q, E{1n+c, x, True{}, [], False{}, d, False{}})), 1n+c, nf_tell(snoc(q, E{1n+c, x, True{}, [], False{}, d, False{}}), nf_snoc(q, 1n+c, x, True{}, [], False{}, d, h)))

# --- laws -------------------------------------------------------------------------

# One holder, and the turn passes on at once: after any inputs, the first ticket
# holds the turn and every other ticket waits with no command bound to it. So no
# command runs outside its turn, and a queue with a ticket in it is never idle.
law one_holder:
  for +os: +List<Op>
  TurnOk(queue(run(os, empty())))

# FIFO, no barging: after any inputs, ticket numbers rise strictly from the holder
# to the last waiter and none exceeds the last number given. Every ticket ahead of
# a land asked before it, so a land waits only for the holds of lands asked earlier.
law fifo:
  for +os: +List<Op>
  Chain(0n, queue(run(os, empty())), counter(run(os, empty())))

# A bind of a waiting ticket changes nothing.
law waiting_bind_refused:
  for +q: +List<E>
  for +c: Nat
  for +f: Nat
  for +w: Nat
  for +k: +List<P>
  for +n: Nat
  for +p: Nat
  for g: {find(n, q) == BWaiting{} : Bound}
  {step(TBind{n, p}, St{q, c, f, w, k}) == St{q, c, f, w, k} : St}

# A bind of a fenced ticket changes nothing until it resumes or unfences.
law fenced_bind_refused:
  for +q: +List<E>
  for +c: Nat
  for +f: Nat
  for +w: Nat
  for +k: +List<P>
  for +n: Nat
  for +p: Nat
  for g: {find(n, q) == BFenced{} : Bound}
  {step(TBind{n, p}, St{q, c, f, w, k}) == St{q, c, f, w, k} : St}

# Definedness: an ask with no install waiting and no fence held enters its queue
# last, numbered one past the last number, whoever asks it and whatever its class.
law ask_enters:
  for +x: Nat
  for +d: Bool
  for +q: +List<E>
  for +c: Nat
  for +p: +List<P>
  {nums(queue(step(TAsk{x, d}, St{q, c, 0n, 0n, p}))) == snocn(nums(q), 1n+c) : +List<Nat>}

# A fenced ask is parked: an ask from outside every unfenced held turn that
# meets a fence changes no queue, numbers nothing and keeps the fence
# (05_gateway.md "The read fences the turns").
law fenced_ask_parked:
  for +x: Nat
  for +d: Bool
  for +q: +List<E>
  for +c: Nat
  for +f: Nat
  for +w: Nat
  for +p: +List<P>
  for g: {inside(x, q) == False{} : Bool}
  {step(TAsk{x, d}, St{q, c, 1n+f, w, p}) == St{q, c, 1n+f, w, push_p(p, P{x, d})} : St}

# A non-bounded ask from outside every unfenced held turn that meets a waiting
# install is parked: it changes no queue and numbers nothing, so no land from
# outside lengthens the install's wait.
law waiting_install_numbers_no_ask:
  for +x: Nat
  for +q: +List<E>
  for +c: Nat
  for +f: Nat
  for +w: Nat
  for +p: +List<P>
  for g: {inside(x, q) == False{} : Bool}
  {step(TAsk{x, False{}}, St{q, c, f, 1n+w, p}) == St{q, c, f, 1n+w, push_p(p, P{x, False{}})} : St}

# While an install waits with no fence held and some non-bounded ticket
# (unfenced or fenced) remains queued, a bounded ask from outside enters the
# queue last, numbered one past the last number given, without disturbing the
# waiting install or parked asks.
law waiting_install_numbers_a_bounded_ask_behind_a_land:
  for +x: Nat
  for +q: +List<E>
  for +c: Nat
  for +w: Nat
  for +p: +List<P>
  for g: {inside(x, q) == False{} : Bool}
  for u: {has_unbounded(q) == True{} : Bool}
  {step(TAsk{x, True{}}, St{q, c, 0n, 1n+w, p}) == St{tell(snoc(q, E{1n+c, x, True{}, [], False{}, True{}, False{}})), 1n+c, 0n, 1n+w, p} : St}

# Once no non-bounded ticket remains in the queue while an install waits, no ask
# from outside — bounded or non-bounded — is numbered before the install fences,
# so the install's remaining wait past the last land is bounded by the bounded
# tickets already queued.
law install_delay_is_one_bound:
  for +x: Nat
  for +d: Bool
  for +q: +List<E>
  for +c: Nat
  for +f: Nat
  for +w: Nat
  for +p: +List<P>
  for g: {inside(x, q) == False{} : Bool}
  for u: {has_unbounded(q) == False{} : Bool}
  {step(TAsk{x, d}, St{q, c, f, 1n+w, p}) == St{q, c, f, 1n+w, push_p(p, P{x, d})} : St}

# An ask made inside an unfenced held turn is numbered whatever installs wait
# and whatever fences hold: it enters its queue last, and the waiting installs,
# the fences and the parked asks stay as they were.
law held_turn_ask_numbered:
  for +x: Nat
  for +d: Bool
  for +q: +List<E>
  for +c: Nat
  for +f: Nat
  for +w: Nat
  for +p: +List<P>
  for g: {inside(x, q) == True{} : Bool}
  {step(TAsk{x, d}, St{q, c, f, w, p}) == St{tell(snoc(q, E{1n+c, x, True{}, [], False{}, d, False{}})), 1n+c, f, w, p} : St}

# The asker of an unfenced ticket that holds the turn asks from inside it until
# it fences or ends its hold, whatever else is queued ...
law the_holder_asks_inside_its_turn:
  for +x: Nat
  for +n: Nat
  for +b: +List<Nat>
  for +d: Bool
  for +t: +List<E>
  {inside(x, E{n, x, True{}, b, True{}, d, False{}} <> t) == True{} : Bool}

# ... and so does a command bound to an unfenced turn.
law a_bound_command_asks_inside_its_turn:
  for +x: Nat
  for +n: Nat
  for +z: Nat
  for +a: Bool
  for +b: +List<Nat>
  for +h: Bool
  for +d: Bool
  for +t: +List<E>
  {inside(x, E{n, z, a, x <> b, h, d, False{}} <> t) == True{} : Bool}

# A fenced ticket asks from outside.
law a_fenced_ticket_asks_from_outside:
  for +x: Nat
  for +n: Nat
  for +z: Nat
  for +a: Bool
  for +b: +List<Nat>
  for +h: Bool
  for +d: Bool
  {within(x, E{n, z, a, b, h, d, True{}}) == False{} : Bool}

# Recorded 2026-11-16: a land held its turn, the install of the land before it
# waited for that turn, and the land's ask for its ref turn was parked behind
# the install for 24 minutes, each waiting for the other, until the land was
# ended by hand. As the queue meets it now: the ask inside the unfenced turn is
# numbered, the land ends both turns, and that step fences the install; a land
# that asked meanwhile from outside is parked behind it.
law a_land_takes_its_ref_turn_under_a_waiting_install:
  for +b: Bool
  {run([TAsk{7n, False{}}, TWait{}, TAsk{8n, False{}}, TAsk{7n, False{}}, TLeave{2n}, TLeave{1n}], empty()) == St{[], 2n, 1n, 0n, [P{8n, False{}}]} : St}

# The step that leaves every live ticket fenced — the last live asker's TFenced
# — fences every waiting install at once, keeping every fenced ticket in q.
law install_fenced_when_all_turns_fence:
  for +n: Nat
  for +q: +List<E>
  for +c: Nat
  for +f: Nat
  for +w: Nat
  for +p: +List<P>
  for g: {all_fenced(edit(n, MarkFenced{}, q)) == True{} : Bool}
  {step(TFenced{n}, St{q, c, f, 1n+w, p}) == St{edit(n, MarkFenced{}, q), c, 1n+plus(w, f), 0n, p} : St}

# Ending the last unfenced ticket (including emptying the queue) also fences
# every waiting install: a departure or a bound command's exit. With
# waiting_install_numbers_no_ask, install_delay_is_one_bound and
# install_fenced_when_all_turns_fence, an install waits at most one heartbeat
# for resumable tickets to fence (and at most one bound past the last legacy
# non-bounded ticket for any bounded tickets admitted behind it).
law install_fenced_when_queue_empties:
  for +n: Nat
  for +q: +List<E>
  for +c: Nat
  for +f: Nat
  for +w: Nat
  for +p: +List<P>
  for g: {settle(n, edit(n, Depart{}, q)) == [] : +List<E>}
  {step(TLeave{n}, St{q, c, f, 1n+w, p}) == St{[], c, 1n+plus(w, f), 0n, p} : St}

law install_fenced_when_the_last_command_exits:
  for +n: Nat
  for +x: Nat
  for +q: +List<E>
  for +c: Nat
  for +f: Nat
  for +w: Nat
  for +p: +List<P>
  for g: {settle(n, edit(n, DropPid{x}, q)) == [] : +List<E>}
  {step(TExit{n, x}, St{q, c, f, 1n+w, p}) == St{[], c, 1n+plus(w, f), 0n, p} : St}

# On the lift of the last fence, with no install waiting, the parked asks are
# numbered in the order they arrived, each one past the last, and enter the queue
# behind every ticket already in it.
law parked_asks_enter_in_order:
  for +q: +List<E>
  for +c: Nat
  for +p: +List<P>
  {nums(queue(step(TUnfence{}, St{q, c, 1n, 0n, p}))) == arrived(p, nums(q), c) : +List<Nat>}

# Lifting the last fence without a restart clears the fenced mark on every
# ticket in the queue.
law unfence_clears_fenced:
  for +q: +List<E>
  for +c: Nat
  for +p: +List<P>
  {none_fenced(queue(step(TUnfence{}, St{q, c, 1n, 0n, p}))) == True{} : Bool}

# A fenced read sets a fence only when every live ticket is fenced: one that
# finds an unfenced ticket changes nothing.
law fence_only_when_all_fenced:
  for +q: +List<E>
  for +c: Nat
  for +f: Nat
  for +w: Nat
  for +p: +List<P>
  for g: {all_fenced(q) == False{} : Bool}
  {step(TFence{}, St{q, c, f, w, p}) == St{q, c, f, w, p} : St}

# Definedness: a read that finds every live ticket fenced fences the queue.
law fenced_read_fences:
  for +q: +List<E>
  for +c: Nat
  for +f: Nat
  for +w: Nat
  for +p: +List<P>
  for g: {all_fenced(q) == True{} : Bool}
  {fences(step(TFence{}, St{q, c, f, w, p})) == 1n+f : Nat}

# Definedness: a read that finds no ticket fences the queue.
law free_read_fences:
  for +c: Nat
  for +f: Nat
  for +w: Nat
  for +p: +List<P>
  {fences(step(TFence{}, St{[], c, f, w, p})) == 1n+f : Nat}

# The install of land 124380eca9, 2026-11-10, as a reading install met it: it
# read the turns, found an unfenced ticket and deferred, and a land asked before
# its next read, three times over; no read ever fenced (land_turn_open.bend).
law a_reading_install_is_overtaken_by_every_later_ask:
  for +b: Bool
  {fences(run([TAsk{1n, False{}}, TFence{}, TAsk{2n, False{}}, TLeave{1n}, TFence{}, TAsk{3n, False{}}, TLeave{2n}, TFence{}], empty())) == 0n : Nat}

# ... and as the waiting install meets it when the ticket ends ...
law a_waiting_install_is_fenced_after_the_ticket_before_it:
  for +b: Bool
  {run([TAsk{1n, False{}}, TWait{}, TAsk{2n, False{}}, TLeave{1n}, TAsk{3n, False{}}], empty()) == St{[], 1n, 1n, 0n, [P{2n, False{}}, P{3n, False{}}]} : St}

# ... and under resumable turns: the held ticket acknowledges TFenced, fencing
# the waiting install immediately while the ticket stays in the queue.
law a_held_turn_fences_a_waiting_install_without_ending:
  for +b: Bool
  {run([TAsk{1n, False{}}, TWait{}, TAsk{2n, False{}}, TFenced{1n}, TAsk{3n, False{}}], empty()) == St{[E{1n, 1n, True{}, [], True{}, False{}, True{}}], 1n, 1n, 0n, [P{2n, False{}}, P{3n, False{}}]} : St}

def one_holder(os):
  turn_of(run(os, empty()), inv_run(os, empty(), inv_empty()))

def fifo(os):
  chain_of(run(os, empty()), inv_run(os, empty(), inv_empty()))

def waiting_bind_refused(q, c, f, w, k, n, p, g):
  %Equal.sym(Bound, find(n, q), BWaiting{}, g) : {St{bind.if(n, p, q, _), c, f, w, k} == St{q, c, f, w, k} : St}
  {==}

def fenced_bind_refused(q, c, f, w, k, n, p, g):
  %Equal.sym(Bound, find(n, q), BFenced{}, g) : {St{bind.if(n, p, q, _), c, f, w, k} == St{q, c, f, w, k} : St}
  {==}

def ask_enters.in(+x: Nat, +d: Bool, +q: +List<E>, +c: Nat, +p: +List<P>, +i: Bool) -> {nums(queue(ask.in(q, c, 0n, 0n, p, x, d, i))) == snocn(nums(q), 1n+c) : +List<Nat>}:
  match i:
    case True{}:
      %nums_snoc(q, E{1n+c, x, True{}, [], False{}, d, False{}}) : {nums(tell(snoc(q, E{1n+c, x, True{}, [], False{}, d, False{}}))) == _ : +List<Nat>}
      nums_tell(snoc(q, E{1n+c, x, True{}, [], False{}, d, False{}}))
    case False{}:
      %nums_snoc(q, E{1n+c, x, True{}, [], False{}, d, False{}}) : {nums(tell(snoc(q, E{1n+c, x, True{}, [], False{}, d, False{}}))) == _ : +List<Nat>}
      nums_tell(snoc(q, E{1n+c, x, True{}, [], False{}, d, False{}}))

def ask_enters(x, d, q, c, p):
  ask_enters.in(x, d, q, c, p, inside(x, q))

def fenced_ask_parked(x, d, q, c, f, w, p, g):
  %Equal.sym(Bool, inside(x, q), False{}, g) : {ask.in(q, c, 1n+f, w, p, x, d, _) == St{q, c, 1n+f, w, push_p(p, P{x, d})} : St}
  {==}

def waiting_install_numbers_no_ask.f(+x: Nat, +q: +List<E>, +c: Nat, +f: Nat, +w: Nat, +p: +List<P>) -> {ask(q, c, f, 1n+w, p, x, False{}) == St{q, c, f, 1n+w, push_p(p, P{x, False{}})} : St}:
  match f:
    case 0n:
      {==}
    case 1n+g:
      {==}

def waiting_install_numbers_no_ask(x, q, c, f, w, p, g):
  %Equal.sym(Bool, inside(x, q), False{}, g) : {ask.in(q, c, f, 1n+w, p, x, False{}, _) == St{q, c, f, 1n+w, push_p(p, P{x, False{}})} : St}
  waiting_install_numbers_no_ask.f(x, q, c, f, w, p)

def waiting_install_numbers_a_bounded_ask_behind_a_land(x, q, c, w, p, g, u):
  %Equal.sym(Bool, inside(x, q), False{}, g) : {ask.in(q, c, 0n, 1n+w, p, x, True{}, _) == St{tell(snoc(q, E{1n+c, x, True{}, [], False{}, True{}, False{}})), 1n+c, 0n, 1n+w, p} : St}
  %Equal.sym(Bool, has_unbounded(q), True{}, u) : {ask.wb(q, c, w, p, x, _) == St{tell(snoc(q, E{1n+c, x, True{}, [], False{}, True{}, False{}})), 1n+c, 0n, 1n+w, p} : St}
  {==}

def install_delay_is_one_bound.d(+x: Nat, +d: Bool, +q: +List<E>, +c: Nat, +w: Nat, +p: +List<P>, u: {has_unbounded(q) == False{} : Bool}) -> {ask.w(q, c, 1n+w, p, x, d) == St{q, c, 0n, 1n+w, push_p(p, P{x, d})} : St}:
  match d:
    case False{}:
      {==}
    case True{}:
      %Equal.sym(Bool, has_unbounded(q), False{}, u) : {ask.wb(q, c, w, p, x, _) == St{q, c, 0n, 1n+w, push_p(p, P{x, True{}})} : St}
      {==}

def install_delay_is_one_bound.f(+x: Nat, +d: Bool, +q: +List<E>, +c: Nat, +f: Nat, +w: Nat, +p: +List<P>, u: {has_unbounded(q) == False{} : Bool}) -> {ask(q, c, f, 1n+w, p, x, d) == St{q, c, f, 1n+w, push_p(p, P{x, d})} : St}:
  match f:
    case 0n:
      install_delay_is_one_bound.d(x, d, q, c, w, p, u)
    case 1n+g:
      {==}

def install_delay_is_one_bound(x, d, q, c, f, w, p, g, u):
  %Equal.sym(Bool, inside(x, q), False{}, g) : {ask.in(q, c, f, 1n+w, p, x, d, _) == St{q, c, f, 1n+w, push_p(p, P{x, d})} : St}
  install_delay_is_one_bound.f(x, d, q, c, f, w, p, u)

def held_turn_ask_numbered(x, d, q, c, f, w, p, g):
  %Equal.sym(Bool, inside(x, q), True{}, g) : {ask.in(q, c, f, w, p, x, d, _) == St{tell(snoc(q, E{1n+c, x, True{}, [], False{}, d, False{}})), 1n+c, f, w, p} : St}
  {==}

def same_refl(+x: Nat) -> {same(x, x) == True{} : Bool}:
  match x:
    case 0n:
      {==}
    case 1n+y:
      same_refl(y)

def or_true(+a: Bool) -> {or(a, True{}) == True{} : Bool}:
  match a:
    case True{}:
      {==}
    case False{}:
      {==}

def the_holder_asks_inside_its_turn(x, n, b, d, t):
  %Equal.sym(Bool, same(x, x), True{}, same_refl(x)) : {or(or(mem(x, b), mine.if(True{}, True{}, _)), inside(x, t)) == True{} : Bool}
  %Equal.sym(Bool, or(mem(x, b), True{}), True{}, or_true(mem(x, b))) : {or(_, inside(x, t)) == True{} : Bool}
  {==}

def a_bound_command_asks_inside_its_turn(x, n, z, a, b, h, d, t):
  %Equal.sym(Bool, same(x, x), True{}, same_refl(x)) : {or(or(or(_, mem(x, b)), mine.if(a, h, same(x, z))), inside(x, t)) == True{} : Bool}
  {==}

def a_fenced_ticket_asks_from_outside(x, n, z, a, b, h, d):
  {==}

def a_land_takes_its_ref_turn_under_a_waiting_install(b):
  {==}

def install_fenced_when_all_turns_fence(n, q, c, f, w, p, g):
  %Equal.sym(Bool, all_fenced(edit(n, MarkFenced{}, q)), True{}, g) : {wake.if(edit(n, MarkFenced{}, q), c, f, 1n+w, p, _) == St{edit(n, MarkFenced{}, q), c, 1n+plus(w, f), 0n, p} : St}
  {==}

def install_fenced_when_queue_empties(n, q, c, f, w, p, g):
  %Equal.sym(+List<E>, settle(n, edit(n, Depart{}, q)), [], g) : {wake(_, c, f, 1n+w, p) == St{[], c, 1n+plus(w, f), 0n, p} : St}
  {==}

def install_fenced_when_the_last_command_exits(n, x, q, c, f, w, p, g):
  %Equal.sym(+List<E>, settle(n, edit(n, DropPid{x}, q)), [], g) : {wake(_, c, f, 1n+w, p) == St{[], c, 1n+plus(w, f), 0n, p} : St}
  {==}

def parked_asks_enter_in_order(q, c, p):
  %nums_unfence_all(q) : {nums(queue(admit(p, unfence_all(q), c))) == arrived(p, _, c) : +List<Nat>}
  nums_admit(p, unfence_all(q), c)

def unfence_clears_fenced(q, c, p):
  nf_admit(p, unfence_all(q), c, nf_unfence_all(q))

def fence_only_when_all_fenced(q, c, f, w, p, g):
  %Equal.sym(Bool, all_fenced(q), False{}, g) : {St{q, c, fence.if(f, _), w, p} == St{q, c, f, w, p} : St}
  {==}

def fenced_read_fences(q, c, f, w, p, g):
  %Equal.sym(Bool, all_fenced(q), True{}, g) : {fence.if(f, _) == 1n+f : Nat}
  {==}

def free_read_fences(c, f, w, p):
  {==}

def a_reading_install_is_overtaken_by_every_later_ask(b):
  {==}

def a_waiting_install_is_fenced_after_the_ticket_before_it(b):
  {==}

def a_held_turn_fences_a_waiting_install_without_ending(b):
  {==}

# --- the laws as checks a driver evaluates on the code's own state ----------------

def le_b(a: Nat, b: Nat) -> Bool:
  match a b:
    case 0n b0:
      True{}
    case 1n+a1 0n:
      False{}
    case 1n+a1 1n+b1:
      le_b(a1, b1)

def idle_b(e: E) -> Bool:
  match e:
    case E{n, z, a, b, h, d, f}:
      and(not(h), is_nil(b))

def waiters_b(q: +List<E>) -> Bool:
  match q:
    case Nil{}:
      True{}
    case Con{e, t}:
      and(idle_b(e), waiters_b(t))

def turn_ok_b(q: +List<E>) -> Bool:
  match q:
    case Nil{}:
      True{}
    case Con{e, t}:
      and(held(e), waiters_b(t))

def chain_b(q: +List<E>, +lo: Nat, +top: Nat) -> Bool:
  match q:
    case Nil{}:
      le_b(lo, top)
    case Con{+e, t}:
      and(le_b(1n+lo, num(e)), chain_b(t, num(e), top))

def le_refl_b(+a: Nat, +b: Nat, h: LE(a, b)) -> {le_b(a, b) == True{} : Bool}:
  match a b:
    case 0n b0:
      {==}
    case 1n+a1 0n:
      match h:
    case 1n+a1 1n+b1:
      le_refl_b(a1, b1, h)

def waiters_refl(+q: +List<E>, w: Waiters(q)) -> {waiters_b(q) == True{} : Bool}:
  match q:
    case Nil{}:
      {==}
    case Con{E{n, z, a, Nil{}, False{}, d, f}, t}:
      (ie, wt) = w
      waiters_refl(t, wt)
    case Con{E{n, z, a, Con{x, xs}, False{}, d, f}, t}:
      ((fh, nb), wt) = w
      match nb:
    case Con{E{n, z, a, b, True{}, d, f}, t}:
      ((fh, nb), wt) = w
      match fh:

def turn_refl(+q: +List<E>, ok: TurnOk(q)) -> {turn_ok_b(q) == True{} : Bool}:
  match q:
    case Nil{}:
      {==}
    case Con{E{n, z, a, b, True{}, d, f}, t}:
      (ht, w) = ok
      waiters_refl(t, w)
    case Con{E{n, z, a, b, False{}, d, f}, t}:
      (ht, w) = ok
      match ht:

def chain_refl(+q: +List<E>, +lo: Nat, +top: Nat, ch: Chain(lo, q, top)) -> {chain_b(q, lo, top) == True{} : Bool}:
  match q:
    case Nil{}:
      le_refl_b(lo, top, ch)
    case Con{e, t}:
      (lt, rest) = ch
      %Equal.sym(Bool, le_b(1n+lo, num(e)), True{}, le_refl_b(1n+lo, num(e), lt)) : {and(_, chain_b(t, num(e), top)) == True{} : Bool}
      chain_refl(t, num(e), top, rest)

law one_holder_check:
  for +os: +List<Op>
  {turn_ok_b(queue(run(os, empty()))) == True{} : Bool}

law fifo_check:
  for +os: +List<Op>
  {chain_b(queue(run(os, empty())), 0n, counter(run(os, empty()))) == True{} : Bool}

def one_holder_check(os):
  turn_refl(queue(run(os, empty())), one_holder(os))

def fifo_check(os):
  chain_refl(queue(run(os, empty())), 0n, counter(run(os, empty())), fifo(os))
