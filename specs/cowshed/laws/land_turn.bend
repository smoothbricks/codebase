# Land turns (specs/cowshed/05_gateway.md "Land turns", contract "Land-turn
# laws"): one repository's queue as the gateway's pure core `Turns` keeps it
# (cowshed-gateway src/land_turn.rs). A ticket is numbered as its ask arrives
# and takes its place at the end; the first ticket holds the turn; a turn is
# held by its asker and by every command bound to it, and passes on at once
# when the last of them has gone; only a held turn binds a command.
# Repositories' queues are independent (tickets are numbered globally and found
# by number), so one queue is the model.
#
# An install's read of the turns asked with the fence (05_gateway.md "The read
# fences the turns") fences every queue when it finds no ticket: `TFence` is
# that read, `TUnfence` its connection's close, and `fences` how many such
# reads are open. An ask under a fence is refused and never numbered.
#
# Check: bun bend2/main.ts land_turn.bend --verdict
# Bound to the code by cowshed-gateway src/land_turn/laws.rs, which runs the
# emitted model (src/land_turn/laws/model.rs) and `Turns` on the same inputs.
import Base

type E is Data:
  E{num: Nat, asker: Bool, bound: +List<Nat>, held: Bool}

type Op is Data:
  TAsk{}
  TBind{num: Nat, pid: Nat}
  TExit{num: Nat, pid: Nat}
  TLeave{num: Nat}
  TFence{}
  TUnfence{}

type Bound is Data:
  BGranted{}
  BGone{}
  BWaiting{}

type St is Data:
  St{q: +List<E>, numbered: Nat, fences: Nat}

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
    case E{n, a, b, h}:
      n

def held(e: E) -> Bool:
  match e:
    case E{n, a, b, h}:
      h

# nothing holds the ticket: neither its asker nor a bound command
def done(e: E) -> Bool:
  match e:
    case E{n, a, b, h}:
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
    case Con{E{n, a, b, h}, t}:
      E{n, a, b, True{}} <> t

# what one input does to the ticket it names
type Edit is Data:
  AddPid{pid: Nat}
  DropPid{pid: Nat}
  Depart{}

def apply(k: Edit, e: E) -> E:
  match k e:
    case AddPid{p} E{n, a, +b, h}:
      E{n, a, add(p, b), h}
    case DropPid{p} E{n, a, +b, h}:
      E{n, a, remove(p, b), h}
    case Depart{} E{n, a, b, h}:
      E{n, False{}, b, h}

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

def find.held(h: Bool) -> Bound:
  match h:
    case True{}:
      BGranted{}
    case False{}:
      BWaiting{}

def find.if(+e: E, r: Bound, b: Bool) -> Bound:
  match b:
    case True{}:
      find.held(held(e))
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
    case TBind{n, p} St{+q, c, f}:
      find(n, q)
    case TAsk{} St{q, c, f}:
      BGranted{}
    case TExit{n, p} St{q, c, f}:
      BGranted{}
    case TLeave{n} St{q, c, f}:
      BGranted{}
    case TFence{} St{q, c, f}:
      BGranted{}
    case TUnfence{} St{q, c, f}:
      BGranted{}

def bind.if(+n: Nat, +p: Nat, +q: +List<E>, v: Bound) -> +List<E>:
  match v:
    case BGranted{}:
      tell(edit(n, AddPid{p}, q))
    case BGone{}:
      q
    case BWaiting{}:
      q

# an ask under a fence is refused and changes nothing (land_turn.rs `ask`)
def ask(+q: +List<E>, +c: Nat, f: Nat) -> St:
  match f:
    case 0n:
      St{tell(snoc(q, E{1n+c, True{}, [], False{}})), 1n+c, 0n}
    case 1n+g:
      St{q, c, 1n+g}

# a read fences only an empty queue (land_turn.rs `fence`)
def fence(q: +List<E>, f: Nat) -> Nat:
  match q:
    case Nil{}:
      1n+f
    case Con{e, t}:
      f

# a fencing read's connection closed (land_turn.rs `unfence`)
def unfence(f: Nat) -> Nat:
  match f:
    case 0n:
      0n
    case 1n+g:
      g

def step(o: Op, s: St) -> St:
  match o s:
    case TAsk{} St{+q, +c, f}:
      ask(q, c, f)
    case TBind{+n, +p} St{+q, c, f}:
      St{bind.if(n, p, q, find(n, q)), c, f}
    case TExit{+n, +p} St{+q, c, f}:
      St{settle(n, edit(n, DropPid{p}, q)), c, f}
    case TLeave{+n} St{+q, c, f}:
      St{settle(n, edit(n, Depart{}, q)), c, f}
    case TFence{} St{+q, c, f}:
      St{q, c, fence(q, f)}
    case TUnfence{} St{q, c, f}:
      St{q, c, unfence(f)}

def run(os: +List<Op>, s: St) -> St:
  match os:
    case Nil{}:
      s
    case Con{o, t}:
      run(t, step(o, s))

def empty() -> St:
  St{[], 0n, 0n}

def queue(s: St) -> +List<E>:
  match s:
    case St{q, c, f}:
      q

def counter(s: St) -> Nat:
  match s:
    case St{q, c, f}:
      c

def fences(s: St) -> Nat:
  match s:
    case St{q, c, f}:
      f
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
    case E{n, a, b, h}:
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
    case St{+q, +c, f}:
      Sigma<&2, &2, TurnOk(q), u => Chain(0n, q, c)>
# --- FIFO is kept ---------------------------------------------------------------

def chain_weaken(+lo: Nat, +n: Nat, +t: +List<E>, +top: Nat, le: LE(lo, n), ch: Chain(n, t, top)) -> Chain(lo, t, top):
  match t:
    case Nil{}:
      le_trans(lo, n, top, le, ch)
    case Con{e, r}:
      (lt, rest) = ch
      (le_trans(1n+lo, 1n+n, num(e), le, lt), rest)

def chain_snoc(+q: +List<E>, +lo: Nat, +top: Nat, +a: Bool, +b: +List<Nat>, +h: Bool, ch: Chain(lo, q, top)) -> Chain(lo, snoc(q, E{1n+top, a, b, h}), 1n+top):
  match q:
    case Nil{}:
      (ch, le_refl(1n+top))
    case Con{e, t}:
      (lt, rest) = ch
      (lt, chain_snoc(t, num(e), top, a, b, h, rest))

def chain_tell(+lo: Nat, +q: +List<E>, +top: Nat, ch: Chain(lo, q, top)) -> Chain(lo, tell(q), top):
  match q:
    case Nil{}:
      ch
    case Con{E{n, a, b, h}, t}:
      ch

def chain_apply(+lo: Nat, +k: Edit, +e: E, +t: +List<E>, +top: Nat, ch: Chain(lo, e <> t, top)) -> Chain(lo, apply(k, e) <> t, top):
  match k e:
    case AddPid{p} E{n, a, b, h}:
      ch
    case DropPid{p} E{n, a, b, h}:
      ch
    case Depart{} E{n, a, b, h}:
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
    case Con{E{n, a, b, h}, t}:
      (Unit{}, p)

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

# DropPid and Depart are what an exit and a departure do; AddPid is a bind
def NotAdd(k: Edit) -> Data:
  match k:
    case AddPid{p}:
      Empty
    case DropPid{p}:
      Unit
    case Depart{}:
      Unit

def idle_apply(+k: Edit, +e: E, nk: NotAdd(k), ie: Idle(e)) -> Idle(apply(k, e)):
  match k e:
    case AddPid{p} E{n, a, b, h}:
      match nk:
    case DropPid{p} E{n, a, Nil{}, h}:
      ie
    case DropPid{p} E{n, a, Con{x, xs}, h}:
      (fh, nb) = ie
      match nb:
    case Depart{} E{n, a, b, h}:
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
    case AddPid{p} E{n, a, x, h}:
      wt
    case DropPid{p} E{n, a, x, h}:
      wt
    case Depart{} E{n, a, x, h}:
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

# --- a bind is granted only to the ticket that holds the turn --------------------

def g_disc(v: Bound) -> Data:
  match v:
    case BGranted{}:
      Empty
    case BGone{}:
      Unit
    case BWaiting{}:
      Unit

def gone_not_granted(g: {BGone{} == BGranted{} : Bound}) -> Empty:
  %g : g_disc(_)
  Unit{}

def waiting_not_granted(g: {BWaiting{} == BGranted{} : Bound}) -> Empty:
  %g : g_disc(_)
  Unit{}

def idle_not_granted(+e: E, ie: Idle(e), g: {find.held(held(e)) == BGranted{} : Bound}) -> Empty:
  match e:
    case E{n, a, b, True{}}:
      (fh, nb) = ie
      match fh:
    case E{n, a, b, False{}}:
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
    case E{m, a, x, h}:
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

def bind_chain(+q: +List<E>, +n: Nat, +p: Nat, +v: Bound, +top: Nat, ch: Chain(0n, q, top)) -> Chain(0n, bind.if(n, p, q, v), top):
  match v:
    case BGranted{}:
      chain_tell(0n, edit(n, AddPid{p}, q), top, chain_edit(q, 0n, n, AddPid{p}, top, ch))
    case BGone{}:
      ch
    case BWaiting{}:
      ch

# --- every input keeps both invariants -------------------------------------------

# an ask keeps them whether it is numbered or fenced
def inv_ask(+q: +List<E>, +c: Nat, +f: Nat, i: Inv(St{q, c, f})) -> Inv(ask(q, c, f)):
  match f:
    case 0n:
      (ok, ch) = i
      (pre_tell(snoc(q, E{1n+c, True{}, [], False{}}), pre_snoc(q, E{1n+c, True{}, [], False{}}, ok, (Unit{}, Unit{}))),
       chain_tell(0n, snoc(q, E{1n+c, True{}, [], False{}}), 1n+c, chain_snoc(q, 0n, c, True{}, [], False{}, ch)))
    case 1n+g:
      i

def inv_step(+o: Op, +s: St, i: Inv(s)) -> Inv(step(o, s)):
  match o s:
    case TAsk{} St{q, c, f}:
      inv_ask(q, c, f, i)
    case TBind{n, p} St{q, c, f}:
      (ok, ch) = i
      (bind_ok(q, n, p, find(n, q), ok, {==}), bind_chain(q, n, p, find(n, q), c, ch))
    case TExit{n, p} St{q, c, f}:
      (ok, ch) = i
      (pre_tell(sweep(n, edit(n, DropPid{p}, q)), pre_sweep(edit(n, DropPid{p}, q), n, pre_edit(q, n, DropPid{p}, Unit{}, ok))),
       chain_tell(0n, sweep(n, edit(n, DropPid{p}, q)), c, chain_sweep(edit(n, DropPid{p}, q), 0n, n, c, chain_edit(q, 0n, n, DropPid{p}, c, ch))))
    case TLeave{n} St{q, c, f}:
      (ok, ch) = i
      (pre_tell(sweep(n, edit(n, Depart{}, q)), pre_sweep(edit(n, Depart{}, q), n, pre_edit(q, n, Depart{}, Unit{}, ok))),
       chain_tell(0n, sweep(n, edit(n, Depart{}, q)), c, chain_sweep(edit(n, Depart{}, q), 0n, n, c, chain_edit(q, 0n, n, Depart{}, c, ch))))
    case TFence{} St{q, c, f}:
      (ok, ch) = i
      (ok, ch)
    case TUnfence{} St{q, c, f}:
      (ok, ch) = i
      (ok, ch)

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
    case St{q, c, f}:
      (ok, ch) = i
      ok

def chain_of(+s: St, i: Inv(s)) -> Chain(0n, queue(s), counter(s)):
  match s:
    case St{q, c, f}:
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
    case Con{E{n, a, b, h}, t}:
      {==}

def nums_snoc(+q: +List<E>, +x: E) -> {nums(snoc(q, x)) == snocn(nums(q), num(x)) : +List<Nat>}:
  match q:
    case Nil{}:
      {==}
    case Con{e, t}:
      %nums_snoc(t, x) : {num(e) <> nums(snoc(t, x)) == num(e) <> _ : +List<Nat>}
      {==}

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
  for +n: Nat
  for +p: Nat
  for g: {find(n, q) == BWaiting{} : Bound}
  {step(TBind{n, p}, St{q, c, f}) == St{q, c, f} : St}

# Definedness: an unfenced ask enters its queue last, numbered one past the last
# number.
law ask_enters:
  for +q: +List<E>
  for +c: Nat
  {nums(queue(step(TAsk{}, St{q, c, 0n}))) == snocn(nums(q), 1n+c) : +List<Nat>}

# A fenced ask is refused: it changes no queue, numbers nothing and keeps the
# fence (05_gateway.md "The read fences the turns").
law fenced_ask_refused:
  for +q: +List<E>
  for +c: Nat
  for +f: Nat
  {step(TAsk{}, St{q, c, 1n+f}) == St{q, c, 1n+f} : St}

# A fence is set only on an empty queue: a read that finds a ticket, whose
# install refuses, changes nothing.
law fence_only_when_free:
  for +e: E
  for +t: +List<E>
  for +c: Nat
  for +f: Nat
  {step(TFence{}, St{e <> t, c, f}) == St{e <> t, c, f} : St}

# Definedness: a read that finds no ticket fences the queue.
law free_read_fences:
  for +c: Nat
  for +f: Nat
  {fences(step(TFence{}, St{[], c, f})) == 1n+f : Nat}

def one_holder(os):
  turn_of(run(os, empty()), inv_run(os, empty(), inv_empty()))

def fifo(os):
  chain_of(run(os, empty()), inv_run(os, empty(), inv_empty()))

def waiting_bind_refused(q, c, f, n, p, g):
  %Equal.sym(Bound, find(n, q), BWaiting{}, g) : {St{bind.if(n, p, q, _), c, f} == St{q, c, f} : St}
  {==}

def ask_enters(q, c):
  %nums_snoc(q, E{1n+c, True{}, [], False{}}) : {nums(tell(snoc(q, E{1n+c, True{}, [], False{}}))) == _ : +List<Nat>}
  nums_tell(snoc(q, E{1n+c, True{}, [], False{}}))

def fenced_ask_refused(q, c, f):
  {==}

def fence_only_when_free(e, t, c, f):
  {==}

def free_read_fences(c, f):
  {==}
# --- the laws as checks a driver evaluates on the code's own state ----------------

def and(a: Bool, b: Bool) -> Bool:
  match a:
    case True{}:
      b
    case False{}:
      False{}

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
    case E{n, a, b, h}:
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
    case Con{E{n, a, Nil{}, False{}}, t}:
      (ie, wt) = w
      waiters_refl(t, wt)
    case Con{E{n, a, Con{x, xs}, False{}}, t}:
      ((fh, nb), wt) = w
      match nb:
    case Con{E{n, a, b, True{}}, t}:
      ((fh, nb), wt) = w
      match fh:

def turn_refl(+q: +List<E>, ok: TurnOk(q)) -> {turn_ok_b(q) == True{} : Bool}:
  match q:
    case Nil{}:
      {==}
    case Con{E{n, a, b, True{}}, t}:
      (ht, w) = ok
      waiters_refl(t, w)
    case Con{E{n, a, b, False{}}, t}:
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