# Restart safety across an install and a land (specs/cowshed/05_gateway.md "An
# install refuses while a land runs", "A land step waits for an install in
# progress", "The install slot", "Land turns"): every interleaving of one
# install and any number of lands, with the gateway's land-turn queue, the
# install slot and the workspace supervisors' admitted gate jobs.
#
# The install (tooling/land-install.sh, then cowshed-cli gateway_service.rs
# `start_service` or setup's refresh):
#   InstallBuild  land-install builds the release CLI. Today's design
#                 (turn=False): readiness, no held turn, is judged first and
#                 the build runs in a shed forked at the head, asking no turn
#                                                     IRUnbuilt -> IRFree | IRRefused
#                 The old design (turn=True): the build asks a land turn of
#                 its own, holds it, then ends it  IRUnbuilt -> IRBuilding -> IRFree
#   TakeSlot      exclusive flock on install.lock     IRFree -> IRHeld
#   ReadBlockers  agent_cycle `restart_blockers` and `refusal` over the
#                 gateway's `land-turns`: refused while a queue has a holder
#                                                     IRHeld -> IRRefused | IRClear
#   Restart       activation: the old gateway drains, its control socket and
#                 every land-turn connection close    IRClear -> IRRestarted
#   ReleaseSlot   the new gateway serves              IRRestarted -> IRDone
# A land (run.rs `is_land_step`, declared_land.rs `land`, land_turn.rs):
#   WaitSlot  install_basis `wait_for_install`: waits while an install holds
#             the slot, then holds nothing           absent -> LRReady
#   AskTurn   core land_turn `take`: a ticket at the end of the queue; with no
#             gateway serving the ask fails the land  LRReady -> LRAsked | LRLost
#   BindGate  the holder (first ticket) binds its gate, which a workspace
#             supervisor admits                      LRAsked -> LRGate
#   End       the gate ends and the land ends its hold  LRGate -> LRDone
#   a land whose turn's connection closed fails; one with a bound gate sends
#   it SIGTERM (declared_land.rs `gate`)            LRDropped|LROrphan -> LRLost
#
# `fenced` is the proposed repair: the read of the blockers fences the
# gateway's turns, and an ask the fence meets waits for the install slot and
# asks again (the land goes back to WaitSlot). The code today is fenced=False.
# One repository's queue is the model: an install is refused by any queue's
# holder, so other queues only add refusals.
#
# Check: bun bend2/main.ts install_restart.bend --verdict
# Bound to the code by cowshed-cli src/agent_cycle/laws.rs (`restart_blockers`,
# `refusal` against `blockers`, `refuses`).
import Base

type IR is Data:
  IRUnbuilt{turn: Bool}
  IRBuilding{}
  IRFree{}
  IRHeld{}
  IRClear{}
  IRRestarted{}
  IRDone{}
  IRRefused{}

type LR is Data:
  LRAbsent{}
  LRReady{}
  LRAsked{}
  LRGate{}
  LRDone{}
  LRDropped{}
  LROrphan{}
  LRLost{}

type Ld is Data:
  Ld{id: Nat, ph: LR}

# a ticket: the land that asked, and whether it asked before the blockers were read
type Tk is Data:
  Tk{id: Nat, early: Bool}
  TkI{}

type RO is Data:
  ROInstall{}
  ROLand{id: Nat}

# one repository's queue as `land-turns` lists it: its ticket numbers in order
type RQ is Data:
  RQ{repo: Nat, tickets: +List<Nat>}

# cuts: restarts that closed a held turn; early: those whose queue held a ticket
# asked before the read; kills: admitted gates sent SIGTERM for a lost turn;
# refused: asks that met no gateway
type Ty is Data:
  Ty{cuts: Nat, early: Nat, kills: Nat, refused: Nat}

type RS is Data:
  RS{ph: IR, q: +List<Tk>, lands: +List<Ld>, jobs: +List<Nat>, ty: Ty, fenced: Bool}

# --- helpers --------------------------------------------------------------------

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

def addb(b: Bool, n: Nat) -> Nat:
  match b:
    case True{}:
      1n+n
    case False{}:
      n

def is_nil(xs: +List<Nat>) -> Bool:
  match xs:
    case Nil{}:
      True{}
    case Con{h, t}:
      False{}

def snoc_n(xs: +List<Nat>, +x: Nat) -> +List<Nat>:
  match xs:
    case Nil{}:
      [x]
    case Con{h, t}:
      h <> snoc_n(t, x)

def remove_n.if(h: Nat, r: +List<Nat>, b: Bool) -> +List<Nat>:
  match b:
    case True{}:
      r
    case False{}:
      h <> r

def remove_n(+x: Nat, xs: +List<Nat>) -> +List<Nat>:
  match xs:
    case Nil{}:
      []
    case Con{+h, t}:
      remove_n.if(h, remove_n(x, t), same(x, h))

# --- the blocker judgement (agent_cycle.rs `restart_blockers`, `refusal`) --------

def blockers.q(ts: +List<Nat>, rest: +List<Nat>) -> +List<Nat>:
  match ts:
    case Nil{}:
      rest
    case Con{n, u}:
      n <> rest

# the holder of each repository's turn: the first ticket of each queue
def blockers(qs: +List<RQ>) -> +List<Nat>:
  match qs:
    case Nil{}:
      []
    case Con{RQ{r, ts}, t}:
      blockers.q(ts, blockers(t))

# an install refuses while anything blocks it
def refuses(+qs: +List<RQ>) -> Bool:
  not(is_nil(blockers(qs)))

# --- the queue --------------------------------------------------------------------

# a ticket's number as `land-turns` lists it; the install build's own ticket is
# listed like any other
def tk_id(t: Tk) -> Nat:
  match t:
    case Tk{i, e}:
      i
    case TkI{}:
      0n

# the ticket is land x's (never the install build's)
def names(+x: Nat, t: Tk) -> Bool:
  match t:
    case Tk{i, e}:
      same(x, i)
    case TkI{}:
      False{}

def ids(q: +List<Tk>) -> +List<Nat>:
  match q:
    case Nil{}:
      []
    case Con{h, t}:
      tk_id(h) <> ids(t)

def is_nil_t(q: +List<Tk>) -> Bool:
  match q:
    case Nil{}:
      True{}
    case Con{h, t}:
      False{}

def any_early(q: +List<Tk>) -> Bool:
  match q:
    case Nil{}:
      False{}
    case Con{Tk{i, e}, t}:
      or(e, any_early(t))
    case Con{TkI{}, t}:
      any_early(t)

def mem_t(+x: Nat, q: +List<Tk>) -> Bool:
  match q:
    case Nil{}:
      False{}
    case Con{h, t}:
      or(names(x, h), mem_t(x, t))

def snoc_t(q: +List<Tk>, +x: Tk) -> +List<Tk>:
  match q:
    case Nil{}:
      [x]
    case Con{h, t}:
      h <> snoc_t(t, x)

def remove_t.if(h: Tk, r: +List<Tk>, b: Bool) -> +List<Tk>:
  match b:
    case True{}:
      r
    case False{}:
      h <> r

def remove_t(+x: Nat, q: +List<Tk>) -> +List<Tk>:
  match q:
    case Nil{}:
      []
    case Con{+h, t}:
      remove_t.if(h, remove_t(x, t), names(x, h))

# the first ticket holds the turn (land_turn.bend `one_holder`)
def first_is(+x: Nat, q: +List<Tk>) -> Bool:
  match q:
    case Nil{}:
      False{}
    case Con{h, t}:
      names(x, h)

def is_tki(t: Tk) -> Bool:
  match t:
    case Tk{i, e}:
      False{}
    case TkI{}:
      True{}

# the install build holds the turn
def build_holds(q: +List<Tk>) -> Bool:
  match q:
    case Nil{}:
      False{}
    case Con{h, t}:
      is_tki(h)

def remove_i(q: +List<Tk>) -> +List<Tk>:
  match q:
    case Nil{}:
      []
    case Con{Tk{i, e}, t}:
      Tk{i, e} <> remove_i(t)
    case Con{TkI{}, t}:
      remove_i(t)

def has_land(q: +List<Tk>) -> Bool:
  match q:
    case Nil{}:
      False{}
    case Con{Tk{i, e}, t}:
      True{}
    case Con{TkI{}, t}:
      has_land(t)

# a land waits behind the install build's ticket
def waits_on_install(q: +List<Tk>) -> Bool:
  match q:
    case Nil{}:
      False{}
    case Con{Tk{i, e}, t}:
      waits_on_install(t)
    case Con{TkI{}, t}:
      has_land(t)

# every ticket is a land's
def lands_only(q: +List<Tk>) -> Bool:
  match q:
    case Nil{}:
      True{}
    case Con{Tk{i, e}, t}:
      lands_only(t)
    case Con{TkI{}, t}:
      False{}

# --- the lands --------------------------------------------------------------------

def ld_id(l: Ld) -> Nat:
  match l:
    case Ld{i, p}:
      i

def ld_ph(l: Ld) -> LR:
  match l:
    case Ld{i, p}:
      p

def look.if(p: LR, r: LR, b: Bool) -> LR:
  match b:
    case True{}:
      p
    case False{}:
      r

# a land not yet seen has taken no step
def look(+x: Nat, ls: +List<Ld>) -> LR:
  match ls:
    case Nil{}:
      LRAbsent{}
    case Con{+h, t}:
      look.if(ld_ph(h), look(x, t), same(x, ld_id(h)))

def put.if(+x: Nat, +p: LR, +h: Ld, t: +List<Ld>, r: +List<Ld>, b: Bool) -> +List<Ld>:
  match b:
    case True{}:
      Ld{x, p} <> t
    case False{}:
      h <> r

def put(+x: Nat, +p: LR, ls: +List<Ld>) -> +List<Ld>:
  match ls:
    case Nil{}:
      [Ld{x, p}]
    case Con{+h, +t}:
      put.if(x, p, h, t, put(x, p, t), same(x, ld_id(h)))

# what a closed turn connection leaves of a land
def lose.ph(p: LR) -> LR:
  match p:
    case LRAbsent{}:
      LRAbsent{}
    case LRReady{}:
      LRReady{}
    case LRAsked{}:
      LRDropped{}
    case LRGate{}:
      LROrphan{}
    case LRDone{}:
      LRDone{}
    case LRDropped{}:
      LRDropped{}
    case LROrphan{}:
      LROrphan{}
    case LRLost{}:
      LRLost{}

def lose.if(b: Bool, p: LR) -> LR:
  match b:
    case True{}:
      lose.ph(p)
    case False{}:
      p

# a restart closes the connection of every land with a ticket
def lose(ls: +List<Ld>, +q: +List<Tk>) -> +List<Ld>:
  match ls:
    case Nil{}:
      []
    case Con{Ld{+i, p}, t}:
      Ld{i, lose.if(mem_t(i, q), p)} <> lose(t, q)

# --- the install ------------------------------------------------------------------

def slot_held(p: IR) -> Bool:
  match p:
    case IRUnbuilt{b}:
      False{}
    case IRBuilding{}:
      False{}
    case IRFree{}:
      False{}
    case IRHeld{}:
      True{}
    case IRClear{}:
      True{}
    case IRRestarted{}:
      True{}
    case IRDone{}:
      False{}
    case IRRefused{}:
      False{}

# an ask now comes before the install's read of the blockers
def before_read(p: IR) -> Bool:
  match p:
    case IRUnbuilt{b}:
      True{}
    case IRBuilding{}:
      True{}
    case IRFree{}:
      True{}
    case IRHeld{}:
      True{}
    case IRClear{}:
      False{}
    case IRRestarted{}:
      False{}
    case IRDone{}:
      False{}
    case IRRefused{}:
      False{}

# a gateway answers a turn ask (the restarted one serves once the slot is released)
def serving(p: IR) -> Bool:
  match p:
    case IRUnbuilt{b}:
      True{}
    case IRBuilding{}:
      True{}
    case IRFree{}:
      True{}
    case IRHeld{}:
      True{}
    case IRClear{}:
      True{}
    case IRRestarted{}:
      False{}
    case IRDone{}:
      True{}
    case IRRefused{}:
      True{}

def fenced_now(p: IR, f: Bool) -> Bool:
  match p:
    case IRUnbuilt{b}:
      False{}
    case IRBuilding{}:
      False{}
    case IRFree{}:
      False{}
    case IRHeld{}:
      False{}
    case IRClear{}:
      f
    case IRRestarted{}:
      f
    case IRDone{}:
      False{}
    case IRRefused{}:
      False{}

def ty_cut(+q: +List<Tk>, y: Ty) -> Ty:
  match y:
    case Ty{c, e, k, r}:
      Ty{addb(not(is_nil_t(q)), c), addb(any_early(q), e), k, r}

def ty_kill(y: Ty) -> Ty:
  match y:
    case Ty{c, e, k, r}:
      Ty{c, e, 1n+k, r}

def ty_refuse(y: Ty) -> Ty:
  match y:
    case Ty{c, e, k, r}:
      Ty{c, e, k, 1n+r}

# Restart: the queue is gone with the gateway; supervisors keep their jobs
def restart(s: RS) -> RS:
  match s:
    case RS{p, +q, ls, js, y, f}:
      RS{IRRestarted{}, [], lose(ls, q), js, ty_cut(q, y), f}

def read.if(b: Bool, q: +List<Tk>, ls: +List<Ld>, js: +List<Nat>, y: Ty, f: Bool) -> RS:
  match b:
    case True{}:
      RS{IRRefused{}, q, ls, js, y, f}
    case False{}:
      RS{IRClear{}, q, ls, js, y, f}

# the new land-install: readiness is judged before the build, which runs in a
# shed of its own and asks no turn
def built.if(b: Bool, q: +List<Tk>, ls: +List<Ld>, js: +List<Nat>, y: Ty, f: Bool) -> RS:
  match b:
    case True{}:
      RS{IRRefused{}, q, ls, js, y, f}
    case False{}:
      RS{IRFree{}, q, ls, js, y, f}

# the old land-install: the build holds a land turn of its own, then ends it
def turn_built.if(b: Bool, q: +List<Tk>, ls: +List<Ld>, js: +List<Nat>, y: Ty, f: Bool) -> RS:
  match b:
    case True{}:
      RS{IRFree{}, remove_i(q), ls, js, y, f}
    case False{}:
      RS{IRBuilding{}, q, ls, js, y, f}

def istep(s: RS) -> RS:
  match s:
    case RS{IRUnbuilt{False{}}, +q, ls, js, y, f}:
      built.if(refuses([RQ{0n, ids(q)}]), q, ls, js, y, f)
    case RS{IRUnbuilt{True{}}, q, ls, js, y, f}:
      RS{IRBuilding{}, snoc_t(q, TkI{}), ls, js, y, f}
    case RS{IRBuilding{}, +q, ls, js, y, f}:
      turn_built.if(build_holds(q), q, ls, js, y, f)
    case RS{IRFree{}, q, ls, js, y, f}:
      RS{IRHeld{}, q, ls, js, y, f}
    case RS{IRHeld{}, +q, ls, js, y, f}:
      read.if(refuses([RQ{0n, ids(q)}]), q, ls, js, y, f)
    case RS{IRClear{}, q, ls, js, y, f}:
      restart(RS{IRClear{}, q, ls, js, y, f})
    case RS{IRRestarted{}, q, ls, js, y, f}:
      RS{IRDone{}, q, ls, js, y, f}
    case RS{IRDone{}, q, ls, js, y, f}:
      RS{IRDone{}, q, ls, js, y, f}
    case RS{IRRefused{}, q, ls, js, y, f}:
      RS{IRRefused{}, q, ls, js, y, f}

# --- a land's step ----------------------------------------------------------------

def wait.if(+x: Nat, b: Bool, p: IR, q: +List<Tk>, ls: +List<Ld>, js: +List<Nat>, y: Ty, f: Bool) -> RS:
  match b:
    case True{}:
      RS{p, q, ls, js, y, f}
    case False{}:
      RS{p, q, put(x, LRReady{}, ls), js, y, f}

def ask.b(+x: Nat, b: Bool, +p: IR, q: +List<Tk>, ls: +List<Ld>, js: +List<Nat>, y: Ty, f: Bool) -> RS:
  match b:
    case True{}:
      RS{p, snoc_t(q, Tk{x, before_read(p)}), put(x, LRAsked{}, ls), js, y, f}
    case False{}:
      RS{p, q, put(x, LRLost{}, ls), js, ty_refuse(y), f}

def ask.a(+x: Nat, b: Bool, +p: IR, q: +List<Tk>, ls: +List<Ld>, js: +List<Nat>, y: Ty, f: Bool) -> RS:
  match b:
    case True{}:
      RS{p, q, put(x, LRAbsent{}, ls), js, y, f}
    case False{}:
      ask.b(x, serving(p), p, q, ls, js, y, f)

def bind.if(+x: Nat, b: Bool, p: IR, q: +List<Tk>, ls: +List<Ld>, js: +List<Nat>, y: Ty, f: Bool) -> RS:
  match b:
    case True{}:
      RS{p, q, put(x, LRGate{}, ls), snoc_n(js, x), y, f}
    case False{}:
      RS{p, q, ls, js, y, f}

def lact(+x: Nat, lp: LR, s: RS) -> RS:
  match lp s:
    case LRAbsent{} RS{+p, q, ls, js, y, f}:
      wait.if(x, slot_held(p), p, q, ls, js, y, f)
    case LRReady{} RS{+p, q, ls, js, y, +f}:
      ask.a(x, fenced_now(p, f), p, q, ls, js, y, f)
    case LRAsked{} RS{p, +q, ls, js, y, f}:
      bind.if(x, first_is(x, q), p, q, ls, js, y, f)
    case LRGate{} RS{p, q, ls, js, y, f}:
      RS{p, remove_t(x, q), put(x, LRDone{}, ls), remove_n(x, js), y, f}
    case LRDone{} RS{p, q, ls, js, y, f}:
      RS{p, q, ls, js, y, f}
    case LRDropped{} RS{p, q, ls, js, y, f}:
      RS{p, q, put(x, LRLost{}, ls), js, y, f}
    case LROrphan{} RS{p, q, ls, js, y, f}:
      RS{p, q, put(x, LRLost{}, ls), remove_n(x, js), ty_kill(y), f}
    case LRLost{} RS{p, q, ls, js, y, f}:
      RS{p, q, ls, js, y, f}

def lands_of(s: RS) -> +List<Ld>:
  match s:
    case RS{p, q, ls, js, y, f}:
      ls

def step(o: RO, +s: RS) -> RS:
  match o:
    case ROInstall{}:
      istep(s)
    case ROLand{+x}:
      lact(x, look(x, lands_of(s)), s)

def run(os: +List<RO>, s: RS) -> RS:
  match os:
    case Nil{}:
      s
    case Con{o, t}:
      run(t, step(o, s))

# `turn`: the install's build asks a land turn of its own (the old land-install)
def empty(f: Bool, turn: Bool) -> RS:
  RS{IRUnbuilt{turn}, [], [], [], Ty{0n, 0n, 0n, 0n}, f}

def phase(s: RS) -> IR:
  match s:
    case RS{p, q, ls, js, y, f}:
      p

def queue(s: RS) -> +List<Tk>:
  match s:
    case RS{p, q, ls, js, y, f}:
      q

def jobs(s: RS) -> +List<Nat>:
  match s:
    case RS{p, q, ls, js, y, f}:
      js

def n_cuts(s: RS) -> Nat:
  match s:
    case RS{p, q, ls, js, Ty{c, e, k, r}, f}:
      c

def n_early(s: RS) -> Nat:
  match s:
    case RS{p, q, ls, js, Ty{c, e, k, r}, f}:
      e

def n_kills(s: RS) -> Nat:
  match s:
    case RS{p, q, ls, js, Ty{c, e, k, r}, f}:
      k

def n_refused(s: RS) -> Nat:
  match s:
    case RS{p, q, ls, js, Ty{c, e, k, r}, f}:
      r

# --- predicates -------------------------------------------------------------------

def absurd(-A: Type, e: Empty) -> A:
  match e:

def IsF(b: Bool) -> Data:
  match b:
    case True{}:
      Empty
    case False{}:
      Unit

def IsT(b: Bool) -> Data:
  match b:
    case True{}:
      Unit
    case False{}:
      Empty

def IsZ(n: Nat) -> Data:
  match n:
    case 0n:
      Unit
    case 1n+p:
      Empty

def z_eq(+n: Nat, z: IsZ(n)) -> {n == 0n : Nat}:
  match n:
    case 0n:
      {==}
    case 1n+p:
      match z:

# every ticket asked after the read
def AllLate(q: +List<Tk>) -> Data:
  match q:
    case Nil{}:
      Unit
    case Con{Tk{i, e}, t}:
      Sigma<&2, &2, IsF(e), u => AllLate(t)>
    case Con{TkI{}, t}:
      AllLate(t)

# between a clear read and the restart, no ticket was asked before the read
def Safe(p: IR, q: +List<Tk>) -> Data:
  match p:
    case IRUnbuilt{b}:
      Unit
    case IRBuilding{}:
      Unit
    case IRFree{}:
      Unit
    case IRHeld{}:
      Unit
    case IRClear{}:
      AllLate(q)
    case IRRestarted{}:
      Unit
    case IRDone{}:
      Unit
    case IRRefused{}:
      Unit

def Inv(s: RS) -> Data:
  match s:
    case RS{p, +q, ls, js, Ty{c, e, k, r}, f}:
      Sigma<&2, &2, IsZ(e), u => Safe(p, q)>

# --- tickets asked after the read stay so --------------------------------------------

def late_snoc(+q: +List<Tk>, +x: Nat, l: AllLate(q)) -> AllLate(snoc_t(q, Tk{x, False{}})):
  match q:
    case Nil{}:
      (Unit{}, Unit{})
    case Con{Tk{i, e}, t}:
      (u, r) = l
      (u, late_snoc(t, x, r))
    case Con{TkI{}, t}:
      late_snoc(t, x, l)

def late_rm.if(+i: Nat, +e: Bool, +r: +List<Tk>, b: Bool, u: IsF(e), lr: AllLate(r)) -> AllLate(remove_t.if(Tk{i, e}, r, b)):
  match b:
    case True{}:
      lr
    case False{}:
      (u, lr)

def late_remove(+q: +List<Tk>, +x: Nat, l: AllLate(q)) -> AllLate(remove_t(x, q)):
  match q:
    case Nil{}:
      Unit{}
    case Con{Tk{i, e}, t}:
      (u, r) = l
      late_rm.if(i, e, remove_t(x, t), same(x, i), u, late_remove(t, x, r))
    case Con{TkI{}, t}:
      late_remove(t, x, l)

def z_late(+q: +List<Tk>, +e: Nat, l: AllLate(q), z: IsZ(e)) -> IsZ(addb(any_early(q), e)):
  match q:
    case Nil{}:
      z
    case Con{Tk{i, True{}}, t}:
      (u, r) = l
      match u:
    case Con{Tk{i, False{}}, t}:
      (u, r) = l
      z_late(t, e, r, z)
    case Con{TkI{}, t}:
      z_late(t, e, l, z)

def safe_snoc(+p: IR, +q: +List<Tk>, +x: Nat, sq: Safe(p, q)) -> Safe(p, snoc_t(q, Tk{x, before_read(p)})):
  match p:
    case IRUnbuilt{b}:
      Unit{}
    case IRBuilding{}:
      Unit{}
    case IRFree{}:
      Unit{}
    case IRHeld{}:
      Unit{}
    case IRClear{}:
      late_snoc(q, x, sq)
    case IRRestarted{}:
      Unit{}
    case IRDone{}:
      Unit{}
    case IRRefused{}:
      Unit{}

def safe_remove(+p: IR, +q: +List<Tk>, +x: Nat, sq: Safe(p, q)) -> Safe(p, remove_t(x, q)):
  match p:
    case IRUnbuilt{b}:
      Unit{}
    case IRBuilding{}:
      Unit{}
    case IRFree{}:
      Unit{}
    case IRHeld{}:
      Unit{}
    case IRClear{}:
      late_remove(q, x, sq)
    case IRRestarted{}:
      Unit{}
    case IRDone{}:
      Unit{}
    case IRRefused{}:
      Unit{}

# --- every step keeps the invariant ---------------------------------------------------

def inv_tb(+b: Bool, +q: +List<Tk>, +ls: +List<Ld>, +js: +List<Nat>, +c: Nat, +e: Nat, +k: Nat, +r: Nat, +f: Bool, z: IsZ(e)) -> Inv(turn_built.if(b, q, ls, js, Ty{c, e, k, r}, f)):
  match b:
    case True{}:
      (z, Unit{})
    case False{}:
      (z, Unit{})

def inv_istep(+s: RS, i: Inv(s)) -> Inv(istep(s)):
  match s:
    case RS{IRUnbuilt{False{}}, Nil{}, ls, js, Ty{c, e, k, r}, f}:
      (z, sq) = i
      (z, Unit{})
    case RS{IRUnbuilt{False{}}, Con{h, t}, ls, js, Ty{c, e, k, r}, f}:
      (z, sq) = i
      (z, Unit{})
    case RS{IRUnbuilt{True{}}, q, ls, js, Ty{c, e, k, r}, f}:
      (z, sq) = i
      (z, Unit{})
    case RS{IRBuilding{}, q, ls, js, Ty{c, e, k, r}, f}:
      (z, sq) = i
      inv_tb(build_holds(q), q, ls, js, c, e, k, r, f, z)
    case RS{IRFree{}, q, ls, js, Ty{c, e, k, r}, f}:
      (z, sq) = i
      (z, Unit{})
    case RS{IRHeld{}, Nil{}, ls, js, Ty{c, e, k, r}, f}:
      (z, sq) = i
      (z, Unit{})
    case RS{IRHeld{}, Con{h, t}, ls, js, Ty{c, e, k, r}, f}:
      (z, sq) = i
      (z, Unit{})
    case RS{IRClear{}, q, ls, js, Ty{c, e, k, r}, f}:
      (z, sq) = i
      (z_late(q, e, sq, z), Unit{})
    case RS{IRRestarted{}, q, ls, js, Ty{c, e, k, r}, f}:
      (z, sq) = i
      (z, Unit{})
    case RS{IRDone{}, q, ls, js, Ty{c, e, k, r}, f}:
      i
    case RS{IRRefused{}, q, ls, js, Ty{c, e, k, r}, f}:
      i

def inv_wait(+x: Nat, +b: Bool, +p: IR, +q: +List<Tk>, +ls: +List<Ld>, +js: +List<Nat>, +c: Nat, +e: Nat, +k: Nat, +r: Nat, +f: Bool, i: Inv(RS{p, q, ls, js, Ty{c, e, k, r}, f})) -> Inv(wait.if(x, b, p, q, ls, js, Ty{c, e, k, r}, f)):
  match b:
    case True{}:
      i
    case False{}:
      i

def inv_ask_b(+x: Nat, +b: Bool, +p: IR, +q: +List<Tk>, +ls: +List<Ld>, +js: +List<Nat>, +c: Nat, +e: Nat, +k: Nat, +r: Nat, +f: Bool, i: Inv(RS{p, q, ls, js, Ty{c, e, k, r}, f})) -> Inv(ask.b(x, b, p, q, ls, js, Ty{c, e, k, r}, f)):
  match b:
    case True{}:
      (z, sq) = i
      (z, safe_snoc(p, q, x, sq))
    case False{}:
      i

def inv_ask_a(+x: Nat, +b: Bool, +p: IR, +q: +List<Tk>, +ls: +List<Ld>, +js: +List<Nat>, +c: Nat, +e: Nat, +k: Nat, +r: Nat, +f: Bool, i: Inv(RS{p, q, ls, js, Ty{c, e, k, r}, f})) -> Inv(ask.a(x, b, p, q, ls, js, Ty{c, e, k, r}, f)):
  match b:
    case True{}:
      i
    case False{}:
      inv_ask_b(x, serving(p), p, q, ls, js, c, e, k, r, f, i)

def inv_bind(+x: Nat, +b: Bool, +p: IR, +q: +List<Tk>, +ls: +List<Ld>, +js: +List<Nat>, +c: Nat, +e: Nat, +k: Nat, +r: Nat, +f: Bool, i: Inv(RS{p, q, ls, js, Ty{c, e, k, r}, f})) -> Inv(bind.if(x, b, p, q, ls, js, Ty{c, e, k, r}, f)):
  match b:
    case True{}:
      i
    case False{}:
      i

def inv_lact(+x: Nat, +lp: LR, +s: RS, i: Inv(s)) -> Inv(lact(x, lp, s)):
  match lp s:
    case LRAbsent{} RS{p, q, ls, js, Ty{c, e, k, r}, f}:
      inv_wait(x, slot_held(p), p, q, ls, js, c, e, k, r, f, i)
    case LRReady{} RS{p, q, ls, js, Ty{c, e, k, r}, f}:
      inv_ask_a(x, fenced_now(p, f), p, q, ls, js, c, e, k, r, f, i)
    case LRAsked{} RS{p, q, ls, js, Ty{c, e, k, r}, f}:
      inv_bind(x, first_is(x, q), p, q, ls, js, c, e, k, r, f, i)
    case LRGate{} RS{p, q, ls, js, Ty{c, e, k, r}, f}:
      (z, sq) = i
      (z, safe_remove(p, q, x, sq))
    case LRDone{} RS{p, q, ls, js, Ty{c, e, k, r}, f}:
      i
    case LRDropped{} RS{p, q, ls, js, Ty{c, e, k, r}, f}:
      i
    case LROrphan{} RS{p, q, ls, js, Ty{c, e, k, r}, f}:
      i
    case LRLost{} RS{p, q, ls, js, Ty{c, e, k, r}, f}:
      i

def inv_step(+o: RO, +s: RS, i: Inv(s)) -> Inv(step(o, s)):
  match o:
    case ROInstall{}:
      inv_istep(s, i)
    case ROLand{x}:
      inv_lact(x, look(x, lands_of(s)), s, i)

def inv_run(+os: +List<RO>, +s: RS, i: Inv(s)) -> Inv(run(os, s)):
  match os:
    case Nil{}:
      i
    case Con{o, t}:
      inv_run(t, step(o, s), inv_step(o, s, i))

def inv_empty(+f: Bool, +turn: Bool) -> Inv(empty(f, turn)):
  (Unit{}, Unit{})

def early_of(+s: RS, i: Inv(s)) -> IsZ(n_early(s)):
  match s:
    case RS{p, q, ls, js, Ty{c, e, k, r}, f}:
      (z, sq) = i
      z

# --- the fenced repair: the read fences the turns until the restarted gateway serves --

def IsNilT(q: +List<Tk>) -> Data:
  match q:
    case Nil{}:
      Unit
    case Con{h, t}:
      Empty

# between a clear read and the restart the queue is empty
def Clear(p: IR, q: +List<Tk>) -> Data:
  match p:
    case IRUnbuilt{b}:
      Unit
    case IRBuilding{}:
      Unit
    case IRFree{}:
      Unit
    case IRHeld{}:
      Unit
    case IRClear{}:
      IsNilT(q)
    case IRRestarted{}:
      Unit
    case IRDone{}:
      Unit
    case IRRefused{}:
      Unit

def NotOrph(p: LR) -> Data:
  match p:
    case LRAbsent{}:
      Unit
    case LRReady{}:
      Unit
    case LRAsked{}:
      Unit
    case LRGate{}:
      Unit
    case LRDone{}:
      Unit
    case LRDropped{}:
      Unit
    case LROrphan{}:
      Empty
    case LRLost{}:
      Unit

# no land has a gate whose turn a restart closed
def NoOrph(ls: +List<Ld>) -> Data:
  match ls:
    case Nil{}:
      Unit
    case Con{Ld{i, p}, t}:
      Sigma<&2, &2, NotOrph(p), u => NoOrph(t)>

def Zs(c: Nat, k: Nat, r: Nat) -> Data:
  Sigma<&2, &2, IsZ(c), u => Sigma<&2, &2, IsZ(k), v => IsZ(r)>>

def Inv2(s: RS) -> Data:
  match s:
    case RS{+p, +q, +ls, js, Ty{c, e, k, r}, +f}:
      Sigma<&2, &2, IsT(f), u => Sigma<&2, &2, Zs(c, k, r), v => Sigma<&2, &2, Clear(p, q), w => NoOrph(ls)>>>

def no_look.if(+p: LR, +r: LR, b: Bool, np: NotOrph(p), nr: NotOrph(r)) -> NotOrph(look.if(p, r, b)):
  match b:
    case True{}:
      np
    case False{}:
      nr

def no_look(+x: Nat, +ls: +List<Ld>, n: NoOrph(ls)) -> NotOrph(look(x, ls)):
  match ls:
    case Nil{}:
      Unit{}
    case Con{Ld{i, p}, t}:
      (np, nt) = n
      no_look.if(p, look(x, t), same(x, i), np, no_look(x, t, nt))

def no_put.if(+x: Nat, +p: LR, +i: Nat, +pi: LR, +t: +List<Ld>, +r: +List<Ld>, b: Bool, np: NotOrph(p), ni: NotOrph(pi), nt: NoOrph(t), nr: NoOrph(r)) -> NoOrph(put.if(x, p, Ld{i, pi}, t, r, b)):
  match b:
    case True{}:
      (np, nt)
    case False{}:
      (ni, nr)

def no_put(+x: Nat, +p: LR, +ls: +List<Ld>, +np: NotOrph(p), n: NoOrph(ls)) -> NoOrph(put(x, p, ls)):
  match ls:
    case Nil{}:
      (np, Unit{})
    case Con{Ld{i, pi}, t}:
      (ni, +nt) = n
      no_put.if(x, p, i, pi, t, put(x, p, t), same(x, i), np, ni, nt, no_put(x, p, t, np, nt))

def no_lose_nil(+ls: +List<Ld>, n: NoOrph(ls)) -> NoOrph(lose(ls, [])):
  match ls:
    case Nil{}:
      Unit{}
    case Con{Ld{i, p}, t}:
      (np, nt) = n
      (np, no_lose_nil(t, nt))

def clear_remove(+p: IR, +q: +List<Tk>, +x: Nat, cl: Clear(p, q)) -> Clear(p, remove_t(x, q)):
  match p q:
    case IRUnbuilt{b} q0:
      Unit{}
    case IRBuilding{} q0:
      Unit{}
    case IRFree{} q0:
      Unit{}
    case IRHeld{} q0:
      Unit{}
    case IRClear{} Nil{}:
      Unit{}
    case IRClear{} Con{h, t}:
      match cl:
    case IRRestarted{} q0:
      Unit{}
    case IRDone{} q0:
      Unit{}
    case IRRefused{} q0:
      Unit{}

def inv2_tb(+b: Bool, +q: +List<Tk>, +ls: +List<Ld>, +js: +List<Nat>, +c: Nat, +e: Nat, +k: Nat, +r: Nat, +f: Bool, i: Inv2(RS{IRBuilding{}, q, ls, js, Ty{c, e, k, r}, f})) -> Inv2(turn_built.if(b, q, ls, js, Ty{c, e, k, r}, f)):
  match b:
    case True{}:
      (ft, i1) = i
      (zs, i2) = i1
      (cl, no) = i2
      (ft, (zs, (Unit{}, no)))
    case False{}:
      i

def inv2_istep(+s: RS, i: Inv2(s)) -> Inv2(istep(s)):
  match s:
    case RS{IRUnbuilt{False{}}, Nil{}, ls, js, Ty{c, e, k, r}, f}:
      (ft, i1) = i
      (zs, i2) = i1
      (cl, no) = i2
      (ft, (zs, (Unit{}, no)))
    case RS{IRUnbuilt{False{}}, Con{h, t}, ls, js, Ty{c, e, k, r}, f}:
      (ft, i1) = i
      (zs, i2) = i1
      (cl, no) = i2
      (ft, (zs, (Unit{}, no)))
    case RS{IRUnbuilt{True{}}, q, ls, js, Ty{c, e, k, r}, f}:
      (ft, i1) = i
      (zs, i2) = i1
      (cl, no) = i2
      (ft, (zs, (Unit{}, no)))
    case RS{IRBuilding{}, q, ls, js, Ty{c, e, k, r}, f}:
      inv2_tb(build_holds(q), q, ls, js, c, e, k, r, f, i)
    case RS{IRFree{}, q, ls, js, Ty{c, e, k, r}, f}:
      (ft, i1) = i
      (zs, i2) = i1
      (cl, no) = i2
      (ft, (zs, (Unit{}, no)))
    case RS{IRHeld{}, Nil{}, ls, js, Ty{c, e, k, r}, f}:
      (ft, i1) = i
      (zs, i2) = i1
      (cl, no) = i2
      (ft, (zs, (Unit{}, no)))
    case RS{IRHeld{}, Con{h, t}, ls, js, Ty{c, e, k, r}, f}:
      (ft, i1) = i
      (zs, i2) = i1
      (cl, no) = i2
      (ft, (zs, (Unit{}, no)))
    case RS{IRClear{}, Nil{}, ls, js, Ty{c, e, k, r}, f}:
      (ft, i1) = i
      (zs, i2) = i1
      (cl, no) = i2
      (ft, (zs, (Unit{}, no_lose_nil(ls, no))))
    case RS{IRClear{}, Con{h, t}, ls, js, Ty{c, e, k, r}, f}:
      (ft, i1) = i
      (zs, i2) = i1
      (cl, no) = i2
      match cl:
    case RS{IRRestarted{}, q, ls, js, Ty{c, e, k, r}, f}:
      (ft, i1) = i
      (zs, i2) = i1
      (cl, no) = i2
      (ft, (zs, (Unit{}, no)))
    case RS{IRDone{}, q, ls, js, Ty{c, e, k, r}, f}:
      i
    case RS{IRRefused{}, q, ls, js, Ty{c, e, k, r}, f}:
      i

def inv2_wait(+x: Nat, +b: Bool, +p: IR, +q: +List<Tk>, +ls: +List<Ld>, +js: +List<Nat>, +c: Nat, +e: Nat, +k: Nat, +r: Nat, +f: Bool, i: Inv2(RS{p, q, ls, js, Ty{c, e, k, r}, f})) -> Inv2(wait.if(x, b, p, q, ls, js, Ty{c, e, k, r}, f)):
  match b:
    case True{}:
      i
    case False{}:
      (ft, i1) = i
      (zs, i2) = i1
      (cl, no) = i2
      (ft, (zs, (cl, no_put(x, LRReady{}, ls, Unit{}, no))))

def inv2_bind(+x: Nat, +b: Bool, +p: IR, +q: +List<Tk>, +ls: +List<Ld>, +js: +List<Nat>, +c: Nat, +e: Nat, +k: Nat, +r: Nat, +f: Bool, i: Inv2(RS{p, q, ls, js, Ty{c, e, k, r}, f})) -> Inv2(bind.if(x, b, p, q, ls, js, Ty{c, e, k, r}, f)):
  match b:
    case True{}:
      (ft, i1) = i
      (zs, i2) = i1
      (cl, no) = i2
      (ft, (zs, (cl, no_put(x, LRGate{}, ls, Unit{}, no))))
    case False{}:
      i

# an ask enters a queue only while no read is fenced; one the fence meets waits for the slot
def inv2_ready(+x: Nat, +s: RS, i: Inv2(s)) -> Inv2(lact(x, LRReady{}, s)):
  match s:
    case RS{IRUnbuilt{tb}, q, ls, js, Ty{c, e, k, r}, f}:
      (ft, i1) = i
      (zs, i2) = i1
      (cl, no) = i2
      (ft, (zs, (Unit{}, no_put(x, LRAsked{}, ls, Unit{}, no))))
    case RS{IRBuilding{}, q, ls, js, Ty{c, e, k, r}, f}:
      (ft, i1) = i
      (zs, i2) = i1
      (cl, no) = i2
      (ft, (zs, (Unit{}, no_put(x, LRAsked{}, ls, Unit{}, no))))
    case RS{IRFree{}, q, ls, js, Ty{c, e, k, r}, f}:
      (ft, i1) = i
      (zs, i2) = i1
      (cl, no) = i2
      (ft, (zs, (Unit{}, no_put(x, LRAsked{}, ls, Unit{}, no))))
    case RS{IRHeld{}, q, ls, js, Ty{c, e, k, r}, f}:
      (ft, i1) = i
      (zs, i2) = i1
      (cl, no) = i2
      (ft, (zs, (Unit{}, no_put(x, LRAsked{}, ls, Unit{}, no))))
    case RS{IRClear{}, q, ls, js, Ty{c, e, k, r}, True{}}:
      (ft, i1) = i
      (zs, i2) = i1
      (cl, no) = i2
      (ft, (zs, (cl, no_put(x, LRAbsent{}, ls, Unit{}, no))))
    case RS{IRClear{}, q, ls, js, Ty{c, e, k, r}, False{}}:
      (ft, i1) = i
      match ft:
    case RS{IRRestarted{}, q, ls, js, Ty{c, e, k, r}, True{}}:
      (ft, i1) = i
      (zs, i2) = i1
      (cl, no) = i2
      (ft, (zs, (Unit{}, no_put(x, LRAbsent{}, ls, Unit{}, no))))
    case RS{IRRestarted{}, q, ls, js, Ty{c, e, k, r}, False{}}:
      (ft, i1) = i
      match ft:
    case RS{IRDone{}, q, ls, js, Ty{c, e, k, r}, f}:
      (ft, i1) = i
      (zs, i2) = i1
      (cl, no) = i2
      (ft, (zs, (Unit{}, no_put(x, LRAsked{}, ls, Unit{}, no))))
    case RS{IRRefused{}, q, ls, js, Ty{c, e, k, r}, f}:
      (ft, i1) = i
      (zs, i2) = i1
      (cl, no) = i2
      (ft, (zs, (Unit{}, no_put(x, LRAsked{}, ls, Unit{}, no))))

def inv2_lact(+x: Nat, +lp: LR, +s: RS, nl: NotOrph(lp), i: Inv2(s)) -> Inv2(lact(x, lp, s)):
  match lp s:
    case LRAbsent{} RS{p, q, ls, js, Ty{c, e, k, r}, f}:
      inv2_wait(x, slot_held(p), p, q, ls, js, c, e, k, r, f, i)
    case LRReady{} RS{p, q, ls, js, Ty{c, e, k, r}, f}:
      inv2_ready(x, RS{p, q, ls, js, Ty{c, e, k, r}, f}, i)
    case LRAsked{} RS{p, q, ls, js, Ty{c, e, k, r}, f}:
      inv2_bind(x, first_is(x, q), p, q, ls, js, c, e, k, r, f, i)
    case LRGate{} RS{p, q, ls, js, Ty{c, e, k, r}, f}:
      (ft, i1) = i
      (zs, i2) = i1
      (cl, no) = i2
      (ft, (zs, (clear_remove(p, q, x, cl), no_put(x, LRDone{}, ls, Unit{}, no))))
    case LRDone{} RS{p, q, ls, js, Ty{c, e, k, r}, f}:
      i
    case LRDropped{} RS{p, q, ls, js, Ty{c, e, k, r}, f}:
      (ft, i1) = i
      (zs, i2) = i1
      (cl, no) = i2
      (ft, (zs, (cl, no_put(x, LRLost{}, ls, Unit{}, no))))
    case LROrphan{} RS{p, q, ls, js, Ty{c, e, k, r}, f}:
      match nl:
    case LRLost{} RS{p, q, ls, js, Ty{c, e, k, r}, f}:
      i

def noorph_of(+s: RS, i: Inv2(s)) -> NoOrph(lands_of(s)):
  match s:
    case RS{p, q, ls, js, Ty{c, e, k, r}, f}:
      (ft, i1) = i
      (zs, i2) = i1
      (cl, no) = i2
      no

def inv2_step(+o: RO, +s: RS, +i: Inv2(s)) -> Inv2(step(o, s)):
  match o:
    case ROInstall{}:
      inv2_istep(s, i)
    case ROLand{x}:
      inv2_lact(x, look(x, lands_of(s)), s, no_look(x, lands_of(s), noorph_of(s, i)), i)

def inv2_run(+os: +List<RO>, +s: RS, i: Inv2(s)) -> Inv2(run(os, s)):
  match os:
    case Nil{}:
      i
    case Con{o, t}:
      inv2_run(t, step(o, s), inv2_step(o, s, i))

def inv2_empty() -> Inv2(empty(True{}, False{})):
  (Unit{}, ((Unit{}, (Unit{}, Unit{})), (Unit{}, Unit{})))

def zs_of(+s: RS, i: Inv2(s)) -> Zs(n_cuts(s), n_kills(s), n_refused(s)):
  match s:
    case RS{p, q, ls, js, Ty{c, e, k, r}, f}:
      (ft, i1) = i
      (zs, i2) = i1
      zs

# --- the install build takes no land turn --------------------------------------------

def LandsOnly(q: +List<Tk>) -> Data:
  match q:
    case Nil{}:
      Unit
    case Con{Tk{i, e}, t}:
      LandsOnly(t)
    case Con{TkI{}, t}:
      Empty

# the build does not ask a turn
def NotTurnBuild(p: IR) -> Data:
  match p:
    case IRUnbuilt{b}:
      IsF(b)
    case IRBuilding{}:
      Empty
    case IRFree{}:
      Unit
    case IRHeld{}:
      Unit
    case IRClear{}:
      Unit
    case IRRestarted{}:
      Unit
    case IRDone{}:
      Unit
    case IRRefused{}:
      Unit

def Inv3(s: RS) -> Data:
  match s:
    case RS{+p, +q, ls, js, y, f}:
      Sigma<&2, &2, LandsOnly(q), u => NotTurnBuild(p)>

def lo_snoc(+q: +List<Tk>, +x: Nat, +e: Bool, lo: LandsOnly(q)) -> LandsOnly(snoc_t(q, Tk{x, e})):
  match q:
    case Nil{}:
      Unit{}
    case Con{Tk{i, d}, t}:
      lo_snoc(t, x, e, lo)
    case Con{TkI{}, t}:
      match lo:

def lo_rm.if(+i: Nat, +e: Bool, +r: +List<Tk>, b: Bool, lr: LandsOnly(r)) -> LandsOnly(remove_t.if(Tk{i, e}, r, b)):
  match b:
    case True{}:
      lr
    case False{}:
      lr

def lo_remove(+q: +List<Tk>, +x: Nat, lo: LandsOnly(q)) -> LandsOnly(remove_t(x, q)):
  match q:
    case Nil{}:
      Unit{}
    case Con{Tk{i, e}, t}:
      lo_rm.if(i, e, remove_t(x, t), same(x, i), lo_remove(t, x, lo))
    case Con{TkI{}, t}:
      match lo:

def inv3_istep(+s: RS, i: Inv3(s)) -> Inv3(istep(s)):
  match s:
    case RS{IRUnbuilt{False{}}, Nil{}, ls, js, y, f}:
      (Unit{}, Unit{})
    case RS{IRUnbuilt{False{}}, Con{h, t}, ls, js, y, f}:
      (lo, nb) = i
      (lo, Unit{})
    case RS{IRUnbuilt{True{}}, q, ls, js, y, f}:
      (lo, nb) = i
      match nb:
    case RS{IRBuilding{}, q, ls, js, y, f}:
      (lo, nb) = i
      match nb:
    case RS{IRFree{}, q, ls, js, y, f}:
      (lo, nb) = i
      (lo, Unit{})
    case RS{IRHeld{}, Nil{}, ls, js, y, f}:
      (Unit{}, Unit{})
    case RS{IRHeld{}, Con{h, t}, ls, js, y, f}:
      (lo, nb) = i
      (lo, Unit{})
    case RS{IRClear{}, q, ls, js, y, f}:
      (Unit{}, Unit{})
    case RS{IRRestarted{}, q, ls, js, y, f}:
      (lo, nb) = i
      (lo, Unit{})
    case RS{IRDone{}, q, ls, js, y, f}:
      i
    case RS{IRRefused{}, q, ls, js, y, f}:
      i

def inv3_wait(+x: Nat, +b: Bool, +p: IR, +q: +List<Tk>, +ls: +List<Ld>, +js: +List<Nat>, +y: Ty, +f: Bool, i: Inv3(RS{p, q, ls, js, y, f})) -> Inv3(wait.if(x, b, p, q, ls, js, y, f)):
  match b:
    case True{}:
      i
    case False{}:
      i

def inv3_ask_b(+x: Nat, +b: Bool, +p: IR, +q: +List<Tk>, +ls: +List<Ld>, +js: +List<Nat>, +y: Ty, +f: Bool, i: Inv3(RS{p, q, ls, js, y, f})) -> Inv3(ask.b(x, b, p, q, ls, js, y, f)):
  match b:
    case True{}:
      (lo, nb) = i
      (lo_snoc(q, x, before_read(p), lo), nb)
    case False{}:
      i

def inv3_ask_a(+x: Nat, +b: Bool, +p: IR, +q: +List<Tk>, +ls: +List<Ld>, +js: +List<Nat>, +y: Ty, +f: Bool, i: Inv3(RS{p, q, ls, js, y, f})) -> Inv3(ask.a(x, b, p, q, ls, js, y, f)):
  match b:
    case True{}:
      i
    case False{}:
      inv3_ask_b(x, serving(p), p, q, ls, js, y, f, i)

def inv3_bind(+x: Nat, +b: Bool, +p: IR, +q: +List<Tk>, +ls: +List<Ld>, +js: +List<Nat>, +y: Ty, +f: Bool, i: Inv3(RS{p, q, ls, js, y, f})) -> Inv3(bind.if(x, b, p, q, ls, js, y, f)):
  match b:
    case True{}:
      i
    case False{}:
      i

def inv3_lact(+x: Nat, +lp: LR, +s: RS, i: Inv3(s)) -> Inv3(lact(x, lp, s)):
  match lp s:
    case LRAbsent{} RS{p, q, ls, js, y, f}:
      inv3_wait(x, slot_held(p), p, q, ls, js, y, f, i)
    case LRReady{} RS{p, q, ls, js, y, f}:
      inv3_ask_a(x, fenced_now(p, f), p, q, ls, js, y, f, i)
    case LRAsked{} RS{p, q, ls, js, y, f}:
      inv3_bind(x, first_is(x, q), p, q, ls, js, y, f, i)
    case LRGate{} RS{p, q, ls, js, y, f}:
      (lo, nb) = i
      (lo_remove(q, x, lo), nb)
    case LRDone{} RS{p, q, ls, js, y, f}:
      i
    case LRDropped{} RS{p, q, ls, js, y, f}:
      i
    case LROrphan{} RS{p, q, ls, js, y, f}:
      i
    case LRLost{} RS{p, q, ls, js, y, f}:
      i

def inv3_step(+o: RO, +s: RS, i: Inv3(s)) -> Inv3(step(o, s)):
  match o:
    case ROInstall{}:
      inv3_istep(s, i)
    case ROLand{x}:
      inv3_lact(x, look(x, lands_of(s)), s, i)

def inv3_run(+os: +List<RO>, +s: RS, i: Inv3(s)) -> Inv3(run(os, s)):
  match os:
    case Nil{}:
      i
    case Con{o, t}:
      inv3_run(t, step(o, s), inv3_step(o, s, i))

def inv3_empty(+f: Bool) -> Inv3(empty(f, False{})):
  (Unit{}, Unit{})

def lo_of(+s: RS, i: Inv3(s)) -> LandsOnly(queue(s)):
  match s:
    case RS{p, q, ls, js, y, f}:
      (lo, nb) = i
      lo

def lo_refl(+q: +List<Tk>, lo: LandsOnly(q)) -> {lands_only(q) == True{} : Bool}:
  match q:
    case Nil{}:
      {==}
    case Con{Tk{i, e}, t}:
      lo_refl(t, lo)
    case Con{TkI{}, t}:
      match lo:

def lo_waits(+q: +List<Tk>, lo: LandsOnly(q)) -> {waits_on_install(q) == False{} : Bool}:
  match q:
    case Nil{}:
      {==}
    case Con{Tk{i, e}, t}:
      lo_waits(t, lo)
    case Con{TkI{}, t}:
      match lo:

# --- laws -------------------------------------------------------------------------

# A restart never closes a turn whose land asked before the blockers were read:
# such a ticket is still queued at the read, and the read refuses the install.
law restart_spares_turns_asked_before_the_read:
  for +f: Bool
  for +turn: Bool
  for +os: +List<RO>
  {n_early(run(os, empty(f, turn))) == 0n : Nat}

# The supervisors' admitted jobs survive a restart: it closes the gateway's
# queue and connections and touches no job.
law restart_keeps_admitted_jobs:
  for +s: RS
  {jobs(restart(s)) == jobs(s) : +List<Nat>}

# Definedness: a restart does happen, and loses every queue.
law restart_loses_the_queue:
  for +s: RS
  {queue(restart(s)) == [] : +List<Tk>}

# Definedness: an install that reads no blockers restarts the gateway.
law a_clear_read_restarts:
  for +ls: +List<Ld>
  for +js: +List<Nat>
  for +y: Ty
  for +f: Bool
  {phase(istep(istep(RS{IRHeld{}, [], ls, js, y, f}))) == IRRestarted{} : IR}

# Definedness: a held turn refuses the install, the holder named first.
law a_held_turn_refuses:
  for +r: Nat
  for +n: Nat
  for +t: +List<Nat>
  for +qs: +List<RQ>
  {blockers(RQ{r, n <> t} <> qs) == n <> blockers(qs) : +List<Nat>}

# Definedness: with no install running, a land's ask enters the queue.
law an_ask_enters:
  for +f: Bool
  {queue(run([ROLand{0n}, ROLand{0n}], empty(f, False{}))) == [Tk{0n, True{}}] : +List<Tk>}

# The fenced repair, proven: no restart cuts a turn, kills an admitted gate or
# refuses an ask, in every interleaving.
law fenced_restart_never_cuts_a_turn:
  for +os: +List<RO>
  {n_cuts(run(os, empty(True{}, False{}))) == 0n : Nat}

law fenced_restart_never_kills_an_admitted_gate:
  for +os: +List<RO>
  {n_kills(run(os, empty(True{}, False{}))) == 0n : Nat}

law fenced_restart_never_refuses_an_ask:
  for +os: +List<RO>
  {n_refused(run(os, empty(True{}, False{}))) == 0n : Nat}

# The install build takes no land turn (the new land-install builds in a shed
# of its own): in every interleaving the queue's tickets are lands' only, so no
# land's ahead-set holds an install.
law no_land_waits_on_install_build:
  for +f: Bool
  for +os: +List<RO>
  {lands_only(queue(run(os, empty(f, False{})))) == True{} : Bool}

# The same, as the open control in install_restart_open.bend states it.
law no_land_queues_behind_an_install_build:
  for +f: Bool
  for +os: +List<RO>
  {waits_on_install(queue(run(os, empty(f, False{})))) == False{} : Bool}

# Definedness: with no turn held, the build goes on to the install.
law a_shed_build_goes_on_to_install:
  for +f: Bool
  {phase(istep(empty(f, False{}))) == IRFree{} : IR}

# The open laws' counterexamples, computed by the checker on the code's model
# (fenced=False): a land passes its slot check, the install judges no turn held,
# builds, takes the slot and reads no blocker, the land asks its turn, and the
# install restarts the gateway under it.
law the_race_cuts_a_turn:
  for +f: Bool
  {n_cuts(run([ROLand{0n}, ROInstall{}, ROInstall{}, ROInstall{}, ROLand{0n}, ROInstall{}], empty(False{}, False{}))) == 1n : Nat}

# ... and the land binds its gate before the restart, then sends it SIGTERM.
law the_race_kills_a_gate:
  for +f: Bool
  {n_kills(run([ROLand{0n}, ROInstall{}, ROInstall{}, ROInstall{}, ROLand{0n}, ROLand{0n}, ROInstall{}, ROLand{0n}], empty(False{}, False{}))) == 1n : Nat}

# ... or the land asks once the restart has begun, and its ask fails.
law the_race_refuses_an_ask:
  for +f: Bool
  {n_refused(run([ROLand{0n}, ROInstall{}, ROInstall{}, ROInstall{}, ROInstall{}, ROLand{0n}], empty(False{}, False{}))) == 1n : Nat}

# The old land-install: its build asks a land turn, and a land that asks next
# waits behind it.
law a_land_queues_behind_a_turn_build:
  for +f: Bool
  {waits_on_install(queue(run([ROInstall{}, ROLand{0n}, ROLand{0n}], empty(False{}, True{})))) == True{} : Bool}

def restart_spares_turns_asked_before_the_read(f, turn, os):
  z_eq(n_early(run(os, empty(f, turn))), early_of(run(os, empty(f, turn)), inv_run(os, empty(f, turn), inv_empty(f, turn))))

def restart_keeps_admitted_jobs(s):
  match s:
    case RS{p, q, ls, js, y, f}:
      {==}

def restart_loses_the_queue(s):
  match s:
    case RS{p, q, ls, js, y, f}:
      {==}

def a_clear_read_restarts(ls, js, y, f):
  {==}

def a_held_turn_refuses(r, n, t, qs):
  {==}

def an_ask_enters(f):
  {==}

def fenced_zs(+os: +List<RO>) -> Zs(n_cuts(run(os, empty(True{}, False{}))), n_kills(run(os, empty(True{}, False{}))), n_refused(run(os, empty(True{}, False{})))):
  zs_of(run(os, empty(True{}, False{})), inv2_run(os, empty(True{}, False{}), inv2_empty()))

def z_c(+c: Nat, +k: Nat, +r: Nat, z: Zs(c, k, r)) -> IsZ(c):
  (zc, rest) = z
  zc

def z_k(+c: Nat, +k: Nat, +r: Nat, z: Zs(c, k, r)) -> IsZ(k):
  (zc, rest) = z
  (zk, zr) = rest
  zk

def z_r(+c: Nat, +k: Nat, +r: Nat, z: Zs(c, k, r)) -> IsZ(r):
  (zc, rest) = z
  (zk, zr) = rest
  zr

def fenced_restart_never_cuts_a_turn(os):
  z_eq(n_cuts(run(os, empty(True{}, False{}))), z_c(n_cuts(run(os, empty(True{}, False{}))), n_kills(run(os, empty(True{}, False{}))), n_refused(run(os, empty(True{}, False{}))), fenced_zs(os)))

def fenced_restart_never_kills_an_admitted_gate(os):
  z_eq(n_kills(run(os, empty(True{}, False{}))), z_k(n_cuts(run(os, empty(True{}, False{}))), n_kills(run(os, empty(True{}, False{}))), n_refused(run(os, empty(True{}, False{}))), fenced_zs(os)))

def fenced_restart_never_refuses_an_ask(os):
  z_eq(n_refused(run(os, empty(True{}, False{}))), z_r(n_cuts(run(os, empty(True{}, False{}))), n_kills(run(os, empty(True{}, False{}))), n_refused(run(os, empty(True{}, False{}))), fenced_zs(os)))

def no_land_waits_on_install_build(f, os):
  lo_refl(queue(run(os, empty(f, False{}))), lo_of(run(os, empty(f, False{})), inv3_run(os, empty(f, False{}), inv3_empty(f))))

def no_land_queues_behind_an_install_build(f, os):
  lo_waits(queue(run(os, empty(f, False{}))), lo_of(run(os, empty(f, False{})), inv3_run(os, empty(f, False{}), inv3_empty(f))))

def a_shed_build_goes_on_to_install(f):
  {==}

def the_race_cuts_a_turn(f):
  {==}

def the_race_kills_a_gate(f):
  {==}

def the_race_refuses_an_ask(f):
  {==}

def a_land_queues_behind_a_turn_build(f):
  {==}