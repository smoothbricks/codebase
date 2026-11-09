# Disk-lifecycle lease (specs/cowshed/05_gateway.md "Disk-lifecycle lease", contract
# "Disk-lease laws"): the pure core `Ledger` of cowshed-gateway src/disk_lease.rs. Two classes
# (Storage, Namespace) exclude each other. Members of one class share the running phase up to a
# cap; a lease asked alone takes all of it. A waiter of the other class stops new entries; when
# the phase drains the next goes to the waiting class, whose oldest waiters enter it in arrival
# order. A holder held the bound while the other class waits is evicted.
#
# The model: `Lg` is the ledger (cap, bound, the running phase or none, the queue oldest first);
# `ask`, `leave`, `expire` and `settle` are the code's functions of the same names, `admit` is
# `Phase::admit_waiting`; each returns the ledger and the effects it would hand the actor (`Fx`).
# Time is Nat ticks: a tick advances `now`, and `ask`, `leave` and `expire` read it. The cap and
# the bound are one more than the Nat a run is given (the code's cap is a NonZeroUsize and
# `validate` refuses a zero bound). Holders are kept newest first; the code keeps them in a vector
# it swap-removes from, and its order is read nowhere. An input names the ticket it leaves by how
# many tickets ago it was asked (`DLeave{back}`); a ticket the ledger no longer knows changes
# nothing.
#
# Laws proven for every sequence of inputs, each with its definedness companion:
#   exclusion              (a) every holder is of the phase's class and the places taken are at
#                              most the cap
#   no_phase_no_wait       (b) with no phase running nobody waits (the code's own invariant);
#                              phase_has_holder: a running phase has a holder
#   fifo, no_overtaking    (c) the queue is in arrival order, and every holder is older than every
#                              waiter of its class: a waiter is granted only when no older waiter
#                              of its class is still waiting
#   no_starvation          (d) while a waiter of the other class waits, the running phase admits
#                              nobody new
#   bounded_hold           (e) after an expire at any time, while the other class waits no holder
#                              remains that has been held the bound
# `gate_io_priority` (a gate's lease never waits behind a background job's) is open: the kernel
# has no job-class input. gate_io_priority_refuted proves its counterexample; the law itself is
# disk_lease_open.bend's, and has no proof.
#
# Check: bun bend2/main.ts disk_lease.bend --verdict
# Bound to the code by cowshed-gateway src/disk_lease/laws.rs, which runs the emitted model
# (src/disk_lease/laws/model.rs) and `Ledger` on the same inputs.
import Base

type Cls is Data:
  CStorage{}
  CNamespace{}

type Shr is Data:
  SShared{}
  SAlone{}

# a holder: its ticket, the class it asked, its share, and the tick of its grant
type Hd is Data:
  Hd{ticket: Nat, cls: Cls, share: Shr, granted: Nat}

# a waiter, oldest first in the queue
type Wt is Data:
  Wt{ticket: Nat, cls: Cls, share: Shr}

# what a phase reports when it ends: when it started, how many it admitted, how many it evicted
type Mt is Data:
  Mt{started: Nat, members: Nat, evicted: Nat}

type Ph is Data:
  PhIdle{}
  PhRun{cls: Cls, meta: Mt, holders: +List<Hd>}

type Lg is Data:
  Lg{cap: Nat, bound: Nat, phase: Ph, waiting: +List<Wt>}

# what a transition tells the actor (Effect in the code)
type Fx is Data:
  FxGrant{ticket: Nat}
  FxEvict{ticket: Nat, cls: Cls, held: Nat, waiting: Cls}
  FxEnd{cls: Cls, elapsed: Nat, members: Nat, evicted: Nat}

type Out is Data:
  Out{lg: Lg, fx: +List<Fx>}

type Adm is Data:
  Adm{holders: +List<Hd>, members: Nat, rest: +List<Wt>, fx: +List<Fx>}

type Ev is Data:
  Ev{keep: +List<Hd>, count: Nat, fx: +List<Fx>}

type Dl is Data:
  DlNone{}
  DlAt{at: Nat}

# the ledger, the last ticket given, the tick, and what the last input did
type St is Data:
  St{lg: Lg, last: Nat, now: Nat, fx: +List<Fx>}

# DLeave names the ticket `back` before the last given; none is a ticket the ledger never knew
type Op is Data:
  DAsk{cls: Cls, share: Shr}
  DLeave{back: Nat}
  DExpire{}
  DTick{}

# --- Nat, Bool and list helpers -----------------------------------------------

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

def le_b(a: Nat, b: Nat) -> Bool:
  match a b:
    case 0n b0:
      True{}
    case 1n+a1 0n:
      False{}
    case 1n+a1 1n+b1:
      le_b(a1, b1)

# a - b, and 0 once b is past a (Instant::saturating_duration_since)
def monus(a: Nat, b: Nat) -> Nat:
  match a b:
    case 0n b0:
      0n
    case 1n+a1 0n:
      1n+a1
    case 1n+a1 1n+b1:
      monus(a1, b1)

def nat_min.if(a: Nat, b: Nat, c: Bool) -> Nat:
  match c:
    case True{}:
      a
    case False{}:
      b

def nat_min(+a: Nat, +b: Nat) -> Nat:
  nat_min.if(a, b, le_b(a, b))

def cls_same(a: Cls, b: Cls) -> Bool:
  match a b:
    case CStorage{} CStorage{}:
      True{}
    case CStorage{} CNamespace{}:
      False{}
    case CNamespace{} CStorage{}:
      False{}
    case CNamespace{} CNamespace{}:
      True{}

def other(c: Cls) -> Cls:
  match c:
    case CStorage{}:
      CNamespace{}
    case CNamespace{}:
      CStorage{}

# the places of a phase of `cap` that a lease takes
def places(s: Shr, cap: Nat) -> Nat:
  match s:
    case SShared{}:
      1n
    case SAlone{}:
      cap

def hd_ticket(h: Hd) -> Nat:
  match h:
    case Hd{t, c, s, g}:
      t

def hd_cls(h: Hd) -> Cls:
  match h:
    case Hd{t, c, s, g}:
      c

def hd_share(h: Hd) -> Shr:
  match h:
    case Hd{t, c, s, g}:
      s

def hd_granted(h: Hd) -> Nat:
  match h:
    case Hd{t, c, s, g}:
      g

def wt_ticket(w: Wt) -> Nat:
  match w:
    case Wt{t, c, s}:
      t

def wt_cls(w: Wt) -> Cls:
  match w:
    case Wt{t, c, s}:
      c

def wt_share(w: Wt) -> Shr:
  match w:
    case Wt{t, c, s}:
      s

# the places the holders take
def taken(hs: +List<Hd>, +cap: Nat) -> Nat:
  match hs:
    case Nil{}:
      0n
    case Con{h, t}:
      Nat.add(places(hd_share(h), cap), taken(t, cap))

# Phase::has_room: a lease of `s` still fits beside the holders
def has_room(+hs: +List<Hd>, s: Shr, +cap: Nat) -> Bool:
  le_b(Nat.add(places(s, cap), taken(hs, cap)), cap)

def snoc_w(q: +List<Wt>, +x: Wt) -> +List<Wt>:
  match q:
    case Nil{}:
      [x]
    case Con{h, t}:
      h <> snoc_w(t, x)

def snoc_f(q: +List<Fx>, +x: Fx) -> +List<Fx>:
  match q:
    case Nil{}:
      [x]
    case Con{h, t}:
      h <> snoc_f(t, x)

def has_w(+t: Nat, ws: +List<Wt>) -> Bool:
  match ws:
    case Nil{}:
      False{}
    case Con{w, r}:
      or(same(t, wt_ticket(w)), has_w(t, r))

def has_h(+t: Nat, hs: +List<Hd>) -> Bool:
  match hs:
    case Nil{}:
      False{}
    case Con{h, r}:
      or(same(t, hd_ticket(h)), has_h(t, r))

# remove the first match (leave finds the ticket once)
def rm_w.if(+w: Wt, r: +List<Wt>, rec: +List<Wt>, b: Bool) -> +List<Wt>:
  match b:
    case True{}:
      r
    case False{}:
      w <> rec

def rm_w(+t: Nat, ws: +List<Wt>) -> +List<Wt>:
  match ws:
    case Nil{}:
      []
    case Con{+w, +r}:
      rm_w.if(w, r, rm_w(t, r), same(t, wt_ticket(w)))

def rm_h.if(+h: Hd, r: +List<Hd>, rec: +List<Hd>, b: Bool) -> +List<Hd>:
  match b:
    case True{}:
      r
    case False{}:
      h <> rec

def rm_h(+t: Nat, hs: +List<Hd>) -> +List<Hd>:
  match hs:
    case Nil{}:
      []
    case Con{+h, +r}:
      rm_h.if(h, r, rm_h(t, r), same(t, hd_ticket(h)))

# some waiter is of class c
def any_cls(+c: Cls, ws: +List<Wt>) -> Bool:
  match ws:
    case Nil{}:
      False{}
    case Con{w, r}:
      or(cls_same(c, wt_cls(w)), any_cls(c, r))

# --- admission: Phase::admit_waiting -------------------------------------------

# the guards of the oldest waiter: its class is the phase's, and the phase has room for it
def head_adm(+c: Cls, ws: +List<Wt>) -> Bool:
  match ws:
    case Nil{}:
      False{}
    case Con{w, r}:
      cls_same(c, wt_cls(w))

def head_room(+hs: +List<Hd>, ws: +List<Wt>, +cap: Nat) -> Bool:
  match ws:
    case Nil{}:
      False{}
    case Con{w, r}:
      has_room(hs, wt_share(w), cap)

def adm_holders(a: Adm) -> +List<Hd>:
  match a:
    case Adm{hs, m, rs, fx}:
      hs

def adm_members(a: Adm) -> Nat:
  match a:
    case Adm{hs, m, rs, fx}:
      m

def adm_rest(a: Adm) -> +List<Wt>:
  match a:
    case Adm{hs, m, rs, fx}:
      rs

def adm_fx(a: Adm) -> +List<Fx>:
  match a:
    case Adm{hs, m, rs, fx}:
      fx

# a waiter passed over stays first in what is left
def admit.skip(+w: Wt, +a: Adm) -> Adm:
  Adm{adm_holders(a), adm_members(a), w <> adm_rest(a), adm_fx(a)}

# Move c's waiters into the phase, oldest first: a waiter of the other class is passed over, a
# waiter of c enters while the phase has room, and the first without room ends the pass.
# `adm` and `room` are the guards of the oldest waiter in `ws` (head_adm, head_room).
def admit(ws: +List<Wt>, +c: Cls, +cap: Nat, +now: Nat, +hs: +List<Hd>, +m: Nat, +fx: +List<Fx>, adm: Bool, room: Bool) -> Adm:
  match ws:
    case Nil{}:
      Adm{hs, m, [], fx}
    case Con{Wt{+t, +wc, +sh}, +r}:
      match adm:
        case False{}:
          admit.skip(Wt{t, wc, sh}, admit(r, c, cap, now, hs, m, fx, head_adm(c, r), head_room(hs, r, cap)))
        case True{}:
          match room:
            case False{}:
              Adm{hs, m, Wt{t, wc, sh} <> r, fx}
            case True{}:
              admit(r, c, cap, now, Hd{t, wc, sh, now} <> hs, 1n+m, snoc_f(fx, FxGrant{t}), head_adm(c, r), head_room(Hd{t, wc, sh, now} <> hs, r, cap))

# the phase of `c` with these holders, over what the pass left waiting
def admit.out(+cap: Nat, +bound: Nat, +c: Cls, +st: Nat, +ev: Nat, +a: Adm) -> Out:
  Out{Lg{cap, bound, PhRun{c, Mt{st, adm_members(a), ev}, adm_holders(a)}, adm_rest(a)}, adm_fx(a)}

# --- settle: Ledger::settle ------------------------------------------------------

def end_fx(+c: Cls, mt: Mt, +now: Nat) -> Fx:
  match mt:
    case Mt{st, m, ev}:
      FxEnd{c, monus(now, st), m, ev}

# a new phase of nc, started now: Phase::new then admit_waiting
def settle.start(+cap: Nat, +bound: Nat, +nc: Cls, +ws: +List<Wt>, +now: Nat, fx: +List<Fx>) -> Out:
  admit.out(cap, bound, nc, now, 0n, admit(ws, nc, cap, now, [], 0n, fx, head_adm(nc, ws), head_room([], ws, cap)))

# a drained phase of pc ends: the other class's phase if it waits, else pc's again if anyone waits
def settle.drained(+cap: Nat, +bound: Nat, +pc: Cls, ws: +List<Wt>, +now: Nat, fx: +List<Fx>, other_waits: Bool) -> Out:
  match ws other_waits:
    case Nil{} b0:
      Out{Lg{cap, bound, PhIdle{}, []}, fx}
    case Con{+w, +r} True{}:
      settle.start(cap, bound, other(pc), w <> r, now, fx)
    case Con{+w, +r} False{}:
      settle.start(cap, bound, pc, w <> r, now, fx)

# a phase with holders: refilled from its own class's waiters unless the other class waits
def settle.live(+cap: Nat, +bound: Nat, +pc: Cls, mt: Mt, +hs: +List<Hd>, +ws: +List<Wt>, +now: Nat, fx: +List<Fx>, other_waits: Bool) -> Out:
  match mt other_waits:
    case Mt{st, m, ev} True{}:
      Out{Lg{cap, bound, PhRun{pc, Mt{st, m, ev}, hs}, ws}, fx}
    case Mt{st, m, ev} False{}:
      admit.out(cap, bound, pc, st, ev, admit(ws, pc, cap, now, hs, m, fx, head_adm(pc, ws), head_room(hs, ws, cap)))

def settle.run(+cap: Nat, +bound: Nat, +pc: Cls, +mt: Mt, hs: +List<Hd>, +ws: +List<Wt>, +now: Nat, +fx: +List<Fx>, other_waits: Bool) -> Out:
  match hs:
    case Nil{}:
      settle.drained(cap, bound, pc, ws, now, snoc_f(fx, end_fx(pc, mt, now)), other_waits)
    case Con{+h, +t}:
      settle.live(cap, bound, pc, mt, h <> t, ws, now, fx, other_waits)

def settle(l: Lg, +now: Nat, +fx: +List<Fx>) -> Out:
  match l:
    case Lg{cap, bound, PhIdle{}, ws}:
      Out{Lg{cap, bound, PhIdle{}, ws}, fx}
    case Lg{cap, bound, PhRun{+pc, mt, hs}, +ws}:
      settle.run(cap, bound, pc, mt, hs, ws, now, fx, any_cls(other(pc), ws))

# --- ask: Ledger::ask -------------------------------------------------------------

def ask.fit(+cap: Nat, +bound: Nat, +pc: Cls, +st: Nat, +m: Nat, +ev: Nat, +hs: +List<Hd>, +now: Nat, +t: Nat, +c: Cls, +s: Shr, adm: Bool, room: Bool) -> Out:
  match adm:
    case False{}:
      Out{Lg{cap, bound, PhRun{pc, Mt{st, m, ev}, hs}, [Wt{t, c, s}]}, []}
    case True{}:
      match room:
        case False{}:
          Out{Lg{cap, bound, PhRun{pc, Mt{st, m, ev}, hs}, [Wt{t, c, s}]}, []}
        case True{}:
          Out{Lg{cap, bound, PhRun{pc, Mt{st, 1n+m, ev}, Hd{t, c, s, now} <> hs}, []}, [FxGrant{t}]}

# a running phase: the asker enters at once if it is the phase's class, fits, and nobody waits
def ask.run(+cap: Nat, +bound: Nat, +pc: Cls, +st: Nat, +m: Nat, +ev: Nat, +hs: +List<Hd>, ws: +List<Wt>, +now: Nat, +t: Nat, +c: Cls, +s: Shr) -> Out:
  match ws:
    case Nil{}:
      ask.fit(cap, bound, pc, st, m, ev, hs, now, t, c, s, cls_same(pc, c), has_room(hs, s, cap))
    case Con{+w, +r}:
      Out{Lg{cap, bound, PhRun{pc, Mt{st, m, ev}, hs}, snoc_w(w <> r, Wt{t, c, s})}, []}

def ask(l: Lg, +now: Nat, +t: Nat, +c: Cls, +s: Shr) -> Out:
  match l:
    case Lg{cap, bound, PhIdle{}, ws}:
      Out{Lg{cap, bound, PhRun{c, Mt{now, 1n, 0n}, [Hd{t, c, s, now}]}, ws}, [FxGrant{t}]}
    case Lg{cap, bound, PhRun{pc, Mt{st, m, ev}, hs}, ws}:
      ask.run(cap, bound, pc, st, m, ev, hs, ws, now, t, c, s)

# --- leave: Ledger::leave ---------------------------------------------------------

def leave.hh(+cap: Nat, +bound: Nat, +pc: Cls, +mt: Mt, +hs: +List<Hd>, +ws: +List<Wt>, +now: Nat, +t: Nat, found: Bool) -> Out:
  match found:
    case True{}:
      settle(Lg{cap, bound, PhRun{pc, mt, rm_h(t, hs)}, ws}, now, [])
    case False{}:
      Out{Lg{cap, bound, PhRun{pc, mt, hs}, ws}, []}

def leave.h(+cap: Nat, +bound: Nat, ph: Ph, +ws: +List<Wt>, +now: Nat, +t: Nat) -> Out:
  match ph:
    case PhIdle{}:
      Out{Lg{cap, bound, PhIdle{}, ws}, []}
    case PhRun{pc, mt, +hs}:
      leave.hh(cap, bound, pc, mt, hs, ws, now, t, has_h(t, hs))

def leave.w(+cap: Nat, +bound: Nat, ph: Ph, +ws: +List<Wt>, +now: Nat, +t: Nat, found: Bool) -> Out:
  match found:
    case True{}:
      settle(Lg{cap, bound, ph, rm_w(t, ws)}, now, [])
    case False{}:
      leave.h(cap, bound, ph, ws, now, t)

def leave(l: Lg, +now: Nat, +t: Nat) -> Out:
  match l:
    case Lg{cap, bound, ph, +ws}:
      leave.w(cap, bound, ph, ws, now, t, has_w(t, ws))

# --- expire: Ledger::expire ---------------------------------------------------------

def ev_keep(e: Ev) -> +List<Hd>:
  match e:
    case Ev{keep, n, fx}:
      keep

def ev_count(e: Ev) -> Nat:
  match e:
    case Ev{keep, n, fx}:
      n

def ev_fx(e: Ev) -> +List<Fx>:
  match e:
    case Ev{keep, n, fx}:
      fx

def evict.if(+h: Hd, +c: Cls, +o: Cls, +now: Nat, +r: Ev, b: Bool) -> Ev:
  match b:
    case True{}:
      Ev{ev_keep(r), 1n+ev_count(r), FxEvict{hd_ticket(h), c, monus(now, hd_granted(h)), o} <> ev_fx(r)}
    case False{}:
      Ev{h <> ev_keep(r), ev_count(r), ev_fx(r)}

# the holders past the bound go, in the order the phase lists them
def evict(hs: +List<Hd>, +c: Cls, +o: Cls, +bound: Nat, +now: Nat) -> Ev:
  match hs:
    case Nil{}:
      Ev{[], 0n, []}
    case Con{+h, t}:
      evict.if(h, c, o, now, evict(t, c, o, bound, now), le_b(bound, monus(now, hd_granted(h))))

def expire.go(+cap: Nat, +bound: Nat, +pc: Cls, mt: Mt, +ws: +List<Wt>, +now: Nat, +e: Ev) -> Out:
  match mt:
    case Mt{st, m, ev}:
      settle(Lg{cap, bound, PhRun{pc, Mt{st, m, Nat.add(ev, ev_count(e))}, ev_keep(e)}, ws}, now, ev_fx(e))

def expire.run(+cap: Nat, +bound: Nat, +pc: Cls, +mt: Mt, +hs: +List<Hd>, +ws: +List<Wt>, +now: Nat, waits: Bool) -> Out:
  match waits:
    case False{}:
      Out{Lg{cap, bound, PhRun{pc, mt, hs}, ws}, []}
    case True{}:
      expire.go(cap, bound, pc, mt, ws, now, evict(hs, pc, other(pc), bound, now))

def expire(l: Lg, +now: Nat) -> Out:
  match l:
    case Lg{cap, bound, PhIdle{}, ws}:
      Out{Lg{cap, bound, PhIdle{}, ws}, []}
    case Lg{cap, bound, PhRun{+pc, mt, hs}, +ws}:
      expire.run(cap, bound, pc, mt, hs, ws, now, any_cls(other(pc), ws))

# --- deadline: Ledger::deadline -----------------------------------------------------

def min_g(hs: +List<Hd>, +cur: Nat) -> Nat:
  match hs:
    case Nil{}:
      cur
    case Con{h, t}:
      min_g(t, nat_min(cur, hd_granted(h)))

def deadline.run(+bound: Nat, hs: +List<Hd>, waits: Bool) -> Dl:
  match hs waits:
    case Nil{} w0:
      DlNone{}
    case Con{h, t} False{}:
      DlNone{}
    case Con{h, t} True{}:
      DlAt{Nat.add(min_g(t, hd_granted(h)), bound)}

def deadline(l: Lg) -> Dl:
  match l:
    case Lg{cap, bound, PhIdle{}, ws}:
      DlNone{}
    case Lg{cap, bound, PhRun{+pc, mt, hs}, ws}:
      deadline.run(bound, hs, any_cls(other(pc), ws))

# --- the inputs ---------------------------------------------------------------------

def lg_of(o: Out) -> Lg:
  match o:
    case Out{lg, fx}:
      lg

def fx_of(o: Out) -> +List<Fx>:
  match o:
    case Out{lg, fx}:
      fx

def step.out(+o: Out, +last: Nat, +now: Nat) -> St:
  St{lg_of(o), last, now, fx_of(o)}

def step(o: Op, s: St) -> St:
  match o s:
    case DAsk{c, sh} St{lg, +last, +now, fx}:
      step.out(ask(lg, now, 1n+last, c, sh), 1n+last, now)
    case DLeave{back} St{lg, +last, +now, fx}:
      step.out(leave(lg, now, monus(last, back)), last, now)
    case DExpire{} St{lg, last, +now, fx}:
      step.out(expire(lg, now), last, now)
    case DTick{} St{lg, last, now, fx}:
      St{lg, last, 1n+now, []}

def run(os: +List<Op>, s: St) -> St:
  match os:
    case Nil{}:
      s
    case Con{o, t}:
      run(t, step(o, s))

# a ledger of cap c0+1 and bound b0+1, idle, at tick 0, no ticket given
def empty(c0: Nat, b0: Nat) -> St:
  St{Lg{1n+c0, 1n+b0, PhIdle{}, []}, 0n, 0n, []}

def lg(s: St) -> Lg:
  match s:
    case St{l, last, now, fx}:
      l

def last(s: St) -> Nat:
  match s:
    case St{l, la, now, fx}:
      la

def now(s: St) -> Nat:
  match s:
    case St{l, last, n, fx}:
      n

def fx(s: St) -> +List<Fx>:
  match s:
    case St{l, last, now, f}:
      f

# --- predicates on Bool and Nat -----------------------------------------------------

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

def absurd(-A: Type, e: Empty) -> A:
  match e:

def refl_t(b: Bool, p: IsT(b)) -> {b == True{} : Bool}:
  match b:
    case True{}:
      {==}
    case False{}:
      absurd({False{} == True{} : Bool}, p)

def and_t(a: Bool, b: Bool, pa: IsT(a), pb: IsT(b)) -> IsT(and(a, b)):
  match a:
    case True{}:
      pb
    case False{}:
      pa

def and_l(a: Bool, b: Bool, p: IsT(and(a, b))) -> IsT(a):
  match a:
    case True{}:
      Unit{}
    case False{}:
      p

def and_r(a: Bool, b: Bool, p: IsT(and(a, b))) -> IsT(b):
  match a:
    case True{}:
      p
    case False{}:
      absurd(IsT(b), p)

# a implies b
def imp(a: Bool, b: Bool) -> Bool:
  or(not(a), b)

def imp_f(a: Bool, b: Bool, p: IsF(a)) -> IsT(imp(a, b)):
  match a:
    case True{}:
      absurd(IsT(imp(True{}, b)), p)
    case False{}:
      Unit{}

def imp_t(a: Bool, b: Bool, p: IsT(b)) -> IsT(imp(a, b)):
  match a:
    case True{}:
      p
    case False{}:
      Unit{}

def imp_use(a: Bool, b: Bool, pa: IsT(a), p: IsT(imp(a, b))) -> IsT(b):
  match a:
    case True{}:
      p
    case False{}:
      absurd(IsT(b), pa)

def same_refl(+n: Nat) -> IsT(same(n, n)):
  match n:
    case 0n:
      Unit{}
    case 1n+p:
      same_refl(p)

def cls_same_refl(c: Cls) -> IsT(cls_same(c, c)):
  match c:
    case CStorage{}:
      Unit{}
    case CNamespace{}:
      Unit{}

# the other class is not the same class
def cls_same_other(c: Cls) -> IsF(cls_same(c, other(c))):
  match c:
    case CStorage{}:
      Unit{}
    case CNamespace{}:
      Unit{}

def LE(a: Nat, b: Nat) -> Data:
  IsT(le_b(a, b))

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

def le_succ(+a: Nat, +b: Nat, h: LE(a, b)) -> LE(a, 1n+b):
  match a b:
    case 0n b0:
      Unit{}
    case 1n+a1 0n:
      match h:
    case 1n+a1 1n+b1:
      le_succ(a1, b1, h)

# a <= p + a
def le_add_r(+p: Nat, +a: Nat) -> LE(a, Nat.add(p, a)):
  match p:
    case 0n:
      le_refl(a)
    case 1n+p1:
      le_succ(a, Nat.add(p1, a), le_add_r(p1, a))

def le_add_l(+p: Nat, +a: Nat, +b: Nat, h: LE(a, b)) -> LE(Nat.add(p, a), Nat.add(p, b)):
  match p:
    case 0n:
      h
    case 1n+p1:
      le_add_l(p1, a, b, h)

# c + 0 <= c
def add_zero_le(+c: Nat) -> LE(Nat.add(c, 0n), c):
  match c:
    case 0n:
      Unit{}
    case 1n+p:
      add_zero_le(p)

def monus_self(+n: Nat) -> {monus(n, n) == 0n : Nat}:
  match n:
    case 0n:
      {==}
    case 1n+p:
      monus_self(p)

# --- FIFO: the queue is in arrival order ----------------------------------------------

# ticket numbers rise strictly from the oldest waiter to the newest, every one above `lo`
# and none above the last number given
def Chain(+lo: Nat, ws: +List<Wt>, +top: Nat) -> Data:
  match ws:
    case Nil{}:
      LE(lo, top)
    case Con{+w, t}:
      Sigma<&2, &2, LE(1n+lo, wt_ticket(w)), u => Chain(wt_ticket(w), t, top)>

def chain_b(ws: +List<Wt>, +lo: Nat, +top: Nat) -> Bool:
  match ws:
    case Nil{}:
      le_b(lo, top)
    case Con{+w, t}:
      and(le_b(1n+lo, wt_ticket(w)), chain_b(t, wt_ticket(w), top))

def chain_refl(+ws: +List<Wt>, +lo: Nat, +top: Nat, ch: Chain(lo, ws, top)) -> IsT(chain_b(ws, lo, top)):
  match ws:
    case Nil{}:
      ch
    case Con{w, t}:
      (lt, rest) = ch
      and_t(le_b(1n+lo, wt_ticket(w)), chain_b(t, wt_ticket(w), top), lt, chain_refl(t, wt_ticket(w), top, rest))

def chain_weaken(+lo: Nat, +n: Nat, +t: +List<Wt>, +top: Nat, le: LE(lo, n), ch: Chain(n, t, top)) -> Chain(lo, t, top):
  match t:
    case Nil{}:
      le_trans(lo, n, top, le, ch)
    case Con{w, r}:
      (lt, rest) = ch
      (le_trans(1n+lo, 1n+n, wt_ticket(w), le, lt), rest)

def chain_snoc(+q: +List<Wt>, +lo: Nat, +top: Nat, +c: Cls, +s: Shr, ch: Chain(lo, q, top)) -> Chain(lo, snoc_w(q, Wt{1n+top, c, s}), 1n+top):
  match q:
    case Nil{}:
      (ch, le_refl(1n+top))
    case Con{w, t}:
      (lt, rest) = ch
      (lt, chain_snoc(t, wt_ticket(w), top, c, s, rest))

def chain_rm.if(+lo: Nat, +w: Wt, +t: +List<Wt>, +r: +List<Wt>, +top: Nat, b: Bool, ch: Chain(lo, w <> t, top), cr: Chain(wt_ticket(w), r, top)) -> Chain(lo, rm_w.if(w, t, r, b), top):
  match b:
    case True{}:
      (lt, rest) = ch
      chain_weaken(lo, wt_ticket(w), t, top, le_of_lt(lo, wt_ticket(w), lt), rest)
    case False{}:
      (lt, rest) = ch
      (lt, cr)

# one waiter leaving keeps the rest in order
def chain_rm(+q: +List<Wt>, +lo: Nat, +n: Nat, +top: Nat, +ch: Chain(lo, q, top)) -> Chain(lo, rm_w(n, q), top):
  match q:
    case Nil{}:
      ch
    case Con{w, t}:
      (lt, +rest) = ch
      chain_rm.if(lo, w, t, rm_w(n, t), top, same(n, wt_ticket(w)), ch, chain_rm(t, wt_ticket(w), n, top, rest))

# --- the laws as checks: Bool functions, each also a Data predicate through IsT -------

def nil_w(ws: +List<Wt>) -> Bool:
  match ws:
    case Nil{}:
      True{}
    case Con{w, r}:
      False{}

def con_h(hs: +List<Hd>) -> Bool:
  match hs:
    case Nil{}:
      False{}
    case Con{h, r}:
      True{}

# every holder asked the phase's class
def cls_all_b(+c: Cls, hs: +List<Hd>) -> Bool:
  match hs:
    case Nil{}:
      True{}
    case Con{h, t}:
      and(cls_same(c, hd_cls(h)), cls_all_b(c, t))

# the places the holders take are at most the cap
def fits_b(+hs: +List<Hd>, +cap: Nat) -> Bool:
  le_b(taken(hs, cap), cap)

def excl_ph(ph: Ph, +cap: Nat) -> Bool:
  match ph:
    case PhIdle{}:
      True{}
    case PhRun{+c, mt, +hs}:
      and(cls_all_b(c, hs), fits_b(hs, cap))

def excl_b(l: Lg) -> Bool:
  match l:
    case Lg{cap, bound, ph, ws}:
      excl_ph(ph, cap)

def tt(b: Bool, e: {b == True{} : Bool}) -> IsT(b):
  match b:
    case True{}:
      Unit{}
    case False{}:
      %e : IsF(_)
      Unit{}

def ff(b: Bool, e: {b == False{} : Bool}) -> IsF(b):
  match b:
    case True{}:
      %e : IsT(_)
      Unit{}
    case False{}:
      Unit{}

# --- (a) exclusion through the pass: what enters is the phase's class and fits --------------

def admit_excl(+ws: +List<Wt>, +c: Cls, +cap: Nat, +now: Nat, +hs: +List<Hd>, +m: Nat, +fx: +List<Fx>, +adm: Bool, +room: Bool, ea: {head_adm(c, ws) == adm : Bool}, er: {head_room(hs, ws, cap) == room : Bool}, +ok: IsT(and(cls_all_b(c, hs), fits_b(hs, cap)))) -> IsT(and(cls_all_b(c, adm_holders(admit(ws, c, cap, now, hs, m, fx, adm, room))), fits_b(adm_holders(admit(ws, c, cap, now, hs, m, fx, adm, room)), cap))):
  match ws:
    case Nil{}:
      ok
    case Con{Wt{+t, +wc, +sh}, +r}:
      match adm:
        case False{}:
          admit_excl(r, c, cap, now, hs, m, fx, head_adm(c, r), head_room(hs, r, cap), {==}, {==}, ok)
        case True{}:
          match room:
            case False{}:
              ok
            case True{}:
              admit_excl(r, c, cap, now, Hd{t, wc, sh, now} <> hs, 1n+m, snoc_f(fx, FxGrant{t}), head_adm(c, r), head_room(Hd{t, wc, sh, now} <> hs, r, cap), {==}, {==}, and_t(cls_all_b(c, Hd{t, wc, sh, now} <> hs), fits_b(Hd{t, wc, sh, now} <> hs, cap), and_t(cls_same(c, wc), cls_all_b(c, hs), tt(cls_same(c, wc), ea), and_l(cls_all_b(c, hs), fits_b(hs, cap), ok)), tt(has_room(hs, sh, cap), er)))

# --- holders leaving never break exclusion --------------------------------------------------

def taken_rm_h.if(+t: Nat, +h: Hd, +r: +List<Hd>, +cap: Nat, b: Bool, ih: LE(taken(rm_h(t, r), cap), taken(r, cap))) -> LE(taken(rm_h.if(h, r, rm_h(t, r), b), cap), taken(h <> r, cap)):
  match b:
    case True{}:
      le_add_r(places(hd_share(h), cap), taken(r, cap))
    case False{}:
      le_add_l(places(hd_share(h), cap), taken(rm_h(t, r), cap), taken(r, cap), ih)

def taken_rm_h(+t: Nat, +hs: +List<Hd>, +cap: Nat) -> LE(taken(rm_h(t, hs), cap), taken(hs, cap)):
  match hs:
    case Nil{}:
      Unit{}
    case Con{+h, +r}:
      taken_rm_h.if(t, h, r, cap, same(t, hd_ticket(h)), taken_rm_h(t, r, cap))

def cls_rm_h.if(+c: Cls, +t: Nat, +h: Hd, +r: +List<Hd>, b: Bool, ch: IsT(cls_all_b(c, h <> r)), ih: IsT(cls_all_b(c, rm_h(t, r)))) -> IsT(cls_all_b(c, rm_h.if(h, r, rm_h(t, r), b))):
  match b:
    case True{}:
      and_r(cls_same(c, hd_cls(h)), cls_all_b(c, r), ch)
    case False{}:
      and_t(cls_same(c, hd_cls(h)), cls_all_b(c, rm_h(t, r)), and_l(cls_same(c, hd_cls(h)), cls_all_b(c, r), ch), ih)

def cls_rm_h(+c: Cls, +t: Nat, +hs: +List<Hd>, +ch: IsT(cls_all_b(c, hs))) -> IsT(cls_all_b(c, rm_h(t, hs))):
  match hs:
    case Nil{}:
      Unit{}
    case Con{+h, +r}:
      cls_rm_h.if(c, t, h, r, same(t, hd_ticket(h)), ch, cls_rm_h(c, t, r, and_r(cls_same(c, hd_cls(h)), cls_all_b(c, r), ch)))

def excl_rm_h(+c: Cls, +t: Nat, +hs: +List<Hd>, +cap: Nat, +ok: IsT(and(cls_all_b(c, hs), fits_b(hs, cap)))) -> IsT(and(cls_all_b(c, rm_h(t, hs)), fits_b(rm_h(t, hs), cap))):
  and_t(cls_all_b(c, rm_h(t, hs)), fits_b(rm_h(t, hs), cap), cls_rm_h(c, t, hs, and_l(cls_all_b(c, hs), fits_b(hs, cap), ok)), le_trans(taken(rm_h(t, hs), cap), taken(hs, cap), cap, taken_rm_h(t, hs, cap), and_r(cls_all_b(c, hs), fits_b(hs, cap), ok)))

# --- evicting holders never breaks exclusion --------------------------------------------------

def taken_keep.if(+h: Hd, +c: Cls, +o: Cls, +now: Nat, +r: Ev, +t: +List<Hd>, +cap: Nat, b: Bool, ih: LE(taken(ev_keep(r), cap), taken(t, cap))) -> LE(taken(ev_keep(evict.if(h, c, o, now, r, b)), cap), taken(h <> t, cap)):
  match b:
    case True{}:
      le_trans(taken(ev_keep(r), cap), taken(t, cap), taken(h <> t, cap), ih, le_add_r(places(hd_share(h), cap), taken(t, cap)))
    case False{}:
      le_add_l(places(hd_share(h), cap), taken(ev_keep(r), cap), taken(t, cap), ih)

def taken_keep(+hs: +List<Hd>, +c: Cls, +o: Cls, +bound: Nat, +now: Nat, +cap: Nat) -> LE(taken(ev_keep(evict(hs, c, o, bound, now)), cap), taken(hs, cap)):
  match hs:
    case Nil{}:
      Unit{}
    case Con{+h, +t}:
      taken_keep.if(h, c, o, now, evict(t, c, o, bound, now), t, cap, le_b(bound, monus(now, hd_granted(h))), taken_keep(t, c, o, bound, now, cap))

def cls_keep.if(+k: Cls, +h: Hd, +c: Cls, +o: Cls, +now: Nat, +r: Ev, +t: +List<Hd>, b: Bool, ch: IsT(cls_all_b(k, h <> t)), ih: IsT(cls_all_b(k, ev_keep(r)))) -> IsT(cls_all_b(k, ev_keep(evict.if(h, c, o, now, r, b)))):
  match b:
    case True{}:
      ih
    case False{}:
      and_t(cls_same(k, hd_cls(h)), cls_all_b(k, ev_keep(r)), and_l(cls_same(k, hd_cls(h)), cls_all_b(k, t), ch), ih)

def cls_keep(+k: Cls, +hs: +List<Hd>, +c: Cls, +o: Cls, +bound: Nat, +now: Nat, +ch: IsT(cls_all_b(k, hs))) -> IsT(cls_all_b(k, ev_keep(evict(hs, c, o, bound, now)))):
  match hs:
    case Nil{}:
      Unit{}
    case Con{+h, +t}:
      cls_keep.if(k, h, c, o, now, evict(t, c, o, bound, now), t, le_b(bound, monus(now, hd_granted(h))), ch, cls_keep(k, t, c, o, bound, now, and_r(cls_same(k, hd_cls(h)), cls_all_b(k, t), ch)))

def excl_keep(+c: Cls, +hs: +List<Hd>, +o: Cls, +bound: Nat, +now: Nat, +cap: Nat, +ok: IsT(and(cls_all_b(c, hs), fits_b(hs, cap)))) -> IsT(and(cls_all_b(c, ev_keep(evict(hs, c, o, bound, now))), fits_b(ev_keep(evict(hs, c, o, bound, now)), cap))):
  and_t(cls_all_b(c, ev_keep(evict(hs, c, o, bound, now))), fits_b(ev_keep(evict(hs, c, o, bound, now)), cap), cls_keep(c, hs, c, o, bound, now, and_l(cls_all_b(c, hs), fits_b(hs, cap), ok)), le_trans(taken(ev_keep(evict(hs, c, o, bound, now)), cap), taken(hs, cap), cap, taken_keep(hs, c, o, bound, now, cap), and_r(cls_all_b(c, hs), fits_b(hs, cap), ok)))

# --- (b) and the rest of the ledger's own invariants -----------------------------------------

def pos_n(n: Nat) -> Bool:
  match n:
    case 0n:
      False{}
    case 1n+p:
      True{}

def pos_b(l: Lg) -> Bool:
  match l:
    case Lg{cap, bound, ph, ws}:
      and(pos_n(cap), pos_n(bound))

# no phase running means nobody waits
def nowait_ph(ph: Ph, ws: +List<Wt>) -> Bool:
  match ph:
    case PhIdle{}:
      nil_w(ws)
    case PhRun{c, mt, hs}:
      True{}

def nowait_b(l: Lg) -> Bool:
  match l:
    case Lg{cap, bound, ph, ws}:
      nowait_ph(ph, ws)

# a running phase has a holder: a phase never sits drained while anyone waits
def live_ph(ph: Ph) -> Bool:
  match ph:
    case PhIdle{}:
      True{}
    case PhRun{c, mt, hs}:
      con_h(hs)

def live_b(l: Lg) -> Bool:
  match l:
    case Lg{cap, bound, ph, ws}:
      live_ph(ph)

def waiting_of(l: Lg) -> +List<Wt>:
  match l:
    case Lg{cap, bound, ph, ws}:
      ws

# what settle needs of a ledger, and what every ledger the inputs reach has
def Pre(l: Lg) -> Data:
  Sigma<&2, &2, IsT(excl_b(l)), u => Sigma<&2, &2, IsT(nowait_b(l)), v => IsT(pos_b(l))>>

def Lin(l: Lg) -> Data:
  Sigma<&2, &2, IsT(excl_b(l)), u => Sigma<&2, &2, IsT(nowait_b(l)), v => Sigma<&2, &2, IsT(live_b(l)), w => IsT(pos_b(l))>>>

def pre_of(l: Lg, i: Lin(l)) -> Pre(l):
  (e, (n, (v, p))) = i
  (e, (n, p))

def or_l(a: Bool, b: Bool, p: IsT(a)) -> IsT(or(a, b)):
  match a:
    case True{}:
      Unit{}
    case False{}:
      absurd(IsT(b), p)

def or_false_l(a: Bool, b: Bool, pf: IsF(a), p: IsT(or(a, b))) -> IsT(b):
  match a:
    case True{}:
      absurd(IsT(b), pf)
    case False{}:
      p

def or_f_l(a: Bool, b: Bool, p: IsF(or(a, b))) -> IsF(a):
  match a:
    case True{}:
      p
    case False{}:
      Unit{}

def tf_absurd(b: Bool, pt: IsT(b), pf: IsF(b)) -> Empty:
  match b:
    case True{}:
      pf
    case False{}:
      pt

# no waiter of the other class means a waiter of the phase's class is of the phase's class
def other_not_then_same(c: Cls, w: Cls, p: IsF(cls_same(other(c), w))) -> IsT(cls_same(c, w)):
  match c w:
    case CStorage{} CStorage{}:
      Unit{}
    case CStorage{} CNamespace{}:
      absurd(IsT(cls_same(CStorage{}, CNamespace{})), p)
    case CNamespace{} CStorage{}:
      absurd(IsT(cls_same(CNamespace{}, CStorage{})), p)
    case CNamespace{} CNamespace{}:
      Unit{}

# a lease fits an empty phase of a positive cap
def room_empty(s: Shr, +cap: Nat, pos: IsT(pos_n(cap))) -> IsT(has_room([], s, cap)):
  match s cap:
    case SShared{} 0n:
      absurd(IsT(has_room([], SShared{}, 0n)), pos)
    case SShared{} 1n+p:
      Unit{}
    case SAlone{} c:
      add_zero_le(c)

# a pass over holders that has one leaves one
def admit_con(+ws: +List<Wt>, +c: Cls, +cap: Nat, +now: Nat, +hs: +List<Hd>, +m: Nat, +fx: +List<Fx>, +adm: Bool, +room: Bool, ne: IsT(con_h(hs))) -> IsT(con_h(adm_holders(admit(ws, c, cap, now, hs, m, fx, adm, room)))):
  match ws:
    case Nil{}:
      ne
    case Con{Wt{+t, +wc, +sh}, +r}:
      match adm:
        case False{}:
          admit_con(r, c, cap, now, hs, m, fx, head_adm(c, r), head_room(hs, r, cap), ne)
        case True{}:
          match room:
            case False{}:
              ne
            case True{}:
              admit_con(r, c, cap, now, Hd{t, wc, sh, now} <> hs, 1n+m, snoc_f(fx, FxGrant{t}), head_adm(c, r), head_room(Hd{t, wc, sh, now} <> hs, r, cap), Unit{})

# an empty phase takes the oldest waiter of its class: the pass leaves a holder
def admit_first(+ws: +List<Wt>, +c: Cls, +cap: Nat, +now: Nat, +m: Nat, +fx: +List<Fx>, +adm: Bool, +room: Bool, ea: {head_adm(c, ws) == adm : Bool}, er: {head_room([], ws, cap) == room : Bool}, pos: IsT(pos_n(cap)), an: IsT(any_cls(c, ws))) -> IsT(con_h(adm_holders(admit(ws, c, cap, now, [], m, fx, adm, room)))):
  match ws:
    case Nil{}:
      absurd(IsT(con_h(adm_holders(admit([], c, cap, now, [], m, fx, adm, room)))), an)
    case Con{Wt{+t, +wc, +sh}, +r}:
      match adm:
        case False{}:
          admit_first(r, c, cap, now, m, fx, head_adm(c, r), head_room([], r, cap), {==}, {==}, pos, or_false_l(cls_same(c, wc), any_cls(c, r), ff(cls_same(c, wc), ea), an))
        case True{}:
          match room:
            case False{}:
              absurd(IsT(con_h(adm_holders(admit(Wt{t, wc, sh} <> r, c, cap, now, [], m, fx, True{}, False{})))), tf_absurd(has_room([], sh, cap), room_empty(sh, cap, pos), ff(has_room([], sh, cap), er)))
            case True{}:
              admit_con(r, c, cap, now, Hd{t, wc, sh, now} <> [], 1n+m, snoc_f(fx, FxGrant{t}), head_adm(c, r), head_room(Hd{t, wc, sh, now} <> [], r, cap), Unit{})

# --- settle keeps the ledger's invariants ---------------------------------------------------

def start_lin(+cap: Nat, +bound: Nat, +nc: Cls, +ws: +List<Wt>, +now: Nat, +fx: +List<Fx>, an: IsT(any_cls(nc, ws)), +pc: IsT(pos_n(cap)), pb: IsT(pos_n(bound))) -> Lin(lg_of(settle.start(cap, bound, nc, ws, now, fx))):
  (admit_excl(ws, nc, cap, now, [], 0n, fx, head_adm(nc, ws), head_room([], ws, cap), {==}, {==}, Unit{}), (Unit{}, (admit_first(ws, nc, cap, now, 0n, fx, head_adm(nc, ws), head_room([], ws, cap), {==}, {==}, pc, an), and_t(pos_n(cap), pos_n(bound), pc, pb))))

def drained_lin(+cap: Nat, +bound: Nat, +pc: Cls, ws: +List<Wt>, +now: Nat, +fx: +List<Fx>, ow: Bool, ew: {any_cls(other(pc), ws) == ow : Bool}, +pcap: IsT(pos_n(cap)), pb: IsT(pos_n(bound))) -> Lin(lg_of(settle.drained(cap, bound, pc, ws, now, fx, ow))):
  match ws ow:
    case Nil{} b0:
      (Unit{}, (Unit{}, (Unit{}, and_t(pos_n(cap), pos_n(bound), pcap, pb))))
    case Con{+w, +r} True{}:
      start_lin(cap, bound, other(pc), w <> r, now, fx, tt(any_cls(other(pc), w <> r), ew), pcap, pb)
    case Con{+w, +r} False{}:
      start_lin(cap, bound, pc, w <> r, now, fx, or_l(cls_same(pc, wt_cls(w)), any_cls(pc, r), other_not_then_same(pc, wt_cls(w), or_f_l(cls_same(other(pc), wt_cls(w)), any_cls(other(pc), r), ff(any_cls(other(pc), w <> r), ew)))), pcap, pb)

def live_lin(+cap: Nat, +bound: Nat, +pc: Cls, mt: Mt, +hs: +List<Hd>, +ws: +List<Wt>, +now: Nat, +fx: +List<Fx>, ow: Bool, +ne: IsT(con_h(hs)), +ex: IsT(and(cls_all_b(pc, hs), fits_b(hs, cap))), +pcap: IsT(pos_n(cap)), pb: IsT(pos_n(bound))) -> Lin(lg_of(settle.live(cap, bound, pc, mt, hs, ws, now, fx, ow))):
  match mt ow:
    case Mt{st, +m, ev} True{}:
      (ex, (Unit{}, (ne, and_t(pos_n(cap), pos_n(bound), pcap, pb))))
    case Mt{st, +m, ev} False{}:
      (admit_excl(ws, pc, cap, now, hs, m, fx, head_adm(pc, ws), head_room(hs, ws, cap), {==}, {==}, ex), (Unit{}, (admit_con(ws, pc, cap, now, hs, m, fx, head_adm(pc, ws), head_room(hs, ws, cap), ne), and_t(pos_n(cap), pos_n(bound), pcap, pb))))

def run_lin(+cap: Nat, +bound: Nat, +pc: Cls, +mt: Mt, hs: +List<Hd>, +ws: +List<Wt>, +now: Nat, +fx: +List<Fx>, ow: Bool, ew: {any_cls(other(pc), ws) == ow : Bool}, ex: IsT(and(cls_all_b(pc, hs), fits_b(hs, cap))), pcap: IsT(pos_n(cap)), pb: IsT(pos_n(bound))) -> Lin(lg_of(settle.run(cap, bound, pc, mt, hs, ws, now, fx, ow))):
  match hs:
    case Nil{}:
      drained_lin(cap, bound, pc, ws, now, snoc_f(fx, end_fx(pc, mt, now)), ow, ew, pcap, pb)
    case Con{+h, +t}:
      live_lin(cap, bound, pc, mt, h <> t, ws, now, fx, ow, Unit{}, ex, pcap, pb)

def settle_lin(+l: Lg, +now: Nat, +fx: +List<Fx>, i: Pre(l)) -> Lin(lg_of(settle(l, now, fx))):
  match l:
    case Lg{cap, bound, PhIdle{}, ws}:
      (e, (n, p)) = i
      (e, (n, (Unit{}, p)))
    case Lg{+cap, +bound, PhRun{+pc, mt, hs}, +ws}:
      (e, (n, +p)) = i
      run_lin(cap, bound, pc, mt, hs, ws, now, fx, any_cls(other(pc), ws), {==}, e, and_l(pos_n(cap), pos_n(bound), p), and_r(pos_n(cap), pos_n(bound), p))

# --- every input keeps them ------------------------------------------------------------------

def ask_lin.fit(+cap: Nat, +bound: Nat, +pc: Cls, +st: Nat, +m: Nat, +ev: Nat, +hs: +List<Hd>, +now: Nat, +t: Nat, +c: Cls, +s: Shr, adm: Bool, room: Bool, ea: {cls_same(pc, c) == adm : Bool}, er: {has_room(hs, s, cap) == room : Bool}, i: Lin(Lg{cap, bound, PhRun{pc, Mt{st, m, ev}, hs}, []})) -> Lin(lg_of(ask.fit(cap, bound, pc, st, m, ev, hs, now, t, c, s, adm, room))):
  match adm:
    case False{}:
      i
    case True{}:
      match room:
        case False{}:
          i
        case True{}:
          (e, (n, (v, p))) = i
          (and_t(cls_all_b(pc, Hd{t, c, s, now} <> hs), fits_b(Hd{t, c, s, now} <> hs, cap), and_t(cls_same(pc, c), cls_all_b(pc, hs), tt(cls_same(pc, c), ea), and_l(cls_all_b(pc, hs), fits_b(hs, cap), e)), tt(has_room(hs, s, cap), er)), (Unit{}, (Unit{}, p)))

def ask_lin.run(+cap: Nat, +bound: Nat, +pc: Cls, +st: Nat, +m: Nat, +ev: Nat, +hs: +List<Hd>, ws: +List<Wt>, +now: Nat, +t: Nat, +c: Cls, +s: Shr, i: Lin(Lg{cap, bound, PhRun{pc, Mt{st, m, ev}, hs}, ws})) -> Lin(lg_of(ask.run(cap, bound, pc, st, m, ev, hs, ws, now, t, c, s))):
  match ws:
    case Nil{}:
      ask_lin.fit(cap, bound, pc, st, m, ev, hs, now, t, c, s, cls_same(pc, c), has_room(hs, s, cap), {==}, {==}, i)
    case Con{+w, +r}:
      i

def ask_lin(+l: Lg, +now: Nat, +t: Nat, +c: Cls, +s: Shr, i: Lin(l)) -> Lin(lg_of(ask(l, now, t, c, s))):
  match l:
    case Lg{+cap, bound, PhIdle{}, ws}:
      (e, (n, (v, +p))) = i
      (and_t(cls_all_b(c, [Hd{t, c, s, now}]), fits_b([Hd{t, c, s, now}], cap), and_t(cls_same(c, c), True{}, cls_same_refl(c), Unit{}), room_empty(s, cap, and_l(pos_n(cap), pos_n(bound), p))), (Unit{}, (Unit{}, p)))
    case Lg{cap, bound, PhRun{pc, Mt{st, m, ev}, hs}, ws}:
      ask_lin.run(cap, bound, pc, st, m, ev, hs, ws, now, t, c, s, i)

def rm_w_nil(+t: Nat, ws: +List<Wt>, n: IsT(nil_w(ws))) -> IsT(nil_w(rm_w(t, ws))):
  match ws:
    case Nil{}:
      Unit{}
    case Con{w, r}:
      absurd(IsT(nil_w(rm_w(t, w <> r))), n)

def leave_lin.hh(+cap: Nat, +bound: Nat, +pc: Cls, +mt: Mt, +hs: +List<Hd>, +ws: +List<Wt>, +now: Nat, +t: Nat, found: Bool, i: Lin(Lg{cap, bound, PhRun{pc, mt, hs}, ws})) -> Lin(lg_of(leave.hh(cap, bound, pc, mt, hs, ws, now, t, found))):
  match found:
    case True{}:
      (e, (n, (v, p))) = i
      settle_lin(Lg{cap, bound, PhRun{pc, mt, rm_h(t, hs)}, ws}, now, [], (excl_rm_h(pc, t, hs, cap, e), (n, p)))
    case False{}:
      i

def leave_lin.h(+cap: Nat, +bound: Nat, ph: Ph, +ws: +List<Wt>, +now: Nat, +t: Nat, i: Lin(Lg{cap, bound, ph, ws})) -> Lin(lg_of(leave.h(cap, bound, ph, ws, now, t))):
  match ph:
    case PhIdle{}:
      i
    case PhRun{pc, mt, +hs}:
      leave_lin.hh(cap, bound, pc, mt, hs, ws, now, t, has_h(t, hs), i)

def leave_lin.w(+cap: Nat, +bound: Nat, ph: Ph, +ws: +List<Wt>, +now: Nat, +t: Nat, found: Bool, i: Lin(Lg{cap, bound, ph, ws})) -> Lin(lg_of(leave.w(cap, bound, ph, ws, now, t, found))):
  match ph found:
    case PhIdle{} True{}:
      (e, (n, (v, p))) = i
      settle_lin(Lg{cap, bound, PhIdle{}, rm_w(t, ws)}, now, [], (e, (rm_w_nil(t, ws, n), p)))
    case PhRun{pc, mt, hs} True{}:
      (e, (n, (v, p))) = i
      settle_lin(Lg{cap, bound, PhRun{pc, mt, hs}, rm_w(t, ws)}, now, [], (e, (n, p)))
    case p0 False{}:
      leave_lin.h(cap, bound, p0, ws, now, t, i)

def leave_lin(+l: Lg, +now: Nat, +t: Nat, i: Lin(l)) -> Lin(lg_of(leave(l, now, t))):
  match l:
    case Lg{cap, bound, ph, +ws}:
      leave_lin.w(cap, bound, ph, ws, now, t, has_w(t, ws), i)

def expire_lin.go(+cap: Nat, +bound: Nat, +pc: Cls, mt: Mt, +hs: +List<Hd>, +ws: +List<Wt>, +now: Nat, ex: IsT(and(cls_all_b(pc, hs), fits_b(hs, cap))), p: IsT(pos_b(Lg{cap, bound, PhRun{pc, mt, hs}, ws}))) -> Lin(lg_of(expire.go(cap, bound, pc, mt, ws, now, evict(hs, pc, other(pc), bound, now)))):
  match mt:
    case Mt{st, m, ev}:
      settle_lin(Lg{cap, bound, PhRun{pc, Mt{st, m, Nat.add(ev, ev_count(evict(hs, pc, other(pc), bound, now)))}, ev_keep(evict(hs, pc, other(pc), bound, now))}, ws}, now, ev_fx(evict(hs, pc, other(pc), bound, now)), (excl_keep(pc, hs, other(pc), bound, now, cap, ex), (Unit{}, p)))

def expire_lin.run(+cap: Nat, +bound: Nat, +pc: Cls, +mt: Mt, +hs: +List<Hd>, +ws: +List<Wt>, +now: Nat, waits: Bool, i: Lin(Lg{cap, bound, PhRun{pc, mt, hs}, ws})) -> Lin(lg_of(expire.run(cap, bound, pc, mt, hs, ws, now, waits))):
  match waits:
    case False{}:
      i
    case True{}:
      (e, (n, (v, p))) = i
      expire_lin.go(cap, bound, pc, mt, hs, ws, now, e, p)

def expire_lin(+l: Lg, +now: Nat, i: Lin(l)) -> Lin(lg_of(expire(l, now))):
  match l:
    case Lg{cap, bound, PhIdle{}, ws}:
      i
    case Lg{+cap, +bound, PhRun{+pc, mt, hs}, +ws}:
      expire_lin.run(cap, bound, pc, mt, hs, ws, now, any_cls(other(pc), ws), i)

# --- the queue stays in arrival order through every input ------------------------------------------

def chain_top_succ(+ws: +List<Wt>, +lo: Nat, +top: Nat, ch: Chain(lo, ws, top)) -> Chain(lo, ws, 1n+top):
  match ws:
    case Nil{}:
      le_succ(lo, top, ch)
    case Con{w, r}:
      (lt, rest) = ch
      (lt, chain_top_succ(r, wt_ticket(w), top, rest))

# a pass takes waiters out of the queue and passes others over: what is left keeps its order
def admit_chain(+ws: +List<Wt>, +c: Cls, +cap: Nat, +now: Nat, +hs: +List<Hd>, +m: Nat, +fx: +List<Fx>, +adm: Bool, +room: Bool, +lo: Nat, +top: Nat, ch: Chain(lo, ws, top)) -> Chain(lo, adm_rest(admit(ws, c, cap, now, hs, m, fx, adm, room)), top):
  match ws:
    case Nil{}:
      ch
    case Con{Wt{+t, +wc, +sh}, +r}:
      match adm:
        case False{}:
          (lt, rest) = ch
          (lt, admit_chain(r, c, cap, now, hs, m, fx, head_adm(c, r), head_room(hs, r, cap), t, top, rest))
        case True{}:
          match room:
            case False{}:
              ch
            case True{}:
              (lt, rest) = ch
              chain_weaken(lo, t, adm_rest(admit(r, c, cap, now, Hd{t, wc, sh, now} <> hs, 1n+m, snoc_f(fx, FxGrant{t}), head_adm(c, r), head_room(Hd{t, wc, sh, now} <> hs, r, cap))), top, le_of_lt(lo, t, lt), admit_chain(r, c, cap, now, Hd{t, wc, sh, now} <> hs, 1n+m, snoc_f(fx, FxGrant{t}), head_adm(c, r), head_room(Hd{t, wc, sh, now} <> hs, r, cap), t, top, rest))

def drained_chain(+cap: Nat, +bound: Nat, +pc: Cls, ws: +List<Wt>, +now: Nat, +fx: +List<Fx>, ow: Bool, +top: Nat, ch: Chain(0n, ws, top)) -> Chain(0n, waiting_of(lg_of(settle.drained(cap, bound, pc, ws, now, fx, ow))), top):
  match ws ow:
    case Nil{} b0:
      Unit{}
    case Con{+w, +r} True{}:
      admit_chain(w <> r, other(pc), cap, now, [], 0n, fx, head_adm(other(pc), w <> r), head_room([], w <> r, cap), 0n, top, ch)
    case Con{+w, +r} False{}:
      admit_chain(w <> r, pc, cap, now, [], 0n, fx, head_adm(pc, w <> r), head_room([], w <> r, cap), 0n, top, ch)

def live_chain(+cap: Nat, +bound: Nat, +pc: Cls, mt: Mt, +hs: +List<Hd>, +ws: +List<Wt>, +now: Nat, +fx: +List<Fx>, ow: Bool, +top: Nat, ch: Chain(0n, ws, top)) -> Chain(0n, waiting_of(lg_of(settle.live(cap, bound, pc, mt, hs, ws, now, fx, ow))), top):
  match mt ow:
    case Mt{st, m, ev} True{}:
      ch
    case Mt{st, m, ev} False{}:
      admit_chain(ws, pc, cap, now, hs, m, fx, head_adm(pc, ws), head_room(hs, ws, cap), 0n, top, ch)

def run_chain(+cap: Nat, +bound: Nat, +pc: Cls, +mt: Mt, hs: +List<Hd>, +ws: +List<Wt>, +now: Nat, +fx: +List<Fx>, ow: Bool, +top: Nat, ch: Chain(0n, ws, top)) -> Chain(0n, waiting_of(lg_of(settle.run(cap, bound, pc, mt, hs, ws, now, fx, ow))), top):
  match hs:
    case Nil{}:
      drained_chain(cap, bound, pc, ws, now, snoc_f(fx, end_fx(pc, mt, now)), ow, top, ch)
    case Con{+h, +t}:
      live_chain(cap, bound, pc, mt, h <> t, ws, now, fx, ow, top, ch)

def settle_chain(+l: Lg, +now: Nat, +fx: +List<Fx>, +top: Nat, ch: Chain(0n, waiting_of(l), top)) -> Chain(0n, waiting_of(lg_of(settle(l, now, fx))), top):
  match l:
    case Lg{cap, bound, PhIdle{}, ws}:
      ch
    case Lg{+cap, +bound, PhRun{+pc, mt, hs}, +ws}:
      run_chain(cap, bound, pc, mt, hs, ws, now, fx, any_cls(other(pc), ws), top, ch)

def ask_chain.fit(+cap: Nat, +bound: Nat, +pc: Cls, +st: Nat, +m: Nat, +ev: Nat, +hs: +List<Hd>, +now: Nat, +last: Nat, +c: Cls, +s: Shr, adm: Bool, room: Bool, ch: Chain(0n, [], last)) -> Chain(0n, waiting_of(lg_of(ask.fit(cap, bound, pc, st, m, ev, hs, now, 1n+last, c, s, adm, room))), 1n+last):
  match adm:
    case False{}:
      chain_snoc([], 0n, last, c, s, ch)
    case True{}:
      match room:
        case False{}:
          chain_snoc([], 0n, last, c, s, ch)
        case True{}:
          Unit{}

def ask_chain.run(+cap: Nat, +bound: Nat, +pc: Cls, +st: Nat, +m: Nat, +ev: Nat, +hs: +List<Hd>, ws: +List<Wt>, +now: Nat, +last: Nat, +c: Cls, +s: Shr, ch: Chain(0n, ws, last)) -> Chain(0n, waiting_of(lg_of(ask.run(cap, bound, pc, st, m, ev, hs, ws, now, 1n+last, c, s))), 1n+last):
  match ws:
    case Nil{}:
      ask_chain.fit(cap, bound, pc, st, m, ev, hs, now, last, c, s, cls_same(pc, c), has_room(hs, s, cap), ch)
    case Con{+w, +r}:
      chain_snoc(w <> r, 0n, last, c, s, ch)

def ask_chain(+l: Lg, +now: Nat, +last: Nat, +c: Cls, +s: Shr, ch: Chain(0n, waiting_of(l), last)) -> Chain(0n, waiting_of(lg_of(ask(l, now, 1n+last, c, s))), 1n+last):
  match l:
    case Lg{cap, bound, PhIdle{}, ws}:
      chain_top_succ(ws, 0n, last, ch)
    case Lg{cap, bound, PhRun{pc, Mt{st, m, ev}, hs}, ws}:
      ask_chain.run(cap, bound, pc, st, m, ev, hs, ws, now, last, c, s, ch)

def leave_chain.hh(+cap: Nat, +bound: Nat, +pc: Cls, +mt: Mt, +hs: +List<Hd>, +ws: +List<Wt>, +now: Nat, +t: Nat, found: Bool, +top: Nat, ch: Chain(0n, ws, top)) -> Chain(0n, waiting_of(lg_of(leave.hh(cap, bound, pc, mt, hs, ws, now, t, found))), top):
  match found:
    case True{}:
      settle_chain(Lg{cap, bound, PhRun{pc, mt, rm_h(t, hs)}, ws}, now, [], top, ch)
    case False{}:
      ch

def leave_chain.h(+cap: Nat, +bound: Nat, ph: Ph, +ws: +List<Wt>, +now: Nat, +t: Nat, +top: Nat, ch: Chain(0n, ws, top)) -> Chain(0n, waiting_of(lg_of(leave.h(cap, bound, ph, ws, now, t))), top):
  match ph:
    case PhIdle{}:
      ch
    case PhRun{pc, mt, +hs}:
      leave_chain.hh(cap, bound, pc, mt, hs, ws, now, t, has_h(t, hs), top, ch)

def leave_chain.w(+cap: Nat, +bound: Nat, ph: Ph, +ws: +List<Wt>, +now: Nat, +t: Nat, found: Bool, +top: Nat, ch: Chain(0n, ws, top)) -> Chain(0n, waiting_of(lg_of(leave.w(cap, bound, ph, ws, now, t, found))), top):
  match found:
    case True{}:
      settle_chain(Lg{cap, bound, ph, rm_w(t, ws)}, now, [], top, chain_rm(ws, 0n, t, top, ch))
    case False{}:
      leave_chain.h(cap, bound, ph, ws, now, t, top, ch)

def leave_chain(+l: Lg, +now: Nat, +t: Nat, +top: Nat, ch: Chain(0n, waiting_of(l), top)) -> Chain(0n, waiting_of(lg_of(leave(l, now, t))), top):
  match l:
    case Lg{cap, bound, ph, +ws}:
      leave_chain.w(cap, bound, ph, ws, now, t, has_w(t, ws), top, ch)

def expire_chain.go(+cap: Nat, +bound: Nat, +pc: Cls, mt: Mt, +hs: +List<Hd>, +ws: +List<Wt>, +now: Nat, +top: Nat, ch: Chain(0n, ws, top)) -> Chain(0n, waiting_of(lg_of(expire.go(cap, bound, pc, mt, ws, now, evict(hs, pc, other(pc), bound, now)))), top):
  match mt:
    case Mt{st, m, ev}:
      settle_chain(Lg{cap, bound, PhRun{pc, Mt{st, m, Nat.add(ev, ev_count(evict(hs, pc, other(pc), bound, now)))}, ev_keep(evict(hs, pc, other(pc), bound, now))}, ws}, now, ev_fx(evict(hs, pc, other(pc), bound, now)), top, ch)

def expire_chain.run(+cap: Nat, +bound: Nat, +pc: Cls, +mt: Mt, +hs: +List<Hd>, +ws: +List<Wt>, +now: Nat, waits: Bool, +top: Nat, ch: Chain(0n, ws, top)) -> Chain(0n, waiting_of(lg_of(expire.run(cap, bound, pc, mt, hs, ws, now, waits))), top):
  match waits:
    case False{}:
      ch
    case True{}:
      expire_chain.go(cap, bound, pc, mt, hs, ws, now, top, ch)

def expire_chain(+l: Lg, +now: Nat, +top: Nat, ch: Chain(0n, waiting_of(l), top)) -> Chain(0n, waiting_of(lg_of(expire(l, now))), top):
  match l:
    case Lg{cap, bound, PhIdle{}, ws}:
      ch
    case Lg{+cap, +bound, PhRun{+pc, mt, hs}, +ws}:
      expire_chain.run(cap, bound, pc, mt, hs, ws, now, any_cls(other(pc), ws), top, ch)

# --- the invariants, kept by every input ------------------------------------------------------------

def Inv(s: St) -> Data:
  match s:
    case St{+l, +last, now, fx}:
      Sigma<&2, &2, Lin(l), u => Chain(0n, waiting_of(l), last)>

def inv_step(+o: Op, +s: St, i: Inv(s)) -> Inv(step(o, s)):
  match o s:
    case DAsk{+c, +sh} St{+lg, +last, +now, fx}:
      (lin, ch) = i
      (ask_lin(lg, now, 1n+last, c, sh, lin), ask_chain(lg, now, last, c, sh, ch))
    case DLeave{back} St{+lg, +last, +now, fx}:
      (lin, ch) = i
      (leave_lin(lg, now, monus(last, back), lin), leave_chain(lg, now, monus(last, back), last, ch))
    case DExpire{} St{+lg, last, +now, fx}:
      (lin, ch) = i
      (expire_lin(lg, now, lin), expire_chain(lg, now, last, ch))
    case DTick{} St{lg, last, now, fx}:
      i

def inv_run(+os: +List<Op>, +s: St, i: Inv(s)) -> Inv(run(os, s)):
  match os:
    case Nil{}:
      i
    case Con{o, t}:
      inv_run(t, step(o, s), inv_step(o, s, i))

def inv_empty(c0: Nat, b0: Nat) -> Inv(empty(c0, b0)):
  ((Unit{}, (Unit{}, (Unit{}, Unit{}))), Unit{})

def inv_of(+os: +List<Op>, +c0: Nat, +b0: Nat) -> Inv(run(os, empty(c0, b0))):
  inv_run(os, empty(c0, b0), inv_empty(c0, b0))

# --- laws (a) exclusion, (b) no phase means nobody waits, and FIFO ---------------------------------

def excl_of(+s: St, i: Inv(s)) -> IsT(excl_b(lg(s))):
  match s:
    case St{l, last, now, fx}:
      ((e, (n, (v, p))), ch) = i
      e

def nowait_of(+s: St, i: Inv(s)) -> IsT(nowait_b(lg(s))):
  match s:
    case St{l, last, now, fx}:
      ((e, (n, (v, p))), ch) = i
      n

def live_of(+s: St, i: Inv(s)) -> IsT(live_b(lg(s))):
  match s:
    case St{l, last, now, fx}:
      ((e, (n, (v, p))), ch) = i
      v

def chain_of(+s: St, i: Inv(s)) -> Chain(0n, waiting_of(lg(s)), last(s)):
  match s:
    case St{l, la, now, fx}:
      (lin, ch) = i
      ch

def pos_of(+s: St, i: Inv(s)) -> IsT(pos_b(lg(s))):
  match s:
    case St{l, last, now, fx}:
      ((e, (n, (v, p))), ch) = i
      p

# (a) Exclusion: after any inputs, every holder is of the running phase's class, and the places
# they take are at most the cap.
law exclusion:
  for +c0: Nat
  for +b0: Nat
  for +os: +List<Op>
  IsT(excl_b(lg(run(os, empty(c0, b0)))))

# (b) After any inputs, with no phase running nobody waits, and a running phase has a holder.
law no_phase_no_wait:
  for +c0: Nat
  for +b0: Nat
  for +os: +List<Op>
  IsT(nowait_b(lg(run(os, empty(c0, b0)))))

law phase_has_holder:
  for +c0: Nat
  for +b0: Nat
  for +os: +List<Op>
  IsT(live_b(lg(run(os, empty(c0, b0)))))

# The queue is in arrival order: tickets rise from the oldest waiter to the newest, none above
# the last one given. "Older" in the laws below is this order.
law fifo:
  for +c0: Nat
  for +b0: Nat
  for +os: +List<Op>
  Chain(0n, waiting_of(lg(run(os, empty(c0, b0)))), last(run(os, empty(c0, b0))))

def exclusion(c0, b0, os):
  excl_of(run(os, empty(c0, b0)), inv_of(os, c0, b0))

def no_phase_no_wait(c0, b0, os):
  nowait_of(run(os, empty(c0, b0)), inv_of(os, c0, b0))

def phase_has_holder(c0, b0, os):
  live_of(run(os, empty(c0, b0)), inv_of(os, c0, b0))

def fifo(c0, b0, os):
  chain_of(run(os, empty(c0, b0)), inv_of(os, c0, b0))

# the same laws as checks the driver evaluates on the code's own ledger

law exclusion_check:
  for +c0: Nat
  for +b0: Nat
  for +os: +List<Op>
  {excl_b(lg(run(os, empty(c0, b0)))) == True{} : Bool}

law no_phase_no_wait_check:
  for +c0: Nat
  for +b0: Nat
  for +os: +List<Op>
  {nowait_b(lg(run(os, empty(c0, b0)))) == True{} : Bool}

law phase_has_holder_check:
  for +c0: Nat
  for +b0: Nat
  for +os: +List<Op>
  {live_b(lg(run(os, empty(c0, b0)))) == True{} : Bool}

law fifo_check:
  for +c0: Nat
  for +b0: Nat
  for +os: +List<Op>
  {chain_b(waiting_of(lg(run(os, empty(c0, b0)))), 0n, last(run(os, empty(c0, b0)))) == True{} : Bool}

def exclusion_check(c0, b0, os):
  refl_t(excl_b(lg(run(os, empty(c0, b0)))), exclusion(c0, b0, os))

def no_phase_no_wait_check(c0, b0, os):
  refl_t(nowait_b(lg(run(os, empty(c0, b0)))), no_phase_no_wait(c0, b0, os))

def phase_has_holder_check(c0, b0, os):
  refl_t(live_b(lg(run(os, empty(c0, b0)))), phase_has_holder(c0, b0, os))

def fifo_check(c0, b0, os):
  refl_t(chain_b(waiting_of(lg(run(os, empty(c0, b0)))), 0n, last(run(os, empty(c0, b0)))), chain_refl(waiting_of(lg(run(os, empty(c0, b0)))), 0n, last(run(os, empty(c0, b0))), fifo(c0, b0, os)))

# --- (c) no overtaking: every holder is older than every waiter of its class --------------------

# every waiter of class k is newer than m
def allgt_b(+k: Cls, +m: Nat, ws: +List<Wt>) -> Bool:
  match ws:
    case Nil{}:
      True{}
    case Con{+w, r}:
      and(imp(cls_same(k, wt_cls(w)), le_b(1n+m, wt_ticket(w))), allgt_b(k, m, r))

# a holder: older than every waiter of the phase's class, and not newer than the last ticket given
def hp(+c: Cls, +last: Nat, +ws: +List<Wt>, +h: Hd) -> Bool:
  and(allgt_b(c, hd_ticket(h), ws), le_b(hd_ticket(h), last))

def hok_b(+c: Cls, +last: Nat, +ws: +List<Wt>, hs: +List<Hd>) -> Bool:
  match hs:
    case Nil{}:
      True{}
    case Con{+h, t}:
      and(hp(c, last, ws, h), hok_b(c, last, ws, t))

def hok_ph(l: Lg, +last: Nat) -> Bool:
  match l:
    case Lg{cap, bound, PhIdle{}, ws}:
      True{}
    case Lg{cap, bound, PhRun{c, mt, hs}, ws}:
      hok_b(c, last, ws, hs)

def chain_head_le(+ws: +List<Wt>, +lo: Nat, +top: Nat, ch: Chain(lo, ws, top)) -> LE(lo, top):
  match ws:
    case Nil{}:
      ch
    case Con{w, r}:
      (+lt, rest) = ch
      le_trans(lo, wt_ticket(w), top, le_of_lt(lo, wt_ticket(w), lt), chain_head_le(r, wt_ticket(w), top, rest))

# every waiter in a chain above m is newer than m
def chain_allgt(+k: Cls, +ws: +List<Wt>, +m: Nat, +top: Nat, ch: Chain(m, ws, top)) -> IsT(allgt_b(k, m, ws)):
  match ws:
    case Nil{}:
      Unit{}
    case Con{w, r}:
      (+lt, +rest) = ch
      and_t(imp(cls_same(k, wt_cls(w)), le_b(1n+m, wt_ticket(w))), allgt_b(k, m, r), imp_t(cls_same(k, wt_cls(w)), le_b(1n+m, wt_ticket(w)), lt), chain_allgt(k, r, m, top, chain_weaken(m, wt_ticket(w), r, top, le_of_lt(m, wt_ticket(w), lt), rest)))

def allgt_rm_w.if(+k: Cls, +m: Nat, +w: Wt, +r: +List<Wt>, +t: Nat, b: Bool, +p: IsT(allgt_b(k, m, w <> r)), ih: IsT(allgt_b(k, m, rm_w(t, r)))) -> IsT(allgt_b(k, m, rm_w.if(w, r, rm_w(t, r), b))):
  match b:
    case True{}:
      and_r(imp(cls_same(k, wt_cls(w)), le_b(1n+m, wt_ticket(w))), allgt_b(k, m, r), p)
    case False{}:
      and_t(imp(cls_same(k, wt_cls(w)), le_b(1n+m, wt_ticket(w))), allgt_b(k, m, rm_w(t, r)), and_l(imp(cls_same(k, wt_cls(w)), le_b(1n+m, wt_ticket(w))), allgt_b(k, m, r), p), ih)

def allgt_rm_w(+k: Cls, +m: Nat, +ws: +List<Wt>, +t: Nat, +p: IsT(allgt_b(k, m, ws))) -> IsT(allgt_b(k, m, rm_w(t, ws))):
  match ws:
    case Nil{}:
      Unit{}
    case Con{+w, +r}:
      allgt_rm_w.if(k, m, w, r, t, same(t, wt_ticket(w)), p, allgt_rm_w(k, m, r, t, and_r(imp(cls_same(k, wt_cls(w)), le_b(1n+m, wt_ticket(w))), allgt_b(k, m, r), p)))

def allgt_snoc(+k: Cls, +m: Nat, +ws: +List<Wt>, +top: Nat, +c: Cls, +s: Shr, +p: IsT(allgt_b(k, m, ws)), +le: LE(m, top)) -> IsT(allgt_b(k, m, snoc_w(ws, Wt{1n+top, c, s}))):
  match ws:
    case Nil{}:
      and_t(imp(cls_same(k, c), le_b(1n+m, 1n+top)), True{}, imp_t(cls_same(k, c), le_b(1n+m, 1n+top), le), Unit{})
    case Con{w, r}:
      and_t(imp(cls_same(k, wt_cls(w)), le_b(1n+m, wt_ticket(w))), allgt_b(k, m, snoc_w(r, Wt{1n+top, c, s})), and_l(imp(cls_same(k, wt_cls(w)), le_b(1n+m, wt_ticket(w))), allgt_b(k, m, r), p), allgt_snoc(k, m, r, top, c, s, and_r(imp(cls_same(k, wt_cls(w)), le_b(1n+m, wt_ticket(w))), allgt_b(k, m, r), p), le))

# a waiter of another class changes no holder's standing
def allgt_other(+k: Cls, +m: Nat, +w: Wt, +r: +List<Wt>, nk: IsF(cls_same(k, wt_cls(w))), p: IsT(allgt_b(k, m, r))) -> IsT(allgt_b(k, m, w <> r)):
  and_t(imp(cls_same(k, wt_cls(w)), le_b(1n+m, wt_ticket(w))), allgt_b(k, m, r), imp_f(cls_same(k, wt_cls(w)), le_b(1n+m, wt_ticket(w)), nk), p)

def allgt_tail(+k: Cls, +m: Nat, +w: Wt, +r: +List<Wt>, p: IsT(allgt_b(k, m, w <> r))) -> IsT(allgt_b(k, m, r)):
  and_r(imp(cls_same(k, wt_cls(w)), le_b(1n+m, wt_ticket(w))), allgt_b(k, m, r), p)

def hp_allgt(+c: Cls, +last: Nat, +ws: +List<Wt>, +h: Hd, p: IsT(hp(c, last, ws, h))) -> IsT(allgt_b(c, hd_ticket(h), ws)):
  and_l(allgt_b(c, hd_ticket(h), ws), le_b(hd_ticket(h), last), p)

def hp_le(+c: Cls, +last: Nat, +ws: +List<Wt>, +h: Hd, p: IsT(hp(c, last, ws, h))) -> IsT(le_b(hd_ticket(h), last)):
  and_r(allgt_b(c, hd_ticket(h), ws), le_b(hd_ticket(h), last), p)

def hp_t(+c: Cls, +last: Nat, +ws: +List<Wt>, +h: Hd, a: IsT(allgt_b(c, hd_ticket(h), ws)), l: IsT(le_b(hd_ticket(h), last))) -> IsT(hp(c, last, ws, h)):
  and_t(allgt_b(c, hd_ticket(h), ws), le_b(hd_ticket(h), last), a, l)

def hok_head(+c: Cls, +last: Nat, +ws: +List<Wt>, +h: Hd, +t: +List<Hd>, p: IsT(hok_b(c, last, ws, h <> t))) -> IsT(hp(c, last, ws, h)):
  and_l(hp(c, last, ws, h), hok_b(c, last, ws, t), p)

def hok_rest(+c: Cls, +last: Nat, +ws: +List<Wt>, +h: Hd, +t: +List<Hd>, p: IsT(hok_b(c, last, ws, h <> t))) -> IsT(hok_b(c, last, ws, t)):
  and_r(hp(c, last, ws, h), hok_b(c, last, ws, t), p)

def hok_cons(+c: Cls, +last: Nat, +ws: +List<Wt>, +h: Hd, +t: +List<Hd>, a: IsT(hp(c, last, ws, h)), b: IsT(hok_b(c, last, ws, t))) -> IsT(hok_b(c, last, ws, h <> t)):
  and_t(hp(c, last, ws, h), hok_b(c, last, ws, t), a, b)

def hok_rm_h.if(+c: Cls, +last: Nat, +ws: +List<Wt>, +h: Hd, +r: +List<Hd>, +t: Nat, b: Bool, +p: IsT(hok_b(c, last, ws, h <> r)), ih: IsT(hok_b(c, last, ws, rm_h(t, r)))) -> IsT(hok_b(c, last, ws, rm_h.if(h, r, rm_h(t, r), b))):
  match b:
    case True{}:
      hok_rest(c, last, ws, h, r, p)
    case False{}:
      hok_cons(c, last, ws, h, rm_h(t, r), hok_head(c, last, ws, h, r, p), ih)

def hok_rm_h(+c: Cls, +last: Nat, +ws: +List<Wt>, +hs: +List<Hd>, +t: Nat, +p: IsT(hok_b(c, last, ws, hs))) -> IsT(hok_b(c, last, ws, rm_h(t, hs))):
  match hs:
    case Nil{}:
      Unit{}
    case Con{+h, +r}:
      hok_rm_h.if(c, last, ws, h, r, t, same(t, hd_ticket(h)), p, hok_rm_h(c, last, ws, r, t, hok_rest(c, last, ws, h, r, p)))

def hok_keep.if(+c: Cls, +last: Nat, +ws: +List<Wt>, +h: Hd, +ec: Cls, +o: Cls, +now: Nat, +r: Ev, +t: +List<Hd>, b: Bool, +p: IsT(hok_b(c, last, ws, h <> t)), ih: IsT(hok_b(c, last, ws, ev_keep(r)))) -> IsT(hok_b(c, last, ws, ev_keep(evict.if(h, ec, o, now, r, b)))):
  match b:
    case True{}:
      ih
    case False{}:
      hok_cons(c, last, ws, h, ev_keep(r), hok_head(c, last, ws, h, t, p), ih)

def hok_keep(+c: Cls, +last: Nat, +ws: +List<Wt>, +hs: +List<Hd>, +ec: Cls, +o: Cls, +bound: Nat, +now: Nat, +p: IsT(hok_b(c, last, ws, hs))) -> IsT(hok_b(c, last, ws, ev_keep(evict(hs, ec, o, bound, now)))):
  match hs:
    case Nil{}:
      Unit{}
    case Con{+h, +t}:
      hok_keep.if(c, last, ws, h, ec, o, now, evict(t, ec, o, bound, now), t, le_b(bound, monus(now, hd_granted(h))), p, hok_keep(c, last, ws, t, ec, o, bound, now, hok_rest(c, last, ws, h, t, p)))

def hok_rm_w(+c: Cls, +last: Nat, +ws: +List<Wt>, +hs: +List<Hd>, +t: Nat, +p: IsT(hok_b(c, last, ws, hs))) -> IsT(hok_b(c, last, rm_w(t, ws), hs)):
  match hs:
    case Nil{}:
      Unit{}
    case Con{+h, +r}:
      hok_cons(c, last, rm_w(t, ws), h, r, hp_t(c, last, rm_w(t, ws), h, allgt_rm_w(c, hd_ticket(h), ws, t, hp_allgt(c, last, ws, h, hok_head(c, last, ws, h, r, p))), hp_le(c, last, ws, h, hok_head(c, last, ws, h, r, p))), hok_rm_w(c, last, ws, r, t, hok_rest(c, last, ws, h, r, p)))

def hok_snoc(+c: Cls, +last: Nat, +ws: +List<Wt>, +hs: +List<Hd>, +nc: Cls, +ns: Shr, +p: IsT(hok_b(c, last, ws, hs))) -> IsT(hok_b(c, 1n+last, snoc_w(ws, Wt{1n+last, nc, ns}), hs)):
  match hs:
    case Nil{}:
      Unit{}
    case Con{+h, +r}:
      hok_cons(c, 1n+last, snoc_w(ws, Wt{1n+last, nc, ns}), h, r, hp_t(c, 1n+last, snoc_w(ws, Wt{1n+last, nc, ns}), h, allgt_snoc(c, hd_ticket(h), ws, last, nc, ns, hp_allgt(c, last, ws, h, hok_head(c, last, ws, h, r, p)), hp_le(c, last, ws, h, hok_head(c, last, ws, h, r, p))), le_succ(hd_ticket(h), last, hp_le(c, last, ws, h, hok_head(c, last, ws, h, r, p)))), hok_snoc(c, last, ws, r, nc, ns, hok_rest(c, last, ws, h, r, p)))

def hok_succ(+c: Cls, +last: Nat, +ws: +List<Wt>, +hs: +List<Hd>, +p: IsT(hok_b(c, last, ws, hs))) -> IsT(hok_b(c, 1n+last, ws, hs)):
  match hs:
    case Nil{}:
      Unit{}
    case Con{+h, +r}:
      hok_cons(c, 1n+last, ws, h, r, hp_t(c, 1n+last, ws, h, hp_allgt(c, last, ws, h, hok_head(c, last, ws, h, r, p)), le_succ(hd_ticket(h), last, hp_le(c, last, ws, h, hok_head(c, last, ws, h, r, p)))), hok_succ(c, last, ws, r, hok_rest(c, last, ws, h, r, p)))

# a waiter of another class joins the front of the queue
def hok_other(+c: Cls, +last: Nat, +w: Wt, +r: +List<Wt>, +hs: +List<Hd>, +nk: IsF(cls_same(c, wt_cls(w))), +p: IsT(hok_b(c, last, r, hs))) -> IsT(hok_b(c, last, w <> r, hs)):
  match hs:
    case Nil{}:
      Unit{}
    case Con{+h, +t}:
      hok_cons(c, last, w <> r, h, t, hp_t(c, last, w <> r, h, allgt_other(c, hd_ticket(h), w, r, nk, hp_allgt(c, last, r, h, hok_head(c, last, r, h, t, p))), hp_le(c, last, r, h, hok_head(c, last, r, h, t, p))), hok_other(c, last, w, r, t, nk, hok_rest(c, last, r, h, t, p)))

# the oldest waiter leaves the queue: standing is kept
def hok_tail(+c: Cls, +last: Nat, +w: Wt, +r: +List<Wt>, +hs: +List<Hd>, +p: IsT(hok_b(c, last, w <> r, hs))) -> IsT(hok_b(c, last, r, hs)):
  match hs:
    case Nil{}:
      Unit{}
    case Con{+h, +t}:
      hok_cons(c, last, r, h, t, hp_t(c, last, r, h, allgt_tail(c, hd_ticket(h), w, r, hp_allgt(c, last, w <> r, h, hok_head(c, last, w <> r, h, t, p))), hp_le(c, last, w <> r, h, hok_head(c, last, w <> r, h, t, p))), hok_tail(c, last, w, r, t, hok_rest(c, last, w <> r, h, t, p)))

# --- the pass keeps every holder older than the waiters of its class ----------------------------

def admit_hok(+ws: +List<Wt>, +c: Cls, +cap: Nat, +now: Nat, +hs: +List<Hd>, +m: Nat, +fx: +List<Fx>, +adm: Bool, +room: Bool, ea: {head_adm(c, ws) == adm : Bool}, +lo: Nat, +top: Nat, +ch: Chain(lo, ws, top), +p: IsT(hok_b(c, top, ws, hs))) -> IsT(hok_b(c, top, adm_rest(admit(ws, c, cap, now, hs, m, fx, adm, room)), adm_holders(admit(ws, c, cap, now, hs, m, fx, adm, room)))):
  match ws:
    case Nil{}:
      p
    case Con{Wt{+t, +wc, +sh}, +r}:
      match adm:
        case False{}:
          (lt, +rest) = ch
          hok_other(c, top, Wt{t, wc, sh}, adm_rest(admit(r, c, cap, now, hs, m, fx, head_adm(c, r), head_room(hs, r, cap))), adm_holders(admit(r, c, cap, now, hs, m, fx, head_adm(c, r), head_room(hs, r, cap))), ff(cls_same(c, wc), ea), admit_hok(r, c, cap, now, hs, m, fx, head_adm(c, r), head_room(hs, r, cap), {==}, t, top, rest, hok_tail(c, top, Wt{t, wc, sh}, r, hs, p)))
        case True{}:
          match room:
            case False{}:
              p
            case True{}:
              (lt, +rest) = ch
              admit_hok(r, c, cap, now, Hd{t, wc, sh, now} <> hs, 1n+m, snoc_f(fx, FxGrant{t}), head_adm(c, r), head_room(Hd{t, wc, sh, now} <> hs, r, cap), {==}, t, top, rest, hok_cons(c, top, r, Hd{t, wc, sh, now}, hs, hp_t(c, top, r, Hd{t, wc, sh, now}, chain_allgt(c, r, t, top, rest), chain_head_le(r, t, top, rest)), hok_tail(c, top, Wt{t, wc, sh}, r, hs, p)))

def hok_start(+cap: Nat, +bound: Nat, +nc: Cls, +ws: +List<Wt>, +now: Nat, +fx: +List<Fx>, +top: Nat, ch: Chain(0n, ws, top)) -> IsT(hok_ph(lg_of(settle.start(cap, bound, nc, ws, now, fx)), top)):
  admit_hok(ws, nc, cap, now, [], 0n, fx, head_adm(nc, ws), head_room([], ws, cap), {==}, 0n, top, ch, Unit{})

def hok_drained(+cap: Nat, +bound: Nat, +pc: Cls, ws: +List<Wt>, +now: Nat, +fx: +List<Fx>, ow: Bool, +top: Nat, ch: Chain(0n, ws, top)) -> IsT(hok_ph(lg_of(settle.drained(cap, bound, pc, ws, now, fx, ow)), top)):
  match ws ow:
    case Nil{} b0:
      Unit{}
    case Con{+w, +r} True{}:
      hok_start(cap, bound, other(pc), w <> r, now, fx, top, ch)
    case Con{+w, +r} False{}:
      hok_start(cap, bound, pc, w <> r, now, fx, top, ch)

def hok_live(+cap: Nat, +bound: Nat, +pc: Cls, mt: Mt, +hs: +List<Hd>, +ws: +List<Wt>, +now: Nat, +fx: +List<Fx>, ow: Bool, +top: Nat, ch: Chain(0n, ws, top), +p: IsT(hok_b(pc, top, ws, hs))) -> IsT(hok_ph(lg_of(settle.live(cap, bound, pc, mt, hs, ws, now, fx, ow)), top)):
  match mt ow:
    case Mt{st, +m, ev} True{}:
      p
    case Mt{st, +m, ev} False{}:
      admit_hok(ws, pc, cap, now, hs, m, fx, head_adm(pc, ws), head_room(hs, ws, cap), {==}, 0n, top, ch, p)

def hok_run(+cap: Nat, +bound: Nat, +pc: Cls, +mt: Mt, hs: +List<Hd>, +ws: +List<Wt>, +now: Nat, +fx: +List<Fx>, ow: Bool, +top: Nat, ch: Chain(0n, ws, top), p: IsT(hok_b(pc, top, ws, hs))) -> IsT(hok_ph(lg_of(settle.run(cap, bound, pc, mt, hs, ws, now, fx, ow)), top)):
  match hs:
    case Nil{}:
      hok_drained(cap, bound, pc, ws, now, snoc_f(fx, end_fx(pc, mt, now)), ow, top, ch)
    case Con{+h, +t}:
      hok_live(cap, bound, pc, mt, h <> t, ws, now, fx, ow, top, ch, p)

def hok_settle(+l: Lg, +now: Nat, +fx: +List<Fx>, +top: Nat, ch: Chain(0n, waiting_of(l), top), p: IsT(hok_ph(l, top))) -> IsT(hok_ph(lg_of(settle(l, now, fx)), top)):
  match l:
    case Lg{cap, bound, PhIdle{}, ws}:
      Unit{}
    case Lg{+cap, +bound, PhRun{+pc, mt, hs}, +ws}:
      hok_run(cap, bound, pc, mt, hs, ws, now, fx, any_cls(other(pc), ws), top, ch, p)

# --- every input keeps it ----------------------------------------------------------------------------

def hok_ask.fit(+cap: Nat, +bound: Nat, +pc: Cls, +st: Nat, +m: Nat, +ev: Nat, +hs: +List<Hd>, +now: Nat, +last: Nat, +c: Cls, +s: Shr, adm: Bool, room: Bool, p: IsT(hok_b(pc, last, [], hs))) -> IsT(hok_ph(lg_of(ask.fit(cap, bound, pc, st, m, ev, hs, now, 1n+last, c, s, adm, room)), 1n+last)):
  match adm:
    case False{}:
      hok_snoc(pc, last, [], hs, c, s, p)
    case True{}:
      match room:
        case False{}:
          hok_snoc(pc, last, [], hs, c, s, p)
        case True{}:
          hok_cons(pc, 1n+last, [], Hd{1n+last, c, s, now}, hs, hp_t(pc, 1n+last, [], Hd{1n+last, c, s, now}, Unit{}, le_refl(1n+last)), hok_succ(pc, last, [], hs, p))

def hok_ask.run(+cap: Nat, +bound: Nat, +pc: Cls, +st: Nat, +m: Nat, +ev: Nat, +hs: +List<Hd>, ws: +List<Wt>, +now: Nat, +last: Nat, +c: Cls, +s: Shr, p: IsT(hok_b(pc, last, ws, hs))) -> IsT(hok_ph(lg_of(ask.run(cap, bound, pc, st, m, ev, hs, ws, now, 1n+last, c, s)), 1n+last)):
  match ws:
    case Nil{}:
      hok_ask.fit(cap, bound, pc, st, m, ev, hs, now, last, c, s, cls_same(pc, c), has_room(hs, s, cap), p)
    case Con{+w, +r}:
      hok_snoc(pc, last, w <> r, hs, c, s, p)

def hok_ask(+l: Lg, +now: Nat, +last: Nat, +c: Cls, +s: Shr, nw: IsT(nowait_b(l)), p: IsT(hok_ph(l, last))) -> IsT(hok_ph(lg_of(ask(l, now, 1n+last, c, s)), 1n+last)):
  match l:
    case Lg{cap, bound, PhIdle{}, Nil{}}:
      and_t(hp(c, 1n+last, [], Hd{1n+last, c, s, now}), True{}, hp_t(c, 1n+last, [], Hd{1n+last, c, s, now}, Unit{}, le_refl(1n+last)), Unit{})
    case Lg{cap, bound, PhIdle{}, Con{w, r}}:
      absurd(IsT(hok_ph(lg_of(ask(Lg{cap, bound, PhIdle{}, w <> r}, now, 1n+last, c, s)), 1n+last)), nw)
    case Lg{cap, bound, PhRun{pc, Mt{st, m, ev}, hs}, ws}:
      hok_ask.run(cap, bound, pc, st, m, ev, hs, ws, now, last, c, s, p)

def hok_leave.hh(+cap: Nat, +bound: Nat, +pc: Cls, +mt: Mt, +hs: +List<Hd>, +ws: +List<Wt>, +now: Nat, +t: Nat, found: Bool, +top: Nat, ch: Chain(0n, ws, top), p: IsT(hok_b(pc, top, ws, hs))) -> IsT(hok_ph(lg_of(leave.hh(cap, bound, pc, mt, hs, ws, now, t, found)), top)):
  match found:
    case True{}:
      hok_settle(Lg{cap, bound, PhRun{pc, mt, rm_h(t, hs)}, ws}, now, [], top, ch, hok_rm_h(pc, top, ws, hs, t, p))
    case False{}:
      p

def hok_leave.h(+cap: Nat, +bound: Nat, ph: Ph, +ws: +List<Wt>, +now: Nat, +t: Nat, +top: Nat, ch: Chain(0n, ws, top), p: IsT(hok_ph(Lg{cap, bound, ph, ws}, top))) -> IsT(hok_ph(lg_of(leave.h(cap, bound, ph, ws, now, t)), top)):
  match ph:
    case PhIdle{}:
      p
    case PhRun{pc, mt, +hs}:
      hok_leave.hh(cap, bound, pc, mt, hs, ws, now, t, has_h(t, hs), top, ch, p)

def hok_leave.w(+cap: Nat, +bound: Nat, ph: Ph, +ws: +List<Wt>, +now: Nat, +t: Nat, found: Bool, +top: Nat, ch: Chain(0n, ws, top), p: IsT(hok_ph(Lg{cap, bound, ph, ws}, top))) -> IsT(hok_ph(lg_of(leave.w(cap, bound, ph, ws, now, t, found)), top)):
  match ph found:
    case PhIdle{} True{}:
      hok_settle(Lg{cap, bound, PhIdle{}, rm_w(t, ws)}, now, [], top, chain_rm(ws, 0n, t, top, ch), Unit{})
    case PhRun{+pc, mt, +hs} True{}:
      hok_settle(Lg{cap, bound, PhRun{pc, mt, hs}, rm_w(t, ws)}, now, [], top, chain_rm(ws, 0n, t, top, ch), hok_rm_w(pc, top, ws, hs, t, p))
    case p0 False{}:
      hok_leave.h(cap, bound, p0, ws, now, t, top, ch, p)

def hok_leave(+l: Lg, +now: Nat, +t: Nat, +top: Nat, ch: Chain(0n, waiting_of(l), top), p: IsT(hok_ph(l, top))) -> IsT(hok_ph(lg_of(leave(l, now, t)), top)):
  match l:
    case Lg{cap, bound, ph, +ws}:
      hok_leave.w(cap, bound, ph, ws, now, t, has_w(t, ws), top, ch, p)

def hok_expire.run(+cap: Nat, +bound: Nat, +pc: Cls, mt: Mt, +hs: +List<Hd>, +ws: +List<Wt>, +now: Nat, waits: Bool, +top: Nat, ch: Chain(0n, ws, top), p: IsT(hok_b(pc, top, ws, hs))) -> IsT(hok_ph(lg_of(expire.run(cap, bound, pc, mt, hs, ws, now, waits)), top)):
  match mt waits:
    case Mt{st, m, ev} False{}:
      p
    case Mt{st, m, ev} True{}:
      hok_settle(Lg{cap, bound, PhRun{pc, Mt{st, m, Nat.add(ev, ev_count(evict(hs, pc, other(pc), bound, now)))}, ev_keep(evict(hs, pc, other(pc), bound, now))}, ws}, now, ev_fx(evict(hs, pc, other(pc), bound, now)), top, ch, hok_keep(pc, top, ws, hs, pc, other(pc), bound, now, p))

def hok_expire(+l: Lg, +now: Nat, +top: Nat, ch: Chain(0n, waiting_of(l), top), p: IsT(hok_ph(l, top))) -> IsT(hok_ph(lg_of(expire(l, now)), top)):
  match l:
    case Lg{cap, bound, PhIdle{}, ws}:
      p
    case Lg{+cap, +bound, PhRun{+pc, mt, hs}, +ws}:
      hok_expire.run(cap, bound, pc, mt, hs, ws, now, any_cls(other(pc), ws), top, ch, p)

def Fair(+s: St) -> Data:
  IsT(hok_ph(lg(s), last(s)))

def fair_step(+o: Op, +s: St, +i: Inv(s), f: Fair(s)) -> Fair(step(o, s)):
  match o s:
    case DAsk{+c, +sh} St{+l, +la, +now, fx}:
      ((e, (n, (v, pp))), ch) = i
      hok_ask(l, now, la, c, sh, n, f)
    case DLeave{back} St{+l, +la, +now, fx}:
      ((e, (n, (v, pp))), ch) = i
      hok_leave(l, now, monus(la, back), la, ch, f)
    case DExpire{} St{+l, +la, +now, fx}:
      ((e, (n, (v, pp))), ch) = i
      hok_expire(l, now, la, ch, f)
    case DTick{} St{l, la, now, fx}:
      f

def fair_run(+os: +List<Op>, +s: St, +i: Inv(s), f: Fair(s)) -> Fair(run(os, s)):
  match os:
    case Nil{}:
      f
    case Con{o, t}:
      fair_run(t, step(o, s), inv_step(o, s, i), fair_step(o, s, i, f))

def fair_empty(c0: Nat, b0: Nat) -> Fair(empty(c0, b0)):
  Unit{}

def fair_of(+os: +List<Op>, +c0: Nat, +b0: Nat) -> Fair(run(os, empty(c0, b0))):
  fair_run(os, empty(c0, b0), inv_empty(c0, b0), fair_empty(c0, b0))

# --- (d) no starvation: while a waiter of the other class waits, the running phase admits nobody new ---

def or_f_r(a: Bool, b: Bool, p: IsF(or(a, b))) -> IsF(b):
  match a:
    case True{}:
      absurd(IsF(b), p)
    case False{}:
      p

def or_f_t(a: Bool, b: Bool, pa: IsF(a), pb: IsF(b)) -> IsF(or(a, b)):
  match a:
    case True{}:
      absurd(IsF(or(True{}, b)), pa)
    case False{}:
      pb

def and_f_r(a: Bool, b: Bool, pb: IsF(b)) -> IsF(and(a, b)):
  match a:
    case True{}:
      pb
    case False{}:
      Unit{}

def not_t(b: Bool, pf: IsF(b)) -> IsT(not(b)):
  match b:
    case True{}:
      absurd(IsT(not(True{})), pf)
    case False{}:
      Unit{}

def not_f(b: Bool, pt: IsT(b)) -> IsF(not(b)):
  match b:
    case True{}:
      Unit{}
    case False{}:
      absurd(IsF(not(False{})), pt)

def f_of_not(b: Bool, p: IsT(not(b))) -> IsF(b):
  match b:
    case True{}:
      absurd(IsF(True{}), p)
    case False{}:
      Unit{}

# the pass leaves no waiter of a class that none of its input had
def anyf_rest(+ws: +List<Wt>, +c: Cls, +cap: Nat, +now: Nat, +hs: +List<Hd>, +m: Nat, +fx: +List<Fx>, +adm: Bool, +room: Bool, +k: Cls, +p: IsF(any_cls(k, ws))) -> IsF(any_cls(k, adm_rest(admit(ws, c, cap, now, hs, m, fx, adm, room)))):
  match ws:
    case Nil{}:
      p
    case Con{Wt{+t, +wc, +sh}, +r}:
      match adm:
        case False{}:
          or_f_t(cls_same(k, wc), any_cls(k, adm_rest(admit(r, c, cap, now, hs, m, fx, head_adm(c, r), head_room(hs, r, cap)))), or_f_l(cls_same(k, wc), any_cls(k, r), p), anyf_rest(r, c, cap, now, hs, m, fx, head_adm(c, r), head_room(hs, r, cap), k, or_f_r(cls_same(k, wc), any_cls(k, r), p)))
        case True{}:
          match room:
            case False{}:
              p
            case True{}:
              anyf_rest(r, c, cap, now, Hd{t, wc, sh, now} <> hs, 1n+m, snoc_f(fx, FxGrant{t}), head_adm(c, r), head_room(Hd{t, wc, sh, now} <> hs, r, cap), k, or_f_r(cls_same(k, wc), any_cls(k, r), p))

# the phase of nc that settle starts: no one is admitted into a phase another class waits for
def starve_after(+pc: Cls, +st: Nat, +m: Nat, b: Lg) -> Bool:
  match b:
    case Lg{cap, bound, PhIdle{}, ws}:
      True{}
    case Lg{cap, bound, PhRun{+pc2, Mt{+st2, +m2, ev2}, hs2}, +ws2}:
      imp(and(and(cls_same(pc, pc2), same(st, st2)), any_cls(other(pc2), ws2)), same(m, m2))

def starve_b(a: Lg, b: Lg) -> Bool:
  match a:
    case Lg{cap, bound, PhIdle{}, ws}:
      True{}
    case Lg{cap, bound, PhRun{pc, Mt{st, m, ev}, hs}, ws}:
      starve_after(pc, st, m, b)

def starve_start.go(+bb: Bool, +st: Nat, +now: Nat, +m: Nat, +mem: Nat, +ow: Bool, +ww: Bool, +rw: Bool, cond: IsT(imp(bb, not(ww))), rw_f: IsF(ww) -> IsF(rw)) -> IsT(imp(and(and(bb, same(st, now)), rw), same(m, mem))):
  match bb:
    case False{}:
      imp_f(and(and(False{}, same(st, now)), rw), same(m, mem), Unit{})
    case True{}:
      imp_f(and(and(True{}, same(st, now)), rw), same(m, mem), and_f_r(and(True{}, same(st, now)), rw, rw_f(f_of_not(ww, cond))))

def starve_start(+pc: Cls, +st: Nat, +m: Nat, +cap: Nat, +bound: Nat, +nc: Cls, +ws: +List<Wt>, +now: Nat, +fx: +List<Fx>, cond: IsT(imp(cls_same(pc, nc), not(any_cls(other(nc), ws))))) -> IsT(starve_after(pc, st, m, lg_of(settle.start(cap, bound, nc, ws, now, fx)))):
  starve_start.go(cls_same(pc, nc), st, now, m, adm_members(admit(ws, nc, cap, now, [], 0n, fx, head_adm(nc, ws), head_room([], ws, cap))), True{}, any_cls(other(nc), ws), any_cls(other(nc), adm_rest(admit(ws, nc, cap, now, [], 0n, fx, head_adm(nc, ws), head_room([], ws, cap)))), cond, g => anyf_rest(ws, nc, cap, now, [], 0n, fx, head_adm(nc, ws), head_room([], ws, cap), other(nc), g))

def starve_drained(+st: Nat, +m: Nat, +cap: Nat, +bound: Nat, +pc: Cls, ws: +List<Wt>, +now: Nat, +fx: +List<Fx>, ow: Bool, ew: {any_cls(other(pc), ws) == ow : Bool}) -> IsT(starve_after(pc, st, m, lg_of(settle.drained(cap, bound, pc, ws, now, fx, ow)))):
  match ws ow:
    case Nil{} b0:
      Unit{}
    case Con{+w, +r} True{}:
      starve_start(pc, st, m, cap, bound, other(pc), w <> r, now, fx, imp_f(cls_same(pc, other(pc)), not(any_cls(other(other(pc)), w <> r)), cls_same_other(pc)))
    case Con{+w, +r} False{}:
      starve_start(pc, st, m, cap, bound, pc, w <> r, now, fx, imp_t(cls_same(pc, pc), not(any_cls(other(pc), w <> r)), not_t(any_cls(other(pc), w <> r), ff(any_cls(other(pc), w <> r), ew))))

def starve_live(+st: Nat, +m: Nat, +ev: Nat, +cap: Nat, +bound: Nat, +pc: Cls, +hs: +List<Hd>, +ws: +List<Wt>, +now: Nat, +fx: +List<Fx>, ow: Bool, ew: {any_cls(other(pc), ws) == ow : Bool}) -> IsT(starve_after(pc, st, m, lg_of(settle.live(cap, bound, pc, Mt{st, m, ev}, hs, ws, now, fx, ow)))):
  match ow:
    case True{}:
      imp_t(and(and(cls_same(pc, pc), same(st, st)), any_cls(other(pc), ws)), same(m, m), same_refl(m))
    case False{}:
      starve_start.go(cls_same(pc, pc), st, st, m, adm_members(admit(ws, pc, cap, now, hs, m, fx, head_adm(pc, ws), head_room(hs, ws, cap))), True{}, any_cls(other(pc), ws), any_cls(other(pc), adm_rest(admit(ws, pc, cap, now, hs, m, fx, head_adm(pc, ws), head_room(hs, ws, cap)))), imp_t(cls_same(pc, pc), not(any_cls(other(pc), ws)), not_t(any_cls(other(pc), ws), ff(any_cls(other(pc), ws), ew))), g => anyf_rest(ws, pc, cap, now, hs, m, fx, head_adm(pc, ws), head_room(hs, ws, cap), other(pc), g))

def starve_run(+st: Nat, +m: Nat, +ev: Nat, +cap: Nat, +bound: Nat, +pc: Cls, hs: +List<Hd>, +ws: +List<Wt>, +now: Nat, +fx: +List<Fx>, ow: Bool, ew: {any_cls(other(pc), ws) == ow : Bool}) -> IsT(starve_after(pc, st, m, lg_of(settle.run(cap, bound, pc, Mt{st, m, ev}, hs, ws, now, fx, ow)))):
  match hs:
    case Nil{}:
      starve_drained(st, m, cap, bound, pc, ws, now, snoc_f(fx, end_fx(pc, Mt{st, m, ev}, now)), ow, ew)
    case Con{+h, +t}:
      starve_live(st, m, ev, cap, bound, pc, h <> t, ws, now, fx, ow, ew)

# settle, from a phase of pc started at st that has admitted m
def starve_settle(+st: Nat, +m: Nat, +ev: Nat, +cap: Nat, +bound: Nat, +pc: Cls, +hs: +List<Hd>, +ws: +List<Wt>, +now: Nat, +fx: +List<Fx>) -> IsT(starve_after(pc, st, m, lg_of(settle(Lg{cap, bound, PhRun{pc, Mt{st, m, ev}, hs}, ws}, now, fx)))):
  starve_run(st, m, ev, cap, bound, pc, hs, ws, now, fx, any_cls(other(pc), ws), {==})

def starve_same(+st: Nat, +m: Nat, +ev: Nat, +cap: Nat, +bound: Nat, +pc: Cls, +hs: +List<Hd>, +ws: +List<Wt>) -> IsT(starve_after(pc, st, m, Lg{cap, bound, PhRun{pc, Mt{st, m, ev}, hs}, ws})):
  imp_t(and(and(cls_same(pc, pc), same(st, st)), any_cls(other(pc), ws)), same(m, m), same_refl(m))

def starve_ask.fit(+cap: Nat, +bound: Nat, +pc: Cls, +st: Nat, +m: Nat, +ev: Nat, +hs: +List<Hd>, +now: Nat, +t: Nat, +c: Cls, +s: Shr, adm: Bool, room: Bool) -> IsT(starve_after(pc, st, m, lg_of(ask.fit(cap, bound, pc, st, m, ev, hs, now, t, c, s, adm, room)))):
  match adm:
    case False{}:
      starve_same(st, m, ev, cap, bound, pc, hs, [Wt{t, c, s}])
    case True{}:
      match room:
        case False{}:
          starve_same(st, m, ev, cap, bound, pc, hs, [Wt{t, c, s}])
        case True{}:
          imp_f(and(and(cls_same(pc, pc), same(st, st)), any_cls(other(pc), [])), same(m, 1n+m), and_f_r(and(cls_same(pc, pc), same(st, st)), any_cls(other(pc), []), Unit{}))

def starve_ask.run(+cap: Nat, +bound: Nat, +pc: Cls, +st: Nat, +m: Nat, +ev: Nat, +hs: +List<Hd>, ws: +List<Wt>, +now: Nat, +t: Nat, +c: Cls, +s: Shr) -> IsT(starve_after(pc, st, m, lg_of(ask.run(cap, bound, pc, st, m, ev, hs, ws, now, t, c, s)))):
  match ws:
    case Nil{}:
      starve_ask.fit(cap, bound, pc, st, m, ev, hs, now, t, c, s, cls_same(pc, c), has_room(hs, s, cap))
    case Con{+w, +r}:
      starve_same(st, m, ev, cap, bound, pc, hs, snoc_w(w <> r, Wt{t, c, s}))

def starve_ask(+l: Lg, +now: Nat, +t: Nat, +c: Cls, +s: Shr) -> IsT(starve_b(l, lg_of(ask(l, now, t, c, s)))):
  match l:
    case Lg{cap, bound, PhIdle{}, ws}:
      Unit{}
    case Lg{cap, bound, PhRun{pc, Mt{st, m, ev}, hs}, ws}:
      starve_ask.run(cap, bound, pc, st, m, ev, hs, ws, now, t, c, s)

def starve_leave.hh(+cap: Nat, +bound: Nat, +pc: Cls, +st: Nat, +m: Nat, +ev: Nat, +hs: +List<Hd>, +ws: +List<Wt>, +now: Nat, +t: Nat, found: Bool) -> IsT(starve_after(pc, st, m, lg_of(leave.hh(cap, bound, pc, Mt{st, m, ev}, hs, ws, now, t, found)))):
  match found:
    case True{}:
      starve_settle(st, m, ev, cap, bound, pc, rm_h(t, hs), ws, now, [])
    case False{}:
      starve_same(st, m, ev, cap, bound, pc, hs, ws)

def starve_leave.w(+cap: Nat, +bound: Nat, +pc: Cls, +st: Nat, +m: Nat, +ev: Nat, +hs: +List<Hd>, +ws: +List<Wt>, +now: Nat, +t: Nat, found: Bool) -> IsT(starve_after(pc, st, m, lg_of(leave.w(cap, bound, PhRun{pc, Mt{st, m, ev}, hs}, ws, now, t, found)))):
  match found:
    case True{}:
      starve_settle(st, m, ev, cap, bound, pc, hs, rm_w(t, ws), now, [])
    case False{}:
      starve_leave.hh(cap, bound, pc, st, m, ev, hs, ws, now, t, has_h(t, hs))

def starve_leave(+l: Lg, +now: Nat, +t: Nat) -> IsT(starve_b(l, lg_of(leave(l, now, t)))):
  match l:
    case Lg{cap, bound, PhIdle{}, ws}:
      Unit{}
    case Lg{cap, bound, PhRun{pc, Mt{st, m, ev}, hs}, +ws}:
      starve_leave.w(cap, bound, pc, st, m, ev, hs, ws, now, t, has_w(t, ws))

def starve_expire.run(+cap: Nat, +bound: Nat, +pc: Cls, +st: Nat, +m: Nat, +ev: Nat, +hs: +List<Hd>, +ws: +List<Wt>, +now: Nat, waits: Bool) -> IsT(starve_after(pc, st, m, lg_of(expire.run(cap, bound, pc, Mt{st, m, ev}, hs, ws, now, waits)))):
  match waits:
    case False{}:
      starve_same(st, m, ev, cap, bound, pc, hs, ws)
    case True{}:
      starve_settle(st, m, Nat.add(ev, ev_count(evict(hs, pc, other(pc), bound, now))), cap, bound, pc, ev_keep(evict(hs, pc, other(pc), bound, now)), ws, now, ev_fx(evict(hs, pc, other(pc), bound, now)))

def starve_expire(+l: Lg, +now: Nat) -> IsT(starve_b(l, lg_of(expire(l, now)))):
  match l:
    case Lg{cap, bound, PhIdle{}, ws}:
      Unit{}
    case Lg{cap, bound, PhRun{pc, Mt{st, m, ev}, hs}, +ws}:
      starve_expire.run(cap, bound, pc, st, m, ev, hs, ws, now, any_cls(other(pc), ws))

def starve_refl(+l: Lg) -> IsT(starve_b(l, l)):
  match l:
    case Lg{cap, bound, PhIdle{}, ws}:
      Unit{}
    case Lg{cap, bound, PhRun{pc, Mt{st, m, ev}, hs}, ws}:
      starve_same(st, m, ev, cap, bound, pc, hs, ws)

def starve_step(+o: Op, +s: St) -> IsT(starve_b(lg(s), lg(step(o, s)))):
  match o s:
    case DAsk{c, sh} St{+l, +la, +now, fx}:
      starve_ask(l, now, 1n+la, c, sh)
    case DLeave{back} St{+l, +la, +now, fx}:
      starve_leave(l, now, monus(la, back))
    case DExpire{} St{+l, la, +now, fx}:
      starve_expire(l, now)
    case DTick{} St{l, la, now, fx}:
      starve_refl(l)

# (d) No starvation: after any inputs, whatever the next input is, if the same phase is still running
# afterwards and a waiter of the other class waits, the input admitted nobody into it.
law no_starvation:
  for +c0: Nat
  for +b0: Nat
  for +os: +List<Op>
  for +o: Op
  IsT(starve_b(lg(run(os, empty(c0, b0))), lg(step(o, run(os, empty(c0, b0))))))

def no_starvation(c0, b0, os, o):
  starve_step(o, run(os, empty(c0, b0)))

# --- (e) bounded hold: after expire, no holder past the bound remains while the other class waits ---

def young_b(+bound: Nat, +now: Nat, hs: +List<Hd>) -> Bool:
  match hs:
    case Nil{}:
      True{}
    case Con{+h, t}:
      and(not(le_b(bound, monus(now, hd_granted(h)))), young_b(bound, now, t))

# the phase's holders have all been held less than the bound
def young_u(l: Lg, +now: Nat) -> Bool:
  match l:
    case Lg{cap, bound, PhIdle{}, ws}:
      True{}
    case Lg{cap, bound, PhRun{c, mt, hs}, ws}:
      young_b(bound, now, hs)

# while the other class waits, the phase's holders have all been held less than the bound
def young_after(l: Lg, +now: Nat) -> Bool:
  match l:
    case Lg{cap, bound, PhIdle{}, ws}:
      True{}
    case Lg{cap, bound, PhRun{+pc, mt, hs}, +ws}:
      imp(any_cls(other(pc), ws), young_b(bound, now, hs))

def young_now(+now: Nat, +bound: Nat, pb: IsT(pos_n(bound))) -> IsT(not(le_b(bound, monus(now, now)))):
  match now bound:
    case 0n 0n:
      absurd(IsT(not(le_b(0n, monus(0n, 0n)))), pb)
    case 0n 1n+p:
      Unit{}
    case 1n+q 0n:
      absurd(IsT(not(le_b(0n, monus(1n+q, 1n+q)))), pb)
    case 1n+q 1n+p:
      young_now(q, 1n+p, pb)

def young_head(+bound: Nat, +now: Nat, +h: Hd, +t: +List<Hd>, p: IsT(young_b(bound, now, h <> t))) -> IsT(not(le_b(bound, monus(now, hd_granted(h))))):
  and_l(not(le_b(bound, monus(now, hd_granted(h)))), young_b(bound, now, t), p)

def young_tail(+bound: Nat, +now: Nat, +h: Hd, +t: +List<Hd>, p: IsT(young_b(bound, now, h <> t))) -> IsT(young_b(bound, now, t)):
  and_r(not(le_b(bound, monus(now, hd_granted(h)))), young_b(bound, now, t), p)

# what an eviction keeps has been held less than the bound
def young_keep.if(+h: Hd, +ec: Cls, +o: Cls, +bound: Nat, +now: Nat, +r: Ev, b: Bool, eb: {le_b(bound, monus(now, hd_granted(h))) == b : Bool}, ih: IsT(young_b(bound, now, ev_keep(r)))) -> IsT(young_b(bound, now, ev_keep(evict.if(h, ec, o, now, r, b)))):
  match b:
    case True{}:
      ih
    case False{}:
      and_t(not(le_b(bound, monus(now, hd_granted(h)))), young_b(bound, now, ev_keep(r)), not_t(le_b(bound, monus(now, hd_granted(h))), ff(le_b(bound, monus(now, hd_granted(h))), eb)), ih)

def young_keep(+hs: +List<Hd>, +ec: Cls, +o: Cls, +bound: Nat, +now: Nat) -> IsT(young_b(bound, now, ev_keep(evict(hs, ec, o, bound, now)))):
  match hs:
    case Nil{}:
      Unit{}
    case Con{+h, +t}:
      young_keep.if(h, ec, o, bound, now, evict(t, ec, o, bound, now), le_b(bound, monus(now, hd_granted(h))), {==}, young_keep(t, ec, o, bound, now))

# the pass admits at now: its holders are young
def admit_young(+ws: +List<Wt>, +c: Cls, +cap: Nat, +now: Nat, +hs: +List<Hd>, +m: Nat, +fx: +List<Fx>, +adm: Bool, +room: Bool, +bound: Nat, +pb: IsT(pos_n(bound)), +p: IsT(young_b(bound, now, hs))) -> IsT(young_b(bound, now, adm_holders(admit(ws, c, cap, now, hs, m, fx, adm, room)))):
  match ws:
    case Nil{}:
      p
    case Con{Wt{+t, +wc, +sh}, +r}:
      match adm:
        case False{}:
          admit_young(r, c, cap, now, hs, m, fx, head_adm(c, r), head_room(hs, r, cap), bound, pb, p)
        case True{}:
          match room:
            case False{}:
              p
            case True{}:
              admit_young(r, c, cap, now, Hd{t, wc, sh, now} <> hs, 1n+m, snoc_f(fx, FxGrant{t}), head_adm(c, r), head_room(Hd{t, wc, sh, now} <> hs, r, cap), bound, pb, and_t(not(le_b(bound, monus(now, now))), young_b(bound, now, hs), young_now(now, bound, pb), p))

def young_start(+cap: Nat, +bound: Nat, +nc: Cls, +ws: +List<Wt>, +now: Nat, +fx: +List<Fx>, pb: IsT(pos_n(bound))) -> IsT(young_u(lg_of(settle.start(cap, bound, nc, ws, now, fx)), now)):
  admit_young(ws, nc, cap, now, [], 0n, fx, head_adm(nc, ws), head_room([], ws, cap), bound, pb, Unit{})

def young_drained(+cap: Nat, +bound: Nat, +pc: Cls, ws: +List<Wt>, +now: Nat, +fx: +List<Fx>, ow: Bool, pb: IsT(pos_n(bound))) -> IsT(young_u(lg_of(settle.drained(cap, bound, pc, ws, now, fx, ow)), now)):
  match ws ow:
    case Nil{} b0:
      Unit{}
    case Con{+w, +r} True{}:
      young_start(cap, bound, other(pc), w <> r, now, fx, pb)
    case Con{+w, +r} False{}:
      young_start(cap, bound, pc, w <> r, now, fx, pb)

def young_live(+cap: Nat, +bound: Nat, +pc: Cls, mt: Mt, +hs: +List<Hd>, +ws: +List<Wt>, +now: Nat, +fx: +List<Fx>, ow: Bool, pb: IsT(pos_n(bound)), +p: IsT(young_b(bound, now, hs))) -> IsT(young_u(lg_of(settle.live(cap, bound, pc, mt, hs, ws, now, fx, ow)), now)):
  match mt ow:
    case Mt{st, +m, ev} True{}:
      p
    case Mt{st, +m, ev} False{}:
      admit_young(ws, pc, cap, now, hs, m, fx, head_adm(pc, ws), head_room(hs, ws, cap), bound, pb, p)

def young_run(+cap: Nat, +bound: Nat, +pc: Cls, +mt: Mt, hs: +List<Hd>, +ws: +List<Wt>, +now: Nat, +fx: +List<Fx>, ow: Bool, pb: IsT(pos_n(bound)), p: IsT(young_b(bound, now, hs))) -> IsT(young_u(lg_of(settle.run(cap, bound, pc, mt, hs, ws, now, fx, ow)), now)):
  match hs:
    case Nil{}:
      young_drained(cap, bound, pc, ws, now, snoc_f(fx, end_fx(pc, mt, now)), ow, pb)
    case Con{+h, +t}:
      young_live(cap, bound, pc, mt, h <> t, ws, now, fx, ow, pb, p)

def young_settle(+l: Lg, +now: Nat, +fx: +List<Fx>, +pb: IsT(pos_b(l)), p: IsT(young_u(l, now))) -> IsT(young_u(lg_of(settle(l, now, fx)), now)):
  match l:
    case Lg{cap, bound, PhIdle{}, ws}:
      Unit{}
    case Lg{+cap, +bound, PhRun{+pc, mt, hs}, +ws}:
      young_run(cap, bound, pc, mt, hs, ws, now, fx, any_cls(other(pc), ws), and_r(pos_n(cap), pos_n(bound), pb), p)

def young_after_of(+l: Lg, +now: Nat, p: IsT(young_u(l, now))) -> IsT(young_after(l, now)):
  match l:
    case Lg{cap, bound, PhIdle{}, ws}:
      Unit{}
    case Lg{cap, bound, PhRun{+pc, mt, hs}, +ws}:
      imp_t(any_cls(other(pc), ws), young_b(bound, now, hs), p)

def young_expire.run(+cap: Nat, +bound: Nat, +pc: Cls, mt: Mt, +hs: +List<Hd>, +ws: +List<Wt>, +now: Nat, waits: Bool, ew: {any_cls(other(pc), ws) == waits : Bool}, pb: IsT(pos_n(cap)), pbb: IsT(pos_n(bound))) -> IsT(young_after(lg_of(expire.run(cap, bound, pc, mt, hs, ws, now, waits)), now)):
  match mt waits:
    case Mt{+st, +m, +ev} False{}:
      imp_f(any_cls(other(pc), ws), young_b(bound, now, hs), ff(any_cls(other(pc), ws), ew))
    case Mt{+st, +m, +ev} True{}:
      young_after_of(lg_of(settle(Lg{cap, bound, PhRun{pc, Mt{st, m, Nat.add(ev, ev_count(evict(hs, pc, other(pc), bound, now)))}, ev_keep(evict(hs, pc, other(pc), bound, now))}, ws}, now, ev_fx(evict(hs, pc, other(pc), bound, now)))), now, young_settle(Lg{cap, bound, PhRun{pc, Mt{st, m, Nat.add(ev, ev_count(evict(hs, pc, other(pc), bound, now)))}, ev_keep(evict(hs, pc, other(pc), bound, now))}, ws}, now, ev_fx(evict(hs, pc, other(pc), bound, now)), and_t(pos_n(cap), pos_n(bound), pb, pbb), young_keep(hs, pc, other(pc), bound, now)))

def young_expire(+l: Lg, +now: Nat, +pb: IsT(pos_b(l))) -> IsT(young_after(lg_of(expire(l, now)), now)):
  match l:
    case Lg{cap, bound, PhIdle{}, ws}:
      Unit{}
    case Lg{+cap, +bound, PhRun{+pc, mt, hs}, +ws}:
      young_expire.run(cap, bound, pc, mt, hs, ws, now, any_cls(other(pc), ws), {==}, and_l(pos_n(cap), pos_n(bound), pb), and_r(pos_n(cap), pos_n(bound), pb))

# (e) Bounded hold: after an expire at any time, while a waiter of the other class waits no holder
# remains that has been held the bound (the code evicts a holder once it has been held the bound).
law bounded_hold:
  for +c0: Nat
  for +b0: Nat
  for +os: +List<Op>
  IsT(young_after(lg_of(expire(lg(run(os, empty(c0, b0))), now(run(os, empty(c0, b0))))), now(run(os, empty(c0, b0)))))

def pos_of_run(+os: +List<Op>, +c0: Nat, +b0: Nat) -> IsT(pos_b(lg(run(os, empty(c0, b0))))):
  pos_of(run(os, empty(c0, b0)), inv_of(os, c0, b0))

def bounded_hold(c0, b0, os):
  young_expire(lg(run(os, empty(c0, b0))), now(run(os, empty(c0, b0))), pos_of_run(os, c0, b0))

# --- the laws as checks a driver evaluates on the code's own ledger -------------------------------------

law no_overtaking:
  for +c0: Nat
  for +b0: Nat
  for +os: +List<Op>
  IsT(hok_ph(lg(run(os, empty(c0, b0))), last(run(os, empty(c0, b0)))))

def no_overtaking(c0, b0, os):
  fair_of(os, c0, b0)

law no_overtaking_check:
  for +c0: Nat
  for +b0: Nat
  for +os: +List<Op>
  {hok_ph(lg(run(os, empty(c0, b0))), last(run(os, empty(c0, b0)))) == True{} : Bool}

law no_starvation_check:
  for +c0: Nat
  for +b0: Nat
  for +os: +List<Op>
  for +o: Op
  {starve_b(lg(run(os, empty(c0, b0))), lg(step(o, run(os, empty(c0, b0))))) == True{} : Bool}

law bounded_hold_check:
  for +c0: Nat
  for +b0: Nat
  for +os: +List<Op>
  {young_after(lg_of(expire(lg(run(os, empty(c0, b0))), now(run(os, empty(c0, b0))))), now(run(os, empty(c0, b0)))) == True{} : Bool}

def no_overtaking_check(c0, b0, os):
  refl_t(hok_ph(lg(run(os, empty(c0, b0))), last(run(os, empty(c0, b0)))), no_overtaking(c0, b0, os))

def no_starvation_check(c0, b0, os, o):
  refl_t(starve_b(lg(run(os, empty(c0, b0))), lg(step(o, run(os, empty(c0, b0))))), no_starvation(c0, b0, os, o))

def bounded_hold_check(c0, b0, os):
  refl_t(young_after(lg_of(expire(lg(run(os, empty(c0, b0))), now(run(os, empty(c0, b0))))), now(run(os, empty(c0, b0)))), bounded_hold(c0, b0, os))

# --- definedness: each law's claim is not vacuous, because the ledger can grant ---------------------------

# an ask to an idle ledger starts a phase for it and is granted at once
law ask_idle_granted:
  for +c0: Nat
  for +b0: Nat
  for +c: Cls
  for +s: Shr
  {step(DAsk{c, s}, empty(c0, b0)) == St{Lg{1n+c0, 1n+b0, PhRun{c, Mt{0n, 1n, 0n}, [Hd{1n, c, s, 0n}]}, []}, 1n, 0n, [FxGrant{1n}]} : St}

def ask_idle_granted(c0, b0, c, s):
  {==}

# the holder leaving drains the phase and the ledger is idle again, having said so
law leave_drains_to_idle:
  for +c0: Nat
  for +b0: Nat
  for +c: Cls
  for +s: Shr
  {run([DAsk{c, s}, DLeave{0n}], empty(c0, b0)) == St{Lg{1n+c0, 1n+b0, PhIdle{}, []}, 1n, 0n, [FxEnd{c, 0n, 1n, 0n}]} : St}

def leave_drains_to_idle(c0, b0, c, s):
  {==}

# asks that cannot enter wait in arrival order
law queue_in_arrival_order:
  for +c0: Nat
  for +b0: Nat
  {waiting_of(lg(run([DAsk{CStorage{}, SShared{}}, DAsk{CNamespace{}, SShared{}}, DAsk{CNamespace{}, SAlone{}}], empty(c0, b0)))) == [Wt{2n, CNamespace{}, SShared{}}, Wt{3n, CNamespace{}, SAlone{}}] : +List<Wt>}

def queue_in_arrival_order(c0, b0):
  {==}

# a request of the running class with room, and nobody waiting, enters at once
law running_class_enters_with_room:
  for +b0: Nat
  {fx(run([DAsk{CStorage{}, SShared{}}, DAsk{CStorage{}, SShared{}}], empty(1n, b0))) == [FxGrant{2n}] : +List<Fx>}

def running_class_enters_with_room(b0):
  {==}

# a full phase's drain grants the oldest waiter of its class, and only it
law oldest_waiter_granted_when_room:
  for +b0: Nat
  {run([DAsk{CStorage{}, SShared{}}, DAsk{CStorage{}, SShared{}}, DAsk{CStorage{}, SShared{}}, DLeave{2n}], empty(0n, b0)) == St{Lg{1n, 1n+b0, PhRun{CStorage{}, Mt{0n, 1n, 0n}, [Hd{2n, CStorage{}, SShared{}, 0n}]}, [Wt{3n, CStorage{}, SShared{}}]}, 3n, 0n, [FxEnd{CStorage{}, 0n, 1n, 0n}, FxGrant{2n}]} : St}

def oldest_waiter_granted_when_room(b0):
  {==}

# a drained phase goes to the waiting class
law waiter_granted_when_phase_drains:
  for +c0: Nat
  for +b0: Nat
  {fx(run([DAsk{CStorage{}, SShared{}}, DAsk{CNamespace{}, SShared{}}, DLeave{1n}], empty(c0, b0))) == [FxEnd{CStorage{}, 0n, 1n, 0n}, FxGrant{2n}] : +List<Fx>}

def waiter_granted_when_phase_drains(c0, b0):
  {==}

# a waiter of the other class holds the running phase shut though it has room
law waiting_class_stops_entries:
  for +b0: Nat
  {run([DAsk{CStorage{}, SShared{}}, DAsk{CNamespace{}, SShared{}}, DAsk{CStorage{}, SShared{}}], empty(1n, b0)) == St{Lg{2n, 1n+b0, PhRun{CStorage{}, Mt{0n, 1n, 0n}, [Hd{1n, CStorage{}, SShared{}, 0n}]}, [Wt{2n, CNamespace{}, SShared{}}, Wt{3n, CStorage{}, SShared{}}]}, 3n, 0n, []} : St}

def waiting_class_stops_entries(b0):
  {==}

# a holder held the bound is evicted, the phase hands over, and the waiter is granted
law expire_evicts_at_bound:
  for +c0: Nat
  {fx(run([DAsk{CStorage{}, SShared{}}, DAsk{CNamespace{}, SShared{}}, DTick{}, DTick{}, DExpire{}], empty(c0, 1n))) == [FxEvict{1n, CStorage{}, 2n, CNamespace{}}, FxEnd{CStorage{}, 2n, 1n, 1n}, FxGrant{2n}] : +List<Fx>}

def expire_evicts_at_bound(c0):
  {==}

# and not a tick before
law expire_waits_for_bound:
  for +c0: Nat
  {fx(run([DAsk{CStorage{}, SShared{}}, DAsk{CNamespace{}, SShared{}}, DTick{}, DExpire{}], empty(c0, 1n))) == [] : +List<Fx>}

def expire_waits_for_bound(c0):
  {==}

# --- gate_io_priority: what the kernel cannot say (the open law is disk_lease_open.bend's) ------------------
#
# GATEWAY-IO-PRIORITY wants a disk lease a land's gate asks never to wait behind a background job's lease.
# `Ledger::ask` takes a class, a share and a claim: nothing says whose lease it is. The law is therefore over
# every labelling of the asks as gate or background (`gates` lists the tickets that are gates' leases), and the
# kernel, which never reads a label, refutes it.

def mem_n(+x: Nat, xs: +List<Nat>) -> Bool:
  match xs:
    case Nil{}:
      False{}
    case Con{h, t}:
      or(same(x, h), mem_n(x, t))

# every holder is a gate's lease
def gates_hold_b(hs: +List<Hd>, +gates: +List<Nat>) -> Bool:
  match hs:
    case Nil{}:
      True{}
    case Con{h, t}:
      and(mem_n(hd_ticket(h), gates), gates_hold_b(t, gates))

# a waiting gate lease has only gates' leases ahead of it: `clear` says every holder and every older waiter is one
def gate_wait_b(+ws: +List<Wt>, +gates: +List<Nat>, +clear: Bool) -> Bool:
  match ws:
    case Nil{}:
      True{}
    case Con{+w, r}:
      and(imp(mem_n(wt_ticket(w), gates), clear), gate_wait_b(r, gates, and(clear, mem_n(wt_ticket(w), gates))))

def gate_ok_b(l: Lg, +gates: +List<Nat>) -> Bool:
  match l:
    case Lg{cap, bound, PhIdle{}, ws}:
      gate_wait_b(ws, gates, True{})
    case Lg{cap, bound, PhRun{c, mt, hs}, ws}:
      gate_wait_b(ws, gates, gates_hold_b(hs, gates))

# Refuted, proven: a gate's storage lease asked while a background namespace lease holds the ledger waits
# behind it (ticket 2 is the gate's). This is the open law's counterexample, computed.
law gate_io_priority_refuted:
  for +c0: Nat
  for +b0: Nat
  {gate_ok_b(lg(run([DAsk{CNamespace{}, SShared{}}, DAsk{CStorage{}, SShared{}}], empty(c0, b0))), [2n]) == False{} : Bool}

def gate_io_priority_refuted(c0, b0):
  {==}

# Definedness of gate_io_priority: a gate lease granted at once leaves nothing to wait behind, and a gate lease
# that waits behind another gate's is no violation.
law gate_lease_on_idle_ledger_is_granted:
  for +c0: Nat
  for +b0: Nat
  for +c: Cls
  for +s: Shr
  {gate_ok_b(lg(run([DAsk{c, s}], empty(c0, b0))), [1n]) == True{} : Bool}

def gate_lease_on_idle_ledger_is_granted(c0, b0, c, s):
  {==}

law gate_behind_gate_is_no_violation:
  for +c0: Nat
  for +b0: Nat
  {gate_ok_b(lg(run([DAsk{CNamespace{}, SShared{}}, DAsk{CStorage{}, SShared{}}], empty(c0, b0))), [1n, 2n]) == True{} : Bool}

def gate_behind_gate_is_no_violation(c0, b0):
  {==}
