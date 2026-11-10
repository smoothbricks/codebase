
# mbx's pool admission (specs/cowshed/05_gateway.md "Classes", "Admission", "Held jobs" Order,
# "Gates and lands"), as a model whose laws are the policy's contract.
#
# One lane per band, in priority order: interactive, the gate's (`normal`), background (`low`).
# An interactive job decays by its elapsed time when it asks: its ask enters the interactive
# lane before `b1()`, the gate's until `b2()`, background's after. A lane is two lists:
# `old`, the requests that have waited their aging bound, in arrival order (an entry's id is its
# arrival ticket), and `new`, the rest. A decision walks the lanes in order, each lane's old then
# its new, and hands each request what it still needs from what is free: a
# request is granted min(want, free) once, and a grant is never taken back nor grown. `Tick`
# ages every request in `new` and moves those that reached the bound into `old` by ticket.
#
# A minimum (mbx e95a64c, `min`): until it ages, a request in `new` that names a minimum m
# starts only once least = min(m, want) permits are free (a least of 0 or 1 is none), then takes
# what is free up to its want. When its turn comes with fewer free it takes nothing and opens a
# gather: while that gather is open, later requests of its party (`p`, 0: none) and all low work
# take nothing; everything else passes. `old` is walked as today, so a gather ends at the bound.
# `step.today` is the policy without minimums, kept to state what changed.
#
# Today: `new` is in arrival order, so the whole lane is. Spt: `new` is ordered by the
# predicted run time measured from the request's class history, so a lane's shortest request
# goes first unless an older one has aged.
import Base

type Pol is Data:
  Today{}
  Spt{}

# k: the permits it holds; pred: its predicted run time; age: ticks waited in `new`; m: the
# fewest permits it starts on before it ages (0 or 1: none); p: its party (0: none).
type E is Data:
  E{id: Nat, want: Nat, k: Nat, pred: Nat, age: Nat, m: Nat, p: Nat}

type Lane is Data:
  Lane{old: +List<E>, new: +List<E>}

# The pool: its free permits, its aging bound, and its lanes in priority order. A decision
# walks them first to last; `empty` makes three: interactive, the gate's (`normal`), background
# (`low`).
type St is Data:
  St{free: Nat, bound: Nat, lanes: +List<Lane>}

# What a walk or a finish over the lanes leaves: the free permits, and the lanes.
type LS is Data:
  LS{free: Nat, lanes: +List<Lane>}

def lsf(w: LS) -> Nat:
  match w:
    case LS{f, ls}:
      f

def lsl(w: LS) -> +List<Lane>:
  match w:
    case LS{f, ls}:
      ls

# The band a request asks for: a land's gate, interactive work (a check or one test an agent
# waits on), or background.
type Band is Data:
  Gate{}
  Interactive{}
  Background{}

# `elapsed`: how long the request's job has run when it asks (seconds).
type Op is Data:
  Arrive{band: Band, id: Nat, want: Nat, pred: Nat, elapsed: Nat, min: Nat, party: Nat}
  Decide{}
  Tick{}
  Finish{id: Nat}

# --- arithmetic ------------------------------------------------------------

def add(a: Nat, b: Nat) -> Nat:
  match a:
    case 0n:
      b
    case 1n+p:
      1n+add(p, b)

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

# a <= b
def le(a: Nat, b: Nat) -> Bool:
  match a b:
    case 0n b0:
      True{}
    case 1n+a1 0n:
      False{}
    case 1n+a1 1n+b1:
      le(a1, b1)

def LE(a: Nat, b: Nat) -> Data:
  match a b:
    case 0n b0:
      Unit
    case 1n+a1 0n:
      Empty
    case 1n+a1 1n+b1:
      LE(a1, b1)

def IsZero(n: Nat) -> Data:
  match n:
    case 0n:
      Unit
    case 1n+p:
      Empty

def IsTrue(b: Bool) -> Data:
  match b:
    case True{}:
      Unit
    case False{}:
      Empty

# Of `b` free, an entry needing `a` is handed gg(a, b) = min(a, b), leaving gr(a, b).
def gg(a: Nat, b: Nat) -> Nat:
  match a b:
    case 0n b0:
      0n
    case 1n+a1 0n:
      0n
    case 1n+a1 1n+b1:
      1n+gg(a1, b1)

def gr(a: Nat, b: Nat) -> Nat:
  match a b:
    case 0n b0:
      b0
    case 1n+a1 0n:
      0n
    case 1n+a1 1n+b1:
      gr(a1, b1)

# --- what an entry still needs: its want while it waits, nothing once granted ----

def need.k(+want: Nat, k: Nat) -> Nat:
  match k:
    case 0n:
      want
    case 1n+p:
      0n

def need(e: E) -> Nat:
  match e:
    case E{id, want, k, pred, age, mi, pa}:
      need.k(want, k)

# --- a list walk ------------------------------------------------------------------

type W is Data:
  W{free: Nat, lane: +List<E>}

def wf(w: W) -> Nat:
  match w:
    case W{f, l}:
      f

def wl(w: W) -> +List<E>:
  match w:
    case W{f, l}:
      l

def W.cons(+h: E, w: W) -> W:
  match w:
    case W{f, t}:
      W{f, h <> t}

# The head takes what it still needs of `free`, the rest walks on what is left: a later
# entry gets a permit only once every earlier one needs none.
def walk(lane: +List<E>, +free: Nat) -> W:
  match lane:
    case Nil{}:
      W{free, []}
    case Con{E{+id, +want, +k, +pred, +age, +mi, +pa}, t}:
      W.cons(E{id, want, add(k, gg(need(E{id, want, k, pred, age, mi, pa}), free)), pred, age, mi, pa}, walk(t, gr(need(E{id, want, k, pred, age, mi, pa}), free)))

# --- a lane walk: its aged requests, then the rest ----------------------------------

type LW is Data:
  LW{free: Nat, lane: Lane}

def lf(w: LW) -> Nat:
  match w:
    case LW{f, l}:
      f

def ll(w: LW) -> Lane:
  match w:
    case LW{f, l}:
      l

def lwalk(l: Lane, +free: Nat) -> LW:
  match l:
    case Lane{+o, +n}:
      LW{wf(walk(n, wf(walk(o, free)))), Lane{wl(walk(o, free)), wl(walk(n, wf(walk(o, free))))}}

# Today's decision walks each lane in order with what the lanes before it left free.
def walks(ls: +List<Lane>, +f: Nat) -> LS:
  match ls:
    case Nil{}:
      LS{f, []}
    case Con{+l, +t}:
      LS{lsf(walks(t, lf(lwalk(l, f)))), Con{ll(lwalk(l, f)), lsl(walks(t, lf(lwalk(l, f))))}}

def decide.today(s: St) -> St:
  match s:
    case St{+f, +b, +ls}:
      St{lsf(walks(ls, f)), b, lsl(walks(ls, f))}

# --- the gather (mbx e95a64c, `admission_pass`) -------------------------------------
# A request in `new` that names a minimum gathers: when its turn comes with fewer than
# least = min(m, want) permits free, and least is 2 or more, it takes none and opens a gather.
# While a gather is open, a later request of the gatherer's party (not 0) takes none, and so does
# all low work. Aged requests (`old`) are walked as today, so a gather ends at the aging bound.
#
# A decision carries the parties of the open gathers, one per gather (0: none), in `g`.

# What a request in `new` does when its turn comes.
type Act is Data:
  Take{}
  Hold{}
  Open{}

# p and q are one party: 0 is none, and matches nothing.
def pmatch(p: Nat, +q: Nat) -> Bool:
  match p:
    case 0n:
      False{}
    case 1n+a:
      same(1n+a, q)

def anyp(+p: Nat, g: +List<Nat>) -> Bool:
  match g:
    case Nil{}:
      False{}
    case Con{q, t}:
      Bool.or(pmatch(p, q), anyp(p, t))

# Held behind an open gather: low work (`lo`), or of a gatherer's party.
def held(+lo: Bool, +p: Nat, g: +List<Nat>) -> Bool:
  match g:
    case Nil{}:
      False{}
    case Con{+q, +t}:
      Bool.or(lo, anyp(p, q <> t))

# Fewer than `least` free, when `least` is 2 or more (a minimum of 0 or 1 is none).
def short(l: Nat, +f: Nat) -> Bool:
  match l:
    case 0n:
      False{}
    case 1n+a:
      match a:
        case 0n:
          False{}
        case 1n+b:
          Bool.not(le(2n+b, f))

def act0(h: Bool, s: Bool) -> Act:
  match h s:
    case True{} s0:
      Hold{}
    case False{} True{}:
      Open{}
    case False{} False{}:
      Take{}

# A request that holds permits has started and takes none; one that waits is held, gathers, or
# takes what it needs.
def act(+lo: Bool, +g: +List<Nat>, k: Nat, +want: Nat, +mi: Nat, +pa: Nat, +f: Nat) -> Act:
  match k:
    case 0n:
      act0(held(lo, pa, g), short(gg(mi, want), f))
    case 1n+j:
      Take{}

def gact(+lo: Bool, +g: +List<Nat>, e: E, +f: Nat) -> Act:
  match e:
    case E{id, +want, +k, pred, age, +mi, +pa}:
      act(lo, g, k, want, mi, pa, f)

def en(a: Act, n: Nat) -> Nat:
  match a:
    case Take{}:
      n
    case Hold{}:
      0n
    case Open{}:
      0n

# What a request in `new` takes toward: what it still needs, or nothing while held or gathering.
def gneed(+lo: Bool, +g: +List<Nat>, e: E, +f: Nat) -> Nat:
  match e:
    case E{id, +want, +k, pred, age, +mi, +pa}:
      en(act(lo, g, k, want, mi, pa, f), need.k(want, k))

def gnext(a: Act, +pa: Nat, g: +List<Nat>) -> +List<Nat>:
  match a:
    case Take{}:
      g
    case Hold{}:
      g
    case Open{}:
      pa <> g

# A walk of `new` in a lane (`lo`: the low lane) with the gathers `g` open.
def gwalk(+lo: Bool, lane: +List<E>, +free: Nat, +g: +List<Nat>) -> W:
  match lane:
    case Nil{}:
      W{free, []}
    case Con{E{+id, +want, +k, +pred, +age, +mi, +pa}, t}:
      W.cons(E{id, want, add(k, gg(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, free), free)), pred, age, mi, pa},
        gwalk(lo, t, gr(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, free), free), gnext(gact(lo, g, E{id, want, k, pred, age, mi, pa}, free), pa, g)))

# The gathers open after that walk.
def gend(+lo: Bool, lane: +List<E>, +free: Nat, +g: +List<Nat>) -> +List<Nat>:
  match lane:
    case Nil{}:
      g
    case Con{E{+id, +want, +k, +pred, +age, +mi, +pa}, t}:
      gend(lo, t, gr(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, free), free), gnext(gact(lo, g, E{id, want, k, pred, age, mi, pa}, free), pa, g))

# The low lane takes nothing while a gather above it is open.
def skip(+lo: Bool, g: +List<Nat>) -> Bool:
  match g:
    case Nil{}:
      False{}
    case Con{q, t}:
      lo

# A lane: its aged requests as today, then the rest with the gathers open.
def glwalk.if(sk: Bool, +lo: Bool, +o: +List<E>, +n: +List<E>, +free: Nat, +g: +List<Nat>) -> LW:
  match sk:
    case True{}:
      LW{free, Lane{o, n}}
    case False{}:
      LW{wf(gwalk(lo, n, wf(walk(o, free)), g)), Lane{wl(walk(o, free)), wl(gwalk(lo, n, wf(walk(o, free)), g))}}

def glwalk(+lo: Bool, l: Lane, +free: Nat, +g: +List<Nat>) -> LW:
  match l:
    case Lane{+o, +n}:
      glwalk.if(skip(lo, g), lo, o, n, free, g)

def glg.if(sk: Bool, +lo: Bool, +o: +List<E>, +n: +List<E>, +free: Nat, +g: +List<Nat>) -> +List<Nat>:
  match sk:
    case True{}:
      g
    case False{}:
      gend(lo, n, wf(walk(o, free)), g)

def glg(+lo: Bool, l: Lane, +free: Nat, +g: +List<Nat>) -> +List<Nat>:
  match l:
    case Lane{+o, +n}:
      glg.if(skip(lo, g), lo, o, n, free, g)

# The last lane is the low one.
def islow(t: +List<Lane>) -> Bool:
  match t:
    case Nil{}:
      True{}
    case Con{h, r}:
      False{}

# A decision walks each lane in order with what the lanes before it left free and the gathers
# they left open.
def gwalks(ls: +List<Lane>, +f: Nat, +g: +List<Nat>) -> LS:
  match ls:
    case Nil{}:
      LS{f, []}
    case Con{+l, +t}:
      LS{lsf(gwalks(t, lf(glwalk(islow(t), l, f, g)), glg(islow(t), l, f, g))), Con{ll(glwalk(islow(t), l, f, g)), lsl(gwalks(t, lf(glwalk(islow(t), l, f, g)), glg(islow(t), l, f, g)))}}

def decide(s: St) -> St:
  match s:
    case St{+f, +b, +ls}:
      St{lsf(gwalks(ls, f, [])), b, lsl(gwalks(ls, f, []))}

# --- arrivals: Today appends, Spt inserts by prediction ----------------------------

def eid(e: E) -> Nat:
  match e:
    case E{id, want, k, pred, age, mi, pa}:
      id

def epred(e: E) -> Nat:
  match e:
    case E{id, want, k, pred, age, mi, pa}:
      pred

def snoc(xs: +List<E>, +y: E) -> +List<E>:
  match xs:
    case Nil{}:
      [y]
    case Con{h, t}:
      h <> snoc(t, y)

def insert.if(+y: E, +h: E, +t: +List<E>, rest: +List<E>, before: Bool) -> +List<E>:
  match before:
    case True{}:
      y <> h <> t
    case False{}:
      h <> rest

# Before the first entry predicted longer: among equals, the later arrival goes last.
def insert(xs: +List<E>, +y: E) -> +List<E>:
  match xs:
    case Nil{}:
      [y]
    case Con{+h, +t}:
      insert.if(y, h, t, insert(t, y), Bool.not(le(epred(h), epred(y))))

# Into `old` by arrival ticket (an entry's id is its ticket): before the first entry asked later.
def insid.if(+y: E, +h: E, +t: +List<E>, rest: +List<E>, before: Bool) -> +List<E>:
  match before:
    case True{}:
      y <> h <> t
    case False{}:
      h <> rest

def insid(xs: +List<E>, +y: E) -> +List<E>:
  match xs:
    case Nil{}:
      [y]
    case Con{+h, +t}:
      insid.if(y, h, t, insid(t, y), Bool.not(le(eid(h), eid(y))))

# Every entry of m into o, by ticket.
def insall(m: +List<E>, +o: +List<E>) -> +List<E>:
  match m:
    case Nil{}:
      o
    case Con{y, t}:
      insall(t, insid(o, y))

def enter(+p: Pol, +n: +List<E>, +y: E) -> +List<E>:
  match p:
    case Today{}:
      snoc(n, y)
    case Spt{}:
      insert(n, y)

def arrive.lane(+p: Pol, +y: E, l: Lane) -> Lane:
  match l:
    case Lane{o, n}:
      Lane{o, enter(p, n, y)}

# A request enters lane `i`; there is no lane past the last, and `empty` has one per band.
def arrive.at(+p: Pol, i: Nat, +y: E, ls: +List<Lane>) -> +List<Lane>:
  match i ls:
    case 0n Con{+l, +t}:
      Con{arrive.lane(p, y, l), t}
    case 1n+j Con{+l, +t}:
      Con{l, arrive.at(p, j, y, t)}
    case i0 Nil{}:
      []

def arrive(+p: Pol, +i: Nat, +y: E, s: St) -> St:
  match s:
    case St{f, b, ls}:
      St{f, b, arrive.at(p, i, y, ls)}

# --- a tick: every request in `new` ages; those at the bound move to `old` ----------

type Sp is Data:
  Sp{moved: +List<E>, stay: +List<E>}

def sm(x: Sp) -> +List<E>:
  match x:
    case Sp{m, s}:
      m

def ss(x: Sp) -> +List<E>:
  match x:
    case Sp{m, s}:
      s

def split.if(+e: E, +rest: Sp, aged: Bool) -> Sp:
  match aged:
    case True{}:
      Sp{e <> sm(rest), ss(rest)}
    case False{}:
      Sp{sm(rest), e <> ss(rest)}

def split(n: +List<E>, +b: Nat) -> Sp:
  match n:
    case Nil{}:
      Sp{[], []}
    case Con{E{+id, +want, +k, +pred, +age, +mi, +pa}, t}:
      split.if(E{id, want, k, pred, 1n+age, mi, pa}, split(t, b), le(b, 1n+age))

def append(xs: +List<E>, +ys: +List<E>) -> +List<E>:
  match xs:
    case Nil{}:
      ys
    case Con{h, t}:
      h <> append(t, ys)

def tick.lane(+b: Nat, l: Lane) -> Lane:
  match l:
    case Lane{+o, +n}:
      Lane{insall(sm(split(n, b)), o), ss(split(n, b))}

def ticks(+b: Nat, ls: +List<Lane>) -> +List<Lane>:
  match ls:
    case Nil{}:
      []
    case Con{+l, +t}:
      Con{tick.lane(b, l), ticks(b, t)}

def tick(s: St) -> St:
  match s:
    case St{+f, +b, +ls}:
      St{f, b, ticks(b, ls)}

# --- an end gives its entry's permits back -----------------------------------------

def fin.if(+id: Nat, +want: Nat, +k: Nat, +pred: Nat, +age: Nat, +mi: Nat, +pa: Nat, t: +List<E>, rest: W, hit: Bool) -> W:
  match hit:
    case True{}:
      W{k, t}
    case False{}:
      W.cons(E{id, want, k, pred, age, mi, pa}, rest)

def fin(lane: +List<E>, +x: Nat) -> W:
  match lane:
    case Nil{}:
      W{0n, []}
    case Con{E{+id, +want, +k, +pred, +age, +mi, +pa}, +t}:
      fin.if(id, want, k, pred, age, mi, pa, t, fin(t, x), same(x, id))

def lfin(l: Lane, +x: Nat) -> LW:
  match l:
    case Lane{+o, +n}:
      LW{add(wf(fin(o, x)), wf(fin(n, x))), Lane{wl(fin(o, x)), wl(fin(n, x))}}

def fins(ls: +List<Lane>, +x: Nat) -> LS:
  match ls:
    case Nil{}:
      LS{0n, []}
    case Con{+l, +t}:
      LS{add(lf(lfin(l, x)), lsf(fins(t, x))), Con{ll(lfin(l, x)), lsl(fins(t, x))}}

def finish(+x: Nat, s: St) -> St:
  match s:
    case St{+f, +b, +ls}:
      St{add(f, lsf(fins(ls, x))), b, lsl(fins(ls, x))}

# An interactive job decays (05 "Classes"): its asks are interactive while the job has run less
# than `b1()`, gate-band work until `b2()`, and background work from then on. One unit is one
# second.
def b1() -> Nat:
  120n

def b2() -> Nat:
  240n

# The band an interactive job's ask takes at `elapsed`: 0 interactive, 1 the gate's, 2 background.
def iband.go(late1: Bool, late2: Bool) -> Nat:
  match late1 late2:
    case False{} l2:
      0n
    case True{} False{}:
      1n
    case True{} True{}:
      2n

def iband(+elapsed: Nat) -> Nat:
  iband.go(le(b1(), elapsed), le(b2(), elapsed))

# The lane a request enters: the gate's band is lane 1, background lane 2, and an interactive
# job's ask the lane of its band at its elapsed time.
def lane(band: Band, +elapsed: Nat) -> Nat:
  match band:
    case Gate{}:
      1n
    case Interactive{}:
      iband(elapsed)
    case Background{}:
      2n

def step(+p: Pol, o: Op, +s: St) -> St:
  match o:
    case Arrive{+band, id, want, +pred, +elapsed, mi, pa}:
      arrive(p, lane(band, elapsed), E{id, 1n+want, 0n, pred, 0n, mi, pa}, s)
    case Decide{}:
      decide(s)
    case Tick{}:
      tick(s)
    case Finish{x}:
      finish(x, s)

def run(+p: Pol, os: +List<Op>, s: St) -> St:
  match os:
    case Nil{}:
      s
    case Con{o, t}:
      run(p, t, step(p, o, s))

# Today's policy: the same steps, with a decision that knows no minimum.
def step.today(+p: Pol, o: Op, +s: St) -> St:
  match o:
    case Arrive{+band, id, want, +pred, +elapsed, mi, pa}:
      arrive(p, lane(band, elapsed), E{id, 1n+want, 0n, pred, 0n, mi, pa}, s)
    case Decide{}:
      decide.today(s)
    case Tick{}:
      tick(s)
    case Finish{x}:
      finish(x, s)

def run.today(+p: Pol, os: +List<Op>, s: St) -> St:
  match os:
    case Nil{}:
      s
    case Con{o, t}:
      run.today(p, t, step.today(p, o, s))

def empty(cap: Nat, bound: Nat) -> St:
  St{cap, bound, [Lane{[], []}, Lane{[], []}, Lane{[], []}]}

# === laws ==========================================================================

# --- arithmetic lemmas --------------------------------------------------------------

def add_zero_r(+a: Nat) -> {add(a, 0n) == a : Nat}:
  match a:
    case 0n:
      {==}
    case 1n+p:
      %add_zero_r(p) : {1n+add(p, 0n) == 1n+_ : Nat}
      {==}

def add_succ_r(+a: Nat, +b: Nat) -> {add(a, 1n+b) == 1n+add(a, b) : Nat}:
  match a:
    case 0n:
      {==}
    case 1n+p:
      %add_succ_r(p, b) : {1n+add(p, 1n+b) == 1n+_ : Nat}
      {==}

def add_assoc(+a: Nat, +b: Nat, +c: Nat) -> {add(add(a, b), c) == add(a, add(b, c)) : Nat}:
  match a:
    case 0n:
      {==}
    case 1n+p:
      %add_assoc(p, b, c) : {1n+add(add(p, b), c) == 1n+_ : Nat}
      {==}

def add_comm(+a: Nat, +b: Nat) -> {add(a, b) == add(b, a) : Nat}:
  match a:
    case 0n:
      Equal.sym(Nat, add(b, 0n), b, add_zero_r(b))
    case 1n+p:
      %Equal.sym(Nat, add(b, 1n+p), 1n+add(b, p), add_succ_r(b, p)) : {1n+add(p, b) == _ : Nat}
      %add_comm(p, b) : {1n+add(p, b) == 1n+_ : Nat}
      {==}

# a + (b + c) == b + (a + c)
def add_swap(+a: Nat, +b: Nat, +c: Nat) -> {add(a, add(b, c)) == add(b, add(a, c)) : Nat}:
  Equal.trans(Nat, add(a, add(b, c)), add(add(a, b), c), add(b, add(a, c)),
    Equal.sym(Nat, add(add(a, b), c), add(a, add(b, c)), add_assoc(a, b, c)),
    Equal.trans(Nat, add(add(a, b), c), add(add(b, a), c), add(b, add(a, c)),
      Equal.cong(Nat, Nat, x => add(x, c), add(a, b), add(b, a), add_comm(a, b)),
      add_assoc(b, a, c)))

def gg_gr(+a: Nat, +b: Nat) -> {add(gg(a, b), gr(a, b)) == b : Nat}:
  match a b:
    case 0n b0:
      {==}
    case 1n+a1 0n:
      {==}
    case 1n+a1 1n+b1:
      %gg_gr(a1, b1) : {1n+add(gg(a1, b1), gr(a1, b1)) == 1n+_ : Nat}
      {==}

# Two walks in a row move permits only between `free` and the entries they walk:
# from f1 + b == f + s1 and a + c == f1 + s2 follows a + (b + c) == f + (s1 + s2).
def chain2(+a: Nat, +b: Nat, +c: Nat, +f1: Nat, +f: Nat, +s1: Nat, +s2: Nat,
  e1: {add(f1, b) == add(f, s1) : Nat}, e2: {add(a, c) == add(f1, s2) : Nat})
  -> {add(a, add(b, c)) == add(f, add(s1, s2)) : Nat}:
  Equal.trans(Nat, add(a, add(b, c)), add(b, add(a, c)), add(f, add(s1, s2)),
    add_swap(a, b, c),
  Equal.trans(Nat, add(b, add(a, c)), add(b, add(f1, s2)), add(f, add(s1, s2)),
    Equal.cong(Nat, Nat, x => add(b, x), add(a, c), add(f1, s2), e2),
  Equal.trans(Nat, add(b, add(f1, s2)), add(add(b, f1), s2), add(f, add(s1, s2)),
    Equal.sym(Nat, add(add(b, f1), s2), add(b, add(f1, s2)), add_assoc(b, f1, s2)),
  Equal.trans(Nat, add(add(b, f1), s2), add(add(f1, b), s2), add(f, add(s1, s2)),
    Equal.cong(Nat, Nat, x => add(x, s2), add(b, f1), add(f1, b), add_comm(b, f1)),
  Equal.trans(Nat, add(add(f1, b), s2), add(add(f, s1), s2), add(f, add(s1, s2)),
    Equal.cong(Nat, Nat, x => add(x, s2), add(f1, b), add(f, s1), e1),
    add_assoc(f, s1, s2))))))

# (a + b) + (c + d) == (a + c) + (b + d)
def swap4(+a: Nat, +b: Nat, +c: Nat, +d: Nat) -> {add(add(a, b), add(c, d)) == add(add(a, c), add(b, d)) : Nat}:
  Equal.trans(Nat, add(add(a, b), add(c, d)), add(a, add(b, add(c, d))), add(add(a, c), add(b, d)),
    add_assoc(a, b, add(c, d)),
  Equal.trans(Nat, add(a, add(b, add(c, d))), add(a, add(c, add(b, d))), add(add(a, c), add(b, d)),
    Equal.cong(Nat, Nat, x => add(a, x), add(b, add(c, d)), add(c, add(b, d)), add_swap(b, c, d)),
    Equal.sym(Nat, add(add(a, c), add(b, d)), add(a, add(c, add(b, d))), add_assoc(a, c, add(b, d)))))

# from x1 + y1 == z1 and x2 + y2 == z2 follows (x1 + x2) + (y1 + y2) == z1 + z2
def sum2(+x1: Nat, +y1: Nat, +z1: Nat, +x2: Nat, +y2: Nat, +z2: Nat,
  e1: {add(x1, y1) == z1 : Nat}, e2: {add(x2, y2) == z2 : Nat})
  -> {add(add(x1, x2), add(y1, y2)) == add(z1, z2) : Nat}:
  Equal.trans(Nat, add(add(x1, x2), add(y1, y2)), add(add(x1, y1), add(x2, y2)), add(z1, z2),
    swap4(x1, x2, y1, y2),
  Equal.trans(Nat, add(add(x1, y1), add(x2, y2)), add(z1, add(x2, y2)), add(z1, z2),
    Equal.cong(Nat, Nat, x => add(x, add(x2, y2)), add(x1, y1), z1, e1),
    Equal.cong(Nat, Nat, x => add(z1, x), add(x2, y2), z2, e2)))

# --- the ledger -------------------------------------------------------------------------

def ek(e: E) -> Nat:
  match e:
    case E{id, want, k, pred, age, mi, pa}:
      k

def sumk(lane: +List<E>) -> Nat:
  match lane:
    case Nil{}:
      0n
    case Con{e, t}:
      add(ek(e), sumk(t))

def suml(l: Lane) -> Nat:
  match l:
    case Lane{o, n}:
      add(sumk(o), sumk(n))

def sumls(ls: +List<Lane>) -> Nat:
  match ls:
    case Nil{}:
      0n
    case Con{+l, +t}:
      add(suml(l), sumls(t))

def total(s: St) -> Nat:
  match s:
    case St{f, b, ls}:
      add(f, sumls(ls))

def cons_sum(+h: E, +w: W) -> {add(wf(W.cons(h, w)), sumk(wl(W.cons(h, w)))) == add(ek(h), add(wf(w), sumk(wl(w)))) : Nat}:
  match w:
    case W{f, l}:
      add_swap(f, ek(h), sumk(l))

def walk_sum.con(+id: Nat, +want: Nat, +k: Nat, +pred: Nat, +age: Nat, +mi: Nat, +pa: Nat, +t: +List<E>, +f: Nat, +n: Nat, +w: W,
  ih: {add(wf(w), sumk(wl(w))) == add(gr(n, f), sumk(t)) : Nat})
  -> {add(wf(W.cons(E{id, want, add(k, gg(n, f)), pred, age, mi, pa}, w)), sumk(wl(W.cons(E{id, want, add(k, gg(n, f)), pred, age, mi, pa}, w)))) == add(f, add(k, sumk(t))) : Nat}:
  Equal.trans(Nat,
    add(wf(W.cons(E{id, want, add(k, gg(n, f)), pred, age, mi, pa}, w)), sumk(wl(W.cons(E{id, want, add(k, gg(n, f)), pred, age, mi, pa}, w)))),
    add(add(k, gg(n, f)), add(wf(w), sumk(wl(w)))),
    add(f, add(k, sumk(t))),
    cons_sum(E{id, want, add(k, gg(n, f)), pred, age, mi, pa}, w),
  Equal.trans(Nat,
    add(add(k, gg(n, f)), add(wf(w), sumk(wl(w)))),
    add(add(k, gg(n, f)), add(gr(n, f), sumk(t))),
    add(f, add(k, sumk(t))),
    Equal.cong(Nat, Nat, x => add(add(k, gg(n, f)), x),
      add(wf(w), sumk(wl(w))), add(gr(n, f), sumk(t)), ih),
  Equal.trans(Nat,
    add(add(k, gg(n, f)), add(gr(n, f), sumk(t))),
    add(k, add(gg(n, f), add(gr(n, f), sumk(t)))),
    add(f, add(k, sumk(t))),
    add_assoc(k, gg(n, f), add(gr(n, f), sumk(t))),
  Equal.trans(Nat,
    add(k, add(gg(n, f), add(gr(n, f), sumk(t)))),
    add(k, add(add(gg(n, f), gr(n, f)), sumk(t))),
    add(f, add(k, sumk(t))),
    Equal.cong(Nat, Nat, x => add(k, x),
      add(gg(n, f), add(gr(n, f), sumk(t))), add(add(gg(n, f), gr(n, f)), sumk(t)),
      Equal.sym(Nat, add(add(gg(n, f), gr(n, f)), sumk(t)), add(gg(n, f), add(gr(n, f), sumk(t))),
        add_assoc(gg(n, f), gr(n, f), sumk(t)))),
  Equal.trans(Nat,
    add(k, add(add(gg(n, f), gr(n, f)), sumk(t))),
    add(k, add(f, sumk(t))),
    add(f, add(k, sumk(t))),
    Equal.cong(Nat, Nat, x => add(k, add(x, sumk(t))), add(gg(n, f), gr(n, f)), f, gg_gr(n, f)),
    add_swap(k, f, sumk(t)))))))

# A walk only moves permits between free and the entries it walks.
def walk_sum(+lane: +List<E>, +f: Nat) -> {add(wf(walk(lane, f)), sumk(wl(walk(lane, f)))) == add(f, sumk(lane)) : Nat}:
  match lane:
    case Nil{}:
      {==}
    case Con{E{id, want, k, pred, age, mi, pa}, t}:
      walk_sum.con(id, want, k, pred, age, mi, pa, t, f, need(E{id, want, k, pred, age, mi, pa}), walk(t, gr(need(E{id, want, k, pred, age, mi, pa}), f)),
        walk_sum(t, gr(need(E{id, want, k, pred, age, mi, pa}), f)))

def gwalk_sum(+lo: Bool, +lane: +List<E>, +f: Nat, +g: +List<Nat>) -> {add(wf(gwalk(lo, lane, f, g)), sumk(wl(gwalk(lo, lane, f, g)))) == add(f, sumk(lane)) : Nat}:
  match lane:
    case Nil{}:
      {==}
    case Con{E{id, want, k, pred, age, mi, pa}, t}:
      walk_sum.con(id, want, k, pred, age, mi, pa, t, f, gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f),
        gwalk(lo, t, gr(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f), gnext(gact(lo, g, E{id, want, k, pred, age, mi, pa}, f), pa, g)),
        gwalk_sum(lo, t, gr(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f), gnext(gact(lo, g, E{id, want, k, pred, age, mi, pa}, f), pa, g)))

def glwalk_sum.if(+sk: Bool, +lo: Bool, +o: +List<E>, +n: +List<E>, +f: Nat, +g: +List<Nat>)
  -> {add(lf(glwalk.if(sk, lo, o, n, f, g)), suml(ll(glwalk.if(sk, lo, o, n, f, g)))) == add(f, suml(Lane{o, n})) : Nat}:
  match sk:
    case True{}:
      {==}
    case False{}:
      chain2(wf(gwalk(lo, n, wf(walk(o, f)), g)), sumk(wl(walk(o, f))), sumk(wl(gwalk(lo, n, wf(walk(o, f)), g))),
        wf(walk(o, f)), f, sumk(o), sumk(n), walk_sum(o, f), gwalk_sum(lo, n, wf(walk(o, f)), g))

def glwalk_sum(+lo: Bool, +l: Lane, +f: Nat, +g: +List<Nat>) -> {add(lf(glwalk(lo, l, f, g)), suml(ll(glwalk(lo, l, f, g)))) == add(f, suml(l)) : Nat}:
  match l:
    case Lane{o, n}:
      glwalk_sum.if(skip(lo, g), lo, o, n, f, g)

def gwalks_sum(+ls: +List<Lane>, +f: Nat, +g: +List<Nat>) -> {add(lsf(gwalks(ls, f, g)), sumls(lsl(gwalks(ls, f, g)))) == add(f, sumls(ls)) : Nat}:
  match ls:
    case Nil{}:
      {==}
    case Con{+l, +t}:
      chain2(lsf(gwalks(t, lf(glwalk(islow(t), l, f, g)), glg(islow(t), l, f, g))), suml(ll(glwalk(islow(t), l, f, g))),
        sumls(lsl(gwalks(t, lf(glwalk(islow(t), l, f, g)), glg(islow(t), l, f, g)))),
        lf(glwalk(islow(t), l, f, g)), f, suml(l), sumls(t), glwalk_sum(islow(t), l, f, g),
        gwalks_sum(t, lf(glwalk(islow(t), l, f, g)), glg(islow(t), l, f, g)))

def decide_total(+s: St) -> {total(decide(s)) == total(s) : Nat}:
  match s:
    case St{f, b, ls}:
      gwalks_sum(ls, f, [])

# --- an arrival holds nothing ----------------------------------------------------------

def snoc_sum(+xs: +List<E>, +id: Nat, +want: Nat, +pred: Nat, +mi: Nat, +pa: Nat) -> {sumk(snoc(xs, E{id, want, 0n, pred, 0n, mi, pa})) == sumk(xs) : Nat}:
  match xs:
    case Nil{}:
      {==}
    case Con{h, t}:
      %snoc_sum(t, id, want, pred, mi, pa) : {add(ek(h), sumk(snoc(t, E{id, want, 0n, pred, 0n, mi, pa}))) == add(ek(h), _) : Nat}
      {==}

def insert_sum.if(+id: Nat, +want: Nat, +pred: Nat, +mi: Nat, +pa: Nat, +h: E, +t: +List<E>, +rest: +List<E>, +before: Bool,
  ih: {sumk(rest) == sumk(t) : Nat})
  -> {sumk(insert.if(E{id, want, 0n, pred, 0n, mi, pa}, h, t, rest, before)) == add(ek(h), sumk(t)) : Nat}:
  match before:
    case True{}:
      {==}
    case False{}:
      %Equal.sym(Nat, sumk(rest), sumk(t), ih) : {add(ek(h), _) == add(ek(h), sumk(t)) : Nat}
      {==}

def insert_sum(+xs: +List<E>, +id: Nat, +want: Nat, +pred: Nat, +mi: Nat, +pa: Nat) -> {sumk(insert(xs, E{id, want, 0n, pred, 0n, mi, pa})) == sumk(xs) : Nat}:
  match xs:
    case Nil{}:
      {==}
    case Con{h, t}:
      insert_sum.if(id, want, pred, mi, pa, h, t, insert(t, E{id, want, 0n, pred, 0n, mi, pa}),
        Bool.not(le(epred(h), epred(E{id, want, 0n, pred, 0n, mi, pa}))), insert_sum(t, id, want, pred, mi, pa))

def enter_sum(+p: Pol, +n: +List<E>, +id: Nat, +want: Nat, +pred: Nat, +mi: Nat, +pa: Nat) -> {sumk(enter(p, n, E{id, want, 0n, pred, 0n, mi, pa})) == sumk(n) : Nat}:
  match p:
    case Today{}:
      snoc_sum(n, id, want, pred, mi, pa)
    case Spt{}:
      insert_sum(n, id, want, pred, mi, pa)

def arrive_lane_sum(+p: Pol, +id: Nat, +want: Nat, +pred: Nat, +mi: Nat, +pa: Nat, +l: Lane) -> {suml(arrive.lane(p, E{id, want, 0n, pred, 0n, mi, pa}, l)) == suml(l) : Nat}:
  match l:
    case Lane{o, n}:
      %Equal.sym(Nat, sumk(enter(p, n, E{id, want, 0n, pred, 0n, mi, pa})), sumk(n), enter_sum(p, n, id, want, pred, mi, pa)) : {add(sumk(o), _) == add(sumk(o), sumk(n)) : Nat}
      {==}

def arrive_at_sum(+p: Pol, +i: Nat, +id: Nat, +want: Nat, +pred: Nat, +mi: Nat, +pa: Nat, +ls: +List<Lane>) -> {sumls(arrive.at(p, i, E{id, want, 0n, pred, 0n, mi, pa}, ls)) == sumls(ls) : Nat}:
  match i ls:
    case 0n Con{+l, +t}:
      %Equal.sym(Nat, suml(arrive.lane(p, E{id, want, 0n, pred, 0n, mi, pa}, l)), suml(l), arrive_lane_sum(p, id, want, pred, mi, pa, l)) : {add(_, sumls(t)) == add(suml(l), sumls(t)) : Nat}
      {==}
    case 1n+j Con{+l, +t}:
      %Equal.sym(Nat, sumls(arrive.at(p, j, E{id, want, 0n, pred, 0n, mi, pa}, t)), sumls(t), arrive_at_sum(p, j, id, want, pred, mi, pa, t)) : {add(suml(l), _) == add(suml(l), sumls(t)) : Nat}
      {==}
    case i0 Nil{}:
      {==}

def arrive_total(+p: Pol, +i: Nat, +id: Nat, +want: Nat, +pred: Nat, +mi: Nat, +pa: Nat, +s: St) -> {total(arrive(p, i, E{id, want, 0n, pred, 0n, mi, pa}, s)) == total(s) : Nat}:
  match s:
    case St{f, b, ls}:
      %Equal.sym(Nat, sumls(arrive.at(p, i, E{id, want, 0n, pred, 0n, mi, pa}, ls)), sumls(ls), arrive_at_sum(p, i, id, want, pred, mi, pa, ls)) : {add(f, _) == add(f, sumls(ls)) : Nat}
      {==}

# --- a tick moves requests between a lane's lists, and no permit ----------------------

def split_sum.if(+e: E, +rest: Sp, +aged: Bool, -s: Nat, ih: {add(sumk(sm(rest)), sumk(ss(rest))) == s : Nat})
  -> {add(sumk(sm(split.if(e, rest, aged))), sumk(ss(split.if(e, rest, aged)))) == add(ek(e), s) : Nat}:
  match aged:
    case True{}:
      Equal.trans(Nat, add(add(ek(e), sumk(sm(rest))), sumk(ss(rest))), add(ek(e), add(sumk(sm(rest)), sumk(ss(rest)))), add(ek(e), s),
        add_assoc(ek(e), sumk(sm(rest)), sumk(ss(rest))),
        Equal.cong(Nat, Nat, x => add(ek(e), x), add(sumk(sm(rest)), sumk(ss(rest))), s, ih))
    case False{}:
      Equal.trans(Nat, add(sumk(sm(rest)), add(ek(e), sumk(ss(rest)))), add(ek(e), add(sumk(sm(rest)), sumk(ss(rest)))), add(ek(e), s),
        add_swap(sumk(sm(rest)), ek(e), sumk(ss(rest))),
        Equal.cong(Nat, Nat, x => add(ek(e), x), add(sumk(sm(rest)), sumk(ss(rest))), s, ih))

def split_sum(+n: +List<E>, +b: Nat) -> {add(sumk(sm(split(n, b))), sumk(ss(split(n, b)))) == sumk(n) : Nat}:
  match n:
    case Nil{}:
      {==}
    case Con{E{id, want, k, pred, age, mi, pa}, t}:
      split_sum.if(E{id, want, k, pred, 1n+age, mi, pa}, split(t, b), le(b, 1n+age), sumk(t), split_sum(t, b))

def append_sum(+xs: +List<E>, +ys: +List<E>) -> {sumk(append(xs, ys)) == add(sumk(xs), sumk(ys)) : Nat}:
  match xs:
    case Nil{}:
      {==}
    case Con{h, t}:
      %Equal.sym(Nat, sumk(append(t, ys)), add(sumk(t), sumk(ys)), append_sum(t, ys)) : {add(ek(h), _) == add(add(ek(h), sumk(t)), sumk(ys)) : Nat}
      Equal.sym(Nat, add(add(ek(h), sumk(t)), sumk(ys)), add(ek(h), add(sumk(t), sumk(ys))), add_assoc(ek(h), sumk(t), sumk(ys)))

def insid_sum.if(+y: E, +h: E, +t: +List<E>, +rest: +List<E>, +before: Bool, +ih: {sumk(rest) == add(ek(y), sumk(t)) : Nat})
  -> {sumk(insid.if(y, h, t, rest, before)) == add(ek(y), add(ek(h), sumk(t))) : Nat}:
  match before:
    case True{}:
      {==}
    case False{}:
      %Equal.sym(Nat, sumk(rest), add(ek(y), sumk(t)), ih) : {add(ek(h), _) == add(ek(y), add(ek(h), sumk(t))) : Nat}
      add_swap(ek(h), ek(y), sumk(t))

def insid_sum(+xs: +List<E>, +y: E) -> {sumk(insid(xs, y)) == add(ek(y), sumk(xs)) : Nat}:
  match xs:
    case Nil{}:
      {==}
    case Con{h, t}:
      insid_sum.if(y, h, t, insid(t, y), Bool.not(le(eid(h), eid(y))), insid_sum(t, y))

def insall_sum(+m: +List<E>, +o: +List<E>) -> {sumk(insall(m, o)) == add(sumk(m), sumk(o)) : Nat}:
  match m:
    case Nil{}:
      {==}
    case Con{y, t}:
      Equal.trans(Nat, sumk(insall(t, insid(o, y))), add(sumk(t), sumk(insid(o, y))), add(add(ek(y), sumk(t)), sumk(o)),
        insall_sum(t, insid(o, y)),
      Equal.trans(Nat, add(sumk(t), sumk(insid(o, y))), add(sumk(t), add(ek(y), sumk(o))), add(add(ek(y), sumk(t)), sumk(o)),
        Equal.cong(Nat, Nat, x => add(sumk(t), x), sumk(insid(o, y)), add(ek(y), sumk(o)), insid_sum(o, y)),
      Equal.trans(Nat, add(sumk(t), add(ek(y), sumk(o))), add(ek(y), add(sumk(t), sumk(o))), add(add(ek(y), sumk(t)), sumk(o)),
        add_swap(sumk(t), ek(y), sumk(o)),
        Equal.sym(Nat, add(add(ek(y), sumk(t)), sumk(o)), add(ek(y), add(sumk(t), sumk(o))), add_assoc(ek(y), sumk(t), sumk(o))))))

def tick_lane_sum(+b: Nat, +l: Lane) -> {suml(tick.lane(b, l)) == suml(l) : Nat}:
  match l:
    case Lane{o, n}:
      Equal.trans(Nat, add(sumk(insall(sm(split(n, b)), o)), sumk(ss(split(n, b)))),
        add(add(sumk(sm(split(n, b))), sumk(o)), sumk(ss(split(n, b)))), add(sumk(o), sumk(n)),
        Equal.cong(Nat, Nat, x => add(x, sumk(ss(split(n, b)))), sumk(insall(sm(split(n, b)), o)), add(sumk(sm(split(n, b))), sumk(o)), insall_sum(sm(split(n, b)), o)),
      Equal.trans(Nat, add(add(sumk(sm(split(n, b))), sumk(o)), sumk(ss(split(n, b)))),
        add(add(sumk(o), sumk(sm(split(n, b)))), sumk(ss(split(n, b)))), add(sumk(o), sumk(n)),
        Equal.cong(Nat, Nat, x => add(x, sumk(ss(split(n, b)))), add(sumk(sm(split(n, b))), sumk(o)), add(sumk(o), sumk(sm(split(n, b)))), add_comm(sumk(sm(split(n, b))), sumk(o))),
      Equal.trans(Nat, add(add(sumk(o), sumk(sm(split(n, b)))), sumk(ss(split(n, b)))),
        add(sumk(o), add(sumk(sm(split(n, b))), sumk(ss(split(n, b))))), add(sumk(o), sumk(n)),
        add_assoc(sumk(o), sumk(sm(split(n, b))), sumk(ss(split(n, b)))),
        Equal.cong(Nat, Nat, x => add(sumk(o), x), add(sumk(sm(split(n, b))), sumk(ss(split(n, b)))), sumk(n), split_sum(n, b)))))

def ticks_sum(+b: Nat, +ls: +List<Lane>) -> {sumls(ticks(b, ls)) == sumls(ls) : Nat}:
  match ls:
    case Nil{}:
      {==}
    case Con{+l, +t}:
      %Equal.sym(Nat, suml(tick.lane(b, l)), suml(l), tick_lane_sum(b, l)) : {add(_, sumls(ticks(b, t))) == add(suml(l), sumls(t)) : Nat}
      %Equal.sym(Nat, sumls(ticks(b, t)), sumls(t), ticks_sum(b, t)) : {add(suml(l), _) == add(suml(l), sumls(t)) : Nat}
      {==}

def tick_total(+s: St) -> {total(tick(s)) == total(s) : Nat}:
  match s:
    case St{f, b, ls}:
      %Equal.sym(Nat, sumls(ticks(b, ls)), sumls(ls), ticks_sum(b, ls)) : {add(f, _) == add(f, sumls(ls)) : Nat}
      {==}

# --- an end gives back what its entry held ----------------------------------------------

def fin_sum.if(+id: Nat, +want: Nat, +k: Nat, +pred: Nat, +age: Nat, +mi: Nat, +pa: Nat, +t: +List<E>, +rest: W, +hit: Bool,
  ih: {add(wf(rest), sumk(wl(rest))) == sumk(t) : Nat})
  -> {add(wf(fin.if(id, want, k, pred, age, mi, pa, t, rest, hit)), sumk(wl(fin.if(id, want, k, pred, age, mi, pa, t, rest, hit)))) == add(k, sumk(t)) : Nat}:
  match hit:
    case True{}:
      {==}
    case False{}:
      Equal.trans(Nat, add(wf(W.cons(E{id, want, k, pred, age, mi, pa}, rest)), sumk(wl(W.cons(E{id, want, k, pred, age, mi, pa}, rest)))),
        add(k, add(wf(rest), sumk(wl(rest)))), add(k, sumk(t)),
        cons_sum(E{id, want, k, pred, age, mi, pa}, rest),
        Equal.cong(Nat, Nat, x => add(k, x), add(wf(rest), sumk(wl(rest))), sumk(t), ih))

def fin_sum(+lane: +List<E>, +x: Nat) -> {add(wf(fin(lane, x)), sumk(wl(fin(lane, x)))) == sumk(lane) : Nat}:
  match lane:
    case Nil{}:
      {==}
    case Con{E{id, want, k, pred, age, mi, pa}, t}:
      fin_sum.if(id, want, k, pred, age, mi, pa, t, fin(t, x), same(x, id), fin_sum(t, x))

def lfin_sum(+l: Lane, +x: Nat) -> {add(lf(lfin(l, x)), suml(ll(lfin(l, x)))) == suml(l) : Nat}:
  match l:
    case Lane{o, n}:
      sum2(wf(fin(o, x)), sumk(wl(fin(o, x))), sumk(o), wf(fin(n, x)), sumk(wl(fin(n, x))), sumk(n), fin_sum(o, x), fin_sum(n, x))

def fins_sum(+ls: +List<Lane>, +x: Nat) -> {add(lsf(fins(ls, x)), sumls(lsl(fins(ls, x)))) == sumls(ls) : Nat}:
  match ls:
    case Nil{}:
      {==}
    case Con{+l, +t}:
      sum2(lf(lfin(l, x)), suml(ll(lfin(l, x))), suml(l), lsf(fins(t, x)), sumls(lsl(fins(t, x))), sumls(t), lfin_sum(l, x), fins_sum(t, x))

def finish_total(+x: Nat, +s: St) -> {total(finish(x, s)) == total(s) : Nat}:
  match s:
    case St{f, b, ls}:
      Equal.trans(Nat, add(add(f, lsf(fins(ls, x))), sumls(lsl(fins(ls, x)))),
        add(f, add(lsf(fins(ls, x)), sumls(lsl(fins(ls, x))))), add(f, sumls(ls)),
        add_assoc(f, lsf(fins(ls, x)), sumls(lsl(fins(ls, x)))),
        Equal.cong(Nat, Nat, y => add(f, y), add(lsf(fins(ls, x)), sumls(lsl(fins(ls, x)))), sumls(ls), fins_sum(ls, x)))

# LAW (conservation): no step creates or destroys a permit, so from an empty pool of
# `cap` permits, free plus every entry's grant stays `cap`: the grants never exceed it.
law conserved:
  for +p: Pol
  for +o: Op
  for +s: St
  {total(step(p, o, s)) == total(s) : Nat}

def conserved(p, o, s):
  match o:
    case Arrive{band, id, want, pred, elapsed, mi, pa}:
      arrive_total(p, lane(band, elapsed), id, 1n+want, pred, mi, pa, s)
    case Decide{}:
      decide_total(s)
    case Tick{}:
      tick_total(s)
    case Finish{x}:
      finish_total(x, s)

def run_total(+p: Pol, +os: +List<Op>, +s: St) -> {total(run(p, os, s)) == total(s) : Nat}:
  match os:
    case Nil{}:
      {==}
    case Con{o, t}:
      Equal.trans(Nat, total(run(p, t, step(p, o, s))), total(step(p, o, s)), total(s),
        run_total(p, t, step(p, o, s)), conserved(p, o, s))

law never_overcommits:
  for +p: Pol
  for +os: +List<Op>
  for +cap: Nat
  for +bound: Nat
  {total(run(p, os, empty(cap, bound))) == cap : Nat}

def never_overcommits(p, os, cap, bound):
  Equal.trans(Nat, total(run(p, os, empty(cap, bound))), total(empty(cap, bound)), cap,
    run_total(p, os, empty(cap, bound)), add_zero_r(cap))

# --- a walk leaves its list filled, or nothing free -------------------------------------

def AllFilled(lane: +List<E>) -> Data:
  match lane:
    case Nil{}:
      Unit
    case Con{h, t}:
      Sigma<&2, &2, IsZero(need(h)), u => AllFilled(t)>

def head_fd(+k: Nat, +want: Nat, +f: Nat, +id: Nat, +pred: Nat, +age: Nat, +mi: Nat, +pa: Nat)
  -> Either<&2, &2, IsZero(need(E{id, want, add(k, gg(need(E{id, want, k, pred, age, mi, pa}), f)), pred, age, mi, pa})), IsZero(gr(need(E{id, want, k, pred, age, mi, pa}), f))>:
  match k want f:
    case 0n 0n f0:
      Inl{Unit{}}
    case 0n 1n+a 0n:
      Inr{Unit{}}
    case 0n 1n+a 1n+c:
      Inl{Unit{}}
    case 1n+j w0 f0:
      Inl{Unit{}}

def gg_z(+n: Nat) -> {0n == gg(n, 0n) : Nat}:
  match n:
    case 0n:
      {==}
    case 1n+a:
      {==}

def gr_z(+n: Nat) -> {0n == gr(n, 0n) : Nat}:
  match n:
    case 0n:
      {==}
    case 1n+a:
      {==}

def wl_cons(+h: E, +w: W) -> {h <> wl(w) == wl(W.cons(h, w)) : +List<E>}:
  match w:
    case W{f, l}:
      {==}

def wf_cons(+h: E, +w: W) -> {wf(w) == wf(W.cons(h, w)) : Nat}:
  match w:
    case W{f, l}:
      {==}

def walk_zero_l(+lane: +List<E>) -> {lane == wl(walk(lane, 0n)) : +List<E>}:
  match lane:
    case Nil{}:
      {==}
    case Con{E{id, want, k, pred, age, mi, pa}, t}:
      %gg_z(need(E{id, want, k, pred, age, mi, pa})) : {E{id, want, k, pred, age, mi, pa} <> t == wl(W.cons(E{id, want, add(k, _), pred, age, mi, pa}, walk(t, gr(need(E{id, want, k, pred, age, mi, pa}), 0n)))) : +List<E>}
      %Equal.sym(Nat, add(k, 0n), k, add_zero_r(k)) : {E{id, want, k, pred, age, mi, pa} <> t == wl(W.cons(E{id, want, _, pred, age, mi, pa}, walk(t, gr(need(E{id, want, k, pred, age, mi, pa}), 0n)))) : +List<E>}
      %gr_z(need(E{id, want, k, pred, age, mi, pa})) : {E{id, want, k, pred, age, mi, pa} <> t == wl(W.cons(E{id, want, k, pred, age, mi, pa}, walk(t, _))) : +List<E>}
      %wl_cons(E{id, want, k, pred, age, mi, pa}, walk(t, 0n)) : {E{id, want, k, pred, age, mi, pa} <> t == _ : +List<E>}
      %walk_zero_l(t) : {E{id, want, k, pred, age, mi, pa} <> t == E{id, want, k, pred, age, mi, pa} <> _ : +List<E>}
      {==}

def walk_zero_f(+lane: +List<E>) -> {0n == wf(walk(lane, 0n)) : Nat}:
  match lane:
    case Nil{}:
      {==}
    case Con{E{id, want, k, pred, age, mi, pa}, t}:
      %wf_cons(E{id, want, add(k, gg(need(E{id, want, k, pred, age, mi, pa}), 0n)), pred, age, mi, pa}, walk(t, gr(need(E{id, want, k, pred, age, mi, pa}), 0n))) : {0n == _ : Nat}
      %gr_z(need(E{id, want, k, pred, age, mi, pa})) : {0n == wf(walk(t, _)) : Nat}
      walk_zero_f(t)

def iz_eq(+x: Nat, z: IsZero(x)) -> {0n == x : Nat}:
  match x:
    case 0n:
      {==}
    case 1n+q:
      match z:

def dry_tail(+t: +List<E>, +r: Nat, z: IsZero(r)) -> IsZero(wf(walk(t, r))):
  match r:
    case 0n:
      %walk_zero_f(t) : IsZero(_)
      Unit{}
    case 1n+q:
      match z:

def zero_tail_l(+t: +List<E>, +r: Nat, z: IsZero(r)) -> {wl(walk(t, r)) == t : +List<E>}:
  match r:
    case 0n:
      Equal.sym(+List<E>, t, wl(walk(t, 0n)), walk_zero_l(t))
    case 1n+q:
      match z:

def fd_combine(-A: Data, -B: Data, -C: Data, -D: Data, head: Either<&2, &2, A, D>, ih: Either<&2, &2, B, C>, dry: D -> C)
  -> Either<&2, &2, Sigma<&2, &2, A, u => B>, C>:
  match head:
    case Inl{a}:
      match ih:
        case Inl{b}:
          Inl{(a, b)}
        case Inr{c}:
          Inr{c}
    case Inr{d}:
      Inr{dry(d)}

def walk_fd(+lane: +List<E>, +f: Nat) -> Either<&2, &2, AllFilled(wl(walk(lane, f))), IsZero(wf(walk(lane, f)))>:
  match lane:
    case Nil{}:
      Inl{Unit{}}
    case Con{E{id, want, k, pred, age, mi, pa}, t}:
      %wl_cons(E{id, want, add(k, gg(need(E{id, want, k, pred, age, mi, pa}), f)), pred, age, mi, pa}, walk(t, gr(need(E{id, want, k, pred, age, mi, pa}), f)))
        : Either<&2, &2, AllFilled(_), IsZero(wf(W.cons(E{id, want, add(k, gg(need(E{id, want, k, pred, age, mi, pa}), f)), pred, age, mi, pa}, walk(t, gr(need(E{id, want, k, pred, age, mi, pa}), f)))))>
      %wf_cons(E{id, want, add(k, gg(need(E{id, want, k, pred, age, mi, pa}), f)), pred, age, mi, pa}, walk(t, gr(need(E{id, want, k, pred, age, mi, pa}), f)))
        : Either<&2, &2, AllFilled(E{id, want, add(k, gg(need(E{id, want, k, pred, age, mi, pa}), f)), pred, age, mi, pa} <> wl(walk(t, gr(need(E{id, want, k, pred, age, mi, pa}), f)))), IsZero(_)>
      fd_combine(
        IsZero(need(E{id, want, add(k, gg(need(E{id, want, k, pred, age, mi, pa}), f)), pred, age, mi, pa})),
        AllFilled(wl(walk(t, gr(need(E{id, want, k, pred, age, mi, pa}), f)))),
        IsZero(wf(walk(t, gr(need(E{id, want, k, pred, age, mi, pa}), f)))),
        IsZero(gr(need(E{id, want, k, pred, age, mi, pa}), f)),
        head_fd(k, want, f, id, pred, age, mi, pa),
        walk_fd(t, gr(need(E{id, want, k, pred, age, mi, pa}), f)),
        z => dry_tail(t, gr(need(E{id, want, k, pred, age, mi, pa}), f), z))

# --- a lane walk: its old list first, then its new one --------------------------------

def AllFilledL(l: Lane) -> Data:
  match l:
    case Lane{o, n}:
      Sigma<&2, &2, AllFilled(o), u => AllFilled(n)>

def lwalk_fd(+l: Lane, +f: Nat) -> Either<&2, &2, AllFilledL(ll(lwalk(l, f))), IsZero(lf(lwalk(l, f)))>:
  match l:
    case Lane{o, n}:
      fd_combine(AllFilled(wl(walk(o, f))), AllFilled(wl(walk(n, wf(walk(o, f))))), IsZero(wf(walk(n, wf(walk(o, f))))), IsZero(wf(walk(o, f))),
        walk_fd(o, f), walk_fd(n, wf(walk(o, f))), z => dry_tail(n, wf(walk(o, f)), z))

def lwalk_zero_l(+l: Lane) -> {l == ll(lwalk(l, 0n)) : Lane}:
  match l:
    case Lane{o, n}:
      %walk_zero_f(o) : {Lane{o, n} == Lane{wl(walk(o, 0n)), wl(walk(n, _))} : Lane}
      %walk_zero_l(o) : {Lane{o, n} == Lane{_, wl(walk(n, 0n))} : Lane}
      %walk_zero_l(n) : {Lane{o, n} == Lane{o, _} : Lane}
      {==}

def lwalk_zero_f(+l: Lane) -> {0n == lf(lwalk(l, 0n)) : Nat}:
  match l:
    case Lane{o, n}:
      %walk_zero_f(o) : {0n == wf(walk(n, _)) : Nat}
      walk_zero_f(n)

def lzero_tail(+l: Lane, +r: Nat, z: IsZero(r)) -> {ll(lwalk(l, r)) == l : Lane}:
  match r:
    case 0n:
      Equal.sym(Lane, l, ll(lwalk(l, 0n)), lwalk_zero_l(l))
    case 1n+q:
      match z:

def ldry_tail(+l: Lane, +r: Nat, z: IsZero(r)) -> IsZero(lf(lwalk(l, r))):
  match r:
    case 0n:
      %lwalk_zero_f(l) : IsZero(_)
      Unit{}
    case 1n+q:
      match z:

def sfree(s: St) -> Nat:
  match s:
    case St{f, b, ls}:
      f

def slanes(s: St) -> +List<Lane>:
  match s:
    case St{f, b, ls}:
      ls

# Lane `i` of `ls`, or an empty lane past the last.
def nth(i: Nat, ls: +List<Lane>) -> Lane:
  match i ls:
    case 0n Con{+l, +t}:
      l
    case 1n+j Con{+l, +t}:
      nth(j, t)
    case i0 Nil{}:
      Lane{[], []}

# `R` holds of every lane.
def AllL(R: Lane -> Data, ls: +List<Lane>) -> Data:
  match ls:
    case Nil{}:
      Unit
    case Con{+l, +t}:
      Sigma<&2, &2, R(l), u => AllL(R, t)>

# `R` holds of every lane before a step and the same lane after it.
def All2(R: Lane -> Lane -> Data, ls: +List<Lane>, ms: +List<Lane>) -> Data:
  match ls ms:
    case Nil{} Nil{}:
      Unit
    case Con{l, t} Con{m, u}:
      Sigma<&2, &2, R(l, m), v => All2(R, t, u)>
    case Nil{} Con{m, u}:
      Empty
    case Con{l, t} Nil{}:
      Empty

# Lane by lane: each lane after a decision needs nothing, or every lane after it is as it was.
def Before(ls: +List<Lane>, ms: +List<Lane>) -> Data:
  match ls ms:
    case Nil{} Nil{}:
      Unit
    case Con{l, t} Con{m, u}:
      Sigma<&2, &2, Either<&2, &2, AllFilledL(m), {u == t : +List<Lane>}>, v => Before(t, u)>
    case Nil{} Con{m, u}:
      Empty
    case Con{l, t} Nil{}:
      Empty

def walks_zero_l(+ls: +List<Lane>) -> {ls == lsl(walks(ls, 0n)) : +List<Lane>}:
  match ls:
    case Nil{}:
      {==}
    case Con{+l, +t}:
      %lwalk_zero_f(l) : {Con{l, t} == Con{ll(lwalk(l, 0n)), lsl(walks(t, _))} : +List<Lane>}
      %lwalk_zero_l(l) : {Con{l, t} == Con{_, lsl(walks(t, 0n))} : +List<Lane>}
      %walks_zero_l(t) : {Con{l, t} == Con{l, _} : +List<Lane>}
      {==}

def walks_zero_f(+ls: +List<Lane>) -> {0n == lsf(walks(ls, 0n)) : Nat}:
  match ls:
    case Nil{}:
      {==}
    case Con{+l, +t}:
      %lwalk_zero_f(l) : {0n == lsf(walks(t, _)) : Nat}
      walks_zero_f(t)

def lszero_tail(+ls: +List<Lane>, +r: Nat, z: IsZero(r)) -> {lsl(walks(ls, r)) == ls : +List<Lane>}:
  match r:
    case 0n:
      Equal.sym(+List<Lane>, ls, lsl(walks(ls, 0n)), walks_zero_l(ls))
    case 1n+q:
      match z:

def lsdry_tail(+ls: +List<Lane>, +r: Nat, z: IsZero(r)) -> IsZero(lsf(walks(ls, r))):
  match r:
    case 0n:
      %walks_zero_f(ls) : IsZero(_)
      Unit{}
    case 1n+q:
      match z:

# --- LAW (lanes in order): a lane gains a permit only once every lane before it needs none ---
# This and the next two laws are today's decision's (`step.today`). A gather breaks each of them
# on purpose: it leaves permits free while it waits and lets requests behind it pass. They hold
# of `step` for every state without a minimum (`..._without_min`, below `min_le_one_is_today`).

law lanes_in_order:
  for +p: Pol
  for +s: St
  Before(slanes(s), slanes(step.today(p, Decide{}, s)))

def lanes_in_order.go(+l: Lane, +t: +List<Lane>, +f: Nat,
  fd: Either<&2, &2, AllFilledL(ll(lwalk(l, f))), IsZero(lf(lwalk(l, f)))>)
  -> Either<&2, &2, AllFilledL(ll(lwalk(l, f))), {lsl(walks(t, lf(lwalk(l, f)))) == t : +List<Lane>}>:
  match fd:
    case Inl{a}:
      Inl{a}
    case Inr{z}:
      Inr{lszero_tail(t, lf(lwalk(l, f)), z)}

def lanes_before(+ls: +List<Lane>, +f: Nat) -> Before(ls, lsl(walks(ls, f))):
  match ls:
    case Nil{}:
      Unit{}
    case Con{+l, +t}:
      (lanes_in_order.go(l, t, f, lwalk_fd(l, f)), lanes_before(t, lf(lwalk(l, f))))

def lanes_in_order(p, s):
  match s:
    case St{f, b, ls}:
      lanes_before(ls, f)

# --- LAW (work conserving): no permit stays free while any request needs one -------------
# Landing gets priority, never exclusivity: a later lane is admitted whenever earlier ones fill.

law work_conserving:
  for +p: Pol
  for +s: St
  Either<&2, &2, AllL(l => AllFilledL(l), slanes(step.today(p, Decide{}, s))), IsZero(sfree(step.today(p, Decide{}, s)))>

def walks_fd(+ls: +List<Lane>, +f: Nat) -> Either<&2, &2, AllL(l => AllFilledL(l), lsl(walks(ls, f))), IsZero(lsf(walks(ls, f)))>:
  match ls:
    case Nil{}:
      Inl{Unit{}}
    case Con{+l, +t}:
      fd_combine(AllFilledL(ll(lwalk(l, f))), AllL(m => AllFilledL(m), lsl(walks(t, lf(lwalk(l, f))))), IsZero(lsf(walks(t, lf(lwalk(l, f))))), IsZero(lf(lwalk(l, f))),
        lwalk_fd(l, f), walks_fd(t, lf(lwalk(l, f))), z => lsdry_tail(t, lf(lwalk(l, f)), z))

def work_conserving(p, s):
  match s:
    case St{f, b, ls}:
      walks_fd(ls, f)

# --- LAW (first in order, first filled) ----------------------------------------------------
# In each list a request is filled, or nothing after it changed; in each lane, `new` changes
# only once `old` is filled. With `old` in arrival order and `new` in Spt's predicted order,
# that is the policy's order: an aged request first, oldest first, then the shortest.

def Order(xs: +List<E>, ys: +List<E>) -> Data:
  match xs ys:
    case Nil{} Nil{}:
      Unit
    case Con{x, xt} Con{y, yt}:
      Either<&2, &2, Sigma<&2, &2, IsZero(need(y)), u => Order(xt, yt)>, {yt == xt : +List<E>}>
    case Nil{} Con{y, yt}:
      Empty
    case Con{x, xt} Nil{}:
      Empty

def order_head(-A: Data, -B: Data, -C: Data, -D: Data, head: Either<&2, &2, A, D>, ih: B, rest: D -> C)
  -> Either<&2, &2, Sigma<&2, &2, A, u => B>, C>:
  match head:
    case Inl{a}:
      Inl{(a, ih)}
    case Inr{d}:
      Inr{rest(d)}

def walk_order(+lane: +List<E>, +f: Nat) -> Order(lane, wl(walk(lane, f))):
  match lane:
    case Nil{}:
      Unit{}
    case Con{E{id, want, k, pred, age, mi, pa}, t}:
      %wl_cons(E{id, want, add(k, gg(need(E{id, want, k, pred, age, mi, pa}), f)), pred, age, mi, pa}, walk(t, gr(need(E{id, want, k, pred, age, mi, pa}), f)))
        : Order(E{id, want, k, pred, age, mi, pa} <> t, _)
      order_head(
        IsZero(need(E{id, want, add(k, gg(need(E{id, want, k, pred, age, mi, pa}), f)), pred, age, mi, pa})),
        Order(t, wl(walk(t, gr(need(E{id, want, k, pred, age, mi, pa}), f)))),
        {wl(walk(t, gr(need(E{id, want, k, pred, age, mi, pa}), f))) == t : +List<E>},
        IsZero(gr(need(E{id, want, k, pred, age, mi, pa}), f)),
        head_fd(k, want, f, id, pred, age, mi, pa),
        walk_order(t, gr(need(E{id, want, k, pred, age, mi, pa}), f)),
        z => zero_tail_l(t, gr(need(E{id, want, k, pred, age, mi, pa}), f), z))

def lold(l: Lane) -> +List<E>:
  match l:
    case Lane{o, n}:
      o

def lnew(l: Lane) -> +List<E>:
  match l:
    case Lane{o, n}:
      n

def OrderL(l: Lane, m: Lane) -> Data:
  Sigma<&2, &2, Order(lold(l), lold(m)), u => Sigma<&2, &2, Order(lnew(l), lnew(m)), v => Either<&2, &2, AllFilled(lold(m)), {lnew(m) == lnew(l) : +List<E>}>>>

def old_first(+o: +List<E>, +n: +List<E>, +f: Nat, fd: Either<&2, &2, AllFilled(wl(walk(o, f))), IsZero(wf(walk(o, f)))>)
  -> Either<&2, &2, AllFilled(wl(walk(o, f))), {wl(walk(n, wf(walk(o, f)))) == n : +List<E>}>:
  match fd:
    case Inl{a}:
      Inl{a}
    case Inr{z}:
      Inr{zero_tail_l(n, wf(walk(o, f)), z)}

def lwalk_order(+l: Lane, +f: Nat) -> OrderL(l, ll(lwalk(l, f))):
  match l:
    case Lane{o, n}:
      (walk_order(o, f), (walk_order(n, wf(walk(o, f))), old_first(o, n, f, walk_fd(o, f))))

law first_in_order_filled:
  for +p: Pol
  for +s: St
  All2(l => m => OrderL(l, m), slanes(s), slanes(step.today(p, Decide{}, s)))

def walks_order(+ls: +List<Lane>, +f: Nat) -> All2(l => m => OrderL(l, m), ls, lsl(walks(ls, f))):
  match ls:
    case Nil{}:
      Unit{}
    case Con{+l, +t}:
      (lwalk_order(l, f), walks_order(t, lf(lwalk(l, f))))

def first_in_order_filled(p, s):
  match s:
    case St{f, b, ls}:
      walks_order(ls, f)

# --- LAW (no preemption): nothing but a request's own end takes a permit back ---------------

def same_refl(+n: Nat) -> IsTrue(same(n, n)):
  match n:
    case 0n:
      Unit{}
    case 1n+q:
      same_refl(q)

def le_add(+k: Nat, +g: Nat) -> LE(k, add(k, g)):
  match k:
    case 0n:
      Unit{}
    case 1n+q:
      le_add(q, g)

def LE_refl(+n: Nat) -> LE(n, n):
  match n:
    case 0n:
      Unit{}
    case 1n+q:
      LE_refl(q)

# A decision keeps every request where it was, with its grant or more.
def Kept(xs: +List<E>, ys: +List<E>) -> Data:
  match xs ys:
    case Nil{} Nil{}:
      Unit
    case Con{x, xt} Con{y, yt}:
      Sigma<&2, &2, IsTrue(same(eid(x), eid(y))), u => Sigma<&2, &2, LE(ek(x), ek(y)), v => Kept(xt, yt)>>
    case Nil{} Con{y, yt}:
      Empty
    case Con{x, xt} Nil{}:
      Empty

def walk_kept(+lane: +List<E>, +f: Nat) -> Kept(lane, wl(walk(lane, f))):
  match lane:
    case Nil{}:
      Unit{}
    case Con{E{id, want, k, pred, age, mi, pa}, t}:
      %wl_cons(E{id, want, add(k, gg(need(E{id, want, k, pred, age, mi, pa}), f)), pred, age, mi, pa}, walk(t, gr(need(E{id, want, k, pred, age, mi, pa}), f)))
        : Kept(E{id, want, k, pred, age, mi, pa} <> t, _)
      (same_refl(id), (le_add(k, gg(need(E{id, want, k, pred, age, mi, pa}), f)), walk_kept(t, gr(need(E{id, want, k, pred, age, mi, pa}), f))))

def gwalk_kept(+lo: Bool, +lane: +List<E>, +f: Nat, +g: +List<Nat>) -> Kept(lane, wl(gwalk(lo, lane, f, g))):
  match lane:
    case Nil{}:
      Unit{}
    case Con{E{id, want, k, pred, age, mi, pa}, t}:
      %wl_cons(E{id, want, add(k, gg(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f)), pred, age, mi, pa}, gwalk(lo, t, gr(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f), gnext(gact(lo, g, E{id, want, k, pred, age, mi, pa}, f), pa, g)))
        : Kept(E{id, want, k, pred, age, mi, pa} <> t, _)
      (same_refl(id), (le_add(k, gg(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f)),
        gwalk_kept(lo, t, gr(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f), gnext(gact(lo, g, E{id, want, k, pred, age, mi, pa}, f), pa, g))))

def kept_refl(+xs: +List<E>) -> Kept(xs, xs):
  match xs:
    case Nil{}:
      Unit{}
    case Con{h, t}:
      (same_refl(eid(h)), (LE_refl(ek(h)), kept_refl(t)))

def KeptL(l: Lane, m: Lane) -> Data:
  Sigma<&2, &2, Kept(lold(l), lold(m)), u => Kept(lnew(l), lnew(m))>

def glwalk_kept.if(+sk: Bool, +lo: Bool, +o: +List<E>, +n: +List<E>, +f: Nat, +g: +List<Nat>) -> KeptL(Lane{o, n}, ll(glwalk.if(sk, lo, o, n, f, g))):
  match sk:
    case True{}:
      (kept_refl(o), kept_refl(n))
    case False{}:
      (walk_kept(o, f), gwalk_kept(lo, n, wf(walk(o, f)), g))

def glwalk_kept(+lo: Bool, +l: Lane, +f: Nat, +g: +List<Nat>) -> KeptL(l, ll(glwalk(lo, l, f, g))):
  match l:
    case Lane{o, n}:
      glwalk_kept.if(skip(lo, g), lo, o, n, f, g)

law decide_never_preempts:
  for +p: Pol
  for +s: St
  All2(l => m => KeptL(l, m), slanes(s), slanes(step(p, Decide{}, s)))

def gwalks_kept(+ls: +List<Lane>, +f: Nat, +g: +List<Nat>) -> All2(l => m => KeptL(l, m), ls, lsl(gwalks(ls, f, g))):
  match ls:
    case Nil{}:
      Unit{}
    case Con{+l, +t}:
      (glwalk_kept(islow(t), l, f, g), gwalks_kept(t, lf(glwalk(islow(t), l, f, g)), glg(islow(t), l, f, g)))

def decide_never_preempts(p, s):
  match s:
    case St{f, b, ls}:
      gwalks_kept(ls, f, [])

# Every entry of xs is in ys, unchanged and in its order.
def Sub(xs: +List<E>, ys: +List<E>) -> Data:
  match xs ys:
    case Nil{} ys0:
      Unit
    case Con{x, xt} Nil{}:
      Empty
    case Con{x, xt} Con{y, yt}:
      Either<&2, &2, Sigma<&2, &2, {x == y : E}, u => Sub(xt, yt)>, Sub(x <> xt, yt)>

def sub_refl(+xs: +List<E>) -> Sub(xs, xs):
  match xs:
    case Nil{}:
      Unit{}
    case Con{h, t}:
      Inl{({==}, sub_refl(t))}

def sub_snoc(+xs: +List<E>, +y: E) -> Sub(xs, snoc(xs, y)):
  match xs:
    case Nil{}:
      Unit{}
    case Con{h, t}:
      Inl{({==}, sub_snoc(t, y))}

def sub_insert.if(+y: E, +h: E, +t: +List<E>, +rest: +List<E>, +before: Bool, ih: Sub(t, rest))
  -> Sub(h <> t, insert.if(y, h, t, rest, before)):
  match before:
    case True{}:
      Inr{sub_refl(h <> t)}
    case False{}:
      Inl{({==}, ih)}

def sub_insert(+xs: +List<E>, +y: E) -> Sub(xs, insert(xs, y)):
  match xs:
    case Nil{}:
      Unit{}
    case Con{h, t}:
      sub_insert.if(y, h, t, insert(t, y), Bool.not(le(epred(h), epred(y))), sub_insert(t, y))

def sub_enter(+p: Pol, +n: +List<E>, +y: E) -> Sub(n, enter(p, n, y)):
  match p:
    case Today{}:
      sub_snoc(n, y)
    case Spt{}:
      sub_insert(n, y)

def SubL(l: Lane, m: Lane) -> Data:
  Sigma<&2, &2, Sub(lold(l), lold(m)), u => Sub(lnew(l), lnew(m))>

def subl_refl(+l: Lane) -> SubL(l, l):
  match l:
    case Lane{o, n}:
      (sub_refl(o), sub_refl(n))

def subl_arrive(+p: Pol, +y: E, +l: Lane) -> SubL(l, arrive.lane(p, y, l)):
  match l:
    case Lane{o, n}:
      (sub_refl(o), sub_enter(p, n, y))

def subls_refl(+ls: +List<Lane>) -> All2(l => m => SubL(l, m), ls, ls):
  match ls:
    case Nil{}:
      Unit{}
    case Con{+l, +t}:
      (subl_refl(l), subls_refl(t))

def subls_arrive(+p: Pol, +i: Nat, +y: E, +ls: +List<Lane>) -> All2(l => m => SubL(l, m), ls, arrive.at(p, i, y, ls)):
  match i ls:
    case 0n Con{+l, +t}:
      (subl_arrive(p, y, l), subls_refl(t))
    case 1n+j Con{+l, +t}:
      (subl_refl(l), subls_arrive(p, j, y, t))
    case i0 Nil{}:
      Unit{}

law arrival_never_preempts:
  for +p: Pol
  for +band: Band
  for +id: Nat
  for +want: Nat
  for +pred: Nat
  for +elapsed: Nat
  for +mi: Nat
  for +pa: Nat
  for +s: St
  All2(l => m => SubL(l, m), slanes(s), slanes(step(p, Arrive{band, id, want, pred, elapsed, mi, pa}, s)))

def arrival_never_preempts(p, band, id, want, pred, elapsed, mi, pa, s):
  match s:
    case St{f, b, ls}:
      subls_arrive(p, lane(band, elapsed), E{id, 1n+want, 0n, pred, 0n, mi, pa}, ls)

# An end removes its own entry; every other entry stays as it was.
def sub_cons_r(+xs: +List<E>, +e: E) -> Sub(xs, e <> xs):
  match xs:
    case Nil{}:
      Unit{}
    case Con{h, t}:
      Inr{sub_refl(h <> t)}

def sub_fin.if(+id: Nat, +want: Nat, +k: Nat, +pred: Nat, +age: Nat, +mi: Nat, +pa: Nat, +t: +List<E>, +rest: W, +hit: Bool, ih: Sub(wl(rest), t))
  -> Sub(wl(fin.if(id, want, k, pred, age, mi, pa, t, rest, hit)), E{id, want, k, pred, age, mi, pa} <> t):
  match hit:
    case True{}:
      sub_cons_r(t, E{id, want, k, pred, age, mi, pa})
    case False{}:
      %wl_cons(E{id, want, k, pred, age, mi, pa}, rest)
        : Sub(_, E{id, want, k, pred, age, mi, pa} <> t)
      Inl{({==}, ih)}

def sub_fin(+lane: +List<E>, +x: Nat) -> Sub(wl(fin(lane, x)), lane):
  match lane:
    case Nil{}:
      Unit{}
    case Con{E{id, want, k, pred, age, mi, pa}, t}:
      sub_fin.if(id, want, k, pred, age, mi, pa, t, fin(t, x), same(x, id), sub_fin(t, x))

def subl_fin(+l: Lane, +x: Nat) -> SubL(ll(lfin(l, x)), l):
  match l:
    case Lane{o, n}:
      (sub_fin(o, x), sub_fin(n, x))

law finish_takes_only_its_own:
  for +p: Pol
  for +x: Nat
  for +s: St
  All2(l => m => SubL(l, m), slanes(step(p, Finish{x}, s)), slanes(s))

def subls_fin(+ls: +List<Lane>, +x: Nat) -> All2(l => m => SubL(l, m), lsl(fins(ls, x)), ls):
  match ls:
    case Nil{}:
      Unit{}
    case Con{+l, +t}:
      (subl_fin(l, x), subls_fin(t, x))

def finish_takes_only_its_own(p, x, s):
  match s:
    case St{f, b, ls}:
      subls_fin(ls, x)

# --- LAW (a tick takes no permit): it regroups `new`, each entry keeping its id and grant -----

def eage(e: E) -> Nat:
  match e:
    case E{id, want, k, pred, age, mi, pa}:
      age

def Regroup(x: E, y: E) -> Data:
  Sigma<&2, &2, {eid(y) == eid(x) : Nat}, u => Sigma<&2, &2, {ek(y) == ek(x) : Nat}, v => {eage(y) == 1n+eage(x) : Nat}>>

# xs split into ms and ss, in order, each entry regrouped.
def Il(xs: +List<E>, ms: +List<E>, ss: +List<E>) -> Data:
  match xs ms ss:
    case Nil{} Nil{} Nil{}:
      Unit
    case Nil{} Nil{} Con{b, bt}:
      Empty
    case Nil{} Con{a, at} s0:
      Empty
    case Con{x, xt} Nil{} Nil{}:
      Empty
    case Con{x, xt} Con{a, at} Nil{}:
      Sigma<&2, &2, Regroup(x, a), u => Il(xt, at, [])>
    case Con{x, xt} Nil{} Con{b, bt}:
      Sigma<&2, &2, Regroup(x, b), u => Il(xt, [], bt)>
    case Con{x, xt} Con{a, at} Con{b, bt}:
      Either<&2, &2, Sigma<&2, &2, Regroup(x, a), u => Il(xt, at, b <> bt)>, Sigma<&2, &2, Regroup(x, b), u => Il(xt, a <> at, bt)>>

def split_il.go(+x: E, +y: E, +t: +List<E>, +aged: Bool, +s: +List<E>, +m: +List<E>, r: Regroup(x, y), ih: Il(t, m, s))
  -> Il(x <> t, sm(split.if(y, Sp{m, s}, aged)), ss(split.if(y, Sp{m, s}, aged))):
  match aged s m:
    case True{} Nil{} m0:
      (r, ih)
    case True{} Con{b, bt} m0:
      Inl{(r, ih)}
    case False{} s0 Nil{}:
      (r, ih)
    case False{} s0 Con{a, at}:
      Inr{(r, ih)}

def split_il.if(+x: E, +y: E, +t: +List<E>, +rest: Sp, +aged: Bool, r: Regroup(x, y), ih: Il(t, sm(rest), ss(rest)))
  -> Il(x <> t, sm(split.if(y, rest, aged)), ss(split.if(y, rest, aged))):
  match rest:
    case Sp{m, s}:
      split_il.go(x, y, t, aged, s, m, r, ih)

def split_il(+n: +List<E>, +b: Nat) -> Il(n, sm(split(n, b)), ss(split(n, b))):
  match n:
    case Nil{}:
      Unit{}
    case Con{E{id, want, k, pred, age, mi, pa}, t}:
      split_il.if(E{id, want, k, pred, age, mi, pa}, E{id, want, k, pred, 1n+age, mi, pa}, t, split(t, b), le(b, 1n+age),
        ({==}, ({==}, {==})), split_il(t, b))

def hd_eq(+x: E, +y: E, +xt: +List<E>, +yt: +List<E>, +q: Sigma<&2, &2, {x == y : E}, u => Sub(xt, yt)>) -> {x == y : E}:
  (e, r) = q
  e

def hd_sub(+x: E, +y: E, +xt: +List<E>, +yt: +List<E>, +q: Sigma<&2, &2, {x == y : E}, u => Sub(xt, yt)>) -> Sub(xt, yt):
  (e, r) = q
  r

def sub_trans(+a: +List<E>, +b: +List<E>, +c: +List<E>, +bc: Sub(b, c), +ab: Sub(a, b)) -> Sub(a, c):
  match a b c:
    case Nil{} b0 c0:
      Unit{}
    case Con{x, xt} Nil{} c0:
      match ab:
    case Con{x, xt} Con{y, yt} Nil{}:
      match bc:
    case Con{x, xt} Con{y, yt} Con{z, zt}:
      match bc:
        case Inr{r}:
          Inr{sub_trans(x <> xt, y <> yt, zt, r, ab)}
        case Inl{q}:
          match ab:
            case Inl{q2}:
              Inl{(Equal.trans(E, x, y, z, hd_eq(x, y, xt, yt, q2), hd_eq(y, z, yt, zt, q)), sub_trans(xt, yt, zt, hd_sub(y, z, yt, zt, q), hd_sub(x, y, xt, yt, q2)))}
            case Inr{r2}:
              Inr{sub_trans(x <> xt, yt, zt, hd_sub(y, z, yt, zt, q), r2)}

def sub_insid.if(+y: E, +h: E, +t: +List<E>, +rest: +List<E>, +before: Bool, +ih: Sub(t, rest))
  -> Sub(h <> t, insid.if(y, h, t, rest, before)):
  match before:
    case True{}:
      Inr{sub_refl(h <> t)}
    case False{}:
      Inl{({==}, ih)}

def sub_insid(+xs: +List<E>, +y: E) -> Sub(xs, insid(xs, y)):
  match xs:
    case Nil{}:
      Unit{}
    case Con{h, t}:
      sub_insid.if(y, h, t, insid(t, y), Bool.not(le(eid(h), eid(y))), sub_insid(t, y))

def sub_insall(+m: +List<E>, +o: +List<E>) -> Sub(o, insall(m, o)):
  match m:
    case Nil{}:
      sub_refl(o)
    case Con{y, t}:
      sub_trans(o, insid(o, y), insall(t, insid(o, y)), sub_insall(t, insid(o, y)), sub_insid(o, y))

# Old keeps every entry it had, in order, and gains the regrouped aged entries by ticket;
# every entry of new is in moved or in the new `new`, with its id and grant.
def TickL(b: Nat, l: Lane) -> Data:
  Sigma<&2, &2, Sub(lold(l), lold(tick.lane(b, l))), u => Sigma<&2, &2, {lold(tick.lane(b, l)) == insall(sm(split(lnew(l), b)), lold(l)) : +List<E>}, v => Il(lnew(l), sm(split(lnew(l), b)), lnew(tick.lane(b, l)))>>

def tick_lane_law(+b: Nat, +l: Lane) -> TickL(b, l):
  match l:
    case Lane{o, n}:
      (sub_insall(sm(split(n, b)), o), ({==}, split_il(n, b)))

def sbound(s: St) -> Nat:
  match s:
    case St{f, b, ls}:
      b

law tick_regroups:
  for +p: Pol
  for +s: St
  Sigma<&2, &2, {slanes(step(p, Tick{}, s)) == ticks(sbound(s), slanes(s)) : +List<Lane>}, u =>
  AllL(l => TickL(sbound(s), l), slanes(s))>

def ticks_law(+b: Nat, +ls: +List<Lane>) -> AllL(l => TickL(b, l), ls):
  match ls:
    case Nil{}:
      Unit{}
    case Con{+l, +t}:
      (tick_lane_law(b, l), ticks_law(b, t))

def tick_regroups(p, s):
  match s:
    case St{f, b, ls}:
      ({==}, ticks_law(b, ls))

# --- LAW (bounded wait): a request leaves `new` once it has waited its bound ---------------
# After a tick every request still in `new` has waited fewer than `bound` ticks, and every one
# moved to `old` has waited `bound` or more. An arrival enters `new` at age 0, so a request
# is ordered by its prediction for at most `bound` ticks, and from then on by its arrival.

def Young(+b: Nat, xs: +List<E>) -> Data:
  match xs:
    case Nil{}:
      Unit
    case Con{x, xt}:
      Sigma<&2, &2, IsTrue(Bool.not(le(b, eage(x)))), u => Young(b, xt)>

def Aged(+b: Nat, xs: +List<E>) -> Data:
  match xs:
    case Nil{}:
      Unit
    case Con{x, xt}:
      Sigma<&2, &2, IsTrue(le(b, eage(x))), u => Aged(b, xt)>

def split_age.if(+b: Nat, +y: E, +rest: Sp, +aged: Bool, ea: {aged == le(b, eage(y)) : Bool},
  ih: Sigma<&2, &2, Aged(b, sm(rest)), u => Young(b, ss(rest))>)
  -> Sigma<&2, &2, Aged(b, sm(split.if(y, rest, aged))), u => Young(b, ss(split.if(y, rest, aged)))>:
  match aged:
    case True{}:
      (ihm, ihs) = ih
      ((%ea : IsTrue(_); Unit{}, ihm), ihs)
    case False{}:
      (ihm, ihs) = ih
      (ihm, (%ea : IsTrue(Bool.not(_)); Unit{}, ihs))

def split_age(+n: +List<E>, +b: Nat) -> Sigma<&2, &2, Aged(b, sm(split(n, b))), u => Young(b, ss(split(n, b)))>:
  match n:
    case Nil{}:
      (Unit{}, Unit{})
    case Con{E{id, want, k, pred, age, mi, pa}, t}:
      split_age.if(b, E{id, want, k, pred, 1n+age, mi, pa}, split(t, b), le(b, 1n+age), {==}, split_age(t, b))

law tick_bounds_wait:
  for +p: Pol
  for +s: St
  AllL(l => Young(sbound(s), lnew(l)), slanes(step(p, Tick{}, s)))

def snd_young.go(+b: Nat, +n: +List<E>, ih: Sigma<&2, &2, Aged(b, sm(split(n, b))), u => Young(b, ss(split(n, b)))>) -> Young(b, ss(split(n, b))):
  (ihm, ihs) = ih
  ihs

def snd_young(+b: Nat, +n: +List<E>) -> Young(b, ss(split(n, b))):
  snd_young.go(b, n, split_age(n, b))

def tick_lane_young(+b: Nat, +l: Lane) -> Young(b, lnew(tick.lane(b, l))):
  match l:
    case Lane{o, n}:
      snd_young(b, n)

def ticks_young(+b: Nat, +ls: +List<Lane>) -> AllL(l => Young(b, lnew(l)), ticks(b, ls)):
  match ls:
    case Nil{}:
      Unit{}
    case Con{+l, +t}:
      (tick_lane_young(b, l), ticks_young(b, t))

def tick_bounds_wait(p, s):
  match s:
    case St{f, b, ls}:
      ticks_young(b, ls)

# --- LAW (shortest predicted first): under Spt every lane's `new` stays sorted -----------
# by prediction, so a decision, which walks `new` in its order, serves the request predicted
# shortest first among those that have not aged.

def le_refl(+a: Nat) -> IsTrue(le(a, a)):
  match a:
    case 0n:
      Unit{}
    case 1n+p:
      le_refl(p)

def le_trans(+a: Nat, +b: Nat, +c: Nat, ab: IsTrue(le(a, b)), bc: IsTrue(le(b, c))) -> IsTrue(le(a, c)):
  match a b c:
    case 0n b0 c0:
      Unit{}
    case 1n+a1 0n c0:
      match ab:
    case 1n+a1 1n+b1 0n:
      match bc:
    case 1n+a1 1n+b1 1n+c1:
      le_trans(a1, b1, c1, ab, bc)

def le_total(+a: Nat, +b: Nat, n: IsTrue(Bool.not(le(a, b)))) -> IsTrue(le(b, a)):
  match a b:
    case 0n b0:
      match n:
    case 1n+a1 0n:
      Unit{}
    case 1n+a1 1n+b1:
      le_total(a1, b1, n)

def AllGe(+p: Nat, xs: +List<E>) -> Data:
  match xs:
    case Nil{}:
      Unit
    case Con{x, xt}:
      Sigma<&2, &2, IsTrue(le(p, epred(x))), u => AllGe(p, xt)>

def Sorted(xs: +List<E>) -> Data:
  match xs:
    case Nil{}:
      Unit
    case Con{x, xt}:
      Sigma<&2, &2, AllGe(epred(x), xt), u => Sorted(xt)>

def allge_weaken(+q: Nat, +p: Nat, +xs: +List<E>, +qp: IsTrue(le(q, p)), +a: AllGe(p, xs)) -> AllGe(q, xs):
  match xs:
    case Nil{}:
      Unit{}
    case Con{x, xt}:
      (h, t) = a
      (le_trans(q, p, epred(x), qp, h), allge_weaken(q, p, xt, qp, t))

def allge_insert.if(+p: Nat, +y: E, +h: E, +t: +List<E>, +rest: +List<E>, +before: Bool,
  +py: IsTrue(le(p, epred(y))), +ph: IsTrue(le(p, epred(h))), +pt: AllGe(p, t), +ih: AllGe(p, rest))
  -> AllGe(p, insert.if(y, h, t, rest, before)):
  match before:
    case True{}:
      (py, (ph, pt))
    case False{}:
      (ph, ih)

def allge_insert(+p: Nat, +xs: +List<E>, +y: E, +a: AllGe(p, xs), +py: IsTrue(le(p, epred(y)))) -> AllGe(p, insert(xs, y)):
  match xs:
    case Nil{}:
      (py, Unit{})
    case Con{h, t}:
      (ph, pt) = a
      allge_insert.if(p, y, h, t, insert(t, y), Bool.not(le(epred(h), epred(y))), py, ph, pt, allge_insert(p, t, y, pt, py))

def true_is(+b: Bool, e: {True{} == b : Bool}) -> IsTrue(b):
  %e : IsTrue(_)
  Unit{}

def not_false(+x: Bool, e: {False{} == Bool.not(x) : Bool}) -> IsTrue(x):
  match x:
    case True{}:
      Unit{}
    case False{}:
      %e : IsTrue(Bool.not(_))
      Unit{}

def sorted_insert.if(+y: E, +h: E, +t: +List<E>, +before: Bool,
  +eb: {before == Bool.not(le(epred(h), epred(y))) : Bool},
  +sh: AllGe(epred(h), t), +st: Sorted(t), +ih: Sorted(insert(t, y)))
  -> Sorted(insert.if(y, h, t, insert(t, y), before)):
  match before:
    case True{}:
      ((le_total(epred(h), epred(y), true_is(Bool.not(le(epred(h), epred(y))), eb)),
        allge_weaken(epred(y), epred(h), t, le_total(epred(h), epred(y), true_is(Bool.not(le(epred(h), epred(y))), eb)), sh)),
       (sh, st))
    case False{}:
      (allge_insert(epred(h), t, y, sh, not_false(le(epred(h), epred(y)), eb)), ih)

def sorted_insert(+xs: +List<E>, +y: E, +s: Sorted(xs)) -> Sorted(insert(xs, y)):
  match xs:
    case Nil{}:
      (Unit{}, Unit{})
    case Con{h, t}:
      (sh, st) = s
      sorted_insert.if(y, h, t, Bool.not(le(epred(h), epred(y))), {==}, sh, st, sorted_insert(t, y, st))

def gwalk_allge(+lo: Bool, +p: Nat, +n: +List<E>, +f: Nat, +g: +List<Nat>, +a: AllGe(p, n)) -> AllGe(p, wl(gwalk(lo, n, f, g))):
  match n:
    case Nil{}:
      Unit{}
    case Con{E{id, want, k, pred, age, mi, pa}, t}:
      (h, at) = a
      %wl_cons(E{id, want, add(k, gg(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f)), pred, age, mi, pa}, gwalk(lo, t, gr(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f), gnext(gact(lo, g, E{id, want, k, pred, age, mi, pa}, f), pa, g))) : AllGe(p, _)
      (h, gwalk_allge(lo, p, t, gr(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f), gnext(gact(lo, g, E{id, want, k, pred, age, mi, pa}, f), pa, g), at))

def gwalk_sorted(+lo: Bool, +n: +List<E>, +f: Nat, +g: +List<Nat>, +s: Sorted(n)) -> Sorted(wl(gwalk(lo, n, f, g))):
  match n:
    case Nil{}:
      Unit{}
    case Con{E{id, want, k, pred, age, mi, pa}, t}:
      (sh, st) = s
      %wl_cons(E{id, want, add(k, gg(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f)), pred, age, mi, pa}, gwalk(lo, t, gr(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f), gnext(gact(lo, g, E{id, want, k, pred, age, mi, pa}, f), pa, g))) : Sorted(_)
      (gwalk_allge(lo, pred, t, gr(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f), gnext(gact(lo, g, E{id, want, k, pred, age, mi, pa}, f), pa, g), sh), gwalk_sorted(lo, t, gr(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f), gnext(gact(lo, g, E{id, want, k, pred, age, mi, pa}, f), pa, g), st))

def split_allge.if(+p: Nat, +y: E, +rest: Sp, +aged: Bool, +py: IsTrue(le(p, epred(y))), +ih: AllGe(p, ss(rest)))
  -> AllGe(p, ss(split.if(y, rest, aged))):
  match rest:
    case Sp{m, s}:
      match aged:
        case True{}:
          ih
        case False{}:
          (py, ih)

def split_allge(+p: Nat, +n: +List<E>, +b: Nat, +a: AllGe(p, n)) -> AllGe(p, ss(split(n, b))):
  match n:
    case Nil{}:
      Unit{}
    case Con{E{id, want, k, pred, age, mi, pa}, t}:
      (h, at) = a
      split_allge.if(p, E{id, want, k, pred, 1n+age, mi, pa}, split(t, b), le(b, 1n+age), h, split_allge(p, t, b, at))

def split_sorted.if(+y: E, +t: +List<E>, +b: Nat, +rest: Sp, +aged: Bool, +ya: AllGe(epred(y), ss(rest)), +ih: Sorted(ss(rest)))
  -> Sorted(ss(split.if(y, rest, aged))):
  match rest:
    case Sp{m, s}:
      match aged:
        case True{}:
          ih
        case False{}:
          (ya, ih)

def split_sorted(+n: +List<E>, +b: Nat, +s: Sorted(n)) -> Sorted(ss(split(n, b))):
  match n:
    case Nil{}:
      Unit{}
    case Con{E{id, want, k, pred, age, mi, pa}, t}:
      (sh, st) = s
      split_sorted.if(E{id, want, k, pred, 1n+age, mi, pa}, t, b, split(t, b), le(b, 1n+age), split_allge(pred, t, b, sh), split_sorted(t, b, st))

def fin_allge.if(+p: Nat, +id: Nat, +want: Nat, +k: Nat, +pred: Nat, +age: Nat, +mi: Nat, +pa: Nat, +t: +List<E>, +rest: W, +hit: Bool,
  +ph: IsTrue(le(p, pred)), +pt: AllGe(p, t), +ih: AllGe(p, wl(rest)))
  -> AllGe(p, wl(fin.if(id, want, k, pred, age, mi, pa, t, rest, hit))):
  match hit:
    case True{}:
      pt
    case False{}:
      %wl_cons(E{id, want, k, pred, age, mi, pa}, rest) : AllGe(p, _)
      (ph, ih)

def fin_allge(+p: Nat, +n: +List<E>, +x: Nat, +a: AllGe(p, n)) -> AllGe(p, wl(fin(n, x))):
  match n:
    case Nil{}:
      Unit{}
    case Con{E{id, want, k, pred, age, mi, pa}, t}:
      (h, at) = a
      fin_allge.if(p, id, want, k, pred, age, mi, pa, t, fin(t, x), same(x, id), h, at, fin_allge(p, t, x, at))

def fin_sorted.if(+id: Nat, +want: Nat, +k: Nat, +pred: Nat, +age: Nat, +mi: Nat, +pa: Nat, +t: +List<E>, +rest: W, +hit: Bool,
  +sh: AllGe(pred, wl(rest)), +st: Sorted(t), +ih: Sorted(wl(rest)))
  -> Sorted(wl(fin.if(id, want, k, pred, age, mi, pa, t, rest, hit))):
  match hit:
    case True{}:
      st
    case False{}:
      %wl_cons(E{id, want, k, pred, age, mi, pa}, rest) : Sorted(_)
      (sh, ih)

def fin_sorted(+n: +List<E>, +x: Nat, +s: Sorted(n)) -> Sorted(wl(fin(n, x))):
  match n:
    case Nil{}:
      Unit{}
    case Con{E{id, want, k, pred, age, mi, pa}, t}:
      (sh, st) = s
      fin_sorted.if(id, want, k, pred, age, mi, pa, t, fin(t, x), same(x, id), fin_allge(pred, t, x, sh), st, fin_sorted(t, x, st))

def SortedL(l: Lane) -> Data:
  Sorted(lnew(l))

def Ordered(s: St) -> Data:
  AllL(l => SortedL(l), slanes(s))

def sorted_lane.arrive(+y: E, +l: Lane, +s: SortedL(l)) -> SortedL(arrive.lane(Spt{}, y, l)):
  match l:
    case Lane{o, n}:
      sorted_insert(n, y, s)

def sorted_lane.walk.if(+sk: Bool, +lo: Bool, +o: +List<E>, +n: +List<E>, +f: Nat, +g: +List<Nat>, +s: Sorted(n)) -> SortedL(ll(glwalk.if(sk, lo, o, n, f, g))):
  match sk:
    case True{}:
      s
    case False{}:
      gwalk_sorted(lo, n, wf(walk(o, f)), g, s)

def sorted_lane.walk(+lo: Bool, +l: Lane, +f: Nat, +g: +List<Nat>, +s: SortedL(l)) -> SortedL(ll(glwalk(lo, l, f, g))):
  match l:
    case Lane{o, n}:
      sorted_lane.walk.if(skip(lo, g), lo, o, n, f, g, s)

def sorted_lane.tick(+b: Nat, +l: Lane, +s: SortedL(l)) -> SortedL(tick.lane(b, l)):
  match l:
    case Lane{o, n}:
      split_sorted(n, b, s)

def sorted_lane.fin(+l: Lane, +x: Nat, +s: SortedL(l)) -> SortedL(ll(lfin(l, x))):
  match l:
    case Lane{o, n}:
      fin_sorted(n, x, s)

law spt_keeps_new_sorted:
  for +o: Op
  for +s: St
  for +i: Ordered(s)
  Ordered(step(Spt{}, o, s))

def sorted_arrive(+i: Nat, +y: E, +ls: +List<Lane>, +h: AllL(l => SortedL(l), ls)) -> AllL(l => SortedL(l), arrive.at(Spt{}, i, y, ls)):
  match i ls:
    case 0n Con{+l, +t}:
      (sl, st) = h
      (sorted_lane.arrive(y, l, sl), st)
    case 1n+j Con{+l, +t}:
      (sl, st) = h
      (sl, sorted_arrive(j, y, t, st))
    case i0 Nil{}:
      Unit{}

def sorted_walks(+ls: +List<Lane>, +f: Nat, +g: +List<Nat>, +h: AllL(l => SortedL(l), ls)) -> AllL(l => SortedL(l), lsl(gwalks(ls, f, g))):
  match ls:
    case Nil{}:
      Unit{}
    case Con{+l, +t}:
      (sl, st) = h
      (sorted_lane.walk(islow(t), l, f, g, sl), sorted_walks(t, lf(glwalk(islow(t), l, f, g)), glg(islow(t), l, f, g), st))

def sorted_ticks(+b: Nat, +ls: +List<Lane>, +h: AllL(l => SortedL(l), ls)) -> AllL(l => SortedL(l), ticks(b, ls)):
  match ls:
    case Nil{}:
      Unit{}
    case Con{+l, +t}:
      (sl, st) = h
      (sorted_lane.tick(b, l, sl), sorted_ticks(b, t, st))

def sorted_fins(+ls: +List<Lane>, +x: Nat, +h: AllL(l => SortedL(l), ls)) -> AllL(l => SortedL(l), lsl(fins(ls, x))):
  match ls:
    case Nil{}:
      Unit{}
    case Con{+l, +t}:
      (sl, st) = h
      (sorted_lane.fin(l, x, sl), sorted_fins(t, x, st))

def spt_keeps_new_sorted(o, s, i):
  match o s:
    case Arrive{band, id, want, pred, elapsed, mi, pa} St{f, b, ls}:
      sorted_arrive(lane(band, elapsed), E{id, 1n+want, 0n, pred, 0n, mi, pa}, ls, i)
    case Decide{} St{f, b, ls}:
      sorted_walks(ls, f, [], i)
    case Tick{} St{f, b, ls}:
      sorted_ticks(b, ls, i)
    case Finish{x} St{f, b, ls}:
      sorted_fins(ls, x, i)

# --- `old` by arrival ticket -------------------------------------------------------

def AllGeI(+p: Nat, xs: +List<E>) -> Data:
  match xs:
    case Nil{}:
      Unit
    case Con{x, xt}:
      Sigma<&2, &2, IsTrue(le(p, eid(x))), u => AllGeI(p, xt)>

def SortedI(xs: +List<E>) -> Data:
  match xs:
    case Nil{}:
      Unit
    case Con{x, xt}:
      Sigma<&2, &2, AllGeI(eid(x), xt), u => SortedI(xt)>

def allgei_weaken(+q: Nat, +p: Nat, +xs: +List<E>, +qp: IsTrue(le(q, p)), +a: AllGeI(p, xs)) -> AllGeI(q, xs):
  match xs:
    case Nil{}:
      Unit{}
    case Con{x, xt}:
      (h, t) = a
      (le_trans(q, p, eid(x), qp, h), allgei_weaken(q, p, xt, qp, t))

def allgei_insid.if(+p: Nat, +y: E, +h: E, +t: +List<E>, +rest: +List<E>, +before: Bool,
  +py: IsTrue(le(p, eid(y))), +ph: IsTrue(le(p, eid(h))), +pt: AllGeI(p, t), +ih: AllGeI(p, rest))
  -> AllGeI(p, insid.if(y, h, t, rest, before)):
  match before:
    case True{}:
      (py, (ph, pt))
    case False{}:
      (ph, ih)

def allgei_insid(+p: Nat, +xs: +List<E>, +y: E, +a: AllGeI(p, xs), +py: IsTrue(le(p, eid(y)))) -> AllGeI(p, insid(xs, y)):
  match xs:
    case Nil{}:
      (py, Unit{})
    case Con{h, t}:
      (ph, pt) = a
      allgei_insid.if(p, y, h, t, insid(t, y), Bool.not(le(eid(h), eid(y))), py, ph, pt, allgei_insid(p, t, y, pt, py))

def sorted_insid.if(+y: E, +h: E, +t: +List<E>, +before: Bool,
  +eb: {before == Bool.not(le(eid(h), eid(y))) : Bool},
  +sh: AllGeI(eid(h), t), +st: SortedI(t), +ih: SortedI(insid(t, y)))
  -> SortedI(insid.if(y, h, t, insid(t, y), before)):
  match before:
    case True{}:
      ((le_total(eid(h), eid(y), true_is(Bool.not(le(eid(h), eid(y))), eb)),
        allgei_weaken(eid(y), eid(h), t, le_total(eid(h), eid(y), true_is(Bool.not(le(eid(h), eid(y))), eb)), sh)),
       (sh, st))
    case False{}:
      (allgei_insid(eid(h), t, y, sh, not_false(le(eid(h), eid(y)), eb)), ih)

def sorted_insid(+xs: +List<E>, +y: E, +s: SortedI(xs)) -> SortedI(insid(xs, y)):
  match xs:
    case Nil{}:
      (Unit{}, Unit{})
    case Con{h, t}:
      (sh, st) = s
      sorted_insid.if(y, h, t, Bool.not(le(eid(h), eid(y))), {==}, sh, st, sorted_insid(t, y, st))

def walk_allgei(+p: Nat, +n: +List<E>, +f: Nat, +a: AllGeI(p, n)) -> AllGeI(p, wl(walk(n, f))):
  match n:
    case Nil{}:
      Unit{}
    case Con{E{id, want, k, pred, age, mi, pa}, t}:
      (h, at) = a
      %wl_cons(E{id, want, add(k, gg(need(E{id, want, k, pred, age, mi, pa}), f)), pred, age, mi, pa}, walk(t, gr(need(E{id, want, k, pred, age, mi, pa}), f))) : AllGeI(p, _)
      (h, walk_allgei(p, t, gr(need(E{id, want, k, pred, age, mi, pa}), f), at))

def walk_sortedI(+n: +List<E>, +f: Nat, +s: SortedI(n)) -> SortedI(wl(walk(n, f))):
  match n:
    case Nil{}:
      Unit{}
    case Con{E{id, want, k, pred, age, mi, pa}, t}:
      (sh, st) = s
      %wl_cons(E{id, want, add(k, gg(need(E{id, want, k, pred, age, mi, pa}), f)), pred, age, mi, pa}, walk(t, gr(need(E{id, want, k, pred, age, mi, pa}), f))) : SortedI(_)
      (walk_allgei(id, t, gr(need(E{id, want, k, pred, age, mi, pa}), f), sh), walk_sortedI(t, gr(need(E{id, want, k, pred, age, mi, pa}), f), st))

def fin_allgei.if(+p: Nat, +id: Nat, +want: Nat, +k: Nat, +pred: Nat, +age: Nat, +mi: Nat, +pa: Nat, +t: +List<E>, +rest: W, +hit: Bool,
  +ph: IsTrue(le(p, id)), +pt: AllGeI(p, t), +ih: AllGeI(p, wl(rest)))
  -> AllGeI(p, wl(fin.if(id, want, k, pred, age, mi, pa, t, rest, hit))):
  match hit:
    case True{}:
      pt
    case False{}:
      %wl_cons(E{id, want, k, pred, age, mi, pa}, rest) : AllGeI(p, _)
      (ph, ih)

def fin_allgei(+p: Nat, +n: +List<E>, +x: Nat, +a: AllGeI(p, n)) -> AllGeI(p, wl(fin(n, x))):
  match n:
    case Nil{}:
      Unit{}
    case Con{E{id, want, k, pred, age, mi, pa}, t}:
      (h, at) = a
      fin_allgei.if(p, id, want, k, pred, age, mi, pa, t, fin(t, x), same(x, id), h, at, fin_allgei(p, t, x, at))

def fin_sortedI.if(+id: Nat, +want: Nat, +k: Nat, +pred: Nat, +age: Nat, +mi: Nat, +pa: Nat, +t: +List<E>, +rest: W, +hit: Bool,
  +sh: AllGeI(id, wl(rest)), +st: SortedI(t), +ih: SortedI(wl(rest)))
  -> SortedI(wl(fin.if(id, want, k, pred, age, mi, pa, t, rest, hit))):
  match hit:
    case True{}:
      st
    case False{}:
      %wl_cons(E{id, want, k, pred, age, mi, pa}, rest) : SortedI(_)
      (sh, ih)

def fin_sortedI(+n: +List<E>, +x: Nat, +s: SortedI(n)) -> SortedI(wl(fin(n, x))):
  match n:
    case Nil{}:
      Unit{}
    case Con{E{id, want, k, pred, age, mi, pa}, t}:
      (sh, st) = s
      fin_sortedI.if(id, want, k, pred, age, mi, pa, t, fin(t, x), same(x, id), fin_allgei(id, t, x, sh), st, fin_sortedI(t, x, st))


def sorted_insall(+m: +List<E>, +o: +List<E>, +s: SortedI(o)) -> SortedI(insall(m, o)):
  match m:
    case Nil{}:
      s
    case Con{y, t}:
      sorted_insall(t, insid(o, y), sorted_insid(o, y, s))

def OldOrderedL(l: Lane) -> Data:
  SortedI(lold(l))

def OldOrdered(s: St) -> Data:
  AllL(l => OldOrderedL(l), slanes(s))

def old_lane.walk.if(+sk: Bool, +lo: Bool, +o: +List<E>, +n: +List<E>, +f: Nat, +g: +List<Nat>, +s: SortedI(o)) -> OldOrderedL(ll(glwalk.if(sk, lo, o, n, f, g))):
  match sk:
    case True{}:
      s
    case False{}:
      walk_sortedI(o, f, s)

def old_lane.walk(+lo: Bool, +l: Lane, +f: Nat, +g: +List<Nat>, +s: OldOrderedL(l)) -> OldOrderedL(ll(glwalk(lo, l, f, g))):
  match l:
    case Lane{o, n}:
      old_lane.walk.if(skip(lo, g), lo, o, n, f, g, s)

def old_lane.tick(+b: Nat, +l: Lane, +s: OldOrderedL(l)) -> OldOrderedL(tick.lane(b, l)):
  match l:
    case Lane{o, n}:
      sorted_insall(sm(split(n, b)), o, s)

def old_lane.fin(+l: Lane, +x: Nat, +s: OldOrderedL(l)) -> OldOrderedL(ll(lfin(l, x))):
  match l:
    case Lane{o, n}:
      fin_sortedI(o, x, s)

def old_lane.arrive(+p: Pol, +y: E, +l: Lane, +s: OldOrderedL(l)) -> OldOrderedL(arrive.lane(p, y, l)):
  match l:
    case Lane{o, n}:
      s

# LAW (aged requests go oldest first): with ids as arrival tickets, every lane's `old` stays
# in ticket order under every step, so the requests that aged on one tick keep their order.
law old_stays_in_arrival_order:
  for +p: Pol
  for +o: Op
  for +s: St
  for +i: OldOrdered(s)
  OldOrdered(step(p, o, s))

def old_arrive(+p: Pol, +i: Nat, +y: E, +ls: +List<Lane>, +h: AllL(l => OldOrderedL(l), ls)) -> AllL(l => OldOrderedL(l), arrive.at(p, i, y, ls)):
  match i ls:
    case 0n Con{+l, +t}:
      (sl, st) = h
      (old_lane.arrive(p, y, l, sl), st)
    case 1n+j Con{+l, +t}:
      (sl, st) = h
      (sl, old_arrive(p, j, y, t, st))
    case i0 Nil{}:
      Unit{}

def old_walks(+ls: +List<Lane>, +f: Nat, +g: +List<Nat>, +h: AllL(l => OldOrderedL(l), ls)) -> AllL(l => OldOrderedL(l), lsl(gwalks(ls, f, g))):
  match ls:
    case Nil{}:
      Unit{}
    case Con{+l, +t}:
      (sl, st) = h
      (old_lane.walk(islow(t), l, f, g, sl), old_walks(t, lf(glwalk(islow(t), l, f, g)), glg(islow(t), l, f, g), st))

def old_ticks(+b: Nat, +ls: +List<Lane>, +h: AllL(l => OldOrderedL(l), ls)) -> AllL(l => OldOrderedL(l), ticks(b, ls)):
  match ls:
    case Nil{}:
      Unit{}
    case Con{+l, +t}:
      (sl, st) = h
      (old_lane.tick(b, l, sl), old_ticks(b, t, st))

def old_fins(+ls: +List<Lane>, +x: Nat, +h: AllL(l => OldOrderedL(l), ls)) -> AllL(l => OldOrderedL(l), lsl(fins(ls, x))):
  match ls:
    case Nil{}:
      Unit{}
    case Con{+l, +t}:
      (sl, st) = h
      (old_lane.fin(l, x, sl), old_fins(t, x, st))

def old_stays_in_arrival_order(p, o, s, i):
  match o s:
    case Arrive{band, id, want, pred, elapsed, mi, pa} St{f, b, ls}:
      old_arrive(p, lane(band, elapsed), E{id, 1n+want, 0n, pred, 0n, mi, pa}, ls, i)
    case Decide{} St{f, b, ls}:
      old_walks(ls, f, [], i)
    case Tick{} St{f, b, ls}:
      old_ticks(b, ls, i)
    case Finish{x} St{f, b, ls}:
      old_fins(ls, x, i)

# --- the difference, computed --------------------------------------------------------------
# One permit free; a request predicted at 9 asked first, one predicted at 1 after it. Today
# grants the first; Spt grants the shorter. With a bound of 2, two ticks age the longer one
# into `old`, and Spt then grants it ahead of a newer, shorter request.
law spt_serves_shortest_then_the_aged:
  Sigma<&2, &2,
    {nth(2n, slanes(run(Today{}, [Arrive{Background{}, 1n, 0n, 9n, 0n, 0n, 0n}, Arrive{Background{}, 2n, 0n, 1n, 0n, 0n, 0n}, Decide{}], empty(1n, 2n)))) == Lane{[], [E{1n, 1n, 1n, 9n, 0n, 0n, 0n}, E{2n, 1n, 0n, 1n, 0n, 0n, 0n}]} : Lane},
    u => Sigma<&2, &2,
    {nth(2n, slanes(run(Spt{}, [Arrive{Background{}, 1n, 0n, 9n, 0n, 0n, 0n}, Arrive{Background{}, 2n, 0n, 1n, 0n, 0n, 0n}, Decide{}], empty(1n, 2n)))) == Lane{[], [E{2n, 1n, 1n, 1n, 0n, 0n, 0n}, E{1n, 1n, 0n, 9n, 0n, 0n, 0n}]} : Lane},
    v =>
    {nth(2n, slanes(run(Spt{}, [Arrive{Background{}, 1n, 0n, 9n, 0n, 0n, 0n}, Tick{}, Tick{}, Arrive{Background{}, 2n, 0n, 1n, 0n, 0n, 0n}, Decide{}], empty(1n, 2n)))) == Lane{[E{1n, 1n, 1n, 9n, 2n, 0n, 0n}], [E{2n, 1n, 0n, 1n, 0n, 0n, 0n}]} : Lane}>>

def spt_serves_shortest_then_the_aged():
  ({==}, ({==}, {==}))

# Two requests aging on one tick enter `old` by ticket, whatever their predictions.
law same_tick_ages_by_ticket:
  {lold(nth(2n, slanes(run(Spt{}, [Arrive{Background{}, 1n, 0n, 9n, 0n, 0n, 0n}, Arrive{Background{}, 2n, 0n, 1n, 0n, 0n, 0n}, Tick{}], empty(0n, 1n))))) == [E{1n, 1n, 0n, 9n, 1n, 0n, 0n}, E{2n, 1n, 0n, 1n, 1n, 0n, 0n}] : +List<E>}

def same_tick_ages_by_ticket():
  {==}


# --- LAW (interactive decays and never rises) ---------------------------------------------------

def le_trans_b(+a: Nat, +b: Nat, +c: Nat, ab: IsTrue(le(a, b)), bc: IsTrue(le(b, c))) -> IsTrue(le(a, c)):
  match a b c:
    case 0n b0 c0:
      Unit{}
    case 1n+a1 0n c0:
      match ab:
    case 1n+a1 1n+b1 0n:
      match bc:
    case 1n+a1 1n+b1 1n+c1:
      le_trans_b(a1, b1, c1, ab, bc)

# The band an ask takes is monotone in its job's elapsed time: from late facts (`x` implies `y`
# for each bound) the later ask's band is no higher than the earlier one's.
def absurd_nat(-A: Data, e: Empty) -> A:
  match e:

def iband_mono(+x1: Bool, +x2: Bool, +y1: Bool, +y2: Bool, i1: IsTrue(x1) -> IsTrue(y1), i2: IsTrue(x2) -> IsTrue(y2))
  -> IsTrue(le(iband.go(x1, x2), iband.go(y1, y2))):
  match x1 x2 y1 y2:
    case False{} x20 y10 y20:
      Unit{}
    case True{} False{} False{} y20:
      absurd_nat(IsTrue(le(1n, iband.go(False{}, y20))), i1(Unit{}))
    case True{} False{} True{} False{}:
      Unit{}
    case True{} False{} True{} True{}:
      Unit{}
    case True{} True{} False{} y20:
      absurd_nat(IsTrue(le(2n, iband.go(False{}, y20))), i1(Unit{}))
    case True{} True{} True{} False{}:
      absurd_nat(IsTrue(le(2n, 1n)), i2(Unit{}))
    case True{} True{} True{} True{}:
      Unit{}

# LAW (decay never rises): an interactive job's later ask is never in a higher band than an
# earlier one: interactive, then the gate's band at 120 s, then background at 240 s.
law decay_never_rises:
  for +e1: Nat
  for +e2: Nat
  for +ee: IsTrue(le(e1, e2))
  IsTrue(le(iband(e1), iband(e2)))

def decay_never_rises(e1, e2, ee):
  iband_mono(le(b1(), e1), le(b2(), e1), le(b1(), e2), le(b2(), e2),
    h => le_trans_b(b1(), e1, e2, h, ee),
    h => le_trans_b(b2(), e1, e2, h, ee))

# An interactive ask before 120 s is ordered before every gate-band request; one that has
# decayed past 120 s is ordered among them; past 240 s it is background work.
law interactive_decays_through_the_bands:
  Sigma<&2, &2, {iband(0n) == 0n : Nat}, u => Sigma<&2, &2, {iband(120n) == 1n : Nat}, v => {iband(240n) == 2n : Nat}>>

def interactive_decays_through_the_bands():
  ({==}, ({==}, {==}))

# A gate leaf predicted 1 s and a fresh interactive leaf predicted 5 s both wait, one permit
# free: the interactive leaf goes first. Asked at 130 s, it waits in the gate's lane behind the
# shorter gate leaf; at 250 s it is background work.
law interactive_before_the_gate_until_it_decays:
  Sigma<&2, &2,
    {slanes(run(Spt{}, [Arrive{Gate{}, 1n, 0n, 1n, 0n, 0n, 0n}, Arrive{Interactive{}, 2n, 0n, 5n, 0n, 0n, 0n}, Decide{}], empty(1n, 30n))) == [Lane{[], [E{2n, 1n, 1n, 5n, 0n, 0n, 0n}]}, Lane{[], [E{1n, 1n, 0n, 1n, 0n, 0n, 0n}]}, Lane{[], []}] : +List<Lane>},
    u => Sigma<&2, &2,
    {slanes(run(Spt{}, [Arrive{Gate{}, 1n, 0n, 1n, 0n, 0n, 0n}, Arrive{Interactive{}, 2n, 0n, 5n, 130n, 0n, 0n}, Decide{}], empty(1n, 30n))) == [Lane{[], []}, Lane{[], [E{1n, 1n, 1n, 1n, 0n, 0n, 0n}, E{2n, 1n, 0n, 5n, 0n, 0n, 0n}]}, Lane{[], []}] : +List<Lane>},
    v => {slanes(run(Spt{}, [Arrive{Interactive{}, 3n, 0n, 5n, 250n, 0n, 0n}], empty(1n, 30n))) == [Lane{[], []}, Lane{[], []}, Lane{[], [E{3n, 1n, 0n, 5n, 0n, 0n, 0n}]}] : +List<Lane>}>>

def interactive_before_the_gate_until_it_decays():
  ({==}, ({==}, {==}))

# Aging stays within a band: a gate request that has aged into `old` still waits behind a
# fresh interactive one (the case a shared top lane got wrong).
law aged_gate_stays_behind_fresh_interactive:
  {slanes(run(Spt{}, [Arrive{Gate{}, 1n, 0n, 9n, 0n, 0n, 0n}, Tick{}, Tick{}, Arrive{Interactive{}, 2n, 0n, 1n, 0n, 0n, 0n}, Decide{}], empty(1n, 2n))) == [Lane{[], [E{2n, 1n, 1n, 1n, 0n, 0n, 0n}]}, Lane{[E{1n, 1n, 0n, 9n, 2n, 0n, 0n}], []}, Lane{[], []}] : +List<Lane>}

def aged_gate_stays_behind_fresh_interactive():
  {==}

# === the minimum (mbx e95a64c) ======================================================

# --- every request in `new`: a fact of its grant, want, minimum and party ----------------
# A tick, an end and an arrival move requests and keep those four fields; only a decision
# changes a grant, so each fact below is proven once for those steps and once for the walk.

def RE(R: Nat -> Nat -> Nat -> Nat -> Data, e: E) -> Data:
  match e:
    case E{id, want, k, pred, age, mi, pa}:
      R(k, want, mi, pa)

def AllE(R: Nat -> Nat -> Nat -> Nat -> Data, xs: +List<E>) -> Data:
  match xs:
    case Nil{}:
      Unit
    case Con{x, xt}:
      Sigma<&2, &2, RE(R, x), u => AllE(R, xt)>

def AllN(R: Nat -> Nat -> Nat -> Nat -> Data, ls: +List<Lane>) -> Data:
  AllL(l => AllE(R, lnew(l)), ls)

def alle_snoc(-R: Nat -> Nat -> Nat -> Nat -> Data, +xs: +List<E>, +y: E, +a: AllE(R, xs), +ry: RE(R, y)) -> AllE(R, snoc(xs, y)):
  match xs:
    case Nil{}:
      (ry, Unit{})
    case Con{h, t}:
      (ah, at) = a
      (ah, alle_snoc(R, t, y, at, ry))

def alle_insert.if(-R: Nat -> Nat -> Nat -> Nat -> Data, +y: E, +h: E, +t: +List<E>, +rest: +List<E>, +before: Bool,
  +ry: RE(R, y), +rh: RE(R, h), +at: AllE(R, t), +ih: AllE(R, rest))
  -> AllE(R, insert.if(y, h, t, rest, before)):
  match before:
    case True{}:
      (ry, (rh, at))
    case False{}:
      (rh, ih)

def alle_insert(-R: Nat -> Nat -> Nat -> Nat -> Data, +xs: +List<E>, +y: E, +a: AllE(R, xs), +ry: RE(R, y)) -> AllE(R, insert(xs, y)):
  match xs:
    case Nil{}:
      (ry, Unit{})
    case Con{h, t}:
      (ah, at) = a
      alle_insert.if(R, y, h, t, insert(t, y), Bool.not(le(epred(h), epred(y))), ry, ah, at, alle_insert(R, t, y, at, ry))

def alle_lane_arrive(-R: Nat -> Nat -> Nat -> Nat -> Data, +p: Pol, +y: E, +l: Lane, +a: AllE(R, lnew(l)), +ry: RE(R, y)) -> AllE(R, lnew(arrive.lane(p, y, l))):
  match p l:
    case Today{} Lane{o, n}:
      alle_snoc(R, n, y, a, ry)
    case Spt{} Lane{o, n}:
      alle_insert(R, n, y, a, ry)

def alle_arrive(-R: Nat -> Nat -> Nat -> Nat -> Data, +p: Pol, +i: Nat, +y: E, +ls: +List<Lane>, +ry: RE(R, y), +h: AllN(R, ls)) -> AllN(R, arrive.at(p, i, y, ls)):
  match i ls:
    case 0n Con{+l, +t}:
      (sl, st) = h
      (alle_lane_arrive(R, p, y, l, sl, ry), st)
    case 1n+j Con{+l, +t}:
      (sl, st) = h
      (sl, alle_arrive(R, p, j, y, t, ry, st))
    case i0 Nil{}:
      Unit{}

def alle_split.if(-R: Nat -> Nat -> Nat -> Nat -> Data, +y: E, +rest: Sp, +aged: Bool, +ry: RE(R, y), +ih: AllE(R, ss(rest))) -> AllE(R, ss(split.if(y, rest, aged))):
  match rest:
    case Sp{m, s}:
      match aged:
        case True{}:
          ih
        case False{}:
          (ry, ih)

def alle_split(-R: Nat -> Nat -> Nat -> Nat -> Data, +n: +List<E>, +b: Nat, +a: AllE(R, n)) -> AllE(R, ss(split(n, b))):
  match n:
    case Nil{}:
      Unit{}
    case Con{E{id, want, k, pred, age, mi, pa}, t}:
      (h, at) = a
      alle_split.if(R, E{id, want, k, pred, 1n+age, mi, pa}, split(t, b), le(b, 1n+age), h, alle_split(R, t, b, at))

def alle_tick_lane(-R: Nat -> Nat -> Nat -> Nat -> Data, +b: Nat, +l: Lane, +a: AllE(R, lnew(l))) -> AllE(R, lnew(tick.lane(b, l))):
  match l:
    case Lane{o, n}:
      alle_split(R, n, b, a)

def alle_ticks(-R: Nat -> Nat -> Nat -> Nat -> Data, +b: Nat, +ls: +List<Lane>, +h: AllN(R, ls)) -> AllN(R, ticks(b, ls)):
  match ls:
    case Nil{}:
      Unit{}
    case Con{+l, +t}:
      (sl, st) = h
      (alle_tick_lane(R, b, l, sl), alle_ticks(R, b, t, st))

def alle_fin.if(-R: Nat -> Nat -> Nat -> Nat -> Data, +id: Nat, +want: Nat, +k: Nat, +pred: Nat, +age: Nat, +mi: Nat, +pa: Nat, +t: +List<E>, +rest: W, +hit: Bool,
  +rh: R(k, want, mi, pa), +at: AllE(R, t), +ih: AllE(R, wl(rest)))
  -> AllE(R, wl(fin.if(id, want, k, pred, age, mi, pa, t, rest, hit))):
  match hit:
    case True{}:
      at
    case False{}:
      %wl_cons(E{id, want, k, pred, age, mi, pa}, rest) : AllE(R, _)
      (rh, ih)

def alle_fin(-R: Nat -> Nat -> Nat -> Nat -> Data, +n: +List<E>, +x: Nat, +a: AllE(R, n)) -> AllE(R, wl(fin(n, x))):
  match n:
    case Nil{}:
      Unit{}
    case Con{E{id, want, k, pred, age, mi, pa}, t}:
      (h, at) = a
      alle_fin.if(R, id, want, k, pred, age, mi, pa, t, fin(t, x), same(x, id), h, at, alle_fin(R, t, x, at))

def alle_lane_fin(-R: Nat -> Nat -> Nat -> Nat -> Data, +l: Lane, +x: Nat, +a: AllE(R, lnew(l))) -> AllE(R, lnew(ll(lfin(l, x)))):
  match l:
    case Lane{o, n}:
      alle_fin(R, n, x, a)

def alle_fins(-R: Nat -> Nat -> Nat -> Nat -> Data, +ls: +List<Lane>, +x: Nat, +h: AllN(R, ls)) -> AllN(R, lsl(fins(ls, x))):
  match ls:
    case Nil{}:
      Unit{}
    case Con{+l, +t}:
      (sl, st) = h
      (alle_lane_fin(R, l, x, sl), alle_fins(R, t, x, st))

# --- LAW (a minimum of 0 or 1 is none): without minimums, the policy is today's -----------

# A request names no minimum.
def RM() -> Nat -> Nat -> Nat -> Nat -> Data:
  k => w => m => q => IsTrue(le(m, 1n))

def NoMin(s: St) -> Data:
  AllN(RM(), slanes(s))

# An arrival names no minimum.
def ArrM(o: Op) -> Data:
  match o:
    case Arrive{band, id, want, pred, elapsed, mi, pa}:
      IsTrue(le(mi, 1n))
    case Decide{}:
      Unit
    case Tick{}:
      Unit
    case Finish{x}:
      Unit

def AllArr(os: +List<Op>) -> Data:
  match os:
    case Nil{}:
      Unit
    case Con{o, t}:
      Sigma<&2, &2, ArrM(o), u => AllArr(t)>

# With no gather open, a request without a minimum takes what it needs, as today.
def act_take(+lo: Bool, +k: Nat, +mi: Nat, +want: Nat, +pa: Nat, +f: Nat, h: IsTrue(le(mi, 1n))) -> {Take{} == act(lo, [], k, want, mi, pa, f) : Act}:
  match k:
    case 0n:
      match mi:
        case 0n:
          {==}
        case 1n+z:
          match z:
            case 0n:
              match want:
                case 0n:
                  {==}
                case 1n+w:
                  {==}
            case 1n+y:
              match h:
    case 1n+j:
      {==}

def gwalk_today(+lo: Bool, +lane: +List<E>, +f: Nat, +a: AllE(RM(), lane)) -> {walk(lane, f) == gwalk(lo, lane, f, []) : W}:
  match lane:
    case Nil{}:
      {==}
    case Con{E{id, want, k, pred, age, mi, pa}, t}:
      (h, at) = a
      %act_take(lo, k, mi, want, pa, f, h) : {W.cons(E{id, want, add(k, gg(need.k(want, k), f)), pred, age, mi, pa}, walk(t, gr(need.k(want, k), f))) == W.cons(E{id, want, add(k, gg(en(_, need.k(want, k)), f)), pred, age, mi, pa}, gwalk(lo, t, gr(en(_, need.k(want, k)), f), gnext(_, pa, []))) : W}
      Equal.cong(W, W, w => W.cons(E{id, want, add(k, gg(need.k(want, k), f)), pred, age, mi, pa}, w),
        walk(t, gr(need.k(want, k), f)), gwalk(lo, t, gr(need.k(want, k), f), []), gwalk_today(lo, t, gr(need.k(want, k), f), at))

def gend_today(+lo: Bool, +lane: +List<E>, +f: Nat, +a: AllE(RM(), lane)) -> {[] == gend(lo, lane, f, []) : +List<Nat>}:
  match lane:
    case Nil{}:
      {==}
    case Con{E{id, want, k, pred, age, mi, pa}, t}:
      (h, at) = a
      %act_take(lo, k, mi, want, pa, f, h) : {[] == gend(lo, t, gr(en(_, need.k(want, k)), f), gnext(_, pa, [])) : +List<Nat>}
      gend_today(lo, t, gr(need.k(want, k), f), at)

def glwalk_today(+lo: Bool, +l: Lane, +f: Nat, +a: AllE(RM(), lnew(l))) -> {lwalk(l, f) == glwalk(lo, l, f, []) : LW}:
  match l:
    case Lane{o, n}:
      %gwalk_today(lo, n, wf(walk(o, f)), a) : {LW{wf(walk(n, wf(walk(o, f)))), Lane{wl(walk(o, f)), wl(walk(n, wf(walk(o, f))))}} == LW{wf(_), Lane{wl(walk(o, f)), wl(_)}} : LW}
      {==}

def glg_today(+lo: Bool, +l: Lane, +f: Nat, +a: AllE(RM(), lnew(l))) -> {[] == glg(lo, l, f, []) : +List<Nat>}:
  match l:
    case Lane{o, n}:
      gend_today(lo, n, wf(walk(o, f)), a)

def gwalks_today(+ls: +List<Lane>, +f: Nat, +a: AllN(RM(), ls)) -> {walks(ls, f) == gwalks(ls, f, []) : LS}:
  match ls:
    case Nil{}:
      {==}
    case Con{+l, +t}:
      (al, at) = a
      %glg_today(islow(t), l, f, al) : {LS{lsf(walks(t, lf(lwalk(l, f)))), Con{ll(lwalk(l, f)), lsl(walks(t, lf(lwalk(l, f))))}} == LS{lsf(gwalks(t, lf(glwalk(islow(t), l, f, [])), _)), Con{ll(glwalk(islow(t), l, f, [])), lsl(gwalks(t, lf(glwalk(islow(t), l, f, [])), _))}} : LS}
      %glwalk_today(islow(t), l, f, al) : {LS{lsf(walks(t, lf(lwalk(l, f)))), Con{ll(lwalk(l, f)), lsl(walks(t, lf(lwalk(l, f))))}} == LS{lsf(gwalks(t, lf(_), [])), Con{ll(_), lsl(gwalks(t, lf(_), []))}} : LS}
      %gwalks_today(t, lf(lwalk(l, f)), at) : {LS{lsf(walks(t, lf(lwalk(l, f)))), Con{ll(lwalk(l, f)), lsl(walks(t, lf(lwalk(l, f))))}} == LS{lsf(_), Con{ll(lwalk(l, f)), lsl(_)}} : LS}
      {==}

def decide_today(+s: St, +a: NoMin(s)) -> {decide.today(s) == decide(s) : St}:
  match s:
    case St{f, b, ls}:
      %gwalks_today(ls, f, a) : {St{lsf(walks(ls, f)), b, lsl(walks(ls, f))} == St{lsf(_), b, lsl(_)} : St}
      {==}

def step_today(+p: Pol, +o: Op, +s: St, +a: NoMin(s)) -> {step.today(p, o, s) == step(p, o, s) : St}:
  match o:
    case Arrive{band, id, want, pred, elapsed, mi, pa}:
      {==}
    case Decide{}:
      decide_today(s, a)
    case Tick{}:
      {==}
    case Finish{x}:
      {==}

# A decision keeps every request's minimum.
def nomin_gwalk(+lo: Bool, +n: +List<E>, +f: Nat, +g: +List<Nat>, +a: AllE(RM(), n)) -> AllE(RM(), wl(gwalk(lo, n, f, g))):
  match n:
    case Nil{}:
      Unit{}
    case Con{E{id, want, k, pred, age, mi, pa}, t}:
      (h, at) = a
      %wl_cons(E{id, want, add(k, gg(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f)), pred, age, mi, pa}, gwalk(lo, t, gr(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f), gnext(gact(lo, g, E{id, want, k, pred, age, mi, pa}, f), pa, g))) : AllE(RM(), _)
      (h, nomin_gwalk(lo, t, gr(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f), gnext(gact(lo, g, E{id, want, k, pred, age, mi, pa}, f), pa, g), at))

def nomin_glwalk.if(+sk: Bool, +lo: Bool, +o: +List<E>, +n: +List<E>, +f: Nat, +g: +List<Nat>, +a: AllE(RM(), n)) -> AllE(RM(), lnew(ll(glwalk.if(sk, lo, o, n, f, g)))):
  match sk:
    case True{}:
      a
    case False{}:
      nomin_gwalk(lo, n, wf(walk(o, f)), g, a)

def nomin_glwalk(+lo: Bool, +l: Lane, +f: Nat, +g: +List<Nat>, +a: AllE(RM(), lnew(l))) -> AllE(RM(), lnew(ll(glwalk(lo, l, f, g)))):
  match l:
    case Lane{o, n}:
      nomin_glwalk.if(skip(lo, g), lo, o, n, f, g, a)

def nomin_gwalks(+ls: +List<Lane>, +f: Nat, +g: +List<Nat>, +h: AllN(RM(), ls)) -> AllN(RM(), lsl(gwalks(ls, f, g))):
  match ls:
    case Nil{}:
      Unit{}
    case Con{+l, +t}:
      (sl, st) = h
      (nomin_glwalk(islow(t), l, f, g, sl), nomin_gwalks(t, lf(glwalk(islow(t), l, f, g)), glg(islow(t), l, f, g), st))

def step_nomin(+p: Pol, +o: Op, +s: St, +ao: ArrM(o), +a: NoMin(s)) -> NoMin(step(p, o, s)):
  match o s:
    case Arrive{band, id, want, pred, elapsed, mi, pa} St{f, b, ls}:
      alle_arrive(RM(), p, lane(band, elapsed), E{id, 1n+want, 0n, pred, 0n, mi, pa}, ls, ao, a)
    case Decide{} St{f, b, ls}:
      nomin_gwalks(ls, f, [], a)
    case Tick{} St{f, b, ls}:
      alle_ticks(RM(), b, ls, a)
    case Finish{x} St{f, b, ls}:
      alle_fins(RM(), ls, x, a)

def run_today(+p: Pol, +os: +List<Op>, +s: St, +ao: AllArr(os), +a: NoMin(s)) -> {run.today(p, os, s) == run(p, os, s) : St}:
  match os:
    case Nil{}:
      {==}
    case Con{o, t}:
      (ho, at) = ao
      %Equal.sym(St, step.today(p, o, s), step(p, o, s), step_today(p, o, s, a)) : {run.today(p, t, _) == run(p, t, step(p, o, s)) : St}
      run_today(p, t, step(p, o, s), at, step_nomin(p, o, s, ho, a))

# LAW (a minimum of 0 or 1 is none): every trace whose arrivals name no minimum (min <= 1)
# decides as today's policy, step by step from an empty pool.
law min_le_one_is_today:
  for +p: Pol
  for +os: +List<Op>
  for +cap: Nat
  for +bound: Nat
  for +a: AllArr(os)
  {run(p, os, empty(cap, bound)) == run.today(p, os, empty(cap, bound)) : St}

def min_le_one_is_today(p, os, cap, bound, a):
  Equal.sym(St, run.today(p, os, empty(cap, bound)), run(p, os, empty(cap, bound)),
    run_today(p, os, empty(cap, bound), a, (Unit{}, (Unit{}, (Unit{}, Unit{})))))

# In any state whose waiting requests name no minimum, a step is today's step.
law step_without_min_is_today:
  for +p: Pol
  for +o: Op
  for +s: St
  for +a: NoMin(s)
  {step(p, o, s) == step.today(p, o, s) : St}

def step_without_min_is_today(p, o, s, a):
  Equal.sym(St, step.today(p, o, s), step(p, o, s), step_today(p, o, s, a))

# Today's order laws hold of every decision on a state without a minimum.
law lanes_in_order_without_min:
  for +p: Pol
  for +s: St
  for +a: NoMin(s)
  Before(slanes(s), slanes(step(p, Decide{}, s)))

def lanes_in_order_without_min(p, s, a):
  %step_today(p, Decide{}, s, a) : Before(slanes(s), slanes(_))
  lanes_in_order(p, s)

law work_conserving_without_min:
  for +p: Pol
  for +s: St
  for +a: NoMin(s)
  Either<&2, &2, AllL(l => AllFilledL(l), slanes(step(p, Decide{}, s))), IsZero(sfree(step(p, Decide{}, s)))>

def work_conserving_without_min(p, s, a):
  %step_today(p, Decide{}, s, a) : Either<&2, &2, AllL(l => AllFilledL(l), slanes(_)), IsZero(sfree(_))>
  work_conserving(p, s)

law first_in_order_filled_without_min:
  for +p: Pol
  for +s: St
  for +a: NoMin(s)
  All2(l => m => OrderL(l, m), slanes(s), slanes(step(p, Decide{}, s)))

def first_in_order_filled_without_min(p, s, a):
  %step_today(p, Decide{}, s, a) : All2(l => m => OrderL(l, m), slanes(s), slanes(_))
  first_in_order_filled(p, s)

# --- LAW (a gatherer starts with its minimum) -------------------------------------------
# Every request in `new` holds nothing, or at least least = min(m, want): before it ages, no
# request starts short of its minimum. (A minimum of 0 or 1 asks for no more than any start.)

def Least(+k: Nat, want: Nat, mi: Nat) -> Data:
  Either<&2, &2, IsZero(k), IsTrue(le(gg(mi, want), k))>

def RL() -> Nat -> Nat -> Nat -> Nat -> Data:
  k => w => m => q => Least(k, w, m)

def Started(s: St) -> Data:
  AllN(RL(), slanes(s))

def gg_le_r(+a: Nat, +b: Nat) -> IsTrue(le(gg(a, b), b)):
  match a b:
    case 0n b0:
      Unit{}
    case 1n+a1 0n:
      Unit{}
    case 1n+a1 1n+b1:
      gg_le_r(a1, b1)

def le_gg(+l: Nat, +a: Nat, +b: Nat, la: IsTrue(le(l, a)), lb: IsTrue(le(l, b))) -> IsTrue(le(l, gg(a, b))):
  match l a b:
    case 0n a0 b0:
      Unit{}
    case 1n+l1 0n b0:
      match la:
    case 1n+l1 1n+a1 0n:
      match lb:
    case 1n+l1 1n+a1 1n+b1:
      le_gg(l1, a1, b1, la, lb)

def short_one(+want: Nat, +f: Nat, lw: IsTrue(le(1n, want))) -> Either<&2, &2, IsZero(gg(want, f)), IsTrue(le(1n, gg(want, f)))>:
  match want f:
    case 0n f0:
      match lw:
    case 1n+w 0n:
      Inl{Unit{}}
    case 1n+w 1n+c:
      Inr{Unit{}}

# Not short of `least` (at most `want`): what is free is none, or covers it.
def short_false(+l: Nat, +want: Nat, +f: Nat, lw: IsTrue(le(l, want)), es: {False{} == short(l, f) : Bool})
  -> Either<&2, &2, IsZero(gg(want, f)), IsTrue(le(l, gg(want, f)))>:
  match l:
    case 0n:
      Inr{Unit{}}
    case 1n+a:
      match a:
        case 0n:
          short_one(want, f, lw)
        case 1n+b:
          Inr{le_gg(2n+b, want, f, lw, not_false(le(2n+b, f), es))}

def least_act0(+h: Bool, +s: Bool, +want: Nat, +mi: Nat, +f: Nat, es: {s == short(gg(mi, want), f) : Bool})
  -> Least(gg(en(act0(h, s), want), f), want, mi):
  match h s:
    case True{} s0:
      Inl{Unit{}}
    case False{} True{}:
      Inl{Unit{}}
    case False{} False{}:
      short_false(gg(mi, want), want, f, gg_le_r(mi, want), es)

# A request's turn keeps it at none or at least its minimum.
def least_head(+lo: Bool, +g: +List<Nat>, +k: Nat, +want: Nat, +mi: Nat, +pa: Nat, +f: Nat, h: Least(k, want, mi))
  -> Least(add(k, gg(en(act(lo, g, k, want, mi, pa, f), need.k(want, k)), f)), want, mi):
  match k:
    case 0n:
      least_act0(held(lo, pa, g), short(gg(mi, want), f), want, mi, f, {==})
    case 1n+j:
      match h:
        case Inl{z}:
          match z:
        case Inr{r}:
          %Equal.sym(Nat, add(j, 0n), j, add_zero_r(j)) : Least(1n+_, want, mi)
          Inr{r}

def least_gwalk(+lo: Bool, +n: +List<E>, +f: Nat, +g: +List<Nat>, +a: AllE(RL(), n)) -> AllE(RL(), wl(gwalk(lo, n, f, g))):
  match n:
    case Nil{}:
      Unit{}
    case Con{E{id, want, k, pred, age, mi, pa}, t}:
      (h, at) = a
      %wl_cons(E{id, want, add(k, gg(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f)), pred, age, mi, pa}, gwalk(lo, t, gr(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f), gnext(gact(lo, g, E{id, want, k, pred, age, mi, pa}, f), pa, g))) : AllE(RL(), _)
      (least_head(lo, g, k, want, mi, pa, f, h), least_gwalk(lo, t, gr(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f), gnext(gact(lo, g, E{id, want, k, pred, age, mi, pa}, f), pa, g), at))

def least_glwalk.if(+sk: Bool, +lo: Bool, +o: +List<E>, +n: +List<E>, +f: Nat, +g: +List<Nat>, +a: AllE(RL(), n)) -> AllE(RL(), lnew(ll(glwalk.if(sk, lo, o, n, f, g)))):
  match sk:
    case True{}:
      a
    case False{}:
      least_gwalk(lo, n, wf(walk(o, f)), g, a)

def least_glwalk(+lo: Bool, +l: Lane, +f: Nat, +g: +List<Nat>, +a: AllE(RL(), lnew(l))) -> AllE(RL(), lnew(ll(glwalk(lo, l, f, g)))):
  match l:
    case Lane{o, n}:
      least_glwalk.if(skip(lo, g), lo, o, n, f, g, a)

def least_gwalks(+ls: +List<Lane>, +f: Nat, +g: +List<Nat>, +h: AllN(RL(), ls)) -> AllN(RL(), lsl(gwalks(ls, f, g))):
  match ls:
    case Nil{}:
      Unit{}
    case Con{+l, +t}:
      (sl, st) = h
      (least_glwalk(islow(t), l, f, g, sl), least_gwalks(t, lf(glwalk(islow(t), l, f, g)), glg(islow(t), l, f, g), st))

# Every step keeps it: an arrival holds nothing, a tick and an end move requests unchanged,
# and a decision starts a request in `new` only on its minimum or more.
law minimum_kept_by_every_step:
  for +p: Pol
  for +o: Op
  for +s: St
  for +i: Started(s)
  Started(step(p, o, s))

def minimum_kept_by_every_step(p, o, s, i):
  match o s:
    case Arrive{band, id, want, pred, elapsed, mi, pa} St{f, b, ls}:
      alle_arrive(RL(), p, lane(band, elapsed), E{id, 1n+want, 0n, pred, 0n, mi, pa}, ls, Inl{Unit{}}, i)
    case Decide{} St{f, b, ls}:
      least_gwalks(ls, f, [], i)
    case Tick{} St{f, b, ls}:
      alle_ticks(RL(), b, ls, i)
    case Finish{x} St{f, b, ls}:
      alle_fins(RL(), ls, x, i)

def run_started(+p: Pol, +os: +List<Op>, +s: St, +i: Started(s)) -> Started(run(p, os, s)):
  match os:
    case Nil{}:
      i
    case Con{o, t}:
      run_started(p, t, step(p, o, s), minimum_kept_by_every_step(p, o, s, i))

# LAW: in every state a trace reaches from an empty pool, so after every decision, each
# request in any lane's `new` holds 0 permits or at least min(m, want).
law a_gatherer_starts_with_its_minimum:
  for +p: Pol
  for +os: +List<Op>
  for +cap: Nat
  for +bound: Nat
  Started(run(p, os, empty(cap, bound)))

def a_gatherer_starts_with_its_minimum(p, os, cap, bound):
  run_started(p, os, empty(cap, bound), (Unit{}, (Unit{}, (Unit{}, Unit{}))))

# --- LAW (a gather ends at the bound) -----------------------------------------------------
# A lane's aged requests are walked by today's `walk`, which reads no minimum: each takes
# min(want, free) of what is free when its lane's turn comes. A request stops gathering on the
# tick that moves it into `old` (`tick_bounds_wait`) and starts on what is free. The one
# exception is the low lane, which takes nothing while a gather above it is open.

def gather_ends_at_the_bound.if(+sk: Bool, +lo: Bool, +o: +List<E>, +n: +List<E>, +f: Nat, +g: +List<Nat>, e: {sk == skip(lo, g) : Bool})
  -> Either<&2, &2, {lold(ll(glwalk.if(sk, lo, o, n, f, g))) == wl(walk(o, f)) : +List<E>},
       Sigma<&2, &2, IsTrue(skip(lo, g)), u => {ll(glwalk.if(sk, lo, o, n, f, g)) == Lane{o, n} : Lane}>>:
  match sk:
    case True{}:
      Inr{(true_is(skip(lo, g), e), {==})}
    case False{}:
      Inl{{==}}

law gather_ends_at_the_bound:
  for +lo: Bool
  for +l: Lane
  for +f: Nat
  for +g: +List<Nat>
  Either<&2, &2, {lold(ll(glwalk(lo, l, f, g))) == wl(walk(lold(l), f)) : +List<E>},
    Sigma<&2, &2, IsTrue(skip(lo, g)), u => {ll(glwalk(lo, l, f, g)) == l : Lane}>>

def gather_ends_at_the_bound(lo, l, f, g):
  match l:
    case Lane{o, n}:
      gather_ends_at_the_bound.if(skip(lo, g), lo, o, n, f, g, {==})

# --- round 9, computed ----------------------------------------------------------------------
# A pool of 4: a gate job holds 3, so 1 is free. A reservation asks 18 with min 4 (party 1):
# it gathers and holds nothing, where today's policy starts it on 1. The job ends, freeing 3;
# with 4 free the reservation starts on 4.
law round9_gathers_then_starts_on_its_minimum:
  Sigma<&2, &2,
    {run(Spt{}, [Arrive{Gate{}, 1n, 2n, 5n, 0n, 0n, 0n}, Decide{}, Arrive{Gate{}, 2n, 17n, 9n, 0n, 4n, 1n}, Decide{}], empty(4n, 30n)) == St{1n, 30n, [Lane{[], []}, Lane{[], [E{1n, 3n, 3n, 5n, 0n, 0n, 0n}, E{2n, 18n, 0n, 9n, 0n, 4n, 1n}]}, Lane{[], []}]} : St},
    u => Sigma<&2, &2,
    {run.today(Spt{}, [Arrive{Gate{}, 1n, 2n, 5n, 0n, 0n, 0n}, Decide{}, Arrive{Gate{}, 2n, 17n, 9n, 0n, 4n, 1n}, Decide{}], empty(4n, 30n)) == St{0n, 30n, [Lane{[], []}, Lane{[], [E{1n, 3n, 3n, 5n, 0n, 0n, 0n}, E{2n, 18n, 1n, 9n, 0n, 4n, 1n}]}, Lane{[], []}]} : St},
    v =>
    {run(Spt{}, [Arrive{Gate{}, 1n, 2n, 5n, 0n, 0n, 0n}, Decide{}, Arrive{Gate{}, 2n, 17n, 9n, 0n, 4n, 1n}, Decide{}, Finish{1n}, Decide{}], empty(4n, 30n)) == St{0n, 30n, [Lane{[], []}, Lane{[], [E{2n, 18n, 4n, 9n, 0n, 4n, 1n}]}, Lane{[], []}]} : St}>>

def round9_gathers_then_starts_on_its_minimum():
  ({==}, ({==}, {==}))

# The same gather at the bound: with a bound of 2, two ticks age it into `old`, and the next
# decision starts it on the 1 that is free.
law round9_starts_short_at_the_bound:
  {run(Spt{}, [Arrive{Gate{}, 1n, 2n, 5n, 0n, 0n, 0n}, Decide{}, Arrive{Gate{}, 2n, 17n, 9n, 0n, 4n, 1n}, Decide{}, Tick{}, Tick{}, Decide{}], empty(4n, 2n)) == St{0n, 2n, [Lane{[], []}, Lane{[E{1n, 3n, 3n, 5n, 2n, 0n, 0n}, E{2n, 18n, 1n, 9n, 2n, 4n, 1n}], []}, Lane{[], []}]} : St}

def round9_starts_short_at_the_bound():
  {==}

# What a gather holds back: a pool of 6, a gate job holds 3. The reservation (min 4, party 1)
# gathers with 3 free; a later request of its party waits, and so does low work; a later gate
# request of another party passes and starts. Two permits stay free.
law a_gather_holds_back_its_party_and_low_work:
  {run(Spt{}, [Arrive{Gate{}, 1n, 2n, 5n, 0n, 0n, 0n}, Decide{}, Arrive{Gate{}, 2n, 17n, 9n, 0n, 4n, 1n}, Arrive{Gate{}, 3n, 0n, 20n, 0n, 0n, 1n}, Arrive{Gate{}, 4n, 0n, 20n, 0n, 0n, 2n}, Arrive{Background{}, 5n, 0n, 1n, 0n, 0n, 0n}, Decide{}], empty(6n, 30n)) == St{2n, 30n, [Lane{[], []}, Lane{[], [E{1n, 3n, 3n, 5n, 0n, 0n, 0n}, E{2n, 18n, 0n, 9n, 0n, 4n, 1n}, E{3n, 1n, 0n, 20n, 0n, 0n, 1n}, E{4n, 1n, 1n, 20n, 0n, 0n, 2n}]}, Lane{[], [E{5n, 1n, 0n, 1n, 0n, 0n, 0n}]}]} : St}

def a_gather_holds_back_its_party_and_low_work():
  {==}

# What the model leaves out (mbx also holds back a request of the gatherer's party that asked
# after it but is ordered before it): under Spt a shorter request of the gatherer's party that
# asked later sits ahead of it in `new` and starts on the free permit before the gather opens.
law spt_gap_a_shorter_request_of_the_party_passes:
  {run(Spt{}, [Arrive{Gate{}, 1n, 2n, 5n, 0n, 0n, 0n}, Decide{}, Arrive{Gate{}, 2n, 17n, 9n, 0n, 4n, 1n}, Arrive{Gate{}, 3n, 0n, 1n, 0n, 0n, 1n}, Decide{}], empty(4n, 30n)) == St{0n, 30n, [Lane{[], []}, Lane{[], [E{3n, 1n, 1n, 1n, 0n, 0n, 1n}, E{1n, 3n, 3n, 5n, 0n, 0n, 0n}, E{2n, 18n, 0n, 9n, 0n, 4n, 1n}]}, Lane{[], []}]} : St}

def spt_gap_a_shorter_request_of_the_party_passes():
  {==}

# --- LAW (only a gather leaves permits free) ------------------------------------------------
# What replaces `work_conserving` once requests gather: after a decision either no permit is
# free, or every request still waiting is one a gather holds back. Re-examine the lanes in pass
# order with what is left free (`fe`): a lane's aged requests are all filled unless it is the low
# lane behind an open gather; and each request in `new`, given its turn again, takes nothing
# more (`Waits`): it has all it asked for, it is short of its minimum (a gatherer), or it is low
# work or of a gatherer's party behind one. The gathers it sees are those of the requests before
# it in that re-examination, so "behind a gather" is read off the state the decision leaves.

def Waits(a: Act, n: Nat) -> Data:
  match a:
    case Take{}:
      IsZero(n)
    case Hold{}:
      Unit
    case Open{}:
      Unit

def WaitsN(+fe: Nat, +lo: Bool, xs: +List<E>, +acc: +List<Nat>) -> Data:
  match xs:
    case Nil{}:
      Unit
    case Con{E{id, +want, +k, pred, age, +mi, +pa}, t}:
      Sigma<&2, &2, Waits(act(lo, acc, k, want, mi, pa, fe), need.k(want, k)), u => WaitsN(fe, lo, t, gnext(act(lo, acc, k, want, mi, pa, fe), pa, acc))>

# The gathers open after that re-examination of `xs`.
def gacc(+fe: Nat, +lo: Bool, xs: +List<E>, +acc: +List<Nat>) -> +List<Nat>:
  match xs:
    case Nil{}:
      acc
    case Con{E{id, +want, +k, pred, age, +mi, +pa}, t}:
      gacc(fe, lo, t, gnext(act(lo, acc, k, want, mi, pa, fe), pa, acc))

def WaitsLane(+fe: Nat, +lo: Bool, +acc: +List<Nat>, l: Lane) -> Data:
  match l:
    case Lane{o, n}:
      Sigma<&2, &2, Either<&2, &2, AllFilled(o), IsTrue(skip(lo, acc))>, u => WaitsN(fe, lo, n, acc)>

def gaccL(+fe: Nat, +lo: Bool, +acc: +List<Nat>, l: Lane) -> +List<Nat>:
  match l:
    case Lane{o, n}:
      gacc(fe, lo, n, acc)

def WaitsLs(+fe: Nat, ls: +List<Lane>, +acc: +List<Nat>) -> Data:
  match ls:
    case Nil{}:
      Unit
    case Con{+l, +t}:
      Sigma<&2, &2, WaitsLane(fe, islow(t), acc, l), u => WaitsLs(fe, t, gaccL(fe, islow(t), acc, l))>

def pfst(-A: Data, -B: Data, +p: Sigma<&2, &2, A, u => B>) -> A:
  (a, b) = p
  a

def psnd(-A: Data, -B: Data, +p: Sigma<&2, &2, A, u => B>) -> B:
  (a, b) = p
  b

# What is free only shrinks along a decision.
def le_succ(+b: Nat) -> IsTrue(le(b, 1n+b)):
  match b:
    case 0n:
      Unit{}
    case 1n+c:
      le_succ(c)

def gr_le(+a: Nat, +b: Nat) -> IsTrue(le(gr(a, b), b)):
  match a b:
    case 0n b0:
      le_refl(b0)
    case 1n+a1 0n:
      Unit{}
    case 1n+a1 1n+b1:
      le_trans(gr(a1, b1), b1, 1n+b1, gr_le(a1, b1), le_succ(b1))

def walk_le(+xs: +List<E>, +f: Nat) -> IsTrue(le(wf(walk(xs, f)), f)):
  match xs:
    case Nil{}:
      le_refl(f)
    case Con{E{id, want, k, pred, age, mi, pa}, t}:
      %wf_cons(E{id, want, add(k, gg(need(E{id, want, k, pred, age, mi, pa}), f)), pred, age, mi, pa}, walk(t, gr(need(E{id, want, k, pred, age, mi, pa}), f))) : IsTrue(le(_, f))
      le_trans(wf(walk(t, gr(need(E{id, want, k, pred, age, mi, pa}), f))), gr(need(E{id, want, k, pred, age, mi, pa}), f), f, walk_le(t, gr(need(E{id, want, k, pred, age, mi, pa}), f)), gr_le(need(E{id, want, k, pred, age, mi, pa}), f))

def gwalk_le(+lo: Bool, +xs: +List<E>, +f: Nat, +g: +List<Nat>) -> IsTrue(le(wf(gwalk(lo, xs, f, g)), f)):
  match xs:
    case Nil{}:
      le_refl(f)
    case Con{E{id, want, k, pred, age, mi, pa}, t}:
      %wf_cons(E{id, want, add(k, gg(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f)), pred, age, mi, pa}, gwalk(lo, t, gr(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f), gnext(gact(lo, g, E{id, want, k, pred, age, mi, pa}, f), pa, g))) : IsTrue(le(_, f))
      le_trans(wf(gwalk(lo, t, gr(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f), gnext(gact(lo, g, E{id, want, k, pred, age, mi, pa}, f), pa, g))), gr(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f), f, gwalk_le(lo, t, gr(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f), gnext(gact(lo, g, E{id, want, k, pred, age, mi, pa}, f), pa, g)), gr_le(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f))

def glwalk_le.if(+sk: Bool, +lo: Bool, +o: +List<E>, +n: +List<E>, +f: Nat, +g: +List<Nat>) -> IsTrue(le(lf(glwalk.if(sk, lo, o, n, f, g)), f)):
  match sk:
    case True{}:
      le_refl(f)
    case False{}:
      le_trans(wf(gwalk(lo, n, wf(walk(o, f)), g)), wf(walk(o, f)), f, gwalk_le(lo, n, wf(walk(o, f)), g), walk_le(o, f))

def glwalk_le(+lo: Bool, +l: Lane, +f: Nat, +g: +List<Nat>) -> IsTrue(le(lf(glwalk(lo, l, f, g)), f)):
  match l:
    case Lane{o, n}:
      glwalk_le.if(skip(lo, g), lo, o, n, f, g)

def gwalks_le(+ls: +List<Lane>, +f: Nat, +g: +List<Nat>) -> IsTrue(le(lsf(gwalks(ls, f, g)), f)):
  match ls:
    case Nil{}:
      le_refl(f)
    case Con{+l, +t}:
      le_trans(lsf(gwalks(t, lf(glwalk(islow(t), l, f, g)), glg(islow(t), l, f, g))), lf(glwalk(islow(t), l, f, g)), f, gwalks_le(t, lf(glwalk(islow(t), l, f, g)), glg(islow(t), l, f, g)), glwalk_le(islow(t), l, f, g))

def le_wf_cons(+fe: Nat, +h: E, +w: W, hf: IsTrue(le(fe, wf(W.cons(h, w))))) -> IsTrue(le(fe, wf(w))):
  match w:
    case W{f0, l0}:
      hf

def no_room(+fe: Nat, +x: Nat, nz: IsTrue(le(1n, fe)), hx: IsTrue(le(fe, x)), z: IsZero(x)) -> Empty:
  match fe x:
    case 0n x0:
      match nz:
    case 1n+q 0n:
      match hx:
    case 1n+q 1n+r:
      match z:

def is_true_eq(+b: Bool, h: IsTrue(b)) -> {True{} == b : Bool}:
  match b:
    case True{}:
      {==}
    case False{}:
      match h:

def not_le_mono(+l: Nat, +fe: Nat, +f: Nat, hle: IsTrue(le(fe, f)), hn: IsTrue(Bool.not(le(l, f)))) -> IsTrue(Bool.not(le(l, fe))):
  match l fe f:
    case 0n fe0 f0:
      match hn:
    case 1n+l1 0n f0:
      Unit{}
    case 1n+l1 1n+e1 0n:
      match hle:
    case 1n+l1 1n+e1 1n+f1:
      not_le_mono(l1, e1, f1, hle, hn)

# Short of `least` with `f` free is short of it with less.
def short_mono(+l: Nat, +fe: Nat, +f: Nat, hle: IsTrue(le(fe, f)), hs: IsTrue(short(l, f))) -> IsTrue(short(l, fe)):
  match l:
    case 0n:
      match hs:
    case 1n+a:
      match a:
        case 0n:
          match hs:
        case 1n+b:
          not_le_mono(2n+b, fe, f, hle, hs)

# A request's turn, given again with what is finally left free, takes nothing more and opens
# the same gather.
def waits_head0(+h: Bool, +s: Bool, +lo: Bool, +g: +List<Nat>, +mi: Nat, +pa: Nat, +fe: Nat, +nz: IsTrue(le(1n, fe)),
  +want: Nat, +f: Nat, +hf: IsTrue(le(fe, f)), +eh: {h == held(lo, pa, g) : Bool}, +es: {s == short(gg(mi, want), f) : Bool})
  -> Sigma<&2, &2, Waits(act(lo, g, gg(en(act0(h, s), want), f), want, mi, pa, fe), need.k(want, gg(en(act0(h, s), want), f))),
       u => {gnext(act(lo, g, gg(en(act0(h, s), want), f), want, mi, pa, fe), pa, g) == gnext(act0(h, s), pa, g) : +List<Nat>}>:
  match h s:
    case True{} s0:
      %eh : Sigma<&2, &2, Waits(act0(_, short(gg(mi, want), fe)), want), u => {gnext(act0(_, short(gg(mi, want), fe)), pa, g) == g : +List<Nat>}>
      (Unit{}, {==})
    case False{} True{}:
      %eh : Sigma<&2, &2, Waits(act0(_, short(gg(mi, want), fe)), want), u => {gnext(act0(_, short(gg(mi, want), fe)), pa, g) == pa <> g : +List<Nat>}>
      %is_true_eq(short(gg(mi, want), fe), short_mono(gg(mi, want), fe, f, hf, true_is(short(gg(mi, want), f), es)))
        : Sigma<&2, &2, Waits(act0(False{}, _), want), u => {gnext(act0(False{}, _), pa, g) == pa <> g : +List<Nat>}>
      (Unit{}, {==})
    case False{} False{}:
      match want f:
        case 0n f0:
          %eh : Sigma<&2, &2, Waits(act0(_, short(gg(mi, 0n), fe)), 0n), u => {gnext(act0(_, short(gg(mi, 0n), fe)), pa, g) == g : +List<Nat>}>
          %gg_z(mi) : Sigma<&2, &2, Waits(act0(False{}, short(_, fe)), 0n), u => {gnext(act0(False{}, short(_, fe)), pa, g) == g : +List<Nat>}>
          (Unit{}, {==})
        case 1n+w 0n:
          absurd_nat(Sigma<&2, &2, Waits(act(lo, g, 0n, 1n+w, mi, pa, fe), need.k(1n+w, 0n)), u => {gnext(act(lo, g, 0n, 1n+w, mi, pa, fe), pa, g) == g : +List<Nat>}>,
            no_room(fe, 0n, nz, hf, Unit{}))
        case 1n+w 1n+c:
          (Unit{}, {==})

def waits_head(+fe: Nat, +nz: IsTrue(le(1n, fe)), +lo: Bool, +g: +List<Nat>, +k: Nat, +want: Nat, +mi: Nat, +pa: Nat, +f: Nat, +hf: IsTrue(le(fe, f)))
  -> Sigma<&2, &2, Waits(act(lo, g, add(k, gg(en(act(lo, g, k, want, mi, pa, f), need.k(want, k)), f)), want, mi, pa, fe), need.k(want, add(k, gg(en(act(lo, g, k, want, mi, pa, f), need.k(want, k)), f)))),
       u => {gnext(act(lo, g, add(k, gg(en(act(lo, g, k, want, mi, pa, f), need.k(want, k)), f)), want, mi, pa, fe), pa, g) == gnext(act(lo, g, k, want, mi, pa, f), pa, g) : +List<Nat>}>:
  match k:
    case 0n:
      waits_head0(held(lo, pa, g), short(gg(mi, want), f), lo, g, mi, pa, fe, nz, want, f, hf, {==}, {==})
    case 1n+j:
      (Unit{}, {==})

def waits_gwalk(+fe: Nat, +nz: IsTrue(le(1n, fe)), +lo: Bool, +xs: +List<E>, +f: Nat, +g: +List<Nat>, +hf: IsTrue(le(fe, wf(gwalk(lo, xs, f, g)))))
  -> Sigma<&2, &2, WaitsN(fe, lo, wl(gwalk(lo, xs, f, g)), g), u => {gacc(fe, lo, wl(gwalk(lo, xs, f, g)), g) == gend(lo, xs, f, g) : +List<Nat>}>:
  match xs:
    case Nil{}:
      (Unit{}, {==})
    case Con{E{id, want, k, pred, age, mi, pa}, t}:
      +ht = le_wf_cons(fe, E{id, want, add(k, gg(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f)), pred, age, mi, pa}, gwalk(lo, t, gr(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f), gnext(gact(lo, g, E{id, want, k, pred, age, mi, pa}, f), pa, g)), hf)
      +hp = waits_head(fe, nz, lo, g, k, want, mi, pa, f, le_trans(fe, wf(gwalk(lo, t, gr(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f), gnext(gact(lo, g, E{id, want, k, pred, age, mi, pa}, f), pa, g))), f, ht, le_trans(wf(gwalk(lo, t, gr(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f), gnext(gact(lo, g, E{id, want, k, pred, age, mi, pa}, f), pa, g))), gr(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f), f, gwalk_le(lo, t, gr(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f), gnext(gact(lo, g, E{id, want, k, pred, age, mi, pa}, f), pa, g)), gr_le(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f))))
      +tp = waits_gwalk(fe, nz, lo, t, gr(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f), gnext(gact(lo, g, E{id, want, k, pred, age, mi, pa}, f), pa, g), ht)
      %wl_cons(E{id, want, add(k, gg(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f)), pred, age, mi, pa}, gwalk(lo, t, gr(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f), gnext(gact(lo, g, E{id, want, k, pred, age, mi, pa}, f), pa, g))) : Sigma<&2, &2, WaitsN(fe, lo, _, g), u => {gacc(fe, lo, _, g) == gend(lo, t, gr(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f), gnext(gact(lo, g, E{id, want, k, pred, age, mi, pa}, f), pa, g)) : +List<Nat>}>
      %Equal.sym(+List<Nat>, gnext(act(lo, g, add(k, gg(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f)), want, mi, pa, fe), pa, g), gnext(gact(lo, g, E{id, want, k, pred, age, mi, pa}, f), pa, g), psnd(Waits(act(lo, g, add(k, gg(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f)), want, mi, pa, fe), need.k(want, add(k, gg(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f)))), {gnext(act(lo, g, add(k, gg(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f)), want, mi, pa, fe), pa, g) == gnext(gact(lo, g, E{id, want, k, pred, age, mi, pa}, f), pa, g) : +List<Nat>}, hp))
        : Sigma<&2, &2, Sigma<&2, &2, Waits(act(lo, g, add(k, gg(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f)), want, mi, pa, fe), need.k(want, add(k, gg(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f)))), u => WaitsN(fe, lo, wl(gwalk(lo, t, gr(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f), gnext(gact(lo, g, E{id, want, k, pred, age, mi, pa}, f), pa, g))), _)>, u => {gacc(fe, lo, wl(gwalk(lo, t, gr(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f), gnext(gact(lo, g, E{id, want, k, pred, age, mi, pa}, f), pa, g))), _) == gend(lo, t, gr(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f), gnext(gact(lo, g, E{id, want, k, pred, age, mi, pa}, f), pa, g)) : +List<Nat>}>
      ((pfst(Waits(act(lo, g, add(k, gg(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f)), want, mi, pa, fe), need.k(want, add(k, gg(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f)))), {gnext(act(lo, g, add(k, gg(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f)), want, mi, pa, fe), pa, g) == gnext(gact(lo, g, E{id, want, k, pred, age, mi, pa}, f), pa, g) : +List<Nat>}, hp), pfst(WaitsN(fe, lo, wl(gwalk(lo, t, gr(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f), gnext(gact(lo, g, E{id, want, k, pred, age, mi, pa}, f), pa, g))), gnext(gact(lo, g, E{id, want, k, pred, age, mi, pa}, f), pa, g)), {gacc(fe, lo, wl(gwalk(lo, t, gr(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f), gnext(gact(lo, g, E{id, want, k, pred, age, mi, pa}, f), pa, g))), gnext(gact(lo, g, E{id, want, k, pred, age, mi, pa}, f), pa, g)) == gend(lo, t, gr(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f), gnext(gact(lo, g, E{id, want, k, pred, age, mi, pa}, f), pa, g)) : +List<Nat>}, tp)), psnd(WaitsN(fe, lo, wl(gwalk(lo, t, gr(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f), gnext(gact(lo, g, E{id, want, k, pred, age, mi, pa}, f), pa, g))), gnext(gact(lo, g, E{id, want, k, pred, age, mi, pa}, f), pa, g)), {gacc(fe, lo, wl(gwalk(lo, t, gr(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f), gnext(gact(lo, g, E{id, want, k, pred, age, mi, pa}, f), pa, g))), gnext(gact(lo, g, E{id, want, k, pred, age, mi, pa}, f), pa, g)) == gend(lo, t, gr(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, f), f), gnext(gact(lo, g, E{id, want, k, pred, age, mi, pa}, f), pa, g)) : +List<Nat>}, tp))

def waits_skip.go(+fe: Nat, +q: Nat, +r: +List<Nat>, +xs: +List<E>) -> Sigma<&2, &2, WaitsN(fe, True{}, xs, q <> r), u => {gacc(fe, True{}, xs, q <> r) == q <> r : +List<Nat>}>:
  match xs:
    case Nil{}:
      (Unit{}, {==})
    case Con{E{id, want, k, pred, age, mi, pa}, t}:
      match k:
        case 0n:
          +sp = waits_skip.go(fe, q, r, t)
          ((Unit{}, pfst(WaitsN(fe, True{}, t, q <> r), {gacc(fe, True{}, t, q <> r) == q <> r : +List<Nat>}, sp)), psnd(WaitsN(fe, True{}, t, q <> r), {gacc(fe, True{}, t, q <> r) == q <> r : +List<Nat>}, sp))
        case 1n+j:
          +sp = waits_skip.go(fe, q, r, t)
          ((Unit{}, pfst(WaitsN(fe, True{}, t, q <> r), {gacc(fe, True{}, t, q <> r) == q <> r : +List<Nat>}, sp)), psnd(WaitsN(fe, True{}, t, q <> r), {gacc(fe, True{}, t, q <> r) == q <> r : +List<Nat>}, sp))

# The low lane behind an open gather: every request in it is held.
def waits_skip(+fe: Nat, +lo: Bool, +g: +List<Nat>, +xs: +List<E>, h: IsTrue(skip(lo, g)))
  -> Sigma<&2, &2, WaitsN(fe, lo, xs, g), u => {gacc(fe, lo, xs, g) == g : +List<Nat>}>:
  match lo g:
    case False{} Nil{}:
      match h:
    case False{} Con{q, r}:
      match h:
    case True{} Nil{}:
      match h:
    case True{} Con{+q, +r}:
      waits_skip.go(fe, q, r, xs)

def old_ok(-A: Data, -B: Data, +fe: Nat, +x: Nat, +nz: IsTrue(le(1n, fe)), +hx: IsTrue(le(fe, x)), fd: Either<&2, &2, A, IsZero(x)>) -> Either<&2, &2, A, B>:
  match fd:
    case Inl{a}:
      Inl{a}
    case Inr{z}:
      absurd_nat(Either<&2, &2, A, B>, no_room(fe, x, nz, hx, z))

def waits_glwalk.if(+sk: Bool, +fe: Nat, +nz: IsTrue(le(1n, fe)), +lo: Bool, +o: +List<E>, +n: +List<E>, +f: Nat, +g: +List<Nat>,
  +hl: IsTrue(le(fe, lf(glwalk.if(sk, lo, o, n, f, g)))), +e: {sk == skip(lo, g) : Bool})
  -> Sigma<&2, &2, WaitsLane(fe, lo, g, ll(glwalk.if(sk, lo, o, n, f, g))), u => {gaccL(fe, lo, g, ll(glwalk.if(sk, lo, o, n, f, g))) == glg.if(sk, lo, o, n, f, g) : +List<Nat>}>:
  match sk:
    case True{}:
      +sp = waits_skip(fe, lo, g, n, true_is(skip(lo, g), e))
      ((Inr{true_is(skip(lo, g), e)}, pfst(WaitsN(fe, lo, n, g), {gacc(fe, lo, n, g) == g : +List<Nat>}, sp)), psnd(WaitsN(fe, lo, n, g), {gacc(fe, lo, n, g) == g : +List<Nat>}, sp))
    case False{}:
      +np = waits_gwalk(fe, nz, lo, n, wf(walk(o, f)), g, hl)
      ((old_ok(AllFilled(wl(walk(o, f))), IsTrue(skip(lo, g)), fe, wf(walk(o, f)), nz,
         le_trans(fe, wf(gwalk(lo, n, wf(walk(o, f)), g)), wf(walk(o, f)), hl, gwalk_le(lo, n, wf(walk(o, f)), g)), walk_fd(o, f)), pfst(WaitsN(fe, lo, wl(gwalk(lo, n, wf(walk(o, f)), g)), g), {gacc(fe, lo, wl(gwalk(lo, n, wf(walk(o, f)), g)), g) == gend(lo, n, wf(walk(o, f)), g) : +List<Nat>}, np)), psnd(WaitsN(fe, lo, wl(gwalk(lo, n, wf(walk(o, f)), g)), g), {gacc(fe, lo, wl(gwalk(lo, n, wf(walk(o, f)), g)), g) == gend(lo, n, wf(walk(o, f)), g) : +List<Nat>}, np))

def waits_glwalk(+fe: Nat, +nz: IsTrue(le(1n, fe)), +lo: Bool, +l: Lane, +f: Nat, +g: +List<Nat>, +hl: IsTrue(le(fe, lf(glwalk(lo, l, f, g)))))
  -> Sigma<&2, &2, WaitsLane(fe, lo, g, ll(glwalk(lo, l, f, g))), u => {gaccL(fe, lo, g, ll(glwalk(lo, l, f, g))) == glg(lo, l, f, g) : +List<Nat>}>:
  match l:
    case Lane{o, n}:
      waits_glwalk.if(skip(lo, g), fe, nz, lo, o, n, f, g, hl, {==})

def islow_gwalks(+t: +List<Lane>, +f: Nat, +g: +List<Nat>) -> {islow(t) == islow(lsl(gwalks(t, f, g))) : Bool}:
  match t:
    case Nil{}:
      {==}
    case Con{+l, +r}:
      {==}

def waits_gwalks(+fe: Nat, +nz: IsTrue(le(1n, fe)), +ls: +List<Lane>, +f: Nat, +g: +List<Nat>, +hf: IsTrue(le(fe, lsf(gwalks(ls, f, g)))))
  -> WaitsLs(fe, lsl(gwalks(ls, f, g)), g):
  match ls:
    case Nil{}:
      Unit{}
    case Con{+l, +t}:
      +lp = waits_glwalk(fe, nz, islow(t), l, f, g, le_trans(fe, lsf(gwalks(t, lf(glwalk(islow(t), l, f, g)), glg(islow(t), l, f, g))), lf(glwalk(islow(t), l, f, g)), hf, gwalks_le(t, lf(glwalk(islow(t), l, f, g)), glg(islow(t), l, f, g))))
      %islow_gwalks(t, lf(glwalk(islow(t), l, f, g)), glg(islow(t), l, f, g)) : Sigma<&2, &2, WaitsLane(fe, _, g, ll(glwalk(islow(t), l, f, g))), u => WaitsLs(fe, lsl(gwalks(t, lf(glwalk(islow(t), l, f, g)), glg(islow(t), l, f, g))), gaccL(fe, _, g, ll(glwalk(islow(t), l, f, g))))>
      %Equal.sym(+List<Nat>, gaccL(fe, islow(t), g, ll(glwalk(islow(t), l, f, g))), glg(islow(t), l, f, g), psnd(WaitsLane(fe, islow(t), g, ll(glwalk(islow(t), l, f, g))), {gaccL(fe, islow(t), g, ll(glwalk(islow(t), l, f, g))) == glg(islow(t), l, f, g) : +List<Nat>}, lp)) : Sigma<&2, &2, WaitsLane(fe, islow(t), g, ll(glwalk(islow(t), l, f, g))), u => WaitsLs(fe, lsl(gwalks(t, lf(glwalk(islow(t), l, f, g)), glg(islow(t), l, f, g))), _)>
      (pfst(WaitsLane(fe, islow(t), g, ll(glwalk(islow(t), l, f, g))), {gaccL(fe, islow(t), g, ll(glwalk(islow(t), l, f, g))) == glg(islow(t), l, f, g) : +List<Nat>}, lp), waits_gwalks(fe, nz, t, lf(glwalk(islow(t), l, f, g)), glg(islow(t), l, f, g), hf))

def le_of_eq(+a: Nat, +b: Nat, e: {a == b : Nat}) -> IsTrue(le(a, b)):
  %e : IsTrue(le(a, _))
  le_refl(a)

def only_a_gather_leaves_permits_free.top(+fe: Nat, +ls: +List<Lane>, +f: Nat, +e: {fe == lsf(gwalks(ls, f, [])) : Nat})
  -> Either<&2, &2, IsZero(lsf(gwalks(ls, f, []))), WaitsLs(lsf(gwalks(ls, f, [])), lsl(gwalks(ls, f, [])), [])>:
  match fe:
    case 0n:
      %e : Either<&2, &2, IsZero(_), WaitsLs(_, lsl(gwalks(ls, f, [])), [])>
      Inl{Unit{}}
    case 1n+q:
      %e : Either<&2, &2, IsZero(_), WaitsLs(_, lsl(gwalks(ls, f, [])), [])>
      Inr{waits_gwalks(1n+q, Unit{}, ls, f, [], le_of_eq(1n+q, lsf(gwalks(ls, f, [])), e))}

law only_a_gather_leaves_permits_free:
  for +p: Pol
  for +s: St
  Either<&2, &2, IsZero(sfree(step(p, Decide{}, s))), WaitsLs(sfree(step(p, Decide{}, s)), slanes(step(p, Decide{}, s)), [])>

def only_a_gather_leaves_permits_free(p, s):
  match s:
    case St{f, b, ls}:
      only_a_gather_leaves_permits_free.top(lsf(gwalks(ls, f, [])), ls, f, {==})

# --- LAW (a gate waits only for admitted work or a gather) ---------------------------------
# What a land's gate leaves wait for (05 "Gates and lands", "Land latency"). Without a minimum, a
# decision leaves every gate request filled, or no permit free and the low lane as it was, so a
# waiting gate leaf waits only for permits held by admitted work. A gather breaks that
# (`gate_waits_only_for_admitted_work_refuted`): a gate request that gathers waits with permits
# free, and so does a later gate request of its party (`a_gather_holds_back_its_party_and_low_work`).
# What holds on the pool `empty` makes: after a decision every gate request is filled, or the low
# lane is as it was and no permit is free or every request still waiting is one a gather holds
# back (`only_a_gather_leaves_permits_free`): a gatherer short of its minimum, or a request of the
# party of a gather opened before it. Low work never runs in a waiting gate request's place, and a
# gather ends at the bound (`gather_ends_at_the_bound`).

def sgate(s: St) -> Lane:
  nth(1n, slanes(s))

def slow(s: St) -> Lane:
  nth(2n, slanes(s))

# A gather is open once `g` names one, and stays open along a re-examination.
def gnext_open(a: Act, +pa: Nat, +g: +List<Nat>, h: IsTrue(skip(True{}, g))) -> IsTrue(skip(True{}, gnext(a, pa, g))):
  match a:
    case Take{}:
      h
    case Hold{}:
      h
    case Open{}:
      Unit{}

def gacc_open(+fe: Nat, +lo: Bool, +xs: +List<E>, +acc: +List<Nat>, h: IsTrue(skip(True{}, acc))) -> IsTrue(skip(True{}, gacc(fe, lo, xs, acc))):
  match xs:
    case Nil{}:
      h
    case Con{E{id, want, k, pred, age, mi, pa}, t}:
      gacc_open(fe, lo, t, gnext(act(lo, acc, k, want, mi, pa, fe), pa, acc), gnext_open(act(lo, acc, k, want, mi, pa, fe), pa, acc, h))

def open_eq(+a: +List<Nat>, +b: +List<Nat>, e: {a == b : +List<Nat>}, h: IsTrue(skip(True{}, a))) -> IsTrue(skip(True{}, b)):
  %e : IsTrue(skip(True{}, _))
  h

# A lane that is not the low one is never skipped.
def skip_false(+acc: +List<Nat>, x: IsTrue(skip(False{}, acc))) -> Empty:
  match acc:
    case Nil{}:
      match x:
    case Con{q, r}:
      match x:

def filled_cons(-A: Data, -B: Data, -C: Data, a: A, ih: Either<&2, &2, B, C>) -> Either<&2, &2, Sigma<&2, &2, A, u => B>, C>:
  match ih:
    case Inl{b}:
      Inl{(a, b)}
    case Inr{c}:
      Inr{c}

# A request outside the low lane with no gather before it waits filled or opens a gather.
def waitsn_open.s(+fe: Nat, +s: Bool, +want: Nat, +pa: Nat, +t: +List<E>,
  w: Sigma<&2, &2, Waits(act0(False{}, s), want), u => WaitsN(fe, False{}, t, gnext(act0(False{}, s), pa, []))>,
  rec: WaitsN(fe, False{}, t, []) -> Either<&2, &2, AllFilled(t), IsTrue(skip(True{}, gacc(fe, False{}, t, [])))>)
  -> Either<&2, &2, Sigma<&2, &2, IsZero(want), u => AllFilled(t)>, IsTrue(skip(True{}, gacc(fe, False{}, t, gnext(act0(False{}, s), pa, []))))>:
  match s:
    case True{}:
      Inr{gacc_open(fe, False{}, t, pa <> [], Unit{})}
    case False{}:
      (w1, w2) = w
      filled_cons(IsZero(want), AllFilled(t), IsTrue(skip(True{}, gacc(fe, False{}, t, []))), w1, rec(w2))

def waitsn_open.k(+fe: Nat, +id: Nat, +want: Nat, +k: Nat, +pred: Nat, +age: Nat, +mi: Nat, +pa: Nat, +t: +List<E>,
  w: WaitsN(fe, False{}, E{id, want, k, pred, age, mi, pa} <> t, []),
  rec: WaitsN(fe, False{}, t, []) -> Either<&2, &2, AllFilled(t), IsTrue(skip(True{}, gacc(fe, False{}, t, [])))>)
  -> Either<&2, &2, AllFilled(E{id, want, k, pred, age, mi, pa} <> t), IsTrue(skip(True{}, gacc(fe, False{}, E{id, want, k, pred, age, mi, pa} <> t, [])))>:
  match k:
    case 0n:
      waitsn_open.s(fe, short(gg(mi, want), fe), want, pa, t, w, rec)
    case 1n+j:
      (w1, w2) = w
      filled_cons(Unit, AllFilled(t), IsTrue(skip(True{}, gacc(fe, False{}, t, []))), w1, rec(w2))

def waitsn_open.nil(+fe: Nat, +xs: +List<E>, w: WaitsN(fe, False{}, xs, []))
  -> Either<&2, &2, AllFilled(xs), IsTrue(skip(True{}, gacc(fe, False{}, xs, [])))>:
  match xs:
    case Nil{}:
      Inl{Unit{}}
    case Con{E{id, want, k, pred, age, mi, pa}, t}:
      waitsn_open.k(fe, id, want, k, pred, age, mi, pa, t, w, r => waitsn_open.nil(fe, t, r))

def waitsn_open(+fe: Nat, +xs: +List<E>, +acc: +List<Nat>, w: WaitsN(fe, False{}, xs, acc))
  -> Either<&2, &2, AllFilled(xs), IsTrue(skip(True{}, gacc(fe, False{}, xs, acc)))>:
  match acc:
    case Nil{}:
      waitsn_open.nil(fe, xs, w)
    case Con{q, r}:
      Inr{gacc_open(fe, False{}, xs, q <> r, Unit{})}

def waitslane_open.o(+fe: Nat, +acc: +List<Nat>, +o: +List<E>, +n: +List<E>,
  wo: Either<&2, &2, AllFilled(o), IsTrue(skip(False{}, acc))>, wn: WaitsN(fe, False{}, n, acc))
  -> Either<&2, &2, Sigma<&2, &2, AllFilled(o), u => AllFilled(n)>, IsTrue(skip(True{}, gacc(fe, False{}, n, acc)))>:
  match wo:
    case Inl{ao}:
      filled_cons(AllFilled(o), AllFilled(n), IsTrue(skip(True{}, gacc(fe, False{}, n, acc))), ao, waitsn_open(fe, n, acc, wn))
    case Inr{x}:
      absurd_nat(Either<&2, &2, Sigma<&2, &2, AllFilled(o), u => AllFilled(n)>, IsTrue(skip(True{}, gacc(fe, False{}, n, acc)))>, skip_false(acc, x))

# A lane outside the low one, every request of it waiting, is filled or leaves a gather open.
def waitslane_open(+fe: Nat, +acc: +List<Nat>, +m: Lane, w: WaitsLane(fe, False{}, acc, m))
  -> Either<&2, &2, AllFilledL(m), IsTrue(skip(True{}, gaccL(fe, False{}, acc, m)))>:
  match m:
    case Lane{o, n}:
      (wo, wn) = w
      waitslane_open.o(fe, acc, o, n, wo, wn)

# A walk on no free permit changes no request.
def gwalk_zero_l(+lo: Bool, +lane: +List<E>, +g: +List<Nat>) -> {lane == wl(gwalk(lo, lane, 0n, g)) : +List<E>}:
  match lane:
    case Nil{}:
      {==}
    case Con{E{id, want, k, pred, age, mi, pa}, t}:
      %gg_z(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, 0n)) : {E{id, want, k, pred, age, mi, pa} <> t == wl(W.cons(E{id, want, add(k, _), pred, age, mi, pa}, gwalk(lo, t, gr(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, 0n), 0n), gnext(gact(lo, g, E{id, want, k, pred, age, mi, pa}, 0n), pa, g)))) : +List<E>}
      %Equal.sym(Nat, add(k, 0n), k, add_zero_r(k)) : {E{id, want, k, pred, age, mi, pa} <> t == wl(W.cons(E{id, want, _, pred, age, mi, pa}, gwalk(lo, t, gr(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, 0n), 0n), gnext(gact(lo, g, E{id, want, k, pred, age, mi, pa}, 0n), pa, g)))) : +List<E>}
      %gr_z(gneed(lo, g, E{id, want, k, pred, age, mi, pa}, 0n)) : {E{id, want, k, pred, age, mi, pa} <> t == wl(W.cons(E{id, want, k, pred, age, mi, pa}, gwalk(lo, t, _, gnext(gact(lo, g, E{id, want, k, pred, age, mi, pa}, 0n), pa, g)))) : +List<E>}
      %wl_cons(E{id, want, k, pred, age, mi, pa}, gwalk(lo, t, 0n, gnext(gact(lo, g, E{id, want, k, pred, age, mi, pa}, 0n), pa, g))) : {E{id, want, k, pred, age, mi, pa} <> t == _ : +List<E>}
      %gwalk_zero_l(lo, t, gnext(gact(lo, g, E{id, want, k, pred, age, mi, pa}, 0n), pa, g)) : {E{id, want, k, pred, age, mi, pa} <> t == E{id, want, k, pred, age, mi, pa} <> _ : +List<E>}
      {==}

def glwalk_zero.if(+sk: Bool, +lo: Bool, +o: +List<E>, +n: +List<E>, +g: +List<Nat>) -> {ll(glwalk.if(sk, lo, o, n, 0n, g)) == Lane{o, n} : Lane}:
  match sk:
    case True{}:
      {==}
    case False{}:
      %walk_zero_f(o) : {Lane{wl(walk(o, 0n)), wl(gwalk(lo, n, _, g))} == Lane{o, n} : Lane}
      %walk_zero_l(o) : {Lane{_, wl(gwalk(lo, n, 0n, g))} == Lane{o, n} : Lane}
      %gwalk_zero_l(lo, n, g) : {Lane{o, _} == Lane{o, n} : Lane}
      {==}

def glwalk_zero(+lo: Bool, +l: Lane, +g: +List<Nat>) -> {ll(glwalk(lo, l, 0n, g)) == l : Lane}:
  match l:
    case Lane{o, n}:
      glwalk_zero.if(skip(lo, g), lo, o, n, g)

# The low lane behind an open gather is skipped.
def low_skip(+l: Lane, +f: Nat, +g: +List<Nat>, h: IsTrue(skip(True{}, g))) -> {ll(glwalk(True{}, l, f, g)) == l : Lane}:
  match l:
    case Lane{o, n}:
      match g:
        case Nil{}:
          match h:
        case Con{q, r}:
          {==}

def gate_or_low_kept.or(+gl: Lane, +l: Lane, +f: Nat, +g: +List<Nat>, +fe: Nat,
  eq: {gaccL(fe, False{}, g, ll(glwalk(False{}, gl, f, g))) == glg(False{}, gl, f, g) : +List<Nat>},
  r: Either<&2, &2, AllFilledL(ll(glwalk(False{}, gl, f, g))), IsTrue(skip(True{}, gaccL(fe, False{}, g, ll(glwalk(False{}, gl, f, g)))))>)
  -> Either<&2, &2, AllFilledL(ll(glwalk(False{}, gl, f, g))), {ll(glwalk(True{}, l, lf(glwalk(False{}, gl, f, g)), glg(False{}, gl, f, g))) == l : Lane}>:
  match r:
    case Inl{a}:
      Inl{a}
    case Inr{op}:
      Inr{low_skip(l, lf(glwalk(False{}, gl, f, g)), glg(False{}, gl, f, g), open_eq(gaccL(fe, False{}, g, ll(glwalk(False{}, gl, f, g))), glg(False{}, gl, f, g), eq, op))}

def gate_or_low_kept.go(+gl: Lane, +l: Lane, +f: Nat, +g: +List<Nat>, +fe: Nat,
  w: Sigma<&2, &2, WaitsLane(fe, False{}, g, ll(glwalk(False{}, gl, f, g))), u => {gaccL(fe, False{}, g, ll(glwalk(False{}, gl, f, g))) == glg(False{}, gl, f, g) : +List<Nat>}>)
  -> Either<&2, &2, AllFilledL(ll(glwalk(False{}, gl, f, g))), {ll(glwalk(True{}, l, lf(glwalk(False{}, gl, f, g)), glg(False{}, gl, f, g))) == l : Lane}>:
  (wa, eq) = w
  gate_or_low_kept.or(gl, l, f, g, fe, eq, waitslane_open(fe, g, ll(glwalk(False{}, gl, f, g)), wa))

# The gate's lane, walked on what the interactive lane left free with the gathers it left open:
# filled, or it leaves no permit free, or a gather open, and either way the low lane as it was.
def gate_or_low_kept(+fe: Nat, +gl: Lane, +l: Lane, +f: Nat, +g: +List<Nat>, +e: {fe == lf(glwalk(False{}, gl, f, g)) : Nat})
  -> Either<&2, &2, AllFilledL(ll(glwalk(False{}, gl, f, g))), {ll(glwalk(True{}, l, lf(glwalk(False{}, gl, f, g)), glg(False{}, gl, f, g))) == l : Lane}>:
  match fe:
    case 0n:
      %e : Either<&2, &2, AllFilledL(ll(glwalk(False{}, gl, f, g))), {ll(glwalk(True{}, l, _, glg(False{}, gl, f, g))) == l : Lane}>
      Inr{glwalk_zero(True{}, l, glg(False{}, gl, f, g))}
    case 1n+q:
      gate_or_low_kept.go(gl, l, f, g, 1n+q, waits_glwalk(1n+q, Unit{}, False{}, gl, f, g, le_of_eq(1n+q, lf(glwalk(False{}, gl, f, g)), e)))

# LAW (low work never passes a waiting gate request): after a decision on the pool's three lanes,
# every gate request is filled, or the low lane is as it was.
law low_never_passes_a_waiting_gate:
  for +p: Pol
  for +f: Nat
  for +b: Nat
  for +i: Lane
  for +g: Lane
  for +l: Lane
  Either<&2, &2, AllFilledL(sgate(step(p, Decide{}, St{f, b, [i, g, l]}))), {slow(step(p, Decide{}, St{f, b, [i, g, l]})) == l : Lane}>

def low_never_passes_a_waiting_gate(p, f, b, i, g, l):
  gate_or_low_kept(lf(glwalk(False{}, g, lf(glwalk(False{}, i, f, [])), glg(False{}, i, f, []))), g, l, lf(glwalk(False{}, i, f, [])), glg(False{}, i, f, []), {==})

def gate_waits.go(-A: Data, -B: Data, -C: Data, gf: Either<&2, &2, A, B>, og: C) -> Either<&2, &2, A, Sigma<&2, &2, B, u => C>>:
  match gf:
    case Inl{a}:
      Inl{a}
    case Inr{e}:
      Inr{(e, og)}

# LAW: after a decision on the pool's three lanes, every gate request is filled, or the low lane is
# as it was and either no permit is free or every request still waiting is held back by a gather.
law gate_waits_only_for_admitted_work_or_a_gather:
  for +p: Pol
  for +f: Nat
  for +b: Nat
  for +i: Lane
  for +g: Lane
  for +l: Lane
  Either<&2, &2, AllFilledL(sgate(step(p, Decide{}, St{f, b, [i, g, l]}))),
    Sigma<&2, &2, {slow(step(p, Decide{}, St{f, b, [i, g, l]})) == l : Lane},
      u => Either<&2, &2, IsZero(sfree(step(p, Decide{}, St{f, b, [i, g, l]}))), WaitsLs(sfree(step(p, Decide{}, St{f, b, [i, g, l]})), slanes(step(p, Decide{}, St{f, b, [i, g, l]})), [])>>>

def gate_waits_only_for_admitted_work_or_a_gather(p, f, b, i, g, l):
  gate_waits.go(AllFilledL(sgate(step(p, Decide{}, St{f, b, [i, g, l]}))), {slow(step(p, Decide{}, St{f, b, [i, g, l]})) == l : Lane},
    Either<&2, &2, IsZero(sfree(step(p, Decide{}, St{f, b, [i, g, l]}))), WaitsLs(sfree(step(p, Decide{}, St{f, b, [i, g, l]})), slanes(step(p, Decide{}, St{f, b, [i, g, l]})), [])>,
    low_never_passes_a_waiting_gate(p, f, b, i, g, l), only_a_gather_leaves_permits_free(p, St{f, b, [i, g, l]}))

# Refuted, proven: the law as it held without minimums. Round 9's pool of 4: a gate job holds 3,
# and a gate reservation asks 18 with min 4 (party 1). The decision leaves it gathering, unfilled,
# with 1 permit free: a gate request waits for its own gather, not for admitted work.
law gate_waits_only_for_admitted_work_refuted:
  for +h: Either<&2, &2, AllFilledL(sgate(step(Spt{}, Decide{}, run(Spt{}, [Arrive{Gate{}, 1n, 2n, 5n, 0n, 0n, 0n}, Decide{}, Arrive{Gate{}, 2n, 17n, 9n, 0n, 4n, 1n}], empty(4n, 30n))))),
    Sigma<&2, &2, {slow(step(Spt{}, Decide{}, run(Spt{}, [Arrive{Gate{}, 1n, 2n, 5n, 0n, 0n, 0n}, Decide{}, Arrive{Gate{}, 2n, 17n, 9n, 0n, 4n, 1n}], empty(4n, 30n)))) == slow(run(Spt{}, [Arrive{Gate{}, 1n, 2n, 5n, 0n, 0n, 0n}, Decide{}, Arrive{Gate{}, 2n, 17n, 9n, 0n, 4n, 1n}], empty(4n, 30n))) : Lane},
      u => IsZero(sfree(step(Spt{}, Decide{}, run(Spt{}, [Arrive{Gate{}, 1n, 2n, 5n, 0n, 0n, 0n}, Decide{}, Arrive{Gate{}, 2n, 17n, 9n, 0n, 4n, 1n}], empty(4n, 30n)))))>>
  Empty

def gate_waits_only_for_admitted_work_refuted(h):
  match h:
    case Inl{a}:
      (ao, an) = a
      (h1, t1) = an
      (h2, t2) = t1
      match h2:
    case Inr{c}:
      (e, z) = c
      match z:
