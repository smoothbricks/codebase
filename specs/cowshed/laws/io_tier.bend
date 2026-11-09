# Disk I/O tiers for cowshed's job classes, as a model whose laws are a
# proposal's contract. It is not today's policy: 05_gateway.md "On macOS nothing cowshed starts
# is throttled for I/O" forbids any I/O tier below IMPORTANT, and this model states why, and what
# a tier would have to add to be safe.
#
# One disk serves one request per `Serve`. A request is a gate's (G) or a low job's (L).
# `held` counts the services a low request still needs while it holds a lock every gate
# request waits on: an APFS container's metadata transaction, a directory's vnode lock, a
# journal barrier. A service the disk gives to nobody while a gate waits on that lock is W.
#
# Flat: today, one queue in arrival order (every job at IOPOL_IMPORTANT).
# Tier: low at IOPOL_THROTTLE: a gate request goes first while `left` > 0; each pass over a
#   waiting low request spends one of `left`, and at 0 the low request goes next and `left`
#   refills to `bound` (the kernel's throttle window: a throttled I/O is delayed, never
#   starved). A low lock holder is throttled too: it moves only on a low turn, and every
#   other service is W.
# Inherit: Tier, except that a low lock holder runs at the tier of the gate waiting on it.
#
# Check: bun bend2/main.ts io_tier.bend --verdict
import Base

type T is Data:
  G{}
  L{}
  W{}

type Pol is Data:
  Flat{}
  Tier{}
  Inherit{}

type St is Data:
  St{bound: Nat, left: Nat, held: Nat, q: +List<T>, log: +List<T>}

type Op is Data:
  Arrive{t: T}
  Hold{d: Nat}
  Serve{}

# --- arithmetic ------------------------------------------------------------

def add(a: Nat, b: Nat) -> Nat:
  match a:
    case 0n:
      b
    case 1n+p:
      1n+add(p, b)

# a <= b
def le(a: Nat, b: Nat) -> Bool:
  match a b:
    case 0n b0:
      True{}
    case 1n+a1 0n:
      False{}
    case 1n+a1 1n+b1:
      le(a1, b1)

def IsTrue(b: Bool) -> Data:
  match b:
    case True{}:
      Unit
    case False{}:
      Empty

# --- the queue -------------------------------------------------------------

def hasG(q: +List<T>) -> Bool:
  match q:
    case Nil{}:
      False{}
    case Con{G{}, t}:
      True{}
    case Con{L{}, t}:
      hasG(t)
    case Con{W{}, t}:
      hasG(t)

def hasL(q: +List<T>) -> Bool:
  match q:
    case Nil{}:
      False{}
    case Con{G{}, t}:
      hasL(t)
    case Con{L{}, t}:
      True{}
    case Con{W{}, t}:
      hasL(t)

# The queue without its first request of a tier.
def dropG(q: +List<T>) -> +List<T>:
  match q:
    case Nil{}:
      []
    case Con{G{}, t}:
      t
    case Con{L{}, t}:
      L{} <> dropG(t)
    case Con{W{}, t}:
      W{} <> dropG(t)

def dropL(q: +List<T>) -> +List<T>:
  match q:
    case Nil{}:
      []
    case Con{G{}, t}:
      G{} <> dropL(t)
    case Con{L{}, t}:
      t
    case Con{W{}, t}:
      W{} <> dropL(t)

def snoc(xs: +List<T>, +y: T) -> +List<T>:
  match xs:
    case Nil{}:
      [y]
    case Con{h, t}:
      h <> snoc(t, y)

# --- a service ---------------------------------------------------------------

def flat(+b: Nat, +l: Nat, h: Nat, q: +List<T>, +log: +List<T>) -> St:
  match h q:
    case 1n+h1 q0:
      St{b, l, h1, q0, L{} <> log}
    case 0n Nil{}:
      St{b, l, 0n, [], log}
    case 0n Con{x, t}:
      St{b, l, 0n, t, x <> log}

# w: a low request waits; g: a gate request waits.
def tier.if(+b: Nat, +q: +List<T>, +log: +List<T>, w: Bool, g: Bool, l: Nat) -> St:
  match w g l:
    case True{} g0 0n:
      St{b, b, 0n, dropL(q), L{} <> log}
    case True{} True{} 1n+l1:
      St{b, l1, 0n, dropG(q), G{} <> log}
    case True{} False{} 1n+l1:
      St{b, b, 0n, dropL(q), L{} <> log}
    case False{} True{} l0:
      St{b, b, 0n, dropG(q), G{} <> log}
    case False{} False{} l0:
      St{b, b, 0n, q, log}

def tier(+b: Nat, h: Nat, l: Nat, +q: +List<T>, +log: +List<T>) -> St:
  match h l:
    case 0n l0:
      tier.if(b, q, log, hasL(q), hasG(q), l0)
    case 1n+h1 0n:
      St{b, b, h1, q, L{} <> log}
    case 1n+h1 1n+l1:
      St{b, l1, 1n+h1, q, W{} <> log}

def inherit(+b: Nat, l: Nat, h: Nat, +q: +List<T>, +log: +List<T>) -> St:
  match h:
    case 0n:
      tier(b, 0n, l, q, log)
    case 1n+h1:
      St{b, l, h1, q, L{} <> log}

def serve(+p: Pol, s: St) -> St:
  match p s:
    case Flat{} St{b, l, h, q, log}:
      flat(b, l, h, q, log)
    case Tier{} St{b, l, h, q, log}:
      tier(b, h, l, q, log)
    case Inherit{} St{b, l, h, q, log}:
      inherit(b, l, h, q, log)

def step(+p: Pol, o: Op, s: St) -> St:
  match o s:
    case Arrive{t} St{b, l, h, q, log}:
      St{b, l, h, snoc(q, t), log}
    case Hold{d} St{b, l, h, q, log}:
      St{b, l, add(h, d), q, log}
    case Serve{} s0:
      serve(p, s0)

def run(+p: Pol, os: +List<Op>, s: St) -> St:
  match os:
    case Nil{}:
      s
    case Con{o, t}:
      run(p, t, step(p, o, s))

def empty(+b: Nat) -> St:
  St{b, b, 0n, [], []}

def sbound(s: St) -> Nat:
  match s:
    case St{b, l, h, q, log}:
      b

def sleft(s: St) -> Nat:
  match s:
    case St{b, l, h, q, log}:
      l

def sheld(s: St) -> Nat:
  match s:
    case St{b, l, h, q, log}:
      h

def slog(s: St) -> +List<T>:
  match s:
    case St{b, l, h, q, log}:
      log

# What the last service gave to; W for none yet.
def last(s: St) -> T:
  match s:
    case St{b, l, h, q, Nil{}}:
      W{}
    case St{b, l, h, q, Con{x, t}}:
      x

# === laws ==========================================================================

# --- lemmas ------------------------------------------------------------------------

def le_refl(+a: Nat) -> IsTrue(le(a, a)):
  match a:
    case 0n:
      Unit{}
    case 1n+p:
      le_refl(p)

def le_pred(+a: Nat, +b: Nat) -> IsTrue(le(1n+a, b)) -> IsTrue(le(a, b)):
  match a b:
    case 0n b0:
      i => Unit{}
    case 1n+a1 0n:
      i => i
    case 1n+a1 1n+b1:
      le_pred(a1, b1)

# --- low liveness: the throttle window never overruns its bound ------------------

def Live(+s: St) -> Data:
  IsTrue(le(sleft(s), sbound(s)))

def tier_if_live(+b: Nat, +q: +List<T>, +log: +List<T>, +w: Bool, +g: Bool, +l: Nat)
  -> IsTrue(le(l, b)) -> Live(tier.if(b, q, log, w, g, l)):
  match w g l:
    case True{} g0 0n:
      i => le_refl(b)
    case True{} True{} 1n+l1:
      i => le_pred(l1, b, i)
    case True{} False{} 1n+l1:
      i => le_refl(b)
    case False{} True{} l0:
      i => le_refl(b)
    case False{} False{} l0:
      i => le_refl(b)

def tier_live(+b: Nat, +h: Nat, +l: Nat, +q: +List<T>, +log: +List<T>)
  -> IsTrue(le(l, b)) -> Live(tier(b, h, l, q, log)):
  match h l:
    case 0n l0:
      tier_if_live(b, q, log, hasL(q), hasG(q), l0)
    case 1n+h1 0n:
      i => le_refl(b)
    case 1n+h1 1n+l1:
      le_pred(l1, b)

def inherit_live(+b: Nat, +l: Nat, +h: Nat, +q: +List<T>, +log: +List<T>)
  -> IsTrue(le(l, b)) -> Live(inherit(b, l, h, q, log)):
  match h:
    case 0n:
      tier_live(b, 0n, l, q, log)
    case 1n+h1:
      i => i

def flat_live(+b: Nat, +l: Nat, +h: Nat, +q: +List<T>, +log: +List<T>)
  -> IsTrue(le(l, b)) -> Live(flat(b, l, h, q, log)):
  match h q:
    case 1n+h1 q0:
      i => i
    case 0n Nil{}:
      i => i
    case 0n Con{x, t}:
      i => i

# LAW (the window stays bounded): under every policy and every step, if `left` is at most
# `bound`, it still is after the step.
law low_window_stays_bounded:
  for +p: Pol
  for +o: Op
  for +s: St
  for +i: Live(s)
  Live(step(p, o, s))

def low_window_stays_bounded(p, o, s, i):
  match p o s:
    case p0 Arrive{t} St{b, l, h, q, log}:
      i
    case p0 Hold{d} St{b, l, h, q, log}:
      i
    case Flat{} Serve{} St{b, l, h, q, log}:
      flat_live(b, l, h, q, log, i)
    case Tier{} Serve{} St{b, l, h, q, log}:
      tier_live(b, h, l, q, log, i)
    case Inherit{} Serve{} St{b, l, h, q, log}:
      inherit_live(b, l, h, q, log, i)

# --- gate first --------------------------------------------------------------------

def gate_first.if(+b: Nat, +l1: Nat, +q: +List<T>, +log: +List<T>, +w: Bool, +g: Bool)
  -> IsTrue(g) -> {last(tier.if(b, q, log, w, g, 1n+l1)) == G{} : T}:
  match w g:
    case True{} True{}:
      i => {==}
    case True{} False{}:
      i => Empty.absurd({last(tier.if(b, q, log, True{}, False{}, 1n+l1)) == G{} : T}, i)
    case False{} True{}:
      i => {==}
    case False{} False{}:
      i => Empty.absurd({last(tier.if(b, q, log, False{}, False{}, 1n+l1)) == G{} : T}, i)

# LAW (under Tier a gate goes next): with no lock held and passes left, a waiting gate
# request is served next, whatever low requests arrived before it. Flat has no such law
# (`flat_gate_waits_behind_low`).
law gate_goes_first:
  for +b: Nat
  for +l1: Nat
  for +q: +List<T>
  for +log: +List<T>
  for +i: IsTrue(hasG(q))
  {last(serve(Tier{}, St{b, 1n+l1, 0n, q, log})) == G{} : T}

def gate_goes_first(b, l1, q, log, i):
  gate_first.if(b, l1, q, log, hasL(q), hasG(q), i)

def aged_low.if(+b: Nat, +q: +List<T>, +log: +List<T>, +w: Bool, +g: Bool)
  -> IsTrue(w) -> {last(tier.if(b, q, log, w, g, 0n)) == L{} : T}:
  match w g:
    case True{} True{}:
      i => {==}
    case True{} False{}:
      i => {==}
    case False{} g0:
      i => Empty.absurd({last(tier.if(b, q, log, False{}, g0, 0n)) == L{} : T}, i)

# LAW (the bounded exception): under Tier, with no lock held, once the window is spent a
# waiting low request is served next, even if a gate request is waiting.
law aged_low_goes_next:
  for +b: Nat
  for +q: +List<T>
  for +log: +List<T>
  for +i: IsTrue(hasL(q))
  {last(serve(Tier{}, St{b, 0n, 0n, q, log})) == L{} : T}

def aged_low_goes_next(b, q, log, i):
  aged_low.if(b, q, log, hasL(q), hasG(q), i)

# --- the inversion -------------------------------------------------------------------

# LAW (throttle inverts priority): under Tier, a low request holding the lock does not move
# while the window runs, and the service goes to nobody (W) while a gate waits on that lock.
law throttled_holder_stalls_the_gate:
  for +b: Nat
  for +l1: Nat
  for +h1: Nat
  for +q: +List<T>
  for +log: +List<T>
  Sigma<&2, &2, {sheld(serve(Tier{}, St{b, 1n+l1, 1n+h1, q, log})) == 1n+h1 : Nat},
    u => {last(serve(Tier{}, St{b, 1n+l1, 1n+h1, q, log})) == W{} : T}>

def throttled_holder_stalls_the_gate(b, l1, h1, q, log):
  ({==}, {==})

# LAW (inheritance moves the holder): under Inherit, while a lock is held, the next service
# is the holder's and `held` falls by one.
law inherited_holder_always_moves:
  for +b: Nat
  for +l: Nat
  for +h1: Nat
  for +q: +List<T>
  for +log: +List<T>
  Sigma<&2, &2, {sheld(serve(Inherit{}, St{b, l, 1n+h1, q, log})) == h1 : Nat},
    u => {last(serve(Inherit{}, St{b, l, 1n+h1, q, log})) == L{} : T}>

def inherited_holder_always_moves(b, l, h1, q, log):
  ({==}, {==})

# --- the difference, computed ----------------------------------------------------------

# Two low requests, then a gate request. Flat serves the gate third; Tier serves it first.
law flat_gate_waits_behind_low:
  Sigma<&2, &2,
    {slog(run(Flat{}, [Arrive{L{}}, Arrive{L{}}, Arrive{G{}}, Serve{}, Serve{}, Serve{}], empty(2n))) == [G{}, L{}, L{}] : +List<T>},
    u => {slog(run(Tier{}, [Arrive{L{}}, Arrive{L{}}, Arrive{G{}}, Serve{}, Serve{}, Serve{}], empty(2n))) == [L{}, L{}, G{}] : +List<T>}>

def flat_gate_waits_behind_low():
  ({==}, {==})

# A gate stream with one low request at bound 1: low goes after one gate service.
law low_is_live_under_a_gate_stream:
  {slog(run(Tier{}, [Arrive{L{}}, Arrive{G{}}, Arrive{G{}}, Arrive{G{}}, Serve{}, Serve{}, Serve{}], empty(1n))) == [G{}, L{}, G{}] : +List<T>}

def low_is_live_under_a_gate_stream():
  {==}

# A low request holding the lock for 2 services, bound 2, then a gate request: Tier serves the
# gate on the 7th service, Inherit on the 3rd.
law throttle_stretches_a_lock_wait:
  Sigma<&2, &2,
    {slog(run(Tier{}, [Hold{2n}, Arrive{G{}}, Serve{}, Serve{}, Serve{}, Serve{}, Serve{}, Serve{}, Serve{}], empty(2n))) == [G{}, L{}, W{}, W{}, L{}, W{}, W{}] : +List<T>},
    u => {slog(run(Inherit{}, [Hold{2n}, Arrive{G{}}, Serve{}, Serve{}, Serve{}], empty(2n))) == [G{}, L{}, L{}] : +List<T>}>

def throttle_stretches_a_lock_wait():
  ({==}, {==})
