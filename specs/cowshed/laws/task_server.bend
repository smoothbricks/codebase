# Task-server work units (specs/cowshed/05_gateway.md "The task server schedules work units",
# "Classes"; contract "Task-server laws"): the ready order and the lease places as the gateway's
# pure core `Ledger` keeps them (cowshed-gateway src/task_server.rs: `order_cmp`, `ready_order`,
# `select`). A unit has a class, the coordinator's urgent override, leverage, a cache-domain
# deadline, a unit id and the lease classes it holds from selection to its terminal fact; it is
# aged once it has waited the age bound, and `order_cmp` says which of two units goes first.
# `select` grants units in that order and skips one whose lease class is held: one place per
# class, LeasePlaces::default(). A skipped unit holds up nothing beside it.
#
# The model leaves out what the order does not read. Leverage is a number each unit carries (the
# code counts the live units that depend on it; the driver gives a unit that many dependents).
# Memory pressure holds a background unit back, which changes who is selectable, never who comes
# first, and the model has no dependencies, so every unit it holds is ready. The coordinator's
# `priority` is not read by `order_cmp` and is not modelled.
#
# Check: bun bend2/main.ts task_server.bend --verdict
# Bound to the code by cowshed-gateway src/task_server/laws.rs, which runs the emitted model
# (src/task_server/laws/model.rs) and `Ledger` on the same inputs. The laws the code refutes are in
# task_server_open.bend.
import Base

# the unit's class: Gate ranks first, Interactive and Background share the rank behind it
type Cls is Data:
  CGate{}
  CInter{}
  CBack{}

# a cache-domain deadline: known ones first, soonest first, an unknown one behind them
type Dl is Data:
  DNone{}
  DAt{at: Nat}

# the lease classes a unit holds while it runs: cowshed-new, image, land-publication; a unit
# that holds land-publication is a land
type Ls is Data:
  Ls{cn: Bool, im: Bool, ld: Bool}

# `adm` is when the unit was admitted (the age bound is measured from it), `entry` its place in
# the admit fold
type U is Data:
  U{id: Nat, cls: Cls, urgent: Bool, lev: Nat, dl: Dl, ls: Ls, adm: Nat, entry: Nat}

# the units holding each lease class
type Hold is Data:
  Hold{hn: +List<Nat>, hi: +List<Nat>, hl: +List<Nat>}

# the queue, the units that run, the holders, the clock, the age bound, the next entry, and the
# units the last pass granted in order
type St is Data:
  St{q: +List<U>, sel: +List<Nat>, hold: Hold, now: Nat, bound: Nat, next: Nat, last: +List<Nat>}

# an admission, time passing, a selection pass, a unit's terminal fact
type Op is Data:
  OAdmit{id: Nat, cls: Cls, urgent: Bool, lev: Nat, dl: Dl, ls: Ls}
  OTick{by: Nat}
  OSelect{}
  ODone{id: Nat}

type Pick is Data:
  PNone{}
  PSome{u: U}

type Ord is Data:
  KLt{}
  KEq{}
  KGt{}

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

def lt_b(a: Nat, b: Nat) -> Bool:
  match a b:
    case 0n 0n:
      False{}
    case 0n 1n+q:
      True{}
    case 1n+p 0n:
      False{}
    case 1n+p 1n+q:
      lt_b(p, q)

def le_b(a: Nat, b: Nat) -> Bool:
  match a b:
    case 0n b0:
      True{}
    case 1n+a1 0n:
      False{}
    case 1n+a1 1n+b1:
      le_b(a1, b1)

def and(a: Bool, b: Bool) -> Bool:
  match a:
    case True{}:
      b
    case False{}:
      False{}

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

def is_nil(xs: +List<Nat>) -> Bool:
  match xs:
    case Nil{}:
      True{}
    case Con{h, t}:
      False{}

def pred(n: Nat) -> Nat:
  match n:
    case 0n:
      0n
    case 1n+p:
      p

# a - b, never below zero (the code's saturating_duration_since)
def monus(b: Nat, +a: Nat) -> Nat:
  match b:
    case 0n:
      a
    case 1n+q:
      pred(monus(q, a))

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

def snoc(xs: +List<Nat>, +y: Nat) -> +List<Nat>:
  match xs:
    case Nil{}:
      [y]
    case Con{h, t}:
      h <> snoc(t, y)

def len(xs: +List<U>) -> Nat:
  match xs:
    case Nil{}:
      0n
    case Con{h, t}:
      1n+len(t)

def id_of(+u: U) -> Nat:
  match u:
    case U{i, c, ur, lv, d, l, a, e}:
      i

def drop_id.if(+h: U, r: +List<U>, b: Bool) -> +List<U>:
  match b:
    case True{}:
      r
    case False{}:
      h <> r

def drop_id(+x: Nat, q: +List<U>) -> +List<U>:
  match q:
    case Nil{}:
      []
    case Con{+h, t}:
      drop_id.if(h, drop_id(x, t), same(x, id_of(h)))

def has_id(+x: Nat, q: +List<U>) -> Bool:
  match q:
    case Nil{}:
      False{}
    case Con{h, t}:
      or(same(x, id_of(h)), has_id(x, t))

def mem(+x: Nat, xs: +List<Nat>) -> Bool:
  match xs:
    case Nil{}:
      False{}
    case Con{h, t}:
      or(same(x, h), mem(x, t))

# --- the unit's facts -------------------------------------------------------

def u_aged(+now: Nat, +bound: Nat, u: U) -> Bool:
  match u:
    case U{i, c, ur, lv, d, l, a, e}:
      le_b(bound, monus(a, now))

def u_land(u: U) -> Bool:
  match u:
    case U{i, c, ur, lv, d, Ls{cn, im, ld}, a, e}:
      ld

def u_gate(u: U) -> Bool:
  match u:
    case U{i, CGate{}, ur, lv, d, l, a, e}:
      True{}
    case U{i, CInter{}, ur, lv, d, l, a, e}:
      False{}
    case U{i, CBack{}, ur, lv, d, l, a, e}:
      False{}

# tier 0: aged land, 1: fresh land, 2: aged non-land,
# 3: fresh non-land Gate, 4: fresh non-land Interactive or Background
def tier.of(aged: Bool, land: Bool, gate: Bool) -> Nat:
  match aged land gate:
    case True{} True{} g:
      0n
    case False{} True{} g:
      1n
    case True{} False{} g:
      2n
    case False{} False{} True{}:
      3n
    case False{} False{} False{}:
      4n

def tier(+now: Nat, +bound: Nat, +u: U) -> Nat:
  tier.of(u_aged(now, bound, u), u_land(u), u_gate(u))

# --- the total order (cowshed-gateway task_server.rs `order_cmp`) ---------------

def cmp(a: Nat, b: Nat) -> Ord:
  match a b:
    case 0n 0n:
      KEq{}
    case 0n 1n+q:
      KLt{}
    case 1n+p 0n:
      KGt{}
    case 1n+p 1n+q:
      cmp(p, q)

def then(o: Ord, p: Ord) -> Ord:
  match o:
    case KEq{}:
      p
    case KLt{}:
      KLt{}
    case KGt{}:
      KGt{}

def is_lt(o: Ord) -> Bool:
  match o:
    case KLt{}:
      True{}
    case KEq{}:
      False{}
    case KGt{}:
      False{}

# `b.urgent.cmp(&a.urgent)`: an urgent unit sorts before one that is not
def cmp_urgent(a: Bool, b: Bool) -> Ord:
  match a b:
    case True{} True{}:
      KEq{}
    case True{} False{}:
      KLt{}
    case False{} True{}:
      KGt{}
    case False{} False{}:
      KEq{}

# known deadlines first, soonest first; an unknown one sorts behind
def cmp_dl(a: Dl, b: Dl) -> Ord:
  match a b:
    case DAt{x} DAt{y}:
      cmp(x, y)
    case DAt{x} DNone{}:
      KLt{}
    case DNone{} DAt{y}:
      KGt{}
    case DNone{} DNone{}:
      KEq{}

def fresh_key(a: U, b: U) -> Ord:
  match a b:
    case U{ia, ca, ua, la, da, sa, aa, ea} U{ib, cb, ub, lb, db, sb, ab, eb}:
      then(cmp_urgent(ua, ub), then(cmp(lb, la), then(cmp_dl(da, db), cmp(ia, ib))))

# the aged wait in the order they waited
def aged_key(a: U, b: U) -> Ord:
  match a b:
    case U{ia, ca, ua, la, da, sa, aa, ea} U{ib, cb, ub, lb, db, sb, ab, eb}:
      then(cmp(aa, ab), cmp(ia, ib))

# lands queue in admission order
def land_key(a: U, b: U) -> Ord:
  match a b:
    case U{ia, ca, ua, la, da, sa, aa, ea} U{ib, cb, ub, lb, db, sb, ab, eb}:
      cmp(ea, eb)

def lt_in(aged: Bool, land: Bool, a: U, b: U) -> Bool:
  match aged land:
    case g True{}:
      is_lt(land_key(a, b))
    case True{} False{}:
      is_lt(aged_key(a, b))
    case False{} False{}:
      is_lt(fresh_key(a, b))

# (tier, then the tier's own key): tier, a coarse order, decides before the key does
def lt_tt(p: Nat, q: Nat, tie: Bool) -> Bool:
  match p q:
    case 0n 0n:
      tie
    case 0n 1n+q1:
      True{}
    case 1n+p1 0n:
      False{}
    case 1n+p1 1n+q1:
      lt_tt(p1, q1, tie)

def lt(+now: Nat, +bound: Nat, +a: U, +b: U) -> Bool:
  lt_tt(tier(now, bound, a), tier(now, bound, b), lt_in(u_aged(now, bound, a), u_land(a), a, b))

# --- places: one per lease class ------------------------------------------------

def free1(flag: Bool, h: +List<Nat>) -> Bool:
  match flag:
    case False{}:
      True{}
    case True{}:
      is_nil(h)

def grant1(flag: Bool, +i: Nat, h: +List<Nat>) -> +List<Nat>:
  match flag:
    case True{}:
      i <> h
    case False{}:
      h

def fits(u: U, h: Hold) -> Bool:
  match u h:
    case U{i, c, ur, lv, d, Ls{cn, im, ld}, a, e} Hold{hn, hi, hl}:
      and(free1(cn, hn), and(free1(im, hi), free1(ld, hl)))

def grant(u: U, h: Hold) -> Hold:
  match u h:
    case U{+i, c, ur, lv, d, Ls{cn, im, ld}, a, e} Hold{hn, hi, hl}:
      Hold{grant1(cn, i, hn), grant1(im, i, hi), grant1(ld, i, hl)}

def release(+i: Nat, h: Hold) -> Hold:
  match h:
    case Hold{hn, hi, hl}:
      Hold{remove(i, hn), remove(i, hi), remove(i, hl)}

def nothing() -> Hold:
  Hold{[], [], []}

# --- the first selectable unit: the least in the order whose places are free ---------

def pick.lt(x: U, y: U, b: Bool) -> Pick:
  match b:
    case True{}:
      PSome{x}
    case False{}:
      PSome{y}

def pick.y(+now: Nat, +bound: Nat, +x: U, r: Pick) -> Pick:
  match r:
    case PNone{}:
      PSome{x}
    case PSome{+y}:
      pick.lt(x, y, lt(now, bound, x, y))

def pick.x(+now: Nat, +bound: Nat, +x: U, free: Bool, r: Pick) -> Pick:
  match free:
    case True{}:
      pick.y(now, bound, x, r)
    case False{}:
      r

def pick(+q: +List<U>, +h: Hold, +now: Nat, +bound: Nat) -> Pick:
  match q:
    case Nil{}:
      PNone{}
    case Con{+x, t}:
      pick.x(now, bound, x, fits(x, h), pick(t, h, now, bound))

# --- the schedule's inputs ----------------------------------------------------------

def snocu(q: +List<U>, +u: U) -> +List<U>:
  match q:
    case Nil{}:
      [u]
    case Con{h, t}:
      h <> snocu(t, u)

def ids(xs: +List<U>) -> +List<Nat>:
  match xs:
    case Nil{}:
      []
    case Con{h, t}:
      id_of(h) <> ids(t)

def clear(s: St) -> St:
  match s:
    case St{q, sel, hold, now, bound, next, last}:
      St{q, sel, hold, now, bound, next, []}

def pick_st(s: St) -> Pick:
  match s:
    case St{+q, sel, +hold, +now, +bound, next, last}:
      pick(q, hold, now, bound)

def len_st(s: St) -> Nat:
  match s:
    case St{q, sel, hold, now, bound, next, last}:
      len(q)

# a unit that is live is refused: one live attempt per unit
def admit.if(+id: Nat, +cls: Cls, +urg: Bool, +lev: Nat, +dl: Dl, +ls: Ls, q: +List<U>, sel: +List<Nat>, hold: Hold, +now: Nat, bound: Nat, +next: Nat, b: Bool) -> St:
  match b:
    case True{}:
      St{q, sel, hold, now, bound, next, []}
    case False{}:
      St{snocu(q, U{id, cls, urg, lev, dl, ls, now, next}), sel, hold, now, bound, 1n+next, []}

def admit(+id: Nat, cls: Cls, urg: Bool, lev: Nat, dl: Dl, ls: Ls, s: St) -> St:
  match s:
    case St{+q, +sel, hold, now, bound, next, last}:
      admit.if(id, cls, urg, lev, dl, ls, q, sel, hold, now, bound, next, or(has_id(id, q), mem(id, sel)))

def tick(by: Nat, s: St) -> St:
  match s:
    case St{q, sel, hold, +now, bound, next, last}:
      St{q, sel, hold, Nat.add(now, by), bound, next, []}

def done(+id: Nat, s: St) -> St:
  match s:
    case St{q, sel, hold, now, bound, next, last}:
      St{drop_id(id, q), remove(id, sel), release(id, hold), now, bound, next, []}

# the unit takes its places and leaves the queue
def take(+u: U, s: St) -> St:
  match s:
    case St{q, sel, hold, now, bound, next, last}:
      St{drop_id(id_of(u), q), id_of(u) <> sel, grant(u, hold), now, bound, next, snoc(last, id_of(u))}

# every unit whose places are free is selected, the least in the order first:
# `fuel` bounds the picks by the units that were waiting
def select(fuel: Nat, +s: St, p: Pick) -> St:
  match fuel:
    case 0n:
      s
    case 1n+f:
      match p:
        case PNone{}:
          s
        case PSome{+u}:
          select(f, take(u, s), pick_st(take(u, s)))

def pass(+s: St) -> St:
  select(len_st(s), clear(s), pick_st(s))

def step(o: Op, s: St) -> St:
  match o:
    case OAdmit{id, cls, urg, lev, dl, ls}:
      admit(id, cls, urg, lev, dl, ls, s)
    case OTick{by}:
      tick(by, s)
    case OSelect{}:
      pass(s)
    case ODone{id}:
      done(id, s)

def run(os: +List<Op>, s: St) -> St:
  match os:
    case Nil{}:
      s
    case Con{o, t}:
      run(t, step(o, s))

def empty(bound: Nat) -> St:
  St{[], [], nothing(), 0n, bound, 0n, []}

# the ready order, in the order the schedule would grant it with every place free
def order(fuel: Nat, +q: +List<U>, +now: Nat, +bound: Nat, p: Pick) -> +List<U>:
  match fuel:
    case 0n:
      []
    case 1n+f:
      match p:
        case PNone{}:
          []
        case PSome{+u}:
          u <> order(f, drop_id(id_of(u), q), now, bound, pick(drop_id(id_of(u), q), nothing(), now, bound))

def ready(+s: St) -> +List<Nat>:
  match s:
    case St{+q, sel, hold, +now, +bound, next, last}:
      ids(order(len(q), q, now, bound, pick(q, nothing(), now, bound)))

def queue(s: St) -> +List<U>:
  match s:
    case St{q, sel, hold, now, bound, next, last}:
      q

def granted(s: St) -> +List<Nat>:
  match s:
    case St{q, sel, hold, now, bound, next, last}:
      last

# the units that run: selected and not done
def selected(s: St) -> +List<Nat>:
  match s:
    case St{q, sel, hold, now, bound, next, last}:
      sel

# --- order facts on Nat ---------------------------------------------------------------

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

def absurd(-A: Type, e: Empty) -> A:
  match e:

def IsT(b: Bool) -> Data:
  match b:
    case True{}:
      Unit
    case False{}:
      Empty

# what lt_tt answered says how the tiers compare
def LtCase(b: Bool, +p: Nat, +q: Nat) -> Data:
  match b:
    case True{}:
      LE(p, q)
    case False{}:
      LE(q, p)

def lt_tt_case.s(b: Bool, +p: Nat, +q: Nat, ih: LtCase(b, p, q)) -> LtCase(b, 1n+p, 1n+q):
  match b:
    case True{}:
      ih
    case False{}:
      ih

def lt_tt_case(+p: Nat, +q: Nat, +tie: Bool) -> LtCase(lt_tt(p, q, tie), p, q):
  match p q:
    case 0n 0n:
      match tie:
        case True{}:
          Unit{}
        case False{}:
          Unit{}
    case 0n 1n+q1:
      Unit{}
    case 1n+p1 0n:
      Unit{}
    case 1n+p1 1n+q1:
      lt_tt_case.s(lt_tt(p1, q1, tie), p1, q1, lt_tt_case(p1, q1, tie))

# --- the first selectable unit has the least tier of all the selectable --------------------

# every unit of the queue that fits has a tier of at least k
def LowerC(b: Bool, +k: Nat, +n: Nat, R: Data) -> Data:
  match b:
    case True{}:
      Sigma<&2, &2, LE(k, n), u => R>
    case False{}:
      R

def Lower(+q: +List<U>, +h: Hold, +now: Nat, +bound: Nat, +k: Nat) -> Data:
  match q:
    case Nil{}:
      Unit
    case Con{+x, t}:
      LowerC(fits(x, h), k, tier(now, bound, x), Lower(t, h, now, bound, k))

# no unit of the queue fits
def NoFreeC(b: Bool, R: Data) -> Data:
  match b:
    case True{}:
      Empty
    case False{}:
      R

def NoFree(+q: +List<U>, +h: Hold) -> Data:
  match q:
    case Nil{}:
      Unit
    case Con{x, t}:
      NoFreeC(fits(x, h), NoFree(t, h))

def PickOk(p: Pick, +q: +List<U>, +h: Hold, +now: Nat, +bound: Nat) -> Data:
  match p:
    case PNone{}:
      NoFree(q, h)
    case PSome{+c}:
      Sigma<&2, &2, IsT(fits(c, h)), u => Lower(q, h, now, bound, tier(now, bound, c))>

def nofree_lower.c(b: Bool, +k: Nat, +n: Nat, -R1: Data, -R2: Data, nf: NoFreeC(b, R1), ih: R1 -> R2) -> LowerC(b, k, n, R2):
  match b:
    case True{}:
      match nf:
    case False{}:
      ih(nf)

def nofree_lower(+q: +List<U>, +h: Hold, +now: Nat, +bound: Nat, +k: Nat, nf: NoFree(q, h)) -> Lower(q, h, now, bound, k):
  match q:
    case Nil{}:
      Unit{}
    case Con{+x, t}:
      nofree_lower.c(fits(x, h), k, tier(now, bound, x), NoFree(t, h), Lower(t, h, now, bound, k), nf, w => nofree_lower(t, h, now, bound, k, w))

def lower_weaken.c(b: Bool, +k1: Nat, +k2: Nat, +n: Nat, -R1: Data, -R2: Data, le: LE(k1, k2), lw: LowerC(b, k2, n, R2), ih: R2 -> R1) -> LowerC(b, k1, n, R1):
  match b:
    case True{}:
      (lw0, lw1) = lw
      (le_trans(k1, k2, n, le, lw0), ih(lw1))
    case False{}:
      ih(lw)

def lower_weaken(+q: +List<U>, +h: Hold, +now: Nat, +bound: Nat, +k1: Nat, +k2: Nat, +le: LE(k1, k2), lw: Lower(q, h, now, bound, k2)) -> Lower(q, h, now, bound, k1):
  match q:
    case Nil{}:
      Unit{}
    case Con{+x, t}:
      lower_weaken.c(fits(x, h), k1, k2, tier(now, bound, x), Lower(t, h, now, bound, k1), Lower(t, h, now, bound, k2), le, lw, w => lower_weaken(t, h, now, bound, k1, k2, le, w))

def pick_ok.lt(b: Bool, +x: U, +y: U, +t: +List<U>, +h: Hold, +now: Nat, +bound: Nat, lc: LtCase(b, tier(now, bound, x), tier(now, bound, y)), fx: {fits(x, h) == True{} : Bool}, ih: PickOk(PSome{y}, t, h, now, bound)) -> PickOk(pick.lt(x, y, b), Con{x, t}, h, now, bound):
  match b:
    case True{}:
      (ihf, lw) = ih
      %Equal.sym(Bool, fits(x, h), True{}, fx) : Sigma<&2, &2, IsT(_), u => LowerC(_, tier(now, bound, x), tier(now, bound, x), Lower(t, h, now, bound, tier(now, bound, x)))>
      (Unit{}, (le_refl(tier(now, bound, x)), lower_weaken(t, h, now, bound, tier(now, bound, x), tier(now, bound, y), lc, lw)))
    case False{}:
      (ihf, lw) = ih
      %Equal.sym(Bool, fits(x, h), True{}, fx) : Sigma<&2, &2, IsT(fits(y, h)), u => LowerC(_, tier(now, bound, y), tier(now, bound, x), Lower(t, h, now, bound, tier(now, bound, y)))>
      (ihf, (lc, lw))

def pick_ok.y(+x: U, +r: Pick, +t: +List<U>, +h: Hold, +now: Nat, +bound: Nat, fx: {fits(x, h) == True{} : Bool}, ih: PickOk(r, t, h, now, bound)) -> PickOk(pick.y(now, bound, x, r), Con{x, t}, h, now, bound):
  match r:
    case PNone{}:
      %Equal.sym(Bool, fits(x, h), True{}, fx) : Sigma<&2, &2, IsT(_), u => LowerC(_, tier(now, bound, x), tier(now, bound, x), Lower(t, h, now, bound, tier(now, bound, x)))>
      (Unit{}, (le_refl(tier(now, bound, x)), nofree_lower(t, h, now, bound, tier(now, bound, x), ih)))
    case PSome{+y}:
      pick_ok.lt(lt(now, bound, x, y), x, y, t, h, now, bound, lt_tt_case(tier(now, bound, x), tier(now, bound, y), lt_in(u_aged(now, bound, x), u_land(x), x, y)), fx, ih)

def pick_ok.x(b: Bool, +x: U, +r: Pick, +t: +List<U>, +h: Hold, +now: Nat, +bound: Nat, fx: {fits(x, h) == b : Bool}, ih: PickOk(r, t, h, now, bound)) -> PickOk(pick.x(now, bound, x, b, r), Con{x, t}, h, now, bound):
  match b:
    case True{}:
      pick_ok.y(x, r, t, h, now, bound, fx, ih)
    case False{}:
      match r:
        case PNone{}:
          %Equal.sym(Bool, fits(x, h), False{}, fx) : NoFreeC(_, NoFree(t, h))
          ih
        case PSome{c}:
          %Equal.sym(Bool, fits(x, h), False{}, fx) : Sigma<&2, &2, IsT(fits(c, h)), u => LowerC(_, tier(now, bound, c), tier(now, bound, x), Lower(t, h, now, bound, tier(now, bound, c)))>
          ih

def pick_ok(+q: +List<U>, +h: Hold, +now: Nat, +bound: Nat) -> PickOk(pick(q, h, now, bound), q, h, now, bound):
  match q:
    case Nil{}:
      Unit{}
    case Con{+x, +t}:
      pick_ok.x(fits(x, h), x, pick(t, h, now, bound), t, h, now, bound, {==}, pick_ok(t, h, now, bound))

# --- the sets the laws speak of ---------------------------------------------------------

type Sel is Data:
  SAged{}
  SFreshGate{}
  SAgedPlain{}

# a unit's age, land and class decide its tier and its set
def in_sel.b(k: Sel, ag: Bool, ld: Bool, gt: Bool) -> Bool:
  match k:
    case SAged{}:
      ag
    case SFreshGate{}:
      and(not(ag), gt)
    case SAgedPlain{}:
      and(ag, not(ld))

def in_sel(k: Sel, +now: Nat, +bound: Nat, +u: U) -> Bool:
  in_sel.b(k, u_aged(now, bound, u), u_land(u), u_gate(u))

# the greatest tier a unit of the set can have
def top(k: Sel) -> Nat:
  match k:
    case SAged{}:
      2n
    case SFreshGate{}:
      3n
    case SAgedPlain{}:
      2n

# some unit of the set fits its places
def free_in(+q: +List<U>, +h: Hold, +now: Nat, +bound: Nat, +k: Sel) -> Bool:
  match q:
    case Nil{}:
      False{}
    case Con{+x, t}:
      or(and(in_sel(k, now, bound, x), fits(x, h)), free_in(t, h, now, bound, k))

def sel_top(k: Sel, ag: Bool, ld: Bool, gt: Bool, m: IsT(in_sel.b(k, ag, ld, gt))) -> LE(tier.of(ag, ld, gt), top(k)):
  match k ag ld gt:
    case SAged{} True{} True{} g:
      Unit{}
    case SAged{} True{} False{} g:
      Unit{}
    case SAged{} False{} l g:
      match m:
    case SFreshGate{} False{} True{} True{}:
      Unit{}
    case SFreshGate{} False{} False{} True{}:
      Unit{}
    case SFreshGate{} False{} l False{}:
      match m:
    case SFreshGate{} True{} l g:
      match m:
    case SAgedPlain{} True{} False{} g:
      Unit{}
    case SAgedPlain{} True{} True{} g:
      match m:
    case SAgedPlain{} False{} l g:
      match m:

def IsF(b: Bool) -> Data:
  match b:
    case True{}:
      Empty
    case False{}:
      Unit

def false_not_true(g: {False{} == True{} : Bool}) -> Empty:
  %g : IsF(_)
  Unit{}

def lt_irrefl(+p: Nat, h: LE(1n+p, p)) -> Empty:
  match p:
    case 0n:
      match h:
    case 1n+p1:
      lt_irrefl(p1, h)

# the tier bound turns "no unit of the queue has a tier below n" into "no unit of the set fits"
def lnf.c(b: Bool, s: Bool, +r: Bool, +tp: Nat, +n: Nat, +m: Nat, -R: Data, gt: LE(1n+tp, n), lw: LowerC(b, n, m, R), sb: IsT(s) -> LE(m, tp), ih: R -> {r == False{} : Bool}) -> {or(and(s, b), r) == False{} : Bool}:
  match b:
    case True{}:
      match s:
        case True{}:
          (lw0, lw1) = lw
          absurd({or(and(True{}, True{}), r) == False{} : Bool}, lt_irrefl(tp, le_trans(1n+tp, n, tp, gt, le_trans(n, m, tp, lw0, sb(Unit{})))))
        case False{}:
          (lw0, lw1) = lw
          ih(lw1)
    case False{}:
      match s:
        case True{}:
          ih(lw)
        case False{}:
          ih(lw)

def lower_no_free(+q: +List<U>, +h: Hold, +now: Nat, +bound: Nat, +k: Sel, +n: Nat, +gt: LE(1n+top(k), n), lw: Lower(q, h, now, bound, n)) -> {free_in(q, h, now, bound, k) == False{} : Bool}:
  match q:
    case Nil{}:
      {==}
    case Con{+x, +t}:
      lnf.c(fits(x, h), in_sel(k, now, bound, x), free_in(t, h, now, bound, k), top(k), n, tier(now, bound, x), Lower(t, h, now, bound, n), gt, lw, m => sel_top(k, u_aged(now, bound, x), u_land(x), u_gate(x), m), w => lower_no_free(t, h, now, bound, k, n, gt, w))

def nffi.c(b: Bool, s: Bool, +r: Bool, -R: Data, nf: NoFreeC(b, R), ih: R -> {r == False{} : Bool}) -> {or(and(s, b), r) == False{} : Bool}:
  match b:
    case True{}:
      match nf:
    case False{}:
      match s:
        case True{}:
          ih(nf)
        case False{}:
          ih(nf)

def nofree_free_in(+q: +List<U>, +h: Hold, +now: Nat, +bound: Nat, +k: Sel, nf: NoFree(q, h)) -> {free_in(q, h, now, bound, k) == False{} : Bool}:
  match q:
    case Nil{}:
      {==}
    case Con{+x, +t}:
      nffi.c(fits(x, h), in_sel(k, now, bound, x), free_in(t, h, now, bound, k), NoFree(t, h), nf, w => nofree_free_in(t, h, now, bound, k, w))

def fit.c(b: Bool, s: Bool, +r: Bool, +tp: Nat, +n: Nat, +m: Nat, -R: Data, lw: LowerC(b, n, m, R), sb: IsT(s) -> LE(m, tp), ih: R -> ({r == True{} : Bool} -> LE(n, tp)), fi: {or(and(s, b), r) == True{} : Bool}) -> LE(n, tp):
  match b:
    case True{}:
      match s:
        case True{}:
          (lw0, lw1) = lw
          le_trans(n, m, tp, lw0, sb(Unit{}))
        case False{}:
          (lw0, lw1) = lw
          ih(lw1)(fi)
    case False{}:
      match s:
        case True{}:
          ih(lw)(fi)
        case False{}:
          ih(lw)(fi)

def free_in_top(+q: +List<U>, +h: Hold, +now: Nat, +bound: Nat, +k: Sel, +n: Nat, lw: Lower(q, h, now, bound, n), fi: {free_in(q, h, now, bound, k) == True{} : Bool}) -> LE(n, top(k)):
  match q:
    case Nil{}:
      absurd(LE(n, top(k)), false_not_true(fi))
    case Con{+x, +t}:
      fit.c(fits(x, h), in_sel(k, now, bound, x), free_in(t, h, now, bound, k), top(k), n, tier(now, bound, x), Lower(t, h, now, bound, n), lw, m => sel_top(k, u_aged(now, bound, x), u_land(x), u_gate(x), m), w => (v => free_in_top(t, h, now, bound, k, n, w, v)), fi)

# --- the laws as checks a driver evaluates on the code's own state ---------------------------

# aged first: a fresh non-land pick means no aged unit fits its places
def af.f(ag: Bool, ld: Bool, fr: Bool) -> Bool:
  or(or(ag, ld), not(fr))

def af.p(p: Pick, +q: +List<U>, +h: Hold, +now: Nat, +bound: Nat) -> Bool:
  match p:
    case PNone{}:
      True{}
    case PSome{+f}:
      af.f(u_aged(now, bound, f), u_land(f), free_in(q, h, now, bound, SAged{}))

def aged_first_b(s: St) -> Bool:
  match s:
    case St{+q, sel, +hold, +now, +bound, next, last}:
      af.p(pick(q, hold, now, bound), q, hold, now, bound)

def af.ok(ag: Bool, ld: Bool, gt: Bool, +fr: Bool, nf: LE(3n, tier.of(ag, ld, gt)) -> {fr == False{} : Bool}) -> {af.f(ag, ld, fr) == True{} : Bool}:
  match ag ld gt:
    case True{} l g:
      {==}
    case False{} True{} g:
      {==}
    case False{} False{} True{}:
      %Equal.sym(Bool, fr, False{}, nf(le_refl(3n))) : {af.f(False{}, False{}, _) == True{} : Bool}
      {==}
    case False{} False{} False{}:
      %Equal.sym(Bool, fr, False{}, nf(Unit{})) : {af.f(False{}, False{}, _) == True{} : Bool}
      {==}

def af_ok(+q: +List<U>, +h: Hold, +now: Nat, +bound: Nat, p: Pick, po: PickOk(p, q, h, now, bound)) -> {af.p(p, q, h, now, bound) == True{} : Bool}:
  match p:
    case PNone{}:
      {==}
    case PSome{+f}:
      (pf, lw) = po
      af.ok(u_aged(now, bound, f), u_land(f), u_gate(f), free_in(q, h, now, bound, SAged{}), w => lower_no_free(q, h, now, bound, SAged{}, tier(now, bound, f), w, lw))

# gate priority among the non-aged: a fresh non-land non-Gate pick means no non-aged Gate unit fits
def gf.f(ag: Bool, ld: Bool, gt: Bool, fr: Bool) -> Bool:
  or(or(ag, ld), or(gt, not(fr)))

def gf.p(p: Pick, +q: +List<U>, +h: Hold, +now: Nat, +bound: Nat) -> Bool:
  match p:
    case PNone{}:
      True{}
    case PSome{+f}:
      gf.f(u_aged(now, bound, f), u_land(f), u_gate(f), free_in(q, h, now, bound, SFreshGate{}))

def gate_first_b(s: St) -> Bool:
  match s:
    case St{+q, sel, +hold, +now, +bound, next, last}:
      gf.p(pick(q, hold, now, bound), q, hold, now, bound)

def gf.ok(ag: Bool, ld: Bool, gt: Bool, +fr: Bool, nf: LE(4n, tier.of(ag, ld, gt)) -> {fr == False{} : Bool}) -> {gf.f(ag, ld, gt, fr) == True{} : Bool}:
  match ag ld gt:
    case True{} l g:
      {==}
    case False{} True{} g:
      {==}
    case False{} False{} True{}:
      {==}
    case False{} False{} False{}:
      %Equal.sym(Bool, fr, False{}, nf(le_refl(4n))) : {gf.f(False{}, False{}, False{}, _) == True{} : Bool}
      {==}

def gf_ok(+q: +List<U>, +h: Hold, +now: Nat, +bound: Nat, p: Pick, po: PickOk(p, q, h, now, bound)) -> {gf.p(p, q, h, now, bound) == True{} : Bool}:
  match p:
    case PNone{}:
      {==}
    case PSome{+f}:
      (pf, lw) = po
      gf.ok(u_aged(now, bound, f), u_land(f), u_gate(f), free_in(q, h, now, bound, SFreshGate{}), w => lower_no_free(q, h, now, bound, SFreshGate{}, tier(now, bound, f), w, lw))

# no starvation: a selectable aged non-land unit means a grant, and it goes to an aged unit or a land
def ns.f(fr: Bool, ag: Bool, ld: Bool) -> Bool:
  or(not(fr), or(ag, ld))

def ns.p(p: Pick, fr: Bool, +now: Nat, +bound: Nat) -> Bool:
  match p:
    case PNone{}:
      not(fr)
    case PSome{+f}:
      ns.f(fr, u_aged(now, bound, f), u_land(f))

def next_fair_b(s: St) -> Bool:
  match s:
    case St{+q, sel, +hold, +now, +bound, next, last}:
      ns.p(pick(q, hold, now, bound), free_in(q, hold, now, bound, SAgedPlain{}), now, bound)

def ns.ok(+fr: Bool, ag: Bool, ld: Bool, gt: Bool, bd: {fr == True{} : Bool} -> LE(tier.of(ag, ld, gt), 2n)) -> {ns.f(fr, ag, ld) == True{} : Bool}:
  match fr:
    case False{}:
      {==}
    case True{}:
      match ag ld gt:
        case True{} l g:
          {==}
        case False{} True{} g:
          {==}
        case False{} False{} True{}:
          absurd({ns.f(True{}, False{}, False{}) == True{} : Bool}, bd({==}))
        case False{} False{} False{}:
          absurd({ns.f(True{}, False{}, False{}) == True{} : Bool}, bd({==}))

def ns_ok(+q: +List<U>, +h: Hold, +now: Nat, +bound: Nat, p: Pick, po: PickOk(p, q, h, now, bound)) -> {ns.p(p, free_in(q, h, now, bound, SAgedPlain{}), now, bound) == True{} : Bool}:
  match p:
    case PNone{}:
      %Equal.sym(Bool, free_in(q, h, now, bound, SAgedPlain{}), False{}, nofree_free_in(q, h, now, bound, SAgedPlain{}, po)) : {not(_) == True{} : Bool}
      {==}
    case PSome{+f}:
      (pf, lw) = po
      ns.ok(free_in(q, h, now, bound, SAgedPlain{}), u_aged(now, bound, f), u_land(f), u_gate(f), v => free_in_top(q, h, now, bound, SAgedPlain{}, tier(now, bound, f), lw, v))

def aged_first_at(+s: St) -> {aged_first_b(s) == True{} : Bool}:
  match s:
    case St{+q, sel, +hold, +now, +bound, next, last}:
      af_ok(q, hold, now, bound, pick(q, hold, now, bound), pick_ok(q, hold, now, bound))

def gate_first_at(+s: St) -> {gate_first_b(s) == True{} : Bool}:
  match s:
    case St{+q, sel, +hold, +now, +bound, next, last}:
      gf_ok(q, hold, now, bound, pick(q, hold, now, bound), pick_ok(q, hold, now, bound))

# --- lease exclusion: at most one holder per lease class ----------------------------------------

def IsNil(xs: +List<Nat>) -> Data:
  match xs:
    case Nil{}:
      Unit
    case Con{h, t}:
      Empty

# one place: a class has no holder or one
def Short(h: +List<Nat>) -> Data:
  match h:
    case Nil{}:
      Unit
    case Con{x, t}:
      IsNil(t)

def InvH(h: Hold) -> Data:
  match h:
    case Hold{hn, hi, hl}:
      Sigma<&2, &2, Short(hn), u => Sigma<&2, &2, Short(hi), v => Short(hl)>>

def Inv(s: St) -> Data:
  match s:
    case St{q, sel, hold, now, bound, next, last}:
      InvH(hold)

def hold_st(s: St) -> Hold:
  match s:
    case St{q, sel, hold, now, bound, next, last}:
      hold

def short_remove.if(+y: Nat, b: Bool) -> Short(remove.if(y, [], b)):
  match b:
    case True{}:
      Unit{}
    case False{}:
      Unit{}

def short_remove(+x: Nat, h: +List<Nat>, sh: Short(h)) -> Short(remove(x, h)):
  match h:
    case Nil{}:
      Unit{}
    case Con{+y, t}:
      match t:
        case Nil{}:
          short_remove.if(y, same(x, y))
        case Con{a, b}:
          match sh:

def and_fst(a: Bool, b: Bool, m: IsT(and(a, b))) -> IsT(a):
  match a:
    case True{}:
      Unit{}
    case False{}:
      match m:

def and_snd(a: Bool, b: Bool, m: IsT(and(a, b))) -> IsT(b):
  match a:
    case True{}:
      m
    case False{}:
      match m:

def short_grant(flag: Bool, +x: Nat, h: +List<Nat>, sh: Short(h), fr: IsT(free1(flag, h))) -> Short(grant1(flag, x, h)):
  match flag:
    case False{}:
      sh
    case True{}:
      match h:
        case Nil{}:
          Unit{}
        case Con{a, b}:
          match fr:

def inv_admit.if(+id: Nat, +cls: Cls, +urg: Bool, +lev: Nat, +dl: Dl, +ls: Ls, q: +List<U>, sel: +List<Nat>, hold: Hold, +now: Nat, bound: Nat, +next: Nat, b: Bool, i: InvH(hold)) -> Inv(admit.if(id, cls, urg, lev, dl, ls, q, sel, hold, now, bound, next, b)):
  match b:
    case True{}:
      i
    case False{}:
      i

def inv_admit(+id: Nat, cls: Cls, urg: Bool, lev: Nat, dl: Dl, ls: Ls, s: St, i: Inv(s)) -> Inv(admit(id, cls, urg, lev, dl, ls, s)):
  match s:
    case St{+q, +sel, hold, now, bound, next, last}:
      inv_admit.if(id, cls, urg, lev, dl, ls, q, sel, hold, now, bound, next, or(has_id(id, q), mem(id, sel)), i)

def inv_tick(by: Nat, s: St, i: Inv(s)) -> Inv(tick(by, s)):
  match s:
    case St{q, sel, hold, now, bound, next, last}:
      i

def inv_done(+id: Nat, s: St, i: Inv(s)) -> Inv(done(id, s)):
  match s:
    case St{q, sel, Hold{hn, hi, hl}, now, bound, next, last}:
      (s1, (s2, s3)) = i
      (short_remove(id, hn, s1), (short_remove(id, hi, s2), short_remove(id, hl, s3)))

def inv_take(u: U, s: St, i: Inv(s), +fu: IsT(fits(u, hold_st(s)))) -> Inv(take(u, s)):
  match u s:
    case U{+x, c, ur, lv, d, Ls{+cn, +im, +ld}, a, e} St{q, sel, Hold{+hn, +hi, +hl}, now, bound, next, last}:
      (s1, (s2, s3)) = i
      (short_grant(cn, x, hn, s1, and_fst(free1(cn, hn), and(free1(im, hi), free1(ld, hl)), fu)), (short_grant(im, x, hi, s2, and_fst(free1(im, hi), free1(ld, hl), and_snd(free1(cn, hn), and(free1(im, hi), free1(ld, hl)), fu))), short_grant(ld, x, hl, s3, and_snd(free1(im, hi), free1(ld, hl), and_snd(free1(cn, hn), and(free1(im, hi), free1(ld, hl)), fu)))))

def PickOkSt(p: Pick, s: St) -> Data:
  match s:
    case St{+q, sel, +hold, +now, +bound, next, last}:
      PickOk(p, q, hold, now, bound)

def pick_ok_st(s: St) -> PickOkSt(pick_st(s), s):
  match s:
    case St{+q, sel, +hold, +now, +bound, next, last}:
      pick_ok(q, hold, now, bound)

def pick_fits(u: U, s: St, po: PickOkSt(PSome{u}, s)) -> IsT(fits(u, hold_st(s))):
  match s:
    case St{q, sel, hold, now, bound, next, last}:
      (f, lw) = po
      f

def inv_select(fuel: Nat, +s: St, p: Pick, i: Inv(s), po: PickOkSt(p, s)) -> Inv(select(fuel, s, p)):
  match fuel:
    case 0n:
      i
    case 1n+f:
      match p:
        case PNone{}:
          i
        case PSome{+u}:
          inv_select(f, take(u, s), pick_st(take(u, s)), inv_take(u, s, i, pick_fits(u, s, po)), pick_ok_st(take(u, s)))

def inv_pass(s: St, i: Inv(s)) -> Inv(pass(s)):
  match s:
    case St{+q, sel, +hold, +now, +bound, next, last}:
      inv_select(len(q), St{q, sel, hold, now, bound, next, []}, pick(q, hold, now, bound), i, pick_ok(q, hold, now, bound))

def inv_step(+o: Op, +s: St, i: Inv(s)) -> Inv(step(o, s)):
  match o:
    case OAdmit{+id, cls, urg, lev, dl, ls}:
      inv_admit(id, cls, urg, lev, dl, ls, s, i)
    case OTick{by}:
      inv_tick(by, s, i)
    case OSelect{}:
      inv_pass(s, i)
    case ODone{+id}:
      inv_done(id, s, i)

def inv_run(+os: +List<Op>, +s: St, i: Inv(s)) -> Inv(run(os, s)):
  match os:
    case Nil{}:
      i
    case Con{o, t}:
      inv_run(t, step(o, s), inv_step(o, s, i))

def inv_empty(bound: Nat) -> Inv(empty(bound)):
  (Unit{}, (Unit{}, Unit{}))

def short_b(h: +List<Nat>) -> Bool:
  match h:
    case Nil{}:
      True{}
    case Con{x, t}:
      is_nil(t)

def holds_ok_b(s: St) -> Bool:
  match s:
    case St{q, sel, Hold{hn, hi, hl}, now, bound, next, last}:
      and(short_b(hn), and(short_b(hi), short_b(hl)))

def short_refl(h: +List<Nat>, sh: Short(h)) -> {short_b(h) == True{} : Bool}:
  match h:
    case Nil{}:
      {==}
    case Con{x, t}:
      match t:
        case Nil{}:
          {==}
        case Con{a, b}:
          match sh:

def holds_refl(s: St, i: Inv(s)) -> {holds_ok_b(s) == True{} : Bool}:
  match s:
    case St{q, sel, Hold{+hn, +hi, +hl}, now, bound, next, last}:
      (s1, (s2, s3)) = i
      %Equal.sym(Bool, short_b(hn), True{}, short_refl(hn, s1)) : {and(_, and(short_b(hi), short_b(hl))) == True{} : Bool}
      %Equal.sym(Bool, short_b(hi), True{}, short_refl(hi, s2)) : {and(_, short_b(hl)) == True{} : Bool}
      %Equal.sym(Bool, short_b(hl), True{}, short_refl(hl, s3)) : {_ == True{} : Bool}
      {==}

# --- the pass is a sequence of picks: no starvation ---------------------------------------------

def now_of(s: St) -> Nat:
  match s:
    case St{q, sel, hold, now, bound, next, last}:
      now

def bound_of(s: St) -> Nat:
  match s:
    case St{q, sel, hold, now, bound, next, last}:
      bound

def sf.step(+s: St, +u: U) -> Bool:
  match s:
    case St{+q, sel, +hold, +now, +bound, next, last}:
      ns.f(free_in(q, hold, now, bound, SAgedPlain{}), u_aged(now, bound, u), u_land(u))

# a pass takes its picks one after another from the state the last one left, `select`'s own
# recursion; the check holds at every one of them
def sf.sel(fuel: Nat, +s: St, p: Pick) -> Bool:
  match fuel:
    case 0n:
      True{}
    case 1n+f:
      match p:
        case PNone{}:
          True{}
        case PSome{+u}:
          and(sf.step(s, u), sf.sel(f, take(u, s), pick_st(take(u, s))))

def no_starve_b(+s: St) -> Bool:
  sf.sel(len_st(s), s, pick_st(s))

def and_true(+a: Bool, +b: Bool, pa: {a == True{} : Bool}, pb: {b == True{} : Bool}) -> {and(a, b) == True{} : Bool}:
  match a:
    case True{}:
      pb
    case False{}:
      absurd({and(False{}, b) == True{} : Bool}, false_not_true(pa))

def step_ok(+s: St, +u: U, po: PickOkSt(PSome{u}, s)) -> {sf.step(s, u) == True{} : Bool}:
  match s:
    case St{+q, sel, +hold, +now, +bound, next, last}:
      (pf, lw) = po
      ns.ok(free_in(q, hold, now, bound, SAgedPlain{}), u_aged(now, bound, u), u_land(u), u_gate(u), v => free_in_top(q, hold, now, bound, SAgedPlain{}, tier(now, bound, u), lw, v))

def sf_ok(+fuel: Nat, +s: St, p: Pick, po: PickOkSt(p, s)) -> {sf.sel(fuel, s, p) == True{} : Bool}:
  match fuel:
    case 0n:
      {==}
    case 1n+f:
      match p:
        case PNone{}:
          {==}
        case PSome{+u}:
          and_true(sf.step(s, u), sf.sel(f, take(u, s), pick_st(take(u, s))), step_ok(s, u, po), sf_ok(f, take(u, s), pick_st(take(u, s)), pick_ok_st(take(u, s))))

def no_starve_ok(+s: St) -> {no_starve_b(s) == True{} : Bool}:
  sf_ok(len_st(s), s, pick_st(s), pick_ok_st(s))

# --- definedness: the laws are not vacuous ----------------------------------------------------------

# a pass grants a unit whose places are free, and a done unit holds no place

def any_fits(+q: +List<U>, +h: Hold) -> Bool:
  match q:
    case Nil{}:
      False{}
    case Con{+x, t}:
      or(fits(x, h), any_fits(t, h))

def NonNil(xs: +List<Nat>) -> Data:
  match xs:
    case Nil{}:
      Empty
    case Con{h, t}:
      Unit

def snoc_nonnil(xs: +List<Nat>, +y: Nat) -> NonNil(snoc(xs, y)):
  match xs:
    case Nil{}:
      Unit{}
    case Con{h, t}:
      Unit{}

def take_nn(+u: U, +s: St) -> NonNil(granted(take(u, s))):
  match s:
    case St{q, sel, hold, now, bound, next, +last}:
      snoc_nonnil(last, id_of(u))

def sel_nn(+fuel: Nat, +s: St, p: Pick, nn: NonNil(granted(s))) -> NonNil(granted(select(fuel, s, p))):
  match fuel:
    case 0n:
      nn
    case 1n+f:
      match p:
        case PNone{}:
          nn
        case PSome{+u}:
          sel_nn(f, take(u, s), pick_st(take(u, s)), take_nn(u, s))

def nnf.c(b: Bool, +r: Bool, -R: Data, nf: NoFreeC(b, R), ai: IsT(or(b, r)), ih: R -> (IsT(r) -> Empty)) -> Empty:
  match b:
    case True{}:
      match nf:
    case False{}:
      ih(nf)(ai)

def nofree_not_any(+q: +List<U>, +h: Hold, nf: NoFree(q, h), ai: IsT(any_fits(q, h))) -> Empty:
  match q:
    case Nil{}:
      match ai:
    case Con{+x, t}:
      nnf.c(fits(x, h), any_fits(t, h), NoFree(t, h), nf, ai, w => (a => nofree_not_any(t, h, w, a)))


def any_fits_st(+s: St) -> Bool:
  match s:
    case St{+q, sel, +hold, now, bound, next, last}:
      any_fits(q, hold)

def nofree_not_any_st(+s: St, po: PickOkSt(PNone{}, s), ai: IsT(any_fits_st(s))) -> Empty:
  match s:
    case St{+q, sel, +hold, now, bound, next, last}:
      nofree_not_any(q, hold, po, ai)

def grants.sel(+f: Nat, +s: St, p: Pick, po: PickOkSt(p, s), ai: IsT(any_fits_st(s))) -> NonNil(granted(select(1n+f, clear(s), p))):
  match p:
    case PNone{}:
      absurd(NonNil(granted(select(1n+f, clear(s), PNone{}))), nofree_not_any_st(s, po, ai))
    case PSome{+u}:
      sel_nn(f, take(u, clear(s)), pick_st(take(u, clear(s))), take_nn(u, clear(s)))

def nonnil_isf(xs: +List<Nat>, nn: NonNil(xs)) -> IsF(is_nil(xs)):
  match xs:
    case Nil{}:
      match nn:
    case Con{h, t}:
      Unit{}


# a state with a fitting unit has a first pick, and the pass takes it: `pass` needs a unit to take
def grants.pass.q(+q: +List<U>, sel: +List<Nat>, +hold: Hold, +now: Nat, +bound: Nat, next: Nat, last: +List<Nat>, ai: IsT(any_fits(q, hold))) -> NonNil(granted(pass(St{q, sel, hold, now, bound, next, last}))):
  match q:
    case Nil{}:
      match ai:
    case Con{+x, +t}:
      grants.sel(len(t), St{Con{x, t}, sel, hold, now, bound, next, last}, pick(Con{x, t}, hold, now, bound), pick_ok(Con{x, t}, hold, now, bound), ai)

def grants.pass(+s: St, ai: IsT(any_fits_st(s))) -> NonNil(granted(pass(s))):
  match s:
    case St{q, sel, hold, now, bound, next, last}:
      grants.pass.q(q, sel, hold, now, bound, next, last, ai)

def grants_b(+s: St) -> Bool:
  or(not(any_fits_st(s)), not(is_nil(granted(pass(s)))))

def grants.cases(+a: Bool, +n: Bool, h: IsT(a) -> IsF(n)) -> {or(not(a), not(n)) == True{} : Bool}:
  match a:
    case False{}:
      {==}
    case True{}:
      match n:
        case True{}:
          absurd({or(not(True{}), not(True{})) == True{} : Bool}, h(Unit{}))
        case False{}:
          {==}

def grants_ok(+s: St) -> {grants_b(s) == True{} : Bool}:
  grants.cases(any_fits_st(s), is_nil(granted(pass(s))), ai => nonnil_isf(granted(pass(s)), grants.pass(s, ai)))

# a unit that is done holds no place: `release` takes every one of its places back
def held(+i: Nat, h: Hold) -> Bool:
  match h:
    case Hold{hn, hi, hl}:
      or(mem(i, hn), or(mem(i, hi), mem(i, hl)))

def mem_remove.if(+x: Nat, +h: Nat, +r: +List<Nat>, b: Bool, e: {same(x, h) == b : Bool}, ih: {mem(x, r) == False{} : Bool}) -> {mem(x, remove.if(h, r, b)) == False{} : Bool}:
  match b:
    case True{}:
      ih
    case False{}:
      %Equal.sym(Bool, same(x, h), False{}, e) : {or(_, mem(x, r)) == False{} : Bool}
      ih

def mem_remove(+x: Nat, +l: +List<Nat>) -> {mem(x, remove(x, l)) == False{} : Bool}:
  match l:
    case Nil{}:
      {==}
    case Con{+h, +t}:
      mem_remove.if(x, h, remove(x, t), same(x, h), {==}, mem_remove(x, t))

def or_false(+a: Bool, +b: Bool, pa: {a == False{} : Bool}, pb: {b == False{} : Bool}) -> {or(a, b) == False{} : Bool}:
  match a:
    case True{}:
      absurd({or(True{}, b) == False{} : Bool}, false_not_true(Equal.sym(Bool, True{}, False{}, pa)))
    case False{}:
      pb

def hold_of(s: St) -> Hold:
  match s:
    case St{q, sel, hold, now, bound, next, last}:
      hold

def done_frees_at(+i: Nat, +h: Hold) -> {held(i, release(i, h)) == False{} : Bool}:
  match h:
    case Hold{+hn, +hi, +hl}:
      or_false(mem(i, remove(i, hn)), or(mem(i, remove(i, hi)), mem(i, remove(i, hl))), mem_remove(i, hn), or_false(mem(i, remove(i, hi)), mem(i, remove(i, hl)), mem_remove(i, hi), mem_remove(i, hl)))

# --- the wire's class: TaskAdmitIn (cowshed-gateway control.rs) -----------------------------------

type Wire is Data:
  WOmit{}
  WClass{cls: Cls}

# the class a request is admitted with: an omitted class takes `dflt`, which the code
# makes TaskClass::default(); a class the request names is kept
def wire_class(dflt: Cls, w: Wire) -> Cls:
  match w:
    case WOmit{}:
      dflt
    case WClass{c}:
      c

# the class the spec gives a request (05_gateway.md "Classes": ordinary commands default to background)
def spec_class(w: Wire) -> Cls:
  match w:
    case WOmit{}:
      CBack{}
    case WClass{c}:
      c

# --- the order as the spec states it for lands (05_gateway.md "The task server schedules work units") ---

# the units in the order the schedule would grant them with every place free
def ready_units(+s: St) -> +List<U>:
  match s:
    case St{+q, sel, hold, +now, +bound, next, last}:
      order(len(q), q, now, bound, pick(q, nothing(), now, bound))

def adm_of(u: U) -> Nat:
  match u:
    case U{i, c, ur, lv, d, l, a, e}:
      a

# some unit of `ys` waited the age bound and was admitted before `x`
def older_aged(+x: U, +now: Nat, +bound: Nat, ys: +List<U>) -> Bool:
  match ys:
    case Nil{}:
      False{}
    case Con{+y, t}:
      or(and(u_aged(now, bound, y), lt_b(adm_of(y), adm_of(x))), older_aged(x, now, bound, t))

# no land is ordered ahead of an aged unit older than it (05:2060-2061, 2063: lands queue behind
# older aged waiters; the aged wait in the order they waited)
def lands_behind.go(+ys: +List<U>, +now: Nat, +bound: Nat) -> Bool:
  match ys:
    case Nil{}:
      True{}
    case Con{+x, +t}:
      and(not(and(u_land(x), older_aged(x, now, bound, t))), lands_behind.go(t, now, bound))

def lands_behind_b(+s: St) -> Bool:
  lands_behind.go(ready_units(s), now_of(s), bound_of(s))

# --- the laws -------------------------------------------------------------------------------------------

# (a) One place per lease class: after any inputs no lease class has more than one holder.
law lease_exclusion:
  for +os: +List<Op>
  for +b: Nat
  {holds_ok_b(run(os, empty(b))) == True{} : Bool}

# (b) Aged first: the unit the next selection takes, when it is fresh and no land, leaves no aged
# unit with its places free. (A fresh land goes first: 05:2060-2061 says it queues behind older aged
# waiters, and order_cmp ranks it ahead of them, the open law lands_behind_older_aged.)
law aged_first:
  for +os: +List<Op>
  for +b: Nat
  {aged_first_b(run(os, empty(b))) == True{} : Bool}

# (c) Class order among the fresh: the unit the next selection takes, when it is no aged unit and
# no land, is a Gate unit whenever a non-aged Gate unit has its places free.
law gate_first:
  for +os: +List<Op>
  for +b: Nat
  {gate_first_b(run(os, empty(b))) == True{} : Bool}

# (d) No starvation: in every pass, each grant made while an aged non-land unit has its places free
# goes to an aged unit or a land, never to a fresh unit.
law no_starve:
  for +os: +List<Op>
  for +b: Nat
  {no_starve_b(run(os, empty(b))) == True{} : Bool}

# Definedness: a pass grants a unit whose places are free, so (a)-(d) are not vacuous...
law grants_when_free:
  for +os: +List<Op>
  for +b: Nat
  {grants_b(run(os, empty(b))) == True{} : Bool}

# ... and a done unit holds no place, so a place is freed for the next waiter.
law done_frees:
  for +os: +List<Op>
  for +b: Nat
  for +i: Nat
  {held(i, hold_of(done(i, run(os, empty(b))))) == False{} : Bool}

# A class the request names is kept; the spec's default is background (the code's is the open law
# omitted_class_is_background in task_server_open.bend).
law named_class_is_kept:
  for d: Cls
  for +c: Cls
  {wire_class(d, WClass{c}) == c : Cls}

law repaired_default_is_background:
  for +w: Wire
  {wire_class(CBack{}, w) == spec_class(w) : Cls}

def lease_exclusion(os, b):
  holds_refl(run(os, empty(b)), inv_run(os, empty(b), inv_empty(b)))

def aged_first(os, b):
  aged_first_at(run(os, empty(b)))

def gate_first(os, b):
  gate_first_at(run(os, empty(b)))

def no_starve(os, b):
  no_starve_ok(run(os, empty(b)))

def grants_when_free(os, b):
  grants_ok(run(os, empty(b)))

def done_hold(+i: Nat, +s: St) -> {hold_of(done(i, s)) == release(i, hold_of(s)) : Hold}:
  match s:
    case St{q, sel, hold, now, bound, next, last}:
      {==}

def done_frees(os, b, i):
  %Equal.sym(Hold, hold_of(done(i, run(os, empty(b)))), release(i, hold_of(run(os, empty(b)))), done_hold(i, run(os, empty(b)))) : {held(i, _) == False{} : Bool}
  done_frees_at(i, hold_of(run(os, empty(b))))

def named_class_is_kept(d, c):
  {==}

def repaired_default_is_background(w):
  match w:
    case WOmit{}:
      {==}
    case WClass{c}:
      {==}