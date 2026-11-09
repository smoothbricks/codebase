# The mbx object store: eviction against publish (specs/cowshed/03_caches.md "mbx").
#
# A blob has bytes, the tick it was created and the tick it was last used, and whether a live
# checkout roots it. A result names blobs. A producer stores each blob and then publishes the
# result that names them (`StoreBlob`, `Publish`, agent.rs:2294, local.rs:369); a reader finds a
# result and the blobs it names (`Use`, agent.rs:2389, materialize.rs:61); a sweep evicts blobs
# by (rooted, used) ascending until the bytes are within the budget (`SweepObjects`,
# mbx-cache-store lib.rs:2151, :3139) and then drops the results that name a missing blob
# (`SweepResults`, lib.rs:2192).
#
# Three switches say what the store does, and each deployed store is one setting of them:
#   touch   a use writes the access time collection ranks by (agent.rs:518 `record_use`).
#           Off is a build without `record_use` (upstream 8add551): the volume is noatime, so eviction
#           reads the creation time. cowshed's pin, 8cd8f6b (packages/cowshed/nix/mbx/flake.nix), has it.
#   lease   a producer's blobs are protected from the sweep from their store to their publish: the
#           sweep's unlink of a held blob answers `Removal::Blocked`, which `evict_objects` already
#           skips without crossing into rooted blobs (lib.rs:3149-3158).
#   atomic  a sweep drops the results that name an evicted blob before it unlinks the blob, so no
#           reader finds a result whose blob is gone.
# cowshed's pin, fork 8cd8f6b, is (on, off, off); a build without record_use is (off, off, off); the
# protocol this model proves is (on, on, on).
#
# Latency is counted by two fields of the state: `redo`, the publishes a sweep made the store
# refuse (each is a compile that runs twice), and `dang`, the lookups that found a result naming a
# blob that was gone (each is a miss and a compile).
#
# Check: bun bend2/main.ts mbx_store.bend --verdict
# The laws the pinned store does not satisfy are open claims in mbx_store_open.bend; each is
# refuted here by a trace (`t_swept`, `t_inflight`, `t_window`) the checker evaluates. `t_lru`
# refutes lru for a build without record_use, the regression record_use guards against.
# Bound to the code by the mbx fork's mbx-cache-store src/store_laws.rs, which runs the emitted
# model (src/store_laws/model.rs) and `evict_objects` on the same inputs.
import Base

type Blob is Data:
  Blob{id: Nat, bytes: Nat, created: Nat, used: Nat, rooted: Bool}

type Res is Data:
  Res{id: Nat, names: +List<Nat>}

type Pol is Data:
  Pol{touch: Bool, lease: Bool, atomic: Bool}

type Op is Data:
  StoreBlob{id: Nat, bytes: Nat}
  Publish{id: Nat, names: +List<Nat>}
  Use{id: Nat}
  SweepObjects{budget: Nat}
  SweepResults{}
  Root{id: Nat, on: Bool}
  Abandon{id: Nat}

type St is Data:
  St{now: Nat, bs: +List<Blob>, rs: +List<Res>, fl: +List<Nat>, dang: Nat, redo: Nat}

type Pk is Data:
  PNone{}
  PSome{v: Blob, rest: +List<Blob>}

type Swept is Data:
  Swept{kept: +List<Blob>, ev: +List<Blob>}

type Sp is Data:
  Sp{hb: +List<Blob>, eb: +List<Blob>}

type Fd is Data:
  FNone{}
  FSome{names: +List<Nat>}

# --- Bool, Nat and list helpers ---------------------------------------------------

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

def le_b(a: Nat, b: Nat) -> Bool:
  match a b:
    case 0n b0:
      True{}
    case 1n+a1 0n:
      False{}
    case 1n+a1 1n+b1:
      le_b(a1, b1)

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

def remove_one.if(h: Nat, r: +List<Nat>, t: +List<Nat>, b: Bool) -> +List<Nat>:
  match b:
    case True{}:
      t
    case False{}:
      h <> r

# the first occurrence goes: one producer's lease is released
def remove_one(+x: Nat, xs: +List<Nat>) -> +List<Nat>:
  match xs:
    case Nil{}:
      []
    case Con{+h, +t}:
      remove_one.if(h, remove_one(x, t), t, same(x, h))

def release(names: +List<Nat>, fl: +List<Nat>) -> +List<Nat>:
  match names:
    case Nil{}:
      fl
    case Con{n, t}:
      release(t, remove_one(n, fl))

def len(xs: +List<Blob>) -> Nat:
  match xs:
    case Nil{}:
      0n
    case Con{h, t}:
      1n+len(t)

def total(xs: +List<Blob>) -> Nat:
  match xs:
    case Nil{}:
      0n
    case Con{Blob{i, y, c, u, k}, t}:
      Nat.add(y, total(t))

# --- blobs ------------------------------------------------------------------------

def bid(b: Blob) -> Nat:
  match b:
    case Blob{i, y, c, u, k}:
      i

def brooted(b: Blob) -> Bool:
  match b:
    case Blob{i, y, c, u, k}:
      k

def bused(b: Blob) -> Nat:
  match b:
    case Blob{i, y, c, u, k}:
      u

def bcreated(b: Blob) -> Nat:
  match b:
    case Blob{i, y, c, u, k}:
      c

# a blob whose lease some producer holds
def held_b(b: Blob, +h: +List<Nat>) -> Bool:
  mem(bid(b), h)

def ids(bs: +List<Blob>) -> +List<Nat>:
  match bs:
    case Nil{}:
      []
    case Con{b, t}:
      bid(b) <> ids(t)

# the time collection reads off a blob: the access time when something writes it, else the
# modification time, which is when the blob was stored
def ktick(tch: Bool, b: Blob) -> Nat:
  match tch:
    case True{}:
      bused(b)
    case False{}:
      bcreated(b)

# (rooted, time) ascending: unrooted first, each class oldest first
def kle.k(ra: Bool, rb: Bool, ua: Nat, ub: Nat) -> Bool:
  match ra rb:
    case False{} False{}:
      le_b(ua, ub)
    case False{} True{}:
      True{}
    case True{} False{}:
      False{}
    case True{} True{}:
      le_b(ua, ub)

def kle(+tch: Bool, +a: Blob, +b: Blob) -> Bool:
  kle.k(brooted(a), brooted(b), ktick(tch, a), ktick(tch, b))

# --- victim choice: the least key among the blobs no producer holds ---------------------

def app(xs: +List<Blob>, ys: +List<Blob>) -> +List<Blob>:
  match xs:
    case Nil{}:
      ys
    case Con{x, t}:
      x <> app(t, ys)

def hbs.if(+x: Blob, rec: +List<Blob>, b: Bool) -> +List<Blob>:
  match b:
    case True{}:
      x <> rec
    case False{}:
      rec

# the blobs a producer holds, in walk order
def hbs(bs: +List<Blob>, +h: +List<Nat>) -> +List<Blob>:
  match bs:
    case Nil{}:
      []
    case Con{+x, t}:
      hbs.if(x, hbs(t, h), held_b(x, h))

def ebs.if(+x: Blob, rec: +List<Blob>, b: Bool) -> +List<Blob>:
  match b:
    case True{}:
      rec
    case False{}:
      x <> rec

# the blobs no producer holds, in walk order
def ebs(bs: +List<Blob>, +h: +List<Nat>) -> +List<Blob>:
  match bs:
    case Nil{}:
      []
    case Con{+x, t}:
      ebs.if(x, ebs(t, h), held_b(x, h))

def split(+bs: +List<Blob>, +h: +List<Nat>) -> Sp:
  Sp{hbs(bs, h), ebs(bs, h)}

def pick.cmp(+x: Blob, +t: +List<Blob>, +v: Blob, +rest: +List<Blob>, b: Bool) -> Pk:
  match b:
    case True{}:
      PSome{x, t}
    case False{}:
      PSome{v, x <> rest}

# the first of equal keys wins, as the stable sort puts it first (lib.rs:2151)
def pick.go(+x: Blob, +t: +List<Blob>, +tch: Bool, r: Pk) -> Pk:
  match r:
    case PNone{}:
      PSome{x, t}
    case PSome{+v, +rest}:
      pick.cmp(x, t, v, rest, kle(tch, x, v))

def pick(bs: +List<Blob>, +tch: Bool) -> Pk:
  match bs:
    case Nil{}:
      PNone{}
    case Con{+x, +t}:
      pick.go(x, t, tch, pick(t, tch))

def rest_of(+bs: +List<Blob>, pk: Pk) -> +List<Blob>:
  match pk:
    case PNone{}:
      bs
    case PSome{v, rest}:
      rest

# some unrooted blob is held: evict_objects stops at the first rooted one (lib.rs:3149-3158)
def ur_b(bs: +List<Blob>) -> Bool:
  match bs:
    case Nil{}:
      False{}
    case Con{x, t}:
      or(not(brooted(x)), ur_b(t))

def sweep.stop(+bs: +List<Blob>, +v: Blob, r: Swept, b: Bool) -> Swept:
  match r b:
    case Swept{k, e} True{}:
      Swept{bs, []}
    case Swept{k, e} False{}:
      Swept{k, v <> e}

def sweep.go(+bs: +List<Blob>, pk: Pk, r: Swept, wi: Bool, ub: Bool) -> Swept:
  match pk wi:
    case PNone{} True{}:
      Swept{bs, []}
    case PNone{} False{}:
      Swept{bs, []}
    case PSome{v, rest} True{}:
      Swept{bs, []}
    case PSome{+v, rest} False{}:
      sweep.stop(bs, v, r, and(brooted(v), ub))

# evict_objects over the blobs no producer holds `eb`, the held ones `hb` counting toward the
# bytes: take the next victim until the bytes are within the budget; `f` counts the blobs, so
# it never runs out before the loop stops
def sweep(f: Nat, +hb: +List<Blob>, +eb: +List<Blob>, +tch: Bool, +b: Nat) -> Swept:
  match f:
    case 0n:
      Swept{eb, []}
    case 1n+g:
      sweep.go(eb, pick(eb, tch), sweep(g, hb, rest_of(eb, pick(eb, tch)), tch, b), le_b(total(app(hb, eb)), b), ur_b(hb))

def holds(l: Bool, fl: +List<Nat>) -> +List<Nat>:
  match l:
    case True{}:
      fl
    case False{}:
      []

# --- results ----------------------------------------------------------------------

def rid(r: Res) -> Nat:
  match r:
    case Res{i, ns}:
      i

def rnames(r: Res) -> +List<Nat>:
  match r:
    case Res{i, ns}:
      ns

def all_in(names: +List<Nat>, +i: +List<Nat>) -> Bool:
  match names:
    case Nil{}:
      True{}
    case Con{n, t}:
      and(mem(n, i), all_in(t, i))

# every blob a result names is there
def all_present(names: +List<Nat>, bs: +List<Blob>) -> Bool:
  all_in(names, ids(bs))

def drop_dangling.if(+r: Res, rec: +List<Res>, b: Bool) -> +List<Res>:
  match b:
    case True{}:
      r <> rec
    case False{}:
      rec

def drop_dangling(rs: +List<Res>, +bs: +List<Blob>) -> +List<Res>:
  match rs:
    case Nil{}:
      []
    case Con{+r, t}:
      drop_dangling.if(r, drop_dangling(t, bs), all_present(rnames(r), bs))

def put_res.if(+r: Res, h: Res, t: +List<Res>, rec: +List<Res>, b: Bool) -> +List<Res>:
  match b:
    case True{}:
      r <> t
    case False{}:
      h <> rec

def put_res(rs: +List<Res>, +r: Res) -> +List<Res>:
  match rs:
    case Nil{}:
      [r]
    case Con{+h, +t}:
      put_res.if(r, h, t, put_res(t, r), same(rid(h), rid(r)))

def find_res.if(+ns: +List<Nat>, rec: Fd, b: Bool) -> Fd:
  match b:
    case True{}:
      FSome{ns}
    case False{}:
      rec

def find_res(+i: Nat, rs: +List<Res>) -> Fd:
  match rs:
    case Nil{}:
      FNone{}
    case Con{Res{j, ns}, t}:
      find_res.if(ns, find_res(i, t), same(i, j))

# --- what a use, a store and a root change write ------------------------------------

type Ed is Data:
  ETouch{}
  ESet{on: Bool}

def apply(+e: Ed, +now: Nat, b: Blob) -> Blob:
  match e b:
    case ETouch{} Blob{i, y, c, u, k}:
      Blob{i, y, c, now, k}
    case ESet{on} Blob{i, y, c, u, k}:
      Blob{i, y, c, u, on}

def edit.if(+b: Blob, rec: +List<Blob>, e: Ed, now: Nat, m: Bool) -> +List<Blob>:
  match m:
    case True{}:
      apply(e, now, b) <> rec
    case False{}:
      b <> rec

# the blobs named in `sel` take the edit: a use sets the access time, a root change the rooting
def edit(bs: +List<Blob>, +sel: +List<Nat>, +e: Ed, +now: Nat) -> +List<Blob>:
  match bs:
    case Nil{}:
      []
    case Con{+b, t}:
      edit.if(b, edit(t, sel, e, now), e, now, mem(bid(b), sel))

def snoc(bs: +List<Blob>, +b: Blob) -> +List<Blob>:
  match bs:
    case Nil{}:
      [b]
    case Con{h, t}:
      h <> snoc(t, b)

def store.at(+bs: +List<Blob>, +i: Nat, +y: Nat, +now: Nat, pr: Bool) -> +List<Blob>:
  match pr:
    case True{}:
      edit(bs, [i], ETouch{}, now)
    case False{}:
      snoc(bs, Blob{i, y, now, now, False{}})

# a new blob is created and used at once; a stored blob that is there is used again
def store(+bs: +List<Blob>, +i: Nat, +y: Nat, +now: Nat) -> +List<Blob>:
  store.at(bs, i, y, now, mem(i, ids(bs)))

# --- the store ---------------------------------------------------------------------

def ptouch(p: Pol) -> Bool:
  match p:
    case Pol{t, l, a}:
      t

def please(p: Pol) -> Bool:
  match p:
    case Pol{t, l, a}:
      l

def patomic(p: Pol) -> Bool:
  match p:
    case Pol{t, l, a}:
      a

def dang.add(d: Nat, ok: Bool) -> Nat:
  match ok:
    case True{}:
      d
    case False{}:
      1n+d

def publish(+n: Nat, +i: Nat, +names: +List<Nat>, +bs: +List<Blob>, +rs: +List<Res>, +fl: +List<Nat>, d: Nat, w: Nat, ok: Bool) -> St:
  match ok:
    case True{}:
      St{n, bs, put_res(rs, Res{i, names}), release(names, fl), d, w}
    case False{}:
      St{n, bs, rs, release(names, fl), d, 1n+w}

def lookup(+n: Nat, fd: Fd, +bs: +List<Blob>, rs: +List<Res>, fl: +List<Nat>, d: Nat, w: Nat) -> St:
  match fd:
    case FNone{}:
      St{n, bs, rs, fl, d, w}
    case FSome{+names}:
      St{n, edit(bs, names, ETouch{}, n), rs, fl, dang.add(d, all_present(names, bs)), w}

def swept.at(at: Bool, +n: Nat, +k: +List<Blob>, +rs: +List<Res>, fl: +List<Nat>, d: Nat, w: Nat) -> St:
  match at:
    case True{}:
      St{n, k, drop_dangling(rs, k), fl, d, w}
    case False{}:
      St{n, k, rs, fl, d, w}

def swept(at: Bool, +n: Nat, +hb: +List<Blob>, sw: Swept, rs: +List<Res>, fl: +List<Nat>, d: Nat, w: Nat) -> St:
  match sw:
    case Swept{k, ev}:
      swept.at(at, n, app(hb, k), rs, fl, d, w)

def evict(at: Bool, +n: Nat, +tch: Bool, +b: Nat, sp: Sp, rs: +List<Res>, fl: +List<Nat>, d: Nat, w: Nat) -> St:
  match sp:
    case Sp{+hb, +eb}:
      swept(at, n, hb, sweep(len(eb), hb, eb, tch, b), rs, fl, d, w)

def step(+p: Pol, o: Op, s: St) -> St:
  match o s:
    case StoreBlob{+i, +y} St{+n, +bs, rs, +fl, d, w}:
      St{1n+n, store(bs, i, y, 1n+n), rs, i <> fl, d, w}
    case Publish{+i, +names} St{+n, +bs, +rs, +fl, d, w}:
      publish(1n+n, i, names, bs, rs, fl, d, w, all_present(names, bs))
    case Use{+i} St{+n, +bs, +rs, fl, d, w}:
      lookup(1n+n, find_res(i, rs), bs, rs, fl, d, w)
    case SweepObjects{+b} St{+n, +bs, +rs, +fl, d, w}:
      evict(patomic(p), 1n+n, ptouch(p), b, split(bs, holds(please(p), fl)), rs, fl, d, w)
    case SweepResults{} St{+n, +bs, rs, fl, d, w}:
      St{1n+n, bs, drop_dangling(rs, bs), fl, d, w}
    case Root{i, on} St{+n, bs, rs, fl, d, w}:
      St{1n+n, edit(bs, [i], ESet{on}, n), rs, fl, d, w}
    case Abandon{i} St{+n, bs, rs, fl, d, w}:
      St{1n+n, bs, rs, remove_one(i, fl), d, w}

def run(os: +List<Op>, +p: Pol, s: St) -> St:
  match os:
    case Nil{}:
      s
    case Con{o, t}:
      run(t, p, step(p, o, s))

def empty() -> St:
  St{0n, [], [], [], 0n, 0n}

# --- the laws as checks a driver evaluates ---------------------------------------------

def lru_pair(+e: Blob, +s: Blob) -> Bool:
  match e s:
    case Blob{i, y, c, u, k} Blob{j, z, d, v, m}:
      or(or(k, m), le_b(u, v))

def lru_to(+e: Blob, kept: +List<Blob>) -> Bool:
  match kept:
    case Nil{}:
      True{}
    case Con{s, t}:
      and(lru_pair(e, s), lru_to(e, t))

# no evicted unrooted blob was used more recently than a surviving unrooted blob no producer
# holds
def lru_b(ev: +List<Blob>, +kept: +List<Blob>) -> Bool:
  match ev:
    case Nil{}:
      True{}
    case Con{e, t}:
      and(lru_to(e, kept), lru_b(t, kept))

def lru.sw(sw: Swept) -> Bool:
  match sw:
    case Swept{k, e}:
      lru_b(e, k)

def lru.sp(sp: Sp, +tch: Bool, +b: Nat) -> Bool:
  match sp:
    case Sp{+hb, +eb}:
      lru.sw(sweep(len(eb), hb, eb, tch, b))

def lru_after(+p: Pol, +b: Nat, s: St) -> Bool:
  match s:
    case St{n, +bs, rs, +fl, d, w}:
      lru.sp(split(bs, holds(please(p), fl)), ptouch(p), b)

# nothing is left to evict: no blob, or the least is rooted behind a held unrooted one
def stuck.go(pk: Pk, ub: Bool) -> Bool:
  match pk:
    case PNone{}:
      True{}
    case PSome{+v, rest}:
      and(brooted(v), ub)

def stuck_b(+k: +List<Blob>, +ub: Bool, +tch: Bool) -> Bool:
  stuck.go(pick(k, tch), ub)

def done.sw(+hb: +List<Blob>, sw: Swept, +tch: Bool, +b: Nat) -> Bool:
  match sw:
    case Swept{+k, e}:
      or(le_b(total(app(hb, k)), b), stuck_b(k, ur_b(hb), tch))

def done.sp(sp: Sp, +tch: Bool, +b: Nat) -> Bool:
  match sp:
    case Sp{+hb, +eb}:
      done.sw(hb, sweep(len(eb), hb, eb, tch, b), tch, b)

def done_after(+p: Pol, +b: Nat, s: St) -> Bool:
  match s:
    case St{n, +bs, rs, +fl, d, w}:
      done.sp(split(bs, holds(please(p), fl)), ptouch(p), b)

def bs_of(s: St) -> +List<Blob>:
  match s:
    case St{n, bs, rs, fl, d, w}:
      bs

def rs_of(s: St) -> +List<Res>:
  match s:
    case St{n, bs, rs, fl, d, w}:
      rs

def fl_of(s: St) -> +List<Nat>:
  match s:
    case St{n, bs, rs, fl, d, w}:
      fl

def dang_of(s: St) -> Nat:
  match s:
    case St{n, bs, rs, fl, d, w}:
      d

def redo_of(s: St) -> Nat:
  match s:
    case St{n, bs, rs, fl, d, w}:
      w

def inflight_b(s: St) -> Bool:
  match s:
    case St{n, +bs, rs, fl, d, w}:
      all_in(fl, ids(bs))

# a build without record_use (upstream 8add551): nothing writes a use, the volume is noatime
def pin() -> Pol:
  Pol{False{}, False{}, False{}}

# fork HEAD (8cd8f6b): the agent writes the access time of every blob it serves or stores
def head() -> Pol:
  Pol{True{}, False{}, False{}}

# HEAD plus the protocol: producers hold their blobs until they publish, and a sweep drops a
# result in the step that evicts a blob it names
def fixed() -> Pol:
  Pol{True{}, True{}, True{}}


def swk(sw: Swept) -> +List<Blob>:
  match sw:
    case Swept{k, e}:
      k

def swe(sw: Swept) -> +List<Blob>:
  match sw:
    case Swept{k, e}:
      e

# every result names only blobs that are there
def rs_ok(rs: +List<Res>, +x: +List<Nat>) -> Bool:
  match rs:
    case Nil{}:
      True{}
    case Con{r, t}:
      and(all_in(rnames(r), x), rs_ok(t, x))

def fd_ok(fd: Fd, +x: +List<Nat>) -> Bool:
  match fd:
    case FNone{}:
      True{}
    case FSome{ns}:
      all_in(ns, x)

# no reader has found a result naming a blob that is gone, and every result names only blobs that are there
# the ids a sweep evicts, in the order it evicts them: the model's victim choice
def ev_after.sp(sp: Sp, +tch: Bool, +b: Nat) -> +List<Nat>:
  match sp:
    case Sp{+hb, +eb}:
      ids(swe(sweep(len(eb), hb, eb, tch, b)))

def ev_after(+p: Pol, +b: Nat, s: St) -> +List<Nat>:
  match s:
    case St{n, +bs, rs, +fl, d, w}:
      ev_after.sp(split(bs, holds(please(p), fl)), ptouch(p), b)

# ===================================================================================
# proofs
# ===================================================================================

def absurd(-A: Type, e: Empty) -> A:
  match e:

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

def bdisc(b: Bool) -> Data:
  match b:
    case True{}:
      Empty
    case False{}:
      Unit

def ff_tt(g: {False{} == True{} : Bool}) -> Empty:
  %g : bdisc(_)
  Unit{}

def or_r(+a: Bool, +b: Bool, h: {b == True{} : Bool}) -> {or(a, b) == True{} : Bool}:
  match a:
    case True{}:
      {==}
    case False{}:
      h

def or_l(+a: Bool, +b: Bool, h: {a == True{} : Bool}) -> {or(a, b) == True{} : Bool}:
  match a:
    case True{}:
      {==}
    case False{}:
      absurd({or(False{}, b) == True{} : Bool}, ff_tt(h))

def and_t(+a: Bool, +b: Bool, ha: {a == True{} : Bool}, hb: {b == True{} : Bool}) -> {and(a, b) == True{} : Bool}:
  match a:
    case True{}:
      hb
    case False{}:
      absurd({and(False{}, b) == True{} : Bool}, ff_tt(ha))

def istrue(+b: Bool, h: IsT(b)) -> {b == True{} : Bool}:
  match b:
    case True{}:
      {==}
    case False{}:
      match h:

def LE(a: Nat, b: Nat) -> Data:
  match a b:
    case 0n b0:
      Unit
    case 1n+a1 0n:
      Empty
    case 1n+a1 1n+b1:
      LE(a1, b1)

def le_b_refl(+a: Nat) -> IsT(le_b(a, a)):
  match a:
    case 0n:
      Unit{}
    case 1n+p:
      le_b_refl(p)

def le_b_trans(+a: Nat, +b: Nat, +c: Nat, ab: IsT(le_b(a, b)), bc: IsT(le_b(b, c))) -> IsT(le_b(a, c)):
  match a b c:
    case 0n b0 c0:
      Unit{}
    case 1n+a1 0n c0:
      match ab:
    case 1n+a1 1n+b1 0n:
      match bc:
    case 1n+a1 1n+b1 1n+c1:
      le_b_trans(a1, b1, c1, ab, bc)

def le_b_total(+a: Nat, +b: Nat, nf: IsF(le_b(a, b))) -> IsT(le_b(b, a)):
  match a b:
    case 0n b0:
      match nf:
    case 1n+a1 0n:
      Unit{}
    case 1n+a1 1n+b1:
      le_b_total(a1, b1, nf)

# --- the order victims are taken in ------------------------------------------------

def kle.k_refl(+r: Bool, +t: Nat) -> IsT(kle.k(r, r, t, t)):
  match r:
    case False{}:
      le_b_refl(t)
    case True{}:
      le_b_refl(t)

def kle_refl(+tch: Bool, +a: Blob) -> IsT(kle(tch, a, a)):
  kle.k_refl(brooted(a), ktick(tch, a))

def kle.k_trans(+ra: Bool, +rb: Bool, +rc: Bool, +ta: Nat, +tb: Nat, +tc: Nat, ab: IsT(kle.k(ra, rb, ta, tb)), bc: IsT(kle.k(rb, rc, tb, tc))) -> IsT(kle.k(ra, rc, ta, tc)):
  match ra rb rc:
    case False{} False{} False{}:
      le_b_trans(ta, tb, tc, ab, bc)
    case False{} False{} True{}:
      Unit{}
    case False{} True{} False{}:
      match bc:
    case False{} True{} True{}:
      Unit{}
    case True{} False{} rc0:
      match ab:
    case True{} True{} False{}:
      match bc:
    case True{} True{} True{}:
      le_b_trans(ta, tb, tc, ab, bc)

def kle_trans(+tch: Bool, +a: Blob, +b: Blob, +c: Blob, ab: IsT(kle(tch, a, b)), bc: IsT(kle(tch, b, c))) -> IsT(kle(tch, a, c)):
  kle.k_trans(brooted(a), brooted(b), brooted(c), ktick(tch, a), ktick(tch, b), ktick(tch, c), ab, bc)

def kle.k_total(+ra: Bool, +rb: Bool, +ta: Nat, +tb: Nat, nf: IsF(kle.k(ra, rb, ta, tb))) -> IsT(kle.k(rb, ra, tb, ta)):
  match ra rb:
    case False{} False{}:
      le_b_total(ta, tb, nf)
    case False{} True{}:
      match nf:
    case True{} False{}:
      Unit{}
    case True{} True{}:
      le_b_total(ta, tb, nf)

def kle_total(+tch: Bool, +a: Blob, +b: Blob, nf: IsF(kle(tch, a, b))) -> IsT(kle(tch, b, a)):
  kle.k_total(brooted(a), brooted(b), ktick(tch, a), ktick(tch, b), nf)

# --- victim choice -------------------------------------------------------------------

# k is not after any blob of the list
def Ge(+tch: Bool, +k: Blob, bs: +List<Blob>) -> Data:
  match bs:
    case Nil{}:
      Unit
    case Con{x, t}:
      Sigma<&2, &2, IsT(kle(tch, k, x)), u => Ge(tch, k, t)>

def ge_trans(+tch: Bool, +x: Blob, +v: Blob, +t: +List<Blob>, +xv: IsT(kle(tch, x, v)), g: Ge(tch, v, t)) -> Ge(tch, x, t):
  match t:
    case Nil{}:
      Unit{}
    case Con{y, r}:
      (g1, g2) = g
      (kle_trans(tch, x, v, y, xv, g1), ge_trans(tch, x, v, r, xv, g2))

# a pick takes exactly one blob out of the list, or the list is empty
def PLen(bs: +List<Blob>, pk: Pk) -> Data:
  match pk:
    case PNone{}:
      {len(bs) == 0n : Nat}
    case PSome{v, rest}:
      {len(bs) == 1n+len(rest) : Nat}

def pick_len.cmp(+x: Blob, +t: +List<Blob>, +v: Blob, +rest: +List<Blob>, b: Bool, ih: {len(t) == 1n+len(rest) : Nat}) -> PLen(x <> t, pick.cmp(x, t, v, rest, b)):
  match b:
    case True{}:
      {==}
    case False{}:
      %ih : {1n+len(t) == 1n+_ : Nat}
      {==}

def pick_len.go(r: Pk, +x: Blob, +t: +List<Blob>, +tch: Bool, ih: PLen(t, r)) -> PLen(x <> t, pick.go(x, t, tch, r)):
  match r:
    case PNone{}:
      {==}
    case PSome{+v, rest}:
      pick_len.cmp(x, t, v, rest, kle(tch, x, v), ih)

def pick_len(bs: +List<Blob>, +tch: Bool) -> PLen(bs, pick(bs, tch)):
  match bs:
    case Nil{}:
      {==}
    case Con{x, +t}:
      pick_len.go(pick(t, tch), x, t, tch, pick_len(t, tch))

def ist(+b: Bool, g: {True{} == b : Bool}) -> IsT(b):
  %g : IsT(_)
  Unit{}

def isf(+b: Bool, g: {False{} == b : Bool}) -> IsF(b):
  %g : IsF(_)
  Unit{}

def ndisc(n: Nat) -> Data:
  match n:
    case 0n:
      Unit
    case 1n+p:
      Empty

def zdisc(n: Nat) -> Data:
  match n:
    case 0n:
      Empty
    case 1n+p:
      Unit

def zero_ne_succ(+n: Nat, g: {0n == 1n+n : Nat}) -> Empty:
  %g : ndisc(_)
  Unit{}

def succ_ne_zero(+n: Nat, g: {1n+n == 0n : Nat}) -> Empty:
  %g : zdisc(_)
  Unit{}

# the victim is not after any blob of the list
def PMin(tch: Bool, bs: +List<Blob>, pk: Pk) -> Data:
  match pk:
    case PNone{}:
      Unit
    case PSome{v, rest}:
      Ge(tch, v, bs)

def pick_min.cmp(+tch: Bool, +x: Blob, +t: +List<Blob>, +v: Blob, +rest: +List<Blob>, b: Bool, gb: {b == kle(tch, x, v) : Bool}, ih: Ge(tch, v, t)) -> PMin(tch, x <> t, pick.cmp(x, t, v, rest, b)):
  match b:
    case True{}:
      (kle_refl(tch, x), ge_trans(tch, x, v, t, ist(kle(tch, x, v), gb), ih))
    case False{}:
      (kle_total(tch, x, v, isf(kle(tch, x, v), gb)), ih)

def pick_min.go(+t: +List<Blob>, r: Pk, +x: Blob, +tch: Bool, ihl: PLen(t, r), ihm: PMin(tch, t, r)) -> PMin(tch, x <> t, pick.go(x, t, tch, r)):
  match t r:
    case Nil{} PNone{}:
      (kle_refl(tch, x), Unit{})
    case Nil{} PSome{v, rest}:
      absurd(PMin(tch, x <> [], pick.go(x, [], tch, PSome{v, rest})), zero_ne_succ(len(rest), ihl))
    case Con{y, u} PNone{}:
      absurd(PMin(tch, x <> (y <> u), pick.go(x, y <> u, tch, PNone{})), succ_ne_zero(len(u), ihl))
    case Con{y, u} PSome{+v, rest}:
      pick_min.cmp(tch, x, y <> u, v, rest, kle(tch, x, v), {==}, ihm)

def pick_min(bs: +List<Blob>, +tch: Bool) -> PMin(tch, bs, pick(bs, tch)):
  match bs:
    case Nil{}:
      Unit{}
    case Con{x, +t}:
      pick_min.go(t, pick(t, tch), x, tch, pick_len(t, tch), pick_min(t, tch))

# a lower bound of the list is a lower bound of what a pick leaves
def PThr(tch: Bool, c: Blob, pk: Pk) -> Data:
  match pk:
    case PNone{}:
      Unit
    case PSome{v, rest}:
      Ge(tch, c, rest)

def pick_thr.cmp(+tch: Bool, +x: Blob, +t: +List<Blob>, +v: Blob, +rest: +List<Blob>, b: Bool, +c: Blob, cx: IsT(kle(tch, c, x)), ct: Ge(tch, c, t), ih: Ge(tch, c, rest)) -> PThr(tch, c, pick.cmp(x, t, v, rest, b)):
  match b:
    case True{}:
      ct
    case False{}:
      (cx, ih)

def pick_thr.go(r: Pk, +x: Blob, +t: +List<Blob>, +tch: Bool, +c: Blob, cx: IsT(kle(tch, c, x)), ct: Ge(tch, c, t), ih: PThr(tch, c, r)) -> PThr(tch, c, pick.go(x, t, tch, r)):
  match r:
    case PNone{}:
      ct
    case PSome{+v, rest}:
      pick_thr.cmp(tch, x, t, v, rest, kle(tch, x, v), c, cx, ct, ih)

def pick_thr(bs: +List<Blob>, +tch: Bool, +c: Blob, ge: Ge(tch, c, bs)) -> PThr(tch, c, pick(bs, tch)):
  match bs:
    case Nil{}:
      Unit{}
    case Con{x, +t}:
      (cx, +ct) = ge
      pick_thr.go(pick(t, tch), x, t, tch, c, cx, ct, pick_thr(t, tch, c, ct))

# --- (a) the sweep ends within the budget or with nothing left to evict --------------------

def rest_le(+eb: +List<Blob>, pk: Pk, g: Nat, pl: PLen(eb, pk), le: LE(len(eb), 1n+g)) -> LE(len(rest_of(eb, pk)), g):
  match pk:
    case PNone{}:
      %Equal.sym(Nat, len(eb), 0n, pl) : LE(_, g)
      Unit{}
    case PSome{v, rest}:
      %pl : LE(_, 1n+g)
      le

def sweep_done.within(+hb: +List<Blob>, +eb: +List<Blob>, +tch: Bool, +b: Nat, gw: {True{} == le_b(total(app(hb, eb)), b) : Bool}) -> {done.sw(hb, Swept{eb, []}, tch, b) == True{} : Bool}:
  %gw : {or(_, stuck_b(eb, ur_b(hb), tch)) == True{} : Bool}
  {==}

def sweep_done.none(+hb: +List<Blob>, +eb: +List<Blob>, +tch: Bool, +b: Nat, gp: {PNone{} == pick(eb, tch) : Pk}) -> {done.sw(hb, Swept{eb, []}, tch, b) == True{} : Bool}:
  %gp : {or(le_b(total(app(hb, eb)), b), stuck.go(_, ur_b(hb))) == True{} : Bool}
  or_r(le_b(total(app(hb, eb)), b), True{}, {==})

def sweep_done.stop(+hb: +List<Blob>, +eb: +List<Blob>, +tch: Bool, +b: Nat, +v: Blob, rest: +List<Blob>, r: Swept, sb: Bool, gs: {sb == and(brooted(v), ur_b(hb)) : Bool}, gp: {PSome{v, rest} == pick(eb, tch) : Pk}, ih: {done.sw(hb, r, tch, b) == True{} : Bool}) -> {done.sw(hb, sweep.stop(eb, v, r, sb), tch, b) == True{} : Bool}:
  match r sb:
    case Swept{k, e} True{}:
      %gp : {or(le_b(total(app(hb, eb)), b), stuck.go(_, ur_b(hb))) == True{} : Bool}
      %gs : {or(le_b(total(app(hb, eb)), b), _) == True{} : Bool}
      or_r(le_b(total(app(hb, eb)), b), True{}, {==})
    case Swept{k, e} False{}:
      ih

def and_cong(+a: Bool, +x: Bool, y: Bool, g: {x == y : Bool}) -> {and(a, x) == and(a, y) : Bool}:
  %g : {and(a, x) == and(a, _) : Bool}
  {==}

def sweep_done.go(pk: Pk, +hb: +List<Blob>, +eb: +List<Blob>, +tch: Bool, +b: Nat, r: Swept, wi: Bool, +ub: Bool, gp: {pk == pick(eb, tch) : Pk}, gw: {wi == le_b(total(app(hb, eb)), b) : Bool}, gu: {ub == ur_b(hb) : Bool}, ih: {done.sw(hb, r, tch, b) == True{} : Bool}) -> {done.sw(hb, sweep.go(eb, pk, r, wi, ub), tch, b) == True{} : Bool}:
  match pk wi:
    case PNone{} True{}:
      sweep_done.within(hb, eb, tch, b, gw)
    case PNone{} False{}:
      sweep_done.none(hb, eb, tch, b, gp)
    case PSome{v, rest} True{}:
      sweep_done.within(hb, eb, tch, b, gw)
    case PSome{+v, rest} False{}:
      sweep_done.stop(hb, eb, tch, b, v, rest, r, and(brooted(v), ub), and_cong(brooted(v), ub, ur_b(hb), gu), gp, ih)

def sweep_done(+f: Nat, +hb: +List<Blob>, +eb: +List<Blob>, +tch: Bool, +b: Nat, le: LE(len(eb), f)) -> {done.sw(hb, sweep(f, hb, eb, tch, b), tch, b) == True{} : Bool}:
  match f:
    case 0n:
      match eb:
        case Nil{}:
          or_r(le_b(total(app(hb, [])), b), True{}, {==})
        case Con{x, t}:
          match le:
    case 1n+g:
      sweep_done.go(pick(eb, tch), hb, eb, tch, b, sweep(g, hb, rest_of(eb, pick(eb, tch)), tch, b), le_b(total(app(hb, eb)), b), ur_b(hb), {==}, {==}, {==}, sweep_done(g, hb, rest_of(eb, pick(eb, tch)), tch, b, rest_le(eb, pick(eb, tch), g, pick_len(eb, tch), le)))

def le_refl(+n: Nat) -> LE(n, n):
  match n:
    case 0n:
      Unit{}
    case 1n+p:
      le_refl(p)

def budget_sp(sp: Sp, +tch: Bool, +b: Nat) -> {done.sp(sp, tch, b) == True{} : Bool}:
  match sp:
    case Sp{+hb, +eb}:
      sweep_done(len(eb), hb, eb, tch, b, le_refl(len(eb)))

def budget_st(+p: Pol, +b: Nat, s: St) -> {done_after(p, b, s) == True{} : Bool}:
  match s:
    case St{n, +bs, rs, +fl, d, w}:
      budget_sp(split(bs, holds(please(p), fl)), ptouch(p), b)

# --- (b) a sweep takes blobs by their key ---------------------------------------------------

# every evicted blob is not after any blob left
def GeAll(+tch: Bool, ev: +List<Blob>, +kept: +List<Blob>) -> Data:
  match ev:
    case Nil{}:
      Unit
    case Con{e, t}:
      Sigma<&2, &2, Ge(tch, e, kept), u => GeAll(tch, t, kept)>

def thr_rest(+pk: Pk, +eb: +List<Blob>, +tch: Bool, +c: Blob, ge: Ge(tch, c, eb), thr: PThr(tch, c, pk)) -> Ge(tch, c, rest_of(eb, pk)):
  match pk:
    case PNone{}:
      ge
    case PSome{v, rest}:
      thr

def thr_pick(+eb: +List<Blob>, +tch: Bool, +c: Blob, pk: Pk, ge: Ge(tch, c, eb), gp: {pk == pick(eb, tch) : Pk}) -> PThr(tch, c, pk):
  %Equal.sym(Pk, pk, pick(eb, tch), gp) : PThr(tch, c, _)
  pick_thr(eb, tch, c, ge)

def sweep_thr.stop(+eb: +List<Blob>, +v: Blob, r: Swept, sb: Bool, +tch: Bool, +c: Blob, ge: Ge(tch, c, eb), ih: Ge(tch, c, swk(r))) -> Ge(tch, c, swk(sweep.stop(eb, v, r, sb))):
  match r sb:
    case Swept{k, e} True{}:
      ge
    case Swept{k, e} False{}:
      ih

def sweep_thr.go(pk: Pk, +eb: +List<Blob>, +tch: Bool, +c: Blob, r: Swept, wi: Bool, +ub: Bool, ge: Ge(tch, c, eb), ih: Ge(tch, c, swk(r))) -> Ge(tch, c, swk(sweep.go(eb, pk, r, wi, ub))):
  match pk wi:
    case PNone{} True{}:
      ge
    case PNone{} False{}:
      ge
    case PSome{v, rest} True{}:
      ge
    case PSome{+v, rest} False{}:
      sweep_thr.stop(eb, v, r, and(brooted(v), ub), tch, c, ge, ih)

# a lower bound of the blobs left in the sweep stays one of what it leaves
def sweep_thr(+f: Nat, +hb: +List<Blob>, +eb: +List<Blob>, +tch: Bool, +b: Nat, +c: Blob, +ge: Ge(tch, c, eb)) -> Ge(tch, c, swk(sweep(f, hb, eb, tch, b))):
  match f:
    case 0n:
      ge
    case 1n+g:
      sweep_thr.go(pick(eb, tch), eb, tch, c, sweep(g, hb, rest_of(eb, pick(eb, tch)), tch, b), le_b(total(app(hb, eb)), b), ur_b(hb), ge, sweep_thr(g, hb, rest_of(eb, pick(eb, tch)), tch, b, c, thr_rest(pick(eb, tch), eb, tch, c, ge, pick_thr(eb, tch, c, ge))))

def Gk(+tch: Bool, pk: Pk, +kk: +List<Blob>) -> Data:
  match pk:
    case PNone{}:
      Unit
    case PSome{v, rest}:
      Ge(tch, v, kk)

def gk_of(+pk: Pk, +eb: +List<Blob>, +hb: +List<Blob>, +tch: Bool, +b: Nat, +g: Nat, mn: PMin(tch, eb, pk), gp: {pk == pick(eb, tch) : Pk}) -> Gk(tch, pk, swk(sweep(g, hb, rest_of(eb, pk), tch, b))):
  match pk:
    case PNone{}:
      Unit{}
    case PSome{+v, rest}:
      sweep_thr(g, hb, rest, tch, b, v, thr_pick(eb, tch, v, PSome{v, rest}, mn, gp))

def sweep_pairs.stop(+eb: +List<Blob>, +v: Blob, r: Swept, sb: Bool, +tch: Bool, gv: Ge(tch, v, swk(r)), ih: GeAll(tch, swe(r), swk(r))) -> GeAll(tch, swe(sweep.stop(eb, v, r, sb)), swk(sweep.stop(eb, v, r, sb))):
  match r sb:
    case Swept{k, e} True{}:
      Unit{}
    case Swept{k, e} False{}:
      (gv, ih)

def sweep_pairs.go(pk: Pk, +eb: +List<Blob>, +tch: Bool, r: Swept, wi: Bool, +ub: Bool, gk: Gk(tch, pk, swk(r)), ih: GeAll(tch, swe(r), swk(r))) -> GeAll(tch, swe(sweep.go(eb, pk, r, wi, ub)), swk(sweep.go(eb, pk, r, wi, ub))):
  match pk wi:
    case PNone{} True{}:
      Unit{}
    case PNone{} False{}:
      Unit{}
    case PSome{v, rest} True{}:
      Unit{}
    case PSome{+v, rest} False{}:
      sweep_pairs.stop(eb, v, r, and(brooted(v), ub), tch, gk, ih)

# every blob a sweep evicts is not after any blob it leaves
def sweep_pairs(+f: Nat, +hb: +List<Blob>, +eb: +List<Blob>, +tch: Bool, +b: Nat) -> GeAll(tch, swe(sweep(f, hb, eb, tch, b)), swk(sweep(f, hb, eb, tch, b))):
  match f:
    case 0n:
      Unit{}
    case 1n+g:
      sweep_pairs.go(pick(eb, tch), eb, tch, sweep(g, hb, rest_of(eb, pick(eb, tch)), tch, b), le_b(total(app(hb, eb)), b), ur_b(hb), gk_of(pick(eb, tch), eb, hb, tch, b, g, pick_min(eb, tch), {==}), sweep_pairs(g, hb, rest_of(eb, pick(eb, tch)), tch, b))

# --- the pairs a driver can check: no evicted unrooted blob was used after a survivor ---------

def lru_pair_ok.k(+k: Bool, +m: Bool, +u: Nat, +v: Nat, es: IsT(kle.k(k, m, u, v))) -> {or(or(k, m), le_b(u, v)) == True{} : Bool}:
  match k m:
    case False{} False{}:
      istrue(le_b(u, v), es)
    case False{} True{}:
      {==}
    case True{} False{}:
      match es:
    case True{} True{}:
      {==}

def lru_pair_ok(+e: Blob, +s: Blob, es: IsT(kle(True{}, e, s))) -> {lru_pair(e, s) == True{} : Bool}:
  match e s:
    case Blob{i, y, c, u, k} Blob{j, z, d, v, m}:
      lru_pair_ok.k(k, m, u, v, es)

def lru_to_ok(+e: Blob, +kept: +List<Blob>, ge: Ge(True{}, e, kept)) -> {lru_to(e, kept) == True{} : Bool}:
  match kept:
    case Nil{}:
      {==}
    case Con{s, t}:
      (es, ge2) = ge
      and_t(lru_pair(e, s), lru_to(e, t), lru_pair_ok(e, s, es), lru_to_ok(e, t, ge2))

def lru_b_ok(+ev: +List<Blob>, +kept: +List<Blob>, ga: GeAll(True{}, ev, kept)) -> {lru_b(ev, kept) == True{} : Bool}:
  match ev:
    case Nil{}:
      {==}
    case Con{e, t}:
      (ge, ga2) = ga
      and_t(lru_to(e, kept), lru_b(t, kept), lru_to_ok(e, kept, ge), lru_b_ok(t, kept, ga2))

def lru_sw(sw: Swept, +ga: GeAll(True{}, swe(sw), swk(sw))) -> {lru.sw(sw) == True{} : Bool}:
  match sw:
    case Swept{k, e}:
      lru_b_ok(e, k, ga)

def lru_sp(sp: Sp, +b: Nat) -> {lru.sp(sp, True{}, b) == True{} : Bool}:
  match sp:
    case Sp{+hb, +eb}:
      lru_sw(sweep(len(eb), hb, eb, True{}, b), sweep_pairs(len(eb), hb, eb, True{}, b))

def lru_st(+lk: Bool, +at: Bool, +b: Nat, s: St) -> {lru_after(Pol{True{}, lk, at}, b, s) == True{} : Bool}:
  match s:
    case St{n, +bs, rs, +fl, d, w}:
      lru_sp(split(bs, holds(lk, fl)), b)

# --- lists of ids ------------------------------------------------------------------------

def and_l(+a: Bool, +b: Bool, h: {and(a, b) == True{} : Bool}) -> {a == True{} : Bool}:
  match a:
    case True{}:
      {==}
    case False{}:
      absurd({False{} == True{} : Bool}, ff_tt(h))

def and_r(+a: Bool, +b: Bool, h: {and(a, b) == True{} : Bool}) -> {b == True{} : Bool}:
  match a:
    case True{}:
      h
    case False{}:
      absurd({b == True{} : Bool}, ff_tt(h))

def snocn(xs: +List<Nat>, +y: Nat) -> +List<Nat>:
  match xs:
    case Nil{}:
      [y]
    case Con{h, t}:
      h <> snocn(t, y)

def appn(xs: +List<Nat>, ys: +List<Nat>) -> +List<Nat>:
  match xs:
    case Nil{}:
      ys
    case Con{h, t}:
      h <> appn(t, ys)

def imp(a: Bool, b: Bool) -> Bool:
  or(not(a), b)

def imp_mp(+a: Bool, +b: Bool, h: {imp(a, b) == True{} : Bool}, ha: {a == True{} : Bool}) -> {b == True{} : Bool}:
  match a:
    case True{}:
      match b:
        case True{}:
          {==}
        case False{}:
          absurd({False{} == True{} : Bool}, ff_tt(h))
    case False{}:
      absurd({b == True{} : Bool}, ff_tt(ha))

def imp_or_r(+s: Bool, +m: Bool, +m2: Bool, ih: {imp(m, m2) == True{} : Bool}) -> {imp(or(s, m), or(s, m2)) == True{} : Bool}:
  match s:
    case True{}:
      {==}
    case False{}:
      ih

# a name in a list stays in it when the list grows at the end
def mem_snoc(+n: Nat, +xs: +List<Nat>, +y: Nat) -> {imp(mem(n, xs), mem(n, snocn(xs, y))) == True{} : Bool}:
  match xs:
    case Nil{}:
      {==}
    case Con{x, t}:
      imp_or_r(same(n, x), mem(n, t), mem(n, snocn(t, y)), mem_snoc(n, t, y))

def all_in_snoc(+l: +List<Nat>, +xs: +List<Nat>, +y: Nat, +h: {all_in(l, xs) == True{} : Bool}) -> {all_in(l, snocn(xs, y)) == True{} : Bool}:
  match l:
    case Nil{}:
      {==}
    case Con{n, t}:
      and_t(mem(n, snocn(xs, y)), all_in(t, snocn(xs, y)), imp_mp(mem(n, xs), mem(n, snocn(xs, y)), mem_snoc(n, xs, y), and_l(mem(n, xs), all_in(t, xs), h)), all_in_snoc(t, xs, y, and_r(mem(n, xs), all_in(t, xs), h)))

def same_refl(+n: Nat) -> {same(n, n) == True{} : Bool}:
  match n:
    case 0n:
      {==}
    case 1n+p:
      same_refl(p)

def mem_snoc_last(+y: Nat, +xs: +List<Nat>) -> {mem(y, snocn(xs, y)) == True{} : Bool}:
  match xs:
    case Nil{}:
      or_l(same(y, y), False{}, same_refl(y))
    case Con{x, t}:
      or_r(same(y, x), mem(y, snocn(t, y)), mem_snoc_last(y, t))

# --- the ids a store step leaves ------------------------------------------------------------

def bid_apply(+e: Ed, +now: Nat, b: Blob) -> {bid(apply(e, now, b)) == bid(b) : Nat}:
  match e b:
    case ETouch{} Blob{i, y, c, u, p}:
      {==}
    case ESet{on} Blob{i, y, c, u, p}:
      {==}

def ids_edit.if(+b: Blob, rec: +List<Blob>, t: +List<Blob>, +e: Ed, +now: Nat, m: Bool, ih: {ids(rec) == ids(t) : +List<Nat>}) -> {ids(edit.if(b, rec, e, now, m)) == bid(b) <> ids(t) : +List<Nat>}:
  match m:
    case True{}:
      %ih : {bid(apply(e, now, b)) <> ids(rec) == bid(b) <> _ : +List<Nat>}
      %bid_apply(e, now, b) : {bid(apply(e, now, b)) <> ids(rec) == _ <> ids(rec) : +List<Nat>}
      {==}
    case False{}:
      %ih : {bid(b) <> ids(rec) == bid(b) <> _ : +List<Nat>}
      {==}

def ids_edit(+bs: +List<Blob>, +sel: +List<Nat>, +e: Ed, +now: Nat) -> {ids(edit(bs, sel, e, now)) == ids(bs) : +List<Nat>}:
  match bs:
    case Nil{}:
      {==}
    case Con{+b, +t}:
      ids_edit.if(b, edit(t, sel, e, now), t, e, now, mem(bid(b), sel), ids_edit(t, sel, e, now))

def ids_snoc(+bs: +List<Blob>, +b: Blob) -> {ids(snoc(bs, b)) == snocn(ids(bs), bid(b)) : +List<Nat>}:
  match bs:
    case Nil{}:
      {==}
    case Con{h, t}:
      %ids_snoc(t, b) : {bid(h) <> ids(snoc(t, b)) == bid(h) <> _ : +List<Nat>}
      {==}

# a blob is there after its store, and every blob that was there still is
def store_has.at(+bs: +List<Blob>, +i: Nat, +y: Nat, +now: Nat, pr: Bool, gp: {pr == mem(i, ids(bs)) : Bool}) -> {mem(i, ids(store.at(bs, i, y, now, pr))) == True{} : Bool}:
  match pr:
    case True{}:
      %Equal.sym(+List<Nat>, ids(edit(bs, [i], ETouch{}, now)), ids(bs), ids_edit(bs, [i], ETouch{}, now)) : {mem(i, _) == True{} : Bool}
      %gp : {_ == True{} : Bool}
      {==}
    case False{}:
      %Equal.sym(+List<Nat>, ids(snoc(bs, Blob{i, y, now, now, False{}})), snocn(ids(bs), bid(Blob{i, y, now, now, False{}})), ids_snoc(bs, Blob{i, y, now, now, False{}})) : {mem(i, _) == True{} : Bool}
      mem_snoc_last(i, ids(bs))

def store_has(+bs: +List<Blob>, +i: Nat, +y: Nat, +now: Nat) -> {mem(i, ids(store(bs, i, y, now))) == True{} : Bool}:
  store_has.at(bs, i, y, now, mem(i, ids(bs)), {==})

def all_in_store.at(+l: +List<Nat>, +bs: +List<Blob>, +i: Nat, +y: Nat, +now: Nat, pr: Bool, +h: {all_in(l, ids(bs)) == True{} : Bool}) -> {all_in(l, ids(store.at(bs, i, y, now, pr))) == True{} : Bool}:
  match pr:
    case True{}:
      %Equal.sym(+List<Nat>, ids(edit(bs, [i], ETouch{}, now)), ids(bs), ids_edit(bs, [i], ETouch{}, now)) : {all_in(l, _) == True{} : Bool}
      h
    case False{}:
      %Equal.sym(+List<Nat>, ids(snoc(bs, Blob{i, y, now, now, False{}})), snocn(ids(bs), bid(Blob{i, y, now, now, False{}})), ids_snoc(bs, Blob{i, y, now, now, False{}})) : {all_in(l, _) == True{} : Bool}
      all_in_snoc(l, ids(bs), bid(Blob{i, y, now, now, False{}}), h)

def all_in_store(+l: +List<Nat>, +bs: +List<Blob>, +i: Nat, +y: Nat, +now: Nat, +h: {all_in(l, ids(bs)) == True{} : Bool}) -> {all_in(l, ids(store(bs, i, y, now))) == True{} : Bool}:
  all_in_store.at(l, bs, i, y, now, mem(i, ids(bs)), h)

def all_in_edit(+l: +List<Nat>, +bs: +List<Blob>, +sel: +List<Nat>, +e: Ed, +now: Nat, h: {all_in(l, ids(bs)) == True{} : Bool}) -> {all_in(l, ids(edit(bs, sel, e, now))) == True{} : Bool}:
  %Equal.sym(+List<Nat>, ids(edit(bs, sel, e, now)), ids(bs), ids_edit(bs, sel, e, now)) : {all_in(l, _) == True{} : Bool}
  h

# --- what the sweep keeps ------------------------------------------------------------------

def mem_appl(+n: Nat, +hb: +List<Blob>, +k: +List<Blob>) -> {imp(mem(n, ids(hb)), mem(n, ids(app(hb, k)))) == True{} : Bool}:
  match hb:
    case Nil{}:
      {==}
    case Con{x, t}:
      imp_or_r(same(n, bid(x)), mem(n, ids(t)), mem(n, ids(app(t, k))), mem_appl(n, t, k))

def all_in_appl(+l: +List<Nat>, +hb: +List<Blob>, +k: +List<Blob>, +h: {all_in(l, ids(hb)) == True{} : Bool}) -> {all_in(l, ids(app(hb, k))) == True{} : Bool}:
  match l:
    case Nil{}:
      {==}
    case Con{n, t}:
      and_t(mem(n, ids(app(hb, k))), all_in(t, ids(app(hb, k))), imp_mp(mem(n, ids(hb)), mem(n, ids(app(hb, k))), mem_appl(n, hb, k), and_l(mem(n, ids(hb)), all_in(t, ids(hb)), h)), all_in_appl(t, hb, k, and_r(mem(n, ids(hb)), all_in(t, ids(hb)), h)))

def same_eq(+n: Nat, +m: Nat, g: {same(n, m) == True{} : Bool}) -> {n == m : Nat}:
  match n m:
    case 0n 0n:
      {==}
    case 0n 1n+q:
      absurd({0n == 1n+q : Nat}, ff_tt(g))
    case 1n+p 0n:
      absurd({1n+p == 0n : Nat}, ff_tt(g))
    case 1n+p 1n+q:
      %same_eq(p, q, g) : {1n+p == 1n+_ : Nat}
      {==}

def mem_cong(+n: Nat, +m: Nat, +h: +List<Nat>, e: {n == m : Nat}, hn: {mem(n, h) == True{} : Bool}) -> {mem(m, h) == True{} : Bool}:
  %e : {mem(_, h) == True{} : Bool}
  hn

def mem_hbs.f(+n: Nat, +x: Blob, +h: +List<Nat>, s: Bool, gs: {s == same(n, bid(x)) : Bool}, +c: Bool, +d: Bool, hn: {mem(n, h) == True{} : Bool}, gm: {mem(bid(x), h) == False{} : Bool}, ih: {imp(c, d) == True{} : Bool}) -> {imp(or(s, c), d) == True{} : Bool}:
  match s:
    case True{}:
      absurd({imp(or(True{}, c), d) == True{} : Bool}, ff_tt(Equal.trans(Bool, False{}, mem(bid(x), h), True{}, Equal.sym(Bool, mem(bid(x), h), False{}, gm), mem_cong(n, bid(x), h, same_eq(n, bid(x), Equal.sym(Bool, True{}, same(n, bid(x)), gs)), hn))))
    case False{}:
      ih

def mem_hbs.if(+n: Nat, +x: Blob, +t: +List<Blob>, +h: +List<Nat>, m: Bool, gm: {m == mem(bid(x), h) : Bool}, hn: {mem(n, h) == True{} : Bool}, ih: {imp(mem(n, ids(t)), mem(n, ids(hbs(t, h)))) == True{} : Bool}) -> {imp(mem(n, ids(x <> t)), mem(n, ids(hbs.if(x, hbs(t, h), m)))) == True{} : Bool}:
  match m:
    case True{}:
      imp_or_r(same(n, bid(x)), mem(n, ids(t)), mem(n, ids(hbs(t, h))), ih)
    case False{}:
      mem_hbs.f(n, x, h, same(n, bid(x)), {==}, mem(n, ids(t)), mem(n, ids(hbs(t, h))), hn, Equal.sym(Bool, False{}, mem(bid(x), h), gm), ih)

# a name a producer holds that is in the store is among the blobs the sweep sets aside
def mem_hbs(+n: Nat, +bs: +List<Blob>, +h: +List<Nat>, +hn: {mem(n, h) == True{} : Bool}) -> {imp(mem(n, ids(bs)), mem(n, ids(hbs(bs, h)))) == True{} : Bool}:
  match bs:
    case Nil{}:
      {==}
    case Con{+x, +t}:
      mem_hbs.if(n, x, t, h, mem(bid(x), h), {==}, hn, mem_hbs(n, t, h, hn))

def all_in_hbs(+l: +List<Nat>, +bs: +List<Blob>, +h: +List<Nat>, +hl: {all_in(l, h) == True{} : Bool}, +hb: {all_in(l, ids(bs)) == True{} : Bool}) -> {all_in(l, ids(hbs(bs, h))) == True{} : Bool}:
  match l:
    case Nil{}:
      {==}
    case Con{n, t}:
      and_t(mem(n, ids(hbs(bs, h))), all_in(t, ids(hbs(bs, h))), imp_mp(mem(n, ids(bs)), mem(n, ids(hbs(bs, h))), mem_hbs(n, bs, h, and_l(mem(n, h), all_in(t, h), hl)), and_l(mem(n, ids(bs)), all_in(t, ids(bs)), hb)), all_in_hbs(t, bs, h, and_r(mem(n, h), all_in(t, h), hl), and_r(mem(n, ids(bs)), all_in(t, ids(bs)), hb)))

# --- a release gives back at most what was held ----------------------------------------------

def remove_one_sub.if(+h: Nat, rec: +List<Nat>, +t: +List<Nat>, +i: +List<Nat>, b: Bool, mh: {mem(h, i) == True{} : Bool}, at: {all_in(t, i) == True{} : Bool}, ih: {all_in(rec, i) == True{} : Bool}) -> {all_in(remove_one.if(h, rec, t, b), i) == True{} : Bool}:
  match b:
    case True{}:
      at
    case False{}:
      and_t(mem(h, i), all_in(rec, i), mh, ih)

def remove_one_sub(+l: +List<Nat>, +x: Nat, +i: +List<Nat>, +hl: {all_in(l, i) == True{} : Bool}) -> {all_in(remove_one(x, l), i) == True{} : Bool}:
  match l:
    case Nil{}:
      {==}
    case Con{+h, +t}:
      remove_one_sub.if(h, remove_one(x, t), t, i, same(x, h), and_l(mem(h, i), all_in(t, i), hl), and_r(mem(h, i), all_in(t, i), hl), remove_one_sub(t, x, i, and_r(mem(h, i), all_in(t, i), hl)))

def release_sub(+names: +List<Nat>, +fl: +List<Nat>, +i: +List<Nat>, +h: {all_in(fl, i) == True{} : Bool}) -> {all_in(release(names, fl), i) == True{} : Bool}:
  match names:
    case Nil{}:
      h
    case Con{n, t}:
      release_sub(t, remove_one(n, fl), i, remove_one_sub(fl, n, i, h))

# --- a name a producer holds is in the store when everything it holds is ----------------------

def imp_and_l(+a: Bool, +b: Bool) -> {imp(and(a, b), a) == True{} : Bool}:
  match a b:
    case True{} True{}:
      {==}
    case True{} False{}:
      {==}
    case False{} True{}:
      {==}
    case False{} False{}:
      {==}

def imp_drop(+m: Bool, +mx: Bool, +a: Bool, +r: Bool, ih: {imp(m, imp(a, r)) == True{} : Bool}) -> {imp(m, imp(and(mx, a), r)) == True{} : Bool}:
  match m mx a:
    case True{} True{} True{}:
      ih
    case True{} True{} False{}:
      {==}
    case True{} False{} True{}:
      {==}
    case True{} False{} False{}:
      {==}
    case False{} mx0 a0:
      {==}

def mem_in.if(+n: Nat, +x: Nat, +t: +List<Nat>, +i: +List<Nat>, s: Bool, gs: {s == same(n, x) : Bool}, ih: {imp(mem(n, t), imp(all_in(t, i), mem(n, i))) == True{} : Bool}) -> {imp(or(s, mem(n, t)), imp(and(mem(x, i), all_in(t, i)), mem(n, i))) == True{} : Bool}:
  match s:
    case True{}:
      %Equal.sym(Nat, n, x, same_eq(n, x, Equal.sym(Bool, True{}, same(n, x), gs))) : {imp(or(True{}, mem(n, t)), imp(and(mem(x, i), all_in(t, i)), mem(_, i))) == True{} : Bool}
      imp_and_l(mem(x, i), all_in(t, i))
    case False{}:
      imp_drop(mem(n, t), mem(x, i), all_in(t, i), mem(n, i), ih)

def mem_in(+n: Nat, +fl: +List<Nat>, +i: +List<Nat>) -> {imp(mem(n, fl), imp(all_in(fl, i), mem(n, i))) == True{} : Bool}:
  match fl:
    case Nil{}:
      {==}
    case Con{x, t}:
      mem_in.if(n, x, t, i, same(n, x), {==}, mem_in(n, t, i))

def all_in_trans(+l: +List<Nat>, +fl: +List<Nat>, +i: +List<Nat>, +hl: {all_in(l, fl) == True{} : Bool}, +hf: {all_in(fl, i) == True{} : Bool}) -> {all_in(l, i) == True{} : Bool}:
  match l:
    case Nil{}:
      {==}
    case Con{n, t}:
      and_t(mem(n, i), all_in(t, i), imp_mp(all_in(fl, i), mem(n, i), imp_mp(mem(n, fl), imp(all_in(fl, i), mem(n, i)), mem_in(n, fl, i), and_l(mem(n, fl), all_in(t, fl), hl)), hf), all_in_trans(t, fl, i, and_r(mem(n, fl), all_in(t, fl), hl), hf))

# --- (c) a producer's blobs stay in the store from the store to the publish --------------------

def all_in_cons(+l: +List<Nat>, +x: Nat, +i: +List<Nat>, +h: {all_in(l, i) == True{} : Bool}) -> {all_in(l, x <> i) == True{} : Bool}:
  match l:
    case Nil{}:
      {==}
    case Con{n, t}:
      and_t(mem(n, x <> i), all_in(t, x <> i), or_r(same(n, x), mem(n, i), and_l(mem(n, i), all_in(t, i), h)), all_in_cons(t, x, i, and_r(mem(n, i), all_in(t, i), h)))

def all_in_self(+l: +List<Nat>) -> {all_in(l, l) == True{} : Bool}:
  match l:
    case Nil{}:
      {==}
    case Con{n, t}:
      and_t(mem(n, n <> t), all_in(t, n <> t), or_l(same(n, n), mem(n, t), same_refl(n)), all_in_cons(t, n, t, all_in_self(t)))

def Held(s: St) -> Data:
  match s:
    case St{n, +bs, rs, +fl, d, w}:
      {all_in(fl, ids(bs)) == True{} : Bool}

def held_publish(+n: Nat, +i: Nat, +names: +List<Nat>, +bs: +List<Blob>, +rs: +List<Res>, +fl: +List<Nat>, d: Nat, w: Nat, ok: Bool, h: {all_in(fl, ids(bs)) == True{} : Bool}) -> Held(publish(n, i, names, bs, rs, fl, d, w, ok)):
  match ok:
    case True{}:
      release_sub(names, fl, ids(bs), h)
    case False{}:
      release_sub(names, fl, ids(bs), h)

def held_lookup(+n: Nat, fd: Fd, +bs: +List<Blob>, rs: +List<Res>, +fl: +List<Nat>, d: Nat, w: Nat, h: {all_in(fl, ids(bs)) == True{} : Bool}) -> Held(lookup(n, fd, bs, rs, fl, d, w)):
  match fd:
    case FNone{}:
      h
    case FSome{names}:
      all_in_edit(fl, bs, names, ETouch{}, n, h)

def held_swept.at(at: Bool, +n: Nat, +k: +List<Blob>, +rs: +List<Res>, +fl: +List<Nat>, d: Nat, w: Nat, h: {all_in(fl, ids(k)) == True{} : Bool}) -> Held(swept.at(at, n, k, rs, fl, d, w)):
  match at:
    case True{}:
      h
    case False{}:
      h

def held_swept(at: Bool, +n: Nat, +hb: +List<Blob>, sw: Swept, rs: +List<Res>, +fl: +List<Nat>, d: Nat, w: Nat, h: {all_in(fl, ids(hb)) == True{} : Bool}) -> Held(swept(at, n, hb, sw, rs, fl, d, w)):
  match sw:
    case Swept{+k, ev}:
      held_swept.at(at, n, app(hb, k), rs, fl, d, w, all_in_appl(fl, hb, k, h))

def held_step(+tch: Bool, +at: Bool, +o: Op, +s: St, h: Held(s)) -> Held(step(Pol{tch, True{}, at}, o, s)):
  match o s:
    case StoreBlob{+i, +y} St{+n, +bs, rs, +fl, d, w}:
      and_t(mem(i, ids(store(bs, i, y, 1n+n))), all_in(fl, ids(store(bs, i, y, 1n+n))), store_has(bs, i, y, 1n+n), all_in_store(fl, bs, i, y, 1n+n, h))
    case Publish{+i, +names} St{+n, +bs, +rs, +fl, d, w}:
      held_publish(1n+n, i, names, bs, rs, fl, d, w, all_present(names, bs), h)
    case Use{+i} St{+n, +bs, +rs, +fl, d, w}:
      held_lookup(1n+n, find_res(i, rs), bs, rs, fl, d, w, h)
    case SweepObjects{+b} St{+n, +bs, +rs, +fl, d, w}:
      held_swept(at, 1n+n, hbs(bs, fl), sweep(len(ebs(bs, fl)), hbs(bs, fl), ebs(bs, fl), tch, b), rs, fl, d, w, all_in_hbs(fl, bs, fl, all_in_self(fl), h))
    case SweepResults{} St{+n, +bs, +rs, +fl, d, w}:
      h
    case Root{+i, +on} St{+n, +bs, +rs, +fl, d, w}:
      all_in_edit(fl, bs, [i], ESet{on}, n, h)
    case Abandon{+i} St{+n, +bs, +rs, +fl, d, w}:
      remove_one_sub(fl, i, ids(bs), h)

def held_run(+os: +List<Op>, +tch: Bool, +at: Bool, +s: St, h: Held(s)) -> Held(run(os, Pol{tch, True{}, at}, s)):
  match os:
    case Nil{}:
      h
    case Con{o, t}:
      held_run(t, tch, at, step(Pol{tch, True{}, at}, o, s), held_step(tch, at, o, s, h))

def held_empty() -> Held(empty()):
  {==}

law in_flight_kept:
  for +tch: Bool
  for +at: Bool
  for +os: +List<Op>
  {inflight_b(run(os, Pol{tch, True{}, at}, empty())) == True{} : Bool}

def held_of(+s: St, h: Held(s)) -> {inflight_b(s) == True{} : Bool}:
  match s:
    case St{n, bs, rs, fl, d, w}:
      h

def in_flight_kept(tch, at, os):
  held_of(run(os, Pol{tch, True{}, at}, empty()), held_run(os, tch, at, empty(), held_empty()))

# --- (d) a reader never finds a result that names a blob that is gone ---------------------------

def rs_ok_store(+rs: +List<Res>, +bs: +List<Blob>, +i: Nat, +y: Nat, +now: Nat, +h: {rs_ok(rs, ids(bs)) == True{} : Bool}) -> {rs_ok(rs, ids(store(bs, i, y, now))) == True{} : Bool}:
  match rs:
    case Nil{}:
      {==}
    case Con{r, t}:
      and_t(all_in(rnames(r), ids(store(bs, i, y, now))), rs_ok(t, ids(store(bs, i, y, now))), all_in_store(rnames(r), bs, i, y, now, and_l(all_in(rnames(r), ids(bs)), rs_ok(t, ids(bs)), h)), rs_ok_store(t, bs, i, y, now, and_r(all_in(rnames(r), ids(bs)), rs_ok(t, ids(bs)), h)))

def rs_ok_edit(+rs: +List<Res>, +bs: +List<Blob>, +sel: +List<Nat>, +e: Ed, +now: Nat, h: {rs_ok(rs, ids(bs)) == True{} : Bool}) -> {rs_ok(rs, ids(edit(bs, sel, e, now))) == True{} : Bool}:
  %Equal.sym(+List<Nat>, ids(edit(bs, sel, e, now)), ids(bs), ids_edit(bs, sel, e, now)) : {rs_ok(rs, _) == True{} : Bool}
  h

def rs_ok_put.if(+r: Res, h0: Res, +t: +List<Res>, rec: +List<Res>, +x: +List<Nat>, b: Bool, hn: {all_in(rnames(r), x) == True{} : Bool}, hh: {all_in(rnames(h0), x) == True{} : Bool}, ht: {rs_ok(t, x) == True{} : Bool}, ih: {rs_ok(rec, x) == True{} : Bool}) -> {rs_ok(put_res.if(r, h0, t, rec, b), x) == True{} : Bool}:
  match b:
    case True{}:
      and_t(all_in(rnames(r), x), rs_ok(t, x), hn, ht)
    case False{}:
      and_t(all_in(rnames(h0), x), rs_ok(rec, x), hh, ih)

def rs_ok_put(+rs: +List<Res>, +r: Res, +x: +List<Nat>, +hn: {all_in(rnames(r), x) == True{} : Bool}, +h: {rs_ok(rs, x) == True{} : Bool}) -> {rs_ok(put_res(rs, r), x) == True{} : Bool}:
  match rs:
    case Nil{}:
      and_t(all_in(rnames(r), x), True{}, hn, {==})
    case Con{+h0, +t}:
      rs_ok_put.if(r, h0, t, put_res(t, r), x, same(rid(h0), rid(r)), hn, and_l(all_in(rnames(h0), x), rs_ok(t, x), h), and_r(all_in(rnames(h0), x), rs_ok(t, x), h), rs_ok_put(t, r, x, hn, and_r(all_in(rnames(h0), x), rs_ok(t, x), h)))

def rs_ok_drop.if(+r: Res, rec: +List<Res>, +x: +List<Nat>, b: Bool, gb: {b == all_in(rnames(r), x) : Bool}, ih: {rs_ok(rec, x) == True{} : Bool}) -> {rs_ok(drop_dangling.if(r, rec, b), x) == True{} : Bool}:
  match b:
    case True{}:
      and_t(all_in(rnames(r), x), rs_ok(rec, x), Equal.sym(Bool, True{}, all_in(rnames(r), x), gb), ih)
    case False{}:
      ih

def rs_ok_drop(+rs: +List<Res>, +bs: +List<Blob>) -> {rs_ok(drop_dangling(rs, bs), ids(bs)) == True{} : Bool}:
  match rs:
    case Nil{}:
      {==}
    case Con{+r, t}:
      rs_ok_drop.if(r, drop_dangling(t, bs), ids(bs), all_present(rnames(r), bs), {==}, rs_ok_drop(t, bs))

def find_ok.if(+ns: +List<Nat>, rec: Fd, +x: +List<Nat>, b: Bool, hns: {all_in(ns, x) == True{} : Bool}, ih: {fd_ok(rec, x) == True{} : Bool}) -> {fd_ok(find_res.if(ns, rec, b), x) == True{} : Bool}:
  match b:
    case True{}:
      hns
    case False{}:
      ih

def find_ok(+i: Nat, rs: +List<Res>, +x: +List<Nat>, +h: {rs_ok(rs, x) == True{} : Bool}) -> {fd_ok(find_res(i, rs), x) == True{} : Bool}:
  match rs:
    case Nil{}:
      {==}
    case Con{Res{j, +ns}, +t}:
      find_ok.if(ns, find_res(i, t), x, same(i, j), and_l(all_in(ns, x), rs_ok(t, x), h), find_ok(i, t, x, and_r(all_in(ns, x), rs_ok(t, x), h)))

def Clean(s: St) -> Data:
  match s:
    case St{n, +bs, +rs, fl, d, w}:
      Sigma<&2, &2, {rs_ok(rs, ids(bs)) == True{} : Bool}, u => {same(d, 0n) == True{} : Bool}>

def dang_keep(+d: Nat, ok: Bool, hd: {same(d, 0n) == True{} : Bool}, hok: {ok == True{} : Bool}) -> {same(dang.add(d, ok), 0n) == True{} : Bool}:
  match ok:
    case True{}:
      hd
    case False{}:
      absurd({same(dang.add(d, False{}), 0n) == True{} : Bool}, ff_tt(hok))

def clean_publish(+n: Nat, +i: Nat, +names: +List<Nat>, +bs: +List<Blob>, +rs: +List<Res>, +fl: +List<Nat>, +d: Nat, w: Nat, ok: Bool, gok: {ok == all_in(names, ids(bs)) : Bool}, hr: {rs_ok(rs, ids(bs)) == True{} : Bool}, hd: {same(d, 0n) == True{} : Bool}) -> Clean(publish(n, i, names, bs, rs, fl, d, w, ok)):
  match ok:
    case True{}:
      (rs_ok_put(rs, Res{i, names}, ids(bs), Equal.sym(Bool, True{}, all_in(names, ids(bs)), gok), hr), hd)
    case False{}:
      (hr, hd)

def clean_use(+n: Nat, fd: Fd, +bs: +List<Blob>, +rs: +List<Res>, fl: +List<Nat>, +d: Nat, w: Nat, hr: {rs_ok(rs, ids(bs)) == True{} : Bool}, hd: {same(d, 0n) == True{} : Bool}, hf: {fd_ok(fd, ids(bs)) == True{} : Bool}) -> Clean(lookup(n, fd, bs, rs, fl, d, w)):
  match fd:
    case FNone{}:
      (hr, hd)
    case FSome{+names}:
      (rs_ok_edit(rs, bs, names, ETouch{}, n, hr), dang_keep(d, all_present(names, bs), hd, hf))

def clean_swept(+n: Nat, +hb: +List<Blob>, sw: Swept, +rs: +List<Res>, fl: +List<Nat>, d: Nat, w: Nat, hd: {same(d, 0n) == True{} : Bool}) -> Clean(swept(True{}, n, hb, sw, rs, fl, d, w)):
  match sw:
    case Swept{+k, ev}:
      (rs_ok_drop(rs, app(hb, k)), hd)

def clean_step(+tch: Bool, +lk: Bool, +o: Op, +s: St, c: Clean(s)) -> Clean(step(Pol{tch, lk, True{}}, o, s)):
  match o s:
    case StoreBlob{+i, +y} St{+n, +bs, +rs, +fl, +d, w}:
      (hr, hd) = c
      (rs_ok_store(rs, bs, i, y, 1n+n, hr), hd)
    case Publish{+i, +names} St{+n, +bs, +rs, +fl, +d, w}:
      (hr, hd) = c
      clean_publish(1n+n, i, names, bs, rs, fl, d, w, all_present(names, bs), {==}, hr, hd)
    case Use{+i} St{+n, +bs, +rs, +fl, +d, w}:
      (+hr, hd) = c
      clean_use(1n+n, find_res(i, rs), bs, rs, fl, d, w, hr, hd, find_ok(i, rs, ids(bs), hr))
    case SweepObjects{+b} St{+n, +bs, +rs, +fl, +d, w}:
      (hr, hd) = c
      clean_swept(1n+n, hbs(bs, holds(lk, fl)), sweep(len(ebs(bs, holds(lk, fl))), hbs(bs, holds(lk, fl)), ebs(bs, holds(lk, fl)), tch, b), rs, fl, d, w, hd)
    case SweepResults{} St{+n, +bs, +rs, +fl, +d, w}:
      (hr, hd) = c
      (rs_ok_drop(rs, bs), hd)
    case Root{+i, +on} St{+n, +bs, +rs, +fl, +d, w}:
      (hr, hd) = c
      (rs_ok_edit(rs, bs, [i], ESet{on}, n, hr), hd)
    case Abandon{+i} St{+n, +bs, +rs, +fl, +d, w}:
      c

def clean_run(+os: +List<Op>, +tch: Bool, +lk: Bool, +s: St, c: Clean(s)) -> Clean(run(os, Pol{tch, lk, True{}}, s)):
  match os:
    case Nil{}:
      c
    case Con{o, t}:
      clean_run(t, tch, lk, step(Pol{tch, lk, True{}}, o, s), clean_step(tch, lk, o, s, c))

def clean_empty() -> Clean(empty()):
  ({==}, {==})

law no_dangling:
  for +tch: Bool
  for +lk: Bool
  for +os: +List<Op>
  {dang_of(run(os, Pol{tch, lk, True{}}, empty())) == 0n : Nat}

def clean_dang(+s: St, c: Clean(s)) -> {dang_of(s) == 0n : Nat}:
  match s:
    case St{n, bs, rs, fl, d, w}:
      (hr, hd) = c
      same_eq(d, 0n, hd)

def no_dangling(tch, lk, os):
  clean_dang(run(os, Pol{tch, lk, True{}}, empty()), clean_run(os, tch, lk, empty(), clean_empty()))

# --- a publish that follows its stores is never refused ------------------------------------------

def publish_redo(+n: Nat, +i: Nat, +names: +List<Nat>, +bs: +List<Blob>, +rs: +List<Res>, +fl: +List<Nat>, +d: Nat, +w: Nat, ok: Bool, g: {ok == True{} : Bool}) -> {redo_of(publish(n, i, names, bs, rs, fl, d, w, ok)) == w : Nat}:
  match ok:
    case True{}:
      {==}
    case False{}:
      absurd({redo_of(publish(n, i, names, bs, rs, fl, d, w, False{})) == w : Nat}, ff_tt(g))

def publish_kept_st(+tch: Bool, +at: Bool, +i: Nat, +names: +List<Nat>, +s: St, h: Held(s), g: {all_in(names, fl_of(s)) == True{} : Bool}) -> {redo_of(step(Pol{tch, True{}, at}, Publish{i, names}, s)) == redo_of(s) : Nat}:
  match s:
    case St{+n, +bs, +rs, +fl, +d, +w}:
      publish_redo(1n+n, i, names, bs, rs, fl, d, w, all_present(names, bs), all_in_trans(names, fl, ids(bs), g, h))

# A producer that stores the blobs a result names and then publishes it is never refused, so its
# compile never runs twice.
law publish_kept:
  for +tch: Bool
  for +at: Bool
  for +os: +List<Op>
  for +i: Nat
  for +names: +List<Nat>
  for g: {all_in(names, fl_of(run(os, Pol{tch, True{}, at}, empty()))) == True{} : Bool}
  {redo_of(step(Pol{tch, True{}, at}, Publish{i, names}, run(os, Pol{tch, True{}, at}, empty()))) == redo_of(run(os, Pol{tch, True{}, at}, empty())) : Nat}

def publish_kept(tch, at, os, i, names, g):
  publish_kept_st(tch, at, i, names, run(os, Pol{tch, True{}, at}, empty()), held_run(os, tch, at, empty(), held_empty()), g)

# --- companions: a sweep that is over budget evicts, and one that is within budget does not ---------

def sweep_noop.go(pk: Pk, +eb: +List<Blob>, r: Swept, wi: Bool, +ub: Bool, gw: {wi == True{} : Bool}) -> {swe(sweep.go(eb, pk, r, wi, ub)) == [] : +List<Blob>}:
  match pk wi:
    case PNone{} True{}:
      {==}
    case PNone{} False{}:
      {==}
    case PSome{v, rest} True{}:
      {==}
    case PSome{v, rest} False{}:
      absurd({swe(sweep.stop(eb, v, r, and(brooted(v), ub))) == [] : +List<Blob>}, ff_tt(gw))

# A sweep that starts within its budget evicts nothing.
law sweep_noop:
  for +f: Nat
  for +hb: +List<Blob>
  for +eb: +List<Blob>
  for +tch: Bool
  for +b: Nat
  for g: {le_b(total(app(hb, eb)), b) == True{} : Bool}
  {swe(sweep(f, hb, eb, tch, b)) == [] : +List<Blob>}

def sweep_noop(f, hb, eb, tch, b, g):
  match f:
    case 0n:
      {==}
    case 1n+q:
      sweep_noop.go(pick(eb, tch), eb, sweep(q, hb, rest_of(eb, pick(eb, tch)), tch, b), le_b(total(app(hb, eb)), b), ur_b(hb), g)

def sweep_acts.stop(+eb: +List<Blob>, +v: Blob, r: Swept, +ub: Bool, gr: {brooted(v) == False{} : Bool}) -> {swe(sweep.stop(eb, v, r, and(brooted(v), ub))) == v <> swe(r) : +List<Blob>}:
  match r:
    case Swept{k, e}:
      %Equal.sym(Bool, brooted(v), False{}, gr) : {swe(sweep.stop(eb, v, Swept{k, e}, and(_, ub))) == v <> e : +List<Blob>}
      {==}

# A sweep over its budget evicts the first victim when no producer or live checkout holds it.
law sweep_acts:
  for +q: Nat
  for +hb: +List<Blob>
  for +eb: +List<Blob>
  for +tch: Bool
  for +b: Nat
  for +v: Blob
  for +rest: +List<Blob>
  for gp: {pick(eb, tch) == PSome{v, rest} : Pk}
  for gw: {le_b(total(app(hb, eb)), b) == False{} : Bool}
  for gr: {brooted(v) == False{} : Bool}
  {swe(sweep(1n+q, hb, eb, tch, b)) == v <> swe(sweep(q, hb, rest, tch, b)) : +List<Blob>}

def sweep_acts(q, hb, eb, tch, b, v, rest, gp, gw, gr):
  %Equal.sym(Pk, pick(eb, tch), PSome{v, rest}, gp) : {swe(sweep.go(eb, _, sweep(q, hb, rest_of(eb, _), tch, b), le_b(total(app(hb, eb)), b), ur_b(hb))) == v <> swe(sweep(q, hb, rest, tch, b)) : +List<Blob>}
  %Equal.sym(Bool, le_b(total(app(hb, eb)), b), False{}, gw) : {swe(sweep.go(eb, PSome{v, rest}, sweep(q, hb, rest, tch, b), _, ur_b(hb))) == v <> swe(sweep(q, hb, rest, tch, b)) : +List<Blob>}
  sweep_acts.stop(eb, v, sweep(q, hb, rest, tch, b), ur_b(hb), gr)

# --- (a) a sweep ends within its budget or with nothing left to evict ------------------------------

# Under every setting: after SweepObjects the bytes are at most the budget, or the least blob left
# is rooted behind a blob a producer or a lock holds, or none is left.
law budget:
  for +p: Pol
  for +b: Nat
  for +os: +List<Op>
  {done_after(p, b, run(os, p, empty())) == True{} : Bool}

def budget(p, b, os):
  budget_st(p, b, run(os, p, empty()))

# --- (b) a sweep evicts the least recently used --------------------------------------------------

# With touch on, HEAD's setting: no evicted unrooted blob was used more recently than a surviving
# unrooted blob no producer holds.
law lru_head:
  for +lk: Bool
  for +at: Bool
  for +b: Nat
  for +os: +List<Op>
  {lru_after(Pol{True{}, lk, at}, b, run(os, Pol{True{}, lk, at}, empty())) == True{} : Bool}

def lru_head(lk, at, b, os):
  lru_st(lk, at, b, run(os, Pol{True{}, lk, at}, empty()))

# Blob 1 is stored first and used last (a hit on the result that names it); blob 2 is stored
# after it and never used. A sweep with a budget of 1 must evict one of them.
def t_lru() -> +List<Op>:
  [StoreBlob{1n, 1n}, StoreBlob{2n, 1n}, Publish{10n, [1n]}, Use{10n}]

# touch on: blob 2 goes, the blob the hit used stays
law lru_head_evicts:
  {ev_after(head(), 1n, run(t_lru(), head(), empty())) == [2n] : +List<Nat>}

# touch off, a build without record_use: the sweep ranks by creation, so blob 1 goes though blob 2
# is the older use. This is the counterexample to lru without record_use.
law lru_pin_evicts:
  {ev_after(pin(), 1n, run(t_lru(), pin(), empty())) == [1n] : +List<Nat>}

law lru_pin_refuted:
  {lru_after(pin(), 1n, run(t_lru(), pin(), empty())) == False{} : Bool}

def lru_head_evicts():
  {==}

def lru_pin_evicts():
  {==}

def lru_pin_refuted():
  {==}

# --- (c) a producer's blobs are kept until its result is published ---------------------------------

# Blob 1 is stored and a sweep with a budget of 0 runs; the producer then publishes the result
# that names the blob.
def t_swept() -> +List<Op>:
  [StoreBlob{1n, 1n}, SweepObjects{0n}]

def t_inflight() -> +List<Op>:
  [StoreBlob{1n, 1n}, SweepObjects{0n}, Publish{10n, [1n]}]

# The pin (8cd8f6b) and HEAD: the sweep evicts the blob the producer holds, so the publish is refused and the
# compile runs again (local.rs:386, rustc.rs:990, session.rs:1882). This is the counterexample to
# in_flight_kept.
law in_flight_lost_today:
  {inflight_b(run(t_swept(), head(), empty())) == False{} : Bool}

law publish_refused_today:
  {redo_of(run(t_inflight(), head(), empty())) == 1n : Nat}

# With the lease the same trace keeps the blob and the publish lands.
law in_flight_kept_here:
  {inflight_b(run(t_swept(), fixed(), empty())) == True{} : Bool}

law publish_lands_here:
  {rs_of(run(t_inflight(), fixed(), empty())) == [Res{10n, [1n]}] : +List<Res>}

def in_flight_lost_today():
  {==}

def publish_refused_today():
  {==}

def in_flight_kept_here():
  {==}

def publish_lands_here():
  {==}

# --- (d) a reader never finds a result that names a blob that is gone ------------------------------

# Blob 1 is stored and published, a sweep with a budget of 0 evicts it, and a reader then looks the
# result up.
def t_window() -> +List<Op>:
  [StoreBlob{1n, 1n}, Publish{10n, [1n]}, SweepObjects{0n}, Use{10n}]

# The pin (8cd8f6b) and HEAD: step 1 of the sweep took the blob and step 2 has not yet dropped the result, so
# the reader finds a result that names a blob that is gone: the miss `cached action is missing
# blob`, and a compile. This is the counterexample to no_dangling.
law window_today:
  {dang_of(run(t_window(), head(), empty())) == 1n : Nat}

# With the sweep dropping the results before it takes the blobs, the reader finds no result.
law window_closed:
  {dang_of(run(t_window(), fixed(), empty())) == 0n : Nat}

law window_drops:
  {rs_of(run(t_window(), fixed(), empty())) == [] : +List<Res>}

def window_today():
  {==}

def window_closed():
  {==}

def window_drops():
  {==}