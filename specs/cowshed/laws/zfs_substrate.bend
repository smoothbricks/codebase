# The ZFS substrate's three decisions (specs/cowshed/09_substrates.md, "Privileged helper" and
# selection; 01b_zfs.md, "One pool, one root"): what the root helper admits, which substrate a
# project's main entry selects, and what `cowshed setup` does with the root that serves the user.
# All are pure cores; the helper's `zfs get` and `zfs` runs, the store's reads, and setup's reads
# and admin authorization are a shell that gathers their inputs and executes their outputs.
#
# Admission (cowshed-helper src/verb.rs `plan` and `admit`): the helper serves the root
# `<parent>/cowshed` only when the root's `cowshed:owner` is set locally to the caller's uid, and
# runs a verb only when every dataset it names, a snapshot's dataset included, lies strictly below
# that root; `promote` takes only a build dataset, `<root>/projects/<owner>/<repo>/build/<id>`.
#
# Selection (cowshed-core src/storage/directory/zfs.rs `select`): the project's main entry in the
# store is a regular file (an APFS image project), a place where a dataset is mounted or assigned
# (that dataset, `<root>/projects/<owner>/<repo>/main`, names the root), a home directory with
# nothing mounted or assigned (the serving root that holds the project's main by name), or absent
# (not adopted: datasets only when adoption asks, below the root the project's `.cowshed.toml`
# declares when that root names the caller, else below the one root that names the caller).
#
# The root (cowshed-core src/storage/bootstrap/zfs_root.rs `named_parent` and `root_steps`): a
# parent named on the command line and one `.cowshed.toml` declares must agree. Without zfs setup
# does nothing. A named root is read alone: created when absent, taken when its `cowshed:owner` is
# set locally to the caller and, on macOS, its four com.apple properties are `off` locally, and
# otherwise refused naming exactly the wrong properties. With nothing named, each serving root is
# read and taken or refused, one `zfs get` no longer finds is refused, and with none the operator
# is asked to name a pool: nothing unnamed is ever created.
#
# A dataset name is its list of components, each a Nat: 0n is `cowshed`, 1n `projects`, 2n
# `main`, 3n `build`, 4n `.trash` (an id that is hidden, never a build volume), and any other n a
# plain component. A root `R{pool, parents}` is `pool/parents…/cowshed`, so every root is valid.
#
# Check: bun bend2/main.ts zfs_substrate.bend --verdict
# Bound to the code by cowshed-helper src/laws.rs (`plan`, `admit` against `plan`, `admits`),
# cowshed-core src/storage/directory/zfs/laws.rs (`select` against `select`) and
# src/storage/bootstrap/zfs_root/laws.rs (`root_steps` and `named_parent` against `root_steps`,
# `reads` and `parent`), which run the emitted models (src/laws/model.rs, zfs/laws/model.rs,
# zfs_root/laws/model.rs) and the code on the same inputs.
import Base

type Owner is Data:
  Unread{}
  Read{value: Nat, local: Bool}

type R is Data:
  R{pool: Nat, parents: +List<Nat>}

type Verb is Data:
  VCheck{}
  VCreate{d: +List<Nat>}
  VClone{origin: +List<Nat>, snap: Nat, d: +List<Nat>}
  VSnapshot{d: +List<Nat>, snap: Nat}
  VDestroy{d: +List<Nat>}
  VDestroySnap{d: +List<Nat>, snap: Nat, defer: Bool}
  VRename{src: +List<Nat>, dst: +List<Nat>}
  VRenameSnap{d: +List<Nat>, a: Nat, b: Nat}
  VPromote{d: +List<Nat>}
  VMount{d: +List<Nat>}
  VUnmount{d: +List<Nat>, force: Bool}
  VSet{d: +List<Nat>}

type Plan is Data:
  Refused{}
  Runs{names: +List<+List<Nat>>}

type Main is Data:
  MFile{}
  MPlace{d: +List<Nat>}
  MHome{}
  MAbsent{}

type Sel is Data:
  Images{}
  Datasets{root: +List<Nat>}
  NoRoot{}
  Several{roots: +List<R>}
  Unserving{root: R}

type Found is Data:
  Nothing{}
  Named{d: +List<Nat>}

# A root named by the operator, on the command line or in `.cowshed.toml`, or none.
type Named is Data:
  Unnamed{}
  Name{root: R}

# A parent named on the command line and one `.cowshed.toml` declares: the one they agree on, or
# both, refused (zfs_root.rs `named_parent`).
type Parent is Data:
  Agreed{named: Named}
  Disagree{cli: R, declared: R}

# What `zfs get` reads of a root: nothing there, or its `cowshed:owner` and its four com.apple
# properties, each unread or a value and whether it is set locally; a com.apple value 0n is `off`.
type Ev is Data:
  Absent{}
  Present{owner: Owner, devdisk: Owner, browse: Owner, mimic: Owner, ignoreowner: Owner}

# Which properties of an existing root are wrong, in the order the remedy sets them.
type Wrong is Data:
  W{owner: Bool, devdisk: Bool, browse: Bool, mimic: Bool, ignoreowner: Bool}

# zfs_root.rs `RootStep`: `SGone` is the refusal of a serving root `zfs get` no longer finds.
type Step is Data:
  SNothing{}
  SNameAPool{}
  SCreate{root: +List<Nat>, apple: Bool}
  STake{root: +List<Nat>}
  SRefuse{root: +List<Nat>, wrong: Wrong}
  SGone{root: +List<Nat>}

# --- Bool, Nat and list helpers ------------------------------------------------

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

def or(a: Bool, b: Bool) -> Bool:
  match a:
    case True{}:
      True{}
    case False{}:
      b

def IsTrue(b: Bool) -> Data:
  match b:
    case True{}:
      Unit
    case False{}:
      Empty

def cat(xs: +List<Nat>, ys: +List<Nat>) -> +List<Nat>:
  match xs:
    case Nil{}:
      ys
    case Con{h, t}:
      h <> cat(t, ys)

# --- names ------------------------------------------------------------------------

# the components of a root, `pool/parents…/cowshed`
def root_names(r: R) -> +List<Nat>:
  match r:
    case R{p, ps}:
      p <> cat(ps, [0n])

def eq_names(xs: +List<Nat>, ys: +List<Nat>) -> Bool:
  match xs ys:
    case Nil{} Nil{}:
      True{}
    case Nil{} Con{h, t}:
      False{}
    case Con{h, t} Nil{}:
      False{}
    case Con{a, s} Con{b, u}:
      and(same(a, b), eq_names(s, u))

# `r` is one of `rs`, by name
def member(+r: R, rs: +List<R>) -> Bool:
  match rs:
    case Nil{}:
      False{}
    case Con{x, t}:
      or(eq_names(root_names(r), root_names(x)), member(r, t))

# `d` lies strictly below `root`: the root's components, then at least one more
# (name.rs `Root::holds`)
def below(root: +List<Nat>, d: +List<Nat>) -> Bool:
  match root d:
    case Nil{} Nil{}:
      False{}
    case Nil{} Con{h, t}:
      True{}
    case Con{h, t} Nil{}:
      False{}
    case Con{a, s} Con{b, u}:
      and(same(a, b), below(s, u))

def all_below(+root: +List<Nat>, ds: +List<+List<Nat>>) -> Bool:
  match ds:
    case Nil{}:
      True{}
    case Con{d, t}:
      and(below(root, d), all_below(root, t))

# what follows the root of a build dataset: projects/<owner>/<repo>/build/<id>, the id not hidden
def build_tail(xs: +List<Nat>) -> Bool:
  match xs:
    case Con{p, Con{o, Con{r, Con{b, Con{id, Nil{}}}}}}:
      and(same(p, 1n), and(same(b, 3n), not(same(id, 4n))))
    case _:
      False{}

# `d` is a build dataset of `root` (name.rs `Root::holds_build_volume`)
def build_at(root: +List<Nat>, d: +List<Nat>) -> Bool:
  match root d:
    case Nil{} xs:
      build_tail(xs)
    case Con{h, t} Nil{}:
      False{}
    case Con{a, s} Con{b, u}:
      and(same(a, b), build_at(s, u))

# --- admission ----------------------------------------------------------------------

# the datasets a verb names, in the order its `zfs` command names them; a snapshot
# names its dataset
def args(v: Verb) -> +List<+List<Nat>>:
  match v:
    case VCheck{}:
      []
    case VCreate{d}:
      [d]
    case VClone{o, s, d}:
      [o, d]
    case VSnapshot{d, s}:
      [d]
    case VDestroy{d}:
      [d]
    case VDestroySnap{d, s, f}:
      [d]
    case VRename{a, b}:
      [a, b]
    case VRenameSnap{+d, a, b}:
      [d, d]
    case VPromote{d}:
      [d]
    case VMount{d}:
      [d]
    case VUnmount{d, f}:
      [d]
    case VSet{d}:
      [d]

# what a verb asks of its datasets beyond lying below the root
def shape(+root: +List<Nat>, v: Verb) -> Bool:
  match v:
    case VPromote{d}:
      build_at(root, d)
    case _:
      True{}

def guard(ok: Bool, ns: +List<+List<Nat>>) -> Plan:
  match ok:
    case True{}:
      Runs{ns}
    case False{}:
      Refused{}

# verb.rs `plan`: the one `zfs` command, naming only datasets below the root
def plan(+root: R, +v: Verb) -> Plan:
  guard(and(all_below(root_names(root), args(v)), shape(root_names(root), v)), args(v))

def named(p: Plan) -> +List<+List<Nat>>:
  match p:
    case Refused{}:
      []
    case Runs{ns}:
      ns

def planned(p: Plan) -> Bool:
  match p:
    case Refused{}:
      False{}
    case Runs{ns}:
      True{}

# verb.rs `admit`: the root's owner, read with its source, names the caller only when it
# is set locally to the caller's uid
def names(o: Owner, c: Nat) -> Bool:
  match o:
    case Unread{}:
      False{}
    case Read{v, l}:
      and(l, same(v, c))

# the helper runs a verb: it plans, and the root names the caller
def admits(+root: R, o: Owner, +c: Nat, +v: Verb) -> Bool:
  and(names(o, c), planned(plan(root, v)))

# --- selection ----------------------------------------------------------------------

# the four components after a root in a main dataset: projects/<owner>/<repo>/main
def main_tail(xs: +List<Nat>) -> Bool:
  match xs:
    case Con{p, Con{o, Con{r, Con{m, Nil{}}}}}:
      and(same(p, 1n), same(m, 2n))
    case _:
      False{}

def pre(+h: Nat, f: Found) -> Found:
  match f:
    case Nothing{}:
      Nothing{}
    case Named{x}:
      Named{h <> x}

def root_of.if(+h: Nat, b: Bool, f: Found) -> Found:
  match b:
    case True{}:
      Named{[h]}
    case False{}:
      pre(h, f)

# the components before a main dataset's last four (zfs.rs `root_of_main`)
def root_of(d: +List<Nat>) -> Found:
  match d:
    case Nil{}:
      Nothing{}
    case Con{+h, +t}:
      root_of.if(h, main_tail(t), root_of(t))

# a root's name: at least two components, the last `cowshed` (name.rs `Root::parse`)
def last0(t: +List<Nat>, h: Nat) -> Bool:
  match t:
    case Nil{}:
      same(h, 0n)
    case Con{a, u}:
      last0(u, a)

def root_ok(xs: +List<Nat>) -> Bool:
  match xs:
    case Nil{}:
      False{}
    case Con{p, Nil{}}:
      False{}
    case Con{p, Con{h, u}}:
      last0(u, h)

def ok_sel(b: Bool, r: +List<Nat>) -> Sel:
  match b:
    case True{}:
      Datasets{r}
    case False{}:
      Images{}

def root_sel(f: Found) -> Sel:
  match f:
    case Nothing{}:
      Images{}
    case Named{+r}:
      ok_sel(root_ok(r), r)

# the one root of the roots given, none, or several named
def one(rs: +List<R>) -> Sel:
  match rs:
    case Nil{}:
      NoRoot{}
    case Con{r, Nil{}}:
      Datasets{root_names(r)}
    case Con{a, Con{b, t}}:
      Several{a <> b <> t}

# a home main held by no root keeps no datasets
def held(rs: +List<R>) -> Sel:
  match rs:
    case Nil{}:
      Images{}
    case Con{r, t}:
      one(r <> t)

# a declared root serves only when it names the caller (zfs.rs `Unserved::DeclaredNotServing`)
def served.if(b: Bool, +r: R) -> Sel:
  match b:
    case True{}:
      Datasets{root_names(r)}
    case False{}:
      Unserving{r}

def declared_or_one(+d: Named, rs: +List<R>) -> Sel:
  match d:
    case Unnamed{}:
      one(rs)
    case Name{+r}:
      served.if(member(r, rs), r)

def asked(a: Bool, +d: Named, rs: +List<R>) -> Sel:
  match a:
    case True{}:
      declared_or_one(d, rs)
    case False{}:
      Images{}

# zfs.rs `select`: `d` is the root the project's `.cowshed.toml` declares, `serving` are the roots
# that name the caller, `holding` those of them that hold the project's main dataset by name
def select(m: Main, a: Bool, +d: Named, serving: +List<R>, holding: +List<R>) -> Sel:
  match m:
    case MFile{}:
      Images{}
    case MPlace{d}:
      root_sel(root_of(d))
    case MHome{}:
      held(holding)
    case MAbsent{}:
      asked(a, d, serving)

# the selection reads the serving roots only for an absent main that adoption asks onto
# datasets, and the holding roots only for a home main
def needs_serving(m: Main, a: Bool) -> Bool:
  match m:
    case MAbsent{}:
      a
    case _:
      False{}

def needs_holding(m: Main) -> Bool:
  match m:
    case MHome{}:
      True{}
    case _:
      False{}

def is_file(m: Main) -> Bool:
  match m:
    case MFile{}:
      True{}
    case _:
      False{}

def is_images(s: Sel) -> Bool:
  match s:
    case Images{}:
      True{}
    case _:
      False{}

# --- the root setup serves --------------------------------------------------------------

# zfs_root.rs `named_parent`: a parent named once is the parent; two must agree
def agree.if(b: Bool, x: R, y: R) -> Parent:
  match b:
    case True{}:
      Agreed{Name{x}}
    case False{}:
      Disagree{x, y}

def parent(+c: Named, +d: Named) -> Parent:
  match c d:
    case Unnamed{} d0:
      Agreed{d0}
    case Name{x} Unnamed{}:
      Agreed{Name{x}}
    case Name{+x} Name{+y}:
      agree.if(eq_names(root_names(x), root_names(y)), x, y)

def is_disagree(p: Parent) -> Bool:
  match p:
    case Agreed{n}:
      False{}
    case Disagree{x, y}:
      True{}

def differ_from(+x: R, d: Named) -> Bool:
  match d:
    case Unnamed{}:
      False{}
    case Name{y}:
      not(eq_names(root_names(x), root_names(y)))

# both are named, and they name different roots
def differ(c: Named, +d: Named) -> Bool:
  match c:
    case Unnamed{}:
      False{}
    case Name{x}:
      differ_from(x, d)

# a com.apple property is right on the root when it is `off` (0n), set locally
def off(p: Owner) -> Bool:
  match p:
    case Unread{}:
      False{}
    case Read{v, l}:
      and(l, same(v, 0n))

# on macOS (`a`) a com.apple property that is not `off` locally is wrong; elsewhere none is
def apple_wrong(a: Bool, p: Owner) -> Bool:
  match a:
    case True{}:
      not(off(p))
    case False{}:
      False{}

# the properties of an existing root that are wrong for the caller `u`; the owner is the
# helper's own rule (`names`)
def wrong(o: Owner, dk: Owner, bw: Owner, mc: Owner, io: Owner, +u: Nat, +a: Bool) -> Wrong:
  W{not(names(o, u)), apple_wrong(a, dk), apple_wrong(a, bw), apple_wrong(a, mc), apple_wrong(a, io)}

def any(w: Wrong) -> Bool:
  match w:
    case W{o, dk, bw, mc, io}:
      or(o, or(dk, or(bw, or(mc, io))))

def verdict.if(bad: Bool, +r: +List<Nat>, w: Wrong) -> Step:
  match bad:
    case True{}:
      SRefuse{r, w}
    case False{}:
      STake{r}

# an existing root: taken when nothing is wrong, else refused naming each wrong property
def judge(+r: +List<Nat>, +o: Owner, +dk: Owner, +bw: Owner, +mc: Owner, +io: Owner, +u: Nat, +a: Bool) -> Step:
  verdict.if(any(wrong(o, dk, bw, mc, io, u, a)), r, wrong(o, dk, bw, mc, io, u, a))

# the named root: created when absent, with the com.apple properties on macOS, else judged
def named_step(+r: +List<Nat>, +e: Ev, +u: Nat, +a: Bool) -> Step:
  match e:
    case Absent{}:
      SCreate{r, a}
    case Present{o, dk, bw, mc, io}:
      judge(r, o, dk, bw, mc, io, u, a)

# a serving root: nothing named it, so one `zfs get` no longer finds is refused, never created
def served_step(+r: +List<Nat>, +e: Ev, +u: Nat, +a: Bool) -> Step:
  match e:
    case Absent{}:
      SGone{r}
    case Present{o, dk, bw, mc, io}:
      judge(r, o, dk, bw, mc, io, u, a)

# the evidence read for the next root; a read past the end finds nothing
def first(es: +List<Ev>) -> Ev:
  match es:
    case Nil{}:
      Absent{}
    case Con{e, t}:
      e

def rest(es: +List<Ev>) -> +List<Ev>:
  match es:
    case Nil{}:
      []
    case Con{e, t}:
      t

# one step per serving root, each from the evidence read for it, in order
def served_steps(rs: +List<R>, +es: +List<Ev>, +u: Nat, +a: Bool) -> +List<Step>:
  match rs:
    case Nil{}:
      []
    case Con{r, t}:
      served_step(root_names(r), first(es), u, a) <> served_steps(t, rest(es), u, a)

def unnamed_steps(rs: +List<R>, +es: +List<Ev>, +u: Nat, +a: Bool) -> +List<Step>:
  match rs:
    case Nil{}:
      [SNameAPool{}]
    case Con{r, t}:
      served_steps(r <> t, es, u, a)

def zfs_steps(+n: Named, s: +List<R>, +es: +List<Ev>, +u: Nat, +a: Bool) -> +List<Step>:
  match n:
    case Unnamed{}:
      unnamed_steps(s, es, u, a)
    case Name{r}:
      [named_step(root_names(r), first(es), u, a)]

# zfs_root.rs `root_steps`: `z` is whether the host has zfs, `n` the named root, `s` the roots
# whose `cowshed:owner` names the caller, `u` the caller, `a` whether the host is macOS, and `es`
# the evidence read for the roots `reads` names, in its order
def root_steps(z: Bool, +n: Named, s: +List<R>, +u: Nat, +a: Bool, +es: +List<Ev>) -> +List<Step>:
  match z:
    case False{}:
      [SNothing{}]
    case True{}:
      zfs_steps(n, s, es, u, a)

def all_names(rs: +List<R>) -> +List<+List<Nat>>:
  match rs:
    case Nil{}:
      []
    case Con{r, t}:
      root_names(r) <> all_names(t)

def named_reads(+n: Named, s: +List<R>) -> +List<+List<Nat>>:
  match n:
    case Unnamed{}:
      all_names(s)
    case Name{r}:
      [root_names(r)]

# the roots whose evidence `root_steps` reads, in order
def reads(z: Bool, +n: Named, s: +List<R>) -> +List<+List<Nat>>:
  match z:
    case False{}:
      []
    case True{}:
      named_reads(n, s)

def is_create(s: Step) -> Bool:
  match s:
    case SCreate{r, a}:
      True{}
    case _:
      False{}

def creates_none(ss: +List<Step>) -> Bool:
  match ss:
    case Nil{}:
      True{}
    case Con{s, t}:
      and(not(is_create(s)), creates_none(t))

# a step creates nothing, or creates `r`
def only_made(+r: +List<Nat>, s: Step) -> Bool:
  match s:
    case SCreate{x, a}:
      eq_names(x, r)
    case _:
      True{}

def creates_only(+r: +List<Nat>, ss: +List<Step>) -> Bool:
  match ss:
    case Nil{}:
      True{}
    case Con{s, t}:
      and(only_made(r, s), creates_only(r, t))

# what may be created: nothing when nothing is named, else only the named root
def created_ok(+n: Named, ss: +List<Step>) -> Bool:
  match n:
    case Unnamed{}:
      creates_none(ss)
    case Name{r}:
      creates_only(root_names(r), ss)

# --- lemmas ---------------------------------------------------------------------------

def same_refl(+a: Nat) -> IsTrue(same(a, a)):
  match a:
    case 0n:
      Unit{}
    case 1n+p:
      same_refl(p)

def and_intro(+a: Bool, +b: Bool) -> IsTrue(a) -> IsTrue(b) -> IsTrue(and(a, b)):
  match a b:
    case True{} True{}:
      i => j => Unit{}
    case True{} False{}:
      i => j => j
    case False{} b0:
      i => j => i

def and_eq_false(+a: Bool, +b: Bool) -> {b == False{} : Bool} -> {and(a, b) == False{} : Bool}:
  match a:
    case True{}:
      e => e
    case False{}:
      e => {==}

def and_left_false(+a: Bool, +b: Bool) -> IsTrue(not(a)) -> {and(a, b) == False{} : Bool}:
  match a:
    case True{}:
      i => Empty.absurd({and(True{}, b) == False{} : Bool}, i)
    case False{}:
      i => {==}

def and_true(+a: Bool, +b: Bool) -> IsTrue(a) -> {and(a, b) == b : Bool}:
  match a:
    case True{}:
      i => {==}
    case False{}:
      i => Empty.absurd({and(False{}, b) == b : Bool}, i)

def guard_true(+b: Bool, +ns: +List<+List<Nat>>) -> IsTrue(b) -> {guard(b, ns) == Runs{ns} : Plan}:
  match b:
    case True{}:
      i => {==}
    case False{}:
      i => Empty.absurd({guard(False{}, ns) == Runs{ns} : Plan}, i)

def below_cat(+xs: +List<Nat>, +x: Nat, +t: +List<Nat>) -> IsTrue(below(xs, cat(xs, x <> t))):
  match xs:
    case Nil{}:
      Unit{}
    case Con{h, u}:
      and_intro(same(h, h), below(u, cat(u, x <> t)), same_refl(h), below_cat(u, x, t))

def build_cat(+xs: +List<Nat>, +tail: +List<Nat>) -> IsTrue(build_tail(tail)) -> IsTrue(build_at(xs, cat(xs, tail))):
  match xs:
    case Nil{}:
      i => i
    case Con{h, u}:
      i => and_intro(same(h, h), build_at(u, cat(u, tail)), same_refl(h), build_cat(u, tail, i))

def below_self(+xs: +List<Nat>) -> {below(xs, xs) == False{} : Bool}:
  match xs:
    case Nil{}:
      {==}
    case Con{h, u}:
      and_eq_false(same(h, h), below(u, u), below_self(u))

def guard_below(+rn: +List<Nat>, +ns: +List<+List<Nat>>, +b: Bool, +k: Bool)
  -> {all_below(rn, ns) == b : Bool}
  -> {all_below(rn, named(guard(and(b, k), ns))) == True{} : Bool}:
  match b k:
    case True{} True{}:
      e => e
    case True{} False{}:
      e => {==}
    case False{} k0:
      e => {==}

def guard_shape(+b1: Bool, +b2: Bool, +ns: +List<+List<Nat>>)
  -> {and(planned(guard(and(b1, b2), ns)), not(b2)) == False{} : Bool}:
  match b1 b2:
    case True{} True{}:
      {==}
    case True{} False{}:
      {==}
    case False{} b0:
      {==}

def last0_cat(+u: +List<Nat>, +h: Nat) -> IsTrue(last0(cat(u, [0n]), h)):
  match u:
    case Nil{}:
      Unit{}
    case Con{a, t}:
      last0_cat(t, a)

def root_names_ok(+r: R) -> IsTrue(root_ok(root_names(r))):
  match r:
    case R{p, Nil{}}:
      Unit{}
    case R{p, Con{a, t}}:
      last0_cat(t, a)

# a dataset with five or more components is not the four after a root
def long_tail(+h: Nat, +t: +List<Nat>, +o: Nat, +n: Nat) -> IsTrue(not(main_tail(h <> cat(t, [1n, o, n, 2n])))):
  match t:
    case Nil{}:
      Unit{}
    case Con{a, Nil{}}:
      Unit{}
    case Con{a, Con{b, Nil{}}}:
      Unit{}
    case Con{a, Con{b, Con{c, Nil{}}}}:
      Unit{}
    case Con{a, Con{b, Con{c, Con{d, r}}}}:
      Unit{}

def pre_named(+h: Nat, +b: Bool, +f: Found, +x: +List<Nat>)
  -> IsTrue(not(b)) -> {f == Named{x} : Found} -> {root_of.if(h, b, f) == Named{h <> x} : Found}:
  match b:
    case True{}:
      i => e => Empty.absurd({root_of.if(h, True{}, f) == Named{h <> x} : Found}, i)
    case False{}:
      i => e => Equal.cong(Found, Found, y => pre(h, y), f, Named{x}, e)

def main_root(+t: +List<Nat>, +h: Nat, +o: Nat, +n: Nat)
  -> {root_of(cat(h <> t, [1n, o, n, 2n])) == Named{h <> t} : Found}:
  match t:
    case Nil{}:
      {==}
    case Con{a, u}:
      pre_named(h, main_tail(a <> cat(u, [1n, o, n, 2n])), root_of(a <> cat(u, [1n, o, n, 2n])), a <> u,
        long_tail(a, u, o, n), main_root(u, a, o, n))

def ok_true(+b: Bool, +r: +List<Nat>) -> IsTrue(b) -> {ok_sel(b, r) == Datasets{r} : Sel}:
  match b:
    case True{}:
      i => {==}
    case False{}:
      i => Empty.absurd({ok_sel(False{}, r) == Datasets{r} : Sel}, i)

def same_eq(+a: Nat) -> {same(a, a) == True{} : Bool}:
  match a:
    case 0n:
      {==}
    case 1n+p:
      same_eq(p)

def eq_refl(+xs: +List<Nat>) -> {eq_names(xs, xs) == True{} : Bool}:
  match xs:
    case Nil{}:
      {==}
    case Con{h, t}:
      Equal.trans(Bool, and(same(h, h), eq_names(t, t)), eq_names(t, t), True{},
        Equal.cong(Bool, Bool, x => and(x, eq_names(t, t)), same(h, h), True{}, same_eq(h)),
        eq_refl(t))

def or_left(+x: Bool, +y: Bool) -> IsTrue(x) -> IsTrue(or(x, y)):
  match x:
    case True{}:
      i => Unit{}
    case False{}:
      i => Empty.absurd(IsTrue(or(False{}, y)), i)

def or_right(+x: Bool, +y: Bool) -> IsTrue(y) -> IsTrue(or(x, y)):
  match x:
    case True{}:
      j => Unit{}
    case False{}:
      j => j

def served_true(+b: Bool, +r: R) -> IsTrue(b) -> {served.if(b, r) == Datasets{root_names(r)} : Sel}:
  match b:
    case True{}:
      i => {==}
    case False{}:
      i => Empty.absurd({served.if(False{}, r) == Datasets{root_names(r)} : Sel}, i)

def served_false(+b: Bool, +r: R) -> IsTrue(not(b)) -> {served.if(b, r) == Unserving{r} : Sel}:
  match b:
    case True{}:
      i => Empty.absurd({served.if(True{}, r) == Unserving{r} : Sel}, i)
    case False{}:
      i => {==}

def agree_disagrees(+b: Bool, +x: R, +y: R) -> {is_disagree(agree.if(b, x, y)) == not(b) : Bool}:
  match b:
    case True{}:
      {==}
    case False{}:
      {==}

def agree_false(+b: Bool, +x: R, +y: R) -> IsTrue(not(b)) -> {agree.if(b, x, y) == Disagree{x, y} : Parent}:
  match b:
    case True{}:
      i => Empty.absurd({agree.if(True{}, x, y) == Disagree{x, y} : Parent}, i)
    case False{}:
      i => {==}

def verdict_creates_nothing(+b: Bool, +r: +List<Nat>, +w: Wrong) -> {is_create(verdict.if(b, r, w)) == False{} : Bool}:
  match b:
    case True{}:
      {==}
    case False{}:
      {==}

def verdict_made_only(+b: Bool, +r: +List<Nat>, +w: Wrong) -> {only_made(r, verdict.if(b, r, w)) == True{} : Bool}:
  match b:
    case True{}:
      {==}
    case False{}:
      {==}

def verdict_refuses(+b: Bool, +r: +List<Nat>, +w: Wrong) -> IsTrue(b) -> {verdict.if(b, r, w) == SRefuse{r, w} : Step}:
  match b:
    case True{}:
      i => {==}
    case False{}:
      i => Empty.absurd({verdict.if(False{}, r, w) == SRefuse{r, w} : Step}, i)

def served_creates_nothing(+r: +List<Nat>, +e: Ev, +u: Nat, +a: Bool) -> {is_create(served_step(r, e, u, a)) == False{} : Bool}:
  match e:
    case Absent{}:
      {==}
    case Present{o, dk, bw, mc, io}:
      verdict_creates_nothing(any(wrong(o, dk, bw, mc, io, u, a)), r, wrong(o, dk, bw, mc, io, u, a))

def served_none(rs: +List<R>, +es: +List<Ev>, +u: Nat, +a: Bool) -> {creates_none(served_steps(rs, es, u, a)) == True{} : Bool}:
  match rs:
    case Nil{}:
      {==}
    case Con{+r, +t}:
      Equal.trans(Bool,
        and(not(is_create(served_step(root_names(r), first(es), u, a))), creates_none(served_steps(t, rest(es), u, a))),
        creates_none(served_steps(t, rest(es), u, a)),
        True{},
        Equal.cong(Bool, Bool, x => and(not(x), creates_none(served_steps(t, rest(es), u, a))),
          is_create(served_step(root_names(r), first(es), u, a)), False{},
          served_creates_nothing(root_names(r), first(es), u, a)),
        served_none(t, rest(es), u, a))

def named_made_only(+r: +List<Nat>, +e: Ev, +u: Nat, +a: Bool) -> {only_made(r, named_step(r, e, u, a)) == True{} : Bool}:
  match e:
    case Absent{}:
      eq_refl(r)
    case Present{o, dk, bw, mc, io}:
      verdict_made_only(any(wrong(o, dk, bw, mc, io, u, a)), r, wrong(o, dk, bw, mc, io, u, a))

# === laws ==============================================================================

# --- admission ---------------------------------------------------------------------------

# Only a root whose owner is set locally names the caller: an inherited, received or default
# owner admits no verb, whatever its value.
law an_inherited_owner_admits_nothing:
  for +root: R
  for +val: Nat
  for +c: Nat
  for +v: Verb
  {admits(root, Read{val, False{}}, c, v) == False{} : Bool}

def an_inherited_owner_admits_nothing(root, val, c, v):
  {==}

# An owner the helper cannot read admits no verb.
law an_unread_owner_admits_nothing:
  for +root: R
  for +c: Nat
  for +v: Verb
  {admits(root, Unread{}, c, v) == False{} : Bool}

def an_unread_owner_admits_nothing(root, c, v):
  {==}

# A local owner that is another uid admits no verb.
law another_uid_admits_nothing:
  for +root: R
  for +val: Nat
  for +c: Nat
  for +v: Verb
  for +i: IsTrue(not(same(val, c)))
  {admits(root, Read{val, True{}}, c, v) == False{} : Bool}

def another_uid_admits_nothing(root, val, c, v, i):
  and_left_false(same(val, c), planned(plan(root, v)), i)

# The caller's own root admits exactly what the verb plans.
law the_callers_root_admits_what_it_plans:
  for +root: R
  for +c: Nat
  for +v: Verb
  {admits(root, Read{c, True{}}, c, v) == planned(plan(root, v)) : Bool}

def the_callers_root_admits_what_it_plans(root, c, v):
  and_true(same(c, c), planned(plan(root, v)), same_refl(c))

# The admission check the binding drives: nothing is admitted unless the owner names the caller.
law admit_check:
  for +root: R
  for +o: Owner
  for +c: Nat
  for +v: Verb
  {and(admits(root, o, c, v), not(names(o, c))) == False{} : Bool}

def admit_names(+b: Bool, +k: Bool) -> {and(and(b, k), not(b)) == False{} : Bool}:
  match b k:
    case True{} True{}:
      {==}
    case True{} False{}:
      {==}
    case False{} k0:
      {==}

def admit_check(root, o, c, v):
  admit_names(names(o, c), planned(plan(root, v)))

# --- containment -------------------------------------------------------------------------

# Every dataset an admitted plan names, a snapshot's dataset included, lies strictly below the
# root. The binding drives this law.
law a_plan_stays_below_its_root:
  for +root: R
  for +v: Verb
  {all_below(root_names(root), named(plan(root, v))) == True{} : Bool}

def a_plan_stays_below_its_root(root, v):
  guard_below(root_names(root), args(v), all_below(root_names(root), args(v)), shape(root_names(root), v), {==})

# The root is never below itself, so no plan names it.
law the_root_is_not_below_itself:
  for +root: R
  {below(root_names(root), root_names(root)) == False{} : Bool}

def the_root_is_not_below_itself(root):
  below_self(root_names(root))

# No parent of a name is below it: a root's parents are never an argument.
law no_parent_is_below_the_root:
  for +p: +List<Nat>
  for +x: Nat
  for +t: +List<Nat>
  {below(cat(p, x <> t), p) == False{} : Bool}

def no_parent_is_below_the_root(p, x, t):
  match p:
    case Nil{}:
      {==}
    case Con{h, u}:
      and_eq_false(same(h, h), below(cat(u, x <> t), u), no_parent_is_below_the_root(u, x, t))

# No sibling of the root, nor anything below one, is below it: `p/x…` with x not `cowshed`
# for the root `p/cowshed`.
law no_sibling_is_below_the_root:
  for +p: +List<Nat>
  for +c: Nat
  for +x: Nat
  for +t: +List<Nat>
  for +i: IsTrue(not(same(c, x)))
  {below(cat(p, [c]), cat(p, x <> t)) == False{} : Bool}

def no_sibling_is_below_the_root(p, c, x, t, i):
  match p:
    case Nil{}:
      and_left_false(same(c, x), below([], t), i)
    case Con{h, u}:
      and_eq_false(same(h, h), below(cat(u, [c]), cat(u, x <> t)), no_sibling_is_below_the_root(u, c, x, t, i))

# --- promote ------------------------------------------------------------------------------

# Promote is admitted only for a build dataset of the root.
law promote_only_a_build_dataset:
  for +root: R
  for +d: +List<Nat>
  {and(planned(plan(root, VPromote{d})), not(build_at(root_names(root), d))) == False{} : Bool}

def promote_only_a_build_dataset(root, d):
  guard_shape(all_below(root_names(root), [d]), build_at(root_names(root), d), [d])

# Every build dataset, `<root>/projects/<owner>/<repo>/build/<id>` with an id that is not
# hidden, is promoted.
law a_build_dataset_is_promoted:
  for +root: R
  for +o: Nat
  for +n: Nat
  for +id: Nat
  for +i: IsTrue(not(same(id, 4n)))
  {plan(root, VPromote{cat(root_names(root), [1n, o, n, 3n, id])}) == Runs{[cat(root_names(root), [1n, o, n, 3n, id])]} : Plan}

def a_build_dataset_is_promoted(root, o, n, id, i):
  guard_true(
    and(and(below(root_names(root), cat(root_names(root), [1n, o, n, 3n, id])), True{}),
      build_at(root_names(root), cat(root_names(root), [1n, o, n, 3n, id]))),
    [cat(root_names(root), [1n, o, n, 3n, id])],
    and_intro(
      and(below(root_names(root), cat(root_names(root), [1n, o, n, 3n, id])), True{}),
      build_at(root_names(root), cat(root_names(root), [1n, o, n, 3n, id])),
      and_intro(below(root_names(root), cat(root_names(root), [1n, o, n, 3n, id])), True{},
        below_cat(root_names(root), 1n, [o, n, 3n, id]), Unit{}),
      build_cat(root_names(root), [1n, o, n, 3n, id], i)))

# --- selection ----------------------------------------------------------------------------

# A project whose main is a regular file is an image project, whatever roots exist, whatever
# adoption asks and whatever the project declares.
law a_file_main_is_an_image_project:
  for +a: Bool
  for +d: Named
  for +serving: +List<R>
  for +holding: +List<R>
  {select(MFile{}, a, d, serving, holding) == Images{} : Sel}

def a_file_main_is_an_image_project(a, d, serving, holding):
  {==}

# The selection check the binding drives.
law a_file_main_check:
  for +m: Main
  for +a: Bool
  for +d: Named
  for +serving: +List<R>
  for +holding: +List<R>
  {and(is_file(m), not(is_images(select(m, a, d, serving, holding)))) == False{} : Bool}

def a_file_main_check(m, a, d, serving, holding):
  match m:
    case MFile{}:
      {==}
    case MPlace{x}:
      {==}
    case MHome{}:
      {==}
    case MAbsent{}:
      {==}

# A mounted or assigned main dataset, `<root>/projects/<owner>/<repo>/main`, names its root,
# whatever roots serve, whatever adoption asks and whatever the project declares.
law a_main_dataset_names_its_root:
  for +root: R
  for +o: Nat
  for +n: Nat
  for +a: Bool
  for +d: Named
  for +serving: +List<R>
  for +holding: +List<R>
  {select(MPlace{cat(root_names(root), [1n, o, n, 2n])}, a, d, serving, holding) == Datasets{root_names(root)} : Sel}

def a_main_dataset_names_its_root(root, o, n, a, d, serving, holding):
  match root:
    case R{p, ps}:
      Equal.trans(Sel,
        root_sel(root_of(cat(p <> cat(ps, [0n]), [1n, o, n, 2n]))),
        root_sel(Named{p <> cat(ps, [0n])}),
        Datasets{p <> cat(ps, [0n])},
        Equal.cong(Found, Sel, f => root_sel(f),
          root_of(cat(p <> cat(ps, [0n]), [1n, o, n, 2n])), Named{p <> cat(ps, [0n])},
          main_root(cat(ps, [0n]), p, o, n)),
        ok_true(root_ok(p <> cat(ps, [0n])), p <> cat(ps, [0n]), root_names_ok(R{p, ps})))

# A home main, nothing mounted or assigned at it, is kept on the one serving root that holds
# its main dataset by name.
law a_home_main_is_its_holding_root:
  for +a: Bool
  for +d: Named
  for +serving: +List<R>
  for +r: R
  {select(MHome{}, a, d, serving, [r]) == Datasets{root_names(r)} : Sel}

def a_home_main_is_its_holding_root(a, d, serving, r):
  {==}

# A home main no serving root holds keeps no datasets.
law a_home_main_no_root_holds_keeps_images:
  for +a: Bool
  for +d: Named
  for +serving: +List<R>
  {select(MHome{}, a, d, serving, []) == Images{} : Sel}

def a_home_main_no_root_holds_keeps_images(a, d, serving):
  {==}

# A home main several serving roots hold is refused, naming them.
law a_home_main_several_roots_hold_is_refused:
  for +a: Bool
  for +d: Named
  for +serving: +List<R>
  for +x: R
  for +y: R
  for +t: +List<R>
  {select(MHome{}, a, d, serving, x <> y <> t) == Several{x <> y <> t} : Sel}

def a_home_main_several_roots_hold_is_refused(a, d, serving, x, y, t):
  {==}

# An absent main that adoption does not ask onto datasets is an image project.
law an_unasked_absent_main_is_an_image_project:
  for +d: Named
  for +serving: +List<R>
  for +holding: +List<R>
  {select(MAbsent{}, False{}, d, serving, holding) == Images{} : Sel}

def an_unasked_absent_main_is_an_image_project(d, serving, holding):
  {==}

# Adoption onto datasets, with no root declared, takes the one root that names the caller.
law an_asked_absent_main_takes_the_one_root:
  for +r: R
  for +holding: +List<R>
  {select(MAbsent{}, True{}, Unnamed{}, [r], holding) == Datasets{root_names(r)} : Sel}

def an_asked_absent_main_takes_the_one_root(r, holding):
  {==}

# ... and is refused when no root names the caller,
law an_asked_absent_main_without_a_root_is_refused:
  for +holding: +List<R>
  {select(MAbsent{}, True{}, Unnamed{}, [], holding) == NoRoot{} : Sel}

def an_asked_absent_main_without_a_root_is_refused(holding):
  {==}

# ... or several do, naming them: cowshed never picks one.
law an_asked_absent_main_with_several_roots_is_refused:
  for +x: R
  for +y: R
  for +t: +List<R>
  for +holding: +List<R>
  {select(MAbsent{}, True{}, Unnamed{}, x <> y <> t, holding) == Several{x <> y <> t} : Sel}

def an_asked_absent_main_with_several_roots_is_refused(x, y, t, holding):
  {==}

# A root the project declares is the root of an adoption onto datasets when it names the
# caller ...
law a_declared_root_that_names_the_caller_is_taken:
  for +r: R
  for +serving: +List<R>
  for +holding: +List<R>
  for +i: IsTrue(member(r, serving))
  {select(MAbsent{}, True{}, Name{r}, serving, holding) == Datasets{root_names(r)} : Sel}

def a_declared_root_that_names_the_caller_is_taken(r, serving, holding, i):
  served_true(member(r, serving), r, i)

# ... over any other root that names the caller ...
law a_declared_root_is_taken_over_any_other:
  for +r: R
  for +others: +List<R>
  for +holding: +List<R>
  {select(MAbsent{}, True{}, Name{r}, r <> others, holding) == Datasets{root_names(r)} : Sel}

def a_declared_root_is_taken_over_any_other(r, others, holding):
  Equal.cong(Bool, Sel, x => served.if(or(x, member(r, others)), r),
    eq_names(root_names(r), root_names(r)), True{}, eq_refl(root_names(r)))

# ... and refused, naming it, when it does not.
law a_declared_root_that_does_not_name_the_caller_is_refused:
  for +r: R
  for +serving: +List<R>
  for +holding: +List<R>
  for +i: IsTrue(not(member(r, serving)))
  {select(MAbsent{}, True{}, Name{r}, serving, holding) == Unserving{r} : Sel}

def a_declared_root_that_does_not_name_the_caller_is_refused(r, serving, holding, i):
  served_false(member(r, serving), r, i)

# The selection reads nothing beyond what it needs: the serving roots, and the declared root,
# decide only an absent main that adoption asks onto datasets ...
law selection_reads_serving_roots_only_when_asked:
  for +m: Main
  for +a: Bool
  for +d: Named
  for +s1: +List<R>
  for +s2: +List<R>
  for +h: +List<R>
  for +i: IsTrue(not(needs_serving(m, a)))
  {select(m, a, d, s1, h) == select(m, a, d, s2, h) : Sel}

def selection_reads_serving_roots_only_when_asked(m, a, d, s1, s2, h, i):
  match m a:
    case MFile{} a0:
      {==}
    case MPlace{x} a0:
      {==}
    case MHome{} a0:
      {==}
    case MAbsent{} True{}:
      Empty.absurd({select(MAbsent{}, True{}, d, s1, h) == select(MAbsent{}, True{}, d, s2, h) : Sel}, i)
    case MAbsent{} False{}:
      {==}

law the_declared_root_decides_only_an_asked_absent_main:
  for +m: Main
  for +a: Bool
  for +d1: Named
  for +d2: Named
  for +s: +List<R>
  for +h: +List<R>
  for +i: IsTrue(not(needs_serving(m, a)))
  {select(m, a, d1, s, h) == select(m, a, d2, s, h) : Sel}

def the_declared_root_decides_only_an_asked_absent_main(m, a, d1, d2, s, h, i):
  match m a:
    case MFile{} a0:
      {==}
    case MPlace{x} a0:
      {==}
    case MHome{} a0:
      {==}
    case MAbsent{} True{}:
      Empty.absurd({select(MAbsent{}, True{}, d1, s, h) == select(MAbsent{}, True{}, d2, s, h) : Sel}, i)
    case MAbsent{} False{}:
      {==}

# ... and the holding roots decide only a home main.
law selection_reads_holding_roots_only_at_home:
  for +m: Main
  for +a: Bool
  for +d: Named
  for +s: +List<R>
  for +h1: +List<R>
  for +h2: +List<R>
  for +i: IsTrue(not(needs_holding(m)))
  {select(m, a, d, s, h1) == select(m, a, d, s, h2) : Sel}

def selection_reads_holding_roots_only_at_home(m, a, d, s, h1, h2, i):
  match m:
    case MFile{}:
      {==}
    case MPlace{x}:
      {==}
    case MHome{}:
      Empty.absurd({select(MHome{}, a, d, s, h1) == select(MHome{}, a, d, s, h2) : Sel}, i)
    case MAbsent{}:
      {==}

# --- the root setup serves ----------------------------------------------------------------

# A parent named once, on the command line or in `.cowshed.toml`, is the parent.
law a_parent_named_on_the_command_line_alone_is_the_parent:
  for +x: R
  {parent(Name{x}, Unnamed{}) == Agreed{Name{x}} : Parent}

def a_parent_named_on_the_command_line_alone_is_the_parent(x):
  {==}

law a_declared_parent_alone_is_the_parent:
  for +d: Named
  {parent(Unnamed{}, d) == Agreed{d} : Parent}

def a_declared_parent_alone_is_the_parent(d):
  {==}

# Two that name the same root are that root ...
law two_parents_that_agree_are_one:
  for +x: R
  {parent(Name{x}, Name{x}) == Agreed{Name{x}} : Parent}

def two_parents_that_agree_are_one(x):
  Equal.cong(Bool, Parent, v => agree.if(v, x, x),
    eq_names(root_names(x), root_names(x)), True{}, eq_refl(root_names(x)))

# ... and two that differ are refused, naming both.
law two_parents_that_differ_are_refused_naming_both:
  for +x: R
  for +y: R
  for +i: IsTrue(not(eq_names(root_names(x), root_names(y))))
  {parent(Name{x}, Name{y}) == Disagree{x, y} : Parent}

def two_parents_that_differ_are_refused_naming_both(x, y, i):
  agree_false(eq_names(root_names(x), root_names(y)), x, y, i)

# The parent check the binding drives: refused exactly when both are named and differ.
law parent_check:
  for +c: Named
  for +d: Named
  {is_disagree(parent(c, d)) == differ(c, d) : Bool}

def parent_check(c, d):
  match c d:
    case Unnamed{} d0:
      {==}
    case Name{x} Unnamed{}:
      {==}
    case Name{+x} Name{+y}:
      agree_disagrees(eq_names(root_names(x), root_names(y)), x, y)

# A host without zfs reads nothing and does nothing.
law without_zfs_nothing_is_read:
  for +n: Named
  for +s: +List<R>
  {reads(False{}, n, s) == [] : +List<+List<Nat>>}

def without_zfs_nothing_is_read(n, s):
  {==}

law without_zfs_nothing_is_done:
  for +n: Named
  for +s: +List<R>
  for +u: Nat
  for +a: Bool
  for +es: +List<Ev>
  {root_steps(False{}, n, s, u, a, es) == [SNothing{}] : +List<Step>}

def without_zfs_nothing_is_done(n, s, u, a, es):
  {==}

# A named root is the only root read, and the roots that name the caller decide nothing then.
law a_named_root_is_the_only_root_read:
  for +r: R
  for +s: +List<R>
  {reads(True{}, Name{r}, s) == [root_names(r)] : +List<+List<Nat>>}

def a_named_root_is_the_only_root_read(r, s):
  {==}

law a_named_root_ignores_the_serving_roots:
  for +r: R
  for +s1: +List<R>
  for +s2: +List<R>
  for +u: Nat
  for +a: Bool
  for +es: +List<Ev>
  {root_steps(True{}, Name{r}, s1, u, a, es) == root_steps(True{}, Name{r}, s2, u, a, es) : +List<Step>}

def a_named_root_ignores_the_serving_roots(r, s1, s2, u, a, es):
  {==}

# With nothing named, the roots that name the caller are read, each once, in order ...
law with_nothing_named_the_serving_roots_are_read:
  for +s: +List<R>
  {reads(True{}, Unnamed{}, s) == all_names(s) : +List<+List<Nat>>}

def with_nothing_named_the_serving_roots_are_read(s):
  {==}

# ... and with none, the operator is asked to name a pool: cowshed never scans for one.
law with_nothing_named_and_no_root_a_pool_is_asked_for:
  for +u: Nat
  for +a: Bool
  for +es: +List<Ev>
  {root_steps(True{}, Unnamed{}, [], u, a, es) == [SNameAPool{}] : +List<Step>}

def with_nothing_named_and_no_root_a_pool_is_asked_for(u, a, es):
  {==}

# With nothing named, nothing is created.
law with_nothing_named_nothing_is_created:
  for +z: Bool
  for +s: +List<R>
  for +u: Nat
  for +a: Bool
  for +es: +List<Ev>
  {creates_none(root_steps(z, Unnamed{}, s, u, a, es)) == True{} : Bool}

def with_nothing_named_nothing_is_created(z, s, u, a, es):
  match z s:
    case False{} s0:
      {==}
    case True{} Nil{}:
      {==}
    case True{} Con{r, t}:
      served_none(r <> t, es, u, a)

# Only a named root is ever created.
law only_the_named_root_is_created:
  for +z: Bool
  for +r: R
  for +s: +List<R>
  for +u: Nat
  for +a: Bool
  for +es: +List<Ev>
  {creates_only(root_names(r), root_steps(z, Name{r}, s, u, a, es)) == True{} : Bool}

def only_the_named_root_is_created(z, r, s, u, a, es):
  match z:
    case False{}:
      {==}
    case True{}:
      Equal.cong(Bool, Bool, x => and(x, True{}),
        only_made(root_names(r), named_step(root_names(r), first(es), u, a)), True{},
        named_made_only(root_names(r), first(es), u, a))

# The creation check the binding drives: nothing unnamed is ever created.
law created_check:
  for +z: Bool
  for +n: Named
  for +s: +List<R>
  for +u: Nat
  for +a: Bool
  for +es: +List<Ev>
  {created_ok(n, root_steps(z, n, s, u, a, es)) == True{} : Bool}

def created_check(z, n, s, u, a, es):
  match n:
    case Unnamed{}:
      with_nothing_named_nothing_is_created(z, s, u, a, es)
    case Name{r}:
      only_the_named_root_is_created(z, r, s, u, a, es)

# An existing root is never created again, ...
law an_existing_root_is_never_created:
  for +r: +List<Nat>
  for +o: Owner
  for +dk: Owner
  for +bw: Owner
  for +mc: Owner
  for +io: Owner
  for +u: Nat
  for +a: Bool
  {is_create(named_step(r, Present{o, dk, bw, mc, io}, u, a)) == False{} : Bool}

def an_existing_root_is_never_created(r, o, dk, bw, mc, io, u, a):
  verdict_creates_nothing(any(wrong(o, dk, bw, mc, io, u, a)), r, wrong(o, dk, bw, mc, io, u, a))

# ... and a serving root `zfs get` no longer finds is refused, never created.
law a_serving_root_that_is_gone_is_refused:
  for +r: +List<Nat>
  for +u: Nat
  for +a: Bool
  {served_step(r, Absent{}, u, a) == SGone{r} : Step}

def a_serving_root_that_is_gone_is_refused(r, u, a):
  {==}

# A root whose `cowshed:owner` is not set locally to the caller is refused, never taken: an
# inherited, received, unset or other owner.
law a_root_the_caller_does_not_own_locally_is_refused:
  for +r: +List<Nat>
  for +o: Owner
  for +dk: Owner
  for +bw: Owner
  for +mc: Owner
  for +io: Owner
  for +u: Nat
  for +a: Bool
  for +h: IsTrue(not(names(o, u)))
  {judge(r, o, dk, bw, mc, io, u, a) == SRefuse{r, wrong(o, dk, bw, mc, io, u, a)} : Step}

def a_root_the_caller_does_not_own_locally_is_refused(r, o, dk, bw, mc, io, u, a, h):
  verdict_refuses(any(wrong(o, dk, bw, mc, io, u, a)), r, wrong(o, dk, bw, mc, io, u, a),
    or_left(not(names(o, u)),
      or(apple_wrong(a, dk), or(apple_wrong(a, bw), or(apple_wrong(a, mc), apple_wrong(a, io)))), h))

# On macOS a root any of whose four com.apple properties is not `off` locally is refused.
law on_macos_a_com_apple_property_not_off_locally_is_refused:
  for +r: +List<Nat>
  for +o: Owner
  for +dk: Owner
  for +bw: Owner
  for +mc: Owner
  for +io: Owner
  for +u: Nat
  for +h: IsTrue(or(not(off(dk)), or(not(off(bw)), or(not(off(mc)), not(off(io))))))
  {judge(r, o, dk, bw, mc, io, u, True{}) == SRefuse{r, wrong(o, dk, bw, mc, io, u, True{})} : Step}

def on_macos_a_com_apple_property_not_off_locally_is_refused(r, o, dk, bw, mc, io, u, h):
  verdict_refuses(any(wrong(o, dk, bw, mc, io, u, True{})), r, wrong(o, dk, bw, mc, io, u, True{}),
    or_right(not(names(o, u)),
      or(not(off(dk)), or(not(off(bw)), or(not(off(mc)), not(off(io))))), h))

# Elsewhere the com.apple properties decide nothing.
law off_macos_no_com_apple_property_decides:
  for +r: +List<Nat>
  for +o: Owner
  for +dk: Owner
  for +bw: Owner
  for +mc: Owner
  for +io: Owner
  for +dk2: Owner
  for +bw2: Owner
  for +mc2: Owner
  for +io2: Owner
  for +u: Nat
  {judge(r, o, dk, bw, mc, io, u, False{}) == judge(r, o, dk2, bw2, mc2, io2, u, False{}) : Step}

def off_macos_no_com_apple_property_decides(r, o, dk, bw, mc, io, dk2, bw2, mc2, io2, u):
  {==}

# A root owned locally by the caller, its com.apple properties `off` locally, is taken: setup
# changes nothing.
law the_callers_right_root_is_taken:
  for +r: +List<Nat>
  for +u: Nat
  for +a: Bool
  {judge(r, Read{u, True{}}, Read{0n, True{}}, Read{0n, True{}}, Read{0n, True{}}, Read{0n, True{}}, u, a) == STake{r} : Step}

def the_callers_right_root_is_taken(r, u, a):
  match a:
    case True{}:
      Equal.cong(Bool, Step, x => verdict.if(or(not(x), False{}), r, W{not(x), False{}, False{}, False{}, False{}}),
        same(u, u), True{}, same_eq(u))
    case False{}:
      Equal.cong(Bool, Step, x => verdict.if(or(not(x), False{}), r, W{not(x), False{}, False{}, False{}, False{}}),
        same(u, u), True{}, same_eq(u))

# The refusal names exactly the wrong properties: the owner when it is not set locally to the
# caller, and on macOS each com.apple property that is not `off` locally.
law the_refusal_names_exactly_the_wrong_properties:
  for +o: Owner
  for +dk: Owner
  for +bw: Owner
  for +mc: Owner
  for +io: Owner
  for +u: Nat
  for +a: Bool
  {wrong(o, dk, bw, mc, io, u, a) == W{not(names(o, u)), and(a, not(off(dk))), and(a, not(off(bw))), and(a, not(off(mc))), and(a, not(off(io)))} : Wrong}

def the_refusal_names_exactly_the_wrong_properties(o, dk, bw, mc, io, u, a):
  match a:
    case True{}:
      {==}
    case False{}:
      {==}

# --- refuted designs, computed ---------------------------------------------------------------

# A containment check that admits the root itself (prefix or equal) would let a verb name the
# root; the strict check refuses it.
def within(root: +List<Nat>, d: +List<Nat>) -> Bool:
  match root d:
    case Nil{} d0:
      True{}
    case Con{h, t} Nil{}:
      False{}
    case Con{a, s} Con{b, u}:
      and(same(a, b), within(s, u))

law a_non_strict_check_admits_the_root:
  {(within([5n, 0n], [5n, 0n]), planned(plan(R{5n, []}, VDestroy{[5n, 0n]}))) == (True{}, False{}) : Bool & Bool}

def a_non_strict_check_admits_the_root():
  {==}

# Adoption that took the first of several serving roots would pick a pool; selection refuses.
law picking_the_first_root_is_refused:
  {select(MAbsent{}, True{}, Unnamed{}, [R{5n, []}, R{6n, []}], []) == Several{[R{5n, []}, R{6n, []}]} : Sel}

def picking_the_first_root_is_refused():
  {==}

# Adoption that took a declared root because it is the one serving root would serve another
# user's root; a declared root that does not name the caller is refused.
law a_declared_root_is_not_the_one_serving_root:
  {select(MAbsent{}, True{}, Name{R{7n, []}}, [R{5n, []}], []) == Unserving{R{7n, []}} : Sel}

def a_declared_root_is_not_the_one_serving_root():
  {==}

# Taking a root whose owner value is the caller's but inherited would serve whoever set it on a
# parent; the root is refused, naming the owner.
law an_inherited_owner_of_the_callers_uid_is_refused:
  {judge([5n, 0n], Read{7n, False{}}, Read{0n, True{}}, Read{0n, True{}}, Read{0n, True{}}, Read{0n, True{}}, 7n, True{}) == SRefuse{[5n, 0n], W{True{}, False{}, False{}, False{}, False{}}} : Step}

def an_inherited_owner_of_the_callers_uid_is_refused():
  {==}