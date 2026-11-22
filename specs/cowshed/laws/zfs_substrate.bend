# The ZFS substrate's two decisions (specs/cowshed/09_substrates.md, "Privileged helper" and
# selection): what the root helper admits, and which substrate a project's main entry selects.
# Both are pure cores; the helper's `zfs get` and `zfs` runs and the store's reads are a shell
# that gathers their inputs and executes their outputs.
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
# (not adopted: datasets only when adoption asks, below the one root that names the caller).
#
# A dataset name is its list of components, each a Nat: 0n is `cowshed`, 1n `projects`, 2n
# `main`, 3n `build`, 4n `.trash` (an id that is hidden, never a build volume), and any other n a
# plain component. A root `R{pool, parents}` is `pool/parents…/cowshed`, so every root is valid.
#
# Check: bun bend2/main.ts zfs_substrate.bend --verdict
# Bound to the code by cowshed-helper src/laws.rs (`plan`, `admit` against `plan`, `admits`) and
# cowshed-core src/storage/directory/zfs/laws.rs (`select` against `select`), which run the
# emitted models (src/laws/model.rs, zfs/laws/model.rs) and the code on the same inputs.
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

type Found is Data:
  Nothing{}
  Named{d: +List<Nat>}

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

def asked(a: Bool, rs: +List<R>) -> Sel:
  match a:
    case True{}:
      one(rs)
    case False{}:
      Images{}

# zfs.rs `select`: `serving` are the roots that name the caller, `holding` those of them that
# hold the project's main dataset by name
def select(m: Main, a: Bool, serving: +List<R>, holding: +List<R>) -> Sel:
  match m:
    case MFile{}:
      Images{}
    case MPlace{d}:
      root_sel(root_of(d))
    case MHome{}:
      held(holding)
    case MAbsent{}:
      asked(a, serving)

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

# A project whose main is a regular file is an image project, whatever roots exist and
# whatever adoption asks.
law a_file_main_is_an_image_project:
  for +a: Bool
  for +serving: +List<R>
  for +holding: +List<R>
  {select(MFile{}, a, serving, holding) == Images{} : Sel}

def a_file_main_is_an_image_project(a, serving, holding):
  {==}

# The selection check the binding drives.
law a_file_main_check:
  for +m: Main
  for +a: Bool
  for +serving: +List<R>
  for +holding: +List<R>
  {and(is_file(m), not(is_images(select(m, a, serving, holding)))) == False{} : Bool}

def a_file_main_check(m, a, serving, holding):
  match m:
    case MFile{}:
      {==}
    case MPlace{d}:
      {==}
    case MHome{}:
      {==}
    case MAbsent{}:
      {==}

# A mounted or assigned main dataset, `<root>/projects/<owner>/<repo>/main`, names its root,
# whatever roots serve and whatever adoption asks.
law a_main_dataset_names_its_root:
  for +root: R
  for +o: Nat
  for +n: Nat
  for +a: Bool
  for +serving: +List<R>
  for +holding: +List<R>
  {select(MPlace{cat(root_names(root), [1n, o, n, 2n])}, a, serving, holding) == Datasets{root_names(root)} : Sel}

def a_main_dataset_names_its_root(root, o, n, a, serving, holding):
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
  for +serving: +List<R>
  for +r: R
  {select(MHome{}, a, serving, [r]) == Datasets{root_names(r)} : Sel}

def a_home_main_is_its_holding_root(a, serving, r):
  {==}

# A home main no serving root holds keeps no datasets.
law a_home_main_no_root_holds_keeps_images:
  for +a: Bool
  for +serving: +List<R>
  {select(MHome{}, a, serving, []) == Images{} : Sel}

def a_home_main_no_root_holds_keeps_images(a, serving):
  {==}

# A home main several serving roots hold is refused, naming them.
law a_home_main_several_roots_hold_is_refused:
  for +a: Bool
  for +serving: +List<R>
  for +x: R
  for +y: R
  for +t: +List<R>
  {select(MHome{}, a, serving, x <> y <> t) == Several{x <> y <> t} : Sel}

def a_home_main_several_roots_hold_is_refused(a, serving, x, y, t):
  {==}

# An absent main that adoption does not ask onto datasets is an image project.
law an_unasked_absent_main_is_an_image_project:
  for +serving: +List<R>
  for +holding: +List<R>
  {select(MAbsent{}, False{}, serving, holding) == Images{} : Sel}

def an_unasked_absent_main_is_an_image_project(serving, holding):
  {==}

# Adoption onto datasets takes the one root that names the caller.
law an_asked_absent_main_takes_the_one_root:
  for +r: R
  for +holding: +List<R>
  {select(MAbsent{}, True{}, [r], holding) == Datasets{root_names(r)} : Sel}

def an_asked_absent_main_takes_the_one_root(r, holding):
  {==}

# ... and is refused when no root names the caller,
law an_asked_absent_main_without_a_root_is_refused:
  for +holding: +List<R>
  {select(MAbsent{}, True{}, [], holding) == NoRoot{} : Sel}

def an_asked_absent_main_without_a_root_is_refused(holding):
  {==}

# ... or several do, naming them: cowshed never picks one.
law an_asked_absent_main_with_several_roots_is_refused:
  for +x: R
  for +y: R
  for +t: +List<R>
  for +holding: +List<R>
  {select(MAbsent{}, True{}, x <> y <> t, holding) == Several{x <> y <> t} : Sel}

def an_asked_absent_main_with_several_roots_is_refused(x, y, t, holding):
  {==}

# The selection reads nothing beyond what it needs: the serving roots decide only an absent
# main that adoption asks onto datasets ...
law selection_reads_serving_roots_only_when_asked:
  for +m: Main
  for +a: Bool
  for +s1: +List<R>
  for +s2: +List<R>
  for +h: +List<R>
  for +i: IsTrue(not(needs_serving(m, a)))
  {select(m, a, s1, h) == select(m, a, s2, h) : Sel}

def selection_reads_serving_roots_only_when_asked(m, a, s1, s2, h, i):
  match m a:
    case MFile{} a0:
      {==}
    case MPlace{d} a0:
      {==}
    case MHome{} a0:
      {==}
    case MAbsent{} True{}:
      Empty.absurd({select(MAbsent{}, True{}, s1, h) == select(MAbsent{}, True{}, s2, h) : Sel}, i)
    case MAbsent{} False{}:
      {==}

# ... and the holding roots decide only a home main.
law selection_reads_holding_roots_only_at_home:
  for +m: Main
  for +a: Bool
  for +s: +List<R>
  for +h1: +List<R>
  for +h2: +List<R>
  for +i: IsTrue(not(needs_holding(m)))
  {select(m, a, s, h1) == select(m, a, s, h2) : Sel}

def selection_reads_holding_roots_only_at_home(m, a, s, h1, h2, i):
  match m:
    case MFile{}:
      {==}
    case MPlace{d}:
      {==}
    case MHome{}:
      Empty.absurd({select(MHome{}, a, s, h1) == select(MHome{}, a, s, h2) : Sel}, i)
    case MAbsent{}:
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
  {select(MAbsent{}, True{}, [R{5n, []}, R{6n, []}], []) == Several{[R{5n, []}, R{6n, []}]} : Sel}

def picking_the_first_root_is_refused():
  {==}