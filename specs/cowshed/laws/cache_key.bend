# Cache-key soundness of the land gate's task graph (specs/cowshed/03_caches.md "Nx cache" and "What a
# land re-runs"): the producers whose outputs a task's key names, against the producers its run reads.
#
# A target has a language (`Lang`), the kind of file it writes (`Out`), the languages of its own sources,
# its runtime inputs (`runs`), the producers it reads at run time (`reads`: the producers whose outputs
# its run reads and whose bytes reach its outputs), the producers it waits for (`deps`: its `dependsOn`
# edges) and its `dependentTasksOutputFiles` inputs (`sels`). A selector names a glob over the outputs of
# the producers it reaches, along one edge or transitively.
#
# (a) rust_key_has_no_ts_output: a Rust target's key names no TypeScript output its run does not read
#     (key_has_only_reads: the same for a producer of any language), the soundness form of "a key
#     changes only when an input that changes the output changes".
# (b) key_complete: every producer output a run reads is in its key.
# (c) unchanged_tree_runs_nothing: a key is a function of the bytes of its declared inputs, so a
#     re-gate of an unchanged tree runs nothing (03 "What a land re-runs"); key_moves_with_input is the
#     definedness companion, a key that ignored its inputs would satisfy (c).
# `infer` is the correct inference: a read gives a `dependsOn` edge and one selector naming that
# producer alone, and (a) and (b) are proven for every graph it builds. `observe` is
# packages/nx-plugin/src/index.ts:2467-2492 as it stands, two transitive selectors on every bounded-exec
# target; observer_key_refuted proves it violates (a) on a three-target graph.
#
# Check: bun bend2/main.ts specs/cowshed/laws/cache_key.bend --verdict
# Bound to the code by packages/nx-plugin/src/cache-key-laws.test.ts, which runs the emitted model
# (src/__tests__/cache-key-model.ts) on the real task graph of this repository.
import Base

type Lang is Data:
  Rust{}
  Ts{}
  Other{}

type Out is Data:
  ONone{}
  ONode{}
  OJs{}
  OData{}

type Glob is Data:
  GNode{}
  GJs{}
  GAll{}
  GOf{p: Nat}

type Sel is Data:
  Sel{glob: Glob, tr: Bool}

type Tgt is Data:
  Tgt{id: Nat, lang: Lang, out: Out, srcs: +List<Lang>, runs: +List<Nat>, reads: +List<Nat>, deps: +List<Nat>, sels: +List<Sel>}

type Viol is Data:
  Viol{t: Nat, p: Nat}

# --- helpers -------------------------------------------------------------------------

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

def implies(a: Bool, b: Bool) -> Bool:
  or(not(a), b)

def mem(+x: Nat, xs: +List<Nat>) -> Bool:
  match xs:
    case Nil{}:
      False{}
    case Con{h, t}:
      or(same(x, h), mem(x, t))

def cat(xs: +List<Nat>, ys: +List<Nat>) -> +List<Nat>:
  match xs:
    case Nil{}:
      ys
    case Con{h, t}:
      h <> cat(t, ys)

def is_ts(l: Lang) -> Bool:
  match l:
    case Rust{}:
      False{}
    case Ts{}:
      True{}
    case Other{}:
      False{}

def is_rust(l: Lang) -> Bool:
  match l:
    case Rust{}:
      True{}
    case Ts{}:
      False{}
    case Other{}:
      False{}

def has_out(o: Out) -> Bool:
  match o:
    case ONone{}:
      False{}
    case ONode{}:
      True{}
    case OJs{}:
      True{}
    case OData{}:
      True{}

def is_node(o: Out) -> Bool:
  match o:
    case ONone{}:
      False{}
    case ONode{}:
      True{}
    case OJs{}:
      False{}
    case OData{}:
      False{}

def is_js(o: Out) -> Bool:
  match o:
    case ONone{}:
      False{}
    case ONode{}:
      False{}
    case OJs{}:
      True{}
    case OData{}:
      False{}

# --- the graph ---------------------------------------------------------------------------

def lang_of.if(b: Bool, l: Lang, r: Lang) -> Lang:
  match b:
    case True{}:
      l
    case False{}:
      r

def lang_of(+id: Nat, g: +List<Tgt>) -> Lang:
  match g:
    case Nil{}:
      Other{}
    case Con{Tgt{i, l, o, s, u, r, d, e}, t}:
      lang_of.if(same(id, i), l, lang_of(id, t))

def out_of.if(b: Bool, o: Out, r: Out) -> Out:
  match b:
    case True{}:
      o
    case False{}:
      r

def out_of(+id: Nat, g: +List<Tgt>) -> Out:
  match g:
    case Nil{}:
      ONone{}
    case Con{Tgt{i, l, o, s, u, r, d, e}, t}:
      out_of.if(same(id, i), o, out_of(id, t))

def deps_of.if(b: Bool, d: +List<Nat>, r: +List<Nat>) -> +List<Nat>:
  match b:
    case True{}:
      d
    case False{}:
      r

def deps_of(+id: Nat, g: +List<Tgt>) -> +List<Nat>:
  match g:
    case Nil{}:
      []
    case Con{Tgt{i, l, o, s, u, r, d, e}, t}:
      deps_of.if(same(id, i), d, deps_of(id, t))

def len(g: +List<Tgt>) -> Nat:
  match g:
    case Nil{}:
      0n
    case Con{t, r}:
      1n+len(r)

def ids(g: +List<Tgt>) -> +List<Nat>:
  match g:
    case Nil{}:
      []
    case Con{Tgt{i, l, o, s, u, r, d, e}, t}:
      i <> ids(t)

def step(+g: +List<Tgt>, ids: +List<Nat>) -> +List<Nat>:
  match ids:
    case Nil{}:
      []
    case Con{p, t}:
      cat(deps_of(p, g), step(g, t))

def add.if(+x: Nat, xs: +List<Nat>, b: Bool) -> +List<Nat>:
  match b:
    case True{}:
      xs
    case False{}:
      x <> xs

# `xs` with the elements of `ys` it does not hold added
def union(ys: +List<Nat>, +xs: +List<Nat>) -> +List<Nat>:
  match ys:
    case Nil{}:
      xs
    case Con{+h, t}:
      union(t, add.if(h, xs, mem(h, xs)))

def reach.n(fuel: Nat, +g: +List<Tgt>, +ids: +List<Nat>) -> +List<Nat>:
  match fuel:
    case 0n:
      ids
    case 1n+f:
      reach.n(f, g, union(step(g, ids), ids))

# the producers a transitive selector reaches from a target that waits for `d`: through every edge (a
# graph of n targets is crossed in n steps); a selector along one edge reaches `d` itself
def reach_t(+g: +List<Tgt>, +d: +List<Nat>) -> +List<Nat>:
  reach.n(len(g), g, d)

def reach_pick(+d: +List<Nat>, +dt: +List<Nat>, tr: Bool) -> +List<Nat>:
  match tr:
    case False{}:
      d
    case True{}:
      dt

def matches(gl: Glob, +p: Nat, o: Out) -> Bool:
  match gl:
    case GNode{}:
      is_node(o)
    case GJs{}:
      is_js(o)
    case GAll{}:
      has_out(o)
    case GOf{q}:
      and(same(p, q), has_out(o))

def sel.hit(+g: +List<Tgt>, +d: +List<Nat>, +dt: +List<Nat>, s: Sel, +p: Nat) -> Bool:
  match s:
    case Sel{gl, tr}:
      and(mem(p, reach_pick(d, dt, tr)), matches(gl, p, out_of(p, g)))

# the producer p's output is among the files the key hashes; `dt` is the transitive reach of `d`, shared by
# every selector and every producer asked about
def in_key(+g: +List<Tgt>, +d: +List<Nat>, +dt: +List<Nat>, sels: +List<Sel>, +p: Nat) -> Bool:
  match sels:
    case Nil{}:
      False{}
    case Con{s, t}:
      or(sel.hit(g, d, dt, s, p), in_key(g, d, dt, t, p))

# --- the correct inference ---------------------------------------------------------------

def infer_sels(r: +List<Nat>) -> +List<Sel>:
  match r:
    case Nil{}:
      []
    case Con{q, t}:
      Sel{GOf{q}, False{}} <> infer_sels(t)

def infer_t(t: Tgt) -> Tgt:
  match t:
    case Tgt{i, l, o, s, u, +r, d, e}:
      Tgt{i, l, o, s, u, r, cat(r, d), infer_sels(r)}

def infer(g: +List<Tgt>) -> +List<Tgt>:
  match g:
    case Nil{}:
      []
    case Con{t, r}:
      infer_t(t) <> infer(r)

# --- the checks: violations are (target, producer) pairs ------------------------------------

def id_of(t: Tgt) -> Nat:
  match t:
    case Tgt{i, l, o, s, u, r, d, e}:
      i

def deps_t(t: Tgt) -> +List<Nat>:
  match t:
    case Tgt{i, l, o, s, u, r, +d, e}:
      d

def catv(xs: +List<Viol>, ys: +List<Viol>) -> +List<Viol>:
  match xs:
    case Nil{}:
      ys
    case Con{h, t}:
      h <> catv(t, ys)

def viol.if(ok: Bool, t: Nat, p: Nat, rest: +List<Viol>) -> +List<Viol>:
  match ok:
    case True{}:
      rest
    case False{}:
      Viol{t, p} <> rest

# (a) a Rust key names a TypeScript output only if the run reads it
def a_ok(+g: +List<Tgt>, t: Tgt, +dt: +List<Nat>, +p: Nat) -> Bool:
  match t:
    case Tgt{i, l, o, s, u, +r, +d, e}:
      implies(and(is_rust(l), and(is_ts(lang_of(p, g)), in_key(g, d, dt, e, p))), mem(p, r))

# (a') a key names an output only if the run reads it, whatever the producer's language
def u_ok(+g: +List<Tgt>, t: Tgt, +dt: +List<Nat>, +p: Nat) -> Bool:
  match t:
    case Tgt{i, l, o, s, u, +r, +d, e}:
      implies(in_key(g, d, dt, e, p), mem(p, r))

# (b) an output a run reads is in its key
def b_ok(+g: +List<Tgt>, t: Tgt, +dt: +List<Nat>, +p: Nat) -> Bool:
  match t:
    case Tgt{i, l, o, s, u, +r, +d, e}:
      implies(and(mem(p, r), has_out(out_of(p, g))), in_key(g, d, dt, e, p))

def a_viol.p(+g: +List<Tgt>, +t: Tgt, +dt: +List<Nat>, ps: +List<Nat>) -> +List<Viol>:
  match ps:
    case Nil{}:
      []
    case Con{+p, r}:
      viol.if(a_ok(g, t, dt, p), id_of(t), p, a_viol.p(g, t, dt, r))

def a_viol.t(+g: +List<Tgt>, ts: +List<Tgt>, +ps: +List<Nat>) -> +List<Viol>:
  match ts:
    case Nil{}:
      []
    case Con{+t, r}:
      catv(a_viol.p(g, t, reach_t(g, deps_t(t)), ps), a_viol.t(g, r, ps))

def a_viol(+g: +List<Tgt>) -> +List<Viol>:
  a_viol.t(g, g, ids(g))

def u_viol.p(+g: +List<Tgt>, +t: Tgt, +dt: +List<Nat>, ps: +List<Nat>) -> +List<Viol>:
  match ps:
    case Nil{}:
      []
    case Con{+p, r}:
      viol.if(u_ok(g, t, dt, p), id_of(t), p, u_viol.p(g, t, dt, r))

def u_viol.t(+g: +List<Tgt>, ts: +List<Tgt>, +ps: +List<Nat>) -> +List<Viol>:
  match ts:
    case Nil{}:
      []
    case Con{+t, r}:
      catv(u_viol.p(g, t, reach_t(g, deps_t(t)), ps), u_viol.t(g, r, ps))

def u_viol(+g: +List<Tgt>) -> +List<Viol>:
  u_viol.t(g, g, ids(g))

def b_viol.p(+g: +List<Tgt>, +t: Tgt, +dt: +List<Nat>, ps: +List<Nat>) -> +List<Viol>:
  match ps:
    case Nil{}:
      []
    case Con{+p, r}:
      viol.if(b_ok(g, t, dt, p), id_of(t), p, b_viol.p(g, t, dt, r))

def b_viol.t(+g: +List<Tgt>, ts: +List<Tgt>, +ps: +List<Nat>) -> +List<Viol>:
  match ts:
    case Nil{}:
      []
    case Con{+t, r}:
      catv(b_viol.p(g, t, reach_t(g, deps_t(t)), ps), b_viol.t(g, r, ps))

def b_viol(+g: +List<Tgt>) -> +List<Viol>:
  b_viol.t(g, g, ids(g))

# what a driver evaluates on one target of a graph: the pairs (target, producer) it breaks
def a_viol_of(+g: +List<Tgt>, +t: Tgt) -> +List<Viol>:
  a_viol.p(g, t, reach_t(g, deps_t(t)), ids(g))

def u_viol_of(+g: +List<Tgt>, +t: Tgt) -> +List<Viol>:
  u_viol.p(g, t, reach_t(g, deps_t(t)), ids(g))

def b_viol_of(+g: +List<Tgt>, +t: Tgt) -> +List<Viol>:
  b_viol.p(g, t, reach_t(g, deps_t(t)), ids(g))

# --- lemmas ----------------------------------------------------------------------------------

def imp_true(x: Bool) -> {implies(x, True{}) == True{} : Bool}:
  match x:
    case False{}:
      {==}
    case True{}:
      {==}

def step2(k: Bool, s: Bool, r: Bool, ih: {implies(k, r) == True{} : Bool}) -> {implies(k, or(s, r)) == True{} : Bool}:
  match k:
    case False{}:
      {==}
    case True{}:
      match s:
        case False{}:
          ih
        case True{}:
          {==}

def step_lemma(m: Bool, s: Bool, o: Bool, k: Bool, r: Bool, ih: {implies(k, r) == True{} : Bool}) -> {implies(or(and(m, and(s, o)), k), or(s, r)) == True{} : Bool}:
  match m:
    case False{}:
      step2(k, s, r, ih)
    case True{}:
      match s:
        case False{}:
          step2(k, False{}, r, ih)
        case True{}:
          imp_true(or(o, k))

# a selector list built by `infer` names only producers the run reads
def key_reads(+g: +List<Tgt>, +d: +List<Nat>, +dt: +List<Nat>, +r: +List<Nat>, +p: Nat) -> {implies(in_key(g, d, dt, infer_sels(r), p), mem(p, r)) == True{} : Bool}:
  match r:
    case Nil{}:
      {==}
    case Con{q, t}:
      step_lemma(mem(p, d), same(p, q), has_out(out_of(p, g)), in_key(g, d, dt, infer_sels(t), p), mem(p, t), key_reads(g, d, dt, t, p))

def rk_step(s: Bool, rd: Bool, m: Bool, h: Bool, k: Bool, ih: {implies(and(rd, and(m, h)), k) == True{} : Bool}) -> {implies(and(or(s, rd), and(m, h)), or(and(m, and(s, h)), k)) == True{} : Bool}:
  match s:
    case True{}:
      match m:
        case False{}:
          {==}
        case True{}:
          match h:
            case False{}:
              {==}
            case True{}:
              {==}
    case False{}:
      match rd:
        case False{}:
          {==}
        case True{}:
          match m:
            case False{}:
              {==}
            case True{}:
              ih

# every read of an output of a producer the target waits for is keyed by `infer_sels`
def reads_key(+g: +List<Tgt>, +d: +List<Nat>, +dt: +List<Nat>, +r: +List<Nat>, +p: Nat) -> {implies(and(mem(p, r), and(mem(p, d), has_out(out_of(p, g)))), in_key(g, d, dt, infer_sels(r), p)) == True{} : Bool}:
  match r:
    case Nil{}:
      {==}
    case Con{q, t}:
      rk_step(same(p, q), mem(p, t), mem(p, d), has_out(out_of(p, g)), in_key(g, d, dt, infer_sels(t), p), reads_key(g, d, dt, t, p))

def disc(b: Bool) -> Data:
  match b:
    case True{}:
      Empty
    case False{}:
      Unit

def false_true(h: {False{} == True{} : Bool}) -> Empty:
  %h : disc(_)
  Unit{}

def b_combine(rd: Bool, m: Bool, h: Bool, k: Bool, hrm: {implies(rd, m) == True{} : Bool}, hk: {implies(and(rd, and(m, h)), k) == True{} : Bool}) -> {implies(and(rd, h), k) == True{} : Bool}:
  match rd:
    case False{}:
      {==}
    case True{}:
      match m:
        case False{}:
          Empty.absurd({implies(and(True{}, h), k) == True{} : Bool}, false_true(hrm))
        case True{}:
          hk

def mem_imp.if(s: Bool, a: Bool, b: Bool, ih: {implies(a, b) == True{} : Bool}) -> {implies(or(s, a), or(s, b)) == True{} : Bool}:
  match s:
    case True{}:
      {==}
    case False{}:
      ih

# a read the inference gave an edge for is a dependency: the premise of `mem_cat` as an implication
def mem_imp(+p: Nat, +r: +List<Nat>, +d: +List<Nat>) -> {implies(mem(p, r), mem(p, cat(r, d))) == True{} : Bool}:
  match r:
    case Nil{}:
      {==}
    case Con{q, t}:
      mem_imp.if(same(p, q), mem(p, t), mem(p, cat(t, d)), mem_imp(p, t, d))

def implies_and(x: Bool, y: Bool, k: Bool, r: Bool, h: {implies(k, r) == True{} : Bool}) -> {implies(and(x, and(y, k)), r) == True{} : Bool}:
  match x:
    case False{}:
      {==}
    case True{}:
      match y:
        case False{}:
          {==}
        case True{}:
          h

def a_ok_infer(+g: +List<Tgt>, t: Tgt, +dt: +List<Nat>, +p: Nat) -> {a_ok(g, infer_t(t), dt, p) == True{} : Bool}:
  match t:
    case Tgt{i, l, o, s, u, +r, +d, e}:
      implies_and(is_rust(l), is_ts(lang_of(p, g)), in_key(g, cat(r, d), dt, infer_sels(r), p), mem(p, r), key_reads(g, cat(r, d), dt, r, p))

def u_ok_infer(+g: +List<Tgt>, t: Tgt, +dt: +List<Nat>, +p: Nat) -> {u_ok(g, infer_t(t), dt, p) == True{} : Bool}:
  match t:
    case Tgt{i, l, o, s, u, +r, +d, e}:
      key_reads(g, cat(r, d), dt, r, p)

def b_ok_infer(+g: +List<Tgt>, t: Tgt, +dt: +List<Nat>, +p: Nat) -> {b_ok(g, infer_t(t), dt, p) == True{} : Bool}:
  match t:
    case Tgt{i, l, o, s, u, +r, +d, e}:
      b_combine(mem(p, r), mem(p, cat(r, d)), has_out(out_of(p, g)), in_key(g, cat(r, d), dt, infer_sels(r), p), mem_imp(p, r, d), reads_key(g, cat(r, d), dt, r, p))

# --- the laws, for every graph the inference builds -----------------------------------------

def a_viol_p_infer(+g: +List<Tgt>, +t: Tgt, +dt: +List<Nat>, +ps: +List<Nat>) -> {a_viol.p(g, infer_t(t), dt, ps) == [] : +List<Viol>}:
  match ps:
    case Nil{}:
      {==}
    case Con{+p, r}:
      %Equal.sym(Bool, a_ok(g, infer_t(t), dt, p), True{}, a_ok_infer(g, t, dt, p)) : {viol.if(_, id_of(infer_t(t)), p, a_viol.p(g, infer_t(t), dt, r)) == [] : +List<Viol>}
      a_viol_p_infer(g, t, dt, r)

def a_viol_t_infer(+g: +List<Tgt>, +ts: +List<Tgt>, +ps: +List<Nat>) -> {a_viol.t(g, infer(ts), ps) == [] : +List<Viol>}:
  match ts:
    case Nil{}:
      {==}
    case Con{t, r}:
      %Equal.sym(+List<Viol>, a_viol.p(g, infer_t(t), reach_t(g, deps_t(infer_t(t))), ps), [], a_viol_p_infer(g, t, reach_t(g, deps_t(infer_t(t))), ps)) : {catv(_, a_viol.t(g, infer(r), ps)) == [] : +List<Viol>}
      a_viol_t_infer(g, r, ps)

def u_viol_p_infer(+g: +List<Tgt>, +t: Tgt, +dt: +List<Nat>, +ps: +List<Nat>) -> {u_viol.p(g, infer_t(t), dt, ps) == [] : +List<Viol>}:
  match ps:
    case Nil{}:
      {==}
    case Con{+p, r}:
      %Equal.sym(Bool, u_ok(g, infer_t(t), dt, p), True{}, u_ok_infer(g, t, dt, p)) : {viol.if(_, id_of(infer_t(t)), p, u_viol.p(g, infer_t(t), dt, r)) == [] : +List<Viol>}
      u_viol_p_infer(g, t, dt, r)

def u_viol_t_infer(+g: +List<Tgt>, +ts: +List<Tgt>, +ps: +List<Nat>) -> {u_viol.t(g, infer(ts), ps) == [] : +List<Viol>}:
  match ts:
    case Nil{}:
      {==}
    case Con{t, r}:
      %Equal.sym(+List<Viol>, u_viol.p(g, infer_t(t), reach_t(g, deps_t(infer_t(t))), ps), [], u_viol_p_infer(g, t, reach_t(g, deps_t(infer_t(t))), ps)) : {catv(_, u_viol.t(g, infer(r), ps)) == [] : +List<Viol>}
      u_viol_t_infer(g, r, ps)

def b_viol_p_infer(+g: +List<Tgt>, +t: Tgt, +dt: +List<Nat>, +ps: +List<Nat>) -> {b_viol.p(g, infer_t(t), dt, ps) == [] : +List<Viol>}:
  match ps:
    case Nil{}:
      {==}
    case Con{+p, r}:
      %Equal.sym(Bool, b_ok(g, infer_t(t), dt, p), True{}, b_ok_infer(g, t, dt, p)) : {viol.if(_, id_of(infer_t(t)), p, b_viol.p(g, infer_t(t), dt, r)) == [] : +List<Viol>}
      b_viol_p_infer(g, t, dt, r)

def b_viol_t_infer(+g: +List<Tgt>, +ts: +List<Tgt>, +ps: +List<Nat>) -> {b_viol.t(g, infer(ts), ps) == [] : +List<Viol>}:
  match ts:
    case Nil{}:
      {==}
    case Con{t, r}:
      %Equal.sym(+List<Viol>, b_viol.p(g, infer_t(t), reach_t(g, deps_t(infer_t(t))), ps), [], b_viol_p_infer(g, t, reach_t(g, deps_t(infer_t(t))), ps)) : {catv(_, b_viol.t(g, infer(r), ps)) == [] : +List<Viol>}
      b_viol_t_infer(g, r, ps)

# (a) A Rust target's key names a TypeScript output only if the target reads it: a graph `infer` builds
# has no (target, producer) violation.
law rust_key_has_no_ts_output:
  for +g: +List<Tgt>
  {a_viol(infer(g)) == [] : +List<Viol>}

def rust_key_has_no_ts_output(g):
  a_viol_t_infer(infer(g), g, ids(infer(g)))

# (a') No key names an output its run does not read, in any language: the soundness form of "a key
# changes only when an input that changes the output changes".
law key_has_only_reads:
  for +g: +List<Tgt>
  {u_viol(infer(g)) == [] : +List<Viol>}

def key_has_only_reads(g):
  u_viol_t_infer(infer(g), g, ids(infer(g)))

# (b) Every output a run reads is in its key, so a change to it re-keys the run.
law key_complete:
  for +g: +List<Tgt>
  {b_viol(infer(g)) == [] : +List<Viol>}

def key_complete(g):
  b_viol_t_infer(infer(g), g, ids(infer(g)))

# --- (c) a key is a function of its declared input bytes -------------------------------------

type Item is Data:
  ISrc{lang: Lang}
  IRun{n: Nat}
  IOut{p: Nat}

type Row is Data:
  Row{i: Item, h: Nat}

def lang_eq(a: Lang, b: Lang) -> Bool:
  match a b:
    case Rust{} Rust{}:
      True{}
    case Rust{} Ts{}:
      False{}
    case Rust{} Other{}:
      False{}
    case Ts{} Rust{}:
      False{}
    case Ts{} Ts{}:
      True{}
    case Ts{} Other{}:
      False{}
    case Other{} Rust{}:
      False{}
    case Other{} Ts{}:
      False{}
    case Other{} Other{}:
      True{}

def item_eq(a: Item, b: Item) -> Bool:
  match a b:
    case ISrc{x} ISrc{y}:
      lang_eq(x, y)
    case ISrc{x} IRun{m}:
      False{}
    case ISrc{x} IOut{q}:
      False{}
    case IRun{n} ISrc{y}:
      False{}
    case IRun{n} IRun{m}:
      same(n, m)
    case IRun{n} IOut{q}:
      False{}
    case IOut{p} ISrc{y}:
      False{}
    case IOut{p} IRun{m}:
      False{}
    case IOut{p} IOut{q}:
      same(p, q)

def look.if(b: Bool, h: Nat, r: Nat) -> Nat:
  match b:
    case True{}:
      h
    case False{}:
      r

# the bytes (a hash) a checkout holds for an input; an input it does not hold is 0
def look(+i: Item, e: +List<Row>) -> Nat:
  match e:
    case Nil{}:
      0n
    case Con{Row{j, h}, t}:
      look.if(item_eq(i, j), h, look(i, t))

def src_items(s: +List<Lang>) -> +List<Item>:
  match s:
    case Nil{}:
      []
    case Con{l, t}:
      ISrc{l} <> src_items(t)

def run_items(r: +List<Nat>) -> +List<Item>:
  match r:
    case Nil{}:
      []
    case Con{n, t}:
      IRun{n} <> run_items(t)

def cati(xs: +List<Item>, ys: +List<Item>) -> +List<Item>:
  match xs:
    case Nil{}:
      ys
    case Con{h, t}:
      h <> cati(t, ys)

def out_items.if(b: Bool, p: Nat, rest: +List<Item>) -> +List<Item>:
  match b:
    case True{}:
      IOut{p} <> rest
    case False{}:
      rest

def out_items(+g: +List<Tgt>, +d: +List<Nat>, +dt: +List<Nat>, +sels: +List<Sel>, ps: +List<Nat>) -> +List<Item>:
  match ps:
    case Nil{}:
      []
    case Con{+p, t}:
      out_items.if(in_key(g, d, dt, sels, p), p, out_items(g, d, dt, sels, t))

# every input a target declares: its sources, its runtime inputs and the outputs its key names
def items(+g: +List<Tgt>, t: Tgt) -> +List<Item>:
  match t:
    case Tgt{i, l, o, s, u, r, +d, e}:
      cati(src_items(s), cati(run_items(u), out_items(g, d, reach_t(g, d), e, ids(g))))

def key(+e: +List<Row>, xs: +List<Item>) -> +List<Nat>:
  match xs:
    case Nil{}:
      []
    case Con{x, t}:
      look(x, e) <> key(e, t)

def agree_b(+e1: +List<Row>, +e2: +List<Row>, xs: +List<Item>) -> Bool:
  match xs:
    case Nil{}:
      True{}
    case Con{+x, t}:
      and(same(look(x, e1), look(x, e2)), agree_b(e1, e2, t))

def differs_b(+e1: +List<Row>, +e2: +List<Row>, xs: +List<Item>) -> Bool:
  match xs:
    case Nil{}:
      False{}
    case Con{+x, t}:
      or(not(same(look(x, e1), look(x, e2))), differs_b(e1, e2, t))

def eq_b(xs: +List<Nat>, ys: +List<Nat>) -> Bool:
  match xs ys:
    case Nil{} Nil{}:
      True{}
    case Nil{} Con{y, b}:
      False{}
    case Con{x, a} Nil{}:
      False{}
    case Con{+x, a} Con{+y, b}:
      and(same(x, y), eq_b(a, b))

def and_l(a: Bool, b: Bool, h: {and(a, b) == True{} : Bool}) -> {a == True{} : Bool}:
  match a:
    case False{}:
      Empty.absurd({False{} == True{} : Bool}, false_true(h))
    case True{}:
      {==}

def and_r(a: Bool, b: Bool, h: {and(a, b) == True{} : Bool}) -> {b == True{} : Bool}:
  match a:
    case False{}:
      Empty.absurd({b == True{} : Bool}, false_true(h))
    case True{}:
      h

def same_eq(+a: Nat, +b: Nat, h: {same(a, b) == True{} : Bool}) -> {a == b : Nat}:
  match a b:
    case 0n 0n:
      {==}
    case 0n 1n+q:
      Empty.absurd({0n == 1n+q : Nat}, false_true(h))
    case 1n+p 0n:
      Empty.absurd({1n+p == 0n : Nat}, false_true(h))
    case 1n+p 1n+q:
      %same_eq(p, q, h) : {1n+p == 1n+_ : Nat}
      {==}

def key_agree(+e1: +List<Row>, +e2: +List<Row>, +xs: +List<Item>, +h: {agree_b(e1, e2, xs) == True{} : Bool}) -> {key(e1, xs) == key(e2, xs) : +List<Nat>}:
  match xs:
    case Nil{}:
      {==}
    case Con{+x, t}:
      %same_eq(look(x, e1), look(x, e2), and_l(same(look(x, e1), look(x, e2)), agree_b(e1, e2, t), h)) : {look(x, e1) <> key(e1, t) == _ <> key(e2, t) : +List<Nat>}
      %key_agree(e1, e2, t, and_r(same(look(x, e1), look(x, e2)), agree_b(e1, e2, t), h)) : {look(x, e1) <> key(e1, t) == look(x, e1) <> _ : +List<Nat>}
      {==}

# The key is a function of the bytes of the items it lists, and of nothing else.
law key_is_pure:
  for +xs: +List<Item>
  for +e1: +List<Row>
  for +e2: +List<Row>
  for h: {agree_b(e1, e2, xs) == True{} : Bool}
  {key(e1, xs) == key(e2, xs) : +List<Nat>}

def key_is_pure(xs, e1, e2, h):
  key_agree(e1, e2, xs, h)

# (c) A re-gate of an unchanged tree runs nothing (03 "What a land re-runs"): two checkouts whose
# bytes agree on every input a target declares give it the same key, whatever else differs.
law unchanged_tree_runs_nothing:
  for +g: +List<Tgt>
  for +t: Tgt
  for +e1: +List<Row>
  for +e2: +List<Row>
  for h: {agree_b(e1, e2, items(g, t)) == True{} : Bool}
  {key(e1, items(g, t)) == key(e2, items(g, t)) : +List<Nat>}

def unchanged_tree_runs_nothing(g, t, e1, e2, h):
  key_is_pure(items(g, t), e1, e2, h)

def ds(s: Bool, d: Bool, r: Bool, h: {or(not(s), d) == True{} : Bool}, ih: {d == True{} : Bool} -> {r == False{} : Bool}) -> {and(s, r) == False{} : Bool}:
  match s:
    case False{}:
      {==}
    case True{}:
      ih(h)

def key_differs(+e1: +List<Row>, +e2: +List<Row>, +xs: +List<Item>, h: {differs_b(e1, e2, xs) == True{} : Bool}) -> {eq_b(key(e1, xs), key(e2, xs)) == False{} : Bool}:
  match xs:
    case Nil{}:
      Empty.absurd({eq_b([], []) == False{} : Bool}, false_true(h))
    case Con{+x, t}:
      ds(same(look(x, e1), look(x, e2)), differs_b(e1, e2, t), eq_b(key(e1, t), key(e2, t)), h, hd => key_differs(e1, e2, t, hd))

# Definedness: a key that ignored its inputs would satisfy the law above; this one moves with them.
law key_moves_with_input:
  for +xs: +List<Item>
  for +e1: +List<Row>
  for +e2: +List<Row>
  for h: {differs_b(e1, e2, xs) == True{} : Bool}
  {eq_b(key(e1, xs), key(e2, xs)) == False{} : Bool}

def key_moves_with_input(xs, e1, e2, h):
  key_differs(e1, e2, xs, h)

law changed_input_changes_key:
  for +g: +List<Tgt>
  for +t: Tgt
  for +e1: +List<Row>
  for +e2: +List<Row>
  for h: {differs_b(e1, e2, items(g, t)) == True{} : Bool}
  {eq_b(key(e1, items(g, t)), key(e2, items(g, t))) == False{} : Bool}

def changed_input_changes_key(g, t, e1, e2, h):
  key_moves_with_input(items(g, t), e1, e2, h)

# --- the plugin's observer injection, as modelled ------------------------------------------------

# packages/nx-plugin/src/index.ts:2467-2492 gives every bounded-exec target that has a cowshed native
# producer `{dependentTasksOutputFiles: '**/*.node', transitive: true}` and
# `{dependentTasksOutputFiles: '**/dist/lineage/**/*.js', transitive: true}`, whether or not the run
# reads either output.
def observe_t(t: Tgt) -> Tgt:
  match t:
    case Tgt{i, l, o, s, u, r, d, e}:
      Tgt{i, l, o, s, u, r, d, Sel{GNode{}, True{}} <> (Sel{GJs{}, True{}} <> e)}

def observe(g: +List<Tgt>) -> +List<Tgt>:
  match g:
    case Nil{}:
      []
    case Con{t, r}:
      observe_t(infer_t(t)) <> observe(r)

# lineage-js (TypeScript, writes JavaScript), napi-native (Rust, writes the addon) and a Rust runner
# that reads the addon and waits for both
def runner_graph() -> +List<Tgt>:
  [Tgt{1n, Ts{}, OJs{}, [Ts{}], [], [], [], []}, Tgt{2n, Rust{}, ONode{}, [Rust{}], [], [], [], []}, Tgt{3n, Rust{}, ONone{}, [Rust{}], [], [2n], [1n], []}]

# the same runner when the lineage JavaScript's bytes reach its outputs
def runner_reads_lineage() -> +List<Tgt>:
  [Tgt{1n, Ts{}, OJs{}, [Ts{}], [], [], [], []}, Tgt{2n, Rust{}, ONode{}, [Rust{}], [], [], [], []}, Tgt{3n, Rust{}, ONone{}, [Rust{}], [], [2n, 1n], [], []}]

# The injection violates (a): the runner's key names lineage-js's TypeScript output, which its run does
# not read. The counterexample is the one pair (runner 3, producer 1).
law observer_key_refuted:
  {a_viol(observe(runner_graph())) == [Viol{3n, 1n}] : +List<Viol>}

def observer_key_refuted():
  {==}

# The same injection is sound exactly when the lineage bytes reach the runner's outputs: whether they do
# is the spec decision (03 "What a land re-runs").
law observer_key_sound_when_read:
  {a_viol(observe(runner_reads_lineage())) == [] : +List<Viol>}

def observer_key_sound_when_read():
  {==}

# Inference keeps the runner complete: it keys the addon it reads and nothing else.
law inferred_runner_keys_what_it_reads:
  {b_viol(infer(runner_graph())) == [] : +List<Viol>}

def inferred_runner_keys_what_it_reads():
  {==}

# --- definedness: the checks can fail -------------------------------------------------------------

# (b) fails on a runner that waits for the addon it reads and keys no output of it
def unkeyed_read_graph() -> +List<Tgt>:
  [Tgt{2n, Rust{}, ONode{}, [Rust{}], [], [], [], []}, Tgt{3n, Rust{}, ONone{}, [Rust{}], [], [2n], [2n], []}]

law key_complete_can_fail:
  {b_viol(unkeyed_read_graph()) == [Viol{3n, 2n}] : +List<Viol>}

def key_complete_can_fail():
  {==}

# (c) two checkouts that hold different bytes for a declared input differ on it, so a key built from
# them can differ
law key_can_differ:
  {differs_b([Row{ISrc{Rust{}}, 1n}], [Row{ISrc{Rust{}}, 2n}], [ISrc{Rust{}}]) == True{} : Bool}

def key_can_differ():
  {==}
