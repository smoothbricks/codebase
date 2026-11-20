// bend-emit: emits Rust, Go, Goose and TypeScript from a CHECKED Bend book.
//
// Input is Bend's own elaborated core (bend2/bend.ts `Book`, `HTerm`: the terms
// the checker accepted), never the surface text: a def is a lambda spine whose
// pattern matches are `Mat` chains, constructors are `Ctr`, calls are `App` of
// `Ref`. Proof terms are erased: laws become executable conformance checks, the
// proofs themselves are not emitted. Each target also gets a generated driver
// that runs every law over a corpus enumerated from the law's parameter types.
//
// Supported fragment (refused by name otherwise): first-order defs over Nat,
// Bool, List<T> and non-generic user ADTs; no closures, no templates, no lets.
//
// Two lowerings keep the emitted code's cost the model's, not its unary
// encoding's (see `lower`): a def that is exactly structural Nat equality or
// order becomes `a == b` or `a <= b`, checked against its recursive form on
// random pairs before it is emitted; and a call a body evaluates more than
// once is let-bound once.

import * as Bend from '../vendor/bend2/bend.js';

type H = Bend.HTerm;

// ---- types -----------------------------------------------------------------

type Ty = { k: 'nat' } | { k: 'bool' } | { k: 'list'; el: Ty } | { k: 'adt'; name: string };

const NAT: Ty = { k: 'nat' };
const BOOL: Ty = { k: 'bool' };

function tyName(t: Ty): string {
  switch (t.k) {
    case 'nat':
      return 'Nat';
    case 'bool':
      return 'Bool';
    case 'list':
      return `List${tyName(t.el)}`;
    case 'adt':
      return t.name;
  }
}

class Refused extends Error {}

function refuse(what: string): never {
  throw new Refused(`bend-emit refuses: ${what}`);
}

function tyOf(t0: H): Ty {
  const t = Bend.term_force(t0);
  switch (t.$) {
    case 'Ref':
      return named(t.k, []);
    case 'ADT':
      return named(t.k, t.x);
    default:
      return refuse(`type form ${t.$}`);
  }
}

function named(k: string, args: H[]): Ty {
  if (k === 'Nat') return NAT;
  if (k === 'Bool') return BOOL;
  if (k === 'List') {
    const el = args[args.length - 1];
    if (el === undefined) refuse('List without an element type');
    return { k: 'list', el: tyOf(el) };
  }
  return { k: 'adt', name: k };
}

// ---- signatures --------------------------------------------------------------

type Param = { name: string; ty: Ty; v: H };
type Hyp = { a: H; b: H; ty: Ty };
type Sig = { params: Param[]; erased: number[]; hyps: Hyp[]; ret: Ty | null; claim: Hyp | null };

let fresh = 0;
function mkVar(k: string): H {
  const name = `${k.replace(/[^A-Za-z0-9_]/g, '_')}_${String(fresh++)}`;
  return Bend.Var(name, 0);
}

function sigOf(T0: H): Sig {
  const sig: Sig = { params: [], erased: [], hyps: [], ret: null, claim: null };
  let t = Bend.term_force(T0);
  let pos = 0;
  while (t.$ === 'All') {
    const v = mkVar(t.k);
    const A = Bend.term_force(t.A);
    if (t.q.$ === 'None') {
      sig.erased.push(pos);
    } else if (A.$ === 'Eql') {
      sig.hyps.push({ a: A.a, b: A.b, ty: tyOf(A.T) });
    } else {
      sig.params.push({ name: varName(v), ty: tyOf(A), v });
    }
    pos += 1;
    t = Bend.term_force(t.B(v));
  }
  if (t.$ === 'Eql') sig.claim = { a: t.a, b: t.b, ty: tyOf(t.T) };
  else sig.ret = tyOf(t);
  return sig;
}

function varName(v: H): string {
  const t = Bend.term_force(v);
  if (t.$ !== 'Var') refuse('binder that is not a variable');
  return t.k;
}

// ---- the book ---------------------------------------------------------------

type CtorInfo = { adt: string; name: string; fields: { name: string; ty: Ty | 'elem' | 'self' }[] };

const EMPTY_BOOK = Bend.book_nil();
let BOOK = EMPTY_BOOK;

function ctorInfo(k: string): CtorInfo {
  const c = BOOK.ctrs[k];
  if (c === undefined) refuse(`unknown constructor ${k}`);
  const fields: CtorInfo['fields'] = [];
  let t = Bend.term_force(c.T);
  while (t.$ === 'All') {
    const v = mkVar(t.k);
    if (t.q.$ !== 'None') {
      const A = Bend.term_force(t.A);
      if (c.f === 'List') fields.push({ name: t.k, ty: A.$ === 'ADT' ? 'self' : 'elem' });
      else fields.push({ name: t.k, ty: tyOf(A) });
    }
    t = Bend.term_force(t.B(v));
  }
  return { adt: c.f, name: k, fields };
}

function adtCtors(name: string): CtorInfo[] {
  const t = BOOK.tlds[name];
  if (t === undefined || t.$ !== 'ADT') return refuse(`${name} is not a datatype`);
  return t.c.map((c) => ctorInfo(c.k));
}

function fieldTy(f: CtorInfo['fields'][number], scrut: Ty): Ty {
  if (f.ty === 'elem') {
    if (scrut.k !== 'list') refuse('list field outside a list');
    return scrut.el;
  }
  if (f.ty === 'self') return scrut;
  return f.ty;
}

// ---- core IR ------------------------------------------------------------------

type Expr =
  | { k: 'var'; n: string; ty: Ty }
  | { k: 'nat'; v: number }
  | { k: 'bool'; v: boolean }
  | { k: 'add'; a: Expr; b: Expr }
  | { k: 'cmp'; op: Rel; a: Expr; b: Expr }
  | { k: 'append'; el: Ty; a: Expr; b: Expr }
  | { k: 'nil'; el: Ty }
  | { k: 'cons'; el: Ty; h: Expr; t: Expr }
  | { k: 'ctor'; adt: string; ctor: string; args: Expr[] }
  | { k: 'call'; f: string; args: Expr[] };

type Arm = { ctor: string; binds: { n: string; ty: Ty }[]; body: Stmt };
type Stmt =
  | { k: 'ret'; e: Expr }
  | { k: 'let'; n: string; ty: Ty; e: Expr; body: Stmt }
  | { k: 'unreachable' }
  | { k: 'match'; scrut: string; ty: Ty; arms: Arm[]; dflt: Stmt | null };

type Fn = { name: string; params: Param[]; ret: Ty; body: Stmt; recursive: boolean };
type Law = {
  name: string;
  params: Param[];
  hyps: { a: Expr; b: Expr; ty: Ty }[];
  a: Expr;
  b: Expr;
  ty: Ty;
  proven: boolean;
};

const SIGS = new Map<string, Sig>();
function sig(k: string): Sig {
  const s = SIGS.get(k);
  if (s !== undefined) return s;
  const t = BOOK.tlds[k];
  if (t === undefined || t.$ !== 'Def') return refuse(`${k} is not a def`);
  const r = sigOf(t.T);
  SIGS.set(k, r);
  return r;
}

const INTRINSIC = new Set(['Nat.add', 'List.append']);

function compileSpine(t0: H, args: H[], env: Map<string, Ty>, ret: Ty): Stmt {
  const t = Bend.term_force(t0);
  if (t.$ === 'Lam') {
    const a = args[0];
    if (a === undefined) refuse('a closure value (not first-order)');
    return compileSpine(t.f(a), args.slice(1), env, ret);
  }
  if (t.$ === 'Mat') {
    const a = args[0];
    if (a === undefined) refuse('a match without a scrutinee');
    const scrut = varName(a);
    const sty = env.get(scrut);
    if (sty === undefined) refuse(`untyped scrutinee ${scrut}`);
    const arms: Arm[] = [];
    let cur: H = t;
    for (;;) {
      const c: H = Bend.term_force(cur);
      if (c.$ !== 'Mat') break;
      const info = ctorInfo(c.k);
      const binds = info.fields.map((f) => {
        const v = mkVar(f.name);
        return { n: varName(v), ty: fieldTy(f, sty), v };
      });
      const env2 = new Map(env);
      for (const b of binds) env2.set(b.n, b.ty);
      arms.push({
        ctor: c.k,
        binds: binds.map(({ n, ty }) => ({ n, ty })),
        body: compileSpine(c.h, [...binds.map((b) => b.v), ...args.slice(1)], env2, ret),
      });
      cur = c.m;
    }
    const tail = Bend.term_force(cur);
    const dflt = tail.$ === 'Efq' ? null : compileSpine(tail, args, env, ret);
    return { k: 'match', scrut, ty: sty, arms, dflt };
  }
  if (t.$ === 'Efq') return { k: 'unreachable' };
  let e: H = t;
  for (const a of args) e = Bend.App(e, a);
  return { k: 'ret', e: expr(e, env, ret) };
}

function expr(t0: H, env: Map<string, Ty>, want: Ty): Expr {
  const t = Bend.term_force(t0);
  switch (t.$) {
    case 'Var': {
      const ty = env.get(t.k);
      if (ty === undefined) refuse(`free variable ${t.k}`);
      return { k: 'var', n: t.k, ty };
    }
    case 'Ann':
      return expr(t.x, env, want);
    case 'Lit':
      if (t.k !== 'Nat') refuse(`${t.k} literal`);
      return { k: 'nat', v: t.v };
    case 'Ctr':
      return ctor(t.k, t.x, env, want);
    case 'App':
    case 'Ref': {
      const [head, all] = Bend.term_unapply(t);
      const h = Bend.term_force(head);
      if (h.$ !== 'Ref') refuse(`a call whose head is ${h.$}`);
      if (h.k === 'Nat.add') {
        const [x, y] = all;
        if (x === undefined || y === undefined) refuse('Nat.add arity');
        return { k: 'add', a: expr(x, env, NAT), b: expr(y, env, NAT) };
      }
      if (h.k === 'List.append') {
        const [x, y] = all.slice(all.length - 2);
        if (x === undefined || y === undefined) refuse('List.append arity');
        if (want.k !== 'list') refuse('List.append outside a list context');
        return { k: 'append', el: want.el, a: expr(x, env, want), b: expr(y, env, want) };
      }
      const s = sig(h.k);
      const live = all.filter((_, i) => !s.erased.includes(i));
      if (live.length !== s.params.length) refuse(`a partial application of ${h.k}`);
      const args = live.map((a, i) => {
        const p = s.params[i];
        if (p === undefined) return refuse(`argument ${i} of ${h.k}`);
        return expr(a, env, p.ty);
      });
      USED.add(h.k);
      return { k: 'call', f: h.k, args };
    }
    default:
      return refuse(`term form ${t.$} in an expression`);
  }
}

function ctor(k: string, xs: H[], env: Map<string, Ty>, want: Ty): Expr {
  if (k === 'Zero') return { k: 'nat', v: 0 };
  if (k === 'Succ') {
    const p = xs[0];
    if (p === undefined) refuse('Succ without a field');
    return { k: 'add', a: expr(p, env, NAT), b: { k: 'nat', v: 1 } };
  }
  if (k === 'True' || k === 'False') return { k: 'bool', v: k === 'True' };
  if (k === 'Nil' || k === 'Con') {
    if (want.k !== 'list') refuse(`${k} where ${tyName(want)} is expected`);
    if (k === 'Nil') return { k: 'nil', el: want.el };
    const [h, tl] = xs.slice(xs.length - 2);
    if (h === undefined || tl === undefined) refuse('Con without fields');
    return { k: 'cons', el: want.el, h: expr(h, env, want.el), t: expr(tl, env, want) };
  }
  const info = ctorInfo(k);
  const live = xs.slice(xs.length - info.fields.length);
  return {
    k: 'ctor',
    adt: info.adt,
    ctor: k,
    args: live.map((x, i) => {
      const f = info.fields[i];
      if (f === undefined) return refuse(`field ${i} of ${k}`);
      return expr(x, env, fieldTy(f, want));
    }),
  };
}

const USED = new Set<string>();

function compileFn(k: string): Fn {
  const t = BOOK.tlds[k];
  if (t === undefined || t.$ !== 'Def' || t.v === null) return refuse(`${k} has no body`);
  const s = sig(k);
  if (s.ret === null) refuse(`${k} is a law, not a function`);
  const env = new Map<string, Ty>();
  for (const p of s.params) env.set(p.name, p.ty);
  // erased positions still take a binder in the spine
  const spine: H[] = [];
  let pi = 0;
  const total = s.params.length + s.erased.length + s.hyps.length;
  for (let i = 0; i < total; i++) {
    if (s.erased.includes(i)) spine.push(mkVar('erased'));
    else {
      const p = s.params[pi++];
      if (p === undefined) refuse(`parameter ${i} of ${k}`);
      spine.push(p.v);
    }
  }
  const before = new Set(USED);
  USED.clear();
  const body = compileSpine(t.v, spine, env, s.ret);
  const recursive = USED.has(k);
  for (const u of before) USED.add(u);
  return { name: k, params: s.params, ret: s.ret, body, recursive };
}

// A law is proven when its def has a body the checker accepted; a law with no
// def is an open claim: the driver still runs it, as a strong test, not a proof.
function lawProven(k: string): boolean {
  const t = BOOK.tlds[k];
  return t !== undefined && t.$ === 'Def' && t.v !== null;
}

function compileLaw(k: string): Law {
  const s = sig(k);
  if (s.claim === null) refuse(`${k} is not an equality law`);
  const env = new Map<string, Ty>();
  for (const p of s.params) env.set(p.name, p.ty);
  return {
    name: k,
    params: s.params,
    proven: lawProven(k),
    hyps: s.hyps.map((h) => ({ a: expr(h.a, env, h.ty), b: expr(h.b, env, h.ty), ty: h.ty })),
    a: expr(s.claim.a, env, s.claim.ty),
    b: expr(s.claim.b, env, s.claim.ty),
    ty: s.claim.ty,
  };
}

// ---- lowering: the model's meaning at the target's cost ------------------------

// A Nat or Bool value of the IR's evaluator.
type V = number | boolean;

// Evaluates a def over Nat and Bool, the reference a lowering is checked against.
// Lists and ADTs are outside it: no lowering reaches them.
function evalFn(fns: ReadonlyMap<string, Fn>, f: Fn, args: readonly V[]): V {
  const env = new Map<string, V>();
  f.params.forEach((p, i) => {
    const a = args[i];
    if (a !== undefined) env.set(p.name, a);
  });
  return evalStmt(fns, f.body, env);
}

function evalStmt(fns: ReadonlyMap<string, Fn>, s: Stmt, env: Map<string, V>): V {
  switch (s.k) {
    case 'ret':
      return evalExpr(fns, s.e, env);
    case 'let':
      return evalStmt(fns, s.body, new Map(env).set(s.n, evalExpr(fns, s.e, env)));
    case 'unreachable':
      throw new Error('evaluated an unreachable arm');
    case 'match': {
      const v = env.get(s.scrut);
      const arm = (c: string): Arm | undefined => s.arms.find((a) => a.ctor === c);
      const take = (a: Arm | undefined, bound: V[]): V => {
        if (a === undefined) {
          if (s.dflt === null) throw new Error(`no arm for ${s.scrut}`);
          return evalStmt(fns, s.dflt, env);
        }
        const env2 = new Map(env);
        a.binds.forEach((b, i) => {
          const x = bound[i];
          if (x !== undefined) env2.set(b.n, x);
        });
        return evalStmt(fns, a.body, env2);
      };
      if (typeof v === 'number') return v === 0 ? take(arm('Zero'), []) : take(arm('Succ'), [v - 1]);
      if (typeof v === 'boolean') return take(arm(v ? 'True' : 'False'), []);
      throw new Error(`unbound scrutinee ${s.scrut}`);
    }
  }
}

function evalExpr(fns: ReadonlyMap<string, Fn>, e: Expr, env: Map<string, V>): V {
  const nat = (x: Expr): number => {
    const v = evalExpr(fns, x, env);
    if (typeof v !== 'number') throw new Error('a Bool where a Nat is expected');
    return v;
  };
  switch (e.k) {
    case 'var': {
      const v = env.get(e.n);
      if (v === undefined) throw new Error(`unbound ${e.n}`);
      return v;
    }
    case 'nat':
      return e.v;
    case 'bool':
      return e.v;
    case 'add':
      return nat(e.a) + nat(e.b);
    case 'cmp':
      return holds(e.op, nat(e.a), nat(e.b));
    case 'call': {
      const f = fns.get(e.f);
      if (f === undefined) throw new Error(`no def ${e.f}`);
      return evalFn(
        fns,
        f,
        e.args.map((a) => evalExpr(fns, a, env)),
      );
    }
    case 'append':
    case 'nil':
    case 'cons':
    case 'ctor':
      throw new Error(`${e.k} is outside the Nat/Bool evaluator`);
  }
}

// The Nat relations a structural def can be lowered to.
type Rel = '==' | '<=';

function holds(op: Rel, a: number, b: number): boolean {
  return op === '==' ? a === b : a <= b;
}

// The relation `f` is exactly, as a structural recursion on two Nats, if any:
//   `==`, admission.bend's `same`: f(0, 0) = True; f(0, 1+q) = False; f(1+p, 0) = False; f(1+p, 1+q) = f(p, q).
//   `<=`, admission.bend's `le`:   f(0, b) = True; f(1+p, 0) = False; f(1+p, 1+q) = f(p, q).
function natRelation(f: Fn): Rel | undefined {
  const [a, b] = f.params;
  if (f.params.length !== 2 || a === undefined || b === undefined) return undefined;
  if (a.ty.k !== 'nat' || b.ty.k !== 'nat' || f.ret.k !== 'bool') return undefined;
  const arm = (s: Stmt | undefined, scrut: string, ctor: string): Arm | undefined =>
    s?.k === 'match' && s.scrut === scrut && s.dflt === null ? s.arms.find((x) => x.ctor === ctor) : undefined;
  const is = (s: Stmt | undefined, v: boolean): boolean => s?.k === 'ret' && s.e.k === 'bool' && s.e.v === v;
  const zero = arm(f.body, a.name, 'Zero')?.body;
  const succ = arm(f.body, a.name, 'Succ');
  const p = succ?.binds[0];
  const both = arm(succ?.body, b.name, 'Succ');
  const q = both?.binds[0];
  const rec = both?.body;
  const descends =
    p !== undefined &&
    q !== undefined &&
    rec?.k === 'ret' &&
    rec.e.k === 'call' &&
    rec.e.f === f.name &&
    rec.e.args.length === 2 &&
    rec.e.args[0]?.k === 'var' &&
    rec.e.args[0].n === p.n &&
    rec.e.args[1]?.k === 'var' &&
    rec.e.args[1].n === q.n &&
    is(arm(succ?.body, b.name, 'Zero')?.body, false);
  if (!descends) return undefined;
  if (is(arm(zero, b.name, 'Zero')?.body, true) && is(arm(zero, b.name, 'Succ')?.body, false)) return '==';
  if (is(zero, true)) return '<=';
  return undefined;
}

// SplitMix64 over bigint, the generators' stream, for the lowering checks.
function* pairs(seed: bigint, n: number, below: bigint): Generator<[number, number]> {
  const M = (1n << 64n) - 1n;
  let s = seed;
  const next = (): bigint => {
    s = (s + 0x9e3779b97f4a7c15n) & M;
    let z = s;
    z = ((z ^ (z >> 30n)) * 0xbf58476d1ce4e5b9n) & M;
    z = ((z ^ (z >> 27n)) * 0x94d049bb133111ebn) & M;
    return z ^ (z >> 31n);
  };
  for (let i = 0; i < n; i++) {
    const x = Number(next() % below);
    // a third equal, a third adjacent, a third independent
    yield [x, i % 3 === 0 ? x : i % 3 === 1 ? x + 1 : Number(next() % below)];
  }
}

// `f` lowered to `a == b` or `a <= b` when it is that structural relation and
// agrees with it on every one of 512 seeded pairs below 200 (each way round).
function lowerNatRelation(fns: ReadonlyMap<string, Fn>, f: Fn): Fn {
  const op = natRelation(f);
  const [a, b] = f.params;
  if (op === undefined || a === undefined || b === undefined) return f;
  for (const [x, y] of pairs(1000003n, 512, 200n)) {
    for (const [u, v] of [
      [x, y],
      [y, x],
    ] as const) {
      if (evalFn(fns, f, [u, v]) !== holds(op, u, v))
        refuse(`${f.name} reads as Nat ${op} but ${f.name}(${u}, ${v}) is not ${holds(op, u, v)}`);
    }
  }
  return {
    ...f,
    recursive: false,
    body: { k: 'ret', e: { k: 'cmp', op, a: { k: 'var', n: a.name, ty: NAT }, b: { k: 'var', n: b.name, ty: NAT } } },
  };
}

function children(e: Expr): Expr[] {
  switch (e.k) {
    case 'add':
    case 'cmp':
    case 'append':
      return [e.a, e.b];
    case 'cons':
      return [e.h, e.t];
    case 'ctor':
    case 'call':
      return e.args;
    case 'var':
    case 'nat':
    case 'bool':
    case 'nil':
      return [];
  }
}

function withChildren(e: Expr, f: (x: Expr) => Expr): Expr {
  switch (e.k) {
    case 'add':
    case 'cmp':
    case 'append':
      return { ...e, a: f(e.a), b: f(e.b) };
    case 'cons':
      return { ...e, h: f(e.h), t: f(e.t) };
    case 'ctor':
    case 'call':
      return { ...e, args: e.args.map(f) };
    case 'var':
    case 'nat':
    case 'bool':
    case 'nil':
      return e;
  }
}

// Every call a returned expression evaluates more than once, evaluated once:
// the fragment is pure and each target evaluates every argument, so binding a
// repeated call before the return changes no value, only how often it is
// computed. Bound innermost first, so each binding's calls are bound before it.
function shareCalls(fns: ReadonlyMap<string, Fn>, f: Fn): Fn {
  let bound = 0;
  const ret = (e: Expr): Stmt => {
    const seen = new Map<string, number>();
    const count = (x: Expr): void => {
      if (x.k === 'call') seen.set(JSON.stringify(x), (seen.get(JSON.stringify(x)) ?? 0) + 1);
      children(x).forEach(count);
    };
    count(e);
    const lets: { n: string; ty: Ty; e: Expr }[] = [];
    const names = new Map<string, Expr>();
    const share = (x: Expr): Expr => {
      const y = withChildren(x, share);
      if (x.k !== 'call') return y;
      const key = JSON.stringify(x);
      if ((seen.get(key) ?? 0) < 2) return y;
      const named = names.get(key);
      if (named !== undefined) return named;
      const ty = fns.get(x.f)?.ret;
      if (ty === undefined) throw new Error(`no def ${x.f}`);
      const v: Expr = { k: 'var', n: `c${bound++}__`, ty };
      lets.push({ n: v.n, ty, e: y });
      names.set(key, v);
      return v;
    };
    const body = share(e);
    return lets.reduceRight<Stmt>((s, l) => ({ k: 'let', ...l, body: s }), { k: 'ret', e: body });
  };
  const stmt = (s: Stmt): Stmt => {
    switch (s.k) {
      case 'ret':
        return ret(s.e);
      case 'let':
        return { ...s, body: stmt(s.body) };
      case 'unreachable':
        return s;
      case 'match':
        return {
          ...s,
          arms: s.arms.map((a) => ({ ...a, body: stmt(a.body) })),
          dflt: s.dflt === null ? null : stmt(s.dflt),
        };
    }
  };
  return { ...f, body: stmt(f.body) };
}

// Both lowerings over a closed set of defs, in its order.
function lower(order: readonly Fn[]): Fn[] {
  const fns = new Map(order.map((f) => [f.name, f]));
  const rel = order.map((f) => lowerNatRelation(fns, f));
  const lowered = new Map(rel.map((f) => [f.name, f]));
  return rel.map((f) => shareCalls(lowered, f));
}

// ---- corpus: values enumerated from the law parameter types --------------------

type Val = { ctor: string; ty: Ty; args: Val[] } | { nat: number } | { bool: boolean } | { list: Val[]; el: Ty };

function enumerate(t: Ty, depth: number): Val[] {
  switch (t.k) {
    case 'nat':
      return [0, 1, 2].map((n) => ({ nat: n }));
    case 'bool':
      return [{ bool: false }, { bool: true }];
    case 'list': {
      // one element per constructor first, so short traces already mix every op
      const els = enumerate(t.el, depth + 1).slice(0, depth === 0 ? 4 : 2);
      const out: Val[] = [{ list: [], el: t.el }];
      let layer: Val[][] = [[]];
      for (let len = 1; len <= (depth === 0 ? LIST_LEN : 2); len++) {
        layer = layer.flatMap((xs) => els.map((e) => [...xs, e]));
        for (const xs of layer) out.push({ list: xs, el: t.el });
      }
      return out;
    }
    case 'adt': {
      const perCtor = adtCtors(t.name).map((c) => {
        let rows: Val[][] = [[]];
        for (const f of c.fields) {
          const fv = enumerate(fieldTy(f, t), depth + 1).slice(0, 3);
          rows = rows.flatMap((r) => fv.map((v) => [...r, v]));
        }
        return rows.map((args): Val => ({ ctor: c.name, ty: t, args }));
      });
      const out: Val[] = [];
      for (let i = 0; perCtor.some((vs) => i < vs.length); i++) {
        for (const vs of perCtor) {
          const v = vs[i];
          if (v !== undefined) out.push(v);
        }
      }
      return out;
    }
  }
}

let LIST_LEN = 3;

function corpus(params: Param[], cap: number): Val[][] {
  // The cross product is never materialised: its rows are decoded by index, the first parameter most
  // significant, so a law of several structured parameters costs `cap` rows and not their product.
  const sets = params.map((p) => enumerate(p.ty, 0));
  const total = sets.reduce((n, vs) => n * vs.length, 1);
  const pick = (index: number): Val[] => {
    const row: Val[] = [];
    let rest = index;
    for (let k = sets.length - 1; k >= 0; k--) {
      const vs = sets[k];
      const v = vs?.[rest % vs.length];
      if (vs === undefined || v === undefined) throw new Error('corpus index');
      row.unshift(v);
      rest = Math.floor(rest / vs.length);
    }
    return row;
  };
  const stride = total / cap;
  return Array.from({ length: Math.min(total, cap) }, (_, i) => pick(total <= cap ? i : Math.floor(i * stride)));
}

// the same canonical text every target's show_* prints, for counterexample args
function showVal(v: Val): string {
  if ('nat' in v) return String(v.nat);
  if ('bool' in v) return String(v.bool);
  if ('list' in v) return `[${v.list.map(showVal).join(',')}]`;
  return v.args.length === 0 ? v.ctor : `${v.ctor}(${v.args.map(showVal).join(',')})`;
}

function valExpr(v: Val): Expr {
  if ('nat' in v) return { k: 'nat', v: v.nat };
  if ('bool' in v) return { k: 'bool', v: v.bool };
  if ('list' in v) {
    let e: Expr = { k: 'nil', el: v.el };
    for (const x of [...v.list].reverse()) e = { k: 'cons', el: v.el, h: valExpr(x), t: e };
    return e;
  }
  if (v.ty.k !== 'adt') return refuse('constructor of a builtin type');
  return { k: 'ctor', adt: v.ty.name, ctor: v.ctor, args: v.args.map(valExpr) };
}

// ---- targets -------------------------------------------------------------------

type Program = {
  fns: Fn[];
  laws: Law[];
  adts: string[];
  lists: Ty[];
  cases: Map<string, Val[][]>;
  props: number;
  propLen: number;
};

// Seeded generators, the same splitmix64 stream in every target, so the
// property run is a second differential: equal seeds give equal inputs.
const MIX = { inc: '0x9E3779B97F4A7C15', m1: '0xBF58476D1CE4E5B9', m2: '0x94D049BB133111EB' };

function genTys(p: Program): Ty[] {
  return [NAT, BOOL, ...p.lists, ...p.adts.map((name): Ty => ({ k: 'adt', name }))];
}

function seedOf(lawIndex: number): string {
  return String(1000003 * (lawIndex + 1));
}

function id(k: string): string {
  return k.replace(/\./g, '_');
}

function collectTypes(p: Pick<Program, 'fns' | 'laws' | 'cases'>): { adts: string[]; lists: Ty[] } {
  const adts = new Set<string>();
  const lists = new Map<string, Ty>();
  const see = (t: Ty): void => {
    if (t.k === 'adt' && !adts.has(t.name)) {
      adts.add(t.name);
      for (const c of adtCtors(t.name)) for (const f of c.fields) see(fieldTy(f, t));
    }
    if (t.k === 'list' && !lists.has(tyName(t))) {
      lists.set(tyName(t), t);
      see(t.el);
    }
  };
  for (const f of p.fns) {
    for (const x of f.params) see(x.ty);
    see(f.ret);
  }
  for (const l of p.laws) {
    for (const x of l.params) see(x.ty);
    see(l.ty);
  }
  return { adts: [...adts], lists: [...lists.values()] };
}

// ---------- Rust

function rsTy(t: Ty): string {
  switch (t.k) {
    case 'nat':
      return 'u64';
    case 'bool':
      return 'bool';
    case 'list':
      return `Rc<List<${rsTy(t.el)}>>`;
    case 'adt':
      return t.name;
  }
}

function rsExpr(e: Expr): string {
  switch (e.k) {
    case 'var':
      return e.ty.k === 'nat' || e.ty.k === 'bool' ? e.n : `${e.n}.clone()`;
    case 'nat':
      return `${e.v}u64`;
    case 'bool':
      return String(e.v);
    case 'add':
      return `nat_add(${rsExpr(e.a)}, ${rsExpr(e.b)})`;
    case 'cmp':
      return `(${rsExpr(e.a)} ${e.op} ${rsExpr(e.b)})`;
    case 'append':
      return `list_append(&${rsExpr(e.a)}, ${rsExpr(e.b)})`;
    case 'nil':
      return 'Rc::new(List::Nil)';
    case 'cons':
      return `Rc::new(List::Con(${rsExpr(e.h)}, ${rsExpr(e.t)}))`;
    case 'ctor':
      return e.args.length === 0 ? `${e.adt}::${e.ctor}` : `${e.adt}::${e.ctor}(${e.args.map(rsExpr).join(', ')})`;
    case 'call':
      return `${id(e.f)}(${e.args.map(rsExpr).join(', ')})`;
  }
}

function rsStmt(s: Stmt, ind: string): string {
  switch (s.k) {
    case 'ret':
      return `${ind}return ${rsExpr(s.e)};\n`;
    case 'let':
      return `${ind}let ${s.n}: ${rsTy(s.ty)} = ${rsExpr(s.e)};\n${rsStmt(s.body, ind)}`;
    case 'unreachable':
      return `${ind}unreachable!()\n`;
    case 'match': {
      const t = s.ty;
      const arm = (c: string): Arm | undefined => s.arms.find((a) => a.ctor === c);
      const body = (a: Arm | undefined, ind2: string): string =>
        a !== undefined ? rsStmt(a.body, ind2) : s.dflt !== null ? rsStmt(s.dflt, ind2) : `${ind2}unreachable!()\n`;
      if (t.k === 'nat') {
        const z = arm('Zero');
        const p = arm('Succ');
        const pb = p?.binds[0];
        return `${ind}if ${s.scrut} == 0 {\n${body(z, `${ind}  `)}${ind}} else {\n${pb !== undefined ? `${ind}  let ${pb.n}: u64 = ${s.scrut} - 1;\n` : ''}${body(p, `${ind}  `)}${ind}}\n`;
      }
      if (t.k === 'bool')
        return `${ind}if ${s.scrut} {\n${body(arm('True'), `${ind}  `)}${ind}} else {\n${body(arm('False'), `${ind}  `)}${ind}}\n`;
      if (t.k === 'list') {
        const c = arm('Con');
        const [hb, tb] = c?.binds ?? [];
        return `${ind}match &*${s.scrut} {\n${ind}  List::Nil => {\n${body(arm('Nil'), `${ind}    `)}${ind}  }\n${ind}  List::Con(h__, t__) => {\n${hb !== undefined ? `${ind}    let ${hb.n}: ${rsTy(hb.ty)} = h__.clone();\n` : ''}${tb !== undefined ? `${ind}    let ${tb.n}: ${rsTy(tb.ty)} = t__.clone();\n` : ''}${body(c, `${ind}    `)}${ind}  }\n${ind}}\n`;
      }
      const ctors = adtCtors(t.name);
      let out = `${ind}match &${s.scrut} {\n`;
      for (const c of ctors) {
        const a = arm(c.name);
        const fs = c.fields.map((_, i) => `f${i}__`);
        out += `${ind}  ${t.name}::${c.name}${fs.length ? `(${fs.join(', ')})` : ''} => {\n`;
        a?.binds.forEach((b, i) => {
          out += `${ind}    let ${b.n}: ${rsTy(b.ty)} = f${i}__.clone();\n`;
        });
        out += `${body(a, `${ind}    `)}${ind}  }\n`;
      }
      return `${out}${ind}}\n`;
    }
  }
}

function rsShow(t: Ty, x: string): string {
  return `show_${tyName(t)}(&${x})`;
}

function emitRust(p: Program, module: string | null = null): string {
  let o =
    module !== null
      ? `// GENERATED by bend-emit from a checked Bend book. Do not edit.\n${module}#![allow(non_snake_case, unused_variables, unused_parens, unreachable_code, dead_code, clippy::all, clippy::pedantic, clippy::nursery)]\nuse std::rc::Rc;\n\n`
      : '// GENERATED by bend-emit from a checked Bend book. Do not edit.\n#![allow(non_snake_case, unused_variables, unused_parens, unreachable_code, dead_code)]\nuse std::rc::Rc;\n\n';
  o += '#[derive(Clone, PartialEq)]\npub enum List<T> { Nil, Con(T, Rc<List<T>>) }\n\n';
  o += 'fn nat_add(a: u64, b: u64) -> u64 { let s = a + b; assert!(s < (1u64 << 48), "Nat past 2^48"); s }\n';
  o +=
    'fn list_append<T: Clone>(a: &Rc<List<T>>, b: Rc<List<T>>) -> Rc<List<T>> {\n  let mut xs = Vec::new(); let mut c = a.clone();\n  while let List::Con(h, t) = &*c { xs.push(h.clone()); let n = t.clone(); c = n; }\n  let mut r = b; for h in xs.into_iter().rev() { r = Rc::new(List::Con(h, r)); } r\n}\n\n';
  for (const a of p.adts) {
    o += `#[derive(Clone, PartialEq)]\npub enum ${a} { ${adtCtors(a)
      .map((c) =>
        c.fields.length
          ? `${c.name}(${c.fields.map((f) => rsTy(fieldTy(f, { k: 'adt', name: a }))).join(', ')})`
          : c.name,
      )
      .join(', ')} }\n`;
    // Each ADT's text is written into one caller buffer (`write_*`), so a show allocates once, never a String per field.
    o += `pub fn write_${a}(x: &${a}, out: &mut String) {\n  match x {\n${adtCtors(a)
      .map((c) => {
        const fs = c.fields.map((_, i) => `f${i}`);
        const body = fs.length
          ? `{ out.push_str("${c.name}("); ${c.fields.map((f, i) => `${i ? `out.push(','); ` : ''}write_${tyName(fieldTy(f, { k: 'adt', name: a }))}(f${i}, out);`).join(' ')} out.push(')'); }`
          : `out.push_str("${c.name}"),`;
        return `    ${a}::${c.name}${fs.length ? `(${fs.join(', ')})` : ''} => ${body}\n`;
      })
      .join('')}  }\n}\n`;
    o += `pub fn show_${a}(x: &${a}) -> String { let mut s = String::new(); write_${a}(x, &mut s); s }\n\n`;
  }
  o +=
    'pub fn write_Nat(x: &u64, out: &mut String) { out.push_str(&x.to_string()); }\npub fn write_Bool(x: &bool, out: &mut String) { out.push_str(if *x { "true" } else { "false" }); }\n';
  o += 'pub fn show_Nat(x: &u64) -> String { x.to_string() }\npub fn show_Bool(x: &bool) -> String { x.to_string() }\n';
  for (const l of p.lists) {
    if (l.k !== 'list') continue;
    o += `pub fn write_${tyName(l)}(x: &Rc<List<${rsTy(l.el)}>>, out: &mut String) {\n  out.push('['); let mut c = x.clone(); let mut first = true;\n  while let List::Con(h, t) = &*c { if !first { out.push(','); } first = false; write_${tyName(l.el)}(h, out); let n = t.clone(); c = n; }\n  out.push(']');\n}\n`;
    o += `pub fn show_${tyName(l)}(x: &Rc<List<${rsTy(l.el)}>>) -> String { let mut s = String::new(); write_${tyName(l)}(x, &mut s); s }\n`;
  }
  for (const f of p.fns) {
    o += `\npub fn ${id(f.name)}(${f.params.map((x) => `${x.name}: ${rsTy(x.ty)}`).join(', ')}) -> ${rsTy(f.ret)} {\n${rsStmt(f.body, '  ')}}\n`;
  }
  for (const l of p.laws) {
    o += `\npub fn law_${id(l.name)}(${l.params.map((x) => `${x.name}: ${rsTy(x.ty)}`).join(', ')}) -> (bool, String) {\n`;
    for (const h of l.hyps)
      o += `  if ${rsShow(h.ty, `(${rsExpr(h.a)})`)} != ${rsShow(h.ty, `(${rsExpr(h.b)})`)} { return (true, "vacuous".to_string()); }\n`;
    o += `  let a = ${rsExpr(l.a)};\n  let b = ${rsExpr(l.b)};\n  let sa = ${rsShow(l.ty, 'a')};\n  (sa == ${rsShow(l.ty, 'b')}, sa)\n}\n`;
  }
  if (module !== null) return o + emitRustCorpus(p);
  o += '\nfn main() {\n  let mut failed = 0u64;\n';
  for (const l of p.laws) {
    const cs = p.cases.get(l.name) ?? [];
    o += `  { let mut pass = 0u64; let n = ${cs.length}u64;\n`;
    cs.forEach((row, i) => {
      o += `    { let (ok, v) = law_${id(l.name)}(${row.map((v) => rsExpr(valExpr(v))).join(', ')}); println!("law ${l.name} ${i} {} {}", ok, v); if ok { pass += 1; } }\n`;
    });
    o += `    println!("summary ${l.name} {}/{} ${l.proven ? 'proven' : 'open'}", pass, n); failed += n - pass; }\n`;
  }
  o += '  props(&mut failed);\n  std::process::exit(if failed == 0 { 0 } else { 1 });\n}\n';
  o += `\nstruct Rng(u64);\nimpl Rng { fn next(&mut self) -> u64 { self.0 = self.0.wrapping_add(${MIX.inc}); let mut z = self.0; z = (z ^ (z >> 30)).wrapping_mul(${MIX.m1}); z = (z ^ (z >> 27)).wrapping_mul(${MIX.m2}); z ^ (z >> 31) } }\n`;
  for (const t of genTys(p)) {
    const n = tyName(t);
    if (t.k === 'nat') o += 'fn gen_Nat(r: &mut Rng, _m: u64) -> u64 { r.next() % 3 }\n';
    else if (t.k === 'bool') o += 'fn gen_Bool(r: &mut Rng, _m: u64) -> bool { r.next() % 2 == 0 }\n';
    else if (t.k === 'list')
      o += `fn gen_${n}(r: &mut Rng, m: u64) -> ${rsTy(t)} { let k = r.next() % (m + 1); let mut xs = Vec::new(); for _ in 0..k { xs.push(gen_${tyName(t.el)}(r, 3)); } let mut l: ${rsTy(t)} = Rc::new(List::Nil); for x in xs.into_iter().rev() { l = Rc::new(List::Con(x, l)); } l }\n`;
    else if (t.k === 'adt') {
      const cs = adtCtors(t.name);
      o += `fn gen_${n}(r: &mut Rng, _m: u64) -> ${n} {\n  match r.next() % ${cs.length} {\n${cs.map((c, i) => `    ${i === cs.length - 1 ? '_' : String(i)} => { ${c.fields.map((f, j) => `let f${j} = gen_${tyName(fieldTy(f, t))}(r, 3); `).join('')}${n}::${c.name}${c.fields.length ? `(${c.fields.map((_, j) => `f${j}`).join(', ')})` : ''} }\n`).join('')}  }\n}\n`;
    }
  }
  o += '\nfn props(failed: &mut u64) {\n';
  p.laws.forEach((l, li) => {
    o += `  { let mut r = Rng(${seedOf(li)}); let mut pass = 0u64;\n    for i in 0..${p.props} { ${l.params.map((x) => `let ${x.name} = gen_${tyName(x.ty)}(&mut r, ${p.propLen}); `).join('')}let (ok, v) = law_${id(l.name)}(${l.params.map((x) => x.name).join(', ')}); println!("prop ${l.name} {} {} {}", i, ok, v); if ok { pass += 1; } }\n    println!("propsummary ${l.name} {}/${p.props}", pass); *failed += ${p.props} - pass; }\n`;
  });
  o += '}\n';
  return o;
}

// Module mode: the model, its laws, the seeded generators and each law's
// enumerated corpus, as a Rust module a binding driver in the code's own crate
// includes. The driver runs the model and the code on the same inputs.
function emitRustCorpus(p: Program): string {
  let o = `\npub struct Rng(pub u64);\nimpl Rng { pub fn next(&mut self) -> u64 { self.0 = self.0.wrapping_add(${MIX.inc}); let mut z = self.0; z = (z ^ (z >> 30)).wrapping_mul(${MIX.m1}); z = (z ^ (z >> 27)).wrapping_mul(${MIX.m2}); z ^ (z >> 31) } }\n`;
  for (const t of genTys(p)) {
    const n = tyName(t);
    if (t.k === 'nat') o += 'pub fn gen_Nat(r: &mut Rng, _m: u64) -> u64 { r.next() % 3 }\n';
    else if (t.k === 'bool') o += 'pub fn gen_Bool(r: &mut Rng, _m: u64) -> bool { r.next() % 2 == 0 }\n';
    else if (t.k === 'list')
      o += `pub fn gen_${n}(r: &mut Rng, m: u64) -> ${rsTy(t)} { let k = r.next() % (m + 1); let mut xs = Vec::new(); for _ in 0..k { xs.push(gen_${tyName(t.el)}(r, 3)); } let mut l: ${rsTy(t)} = Rc::new(List::Nil); for x in xs.into_iter().rev() { l = Rc::new(List::Con(x, l)); } l }\n`;
    else if (t.k === 'adt') {
      const cs = adtCtors(t.name);
      o += `pub fn gen_${n}(r: &mut Rng, _m: u64) -> ${n} {\n  match r.next() % ${cs.length} {\n${cs.map((c, i) => `    ${i === cs.length - 1 ? '_' : String(i)} => { ${c.fields.map((f, j) => `let f${j} = gen_${tyName(fieldTy(f, t))}(r, 3); `).join('')}${n}::${c.name}${c.fields.length ? `(${c.fields.map((_, j) => `f${j}`).join(', ')})` : ''} }\n`).join('')}  }\n}\n`;
    }
  }
  p.laws.forEach((l, li) => {
    const tys = l.params.map((x) => rsTy(x.ty));
    const tuple = tys.length === 1 ? `(${tys[0]},)` : `(${tys.join(', ')})`;
    const rows = p.cases.get(l.name) ?? [];
    o += `\n/// ${l.name} (${l.proven ? 'proven' : 'open'}): ${rows.length} enumerated cases.\npub fn corpus_${id(l.name)}() -> Vec<${tuple}> {\n  vec![\n${rows.map((row) => `    (${row.map((v) => rsExpr(valExpr(v))).join(', ')},),\n`).join('')}  ]\n}\n`;
    o += `/// ${l.name}'s seed and draw size: the same stream every target draws.\npub const SEED_${id(l.name).toUpperCase()}: u64 = ${seedOf(li)};\npub const PROPS_${id(l.name).toUpperCase()}: u64 = ${p.props};\npub const PROP_LEN_${id(l.name).toUpperCase()}: u64 = ${p.propLen};\n`;
  });
  return o;
}

// ---------- Goose

function gsTy(t: Ty): string {
  switch (t.k) {
    case 'nat':
      return 'i64';
    case 'bool':
      return 'bool';
    case 'list':
      return 'i64'; // an index into this list type's arena; -1 is Nil
    case 'adt':
      return t.name;
  }
}

function gsExpr(e: Expr): string {
  switch (e.k) {
    case 'var':
      return e.n;
    case 'nat':
      return String(e.v);
    case 'bool':
      return String(e.v);
    case 'add':
      return `nat_add(${gsExpr(e.a)}, ${gsExpr(e.b)})`;
    case 'cmp':
      return `(${gsExpr(e.a)} ${e.op} ${gsExpr(e.b)})`;
    case 'append':
      return `append_${tyName({ k: 'list', el: e.el })}(${gsExpr(e.a)}, ${gsExpr(e.b)})`;
    case 'nil':
      return '-1';
    case 'cons':
      return `cons_${tyName({ k: 'list', el: e.el })}(${gsExpr(e.h)}, ${gsExpr(e.t)})`;
    case 'ctor': {
      const info = ctorInfo(e.ctor);
      return info.fields.length === 0
        ? `${e.adt}.${e.ctor}`
        : `${e.adt}.${e.ctor} { ${info.fields.map((f, i) => `${f.name}: ${gsExpr(e.args[i] ?? refuse('missing field'))}`).join(', ')} }`;
    }
    case 'call':
      return `${id(e.f)}(${e.args.map(gsExpr).join(', ')})`;
  }
}

function gsStmt(s: Stmt, ind: string): string {
  switch (s.k) {
    case 'ret':
      return `${ind}return ${gsExpr(s.e)};\n`;
    case 'let':
      return `${ind}let ${s.n}: ${gsTy(s.ty)} = ${gsExpr(s.e)};\n${gsStmt(s.body, ind)}`;
    case 'unreachable':
      return `${ind}abort("unreachable");\n`;
    case 'match': {
      const t = s.ty;
      const arm = (c: string): Arm | undefined => s.arms.find((a) => a.ctor === c);
      const body = (a: Arm | undefined, ind2: string): string =>
        a !== undefined
          ? gsStmt(a.body, ind2)
          : s.dflt !== null
            ? gsStmt(s.dflt, ind2)
            : `${ind2}abort("unreachable");\n`;
      if (t.k === 'nat') {
        const pb = arm('Succ')?.binds[0];
        return `${ind}if ${s.scrut} == 0 {\n${body(arm('Zero'), `${ind}  `)}${ind}} else {\n${pb !== undefined ? `${ind}  let ${pb.n}: i64 = ${s.scrut} - 1;\n` : ''}${body(arm('Succ'), `${ind}  `)}${ind}}\n`;
      }
      if (t.k === 'bool')
        return `${ind}if ${s.scrut} {\n${body(arm('True'), `${ind}  `)}${ind}} else {\n${body(arm('False'), `${ind}  `)}${ind}}\n`;
      if (t.k === 'list') {
        const c = arm('Con');
        const [hb, tb] = c?.binds ?? [];
        const ar = `ARENA_${tyName(t)}`;
        return `${ind}if ${s.scrut} < 0 {\n${body(arm('Nil'), `${ind}  `)}${ind}} else {\n${hb !== undefined ? `${ind}  let ${hb.n}: ${gsTy(hb.ty)} = ${ar}[${s.scrut}].head;\n` : ''}${tb !== undefined ? `${ind}  let ${tb.n}: i64 = ${ar}[${s.scrut}].tail;\n` : ''}${body(c, `${ind}  `)}${ind}}\n`;
      }
      let out = `${ind}match ${s.scrut} {\n`;
      for (const c of adtCtors(t.name)) {
        const a = arm(c.name);
        out += `${ind}  ${c.name}${c.fields.length ? ' v__' : ''} => {\n`;
        a?.binds.forEach((b, i) => {
          out += `${ind}    let ${b.n}: ${gsTy(b.ty)} = v__.${c.fields[i]?.name ?? refuse('field')};\n`;
        });
        out += `${body(a, `${ind}    `)}${ind}  },\n`;
      }
      return `${out}${ind}}\n`;
    }
  }
}

function emitGoose(p: Program): string {
  let o = '// GENERATED by bend-emit from a checked Bend book. Do not edit.\n\n';
  o += 'fn nat_add(a: i64, b: i64) -> i64 { let s = a + b; assert(s < (1 << 48)); return s; }\n\n';
  for (const a of p.adts) {
    o += `enum ${a} { ${adtCtors(a)
      .map((c) =>
        c.fields.length
          ? `${c.name} { ${c.fields.map((f) => `${f.name}: ${gsTy(fieldTy(f, { k: 'adt', name: a }))}`).join(', ')} }`
          : c.name,
      )
      .join(', ')} }\n`;
  }
  for (const l of p.lists) {
    if (l.k !== 'list') continue;
    const n = tyName(l);
    o += `struct Cell_${n} { head: ${gsTy(l.el)}, tail: i64 }\nvar ARENA_${n}: Cell_${n}[>..] = [];\n`;
    o += `fn cons_${n}(h: ${gsTy(l.el)}, t: i64) -> i64 { ARENA_${n}.push(Cell_${n} { head: h, tail: t }); return ARENA_${n}.len - 1; }\n`;
    o += `fn append_${n}(a: i64, b: i64) -> i64 {\n  var xs: ${gsTy(l.el)}[>..] = [];\n  var c = a;\n  while c >= 0 { xs.push(ARENA_${n}[c].head); c = ARENA_${n}[c].tail; }\n  var r = b;\n  var i = xs.len - 1;\n  while i >= 0 { r = cons_${n}(xs[i], r); i = i - 1; }\n  return r;\n}\n`;
  }
  o +=
    '\nfn show_Nat(x: i64, out: u8[>..]&) { format(out, x); }\nfn show_Bool(x: bool, out: u8[>..]&) { format(out, x); }\n';
  for (const a of p.adts) {
    o += `fn show_${a}(x: ${a}, out: u8[>..]&) {\n  match x {\n${adtCtors(a)
      .map((c) =>
        c.fields.length
          ? `    ${c.name} v => { format(out, "${c.name}("); ${c.fields.map((f, i) => `${i ? `format(out, ","); ` : ''}show_${tyName(fieldTy(f, { k: 'adt', name: a }))}(v.${f.name}, out);`).join(' ')} format(out, ")"); },\n`
          : `    ${c.name} => { format(out, "${c.name}"); },\n`,
      )
      .join('')}  }\n}\n`;
  }
  for (const l of p.lists) {
    if (l.k !== 'list') continue;
    const n = tyName(l);
    o += `fn show_${n}(x: i64, out: u8[>..]&) {\n  format(out, "[");\n  var c = x;\n  var first = true;\n  while c >= 0 { if !first { format(out, ","); } first = false; show_${tyName(l.el)}(ARENA_${n}[c].head, out); c = ARENA_${n}[c].tail; }\n  format(out, "]");\n}\n`;
  }
  for (const f of p.fns) {
    o += `\n${f.recursive ? 'recursive ' : ''}fn ${id(f.name)}(${f.params.map((x) => `${x.name}: ${gsTy(x.ty)}`).join(', ')}) -> ${gsTy(f.ret)} {\n${gsStmt(f.body, '  ')}}\n`;
  }
  for (const l of p.laws) {
    o += `\nfn law_${id(l.name)}(${l.params.map((x) => `${x.name}: ${gsTy(x.ty)}`).join(', ')}, out: u8[>..]&) -> bool {\n`;
    l.hyps.forEach((h, i) => {
      o += `  var ha${i}: u8[>..] = [];\n  var hb${i}: u8[>..] = [];\n  show_${tyName(h.ty)}(${gsExpr(h.a)}, ha${i});\n  show_${tyName(h.ty)}(${gsExpr(h.b)}, hb${i});\n  if !eq_bytes(ha${i}, hb${i}) { format(out, "vacuous"); return true; }\n`;
    });
    o += `  var sb: u8[>..] = [];\n  show_${tyName(l.ty)}(${gsExpr(l.a)}, out);\n  show_${tyName(l.ty)}(${gsExpr(l.b)}, sb);\n  return eq_bytes(out, sb);\n}\n`;
  }
  o +=
    '\nfn eq_bytes(a: u8[:], b: u8[:]) -> bool {\n  if a.len != b.len { return false; }\n  for i in a.len { if a[i] != b[i] { return false; } }\n  return true;\n}\n';
  o += '\nfn main() {\n  var failed = 0;\n';
  for (const l of p.laws) {
    const cs = p.cases.get(l.name) ?? [];
    o += '  {\n    var pass = 0;\n';
    cs.forEach((row, i) => {
      o += `    { var v: u8[>..] = []; let ok = law_${id(l.name)}(${[...row.map((v) => gsExpr(valExpr(v))), 'v'].join(', ')}); print("law ${l.name} ${i} ", ok, " ", v); if ok { pass++; } }\n`;
    });
    o += `    print("summary ${l.name} ", pass, "/", ${cs.length}, " ${l.proven ? 'proven' : 'open'}");\n    failed = failed + ${cs.length} - pass;\n  }\n`;
  }
  o += '  failed = failed + props();\n  if failed != 0 { exit(1); }\n}\n';
  o += `\nstruct Rng { s: u64 }\nfn rng_next(r: Rng&) -> u64 { r.s = r.s + ${MIX.inc}; var z = r.s; z = (z ^ (z >> 30)) * ${MIX.m1}; z = (z ^ (z >> 27)) * ${MIX.m2}; return z ^ (z >> 31); }\n`;
  for (const t of genTys(p)) {
    const n = tyName(t);
    if (t.k === 'nat') o += 'fn gen_Nat(r: Rng&, m: i64) -> i64 { return (rng_next(r) % 3) as i64; }\n';
    else if (t.k === 'bool') o += 'fn gen_Bool(r: Rng&, m: i64) -> bool { return rng_next(r) % 2 == 0; }\n';
    else if (t.k === 'list')
      o += `fn gen_${n}(r: Rng&, m: i64) -> i64 {\n  let k = (rng_next(r) % ((m + 1) as u64)) as i64;\n  var xs: ${gsTy(t.el)}[>..] = [];\n  for j in k { xs.push(gen_${tyName(t.el)}(r, 3)); }\n  var l: i64 = -1;\n  var j = k - 1;\n  while j >= 0 { l = cons_${n}(xs[j], l); j = j - 1; }\n  return l;\n}\n`;
    else if (t.k === 'adt') {
      const cs = adtCtors(t.name);
      o += `fn gen_${n}(r: Rng&, m: i64) -> ${n} {\n  let c = rng_next(r) % ${cs.length};\n${cs.map((c, i) => `  if c == ${i} { ${c.fields.map((f, j) => `let f${j} = gen_${tyName(fieldTy(f, t))}(r, 3); `).join('')}return ${c.fields.length ? `${n}.${c.name} { ${c.fields.map((f, j) => `${f.name}: f${j}`).join(', ')} }` : `${n}.${c.name}`}; }\n`).join('')}  abort("gen");\n}\n`;
    }
  }
  o += '\nfn props() -> i64 {\n  var failed = 0;\n';
  p.laws.forEach((l, li) => {
    o += `  {\n    var r = Rng { s: ${seedOf(li)} };\n    var pass = 0;\n    for i in ${p.props} {\n${l.params.map((x) => `      let ${x.name} = gen_${tyName(x.ty)}(r, ${p.propLen});\n`).join('')}      var v: u8[>..] = [];\n      let ok = law_${id(l.name)}(${[...l.params.map((x) => x.name), 'v'].join(', ')});\n      print("prop ${l.name} ", i, " ", ok, " ", v);\n      if ok { pass++; }\n    }\n    print("propsummary ${l.name} ", pass, "/${p.props}");\n    failed = failed + ${p.props} - pass;\n  }\n`;
  });
  o += '  return failed;\n}\n';
  return o;
}

// ---------- TypeScript

function tsTy(t: Ty): string {
  switch (t.k) {
    case 'nat':
      return 'number';
    case 'bool':
      return 'boolean';
    case 'list':
      return `List<${tsTy(t.el)}>`;
    case 'adt':
      return t.name;
  }
}

function tsExpr(e: Expr): string {
  switch (e.k) {
    case 'var':
      return e.n;
    case 'nat':
      return String(e.v);
    case 'bool':
      return String(e.v);
    case 'add':
      return `nat_add(${tsExpr(e.a)}, ${tsExpr(e.b)})`;
    case 'cmp':
      return `(${tsExpr(e.a)} ${e.op === '==' ? '===' : e.op} ${tsExpr(e.b)})`;
    case 'append':
      return `list_append(${tsExpr(e.a)}, ${tsExpr(e.b)})`;
    case 'nil':
      return 'NIL';
    case 'cons':
      return `{ $: "Con", head: ${tsExpr(e.h)}, tail: ${tsExpr(e.t)} }`;
    case 'ctor': {
      const info = ctorInfo(e.ctor);
      return `{ $: "${e.ctor}"${info.fields.map((f, i) => `, ${f.name}: ${tsExpr(e.args[i] ?? refuse('missing field'))}`).join('')} } as const`;
    }
    case 'call':
      return `${id(e.f)}(${e.args.map(tsExpr).join(', ')})`;
  }
}

function tsStmt(s: Stmt, ind: string): string {
  switch (s.k) {
    case 'ret':
      return `${ind}return ${tsExpr(s.e)};\n`;
    case 'let':
      return `${ind}const ${s.n} = ${tsExpr(s.e)};\n${tsStmt(s.body, ind)}`;
    case 'unreachable':
      return `${ind}throw new Error("unreachable");\n`;
    case 'match': {
      const t = s.ty;
      const arm = (c: string): Arm | undefined => s.arms.find((a) => a.ctor === c);
      const body = (a: Arm | undefined, ind2: string): string =>
        a !== undefined
          ? tsStmt(a.body, ind2)
          : s.dflt !== null
            ? tsStmt(s.dflt, ind2)
            : `${ind2}throw new Error("unreachable");\n`;
      if (t.k === 'nat') {
        const pb = arm('Succ')?.binds[0];
        const succ = body(arm('Succ'), `${ind}  `);
        const bound =
          pb !== undefined && new RegExp(`\\b${pb.n}\\b`).test(succ) ? `${ind}  const ${pb.n} = ${s.scrut} - 1;\n` : '';
        return `${ind}if (${s.scrut} === 0) {\n${body(arm('Zero'), `${ind}  `)}${ind}} else {\n${bound}${succ}${ind}}\n`;
      }
      if (t.k === 'bool')
        return `${ind}if (${s.scrut}) {\n${body(arm('True'), `${ind}  `)}${ind}} else {\n${body(arm('False'), `${ind}  `)}${ind}}\n`;
      const ctors =
        t.k === 'list'
          ? [
              { name: 'Nil', fields: [] },
              { name: 'Con', fields: [{ name: 'head' }, { name: 'tail' }] },
            ]
          : adtCtors(t.name);
      let out = `${ind}switch (${s.scrut}.$) {\n`;
      for (const c of ctors) {
        const a = arm(c.name);
        out += `${ind}  case "${c.name}": {\n`;
        const arms = body(a, `${ind}    `);
        // a field the arm never reads is not bound: the emitted module compiles under noUnusedLocals
        a?.binds.forEach((b, i) => {
          if (new RegExp(`\\b${b.n}\\b`).test(arms))
            out += `${ind}    const ${b.n} = ${s.scrut}.${c.fields[i]?.name ?? refuse('field')};\n`;
        });
        out += `${arms}${ind}  }\n`;
      }
      return `${out}${ind}}\n`;
    }
  }
}

// The seeded generators, the same splitmix64 stream in every target.
function tsGens(p: Program, ex: string): string {
  let o = `\nconst M64 = (1n << 64n) - 1n;\n${ex}class Rng { s: bigint; constructor(seed: bigint) { this.s = seed; } next(): bigint { this.s = (this.s + ${MIX.inc}n) & M64; let z = this.s; z = ((z ^ (z >> 30n)) * ${MIX.m1}n) & M64; z = ((z ^ (z >> 27n)) * ${MIX.m2}n) & M64; return z ^ (z >> 31n); } }\n`;
  for (const t of genTys(p)) {
    const n = tyName(t);
    if (t.k === 'nat') o += `${ex}function gen_Nat(r: Rng, _m: number): number { return Number(r.next() % 3n); }\n`;
    else if (t.k === 'bool')
      o += `${ex}function gen_Bool(r: Rng, _m: number): boolean { return r.next() % 2n === 0n; }\n`;
    else if (t.k === 'list')
      o += `${ex}function gen_${n}(r: Rng, m: number): ${tsTy(t)} { const k = Number(r.next() % BigInt(m + 1)); const xs: ${tsTy(t.el)}[] = []; for (let j = 0; j < k; j++) xs.push(gen_${tyName(t.el)}(r, 3)); let l: ${tsTy(t)} = NIL; for (let j = xs.length - 1; j >= 0; j--) { const h = xs[j]; if (h === undefined) throw new Error("index"); l = { $: "Con", head: h, tail: l }; } return l; }\n`;
    else if (t.k === 'adt') {
      const cs = adtCtors(t.name);
      o += `${ex}function gen_${n}(r: Rng, _m: number): ${n} {\n  const c = r.next() % ${cs.length}n;\n${cs.map((c, i) => `  if (c === ${i}n) { ${c.fields.map((f, j) => `const f${j} = gen_${tyName(fieldTy(f, t))}(r, 3); `).join('')}return { $: "${c.name}"${c.fields.map((f, j) => `, ${f.name}: f${j}`).join('')} }; }\n`).join('')}  throw new Error("gen");\n}\n`;
    }
  }
  return o;
}

// Module mode: each law's enumerated corpus, its seed and its draw size, beside the generators, for a
// binding driver that runs the model and the code on the same inputs.
function emitTsCorpus(p: Program): string {
  let o = '';
  p.laws.forEach((l, li) => {
    const tuple = `[${l.params.map((x) => tsTy(x.ty)).join(', ')}]`;
    const rows = p.cases.get(l.name) ?? [];
    o += `\n/** ${l.name} (${l.proven ? 'proven' : 'open'}): ${rows.length} enumerated cases. */\nexport function corpus_${id(l.name)}(): ${tuple}[] {\n  return [\n${rows.map((row) => `    [${row.map((v) => tsExpr(valExpr(v))).join(', ')}],\n`).join('')}  ];\n}\n`;
    o += `/** ${l.name}'s seed and draw size: the same stream every target draws. */\nexport const SEED_${id(l.name).toUpperCase()} = ${seedOf(li)}n;\nexport const PROPS_${id(l.name).toUpperCase()} = ${p.props};\nexport const PROP_LEN_${id(l.name).toUpperCase()} = ${p.propLen};\n`;
  });
  return o;
}

function emitTs(p: Program, module: string | null = null): string {
  const ex = module !== null ? 'export ' : '';
  let o = `// GENERATED by bend-emit from a checked Bend book. Do not edit.\n${module ?? ''}`;
  o += `export type List<T> = { readonly $: "Nil" } | { readonly $: "Con"; readonly head: T; readonly tail: List<T> };\n${ex}const NIL: { readonly $: "Nil" } = { $: "Nil" };\n`;
  o += `${ex}function nat_add(a: number, b: number): number { const s = a + b; if (!(s < 2 ** 48)) throw new Error("Nat past 2^48"); return s; }\n`;
  o += `${ex}function list_append<T>(a: List<T>, b: List<T>): List<T> { const xs: T[] = []; for (let c = a; c.$ === "Con"; c = c.tail) xs.push(c.head); let r = b; for (let i = xs.length - 1; i >= 0; i--) { const h = xs[i]; if (h === undefined) throw new Error("index"); r = { $: "Con", head: h, tail: r }; } return r; }\n`;
  o += `${ex}function show_Nat(x: number): string { return String(x); }\n${ex}function show_Bool(x: boolean): string { return String(x); }\n`;
  for (const a of p.adts) {
    o += `export type ${a} = ${adtCtors(a)
      .map(
        (c) =>
          `{ readonly $: "${c.name}"${c.fields.map((f) => `; readonly ${f.name}: ${tsTy(fieldTy(f, { k: 'adt', name: a }))}`).join('')} }`,
      )
      .join(' | ')};\n`;
    o += `${ex}function show_${a}(x: ${a}): string {\n  switch (x.$) {\n${adtCtors(a)
      .map(
        (c) =>
          `    case "${c.name}": return ${c.fields.length ? `"${c.name}(" + ${c.fields.map((f) => `show_${tyName(fieldTy(f, { k: 'adt', name: a }))}(x.${f.name})`).join(` + "," + `)} + ")"` : `"${c.name}"`};\n`,
      )
      .join('')}  }\n}\n`;
  }
  for (const l of p.lists) {
    if (l.k !== 'list') continue;
    o += `${ex}function show_${tyName(l)}(x: ${tsTy(l)}): string { const xs: string[] = []; for (let c = x; c.$ === "Con"; c = c.tail) xs.push(show_${tyName(l.el)}(c.head)); return "[" + xs.join(",") + "]"; }\n`;
  }
  for (const f of p.fns) {
    o += `\nexport function ${id(f.name)}(${f.params.map((x) => `${x.name}: ${tsTy(x.ty)}`).join(', ')}): ${tsTy(f.ret)} {\n${tsStmt(f.body, '  ')}}\n`;
  }
  for (const l of p.laws) {
    o += `\nexport function law_${id(l.name)}(${l.params.map((x) => `${x.name}: ${tsTy(x.ty)}`).join(', ')}): [boolean, string] {\n`;
    for (const h of l.hyps)
      o += `  if (show_${tyName(h.ty)}(${tsExpr(h.a)}) !== show_${tyName(h.ty)}(${tsExpr(h.b)})) return [true, "vacuous"];\n`;
    o += `  const sa = show_${tyName(l.ty)}(${tsExpr(l.a)});\n  return [sa === show_${tyName(l.ty)}(${tsExpr(l.b)}), sa];\n}\n`;
  }
  if (module !== null) return o + tsGens(p, ex) + emitTsCorpus(p);
  let m = '\nif (import.meta.main) {\n  let failed = 0;\n';
  for (const l of p.laws) {
    const cs = p.cases.get(l.name) ?? [];
    m += '  {\n    let pass = 0;\n';
    cs.forEach((row, i) => {
      m += `    { const [ok, v] = law_${id(l.name)}(${row.map((v) => tsExpr(valExpr(v))).join(', ')}); console.log(\`law ${l.name} ${i} \${ok} \${v}\${ok ? "" : " | args ${row.map(showVal).join(' ; ')}"}\`); if (ok) pass++; }\n`;
    });
    m += `    console.log(\`summary ${l.name} \${pass}/${cs.length}\`);\n    failed += ${cs.length} - pass;\n  }\n`;
  }
  m += '  failed += props();\n  process.exit(failed === 0 ? 0 : 1);\n}\n';
  o += tsGens(p, '');
  o += '\nfunction props(): number {\n  let failed = 0;\n';
  p.laws.forEach((l, li) => {
    o += `  {\n    const r = new Rng(${seedOf(li)}n);\n    let pass = 0;\n    for (let i = 0; i < ${p.props}; i++) {\n${l.params.map((x) => `      const ${x.name} = gen_${tyName(x.ty)}(r, ${p.propLen});\n`).join('')}      const [ok, v] = law_${id(l.name)}(${l.params.map((x) => x.name).join(', ')});\n      console.log(\`prop ${l.name} \${i} \${ok} \${v}\${ok ? "" : " | args " + [${l.params.map((x) => `show_${tyName(x.ty)}(${x.name})`).join(', ')}].join(" ; ")}\`);\n      if (ok) pass++;\n    }\n    console.log(\`propsummary ${l.name} \${pass}/${p.props}\`);\n    failed += ${p.props} - pass;\n  }\n`;
  });
  return `${o}  return failed;\n}\n${m}`;
}

// ---------- Go

// Leave ordinary model names readable; encode qualified names in a disjoint namespace.
function goName(role: string, name: string): string {
  return /^[A-Za-z0-9_]+$/.test(name)
    ? `${role}_${name}`
    : `${role}Q_${name.replace(/[^a-zA-Z0-9]/gu, (c) => `_${c.codePointAt(0)?.toString(16)}_`)}`;
}

function goTy(t: Ty): string {
  switch (t.k) {
    case 'nat':
      return 'uint64';
    case 'bool':
      return 'bool';
    case 'list':
      return `*List[${goTy(t.el)}]`;
    case 'adt':
      return goName('Type', t.name);
  }
}

function goHelper(op: string, t: Ty): string {
  switch (t.k) {
    case 'nat':
      return `${op}Nat`;
    case 'bool':
      return `${op}Bool`;
    case 'list':
      return goHelper(`${op}List`, t.el);
    case 'adt':
      return goName(`${op}Type`, t.name);
  }
}

function goExpr(e: Expr): string {
  switch (e.k) {
    case 'var':
      return e.n;
    case 'nat':
      return `uint64(${e.v})`;
    case 'bool':
      return String(e.v);
    case 'add':
      return `natAdd(${goExpr(e.a)}, ${goExpr(e.b)})`;
    case 'cmp':
      return `(${goExpr(e.a)} ${e.op} ${goExpr(e.b)})`;
    case 'nil':
      return `(*List[${goTy(e.el)}])(nil)`;
    case 'cons':
      return `&List[${goTy(e.el)}]{${goExpr(e.h)}, ${goExpr(e.t)}}`;
    case 'append':
      return `listAppend(${goExpr(e.a)}, ${goExpr(e.b)})`;
    case 'ctor':
      return `${goName('Ctor', e.ctor)}{${e.args.map(goExpr).join(', ')}}`;
    case 'call':
      return `${goName('Fn', e.f)}(${e.args.map(goExpr).join(', ')})`;
  }
}

// Only live pattern variables are legal Go locals. IR binders are already unique within a function.
function goReads(s: Stmt, used: Set<string>): void {
  const expr = (e: Expr): void => {
    if (e.k === 'var') used.add(e.n);
    else for (const child of children(e)) expr(child);
  };
  switch (s.k) {
    case 'ret':
      expr(s.e);
      break;
    case 'unreachable':
      break;
    case 'let':
      expr(s.e);
      goReads(s.body, used);
      break;
    case 'match':
      used.add(s.scrut);
      for (const a of s.arms) goReads(a.body, used);
      if (s.dflt !== null) goReads(s.dflt, used);
  }
}

function goStmt(s: Stmt, used: ReadonlySet<string>, ind: string): string {
  switch (s.k) {
    case 'ret':
      return `${ind}return ${goExpr(s.e)}\n`;
    case 'unreachable':
      return `${ind}panic("unreachable Bend pattern")\n`;
    case 'let':
      return `${ind}${s.n} := ${goExpr(s.e)}\n${goStmt(s.body, used, ind)}`;
    case 'match': {
      const x = s.scrut;
      const bindVariant = s.arms.some((a) => a.binds.some((b) => used.has(b.n)));
      const on =
        s.ty.k === 'nat'
          ? `${x} == 0`
          : s.ty.k === 'list'
            ? `${x} == nil`
            : s.ty.k === 'bool'
              ? x
              : `${bindVariant ? 'variant := ' : ''}${x}.(type)`;
      let out = `${ind}switch ${on} {\n`;
      for (const a of s.arms) {
        const pattern = s.ty.k === 'adt' ? goName('Ctor', a.ctor) : String(['Zero', 'True', 'Nil'].includes(a.ctor));
        out += `${ind}case ${pattern}:\n`;
        a.binds.forEach((b, i) => {
          if (!used.has(b.n)) return;
          const field =
            s.ty.k === 'nat' ? `${x} - 1` : s.ty.k === 'list' ? `${x}.${i === 0 ? 'Head' : 'Tail'}` : `variant.F${i}`;
          out += `${ind}\t${b.n} := ${field}\n`;
        });
        out += goStmt(a.body, used, `${ind}\t`);
      }
      return `${out}${ind}default:\n${goStmt(s.dflt ?? { k: 'unreachable' }, used, `${ind}\t`)}${ind}}\n`;
    }
  }
}

function goData(p: Program): string {
  let out = `type List[T any] struct { Head T; Tail *List[T] }

func listAppend[T any](a, b *List[T]) *List[T] {
  var head *List[T]
  end := &head
  for ; a != nil; a = a.Tail { *end = &List[T]{Head: a.Head}; end = &(*end).Tail }
  *end = b
  return head
}

func natAdd(a, b uint64) uint64 {
  if a >= 1 << 48 || b >= (1 << 48) - a { panic("Nat past 2^48") }
  return a + b
}
`;
  for (const adt of p.adts) {
    const marker = goName('is', adt);
    out += `\ntype ${goName('Type', adt)} interface { ${marker}() }\n`;
    for (const c of adtCtors(adt)) {
      const name = goName('Ctor', c.name);
      out += `type ${name} struct { ${c.fields.map((f, i) => `F${i} ${goTy(fieldTy(f, { k: 'adt', name: adt }))}`).join('; ')} }\nfunc (${name}) ${marker}() {}\n`;
    }
  }
  for (const f of p.fns) {
    const used = new Set<string>();
    goReads(f.body, used);
    out += `\nfunc ${goName('Fn', f.name)}(${f.params.map((x) => `${x.name} ${goTy(x.ty)}`).join(', ')}) ${goTy(f.ret)} {\n${goStmt(f.body, used, '\t')}}\n`;
  }
  return out;
}
function goValues(p: Program): string {
  let out = `
func writeNat(s *strings.Builder, x uint64) { s.WriteString(strconv.FormatUint(x, 10)) }
func writeBool(s *strings.Builder, x bool) { s.WriteString(strconv.FormatBool(x)) }
func display[T any](x T, write func(*strings.Builder, T)) string {
  var s strings.Builder
  write(&s, x)
  return s.String()
}

type rng uint64
func (r *rng) next() uint64 {
  *r += ${MIX.inc}
  z := uint64(*r)
  z = (z ^ (z >> 30)) * ${MIX.m1}
  z = (z ^ (z >> 27)) * ${MIX.m2}
  return z ^ (z >> 31)
}
func genNat(r *rng, _ uint64) uint64 { return r.next() % 3 }
func genBool(r *rng, _ uint64) bool { return r.next() % 2 == 0 }
`;
  for (const t of genTys(p)) {
    if (t.k !== 'list' && t.k !== 'adt') continue;
    const ty = goTy(t);
    const writer = `func ${goHelper('write', t)}(s *strings.Builder, x ${ty})`;
    const gen = `func ${goHelper('gen', t)}(r *rng, m uint64) ${ty}`;
    if (t.k === 'list') {
      out += `\n${writer} {\n  s.WriteByte('[')\n  for ; x != nil; x = x.Tail { ${goHelper('write', t.el)}(s, x.Head); if x.Tail != nil { s.WriteByte(',') } }\n  s.WriteByte(']')\n}\n`;
      out += `${gen} {\n  var head ${ty}\n  end := &head\n  for n := r.next() % (m + 1); n != 0; n-- { *end = &List[${goTy(t.el)}]{Head: ${goHelper('gen', t.el)}(r, 3)}; end = &(*end).Tail }\n  return head\n}\n`;
    } else {
      const cs = adtCtors(t.name);
      const binding = cs.some((c) => c.fields.length > 0) ? 'v := ' : '';
      out += `\n${writer} {\n  switch ${binding}x.(type) {\n`;
      for (const c of cs) {
        out += `  case ${goName('Ctor', c.name)}:\n    s.WriteString(${JSON.stringify(c.name + (c.fields.length ? '(' : ''))})\n`;
        c.fields.forEach((f, i) => {
          if (i > 0) out += `    s.WriteByte(',')\n`;
          out += `    ${goHelper('write', fieldTy(f, t))}(s, v.F${i})\n`;
        });
        if (c.fields.length) out += `    s.WriteByte(')')\n`;
      }
      out += `  default: panic("invalid Bend value")\n  }\n}\n${gen} {\n  switch r.next() % ${cs.length} {\n`;
      cs.forEach((c, i) => {
        out += `  ${i === cs.length - 1 ? 'default' : `case ${i}`}: return ${goName('Ctor', c.name)}{${c.fields.map((f) => `${goHelper('gen', fieldTy(f, t))}(r, 3)`).join(', ')}}\n`;
      });
      out += '  }\n}\n';
    }
  }
  return out;
}

function goShow(t: Ty, x: string): string {
  switch (t.k) {
    case 'nat':
      return `strconv.FormatUint(${x}, 10)`;
    case 'bool':
      return `strconv.FormatBool(${x})`;
    default:
      return `display[${goTy(t)}](${x}, ${goHelper('write', t)})`;
  }
}

function goLaw(p: Program, law: Law, index: number): string {
  const params = law.params.map((x) => `${x.name} ${goTy(x.ty)}`);
  const args = law.params.map((x) => x.name);
  const name = goName('Law', law.name);
  let out = `\nfunc ${name}(${params.join(', ')}) (bool, string) {\n`;
  for (const h of law.hyps) {
    const atom = h.ty.k === 'nat' || h.ty.k === 'bool';
    const a = goExpr(h.a);
    const b = goExpr(h.b);
    out += `  if ${atom ? a : goShow(h.ty, a)} != ${atom ? b : goShow(h.ty, b)} { return true, "vacuous" }\n`;
  }
  const atom = law.ty.k === 'nat' || law.ty.k === 'bool';
  if (atom) out += `  a := ${goExpr(law.a)}\n  return a == ${goExpr(law.b)}, ${goShow(law.ty, 'a')}\n}\n`;
  else out += `  lhs := ${goShow(law.ty, goExpr(law.a))}\n  return lhs == ${goShow(law.ty, goExpr(law.b))}, lhs\n}\n`;
  const visit = ['kind string', 'index int', 'ok bool', 'lhs string', ...params].join(', ');
  out += `\nfunc ${goName('Drive', law.name)}(visit func(${visit})) {\n`;
  out += `  run := func(${['kind string', 'index int', ...params].join(', ')}) {\n    ok, lhs := ${name}(${args.join(', ')})\n    visit(${['kind', 'index', 'ok', 'lhs', ...args].join(', ')})\n  }\n`;
  (p.cases.get(law.name) ?? []).forEach((row, i) => {
    out += `  run(${['"law"', i, ...row.map((v) => goExpr(valExpr(v)))].join(', ')})\n`;
  });
  if (args.length > 0) out += `  r := rng(${seedOf(index)})\n`;
  const draws = law.params.map((x) => `${goHelper('gen', x.ty)}(&r, ${p.propLen})`);
  return `${out}  for i := 0; i < ${p.props}; i++ { run(${['"prop"', 'i', ...draws].join(', ')}) }\n}\n`;
}

function emitGo(p: Program, module: string | null = null): string {
  let out = `// Code generated by bend-emit from a checked Bend book. DO NOT EDIT.\n${module ?? ''}package ${module === null ? 'main' : 'bendmodel'}\n`;
  if (p.laws.length > 0) {
    out += `\nimport (\n  "strconv"\n  "strings"\n${module === null ? '  "fmt"\n  "os"\n' : ''})\n`;
  }
  out += `\n${goData(p)}`;
  if (p.laws.length > 0) out += goValues(p);
  p.laws.forEach((l, i) => {
    out += goLaw(p, l, i);
  });
  if (module !== null) return out;
  if (p.laws.length === 0) return `${out}\nfunc main() {}\n`;
  out += '\nfunc main() {\n  failed := false\n';
  for (const l of p.laws) {
    const params = l.params.map((x) => `${x.name} ${goTy(x.ty)}`);
    const show = l.params.map((x) => goShow(x.ty, x.name)).join(', ');
    out += `  ${goName('Drive', l.name)}(func(${['kind string', 'index int', 'ok bool', 'lhs string', ...params].join(', ')}) {\n    fmt.Printf("%s %s %d %t %s", kind, ${JSON.stringify(l.name)}, index, ok, lhs)\n    if !ok { failed = true; fmt.Print(" | args ", strings.Join([]string{${show}}, " ; ")) }\n    fmt.Println()\n  })\n`;
  }
  return `${out}  if failed { os.Exit(1) }\n}\n`;
}
// ---- package entry ------------------------------------------------------------

type Target =
  | { readonly kind: 'all'; readonly stem: string }
  | {
      readonly kind: 'rust-module' | 'ts-module' | 'go-module';
      readonly modelName: string;
      readonly flags: readonly string[];
    };

export interface EmitOptions {
  readonly target: Target;
  readonly functions?: readonly string[];
  readonly laws?: readonly string[];
  readonly cap?: number;
  readonly listLength?: number;
  readonly properties?: number;
  readonly propertyLength?: number;
  readonly noCorpus?: readonly string[];
}

export type Emission = {
  readonly functions: number;
  readonly laws: number;
} & (
  | { readonly kind: 'module'; readonly source: string }
  | {
      readonly kind: 'all';
      readonly rust: string;
      readonly goose: string;
      readonly go: string;
      readonly typescript: string;
      readonly runner: string;
      readonly cases: number;
      readonly properties: number;
    }
);

export interface CheckedBendBook {
  /** Emit from the checked book without writing files or reading process arguments. */
  emit(options: EmitOptions): Emission;
}

function emitBook({
  target,
  functions = [],
  laws: names = [],
  cap = 400,
  properties = 200,
  propertyLength = 12,
  noCorpus = [],
}: EmitOptions): Emission {
  const laws = names.map(compileLaw);
  // close the set of defs the laws and the requested functions reach
  const todo = [...functions, ...USED];
  const fns = new Map<string, Fn>();
  while (todo.length) {
    const k = todo.pop();
    if (k === undefined || fns.has(k) || INTRINSIC.has(k)) continue;
    fns.set(k, compileFn(k));
    for (const u of USED) if (!fns.has(u)) todo.push(u);
  }
  // callee before caller, so Goose and readers see definitions in order
  const order: Fn[] = [];
  const seen = new Set<string>();
  const visit = (k: string): void => {
    if (seen.has(k)) return;
    seen.add(k);
    const f = fns.get(k);
    if (f === undefined) return;
    for (const c of calls(f.body)) if (c !== k) visit(c);
    order.push(f);
  };
  for (const k of fns.keys()) visit(k);
  // A law whose arguments are structured data the driver builds itself lists no enumerated rows.
  const cases = new Map(laws.map((l) => [l.name, noCorpus.includes(l.name) ? [] : corpus(l.params, cap)]));
  const base = { fns: lower(order), laws, cases };
  const prog: Program = { ...base, ...collectTypes(base), props: properties, propLen: propertyLength };
  const counts = { functions: order.length, laws: laws.length };
  if (target.kind === 'all') {
    return {
      kind: 'all',
      ...counts,
      rust: emitRust(prog),
      goose: emitGoose(prog),
      go: emitGo(prog),
      typescript: emitTs(prog),
      runner: emitRunner(target.stem, prog),
      cases: [...cases.values()].reduce((n, c) => n + c.length, 0),
      properties,
    };
  }
  const emittedBy = `Emitted by: bend-emit ${target.flags.join(' ')}\n`;
  const model = `Model: ${target.modelName}, checked by bend2 before emission.\n`;
  if (target.kind === 'go-module') {
    return { kind: 'module', ...counts, source: emitGo(prog, `// ${model}// ${emittedBy}`) };
  }
  const source =
    target.kind === 'rust-module'
      ? emitRust(prog, `//! ${model}//! ${emittedBy}`)
      : emitTs(
          prog,
          `// biome-ignore-all lint: generated by bend-emit from a book bend2 checked; the repository lints its drivers, not the emitter's code\n// biome-ignore-all format: generated by bend-emit; the emitter's layout is the file\n// ${model}// ${emittedBy}`,
        );
  return { kind: 'module', ...counts, source };
}

// The generated runner: builds each target (its own step; a check never builds),
// runs the drivers, and reports by law: pass counts per target, and the
// first case where the targets disagree or a law fails, with its counterexample.
function emitRunner(stem: string, p: Program): string {
  const laws = p.laws.map((l) => ({ name: l.name, cases: p.cases.get(l.name)?.length ?? 0, proven: l.proven }));
  return `// GENERATED by bend-emit. Compare the checked laws on Rust, Goose, Go and TypeScript.
import { spawnSync } from "node:child_process";
const here = import.meta.dir;
const sh = (cmd: string, args: string[]) => {
  const t0 = performance.now();
  const r = spawnSync(cmd, args, { cwd: here, encoding: "utf8", maxBuffer: 1 << 28 });
  return { code: r.status ?? -1, out: r.stdout ?? "", err: r.error?.message ?? r.stderr ?? "", ms: performance.now() - t0 };
};
const build = process.argv.includes("--build");
if (build) {
  const rs = sh("rustc", ["-C", "opt-level=1", "--edition", "2021", "${stem}.rs", "-o", "${stem}_rs"]);
  if (rs.code !== 0) { console.error(rs.err); process.exit(2); }
  const gs = sh(process.env.GOOSE ?? "goose", ["build", "${stem}.goose"]);
  if (gs.code !== 0) { console.error(gs.err + gs.out); process.exit(2); }
  const go = sh("go", ["build", "-o", "${stem}_go", "${stem}.go"]);
  if (go.code !== 0) { console.error(go.err); process.exit(2); }
}
const runs = {
  rust: sh("./${stem}_rs", []),
  goose: sh("./${stem}", []),
  go: sh("./${stem}_go", []),
  ts: sh("bun", ["${stem}.ts"]),
};
type Row = { law: string; i: string; ok: string; v: string; args: string; kind: string };
const parse = (out: string): Row[] => out.split("\\n").filter((l) => l.startsWith("law ") || l.startsWith("prop ")).map((l) => {
  const [kind = "", law = "", i = "", ok = "", ...rest] = l.split(" ");
  const [v = "", args = ""] = rest.join(" ").split(" | args ");
  return { kind, law, i, ok, v, args };
});
const targets = ["rust", "goose", "go", "ts"] as const;
const rows = { rust: parse(runs.rust.out), goose: parse(runs.goose.out), go: parse(runs.go.out), ts: parse(runs.ts.out) };
const laws: { name: string; cases: number; proven: boolean }[] = ${JSON.stringify(laws)};
let bad = 0;
for (const t of targets) {
  const code = rows[t].some((r) => r.ok === "false") ? 1 : 0;
  if (runs[t].code !== code || rows[t].length !== ${laws.reduce((n, l) => n + l.cases + p.props, 0)}) {
    console.error(t + ": unexpected exit or row count", runs[t].code, rows[t].length, runs[t].err);
    process.exit(2);
  }
}
for (const { name: law, cases, proven } of laws) {
  const per = targets.map((t) => rows[t].filter((r) => r.law === law));
  const ts = rows.ts.filter((r) => r.law === law);
  let disagree = -1;
  for (let k = 0; k < Math.max(cases + ${p.props}, ...per.map((xs) => xs.length)); k++) {
    const ref = ts[k];
    const kind = k < cases ? "law" : "prop";
    const i = String(k < cases ? k : k - cases);
    if (!ref || (ref.ok !== "true" && ref.ok !== "false") || per.some((xs, t) => {
      const r = xs[k];
      return !r || r.kind !== kind || r.i !== i || r.ok !== ref.ok || r.v !== ref.v
        || (targets[t] === "go" && r.args !== ref.args);
    })) { disagree = k; break; }
  }
  const fail = ts.find((r) => r.ok !== "true");
  const counts = per.map((xs, i) => targets[i] + " " + xs.filter((r) => r.ok === "true").length + "/" + xs.length);
  const verdict = disagree < 0 && !fail ? "PASS" : "FAIL";
  if (verdict === "FAIL") bad++;
  console.log(verdict + " " + law + " [" + (proven ? "proven" : "open") + "]: " + counts.join("  ") + "  targets " + (disagree < 0 ? "agree" : "DISAGREE at row " + disagree));
  if (fail !== undefined) console.log(\`  counterexample (\${fail.kind} \${fail.i}): args \${fail.args}  lhs = \${fail.v}\`);
  if (disagree >= 0) for (const [i, xs] of per.entries()) console.log("  " + targets[i] + ": " + JSON.stringify(xs[disagree] ?? "missing"));
}
console.log("runtime ms: " + targets.map((t) => t + " " + runs[t].ms.toFixed(0)).join("  "));
process.exit(bad === 0 ? 0 : 1);
`;
}

function calls(s: Stmt): string[] {
  const ex = (e: Expr): string[] => [...(e.k === 'call' ? [e.f] : []), ...children(e).flatMap(ex)];
  switch (s.k) {
    case 'ret':
      return ex(s.e);
    case 'let':
      return [...ex(s.e), ...calls(s.body)];
    case 'unreachable':
      return [];
    case 'match':
      return [...s.arms.flatMap((a) => calls(a.body)), ...(s.dflt ? calls(s.dflt) : [])];
  }
}

/** Load a model and its imports, and accept it only after the bundled checker validates the book. */
export async function checkBendFile(file: string): Promise<CheckedBendBook> {
  const book = Bend.book_nil();
  await Bend.book_load(book, file, '', new Map());
  Bend.book_valid(book, 0);
  return {
    emit(options) {
      // The original emitter's synchronous scratch belongs to this call, never to asynchronous file loading.
      BOOK = book;
      LIST_LEN = options.listLength ?? 3;
      try {
        return emitBook(options);
      } finally {
        BOOK = EMPTY_BOOK;
        fresh = 0;
        SIGS.clear();
        USED.clear();
      }
    },
  };
}
