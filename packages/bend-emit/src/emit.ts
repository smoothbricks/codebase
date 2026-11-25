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
// Bool, List<T> and monomorphized first-order user/Base ADTs (including
// Maybe<T>, Result<E, A>, Either<A, B>, Pair/Sigma); no closures, no templates,
// no lets. Laws accept equality ({a == b : T}) and IsTrue(p) predicates in both
// hypotheses and claims.
//
// Lowerings keep the emitted code's cost the model's, not its unary encoding's
// (see `lower`): structural/Base Nat relations and arithmetic/bitwise ops are
// lowered after verification against the Bend recursive model on boundary and
// seeded inputs; repeated calls in a return expression are let-bound once; and
// self-tail-recursive defs lower to iterative loops in Rust, TS and Go.

import * as Bend from '../vendor/bend2/bend.js';
import { blake3Hex } from './blake3.js';
import {
  defIsProven,
  generateSkeletons,
  type SkeletonDef,
  type SkeletonLeafCounts,
  type SkeletonOptions,
  type SkeletonResult,
} from './skeleton.js';

export type { SkeletonDef, SkeletonLeafCounts, SkeletonOptions, SkeletonResult };

type H = Bend.HTerm;

export const BEND_REVISION = '38103b3d46425026eff2d16bef93684967bee992';

// ---- types -----------------------------------------------------------------

type AdtTy = { k: 'adt'; name: string; args: Ty[] };
type Ty = { k: 'nat' } | { k: 'bool' } | { k: 'list'; el: Ty } | AdtTy;

const NAT: Ty = { k: 'nat' };
const BOOL: Ty = { k: 'bool' };

function id(k: string): string {
  return k.replace(/[.:]/g, '_');
}

function tyName(t: Ty): string {
  switch (t.k) {
    case 'nat':
      return 'Nat';
    case 'bool':
      return 'Bool';
    case 'list':
      return `List${tyName(t.el)}`;
    case 'adt':
      return t.args.length === 0 ? id(t.name) : `${id(t.name)}_${t.args.map(tyName).join('_')}`;
  }
}

class Refused extends Error {}

function refuse(what: string): never {
  throw new Refused(`bend-emit refuses: ${what}`);
}

function tyOf(t0: H): Ty {
  let t = Bend.term_force(t0);
  if (t.$ === 'App' || (t.$ === 'Ref' && BOOK.tlds[t.k]?.$ === 'Def')) {
    t = Bend.term_wnf(BOOK, t);
  }
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
  if (k === 'Sigma') {
    const aTerm = args[2];
    const bTerm = args[3];
    if (aTerm === undefined || bTerm === undefined) refuse('Sigma without element types');
    const bLam = Bend.term_force(bTerm);
    if (bLam.$ !== 'Lam') refuse('dependent Sigma type');
    const aTy = tyOf(aTerm);
    const bTy = tyOf(bLam.f(mkVar('sigma_x')));
    return { k: 'adt', name: 'Sigma', args: [aTy, bTy] };
  }
  const adtDef = BOOK.tlds[k];
  if (adtDef === undefined || adtDef.$ !== 'ADT') refuse(`${k} is not a datatype`);
  if (adtDef.n === 0) return { k: 'adt', name: k, args: [] };
  if (args.length < adtDef.n) refuse(`${k} missing type arguments`);
  const tyArgs = args.slice(adtDef.g, adtDef.n).map(tyOf);
  return { k: 'adt', name: k, args: tyArgs };
}

function tyToTerm(t: Ty): H {
  switch (t.k) {
    case 'nat':
      return Bend.ADT('Nat', []);
    case 'bool':
      return Bend.ADT('Bool', []);
    case 'list':
      return Bend.ADT('List', [Bend.Qua(Bend.Many()), tyToTerm(t.el)]);
    case 'adt': {
      if (t.name === 'Sigma') {
        const a = t.args[0];
        const b = t.args[1];
        if (a === undefined || b === undefined) refuse('Sigma missing type args');
        return Bend.ADT('Sigma', [
          Bend.Qua(Bend.Many()),
          Bend.Qua(Bend.Many()),
          tyToTerm(a),
          Bend.Lam('_', 0, (_x: H): H => tyToTerm(b)),
        ]);
      }
      const adtDef = BOOK.tlds[t.name];
      if (adtDef === undefined || adtDef.$ !== 'ADT') refuse(`${t.name} is not a datatype`);
      const quants: H[] = Array.from({ length: adtDef.g }, () => Bend.Qua(Bend.Many()));
      return Bend.ADT(t.name, [...quants, ...t.args.map(tyToTerm)]);
    }
  }
}

// ---- signatures --------------------------------------------------------------

type Param = { name: string; rawName: string; ty: Ty; v: H };
type Hyp = { a: H; b: H; ty: Ty };
type Sig = { params: Param[]; erased: number[]; hyps: Hyp[]; ret: Ty | null; claim: Hyp | null };

let fresh = 0;
function mkVar(k: string): H {
  const name = `${k.replace(/[^A-Za-z0-9_]/g, '_')}_${String(fresh++)}`;
  return Bend.Var(name, 0);
}

function isUnitOrEmpty(t0: H, want: 'Unit' | 'Empty'): boolean {
  const t = Bend.term_force(t0);
  return (t.$ === 'ADT' || t.$ === 'Ref') && t.k === want;
}

// Recognizes `IsTrue(p)` predicates where `IsTrue` maps `True{} -> Unit` and
// `False{} -> Empty` (or is an abstract law named `IsTrue`).
function asIsTrue(t0: H): H | null {
  const t = Bend.term_force(t0);
  if (t.$ !== 'App') return null;
  const [head, args] = Bend.term_unapply(t);
  const h = Bend.term_force(head);
  if (h.$ !== 'Ref' || args.length !== 1 || args[0] === undefined) return null;
  const def = BOOK.tlds[h.k];
  if (def === undefined || def.$ !== 'Def') return null;
  const bare = h.k.includes(':') ? h.k.slice(h.k.indexOf(':') + 1) : h.k;
  if (def.v !== null) {
    const onTrue = Bend.term_wnf(BOOK, Bend.App(Bend.Ref(h.k), Bend.Ctr('True', [])));
    const onFalse = Bend.term_wnf(BOOK, Bend.App(Bend.Ref(h.k), Bend.Ctr('False', [])));
    if (isUnitOrEmpty(onTrue, 'Unit') && isUnitOrEmpty(onFalse, 'Empty')) return args[0];
    return null;
  }
  if (bare === 'IsTrue') return args[0];
  return null;
}

function sigOf(T0: H, erasedArgs?: ReadonlyMap<number, H>): Sig {
  const sig: Sig = { params: [], erased: [], hyps: [], ret: null, claim: null };
  let t = Bend.term_force(T0);
  let pos = 0;
  while (t.$ === 'All') {
    const v = mkVar(t.k);
    const A = Bend.term_force(t.A);
    let stepArg: H = v;
    if (t.q.$ === 'None') {
      sig.erased.push(pos);
      const sub = erasedArgs?.get(pos);
      if (sub !== undefined) stepArg = sub;
    } else if (A.$ === 'Eql') {
      sig.hyps.push({ a: A.a, b: A.b, ty: tyOf(A.T) });
    } else {
      const pred = asIsTrue(A);
      if (pred !== null) {
        sig.hyps.push({ a: pred, b: Bend.Ctr('True', []), ty: BOOL });
      } else {
        sig.params.push({ name: varName(v), rawName: t.k, ty: tyOf(A), v });
      }
    }
    pos += 1;
    t = Bend.term_force(t.B(stepArg));
  }
  if (t.$ === 'Eql') {
    sig.claim = { a: t.a, b: t.b, ty: tyOf(t.T) };
  } else {
    const pred = asIsTrue(t);
    if (pred !== null) sig.claim = { a: pred, b: Bend.Ctr('True', []), ty: BOOL };
    else sig.ret = tyOf(t);
  }
  return sig;
}

function varName(v: H): string {
  const t = Bend.term_force(v);
  if (t.$ !== 'Var') refuse('binder that is not a variable');
  return t.k;
}

// ---- de-Bruijn canonicalization & BLAKE3 law digest -------------------------

function canonicalTerm(t0: H, depth: number): unknown {
  const t = Bend.term_force(t0);
  switch (t.$) {
    case 'Var':
      return ['Var', t.i];
    case 'Ref':
      return ['Ref', t.k];
    case 'Typ':
      return ['Typ', canonicalTerm(t.g, depth)];
    case 'Qnt':
      return ['Qnt'];
    case 'Qua':
      return ['Qua', t.q.$];
    case 'Min':
      return ['Min', canonicalTerm(t.a, depth), canonicalTerm(t.b, depth)];
    case 'All': {
      const x: H = Bend.Var('_', depth);
      return ['All', t.q.$, canonicalTerm(t.A, depth), canonicalTerm(t.B(x), depth + 1)];
    }
    case 'Lam': {
      const x: H = Bend.Var('_', depth);
      return ['Lam', t.q?.$ ?? '', canonicalTerm(t.f(x), depth + 1)];
    }
    case 'Let': {
      const xs = t.k.map((_, j): H => Bend.Var('_', depth + j));
      return [
        'Let',
        t.q.map((q) => q.$),
        t.v.map((v) => canonicalTerm(v, depth)),
        canonicalTerm(t.f(xs), depth + t.k.length),
      ];
    }
    case 'App':
      return ['App', canonicalTerm(t.f, depth), canonicalTerm(t.x, depth)];
    case 'ADT':
      return ['ADT', t.k, t.x.map((x) => canonicalTerm(x, depth)), t.r];
    case 'Ctr':
      return ['Ctr', t.k, t.x.map((x) => canonicalTerm(x, depth))];
    case 'Lit':
      return ['Lit', t.k, t.v];
    case 'Mat':
      return ['Mat', t.k, canonicalTerm(t.h, depth), canonicalTerm(t.m, depth)];
    case 'Efq':
      return ['Efq'];
    case 'Eql':
      return ['Eql', canonicalTerm(t.a, depth), canonicalTerm(t.b, depth), canonicalTerm(t.T, depth)];
    case 'Rfl':
      return ['Rfl'];
    case 'Rwt':
      return ['Rwt', canonicalTerm(t.e, depth), canonicalTerm(t.p, depth), canonicalTerm(t.f, depth)];
    case 'Hol':
      return ['Hol', t.k];
    case 'Ann':
      return ['Ann', canonicalTerm(t.x, depth), canonicalTerm(t.T, depth)];
    case 'Sub':
      return ['Sub', t.i, canonicalTerm(t.f, depth)];
  }
}

function lawDigest(k: string): string {
  const t = BOOK.tlds[k];
  if (t === undefined || t.$ !== 'Def') return refuse(`${k} is not a def`);
  return blake3Hex(JSON.stringify(canonicalTerm(t.T, 0)));
}

// ---- the book ---------------------------------------------------------------

type CtorField = { name: string; ty: Ty };
type CtorInfo = { adt: AdtTy; name: string; fields: CtorField[] };

const EMPTY_BOOK = Bend.book_nil();
let BOOK = EMPTY_BOOK;

function ctorInfoFor(k: string, adtTy: AdtTy): CtorInfo {
  const c = BOOK.ctrs[k];
  if (c === undefined) refuse(`unknown constructor ${k}`);
  const adtDef = BOOK.tlds[adtTy.name];
  if (adtDef === undefined || adtDef.$ !== 'ADT') refuse(`${adtTy.name} is not a datatype`);
  const fields: CtorField[] = [];
  let t = Bend.term_force(c.T);
  for (let i = 0; i < adtDef.n; i++) {
    if (t.$ !== 'All') refuse(`constructor ${k} arity mismatch`);
    let arg: H;
    if (i < adtDef.g) {
      arg = Bend.Qua(Bend.Many());
    } else if (adtTy.name === 'Sigma' && i === 3) {
      const b = adtTy.args[1];
      if (b === undefined) refuse('Sigma missing snd type');
      arg = Bend.Lam('_', 0, (_x: H): H => tyToTerm(b));
    } else {
      const a = adtTy.args[i - adtDef.g];
      if (a === undefined) refuse(`${adtTy.name} missing type arg ${i - adtDef.g}`);
      arg = tyToTerm(a);
    }
    t = Bend.term_force(t.B(arg));
  }
  while (t.$ === 'All') {
    const v = mkVar(t.k);
    if (t.q.$ !== 'None') {
      fields.push({ name: t.k, ty: tyOf(t.A) });
    }
    t = Bend.term_force(t.B(v));
  }
  return { adt: adtTy, name: k, fields };
}

function adtCtors(adtTy: AdtTy): CtorInfo[] {
  const t = BOOK.tlds[adtTy.name];
  if (t === undefined || t.$ !== 'ADT') return refuse(`${adtTy.name} is not a datatype`);
  return t.c.map((c) => ctorInfoFor(c.k, adtTy));
}

function ctorFieldsForScrut(k: string, scrut: Ty): CtorField[] {
  if (scrut.k === 'nat') {
    if (k === 'Zero') return [];
    if (k === 'Succ') return [{ name: 'pred', ty: NAT }];
    return refuse(`unknown Nat constructor ${k}`);
  }
  if (scrut.k === 'bool') {
    if (k === 'True' || k === 'False') return [];
    return refuse(`unknown Bool constructor ${k}`);
  }
  if (scrut.k === 'list') {
    if (k === 'Nil') return [];
    if (k === 'Con')
      return [
        { name: 'head', ty: scrut.el },
        { name: 'tail', ty: scrut },
      ];
    return refuse(`unknown List constructor ${k}`);
  }
  return ctorInfoFor(k, scrut).fields;
}

// ---- core IR ------------------------------------------------------------------

type Rel = '==' | '!=' | '<=' | '<' | '>=' | '>';
type NatBinOp = 'add' | 'sub' | 'mul' | 'div' | 'mod' | 'bit_and' | 'bit_or' | 'bit_xor' | 'shl' | 'shr';
type NatUnOp = 'popcount' | 'ctz_z0' | 'ctz_z64';

type Expr =
  | { k: 'var'; n: string; ty: Ty }
  | { k: 'nat'; v: number }
  | { k: 'bool'; v: boolean }
  | { k: 'natbin'; op: NatBinOp; a: Expr; b: Expr }
  | { k: 'natun'; op: NatUnOp; a: Expr }
  | { k: 'cmp'; op: Rel; a: Expr; b: Expr }
  | { k: 'append'; el: Ty; a: Expr; b: Expr }
  | { k: 'nil'; el: Ty }
  | { k: 'cons'; el: Ty; h: Expr; t: Expr }
  | { k: 'ctor'; ty: AdtTy; ctor: string; args: Expr[] }
  | { k: 'call'; f: string; args: Expr[] };

type Arm = { ctor: string; binds: { n: string; ty: Ty }[]; body: Stmt };
type Stmt =
  | { k: 'ret'; e: Expr }
  | { k: 'let'; n: string; ty: Ty; e: Expr; body: Stmt }
  | { k: 'unreachable' }
  | { k: 'match'; scrut: string; ty: Ty; arms: Arm[]; dflt: Stmt | null };

type Fn = {
  name: string;
  params: Param[];
  ret: Ty;
  body: Stmt;
  recursive: boolean;
  tailRecursive: boolean;
};

type Law = {
  name: string;
  params: Param[];
  hyps: { a: Expr; b: Expr; ty: Ty }[];
  a: Expr;
  b: Expr;
  ty: Ty;
  proven: boolean;
  digest: string;
};

type SpecEntry = { origKey: string; erasedArgs: Map<number, H>; sig: Sig };

const SIGS = new Map<string, Sig>();
const SPECIALIZED = new Map<string, SpecEntry>();
const USED_BASE_NAT = new Set<string>();

function sig(k: string): Sig {
  const spec = SPECIALIZED.get(k);
  if (spec !== undefined) return spec.sig;
  const s = SIGS.get(k);
  if (s !== undefined) return s;
  const t = BOOK.tlds[k];
  if (t === undefined || t.$ !== 'Def') return refuse(`${k} is not a def`);
  const r = sigOf(t.T);
  SIGS.set(k, r);
  return r;
}

// Checks whether a Def has erased Quant/Type binders and specializes it at the call site.
function resolveCall(k: string, all: H[]): { fnKey: string; s: Sig } {
  const t = BOOK.tlds[k];
  if (t === undefined || t.$ !== 'Def') return refuse(`${k} is not a def`);
  let hasPolyErased = false;
  let cur = Bend.term_force(t.T);
  while (cur.$ === 'All') {
    const A = Bend.term_force(cur.A);
    if (cur.q.$ === 'None' && (A.$ === 'Qnt' || A.$ === 'Typ')) {
      hasPolyErased = true;
      break;
    }
    cur = Bend.term_force(cur.B(mkVar(cur.k)));
  }
  if (!hasPolyErased) return { fnKey: k, s: sig(k) };

  const erasedArgs = new Map<number, H>();
  const suffix: string[] = [];
  let scan = Bend.term_force(t.T);
  let pos = 0;
  while (scan.$ === 'All') {
    const A = Bend.term_force(scan.A);
    if (scan.q.$ === 'None') {
      const arg = all[pos];
      if (arg === undefined) refuse(`a partial application of ${k}`);
      erasedArgs.set(pos, arg);
      if (A.$ === 'Typ') suffix.push(tyName(tyOf(arg)));
      scan = Bend.term_force(scan.B(arg));
    } else {
      scan = Bend.term_force(scan.B(mkVar(scan.k)));
    }
    pos += 1;
  }
  const fnKey = suffix.length > 0 ? `${k}__${suffix.join('_')}` : k;
  const existing = SPECIALIZED.get(fnKey);
  if (existing !== undefined) return { fnKey, s: existing.sig };
  const s = sigOf(t.T, erasedArgs);
  SPECIALIZED.set(fnKey, { origKey: k, erasedArgs, sig: s });
  return { fnKey, s };
}

const BASE_NAT_BIN: ReadonlyMap<string, NatBinOp> = new Map([
  ['Nat.add', 'add'],
  ['Nat.sub', 'sub'],
  ['Nat.mul', 'mul'],
  ['Nat.div', 'div'],
  ['Nat.mod', 'mod'],
]);

const BASE_NAT_CMP: ReadonlyMap<string, Rel> = new Map([
  ['Nat.is_eq', '=='],
  ['Nat.is_ne', '!='],
  ['Nat.is_le', '<='],
  ['Nat.is_lt', '<'],
  ['Nat.is_ge', '>='],
  ['Nat.is_gt', '>'],
]);

const INTRINSIC = new Set(['List.append', ...BASE_NAT_BIN.keys(), ...BASE_NAT_CMP.keys()]);

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
      const fields = ctorFieldsForScrut(c.k, sty);
      const binds = fields.map((f) => {
        const v = mkVar(f.name);
        return { n: varName(v), ty: f.ty, v };
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
      const natOp = BASE_NAT_BIN.get(h.k);
      if (natOp !== undefined) {
        const [x, y] = all;
        if (x === undefined || y === undefined) refuse(`${h.k} arity`);
        USED_BASE_NAT.add(h.k);
        return { k: 'natbin', op: natOp, a: expr(x, env, NAT), b: expr(y, env, NAT) };
      }
      const natCmp = BASE_NAT_CMP.get(h.k);
      if (natCmp !== undefined) {
        const [x, y] = all;
        if (x === undefined || y === undefined) refuse(`${h.k} arity`);
        USED_BASE_NAT.add(h.k);
        return { k: 'cmp', op: natCmp, a: expr(x, env, NAT), b: expr(y, env, NAT) };
      }
      if (h.k === 'List.append') {
        const [x, y] = all.slice(all.length - 2);
        if (x === undefined || y === undefined) refuse('List.append arity');
        if (want.k !== 'list') refuse('List.append outside a list context');
        return { k: 'append', el: want.el, a: expr(x, env, want), b: expr(y, env, want) };
      }
      const { fnKey, s } = resolveCall(h.k, all);
      const live = all.filter((_, i) => !s.erased.includes(i));
      if (live.length !== s.params.length) refuse(`a partial application of ${h.k}`);
      const args = live.map((a, i) => {
        const p = s.params[i];
        if (p === undefined) return refuse(`argument ${i} of ${h.k}`);
        return expr(a, env, p.ty);
      });
      USED.add(fnKey);
      return { k: 'call', f: fnKey, args };
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
    return { k: 'natbin', op: 'add', a: expr(p, env, NAT), b: { k: 'nat', v: 1 } };
  }
  if (k === 'True' || k === 'False') return { k: 'bool', v: k === 'True' };
  if (k === 'Nil' || k === 'Con') {
    if (want.k !== 'list') refuse(`${k} where ${tyName(want)} is expected`);
    if (k === 'Nil') return { k: 'nil', el: want.el };
    const [h, tl] = xs.slice(xs.length - 2);
    if (h === undefined || tl === undefined) refuse('Con without fields');
    return { k: 'cons', el: want.el, h: expr(h, env, want.el), t: expr(tl, env, want) };
  }
  const c = BOOK.ctrs[k];
  if (c === undefined) refuse(`unknown constructor ${k}`);
  if (want.k !== 'adt' || want.name !== c.f) refuse(`${k} where ${tyName(want)} is expected`);
  const info = ctorInfoFor(k, want);
  const live = xs.slice(xs.length - info.fields.length);
  return {
    k: 'ctor',
    ty: want,
    ctor: k,
    args: live.map((x, i) => {
      const f = info.fields[i];
      if (f === undefined) return refuse(`field ${i} of ${k}`);
      return expr(x, env, f.ty);
    }),
  };
}

const USED = new Set<string>();

function compileFn(k: string): Fn {
  const spec = SPECIALIZED.get(k);
  const origKey = spec?.origKey ?? k;
  const t = BOOK.tlds[origKey];
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
    if (s.erased.includes(i)) {
      spine.push(spec?.erasedArgs.get(i) ?? mkVar('erased'));
    } else {
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
  return { name: k, params: s.params, ret: s.ret, body, recursive, tailRecursive: false };
}

// A law is proven when its def has a body the checker accepted and contains no
// hole (`?TODO`) directly or in any user proof def it reaches; otherwise it is
// an open claim: the driver still runs it, as a strong test, not a proof.
function lawProven(k: string): boolean {
  return defIsProven(BOOK, k);
}

function compileLaw(k: string): Law {
  const s = sig(k);
  if (s.claim === null) refuse(`${k} is not an equality or IsTrue law`);
  const env = new Map<string, Ty>();
  for (const p of s.params) env.set(p.name, p.ty);
  return {
    name: k,
    params: s.params,
    proven: lawProven(k),
    digest: lawDigest(k),
    hyps: s.hyps.map((h) => ({ a: expr(h.a, env, h.ty), b: expr(h.b, env, h.ty), ty: h.ty })),
    a: expr(s.claim.a, env, s.claim.ty),
    b: expr(s.claim.b, env, s.claim.ty),
    ty: s.claim.ty,
  };
}

// ---- lowering: the model's meaning at the target's cost ------------------------

// A Nat or Bool value of the IR's evaluator.
type V = number | boolean;

function evalNatBin(op: NatBinOp, a: number, b: number): number {
  switch (op) {
    case 'add':
      return a + b;
    case 'sub':
      return a > b ? a - b : 0;
    case 'mul':
      return a * b;
    case 'div':
      return b === 0 ? 0 : Math.floor(a / b);
    case 'mod':
      return b === 0 ? a : a % b;
    case 'bit_and':
      return Number(BigInt(a) & BigInt(b));
    case 'bit_or':
      return Number(BigInt(a) | BigInt(b));
    case 'bit_xor':
      return Number(BigInt(a) ^ BigInt(b));
    case 'shl':
      return a === 0 ? 0 : Number(BigInt(a) << BigInt(b));
    case 'shr':
      return b >= 48 ? 0 : Number(BigInt(a) >> BigInt(b));
  }
}

function evalNatUn(op: NatUnOp, a: number): number {
  if (op === 'popcount') {
    let x = BigInt(a);
    let c = 0;
    while (x !== 0n) {
      x &= x - 1n;
      c += 1;
    }
    return c;
  }
  if (a === 0) return op === 'ctz_z0' ? 0 : 64;
  let x = BigInt(a);
  let c = 0;
  while ((x & 1n) === 0n) {
    x >>= 1n;
    c += 1;
  }
  return c;
}

function holds(op: Rel, a: number, b: number): boolean {
  switch (op) {
    case '==':
      return a === b;
    case '!=':
      return a !== b;
    case '<=':
      return a <= b;
    case '<':
      return a < b;
    case '>=':
      return a >= b;
    case '>':
      return a > b;
  }
}

// Evaluates a def over Nat and Bool, the reference a lowering is checked against.
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
    case 'natbin':
      return evalNatBin(e.op, nat(e.a), nat(e.b));
    case 'natun':
      return evalNatUn(e.op, nat(e.a));
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

// Verify used Base Nat.* intrinsics directly against their Bend recursive definitions.
function bendToNat(t0: H, limit = 100000): number {
  let acc = 0;
  let cur = Bend.term_wnf(BOOK, t0);
  while (acc < limit) {
    if (cur.$ === 'Lit' && cur.k === 'Nat') return acc + cur.v;
    if (cur.$ === 'Ctr' && cur.k === 'Zero') return acc;
    if (cur.$ === 'Ctr' && cur.k === 'Succ' && cur.x[0] !== undefined) {
      acc += 1;
      cur = Bend.term_wnf(BOOK, cur.x[0]);
      continue;
    }
    throw new Error(`expected Nat value, got ${cur.$}`);
  }
  throw new Error('Nat evaluation limit exceeded');
}

function bendToBool(t0: H): boolean {
  const cur = Bend.term_wnf(BOOK, t0);
  if (cur.$ === 'Ctr' && cur.k === 'True') return true;
  if (cur.$ === 'Ctr' && cur.k === 'False') return false;
  throw new Error(`expected Bool value, got ${cur.$}`);
}

function verifyUsedBaseNatIntrinsics(): void {
  const boundary: readonly [number, number][] = [
    [0, 0],
    [0, 1],
    [1, 0],
    [1, 1],
    [0, 5],
    [5, 0],
    [7, 3],
    [6, 3],
    [3, 7],
    [12, 4],
    [13, 5],
  ];
  const sample: [number, number][] = [...boundary, ...pairs(202601n, 24, 14n)];
  for (const k of USED_BASE_NAT) {
    if (BOOK.tlds[k] === undefined) continue;
    const binOp = BASE_NAT_BIN.get(k);
    if (binOp !== undefined) {
      for (const [u, v] of sample) {
        const got = bendToNat(Bend.App(Bend.App(Bend.Ref(k), Bend.Lit('Nat', u)), Bend.Lit('Nat', v)));
        const want = evalNatBin(binOp, u, v);
        if (got !== want) refuse(`Base intrinsic ${k}(${u}, ${v}) evaluated to ${got}, expected ${want}`);
      }
      continue;
    }
    const rel = BASE_NAT_CMP.get(k);
    if (rel !== undefined) {
      for (const [u, v] of sample) {
        const got = bendToBool(Bend.App(Bend.App(Bend.Ref(k), Bend.Lit('Nat', u)), Bend.Lit('Nat', v)));
        const want = holds(rel, u, v);
        if (got !== want) refuse(`Base intrinsic ${k}(${u}, ${v}) evaluated to ${got}, expected ${want}`);
      }
    }
  }
}

// The relation `f` is exactly, as a structural recursion on two Nats, if any:
//   `==`: f(0, 0) = True;  f(0, 1+q) = False; f(1+p, 0) = False; f(1+p, 1+q) = f(p, q).
//   `!=`: f(0, 0) = False; f(0, 1+q) = True;  f(1+p, 0) = True;  f(1+p, 1+q) = f(p, q).
//   `<=`: f(0, b) = True;                     f(1+p, 0) = False; f(1+p, 1+q) = f(p, q).
//   `<`:  f(0, 0) = False; f(0, 1+q) = True;  f(1+p, 0) = False; f(1+p, 1+q) = f(p, q).
//   `>=`: f(0, 0) = True;  f(0, 1+q) = False; f(1+p, 0) = True;  f(1+p, 1+q) = f(p, q).
//   `>`:  f(0, b) = False;                    f(1+p, 0) = True;  f(1+p, 1+q) = f(p, q).
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
    rec.e.args[1].n === q.n;
  if (!descends) return undefined;
  const succZero = arm(succ?.body, b.name, 'Zero')?.body;
  if (is(succZero, false)) {
    if (is(arm(zero, b.name, 'Zero')?.body, true) && is(arm(zero, b.name, 'Succ')?.body, false)) return '==';
    if (is(arm(zero, b.name, 'Zero')?.body, false) && is(arm(zero, b.name, 'Succ')?.body, true)) return '<';
    if (is(zero, true)) return '<=';
  }
  if (is(succZero, true)) {
    if (is(arm(zero, b.name, 'Zero')?.body, false) && is(arm(zero, b.name, 'Succ')?.body, true)) return '!=';
    if (is(arm(zero, b.name, 'Zero')?.body, true) && is(arm(zero, b.name, 'Succ')?.body, false)) return '>=';
    if (is(zero, false)) return '>';
  }
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

// `f` lowered to a native comparison when it is that structural relation and
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

// Structural Nat sub or mul pattern detector.
function structuralNatBin(f: Fn): 'sub' | 'mul' | undefined {
  const [a, b] = f.params;
  if (f.params.length !== 2 || a === undefined || b === undefined) return undefined;
  if (a.ty.k !== 'nat' || b.ty.k !== 'nat' || f.ret.k !== 'nat') return undefined;
  if (f.body.k !== 'match' || f.body.scrut !== a.name || f.body.dflt !== null) return undefined;
  const zero = f.body.arms.find((x) => x.ctor === 'Zero')?.body;
  const succ = f.body.arms.find((x) => x.ctor === 'Succ');
  const p = succ?.binds[0];
  if (zero?.k !== 'ret' || zero.e.k !== 'nat' || zero.e.v !== 0 || p === undefined || succ === undefined) {
    return undefined;
  }
  // Structural sub: match b { Zero => 1+p (or a), Succ(q) => f(p, q) }
  if (succ.body.k === 'match' && succ.body.scrut === b.name && succ.body.dflt === null) {
    const sb = succ.body.arms.find((x) => x.ctor === 'Succ');
    const q = sb?.binds[0];
    if (
      q !== undefined &&
      sb?.body.k === 'ret' &&
      sb.body.e.k === 'call' &&
      sb.body.e.f === f.name &&
      sb.body.e.args.length === 2 &&
      sb.body.e.args[0]?.k === 'var' &&
      sb.body.e.args[0].n === p.n &&
      sb.body.e.args[1]?.k === 'var' &&
      sb.body.e.args[1].n === q.n
    ) {
      return 'sub';
    }
  }
  // Structural mul: Succ(p) => b + f(p, b) or f(p, b) + b
  if (succ.body.k === 'ret' && succ.body.e.k === 'natbin' && succ.body.e.op === 'add') {
    const isB = (e: Expr): boolean => e.k === 'var' && e.n === b.name;
    const isRec = (e: Expr): boolean =>
      e.k === 'call' &&
      e.f === f.name &&
      e.args.length === 2 &&
      e.args[0]?.k === 'var' &&
      e.args[0].n === p.n &&
      e.args[1]?.k === 'var' &&
      e.args[1].n === b.name;
    if ((isB(succ.body.e.a) && isRec(succ.body.e.b)) || (isRec(succ.body.e.a) && isB(succ.body.e.b))) {
      return 'mul';
    }
  }
  return undefined;
}

const NAMED_NAT_BIN: ReadonlyMap<string, NatBinOp> = new Map([
  ['sub', 'sub'],
  ['mul', 'mul'],
  ['div', 'div'],
  ['mod', 'mod'],
  ['bit_and', 'bit_and'],
  ['bit_or', 'bit_or'],
  ['bit_xor', 'bit_xor'],
  ['shl', 'shl'],
  ['shr', 'shr'],
]);

function bareName(k: string): string {
  const afterColon = k.includes(':') ? k.slice(k.indexOf(':') + 1) : k;
  return afterColon.startsWith('Nat.') ? afterColon.slice(4) : afterColon;
}

function verifyBinCandidate(fns: ReadonlyMap<string, Fn>, f: Fn, op: NatBinOp): boolean {
  const testPairs: [number, number][] = [];
  if (op === 'shl') {
    for (let u = 0; u <= 15; u++) for (let v = 0; v <= 5; v++) testPairs.push([u, v]);
    for (const [x, y] of pairs(900001n, 32, 32n)) testPairs.push([x, y % 7]);
  } else if (op === 'mul') {
    for (let u = 0; u <= 10; u++) for (let v = 0; v <= 10; v++) testPairs.push([u, v]);
    for (const [x, y] of pairs(900002n, 48, 25n)) testPairs.push([x, y]);
  } else {
    for (let u = 0; u <= 12; u++) for (let v = 0; v <= 12; v++) testPairs.push([u, v]);
    for (const [x, y] of pairs(900003n, 64, 128n)) testPairs.push([x, y]);
  }
  try {
    for (const [u, v] of testPairs) {
      if (evalFn(fns, f, [u, v]) !== evalNatBin(op, u, v)) return false;
    }
    return true;
  } catch {
    return false;
  }
}

function verifyUnCandidate(fns: ReadonlyMap<string, Fn>, f: Fn, op: NatUnOp): boolean {
  const values: number[] = [];
  for (let u = 0; u <= 64; u++) values.push(u);
  for (const [x] of pairs(900004n, 64, 256n)) values.push(x);
  try {
    for (const u of values) {
      if (evalFn(fns, f, [u]) !== evalNatUn(op, u)) return false;
    }
    return true;
  } catch {
    return false;
  }
}

// Lowers verified structural sub/mul and verified named arithmetic/bitwise defs.
function lowerNatOp(fns: ReadonlyMap<string, Fn>, f: Fn): Fn {
  if (f.ret.k !== 'nat') return f;
  if (f.params.length === 2 && f.params[0]?.ty.k === 'nat' && f.params[1]?.ty.k === 'nat') {
    const a = f.params[0];
    const b = f.params[1];
    const structOp = structuralNatBin(f);
    if (structOp !== undefined) {
      if (!verifyBinCandidate(fns, f, structOp)) {
        refuse(`${f.name} reads as structural Nat ${structOp} but failed equivalence verification`);
      }
      return {
        ...f,
        recursive: false,
        body: {
          k: 'ret',
          e: { k: 'natbin', op: structOp, a: { k: 'var', n: a.name, ty: NAT }, b: { k: 'var', n: b.name, ty: NAT } },
        },
      };
    }
    const namedOp = NAMED_NAT_BIN.get(bareName(f.name));
    if (namedOp !== undefined && verifyBinCandidate(fns, f, namedOp)) {
      return {
        ...f,
        recursive: false,
        body: {
          k: 'ret',
          e: { k: 'natbin', op: namedOp, a: { k: 'var', n: a.name, ty: NAT }, b: { k: 'var', n: b.name, ty: NAT } },
        },
      };
    }
  }
  if (f.params.length === 1 && f.params[0]?.ty.k === 'nat') {
    const a = f.params[0];
    const bn = bareName(f.name);
    if (bn === 'popcount' && verifyUnCandidate(fns, f, 'popcount')) {
      return {
        ...f,
        recursive: false,
        body: { k: 'ret', e: { k: 'natun', op: 'popcount', a: { k: 'var', n: a.name, ty: NAT } } },
      };
    }
    if (bn === 'ctz') {
      for (const variant of ['ctz_z0', 'ctz_z64'] as const) {
        if (verifyUnCandidate(fns, f, variant)) {
          return {
            ...f,
            recursive: false,
            body: { k: 'ret', e: { k: 'natun', op: variant, a: { k: 'var', n: a.name, ty: NAT } } },
          };
        }
      }
    }
  }
  return f;
}

function children(e: Expr): Expr[] {
  switch (e.k) {
    case 'natbin':
    case 'cmp':
    case 'append':
      return [e.a, e.b];
    case 'natun':
      return [e.a];
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
    case 'natbin':
    case 'cmp':
    case 'append':
      return { ...e, a: f(e.a), b: f(e.b) };
    case 'natun':
      return { ...e, a: f(e.a) };
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

function exprCalls(e: Expr, target: string): boolean {
  if (e.k === 'call' && e.f === target) return true;
  return children(e).some((c) => exprCalls(c, target));
}

// True when every self-call in `f` is in strict tail position with no nested self-call in its arguments.
function isSelfTailRecursive(f: Fn): boolean {
  if (!f.recursive || f.params.length === 0) return false;
  let sawTail = false;
  const check = (s: Stmt): boolean => {
    switch (s.k) {
      case 'ret':
        if (s.e.k === 'call' && s.e.f === f.name) {
          if (s.e.args.some((a) => exprCalls(a, f.name))) return false;
          sawTail = true;
          return true;
        }
        return !exprCalls(s.e, f.name);
      case 'let':
        return !exprCalls(s.e, f.name) && check(s.body);
      case 'unreachable':
        return true;
      case 'match':
        return s.arms.every((a) => check(a.body)) && (s.dflt === null || check(s.dflt));
    }
  };
  return check(f.body) && sawTail;
}

// All lowerings over a closed set of defs, in its order.
function lower(order: readonly Fn[]): Fn[] {
  const fns = new Map(order.map((f) => [f.name, f]));
  const rel = order.map((f) => lowerNatRelation(fns, f));
  const afterRel = new Map(rel.map((f) => [f.name, f]));
  const ops = rel.map((f) => lowerNatOp(afterRel, f));
  const lowered = new Map(ops.map((f) => [f.name, f]));
  return ops.map((f) => {
    const shared = shareCalls(lowered, f);
    return { ...shared, tailRecursive: isSelfTailRecursive(shared) };
  });
}

// ---- corpus: values enumerated from the law parameter types --------------------

type Val = { ctor: string; ty: AdtTy; args: Val[] } | { nat: number } | { bool: boolean } | { list: Val[]; el: Ty };

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
      const ctors = adtCtors(t);
      const isRec = RECURSIVE_ADTS.has(tyName(t));
      const chosen = isRec && depth >= ADT_DEPTH ? baseCtorsOf(t) : ctors;
      const perCtor = chosen.map((c) => {
        let rows: Val[][] = [[]];
        for (const f of c.fields) {
          const fv = enumerate(f.ty, depth + 1).slice(0, isRec ? 2 : 3);
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
let ADT_DEPTH = 2;
let RECURSIVE_ADTS = new Set<string>();

function computeRecursiveAdts(adts: readonly AdtTy[]): Set<string> {
  // Direct (non-List) ADT field edges; any ADT reachable from itself in >= 1 step is recursive (self or mutual).
  const direct = new Map<string, Set<string>>();
  for (const a of adts) {
    const k = tyName(a);
    const deps = new Set<string>();
    for (const c of adtCtors(a)) {
      for (const f of c.fields) {
        if (f.ty.k === 'adt') deps.add(tyName(f.ty));
      }
    }
    direct.set(k, deps);
  }
  const rec = new Set<string>();
  for (const start of direct.keys()) {
    const visited = new Set<string>();
    const stack = [...(direct.get(start) ?? [])];
    while (stack.length > 0) {
      const cur = stack.pop();
      if (cur === undefined) continue;
      if (cur === start) {
        rec.add(start);
        break;
      }
      if (visited.has(cur)) continue;
      visited.add(cur);
      for (const nxt of direct.get(cur) ?? []) stack.push(nxt);
    }
  }
  return rec;
}

function tyHasRecAdt(t: Ty): boolean {
  switch (t.k) {
    case 'nat':
    case 'bool':
      return false;
    case 'list':
      return tyHasRecAdt(t.el);
    case 'adt':
      return RECURSIVE_ADTS.has(tyName(t)) || t.args.some(tyHasRecAdt);
  }
}

function baseCtorsOf(t: AdtTy): CtorInfo[] {
  const cs = adtCtors(t);
  const base = cs.filter((c) => !c.fields.some((f) => tyHasRecAdt(f.ty)));
  return base.length > 0 ? base : cs;
}

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
  return { k: 'ctor', ty: v.ty, ctor: v.ctor, args: v.args.map(valExpr) };
}

// ---- targets -------------------------------------------------------------------

type CompiledMachine = {
  name: string;
  init: Fn;
  step: Fn;
  handled: Fn | null;
  stateTy: Ty;
  inputTy: Ty;
  inputs: Val[];
};

type Program = {
  fns: Fn[];
  laws: Law[];
  machines: CompiledMachine[];
  adts: AdtTy[];
  lists: Ty[];
  cases: Map<string, Val[][]>;
  props: number;
  propLen: number;
  natOps: Set<NatBinOp | NatUnOp>;
};

// Seeded generators, the same splitmix64 stream in every target, so the
// property run is a second differential: equal seeds give equal inputs.
const MIX = { inc: '0x9E3779B97F4A7C15', m1: '0xBF58476D1CE4E5B9', m2: '0x94D049BB133111EB' };

function genTys(p: Program): Ty[] {
  return [NAT, BOOL, ...p.lists, ...p.adts];
}

function seedOf(lawIndex: number): string {
  return String(1000003 * (lawIndex + 1));
}

function collectTypes(p: Pick<Program, 'fns' | 'laws' | 'cases'>): { adts: AdtTy[]; lists: Ty[] } {
  const adts = new Map<string, AdtTy>();
  const lists = new Map<string, Ty>();
  const see = (t: Ty): void => {
    if (t.k === 'adt') {
      const k = tyName(t);
      if (!adts.has(k)) {
        adts.set(k, t);
        for (const c of adtCtors(t)) for (const f of c.fields) see(f.ty);
      }
    }
    if (t.k === 'list') {
      const k = tyName(t);
      if (!lists.has(k)) {
        lists.set(k, t);
        see(t.el);
      }
    }
  };
  const seeExpr = (e: Expr): void => {
    if (e.k === 'var') see(e.ty);
    else if (e.k === 'ctor') see(e.ty);
    else if (e.k === 'nil' || e.k === 'cons' || e.k === 'append') see({ k: 'list', el: e.el });
    for (const c of children(e)) seeExpr(c);
  };
  const seeStmt = (s: Stmt): void => {
    switch (s.k) {
      case 'ret':
        seeExpr(s.e);
        break;
      case 'let':
        see(s.ty);
        seeExpr(s.e);
        seeStmt(s.body);
        break;
      case 'unreachable':
        break;
      case 'match':
        see(s.ty);
        for (const a of s.arms) {
          for (const b of a.binds) see(b.ty);
          seeStmt(a.body);
        }
        if (s.dflt !== null) seeStmt(s.dflt);
        break;
    }
  };
  for (const f of p.fns) {
    for (const x of f.params) see(x.ty);
    see(f.ret);
    seeStmt(f.body);
  }
  for (const l of p.laws) {
    for (const x of l.params) see(x.ty);
    for (const h of l.hyps) {
      see(h.ty);
      seeExpr(h.a);
      seeExpr(h.b);
    }
    see(l.ty);
    seeExpr(l.a);
    seeExpr(l.b);
  }
  return { adts: [...adts.values()], lists: [...lists.values()] };
}

function collectNatOps(p: Pick<Program, 'fns' | 'laws'>): Set<NatBinOp | NatUnOp> {
  const ops = new Set<NatBinOp | NatUnOp>();
  const walkExpr = (e: Expr): void => {
    if (e.k === 'natbin') ops.add(e.op);
    else if (e.k === 'natun') ops.add(e.op);
    for (const c of children(e)) walkExpr(c);
  };
  const walkStmt = (s: Stmt): void => {
    switch (s.k) {
      case 'ret':
        walkExpr(s.e);
        break;
      case 'let':
        walkExpr(s.e);
        walkStmt(s.body);
        break;
      case 'unreachable':
        break;
      case 'match':
        for (const a of s.arms) walkStmt(a.body);
        if (s.dflt !== null) walkStmt(s.dflt);
        break;
    }
  };
  for (const f of p.fns) walkStmt(f.body);
  for (const l of p.laws) {
    for (const h of l.hyps) {
      walkExpr(h.a);
      walkExpr(h.b);
    }
    walkExpr(l.a);
    walkExpr(l.b);
  }
  return ops;
}

// ---------- Rust

type TcoCtx = { name: string; params: Param[] };

function rsTy(t: Ty): string {
  switch (t.k) {
    case 'nat':
      return 'u64';
    case 'bool':
      return 'bool';
    case 'list':
      return `Rc<List<${rsTy(t.el)}>>`;
    case 'adt': {
      const an = tyName(t);
      return RECURSIVE_ADTS.has(an) ? `Rc<${an}>` : an;
    }
  }
}

function rsNatBin(op: NatBinOp, a: string, b: string): string {
  return `nat_${op}(${a}, ${b})`;
}

function rsExpr(e: Expr): string {
  switch (e.k) {
    case 'var':
      return e.ty.k === 'nat' || e.ty.k === 'bool' ? e.n : `${e.n}.clone()`;
    case 'nat':
      return `${e.v}u64`;
    case 'bool':
      return String(e.v);
    case 'natbin':
      return rsNatBin(e.op, rsExpr(e.a), rsExpr(e.b));
    case 'natun':
      return `nat_${e.op}(${rsExpr(e.a)})`;
    case 'cmp':
      return `(${rsExpr(e.a)} ${e.op} ${rsExpr(e.b)})`;
    case 'append':
      return `list_append(&${rsExpr(e.a)}, ${rsExpr(e.b)})`;
    case 'nil':
      return 'Rc::new(List::Nil)';
    case 'cons':
      return `Rc::new(List::Con(${rsExpr(e.h)}, ${rsExpr(e.t)}))`;
    case 'ctor': {
      const an = tyName(e.ty);
      const raw = e.args.length === 0 ? `${an}::${e.ctor}` : `${an}::${e.ctor}(${e.args.map(rsExpr).join(', ')})`;
      return RECURSIVE_ADTS.has(an) ? `Rc::new(${raw})` : raw;
    }
    case 'call':
      return `${id(e.f)}(${e.args.map(rsExpr).join(', ')})`;
  }
}

function rsStmt(s: Stmt, ind: string, tco: TcoCtx | null): string {
  switch (s.k) {
    case 'ret':
      if (tco !== null && s.e.k === 'call' && s.e.f === tco.name) {
        const temps = s.e.args.map((a, i) => `${ind}let __t${i} = ${rsExpr(a)};\n`).join('');
        const assigns = tco.params.map((p, i) => `${ind}${p.name} = __t${i};\n`).join('');
        return `${temps}${assigns}${ind}continue;\n`;
      }
      return `${ind}return ${rsExpr(s.e)};\n`;
    case 'let':
      return `${ind}let ${s.n}: ${rsTy(s.ty)} = ${rsExpr(s.e)};\n${rsStmt(s.body, ind, tco)}`;
    case 'unreachable':
      return `${ind}unreachable!()\n`;
    case 'match': {
      const t = s.ty;
      const arm = (c: string): Arm | undefined => s.arms.find((a) => a.ctor === c);
      const body = (a: Arm | undefined, ind2: string): string =>
        a !== undefined
          ? rsStmt(a.body, ind2, tco)
          : s.dflt !== null
            ? rsStmt(s.dflt, ind2, tco)
            : `${ind2}unreachable!()\n`;
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
        const target = tco !== null ? `&*${s.scrut}.clone()` : `&*${s.scrut}`;
        return `${ind}match ${target} {\n${ind}  List::Nil => {\n${body(arm('Nil'), `${ind}    `)}${ind}  }\n${ind}  List::Con(h__, t__) => {\n${hb !== undefined ? `${ind}    let ${hb.n}: ${rsTy(hb.ty)} = h__.clone();\n` : ''}${tb !== undefined ? `${ind}    let ${tb.n}: ${rsTy(tb.ty)} = t__.clone();\n` : ''}${body(c, `${ind}    `)}${ind}  }\n${ind}}\n`;
      }
      const an = tyName(t);
      const ctors = adtCtors(t);
      const isRec = RECURSIVE_ADTS.has(an);
      const target = isRec
        ? tco !== null
          ? `&*${s.scrut}.clone()`
          : `&*${s.scrut}`
        : tco !== null
          ? `&${s.scrut}.clone()`
          : `&${s.scrut}`;
      let out = `${ind}match ${target} {\n`;
      for (const c of ctors) {
        const a = arm(c.name);
        const fs = c.fields.map((_, i) => `f${i}__`);
        out += `${ind}  ${an}::${c.name}${fs.length ? `(${fs.join(', ')})` : ''} => {\n`;
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

function rsNatHelpers(ops: ReadonlySet<NatBinOp | NatUnOp>): string {
  let o = 'fn nat_add(a: u64, b: u64) -> u64 { let s = a + b; assert!(s < (1u64 << 48), "Nat past 2^48"); s }\n';
  if (ops.has('sub')) o += 'fn nat_sub(a: u64, b: u64) -> u64 { a.saturating_sub(b) }\n';
  if (ops.has('mul'))
    o +=
      'fn nat_mul(a: u64, b: u64) -> u64 { let p = a.checked_mul(b).expect("Nat past 2^48"); assert!(p < (1u64 << 48), "Nat past 2^48"); p }\n';
  if (ops.has('div')) o += 'fn nat_div(a: u64, b: u64) -> u64 { if b == 0 { 0 } else { a / b } }\n';
  if (ops.has('mod')) o += 'fn nat_mod(a: u64, b: u64) -> u64 { if b == 0 { a } else { a % b } }\n';
  if (ops.has('bit_and')) o += 'fn nat_bit_and(a: u64, b: u64) -> u64 { a & b }\n';
  if (ops.has('bit_or')) o += 'fn nat_bit_or(a: u64, b: u64) -> u64 { a | b }\n';
  if (ops.has('bit_xor')) o += 'fn nat_bit_xor(a: u64, b: u64) -> u64 { a ^ b }\n';
  if (ops.has('shl'))
    o +=
      'fn nat_shl(a: u64, b: u64) -> u64 { if a == 0 { return 0; } assert!(b < 48, "Nat past 2^48"); let r = a << b; assert!(r < (1u64 << 48) && (r >> b) == a, "Nat past 2^48"); r }\n';
  if (ops.has('shr')) o += 'fn nat_shr(a: u64, b: u64) -> u64 { if b >= 48 { 0 } else { a >> b } }\n';
  if (ops.has('popcount')) o += 'fn nat_popcount(a: u64) -> u64 { a.count_ones() as u64 }\n';
  if (ops.has('ctz_z0')) o += 'fn nat_ctz_z0(a: u64) -> u64 { if a == 0 { 0 } else { a.trailing_zeros() as u64 } }\n';
  if (ops.has('ctz_z64'))
    o += 'fn nat_ctz_z64(a: u64) -> u64 { if a == 0 { 64 } else { a.trailing_zeros() as u64 } }\n';
  return o;
}

function emitRust(p: Program, module: string | null = null): string {
  let o =
    module !== null
      ? `// GENERATED by bend-emit from a checked Bend book. Do not edit.\n${module}#![allow(non_snake_case, non_camel_case_types, unused_variables, unused_mut, unused_parens, unused_comparisons, unreachable_code, dead_code, clippy::all, clippy::pedantic, clippy::nursery)]\nuse std::rc::Rc;\n\n`
      : '// GENERATED by bend-emit from a checked Bend book. Do not edit.\n#![allow(non_snake_case, non_camel_case_types, unused_variables, unused_mut, unused_parens, unused_comparisons, unreachable_code, dead_code)]\nuse std::rc::Rc;\n\n';
  o += '#[derive(Clone, PartialEq)]\npub enum List<T> { Nil, Con(T, Rc<List<T>>) }\n\n';
  o += rsNatHelpers(p.natOps);
  o +=
    'fn list_append<T: Clone>(a: &Rc<List<T>>, b: Rc<List<T>>) -> Rc<List<T>> {\n  let mut xs = Vec::new(); let mut c = a.clone();\n  while let List::Con(h, t) = &*c { xs.push(h.clone()); let n = t.clone(); c = n; }\n  let mut r = b; for h in xs.into_iter().rev() { r = Rc::new(List::Con(h, r)); } r\n}\n\n';
  for (const a of p.adts) {
    const an = tyName(a);
    const rty = rsTy(a);
    const isRec = RECURSIVE_ADTS.has(an);
    const ctors = adtCtors(a);
    o += `#[derive(Clone, PartialEq)]\npub enum ${an} { ${ctors
      .map((c) => (c.fields.length ? `${c.name}(${c.fields.map((f) => rsTy(f.ty)).join(', ')})` : c.name))
      .join(', ')} }\n`;
    // Each ADT's text is written into one caller buffer (`write_*`), so a show allocates once, never a String per field.
    o += `pub fn write_${an}(x: &${rty}, out: &mut String) {\n  match ${isRec ? '&**x' : 'x'} {\n${ctors
      .map((c) => {
        const fs = c.fields.map((_, i) => `f${i}`);
        const body = fs.length
          ? `{ out.push_str("${c.name}("); ${c.fields.map((f, i) => `${i ? `out.push(','); ` : ''}write_${tyName(f.ty)}(f${i}, out);`).join(' ')} out.push(')'); }`
          : `out.push_str("${c.name}"),`;
        return `    ${an}::${c.name}${fs.length ? `(${fs.join(', ')})` : ''} => ${body}\n`;
      })
      .join('')}  }\n}\n`;
    o += `pub fn show_${an}(x: &${rty}) -> String { let mut s = String::new(); write_${an}(x, &mut s); s }\n\n`;
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
    if (f.tailRecursive) {
      const params = f.params.map((x) => `mut ${x.name}: ${rsTy(x.ty)}`).join(', ');
      const tco: TcoCtx = { name: f.name, params: f.params };
      o += `\npub fn ${id(f.name)}(${params}) -> ${rsTy(f.ret)} {\n  loop {\n${rsStmt(f.body, '    ', tco)}  }\n}\n`;
    } else {
      o += `\npub fn ${id(f.name)}(${f.params.map((x) => `${x.name}: ${rsTy(x.ty)}`).join(', ')}) -> ${rsTy(f.ret)} {\n${rsStmt(f.body, '  ', null)}}\n`;
    }
  }
  for (const l of p.laws) {
    o += `\npub fn law_${id(l.name)}(${l.params.map((x) => `${x.name}: ${rsTy(x.ty)}`).join(', ')}) -> (bool, String) {\n`;
    for (const h of l.hyps)
      o += `  if ${rsShow(h.ty, `(${rsExpr(h.a)})`)} != ${rsShow(h.ty, `(${rsExpr(h.b)})`)} { return (true, "vacuous".to_string()); }\n`;
    o += `  let a = ${rsExpr(l.a)};\n  let b = ${rsExpr(l.b)};\n  let sa = ${rsShow(l.ty, 'a')};\n  (sa == ${rsShow(l.ty, 'b')}, sa)\n}\n`;
  }
  if (module !== null) return o + emitRustCorpus(p);
  o +=
    '\nfn main() {\n  let allow_vacuous = std::env::args().any(|a| a == "--allow-vacuous");\n  let mut failed = 0u64;\n';
  p.laws.forEach((l, li) => {
    const cs = p.cases.get(l.name) ?? [];
    o += `  {\n    let mut pass = 0u64; let mut tested = 0u64; let mut vacuous = 0u64; let n = ${cs.length}u64;\n`;
    cs.forEach((row, i) => {
      o += `    { let (ok, v) = law_${id(l.name)}(${row.map((v) => rsExpr(valExpr(v))).join(', ')}); println!("law ${l.name} ${i} {} {}", ok, v); if !ok { tested += 1; } else if v == "vacuous" { pass += 1; vacuous += 1; } else { pass += 1; tested += 1; } }\n`;
    });
    o += `    println!("summary ${l.name} {}/{} ${l.proven ? 'proven' : 'open'} tested={} vacuous={}", pass, n, tested, vacuous);\n`;
    o += `    ${l.params.length > 0 ? `let mut r = Rng(${seedOf(li)}); ` : ''}let mut ppass = 0u64; let mut ptested = 0u64; let mut pvacuous = 0u64;\n`;
    o += `    for i in 0..${p.props} { ${l.params.map((x) => `let ${x.name} = gen_${tyName(x.ty)}(&mut r, ${p.propLen}); `).join('')}let (ok, v) = law_${id(l.name)}(${l.params.map((x) => x.name).join(', ')}); println!("prop ${l.name} {} {} {}", i, ok, v); if !ok { ptested += 1; } else if v == "vacuous" { ppass += 1; pvacuous += 1; } else { ppass += 1; ptested += 1; } }\n`;
    o += `    println!("propsummary ${l.name} {}/${p.props} tested={} vacuous={}", ppass, ptested, pvacuous);\n`;
    o += `    let tot = n + ${p.props}u64; let tot_tested = tested + ptested; let tot_vacuous = vacuous + pvacuous; let law_failed = (n - pass) + (${p.props}u64 - ppass);\n`;
    o += `    println!("definedness ${l.name} total={} tested={} vacuous={} failed={}", tot, tot_tested, tot_vacuous, law_failed);\n`;
    o += '    failed += law_failed; if !allow_vacuous && tot > 0 && tot_tested == 0 { failed += 1; }\n  }\n';
  });
  o += '  std::process::exit(if failed == 0 { 0 } else { 1 });\n}\n';
  o += `\nstruct Rng(u64);\nimpl Rng { fn next(&mut self) -> u64 { self.0 = self.0.wrapping_add(${MIX.inc}); let mut z = self.0; z = (z ^ (z >> 30)).wrapping_mul(${MIX.m1}); z = (z ^ (z >> 27)).wrapping_mul(${MIX.m2}); z ^ (z >> 31) } }\n`;
  for (const t of genTys(p)) {
    const n = tyName(t);
    if (t.k === 'nat') o += 'fn gen_Nat(r: &mut Rng, _m: u64) -> u64 { r.next() % 3 }\n';
    else if (t.k === 'bool') o += 'fn gen_Bool(r: &mut Rng, _m: u64) -> bool { r.next() % 2 == 0 }\n';
    else if (t.k === 'list')
      o += `fn gen_${n}(r: &mut Rng, m: u64) -> ${rsTy(t)} { let k = r.next() % (m + 1); let mut xs = Vec::new(); for _ in 0..k { xs.push(gen_${tyName(t.el)}(r, 3)); } let mut l: ${rsTy(t)} = Rc::new(List::Nil); for x in xs.into_iter().rev() { l = Rc::new(List::Con(x, l)); } l }\n`;
    else if (t.k === 'adt') {
      const cs = adtCtors(t);
      const isRec = RECURSIVE_ADTS.has(n);
      const wrap = (body: string): string => (isRec ? `Rc::new(${body})` : body);
      const arm = (c: CtorInfo, dExpr: string): string =>
        `${c.fields.map((f, j) => `let f${j} = gen_${tyName(f.ty)}(r, ${tyHasRecAdt(f.ty) ? dExpr : '3'}); `).join('')}${wrap(`${n}::${c.name}${c.fields.length ? `(${c.fields.map((_, j) => `f${j}`).join(', ')})` : ''}`)}`;
      if (isRec) {
        const base = baseCtorsOf(t);
        o += `fn gen_${n}(r: &mut Rng, m: u64) -> ${rsTy(t)} {\n  let d = if m > 3 { 3 } else { m };\n  if d == 0 {\n    match r.next() % ${base.length} {\n${base.map((c, i) => `      ${i === base.length - 1 ? '_' : String(i)} => { ${arm(c, '0')} }\n`).join('')}    }\n  } else {\n    match r.next() % ${cs.length} {\n${cs.map((c, i) => `      ${i === cs.length - 1 ? '_' : String(i)} => { ${arm(c, 'd - 1')} }\n`).join('')}    }\n  }\n}\n`;
      } else {
        o += `fn gen_${n}(r: &mut Rng, _m: u64) -> ${rsTy(t)} {\n  match r.next() % ${cs.length} {\n${cs.map((c, i) => `    ${i === cs.length - 1 ? '_' : String(i)} => { ${arm(c, '3')} }\n`).join('')}  }\n}\n`;
      }
    }
  }
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
      const cs = adtCtors(t);
      const isRec = RECURSIVE_ADTS.has(n);
      const wrap = (body: string): string => (isRec ? `Rc::new(${body})` : body);
      const arm = (c: CtorInfo, dExpr: string): string =>
        `${c.fields.map((f, j) => `let f${j} = gen_${tyName(f.ty)}(r, ${tyHasRecAdt(f.ty) ? dExpr : '3'}); `).join('')}${wrap(`${n}::${c.name}${c.fields.length ? `(${c.fields.map((_, j) => `f${j}`).join(', ')})` : ''}`)}`;
      if (isRec) {
        const base = baseCtorsOf(t);
        o += `pub fn gen_${n}(r: &mut Rng, m: u64) -> ${rsTy(t)} {\n  let d = if m > 3 { 3 } else { m };\n  if d == 0 {\n    match r.next() % ${base.length} {\n${base.map((c, i) => `      ${i === base.length - 1 ? '_' : String(i)} => { ${arm(c, '0')} }\n`).join('')}    }\n  } else {\n    match r.next() % ${cs.length} {\n${cs.map((c, i) => `      ${i === cs.length - 1 ? '_' : String(i)} => { ${arm(c, 'd - 1')} }\n`).join('')}    }\n  }\n}\n`;
      } else {
        o += `pub fn gen_${n}(r: &mut Rng, _m: u64) -> ${rsTy(t)} {\n  match r.next() % ${cs.length} {\n${cs.map((c, i) => `    ${i === cs.length - 1 ? '_' : String(i)} => { ${arm(c, '3')} }\n`).join('')}  }\n}\n`;
      }
    }
  }
  if (p.laws.length > 0) {
    o +=
      '\n#[derive(Clone, Copy, Debug, PartialEq, Eq)]\npub struct LawStats { pub total: u64, pub tested: u64, pub vacuous: u64, pub failed: u64 }\nimpl LawStats {\n  pub fn is_vacuous(&self) -> bool { self.total > 0 && self.tested == 0 }\n  pub fn passed(&self) -> bool { self.failed == 0 && !self.is_vacuous() }\n}\n';
  }
  p.laws.forEach((l, li) => {
    const tys = l.params.map((x) => rsTy(x.ty));
    const tuple = tys.length === 1 ? `(${tys[0]},)` : `(${tys.join(', ')})`;
    const rows = p.cases.get(l.name) ?? [];
    const formatRow = (row: Val[]): string =>
      row.length === 0
        ? '()'
        : row.length === 1
          ? `(${rsExpr(valExpr(row[0] as Val))},)`
          : `(${row.map((v) => rsExpr(valExpr(v))).join(', ')})`;
    o += `\n/// ${l.name} (${l.proven ? 'proven' : 'open'}): ${rows.length} enumerated cases.\npub fn corpus_${id(l.name)}() -> Vec<${tuple}> {\n  vec![\n${rows.map((row) => `    ${formatRow(row)},\n`).join('')}  ]\n}\n`;
    o += `/// ${l.name}'s seed and draw size: the same stream every target draws.\npub const SEED_${id(l.name).toUpperCase()}: u64 = ${seedOf(li)};\npub const PROPS_${id(l.name).toUpperCase()}: u64 = ${p.props};\npub const PROP_LEN_${id(l.name).toUpperCase()}: u64 = ${p.propLen};\n`;
    const pat =
      l.params.length === 0
        ? '()'
        : l.params.length === 1
          ? `(${l.params[0]?.name},)`
          : `(${l.params.map((x) => x.name).join(', ')})`;
    const argList = l.params.map((x) => x.name).join(', ');
    o += `pub fn check_${id(l.name)}_definedness() -> LawStats {\n  let mut s = LawStats { total: 0, tested: 0, vacuous: 0, failed: 0 };\n  for ${pat} in corpus_${id(l.name)}() {\n    let (ok, v) = law_${id(l.name)}(${argList});\n    s.total += 1; if !ok { s.failed += 1; s.tested += 1; } else if v == "vacuous" { s.vacuous += 1; } else { s.tested += 1; }\n  }\n${l.params.length > 0 ? `  let mut r = Rng(SEED_${id(l.name).toUpperCase()});\n` : ''}  for _ in 0..PROPS_${id(l.name).toUpperCase()} {\n    ${l.params.map((x) => `let ${x.name} = gen_${tyName(x.ty)}(&mut r, PROP_LEN_${id(l.name).toUpperCase()}); `).join('')}let (ok, v) = law_${id(l.name)}(${argList});\n    s.total += 1; if !ok { s.failed += 1; s.tested += 1; } else if v == "vacuous" { s.vacuous += 1; } else { s.tested += 1; }\n  }\n  s\n}\n`;
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
      return tyName(t);
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
    case 'natbin':
      return `nat_${e.op}(${gsExpr(e.a)}, ${gsExpr(e.b)})`;
    case 'natun':
      return `nat_${e.op}(${gsExpr(e.a)})`;
    case 'cmp':
      return `(${gsExpr(e.a)} ${e.op} ${gsExpr(e.b)})`;
    case 'append':
      return `append_${tyName({ k: 'list', el: e.el })}(${gsExpr(e.a)}, ${gsExpr(e.b)})`;
    case 'nil':
      return '-1';
    case 'cons':
      return `cons_${tyName({ k: 'list', el: e.el })}(${gsExpr(e.h)}, ${gsExpr(e.t)})`;
    case 'ctor': {
      const info = ctorInfoFor(e.ctor, e.ty);
      const an = tyName(e.ty);
      return info.fields.length === 0
        ? `${an}.${e.ctor}`
        : `${an}.${e.ctor} { ${info.fields.map((f, i) => `${f.name}: ${gsExpr(e.args[i] ?? refuse('missing field'))}`).join(', ')} }`;
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
      for (const c of adtCtors(t)) {
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

function gsNatHelpers(ops: ReadonlySet<NatBinOp | NatUnOp>): string {
  let o = 'fn nat_add(a: i64, b: i64) -> i64 { let s = a + b; assert(s < (1 << 48)); return s; }\n';
  if (ops.has('sub')) o += 'fn nat_sub(a: i64, b: i64) -> i64 { if a > b { return a - b; } return 0; }\n';
  if (ops.has('mul'))
    o +=
      'fn nat_mul(a: i64, b: i64) -> i64 { if a != 0 && b > (((1 << 48) - 1) / a) { assert(false); } let p = a * b; assert(p < (1 << 48)); return p; }\n';
  if (ops.has('div')) o += 'fn nat_div(a: i64, b: i64) -> i64 { if b == 0 { return 0; } return a / b; }\n';
  if (ops.has('mod')) o += 'fn nat_mod(a: i64, b: i64) -> i64 { if b == 0 { return a; } return a % b; }\n';
  if (ops.has('bit_and')) o += 'fn nat_bit_and(a: i64, b: i64) -> i64 { return a & b; }\n';
  if (ops.has('bit_or')) o += 'fn nat_bit_or(a: i64, b: i64) -> i64 { return a | b; }\n';
  if (ops.has('bit_xor')) o += 'fn nat_bit_xor(a: i64, b: i64) -> i64 { return a ^ b; }\n';
  if (ops.has('shl'))
    o +=
      'fn nat_shl(a: i64, b: i64) -> i64 { if a == 0 { return 0; } assert(b < 48); let r = a << b; assert(r < (1 << 48) && (r >> b) == a); return r; }\n';
  if (ops.has('shr')) o += 'fn nat_shr(a: i64, b: i64) -> i64 { if b >= 48 { return 0; } return a >> b; }\n';
  if (ops.has('popcount'))
    o +=
      'fn nat_popcount(a: i64) -> i64 { var x = a; var c = 0; while x != 0 { x = x & (x - 1); c = c + 1; } return c; }\n';
  if (ops.has('ctz_z0'))
    o +=
      'fn nat_ctz_z0(a: i64) -> i64 { if a == 0 { return 0; } var x = a; var c = 0; while (x & 1) == 0 { x = x >> 1; c = c + 1; } return c; }\n';
  if (ops.has('ctz_z64'))
    o +=
      'fn nat_ctz_z64(a: i64) -> i64 { if a == 0 { return 64; } var x = a; var c = 0; while (x & 1) == 0 { x = x >> 1; c = c + 1; } return c; }\n';
  return `${o}\n`;
}

function emitGoose(p: Program): string {
  let o = '// GENERATED by bend-emit from a checked Bend book. Do not edit.\n\n';
  o += gsNatHelpers(p.natOps);
  for (const a of p.adts) {
    const an = tyName(a);
    o += `enum ${an} { ${adtCtors(a)
      .map((c) =>
        c.fields.length ? `${c.name} { ${c.fields.map((f) => `${f.name}: ${gsTy(f.ty)}`).join(', ')} }` : c.name,
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
    const an = tyName(a);
    o += `fn show_${an}(x: ${an}, out: u8[>..]&) {\n  match x {\n${adtCtors(a)
      .map((c) =>
        c.fields.length
          ? `    ${c.name} v => { format(out, "${c.name}("); ${c.fields.map((f, i) => `${i ? `format(out, ","); ` : ''}show_${tyName(f.ty)}(v.${f.name}, out);`).join(' ')} format(out, ")"); },\n`
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
    const params = [...l.params.map((x) => `${x.name}: ${gsTy(x.ty)}`), 'out: u8[>..]&'].join(', ');
    o += `\nfn law_${id(l.name)}(${params}) -> bool {\n`;
    l.hyps.forEach((h, i) => {
      o += `  var ha${i}: u8[>..] = [];\n  var hb${i}: u8[>..] = [];\n  show_${tyName(h.ty)}(${gsExpr(h.a)}, ha${i});\n  show_${tyName(h.ty)}(${gsExpr(h.b)}, hb${i});\n  if !eq_bytes(ha${i}, hb${i}) { format(out, "vacuous"); return true; }\n`;
    });
    o += `  var sb: u8[>..] = [];\n  show_${tyName(l.ty)}(${gsExpr(l.a)}, out);\n  show_${tyName(l.ty)}(${gsExpr(l.b)}, sb);\n  return eq_bytes(out, sb);\n}\n`;
  }
  o +=
    '\nfn eq_bytes(a: u8[:], b: u8[:]) -> bool {\n  if a.len != b.len { return false; }\n  for i in a.len { if a[i] != b[i] { return false; } }\n  return true;\n}\n';
  o +=
    'fn is_vacuous(v: u8[:]) -> bool {\n  return v.len == 7 && v[0] == 118 && v[1] == 97 && v[2] == 99 && v[3] == 117 && v[4] == 111 && v[5] == 117 && v[6] == 115;\n}\n';
  o += '\nfn main() {\n  var failed = 0;\n';
  p.laws.forEach((l, li) => {
    const cs = p.cases.get(l.name) ?? [];
    o += '  {\n    var pass = 0;\n    var tested = 0;\n    var vacuous = 0;\n';
    cs.forEach((row, i) => {
      o += `    { var v: u8[>..] = []; let ok = law_${id(l.name)}(${[...row.map((v) => gsExpr(valExpr(v))), 'v'].join(', ')}); print("law ${l.name} ${i} ", ok, " ", v); if !ok { tested++; } else if is_vacuous(v) { pass++; vacuous++; } else { pass++; tested++; } }\n`;
    });
    o += `    print("summary ${l.name} ", pass, "/", ${cs.length}, " ${l.proven ? 'proven' : 'open'} tested=", tested, " vacuous=", vacuous);\n`;
    o += `    var r = Rng { s: ${seedOf(li)} };\n    var ppass = 0;\n    var ptested = 0;\n    var pvacuous = 0;\n    for i in ${p.props} {\n${l.params.map((x) => `      let ${x.name} = gen_${tyName(x.ty)}(r, ${p.propLen});\n`).join('')}      var v: u8[>..] = [];\n      let ok = law_${id(l.name)}(${[...l.params.map((x) => x.name), 'v'].join(', ')});\n      print("prop ${l.name} ", i, " ", ok, " ", v);\n      if !ok { ptested++; } else if is_vacuous(v) { ppass++; pvacuous++; } else { ppass++; ptested++; }\n    }\n`;
    o += `    print("propsummary ${l.name} ", ppass, "/${p.props} tested=", ptested, " vacuous=", pvacuous);\n`;
    o += `    let tot = ${cs.length + p.props};\n    let tot_tested = tested + ptested;\n    let tot_vacuous = vacuous + pvacuous;\n    let law_failed = (${cs.length} - pass) + (${p.props} - ppass);\n    print("definedness ${l.name} total=", tot, " tested=", tot_tested, " vacuous=", tot_vacuous, " failed=", law_failed);\n    failed = failed + law_failed;\n    if tot > 0 && tot_tested == 0 { failed = failed + 1; }\n  }\n`;
  });
  o += '  if failed != 0 { exit(1); }\n}\n';
  o += `\nstruct Rng { s: u64 }\nfn rng_next(r: Rng&) -> u64 { r.s = r.s + ${MIX.inc}; var z = r.s; z = (z ^ (z >> 30)) * ${MIX.m1}; z = (z ^ (z >> 27)) * ${MIX.m2}; return z ^ (z >> 31); }\n`;
  for (const t of genTys(p)) {
    const n = tyName(t);
    if (t.k === 'nat') o += 'fn gen_Nat(r: Rng&, m: i64) -> i64 { return (rng_next(r) % 3) as i64; }\n';
    else if (t.k === 'bool') o += 'fn gen_Bool(r: Rng&, m: i64) -> bool { return rng_next(r) % 2 == 0; }\n';
    else if (t.k === 'list')
      o += `fn gen_${n}(r: Rng&, m: i64) -> i64 {\n  let k = (rng_next(r) % ((m + 1) as u64)) as i64;\n  var xs: ${gsTy(t.el)}[>..] = [];\n  for j in k { xs.push(gen_${tyName(t.el)}(r, 3)); }\n  var l: i64 = -1;\n  var j = k - 1;\n  while j >= 0 { l = cons_${n}(xs[j], l); j = j - 1; }\n  return l;\n}\n`;
    else if (t.k === 'adt') {
      const cs = adtCtors(t);
      const isRec = RECURSIVE_ADTS.has(n);
      const arm = (c: CtorInfo, dExpr: string): string =>
        `${c.fields.map((f, j) => `let f${j} = gen_${tyName(f.ty)}(r, ${tyHasRecAdt(f.ty) ? dExpr : '3'}); `).join('')}return ${c.fields.length ? `${n}.${c.name} { ${c.fields.map((f, j) => `${f.name}: f${j}`).join(', ')} }` : `${n}.${c.name}`};`;
      if (isRec) {
        const base = baseCtorsOf(t);
        o += `fn gen_${n}(r: Rng&, m: i64) -> ${n} {\n  var d = m;\n  if d > 3 { d = 3; }\n  if d == 0 {\n    let c = rng_next(r) % ${base.length};\n${base.map((c, i) => `    if c == ${i} { ${arm(c, '0')} }\n`).join('')}    abort("gen");\n  }\n  let c = rng_next(r) % ${cs.length};\n${cs.map((c, i) => `  if c == ${i} { ${arm(c, 'd - 1')} }\n`).join('')}  abort("gen");\n}\n`;
      } else {
        o += `fn gen_${n}(r: Rng&, m: i64) -> ${n} {\n  let c = rng_next(r) % ${cs.length};\n${cs.map((c, i) => `  if c == ${i} { ${arm(c, '3')} }\n`).join('')}  abort("gen");\n}\n`;
      }
    }
  }
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
      return tyName(t);
  }
}

function tsRel(op: Rel): string {
  if (op === '==') return '===';
  if (op === '!=') return '!==';
  return op;
}

function tsExpr(e: Expr): string {
  switch (e.k) {
    case 'var':
      return e.n;
    case 'nat':
      return String(e.v);
    case 'bool':
      return String(e.v);
    case 'natbin':
      return `nat_${e.op}(${tsExpr(e.a)}, ${tsExpr(e.b)})`;
    case 'natun':
      return `nat_${e.op}(${tsExpr(e.a)})`;
    case 'cmp':
      return `(${tsExpr(e.a)} ${tsRel(e.op)} ${tsExpr(e.b)})`;
    case 'append':
      return `list_append(${tsExpr(e.a)}, ${tsExpr(e.b)})`;
    case 'nil':
      return 'NIL';
    case 'cons':
      return `{ $: "Con", head: ${tsExpr(e.h)}, tail: ${tsExpr(e.t)} }`;
    case 'ctor': {
      const info = ctorInfoFor(e.ctor, e.ty);
      return `{ $: "${e.ctor}"${info.fields.map((f, i) => `, ${f.name}: ${tsExpr(e.args[i] ?? refuse('missing field'))}`).join('')} } as const`;
    }
    case 'call':
      return `${id(e.f)}(${e.args.map(tsExpr).join(', ')})`;
  }
}

function tsStmt(s: Stmt, ind: string, tco: TcoCtx | null): string {
  switch (s.k) {
    case 'ret':
      if (tco !== null && s.e.k === 'call' && s.e.f === tco.name) {
        const temps = s.e.args
          .map(
            (a, i) => `${ind}const __t${i}: ${tsTy(tco.params[i]?.ty ?? refuse('tail call arity'))} = ${tsExpr(a)};\n`,
          )
          .join('');
        const assigns = tco.params.map((p, i) => `${ind}${p.name} = __t${i};\n`).join('');
        return `${temps}${assigns}${ind}continue tail;\n`;
      }
      return `${ind}return ${tsExpr(s.e)};\n`;
    case 'let':
      return `${ind}const ${s.n} = ${tsExpr(s.e)};\n${tsStmt(s.body, ind, tco)}`;
    case 'unreachable':
      return `${ind}throw new Error("unreachable");\n`;
    case 'match': {
      const t = s.ty;
      const arm = (c: string): Arm | undefined => s.arms.find((a) => a.ctor === c);
      const body = (a: Arm | undefined, ind2: string): string =>
        a !== undefined
          ? tsStmt(a.body, ind2, tco)
          : s.dflt !== null
            ? tsStmt(s.dflt, ind2, tco)
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
          : adtCtors(t);
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

function tsNatHelpers(ops: ReadonlySet<NatBinOp | NatUnOp>, ex: string): string {
  let o = `${ex}function nat_add(a: number, b: number): number { const s = a + b; if (!(s < 2 ** 48)) throw new Error("Nat past 2^48"); return s; }\n`;
  if (ops.has('sub')) o += `${ex}function nat_sub(a: number, b: number): number { return a > b ? a - b : 0; }\n`;
  if (ops.has('mul'))
    o += `${ex}function nat_mul(a: number, b: number): number { const p = a * b; if (!(p < 2 ** 48)) throw new Error("Nat past 2^48"); return p; }\n`;
  if (ops.has('div'))
    o += `${ex}function nat_div(a: number, b: number): number { return b === 0 ? 0 : Math.floor(a / b); }\n`;
  if (ops.has('mod')) o += `${ex}function nat_mod(a: number, b: number): number { return b === 0 ? a : a % b; }\n`;
  if (ops.has('bit_and'))
    o += `${ex}function nat_bit_and(a: number, b: number): number { return Number(BigInt(a) & BigInt(b)); }\n`;
  if (ops.has('bit_or'))
    o += `${ex}function nat_bit_or(a: number, b: number): number { return Number(BigInt(a) | BigInt(b)); }\n`;
  if (ops.has('bit_xor'))
    o += `${ex}function nat_bit_xor(a: number, b: number): number { return Number(BigInt(a) ^ BigInt(b)); }\n`;
  if (ops.has('shl'))
    o += `${ex}function nat_shl(a: number, b: number): number { if (a === 0) return 0; if (b >= 48) throw new Error("Nat past 2^48"); const r = BigInt(a) << BigInt(b); if (r >= (1n << 48n)) throw new Error("Nat past 2^48"); return Number(r); }\n`;
  if (ops.has('shr'))
    o += `${ex}function nat_shr(a: number, b: number): number { return b >= 48 ? 0 : Number(BigInt(a) >> BigInt(b)); }\n`;
  if (ops.has('popcount'))
    o += `${ex}function nat_popcount(a: number): number { let x = BigInt(a); let c = 0; while (x !== 0n) { x &= x - 1n; c++; } return c; }\n`;
  if (ops.has('ctz_z0'))
    o += `${ex}function nat_ctz_z0(a: number): number { if (a === 0) return 0; let x = BigInt(a); let c = 0; while ((x & 1n) === 0n) { x >>= 1n; c++; } return c; }\n`;
  if (ops.has('ctz_z64'))
    o += `${ex}function nat_ctz_z64(a: number): number { if (a === 0) return 64; let x = BigInt(a); let c = 0; while ((x & 1n) === 0n) { x >>= 1n; c++; } return c; }\n`;
  return o;
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
      const cs = adtCtors(t);
      const isRec = RECURSIVE_ADTS.has(n);
      const arm = (c: CtorInfo, dExpr: string): string =>
        `${c.fields.map((f, j) => `const f${j} = gen_${tyName(f.ty)}(r, ${tyHasRecAdt(f.ty) ? dExpr : '3'}); `).join('')}return { $: "${c.name}"${c.fields.map((f, j) => `, ${f.name}: f${j}`).join('')} };`;
      if (isRec) {
        const base = baseCtorsOf(t);
        o += `${ex}function gen_${n}(r: Rng, m: number): ${n} {\n  const d = m > 3 ? 3 : m;\n  if (d === 0) {\n    const c = r.next() % ${base.length}n;\n${base.map((c, i) => `    if (c === ${i}n) { ${arm(c, '0')} }\n`).join('')}    throw new Error("gen");\n  }\n  const c = r.next() % ${cs.length}n;\n${cs.map((c, i) => `  if (c === ${i}n) { ${arm(c, 'd - 1')} }\n`).join('')}  throw new Error("gen");\n}\n`;
      } else {
        o += `${ex}function gen_${n}(r: Rng, _m: number): ${n} {\n  const c = r.next() % ${cs.length}n;\n${cs.map((c, i) => `  if (c === ${i}n) { ${arm(c, '3')} }\n`).join('')}  throw new Error("gen");\n}\n`;
      }
    }
  }
  return o;
}

// Module mode: each law's enumerated corpus, its seed and its draw size, beside the generators, for a
// binding driver that runs the model and the code on the same inputs.
function emitTsCorpus(p: Program): string {
  let o = '';
  if (p.laws.length > 0) {
    o +=
      '\nexport interface LawStats { readonly total: number; readonly tested: number; readonly vacuous: number; readonly failed: number; readonly isVacuous: boolean; readonly passed: boolean; }\n';
  }
  p.laws.forEach((l, li) => {
    const tuple = `[${l.params.map((x) => tsTy(x.ty)).join(', ')}]`;
    const rows = p.cases.get(l.name) ?? [];
    o += `\n/** ${l.name} (${l.proven ? 'proven' : 'open'}): ${rows.length} enumerated cases. */\nexport function corpus_${id(l.name)}(): ${tuple}[] {\n  return [\n${rows.map((row) => `    [${row.map((v) => tsExpr(valExpr(v))).join(', ')}],\n`).join('')}  ];\n}\n`;
    o += `/** ${l.name}'s seed and draw size: the same stream every target draws. */\nexport const SEED_${id(l.name).toUpperCase()} = ${seedOf(li)}n;\nexport const PROPS_${id(l.name).toUpperCase()} = ${p.props};\nexport const PROP_LEN_${id(l.name).toUpperCase()} = ${p.propLen};\n`;
    const pat = `[${l.params.map((x) => x.name).join(', ')}]`;
    const argList = l.params.map((x) => x.name).join(', ');
    o += `export function check_${id(l.name)}_definedness(): LawStats {\n  let total = 0; let tested = 0; let vacuous = 0; let failed = 0;\n  for (const ${pat} of corpus_${id(l.name)}()) {\n    const [ok, v] = law_${id(l.name)}(${argList});\n    total++; if (!ok) { failed++; tested++; } else if (v === "vacuous") vacuous++; else tested++;\n  }\n  const r = new Rng(SEED_${id(l.name).toUpperCase()});\n  for (let i = 0; i < PROPS_${id(l.name).toUpperCase()}; i++) {\n    ${l.params.map((x) => `const ${x.name} = gen_${tyName(x.ty)}(r, PROP_LEN_${id(l.name).toUpperCase()}); `).join('')}const [ok, v] = law_${id(l.name)}(${argList});\n    total++; if (!ok) { failed++; tested++; } else if (v === "vacuous") vacuous++; else tested++;\n  }\n  const isVacuous = total > 0 && tested === 0;\n  return { total, tested, vacuous, failed, isVacuous, passed: failed === 0 && !isVacuous };\n}\n`;
  });
  return o;
}

function emitTsMachines(p: Program): string {
  let o = '';
  for (const m of p.machines) {
    o += `\n/** Machine ${m.name}: ${m.inputs.length} enumerated inputs. */\nexport function inputs_${m.name}(): ${tsTy(m.inputTy)}[] {\n  return [\n${m.inputs.map((v) => `    ${tsExpr(valExpr(v))},\n`).join('')}  ];\n}\n`;
    o += `\nexport const machine_${m.name} = {\n  name: ${JSON.stringify(m.name)},\n  init: ${id(m.init.name)},\n  step: ${id(m.step.name)},\n  inputs: inputs_${m.name},\n  showState: show_${tyName(m.stateTy)},\n  showInput: show_${tyName(m.inputTy)},${m.handled !== null ? `\n  isHandledSelfLoop: ${id(m.handled.name)},` : ''}\n} as const;\n`;
  }
  return o;
}

function emitTs(p: Program, module: string | null = null): string {
  const ex = module !== null ? 'export ' : '';
  let o = `// GENERATED by bend-emit from a checked Bend book. Do not edit.\n${module ?? ''}`;
  o += `export type List<T> = { readonly $: "Nil" } | { readonly $: "Con"; readonly head: T; readonly tail: List<T> };\n${ex}const NIL: { readonly $: "Nil" } = { $: "Nil" };\n`;
  o += tsNatHelpers(p.natOps, ex);
  o += `${ex}function list_append<T>(a: List<T>, b: List<T>): List<T> { const xs: T[] = []; for (let c = a; c.$ === "Con"; c = c.tail) xs.push(c.head); let r = b; for (let i = xs.length - 1; i >= 0; i--) { const h = xs[i]; if (h === undefined) throw new Error("index"); r = { $: "Con", head: h, tail: r }; } return r; }\n`;
  o += `${ex}function show_Nat(x: number): string { return String(x); }\n${ex}function show_Bool(x: boolean): string { return String(x); }\n`;
  for (const a of p.adts) {
    const an = tyName(a);
    const ctors = adtCtors(a);
    o += `export type ${an} = ${ctors
      .map((c) => `{ readonly $: "${c.name}"${c.fields.map((f) => `; readonly ${f.name}: ${tsTy(f.ty)}`).join('')} }`)
      .join(' | ')};\n`;
    o += `${ex}function show_${an}(x: ${an}): string {\n  switch (x.$) {\n${ctors
      .map(
        (c) =>
          `    case "${c.name}": return ${c.fields.length ? `"${c.name}(" + ${c.fields.map((f) => `show_${tyName(f.ty)}(x.${f.name})`).join(` + "," + `)} + ")"` : `"${c.name}"`};\n`,
      )
      .join('')}  }\n}\n`;
  }
  for (const l of p.lists) {
    if (l.k !== 'list') continue;
    o += `${ex}function show_${tyName(l)}(x: ${tsTy(l)}): string { const xs: string[] = []; for (let c = x; c.$ === "Con"; c = c.tail) xs.push(show_${tyName(l.el)}(c.head)); return "[" + xs.join(",") + "]"; }\n`;
  }
  for (const f of p.fns) {
    if (f.tailRecursive) {
      const tco: TcoCtx = { name: f.name, params: f.params };
      o += `\nexport function ${id(f.name)}(${f.params.map((x) => `${x.name}: ${tsTy(x.ty)}`).join(', ')}): ${tsTy(f.ret)} {\n  tail: for (;;) {\n${tsStmt(f.body, '    ', tco)}  }\n}\n`;
    } else {
      o += `\nexport function ${id(f.name)}(${f.params.map((x) => `${x.name}: ${tsTy(x.ty)}`).join(', ')}): ${tsTy(f.ret)} {\n${tsStmt(f.body, '  ', null)}}\n`;
    }
  }
  for (const l of p.laws) {
    o += `\nexport function law_${id(l.name)}(${l.params.map((x) => `${x.name}: ${tsTy(x.ty)}`).join(', ')}): [boolean, string] {\n`;
    for (const h of l.hyps)
      o += `  if (show_${tyName(h.ty)}(${tsExpr(h.a)}) !== show_${tyName(h.ty)}(${tsExpr(h.b)})) return [true, "vacuous"];\n`;
    o += `  const sa = show_${tyName(l.ty)}(${tsExpr(l.a)});\n  return [sa === show_${tyName(l.ty)}(${tsExpr(l.b)}), sa];\n}\n`;
  }
  if (module !== null) return o + tsGens(p, ex) + emitTsCorpus(p) + emitTsMachines(p);
  let m =
    '\nif (import.meta.main) {\n  const allowVacuous = process.argv.includes("--allow-vacuous");\n  let failed = 0;\n';
  p.laws.forEach((l, li) => {
    const cs = p.cases.get(l.name) ?? [];
    m += '  {\n    let pass = 0; let tested = 0; let vacuous = 0;\n';
    cs.forEach((row, i) => {
      m += `    { const [ok, v] = law_${id(l.name)}(${row.map((v) => tsExpr(valExpr(v))).join(', ')}); console.log(\`law ${l.name} ${i} \${ok} \${v}\${ok ? "" : " | args ${row.map(showVal).join(' ; ')}"}\`); if (!ok) { tested++; } else if (v === "vacuous") { pass++; vacuous++; } else { pass++; tested++; } }\n`;
    });
    m += `    console.log(\`summary ${l.name} \${pass}/${cs.length} tested=\${tested} vacuous=\${vacuous}\`);\n`;
    m += `    const r = new Rng(${seedOf(li)}n);\n    let ppass = 0; let ptested = 0; let pvacuous = 0;\n    for (let i = 0; i < ${p.props}; i++) {\n${l.params.map((x) => `      const ${x.name} = gen_${tyName(x.ty)}(r, ${p.propLen});\n`).join('')}      const [ok, v] = law_${id(l.name)}(${l.params.map((x) => x.name).join(', ')});\n      console.log(\`prop ${l.name} \${i} \${ok} \${v}\${ok ? "" : " | args " + [${l.params.map((x) => `show_${tyName(x.ty)}(${x.name})`).join(', ')}].join(" ; ")}\`);\n      if (!ok) { ptested++; } else if (v === "vacuous") { ppass++; pvacuous++; } else { ppass++; ptested++; }\n    }\n    console.log(\`propsummary ${l.name} \${ppass}/${p.props} tested=\${ptested} vacuous=\${pvacuous}\`);\n`;
    m += `    const tot = ${cs.length + p.props}; const totTested = tested + ptested; const totVacuous = vacuous + pvacuous; const lawFailed = (${cs.length} - pass) + (${p.props} - ppass);\n    console.log(\`definedness ${l.name} total=\${tot} tested=\${totTested} vacuous=\${totVacuous} failed=\${lawFailed}\`);\n    failed += lawFailed;\n    if (!allowVacuous && tot > 0 && totTested === 0) failed++;\n  }\n`;
  });
  m += '  process.exit(failed === 0 ? 0 : 1);\n}\n';
  o += tsGens(p, '');
  return `${o}${m}`;
}

// ---------- Go

// Leave ordinary model names readable; encode qualified names in a disjoint namespace.
function goName(role: string, name: string): string {
  return /^[A-Za-z0-9_]+$/.test(name)
    ? `${role}_${name}`
    : `${role}Q_${name.replace(/[^a-zA-Z0-9]/gu, (c) => `_${c.codePointAt(0)?.toString(16)}_`)}`;
}

function goCtorName(t: AdtTy, ctorName: string): string {
  return t.args.length === 0 ? goName('Ctor', ctorName) : goName('Ctor', `${tyName(t)}_${ctorName}`);
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
      return goName('Type', tyName(t));
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
      return goName(`${op}Type`, tyName(t));
  }
}

function goNatBinFn(op: NatBinOp): string {
  switch (op) {
    case 'add':
      return 'natAdd';
    case 'sub':
      return 'natSub';
    case 'mul':
      return 'natMul';
    case 'div':
      return 'natDiv';
    case 'mod':
      return 'natMod';
    case 'bit_and':
      return 'natBitAnd';
    case 'bit_or':
      return 'natBitOr';
    case 'bit_xor':
      return 'natBitXor';
    case 'shl':
      return 'natShl';
    case 'shr':
      return 'natShr';
  }
}

function goNatUnFn(op: NatUnOp): string {
  switch (op) {
    case 'popcount':
      return 'natPopcount';
    case 'ctz_z0':
      return 'natCtzZ0';
    case 'ctz_z64':
      return 'natCtzZ64';
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
    case 'natbin':
      return `${goNatBinFn(e.op)}(${goExpr(e.a)}, ${goExpr(e.b)})`;
    case 'natun':
      return `${goNatUnFn(e.op)}(${goExpr(e.a)})`;
    case 'cmp':
      return `(${goExpr(e.a)} ${e.op} ${goExpr(e.b)})`;
    case 'nil':
      return `(*List[${goTy(e.el)}])(nil)`;
    case 'cons':
      return `&List[${goTy(e.el)}]{${goExpr(e.h)}, ${goExpr(e.t)}}`;
    case 'append':
      return `listAppend(${goExpr(e.a)}, ${goExpr(e.b)})`;
    case 'ctor':
      return `${goCtorName(e.ty, e.ctor)}{${e.args.map(goExpr).join(', ')}}`;
    case 'call':
      return `${goName('Fn', e.f)}(${e.args.map(goExpr).join(', ')})`;
  }
}

// Only live pattern variables are legal Go locals. IR binders are already unique within a function.
function goReads(s: Stmt, used: Set<string>): void {
  const walk = (e: Expr): void => {
    if (e.k === 'var') used.add(e.n);
    else for (const child of children(e)) walk(child);
  };
  switch (s.k) {
    case 'ret':
      walk(s.e);
      break;
    case 'unreachable':
      break;
    case 'let':
      walk(s.e);
      goReads(s.body, used);
      break;
    case 'match':
      used.add(s.scrut);
      for (const a of s.arms) goReads(a.body, used);
      if (s.dflt !== null) goReads(s.dflt, used);
  }
}

function goStmt(s: Stmt, used: ReadonlySet<string>, ind: string, tco: TcoCtx | null): string {
  switch (s.k) {
    case 'ret':
      if (tco !== null && s.e.k === 'call' && s.e.f === tco.name) {
        const lhs = tco.params.map((p) => p.name).join(', ');
        const rhs = s.e.args.map(goExpr).join(', ');
        return `${ind}${lhs} = ${rhs}\n${ind}continue tail\n`;
      }
      return `${ind}return ${goExpr(s.e)}\n`;
    case 'unreachable':
      return `${ind}panic("unreachable Bend pattern")\n`;
    case 'let':
      return `${ind}${s.n} := ${goExpr(s.e)}\n${goStmt(s.body, used, ind, tco)}`;
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
        const pattern = s.ty.k === 'adt' ? goCtorName(s.ty, a.ctor) : String(['Zero', 'True', 'Nil'].includes(a.ctor));
        out += `${ind}case ${pattern}:\n`;
        a.binds.forEach((b, i) => {
          if (!used.has(b.n)) return;
          const field =
            s.ty.k === 'nat' ? `${x} - 1` : s.ty.k === 'list' ? `${x}.${i === 0 ? 'Head' : 'Tail'}` : `variant.F${i}`;
          out += `${ind}\t${b.n} := ${field}\n`;
        });
        out += goStmt(a.body, used, `${ind}\t`, tco);
      }
      return `${out}${ind}default:\n${goStmt(s.dflt ?? { k: 'unreachable' }, used, `${ind}\t`, tco)}${ind}}\n`;
    }
  }
}

function goNatHelpers(ops: ReadonlySet<NatBinOp | NatUnOp>): string {
  let out = `func natAdd(a, b uint64) uint64 {
  if a >= 1 << 48 || b >= (1 << 48) - a { panic("Nat past 2^48") }
  return a + b
}
`;
  if (ops.has('sub')) out += 'func natSub(a, b uint64) uint64 { if a > b { return a - b }; return 0 }\n';
  if (ops.has('mul'))
    out +=
      'func natMul(a, b uint64) uint64 { if a >= (1<<48) || b >= (1<<48) || (a != 0 && b > ((1<<48)-1)/a) { panic("Nat past 2^48") }; return a * b }\n';
  if (ops.has('div')) out += 'func natDiv(a, b uint64) uint64 { if b == 0 { return 0 }; return a / b }\n';
  if (ops.has('mod')) out += 'func natMod(a, b uint64) uint64 { if b == 0 { return a }; return a % b }\n';
  if (ops.has('bit_and')) out += 'func natBitAnd(a, b uint64) uint64 { return a & b }\n';
  if (ops.has('bit_or')) out += 'func natBitOr(a, b uint64) uint64 { return a | b }\n';
  if (ops.has('bit_xor')) out += 'func natBitXor(a, b uint64) uint64 { return a ^ b }\n';
  if (ops.has('shl'))
    out +=
      'func natShl(a, b uint64) uint64 { if a == 0 { return 0 }; if b >= 48 { panic("Nat past 2^48") }; r := a << b; if r >= (1<<48) || (r>>b) != a { panic("Nat past 2^48") }; return r }\n';
  if (ops.has('shr')) out += 'func natShr(a, b uint64) uint64 { if b >= 48 { return 0 }; return a >> b }\n';
  if (ops.has('popcount')) out += 'func natPopcount(a uint64) uint64 { return uint64(bits.OnesCount64(a)) }\n';
  if (ops.has('ctz_z0'))
    out += 'func natCtzZ0(a uint64) uint64 { if a == 0 { return 0 }; return uint64(bits.TrailingZeros64(a)) }\n';
  if (ops.has('ctz_z64'))
    out += 'func natCtzZ64(a uint64) uint64 { if a == 0 { return 64 }; return uint64(bits.TrailingZeros64(a)) }\n';
  return out;
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

${goNatHelpers(p.natOps)}`;
  for (const adt of p.adts) {
    const an = tyName(adt);
    const marker = goName('is', an);
    out += `\ntype ${goName('Type', an)} interface { ${marker}() }\n`;
    for (const c of adtCtors(adt)) {
      const name = goCtorName(adt, c.name);
      out += `type ${name} struct { ${c.fields.map((f, i) => `F${i} ${goTy(f.ty)}`).join('; ')} }\nfunc (${name}) ${marker}() {}\n`;
    }
  }
  for (const f of p.fns) {
    const used = new Set<string>();
    goReads(f.body, used);
    if (f.tailRecursive) {
      const tco: TcoCtx = { name: f.name, params: f.params };
      out += `\nfunc ${goName('Fn', f.name)}(${f.params.map((x) => `${x.name} ${goTy(x.ty)}`).join(', ')}) ${goTy(f.ret)} {\ntail:\n\tfor {\n${goStmt(f.body, used, '\t\t', tco)}\t}\n}\n`;
    } else {
      out += `\nfunc ${goName('Fn', f.name)}(${f.params.map((x) => `${x.name} ${goTy(x.ty)}`).join(', ')}) ${goTy(f.ret)} {\n${goStmt(f.body, used, '\t', null)}}\n`;
    }
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
      const cs = adtCtors(t);
      const binding = cs.some((c) => c.fields.length > 0) ? 'v := ' : '';
      out += `\n${writer} {\n  switch ${binding}x.(type) {\n`;
      for (const c of cs) {
        out += `  case ${goCtorName(t, c.name)}:\n    s.WriteString(${JSON.stringify(c.name + (c.fields.length ? '(' : ''))})\n`;
        c.fields.forEach((f, i) => {
          if (i > 0) out += `    s.WriteByte(',')\n`;
          out += `    ${goHelper('write', f.ty)}(s, v.F${i})\n`;
        });
        if (c.fields.length) out += `    s.WriteByte(')')\n`;
      }
      const isRec = RECURSIVE_ADTS.has(tyName(t));
      const goGenArm = (c: CtorInfo, dExpr: string): string =>
        `${goCtorName(t, c.name)}{${c.fields.map((f) => `${goHelper('gen', f.ty)}(r, ${tyHasRecAdt(f.ty) ? dExpr : '3'})`).join(', ')}}`;
      if (isRec) {
        const base = baseCtorsOf(t);
        out += `  default: panic("invalid Bend value")\n  }\n}\n${gen} {\n  d := m\n  if d > 3 { d = 3 }\n  if d == 0 {\n    switch r.next() % ${base.length} {\n`;
        base.forEach((c, i) => {
          out += `    ${i === base.length - 1 ? 'default' : `case ${i}`}: return ${goGenArm(c, '0')}\n`;
        });
        out += `    }\n  }\n  switch r.next() % ${cs.length} {\n`;
        cs.forEach((c, i) => {
          out += `  ${i === cs.length - 1 ? 'default' : `case ${i}`}: return ${goGenArm(c, 'd - 1')}\n`;
        });
        out += '  }\n}\n';
      } else {
        out += `  default: panic("invalid Bend value")\n  }\n}\n${gen} {\n  switch r.next() % ${cs.length} {\n`;
        cs.forEach((c, i) => {
          out += `  ${i === cs.length - 1 ? 'default' : `case ${i}`}: return ${goGenArm(c, '3')}\n`;
        });
        out += '  }\n}\n';
      }
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

function goLaw(p: Program, law: Law, index: number, includeDefinedness: boolean): string {
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
  out += `  for i := 0; i < ${p.props}; i++ { run(${['"prop"', 'i', ...draws].join(', ')}) }\n}\n`;
  if (includeDefinedness) {
    const ignored = law.params.map((x) => `_ ${goTy(x.ty)}`);
    out += `\nfunc ${goName('Check', `${law.name}_definedness`)}() LawStats {\n  var s LawStats\n  ${goName('Drive', law.name)}(func(${['_ string', '_ int', 'ok bool', 'lhs string', ...ignored].join(', ')}) {\n    s.Total++\n    if !ok { s.Failed++; s.Tested++ } else if lhs == "vacuous" { s.Vacuous++ } else { s.Tested++ }\n  })\n  return s\n}\n`;
  }
  return out;
}

function emitGo(p: Program, module: string | null = null): string {
  const needBits = p.natOps.has('popcount') || p.natOps.has('ctz_z0') || p.natOps.has('ctz_z64');
  let out = `// Code generated by bend-emit from a checked Bend book. DO NOT EDIT.\n${module ?? ''}package ${module === null ? 'main' : 'bendmodel'}\n`;
  const imports: string[] = [];
  if (needBits) imports.push('"math/bits"');
  if (p.laws.length > 0) {
    imports.push('"strconv"', '"strings"');
    if (module === null) imports.push('"fmt"', '"os"');
  }
  if (imports.length > 0) {
    out += `\nimport (\n${imports.map((imp) => `  ${imp}\n`).join('')})\n`;
  }
  out += `\n${goData(p)}`;
  if (p.laws.length > 0) out += goValues(p);
  if (module !== null && p.laws.length > 0) {
    out +=
      '\ntype LawStats struct { Total int; Tested int; Vacuous int; Failed int }\nfunc (s LawStats) IsVacuous() bool { return s.Total > 0 && s.Tested == 0 }\nfunc (s LawStats) Passed() bool { return s.Failed == 0 && !s.IsVacuous() }\n';
  }
  p.laws.forEach((l, i) => {
    out += goLaw(p, l, i, module !== null);
  });
  if (module !== null) return out;
  if (p.laws.length === 0) return `${out}\nfunc main() {}\n`;
  out +=
    '\nfunc main() {\n  allowVacuous := false\n  for _, arg := range os.Args[1:] { if arg == "--allow-vacuous" { allowVacuous = true } }\n  failed := false\n';
  for (const l of p.laws) {
    const params = l.params.map((x) => `${x.name} ${goTy(x.ty)}`);
    const show = l.params.map((x) => goShow(x.ty, x.name)).join(', ');
    out += `  {\n    total, tested, vacuous, lawFailed := 0, 0, 0, 0\n    ${goName('Drive', l.name)}(func(${['kind string', 'index int', 'ok bool', 'lhs string', ...params].join(', ')}) {\n      fmt.Printf("%s %s %d %t %s", kind, ${JSON.stringify(l.name)}, index, ok, lhs)\n      total++\n      if !ok { failed = true; lawFailed++; tested++; fmt.Print(" | args ", strings.Join([]string{${show}}, " ; ")) } else if lhs == "vacuous" { vacuous++ } else { tested++ }\n      fmt.Println()\n    })\n    fmt.Printf("definedness %s total=%d tested=%d vacuous=%d failed=%d\\n", ${JSON.stringify(l.name)}, total, tested, vacuous, lawFailed)\n    if !allowVacuous && total > 0 && tested == 0 { failed = true }\n  }\n`;
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

export interface LawManifestEntry {
  readonly name: string;
  readonly proven: boolean;
  readonly digest: string;
  readonly params: readonly { readonly name: string; readonly type: string }[];
  readonly hypsCount: number;
  readonly corpusCount: number;
  readonly propsCount: number;
  readonly reachedFns: readonly string[];
}

export interface MachineSpec {
  readonly name: string;
  readonly init: string;
  readonly step: string;
  readonly input: string;
  readonly handled?: string;
}

export interface MachineManifestEntry {
  readonly name: string;
  readonly init: string;
  readonly step: string;
  readonly stateType: string;
  readonly inputType: string;
  readonly handled: string | null;
  readonly inputCount: number;
}

export interface EmitManifest {
  readonly modelName: string;
  readonly bendRevision: string;
  readonly hashAlgorithm: 'blake3';
  readonly laws: readonly LawManifestEntry[];
  readonly machines: readonly MachineManifestEntry[];
}

export interface EmitOptions {
  readonly target: Target;
  readonly modelName?: string;
  readonly functions?: readonly string[];
  readonly laws?: readonly string[];
  readonly machines?: readonly MachineSpec[];
  readonly cap?: number;
  readonly listLength?: number;
  readonly adtDepth?: number;
  readonly properties?: number;
  readonly propertyLength?: number;
  readonly noCorpus?: readonly string[];
}

export type Emission = {
  readonly functions: number;
  readonly laws: number;
  readonly manifest: EmitManifest;
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
  /** Generate proof skeletons for unfilled or single-hole laws and proof lemmas in dependency order. */
  skeleton(options?: SkeletonOptions): SkeletonResult;
}

function exprDirectCalls(e: Expr): string[] {
  return [...(e.k === 'call' ? [e.f] : []), ...children(e).flatMap(exprDirectCalls)];
}

function lawReachedFns(law: Law, fns: ReadonlyMap<string, Fn>): string[] {
  const reached = new Set<string>();
  const todo = [
    ...law.hyps.flatMap((h) => [...exprDirectCalls(h.a), ...exprDirectCalls(h.b)]),
    ...exprDirectCalls(law.a),
    ...exprDirectCalls(law.b),
  ];
  while (todo.length > 0) {
    const k = todo.pop();
    if (k === undefined || reached.has(k)) continue;
    const f = fns.get(k);
    if (f === undefined) continue;
    reached.add(k);
    for (const c of calls(f.body)) if (!reached.has(c)) todo.push(c);
  }
  return [...reached].sort();
}

function emitBook({
  target,
  modelName,
  functions = [],
  laws: names = [],
  machines: machineSpecs = [],
  cap = 400,
  properties = 200,
  propertyLength = 12,
  noCorpus = [],
}: EmitOptions): Emission {
  if (machineSpecs.length > 0 && target.kind !== 'ts-module') {
    refuse(`machines are only supported for ts-module, not ${target.kind}`);
  }
  const seenMachineNames = new Set<string>();
  const machineFns: string[] = [];
  for (const m of machineSpecs) {
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(m.name)) {
      refuse(`machine name ${JSON.stringify(m.name)} is not a valid identifier`);
    }
    if (seenMachineNames.has(m.name)) {
      refuse(`duplicate machine name ${m.name}`);
    }
    seenMachineNames.add(m.name);
    machineFns.push(m.init, m.step);
    if (m.handled !== undefined) machineFns.push(m.handled);
  }
  const laws = names.map(compileLaw);
  // close the set of defs the laws, machines and requested functions reach
  const todo = [...functions, ...machineFns, ...USED];
  const fns = new Map<string, Fn>();
  while (todo.length) {
    const k = todo.pop();
    if (k === undefined || fns.has(k) || INTRINSIC.has(k)) continue;
    fns.set(k, compileFn(k));
    for (const u of USED) if (!fns.has(u)) todo.push(u);
  }
  verifyUsedBaseNatIntrinsics();
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
  const lowered = lower(order);
  const loweredMap = new Map(lowered.map((f) => [f.name, f]));
  const types = collectTypes({ fns: lowered, laws, cases: new Map() });
  RECURSIVE_ADTS = computeRecursiveAdts(types.adts);
  const machines: CompiledMachine[] = machineSpecs.map((m) => {
    const initFn = loweredMap.get(m.init);
    if (initFn === undefined) refuse(`machine ${m.name}: init function ${m.init} not found`);
    if (initFn.params.length !== 0) {
      refuse(`machine ${m.name}: init function ${m.init} must take 0 parameters, got ${initFn.params.length}`);
    }
    const stepFn = loweredMap.get(m.step);
    if (stepFn === undefined) refuse(`machine ${m.name}: step function ${m.step} not found`);
    const [pState, pInput] = stepFn.params;
    if (stepFn.params.length !== 2 || pState === undefined || pInput === undefined) {
      refuse(`machine ${m.name}: step function ${m.step} must take 2 parameters, got ${stepFn.params.length}`);
    }
    const stateTy = initFn.ret;
    if (tyName(pState.ty) !== tyName(stateTy) || tyName(stepFn.ret) !== tyName(stateTy)) {
      refuse(
        `machine ${m.name}: step function ${m.step} must map (${tyName(stateTy)}, ${tyName(pInput.ty)}) -> ${tyName(stateTy)}, got (${tyName(pState.ty)}, ${tyName(pInput.ty)}) -> ${tyName(stepFn.ret)}`,
      );
    }
    const inputTy = pInput.ty;
    if (tyName(inputTy) !== m.input) {
      refuse(`machine ${m.name}: input type ${m.input} does not match step parameter type ${tyName(inputTy)}`);
    }
    let handledFn: Fn | null = null;
    if (m.handled !== undefined) {
      const found = loweredMap.get(m.handled);
      if (found === undefined) refuse(`machine ${m.name}: handled function ${m.handled} not found`);
      const [hState, hInput] = found.params;
      if (
        found.params.length !== 2 ||
        hState === undefined ||
        hInput === undefined ||
        tyName(hState.ty) !== tyName(stateTy) ||
        tyName(hInput.ty) !== tyName(inputTy) ||
        found.ret.k !== 'bool'
      ) {
        refuse(
          `machine ${m.name}: handled function ${m.handled} must have signature (${tyName(stateTy)}, ${tyName(inputTy)}) -> Bool`,
        );
      }
      handledFn = found;
    }
    const inputs = enumerate(inputTy, 0);
    if (inputs.length === 0) {
      refuse(`machine ${m.name}: input type ${tyName(inputTy)} enumerated 0 values`);
    }
    if (inputs.length > cap) {
      refuse(`machine ${m.name}: enumerated ${inputs.length} inputs for ${tyName(inputTy)}, exceeding --cap ${cap}`);
    }
    return { name: m.name, init: initFn, step: stepFn, handled: handledFn, stateTy, inputTy, inputs };
  });
  // A law whose arguments are structured data the driver builds itself lists no enumerated rows.
  const cases = new Map(laws.map((l) => [l.name, noCorpus.includes(l.name) ? [] : corpus(l.params, cap)]));
  const base = { fns: lowered, laws, cases };
  const prog: Program = {
    ...base,
    machines,
    ...types,
    props: properties,
    propLen: propertyLength,
    natOps: collectNatOps(base),
  };
  const resolvedModelName = modelName ?? (target.kind === 'all' ? `${target.stem}.bend` : target.modelName);
  const manifest: EmitManifest = {
    modelName: resolvedModelName,
    bendRevision: BEND_REVISION,
    hashAlgorithm: 'blake3',
    laws: laws.map((l) => ({
      name: l.name,
      proven: l.proven,
      digest: l.digest,
      params: l.params.map((p) => ({ name: p.rawName, type: tyName(p.ty) })),
      hypsCount: l.hyps.length,
      corpusCount: (cases.get(l.name) ?? []).length,
      propsCount: properties,
      reachedFns: lawReachedFns(l, loweredMap),
    })),
    machines: machines.map((m) => ({
      name: m.name,
      init: m.init.name,
      step: m.step.name,
      stateType: tyName(m.stateTy),
      inputType: tyName(m.inputTy),
      handled: m.handled?.name ?? null,
      inputCount: m.inputs.length,
    })),
  };
  const counts = { functions: order.length, laws: laws.length, manifest };
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
// runs the drivers, and reports by law: pass counts per target, vacuity counts,
// and the first case where the targets disagree or a law fails, with its counterexample.
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
const allowVacuous = process.argv.includes("--allow-vacuous");
const extra = allowVacuous ? ["--allow-vacuous"] : [];
if (build) {
  const rs = sh("rustc", ["-C", "opt-level=1", "--edition", "2021", "${stem}.rs", "-o", "${stem}_rs"]);
  if (rs.code !== 0) { console.error(rs.err); process.exit(2); }
  const gs = sh(process.env.GOOSE ?? "goose", ["build", "${stem}.goose"]);
  if (gs.code !== 0) { console.error(gs.err + gs.out); process.exit(2); }
  const go = sh("go", ["build", "-o", "${stem}_go", "${stem}.go"]);
  if (go.code !== 0) { console.error(go.err); process.exit(2); }
}
const runs = {
  rust: sh("./${stem}_rs", extra),
  goose: sh("./${stem}", []),
  go: sh("./${stem}_go", extra),
  ts: sh("bun", ["${stem}.ts", ...extra]),
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
const isLawVacuous = (xs: Row[]): boolean => xs.length > 0 && xs.every((r) => r.ok === "true" && r.v === "vacuous");
let bad = 0;
for (const t of targets) {
  const anyVac = laws.some((l) => isLawVacuous(rows[t].filter((r) => r.law === l.name)));
  const vacFail = anyVac && (t === "goose" || !allowVacuous);
  const code = rows[t].some((r) => r.ok === "false") || vacFail ? 1 : 0;
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
  const vacuous = ts.filter((r) => r.ok === "true" && r.v === "vacuous").length;
  const tested = ts.length - vacuous;
  const counts = per.map((xs, i) => targets[i] + " " + xs.filter((r) => r.ok === "true").length + "/" + xs.length);
  const vacFail = !allowVacuous && ts.length > 0 && tested === 0;
  const verdict = disagree < 0 && !fail && !vacFail ? "PASS" : vacFail && disagree < 0 && !fail ? "VACUOUS" : "FAIL";
  if (verdict !== "PASS") bad++;
  console.log(verdict + " " + law + " [" + (proven ? "proven" : "open") + "]: " + counts.join("  ") + "  tested=" + tested + " vacuous=" + vacuous + "  targets " + (disagree < 0 ? "agree" : "DISAGREE at row " + disagree));
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

function isBendErr(err: unknown): err is Bend.Err {
  return (
    typeof err === 'object' &&
    err !== null &&
    '$' in err &&
    err.$ === 'Err' &&
    'bok' in err &&
    'exp' in err &&
    'ctx' in err
  );
}

/** Load a model and its imports, and accept it only after the bundled checker validates the book. */
export async function checkBendFile(file: string): Promise<CheckedBendBook> {
  const book = Bend.book_nil();
  await Bend.book_load(book, file, '', new Map());
  try {
    Bend.book_valid(book, 0);
  } catch (err) {
    if (isBendErr(err)) {
      throw new Error(Bend.err_show(err));
    }
    throw err;
  }
  return {
    emit(options) {
      // The original emitter's synchronous scratch belongs to this call, never to asynchronous file loading.
      BOOK = book;
      LIST_LEN = options.listLength ?? 3;
      ADT_DEPTH = options.adtDepth ?? 2;
      try {
        return emitBook(options);
      } finally {
        BOOK = EMPTY_BOOK;
        fresh = 0;
        ADT_DEPTH = 2;
        RECURSIVE_ADTS = new Set();
        SIGS.clear();
        SPECIALIZED.clear();
        USED.clear();
        USED_BASE_NAT.clear();
      }
    },
    skeleton(options) {
      return generateSkeletons(book, options);
    },
  };
}
