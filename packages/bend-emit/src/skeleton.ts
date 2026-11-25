import * as Bend from '../vendor/bend2/bend.js';

type H = Bend.HTerm;
type L = Bend.LTerm;

export interface SkeletonOptions {
  /** Named laws or proof lemmas to generate skeletons for. When omitted, selects every eligible proof def. */
  readonly laws?: readonly string[];
}

export interface SkeletonLeafCounts {
  readonly total: number;
  readonly refl: number;
  readonly recursive: number;
  readonly absurd: number;
  readonly assumption: number;
  readonly holes: number;
}

export interface SkeletonDef {
  readonly name: string;
  readonly kind: 'law' | 'lemma';
  readonly source: string;
  readonly leaves: SkeletonLeafCounts;
}

export interface SkeletonResult {
  readonly defs: readonly SkeletonDef[];
  readonly skipped: readonly string[];
  readonly source: string;
}

// ---- hole & proof-status inspection ----------------------------------------

function ltermHasHole(book: Bend.Book, term: L, visited: Set<string>): boolean {
  switch (term.$) {
    case 'Hol':
      return true;
    case 'Ann':
      // Types are dead; only the proof term itself can introduce a live hole.
      return ltermHasHole(book, term.x, visited);
    case 'Ref': {
      const k = term.k;
      const tld = book.tlds[k];
      if (tld === undefined || tld.$ !== 'Def' || tld.b === true || tld.i !== undefined) {
        return false;
      }
      if (tld.v === null) return true;
      if (visited.has(k)) return false;
      visited.add(k);
      const body = tld.e ?? Bend.term_lower(tld.v, 0);
      return ltermHasHole(book, body, visited);
    }
    case 'Lam':
      return ltermHasHole(book, term.f, visited);
    case 'Let':
      return term.v.some((v) => ltermHasHole(book, v, visited)) || ltermHasHole(book, term.f, visited);
    case 'App':
      return ltermHasHole(book, term.f, visited) || ltermHasHole(book, term.x, visited);
    case 'Ctr':
      return term.x.some((x) => ltermHasHole(book, x, visited));
    case 'Mat':
      return ltermHasHole(book, term.h, visited) || ltermHasHole(book, term.m, visited);
    case 'Rwt':
      // Motive `p` is dead; evidence `e` and continuation `f` are live.
      return ltermHasHole(book, term.e, visited) || ltermHasHole(book, term.f, visited);
    case 'Sub':
      return ltermHasHole(book, term.f, visited);
    case 'Var':
    case 'Typ':
    case 'Qnt':
    case 'Qua':
    case 'Min':
    case 'All':
    case 'ADT':
    case 'Lit':
    case 'Efq':
    case 'Eql':
    case 'Rfl':
      return false;
  }
}

/**
 * Returns true iff `k` is a defined `Def` whose checked proof body contains no `Hol`
 * directly or in any user proof def it reaches.
 */
export function defIsProven(book: Bend.Book, k: string): boolean {
  const t = book.tlds[k];
  if (t === undefined || t.$ !== 'Def' || t.v === null) return false;
  const visited = new Set<string>([k]);
  const body = t.e ?? Bend.term_lower(t.v, 0);
  return !ltermHasHole(book, body, visited);
}

/** Returns true iff `v` is a lambda spine whose body is a single hole (`?TODO` or `{?TODO : T}`). */
function isSingleHoleBody(v: H): boolean {
  let cur = Bend.term_force(v);
  let d = 0;
  while (cur.$ === 'Lam') {
    cur = Bend.term_force(cur.f(Bend.Var(cur.k, d++)));
  }
  while (cur.$ === 'Ann') {
    cur = Bend.term_force(cur.x);
  }
  return cur.$ === 'Hol';
}

function isUnitOrEmpty(t0: H, want: 'Unit' | 'Empty'): boolean {
  const t = Bend.term_force(t0);
  return (t.$ === 'ADT' || t.$ === 'Ref') && t.k === want;
}

function asIsTrueTerm(book: Bend.Book, t0: H): H | null {
  const t = Bend.term_force(t0);
  if (t.$ !== 'App') return null;
  const [head, args] = Bend.term_unapply(t);
  const h = Bend.term_force(head);
  if (h.$ !== 'Ref' || args.length !== 1 || args[0] === undefined) return null;
  const def = book.tlds[h.k];
  if (def === undefined || def.$ !== 'Def') return null;
  const bare = h.k.includes(':') ? h.k.slice(h.k.indexOf(':') + 1) : h.k;
  if (def.v !== null) {
    const onTrue = Bend.term_wnf(book, Bend.App(Bend.Ref(h.k), Bend.Ctr('True', [])));
    const onFalse = Bend.term_wnf(book, Bend.App(Bend.Ref(h.k), Bend.Ctr('False', [])));
    if (isUnitOrEmpty(onTrue, 'Unit') && isUnitOrEmpty(onFalse, 'Empty')) return args[0];
    return null;
  }
  if (bare === 'IsTrue') return args[0];
  return null;
}

function isProofDef(book: Bend.Book, t: Bend.Def): boolean {
  if (t.b === true || t.i !== undefined) return false;
  let cur = Bend.term_force(t.T);
  let d = 0;
  while (cur.$ === 'All') {
    cur = Bend.term_force(cur.B(Bend.Var(cur.k, d++)));
  }
  const forced = Bend.term_force(cur);
  if (forced.$ === 'Typ') return false;
  if (t.l === true) return true;
  if (forced.$ === 'Eql' || asIsTrueTerm(book, forced) !== null) return true;
  const wnf = Bend.term_wnf(book, forced);
  return wnf.$ === 'Eql' || isUnitOrEmpty(wnf, 'Unit') || isUnitOrEmpty(wnf, 'Empty');
}

// ---- dependency collection & topological ordering --------------------------

function collectTermRefs(term: L, out: Set<string>): void {
  switch (term.$) {
    case 'Ref':
      out.add(term.k);
      return;
    case 'Typ':
      collectTermRefs(term.g, out);
      return;
    case 'Min':
      collectTermRefs(term.a, out);
      collectTermRefs(term.b, out);
      return;
    case 'All':
      collectTermRefs(term.A, out);
      collectTermRefs(term.B, out);
      return;
    case 'Lam':
      collectTermRefs(term.f, out);
      return;
    case 'Let':
      for (const v of term.v) collectTermRefs(v, out);
      collectTermRefs(term.f, out);
      return;
    case 'App':
      collectTermRefs(term.f, out);
      collectTermRefs(term.x, out);
      return;
    case 'ADT':
      for (const x of term.x) collectTermRefs(x, out);
      return;
    case 'Ctr':
      for (const x of term.x) collectTermRefs(x, out);
      return;
    case 'Mat':
      collectTermRefs(term.h, out);
      collectTermRefs(term.m, out);
      return;
    case 'Eql':
      collectTermRefs(term.a, out);
      collectTermRefs(term.b, out);
      collectTermRefs(term.T, out);
      return;
    case 'Rwt':
      collectTermRefs(term.e, out);
      collectTermRefs(term.p, out);
      collectTermRefs(term.f, out);
      return;
    case 'Ann':
      collectTermRefs(term.x, out);
      collectTermRefs(term.T, out);
      return;
    case 'Sub':
      collectTermRefs(term.f, out);
      return;
    case 'Var':
    case 'Qnt':
    case 'Qua':
    case 'Lit':
    case 'Efq':
    case 'Rfl':
    case 'Hol':
      return;
  }
}

function topoOrderDefs(
  book: Bend.Book,
  selected: readonly string[],
  generatedBodies: ReadonlyMap<string, L>,
): string[] {
  const selectedSet = new Set(selected);
  // Book declaration order as deterministic tie-breaker
  const declRank = new Map<string, number>();
  for (let i = 0; i < book.order.length; i++) {
    const k = book.order[i];
    if (k !== undefined && !declRank.has(k)) declRank.set(k, i);
  }
  const baseOrder = [...selected].sort((a, b) => (declRank.get(a) ?? 0) - (declRank.get(b) ?? 0));

  const deps = new Map<string, string[]>();
  for (const k of baseOrder) {
    const refs = new Set<string>();
    const tld = book.tlds[k];
    if (tld !== undefined && tld.$ === 'Def') {
      collectTermRefs(Bend.term_lower(tld.T, 0), refs);
    }
    const gen = generatedBodies.get(k);
    if (gen !== undefined) collectTermRefs(gen, refs);
    const direct = [...refs]
      .filter((r) => r !== k && selectedSet.has(r))
      .sort((a, b) => (declRank.get(a) ?? 0) - (declRank.get(b) ?? 0));
    deps.set(k, direct);
  }

  const out: string[] = [];
  const state = new Map<string, 'visiting' | 'done'>();
  const visit = (k: string, stack: string[]): void => {
    const st = state.get(k);
    if (st === 'done') return;
    if (st === 'visiting') {
      const cycle = [...stack.slice(stack.indexOf(k)), k].join(' -> ');
      throw new Error(`bend-emit refuses: mutual dependency cycle among proof defs: ${cycle}`);
    }
    state.set(k, 'visiting');
    stack.push(k);
    for (const d of deps.get(k) ?? []) {
      visit(d, stack);
    }
    stack.pop();
    state.set(k, 'done');
    out.push(k);
  };
  for (const k of baseOrder) visit(k, []);
  return out;
}

// ---- skeleton AST ----------------------------------------------------------

type LeafKind = 'refl' | 'recursive' | 'absurd' | 'assumption' | 'hole';

type SkelNode =
  | { readonly k: 'leaf'; readonly kind: LeafKind; readonly code: string }
  | {
      readonly k: 'match';
      readonly scrutName: string;
      readonly adtName: string;
      readonly arms: readonly SkelArm[];
    };

interface SkelArm {
  readonly ctorName: string;
  readonly fieldNames: readonly string[];
  readonly body: SkelNode;
}

interface OpenVar {
  readonly name: string;
  readonly depth: number;
  readonly quant: Bend.Quant;
  readonly ty: H;
  readonly recDepth: number;
}

interface CtxEntry {
  readonly name: string;
  readonly depth: number;
  readonly quant: Bend.Quant;
  readonly ty: H;
}

interface OrigParam {
  readonly name: string;
  readonly depth: number;
  readonly quant: Bend.Quant;
}

interface BuildEnv {
  readonly book: Bend.Book;
  readonly defName: string;
  readonly def: Bend.Def;
  readonly origParams: readonly OrigParam[];
  readonly lhsQs: readonly Bend.Quant[];
  readonly templateGoal: H;
  readonly recAdtCache: Map<string, boolean>;
  nodeBudget: number;
}

// ---- helpers for names, types, and printing --------------------------------

function freshLocalName(base: string, usedNames: ReadonlySet<string>, book: Bend.Book): string {
  const clean = base.replace(/[^A-Za-z0-9_]/g, '_') || 'x';
  const collides = (candidate: string): boolean =>
    usedNames.has(candidate) ||
    candidate in book.tlds ||
    candidate in book.ctrs ||
    candidate === 'match' ||
    candidate === 'case' ||
    candidate === 'def' ||
    candidate === 'law' ||
    candidate === 'type' ||
    candidate === 'for' ||
    candidate === 'import';
  if (!collides(clean)) return clean;
  for (let i = 0; ; i++) {
    const candidate = `${clean}${i}`;
    if (!collides(candidate)) return candidate;
  }
}

function showHTerm(book: Bend.Book, term: H, bnd: readonly string[], useSnf: boolean): string {
  const lowered = Bend.term_lower(useSnf ? Bend.term_snf(book, term) : term, bnd.length);
  return Bend.term_show(lowered, -1, [...bnd]);
}

function showGoalType(book: Bend.Book, goal: H, bnd: readonly string[]): string {
  const forced = Bend.term_force(goal);
  const wnf = Bend.term_wnf(book, forced);
  if (wnf.$ === 'Eql') {
    const a = showHTerm(book, wnf.a, bnd, true);
    const b = showHTerm(book, wnf.b, bnd, true);
    const T = showHTerm(book, wnf.T, bnd, false);
    return `{${a} == ${b} : ${T}}`;
  }
  const pred = asIsTrueTerm(book, forced);
  if (pred !== null) {
    const [head] = Bend.term_unapply(forced);
    const headName = head.$ === 'Ref' ? Bend.name_key(head.k) : showHTerm(book, head, bnd, false);
    return `${headName}(${showHTerm(book, pred, bnd, true)})`;
  }
  return showHTerm(book, forced, bnd, false);
}

// ---- blocking scrutinee analysis -------------------------------------------

function adtFamilyKey(book: Bend.Book, ty: H): string | null {
  const w = Bend.term_wnf(book, ty);
  return w.$ === 'ADT' ? w.k : null;
}

/**
 * Returns true iff the ADT family `adtKey` is self- or mutually-recursive
 * (its constructor fields reach `adtKey` again).
 */
function isRecursiveAdt(book: Bend.Book, adtKey: string, cache: Map<string, boolean>): boolean {
  const cached = cache.get(adtKey);
  if (cached !== undefined) return cached;
  const visited = new Set<string>();
  const stack: string[] = [adtKey];
  let result = false;
  while (stack.length > 0 && !result) {
    const cur = stack.pop();
    if (cur === undefined) continue;
    const tld = book.tlds[cur];
    if (tld === undefined || tld.$ !== 'ADT') continue;
    for (const ctr of tld.c) {
      let tel = Bend.term_force(ctr.T);
      for (let i = 0; i < tld.n && tel.$ === 'All'; i++) {
        tel = Bend.term_force(tel.B(Bend.Var('_', i)));
      }
      let d = tld.n;
      while (tel.$ === 'All') {
        const refs = new Set<string>();
        collectTermRefs(Bend.term_lower(tel.A, d), refs);
        for (const r of refs) {
          if (r === adtKey) {
            result = true;
            break;
          }
          if (!visited.has(r) && book.tlds[r]?.$ === 'ADT') {
            visited.add(r);
            stack.push(r);
          }
        }
        if (result) break;
        const collectAdts = (lt: L): void => {
          switch (lt.$) {
            case 'ADT':
              if (lt.k === adtKey) {
                result = true;
              } else if (!visited.has(lt.k) && book.tlds[lt.k]?.$ === 'ADT') {
                visited.add(lt.k);
                stack.push(lt.k);
              }
              for (const x of lt.x) collectAdts(x);
              return;
            case 'All':
              collectAdts(lt.A);
              collectAdts(lt.B);
              return;
            case 'App':
              collectAdts(lt.f);
              collectAdts(lt.x);
              return;
            case 'Lam':
              collectAdts(lt.f);
              return;
            default:
              return;
          }
        };
        collectAdts(Bend.term_lower(tel.A, d));
        if (result) break;
        tel = Bend.term_force(tel.B(Bend.Var(tel.k, d++)));
      }
      if (result) break;
    }
  }
  cache.set(adtKey, result);
  return result;
}

/**
 * Inspects a WNF-reduced term to find open-variable scrutinees blocking computation
 * of defined functions in `book`. Maps blocking depth -> whether found inside a stepped-into arm.
 */
function collectBlockingOpenVars(
  book: Bend.Book,
  term0: H,
  openByDepth: ReadonlyMap<number, OpenVar>,
  hasSplitRec: boolean,
  recAdtCache: Map<string, boolean>,
  out: Map<number, boolean>,
  visitingFns: Set<string>,
  fuel = 32,
): void {
  if (fuel <= 0) return;
  const t = Bend.term_wnf(book, term0);
  switch (t.$) {
    case 'Ctr': {
      for (const x of t.x) {
        collectBlockingOpenVars(book, x, openByDepth, hasSplitRec, recAdtCache, out, visitingFns, fuel - 1);
      }
      return;
    }
    case 'App':
    case 'Ref': {
      const [head, args] = Bend.term_unapply(t);
      const h = Bend.term_force(head);
      if (h.$ === 'Ref') {
        const tld = book.tlds[h.k];
        if (tld !== undefined && tld.$ === 'Def' && tld.v !== null && !visitingFns.has(h.k)) {
          const nextVisiting = new Set(visitingFns);
          nextVisiting.add(h.k);
          traceDefMat(
            book,
            h.k,
            tld.v,
            args,
            false,
            openByDepth,
            hasSplitRec,
            recAdtCache,
            out,
            nextVisiting,
            fuel - 1,
          );
          return;
        }
      }
      for (const a of args) {
        collectBlockingOpenVars(book, a, openByDepth, hasSplitRec, recAdtCache, out, new Set<string>(), fuel - 1);
      }
      return;
    }
    case 'Eql': {
      collectBlockingOpenVars(book, t.a, openByDepth, hasSplitRec, recAdtCache, out, visitingFns, fuel - 1);
      collectBlockingOpenVars(book, t.b, openByDepth, hasSplitRec, recAdtCache, out, visitingFns, fuel - 1);
      return;
    }
    case 'Ann': {
      collectBlockingOpenVars(book, t.x, openByDepth, hasSplitRec, recAdtCache, out, visitingFns, fuel - 1);
      return;
    }
    default:
      return;
  }
}

function litToCtr(lit: Extract<H, { $: 'Lit' }>): { k: string; x: H[] } | null {
  if (lit.k === 'Nat') {
    const n = lit.v;
    if (n === 0) return { k: 'Zero', x: [] };
    return { k: 'Succ', x: [Bend.Lit('Nat', n - 1)] };
  }
  return null;
}

function traceDefMat(
  book: Bend.Book,
  fnName: string,
  body0: H,
  args: readonly H[],
  steppedIntoArm: boolean,
  openByDepth: ReadonlyMap<number, OpenVar>,
  hasSplitRec: boolean,
  recAdtCache: Map<string, boolean>,
  out: Map<number, boolean>,
  visitingFns: Set<string>,
  fuel: number,
): void {
  if (fuel <= 0) return;
  let body = Bend.term_force(body0);
  let idx = 0;
  while (body.$ === 'Lam' && idx < args.length) {
    const arg = args[idx];
    if (arg === undefined) break;
    body = Bend.term_force(body.f(arg));
    idx++;
  }
  const restArgs = args.slice(idx);
  if (body.$ === 'Mat' && restArgs.length > 0) {
    const scrutArg = restArgs[0];
    if (scrutArg === undefined) return;
    const scrut = Bend.term_wnf(book, scrutArg);
    if (scrut.$ === 'Var') {
      const ov = openByDepth.get(scrut.i);
      if (ov !== undefined && ov.quant.$ !== 'None') {
        const fam = adtFamilyKey(book, ov.ty);
        if (fam !== null) {
          const rec = isRecursiveAdt(book, fam, recAdtCache);
          const allowed = rec
            ? (ov.recDepth === 0 && !hasSplitRec) || (steppedIntoArm && ov.recDepth < 3)
            : ov.recDepth === 0;
          if (allowed) {
            out.set(ov.depth, (out.get(ov.depth) ?? false) || steppedIntoArm);
          }
        }
      }
      for (const extra of restArgs.slice(1)) {
        collectBlockingOpenVars(book, extra, openByDepth, hasSplitRec, recAdtCache, out, visitingFns, fuel - 1);
      }
      return;
    }
    if (scrut.$ === 'Ctr' || scrut.$ === 'Lit') {
      const ctr = scrut.$ === 'Ctr' ? { k: scrut.k, x: scrut.x } : litToCtr(scrut);
      if (ctr === null) return;
      let cur: H = body;
      while (cur.$ === 'Mat') {
        if (cur.k === ctr.k) {
          traceDefMat(
            book,
            fnName,
            cur.h,
            [...ctr.x, ...restArgs.slice(1)],
            true,
            openByDepth,
            hasSplitRec,
            recAdtCache,
            out,
            visitingFns,
            fuel - 1,
          );
          return;
        }
        cur = Bend.term_force(cur.m);
      }
      if (cur.$ !== 'Efq') {
        traceDefMat(
          book,
          fnName,
          cur,
          restArgs,
          true,
          openByDepth,
          hasSplitRec,
          recAdtCache,
          out,
          visitingFns,
          fuel - 1,
        );
      }
      return;
    }
    // Computed scrutinee is a strictly smaller subterm: reset visitingFns so nested calls
    // like `not(not(b))` or `run(ys, run(xs, s))` are chased; also check remaining args.
    collectBlockingOpenVars(book, scrut, openByDepth, hasSplitRec, recAdtCache, out, new Set<string>(), fuel - 1);
    for (const extra of restArgs.slice(1)) {
      collectBlockingOpenVars(book, extra, openByDepth, hasSplitRec, recAdtCache, out, visitingFns, fuel - 1);
    }
    return;
  }
  if (body.$ === 'Efq') return;
  // Reached a non-Mat body expression: apply any remaining args and inspect it.
  let app: H = body;
  for (const a of restArgs) app = Bend.App(app, a);
  collectBlockingOpenVars(book, app, openByDepth, hasSplitRec, recAdtCache, out, visitingFns, fuel - 1);
}

// ---- recursive call synthesis ----------------------------------------------

/**
 * Unifies a goal template (containing original parameter Vars with depths 0..n-1)
 * against the current branch's WNF-reduced goal to discover candidate recursive arguments.
 */
function matchGoalTemplate(
  book: Bend.Book,
  tmpl0: H,
  cur0: H,
  origDepths: ReadonlySet<number>,
  subst: Map<number, H>,
): boolean {
  const tmpl = Bend.term_wnf(book, tmpl0);
  const cur = Bend.term_wnf(book, cur0);
  if (tmpl.$ === 'Var' && origDepths.has(tmpl.i)) {
    const prev = subst.get(tmpl.i);
    if (prev === undefined) {
      subst.set(tmpl.i, cur);
      return true;
    }
    return Bend.term_compare('EQ', book, prev, cur, 1000);
  }
  if (tmpl.$ === 'Var') {
    return cur.$ === 'Var' && tmpl.i === cur.i;
  }
  if (tmpl.$ === 'Ref') {
    return cur.$ === 'Ref' && tmpl.k === cur.k;
  }
  if (tmpl.$ === 'Lit') {
    if (cur.$ === 'Lit') return tmpl.k === cur.k && tmpl.v === cur.v;
    const tc = litToCtr(tmpl);
    if (tc !== null && cur.$ === 'Ctr' && cur.k === tc.k && cur.x.length === tc.x.length) {
      return tc.x.every((x, i) => {
        const cx = cur.x[i];
        return cx !== undefined && matchGoalTemplate(book, x, cx, origDepths, subst);
      });
    }
    return false;
  }
  if (tmpl.$ === 'Ctr') {
    if (cur.$ === 'Lit') {
      const cc = litToCtr(cur);
      if (cc !== null && cc.k === tmpl.k && cc.x.length === tmpl.x.length) {
        return tmpl.x.every((x, i) => {
          const cx = cc.x[i];
          return cx !== undefined && matchGoalTemplate(book, x, cx, origDepths, subst);
        });
      }
      return false;
    }
    if (cur.$ !== 'Ctr' || cur.k !== tmpl.k || cur.x.length !== tmpl.x.length) return false;
    return tmpl.x.every((x, i) => {
      const cx = cur.x[i];
      return cx !== undefined && matchGoalTemplate(book, x, cx, origDepths, subst);
    });
  }
  if (tmpl.$ === 'App') {
    if (cur.$ !== 'App') return false;
    return (
      matchGoalTemplate(book, tmpl.f, cur.f, origDepths, subst) &&
      matchGoalTemplate(book, tmpl.x, cur.x, origDepths, subst)
    );
  }
  if (tmpl.$ === 'Eql') {
    if (cur.$ !== 'Eql') return false;
    return (
      matchGoalTemplate(book, tmpl.a, cur.a, origDepths, subst) &&
      matchGoalTemplate(book, tmpl.b, cur.b, origDepths, subst)
    );
  }
  if (tmpl.$ === 'ADT') {
    if (cur.$ !== 'ADT' || cur.k !== tmpl.k || cur.x.length !== tmpl.x.length) return false;
    return tmpl.x.every((x, i) => {
      const cx = cur.x[i];
      return cx !== undefined && matchGoalTemplate(book, x, cx, origDepths, subst);
    });
  }
  return false;
}

function stripCells(t0: H): H {
  const t = Bend.term_force(t0);
  switch (t.$) {
    case 'Ctr':
      return Bend.Ctr(t.k, t.x.map(stripCells));
    case 'App':
      return Bend.App(stripCells(t.f), stripCells(t.x));
    case 'Ann':
      return stripCells(t.x);
    default:
      return t;
  }
}

function tryBuildRecursiveCall(
  env: BuildEnv,
  goal: H,
  lhsT: H,
  allEntries: readonly CtxEntry[],
  bnd: readonly string[],
  depth: number,
): string | null {
  const origDepths = new Set(env.origParams.map((p) => p.depth));
  const subst = new Map<number, H>();
  if (!matchGoalTemplate(env.book, env.templateGoal, goal, origDepths, subst)) {
    return null;
  }

  // Instantiate def.T left-to-right to fill arguments (including hypothesis parameters from ctx).
  const liveEntries = allEntries.filter((e) => e.quant.$ !== 'None');
  const args: H[] = [];
  let tel = Bend.term_force(env.def.T);
  for (let i = 0; i < env.origParams.length; i++) {
    const param = env.origParams[i];
    if (param === undefined || tel.$ !== 'All') return null;
    let arg = subst.get(param.depth);
    if (arg === undefined) {
      // Try an exact type match from live context entries (handles hypothesis parameters)
      const wantTy = tel.A;
      const found = liveEntries.find((e) => Bend.term_compare('LE', env.book, e.ty, wantTy, depth));
      if (found !== undefined) {
        arg = Bend.Var(found.name, found.depth);
      } else {
        // Check if original param is still live and unchanged in scope
        const sameDepth = allEntries.find((e) => e.depth === param.depth);
        if (sameDepth !== undefined && Bend.term_compare('LE', env.book, sameDepth.ty, wantTy, depth)) {
          arg = Bend.Var(sameDepth.name, sameDepth.depth);
        } else {
          return null;
        }
      }
    }
    const cleanArg = stripCells(arg);
    args.push(cleanArg);
    tel = Bend.term_force(tel.B(cleanArg));
  }

  // Verify the instantiated return type fits the current goal!
  if (!Bend.term_compare('LE', env.book, tel, goal, depth)) {
    return null;
  }

  // Verify Bend2's left-to-right structural descent rule against lhs.t!
  const cols = Bend.term_unapply(stripCells(lhsT))[1];
  let ord: 'LT' | 'EQ' | 'GT' = 'EQ';
  for (let j = 0; j < cols.length && j < args.length && ord === 'EQ'; j++) {
    const q = env.lhsQs[j];
    const arg = args[j];
    const col = cols[j];
    if (q === undefined || arg === undefined || col === undefined) return null;
    ord = Bend.term_descend(q, arg, col);
  }
  if (ord !== 'LT') return null;

  // Render the call using Bend2's own printer
  const renderedArgs = args.map((a) => showHTerm(env.book, a, bnd, true));
  return `${Bend.name_key(env.defName)}(${renderedArgs.join(', ')})`;
}

// ---- telescope probing & recursive tree builder ----------------------------

interface ProbedTel {
  readonly allEntries: readonly CtxEntry[];
  readonly goal: H;
  readonly fullLhsT: H;
  readonly bnd: readonly string[];
  readonly endDepth: number;
}

function probeTelescope(
  tel0: H,
  openVars: readonly OpenVar[],
  closedEntries: readonly CtxEntry[],
  lhsT0: H,
  lhsN0: number,
): ProbedTel {
  const allEntries: CtxEntry[] = [...closedEntries];
  const bnd: string[] = closedEntries.map((e) => e.name);
  let tel = Bend.term_force(tel0);
  let lhsT = lhsT0;
  let lhsN = lhsN0;
  let d = closedEntries.length;

  for (const ov of openVars) {
    if (tel.$ !== 'All') break;
    const x: H = Bend.Var(ov.name, d);
    allEntries.push({ name: ov.name, depth: d, quant: tel.q, ty: tel.A });
    bnd.push(ov.name);
    if (lhsN > 0) {
      lhsT = Bend.term_apply(lhsT, x);
      lhsN--;
    }
    tel = Bend.term_force(tel.B(x));
    d++;
  }

  return { allEntries, goal: tel, fullLhsT: lhsT, bnd, endDepth: d };
}

function buildSkeletonNode(
  env: BuildEnv,
  tel0: H,
  openVars: readonly OpenVar[],
  closedEntries: readonly CtxEntry[],
  usedNames: ReadonlySet<string>,
  lhsT0: H,
  lhsN0: number,
  splitDepth: number,
  hasSplitRec: boolean,
): SkelNode {
  const probed = probeTelescope(tel0, openVars, closedEntries, lhsT0, lhsN0);
  const goalForced = Bend.term_force(probed.goal);
  const goalWnf = Bend.term_wnf(env.book, goalForced);

  // 1. Check reflexivity / Unit{} discharge
  if (goalWnf.$ === 'Eql' && Bend.term_compare('EQ', env.book, goalWnf.a, goalWnf.b, probed.endDepth)) {
    return { k: 'leaf', kind: 'refl', code: '{==}' };
  }
  if (isUnitOrEmpty(goalWnf, 'Unit')) {
    return { k: 'leaf', kind: 'refl', code: 'Unit{}' };
  }

  // 2. Check for an Empty hypothesis in context (e.g. IsTrue(False{}))
  for (const entry of probed.allEntries) {
    if (entry.quant.$ === 'None') continue;
    const entryWnf = Bend.term_wnf(env.book, entry.ty);
    if (isUnitOrEmpty(entryWnf, 'Empty')) {
      const goalStr = showGoalType(env.book, goalForced, probed.bnd);
      return { k: 'leaf', kind: 'absurd', code: `Empty.absurd(${goalStr}, ${entry.name})` };
    }
  }

  // 3. Check for an exact assumption hypothesis in context
  for (const entry of probed.allEntries) {
    if (entry.quant.$ === 'None') continue;
    if (Bend.term_compare('LE', env.book, entry.ty, goalForced, probed.endDepth)) {
      return { k: 'leaf', kind: 'assumption', code: entry.name };
    }
  }

  // 4. Check for a direct structurally-decreasing recursive call
  const recCall = tryBuildRecursiveCall(
    env,
    goalForced,
    probed.fullLhsT,
    probed.allEntries,
    probed.bnd,
    probed.endDepth,
  );
  if (recCall !== null) {
    return { k: 'leaf', kind: 'recursive', code: recCall };
  }

  // 5. Find blocking open variables in the goal (and if none, in hypotheses)
  if (splitDepth < 6 && env.nodeBudget > 0 && openVars.length > 0) {
    // Re-index openVars by their probed depth (closedEntries.length + j)
    const openByDepth = new Map<number, OpenVar>();
    const openIdxByDepth = new Map<number, number>();
    for (let j = 0; j < openVars.length; j++) {
      const ov = openVars[j];
      const entry = probed.allEntries[closedEntries.length + j];
      if (ov === undefined || entry === undefined) continue;
      openByDepth.set(entry.depth, { ...ov, depth: entry.depth, quant: entry.quant, ty: entry.ty });
      openIdxByDepth.set(entry.depth, j);
    }

    const blocking = new Map<number, boolean>();
    const visiting = new Set<string>();
    const pred = asIsTrueTerm(env.book, goalForced);
    if (pred !== null) {
      collectBlockingOpenVars(env.book, pred, openByDepth, hasSplitRec, env.recAdtCache, blocking, visiting);
    } else {
      collectBlockingOpenVars(env.book, goalWnf, openByDepth, hasSplitRec, env.recAdtCache, blocking, visiting);
    }

    if (blocking.size === 0) {
      for (const entry of probed.allEntries) {
        if (entry.quant.$ === 'None') continue;
        const hPred = asIsTrueTerm(env.book, entry.ty);
        if (hPred !== null) {
          collectBlockingOpenVars(env.book, hPred, openByDepth, hasSplitRec, env.recAdtCache, blocking, visiting);
        } else {
          const hWnf = Bend.term_wnf(env.book, entry.ty);
          if (hWnf.$ === 'Eql') {
            collectBlockingOpenVars(env.book, hWnf, openByDepth, hasSplitRec, env.recAdtCache, blocking, visiting);
          }
        }
      }
    }

    if (blocking.size > 0) {
      // Pick the earliest blocking binder in `openVars` order (required by Bend2's match_flatten)
      let bestIdx = -1;
      let bestStepped = false;
      for (const [d, stepped] of blocking) {
        const idx = openIdxByDepth.get(d);
        if (idx !== undefined && (bestIdx < 0 || idx < bestIdx)) {
          bestIdx = idx;
          bestStepped = stepped;
        }
      }

      if (bestIdx >= 0) {
        // Advance telescope past openVars[0..bestIdx-1] (closing them as Lam)
        let curTel = Bend.term_force(tel0);
        let curLhsT = lhsT0;
        let curLhsN = lhsN0;
        const nextClosed: CtxEntry[] = [...closedEntries];
        let d = closedEntries.length;
        for (let m = 0; m < bestIdx; m++) {
          const ov = openVars[m];
          if (ov === undefined || curTel.$ !== 'All') break;
          const x: H = Bend.Var(ov.name, d);
          nextClosed.push({ name: ov.name, depth: d, quant: curTel.q, ty: curTel.A });
          if (curLhsN > 0) {
            curLhsT = Bend.term_apply(curLhsT, x);
            curLhsN--;
          }
          curTel = Bend.term_force(curTel.B(x));
          d++;
        }

        const targetVar = openVars[bestIdx];
        if (targetVar !== undefined && curTel.$ === 'All') {
          const scrutTyWnf = Bend.term_wnf(env.book, curTel.A);
          if (scrutTyWnf.$ === 'ADT') {
            const adt = Bend.book_adt(env.book, scrutTyWnf, Bend.ctx_nil(), env.defName);
            if (adt.c.length > 0 && adt.c.length <= env.nodeBudget) {
              env.nodeBudget -= adt.c.length;
              const tailOpen = openVars.slice(bestIdx + 1);
              const scrutAll = curTel;
              const nextHasSplitRec = hasSplitRec || isRecursiveAdt(env.book, scrutTyWnf.k, env.recAdtCache);
              const arms: SkelArm[] = [];

              for (const ctr of adt.c) {
                const filledTel = Bend.tele_fill(env.book, ctr.T, scrutTyWnf.x, Bend.ctx_nil(), env.defName);
                const armUsed = new Set(usedNames);
                const fieldVars: OpenVar[] = [];
                const fieldNames: string[] = [];

                // Eagerly inspect constructor fields so `fieldVars` and `fieldNames` are complete
                // before descending into the arm (the HOAS telescope below is lazy).
                let scanTel = filledTel;
                for (let fi = 0; fi < ctr.n; fi++) {
                  const cAll = Bend.tele_head(env.book, scanTel, Bend.ctx_nil(), env.defName);
                  const cDem: Bend.Quant =
                    cAll.q.$ === 'None' ? Bend.None() : cAll.q.$ === 'Lone' ? scrutAll.q : Bend.Many();
                  const fname = freshLocalName(cAll.k, armUsed, env.book);
                  armUsed.add(fname);
                  fieldNames.push(fname);
                  const fAdt = adtFamilyKey(env.book, cAll.A);
                  const isRecField = fAdt !== null && fAdt === scrutTyWnf.k;
                  fieldVars.push({
                    name: fname,
                    depth: 0, // assigned during probeTelescope
                    quant: cDem,
                    ty: cAll.A,
                    recDepth: isRecField ? targetVar.recDepth + 1 : 0,
                  });
                  scanTel = cAll.B(Bend.Var(fname, nextClosed.length + fi));
                }

                // Pure HOAS arm telescope mirroring Bend2's `term_check_mat_goal`
                const buildArmTel = (ctel: H, fi: number, xs: H[]): H => {
                  if (fi === ctr.n) {
                    return scrutAll.B(Bend.Ctr(ctr.k, xs));
                  }
                  const cAll = Bend.tele_head(env.book, ctel, Bend.ctx_nil(), env.defName);
                  const fv = fieldVars[fi];
                  const cDem: Bend.Quant =
                    fv?.quant ?? (cAll.q.$ === 'None' ? Bend.None() : cAll.q.$ === 'Lone' ? scrutAll.q : Bend.Many());
                  const fname = fv?.name ?? cAll.k;
                  return Bend.All(cDem, fname, 0, cAll.A, (x: H) => buildArmTel(cAll.B(x), fi + 1, [...xs, x]));
                };

                const armTel = buildArmTel(filledTel, 0, []);
                const armLhsT = curLhsN > 0 ? Bend.lhs_ext(curLhsT, ctr.k, ctr.n) : curLhsT;
                const armLhsN = curLhsN > 0 ? curLhsN - 1 + ctr.n : curLhsN;
                const armOpenVars: OpenVar[] = [...fieldVars, ...tailOpen];

                const armBody = buildSkeletonNode(
                  env,
                  armTel,
                  armOpenVars,
                  nextClosed,
                  armUsed,
                  armLhsT,
                  armLhsN,
                  splitDepth + 1,
                  nextHasSplitRec,
                );
                arms.push({ ctorName: ctr.k, fieldNames, body: armBody });
              }

              const matchNode: SkelNode = {
                k: 'match',
                scrutName: targetVar.name,
                adtName: scrutTyWnf.k,
                arms,
              };
              // Prune secondary multi-way helper splits where every single arm is an unsolved hole
              // (zero closed leaves), while keeping direct subject-arm splits (e.g. `match a b:`).
              if (splitDepth >= 1 && !bestStepped && arms.length > 1) {
                const counts = countLeaves(matchNode);
                if (counts.holes === counts.total) {
                  const goalStr = showGoalType(env.book, goalForced, probed.bnd);
                  return { k: 'leaf', kind: 'hole', code: `{?TODO : ${goalStr}}` };
                }
              }
              return matchNode;
            }
          }
        }
      }
    }
  }

  // 6. Unsolved leaf: emit typed hole naming the remaining goal
  const goalStr = showGoalType(env.book, goalForced, probed.bnd);
  return { k: 'leaf', kind: 'hole', code: `{?TODO : ${goalStr}}` };
}

// ---- rendering (with multi-scrutinee coalescing & Nat sugar) ---------------

function formatPattern(adtName: string, ctorName: string, fieldNames: readonly string[]): string {
  const bare = Bend.name_key(ctorName);
  if (adtName === 'Nat') {
    if (bare === 'Zero' && fieldNames.length === 0) return '0n';
    if (bare === 'Succ' && fieldNames.length === 1 && fieldNames[0] !== undefined) {
      return `1n+${fieldNames[0]}`;
    }
  }
  return `${bare}{${fieldNames.join(', ')}}`;
}

function canCoalesceNextMatch(node: Extract<SkelNode, { k: 'match' }>): string | null {
  if (node.arms.length === 0) return null;
  let nextScrut: string | null = null;
  for (const arm of node.arms) {
    if (arm.body.k !== 'match') return null;
    if (arm.fieldNames.includes(arm.body.scrutName)) return null;
    if (nextScrut === null) {
      nextScrut = arm.body.scrutName;
    } else if (nextScrut !== arm.body.scrutName) {
      return null;
    }
  }
  return nextScrut;
}

function renderNode(node: SkelNode, indent: string): string[] {
  if (node.k === 'leaf') {
    return [`${indent}${node.code}`];
  }
  // Coalesce uniform multi-scrutinee matches (e.g. `match a b:` or `match h r:`)
  const scruts: string[] = [node.scrutName];
  type Row = { patterns: string[]; body: SkelNode };
  let rows: Row[] = node.arms.map((a) => ({
    patterns: [formatPattern(node.adtName, a.ctorName, a.fieldNames)],
    body: a.body,
  }));

  let probe: Extract<SkelNode, { k: 'match' }> = node;
  for (;;) {
    const nextScrut = canCoalesceNextMatch(probe);
    if (nextScrut === null) break;
    // Also verify all current rows have a match on `nextScrut`
    const allMatch = rows.every((r) => r.body.k === 'match' && r.body.scrutName === nextScrut);
    if (!allMatch) break;
    scruts.push(nextScrut);
    const expanded: Row[] = [];
    for (const r of rows) {
      if (r.body.k !== 'match') continue;
      for (const innerArm of r.body.arms) {
        expanded.push({
          patterns: [...r.patterns, formatPattern(r.body.adtName, innerArm.ctorName, innerArm.fieldNames)],
          body: innerArm.body,
        });
      }
    }
    rows = expanded;
    const first = rows[0]?.body;
    if (first === undefined || first.k !== 'match') break;
    probe = first;
  }

  const lines: string[] = [`${indent}match ${scruts.join(' ')}:`];
  for (const r of rows) {
    lines.push(`${indent}  case ${r.patterns.join(' ')}:`);
    lines.push(...renderNode(r.body, `${indent}    `));
  }
  return lines;
}

function countLeaves(node: SkelNode): SkeletonLeafCounts {
  if (node.k === 'leaf') {
    return {
      total: 1,
      refl: node.kind === 'refl' ? 1 : 0,
      recursive: node.kind === 'recursive' ? 1 : 0,
      absurd: node.kind === 'absurd' ? 1 : 0,
      assumption: node.kind === 'assumption' ? 1 : 0,
      holes: node.kind === 'hole' ? 1 : 0,
    };
  }
  let total = 0;
  let refl = 0;
  let recursive = 0;
  let absurd = 0;
  let assumption = 0;
  let holes = 0;
  for (const arm of node.arms) {
    const c = countLeaves(arm.body);
    total += c.total;
    refl += c.refl;
    recursive += c.recursive;
    absurd += c.absurd;
    assumption += c.assumption;
    holes += c.holes;
  }
  return { total, refl, recursive, absurd, assumption, holes };
}

function replaceTypedHolesWithBareTodo(node: SkelNode): SkelNode {
  if (node.k === 'leaf') {
    if (node.kind === 'hole') return { k: 'leaf', kind: 'hole', code: '?TODO' };
    return node;
  }
  return {
    ...node,
    arms: node.arms.map((a) => ({ ...a, body: replaceTypedHolesWithBareTodo(a.body) })),
  };
}

// ---- header rendering & def_check verification -----------------------------

function buildDefHeader(book: Bend.Book, name: string, def: Bend.Def, paramNames: readonly string[]): string {
  const displayName = Bend.name_key(name);
  if (def.l === true) {
    return `def ${displayName}(${paramNames.join(', ')}):`;
  }
  // Standalone lemma: render full typed signature from def.T
  const paramSpecs: string[] = [];
  const bnd: string[] = [];
  let cur = Bend.term_force(def.T);
  for (let i = 0; i < paramNames.length && cur.$ === 'All'; i++) {
    const pname = paramNames[i] ?? cur.k;
    const qStr = Bend.quant_show(cur.q);
    const tyStr = showHTerm(book, cur.A, bnd, false);
    paramSpecs.push(`${qStr}${pname}: ${tyStr}`);
    bnd.push(pname);
    cur = Bend.term_force(cur.B(Bend.Var(pname, i)));
  }
  const retStr = showHTerm(book, cur, bnd, false);
  return `def ${displayName}(${paramSpecs.join(', ')}) -> ${retStr}:`;
}

function parseAndCheckSingleDef(book: Bend.Book, defName: string, def: Bend.Def, source: string): L {
  // Clone tlds into a fresh map so we never mutate the caller's checked book and can
  // cleanly redefine a single-hole standalone lemma without prototype shadowing.
  const tlds: Record<string, Bend.TLD> = Object.create(null);
  for (const key in book.tlds) {
    const entry = book.tlds[key];
    if (entry === undefined) continue;
    if (key === defName) {
      if (def.l === true) {
        tlds[key] = { ...def, v: null };
      }
      continue;
    }
    tlds[key] = entry;
  }
  const scratch: Bend.Book = {
    tlds,
    ctrs: book.ctrs,
    order: [],
    tmps: book.tmps,
    hols: 0,
  };
  Bend.parse_book(scratch, '', source, def.m ?? '', {});
  const parsed = scratch.tlds[defName];
  if (parsed === undefined || parsed.$ !== 'Def' || parsed.v === null) {
    throw new Error(`bend-emit skeleton: failed to parse generated skeleton for ${defName}`);
  }
  return Bend.def_check(scratch, defName, parsed);
}

// ---- entry point -----------------------------------------------------------

function generateOneSkeleton(
  book: Bend.Book,
  defName: string,
  def: Bend.Def,
  recAdtCache: Map<string, boolean>,
): { def: SkeletonDef; checkedBody: L } {
  // Peel leading `All` parameters from def.T
  const origParams: OrigParam[] = [];
  const openVars: OpenVar[] = [];
  const lhsQs: Bend.Quant[] = [];
  const usedNames = new Set<string>();

  let tel = Bend.term_force(def.T);
  let d = 0;
  while (tel.$ === 'All') {
    const pname = freshLocalName(tel.k, usedNames, book);
    usedNames.add(pname);
    origParams.push({ name: pname, depth: d, quant: tel.q });
    openVars.push({ name: pname, depth: d, quant: tel.q, ty: tel.A, recDepth: 0 });
    lhsQs.push(tel.q);
    tel = Bend.term_force(tel.B(Bend.Var(pname, d)));
    d++;
  }
  const templateGoal = tel;

  const env: BuildEnv = {
    book,
    defName,
    def,
    origParams,
    lhsQs,
    templateGoal,
    recAdtCache,
    nodeBudget: 64,
  };

  const tree = buildSkeletonNode(env, def.T, openVars, [], usedNames, Bend.Ref(defName), origParams.length, 0, false);

  const paramNames = origParams.map((p) => p.name);
  const header = buildDefHeader(book, defName, def, paramNames);
  const bodyLines = renderNode(tree, '  ');
  let source = `${header}\n${bodyLines.join('\n')}`;
  let checkedBody: L;
  try {
    checkedBody = parseAndCheckSingleDef(book, defName, def, source);
  } catch {
    // Fallback if a printed goal annotation could not be re-parsed (e.g. exotic type syntax):
    // emit bare `?TODO` leaves so the skeleton is guaranteed to pass Bend2's checker.
    const fallbackTree = replaceTypedHolesWithBareTodo(tree);
    source = `${header}\n${renderNode(fallbackTree, '  ').join('\n')}`;
    checkedBody = parseAndCheckSingleDef(book, defName, def, source);
  }

  return {
    def: {
      name: Bend.name_key(defName),
      kind: def.l === true ? 'law' : 'lemma',
      source,
      leaves: countLeaves(tree),
    },
    checkedBody,
  };
}

function definedInRoot(tld: Bend.Def): boolean {
  return (tld.m ?? '') === '' && tld.b !== true;
}

/**
 * Generates proof skeletons for unfilled or single-hole laws and proof lemmas in a checked book.
 * Never overwrites an existing non-hole proof (returns such names in `skipped`).
 */
export function generateSkeletons(book: Bend.Book, options: SkeletonOptions = {}): SkeletonResult {
  const requested = options.laws;
  const candidateKeys: string[] = [];

  if (requested !== undefined && requested.length > 0) {
    const seen = new Set<string>();
    for (const rawName of requested) {
      if (seen.has(rawName)) continue;
      seen.add(rawName);
      const tld = book.tlds[rawName];
      if (tld === undefined || tld.$ !== 'Def') {
        throw new Error(`bend-emit refuses: ${rawName} is not a def or law in this book`);
      }
      if (!definedInRoot(tld)) {
        const origin = tld.b === true ? 'Base' : (tld.m ?? '');
        throw new Error(
          `bend-emit refuses: ${rawName} is defined in imported book ${origin}; run skeleton on that book`,
        );
      }
      if (!isProofDef(book, tld)) {
        throw new Error(`bend-emit refuses: ${rawName} is not a law or proof lemma`);
      }
      candidateKeys.push(rawName);
    }
  } else {
    const seen = new Set<string>();
    for (const k of book.order) {
      if (seen.has(k)) continue;
      seen.add(k);
      const tld = book.tlds[k];
      if (tld === undefined || tld.$ !== 'Def') continue;
      if (!definedInRoot(tld)) continue;
      if (isProofDef(book, tld)) candidateKeys.push(k);
    }
  }

  const eligible: string[] = [];
  const skipped: string[] = [];
  for (const k of candidateKeys) {
    const tld = book.tlds[k];
    if (tld === undefined || tld.$ !== 'Def') continue;
    if (tld.v === null || isSingleHoleBody(tld.v)) {
      eligible.push(k);
    } else {
      skipped.push(Bend.name_key(k));
    }
  }

  const recAdtCache = new Map<string, boolean>();
  const generatedMap = new Map<string, SkeletonDef>();
  const checkedBodies = new Map<string, L>();
  for (const k of eligible) {
    const tld = book.tlds[k];
    if (tld === undefined || tld.$ !== 'Def') continue;
    const { def, checkedBody } = generateOneSkeleton(book, k, tld, recAdtCache);
    generatedMap.set(k, def);
    checkedBodies.set(k, checkedBody);
  }

  const orderedKeys = topoOrderDefs(book, eligible, checkedBodies);
  const defs: SkeletonDef[] = [];
  for (const k of orderedKeys) {
    const d = generatedMap.get(k);
    if (d !== undefined) defs.push(d);
  }

  return {
    defs,
    skipped,
    source: defs.map((d) => d.source).join('\n\n'),
  };
}
