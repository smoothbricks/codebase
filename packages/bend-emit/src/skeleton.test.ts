import { expect, test } from 'bun:test';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import * as Bend from '../vendor/bend2/bend.js';
import { run as runCli } from './cli.js';
import { checkBendFile } from './emit.js';

function withTempDir<T>(fn: (dir: string) => Promise<T> | T): Promise<T> {
  const dir = mkdtempSync(join(tmpdir(), 'bend-emit-skeleton-test-'));
  return Promise.resolve(fn(dir)).finally(() => rmSync(dir, { recursive: true, force: true }));
}

async function checkBookFile(file: string): Promise<Bend.Book> {
  const book = Bend.book_nil();
  await Bend.book_load(book, file, '', new Map());
  Bend.book_valid(book, 0);
  return book;
}

async function captureStdout(fn: () => Promise<void>): Promise<string> {
  const chunks: string[] = [];
  const origLog = console.log;
  console.log = (...args: unknown[]): void => {
    chunks.push(`${args.map(String).join(' ')}\n`);
  };
  try {
    await fn();
  } finally {
    console.log = origLog;
  }
  return chunks.join('');
}

// ---------------------------------------------------------------------------
// 1. lawProven: ?TODO directly or transitively must report proven: false
// ---------------------------------------------------------------------------

test('lawProven: a law whose body is ?TODO or reaches a ?TODO lemma reports proven: false', async () => {
  await withTempDir(async (dir) => {
    const modelPath = join(dir, 'todo_proven.bend');
    writeFileSync(
      modelPath,
      `import Base

def id_bool(+b: Bool) -> Bool:
  match b:
    case True{}:
      True{}
    case False{}:
      False{}

def helper_hole(+b: Bool) -> {id_bool(b) == b : Bool}:
  ?TODO

law proved_law:
  for +b: Bool
  {id_bool(b) == b : Bool}

def proved_law(b):
  match b:
    case True{}:
      {==}
    case False{}:
      {==}

law direct_hole_law:
  for +b: Bool
  {id_bool(b) == b : Bool}

def direct_hole_law(b):
  ?TODO

law transitive_hole_law:
  for +b: Bool
  {id_bool(b) == b : Bool}

def transitive_hole_law(b):
  helper_hole(b)

law unfilled_law:
  for +b: Bool
  {id_bool(b) == b : Bool}
`,
    );
    const book = await checkBendFile(modelPath);
    const em = book.emit({
      target: { kind: 'ts-module', modelName: 'todo_proven.bend', flags: [] },
      laws: ['proved_law', 'direct_hole_law', 'transitive_hole_law', 'unfilled_law'],
      properties: 2,
    });
    const byName = new Map(em.manifest.laws.map((l) => [l.name, l.proven]));
    expect(byName.get('proved_law')).toBe(true);
    expect(byName.get('direct_hole_law')).toBe(false);
    expect(byName.get('transitive_hole_law')).toBe(false);
    expect(byName.get('unfilled_law')).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// 2. (a) Pure-mirror laws (state machine + nested fields + two-binder + fold)
// ---------------------------------------------------------------------------

const PURE_MIRROR_MODEL = `import Base

type Holder is Data:
  Live{}
  Retired{}
  Nobody{}

type Who is Data:
  Writer{}
  Resolver{}
  Unvisited{}

type Role is Data:
  OfWriter{}
  OfResolver{}

type End is Data:
  Restored{}
  Failed{}

type State is Data:
  Told{n: Nat}
  Stopped{k: Nat, h: Holder}
  Over{end: End}

type StepIn is Data:
  StepIn{h: Holder}

def and(a: Bool, b: Bool) -> Bool:
  match a:
    case True{}:
      b
    case False{}:
      False{}

def implies(p: Bool, q: Bool) -> Bool:
  match p:
    case True{}:
      q
    case False{}:
      True{}

def who(+h: Holder) -> Who:
  match h:
    case Live{}:
      Writer{}
    case Retired{}:
      Resolver{}
    case Nobody{}:
      Resolver{}

def visited(+w: Who) -> Bool:
  match w:
    case Writer{}:
      True{}
    case Resolver{}:
      True{}
    case Unvisited{}:
      False{}

def given(+w: Who, +r: Role) -> Bool:
  match w:
    case Writer{}:
      match r:
        case OfWriter{}:
          True{}
        case OfResolver{}:
          False{}
    case Resolver{}:
      match r:
        case OfWriter{}:
          False{}
        case OfResolver{}:
          True{}
    case Unvisited{}:
      False{}

def acts(+s: State) -> Bool:
  match s:
    case Told{n}:
      True{}
    case Stopped{k, h}:
      visited(who(h))
    case Over{end}:
      True{}

def held(+s: State) -> Bool:
  match s:
    case Told{n}:
      False{}
    case Stopped{k, h}:
      True{}
    case Over{end}:
      False{}

def may_prompt(+s: State, +r: Role) -> Bool:
  match s:
    case Told{n}:
      match r:
        case OfWriter{}:
          True{}
        case OfResolver{}:
          False{}
    case Stopped{k, h}:
      given(who(h), r)
    case Over{end}:
      match r:
        case OfWriter{}:
          True{}
        case OfResolver{}:
          False{}

def at_stop(+s: State, +r: Role) -> Bool:
  match s:
    case Told{n}:
      False{}
    case Stopped{k, h}:
      given(who(h), r)
    case Over{end}:
      False{}

def step_in(+s: State, +i: StepIn) -> State:
  match i:
    case StepIn{h}:
      Stopped{0n, h}

def run(+xs: +List<StepIn>, +s: State) -> State:
  match xs:
    case Nil{}:
      s
    case Con{h, t}:
      run(t, step_in(s, h))

def cat(+xs: +List<StepIn>, +ys: +List<StepIn>) -> +List<StepIn>:
  match xs:
    case Nil{}:
      ys
    case Con{h, t}:
      Con{h, cat(t, ys)}

law direct_refl_law:
  {(who(Live{}), who(Retired{}), who(Nobody{})) == (Writer{}, Resolver{}, Resolver{}) : Who & Who & Who}

law no_state_is_stuck:
  for +s: State
  {acts(s) == True{} : Bool}

law only_stop_assignee_prompted:
  for +s: State
  for +r: Role
  {implies(and(held(s), may_prompt(s, r)), at_stop(s, r)) == True{} : Bool}

law restart_resumes:
  for +xs: +List<StepIn>
  for +s: State
  for +ys: +List<StepIn>
  {run(ys, run(xs, s)) == run(cat(xs, ys), s) : State}
`;

test('(a) pure-mirror laws regenerate with zero holes and ALL PROOFS CHECK', async () => {
  await withTempDir(async (dir) => {
    const unfilledPath = join(dir, 'pure_mirror.bend');
    writeFileSync(unfilledPath, PURE_MIRROR_MODEL);

    const book = await checkBendFile(unfilledPath);
    const res = book.skeleton();

    expect(res.skipped).toEqual([]);
    expect(res.defs.map((d) => d.name)).toEqual([
      'direct_refl_law',
      'no_state_is_stuck',
      'only_stop_assignee_prompted',
      'restart_resumes',
    ]);
    for (const d of res.defs) {
      expect(d.leaves.holes).toBe(0);
    }

    const splicedPath = join(dir, 'pure_mirror_proved.bend');
    writeFileSync(splicedPath, `${PURE_MIRROR_MODEL}\n${res.source}\n`);
    const checked = await checkBookFile(splicedPath);
    expect(checked.hols).toBe(0);

    // Also verify single-?TODO laws are filled identically
    const todoModel = `${PURE_MIRROR_MODEL}
def direct_refl_law():
  ?TODO

def no_state_is_stuck(s):
  ?TODO
`;
    const todoPath = join(dir, 'pure_mirror_todo.bend');
    writeFileSync(todoPath, todoModel);
    const todoBook = await checkBendFile(todoPath);
    const todoRes = todoBook.skeleton({ laws: ['direct_refl_law', 'no_state_is_stuck'] });
    expect(todoRes.skipped).toEqual([]);
    expect(todoRes.defs.map((d) => d.name)).toEqual(['direct_refl_law', 'no_state_is_stuck']);
    for (const d of todoRes.defs) expect(d.leaves.holes).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// 3. (b) Lemma-heavy fixture: hypotheses, implies, IsTrue, nested fields
// ---------------------------------------------------------------------------

test('(b) lemma-heavy laws match split structure with typed holes only at lemma leaves', async () => {
  await withTempDir(async (dir) => {
    const model = `import Base

def IsTrue(b: Bool) -> Data:
  match b:
    case True{}:
      Unit
    case False{}:
      Empty

def implies(p: Bool, q: Bool) -> Bool:
  match p:
    case True{}:
      q
    case False{}:
      True{}

def same(+a: Nat, +b: Nat) -> Bool:
  match a b:
    case 0n 0n:
      True{}
    case 0n 1n+q:
      False{}
    case 1n+p 0n:
      False{}
    case 1n+p 1n+q:
      same(p, q)

def add(+a: Nat, +b: Nat) -> Nat:
  match a:
    case 0n:
      b
    case 1n+p:
      1n+add(p, b)

type Box is Data:
  Box{items: +List<Nat>}

def sum_boxes(+bs: +List<Box>) -> Nat:
  match bs:
    case Nil{}:
      0n
    case Con{+b, +bt}:
      match b:
        case Box{+xs}:
          add(sum_list(xs), sum_boxes(bt))

def sum_list(+xs: +List<Nat>) -> Nat:
  match xs:
    case Nil{}:
      0n
    case Con{+h, +t}:
      add(h, sum_list(t))

def is_zero(+n: Nat) -> Bool:
  match n:
    case 0n:
      True{}
    case 1n+p:
      False{}

law same_to_eq:
  for +a: Nat
  for +b: Nat
  for h: {same(a, b) == True{} : Bool}
  {a == b : Nat}

law add_assoc:
  for +a: Nat
  for +b: Nat
  for +c: Nat
  {add(add(a, b), c) == add(a, add(b, c)) : Nat}

law is_true_zero_implies_zero:
  for +n: Nat
  for +h: IsTrue(is_zero(n))
  {n == 0n : Nat}

law is_true_refl:
  for +n: Nat
  IsTrue(same(n, n))

law implies_zero:
  for +n: Nat
  {implies(is_zero(n), same(add(n, 0n), 0n)) == True{} : Bool}

law sum_boxes_same:
  for +bs: +List<Box>
  {same(sum_boxes(bs), sum_boxes(bs)) == True{} : Bool}
`;
    // Fix def order: sum_list before sum_boxes so book checks cleanly
    const fixedModel = model.replace(
      `def sum_boxes(+bs: +List<Box>) -> Nat:
  match bs:
    case Nil{}:
      0n
    case Con{+b, +bt}:
      match b:
        case Box{+xs}:
          add(sum_list(xs), sum_boxes(bt))

def sum_list(+xs: +List<Nat>) -> Nat:
  match xs:
    case Nil{}:
      0n
    case Con{+h, +t}:
      add(h, sum_list(t))`,
      `def sum_list(+xs: +List<Nat>) -> Nat:
  match xs:
    case Nil{}:
      0n
    case Con{+h, +t}:
      add(h, sum_list(t))

def sum_boxes(+bs: +List<Box>) -> Nat:
  match bs:
    case Nil{}:
      0n
    case Con{+b, +bt}:
      match b:
        case Box{+xs}:
          add(sum_list(xs), sum_boxes(bt))`,
    );
    const modelPath = join(dir, 'lemma_heavy.bend');
    writeFileSync(modelPath, fixedModel);
    const book = await checkBendFile(modelPath);
    const res = book.skeleton();

    const byName = new Map(res.defs.map((d) => [d.name, d]));

    // same_to_eq splits a and b (4 leaves: 0n/0n is {==}, other 3 are holes)
    const sameToEq = byName.get('same_to_eq');
    expect(sameToEq).toBeDefined();
    expect(sameToEq?.leaves.refl).toBe(1);
    expect(sameToEq?.leaves.holes).toBe(3);

    // add_assoc splits a (2 leaves: 0n is {==}, 1n+p is a typed hole)
    const addAssoc = byName.get('add_assoc');
    expect(addAssoc).toBeDefined();
    expect(addAssoc?.leaves.refl).toBe(1);
    expect(addAssoc?.leaves.holes).toBe(1);

    // is_true_zero_implies_zero splits n: 0n -> {==}, 1n+p -> Empty.absurd(..., h)
    const isTrueZero = byName.get('is_true_zero_implies_zero');
    expect(isTrueZero).toBeDefined();
    expect(isTrueZero?.leaves.refl).toBe(1);
    expect(isTrueZero?.leaves.absurd).toBe(1);
    expect(isTrueZero?.leaves.holes).toBe(0);

    // is_true_refl splits n: 0n -> Unit{}, 1n+p -> is_true_refl(p)
    const isTrueRefl = byName.get('is_true_refl');
    expect(isTrueRefl).toBeDefined();
    expect(isTrueRefl?.leaves.refl).toBe(1);
    expect(isTrueRefl?.leaves.recursive).toBe(1);
    expect(isTrueRefl?.leaves.holes).toBe(0);

    // implies_zero splits n: 0n -> {==}, 1n+p -> {==} (implies(False{}, _) == True{})
    const impliesZero = byName.get('implies_zero');
    expect(impliesZero).toBeDefined();
    expect(impliesZero?.leaves.refl).toBe(2);
    expect(impliesZero?.leaves.holes).toBe(0);

    // sum_boxes_same splits bs, and inside Con{+b, +bt} splits b on Box{+xs}
    const sumBoxes = byName.get('sum_boxes_same');
    expect(sumBoxes).toBeDefined();
    expect(sumBoxes?.source).toContain('match bs:');
    expect(sumBoxes?.source).toContain('match b:');
    expect(sumBoxes?.leaves.refl).toBe(1);
    expect(sumBoxes?.leaves.holes).toBe(1);

    // Spliced skeleton file must typecheck cleanly under Bend2
    const splicedPath = join(dir, 'lemma_heavy_skel.bend');
    writeFileSync(splicedPath, `${fixedModel}\n${res.source}\n`);
    const checked = await checkBookFile(splicedPath);
    // Only the 5 genuine lemma leaves remain as ?TODO holes
    expect(checked.hols).toBe(5);
  });
});

// ---------------------------------------------------------------------------
// 4. (c) Second-binder split fixture
// ---------------------------------------------------------------------------

test('(c) law whose subject scrutinizes the second binder splits on the second binder and checks', async () => {
  await withTempDir(async (dir) => {
    const model = `import Base

def same(+a: Nat, +b: Nat) -> Bool:
  match a b:
    case 0n 0n:
      True{}
    case 0n 1n+q:
      False{}
    case 1n+p 0n:
      False{}
    case 1n+p 1n+q:
      same(p, q)

def bump_if(+hit: Bool, +n: Nat) -> Nat:
  match hit:
    case True{}:
      1n+n
    case False{}:
      n

def count_nat(+x: Nat, +xs: +List<Nat>) -> Nat:
  match xs:
    case Nil{}:
      0n
    case Con{+h, +t}:
      bump_if(same(x, h), count_nat(x, t))

def append_nats(+xs: +List<Nat>, +ys: +List<Nat>) -> +List<Nat>:
  match xs:
    case Nil{}:
      ys
    case Con{+h, +t}:
      Con{h, append_nats(t, ys)}

def id_nats(+xs: +List<Nat>) -> +List<Nat>:
  match xs:
    case Nil{}:
      Nil{}
    case Con{+h, +t}:
      Con{h, id_nats(t)}

def has_nat(+x: Nat, +xs: +List<Nat>) -> Bool:
  match xs:
    case Nil{}:
      False{}
    case Con{+h, +t}:
      has_nat(x, t)

law has_nat_always_false:
  for +x: Nat
  for +xs: +List<Nat>
  {has_nat(x, xs) == False{} : Bool}

law count_nat_id:
  for +x: Nat
  for +xs: +List<Nat>
  {count_nat(x, id_nats(xs)) == count_nat(x, xs) : Nat}
`;
    const modelPath = join(dir, 'second_binder.bend');
    writeFileSync(modelPath, model);
    const book = await checkBendFile(modelPath);
    const res = book.skeleton();

    const byName = new Map(res.defs.map((d) => [d.name, d]));
    const alwaysFalse = byName.get('has_nat_always_false');
    expect(alwaysFalse).toBeDefined();
    expect(alwaysFalse?.source).toContain('match xs:');
    expect(alwaysFalse?.source).not.toContain('match x:');
    expect(alwaysFalse?.leaves.refl).toBe(1);
    expect(alwaysFalse?.leaves.recursive).toBe(1);
    expect(alwaysFalse?.leaves.holes).toBe(0);

    const countId = byName.get('count_nat_id');
    expect(countId).toBeDefined();
    expect(countId?.source).toContain('match xs:');
    expect(countId?.source).not.toContain('match x:');
    expect(countId?.leaves.refl).toBe(1);
    expect(countId?.leaves.holes).toBe(1);

    const splicedPath = join(dir, 'second_binder_skel.bend');
    writeFileSync(splicedPath, `${model}\n${res.source}\n`);
    const checked = await checkBookFile(splicedPath);
    expect(checked.hols).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// 5. Dependency-order emission
// ---------------------------------------------------------------------------

test('emits proof defs in dependency order (callee before caller)', async () => {
  await withTempDir(async (dir) => {
    const model = `import Base

def not(+b: Bool) -> Bool:
  match b:
    case True{}:
      False{}
    case False{}:
      True{}

law helper_lemma:
  for +b: Bool
  {not(not(b)) == b : Bool}

law caller_law:
  for +b: Bool
  for h: {not(not(b)) == b : Bool}
  {not(not(not(not(b)))) == b : Bool}
`;
    const modelPath = join(dir, 'dep_order.bend');
    writeFileSync(modelPath, model);
    const book = await checkBendFile(modelPath);

    // Even when caller_law is requested before helper_lemma, callee comes first
    const modelWithDep = `import Base

def flip(+b: Bool) -> Bool:
  match b:
    case True{}:
      False{}
    case False{}:
      True{}

law base_eq:
  for +b: Bool
  {flip(flip(b)) == b : Bool}

def use_base(+b: Bool) -> {flip(flip(b)) == b : Bool}:
  ?TODO

law top_law:
  for +b: Bool
  for _dep: {use_base(b) == base_eq(b) : {flip(flip(b)) == b : Bool}}
  {flip(flip(b)) == b : Bool}
`;
    const depPath = join(dir, 'dep_order2.bend');
    writeFileSync(depPath, modelWithDep);
    const depBook = await checkBendFile(depPath);
    const res = depBook.skeleton({ laws: ['top_law', 'use_base', 'base_eq'] });
    const names = res.defs.map((d) => d.name);
    expect(names.indexOf('base_eq')).toBeLessThan(names.indexOf('top_law'));
    expect(names.indexOf('use_base')).toBeLessThan(names.indexOf('top_law'));

    // Also verify simple order preservation when no edge exists
    const resSimple = book.skeleton({ laws: ['caller_law', 'helper_lemma'] });
    expect(resSimple.defs.map((d) => d.name)).toEqual(['helper_lemma', 'caller_law']);
  });
});

// ---------------------------------------------------------------------------
// 6. Non-overwrite rule (Library API + CLI verb)
// ---------------------------------------------------------------------------

test('non-overwrite rule: library API returns already-proved defs in skipped; CLI --law refuses and --all skips', async () => {
  await withTempDir(async (dir) => {
    const model = `import Base

def not(+b: Bool) -> Bool:
  match b:
    case True{}:
      False{}
    case False{}:
      True{}

law already_proved:
  for +b: Bool
  {not(not(b)) == b : Bool}

def already_proved(b):
  match b:
    case True{}:
      {==}
    case False{}:
      {==}

law unfilled_law:
  for +b: Bool
  {not(not(b)) == b : Bool}

law single_hole_law:
  for +b: Bool
  {not(not(b)) == b : Bool}

def single_hole_law(b):
  ?TODO

def single_hole_lemma(+b: Bool) -> {not(not(b)) == b : Bool}:
  ?TODO
`;
    const modelPath = join(dir, 'non_overwrite.bend');
    writeFileSync(modelPath, model);

    // Library API
    const book = await checkBendFile(modelPath);
    const allRes = book.skeleton();
    expect(allRes.skipped).toEqual(['already_proved']);
    expect(allRes.defs.map((d) => d.name)).toEqual(['unfilled_law', 'single_hole_law', 'single_hole_lemma']);
    expect(allRes.source).not.toContain('def already_proved');

    // Explicit library API request on already_proved returns it in skipped, empty defs
    const explicitRes = book.skeleton({ laws: ['already_proved'] });
    expect(explicitRes.defs).toEqual([]);
    expect(explicitRes.skipped).toEqual(['already_proved']);

    // CLI explicit --law on unfilled_law succeeds and prints def
    const outUnfilled = await captureStdout(() => runCli(['skeleton', modelPath, '--law', 'unfilled_law']));
    expect(outUnfilled).toContain('def unfilled_law(b):');
    expect(outUnfilled).toContain('match b:');

    // CLI explicit --law on already_proved refuses with message naming the def
    await expect(runCli(['skeleton', modelPath, '--law', 'already_proved'])).rejects.toThrow(/already_proved/);

    // CLI --all skips already_proved and prints eligible defs
    const outAll = await captureStdout(() => runCli(['skeleton', modelPath, '--all']));
    expect(outAll).toContain('def unfilled_law(b):');
    expect(outAll).toContain('def single_hole_law(b):');
    expect(outAll).toContain('def single_hole_lemma(');
    expect(outAll).not.toContain('def already_proved(');
  });
});
