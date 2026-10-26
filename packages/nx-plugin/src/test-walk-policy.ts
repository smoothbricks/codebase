// One walk per test path. Within one test file, a case that walks another case's path again, differs from it only in
// literals, or replays its scenario with one injected step changed, tests one path twice: the cases belong in one walk
// that pays the setup once and asserts after each step, or in one table or property over what differs.
//
// Each top-level statement of a case body is normalized three ways: `exact` renames the names the case declares to
// their declaration index and keeps literals, so equal statements do the same thing; `shape` also abstracts every
// literal, so equal shapes differ only in values; `skeleton` also collapses every declared name, so a statement
// matches across cases that declare a different number of names before it. Field, method, key and path names are
// never renamed. Callers choose the files and, for a diff-scoped check, the changed line ranges: a pair is judged when
// either of its cases overlaps a change.
import { spawnSync } from 'node:child_process';
import { existsSync, readFileSync } from 'node:fs';
import { extname, join } from 'node:path';
import ts from 'typescript';
import { type RustToken, rustTokens } from './rust-tokens.js';

export const TEST_ONE_WALK = {
  id: 'TEST-ONE-WALK',
  help:
    'Fold the cases into one walk that pays the setup once and asserts after each step, or into one ' +
    'table/property over the values or injected step that differ. Keep a separate case only for a distinct ' +
    'boundary or edge.',
  /** Leading statements two cases must share exactly before one that only asserts beyond them repeats the other. */
  samePath: 2,
  /** Statements a case needs before a literal-only twin counts. */
  literalTwin: 2,
  /** A replayed scenario shares at least this many leading statements in skeleton (the setup)… */
  scenarioHead: 1,
  /** …and this many trailing ones (the action and its outcome), after the longer case's extra trailing assertions… */
  scenarioTail: 3,
};

/** One top-level statement of a case body, normalized. */
export interface Step {
  readonly exact: string;
  readonly shape: string;
  readonly skeleton: string;
  /** `exact` with every declared name collapsed: equal flows do the same thing with the same values. */
  readonly flow: string;
  /** A pure assertion: it only reads what earlier steps built, so it walks no path of its own. */
  readonly assertion: boolean;
}

/** One test case: `test`/`it` in TypeScript, a `#[test]`-family fn in Rust. */
export interface TestCase {
  readonly name: string;
  readonly start: number;
  /** The 1-based column of the case's first token on its `start` line. */
  readonly column: number;
  readonly end: number;
  /**
   * What decides the case's outcome besides its body: an expected-failure or ignore modifier, and in TypeScript the
   * `before*`/`after*` hooks of its enclosing `describe` chain. Only cases with equal identities are compared.
   */
  readonly identity: string;
  readonly steps: readonly Step[];
}

/**
 * `same-path`: the cases share their leading statements exactly, and one of them only asserts beyond them, so it
 * walks a prefix of the other's path. `literal-twin`: the bodies have the same shape and differ only in literals.
 * `same-scenario`: one case is the other's setup (in skeleton) and its action and outcome (exactly, up to declared
 * names) around one injected statement of its own. It is advisory: measured at about 80% precision, it is reported to
 * the writer and reviewer and never refuses.
 */
export type Duplication = 'same-path' | 'literal-twin' | 'same-scenario';

/** Whether a kind refuses (a lint error) or only advises (a warning for the writer and reviewer). */
export const REFUSES: Record<Duplication, boolean> = {
  'same-path': true,
  'literal-twin': true,
  'same-scenario': false,
};

export interface DuplicateScenario {
  readonly file: string;
  readonly first: TestCase;
  readonly second: TestCase;
  readonly kind: Duplication;
  /** Statements the two cases share: exactly, in shape, or in skeleton, by kind. */
  readonly shared: number;
}

/** A changed line range on the new side of a diff, inclusive. */
export interface ChangeRange {
  readonly start: number;
  readonly end: number;
}

interface Token {
  readonly text: string;
  readonly literal: boolean;
  /** A name the case declares, renamed to `$<declaration index>`. */
  readonly local: boolean;
}

function step(tokens: readonly Token[], assertion: boolean): Step {
  return {
    exact: tokens.map((token) => token.text).join(' '),
    shape: tokens.map((token) => (token.literal ? '$L' : token.text)).join(' '),
    skeleton: tokens.map((token) => (token.literal ? '$L' : token.local ? '$' : token.text)).join(' '),
    flow: tokens.map((token) => (token.local ? '$' : token.text)).join(' '),
    assertion,
  };
}

const JS_EXTENSION = /^\.(?:[cm]?[jt]sx?)$/u;
/** A file under a test directory, or named as a test. Rust test fns are read from any `.rs` file. */
export const TEST_PATH =
  /(?:^|\/)(?:__tests__|tests?|nx-it)(?:\/|$)|(?:^|\/)(?:test[-_]|[^/]+\.(?:test|spec)\.)|[-_]test\.(?:sh|py|[cm]?tsx?)$/u;

// --- TypeScript -------------------------------------------------------------------------------------------------

const CASE_CALLEE = /^(?:test|it)(?:\.(?:only|concurrent|serial|failing))?$/u;
const ASSERT_CALLEE = /^(?:expect|expectTypeOf|assert\w*)$/u;
/** Matchers that run the subject: a throw, a rejection or a resolution is a path of its own. */
const ACTING_MATCHER = /^(?:rejects|resolves|toThrow\w*)$/u;

function declaredNames(body: ts.Node): Map<string, number> {
  const names = new Map<string, number>();
  const bind = (name: ts.BindingName): void => {
    if (ts.isIdentifier(name)) {
      if (!names.has(name.text)) names.set(name.text, names.size);
      return;
    }
    // A shorthand `{ name }` binds the property it reads: renaming it would equate `{ trace }` with `{ flush }`.
    for (const element of name.elements)
      if (!ts.isOmittedExpression(element) && (ts.isArrayBindingPattern(name) || element.propertyName !== undefined))
        bind(element.name);
  };
  const visit = (node: ts.Node): void => {
    if (ts.isVariableDeclaration(node) || ts.isParameter(node)) bind(node.name);
    if ((ts.isFunctionDeclaration(node) || ts.isClassDeclaration(node)) && node.name !== undefined) bind(node.name);
    ts.forEachChild(node, visit);
  };
  visit(body);
  return names;
}

/**
 * `expect(…)…` or `assert…(…)` that calls only methods (no free function, local closure or constructor) and runs no
 * function, rejection or throw; or `if (…) throw …` with no else.
 */
function tsAssertion(statement: ts.Node): boolean {
  if (ts.isIfStatement(statement)) {
    const then = statement.thenStatement;
    const thrown = ts.isThrowStatement(then) || (ts.isBlock(then) && then.statements.every(ts.isThrowStatement));
    return statement.elseStatement === undefined && thrown;
  }
  if (!ts.isExpressionStatement(statement)) return false;
  let asserts = false;
  let acts = false;
  const visit = (node: ts.Node): void => {
    if (ts.isArrowFunction(node) || ts.isFunctionExpression(node) || ts.isNewExpression(node)) acts = true;
    else if (ts.isPropertyAccessExpression(node) && ACTING_MATCHER.test(node.name.text)) acts = true;
    else if (ts.isCallExpression(node) && ts.isIdentifier(node.expression)) {
      if (ASSERT_CALLEE.test(node.expression.text)) asserts = true;
      else acts = true;
    }
    ts.forEachChild(node, visit);
  };
  visit(statement.expression);
  return asserts && !acts;
}

function tsTokens(node: ts.Node, source: ts.SourceFile, locals: ReadonlyMap<string, number>): Token[] {
  const tokens: Token[] = [];
  const visit = (child: ts.Node): void => {
    if (ts.isIdentifier(child)) {
      // A property name (`.name`, `name:`, `{ name }`) is a key, never the local it may share a spelling with.
      const parent = child.parent;
      const key =
        ((ts.isPropertyAccessExpression(parent) || ts.isPropertyAssignment(parent)) && parent.name === child) ||
        ts.isShorthandPropertyAssignment(parent);
      const local = key ? undefined : locals.get(child.text);
      tokens.push(
        local === undefined
          ? { text: child.text, literal: false, local: false }
          : { text: `$${local}`, literal: false, local: true },
      );
      return;
    }
    switch (child.kind) {
      case ts.SyntaxKind.StringLiteral:
      case ts.SyntaxKind.NumericLiteral:
      case ts.SyntaxKind.BigIntLiteral:
      case ts.SyntaxKind.NoSubstitutionTemplateLiteral:
      case ts.SyntaxKind.RegularExpressionLiteral:
      case ts.SyntaxKind.TemplateHead:
      case ts.SyntaxKind.TemplateMiddle:
      case ts.SyntaxKind.TemplateTail:
      case ts.SyntaxKind.TrueKeyword:
      case ts.SyntaxKind.FalseKeyword:
        tokens.push({ text: child.getText(source), literal: true, local: false });
        return;
    }
    const children = child.getChildren(source);
    if (children.length === 0) tokens.push({ text: child.getText(source), literal: false, local: false });
    else for (const grandchild of children) visit(grandchild);
  };
  visit(node);
  return tokens;
}

const HOOK_CALLEE = /^(?:before|after)(?:Each|All)$/u;

/**
 * What the enclosing `describe` chain fixes for a case, outermost first, as source text: its `before*`/`after*` hook
 * calls and its own declarations (`const make = () => new MemoryStore()` makes a different subject than a DiskStore).
 */
function describeChain(node: ts.Node, source: ts.SourceFile): string[] {
  const context: string[] = [];
  for (let scope = node.parent; scope !== undefined; scope = scope.parent) {
    if (!(ts.isArrowFunction(scope) || ts.isFunctionExpression(scope)) || !ts.isBlock(scope.body)) continue;
    const call = scope.parent;
    if (!ts.isCallExpression(call) || !/^describe(?:[.(]|$)/u.test(call.expression.getText(source))) continue;
    const own = scope.body.statements
      .filter(
        (statement) =>
          ts.isVariableStatement(statement) ||
          ts.isFunctionDeclaration(statement) ||
          (ts.isExpressionStatement(statement) &&
            ts.isCallExpression(statement.expression) &&
            ts.isIdentifier(statement.expression.expression) &&
            HOOK_CALLEE.test(statement.expression.expression.text)),
      )
      .map((statement) => statement.getText(source).replace(/\s+/gu, ' '));
    context.unshift(...own);
  }
  return context;
}

/**
 * The steps a case walks. A body that is one call whose last argument is a function, such as
 * `await withHarness(memoryBackend, async (h) => { … })`, walks the call itself as its first step, with the function
 * replaced by `$body` so the wrapper's other arguments still tell cases apart, and then that function's statements.
 */
function caseSteps(body: ts.ConciseBody, source: ts.SourceFile, locals: ReadonlyMap<string, number>): Step[] {
  const steps: Step[] = [];
  let statements: readonly ts.Node[] = ts.isBlock(body) ? body.statements : [body];
  for (;;) {
    const only = statements.length === 1 ? statements[0] : undefined;
    let expression = only !== undefined && ts.isExpressionStatement(only) ? only.expression : only;
    while (expression !== undefined && ts.isAwaitExpression(expression)) expression = expression.expression;
    const call = expression !== undefined && ts.isCallExpression(expression) ? expression : undefined;
    const inner = call?.arguments.at(-1);
    if (
      call === undefined ||
      inner === undefined ||
      !(ts.isArrowFunction(inner) || ts.isFunctionExpression(inner)) ||
      !ts.isBlock(inner.body)
    )
      break;
    const punct = (text: string): Token => ({ text, literal: false, local: false });
    const wrapper = [
      ...tsTokens(call.expression, source, locals),
      punct('('),
      ...call.arguments.slice(0, -1).flatMap((argument) => [...tsTokens(argument, source, locals), punct(',')]),
      punct('$body'),
      punct(')'),
    ];
    steps.push(step(wrapper, false));
    statements = inner.body.statements;
  }
  for (const statement of statements) steps.push(step(tsTokens(statement, source, locals), tsAssertion(statement)));
  return steps;
}

export function typeScriptCases(file: string, text: string): TestCase[] {
  const source = ts.createSourceFile(file, text, ts.ScriptTarget.Latest, true);
  const cases: TestCase[] = [];
  const line = (offset: number): number => source.getLineAndCharacterOfPosition(offset).line + 1;
  const visit = (node: ts.Node): void => {
    const callee = ts.isCallExpression(node) ? node.expression.getText(source) : '';
    if (ts.isCallExpression(node) && CASE_CALLEE.test(callee)) {
      const body = node.arguments.find(
        (argument): argument is ts.ArrowFunction | ts.FunctionExpression =>
          ts.isArrowFunction(argument) || ts.isFunctionExpression(argument),
      );
      const title = node.arguments[0];
      if (body !== undefined && title !== undefined) {
        cases.push({
          name: ts.isStringLiteralLike(title) ? title.text : title.getText(source),
          start: line(node.getStart(source)),
          column: source.getLineAndCharacterOfPosition(node.getStart(source)).character + 1,
          end: line(node.end),
          identity: [callee.endsWith('.failing') ? 'failing' : '', ...describeChain(node, source)].join('\n'),
          steps: caseSteps(body.body, source, declaredNames(body)),
        });
        return;
      }
    }
    ts.forEachChild(node, visit);
  };
  visit(source);
  return cases;
}

// --- Rust -------------------------------------------------------------------------------------------------------

const RUST_ASSERT = /^(?:debug_)?(?:prop_)?assert(?:_eq|_ne|_matches)?$/u;

/**
 * `assert*!(…)` that borrows nothing mutably, calls no method on a `let mut` local (`mutable`, by renamed name), and
 * otherwise calls only methods, macros and `Type::` items.
 */
function rustAssertion(tokens: readonly Token[], mutable: ReadonlySet<string>): boolean {
  if (!RUST_ASSERT.test(tokens[0]?.text ?? '') || tokens[1]?.text !== '!') return false;
  return tokens.every((token, index) => {
    const before = tokens[index - 1]?.text;
    if (token.text === 'mut') return before !== '&';
    if (mutable.has(token.text) && tokens[index + 1]?.text === '.' && tokens[index + 3]?.text === '(') return false;
    if (tokens[index + 1]?.text !== '(' || !/^[a-z_$]/u.test(token.text) || before === '.') return true;
    return before === '::' && /^[A-Z]/u.test(tokens[index - 2]?.text ?? '');
  });
}

const OPEN: Record<string, true> = { '(': true, '[': true, '{': true };
const CLOSE: Record<string, true> = { ')': true, ']': true, '}': true };
/** Statements that end at their closing brace with no `;`. */
const BLOCK_STATEMENT: Record<string, true> = {
  if: true,
  for: true,
  while: true,
  loop: true,
  match: true,
  unsafe: true,
  '{': true,
};
/** Pattern keywords that bind nothing. */
const PATTERN_WORDS: Record<string, true> = { mut: true, ref: true, _: true };

/** The index of the token closing the group opened at `open`. */
function closing(tokens: readonly RustToken[], open: number): number {
  let depth = 0;
  for (let index = open; index < tokens.length; index += 1) {
    const token = tokens[index];
    if (token?.kind !== 'punct') continue;
    if (OPEN[token.text]) depth += 1;
    else if (CLOSE[token.text]) {
      depth -= 1;
      if (depth === 0) return index;
    }
  }
  return tokens.length - 1;
}

function rustSteps(body: readonly RustToken[]): Step[] {
  const locals = new Map<string, number>();
  const mutable = new Set<string>();
  for (let index = 0; index < body.length; index += 1) {
    if (body[index]?.text !== 'let') continue;
    for (let cursor = index + 1; cursor < body.length; cursor += 1) {
      const token = body[cursor];
      if (token === undefined || token.text === '=' || token.text === ':' || token.text === ';') break;
      if (token.kind !== 'ident' || !/^[a-z_]/u.test(token.text) || PATTERN_WORDS[token.text]) continue;
      if (!locals.has(token.text)) locals.set(token.text, locals.size);
      if (body[cursor - 1]?.text === 'mut') mutable.add(`$${locals.get(token.text)}`);
    }
  }
  const steps: Step[] = [];
  let current: Token[] = [];
  let depth = 0;
  const flush = (): void => {
    if (current.length === 0) return;
    steps.push(step(current, rustAssertion(current, mutable)));
    current = [];
  };
  for (let index = 0; index < body.length; index += 1) {
    const token = body[index];
    if (token === undefined) break;
    // A name after `.` or `::` is a field, method or path item, and a name before a lone `:` that opens or follows a
    // `,` in a group is a struct-literal key: neither is the local it may share a spelling with.
    const before = body[index - 1]?.text;
    const member = before === '.' || before === '::';
    const key = body[index + 1]?.text === ':' && (before === '{' || before === ',');
    const local = token.kind === 'ident' && !member && !key ? locals.get(token.text) : undefined;
    current.push(local === undefined ? token : { text: `$${local}`, literal: false, local: true });
    if (token.kind !== 'punct') continue;
    if (OPEN[token.text]) depth += 1;
    else if (CLOSE[token.text]) depth -= 1;
    if (depth !== 0) continue;
    if (token.text === ';') flush();
    else if (token.text === '}' && BLOCK_STATEMENT[current[0]?.text ?? '']) {
      const next = body[index + 1]?.text;
      if (next !== 'else' && next !== '.' && next !== '?') flush();
    }
  }
  flush();
  return steps;
}

/**
 * An `async { … }` block that is the whole of a one-statement body, as in `runtime().block_on(async { … })`: the tokens
 * before it (the wrapper, kept as the case's first step so a different runtime still tells cases apart) and the
 * block's statements. Undefined for any other body.
 */
function asyncBody(
  body: readonly RustToken[],
): { readonly wrapper: readonly RustToken[]; readonly inner: readonly RustToken[] } | undefined {
  const at = body.findIndex((token) => token.text === 'async');
  if (at === -1) return undefined;
  const open = body[at + 1]?.text === 'move' ? at + 2 : at + 1;
  if (body[open]?.text !== '{') return undefined;
  const close = closing(body, open);
  // Only the call's closing parentheses, a `;` and an unwrap or `?` may follow the block.
  return body.length - close <= 8 ? { wrapper: body.slice(0, at), inner: body.slice(open + 1, close) } : undefined;
}

/** Attributes that change what a test's outcome means or where it runs: an expected panic, not running it, a cfg gate. */
const OUTCOME_ATTRIBUTE: Record<string, true> = { should_panic: true, ignore: true, cfg: true, cfg_attr: true };

export function rustCases(text: string): TestCase[] {
  const tokens = rustTokens(text);
  const newlines: number[] = [];
  for (let offset = text.indexOf('\n'); offset !== -1; offset = text.indexOf('\n', offset + 1)) newlines.push(offset);
  const line = (offset: number): number => {
    let [low, high] = [0, newlines.length];
    while (low < high) {
      const middle = (low + high) >> 1;
      if ((newlines[middle] ?? 0) < offset) low = middle + 1;
      else high = middle;
    }
    return low + 1;
  };
  const cases: TestCase[] = [];
  // Outcome attributes may sit before or after `#[test]`; those before it are held until a non-attribute token.
  let held: string[] = [];
  let heldUntil = -1;
  for (let index = 0; index < tokens.length; index += 1) {
    if (tokens[index]?.text !== '#' || tokens[index + 1]?.text !== '[') {
      if (index !== heldUntil) held = [];
      continue;
    }
    const close = closing(tokens, index + 1);
    const attribute = tokens.slice(index + 2, close).map((token) => token.text);
    const pathEnd = attribute.findIndex((text) => text === '(' || text === '=');
    const path = (pathEnd === -1 ? attribute : attribute.slice(0, pathEnd)).at(-1) ?? '';
    if (path !== 'test') {
      if (index !== heldUntil) held = [];
      if (OUTCOME_ATTRIBUTE[path]) held.push(attribute.join(' '));
      heldUntil = close + 1;
      index = close;
      continue;
    }
    const modifiers = [...held];
    held = [];
    let cursor = close + 1;
    while (cursor < tokens.length && tokens[cursor]?.text !== 'fn') {
      if (tokens[cursor]?.text === '#' && tokens[cursor + 1]?.text === '[') {
        const end = closing(tokens, cursor + 1);
        const attribute = tokens.slice(cursor + 2, end).map((token) => token.text);
        if (OUTCOME_ATTRIBUTE[attribute[0] ?? '']) modifiers.push(attribute.join(' '));
        cursor = end;
      }
      cursor += 1;
    }
    const name = tokens[cursor + 1]?.text;
    while (cursor < tokens.length && tokens[cursor]?.text !== '{') {
      if (tokens[cursor]?.text === '(') cursor = closing(tokens, cursor);
      cursor += 1;
    }
    if (name === undefined || cursor >= tokens.length) break;
    const end = closing(tokens, cursor);
    const start = tokens[index]?.offset ?? 0;
    const body = tokens.slice(cursor + 1, end);
    const steps = rustSteps(body);
    const wrapped = steps.length === 1 ? asyncBody(body) : undefined;
    cases.push({
      name,
      start: line(start),
      column: start - (newlines[line(start) - 2] ?? -1),
      end: line(tokens[end]?.offset ?? start),
      identity: modifiers.sort().join('\n'),
      steps: wrapped === undefined ? steps : [step(wrapped.wrapper, false), ...rustSteps(wrapped.inner)],
    });
    index = end;
  }
  return cases;
}

// --- Comparison -------------------------------------------------------------------------------------------------

/**
 * The duplication between two cases of one identity, if either walks the other's path again, differs from it only in
 * literals, or replays its scenario with one injected step changed.
 */
export function duplication(
  first: TestCase,
  second: TestCase,
): { readonly kind: Duplication; readonly shared: number } | undefined {
  if (first.identity !== second.identity) return undefined;
  const [a, b] = [first.steps, second.steps];
  if (a.length === b.length && a.length >= TEST_ONE_WALK.literalTwin && a.every((s, i) => s.shape === b[i]?.shape))
    return { kind: 'literal-twin', shared: a.length };
  const shared = exactPrefix(a, b);
  if (shared >= TEST_ONE_WALK.samePath && (onlyAssertsAfter(a, shared) || onlyAssertsAfter(b, shared)))
    return { kind: 'same-path', shared };
  return sameScenario(a, b);
}

function exactPrefix(a: readonly Step[], b: readonly Step[]): number {
  let shared = 0;
  while (shared < a.length && shared < b.length && a[shared]?.exact === b[shared]?.exact) shared += 1;
  return shared;
}

function onlyAssertsAfter(steps: readonly Step[], shared: number): boolean {
  return steps.slice(shared).every((s) => s.assertion);
}

/**
 * One case is the other's setup (equal in skeleton) and action and outcome (equal in flow: the same calls with the
 * same values, up to declared names) around exactly one statement of its own: the same scenario with a different
 * injected step. The other case may inject more steps, and either case may end with extra pure assertions; they are
 * more checks on the same outcome. The shared tail must hold an action, so two cases that only assert alike after
 * different actions are not one scenario.
 */
function sameScenario(
  a: readonly Step[],
  b: readonly Step[],
): { readonly kind: Duplication; readonly shared: number } | undefined {
  const shared = Math.max(framed(a, b), framed(b, a));
  return shared > 0 ? { kind: 'same-scenario', shared } : undefined;
}

/** How many trailing statements of `steps` are pure assertions. */
function trailingAssertions(steps: readonly Step[]): number {
  let count = 0;
  while (count < steps.length && steps[steps.length - 1 - count]?.assertion === true) count += 1;
  return count;
}

/** The head and tail `own` shares with `other` when exactly one statement of `own` lies between them, else 0. */
function framed(own: readonly Step[], other: readonly Step[]): number {
  let head = 0;
  while (head < own.length && own[head]?.skeleton === other[head]?.skeleton) head += 1;
  if (head < TEST_ONE_WALK.scenarioHead) return 0;
  let best = 0;
  for (let ownExtra = 0; ownExtra <= trailingAssertions(own); ownExtra += 1) {
    const ownEnd = own.length - ownExtra;
    for (let otherExtra = 0; otherExtra <= trailingAssertions(other); otherExtra += 1) {
      const otherEnd = other.length - otherExtra;
      let tail = 0;
      while (
        tail < ownEnd - head &&
        tail < otherEnd - head &&
        own[ownEnd - 1 - tail]?.flow === other[otherEnd - 1 - tail]?.flow
      )
        tail += 1;
      const acts = own.slice(ownEnd - tail, ownEnd).some((s) => !s.assertion);
      if (acts && tail >= TEST_ONE_WALK.scenarioTail && ownEnd - head - tail === 1) best = Math.max(best, head + tail);
    }
  }
  return best;
}

/**
 * For each case, the earlier case it most nearly repeats among the pairs `judged` admits (a diff-scoped caller admits
 * a pair when either case changed; partners are chosen only among those). The nearest refusing partner and the nearest
 * advisory partner are reported separately, so an advisory pair can never hide a refusing one.
 */
export function duplicateScenarios(
  file: string,
  cases: readonly TestCase[],
  judged: (first: TestCase, second: TestCase) => boolean = () => true,
): DuplicateScenario[] {
  const found: DuplicateScenario[] = [];
  cases.forEach((second, later) => {
    const best: Partial<Record<'refusing' | 'advisory', DuplicateScenario>> = {};
    for (const first of cases.slice(0, later)) {
      if (!judged(first, second)) continue;
      const match = duplication(first, second);
      if (match === undefined) continue;
      const slot = REFUSES[match.kind] ? 'refusing' : 'advisory';
      const held = best[slot];
      if (held === undefined || match.shared > held.shared) best[slot] = { file, first, second, ...match };
    }
    if (best.refusing !== undefined) found.push(best.refusing);
    if (best.advisory !== undefined) found.push(best.advisory);
  });
  return found;
}

export function testCases(file: string, text: string): TestCase[] | undefined {
  if (JS_EXTENSION.test(extname(file))) return TEST_PATH.test(file) ? typeScriptCases(file, text) : undefined;
  return extname(file) === '.rs' ? rustCases(text) : undefined;
}

export function findDuplicateScenarios(
  root: string,
  files: readonly string[],
  changes?: ReadonlyMap<string, readonly ChangeRange[]>,
): DuplicateScenario[] {
  const found: DuplicateScenario[] = [];
  for (const file of files) {
    const path = join(root, file);
    if (!existsSync(path)) continue;
    const cases = testCases(file, readFileSync(path, 'utf8'));
    if (cases === undefined) continue;
    const ranges = changes?.get(file);
    const touched = (test: TestCase): boolean =>
      ranges === undefined || ranges.some((range) => range.start <= test.end && range.end >= test.start);
    found.push(...duplicateScenarios(file, cases, (first, second) => touched(first) || touched(second)));
  }
  return found;
}

export function formatDuplicateScenario(duplicate: DuplicateScenario): string {
  const { first, second, shared } = duplicate;
  const [a, b] = [`\`${first.name}\``, `\`${second.name}\``];
  const verbatim =
    first.steps.length === second.steps.length && first.steps.every((s, i) => s.exact === second.steps[i]?.exact);
  const asserter = onlyAssertsAfter(first.steps, shared) ? [a, b] : [b, a];
  const what =
    duplicate.kind === 'literal-twin'
      ? verbatim
        ? `${b} repeats ${a} verbatim (${shared} statements)`
        : `${b} differs from ${a} only in literals (${shared} statements of one shape)`
      : duplicate.kind === 'same-path'
        ? `${asserter[0]} only asserts after the ${shared} statements it shares with ${asserter[1]}: its assertions ` +
          `belong in ${asserter[1]} after statement ${shared}`
        : `${b} replays the scenario of ${a} with one step changed (${shared} shared statements of setup, action ` +
          'and outcome)';
  return (
    `${REFUSES[duplicate.kind] ? 'error' : 'warning'}[${TEST_ONE_WALK.id}]: ${what}\n` +
    `  --> ${duplicate.file}:${second.start}:${second.column}\n` +
    `  --> ${duplicate.file}:${first.start}:${first.column}\n` +
    `  = help: ${TEST_ONE_WALK.help}`
  );
}

/** A git answer, or why git could not give one. */
export type GitAnswer<T> = { readonly ok: true; readonly value: T } | { readonly ok: false; readonly reason: string };

/** Runs git without global or system configuration, so a sandboxed caller gets the same answer as its host. */
function git(
  root: string,
  args: readonly string[],
): { readonly status: number; readonly stdout: string; readonly stderr: string } {
  // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: a local git query whose length is its own read of this repository: it asks no remote, and a held index.lock fails it at once rather than waiting, so only the operator's interrupt ends a hung one
  const result = spawnSync('git', args, {
    cwd: root,
    encoding: 'utf8',
    env: { ...process.env, GIT_CONFIG_GLOBAL: '/dev/null', GIT_CONFIG_NOSYSTEM: '1' },
    maxBuffer: 1 << 28,
  });
  return { status: result.status ?? -1, stdout: result.stdout, stderr: result.stderr.trim() };
}

function failed(args: readonly string[], result: { readonly status: number; readonly stderr: string }): string {
  return `git ${args.join(' ')}: ${result.stderr || `exit ${result.status}`}`;
}

/**
 * The merge base of HEAD and the upstream main. A set `NX_BASE` (exported by nx-set-shas in CI) is the upstream and
 * must name a commit. Otherwise the upstream is the first that exists of `refs/remotes/main/main`, the local main's
 * upstream branch and `refs/remotes/origin/main`, and local `main` when none does.
 */
export function upstreamBase(root: string): GitAnswer<string> {
  const nxBase = process.env.NX_BASE;
  const exists = (ref: string): GitAnswer<boolean> => {
    const args = ['rev-parse', '--verify', '--quiet', ref];
    const result = git(root, args);
    // `--verify --quiet` answers an absent ref with status 1; any other failure is git failing, not an answer.
    if (result.status === 0 || result.status === 1) return { ok: true, value: result.status === 0 };
    return { ok: false, reason: failed(args, result) };
  };
  let upstream = 'main';
  if (nxBase !== undefined && nxBase !== '') {
    const named = exists(`${nxBase}^{commit}`);
    if (!named.ok) return named;
    if (!named.value) return { ok: false, reason: `NX_BASE=${nxBase} names no commit` };
    upstream = nxBase;
  } else {
    // The local main's upstream, when it has one: `for-each-ref` answers empty, never fails, when either is missing.
    const tracked = git(root, ['for-each-ref', '--format=%(upstream)', 'refs/heads/main']).stdout.trim();
    for (const ref of ['refs/remotes/main/main', tracked, 'refs/remotes/origin/main'].filter(Boolean)) {
      const found = exists(ref);
      if (!found.ok) return found;
      if (found.value) {
        upstream = ref;
        break;
      }
    }
    if (upstream === 'main') {
      // No upstream ref answered: say so as its own reason rather than letting
      // merge-base fail against a local main that may not exist (a fresh shed
      // has none until it fetches).
      const local = exists('main');
      if (!local.ok) return local;
      if (!local.value)
        return {
          ok: false,
          reason:
            'no main ref in this shed: neither refs/remotes/main/main, refs/remotes/origin/main nor a local main exists (fetch main, set refs/remotes/main/main, or export NX_BASE)',
        };
    }
  }
  const args = ['merge-base', 'HEAD', upstream];
  const result = git(root, args);
  if (result.status !== 0)
    return {
      ok: false,
      reason: `${failed(args, result)} (fetch main, set refs/remotes/main/main, or export NX_BASE)`,
    };
  return { ok: true, value: result.stdout.trim() };
}

/**
 * The upstream base as a run precondition, checked before Nx hashes anything:
 * only a workspace whose own root package.json declares the test-walk check
 * (this repository) is gated, and a missing main ref fails the run with the
 * check's own reason instead of an error smuggled through a runtime input's
 * byte channel. Workspaces that never declare the check are untouched.
 */
export function assertUpstreamBasePrecondition(workspaceRoot: string): void {
  let rootPackage: string;
  try {
    rootPackage = readFileSync(join(workspaceRoot, 'package.json'), 'utf8');
  } catch {
    return;
  }
  if (!rootPackage.includes('smoo-nx-test-walks')) return;
  const base = upstreamBase(workspaceRoot);
  if (!base.ok) throw new Error(`${TEST_ONE_WALK.id} cannot find the upstream base: ${base.reason}`);
}

/**
 * The files changed since `base`, tracked or untracked, and the new-side line ranges of each change; an untracked
 * file is changed throughout.
 */
export function diffScope(
  root: string,
  base: string,
): GitAnswer<{ readonly files: readonly string[]; readonly changes: ReadonlyMap<string, readonly ChangeRange[]> }> {
  const read = (args: readonly string[]): GitAnswer<string> => {
    const result = git(root, args);
    return result.status === 0 ? { ok: true, value: result.stdout } : { ok: false, reason: failed(args, result) };
  };
  const changed = read(['diff', '--name-only', '-z', base, '--']);
  if (!changed.ok) return changed;
  const listed = read(['ls-files', '--others', '--exclude-standard', '-z']);
  if (!listed.ok) return listed;
  const untracked = new Set(listed.value.split('\0'));
  const files = [...new Set([...changed.value.split('\0'), ...untracked].filter(Boolean))].sort();
  const changes = new Map<string, readonly ChangeRange[]>();
  for (const file of files) {
    if (untracked.has(file)) {
      changes.set(file, [{ start: 1, end: Number.POSITIVE_INFINITY }]);
      continue;
    }
    const diff = read(['diff', '--unified=0', '--no-ext-diff', '--no-color', base, '--', file]);
    if (!diff.ok) return diff;
    changes.set(
      file,
      Array.from(diff.value.matchAll(/^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@/gmu), (match) => {
        const start = Number(match[1]);
        return { start, end: start + Math.max(Number(match[2] ?? 1), 1) - 1 };
      }),
    );
  }
  return { ok: true, value: { files, changes } };
}
