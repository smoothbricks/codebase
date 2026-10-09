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
import { execFileSync } from 'node:child_process';
import { existsSync, readFileSync } from 'node:fs';
import { extname, join } from 'node:path';
import ts from 'typescript';

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
  /** A pure assertion: it only reads what earlier steps built, so it walks no path of its own. */
  readonly assertion: boolean;
}

/** One test case: `test`/`it` in TypeScript, a `#[test]`-family fn in Rust. */
export interface TestCase {
  readonly name: string;
  readonly start: number;
  readonly end: number;
  readonly steps: readonly Step[];
}

/**
 * `same-path`: the cases share their leading statements exactly, and one of them only asserts beyond them, so it
 * walks a prefix of the other's path. `literal-twin`: the bodies have the same shape and differ only in literals.
 * `same-scenario`: the cases share their setup and their action and outcome in skeleton, and the shorter one differs
 * in at most one injected statement between them.
 */
export type Duplication = 'same-path' | 'literal-twin' | 'same-scenario';

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

export function typeScriptCases(file: string, text: string): TestCase[] {
  const source = ts.createSourceFile(file, text, ts.ScriptTarget.Latest, true);
  const cases: TestCase[] = [];
  const line = (offset: number): number => source.getLineAndCharacterOfPosition(offset).line + 1;
  const visit = (node: ts.Node): void => {
    if (ts.isCallExpression(node) && CASE_CALLEE.test(node.expression.getText(source))) {
      const body = node.arguments.find(
        (argument): argument is ts.ArrowFunction | ts.FunctionExpression =>
          ts.isArrowFunction(argument) || ts.isFunctionExpression(argument),
      );
      const title = node.arguments[0];
      if (body !== undefined && title !== undefined) {
        const locals = declaredNames(body);
        const statements: readonly ts.Node[] = ts.isBlock(body.body) ? body.body.statements : [body.body];
        cases.push({
          name: ts.isStringLiteralLike(title) ? title.text : title.getText(source),
          start: line(node.getStart(source)),
          end: line(node.end),
          steps: statements.map((statement) =>
            step(tsTokens(statement, source, locals), ts.isBlock(body.body) ? tsAssertion(statement) : false),
          ),
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

interface RustToken extends Token {
  readonly kind: 'ident' | 'literal' | 'punct' | 'lifetime';
  readonly offset: number;
}

const RUST_IDENT = /[A-Za-z_][A-Za-z0-9_]*/uy;
const RUST_NUMBER =
  /(?:0[xob][0-9A-Fa-f_]+|[0-9][0-9_]*(?:\.[0-9][0-9_]*)?(?:[eE][+-]?[0-9_]+)?)(?:[iuf](?:8|16|32|64|128|size))?/uy;
const RUST_RAW_STRING = /b?r(#*)"/uy;
const RUST_CHAR = /b?'(?:\\(?:u\{[0-9A-Fa-f]+\}|x[0-9A-Fa-f]{2}|.)|[^\\'\n])'/uy;
const RUST_PUNCT = ['::', '->', '=>', '==', '!=', '<=', '>=', '&&', '||', '..='];
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

/** A Rust lexer that keeps exactly what statement comparison needs: idents, literals, lifetimes, punctuation. */
export function rustTokens(text: string): RustToken[] {
  const tokens: RustToken[] = [];
  let index = 0;
  const at = (pattern: RegExp): string | undefined => {
    pattern.lastIndex = index;
    return pattern.exec(text)?.[0];
  };
  const push = (kind: RustToken['kind'], start: number): void => {
    tokens.push({ text: text.slice(start, index), literal: kind === 'literal', local: false, kind, offset: start });
  };
  while (index < text.length) {
    const char = text[index] ?? '';
    const start = index;
    if (/\s/u.test(char)) index += 1;
    else if (text.startsWith('//', index)) {
      const end = text.indexOf('\n', index);
      index = end === -1 ? text.length : end;
    } else if (text.startsWith('/*', index)) {
      let depth = 0;
      do {
        if (text.startsWith('/*', index)) depth += 1;
        else if (text.startsWith('*/', index)) depth -= 1;
        index += text.startsWith('/*', index) || text.startsWith('*/', index) ? 2 : 1;
      } while (depth > 0 && index < text.length);
    } else if (at(RUST_RAW_STRING) !== undefined) {
      const raw = at(RUST_RAW_STRING) ?? '';
      const close = `"${raw.slice(raw.indexOf('r') + 1, -1)}`;
      const end = text.indexOf(close, index + raw.length);
      index = end === -1 ? text.length : end + close.length;
      push('literal', start);
    } else if (char === '"' || (char === 'b' && text[index + 1] === '"')) {
      index += char === 'b' ? 2 : 1;
      while (index < text.length && text[index] !== '"') index += text[index] === '\\' ? 2 : 1;
      index += 1;
      push('literal', start);
    } else if (at(RUST_CHAR) !== undefined) {
      index += at(RUST_CHAR)?.length ?? 0;
      push('literal', start);
    } else if (char === "'") {
      index += 1;
      index += at(RUST_IDENT)?.length ?? 0;
      push('lifetime', start);
    } else if (at(RUST_IDENT) !== undefined) {
      const ident = at(RUST_IDENT) ?? '';
      index += ident.length;
      push(ident === 'true' || ident === 'false' ? 'literal' : 'ident', start);
    } else if ((at(RUST_NUMBER) ?? '') !== '') {
      index += at(RUST_NUMBER)?.length ?? 0;
      push('literal', start);
    } else {
      index += RUST_PUNCT.find((punct) => text.startsWith(punct, index))?.length ?? 1;
      push('punct', start);
    }
  }
  return tokens;
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
    // A name after `.` or `::` is a field, method or path item, never the local it may share a spelling with.
    const member = body[index - 1]?.text === '.' || body[index - 1]?.text === '::';
    const local = token.kind === 'ident' && !member ? locals.get(token.text) : undefined;
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

export function rustCases(text: string): TestCase[] {
  const tokens = rustTokens(text);
  const line = (offset: number): number => text.slice(0, offset).split('\n').length;
  const cases: TestCase[] = [];
  for (let index = 0; index < tokens.length; index += 1) {
    if (tokens[index]?.text !== '#' || tokens[index + 1]?.text !== '[') continue;
    const close = closing(tokens, index + 1);
    const attribute = tokens.slice(index + 2, close).map((token) => token.text);
    const pathEnd = attribute.findIndex((text) => text === '(' || text === '=');
    if ((pathEnd === -1 ? attribute : attribute.slice(0, pathEnd)).at(-1) !== 'test') continue;
    let cursor = close + 1;
    while (cursor < tokens.length && tokens[cursor]?.text !== 'fn') {
      if (tokens[cursor]?.text === '#' && tokens[cursor + 1]?.text === '[') cursor = closing(tokens, cursor + 1);
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
    cases.push({
      name,
      start: line(start),
      end: line(tokens[end]?.offset ?? start),
      steps: rustSteps(tokens.slice(cursor + 1, end)),
    });
    index = end;
  }
  return cases;
}

// --- Comparison -------------------------------------------------------------------------------------------------

/** The duplication between two cases, if either walks the other's path again or differs only in literals. */
export function duplication(
  first: TestCase,
  second: TestCase,
): { readonly kind: Duplication; readonly shared: number } | undefined {
  const [a, b] = [first.steps, second.steps];
  if (a.length === b.length && a.length >= TEST_ONE_WALK.literalTwin && a.every((s, i) => s.shape === b[i]?.shape))
    return { kind: 'literal-twin', shared: a.length };
  let shared = 0;
  while (shared < a.length && shared < b.length && a[shared]?.exact === b[shared]?.exact) shared += 1;
  const onlyAsserts = (steps: readonly Step[]): boolean => steps.slice(shared).every((s) => s.assertion);
  if (shared >= TEST_ONE_WALK.samePath && (onlyAsserts(a) || onlyAsserts(b))) return { kind: 'same-path', shared };
  return sameScenario(a, b);
}

/**
 * One case's skeleton is the other's head and tail around exactly one statement of its own: the same setup, a
 * different injected step, then the same action and outcome. The other case may inject more steps, and either case
 * may end with extra pure assertions; they are more checks on the same outcome.
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
        own[ownEnd - 1 - tail]?.skeleton === other[otherEnd - 1 - tail]?.skeleton
      )
        tail += 1;
      if (tail >= TEST_ONE_WALK.scenarioTail && ownEnd - head - tail === 1) best = Math.max(best, head + tail);
    }
  }
  return best;
}

/** For each case, the earlier case in the file it most nearly repeats. */
export function duplicateScenarios(file: string, cases: readonly TestCase[]): DuplicateScenario[] {
  const found: DuplicateScenario[] = [];
  cases.forEach((second, later) => {
    let best: DuplicateScenario | undefined;
    for (const first of cases.slice(0, later)) {
      const match = duplication(first, second);
      if (match !== undefined && (best === undefined || match.shared > best.shared))
        best = { file, first, second, ...match };
    }
    if (best !== undefined) found.push(best);
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
    for (const duplicate of duplicateScenarios(file, cases))
      if (touched(duplicate.first) || touched(duplicate.second)) found.push(duplicate);
  }
  return found;
}

export function formatDuplicateScenario(duplicate: DuplicateScenario): string {
  const first = `\`${duplicate.first.name}\``;
  const what =
    duplicate.kind === 'literal-twin'
      ? `differs from ${first} only in literals (${duplicate.shared} statements of one shape)`
      : duplicate.kind === 'same-path'
        ? `walks the path of ${first} again (${duplicate.shared} shared statements, then one of them only asserts)`
        : `replays the scenario of ${first} with one step changed (${duplicate.shared} shared statements of setup, ` +
          'action and outcome)';
  return (
    `error[${TEST_ONE_WALK.id}]: test \`${duplicate.second.name}\` ${what}\n` +
    `  --> ${duplicate.file}:${duplicate.second.start}\n` +
    `  --> ${duplicate.file}:${duplicate.first.start}\n` +
    `  = help: ${TEST_ONE_WALK.help}`
  );
}

/**
 * The files changed since `base`, tracked or untracked, and the new-side line ranges of each change; an untracked
 * file is changed throughout. Git runs without global or system configuration, so a sandboxed caller gets the same
 * answer as its host.
 */
export function diffScope(
  root: string,
  base: string,
): { readonly files: readonly string[]; readonly changes: ReadonlyMap<string, readonly ChangeRange[]> } {
  const git = (args: readonly string[]): string =>
    execFileSync('git', args, {
      cwd: root,
      encoding: 'utf8',
      env: { ...process.env, GIT_CONFIG_GLOBAL: '/dev/null', GIT_CONFIG_NOSYSTEM: '1' },
      maxBuffer: 1 << 28,
    });
  const changed = git(['diff', '--name-only', '-z', base, '--']).split('\0');
  const untracked = new Set(git(['ls-files', '--others', '--exclude-standard', '-z']).split('\0'));
  const files = [...new Set([...changed, ...untracked].filter(Boolean))].sort();
  const changes = new Map<string, readonly ChangeRange[]>();
  for (const file of files) {
    if (untracked.has(file)) {
      changes.set(file, [{ start: 1, end: Number.POSITIVE_INFINITY }]);
      continue;
    }
    const diff = git(['diff', '--unified=0', '--no-ext-diff', '--no-color', base, '--', file]);
    changes.set(
      file,
      Array.from(diff.matchAll(/^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@/gmu), (match) => {
        const start = Number(match[1]);
        return { start, end: start + Math.max(Number(match[2] ?? 1), 1) - 1 };
      }),
    );
  }
  return { files, changes };
}
