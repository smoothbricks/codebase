// TEST-CLOCK-ORACLE: a test's duration is a property of its work, not of the host. Bun legs delete the runner's
// per-test deadline with `--timeout=0`, the leg's no-progress bound (`BOUNDED_TEST_IDLE_TIMEOUT_MS`) is the only bound
// that decides a hang, and a test waits for the event it is waiting for however long a loaded host makes the work take
// (README "Bounded Test Targets"; specs/cowshed/08_testing.md). Within test code this refuses four kinds:
// - runner-deadline: a deadline handed to the test runner (`test(name, fn, 60_000)`, `{ timeout }`,
//   `setDefaultTimeout`). It is deleted and nothing takes its place.
// - test-deadline: the Rust test's own deadline, a `recv_timeout`, `park_timeout` or `timeout` bounded by a literal
//   below the backstop whose expiry fails the case (`.expect`/`.unwrap`). A quiet window (`.await.is_err()`) passes.
// - clock-delta (class A): an assertion that elapsed host time stays under a bound, `expect(Date.now() - t)
//   .toBeLessThan(N)` or `assert!(started.elapsed() < d)`, which a loaded host breaks while the work is unchanged.
//   Clock values are typed as a reading, a reading moved by a fixed span (a deadline) or time passed, so a lower bound
//   (the wait really waited), ordering against a product timestamp, an epoch stamp, CPU time and an injected clock
//   (`timer.now()`, `clock.elapsed()`) pass.
// - clocked-bound (class D): a literal product bound below the no-progress backstop in a case whose stand-in a timer or
//   sleep ends before that backstop, so the bound races the stand-in's clock. A bound that is the case's subject with
//   the event that would beat it held off by construction (a child that never answers, or one held at or past the
//   backstop such as `exec sleep 3600`), 0 (disabled), or at the backstop passes.
// Callers choose the files and, for a diff-scoped check, the changed line ranges: a site is judged when the test case
// holding it overlaps a change (the whole case, so an edit cannot retain its old site), or, outside any case, when its
// own line changed. Existing sites therefore need no allowance table. Paused tokio time (`tokio::time::pause()`,
// `start_paused = true`) is virtual, so a Rust fn using it is not examined.
//
// What it cannot do. It stops new sites and removes none: a case nobody edits keeps its elapsed bound, its deadline
// and its racing bound until a migration touches it. It does not see: a bound passed positionally
// (`collect(child, 750)`) or held in a constant declared outside the case; a stand-in timer in another file or written
// by a helper; a bound below the backstop beside real work that no timer delays, which reads exactly like a subject
// bound whose event is held off by construction; a host clock read through a helper (`elapsedSince(t)`) or a struct
// field; a deadline enforced by `if (…) throw` or a `while` condition, which it reads as a hang guard, although
// `assert!(Instant::now() < deadline)` is refused; a Rust deadline in a socket option (`set_read_timeout`),
// `libc::poll`, a `Duration` held in a variable, or one whose expiry is matched rather than unwrapped; a Rust runner
// deadline (nextest's slow-timeout is configuration); timer waits themselves (`Bun.sleep` in a test); clock assertions
// in Python or shell.
import { existsSync, readFileSync } from 'node:fs';
import { extname, join } from 'node:path';
import ts from 'typescript';
import { BOUNDED_TEST_IDLE_TIMEOUT_MS } from './bounded-test-policy.js';
import { type ChangeRange, TEST_PATH, testCases } from './test-walk-policy.js';

export type ClockOracleKind = 'runner-deadline' | 'test-deadline' | 'clock-delta' | 'clocked-bound';

export const TEST_CLOCK_ORACLE = {
  id: 'TEST-CLOCK-ORACLE',
  /** The leg's no-progress backstop: a bound at or above it, or 0 (disabled), cannot decide a case. */
  backstopMs: BOUNDED_TEST_IDLE_TIMEOUT_MS,
  message: {
    'runner-deadline': 'tests must not carry a per-test deadline',
    'test-deadline': 'a test-owned deadline decides the case',
    'clock-delta': 'tests must not bound elapsed host time',
    'clocked-bound': 'a configured bound races a stand-in that a timer ends',
  } satisfies Record<ClockOracleKind, string>,
  help: {
    'runner-deadline':
      "Delete the deadline and put nothing in its place: every bun leg runs with --timeout=0 and the leg's " +
      'no-progress bound is the only bound that decides a hang.',
    'test-deadline':
      "Wait for the event itself: the leg's no-progress bound is the only bound that decides a hang. Assert a quiet " +
      'window with `.is_err()`, or advance virtual time with `tokio::time::pause()`.',
    'clock-delta':
      'Elapsed time on a loaded host measures the host, not the work. Assert the order of events or the work done; ' +
      'time-dependent behavior uses an injected clock and asserts its readings.',
    'clocked-bound':
      "A bound a test configures is the case's subject, with the event that would beat it held off by construction " +
      '(a child that never answers, a stand-in that never writes readiness), or it cannot decide: disabled, or at ' +
      "the leg's no-progress backstop (BOUNDED_TEST_IDLE_TIMEOUT_MS). A stand-in that a timer or sleep ends races " +
      'the bound on a loaded host.',
  } satisfies Record<ClockOracleKind, string>,
};

export interface ClockOracle {
  readonly kind: ClockOracleKind;
  readonly file: string;
  readonly line: number;
  /** The 1-based column of the site's first character. */
  readonly column: number;
  readonly text: string;
}

interface Site {
  readonly kind: ClockOracleKind;
  readonly offset: number;
  readonly text: string;
}

const JS_EXTENSION = /^\.(?:[cm]?[jt]sx?)$/u;
/** A call that sleeps: the stand-in sleeps a child or helper runs. */
const SLEEP_CALL =
  /(?:\bBun\.sleep(?:Sync)?|\b(?:std::)?thread::sleep|\btokio::time::sleep|\b(?:asyncio|time)\.sleep|\bThread\.sleep|(?<![.\w])(?:sleep|sleep_for|sleep_until|usleep)|\b\w+\.waitForTimeout)\s*\(/u;
/** A call that arms a timer. */
const TIMER_CALL_PATTERN =
  /(?:(?<![.\w])(?:setTimeout|setInterval)|\b(?:globalThis|window)\.(?:setTimeout|setInterval))\s*\(/u;
/** A call that sleeps or arms a timer. */
const TIMER_CALL = new RegExp(`${SLEEP_CALL.source}|${TIMER_CALL_PATTERN.source}`, 'u');
/** Source text of a stand-in that a timer or sleep ends: a call, a child's `-e` script, a shell line, an argv `sleep`. */
const TIMED_STAND_IN = new RegExp(
  `${TIMER_CALL.source}|${/(?:^|[;&|\n"'`(]|\bexec)\s*sleep\s+(?:\d|["'$])|["'`]sleep["'`]/u.source}`,
  'gu',
);
/** The span of the timer or sleep starting `text`, in milliseconds, when its source states one. */
function timerSpanMs(text: string): number | undefined {
  const shell = /^(?:[;&|\n"'`(]|exec)?\s*sleep\s+(\d+(?:\.\d+)?)([smhd]?)(?![\w.])/u.exec(text);
  if (shell !== null) return Number(shell[1]) * 1000 * ({ m: 60, h: 3600, d: 86_400 }[shell[2] ?? ''] ?? 1);
  const argv = /^["'`]sleep["'`]\s*(?:,|\)\s*\.arg\()\s*["'`](\d+(?:\.\d+)?)["'`]/u.exec(text);
  if (argv !== null) return Number(argv[1]) * 1000;
  const timeout = /^(?:(?:globalThis|window)\.)?setTimeout\s*\([^;]*?,\s*(\d[\d_]*)\s*\)/u.exec(text.slice(0, 400));
  if (timeout !== null) return Number((timeout[1] ?? '').replaceAll('_', ''));
  const bun = /^Bun\.sleep(?:Sync)?\s*\(\s*(\d[\d_]*)\s*\)/u.exec(text);
  if (bun !== null) return Number((bun[1] ?? '').replaceAll('_', ''));
  const seconds = /^(?:asyncio|time)\.sleep\s*\(\s*(\d+(?:\.\d+)?)\s*\)/u.exec(text);
  if (seconds !== null) return Number(seconds[1]) * 1000;
  const rust =
    /^(?:(?:std::)?thread|tokio::time)::sleep\s*\(\s*(?:std::time::)?Duration::from_(secs|millis)\s*\(\s*(\d[\d_]*)\s*\)\s*\)/u.exec(
      text,
    );
  if (rust !== null) return Number((rust[2] ?? '').replaceAll('_', '')) * (rust[1] === 'secs' ? 1000 : 1);
  return undefined;
}
/**
 * Whether source text holds a stand-in that a timer or sleep ends while the leg still runs. One held at or past the
 * backstop (`exec sleep 3600`) outlasts the leg, so it is held off as surely as a child that never answers; a
 * repeating timer or an unstated span may end or progress at any time, so it counts.
 */
function endsByTimer(text: string): boolean {
  for (const match of text.matchAll(TIMED_STAND_IN)) {
    const ms = timerSpanMs(text.slice(match.index));
    if (ms === undefined || ms < TEST_CLOCK_ORACLE.backstopMs) return true;
  }
  return false;
}
/** A liveness bound by name: any timeout or deadline, or a grace, kill, budget or idle span carrying a time unit. */
function boundName(name: string): boolean {
  return (
    /time_?out|deadline/iu.test(name) ||
    (/grace|kill_?after|budget|idle/iu.test(name) && /(?:ms|millis|milliseconds|seconds?|secs?)$/iu.test(name))
  );
}
/** A bound's value in milliseconds from its name's unit; one that is 0 or at the backstop cannot decide a case. */
function decides(name: string, value: number): boolean {
  const ms = /(?:seconds?|secs?|_s)$/iu.test(name) ? value * 1000 : value;
  return ms > 0 && ms < TEST_CLOCK_ORACLE.backstopMs;
}
/** `--timeout=100`, `--idle-timeout 5s`, `--deadline-ms=50` inside a command string. */
const FLAG_BOUND = /--[\w-]*(?:timeout|deadline)[\w-]*[= ](\d[\d_]*(?:\.\d+)?)(ms|s)?\b/giu;

/** What a value read from the host clock is: a reading, a reading moved by a fixed span, or time passed. */
type Clock = 'instant' | 'deadline' | 'duration';
const CLOCK_RANK = { instant: 1, deadline: 2, duration: 3 } as const satisfies Record<Clock, number>;

function strongest(clocks: readonly (Clock | undefined)[]): Clock | undefined {
  let found: Clock | undefined;
  for (const clock of clocks)
    if (clock !== undefined && (found === undefined || CLOCK_RANK[clock] > CLOCK_RANK[found])) found = clock;
  return found;
}

/**
 * Whether asserting `lower` below `upper` bounds host time: a duration under any bound, or a reading under a deadline.
 * A duration over a bound (the wait really waited) and a reading below a product timestamp (ordering) do not.
 */
function boundsHostTime(lower: Clock | undefined, upper: Clock | undefined): boolean {
  return lower === 'duration' || (lower === 'instant' && upper === 'deadline');
}

function literalNumber(node: ts.Expression): number | undefined {
  if (ts.isNumericLiteral(node)) return Number(node.text);
  if (ts.isParenthesizedExpression(node)) return literalNumber(node.expression);
  if (ts.isPrefixUnaryExpression(node) && node.operator === ts.SyntaxKind.MinusToken) {
    const value = literalNumber(node.operand);
    return value === undefined ? undefined : -value;
  }
  if (!ts.isBinaryExpression(node)) return undefined;
  const [left, right] = [literalNumber(node.left), literalNumber(node.right)];
  if (left === undefined || right === undefined) return undefined;
  switch (node.operatorToken.kind) {
    case ts.SyntaxKind.AsteriskToken:
      return left * right;
    case ts.SyntaxKind.SlashToken:
      return left / right;
    case ts.SyntaxKind.PlusToken:
      return left + right;
    case ts.SyntaxKind.MinusToken:
      return left - right;
    default:
      return undefined;
  }
}

const CLOCK_READ = /^(?:Date\.now|performance\.now|Bun\.nanoseconds|process\.hrtime\.bigint)$/u;
const COMPARISON: Partial<Record<ts.SyntaxKind, 'below' | 'above'>> = {
  [ts.SyntaxKind.LessThanToken]: 'below',
  [ts.SyntaxKind.LessThanEqualsToken]: 'below',
  [ts.SyntaxKind.GreaterThanToken]: 'above',
  [ts.SyntaxKind.GreaterThanEqualsToken]: 'above',
};
const ORDER_MATCHER = /^toBe(Less|Greater)Than(?:OrEqual)?$/u;
const TRUTH_MATCHER = /^to(?:Be|Equal|StrictEqual)$/u;
const CASE_CALLEE = /^(?:test|it)(?:\.\w+)*$/u;

/** Class A and class D sites of one TypeScript/JavaScript test file. */
function jsClockSites(parsed: ts.SourceFile): Site[] {
  const sites: Site[] = [];
  const site = (node: ts.Node, kind: ClockOracleKind): void => {
    sites.push({ kind, offset: node.getStart(parsed), text: node.getText(parsed) });
  };
  // Lexical scopes of the names bound to a host clock value; an inner declaration or parameter shadows an outer one.
  const scopes: Map<string, Clock | undefined>[] = [new Map()];
  const lookup = (name: string): Clock | undefined => {
    for (let index = scopes.length - 1; index >= 0; index -= 1) {
      const scope = scopes[index];
      if (scope?.has(name)) return scope.get(name);
    }
    return undefined;
  };
  const clockOf = (node: ts.Expression): Clock | undefined => {
    if (
      ts.isParenthesizedExpression(node) ||
      ts.isAsExpression(node) ||
      ts.isNonNullExpression(node) ||
      ts.isSatisfiesExpression(node) ||
      ts.isTypeAssertionExpression(node) ||
      ts.isElementAccessExpression(node)
    )
      return clockOf(node.expression);
    if (ts.isIdentifier(node)) return lookup(node.text);
    if (ts.isNewExpression(node))
      return ts.isIdentifier(node.expression) && node.expression.text === 'Date' && !node.arguments?.length
        ? 'instant'
        : undefined;
    if (ts.isPrefixUnaryExpression(node)) return clockOf(node.operand);
    if (ts.isConditionalExpression(node)) return strongest([clockOf(node.whenTrue), clockOf(node.whenFalse)]);
    if (ts.isCallExpression(node)) {
      const callee = node.expression.getText(parsed);
      if (CLOCK_READ.test(callee)) return 'instant';
      if (callee === 'process.hrtime') return node.arguments.length === 0 ? 'instant' : 'duration';
      if (ts.isPropertyAccessExpression(node.expression) && /^(?:getTime|valueOf)$/u.test(node.expression.name.text))
        return clockOf(node.expression.expression);
      if (/^(?:Math\.\w+|Number|BigInt)$/u.test(callee)) return strongest(node.arguments.map(clockOf));
      return undefined;
    }
    if (!ts.isBinaryExpression(node)) return undefined;
    const [left, right] = [clockOf(node.left), clockOf(node.right)];
    switch (node.operatorToken.kind) {
      case ts.SyntaxKind.MinusToken:
        if (left === 'duration' || right === 'duration') return 'duration';
        if (left === undefined && right === undefined) return undefined;
        // A reading less a fixed span is still a moment; a reading less anything else is time passed.
        return right === undefined && literalNumber(node.right) !== undefined ? 'deadline' : 'duration';
      case ts.SyntaxKind.PlusToken:
        if (left === 'duration' || right === 'duration') return 'duration';
        return left === undefined && right === undefined ? undefined : 'deadline';
      case ts.SyntaxKind.AsteriskToken:
      case ts.SyntaxKind.SlashToken:
      case ts.SyntaxKind.PercentToken:
        return strongest([left, right]);
      case ts.SyntaxKind.CommaToken:
        return right;
      default:
        return undefined;
    }
  };
  // Whether an assertion over `node` asserts a comparison that bounds host time; `negated` when it asserts the
  // comparison's negation. Nested functions are not the assertion's own comparison.
  const comparesHostTime = (node: ts.Node, negated: boolean): boolean => {
    if (ts.isFunctionLike(node)) return false;
    if (ts.isBinaryExpression(node)) {
      const direction = COMPARISON[node.operatorToken.kind];
      if (direction !== undefined) {
        const [lower, upper] = (direction === 'below') !== negated ? [node.left, node.right] : [node.right, node.left];
        return boundsHostTime(clockOf(lower), clockOf(upper));
      }
    }
    if (ts.isPrefixUnaryExpression(node) && node.operator === ts.SyntaxKind.ExclamationToken)
      return comparesHostTime(node.operand, !negated);
    return ts.forEachChild(node, (child) => comparesHostTime(child, negated) || undefined) ?? false;
  };
  const expectation = (
    call: ts.CallExpression,
  ): { subject: ts.Expression; negated: boolean; matcher: string } | undefined => {
    if (!ts.isPropertyAccessExpression(call.expression)) return undefined;
    let negated = false;
    let receiver = call.expression.expression;
    while (ts.isPropertyAccessExpression(receiver) && receiver.name.text === 'not') {
      negated = !negated;
      receiver = receiver.expression;
    }
    if (!ts.isCallExpression(receiver) || !ts.isIdentifier(receiver.expression)) return undefined;
    const subject = receiver.arguments[0];
    if (receiver.expression.text !== 'expect' || subject === undefined) return undefined;
    return { subject, negated, matcher: call.expression.name.text };
  };
  const assertsHostTime = (call: ts.CallExpression): boolean => {
    const expected = expectation(call);
    const argument = call.arguments[0];
    if (expected === undefined) {
      let root: ts.Expression = call.expression;
      while (ts.isPropertyAccessExpression(root)) root = root.expression;
      return (
        ts.isIdentifier(root) &&
        /^(?:assert\w*|ok)$/u.test(root.text) &&
        call.arguments.some((each) => comparesHostTime(each, false))
      );
    }
    const { subject, negated, matcher } = expected;
    const order = ORDER_MATCHER.exec(matcher);
    if (order !== null && argument !== undefined) {
      const [lower, upper] = (order[1] === 'Less') !== negated ? [subject, argument] : [argument, subject];
      return boundsHostTime(clockOf(lower), clockOf(upper));
    }
    if (matcher === 'toBeCloseTo' && argument !== undefined)
      return clockOf(subject) !== undefined || clockOf(argument) !== undefined;
    const truth =
      matcher === 'toBeTruthy' || matcher === 'toBeTrue'
        ? true
        : matcher === 'toBeFalsy' || matcher === 'toBeFalse'
          ? false
          : TRUTH_MATCHER.test(matcher) && argument?.kind === ts.SyntaxKind.TrueKeyword
            ? true
            : TRUTH_MATCHER.test(matcher) && argument?.kind === ts.SyntaxKind.FalseKeyword
              ? false
              : undefined;
    return truth !== undefined && comparesHostTime(subject, truth === negated);
  };

  // Top-level string constants a case may hand a child as its source (`const SLOW_TEST = \`…setTimeout(r, 5000)…\``).
  const constants = new Map<string, string>();
  for (const statement of parsed.statements) {
    if (!ts.isVariableStatement(statement)) continue;
    for (const declaration of statement.declarationList.declarations) {
      const value = declaration.initializer;
      if (ts.isIdentifier(declaration.name) && value !== undefined && ts.isStringLiteralLike(value))
        constants.set(declaration.name.text, value.getText(parsed));
      if (ts.isIdentifier(declaration.name) && value !== undefined && ts.isTemplateExpression(value))
        constants.set(declaration.name.text, value.getText(parsed));
    }
  }
  const timed = new Map<ts.Node, boolean>();
  const timedStandIn = (scope: ts.Node): boolean => {
    const known = timed.get(scope);
    if (known !== undefined) return known;
    const visit = (node: ts.Node): boolean => {
      if (ts.isStringLiteralLike(node) || ts.isTemplateExpression(node)) {
        if (endsByTimer(node.getText(parsed))) return true;
      } else if (ts.isCallExpression(node)) {
        if (TIMER_CALL.test(`${node.expression.getText(parsed)}(`) && endsByTimer(node.getText(parsed))) return true;
      } else if (ts.isIdentifier(node)) {
        const source = constants.get(node.text);
        if (source !== undefined && endsByTimer(source)) return true;
      }
      return ts.forEachChild(node, (child) => visit(child) || undefined) ?? false;
    };
    const answer = visit(scope);
    timed.set(scope, answer);
    return answer;
  };
  // The case a bound belongs to: its enclosing `test`/`it` call, else its function, else the file.
  const owner = (node: ts.Node): ts.Node => {
    let fn: ts.Node | undefined;
    for (let up = node.parent; up !== undefined; up = up.parent) {
      if (ts.isCallExpression(up) && CASE_CALLEE.test(up.expression.getText(parsed))) return up;
      if (fn === undefined && ts.isFunctionLike(up)) fn = up;
    }
    return fn ?? parsed;
  };
  const bounds: ts.Node[] = [];
  const runnerOptions = new Set<ts.Node>();

  const visit = (node: ts.Node): void => {
    const opens = node !== parsed && (ts.isFunctionLike(node) || ts.isBlock(node));
    if (opens) scopes.push(new Map());
    if (ts.isFunctionLike(node))
      for (const parameter of node.parameters)
        if (ts.isIdentifier(parameter.name)) scopes.at(-1)?.set(parameter.name.text, undefined);
    if (ts.isCallExpression(node) && assertsHostTime(node)) site(node, 'clock-delta');
    if (ts.isCallExpression(node)) {
      // `test(name, fn, 60_000)`, `test(name, fn, { timeout })`, `test.each(table)(name, fn, n)`, `setDefaultTimeout(n)`.
      const callee = node.expression;
      const runner =
        CASE_CALLEE.test(callee.getText(parsed)) ||
        (ts.isCallExpression(callee) && /^(?:test|it)(?:\.\w+)*\.each$/u.test(callee.expression.getText(parsed)));
      const body = node.arguments.findIndex((each) => ts.isArrowFunction(each) || ts.isFunctionExpression(each));
      if (runner && body !== -1)
        for (const option of node.arguments.slice(body + 1))
          if (
            literalNumber(option) !== undefined ||
            (ts.isObjectLiteralExpression(option) &&
              option.properties.some((each) => each.name?.getText(parsed) === 'timeout'))
          ) {
            runnerOptions.add(option);
            site(option, 'runner-deadline');
          }
      if (/^(?:setDefaultTimeout|jest\.setTimeout)$/u.test(callee.getText(parsed))) site(node, 'runner-deadline');
    }
    // A runner's own `{ timeout }` is a runner deadline above, never also a product bound.
    if ((ts.isPropertyAssignment(node) && !runnerOptions.has(node.parent)) || ts.isVariableDeclaration(node)) {
      const name =
        ts.isIdentifier(node.name) || ts.isStringLiteral(node.name) ? node.name.text : node.name.getText(parsed);
      const value = node.initializer === undefined ? undefined : literalNumber(node.initializer);
      if (boundName(name) && value !== undefined && decides(name, value)) bounds.push(node);
    }
    if (ts.isStringLiteralLike(node) || ts.isTemplateExpression(node)) {
      for (const flag of node.getText(parsed).matchAll(FLAG_BOUND)) {
        const value = Number((flag[1] ?? '').replaceAll('_', ''));
        if (decides(flag[2] === 's' ? 'seconds' : 'ms', value)) bounds.push(node);
      }
    }
    ts.forEachChild(node, visit);
    // A declaration binds after its initializer is read; an assignment rebinds the nearest declaration.
    if (ts.isVariableDeclaration(node) && ts.isIdentifier(node.name))
      scopes.at(-1)?.set(node.name.text, node.initializer === undefined ? undefined : clockOf(node.initializer));
    if (
      ts.isBinaryExpression(node) &&
      node.operatorToken.kind === ts.SyntaxKind.EqualsToken &&
      ts.isIdentifier(node.left)
    ) {
      const name = node.left.text;
      for (let index = scopes.length - 1; index >= 0; index -= 1) {
        const scope = scopes[index];
        if (scope?.has(name)) {
          scope.set(name, clockOf(node.right));
          break;
        }
      }
    }
    if (opens) scopes.pop();
  };
  visit(parsed);
  for (const bound of new Set(bounds)) if (timedStandIn(owner(bound))) site(bound, 'clocked-bound');
  return sites;
}

/**
 * Blank comments and, with `strings`, quoted text, keeping every byte position. A single quote never spans a line, so
 * a Rust lifetime (`&'a str`) masks at most the rest of its own line.
 */
function maskNative(source: string, strings: boolean): string {
  return source.replace(
    /\/\/[^\n]*|\/\*[\s\S]*?\*\/|"""[\s\S]*?"""|'''[\s\S]*?'''|\br(#*)"[\s\S]*?"\1|"(?:\\[\s\S]|[^"\\])*"|'(?:\\.|[^'\\\n])*'/gu,
    (text) => (strings || text.startsWith('/') ? text.replace(/[^\r\n]/g, ' ') : text),
  );
}

/** Where a native file's test code starts: Rust src/ files are examined only inside their embedded test module. */
function testCodeStart(masked: string, file: string): number | undefined {
  if (extname(file) !== '.rs' || TEST_PATH.test(file)) return 0;
  return /#\[cfg\(test\)\][\s\S]*?\bmod\s+tests\s*\{/u.exec(masked)?.index;
}

/** The index closing the group opened at `open` in comment/string-masked text. */
function closingIndex(masked: string, open: number): number {
  let depth = 0;
  for (let index = open; index < masked.length; index += 1) {
    const char = masked[index];
    if (char === '(' || char === '[' || char === '{') depth += 1;
    else if (char === ')' || char === ']' || char === '}') {
      depth -= 1;
      if (depth === 0) return index;
    }
  }
  return masked.length;
}

/** The host clock value a Rust expression holds, given the `let` bindings of its fn. */
function rustClock(expression: string, bindings: ReadonlyMap<string, Clock | undefined>): Clock | undefined {
  // `elapsed`/`duration_since` on a host reading is time passed; on an injected clock (`clock.elapsed()`) it is not,
  // and since `UNIX_EPOCH` it is a timestamp, still a reading.
  for (const call of expression.matchAll(
    /(\b[a-z_]\w*|\b(?:Instant|SystemTime)::now\s*\(\s*\))\s*\.\s*(?:elapsed|(?:saturating_|checked_)?duration_since)\s*\(/gu,
  )) {
    if (/^\s*(?:std::time::)?(?:SystemTime::)?UNIX_EPOCH\b/u.test(expression.slice(call.index + call[0].length)))
      continue;
    const receiver = call[1] ?? '';
    const held = bindings.get(receiver);
    if (receiver.endsWith(')') || held === 'instant' || held === 'deadline') return 'duration';
  }
  const clocks = (text: string): Clock | undefined =>
    strongest([
      /\b(?:Instant|SystemTime)::now\s*\(\s*\)/u.test(text) ? 'instant' : undefined,
      ...Array.from(text.matchAll(/\b[a-z_]\w*\b/gu), (name) => bindings.get(name[0])),
    ]);
  const base = clocks(expression);
  if (base === undefined || base === 'duration') return base;
  const minus = /[\w)\]]\s*-(?!>)/u.exec(expression);
  if (minus !== null) {
    // A reading less a reading is time passed; a reading less a `Duration` is still a moment.
    const right = clocks(expression.slice(minus.index + minus[0].length));
    return right === 'instant' || right === 'deadline' ? 'duration' : 'deadline';
  }
  return /[\w)\]]\s*\+/u.test(expression) ? 'deadline' : base;
}

/** The first top-level comparison of a Rust assertion's condition, as its asserted lower and upper side. */
function rustComparison(condition: string): { lower: string; upper: string } | undefined {
  let depth = 0;
  let angles = 0;
  for (let index = 0; index < condition.length; index += 1) {
    const char = condition[index] ?? '';
    const next = condition[index + 1] ?? '';
    const previous = condition[index - 1] ?? '';
    if ('([{'.includes(char)) depth += 1;
    else if (')]}'.includes(char)) depth -= 1;
    if (depth !== 0) continue;
    if (char === '<' && condition.slice(index - 2, index) === '::') {
      angles += 1;
      continue;
    }
    if (char === '>' && angles > 0) {
      angles -= 1;
      continue;
    }
    if (char !== '<' && char !== '>') continue;
    if (next === char || previous === char || previous === '-' || previous === '=') continue;
    const width = next === '=' ? 2 : 1;
    const [left, right] = [condition.slice(0, index), condition.slice(index + width)];
    return char === '<' ? { lower: left, upper: right } : { lower: right, upper: left };
  }
  return undefined;
}

/** The text before a group's first top-level comma: an assertion's condition, a call's first argument. */
function firstArgument(group: string): string {
  let depth = 0;
  for (let index = 0; index < group.length; index += 1) {
    const char = group[index] ?? '';
    if ('([{'.includes(char)) depth += 1;
    else if (')]}'.includes(char)) depth -= 1;
    else if (char === ',' && depth === 0) return group.slice(0, index);
  }
  return group;
}

/** Milliseconds of a `Duration::from_*` unit. */
const DURATION_MS: Record<string, number> = { secs: 1000, millis: 1, micros: 1 / 1000, nanos: 1 / 1_000_000 };

/** Runner deadlines' Rust counterpart, and class A and class D sites, of one Rust file inside its test code. */
function rustClockSites(source: string, file: string): Site[] {
  const masked = maskNative(source, true);
  const begin = testCodeStart(masked, file);
  if (begin === undefined) return [];
  const commentless = maskNative(source, false);
  const sites: Site[] = [];
  for (const declaration of masked.slice(begin).matchAll(/\bfn\s+\w+/gu)) {
    const start = begin + declaration.index;
    const open = masked.indexOf('{', start);
    if (open === -1) continue;
    const end = closingIndex(masked, open);
    const body = masked.slice(open, end + 1);
    // Paused tokio time is virtual: its `Instant::now()`, `sleep` and `timeout` advance with the test, not the host.
    const attributes = masked.slice(Math.max(begin, start - 300), start);
    if (/\btokio::time::pause\s*\(/u.test(body) || /\bstart_paused\s*=\s*true\b/u.test(attributes)) continue;
    const bindings = new Map<string, Clock | undefined>();
    for (const binding of body.matchAll(/\blet\s+(?:mut\s+)?([a-z_]\w*)\s*(?::[^=;]*)?=([^;]*);/gu))
      bindings.set(binding[1] ?? '', rustClock(binding[2] ?? '', bindings));
    // A nested fn is examined as itself and again as part of its enclosing fn; the return keeps one site per offset.
    // A deadline the test owns: a receive, park or `timeout` bounded below the backstop whose expiry fails the case.
    // A quiet window (`timeout(d, f).await.is_err()`) asserts that nothing happened and passes.
    // Anchored at the name (`timeout`, not `tokio::time::`), where a clocked-bound reading of it would start too.
    for (const call of body.matchAll(/\b(?:recv_timeout|park_timeout|timeout)\s*\(/gu)) {
      const opening = open + call.index + call[0].length - 1;
      const closing = closingIndex(masked, opening);
      const span =
        /^\s*(?:std::time::|core::time::)?Duration::from_(secs|millis|micros|nanos)(?:_f32|_f64)?\s*\(\s*(\d[\d_]*(?:\.\d+)?)\s*\)\s*$/u.exec(
          firstArgument(masked.slice(opening + 1, closing)),
        );
      const ms =
        span === null ? undefined : Number((span[2] ?? '').replaceAll('_', '')) * (DURATION_MS[span[1] ?? ''] ?? 1);
      if (ms === undefined || !decides('ms', ms)) continue;
      if (!/^\s*(?:\.await)?\s*\.(?:expect|unwrap|unwrap_or_else)\b/u.test(masked.slice(closing + 1, closing + 200)))
        continue;
      sites.push({
        kind: 'test-deadline',
        offset: open + call.index,
        text: source.slice(open + call.index, closing + 1),
      });
    }
    for (const assertion of body.matchAll(/\b(?:debug_|prop_)?assert!\s*\(/gu)) {
      const opening = open + assertion.index + assertion[0].length - 1;
      const closing = closingIndex(masked, opening);
      const compared = rustComparison(firstArgument(masked.slice(opening + 1, closing)));
      if (
        compared !== undefined &&
        boundsHostTime(rustClock(compared.lower, bindings), rustClock(compared.upper, bindings))
      )
        sites.push({
          kind: 'clock-delta',
          offset: open + assertion.index,
          text: source.slice(open + assertion.index, closing + 1),
        });
    }
    if (!endsByTimer(commentless.slice(open, end + 1))) continue;
    for (const bound of body.matchAll(
      /\b([a-z_]\w*)\s*(?::(?!:)|=(?![=>])|\()\s*(?:Duration::from_(secs|millis|micros|nanos)(?:_f32|_f64)?\s*\(\s*)?(\d[\d_]*(?:\.\d+)?)/giu,
    )) {
      const name = bound[1] ?? '';
      const value = Number((bound[3] ?? '').replaceAll('_', ''));
      const unit = bound[2];
      const decided = unit === undefined ? decides(name, value) : decides('ms', value * (DURATION_MS[unit] ?? 1));
      if (!boundName(name) || !decided) continue;
      const offset = open + bound.index;
      sites.push({ kind: 'clocked-bound', offset, text: source.slice(offset, offset + bound[0].length) });
    }
  }
  // One site per offset, the first found: a test-owned deadline is not also reported as the bound it carries.
  const kept = new Map<number, Site>();
  for (const site of sites) if (!kept.has(site.offset)) kept.set(site.offset, site);
  return [...kept.values()];
}

/** Class A and D sites and runner deadlines of one test file, or undefined for a file holding no test code. */
function clockSites(file: string, text: string): Site[] | undefined {
  if (JS_EXTENSION.test(extname(file)))
    return TEST_PATH.test(file)
      ? jsClockSites(ts.createSourceFile(file, text, ts.ScriptTarget.Latest, true))
      : undefined;
  return extname(file) === '.rs' ? rustClockSites(text, file) : undefined;
}

export function findClockOracles(
  root: string,
  files: readonly string[],
  changes?: ReadonlyMap<string, readonly ChangeRange[]>,
): ClockOracle[] {
  const found: ClockOracle[] = [];
  for (const file of files) {
    const path = join(root, file);
    if (!existsSync(path)) continue;
    const text = readFileSync(path, 'utf8');
    const sites = clockSites(file, text);
    if (sites === undefined || sites.length === 0) continue;
    const ranges = changes?.get(file);
    const cases = ranges === undefined ? [] : (testCases(file, text) ?? []);
    for (const site of sites.sort((left, right) => left.offset - right.offset)) {
      const prefix = text.slice(0, site.offset);
      const line = prefix.split('\n').length;
      if (ranges !== undefined) {
        const owner = cases.find((test) => test.start <= line && test.end >= line);
        const [start, end] = owner === undefined ? [line, line] : [owner.start, owner.end];
        if (!ranges.some((range) => range.start <= end && range.end >= start)) continue;
      }
      found.push({ kind: site.kind, file, line, column: site.offset - prefix.lastIndexOf('\n'), text: site.text });
    }
  }
  return found;
}

export function formatClockOracle(oracle: ClockOracle): string {
  return (
    `error[${TEST_CLOCK_ORACLE.id}]: ${TEST_CLOCK_ORACLE.message[oracle.kind]}: ${oracle.text}\n` +
    `  --> ${oracle.file}:${oracle.line}:${oracle.column}\n` +
    `  = help: ${TEST_CLOCK_ORACLE.help[oracle.kind]}`
  );
}
