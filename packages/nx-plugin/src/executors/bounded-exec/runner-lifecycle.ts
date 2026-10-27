import { fileURLToPath } from 'node:url';
import { shellWord } from '../../runtime-input.js';
import { invocationSpan, withInvocationFlag } from './runner-sizing.js';
import { GO_TEST } from './verdict.js';

const CARGO_TEST = /^cargo(?:\s+(?:-\S+|\+\S+))*\s+test(?=\s|$)/;
const MESSAGE_FORMAT = /(?<![\w-])--message-format(?:=|\s+)(?:"[^"]*"|'[^']*'|[^\s]+)/g;
export type RunnerPhase = 'build' | 'active' | 'terminal';
export interface RunnerPhaseEvent {
  id: number;
  phase: RunnerPhase;
}

/** One segment's supervisor invocation: the runtime that adopts the inherited duplex descriptor, then the hook, its id and kind. */
function hookInvocation(id: number, kind: string, runner: string): string {
  const hook = fileURLToPath(
    new URL(import.meta.url.endsWith('.ts') ? './runner-hook.ts' : './runner-hook.js', import.meta.url),
  );
  // The hook owns an inherited duplex descriptor, which Bun's net.Socket({fd}) does not adopt.
  const runtime = process.versions.bun === undefined ? process.execPath : 'node';
  return `${shellWord(runtime)} ${shellWord(hook)} ${id} ${kind} ${runner}`;
}

/** Only complete, direct chains have a known lifecycle. Shell wrappers remain bounded from spawn. */
export function withRunnerLifecycle(command: string, firstId = 0): { command: string; runners: number } | null {
  // Redirections could replace the private descriptor; newlines/backticks/escapes need a real shell parser.
  if (/[<>\n\r`\\]/.test(command)) return null;
  let rest = command.trim();
  const parts: string[] = [];
  let runners = 0;
  while (rest !== '') {
    const kind = rest.search(GO_TEST) === 0 ? 'go' : CARGO_TEST.test(rest) ? 'cargo' : null;
    if (kind === null) return null;
    const invocation = kind === 'go' ? GO_TEST : CARGO_TEST;
    const { argsEnd } = invocationSpan(rest, invocation);
    let runner = rest.slice(0, argsEnd).trimEnd();
    if (kind === 'cargo') {
      // Only Cargo's own arguments, never those after the libtest separator, select build reporting.
      const separator = /\s--(?:\s|$)/.exec(runner);
      const end = separator?.index ?? runner.length;
      runner =
        withInvocationFlag(
          runner.slice(0, end),
          invocation,
          MESSAGE_FORMAT,
          '--message-format=json-render-diagnostics',
          true,
        ) + runner.slice(end);
    }
    parts.push(hookInvocation(firstId + runners, kind, runner));
    runners += 1;
    rest = rest.slice(argsEnd).trimStart();
    if (rest === '') break;
    const separator = /^(?:&&|\|\||;)\s*/.exec(rest);
    if (separator === null) return null;
    parts.push(separator[0].trim());
    rest = rest.slice(separator[0].length);
    if (rest === '') return null;
  }
  return runners === 0 ? null : { command: parts.join(' '), runners };
}

/**
 * `command` preceded by its compiler-fixture producer (`fixtureCommand`, schema.ts): segment 0 supervises the
 * fixture as a build interval for its whole life, and the command follows under `&&`, so a fixture failure ends
 * the run before any test starts. A direct `cargo test`/`go test` chain keeps its own phases behind a brace
 * group, so a `||` inside it cannot run a leg the fixture refused. Any other command runs as one `run`
 * segment: active from its spawn, its output on the progress bound exactly as an unhooked command's is, with no
 * descendant holding the private descriptor. The fixture is arbitrary shell, so it rides under `/bin/sh -c`;
 * this lifecycle is POSIX-only, as the channel itself is.
 */
export function withFixtureLifecycle(fixtureCommand: string, command: string): { command: string; runners: number } {
  const fixture = hookInvocation(0, 'build', `/bin/sh -c ${shellWord(fixtureCommand)}`);
  const main = withRunnerLifecycle(command, 1);
  if (main !== null) {
    return { command: `${fixture} && { ${main.command}; }`, runners: main.runners + 1 };
  }
  return { command: `${fixture} && ${hookInvocation(1, 'run', `/bin/sh -c ${shellWord(command)}`)}`, runners: 2 };
}

/** A private supervisor channel, not runner output, grants authority to pause the test clock. */
export function createRunnerLifecycle(runners: number) {
  let current = 0;
  let phase: RunnerPhase = 'active'; // Shell and supervisor startup are bounded until their build declaration.
  let failure: string | null = null;
  return {
    isTesting: (): boolean => failure !== null || phase === 'active' || (phase === 'terminal' && current < runners - 1),
    error: (): string | null => failure,
    finished: (): boolean => phase === 'terminal' && failure === null,
    accept(line: string): RunnerPhaseEvent | null {
      if (failure !== null) return null;
      let event: unknown;
      try {
        event = JSON.parse(line);
      } catch (error) {
        if (!(error instanceof SyntaxError)) throw error;
        failure = 'invalid JSON on the runner lifecycle channel';
        return null;
      }
      if (
        typeof event !== 'object' ||
        event === null ||
        !('id' in event) ||
        !('phase' in event) ||
        typeof event.id !== 'number' ||
        !Number.isSafeInteger(event.id) ||
        event.id < current ||
        event.id >= runners ||
        (event.id === current && phase === 'terminal') ||
        (event.phase !== 'build' && event.phase !== 'active' && event.phase !== 'terminal')
      ) {
        failure = 'invalid or retired invocation on the runner lifecycle channel';
        return null;
      }
      // && and || may skip invocations. A new supervisor can advance only after the previous one ended.
      if (event.id !== current && phase !== 'terminal') {
        failure = 'overlapping invocations on a serial runner lifecycle channel';
        return null;
      }
      current = event.id;
      phase = event.phase;
      return { id: current, phase };
    },
  };
}
