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

/** Only complete, direct chains have a known lifecycle. Shell wrappers remain bounded from spawn. */
export function withRunnerLifecycle(command: string): { command: string; runners: number } | null {
  // Redirections could replace the private descriptor; newlines/backticks/escapes need a real shell parser.
  if (/[<>\n\r`\\]/.test(command)) return null;
  let rest = command.trim();
  const parts: string[] = [];
  let runners = 0;
  const hook = fileURLToPath(
    new URL(import.meta.url.endsWith('.ts') ? './runner-hook.ts' : './runner-hook.js', import.meta.url),
  );
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
    // The hook owns an inherited duplex descriptor, which Bun's net.Socket({fd}) does not adopt.
    const runtime = process.versions.bun === undefined ? process.execPath : 'node';
    parts.push(`${shellWord(runtime)} ${shellWord(hook)} ${runners} ${kind} ${runner}`);
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
