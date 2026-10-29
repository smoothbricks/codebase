import { type ChildProcessByStdio, spawn } from 'node:child_process';
import { resolve } from 'node:path';
import type { Readable } from 'node:stream';

import { type AllVerdictsOptions, ESLINT_SOURCES_TOKEN } from './schema.js';

/**
 * A command's verdict. Only a command that exited reached one. A command a signal ended, one that never started and
 * one still running when the run itself was signalled are unjudged: never a pass, and never left out of the report.
 */
export type Verdict =
  | { readonly kind: 'pass' }
  | { readonly kind: 'fail'; readonly code: number }
  | { readonly kind: 'unjudged'; readonly cause: string };

export interface CommandVerdict {
  readonly command: string;
  readonly verdict: Verdict;
}

/** The verdict of a process that closed with exit `code`, or, when `code` is null, was ended by `signal`. */
export function closedVerdict(code: number | null, signal: NodeJS.Signals | null): Verdict {
  if (code === 0) return { kind: 'pass' };
  if (code !== null) return { kind: 'fail', code };
  return { kind: 'unjudged', cause: `${signal ?? 'an unreported signal'} ended it before it gave a verdict` };
}

/** How much of a command the verdict list quotes; the command's output block names it whole. */
const LABEL_LENGTH = 120;

function label(command: string): string {
  return command.length <= LABEL_LENGTH ? command : `${command.slice(0, LABEL_LENGTH - 1)}…`;
}

function tag(verdict: Verdict): string {
  switch (verdict.kind) {
    case 'pass':
      return 'PASS';
    case 'fail':
      return `FAIL (exit ${verdict.code})`;
    case 'unjudged':
      return 'UNJUDGED';
  }
}

/** Every command's verdict, in the order the target lists the commands, under a count of each verdict. */
export function verdictReport(verdicts: readonly CommandVerdict[]): string {
  const count = (kind: Verdict['kind']): number => verdicts.filter(({ verdict }) => verdict.kind === kind).length;
  const lines = verdicts.map(({ command, verdict }) => {
    const line = `  ${tag(verdict).padEnd(15)} ${label(command)}`;
    return verdict.kind === 'unjudged' ? `${line} (${verdict.cause})` : line;
  });
  const counts = `${count('pass')} passed, ${count('fail')} failed, ${count('unjudged')} unjudged`;
  return `\nall-verdicts: ${counts} of ${verdicts.length}\n${lines.join('\n')}\n`;
}

/** On POSIX each command leads a process group of its own, so a signal reaches every process the command started. */
const OWN_GROUP = process.platform !== 'win32';

function signalCommand(pid: number, signal: NodeJS.Signals): void {
  try {
    process.kill(OWN_GROUP ? -pid : pid, signal);
  } catch (error) {
    // ESRCH: every process of the command has already exited.
    if (!(error instanceof Error && 'code' in error && error.code === 'ESRCH')) throw error;
  }
}

const TERMINATION_SIGNALS = ['SIGINT', 'SIGTERM', 'SIGHUP'] as const satisfies readonly NodeJS.Signals[];

interface Run {
  readonly command: string;
  readonly output: string[];
  pid: number | undefined;
  verdict: Verdict | null;
}

/** A command's output as one block, headed by the command the way Nx's run-commands heads it. */
function block(run: Run): string {
  return `\n> ${run.command}\n\n${run.output.join('')}`;
}

/** Starts `run`'s command and settles with its verdict once it closes; its output prints as one block then. */
function judge(run: Run, cwd: string): Promise<Verdict> {
  return new Promise((settle) => {
    const close = (verdict: Verdict): void => {
      // A run the signal handler already judged keeps that verdict: the run is ending by the signal.
      if (run.verdict !== null) return;
      run.verdict = verdict;
      process.stdout.write(block(run));
      settle(verdict);
    };
    let child: ChildProcessByStdio<null, Readable, Readable>;
    try {
      child = spawn(run.command, {
        cwd,
        shell: true,
        stdio: ['ignore', 'pipe', 'pipe'],
        detached: OWN_GROUP,
        windowsHide: true,
      });
    } catch (error) {
      close({ kind: 'unjudged', cause: `it never ran: ${error instanceof Error ? error.message : String(error)}` });
      return;
    }
    run.pid = child.pid;
    for (const stream of [child.stdout, child.stderr]) {
      stream.setEncoding('utf8');
      stream.on('data', (text: string) => run.output.push(text));
    }
    child.once('error', (error) => close({ kind: 'unjudged', cause: `it never ran: ${error.message}` }));
    child.once('close', (code, signal) => close(closedVerdict(code, signal)));
  });
}

/** A command as it will run: the text the shell gets, or why it needs no run. */
interface Planned {
  readonly command: string;
  readonly noRun?: string;
}

/**
 * The commands as they run now. A command that names {@link ESLINT_SOURCES_TOKEN} gets the files ESLint is handed for
 * `eslintProjectRoot` as the filesystem holds them at this moment, read through a fresh workspace file index: a graph the
 * daemon serves past a deletion or an addition its watcher never reported still lints the files that exist. A project
 * with none has nothing to lint, and no ESLint process starts for it.
 */
async function planCommands(options: AllVerdictsOptions, root: string): Promise<readonly Planned[]> {
  if (!options.commands.some((command) => command.includes(ESLINT_SOURCES_TOKEN))) {
    return options.commands.map((command) => ({ command }));
  }
  const projectRoot = options.eslintProjectRoot;
  if (projectRoot === undefined) {
    throw new Error(
      `a command names ${ESLINT_SOURCES_TOKEN}, and the target names no eslintProjectRoot to resolve it for`,
    );
  }
  const { expandEslintSources, freshEslintSources } = await import('../../eslint-sources.js');
  const { files } = freshEslintSources(root, projectRoot);
  return options.commands.map((command): Planned => {
    if (!command.includes(ESLINT_SOURCES_TOKEN)) return { command };
    if (files.length === 0) return { command, noRun: `no ESLint sources under ${projectRoot}: nothing to lint` };
    return { command: expandEslintSources(command, files) };
  });
}

/**
 * Starts every command at once and lets each reach its own verdict. Nx's run-commands cannot: in parallel it terminates
 * the other commands once one fails, and serially it never starts the ones after a failure, so one tool's errors hide
 * another's until a later run. Here no command ends another. Each command's output prints as one block when it closes,
 * then every command's verdict. A signal to the run ends every command's process group, reports each command still
 * running UNJUDGED, and ends the run by the same signal. The run succeeds only when every command passed.
 * A command that names the ESLint sources placeholder is resolved first, and the verdicts name it as it ran.
 */
export default async function allVerdictsExecutor(
  options: AllVerdictsOptions,
  context: { readonly root: string },
): Promise<{ success: boolean }> {
  if (options.commands.length === 0) {
    process.stderr.write('all-verdicts: no commands: an empty set of checks cannot fail, so it cannot pass\n');
    return { success: false };
  }
  const cwd = resolve(context.root, options.cwd);
  let planned: readonly Planned[];
  try {
    planned = await planCommands(options, context.root);
  } catch (error) {
    process.stderr.write(
      `all-verdicts: cannot resolve the commands to run: ${error instanceof Error ? error.message : String(error)}\n`,
    );
    return { success: false };
  }
  const runs = planned.map(
    ({ command, noRun }): Run => ({
      command,
      output: noRun === undefined ? [] : [`${noRun}\n`],
      pid: undefined,
      verdict: noRun === undefined ? null : { kind: 'pass' },
    }),
  );
  const onExit = (): void => {
    for (const run of runs) {
      if (run.verdict === null && run.pid !== undefined) signalCommand(run.pid, 'SIGTERM');
    }
  };
  const onSignal = (signal: NodeJS.Signals): void => {
    detach();
    const verdicts = runs.map((run): CommandVerdict => {
      if (run.verdict === null) {
        if (run.pid !== undefined) signalCommand(run.pid, signal);
        run.verdict = { kind: 'unjudged', cause: `the run received ${signal} before it gave a verdict` };
        process.stdout.write(block(run));
      }
      return { command: run.command, verdict: run.verdict };
    });
    process.stdout.write(verdictReport(verdicts), () => process.kill(process.pid, signal));
  };
  const listeners = TERMINATION_SIGNALS.map((signal) => [signal, () => onSignal(signal)] as const);
  const detach = (): void => {
    process.removeListener('exit', onExit);
    for (const [signal, listener] of listeners) process.removeListener(signal, listener);
  };
  process.on('exit', onExit);
  for (const [signal, listener] of listeners) process.on(signal, listener);
  // A command that needs no run is already judged: its block prints now, the others' as they close.
  for (const run of runs) if (run.verdict !== null) process.stdout.write(block(run));

  const verdicts = await Promise.all(
    runs.map(
      async (run): Promise<CommandVerdict> => ({
        command: run.command,
        verdict: run.verdict ?? (await judge(run, cwd)),
      }),
    ),
  );
  detach();
  process.stdout.write(verdictReport(verdicts));
  return { success: verdicts.every(({ verdict }) => verdict.kind === 'pass') };
}
