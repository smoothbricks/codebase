import { readFileSync } from 'node:fs';

const ANSI = new RegExp(`${String.fromCharCode(27)}\\[[0-9;]*[A-Za-z]`, 'g');
// Recognized diagnostic sections keep their exact lines, including assertion diffs and stack locations.
// Unknown formats fall back to their output; the complete log is retained by the caller in either case.
const DIAGNOSTIC =
  /^(?:\s*(?:error(?:\[[^\]]+\])?:|[\w.]*Error:|thread .*panicked at|assertion .*failed|TIMEOUT\b|FAIL\b|SIG(?:ABRT|SEGV|TERM)\b|bounded-exec:|RAM temp volume:)|\.*Internal error:|.*:\d+(?::\d+)?: (?:error|fatal error)|\s*\d+:\d+\s+error\b)/i;
const PROGRESS =
  /^\s*(?:\(pass\) |\(skip\) |PASS\s+\[|SKIP\s+\[|Compiling |Checking |Finished |Downloaded |Downloading |\d+ (?:pass|fail|skip)$|Ran \d+ tests?|Command exited with status |\[trace\]|DEBUG |\[NX |Time taken for |bun test v|--shard=)|^\.{3,}/;
const FAILED_TEST = /^\s*(?:\(fail\)|\u2717\s)/;

/** A summary is not a verdict: it selects diagnostic text, never changes it or excuses a failed command. */
function diagnostics(lines: readonly string[]): string[] {
  const messages = new Set<string>();
  let block: string[] = [];
  const flush = (): void => {
    if (block.length > 0) messages.add(block.join('\n').trimEnd());
    block = [];
  };
  for (const line of lines) {
    if (DIAGNOSTIC.test(line)) {
      flush();
      block.push(line);
    } else if (PROGRESS.test(line)) {
      flush();
    } else if (block.length > 0) {
      block.push(line);
      if (FAILED_TEST.test(line)) flush();
    }
  }
  flush();
  if (messages.size > 0) return [...messages];
  return [
    ...new Set(
      lines
        .filter((line) => !PROGRESS.test(line))
        .join('\n')
        .split(/\n[ \t]*\n/)
        .map((part) => part.replace(/^\n+/, '').trimEnd())
        .filter((part) => part.trim() !== ''),
    ),
  ];
}

/** Nx's final named list, never an earlier task's own nested run. */
function taskList(lines: readonly string[], header: string): string[] {
  const at = lines.lastIndexOf(header);
  const tasks = new Set<string>();
  for (const line of at < 0 ? [] : lines.slice(at + 1)) {
    const task = /^- (\S+)$/.exec(line)?.[1];
    if (task !== undefined) tasks.add(task);
    else if (line !== '' || tasks.size > 0) break;
  }
  return [...tasks];
}

/** Reporting only: the caller must retain the raw output and propagate the gate's actual exit status. */
export function summarizeFailures(output: string): string {
  const lines = output
    .replace(ANSI, '')
    .split('\n')
    .map((line) => line.trimEnd());
  const failed = taskList(lines, 'Failed tasks:');
  const stopped = taskList(lines, 'Tasks stopped before they finished:');
  const unfinished = taskList(lines, 'Tasks the run ended without finishing:');
  const bodies = new Map<string, string[]>();
  let body: string[] | undefined;
  for (const line of lines) {
    // Full failed-task logs start with '> nx run'; successful compact lines also end the preceding body.
    const command = /^(?:> |\S\s+)nx run (\S+)/.exec(line)?.[1];
    if (command !== undefined) {
      body = undefined;
      if (line.startsWith('> nx run ')) {
        body = bodies.get(command) ?? [];
        bodies.set(command, body);
        body.push('');
      }
    } else if (/^\s*NX\s|^Failed tasks:|^Tasks (?:not run|stopped|the run ended)/.test(line)) {
      body = undefined;
    } else {
      body?.push(line);
    }
  }
  const sections: string[] = [];
  for (const task of failed) {
    const messages = diagnostics(bodies.get(task) ?? []);
    sections.push(
      'failed ' +
        task +
        '\n' +
        (messages.length > 0 ? messages.join('\n\n') : 'diagnostic unavailable: this run printed no task output'),
    );
  }
  for (const task of stopped) sections.push(`stopped ${task}: Nx stopped it before it finished`);
  for (const task of unfinished) sections.push(`unfinished ${task}: the run ended without finishing it`);
  if (sections.length === 0) {
    sections.push(
      'Gate failed without a task-level failure summary:\n' +
        (diagnostics(lines).join('\n\n') || 'diagnostic unavailable: this run printed no output'),
    );
  }
  return `${sections.join('\n\n')}\n`;
}

/** CLI entry shared by the installed binary and repository host entrypoints; success means reporting succeeded only. */
export function runFailureSummary(args: readonly string[]): number {
  const path = args[0];
  if (path === undefined) {
    process.stderr.write('usage: smoo-nx-failures OUTPUT\n');
    return 64;
  }
  try {
    process.stdout.write(summarizeFailures(readFileSync(path, 'utf8')));
    return 0;
  } catch (error) {
    process.stderr.write(`smoo-nx-failures: ${error instanceof Error ? error.message : String(error)}\n`);
    return 1;
  }
}
