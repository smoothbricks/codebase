import { describe, expect, test } from 'bun:test';
import { readFileSync } from 'node:fs';
import { dirname, join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

/**
 * The bounded-exec key (README, "Bounded exec key"): a workspace that runs the plugin from a linked checkout keys every
 * bounded-exec task by the modules the executor's import closure reaches, stopping at the modules the executor's
 * executors.json record lists as output-neutral. A change to a keyed module re-keys every bounded-exec task of every
 * workspace that runs the plugin; a change to a neutral one re-keys none. This pins which is which, from the sources:
 * the closure walk is linked-package-files.ts's, over the sources instead of dist (type-only imports leave no import
 * in dist, so they are not followed here either).
 */
const PACKAGE = fileURLToPath(new URL('../../..', import.meta.url));
const SOURCES = join(PACKAGE, 'src');

interface ExecutorRecord {
  implementation: string;
  outputNeutral?: string[];
}

function record(): ExecutorRecord {
  const registry: { executors: Record<string, ExecutorRecord> } = JSON.parse(
    readFileSync(join(PACKAGE, 'executors.json'), 'utf8'),
  );
  const executor = registry.executors['bounded-exec'];
  if (executor === undefined) throw new Error('executors.json has no bounded-exec');
  return executor;
}

/** The source of a module executors.json names by its dist path. */
const sourceOf = (distPath: string): string =>
  join(PACKAGE, distPath.replace(/^\.\/dist\//u, 'src/').replace(/\.js$/u, '.ts'));

const withoutComments = (text: string): string => text.replace(/\/\*[\s\S]*?\*\/|^\s*\/\/.*$/gmu, '');

const IMPORT =
  /(?:\b(?:import|export)\s+(?<type>type\s+)?(?<names>[^;]*?)\s*from\s*|\bimport\(\s*)(["'])(?<specifier>\.{1,2}\/[^"']+)\3/gu;

/** The relative modules `file` imports at run time: no `import type`, and no import of nothing but types. */
function importsOf(file: string): string[] {
  const found: string[] = [];
  for (const match of withoutComments(readFileSync(file, 'utf8')).matchAll(IMPORT)) {
    const { type, names, specifier } = match.groups ?? {};
    if (type !== undefined || specifier === undefined) continue;
    const braced = /^\{(?<inside>[^}]*)\}$/u.exec((names ?? '').trim())?.groups?.inside;
    const members = braced
      ?.split(',')
      .map((member) => member.trim())
      .filter((member) => member !== '');
    if (members?.length && members.every((member) => member.startsWith('type '))) continue;
    found.push(resolve(dirname(file), specifier.replace(/\.js$/u, '.ts')));
  }
  return found;
}

/** The modules keyed: the implementation and everything it imports, not walking past an output-neutral module. */
function keyedModules(): string[] {
  const executor = record();
  const neutral = new Set((executor.outputNeutral ?? []).map(sourceOf));
  const keyed = new Set<string>();
  const pending = [sourceOf(executor.implementation)];
  for (let file = pending.pop(); file !== undefined; file = pending.pop()) {
    if (keyed.has(file) || neutral.has(file)) continue;
    keyed.add(file);
    pending.push(...importsOf(file));
  }
  return Array.from(keyed, (file) => relative(SOURCES, file)).sort();
}

/**
 * What decides the command a task runs, its environment or the verdict of a run that passes, and so must stay keyed:
 * the executor itself, the sizing of the runner to its grant, the report flags, the lifecycle hooks, the jobserver and
 * pool environment, the mbx leaf wrapping, the verdict and its test results.
 */
const KEYED = [
  'executors/bounded-exec/bounds.ts',
  'executors/bounded-exec/child-exit-diagnostics.ts',
  'executors/bounded-exec/executor.ts',
  'executors/bounded-exec/leaf-peak.ts',
  'executors/bounded-exec/lock-wait.ts',
  'executors/bounded-exec/mbx-pool.ts',
  'executors/bounded-exec/mbx-reservations.ts',
  'executors/bounded-exec/no-space.ts',
  'executors/bounded-exec/owned-subtree.ts',
  'executors/bounded-exec/pool-environment.ts',
  'executors/bounded-exec/progress.ts',
  'executors/bounded-exec/runner-lifecycle.ts',
  'executors/bounded-exec/runner-sizing.ts',
  'executors/bounded-exec/runtime-pin.ts',
  'executors/bounded-exec/test-result-writer.ts',
  'executors/bounded-exec/test-results.ts',
  'executors/bounded-exec/verdict.ts',
  'process-group.ts',
  'runtime-input-policy.ts',
  'runtime-input.ts',
  'type-guards.ts',
];

/** What only decides when and on what resources the command runs: a change to one re-keys no task. */
const SCHEDULING = [
  'executors/bounded-exec/admission-queue.ts',
  'executors/bounded-exec/cpu-grant.ts',
  'executors/bounded-exec/cpu-pool.ts',
  'executors/bounded-exec/host-memory.darwin.ts',
  'executors/bounded-exec/host-memory.linux.ts',
  'executors/bounded-exec/host-memory.ts',
  'executors/bounded-exec/lease-wait.ts',
  'executors/bounded-exec/mbx-queue.ts',
  'executors/bounded-exec/memory-retry.ts',
  'executors/bounded-exec/parent-grant.ts',
  'executors/bounded-exec/peak-charge.ts',
  'executors/bounded-exec/pool-admission.ts',
];

describe('the bounded-exec key', () => {
  test('keys the modules that decide what a task runs, and none that only schedule it', () => {
    const keyed = keyedModules();
    expect(keyed).toEqual(KEYED);
    for (const scheduling of SCHEDULING) expect(keyed).not.toContain(scheduling);
  });

  test('lists only modules that exist', () => {
    for (const path of record().outputNeutral ?? []) {
      expect(() => readFileSync(sourceOf(path), 'utf8')).not.toThrow();
    }
  });

  test('keeps the command, its environment and its flags out of every neutral module', () => {
    // The jobserver flags and the pool's environment names are written by pool-environment.ts, and the runner's report
    // flags by progress.ts: both keyed. A neutral module that wrote them would change a command no key holds.
    const written = /MAKEFLAGS|MFLAGS|jobserver-auth|--dots|CPU_TOKENS_ENV|CPU_POOL_ENV/u;
    for (const path of record().outputNeutral ?? []) {
      expect({ path, writes: written.exec(withoutComments(readFileSync(sourceOf(path), 'utf8')))?.[0] }).toEqual({
        path,
        writes: undefined,
      });
    }
  });
});
