import { afterEach, describe, expect, test } from 'bun:test';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setupDevenvSteps } from '../__tests__/setup-devenv-steps.js';

const locate = setupDevenvSteps().find((step) =>
  String(step.get('name')).includes('Locate the Nx cache on the shared bind'),
);

const binds: string[] = [];
afterEach(() => {
  for (const bind of binds.splice(0)) rmSync(bind, { recursive: true, force: true });
});

function sharedBind(): string {
  const bind = mkdtempSync(join(tmpdir(), 'setup-devenv-records-'));
  binds.push(bind);
  return bind;
}

/** A CI event: a push of `ref`, or a pull request numbered `ref` into `base`. */
interface Event {
  readonly ref: string;
  readonly base: string;
}

/**
 * Runs the step the way GitHub runs a bash step, its shared bind under `bind`, and answers the workspace data
 * directory it hands the job.
 */
function locateNxCache(bind: string, event: Event): string {
  if (!locate) throw new Error('setup-devenv has no step that locates the Nx cache on the shared bind');
  const githubEnv = join(bind, `github-env-${event.ref.replaceAll('/', '-')}-${binds.length}`);
  const result = Bun.spawnSync(['bash', '--noprofile', '--norc', '-eo', 'pipefail', '-c', String(locate.get('run'))], {
    cwd: bind,
    env: {
      ...process.env,
      NX_CACHE_BIND: join(bind, 'nx'),
      NX_CACHE_REPO: 'example/app',
      NX_CACHE_LANE: 'CI',
      SCOPE_REF: event.ref,
      BASE_REF: event.base,
      GITHUB_WORKFLOW: 'CI',
      GITHUB_JOB: 'main',
      RUNNER_OS: 'Linux',
      RUNNER_ARCH: 'X64',
      GITHUB_ENV: githubEnv,
    },
  });
  expect(result.exitCode, result.stderr.toString()).toBe(0);
  const workspaceData = /^NX_WORKSPACE_DATA_DIRECTORY=(.+)$/m.exec(readFileSync(githubEnv, 'utf8'))?.[1];
  if (workspaceData === undefined) throw new Error(`the step handed the job no workspace data: ${githubEnv}`);
  return workspaceData;
}

/** The record bounded-exec keeps for `task` under `workspaceData`, and a report beside it that is no record. */
function writeRecord(workspaceData: string, task: string, peakBytes: number): void {
  const directory = join(workspaceData, 'bounded-exec', encodeURIComponent(task));
  mkdirSync(directory, { recursive: true });
  writeFileSync(join(directory, 'verdict.json'), `${JSON.stringify({ task, hash: null, peakBytes })}\n`);
  writeFileSync(join(directory, 'report.xml'), '<testsuites/>\n');
}

function peakOf(workspaceData: string, task: string): unknown {
  const verdict = join(workspaceData, 'bounded-exec', encodeURIComponent(task), 'verdict.json');
  if (!existsSync(verdict)) return undefined;
  const record: unknown = JSON.parse(readFileSync(verdict, 'utf8'));
  return typeof record === 'object' && record !== null && 'peakBytes' in record ? record.peakBytes : undefined;
}

const push: Event = { ref: 'refs/heads/main', base: '' };
const pullRequest: Event = { ref: '7', base: 'main' };
const GIB = 2 ** 30;

describe("a pull request's first run on the shared bind", () => {
  test("starts from each task record its base branch's runs left, and only the records", () => {
    const bind = sharedBind();
    const main = locateNxCache(bind, push);
    writeRecord(main, 'app:test', 3 * GIB);
    writeRecord(main, 'lib:test', GIB);

    const scope = locateNxCache(bind, pullRequest);

    expect(scope).not.toBe(main);
    expect(peakOf(scope, 'app:test')).toBe(3 * GIB);
    expect(peakOf(scope, 'lib:test')).toBe(GIB);
    expect(existsSync(join(scope, 'bounded-exec', encodeURIComponent('app:test'), 'report.xml'))).toBe(false);
  });

  test('keeps its own records once it has run, whatever its base branch records since', () => {
    const bind = sharedBind();
    const main = locateNxCache(bind, push);
    writeRecord(main, 'app:test', 3 * GIB);
    const scope = locateNxCache(bind, pullRequest);
    writeRecord(scope, 'app:test', 5 * GIB);
    writeRecord(main, 'app:test', 2 * GIB);

    expect(locateNxCache(bind, pullRequest)).toBe(scope);
    expect(peakOf(scope, 'app:test')).toBe(5 * GIB);
  });

  test('a push takes no records from another scope', () => {
    const bind = sharedBind();
    const main = locateNxCache(bind, push);
    writeRecord(main, 'app:test', 3 * GIB);

    const branch = locateNxCache(bind, { ref: 'refs/heads/feature', base: '' });

    expect(existsSync(join(branch, 'bounded-exec'))).toBe(false);
  });
});
