import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { mkdirSync, mkdtempSync, readdirSync, readFileSync, realpathSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { basename, dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

/**
 * The managed Forgejo artifact actions run the pinned original clients through a real child Node. These tests drive the
 * real shared bootstrap against a local git repository standing in for code.forgejo.org (file:// transport, never the
 * network) and observe what the original entry actually received.
 */

const actions = fileURLToPath(new URL('../../managed/templates/github/actions/', import.meta.url));
const helper = join(actions, 'forgejo-artifact.cjs');

const node = Bun.which('node');
const realGit = Bun.which('git');
if (node === null || realGit === null) throw new Error('These tests need node and git on PATH');

const hermeticGit = {
  GIT_CONFIG_NOSYSTEM: '1',
  GIT_CONFIG_GLOBAL: '/dev/null',
  GIT_AUTHOR_NAME: 'fixture',
  GIT_AUTHOR_EMAIL: 'fixture@example.invalid',
  GIT_COMMITTER_NAME: 'fixture',
  GIT_COMMITTER_EMAIL: 'fixture@example.invalid',
  // The pin is deliberately not a branch tip, so the fixture must serve any reachable commit by SHA.
  GIT_CONFIG_COUNT: '1',
  GIT_CONFIG_KEY_0: 'uploadpack.allowAnySHA1InWant',
  GIT_CONFIG_VALUE_0: 'true',
};

/** The original action's stand-in: records exactly what it received, then behaves as FIXTURE_MODE says. */
const originalEntry = `
const fs = require('node:fs');
const path = require('node:path');
const { spawnSync } = require('node:child_process');
const root = path.resolve(__dirname, '..');
const git = (args) => spawnSync('git', ['-C', root, ...args], { encoding: 'utf8' }).stdout.trim();
const inputs = Object.keys(process.env)
  .filter((name) => name.startsWith('INPUT_'))
  .sort()
  .map((name) => name + '=' + process.env[name]);
fs.writeFileSync(process.env.FIXTURE_RECORD, JSON.stringify({
  argv: process.argv.slice(2),
  execPath: process.execPath,
  cwd: process.cwd(),
  root,
  head: git(['rev-parse', 'HEAD']),
  commits: git(['rev-list', '--count', 'HEAD']),
  gitConfig: fs.readFileSync(path.join(root, '.git', 'config'), 'utf8'),
  inputs,
  marker: process.env.FIXTURE_MARKER,
}));
fs.appendFileSync(process.env.GITHUB_OUTPUT, 'artifact-id=42\\n');
process.stdout.write('fixture stdout\\n');
process.stderr.write('fixture stderr\\n');
const mode = process.env.FIXTURE_MODE;
if (mode === 'exit') process.exit(7);
if (mode === 'kill') process.kill(process.pid, 'SIGKILL');
`;

const siblingTipEntry = `
process.stderr.write('the branch tip ran instead of the pin\\n');
process.exit(99);
`;

const driver = `
const [helperPath, pin] = process.argv.slice(2);
if (process.env.FIXTURE_MODE === 'cleanup-error') {
  require('node:fs').rmSync = () => { throw new Error('fixture cleanup failure'); };
}
require(helperPath).run(JSON.parse(pin));
`;

interface ReceivedRecord {
  argv: string[];
  execPath: string;
  cwd: string;
  root: string;
  head: string;
  commits: string;
  gitConfig: string;
  inputs: string[];
  marker: string;
}

interface Pin {
  repository: string;
  commit: string;
  entry: string;
}

interface Outcome {
  status: number | null;
  stdout: string;
  stderr: string;
  runnerTemp: string;
  workspace: string;
  output: string;
  record: ReceivedRecord | undefined;
}

let scratch = '';
let upstream = '';
let pinnedCommit = '';
let nodeExecPath = '';
let caseNumber = 0;

function git(directory: string, args: string[]): string {
  const result = Bun.spawnSync([realGit ?? 'git', '-C', directory, ...args], {
    env: { ...process.env, ...hermeticGit },
  });
  if (result.exitCode !== 0) throw new Error('git ' + args.join(' ') + ': ' + result.stderr.toString());
  return result.stdout.toString().trim();
}

function inheritedEnvironment(): Record<string, string> {
  const environment: Record<string, string> = {};
  for (const [name, value] of Object.entries(process.env)) {
    if (value !== undefined && !name.startsWith('INPUT_')) environment[name] = value;
  }
  return environment;
}

beforeAll(() => {
  scratch = realpathSync(mkdtempSync(join(tmpdir(), 'forgejo-artifact-test-')));
  upstream = join(scratch, 'upstream');
  mkdirSync(join(upstream, 'dist'), { recursive: true });
  git(upstream, ['init', '--quiet', '--initial-branch=main']);
  writeFileSync(join(upstream, 'dist', 'index.js'), originalEntry);
  git(upstream, ['add', '.']);
  git(upstream, ['commit', '--quiet', '-m', 'pinned']);
  pinnedCommit = git(upstream, ['rev-parse', 'HEAD']);
  writeFileSync(join(upstream, 'dist', 'index.js'), siblingTipEntry);
  git(upstream, ['commit', '--quiet', '-am', 'tip']);
  writeFileSync(join(scratch, 'driver.cjs'), driver);
  const execPath = Bun.spawnSync([node ?? 'node', '-p', 'process.execPath']);
  nodeExecPath = execPath.stdout.toString().trim();
});

afterAll(() => {
  rmSync(scratch, { recursive: true, force: true });
});

interface RunOptions {
  /** What to run: the shared bootstrap with this pin, or a managed entry file used as-is. */
  pin?: Partial<Pin>;
  entryFile?: string;
  environment?: Record<string, string>;
  withoutRunnerTemp?: boolean;
  path?: string;
}

function run(options: RunOptions = {}): Outcome {
  caseNumber += 1;
  const directory = join(scratch, 'case-' + caseNumber);
  const runnerTemp = join(directory, 'runner-temp');
  const workspace = join(directory, 'workspace');
  const output = join(directory, 'github-output');
  const recordPath = join(directory, 'record.json');
  mkdirSync(runnerTemp, { recursive: true });
  mkdirSync(workspace);
  writeFileSync(output, '');
  const environment: Record<string, string> = {
    ...inheritedEnvironment(),
    ...hermeticGit,
    RUNNER_TEMP: runnerTemp,
    GITHUB_OUTPUT: output,
    FIXTURE_RECORD: recordPath,
    ...(options.path === undefined ? {} : { PATH: options.path }),
    ...options.environment,
  };
  if (options.withoutRunnerTemp) delete environment.RUNNER_TEMP;
  const pin: Pin = { repository: 'file://' + upstream, commit: pinnedCommit, entry: 'dist/index.js', ...options.pin };
  const command =
    options.entryFile === undefined
      ? [node ?? 'node', join(scratch, 'driver.cjs'), helper, JSON.stringify(pin)]
      : [node ?? 'node', options.entryFile];
  const result = Bun.spawnSync(command, { cwd: workspace, env: environment });
  let record: ReceivedRecord | undefined;
  try {
    record = JSON.parse(readFileSync(recordPath, 'utf8'));
  } catch {
    record = undefined;
  }
  return {
    status: result.exitCode,
    stdout: result.stdout.toString(),
    stderr: result.stderr.toString(),
    runnerTemp,
    workspace,
    output: readFileSync(output, 'utf8'),
    record,
  };
}

describe('forgejo artifact bootstrap', () => {
  test('fetches the exact pinned client, preserves its outputs, and removes the credential-free checkout', () => {
    const outcome = run();
    expect(outcome.stderr).not.toContain('the branch tip ran');
    expect(outcome.status).toBe(0);
    expect(outcome.record?.head).toBe(pinnedCommit);
    expect(outcome.record?.commits).toBe('1');
    expect(basename(outcome.record?.root ?? '')).toStartWith('forgejo-artifact-');
    expect(dirname(outcome.record?.root ?? '')).toBe(realpathSync(outcome.runnerTemp));
    expect(outcome.record?.gitConfig ?? '').not.toMatch(/credential|extraheader|\[remote|\[url|token/i);
    expect(outcome.output).toBe('artifact-id=42\n');
    expect(outcome.stdout).toBe('fixture stdout\n');
    expect(outcome.stderr).toBe('fixture stderr\n');
    expect(readdirSync(outcome.runnerTemp)).toEqual([]);
  });

  test('passes inputs, environment, working directory and the runtime through to the original entry', () => {
    const inputs = {
      INPUT_NAME: 'trace db',
      'INPUT_IF-NO-FILES-FOUND': 'error',
      INPUT_PATH: 'packages/*/.cache/lmao/trace-results.db*\nother/dir',
      'INPUT_RETENTION-DAYS': '',
    };
    const outcome = run({ environment: { ...inputs, FIXTURE_MARKER: 'passed through' } });
    expect(outcome.status).toBe(0);
    expect(outcome.record?.inputs).toEqual(
      Object.entries(inputs)
        .map(([name, value]) => name + '=' + value)
        .sort(),
    );
    expect(outcome.record?.marker).toBe('passed through');
    expect(outcome.record?.argv).toEqual([]);
    expect(outcome.record?.execPath).toBe(nodeExecPath);
    expect(realpathSync(outcome.record?.cwd ?? '')).toBe(realpathSync(outcome.workspace));
  });

  test('propagates the original action exit status with a diagnostic and still cleans up', () => {
    const outcome = run({ environment: { FIXTURE_MODE: 'exit' } });
    expect(outcome.status).toBe(7);
    expect(outcome.stderr).toContain(
      'forgejo-artifact: file://' + upstream + '@' + pinnedCommit + ' dist/index.js exited with status 7',
    );
    expect(readdirSync(outcome.runnerTemp)).toEqual([]);
  });

  test('fails with a diagnostic when the original action is killed by a signal, and cleans up', () => {
    const outcome = run({ environment: { FIXTURE_MODE: 'kill' } });
    expect(outcome.status).toBe(1);
    expect(outcome.stderr).toContain('was killed by signal SIGKILL');
    expect(readdirSync(outcome.runnerTemp)).toEqual([]);
  });

  test('fails before running anything when the pin cannot be fetched, and cleans up', () => {
    const missing = 'a'.repeat(40);
    const outcome = run({ pin: { commit: missing } });
    expect(outcome.status).toBe(1);
    expect(outcome.stderr).toContain(
      'forgejo-artifact: git fetch --quiet --depth=1 --no-tags file://' + upstream + ' ' + missing,
    );
    expect(outcome.record).toBeUndefined();
    expect(readdirSync(outcome.runnerTemp)).toEqual([]);
  });

  test('refuses a checkout whose HEAD is not the pin before running the original entry', () => {
    const bin = join(scratch, 'lying-git');
    mkdirSync(bin, { recursive: true });
    writeFileSync(
      join(bin, 'git'),
      '#!/bin/sh\nfor arg in "$@"; do\n  if [ "$arg" = rev-parse ]; then echo ' +
        '0'.repeat(40) +
        '; exit 0; fi\ndone\nexec "' +
        realGit +
        '" "$@"\n',
      { mode: 0o755 },
    );
    const outcome = run({ path: bin + ':' + (process.env.PATH ?? '') });
    expect(outcome.status).toBe(1);
    expect(outcome.stderr).toContain(
      'checked out HEAD ' + '0'.repeat(40) + ' is not the pinned commit ' + pinnedCommit,
    );
    expect(outcome.record).toBeUndefined();
    expect(readdirSync(outcome.runnerTemp)).toEqual([]);
  });

  test('refuses to run without RUNNER_TEMP', () => {
    const outcome = run({ withoutRunnerTemp: true });
    expect(outcome.status).toBe(1);
    expect(outcome.stderr).toContain('forgejo-artifact: RUNNER_TEMP is not set');
    expect(outcome.record).toBeUndefined();
  });

  test('reports a cleanup failure instead of swallowing it', () => {
    const outcome = run({ environment: { FIXTURE_MODE: 'cleanup-error' } });
    expect(outcome.status).toBe(1);
    expect(outcome.stderr).toContain('forgejo-artifact: could not remove ' + outcome.record?.root);
    expect(outcome.stderr).toContain('fixture cleanup failure');
  });
});

describe('managed Forgejo artifact actions', () => {
  const wrappers: Array<
    Pin & { directory: string; inputs: Record<string, { default?: string; required?: boolean }>; outputs: string[] }
  > = [
    {
      directory: 'forgejo-upload-artifact',
      repository: 'https://code.forgejo.org/forgejo/upload-artifact',
      commit: 'cb8afe72b42edc798abfb8fcb556cf660d894245',
      entry: 'dist/upload/index.js',
      inputs: {
        name: { default: 'artifact' },
        path: { required: true },
        'if-no-files-found': { default: 'warn' },
        'retention-days': {},
        'compression-level': { default: '6' },
        overwrite: { default: 'false' },
        'include-hidden-files': { default: 'false' },
      },
      outputs: ['artifact-id', 'artifact-url', 'artifact-digest'],
    },
    {
      directory: 'forgejo-download-artifact',
      repository: 'https://code.forgejo.org/forgejo/download-artifact',
      commit: '769f970437aa3291b13f35dc23fc87967d7fb19f',
      entry: 'dist/index.js',
      inputs: {
        name: { required: false },
        'artifact-ids': { required: false },
        path: { required: false },
        pattern: { required: false },
        'merge-multiple': { required: false, default: 'false' },
        'github-token': { required: false },
        repository: { required: false, default: '${{ github.repository }}' },
        'run-id': { required: false, default: '${{ github.run_id }}' },
      },
      outputs: ['download-path'],
    },
  ];

  function isRecord(value: unknown): value is Record<string, unknown> {
    return typeof value === 'object' && value !== null && !Array.isArray(value);
  }

  function section(manifest: unknown, key: string): Record<string, unknown> {
    if (!isRecord(manifest)) throw new Error('The action manifest must be a mapping');
    const value = manifest[key];
    if (!isRecord(value)) throw new Error('The action manifest must have a ' + key + ' mapping');
    return value;
  }

  for (const wrapper of wrappers) {
    describe(wrapper.directory, () => {
      const manifest: unknown = Bun.YAML.parse(readFileSync(join(actions, wrapper.directory, 'action.yml'), 'utf8'));

      test('forwards every upstream input default and requirement and every output, on the node24 runtime', () => {
        const inputs = Object.fromEntries(
          Object.entries(section(manifest, 'inputs')).map(([name, input]) => {
            if (!isRecord(input) || typeof input.description !== 'string' || input.description === '') {
              throw new Error('Input ' + name + ' must keep its description');
            }
            return [name, Object.fromEntries(Object.entries(input).filter(([key]) => key !== 'description'))];
          }),
        );
        expect(inputs).toEqual(wrapper.inputs);
        expect(Object.keys(section(manifest, 'outputs'))).toEqual(wrapper.outputs);
        expect(section(manifest, 'runs')).toEqual({ using: 'node24', main: 'index.cjs' });
      });

      test('declares no output value, because the original action writes GITHUB_OUTPUT itself', () => {
        for (const output of Object.values(section(manifest, 'outputs'))) {
          expect(isRecord(output) && 'value' in output).toBe(false);
        }
      });

      test('calls the shared bootstrap with only its exact pinned repository, commit and entry', () => {
        const entry = readFileSync(join(actions, wrapper.directory, 'index.cjs'), 'utf8');
        expect(entry).toContain("require('../forgejo-artifact.cjs').run({");
        expect(entry).toContain("repository: '" + wrapper.repository + "'");
        expect(entry).toContain("commit: '" + wrapper.commit + "'");
        expect(entry).toContain("entry: '" + wrapper.entry + "'");
      });

      test('fails closed naming its pin when the upstream cannot serve that commit', () => {
        const outcome = run({
          entryFile: join(actions, wrapper.directory, 'index.cjs'),
          environment: {
            GIT_CONFIG_COUNT: '2',
            GIT_CONFIG_KEY_1: 'url.file://' + upstream + '.insteadOf',
            GIT_CONFIG_VALUE_1: wrapper.repository,
          },
        });
        expect(outcome.status).toBe(1);
        expect(outcome.stderr).toContain(wrapper.repository + ' ' + wrapper.commit);
        expect(outcome.record).toBeUndefined();
        expect(readdirSync(outcome.runnerTemp)).toEqual([]);
      });
    });
  }
});
