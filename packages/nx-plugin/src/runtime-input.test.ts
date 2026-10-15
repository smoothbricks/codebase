import { describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { existsSync } from 'node:fs';
import { mkdir, mkdtemp, readdir, readFile, rm, stat, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';
import { RUNTIME_INPUT_SCRIPT, runtimeInput, shellWord } from './runtime-input.js';

const packageRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const repoRoot = dirname(dirname(packageRoot));
const script = join(packageRoot, 'runtime-input.sh');
const nxEntry = join(repoRoot, 'node_modules', '.bin', 'nx');

interface Ran {
  readonly status: number | null;
  readonly stdout: Buffer;
  readonly stderr: string;
}

function wrapped(workspace: string, ...command: string[]): Ran {
  const ran = spawnSync('sh', [script, ...command], { cwd: workspace, encoding: 'buffer' });
  return { status: ran.status, stdout: ran.stdout, stderr: ran.stderr.toString('utf8') };
}

async function withWrapperWorkspace(run: (workspace: string) => Promise<void>): Promise<void> {
  const workspace = await mkdtemp(join(tmpdir(), 'runtime-input-wrapper-'));
  try {
    await run(workspace);
  } finally {
    await rm(workspace, { recursive: true, force: true });
  }
}

async function failureRecord(workspace: string): Promise<string> {
  const root = join(workspace, '.runtime-input-failures');
  const records = await readdir(root);
  expect(records).toHaveLength(1);
  return join(root, records[0]);
}

describe('runtime-input.sh', () => {
  it('preserves successful stdout bytes and their hash exactly, including whitespace and UTF-8', async () => {
    await withWrapperWorkspace(async (workspace) => {
      const command = ['sh', '-c', "printf ' \tvalue café\n\n'; printf 'diagnostic\n\n' >&2"];
      // The pre-evidence wrapper's success path: stdout passes through, stderr is discarded.
      const baseline = spawnSync(
        'sh',
        [
          '-c',
          'exec 3>&1; if diagnostic=$("$@" 2>&1 >&3 3>&-); then exit 0; else exit "$?"; fi',
          'baseline',
          ...command,
        ],
        { cwd: workspace, encoding: 'buffer' },
      );
      const ran = wrapped(workspace, ...command);
      expect(baseline.status).toBe(0);
      expect(baseline.stderr).toEqual(Buffer.alloc(0));
      expect(ran).toEqual({ status: 0, stdout: Buffer.from(' \tvalue café\n\n'), stderr: '' });
      expect(ran.stdout).toEqual(baseline.stdout);
      const hash = (bytes: Buffer) => createHash('sha256').update(bytes).digest('hex');
      expect(hash(ran.stdout)).toBe(hash(baseline.stdout));
      expect(existsSync(join(workspace, '.runtime-input-failures'))).toBe(false);
    });
  });

  it('poisons a failure and durably retains exact argv, stderr and exit status', async () => {
    await withWrapperWorkspace(async (workspace) => {
      const command = [
        'sh',
        '-c',
        String.raw`printf 'partial\n'; printf 'the-cause\000raw\n\n' >&2; exit 3`,
        '--',
        '',
        'two words',
        "a'quote",
        'line\nbreak',
      ];
      const ran = wrapped(workspace, ...command);
      expect(ran.status).toBe(3);
      expect(ran.stdout).toEqual(Buffer.from([...Buffer.from('partial\n'), 0xff]));
      const record = await failureRecord(workspace);
      expect((await stat(record)).mode & 0o777).toBe(0o700);
      expect(await readFile(join(record, 'argv'))).toEqual(Buffer.from(`${command.join('\0')}\0`));
      expect(await readFile(join(record, 'stderr'))).toEqual(Buffer.from('the-cause\0raw\n\n'));
      expect(await readFile(join(record, 'exit-status'), 'utf8')).toBe('3\n');
      const report = await readFile(join(record, 'report'), 'utf8');
      expect(report).toContain('runtime input exited 3: sh -c');
      expect(report).toContain('argv[4]: <>');
      expect(report).toContain('the-cause\0raw\n\n');
      // Direct invocation sees the attempted stderr notice; Nx is allowed to drop it.
      expect(ran.stderr).toContain('the-cause');
      expect(ran.stderr).toContain('runtime input exited 3: sh -c');
      expect(ran.stderr).toContain(join(record, 'report'));
    });
  });

  it('refuses to run nothing', async () => {
    await withWrapperWorkspace(async (workspace) => {
      const ran = wrapped(workspace);
      expect(ran.status).toBe(64);
      expect(ran.stdout).toEqual(Buffer.from([0xff]));
    });
  });
});

/**
 * WHY this test is permanent. Nx 23.2.1 hashes a runtime input's stdout and
 * stderr and ignores its exit status; the wrapper's 0xFF byte is what makes a
 * failed input an error instead of a key (runtime-input.sh). Planted through
 * real Nx, both through the daemon that hashes for a workspace's clients and
 * without one, the failure must stop the run before the task executes. A
 * future Nx that hashes those bytes turns this red; one that honours the exit
 * status changes the message, which is the cue to drop the byte.
 */
describe('a planted runtime input failure through real Nx', () => {
  for (const daemon of [false, true]) {
    it(`is an error, not a hash${daemon ? ', through the daemon' : ''}`, async () => {
      await withNxFixture('runtime-input-', async ({ workspace, socketDirectory }) => {
        await writeProbeWorkspace(workspace);
        const env = { ...fixtureNxEnv(workspace, socketDirectory), NX_DAEMON: String(daemon) };

        const good = nx(workspace, env, 'probe:good');
        expect(good.status, good.output).toBe(0);
        expect(existsSync(join(workspace, 'good.ran'))).toBe(true);

        const planted = nx(workspace, env, 'probe:planted');
        expect(planted.status, planted.output).toBe(1);
        expect(planted.output).toContain('invalid utf-8 sequence');
        expect(existsSync(join(workspace, 'planted.ran'))).toBe(false);
        const record = await failureRecord(workspace);
        expect(await readFile(join(record, 'argv'))).toEqual(Buffer.from('sh\0-c\0echo planted-cause >&2; exit 3\0'));
        expect(await readFile(join(record, 'stderr'), 'utf8')).toBe('planted-cause\n');
        expect(await readFile(join(record, 'exit-status'), 'utf8')).toBe('3\n');
        expect(await readFile(join(record, 'report'), 'utf8')).toContain('planted-cause');
        const reset = spawnSync('bun', [nxEntry, 'reset'], { cwd: workspace, env, encoding: 'utf8' });
        expect(reset.status, `${reset.stdout}${reset.stderr}`).toBe(0);
        expect(await readFile(join(record, 'stderr'), 'utf8')).toBe('planted-cause\n');
        expect(await readFile(join(record, 'report'), 'utf8')).toContain('runtime input exited 3: sh -c');
      });
    }, 60_000);
  }
});

describe('runtime inputs of this repository', () => {
  it('all run through the wrapper', async () => {
    const offenders: string[] = [];
    const declared = (file: string, value: unknown): void => {
      if (Array.isArray(value)) {
        for (const item of value) declared(file, item);
      } else if (value !== null && typeof value === 'object') {
        for (const [key, item] of Object.entries(value)) {
          if (key === 'runtime' && !String(item).startsWith(`sh ${RUNTIME_INPUT_SCRIPT} `))
            offenders.push(`${file}: ${item}`);
          else declared(file, item);
        }
      }
    };
    const tracked = spawnSync(
      'git',
      ['ls-files', '-z', 'nx.json', 'package.json', '**/package.json', '**/project.json'],
      {
        cwd: repoRoot,
        encoding: 'utf8',
      },
    );
    expect(tracked.status, tracked.stderr).toBe(0);
    for (const file of tracked.stdout.split('\0').filter(Boolean)) {
      declared(file, JSON.parse(await readFile(join(repoRoot, file), 'utf8')));
    }
    // Inferred inputs: every one goes through runtimeInput(), never a literal.
    for (const file of await readdir(join(packageRoot, 'src'))) {
      if (!file.endsWith('.ts') || file.endsWith('.test.ts') || file === 'runtime-input.ts') continue;
      const source = await readFile(join(packageRoot, 'src', file), 'utf8');
      for (const literal of source.match(/\{\s*runtime:\s*[`'"]/g) ?? []) offenders.push(`src/${file}: ${literal}`);
    }
    expect(offenders).toEqual([]);
  });
});

async function writeProbeWorkspace(workspace: string): Promise<void> {
  expect(spawnSync('git', ['init', '--quiet', workspace]).status).toBe(0);
  await mkdir(join(workspace, 'probe'), { recursive: true });
  // The repository's own node_modules: the Nx under test, and the wrapper at the path inputs name it by.
  await symlink(join(repoRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
  await writeFile(join(workspace, 'nx.json'), JSON.stringify({ useDaemonProcess: true, cacheDirectory: '.nx/cache' }));
  const target = (name: string, command: string) => ({
    executor: 'nx:run-commands',
    cache: true,
    inputs: [runtimeInput(command)],
    options: { command: `touch ${name}.ran` },
  });
  await writeFile(
    join(workspace, 'probe', 'project.json'),
    JSON.stringify({
      name: 'probe',
      targets: {
        good: target('good', `sh -c ${shellWord('echo value; echo noise >&2')}`),
        planted: target('planted', `sh -c ${shellWord('echo planted-cause >&2; exit 3')}`),
      },
    }),
  );
}

function nx(workspace: string, env: Record<string, string>, target: string): { status: number | null; output: string } {
  const ran = spawnSync('bun', [nxEntry, 'run', target, '--outputStyle=static'], {
    cwd: workspace,
    env,
    encoding: 'utf8',
  });
  return { status: ran.status, output: `${ran.stdout}${ran.stderr}` };
}
