import { describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { existsSync } from 'node:fs';
import { mkdir, readdir, readFile, symlink, writeFile } from 'node:fs/promises';
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

function wrapped(...command: string[]): Ran {
  const ran = spawnSync('sh', [script, ...command], { encoding: 'buffer' });
  return { status: ran.status, stdout: ran.stdout, stderr: ran.stderr.toString('utf8') };
}

describe('runtime-input.sh', () => {
  it('keys a command that succeeds on its stdout alone', () => {
    const ran = wrapped('sh', '-c', 'echo value; echo diagnostic >&2');
    expect(ran).toEqual({ status: 0, stdout: Buffer.from('value\n'), stderr: '' });
  });

  it('ends a failed command with a byte Nx refuses, and names the cause on stderr', () => {
    const ran = wrapped('sh', '-c', 'echo partial; echo the-cause >&2; exit 3');
    expect(ran.status).toBe(3);
    expect(ran.stdout).toEqual(Buffer.from([...Buffer.from('partial\n'), 0xff]));
    expect(ran.stderr).toContain('the-cause');
    expect(ran.stderr).toContain('runtime input exited 3: sh -c');
  });

  it('refuses to run nothing', () => {
    const ran = wrapped();
    expect(ran.status).toBe(64);
    expect(ran.stdout).toEqual(Buffer.from([0xff]));
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
      await withNxFixture('runtime-input-', async ({ workspace }) => {
        await writeProbeWorkspace(workspace);
        const env = { ...fixtureNxEnv(workspace), NX_DAEMON: String(daemon) };

        const good = nx(workspace, env, 'probe:good');
        expect(good.status, good.output).toBe(0);
        expect(existsSync(join(workspace, 'good.ran'))).toBe(true);

        const planted = nx(workspace, env, 'probe:planted');
        expect(planted.status, planted.output).toBe(1);
        expect(planted.output).toContain('invalid utf-8 sequence');
        expect(existsSync(join(workspace, 'planted.ran'))).toBe(false);
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
