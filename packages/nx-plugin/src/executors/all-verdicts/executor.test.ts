import { describe, expect, it } from 'bun:test';
import { spawn, spawnSync } from 'node:child_process';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import allVerdictsExecutor, { verdictReport } from './executor.js';

const here = dirname(fileURLToPath(import.meta.url));
const executorModule = join(here, 'executor.ts');
const repoRoot = resolve(here, '../../../../..');
/** The workspace's own installed tool, quoted for the shell. */
const tool = (name: string): string => `'${join(repoRoot, 'node_modules', '.bin', name)}'`;

interface Finished {
  readonly stdout: string;
  readonly stderr: string;
  readonly code: number | null;
  readonly signal: NodeJS.Signals | null;
}

/**
 * The executor in a process of its own, as Nx runs it: how it ended, and a wait for text it prints. Its Nx state is
 * the fixture's, never the runner's: a file index the executor reads must not rewrite the archive of a workspace the
 * runner is part of.
 */
function startExecutor(
  root: string,
  commands: readonly string[],
  cwd = '.',
  options: { readonly eslintProjectRoot?: string } = {},
) {
  const script = [
    `const { default: run } = await import(${JSON.stringify(executorModule)});`,
    `const { success } = await run(${JSON.stringify({ commands, cwd, ...options })}, { root: ${JSON.stringify(root)} });`,
    'process.exitCode = success ? 0 : 1;',
  ].join('\n');
  const child = spawn(process.execPath, ['--eval', script], {
    cwd: root,
    env: { ...process.env, NX_WORKSPACE_DATA_DIRECTORY: join(root, '.nx', 'workspace-data') },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let stdout = '';
  let stderr = '';
  const waits: { readonly text: string; readonly printed: () => void }[] = [];
  child.stdout.setEncoding('utf8');
  child.stdout.on('data', (text: string) => {
    stdout += text;
    for (const wait of waits) if (stdout.includes(wait.text)) wait.printed();
  });
  child.stderr.setEncoding('utf8');
  child.stderr.on('data', (text: string) => {
    stderr += text;
  });
  const finished = new Promise<Finished>((settle) => {
    child.once('close', (code, signal) => settle({ stdout, stderr, code, signal }));
  });
  const printed = (text: string): Promise<void> =>
    new Promise((settle) => {
      if (stdout.includes(text)) settle();
      else waits.push({ text, printed: settle });
    });
  return { child, finished, printed };
}

async function withRoot(body: (root: string) => Promise<void>): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), 'all-verdicts-'));
  try {
    await body(root);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

async function put(root: string, path: string, text = 'export const value = 1;\n'): Promise<void> {
  await mkdir(dirname(join(root, path)), { recursive: true });
  await writeFile(join(root, path), text);
}

describe('all-verdicts executor', () => {
  it('lets Biome and ESLint each reach a verdict when both find errors in one run', () =>
    withRoot(async (root) => {
      await writeFile(join(root, 'checked.js'), 'debugger;\n');
      await writeFile(join(root, 'eslint.config.mjs'), "export default [{ rules: { 'no-debugger': 'error' } }];\n");
      const biome = `${tool('biome')} check checked.js`;
      const eslint = `${tool('eslint')} checked.js`;
      const { stdout, stderr, code } = await startExecutor(root, [biome, eslint]).finished;
      // Each tool's own diagnostics, not only its exit status: neither run ended the other.
      expect(stdout, stderr).toContain('lint/suspicious/noDebugger');
      expect(stdout, stderr).toContain("Unexpected 'debugger' statement");
      expect(stdout, stderr).toEndWith(
        verdictReport([
          { command: biome, verdict: { kind: 'fail', code: 1 } },
          { command: eslint, verdict: { kind: 'fail', code: 1 } },
        ]),
      );
      expect(code).toBe(1);
    }));

  it('reports a command a signal ended UNJUDGED by name, beside the verdicts of the rest', () =>
    withRoot(async (root) => {
      const killed = 'kill -TERM $$';
      const { stdout, stderr, code } = await startExecutor(root, [killed, 'exit 0', 'exit 2']).finished;
      expect(stdout, stderr).toEndWith(
        verdictReport([
          { command: killed, verdict: { kind: 'unjudged', cause: 'SIGTERM ended it before it gave a verdict' } },
          { command: 'exit 0', verdict: { kind: 'pass' } },
          { command: 'exit 2', verdict: { kind: 'fail', code: 2 } },
        ]),
      );
      expect(code).toBe(1);
    }));

  it('reports a command that never ran UNJUDGED by name', () =>
    withRoot(async (root) => {
      const { stdout, stderr, code } = await startExecutor(root, ['exit 0'], 'missing').finished;
      expect(stdout, stderr).toMatch(/\n {2}UNJUDGED +exit 0 \(it never ran: .+\)\n$/);
      expect(code).toBe(1);
    }));

  it('ends every command still running when the run is signalled, and reports each one UNJUDGED', () =>
    withRoot(async (root) => {
      const ready = join(root, 'ready.fifo');
      const lifetime = join(root, 'lifetime.fifo');
      expect(spawnSync('mkfifo', [ready, lifetime]).status).toBe(0);
      // The background process opens the lifetime FIFO before it says it is up, and only it (then the sleep it execs)
      // holds it, so its EOF observes that the signal reached the command's whole process group, not only the shell
      // that leads it. Were the shell to say it is up instead, the signal could end the background process before it
      // opened the FIFO, and nothing would ever open it for the test's read.
      const holding = '(echo up > ready.fifo; exec sleep 3600) 3>lifetime.fifo & wait';
      const closed = readFile(lifetime, 'utf8');
      const run = startExecutor(root, ['exit 3', holding]);
      expect(await readFile(ready, 'utf8')).toBe('up\n');
      await run.printed('\n> exit 3\n');
      run.child.kill('SIGTERM');
      const { stdout, stderr, signal } = await run.finished;
      expect(await closed).toBe('');
      expect(signal, stderr).toBe('SIGTERM');
      expect(stdout, stderr).toContain(`\n> ${holding}\n\n`);
      expect(stdout, stderr).toEndWith(
        verdictReport([
          { command: 'exit 3', verdict: { kind: 'fail', code: 3 } },
          {
            command: holding,
            verdict: { kind: 'unjudged', cause: 'the run received SIGTERM before it gave a verdict' },
          },
        ]),
      );
    }));

  it('hands a command the ESLint sources that exist when it runs, whatever the graph that named it knew', () =>
    withRoot(async (root) => {
      const project = 'packages/example';
      const listing = "printf '%s\\n' {eslintSources}";
      await put(root, `${project}/src/a.ts`);
      await put(root, `${project}/src/b.ts`);
      const before = await startExecutor(root, [listing], '.', { eslintProjectRoot: project }).finished;
      expect(before.stdout, before.stderr).toContain(
        `\n> printf '%s\\n' ${project}/src/a.ts ${project}/src/b.ts\n\n${project}/src/a.ts\n${project}/src/b.ts\n`,
      );

      // What the graph that named the files a moment ago never learns: one is deleted, one is added.
      await rm(join(root, project, 'src/b.ts'));
      await put(root, `${project}/src/c.ts`);
      const after = await startExecutor(root, [listing], '.', { eslintProjectRoot: project }).finished;
      expect(after.stdout, after.stderr).toContain(
        `\n> printf '%s\\n' ${project}/src/a.ts ${project}/src/c.ts\n\n${project}/src/a.ts\n${project}/src/c.ts\n`,
      );
      expect(after.stdout).not.toContain('b.ts');
      expect(after.code).toBe(0);
    }));

  it('passes a project with no ESLint sources without starting the command', () =>
    withRoot(async (root) => {
      const command = '{eslintSources}; exit 7';
      await put(root, 'packages/example/package.json', '{"name":"example"}\n');
      const { stdout, stderr, code } = await startExecutor(root, [command], '.', {
        eslintProjectRoot: 'packages/example',
      }).finished;
      expect(stdout, stderr).toContain(`\n> ${command}\n\nno ESLint sources under packages/example: nothing to lint\n`);
      expect(stdout, stderr).toEndWith(verdictReport([{ command, verdict: { kind: 'pass' } }]));
      expect(code).toBe(0);
    }));

  it('refuses a command that names the ESLint sources when the target names no project to resolve them for', () =>
    withRoot(async (root) => {
      const { stdout, stderr, code } = await startExecutor(root, ['eslint {eslintSources}']).finished;
      expect(stderr).toContain('all-verdicts: cannot resolve the commands to run: a command names {eslintSources}');
      expect(stderr).toContain('names no eslintProjectRoot');
      expect(stdout).toBe('');
      expect(code).toBe(1);
    }));

  it('refuses an empty command list, which could never fail', async () => {
    expect(await allVerdictsExecutor({ commands: [], cwd: '.' }, { root: tmpdir() })).toEqual({ success: false });
  });
});
