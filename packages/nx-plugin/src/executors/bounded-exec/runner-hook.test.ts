import { afterEach, describe, expect, it } from 'bun:test';
import { type ChildProcess, spawn } from 'node:child_process';
import { createInterface } from 'node:readline';
import { Duplex, type Readable } from 'node:stream';
import { fileURLToPath } from 'node:url';

const hook = fileURLToPath(new URL('./runner-hook.ts', import.meta.url));
const children = new Set<ChildProcess>();

afterEach(() => {
  for (const child of children) child.kill('SIGKILL');
  children.clear();
});

async function collect(stream: Readable): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of stream) {
    const bytes: unknown = chunk;
    if (!Buffer.isBuffer(bytes)) throw new Error('expected subprocess bytes');
    chunks.push(bytes);
  }
  return Buffer.concat(chunks);
}

async function runHook(
  runner: 'cargo' | 'go',
  executable: string,
  args: string[],
  acknowledge: (line: string) => string = (line) => `${line}\n`,
) {
  // Node's standalone TypeScript loader needs no project preloader or validation transform.
  const child = spawn('node', [hook, '7', runner, executable, ...args], {
    stdio: ['ignore', 'pipe', 'pipe', 'pipe'],
  });
  children.add(child);
  const closed = Promise.withResolvers<{ code: number | null; signal: NodeJS.Signals | null }>();
  child.once('error', closed.reject);
  child.once('close', (code, signal) => {
    children.delete(child);
    closed.resolve({ code, signal });
  });
  const control = child.stdio[3];
  if (!(control instanceof Duplex) || child.stdout === null || child.stderr === null) {
    throw new Error('runner-hook fixture needs stdout, stderr, and duplex descriptor 3');
  }
  const phases: unknown[] = [];
  const protocol = async (): Promise<void> => {
    try {
      for await (const line of createInterface({ input: control })) {
        const message: unknown = JSON.parse(line);
        phases.push(message);
        control.write(acknowledge(line));
      }
    } finally {
      control.destroy();
    }
  };
  const [status, stdout, stderr] = await Promise.all([
    closed.promise,
    collect(child.stdout),
    collect(child.stderr),
    protocol(),
  ]);
  return { ...status, stdout, stderr, phases };
}

/** A real process emitting exact protocol fragments, not a parser imported into the test process. */
function emit(stdout: readonly string[], stderr = '', ending = 'process.exitCode = 0;'): string {
  return [
    "const { writeSync } = require('node:fs');",
    `writeSync(2, ${JSON.stringify(stderr)});`,
    `for (const fragment of ${JSON.stringify(stdout)}) writeSync(1, fragment);`,
    ending,
  ].join('\n');
}

describe('standalone native runner hook', () => {
  it('accepts Cargo build-finished(false) once, ignores compiler/text/nested terminals, and preserves bytes and status', async () => {
    const stdout = [
      '{"reason":"compiler-artifact","fresh":true}\n',
      '{"reason":"build-finished","success":"true"}\n',
      '{not JSON}\n',
      '{"reason":"build-fin',
      'ished","success":false}\n',
      'test result: ok. 1 passed; 0 failed\n',
      '{"Action":"pass","Package":"nested"}\n',
      '{"reason":"build-finished","success":true}\n',
      'custom harness teardown without newline',
    ];
    const stderr = '{"reason":"build-finished","success":true}\ncompiler diagnostic\n';
    const program =
      "require('node:fs').writeSync(1, Buffer.from([0, 255, 128, 10]));\n" +
      emit(stdout, stderr, 'process.exitCode = 37;');
    const result = await runHook('cargo', process.execPath, ['-e', program]);
    expect(result.code).toBe(37);
    expect(result.signal).toBeNull();
    expect(result.stdout).toEqual(Buffer.concat([Buffer.from([0, 255, 128, 10]), Buffer.from(stdout.join(''))]));
    expect(result.stderr).toEqual(Buffer.from(stderr));
    expect(result.phases).toEqual([
      { id: 7, phase: 'build' },
      { id: 7, phase: 'active' },
      { id: 7, phase: 'terminal' },
    ]);
  });

  it('tracks overlapping Go packages, not test terminals, stderr, build output, or nested output', async () => {
    const stdout = [
      '{"Action":"build-output","ImportPath":"one","Output":"linking\\n"}\n',
      '{"Action":"sta',
      'rt","Package":"one"}\n',
      '{"Action":"start","Package":"two"}\n',
      '{"Action":"pass","Package":"one","Test":"TestDone"}\n',
      '{"Action":"output","Package":"one","Output":"{\\"Action\\":\\"pass\\",\\"Package\\":\\"two\\"}\\n"}\n',
      '{"Action":"fail","Package":"one"}\n',
      '{"Action":"skip","Package":"two"}\n',
      '{"Action":"start","Package":"three"}\n',
      '{"Action":"pass","Package":"three"}',
    ];
    const stderr = '{"Action":"start","Package":"stderr-is-not-native"}\n';
    const result = await runHook('go', process.execPath, ['-e', emit(stdout, stderr)]);
    expect(result.code).toBe(0);
    expect(result.stdout).toEqual(Buffer.from(stdout.join('')));
    expect(result.stderr).toEqual(Buffer.from(stderr));
    expect(result.phases).toEqual([
      { id: 7, phase: 'build' },
      { id: 7, phase: 'active' },
      { id: 7, phase: 'build' },
      { id: 7, phase: 'active' },
      { id: 7, phase: 'build' },
      { id: 7, phase: 'terminal' },
    ]);
  });

  it('preserves a child termination signal after draining output and acknowledging terminal', async () => {
    const stdout = ['{"reason":"build-finished","success":true}\n', 'last stdout\n'];
    const result = await runHook('cargo', process.execPath, [
      '-e',
      emit(stdout, 'last stderr\n', "process.kill(process.pid, 'SIGTERM');"),
    ]);
    expect(result.code).toBeNull();
    expect(result.signal).toBe('SIGTERM');
    expect(result.stdout).toEqual(Buffer.from(stdout.join('')));
    expect(result.stderr.toString()).toBe('last stderr\n');
    expect(result.phases).toEqual([
      { id: 7, phase: 'build' },
      { id: 7, phase: 'active' },
      { id: 7, phase: 'terminal' },
    ]);
  });

  it('reports spawn failure, acknowledges terminal, and exits nonzero', async () => {
    const result = await runHook('cargo', `/runner-hook-missing-executable-${process.pid}`, []);
    expect(result.code).toBe(1);
    expect(result.signal).toBeNull();
    expect(result.stdout.length).toBe(0);
    expect(result.stderr.toString()).toContain('runner-hook:');
    expect(result.stderr.toString()).toContain('runner-hook-missing-executable-');
    expect(result.phases).toEqual([
      { id: 7, phase: 'build' },
      { id: 7, phase: 'terminal' },
    ]);
  });

  it('does not start the child until its initial build phase is acknowledged correctly', async () => {
    const result = await runHook(
      'cargo',
      process.execPath,
      ['-e', emit(['child must not run\n'])],
      () => '{"id":8,"phase":"build"}\n',
    );
    expect(result.code).toBe(1);
    expect(result.stdout.length).toBe(0);
    expect(result.stderr.toString()).toContain('unexpected lifecycle acknowledgement');
    expect(result.phases).toEqual([{ id: 7, phase: 'build' }]);
  });

  it('withholds the active record and subsequent stdout when its acknowledgement is invalid', async () => {
    const compiler = '{"reason":"compiler-artifact","fresh":true}\n';
    const result = await runHook(
      'cargo',
      process.execPath,
      ['-e', emit([compiler, '{"reason":"build-finished","success":true}\n', 'fixture ready\n'])],
      (line) => (line.includes('"phase":"active"') ? '{"id":8,"phase":"active"}\n' : `${line}\n`),
    );
    expect(result.code).toBe(1);
    expect(result.stdout.toString()).toBe(compiler);
    expect(result.stderr.toString()).toContain('unexpected lifecycle acknowledgement');
    expect(result.phases).toEqual([
      { id: 7, phase: 'build' },
      { id: 7, phase: 'active' },
    ]);
  });
});
