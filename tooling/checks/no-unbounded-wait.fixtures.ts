/// <reference types="bun" />
// Planted fixtures for tooling/checks/no-unbounded-wait.ts: a small typed program in a temp directory, linted by the
// real rules through the real typescript-eslint parser against the workspace's own @types/node, bun-types and lmao
// Deadline module, so each unbounded spelling is refused, its bounded spelling passes, and a directive without its
// reason is itself refused. A rule that stopped firing looks exactly like a clean tree, so every refusal is pinned.
//
// (kept as fixtures, not *.test.ts: the name must stay invisible to bun test globs, since no package tsconfig owns
// this directory.)
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { ESLint } from 'eslint';
import tseslint from 'typescript-eslint';
import { deadlinePlugin } from './no-unbounded-wait.ts';

const workspace = dirname(dirname(import.meta.dir));
const root = mkdtempSync(join(tmpdir(), 'no-unbounded-wait-'));
let failures = 0;

const fixtures: Record<string, string> = {
  'tsconfig.json': JSON.stringify({
    compilerOptions: {
      strict: true,
      noEmit: true,
      target: 'ES2022',
      lib: ['ES2022', 'ES2024.Promise'],
      module: 'Preserve',
      moduleResolution: 'Bundler',
      typeRoots: [join(workspace, 'node_modules/@types')],
      types: ['node', 'bun'],
      paths: {
        '@smoothbricks/lmao/result': [join(workspace, 'packages/lmao/src/lib/result.ts')],
        '@smoothbricks/lmao/errors/Deadline': [join(workspace, 'packages/lmao/src/lib/errors/Deadline.ts')],
      },
      skipLibCheck: true,
    },
    include: ['src/**/*.ts'],
  }),
  'src/fetch.ts': `export async function get(url: string, init: RequestInit, timed: { signal: AbortSignal }) {
  await fetch(url);
  await fetch(url, { method: 'POST' });
  await fetch(url, init);
  await fetch(url, { signal: AbortSignal.timeout(5_000) });
  await fetch(url, timed);
  await fetch(url, { ...timed, method: 'POST' });
}
`,
  'src/connect.ts': `import net, { connect, createConnection, Socket } from 'node:net';
import tls from 'node:tls';
export function open(path: string) {
  connect(path);
  net.createConnection(path);
  createConnection({ path });
  new Socket().connect(path);
  tls.connect(443, 'example.com');
  Bun.connect({ unix: path, socket: { data() {} } });
}
function connectLocal(path: string) {
  return path;
}
connectLocal('not a socket');
`,
  'src/children.ts': `import { exec, execFile, execFileSync, execSync, spawnSync } from 'node:child_process';
const options = { timeout: 5_000 } as const;
export function run(file: string) {
  spawnSync(file, ['--version']);
  execFileSync(file, ['--version'], { encoding: 'utf8' });
  execSync(file);
  execFile(file, ['--version'], () => {});
  exec(file, () => {});
  Bun.spawnSync({ cmd: [file] });
  Bun.spawnSync([file]);
  spawnSync(file, ['--version'], { timeout: 5_000 });
  execFileSync(file, [], options);
  execFile(file, [], { timeout: 5_000 }, () => {});
  Bun.spawnSync({ cmd: [file], timeout: 5_000 });
}
`,
  'src/exit.ts': `import { spawn } from 'node:child_process';
import { EventEmitter, once } from 'node:events';
import { connect } from 'node:net';
export async function wait(file: string, signal: AbortSignal) {
  const proc = Bun.spawn([file]);
  await proc.exited;
  const child = spawn(file);
  await once(child, 'close');
  await once(child, 'exit', { signal });
  await once(new EventEmitter(), 'ready');
  // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: the gateway owns the socket's end
  await once(connect('/gateway.sock'), 'close');
}
`,
  'src/promises.ts': `import { spawn } from 'node:child_process';
import { Socket } from 'node:net';
export function closed(file: string, socket: Socket, abort: AbortSignal) {
  const child = spawn(file);
  const onlyPeer = new Promise<number | null>((resolve, reject) => {
    child.once('error', reject);
    child.once('close', (code) => resolve(code));
  });
  const passed = new Promise((resolve) => child.once('exit', resolve));
  const written = new Promise<void>((resolve) => {
    socket.write('hello', () => resolve());
  });
  const { promise, resolve } = Promise.withResolvers<void>();
  child.stdout?.on('end', resolve);
  const timed = new Promise<void>((resolve, reject) => {
    child.once('close', () => resolve());
    setTimeout(() => reject(new Error('late')), 5_000);
  });
  const aborted = new Promise<void>((resolve) => {
    child.once('close', () => resolve());
    abort.addEventListener('abort', () => resolve());
  });
  const immediate = new Promise<number>((resolve) => resolve(1));
  let escaped: (() => void) | undefined;
  const stored = new Promise<void>((resolve) => {
    escaped = resolve;
  });
  const pause = new Promise((resolve) => setTimeout(resolve, 1));
  return [onlyPeer, passed, written, promise, timed, aborted, immediate, stored, escaped, pause];
}
`,
  'src/named.ts': `import { spawn } from 'node:child_process';
export function settled(file: string) {
  const child = spawn(file);
  return new Promise<number | null>((resolve, reject) => {
    function onError(error: Error) {
      child.removeListener('close', onClose);
      reject(error);
    }
    const onClose = (code: number | null) => resolve(code);
    child.once('error', onError);
    child.once('close', onClose);
  });
}
export function stored(file: string) {
  const child = spawn(file);
  return new Promise<void>((resolve) => {
    const done = () => resolve();
    child.once('close', done);
    setTimeout(done, 5_000);
  });
}
`,
  'src/bounded.ts': `import { spawn, spawnSync } from 'node:child_process';
import { once } from 'node:events';
import { connect } from 'node:net';
import { deadline, idle } from '@smoothbricks/lmao/errors/Deadline';
import { Ok } from '@smoothbricks/lmao/result';
const CONTROL = { class: 'control', withinMs: 5_000 } as const;
export async function bounded(path: string, file: string, url: string) {
  await deadline('status', path, CONTROL, async (wait) => {
    const socket = connect(path);
    wait.signal.addEventListener('abort', () => socket.destroy());
    await once(socket, 'connect');
    await new Promise<void>((resolve) => socket.once('data', () => resolve()));
    await Bun.spawn([file]).exited;
    await fetch(url);
    spawnSync(file);
    return new Ok(undefined);
  });
  await idle('job', file, 15_000, async (wait) => {
    const child = spawn(file);
    child.stdout?.on('data', () => wait.progress({ kind: 'state', state: 'running' }));
    await once(child, 'close');
    return new Ok(undefined);
  });
}
`,
  'src/directives.ts': `import { connect } from 'node:net';
export function directives(path: string) {
  // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: the supervisor's EOF ends this wait
  connect(path);
  // eslint-disable-next-line deadline/no-unbounded-wait
  connect(path);
  // eslint-disable-next-line deadline/no-unbounded-wait -- the peer is fast
  connect(path);
  connect(path); // eslint-disable-line deadline/no-unbounded-wait -- WHY unbounded: the supervisor's EOF ends this wait
  // eslint-disable-next-line
  connect(path);
}
`,
};

try {
  for (const [file, source] of Object.entries(fixtures)) {
    mkdirSync(dirname(join(root, file)), { recursive: true });
    writeFileSync(join(root, file), source);
  }
  const eslint = new ESLint({
    cwd: root,
    overrideConfigFile: true,
    overrideConfig: [
      {
        files: ['src/**/*.ts'],
        languageOptions: {
          parser: tseslint.parser,
          parserOptions: { projectService: true, tsconfigRootDir: root },
        },
        plugins: { deadline: deadlinePlugin },
        linterOptions: { reportUnusedDisableDirectives: 'error' },
        rules: { 'deadline/no-unbounded-wait': 'error', 'deadline/unbounded-wait-why': 'error' },
      },
    ],
  });
  const results = await eslint.lintFiles(['src/**/*.ts']);
  /** Each finding as `<line> <rule id>`, the id each message opens with (`directive` for ESLint's own). */
  const findings = (file: string): string[] => {
    const result = results.find((candidate) => candidate.filePath === join(root, 'src', file));
    if (result === undefined) return [`no lint result for ${file}`];
    return result.messages.map((message) =>
      message.fatal === true
        ? `${message.line} fatal: ${message.message}`
        : `${message.line} ${message.ruleId === null ? 'directive' : message.message.slice(0, message.message.indexOf(':'))}`,
    );
  };
  const expected: Record<string, readonly string[]> = {
    // fetch without a signal is refused; one that carries it (literal, typed value, spread) passes.
    'fetch.ts': ['2 WAIT-FETCH', '3 WAIT-FETCH', '4 WAIT-FETCH'],
    // Every node, tls and Bun socket connect is refused, never a local namesake.
    'connect.ts': [
      '4 WAIT-CONNECT',
      '5 WAIT-CONNECT',
      '6 WAIT-CONNECT',
      '7 WAIT-CONNECT',
      '8 WAIT-CONNECT',
      '9 WAIT-CONNECT',
    ],
    // A child call that waits for its end without timeout is refused.
    'children.ts': [
      '4 WAIT-SYNC-CHILD',
      '5 WAIT-SYNC-CHILD',
      '6 WAIT-SYNC-CHILD',
      '7 WAIT-SYNC-CHILD',
      '8 WAIT-SYNC-CHILD',
      '9 WAIT-SYNC-CHILD',
      '10 WAIT-SYNC-CHILD',
    ],
    // A child's exit and a peer's event without a signal are refused; a kept wait with its reason passes.
    'exit.ts': ['6 WAIT-EXIT', '8 WAIT-EVENT'],
    // A promise settled only from a peer callback is refused, never one a timer, an abort or its body can settle.
    'promises.ts': ['5 WAIT-PROMISE', '9 WAIT-PROMISE', '10 WAIT-PROMISE', '13 WAIT-PROMISE'],
    // A named listener is followed to the peer calls that run it.
    'named.ts': ['4 WAIT-PROMISE'],
    // Inside deadline and idle every wait passes but a signal-less fetch and a synchronous child.
    'bounded.ts': ['14 WAIT-FETCH', '15 WAIT-SYNC-CHILD'],
    // A directive that disables the rule without its scope or its reason is refused.
    'directives.ts': ['5 WAIT-WHY', '7 WAIT-WHY', '9 WAIT-WHY', '10 WAIT-WHY'],
  };
  for (const [file, want] of Object.entries(expected)) {
    const got = findings(file);
    if (JSON.stringify(got) !== JSON.stringify(want)) {
      failures += 1;
      console.error(`FAIL ${file}\n  expected: ${JSON.stringify(want)}\n  actual:   ${JSON.stringify(got)}`);
    }
  }
} finally {
  rmSync(root, { recursive: true, force: true });
}
if (failures > 0) {
  console.error(`no-unbounded-wait fixtures: ${failures} failed`);
  process.exit(1);
}
console.log('no-unbounded-wait fixtures: every planted wait refused, every bounded one passed');
