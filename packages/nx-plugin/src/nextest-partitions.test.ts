import { afterEach, expect, it } from 'bun:test';
import { type ChildProcess, spawn } from 'node:child_process';
import { mkdir, mkdtemp, readFile, realpath, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { TargetConfiguration } from 'nx/src/devkit-exports.js';
import { createNodesV2 } from './index.js';

it('executes every archived test once across ordinary and exceptional partitions', async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'smoo-nextest-partitions-')));
  fixtureRoot = root;
  const ordinary = Array.from({ length: 13 }, (_, index) => `ordinary_${index}`);
  // The classes the plugin's nextest.toml singles out with an override: a
  // compile-fail test and the real-APFS lifecycle test each carry a raised
  // slow-timeout, so each is lifted out of the ordinary hash.
  const exceptional = [
    ...Array.from({ length: 6 }, (_, index) => `compile_fail_partition_${index}`),
    ...Array.from({ length: 6 }, (_, index) => `partition_${index}_fail_to_compile`),
    'real_apfs_partition_substrate_lifecycle',
  ];
  const expected = [...ordinary, ...exceptional];
  await writeFile(join(root, 'package.json'), '{"name":"partition-fixture","private":true}\n');
  await writeFile(join(root, '.gitignore'), 'node_modules/\ntarget/\n.nx/\n*.log\n');
  await writeFile(
    join(root, 'Cargo.toml'),
    '[workspace]\nresolver = "2"\n\n[package]\nname = "partition-probe"\nversion = "0.1.0"\nedition = "2021"\n\n[package.metadata.smoothbricks.test]\nshards = 3\n',
  );
  await mkdir(join(root, 'src'));
  await writeFile(
    join(root, 'src/lib.rs'),
    `#[cfg(test)] mod tests {
    use std::io::Write;
    fn record(name: &str) {
        let mut file = std::fs::OpenOptions::new().create(true).append(true)
            .open(std::env::var("PARTITION_EXECUTIONS").unwrap()).unwrap();
        file.write_all(format!("{name}\\n").as_bytes()).unwrap();
    }
${expected.map((name) => `    #[test] fn ${name}() { record("${name}"); }`).join('\n')}
}
`,
  );
  await symlink(join(import.meta.dir, '../../../node_modules'), join(root, 'node_modules'), 'dir');
  await run(root, ['git', 'init', '--quiet']);
  await run(root, ['cargo', 'generate-lockfile', '--offline']);
  const [, infer] = createNodesV2;
  const result = await infer(['package.json'], undefined, { workspaceRoot: root, nxJsonConfiguration: {} });
  const targets = result[0]?.[1].projects?.['.']?.targets;
  if (targets === undefined) throw new Error('Cargo fixture inferred no targets');
  // The archive is the one real prerequisite: every runner executes its binaries.
  await run(root, ['sh', '-c', command(targets, 'cargo-test-archive')]);
  // One log per partition kind, so the proof is exact selection and not only
  // coverage: the ordinary shards ran every ordinary test once and nothing
  // else, and the exceptional shards did the same for theirs. The six runners
  // are independent, so they run side by side exactly as Nx fans them out.
  const partitions = [
    { kind: 'shard', log: join(root, 'ordinary.log'), tests: ordinary },
    { kind: 'exceptions-shard', log: join(root, 'exceptional.log'), tests: exceptional },
  ];
  await Promise.all(
    partitions.flatMap(({ kind, log }) =>
      [1, 2, 3].map((index) =>
        run(root, ['sh', '-c', command(targets, `cargo-test-partition-probe-${kind}${index}`)], {
          PARTITION_EXECUTIONS: log,
        }),
      ),
    ),
  );
  for (const { log, tests } of partitions) {
    const observed = (await readFile(log, 'utf8')).trim().split('\n').sort();
    expect(observed).toEqual([...tests].sort());
  }
});

function command(targets: Readonly<Record<string, TargetConfiguration>>, name: string): string {
  const single: unknown = targets[name]?.options?.command;
  if (typeof single !== 'string') throw new Error(`${name} inferred no executable command`);
  return single;
}

/** A child this test started and has not yet seen close, with the command line that names it. */
interface OpenChild {
  readonly command: string;
  readonly closed: Promise<unknown>;
}

const openChildren = new Map<ChildProcess, OpenChild>();
let fixtureRoot: string | undefined;

/**
 * The only teardown, for a pass, a failure and the per-test deadline alike.
 *
 * The deadline abandons the test body mid-`await`, so the body cannot be
 * trusted to clean up after itself. This hook signals each open child's whole
 * process group (the same detached-group kill the bounded executor uses, since
 * `sh -c` leaves cargo and nextest as grandchildren), waits for every one to
 * close, and only then deletes the fixture they were running in. It then
 * names what was still open instead of leaving a bare timeout.
 */
afterEach(async () => {
  const open = [...openChildren.values()];
  for (const child of openChildren.keys()) killProcessGroup(child);
  openChildren.clear();
  await Promise.allSettled(open.map((child) => child.closed));
  if (fixtureRoot !== undefined) {
    const root = fixtureRoot;
    fixtureRoot = undefined;
    await rm(root, { recursive: true, force: true });
  }
  if (open.length > 0) {
    throw new Error(`test ended with child processes still open:\n${open.map((child) => child.command).join('\n')}`);
  }
});

function killProcessGroup(child: ChildProcess): void {
  if (child.pid === undefined) return;
  try {
    process.kill(process.platform === 'win32' ? child.pid : -child.pid, 'SIGKILL');
  } catch (error) {
    if (!(error instanceof Error && 'code' in error && error.code === 'ESRCH')) throw error;
  }
}

async function run(root: string, args: string[], overlay: Record<string, string> = {}): Promise<void> {
  const [program, ...rest] = args;
  if (program === undefined) throw new Error('run needs a program');
  const child = spawn(program, rest, {
    cwd: root,
    env: { ...process.env, ...overlay },
    detached: process.platform !== 'win32',
  });
  child.stdin?.end();
  const output: Buffer[] = [];
  child.stdout?.on('data', (chunk: Buffer) => output.push(chunk));
  child.stderr?.on('data', (chunk: Buffer) => output.push(chunk));
  const closed = new Promise<number | null>((resolve, reject) => {
    child.once('error', reject);
    child.once('close', resolve);
  });
  openChildren.set(child, { command: args.join(' '), closed });
  let code: number | null;
  try {
    code = await closed;
  } finally {
    openChildren.delete(child);
  }
  expect(code, `${args.join(' ')}\n${Buffer.concat(output).toString()}`).toBe(0);
}
