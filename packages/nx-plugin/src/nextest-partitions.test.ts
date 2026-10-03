import { expect, it } from 'bun:test';
import { mkdir, mkdtemp, readFile, realpath, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { TargetConfiguration } from 'nx/src/devkit-exports.js';
import { createNodesV2 } from './index.js';

it('executes every archived test once across ordinary and exceptional partitions', async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'smoo-nextest-partitions-')));
  const ordinary = Array.from({ length: 13 }, (_, index) => `ordinary_${index}`);
  const exceptional = [
    ...Array.from({ length: 11 }, (_, index) => `real_apfs_partition_${index}`),
    'compile_fail_partition',
    'real_apfs_partition_substrate_lifecycle',
  ];
  const expected = [...ordinary, ...exceptional].sort();
  try {
    await writeFile(join(root, 'package.json'), '{"name":"partition-fixture","private":true}\n');
    await writeFile(join(root, '.gitignore'), 'node_modules/\ntarget/\n.nx/\nexecutions.log\n');
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
    for (const script of commands(targets, 'cargo-test-archive')) {
      await run(root, ['sh', '-c', script]);
    }
    const executions = join(root, 'executions.log');
    for (const kind of ['shard', 'exceptions-shard']) {
      for (const index of [1, 2, 3]) {
        for (const script of commands(targets, `cargo-test-partition-probe-${kind}${index}`)) {
          await run(root, ['sh', '-c', script], { PARTITION_EXECUTIONS: executions });
        }
      }
    }
    const observed = (await readFile(executions, 'utf8')).trim().split('\n').sort();
    expect(observed).toEqual(expected);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

function commands(targets: Readonly<Record<string, TargetConfiguration>>, name: string): readonly string[] {
  const single: unknown = targets[name]?.options?.command;
  if (typeof single === 'string') return [single];
  const multiple: unknown = targets[name]?.options?.commands;
  if (isCommandList(multiple)) return multiple;
  throw new Error(`${name} inferred no executable commands`);
}

function isCommandList(value: unknown): value is string[] {
  return Array.isArray(value) && value.every((item: unknown) => typeof item === 'string');
}

async function run(root: string, args: string[], overlay: Record<string, string> = {}): Promise<void> {
  const child = Bun.spawn(args, { cwd: root, env: { ...process.env, ...overlay }, stdout: 'pipe', stderr: 'pipe' });
  const [code, stdout, stderr] = await Promise.all([
    child.exited,
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
  ]);
  expect(code, `${args.join(' ')}\n${stdout}${stderr}`).toBe(0);
}
