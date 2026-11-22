import { spawnSync } from 'node:child_process';
import { readFileSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { checkBendFile } from '@smoothbricks/bend-emit';

// The ZFS substrate's model (09_substrates.md, "Privileged helper" and selection) binds two crates:
// the root helper's admission and the store's selection, each through a kernel of its own.
const modelName = 'specs/cowshed/laws/zfs_substrate.bend';
const model = new URL(`../../../${modelName}`, import.meta.url);
const kernels = [
  {
    path: new URL('../crates/cowshed-helper/src/laws/model.rs', import.meta.url),
    functions: ['plan', 'admits', 'names', 'planned', 'named', 'root_names'],
    laws: ['a_plan_stays_below_its_root', 'admit_check'],
  },
  {
    path: new URL('../crates/cowshed-core/src/storage/directory/zfs/laws/model.rs', import.meta.url),
    functions: ['select', 'needs_serving', 'needs_holding', 'root_names'],
    laws: ['a_file_main_check'],
  },
];
const cap = 400;
const properties = 2000;
const propertyLength = 24;
const book = await checkBendFile(fileURLToPath(model));
const stale: string[] = [];
for (const { path, functions, laws } of kernels) {
  const emission = book.emit({
    target: {
      kind: 'rust-module',
      modelName,
      flags: [
        '--fns',
        functions.join(','),
        '--laws',
        laws.join(','),
        '--cap',
        String(cap),
        '--props',
        String(properties),
        '--prop-len',
        String(propertyLength),
        '--rust-module',
        '--model-name',
        modelName,
      ],
    },
    functions,
    laws,
    cap,
    properties,
    propertyLength,
  });
  if (emission.kind !== 'module') throw new Error('zfs-substrate law: expected a Rust module');
  const formatted = spawnSync('rustfmt', ['--edition', '2024'], {
    input: emission.source,
    encoding: 'utf8',
    timeout: 30_000,
  });
  if (formatted.error || formatted.status !== 0) {
    throw new Error(`zfs-substrate law: rustfmt failed (exit ${formatted.status}): ${formatted.stderr}`, {
      cause: formatted.error,
    });
  }
  if (process.argv.includes('--check')) {
    if (readFileSync(path, 'utf8') !== formatted.stdout) stale.push(fileURLToPath(path));
  } else {
    writeFileSync(path, formatted.stdout);
  }
}
if (stale.length > 0) {
  throw new Error(
    `zfs-substrate law: ${stale.join(', ')} differ; run bun run --filter @smoothbricks/cowshed emit-zfs-substrate-law`,
  );
}
