import { spawnSync } from 'node:child_process';
import { readFileSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { checkBendFile } from '@smoothbricks/bend-emit';

const modelName = 'specs/cowshed/laws/land_turn.bend';
const model = new URL(`../../../${modelName}`, import.meta.url);
const kernel = new URL('../crates/cowshed-gateway/src/land_turn/laws/model.rs', import.meta.url);
const functions = [
  'step',
  'run',
  'empty',
  'queue',
  'counter',
  'fences',
  'waiting',
  'parked',
  'find',
  'inside',
  'turn_ok_b',
  'chain_b',
];
const laws = ['one_holder_check', 'fifo_check'];
const cap = 400;
const properties = 2000;
const propertyLength = 24;
const book = await checkBendFile(fileURLToPath(model));
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
if (emission.kind !== 'module') throw new Error('land-turn law: expected a Rust module');
const formatted = spawnSync('rustfmt', ['--edition', '2024'], {
  input: emission.source,
  encoding: 'utf8',
  timeout: 30_000,
});
if (formatted.error || formatted.status !== 0) {
  throw new Error(`land-turn law: rustfmt failed (exit ${formatted.status}): ${formatted.stderr}`, {
    cause: formatted.error,
  });
}
if (process.argv.includes('--check')) {
  if (readFileSync(kernel, 'utf8') !== formatted.stdout) {
    throw new Error('land-turn law: kernel differs; run bun run --filter @smoothbricks/cowshed emit-land-turn-law');
  }
} else {
  writeFileSync(kernel, formatted.stdout);
}
