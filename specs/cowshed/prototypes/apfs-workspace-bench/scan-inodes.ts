import { stat } from 'node:fs/promises';
import { resolve } from 'node:path';
import { $ } from 'bun';
import { Command, InvalidArgumentError } from 'commander';

function parsePositiveInteger(value: string): number {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new InvalidArgumentError('must be a positive integer');
  }
  return parsed;
}

const program = new Command()
  .name('scan-inodes.ts')
  .description(
    'Reports filesystem-object counts per subtree.\nUses GNU du through Bun Shell. It does not cross filesystem boundaries.',
  )
  .argument('[path]', 'directory to scan (default: current directory)')
  .option('--depth <n>', 'maximum report depth', parsePositiveInteger, 2)
  .option('--limit <n>', 'maximum output rows', parsePositiveInteger, 50)
  .option('--threshold <n>', 'omit trees below N filesystem objects', parsePositiveInteger, 0)
  .parse(Bun.argv);

const { depth, limit, threshold } = program.opts();
const target = resolve(program.args[0] ?? process.cwd());

if (!(await stat(target).catch(() => null))) {
  throw new Error(`${target} does not exist`);
}

const duVersion = await $`du --version`.nothrow().quiet();
if (duVersion.exitCode !== 0 || !duVersion.text().includes('GNU coreutils')) {
  throw new Error('GNU du is required; this Mac normally provides it through Nix/Home Manager');
}

const report =
  threshold > 0
    ? await $`du --inodes -x --max-depth=${depth} --threshold=${threshold} ${target} | sort -nr | head -n ${limit}`.text()
    : await $`du --inodes -x --max-depth=${depth} ${target} | sort -nr | head -n ${limit}`.text();

console.log(report.trimEnd());
