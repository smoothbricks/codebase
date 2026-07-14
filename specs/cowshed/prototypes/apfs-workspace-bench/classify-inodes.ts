import { basename, relative, resolve, sep } from 'node:path';
import { $ } from 'bun';
import { Command } from 'commander';

const program = new Command()
  .name('classify-inodes.ts')
  .description(
    'Classifies filesystem objects in a directory whose immediate children are workspaces.\nGNU du is invoked through Bun Shell and never crosses filesystem boundaries.',
  )
  .argument('<directory>', 'directory whose immediate children are workspaces')
  .parse(Bun.argv);

const target = resolve(program.args[0]);

const duVersion = await $`du --version`.nothrow().quiet();
if (duVersion.exitCode !== 0 || !duVersion.text().includes('GNU coreutils')) {
  throw new Error('GNU du is required; this Mac normally provides it through Nix/Home Manager');
}

const output = await $`du --inodes -x --max-depth=2 ${target}`.text();
const rows = output
  .trim()
  .split('\n')
  .map((line) => {
    const separator = line.indexOf('\t');
    return { count: Number(line.slice(0, separator)), path: line.slice(separator + 1) };
  });
const total = rows.find((row) => row.path === target)?.count;
if (total === undefined) throw new Error(`du did not report ${target}`);

const workspaces = rows.filter((row) => {
  const path = relative(target, row.path);
  return path.length > 0 && !path.includes(sep);
});
const categories = new Map<string, number>();
for (const row of rows) {
  const path = relative(target, row.path);
  if (path.split(sep).length !== 2) continue;
  const category = basename(row.path);
  categories.set(category, (categories.get(category) ?? 0) + row.count);
}

const classified = [...categories.entries()]
  .map(([category, objects]) => ({
    category,
    objects,
    'share of root': `${((objects / total) * 100).toFixed(2)}%`,
  }))
  .sort((left, right) => right.objects - left.objects);

console.log(`Root: ${target}`);
console.log(`Filesystem objects: ${total.toLocaleString()}`);
console.log(`Immediate workspaces: ${workspaces.length.toLocaleString()}`);
console.table(classified);
