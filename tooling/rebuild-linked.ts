#!/usr/bin/env bun
/**
 * After a land moves main: rebuild exactly the build units developer links judge stale in this checkout, so a link
 * from another checkout keeps describing HEAD (packages/nx-plugin/managed/raw/tooling/direnv/developer-links.ts,
 * staleBuilds). The list is the judge's own: every workspace package's stale units and the rebuild each names, run
 * once each. After a gated land every one is an Nx cache hit, so the window in which a link refuses is seconds.
 */
import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { type StaleBuild, staleBuilds } from './direnv/developer-links.js';

/** The rebuilds `judge` names for the workspace packages under `root`, each once, in a stable order. */
export function linkedRebuilds(
  root: string,
  judge: (packageName: string, directory: string) => readonly StaleBuild[],
): string[] {
  const rebuilds = new Set<string>();
  for (const entry of readdirSync(join(root, 'packages'), { withFileTypes: true })) {
    if (!entry.isDirectory()) continue;
    const directory = join(root, 'packages', entry.name);
    const manifestPath = join(directory, 'package.json');
    if (!existsSync(manifestPath)) continue;
    const manifest: unknown = JSON.parse(readFileSync(manifestPath, 'utf8'));
    const name = typeof manifest === 'object' && manifest !== null ? Reflect.get(manifest, 'name') : undefined;
    if (typeof name !== 'string') continue;
    for (const stale of judge(name, directory)) rebuilds.add(stale.rebuild);
  }
  return [...rebuilds].sort();
}

if (import.meta.main) {
  for (const rebuild of linkedRebuilds(process.cwd(), staleBuilds)) {
    process.stderr.write(`rebuild-linked: ${rebuild}\n`);
    const run = Bun.spawnSync(['bun', ...rebuild.split(' ')], { stdio: ['inherit', 'inherit', 'inherit'] });
    if (run.exitCode !== 0) process.exit(run.exitCode ?? 1);
  }
}
