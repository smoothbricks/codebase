import { expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { chmodSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';

// spec16 §One Cargo environment: the same PATH program links at every checkout,
// and an exported SDK avoids all per-link Xcode discovery.
it.skipIf(process.platform !== 'darwin')(
  'links runnable binaries with zero discovery for SDKROOT and one fallback xcrun',
  () => {
    const root = mkdtempSync(join(tmpdir(), 'smoo-linker-'));
    try {
      const repo = resolve(import.meta.dir, '../../../..');
      const linker = join(repo, 'packages/cowshed/scripts/macos-linker.sh');
      const sdk = spawnSync('/usr/bin/xcrun', ['--sdk', 'macosx', '--show-sdk-path'], { encoding: 'utf8' });
      expect(sdk.status, sdk.stderr).toBe(0);
      const bin = join(root, 'bin');
      const calls = join(root, 'calls');
      mkdirSync(bin);
      writeFileSync(join(bin, 'xcrun'), '#!/bin/sh\nprintf "xcrun\\n" >> "$LINKER_CALLS"\nexec /usr/bin/xcrun "$@"\n');
      writeFileSync(join(bin, 'xcode-select'), '#!/bin/sh\nprintf "xcode-select\\n" >> "$LINKER_CALLS"\nexit 1\n');
      for (const program of ['xcrun', 'xcode-select']) chmodSync(join(bin, program), 0o755);
      const source = join(root, 'probe.c');
      writeFileSync(source, 'int main(void) { return 42; }\n');
      for (const selected of [true, false]) {
        writeFileSync(calls, '');
        const env: NodeJS.ProcessEnv = {
          ...process.env,
          PATH: `${bin}:/usr/bin:/bin`,
          LINKER_CALLS: calls,
          DEVELOPER_DIR: '/nix/store/not-apple',
        };
        delete env.SDKROOT;
        if (selected) env.SDKROOT = sdk.stdout.trim();
        const binary = join(root, selected ? 'selected' : 'fallback');
        const linked = spawnSync('/bin/sh', [linker, source, '-Wl,-v', '-o', binary], { env, encoding: 'utf8' });
        expect(linked.status, `${linked.stdout}${linked.stderr}`).toBe(0);
        expect(`${linked.stdout}${linked.stderr}`).toMatch(/PROJECT:ld-/);
        expect(readFileSync(calls, 'utf8')).toBe(selected ? '' : 'xcrun\n');
        expect(spawnSync(binary).status).toBe(42);
      }
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  },
);
