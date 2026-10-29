import { expect, test } from 'bun:test';
import { spawnSync } from 'node:child_process';
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  realpathSync,
  rmSync,
  statSync,
  symlinkSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, relative } from 'node:path';
import { managedAssetsRoot } from '../managed-assets.js';

test("copied and symlinked devenv wrappers refuse the invocation checkout's missing configuration", () => {
  const scratch = realpathSync(mkdtempSync(join(tmpdir(), 'smoo-devenv-root-')));
  try {
    const tools = join(scratch, 'bin');
    mkdirSync(tools);
    for (const name of ['bash', 'dirname', 'realpath']) {
      const executable = Bun.which(name);
      if (executable === null) throw new Error(`${name} must be on PATH`);
      symlinkSync(executable, join(tools, name));
    }
    const script = readFileSync(join(managedAssetsRoot, 'raw/tooling/devenv'));
    const layouts = [
      { name: 'copy', source: 'tooling/devenv' },
      { name: 'source-link', source: 'packages/nx-plugin/managed/raw/tooling/devenv' },
      { name: 'consumer-link', source: 'node_modules/@smoothbricks/nx-plugin/managed/raw/tooling/devenv' },
    ];
    for (const layout of layouts) {
      const root = join(scratch, layout.name);
      const config = join(root, 'tooling/direnv');
      const wrapper = join(root, 'tooling/devenv');
      const source = join(root, layout.source);
      mkdirSync(config, { recursive: true });
      mkdirSync(dirname(source), { recursive: true });
      writeFileSync(source, script, { mode: 0o755 });
      if (source !== wrapper) {
        symlinkSync(relative(dirname(wrapper), source), wrapper);
        // A source-root lookup would find this decoy instead of refusing our config.
        const decoy = join(dirname(source), 'direnv');
        mkdirSync(decoy);
        writeFileSync(join(decoy, 'devenv.nix'), '{}\n');
      }
      expect(statSync(wrapper).isFile()).toBe(true);
      const nested = join(root, 'packages/client');
      mkdirSync(nested, { recursive: true });
      for (const cwd of [root, nested]) {
        const result = spawnSync(relative(cwd, wrapper), ['version'], {
          cwd,
          encoding: 'utf8',
          env: { PATH: tools, HOME: scratch },
        });
        expect(result.error).toBeUndefined();
        expect(result.status).toBe(1);
        expect(result.stdout).toBe('');
        expect(result.stderr).toBe(
          `Error: ${config} holds no devenv.yaml or devenv.nix.\n` +
            '       Refusing to run: devenv would report success having done nothing.\n',
        );
      }
      expect(existsSync(join(root, '.devenv'))).toBe(false);
    }
  } finally {
    rmSync(scratch, { recursive: true, force: true });
  }
});
