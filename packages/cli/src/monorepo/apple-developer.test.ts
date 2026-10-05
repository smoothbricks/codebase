import { describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { chmodSync, existsSync, mkdirSync, mkdtempSync, realpathSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { managedAssetsRoot } from '@smoothbricks/nx-plugin/managed-assets';
import { printCommandOutput } from '../lib/run.js';

const script = join(managedAssetsRoot, 'raw/tooling/direnv/apple-developer.sh');

interface Scratch {
  /** Stands in for /nix/store: the script drops only what lives below it. */
  readonly store: string;
  /** A store package's bin holding xcbuild's xcrun, which cannot find Xcode's SDKs. */
  readonly xcbuild: string;
  /** A store package's bin with no xcrun. */
  readonly tool: string;
  /** A directory outside the store that also holds an xcrun: the operator's own. */
  readonly operator: string;
}

/** The answer xcbuild's xcrun gives once the nix DEVELOPER_DIR is gone. */
const XCBUILD_XCRUN = '#!/bin/sh\necho "error: unable to find sdk: \'macosx\'" >&2\nexit 255\n';

function withScratch(run: (scratch: Scratch) => void): void {
  const dir = realpathSync(mkdtempSync(join(tmpdir(), 'smoo-apple-developer-')));
  try {
    const store = join(dir, 'store');
    const scratch: Scratch = {
      store,
      xcbuild: join(store, '00000000000000000000000000000000-xcbuild-0.1.1-xcrun', 'bin'),
      tool: join(store, '11111111111111111111111111111111-jq-1.8.2-bin', 'bin'),
      operator: join(dir, 'profile', 'bin'),
    };
    for (const path of [scratch.xcbuild, scratch.tool, scratch.operator]) mkdirSync(path, { recursive: true });
    for (const xcrun of [join(scratch.xcbuild, 'xcrun'), join(scratch.operator, 'xcrun')]) {
      writeFileSync(xcrun, XCBUILD_XCRUN);
      chmodSync(xcrun, 0o755);
    }
    run(scratch);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

interface Entered {
  readonly status: number | null;
  readonly stdout: string;
  readonly stderr: string;
}

/**
 * Source the script the way devenv.smoo.nix's prologue does — naming the
 * store — then run `command` in the same shell.
 */
function enterShell(scratch: Scratch, env: Record<string, string>, command: string): Entered {
  const result = spawnSync('bash', ['-c', `. "$1" "$2" && ${command}`, 'shell', script, scratch.store], {
    encoding: 'utf8',
    env,
  });
  return { status: result.status, stdout: result.stdout, stderr: result.stderr };
}

function entered(result: Entered): Entered {
  if (result.status !== 0) printCommandOutput(result.stdout, result.stderr);
  expect(result.status).toBe(0);
  return result;
}

describe('apple-developer.sh', () => {
  it('drops a store SDKROOT and DEVELOPER_DIR and the nix compilers, announcing each SDK drop', () => {
    withScratch((scratch) => {
      const sdk = join(scratch.store, '22222222222222222222222222222222-apple-sdk-14.4');
      const result = entered(
        enterShell(
          scratch,
          {
            PATH: '/usr/bin:/bin',
            CC: 'clang',
            CXX: 'clang++',
            SDKROOT: join(sdk, 'Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk'),
            DEVELOPER_DIR: sdk,
            NIX_APPLE_SDK_VERSION: '14.4',
          },
          `printf '%s|%s|%s|%s|%s\\n' "\${CC-unset}" "\${CXX-unset}" "\${SDKROOT-unset}" "\${DEVELOPER_DIR-unset}" "\${NIX_APPLE_SDK_VERSION-unset}"`,
        ),
      );
      expect(result.stdout).toBe('unset|unset|unset|unset|unset\n');
      expect(result.stderr).toContain('dropping nix-store SDKROOT');
      expect(result.stderr).toContain('dropping nix-store DEVELOPER_DIR');
    });
  });

  it('keeps an SDKROOT and DEVELOPER_DIR the operator exported outside the store, silently', () => {
    withScratch((scratch) => {
      const result = entered(
        enterShell(
          scratch,
          {
            PATH: '/usr/bin:/bin',
            SDKROOT: '/opt/apple-sdk/MacOSX.sdk',
            DEVELOPER_DIR: '/Applications/Xcode.app/Contents/Developer',
          },
          `printf '%s|%s\\n' "$SDKROOT" "$DEVELOPER_DIR"`,
        ),
      );
      expect(result.stdout).toBe('/opt/apple-sdk/MacOSX.sdk|/Applications/Xcode.app/Contents/Developer\n');
      expect(result.stderr).toBe('');
    });
  });

  it('removes exactly the store PATH entries carrying an xcrun and keeps every other entry in order', () => {
    withScratch((scratch) => {
      // A leading and an inner empty entry (the current directory) must survive in place.
      const path = ['', scratch.xcbuild, scratch.tool, '', scratch.operator, scratch.xcbuild, '/usr/bin', '/bin'];
      const result = entered(enterShell(scratch, { PATH: path.join(':') }, `printf '%s\\n' "$PATH"`));
      expect(result.stdout).toBe(`${['', scratch.tool, '', scratch.operator, '/usr/bin', '/bin'].join(':')}\n`);
      expect(result.stderr).toBe(
        `devenv: dropping nix-store ${scratch.xcbuild} from PATH; xcrun must answer from Xcode\n`.repeat(2),
      );
    });
  });

  // The regression: an outer nix shell put xcbuild's xcrun first on PATH and its
  // DEVELOPER_DIR into the environment, and after shell entry `xcrun` answered
  // "unable to find sdk: 'macosx'" — a linker warning from rustc and a failed
  // build script that asks xcrun for the SDK.
  it.skipIf(process.platform !== 'darwin')(
    "answers `xcrun --sdk macosx --show-sdk-path` with the selected Xcode or CLT SDK after an outer nix shell's xcrun",
    () => {
      withScratch((scratch) => {
        const developer = spawnSync('/usr/bin/xcode-select', ['-p'], { encoding: 'utf8' });
        expect(developer.status).toBe(0);
        const developerDir = developer.stdout.trim();
        const sdk = join(scratch.store, '22222222222222222222222222222222-apple-sdk-14.4');
        const result = entered(
          enterShell(
            scratch,
            {
              PATH: [scratch.xcbuild, scratch.tool, '/usr/bin', '/bin'].join(':'),
              HOME: process.env.HOME ?? '',
              TMPDIR: process.env.TMPDIR ?? tmpdir(),
              SDKROOT: join(sdk, 'Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk'),
              DEVELOPER_DIR: sdk,
            },
            'command -v xcrun && xcrun --sdk macosx --show-sdk-path',
          ),
        );
        const [xcrun = '', sdkPath = ''] = result.stdout.trim().split('\n');
        expect(xcrun).toBe('/usr/bin/xcrun');
        expect(sdkPath).toStartWith(`${developerDir}/`);
        expect(sdkPath).toMatch(/\/MacOSX[0-9.]*\.sdk$/);
        expect(existsSync(join(sdkPath, 'SDKSettings.json'))).toBe(true);
        expect(result.stderr).not.toContain('unable to find sdk');
      });
    },
  );
});
