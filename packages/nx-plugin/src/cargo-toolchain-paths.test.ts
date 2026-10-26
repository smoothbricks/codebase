import { describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { chmod, mkdir, mkdtemp, realpath, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  type CargoToolchainProbe,
  cargoToolchainPaths,
  NIX_STORE_ROOT,
  resolveProgram,
} from './cargo-toolchain-paths.js';

const STORE = '/nix/store';
const STOCK_LINKER = `${STORE}/9mp0kzihmywbpmw32qkbzync25rjnq8d-devenv-rust-linker`;
const PATCHED_LINKER = `${STORE}/13a0ghfiqzm7c1ndpjz08ib4yzcl1a1x-devenv-rust-linker`;
const GCC = `${STORE}/7sh9061yj9yl20ndc00rqw4d0bd80m1j-gcc-wrapper-15.3.0`;

/** A probe over a fixed table of programs: `resolve` answers from it, as a PATH search over real links would. */
function probe(
  environment: Record<string, string | undefined>,
  programs: Record<string, string> = {},
): CargoToolchainProbe {
  return { environment, storeRoot: STORE, resolve: (program) => programs[program] };
}

describe('cargoToolchainPaths', () => {
  const cases: readonly { name: string; probe: CargoToolchainProbe; lines: string[] }[] = [
    {
      name: 'a shell that declares nothing names nothing',
      probe: probe({ PATH: '/nix/store/aaa-profile/bin', HOME: '/home/user' }),
      lines: [],
    },
    {
      name: 'the linker is the store object its wrapper is, however deep in the object the path points',
      probe: probe({ CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER: STOCK_LINKER }, { [STOCK_LINKER]: STOCK_LINKER }),
      lines: [`CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER ${STOCK_LINKER}`],
    },
    {
      name: 'target triples keep both cc-rs spellings and reject compiler settings',
      probe: probe(
        {
          'CC_x86_64-unknown-linux-gnu': 'cc',
          CXX_x86_64_unknown_linux_gnu: 'c++',
          CC_ENABLE_DEBUG_OUTPUT: 'setting',
          TARGET_CC_ENABLE_DEBUG_OUTPUT: 'setting',
        },
        { cc: `${GCC}/bin/cc`, 'c++': `${GCC}/bin/c++`, setting: `${GCC}/bin/setting` },
      ),
      lines: [`CC_x86_64-unknown-linux-gnu ${GCC}`, `CXX_x86_64_unknown_linux_gnu ${GCC}`],
    },
    {
      name: 'a compiler named by a path inside its store object is named by the object',
      probe: probe({ CC_x86_64_unknown_linux_gnu: `${GCC}/bin/cc` }, { [`${GCC}/bin/cc`]: `${GCC}/bin/gcc` }),
      lines: [`CC_x86_64_unknown_linux_gnu ${GCC}`],
    },
    {
      name: 'a bare program name is looked up, and only its first word counts',
      probe: probe({ CC: 'ccache gcc', CXX: 'g++' }, { ccache: `${GCC}/bin/ccache`, 'g++': `${GCC}/bin/g++` }),
      lines: [`CC ${GCC}`, `CXX ${GCC}`],
    },
    {
      name: 'every form cc-rs reads takes part, in name order',
      probe: probe(
        {
          TARGET_RANLIB: 'ranlib',
          HOST_AR: 'ar',
          AR_aarch64_unknown_linux_gnu: 'ar',
          CXX_x86_64_unknown_linux_gnu: 'c++',
        },
        { ranlib: `${GCC}/bin/ranlib`, ar: `${GCC}/bin/ar`, 'c++': `${GCC}/bin/c++` },
      ),
      lines: [
        `AR_aarch64_unknown_linux_gnu ${GCC}`,
        `CXX_x86_64_unknown_linux_gnu ${GCC}`,
        `HOST_AR ${GCC}`,
        `TARGET_RANLIB ${GCC}`,
      ],
    },
    {
      name: 'a program outside the store belongs to the machine and takes no part',
      probe: probe(
        { CC: 'cc', CARGO_TARGET_AARCH64_APPLE_DARWIN_LINKER: '/usr/bin/clang' },
        { cc: '/usr/bin/cc', '/usr/bin/clang': '/usr/bin/clang' },
      ),
      lines: [],
    },
    {
      name: 'a program that resolves to nothing, or to a bare store root, takes no part',
      probe: probe({ CC: 'missing', CXX: STORE, AR: '' }, { CXX: `${STORE}/` }),
      lines: [],
    },
    {
      name: 'flags and unrelated variables name no program',
      probe: probe(
        {
          CFLAGS: `-I${GCC}/include`,
          CXXFLAGS: '-O2',
          CCACHE_DIR: '/tmp/ccache',
          RUSTFLAGS: '-C debuginfo=0',
          CARGO_HOME: '/tmp',
        },
        { [`-I${GCC}/include`]: `${GCC}/include` },
      ),
      lines: [],
    },
  ];
  it.each(cases)('$name', ({ probe: given, lines }) => {
    // Split on the newline the line format promises, so a separator the value spelled some other way fails here.
    expect(cargoToolchainPaths(given).split('\n')).toEqual([...lines, '']);
  });

  // The failure this input exists for: a patch to the linker moves no lock byte and no `rustc -vV` line, yet the
  // wrapper devenv builds names the patched lld, so its store object moves. Nothing else in the shell does.
  it('moves with the linker wrapper and with nothing else the shell holds', () => {
    const shell = (linker: string, extra: Record<string, string> = {}) =>
      cargoToolchainPaths(
        probe(
          {
            CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER: linker,
            CC_x86_64_unknown_linux_gnu: `${GCC}/bin/cc`,
            PATH: '/nix/store/aaa-profile/bin',
            ...extra,
          },
          { [linker]: linker, [`${GCC}/bin/cc`]: `${GCC}/bin/gcc` },
        ),
      );
    const stock = shell(STOCK_LINKER);
    expect(shell(PATCHED_LINKER)).not.toBe(stock);
    // A package added to the shell moves its profile and its PATH, and a variable an agent exports moves its environment.
    expect(shell(STOCK_LINKER, { PATH: '/nix/store/bbb-profile/bin', NX_PARALLEL: '100%', RUSTFLAGS: '-Zfoo' })).toBe(
      stock,
    );
  });
});

describe('resolveProgram', () => {
  async function withDirectory<T>(use: (directory: string) => Promise<T>): Promise<T> {
    const directory = await realpath(await mkdtemp(join(tmpdir(), 'cargo-toolchain-paths-')));
    try {
      return await use(directory);
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  }

  it('follows every link from a PATH entry to the file, and skips what is not an executable file', async () => {
    await withDirectory(async (directory) => {
      const object = join(directory, 'store', 'aaa-gcc-wrapper');
      await mkdir(join(object, 'bin'), { recursive: true });
      await writeFile(join(object, 'bin', 'gcc'), '#!/bin/sh\n');
      await chmod(join(object, 'bin', 'gcc'), 0o755);
      await symlink('gcc', join(object, 'bin', 'cc'));
      await mkdir(join(directory, 'profile', 'bin'), { recursive: true });
      await symlink(join(object, 'bin', 'cc'), join(directory, 'profile', 'bin', 'cc'));
      await mkdir(join(directory, 'data'));
      await writeFile(join(directory, 'data', 'cc'), 'not executable\n');
      const path = [join(directory, 'missing'), join(directory, 'data'), join(directory, 'profile', 'bin')].join(':');
      expect(resolveProgram('cc', path)).toBe(join(object, 'bin', 'gcc'));
      expect(resolveProgram(join(directory, 'profile', 'bin', 'cc'), undefined)).toBe(join(object, 'bin', 'gcc'));
      expect(resolveProgram('cc', join(directory, 'data'))).toBeUndefined();
      expect(resolveProgram('cc', undefined)).toBeUndefined();
      expect(resolveProgram('nothing', path)).toBeUndefined();
    });
  });
});

// The runtime input is this module run over the process's own environment: the bin adds no decision.
describe('smoo-nx-cargo-toolchain', () => {
  it('prints what the module names for the environment it runs in, and exits cleanly', () => {
    const environment = { ...process.env };
    const run = spawnSync(process.execPath, [join(import.meta.dir, 'bin', 'smoo-nx-cargo-toolchain.ts')], {
      env: environment,
      encoding: 'utf8',
    });
    expect(run.status).toBe(0);
    expect(run.stdout).toBe(
      cargoToolchainPaths({
        environment,
        storeRoot: NIX_STORE_ROOT,
        resolve: (program) => resolveProgram(program, environment.PATH),
      }),
    );
  });
});
