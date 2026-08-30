import { join } from 'node:path';

// Every host this package ships a native addon and CLI binary for. `platformDirectory` is
// derived from this table. Kept free of typia/native imports because the CLI trampoline loads
// it before deciding whether Node-API startup can be skipped.
//
// `package.json` restates the triples as `napi.targets` and the directories as
// `--output-dir dist/bin/<directory>` because nx command strings cannot import this module.
// `platform.test.ts` is the check that those strings still name this table.
export const NATIVE_TARGETS = [
  { triple: 'aarch64-apple-darwin', platform: 'darwin', arch: 'arm64', directory: 'darwin-arm64' },
  { triple: 'x86_64-apple-darwin', platform: 'darwin', arch: 'x64', directory: 'darwin-x64' },
  { triple: 'aarch64-unknown-linux-gnu', platform: 'linux', arch: 'arm64', directory: 'linux-arm64-gnu' },
  { triple: 'x86_64-unknown-linux-gnu', platform: 'linux', arch: 'x64', directory: 'linux-x64-gnu' },
] as const;

export function platformDirectory(platform: NodeJS.Platform, arch: string): string | null {
  return NATIVE_TARGETS.find((target) => target.platform === platform && target.arch === arch)?.directory ?? null;
}

// Path under $HOME that launchd's `HostStableExecutable` derives for the installed cowshed
// binary. The trampoline must not load napi just to ask for this path, so the segments live
// here; cowshed-napi's parity test pins them against the rust constructor.
export const HOST_STABLE_BINARY_SEGMENTS = ['Library', 'Application Support', 'dev.cowshed', 'bin'] as const;
export const HOST_STABLE_BINARY_NAME = 'cowshed';

export function hostStableCowshedBinary(home: string): string {
  return join(home, ...HOST_STABLE_BINARY_SEGMENTS, HOST_STABLE_BINARY_NAME);
}
