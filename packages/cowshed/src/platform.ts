// Every host this package ships a native addon and a CLI binary for. `addonDirectory` and
// `cliDirectory` are derived from this table.
//
// `package.json` restates the addon triples as `napi.targets`, and the CLI triples and directories
// in the per-arch `cli-*` targets: napi `--target`/`--output-dir` on macOS, the `cargo-zigbuild`
// executor's `target`/`output` on Linux. `bin/cowshed` restates the CLI directories because the
// launcher is a shell script. `platform.test.ts` checks the `package.json` restatements, and
// `launcher.test.ts` checks `bin/cowshed`.
//
// The Linux CLI is a static musl executable (no loader, no libraries), so its directory carries no
// libc suffix, while the addon is glibc and keeps `-gnu`.
export const NATIVE_TARGETS = [
  {
    platform: 'darwin',
    arch: 'arm64',
    addon: { triple: 'aarch64-apple-darwin', directory: 'darwin-arm64' },
    cli: { triple: 'aarch64-apple-darwin', directory: 'darwin-arm64' },
  },
  {
    platform: 'darwin',
    arch: 'x64',
    addon: { triple: 'x86_64-apple-darwin', directory: 'darwin-x64' },
    cli: { triple: 'x86_64-apple-darwin', directory: 'darwin-x64' },
  },
  {
    platform: 'linux',
    arch: 'arm64',
    addon: { triple: 'aarch64-unknown-linux-gnu', directory: 'linux-arm64-gnu' },
    cli: { triple: 'aarch64-unknown-linux-musl', directory: 'linux-arm64' },
  },
  {
    platform: 'linux',
    arch: 'x64',
    addon: { triple: 'x86_64-unknown-linux-gnu', directory: 'linux-x64-gnu' },
    cli: { triple: 'x86_64-unknown-linux-musl', directory: 'linux-x64' },
  },
] as const;

export function addonDirectory(platform: NodeJS.Platform, arch: string): string | null {
  const row = NATIVE_TARGETS.find((target) => target.platform === platform && target.arch === arch);
  return row?.addon.directory ?? null;
}

export function cliDirectory(platform: NodeJS.Platform, arch: string): string | null {
  const row = NATIVE_TARGETS.find((target) => target.platform === platform && target.arch === arch);
  return row?.cli.directory ?? null;
}
