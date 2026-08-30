// Single owner of the platform → dist/native directory mapping (mirrored by the napi
// --output-dir literals in package.json). Kept free of typia/native imports because the
// CLI trampoline loads it before deciding whether Node-API startup can be skipped.
export function platformDirectory(platform: NodeJS.Platform, arch: string): string | null {
  if (platform === 'darwin' && (arch === 'arm64' || arch === 'x64')) {
    return `darwin-${arch}`;
  }
  if (platform === 'linux' && (arch === 'arm64' || arch === 'x64')) {
    return `linux-${arch}-gnu`;
  }
  return null;
}
