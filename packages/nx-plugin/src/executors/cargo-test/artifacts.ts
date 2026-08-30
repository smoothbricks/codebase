import { basename, join } from 'node:path';

export const CARGO_TEST_BINS_DIR = '.cache/cargo-test-bins';
export const CARGO_TEST_MANIFEST = 'manifest.json';

export interface CargoTestBinary {
  name: string;
  fileName: string;
  kind: string[];
  sourcePath: string;
}

export interface CargoTestManifest {
  binaries: CargoTestBinary[];
}

export function parseCargoTestArtifacts(ndjson: string): CargoTestBinary[] {
  const seen = new Set<string>();
  const binaries: CargoTestBinary[] = [];
  for (const line of ndjson.split('\n')) {
    const trimmed = line.trim();
    if (trimmed.length === 0) {
      continue;
    }
    let parsed: unknown;
    try {
      parsed = JSON.parse(trimmed);
    } catch {
      continue;
    }
    if (typeof parsed !== 'object' || parsed === null || !('reason' in parsed)) {
      continue;
    }
    if (parsed.reason !== 'compiler-artifact' || !('executable' in parsed)) {
      continue;
    }
    const executable = parsed.executable;
    if (typeof executable !== 'string' || executable.length === 0) {
      continue;
    }
    const target =
      'target' in parsed && typeof parsed.target === 'object' && parsed.target !== null ? parsed.target : {};
    const profile =
      'profile' in parsed && typeof parsed.profile === 'object' && parsed.profile !== null ? parsed.profile : {};
    const kind =
      'kind' in target && Array.isArray(target.kind)
        ? target.kind.filter((entry): entry is string => typeof entry === 'string')
        : [];
    const isTestProfile = 'test' in profile && profile.test === true;
    if (!isTestProfile && !kind.includes('test')) {
      continue;
    }
    const name =
      'name' in target && typeof target.name === 'string' && target.name.length > 0
        ? target.name
        : basename(executable);
    const fileName = basename(executable);
    if (seen.has(fileName)) {
      continue;
    }
    seen.add(fileName);
    binaries.push({ name, fileName, kind, sourcePath: executable });
  }
  binaries.sort((left, right) => left.fileName.localeCompare(right.fileName));
  return binaries;
}

export function parseCargoTestManifest(text: string): CargoTestManifest | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    return null;
  }
  if (typeof parsed !== 'object' || parsed === null || !('binaries' in parsed) || !Array.isArray(parsed.binaries)) {
    return null;
  }
  const binaries: CargoTestBinary[] = [];
  for (const entry of parsed.binaries) {
    if (typeof entry !== 'object' || entry === null) {
      return null;
    }
    if (!('name' in entry) || !('fileName' in entry) || !('kind' in entry) || !('sourcePath' in entry)) {
      return null;
    }
    if (
      typeof entry.name !== 'string' ||
      typeof entry.fileName !== 'string' ||
      typeof entry.sourcePath !== 'string' ||
      !Array.isArray(entry.kind)
    ) {
      return null;
    }
    binaries.push({
      name: entry.name,
      fileName: entry.fileName,
      sourcePath: entry.sourcePath,
      kind: entry.kind.filter((value: unknown): value is string => typeof value === 'string'),
    });
  }
  return { binaries };
}

export function stagedBinaryPath(stagingDirectory: string, binary: CargoTestBinary): string {
  return join(stagingDirectory, binary.fileName);
}

export function harnessArgsFor(binary: CargoTestBinary): string[] {
  // APFS tests drive host-global hdiutil. Threads inside one binary share a pid
  // and therefore one IntegrationRoot prefix — concurrent attaches collide.
  // Separate binaries have separate pids and may run in parallel with each other
  // and with ordinary unit-test binaries.
  if (binary.name.toLowerCase().includes('apfs') || binary.fileName.toLowerCase().includes('apfs')) {
    return ['--test-threads', '1'];
  }
  return [];
}
