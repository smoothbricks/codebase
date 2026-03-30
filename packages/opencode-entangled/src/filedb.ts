/**
 * Reader for Entangled's `.entangled/filedb.json` persistence file.
 *
 * The Python entangled-cli stores:
 *   { version: string, files: Record<path, Stat>, targets: string[] }
 *
 * `targets` is the set of managed tangle output paths (files generated from .md sources).
 * `files` contains stat info (modified timestamp + sha256 hexdigest) for both sources and targets.
 *
 * WHY no getSource(): filedb.json does not record which .md file produced a given target.
 * That mapping lives in the block index, not the file database.
 */

// --- Schema types matching the Python msgspec Struct ---

export interface FileStat {
  modified: string; // ISO datetime from Python's datetime.fromtimestamp
  hexdigest: string; // SHA-256 hex digest
}

/** Raw shape from .entangled/filedb.json — fields may be missing in older versions */
interface FileDBRaw {
  version?: string;
  files?: Record<string, FileStat>;
  targets?: string[];
}

export interface FileDBData {
  version: string;
  files: Record<string, FileStat>;
  targets: string[];
}

// --- Public API ---

export class FileDB {
  private targets = new Set<string>();
  private files = new Map<string, FileStat>();

  constructor(private projectRoot: string) {}

  /** Read and parse the `.entangled/filedb.json` file.
   *  Caller provides a read function so this stays runtime-agnostic (lib tsconfig has `"types": []`). */
  async load(readFile: (path: string) => Promise<string>): Promise<FileDBData> {
    const dbPath = posixJoin(this.projectRoot, '.entangled', 'filedb.json');

    let raw: string;
    try {
      raw = await readFile(dbPath);
    } catch {
      // WHY: missing file is a normal state (entangled hasn't run yet), not an error
      this.clear();
      return { version: '', files: {}, targets: [] };
    }

    let parsed: unknown;
    try {
      parsed = JSON.parse(raw);
    } catch {
      // WHY: malformed JSON is recoverable — treat as empty so the plugin doesn't crash
      this.clear();
      return { version: '', files: {}, targets: [] };
    }

    const data = parseFileDB(parsed);
    this.targets = new Set(data.targets);
    this.files = new Map(Object.entries(data.files));
    return data;
  }

  /** Check if a file path is a managed tangle target. */
  isManaged(filePath: string): boolean {
    const normalized = this.normalize(filePath);
    return this.targets.has(normalized);
  }

  /** List all managed tangle-target file paths (relative to project root). */
  listTargets(): string[] {
    return [...this.targets];
  }

  /** Get the stat info for a tracked file (source or target). */
  getStat(filePath: string): FileStat | undefined {
    const normalized = this.normalize(filePath);
    return this.files.get(normalized);
  }

  /** List all tracked file paths (both sources and targets). */
  listFiles(): string[] {
    return [...this.files.keys()];
  }

  private clear(): void {
    this.targets = new Set();
    this.files = new Map();
  }

  // WHY: entangled stores posix-style relative paths; normalize input for consistent lookups.
  // We avoid node:path imports because the lib tsconfig has "types": [].
  private normalize(filePath: string): string {
    const posixPath = filePath.split('\\').join('/');
    // WHY: if the path starts with the project root prefix, strip it to get a relative path
    const root = `${this.projectRoot.split('\\').join('/').replace(/\/$/, '')}/`;
    if (posixPath.startsWith(root)) {
      return posixPath.slice(root.length);
    }
    return posixPath;
  }
}

// --- Helpers ---

function posixJoin(...segments: string[]): string {
  return segments.map((s) => s.replace(/\/$/, '')).join('/');
}

// --- Validation via Typia ---

import typia from 'typia';

const validateFileDB = typia.createValidate<FileDBRaw>();

function parseFileDB(raw: unknown): FileDBData {
  const result = validateFileDB(raw);
  if (!result.success) {
    // WHY: invalid data is recoverable — treat as empty so the plugin doesn't crash
    return { version: '', files: {}, targets: [] };
  }
  // WHY: normalize optional fields to required with defaults
  return {
    version: result.data.version ?? '',
    files: result.data.files ?? {},
    targets: result.data.targets ?? [],
  };
}
