import { type ParsedBlock, parseFences } from './fence-parser.js';

export interface BlockEntry {
  /** Block identifier (#name) */
  id?: string;
  /** Source markdown file path (relative to project root) */
  file: string;
  /** 1-indexed line number of fence open */
  line: number;
  /** Tangle target (file= attribute) */
  target?: string;
  /** Block body content */
  content: string;
  /** <<ref>> references in this block */
  refs: string[];
  /** Language from fence info */
  language?: string;
}

function parsedToEntry(parsed: ParsedBlock, filePath: string): BlockEntry {
  return {
    ...(parsed.id != null && { id: parsed.id }),
    file: filePath,
    line: parsed.line,
    ...(parsed.file != null && { target: parsed.file }),
    content: parsed.content,
    refs: parsed.refs,
    ...(parsed.language != null && { language: parsed.language }),
  };
}

export class BlockIndex {
  // WHY: separate maps for O(1) lookup by id and by file, avoiding full scans
  private byId = new Map<string, BlockEntry>();
  private byFile = new Map<string, BlockEntry[]>();
  private byTarget = new Map<string, BlockEntry[]>();

  /** Add/replace all blocks from a markdown file */
  addFile(filePath: string, content: string): void {
    // WHY: remove previous entries for this file first so re-indexing replaces cleanly
    this.removeFile(filePath);

    const parsed = parseFences(content);
    const entries: BlockEntry[] = [];

    for (const block of parsed) {
      const entry = parsedToEntry(block, filePath);
      entries.push(entry);

      if (entry.id) {
        this.byId.set(entry.id, entry);
      }

      if (entry.target) {
        const list = this.byTarget.get(entry.target);
        if (list) {
          list.push(entry);
        } else {
          this.byTarget.set(entry.target, [entry]);
        }
      }
    }

    this.byFile.set(filePath, entries);
  }

  /** Remove all blocks from a file */
  removeFile(filePath: string): void {
    const entries = this.byFile.get(filePath);
    if (!entries) return;

    for (const entry of entries) {
      if (entry.id) {
        this.byId.delete(entry.id);
      }

      if (entry.target) {
        const list = this.byTarget.get(entry.target);
        if (list) {
          const filtered = list.filter((e) => e.file !== filePath);
          if (filtered.length === 0) {
            this.byTarget.delete(entry.target);
          } else {
            this.byTarget.set(entry.target, filtered);
          }
        }
      }
    }

    this.byFile.delete(filePath);
  }

  /** Look up a block by its #id */
  get(blockId: string): BlockEntry | undefined {
    return this.byId.get(blockId);
  }

  /** Find all blocks that tangle to a specific file= target */
  getByTarget(targetPath: string): BlockEntry[] {
    return this.byTarget.get(targetPath) ?? [];
  }

  /** List all unique file= targets */
  listTargets(): string[] {
    return [...this.byTarget.keys()];
  }

  /** Find all blocks that reference <<blockId>> */
  findReferences(blockId: string): BlockEntry[] {
    const results: BlockEntry[] = [];
    for (const entries of this.byFile.values()) {
      for (const entry of entries) {
        if (entry.refs.includes(blockId)) {
          results.push(entry);
        }
      }
    }
    return results;
  }

  /** List all blocks in a specific markdown file */
  listBlocks(filePath: string): BlockEntry[] {
    return this.byFile.get(filePath) ?? [];
  }

  /** Recursively expand <<ref>> in a block, returning full content */
  expand(blockId: string): string {
    const visited = new Set<string>();
    return this.expandInner(blockId, visited);
  }

  private expandInner(blockId: string, visited: Set<string>): string {
    if (visited.has(blockId)) {
      throw new Error(`Circular reference detected: ${blockId}`);
    }
    const entry = this.byId.get(blockId);
    if (!entry) {
      throw new Error(`Block not found: ${blockId}`);
    }

    visited.add(blockId);

    // WHY: replace each <<ref>> line with the expanded content of the referenced block,
    // preserving the indentation of the reference line
    const lines = entry.content.split('\n');
    const expanded = lines.map((line) => {
      const match = line.match(/^(\s*)<<([^>]+)>>(\s*)$/);
      if (!match) return line;
      const indent = match[1] ?? '';
      // biome-ignore lint/style/noNonNullAssertion: capture group 2 always exists when the regex matches
      const refId = match[2]!;
      const inner = this.expandInner(refId, new Set(visited));
      // WHY: indent each line of the expanded block to match the reference's indentation
      if (!indent) return inner;
      return inner
        .split('\n')
        .map((l) => indent + l)
        .join('\n');
    });

    return expanded.join('\n');
  }

  /** Find all blocks that depend on blockId (transitive reverse deps) */
  dependents(blockId: string): BlockEntry[] {
    const result = new Set<BlockEntry>();
    const visited = new Set<string>();
    this.collectDependents(blockId, result, visited);
    return [...result];
  }

  private collectDependents(blockId: string, result: Set<BlockEntry>, visited: Set<string>): void {
    if (visited.has(blockId)) return;
    visited.add(blockId);

    const refs = this.findReferences(blockId);
    for (const entry of refs) {
      result.add(entry);
      // WHY: recurse on the entry's id to find transitive dependents
      if (entry.id) {
        this.collectDependents(entry.id, result, visited);
      }
    }
  }

  /** Scan a directory for .md files and build the index.
   *  Caller provides a list function and a read function so this stays runtime-agnostic. */
  async scanDirectory(
    listMdFiles: () => Promise<string[]>,
    readFile: (path: string) => Promise<string>,
  ): Promise<void> {
    const files = await listMdFiles();
    for (const filePath of files) {
      const content = await readFile(filePath);
      this.addFile(filePath, content);
    }
  }
}
