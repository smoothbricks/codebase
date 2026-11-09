import { createHash } from 'node:crypto';
import type { FileChange, Tree } from 'nx/src/devkit-exports.js';
import type { ManagedFile } from './tree.js';

/** What a render can ask a Tree. */
type Question = 'read' | 'exists' | 'isFile' | 'children';

/** One question a render asked, and the answer it got: a digest of the bytes for a read. */
interface Answer {
  readonly question: Question;
  readonly path: string;
  readonly value: string;
}

/** A render's result, and every answer it was computed from. */
interface Render {
  readonly key: string;
  readonly answers: readonly Answer[];
  readonly files: readonly ManagedFile[];
}

/** The last render of each workspace root. Replaced by the next render of that root; never expired by time. */
const renders = new Map<string, Render>();

function readValue(bytes: Buffer | null): string {
  return bytes === null ? 'absent' : createHash('sha256').update(bytes).digest('base64');
}

function answerOf(tree: Tree, question: Question, path: string): string {
  switch (question) {
    case 'read':
      return readValue(tree.read(path));
    case 'exists':
      return String(tree.exists(path));
    case 'isFile':
      return String(tree.isFile(path));
    case 'children':
      return JSON.stringify(tree.children(path));
  }
}

/**
 * A Tree that answers from another and keeps every answer it gave. A render that reads the workspace only through
 * it is a function of those answers and of what its caller keys it on. It refuses every write: a render reads the
 * Tree, and staging alone writes it.
 */
class AnsweringTree implements Tree {
  readonly answers = new Map<string, Answer>();

  constructor(private readonly tree: Tree) {}

  get root(): string {
    return this.tree.root;
  }

  read(filePath: string): Buffer | null;
  read(filePath: string, encoding: BufferEncoding): string | null;
  read(filePath: string, encoding?: BufferEncoding): Buffer | string | null {
    const bytes = this.tree.read(filePath);
    this.keep('read', filePath, readValue(bytes));
    return bytes === null || encoding === undefined ? bytes : bytes.toString(encoding);
  }

  exists(filePath: string): boolean {
    return this.keep('exists', filePath, String(this.tree.exists(filePath))) === 'true';
  }

  isFile(filePath: string): boolean {
    return this.keep('isFile', filePath, String(this.tree.isFile(filePath))) === 'true';
  }

  children(dirPath: string): string[] {
    const children = this.tree.children(dirPath);
    this.keep('children', dirPath, JSON.stringify(children));
    return children;
  }

  listChanges(): FileChange[] {
    return this.tree.listChanges();
  }

  write(filePath: string): never {
    throw readOnly(filePath);
  }

  delete(filePath: string): never {
    throw readOnly(filePath);
  }

  rename(from: string): never {
    throw readOnly(from);
  }

  changePermissions(filePath: string): never {
    throw readOnly(filePath);
  }

  private keep(question: Question, path: string, value: string): string {
    this.answers.set(`${question}\0${path}`, { question, path, value });
    return value;
  }
}

function readOnly(path: string): Error {
  return new Error(`${path}: a managed-files render reads the Tree; only staging writes it`);
}

/**
 * The files `render` gives for `tree`, rendered again only when one of its inputs changed: `key`, which names what the
 * render reads beside the Tree (the context, the source listing), or an answer the Tree gave it. Checking the answers
 * reads each file the last render read, once; the render itself (policy, JSON edits, formatter runs) is skipped.
 *
 * A Tree with pending edits is rendered and nothing is kept: no later Tree shows those edits as they are here.
 * The formatters' own configuration is not an answer: Prettier and Biome read it from disk, and Prettier keeps it for
 * the life of the process, so a daemon already formats with what it first read.
 */
export async function renderedOnChange(
  tree: Tree,
  key: string,
  render: (tree: Tree) => Promise<ManagedFile[]>,
): Promise<readonly ManagedFile[]> {
  if (tree.listChanges().length > 0) return render(tree);
  const last = renders.get(tree.root);
  if (
    last !== undefined &&
    last.key === key &&
    last.answers.every(({ question, path, value }) => answerOf(tree, question, path) === value)
  ) {
    return last.files;
  }
  const answering = new AnsweringTree(tree);
  const files = await render(answering);
  renders.set(tree.root, { key, answers: [...answering.answers.values()], files });
  return files;
}
