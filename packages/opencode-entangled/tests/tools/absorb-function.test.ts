import { beforeEach, describe, expect, it } from 'bun:test';
import { BlockIndex } from '../../src/block-index.js';
import type { FileIO, RunCommand } from '../../src/tools/absorb.js';
import { createAbsorbFunctionTool } from '../../src/tools/absorb-function.js';

function createMockIO(files: Map<string, string>): FileIO {
  return {
    readFile: async (path: string) => {
      const content = files.get(path);
      if (content === undefined) throw new Error(`File not found: ${path}`);
      return content;
    },
    writeFile: async (path: string, content: string) => {
      files.set(path, content);
    },
  };
}

/** Get a file from the map, throwing if missing (avoids non-null assertions) */
function getFile(files: Map<string, string>, path: string): string {
  const content = files.get(path);
  if (content === undefined) throw new Error(`Expected file ${path} in mock fs`);
  return content;
}

const mockRunCommand: RunCommand = async () => ({ stdout: '', stderr: '', exitCode: 0 });

const SIMPLE_TS = `import { something } from 'somewhere';

export function processOrder(order: Order): Result {
  const total = order.items.reduce((sum, i) => sum + i.price, 0);
  return { total };
}

export const otherStuff = 42;
`;

const ARROW_TS = `export const calculateTotal = (items: Item[]): number => {
  return items.reduce((sum, i) => sum + i.price, 0);
};

export const unused = true;
`;

const ASYNC_TS = `export async function fetchData(url: string): Promise<Data> {
  const res = await fetch(url);
  return res.json();
}
`;

const SIMPLE_CONST_TS = `export const MAX_RETRIES = 5;

export function doWork() {
  return true;
}
`;

const TARGET_MD = `# Agent

\`\`\`typescript {#state}
export const state = defineState({});
\`\`\`
`;

describe('entangled_absorb_function', () => {
  let files: Map<string, string>;
  let index: BlockIndex;
  let io: FileIO;

  beforeEach(() => {
    files = new Map<string, string>();
    files.set('src/order.ts', SIMPLE_TS);
    files.set('src/calc.ts', ARROW_TS);
    files.set('src/fetch.ts', ASYNC_TS);
    files.set('src/config.ts', SIMPLE_CONST_TS);
    files.set('agents/review.md', TARGET_MD);
    index = new BlockIndex();
    index.addFile('agents/review.md', TARGET_MD);
    io = createMockIO(files);
  });

  it('extracts a simple exported function', async () => {
    const tool = createAbsorbFunctionTool(index, io, mockRunCommand);
    const result = await tool.execute({
      sourcePath: 'src/order.ts',
      exportName: 'processOrder',
      targetMd: 'agents/review.md',
    });

    expect(result.metadata.success).toBe(true);
    expect(result.metadata.linesAbsorbed).toBe(4);

    const md = getFile(files, 'agents/review.md');
    // WHY: default block name should be kebab-case of the export name
    expect(md).toContain('```typescript {#process-order}');
    expect(md).toContain('export function processOrder');
    expect(md).toContain('return { total };');

    // WHY: source should have tangle marker replacing the function
    const ts = getFile(files, 'src/order.ts');
    expect(ts).toContain('// ~/~ begin <<agents/review.md#process-order>>[0]');
    expect(ts).toContain('// ~/~ end');
    // WHY: other code should remain
    expect(ts).toContain("import { something } from 'somewhere';");
    expect(ts).toContain('export const otherStuff = 42;');
  });

  it('extracts an exported const arrow function', async () => {
    const tool = createAbsorbFunctionTool(index, io, mockRunCommand);
    const result = await tool.execute({
      sourcePath: 'src/calc.ts',
      exportName: 'calculateTotal',
      targetMd: 'agents/review.md',
    });

    expect(result.metadata.success).toBe(true);

    const md = getFile(files, 'agents/review.md');
    expect(md).toContain('```typescript {#calculate-total}');
    expect(md).toContain('export const calculateTotal');
  });

  it('extracts an async function', async () => {
    const tool = createAbsorbFunctionTool(index, io, mockRunCommand);
    const result = await tool.execute({
      sourcePath: 'src/fetch.ts',
      exportName: 'fetchData',
      targetMd: 'agents/review.md',
    });

    expect(result.metadata.success).toBe(true);

    const md = getFile(files, 'agents/review.md');
    expect(md).toContain('```typescript {#fetch-data}');
    expect(md).toContain('export async function fetchData');
  });

  it('converts camelCase export name to kebab-case for block name', async () => {
    const tool = createAbsorbFunctionTool(index, io, mockRunCommand);
    await tool.execute({
      sourcePath: 'src/order.ts',
      exportName: 'processOrder',
      targetMd: 'agents/review.md',
    });

    const md = getFile(files, 'agents/review.md');
    expect(md).toContain('#process-order');
    expect(md).not.toContain('#processOrder');
  });

  it('uses explicit blockName when provided', async () => {
    const tool = createAbsorbFunctionTool(index, io, mockRunCommand);
    await tool.execute({
      sourcePath: 'src/order.ts',
      exportName: 'processOrder',
      targetMd: 'agents/review.md',
      blockName: 'my-custom-block',
    });

    const md = getFile(files, 'agents/review.md');
    expect(md).toContain('#my-custom-block');
  });

  it('returns error when function is not found in source', async () => {
    const tool = createAbsorbFunctionTool(index, io, mockRunCommand);
    const result = await tool.execute({
      sourcePath: 'src/order.ts',
      exportName: 'nonexistentFunction',
      targetMd: 'agents/review.md',
    });

    expect(result.metadata.success).toBe(false);
    expect(result.output).toContain('Export "nonexistentFunction" not found');
  });

  it('returns error when source file is not found', async () => {
    const tool = createAbsorbFunctionTool(index, io, mockRunCommand);
    const result = await tool.execute({
      sourcePath: 'src/nonexistent.ts',
      exportName: 'processOrder',
      targetMd: 'agents/review.md',
    });

    expect(result.metadata.success).toBe(false);
    expect(result.output).toContain('Source file not found');
  });

  it('produces correct fence markers with closing backticks', async () => {
    const tool = createAbsorbFunctionTool(index, io, mockRunCommand);
    await tool.execute({
      sourcePath: 'src/fetch.ts',
      exportName: 'fetchData',
      targetMd: 'agents/review.md',
    });

    const md = getFile(files, 'agents/review.md');
    const lines = md.split('\n');
    // WHY: find the opening fence for the new block
    const openIdx = lines.findIndex((l) => l.includes('#fetch-data'));
    expect(openIdx).toBeGreaterThan(-1);
    // WHY: there should be a closing ``` fence after the block content
    const closeIdx = lines.findIndex((l, i) => i > openIdx && l.trim() === '```');
    expect(closeIdx).toBeGreaterThan(openIdx);
  });

  it('adds prose before the block when provided', async () => {
    const tool = createAbsorbFunctionTool(index, io, mockRunCommand);
    await tool.execute({
      sourcePath: 'src/order.ts',
      exportName: 'processOrder',
      targetMd: 'agents/review.md',
      prose: '## Order Processing\n\nHandles order total calculation:',
    });

    const md = getFile(files, 'agents/review.md');
    expect(md).toContain('## Order Processing');
    // WHY: prose should appear before the code block
    const proseIdx = md.indexOf('## Order Processing');
    const fenceIdx = md.indexOf('```typescript {#process-order}');
    expect(fenceIdx).toBeGreaterThan(proseIdx);
  });

  it('rebuilds the index after absorbing', async () => {
    const tool = createAbsorbFunctionTool(index, io, mockRunCommand);
    await tool.execute({
      sourcePath: 'src/order.ts',
      exportName: 'processOrder',
      targetMd: 'agents/review.md',
    });

    const block = index.get('process-order');
    expect(block).toBeDefined();
    expect(block?.file).toBe('agents/review.md');
  });
});
