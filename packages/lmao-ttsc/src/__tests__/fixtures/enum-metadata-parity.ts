import {
  convertSpanTreeToArrowTable,
  createTraceRoot,
  defineLogSchema,
  defineOpContext,
  iterateSpanTree,
  JsBufferStrategy,
  S,
  TestTracer,
} from '@smoothbricks/lmao/node';

const OPERATIONS: readonly ['READ', 'WRITE'] = ['READ', 'WRITE'];

const schema = defineLogSchema({
  operation: S.enum(OPERATIONS),
});
const context = defineOpContext({ logSchema: schema });

function dynamicOperation(index: number): 'READ' | 'WRITE' {
  const value = index === 5 ? 'INVALID' : index % 2 === 0 ? 'READ' : 'WRITE';
  return JSON.parse(JSON.stringify(value));
}


const child = context.defineOp('enum-metadata-child', (ctx) => {
  ctx.tag.operation('WRITE');
  for (let index = 0; index < 12; index++) {
    ctx.log.info('Processing complete').operation(dynamicOperation(index));
  }
  return ctx.ok('done').with({ operation: 'WRITE' });
});

const parent = context.defineOp('enum-metadata-parent', async (ctx) => {
  await ctx.span('validate-items', child);
  await ctx.span('validate-items', child);
  return ctx.ok('done');
});

function storageBytes(value: unknown): number[] {
  if (value instanceof Uint8Array || value instanceof Uint16Array || value instanceof Uint32Array) {
    return Array.from(new Uint8Array(value.buffer, value.byteOffset, value.byteLength));
  }
  throw new TypeError('Expected an enum index typed array');
}

async function main(): Promise<void> {
  const tracer = new TestTracer(context, {
    bufferStrategy: new JsBufferStrategy(),
    createTraceRoot,
  });
  await tracer.trace('enum-metadata-root', parent);
  const root = tracer.rootBuffers[0];
  if (!root) throw new Error('Enum metadata parity fixture produced no trace');

  const storage = [];
  for (const segment of iterateSpanTree(root)) {
    const values = segment.getColumnIfAllocated('operation');
    const nulls = segment.getNullsIfAllocated('operation');
    if (values === undefined || nulls === undefined) continue;
    storage.push({
      writeIndex: segment._writeIndex,
      valueBytes: storageBytes(values),
      nullBytes: Array.from(nulls),
    });
  }

  const table = convertSpanTreeToArrowTable(root);
  const operation = table.getChild('operation');
  if (!operation) throw new Error('Enum metadata parity fixture produced no operation column');
  const decoded = Array.from({ length: operation.length }, (_, index) => operation.get(index));

  process.stdout.write(`${JSON.stringify({ storage, decoded })}\n`);
}

await main();
