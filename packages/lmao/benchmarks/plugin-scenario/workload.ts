import { convertSpanTreeToArrowTable } from '@smoothbricks/lmao';
import { createTraceRoot } from '@smoothbricks/lmao/node';
import { bench, do_not_optimize, run } from 'mitata';
import {
  createScenarioTracer,
  executeScenario,
  generateCanonicalSemanticSnapshot,
  resetScenarioBufferStats,
} from './scenario.js';

const LIFECYCLE_WARMUP_ITERATIONS = 2_048;
const ARROW_WARMUP_ITERATIONS = 256;

async function semanticPreflight(outputPath?: string): Promise<string> {
  const canonicalJson = await generateCanonicalSemanticSnapshot(createTraceRoot);
  if (outputPath !== undefined) {
    await Bun.write(outputPath, canonicalJson);
  }
  return canonicalJson;
}

async function benchmarkScenario(): Promise<void> {
  await semanticPreflight();

  const lifecycleTracer = createScenarioTracer(createTraceRoot);
  const arrowTracer = createScenarioTracer(createTraceRoot);
  resetScenarioBufferStats();

  for (let index = 0; index < LIFECYCLE_WARMUP_ITERATIONS; index++) {
    lifecycleTracer.clear();
    do_not_optimize(await executeScenario(lifecycleTracer));
  }
  for (let index = 0; index < ARROW_WARMUP_ITERATIONS; index++) {
    arrowTracer.clear();
    const result = await executeScenario(arrowTracer);
    const rootBuffer = arrowTracer.rootBuffers[0];
    if (!rootBuffer) {
      throw new Error('Arrow warmup did not capture a root buffer');
    }
    const table = convertSpanTreeToArrowTable(rootBuffer);
    do_not_optimize(result);
    do_not_optimize(table.numRows);
  }
  lifecycleTracer.clear();
  arrowTracer.clear();

  bench('request lifecycle', async () => {
    lifecycleTracer.clear();
    const result = await executeScenario(lifecycleTracer);
    do_not_optimize(result);
  }).gc('inner');

  bench('request lifecycle + Arrow', async () => {
    arrowTracer.clear();
    const result = await executeScenario(arrowTracer);
    const rootBuffer = arrowTracer.rootBuffers[0];
    if (!rootBuffer) {
      throw new Error('Arrow benchmark did not capture a root buffer');
    }
    const table = convertSpanTreeToArrowTable(rootBuffer);
    do_not_optimize(result);
    do_not_optimize(table.numRows);
  }).gc('inner');

  await run({ colors: false, format: { json: { samples: true } }, throw: true });
}

function optionValue(args: string[], name: string): string | undefined {
  const index = args.indexOf(name);
  if (index === -1) {
    return undefined;
  }
  const value = args[index + 1];
  if (value === undefined || value.startsWith('--')) {
    throw new Error(`${name} requires an explicit path`);
  }
  return value;
}

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  const semanticOutput = optionValue(args, '--semantic-output');
  const allowedArgumentCount = semanticOutput === undefined ? 0 : 2;
  if (args.length !== allowedArgumentCount) {
    throw new Error(`Unknown or duplicate workload arguments: ${args.join(' ')}`);
  }

  if (semanticOutput !== undefined) {
    await semanticPreflight(semanticOutput);
    return;
  }

  await benchmarkScenario();
}

await main();
