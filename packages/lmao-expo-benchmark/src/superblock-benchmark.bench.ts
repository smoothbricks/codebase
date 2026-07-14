import { createWasmAllocator } from '@smoothbricks/lmao/wasm';
import { runSuperblockBenchmark } from './superblock-benchmark.js';

const allocator = await createWasmAllocator({ capacity: 64 });
const result = runSuperblockBenchmark(allocator, () => performance.now());
const summary = {
  root: {
    legacyMedianNsPerOp: result.root.legacy.medianNsPerOp,
    packedMedianNsPerOp: result.root.packed.medianNsPerOp,
    speedup: result.root.speedup,
    reductionPercent: result.root.reductionPercent,
  },
  overflow: {
    legacyMedianNsPerOp: result.overflow.legacy.medianNsPerOp,
    packedMedianNsPerOp: result.overflow.packed.medianNsPerOp,
    speedup: result.overflow.speedup,
    reductionPercent: result.overflow.reductionPercent,
  },
};
console.log(`LMAO_SUPERBLOCK_SUMMARY ${JSON.stringify(summary)}`);
console.log(`LMAO_SUPERBLOCK_RESULT ${JSON.stringify(result)}`);
