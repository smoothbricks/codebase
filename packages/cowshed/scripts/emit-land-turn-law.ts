import { emitLaw } from './emit-law.ts';

await emitLaw({
  label: 'land-turn law',
  modelName: 'specs/cowshed/laws/land_turn.bend',
  kernel: new URL('../crates/cowshed-gateway/src/land_turn/laws/model.rs', import.meta.url),
  functions: [
    'step',
    'run',
    'empty',
    'queue',
    'counter',
    'fences',
    'waiting',
    'parked',
    'find',
    'inside',
    'turn_ok_b',
    'chain_b',
  ],
  laws: ['one_holder_check', 'fifo_check'],
  cap: 400,
  properties: 2000,
  propertyLength: 24,
  regenerate: 'bun run --filter @smoothbricks/cowshed emit-land-turn-law',
});
