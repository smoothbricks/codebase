import { censusStart, SAME_TICK_NS } from '../ensure-built-witness.js';

/**
 * Returns once a census started from then on proves every node that changed before the call: until then such a census
 * records it unproven (ensure-built-witness.ts, `unproven`), and a reader distrusts its stamp. A node's change time is
 * at most the wall clock at the call, which `censusStart` reads to the millisecond below, so the wait ends one
 * millisecond and {@link SAME_TICK_NS} past that reading. It waits on the clock the rule reads, not a guessed delay.
 */
export async function pastTick(): Promise<void> {
  const proven = censusStart() + 1_000_000n + SAME_TICK_NS;
  while (censusStart() < proven) await Bun.sleep(1);
}
