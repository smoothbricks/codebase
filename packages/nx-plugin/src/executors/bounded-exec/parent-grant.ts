import { type TokenGrant, takeParentCpuTokens } from './cpu-pool.js';
import type { CpuWaitRow } from './lease-wait.js';
import type { InheritedCpuPool } from './pool-environment.js';

/**
 * What a run asks its parent's CPU pool and says of the answer. How many tokens the pool lends, how long the run waits
 * and what it is charged are values that differ between runs of one task key; what a token count does to the command is
 * `runner-sizing.ts`'s, keyed (README, "Bounded exec key").
 */

/** Tokens a parent pool lent, held until released. */
export type GrantedTokens = Extract<TokenGrant, { granted: true }>;

/** The parent pool's answer to a run: a grant, a refusal that fails the run, or neither, and what to say of it. */
export interface ParentGrant {
  readonly grant: GrantedTokens | null;
  /** A pool that answered and refused is a live budget, and the run fails rather than run beside it. */
  readonly refusal: string | null;
  readonly wait: CpuWaitRow | undefined;
  /** What the run says, in order. */
  readonly said: readonly string[];
}

/** What a run asks for: its count, what its task's last recorded peak charges it, and whether it runs alone. */
export interface Ask {
  readonly want: number;
  readonly charge: number | null;
  /** The tokens the task was lent when its recorded peak was measured. */
  readonly parallelism: number | null;
  readonly alone: boolean;
  readonly task: string | undefined;
}

/**
 * Ask the parent pool for the run's tokens. A run that inherits no pool runs as asked and says nothing; a pool whose
 * endpoint is gone (its holder exited) lends nothing, and the run goes on as asked.
 */
export async function askParentPool(inherited: InheritedCpuPool | null, ask: Ask): Promise<ParentGrant> {
  if (inherited === null) {
    return { grant: null, refusal: null, wait: undefined, said: [] };
  }
  const answer = await takeParentCpuTokens(inherited, ask.want, ask.charge, {
    parallelism: ask.parallelism ?? undefined,
    alone: ask.alone ? true : undefined,
    task: ask.task,
  });
  if (answer.granted) {
    return { grant: answer, refusal: null, wait: undefined, said: [] };
  }
  if (answer.cause === 'absent') {
    return {
      grant: null,
      refusal: null,
      wait: undefined,
      said: [`cowshed: cpu-pool parent pool absent: ${answer.reason}; running the command as asked\n`],
    };
  }
  return { grant: null, refusal: answer.reason, wait: answer.wait, said: [] };
}

/** What a run says once its grant is in hand. */
export function describeGrant(
  grant: GrantedTokens,
  run: { waitedMs: number; want: number; runner: string; charge: number | null },
): string[] {
  const said = [
    `cowshed: cpu-pool wait done elapsed=${run.waitedMs}ms status=ok tokens=${grant.tokens}/${run.want} runner=${run.runner} memory=${run.charge ?? 'none'}${grant.charged !== null && grant.charged !== run.charge ? ` charged=${grant.charged}` : ''}\n`,
  ];
  if (grant.oversized !== null) {
    said.push(
      `cowshed: cpu-pool granted while nothing else holds memory: this task's last peak of ${run.charge} bytes alone exceeds the run's ${grant.oversized.memory} bytes\n`,
    );
  }
  return said;
}
