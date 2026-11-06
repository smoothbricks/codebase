import typia from 'typia';
import {
  HOST_MEMORY,
  type LiveUsageReading,
  type ResidentUsageReading,
  type RunMemoryReading,
  readLiveUsage,
  readRunMemory,
} from './host-memory.js';

/** Diagnostic facts, not configuration: neither value changes a grant or a runner's parallelism. */
export const DIAGNOSTIC_CONTEXT_ENV = 'BOUNDED_EXEC_MEMORY_DIAGNOSTICS';
export const DIAGNOSTIC_MODULE_ENV = 'BOUNDED_EXEC_DIAGNOSTICS_MODULE';

interface GrantMemoryContext {
  readonly chargedBytes: number;
  readonly bound: RunMemoryReading;
}
const parseContext = typia.json.createAssertParse<GrantMemoryContext>();

/** A sub-pool charges no memory: descendants retain the memory-bearing ancestor's facts. */
export async function grantMemoryEnvironment(
  chargedBytes: number | null,
  inherited: Readonly<NodeJS.ProcessEnv>,
  readMemory: () => Promise<RunMemoryReading> = readRunMemory,
): Promise<Record<string, string>> {
  if (chargedBytes === null) {
    return {
      [DIAGNOSTIC_CONTEXT_ENV]: inherited[DIAGNOSTIC_CONTEXT_ENV] ?? '',
      [DIAGNOSTIC_MODULE_ENV]: inherited[DIAGNOSTIC_MODULE_ENV] ?? '',
    };
  }
  const context: GrantMemoryContext = { chargedBytes, bound: await readMemory() };
  return { [DIAGNOSTIC_CONTEXT_ENV]: JSON.stringify(context), [DIAGNOSTIC_MODULE_ENV]: import.meta.url };
}

export interface ResidentSample {
  readonly sampledAtMs: number;
  readonly reading: ResidentUsageReading;
}

/** The original grant plus the bound's live reading at exit, never a new accounting ledger. */
export function childExitMemory(
  contextText: string | undefined,
  lastResident: ResidentSample,
  readLive: (bound: string) => LiveUsageReading = readLiveUsage,
) {
  if (!contextText)
    return { lastResident, pool: { ok: false as const, reason: 'no memory-bearing CPU grant was inherited' } };
  let context: GrantMemoryContext;
  try {
    context = parseContext(contextText);
  } catch (error) {
    return {
      lastResident,
      pool: {
        ok: false as const,
        reason: `invalid inherited memory diagnostic: ${error instanceof Error ? error.message : String(error)}`,
      },
    };
  }
  const live: LiveUsageReading = context.bound.ok
    ? readLive(context.bound.memory.source)
    : { ok: false, reason: `${context.bound.error.subject}: ${context.bound.error.cause}` };
  return { lastResident, pool: { ok: true as const, ...context, live } };
}

/** Retains the last readable sample through PID exit; stop is called by the child's exit event. */
export function observeChildMemory(pid: number, contextText: string | undefined) {
  const sample = (): ResidentSample => ({
    sampledAtMs: Date.now(),
    reading: HOST_MEMORY?.residentUsage(pid) ?? {
      ok: false,
      reason: `${process.platform} has no per-process RSS reader`,
    },
  });
  let lastResident = sample();
  const timer = setInterval(() => {
    const next = sample();
    if (next.reading.ok || !lastResident.reading.ok) lastResident = next;
  }, 250);
  timer.unref();
  return () => {
    clearInterval(timer);
    return childExitMemory(contextText, lastResident);
  };
}
