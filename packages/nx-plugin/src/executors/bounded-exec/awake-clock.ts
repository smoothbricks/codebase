/** The idle clock tracks awake time, not wall time or CPU consumed by this process. */
export async function openAwakeClock(): Promise<() => number> {
  // Bun 1.4.4 uses Rust Instant, and Linux Node uses CLOCK_MONOTONIC. Darwin Node's libuv uses
  // mach_continuous_time, which includes sleep; its awake reading comes from the cowshed addon.
  if (process.platform !== 'darwin' || process.versions.bun !== undefined) return () => performance.now();
  const { awakeTimeMs } = await import('@smoothbricks/cowshed/awake-clock');
  awakeTimeMs(); // Refuse a missing or stale addon before the executor spawns anything.
  return awakeTimeMs;
}

/** A timer is only a wake-up. An early callback arms the remaining time on the same clock. */
export function atClockDeadline(at: number, now: () => number, reached: () => void): () => void {
  const wake = (): void => {
    const remaining = at - now();
    if (remaining > 0) timer = setTimeout(wake, Math.ceil(remaining));
    else reached();
  };
  let timer = setTimeout(wake, Math.ceil(Math.max(0, at - now())));
  return () => clearTimeout(timer);
}
