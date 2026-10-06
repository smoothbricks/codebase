//#region smoo/lmao!n/trace-root-timestamps #wall-anchor
/**
 * How fast a host may slew its wall clock against its monotonic clock, in parts per million: the adjtime/NTP bound on
 * Linux and Darwin. A bound learned at one monotonic instant widens by this much of the time elapsed since.
 */
const MAX_SLEW_PPM = 500n;

const NANOS_PER_MILLI = 1_000_000n;

/**
 * Where the wall clock stands against one monotonic clock: a trace root's sub-millisecond wall-clock anchor.
 *
 * A trace root's anchor is the wall clock every stamp of the trace is measured from. `Date.now()` alone is the wall
 * clock truncated to the millisecond, so an anchor taken from it stamps the whole trace up to 1 ms early: a span whose
 * parent another process stamped with an exact wall-clock read (a Rust writer reads `CLOCK_REALTIME` at every span
 * boundary) then appears to start before that parent. JavaScript has no sub-millisecond wall-clock read, so the offset
 * between the wall clock and the monotonic clock is learned instead.
 *
 * Each anchor reads `Date.now()` just before and just after its monotonic read. The wall clock at that read is at least
 * the millisecond read before it and less than the millisecond after the one read after it, which bounds the offset
 * from both sides. Anchors that fall at different points of a millisecond tighten the bounds, so they converge on the
 * offset as roots are created. A bound learned earlier widens by the host's maximum slew rate for the monotonic time
 * since, so a slewing clock stays inside them; a fresh read that contradicts them (the wall clock was stepped) starts
 * them over. Within the bounds the anchor follows `guess`, the platform's own sub-millisecond estimate
 * (`performance.timeOrigin + performance.now()`): measured within ~2 µs of `CLOCK_REALTIME` in a plain Bun process, but
 * 200–320 µs early under `bun test`, which the bounds pull back once a root proves it impossible. With no guess the
 * anchor takes the middle of the bounds.
 */
export class WallClock {
  /** Lower bound on `wall − monotonic` in nanoseconds; `undefined` until the first anchor. */
  private lower: bigint | undefined = undefined;
  /** Upper bound on `wall − monotonic` in nanoseconds. */
  private upper = 0n;
  /** The monotonic instant the bounds were last tightened at. */
  private learnedAt = 0n;

  /**
   * The wall clock, in nanoseconds since the Unix epoch, at monotonic instant `monotonic`. `beforeMs` is `Date.now()`
   * read just before that monotonic read and `afterMs` just after it; `guess` is the platform's estimate of the wall
   * clock at the same instant, `undefined` where it has none.
   */
  anchor(monotonic: bigint, beforeMs: number, afterMs: number, guess: bigint | undefined): bigint {
    // A wall clock stepped back between the two reads leaves only the later one describing it.
    const lowerNow = BigInt(Math.min(beforeMs, afterMs)) * NANOS_PER_MILLI - monotonic;
    const upperNow = (BigInt(afterMs) + 1n) * NANOS_PER_MILLI - 1n - monotonic;
    if (this.lower === undefined) {
      this.lower = lowerNow;
      this.upper = upperNow;
    } else {
      const slewed = monotonic > this.learnedAt ? ((monotonic - this.learnedAt) * MAX_SLEW_PPM) / 1_000_000n : 0n;
      const lower = maxOf(this.lower - slewed, lowerNow);
      const upper = minOf(this.upper + slewed, upperNow);
      // Bounds that no longer meet mean the wall clock stepped: only this read describes it now.
      [this.lower, this.upper] = lower > upper ? [lowerNow, upperNow] : [lower, upper];
    }
    this.learnedAt = monotonic;
    const offset =
      guess === undefined
        ? this.lower + (this.upper - this.lower) / 2n
        : minOf(maxOf(guess - monotonic, this.lower), this.upper);
    return monotonic + offset;
  }
}

/** `performance.timeOrigin + performanceNowMs` in nanoseconds; `undefined` in a runtime with no `timeOrigin`. */
export function performanceWallNanos(performanceNowMs: number): bigint | undefined {
  const origin = performance.timeOrigin;
  if (!Number.isFinite(origin)) return undefined;
  return millisToNanos(origin) + millisToNanos(performanceNowMs);
}

/** A fractional millisecond count in nanoseconds, the whole and fractional parts converted apart to keep the digits. */
export function millisToNanos(millis: number): bigint {
  const whole = Math.floor(millis);
  return BigInt(whole) * NANOS_PER_MILLI + BigInt(Math.round((millis - whole) * 1_000_000));
}

function maxOf(left: bigint, right: bigint): bigint {
  return left > right ? left : right;
}

function minOf(left: bigint, right: bigint): bigint {
  return left < right ? left : right;
}
//#endregion smoo/lmao!n/trace-root-timestamps #wall-anchor
