/**
 * Which of a run's bounds is armed, and when each expires: the decision bounded-exec's timers carry out, kept here as
 * a pure function of the run's events and the times they happened at, so a table can test it with no clock and no
 * process (verdict.ts says why the bounds are separate).
 *
 * - A scheduled leaf arms nothing until its first grant. Later zero-hold intervals pause only the idle budget;
 *   total and fixture ceilings keep running. Unscheduled leaves start at spawn and never wait for a grant.
 * - The fixture ceiling ends with its segment. The idle clock also stops outside the testing lifecycle.
 * - Finished tests reset the idle budget whether their release byte arrived before or after their output.
 */

/** The limits a run was given, in milliseconds; an absent one never expires. */
export interface BoundLimits {
  readonly idleMs?: number;
  readonly fixtureMs?: number;
  readonly totalMs?: number;
}

/**
 * When each armed bound expires, in the clock's milliseconds; `null` when it is not armed. `queued`: the run waits for
 * its pool's grant, so none is armed yet.
 */
export interface Bounds {
  readonly idle: number | null;
  readonly fixture: number | null;
  readonly total: number | null;
  readonly queued: boolean;
  readonly holds: number | null;
  readonly idleRemaining: number | null;
}

export type BoundKind = 'idle' | 'fixture' | 'total';

export type BoundEvent =
  /**
   * The command started; `testing` is the lifecycle's answer then, `fixture` whether a supervised fixture leads it,
   * `queued` whether mbx schedules this leaf, as known before spawn.
   */
  | {
      readonly kind: 'spawn';
      readonly at: number;
      readonly testing: boolean;
      readonly fixture: boolean;
      readonly queued: boolean;
    }
  /** Each admission begins a hold; the first one starts the queued leaf's ceilings. */
  | { readonly kind: 'granted'; readonly at: number; readonly testing: boolean; readonly fixture: boolean }
  | { readonly kind: 'released'; readonly at: number }
  /**
   * The runner's state may have moved: a test finished (`finished`), a phase changed, or the lifecycle failed.
   * `wasTesting` is the answer before the event, `testing` the answer after it.
   */
  | {
      readonly kind: 'progress';
      readonly at: number;
      readonly testing: boolean;
      readonly wasTesting: boolean;
      readonly finished: boolean;
    }
  /** The fixture's segment ended: its ceiling no longer applies. */
  | { readonly kind: 'fixture-done' };

export const UNARMED: Bounds = {
  idle: null,
  fixture: null,
  total: null,
  queued: false,
  holds: null,
  idleRemaining: null,
};

/** The bounds a command that starts running at `at` arms. */
function armed(limits: BoundLimits, at: number, testing: boolean, fixture: boolean): Bounds {
  return {
    idle: testing && limits.idleMs !== undefined ? at + limits.idleMs : null,
    fixture: fixture && limits.fixtureMs !== undefined ? at + limits.fixtureMs : null,
    total: limits.totalMs !== undefined ? at + limits.totalMs : null,
    queued: false,
    holds: null,
    idleRemaining: limits.idleMs ?? null,
  };
}

/** The bounds after `event`. */
export function stepBounds(bounds: Bounds, limits: BoundLimits, event: BoundEvent): Bounds {
  switch (event.kind) {
    case 'spawn':
      return event.queued
        ? { ...UNARMED, queued: true, holds: 0, idleRemaining: limits.idleMs ?? null }
        : armed(limits, event.at, event.testing, event.fixture);
    case 'granted': {
      if (bounds.holds === null) return bounds;
      if (bounds.queued) return { ...armed(limits, event.at, event.testing, event.fixture), holds: 1 };
      return {
        ...bounds,
        holds: bounds.holds + 1,
        idle:
          bounds.holds === 0 && event.testing && bounds.idleRemaining !== null
            ? event.at + bounds.idleRemaining
            : bounds.idle,
      };
    }
    case 'released': {
      if (bounds.holds === null || bounds.holds === 0) return bounds;
      if (bounds.holds > 1) return { ...bounds, holds: bounds.holds - 1 };
      return {
        ...bounds,
        holds: 0,
        idle: null,
        idleRemaining: bounds.idle === null ? bounds.idleRemaining : Math.max(0, bounds.idle - event.at),
      };
    }
    case 'progress': {
      if (bounds.queued) return bounds;
      if (!event.testing) return { ...bounds, idle: null, idleRemaining: limits.idleMs ?? null };
      const restart = event.finished || !event.wasTesting;
      if (!restart || limits.idleMs === undefined) return bounds;
      return {
        ...bounds,
        idle: bounds.holds === 0 ? null : event.at + limits.idleMs,
        idleRemaining: limits.idleMs,
      };
    }
    case 'fixture-done':
      return { ...bounds, fixture: null };
  }
}

/** The bound that expires first, or `null` when none is armed. A tie names the total, then the fixture, then idle. */
export function earliestBound(bounds: Bounds): { readonly kind: BoundKind; readonly at: number } | null {
  let earliest: { kind: BoundKind; at: number } | null = null;
  for (const kind of ['total', 'fixture', 'idle'] as const) {
    const at = bounds[kind];
    if (at !== null && (earliest === null || at < earliest.at)) earliest = { kind, at };
  }
  return earliest;
}
