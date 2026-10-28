import { describe, expect, it } from 'bun:test';
import { type BoundEvent, type BoundKind, type BoundLimits, earliestBound, stepBounds, UNARMED } from './bounds.js';

/** The bound that would fire first after `events`, all on one injected clock. */
function firstExpiry(limits: BoundLimits, events: readonly BoundEvent[]): { kind: BoundKind; at: number } | null {
  return earliestBound(events.reduce((bounds, event) => stepBounds(bounds, limits, event), UNARMED));
}

const progress = (at: number, wasTesting: boolean, testing: boolean, finished = false): BoundEvent => ({
  kind: 'progress',
  at,
  wasTesting,
  testing,
  finished,
});

describe('bounded-exec bounds', () => {
  it.each<[string, BoundLimits, readonly BoundEvent[], { kind: BoundKind; at: number } | null]>([
    [
      'a silent fixture ends at its own ceiling: the progress bound stops for its build',
      { idleMs: 100, fixtureMs: 200 },
      [{ kind: 'spawn', at: 0, testing: true, fixture: true }, progress(10, true, false)],
      { kind: 'fixture', at: 200 },
    ],
    [
      // The 2026-10-28 Linux Publish red: the runner's startup took longer than the progress bound before the
      // fixture's build declared itself, and startup is bounded.
      'startup is bounded: a build declared after the progress bound comes too late',
      { idleMs: 100, fixtureMs: 200 },
      [{ kind: 'spawn', at: 0, testing: true, fixture: true }],
      { kind: 'idle', at: 100 },
    ],
    [
      'after the fixture, the command is on the progress bound from the fixture’s end',
      { idleMs: 100, fixtureMs: 200 },
      [
        { kind: 'spawn', at: 0, testing: true, fixture: true },
        progress(10, true, false),
        { kind: 'fixture-done' },
        progress(150, false, true),
      ],
      { kind: 'idle', at: 250 },
    ],
    [
      'a finished test restarts the progress bound; other output does not',
      { idleMs: 100 },
      [
        { kind: 'spawn', at: 0, testing: true, fixture: false },
        progress(60, true, true, true),
        progress(90, true, true),
      ],
      { kind: 'idle', at: 160 },
    ],
    [
      'the total ceiling wins when it comes first, and on a tie',
      { idleMs: 100, totalMs: 100 },
      [{ kind: 'spawn', at: 0, testing: true, fixture: false }],
      { kind: 'total', at: 100 },
    ],
    [
      'no limits arm nothing',
      {},
      [{ kind: 'spawn', at: 0, testing: true, fixture: true }, progress(5, true, true, true)],
      null,
    ],
  ])('%s', (_title, limits, events, expected) => {
    expect(firstExpiry(limits, events)).toEqual(expected);
  });
});
