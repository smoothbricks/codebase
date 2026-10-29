import { describe, expect, it } from 'bun:test';
import {
  type AdmissionWaitEvent,
  admissionWaitMs,
  type BoundEvent,
  type BoundKind,
  type BoundLimits,
  earliestBound,
  NO_ADMISSION_WAIT,
  stepAdmissionWait,
  stepBounds,
  UNARMED,
} from './bounds.js';

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
      [{ kind: 'spawn', at: 0, testing: true, fixture: true, queued: false }, progress(10, true, false)],
      { kind: 'fixture', at: 200 },
    ],
    [
      // The 2026-10-28 Linux Publish red: the runner's startup took longer than the progress bound before the
      // fixture's build declared itself, and startup is bounded.
      'startup is bounded: a build declared after the progress bound comes too late',
      { idleMs: 100, fixtureMs: 200 },
      [{ kind: 'spawn', at: 0, testing: true, fixture: true, queued: false }],
      { kind: 'idle', at: 100 },
    ],
    [
      'after the fixture, the command is on the progress bound from the fixture’s end',
      { idleMs: 100, fixtureMs: 200 },
      [
        { kind: 'spawn', at: 0, testing: true, fixture: true, queued: false },
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
        { kind: 'spawn', at: 0, testing: true, fixture: false, queued: false },
        progress(60, true, true, true),
        progress(90, true, true),
      ],
      { kind: 'idle', at: 160 },
    ],
    [
      'the total ceiling wins when it comes first, and on a tie',
      { idleMs: 100, totalMs: 100 },
      [{ kind: 'spawn', at: 0, testing: true, fixture: false, queued: false }],
      { kind: 'total', at: 100 },
    ],
    [
      'no limits arm nothing',
      {},
      [{ kind: 'spawn', at: 0, testing: true, fixture: true, queued: false }, progress(5, true, true, true)],
      null,
    ],
    [
      // The 2026-10-29 sb-r22 gate: `mbx reserve -- bun test …` waited in mbx's pool on a loaded host and the progress
      // bound expired it before the pool admitted it.
      'a run queued in its pool arms nothing, however long the queue, and what it reports moves no clock',
      { idleMs: 100, totalMs: 1_000 },
      [{ kind: 'spawn', at: 0, testing: true, fixture: false, queued: true }, progress(500, true, true, true)],
      null,
    ],
    [
      'the grant starts every bound a spawn would have started',
      { idleMs: 100, fixtureMs: 200, totalMs: 1_000 },
      [
        { kind: 'spawn', at: 0, testing: true, fixture: true, queued: true },
        { kind: 'granted', at: 5_000, testing: true, fixture: true },
      ],
      { kind: 'idle', at: 5_100 },
    ],
    [
      'after the grant a finished test restarts the progress bound as ever',
      { idleMs: 100 },
      [
        { kind: 'spawn', at: 0, testing: true, fixture: false, queued: true },
        { kind: 'granted', at: 5_000, testing: true, fixture: false },
        progress(5_060, true, true, true),
      ],
      { kind: 'idle', at: 5_160 },
    ],
    [
      'a grant for a run that never queued changes nothing',
      { idleMs: 100 },
      [
        { kind: 'spawn', at: 0, testing: true, fixture: false, queued: false },
        { kind: 'granted', at: 5_000, testing: true, fixture: false },
      ],
      { kind: 'idle', at: 100 },
    ],
    [
      'g/r pauses the idle budget until another admission, without resetting it',
      { idleMs: 100 },
      [
        { kind: 'spawn', at: 0, testing: true, fixture: false, queued: true },
        { kind: 'granted', at: 500, testing: true, fixture: false },
        { kind: 'released', at: 540 },
        { kind: 'granted', at: 5_000, testing: true, fixture: false },
      ],
      { kind: 'idle', at: 5_060 },
    ],
    [
      'the final r stops idle, not the total ceiling',
      { idleMs: 100, totalMs: 1_000 },
      [
        { kind: 'spawn', at: 0, testing: true, fixture: false, queued: true },
        { kind: 'granted', at: 500, testing: true, fixture: false },
        { kind: 'released', at: 540 },
      ],
      { kind: 'total', at: 1_500 },
    ],
    [
      'one release of overlapping holds leaves idle running',
      { idleMs: 100 },
      [
        { kind: 'spawn', at: 0, testing: true, fixture: false, queued: true },
        { kind: 'granted', at: 500, testing: true, fixture: false },
        { kind: 'granted', at: 510, testing: true, fixture: false },
        { kind: 'released', at: 540 },
      ],
      { kind: 'idle', at: 600 },
    ],
    [
      'a finished test resets the paused budget even when its release arrived first',
      { idleMs: 100 },
      [
        { kind: 'spawn', at: 0, testing: true, fixture: false, queued: true },
        { kind: 'granted', at: 500, testing: true, fixture: false },
        { kind: 'released', at: 540 },
        progress(550, true, true, true),
        { kind: 'granted', at: 5_000, testing: true, fixture: false },
      ],
      { kind: 'idle', at: 5_100 },
    ],
    [
      'an unscheduled leaf stays bounded with no hold, including after compiler g/r',
      { idleMs: 100 },
      [
        { kind: 'spawn', at: 0, testing: true, fixture: false, queued: false },
        { kind: 'granted', at: 10, testing: true, fixture: false },
        { kind: 'released', at: 20 },
        progress(30, true, true, true),
      ],
      { kind: 'idle', at: 130 },
    ],
    [
      'a holder killed without r remains bounded',
      { idleMs: 100 },
      [
        { kind: 'spawn', at: 0, testing: true, fixture: false, queued: true },
        { kind: 'granted', at: 500, testing: true, fixture: false },
      ],
      { kind: 'idle', at: 600 },
    ],
    [
      'any pending task request pauses idle despite overlapping held grants, and resumes only after the queue empties',
      { idleMs: 100 },
      [
        { kind: 'spawn', at: 0, testing: true, fixture: false, queued: true },
        { kind: 'granted', at: 500, testing: true, fixture: false },
        { kind: 'granted', at: 510, testing: true, fixture: false },
        { kind: 'admission-queue', at: 540, waiting: true, testing: true },
        { kind: 'granted', at: 550, testing: true, fixture: false },
        { kind: 'released', at: 560 },
        { kind: 'admission-queue', at: 5_000, waiting: false, testing: true },
      ],
      { kind: 'idle', at: 5_060 },
    ],
    [
      'a finished test during explicit queue wait resets the remaining budget, not the queue pause',
      { idleMs: 100 },
      [
        { kind: 'spawn', at: 0, testing: true, fixture: false, queued: false },
        { kind: 'admission-queue', at: 40, waiting: true, testing: true },
        progress(50, true, true, true),
        { kind: 'admission-queue', at: 5_000, waiting: false, testing: true },
      ],
      { kind: 'idle', at: 5_100 },
    ],
    [
      'an unsupported runner pauses only on explicit queue evidence and resumes its remaining budget',
      { idleMs: 100 },
      [
        { kind: 'spawn', at: 0, testing: true, fixture: false, queued: false },
        { kind: 'admission-queue', at: 40, waiting: true, testing: true },
        { kind: 'admission-queue', at: 60, waiting: true, testing: true },
        { kind: 'granted', at: 70, testing: true, fixture: false },
        { kind: 'admission-queue', at: 5_000, waiting: false, testing: true },
      ],
      { kind: 'idle', at: 5_060 },
    ],
    [
      'an empty snapshot never exempts an unsupported runner',
      { idleMs: 100 },
      [
        { kind: 'spawn', at: 0, testing: true, fixture: false, queued: false },
        { kind: 'admission-queue', at: 50, waiting: false, testing: true },
      ],
      { kind: 'idle', at: 100 },
    ],
    [
      'explicit queue wait after first admission never pauses total or fixture ceilings',
      { idleMs: 100, fixtureMs: 200, totalMs: 1_000 },
      [
        { kind: 'spawn', at: 0, testing: true, fixture: true, queued: true },
        { kind: 'granted', at: 500, testing: true, fixture: true },
        { kind: 'admission-queue', at: 540, waiting: true, testing: true },
      ],
      { kind: 'fixture', at: 700 },
    ],
    [
      'clearing an explicit queue cannot resume a supported leaf with no held grant',
      { idleMs: 100 },
      [
        { kind: 'spawn', at: 0, testing: true, fixture: false, queued: true },
        { kind: 'granted', at: 500, testing: true, fixture: false },
        { kind: 'admission-queue', at: 540, waiting: true, testing: true },
        { kind: 'released', at: 550 },
        { kind: 'admission-queue', at: 600, waiting: false, testing: true },
        { kind: 'granted', at: 5_000, testing: true, fixture: false },
      ],
      { kind: 'idle', at: 5_060 },
    ],
    [
      'a pending task request stops idle even while another grant remains held',
      { idleMs: 100 },
      [
        { kind: 'spawn', at: 0, testing: true, fixture: false, queued: true },
        { kind: 'granted', at: 500, testing: true, fixture: false },
        { kind: 'admission-queue', at: 540, waiting: true, testing: true },
      ],
      null,
    ],
    [
      'explicit admission wait cannot pause the total ceiling after its first grant',
      { idleMs: 100, totalMs: 1_000 },
      [
        { kind: 'spawn', at: 0, testing: true, fixture: false, queued: true },
        { kind: 'granted', at: 500, testing: true, fixture: false },
        { kind: 'admission-queue', at: 540, waiting: true, testing: true },
      ],
      { kind: 'total', at: 1_500 },
    ],
    [
      'a snapshot completing after a finished test pauses only the budget left at completion, never its old poll start',
      { idleMs: 100 },
      [
        { kind: 'spawn', at: 0, testing: true, fixture: false, queued: false },
        // Poll starts at 20. A finished test at 60 owns the new budget before its snapshot completes at 80.
        progress(60, true, true, true),
        { kind: 'admission-queue', at: 80, waiting: true, testing: true },
        { kind: 'admission-queue', at: 1_000, waiting: false, testing: true },
      ],
      { kind: 'idle', at: 1_080 },
    ],
  ])('%s', (_title, limits, events, expected) => {
    expect(firstExpiry(limits, events)).toEqual(expected);
  });
});

it('freezes observed admission wait at terminal/cancellation stop, excluding teardown and report reads', () => {
  let wait = NO_ADMISSION_WAIT;
  const walk: readonly [AdmissionWaitEvent, number][] = [
    [{ kind: 'snapshot', at: 0, waiting: false }, 0],
    [{ kind: 'snapshot', at: 10, waiting: true }, 0],
    [{ kind: 'snapshot', at: 30, waiting: true }, 20],
    [{ kind: 'snapshot', at: 50, waiting: false }, 40],
    [{ kind: 'snapshot', at: 60, waiting: true }, 40],
    [{ kind: 'stop', at: 80 }, 60],
    [{ kind: 'stop', at: 1_000 }, 60],
    [{ kind: 'snapshot', at: 2_000, waiting: true }, 60],
  ];
  for (const [event, expected] of walk) {
    wait = stepAdmissionWait(wait, event);
    expect(admissionWaitMs(wait, event.at)).toBe(expected);
  }
  expect(wait).toEqual({ kind: 'stopped', waitedMs: 60 });
  expect(admissionWaitMs(wait, 10_000)).toBe(60);
});
