/// <reference types="bun" />
/// <reference types="node" />

import { describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { cpSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { basename, dirname, join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import typia from 'typia';
import type {
  CpuBudgetStatus,
  JobProcessRetirement,
  JobSubtreeAdmission,
  JobSubtreeRefusal,
  JobSubtreeRetirement,
} from './api.generated.js';
import type { JobKillArguments } from './native.generated.js';
import * as readers from './reader.generated.js';
import type { GrantDelta } from './types.js';
import * as validators from './validators.generated.js';

/**
 * The consumer half of the N-API wire contract. The corpus is real core serde output;
 * these are the exact generated validators the public facade consumes. A source DTO
 * change regenerates both the types and their validators, while the corpus witnesses
 * the serializer's actual shape. Exact assertions reject extra fields as well as missing
 * ones; JSON parsing uses those same assertions rather than silently retaining extras.
 */

/** One document, or a list of them where the export returns an array. */
interface SeamType<T> {
  readonly assertOne: (value: unknown) => T;
  readonly parseOne: (json: string) => T;
  /** Present only for the types an export carries as a JSON array (`listJobs`; `listWorkspaces`' two lists). */
  readonly assertMany?: (value: unknown) => readonly T[];
  readonly parseMany?: (json: string) => readonly T[];
}

/** A peer's result: its document also has the generated reader (07_api.md, "Reader projection"). */
interface ResultSeam<T> extends SeamType<T> {
  readonly readOne: (json: string) => T;
  /** Present only where a peer answers the array (`listJobs`). */
  readonly readMany?: (json: string) => readonly T[];
}

/**
 * `value` once for every object of two or more members in it, at any depth, with a member
 * `lingering` added to that object alone, holding a copy of one of its own values. In a record that
 * member is one a newer peer wrote and this build does not declare; in a map (keyed by data) it is
 * one more valid entry. An object of one member may be an externally tagged variant, which is
 * exactly one member however new its writer, and an empty one has no value to copy: neither is
 * widened, and a seam's root is widened by its caller.
 */
function widenings(value: unknown): readonly unknown[] {
  if (Array.isArray(value)) {
    return value.flatMap((element, index) =>
      widenings(element).map((wider) => value.map((entry, position) => (position === index ? wider : entry))),
    );
  }
  if (typeof value !== 'object' || value === null) {
    return [];
  }
  const members = Object.entries(value);
  const first = members[0];
  const own = first !== undefined && members.length > 1 ? [{ ...value, lingering: first[1] }] : [];
  return [
    ...own,
    ...members.flatMap(([key, member]) => widenings(member).map((wider) => ({ ...value, [key]: wider }))),
  ];
}

/** The corpus case name the Rust side uses for the array document of a listing export. */
const LIST_CASE = 'list';

const seamTypes = {
  JobInfo: {
    assertOne: validators.assertJobInfo,
    parseOne: validators.parseJobInfo,
    assertMany: validators.assertJobInfoList,
    parseMany: validators.parseJobInfoList,
    readOne: readers.readJobInfo,
    readMany: readers.readJobInfoList,
  },
  JobProcessTree: {
    assertOne: validators.assertJobProcessTree,
    parseOne: validators.parseJobProcessTree,
    readOne: readers.readJobProcessTree,
  },
  JobProcessEvent: {
    assertOne: validators.assertJobProcessEvent,
    parseOne: validators.parseJobProcessEvent,
    readOne: readers.readJobProcessEvent,
  },
  WorkspaceInfo: {
    assertOne: validators.assertWorkspaceInfo,
    parseOne: validators.parseWorkspaceInfo,
    assertMany: validators.assertWorkspaceInfoList,
    parseMany: validators.parseWorkspaceInfoList,
    readOne: readers.readWorkspaceInfo,
  },
  UnprovenWorkspace: {
    assertOne: validators.assertUnprovenWorkspace,
    parseOne: validators.parseUnprovenWorkspace,
    assertMany: validators.assertUnprovenWorkspaceList,
    parseMany: validators.parseUnprovenWorkspaceList,
    // This DTO reaches a peer inside project.list, never as a standalone result.
    readOne: (json: string) => {
      const listing = readers.readWorkspaceListView(JSON.stringify({ workspaces: [], unproven: [JSON.parse(json)] }));
      return validators.assertUnprovenWorkspace(listing.unproven[0]);
    },
    readMany: (json: string) =>
      readers.readWorkspaceListView(JSON.stringify({ workspaces: [], unproven: JSON.parse(json) })).unproven,
  },
  GrantSet: {
    assertOne: validators.assertGrantSet,
    parseOne: validators.parseGrantSet,
    readOne: readers.readGrantSet,
  },
  LandReport: {
    assertOne: validators.assertLandReport,
    parseOne: validators.parseLandReport,
    readOne: readers.readLandReport,
  },
  RebaseReport: {
    assertOne: validators.assertRebaseReport,
    parseOne: validators.parseRebaseReport,
    readOne: readers.readRebaseReport,
  },
  PushReport: {
    assertOne: validators.assertPushReport,
    parseOne: validators.parsePushReport,
    readOne: readers.readPushReport,
  },
  GcReport: {
    assertOne: validators.assertGcReport,
    parseOne: validators.parseGcReport,
    readOne: readers.readGcReport,
  },
  DoctorReport: {
    assertOne: validators.assertDoctorReport,
    parseOne: validators.parseDoctorReport,
    readOne: readers.readDoctorReport,
  },
  RemoveReport: {
    assertOne: validators.assertRemoveReport,
    parseOne: validators.parseRemoveReport,
    readOne: readers.readRemoveReport,
  },
  ResizeResult: {
    assertOne: validators.assertResizeResult,
    parseOne: validators.parseResizeResult,
    readOne: readers.readResizeResult,
  },
} satisfies Record<string, ResultSeam<unknown>>;

/**
 * The corpus is a build artifact of the Rust side, so it is validated as untrusted input rather
 * than imported as a typed module: a truncated or hand-edited file must fail loudly here instead
 * of quietly reducing the number of documents that get checked.
 */
const assertCorpus = typia.createAssert<Record<string, Record<string, unknown>>>();

const corpus = assertCorpus(JSON.parse(readFileSync(new URL('./wire-fixtures.json', import.meta.url), 'utf8')));

it('decodes repository-bearing gate queue status with the generated strict validator', () => {
  const ledger: CpuBudgetStatus = {
    total: 4,
    held: 1,
    checkouts: [
      { checkout: '/w/holder', held: 1, running: 1, waiting: 0 },
      { checkout: '/w/waiter', held: 0, running: 0, waiting: 1 },
    ],
    gateJobs: [
      {
        repository: 'owner/repo',
        checkout: '/w/holder',
        job: 0,
        held: 1,
        holders: [{ command: 'held gate', tokens: 1, want: 1, heldMs: 0 }],
      },
    ],
    gateQueue: [
      { repository: 'owner/repo', checkout: '/w/waiter', job: 1, command: 'waiting gate', want: 1, waitedMs: 0 },
    ],
    background: [],
  };
  expect(validators.parseCpuBudgetStatus(JSON.stringify(ledger))).toEqual(ledger);
  expect(() => validators.assertCpuGateJob({ checkout: '/w/holder', job: 0, held: 1, holders: [] })).toThrow();
  expect(() =>
    validators.assertCpuGateWaiter({ checkout: '/w/waiter', job: 1, command: 'waiting gate', want: 1, waitedMs: 0 }),
  ).toThrow();
});

describe('napi wire contract', () => {
  it('checks admission keys by UTF-8 bytes without allocating an encoder', () => {
    const Encoder = globalThis.TextEncoder;
    let encoders = 0;
    class CountedEncoder extends Encoder {
      constructor() {
        super();
        encoders += 1;
      }
    }
    globalThis.TextEncoder = CountedEncoder;
    try {
      for (const key of ['a'.repeat(4096), 'é'.repeat(2048), `${'€'.repeat(1365)}a`, '😀'.repeat(1024)]) {
        expect(validators.assertAdmissionKey(key)).toBe(key);
        expect(() => validators.assertAdmissionKey(`${key}a`)).toThrow();
      }
      expect(() => validators.assertAdmissionKey('')).toThrow();
    } finally {
      globalThis.TextEncoder = Encoder;
    }
    expect(encoders).toBe(0);
  });

  it('has a corpus and a validator for exactly the same seam types', () => {
    // A name on one side only is drift by itself: a new Rust DTO with no TypeScript validator, or
    // a validator whose corpus was deleted. Either way nothing is being witnessed.
    expect(Object.keys(corpus).sort()).toEqual(Object.keys(seamTypes).sort());
  });

  for (const [name, seam] of Object.entries<ResultSeam<unknown>>(seamTypes)) {
    describe(name, () => {
      const cases = corpus[name] ?? {};

      for (const [caseName, document] of Object.entries(cases)) {
        it(`accepts the ${caseName} document and nothing wider`, () => {
          const assertValue = caseName === LIST_CASE ? seam.assertMany : seam.assertOne;
          const parseValue = caseName === LIST_CASE ? seam.parseMany : seam.parseOne;
          if (assertValue === undefined || parseValue === undefined) {
            throw new Error(`${name} has a ${caseName} document but no matching validator`);
          }

          expect(assertValue(document)).toEqual(document);
          expect(parseValue(JSON.stringify(document))).toEqual(document);

          // Widen records, not their list wrapper; an empty list has no record to widen.
          const documents: readonly unknown[] = Array.isArray(document) ? document : [document];
          for (const [index, value] of documents.entries()) {
            if (typeof value !== 'object' || value === null || Array.isArray(value)) {
              throw new Error(`${name} ${caseName} has a non-object document at index ${index}`);
            }
            const widerValue = { ...value, lingering: true };
            const wider =
              caseName === LIST_CASE
                ? documents.map((entry, position) => (position === index ? widerValue : entry))
                : widerValue;
            expect(() => assertValue(wider)).toThrow();
            expect(() => parseValue(JSON.stringify(wider))).toThrow();
          }
        });

        it(`reads a newer peer's ${caseName} document as the declared one at every depth`, () => {
          // Each record through its reader, and a listing through its own where a peer answers one.
          const read = caseName === LIST_CASE ? seam.readMany : seam.readOne;
          const parse = caseName === LIST_CASE ? seam.parseMany : seam.parseOne;
          const documents: readonly unknown[] =
            caseName === LIST_CASE && Array.isArray(document) ? document : [document];
          const cases: ReadonlyArray<readonly [unknown, (json: string) => unknown, (json: string) => unknown]> = [
            ...documents.map((value) => [value, seam.readOne, seam.parseOne] as const),
            ...(read === undefined || parse === undefined || read === seam.readOne
              ? []
              : [[document, read, parse] as const]),
          ];
          let projected = 0;
          for (const [value, readValue, parseValue] of cases) {
            // Each seam is a record, so its root takes an undeclared member even when it is empty.
            const root =
              typeof value === 'object' && value !== null && !Array.isArray(value)
                ? [{ ...value, lingering: true }]
                : [];
            for (const newer of [...root, ...widenings(value)]) {
              const json = JSON.stringify(newer);
              let strict: unknown;
              try {
                strict = parseValue(json);
              } catch {
                // A member no declaration names: the reader drops it and reads the declared document.
                expect(readValue(json)).toEqual(value);
                projected += 1;
                continue;
              }
              // A map's data key: one more entry both readers keep.
              expect(readValue(json)).toEqual(strict);
            }
          }
          expect(projected).toBeGreaterThan(0);
        });
      }
    });
  }

  it('projects no unknown variant, no missing member and no request', () => {
    // Members of two untagged arms at once select neither, and an external tag with a second
    // member is no variant: both refuse, as the Rust reader does.
    const usage = seamTypes.JobProcessEvent.readOne(JSON.stringify(corpus.JobProcessEvent?.usage));
    expect(() => readers.readJobProcessEvent(JSON.stringify({ ...usage, blockedOn: 'clear' }))).toThrow();
    const blocker = seamTypes.JobProcessEvent.assertOne(corpus.JobProcessEvent?.blockerPipe);
    expect(readers.readJobProcessEvent(JSON.stringify(blocker))).toEqual(blocker);
    expect(() =>
      readers.readJobProcessEvent(JSON.stringify({ ...blocker, blockedOn: { set: { kind: 'pipe' }, futureKey: 1 } })),
    ).toThrow();

    const job = seamTypes.JobInfo.assertOne(corpus.JobInfo?.exited);
    expect(() => readers.readJobInfo(JSON.stringify({ ...job, state: 'hibernating' }))).toThrow();
    const { jobId: _jobId, ...withoutJob } = job;
    expect(() => readers.readJobInfo(JSON.stringify(withoutJob))).toThrow();

    const request = {
      repoId: 'smoothbricks/codebase',
      workspace: 'raven',
      workspaceIncarnation: '0198f2c0b7e34dc795f17b238b331c80',
      jobId: 3,
    };
    expect(validators.parseJobRequest(JSON.stringify(request))).toEqual(request);
    expect(() => validators.parseJobRequest(JSON.stringify({ ...request, lingering: true }))).toThrow();
  });

  it('pins process observation identity, omitted evidence, and non-empty changes', () => {
    const tree = seamTypes.JobProcessTree.assertOne(corpus.JobProcessTree?.unread);
    expect(tree.processes).toHaveLength(1);
    expect(tree.processes[0]).not.toHaveProperty('usage');
    expect(tree.processes[0]).not.toHaveProperty('blockedOn');
    expect(tree.processes[0]).not.toHaveProperty('exit');
    const { jobId: _jobId, ...withoutJob } = tree;
    expect(() => seamTypes.JobProcessTree.assertOne(withoutJob)).toThrow();

    const event = seamTypes.JobProcessEvent.assertOne(corpus.JobProcessEvent?.born);
    expect(event.kind).toBe('born');
    expect(() => seamTypes.JobProcessEvent.assertOne({ ...event, index: -1 })).toThrow();
    expect(() => seamTypes.JobProcessEvent.assertOne({ kind: 'changed', index: 0 })).toThrow();
    expect(() =>
      seamTypes.JobProcessEvent.assertOne({ kind: 'gap', reason: { kind: 'eventsLost' }, index: 0 }),
    ).toThrow();
  });

  it('rejects the flattened argv this file was written to catch', () => {
    // The substitution test for the whole corpus. `JobInfo.argv` was typed `string[]` while the
    // wire carried tagged `CommandArg` objects, so every real listJobs/status/wait parse threw.
    // Flattening argv back to bare strings must fail, or none of the above is guarding anything.
    const job = seamTypes.JobInfo.assertOne(corpus.JobInfo?.queued);
    const flattened = { ...job, argv: (job.argv ?? []).map((argument) => argument.data) };

    expect(() => seamTypes.JobInfo.assertOne(flattened)).toThrow();
    expect(() => seamTypes.JobInfo.parseOne(JSON.stringify(flattened))).toThrow();
  });

  it('rejects an unknown property the Rust side would refuse', () => {
    // `deny_unknown_fields` on the Rust DTOs has no force on the parse direction; `assertEquals`
    // is the only thing that makes an extra wire field visible in TypeScript.
    const job = seamTypes.JobInfo.assertOne(corpus.JobInfo?.exited);

    expect(() => seamTypes.JobInfo.assertOne({ ...job, lingering: true })).toThrow();
  });

  it('rejects a stream whose typed shape has been hollowed back out to unknown', () => {
    // `stdout`/`stderr`/`stdin`/`exit`/`outputLimit` were `unknown`, which accepted anything.
    // A swapped stream object must now fail rather than survive the boundary.
    const job = seamTypes.JobInfo.assertOne(corpus.JobInfo?.exited);

    expect(() => seamTypes.JobInfo.assertOne({ ...job, stdout: { storage: 'captured' } })).toThrow();
    expect(() => seamTypes.JobInfo.assertOne({ ...job, stdin: {} })).toThrow();
    expect(() => seamTypes.JobInfo.assertOne({ ...job, exit: { kind: 'exited' } })).toThrow();
  });

  it('keeps a non-UTF-8 argument tagged rather than mangled', () => {
    const job = seamTypes.JobInfo.assertOne(corpus.JobInfo?.signaledNonUtf8Argv);

    expect((job.argv ?? []).map((argument) => argument.encoding)).toEqual(['utf8', 'utf8', 'base64']);
    // The tag is load-bearing: these bytes are not valid UTF-8, so a `string[]` argv would have
    // had to lose them. Decoding the tagged form must reproduce them exactly.
    const tagged = job.argv?.at(-1);
    expect(tagged?.encoding).toBe('base64');
    expect(Array.from(Buffer.from(tagged?.data ?? '', 'base64'))).toEqual([0xff, 0xfe, 0x80]);
  });

  it('states a retirement receipt only as the receipt, never as a default', () => {
    // A job that retired nothing omits both facts, so an absent receipt can never read as `kernel`
    // or as a survivor list that found nothing.
    const exited = seamTypes.JobInfo.assertOne(corpus.JobInfo?.exited);
    expect(exited).not.toHaveProperty('containment');
    expect(exited).not.toHaveProperty('outlived');
    expect(exited).not.toHaveProperty('outlivedIncomplete');

    const kernel = seamTypes.JobInfo.assertOne(corpus.JobInfo?.retiredKernel);
    expect(kernel.containment).toEqual({ kind: 'kernel' });
    expect(kernel).not.toHaveProperty('outlived');
    expect(kernel).not.toHaveProperty('outlivedIncomplete');

    const incomplete = seamTypes.JobInfo.assertOne(corpus.JobInfo?.retiredKernelIncomplete);
    expect(incomplete.containment).toEqual({ kind: 'kernel' });
    expect(incomplete.outlivedIncomplete).toBe(true);
    expect(incomplete).not.toHaveProperty('outlived');
    expect(seamTypes.JobInfo.parseOne(JSON.stringify(incomplete))).toEqual(incomplete);
    const incompleteWithRows = seamTypes.JobInfo.assertOne(corpus.JobInfo?.retiredKernelIncompleteWithRows);
    expect(incompleteWithRows.outlivedIncomplete).toBe(true);
    expect(incompleteWithRows.outlived).toHaveLength(2);
    for (const outlivedIncomplete of [null, 'true', 1]) {
      expect(() => seamTypes.JobInfo.assertOne({ ...incomplete, outlivedIncomplete })).toThrow();
    }

    const observed = seamTypes.JobInfo.assertOne(corpus.JobInfo?.retiredObserved);
    expect(observed.containment?.kind).toBe('observed');
    // An index or image nothing observed is absent: unknown, never zero or an empty string.
    expect(observed.outlived?.[1]).toEqual({ pid: 4248, signal: 9 });
    expect(() => seamTypes.JobInfo.assertOne({ ...observed, containment: { kind: 'observed' } })).toThrow();
    expect(() => seamTypes.JobInfo.assertOne({ ...kernel, containment: { kind: 'userspace' } })).toThrow();

    // Draining is a state of its own: the leader ended and its membership is being retired.
    expect(seamTypes.JobInfo.assertOne(corpus.JobInfo?.draining).state).toBe('draining');
  });

  it('keeps subtree handles opaque and outcomes closed in generated validators', () => {
    const handle = { jobId: 7, index: 2, incarnation: '0123456789abcdef0123456789abcdef' };
    const admitted: JobSubtreeAdmission = { kind: 'admitted', handle };
    expect(validators.assertJobSubtreeHandle(handle)).toEqual(handle);
    expect(validators.parseJobSubtreeAdmission(JSON.stringify(admitted))).toEqual(admitted);
    const reasons: readonly JobSubtreeRefusal[] = [
      'foldUnavailable',
      'notMember',
      'foreignHandle',
      'jobEnded',
      'spentHandle',
    ];
    for (const reason of reasons) {
      const refused: JobSubtreeAdmission = { kind: 'refused', reason };
      expect(validators.assertJobSubtreeAdmission(refused)).toEqual(refused);
      expect(validators.parseJobSubtreeRetirement(JSON.stringify(refused))).toEqual(refused);
    }
    const retirement: JobProcessRetirement = {
      containment: { kind: 'kernel' },
      outlived: [],
      outlivedIncomplete: true,
    };
    const retired: JobSubtreeRetirement = { kind: 'retired', retirement };
    expect(validators.parseJobSubtreeRetirement(JSON.stringify(retired))).toEqual(retired);
    expect(() => validators.assertJobSubtreeHandle({ ...handle, pid: 4242 })).toThrow();
    expect(() => validators.assertJobSubtreeAdmission({ ...admitted, reason: 'notMember' })).toThrow();
    expect(() => validators.assertJobSubtreeAdmission({ kind: 'refused', reason: 'unknown' })).toThrow();
    expect(() => validators.assertJobSubtreeRetirement({ kind: 'retired' })).toThrow();
    expect(() => validators.assertJobSubtreeRetirement({ kind: 'retired', retirement: { outlived: [] } })).toThrow();
    expect(() => validators.assertJobSubtreeRetirement({ ...retired, handle })).toThrow();
  });

  it('preserves the null working directory as null rather than as absent', () => {
    // Unlike every other optional in JobInfo, the controller always emits `cwd`. Typing it
    // optional would have admitted an absent third state the wire cannot produce.
    const job = seamTypes.JobInfo.assertOne(corpus.JobInfo?.queued);
    expect(job.cwd).toBeNull();

    const { cwd: _cwd, ...withoutCwd } = job;
    expect(() => seamTypes.JobInfo.assertOne(withoutCwd)).toThrow();
  });

  it('admits a job with exactly one of argv and script', () => {
    // The Rust side flattens one `ExecCommand`, so no job carries both or neither. A type that
    // made both fields optional would accept either impossible shape.
    const script = seamTypes.JobInfo.assertOne(corpus.JobInfo?.scriptSyntax);
    const argv = seamTypes.JobInfo.assertOne(corpus.JobInfo?.queued);
    expect(script.failure).toBe('scriptSyntax');

    expect(() => seamTypes.JobInfo.assertOne({ ...script, argv: argv.argv })).toThrow();
    const { script: _script, ...neither } = script;
    expect(() => seamTypes.JobInfo.assertOne(neither)).toThrow();
  });

  it('sends a port request under the name the Rust GrantDelta deserializes', () => {
    // The input direction has no serde corpus: `public_api_contracts.rs` pins the Rust side to
    // `{"servicePorts":80}`, and this pins `GrantDelta` in `types.ts` to the same key, so a rename
    // on either side is red here rather than a request the controller refuses as unknown.
    const assertDelta = typia.createAssertEquals<GrantDelta>();
    expect(assertDelta({ servicePorts: 80 })).toEqual({ servicePorts: 80 });
    expect(() => assertDelta({ service_ports: 80 })).toThrow();
    expect(() => assertDelta({ servicePorts: '80' })).toThrow();
  });

  it('types the arguments of a handle that binds every request field as exactly empty', () => {
    // A job handle binds every `job.kill` field. `Pick<JobRequest, never>` would be `{}`, which
    // admits any non-nullish value; the generated type admits exactly an empty object.
    // @ts-expect-error a primitive is not a job.kill caller's arguments
    const _primitive: JobKillArguments = 1;
    const isKillArguments = typia.createEquals<JobKillArguments>();
    expect(isKillArguments({})).toBe(true);
    expect(isKillArguments({ jobId: 7 })).toBe(false);
    expect(isKillArguments(1)).toBe(false);
  });

  it('keeps blocks a relocated workspace still reserves apart from its current block', () => {
    // The `open` corpus document carries both: `portBlock` is the gateway endpoint and job env, while
    // `retainedPortBlocks` are the blocks it moved away from and still owns. A singular or misspelled
    // retained field must fail rather than drop the reservation on the TypeScript side.
    const grants = seamTypes.GrantSet.assertOne(corpus.GrantSet?.open);
    expect(grants.portBlock).toEqual({ base: 40960, size: 128 });
    expect(grants.retainedPortBlocks).toEqual([{ base: 41088, size: 64 }]);

    const { retainedPortBlocks, ...current } = grants;
    expect(() => seamTypes.GrantSet.assertOne({ ...current, retainedPortBlock: retainedPortBlocks?.[0] })).toThrow();
    expect(() => seamTypes.GrantSet.assertOne({ ...current, retainedPortBlocks: retainedPortBlocks?.[0] })).toThrow();
  });

  it('refuses a host load no finite f64 holds, as the Rust constructor does', () => {
    // JSON has no infinity literal, but an overflowing one parses to it: `1e400` is Infinity.
    for (const load1 of [Number.POSITIVE_INFINITY, Number.NEGATIVE_INFINITY, Number.NaN, -1]) {
      expect(() => validators.assertHostLoad1(load1)).toThrow();
      expect(() => validators.assertHostLoadSample({ load1, cores: 1 })).toThrow();
    }
    expect(() => validators.parseHostLoad1('1e400')).toThrow();
    expect(() => validators.parseHostLoadSample('{"load1":1e400,"cores":1}')).toThrow();
    for (const load1 of [0, 12.5, Number.MAX_VALUE]) {
      expect(validators.assertHostLoad1(load1)).toBe(load1);
      expect(validators.assertHostLoadSample({ load1, cores: 1 })).toEqual({ load1, cores: 1 });
    }
    expect(validators.parseHostLoad1('1.7976931348623157e308')).toBe(Number.MAX_VALUE);
  });
});

const resourceUnitSeams = {
  CpuMicros: { assertOne: validators.assertCpuMicros, parseOne: validators.parseCpuMicros },
  ResidentBytes: { assertOne: validators.assertResidentBytes, parseOne: validators.parseResidentBytes },
  StorageIoBytes: { assertOne: validators.assertStorageIoBytes, parseOne: validators.parseStorageIoBytes },
} satisfies Record<string, SeamType<number>>;

describe('checked resource units', () => {
  for (const [name, seam] of Object.entries<SeamType<number>>(resourceUnitSeams)) {
    it(`${name} shares Rust's safe-integer boundary`, () => {
      for (const value of [0, Number.MAX_SAFE_INTEGER]) {
        expect(seam.assertOne(value)).toBe(value);
        expect(seam.parseOne(JSON.stringify(value))).toBe(value);
      }
      for (const value of [-1, 0.5, Number.MAX_SAFE_INTEGER + 1]) {
        expect(() => seam.assertOne(value)).toThrow();
        expect(() => seam.parseOne(JSON.stringify(value))).toThrow();
      }
      expect(() => seam.assertOne(Number.POSITIVE_INFINITY)).toThrow();
      expect(() => seam.assertOne(Number.NaN)).toThrow();
    });
  }
});

describe('job accounting', () => {
  it('names its source and keeps unavailable bytes absent, never zero', () => {
    const accounting = {
      kind: 'macOsRusageChildren',
      cpu: { userUs: 1_250_000, sysUs: 80_000 },
      reconciliation: {
        cpuUs: -12_500,
        coverage: { kind: 'gap', reason: { kind: 'uncountedFork', pid: 4242 } },
      },
    };
    expect<unknown>(validators.assertJobAccounting(accounting)).toEqual(accounting);
    expect<unknown>(validators.parseJobAccounting(JSON.stringify(accounting))).toEqual(accounting);
    for (const refused of [
      // Unavailable bytes are absent, never null.
      { ...accounting, io: null },
      // The reconciliation is part of the total, never omitted.
      { kind: 'macOsRusageChildren', cpu: accounting.cpu },
      // No block-operation count stands in for bytes.
      { ...accounting, pageins: 12 },
      // Totals come from a declared source, never a sum of the processes a sampler saw.
      { ...accounting, kind: 'liveMembers' },
      { ...accounting, cpu: { userUs: Number.MAX_SAFE_INTEGER + 1, sysUs: 0 } },
    ]) {
      expect(() => validators.assertJobAccounting(refused)).toThrow();
    }
  });

  it('accepts the Linux cgroup source with charged memory and reconciled byte totals', () => {
    const linux = {
      kind: 'linuxCgroupV2',
      cpu: { userUs: 1_250_000, sysUs: 80_000 },
      io: { readBytes: 4096, writeBytes: 0 },
      chargedMemory: { currentBytes: 1_048_576, peakBytes: 2_097_152 },
      reconciliation: {
        cpuUs: 0,
        ioReadBytes: 4096,
        ioWriteBytes: 0,
        coverage: { kind: 'complete' },
      },
    };
    expect<unknown>(validators.assertJobAccounting(linux)).toEqual(linux);
  });

  it('attributes CPU to a named leaf, or names why it cannot', () => {
    const sample = {
      jobId: 1,
      sampledAt: '2026-07-13T00:00:00Z',
      wallMs: 50,
      wallUs: 50_000,
      leaderPid: 4242,
      members: [4242],
      leaf: { kind: 'noAccountedCpu' },
      cpuUserMs: 0,
      cpuSysMs: 0,
      cpuPct: 0,
      hostStart: { load1: 1.5, cores: 10 },
      host: { load1: 2.5, cores: 10 },
      rssBytes: 4096,
      rssPeakBytes: 4096,
      accounting: {
        kind: 'macOsRusageChildren',
        cpu: { userUs: 0, sysUs: 0 },
        reconciliation: {
          cpuUs: 0,
          coverage: { kind: 'gap', reason: { kind: 'uncountedFork', pid: 4242 } },
        },
      },
      volumes: { workspace: { kind: 'unavailable', reason: { kind: 'unconfigured' } } },
      stdout: { bytes: 0, lines: 0 },
      stderr: { bytes: 0, lines: 0 },
    };
    expect<unknown>(validators.assertJobResourceSample(sample)).toEqual(sample);
    for (const refused of [
      // A leaf is a named process or a stated reason; no third kind exists.
      { ...sample, leaf: { kind: 'largest' } },
      // The leaf is part of the sample, never omitted.
      { ...sample, leaf: undefined },
    ]) {
      expect(() => validators.assertJobResourceSample(refused)).toThrow();
    }
  });
});

describe('process events', () => {
  it('preserves a coverage gap without inventing a process index', () => {
    for (const reason of [
      { kind: 'eventsLost' },
      { kind: 'unobservedBirth', pid: 7 },
      { kind: 'unobservedExit', pid: 7 },
      { kind: 'uncountedFork', pid: 7 },
      { kind: 'unreadImage', pid: 7 },
      { kind: 'unreadFinalUsage', pid: 7 },
      { kind: 'unreadExitStatus', pid: 7 },
    ]) {
      const event = { kind: 'gap', reason };
      expect<unknown>(validators.assertJobProcessEvent(event)).toEqual(event);
      expect<unknown>(validators.parseJobProcessEvent(JSON.stringify(event))).toEqual(event);
      expect(() => validators.assertJobProcessEvent({ ...event, index: 0 })).toThrow();
    }
    expect(() => validators.assertJobProcessEvent({ kind: 'gap' })).toThrow();
  });

  it('refuses a process change that changes nothing', () => {
    // Rust cannot build a change of nothing; the generated validator must not admit one either,
    // or a TypeScript consumer accepts an event no supervisor can emit.
    const usage = {
      cpuUserUs: 10,
      cpuSysUs: 0,
      busy: false,
      rssBytes: 4096,
      rssPeakBytes: 4096,
      io: { kind: 'read', readBytes: 0, writeBytes: 0 },
    };
    for (const change of [{ blockedOn: { set: { kind: 'none' } } }, { blockedOn: 'clear' }, { usage }]) {
      const event = { kind: 'changed', index: 3, ...change };
      expect<unknown>(validators.assertJobProcessEvent(event)).toEqual(event);
      expect<unknown>(validators.parseJobProcessEvent(JSON.stringify(event))).toEqual(event);
    }
    expect(() => validators.assertJobProcessEvent({ kind: 'changed', index: 3 })).toThrow();
    expect(() => validators.parseJobProcessEvent('{"kind":"changed","index":3}')).toThrow();
    expect(() => validators.assertJobProcessDelta({ index: 3 })).toThrow();
    // One observation changes one field: a usage and a blocker are two changes, as in Rust.
    expect(() => validators.assertJobProcessDelta({ index: 3, usage, blockedOn: 'clear' })).toThrow();
  });
});

const assertGeneratedModule = typia.createAssert<{
  readonly assertWorkspaceInfo: (value: unknown) => unknown;
  readonly parseWorkspaceInfo: (json: string) => unknown;
}>();

const project = fileURLToPath(new URL('..', import.meta.url));

/**
 * Where a generation run makes its throwaway project: the package's `.cache/`, never `src/`.
 * `cowshed:lint` runs beside `wire-test`, and a Biome pass that met the compact
 * `tsconfig.test.json` the generation test writes failed the gate. `.cache/` is gitignored, so
 * Biome (`vcs.useIgnoreFile`) and Nx's file index never list what is in it. It stays inside the
 * package because the compiler preload finds Typia's transform through the nearest `package.json`
 * and resolves `typia` through the nearest `node_modules`.
 */
const scratchPrefix = join(project, '.cache', 'api-generation-');

describe('canonical DTO generation', () => {
  it('a declared field changes the generated type and the executed validator', async () => {
    const sourceDirectory = dirname(fileURLToPath(import.meta.url));
    const scratch = mkdtempSync(scratchPrefix);
    try {
      const core = join(scratch, 'crates/cowshed-core/src');
      const gateway = join(scratch, 'crates/cowshed-gateway-types/src');
      cpSync(join(project, 'crates/cowshed-core/src'), core, { recursive: true });
      mkdirSync(gateway, { recursive: true });
      for (const name of ['cpu_budget.rs', 'status.rs']) {
        cpSync(join(project, 'crates/cowshed-gateway-types/src', name), join(gateway, name));
      }
      // Every generation runs the CLI/N-API parity gate, which reads the CLI grammar from args.rs
      // (specs/cowshed/07_api.md, "CLI and N-API option parity").
      const cli = join(scratch, 'crates/cowshed-cli/src');
      mkdirSync(cli, { recursive: true });
      cpSync(join(project, 'crates/cowshed-cli/src/args.rs'), join(cli, 'args.rs'));
      mkdirSync(join(scratch, 'src'));
      mkdirSync(join(scratch, 'crates/cowshed-napi/src'), { recursive: true });
      // A separate project snapshots the mutant after it exists; the running test's program
      // was already snapshotted before the scratch files were created.
      writeFileSync(
        join(scratch, 'src/tsconfig.test.json'),
        JSON.stringify({
          extends: join(sourceDirectory, 'tsconfig.test.json'),
          compilerOptions: { rootDir: '.', composite: false, noEmit: true },
          include: ['./*.ts'],
        }),
      );
      const declaration = join(core, 'api/dto.rs');
      const original = readFileSync(declaration, 'utf8');
      const changed = original.replace(
        'pub struct WorkspaceInfo {',
        'pub struct WorkspaceInfo {\n    pub generation_probe: bool,',
      );
      expect(changed).not.toBe(original);
      writeFileSync(declaration, changed);

      const generator = fileURLToPath(new URL('../.cache/api-generator/cowshed-api-gen', import.meta.url));
      const generated = spawnSync(generator, ['write', scratch], { encoding: 'utf8' });
      if (generated.status !== 0) {
        throw new Error(`API generator failed (${generated.status}): ${generated.stderr}`, { cause: generated.error });
      }
      expect(readFileSync(join(scratch, 'src/api.generated.ts'), 'utf8')).toMatch(
        /readonly ['"]?generationProbe['"]?: boolean/,
      );
      const generatedPath = join(scratch, 'src/api.generated.ts');
      const canonical = readFileSync(generatedPath, 'utf8');
      const drifted = `${canonical}\n// deliberately drifted output\n`;
      writeFileSync(generatedPath, drifted);
      const checked = spawnSync(generator, ['check', scratch], { encoding: 'utf8' });
      expect(checked.status).toBe(1);
      expect(checked.stderr).toContain('is stale');
      expect(readFileSync(generatedPath, 'utf8')).toBe(drifted);
      writeFileSync(generatedPath, canonical);
      const module: unknown = await import(pathToFileURL(join(scratch, 'src/validators.generated.ts')).href);
      const changedValidators = assertGeneratedModule(module);
      const baseline = seamTypes.WorkspaceInfo.assertOne(
        Object.values(corpus.WorkspaceInfo ?? {}).find((value) => !Array.isArray(value)),
      );
      expect(() => changedValidators.assertWorkspaceInfo(baseline)).toThrow();
      expect(() => changedValidators.parseWorkspaceInfo(JSON.stringify(baseline))).toThrow();
      const withField = { ...baseline, generationProbe: true };
      expect(changedValidators.assertWorkspaceInfo(withField)).toEqual(withField);
      expect(changedValidators.parseWorkspaceInfo(JSON.stringify(withField))).toEqual(withField);
      expect(() => validators.assertWorkspaceInfo(withField)).toThrow();
    } finally {
      rmSync(scratch, { recursive: true });
    }
  });

  it('keeps its scratch project out of the Biome pass that lints this package', () => {
    const scratch = mkdtempSync(scratchPrefix);
    try {
      // The file that failed the gate: compact JSON, which Biome's formatter rejects wherever it can see it.
      writeFileSync(join(scratch, 'tsconfig.test.json'), JSON.stringify({ include: ['./*.ts'] }));
      const lint = spawnSync('biome', ['check', '--files-ignore-unknown=true', '.'], {
        cwd: project,
        encoding: 'utf8',
      });
      expect(lint.error).toBeUndefined();
      const report = `${lint.stdout}${lint.stderr}`;
      // The pass ran to the end and never named the scratch, so it did not read it.
      expect(report).toContain('Checked ');
      expect(report).not.toContain(basename(scratch));
    } finally {
      rmSync(scratch, { recursive: true });
    }
  });
});
