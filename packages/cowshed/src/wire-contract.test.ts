/// <reference types="bun" />
/// <reference types="node" />

import { describe, expect, it } from 'bun:test';
import { readFileSync } from 'node:fs';
import typia from 'typia';
import type {
  DoctorReport,
  GcReport,
  GrantSet,
  JobInfo,
  LandReport,
  PushReport,
  RemoveReport,
  ResizeResult,
  WorkspaceInfo,
} from './types.js';

/**
 * The consumer half of the napi wire contract.
 *
 * `wire-fixtures.json` is real `serde_json` output for every DTO the napi exports hand back, kept
 * current by `the_committed_wire_corpus_is_what_core_serializes` in
 * `crates/cowshed-napi/src/wire_contract.rs`. Here those same documents go through the typia
 * types `index.ts` validates with, so a Rust DTO that grows, loses, or renames a field is red on
 * this side until `types.ts` moves with it.
 *
 * Both directions of drift fail:
 * - a field the wire carries that `types.ts` does not name is an unexpected property — this is
 *   what caught `JobInfo.argv` typed `string[]` against the tagged `CommandArg` objects the
 *   controller actually emits;
 * - a field `types.ts` requires that the wire omits is a missing property.
 *
 * `assertEquals` rather than `assert` is the entire point. `assert` ignores excess properties and
 * would have stayed green through every drift this file exists to catch. `assertParse` runs beside
 * it because that is literally the validator `index.ts` applies to the napi JSON string.
 */

/** One document, or a list of them where the export returns an array. */
interface SeamType<T> {
  readonly assertOne: (value: unknown) => T;
  readonly parseOne: (json: string) => T;
  /** Present only for the exports that resolve a JSON array (`listJobs`, `listWorkspaces`). */
  readonly assertMany?: (value: unknown) => readonly T[];
  readonly parseMany?: (json: string) => readonly T[];
}

/** The corpus case name the Rust side uses for the array document of a listing export. */
const LIST_CASE = 'list';

const seamTypes = {
  JobInfo: {
    assertOne: typia.createAssertEquals<JobInfo>(),
    parseOne: typia.json.createAssertParse<JobInfo>(),
    assertMany: typia.createAssertEquals<JobInfo[]>(),
    parseMany: typia.json.createAssertParse<JobInfo[]>(),
  },
  WorkspaceInfo: {
    assertOne: typia.createAssertEquals<WorkspaceInfo>(),
    parseOne: typia.json.createAssertParse<WorkspaceInfo>(),
    assertMany: typia.createAssertEquals<WorkspaceInfo[]>(),
    parseMany: typia.json.createAssertParse<WorkspaceInfo[]>(),
  },
  GrantSet: {
    assertOne: typia.createAssertEquals<GrantSet>(),
    parseOne: typia.json.createAssertParse<GrantSet>(),
  },
  LandReport: {
    assertOne: typia.createAssertEquals<LandReport>(),
    parseOne: typia.json.createAssertParse<LandReport>(),
  },
  PushReport: {
    assertOne: typia.createAssertEquals<PushReport>(),
    parseOne: typia.json.createAssertParse<PushReport>(),
  },
  GcReport: {
    assertOne: typia.createAssertEquals<GcReport>(),
    parseOne: typia.json.createAssertParse<GcReport>(),
  },
  DoctorReport: {
    assertOne: typia.createAssertEquals<DoctorReport>(),
    parseOne: typia.json.createAssertParse<DoctorReport>(),
  },
  RemoveReport: {
    assertOne: typia.createAssertEquals<RemoveReport>(),
    parseOne: typia.json.createAssertParse<RemoveReport>(),
  },
  ResizeResult: {
    assertOne: typia.createAssertEquals<ResizeResult>(),
    parseOne: typia.json.createAssertParse<ResizeResult>(),
  },
} satisfies Record<string, SeamType<unknown>>;

/**
 * The corpus is a build artifact of the Rust side, so it is validated as untrusted input rather
 * than imported as a typed module: a truncated or hand-edited file must fail loudly here instead
 * of quietly reducing the number of documents that get checked.
 */
const assertCorpus = typia.createAssert<Record<string, Record<string, unknown>>>();

const corpus = assertCorpus(
  JSON.parse(readFileSync(new URL('./wire-fixtures.json', import.meta.url), 'utf8')),
);

describe('napi wire contract', () => {
  it('has a corpus and a validator for exactly the same seam types', () => {
    // A name on one side only is drift by itself: a new Rust DTO with no TypeScript validator, or
    // a validator whose corpus was deleted. Either way nothing is being witnessed.
    expect(Object.keys(corpus).sort()).toEqual(Object.keys(seamTypes).sort());
  });

  for (const [name, seam] of Object.entries<SeamType<unknown>>(seamTypes)) {
    describe(name, () => {
      const cases = corpus[name] ?? {};

      it('carries at least one document', () => {
        expect(Object.keys(cases).length).toBeGreaterThan(0);
      });

      for (const [caseName, document] of Object.entries(cases)) {
        it(`accepts the ${caseName} document and nothing wider`, () => {
          if (caseName === LIST_CASE) {
            const assertMany = seam.assertMany;
            const parseMany = seam.parseMany;
            expect(assertMany).toBeDefined();
            expect(parseMany).toBeDefined();
            expect(assertMany?.(document)).toEqual(document);
            expect(parseMany?.(JSON.stringify(document))).toEqual(document);
            return;
          }
          expect(seam.assertOne(document)).toEqual(document);
          expect(seam.parseOne(JSON.stringify(document))).toEqual(document);
        });
      }
    });
  }

  it('rejects the flattened argv this file was written to catch', () => {
    // The substitution test for the whole corpus. `JobInfo.argv` was typed `string[]` while the
    // wire carried tagged `CommandArg` objects, so every real listJobs/status/wait parse threw.
    // Flattening argv back to bare strings must fail, or none of the above is guarding anything.
    const job = seamTypes.JobInfo.assertOne(corpus.JobInfo?.queued);
    const flattened = { ...job, argv: job.argv.map((argument) => argument.data) };

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

    expect(job.argv.map((argument) => argument.encoding)).toEqual(['utf8', 'utf8', 'base64']);
    // The tag is load-bearing: these bytes are not valid UTF-8, so a `string[]` argv would have
    // had to lose them. Decoding the tagged form must reproduce them exactly.
    const tagged = job.argv.at(-1);
    expect(tagged?.encoding).toBe('base64');
    expect(Array.from(Buffer.from(tagged?.data ?? '', 'base64'))).toEqual([0xff, 0xfe, 0x80]);
  });

  it('preserves the null working directory as null rather than as absent', () => {
    // Unlike every other optional in JobInfo, the controller always emits `cwd`. Typing it
    // optional would have admitted an absent third state the wire cannot produce.
    const job = seamTypes.JobInfo.assertOne(corpus.JobInfo?.queued);
    expect(job.cwd).toBeNull();

    const { cwd: _cwd, ...withoutCwd } = job;
    expect(() => seamTypes.JobInfo.assertOne(withoutCwd)).toThrow();
  });
});
