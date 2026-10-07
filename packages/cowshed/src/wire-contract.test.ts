/// <reference types="bun" />
/// <reference types="node" />

import { describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { cpSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import typia from 'typia';
import type { JobKillArguments } from './native.generated.js';
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
  /** Present only for the exports that resolve a JSON array (`listJobs`, `listWorkspaces`). */
  readonly assertMany?: (value: unknown) => readonly T[];
  readonly parseMany?: (json: string) => readonly T[];
}

/** The corpus case name the Rust side uses for the array document of a listing export. */
const LIST_CASE = 'list';

const seamTypes = {
  JobInfo: {
    assertOne: validators.assertJobInfo,
    parseOne: validators.parseJobInfo,
    assertMany: validators.assertJobInfoList,
    parseMany: validators.parseJobInfoList,
  },
  WorkspaceInfo: {
    assertOne: validators.assertWorkspaceInfo,
    parseOne: validators.parseWorkspaceInfo,
    assertMany: validators.assertWorkspaceInfoList,
    parseMany: validators.parseWorkspaceInfoList,
  },
  GrantSet: { assertOne: validators.assertGrantSet, parseOne: validators.parseGrantSet },
  LandReport: { assertOne: validators.assertLandReport, parseOne: validators.parseLandReport },
  RebaseReport: { assertOne: validators.assertRebaseReport, parseOne: validators.parseRebaseReport },
  PushReport: { assertOne: validators.assertPushReport, parseOne: validators.parsePushReport },
  GcReport: { assertOne: validators.assertGcReport, parseOne: validators.parseGcReport },
  DoctorReport: { assertOne: validators.assertDoctorReport, parseOne: validators.parseDoctorReport },
  RemoveReport: { assertOne: validators.assertRemoveReport, parseOne: validators.parseRemoveReport },
  ResizeResult: { assertOne: validators.assertResizeResult, parseOne: validators.parseResizeResult },
} satisfies Record<string, SeamType<unknown>>;

/**
 * The corpus is a build artifact of the Rust side, so it is validated as untrusted input rather
 * than imported as a typed module: a truncated or hand-edited file must fail loudly here instead
 * of quietly reducing the number of documents that get checked.
 */
const assertCorpus = typia.createAssert<Record<string, Record<string, unknown>>>();

const corpus = assertCorpus(JSON.parse(readFileSync(new URL('./wire-fixtures.json', import.meta.url), 'utf8')));

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

  for (const [name, seam] of Object.entries<SeamType<unknown>>(seamTypes)) {
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
        });
      }
    });
  }

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
    const accounting = { kind: 'macOsRusageChildren', cpu: { userUs: 1_250_000, sysUs: 80_000 }, io: null };
    expect<unknown>(validators.assertJobAccounting(accounting)).toEqual(accounting);
    expect<unknown>(validators.parseJobAccounting(JSON.stringify(accounting))).toEqual(accounting);
    for (const refused of [
      // The bytes are unavailable, not omitted.
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
});

describe('process events', () => {
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

describe('canonical DTO generation', () => {
  it('a declared field changes the generated type and the executed validator', async () => {
    const sourceDirectory = dirname(fileURLToPath(import.meta.url));
    const project = fileURLToPath(new URL('..', import.meta.url));
    const scratch = mkdtempSync(join(sourceDirectory, '.api-generation-'));
    try {
      const core = join(scratch, 'crates/cowshed-core/src');
      const gateway = join(scratch, 'crates/cowshed-gateway-types/src');
      cpSync(join(project, 'crates/cowshed-core/src'), core, { recursive: true });
      mkdirSync(gateway, { recursive: true });
      cpSync(join(project, 'crates/cowshed-gateway-types/src/status.rs'), join(gateway, 'status.rs'));
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

      const generator = fileURLToPath(new URL('../../../target/debug/cowshed-api-gen', import.meta.url));
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
});
