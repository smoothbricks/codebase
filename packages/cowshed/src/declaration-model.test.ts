/// <reference types="bun" />
/// <reference types="node" />

import { afterAll, beforeAll, describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { cpSync, mkdirSync, rmSync, writeFileSync } from 'node:fs';
import { basename, join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import type { DeclarationModel } from '@smoothbricks/cowshed/declaration-ir';
import { ownedFixtureRoot } from '@smoothbricks/nx-plugin/testing';
import typia, { TypeGuardError } from 'typia';

const assertPublicModel = typia.createAssertEquals<DeclarationModel>();

/**
 * The owner's public declaration IR, admitted as JSON. The validators are compiled from the owner CLI's own
 * `ir-types` and `ir-validators` output, so admission is exactly as strict as the generated `*Equals` types: an
 * extra key, a missing key or an unknown argument kind is refused by Typia, never filtered by hand.
 *
 * The real cowshed-core model is admitted unchanged. Refusals run on a scratch model whose probe declaration
 * guarantees the nodes being broken (a reference, a `from` attribute, a typed argument and a scalar accessor)
 * exist, so each refusal is caused by its own mutation and by nothing else.
 */

const project = fileURLToPath(new URL('..', import.meta.url));
const generator = fileURLToPath(new URL('../.cache/api-generator/cowshed-api-gen', import.meta.url));

/** The declarations the refusals break: a structural probe and one scalar per accessor spelling, addressed by name. */
const PROBE = `#[derive(Serialize)]
#[serde(rename_all = "camelCase", from = "AdmissionSource")]
pub struct AdmissionProbe<'a> {
    pub label: &'a str,
    pub bounded: Option<Vec<u8>>,
    pub token: Token<'static, 32>,
}
#[cfg_attr(
    any(),
    cowshed_api(
        accessor = "get",
        scalar = "number & tags.Type<'uint64'> & tags.Maximum<9007199254740991>"
    )
)]
pub struct ProbeCount(u64);
#[cfg_attr(any(), cowshed_api(accessor = "as-str", scalar = "string"))]
pub struct ProbeText(String);
#[cfg_attr(
    any(),
    cowshed_api(
        accessor = "display",
        scalar = "string & tags.Pattern<'^[0-9a-f]{64}$'>"
    )
)]
pub struct ProbeDigest([u8; 32]);
`;

/** Runs the owner CLI. A failing generator fails the test with its own diagnostics attached. */
function generate(args: readonly string[]): string {
  const result = spawnSync(generator, args, { encoding: 'utf8', maxBuffer: 256 * 1024 * 1024 });
  if (result.status !== 0) {
    throw new Error(`cowshed-api-gen ${args.join(' ')} failed (${result.status}): ${result.stderr}`, {
      cause: result.error,
    });
  }
  return result.stdout;
}

/** A parsed JSON value. The navigation below works over this type, never over `any`. */
type Json = null | boolean | number | string | Json[] | { [key: string]: Json };
type Path = readonly (string | number)[];

function child(parent: Json, segment: string | number): Json {
  if (typeof parent === 'object' && parent !== null) {
    const next: Json | undefined = Array.isArray(parent)
      ? parent[Number(segment)]
      : Object.hasOwn(parent, segment)
        ? parent[segment]
        : undefined;
    if (next !== undefined) {
      return next;
    }
  }
  throw new Error(`the probe model has no ${segment}`);
}

function objectAt(root: Json, path: Path): { [key: string]: Json } {
  const found = path.reduce<Json>(child, root);
  if (typeof found !== 'object' || found === null || Array.isArray(found)) {
    throw new Error(`${path.join('.')} is not an object`);
  }
  return found;
}

interface Refusal {
  readonly name: string;
  readonly mutate: (model: Json) => void;
}

const RECORD: Path = ['api', 'records', 'AdmissionProbe'];
const FIELDS: Path = [...RECORD, 'shape', 'data', 'data'];
const COUNT_PROJECTION: Path = ['api', 'records', 'ProbeCount', 'projection'];
const TEXT_PROJECTION: Path = ['api', 'records', 'ProbeText', 'projection'];
const DIGEST_PROJECTION: Path = ['api', 'records', 'ProbeDigest', 'projection'];

/** The closed accessor spellings, each with the exact scalar text its probe declares. */
const SCALARS = [
  ['ProbeCount', 'get', "number & tags.Type<'uint64'> & tags.Maximum<9007199254740991>"],
  ['ProbeText', 'as-str', 'string'],
  ['ProbeDigest', 'display', "string & tags.Pattern<'^[0-9a-f]{64}$'>"],
] as const;

const refusals: readonly Refusal[] = [
  {
    name: 'a missing native Copy fact',
    mutate: (model) => {
      Reflect.deleteProperty(objectAt(model, RECORD), 'copy');
    },
  },
  {
    name: 'a non-boolean native Copy fact',
    mutate: (model) => {
      objectAt(model, RECORD).copy = 'true';
    },
  },
  {
    name: 'a missing static native size',
    mutate: (model) => {
      Reflect.deleteProperty(objectAt(model, RECORD), 'staticSize');
    },
  },
  {
    name: 'a non-numeric static native size',
    mutate: (model) => {
      objectAt(model, RECORD).staticSize = '8';
    },
  },
  {
    name: 'an extra key in serde',
    mutate: (model) => {
      objectAt(model, [...RECORD, 'serde']).lingering = true;
    },
  },
  {
    name: 'a missing serde.from',
    mutate: (model) => {
      Reflect.deleteProperty(objectAt(model, [...RECORD, 'serde']), 'from');
    },
  },
  {
    name: 'a reference missing its lifetime',
    mutate: (model) => {
      Reflect.deleteProperty(objectAt(model, [...FIELDS, 0, 'ty']), 'lifetime');
    },
  },
  {
    name: 'an unknown type argument kind',
    mutate: (model) => {
      objectAt(model, [...FIELDS, 1, 'ty', 'arguments', 0]).kind = 'unknown';
    },
  },
  {
    name: 'an unknown scalar accessor',
    mutate: (model) => {
      objectAt(model, COUNT_PROJECTION).accessor = 'unknown';
    },
  },
  {
    name: 'a missing scalar accessor',
    mutate: (model) => {
      Reflect.deleteProperty(objectAt(model, TEXT_PROJECTION), 'accessor');
    },
  },
  {
    name: 'an extra accessor fact beside the display accessor',
    mutate: (model) => {
      objectAt(model, DIGEST_PROJECTION).guessedAccessor = 'get';
    },
  },
];

/**
 * The generated module's two exports under test. The owner emits an `assert*` and a `parse*` for every declaration,
 * and both run the same generated `*Equals` validator.
 */
interface Admission {
  readonly assertDeclarationModel: (value: unknown) => unknown;
  readonly parseDeclarationModel: (json: string) => unknown;
}
const loadAdmission = typia.createAssert<Admission>();

/** The error the generated validator raised for `admit`; a failure if it admitted the model instead. */
function refusal(admit: () => unknown): unknown {
  try {
    admit();
  } catch (error) {
    return error;
  }
  throw new Error('the generated validator admitted a model that must be refused');
}

describe('declaration model admission', () => {
  let scratch = '';
  let admission: Admission;
  let probe = '';

  beforeAll(async () => {
    scratch = await ownedFixtureRoot(join(project, '.cache'), 'ir-model-admission', 'clone-');
    const core = join(scratch, 'crates/cowshed-core/src');
    cpSync(join(project, 'crates/cowshed-core/src'), core, { recursive: true });
    mkdirSync(join(scratch, 'crates/cowshed-gateway-types/src'), { recursive: true });
    for (const file of ['status.rs', 'host_status.rs']) {
      cpSync(
        join(project, 'crates/cowshed-gateway-types/src', file),
        join(scratch, 'crates/cowshed-gateway-types/src', file),
      );
    }
    // The probe is written before the owner CLI reads the scratch project, so the model carries it.
    writeFileSync(join(core, 'api/admission_probe.rs'), PROBE);
    const rootModule = join(core, 'api/mod.rs');
    writeFileSync(rootModule, `${await Bun.file(rootModule).text()}\npub mod admission_probe;\n`);
    probe = generate(['ir-model', scratch]);

    // The generated files live in their own project so the Typia transform snapshots them after they exist.
    const src = join(scratch, 'src');
    mkdirSync(src);
    writeFileSync(
      join(src, 'tsconfig.test.json'),
      JSON.stringify({
        extends: join(project, 'tsconfig.test.json'),
        compilerOptions: { rootDir: '.', composite: false, noEmit: true },
        include: ['./*.ts'],
      }),
    );
    writeFileSync(join(src, 'declaration-model.ts'), generate(['ir-types', './declaration-model.js']));
    writeFileSync(join(src, 'declaration-model-validators.ts'), generate(['ir-validators', './declaration-model.js']));
    // The validators are generated into this scratch project at test time, so their specifier is runtime-selected.
    const module: unknown = await import(pathToFileURL(join(src, 'declaration-model-validators.ts')).href);
    admission = loadAdmission(module);
  });

  afterAll(() => {
    if (scratch !== '') {
      rmSync(scratch, { recursive: true, force: true });
    }
  });

  it('keeps its live source clone outside the strict disk-command source inventory', () => {
    const inventory = spawnSync('git', ['ls-files', '-z', '--cached', '--others', '--exclude-standard'], {
      cwd: project,
      encoding: 'utf8',
    });
    expect(inventory.error).toBeUndefined();
    expect(inventory.status).toBe(0);
    expect(inventory.stdout).toContain('crates/cowshed-core/src/storage/bootstrap/native/macos.rs');
    expect(inventory.stdout).not.toContain(basename(scratch));
  });

  it('admits the model the owner emits for the real cowshed-core declarations', () => {
    const real = generate(['ir-model', project]);
    const publicModel = assertPublicModel(admission.parseDeclarationModel(real));
    expect(publicModel).toEqual(JSON.parse(real));
    expect(Object.keys(publicModel.api.records).length).toBeGreaterThan(0);
  });

  it('admits the probe model unmutated, so each refusal below is caused by its mutation', () => {
    const model: Json = JSON.parse(probe);
    expect(admission.assertDeclarationModel(model)).toEqual(model);
  });

  it('admits each scalar accessor spelling with the exact owner value and scalar text', () => {
    const admitted = assertPublicModel(admission.parseDeclarationModel(probe));
    for (const [name, accessor, scalar] of SCALARS) {
      const { projection } = admitted.api.records[name];
      expect(projection.accessor).toBe(accessor);
      expect(projection.scalar).toBe(scalar);
    }
  });

  for (const { name, mutate } of refusals) {
    it(`refuses ${name} through the generated Equals validators`, () => {
      const model: Json = JSON.parse(probe);
      const before = JSON.stringify(model);
      mutate(model);
      expect(JSON.stringify(model)).not.toBe(before);
      expect(refusal(() => admission.assertDeclarationModel(model))).toBeInstanceOf(TypeGuardError);
      expect(refusal(() => admission.parseDeclarationModel(JSON.stringify(model)))).toBeInstanceOf(TypeGuardError);
    });
  }
});
