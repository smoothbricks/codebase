/// <reference types="bun" />
/// <reference types="node" />

import { afterAll, beforeAll, describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { cpSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import type { DeclarationModel } from '@smoothbricks/cowshed/declaration-ir';
import typia, { TypeGuardError } from 'typia';

const assertPublicModel = typia.createAssertEquals<DeclarationModel>();

/**
 * The owner's public declaration IR, admitted as JSON. The validators are compiled from the owner CLI's own
 * `ir-types` and `ir-validators` output, so admission is exactly as strict as the generated `*Equals` types: an
 * extra key, a missing key or an unknown argument kind is refused by Typia, never filtered by hand.
 *
 * The real cowshed-core model is admitted unchanged. Refusals run on a scratch model whose probe declaration
 * guarantees the nodes being broken (a reference, a `from` attribute and a typed argument) exist, so each refusal
 * is caused by its own mutation and by nothing else.
 */

const sourceDirectory = dirname(fileURLToPath(import.meta.url));
const project = fileURLToPath(new URL('..', import.meta.url));
const generator = fileURLToPath(new URL('../.cache/api-generator/cowshed-api-gen', import.meta.url));

/** One declaration carrying every node the refusals break. The mutations address it by name. */
const PROBE = `#[derive(Serialize)]
#[serde(rename_all = "camelCase", from = "AdmissionSource")]
pub struct AdmissionProbe<'a> {
    pub label: &'a str,
    pub bounded: Option<Vec<u8>>,
    pub token: Token<'static, 32>,
}
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

const refusals: readonly Refusal[] = [
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
    scratch = mkdtempSync(join(sourceDirectory, '.ir-model-admission-'));
    const core = join(scratch, 'crates/cowshed-core/src');
    cpSync(join(project, 'crates/cowshed-core/src'), core, { recursive: true });
    mkdirSync(join(scratch, 'crates/cowshed-gateway-types/src'), { recursive: true });
    cpSync(
      join(project, 'crates/cowshed-gateway-types/src/status.rs'),
      join(scratch, 'crates/cowshed-gateway-types/src/status.rs'),
    );
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
