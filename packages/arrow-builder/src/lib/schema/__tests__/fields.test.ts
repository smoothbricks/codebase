import { describe, expect, it } from 'bun:test';
import {
  composeColumns,
  defineFieldKind,
  defineFields,
  fieldsMetadata,
  publishFields,
  S,
  storageLaneOf,
  typeDigest,
  typeText,
  validateField,
} from '../index.js';

//#region smoo/lmao!n/schema-log-schema
describe('a field node', () => {
  it('an_optional_field_keeps_its_kind_and_column', () => {
    const required = S.number();
    const optional = S.optional(required);
    expect(optional.kind).toBe('f64');
    expect(optional.physical).toEqual(required.physical);
    expect(optional.optional).toBe(true);
    expect(storageLaneOf(optional)).toBe('number');
    expect(typeDigest(optional)).toBe(typeDigest(required));
    expect(validateField('n', optional, undefined)).toEqual({ ok: true, value: undefined });
    expect(validateField('n', required, undefined).ok).toBe(false);
  });

  it('an_eager_field_cannot_be_optional', () => {
    expect(() => S.optional(S.number().eager())).toThrow(/eager/);
  });

  it('a_column_type_follows_from_the_declaration_alone', () => {
    expect(S.text().physical).toEqual({ type: 'utf8' });
    expect(S.category().physical).toEqual({ type: 'dictionary', indexBits: 32, dictionary: 'stream' });
    expect(S.enum(['a', 'b']).physical).toEqual({ type: 'dictionary', indexBits: 8, dictionary: 'schema' });
    expect(S.enum(Array.from({ length: 257 }, (_, i) => `v${i}`)).physical).toEqual({
      type: 'dictionary',
      indexBits: 16,
      dictionary: 'schema',
    });
    expect(S.u16().physical).toEqual({ type: 'int', bits: 16, signed: false });
    expect(S.i64().physical).toEqual({ type: 'int', bits: 64, signed: true });
    expect(S.timestamp('ns').physical).toEqual({ type: 'timestamp', unit: 'ns', timezone: 'UTC' });
    expect(S.fixedBinary(16).physical).toEqual({ type: 'fixedBinary', width: 16 });
  });

  it('an_enum_cell_is_the_declared_ordinal', () => {
    const method = S.enum(['GET', 'POST', 'PUT']);
    expect(method.codec.encode(method.params, 'PUT')).toBe(2);
    expect(method.codec.decode(method.params, 1)).toBe('POST');
    expect(validateField('method', method, 'PATCH')).toEqual({
      ok: false,
      error: { path: 'method', expected: 'one of [GET, POST, PUT]', observed: '"PATCH"' },
    });
  });

  it('an_integer_refuses_a_value_outside_its_width', () => {
    expect(validateField('n', S.u8(), 255).ok).toBe(true);
    expect(validateField('n', S.u8(), 256).ok).toBe(false);
    expect(validateField('n', S.i8(), -129).ok).toBe(false);
    expect(validateField('n', S.u64(), 2n ** 64n - 1n).ok).toBe(true);
    expect(validateField('n', S.u64(), -1n).ok).toBe(false);
    expect(validateField('n', S.i32(), 1.5).ok).toBe(false);
  });

  it('a_type_digest_ignores_names_hints_and_optional_and_follows_the_params', () => {
    expect(typeText(S.timestamp('ns'))).toBe('timestamp(ns)');
    expect(typeText(S.enum(['a', 'b']))).toBe('enum(["a","b"])');
    expect(typeDigest(S.text())).toMatch(/^[0-9a-f]{32}$/);
    expect(typeDigest(S.text().mask('hash'))).toBe(typeDigest(S.text()));
    expect(typeDigest(S.category().eager())).toBe(typeDigest(S.category()));
    expect(typeDigest(S.timestamp('ns'))).not.toBe(typeDigest(S.timestamp('us')));
    expect(typeDigest(S.enum(['a', 'b']))).not.toBe(typeDigest(S.enum(['a', 'b', 'c'])));
    expect(typeDigest(S.text())).not.toBe(typeDigest(S.category()));
  });

  it('a_node_is_frozen_plain_data', () => {
    const field = S.enum(['a']);
    expect(Object.isFrozen(field)).toBe(true);
    expect(Object.keys(field).sort()).toEqual(['hints', 'kind', 'optional', 'params', 'physical']);
  });
});

describe('an extension kind', () => {
  // A private kind of an outside package: a key with a grammar, stored as a plain string column.
  type Key = `${string}/${string}`;
  const isKey = (value: unknown): value is Key => typeof value === 'string' && /^[a-z]+\/[a-z0-9]+$/.test(value);
  const key = defineFieldKind<Key, { readonly name: string }, 'test.key'>({
    id: 'test.key',
    physical: () => ({ type: 'utf8' }),
    digest: ({ name }) => name,
    validate: (_params, value) =>
      isKey(value)
        ? { ok: true, value }
        : { ok: false, error: { path: '', expected: 'a key <family>/<id>', observed: String(value) } },
    encode: (_params, value) => value,
    decode: (_params, cell) => {
      if (isKey(cell)) return cell;
      throw new TypeError('not a key cell');
    },
  });

  it('validates_encodes_and_decodes_through_its_own_functions', () => {
    const planKey = key({ name: 'plan' });
    expect(planKey.kind).toBe('test.key');
    expect(planKey.physical).toEqual({ type: 'utf8' });
    expect(validateField('plan', planKey, 'plan/a1')).toEqual({ ok: true, value: 'plan/a1' });
    expect(validateField('plan', planKey, 'nope')).toEqual({
      ok: false,
      error: { path: 'plan', expected: 'a key <family>/<id>', observed: 'nope' },
    });
    expect(planKey.codec.decode(planKey.params, planKey.codec.encode(planKey.params, 'plan/a1'))).toBe('plan/a1');
  });

  it('has_its_own_identity', () => {
    expect(typeText(key({ name: 'plan' }))).toBe('test.key(plan)');
    expect(typeDigest(key({ name: 'plan' }))).not.toBe(typeDigest(key({ name: 'unit' })));
    expect(typeDigest(key({ name: 'plan' }))).not.toBe(typeDigest(S.text()));
  });

  it('has_no_buffer_lane_unless_its_column_is_binary', () => {
    expect(storageLaneOf(key({ name: 'plan' }))).toBeUndefined();
  });
});
//#endregion smoo/lmao!n/schema-log-schema

//#region smoo/lmao!n/library-integration-column-mapping
describe('the column mapping of a library', () => {
  const http = defineFields('http', { status: S.number(), method: S.enum(['GET', 'POST']), debug: S.text() });

  it('prefix_namespaces_every_field', () => {
    expect(http.prefix('http').columns).toEqual({ status: 'http_status', method: 'http_method', debug: 'http_debug' });
  });

  it('a_later_prefix_prefixes_the_earlier_names', () => {
    expect(http.prefix('api').prefix('v1').columns.status).toBe('v1_api_status');
  });

  it('mapColumns_renames_shares_and_drops_and_keeps_the_rest', () => {
    const mapped = http.mapColumns({ status: 'code', debug: null });
    expect(mapped.columns).toEqual({ status: 'code', method: 'method', debug: null });
    expect([...mapped.named]).toEqual(['status']);
    expect(mapped.prefix('x').columns).toEqual({ status: 'x_code', method: 'x_method', debug: null });
  });

  it('a_field_has_exactly_one_place', () => {
    const composed = composeColumns([http.mapColumns({ debug: null }).prefix('h')]);
    if (!composed.ok) throw new Error('expected a composition');
    expect(composed.value.columns.map((column) => column.name)).toEqual(['h_method', 'h_status']);
    expect(composed.value.columns.every((column) => column.fields.length === 1)).toBe(true);
  });
});
//#endregion smoo/lmao!n/library-integration-column-mapping

//#region smoo/lmao!n/library-integration-shared-columns
describe('a composition of libraries', () => {
  const pg = defineFields('pg', { query: S.text(), rows: S.number() });
  const mysql = defineFields('mysql', { query: S.text(), rows: S.number() });

  it('two_fields_mapped_to_one_column_of_one_type_share_it', () => {
    const composed = composeColumns([
      pg.mapColumns({ query: 'query', rows: 'pg_rows' }),
      mysql.mapColumns({ query: 'query', rows: 'mysql_rows' }),
    ]);
    if (!composed.ok) throw new Error('expected a composition');
    const query = composed.value.columns.find((column) => column.name === 'query');
    expect(query?.fields.map((field) => field.library)).toEqual(['pg', 'mysql']);
    expect(query?.typeDigest).toBe(typeDigest(S.text()));
  });

  it('the_composers_own_field_counts_as_mapped', () => {
    const app = defineFields('app', { query: S.text() });
    const composed = composeColumns([pg.mapColumns({ query: 'query' })], { composer: app });
    if (!composed.ok) throw new Error('expected a composition');
    const query = composed.value.columns.find((column) => column.name === 'query');
    expect(query?.fields.map((field) => field.part)).toEqual(['composer', 0]);
  });

  it('one_column_never_holds_two_types', () => {
    const process = defineFields('process', { status: S.text() });
    const http = defineFields('http', { status: S.number() });
    const composed = composeColumns([http.mapColumns({ status: 's' }), process.mapColumns({ status: 's' })]);
    expect(composed).toEqual({
      ok: false,
      error: [
        {
          code: 'two-types-one-column',
          column: 's',
          fields: [
            { library: 'http', name: 'status', number: null, part: 0 },
            { library: 'process', name: 'status', number: null, part: 1 },
          ],
          types: ['f64()', 'text()'],
        },
      ],
    });
  });

  it('a_name_two_fields_reach_unmapped_is_refused', () => {
    const composed = composeColumns([pg, mysql.mapColumns({ query: 'query', rows: 'mysql_rows' })]);
    expect(composed.ok).toBe(false);
    if (composed.ok) return;
    expect(composed.error.map((refusal) => [refusal.code, refusal.column])).toEqual([
      ['unmapped-shared-name', 'query'],
    ]);
  });

  it('prefixed_libraries_never_meet', () => {
    expect(composeColumns([pg.prefix('pg'), mysql.prefix('mysql')]).ok).toBe(true);
  });
});
//#endregion smoo/lmao!n/library-integration-shared-columns

//#region smoo/lmao!n/schema-field-numbers
describe('a published field list', () => {
  const v1 = defineFields('swarm', { plan_id: S.text(), attempt: S.u32() });

  it('numbers_fields_by_position_and_a_rename_keeps_the_number', () => {
    const first = publishFields(v1, null);
    if (!first.ok) throw new Error('expected a first list');
    expect(first.value.fields.map((entry) => entry.name)).toEqual(['plan_id', 'attempt']);
    const renamed = {
      ...first.value,
      fields: first.value.fields.map((e) => (e.name === 'attempt' ? { ...e, name: 'try' } : e)),
    };
    const v2 = defineFields('swarm', { try: S.u32(), plan_id: S.text() }).numbered(renamed);
    if (!v2.ok) throw new Error('expected numbers');
    expect(v2.value.numbers).toEqual({ plan_id: 0, try: 1 });
  });

  it('a_field_no_longer_declared_is_retired_and_its_number_is_never_reused', () => {
    const first = publishFields(v1, null);
    if (!first.ok) throw new Error('expected a first list');
    const next = publishFields(defineFields('swarm', { plan_id: S.text(), unit_id: S.text() }), first.value);
    if (!next.ok) throw new Error('expected an appended list');
    expect(next.value.fields.map((e) => [e.name, e.retired === true])).toEqual([
      ['plan_id', false],
      ['attempt', true],
      ['unit_id', false],
    ]);
    const again = publishFields(v1, next.value);
    expect(again.ok).toBe(false);
    if (!again.ok) expect(again.error.map((v) => v.code)).toContain('retired-declared');
  });

  it('the_check_refuses_an_unpublished_field_and_a_changed_type_and_rewrites_nothing', () => {
    const first = publishFields(v1, null);
    if (!first.ok) throw new Error('expected a first list');
    const changed = defineFields('swarm', { plan_id: S.category(), attempt: S.u32(), extra: S.text() }).numbered(
      first.value,
    );
    expect(changed.ok).toBe(false);
    if (!changed.ok) expect(changed.error.map((v) => v.code).sort()).toEqual(['type-changed', 'unpublished-field']);
  });

  it('a_column_names_its_fields_only_when_they_are_numbered', () => {
    const first = publishFields(v1, null);
    if (!first.ok) throw new Error('expected a first list');
    const numbered = v1.numbered(first.value);
    if (!numbered.ok) throw new Error('expected numbers');
    const composed = composeColumns([numbered.value.prefix('swarm')]);
    if (!composed.ok) throw new Error('expected a composition');
    const column = composed.value.columns.find((entry) => entry.name === 'swarm_attempt');
    if (column === undefined) throw new Error('expected swarm_attempt');
    expect(fieldsMetadata(column)).toEqual({
      ok: true,
      value: JSON.stringify([{ lib: 'swarm', n: 1, name: 'attempt', type: typeDigest(S.u32()) }]),
    });
    const unnumbered = composeColumns([v1]);
    if (!unnumbered.ok) throw new Error('expected a composition');
    expect(fieldsMetadata(unnumbered.value.columns[0]).ok).toBe(false);
  });
});
//#endregion smoo/lmao!n/schema-field-numbers
