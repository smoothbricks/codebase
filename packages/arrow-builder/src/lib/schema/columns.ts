/**
 * A library's field schema, and the algebra that names its columns.
 *
 * A library declares its fields under local names (`defineFields`). Whoever composes libraries names the columns:
 * `prefix` namespaces a library's fields, `mapColumns` renames one, shares one or gives one no column. The result
 * is a value, `MappedFields`: for every field, a column name or none. `composeColumns` joins the mappings of every
 * library of one composition into its columns and refuses what no composer can mean: one column with two types,
 * and one name reached by two fields that nobody mapped there.
 *
 * Nothing here knows what a column is stored in. A tracing library renders a mapping as a remap of its span
 * buffers; an event log renders it as layout rows.
 */

import type { SchemaResult } from './core.js';
import { type Field, isField, typeDigest, typeText } from './field.js';
import type { PhysicalType } from './physical.js';
import { checkPublishedFields, type FieldListViolation, type PublishedFields } from './published.js';

/** A library's fields: local name → field. One name, one type, by construction. */
export type FieldRecord = Readonly<Record<string, Field>>;

/** What `mapColumns` takes: per field, a column name, or `null` for no column. An absent field keeps its column. */
export type ColumnMapping<F> = { readonly [K in keyof F & string]?: string | null };

/** The columns of an unmapped library: every field under its local name. */
export type LocalColumns<F> = { readonly [K in keyof F & string]: K };

/** Columns after `prefix(p)`: every column `p_<column>`; a field without a column stays without. */
export type PrefixedColumns<C, P extends string> = {
  readonly [K in keyof C]: C[K] extends string ? `${P}_${C[K]}` : null;
};

/** Columns after `mapColumns(m)`: `m`'s entry where it has one, the current column otherwise. */
export type MappedColumns<C, M> = {
  readonly [K in keyof C]: K extends keyof M ? (M[K] extends string ? M[K] : M[K] extends null ? null : C[K]) : C[K];
};

/** The columns a mapping contributes, each with its field: column name → field. */
export type ColumnFields<F, C> = {
  readonly [K in keyof F & string as K extends keyof C ? (C[K] extends string ? C[K] : never) : never]: F[K];
};

/** A library's field schema. */
export interface FieldSchema<L extends string = string, F extends FieldRecord = FieldRecord> {
  readonly library: L;
  readonly fields: F;
  /**
   * Each field's number in the library's published field list, or `null` for a library that publishes none (a
   * schema that only traces). A number is a field's durable identity: a rename changes no number.
   */
  readonly numbers: Readonly<Record<string, number>> | null;
  /** Namespace every field: `<p>_<name>`. */
  prefix<P extends string>(p: P): MappedFields<L, F, PrefixedColumns<LocalColumns<F>, P>>;
  /** Name, share or drop single fields; the others keep their local names. */
  mapColumns<const M extends ColumnMapping<F>>(mapping: M): MappedFields<L, F, MappedColumns<LocalColumns<F>, M>>;
  /** This schema with its fields numbered by the library's published list, or why the list does not fit it. */
  numbered(published: PublishedFields): SchemaResult<FieldSchema<L, F>, readonly FieldListViolation[]>;
}

/** A library's fields with their columns named: the composer's side of a schema. */
export interface MappedFields<
  L extends string = string,
  F extends FieldRecord = FieldRecord,
  C = Readonly<Record<string, string | null>>,
> {
  readonly schema: FieldSchema<L, F>;
  /** Field → column name, or `null`: the field has no column. Total over the schema's fields. */
  readonly columns: C;
  /** The fields whose column a `mapColumns` named. Only such fields may share a column. */
  readonly named: ReadonlySet<string>;
  /** Prefix every current column: `.prefix('api').prefix('v1')` gives `v1_api_<name>`. */
  prefix<P extends string>(p: P): MappedFields<L, F, PrefixedColumns<C, P>>;
  /** Override single fields' columns. */
  mapColumns<const M extends ColumnMapping<F>>(mapping: M): MappedFields<L, F, MappedColumns<C, M>>;
}

type Columns = Readonly<Record<string, string | null>>;

function assertName(what: string, name: string): void {
  if (name.length === 0) throw new TypeError(`${what} must not be empty`);
}

function assertColumnsType<C>(columns: unknown, fields: FieldRecord): asserts columns is C {
  if (typeof columns !== 'object' || columns === null) throw new TypeError('a column mapping must be an object');
  for (const name of Object.keys(fields)) {
    if (!(name in columns)) throw new TypeError(`column mapping lost field '${name}'`);
  }
}

function mapped<L extends string, F extends FieldRecord, C>(
  schema: FieldSchema<L, F>,
  current: Columns,
  named: ReadonlySet<string>,
): MappedFields<L, F, C> {
  const columns = Object.freeze({ ...current });
  assertColumnsType<C>(columns, schema.fields);
  return Object.freeze({
    schema,
    columns,
    named,
    prefix<P extends string>(p: P) {
      assertName('a column prefix', p);
      const next: Record<string, string | null> = {};
      for (const [field, column] of Object.entries(current)) next[field] = column === null ? null : `${p}_${column}`;
      return mapped<L, F, PrefixedColumns<C, P>>(schema, next, named);
    },
    mapColumns<const M extends ColumnMapping<F>>(mapping: M) {
      const next: Record<string, string | null> = { ...current };
      const nextNamed = new Set(named);
      for (const [field, column] of Object.entries<string | null | undefined>(mapping)) {
        if (!(field in schema.fields)) {
          throw new TypeError(
            `mapColumns: library '${schema.library}' has no field '${field}' (fields: ${Object.keys(schema.fields).join(', ')})`,
          );
        }
        if (column === undefined) continue;
        if (column === null) {
          nextNamed.delete(field);
        } else {
          assertName(`the column of field '${field}'`, column);
          nextNamed.add(field);
        }
        next[field] = column;
      }
      return mapped<L, F, MappedColumns<C, M>>(schema, next, nextNamed);
    },
  });
}

const NO_NAMED: ReadonlySet<string> = new Set();

function localColumns(fields: FieldRecord): Columns {
  const columns: Record<string, string> = {};
  for (const name of Object.keys(fields)) columns[name] = name;
  return columns;
}

function schemaOf<L extends string, F extends FieldRecord>(
  library: L,
  fields: F,
  numbers: Readonly<Record<string, number>> | null,
): FieldSchema<L, F> {
  const schema: FieldSchema<L, F> = Object.freeze({
    library,
    fields,
    numbers,
    prefix: <P extends string>(p: P) => mapped<L, F, LocalColumns<F>>(schema, localColumns(fields), NO_NAMED).prefix(p),
    mapColumns: <const M extends ColumnMapping<F>>(mapping: M) =>
      mapped<L, F, LocalColumns<F>>(schema, localColumns(fields), NO_NAMED).mapColumns(mapping),
    numbered: (published: PublishedFields) => {
      const checked = checkPublishedFields(schema, published);
      return checked.ok ? { ok: true as const, value: schemaOf(library, fields, checked.value) } : checked;
    },
  });
  return schema;
}

/**
 * Declare a library's fields. `library` names the library wherever a field's identity is written; `fields` maps
 * each local name to its field.
 *
 * @throws TypeError for an empty library name or a value that is not a field node.
 */
export function defineFields<const L extends string, const F extends FieldRecord>(
  library: L,
  fields: F,
): FieldSchema<L, F> {
  assertName('a library name', library);
  for (const [name, field] of Object.entries(fields)) {
    assertName(`a field name of library '${library}'`, name);
    if (!isField(field)) throw new TypeError(`field '${name}' of library '${library}' is not a field node`);
  }
  return schemaOf(library, Object.freeze({ ...fields }), null);
}

// ---------------------------------------------------------------------------------------------------------------
// Composition
// ---------------------------------------------------------------------------------------------------------------

/** One library's part in a composition: its mapping, or the schema itself for local names. */
export type ComposedPart = MappedFields<string, FieldRecord, Columns> | FieldSchema;

/** A field's identity: its library and its number there. The name is display. */
export interface FieldIdentity {
  readonly library: string;
  readonly name: string;
  /** `null` for a library without a published field list. */
  readonly number: number | null;
}

/** A field of a composition: its identity and the part that brought it (`'composer'`: the composer's own schema). */
export interface ComposedField extends FieldIdentity {
  readonly part: number | 'composer';
}

export interface ComposedColumn {
  readonly name: string;
  readonly physical: PhysicalType;
  readonly typeText: string;
  readonly typeDigest: string;
  /** The fields this column holds; two or more make it a shared column. */
  readonly fields: readonly ComposedField[];
}

/** The columns of a composition, sorted by name. */
export interface ComposedColumns {
  readonly columns: readonly ComposedColumn[];
}

export type CompositionRefusal =
  /** Two fields of different types were given one column. */
  | {
      readonly code: 'two-types-one-column';
      readonly column: string;
      readonly fields: readonly [ComposedField, ComposedField];
      readonly types: readonly [string, string];
    }
  /** Two fields reach one column and at least one was not mapped there by `mapColumns`. */
  | {
      readonly code: 'unmapped-shared-name';
      readonly column: string;
      readonly fields: readonly [ComposedField, ComposedField];
    };

export interface ComposeOptions {
  /**
   * The composer's own schema. Its fields are columns under their local names, named by the composer: a library
   * field mapped onto one shares it.
   */
  readonly composer?: FieldSchema;
}

interface Placed {
  readonly field: ComposedField;
  readonly node: Field;
  readonly text: string;
  readonly named: boolean;
}

/**
 * Join the parts of one composition into its columns.
 *
 * A column holds one type. Two fields share a column only when each was mapped there by `mapColumns` (or is the
 * composer's own) and their types are equal; which field a cell holds is then a fact of the row.
 */
export function composeColumns(
  parts: readonly ComposedPart[],
  options: ComposeOptions = {},
): SchemaResult<ComposedColumns, readonly CompositionRefusal[]> {
  const byColumn = new Map<string, Placed[]>();
  const refusals: CompositionRefusal[] = [];

  const place = (
    schema: FieldSchema,
    columns: Columns,
    named: ReadonlySet<string> | 'all',
    part: number | 'composer',
  ) => {
    for (const [name, node] of Object.entries(schema.fields)) {
      const column = columns[name];
      if (column === null || column === undefined) continue;
      const placed: Placed = {
        field: { library: schema.library, name, number: schema.numbers?.[name] ?? null, part },
        node,
        text: typeText(node),
        named: named === 'all' || named.has(name),
      };
      const held = byColumn.get(column);
      if (held === undefined) {
        byColumn.set(column, [placed]);
        continue;
      }
      const first = held[0];
      if (first.text !== placed.text) {
        refusals.push({
          code: 'two-types-one-column',
          column,
          fields: [first.field, placed.field],
          types: [first.text, placed.text],
        });
        continue;
      }
      const unnamed = placed.named ? held.find((other) => !other.named) : first;
      if (unnamed !== undefined) {
        refusals.push({ code: 'unmapped-shared-name', column, fields: [unnamed.field, placed.field] });
        continue;
      }
      held.push(placed);
    }
  };

  if (options.composer !== undefined) place(options.composer, localColumns(options.composer.fields), 'all', 'composer');
  parts.forEach((part, index) => {
    if ('columns' in part) place(part.schema, part.columns, part.named, index);
    else place(part, localColumns(part.fields), NO_NAMED, index);
  });

  if (refusals.length > 0) return { ok: false, error: refusals };
  const columns = [...byColumn.entries()]
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
    .map(
      ([name, held]): ComposedColumn => ({
        name,
        physical: held[0].node.physical,
        typeText: held[0].text,
        typeDigest: typeDigest(held[0].node),
        fields: held.map((placed) => placed.field),
      }),
    );
  return { ok: true, value: { columns } };
}

// ---------------------------------------------------------------------------------------------------------------
// Field identity in a column's metadata
// ---------------------------------------------------------------------------------------------------------------

/** The Arrow field metadata key under which a column names the fields it holds. */
export const FIELDS_METADATA_KEY = 'sb.fields';

/**
 * The value of `sb.fields` for a column: a JSON array, one entry per field the column holds,
 * `{ "lib", "n", "name", "type" }`. Refused for a field without a number: a column that records identity names
 * fields of libraries with a published list only.
 */
export function fieldsMetadata(
  column: ComposedColumn,
): SchemaResult<string, { readonly code: 'unnumbered-field'; readonly field: ComposedField }> {
  const entries: { lib: string; n: number; name: string; type: string }[] = [];
  for (const field of column.fields) {
    if (field.number === null) return { ok: false, error: { code: 'unnumbered-field', field } };
    entries.push({ lib: field.library, n: field.number, name: field.name, type: column.typeDigest });
  }
  return { ok: true, value: JSON.stringify(entries) };
}
