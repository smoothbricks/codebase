/**
 * A library's published field list: the append-only order that gives every field its number.
 *
 * A field's number is its position in the list. An entry is never removed, reordered or re-typed: a field no
 * longer declared is marked `retired` and its name is never declared again. A rename edits `name` in place and
 * changes no number. The list is plain data; where it is kept (a tracked file beside the package, a manifest of
 * the consumer's own) is the library's.
 */

import type { SchemaResult } from './core.js';
import { type Field, typeDigest } from './field.js';

export interface PublishedField {
  readonly name: string;
  /** The field's type digest (`typeDigest`). */
  readonly type: string;
  readonly retired?: true;
}

export interface PublishedFields {
  readonly library: string;
  readonly fields: readonly PublishedField[];
}

export type FieldListViolation =
  | { readonly code: 'library-mismatch'; readonly declared: string; readonly published: string }
  | { readonly code: 'duplicate-name'; readonly name: string; readonly numbers: readonly [number, number] }
  /** A declared field the list does not hold: the list was not written after the field was added. */
  | { readonly code: 'unpublished-field'; readonly name: string }
  /** A declared field whose type is not the published one: a type change is a new name. */
  | {
      readonly code: 'type-changed';
      readonly name: string;
      readonly number: number;
      readonly declared: string;
      readonly published: string;
    }
  /** A declared field whose name the list retired: a retired name is never declared again. */
  | { readonly code: 'retired-declared'; readonly name: string; readonly number: number }
  /** A listed field no longer declared and not marked retired. */
  | { readonly code: 'undeclared-unretired'; readonly name: string; readonly number: number };

interface Declared {
  readonly library: string;
  readonly fields: Readonly<Record<string, Field>>;
}

/**
 * Check a library's declared fields against its published list and give each its number. The check never
 * rewrites the list: every difference is a violation.
 */
export function checkPublishedFields(
  schema: Declared,
  published: PublishedFields,
): SchemaResult<Readonly<Record<string, number>>, readonly FieldListViolation[]> {
  const violations: FieldListViolation[] = [];
  if (published.library !== schema.library) {
    violations.push({ code: 'library-mismatch', declared: schema.library, published: published.library });
  }
  const numberOf = new Map<string, number>();
  published.fields.forEach((entry, number) => {
    const earlier = numberOf.get(entry.name);
    if (earlier !== undefined)
      violations.push({ code: 'duplicate-name', name: entry.name, numbers: [earlier, number] });
    else numberOf.set(entry.name, number);
    if (entry.retired !== true && !(entry.name in schema.fields)) {
      violations.push({ code: 'undeclared-unretired', name: entry.name, number });
    }
  });
  const numbers: Record<string, number> = {};
  for (const [name, field] of Object.entries(schema.fields)) {
    const number = numberOf.get(name);
    if (number === undefined) {
      violations.push({ code: 'unpublished-field', name });
      continue;
    }
    const entry = published.fields[number];
    const declared = typeDigest(field);
    if (entry.retired === true) violations.push({ code: 'retired-declared', name, number });
    else if (entry.type !== declared)
      violations.push({ code: 'type-changed', name, number, declared, published: entry.type });
    numbers[name] = number;
  }
  return violations.length > 0 ? { ok: false, error: violations } : { ok: true, value: Object.freeze(numbers) };
}

/**
 * The published list after the author's run: fields declared for the first time appended in declaration order,
 * fields no longer declared marked retired. Refuses what no append can express (a changed type, a retired name
 * declared again, a list of another library); a first list starts from `null`.
 */
export function publishFields(
  schema: Declared,
  published: PublishedFields | null,
): SchemaResult<PublishedFields, readonly FieldListViolation[]> {
  const base = published ?? { library: schema.library, fields: [] };
  const fields: PublishedField[] = base.fields.map((entry) =>
    entry.retired === true || entry.name in schema.fields ? entry : { ...entry, retired: true as const },
  );
  const listed = new Set(fields.map((entry) => entry.name));
  for (const [name, field] of Object.entries(schema.fields)) {
    if (!listed.has(name)) fields.push({ name, type: typeDigest(field) });
  }
  const next: PublishedFields = { library: base.library, fields };
  const checked = checkPublishedFields(schema, next);
  return checked.ok ? { ok: true, value: next } : checked;
}
