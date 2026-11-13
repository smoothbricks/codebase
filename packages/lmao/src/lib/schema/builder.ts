/**
 * Schema builder for lmao: arrow-builder's field builder with lmao's own vocabulary on top.
 *
 * - Feature flags: `.default(value).sync()` / `.async()` on the kinds a flag can hold.
 * - `S.unknown()` and `S.object<T>()`: any value in a binary column, encoded as MessagePack where the column is
 *   written out. They are one extension kind (`lmao.msgpack`) defined through arrow-builder's `defineFieldKind`,
 *   with its codec, validation and identity.
 */

import { decode, encode } from '@msgpack/msgpack';
import {
  S as ArrowS,
  defineFieldKind,
  extendField,
  type Field,
  type LazyField,
  lazyField,
  type Schema,
} from '@smoothbricks/arrow-builder';
import type { FeatureFlagDefinition, FlagBuilder, FlagBuilderWithDefault, SchemaBuilder } from './types.js';

/** A field that can also declare a feature flag of its value type. */
function flaggable<Value, F extends Field>(field: F & Schema<Value>): F & FlagBuilder<Value> {
  const schema: F & Schema<Value> & FlagBuilder<Value> = extendField(field, {
    default(defaultValue: Value): FlagBuilderWithDefault<Value> {
      return {
        sync: (): FeatureFlagDefinition<Value, 'sync'> => ({ schema, defaultValue, evaluationType: 'sync' }),
        async: (): FeatureFlagDefinition<Value, 'async'> => ({ schema, defaultValue, evaluationType: 'async' }),
      };
    },
  });
  return schema;
}

/**
 * Any value, as MessagePack in a binary column. The hot path stores the reference; the bytes are made where the
 * column is written out. Functions and symbols inside a value are dropped by the encoder.
 */
const msgpackKind = defineFieldKind<unknown, null, 'lmao.msgpack'>({
  id: 'lmao.msgpack',
  physical: () => ({ type: 'binary' }),
  digest: () => '',
  validate: (_params, value) => ({ ok: true, value }),
  encode: (_params, value) => encode(value),
  decode: (_params, cell) => {
    if (cell instanceof Uint8Array) return decode(cell);
    throw new TypeError(`an lmao.msgpack cell holds bytes, got ${typeof cell}`);
  },
});

function assertObjectField<T extends object>(
  field: LazyField<Field<unknown, 'lmao.msgpack', null>>,
): asserts field is LazyField<Field<T, 'lmao.msgpack', null>> {
  if (field.kind !== 'lmao.msgpack') throw new TypeError('expected an lmao.msgpack field');
}

//#region smoo/lmao!n/schema-string-types
/**
 * Schema builder that wraps arrow-builder's S with feature flag support
 *
 * STRING TYPE SYSTEM (See specs/lmao/01a_trace_schema_system.md):
 * Three distinct string types, each with one meaning:
 * - S.enum(['A', 'B', 'C']) - Known values at compile time (the column is a dictionary of the variants)
 * - S.category() - Repeated values (a dictionary of the stream)
 * - S.text() - Everything else (a plain string column)
 */
export const S: SchemaBuilder = {
  ...ArrowS,
  number: () => flaggable(ArrowS.number()),
  boolean: () => flaggable(ArrowS.boolean()),
  enum: <const T extends readonly string[]>(values: T) => flaggable(ArrowS.enum(values)),
  category: () => flaggable(ArrowS.category()),
  text: () => flaggable(ArrowS.text()),
  unknown: () => lazyField(msgpackKind(null)),
  object: <T extends object>() => {
    const field = lazyField(msgpackKind(null));
    assertObjectField<T>(field);
    return field;
  },
};
//#endregion smoo/lmao!n/schema-string-types
