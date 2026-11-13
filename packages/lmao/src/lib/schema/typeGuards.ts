/**
 * Schema type-guard re-exports.
 *
 * Re-exports the schema introspection guards from arrow-builder so the rest of
 * the package imports them from a single place.
 *
 * NOTE: Do not hand-write `isRecord`/`typeof` guards here. For runtime
 * validation at trust boundaries use Typia (`typia.is<T>()`) or
 * `@smoothbricks/validation`'s shared helpers (e.g. `isRecord`).
 */

export {
  getBinaryEncoder,
  getEnumUtf8,
  getEnumValues,
  isEnumField,
  isField,
  storageLaneOf,
} from '@smoothbricks/arrow-builder';
