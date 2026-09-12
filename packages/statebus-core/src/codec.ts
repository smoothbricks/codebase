import { CaptureError } from './capture.js';
import type { ValueCodec } from './composition.js';

export interface CodecMigration<T> {
  readonly schema: string;
  readonly version: number;
  readonly decode: (value: unknown) => T;
}
export interface CodecValue {
  readonly schema: string;
  readonly version: number;
  readonly value: unknown;
}

/** A pure, typed upgrade. Previous validators run before upgrade; current validators run afterwards. */
export function evolveCodec<Previous, Current>(
  current: ValueCodec<Current>,
  previous: ValueCodec<Previous>,
  upgrade: (value: Previous) => NoInfer<Current>,
): ValueCodec<Current> {
  if (
    !Number.isSafeInteger(current.version) ||
    current.version < 1 ||
    !current.schema ||
    !Number.isSafeInteger(previous.version) ||
    previous.version < 1 ||
    !previous.schema ||
    (current.schema === previous.schema && previous.version >= current.version)
  )
    throw new Error('Codec migration must advance a valid schema version.');
  const migrate =
    (decode: (value: unknown) => Previous) =>
    (value: unknown): Current =>
      current.decode(current.encode(upgrade(decode(value))));
  const migrations: CodecMigration<Current>[] = [
    ...(current.migrations ?? []),
    { schema: previous.schema, version: previous.version, decode: migrate((value) => previous.decode(value)) },
    ...(previous.migrations ?? []).map((entry) => ({ ...entry, decode: migrate(entry.decode) })),
  ];
  const identities = new Map<string, Set<number>>();
  for (const migration of migrations) {
    let versions = identities.get(migration.schema);
    if (!versions) {
      versions = new Set();
      identities.set(migration.schema, versions);
    }
    if (versions.has(migration.version)) throw new Error('Duplicate codec migration source.');
    versions.add(migration.version);
    Object.freeze(migration);
  }
  return Object.freeze({ ...current, migrations: Object.freeze(migrations) });
}

export function decodeCodec<T>(codec: ValueCodec<T> | undefined, encoded: CodecValue): T {
  if (codec && Number.isSafeInteger(encoded.version) && encoded.version > 0) {
    if (encoded.schema === codec.schema && encoded.version === codec.version) return codec.decode(encoded.value);
    for (const migration of codec.migrations ?? [])
      if (migration.schema === encoded.schema && migration.version === encoded.version)
        return migration.decode(encoded.value);
  }
  throw new CaptureError({
    code: 'schema',
    boundary: 'codec version',
    schema: encoded.schema,
    fromVersion: encoded.version,
    toVersion: codec?.version,
  });
}

export function supportsCodec<T>(
  codec: ValueCodec<T> | undefined,
  source: { readonly schema?: string; readonly version?: number },
): boolean {
  return (
    !!codec &&
    Number.isSafeInteger(source.version) &&
    (source.version ?? 0) > 0 &&
    ((codec.schema === source.schema && codec.version === source.version) ||
      (codec.migrations?.some(
        (migration) => migration.schema === source.schema && migration.version === source.version,
      ) ??
        false))
  );
}
