export interface TraceChunkEnvelopeInput {
  readonly file_ref: string;
  readonly chunk_ref: string;
  readonly row_count: number;
  readonly started_at_ms: number;
  readonly ended_at_ms: number;
  readonly partition_keys?: readonly string[];
  readonly metadata?: Record<string, unknown>;
}

export interface TraceChunkEnvelope {
  readonly chunk_id: string;
  readonly file_ref: string;
  readonly chunk_ref: string;
  readonly row_count: number;
  readonly started_at_ms: number;
  readonly ended_at_ms: number;
  readonly partition_keys: readonly string[];
  readonly metadata?: Record<string, unknown>;
}

export function buildTraceChunkEnvelope(input: TraceChunkEnvelopeInput): TraceChunkEnvelope {
  const canonical = {
    chunk_ref: input.chunk_ref,
    ended_at_ms: input.ended_at_ms,
    file_ref: input.file_ref,
    metadata: input.metadata ?? null,
    partition_keys: [...(input.partition_keys ?? [])].sort(),
    row_count: input.row_count,
    started_at_ms: input.started_at_ms,
  };

  const chunk_id = `chunk_${hashDeterministic(canonical)}`;
  return {
    chunk_id,
    file_ref: input.file_ref,
    chunk_ref: input.chunk_ref,
    row_count: input.row_count,
    started_at_ms: input.started_at_ms,
    ended_at_ms: input.ended_at_ms,
    partition_keys: canonical.partition_keys,
    metadata: input.metadata,
  };
}

function hashDeterministic(value: unknown): string {
  return fnv1a64(stableSerialize(value));
}

function stableSerialize(value: unknown): string {
  if (value === null) return 'null';
  const t = typeof value;
  if (t === 'number' || t === 'boolean') return JSON.stringify(value);
  if (t === 'string') return JSON.stringify(value);
  if (t === 'bigint') {
    const bigintValue = value as bigint;
    return `"${bigintValue.toString()}n"`;
  }
  if (Array.isArray(value)) {
    return `[${value.map((item) => stableSerialize(item)).join(',')}]`;
  }
  if (t === 'object') {
    const obj = value as Record<string, unknown>;
    const keys = Object.keys(obj).sort();
    return `{${keys.map((k) => `${JSON.stringify(k)}:${stableSerialize(obj[k])}`).join(',')}}`;
  }
  return JSON.stringify(String(value));
}

function fnv1a64(input: string): string {
  let hash = 0xcbf29ce484222325n;
  const prime = 0x100000001b3n;
  const mod = 0xffffffffffffffffn;

  for (let i = 0; i < input.length; i++) {
    hash ^= BigInt(input.charCodeAt(i));
    hash = (hash * prime) & mod;
  }

  return hash.toString(16).padStart(16, '0');
}
