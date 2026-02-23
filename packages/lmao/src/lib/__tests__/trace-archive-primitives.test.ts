import { describe, expect, it } from 'bun:test';
import { compactTraceChunks } from '../archive/chunkCompaction.js';
import { buildTraceChunkEnvelope } from '../archive/chunkEnvelope.js';
import { inspectPartitionCardinality, splitChunkByPartition } from '../archive/chunkRouting.js';
import { extractChunkStats } from '../archive/chunkStats.js';

describe('trace archive primitives', () => {
  it('buildTraceChunkEnvelope produces stable chunk_id', () => {
    const a = buildTraceChunkEnvelope({
      file_ref: 's3://bucket/trace.arrow',
      chunk_ref: 'chunk-001',
      row_count: 4,
      started_at_ms: 100,
      ended_at_ms: 240,
      partition_keys: ['group_b', 'group_a'],
      metadata: { z: 1, a: 'x' },
    });

    const b = buildTraceChunkEnvelope({
      file_ref: 's3://bucket/trace.arrow',
      chunk_ref: 'chunk-001',
      row_count: 4,
      started_at_ms: 100,
      ended_at_ms: 240,
      partition_keys: ['group_a', 'group_b'],
      metadata: { a: 'x', z: 1 },
    });

    expect(a.chunk_id).toBe(b.chunk_id);
  });

  it('inspectPartitionCardinality returns single, mixed, unknown', () => {
    const singleRows = [{ group: 'g1' }, { group: 'g1' }];
    const mixedRows = [{ group: 'g1' }, { group: 'g2' }];
    const unknownRows = [{ group: 'g1' }, { group: '' }];

    expect(inspectPartitionCardinality(singleRows, (row) => row.group)).toBe('single');
    expect(inspectPartitionCardinality(mixedRows, (row) => row.group)).toBe('mixed');
    expect(inspectPartitionCardinality(unknownRows, (row) => row.group)).toBe('unknown');
  });

  it('splitChunkByPartition emits deterministic selectors', () => {
    const rows = [
      { id: 1, group: 'g2' },
      { id: 2, group: 'g1' },
      { id: 3, group: 'g2' },
      { id: 4, group: 'g1' },
    ];

    const splits = splitChunkByPartition(rows, (row) => row.group, 'listener_group');

    expect(splits.map((split) => split.partition)).toEqual(['g1', 'g2']);
    expect(splits.map((split) => split.selector)).toEqual(['listener_group == "g1"', 'listener_group == "g2"']);
    expect(splits[0].row_indexes).toEqual([1, 3]);
    expect(splits[1].row_indexes).toEqual([0, 2]);
  });

  it('compactTraceChunks is idempotent for source order', () => {
    const c1 = buildTraceChunkEnvelope({
      file_ref: 'f',
      chunk_ref: 'a',
      row_count: 2,
      started_at_ms: 10,
      ended_at_ms: 20,
    });
    const c2 = buildTraceChunkEnvelope({
      file_ref: 'f',
      chunk_ref: 'b',
      row_count: 3,
      started_at_ms: 21,
      ended_at_ms: 40,
    });

    const x = compactTraceChunks([c1, c2], { started_at_ms: 0, ended_at_ms: 100 });
    const y = compactTraceChunks([c2, c1], { started_at_ms: 0, ended_at_ms: 100 });

    expect(x.source_set_hash).toBe(y.source_set_hash);
    expect(x.compaction_id).toBe(y.compaction_id);
    expect(x.total_rows).toBe(5);
  });

  it('extractChunkStats reports chunk level metrics', () => {
    const envelope = buildTraceChunkEnvelope({
      file_ref: 'f',
      chunk_ref: 'x',
      row_count: 7,
      started_at_ms: 100,
      ended_at_ms: 190,
    });

    const splits = splitChunkByPartition([{ group: 'g1' }, { group: 'g1' }, { group: 'g2' }], (row) => row.group);

    const stats = extractChunkStats(envelope, splits);
    expect(stats.chunk_id).toBe(envelope.chunk_id);
    expect(stats.partition_count).toBe(2);
    expect(stats.duration_ms).toBe(90);
  });
});
