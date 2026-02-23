export type PartitionCardinality = 'single' | 'mixed' | 'unknown';

export interface PartitionSplit<T> {
  readonly partition: string;
  readonly selector: string;
  readonly row_indexes: readonly number[];
  readonly rows: readonly T[];
}

export function inspectPartitionCardinality<T>(
  rows: readonly T[],
  partitionOf: (row: T) => string | null | undefined,
): PartitionCardinality {
  if (rows.length === 0) {
    return 'unknown';
  }

  const partitions = new Set<string>();
  for (const row of rows) {
    const partition = partitionOf(row);
    if (partition === undefined || partition === null || partition.length === 0) {
      return 'unknown';
    }
    partitions.add(partition);
    if (partitions.size > 1) {
      return 'mixed';
    }
  }

  return partitions.size === 1 ? 'single' : 'unknown';
}

export function splitChunkByPartition<T>(
  rows: readonly T[],
  partitionOf: (row: T) => string | null | undefined,
  partitionField = 'group',
): readonly PartitionSplit<T>[] {
  const grouped = new Map<string, { indexes: number[]; rows: T[] }>();

  for (let index = 0; index < rows.length; index++) {
    const row = rows[index];
    const partition = partitionOf(row);
    if (partition === undefined || partition === null || partition.length === 0) {
      continue;
    }

    const group = grouped.get(partition);
    if (group) {
      group.indexes.push(index);
      group.rows.push(row);
      continue;
    }

    grouped.set(partition, { indexes: [index], rows: [row] });
  }

  const keys = [...grouped.keys()].sort();
  return keys.map((partition) => {
    const group = grouped.get(partition);
    if (!group) {
      throw new Error(`invariant throw: missing partition group for ${partition}`);
    }
    return {
      partition,
      selector: `${partitionField} == ${JSON.stringify(partition)}`,
      row_indexes: group.indexes,
      rows: group.rows,
    };
  });
}
