import type { Table } from '@uwdata/flechette';

export interface ArrowLease {
  readonly table: Table;
  readonly released: boolean;
  release(): void;
  [Symbol.dispose](): void;
}

/** Internal constructor for a lease over borrowed Arrow source chunks. */
export function createArrowLease(
  table: Table,
  pinned: readonly unknown[],
  releases: readonly (() => void)[],
): ArrowLease {
  let released = false;
  // Retained by the closures for the full lease lifetime. Clearing this array on
  // release makes the backing epochs and dictionary generation collectible.
  const retained = [...pinned];
  return Object.freeze({
    table,
    get released() {
      return released;
    },
    release(): void {
      if (released) return;
      released = true;
      let failure: unknown;
      for (let index = releases.length - 1; index >= 0; index--) {
        try {
          releases[index]();
        } catch (error) {
          failure ??= error;
        }
      }
      retained.length = 0;
      if (failure !== undefined) throw failure;
    },
    [Symbol.dispose](): void {
      this.release();
    },
  });
}
