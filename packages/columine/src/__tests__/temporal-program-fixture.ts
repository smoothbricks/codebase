/** Raw fixture encoding of columine/01 §Temporal program table, with no stored expiry entries. */
export function rowClockTable(column: number): Uint8Array {
  const bytes = new Uint8Array(16);
  const view = new DataView(bytes.buffer);
  view.setUint32(0, 0x3150_4d54, true); // TMP1
  view.setUint32(4, 8, true); // clock column and zero entry count
  view.setUint32(8, column, true);
  view.setUint32(12, 0, true);
  return bytes;
}

/** Manual-eviction fixtures author this clock so only their explicit evictExpired calls advance expiry. */
export const MANUAL_EVICTION_CLOCK_MICROS = -(1n << 63n);
