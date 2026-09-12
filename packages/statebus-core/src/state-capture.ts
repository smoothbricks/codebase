import type { EncodedState } from './composition.js';

// Internal cold-path seam. The public surface remains typed state/event handles.
export const visitState = Symbol('visit checkpoint entries');
export const trackState = Symbol('track replay checkpoint writes');
export type ResourceKey = string | number | undefined;
export type CapturedEntry = EncodedState['entries'][number];
export type CaptureSink = (id: ResourceKey, entry: CapturedEntry | undefined) => boolean;
export interface StateCapture {
  drain(sink: CaptureSink): boolean;
  dispose(): void;
}

/** Native Map identity: finite numbers first, then strings in UTF-16 code-unit order. */
export function compareResourceKeys(a: ResourceKey, b: ResourceKey): number {
  if (a === b) return 0;
  if (a === undefined) return -1;
  if (b === undefined) return 1;
  if (typeof a === 'number') return typeof b === 'number' ? a - b : -1;
  if (typeof b === 'number') return 1;
  return a < b ? -1 : 1;
}
