import { computed as computedSignal } from '@tldraw/state';

import type { ReadonlyState, StateBusReader, StateKeys } from './types.js';

export type ViewPrimitiveProp = undefined | null | string | number;
export type ViewProps = ViewPrimitiveProp | Readonly<Record<string, ViewPrimitiveProp>>;
export type ViewFunction<SK extends StateKeys, Props extends ViewProps, R> = (
  states: Pick<ReadonlyState, SK>,
  props: Props,
) => R;

/** Compare plain primitive props without sorted keys, tuples, coercion, or serialization. */
export function sameViewProps(left: ViewProps, right: ViewProps): boolean {
  if (Object.is(left, right)) return true;
  if (left === null || right === null || typeof left !== 'object' || typeof right !== 'object') return false;
  let leftCount = 0;
  for (const key in left) {
    if (!Object.hasOwn(left, key)) continue;
    if (!Object.hasOwn(right, key) || !Object.is(left[key], right[key])) return false;
    leftCount += 1;
  }
  let rightCount = 0;
  for (const key in right) {
    if (Object.hasOwn(right, key)) rightCount += 1;
  }
  return leftCount === rightCount;
}

/** Capture only when binding a new view. Caller mutation must not alter an already-bound computation. */
export function captureViewProps<Props extends ViewProps>(props: Props): Props {
  return props !== null && typeof props === 'object' ? { ...props } : props;
}

export function computed<SK extends StateKeys, Props extends ViewProps, R>(
  statebus: StateBusReader,
  viewId: string,
  hook: ViewFunction<SK, Props, R>,
  props: Props,
) {
  // The name is diagnostic, not a cache key. Signal identity belongs to its binding.
  return computedSignal(viewId, () => hook(statebus.state, props));
}
