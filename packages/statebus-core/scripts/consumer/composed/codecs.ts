import { Err, type ErrResult, Ok, type OkResult, type Result } from '@smoothbricks/lmao';
import type { EffectOutcome, RecordedScenario, ValueCodec } from '@smoothbricks/statebus-core';
import type { LoaderEvent, LoadState } from '@smoothbricks/statebus-data-loader';
import typia from 'typia';

declare const shelfBrand: unique symbol;
declare const requestBrand: unique symbol;
export type ShelfId = `shelf:${string}` & { readonly [shelfBrand]: true };
export type RequestId = `request:${string}` & { readonly [requestBrand]: true };
const assertShelfId = typia.createAssert<ShelfId>();
const assertRequestId = typia.createAssert<RequestId>();
export function shelfId(value: string): ShelfId {
  return assertShelfId(value);
}
export function requestId(value: string): RequestId {
  return assertRequestId(value);
}

// Validators are generated once by the normal ttsc/Typia compiler. They validate the actual
// public contracts; no mirrored loader schema, hand-written property walker or consumer cast.
function codec<T>(schema: string, decode: (value: unknown) => T): ValueCodec<T> {
  return { schema, version: 1, encode: (value) => structuredClone(value), decode };
}
export const numberCodec = codec('inventory.number', typia.createAssert<number>());
export const shelfCodec = codec('inventory.shelf-id', assertShelfId);
export const requestCodec = codec('inventory.request-id', typia.createAssert<RequestId | null>());
export const loadStateCodec = codec('inventory.load-state', typia.createAssert<LoadState<number, string>>());
export const loadEventCodec = codec('inventory.load-event', typia.createAssert<LoaderEvent<number, string>>());

export interface Adjust {
  readonly shelfId: ShelfId;
  readonly requestId: RequestId;
  readonly add: number;
}
export interface AdjustPlan {
  readonly shelfId: ShelfId;
  readonly requestId: RequestId;
  readonly next: number;
}
export type AdjustResult =
  | { readonly kind: 'ok'; readonly shelfId: ShelfId; readonly requestId: RequestId; readonly value: number }
  | { readonly kind: 'err'; readonly shelfId: ShelfId; readonly requestId: RequestId; readonly error: string };
export const adjustCodec = codec('inventory.adjust', typia.createAssert<Adjust>());
export const adjustResultCodec = codec('inventory.adjusted', typia.createAssert<AdjustResult>());

// A captured result carries public result data, never live span ownership or class methods.
type DetachedResult = Pick<OkResult<number>, 'success' | 'value'> | Pick<ErrResult<string>, 'success' | 'error'>;
const assertOutcome = typia.createAssert<EffectOutcome<AdjustPlan, DetachedResult>>();
export const outcomeCodec: ValueCodec<EffectOutcome<AdjustPlan, Result<number, string>>> = {
  schema: 'inventory.adjust-outcome',
  version: 1,
  encode: ({ plan, outcome }) => ({
    plan: { ...plan },
    outcome: outcome.success ? { success: true, value: outcome.value } : { success: false, error: outcome.error },
  }),
  decode(value) {
    const { plan, outcome } = assertOutcome(value);
    return { plan, outcome: outcome.success ? new Ok(outcome.value) : new Err(outcome.error) };
  },
};
// Storage/import boundary: validate the envelope here, then each declaration's codec validates its value.
export const parseScenario = typia.json.createAssertParse<RecordedScenario>();
