import { Err, type Op, type OpContext, type Result, type SpanContext } from '@smoothbricks/lmao';
import {
  bindEffect,
  type ComposedRuntime,
  defineCapability,
  defineEffect,
  defineLibrary,
  type EffectDefinition,
} from '@smoothbricks/statebus-core';
import {
  initialLoadState,
  type LoaderEvent,
  type LoadState,
  reduceComposedLoader,
} from '@smoothbricks/statebus-data-loader';
import {
  type Adjust,
  type AdjustPlan,
  type AdjustResult,
  adjustCodec,
  adjustResultCodec,
  loadEventCodec,
  loadStateCodec,
  numberCodec,
  outcomeCodec,
  type RequestId,
  requestCodec,
  type ShelfId,
  shelfCodec,
  shelfId,
} from './codecs.js';

export const canAdjust = defineCapability<boolean>('inventory.can-adjust');
export const inventoryLibrary = defineLibrary({
  name: 'inventory',
  requires: [canAdjust],
  setup(scope) {
    const allowed = scope.require(canAdjust);
    const selected = scope.scalar('selected', () => shelfId('shelf:a'), { codec: shelfCodec });
    const inventory = scope.keyed('inventory', (_id: ShelfId) => initialLoadState<number, string>(), {
      codec: loadStateCodec,
      idCodec: shelfCodec,
    });
    const incoming = scope.keyed('incoming', (_id: ShelfId) => initialLoadState<number, string>(), {
      codec: loadStateCodec,
      idCodec: shelfCodec,
    });
    const pending = scope.keyed('pending', (_id: ShelfId): RequestId | null => null, {
      codec: requestCodec,
      idCodec: shelfCodec,
    });
    const completed = scope.keyed('completed', (_id: ShelfId): RequestId | null => null, {
      codec: requestCodec,
      idCodec: shelfCodec,
    });
    const selectionChanged = scope.event<ShelfId>('selectionChanged', { codec: shelfCodec });
    const load = scope.event<LoaderEvent<number, string>>('load', { codec: loadEventCodec });
    const loadIncoming = scope.event<LoaderEvent<number, string>>('loadIncoming', { codec: loadEventCodec });
    scope.reduce(selectionChanged, (state, id) => state.set(selected, id));
    reduceComposedLoader(scope, inventory, load);
    reduceComposedLoader(scope, incoming, loadIncoming);
    const adjusted = scope.event<AdjustResult>('adjusted', { codec: adjustResultCodec });
    const adjust = scope.command<Adjust>(
      'adjust',
      (state, command) => {
        if (
          !allowed ||
          !Number.isSafeInteger(command.add) ||
          state.readKeyed(pending, command.shelfId) !== null ||
          state.readKeyed(completed, command.shelfId) === command.requestId
        )
          return false;
        if (state.readKeyed(inventory, command.shelfId).kind !== 'ready') return false;
        state.setKeyed(pending, command.shelfId, command.requestId);
        return true;
      },
      { codec: adjustCodec, classify: () => 'sensitive' },
    );
    scope.reduce(adjusted, (state, result) => {
      if (state.readKeyed(pending, result.shelfId) !== result.requestId) return;
      state.setKeyed(pending, result.shelfId, null);
      state.setKeyed(completed, result.shelfId, result.requestId);
      const data = state.readKeyed(inventory, result.shelfId);
      if (result.kind === 'ok' && data.kind === 'ready')
        state.setKeyed(inventory, result.shelfId, { ...data, data: { ...data.data, value: result.value } });
    });
    const effect = defineEffect({
      command: adjust,
      result: adjusted,
      codec: outcomeCodec,
      plan(state, command): AdjustPlan | undefined {
        const data = state.readKeyed(inventory, command.shelfId);
        if (data.kind !== 'ready' || state.readKeyed(pending, command.shelfId) !== command.requestId) return undefined;
        return { shelfId: command.shelfId, requestId: command.requestId, next: data.data.value + command.add };
      },
      decode(plan: AdjustPlan, outcome: Result<number, string>): AdjustResult {
        return outcome.success
          ? { kind: 'ok', shelfId: plan.shelfId, requestId: plan.requestId, value: outcome.value }
          : { kind: 'err', shelfId: plan.shelfId, requestId: plan.requestId, error: outcome.error };
      },
    });
    return {
      selected,
      inventory,
      incoming,
      pending,
      completed,
      selectionChanged,
      load,
      loadIncoming,
      adjust,
      adjusted,
      effect,
    };
  },
});
export type Inventory = ReturnType<typeof inventoryLibrary.setup>;

// This binding is typechecked against the built LMAO and StateBus public exports.
// The caller supplies its existing context and operation. No tracer, DI container or Op is invented.
export function bindInventoryOperation<Ctx extends OpContext>(
  runtime: ComposedRuntime,
  definition: EffectDefinition<Adjust, AdjustPlan, Result<number, string>, AdjustResult>,
  context: SpanContext<Ctx>,
  operation: Op<Ctx, [AdjustPlan, AbortSignal], number, string>,
) {
  return bindEffect(runtime, definition, {
    execute: (plan, { signal }) => context.span('inventory.adjust', operation, plan, signal),
    failure: () => new Err('unexpected-operation-rejection'),
  });
}

export const hostLibrary = defineLibrary({
  name: 'host',
  requires: [],
  setup(scope) {
    const ticks = scope.scalar('ticks', () => 0, { codec: numberCodec });
    const tick = scope.event<number>('tick', { codec: numberCodec });
    scope.reduce(tick, (state, amount) => state.set(ticks, state.read(ticks) + amount));
    return { ticks, tick };
  },
});
// Consumer-facing types remain branded and non-ambient.
export type InventoryState = LoadState<number, string>;
