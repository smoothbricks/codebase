import type { ComposedRuntime } from '@smoothbricks/statebus-core';
import type { ShelfId } from './codecs.js';
import { canAdjust, type Inventory } from './library.js';

// Compiled against installed declarations, never imported by the runtime scenarios.
export function rejectInvalidCalls(runtime: ComposedRuntime, model: Inventory, id: ShelfId): void {
  // @ts-expect-error Unbranded strings cannot address branded resources.
  runtime.readKeyed(model.inventory, 'shelf:a');
  // @ts-expect-error Shelf IDs cannot masquerade as request IDs.
  runtime.publish(model.adjust, { shelfId: id, requestId: id, add: 1 });
  // @ts-expect-error The declared scalar payload is not an arbitrary object.
  runtime.publish(model.selectionChanged, { id });
  // @ts-expect-error Required capability providers preserve their value type.
  canAdjust.provide('yes');
}
