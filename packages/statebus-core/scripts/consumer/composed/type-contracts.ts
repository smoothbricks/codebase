import {
  type ComposedRuntime,
  type EventHandle,
  type LibraryScope,
  replayCaptureEnvelope,
  type StateBusComposition,
  type SupportArtifact,
} from '@smoothbricks/statebus-core';
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

export function rejectUnsafeCaptureReplay(composition: StateBusComposition, artifact: SupportArtifact): void {
  // @ts-expect-error Sanitized support artifacts are not lossless local-replay envelopes.
  replayCaptureEnvelope(composition, artifact);
}
export function rejectUntypedReaction(
  scope: LibraryScope,
  source: EventHandle<number>,
  target: EventHandle<string>,
): void {
  // @ts-expect-error A reaction must produce the target event payload, not its source payload type.
  scope.react(source, target, (_state, payload) => payload);
}
