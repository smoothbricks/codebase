/// <reference types="node" />

import { fileURLToPath } from 'node:url';
import typia from 'typia';
import { loadAddon } from './addon.js';
import type {
  NativeCoordinatorOperations,
  NativeEvents,
  NativeHostOperations,
  NativeJobAttachmentCapabilities,
  NativeJobHandleCapabilities,
  NativeJobHandleOperations,
  NativeProjectOperations,
  NativeWorkspaceHandleOperations,
  NativeWorkspaceRefOperations,
} from './native.generated.js';
import type { CoordinatorEndpoint } from './types.js';

// Every controller operation's method is declared in `native.generated.ts`, from the operation
// table the addon's adapters are generated from, and each handle extends exactly the operations
// its authority admits. What follows is only what no operation declares: each handle's identity,
// read from the call that minted it.

export interface NativeProjectHandle extends NativeProjectOperations {
  readonly repoId: string;
  readonly gitRoot: string;
}

export interface NativeWorkspaceRefHandle extends NativeWorkspaceRefOperations {
  readonly name: string;
  readonly mountPath: string;
  /** The reference as a land or rebase target, pinned to the incarnation it was resolved at. */
  readonly targetJson: string;
}

export type NativeCoordinatorHandle = NativeCoordinatorOperations;

/** A host connection's adapters and the close that ends it. */
export type NativeHostHandle = NativeHostOperations & { close(): Promise<void> };

export interface NativeWorkspaceHandle extends NativeWorkspaceHandleOperations {
  readonly workspaceIncarnation: string;
  readonly name: string;
  readonly mountPath: string;
}

export interface NativeJobHandle extends NativeJobHandleOperations, NativeJobHandleCapabilities {
  readonly id: number;
}

/**
 * A stream-lane call's events as an async iterator. Unlike an async generator, whose `return`
 * waits behind a `next` in flight, `return` here closes the call at once.
 */
export interface EventIterator<T> {
  next(): Promise<IteratorResult<T, undefined>>;
  return(): Promise<IteratorResult<T, undefined>>;
  [Symbol.asyncIterator](): EventIterator<T>;
}

/**
 * Iterates the call `open` starts, each event parsed by `parse`. The first `next` opens the call
 * and each `next` sends one demand; `return` closes the call -- even while a `next` waits for its
 * event, which then resolves done -- and opens none that was never opened. A `next` whose event
 * fails to parse closes the call before it rejects.
 */
export function eventIterator<T>(open: () => Promise<NativeEvents>, parse: (json: string) => T): EventIterator<T> {
  const done: IteratorResult<T, undefined> = { done: true, value: undefined };
  let events: Promise<NativeEvents> | undefined;
  let returned = false;
  const iterator: EventIterator<T> = {
    async next() {
      if (returned) {
        return done;
      }
      events ??= open();
      const opened = await events;
      const event = await opened.next();
      if (event === null) {
        return done;
      }
      try {
        return { done: false, value: parse(event) };
      } catch (error) {
        await opened.close();
        throw error;
      }
    },
    async return() {
      returned = true;
      // A call that failed to open has no close to send; its failure rejected the `next` that
      // opened it.
      await events?.then(
        (opened) => opened.close(),
        () => undefined,
      );
      return done;
    },
    [Symbol.asyncIterator]: () => iterator,
  };
  return iterator;
}
export type NativeJobAttachmentHandle = NativeJobAttachmentCapabilities;

interface NativeModule {
  coordinatorEndpoint(descriptor: number): CoordinatorEndpoint;
  openProject(endpoint: CoordinatorEndpoint, path: string): Promise<NativeProjectHandle>;
  connectCoordinator(endpoint: CoordinatorEndpoint, path: string): Promise<NativeCoordinatorHandle>;
  openHost(launcher: string): Promise<NativeHostHandle>;
}

const assertNativeModule = typia.createAssert<NativeModule>();

export function loadNativeModule(): NativeModule {
  return loadAddon(fileURLToPath(import.meta.url), assertNativeModule);
}
