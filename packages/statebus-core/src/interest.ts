/** Plain, serializable identity. A missing ID means scalar/whole-property interest. */
export interface StateInterest<Key extends string = string> {
  readonly key: Key;
  readonly id?: string | number;
}

export interface StateInterestChange<Key extends string = string> {
  readonly interest: StateInterest<Key>;
  readonly subscribers: number;
}

/** JSON tuple encoding distinguishes absent, numeric and string IDs and delimiter characters. */
export function stateInterestKey(interest: StateInterest): string {
  if (typeof interest.id === 'number' && !Number.isFinite(interest.id)) {
    throw new RangeError('State interest IDs must be finite numbers or strings.');
  }
  return JSON.stringify([interest.key, typeof interest.id, interest.id ?? null]);
}

/** Last count for each exact address wins; neither input nor published payloads are mutated. */
export function mergeStateInterests<Key extends string>(
  previous: readonly StateInterestChange<Key>[],
  incoming: readonly StateInterestChange<Key>[],
): readonly StateInterestChange<Key>[] {
  const byAddress = new Map<string, StateInterestChange<Key>>();
  for (const change of previous) byAddress.set(stateInterestKey(change.interest), change);
  for (const change of incoming) byAddress.set(stateInterestKey(change.interest), change);
  return [...byAddress.values()];
}

/** Runtime-owned bookkeeping, not application state or an eviction policy. */
export class StateInterestRegistry<Key extends string = string> {
  private readonly entries = new Map<string, StateInterestChange<Key>>();

  constructor(private readonly changed: (changes: readonly StateInterestChange<Key>[]) => void) {}

  snapshot(): readonly StateInterestChange<Key>[] {
    return [...this.entries.values()];
  }

  count(interest: StateInterest): number {
    return this.entries.get(stateInterestKey(interest))?.subscribers ?? 0;
  }

  acquire(interests: readonly StateInterest<Key>[]): () => void {
    // Copy addresses, not just the input array: callers may reuse/mutate their descriptors.
    const owned = interests.map((interest) => {
      stateInterestKey(interest); // Validate the complete lease before changing any counts.
      return Object.freeze({ ...interest });
    });
    this.adjust(owned, 1);
    let released = false;
    return () => {
      if (released) return;
      released = true;
      this.adjust(owned, -1);
    };
  }

  private adjust(interests: readonly StateInterest<Key>[], delta: 1 | -1): void {
    const changes: StateInterestChange<Key>[] = [];
    for (const interest of interests) {
      const key = stateInterestKey(interest);
      const subscribers = Math.max(0, (this.entries.get(key)?.subscribers ?? 0) + delta);
      const change = Object.freeze({ interest, subscribers });
      if (subscribers === 0) this.entries.delete(key);
      else this.entries.set(key, change);
      changes.push(change);
    }
    if (changes.length !== 0) this.changed(changes);
  }
}
