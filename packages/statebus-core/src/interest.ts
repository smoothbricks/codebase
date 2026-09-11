/** Plain address data. An absent ID means scalar/whole-property interest. */
export interface StateInterest<Key extends string = string> {
  readonly key: Key;
  readonly id?: string | number;
}

export interface StateInterestChange<Key extends string = string> {
  readonly interest: StateInterest<Key>;
  readonly subscribers: number;
}

declare const interestKeyBrand: unique symbol;
/** Wire identity, deliberately not interchangeable with a request ID or arbitrary string. */
export type StateInterestKey = string & { readonly [interestKeyBrand]: 'StateInterestKey' };

/** Cold-path codec for persistence/fingerprints. Runtime lookups use the structural address directly. */
export function stateInterestKey(interest: StateInterest): StateInterestKey;
export function stateInterestKey(interest: StateInterest): string {
  validateInterest(interest);
  return JSON.stringify([interest.key, typeof interest.id, interest.id ?? null]);
}

function validateInterest(interest: StateInterest): void {
  if (typeof interest.id === 'number' && !Number.isFinite(interest.id)) {
    throw new RangeError('State interest IDs must be finite numbers or strings.');
  }
}

/** Structural indexing without tuple allocation or string encoding on get/set/delete. */
export class StateInterestMap<Value, Key extends string = string> {
  private readonly properties = new Map<string, Map<string | number | undefined, Value>>();
  private count = 0;

  get size(): number {
    return this.count;
  }

  get(interest: StateInterest<Key>): Value | undefined {
    return this.getAt(interest.key, interest.id);
  }

  getAt(key: string, id?: string | number): Value | undefined {
    return this.properties.get(key)?.get(id);
  }

  has(interest: StateInterest<Key>): boolean {
    return this.properties.get(interest.key)?.has(interest.id) ?? false;
  }

  set(interest: StateInterest<Key>, value: Value): void {
    let ids = this.properties.get(interest.key);
    if (ids === undefined) {
      ids = new Map();
      this.properties.set(interest.key, ids);
    }
    if (!ids.has(interest.id)) this.count += 1;
    ids.set(interest.id, value);
  }

  delete(interest: StateInterest<Key>): boolean {
    const ids = this.properties.get(interest.key);
    if (ids === undefined || !ids.delete(interest.id)) return false;
    this.count -= 1;
    if (ids.size === 0) this.properties.delete(interest.key);
    return true;
  }

  *values(): IterableIterator<Value> {
    for (const ids of this.properties.values()) yield* ids.values();
  }

  clear(): void {
    this.properties.clear();
    this.count = 0;
  }
}

interface InterestSlot {
  epoch: number;
  position: number;
}

const EMPTY_CHANGES: readonly StateInterestChange<never>[] = Object.freeze([]);
const noop = () => {};

/**
 * Runtime-owned wave scratch. Each incoming change is visited once. Resolved slots
 * and scratch capacity survive warmed waves; only the final owned array escapes.
 * The slot cache is capped between waves, never an unbounded address interner.
 */
export class StateInterestBatch<Key extends string = string> {
  private count = 0;
  private epoch = 0;
  private readonly slots = new StateInterestMap<InterestSlot, Key>();
  private readonly latest = new Array<StateInterestChange<Key> | undefined>(32).fill(undefined);
  private first: readonly StateInterestChange<Key>[] | undefined;
  private multiple = false;

  constructor(private readonly maxRetainedAddresses = 1024) {
    if (!Number.isSafeInteger(maxRetainedAddresses) || maxRetainedAddresses < 0) {
      throw new RangeError('maxRetainedAddresses must be a nonnegative safe integer.');
    }
  }

  append(changes: readonly StateInterestChange<Key>[]): void {
    if (changes.length === 0) return;
    if (this.first === undefined) this.first = changes;
    else this.multiple = true;
    for (let index = 0; index < changes.length; index += 1) {
      const change = changes[index];
      let slot = this.slots.get(change.interest);
      if (slot === undefined) {
        validateInterest(change.interest);
        slot = { epoch: this.epoch, position: this.count++ };
        this.slots.set(change.interest, slot);
      } else if (slot.epoch !== this.epoch) {
        slot.epoch = this.epoch;
        slot.position = this.count++;
      }
      this.latest[slot.position] = change;
    }
  }

  take(): readonly StateInterestChange<Key>[] {
    let result: readonly StateInterestChange<Key>[];
    if (this.count === 0) result = EMPTY_CHANGES;
    else if (!this.multiple && this.first !== undefined && this.first.length === this.count) result = this.first;
    else {
      const owned: StateInterestChange<Key>[] = new Array(this.count);
      for (let index = 0; index < this.count; index += 1) {
        const change = this.latest[index];
        if (change === undefined) throw new Error('Missing dirty interest slot.');
        owned[index] = change;
      }
      result = owned;
    }
    this.clear();
    return result;
  }

  clear(): void {
    for (let index = 0; index < this.count; index += 1) this.latest[index] = undefined;
    this.count = 0;
    this.first = undefined;
    this.multiple = false;
    this.epoch += 1;
    if (this.slots.size > this.maxRetainedAddresses || this.epoch === Number.MAX_SAFE_INTEGER) {
      this.slots.clear();
      this.epoch = 0;
    }
  }

  /** Diagnostic bound, not an application state API. */
  get retainedAddresses(): number {
    return this.slots.size;
  }
}

/** Pure owned merge for standalone reducers; the dispatcher reuses its own batch instead. */
export function mergeStateInterests<Key extends string>(
  previous: readonly StateInterestChange<Key>[],
  incoming: readonly StateInterestChange<Key>[],
): readonly StateInterestChange<Key>[] {
  const batch = new StateInterestBatch<Key>(0);
  batch.append(previous);
  batch.append(incoming);
  return batch.take();
}

/** Runtime-owned demand. Lease setup/publication may allocate; warmed count reads do not encode or copy. */
export class StateInterestRegistry<Key extends string = string> {
  private readonly entries = new StateInterestMap<StateInterestChange<Key>, Key>();
  private readonly counts = new Map<Key, number>();

  constructor(private readonly changed: (changes: readonly StateInterestChange<Key>[]) => void) {}

  get propertyCounts(): ReadonlyMap<Key, number> {
    return this.counts;
  }

  snapshot(): readonly StateInterestChange<Key>[] {
    return this.entries.size === 0 ? EMPTY_CHANGES : Array.from(this.entries.values());
  }

  count(interest: StateInterest<Key>): number {
    return this.countAt(interest.key, interest.id);
  }

  countAt(key: string, id?: string | number): number {
    return this.entries.getAt(key, id)?.subscribers ?? 0;
  }

  acquire(interests: readonly (Key | StateInterest<Key>)[]): () => void {
    if (interests.length === 0) return noop;
    const owned: StateInterest<Key>[] = new Array(interests.length);
    // Validate and capture the complete lease before changing either exact or property counts.
    for (let index = 0; index < interests.length; index += 1) {
      const value = interests[index];
      if (typeof value === 'string') owned[index] = Object.freeze({ key: value });
      else {
        validateInterest(value);
        owned[index] = Object.freeze(value.id === undefined ? { key: value.key } : { key: value.key, id: value.id });
      }
    }
    this.adjust(owned, 1);
    let released = false;
    return () => {
      if (released) return;
      released = true;
      this.adjust(owned, -1);
    };
  }

  private adjust(interests: readonly StateInterest<Key>[], delta: 1 | -1): void {
    const changes: StateInterestChange<Key>[] = new Array(interests.length);
    for (let index = 0; index < interests.length; index += 1) {
      const interest = interests[index];
      const subscribers = (this.entries.get(interest)?.subscribers ?? 0) + delta;
      const propertyCount = (this.counts.get(interest.key) ?? 0) + delta;
      const change = Object.freeze({ interest, subscribers });
      if (subscribers === 0) this.entries.delete(interest);
      else this.entries.set(interest, change);
      if (propertyCount === 0) this.counts.delete(interest.key);
      else this.counts.set(interest.key, propertyCount);
      changes[index] = change;
    }
    this.changed(changes);
  }
}
