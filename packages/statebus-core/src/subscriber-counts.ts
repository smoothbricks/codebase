type SubscriberCounts<Key extends string> = Readonly<Partial<Record<Key, number>>>;

/** Copy at the ownership boundary; published payloads must never be recycled. */
export function mergeSubscriberCounts<Key extends string>(
  previous: SubscriberCounts<Key>,
  next: SubscriberCounts<Key>,
): Partial<Record<Key, number>> {
  return { ...previous, ...next };
}

/**
 * One accumulator per bus. A multi-payload wave allocates one owned record,
 * not a copy of the accumulated prefix for each event. Single payloads pass
 * through unchanged. take() transfers ownership; no published storage is reused.
 */
export class SubscriberCountBatch<Key extends string> {
  private first: SubscriberCounts<Key> | undefined;
  private owned: Partial<Record<Key, number>> | undefined;

  append(next: SubscriberCounts<Key>): void {
    if (this.first === undefined) {
      this.first = next;
    } else if (this.owned === undefined) {
      this.owned = mergeSubscriberCounts(this.first, next);
    } else {
      for (const key in next) {
        if (!Object.hasOwn(next, key)) continue;
        if (key === '__proto__') {
          // This legal state name must be an own data property, not a setter.
          Object.defineProperty(this.owned, key, {
            value: next[key],
            enumerable: true,
            configurable: true,
            writable: true,
          });
        } else {
          this.owned[key] = next[key];
        }
      }
    }
  }

  take(): SubscriberCounts<Key> | undefined {
    const result = this.owned ?? this.first;
    this.clear();
    return result;
  }

  clear(): void {
    this.first = undefined;
    this.owned = undefined;
  }
}
