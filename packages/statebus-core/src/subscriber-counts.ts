/** Coalesce one wave without mutating event payloads retained by callers or journals. */
export function mergeSubscriberCounts<Key extends string>(
  previous: Readonly<Partial<Record<Key, number>>>,
  next: Readonly<Partial<Record<Key, number>>>,
): Partial<Record<Key, number>> {
  return { ...previous, ...next };
}
