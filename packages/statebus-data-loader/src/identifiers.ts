declare const requestIdBrand: unique symbol;
declare const fingerprintBrand: unique symbol;

/** A logical load identity, stable across retries but distinct from resource/cache identities. */
export type LoadRequestId = string & { readonly [requestIdBrand]: 'LoadRequestId' };
export type LoadFingerprint = string & { readonly [fingerprintBrand]: 'LoadFingerprint' };

/** Bind an ID from the composition's identity source. No wrapper or string copy is created. */
export function loadRequestId(value: string): LoadRequestId;
export function loadRequestId(value: string): string {
  if (value.length === 0) throw new RangeError('A load request ID must not be empty.');
  return value;
}

/** Bind the feature's deterministic operation fingerprint at request creation, not per result/read. */
export function loadFingerprint(value: string): LoadFingerprint;
export function loadFingerprint(value: string): string {
  return value;
}
