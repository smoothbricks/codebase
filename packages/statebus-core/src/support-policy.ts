import { type CaptureValue, captureValue } from './capture.js';
import type { SupportClassification } from './composition.js';

export type RedactionReason = 'unclassified' | 'secret' | 'excluded' | 'consent';
export type RedactedValue = {
  readonly $statebus: 'redacted';
  readonly reason: RedactionReason;
};
export type SupportDecision =
  | { readonly kind: 'public'; readonly value: unknown }
  | { readonly kind: 'consent'; readonly value: unknown }
  | { readonly kind: 'redact'; readonly reason: RedactionReason };
/** The decoded input is detached from runtime state. The application's projection must be pure. */
export type SupportPolicy<T> = (value: T) => SupportDecision | undefined;

const markers = Object.freeze({
  unclassified: Object.freeze({ $statebus: 'redacted', reason: 'unclassified' }),
  secret: Object.freeze({ $statebus: 'redacted', reason: 'secret' }),
  excluded: Object.freeze({ $statebus: 'redacted', reason: 'excluded' }),
  consent: Object.freeze({ $statebus: 'redacted', reason: 'consent' }),
} satisfies Record<RedactionReason, RedactedValue>);

export function redactSupport(reason: RedactionReason): RedactedValue {
  return markers[reason];
}
export function publicSupport<T>(project: (value: T) => unknown): SupportPolicy<T> {
  return (value) => ({ kind: 'public', value: project(value) });
}
export function consentSupport<T>(project: (value: T) => unknown): SupportPolicy<T> {
  return (value) => ({ kind: 'consent', value: project(value) });
}
export const secretSupport: SupportPolicy<unknown> = () => ({ kind: 'redact', reason: 'secret' });

// This is a last-line transport guard, NOT a replacement for application field classification.
// It cannot identify an arbitrary opaque secret deliberately mislabeled as public text.
const forbiddenKey = /password|passwd|secret|token|credential|cookie|authorization|privatekey|apikey|headers|^auth$/i;
const credentialText =
  /^(?:bearer\s|basic\s|-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----)|\b(?:access_token|refresh_token|api_key|password)=/i;
function scrub(value: CaptureValue): CaptureValue | RedactedValue {
  if (typeof value === 'string') return credentialText.test(value) ? markers.secret : value;
  if (value === null || typeof value !== 'object') return value;
  if (Array.isArray(value)) return Object.freeze(value.map((item: CaptureValue) => scrub(item)));
  const result: { [key: string]: CaptureValue | RedactedValue } = {};
  for (const [key, item] of Object.entries(value)) {
    const field: CaptureValue = item;
    Object.defineProperty(result, key, {
      value: forbiddenKey.test(key.replace(/[^a-z0-9]/gi, '')) ? markers.secret : scrub(field),
      enumerable: true,
    });
  }
  return Object.freeze(result);
}

/** No broad include callback: secret/excluded classifications cannot be overridden by consent or a projection. */
export function projectSupport(
  classification: SupportClassification,
  policy: SupportPolicy<unknown> | undefined,
  value: unknown,
  consent: boolean,
  maxBytes: number,
): unknown {
  if (classification === 'secret' || classification === 'excluded') return markers[classification];
  if (!policy) return markers.unclassified;
  const decision = policy(value);
  if (!decision) return markers.unclassified;
  if (decision.kind === 'redact') return markers[decision.reason];
  if ((classification === 'sensitive' || decision.kind === 'consent') && !consent) return markers.consent;
  return scrub(captureValue(decision.value, maxBytes).value);
}
