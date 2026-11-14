/**
 * The canonical currency list and an embedder's accepted subset (specs/money/00-money.md §2).
 */

import { CURRENCY_LIST } from './currency-list.generated.js';
import type { MoneyViolation } from './errors.js';
import type { CanonicalPolicy, CurrencyType } from './publish.js';
import { err, ok, type Result } from './result.js';

/** Every code the list holds. */
export type CurrencyCode = (typeof CURRENCY_LIST)[number]['code'];

/** A currency of the list. */
export interface Currency<Code extends string = string> {
  /** ISO 4217 numeric code for fiat; this list's own range from 1000 for crypto. */
  readonly id: number;
  readonly code: Code;
  readonly name: string;
  /** For payment rails and the choice of FX rate source; no arithmetic reads it. */
  readonly type: CurrencyType;
  /** The display and settlement unit (USD 2, JPY 0, BTC 8). */
  readonly minorDecimals: number;
  /** The scale of a cell whose offset is 0; frozen. */
  readonly baseDecimals: number;
  /** The scale of an `Amount` in a program; may only rise. */
  readonly runtimeDecimals: number;
  /** Whether base-scale amounts can pass 64 bits: such a currency's sums accumulate in 128. */
  readonly wide: boolean;
  /** The canonical variant rule of its cells; frozen. */
  readonly policy: CanonicalPolicy;
  readonly withdrawn?: true;
}

/** The scale of rate arithmetic: four digits finer than the runtime scale. */
export function basisDecimals(currency: Currency): number {
  return currency.runtimeDecimals + 4;
}

const listed: readonly Currency<CurrencyCode>[] = CURRENCY_LIST;
const byCode = new Map<string, Currency<CurrencyCode>>(listed.map((entry) => [entry.code, entry]));
const byId = new Map<number, Currency<CurrencyCode>>(listed.map((entry) => [entry.id, entry]));

/** The list, ordered by id. */
export const CURRENCIES: readonly Currency<CurrencyCode>[] = listed;

export function currencyByCode(code: string): Result<Currency<CurrencyCode>, MoneyViolation> {
  const found = byCode.get(code);
  return found === undefined ? err({ code: 'UNKNOWN_CURRENCY', op: 'currencyByCode', currency: code }) : ok(found);
}

export function currencyById(id: number): Result<Currency<CurrencyCode>, MoneyViolation> {
  const found = byId.get(id);
  return found === undefined ? err({ code: 'UNKNOWN_CURRENCY', op: 'currencyById', currency: id }) : ok(found);
}

/** The currencies an embedder accepts: a subset of the list. */
export interface CurrencyRegistry<Code extends string> {
  readonly currencies: readonly Currency<Code>[];
  get(code: string): Result<Currency<Code>, MoneyViolation>;
  byId(id: number): Result<Currency<Code>, MoneyViolation>;
  accepts(code: string): code is Code;
}

/**
 * The subset of the list an embedder accepts.
 * @throws TypeError for a code the list does not hold, or one named twice (a programmer error at definition).
 */
export function defineCurrencies<const Codes extends readonly CurrencyCode[]>(
  codes: Codes,
): CurrencyRegistry<Codes[number]> {
  type Code = Codes[number];
  const accepted = new Set<string>(codes);
  if (accepted.size !== codes.length)
    throw new TypeError(`defineCurrencies: a code is named twice in [${codes.join(', ')}]`);
  const isAccepted = (entry: Currency<CurrencyCode>): entry is Currency<Code> => accepted.has(entry.code);
  const currencies = listed.filter(isAccepted);
  const unknown = codes.filter((code) => !byCode.has(code));
  if (unknown.length > 0) throw new TypeError(`defineCurrencies: the currency list holds no ${unknown.join(', ')}`);
  const accepts = (code: string): code is Code => accepted.has(code);
  return {
    currencies,
    accepts,
    get(code) {
      const found = byCode.get(code);
      return found !== undefined && isAccepted(found)
        ? ok(found)
        : err({ code: 'UNKNOWN_CURRENCY', op: 'registry.get', currency: code });
    },
    byId(id) {
      const found = byId.get(id);
      return found !== undefined && isAccepted(found)
        ? ok(found)
        : err({ code: 'UNKNOWN_CURRENCY', op: 'registry.byId', currency: id });
    },
  };
}
