/**
 * FX (foreign exchange) conversion with explicit remainder and audit trail.
 *
 * Phase 43 scope: function signature and minimal implementation.
 * Full FX integration (rate feeds, cross-currency Basis alignment) deferred to Phase 46.
 *
 * The audit trail ensures every conversion is traceable -- amount in, rate applied,
 * rounding mode used, remainder captured.
 */

import type { CurrencyDef } from './currency-registry.js';
import type { RoundingMode } from './rounding.js';
import { roundBasisToAmount } from './rounding.js';
import { type Amount, Basis, type Basis as BasisValue } from './types.js';

/**
 * Audit trail for an FX conversion. Captures all inputs for traceability.
 */
export interface FxAudit<From extends string, To extends string> {
  readonly fromAmount: Amount<From>;
  readonly rate: BasisValue<To>;
  readonly mode: RoundingMode;
}

/**
 * Result of an FX conversion including the remainder for reconciliation.
 */
export interface FxResult<From extends string, To extends string> {
  readonly result: Amount<To>;
  readonly remainder: BasisValue<To>;
  readonly audit: FxAudit<From, To>;
}

/**
 * Convert an amount from one currency to another using an FX rate.
 *
 * The rate is expressed as Basis<To> per one minor unit of From.
 * The conversion multiplies fromAmount by the rate in Basis precision,
 * then rounds to the target currency's Amount precision.
 *
 * The remainder captures what was lost in rounding, enabling reconciliation.
 *
 * @example
 * ```ts
 * // Convert 100 USD cents to EUR at rate 0.92 (expressed in EUR basis)
 * const result = convertFx(
 *   Amount<'USD'>(100n),
 *   Basis<'EUR'>(9200000000n), // 0.92 in EUR basis (10 decimals)
 *   getCurrency('USD'),
 *   getCurrency('EUR'),
 *   RoundingMode.HalfUp
 * );
 * // result.result: Amount<'EUR'>(92n) -- 0.92 EUR
 * // result.remainder: Basis<'EUR'>(...) -- rounding remainder
 * ```
 */
export function convertFx<From extends string, To extends string>(
  amount: Amount<From>,
  rate: BasisValue<To>,
  _fromCurrency: CurrencyDef,
  toCurrency: CurrencyDef,
  mode: RoundingMode,
): FxResult<From, To> {
  // Multiply amount by rate in Basis precision
  const basisTotal = Basis<To>(amount * rate);

  // Round to target Amount
  const settled = roundBasisToAmount(basisTotal, toCurrency, mode);

  // Compute remainder: what was lost in rounding
  const scaleDiff = BigInt(toCurrency.basisDecimals - toCurrency.decimals);
  const divisor = 10n ** scaleDiff;
  const settledInBasis = settled * divisor;
  const remainder = Basis<To>(basisTotal - settledInBasis);

  return {
    result: settled,
    remainder,
    audit: {
      fromAmount: amount,
      rate,
      mode,
    },
  };
}
