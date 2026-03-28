/**
 * Parse and format monetary values to/from canonical decimal strings.
 *
 * These functions handle the conversion between human-readable decimal
 * strings ("1.50") and branded bigint types (Amount<'USD'>(150n)).
 *
 * For display purposes (locale-aware formatting with currency symbols),
 * use formatAmountDisplay which delegates to Intl.NumberFormat.
 */

import type { CurrencyDef } from './currency-registry.js';
import { Amount, type Amount as AmountValue, Basis, type Basis as BasisValue } from './types.js';

/**
 * Parse a canonical decimal string into an Amount<C>.
 *
 * Handles integers ("100"), decimals ("1.50"), and negatives ("-1.50").
 * Does NOT parse locale-specific formats (commas, spaces) -- use canonical format only.
 *
 * @throws if the string is empty, contains non-numeric characters, or has too many decimal points
 */
export function parseAmount<C extends string>(str: string, currency: CurrencyDef): AmountValue<C> {
  return Amount<C>(parseDecimalToBigInt(str, currency.decimals));
}

/**
 * Format an Amount<C> as a canonical decimal string.
 *
 * Always produces exact decimal places per currency (e.g., "1.50" for USD, "100" for JPY).
 */
export function formatAmount<C extends string>(amount: AmountValue<C>, currency: CurrencyDef): string {
  return formatBigIntToDecimal(amount, currency.decimals);
}

/**
 * Parse a canonical decimal string into a Basis<C>.
 *
 * Same rules as parseAmount but uses basisDecimals for scale.
 */
export function parseBasis<C extends string>(str: string, currency: CurrencyDef): BasisValue<C> {
  return Basis<C>(parseDecimalToBigInt(str, currency.basisDecimals));
}

/**
 * Format a Basis<C> as a canonical decimal string.
 */
export function formatBasis<C extends string>(basis: BasisValue<C>, currency: CurrencyDef): string {
  return formatBigIntToDecimal(basis, currency.basisDecimals);
}

/**
 * Format an Amount<C> for display using Intl.NumberFormat.
 * NOT for round-tripping -- output is locale-dependent and may include symbols.
 *
 * @example
 * ```ts
 * formatAmountDisplay(Amount<'USD'>(150n), getCurrency('USD')) // "$1.50"
 * formatAmountDisplay(Amount<'USD'>(150n), getCurrency('USD'), 'de-DE') // "1,50\xa0$"
 * ```
 */
export function formatAmountDisplay<C extends string>(
  amount: AmountValue<C>,
  currency: CurrencyDef,
  locale?: string,
): string {
  // Convert via canonical decimal string to avoid BigInt->Number precision loss.
  // Number() on a decimal string like "92000000000.50" loses precision beyond
  // ~15 significant digits, but that's inherent to Intl.NumberFormat accepting
  // only number. For amounts > Number.MAX_SAFE_INTEGER minor units, the
  // trailing digits may be wrong — acceptable for a display-only function.
  const numericValue = Number(formatAmount(amount, currency));

  return new Intl.NumberFormat(locale, {
    style: 'currency',
    currency: currency.code,
    minimumFractionDigits: currency.decimals,
    maximumFractionDigits: currency.decimals,
  }).format(numericValue);
}

// --- Internal helpers ---

/**
 * Parse a decimal string to bigint at the given scale (number of decimal places).
 */
function parseDecimalToBigInt(str: string, decimals: number): bigint {
  if (!str || str.trim().length === 0) {
    throw new Error('Cannot parse empty string as monetary value');
  }

  const trimmed = str.trim();

  // Handle negative sign
  const isNegative = trimmed.startsWith('-');
  const unsigned = isNegative ? trimmed.slice(1) : trimmed;

  // Validate: only digits and at most one decimal point
  if (!/^\d+(\.\d+)?$/.test(unsigned)) {
    throw new Error(`Invalid monetary string: "${str}"`);
  }

  const dotIndex = unsigned.indexOf('.');

  let intPart: string;
  let fracPart: string;

  if (dotIndex === -1) {
    intPart = unsigned;
    fracPart = '';
  } else {
    intPart = unsigned.slice(0, dotIndex);
    fracPart = unsigned.slice(dotIndex + 1);
  }

  // Pad or truncate fractional part to exactly `decimals` digits
  if (fracPart.length > decimals) {
    // Truncate excess digits (caller should not provide excess, but handle gracefully)
    fracPart = fracPart.slice(0, decimals);
  } else {
    fracPart = fracPart.padEnd(decimals, '0');
  }

  // For zero-decimal currencies, fracPart will be empty
  const combined = decimals === 0 ? intPart : intPart + fracPart;
  const value = BigInt(combined);

  return isNegative ? -value : value;
}

/**
 * Format a bigint at the given scale to a canonical decimal string.
 */
function formatBigIntToDecimal(value: bigint, decimals: number): string {
  if (decimals === 0) {
    return value.toString();
  }

  const isNegative = value < 0n;
  const abs = isNegative ? -value : value;
  const absStr = abs.toString();

  let intPart: string;
  let fracPart: string;

  if (absStr.length <= decimals) {
    // Value is less than 1.0 -- pad with leading zeros
    intPart = '0';
    fracPart = absStr.padStart(decimals, '0');
  } else {
    intPart = absStr.slice(0, absStr.length - decimals);
    fracPart = absStr.slice(absStr.length - decimals);
  }

  const result = `${intPart}.${fracPart}`;
  return isNegative ? `-${result}` : result;
}
