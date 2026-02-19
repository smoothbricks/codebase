/**
 * ISO 4217 currency registry with per-currency decimals and basisDecimals.
 *
 * The registry is mutable to allow integrator extension via registerCurrency().
 * Built-in currencies cover common fiat (USD, EUR, GBP, JPY, CHF, BHD)
 * and crypto (BTC, USDC).
 */

/**
 * Currency definition with both Amount and Basis precision.
 */
export interface CurrencyDef {
  readonly code: string;
  readonly symbol: string;
  /** Minor unit scale for Amount<C> (USD=2, BTC=8, JPY=0) */
  readonly decimals: number;
  /** High-precision scale for Basis<C> (USD=10, BTC=10, ETH=18) */
  readonly basisDecimals: number;
  readonly type: 'fiat' | 'crypto';
}

// Mutable registry -- integrators can add custom currencies
const registry = new Map<string, CurrencyDef>();

// Built-in currencies
const builtins: readonly CurrencyDef[] = [
  { code: 'USD', symbol: '$', decimals: 2, basisDecimals: 10, type: 'fiat' },
  { code: 'EUR', symbol: '\u20ac', decimals: 2, basisDecimals: 10, type: 'fiat' },
  { code: 'GBP', symbol: '\u00a3', decimals: 2, basisDecimals: 10, type: 'fiat' },
  { code: 'JPY', symbol: '\u00a5', decimals: 0, basisDecimals: 8, type: 'fiat' },
  { code: 'CHF', symbol: 'CHF', decimals: 2, basisDecimals: 10, type: 'fiat' },
  { code: 'BHD', symbol: 'BD', decimals: 3, basisDecimals: 10, type: 'fiat' },
  { code: 'BTC', symbol: '\u20bf', decimals: 8, basisDecimals: 10, type: 'crypto' },
  { code: 'USDC', symbol: 'USDC', decimals: 6, basisDecimals: 10, type: 'crypto' },
];

for (const def of builtins) {
  registry.set(def.code, def);
}

/**
 * Look up a currency by ISO 4217 code.
 * @throws if the currency code is not registered (invariant -- programmer/config bug)
 */
export function getCurrency(code: string): CurrencyDef {
  const def = registry.get(code);
  if (!def) {
    // invariant throw: unknown currency code is a programmer/config bug
    throw new Error(`Unknown currency code: ${code}`);
  }
  return def;
}

/**
 * Check if a currency code is registered.
 */
export function hasCurrency(code: string): boolean {
  return registry.has(code);
}

/**
 * Register a custom currency. Allows integrator extension of the registry.
 */
export function registerCurrency(def: CurrencyDef): void {
  registry.set(def.code, def);
}

/**
 * Amount scale factor: 10^decimals.
 * For USD (decimals=2): 100n (1 dollar = 100 cents)
 * For JPY (decimals=0): 1n (1 yen = 1 yen)
 * For BTC (decimals=8): 100000000n (1 BTC = 100M satoshi)
 */
export function amountScale(currency: CurrencyDef): bigint {
  return 10n ** BigInt(currency.decimals);
}

/**
 * Basis scale factor: 10^basisDecimals.
 * For USD (basisDecimals=10): 10000000000n
 */
export function basisScale(currency: CurrencyDef): bigint {
  return 10n ** BigInt(currency.basisDecimals);
}
