/**
 * @smoothbricks/money: one exact money value, its canonical currency list and its stored form
 * (specs/money/00-money.md). No dependencies at run time; Typia tags for the wire live in `@smoothbricks/money/wire`.
 *
 * @example
 * ```typescript
 * import { defineCurrencies, money, add, formatAmount } from '@smoothbricks/money';
 *
 * const accepted = defineCurrencies(['USD', 'EUR', 'BTC']);
 * const price = money(accepted, 'USD', 1_500_000); // $1.50 at six decimals
 * ```
 *
 * @packageDocumentation
 */

export { allocateProportional } from './allocate.js';
export {
  add,
  amount,
  compare,
  divideWithRemainder,
  isZero,
  multiply,
  negate,
  subtract,
  sum,
  toBigInt,
} from './amount.js';
export { applyRate, basis, basisAdd, basisMultiply, basisSubtract, toBasis } from './basis.js';
export type { StoredMoney } from './cell.js';
export { cellCurrencyId, combineVariantSums, decodeCell, encodeCells, int64FromWords, int64ToWords } from './cell.js';
export type { Currency, CurrencyCode, CurrencyRegistry } from './currencies.js';
export { basisDecimals, CURRENCIES, currencyByCode, currencyById, defineCurrencies } from './currencies.js';
export type { MoneyViolation } from './errors.js';
export { formatAmount, formatAmountDisplay, identityText, parseAmount } from './format.js';
export type { FxRate, FxResult, Residual } from './fx.js';
export { convertFx, fxRate } from './fx.js';
export {
  addMoney,
  checkBalanced,
  compareMoney,
  money,
  moneyFromWire,
  moneyOf,
  moneyToWire,
  sumMoney,
} from './money.js';
export type { CanonicalPolicy, CurrencyType } from './publish.js';
export type { Result } from './result.js';
export { fromMinor, minor, RoundingMode, roundBasis, toMinor } from './rounding.js';
export type { Amount, Basis, Minor, Money, Rate, Unit } from './types.js';
export { rate, unit } from './types.js';
