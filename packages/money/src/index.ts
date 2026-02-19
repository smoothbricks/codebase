/**
 * @smoothbricks/money
 *
 * Branded monetary types and pure arithmetic for monetary operations.
 * Zero external dependencies. Works on Bun and CF Workers.
 *
 * @example
 * ```typescript
 * import { Amount, add, getCurrency, amountScale } from '@smoothbricks/money';
 *
 * const a = Amount<'USD'>(100n);  // 100 cents = $1.00
 * const b = Amount<'USD'>(250n);  // 250 cents = $2.50
 * const total = add(a, b);        // 350 cents = $3.50
 *
 * const usd = getCurrency('USD');
 * amountScale(usd);               // 100n
 * ```
 *
 * @packageDocumentation
 */

// Proportional allocation
export { allocateProportional } from './allocate.js';
// Amount arithmetic
export { add, divideWithRemainder, multiply, subtract } from './amount.js';
// Journal assertions
export { assertBalanced, isBalanced } from './assertions.js';
// Basis arithmetic
export { basisAdd, basisMultiply, basisSubtract, toBasis } from './basis.js';

// Currency registry
export type { CurrencyDef } from './currency-registry.js';
export { amountScale, basisScale, getCurrency, hasCurrency, registerCurrency } from './currency-registry.js';
// Formatting (canonical decimal strings)
export { formatAmount, formatAmountDisplay, formatBasis, parseAmount, parseBasis } from './format.js';
// FX conversion
export type { FxAudit, FxResult } from './fx.js';
export { convertFx } from './fx.js';
// Rate operations
export { applyRate } from './rate.js';
// Rounding
export { RoundingMode, roundBasisToAmount } from './rounding.js';
export type { Rate as RateType } from './types.js';
// Branded types
export { Amount, Basis, Rate, Unit } from './types.js';
