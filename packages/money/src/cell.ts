/**
 * The variant cell (specs/money/00-money.md §8): a stored money value is a u32 currency cell and a 64-bit amount
 * cell. The currency cell declares the scale the amount is written at, so a base-scale value's cell is the plain
 * currency id and a reader needs nothing but the cell to know the scale.
 */

import { addIntegers, scaleUp } from './amount.js';
import type { Currency, CurrencyRegistry } from './currencies.js';
import type { MoneyViolation } from './errors.js';
import { canonical, fitsI64, pow10, toBig } from './integer.js';
import { err, ok, type Result } from './result.js';
import { type Amount, brandAmount, brandMoney, type Money, moneyOfCode } from './types.js';

const ID_MASK = 0xff_ffff;
const OFFSET_SHIFT = 24;
const OFFSET_MASK = 0x3f;
const WIDE_BIT = 0x4000_0000;
const OFFSET_MIN = -32;

/** A stored row of one currency field: its currency cell and one 64-bit amount cell per amount, a number while safe. */
export interface StoredMoney {
  readonly cell: number;
  readonly values: readonly (number | bigint)[];
}

function cellOf(currency: Currency, scale: number): number {
  return currency.id | (((scale - currency.baseDecimals) & OFFSET_MASK) << OFFSET_SHIFT);
}

/** The currency id a cell names, whatever its variant. */
export function cellCurrencyId(cell: number): number {
  return cell & ID_MASK;
}

/**
 * The stored form of a row's amounts that share one currency field (spec §8): one currency cell, in the canonical
 * variant chosen over all of them, and their amount cells at its scale. Refuses a row no narrow variant holds.
 */
export function encodeCells<C extends string>(
  currency: Currency<C>,
  amounts: readonly Amount<C>[],
): Result<StoredMoney, MoneyViolation> {
  const base = currency.baseDecimals;
  const runtime = currency.runtimeDecimals;
  if (runtime === base && currency.policy === 'base' && amounts.every((a) => typeof a === 'number')) {
    return ok({ cell: currency.id, values: amounts });
  }
  const vs = amounts.map(toBig);
  if (vs.every((v) => v === 0n)) return ok({ cell: currency.id, values: vs.map(() => 0) });
  const scale = currency.policy === 'base' ? baseScale(vs, base, runtime) : minimalScale(vs, base, runtime);
  if (scale === undefined) {
    return err({ code: 'OUT_OF_BACKING', op: 'encodeCells', currency: currency.code, amount: vs.join(',') });
  }
  const unit = pow10(runtime - scale);
  return ok({ cell: cellOf(currency, scale), values: vs.map((v) => canonical(v / unit)) });
}

/** Whether every value is a whole number of units at `scale` and fits 64 bits there. */
function holds(vs: readonly bigint[], runtime: number, scale: number): boolean {
  const unit = pow10(runtime - scale);
  return vs.every((v) => v % unit === 0n && fitsI64(v / unit));
}

function whole(vs: readonly bigint[], runtime: number, scale: number): boolean {
  const unit = pow10(runtime - scale);
  return vs.every((v) => v % unit === 0n);
}

/** Spec §8 `base`: the base scale when the row is whole there and fits; then coarser, then finer. */
function baseScale(vs: readonly bigint[], base: number, runtime: number): number | undefined {
  if (whole(vs, runtime, base)) {
    for (let s = base; s >= base + OFFSET_MIN && whole(vs, runtime, s); s--) {
      if (holds(vs, runtime, s)) return s;
    }
    return undefined;
  }
  for (let s = base + 1; s <= runtime; s++) {
    if (whole(vs, runtime, s)) return holds(vs, runtime, s) ? s : undefined;
  }
  return undefined;
}

/** Spec §8 `minimal`: the smallest scale down to `base − 32` at which the row is whole, if it fits. */
function minimalScale(vs: readonly bigint[], base: number, runtime: number): number | undefined {
  for (let s = base + OFFSET_MIN; s <= runtime; s++) {
    if (whole(vs, runtime, s)) return holds(vs, runtime, s) ? s : undefined;
  }
  return undefined;
}

/** The money value a stored pair holds, at its currency's current runtime scale: exact. */
export function decodeCell<Code extends string>(
  registry: CurrencyRegistry<Code>,
  cell: number,
  value: number | bigint,
): Result<Money<Code>, MoneyViolation> {
  if (cell < 0 || cell > 0x7fff_ffff || !Number.isInteger(cell)) {
    return err({ code: 'INVALID_CELL', op: 'decodeCell', cell, reason: 'not a u32 with bit 31 clear' });
  }
  if ((cell & WIDE_BIT) !== 0) {
    return err({ code: 'INVALID_CELL', op: 'decodeCell', cell, reason: 'the wide variant is not built' });
  }
  const found = registry.byId(cell & ID_MASK);
  if (!found.ok) return found;
  const currency = found.value;
  const raw = (cell >>> OFFSET_SHIFT) & OFFSET_MASK;
  const scale = currency.baseDecimals + (raw >= 32 ? raw - 64 : raw);
  if (scale > currency.runtimeDecimals) {
    return err({
      code: 'INVALID_CELL',
      op: 'decodeCell',
      cell,
      reason: `scale ${scale} is finer than ${currency.code}'s runtime scale`,
    });
  }
  return ok(brandMoney(currency.code, brandAmount(scaleUp(value, currency.runtimeDecimals - scale))));
}

/**
 * Per-currency totals from keyed sums over currency cells (spec §10): each variant group's sum is scaled to its
 * currency's runtime scale and the groups of one currency are added, exactly.
 */
export function combineVariantSums<Code extends string>(
  registry: CurrencyRegistry<Code>,
  sums: Iterable<readonly [cell: number, sum: number | bigint]>,
): Result<Money<Code>[], MoneyViolation> {
  const totals = new Map<number, Money<Code>>();
  for (const [cell, sum] of sums) {
    const part = decodeCell(registry, cell, sum);
    if (!part.ok) return part;
    const id = cell & ID_MASK;
    const before = totals.get(id);
    totals.set(
      id,
      before === undefined
        ? part.value
        : moneyOfCode<Code>(before.currency, addIntegers(before.amount, part.value.amount)),
    );
  }
  return ok([...totals.values()]);
}

/** A 64-bit cell read as two 32-bit words (little-endian: low word first), in its one spelling. */
export function int64FromWords(lo: number, hi: number): number | bigint {
  if ((hi > -0x20_0000 && hi < 0x20_0000) || (hi === -0x20_0000 && lo !== 0)) return hi * 0x1_0000_0000 + lo;
  return canonical(BigInt.asIntN(64, (BigInt(hi) << 32n) | BigInt(lo)));
}

/** Writes a 64-bit cell as two 32-bit words at `index` of a word view of the column (low word first). */
export function int64ToWords(value: number | bigint, words: Uint32Array, index: number): void {
  if (typeof value === 'number') {
    const hi = Math.floor(value / 0x1_0000_0000);
    words[2 * index] = value - hi * 0x1_0000_0000;
    words[2 * index + 1] = hi >>> 0;
    return;
  }
  words[2 * index] = Number(BigInt.asUintN(32, value));
  words[2 * index + 1] = Number(BigInt.asUintN(32, value >> 32n));
}
