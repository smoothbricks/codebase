/**
 * Exact integer helpers shared by the money modules. An integer amount is a `number` while it is a safe integer and
 * a `bigint` beyond: one spelling per value.
 */

export const SAFE = Number.MAX_SAFE_INTEGER;
const SAFE_N = BigInt(SAFE);
const I64_MIN = -(2n ** 63n);
const I64_MAX = 2n ** 63n - 1n;

export function isSafe(value: number): boolean {
  return value <= SAFE && value >= -SAFE;
}

/** The one spelling of an integer: a number while safe, a bigint beyond. */
export function canonical(value: bigint): number | bigint {
  return value <= SAFE_N && value >= -SAFE_N ? Number(value) : value;
}

export function toBig(value: number | bigint): bigint {
  return typeof value === 'bigint' ? value : BigInt(value);
}

export function fitsI64(value: bigint): boolean {
  return value >= I64_MIN && value <= I64_MAX;
}

/**
 * An integer handed in by typed code, in its one spelling.
 * @throws TypeError for a number that is not an integer: a programmer error, never data (data goes through `money`).
 */
export function integer(value: number | bigint, op: string): number | bigint {
  if (typeof value === 'bigint') return canonical(value);
  if (!Number.isInteger(value)) throw new TypeError(`${op}: ${value} is not an integer`);
  return isSafe(value) ? value : canonical(BigInt(value));
}

const POW10_N: bigint[] = [1n];
/** 10^k as a bigint; k is a scale difference, never negative. */
export function pow10(k: number): bigint {
  for (let i = POW10_N.length; i <= k; i++) POW10_N[i] = POW10_N[i - 1] * 10n;
  return POW10_N[k];
}
