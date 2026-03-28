/**
 * Branded type for sorted arrays.
 *
 * Ensures sorting happens exactly once and the type system enforces it.
 * Used for Arrow dictionary columns that require sorted values (enum, category).
 */

declare const sortedArrayBrand: unique symbol;

/** Branded type for arrays that have been sorted */
export type SortedArray<T> = readonly T[] & { readonly [sortedArrayBrand]: true };

function brandSortedArray<T>(arr: T[]): SortedArray<T>;
function brandSortedArray<T>(arr: T[]): readonly T[] {
  return arr;
}

/**
 * Sort an array in-place and return it as a branded SortedArray.
 * @param arr - Array to sort (mutated in-place)
 * @param compare - Comparison function
 */
export function sortInPlace<T>(arr: T[], compare: (a: T, b: T) => number): SortedArray<T> {
  arr.sort(compare);
  return brandSortedArray(arr);
}

/**
 * String comparison function for sorting.
 */
export function compareStrings(a: string, b: string): number {
  return a < b ? -1 : a > b ? 1 : 0;
}
