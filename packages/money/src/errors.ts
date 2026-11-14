/**
 * Why a money operation refused. Every refusal names its operation and the values involved; amounts are written as
 * exact decimal integers of their stored units, so a refusal serializes as it is.
 */
export type MoneyViolation =
  /** The currency is not in the list, or not in the registry's accepted subset. */
  | { readonly code: 'UNKNOWN_CURRENCY'; readonly op: string; readonly currency: string | number }
  /** No narrow cell holds the amount (spec §8): the wide variant is not built. */
  | { readonly code: 'OUT_OF_BACKING'; readonly op: string; readonly currency: string; readonly amount: string }
  | { readonly code: 'CURRENCY_MISMATCH'; readonly op: string; readonly left: string; readonly right: string }
  /** Decimal text with a nonzero digit past the currency's runtime scale. */
  | {
      readonly code: 'FINER_THAN_RUNTIME';
      readonly op: string;
      readonly currency: string;
      readonly text: string;
      readonly runtimeDecimals: number;
    }
  | { readonly code: 'INVALID_TEXT'; readonly op: string; readonly text: string }
  | { readonly code: 'UNBALANCED'; readonly op: string; readonly currency: string; readonly imbalance: string }
  | { readonly code: 'INVALID_CELL'; readonly op: string; readonly cell: number; readonly reason: string };
