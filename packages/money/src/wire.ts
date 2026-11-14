/**
 * The money wire (specs/money/00-money.md §3): a currency code and exact decimal text, with Typia tags for the
 * envelope's shape. A consumer validates the shape with code Typia generates from these types, then
 * `moneyFromWire(registry, wire)` judges what Typia cannot see: the accepted currencies and the text's scale.
 */

import type { tags } from 'typia';

/** Exact decimal text: an optional minus, an integer part without leading zeros, an optional fraction. */
export type AmountText = string & tags.Pattern<'^-?(0|[1-9][0-9]*)(\\.[0-9]+)?$'>;

/** A currency code of the list: ISO 4217 alphabetic, or a crypto asset's ticker. */
export type CurrencyCodeText = string & tags.Pattern<'^[A-Z0-9]{3,12}$'>;

export interface MoneyWire {
  readonly currency: CurrencyCodeText;
  readonly amount: AmountText;
}
