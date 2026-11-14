# Money

## 1. Scope

`@smoothbricks/money` defines one money value: an exact integer amount of one currency, with its currency. It owns the
canonical currency list, the value types and their arithmetic, the one rounding step, FX conversion, decimal text and
the cell codec that stores a value in two columns. `@smoothbricks/arrow-builder` owns the field kinds that declare those
columns (§9); columine owns the reducer's sums over them (§10). Nothing here is a float, and nothing rounds except the
named rounding step (§6).

## 2. The currency list <a id="smoo/money!n/currency-list"></a>

One list, published by this package, names every currency any embedder may accept. An entry is plain data:

| field             | meaning                                                                                                    |
| ----------------- | ---------------------------------------------------------------------------------------------------------- |
| `id`              | a fiat currency's ISO 4217 numeric code; a crypto asset's id, from 1000, in this list's own range          |
| `code`            | the ISO 4217 alphabetic code, or the asset's ticker                                                        |
| `name`            | the currency's name                                                                                        |
| `type`            | `fiat` or `crypto`: for payment rails and the choice of FX rate source; no arithmetic reads it             |
| `minorDecimals`   | the display and settlement unit: USD 2, JPY 0, BTC 8                                                       |
| `baseDecimals`    | the scale of a cell whose offset is 0 (§8); every fiat 6, BTC 11, USDC 6, ETH 18                           |
| `runtimeDecimals` | the scale of a value in a program (§3); never below `baseDecimals`                                         |
| `wide`            | whether amounts at the base scale can pass 64 bits (ETH); such a currency's sums accumulate in 128         |
| `policy`          | the canonical variant rule of its cells, `base` or `minimal` (§8); ETH `minimal`, every other entry `base` |

The fiat entries are generated from the ISO 4217 list (list one); an ISO currency without numeric minor units (gold, the
SDR, the testing code) is not money of account and has no entry. The list is append-only: an `id`, `code`, `type`,
`baseDecimals` or `policy` never changes, an entry is never removed (an ISO withdrawal marks it `withdrawn`),
`runtimeDecimals` may only rise and `wide` may only turn on. The publishing step refuses every other change by name.
Raising `runtimeDecimals` subdivides a currency further: no stored cell changes (§8).

<a id="smoo/money!n/currency-subset"></a> An embedder declares the currencies it accepts, a subset of the list, with
`defineCurrencies(codes)`. The subset is a business rule, not an identity: it numbers nothing, and widening it changes
no stored value and no field's type. A code the list does not hold is a programmer error at definition.

## 3. Values <a id="smoo/money!n/amount-runtime"></a>

| type        | holds                                                                 | made by                                                   |
| ----------- | --------------------------------------------------------------------- | --------------------------------------------------------- |
| `Amount<C>` | the integer amount at C's `runtimeDecimals`                           | `money`, `amount`, `parseAmount`, `fromMinor`, arithmetic |
| `Minor<C>`  | the integer amount at C's `minorDecimals`                             | `toMinor` only                                            |
| `Basis<C>`  | a bigint at C's `basisDecimals`, `runtimeDecimals + 4`, for rate math | `toBasis`, `applyRate`                                    |
| `Money<C>`  | `{ currency, amount }`, a union discriminated by `currency`           | `money`, `decodeCell`                                     |

An `Amount` or a `Minor` is a JavaScript `number` while its integer is a safe integer and a `bigint` beyond: one
spelling per value, so `===` and map keys see equal amounts as equal. Arithmetic takes the number path while its result
is safe and widens to a bigint exactly when it is not; operators are never applied to amounts directly.

<a id="smoo/money!n/compile-time-currency"></a> The brands are invariant in the currency: an operation on two amounts
compiles only when both have the same single currency, so amounts of two currencies, or of a union of currencies, are
never added. A `Money` read from data, whose currency is a union, narrows by comparing its `currency` to a code.

<a id="smoo/money!n/checked-construction"></a> `money(registry, code, value)` is the one door from data: it refuses a
currency the registry does not accept (`UNKNOWN_CURRENCY`) and a value no stored form can hold (`OUT_OF_BACKING`, §8).
`amount(value)` makes an amount of a currency known in the type from an integer, refusing a non-integer `number` as a
programmer error. Operations that take currencies as data (`addMoney`, `sumMoney`, `compareMoney`) return
`CURRENCY_MISMATCH` as a value; `checkBalanced` returns the imbalance as a value. Every refusal names the operation, the
currency and the values involved.

The wire types (`@smoothbricks/money/wire`) carry an amount as exact decimal text and a currency as its code, with Typia
tags for the envelope's shape; `moneyFromWire` then judges the registry facts and the text.

## 4. Arithmetic <a id="smoo/money!n/arithmetic"></a>

Addition, subtraction, negation, multiplication by an integer, division with remainder
(`quotient × divisor + remainder = amount`), comparison and summation are exact. `allocateProportional` splits an amount
by integer weights with the largest-remainder method: its parts sum to the amount, and ties go to the earlier weight.

## 5. Identity <a id="smoo/money!n/identity"></a>

A value's identity, for equality, dedup keys and content hashes, is its currency `id` and its exact amount, written as
`identityText`: the decimal text with every trailing fraction zero removed. It does not depend on any scale, so raising
a currency's `runtimeDecimals` changes no identity.

## 6. Rounding <a id="smoo/money!n/rounding"></a>

Rounding happens in `toMinor`, `roundBasis` and `convertFx` only, with a named mode: `Floor`, `Ceil`, `TowardZero`,
`HalfUp` (half away from zero) or `HalfEven`. Each returns its remainder beside its value, so no residual is dropped.
`fromMinor` and `toBasis` are exact.

## 7. FX <a id="smoo/money!n/fx"></a>

An `FxRate<From, To>` is `{ from, to, basis, scale }`: `basis / 10^scale` units of `To` per one unit of `From`.
`convertFx(amount, rate, from, to, mode)` multiplies exactly, rounds once to `To`'s `runtimeDecimals` with the mode, and
returns the result, the remainder as an exact decimal (`{ units, scale }` in `To`), and the audit of its inputs. Two
currencies' scales meet nowhere else.

## 8. The cell <a id="smoo/money!n/storage-cell"></a>

A stored money value is two cells: a currency cell and a 64-bit amount cell. The currency cell is a `u32` that declares
the variant the amount is written in:

| bits  | meaning                                                                                      |
| ----- | -------------------------------------------------------------------------------------------- |
| 0–23  | the currency `id`                                                                            |
| 24–29 | the scale `s` of the amount cell, as the signed offset `s − baseDecimals` (two's complement) |
| 30    | wide: the amount's high 64 bits live outside the amount cell                                 |
| 31    | zero                                                                                         |

A value at the base scale whose integer fits 64 bits has offset 0 and is not wide, so its currency cell is the plain
`id`: every fiat, USDC and BTC value, as the list stands, is written so.

**The canonical variant.** Several money fields of a row may share one currency field, so one currency cell declares the
scale of every amount cell of that row that names it. `encodeCells` chooses it over all of them, by the currency's
`policy`. With `v` ranging over the row's exact amounts, `B` the currency's `baseDecimals` and `R` its
`runtimeDecimals`, a "whole" scale is one at which every `v` is a whole number of units, a row of zeros is written at
`B`, and otherwise:

- `base`, for currencies whose amounts at their base scale fit 64 bits (every fiat, USDC, BTC):
  1. if `v` is a whole number of units at `B` and that integer fits 64 bits, the variant is `B`;
  2. else if `v` is a whole number of units at `B`, the variant is the largest scale `s < B` at which `v` is a whole
     number of units that fits 64 bits;
  3. else the variant is the smallest scale `s` in `(B, R]` at which `v` is a whole number of units, if that integer
     fits 64 bits.
- `minimal`, for currencies whose base-scale integers are rarely safe JavaScript numbers (ETH): the variant is the
  smallest scale `s` in `[B − 32, R]` at which `v` is a whole number of units, if that integer fits 64 bits. So 200 ETH
  is 2 at scale −2, half an ETH is 5 at scale 1, and a round amount of 10^15 ETH or more is written at `B − 32`.
- Otherwise the value is wide.

A currency has exactly one policy, frozen with its base scale, and each policy's candidates are totally ordered with the
first that fits deciding, so equal rows of one currency have equal cells. For a row of one money field this is the
variant of its one amount; identity (§5) never depends on the cell. The wide variant is reserved and specified: its low
64 bits are the amount cell and its high 64 bits are the money field's high-word column. It is not built: `encodeCells`
refuses a row no narrow variant holds (`OUT_OF_BACKING`, naming the currency and the amounts), and `decodeCell` refuses
a wide cell.

Because the cell is the row's, so are the wide flag and the refusal: one amount that fits no narrow variant at the row's
scale makes the row wide, refused while the wide variant is not built. [10^9 ETH, 1 wei] cannot share a narrow cell: the
wei needs scale 18, at which 10^9 ETH is 10^27 units. A schema gives a money field its own currency field when its
amounts are of another magnitude than its siblings'.

`decodeCell(registry, cell, value)` answers the `Money` whose amount is `value × 10^(R − s)` at the currency's current
`runtimeDecimals`, exactly. Raising `runtimeDecimals` is therefore a subdivision without a migration: every stored cell
keeps its meaning, because it states its own scale. A cell with a scale above `R` or below 0, an id the registry does
not accept, or bit 31 set is refused (`INVALID_CELL`).

## 9. Columns <a id="smoo/money!n/storage-columns"></a>

`S.currency(registry)` is a required `UInt32` column of currency cells; `S.money('<currency field>')` is an `Int64`
column of amount cells whose Arrow field carries the extension type `ARROW:extension:name = smoothbricks.money` and
`ARROW:extension:metadata = {"currency":"<currency column>"}`. Several money fields may share one currency field. A
schema whose money field names no currency field, a field of another kind, or an optional currency field is refused when
it is defined. A money field's column type never depends on the accepted currencies.

## 10. Summing in a reducer <a id="smoo/money!n/reducer-sums"></a>

A sum over a money column is keyed by its currency column's cell, so every row of one key has one scale and the sum
aligns nothing; a currency's total combines its variant groups at the end (`combineVariantSums`). An i64 sum refuses on
overflow and never wraps. A currency that is not `wide` (every fiat, USDC, BTC) sums in 64 bits; a `wide` one needs a
128-bit accumulator over the same 64-bit cells.

Because the cell is the row's, the same amount of one field may sit under two keys in two rows (200 ETH is 2 at scale −2
alone and 2000 at scale 1 beside half an ETH); totals combine every group of a currency at its runtime scale, so they
are the same either way.

## 11. Behavioral acceptance

`packages/money` tests cover, each fenced with the id of its section: the list's fields for USD, JPY, BHD, BTC, USDC and
ETH and a publish run keeping a withdrawn entry and refusing a changed code, type, base scale or policy, a lowered
`runtimeDecimals` and a `wide` turned off; a subset refusing an unknown code; amounts of two currencies, and of a union,
failing to compile, and narrowing by `currency`; arithmetic staying a number while safe and widening exactly past 2^53;
each refusal of `money`, `parseAmount` and the cell codec; every rounding mode with its remainder; `convertFx` turning
1.00 USD at 0.92 into 0.92 EUR; `allocateProportional` summing to its total; `identityText` equal across a raised
`runtimeDecimals`; the canonical variant for a base value, a large round ETH amount, a finer BTC amount after a raise,
and a wide ETH amount; and a cell round trip for every variant.
