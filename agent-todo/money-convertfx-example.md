# money: `convertFx`'s documented example is wrong

`packages/money/src/fx.ts:45-63` documents 100 USD cents at `Basis<'EUR'>(9200000000n)` becoming `Amount<'EUR'>(92n)`.
The function returns `9200n`, because its rate is per minor unit of the source currency (0.92 EUR per cent).

Red first: a test that runs the documented example and asserts the documented result.

Fix: decide the rate's unit. Per whole unit of the source currency with an explicit scale reads as people quote rates;
until that is decided, correct the example to the function's contract.
