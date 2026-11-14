# columine: a parsed integer outside `i64` has no view

`packages/columine/crates/columine-parsing/src/validate.rs:2708` holds a MessagePack big-integer extension as
`BigInt(Option<i64>)`, filled at `:2849` from `msgpack_scanner.rs:416` (`bigint_ext_i64`), which answers `None` for a
value outside `i64`. `ValueView::as_number` is then `None`, and a consumer that stores dynamic values cannot read a
`u64` above `i64::MAX` or any wider integer: it can only refuse the cell.

- Red first: a dynamic member holding `2^64 - 1`, `-(2^63) - 1` and a 128-bit value reads back exactly through the view,
  in the MessagePack and the JSON extractor.
- The view gains an accessor that carries the full value (an `i128`/`u128` pair or sign and magnitude bytes);
  `as_number` keeps its meaning for values inside `i64`.
- No silent narrowing: a consumer that cannot store the width refuses by name with the member's path.
