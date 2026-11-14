# columine: a parsed integer wider than 64 bits has no view

The MessagePack bigint extension's view (`MsgpackValue::BigInt` in
`packages/columine/crates/columine-parsing/src/validate.rs`) answers `as_number` exactly for a value in
`-(2^63) ..= 2^64 - 1` (`Signed` within `i64`, `Unsigned` above it), and the integer markers and JSON integer literals
already read that whole range. A magnitude past 64 bits (`2^64`, `-(2^63) - 1`, a 128-bit value) answers `None`, and a
consumer that stores dynamic values can only refuse the cell.

- Red first: a dynamic member holding `2^64`, `-(2^63) - 1` and a 128-bit value reads back exactly through the view, in
  the MessagePack and the JSON extractor.
- The view gains an accessor that carries the full value (an `i128`/`u128` pair or sign and magnitude bytes);
  `as_number` keeps its meaning for values inside 64 bits.
- No silent narrowing: a consumer that cannot store the width refuses by name with the member's path.
