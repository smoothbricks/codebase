//! The read-only value model a schema judges and a value cell is written
//! from: one view over JSON, MessagePack and native values.

use core::fmt;
/// Semantic kinds understood by the signal schema tree and value adapters.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum ValueKind {
    Unknown,
    Null,
    String,
    Number,
    Boolean,
    Binary,
    BigInt,
    Array,
    Object,
    /// An exact signed integer from a canonical codec that does not retain the
    /// originating host's number/bigint distinction. Never a JSON number token.
    CanonicalInt64,
}

impl ValueKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Null => "null",
            Self::String => "string",
            Self::Number => "number",
            Self::Boolean => "boolean",
            Self::Binary => "binary",
            Self::BigInt => "bigint",
            Self::Array => "array",
            Self::Object => "object",
            Self::CanonicalInt64 => "canonical int64",
        }
    }
}

impl fmt::Display for ValueKind {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output.write_str(self.as_str())
    }
}

/// Exact numeric forms exposed by a value adapter.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum NumberValue {
    Signed(i128),
    Unsigned(u128),
    Float(f64),
}

/// Read-only value-model seam for schema validation.
///
/// Object and array traversal is callback-based so implementations can expose
/// borrowed values without allocating or requiring a common owned tree. A
/// runtime may implement only the accessors relevant to its [`ValueKind`]. The
/// default accessors are intentionally empty: a malformed adapter is rejected
/// as a type/shape violation rather than causing a panic. Reporting
/// [`ValueKind::CanonicalInt64`] without answering
/// [`ValueView::as_canonical_i64`] is such an adapter: the kind alone proves no
/// value, and no numeric schema accepts it.
pub trait ValueView {
    fn kind(&self) -> ValueKind;

    fn as_str(&self) -> Option<&str> {
        None
    }

    /// The exact number of a [`ValueKind::Number`], or the exact integer of a
    /// [`ValueKind::BigInt`] whose adapter carries one; a bounded `bigint`
    /// refuses a bigint that does not answer.
    fn as_number(&self) -> Option<NumberValue> {
        None
    }

    /// The exact signed payload witnessing [`ValueKind::CanonicalInt64`].
    /// Adapters must not reconstruct it from a floating-point value or infer
    /// this representation from an ordinary JSON/MessagePack number token.
    fn as_canonical_i64(&self) -> Option<i64> {
        None
    }

    fn as_bytes(&self) -> Option<&[u8]> {
        None
    }

    fn object_field(&self, _name: &str) -> Option<&dyn ValueView> {
        None
    }

    fn visit_object(&self, _visit: &mut dyn FnMut(&str, &dyn ValueView)) {}

    fn visit_array(&self, _visit: &mut dyn FnMut(usize, &dyn ValueView)) {}
}
