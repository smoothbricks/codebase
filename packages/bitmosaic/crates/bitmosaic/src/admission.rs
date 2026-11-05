//! Allocation-free diagnostics for admitting a native compressed-set image.

/// The violated native-image discipline, shared by every language binding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdmissionClass {
    /// The three-byte BMS identification does not match.
    Magic,
    /// A referenced or derived byte extent is unavailable or overflows.
    Extent,
    /// An encoding version, root mode, width or container tag is unsupported.
    Kind,
    /// Cardinalities or their prefix planes disagree.
    Count,
    /// Ordered keys or members are not strictly ascending.
    MapOrder,
    /// A value or its derived metadata is outside the declared plane domain.
    PlaneRange,
    /// An unused packed bit is nonzero.
    BitPad,
    /// A reference or sample disagrees with its computed position.
    Placement,
}
impl AdmissionClass {
    /// Stable wire-refusal spelling; diagnostics never depend on enum debug names.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Magic => "magic",
            Self::Extent => "extent",
            Self::Kind => "kind",
            Self::Count => "count",
            Self::MapOrder => "map-order",
            Self::PlaneRange => "plane-range",
            Self::BitPad => "bit-pad",
            Self::Placement => "placement",
        }
    }
}
impl std::fmt::Display for AdmissionClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Why full admission refused an image, at a byte relative to that image's first byte.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AdmissionError {
    /// The stable violated discipline, not an opaque malformed-image bucket.
    pub class: AdmissionClass,
    /// The offending byte, or the available extent for a truncated field.
    pub offset: usize,
    /// A static explanation; producing an error allocates nothing.
    pub what: &'static str,
}
impl AdmissionError {
    pub(crate) const fn new(class: AdmissionClass, offset: usize, what: &'static str) -> Self {
        Self {
            class,
            offset,
            what,
        }
    }
    pub(crate) fn shifted(mut self, base: usize) -> Self {
        self.offset += base;
        self
    }
}
impl std::fmt::Display for AdmissionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "bitmosaic refused [{}]: {} at byte {}",
            self.class, self.what, self.offset
        )
    }
}
impl std::error::Error for AdmissionError {}
