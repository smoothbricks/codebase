//! What a workspace image is on disk: its one file extension and its capacity.

use std::error::Error;
use std::ffi::OsStr;
use std::fmt;
use std::path::Path;

/// Every workspace image is a sparse RAW file holding one case-sensitive APFS volume
/// (specs/cowshed/01a_apfs.md, "Images"): one extension, no alias, nothing inferred from
/// anything else.
pub const IMAGE_EXTENSION: &str = "dmg";

/// Whether `path` names `<stem>.dmg`.
pub fn is_image_path(path: &Path) -> bool {
    path.extension() == Some(OsStr::new(IMAGE_EXTENSION))
}

/// A disk-image capacity, held as an exact byte count.
///
/// Capacities are spelled `100g`, `200g`, `1t` on the command line and the units are binary,
/// which is what every image cowshed has ever created is sized in. `diskutil`'s image verbs read
/// the same letters as decimal SI, so cowshed hands it no unit — it resolves the letters here once
/// and passes the byte count, the only spelling a resize can be verified against afterwards.
///
/// The smallest unit accepted from a caller is a mebibyte, which keeps every requested capacity
/// a whole number of the 4 KiB blocks a resize rounds to. A request that had to be
/// rounded could not be checked against what the image reports back.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ImageCapacity(u64);

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ImageCapacityError {
    Malformed(String),
    TooSmall(String),
    Unaligned(String),
}

impl fmt::Display for ImageCapacityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed(value) => write!(
                formatter,
                "capacity `{value}` is not a positive whole number with an optional m, g, or t unit"
            ),
            Self::TooSmall(value) => write!(
                formatter,
                "capacity `{value}` is below the one mebibyte minimum image capacity"
            ),
            Self::Unaligned(value) => write!(
                formatter,
                "capacity `{value}` is not a multiple of the 4 KiB block the image tools resize in"
            ),
        }
    }
}

impl Error for ImageCapacityError {}

impl ImageCapacity {
    pub const KIBIBYTE: u64 = 1024;
    pub const MEBIBYTE: u64 = 1024 * Self::KIBIBYTE;
    pub const GIBIBYTE: u64 = 1024 * Self::MEBIBYTE;
    pub const TEBIBYTE: u64 = 1024 * Self::GIBIBYTE;
    /// The block size `diskutil image resize` rounds a request to.
    const BLOCK: u64 = 4 * Self::KIBIBYTE;

    pub const fn from_gibibytes(count: u64) -> Self {
        Self(count * Self::GIBIBYTE)
    }

    /// A capacity observed from a tool, which reports whatever the image actually holds.
    pub const fn from_bytes(bytes: u64) -> Self {
        Self(bytes)
    }

    pub const fn bytes(self) -> u64 {
        self.0
    }

    pub fn parse(value: &str) -> Result<Self, ImageCapacityError> {
        let text = value.trim();
        let malformed = || ImageCapacityError::Malformed(value.to_owned());
        let (digits, unit) = match text.as_bytes().last() {
            Some(byte) if byte.is_ascii_digit() => (text, 1),
            Some(b'm' | b'M') => (&text[..text.len() - 1], Self::MEBIBYTE),
            Some(b'g' | b'G') => (&text[..text.len() - 1], Self::GIBIBYTE),
            Some(b't' | b'T') => (&text[..text.len() - 1], Self::TEBIBYTE),
            _ => return Err(malformed()),
        };
        let count: u64 = digits.parse().map_err(|_| malformed())?;
        let bytes = count.checked_mul(unit).ok_or_else(malformed)?;
        if bytes < Self::MEBIBYTE {
            return Err(ImageCapacityError::TooSmall(value.to_owned()));
        }
        if !bytes.is_multiple_of(Self::BLOCK) {
            return Err(ImageCapacityError::Unaligned(value.to_owned()));
        }
        Ok(Self(bytes))
    }
}

/// Renders the capacity in the largest binary unit that divides it exactly, so a capacity parsed
/// from `100g` prints as `100g`. A capacity observed from a tool that lands on no whole unit
/// prints as its byte count, which is what the tools themselves were given.
impl fmt::Display for ImageCapacity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (unit, suffix) in [
            (Self::TEBIBYTE, "t"),
            (Self::GIBIBYTE, "g"),
            (Self::MEBIBYTE, "m"),
        ] {
            if self.0 >= unit && self.0.is_multiple_of(unit) {
                return write!(formatter, "{}{suffix}", self.0 / unit);
            }
        }
        write!(formatter, "{}", self.0)
    }
}
