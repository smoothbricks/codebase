//! The text an interned lane holds: an identity, never content (specs/columine/09-row-interning.md §2).

/// The longest text an interned lane holds, in UTF-8 bytes.
pub const MAX_INTERNED_STR_BYTES: usize = 65_536;

/// Text at most [`MAX_INTERNED_STR_BYTES`] long: the only text [`Interner::intern`] takes, so content of any length is
/// refused where it is made into an identity, by a value, and never reaches a table.
///
/// [`Interner::intern`]: ../columine_vm/logical_export/trait.Interner.html
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct InternedStr<'a>(&'a str);

/// Why text is no interned identity: it is `bytes` long, past [`MAX_INTERNED_STR_BYTES`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TooLongToIntern {
    pub bytes: usize,
}

impl<'a> InternedStr<'a> {
    /// `text` as an interned identity, or why it is none.
    pub const fn new(text: &'a str) -> Result<Self, TooLongToIntern> {
        if text.len() > MAX_INTERNED_STR_BYTES {
            Err(TooLongToIntern { bytes: text.len() })
        } else {
            Ok(Self(text))
        }
    }

    /// The text.
    #[must_use]
    pub const fn as_str(self) -> &'a str {
        self.0
    }
}

impl<'a> TryFrom<&'a str> for InternedStr<'a> {
    type Error = TooLongToIntern;

    fn try_from(text: &'a str) -> Result<Self, Self::Error> {
        Self::new(text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn holds_text_up_to_the_ceiling_and_refuses_one_byte_more() {
        let at = "a".repeat(MAX_INTERNED_STR_BYTES);
        assert_eq!(
            InternedStr::new(&at).map(InternedStr::as_str),
            Ok(at.as_str())
        );
        let past = "a".repeat(MAX_INTERNED_STR_BYTES + 1);
        assert_eq!(
            InternedStr::new(&past),
            Err(TooLongToIntern {
                bytes: MAX_INTERNED_STR_BYTES + 1
            })
        );
    }
}
