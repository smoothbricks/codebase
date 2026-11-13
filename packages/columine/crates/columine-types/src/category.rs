//! The stream dictionary's image: the seam between a parse lane and whoever
//! owns a stream's dictionary values (specs/columine/10-physical-vocabulary.md
//! §5).
//!
//! An image is an ordered list of distinct texts. A value's ordinal is its
//! position; it never changes and is never reused. A parse lane asks the
//! image for each dictionary cell's ordinal and stores that; the writer reads
//! the values a batch added back out of it for the batch's dictionary
//! message. The image's owner decides its bounds and undoes a refused batch.

extern crate alloc;

use alloc::{boxed::Box, collections::BTreeMap, vec::Vec};
use core::fmt;

/// Why an image did not take a value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CategoryRefusal {
    /// The image holds `limit` values, its bound, and this one is new.
    Full { limit: u32 },
    /// The value is `bytes` long, past the `max` one value may be.
    TooLong { bytes: usize, max: usize },
}

impl fmt::Display for CategoryRefusal {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full { limit } => {
                write!(output, "the dictionary holds its limit of {limit} values")
            }
            Self::TooLong { bytes, max } => {
                write!(
                    output,
                    "a value of {bytes} bytes is past the {max} one value may be"
                )
            }
        }
    }
}

/// One stream's dictionary values, by ordinal.
pub trait CategoryImage {
    /// The ordinal of `value`. A value the image holds answers its ordinal
    /// and changes nothing; a new one is appended and answers the ordinal it
    /// was given.
    fn ordinal(&mut self, value: &str) -> Result<u32, CategoryRefusal>;

    /// How many values the image holds, which is the next ordinal.
    fn len(&self) -> u32;

    /// Whether the image holds no value.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The value at `ordinal`, `None` past the image's length.
    fn value(&self, ordinal: u32) -> Option<&str>;
}

/// The image of a stream that has no dictionary: it holds nothing and takes
/// nothing. A schema with a stream-dictionary column parsed against it
/// refuses the first value it meets.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NoCategories;

impl CategoryImage for NoCategories {
    fn ordinal(&mut self, _value: &str) -> Result<u32, CategoryRefusal> {
        Err(CategoryRefusal::Full { limit: 0 })
    }

    fn len(&self) -> u32 {
        0
    }

    fn value(&self, _ordinal: u32) -> Option<&str> {
        None
    }
}

/// An image held in memory: the values in ordinal order and a sorted index
/// over them, bounded by a count of values and a length per value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListImage {
    values: Vec<Box<str>>,
    ordinals: BTreeMap<Box<str>, u32>,
    limit: u32,
    max_value_bytes: usize,
}

impl ListImage {
    /// An empty image that takes at most `limit` values of at most
    /// `max_value_bytes` each.
    pub fn new(limit: u32, max_value_bytes: usize) -> Self {
        Self {
            values: Vec::new(),
            ordinals: BTreeMap::new(),
            limit,
            max_value_bytes,
        }
    }

    /// Drop every value from ordinal `mark` on: the undo of a batch that
    /// began when the image held `mark` values. An image no longer than
    /// `mark` is left as it is.
    pub fn truncate(&mut self, mark: u32) {
        for value in self.values.drain((mark as usize).min(self.values.len())..) {
            self.ordinals.remove(&value);
        }
    }
}

impl CategoryImage for ListImage {
    fn ordinal(&mut self, value: &str) -> Result<u32, CategoryRefusal> {
        if let Some(ordinal) = self.ordinals.get(value) {
            return Ok(*ordinal);
        }
        if value.len() > self.max_value_bytes {
            return Err(CategoryRefusal::TooLong {
                bytes: value.len(),
                max: self.max_value_bytes,
            });
        }
        let ordinal = self.len();
        if ordinal >= self.limit {
            return Err(CategoryRefusal::Full { limit: self.limit });
        }
        self.values.push(value.into());
        self.ordinals.insert(value.into(), ordinal);
        Ok(ordinal)
    }

    fn len(&self) -> u32 {
        self.values.len() as u32
    }

    fn value(&self, ordinal: u32) -> Option<&str> {
        self.values.get(ordinal as usize).map(|value| &**value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_held_value_answers_its_ordinal_and_changes_nothing() {
        let mut image = ListImage::new(8, 16);
        assert_eq!(image.ordinal("alpha"), Ok(0));
        assert_eq!(image.ordinal("beta"), Ok(1));
        let before = image.clone();
        assert_eq!(image.ordinal("alpha"), Ok(0));
        assert_eq!(image, before);
        assert_eq!(image.value(1), Some("beta"));
        assert_eq!(image.value(2), None);
    }

    #[test]
    fn an_ordinal_keeps_its_value_through_every_later_value() {
        let mut image = ListImage::new(64, 16);
        let mut given = Vec::new();
        for round in 0..3 {
            for name in ["a", "b", "c", "d", "a", "c"] {
                let value = alloc::format!("{name}{}", round % 2);
                given.push((image.ordinal(&value).unwrap(), value));
                for (ordinal, value) in &given {
                    assert_eq!(image.value(*ordinal), Some(value.as_str()));
                }
            }
        }
    }

    #[test]
    fn a_value_past_either_bound_is_refused_and_a_held_one_still_answers() {
        let mut image = ListImage::new(1, 4);
        assert_eq!(
            image.ordinal("toolong"),
            Err(CategoryRefusal::TooLong { bytes: 7, max: 4 })
        );
        assert_eq!(image.ordinal("one"), Ok(0));
        assert_eq!(
            image.ordinal("two"),
            Err(CategoryRefusal::Full { limit: 1 })
        );
        assert_eq!(image.ordinal("one"), Ok(0));
        assert_eq!(image.len(), 1);
    }

    #[test]
    fn truncating_drops_a_batch_and_twice_is_once() {
        let mut image = ListImage::new(8, 16);
        image.ordinal("kept").unwrap();
        let mark = image.len();
        image.ordinal("dropped").unwrap();
        image.truncate(mark);
        let once = image.clone();
        image.truncate(mark);
        assert_eq!(image, once);
        assert_eq!(image.len(), 1);
        assert_eq!(image.ordinal("dropped"), Ok(1));
    }

    #[test]
    fn no_categories_takes_nothing() {
        let mut image = NoCategories;
        assert_eq!(image.ordinal("x"), Err(CategoryRefusal::Full { limit: 0 }));
        assert!(image.is_empty());
        assert_eq!(image.value(0), None);
    }
}
