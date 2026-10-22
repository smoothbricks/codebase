//! Reader projection (07_api.md, "DTO freeze", "Reader projection"): writers are strict and readers
//! project. A client reads a
//! peer controller's result or event through the declaration it was built from. Every object key
//! that declaration does not name is dropped, at every depth, and the strict decode of the
//! declared result then certifies what is left. A missing field, a retyped value and an unknown
//! variant are not projected: they still refuse by name.
//!
//! The readers are data generated from the same declarations as every other projection
//! (`reader.generated.rs`); this module only walks them.

use serde_json::{Map, Value};

#[path = "reader.generated.rs"]
mod generated;

/// The members of one closed object: each wire name with the reader of its value.
pub type Fields = &'static [(&'static str, &'static Reader)];

/// How a client projects one declared wire value.
#[derive(Debug)]
pub enum Reader {
    /// A value with no declared object inside it: a scalar, or a scalar wire projection.
    Leaf,
    /// An array of one element type.
    Seq(&'static Reader),
    /// An object keyed by data, every value of one type: no key is dropped.
    Map(&'static Reader),
    /// A fixed-length array, one reader per position.
    Tuple(&'static [&'static Reader]),
    /// An object of declared members.
    Record(Fields),
    /// An externally tagged enum: a unit variant's name, or an object of one variant name.
    External(&'static [(&'static str, &'static Reader)]),
    /// An internally tagged enum: the tag's value names the arm whose object reader reads the
    /// members beside it, a `Record` or an `Untagged`.
    Internal {
        tag: &'static str,
        arms: &'static [(&'static str, &'static Reader)],
    },
    /// An adjacently tagged enum: the tag's value names the arm the content holds.
    Adjacent {
        tag: &'static str,
        content: &'static str,
        arms: &'static [(&'static str, &'static Reader)],
    },
    /// An untagged enum's object arms, in declaration order.
    Untagged(&'static [Fields]),
}

/// A result or event a client reads from a peer: the reader generated for its declaration.
pub trait Projected {
    const READER: &'static Reader;
}

impl<T: Projected> Projected for Vec<T> {
    const READER: &'static Reader = &Reader::Seq(T::READER);
}

/// `null` holds no object, so an absent result projects as its present one would.
impl<T: Projected> Projected for Option<T> {
    const READER: &'static Reader = T::READER;
}

/// Drops from `value` every object key `reader` does not declare. A value whose shape is not the
/// declared one is left as it is, for the strict decode to name.
pub fn project(value: &mut Value, reader: &Reader) {
    match (reader, value) {
        (Reader::Seq(element), Value::Array(elements)) => {
            for element_value in elements {
                project(element_value, element);
            }
        }
        (Reader::Map(element), Value::Object(members)) => {
            for member in members.values_mut() {
                project(member, element);
            }
        }
        (Reader::Tuple(positions), Value::Array(elements)) => {
            for (position, element) in positions.iter().zip(elements) {
                project(element, position);
            }
        }
        (Reader::External(arms), Value::Object(members)) if members.len() == 1 => {
            for (name, content) in members.iter_mut() {
                if let Some(reader) = arm(arms, name) {
                    project(content, reader);
                }
            }
        }
        (Reader::Internal { tag, arms }, Value::Object(members)) => {
            if let Some(reader) = tag_of(members, tag).and_then(|name| arm(arms, name)) {
                object(members, reader, Some(*tag));
            }
        }
        (Reader::Adjacent { tag, content, arms }, Value::Object(members)) => {
            if let Some(reader) = tag_of(members, tag).and_then(|name| arm(arms, name)) {
                members.retain(|key, _| key == tag || key == content);
                if let Some(value) = members.get_mut(*content) {
                    project(value, reader);
                }
            }
        }
        (Reader::Record(_) | Reader::Untagged(_), Value::Object(members)) => {
            object(members, reader, None);
        }
        // A leaf holds nothing to drop; any other pairing is a shape the strict decode refuses.
        _ => {}
    }
}

/// Projects an object through a `Record` or an `Untagged` reader, keeping an enclosing enum's
/// `tag` beside the members.
fn object(members: &mut Map<String, Value>, reader: &Reader, tag: Option<&str>) {
    match reader {
        Reader::Record(fields) => retain(members, fields, tag),
        Reader::Untagged(arms) => {
            // The arm is the first that declares every key the object shares with any arm; a key
            // no arm declares (the tag among them) is the newer writer's, and selects nothing.
            let declared = |key: &str| arms.iter().any(|&fields| arm(fields, key).is_some());
            let chosen = arms.iter().copied().find(|&fields| {
                members
                    .keys()
                    .all(|key| !declared(key) || arm(fields, key).is_some())
            });
            if let Some(fields) = chosen {
                retain(members, fields, tag);
            }
        }
        // The generator gives an object position no other reader.
        _ => {}
    }
}

/// Keeps the declared members, each projected through its reader, and the enum tag beside them.
fn retain(members: &mut Map<String, Value>, fields: Fields, tag: Option<&str>) {
    members.retain(|key, value| match arm(fields, key) {
        Some(reader) => {
            project(value, reader);
            true
        }
        None => tag == Some(key.as_str()),
    });
}

fn arm<T: Copy>(arms: &'static [(&'static str, T)], name: &str) -> Option<T> {
    arms.iter()
        .find(|(arm, _)| *arm == name)
        .map(|(_, reader)| *reader)
}

fn tag_of<'a>(members: &'a Map<String, Value>, tag: &str) -> Option<&'a str> {
    members.get(tag).and_then(Value::as_str)
}
