//! Handing a released image to the gateway's cleanup (05_gateway.md, "Deleting released images").
//!
//! A releaser renames an image into its `.deleting` directory, makes the rename durable, and then
//! writes one [`ReleasedRequest`] line and half-closes; the gateway answers `{"ok":true}` once the
//! entry is queued. The counts are what the releaser knew of the image, when it knew them; they
//! order the queue (`cowshed_core::storage::apfs::cleanup_order`).

use std::num::NonZeroU64;
use std::path::Path;

use serde::Serialize;

/// The control operation a releaser hands an entry over with.
pub const RELEASED_OP: &str = "released";

/// What a releaser knew of the image it released: the bytes it does not share with its source,
/// what freeing it returns, and its physical runs, what freeing it costs. Either is `None` when
/// unknown.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReleaseRecord {
    pub unique_bytes: Option<u64>,
    pub extents: Option<NonZeroU64>,
}

/// `{"op":"released","entry":"<absolute path>","uniqueBytes":<n>?,"extents":<n>?}`.
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReleasedRequest<'a> {
    op: &'static str,
    pub entry: &'a Path,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unique_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extents: Option<NonZeroU64>,
}

impl<'a> ReleasedRequest<'a> {
    pub const fn new(entry: &'a Path, record: ReleaseRecord) -> Self {
        Self {
            op: RELEASED_OP,
            entry,
            unique_bytes: record.unique_bytes,
            extents: record.extents,
        }
    }
}
