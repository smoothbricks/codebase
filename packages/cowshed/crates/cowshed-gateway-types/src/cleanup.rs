//! Nudging the gateway's cleanup (05_gateway.md, "Deleting released images").
//!
//! A release is one rename into a `.deleting` directory, and those directories are the cleanup's
//! queue. A releaser makes its rename durable, then writes one `{"op":"released"}` line and
//! half-closes; the gateway answers `{"ok":true}` and lists its store again. The line names
//! nothing the listing does not.

/// The control operation a releaser nudges the cleanup with.
pub const RELEASED_OP: &str = "released";
