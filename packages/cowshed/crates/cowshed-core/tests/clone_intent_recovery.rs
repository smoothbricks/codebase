//! The lifecycle intent of `new` and `fork` against the production runtime on scratch APFS: a
//! clone refused before its first durable mutation leaves nothing a later store-wide recovery
//! (`gc`) can turn into a workspace, while one that crossed the mutation fence — or that an
//! older binary left with durable artifacts — still resumes.

#[cfg(target_os = "macos")]
#[path = "clone_intent_recovery/macos.rs"]
mod macos;
