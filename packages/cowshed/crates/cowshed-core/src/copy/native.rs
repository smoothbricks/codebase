#[cfg(not(target_os = "macos"))]
mod linux;
#[cfg(target_os = "macos")]
mod macos;

#[cfg(not(target_os = "macos"))]
pub use linux::{copy_leaf, directory_metadata};
#[cfg(target_os = "macos")]
pub use macos::{copy_leaf, directory_metadata};
