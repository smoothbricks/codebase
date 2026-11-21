//! What the root helper and its unprivileged caller share: dataset names with the containment
//! rule, and the kernel's answer to "which dataset is mounted here". The binary's own policy
//! (verbs, properties, mountpoints) stays in the binary.

pub mod host;
pub mod name;
