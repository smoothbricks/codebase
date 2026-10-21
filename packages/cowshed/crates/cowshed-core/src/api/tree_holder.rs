//! The wire shape of one holder of a directory tree, as `cowshed tree-holders` prints it
//! (02_workspaces.md "Holders cowshed does not run"); the generated TypeScript validator reads it.
//! Every field borrows the census's own record, so a line is written without a copy.

use std::os::unix::ffi::OsStrExt as _;
use std::path::Path;

use serde::{Serialize, Serializer};

use crate::build_volume::nx::{Hold, TreeHold};

/// How a process holds the tree: the first hold the census found, cheapest first.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum TreeHoldKind {
    /// Its working directory is in the tree.
    WorkingDirectory,
    /// Its root directory (`chroot`) is in the tree.
    RootDirectory,
    /// It runs an executable from the tree.
    Executable,
    /// It has a file of the tree open.
    OpenFile,
    /// It maps a file of the tree (Linux only: Darwin's census reads no mappings).
    MappedFile,
}

/// A path inside the tree, exactly as the kernel names it: its bytes as tagged bytes, `utf8` when
/// they are UTF-8 and canonical standard base64 otherwise, so no path is lost or refused.
#[cfg_attr(any(), cowshed_api(wire = "TaggedBytesRef"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TreePath<'a>(&'a Path);

impl Serialize for TreePath<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        super::dto::serialize_tagged_bytes(self.0.as_os_str().as_bytes(), serializer)
    }
}

/// One process holding something inside a directory tree: one line of `cowshed tree-holders`, its
/// fields in the order a reader matches them, the pid and how it holds the tree first. The pid
/// names the process; the path is exact; the command is for a person to read.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TreeHolder<'a> {
    pub pid: i32,
    pub hold: TreeHoldKind,
    pub path: TreePath<'a>,
    /// Its argument vector joined by spaces, each argument decoded lossily (or its executable's
    /// path, or `command unreadable`): never an identity.
    pub command: &'a str,
}

impl<'a> From<&'a TreeHold> for TreeHolder<'a> {
    fn from(held: &'a TreeHold) -> Self {
        let (hold, path) = match &held.hold {
            Hold::WorkingDirectory(path) => (TreeHoldKind::WorkingDirectory, path),
            Hold::RootDirectory(path) => (TreeHoldKind::RootDirectory, path),
            Hold::Executable(path) => (TreeHoldKind::Executable, path),
            Hold::OpenFile(path) => (TreeHoldKind::OpenFile, path),
            Hold::MappedFile(path) => (TreeHoldKind::MappedFile, path),
        };
        Self {
            pid: held.holder.pid,
            hold,
            path: TreePath(path),
            command: &held.holder.command,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;
    use std::path::PathBuf;

    use base64::Engine as _;

    use cowshed_os::process::Holder;

    use super::*;

    fn held(hold: Hold) -> TreeHold {
        TreeHold {
            holder: Holder {
                pid: 4242,
                command: "nx run-many -t test".to_owned(),
            },
            hold,
        }
    }

    /// A holder is one line whose fields come in the order a shell reader matches them, every
    /// hold its own kebab-case kind, and a UTF-8 path its own text under the `utf8` tag.
    #[test]
    fn a_holder_line_names_its_pid_and_hold_first_and_its_path_as_tagged_text() {
        let cases = [
            (
                Hold::WorkingDirectory(PathBuf::from("/t/a\nb")),
                "working-directory",
            ),
            (Hold::RootDirectory(PathBuf::from("/t/r")), "root-directory"),
            (Hold::Executable(PathBuf::from("/t/x")), "executable"),
            (Hold::OpenFile(PathBuf::from("/t/f")), "open-file"),
            (Hold::MappedFile(PathBuf::from("/t/m")), "mapped-file"),
        ];
        for (hold, kind) in cases {
            let path = serde_json::to_string(hold.path().to_str().expect("a UTF-8 fixture path"))
                .expect("a JSON string");
            let tree_hold = held(hold);
            let wire = serde_json::to_string(&TreeHolder::from(&tree_hold)).expect("serialize");
            assert_eq!(
                wire,
                format!(
                    r#"{{"pid":4242,"hold":"{kind}","path":{{"encoding":"utf8","data":{path}}},"command":"nx run-many -t test"}}"#
                ),
            );
            assert!(!wire.contains('\n'), "{wire}");
        }
    }

    /// A path that is not UTF-8 goes out as the canonical base64 of its exact bytes, which decode
    /// back to them; nothing about it fails the line.
    #[test]
    fn a_path_that_is_not_utf8_goes_out_as_its_exact_bytes() {
        let bytes = b"/t/shed-\xff\xfe/x";
        let tree_hold = held(Hold::OpenFile(PathBuf::from(OsStr::from_bytes(bytes))));
        let wire: serde_json::Value =
            serde_json::to_value(TreeHolder::from(&tree_hold)).expect("serialize");
        assert_eq!(wire["path"]["encoding"], "base64", "{wire}");
        let data = wire["path"]["data"].as_str().expect("base64 text");
        assert_eq!(
            base64::engine::general_purpose::STANDARD
                .decode(data)
                .expect("canonical base64"),
            bytes
        );
        assert_eq!(wire["hold"], "open-file", "{wire}");
    }
}
