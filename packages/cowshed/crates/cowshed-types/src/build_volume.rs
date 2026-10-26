//! Build-volume vocabulary shared below storage: the capacity a volume created from
//! nothing gets, the refresh result the wire carries, and the holder census values
//! storage refusals name. Census and record IO stay with their shells.

use std::path::{Path, PathBuf};

use cowshed_os::process::Holder;
use serde::{Deserialize, Serialize};

use crate::capabilities::BuildStatePath;

/// The capacity of a build volume created from nothing, unless `.cowshed.toml` `[build] capacity`
/// says otherwise (16_build_volumes.md, "Substrate"). Sparse: a cap on build-cache growth, not an
/// allocation.
pub const DEFAULT_BUILD_VOLUME_CAPACITY: crate::image::ImageCapacity =
    crate::image::ImageCapacity::from_gibibytes(100);
/// Processes, each as `pid <n> (<argv>)`, joined by `; `.
pub struct Holders<'a>(pub &'a [Holder]);

impl std::fmt::Display for Holders<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (index, holder) in self.0.iter().enumerate() {
            if index > 0 {
                formatter.write_str("; ")?;
            }
            write!(formatter, "{holder}")?;
        }
        Ok(())
    }
}
/// What a process holds inside a tree: the first hold the census found, cheapest first. A
/// process can hold a tree several ways at once; one is proof enough that it holds it, and
/// naming one keeps the census to one look per process.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Hold {
    /// Its working directory is in the tree.
    WorkingDirectory(PathBuf),
    /// Its root directory (`chroot`) is in the tree.
    RootDirectory(PathBuf),
    /// It runs an executable from the tree.
    Executable(PathBuf),
    /// It has a file of the tree open. Event-only opens (`O_EVTONLY`) hold nothing.
    OpenFile(PathBuf),
    /// It maps a file of the tree (procfs `maps`; Darwin's census reads no mappings).
    MappedFile(PathBuf),
}

impl Hold {
    pub fn path(&self) -> &Path {
        match self {
            Self::WorkingDirectory(path)
            | Self::RootDirectory(path)
            | Self::Executable(path)
            | Self::OpenFile(path)
            | Self::MappedFile(path) => path,
        }
    }
}

impl std::fmt::Display for Hold {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WorkingDirectory(path) => {
                write!(formatter, "working directory {}", path.display())
            }
            Self::RootDirectory(path) => write!(formatter, "root directory {}", path.display()),
            Self::Executable(path) => write!(formatter, "executable {}", path.display()),
            Self::OpenFile(path) => write!(formatter, "open file {}", path.display()),
            Self::MappedFile(path) => write!(formatter, "mapped file {}", path.display()),
        }
    }
}

/// A process holding a tree, with the hold that shows it: `pid <n> (<argv>), <hold>`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TreeHold {
    pub holder: Holder,
    pub hold: Hold,
}

impl std::fmt::Display for TreeHold {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}, {}", self.holder, self.hold)
    }
}
/// One process holding a kernel object a census asked about: the process,
/// the kernel's identity of its life, and each of its
/// descriptors on one of the objects, in descriptor order, with the caller's key for that object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectHold<K> {
    pub holder: Holder,
    pub life: u64,
    pub descriptors: Vec<(i32, K)>,
}

/// What a census of descriptor holders found.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectCensus<K> {
    /// Every process it read that holds one of the objects, in pid listing order.
    pub holds: Vec<ObjectHold<K>>,
    /// The processes whose descriptors were not read: those the kernel withheld from this user
    /// (another user's, unless the census runs as root) and, on Darwin, those a cowshed lifecycle
    /// operation runs in, which the census never asks. Any of them may hold an object, so they are
    /// counted, never reported as holding none.
    pub uninspected: u32,
}
/// A build volume's identity: 32 lowercase hex digits, minted once and never reused.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BuildVolumeId(String);

impl BuildVolumeId {
    /// The id `value` spells, or `None` for anything else: a name in a build directory that is
    /// not exactly an id is never a build volume.
    pub fn parse(value: &str) -> Option<Self> {
        (value.len() == 32
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)))
        .then(|| Self(value.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for BuildVolumeId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Serialize for BuildVolumeId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for BuildVolumeId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Self::parse(&value)
            .ok_or_else(|| serde::de::Error::custom(format!("invalid build volume id {value:?}")))
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum BuildStateTool {
    Cargo,
    Nx,
    /// `.cowshed.toml` `[build] state`: a tool no capability detects.
    Declared,
}

impl BuildStateTool {
    /// The tool whose state `path` is: declared state by its own volume namespace, Nx's
    /// directories under `.nx`, and otherwise a Cargo target directory.
    pub fn of(path: &BuildStatePath) -> Self {
        let checkout = path.checkout.as_path();
        if path.is_declared() {
            Self::Declared
        } else if checkout
            .parent()
            .and_then(Path::file_name)
            .is_some_and(|name| name == ".nx")
        {
            Self::Nx
        } else {
            Self::Cargo
        }
    }
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DisplacedBuildStateFinding {
    pub path: PathBuf,
    pub likely_tool: BuildStateTool,
}

impl std::fmt::Display for DisplacedBuildStateFinding {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let tool = match self.likely_tool {
            BuildStateTool::Cargo => "cargo clean or another Cargo command",
            BuildStateTool::Nx => "an Nx reset or cache command",
            BuildStateTool::Declared => {
                return write!(
                    formatter,
                    "discarded the real directory {} that .cowshed.toml [build] state declares \
                     and linked it onto the build volume; declared state is rebuildable, so \
                     re-run whatever reconstructs it once, and from then on it lives on the \
                     volume and every fork inherits it warm",
                    self.path.display()
                );
            }
        };
        write!(
            formatter,
            "discarded the real build-state directory {} and restored its volume link; {tool} may have displaced it; the next build rebuilds what is missing",
            self.path.display()
        )
    }
}
/// What refreshing a checkout's build state did (16_build_volumes.md, "One link per checkout"):
/// the volume it links afterwards, whether that volume was created now (the checkout's first
/// touch), the build-state paths linked for the first time, the real directories a tool left
/// where a link belongs, and what discovery could not decide.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BuildStateRefresh {
    pub volume: Option<BuildVolumeId>,
    pub created: bool,
    /// Checkout-relative.
    pub added: Vec<PathBuf>,
    pub displaced: Vec<DisplacedBuildStateFinding>,
    pub findings: Vec<crate::capabilities::BuildStateFinding>,
}
