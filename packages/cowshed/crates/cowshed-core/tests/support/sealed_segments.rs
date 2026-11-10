//! The tests' reader of a telemetry directory (13_telemetry.md, "Unsynced seals"): every sealed
//! segment in a root's date partitions, a torn or empty one skipped and named. Seals take no sync,
//! so a power cut can leave a sealed name torn (a truncated Arrow stream) or empty, and every
//! reader skips such a segment. No production code of this repository reads a telemetry
//! directory; `lmao-inspect tail -f` is specified as that reader (specs/lmao/04_inspect_cli.md).
//! Shared by every crate's tests that read sealed segments (`#[path]`-included, not a crate).

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;

/// One readable segment under a telemetry root: its path and every batch it holds.
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct SealedSegment {
    pub(crate) path: PathBuf,
    pub(crate) batches: Vec<RecordBatch>,
}

#[allow(dead_code)]
impl SealedSegment {
    /// The segment's file name: `<stem>.arrow`, whose stem leads with its kind and its writer's
    /// order.
    pub(crate) fn name(&self) -> &str {
        self.path
            .file_name()
            .and_then(|name| name.to_str())
            .expect("read_sealed keeps only UTF-8 segment names")
    }
}

/// What [`read_sealed`] found under a telemetry root.
#[allow(dead_code)]
#[derive(Debug, Default)]
pub(crate) struct Sealed {
    /// Every readable segment, by path: date partition, then name.
    pub(crate) segments: Vec<SealedSegment>,
    /// Every segment a power cut left torn or empty: skipped and counted, never an error.
    pub(crate) skipped: Vec<PathBuf>,
}

/// Every sealed segment in `root`'s date partitions. A sealed name a power cut left torn (a
/// truncated Arrow stream) or empty (no bytes, or no batch) is skipped and named in
/// [`Sealed::skipped`]. A temporary (a name led by `.`), a file that is not `*.arrow` and anything
/// not a directory at the root are not segments. Listing a directory or reading a segment's bytes
/// failing is an error.
#[allow(dead_code)]
pub(crate) fn read_sealed(root: &Path) -> io::Result<Sealed> {
    let mut paths = Vec::new();
    for partition in fs::read_dir(root)? {
        let partition = partition?;
        if !partition.file_type()?.is_dir() {
            continue;
        }
        for entry in fs::read_dir(partition.path())? {
            let entry = entry?;
            let name = entry.file_name();
            let segment = name
                .to_str()
                .is_some_and(|name| !name.starts_with('.') && name.ends_with(".arrow"));
            if segment && entry.file_type()?.is_file() {
                paths.push(entry.path());
            }
        }
    }
    paths.sort();
    let mut sealed = Sealed::default();
    for path in paths {
        match read_batches(&path)? {
            Some(batches) => sealed.segments.push(SealedSegment { path, batches }),
            None => sealed.skipped.push(path),
        }
    }
    Ok(sealed)
}

/// The batches of the segment at `path`, or `None` when it is torn or holds no batch. Its bytes
/// are read whole first, so a read the device failed is an error and only what those bytes fail to
/// decode as is a torn segment.
fn read_batches(path: &Path) -> io::Result<Option<Vec<RecordBatch>>> {
    let bytes = fs::read(path)?;
    let Ok(reader) = StreamReader::try_new(io::Cursor::new(bytes), None) else {
        return Ok(None);
    };
    let Ok(batches) = reader.collect::<Result<Vec<_>, _>>() else {
        return Ok(None);
    };
    Ok((!batches.is_empty()).then_some(batches))
}
