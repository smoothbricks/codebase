//! `cowshed fd-holders [--device <0x…>]… [--inode <n>]…` (02_workspaces.md "Descriptor holders:
//! `cowshed fd-holders`"): every process with a descriptor on one of the kernel objects a caller
//! names, the objects named as `hold`'s `started.output` names them, as one `FdHolders` JSON
//! object. Host-only: no project, no daemon, no runtime.

use std::io::Write;

use cowshed_core::CowshedError;
use cowshed_core::api::fd_holder::{FdHolders, FdIdentity};
use cowshed_core::api::operations::FdHoldersRequest;
use cowshed_core::build_volume::nx;

use crate::output::Output;

pub(crate) fn dispatch<W, E>(request: &FdHoldersRequest, output: &mut Output<W, E>) -> i32
where
    W: Write + Send,
    E: Write + Send,
{
    let written = holders(request).and_then(|holders| {
        output.bare_record(&holders).map_err(|error| {
            CowshedError::internal(format!(
                "cowshed cannot write its descriptor holders: {error}"
            ))
        })
    });
    match written {
        Ok(()) => 0,
        Err(error) => {
            // The exit code is the report a caller that closed stderr still reads.
            let _ = output
                .error(&error.message)
                .and_then(|()| output.hint(&error.hint));
            i32::from(error.exec_wrapper_exit_code())
        }
    }
}

/// Every holder of the objects `request` names: what `fd-holders` prints and `host.fdHolders`
/// answers. A request naming no object is refused, never answered with no holder.
pub(crate) fn holders(request: &FdHoldersRequest) -> Result<FdHolders, CowshedError> {
    if request.devices.is_empty() && request.inodes.is_empty() {
        return Err(CowshedError::usage(
            "fd-holders names no object: a request names at least one device or inode",
            "name the device or inode `hold`'s started.output gave",
        ));
    }
    let objects: Vec<_> = request
        .devices
        .iter()
        .map(|&device| FdIdentity::Device { device })
        .chain(
            request
                .inodes
                .iter()
                .map(|&inode| FdIdentity::Inode { inode }),
        )
        .map(|identity| (identity.name(), identity))
        .collect();
    nx::object_holders(
        &cowshed_core::storage::bootstrap::CanonicalRoots::global().census(),
        &objects,
    )
    .map(FdHolders::from)
    .map_err(|error| {
        CowshedError::environment_missing(
            format!("cannot read who holds the named descriptors: {error}"),
            "run it where this user can list the host's processes",
        )
    })
}
