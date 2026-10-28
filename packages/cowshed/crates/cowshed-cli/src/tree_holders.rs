//! `cowshed tree-holders <dir>`: the census `rm` and `detach` refuse with (02_workspaces.md
//! "Holders cowshed does not run"), read out for a caller that is not cowshed: every process
//! holding something inside a directory tree, one `TreeHolder` JSON object per line. Host-only: no
//! project, no daemon, no runtime.

use std::io::{self, Write};
use std::path::Path;

use cowshed_core::CowshedError;
use cowshed_core::api::tree_holder::TreeHolder;
use cowshed_core::build_volume::nx;

use crate::args::TreeHoldersArgs;
use crate::output::Output;

pub(crate) fn dispatch<W, E>(args: &TreeHoldersArgs, output: &mut Output<W, E>) -> i32
where
    W: Write + Send,
    E: Write + Send,
{
    let written = holders(&args.path).and_then(|holders| {
        holders
            .iter()
            .try_for_each(|holder| output.bare_record(holder).map_err(write_failed))
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

/// Every holder of the tree at `path`: what `tree-holders` prints, one per line, and
/// `host.treeHolders` answers.
pub(crate) fn holders(path: &Path) -> Result<Vec<TreeHolder>, CowshedError> {
    nx::tree_holds(
        &cowshed_core::storage::bootstrap::CanonicalRoots::global().census(),
        path,
    )
    .map(|holds| holds.into_iter().map(TreeHolder::from).collect())
    .map_err(|error| {
        CowshedError::environment_missing(
            format!("cannot read who holds the tree {}: {error}", path.display()),
            "name a directory this user can read",
        )
    })
}

fn write_failed(error: io::Error) -> CowshedError {
    CowshedError::internal(format!("cowshed cannot write its tree holders: {error}"))
}
