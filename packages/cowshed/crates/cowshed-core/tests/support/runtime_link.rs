//! A fixture owns its short runtime name across concurrent tests and binaries.
//! Creating the symlink reserves the namespace atomically; dropping it releases it,
//! including on assertion failures. No listener or timing probe is involved.

use std::fs;
use std::io;
use std::path::PathBuf;

use super::{PortBlock, SandboxConfig, sandbox_runtime_dir, sandbox_runtime_link};

pub(super) struct RuntimeLink(PathBuf);

impl RuntimeLink {
    pub(super) fn reserve(sandbox: &mut SandboxConfig) -> Self {
        let runtime = sandbox_runtime_dir(sandbox);
        fs::create_dir_all(&runtime).unwrap();
        for base in (49_184..=65_520).step_by(16) {
            sandbox.port_block = PortBlock::new(base, 16).unwrap();
            let link = sandbox_runtime_link(sandbox);
            match std::os::unix::fs::symlink(&runtime, &link) {
                Ok(()) => return Self(link),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => panic!("reserve runtime link {}: {error}", link.display()),
            }
        }
        panic!("all fixture runtime names are occupied");
    }
}

impl Drop for RuntimeLink {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_file(&self.0) {
            eprintln!("release fixture runtime link {}: {error}", self.0.display());
        }
    }
}
