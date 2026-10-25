//! The one pinned Nx cache boundary: every body that opens the checkout's own Nx cache (a land's
//! publish, 05_gateway.md "Publication") runs behind this adapter's version and path checks.

use bytes::{Bytes, BytesMut};

const ADAPTER: &str = include_str!("cache_adapter.js");

/// Prefix a body once, then retain/clone the returned Bytes for every root's job. No body owns a
/// second constructor or its own version-sensitive cache access.
pub fn script(body: &'static str) -> Bytes {
    let mut script = BytesMut::with_capacity(ADAPTER.len() + 1 + body.len());
    script.extend_from_slice(ADAPTER.as_bytes());
    script.extend_from_slice(b"\n");
    script.extend_from_slice(body.as_bytes());
    script.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FixtureGuard(std::path::PathBuf);

    impl Drop for FixtureGuard {
        fn drop(&mut self) {
            match std::fs::remove_dir_all(&self.0) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => panic!(
                    "failed to remove fixture root {}: {error}",
                    self.0.display()
                ),
            }
        }
    }

    #[test]
    fn unsupported_nx_metadata_is_refused_before_any_cache_side_effect() {
        use cowshed_os::fork_lock::Spawn as _;
        use std::io::Write;
        use std::process::{Command, Stdio};

        let root = std::env::temp_dir().join(format!(
            "cowshed-cache-adapter-{}",
            uuid::Uuid::new_v4().simple()
        ));
        let _guard = FixtureGuard(root.clone());
        std::fs::create_dir_all(root.join("node_modules/nx")).expect("create negative Nx fixture");
        std::fs::write(root.join("package.json"), "{}").expect("write fixture package.json");
        std::fs::write(
            root.join("node_modules/nx/package.json"),
            r#"{"version":"23.2.2"}"#,
        )
        .expect("write fixture Nx metadata");

        let mut child = Command::new("node")
            .arg("-")
            .arg(&root)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn_locked()
            .expect("run Nx cache adapter with node");
        child
            .stdin
            .take()
            .expect("node stdin")
            .write_all(&script("openNxCache(process.argv[2]);"))
            .expect("send adapter script");
        let output = child.wait_with_output().expect("collect node output");

        assert!(!output.status.success());
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("supports exactly Nx 23.2.1, not 23.2.2"),
            "{stderr}"
        );
        assert!(
            output.stdout.is_empty(),
            "{}",
            String::from_utf8_lossy(&output.stdout)
        );
        assert!(
            !root.join(".nx").exists(),
            "unsupported Nx version must not create cache state"
        );
    }
}
