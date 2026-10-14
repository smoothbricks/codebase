//! The one pinned Nx cache boundary (16_build_volumes.md, "Carry"). Nx creates its own database;
//! the carry's small row shim only recognizes and extends the two Nx-owned cache tables.

use bytes::{Bytes, BytesMut};
use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

const ADAPTER: &str = include_str!("cache_adapter.js");
const INITIALIZE: &str = r#"
const answer = process.stdout.write.bind(process.stdout);
process.stdout.write = process.stderr.write.bind(process.stderr);
try {
  const { databases } = openNxCache(process.argv[2]);
  answer(`${JSON.stringify({ initialized: true, databases })}\n`);
} catch (error) {
  console.error(error);
  process.exitCode = 1;
}
"#;

/// Prefix a body once, then retain/clone the returned Bytes for every root's job. No body owns a
/// second constructor or its own version-sensitive cache access.
pub fn script(body: &'static str) -> Bytes {
    let mut script = BytesMut::with_capacity(ADAPTER.len() + 1 + body.len());
    script.extend_from_slice(ADAPTER.as_bytes());
    script.extend_from_slice(b"\n");
    script.extend_from_slice(body.as_bytes());
    script.freeze()
}

pub fn initialize_script() -> Bytes {
    static SCRIPT: LazyLock<Bytes> = LazyLock::new(|| script(INITIALIZE));
    SCRIPT.clone()
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Initialized {
    initialized: bool,
    databases: Vec<PathBuf>,
}

/// Accepts only an initialization that names this checkout's own v3 task databases.
pub fn initialized(stdout: &[u8], root: &Path) -> Result<(), String> {
    let result: Initialized = serde_json::from_slice(stdout)
        .map_err(|error| format!("Nx cache initialization answered invalid JSON: {error}"))?;
    if !result.initialized {
        return Err("Nx cache initialization did not initialize the cache".into());
    }
    let workspace_data = root.join(".nx").join("workspace-data");
    if result.databases.is_empty() {
        return Err(format!(
            "Nx cache initialization named no task database under {}",
            workspace_data.display()
        ));
    }
    for database in &result.databases {
        let names_v3 = database
            .file_name()
            .is_some_and(|name| name.to_string_lossy().ends_with("-v3.db"));
        if database.parent() != Some(workspace_data.as_path()) || !names_v3 {
            return Err(format!(
                "Nx cache initialization named {}, not a v3 task database of {}",
                database.display(),
                workspace_data.display()
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_initialization_requires_the_exact_success_receipt() {
        let root = Path::new("/checkout");
        let workspace_data = "/checkout/.nx/workspace-data";
        let good = format!(r#"{{"initialized":true,"databases":["{workspace_data}/task-v3.db"]}}"#);
        assert!(initialized(good.as_bytes(), root).is_ok());
        let refused = [
            r#"{"initialized":false,"databases":[]}"#.to_string(),
            r#"{"initialized":true}"#.to_string(),
            r#"{"initialized":true,"databases":[]}"#.to_string(),
            r#"{"initialized":true,"databases":["/elsewhere/.nx/workspace-data/task-v3.db"]}"#
                .to_string(),
            format!(r#"{{"initialized":true,"databases":["{workspace_data}/task.db"]}}"#),
            format!(
                r#"{{"initialized":true,"databases":["{workspace_data}/task-v3.db"],"foreign":true}}"#
            ),
            "not JSON".to_string(),
        ];
        for body in &refused {
            assert!(initialized(body.as_bytes(), root).is_err(), "{body}");
        }
    }

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
            .write_all(&initialize_script())
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
