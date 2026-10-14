//! Operator-run macOS proof; not a privileged test in the ordinary suite.
//! cargo run -p cowshed-cli --example stable_mount_root -- <new-receipt-directory>
//! Uses setup's real authorization transaction, then the production CLI dispatcher on a
//! scratch store. The installed gateway and live host configuration are never changed.

#[cfg(target_os = "macos")]
use cowshed_apfs::store_lock::verify_no_symlinks;
#[cfg(target_os = "macos")]
use cowshed_apfs::{
    CommandRunner, DetachIntent, DiskImageSource, MacOsApfsBackend, SystemCommandRunner,
};
#[cfg(target_os = "macos")]
#[path = "../../cowshed-apfs/tests/support/scratch_apfs.rs"]
pub mod scratch_apfs;

#[cfg(target_os = "macos")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    native::run().await
}

#[cfg(not(target_os = "macos"))]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    Err("stable_mount_root requires macOS and operator authorization".into())
}

#[cfg(target_os = "macos")]
mod native {
    use cowshed_apfs::attach_budget::{self, KernelReading};
    use cowshed_cli::args::parse_args;
    use cowshed_cli::output::Output;
    use cowshed_cli::runtime::{ActorBridge, CliService, dispatch};
    use cowshed_core::repository::RepoId;
    use cowshed_core::storage::bootstrap::{
        CanonicalRoots, ValidatedHostStorage, execute_mount_root_provisioning,
    };
    use cowshed_core::storage::host_config::{HOST_CONFIG_FILE, HostConfig};
    use cowshed_gateway::{
        ArrowAuditConfig, ArrowAuditSink, Gateway, GatewayConfig, KeychainCredentialProvider,
        MirrorCacheConfig, SystemConnector,
    };
    use serde_json::{Value, json};
    use std::fs;
    use std::io;
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::sync::Arc;
    use std::time::Instant;

    type ProofResult<T> = Result<T, Box<dyn std::error::Error>>;

    fn tool(program: &str, args: &[&str]) -> ProofResult<Vec<u8>> {
        let output = Command::new(program).args(args).output()?;
        if !output.status.success() {
            return Err(io::Error::other(format!(
                "{program} {args:?}: {}; stdout: {}; stderr: {}",
                output.status,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            ))
            .into());
        }
        Ok(output.stdout)
    }

    fn census(receipts: &Path, phase: &str) -> ProofResult<KernelReading> {
        let launch = tool(
            "/bin/launchctl",
            &["print", "system/com.apple.metadata.mds"],
        )?;
        fs::write(receipts.join(format!("{phase}.mds-launch.txt")), &launch)?;
        let text = std::str::from_utf8(&launch)?;
        let pid: u32 = text
            .lines()
            .find_map(|line| {
                line.trim()
                    .strip_prefix("pid = ")
                    .and_then(|pid| pid.parse().ok())
            })
            .ok_or("mds is not running; this is not a retention-prevention proof")?;
        let ports = tool(
            "/usr/bin/sudo",
            &["-n", "/usr/bin/lsmp", "-p", &pid.to_string(), "-v"],
        )?;
        fs::write(receipts.join(format!("{phase}.mds-ports.txt")), ports)?;
        for class in [
            "AppleAPFSVolume",
            "IOMedia",
            "AppleDiskImageDevice",
            "DIDeviceIOUserClient",
        ] {
            fs::write(
                receipts.join(format!("{phase}.{class}.plist")),
                tool("/usr/sbin/ioreg", &["-r", "-c", class, "-d", "1", "-a"])?,
            )?;
        }
        let mounts = cowshed_apfs::mounts::kernel_mounts()?.into_iter()
            .map(|mount| json!({"mount": mount.mount_point, "device": mount.source_device, "mountId": mount.mount_id}))
            .collect::<Vec<_>>();
        fs::write(
            receipts.join(format!("{phase}.mounts.json")),
            serde_json::to_vec_pretty(&mounts)?,
        )?;
        let reading = attach_budget::observe()?.reading;
        fs::write(
            receipts.join(format!("{phase}.counts.json")),
            serde_json::to_vec_pretty(&reading)?,
        )?;
        println!(
            "{}",
            json!({"phase": phase, "mdsPid": pid, "counts": reading})
        );
        Ok(reading)
    }

    async fn cli(
        service: &mut ActorBridge,
        receipts: &Path,
        label: &str,
        args: &[&str],
    ) -> ProofResult<Value> {
        let parsed = parse_args(args.iter().copied())?;
        let mut output = Output::new(Vec::new(), Vec::new(), false);
        let started = Instant::now();
        let result = dispatch(service, parsed, tokio::io::empty(), &mut output).await;
        let (stdout, stderr) = output.into_inner();
        fs::write(receipts.join(format!("{label}.stdout")), &stdout)?;
        fs::write(receipts.join(format!("{label}.stderr")), &stderr)?;
        let exit = result?.code;
        let record = json!({"command": args, "exit": exit, "elapsedSeconds": started.elapsed().as_secs_f64(), "stdout": String::from_utf8_lossy(&stdout), "stderr": String::from_utf8_lossy(&stderr)});
        fs::write(
            receipts.join(format!("{label}.json")),
            serde_json::to_vec_pretty(&record)?,
        )?;
        if exit != 0 && !(args.contains(&"doctor") && exit == 5) {
            return Err(io::Error::other(format!("CLI {args:?} failed: {record}")).into());
        }
        println!(
            "{}",
            json!({"phase": label, "exit": exit, "elapsedSeconds": started.elapsed().as_secs_f64()})
        );
        Ok(record)
    }

    fn live_configuration(path: &Path) -> io::Result<Option<Vec<u8>>> {
        match fs::read(path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    pub async fn run() -> ProofResult<()> {
        let mut args = std::env::args_os().skip(1);
        let receipts = PathBuf::from(
            args.next()
                .ok_or("usage: stable_mount_root <new-receipt-directory>")?,
        );
        if args.next().is_some() {
            return Err("usage: stable_mount_root <new-receipt-directory>".into());
        }
        fs::create_dir(&receipts)?;
        let live_path = Path::new(cowshed_gateway_types::STORE_ROOT).join(HOST_CONFIG_FILE);
        let live_before = live_configuration(&live_path)?;
        let scratch = crate::scratch_apfs::ScratchRoot::new_owned("root")?;
        let scratch_path = scratch.path().to_path_buf();
        let store = scratch.path().join("store");
        let home = scratch.path().join("home");
        fs::create_dir(&store)?;
        fs::create_dir(&home)?;
        let config = HostConfig::load(&store, &home)?;
        let root = config.mount_root().to_path_buf();
        eprintln!(
            "Provisioning {} through setup's native macOS authorization transaction",
            root.display()
        );
        execute_mount_root_provisioning(&root).await?;
        config.save(&store)?;
        let directories = [root.parent().and_then(Path::parent).ok_or("mount prefix")?, root.parent().ok_or("uid directory")?, root.as_path()]
            .into_iter().map(|path| {
                let metadata = fs::symlink_metadata(path)?;
                Ok(json!({"path": path, "uid": metadata.uid(), "gid": metadata.gid(), "mode": format!("{:o}", metadata.mode() & 0o7777)}))
            }).collect::<io::Result<Vec<_>>>()?;
        fs::write(
            receipts.join("provisioned-directories.json"),
            serde_json::to_vec_pretty(&directories)?,
        )?;
        println!(
            "{}",
            json!({"phase": "provisioned", "directories": directories})
        );
        let repo_name = format!("stable-root-{}", std::process::id());
        let repo_id = RepoId::parse(&format!("fixture/{repo_name}"))?;
        let owner = root.join("fixture");
        fs::create_dir_all(&owner)?;
        let project = owner.join(&repo_name);
        fs::create_dir(&project)?;
        let checkout = project.join("main");
        fs::create_dir(&checkout)?;
        fs::write(checkout.join("tracked"), b"stable mount-root proof\n")?;
        fs::write(
            checkout.join(".cowshed.toml"),
            b"[build]\ncapacity = \"1g\"\n",
        )?;
        for args in [
            vec!["init", "-q", "-b", "main"],
            vec!["add", "tracked", ".cowshed.toml"],
            vec![
                "-c",
                "user.name=Fixture",
                "-c",
                "user.email=fixture@example.invalid",
                "-c",
                "commit.gpgSign=false",
                "commit",
                "-q",
                "-m",
                "initial",
            ],
        ] {
            let output = Command::new("git")
                .current_dir(&checkout)
                .args(&args)
                .env_remove("GIT_DIR")
                .env_remove("GIT_WORK_TREE")
                .output()?;
            if !output.status.success() {
                return Err(io::Error::other(format!(
                    "git {args:?}: {}",
                    String::from_utf8_lossy(&output.stderr)
                ))
                .into());
            }
        }
        let storage = ValidatedHostStorage::new(home, CanonicalRoots::at(store));
        let outcome = cycle(&checkout, repo_id, storage, &root, &receipts).await;
        drop(scratch);
        let scratch_released = !scratch_path.exists();
        let live_unchanged = live_configuration(&live_path)? == live_before;
        // The native scratch guard releases only images backed by its own store, before any
        // outside checkout scaffolding is removed. Refuse to delete through a remaining mount.
        let mounts = cowshed_apfs::mounts::kernel_mounts()?;
        let project_mounted = mounts
            .iter()
            .any(|mount| mount.mount_point.starts_with(&project));
        if scratch_released && !project_mounted {
            fs::remove_dir_all(&project)?;
        }
        fs::write(
            receipts.join("final.json"),
            serde_json::to_vec_pretty(
                &json!({"scratchReleased": scratch_released, "projectMounted": project_mounted, "liveHostConfigurationUnchanged": live_unchanged, "cycle": outcome.as_ref().map(|_| "passed").map_err(ToString::to_string)}),
            )?,
        )?;
        outcome?;
        if !scratch_released || project_mounted || !live_unchanged {
            return Err(
                "fixture cleanup or live host-configuration invariance failed; see final.json"
                    .into(),
            );
        }
        Ok(())
    }

    async fn cycle(
        checkout: &Path,
        repo: RepoId,
        storage: ValidatedHostStorage,
        mount_root: &Path,
        receipts: &Path,
    ) -> ProofResult<()> {
        let cache = cowshed_core::host_dirs::gateway_mirror(storage.home());
        let audit = storage.telemetry().join("gateway");
        for path in [&cache, &audit] {
            fs::create_dir_all(path)?;
            fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
        }
        let gateway_config = GatewayConfig {
            control_socket: Some(storage.store().join("gateway.sock")),
            mirror_cache: MirrorCacheConfig::new(cache),
            ..GatewayConfig::default()
        };
        let connector = SystemConnector::new(
            gateway_config.timeouts.connect,
            gateway_config.timeouts.tls_handshake,
        )?;
        let audit = ArrowAuditSink::start(ArrowAuditConfig::new(audit)?)?;
        let gateway = Gateway::start(
            gateway_config,
            Arc::new(KeychainCredentialProvider::new()),
            Arc::new(connector),
            Arc::new(audit),
        )
        .await?;
        let mut service =
            ActorBridge::open_for_adopt_at(checkout, Some(repo.clone()), storage).await?;
        let operation: ProofResult<()> = async {
            cli(&mut service, receipts, "adopt", &["--json", "adopt", checkout.to_str().ok_or("non-UTF8 checkout")?, "--repo-id", &repo.to_string(), "--capacity", "1g"]).await?;
            service.reconcile_gateway().await?;
            cli(&mut service, receipts, "doctor-before", &["--json", "doctor"]).await?;
            let before = census(receipts, "before-fork")?;
            cli(&mut service, receipts, "fork", &["--json", "fork", "cycle"]).await?;
            let mounted = service.path("cycle", false).await?.mount;
            if !mounted.starts_with(mount_root) || fs::read(mounted.join("tracked"))? != b"stable mount-root proof\n" {
                return Err("published fork does not expose the expected payload under the stable root".into());
            }
            println!("{}", json!({"phase": "published", "mount": mounted}));
            census(receipts, "after-fork")?;
            cli(&mut service, receipts, "rm", &["--json", "rm", "cycle"]).await?;
            cli(&mut service, receipts, "doctor-after", &["--json", "doctor"]).await?;
            let after = census(receipts, "after-rm")?;
            for (class, old, new) in [("AppleAPFSVolume", before.apfs_volumes, after.apfs_volumes), ("IOMedia", before.media, after.media), ("AppleDiskImageDevice", before.image_devices, after.image_devices), ("DIDeviceIOUserClient", before.user_clients, after.user_clients)] {
                if new.leaked() > old.leaked() {
                    return Err(io::Error::other(format!("{class} departed objects grew across fork/rm: {old} -> {new}; inspect exact registry IDs and mds rights in the receipt directory")).into());
                }
            }
            Ok(())
        }.await;
        let shutdown = service.shutdown().await;
        let drained = gateway.drain().await;
        let mut failures = Vec::new();
        if let Err(error) = operation {
            failures.push(format!("cycle: {error}"));
        }
        if let Err(error) = shutdown {
            failures.push(format!("controller shutdown: {error}"));
        }
        if let Err(error) = drained {
            failures.push(format!("scratch gateway drain: {error}"));
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(io::Error::other(failures.join("; ")).into())
        }
    }
}
