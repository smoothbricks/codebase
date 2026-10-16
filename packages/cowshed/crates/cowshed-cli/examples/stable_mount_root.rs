//! Operator-run macOS proof; not a privileged test in the ordinary suite.
//! cargo run -p cowshed-cli --example stable_mount_root -- <new-receipt-directory>
//! Uses setup's real authorization transaction and the production APFS backend at the fixed
//! root, accessed through a preferred-path symlink. Live host configuration is never changed.

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
    use cowshed_apfs::image::ImageCapacity;
    use cowshed_apfs::{
        ApfsBackend, CreateImageRequest, DetachIntent, MacOsApfsBackend, MountAccess,
        SystemCommandRunner,
    };
    use cowshed_core::storage::bootstrap::execute_mount_root_provisioning;
    use cowshed_core::storage::host_config::{HOST_CONFIG_FILE, HostConfig};
    use serde_json::json;
    use std::fs;
    use std::io;
    use std::os::unix::fs::{MetadataExt, symlink};
    use std::path::{Path, PathBuf};
    use std::process::Command;
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

    fn census(receipts: &Path, phase: &str) -> ProofResult<(u32, KernelReading)> {
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
        let mounts = cowshed_apfs::mounts::kernel_mounts()?
            .into_iter()
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
        Ok((pid, reading))
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
        let receipts = fs::canonicalize(receipts)?;
        let live_path = Path::new(cowshed_gateway_types::STORE_ROOT).join(HOST_CONFIG_FILE);
        let live_before = live_configuration(&live_path)?;
        let scratch = crate::scratch_apfs::ScratchRoot::new_owned("root")?;
        let scratch_path = scratch.path().to_path_buf();
        let root = HostConfig::load(scratch.path(), scratch.path())?
            .mount_root()
            .to_path_buf();
        eprintln!("Provisioning fixed workspace root {}", root.display());
        execute_mount_root_provisioning(&root).await?;
        let directories = [
            root.parent().and_then(Path::parent).ok_or("mount prefix")?,
            root.parent().ok_or("uid directory")?,
            root.as_path(),
        ]
        .into_iter()
        .map(|path| {
            let metadata = fs::symlink_metadata(path)?;
            Ok(json!({"path": path, "uid": metadata.uid(), "gid": metadata.gid(), "mode": format!("{:o}", metadata.mode() & 0o7777)}))
        })
        .collect::<io::Result<Vec<_>>>()?;
        fs::write(
            receipts.join("provisioned-directories.json"),
            serde_json::to_vec_pretty(&directories)?,
        )?;
        let owner = root.join("fixture");
        fs::create_dir_all(&owner)?;
        let project = owner.join(format!("stable-root-{}", std::process::id()));
        fs::create_dir(&project)?;
        let mount = project.join("cycle");
        fs::create_dir(&mount)?;
        let alias = receipts.join("workspaces");
        symlink(&root, &alias)?;
        let alias_mount = alias.join(mount.strip_prefix(&root)?);
        let outcome = cycle(scratch.path(), &mount, &alias_mount, &receipts);
        // The scratch guard releases only images backed by its own root, including on failure.
        drop(scratch);
        let scratch_released = !scratch_path.exists();
        let live_unchanged = live_configuration(&live_path)? == live_before;
        let project_mounted = cowshed_apfs::mounts::kernel_mounts()?
            .iter()
            .any(|mounted| mounted.mount_point.starts_with(&project));
        if scratch_released && !project_mounted {
            fs::remove_dir_all(&project)?;
        }
        fs::write(
            receipts.join("final.json"),
            serde_json::to_vec_pretty(&json!({
                "fixedRoot": root, "preferredPath": alias,
                "scratchReleased": scratch_released, "projectMounted": project_mounted,
                "liveHostConfigurationUnchanged": live_unchanged,
                "cycle": outcome.as_ref().map(|_| "passed").map_err(ToString::to_string),
            }))?,
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

    fn cycle(scratch: &Path, mount: &Path, alias: &Path, receipts: &Path) -> ProofResult<()> {
        let backend = MacOsApfsBackend::new(SystemCommandRunner);
        let image = backend.create_staged_image(&CreateImageRequest {
            staged_stem: scratch.join("cycle"),
            capacity: ImageCapacity::from_gibibytes(1),
            volume_name: "[cowshed] stable root proof".into(),
            owner_uid: unsafe { libc::getuid() },
            owner_gid: unsafe { libc::getgid() },
        })?;
        let (mds_before, before) = census(receipts, "before-mount")?;
        let started = Instant::now();
        let attachment = backend.attach_mounted(&image, mount, MountAccess::ReadWrite, false)?;
        fs::write(mount.join("tracked"), b"stable mount-root proof\n")?;
        if fs::canonicalize(alias)? != mount
            || fs::read(alias.join("tracked"))? != b"stable mount-root proof\n"
        {
            return Err("preferred-path symlink did not expose the fixed physical mount".into());
        }
        println!(
            "{}",
            json!({"phase": "mounted", "mount": mount, "alias": alias, "elapsedSeconds": started.elapsed().as_secs_f64()})
        );
        census(receipts, "while-mounted")?;
        backend.detach(&attachment, DetachIntent::Release)?;
        let (mds_after, after) = census(receipts, "after-detach")?;
        if mds_after != mds_before {
            return Err(format!("mds restarted during proof: {mds_before} -> {mds_after}").into());
        }
        for (class, old, new) in [
            ("AppleAPFSVolume", before.apfs_volumes, after.apfs_volumes),
            ("IOMedia", before.media, after.media),
            (
                "AppleDiskImageDevice",
                before.image_devices,
                after.image_devices,
            ),
            (
                "DIDeviceIOUserClient",
                before.user_clients,
                after.user_clients,
            ),
        ] {
            if new.leaked() > old.leaked() {
                return Err(format!("{class} departed objects grew across mount/detach: {old} -> {new}; inspect exact registry IDs and mds rights in the receipts").into());
            }
        }
        Ok(())
    }
}
