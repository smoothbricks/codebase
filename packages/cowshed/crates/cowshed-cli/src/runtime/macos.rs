//! What the CLI says and reads of a macOS host: the APFS volumes, the FileVault step and the
//! LaunchDaemon of host setup, the `.dmg` or the dataset an adoption creates, and the disk-image
//! and IOKit device classes doctor reports.

use std::path::{Path, PathBuf};

use cowshed_apfs::image::IMAGE_EXTENSION;
use cowshed_core::Result;
use cowshed_core::api::{Finding, FindingSeverity, StoreSubstrate, WorkspaceInfo};
use cowshed_core::doctor::probe_sync;
use cowshed_core::storage::bootstrap::MOUNT_SERVICE_PLIST;

/// The parties doctor's host-storage probe waits on: planning host setup asks securityd for the
/// volumes' passphrases and launchd for the storage mount service.
pub(super) const HOST_STORAGE_PARTIES: &str = "securityd (security find-generic-password, System.keychain) and launchd (system/dev.cowshed.storage)";

/// `disk-command` runs `diskutil` or `hdiutil`, and this host has both.
pub(super) fn disk_tools() -> Result<()> {
    Ok(())
}

/// What an adoption created: main's image, at the capacity it was minted at, or, for an adoption
/// that named ZFS, main's dataset, capped at it. An adoption that named nothing made an image.
pub(super) fn created_main(
    info: &WorkspaceInfo,
    capacity: &str,
    filesystem: Option<StoreSubstrate>,
) -> String {
    match filesystem {
        Some(StoreSubstrate::Zfs) => format!(
            "created {} for {} as a ZFS dataset (capped at {capacity})",
            info.workspace, info.repo_id,
        ),
        Some(StoreSubstrate::Apfs | StoreSubstrate::Directory) | None => format!(
            "created {}.{IMAGE_EXTENSION} for {} (capacity {capacity})",
            info.workspace, info.repo_id,
        ),
    }
}

/// What creating one APFS volume will do.
pub(super) fn create_volume_evidence(name: &str, container: &str, mount_at: &Path) -> String {
    format!(
        "create {name} in APFS container {container} and mount it at {}",
        mount_at.display()
    )
}

/// What encrypting one volume will do.
pub(super) fn encrypt_volume_evidence(name: &str, uuid: &str, size_bytes: u64) -> String {
    format!(
        "FileVault-encrypt existing {name} ({uuid}, {size_bytes} bytes) in place and store its passphrase in System.keychain"
    )
}

/// What installing the storage mount service will do.
pub(super) fn mount_service_evidence(label: &str) -> String {
    format!(
        "converge system LaunchDaemon {label} and verified encrypted-volume credentials for boot and login discovery"
    )
}

/// A volume that is current: mounted where it belongs, carrying its marker.
pub(super) fn current_volume(name: &str, expected: &Path) -> String {
    format!(
        "{name}: present, mounted at {}, marker valid",
        expected.display()
    )
}

/// A volume setup has yet to create, in the container it will be created in when the plan
/// names one.
pub(super) fn absent_volume(name: &str, container: Option<&str>, expected: &Path) -> String {
    match container {
        Some(container) => format!(
            "{name}: absent from APFS container {container}; expected mount {}; marker absent",
            expected.display()
        ),
        None => format!(
            "{name}: absent; expected mount {}; marker absent",
            expected.display()
        ),
    }
}

/// Doctor's finding for a volume whose FileVault encryption setup has yet to configure.
pub(super) fn encryption_finding(name: &str, uuid: &str, size_bytes: u64) -> Finding {
    Finding {
        code: "host-filevault".into(),
        severity: FindingSeverity::Error,
        message: format!(
            "{name}: FileVault encryption or its System.keychain passphrase is not configured ({uuid}, {size_bytes} bytes)"
        ),
        hint: "cowshed setup".into(),
        path: None,
    }
}

/// Doctor's finding for a storage mount service setup has yet to converge, naming the plist
/// launchd reads it from.
pub(super) fn mount_service_finding(label: &str) -> Finding {
    Finding {
        code: "host-mount-service".into(),
        severity: FindingSeverity::Error,
        message: format!(
            "system LaunchDaemon {label} or its encrypted-volume discovery credentials need repair"
        ),
        hint: "cowshed setup".into(),
        path: Some(PathBuf::from(MOUNT_SERVICE_PLIST)),
    }
}

/// The kernel device classes doctor reports, each read from the IOKit registry: the disk-image
/// helpers that outlived their image, the disk-image device classes, the IOMedia
/// match-notification cap, and the departed volume references.
pub(super) fn device_findings() -> Vec<Finding> {
    let mut findings = Vec::new();
    findings.extend(orphaned_disk_image_helpers_finding(probe_sync(
        format_args!("disk-image-helpers"),
        cowshed_apfs::disk_image_helpers::OrphanedHelpers::read,
    )));
    // One snapshot answers both findings: departed volumes need no census of their own.
    let classes = probe_sync(
        format_args!("attach-budget"),
        cowshed_apfs::attach_budget::read_kernel,
    );
    let departed = classes
        .as_ref()
        .ok()
        .and_then(departed_volume_references_finding);
    findings.extend(disk_image_attach_budget_finding(classes));
    findings.extend(media_notification_cap_finding(probe_sync(
        format_args!("media-notification-cap"),
        cowshed_apfs::media_cap::read,
    )));
    findings.extend(departed);
    findings
}

/// `diskimagesiod` helpers that outlived their disk image, each still holding the 72 MiB of
/// kernel mapping whose exhaustion fails every attach with error code 150. Only root can end
/// them, so the finding names the pids and the command, and cowshed signals nothing itself. A
/// pid is named only when two reads agree with no disk-image tool running; while one runs, a
/// helper may be attaching, and the finding says so without naming any.
fn orphaned_disk_image_helpers_finding(
    reading: std::io::Result<cowshed_apfs::disk_image_helpers::HelperReading>,
) -> Option<Finding> {
    use cowshed_apfs::disk_image_helpers::HelperReading;
    match reading {
        Ok(HelperReading::Settled(orphans)) if orphans.is_empty() => None,
        Ok(HelperReading::Settled(orphans)) => Some(Finding {
            code: "orphaned-disk-image-helpers".into(),
            severity: FindingSeverity::Warning,
            message: orphans.to_string(),
            hint: orphans.remedy(),
            path: None,
        }),
        Ok(HelperReading::Busy { suspected: 0 }) => None,
        Ok(HelperReading::Busy { suspected }) => Some(Finding {
            code: "orphaned-disk-image-helpers".into(),
            severity: FindingSeverity::Info,
            message: format!(
                "{suspected} diskimagesiod helper(s) serve no attached disk image, but a diskutil \
                 or hdiutil was running, so they may still be attaching: no pid is named, \
                 because killing a helper mid-attach leaks its device until reboot"
            ),
            hint: "cowshed doctor once no diskutil or hdiutil is running".into(),
            path: None,
        }),
        Err(error) if error.kind() == std::io::ErrorKind::Unsupported => None,
        Err(error) => Some(Finding {
            code: "disk-image-helpers".into(),
            severity: FindingSeverity::Warning,
            message: format!(
                "could not tell orphaned diskimagesiod helpers from the ones serving attached \
                 disk images: {error}"
            ),
            hint: "ioreg -r -c DIDeviceIOUserClient -l | grep IOUserClientCreator".into(),
            path: None,
        }),
    }
}

/// The host's disk-image device classes (attach_budget.rs): leaked image devices, user clients,
/// volumes and media. Cowshed runs no DiskImages2 attach, so nothing is gated on them: a leaked
/// image device warns and names what frees it. Departed volumes and media take no attach-pool
/// slot, so [`departed_volume_references_finding`] reports them, never this finding.
fn disk_image_attach_budget_finding(
    reading: std::io::Result<cowshed_apfs::attach_budget::KernelReading>,
) -> Option<Finding> {
    match reading {
        Ok(reading) => Some(Finding {
            code: "disk-image-attach-budget".into(),
            severity: if reading.is_leaking() {
                FindingSeverity::Warning
            } else {
                FindingSeverity::Info
            },
            message: reading.to_string(),
            hint: reading.remedy().into(),
            path: None,
        }),
        Err(error) if error.kind() == std::io::ErrorKind::Unsupported => None,
        Err(error) => Some(Finding {
            code: "disk-image-attach-budget".into(),
            severity: FindingSeverity::Warning,
            message: format!("could not read the disk-image device classes: {error}"),
            hint: "ioclasscount AppleDiskImageDevice DIDeviceIOUserClient AppleAPFSVolume IOMedia"
                .into(),
            path: None,
        }),
    }
}

/// Fewer images than this left under the media cap warn: about ten workspaces with build volumes.
const MEDIA_LOW_IMAGES: u64 = 20;

/// The registered IOMedia against IOKit's match-notification ceiling (media_cap.rs), a soft one:
/// cowshed's in-kernel attaches wait on no IOMedia notification, so verbs make room best effort
/// and attach past it. A warning while fewer than [`MEDIA_LOW_IMAGES`] images fit, none past it.
fn media_notification_cap_finding(
    reading: std::io::Result<cowshed_apfs::media_cap::MediaReading>,
) -> Option<Finding> {
    use cowshed_apfs::media_cap::{MEDIA_CAP_HINT, NOTIFICATION_CAP};
    match reading {
        Ok(reading) => {
            let left = reading.images_left(NOTIFICATION_CAP);
            Some(Finding {
                code: "media-notification-cap".into(),
                severity: if left < MEDIA_LOW_IMAGES {
                    FindingSeverity::Warning
                } else {
                    FindingSeverity::Info
                },
                message: format!(
                    "{reading}; with the host's reserve that commits {} of IOKit's \
                     {NOTIFICATION_CAP}-entry IOMedia match-notification queue (XNU \
                     IOServiceUserNotification::kMaxOutstanding), room for {left} more image(s), \
                     {} workspace(s) with a build volume. Past it an unfiltered IOMedia \
                     notification misses matches; cowshed's in-kernel attaches wait on none, so a \
                     verb makes room where it can and attaches anyway",
                    reading.committed(),
                    left / 2
                ),
                hint: if left < MEDIA_LOW_IMAGES {
                    MEDIA_CAP_HINT.into()
                } else {
                    "nothing to free".into()
                },
                path: None,
            })
        }
        Err(error) if error.kind() == std::io::ErrorKind::Unsupported => None,
        Err(error) => Some(Finding {
            code: "media-notification-cap".into(),
            severity: FindingSeverity::Warning,
            message: format!(
                "could not read the registered IOMedia and the running image attaches that \
                 IOKit's match-notification cap is counted from: {error}"
            ),
            hint: "ioreg -r -c IOMedia -d 1 -w0 | grep -c '^+-o' counts the registered media"
                .into(),
            path: None,
        }),
    }
}

/// Departed APFS volumes and media the kernel has not freed (attach_budget.rs, `Departed`),
/// read from the same snapshot as the attach pool. A warning from the first one, never an error:
/// they take no attach-pool slot, so they stay out of `disk-image-attach-budget` and its remedy.
/// What holds them is not assumed: the hint attributes them by registry ID and changes nothing
/// (01a_apfs.md, "Spotlight media references").
fn departed_volume_references_finding(
    reading: &cowshed_apfs::attach_budget::KernelReading,
) -> Option<Finding> {
    reading.departed().map(|departed| Finding {
        code: "departed-volume-references".into(),
        severity: FindingSeverity::Warning,
        message: departed.to_string(),
        hint: cowshed_apfs::attach_budget::DEPARTED_ATTRIBUTION_HINT.into(),
        path: None,
    })
}

#[cfg(test)]
mod tests {
    use super::super::{doctor_report, host_storage_findings};
    use super::*;
    use cowshed_core::storage::bootstrap::{
        BUILD_CACHE_MOUNTPOINT, HostAction, HostSetupPlan, VolumeOutcome, VolumeRole, VolumeState,
    };

    fn planned_volume(name: &str, role: VolumeRole, state_before: VolumeState) -> VolumeOutcome {
        VolumeOutcome {
            name: name.to_owned(),
            role,
            state_before,
            action: "planned".to_owned(),
        }
    }

    #[test]
    fn doctor_reports_the_disk_image_classes_and_warns_on_a_leaked_device() {
        use cowshed_apfs::attach_budget::{
            ClassCount, DEPARTED_ATTRIBUTION_HINT, KernelReading, LEAKED_DEVICES_HINT,
        };
        let count = |instances, registered| ClassCount {
            instances,
            registered,
        };
        // This host at its first 150 on 2026-10-10, and just after the reboot.
        let leaking_host = KernelReading {
            image_devices: count(1090, 66),
            user_clients: count(1077, 66),
            apfs_volumes: count(1065, 81),
            media: count(1075, 80),
        };
        let rebooted_host = KernelReading {
            image_devices: count(178, 178),
            user_clients: count(178, 178),
            apfs_volumes: count(193, 193),
            media: count(185, 185),
        };
        let classes = |reading: KernelReading| {
            disk_image_attach_budget_finding(Ok(reading)).expect("the classes are always reported")
        };

        let fresh = classes(rebooted_host);
        assert_eq!(fresh.code, "disk-image-attach-budget");
        assert_eq!(fresh.severity, FindingSeverity::Info);
        assert_eq!(fresh.hint, "nothing to free");
        assert!(
            fresh.message.starts_with(
                "AppleDiskImageDevice 178 instances, 178 registered, 0 leaked; DIDeviceIOUserClient"
            ),
            "{fresh:?}"
        );
        // Leaked image devices warn and name what frees them; nothing is ever an error.
        let leaked = classes(leaking_host);
        assert_eq!(leaked.severity, FindingSeverity::Warning);
        assert_eq!(leaked.hint, LEAKED_DEVICES_HINT);
        assert!(
            leaked
                .message
                .starts_with("AppleDiskImageDevice 1090 instances, 66 registered, 1024 leaked"),
            "{leaked:?}"
        );

        let linux = std::io::Error::new(std::io::ErrorKind::Unsupported, "no classes");
        assert!(disk_image_attach_budget_finding(Err(linux)).is_none());
        let refused = std::io::Error::other("IOServiceMatching refused");
        let unread = disk_image_attach_budget_finding(Err(refused)).expect("a failed read");
        assert_eq!(unread.severity, FindingSeverity::Warning);

        // Departed volumes are a warning of their own whose hint attributes them.
        let departed = departed_volume_references_finding(&leaking_host)
            .expect("departed volumes are a finding");
        assert_eq!(departed.code, "departed-volume-references");
        assert_eq!(departed.severity, FindingSeverity::Warning);
        assert!(
            departed
                .message
                .starts_with("984 departed AppleAPFSVolume and 995 departed IOMedia instances: "),
            "{departed:?}"
        );
        assert_eq!(departed.hint, DEPARTED_ATTRIBUTION_HINT);
        assert_eq!(departed.path, None);

        // The first departed instance of either class warns, and the device classes do not move
        // for it.
        for (reading, counts) in [
            (
                KernelReading {
                    apfs_volumes: count(194, 193),
                    ..rebooted_host
                },
                "1 departed AppleAPFSVolume and 0 departed IOMedia instances: ",
            ),
            (
                KernelReading {
                    media: count(186, 185),
                    ..rebooted_host
                },
                "0 departed AppleAPFSVolume and 1 departed IOMedia instances: ",
            ),
        ] {
            let first = departed_volume_references_finding(&reading)
                .expect("the first departed instance is a finding");
            assert_eq!(first.severity, FindingSeverity::Warning);
            assert!(first.message.starts_with(counts), "{first:?}");
            let unmoved = classes(reading);
            assert_eq!(unmoved.severity, FindingSeverity::Info);
            assert_eq!(unmoved.hint, "nothing to free");
        }
        // Nothing departed, nothing to report.
        assert!(departed_volume_references_finding(&rebooted_host).is_none());

        // A warning never makes the host unhealthy: only an error does.
        assert!(doctor_report(vec![departed, leaked]).healthy);
    }

    /// The media finding counts what making room counts, warns while few images fit, past the
    /// ceiling too, never errs, and says nothing to free while many fit.
    #[test]
    fn doctor_reports_the_media_notification_cap_and_what_frees_media() {
        use cowshed_apfs::media_cap::{MEDIA_CAP_HINT, MediaReading};
        let finding = |registered| {
            media_notification_cap_finding(Ok(MediaReading {
                registered,
                attaching: 0,
            }))
            .expect("a reading is always reported")
        };
        // 16 media for the host's own disks are committed too.
        let roomy = finding(900);
        assert_eq!(roomy.code, "media-notification-cap");
        assert_eq!(roomy.severity, FindingSeverity::Info);
        assert_eq!(roomy.hint, "nothing to free");
        assert!(
            roomy.message.starts_with(
                "900 IOMedia registered (subclasses included) and 0 image attach(es) running; \
                 with the host's reserve that commits 916 of IOKit's 1024-entry"
            ) && roomy
                .message
                .contains("room for 35 more image(s), 17 workspace(s)"),
            "{roomy:?}"
        );
        let low = finding(960);
        assert_eq!(low.severity, FindingSeverity::Warning);
        assert_eq!(low.hint, MEDIA_CAP_HINT);
        assert!(low.message.contains("room for 15 more image(s)"), "{low:?}");
        let full = finding(1_100);
        assert_eq!(full.severity, FindingSeverity::Warning);
        assert_eq!(full.hint, MEDIA_CAP_HINT);
        assert!(
            full.message.contains("room for 0 more image(s)"),
            "{full:?}"
        );
        assert_eq!(
            media_notification_cap_finding(Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "linux"
            ))),
            None
        );
    }

    #[test]
    fn doctor_names_orphaned_disk_image_helpers_and_the_root_command() {
        use cowshed_apfs::disk_image_helpers::{HelperReading, OrphanedHelpers};
        use std::collections::BTreeSet;
        let helpers = BTreeSet::from([4353, 25101, 75251]);
        let orphans = OrphanedHelpers::among(&helpers, &BTreeSet::from([25101]));
        let finding =
            orphaned_disk_image_helpers_finding(Ok(HelperReading::Settled(orphans.clone())))
                .expect("orphans are a finding");
        assert_eq!(finding.code, "orphaned-disk-image-helpers");
        assert_eq!(finding.severity, FindingSeverity::Warning);
        assert_eq!(finding.hint, "sudo kill -9 4353 75251");
        assert!(finding.message.contains("(pid 4353, 75251)"), "{finding:?}");

        // While a disk-image tool runs, no pid is named.
        let busy = orphaned_disk_image_helpers_finding(Ok(HelperReading::from_reads(
            (orphans.clone(), true),
            (orphans, false),
        )))
        .expect("suspects are said");
        assert_eq!(busy.severity, FindingSeverity::Info);
        assert!(!busy.message.contains("4353"), "{busy:?}");
        assert!(!busy.hint.contains("kill"), "{busy:?}");

        let served = OrphanedHelpers::among(&helpers, &helpers);
        assert!(orphaned_disk_image_helpers_finding(Ok(HelperReading::Settled(served))).is_none());
        let linux = std::io::Error::new(std::io::ErrorKind::Unsupported, "no helpers");
        assert!(orphaned_disk_image_helpers_finding(Err(linux)).is_none());
        let refused = std::io::Error::other("proc_pidpath(1): refused");
        let finding = orphaned_disk_image_helpers_finding(Err(refused)).expect("a failed read");
        assert_eq!(finding.code, "disk-image-helpers");
    }

    /// The build-cache volume reports with the store's findings, each naming the observed and
    /// canonical paths and prescribing `cowshed setup` (01a_apfs.md, "Dedicated build-cache
    /// volume").
    #[test]
    fn the_build_cache_volume_reports_the_store_findings_at_its_mountpoint() {
        let name = cowshed_core::storage::bootstrap::APFS_BUILD_CACHE_VOLUME;
        let canonical = Path::new(BUILD_CACHE_MOUNTPOINT);
        let observed = PathBuf::from("/Volumes/cowshed.build-cache");
        let cells = [
            (VolumeState::MountedValid, "host-volume", "mounted at"),
            (VolumeState::Absent, "host-volume-absent", "absent"),
            (VolumeState::Detached, "mount", "not mounted"),
            (
                VolumeState::MountedIncomplete,
                "marker",
                "marker missing or invalid",
            ),
            (
                VolumeState::MisMounted {
                    mounted_at: observed.clone(),
                },
                "mount",
                "/Volumes/cowshed.build-cache",
            ),
        ];
        for (state, code, says) in cells {
            let findings = host_storage_findings(&HostSetupPlan {
                actions: vec![HostAction::EncryptVolume {
                    name: name.into(),
                    uuid: "BC-UUID".into(),
                    size_bytes: 4096,
                }],
                volumes: vec![planned_volume(name, VolumeRole::BuildCache, state.clone())],
                requires_authorization: true,
                non_destructive: true,
            });
            let volume = findings
                .iter()
                .find(|finding| finding.message.starts_with(&format!("{name}:")))
                .unwrap_or_else(|| panic!("a finding for {state:?}"));
            assert_eq!(volume.code, code, "{}", volume.message);
            assert_eq!(volume.path.as_deref(), Some(canonical));
            assert!(volume.message.contains(says), "{}", volume.message);
            assert!(
                volume.message.contains(&canonical.display().to_string()),
                "{}",
                volume.message
            );
            if state != VolumeState::MountedValid {
                assert_eq!(volume.hint, "cowshed setup");
            }
            assert!(
                findings
                    .iter()
                    .any(|finding| finding.code == "host-filevault"
                        && finding.message.contains(name)
                        && finding.hint == "cowshed setup")
            );
        }
    }
}
