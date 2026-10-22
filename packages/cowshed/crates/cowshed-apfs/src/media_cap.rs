//! IOKit's match-notification queue and the live media it caps (01a_apfs.md, "The IOMedia
//! match-notification ceiling").
//!
//! DiskImages2 finds the device an attach creates by waiting, with no overall timeout, in
//! `-[DIDeviceHandle waitForDeviceWithError:]` for an IOMedia match notification that names it.
//! One notification queues at most `IOServiceUserNotification::kMaxOutstanding` = 1024 matches,
//! and its handler drops every match past them without a word (XNU
//! `iokit/Kernel/IOUserClient.cpp`: the constant at line 809, the drop at lines 1031-1038).
//! Measured 2026-10-22 with 352 images attached: `IOServiceGetMatchingServices` matched 1081
//! registered IOMedia, an unfiltered `IOServiceAddMatchingNotification(IOServiceMatched, IOMedia)`
//! delivered exactly 1024 of them, and every new attach hung. A registration filtered to the new
//! device, made after it was published, does find it, so the cap is on what one unfiltered IOMedia
//! match enumerates: every registered IOMedia, subclasses included. A count lowered afterwards
//! replays nothing, so an attach that already lost its match stays hung; the only defense is never
//! to start one past the cap.
//!
//! The gate here runs where every disk child spawns ([`crate::SystemCommandRunner`]), inside the
//! storage lease: no `diskutil image attach` or `hdiutil attach` starts while the media it adds
//! would cross the cap. It is refused as [`MediaCapReached`] instead. Making room, by detaching
//! workspaces nothing uses, belongs to the layer that knows workspaces (`cowshed_core::media_room`).
//! This cap is separate from the attach pool's leaked devices ([`crate::attach_budget`]), which
//! fail an attach with error code 150 rather than hanging it.

use std::fmt;
use std::io;

use crate::{CommandRequest, CommandRunError, CommandRunFailure};

/// XNU's `IOServiceUserNotification::kMaxOutstanding`: the matches one notification queues before
/// its handler drops the rest (module docs).
pub const NOTIFICATION_CAP: u64 = 1024;

/// The IOMedia, subclasses included, one attached APFS image registers: its whole disk
/// (`IOMedia`), its container's synthesized disk (`AppleAPFSMedia`) and its volume
/// (`AppleAPFSVolume`). Measured 2026-10-22 with 342 images attached: 347 `IOMedia`, 346
/// `AppleAPFSMedia` and 357 `AppleAPFSVolume` registered, the host's own disks included.
pub const MEDIA_PER_IMAGE: u64 = 3;

/// Media kept free for disks the host attaches on its own, which no cowshed gate sees coming: an
/// external drive's partitions and volumes, a Time Machine snapshot mount.
pub const HOST_MEDIA_HEADROOM: u64 = 16;

/// What frees media: the next step every refusal's caller names.
pub const MEDIA_CAP_HINT: &str = "detach or remove workspaces nothing uses (`cowshed detach \
     <ws>`, `cowshed rm <ws>`): an attached image holds 3 IOMedia, a workspace with its build \
     volume 6; `cowshed doctor` reports media-notification-cap";

/// The media one instant shows the cap.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MediaReading {
    /// Registered IOMedia, subclasses included: what an unfiltered IOMedia match enumerates.
    pub registered: u64,
    /// Image attaches running on the host, any user's (`diskutil image attach`, `hdiutil
    /// attach`). One whose media are registered already counts twice, never zero times.
    pub attaching: u64,
}

impl MediaReading {
    /// The media this reading spends before one more attach: the registered, what each running
    /// attach and every other member of the storage phase may still add (each may pass this gate
    /// before any of them spawns; 05_gateway.md, "Disk-lifecycle lease"), and the host's headroom.
    pub fn committed(self) -> u64 {
        let peers = u64::from(cowshed_gateway_types::DISK_PHASE_MEMBERS.get()) - 1;
        self.registered
            .saturating_add(
                self.attaching
                    .saturating_add(peers)
                    .saturating_mul(MEDIA_PER_IMAGE),
            )
            .saturating_add(HOST_MEDIA_HEADROOM)
    }

    /// The media to free so that `images` more images keep the total below `cap`, as the
    /// ceiling requires (it must not be reached); 0 when they fit.
    pub fn excess(self, cap: u64, images: u64) -> u64 {
        self.committed()
            .saturating_add(images.saturating_mul(MEDIA_PER_IMAGE))
            .saturating_add(1)
            .saturating_sub(cap)
    }

    /// How many more images keep the total below `cap`.
    pub fn images_left(self, cap: u64) -> u64 {
        cap.saturating_sub(1).saturating_sub(self.committed()) / MEDIA_PER_IMAGE
    }

    /// Refuse `images` more attaches whose media would reach `cap`.
    pub fn admits(self, cap: u64, images: u64) -> Result<(), MediaCapReached> {
        match self.excess(cap, images) {
            0 => Ok(()),
            excess => Err(MediaCapReached {
                cap,
                images,
                reading: self,
                excess,
            }),
        }
    }
}

impl fmt::Display for MediaReading {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} IOMedia registered (subclasses included) and {} image attach(es) running",
            self.registered, self.attaching
        )
    }
}

/// The refusal to start attaches whose media would cross the cap: nothing was attached.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MediaCapReached {
    pub cap: u64,
    pub images: u64,
    pub reading: MediaReading,
    /// The media past the cap.
    pub excess: u64,
}

impl fmt::Display for MediaCapReached {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "MediaCapReached: {} more disk image(s) would add {} IOMedia to {}, beside the storage \
             phase's other attaches and {HOST_MEDIA_HEADROOM} kept for the host's own disks: {} \
             IOMedia too many to stay below the {}-entry IOKit match-notification queue (XNU \
             IOServiceUserNotification::kMaxOutstanding), which drops the match DiskImages2 waits \
             for, so the attach would hang forever; nothing was attached",
            self.images,
            self.images.saturating_mul(MEDIA_PER_IMAGE),
            self.reading,
            self.excess,
            self.cap
        )
    }
}

impl std::error::Error for MediaCapReached {}

/// The media now.
#[cfg(target_os = "macos")]
pub fn read() -> io::Result<MediaReading> {
    Ok(MediaReading {
        registered: crate::registered_media()?,
        attaching: crate::disk_image_helpers::running_attaches()?,
    })
}

#[cfg(not(target_os = "macos"))]
pub fn read() -> io::Result<MediaReading> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "this kernel has no IOKit match notifications",
    ))
}

/// Refuse an image attach whose media would cross [`NOTIFICATION_CAP`], before it spawns; every
/// other command passes. A reading that fails refuses too: an attach past the cap never returns,
/// while a refused one can run again.
pub(crate) fn refuse_attach_past_cap(request: &CommandRequest) -> Result<(), CommandRunError> {
    if !crate::is_image_attach(request) {
        return Ok(());
    }
    let failure = match read() {
        Ok(reading) => match reading.admits(NOTIFICATION_CAP, 1) {
            Ok(()) => return Ok(()),
            Err(reached) => CommandRunFailure::MediaCap(reached),
        },
        Err(error) => CommandRunFailure::MediaUnread(error),
    };
    Err(CommandRunError {
        request: request.clone(),
        failure,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const PEERS: u64 = 7;

    fn reading(registered: u64, attaching: u64) -> MediaReading {
        MediaReading {
            registered,
            attaching,
        }
    }

    /// Every running attach and every other member of the storage phase reserves an image's
    /// media, and the host keeps its headroom.
    #[test]
    fn committed_reserves_running_attaches_phase_peers_and_host_headroom() {
        assert_eq!(
            reading(900, 0).committed(),
            900 + PEERS * MEDIA_PER_IMAGE + HOST_MEDIA_HEADROOM
        );
        assert_eq!(
            reading(900, 2).committed(),
            900 + (2 + PEERS) * MEDIA_PER_IMAGE + HOST_MEDIA_HEADROOM
        );
        assert_eq!(reading(u64::MAX, u64::MAX).committed(), u64::MAX);
    }

    /// An attach that leaves the total one below the cap fits; one that reaches the cap is
    /// refused, by the one medium too many.
    #[test]
    fn an_attach_fits_below_the_cap_and_no_further() {
        let below = NOTIFICATION_CAP - 1 - MEDIA_PER_IMAGE - reading(0, 0).committed();
        assert_eq!(reading(below, 0).admits(NOTIFICATION_CAP, 1), Ok(()));
        assert_eq!(reading(below, 0).images_left(NOTIFICATION_CAP), 1);
        let refused = reading(below + 1, 0)
            .admits(NOTIFICATION_CAP, 1)
            .expect_err("an attach that reaches the cap");
        assert_eq!(refused.excess, 1);
        assert_eq!(reading(below + 1, 0).images_left(NOTIFICATION_CAP), 0);
        // A running attach reserves its image's media too.
        assert_eq!(
            reading(below, 1)
                .admits(NOTIFICATION_CAP, 1)
                .map_err(|reached| reached.excess),
            Err(MEDIA_PER_IMAGE)
        );
    }

    /// The refusal says what it counted, why the attach would hang, and that nothing ran.
    #[test]
    fn the_refusal_names_the_count_the_cap_and_that_nothing_was_attached() {
        let said = reading(1050, 1)
            .admits(NOTIFICATION_CAP, 2)
            .expect_err("over the cap")
            .to_string();
        assert!(
            said.starts_with(
                "MediaCapReached: 2 more disk image(s) would add 6 IOMedia to 1050 IOMedia \
                 registered (subclasses included) and 1 image attach(es) running"
            ),
            "{said}"
        );
        assert!(said.contains("kMaxOutstanding"), "{said}");
        assert!(said.contains("nothing was attached"), "{said}");
        assert!(said.ends_with("nothing was attached"), "{said}");
    }

    /// Only an image attach is gated: a detach or an info read never waits on the media count.
    #[test]
    fn only_an_image_attach_is_gated() {
        for request in [
            CommandRequest::new(crate::DISKUTIL, ["image", "info", "/x.dmg"]),
            CommandRequest::new(crate::HDIUTIL, ["detach", "/dev/disk9"]),
            CommandRequest::new(crate::DISKUTIL, ["info", "/dev/disk9"]),
        ] {
            assert!(refuse_attach_past_cap(&request).is_ok(), "{request:?}");
        }
    }
}
