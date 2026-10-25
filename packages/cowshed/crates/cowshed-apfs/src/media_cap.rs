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
//! Nothing here refuses an attach: cowshed attaches in the kernel, which waits on no match
//! notification, measured to attach and detach cleanly up to 1,084 registered IOMedia. The
//! ceiling stays a soft target, since any other unfiltered IOMedia notification on the host still
//! misses matches past it: making room, by detaching workspaces nothing uses, belongs to the layer
//! that knows workspaces (`cowshed_core::media_room`), and `doctor` reports the count.

use std::fmt;
use std::io;
use std::path::Path;

use cowshed_os::namespace::HostNamespace;

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

/// What frees media: the next step `doctor` and making room name.
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
    /// attach may still add, and the host's headroom.
    pub fn committed(self) -> u64 {
        self.registered
            .saturating_add(self.attaching.saturating_mul(MEDIA_PER_IMAGE))
            .saturating_add(HOST_MEDIA_HEADROOM)
    }

    /// The media to free so that `images` more images keep the total below `cap`; 0 when they
    /// fit.
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

/// The media `namespace` makes room by (01_storage.md, "Host namespace"): production's are the
/// host's, every user's ([`read`]); a test namespace's are those the images attached from its own
/// `store` register, so no other user's attach moves a reading its fixture decides on.
pub fn read_namespace(namespace: HostNamespace, store: &Path) -> io::Result<MediaReading> {
    match namespace {
        HostNamespace::Production => read(),
        HostNamespace::Test(_) => read_store(store),
    }
}

/// The IOMedia the images attached from backing files below `store` register, from the I/O
/// Registry's attachment inventory. Cowshed attaches a store's images in the kernel, never through
/// a disk tool, so none of them is a running attach [`read`] counts.
#[cfg(target_os = "macos")]
fn read_store(store: &Path) -> io::Result<MediaReading> {
    let mut registered = 0_u64;
    for attached in crate::io_registry::attached_disk_images()? {
        if matches!(&attached.source, crate::DiskImageSource::File(image) if image.starts_with(store))
        {
            let media = u64::try_from(attached.media.len())
                .map_err(|_| io::Error::other("more media than a u64 counts"))?;
            registered = registered.saturating_add(media);
        }
    }
    Ok(MediaReading {
        registered,
        attaching: 0,
    })
}

#[cfg(not(target_os = "macos"))]
fn read_store(_store: &Path) -> io::Result<MediaReading> {
    read()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reading(registered: u64, attaching: u64) -> MediaReading {
        MediaReading {
            registered,
            attaching,
        }
    }

    /// Every running attach reserves an image's media, and the host keeps its headroom.
    #[test]
    fn committed_reserves_running_attaches_and_host_headroom() {
        assert_eq!(reading(900, 0).committed(), 900 + HOST_MEDIA_HEADROOM);
        assert_eq!(
            reading(900, 2).committed(),
            900 + 2 * MEDIA_PER_IMAGE + HOST_MEDIA_HEADROOM
        );
        assert_eq!(reading(u64::MAX, u64::MAX).committed(), u64::MAX);
    }

    /// An image that leaves the total one below the ceiling fits; one that reaches it is over by
    /// the one medium too many, and no image is left.
    #[test]
    fn an_image_fits_below_the_ceiling_and_no_further() {
        let below = NOTIFICATION_CAP - 1 - MEDIA_PER_IMAGE - reading(0, 0).committed();
        assert_eq!(reading(below, 0).excess(NOTIFICATION_CAP, 1), 0);
        assert_eq!(reading(below, 0).images_left(NOTIFICATION_CAP), 1);
        assert_eq!(reading(below + 1, 0).excess(NOTIFICATION_CAP, 1), 1);
        assert_eq!(reading(below + 1, 0).images_left(NOTIFICATION_CAP), 0);
        assert_eq!(
            reading(below, 1).excess(NOTIFICATION_CAP, 1),
            MEDIA_PER_IMAGE
        );
    }
}
