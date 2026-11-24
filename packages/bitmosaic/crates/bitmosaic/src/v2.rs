//! Format 2 unified views, closed-form §7 selection, encoders, `u64` directory, and patching
//! (`specs/bitmosaic/01-format.md`).
//!
//! All Format 2 borrowed views are zero-allocation and never read a byte past their image extent
//! (§12.2). Format 1 images (`42 4D 53 ...`) remain 100% supported through [`Image`] and
//! [`BitmosaicView`] / [`Bitmosaic64View`].

use crate::admission::{AdmissionClass, AdmissionError};
use crate::bitmap::{
    BITMAP_LAST_MEMBER, BITMAP_TAG, BitmapRange, BitmapView, bitmap_len, write_bitmap,
};
use crate::codec::{
    PatchError, read_u32_at, read_u64_at, read_uleb, read_uleb_checked, uleb_len, width_of,
    write_uleb,
};
use crate::ef::{EF2_CONTROL, Ef2Layout, Ef2Range, Ef2View, ef2_len, encode_ef2_into, write_ef2};
use crate::list::{
    LIST_MAX_MEMBERS, ListRange, ListView, SingleRange, SingleView, list_len_from, write_list,
    write_single,
};
use crate::patch::{PatchReport, PatchScratch, Witness};
use crate::range::Range;
use crate::wire::{
    Bitmosaic64View, BitmosaicView, BitmosaicViewRange, Geometry, KeyWidth, WORDS_AT,
    forest_header_len, image_header, runs_payload_len, slot_bits_for_span, words_payload_len,
};
use crate::{
    ARRAY_MAX, Bitmosaic, Bitmosaic64, CHUNK_WORDS, CONE_MAX_EPS, CONE_MAX_LEN, DIR_STRIDE,
};

/// Control byte of a Format 2 `u32` directory (§3, §8).
pub(crate) const DIR32_CONTROL: u8 = 0x01;
/// Control byte of a Format 2 `u64` directory (§3, §9).
pub(crate) const DIR64_CONTROL: u8 = 0x03;

/// The canonical empty set in Format 2, for both `u32` and `u64` (§3): Bitmap of 0 bytes (`0x80`).
pub const EMPTY_V2_IMAGE: [u8; 1] = [BITMAP_TAG];

/// Which wire format an image uses (§1, §10).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Format {
    Format1,
    Format2,
}

/// Which form an image has (§3, §10).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ImageForm {
    Bitmap,
    Single,
    List,
    EliasFano,
    Directory,
    Directory64,
}

/// Bounded constant-work framing summary for any Format 1 or Format 2 image (§11).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ImageHeaderV2 {
    pub format: Format,
    pub form: ImageForm,
    pub width: KeyWidth,
    pub extent: usize,
}

/// Inspect any Format 1 or Format 2 image header in bounded O(1) work without scanning its body
/// (§11 `image_header`).
pub fn image_header_v2(bytes: &[u8]) -> Result<ImageHeaderV2, AdmissionError> {
    let Some(&control) = bytes.first() else {
        return Err(AdmissionError::new(
            AdmissionClass::Extent,
            0,
            "an image has no control byte",
        ));
    };
    if control == 0x42 {
        let h = image_header(bytes).ok_or_else(|| {
            AdmissionError::new(AdmissionClass::Kind, 0, "malformed format-1 header")
        })?;
        let form = match h.width {
            KeyWidth::U64 => ImageForm::Directory64,
            KeyWidth::U32 => {
                if bytes.get(3).is_some_and(|b| b & 2 != 0) {
                    ImageForm::EliasFano
                } else {
                    ImageForm::Directory
                }
            }
        };
        return Ok(ImageHeaderV2 {
            format: Format::Format1,
            form,
            width: h.width,
            extent: h.encoded_len,
        });
    }
    if control == DIR64_CONTROL {
        let extent = dir64_extent(bytes)?;
        return Ok(ImageHeaderV2 {
            format: Format::Format2,
            form: ImageForm::Directory64,
            width: KeyWidth::U64,
            extent,
        });
    }
    let (form, extent) = u32_header_v2(bytes)?;
    Ok(ImageHeaderV2 {
        format: Format::Format2,
        form,
        width: KeyWidth::U32,
        extent,
    })
}

/// O(1) Format 2 `u32` form + extent check (refuses `0x03`, `0x40..=0x43`, `0x60..=0x63`, `0x00`).
fn u32_header_v2(bytes: &[u8]) -> Result<(ImageForm, usize), AdmissionError> {
    let Some(&control) = bytes.first() else {
        return Err(AdmissionError::new(
            AdmissionClass::Extent,
            0,
            "an image has no control byte",
        ));
    };
    if control & BITMAP_TAG != 0 {
        let n = usize::from(control & !BITMAP_TAG);
        let extent = 1 + n;
        if bytes.len() < extent {
            return Err(AdmissionError::new(
                AdmissionClass::Extent,
                0,
                "a Bitmap is cut short of its bytes",
            ));
        }
        return Ok((ImageForm::Bitmap, extent));
    }
    match control {
        0x00 => Err(AdmissionError::new(
            AdmissionClass::Kind,
            0,
            "the control 00 is no image",
        )),
        DIR32_CONTROL => {
            let mut at = 1usize;
            let body_bytes =
                read_uleb_checked(bytes, &mut at, u32::MAX, AdmissionClass::Extent)? as usize;
            let total = at.checked_add(body_bytes).ok_or_else(|| {
                AdmissionError::new(
                    AdmissionClass::Extent,
                    1,
                    "directory extent overflows the address space",
                )
            })?;
            if total > u32::MAX as usize || total > bytes.len() {
                return Err(AdmissionError::new(
                    AdmissionClass::Extent,
                    1,
                    "directory body exceeds its available u32 extent",
                ));
            }
            Ok((ImageForm::Directory, total))
        }
        EF2_CONTROL => {
            let mut at = 1usize;
            let count_at = at;
            let len = u64::from(read_uleb_checked(
                bytes,
                &mut at,
                u32::MAX,
                AdmissionClass::Count,
            )?) + 1;
            if len < 2 {
                return Err(AdmissionError::new(
                    AdmissionClass::Count,
                    count_at,
                    "Elias-Fano requires at least two members",
                ));
            }
            let base = read_uleb_checked(bytes, &mut at, u32::MAX, AdmissionClass::PlaneRange)?;
            let span_at = at;
            let span = read_uleb_checked(bytes, &mut at, u32::MAX, AdmissionClass::PlaneRange)?;
            if base.checked_add(span).is_none() {
                return Err(AdmissionError::new(
                    AdmissionClass::PlaneRange,
                    span_at,
                    "EF base plus span overflows u32",
                ));
            }
            if len > u64::from(span) + 1 {
                return Err(AdmissionError::new(
                    AdmissionClass::Count,
                    count_at,
                    "EF cardinality exceeds its member span",
                ));
            }
            let layout = Ef2Layout::new(len, u64::from(span)).ok_or_else(|| {
                AdmissionError::new(
                    AdmissionClass::Extent,
                    at,
                    "EF geometry exceeds its position domain",
                )
            })?;
            let total = at.checked_add(layout.payload_bytes()).ok_or_else(|| {
                AdmissionError::new(
                    AdmissionClass::Extent,
                    at,
                    "EF extent overflows the address space",
                )
            })?;
            if total > bytes.len() {
                return Err(AdmissionError::new(
                    AdmissionClass::Extent,
                    at,
                    "EF image is cut short of its planes",
                ));
            }
            Ok((ImageForm::EliasFano, total))
        }
        DIR64_CONTROL => Err(AdmissionError::new(
            AdmissionClass::Kind,
            0,
            "a u64 directory was handed to a u32 reader",
        )),
        0x20..=0x23 => {
            let extent = 1 + usize::from(control & 3) + 1;
            if bytes.len() < extent {
                return Err(AdmissionError::new(
                    AdmissionClass::Extent,
                    0,
                    "a Single is cut short of its member bytes",
                ));
            }
            Ok((ImageForm::Single, extent))
        }
        0x40..=0x43 => Err(AdmissionError::new(
            AdmissionClass::Kind,
            0,
            "the control is not format 2",
        )),
        0x60..=0x63 => Err(AdmissionError::new(
            AdmissionClass::Kind,
            0,
            "the control is unassigned",
        )),
        _ => {
            let k = usize::from((control >> 2) & 7) + 1;
            let wa = usize::from(control & 3) + 1;
            let wd = usize::from(control >> 5) + 1;
            let extent = 1 + wa + (k - 1) * wd;
            if bytes.len() < extent {
                return Err(AdmissionError::new(
                    AdmissionClass::Extent,
                    0,
                    "a List is cut short of its fields",
                ));
            }
            Ok((ImageForm::List, extent))
        }
    }
}

/// Geometry of a Format 2 `u64` directory (§9): no root-bit plane because every child is a
/// complete sealed Format 2 `u32` image.
#[derive(Clone, Copy, Debug)]
struct Geometry64V2 {
    keys_at: usize,
    before_at: usize,
    offsets_at: usize,
    children_at: usize,
}

impl Geometry64V2 {
    fn new(m: usize, keys_at: usize) -> Option<Self> {
        let before_at = keys_at.checked_add(m.checked_mul(4)?)?;
        let offsets_at = before_at.checked_add(m.saturating_sub(2).checked_mul(8)?)?;
        let children_at = offsets_at.checked_add(m.saturating_sub(1).checked_mul(4)?)?;
        Some(Self {
            keys_at,
            before_at,
            offsets_at,
            children_at,
        })
    }
}

fn dir64_extent(bytes: &[u8]) -> Result<usize, AdmissionError> {
    let mut at = 1usize;
    let count_at = at;
    let m = read_uleb_checked(bytes, &mut at, u32::MAX, AdmissionClass::Count)?
        .checked_add(1)
        .ok_or_else(|| {
            AdmissionError::new(
                AdmissionClass::Count,
                count_at,
                "u64 directory child count overflows u32",
            )
        })? as usize;
    let geom = Geometry64V2::new(m, at).ok_or_else(|| {
        AdmissionError::new(
            AdmissionClass::Extent,
            at,
            "u64 directory geometry overflows the address space",
        )
    })?;
    if bytes.len() < geom.children_at {
        return Err(AdmissionError::new(
            AdmissionClass::Extent,
            at,
            "u64 directory header is truncated",
        ));
    }
    let last_rel = if m == 1 {
        0
    } else {
        read_u32_at(bytes, geom.offsets_at + 4 * (m - 2)) as usize
    };
    let last_start = geom.children_at.checked_add(last_rel).ok_or_else(|| {
        AdmissionError::new(
            AdmissionClass::Extent,
            geom.offsets_at + 4 * (m - 2),
            "u64 last child offset overflows",
        )
    })?;
    let last_slice = bytes.get(last_start..).ok_or_else(|| {
        AdmissionError::new(
            AdmissionClass::Placement,
            if m == 1 {
                geom.children_at
            } else {
                geom.offsets_at + 4 * (m - 2)
            },
            "u64 last child starts past the image",
        )
    })?;
    let (_, last_len) = u32_header_v2(last_slice).map_err(|e| e.shifted(last_start))?;
    last_start.checked_add(last_len).ok_or_else(|| {
        AdmissionError::new(
            AdmissionClass::Extent,
            last_start,
            "u64 directory extent overflows",
        )
    })
}

// ── Unified Format 1 + Format 2 Image enum (§3, §10) ─────────────────────

/// An image of any format (Format 1 or Format 2), identified by its first byte (§3, §10).
#[derive(Clone, Copy, Debug)]
pub enum Image<'a> {
    /// Format 2: Bitmap root (`0x80..=0xFF`, empty set is `0x80`).
    Bitmap(BitmapView<'a>),
    /// Format 2: Single root (`0x20..=0x23`).
    Single(SingleView<'a>),
    /// Format 2: List root (`0x04..=0x7F` with bits 2..4 nonzero).
    List(ListView<'a>),
    /// Format 2: Lean Elias-Fano root (`0x02`).
    EliasFano(Ef2View<'a>),
    /// Format 2: `u32` directory (`0x01`).
    Directory(BitmosaicView<'a>),
    /// Format 2: `u64` directory (`0x03`).
    Directory64(Dir64V2View<'a>),
    /// Format 1 (`42 4D 53`), a `u32` set, verified.
    Format1(BitmosaicView<'a>),
    /// Format 1 (`42 4D 53`), a `u64` set, verified.
    Format1Wide(Bitmosaic64View<'a>),
}

impl<'a> Image<'a> {
    /// Admit any Format 1 or Format 2 image at the start of `bytes`.
    /// Format 1 images are admitted as `open_verified`; Format 2 images are admitted at `open`'s
    /// structural level (use [`Self::open_verified`] or [`Self::open_canonical`] for member and
    /// canonical verification).
    pub fn open(bytes: &'a [u8]) -> Result<Self, AdmissionError> {
        Self::open_mode(bytes, false, false)
    }

    /// Admit and verify every member, derived plane, and sealed-or-forest canonicality (§11).
    pub fn open_verified(bytes: &'a [u8]) -> Result<Self, AdmissionError> {
        Self::open_mode(bytes, true, false)
    }

    /// [`Self::open_verified`] plus require the sealed canonical image (§7.1, §11).
    pub fn open_canonical(bytes: &'a [u8]) -> Result<Self, AdmissionError> {
        Self::open_mode(bytes, true, true)
    }

    /// O(1) attach for an already-admitted Format 1 or Format 2 image.
    pub fn open_admitted(bytes: &'a [u8]) -> Option<Self> {
        match bytes {
            [] => None,
            [control, ..] if control & BITMAP_TAG != 0 => {
                BitmapView::open_admitted(bytes).map(Self::Bitmap)
            }
            [0x42, 0x4D, 0x53, control, ..] if control & 1 != 0 => {
                Bitmosaic64View::open_admitted(bytes).map(Self::Format1Wide)
            }
            [0x42, ..] => BitmosaicView::open_admitted(bytes).map(Self::Format1),
            [DIR32_CONTROL, ..] => open_v2_dir::<false>(bytes, false).ok().map(Self::Directory),
            [EF2_CONTROL, ..] => Ef2View::open_admitted(bytes).map(Self::EliasFano),
            [DIR64_CONTROL, ..] => Dir64V2View::open_admitted(bytes).map(Self::Directory64),
            [0x20..=0x23, ..] => SingleView::open_admitted(bytes).map(Self::Single),
            [0x00, ..] | [0x40..=0x43, ..] | [0x60..=0x63, ..] => None,
            [_, ..] => ListView::open_admitted(bytes).map(Self::List),
        }
    }

    fn open_mode(bytes: &'a [u8], verify: bool, canonical: bool) -> Result<Self, AdmissionError> {
        match bytes {
            [] => Err(AdmissionError::new(
                AdmissionClass::Extent,
                0,
                "an image has no control byte",
            )),
            [0x42, 0x4D, 0x53, control, ..] if control & 1 != 0 => {
                Bitmosaic64View::open_verified(bytes).map(Self::Format1Wide)
            }
            [0x42, ..] => BitmosaicView::open_verified(bytes).map(Self::Format1),
            [DIR64_CONTROL, ..] => {
                let v = if verify {
                    Dir64V2View::open_verified(bytes)?
                } else {
                    Dir64V2View::open(bytes)?
                };
                Ok(Self::Directory64(v))
            }
            [0x00, ..] => Err(AdmissionError::new(
                AdmissionClass::Kind,
                0,
                "the control 00 is no image",
            )),
            [0x40..=0x43, ..] => Err(AdmissionError::new(
                AdmissionClass::Kind,
                0,
                "the control is kept for a later format",
            )),
            [0x60..=0x63, ..] => Err(AdmissionError::new(
                AdmissionClass::Kind,
                0,
                "the control is unassigned",
            )),
            _ => {
                let v = if canonical {
                    V2View::open_canonical(bytes)?
                } else if verify {
                    V2View::open_verified(bytes)?
                } else {
                    V2View::open(bytes)?
                };
                Ok(match v {
                    V2View::Bitmap(b) => Self::Bitmap(b),
                    V2View::Single(s) => Self::Single(s),
                    V2View::List(l) => Self::List(l),
                    V2View::EliasFano(e) => Self::EliasFano(e),
                    V2View::Directory(d) => Self::Directory(d),
                })
            }
        }
    }

    /// Total byte extent of the image.
    pub fn extent(&self) -> usize {
        match self {
            Self::Bitmap(v) => v.extent(),
            Self::Single(v) => v.extent(),
            Self::List(v) => v.extent(),
            Self::EliasFano(v) => v.extent(),
            Self::Directory(v) | Self::Format1(v) => v.serialized_len(),
            Self::Directory64(v) => v.serialized_len(),
            Self::Format1Wide(v) => v.serialized_len(),
        }
    }

    /// Cardinality of the set.
    pub fn len(&self) -> u64 {
        match self {
            Self::Bitmap(v) => u64::from(v.len()),
            Self::Single(_) => 1,
            Self::List(v) => u64::from(v.len()),
            Self::EliasFano(v) => v.len(),
            Self::Directory(v) | Self::Format1(v) => v.len(),
            Self::Directory64(v) => v.len(),
            Self::Format1Wide(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

fn open_v2_dir<'a, const CHECK: bool>(
    bytes: &'a [u8],
    verify: bool,
) -> Result<BitmosaicView<'a>, AdmissionError> {
    let Some(&control) = bytes.first() else {
        return Err(AdmissionError::new(
            AdmissionClass::Extent,
            0,
            "a directory has no control byte",
        ));
    };
    if control != DIR32_CONTROL {
        return Err(AdmissionError::new(
            AdmissionClass::Kind,
            0,
            "the control is not a u32 directory's",
        ));
    }
    let mut at = 1usize;
    let body_bytes = read_uleb_checked(bytes, &mut at, u32::MAX, AdmissionClass::Extent)? as usize;
    let total = at.checked_add(body_bytes).ok_or_else(|| {
        AdmissionError::new(
            AdmissionClass::Extent,
            1,
            "directory extent overflows the address space",
        )
    })?;
    if total > u32::MAX as usize || total > bytes.len() {
        return Err(AdmissionError::new(
            AdmissionClass::Extent,
            1,
            "directory body exceeds its available u32 extent",
        ));
    }
    BitmosaicView::open_forest_v2::<CHECK>(&bytes[..total], at, verify)
}

// ── V2View (Format 2 u32 unified view) ───────────────────────────────────

/// Borrowed Format 2 `u32` set view across all five Format 2 `u32` forms:
/// Bitmap, Single, List, Lean Elias-Fano, and `u32` Directory.
///
/// Allocation-free on every admission and read path; never reads past its extent (§12.2).
#[derive(Clone, Copy, Debug)]
pub enum V2View<'a> {
    Bitmap(BitmapView<'a>),
    Single(SingleView<'a>),
    List(ListView<'a>),
    EliasFano(Ef2View<'a>),
    Directory(BitmosaicView<'a>),
}

impl<'a> V2View<'a> {
    /// Structural admission (§11 `open`). Refuses Format 1 (`0x42`), `u64` directory (`0x03`),
    /// `0x00`, and unassigned/later-format controls as `Kind`.
    pub fn open(bytes: &'a [u8]) -> Result<Self, AdmissionError> {
        let Some(&control) = bytes.first() else {
            return Err(AdmissionError::new(
                AdmissionClass::Extent,
                0,
                "an image has no control byte",
            ));
        };
        if control & BITMAP_TAG != 0 {
            return BitmapView::open(bytes).map(Self::Bitmap);
        }
        match control {
            0x00 => Err(AdmissionError::new(
                AdmissionClass::Kind,
                0,
                "the control 00 is no image",
            )),
            DIR32_CONTROL => open_v2_dir::<true>(bytes, false).map(Self::Directory),
            EF2_CONTROL => Ef2View::open(bytes).map(Self::EliasFano),
            DIR64_CONTROL => Err(AdmissionError::new(
                AdmissionClass::Kind,
                0,
                "a u64 directory was handed to a u32 reader",
            )),
            0x20..=0x23 => SingleView::open(bytes).map(Self::Single),
            0x40..=0x43 => Err(AdmissionError::new(
                AdmissionClass::Kind,
                0,
                "the control is not format 2",
            )),
            0x60..=0x63 => Err(AdmissionError::new(
                AdmissionClass::Kind,
                0,
                "the control is unassigned",
            )),
            _ => ListView::open(bytes).map(Self::List),
        }
    }

    /// Full admission (§11 `open_verified`): structural checks + every member & derived plane +
    /// proves the image is either the sealed (§7.1) or forest (§7.2) image of its set.
    pub fn open_verified(bytes: &'a [u8]) -> Result<Self, AdmissionError> {
        Self::open_verified_inner(bytes, false)
    }

    /// Canonical admission (§11 `open_canonical`): [`Self::open_verified`] + proves the image is
    /// the unique sealed image of its set (§7.1).
    pub fn open_canonical(bytes: &'a [u8]) -> Result<Self, AdmissionError> {
        Self::open_verified_inner(bytes, true)
    }

    /// O(1) attach for an already-admitted Format 2 `u32` image.
    #[inline]
    pub fn open_admitted(bytes: &'a [u8]) -> Option<Self> {
        let control = *bytes.first()?;
        if control & BITMAP_TAG != 0 {
            return BitmapView::open_admitted(bytes).map(Self::Bitmap);
        }
        match control {
            DIR32_CONTROL => open_v2_dir::<false>(bytes, false).ok().map(Self::Directory),
            EF2_CONTROL => Ef2View::open_admitted(bytes).map(Self::EliasFano),
            0x20..=0x23 => SingleView::open_admitted(bytes).map(Self::Single),
            0x00 | DIR64_CONTROL | 0x40..=0x43 | 0x60..=0x63 => None,
            _ => ListView::open_admitted(bytes).map(Self::List),
        }
    }

    fn open_verified_inner(bytes: &'a [u8], require_sealed: bool) -> Result<Self, AdmissionError> {
        let Some(&control) = bytes.first() else {
            return Err(AdmissionError::new(
                AdmissionClass::Extent,
                0,
                "an image has no control byte",
            ));
        };
        let view = if control & BITMAP_TAG != 0 {
            Self::Bitmap(BitmapView::open(bytes)?)
        } else {
            match control {
                0x00 => {
                    return Err(AdmissionError::new(
                        AdmissionClass::Kind,
                        0,
                        "the control 00 is no image",
                    ));
                }
                DIR32_CONTROL => {
                    let dir = open_v2_dir::<true>(bytes, true)?;
                    dir.verify_forest_canonical()?;
                    Self::Directory(dir)
                }
                EF2_CONTROL => Self::EliasFano(Ef2View::open_verified(bytes)?),
                DIR64_CONTROL => {
                    return Err(AdmissionError::new(
                        AdmissionClass::Kind,
                        0,
                        "a u64 directory was handed to a u32 reader",
                    ));
                }
                0x20..=0x23 => Self::Single(SingleView::open(bytes)?),
                0x40..=0x43 => {
                    return Err(AdmissionError::new(
                        AdmissionClass::Kind,
                        0,
                        "the control is not format 2",
                    ));
                }
                0x60..=0x63 => {
                    return Err(AdmissionError::new(
                        AdmissionClass::Kind,
                        0,
                        "the control is unassigned",
                    ));
                }
                _ => Self::List(ListView::open(bytes)?),
            }
        };
        view.verify_form_canonical(require_sealed)?;
        Ok(view)
    }

    /// Check §7.1 (sealed) / §7.2 (forest) canonicality with zero heap allocation.
    fn verify_form_canonical(&self, require_sealed: bool) -> Result<(), AdmissionError> {
        match *self {
            Self::Bitmap(b) => {
                if b.extent() == 1 {
                    return Ok(());
                }
                let b_len = b.extent();
                let n = b.len() as usize;
                let first = b.first_member().unwrap_or(0);
                let last = b.last_member().unwrap_or(0);
                let span = last - first;
                if n <= LIST_MAX_MEMBERS && list_len_from(n, first, span) < b_len {
                    return Err(AdmissionError::new(
                        AdmissionClass::Canonical,
                        0,
                        "Bitmap is longer than Single/List for this set",
                    ));
                }
                if b_len > 11 && dir_total_from_bitmap(&b, n, first, last) < b_len {
                    return Err(AdmissionError::new(
                        AdmissionClass::Canonical,
                        0,
                        "Bitmap is longer than Directory for this set",
                    ));
                }
                if require_sealed
                    && n >= 2
                    && ef2_len(n as u64, first, span).is_some_and(|e| e < b_len)
                {
                    return Err(AdmissionError::new(
                        AdmissionClass::Canonical,
                        0,
                        "Bitmap is not the sealed image; Elias-Fano is shorter",
                    ));
                }
                Ok(())
            }
            Self::Single(s) => {
                if s.value() <= 7 {
                    return Err(AdmissionError::new(
                        AdmissionClass::Canonical,
                        0,
                        "Single member <= 7 must be encoded as Bitmap",
                    ));
                }
                Ok(())
            }
            Self::List(l) => {
                let l_len = l.extent();
                let n = l.len() as usize;
                let first = l.first();
                let last = l.last();
                let span = l.span();
                if last <= BITMAP_LAST_MEMBER && bitmap_len(last) <= l_len {
                    return Err(AdmissionError::new(
                        AdmissionClass::Canonical,
                        0,
                        "List is no shorter than Bitmap for this set",
                    ));
                }
                if l_len > 11 {
                    let mut buf = [0u32; LIST_MAX_MEMBERS];
                    let members = l.copy_members(&mut buf);
                    if dir_total_from_slice(members) < l_len {
                        return Err(AdmissionError::new(
                            AdmissionClass::Canonical,
                            0,
                            "List is longer than Directory for this set",
                        ));
                    }
                }
                if require_sealed && ef2_len(n as u64, first, span).is_some_and(|e| e < l_len) {
                    return Err(AdmissionError::new(
                        AdmissionClass::Canonical,
                        0,
                        "List is not the sealed image; Elias-Fano is shorter",
                    ));
                }
                Ok(())
            }
            Self::Directory(d) => {
                let d_len = d.serialized_len();
                let n = d.len();
                let first = d.select(0).unwrap_or(0);
                let last = d.select(n - 1).unwrap_or(0);
                let span = last - first;
                if last <= BITMAP_LAST_MEMBER && bitmap_len(last) <= d_len {
                    return Err(AdmissionError::new(
                        AdmissionClass::Canonical,
                        0,
                        "Directory is no shorter than Bitmap for this set",
                    ));
                }
                if n <= LIST_MAX_MEMBERS as u64 && list_len_from(n as usize, first, span) <= d_len {
                    return Err(AdmissionError::new(
                        AdmissionClass::Canonical,
                        0,
                        "Directory is no shorter than Single/List for this set",
                    ));
                }
                if require_sealed && n >= 2 && ef2_len(n, first, span).is_some_and(|e| e < d_len) {
                    return Err(AdmissionError::new(
                        AdmissionClass::Canonical,
                        0,
                        "Directory is not the sealed image; Elias-Fano is shorter",
                    ));
                }
                Ok(())
            }
            Self::EliasFano(e) => {
                // Elias-Fano is NEVER a forest image, so even open_verified requires it to beat
                // Bitmap, List, and Directory strictly (§7.1, §7.2, §11).
                let e_len = e.extent();
                let n = e.len();
                let first = e.first();
                let span = e.span();
                let last = first + span;
                if last <= BITMAP_LAST_MEMBER && bitmap_len(last) <= e_len {
                    return Err(AdmissionError::new(
                        AdmissionClass::Canonical,
                        0,
                        "Elias-Fano is no shorter than Bitmap for this set",
                    ));
                }
                if n <= LIST_MAX_MEMBERS as u64 && list_len_from(n as usize, first, span) <= e_len {
                    return Err(AdmissionError::new(
                        AdmissionClass::Canonical,
                        0,
                        "Elias-Fano is no shorter than List for this set",
                    ));
                }
                if e_len >= 11 && dir_total_from_ef2(&e) <= e_len {
                    return Err(AdmissionError::new(
                        AdmissionClass::Canonical,
                        0,
                        "Elias-Fano is no shorter than Directory for this set",
                    ));
                }
                Ok(())
            }
        }
    }

    #[inline(always)]
    pub fn form(&self) -> ImageForm {
        match self {
            Self::Bitmap(_) => ImageForm::Bitmap,
            Self::Single(_) => ImageForm::Single,
            Self::List(_) => ImageForm::List,
            Self::EliasFano(_) => ImageForm::EliasFano,
            Self::Directory(_) => ImageForm::Directory,
        }
    }

    #[inline(always)]
    pub fn serialized_len(&self) -> usize {
        match self {
            Self::Bitmap(v) => v.extent(),
            Self::Single(v) => v.extent(),
            Self::List(v) => v.extent(),
            Self::EliasFano(v) => v.extent(),
            Self::Directory(v) => v.serialized_len(),
        }
    }

    #[inline(always)]
    pub fn len(&self) -> u64 {
        match self {
            Self::Bitmap(v) => u64::from(v.len()),
            Self::Single(_) => 1,
            Self::List(v) => u64::from(v.len()),
            Self::EliasFano(v) => v.len(),
            Self::Directory(v) => v.len(),
        }
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Bitmap(v) => v.is_empty(),
            Self::Single(_) | Self::List(_) => false,
            Self::EliasFano(v) => v.is_empty(),
            Self::Directory(v) => v.is_empty(),
        }
    }

    #[inline]
    pub fn contains(&self, v: u32) -> bool {
        match self {
            Self::Bitmap(b) => b.contains(v),
            Self::Single(s) => s.contains(v),
            Self::List(l) => l.contains(v),
            Self::EliasFano(e) => e.contains(v),
            Self::Directory(d) => d.contains(v),
        }
    }

    #[inline]
    pub fn rank(&self, v: u32) -> u64 {
        match self {
            Self::Bitmap(b) => u64::from(b.rank(v)),
            Self::Single(s) => u64::from(s.rank(v)),
            Self::List(l) => u64::from(l.rank(v)),
            Self::EliasFano(e) => e.rank(v),
            Self::Directory(d) => d.rank(v),
        }
    }

    #[inline]
    pub fn rank_if_present(&self, v: u32) -> Option<u64> {
        match self {
            Self::Bitmap(b) => b.rank_if_present(v).map(u64::from),
            Self::Single(s) => s.rank_if_present(v).map(u64::from),
            Self::List(l) => l.rank_if_present(v).map(u64::from),
            Self::EliasFano(e) => e.rank_if_present(v),
            Self::Directory(d) => d.rank_if_present(v),
        }
    }

    #[inline]
    pub fn select(&self, k: u64) -> Option<u32> {
        match self {
            Self::Bitmap(b) => u32::try_from(k).ok().and_then(|k| b.select(k)),
            Self::Single(s) => u32::try_from(k).ok().and_then(|k| s.select(k)),
            Self::List(l) => u32::try_from(k).ok().and_then(|k| l.select(k)),
            Self::EliasFano(e) => e.select(k),
            Self::Directory(d) => d.select(k),
        }
    }

    #[inline]
    pub fn seek(&self, v: u32) -> Option<u32> {
        match self {
            Self::Bitmap(b) => b.seek(v),
            Self::Single(s) => s.seek(v),
            Self::List(l) => l.seek(v),
            Self::EliasFano(e) => e.seek(v),
            Self::Directory(d) => {
                let mut r = d.range();
                r.seek(v);
                (!r.empty()).then(|| r.front())
            }
        }
    }

    pub fn for_each(&self, f: impl FnMut(u32)) {
        match self {
            Self::Bitmap(b) => b.for_each(f),
            Self::Single(s) => s.for_each(f),
            Self::List(l) => l.for_each(f),
            Self::EliasFano(e) => e.for_each(f),
            Self::Directory(d) => d.for_each(f),
        }
    }

    #[inline]
    pub fn range(&self) -> V2Range<'a> {
        match *self {
            Self::Bitmap(b) => V2Range::Bitmap(b.range()),
            Self::Single(s) => V2Range::Single(s.range()),
            Self::List(l) => V2Range::List(l.range()),
            Self::EliasFano(e) => V2Range::EliasFano(e.range()),
            Self::Directory(d) => V2Range::Directory(d.range()),
        }
    }

    /// Cardinality of the intersection, allocation-free (§12.1).
    pub fn and_len(&self, other: &V2View<'_>) -> u64 {
        fn and_list(l: &ListView<'_>, o: &V2View<'_>) -> u64 {
            let mut count = 0u64;
            l.for_each(|v| count += u64::from(o.contains(v)));
            count
        }
        fn and_bitmap_other(b: &BitmapView<'_>, o: &V2View<'_>) -> u64 {
            let mut count = 0u64;
            if u64::from(b.len()) <= o.len() {
                b.for_each(|v| count += u64::from(o.contains(v)));
            } else {
                let mut r = o.range();
                while !r.empty() {
                    let v = r.front();
                    if v > BITMAP_LAST_MEMBER {
                        break;
                    }
                    count += u64::from(b.contains(v));
                    r.pop_front();
                }
            }
            count
        }
        fn and_ef_dir(ef: &Ef2View<'_>, d: &BitmosaicView<'_>) -> u64 {
            let mut count = 0u64;
            if ef.len() <= d.len() {
                ef.for_each(|v| count += u64::from(d.contains(v)));
            } else {
                let mut cursor = ef.probe_cursor();
                let mut r = d.range();
                r.seek(ef.first());
                let max = ef.first().saturating_add(ef.span());
                while !r.empty() {
                    let v = r.front();
                    if v > max {
                        break;
                    }
                    count += u64::from(cursor.contains_ascending(v));
                    r.pop_front();
                }
            }
            count
        }
        match (self, other) {
            (V2View::Bitmap(a), V2View::Bitmap(b)) => u64::from(a.and_len(b)),
            (V2View::Single(s), o) => u64::from(o.contains(s.value())),
            (o, V2View::Single(s)) => u64::from(o.contains(s.value())),
            (V2View::List(l), o) => and_list(l, o),
            (o, V2View::List(l)) => and_list(l, o),
            (V2View::Directory(a), V2View::Directory(b)) => a.and_len(b),
            (V2View::EliasFano(a), V2View::EliasFano(b)) => a.and_len(b),
            (V2View::Bitmap(b), o) => and_bitmap_other(b, o),
            (o, V2View::Bitmap(b)) => and_bitmap_other(b, o),
            (V2View::EliasFano(ef), V2View::Directory(d)) => and_ef_dir(ef, d),
            (V2View::Directory(d), V2View::EliasFano(ef)) => and_ef_dir(ef, d),
        }
    }

    /// Members of the intersection strictly below `limit`, allocation-free (§12.1).
    pub fn and_rank(&self, other: &V2View<'_>, limit: u32) -> u64 {
        if limit == 0 {
            return 0;
        }
        fn and_rank_list(l: &ListView<'_>, o: &V2View<'_>, limit: u32) -> u64 {
            let mut count = 0u64;
            l.for_each(|v| count += u64::from(v < limit && o.contains(v)));
            count
        }
        match (self, other) {
            (V2View::Bitmap(a), V2View::Bitmap(b)) => u64::from(a.and_rank(b, limit)),
            (V2View::Single(s), o) => u64::from(s.value() < limit && o.contains(s.value())),
            (o, V2View::Single(s)) => u64::from(s.value() < limit && o.contains(s.value())),
            (V2View::List(l), o) => and_rank_list(l, o, limit),
            (o, V2View::List(l)) => and_rank_list(l, o, limit),
            (V2View::EliasFano(a), V2View::EliasFano(b)) => a.and_rank(b, limit),
            _ => {
                let (mut a, mut b) = (self.range(), other.range());
                let mut count = 0u64;
                while !a.empty() && !b.empty() {
                    let x = a.front();
                    let y = b.front();
                    if x >= limit || y >= limit {
                        break;
                    }
                    if x == y {
                        count += 1;
                        a.pop_front();
                        b.pop_front();
                    } else if x < y {
                        a.seek(y);
                    } else {
                        b.seek(x);
                    }
                }
                count
            }
        }
    }
}

/// Ascending [`Range`] cursor over a [`V2View`].
#[derive(Clone, Copy, Debug)]
pub enum V2Range<'a> {
    Bitmap(BitmapRange<'a>),
    Single(SingleRange),
    List(ListRange<'a>),
    EliasFano(Ef2Range<'a>),
    Directory(BitmosaicViewRange<'a>),
}

impl Range for V2Range<'_> {
    type Item = u32;

    #[inline(always)]
    fn empty(&self) -> bool {
        match self {
            Self::Bitmap(r) => r.empty(),
            Self::Single(r) => r.empty(),
            Self::List(r) => r.empty(),
            Self::EliasFano(r) => r.empty(),
            Self::Directory(r) => r.empty(),
        }
    }

    #[inline(always)]
    fn front(&self) -> u32 {
        match self {
            Self::Bitmap(r) => r.front(),
            Self::Single(r) => r.front(),
            Self::List(r) => r.front(),
            Self::EliasFano(r) => r.front(),
            Self::Directory(r) => r.front(),
        }
    }

    #[inline(always)]
    fn pop_front(&mut self) {
        match self {
            Self::Bitmap(r) => r.pop_front(),
            Self::Single(r) => r.pop_front(),
            Self::List(r) => r.pop_front(),
            Self::EliasFano(r) => r.pop_front(),
            Self::Directory(r) => r.pop_front(),
        }
    }

    #[inline]
    fn seek(&mut self, target: u32) {
        match self {
            Self::Bitmap(r) => r.seek(target),
            Self::Single(r) => r.seek(target),
            Self::List(r) => r.seek(target),
            Self::EliasFano(r) => r.seek(target),
            Self::Directory(r) => r.seek(target),
        }
    }
}

impl Iterator for V2Range<'_> {
    type Item = u32;

    #[inline(always)]
    fn next(&mut self) -> Option<u32> {
        if self.empty() {
            return None;
        }
        let v = self.front();
        self.pop_front();
        Some(v)
    }
}

// ── Format 2 u64 directory (§9) & unified V264View (§7.4) ────────────────

/// Borrowed Format 2 `u64` directory (`0x03`, §9): high-32 keys, interior exclusive prefixes,
/// child offsets, and adjacent sealed Format 2 `u32` child images.
#[derive(Clone, Copy, Debug)]
pub struct Dir64V2View<'a> {
    bytes: &'a [u8],
    children: u32,
    len: u64,
    geometry: Geometry64V2,
    last_before: u64,
}

impl<'a> Dir64V2View<'a> {
    /// Structural admission (§9, §11 `open`).
    pub fn open(bytes: &'a [u8]) -> Result<Self, AdmissionError> {
        Self::open_with(bytes, false)
    }

    /// Full admission (§9, §11 `open_verified` / `open_canonical`): refuses single child at key 0
    /// as `Canonical` and verifies every child is its sealed `u32` image.
    pub fn open_verified(bytes: &'a [u8]) -> Result<Self, AdmissionError> {
        Self::open_with(bytes, true)
    }

    /// O(1) attach for an already-admitted Format 2 `u64` directory: touches at most the last two
    /// children to compute `last_before`, `len`, and `extent`.
    #[inline]
    pub fn open_admitted(bytes: &'a [u8]) -> Option<Self> {
        if *bytes.first()? != DIR64_CONTROL {
            return None;
        }
        let mut at = 1usize;
        let children = read_uleb_checked(bytes, &mut at, u32::MAX, AdmissionClass::Count)
            .ok()?
            .checked_add(1)?;
        let m = children as usize;
        let geometry = Geometry64V2::new(m, at)?;
        if bytes.len() < geometry.children_at {
            return None;
        }
        let last_rel = if m == 1 {
            0
        } else {
            read_u32_at(bytes, geometry.offsets_at + 4 * (m - 2)) as usize
        };
        let last_start = geometry.children_at.checked_add(last_rel)?;
        let last_child = V2View::open_admitted(bytes.get(last_start..)?)?;
        let extent = last_start.checked_add(last_child.serialized_len())?;
        let slice = bytes.get(..extent)?;
        let (last_before, len) = match m {
            1 => (0, last_child.len()),
            2 => {
                let c0 = V2View::open_admitted(slice.get(geometry.children_at..last_start)?)?;
                let lb = c0.len();
                (lb, lb.checked_add(last_child.len())?)
            }
            _ => {
                let prev_before = read_u64_at(slice, geometry.before_at + 8 * (m - 3));
                let prev_rel = read_u32_at(slice, geometry.offsets_at + 4 * (m - 3)) as usize;
                let prev_start = geometry.children_at.checked_add(prev_rel)?;
                let c_prev = V2View::open_admitted(slice.get(prev_start..last_start)?)?;
                let lb = prev_before.checked_add(c_prev.len())?;
                (lb, lb.checked_add(last_child.len())?)
            }
        };
        Some(Self {
            bytes: slice,
            children,
            len,
            geometry,
            last_before,
        })
    }

    fn open_with(bytes: &'a [u8], verify: bool) -> Result<Self, AdmissionError> {
        let Some(&control) = bytes.first() else {
            return Err(AdmissionError::new(
                AdmissionClass::Extent,
                0,
                "a u64 directory has no control byte",
            ));
        };
        if control != DIR64_CONTROL {
            return Err(AdmissionError::new(
                AdmissionClass::Kind,
                0,
                "the control is not a u64 directory's",
            ));
        }
        let mut at = 1usize;
        let count_at = at;
        let children = read_uleb_checked(bytes, &mut at, u32::MAX, AdmissionClass::Count)?
            .checked_add(1)
            .ok_or_else(|| {
                AdmissionError::new(
                    AdmissionClass::Count,
                    count_at,
                    "u64 directory count overflows u32",
                )
            })?;
        let m = children as usize;
        let geometry = Geometry64V2::new(m, at).ok_or_else(|| {
            AdmissionError::new(
                AdmissionClass::Extent,
                at,
                "u64 directory geometry overflows the address space",
            )
        })?;
        if bytes.len() < geometry.children_at {
            return Err(AdmissionError::new(
                AdmissionClass::Extent,
                at,
                "u64 directory planes are truncated",
            ));
        }
        let mut len = 0u64;
        let mut last_before = 0u64;
        let mut total_extent = geometry.children_at;
        for i in 0..m {
            let key = read_u32_at(bytes, geometry.keys_at + 4 * i);
            if i != 0 && key <= read_u32_at(bytes, geometry.keys_at + 4 * (i - 1)) {
                return Err(AdmissionError::new(
                    AdmissionClass::MapOrder,
                    geometry.keys_at + 4 * i,
                    "u64 high keys are not strictly ascending",
                ));
            }
            if i != 0 && i + 1 != m {
                let stored = read_u64_at(bytes, geometry.before_at + 8 * (i - 1));
                if stored != len {
                    return Err(AdmissionError::new(
                        AdmissionClass::Count,
                        geometry.before_at + 8 * (i - 1),
                        "u64 exclusive prefix differs from preceding children",
                    ));
                }
            }
            let offset_at = if i == 0 {
                geometry.offsets_at
            } else {
                geometry.offsets_at + 4 * (i - 1)
            };
            let rel = if i == 0 {
                0
            } else {
                read_u32_at(bytes, offset_at) as usize
            };
            let start = geometry.children_at.checked_add(rel).ok_or_else(|| {
                AdmissionError::new(
                    AdmissionClass::Extent,
                    offset_at,
                    "u64 child offset overflows",
                )
            })?;
            let child_slice = if i + 1 < m {
                let next_at = geometry.offsets_at + 4 * i;
                let next_rel = read_u32_at(bytes, next_at) as usize;
                let end = geometry.children_at.checked_add(next_rel).ok_or_else(|| {
                    AdmissionError::new(AdmissionClass::Extent, next_at, "u64 child end overflows")
                })?;
                bytes.get(start..end).ok_or_else(|| {
                    AdmissionError::new(
                        AdmissionClass::Placement,
                        offset_at,
                        "u64 child offsets are not adjacent within the image",
                    )
                })?
            } else {
                bytes.get(start..).ok_or_else(|| {
                    AdmissionError::new(
                        AdmissionClass::Placement,
                        offset_at,
                        "u64 last child starts past the image",
                    )
                })?
            };
            let child = if verify {
                V2View::open_canonical(child_slice).map_err(|e| e.shifted(start))?
            } else {
                V2View::open(child_slice).map_err(|e| e.shifted(start))?
            };
            if child.is_empty() {
                return Err(AdmissionError::new(
                    AdmissionClass::Kind,
                    start,
                    "u64 child image must not be empty",
                ));
            }
            if i + 1 < m {
                if child.serialized_len() != child_slice.len() {
                    return Err(AdmissionError::new(
                        AdmissionClass::Placement,
                        offset_at,
                        "u64 child does not cover its offset span exactly",
                    ));
                }
            } else {
                total_extent = start + child.serialized_len();
            }
            last_before = len;
            len = len.checked_add(child.len()).ok_or_else(|| {
                AdmissionError::new(
                    AdmissionClass::Count,
                    count_at,
                    "u64 total cardinality overflows",
                )
            })?;
        }
        if verify && m == 1 && read_u32_at(bytes, geometry.keys_at) == 0 {
            return Err(AdmissionError::new(
                AdmissionClass::Canonical,
                geometry.keys_at,
                "a u64 directory of one child at key zero must be encoded as the u32 image",
            ));
        }
        Ok(Self {
            bytes: &bytes[..total_extent],
            children,
            len,
            geometry,
            last_before,
        })
    }

    #[inline(always)]
    pub fn serialized_len(&self) -> usize {
        self.bytes.len()
    }

    #[inline(always)]
    pub fn len(&self) -> u64 {
        self.len
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline(always)]
    fn high(&self, i: usize) -> u32 {
        read_u32_at(self.bytes, self.geometry.keys_at + 4 * i)
    }

    #[inline(always)]
    fn before(&self, i: usize) -> u64 {
        if i == 0 {
            0
        } else if i + 1 == self.children as usize {
            self.last_before
        } else {
            read_u64_at(self.bytes, self.geometry.before_at + 8 * (i - 1))
        }
    }

    #[inline]
    fn child(&self, i: usize) -> Option<V2View<'a>> {
        let start = self.geometry.children_at
            + if i == 0 {
                0
            } else {
                read_u32_at(self.bytes, self.geometry.offsets_at + 4 * (i - 1)) as usize
            };
        let end = if i + 1 == self.children as usize {
            self.bytes.len()
        } else {
            self.geometry.children_at
                + read_u32_at(self.bytes, self.geometry.offsets_at + 4 * i) as usize
        };
        V2View::open_admitted(self.bytes.get(start..end)?)
    }

    #[inline]
    fn walk(&self, high: u32) -> usize {
        let m = self.children as usize;
        if m <= 16 {
            let mut at = 0usize;
            for i in 0..m {
                at += usize::from(self.high(i) < high);
            }
            return at;
        }
        let (mut lo, mut hi) = (0usize, m);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.high(mid) < high {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    #[inline]
    fn locate(&self, high: u32) -> Option<usize> {
        let m = self.children as usize;
        if m == 0 {
            return None;
        }
        let first = self.high(0);
        let guess = (high.wrapping_sub(first) as usize).min(m - 1);
        if self.high(guess) == high {
            return Some(guess);
        }
        if high < first {
            return None;
        }
        let at = self.walk(high);
        (at < m && self.high(at) == high).then_some(at)
    }

    #[inline]
    pub fn contains(&self, value: u64) -> bool {
        self.locate((value >> 32) as u32)
            .and_then(|i| self.child(i))
            .is_some_and(|c| c.contains(value as u32))
    }

    #[inline]
    pub fn rank(&self, value: u64) -> u64 {
        let m = self.children as usize;
        if m == 0 {
            return 0;
        }
        let high = (value >> 32) as u32;
        let at = self.walk(high);
        if at < m && self.high(at) == high {
            self.before(at) + self.child(at).map_or(0, |c| c.rank(value as u32))
        } else if at == 0 {
            0
        } else if at == m {
            self.len
        } else {
            self.before(at)
        }
    }

    #[inline]
    pub fn rank_if_present(&self, value: u64) -> Option<u64> {
        let i = self.locate((value >> 32) as u32)?;
        let low = self.child(i)?.rank_if_present(value as u32)?;
        Some(self.before(i) + low)
    }

    #[inline]
    pub fn select(&self, k: u64) -> Option<u64> {
        if k >= self.len {
            return None;
        }
        let m = self.children as usize;
        let (mut lo, mut hi) = (0usize, m);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.before(mid) <= k {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        let at = lo - 1;
        let low = self.child(at)?.select(k - self.before(at))?;
        Some((u64::from(self.high(at)) << 32) | u64::from(low))
    }

    #[inline]
    pub fn seek(&self, value: u64) -> Option<u64> {
        let m = self.children as usize;
        let high = (value >> 32) as u32;
        let mut at = self.walk(high);
        if at < m && self.high(at) == high {
            if let Some(low) = self.child(at).and_then(|c| c.seek(value as u32)) {
                return Some((u64::from(high) << 32) | u64::from(low));
            }
            at += 1;
        }
        if at >= m {
            return None;
        }
        let low = self.child(at)?.select(0)?;
        Some((u64::from(self.high(at)) << 32) | u64::from(low))
    }

    pub fn for_each(&self, mut f: impl FnMut(u64)) {
        for i in 0..self.children as usize {
            let high = u64::from(self.high(i)) << 32;
            if let Some(c) = self.child(i) {
                c.for_each(|v| f(high | u64::from(v)));
            }
        }
    }

    #[inline]
    pub fn range(&self) -> Dir64V2Range<'a> {
        Dir64V2Range::new(*self)
    }

    pub fn and_len(&self, other: &Dir64V2View<'_>) -> u64 {
        let (na, nb) = (self.children as usize, other.children as usize);
        let (mut i, mut j, mut count) = (0, 0, 0u64);
        while i < na && j < nb {
            let (ha, hb) = (self.high(i), other.high(j));
            if ha == hb {
                if let (Some(a), Some(b)) = (self.child(i), other.child(j)) {
                    count += a.and_len(&b);
                }
                i += 1;
                j += 1;
            } else if ha < hb {
                i += 1;
            } else {
                j += 1;
            }
        }
        count
    }

    pub fn and_rank(&self, other: &Dir64V2View<'_>, limit: u64) -> u64 {
        let limit_hi = (limit >> 32) as u32;
        let limit_lo = limit as u32;
        let (na, nb) = (self.children as usize, other.children as usize);
        let (mut i, mut j, mut count) = (0, 0, 0u64);
        while i < na && j < nb {
            let (ha, hb) = (self.high(i), other.high(j));
            if ha > limit_hi || hb > limit_hi {
                break;
            }
            if ha == hb {
                if let (Some(a), Some(b)) = (self.child(i), other.child(j)) {
                    count += if ha < limit_hi {
                        a.and_len(&b)
                    } else {
                        a.and_rank(&b, limit_lo)
                    };
                }
                i += 1;
                j += 1;
            } else if ha < hb {
                i += 1;
            } else {
                j += 1;
            }
        }
        count
    }
}

/// Ascending [`Range`] cursor over a [`Dir64V2View`].
#[derive(Clone, Copy, Debug)]
pub struct Dir64V2Range<'a> {
    view: Dir64V2View<'a>,
    child_idx: u32,
    high: u64,
    inner: Option<V2Range<'a>>,
}

impl<'a> Dir64V2Range<'a> {
    fn new(view: Dir64V2View<'a>) -> Self {
        let mut r = Self {
            view,
            child_idx: 0,
            high: 0,
            inner: None,
        };
        r.open_next();
        r
    }

    fn open_next(&mut self) {
        while self.inner.as_ref().is_none_or(Range::empty) {
            let at = self.child_idx as usize;
            if at >= self.view.children as usize {
                self.inner = None;
                return;
            }
            self.high = u64::from(self.view.high(at)) << 32;
            self.inner = self.view.child(at).map(|c| c.range());
            self.child_idx += 1;
        }
    }
}

impl Range for Dir64V2Range<'_> {
    type Item = u64;

    #[inline(always)]
    fn empty(&self) -> bool {
        self.inner.is_none()
    }

    #[inline(always)]
    fn front(&self) -> u64 {
        let inner = self.inner.as_ref().expect("front on empty Dir64V2Range");
        self.high | u64::from(inner.front())
    }

    #[inline(always)]
    fn pop_front(&mut self) {
        if let Some(inner) = &mut self.inner {
            inner.pop_front();
            if inner.empty() {
                self.open_next();
            }
        }
    }

    #[inline]
    fn seek(&mut self, target: u64) {
        if self.empty() || self.front() >= target {
            return;
        }
        let target_hi = (target >> 32) as u32;
        if u64::from(target_hi) << 32 == self.high {
            if let Some(inner) = &mut self.inner {
                inner.seek(target as u32);
                if inner.empty() {
                    self.open_next();
                }
            }
            return;
        }
        let m = self.view.children as usize;
        let mut at = self.view.walk(target_hi);
        if at < m && self.view.high(at) == target_hi {
            if let Some(c) = self.view.child(at) {
                let mut inner = c.range();
                inner.seek(target as u32);
                if !inner.empty() {
                    self.high = u64::from(target_hi) << 32;
                    self.inner = Some(inner);
                    self.child_idx = at as u32 + 1;
                    return;
                }
            }
            at += 1;
        }
        self.inner = None;
        self.child_idx = at as u32;
        self.open_next();
    }
}

impl Iterator for Dir64V2Range<'_> {
    type Item = u64;

    #[inline(always)]
    fn next(&mut self) -> Option<u64> {
        if self.empty() {
            return None;
        }
        let v = self.front();
        self.pop_front();
        Some(v)
    }
}

/// Unified Format 2 `u64` view (§7.4): admits every Format 2 `u32` image (members `< 2^32`,
/// including `0x80` empty) and Format 2 `u64` directories (`0x03`).
#[derive(Clone, Copy, Debug)]
pub enum V264View<'a> {
    U32(V2View<'a>),
    Dir64(Dir64V2View<'a>),
}

impl<'a> V264View<'a> {
    pub fn open(bytes: &'a [u8]) -> Result<Self, AdmissionError> {
        if bytes.first() == Some(&DIR64_CONTROL) {
            Dir64V2View::open(bytes).map(Self::Dir64)
        } else {
            V2View::open(bytes).map(Self::U32)
        }
    }

    pub fn open_verified(bytes: &'a [u8]) -> Result<Self, AdmissionError> {
        if bytes.first() == Some(&DIR64_CONTROL) {
            Dir64V2View::open_verified(bytes).map(Self::Dir64)
        } else {
            V2View::open_verified(bytes).map(Self::U32)
        }
    }

    pub fn open_canonical(bytes: &'a [u8]) -> Result<Self, AdmissionError> {
        if bytes.first() == Some(&DIR64_CONTROL) {
            Dir64V2View::open_verified(bytes).map(Self::Dir64)
        } else {
            V2View::open_canonical(bytes).map(Self::U32)
        }
    }

    #[inline]
    pub fn open_admitted(bytes: &'a [u8]) -> Option<Self> {
        if bytes.first() == Some(&DIR64_CONTROL) {
            Dir64V2View::open_admitted(bytes).map(Self::Dir64)
        } else {
            V2View::open_admitted(bytes).map(Self::U32)
        }
    }

    #[inline(always)]
    pub fn serialized_len(&self) -> usize {
        match self {
            Self::U32(v) => v.serialized_len(),
            Self::Dir64(v) => v.serialized_len(),
        }
    }

    #[inline(always)]
    pub fn len(&self) -> u64 {
        match self {
            Self::U32(v) => v.len(),
            Self::Dir64(v) => v.len(),
        }
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn contains(&self, v: u64) -> bool {
        match self {
            Self::U32(u) => u32::try_from(v).is_ok_and(|x| u.contains(x)),
            Self::Dir64(d) => d.contains(v),
        }
    }

    #[inline]
    pub fn rank(&self, v: u64) -> u64 {
        match self {
            Self::U32(u) => match u32::try_from(v) {
                Ok(x) => u.rank(x),
                Err(_) => u.len(),
            },
            Self::Dir64(d) => d.rank(v),
        }
    }

    #[inline]
    pub fn rank_if_present(&self, v: u64) -> Option<u64> {
        match self {
            Self::U32(u) => u32::try_from(v).ok().and_then(|x| u.rank_if_present(x)),
            Self::Dir64(d) => d.rank_if_present(v),
        }
    }

    #[inline]
    pub fn select(&self, k: u64) -> Option<u64> {
        match self {
            Self::U32(u) => u.select(k).map(u64::from),
            Self::Dir64(d) => d.select(k),
        }
    }

    #[inline]
    pub fn seek(&self, v: u64) -> Option<u64> {
        match self {
            Self::U32(u) => u32::try_from(v).ok().and_then(|x| u.seek(x)).map(u64::from),
            Self::Dir64(d) => d.seek(v),
        }
    }

    pub fn for_each(&self, mut f: impl FnMut(u64)) {
        match self {
            Self::U32(u) => u.for_each(|v| f(u64::from(v))),
            Self::Dir64(d) => d.for_each(f),
        }
    }

    #[inline]
    pub fn range(&self) -> V264Range<'a> {
        match *self {
            Self::U32(u) => V264Range::U32(u.range()),
            Self::Dir64(d) => V264Range::Dir64(d.range()),
        }
    }

    pub fn and_len(&self, other: &V264View<'_>) -> u64 {
        fn and_u32_dir(u: &V2View<'_>, d: &Dir64V2View<'_>) -> u64 {
            if d.high(0) == 0 {
                d.child(0).map_or(0, |c0| u.and_len(&c0))
            } else {
                0
            }
        }
        match (self, other) {
            (V264View::U32(a), V264View::U32(b)) => a.and_len(b),
            (V264View::Dir64(a), V264View::Dir64(b)) => a.and_len(b),
            (V264View::U32(u), V264View::Dir64(d)) => and_u32_dir(u, d),
            (V264View::Dir64(d), V264View::U32(u)) => and_u32_dir(u, d),
        }
    }

    pub fn and_rank(&self, other: &V264View<'_>, limit: u64) -> u64 {
        fn and_rank_u32_dir(u: &V2View<'_>, d: &Dir64V2View<'_>, limit: u64) -> u64 {
            if d.high(0) == 0 {
                d.child(0).map_or(0, |c0| match u32::try_from(limit) {
                    Ok(x) => u.and_rank(&c0, x),
                    Err(_) => u.and_len(&c0),
                })
            } else {
                0
            }
        }
        match (self, other) {
            (V264View::U32(a), V264View::U32(b)) => match u32::try_from(limit) {
                Ok(x) => a.and_rank(b, x),
                Err(_) => a.and_len(b),
            },
            (V264View::Dir64(a), V264View::Dir64(b)) => a.and_rank(b, limit),
            (V264View::U32(u), V264View::Dir64(d)) => and_rank_u32_dir(u, d, limit),
            (V264View::Dir64(d), V264View::U32(u)) => and_rank_u32_dir(u, d, limit),
        }
    }
}

/// Ascending [`Range`] cursor over a [`V264View`].
#[derive(Clone, Copy, Debug)]
pub enum V264Range<'a> {
    U32(V2Range<'a>),
    Dir64(Dir64V2Range<'a>),
}

impl Range for V264Range<'_> {
    type Item = u64;

    #[inline(always)]
    fn empty(&self) -> bool {
        match self {
            Self::U32(r) => r.empty(),
            Self::Dir64(r) => r.empty(),
        }
    }

    #[inline(always)]
    fn front(&self) -> u64 {
        match self {
            Self::U32(r) => u64::from(r.front()),
            Self::Dir64(r) => r.front(),
        }
    }

    #[inline(always)]
    fn pop_front(&mut self) {
        match self {
            Self::U32(r) => r.pop_front(),
            Self::Dir64(r) => r.pop_front(),
        }
    }

    #[inline]
    fn seek(&mut self, target: u64) {
        match self {
            Self::U32(r) => match u32::try_from(target) {
                Ok(x) => r.seek(x),
                Err(_) => {
                    while !r.empty() {
                        r.pop_front();
                    }
                }
            },
            Self::Dir64(r) => r.seek(target),
        }
    }
}

impl Iterator for V264Range<'_> {
    type Item = u64;

    #[inline(always)]
    fn next(&mut self) -> Option<u64> {
        if self.empty() {
            return None;
        }
        let v = self.front();
        self.pop_front();
        Some(v)
    }
}

// ── Closed-form §7 selection & zero-alloc Directory pricing ──────────────

#[derive(Clone, Copy, Debug)]
enum ChoiceV2 {
    Empty,
    Bitmap {
        total: usize,
    },
    Single {
        total: usize,
    },
    List {
        total: usize,
    },
    Directory {
        body_bytes: usize,
        total: usize,
    },
    EliasFano {
        layout: Ef2Layout,
        first: u32,
        span: u32,
        total: usize,
    },
}

impl ChoiceV2 {
    #[inline(always)]
    fn total(self) -> usize {
        match self {
            Self::Empty => 1,
            Self::Bitmap { total }
            | Self::Single { total }
            | Self::List { total }
            | Self::Directory { total, .. }
            | Self::EliasFano { total, .. } => total,
        }
    }
}

/// Exact §8.4 payload length of one chunk (members sharing `v >> 16`), allocation-free.
fn chunk_payload_len_from_slice(chunk: &[u32]) -> usize {
    let n = chunk.len();
    if n == 1 {
        return 2;
    }
    let first = (chunk[0] & 0xFFFF) as u16;
    let second = (chunk[1] & 0xFFFF) as u16;
    let step = u32::from(second - first);
    let mut is_stride = true;
    let mut runs = 1usize;
    let mut prev = first;
    for &v in &chunk[1..] {
        let low = (v & 0xFFFF) as u16;
        let d = u32::from(low - prev);
        if d != step {
            is_stride = false;
        }
        if d != 1 {
            runs += 1;
        }
        prev = low;
    }
    if is_stride {
        return 4;
    }
    if n <= ARRAY_MAX {
        if n <= CONE_MAX_LEN && 16 + n <= 2 * n && 16 + n <= 6 * runs {
            let scale = (u64::from(prev - first) << 32) / (n as u64 - 1);
            let mut fits = true;
            for (i, &v) in chunk.iter().enumerate() {
                let low = (v & 0xFFFF) as u16;
                let pred = i64::from(first) + ((i as u64 * scale) >> 32) as i64;
                if (i64::from(low) - pred).abs() > CONE_MAX_EPS {
                    fits = false;
                    break;
                }
            }
            if fits {
                return 11 + n;
            }
        }
        if 2 * n <= 6 * runs {
            2 * n
        } else {
            runs_payload_len(runs)
        }
    } else if 6 * runs < crate::WORDS_HEAP_BYTES {
        runs_payload_len(runs)
    } else {
        let lo = (usize::from(first) >> 6) / 8 * 8;
        let hi = ((usize::from(prev) >> 6) / 8 + 1) * 8;
        words_payload_len(hi - lo)
    }
}

/// Exact Format 2 directory body bytes for an ascending slice `members` (`!members.is_empty()`),
/// zero heap allocation.
fn dir_body_bytes_from_slice(members: &[u32]) -> usize {
    let mut chunks = 0u32;
    let first_key = (members[0] >> 16) as u16;
    let last_key = (members[members.len() - 1] >> 16) as u16;
    let mut payload_sum = 0usize;
    let mut i = 0usize;
    while i < members.len() {
        let key = members[i] >> 16;
        let start = i;
        i += 1;
        while i < members.len() && (members[i] >> 16) == key {
            i += 1;
        }
        payload_sum += chunk_payload_len_from_slice(&members[start..i]);
        chunks += 1;
    }
    let header = forest_header_len(members.len() as u64, chunks);
    let slot_bits = slot_bits_for_span(chunks, first_key, last_key);
    Geometry::new(chunks, slot_bits, header).payload_at + payload_sum
}

#[inline]
fn dir_total_from_body(body_bytes: usize) -> usize {
    1 + uleb_len(body_bytes as u32) + body_bytes
}

fn dir_total_from_slice(members: &[u32]) -> usize {
    dir_total_from_body(dir_body_bytes_from_slice(members))
}

/// Exact Format 2 directory total length for a nonempty Bitmap (`last <= 1015`, 1 chunk),
/// zero heap allocation.
fn dir_total_from_bitmap(bm: &BitmapView<'_>, n: usize, first: u32, last: u32) -> usize {
    let payload = if n == 1 {
        2
    } else {
        let mut it = bm.iter();
        let v0 = it.next().unwrap_or(0);
        let v1 = it.next().unwrap_or(0);
        let step = v1 - v0;
        let mut is_stride = true;
        let mut runs = 1usize + usize::from(step != 1);
        let mut prev = v1;
        for v in it {
            let d = v - prev;
            if d != step {
                is_stride = false;
            }
            if d != 1 {
                runs += 1;
            }
            prev = v;
        }
        if is_stride {
            4
        } else if n <= CONE_MAX_LEN && 16 + n <= 2 * n && 16 + n <= 6 * runs {
            let scale = (u64::from(last - first) << 32) / (n as u64 - 1);
            let mut fits = true;
            for (i, v) in bm.iter().enumerate() {
                let pred = i64::from(first) + ((i as u64 * scale) >> 32) as i64;
                if (i64::from(v) - pred).abs() > CONE_MAX_EPS {
                    fits = false;
                    break;
                }
            }
            if fits {
                11 + n
            } else if 2 * n <= 6 * runs {
                2 * n
            } else {
                runs_payload_len(runs)
            }
        } else if 2 * n <= 6 * runs {
            2 * n
        } else {
            runs_payload_len(runs)
        }
    };
    let header = forest_header_len(n as u64, 1);
    let body_bytes = Geometry::new(1, 0, header).payload_at + payload;
    dir_total_from_body(body_bytes)
}

/// Exact Format 2 directory total length for a Format 2 Elias-Fano view, zero heap allocation
/// (uses `Copy` cursor checkpoints via `Range::save` for the rare Cone-fit check).
fn dir_total_from_ef2(ef: &Ef2View<'_>) -> usize {
    let mut r = ef.range();
    let mut chunks = 0u32;
    let first_key = (ef.first() >> 16) as u16;
    let last_key = ((ef.first() + ef.span()) >> 16) as u16;
    let mut payload_sum = 0usize;
    while !r.empty() {
        let start_cursor = r.save();
        let v0 = r.front();
        let key = v0 >> 16;
        let first_low = (v0 & 0xFFFF) as u16;
        r.pop_front();
        let mut n = 1usize;
        let mut prev = first_low;
        let mut step = 0u32;
        let mut is_stride = true;
        let mut runs = 1usize;
        while !r.empty() && (r.front() >> 16) == key {
            let low = (r.front() & 0xFFFF) as u16;
            let d = u32::from(low - prev);
            if n == 1 {
                step = d;
            } else if d != step {
                is_stride = false;
            }
            if d != 1 {
                runs += 1;
            }
            prev = low;
            n += 1;
            r.pop_front();
        }
        let payload = if n == 1 {
            2
        } else if is_stride {
            4
        } else if n <= ARRAY_MAX {
            if n <= CONE_MAX_LEN && 16 + n <= 2 * n && 16 + n <= 6 * runs {
                let scale = (u64::from(prev - first_low) << 32) / (n as u64 - 1);
                let mut check = start_cursor;
                let mut fits = true;
                for i in 0..n {
                    let low = (check.front() & 0xFFFF) as u16;
                    check.pop_front();
                    let pred = i64::from(first_low) + ((i as u64 * scale) >> 32) as i64;
                    if (i64::from(low) - pred).abs() > CONE_MAX_EPS {
                        fits = false;
                        break;
                    }
                }
                if fits {
                    11 + n
                } else if 2 * n <= 6 * runs {
                    2 * n
                } else {
                    runs_payload_len(runs)
                }
            } else if 2 * n <= 6 * runs {
                2 * n
            } else {
                runs_payload_len(runs)
            }
        } else if 6 * runs < crate::WORDS_HEAP_BYTES {
            runs_payload_len(runs)
        } else {
            let lo = (usize::from(first_low) >> 6) / 8 * 8;
            let hi = ((usize::from(prev) >> 6) / 8 + 1) * 8;
            words_payload_len(hi - lo)
        };
        payload_sum += payload;
        chunks += 1;
    }
    let header = forest_header_len(ef.len(), chunks);
    let slot_bits = slot_bits_for_span(chunks, first_key, last_key);
    let body_bytes = Geometry::new(chunks, slot_bits, header).payload_at + payload_sum;
    dir_total_from_body(body_bytes)
}

/// §7.1 (sealed) or §7.2 (forest) closed-form choice over `(n, first, last)` and lazy directory
/// pricer. Applies §7.5's law-licensed shortcuts so small sets never price the directory or List
/// unnecessarily.
fn choose_v2(
    n: u64,
    first: u32,
    last: u32,
    sealed: bool,
    price_dir_body: impl FnOnce() -> usize,
) -> ChoiceV2 {
    if n == 0 {
        return ChoiceV2::Empty;
    }
    if n == 1 {
        return if first <= 7 {
            ChoiceV2::Bitmap { total: 2 }
        } else {
            ChoiceV2::Single {
                total: 1 + width_of(first),
            }
        };
    }
    let span = last - first;

    // Candidate 1: Bitmap (order 0)
    let mut best_len = usize::MAX;
    let mut best_order = 99u8;
    let mut best = ChoiceV2::Empty;

    let b_len = (last <= BITMAP_LAST_MEMBER).then(|| bitmap_len(last));
    if let Some(len) = b_len {
        best_len = len;
        best_order = 0;
        best = ChoiceV2::Bitmap { total: len };
    }

    // Candidate 2: List (order 1). Shortcut (one_image.bend): a Bitmap of <= n + 1 bytes
    // (last < 8 * n) is never beaten by a List.
    if n <= LIST_MAX_MEMBERS as u64 {
        let skip_list = b_len.is_some_and(|b| b <= n as usize + 1);
        if !skip_list {
            let l_len = list_len_from(n as usize, first, span);
            if l_len < best_len {
                best_len = l_len;
                best_order = 1;
                best = ChoiceV2::List { total: l_len };
            }
        }
    }

    // Candidate 4: Elias-Fano (order 3, sealed only).
    if sealed && let Some(layout) = Ef2Layout::new(n, u64::from(span)) {
        let e_len = 1
            + uleb_len((n - 1) as u32)
            + uleb_len(first)
            + uleb_len(span)
            + layout.payload_bytes();
        if e_len < best_len {
            best_len = e_len;
            best_order = 3;
            best = ChoiceV2::EliasFano {
                layout,
                first,
                span,
                total: e_len,
            };
        }
    }

    // Candidate 3: Directory (order 2). Shortcut (form_choice.bend): any Directory for n >= 2 is
    // at least 11 bytes (7B framing + 4B Stride/Array), and Bitmap/List win ties at 11B.
    let dir_impossible = (best_order <= 1 && best_len <= 11) || (best_order == 3 && best_len < 11);
    if !dir_impossible {
        let body_bytes = price_dir_body();
        let d_len = dir_total_from_body(body_bytes);
        if d_len < best_len || (d_len == best_len && best_order > 2) {
            best = ChoiceV2::Directory {
                body_bytes,
                total: d_len,
            };
        }
    }
    best
}

// ── Public Format 2 slice writers (§7.5) ─────────────────────────────────

/// Exact byte length of the Format 2 sealed image (§7.1) for an ascending slice `members`,
/// allocation-free.
pub fn image_len_v2(members: &[u32]) -> usize {
    if members.is_empty() {
        return 1;
    }
    choose_v2(
        members.len() as u64,
        members[0],
        members[members.len() - 1],
        true,
        || dir_body_bytes_from_slice(members),
    )
    .total()
}

/// Exact byte length of the Format 2 forest image (§7.2) for an ascending slice `members`,
/// allocation-free.
pub fn forest_len_v2(members: &[u32]) -> usize {
    if members.is_empty() {
        return 1;
    }
    choose_v2(
        members.len() as u64,
        members[0],
        members[members.len() - 1],
        false,
        || dir_body_bytes_from_slice(members),
    )
    .total()
}

/// Write the Format 2 sealed image (§7.1) for an ascending slice `members` into `out`, returning
/// the bytes written. Allocation-free for Empty, Bitmap, Single, List, and Elias-Fano (§7.5).
pub fn write_image_into_v2(members: &[u32], out: &mut [u8]) -> Result<usize, PatchError> {
    write_slice_v2(members, out, true)
}

/// Write the Format 2 forest image (§7.2) for an ascending slice `members` into `out`, returning
/// the bytes written. Allocation-free for Empty, Bitmap, Single, and List.
pub fn write_forest_into_v2(members: &[u32], out: &mut [u8]) -> Result<usize, PatchError> {
    write_slice_v2(members, out, false)
}

fn write_slice_v2(members: &[u32], out: &mut [u8], sealed: bool) -> Result<usize, PatchError> {
    if members.is_empty() {
        if out.is_empty() {
            return Err(PatchError::Capacity { needed: 1 });
        }
        out[0] = BITMAP_TAG;
        return Ok(1);
    }
    let choice = choose_v2(
        members.len() as u64,
        members[0],
        members[members.len() - 1],
        sealed,
        || dir_body_bytes_from_slice(members),
    );
    let needed = choice.total();
    if out.len() < needed {
        return Err(PatchError::Capacity { needed });
    }
    match choice {
        ChoiceV2::Empty => {
            out[0] = BITMAP_TAG;
            Ok(1)
        }
        ChoiceV2::Bitmap { .. } => {
            let written = write_bitmap(members, out).expect("checked capacity");
            debug_assert_eq!(written, needed);
            Ok(written)
        }
        ChoiceV2::Single { .. } => {
            let written = write_single(members[0], out).expect("checked capacity");
            debug_assert_eq!(written, needed);
            Ok(written)
        }
        ChoiceV2::List { .. } => {
            let written = write_list(members, out).expect("checked capacity");
            debug_assert_eq!(written, needed);
            Ok(written)
        }
        ChoiceV2::EliasFano { .. } => {
            let written = write_ef2(members, out).expect("checked capacity");
            debug_assert_eq!(written, needed);
            Ok(written)
        }
        ChoiceV2::Directory { body_bytes, .. } => {
            let owned = Bitmosaic::from_sorted(members.iter().copied());
            debug_assert_eq!(owned.forest_body_bytes(), body_bytes);
            out[0] = DIR32_CONTROL;
            let mut at = 1usize;
            write_uleb(out, &mut at, body_bytes as u32);
            owned.write_forest_body(&mut out[at..at + body_bytes]);
            debug_assert_eq!(at + body_bytes, needed);
            Ok(needed)
        }
    }
}

/// Write the Format 2 sealed image for an ascending `u64` slice (§7.4 & §9).
pub fn to_v2_bytes_u64(members: &[u64]) -> Vec<u8> {
    if members.is_empty() {
        return EMPTY_V2_IMAGE.to_vec();
    }
    if members[members.len() - 1] <= u64::from(u32::MAX) {
        let low: Vec<u32> = members.iter().map(|&v| v as u32).collect();
        let len = image_len_v2(&low);
        let mut out = vec![0u8; len];
        write_image_into_v2(&low, &mut out).expect("exact capacity");
        return out;
    }
    Bitmosaic64::from_sorted(members.iter().copied()).to_v2_bytes()
}

// ── Bitmosaic / Bitmosaic64 Format 2 encoders ────────────────────────────

impl Bitmosaic {
    fn plan_v2(&self, sealed: bool) -> ChoiceV2 {
        let n = self.len();
        if n == 0 {
            return ChoiceV2::Empty;
        }
        let first = self.select(0).unwrap_or(0);
        let last = self.select(n - 1).unwrap_or(0);
        choose_v2(n, first, last, sealed, || self.forest_body_bytes())
    }

    fn write_choice_v2(&self, choice: ChoiceV2, out: &mut [u8]) {
        match choice {
            ChoiceV2::Empty => {
                out[0] = BITMAP_TAG;
            }
            ChoiceV2::Bitmap { total } => {
                out[0] = BITMAP_TAG | (total - 1) as u8;
                out[1..total].fill(0);
                for v in self.iter() {
                    out[1 + (v >> 3) as usize] |= 1 << (v & 7);
                }
            }
            ChoiceV2::Single { .. } => {
                let v = self.select(0).expect("nonempty");
                write_single(v, out).expect("checked capacity");
            }
            ChoiceV2::List { .. } => {
                let mut buf = [0u32; LIST_MAX_MEMBERS];
                let mut k = 0usize;
                for v in self.iter() {
                    buf[k] = v;
                    k += 1;
                }
                write_list(&buf[..k], out).expect("checked capacity");
            }
            ChoiceV2::EliasFano {
                layout,
                first,
                span,
                ..
            } => {
                encode_ef2_into(self.iter(), self.len(), first, span, layout, out);
            }
            ChoiceV2::Directory { body_bytes, .. } => {
                out[0] = DIR32_CONTROL;
                let mut at = 1usize;
                write_uleb(out, &mut at, body_bytes as u32);
                self.write_forest_body(&mut out[at..at + body_bytes]);
            }
        }
    }

    /// Exact byte length of the Format 2 sealed image (§7.1).
    pub fn v2_serialized_len(&self) -> usize {
        self.plan_v2(true).total()
    }

    /// Encode this set as a Format 2 sealed image (§7.1).
    pub fn to_v2_bytes(&self) -> Vec<u8> {
        let choice = self.plan_v2(true);
        let mut out = vec![0u8; choice.total()];
        self.write_choice_v2(choice, &mut out);
        out
    }

    /// Write the Format 2 forest image into `out`, returning bytes written.
    pub fn write_v2_forest_into_slice(&self, out: &mut [u8]) -> Result<usize, PatchError> {
        let choice = self.plan_v2(false);
        let needed = choice.total();
        let Some(dst) = out.get_mut(..needed) else {
            return Err(PatchError::Capacity { needed });
        };
        self.write_choice_v2(choice, dst);
        Ok(needed)
    }
}

impl Bitmosaic64 {
    /// Exact byte length of the Format 2 sealed `u64` image (§7.4 & §9).
    pub fn v2_serialized_len(&self) -> usize {
        let m = self.forests().len();
        if m == 0 {
            return 1;
        }
        if m == 1 && self.highs()[0] == 0 {
            return self.forests()[0].v2_serialized_len();
        }
        let header = 1 + uleb_len((m - 1) as u32);
        let geom = Geometry64V2::new(m, header).expect("u64 directory fits address space");
        self.forests()
            .iter()
            .fold(geom.children_at, |acc, f| acc + f.v2_serialized_len())
    }

    /// Encode this `u64` set as a Format 2 sealed image (§7.4 & §9).
    pub fn to_v2_bytes(&self) -> Vec<u8> {
        let m = self.forests().len();
        if m == 0 {
            return EMPTY_V2_IMAGE.to_vec();
        }
        if m == 1 && self.highs()[0] == 0 {
            return self.forests()[0].to_v2_bytes();
        }
        let plans: Vec<ChoiceV2> = self.forests().iter().map(|f| f.plan_v2(true)).collect();
        let count = u32::try_from(m - 1).expect("u64 directory count fits u32");
        let header = 1 + uleb_len(count);
        let geom = Geometry64V2::new(m, header).expect("u64 directory fits address space");
        let total = plans
            .iter()
            .fold(geom.children_at, |acc, p| acc + p.total());
        let mut out = vec![0u8; total];
        out[0] = DIR64_CONTROL;
        let mut at = 1usize;
        write_uleb(&mut out, &mut at, count);
        debug_assert_eq!(at, geom.keys_at);
        let mut before = 0u64;
        let mut child_at = geom.children_at;
        for (i, ((&high, forest), plan)) in self
            .highs()
            .iter()
            .zip(self.forests())
            .zip(plans)
            .enumerate()
        {
            out[geom.keys_at + 4 * i..][..4].copy_from_slice(&high.to_le_bytes());
            if i != 0 && i + 1 != m {
                out[geom.before_at + 8 * (i - 1)..][..8].copy_from_slice(&before.to_le_bytes());
            }
            if i != 0 {
                out[geom.offsets_at + 4 * (i - 1)..][..4]
                    .copy_from_slice(&((child_at - geom.children_at) as u32).to_le_bytes());
            }
            let c_len = plan.total();
            forest.write_choice_v2(plan, &mut out[child_at..child_at + c_len]);
            child_at += c_len;
            before = before
                .checked_add(forest.len())
                .expect("u64 cardinality overflow");
        }
        debug_assert_eq!(child_at, total);
        out
    }
}

// ── Format 2 Patching (§12.3) ────────────────────────────────────────────

/// Upper bound on the Format 2 forest image length after `adds` new members landing in `touches`
/// distinct chunks, in bounded constant work (§12.3).
///
/// The directory of the patched set bounds every Format 2 forest image, so this is the directory
/// bound over the image's member and chunk counts.
pub fn patched_len_bound_v2(image: &[u8], adds: u64, touches: u64) -> Option<u64> {
    let header = image_header_v2(image).ok()?;
    if header.format != Format::Format2 || header.width != KeyWidth::U32 {
        return None;
    }
    let (encoded, members, chunks) = match header.form {
        ImageForm::Bitmap => {
            let b = BitmapView::open(image).ok()?;
            let len = u64::from(b.len());
            (b.extent() as u64, len, u64::from(len != 0))
        }
        ImageForm::Single => {
            let s = SingleView::open(image).ok()?;
            (s.extent() as u64, 1, 1)
        }
        ImageForm::List => {
            let l = ListView::open(image).ok()?;
            // At most 8 members => at most 8 chunks.
            (l.extent() as u64, u64::from(l.len()), u64::from(l.len()))
        }
        ImageForm::EliasFano => {
            let mut at = 1usize;
            let len = u64::from(read_uleb(image, &mut at, u32::MAX)?) + 1;
            let chunks = len.min(1 << 16);
            (header.extent as u64, len, chunks)
        }
        ImageForm::Directory => {
            let mut at = 1usize;
            let _body = read_uleb(image, &mut at, u32::MAX)?;
            let len = u64::from(read_uleb(image, &mut at, u32::MAX)?) + 1;
            let chunks = u64::from(read_uleb(image, &mut at, u32::MAX)?) + 1;
            (header.extent as u64, len, chunks)
        }
        ImageForm::Directory64 => return None,
    };
    let total_members = members.saturating_add(adds);
    let words_max =
        u64::try_from(WORDS_AT + 8 * CHUNK_WORDS + 2 * (CHUNK_WORDS / DIR_STRIDE - 1)).ok()?;
    let array_chunk = u64::try_from(ARRAY_MAX).ok()? + 1;
    let dense_excess = words_max.saturating_sub(2 * array_chunk);
    let dense_chunks = total_members / array_chunk;
    let payload = total_members
        .saturating_mul(2)
        .saturating_add(touches.saturating_mul(3))
        .saturating_add(dense_chunks.saturating_mul(dense_excess));
    let directory = adds
        .saturating_mul(11)
        .saturating_add(chunks.saturating_mul(11));
    let grown_chunks = chunks.saturating_add(adds).min(1 << 16);
    let slot_plane = if grown_chunks <= 64 {
        0
    } else {
        8 * (grown_chunks * 8 / 7 + 1).next_power_of_two()
    };
    Some(
        encoded
            .saturating_add(payload)
            .saturating_add(directory)
            .saturating_add(slot_plane)
            .saturating_add(16),
    )
}

#[inline]
fn is_strictly_ascending(xs: &[u32]) -> bool {
    xs.windows(2).all(|w| w[0] < w[1])
}

/// Patch a Format 2 `u32` image in `slot` with strictly ascending `adds` and `removes` (adds land
/// before removes when a value appears in both), leaving the canonical Format 2 forest image
/// (§7.2, §12.3) at `slot[0..report.serialized_len]` with zeroed tail bytes if shrunk.
///
/// On any error (`Capacity`, `Malformed`, `Unsorted`), `slot` is left completely untouched.
pub fn patch_v2(
    slot: &mut [u8],
    adds: &[u32],
    removes: &[u32],
    scratch: &mut PatchScratch,
) -> Result<PatchReport, PatchError> {
    patch_v2_witnessed(slot, adds, removes, scratch, &mut ())
}

/// [`patch_v2`] with a per-value [`Witness`] reporting which adds were new and which removes were
/// held (§12.3).
pub fn patch_v2_witnessed<W: Witness>(
    slot: &mut [u8],
    adds: &[u32],
    removes: &[u32],
    _scratch: &mut PatchScratch,
    witness: &mut W,
) -> Result<PatchReport, PatchError> {
    if !is_strictly_ascending(adds) || !is_strictly_ascending(removes) {
        return Err(PatchError::Unsorted);
    }
    enum PatchKind {
        Bitmap {
            buf: [u8; 128],
            old_extent: usize,
        },
        Small {
            old_buf: [u32; LIST_MAX_MEMBERS],
            old_len: usize,
            old_extent: usize,
        },
        General {
            merged: Vec<u32>,
            added: u32,
            removed: u32,
            old_extent: usize,
        },
    }
    let kind = {
        let old = V2View::open(slot).map_err(|_| PatchError::Malformed)?;
        let old_extent = old.serialized_len();
        if let V2View::Bitmap(bm) = old
            && adds.last().copied().unwrap_or(0) <= BITMAP_LAST_MEMBER
        {
            let mut buf = [0u8; 128];
            let old_n = old_extent - 1;
            buf[1..1 + old_n].copy_from_slice(bm.bytes());
            PatchKind::Bitmap { buf, old_extent }
        } else if matches!(old, V2View::Single(_) | V2View::List(_))
            && old.len() as usize + adds.len() <= 32
        {
            let mut old_buf = [0u32; LIST_MAX_MEMBERS];
            let mut old_len = 0usize;
            old.for_each(|v| {
                old_buf[old_len] = v;
                old_len += 1;
            });
            PatchKind::Small {
                old_buf,
                old_len,
                old_extent,
            }
        } else {
            let (merged, added, removed) = merge_v2_general(old, adds, removes, witness);
            PatchKind::General {
                merged,
                added,
                removed,
                old_extent,
            }
        }
    };

    match kind {
        PatchKind::Bitmap { buf, old_extent } => {
            patch_v2_bitmap_fast(slot, buf, old_extent, adds, removes, witness)
        }
        PatchKind::Small {
            old_buf,
            old_len,
            old_extent,
        } => patch_v2_small_merge(
            slot,
            &old_buf[..old_len],
            old_extent,
            adds,
            removes,
            witness,
        ),
        PatchKind::General {
            merged,
            added,
            removed,
            old_extent,
        } => {
            let written = write_forest_into_v2(&merged, slot)?;
            if written < old_extent {
                slot[written..old_extent].fill(0);
            }
            Ok(PatchReport {
                added,
                removed,
                len: merged.len() as u64,
                serialized_len: written,
            })
        }
    }
}

/// Zero-allocation in-place Bitmap patch fast path (§12.3).
fn patch_v2_bitmap_fast<W: Witness>(
    slot: &mut [u8],
    mut buf: [u8; 128],
    old_extent: usize,
    adds: &[u32],
    removes: &[u32],
    witness: &mut W,
) -> Result<PatchReport, PatchError> {
    let old_n = old_extent - 1;
    let max_add_bytes = adds.last().map_or(0, |&v| 1 + (v >> 3) as usize);
    let mut work_n = old_n.max(max_add_bytes);

    let mut added = 0u32;
    for (i, &v) in adds.iter().enumerate() {
        let idx = 1 + (v >> 3) as usize;
        let bit = 1u8 << (v & 7);
        if buf[idx] & bit == 0 {
            buf[idx] |= bit;
            added += 1;
            if W::LISTENS {
                witness.add_new(i);
            }
        }
    }

    let mut removed = 0u32;
    for (i, &v) in removes.iter().enumerate() {
        if v <= BITMAP_LAST_MEMBER {
            let idx = 1 + (v >> 3) as usize;
            let bit = 1u8 << (v & 7);
            if idx <= work_n && buf[idx] & bit != 0 {
                buf[idx] &= !bit;
                removed += 1;
                if W::LISTENS {
                    witness.remove_held(i);
                }
            }
        }
    }

    while work_n > 0 && buf[work_n] == 0 {
        work_n -= 1;
    }

    if work_n == 0 {
        // Empty set -> 0x80 (1 byte).
        slot[0] = BITMAP_TAG;
        if old_extent > 1 {
            slot[1..old_extent].fill(0);
        }
        return Ok(PatchReport {
            added,
            removed,
            len: 0,
            serialized_len: 1,
        });
    }

    buf[0] = BITMAP_TAG | work_n as u8;
    let new_bm_len = 1 + work_n;
    let new_bm = BitmapView::open_admitted(&buf[..new_bm_len]).expect("valid staged bitmap");
    let new_len = new_bm.len();

    // Fast path: when new_bm_len <= 11 (last <= 79), Directory (>= 11, loses ties) cannot win,
    // and when new_len > 8 (or new_bm_len <= new_len + 1), List cannot win either!
    let stay_bitmap = if new_bm_len <= 11 && (new_len > 8 || new_bm_len <= new_len as usize + 1) {
        true
    } else {
        let first = new_bm.first_member().expect("nonempty");
        let last = new_bm.last_member().expect("nonempty");
        let list_beats = new_len <= LIST_MAX_MEMBERS as u32
            && list_len_from(new_len as usize, first, last - first) < new_bm_len;
        let dir_beats = !list_beats
            && new_bm_len > 11
            && dir_total_from_bitmap(&new_bm, new_len as usize, first, last) < new_bm_len;
        !list_beats && !dir_beats
    };

    if stay_bitmap {
        if slot.len() < new_bm_len {
            return Err(PatchError::Capacity { needed: new_bm_len });
        }
        slot[..new_bm_len].copy_from_slice(&buf[..new_bm_len]);
        if new_bm_len < old_extent {
            slot[new_bm_len..old_extent].fill(0);
        }
        return Ok(PatchReport {
            added,
            removed,
            len: u64::from(new_len),
            serialized_len: new_bm_len,
        });
    }

    // Form transition from Bitmap -> Single/List or Directory.
    if new_len <= LIST_MAX_MEMBERS as u32 {
        let mut members = [0u32; LIST_MAX_MEMBERS];
        let mut k = 0usize;
        new_bm.for_each(|v| {
            members[k] = v;
            k += 1;
        });
        let written = write_forest_into_v2(&members[..k], slot)?;
        if written < old_extent {
            slot[written..old_extent].fill(0);
        }
        return Ok(PatchReport {
            added,
            removed,
            len: u64::from(new_len),
            serialized_len: written,
        });
    }

    let owned = Bitmosaic::from_sorted(new_bm.iter());
    let written = owned.write_v2_forest_into_slice(slot)?;
    if written < old_extent {
        slot[written..old_extent].fill(0);
    }
    Ok(PatchReport {
        added,
        removed,
        len: u64::from(new_len),
        serialized_len: written,
    })
}

/// Zero-allocation stack merge for Single / List when `old.len() + adds.len() <= 32`.
fn patch_v2_small_merge<W: Witness>(
    slot: &mut [u8],
    old_slice: &[u32],
    old_extent: usize,
    adds: &[u32],
    removes: &[u32],
    witness: &mut W,
) -> Result<PatchReport, PatchError> {
    // Step 1: union(old_slice, adds) into stack buffer `with_adds`.
    let mut with_adds = [0u32; 32];
    let mut wa_len = 0usize;
    let (mut i, mut j) = (0usize, 0usize);
    let mut added = 0u32;
    while i < old_slice.len() && j < adds.len() {
        let (o, a) = (old_slice[i], adds[j]);
        if o < a {
            with_adds[wa_len] = o;
            wa_len += 1;
            i += 1;
        } else if a < o {
            with_adds[wa_len] = a;
            wa_len += 1;
            added += 1;
            if W::LISTENS {
                witness.add_new(j);
            }
            j += 1;
        } else {
            with_adds[wa_len] = o;
            wa_len += 1;
            i += 1;
            j += 1;
        }
    }
    while i < old_slice.len() {
        with_adds[wa_len] = old_slice[i];
        wa_len += 1;
        i += 1;
    }
    while j < adds.len() {
        with_adds[wa_len] = adds[j];
        wa_len += 1;
        added += 1;
        if W::LISTENS {
            witness.add_new(j);
        }
        j += 1;
    }

    // Step 2: difference(with_adds, removes) into stack buffer `out_members`.
    let mut out_members = [0u32; 32];
    let mut out_len = 0usize;
    let (mut i, mut r) = (0usize, 0usize);
    let mut removed = 0u32;
    while i < wa_len && r < removes.len() {
        let (v, rem) = (with_adds[i], removes[r]);
        if v < rem {
            out_members[out_len] = v;
            out_len += 1;
            i += 1;
        } else if rem < v {
            r += 1;
        } else {
            removed += 1;
            if W::LISTENS {
                witness.remove_held(r);
            }
            i += 1;
            r += 1;
        }
    }
    while i < wa_len {
        out_members[out_len] = with_adds[i];
        out_len += 1;
        i += 1;
    }

    let written = write_forest_into_v2(&out_members[..out_len], slot)?;
    if written < old_extent {
        slot[written..old_extent].fill(0);
    }
    Ok(PatchReport {
        added,
        removed,
        len: out_len as u64,
        serialized_len: written,
    })
}

/// General Format 2 merge for Directory, Elias-Fano, or large batches.
fn merge_v2_general<W: Witness>(
    old: V2View<'_>,
    adds: &[u32],
    removes: &[u32],
    witness: &mut W,
) -> (Vec<u32>, u32, u32) {
    let mut merged = Vec::with_capacity(old.len() as usize + adds.len());
    let mut r_old = old.range();
    let (mut a_idx, mut r_idx) = (0usize, 0usize);
    let mut added = 0u32;
    let mut removed = 0u32;

    // Single streaming 3-way merge: union(old, adds) minus removes, adds landing before removes.
    loop {
        let next_val = match (
            (!r_old.empty()).then(|| r_old.front()),
            adds.get(a_idx).copied(),
        ) {
            (Some(o), Some(a)) => {
                if o < a {
                    r_old.pop_front();
                    o
                } else if a < o {
                    added += 1;
                    if W::LISTENS {
                        witness.add_new(a_idx);
                    }
                    a_idx += 1;
                    a
                } else {
                    r_old.pop_front();
                    a_idx += 1;
                    o
                }
            }
            (Some(o), None) => {
                r_old.pop_front();
                o
            }
            (None, Some(a)) => {
                added += 1;
                if W::LISTENS {
                    witness.add_new(a_idx);
                }
                a_idx += 1;
                a
            }
            (None, None) => break,
        };

        while r_idx < removes.len() && removes[r_idx] < next_val {
            r_idx += 1;
        }
        if r_idx < removes.len() && removes[r_idx] == next_val {
            removed += 1;
            if W::LISTENS {
                witness.remove_held(r_idx);
            }
            r_idx += 1;
        } else {
            merged.push(next_val);
        }
    }

    (merged, added, removed)
}
