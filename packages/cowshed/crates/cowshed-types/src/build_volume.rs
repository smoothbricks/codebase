//! Build-volume vocabulary shared below storage: the capacity a volume created from
//! nothing gets, the refresh result the wire carries, and the holder census values
//! storage refusals name. Census and record IO stay with their shells.

/// The capacity of a build volume created from nothing, unless `.cowshed.toml` `[build] capacity`
/// says otherwise (16_build_volumes.md, "Substrate"). Sparse: a cap on build-cache growth, not an
/// allocation.
pub const DEFAULT_BUILD_VOLUME_CAPACITY: crate::image::ImageCapacity =
    crate::image::ImageCapacity::from_gibibytes(100);
