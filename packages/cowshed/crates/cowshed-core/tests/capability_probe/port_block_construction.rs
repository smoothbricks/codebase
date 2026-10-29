//! A `PortBlock` is built by struct literal.
//! must name: PortBlock
//! must name: private
use cowshed_core::api::PortBlock;
fn deny() {
    let _ = PortBlock {
        base: 40960,
        size: 16,
    };
}
