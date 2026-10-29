//! A `JsonEnvelope` succeeds with the unit value.
//! must name: success
//! must name: Sealed
use cowshed_core::api::JsonEnvelope;
fn deny() {
    let _ = JsonEnvelope::success(());
}
