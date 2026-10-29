//! A `CoordinatorToken` is cloned and serialized.
//! must name: Clone
//! must name: Serialize
use cowshed_core::CoordinatorToken;
fn must_clone<T: Clone>() {}
fn must_serialize<T: serde::Serialize>() {}
fn deny() {
    must_clone::<CoordinatorToken>();
    must_serialize::<CoordinatorToken>();
}
