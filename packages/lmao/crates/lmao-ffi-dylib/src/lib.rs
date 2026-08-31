//! Native shared-library surface for Bun's `bun:ffi` thread lane.
//!
//! The ABI lives in `lmao-core`; this crate only turns its native symbols into
//! the consumer-supplied cdylib artifact. Keeping the implementation in core
//! makes the pointer-handle contract identical across native and Wasm lanes.

pub use lmao_core::thread_ffi::*;

#[cfg(test)]
mod tests {
    use super::{thread_span_buffer_free, thread_span_buffer_new};

    #[test]
    fn reexported_constructor_and_destructor_are_linked() {
        let handle = thread_span_buffer_new(7, 8);
        assert!(!handle.is_null());
        unsafe { thread_span_buffer_free(handle) };
    }
}
