#![cfg_attr(target_arch = "wasm32", no_std)]
//! `span_start.{wasm,node}` — timestamp-accuracy proof instrumentation.
//!
//! This crate implements the proof machinery (measuring span-timestamp accuracy
//! for the proof harness `proofs/timestamp-accuracy.proof.ts`), not runtime code.
//! Its WASM and Node-API exports share the fixed proof-buffer layout.

#[cfg(target_arch = "wasm32")]
#[panic_handler]
fn panic(_info: &core::panic::PanicInfo<'_>) -> ! {
    core::arch::wasm32::unreachable()
}

pub mod layout;

#[cfg(target_arch = "wasm32")]
mod wasm {
    //! The `span_start.wasm` exports use `performanceNow`/`dateNow`; JS supplies
    //! both host clocks and owns the imported memory. Pointers are u32 offsets
    //! into that memory.

    use crate::layout;

    #[link(wasm_import_module = "env")]
    unsafe extern "C" {
        #[link_name = "performanceNow"]
        fn performance_now() -> f64;
        #[link_name = "dateNow"]
        fn date_now() -> f64;
    }

    const TRACE_ROOT_WALL_CLOCK_OFFSET: usize = 0;
    const TRACE_ROOT_MONOTONIC_OFFSET: usize = 8;

    #[inline]
    unsafe fn buf_at<'a>(offset: u32, len: usize) -> &'a mut [u8] {
        unsafe { core::slice::from_raw_parts_mut(offset as usize as *mut u8, len) }
    }

    #[inline]
    unsafe fn timestamp_nanos(trace_root_ptr: u32) -> i64 {
        let root = unsafe { buf_at(trace_root_ptr, 16) };
        let wall_clock = i64::from_le_bytes(
            root[TRACE_ROOT_WALL_CLOCK_OFFSET..TRACE_ROOT_WALL_CLOCK_OFFSET + 8]
                .try_into()
                .expect("trace root wall clock"),
        );
        let monotonic_ms = f64::from_le_bytes(
            root[TRACE_ROOT_MONOTONIC_OFFSET..TRACE_ROOT_MONOTONIC_OFFSET + 8]
                .try_into()
                .expect("trace root monotonic"),
        );
        let elapsed_ms = unsafe { performance_now() } - monotonic_ms;
        wall_clock + (elapsed_ms * 1_000_000.0) as i64
    }

    #[unsafe(no_mangle)]
    pub unsafe extern "C" fn init_trace_root(trace_root_ptr: u32) {
        let root = unsafe { buf_at(trace_root_ptr, 16) };
        let wall_clock = (unsafe { date_now() }) as i64 * 1_000_000;
        root[0..8].copy_from_slice(&wall_clock.to_le_bytes());
        let monotonic = unsafe { performance_now() };
        root[8..16].copy_from_slice(&monotonic.to_le_bytes());
    }

    #[unsafe(no_mangle)]
    pub unsafe extern "C" fn span_start(system_ptr: u32, capacity: u32, trace_root_ptr: u32) {
        let ts = unsafe { timestamp_nanos(trace_root_ptr) };
        let buf = unsafe { buf_at(system_ptr, layout::buffer_len(capacity)) };
        layout::write_span_start(buf, capacity, ts);
    }

    #[unsafe(no_mangle)]
    pub unsafe extern "C" fn span_end_ok(system_ptr: u32, capacity: u32, trace_root_ptr: u32) {
        let ts = unsafe { timestamp_nanos(trace_root_ptr) };
        let buf = unsafe { buf_at(system_ptr, layout::buffer_len(capacity)) };
        layout::write_span_end(buf, capacity, layout::ENTRY_TYPE_SPAN_OK, ts);
    }

    #[unsafe(no_mangle)]
    pub unsafe extern "C" fn span_end_err(system_ptr: u32, capacity: u32, trace_root_ptr: u32) {
        let ts = unsafe { timestamp_nanos(trace_root_ptr) };
        let buf = unsafe { buf_at(system_ptr, layout::buffer_len(capacity)) };
        layout::write_span_end(buf, capacity, layout::ENTRY_TYPE_SPAN_ERR, ts);
    }

    #[unsafe(no_mangle)]
    pub unsafe extern "C" fn write_log_entry(
        system_ptr: u32,
        capacity: u32,
        trace_root_ptr: u32,
        entry_type: u8,
    ) -> u32 {
        let ts = unsafe { timestamp_nanos(trace_root_ptr) };
        let buf = unsafe { buf_at(system_ptr, layout::buffer_len(capacity)) };
        layout::write_log_entry(buf, capacity, entry_type, ts)
    }

    #[unsafe(no_mangle)]
    pub unsafe extern "C" fn get_performance_now() -> f64 {
        unsafe { performance_now() }
    }

    #[unsafe(no_mangle)]
    pub unsafe extern "C" fn debug_compute_timestamp(trace_root_ptr: u32) -> i64 {
        unsafe { timestamp_nanos(trace_root_ptr) }
    }
}

#[cfg_attr(test, allow(dead_code))]
#[cfg(all(feature = "napi", not(target_arch = "wasm32")))]
mod node {
    //! The `span_start.node` surface is the Node-API implementation: trace-root
    //! anchors are keyed by the ArrayBuffer's data pointer, wall-clock time is
    //! captured at init, and monotonic elapsed time comes from `Instant`.

    use crate::layout;
    use napi::JsArrayBuffer;
    use napi::bindgen_prelude::*;
    use napi_derive::napi;
    use std::sync::Mutex;
    use std::time::Instant;

    struct TraceRootAnchor {
        key: usize,
        start_wall_clock_nanos: i64,
        started: Instant,
    }

    const MAX_TRACE_ROOTS: usize = 64;
    static ANCHORS: Mutex<Vec<TraceRootAnchor>> = Mutex::new(Vec::new());

    fn wall_clock_nanos() -> i64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0)
    }

    fn key_of(buf: &[u8]) -> usize {
        buf.as_ptr() as usize
    }

    fn upsert_anchor(key: usize) -> Result<()> {
        let mut anchors = ANCHORS.lock().expect("anchor table");
        if let Some(a) = anchors.iter_mut().find(|a| a.key == key) {
            a.start_wall_clock_nanos = wall_clock_nanos();
            a.started = Instant::now();
            return Ok(());
        }
        if anchors.len() >= MAX_TRACE_ROOTS {
            return Err(Error::from_reason("trace-root anchor table full (64)"));
        }
        anchors.push(TraceRootAnchor {
            key,
            start_wall_clock_nanos: wall_clock_nanos(),
            started: Instant::now(),
        });
        Ok(())
    }

    fn current_timestamp(key: usize) -> Result<i64> {
        let anchors = ANCHORS.lock().expect("anchor table");
        let anchor = anchors
            .iter()
            .find(|a| a.key == key)
            .ok_or_else(|| Error::from_reason("unknown trace root (initTraceRoot first)"))?;
        Ok(anchor.start_wall_clock_nanos + anchor.started.elapsed().as_nanos() as i64)
    }

    #[napi(js_name = "initTraceRoot")]
    pub fn init_trace_root(trace_root_system: JsArrayBuffer) -> Result<()> {
        let trace_root_system = trace_root_system.into_value()?;
        upsert_anchor(key_of(trace_root_system.as_ref()))
    }

    #[napi(js_name = "spanStart")]
    pub fn span_start(
        system: JsArrayBuffer,
        capacity: u32,
        trace_root_system: JsArrayBuffer,
    ) -> Result<()> {
        let mut system = system.into_value()?;
        let trace_root_system = trace_root_system.into_value()?;
        let ts = current_timestamp(key_of(trace_root_system.as_ref()))?;
        layout::write_span_start(system.as_mut(), capacity, ts);
        Ok(())
    }

    #[napi(js_name = "spanEndOk")]
    pub fn span_end_ok(
        system: JsArrayBuffer,
        capacity: u32,
        trace_root_system: JsArrayBuffer,
    ) -> Result<()> {
        let mut system = system.into_value()?;
        let trace_root_system = trace_root_system.into_value()?;
        let ts = current_timestamp(key_of(trace_root_system.as_ref()))?;
        layout::write_span_end(system.as_mut(), capacity, layout::ENTRY_TYPE_SPAN_OK, ts);
        Ok(())
    }

    #[napi(js_name = "spanEndErr")]
    pub fn span_end_err(
        system: JsArrayBuffer,
        capacity: u32,
        trace_root_system: JsArrayBuffer,
    ) -> Result<()> {
        let mut system = system.into_value()?;
        let trace_root_system = trace_root_system.into_value()?;
        let ts = current_timestamp(key_of(trace_root_system.as_ref()))?;
        layout::write_span_end(system.as_mut(), capacity, layout::ENTRY_TYPE_SPAN_ERR, ts);
        Ok(())
    }

    #[napi(js_name = "writeLogEntry")]
    pub fn write_log_entry(
        system: JsArrayBuffer,
        capacity: u32,
        trace_root_system: JsArrayBuffer,
        entry_type: u8,
    ) -> Result<u32> {
        let mut system = system.into_value()?;
        let trace_root_system = trace_root_system.into_value()?;
        let ts = current_timestamp(key_of(trace_root_system.as_ref()))?;
        Ok(layout::write_log_entry(
            system.as_mut(),
            capacity,
            entry_type,
            ts,
        ))
    }
}
