//! Message-free invariant aborts for the wasm artifact.
//!
//! Every invariant panic in the VM crates costs its message string, a
//! `core::panic::Location` file-path string, and a slice of `core::fmt`
//! plumbing in the shipped wasm — none of which is observable there:
//! `panic = "abort"` traps without printing, exactly like the Zig
//! ReleaseSmall build it replaces. Native builds (tests, NAPI, FFI) keep the
//! fully formatted message, because there the message IS observable and
//! debugging depends on it.
//!
//! `die!` is the panic-with-message replacement; `check!` replaces
//! `assert!(cond, "msg")`. Both drop the message tokens entirely on wasm32
//! (real `#[cfg]`, not `cfg!`, so the string literal never reaches codegen).
//! Only genuine programmer-bug invariants may use these — operational
//! failures still return `Err`/`Result` per the repo error discipline.

/// Trap without constructing a panic message or `Location`.
#[cfg(target_arch = "wasm32")]
#[cold]
#[inline(never)]
pub fn trap() -> ! {
    core::arch::wasm32::unreachable()
}

/// Invariant failure: message-free trap on wasm32, formatted panic natively.
#[macro_export]
macro_rules! die {
    ($($arg:tt)*) => {{
        #[cfg(target_arch = "wasm32")]
        {
            $crate::abort::trap()
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            ::core::panic!($($arg)*)
        }
    }};
}

/// Invariant assertion: `assert!` whose message (and fmt plumbing) is
/// dropped from the wasm artifact.
#[macro_export]
macro_rules! check {
    ($cond:expr, $($arg:tt)*) => {{
        if !$cond {
            $crate::die!($($arg)*);
        }
    }};
}
