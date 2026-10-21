//! Diagnostic lines on stderr, each in one write.
//!
//! Every cowshed process of a host (the gateway, each workspace supervisor, their children)
//! writes stderr to one shared file, launchd's `StandardErrorPath`
//! (13_telemetry.md, "The one text-file survivor"). `std`'s `eprintln!` writes an unbuffered
//! stderr one formatted piece at a time, one `write(2)` per piece. Its lock orders threads
//! within a process but nothing across processes, so lines of concurrent processes splice
//! mid-word. Measured 2026-10-21: an install's `supervisor-recovery drain` spans and its
//! supervisors' "retires after draining" lines interleaved beyond reading. [`eprintln!`] formats
//! the whole line first and hands it to the kernel in one write, which an append-mode file
//! places whole.

use std::fmt;
use std::io::Write;

/// Write `args` and a newline to stderr in one write. Like `std::eprintln!`, panics if stderr
/// cannot be written.
pub fn line(args: fmt::Arguments<'_>) {
    let mut text = args.to_string();
    text.push('\n');
    write(&text);
}

/// Write `args` to stderr in one write, with no newline. Like `std::eprint!`, panics if stderr
/// cannot be written.
pub fn text(args: fmt::Arguments<'_>) {
    write(&args.to_string());
}

fn write(text: &str) {
    if let Err(error) = std::io::stderr().lock().write_all(text.as_bytes()) {
        panic!("failed printing to stderr: {error}");
    }
}

/// `std::eprintln!`, as one write: see the module.
#[macro_export]
macro_rules! eprintln {
    () => {
        $crate::stderr::line(::std::format_args!(""))
    };
    ($($arg:tt)*) => {
        $crate::stderr::line(::std::format_args!($($arg)*))
    };
}

/// `std::eprint!`, as one write: see the module.
#[macro_export]
macro_rules! eprint {
    ($($arg:tt)*) => {
        $crate::stderr::text(::std::format_args!($($arg)*))
    };
}

#[cfg(test)]
mod tests {
    /// A line reaches the kernel as one write: formatting completes before anything is
    /// written, so a format argument that panics writes nothing at all, where `std`'s macro
    /// would already have written the text before it.
    #[test]
    fn a_line_is_formatted_whole_before_it_is_written() {
        struct Fails;
        impl std::fmt::Display for Fails {
            fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                Err(std::fmt::Error)
            }
        }
        let formatted = std::panic::catch_unwind(|| {
            crate::eprintln!("cowshed: written only whole {}", Fails);
        });
        assert!(
            formatted.is_err(),
            "a line that cannot be formatted is not written in part"
        );
    }
}
