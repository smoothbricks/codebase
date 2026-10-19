//! The one bound that decides a test hang: the test target's no-progress backstop.
//!
//! A test waits for the event it is waiting for; no test carries a deadline of its own
//! (specs/cowshed/08_testing.md, the bounded test targets). A product bound a test must configure
//! that is not the case's subject — a gateway's connect or request bound around a fixture that
//! answers — is either disabled, where the product can disable it, or set to this backstop, so
//! it never races the host's load and the run's no-progress bound stays the one that decides.
//!
//! The value is the plugin's `BOUNDED_TEST_IDLE_TIMEOUT_MS`, read from its source at compile time,
//! so there is one number and no reader can pick a different one. The module is always compiled
//! (two constants, no runtime cost), so a dependant's tests take it through a plain dependency,
//! with no feature that would split the build's units from the tests'.

use std::time::Duration;

const POLICY: &str = include_str!("../../../../nx-plugin/src/test-backstop.ts");

/// The test target's no-progress backstop in milliseconds (`BOUNDED_TEST_IDLE_TIMEOUT_MS`), for
/// product bounds a test configures as a millisecond count.
pub const NO_PROGRESS_MS: u64 = declared_millis(
    POLICY.as_bytes(),
    b"export const BOUNDED_TEST_IDLE_TIMEOUT_MS = ",
);

/// The test target's no-progress backstop, for product bounds a test configures as a `Duration`.
pub const NO_PROGRESS: Duration = Duration::from_millis(NO_PROGRESS_MS);

/// The digits (`_` separators allowed) that follow `declaration` in `source`, up to `;`. A source
/// that does not declare it, or declares anything but a number literal, fails the build.
const fn declared_millis(source: &[u8], declaration: &[u8]) -> u64 {
    let mut start = 0;
    'search: while start + declaration.len() <= source.len() {
        let mut index = 0;
        while index < declaration.len() {
            if source[start + index] != declaration[index] {
                start += 1;
                continue 'search;
            }
            index += 1;
        }
        let mut position = start + declaration.len();
        let mut millis: u64 = 0;
        let mut digits = 0;
        while position < source.len() && source[position] != b';' {
            match source[position] {
                b'_' => {}
                byte @ b'0'..=b'9' => {
                    millis = millis * 10 + (byte - b'0') as u64;
                    digits += 1;
                }
                _ => panic!("{}", EXPECTED),
            }
            position += 1;
        }
        assert!(digits > 0 && position < source.len(), "{}", EXPECTED);
        return millis;
    }
    panic!("{}", EXPECTED)
}

const EXPECTED: &str = "packages/nx-plugin/src/test-backstop.ts must declare the test backstop \
     as one line `export const BOUNDED_TEST_IDLE_TIMEOUT_MS = <milliseconds>;` (digits and `_` only), \
     which cowshed-os/src/test_backstop.rs reads at compile time";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_declared_number_is_read_with_its_separators() {
        assert_eq!(
            declared_millis(b"x\nexport const B = 180_000;\n", b"export const B = "),
            180_000
        );
    }
}
