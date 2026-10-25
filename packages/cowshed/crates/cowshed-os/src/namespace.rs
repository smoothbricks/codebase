//! The host namespace: the one value every name cowshed mints on a host derives from
//! (specs/cowshed/01_storage.md, "Host namespace"). Production is `cowshed`. A test run mints a
//! namespace of its own and passes it to every constructor it builds, so no name a test makes can
//! be one production makes or reads: production's inventories, sweeps and collection match only
//! production's names.

use std::fmt;
use std::sync::atomic::{AtomicU32, Ordering};

/// Production's name: its volumes are labelled `[cowshed] …`, its launchd agents are
/// `dev.cowshed.<service>`.
pub const PRODUCTION: &str = "cowshed";

/// The root-owned parent of every user's per-user root on macOS, which `cowshed setup` makes.
pub const MACOS_PREFIX: &str = "/private/var/folders/cowshed";

/// Whose names a value of the host's carries.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum HostNamespace {
    Production,
    Test(TestNamespace),
}

impl HostNamespace {
    /// `cowshed`, or `cowshed-test-<test>`.
    pub fn name(&self) -> String {
        match self {
            Self::Production => PRODUCTION.to_owned(),
            Self::Test(test) => format!("{PRODUCTION}-test-{test}"),
        }
    }

    /// The bracketed name every volume label starts with: `[cowshed]`, or `[cowshed-test-<test>]`.
    pub fn label_prefix(&self) -> String {
        format!("[{}]", self.name())
    }

    /// `<prefix> <detail>`: a volume label of this namespace.
    pub fn volume_label(&self, detail: fmt::Arguments<'_>) -> String {
        format!("[{}] {detail}", self.name())
    }

    /// `dev.<name>.<service>`: the launchd label of one of this namespace's agents.
    pub fn launchd_label(&self, service: &str) -> String {
        format!("dev.{}.{service}", self.name())
    }

    /// This namespace's per-user root on macOS: production's is `/private/var/folders/cowshed/<uid>`,
    /// which holds production's own entries (`mnt`, `ram<revision>`); a test namespace's is the
    /// directory of its name beside them.
    pub fn macos_user_root(&self, uid: u32) -> std::path::PathBuf {
        let root = std::path::Path::new(MACOS_PREFIX).join(uid.to_string());
        match self {
            Self::Production => root,
            Self::Test(test) => root.join(test.as_str()),
        }
    }
}

/// A test run's namespace: seven lowercase base-36 characters, the minting process's pid in the
/// first five (zero-padded, so the first is always a digit, which begins no production name) and
/// that process's ordinal in the last two. The pid makes a dead run's namespace reclaimable by a
/// later one without shared state; the ordinal keeps one process's runs apart.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct TestNamespace([u8; TestNamespace::LEN]);

/// Every namespace this process has minted.
static MINTED: AtomicU32 = AtomicU32::new(0);

const PID_DIGITS: usize = 5;
const ORDINAL_DIGITS: usize = 2;
const BASE: u32 = 36;

impl TestNamespace {
    pub const LEN: usize = PID_DIGITS + ORDINAL_DIGITS;

    /// A namespace no other live process holds: this process's pid and its next ordinal.
    pub fn mint() -> Result<Self, NamespaceError> {
        let ordinal = MINTED.fetch_add(1, Ordering::Relaxed);
        Self::of(std::process::id(), ordinal)
    }

    /// The namespace `pid` mints as its `ordinal`th.
    pub fn of(pid: u32, ordinal: u32) -> Result<Self, NamespaceError> {
        let mut name = [0; Self::LEN];
        encode(pid, &mut name[..PID_DIGITS]).ok_or(NamespaceError::Pid(pid))?;
        encode(ordinal, &mut name[PID_DIGITS..]).ok_or(NamespaceError::Exhausted(ordinal))?;
        Ok(Self(name))
    }

    /// The namespace `name` spells, if it is one.
    pub fn parse(name: &str) -> Option<Self> {
        let bytes: [u8; Self::LEN] = name.as_bytes().try_into().ok()?;
        (bytes[0].is_ascii_digit()
            && bytes
                .iter()
                .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'z')))
        .then_some(Self(bytes))
    }

    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.0).expect("a namespace is base-36 ASCII")
    }

    /// The pid of the process that minted it.
    pub fn owner_pid(&self) -> u32 {
        self.0[..PID_DIGITS].iter().fold(0, |pid, digit| {
            pid * BASE + char::from(*digit).to_digit(BASE).expect("base-36")
        })
    }
}

/// `value` in base 36 across all of `digits`, or `None` when it does not fit.
fn encode(mut value: u32, digits: &mut [u8]) -> Option<()> {
    for digit in digits.iter_mut().rev() {
        *digit = char::from_digit(value % BASE, BASE).expect("a base-36 digit") as u8;
        value /= BASE;
    }
    (value == 0).then_some(())
}

impl fmt::Display for TestNamespace {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl fmt::Debug for TestNamespace {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "TestNamespace({})", self.as_str())
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum NamespaceError {
    #[error("pid {0} does not fit a test namespace's five base-36 digits")]
    Pid(u32),
    #[error("this process has minted all {0} test namespaces two base-36 digits hold")]
    Exhausted(u32),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn production_names_are_cowsheds_own() {
        let production = HostNamespace::Production;
        assert_eq!(production.label_prefix(), "[cowshed]");
        assert_eq!(
            production.volume_label(format_args!("acme · widget — main")),
            "[cowshed] acme · widget — main"
        );
        assert_eq!(production.launchd_label("gateway"), "dev.cowshed.gateway");
        assert_eq!(
            production.macos_user_root(501),
            std::path::Path::new("/private/var/folders/cowshed/501")
        );
    }

    #[test]
    fn a_test_namespace_names_everything_apart_from_production() {
        let test = TestNamespace::of(94_769, 3).unwrap();
        assert_eq!(test.as_str(), "0214h03");
        assert_eq!(test.owner_pid(), 94_769);
        let namespace = HostNamespace::Test(test);
        assert_eq!(namespace.label_prefix(), "[cowshed-test-0214h03]");
        assert_eq!(
            namespace.launchd_label("gateway"),
            "dev.cowshed-test-0214h03.gateway"
        );
        assert_eq!(
            namespace.macos_user_root(501),
            std::path::Path::new("/private/var/folders/cowshed/501/0214h03")
        );
    }

    #[test]
    fn only_a_test_namespace_parses_as_one() {
        let minted = TestNamespace::mint().unwrap();
        assert_eq!(TestNamespace::parse(minted.as_str()), Some(minted));
        assert_eq!(minted.owner_pid(), std::process::id());
        for production in [
            "mnt",
            "t",
            "ram3",
            "ram3-new",
            "ram123",
            "ram3.legacy-leftovers-20261024T2330",
            "a234t03",
            "0234T03",
            "0234t0",
        ] {
            assert_eq!(TestNamespace::parse(production), None, "{production}");
        }
    }

    #[test]
    fn a_namespace_refuses_what_does_not_fit() {
        assert_eq!(
            TestNamespace::of(BASE.pow(5), 0),
            Err(NamespaceError::Pid(BASE.pow(5)))
        );
        assert_eq!(
            TestNamespace::of(1, BASE.pow(2)),
            Err(NamespaceError::Exhausted(BASE.pow(2)))
        );
    }
}
