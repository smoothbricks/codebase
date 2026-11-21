//! The profile a Linux launch reads: what `compile` decided, as the launcher enforces it.
//!
//! It is JSON, one object, written once per workspace authority into the store's profile
//! directory like a Seatbelt profile is. It names no build volume: the one path that moves under
//! a workspace is bound per launch (`-D BUILD_VOLUME=<path>`), so a pivot launches the same file.
//!
//! The format is private to a build and has no version: a profile names the build that wrote it
//! ([`BUILD`]) and a launcher enforces only its own.

use std::fmt;

use serde::{Deserialize, Serialize};

/// The build of the Linux sandbox this code is: a digest of its sources, the code that writes a
/// profile and the code that enforces one. A writer and a launcher are one image only in a
/// binary that is its own launcher; a program that names a `cowshed-sandbox-exec` may name one
/// built before its last change, and a profile file outlives the build that wrote it. So every
/// profile carries its writer's build and a launcher refuses any other, by name, before it
/// enforces anything: what a field means is never guessed across builds.
///
/// The sources are `sandbox/linux.rs` and every file of `sandbox/linux` ([`SOURCES`]).
pub(super) const BUILD: Build = Build(digest(SOURCES));

/// A source of the Linux sandbox by its path from this file, and its text.
macro_rules! sources {
    ($($path:literal),* $(,)?) => {
        &[$(($path, include_bytes!($path).as_slice())),*]
    };
}

/// What [`BUILD`] digests. A file this list missed would let two builds that differ in it pass
/// for one, so `every_source_of_the_linux_sandbox_is_in_its_build` holds it to the directory.
const SOURCES: &[(&str, &[u8])] = sources![
    "../linux.rs",
    "connector.rs",
    "escape_tests.rs",
    "failure.rs",
    "landlock.rs",
    "launcher.rs",
    "mounts.rs",
    "profile.rs",
];

/// FNV-1a over the text of `sources` in order, each closed by its length so no two lists of
/// files share a stream. It tells builds apart by accident of editing, not against an
/// adversary: the profile directory is the controller's, and no sandboxed process writes there.
const fn digest(sources: &[(&str, &[u8])]) -> u64 {
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    let mut source = 0;
    while source < sources.len() {
        let bytes = sources[source].1;
        let mut at = 0;
        while at < bytes.len() {
            hash = (hash ^ bytes[at] as u64).wrapping_mul(PRIME);
            at += 1;
        }
        hash = (hash ^ bytes.len() as u64).wrapping_mul(PRIME);
        source += 1;
    }
    hash
}

/// A build of the Linux sandbox ([`BUILD`]), spelled as sixteen hexadecimal digits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Build(u64);

impl fmt::Display for Build {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:016x}", self.0)
    }
}

impl Serialize for Build {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Build {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = <&str>::deserialize(deserializer)?;
        if text.len() != 16 {
            return Err(serde::de::Error::custom("a build is sixteen hex digits"));
        }
        u64::from_str_radix(text, 16)
            .map(Self)
            .map_err(serde::de::Error::custom)
    }
}

/// What a rule admits beneath its path. Landlock grants are unions: a path two rules cover holds
/// both rules' access.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(super) enum Access {
    /// Read files and list directories; start programs too where exec follows read
    /// ([`ExecAuthority::FollowsRead`]).
    Read,
    /// [`Self::Read`], and write, truncate, create, remove, rename and link beneath the path.
    ReadWrite,
    /// Open what already exists there for reading and writing, and nothing else: no listing, no
    /// create, no remove, no exec. The jobserver fifos of a CPU pool directory.
    Files,
    /// Read, write and control a device node, and list a directory of them.
    Device,
}

/// How far a rule reaches from its path.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(super) enum Scope {
    /// The path and everything beneath it.
    Subtree,
    /// The path alone, when it is not a directory. Landlock has no rule for one directory
    /// without its contents, and lookup needs none, so a literal on a directory admits nothing.
    Literal,
}

/// One Landlock rule. A path that does not exist at launch has no rule: Landlock ties a rule to
/// an inode, never to a spelling.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Rule {
    pub(super) path: String,
    pub(super) scope: Scope,
    pub(super) access: Access,
}

/// Which programs a launch may start.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(super) enum ExecAuthority {
    /// A program starts exactly where it is read.
    FollowsRead,
    /// Only the launched program, its ELF interpreter and the `libexec/git-core` beside it
    /// start: controller Git that reads what its workspace's jobs cannot runs no program a
    /// repository's configuration names.
    LaunchedProgram,
}

/// Everything one launch enforces.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Profile {
    /// The build that wrote this profile: the only one whose launcher enforces it.
    pub(super) build: Build,
    pub(super) exec: ExecAuthority,
    /// Sorted and free of duplicates, so one authority renders one text.
    pub(super) rules: Vec<Rule>,
    /// Whether the launch binds a build volume, read-write as a subtree.
    pub(super) build_volume: bool,
    /// Directories hidden behind an empty read-only tmpfs in the launch's mount namespace: what
    /// lies there is controller sockets no job may connect to. Landlock at ABI 7 does not decide
    /// `connect(2)` on a pathname socket, so hiding the name is what closes it. Every rule path
    /// and socket beneath a mask is bound back into it.
    pub(super) masks: Vec<String>,
    /// The controller-selected sockets the launch connects to; kept reachable beneath a mask.
    pub(super) sockets: Vec<String>,
    /// The workspace's gateway socket, the loopback connector's one upstream. `None` runs no
    /// connector: the launch has loopback and nothing else.
    pub(super) gateway: Option<String>,
}

/// Why a profile file is not one this launcher enforces.
#[derive(Debug, Eq, PartialEq)]
pub(super) enum ProfileError {
    Malformed(String),
    /// Another build wrote it.
    Build {
        found: Build,
    },
}

impl fmt::Display for ProfileError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed(cause) => write!(formatter, "it is not a sandbox profile: {cause}"),
            Self::Build { found } => write!(
                formatter,
                "sandbox build {found} wrote it and this launcher is sandbox build {BUILD}: a launcher enforces only its own build's profiles, so launch through the launcher built with the program that wrote this one"
            ),
        }
    }
}

impl Profile {
    pub(super) fn to_text(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    pub(super) fn parse(text: &str) -> Result<Self, ProfileError> {
        /// The writer alone, so another build's profile is named as one whatever else it holds.
        #[derive(Deserialize)]
        struct Written {
            build: Build,
        }
        let Written { build } = serde_json::from_str(text)
            .map_err(|error| ProfileError::Malformed(error.to_string()))?;
        if build != BUILD {
            return Err(ProfileError::Build { found: build });
        }
        serde_json::from_str(text).map_err(|error| ProfileError::Malformed(error.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn profile() -> Profile {
        Profile {
            build: BUILD,
            exec: ExecAuthority::FollowsRead,
            rules: vec![
                Rule {
                    path: "/dev/null".into(),
                    scope: Scope::Literal,
                    access: Access::Device,
                },
                Rule {
                    path: "/work".into(),
                    scope: Scope::Subtree,
                    access: Access::ReadWrite,
                },
            ],
            build_volume: true,
            masks: vec!["/store/run".into()],
            sockets: vec!["/nix/var/nix/daemon-socket/socket".into()],
            gateway: Some("/store/run/gateway/0123.sock".into()),
        }
    }

    #[test]
    fn a_profile_reads_back_as_it_was_written() {
        let text = profile().to_text().unwrap();
        assert_eq!(Profile::parse(&text).unwrap(), profile());
        assert_eq!(
            text,
            profile().to_text().unwrap(),
            "one profile is one text"
        );
        assert!(
            text.contains(r#"{"path":"/work","scope":"subtree","access":"read-write"}"#),
            "{text}"
        );
        assert!(
            text.starts_with(&format!(r#"{{"build":"{BUILD}","#)),
            "the writer comes first: {text}"
        );
    }

    #[test]
    fn the_build_is_a_digest_of_the_sources_it_is_given() {
        // FNV-1a's published vectors: the empty input, then "a", each closed by its length.
        assert_eq!(digest(&[]), 0xcbf2_9ce4_8422_2325);
        let a = (0xaf63_dc4c_8601_ec8c_u64 ^ 1).wrapping_mul(0x0000_0100_0000_01b3);
        assert_eq!(digest(&[("one", b"a")]), a);
        assert_eq!(digest(&[("another name", b"a")]), a, "text, not names");
        assert_ne!(
            digest(&[("", b"ab"), ("", b"c")]),
            digest(&[("", b"a"), ("", b"bc")])
        );
        assert_ne!(digest(&[("", b"a"), ("", b"")]), digest(&[("", b"a")]));
        assert_eq!(BUILD.to_string().len(), 16);
        assert_eq!(
            serde_json::from_str::<Build>(&serde_json::to_string(&BUILD).unwrap()).unwrap(),
            BUILD
        );
    }

    /// The build digests `sandbox/linux.rs` and every file beside this one: a source added to
    /// the directory and left out of [`SOURCES`] would change what a launcher does without
    /// changing its build.
    #[test]
    fn every_source_of_the_linux_sandbox_is_in_its_build() {
        let directory = std::path::PathBuf::from(
            std::env::var_os("CARGO_MANIFEST_DIR")
                .expect("cargo and nextest export CARGO_MANIFEST_DIR to the test process"),
        )
        .join("src/sandbox/linux");
        let mut present: Vec<String> = std::fs::read_dir(&directory)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        present.push("../linux.rs".to_owned());
        present.sort();
        let mut listed: Vec<&str> = SOURCES.iter().map(|(path, _)| *path).collect();
        listed.sort_unstable();
        assert_eq!(listed, present, "{}", directory.display());
        for (path, text) in SOURCES {
            assert_eq!(
                std::fs::read(directory.join(path)).unwrap(),
                *text,
                "{path} is the file it names"
            );
        }
    }

    /// A profile another build wrote is refused as that build's, whatever else it holds: its
    /// fields are never read as this build would have meant them.
    #[test]
    fn another_builds_profile_and_a_malformed_file_are_refused_by_name() {
        let other = Build(BUILD.0 ^ 1);
        let mut theirs = profile();
        theirs.build = other;
        assert_eq!(
            Profile::parse(&theirs.to_text().unwrap()),
            Err(ProfileError::Build { found: other })
        );
        assert_eq!(
            Profile::parse(&format!(r#"{{"build":"{other}","fields":"of its own"}}"#)),
            Err(ProfileError::Build { found: other }),
            "a format this build has never seen is still another build's"
        );
        let refusal = ProfileError::Build { found: other }.to_string();
        assert!(
            refusal.contains(&other.to_string()) && refusal.contains(&BUILD.to_string()),
            "{refusal}"
        );
        let own = format!(
            r#"{{"build":"{BUILD}","exec":"follows-read","rules":[],"build_volume":false,"masks":[],"sockets":[],"gateway":null"#
        );
        for malformed in [
            String::new(),
            "(version 1)".to_owned(),
            r#"{"version":1}"#.to_owned(),
            r#"{"build":"0123"}"#.to_owned(),
            r#"{"build":7}"#.to_owned(),
            format!(r#"{{"build":"{BUILD}"}}"#),
            format!(r#"{own},"extra":1}}"#),
        ] {
            assert!(
                matches!(Profile::parse(&malformed), Err(ProfileError::Malformed(_))),
                "{malformed}"
            );
        }
        assert!(Profile::parse(&format!("{own}}}")).is_ok());
    }
}
