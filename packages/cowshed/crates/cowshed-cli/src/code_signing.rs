//! The gateway host's code-signing identity (05_gateway.md "The gateway host").
//!
//! macOS charges its first-launch malware scan to a run's responsible process, and a Developer
//! Tools entry exempts one. tccd keys that entry to the program's resolved path and its
//! designated requirement. An ad-hoc signature's requirement is its cdhash, new with every build,
//! so cowshed signs the gateway host with an identity of its own: a self-signed P-256
//! code-signing certificate, made once per user in cowshed's own keychain,
//! `~/Library/Keychains/cowshed-signing.keychain`, which needs no administrator and raises no
//! prompt. The requirement names that certificate and a fixed identifier, so every build signed
//! with it satisfies the one entry.
//!
//! Not the login keychain: a key imported there carries `security`'s partition, and `codesign`
//! signing with it raises a keychain dialog (observed on macOS 26.6), and setting the key's
//! partition list needs the login password. cowshed's keychain has a password cowshed knows, the
//! empty one, so the partition list is set without asking anyone. The empty password protects
//! nothing a same-user process could not already do: any of them may ask `codesign`, which the
//! key's access list names, to sign with it.
//!
//! The certificate is never trusted for code signing: trusting it needs an authorization, and
//! nothing here needs trust. `codesign` signs with an untrusted identity, and a requirement of
//! the form `certificate leaf = H"…"` is satisfied by the certificate's hash alone.

use std::ffi::{OsStr, OsString};
use std::fmt;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use cowshed_core::CowshedError;
use cowshed_os::fork_lock::{Run as _, Spawn as _};
use rcgen::{
    CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose, PKCS_ECDSA_P256_SHA256,
};
use zeroize::Zeroizing;

use crate::launchd::CommandStatus;

pub const SECURITY: &str = "/usr/bin/security";
pub const CODESIGN: &str = "/usr/bin/codesign";

/// The common name of this user's cowshed code-signing certificate: how setup finds it again.
pub const IDENTITY_NAME: &str = "cowshed gateway host";

/// The signing identifier of the gateway host, which its designated requirement names.
pub const GATEWAY_HOST_IDENTIFIER: &str = "dev.cowshed.gateway-host";

/// The SHA-1 of a certificate's DER bytes: how `security` lists an identity, `codesign --sign`
/// selects one, and a designated requirement pins one.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct CertificateHash([u8; 20]);

impl CertificateHash {
    /// `hex` when it is 40 hexadecimal digits, in either case.
    pub fn parse(hex: &str) -> Option<Self> {
        if hex.len() != 40 {
            return None;
        }
        let mut bytes = [0u8; 20];
        for (byte, pair) in bytes.iter_mut().zip(hex.as_bytes().as_chunks::<2>().0) {
            *byte = (nibble(pair[0])? << 4) | nibble(pair[1])?;
        }
        Some(Self(bytes))
    }
}

fn nibble(digit: u8) -> Option<u8> {
    match digit {
        b'0'..=b'9' => Some(digit - b'0'),
        b'a'..=b'f' => Some(digit - b'a' + 10),
        b'A'..=b'F' => Some(digit - b'A' + 10),
        _ => None,
    }
}

impl fmt::Display for CertificateHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// This user's cowshed code-signing identity: the certificate whose key signs the gateway host.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SigningIdentity {
    pub certificate: CertificateHash,
}

impl SigningIdentity {
    /// The designated requirement every gateway host signed with this identity carries: the
    /// fixed identifier and the certificate, and nothing that changes with the build.
    pub fn designated_requirement(&self) -> String {
        format!(
            "identifier \"{GATEWAY_HOST_IDENTIFIER}\" and certificate leaf = H\"{}\"",
            self.certificate
        )
    }

    /// `codesign` arguments that sign `path` in place as the gateway host, replacing the
    /// linker's ad-hoc signature. No timestamp: a secure timestamp asks Apple's server, and the
    /// requirement does not depend on one.
    pub fn sign_arguments(&self, path: &Path) -> Vec<OsString> {
        vec![
            "--force".into(),
            "--sign".into(),
            self.certificate.to_string().into(),
            "--identifier".into(),
            GATEWAY_HOST_IDENTIFIER.into(),
            "--requirements".into(),
            format!("=designated => {}", self.designated_requirement()).into(),
            "--timestamp=none".into(),
            path.into(),
        ]
    }

    /// `codesign` arguments that check `path` is intact and satisfies
    /// [`Self::designated_requirement`].
    pub fn verify_arguments(&self, path: &Path) -> Vec<OsString> {
        vec![
            "--verify".into(),
            "--strict".into(),
            "-R".into(),
            format!("={}", self.designated_requirement()).into(),
            path.into(),
        ]
    }
}

/// What `codesign --verify` said of a file: `Ok(true)` when it satisfies the requirement,
/// `Ok(false)` when it is unsigned, signed otherwise, or altered (codesign's exit 1 and 3), and
/// `Err` with codesign's own words for anything else.
pub fn verification(status: CommandStatus, stderr: &[u8]) -> Result<bool, String> {
    match status {
        CommandStatus::Success => Ok(true),
        CommandStatus::ExitCode(1 | 3) => Ok(false),
        CommandStatus::ExitCode(code) => Err(format!(
            "codesign --verify exited {code}: {}",
            String::from_utf8_lossy(stderr).trim()
        )),
        CommandStatus::Terminated => Err(format!(
            "codesign --verify was ended by a signal: {}",
            String::from_utf8_lossy(stderr).trim()
        )),
    }
}

/// The identities named exactly `name` in `security find-identity` output. Only the "Matching
/// identities" section counts: the certificate is never trusted, so it is never among the
/// "Valid identities only" that follow.
pub fn identities_named(listing: &str, name: &str) -> Vec<CertificateHash> {
    let quoted = format!("\"{name}\"");
    let mut found = Vec::new();
    for line in listing.lines().map(str::trim) {
        if line == "Valid identities only" {
            break;
        }
        let Some((ordinal, rest)) = line.split_once(") ") else {
            continue;
        };
        if ordinal.is_empty() || !ordinal.bytes().all(|digit| digit.is_ascii_digit()) {
            continue;
        }
        let Some((hex, described)) = rest.split_once(' ') else {
            continue;
        };
        let Some(after) = described.strip_prefix(quoted.as_str()) else {
            continue;
        };
        if !(after.is_empty() || after.starts_with(" (")) {
            continue;
        }
        if let Some(hash) = CertificateHash::parse(hex)
            && !found.contains(&hash)
        {
            found.push(hash);
        }
    }
    found
}

/// A keychain file `security` reads and writes, and the password that unlocks it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Keychain {
    path: PathBuf,
    password: &'static str,
}

/// cowshed's signing keychain under `~/Library/Keychains`, named as `security` was given it at
/// creation (the file on disk gains `-db`): the search-list entry must say the same, since
/// `codesign` finds no identity through the `-db` spelling of a keychain created this way.
pub const KEYCHAIN_NAME: &str = "cowshed-signing.keychain";

impl Keychain {
    pub fn at(path: PathBuf, password: &'static str) -> Self {
        Self { path, password }
    }

    /// cowshed's signing keychain in `home`: [`KEYCHAIN_NAME`], with the empty password.
    pub fn cowshed(home: &Path) -> Self {
        Self::at(home.join("Library/Keychains").join(KEYCHAIN_NAME), "")
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Make `keychain` ready for `codesign`: created when absent (never locking itself by time or
/// sleep), unlocked, and on the user's keychain search list, the only place `codesign --sign`
/// looks for an identity. Nothing here prompts: every step is given the password.
pub fn prepare(keychain: &Keychain) -> Result<(), CowshedError> {
    let path = keychain.path().as_os_str();
    let password = OsStr::new(keychain.password);
    if !keychain.path().with_extension("keychain-db").exists() {
        security(
            &[
                OsStr::new("create-keychain"),
                OsStr::new("-p"),
                password,
                path,
            ],
            None,
        )?;
        security(&[OsStr::new("set-keychain-settings"), path], None)?;
    }
    security(
        &[
            OsStr::new("unlock-keychain"),
            OsStr::new("-p"),
            password,
            path,
        ],
        None,
    )?;
    let listed = security(
        &[
            OsStr::new("list-keychains"),
            OsStr::new("-d"),
            OsStr::new("user"),
        ],
        None,
    )?;
    if let Some(list) = search_list_with(&String::from_utf8_lossy(&listed.stdout), keychain.path())
    {
        let mut arguments = vec![
            OsStr::new("list-keychains"),
            OsStr::new("-d"),
            OsStr::new("user"),
            OsStr::new("-s"),
        ];
        arguments.extend(list.iter().map(|entry| entry.as_os_str()));
        security(&arguments, None)?;
    }
    Ok(())
}

/// The user search list `security list-keychains -d user` printed, with `keychain` appended:
/// `None` when it is already there. Every entry already listed is kept, in its order.
pub fn search_list_with(listing: &str, keychain: &Path) -> Option<Vec<PathBuf>> {
    let mut entries: Vec<PathBuf> = listing
        .lines()
        .filter_map(|line| {
            line.trim()
                .strip_prefix('"')?
                .strip_suffix('"')
                .map(PathBuf::from)
        })
        .collect();
    // security may list the entry by the file it resolved to, with `-db` appended.
    let file = keychain.with_extension("keychain-db");
    if entries
        .iter()
        .any(|entry| entry == keychain || *entry == file)
    {
        return None;
    }
    entries.push(keychain.to_path_buf());
    Some(entries)
}

/// This user's cowshed signing identity in `keychain`, made there first when it holds none.
///
/// Making one asks for no authorization: the key and its certificate are imported into a
/// keychain the user owns, the key's access list naming `codesign`, so signing with it raises
/// no prompt either. Two identities of that name are refused by name rather than chosen
/// between: the Developer Tools entry names exactly one.
pub fn ensure_identity(keychain: &Keychain) -> Result<SigningIdentity, CowshedError> {
    match find_identities(keychain)?.as_slice() {
        [certificate] => {
            return Ok(SigningIdentity {
                certificate: *certificate,
            });
        }
        [] => {}
        many => return Err(ambiguous(keychain, many)),
    }
    create_identity(keychain)?;
    match find_identities(keychain)?.as_slice() {
        [certificate] => Ok(SigningIdentity {
            certificate: *certificate,
        }),
        [] => Err(CowshedError::environment_missing(
            format!(
                "imported the \"{IDENTITY_NAME}\" key and certificate into {}, but security lists no code-signing identity of that name",
                keychain.path().display()
            ),
            "security find-identity -p codesigning",
        )),
        many => Err(ambiguous(keychain, many)),
    }
}

fn ambiguous(keychain: &Keychain, many: &[CertificateHash]) -> CowshedError {
    let hashes: Vec<String> = many.iter().map(ToString::to_string).collect();
    CowshedError::integrity(
        format!(
            "{} holds {} code-signing identities named \"{IDENTITY_NAME}\" ({}); the gateway host is signed with exactly one",
            keychain.path().display(),
            many.len(),
            hashes.join(", ")
        ),
        format!(
            "keep the one the Developer Tools entry was made with and delete the others: security delete-identity -Z <hash> {}",
            keychain.path().display()
        ),
    )
}

/// The code-signing identities named [`IDENTITY_NAME`] in `keychain`.
pub fn find_identities(keychain: &Keychain) -> Result<Vec<CertificateHash>, CowshedError> {
    let output = security(
        &[
            OsStr::new("find-identity"),
            OsStr::new("-p"),
            OsStr::new("codesigning"),
            keychain.path().as_os_str(),
        ],
        None,
    )?;
    Ok(identities_named(
        &String::from_utf8_lossy(&output.stdout),
        IDENTITY_NAME,
    ))
}

/// Mint a P-256 key and a self-signed code-signing certificate named [`IDENTITY_NAME`], and
/// import both into `keychain`. The key reaches `security` on a pipe, never a file.
pub fn create_identity(keychain: &Keychain) -> Result<(), CowshedError> {
    let minted = mint().map_err(|reason| {
        CowshedError::internal(format!(
            "could not mint the cowshed signing identity: {reason}"
        ))
    })?;
    let destination = keychain.path().as_os_str();
    security(
        &[
            OsStr::new("import"),
            OsStr::new("/dev/stdin"),
            OsStr::new("-k"),
            destination,
            OsStr::new("-t"),
            OsStr::new("priv"),
            OsStr::new("-f"),
            OsStr::new("openssl"),
            OsStr::new("-T"),
            OsStr::new(CODESIGN),
        ],
        Some(minted.key_pem.as_bytes()),
    )?;
    security(
        &[
            OsStr::new("import"),
            OsStr::new("/dev/stdin"),
            OsStr::new("-k"),
            destination,
            OsStr::new("-t"),
            OsStr::new("cert"),
            OsStr::new("-f"),
            OsStr::new("pemseq"),
        ],
        Some(minted.certificate_pem.as_bytes()),
    )?;
    // The key is usable by `codesign` without a dialog only once its partition list names it;
    // the password is the keychain's own, so this asks no one either.
    security(
        &[
            OsStr::new("set-key-partition-list"),
            OsStr::new("-S"),
            OsStr::new("apple-tool:,apple:,codesign:"),
            OsStr::new("-s"),
            OsStr::new("-k"),
            OsStr::new(keychain.password),
            destination,
        ],
        None,
    )?;
    Ok(())
}

/// A freshly minted identity, in the PEM forms `security import` reads: the key as SEC1
/// (`EC PRIVATE KEY`, which `security` takes as `-f openssl`; it refuses PKCS#8) and the
/// certificate.
pub struct MintedIdentity {
    pub key_pem: Zeroizing<String>,
    pub certificate_pem: String,
}

/// A P-256 key and a self-signed certificate for it, named [`IDENTITY_NAME`], for code signing
/// only: an end entity (basicConstraints CA:FALSE), keyUsage digitalSignature, and extended key
/// usage codeSigning, which `security find-identity -p codesigning` selects on.
pub fn mint() -> Result<MintedIdentity, String> {
    let key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256)
        .map_err(|error| format!("P-256 key generation: {error}"))?;
    let mut params = CertificateParams::new(Vec::<String>::new())
        .map_err(|error| format!("certificate parameters: {error}"))?;
    let mut name = DistinguishedName::new();
    name.push(DnType::CommonName, IDENTITY_NAME);
    params.distinguished_name = name;
    params.is_ca = IsCa::ExplicitNoCa;
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::CodeSigning];
    let certificate = params
        .self_signed(&key)
        .map_err(|error| format!("self-signed certificate: {error}"))?;
    let pkcs8 = Zeroizing::new(key.serialize_der());
    let scalar = p256_scalar(&pkcs8).ok_or("the P-256 key's PKCS#8 holds no 32-byte scalar")?;
    let public: &[u8; 65] = key
        .public_key_raw()
        .try_into()
        .map_err(|_| "the P-256 public key is not a 65-byte uncompressed point")?;
    Ok(MintedIdentity {
        key_pem: pem("EC PRIVATE KEY", &sec1_p256(&scalar, public)),
        certificate_pem: certificate.pem(),
    })
}

/// The 32-byte private scalar of a P-256 key in PKCS#8: `PrivateKeyInfo { version,
/// algorithm, privateKey OCTET STRING (ECPrivateKey { version 1, privateKey OCTET STRING, … }) }`
/// (RFC 5208, RFC 5915).
pub fn p256_scalar(pkcs8: &[u8]) -> Option<Zeroizing<[u8; 32]>> {
    let (0x30, info, _) = tlv(pkcs8)? else {
        return None;
    };
    let (0x02, _, info) = tlv(info)? else {
        return None;
    };
    let (0x30, _, info) = tlv(info)? else {
        return None;
    };
    let (0x04, wrapped, _) = tlv(info)? else {
        return None;
    };
    let (0x30, ec, _) = tlv(wrapped)? else {
        return None;
    };
    let (0x02, [1], ec) = tlv(ec)? else {
        return None;
    };
    let (0x04, scalar, _) = tlv(ec)? else {
        return None;
    };
    let mut out = Zeroizing::new([0u8; 32]);
    out.copy_from_slice(<&[u8; 32]>::try_from(scalar).ok()?);
    Some(out)
}

/// One DER tag-length-value: its tag, its contents and what follows it. Definite lengths of up
/// to two bytes, which is all a P-256 key uses.
fn tlv(input: &[u8]) -> Option<(u8, &[u8], &[u8])> {
    let (&tag, rest) = input.split_first()?;
    let (&first, rest) = rest.split_first()?;
    let (length, rest) = match first {
        0..=0x7f => (usize::from(first), rest),
        0x81 => {
            let (&length, rest) = rest.split_first()?;
            (usize::from(length), rest)
        }
        0x82 => {
            let (bytes, rest) = rest.split_at_checked(2)?;
            (usize::from(u16::from_be_bytes([bytes[0], bytes[1]])), rest)
        }
        _ => return None,
    };
    let (contents, rest) = rest.split_at_checked(length)?;
    Some((tag, contents, rest))
}

/// A P-256 key as SEC1 DER (RFC 5915): `ECPrivateKey { version 1, privateKey, [0] prime256v1,
/// [1] publicKey }`. `security import` refuses one without the curve parameters.
pub fn sec1_p256(scalar: &[u8; 32], public: &[u8; 65]) -> Zeroizing<Vec<u8>> {
    const PRIME256V1: [u8; 10] = [0x06, 0x08, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x03, 0x01, 0x07];
    let mut der = Zeroizing::new(Vec::with_capacity(121));
    der.extend_from_slice(&[0x30, 0x77, 0x02, 0x01, 0x01, 0x04, 0x20]);
    der.extend_from_slice(scalar);
    der.extend_from_slice(&[0xa0, 0x0a]);
    der.extend_from_slice(&PRIME256V1);
    der.extend_from_slice(&[0xa1, 0x44, 0x03, 0x42, 0x00]);
    der.extend_from_slice(public);
    der
}

fn pem(label: &str, der: &[u8]) -> Zeroizing<String> {
    let encoded = Zeroizing::new(STANDARD.encode(der));
    let mut text = Zeroizing::new(String::with_capacity(encoded.len() + 80));
    text.push_str("-----BEGIN ");
    text.push_str(label);
    text.push_str("-----\n");
    for line in encoded.as_bytes().chunks(64) {
        text.push_str(std::str::from_utf8(line).expect("base64 is ASCII"));
        text.push('\n');
    }
    text.push_str("-----END ");
    text.push_str(label);
    text.push_str("-----\n");
    text
}

/// Run `security` with `arguments`, `stdin` written to it when given, and require it to
/// succeed: its output, or an error naming the command and what it said.
// WHY unbounded: `security` runs for `cowshed setup` and `cowshed install`, an operator's verbs;
// the operator's interrupt ends it.
#[allow(clippy::disallowed_methods)]
fn security(arguments: &[&OsStr], stdin: Option<&[u8]>) -> Result<Output, CowshedError> {
    let described = || {
        arguments
            .iter()
            .map(|argument| argument.to_string_lossy().into_owned())
            .collect::<Vec<_>>()
            .join(" ")
    };
    let mut command = Command::new(SECURITY);
    command.args(arguments);
    let output = match stdin {
        None => command.output_locked(),
        Some(bytes) => command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn_locked()
            .and_then(|mut child| {
                let written = child
                    .stdin
                    .take()
                    .expect("stdin was piped")
                    .write_all(bytes);
                let output = child.wait_with_output()?;
                written.map(|()| output)
            }),
    }
    .map_err(|error| {
        CowshedError::environment_missing(
            format!("could not run {SECURITY} {}: {error}", described()),
            "cowshed setup runs on macOS, where /usr/bin/security is part of the system",
        )
    })?;
    if output.status.success() {
        return Ok(output);
    }
    Err(CowshedError::environment_missing(
        format!(
            "{SECURITY} {} failed ({}): {}",
            described(),
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ),
        "rerun cowshed setup; the error is what security said",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(target_os = "macos")]
    use cowshed_os::test_backstop::CommandBackstop as _;

    /// `security find-identity -p codesigning <keychain>` on macOS 26.6, with this identity,
    /// another one and a valid identity after the matching section.
    const LISTING: &str = "\nPolicy: Code Signing\n  Matching identities\n  1) 8EADAF1CDF363F71ED26D32D033C4CF4ED65010C \"cowshed gateway host\" (CSSMERR_TP_NOT_TRUSTED)\n  2) 0123456789ABCDEF0123456789ABCDEF01234567 \"cowshed gateway host (old)\" (CSSMERR_TP_NOT_TRUSTED)\n  3) 89ABCDEF0123456789ABCDEF0123456789ABCDEF \"Developer ID Application: Someone\"\n     3 identities found\n\n  Valid identities only\n  1) FEDCBA9876543210FEDCBA9876543210FEDCBA98 \"cowshed gateway host\"\n     1 valid identities found\n";

    #[test]
    fn only_the_exactly_named_matching_identity_is_found() {
        assert_eq!(
            identities_named(LISTING, IDENTITY_NAME),
            [CertificateHash::parse("8eadaf1cdf363f71ed26d32d033c4cf4ed65010c").unwrap()]
        );
        assert_eq!(
            identities_named(LISTING, "Developer ID Application: Someone"),
            [CertificateHash::parse("89ABCDEF0123456789ABCDEF0123456789ABCDEF").unwrap()]
        );
        assert!(
            identities_named(
                "\nPolicy: Code Signing\n  Matching identities\n     0 identities found\n",
                IDENTITY_NAME
            )
            .is_empty()
        );
    }

    #[test]
    fn a_certificate_hash_is_forty_hex_digits_spelt_lowercase() {
        let hash = CertificateHash::parse("8EADAF1CDF363F71ED26D32D033C4CF4ED65010C").unwrap();
        assert_eq!(hash.to_string(), "8eadaf1cdf363f71ed26d32d033c4cf4ed65010c");
        assert_eq!(CertificateHash::parse(&hash.to_string()), Some(hash));
        for bad in [
            "",
            "8EAD",
            "8EADAF1CDF363F71ED26D32D033C4CF4ED65010",
            "ZEADAF1CDF363F71ED26D32D033C4CF4ED65010C",
        ] {
            assert_eq!(CertificateHash::parse(bad), None, "{bad}");
        }
    }

    /// The requirement is the one string a Developer Tools entry keeps: it must name the fixed
    /// identifier and the certificate and nothing a build changes, and sign and verify must use
    /// the same one.
    #[test]
    fn the_designated_requirement_names_the_identifier_and_the_certificate_only() {
        let identity = SigningIdentity {
            certificate: CertificateHash::parse("8eadaf1cdf363f71ed26d32d033c4cf4ed65010c")
                .unwrap(),
        };
        let requirement = identity.designated_requirement();
        assert_eq!(
            requirement,
            "identifier \"dev.cowshed.gateway-host\" and certificate leaf = H\"8eadaf1cdf363f71ed26d32d033c4cf4ed65010c\""
        );
        let path = Path::new("/home/bin/.cowshed-gateway-host.1.0");
        let sign = identity.sign_arguments(path);
        assert_eq!(
            sign,
            [
                "--force",
                "--sign",
                "8eadaf1cdf363f71ed26d32d033c4cf4ed65010c",
                "--identifier",
                "dev.cowshed.gateway-host",
                "--requirements",
                &format!("=designated => {requirement}"),
                "--timestamp=none",
                "/home/bin/.cowshed-gateway-host.1.0",
            ]
            .map(OsString::from)
        );
        assert_eq!(
            identity.verify_arguments(path),
            [
                "--verify",
                "--strict",
                "-R",
                &format!("={requirement}"),
                "/home/bin/.cowshed-gateway-host.1.0"
            ]
            .map(OsString::from)
        );
    }

    #[test]
    fn the_signing_keychain_is_appended_to_the_search_list_once() {
        let listing = "    \"/Users/u/Library/Keychains/login.keychain-db\"\n    \"/Users/u/Library/Keychains/other.keychain-db\"\n";
        let ours = Path::new("/Users/u/Library/Keychains/cowshed-signing.keychain");
        assert_eq!(
            search_list_with(listing, ours),
            Some(vec![
                PathBuf::from("/Users/u/Library/Keychains/login.keychain-db"),
                PathBuf::from("/Users/u/Library/Keychains/other.keychain-db"),
                ours.to_path_buf(),
            ])
        );
        let with = format!("{listing}    \"{}\"\n", ours.display());
        assert_eq!(search_list_with(&with, ours), None);
        let resolved = format!("{listing}    \"{}-db\"\n", ours.display());
        assert_eq!(search_list_with(&resolved, ours), None);
        assert_eq!(Keychain::cowshed(Path::new("/Users/u")).path(), ours);
    }
    #[test]
    fn verification_reads_codesign_exit_statuses() {
        assert_eq!(verification(CommandStatus::Success, b""), Ok(true));
        assert_eq!(
            verification(CommandStatus::ExitCode(3), b"failed to satisfy"),
            Ok(false)
        );
        assert_eq!(
            verification(CommandStatus::ExitCode(1), b"not signed at all"),
            Ok(false)
        );
        assert!(verification(CommandStatus::ExitCode(2), b"bad option").is_err());
        assert!(verification(CommandStatus::Terminated, b"").is_err());
    }

    /// The SEC1 key carries the PKCS#8 key's own scalar and public point, under the curve
    /// `security` needs named: decoding it back yields both.
    #[test]
    fn a_minted_key_is_sec1_with_its_curve_and_public_point() {
        let key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).unwrap();
        let scalar = p256_scalar(&key.serialize_der()).expect("ring's PKCS#8 holds the scalar");
        let public: &[u8; 65] = key.public_key_raw().try_into().unwrap();
        let der = sec1_p256(&scalar, public);
        let (0x30, body, rest) = tlv(&der).unwrap() else {
            panic!("not a SEQUENCE")
        };
        assert!(rest.is_empty());
        let (0x02, [1], body) = tlv(body).unwrap() else {
            panic!("not version 1")
        };
        let (0x04, decoded, body) = tlv(body).unwrap() else {
            panic!("no privateKey")
        };
        assert_eq!(decoded, &scalar[..]);
        let (0xa0, curve, body) = tlv(body).unwrap() else {
            panic!("no parameters")
        };
        assert_eq!(
            curve,
            [0x06, 0x08, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x03, 0x01, 0x07]
        );
        let (0xa1, point, body) = tlv(body).unwrap() else {
            panic!("no publicKey")
        };
        assert!(body.is_empty());
        assert_eq!(point[..3], [0x03, 0x42, 0x00]);
        assert_eq!(&point[3..], &public[..]);
        let minted = mint().unwrap();
        assert!(
            minted
                .key_pem
                .starts_with("-----BEGIN EC PRIVATE KEY-----\n")
        );
        assert!(
            minted
                .certificate_pem
                .starts_with("-----BEGIN CERTIFICATE-----")
        );
    }

    /// The real boundary: `security` takes the minted key and certificate into a scratch
    /// keychain as one code-signing identity, found again by name, and a second ensure makes no
    /// second one. The scratch keychain is never on the user's search list and is deleted on
    /// every exit path.
    #[cfg(target_os = "macos")]
    #[test]
    #[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
    fn host_controller_a_minted_identity_is_one_code_signing_identity_in_its_keychain() {
        struct Scratch(PathBuf);
        impl Drop for Scratch {
            fn drop(&mut self) {
                let _ = Command::new(SECURITY)
                    .arg("delete-keychain")
                    .arg(&self.0)
                    .output_bounded();
                let _ = std::fs::remove_dir_all(self.0.parent().expect("keychain directory"));
            }
        }
        let directory = std::env::temp_dir().join(format!(
            "cowshed-signing-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&directory).unwrap();
        let scratch = Scratch(directory.join("signing.keychain"));
        for arguments in [
            ["create-keychain", "-p", "scratch"],
            ["unlock-keychain", "-p", "scratch"],
        ] {
            security(
                &[
                    OsStr::new(arguments[0]),
                    OsStr::new(arguments[1]),
                    OsStr::new(arguments[2]),
                    scratch.0.as_os_str(),
                ],
                None,
            )
            .unwrap();
        }
        let keychain = Keychain::at(scratch.0.clone(), "scratch");
        assert!(find_identities(&keychain).unwrap().is_empty());
        let made = ensure_identity(&keychain).unwrap();
        assert_eq!(find_identities(&keychain).unwrap(), [made.certificate]);
        assert_eq!(ensure_identity(&keychain).unwrap(), made);
        assert_eq!(find_identities(&keychain).unwrap(), [made.certificate]);
    }
}
