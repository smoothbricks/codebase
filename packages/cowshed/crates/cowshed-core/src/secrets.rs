use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};
use walkdir::{DirEntry, WalkDir};

const REDACTION: &[u8] = b"[REDACTED]";

/// A controller-owned exception to the worktree secret policy.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SecretWaiver {
    pub path: PathBuf,
    pub reason: String,
}

/// A scanner match. `context` is always a redacted projection, never source bytes.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SecretFinding {
    pub path: PathBuf,
    pub rule_id: String,
    pub line: Option<usize>,
    pub context: String,
}

/// A finding suppressed by a controller-owned waiver, retained for auditing.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WaivedSecretFinding {
    pub finding: SecretFinding,
    pub reason: String,
}

/// Deterministic result of a complete tree scan.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SecretScan {
    pub findings: Vec<SecretFinding>,
    pub waived_findings: Vec<WaivedSecretFinding>,
}

/// Operational failures are returned rather than converted into an incomplete scan.
#[derive(Debug)]
pub enum SecretScanError {
    InvalidRoot {
        path: PathBuf,
        reason: &'static str,
    },
    InvalidWaiver {
        path: PathBuf,
        reason: &'static str,
    },
    DuplicateWaiver {
        path: PathBuf,
    },
    Walk {
        path: PathBuf,
        source: walkdir::Error,
    },
    Read {
        path: PathBuf,
        source: io::Error,
    },
}

impl fmt::Display for SecretScanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRoot { path, reason } => {
                write!(formatter, "cannot scan {}: {reason}", path.display())
            }
            Self::InvalidWaiver { path, reason } => {
                write!(formatter, "invalid waiver for {}: {reason}", path.display())
            }
            Self::DuplicateWaiver { path } => {
                write!(formatter, "duplicate waiver for {}", path.display())
            }
            Self::Walk { path, source } => {
                write!(formatter, "cannot walk {}: {source}", path.display())
            }
            Self::Read { path, source } => {
                write!(formatter, "cannot read {}: {source}", path.display())
            }
        }
    }
}

impl std::error::Error for SecretScanError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Walk { source, .. } => Some(source),
            Self::Read { source, .. } => Some(source),
            _ => None,
        }
    }
}

/// Scan every regular file below `root` without following symlinks.
///
/// `.git` and workspace-local build/cache roots are pruned before traversal. Waivers
/// match normalized, repository-relative paths exactly and must include a reason.
pub fn scan_tree(root: &Path, waivers: &[SecretWaiver]) -> Result<SecretScan, SecretScanError> {
    let root_metadata = fs::symlink_metadata(root).map_err(|source| SecretScanError::Read {
        path: root.to_path_buf(),
        source,
    })?;
    if root_metadata.file_type().is_symlink() {
        return Err(SecretScanError::InvalidRoot {
            path: root.to_path_buf(),
            reason: "root is a symlink",
        });
    }
    if !root_metadata.is_dir() {
        return Err(SecretScanError::InvalidRoot {
            path: root.to_path_buf(),
            reason: "root is not a directory",
        });
    }

    let waivers = validate_waivers(waivers)?;
    let mut findings = Vec::new();
    let walker = WalkDir::new(root)
        .follow_links(false)
        .sort_by_file_name()
        .into_iter()
        .filter_entry(|entry| should_visit(root, entry));

    for entry in walker {
        let entry = entry.map_err(|source| SecretScanError::Walk {
            path: source
                .path()
                .map(Path::to_path_buf)
                .unwrap_or_else(|| root.to_path_buf()),
            source,
        })?;
        if !entry.file_type().is_file() {
            continue;
        }
        let relative =
            entry
                .path()
                .strip_prefix(root)
                .map_err(|_| SecretScanError::InvalidRoot {
                    path: entry.path().to_path_buf(),
                    reason: "walker produced a path outside the scan root",
                })?;
        scan_file(entry.path(), relative, &mut findings)?;
    }

    findings.sort();
    findings.dedup();

    let mut result = SecretScan::default();
    for finding in findings {
        if let Some(reason) = waivers.get(finding.path.as_path()) {
            result.waived_findings.push(WaivedSecretFinding {
                finding,
                reason: (*reason).to_owned(),
            });
        } else {
            result.findings.push(finding);
        }
    }
    Ok(result)
}

fn validate_waivers(waivers: &[SecretWaiver]) -> Result<BTreeMap<&Path, &str>, SecretScanError> {
    let mut validated = BTreeMap::new();
    for waiver in waivers {
        if waiver.reason.trim().is_empty() {
            return Err(SecretScanError::InvalidWaiver {
                path: waiver.path.clone(),
                reason: "reason is required",
            });
        }
        if waiver.path.as_os_str().is_empty()
            || waiver.path.is_absolute()
            || waiver
                .path
                .components()
                .any(|component| !matches!(component, Component::Normal(_)))
        {
            return Err(SecretScanError::InvalidWaiver {
                path: waiver.path.clone(),
                reason: "path must be a normalized repository-relative path",
            });
        }
        if validated
            .insert(waiver.path.as_path(), waiver.reason.as_str())
            .is_some()
        {
            return Err(SecretScanError::DuplicateWaiver {
                path: waiver.path.clone(),
            });
        }
    }
    Ok(validated)
}

fn should_visit(root: &Path, entry: &DirEntry) -> bool {
    if entry.depth() == 0 {
        return true;
    }
    if entry.file_type().is_symlink() {
        return false;
    }
    let relative = entry.path().strip_prefix(root).unwrap_or(entry.path());
    if entry.file_name() == ".git" {
        return false;
    }
    if !entry.file_type().is_dir() {
        return true;
    }

    let name = entry.file_name().to_string_lossy();
    if matches!(
        name.as_ref(),
        "node_modules"
            | ".nx"
            | ".turbo"
            | ".next"
            | ".expo"
            | ".gradle"
            | ".zig-cache"
            | "zig-cache"
            | "zig-out"
            | "DerivedData"
            | "Pods"
    ) {
        return false;
    }
    if name == "target"
        && entry
            .path()
            .parent()
            .is_some_and(|parent| parent.join("Cargo.toml").is_file())
    {
        return false;
    }
    !is_cowshed_cache(relative)
}

fn is_cowshed_cache(path: &Path) -> bool {
    let mut components = path.components().peekable();
    while let Some(Component::Normal(component)) = components.next() {
        if component == ".cowshed"
            && matches!(components.peek(), Some(Component::Normal(next)) if *next == "cache")
        {
            return true;
        }
    }
    false
}

fn scan_file(
    absolute: &Path,
    relative: &Path,
    findings: &mut Vec<SecretFinding>,
) -> Result<(), SecretScanError> {
    if let Some(rule_id) = filename_rule(relative) {
        findings.push(SecretFinding {
            path: relative.to_path_buf(),
            rule_id: rule_id.to_owned(),
            line: None,
            context: "[REDACTED: sensitive filename]".to_owned(),
        });
    }

    let contents = fs::read(absolute).map_err(|source| SecretScanError::Read {
        path: absolute.to_path_buf(),
        source,
    })?;
    let file_name = relative
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("");
    let shell_file = is_shell_file(file_name, relative);

    for (index, line) in contents.split(|byte| *byte == b'\n').enumerate() {
        let mut matches = Vec::new();
        find_prefixed_token(
            line,
            b"ghp_",
            20,
            github_token_byte,
            "token.github-pat",
            &mut matches,
        );
        find_prefixed_token(
            line,
            b"xoxb-",
            10,
            slack_token_byte,
            "token.slack-bot",
            &mut matches,
        );
        find_prefixed_token(
            line,
            b"sk-",
            20,
            secret_key_byte,
            "token.secret-key",
            &mut matches,
        );
        find_aws_access_keys(line, &mut matches);
        find_pem_marker(line, &mut matches);
        find_auth_config(file_name, line, &mut matches);
        find_shell_secret(shell_file, file_name == ".envrc", line, &mut matches);

        if matches.is_empty() {
            continue;
        }
        matches.sort_by_key(|matched| (matched.start, matched.end, matched.rule_id));
        matches.dedup();
        let context = redact_line(line, &matches);
        let mut rules: Vec<&str> = matches.iter().map(|matched| matched.rule_id).collect();
        rules.sort_unstable();
        rules.dedup();
        for rule_id in rules {
            findings.push(SecretFinding {
                path: relative.to_path_buf(),
                rule_id: rule_id.to_owned(),
                line: Some(index + 1),
                context: context.clone(),
            });
        }
    }
    Ok(())
}

fn filename_rule(path: &Path) -> Option<&'static str> {
    let name = path.file_name()?.to_str()?;
    if name == ".env" || name.starts_with(".env.") {
        return Some("filename.env");
    }
    if name.ends_with(".pem") {
        return Some("filename.pem");
    }
    if name.starts_with("id_") && !name.ends_with(".pub") {
        return Some("filename.ssh-private-key");
    }
    if name == ".netrc" {
        return Some("filename.netrc");
    }
    if is_cloud_credential_path(path) {
        return Some("filename.cloud-credentials");
    }
    None
}

fn is_cloud_credential_path(path: &Path) -> bool {
    let mut previous_was_config = false;
    for component in path.components() {
        let Component::Normal(value) = component else {
            previous_was_config = false;
            continue;
        };
        let Some(value) = value.to_str() else {
            previous_was_config = false;
            continue;
        };
        if matches!(value, ".aws" | ".azure" | ".gcloud")
            || (previous_was_config && value == "gcloud")
        {
            return true;
        }
        previous_was_config = value == ".config";
    }
    false
}

fn is_shell_file(name: &str, path: &Path) -> bool {
    matches!(name, ".envrc" | ".bashrc" | ".zshrc" | ".profile")
        || path
            .extension()
            .and_then(|extension| extension.to_str())
            .is_some_and(|extension| matches!(extension, "sh" | "bash" | "zsh"))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct LineMatch {
    start: usize,
    end: usize,
    rule_id: &'static str,
}

fn find_prefixed_token(
    line: &[u8],
    prefix: &[u8],
    minimum_suffix: usize,
    allowed: fn(u8) -> bool,
    rule_id: &'static str,
    matches: &mut Vec<LineMatch>,
) {
    if prefix.is_empty() {
        return;
    }
    for start in line
        .windows(prefix.len())
        .enumerate()
        .filter_map(|(start, window)| (window == prefix).then_some(start))
    {
        let suffix_start = start
            .checked_add(prefix.len())
            .expect("prefix position is within the line");
        let suffix_len = line[suffix_start..]
            .iter()
            .take_while(|byte| allowed(**byte))
            .count();
        let end = suffix_start
            .checked_add(suffix_len)
            .expect("token suffix is within the line");
        let boundary_ok = start == 0 || !secret_key_byte(line[start - 1]);
        if boundary_ok && suffix_len >= minimum_suffix {
            matches.push(LineMatch {
                start,
                end,
                rule_id,
            });
        }
    }
}

fn find_aws_access_keys(line: &[u8], matches: &mut Vec<LineMatch>) {
    const PREFIX: &[u8] = b"AKIA";
    for start in line
        .windows(PREFIX.len())
        .enumerate()
        .filter_map(|(start, window)| (window == PREFIX).then_some(start))
    {
        let suffix_start = start
            .checked_add(PREFIX.len())
            .expect("prefix position is within the line");
        let suffix_len = line[suffix_start..]
            .iter()
            .take_while(|byte| aws_key_byte(**byte))
            .count();
        let end = suffix_start
            .checked_add(suffix_len)
            .expect("access key is within the line");
        let boundary_ok = start == 0 || !aws_key_byte(line[start - 1]);
        if boundary_ok && suffix_len == 16 {
            matches.push(LineMatch {
                start,
                end,
                rule_id: "token.aws-access-key",
            });
        }
    }
}

fn find_pem_marker(line: &[u8], matches: &mut Vec<LineMatch>) {
    const BEGIN: &[u8] = b"-----BEGIN ";
    let Some(start) = find_bytes(line, BEGIN) else {
        return;
    };
    let Some(relative_end) = find_bytes(&line[start + BEGIN.len()..], b"PRIVATE KEY-----") else {
        return;
    };
    let end = start + BEGIN.len() + relative_end + b"PRIVATE KEY-----".len();
    matches.push(LineMatch {
        start,
        end,
        rule_id: "content.pem-private-key",
    });
}

fn find_auth_config(file_name: &str, line: &[u8], matches: &mut Vec<LineMatch>) {
    if !matches!(file_name, ".npmrc" | ".pypirc") {
        return;
    }
    let Some(separator) = line
        .iter()
        .position(|byte| *byte == b'=')
        .or_else(|| line.iter().position(|byte| *byte == b':'))
    else {
        return;
    };
    let key = String::from_utf8_lossy(&line[..separator]).to_ascii_lowercase();
    let sensitive = if file_name == ".npmrc" {
        key.contains("_auth") || key.contains("auth-token") || key.contains("password")
    } else {
        matches!(key.trim(), "password" | "token")
    };
    let after_separator = &line[separator.saturating_add(1)..];
    let value = after_separator.trim_ascii_start();
    if sensitive && !value.is_empty() {
        matches.push(LineMatch {
            start: line.len().saturating_sub(value.len()),
            end: line.len(),
            rule_id: if file_name == ".npmrc" {
                "content.npm-auth"
            } else {
                "content.pypi-auth"
            },
        });
    }
}

fn find_shell_secret(shell_file: bool, envrc: bool, line: &[u8], matches: &mut Vec<LineMatch>) {
    if !shell_file {
        return;
    }
    let leading = line
        .iter()
        .take_while(|byte| byte.is_ascii_whitespace())
        .count();
    let mut declaration = &line[leading..];
    let exported = declaration.starts_with(b"export ");
    if exported {
        declaration = &declaration[b"export ".len()..];
    } else if !envrc {
        return;
    }
    let Some(equal) = declaration.iter().position(|byte| *byte == b'=') else {
        return;
    };
    let variable = &declaration[..equal];
    if variable.is_empty()
        || !variable
            .iter()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || *byte == b'_')
        || ![
            b"_TOKEN".as_slice(),
            b"_SECRET".as_slice(),
            b"_KEY".as_slice(),
        ]
        .iter()
        .any(|suffix| variable.ends_with(suffix))
    {
        return;
    }
    let value = declaration[equal.saturating_add(1)..].trim_ascii_start();
    if !value.is_empty() {
        matches.push(LineMatch {
            start: line.len().saturating_sub(value.len()),
            end: line.len(),
            rule_id: "content.shell-secret-export",
        });
    }
}

fn redact_line(line: &[u8], matches: &[LineMatch]) -> String {
    let mut ranges: Vec<(usize, usize)> = matches
        .iter()
        .map(|matched| (matched.start, matched.end))
        .collect();
    ranges.sort_unstable();
    let mut merged: Vec<(usize, usize)> = Vec::new();
    for (start, end) in ranges {
        if let Some(last) = merged.last_mut().filter(|last| start <= last.1) {
            last.1 = last.1.max(end);
        } else {
            merged.push((start, end));
        }
    }

    let mut redacted = Vec::with_capacity(line.len());
    let mut cursor = 0;
    for (start, end) in merged {
        redacted.extend_from_slice(&line[cursor..start]);
        redacted.extend_from_slice(REDACTION);
        cursor = end;
    }
    redacted.extend_from_slice(&line[cursor..]);
    String::from_utf8_lossy(&redacted).into_owned()
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn github_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
}

fn slack_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'-'
}

fn secret_key_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-')
}

fn aws_key_byte(byte: u8) -> bool {
    byte.is_ascii_uppercase() || byte.is_ascii_digit()
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;
    use std::fs;
    use std::io;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::{
        LineMatch, SecretScanError, SecretWaiver, find_auth_config, find_aws_access_keys,
        find_pem_marker, find_prefixed_token, find_shell_secret, github_token_byte, redact_line,
        scan_tree, secret_key_byte, slack_token_byte,
    };

    static NEXT_DIR: AtomicU64 = AtomicU64::new(0);

    struct TestDir(PathBuf);

    impl TestDir {
        fn new() -> Self {
            let sequence = NEXT_DIR.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "cowshed-secret-test-{}-{sequence}",
                std::process::id()
            ));
            fs::create_dir(&path).expect("temporary test directory is created");
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }

        fn write(&self, relative: &str, contents: &str) {
            let path = self.0.join(relative);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).expect("test parent is created");
            }
            fs::write(path, contents).expect("test fixture is written");
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn filename_classes_are_detected_without_exposing_contents() {
        let tree = TestDir::new();
        tree.write(".env.local", "DATABASE_PASSWORD=hunter2");
        tree.write("keys/deploy.pem", "private-pem-material");
        tree.write("keys/id_ed25519", "private-ssh-material");
        tree.write(".netrc", "machine example.test password secret");
        tree.write(".aws/credentials", "aws_secret_access_key=secret");

        let scan = scan_tree(tree.path(), &[]).expect("scan succeeds");
        let rules: Vec<&str> = scan
            .findings
            .iter()
            .map(|finding| finding.rule_id.as_str())
            .collect();
        assert!(rules.contains(&"filename.env"));
        assert!(rules.contains(&"filename.pem"));
        assert!(rules.contains(&"filename.ssh-private-key"));
        assert!(rules.contains(&"filename.netrc"));
        assert!(rules.contains(&"filename.cloud-credentials"));
        let serialized = serde_json::to_string(&scan).expect("scan serializes");
        for secret in ["hunter2", "private-pem-material", "private-ssh-material"] {
            assert!(!serialized.contains(secret));
        }
    }

    #[test]
    fn known_content_shapes_are_redacted_and_entropy_is_ignored() {
        let tree = TestDir::new();
        let github = "ghp_abcdefghijklmnopqrstuvwxyz0123456789";
        let slack = concat!("xox", "b-1234567890-abcdefghijklmnop");
        let generic = "sk-abcdefghijklmnopqrstuvwxyz012345";
        let aws = "AKIAABCDEFGHIJKLMNOP";
        tree.write(
            "tokens.txt",
            &format!("github={github} slack={slack}\ngeneric={generic}\naws={aws}\n-----BEGIN PRIVATE KEY-----\n"),
        );
        tree.write("random.txt", "pR4mV7zQ2xN9cL6kJ8hG5fD3sA1wE0uYbT");

        let scan = scan_tree(tree.path(), &[]).expect("scan succeeds");
        assert_eq!(scan.findings.len(), 5);
        let serialized = serde_json::to_string(&scan).expect("scan serializes");
        for secret in [github, slack, generic, aws, "-----BEGIN PRIVATE KEY-----"] {
            assert!(!serialized.contains(secret));
        }
        assert!(
            scan.findings
                .iter()
                .all(|finding| finding.context.contains("[REDACTED]"))
        );
        assert!(
            scan.findings
                .iter()
                .all(|finding| finding.path != Path::new("random.txt"))
        );
    }

    #[test]
    fn auth_files_and_shell_exports_only_flag_secret_values() {
        let tree = TestDir::new();
        tree.write(
            ".npmrc",
            "registry=https://registry.npmjs.org\n//registry.npmjs.org/:_authToken=npm-secret",
        );
        tree.write(".pypirc", "username=builder\npassword=pypi-secret");
        tree.write(
            "setup.sh",
            "export BUILD_MODE=debug\nexport SERVICE_TOKEN=super-sensitive-value",
        );
        tree.write(
            ".envrc",
            "GOENV=.cowshed/cache/go/env\nAPI_KEY=envrc-secret",
        );

        let scan = scan_tree(tree.path(), &[]).expect("scan succeeds");
        assert_eq!(scan.findings.len(), 4);
        let serialized = serde_json::to_string(&scan).expect("scan serializes");
        for secret in [
            "npm-secret",
            "pypi-secret",
            "super-sensitive-value",
            "envrc-secret",
        ] {
            assert!(!serialized.contains(secret));
        }
        assert!(!serialized.contains("BUILD_MODE"));
    }

    #[test]
    fn cache_and_git_roots_are_pruned() {
        let tree = TestDir::new();
        for path in [
            ".git/objects/leak",
            "node_modules/pkg/.env",
            ".nx/cache/.env",
            ".cowshed/cache/bun/.env",
            "crate/target/debug/.env",
        ] {
            tree.write(path, "ghp_abcdefghijklmnopqrstuvwxyz0123456789");
        }
        tree.write("crate/Cargo.toml", "[package]\nname='fixture'");
        tree.write("source/.env", "blocked=true");

        let scan = scan_tree(tree.path(), &[]).expect("scan succeeds");
        assert_eq!(scan.findings.len(), 1);
        assert_eq!(scan.findings[0].path, Path::new("source/.env"));
    }

    #[cfg(unix)]
    #[test]
    fn symlinks_are_neither_followed_nor_accepted_as_scan_roots() {
        use std::os::unix::fs::symlink;

        let tree = TestDir::new();
        let outside = TestDir::new();
        outside.write(".env", "outside-secret");
        symlink(outside.path(), tree.path().join("linked")).expect("symlink is created");

        let scan = scan_tree(tree.path(), &[]).expect("interior symlink is safely skipped");
        assert!(scan.findings.is_empty());
        let error =
            scan_tree(&tree.path().join("linked"), &[]).expect_err("symlink root is refused");
        assert!(matches!(error, SecretScanError::InvalidRoot { .. }));
    }

    #[test]
    fn reasoned_waivers_suppress_but_retain_findings() {
        let tree = TestDir::new();
        tree.write("fixtures/.env.test", "documented=fake");
        let waiver = SecretWaiver {
            path: PathBuf::from("fixtures/.env.test"),
            reason: "public test fixture".to_owned(),
        };

        let scan = scan_tree(tree.path(), &[waiver]).expect("scan succeeds");
        assert!(scan.findings.is_empty());
        assert_eq!(scan.waived_findings.len(), 1);
        assert_eq!(scan.waived_findings[0].reason, "public test fixture");

        let invalid = SecretWaiver {
            path: PathBuf::from("fixtures/.env.test"),
            reason: "  ".to_owned(),
        };
        assert!(matches!(
            scan_tree(tree.path(), &[invalid]),
            Err(SecretScanError::InvalidWaiver { .. })
        ));
    }

    #[test]
    fn result_order_is_path_then_rule_then_line() {
        let tree = TestDir::new();
        tree.write("z.txt", "sk-abcdefghijklmnopqrstuvwxyz012345");
        tree.write(
            "a.txt",
            "AKIAABCDEFGHIJKLMNOP\nghp_abcdefghijklmnopqrstuvwxyz0123456789",
        );

        let first = scan_tree(tree.path(), &[]).expect("scan succeeds");
        let second = scan_tree(tree.path(), &[]).expect("scan succeeds");
        assert_eq!(first, second);
        assert_eq!(first.findings[0].path, Path::new("a.txt"));
        assert_eq!(first.findings[1].path, Path::new("a.txt"));
        assert_eq!(first.findings[2].path, Path::new("z.txt"));
    }

    #[test]
    fn io_failures_are_typed() {
        let tree = TestDir::new();
        let missing = tree.path().join("missing");
        assert!(matches!(
            scan_tree(&missing, &[]),
            Err(SecretScanError::Read { path, .. }) if path == missing
        ));
    }
    #[test]
    fn waiver_paths_reject_each_invalid_shape_and_duplicates() {
        let tree = TestDir::new();
        tree.write("fixture.txt", "safe");
        for path in ["", "/absolute", "dir/../fixture", "./fixture"] {
            let waiver = SecretWaiver {
                path: PathBuf::from(path),
                reason: "documented".to_owned(),
            };
            assert!(
                matches!(
                    scan_tree(tree.path(), &[waiver]),
                    Err(SecretScanError::InvalidWaiver { .. })
                ),
                "waiver path {path:?} must be rejected"
            );
        }

        let waiver = SecretWaiver {
            path: PathBuf::from("fixture.txt"),
            reason: "documented".to_owned(),
        };
        assert!(matches!(
            scan_tree(tree.path(), &[waiver.clone(), waiver]),
            Err(SecretScanError::DuplicateWaiver { path })
                if path == Path::new("fixture.txt")
        ));
    }

    #[test]
    fn pruning_is_exact_and_only_applies_to_cache_directories() {
        let tree = TestDir::new();
        for path in [
            ".cowshed/cache/hidden.env",
            "nested/.cowshed/cache/hidden.env",
            "crate/target/hidden.env",
        ] {
            tree.write(path, "ghp_abcdefghijklmnopqrstuvwxyz0123456789");
        }
        tree.write("crate/Cargo.toml", "[package]\nname='fixture'");
        for path in [
            ".cowshed/not-cache/visible.env",
            ".cowshed/cacheish/visible.env",
            "target/visible.env",
            "ordinary/.gitkeep",
        ] {
            tree.write(path, "ghp_abcdefghijklmnopqrstuvwxyz0123456789");
        }

        let scan = scan_tree(tree.path(), &[]).expect("scan succeeds");
        let paths: Vec<&Path> = scan
            .findings
            .iter()
            .map(|finding| finding.path.as_path())
            .collect();
        for visible in [
            ".cowshed/not-cache/visible.env",
            ".cowshed/cacheish/visible.env",
            "target/visible.env",
            "ordinary/.gitkeep",
        ] {
            assert!(paths.contains(&Path::new(visible)));
        }
        assert!(
            !paths
                .iter()
                .any(|path| path.to_string_lossy().contains("hidden.env"))
        );
    }

    #[test]
    fn findings_report_one_based_line_numbers() {
        let tree = TestDir::new();
        tree.write(
            "lines.txt",
            "safe\nghp_abcdefghijklmnopqrstuvwxyz0123456789\nsafe\nAKIAABCDEFGHIJKLMNOP",
        );
        let scan = scan_tree(tree.path(), &[]).expect("scan succeeds");
        assert_eq!(
            scan.findings
                .iter()
                .find(|finding| finding.rule_id == "token.github-pat")
                .and_then(|finding| finding.line),
            Some(2)
        );
        assert_eq!(
            scan.findings
                .iter()
                .find(|finding| finding.rule_id == "token.aws-access-key")
                .and_then(|finding| finding.line),
            Some(4)
        );
    }

    #[test]
    fn cloud_and_shell_path_classification_is_exact() {
        let tree = TestDir::new();
        for path in [
            ".aws/credentials",
            ".azure/profile",
            ".gcloud/configurations/default",
            ".config/gcloud/credentials",
        ] {
            tree.write(path, "safe");
        }
        for path in ["config/gcloud/safe", ".config/other/safe", "aws/safe"] {
            tree.write(path, "safe");
        }
        tree.write("script.txt", "export SERVICE_TOKEN=visible");
        tree.write("script.sh", "export SERVICE_TOKEN=hidden");
        tree.write("profile", "export SERVICE_TOKEN=visible");
        tree.write(".profile", "export SERVICE_TOKEN=hidden");

        let scan = scan_tree(tree.path(), &[]).expect("scan succeeds");
        let cloud: Vec<&Path> = scan
            .findings
            .iter()
            .filter(|finding| finding.rule_id == "filename.cloud-credentials")
            .map(|finding| finding.path.as_path())
            .collect();
        assert_eq!(cloud.len(), 4);
        for expected in [
            ".aws/credentials",
            ".azure/profile",
            ".gcloud/configurations/default",
            ".config/gcloud/credentials",
        ] {
            assert!(cloud.contains(&Path::new(expected)));
        }
        let shell: Vec<&Path> = scan
            .findings
            .iter()
            .filter(|finding| finding.rule_id == "content.shell-secret-export")
            .map(|finding| finding.path.as_path())
            .collect();
        assert_eq!(shell, vec![Path::new(".profile"), Path::new("script.sh")]);
    }

    #[test]
    fn token_scanners_enforce_boundaries_lengths_and_alphabets() {
        let mut matches = Vec::new();
        find_prefixed_token(
            b"x ghp_abcdefghijklmnopqrst!",
            b"ghp_",
            20,
            github_token_byte,
            "github",
            &mut matches,
        );
        assert_eq!(
            matches,
            vec![LineMatch {
                start: 2,
                end: 26,
                rule_id: "github"
            }]
        );
        matches.clear();
        for value in [
            b"_ghp_abcdefghijklmnopqrst".as_slice(),
            b"ghp_abcdefghijklmnopqrs",
        ] {
            find_prefixed_token(
                value,
                b"ghp_",
                20,
                github_token_byte,
                "github",
                &mut matches,
            );
        }
        assert!(matches.is_empty());
        find_prefixed_token(
            b"xoxb-abc-def-123",
            b"xoxb-",
            10,
            slack_token_byte,
            "slack",
            &mut matches,
        );
        assert_eq!(matches.len(), 1);
        matches.clear();
        find_prefixed_token(
            b"xoxb-abc_defghijklmnop",
            b"xoxb-",
            10,
            slack_token_byte,
            "slack",
            &mut matches,
        );
        assert!(matches.is_empty());
        assert!(slack_token_byte(b'-'));
        assert!(!slack_token_byte(b'_'));
        assert!(!slack_token_byte(b'!'));
        find_prefixed_token(
            b"sk-abc_DEF-ghi-jkl-mnop",
            b"sk-",
            20,
            secret_key_byte,
            "secret",
            &mut matches,
        );
        assert_eq!(matches.len(), 1);

        matches.clear();
        find_aws_access_keys(b"x AKIAABCDEFGHIJKLMNOP!", &mut matches);
        assert_eq!(
            matches,
            vec![LineMatch {
                start: 2,
                end: 22,
                rule_id: "token.aws-access-key"
            }]
        );
        matches.clear();
        for value in [
            b"AAKIAABCDEFGHIJKLMNOP".as_slice(),
            b"AKIAABCDEFGHIJKLMNO",
            b"AKIAABCDEFGHIJKLMNO_",
        ] {
            find_aws_access_keys(value, &mut matches);
        }
        assert!(matches.is_empty());
    }

    #[test]
    fn pem_and_auth_scanners_respect_boundaries_and_redact_only_values() {
        let mut matches = Vec::new();
        let pem = b"before -----BEGIN EC PRIVATE KEY----- after";
        find_pem_marker(pem, &mut matches);
        assert_eq!(redact_line(pem, &matches), "before [REDACTED] after");
        matches.clear();
        find_pem_marker(b"-----BEGIN PUBLIC KEY-----", &mut matches);
        assert!(matches.is_empty());

        find_auth_config(".npmrc", b"registry=https://example.test", &mut matches);
        let npm = b"//example/:_authToken = npm-secret";
        find_auth_config(".npmrc", npm, &mut matches);
        assert_eq!(
            redact_line(npm, &matches),
            "//example/:_authToken = [REDACTED]"
        );
        matches.clear();
        find_auth_config(".npmrc", b"password=npm-password", &mut matches);
        find_auth_config(".npmrc", b"auth-token=npm-token", &mut matches);
        assert_eq!(matches.len(), 2);
        assert!(
            matches
                .iter()
                .all(|matched| matched.rule_id == "content.npm-auth")
        );
        matches.clear();
        find_auth_config(".pypirc", b"username: builder", &mut matches);
        let pypi = b" token: pypi-secret";
        find_auth_config(".pypirc", pypi, &mut matches);
        assert_eq!(redact_line(pypi, &matches), " token: [REDACTED]");
        assert_eq!(matches[0].rule_id, "content.pypi-auth");
        matches.clear();
        find_auth_config("other", b"password=visible", &mut matches);
        find_auth_config(".pypirc", b"password=   ", &mut matches);
        assert!(matches.is_empty());
    }

    #[test]
    fn shell_scanner_requires_secret_variable_and_nonempty_value() {
        let mut matches = Vec::new();
        let secret = b"  export API_TOKEN= hidden";
        find_shell_secret(true, false, secret, &mut matches);
        assert_eq!(
            redact_line(secret, &matches),
            "  export API_TOKEN= [REDACTED]"
        );
        matches.clear();
        for line in [
            b"API_TOKEN=visible".as_slice(),
            b"export api_TOKEN=visible",
            b"export API_VALUE=visible",
            b"export API_TOKEN=   ",
        ] {
            find_shell_secret(true, false, line, &mut matches);
        }
        assert!(matches.is_empty());
        find_shell_secret(true, true, b"API_KEY=hidden", &mut matches);
        assert_eq!(
            redact_line(b"API_KEY=hidden", &matches),
            "API_KEY=[REDACTED]"
        );
    }

    #[test]
    fn redaction_merges_touching_ranges_but_preserves_gaps() {
        let line = b"0123456789";
        let touching = [
            LineMatch {
                start: 1,
                end: 3,
                rule_id: "a",
            },
            LineMatch {
                start: 3,
                end: 5,
                rule_id: "b",
            },
        ];
        assert_eq!(redact_line(line, &touching), "0[REDACTED]56789");
        let separated = [
            LineMatch {
                start: 1,
                end: 3,
                rule_id: "a",
            },
            LineMatch {
                start: 4,
                end: 5,
                rule_id: "b",
            },
        ];
        assert_eq!(redact_line(line, &separated), "0[REDACTED]3[REDACTED]56789");
    }

    #[test]
    fn scan_errors_have_stable_messages_and_sources() {
        let tree = TestDir::new();
        let missing = tree.path().join("missing");
        let read_error = scan_tree(&missing, &[]).expect_err("missing root fails");
        assert!(read_error.to_string().starts_with("cannot read "));
        assert_eq!(
            read_error
                .source()
                .and_then(|source| source.downcast_ref::<io::Error>())
                .map(io::Error::kind),
            Some(io::ErrorKind::NotFound)
        );

        let walk_source = walkdir::WalkDir::new(&missing)
            .into_iter()
            .next()
            .expect("walker emits the missing root")
            .expect_err("missing root produces a walk error");
        let walk_error = SecretScanError::Walk {
            path: PathBuf::from("fixture"),
            source: walk_source,
        };
        assert!(walk_error.to_string().starts_with("cannot walk fixture: "));
        assert!(
            walk_error
                .source()
                .and_then(|source| source.downcast_ref::<walkdir::Error>())
                .is_some()
        );
        let invalid = SecretScanError::InvalidRoot {
            path: PathBuf::from("fixture"),
            reason: "bad root",
        };
        assert_eq!(invalid.to_string(), "cannot scan fixture: bad root");
        assert!(invalid.source().is_none());
    }
}
