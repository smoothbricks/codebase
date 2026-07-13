//! macOS APFS disk-image substrate.
//!
//! Every external operation crosses [`CommandRunner`]. Commands are represented
//! as an executable plus an argument vector; this module never invokes a shell.

use crate::metadata::ImageFormat;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;

const DISKUTIL: &str = "/usr/sbin/diskutil";
const HDIUTIL: &str = "/usr/bin/hdiutil";
const FSCK_APFS: &str = "/sbin/fsck_apfs";
const SYNC: &str = "/bin/sync";
const SW_VERS: &str = "/usr/bin/sw_vers";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommandRequest {
    pub program: PathBuf,
    pub args: Vec<OsString>,
}

impl CommandRequest {
    pub fn new(
        program: impl Into<PathBuf>,
        args: impl IntoIterator<Item = impl Into<OsString>>,
    ) -> Self {
        Self {
            program: program.into(),
            args: args.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CommandOutput {
    pub status: i32,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

impl CommandOutput {
    pub fn success(stdout: impl Into<Vec<u8>>) -> Self {
        Self {
            status: 0,
            stdout: stdout.into(),
            stderr: Vec::new(),
        }
    }

    pub fn failure(status: i32, stderr: impl Into<Vec<u8>>) -> Self {
        Self {
            status,
            stdout: Vec::new(),
            stderr: stderr.into(),
        }
    }

    pub fn succeeded(&self) -> bool {
        self.status == 0
    }
}

#[derive(Debug)]
pub struct CommandRunError {
    pub program: PathBuf,
    pub source: io::Error,
}

impl fmt::Display for CommandRunError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "could not run {}: {}",
            self.program.display(),
            self.source
        )
    }
}

impl std::error::Error for CommandRunError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

pub trait CommandRunner {
    fn run(&self, request: &CommandRequest) -> Result<CommandOutput, CommandRunError>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SystemCommandRunner;

impl CommandRunner for SystemCommandRunner {
    fn run(&self, request: &CommandRequest) -> Result<CommandOutput, CommandRunError> {
        let output = Command::new(&request.program)
            .args(&request.args)
            .output()
            .map_err(|source| CommandRunError {
                program: request.program.clone(),
                source,
            })?;
        Ok(CommandOutput {
            status: output.status.code().unwrap_or(-1),
            stdout: output.stdout,
            stderr: output.stderr,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApfsCaseSensitivity {
    Sensitive,
    Insensitive,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateImageRequest {
    /// Staged path without an image extension, e.g. `.staging/main`.
    pub staged_stem: PathBuf,
    /// Sparse capacity accepted by the native tools, e.g. `100g`.
    pub capacity: String,
    pub volume_name: String,
    pub case_sensitivity: ApfsCaseSensitivity,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreatedImage {
    pub path: PathBuf,
    pub format: ImageFormat,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttachedImage {
    image: PathBuf,
    format: ImageFormat,
    whole_device: String,
    volume_device: String,
}

impl AttachedImage {
    pub fn image(&self) -> &Path {
        &self.image
    }
    pub fn format(&self) -> ImageFormat {
        self.format
    }
    pub fn whole_device(&self) -> &str {
        &self.whole_device
    }
    pub fn volume_device(&self) -> &str {
        &self.volume_device
    }
}

#[derive(Debug)]
pub enum CloneFileError {
    InvalidImagePath {
        path: PathBuf,
        format: ImageFormat,
    },
    FormatMismatch {
        source: PathBuf,
        destination: PathBuf,
    },
    CrossVolume {
        source: PathBuf,
        destination: PathBuf,
    },
    DestinationExists {
        destination: PathBuf,
    },
    UnsupportedPlatform,
    Io {
        source_path: PathBuf,
        destination_path: PathBuf,
        source: io::Error,
    },
}

impl fmt::Display for CloneFileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidImagePath { path, format } => write!(
                f,
                "{} does not have the required {} image extension",
                path.display(),
                format.extension()
            ),
            Self::FormatMismatch {
                source,
                destination,
            } => write!(
                f,
                "image clone must preserve format: {} -> {}",
                source.display(),
                destination.display()
            ),
            Self::CrossVolume {
                source,
                destination,
            } => write!(
                f,
                "clonefile requires source and destination on the same volume: {} -> {}",
                source.display(),
                destination.display()
            ),
            Self::DestinationExists { destination } => write!(
                f,
                "clone destination already exists: {}",
                destination.display()
            ),
            Self::UnsupportedPlatform => write!(f, "clonefile is available only on macOS"),
            Self::Io {
                source_path,
                destination_path,
                source,
            } => write!(
                f,
                "clonefile {} -> {} failed: {}",
                source_path.display(),
                destination_path.display(),
                source
            ),
        }
    }
}

impl std::error::Error for CloneFileError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub enum ApfsError {
    InvalidImagePath {
        path: PathBuf,
        format: ImageFormat,
    },
    InvalidStagedStem(PathBuf),
    InvalidCreateRequest(&'static str),
    CommandSpawn(CommandRunError),
    CommandFailed {
        operation: &'static str,
        request: CommandRequest,
        output: CommandOutput,
    },
    UnsupportedMacOsVersion(String),
    InvalidAttachmentPlist(String),
    VerificationFailed {
        device: String,
        output: CommandOutput,
    },
    VerificationAndDetachFailed {
        device: String,
        verification: CommandOutput,
        detach: Box<ApfsError>,
    },
    FileOperation {
        operation: &'static str,
        path: PathBuf,
        source: io::Error,
    },
    Clone(CloneFileError),
}

impl fmt::Display for ApfsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidImagePath { path, format } => write!(
                f,
                "{} does not match image format {:?}",
                path.display(),
                format
            ),
            Self::InvalidStagedStem(path) => write!(
                f,
                "staged image stem must not have an extension: {}",
                path.display()
            ),
            Self::InvalidCreateRequest(message) => f.write_str(message),
            Self::CommandSpawn(error) => error.fmt(f),
            Self::CommandFailed {
                operation, output, ..
            } => write!(
                f,
                "{} failed with status {}: {}",
                operation,
                output.status,
                String::from_utf8_lossy(&output.stderr)
            ),
            Self::UnsupportedMacOsVersion(version) => {
                write!(f, "could not parse macOS version: {version}")
            }
            Self::InvalidAttachmentPlist(message) => {
                write!(f, "invalid attachment plist: {message}")
            }
            Self::VerificationFailed { device, output } => write!(
                f,
                "fsck_apfs failed for {device} with status {}",
                output.status
            ),
            Self::VerificationAndDetachFailed { device, .. } => write!(
                f,
                "fsck_apfs failed for {device}, and detaching the failed attachment also failed"
            ),
            Self::FileOperation {
                operation,
                path,
                source,
            } => write!(f, "{} {} failed: {}", operation, path.display(), source),
            Self::Clone(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for ApfsError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::CommandSpawn(error) => Some(error),
            Self::FileOperation { source, .. } => Some(source),
            Self::Clone(error) => Some(error),
            Self::VerificationAndDetachFailed { detach, .. } => Some(detach),
            _ => None,
        }
    }
}

impl From<CommandRunError> for ApfsError {
    fn from(value: CommandRunError) -> Self {
        Self::CommandSpawn(value)
    }
}

impl From<CloneFileError> for ApfsError {
    fn from(value: CloneFileError) -> Self {
        Self::Clone(value)
    }
}

pub trait ApfsBackend {
    fn create_staged_image(&self, request: &CreateImageRequest) -> Result<CreatedImage, ApfsError>;
    fn sync_for_freshness(&self) -> Result<(), ApfsError>;
    fn clone_image(
        &self,
        source: &Path,
        destination: &Path,
        format: ImageFormat,
    ) -> Result<(), CloneFileError>;
    fn sync_and_clone(
        &self,
        source: &Path,
        destination: &Path,
        format: ImageFormat,
    ) -> Result<(), ApfsError>;
    fn attach_verified(
        &self,
        image: &Path,
        format: ImageFormat,
    ) -> Result<AttachedImage, ApfsError>;
    fn mount(
        &self,
        attachment: &AttachedImage,
        mount_point: &Path,
        browse: bool,
    ) -> Result<(), ApfsError>;
    fn detach(&self, attachment: &AttachedImage, force: bool) -> Result<(), ApfsError>;
    fn delete_image(&self, image: &Path, format: ImageFormat) -> Result<(), ApfsError>;
}

pub struct MacOsApfsBackend<R> {
    runner: R,
}

impl<R> MacOsApfsBackend<R> {
    pub fn new(runner: R) -> Self {
        Self { runner }
    }
    pub fn runner(&self) -> &R {
        &self.runner
    }
}

impl<R: CommandRunner> MacOsApfsBackend<R> {
    fn run_checked(
        &self,
        operation: &'static str,
        request: CommandRequest,
    ) -> Result<CommandOutput, ApfsError> {
        let output = self.runner.run(&request)?;
        if output.succeeded() {
            Ok(output)
        } else {
            Err(ApfsError::CommandFailed {
                operation,
                request,
                output,
            })
        }
    }

    fn macos_major_version(&self) -> Result<u32, ApfsError> {
        let request = CommandRequest::new(SW_VERS, ["-productVersion"]);
        let output = self.run_checked("read macOS version", request)?;
        let version = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        version
            .split('.')
            .next()
            .and_then(|part| part.parse().ok())
            .ok_or(ApfsError::UnsupportedMacOsVersion(version))
    }

    fn create_asif(&self, path: &Path, request: &CreateImageRequest) -> Result<(), ApfsError> {
        validate_image_path(path, ImageFormat::Asif)?;
        let command = CommandRequest::new(
            DISKUTIL,
            [
                OsString::from("image"),
                OsString::from("create"),
                OsString::from("blank"),
                OsString::from("--format"),
                OsString::from("ASIF"),
                OsString::from("--size"),
                OsString::from(&request.capacity),
                OsString::from("--volumeName"),
                OsString::from(&request.volume_name),
                OsString::from("--fs"),
                OsString::from("APFS"),
                path.as_os_str().to_owned(),
            ],
        );
        self.run_checked("create ASIF image", command).map(|_| ())
    }

    fn create_sparse(&self, path: &Path, request: &CreateImageRequest) -> Result<(), ApfsError> {
        validate_image_path(path, ImageFormat::Sparse)?;
        let filesystem = match request.case_sensitivity {
            ApfsCaseSensitivity::Sensitive => "Case-sensitive APFS",
            ApfsCaseSensitivity::Insensitive => "APFS",
        };
        let command = CommandRequest::new(
            HDIUTIL,
            [
                OsString::from("create"),
                OsString::from("-quiet"),
                OsString::from("-size"),
                OsString::from(&request.capacity),
                OsString::from("-type"),
                OsString::from("SPARSE"),
                OsString::from("-fs"),
                OsString::from(filesystem),
                OsString::from("-volname"),
                OsString::from(&request.volume_name),
                OsString::from("-nospotlight"),
                path.as_os_str().to_owned(),
            ],
        );
        self.run_checked("create SPARSE image", command).map(|_| ())
    }

    fn attach_without_mounting(
        &self,
        image: &Path,
        format: ImageFormat,
    ) -> Result<AttachedImage, ApfsError> {
        validate_image_path(image, format)?;
        let request = match format {
            ImageFormat::Asif => CommandRequest::new(
                DISKUTIL,
                [
                    OsString::from("image"),
                    OsString::from("attach"),
                    OsString::from("--nobrowse"),
                    OsString::from("--noMount"),
                    OsString::from("--plist"),
                    image.as_os_str().to_owned(),
                ],
            ),
            ImageFormat::Sparse => CommandRequest::new(
                HDIUTIL,
                [
                    OsString::from("attach"),
                    OsString::from("-nobrowse"),
                    OsString::from("-owners"),
                    OsString::from("on"),
                    OsString::from("-nomount"),
                    OsString::from("-plist"),
                    image.as_os_str().to_owned(),
                ],
            ),
        };
        let output = self.run_checked("attach image without mounting", request)?;
        let (whole_device, volume_device) = parse_attachment_plist(&output.stdout)?;
        Ok(AttachedImage {
            image: image.to_owned(),
            format,
            whole_device,
            volume_device,
        })
    }

    fn detach_device(
        &self,
        format: ImageFormat,
        whole_device: &str,
        force: bool,
    ) -> Result<(), ApfsError> {
        let request = match format {
            ImageFormat::Asif => {
                let mut args = vec![OsString::from("eject")];
                if force {
                    args.push(OsString::from("force"));
                }
                args.push(OsString::from(whole_device));
                CommandRequest::new(DISKUTIL, args)
            }
            ImageFormat::Sparse => {
                let mut args = vec![OsString::from("detach"), OsString::from("-quiet")];
                if force {
                    args.push(OsString::from("-force"));
                }
                args.push(OsString::from(whole_device));
                CommandRequest::new(HDIUTIL, args)
            }
        };
        self.run_checked("detach image", request).map(|_| ())
    }
}

impl<R: CommandRunner> ApfsBackend for MacOsApfsBackend<R> {
    fn create_staged_image(&self, request: &CreateImageRequest) -> Result<CreatedImage, ApfsError> {
        if request.staged_stem.extension().is_some() {
            return Err(ApfsError::InvalidStagedStem(request.staged_stem.clone()));
        }
        if request.staged_stem.file_name().is_none() {
            return Err(ApfsError::InvalidStagedStem(request.staged_stem.clone()));
        }
        if request.capacity.trim().is_empty() {
            return Err(ApfsError::InvalidCreateRequest(
                "image capacity must not be empty",
            ));
        }
        if request.volume_name.is_empty() {
            return Err(ApfsError::InvalidCreateRequest(
                "volume name must not be empty",
            ));
        }

        let asif_path = request
            .staged_stem
            .with_extension(ImageFormat::Asif.extension());
        let sparse_path = request
            .staged_stem
            .with_extension(ImageFormat::Sparse.extension());
        validate_image_path(&asif_path, ImageFormat::Asif)?;
        validate_image_path(&sparse_path, ImageFormat::Sparse)?;

        // diskutil's blank-image API cannot request case-sensitive APFS. Preserve
        // repository case behavior by selecting SPARSE for that case.
        let try_asif = request.case_sensitivity == ApfsCaseSensitivity::Insensitive
            && self.macos_major_version()? >= 26;
        if try_asif {
            match self.create_asif(&asif_path, request) {
                Ok(()) => {
                    return Ok(CreatedImage {
                        path: asif_path,
                        format: ImageFormat::Asif,
                    });
                }
                Err(ApfsError::CommandFailed {
                    operation,
                    request: command,
                    output,
                }) if asif_is_unsupported(&output) => {
                    if asif_path.exists() {
                        fs::remove_file(&asif_path).map_err(|source| ApfsError::FileOperation {
                            operation: "remove unsupported ASIF artifact",
                            path: asif_path.clone(),
                            source,
                        })?;
                    }
                    let _ = (operation, command);
                }
                Err(error) => return Err(error),
            }
        }
        self.create_sparse(&sparse_path, request)?;
        Ok(CreatedImage {
            path: sparse_path,
            format: ImageFormat::Sparse,
        })
    }

    fn sync_for_freshness(&self) -> Result<(), ApfsError> {
        self.run_checked(
            "sync before clone",
            CommandRequest::new(SYNC, std::iter::empty::<OsString>()),
        )
        .map(|_| ())
    }

    fn clone_image(
        &self,
        source: &Path,
        destination: &Path,
        format: ImageFormat,
    ) -> Result<(), CloneFileError> {
        validate_clone_path(source, format)?;
        validate_clone_path(destination, format)?;
        if source.extension() != destination.extension() {
            return Err(CloneFileError::FormatMismatch {
                source: source.to_owned(),
                destination: destination.to_owned(),
            });
        }
        clonefile_native(source, destination)
    }

    fn sync_and_clone(
        &self,
        source: &Path,
        destination: &Path,
        format: ImageFormat,
    ) -> Result<(), ApfsError> {
        self.sync_for_freshness()?;
        self.clone_image(source, destination, format)?;
        Ok(())
    }

    fn attach_verified(
        &self,
        image: &Path,
        format: ImageFormat,
    ) -> Result<AttachedImage, ApfsError> {
        let attachment = self.attach_without_mounting(image, format)?;
        let request = CommandRequest::new(
            FSCK_APFS,
            [
                OsString::from("-q"),
                OsString::from(&attachment.volume_device),
            ],
        );
        let output = self.runner.run(&request)?;
        if output.succeeded() {
            return Ok(attachment);
        }

        match self.detach_device(format, &attachment.whole_device, false) {
            Ok(()) => Err(ApfsError::VerificationFailed {
                device: attachment.volume_device,
                output,
            }),
            Err(detach) => Err(ApfsError::VerificationAndDetachFailed {
                device: attachment.volume_device,
                verification: output,
                detach: Box::new(detach),
            }),
        }
    }

    fn mount(
        &self,
        attachment: &AttachedImage,
        mount_point: &Path,
        browse: bool,
    ) -> Result<(), ApfsError> {
        fs::create_dir_all(mount_point).map_err(|source| ApfsError::FileOperation {
            operation: "create mount point",
            path: mount_point.to_owned(),
            source,
        })?;
        let mut args = vec![OsString::from("mount")];
        if !browse {
            args.push(OsString::from("nobrowse"));
        }
        args.extend([
            OsString::from("-mountPoint"),
            mount_point.as_os_str().to_owned(),
            OsString::from(&attachment.volume_device),
        ]);
        self.run_checked(
            "mount verified APFS volume",
            CommandRequest::new(DISKUTIL, args),
        )
        .map(|_| ())
    }

    fn detach(&self, attachment: &AttachedImage, force: bool) -> Result<(), ApfsError> {
        self.detach_device(attachment.format, &attachment.whole_device, force)
    }

    fn delete_image(&self, image: &Path, format: ImageFormat) -> Result<(), ApfsError> {
        validate_image_path(image, format)?;
        match fs::remove_file(image) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(ApfsError::FileOperation {
                operation: "delete image",
                path: image.to_owned(),
                source,
            }),
        }
    }
}

fn validate_image_path(path: &Path, format: ImageFormat) -> Result<(), ApfsError> {
    if path.extension() == Some(OsStr::new(format.extension())) {
        Ok(())
    } else {
        Err(ApfsError::InvalidImagePath {
            path: path.to_owned(),
            format,
        })
    }
}

fn validate_clone_path(path: &Path, format: ImageFormat) -> Result<(), CloneFileError> {
    if path.extension() == Some(OsStr::new(format.extension())) {
        Ok(())
    } else {
        Err(CloneFileError::InvalidImagePath {
            path: path.to_owned(),
            format,
        })
    }
}

fn asif_is_unsupported(output: &CommandOutput) -> bool {
    let message = format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
    .to_ascii_lowercase();
    [
        "not supported",
        "unsupported",
        "unknown format",
        "invalid format",
        "unrecognized option",
        "unknown option",
    ]
    .iter()
    .any(|needle| message.contains(needle))
}

fn parse_attachment_plist(bytes: &[u8]) -> Result<(String, String), ApfsError> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| ApfsError::InvalidAttachmentPlist("output is not UTF-8 XML".into()))?;
    let mut entities = Vec::new();
    let mut cursor = 0;
    while let Some(relative) = text[cursor..].find("<key>dev-entry</key>") {
        let start = cursor + relative;
        let end = text[start..]
            .find("</dict>")
            .map(|offset| start + offset)
            .unwrap_or(text.len());
        let dict = &text[start..end];
        if let Some(device) = plist_string_after_key(dict, "dev-entry") {
            let hint = plist_string_after_key(dict, "content-hint").unwrap_or_default();
            let kind = plist_string_after_key(dict, "volume-kind").unwrap_or_default();
            entities.push((xml_unescape(device), xml_unescape(hint), xml_unescape(kind)));
        }
        cursor = end.saturating_add(7);
    }
    if entities.is_empty() {
        return Err(ApfsError::InvalidAttachmentPlist(
            "no dev-entry values".into(),
        ));
    }

    let volume = entities
        .iter()
        .filter(|(_, hint, kind)| {
            let hint = hint.to_ascii_lowercase();
            hint.contains("apfs_volume") || kind.eq_ignore_ascii_case("apfs")
        })
        .max_by_key(|(device, _, _)| device_depth(device))
        .or_else(|| {
            entities
                .iter()
                .filter(|(_, hint, _)| hint.to_ascii_lowercase().contains("apfs"))
                .max_by_key(|(device, _, _)| device_depth(device))
        })
        .map(|(device, _, _)| device.clone())
        .ok_or_else(|| ApfsError::InvalidAttachmentPlist("no APFS volume device".into()))?;

    let whole = entities
        .iter()
        .map(|(device, _, _)| device)
        .find(|device| device_depth(device) == 0)
        .cloned()
        .or_else(|| whole_device_from(&volume))
        .ok_or_else(|| ApfsError::InvalidAttachmentPlist("no whole image device".into()))?;
    Ok((whole, volume))
}

fn plist_string_after_key<'a>(dict: &'a str, key: &str) -> Option<&'a str> {
    let marker = format!("<key>{key}</key>");
    let rest = &dict[dict.find(&marker)? + marker.len()..];
    let open = rest.find("<string>")? + "<string>".len();
    let close = rest[open..].find("</string>")? + open;
    Some(&rest[open..close])
}

fn xml_unescape(value: &str) -> String {
    value
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
}

fn device_depth(device: &str) -> usize {
    let Some(tail) = device.strip_prefix("/dev/disk") else {
        return 0;
    };
    tail.bytes().filter(|byte| *byte == b's').count()
}

fn whole_device_from(device: &str) -> Option<String> {
    let tail = device.strip_prefix("/dev/disk")?;
    let digits = tail.bytes().take_while(u8::is_ascii_digit).count();
    (digits > 0).then(|| format!("/dev/disk{}", &tail[..digits]))
}

#[cfg(target_os = "macos")]
fn clonefile_native(source: &Path, destination: &Path) -> Result<(), CloneFileError> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    unsafe extern "C" {
        fn clonefile(
            src: *const std::ffi::c_char,
            dst: *const std::ffi::c_char,
            flags: u32,
        ) -> std::ffi::c_int;
    }
    let src = CString::new(source.as_os_str().as_bytes()).map_err(|_| CloneFileError::Io {
        source_path: source.to_owned(),
        destination_path: destination.to_owned(),
        source: io::Error::new(io::ErrorKind::InvalidInput, "source path contains NUL"),
    })?;
    let dst = CString::new(destination.as_os_str().as_bytes()).map_err(|_| CloneFileError::Io {
        source_path: source.to_owned(),
        destination_path: destination.to_owned(),
        source: io::Error::new(io::ErrorKind::InvalidInput, "destination path contains NUL"),
    })?;
    let result = unsafe { clonefile(src.as_ptr(), dst.as_ptr(), 0) };
    if result == 0 {
        return Ok(());
    }
    Err(classify_clone_error(
        source,
        destination,
        io::Error::last_os_error(),
    ))
}

#[cfg(not(target_os = "macos"))]
fn clonefile_native(_source: &Path, _destination: &Path) -> Result<(), CloneFileError> {
    Err(CloneFileError::UnsupportedPlatform)
}

fn classify_clone_error(source: &Path, destination: &Path, error: io::Error) -> CloneFileError {
    // Darwin EXDEV=18 and EEXIST=17; raw codes are used because std does not
    // expose a cross-device ErrorKind on all supported Rust versions.
    match error.raw_os_error() {
        Some(18) => CloneFileError::CrossVolume {
            source: source.to_owned(),
            destination: destination.to_owned(),
        },
        Some(17) => CloneFileError::DestinationExists {
            destination: destination.to_owned(),
        },
        _ => CloneFileError::Io {
            source_path: source.to_owned(),
            destination_path: destination.to_owned(),
            source: error,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::{Ref, RefCell};
    use std::collections::VecDeque;

    const PLIST: &str = r#"<?xml version="1.0"?><plist><dict><key>system-entities</key><array>
      <dict><key>dev-entry</key><string>/dev/disk9</string><key>content-hint</key><string>GUID_partition_scheme</string></dict>
      <dict><key>dev-entry</key><string>/dev/disk9s2</string><key>content-hint</key><string>Apple_APFS</string></dict>
      <dict><key>dev-entry</key><string>/dev/disk10s1</string><key>content-hint</key><string>Apple_APFS_Volume</string><key>volume-kind</key><string>apfs</string></dict>
    </array></dict></plist>"#;

    #[derive(Default)]
    struct RecordingRunner {
        requests: RefCell<Vec<CommandRequest>>,
        outputs: RefCell<VecDeque<CommandOutput>>,
    }

    impl RecordingRunner {
        fn with_outputs(outputs: impl IntoIterator<Item = CommandOutput>) -> Self {
            Self {
                requests: RefCell::new(Vec::new()),
                outputs: RefCell::new(outputs.into_iter().collect()),
            }
        }
        fn requests(&self) -> Ref<'_, Vec<CommandRequest>> {
            self.requests.borrow()
        }
    }

    impl CommandRunner for RecordingRunner {
        fn run(&self, request: &CommandRequest) -> Result<CommandOutput, CommandRunError> {
            self.requests.borrow_mut().push(request.clone());
            Ok(self
                .outputs
                .borrow_mut()
                .pop_front()
                .expect("test supplied an output for each command"))
        }
    }

    fn argv(request: &CommandRequest) -> Vec<String> {
        request
            .args
            .iter()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect()
    }

    #[test]
    fn rejects_format_extension_mismatch_before_spawning() {
        let backend = MacOsApfsBackend::new(RecordingRunner::default());
        let error = backend
            .attach_verified(Path::new("session.sparseimage"), ImageFormat::Asif)
            .unwrap_err();
        assert!(matches!(error, ApfsError::InvalidImagePath { .. }));
        assert!(backend.runner().requests().is_empty());
    }

    #[test]
    fn asif_attach_never_reaches_hdiutil_and_mount_follows_fsck() {
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
            CommandOutput::success(PLIST),
            CommandOutput::success([]),
            CommandOutput::success([]),
        ]));
        let attachment = backend
            .attach_verified(Path::new("session.asif"), ImageFormat::Asif)
            .unwrap();
        let mount = std::env::temp_dir().join(format!("cowshed-apfs-test-{}", std::process::id()));
        backend.mount(&attachment, &mount, false).unwrap();
        let requests = backend.runner().requests();
        assert_eq!(
            requests
                .iter()
                .map(|r| r.program.as_path())
                .collect::<Vec<_>>(),
            [
                Path::new(DISKUTIL),
                Path::new(FSCK_APFS),
                Path::new(DISKUTIL)
            ]
        );
        assert_eq!(
            argv(&requests[0])[..5],
            ["image", "attach", "--nobrowse", "--noMount", "--plist"]
        );
        assert_eq!(argv(&requests[1]), ["-q", "/dev/disk10s1"]);
        assert_eq!(
            argv(&requests[2])[..3],
            ["mount", "nobrowse", "-mountPoint"]
        );
        let _ = fs::remove_dir(mount);
    }

    #[test]
    fn sparse_attach_uses_hdiutil() {
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
            CommandOutput::success(PLIST),
            CommandOutput::success([]),
        ]));
        backend
            .attach_verified(Path::new("session.sparseimage"), ImageFormat::Sparse)
            .unwrap();
        let requests = backend.runner().requests();
        assert_eq!(requests[0].program, Path::new(HDIUTIL));
        assert_eq!(
            argv(&requests[0]),
            [
                "attach",
                "-nobrowse",
                "-owners",
                "on",
                "-nomount",
                "-plist",
                "session.sparseimage"
            ]
        );
    }

    #[test]
    fn failed_verification_detaches_without_mounting() {
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
            CommandOutput::success(PLIST),
            CommandOutput::failure(8, "not clean"),
            CommandOutput::success([]),
        ]));
        let error = backend
            .attach_verified(Path::new("session.asif"), ImageFormat::Asif)
            .unwrap_err();
        assert!(matches!(error, ApfsError::VerificationFailed { .. }));
        let requests = backend.runner().requests();
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[1].program, Path::new(FSCK_APFS));
        assert_eq!(requests[2].program, Path::new(DISKUTIL));
        assert_eq!(argv(&requests[2]), ["eject", "/dev/disk9"]);
        assert!(
            !requests
                .iter()
                .any(|request| argv(request).first().is_some_and(|arg| arg == "mount"))
        );
    }

    #[test]
    fn unsupported_asif_create_falls_back_to_format_specific_sparse_path() {
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
            CommandOutput::success("26.0\n"),
            CommandOutput::failure(1, "ASIF format is not supported"),
            CommandOutput::success([]),
        ]));
        let created = backend
            .create_staged_image(&CreateImageRequest {
                staged_stem: PathBuf::from(".staging/main"),
                capacity: "100g".into(),
                volume_name: "cowshed.owner--repo.main".into(),
                case_sensitivity: ApfsCaseSensitivity::Insensitive,
            })
            .unwrap();
        assert_eq!(
            created,
            CreatedImage {
                path: PathBuf::from(".staging/main.sparseimage"),
                format: ImageFormat::Sparse
            }
        );
        let requests = backend.runner().requests();
        assert_eq!(
            requests
                .iter()
                .map(|r| r.program.as_path())
                .collect::<Vec<_>>(),
            [Path::new(SW_VERS), Path::new(DISKUTIL), Path::new(HDIUTIL)]
        );
        assert!(argv(&requests[1]).contains(&".staging/main.asif".into()));
        assert!(argv(&requests[2]).contains(&".staging/main.sparseimage".into()));
    }

    #[test]
    fn pre_tahoe_create_selects_sparse_without_asif_request() {
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
            CommandOutput::success("15.6\n"),
            CommandOutput::success([]),
        ]));
        let created = backend
            .create_staged_image(&CreateImageRequest {
                staged_stem: PathBuf::from(".staging/main"),
                capacity: "100g".into(),
                volume_name: "cowshed.owner--repo.main".into(),
                case_sensitivity: ApfsCaseSensitivity::Insensitive,
            })
            .unwrap();
        assert_eq!(created.format, ImageFormat::Sparse);
        assert_eq!(
            backend
                .runner()
                .requests()
                .iter()
                .filter(|r| r.program == Path::new(DISKUTIL))
                .count(),
            0
        );
    }

    #[test]
    fn sync_precedes_clone_validation_and_operation() {
        let backend =
            MacOsApfsBackend::new(RecordingRunner::with_outputs([CommandOutput::success([])]));
        let error = backend
            .sync_and_clone(
                Path::new("main.asif"),
                Path::new("session.sparseimage"),
                ImageFormat::Asif,
            )
            .unwrap_err();
        assert!(matches!(
            error,
            ApfsError::Clone(CloneFileError::InvalidImagePath { .. })
        ));
        assert_eq!(
            backend.runner().requests().as_slice(),
            [CommandRequest::new(SYNC, std::iter::empty::<OsString>())]
        );
    }

    #[test]
    fn plist_selects_apfs_volume_and_whole_image_device() {
        assert_eq!(
            parse_attachment_plist(PLIST.as_bytes()).unwrap(),
            ("/dev/disk9".into(), "/dev/disk10s1".into())
        );
    }

    #[test]
    fn clonefile_cross_volume_error_is_typed() {
        let error = classify_clone_error(
            Path::new("main.asif"),
            Path::new("session.asif"),
            io::Error::from_raw_os_error(18),
        );
        assert!(matches!(error, CloneFileError::CrossVolume { .. }));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn clonefile_creates_an_independent_same_volume_image_file() {
        let nonce = format!("{}-{:?}", std::process::id(), std::thread::current().id());
        let source = std::env::temp_dir().join(format!("cowshed-clone-source-{nonce}.asif"));
        let destination =
            std::env::temp_dir().join(format!("cowshed-clone-destination-{nonce}.asif"));
        fs::write(&source, b"fresh image bytes").unwrap();

        clonefile_native(&source, &destination).unwrap();
        assert_eq!(fs::read(&destination).unwrap(), b"fresh image bytes");
        fs::write(&destination, b"changed clone").unwrap();
        assert_eq!(fs::read(&source).unwrap(), b"fresh image bytes");

        fs::remove_file(source).unwrap();
        fs::remove_file(destination).unwrap();
    }
}
