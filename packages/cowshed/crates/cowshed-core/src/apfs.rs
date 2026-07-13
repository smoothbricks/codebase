//! macOS APFS disk-image substrate.
//!
//! Every external operation crosses [`CommandRunner`]. Commands are represented
//! as an executable plus an argument vector; this module never invokes a shell.

use crate::metadata::ImageFormat;
use std::collections::BTreeSet;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;

const DISKUTIL: &str = "/usr/sbin/diskutil";
const HDIUTIL: &str = "/usr/bin/hdiutil";
const FSCK_APFS: &str = "/sbin/fsck_apfs";
const NEWFS_APFS: &str = "/System/Library/Filesystems/apfs.fs/Contents/Resources/newfs_apfs";
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ImageFormatSelection {
    Auto,
    Exact(ImageFormat),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateImageRequest {
    /// Staged path without an image extension, e.g. `.staging/main`.
    pub staged_stem: PathBuf,
    /// Sparse capacity accepted by the native tools, e.g. `100g`.
    pub capacity: String,
    pub volume_name: String,
    pub case_sensitivity: ApfsCaseSensitivity,
    pub image_format: ImageFormatSelection,
    pub owner_uid: u32,
    pub owner_gid: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreatedImage {
    pub path: PathBuf,
    pub format: ImageFormat,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DetachTarget<'a> {
    Device(&'a str),
    MountPoint(&'a Path),
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VolumeResolutionFailure {
    Missing,
    Ambiguous(Vec<String>),
    InvalidPlist(String),
}

impl fmt::Display for VolumeResolutionFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing => f.write_str("no APFS volume device was reported"),
            Self::Ambiguous(devices) => {
                write!(
                    f,
                    "multiple APFS volume devices were reported: {}",
                    devices.join(", ")
                )
            }
            Self::InvalidPlist(message) => write!(f, "invalid APFS list plist: {message}"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VolumeNameResolutionFailure {
    InvalidPlist(String),
    MissingDeviceIdentifier,
    DeviceMismatch { reported: String },
    MissingVolumeName,
    WrongTypeVolumeName,
    BlankVolumeName,
}

impl fmt::Display for VolumeNameResolutionFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPlist(message) => write!(f, "invalid disk info plist: {message}"),
            Self::MissingDeviceIdentifier => f.write_str("disk info plist has no DeviceIdentifier"),
            Self::DeviceMismatch { reported } => {
                write!(f, "disk info plist reported a different device: {reported}")
            }
            Self::MissingVolumeName => f.write_str("disk info plist has no VolumeName"),
            Self::WrongTypeVolumeName => f.write_str("disk info plist VolumeName is not a string"),
            Self::BlankVolumeName => f.write_str("disk info plist VolumeName is blank"),
        }
    }
}

#[derive(Debug)]
pub struct AttachmentDetachFailure {
    pub device: String,
    pub error: Box<ApfsError>,
}

#[derive(Debug)]
pub struct AttachmentCleanupFailure {
    pub inventory: Option<Box<ApfsError>>,
    pub detach: Vec<AttachmentDetachFailure>,
    pub remaining_devices: Vec<String>,
}

#[derive(Debug)]
pub enum ApfsError {
    InvalidImagePath {
        path: PathBuf,
        format: ImageFormat,
    },
    InvalidStagedStem(PathBuf),
    InvalidCreateRequest(&'static str),
    InvalidDetachTarget(PathBuf),
    InvalidMountPoint(PathBuf),
    InvalidVolumeName(String),
    InvalidVolumeDevice(String),
    UnsupportedOperation {
        operation: &'static str,
        format: ImageFormat,
    },
    CommandSpawn(CommandRunError),
    CommandFailed {
        operation: &'static str,
        request: CommandRequest,
        output: CommandOutput,
    },
    UnsupportedMacOsVersion(String),
    InvalidAttachmentPlist(String),
    InvalidAttachmentInventory(String),
    AttachmentCleanupFailed {
        image: PathBuf,
        primary: Box<ApfsError>,
        cleanup: AttachmentCleanupFailure,
    },
    VolumeResolutionFailed {
        candidate: String,
        reason: VolumeResolutionFailure,
    },
    VolumeNameResolutionFailed {
        device: String,
        reason: VolumeNameResolutionFailure,
    },
    VolumeResolutionAndDetachFailed {
        whole_device: String,
        resolution: Box<ApfsError>,
        detach: Box<ApfsError>,
    },
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
    AsifCreationAndCleanupFailed {
        primary: Box<ApfsError>,
        detach: Option<Box<ApfsError>>,
        remove: Option<Box<ApfsError>>,
    },
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
            Self::InvalidDetachTarget(target) => {
                write!(f, "invalid APFS detach target: {}", target.display())
            }
            Self::InvalidMountPoint(path) => {
                write!(f, "invalid APFS mount point: {}", path.display())
            }
            Self::InvalidVolumeName(name) => {
                write!(f, "invalid APFS volume name: {name:?}")
            }
            Self::InvalidVolumeDevice(device) => {
                write!(f, "invalid APFS volume device: {device}")
            }
            Self::UnsupportedOperation { operation, format } => {
                write!(f, "{operation} is not supported for {format:?} images")
            }
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
            Self::InvalidAttachmentInventory(message) => {
                write!(f, "invalid attachment inventory: {message}")
            }
            Self::AttachmentCleanupFailed { image, .. } => write!(
                f,
                "attachment failed for {}, and cleaning up newly attached devices also failed",
                image.display()
            ),
            Self::VolumeResolutionFailed { candidate, reason } => {
                write!(f, "could not resolve APFS volume for {candidate}: {reason}")
            }
            Self::VolumeNameResolutionFailed { device, reason } => {
                write!(
                    f,
                    "could not resolve APFS volume name for {device}: {reason}"
                )
            }
            Self::VolumeResolutionAndDetachFailed { whole_device, .. } => write!(
                f,
                "resolving the APFS volume attached from {whole_device} failed, and detaching it also failed"
            ),
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
            Self::AsifCreationAndCleanupFailed { .. } => {
                f.write_str("ASIF creation failed, and staged-image cleanup also failed")
            }
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
            Self::VolumeResolutionAndDetachFailed { detach, .. } => Some(detach),
            Self::AsifCreationAndCleanupFailed { primary, .. } => Some(primary),
            Self::AttachmentCleanupFailed { primary, .. } => Some(primary),
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
    fn compact_image(&self, image: &Path, format: ImageFormat) -> Result<(), ApfsError>;
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
    fn volume_name(&self, device: &str) -> Result<String, ApfsError>;
    fn rename_volume(&self, mount_point: &Path, volume_name: &str) -> Result<(), ApfsError>;
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
    fn detach_target(
        &self,
        format: ImageFormat,
        target: DetachTarget<'_>,
        force: bool,
    ) -> Result<(), ApfsError>;
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
    fn attached_whole_devices(&self, image: &Path) -> Result<BTreeSet<String>, ApfsError> {
        let image = attachment_inventory_path(image)?;
        // Tahoe's `diskutil image info --plist` does not expose attachment
        // devices. The read-only hdiutil inventory is the observed authoritative
        // image-path -> system-entities map for both ASIF and SPARSE images; ASIF
        // creation, attachment, and detachment remain diskutil-only.
        let output = self.run_checked(
            "inventory attached disk images",
            CommandRequest::new(HDIUTIL, ["info", "-plist"]),
        )?;
        parse_attachment_inventory(&image, &output.stdout)
    }

    fn cleanup_new_attachments(
        &self,
        image: &Path,
        format: ImageFormat,
        before: &BTreeSet<String>,
    ) -> Result<(), AttachmentCleanupFailure> {
        let after =
            self.attached_whole_devices(image)
                .map_err(|error| AttachmentCleanupFailure {
                    inventory: Some(Box::new(error)),
                    detach: Vec::new(),
                    remaining_devices: Vec::new(),
                })?;
        let new_devices: BTreeSet<_> = after.difference(before).cloned().collect();
        let mut detach = Vec::new();
        for device in &new_devices {
            if let Err(error) = self.detach_device(format, device, false) {
                detach.push(AttachmentDetachFailure {
                    device: device.clone(),
                    error: Box::new(error),
                });
            }
        }

        let verified = match self.attached_whole_devices(image) {
            Ok(verified) => verified,
            Err(error) => {
                return Err(AttachmentCleanupFailure {
                    inventory: Some(Box::new(error)),
                    detach,
                    remaining_devices: Vec::new(),
                });
            }
        };
        let remaining_devices: Vec<String> = new_devices.intersection(&verified).cloned().collect();
        if detach.is_empty() && remaining_devices.is_empty() {
            Ok(())
        } else {
            Err(AttachmentCleanupFailure {
                inventory: None,
                detach,
                remaining_devices,
            })
        }
    }

    fn failed_attachment(
        &self,
        image: &Path,
        format: ImageFormat,
        before: &BTreeSet<String>,
        primary: ApfsError,
    ) -> ApfsError {
        match self.cleanup_new_attachments(image, format, before) {
            Ok(()) => primary,
            Err(cleanup) => ApfsError::AttachmentCleanupFailed {
                image: image.to_owned(),
                primary: Box::new(primary),
                cleanup,
            },
        }
    }

    fn failed_asif_attachment(
        &self,
        path: &Path,
        before: &BTreeSet<String>,
        primary: ApfsError,
    ) -> ApfsError {
        match self.cleanup_new_attachments(path, ImageFormat::Asif, before) {
            Ok(()) => self.cleanup_failed_asif(path, primary),
            Err(cleanup) => ApfsError::AttachmentCleanupFailed {
                image: path.to_owned(),
                primary: Box::new(primary),
                cleanup,
            },
        }
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
        let create = CommandRequest::new(
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
                OsString::from("None"),
                path.as_os_str().to_owned(),
            ],
        );
        self.run_checked("create ASIF image", create)?;
        let attached_before = self.attached_whole_devices(path)?;

        let attach = CommandRequest::new(
            DISKUTIL,
            [
                OsString::from("image"),
                OsString::from("attach"),
                OsString::from("--nobrowse"),
                OsString::from("--noMount"),
                OsString::from("--plist"),
                path.as_os_str().to_owned(),
            ],
        );
        let output = match self.run_checked("attach blank ASIF image", attach) {
            Ok(output) => output,
            Err(primary) => {
                return Err(self.failed_asif_attachment(path, &attached_before, primary));
            }
        };
        let whole_device = match parse_blank_asif_whole_device(&output.stdout) {
            Ok(device) => device,
            Err(primary) => {
                return Err(self.failed_asif_attachment(path, &attached_before, primary));
            }
        };
        if attached_before.contains(&whole_device) {
            return Err(self.failed_attachment(
                path,
                ImageFormat::Asif,
                &attached_before,
                ApfsError::InvalidAttachmentPlist(
                    "attach reported a pre-existing whole image device".into(),
                ),
            ));
        }
        let case_flag = match request.case_sensitivity {
            ApfsCaseSensitivity::Sensitive => "-e",
            ApfsCaseSensitivity::Insensitive => "-i",
        };
        let format = CommandRequest::new(
            NEWFS_APFS,
            [
                OsString::from("-U"),
                OsString::from(request.owner_uid.to_string()),
                OsString::from("-G"),
                OsString::from(request.owner_gid.to_string()),
                OsString::from(case_flag),
                OsString::from("-v"),
                OsString::from(&request.volume_name),
                OsString::from(&whole_device),
            ],
        );
        if let Err(primary) = self.run_checked("format ASIF APFS volume", format) {
            return match self.detach_device(ImageFormat::Asif, &whole_device, true) {
                Ok(()) => Err(self.cleanup_failed_asif(path, primary)),
                Err(detach) => Err(ApfsError::AsifCreationAndCleanupFailed {
                    primary: Box::new(primary),
                    detach: Some(Box::new(detach)),
                    remove: None,
                }),
            };
        }
        self.detach_device(ImageFormat::Asif, &whole_device, true)?;
        Ok(())
    }

    fn cleanup_failed_asif(&self, path: &Path, primary: ApfsError) -> ApfsError {
        let remove = match fs::remove_file(path) {
            Ok(()) => None,
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(source) => Some(ApfsError::FileOperation {
                operation: "remove failed ASIF image",
                path: path.to_owned(),
                source,
            }),
        };
        if remove.is_none() {
            primary
        } else {
            ApfsError::AsifCreationAndCleanupFailed {
                primary: Box::new(primary),
                detach: None,
                remove: remove.map(Box::new),
            }
        }
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

    fn resolve_apfs_volume(&self, candidate: &str) -> Result<String, ApfsError> {
        let output = self.run_checked(
            "resolve APFS volume",
            CommandRequest::new(
                DISKUTIL,
                [
                    OsString::from("apfs"),
                    OsString::from("list"),
                    OsString::from("-plist"),
                ],
            ),
        )?;
        parse_volume_list_plist(candidate, &output.stdout)
    }

    fn attach_without_mounting(
        &self,
        image: &Path,
        format: ImageFormat,
    ) -> Result<AttachedImage, ApfsError> {
        validate_image_path(image, format)?;
        let attached_before = self.attached_whole_devices(image)?;
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
        let output = match self.run_checked("attach image without mounting", request) {
            Ok(output) => output,
            Err(primary) => {
                return Err(self.failed_attachment(image, format, &attached_before, primary));
            }
        };
        let (whole_device, candidate) = match parse_attachment_plist(&output.stdout) {
            Ok(attachment) => attachment,
            Err(primary) => {
                return Err(self.failed_attachment(image, format, &attached_before, primary));
            }
        };
        if attached_before.contains(&whole_device) {
            return Err(self.failed_attachment(
                image,
                format,
                &attached_before,
                ApfsError::InvalidAttachmentPlist(
                    "attach reported a pre-existing whole image device".into(),
                ),
            ));
        }
        let volume_device = match self.resolve_apfs_volume(&candidate) {
            Ok(volume_device) => volume_device,
            Err(resolution) => {
                return match self.detach_device(format, &whole_device, false) {
                    Ok(()) => Err(resolution),
                    Err(detach) => Err(ApfsError::VolumeResolutionAndDetachFailed {
                        whole_device,
                        resolution: Box::new(resolution),
                        detach: Box::new(detach),
                    }),
                };
            }
        };
        Ok(AttachedImage {
            image: image.to_owned(),
            format,
            whole_device,
            volume_device,
        })
    }

    fn detach_target_checked(
        &self,
        format: ImageFormat,
        target: DetachTarget<'_>,
        force: bool,
    ) -> Result<(), ApfsError> {
        let target = validate_detach_target(target)?;
        let request = match format {
            ImageFormat::Asif => {
                let mut args = vec![OsString::from("eject")];
                if force {
                    args.push(OsString::from("force"));
                }
                args.push(target);
                CommandRequest::new(DISKUTIL, args)
            }
            ImageFormat::Sparse => {
                let mut args = vec![OsString::from("detach"), OsString::from("-quiet")];
                if force {
                    args.push(OsString::from("-force"));
                }
                args.push(target);
                CommandRequest::new(HDIUTIL, args)
            }
        };
        self.run_checked("detach image", request).map(|_| ())
    }

    fn detach_device(
        &self,
        format: ImageFormat,
        whole_device: &str,
        force: bool,
    ) -> Result<(), ApfsError> {
        self.detach_target_checked(format, DetachTarget::Device(whole_device), force)
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
        if !is_valid_apfs_volume_name(&request.volume_name) {
            return Err(ApfsError::InvalidCreateRequest(
                "volume name must be path-safe and at most 255 bytes",
            ));
        }

        match request.image_format {
            ImageFormatSelection::Auto => {
                // diskutil's blank-image API cannot request case-sensitive APFS. Preserve
                // repository case behavior by selecting SPARSE for that case.
                let try_asif = request.case_sensitivity == ApfsCaseSensitivity::Insensitive
                    && self.macos_major_version()? >= 26;
                if try_asif {
                    let asif_path = request
                        .staged_stem
                        .with_extension(ImageFormat::Asif.extension());
                    match self.create_asif(&asif_path, request) {
                        Ok(()) => {
                            return Ok(CreatedImage {
                                path: asif_path,
                                format: ImageFormat::Asif,
                            });
                        }
                        Err(ApfsError::CommandFailed {
                            operation: "create ASIF image",
                            request: command,
                            output,
                        }) if asif_is_unsupported(&output) => {
                            if asif_path.exists() {
                                fs::remove_file(&asif_path).map_err(|source| {
                                    ApfsError::FileOperation {
                                        operation: "remove unsupported ASIF artifact",
                                        path: asif_path,
                                        source,
                                    }
                                })?;
                            }
                            let _ = command;
                        }
                        Err(error) => return Err(error),
                    }
                }

                let path = request
                    .staged_stem
                    .with_extension(ImageFormat::Sparse.extension());
                self.create_sparse(&path, request)?;
                Ok(CreatedImage {
                    path,
                    format: ImageFormat::Sparse,
                })
            }
            ImageFormatSelection::Exact(ImageFormat::Asif) => {
                let path = request
                    .staged_stem
                    .with_extension(ImageFormat::Asif.extension());
                self.create_asif(&path, request)?;
                Ok(CreatedImage {
                    path,
                    format: ImageFormat::Asif,
                })
            }
            ImageFormatSelection::Exact(ImageFormat::Sparse) => {
                let path = request
                    .staged_stem
                    .with_extension(ImageFormat::Sparse.extension());
                self.create_sparse(&path, request)?;
                Ok(CreatedImage {
                    path,
                    format: ImageFormat::Sparse,
                })
            }
        }
    }

    fn compact_image(&self, image: &Path, format: ImageFormat) -> Result<(), ApfsError> {
        if format == ImageFormat::Asif {
            return Err(ApfsError::UnsupportedOperation {
                operation: "compact image",
                format,
            });
        }
        validate_image_path(image, format)?;
        self.run_checked(
            "compact SPARSE image",
            CommandRequest::new(
                HDIUTIL,
                [
                    OsString::from("compact"),
                    OsString::from("-quiet"),
                    image.as_os_str().to_owned(),
                ],
            ),
        )
        .map(|_| ())
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

    fn volume_name(&self, device: &str) -> Result<String, ApfsError> {
        if !is_kernel_device_path(device) {
            return Err(ApfsError::InvalidVolumeDevice(device.to_owned()));
        }
        let output = self.run_checked(
            "read APFS volume name",
            CommandRequest::new(DISKUTIL, ["info", "-plist", device]),
        )?;
        parse_volume_name_plist(device, &output.stdout)
    }

    fn rename_volume(&self, mount_point: &Path, volume_name: &str) -> Result<(), ApfsError> {
        if !is_canonical_mount_point(mount_point) {
            return Err(ApfsError::InvalidMountPoint(mount_point.to_owned()));
        }
        if !is_valid_apfs_volume_name(volume_name) {
            return Err(ApfsError::InvalidVolumeName(volume_name.to_owned()));
        }
        self.run_checked(
            "rename APFS volume",
            CommandRequest::new(
                DISKUTIL,
                [
                    OsString::from("renameVolume"),
                    mount_point.as_os_str().to_owned(),
                    OsString::from(volume_name),
                ],
            ),
        )
        .map(|_| ())
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
                OsString::from(raw_device_from(&attachment.volume_device)),
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
            OsString::from("-mountOptions"),
            OsString::from("owners"),
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

    fn detach_target(
        &self,
        format: ImageFormat,
        target: DetachTarget<'_>,
        force: bool,
    ) -> Result<(), ApfsError> {
        self.detach_target_checked(format, target, force)
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

fn validate_detach_target(target: DetachTarget<'_>) -> Result<OsString, ApfsError> {
    let valid = match target {
        DetachTarget::Device(device) => is_kernel_device_path(device),
        DetachTarget::MountPoint(path) => is_canonical_mount_point(path),
    };
    if !valid {
        return Err(ApfsError::InvalidDetachTarget(match target {
            DetachTarget::Device(device) => PathBuf::from(device),
            DetachTarget::MountPoint(path) => path.to_owned(),
        }));
    }
    Ok(match target {
        DetachTarget::Device(device) => OsString::from(device),
        DetachTarget::MountPoint(path) => path.as_os_str().to_owned(),
    })
}

fn attachment_inventory_path(image: &Path) -> Result<PathBuf, ApfsError> {
    std::path::absolute(image).map_err(|source| ApfsError::FileOperation {
        operation: "resolve attachment inventory path",
        path: image.to_owned(),
        source,
    })
}

fn parse_attachment_inventory(image: &Path, bytes: &[u8]) -> Result<BTreeSet<String>, ApfsError> {
    let expected = image.to_str().ok_or_else(|| {
        ApfsError::InvalidAttachmentInventory("image path is not valid UTF-8".into())
    })?;
    let value = plist::Value::from_reader(std::io::Cursor::new(bytes))
        .map_err(|error| ApfsError::InvalidAttachmentInventory(error.to_string()))?;
    let images = value
        .as_dictionary()
        .and_then(|root| root.get("images"))
        .and_then(plist::Value::as_array)
        .ok_or_else(|| ApfsError::InvalidAttachmentInventory("missing images array".into()))?;
    let mut devices = BTreeSet::new();
    for image_entry in images {
        let dictionary = image_entry.as_dictionary().ok_or_else(|| {
            ApfsError::InvalidAttachmentInventory("images entry is not a dictionary".into())
        })?;
        let reported_path = dictionary
            .get("image-path")
            .and_then(plist::Value::as_string)
            .ok_or_else(|| {
                ApfsError::InvalidAttachmentInventory(
                    "images entry has no string image-path".into(),
                )
            })?;
        if reported_path != expected {
            continue;
        }
        let entities = dictionary
            .get("system-entities")
            .and_then(plist::Value::as_array)
            .ok_or_else(|| {
                ApfsError::InvalidAttachmentInventory(
                    "matching image has no system-entities array".into(),
                )
            })?;
        let mut roots = 0usize;
        for entity in entities {
            let device = entity
                .as_dictionary()
                .and_then(|entity| entity.get("dev-entry"))
                .and_then(plist::Value::as_string)
                .and_then(device_path)
                .ok_or_else(|| {
                    ApfsError::InvalidAttachmentInventory(
                        "matching image has an invalid dev-entry".into(),
                    )
                })?;
            if device_depth(&device) == 0 && is_kernel_device_path(&device) {
                roots += 1;
                devices.insert(device);
            }
        }
        if roots == 0 {
            return Err(ApfsError::InvalidAttachmentInventory(
                "matching image has no canonical whole device".into(),
            ));
        }
    }
    Ok(devices)
}

fn is_canonical_mount_point(path: &Path) -> bool {
    let bytes = path.as_os_str().as_encoded_bytes();
    bytes.starts_with(b"/")
        && bytes != b"/"
        && !bytes.contains(&0)
        && !path.starts_with("/dev")
        && bytes[1..]
            .split(|byte| *byte == b'/')
            .all(|segment| !segment.is_empty() && segment != b"." && segment != b"..")
}

fn is_valid_apfs_volume_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 255
        && name.trim() == name
        && name != "."
        && name != ".."
        && !name.as_bytes().contains(&b'/')
        && !name.as_bytes().contains(&0)
}

fn is_kernel_device_path(device: &str) -> bool {
    let Some(relative) = device.strip_prefix("/dev/disk") else {
        return false;
    };
    let mut components = relative.split('s');
    components.all(|component| {
        !component.is_empty()
            && component.bytes().all(|byte| byte.is_ascii_digit())
            && (component == "0" || !component.starts_with('0'))
    })
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

fn parse_volume_name_plist(device: &str, bytes: &[u8]) -> Result<String, ApfsError> {
    let failed = |reason| ApfsError::VolumeNameResolutionFailed {
        device: device.to_owned(),
        reason,
    };
    let value = plist::Value::from_reader(std::io::Cursor::new(bytes))
        .map_err(|error| failed(VolumeNameResolutionFailure::InvalidPlist(error.to_string())))?;
    let dictionary = value.as_dictionary().ok_or_else(|| {
        failed(VolumeNameResolutionFailure::InvalidPlist(
            "root is not a dictionary".into(),
        ))
    })?;
    let reported = dictionary
        .get("DeviceIdentifier")
        .ok_or_else(|| failed(VolumeNameResolutionFailure::MissingDeviceIdentifier))?
        .as_string()
        .ok_or_else(|| {
            failed(VolumeNameResolutionFailure::InvalidPlist(
                "DeviceIdentifier is not a string".into(),
            ))
        })?;
    let reported = device_path(reported).ok_or_else(|| {
        failed(VolumeNameResolutionFailure::InvalidPlist(format!(
            "invalid DeviceIdentifier {reported:?}"
        )))
    })?;
    if reported != device {
        return Err(failed(VolumeNameResolutionFailure::DeviceMismatch {
            reported,
        }));
    }
    let name = dictionary
        .get("VolumeName")
        .ok_or_else(|| failed(VolumeNameResolutionFailure::MissingVolumeName))?
        .as_string()
        .ok_or_else(|| failed(VolumeNameResolutionFailure::WrongTypeVolumeName))?
        .trim();
    if name.is_empty() {
        return Err(failed(VolumeNameResolutionFailure::BlankVolumeName));
    }
    Ok(name.to_owned())
}

fn parse_blank_asif_whole_device(bytes: &[u8]) -> Result<String, ApfsError> {
    let value = plist::Value::from_reader(std::io::Cursor::new(bytes))
        .map_err(|error| ApfsError::InvalidAttachmentPlist(error.to_string()))?;
    let system_entities = value
        .as_dictionary()
        .and_then(|root| root.get("system-entities"))
        .and_then(plist::Value::as_array)
        .ok_or_else(|| ApfsError::InvalidAttachmentPlist("missing system-entities array".into()))?;
    let mut whole_devices = Vec::new();
    for entity in system_entities {
        let Some(device) = entity
            .as_dictionary()
            .and_then(|dictionary| dictionary.get("dev-entry"))
            .and_then(plist::Value::as_string)
            .and_then(device_path)
        else {
            continue;
        };
        if device_depth(&device) == 0 && is_kernel_device_path(&device) {
            whole_devices.push(device);
        }
    }
    match whole_devices.len() {
        1 => Ok(whole_devices.pop().expect("one whole device was counted")),
        0 => Err(ApfsError::InvalidAttachmentPlist(
            "no canonical whole image device".into(),
        )),
        _ => Err(ApfsError::InvalidAttachmentPlist(
            "multiple whole image devices".into(),
        )),
    }
}

fn parse_attachment_plist(bytes: &[u8]) -> Result<(String, String), ApfsError> {
    let value = plist::Value::from_reader(std::io::Cursor::new(bytes))
        .map_err(|error| ApfsError::InvalidAttachmentPlist(error.to_string()))?;
    let system_entities = value
        .as_dictionary()
        .and_then(|root| root.get("system-entities"))
        .and_then(plist::Value::as_array)
        .ok_or_else(|| ApfsError::InvalidAttachmentPlist("missing system-entities array".into()))?;
    let mut entities = Vec::new();
    for entity in system_entities {
        let dictionary = entity.as_dictionary().ok_or_else(|| {
            ApfsError::InvalidAttachmentPlist("system-entities entry is not a dictionary".into())
        })?;
        let Some(device) = dictionary
            .get("dev-entry")
            .and_then(plist::Value::as_string)
        else {
            continue;
        };
        let device = device_path(device).unwrap_or_else(|| device.to_owned());
        let hint = dictionary
            .get("content-hint")
            .and_then(plist::Value::as_string)
            .unwrap_or_default()
            .to_owned();
        let kind = dictionary
            .get("volume-kind")
            .or_else(|| dictionary.get("filesystem-type"))
            .and_then(plist::Value::as_string)
            .unwrap_or_default()
            .to_owned();
        entities.push((device, hint, kind));
    }
    if entities.is_empty() {
        return Err(ApfsError::InvalidAttachmentPlist(
            "no dev-entry values".into(),
        ));
    }

    let mut reported_whole_devices = entities.iter().filter(|(device, hint, _)| {
        device_depth(device) == 0 && hint.eq_ignore_ascii_case("GUID_partition_scheme")
    });
    let reported_whole = reported_whole_devices.next();
    if reported_whole_devices.next().is_some() {
        return Err(ApfsError::InvalidAttachmentPlist(
            "multiple whole image devices".into(),
        ));
    }

    let physical_store = reported_whole.and_then(|(whole, _, _)| {
        entities
            .iter()
            .filter(|(device, hint, kind)| {
                let hint = hint.to_ascii_lowercase();
                device_is_descendant_of(device, whole)
                    && hint.contains("apfs")
                    && !hint.contains("apfs_volume")
                    && !kind.eq_ignore_ascii_case("apfs")
            })
            .max_by_key(|(device, _, _)| device_depth(device))
    });
    let candidate = physical_store
        .or_else(|| {
            entities
                .iter()
                .filter(|(_, hint, kind)| {
                    let hint = hint.to_ascii_lowercase();
                    hint.contains("apfs_volume") || kind.eq_ignore_ascii_case("apfs")
                })
                .max_by_key(|(device, _, _)| device_depth(device))
        })
        .or_else(|| {
            entities
                .iter()
                .filter(|(_, hint, _)| hint.to_ascii_lowercase().contains("apfs"))
                .max_by_key(|(device, _, _)| device_depth(device))
        })
        .map(|(device, _, _)| device.clone())
        .ok_or_else(|| ApfsError::InvalidAttachmentPlist("no APFS device candidate".into()))?;

    let whole = match reported_whole {
        Some((device, _, _)) => device.clone(),
        None => whole_device_from(&candidate)
            .ok_or_else(|| ApfsError::InvalidAttachmentPlist("invalid APFS device".into()))?,
    };
    Ok((whole, candidate))
}

fn parse_volume_list_plist(candidate: &str, bytes: &[u8]) -> Result<String, ApfsError> {
    let invalid = |message| ApfsError::VolumeResolutionFailed {
        candidate: candidate.to_owned(),
        reason: VolumeResolutionFailure::InvalidPlist(message),
    };
    let candidate_path = volume_device_path(candidate)
        .ok_or_else(|| invalid(format!("invalid APFS device candidate {candidate:?}")))?;
    let value = plist::Value::from_reader(std::io::Cursor::new(bytes))
        .map_err(|error| invalid(error.to_string()))?;
    let containers = value
        .as_dictionary()
        .and_then(|root| root.get("Containers"))
        .and_then(plist::Value::as_array)
        .ok_or_else(|| invalid("missing Containers array".into()))?;
    let mut matching_containers: usize = 0;
    let mut devices = Vec::new();
    for container in containers {
        let dictionary = container
            .as_dictionary()
            .ok_or_else(|| invalid("container is not a dictionary".into()))?;
        let mut container_volumes = Vec::new();
        if let Some(volumes) = dictionary.get("Volumes") {
            let volumes = volumes
                .as_array()
                .ok_or_else(|| invalid("Volumes is not an array".into()))?;
            for volume in volumes {
                let identifier = volume
                    .as_dictionary()
                    .and_then(|dictionary| dictionary.get("DeviceIdentifier"))
                    .and_then(plist::Value::as_string)
                    .ok_or_else(|| invalid("volume has no DeviceIdentifier string".into()))?;
                let device = volume_device_path(identifier).ok_or_else(|| {
                    invalid(format!("invalid volume DeviceIdentifier {identifier:?}"))
                })?;
                container_volumes.push(device);
            }
        }

        let volume_matches = container_volumes
            .iter()
            .any(|device| device == &candidate_path);
        let mut physical_store_matches = false;
        if let Some(physical_stores) = dictionary.get("PhysicalStores") {
            let physical_stores = physical_stores
                .as_array()
                .ok_or_else(|| invalid("PhysicalStores is not an array".into()))?;
            for physical_store in physical_stores {
                let identifier = physical_store
                    .as_dictionary()
                    .and_then(|dictionary| dictionary.get("DeviceIdentifier"))
                    .and_then(plist::Value::as_string)
                    .ok_or_else(|| {
                        invalid("physical store has no DeviceIdentifier string".into())
                    })?;
                let device = device_path(identifier).ok_or_else(|| {
                    invalid(format!(
                        "invalid physical store DeviceIdentifier {identifier:?}"
                    ))
                })?;
                physical_store_matches |= device == candidate_path;
            }
        }

        if volume_matches || physical_store_matches {
            matching_containers += 1;
            devices.extend(container_volumes);
        }
    }

    if matching_containers == 0 || devices.is_empty() {
        return Err(ApfsError::VolumeResolutionFailed {
            candidate: candidate.to_owned(),
            reason: VolumeResolutionFailure::Missing,
        });
    }
    if matching_containers != 1 || devices.len() != 1 {
        return Err(ApfsError::VolumeResolutionFailed {
            candidate: candidate.to_owned(),
            reason: VolumeResolutionFailure::Ambiguous(devices),
        });
    }
    Ok(devices.pop().expect("one device was counted"))
}

fn device_path(identifier: &str) -> Option<String> {
    let relative = identifier.strip_prefix("/dev/").unwrap_or(identifier);
    let tail = relative.strip_prefix("disk")?;
    let mut parts = tail.split('s');
    let disk = parts.next()?;
    if disk.is_empty() || !disk.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    for partition in parts {
        if partition.is_empty() || !partition.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
    }
    Some(format!("/dev/{relative}"))
}

fn volume_device_path(identifier: &str) -> Option<String> {
    let device = device_path(identifier)?;
    (device_depth(&device) > 0).then_some(device)
}

fn device_depth(device: &str) -> usize {
    let Some(tail) = device.strip_prefix("/dev/disk") else {
        return 0;
    };
    tail.bytes().filter(|byte| *byte == b's').count()
}

fn device_is_descendant_of(device: &str, whole: &str) -> bool {
    device
        .strip_prefix(whole)
        .is_some_and(|suffix| suffix.starts_with('s'))
}

fn whole_device_from(device: &str) -> Option<String> {
    let tail = device.strip_prefix("/dev/disk")?;
    let digits = tail.bytes().take_while(u8::is_ascii_digit).count();
    (digits > 0).then(|| format!("/dev/disk{}", &tail[..digits]))
}

fn raw_device_from(device: &str) -> String {
    match device.strip_prefix("/dev/") {
        Some(relative) => format!("/dev/r{relative}"),
        None => format!("r{device}"),
    }
}

fn clonefile_native(source: &Path, destination: &Path) -> Result<(), CloneFileError> {
    #[cfg(target_os = "macos")]
    {
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
        let dst =
            CString::new(destination.as_os_str().as_bytes()).map_err(|_| CloneFileError::Io {
                source_path: source.to_owned(),
                destination_path: destination.to_owned(),
                source: io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "destination path contains NUL",
                ),
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
    {
        let _ = (source, destination);
        Err(CloneFileError::UnsupportedPlatform)
    }
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
    use std::collections::{BTreeMap, VecDeque};

    const PLIST: &str = r#"<?xml version="1.0"?><plist><dict><key>system-entities</key><array>
      <dict><key>dev-entry</key><string>disk10</string><key>content-hint</key><string>GUID_partition_scheme</string></dict>
      <dict><key>dev-entry</key><string>disk9s2</string><key>content-hint</key><string>Apple_APFS</string></dict>
      <dict><key>dev-entry</key><string>disk10s1</string><key>content-hint</key><string>Apple_APFS_Volume</string><key>volume-kind</key><string>apfs</string></dict>
    </array></dict></plist>"#;

    const EMPTY_ATTACHMENT_INVENTORY: &str =
        r#"<?xml version="1.0"?><plist><dict><key>images</key><array/></dict></plist>"#;
    const BLANK_ASIF_PLIST: &str = r#"<?xml version="1.0"?><plist><dict><key>system-entities</key><array>
      <dict><key>dev-entry</key><string>disk8</string><key>content-hint</key><string>GUID_partition_scheme</string></dict>
    </array></dict></plist>"#;

    const VOLUME_LIST_PLIST: &str = r#"<?xml version="1.0"?><plist version="1.0"><dict>
      <key>Containers</key><array><dict>
        <key>PhysicalStores</key><array>
          <dict><key>DeviceIdentifier</key><string>disk12</string></dict>
        </array>
        <key>Volumes</key><array>
          <dict><key>DeviceIdentifier</key><string>disk10s1</string></dict>
        </array>
      </dict></array>
    </dict></plist>"#;

    const SPARSE_ATTACH_PLIST: &str = r#"<?xml version="1.0"?><plist><dict><key>system-entities</key><array>
      <dict><key>dev-entry</key><string>/dev/disk4</string><key>content-hint</key><string>GUID_partition_scheme</string></dict>
      <dict><key>dev-entry</key><string>/dev/disk4s1</string><key>content-hint</key><string>Apple_APFS</string></dict>
      <dict><key>dev-entry</key><string>/dev/disk5</string><key>content-hint</key><string>EF57347C-0000-11AA-AA11-00306543ECAC</string></dict>
      <dict><key>dev-entry</key><string>/dev/disk5s1</string><key>content-hint</key><string>41504653-0000-11AA-AA11-00306543ECAC</string><key>volume-kind</key><string>apfs</string></dict>
    </array></dict></plist>"#;

    const SPARSE_VOLUME_LIST_PLIST: &str = r#"<?xml version="1.0"?><plist version="1.0"><dict>
      <key>Containers</key><array><dict>
        <key>PhysicalStores</key><array>
          <dict><key>DeviceIdentifier</key><string>disk4s1</string></dict>
        </array>
        <key>Volumes</key><array>
          <dict><key>DeviceIdentifier</key><string>disk5s2</string></dict>
        </array>
      </dict></array>
    </dict></plist>"#;

    const EMPTY_VOLUME_LIST_PLIST: &str = r#"<?xml version="1.0"?><plist version="1.0"><dict>
      <key>Containers</key><array><dict>
        <key>PhysicalStores</key><array>
          <dict><key>DeviceIdentifier</key><string>disk4s1</string></dict>
        </array>
        <key>Volumes</key><array/>
      </dict></array>
    </dict></plist>"#;

    const AMBIGUOUS_VOLUME_LIST_PLIST: &str = r#"<?xml version="1.0"?><plist version="1.0"><dict>
      <key>Containers</key><array><dict>
        <key>PhysicalStores</key><array>
          <dict><key>DeviceIdentifier</key><string>disk4s1</string></dict>
        </array>
        <key>Volumes</key><array>
          <dict><key>DeviceIdentifier</key><string>disk5s2</string></dict>
          <dict><key>DeviceIdentifier</key><string>/dev/disk5s3</string></dict>
        </array>
      </dict></array>
    </dict></plist>"#;

    const DUPLICATE_CONTAINER_MATCH_PLIST: &str = r#"<?xml version="1.0"?><plist version="1.0"><dict>
      <key>Containers</key><array>
        <dict>
          <key>PhysicalStores</key><array>
            <dict><key>DeviceIdentifier</key><string>disk4s1</string></dict>
          </array>
          <key>Volumes</key><array>
            <dict><key>DeviceIdentifier</key><string>disk5s2</string></dict>
          </array>
        </dict>
        <dict>
          <key>PhysicalStores</key><array>
            <dict><key>DeviceIdentifier</key><string>disk4s1</string></dict>
          </array>
          <key>Volumes</key><array>
            <dict><key>DeviceIdentifier</key><string>disk6s2</string></dict>
          </array>
        </dict>
      </array>
    </dict></plist>"#;

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

    struct StatefulMalformedAttachRunner {
        requests: RefCell<Vec<CommandRequest>>,
        attached: RefCell<BTreeMap<String, BTreeSet<String>>>,
        image: String,
        format: ImageFormat,
        new_device: String,
        fail_detach: bool,
    }

    impl StatefulMalformedAttachRunner {
        fn new(image: &Path, format: ImageFormat, preexisting: &[&str], fail_detach: bool) -> Self {
            let image = attachment_inventory_path(image)
                .unwrap()
                .to_string_lossy()
                .into_owned();
            let mut attached = BTreeMap::new();
            attached.insert(
                image.clone(),
                preexisting
                    .iter()
                    .map(|device| (*device).to_owned())
                    .collect(),
            );
            attached.insert(
                "/tmp/cowshed-unrelated.asif".into(),
                BTreeSet::from(["/dev/disk20".into()]),
            );
            Self {
                requests: RefCell::new(Vec::new()),
                attached: RefCell::new(attached),
                image,
                format,
                new_device: match format {
                    ImageFormat::Asif => "/dev/disk8",
                    ImageFormat::Sparse => "/dev/disk9",
                }
                .into(),
                fail_detach,
            }
        }

        fn inventory(&self) -> String {
            let attached = self.attached.borrow();
            let mut plist =
                String::from(r#"<?xml version="1.0"?><plist><dict><key>images</key><array>"#);
            for (path, devices) in attached.iter().filter(|(_, devices)| !devices.is_empty()) {
                plist.push_str("<dict><key>image-path</key><string>");
                plist.push_str(path);
                plist.push_str("</string><key>system-entities</key><array>");
                for device in devices {
                    plist.push_str("<dict><key>dev-entry</key><string>");
                    plist.push_str(device);
                    plist.push_str("</string></dict>");
                }
                plist.push_str("</array></dict>");
            }
            plist.push_str("</array></dict></plist>");
            plist
        }

        fn requests(&self) -> Ref<'_, Vec<CommandRequest>> {
            self.requests.borrow()
        }

        fn devices_for(&self, image: &Path) -> BTreeSet<String> {
            let image = attachment_inventory_path(image)
                .unwrap()
                .to_string_lossy()
                .into_owned();
            self.attached
                .borrow()
                .get(&image)
                .cloned()
                .unwrap_or_default()
        }
    }

    impl CommandRunner for StatefulMalformedAttachRunner {
        fn run(&self, request: &CommandRequest) -> Result<CommandOutput, CommandRunError> {
            self.requests.borrow_mut().push(request.clone());
            let args = argv(request);
            if request.program == Path::new(HDIUTIL) && args == ["info", "-plist"] {
                return Ok(CommandOutput::success(self.inventory()));
            }
            if request.program == Path::new(DISKUTIL)
                && args.starts_with(&["image".into(), "create".into(), "blank".into()])
            {
                return Ok(CommandOutput::success([]));
            }
            let is_attach = match self.format {
                ImageFormat::Asif => {
                    request.program == Path::new(DISKUTIL)
                        && args.starts_with(&["image".into(), "attach".into()])
                }
                ImageFormat::Sparse => {
                    request.program == Path::new(HDIUTIL)
                        && args.first().is_some_and(|arg| arg == "attach")
                }
            };
            if is_attach {
                self.attached
                    .borrow_mut()
                    .entry(self.image.clone())
                    .or_default()
                    .insert(self.new_device.clone());
                return Ok(CommandOutput::success("<plist><dict><key>malformed"));
            }
            let is_detach = match self.format {
                ImageFormat::Asif => {
                    request.program == Path::new(DISKUTIL)
                        && args == ["eject", self.new_device.as_str()]
                }
                ImageFormat::Sparse => {
                    request.program == Path::new(HDIUTIL)
                        && args == ["detach", "-quiet", self.new_device.as_str()]
                }
            };
            assert!(is_detach, "unexpected command: {request:?}");
            if self.fail_detach {
                return Ok(CommandOutput::failure(16, "busy"));
            }
            self.attached
                .borrow_mut()
                .get_mut(&self.image)
                .expect("target image is inventoried")
                .remove(&self.new_device);
            Ok(CommandOutput::success([]))
        }
    }

    fn attachment_inventory(entries: &[(&str, &[&str])]) -> String {
        let mut plist =
            String::from(r#"<?xml version="1.0"?><plist><dict><key>images</key><array>"#);
        for (path, devices) in entries {
            plist.push_str("<dict><key>image-path</key><string>");
            plist.push_str(path);
            plist.push_str("</string><key>system-entities</key><array>");
            for device in *devices {
                plist.push_str("<dict><key>dev-entry</key><string>");
                plist.push_str(device);
                plist.push_str("</string></dict>");
            }
            plist.push_str("</array></dict>");
        }
        plist.push_str("</array></dict></plist>");
        plist
    }

    fn temp_path(label: &str, extension: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "cowshed-apfs-{label}-{}-{:?}.{extension}",
            std::process::id(),
            std::thread::current().id()
        ))
    }

    #[test]
    fn system_runner_reports_output_spawn_errors_and_signal_status() {
        let output = SystemCommandRunner
            .run(&CommandRequest::new(
                "/bin/sh",
                ["-c", "printf stdout; printf stderr >&2; exit 7"],
            ))
            .unwrap();
        assert_eq!(output.status, 7);
        assert_eq!(output.stdout, b"stdout");
        assert_eq!(output.stderr, b"stderr");

        let signaled = SystemCommandRunner
            .run(&CommandRequest::new("/bin/sh", ["-c", "kill -TERM $$"]))
            .unwrap();
        assert_eq!(signaled.status, -1);

        let missing = temp_path("missing-command", "bin");
        let error = SystemCommandRunner
            .run(&CommandRequest::new(
                &missing,
                std::iter::empty::<OsString>(),
            ))
            .unwrap_err();
        assert_eq!(error.program, missing);
        assert!(error.to_string().contains("could not run"));
        assert!(std::error::Error::source(&error).is_some());
    }

    #[test]
    fn typed_errors_preserve_messages_and_sources() {
        let clone = CloneFileError::Io {
            source_path: PathBuf::from("source.asif"),
            destination_path: PathBuf::from("destination.asif"),
            source: io::Error::new(io::ErrorKind::PermissionDenied, "clone denied"),
        };
        assert!(clone.to_string().contains("clone denied"));
        assert_eq!(
            std::error::Error::source(&clone).unwrap().to_string(),
            "clone denied"
        );

        let spawn = ApfsError::CommandSpawn(CommandRunError {
            program: PathBuf::from("/missing"),
            source: io::Error::new(io::ErrorKind::NotFound, "missing"),
        });
        assert!(spawn.to_string().contains("/missing"));
        assert!(std::error::Error::source(&spawn).is_some());

        let file = ApfsError::FileOperation {
            operation: "delete image",
            path: PathBuf::from("main.asif"),
            source: io::Error::new(io::ErrorKind::PermissionDenied, "denied"),
        };
        assert!(file.to_string().contains("delete image main.asif failed"));
        assert!(std::error::Error::source(&file).is_some());

        let clone = ApfsError::Clone(CloneFileError::DestinationExists {
            destination: PathBuf::from("session.asif"),
        });
        assert!(clone.to_string().contains("session.asif"));
        assert!(std::error::Error::source(&clone).is_some());

        let detach = ApfsError::CommandFailed {
            operation: "detach image",
            request: CommandRequest::new(DISKUTIL, ["eject", "/dev/disk4"]),
            output: CommandOutput::failure(1, "busy"),
        };
        let combined = ApfsError::VerificationAndDetachFailed {
            device: "/dev/disk4s1".into(),
            verification: CommandOutput::failure(8, "not clean"),
            detach: Box::new(detach),
        };
        assert!(combined.to_string().contains("detaching"));
        assert!(std::error::Error::source(&combined).is_some());
        assert_eq!(
            ApfsError::InvalidAttachmentInventory("bad shape".into()).to_string(),
            "invalid attachment inventory: bad shape"
        );
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
    fn asif_attach_uses_read_only_inventory_then_diskutil_attach_and_fsck() {
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
            CommandOutput::success(EMPTY_ATTACHMENT_INVENTORY),
            CommandOutput::success(PLIST),
            CommandOutput::success(VOLUME_LIST_PLIST),
            CommandOutput::success([]),
            CommandOutput::success([]),
        ]));
        let attachment = backend
            .attach_verified(Path::new("session.asif"), ImageFormat::Asif)
            .unwrap();
        assert_eq!(attachment.image(), Path::new("session.asif"));
        assert_eq!(attachment.format(), ImageFormat::Asif);
        assert_eq!(attachment.whole_device(), "/dev/disk10");
        assert_eq!(attachment.volume_device(), "/dev/disk10s1");
        let mount = std::env::temp_dir().join(format!("cowshed-apfs-test-{}", std::process::id()));
        backend.mount(&attachment, &mount, false).unwrap();
        let requests = backend.runner().requests();
        assert_eq!(
            requests
                .iter()
                .map(|request| request.program.as_path())
                .collect::<Vec<_>>(),
            [
                Path::new(HDIUTIL),
                Path::new(DISKUTIL),
                Path::new(DISKUTIL),
                Path::new(FSCK_APFS),
                Path::new(DISKUTIL),
            ]
        );
        assert_eq!(argv(&requests[0]), ["info", "-plist"]);
        assert_eq!(
            argv(&requests[1])[..5],
            ["image", "attach", "--nobrowse", "--noMount", "--plist"]
        );
        assert_eq!(argv(&requests[2]), ["apfs", "list", "-plist"]);
        assert_eq!(argv(&requests[3]), ["-q", "/dev/rdisk10s1"]);
        assert_eq!(
            argv(&requests[4])[..5],
            [
                "mount",
                "nobrowse",
                "-mountOptions",
                "owners",
                "-mountPoint"
            ]
        );
        assert_eq!(argv(&requests[4]).last().unwrap(), "/dev/disk10s1");
        let _ = fs::remove_dir(mount);
    }

    #[test]
    fn mount_always_enables_owners_and_preserves_browse_selection() {
        let attachment = AttachedImage {
            image: PathBuf::from("session.sparseimage"),
            format: ImageFormat::Sparse,
            whole_device: "/dev/disk4".into(),
            volume_device: "/dev/disk5s2".into(),
        };
        for browse in [false, true] {
            let backend =
                MacOsApfsBackend::new(RecordingRunner::with_outputs([CommandOutput::success([])]));
            let mount = temp_path(
                if browse {
                    "mount-browse"
                } else {
                    "mount-nobrowse"
                },
                "mount",
            );
            backend.mount(&attachment, &mount, browse).unwrap();

            let mut expected = vec!["mount".to_owned()];
            if !browse {
                expected.push("nobrowse".to_owned());
            }
            expected.extend([
                "-mountOptions".to_owned(),
                "owners".to_owned(),
                "-mountPoint".to_owned(),
                mount.to_string_lossy().into_owned(),
                "/dev/disk5s2".to_owned(),
            ]);
            let requests = backend.runner().requests();
            assert_eq!(requests.len(), 1);
            assert_eq!(requests[0].program, Path::new(DISKUTIL));
            assert_eq!(argv(&requests[0]), expected);
            fs::remove_dir(mount).unwrap();
        }
    }

    #[test]
    fn sparse_attach_resolves_volume_via_diskutil_before_raw_fsck() {
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
            CommandOutput::success(EMPTY_ATTACHMENT_INVENTORY),
            CommandOutput::success(SPARSE_ATTACH_PLIST),
            CommandOutput::success(SPARSE_VOLUME_LIST_PLIST),
            CommandOutput::success([]),
        ]));
        let attachment = backend
            .attach_verified(Path::new("session.sparseimage"), ImageFormat::Sparse)
            .unwrap();
        assert_eq!(attachment.whole_device(), "/dev/disk4");
        assert_eq!(attachment.volume_device(), "/dev/disk5s2");
        let requests = backend.runner().requests();
        assert_eq!(
            requests
                .iter()
                .map(|request| request.program.as_path())
                .collect::<Vec<_>>(),
            [
                Path::new(HDIUTIL),
                Path::new(HDIUTIL),
                Path::new(DISKUTIL),
                Path::new(FSCK_APFS)
            ]
        );
        assert_eq!(argv(&requests[0]), ["info", "-plist"]);
        assert_eq!(
            argv(&requests[1]),
            [
                "attach",
                "-nobrowse",
                "-owners",
                "on",
                "-nomount",
                "-plist",
                "session.sparseimage",
            ]
        );
        assert_eq!(argv(&requests[2]), ["apfs", "list", "-plist"]);
        assert_eq!(argv(&requests[3]), ["-q", "/dev/rdisk5s2"]);
    }

    #[test]
    fn failed_verification_detaches_the_whole_sparse_image_device() {
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
            CommandOutput::success(EMPTY_ATTACHMENT_INVENTORY),
            CommandOutput::success(SPARSE_ATTACH_PLIST),
            CommandOutput::success(SPARSE_VOLUME_LIST_PLIST),
            CommandOutput::failure(8, "not clean"),
            CommandOutput::success([]),
        ]));
        let error = backend
            .attach_verified(Path::new("session.sparseimage"), ImageFormat::Sparse)
            .unwrap_err();
        assert!(matches!(
            error,
            ApfsError::VerificationFailed {
                device,
                output: CommandOutput { status: 8, .. },
            } if device == "/dev/disk5s2"
        ));
        let requests = backend.runner().requests();
        assert_eq!(requests.len(), 5);
        assert_eq!(requests[3].program, Path::new(FSCK_APFS));
        assert_eq!(argv(&requests[3]), ["-q", "/dev/rdisk5s2"]);
        assert_eq!(requests[4].program, Path::new(HDIUTIL));
        assert_eq!(argv(&requests[4]), ["detach", "-quiet", "/dev/disk4"]);
        assert!(
            !requests
                .iter()
                .any(|request| argv(request).first().is_some_and(|arg| arg == "mount"))
        );
    }

    #[test]
    fn missing_volume_resolution_detaches_the_whole_image_before_failing() {
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
            CommandOutput::success(EMPTY_ATTACHMENT_INVENTORY),
            CommandOutput::success(SPARSE_ATTACH_PLIST),
            CommandOutput::success(EMPTY_VOLUME_LIST_PLIST),
            CommandOutput::success([]),
        ]));
        let error = backend
            .attach_verified(Path::new("session.sparseimage"), ImageFormat::Sparse)
            .unwrap_err();

        assert!(matches!(
            error,
            ApfsError::VolumeResolutionFailed {
                candidate,
                reason: VolumeResolutionFailure::Missing,
            } if candidate == "/dev/disk4s1"
        ));
        let requests = backend.runner().requests();
        assert_eq!(requests.len(), 4);
        assert_eq!(argv(&requests[2]), ["apfs", "list", "-plist"]);
        assert_eq!(requests[3].program, Path::new(HDIUTIL));
        assert_eq!(argv(&requests[3]), ["detach", "-quiet", "/dev/disk4"]);
    }

    #[test]
    fn ambiguous_volume_and_detach_failures_preserve_both_typed_errors() {
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
            CommandOutput::success(EMPTY_ATTACHMENT_INVENTORY),
            CommandOutput::success(SPARSE_ATTACH_PLIST),
            CommandOutput::success(AMBIGUOUS_VOLUME_LIST_PLIST),
            CommandOutput::failure(16, "busy"),
        ]));
        let error = backend
            .attach_verified(Path::new("session.sparseimage"), ImageFormat::Sparse)
            .unwrap_err();

        assert!(std::error::Error::source(&error).is_some());
        match error {
            ApfsError::VolumeResolutionAndDetachFailed {
                whole_device,
                resolution,
                detach,
            } => {
                assert_eq!(whole_device, "/dev/disk4");
                assert!(matches!(
                    *resolution,
                    ApfsError::VolumeResolutionFailed {
                        candidate,
                        reason: VolumeResolutionFailure::Ambiguous(devices),
                    } if candidate == "/dev/disk4s1"
                        && devices == ["/dev/disk5s2", "/dev/disk5s3"]
                ));
                assert!(matches!(
                    *detach,
                    ApfsError::CommandFailed {
                        operation: "detach image",
                        output: CommandOutput { status: 16, .. },
                        ..
                    }
                ));
            }
            other => panic!("unexpected error: {other}"),
        }
        let requests = backend.runner().requests();
        assert_eq!(requests.len(), 4);
        assert_eq!(argv(&requests[3]), ["detach", "-quiet", "/dev/disk4"]);
    }

    #[test]
    fn failed_volume_resolution_command_detaches_and_preserves_command_error() {
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
            CommandOutput::success(EMPTY_ATTACHMENT_INVENTORY),
            CommandOutput::success(SPARSE_ATTACH_PLIST),
            CommandOutput::failure(3, "list failed"),
            CommandOutput::success([]),
        ]));
        let error = backend
            .attach_verified(Path::new("session.sparseimage"), ImageFormat::Sparse)
            .unwrap_err();

        assert!(matches!(
            error,
            ApfsError::CommandFailed {
                operation: "resolve APFS volume",
                output: CommandOutput { status: 3, .. },
                ..
            }
        ));
        let requests = backend.runner().requests();
        assert_eq!(requests.len(), 4);
        assert_eq!(argv(&requests[3]), ["detach", "-quiet", "/dev/disk4"]);
    }

    #[test]
    fn parsed_attach_never_detaches_a_preexisting_same_image_device() {
        let image = Path::new("/tmp/cowshed-preexisting.asif");
        let inventory =
            attachment_inventory(&[("/tmp/cowshed-preexisting.asif", &["/dev/disk10"][..])]);
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
            CommandOutput::success(inventory.as_bytes()),
            CommandOutput::success(PLIST),
            CommandOutput::success(inventory.as_bytes()),
            CommandOutput::success(inventory.as_bytes()),
        ]));

        let error = backend
            .attach_verified(image, ImageFormat::Asif)
            .unwrap_err();

        assert!(matches!(
            error,
            ApfsError::InvalidAttachmentPlist(message)
                if message == "attach reported a pre-existing whole image device"
        ));
        let requests = backend.runner().requests();
        assert_eq!(requests.len(), 4);
        assert_eq!(argv(&requests[0]), ["info", "-plist"]);
        assert_eq!(argv(&requests[2]), ["info", "-plist"]);
        assert_eq!(argv(&requests[3]), ["info", "-plist"]);
        assert!(
            !requests
                .iter()
                .any(|request| argv(request).first().is_some_and(|arg| arg == "eject"))
        );
    }

    #[test]
    fn malformed_asif_attach_detaches_only_the_new_device_and_verifies_absence() {
        let image = Path::new("/tmp/cowshed-malformed-attach.asif");
        let backend = MacOsApfsBackend::new(StatefulMalformedAttachRunner::new(
            image,
            ImageFormat::Asif,
            &["/dev/disk4"],
            false,
        ));

        let error = backend
            .attach_verified(image, ImageFormat::Asif)
            .unwrap_err();

        assert!(matches!(error, ApfsError::InvalidAttachmentPlist(_)));
        assert_eq!(
            backend.runner().devices_for(image),
            BTreeSet::from(["/dev/disk4".into()])
        );
        assert_eq!(
            backend
                .runner()
                .devices_for(Path::new("/tmp/cowshed-unrelated.asif")),
            BTreeSet::from(["/dev/disk20".into()])
        );
        let requests = backend.runner().requests();
        assert_eq!(requests.len(), 5);
        assert_eq!(argv(&requests[0]), ["info", "-plist"]);
        assert_eq!(
            argv(&requests[1]),
            [
                "image",
                "attach",
                "--nobrowse",
                "--noMount",
                "--plist",
                "/tmp/cowshed-malformed-attach.asif",
            ]
        );
        assert_eq!(argv(&requests[2]), ["info", "-plist"]);
        assert_eq!(argv(&requests[3]), ["eject", "/dev/disk8"]);
        assert_eq!(argv(&requests[4]), ["info", "-plist"]);
        assert!(!requests.iter().any(|request| {
            let args = argv(request);
            args.iter()
                .any(|arg| arg == "/dev/disk4" || arg == "/dev/disk20")
        }));
    }

    #[test]
    fn malformed_sparse_attach_detaches_the_new_device_and_verifies_absence() {
        let image = Path::new("/tmp/cowshed-malformed-attach.sparseimage");
        let backend = MacOsApfsBackend::new(StatefulMalformedAttachRunner::new(
            image,
            ImageFormat::Sparse,
            &[],
            false,
        ));

        let error = backend
            .attach_verified(image, ImageFormat::Sparse)
            .unwrap_err();

        assert!(matches!(error, ApfsError::InvalidAttachmentPlist(_)));
        assert!(backend.runner().devices_for(image).is_empty());
        assert_eq!(
            backend
                .runner()
                .devices_for(Path::new("/tmp/cowshed-unrelated.asif")),
            BTreeSet::from(["/dev/disk20".into()])
        );
        let requests = backend.runner().requests();
        assert_eq!(requests.len(), 5);
        assert_eq!(argv(&requests[0]), ["info", "-plist"]);
        assert_eq!(
            argv(&requests[1]),
            [
                "attach",
                "-nobrowse",
                "-owners",
                "on",
                "-nomount",
                "-plist",
                "/tmp/cowshed-malformed-attach.sparseimage",
            ]
        );
        assert_eq!(argv(&requests[2]), ["info", "-plist"]);
        assert_eq!(argv(&requests[3]), ["detach", "-quiet", "/dev/disk9"]);
        assert_eq!(argv(&requests[4]), ["info", "-plist"]);
    }

    #[test]
    fn malformed_blank_asif_cleanup_failure_preserves_image_and_typed_context() {
        let stem = temp_path("malformed-blank-cleanup", "stem").with_extension("");
        let image = stem.with_extension(ImageFormat::Asif.extension());
        fs::write(&image, b"created").unwrap();
        let backend = MacOsApfsBackend::new(StatefulMalformedAttachRunner::new(
            &image,
            ImageFormat::Asif,
            &[],
            true,
        ));

        let error = backend
            .create_staged_image(&CreateImageRequest {
                staged_stem: stem,
                capacity: "5g".into(),
                volume_name: "main".into(),
                case_sensitivity: ApfsCaseSensitivity::Insensitive,
                image_format: ImageFormatSelection::Exact(ImageFormat::Asif),
                owner_uid: 502,
                owner_gid: 20,
            })
            .unwrap_err();

        assert!(image.exists());
        assert_eq!(
            backend.runner().devices_for(&image),
            BTreeSet::from(["/dev/disk8".into()])
        );
        assert!(
            error
                .to_string()
                .contains("cleaning up newly attached devices also failed")
        );
        assert!(std::error::Error::source(&error).is_some());
        match error {
            ApfsError::AttachmentCleanupFailed {
                image: failed_image,
                primary,
                cleanup,
            } => {
                assert_eq!(failed_image, image);
                assert!(matches!(*primary, ApfsError::InvalidAttachmentPlist(_)));
                assert!(cleanup.inventory.is_none());
                assert_eq!(cleanup.detach.len(), 1);
                assert_eq!(cleanup.detach[0].device, "/dev/disk8");
                assert!(matches!(
                    cleanup.detach[0].error.as_ref(),
                    ApfsError::CommandFailed {
                        operation: "detach image",
                        output: CommandOutput { status: 16, .. },
                        ..
                    }
                ));
                assert_eq!(cleanup.remaining_devices, ["/dev/disk8"]);
            }
            other => panic!("unexpected error: {other}"),
        }
        let requests = backend.runner().requests();
        assert_eq!(requests.len(), 6);
        assert_eq!(argv(&requests[0])[..3], ["image", "create", "blank"]);
        assert_eq!(argv(&requests[1]), ["info", "-plist"]);
        assert_eq!(argv(&requests[3]), ["info", "-plist"]);
        assert_eq!(argv(&requests[4]), ["eject", "/dev/disk8"]);
        assert_eq!(argv(&requests[5]), ["info", "-plist"]);
        fs::remove_file(image).unwrap();
    }

    #[test]
    fn failed_detach_remains_a_cleanup_error_when_inventory_reports_absence() {
        let image = Path::new("/tmp/cowshed-failed-detach.asif");
        let attached =
            attachment_inventory(&[("/tmp/cowshed-failed-detach.asif", &["/dev/disk8"][..])]);
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
            CommandOutput::success(EMPTY_ATTACHMENT_INVENTORY),
            CommandOutput::success("<plist><dict><key>malformed"),
            CommandOutput::success(attached.as_bytes()),
            CommandOutput::failure(16, "busy after eject"),
            CommandOutput::success(EMPTY_ATTACHMENT_INVENTORY),
        ]));

        let error = backend
            .attach_verified(image, ImageFormat::Asif)
            .unwrap_err();

        match error {
            ApfsError::AttachmentCleanupFailed {
                primary, cleanup, ..
            } => {
                assert!(matches!(*primary, ApfsError::InvalidAttachmentPlist(_)));
                assert!(cleanup.inventory.is_none());
                assert_eq!(cleanup.detach.len(), 1);
                assert_eq!(cleanup.detach[0].device, "/dev/disk8");
                assert!(cleanup.remaining_devices.is_empty());
            }
            other => panic!("unexpected error: {other}"),
        }
        let requests = backend.runner().requests();
        assert_eq!(requests.len(), 5);
        assert_eq!(argv(&requests[3]), ["eject", "/dev/disk8"]);
        assert_eq!(argv(&requests[4]), ["info", "-plist"]);
    }

    #[test]
    fn malformed_blank_asif_inventory_failures_preserve_the_image() {
        let cleanup_outputs = [
            CommandOutput::failure(5, "inventory unavailable"),
            CommandOutput::success("not a plist"),
        ];
        for (index, cleanup_output) in cleanup_outputs.into_iter().enumerate() {
            let stem =
                temp_path(&format!("malformed-inventory-{index}"), "stem").with_extension("");
            let image = stem.with_extension(ImageFormat::Asif.extension());
            fs::write(&image, b"created").unwrap();
            let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
                CommandOutput::success([]),
                CommandOutput::success(EMPTY_ATTACHMENT_INVENTORY),
                CommandOutput::success("<plist><dict><key>malformed"),
                cleanup_output,
            ]));

            let error = backend
                .create_staged_image(&CreateImageRequest {
                    staged_stem: stem,
                    capacity: "5g".into(),
                    volume_name: "main".into(),
                    case_sensitivity: ApfsCaseSensitivity::Insensitive,
                    image_format: ImageFormatSelection::Exact(ImageFormat::Asif),
                    owner_uid: 502,
                    owner_gid: 20,
                })
                .unwrap_err();

            assert!(image.exists());
            match error {
                ApfsError::AttachmentCleanupFailed {
                    primary, cleanup, ..
                } => {
                    assert!(matches!(*primary, ApfsError::InvalidAttachmentPlist(_)));
                    let inventory = cleanup.inventory.expect("inventory failure is retained");
                    if index == 0 {
                        assert!(matches!(
                            *inventory,
                            ApfsError::CommandFailed {
                                operation: "inventory attached disk images",
                                output: CommandOutput { status: 5, .. },
                                ..
                            }
                        ));
                    } else {
                        assert!(matches!(
                            *inventory,
                            ApfsError::InvalidAttachmentInventory(_)
                        ));
                    }
                    assert!(cleanup.detach.is_empty());
                    assert!(cleanup.remaining_devices.is_empty());
                }
                other => panic!("unexpected error: {other}"),
            }
            assert_eq!(backend.runner().requests().len(), 4);
            fs::remove_file(image).unwrap();
        }
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
                image_format: ImageFormatSelection::Auto,
                owner_uid: 502,
                owner_gid: 20,
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
    fn auto_tahoe_create_records_staged_asif_formatting_commands() {
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
            CommandOutput::success("26.0\n"),
            CommandOutput::success([]),
            CommandOutput::success(EMPTY_ATTACHMENT_INVENTORY),
            CommandOutput::success(BLANK_ASIF_PLIST),
            CommandOutput::success([]),
            CommandOutput::success([]),
        ]));
        let created = backend
            .create_staged_image(&CreateImageRequest {
                staged_stem: PathBuf::from(".staging/auto"),
                capacity: "5g".into(),
                volume_name: "main".into(),
                case_sensitivity: ApfsCaseSensitivity::Insensitive,
                image_format: ImageFormatSelection::Auto,
                owner_uid: 502,
                owner_gid: 20,
            })
            .unwrap();

        assert_eq!(
            created,
            CreatedImage {
                path: PathBuf::from(".staging/auto.asif"),
                format: ImageFormat::Asif,
            }
        );
        let requests = backend.runner().requests();
        assert_eq!(
            requests
                .iter()
                .map(|request| request.program.as_path())
                .collect::<Vec<_>>(),
            [
                Path::new(SW_VERS),
                Path::new(DISKUTIL),
                Path::new(HDIUTIL),
                Path::new(DISKUTIL),
                Path::new(NEWFS_APFS),
                Path::new(DISKUTIL),
            ]
        );
        assert_eq!(
            argv(&requests[1]),
            [
                "image",
                "create",
                "blank",
                "--format",
                "ASIF",
                "--size",
                "5g",
                "--volumeName",
                "main",
                "--fs",
                "None",
                ".staging/auto.asif",
            ]
        );
        assert_eq!(argv(&requests[2]), ["info", "-plist"]);
        assert_eq!(
            argv(&requests[3]),
            [
                "image",
                "attach",
                "--nobrowse",
                "--noMount",
                "--plist",
                ".staging/auto.asif",
            ]
        );
        assert_eq!(
            argv(&requests[4]),
            ["-U", "502", "-G", "20", "-i", "-v", "main", "/dev/disk8"]
        );
        assert_eq!(argv(&requests[5]), ["eject", "force", "/dev/disk8"]);
    }

    #[test]
    fn auto_case_sensitive_create_records_only_sparse_command() {
        let backend =
            MacOsApfsBackend::new(RecordingRunner::with_outputs([CommandOutput::success([])]));
        let created = backend
            .create_staged_image(&CreateImageRequest {
                staged_stem: PathBuf::from(".staging/sensitive"),
                capacity: "8g".into(),
                volume_name: "main".into(),
                case_sensitivity: ApfsCaseSensitivity::Sensitive,
                image_format: ImageFormatSelection::Auto,
                owner_uid: 502,
                owner_gid: 20,
            })
            .unwrap();

        assert_eq!(
            created,
            CreatedImage {
                path: PathBuf::from(".staging/sensitive.sparseimage"),
                format: ImageFormat::Sparse,
            }
        );
        let requests = backend.runner().requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].program, Path::new(HDIUTIL));
        assert_eq!(
            argv(&requests[0]),
            [
                "create",
                "-quiet",
                "-size",
                "8g",
                "-type",
                "SPARSE",
                "-fs",
                "Case-sensitive APFS",
                "-volname",
                "main",
                "-nospotlight",
                ".staging/sensitive.sparseimage",
            ]
        );
    }

    #[test]
    fn exact_asif_records_unprivileged_formatting_and_extension() {
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
            CommandOutput::success([]),
            CommandOutput::success(EMPTY_ATTACHMENT_INVENTORY),
            CommandOutput::success(BLANK_ASIF_PLIST),
            CommandOutput::success([]),
            CommandOutput::success([]),
        ]));
        let created = backend
            .create_staged_image(&CreateImageRequest {
                staged_stem: PathBuf::from(".staging/exact"),
                capacity: "5g".into(),
                volume_name: "main".into(),
                case_sensitivity: ApfsCaseSensitivity::Insensitive,
                image_format: ImageFormatSelection::Exact(ImageFormat::Asif),
                owner_uid: 501,
                owner_gid: 80,
            })
            .unwrap();

        assert_eq!(
            created,
            CreatedImage {
                path: PathBuf::from(".staging/exact.asif"),
                format: ImageFormat::Asif,
            }
        );
        let requests = backend.runner().requests();
        assert_eq!(requests.len(), 5);
        assert_eq!(
            requests
                .iter()
                .map(|request| request.program.as_path())
                .collect::<Vec<_>>(),
            [
                Path::new(DISKUTIL),
                Path::new(HDIUTIL),
                Path::new(DISKUTIL),
                Path::new(NEWFS_APFS),
                Path::new(DISKUTIL),
            ]
        );
        assert_eq!(
            argv(&requests[3]),
            ["-U", "501", "-G", "80", "-i", "-v", "main", "/dev/disk8"]
        );
        assert_eq!(argv(&requests[4]), ["eject", "force", "/dev/disk8"]);
    }

    #[test]
    fn exact_sparse_records_only_hdiutil_command_and_extension() {
        let backend =
            MacOsApfsBackend::new(RecordingRunner::with_outputs([CommandOutput::success([])]));
        let created = backend
            .create_staged_image(&CreateImageRequest {
                staged_stem: PathBuf::from(".staging/exact"),
                capacity: "8g".into(),
                volume_name: "main".into(),
                case_sensitivity: ApfsCaseSensitivity::Sensitive,
                image_format: ImageFormatSelection::Exact(ImageFormat::Sparse),
                owner_uid: 502,
                owner_gid: 20,
            })
            .unwrap();

        assert_eq!(
            created,
            CreatedImage {
                path: PathBuf::from(".staging/exact.sparseimage"),
                format: ImageFormat::Sparse,
            }
        );
        let requests = backend.runner().requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].program, Path::new(HDIUTIL));
        assert_eq!(
            argv(&requests[0]),
            [
                "create",
                "-quiet",
                "-size",
                "8g",
                "-type",
                "SPARSE",
                "-fs",
                "Case-sensitive APFS",
                "-volname",
                "main",
                "-nospotlight",
                ".staging/exact.sparseimage",
            ]
        );
    }

    #[test]
    fn exact_asif_unsupported_failure_never_falls_back() {
        let backend =
            MacOsApfsBackend::new(RecordingRunner::with_outputs([CommandOutput::failure(
                1,
                "ASIF format is not supported",
            )]));
        let error = backend
            .create_staged_image(&CreateImageRequest {
                staged_stem: PathBuf::from(".staging/exact"),
                capacity: "5g".into(),
                volume_name: "main".into(),
                case_sensitivity: ApfsCaseSensitivity::Insensitive,
                image_format: ImageFormatSelection::Exact(ImageFormat::Asif),
                owner_uid: 502,
                owner_gid: 20,
            })
            .unwrap_err();

        assert!(matches!(
            error,
            ApfsError::CommandFailed {
                operation: "create ASIF image",
                output: CommandOutput { status: 1, .. },
                ..
            }
        ));
        let requests = backend.runner().requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].program, Path::new(DISKUTIL));
    }

    #[test]
    fn exact_sparse_failure_never_probes_or_falls_back() {
        let backend =
            MacOsApfsBackend::new(RecordingRunner::with_outputs([CommandOutput::failure(
                9,
                "create failed",
            )]));
        let error = backend
            .create_staged_image(&CreateImageRequest {
                staged_stem: PathBuf::from(".staging/exact"),
                capacity: "5g".into(),
                volume_name: "main".into(),
                case_sensitivity: ApfsCaseSensitivity::Insensitive,
                image_format: ImageFormatSelection::Exact(ImageFormat::Sparse),
                owner_uid: 502,
                owner_gid: 20,
            })
            .unwrap_err();

        assert!(matches!(
            error,
            ApfsError::CommandFailed {
                operation: "create SPARSE image",
                output: CommandOutput { status: 9, .. },
                ..
            }
        ));
        let requests = backend.runner().requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].program, Path::new(HDIUTIL));
    }

    #[test]
    fn exact_asif_case_sensitive_creation_uses_newfs_e_flag() {
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
            CommandOutput::success([]),
            CommandOutput::success(EMPTY_ATTACHMENT_INVENTORY),
            CommandOutput::success(BLANK_ASIF_PLIST),
            CommandOutput::success([]),
            CommandOutput::success([]),
        ]));
        let created = backend
            .create_staged_image(&CreateImageRequest {
                staged_stem: PathBuf::from(".staging/sensitive-asif"),
                capacity: "5g".into(),
                volume_name: "sensitive".into(),
                case_sensitivity: ApfsCaseSensitivity::Sensitive,
                image_format: ImageFormatSelection::Exact(ImageFormat::Asif),
                owner_uid: 777,
                owner_gid: 88,
            })
            .unwrap();

        assert_eq!(created.format, ImageFormat::Asif);
        assert_eq!(created.path, Path::new(".staging/sensitive-asif.asif"));
        let requests = backend.runner().requests();
        assert_eq!(requests.len(), 5);
        assert_eq!(
            argv(&requests[3]),
            [
                "-U",
                "777",
                "-G",
                "88",
                "-e",
                "-v",
                "sensitive",
                "/dev/disk8",
            ]
        );
    }

    #[test]
    fn post_create_asif_attach_failure_never_falls_back_and_removes_image() {
        let stem = temp_path("asif-attach-failure", "stem").with_extension("");
        let image = stem.with_extension(ImageFormat::Asif.extension());
        fs::write(&image, b"partial").unwrap();
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
            CommandOutput::success("26.0\n"),
            CommandOutput::success([]),
            CommandOutput::success(EMPTY_ATTACHMENT_INVENTORY),
            CommandOutput::failure(1, "unsupported after create"),
            CommandOutput::success(EMPTY_ATTACHMENT_INVENTORY),
            CommandOutput::success(EMPTY_ATTACHMENT_INVENTORY),
        ]));
        let error = backend
            .create_staged_image(&CreateImageRequest {
                staged_stem: stem,
                capacity: "5g".into(),
                volume_name: "main".into(),
                case_sensitivity: ApfsCaseSensitivity::Insensitive,
                image_format: ImageFormatSelection::Auto,
                owner_uid: 502,
                owner_gid: 20,
            })
            .unwrap_err();

        assert!(matches!(
            error,
            ApfsError::CommandFailed {
                operation: "attach blank ASIF image",
                ..
            }
        ));
        assert!(!image.exists());
        let requests = backend.runner().requests();
        assert_eq!(requests.len(), 6);
        assert_eq!(argv(&requests[2]), ["info", "-plist"]);
        assert_eq!(
            requests
                .iter()
                .filter(|request| argv(request) == ["info", "-plist"])
                .count(),
            3
        );
    }

    #[test]
    fn post_create_cleanup_treats_a_missing_staged_image_as_already_removed() {
        let stem = temp_path("asif-missing-cleanup", "stem").with_extension("");
        let image = stem.with_extension(ImageFormat::Asif.extension());
        assert!(!image.exists());
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
            CommandOutput::success([]),
            CommandOutput::success(EMPTY_ATTACHMENT_INVENTORY),
            CommandOutput::failure(9, "attach failed"),
            CommandOutput::success(EMPTY_ATTACHMENT_INVENTORY),
            CommandOutput::success(EMPTY_ATTACHMENT_INVENTORY),
        ]));
        let error = backend
            .create_staged_image(&CreateImageRequest {
                staged_stem: stem,
                capacity: "5g".into(),
                volume_name: "main".into(),
                case_sensitivity: ApfsCaseSensitivity::Insensitive,
                image_format: ImageFormatSelection::Exact(ImageFormat::Asif),
                owner_uid: 502,
                owner_gid: 20,
            })
            .unwrap_err();

        assert!(matches!(
            error,
            ApfsError::CommandFailed {
                operation: "attach blank ASIF image",
                output: CommandOutput { status: 9, .. },
                ..
            }
        ));
        assert_eq!(backend.runner().requests().len(), 5);
    }

    #[test]
    fn failed_newfs_detaches_and_removes_staged_asif() {
        let stem = temp_path("asif-newfs-failure", "stem").with_extension("");
        let image = stem.with_extension(ImageFormat::Asif.extension());
        fs::write(&image, b"partial").unwrap();
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
            CommandOutput::success([]),
            CommandOutput::success(EMPTY_ATTACHMENT_INVENTORY),
            CommandOutput::success(BLANK_ASIF_PLIST),
            CommandOutput::failure(70, "format failed"),
            CommandOutput::success([]),
        ]));
        let error = backend
            .create_staged_image(&CreateImageRequest {
                staged_stem: stem,
                capacity: "5g".into(),
                volume_name: "main".into(),
                case_sensitivity: ApfsCaseSensitivity::Insensitive,
                image_format: ImageFormatSelection::Exact(ImageFormat::Asif),
                owner_uid: 502,
                owner_gid: 20,
            })
            .unwrap_err();

        assert!(matches!(
            error,
            ApfsError::CommandFailed {
                operation: "format ASIF APFS volume",
                output: CommandOutput { status: 70, .. },
                ..
            }
        ));
        assert!(!image.exists());
        let requests = backend.runner().requests();
        assert_eq!(requests.len(), 5);
        assert_eq!(requests[3].program, Path::new(NEWFS_APFS));
        assert_eq!(argv(&requests[4]), ["eject", "force", "/dev/disk8"]);
    }

    #[test]
    fn failed_newfs_preserves_image_when_detach_cleanup_fails() {
        let stem = temp_path("asif-detach-cleanup-failure", "stem").with_extension("");
        let image = stem.with_extension(ImageFormat::Asif.extension());
        fs::write(&image, b"partial").unwrap();
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
            CommandOutput::success([]),
            CommandOutput::success(EMPTY_ATTACHMENT_INVENTORY),
            CommandOutput::success(BLANK_ASIF_PLIST),
            CommandOutput::failure(70, "format failed"),
            CommandOutput::failure(16, "busy"),
        ]));
        let error = backend
            .create_staged_image(&CreateImageRequest {
                staged_stem: stem,
                capacity: "5g".into(),
                volume_name: "main".into(),
                case_sensitivity: ApfsCaseSensitivity::Insensitive,
                image_format: ImageFormatSelection::Exact(ImageFormat::Asif),
                owner_uid: 502,
                owner_gid: 20,
            })
            .unwrap_err();

        assert!(std::error::Error::source(&error).is_some());
        assert!(matches!(
            error,
            ApfsError::AsifCreationAndCleanupFailed {
                primary,
                detach: Some(detach),
                remove: None,
            } if matches!(
                *primary,
                ApfsError::CommandFailed {
                    operation: "format ASIF APFS volume",
                    ..
                }
            ) && matches!(
                *detach,
                ApfsError::CommandFailed {
                    operation: "detach image",
                    output: CommandOutput { status: 16, .. },
                    ..
                }
            )
        ));
        assert!(image.exists());
        fs::remove_file(image).unwrap();
    }

    #[test]
    fn failed_newfs_preserves_remove_cleanup_failure_after_detach() {
        let stem = temp_path("asif-remove-cleanup-failure", "stem").with_extension("");
        let image = stem.with_extension(ImageFormat::Asif.extension());
        fs::create_dir(&image).unwrap();
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
            CommandOutput::success([]),
            CommandOutput::success(EMPTY_ATTACHMENT_INVENTORY),
            CommandOutput::success(BLANK_ASIF_PLIST),
            CommandOutput::failure(70, "format failed"),
            CommandOutput::success([]),
        ]));
        let error = backend
            .create_staged_image(&CreateImageRequest {
                staged_stem: stem,
                capacity: "5g".into(),
                volume_name: "main".into(),
                case_sensitivity: ApfsCaseSensitivity::Insensitive,
                image_format: ImageFormatSelection::Exact(ImageFormat::Asif),
                owner_uid: 502,
                owner_gid: 20,
            })
            .unwrap_err();

        assert!(matches!(
            error,
            ApfsError::AsifCreationAndCleanupFailed {
                primary,
                detach: None,
                remove: Some(remove),
            } if matches!(
                *primary,
                ApfsError::CommandFailed {
                    operation: "format ASIF APFS volume",
                    ..
                }
            ) && matches!(
                *remove,
                ApfsError::FileOperation {
                    operation: "remove failed ASIF image",
                    ..
                }
            )
        ));
        fs::remove_dir(image).unwrap();
    }

    #[test]
    fn failed_final_asif_eject_preserves_attached_image() {
        let stem = temp_path("asif-final-eject-failure", "stem").with_extension("");
        let image = stem.with_extension(ImageFormat::Asif.extension());
        fs::write(&image, b"formatted").unwrap();
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
            CommandOutput::success([]),
            CommandOutput::success(EMPTY_ATTACHMENT_INVENTORY),
            CommandOutput::success(BLANK_ASIF_PLIST),
            CommandOutput::success([]),
            CommandOutput::failure(16, "busy"),
        ]));
        let error = backend
            .create_staged_image(&CreateImageRequest {
                staged_stem: stem,
                capacity: "5g".into(),
                volume_name: "main".into(),
                case_sensitivity: ApfsCaseSensitivity::Insensitive,
                image_format: ImageFormatSelection::Exact(ImageFormat::Asif),
                owner_uid: 502,
                owner_gid: 20,
            })
            .unwrap_err();

        assert!(matches!(
            error,
            ApfsError::CommandFailed {
                operation: "detach image",
                output: CommandOutput { status: 16, .. },
                ..
            }
        ));
        assert!(image.exists());
        fs::remove_file(image).unwrap();
    }

    #[test]
    fn sparse_compaction_records_checked_hdiutil_command() {
        let backend =
            MacOsApfsBackend::new(RecordingRunner::with_outputs([CommandOutput::success([])]));
        backend
            .compact_image(Path::new("main.sparseimage"), ImageFormat::Sparse)
            .unwrap();

        let requests = backend.runner().requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].program, Path::new(HDIUTIL));
        assert_eq!(
            argv(&requests[0]),
            ["compact", "-quiet", "main.sparseimage"]
        );
    }

    #[test]
    fn sparse_compaction_validates_extension_before_spawning() {
        let backend = MacOsApfsBackend::new(RecordingRunner::default());
        let error = backend
            .compact_image(Path::new("main.asif"), ImageFormat::Sparse)
            .unwrap_err();

        assert!(matches!(
            error,
            ApfsError::InvalidImagePath { path, format }
                if path == Path::new("main.asif") && format == ImageFormat::Sparse
        ));
        assert!(backend.runner().requests().is_empty());
    }

    #[test]
    fn asif_compaction_is_typed_unsupported_before_validation_or_spawn() {
        let backend = MacOsApfsBackend::new(RecordingRunner::default());
        let error = backend
            .compact_image(Path::new("wrong.sparseimage"), ImageFormat::Asif)
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "compact image is not supported for Asif images"
        );
        assert!(matches!(
            error,
            ApfsError::UnsupportedOperation {
                operation: "compact image",
                format: ImageFormat::Asif,
            }
        ));
        assert!(backend.runner().requests().is_empty());
    }

    #[test]
    fn sparse_compaction_propagates_checked_command_failure() {
        let backend =
            MacOsApfsBackend::new(RecordingRunner::with_outputs([CommandOutput::failure(
                9,
                "compact failed",
            )]));
        let error = backend
            .compact_image(Path::new("main.sparseimage"), ImageFormat::Sparse)
            .unwrap_err();

        assert!(matches!(
            error,
            ApfsError::CommandFailed {
                operation: "compact SPARSE image",
                request,
                output: CommandOutput { status: 9, .. },
            } if request.program == Path::new(HDIUTIL)
                && argv(&request) == ["compact", "-quiet", "main.sparseimage"]
        ));
        assert_eq!(backend.runner().requests().len(), 1);
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
                image_format: ImageFormatSelection::Auto,
                owner_uid: 502,
                owner_gid: 20,
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
    fn non_capability_asif_failure_is_not_silently_downgraded() {
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
            CommandOutput::success("26.0\n"),
            CommandOutput::failure(77, "permission denied"),
        ]));
        let error = backend
            .create_staged_image(&CreateImageRequest {
                staged_stem: PathBuf::from(".staging/main"),
                capacity: "100g".into(),
                volume_name: "main".into(),
                case_sensitivity: ApfsCaseSensitivity::Insensitive,
                image_format: ImageFormatSelection::Auto,
                owner_uid: 502,
                owner_gid: 20,
            })
            .unwrap_err();
        assert!(matches!(
            error,
            ApfsError::CommandFailed {
                operation: "create ASIF image",
                output: CommandOutput { status: 77, .. },
                ..
            }
        ));
        assert_eq!(backend.runner().requests().len(), 2);
        assert!(!asif_is_unsupported(&CommandOutput::failure(
            77,
            "permission denied"
        )));
        assert!(asif_is_unsupported(&CommandOutput::failure(
            1,
            "unknown format ASIF"
        )));
    }

    #[test]
    fn unsupported_asif_cleanup_removes_partial_artifact() {
        let stem = temp_path("partial", "stem").with_extension("");
        let asif = stem.with_extension(ImageFormat::Asif.extension());
        fs::write(&asif, b"partial").unwrap();
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
            CommandOutput::success("26.0\n"),
            CommandOutput::failure(1, "unsupported"),
            CommandOutput::success([]),
        ]));
        let created = backend
            .create_staged_image(&CreateImageRequest {
                staged_stem: stem,
                capacity: "1g".into(),
                volume_name: "main".into(),
                case_sensitivity: ApfsCaseSensitivity::Insensitive,
                image_format: ImageFormatSelection::Auto,
                owner_uid: 502,
                owner_gid: 20,
            })
            .unwrap();
        assert_eq!(created.format, ImageFormat::Sparse);
        assert!(!asif.exists());
    }

    #[test]
    fn clone_validation_requires_both_paths_to_preserve_requested_format() {
        let backend = MacOsApfsBackend::new(RecordingRunner::default());
        for (source, destination, invalid) in [
            ("main.sparseimage", "session.asif", "main.sparseimage"),
            ("main.asif", "session.sparseimage", "session.sparseimage"),
        ] {
            let error = backend
                .clone_image(Path::new(source), Path::new(destination), ImageFormat::Asif)
                .unwrap_err();
            assert!(matches!(
                error,
                CloneFileError::InvalidImagePath { path, format }
                    if path == Path::new(invalid) && format == ImageFormat::Asif
            ));
        }

        let source = temp_path("validated-missing-source", ImageFormat::Asif.extension());
        let destination = temp_path(
            "validated-missing-destination",
            ImageFormat::Asif.extension(),
        );
        let error = backend
            .clone_image(&source, &destination, ImageFormat::Asif)
            .unwrap_err();
        #[cfg(target_os = "macos")]
        assert!(matches!(error, CloneFileError::Io { .. }));
        #[cfg(not(target_os = "macos"))]
        assert!(matches!(error, CloneFileError::UnsupportedPlatform));
    }

    #[test]
    fn public_detach_delegates_format_force_and_device() {
        let backend =
            MacOsApfsBackend::new(RecordingRunner::with_outputs([CommandOutput::failure(
                16, "busy",
            )]));
        let attachment = AttachedImage {
            image: PathBuf::from("session.sparseimage"),
            format: ImageFormat::Sparse,
            whole_device: "/dev/disk12".into(),
            volume_device: "/dev/disk12s1".into(),
        };
        let error = backend.detach(&attachment, true).unwrap_err();
        assert!(matches!(
            error,
            ApfsError::CommandFailed {
                operation: "detach image",
                output: CommandOutput { status: 16, .. },
                ..
            }
        ));
        let requests = backend.runner().requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].program, Path::new(HDIUTIL));
        assert_eq!(
            argv(&requests[0]),
            ["detach", "-quiet", "-force", "/dev/disk12"]
        );
    }

    #[test]
    fn detach_target_records_format_specific_device_and_mountpoint_commands() {
        let cases: [(ImageFormat, DetachTarget<'_>, bool, &str, &[&str]); 4] = [
            (
                ImageFormat::Asif,
                DetachTarget::Device("/dev/disk3s1"),
                true,
                DISKUTIL,
                &["eject", "force", "/dev/disk3s1"],
            ),
            (
                ImageFormat::Asif,
                DetachTarget::MountPoint(Path::new("/Volumes/cowshed/main")),
                false,
                DISKUTIL,
                &["eject", "/Volumes/cowshed/main"],
            ),
            (
                ImageFormat::Sparse,
                DetachTarget::Device("/dev/disk4"),
                true,
                HDIUTIL,
                &["detach", "-quiet", "-force", "/dev/disk4"],
            ),
            (
                ImageFormat::Sparse,
                DetachTarget::MountPoint(Path::new("/Volumes/cowshed/session")),
                false,
                HDIUTIL,
                &["detach", "-quiet", "/Volumes/cowshed/session"],
            ),
        ];

        for (format, target, force, program, expected_argv) in cases {
            let backend =
                MacOsApfsBackend::new(RecordingRunner::with_outputs([CommandOutput::success([])]));
            backend.detach_target(format, target, force).unwrap();
            let requests = backend.runner().requests();
            assert_eq!(requests.len(), 1);
            assert_eq!(requests[0].program, Path::new(program));
            assert_eq!(argv(&requests[0]), expected_argv);
        }
    }

    #[test]
    fn detach_target_rejects_unvalidated_devices_and_mountpoints_before_spawning() {
        let invalid = [
            DetachTarget::Device("disk1"),
            DetachTarget::Device("/dev/rdisk1"),
            DetachTarget::Device("/dev/disk"),
            DetachTarget::Device("/dev/disk01"),
            DetachTarget::Device("/dev/disk1s01"),
            DetachTarget::Device("/dev/disk1s"),
            DetachTarget::Device("/dev/disk1/child"),
            DetachTarget::MountPoint(Path::new("relative/mount")),
            DetachTarget::MountPoint(Path::new("/")),
            DetachTarget::MountPoint(Path::new("/dev")),
            DetachTarget::MountPoint(Path::new("/dev/disk1")),
            DetachTarget::MountPoint(Path::new("/Volumes/../private/tmp")),
            DetachTarget::MountPoint(Path::new("/Volumes/./main")),
            DetachTarget::MountPoint(Path::new("/Volumes//main")),
            DetachTarget::MountPoint(Path::new("/Volumes/main/")),
            DetachTarget::MountPoint(Path::new("/Volumes/\0main")),
        ];
        let backend = MacOsApfsBackend::new(RecordingRunner::default());

        for target in invalid {
            let expected = match target {
                DetachTarget::Device(device) => PathBuf::from(device),
                DetachTarget::MountPoint(path) => path.to_owned(),
            };
            let error = backend
                .detach_target(ImageFormat::Sparse, target, false)
                .unwrap_err();
            assert!(matches!(
                error,
                ApfsError::InvalidDetachTarget(path) if path == expected
            ));
        }
        assert!(backend.runner().requests().is_empty());
    }

    #[test]
    fn invalid_detach_target_error_preserves_the_rejected_target() {
        let error = ApfsError::InvalidDetachTarget(PathBuf::from("../escape"));
        assert_eq!(error.to_string(), "invalid APFS detach target: ../escape");
    }

    #[test]
    fn volume_name_records_checked_diskutil_info_and_returns_trimmed_name() {
        let output = r#"<?xml version="1.0"?><plist version="1.0"><dict>
          <key>DeviceIdentifier</key><string>disk12s3</string>
          <key>VolumeName</key><string>  cowshed.acme--widget.main  </string>
        </dict></plist>"#;
        let backend =
            MacOsApfsBackend::new(RecordingRunner::with_outputs([CommandOutput::success(
                output,
            )]));

        assert_eq!(
            backend.volume_name("/dev/disk12s3").unwrap(),
            "cowshed.acme--widget.main"
        );
        let requests = backend.runner().requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].program, Path::new(DISKUTIL));
        assert_eq!(argv(&requests[0]), ["info", "-plist", "/dev/disk12s3"]);
    }

    #[test]
    fn volume_name_rejects_noncanonical_devices_before_spawning() {
        let backend = MacOsApfsBackend::new(RecordingRunner::default());
        for device in [
            "",
            "disk12s3",
            "/dev/rdisk12s3",
            "/dev/disk",
            "/dev/disk01",
            "/dev/disk12s03",
            "/dev/disk12s",
            "/dev/disk12/child",
        ] {
            let error = backend.volume_name(device).unwrap_err();
            assert!(matches!(
                error,
                ApfsError::InvalidVolumeDevice(rejected) if rejected == device
            ));
        }
        assert!(backend.runner().requests().is_empty());
    }

    #[test]
    fn volume_name_propagates_checked_diskutil_failure() {
        let backend =
            MacOsApfsBackend::new(RecordingRunner::with_outputs([CommandOutput::failure(
                3,
                "not found",
            )]));
        let error = backend.volume_name("/dev/disk12s3").unwrap_err();
        assert!(matches!(
            error,
            ApfsError::CommandFailed {
                operation: "read APFS volume name",
                output: CommandOutput { status: 3, .. },
                ..
            }
        ));
        assert_eq!(backend.runner().requests().len(), 1);
    }

    #[test]
    fn rename_volume_records_checked_diskutil_rename_volume_command() {
        let backend =
            MacOsApfsBackend::new(RecordingRunner::with_outputs([CommandOutput::success([])]));

        backend
            .rename_volume(
                Path::new("/Volumes/cowshed-stage"),
                "cowshed.acme--widget.main",
            )
            .unwrap();

        let requests = backend.runner().requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].program, Path::new(DISKUTIL));
        assert_eq!(
            argv(&requests[0]),
            [
                "renameVolume",
                "/Volumes/cowshed-stage",
                "cowshed.acme--widget.main",
            ]
        );
    }

    #[test]
    fn rename_volume_accepts_a_255_byte_path_safe_name() {
        let name = "a".repeat(255);
        let backend =
            MacOsApfsBackend::new(RecordingRunner::with_outputs([CommandOutput::success([])]));

        backend
            .rename_volume(Path::new("/Volumes/cowshed-stage"), &name)
            .unwrap();

        let requests = backend.runner().requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].args[2], OsString::from(name));
    }

    #[test]
    fn rename_volume_rejects_noncanonical_mountpoints_before_spawning() {
        let backend = MacOsApfsBackend::new(RecordingRunner::default());
        for mount_point in [
            "",
            ".",
            "Volumes/main",
            "/",
            "/dev",
            "/dev/disk12s3",
            "/Volumes/../private/tmp",
            "/Volumes/./main",
            "/Volumes//main",
            "/Volumes/main/",
            "/Volumes/\0main",
        ] {
            let error = backend
                .rename_volume(Path::new(mount_point), "main")
                .unwrap_err();
            assert!(matches!(
                error,
                ApfsError::InvalidMountPoint(path) if path == Path::new(mount_point)
            ));
        }
        assert!(backend.runner().requests().is_empty());
    }

    #[test]
    fn rename_volume_rejects_unsafe_or_oversized_names_before_spawning() {
        let invalid = [
            String::new(),
            " ".into(),
            ".".into(),
            "..".into(),
            " leading".into(),
            "trailing ".into(),
            "parent/child".into(),
            "nul\0name".into(),
            "a".repeat(256),
            "é".repeat(128),
        ];
        let backend = MacOsApfsBackend::new(RecordingRunner::default());
        for name in invalid {
            let error = backend
                .rename_volume(Path::new("/Volumes/cowshed-stage"), &name)
                .unwrap_err();
            assert!(matches!(
                error,
                ApfsError::InvalidVolumeName(rejected) if rejected == name
            ));
        }
        assert!(backend.runner().requests().is_empty());
    }

    #[test]
    fn rename_volume_propagates_checked_diskutil_failure() {
        let backend =
            MacOsApfsBackend::new(RecordingRunner::with_outputs([CommandOutput::failure(
                7,
                "rename failed",
            )]));

        let error = backend
            .rename_volume(Path::new("/Volumes/cowshed-stage"), "main")
            .unwrap_err();

        assert!(matches!(
            error,
            ApfsError::CommandFailed {
                operation: "rename APFS volume",
                request,
                output: CommandOutput { status: 7, .. },
            } if request.program == Path::new(DISKUTIL)
                && argv(&request) == ["renameVolume", "/Volumes/cowshed-stage", "main"]
        ));
        assert_eq!(backend.runner().requests().len(), 1);
    }

    #[test]
    fn rename_volume_validation_errors_preserve_rejected_values() {
        assert_eq!(
            ApfsError::InvalidMountPoint(PathBuf::from("../escape")).to_string(),
            "invalid APFS mount point: ../escape"
        );
        assert_eq!(
            ApfsError::InvalidVolumeName("bad/name".into()).to_string(),
            r#"invalid APFS volume name: "bad/name""#
        );
    }

    #[test]
    fn volume_name_plist_rejects_malformed_mismatched_and_missing_identity() {
        let malformed = parse_volume_name_plist("/dev/disk12s3", b"not a plist").unwrap_err();
        assert!(matches!(
            malformed,
            ApfsError::VolumeNameResolutionFailed {
                device,
                reason: VolumeNameResolutionFailure::InvalidPlist(_),
            } if device == "/dev/disk12s3"
        ));

        let missing_device = br#"<?xml version="1.0"?><plist><dict>
          <key>VolumeName</key><string>main</string>
        </dict></plist>"#;
        assert!(matches!(
            parse_volume_name_plist("/dev/disk12s3", missing_device),
            Err(ApfsError::VolumeNameResolutionFailed {
                reason: VolumeNameResolutionFailure::MissingDeviceIdentifier,
                ..
            })
        ));

        let wrong_device_type = br#"<?xml version="1.0"?><plist><dict>
          <key>DeviceIdentifier</key><integer>12</integer>
          <key>VolumeName</key><string>main</string>
        </dict></plist>"#;
        assert!(matches!(
            parse_volume_name_plist("/dev/disk12s3", wrong_device_type),
            Err(ApfsError::VolumeNameResolutionFailed {
                reason: VolumeNameResolutionFailure::InvalidPlist(_),
                ..
            })
        ));

        let mismatch = br#"<?xml version="1.0"?><plist><dict>
          <key>DeviceIdentifier</key><string>/dev/disk13s1</string>
          <key>VolumeName</key><string>main</string>
        </dict></plist>"#;
        assert!(matches!(
            parse_volume_name_plist("/dev/disk12s3", mismatch),
            Err(ApfsError::VolumeNameResolutionFailed {
                device,
                reason: VolumeNameResolutionFailure::DeviceMismatch { reported },
            }) if device == "/dev/disk12s3" && reported == "/dev/disk13s1"
        ));
    }

    #[test]
    fn volume_name_plist_rejects_missing_wrong_type_and_blank_names() {
        let missing = br#"<?xml version="1.0"?><plist><dict>
          <key>DeviceIdentifier</key><string>disk12s3</string>
        </dict></plist>"#;
        assert!(matches!(
            parse_volume_name_plist("/dev/disk12s3", missing),
            Err(ApfsError::VolumeNameResolutionFailed {
                reason: VolumeNameResolutionFailure::MissingVolumeName,
                ..
            })
        ));

        let wrong_type = br#"<?xml version="1.0"?><plist><dict>
          <key>DeviceIdentifier</key><string>disk12s3</string>
          <key>VolumeName</key><integer>7</integer>
        </dict></plist>"#;
        assert!(matches!(
            parse_volume_name_plist("/dev/disk12s3", wrong_type),
            Err(ApfsError::VolumeNameResolutionFailed {
                reason: VolumeNameResolutionFailure::WrongTypeVolumeName,
                ..
            })
        ));

        for name in ["", "   ", "\n\t"] {
            let plist = format!(
                r#"<?xml version="1.0"?><plist><dict>
                  <key>DeviceIdentifier</key><string>disk12s3</string>
                  <key>VolumeName</key><string>{name}</string>
                </dict></plist>"#
            );
            assert!(matches!(
                parse_volume_name_plist("/dev/disk12s3", plist.as_bytes()),
                Err(ApfsError::VolumeNameResolutionFailed {
                    reason: VolumeNameResolutionFailure::BlankVolumeName,
                    ..
                })
            ));
        }
    }

    #[test]
    fn volume_name_resolution_failure_messages_preserve_typed_details() {
        let cases = [
            (
                VolumeNameResolutionFailure::InvalidPlist("bad shape".into()),
                "invalid disk info plist: bad shape",
            ),
            (
                VolumeNameResolutionFailure::MissingDeviceIdentifier,
                "disk info plist has no DeviceIdentifier",
            ),
            (
                VolumeNameResolutionFailure::DeviceMismatch {
                    reported: "/dev/disk9s1".into(),
                },
                "disk info plist reported a different device: /dev/disk9s1",
            ),
            (
                VolumeNameResolutionFailure::MissingVolumeName,
                "disk info plist has no VolumeName",
            ),
            (
                VolumeNameResolutionFailure::WrongTypeVolumeName,
                "disk info plist VolumeName is not a string",
            ),
            (
                VolumeNameResolutionFailure::BlankVolumeName,
                "disk info plist VolumeName is blank",
            ),
        ];
        for (failure, expected) in cases {
            assert_eq!(failure.to_string(), expected);
        }
    }

    #[test]
    fn delete_image_removes_files_ignores_absence_and_reports_other_io() {
        let backend = MacOsApfsBackend::new(RecordingRunner::default());
        let image = temp_path("delete", ImageFormat::Asif.extension());
        fs::write(&image, b"image").unwrap();
        backend.delete_image(&image, ImageFormat::Asif).unwrap();
        assert!(!image.exists());
        backend.delete_image(&image, ImageFormat::Asif).unwrap();

        let directory = temp_path("delete-directory", ImageFormat::Asif.extension());
        fs::create_dir(&directory).unwrap();
        let error = backend
            .delete_image(&directory, ImageFormat::Asif)
            .unwrap_err();
        assert!(matches!(
            error,
            ApfsError::FileOperation {
                operation: "delete image",
                ..
            }
        ));
        fs::remove_dir(directory).unwrap();
    }

    #[cfg(target_os = "macos")]
    #[test]
    #[ignore = "creates and attaches a real macOS sparse APFS image"]
    fn real_sparse_attach_resolves_and_verifies_the_synthesized_volume() {
        let stem = temp_path("real-sparse-resolution", "stem").with_extension("");
        let image = stem.with_extension(ImageFormat::Sparse.extension());
        let backend = MacOsApfsBackend::new(SystemCommandRunner);
        let result = (|| -> Result<(), ApfsError> {
            let created = backend.create_staged_image(&CreateImageRequest {
                staged_stem: stem,
                capacity: "64m".into(),
                volume_name: "cowshed-apfs-resolution".into(),
                case_sensitivity: ApfsCaseSensitivity::Insensitive,
                image_format: ImageFormatSelection::Exact(ImageFormat::Sparse),
                owner_uid: 502,
                owner_gid: 20,
            })?;
            let attachment = backend.attach_verified(&created.path, created.format)?;
            assert!(attachment.volume_device().starts_with("/dev/disk"));
            assert_ne!(attachment.whole_device(), attachment.volume_device());
            backend.detach(&attachment, false)?;
            Ok(())
        })();
        let _ = fs::remove_file(image);
        result.unwrap();
    }

    #[cfg(target_os = "macos")]
    #[test]
    #[ignore = "creates and attaches a real macOS ASIF APFS image"]
    fn real_asif_attach_normalizes_bare_devices_and_verifies_the_volume() {
        let stem = temp_path("real-asif-resolution", "stem").with_extension("");
        let image = stem.with_extension(ImageFormat::Asif.extension());
        let backend = MacOsApfsBackend::new(SystemCommandRunner);
        let result = (|| -> Result<(), ApfsError> {
            let created = backend.create_staged_image(&CreateImageRequest {
                staged_stem: stem,
                capacity: "64m".into(),
                volume_name: "cowshed-asif-resolution".into(),
                case_sensitivity: ApfsCaseSensitivity::Insensitive,
                image_format: ImageFormatSelection::Exact(ImageFormat::Asif),
                owner_uid: 502,
                owner_gid: 20,
            })?;
            let attachment = backend.attach_verified(&created.path, created.format)?;
            assert!(attachment.whole_device().starts_with("/dev/disk"));
            assert!(attachment.volume_device().starts_with("/dev/disk"));
            backend.detach(&attachment, false)?;
            Ok(())
        })();
        let _ = fs::remove_file(image);
        result.unwrap();
    }

    #[test]
    fn blank_asif_plist_requires_one_canonical_whole_device() {
        assert_eq!(
            parse_blank_asif_whole_device(BLANK_ASIF_PLIST.as_bytes()).unwrap(),
            "/dev/disk8"
        );
        let missing = br#"<?xml version="1.0"?><plist><dict>
          <key>system-entities</key><array>
            <dict><key>dev-entry</key><string>disk8s1</string></dict>
          </array>
        </dict></plist>"#;
        assert!(matches!(
            parse_blank_asif_whole_device(missing),
            Err(ApfsError::InvalidAttachmentPlist(message))
                if message == "no canonical whole image device"
        ));
        let ambiguous = br#"<?xml version="1.0"?><plist><dict>
          <key>system-entities</key><array>
            <dict><key>dev-entry</key><string>disk8</string></dict>
            <dict><key>dev-entry</key><string>/dev/disk9</string></dict>
          </array>
        </dict></plist>"#;
        assert!(matches!(
            parse_blank_asif_whole_device(ambiguous),
            Err(ApfsError::InvalidAttachmentPlist(message))
                if message == "multiple whole image devices"
        ));
    }

    #[test]
    fn attachment_inventory_selects_only_exact_image_path_whole_devices() {
        let plist = attachment_inventory(&[
            (
                "/tmp/cowshed-target.asif",
                &["disk4", "/dev/disk4s1", "/dev/disk5"][..],
            ),
            ("/tmp/cowshed-unrelated.asif", &["/dev/disk20"][..]),
        ]);

        assert_eq!(
            parse_attachment_inventory(Path::new("/tmp/cowshed-target.asif"), plist.as_bytes())
                .unwrap(),
            BTreeSet::from(["/dev/disk4".into(), "/dev/disk5".into()])
        );
        assert!(
            parse_attachment_inventory(Path::new("/tmp/cowshed-absent.asif"), plist.as_bytes())
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn attachment_inventory_rejects_malformed_matching_records() {
        let malformed = [
            b"not a plist".as_slice(),
            br#"<?xml version="1.0"?><plist><dict/></plist>"#,
            br#"<?xml version="1.0"?><plist><dict><key>images</key><array><string>bad</string></array></dict></plist>"#,
            br#"<?xml version="1.0"?><plist><dict><key>images</key><array><dict><key>image-path</key><string>/tmp/cowshed-target.asif</string></dict></array></dict></plist>"#,
            br#"<?xml version="1.0"?><plist><dict><key>images</key><array><dict><key>image-path</key><string>/tmp/cowshed-target.asif</string><key>system-entities</key><array><dict><key>dev-entry</key><string>not-a-device</string></dict></array></dict></array></dict></plist>"#,
        ];
        for plist in malformed {
            assert!(matches!(
                parse_attachment_inventory(Path::new("/tmp/cowshed-target.asif"), plist),
                Err(ApfsError::InvalidAttachmentInventory(_))
            ));
        }
    }

    #[test]
    fn plist_selects_apfs_volume_and_whole_image_device() {
        assert_eq!(
            parse_attachment_plist(PLIST.as_bytes()).unwrap(),
            ("/dev/disk10".into(), "/dev/disk10s1".into())
        );
    }

    #[test]
    fn sparse_attachment_plist_preserves_image_whole_device_over_container_device() {
        assert_eq!(
            parse_attachment_plist(SPARSE_ATTACH_PLIST.as_bytes()).unwrap(),
            ("/dev/disk4".into(), "/dev/disk4s1".into())
        );
    }

    #[test]
    fn attachment_candidate_filter_rejects_unrelated_and_non_apfs_devices() {
        let plist = br#"<plist><dict><key>system-entities</key><array>
          <dict><key>dev-entry</key><string>disk1</string>
            <key>content-hint</key><string>GUID_partition_scheme</string></dict>
          <dict><key>dev-entry</key><string>disk9s1</string>
            <key>content-hint</key><string>Apple_APFS</string></dict>
          <dict><key>dev-entry</key><string>disk1s2s9</string>
            <key>content-hint</key><string>Apple_HFS</string></dict>
          <dict><key>dev-entry</key><string>disk1s1</string>
            <key>content-hint</key><string>Apple_APFS_Volume</string>
            <key>volume-kind</key><string>apfs</string></dict>
        </array></dict></plist>"#;
        assert_eq!(
            parse_attachment_plist(plist).unwrap(),
            ("/dev/disk1".into(), "/dev/disk1s1".into())
        );
    }

    #[test]
    fn volume_list_plist_selects_one_volume_and_rejects_zero_or_many() {
        assert_eq!(
            parse_volume_list_plist("/dev/disk4s1", SPARSE_VOLUME_LIST_PLIST.as_bytes()).unwrap(),
            "/dev/disk5s2"
        );
        assert!(matches!(
            parse_volume_list_plist("/dev/disk4s1", EMPTY_VOLUME_LIST_PLIST.as_bytes()),
            Err(ApfsError::VolumeResolutionFailed {
                candidate,
                reason: VolumeResolutionFailure::Missing,
            }) if candidate == "/dev/disk4s1"
        ));
        assert!(matches!(
            parse_volume_list_plist("/dev/disk4s1", AMBIGUOUS_VOLUME_LIST_PLIST.as_bytes()),
            Err(ApfsError::VolumeResolutionFailed {
                candidate,
                reason: VolumeResolutionFailure::Ambiguous(devices),
            }) if candidate == "/dev/disk4s1"
                && devices == ["/dev/disk5s2", "/dev/disk5s3"]
        ));
    }

    #[test]
    fn duplicate_container_matches_are_ambiguous_even_with_one_volume_each() {
        assert!(matches!(
            parse_volume_list_plist("/dev/disk4s1", DUPLICATE_CONTAINER_MATCH_PLIST.as_bytes()),
            Err(ApfsError::VolumeResolutionFailed {
                candidate,
                reason: VolumeResolutionFailure::Ambiguous(devices),
            }) if candidate == "/dev/disk4s1"
                && devices == ["/dev/disk5s2", "/dev/disk6s2"]
        ));
    }

    #[test]
    fn volume_resolution_failure_messages_preserve_typed_details() {
        assert_eq!(
            VolumeResolutionFailure::Missing.to_string(),
            "no APFS volume device was reported"
        );
        assert_eq!(
            VolumeResolutionFailure::Ambiguous(vec!["/dev/disk5s1".into(), "/dev/disk5s2".into()])
                .to_string(),
            "multiple APFS volume devices were reported: /dev/disk5s1, /dev/disk5s2"
        );
        assert_eq!(
            VolumeResolutionFailure::InvalidPlist("bad shape".into()).to_string(),
            "invalid APFS list plist: bad shape"
        );
    }

    #[test]
    fn volume_list_plist_rejects_malformed_shapes_and_device_identifiers() {
        for plist in [
            b"not a plist".as_slice(),
            br#"<?xml version="1.0"?><plist><dict></dict></plist>"#.as_slice(),
            br#"<?xml version="1.0"?><plist><dict><key>Containers</key><array>
                <dict><key>Volumes</key><array><dict><key>DeviceIdentifier</key>
                <string>not-a-device</string></dict></array></dict>
                </array></dict></plist>"#
                .as_slice(),
        ] {
            assert!(matches!(
                parse_volume_list_plist("/dev/disk5s1", plist),
                Err(ApfsError::VolumeResolutionFailed {
                    candidate,
                    reason: VolumeResolutionFailure::InvalidPlist(_),
                }) if candidate == "/dev/disk5s1"
            ));
        }
    }

    #[test]
    fn device_identifier_helpers_preserve_block_and_raw_volume_identity() {
        assert_eq!(device_path("disk12"), Some("/dev/disk12".into()));
        assert_eq!(device_path("disk12s3"), Some("/dev/disk12s3".into()));
        assert_eq!(volume_device_path("disk12s3"), Some("/dev/disk12s3".into()));
        assert_eq!(
            volume_device_path("/dev/disk12s3s1"),
            Some("/dev/disk12s3s1".into())
        );
        assert_eq!(volume_device_path("disk12"), None);
        for invalid in ["disks1", "disk12s", "disk12sx", "/dev/not-a-disk"] {
            assert_eq!(device_path(invalid), None);
            assert_eq!(volume_device_path(invalid), None);
        }
        assert_eq!(raw_device_from("/dev/disk12s3"), "/dev/rdisk12s3");
    }

    #[test]
    fn plist_accepts_each_apfs_volume_marker_and_prefers_deepest_volume() {
        let hint_only = br#"<plist><dict><key>system-entities</key><array>
            <dict><key>dev-entry</key><string>/dev/disk4</string></dict>
            <dict><key>dev-entry</key><string>/dev/disk4s1</string>
            <key>content-hint</key><string>Apple_APFS_Volume</string></dict>
            </array></dict></plist>"#;
        assert_eq!(
            parse_attachment_plist(hint_only).unwrap(),
            ("/dev/disk4".into(), "/dev/disk4s1".into())
        );

        let kind_only = br#"<plist><dict><key>system-entities</key><array>
            <dict><key>dev-entry</key><string>/dev/disk5</string></dict>
            <dict><key>dev-entry</key><string>/dev/disk5s2</string>
            <key>volume-kind</key><string>APFS</string></dict>
            </array></dict></plist>"#;
        assert_eq!(
            parse_attachment_plist(kind_only).unwrap(),
            ("/dev/disk5".into(), "/dev/disk5s2".into())
        );

        let nested = br#"<plist><dict><key>system-entities</key><array>
            <dict><key>dev-entry</key><string>/dev/disk7s1</string>
            <key>content-hint</key><string>Apple_APFS</string></dict>
            <dict><key>dev-entry</key><string>/dev/disk7s1s2</string>
            <key>content-hint</key><string>Apple_APFS</string></dict>
            </array></dict></plist>"#;
        assert_eq!(
            parse_attachment_plist(nested).unwrap(),
            ("/dev/disk7".into(), "/dev/disk7s1s2".into())
        );
    }

    #[test]
    fn device_helpers_distinguish_whole_disks_slices_and_invalid_names() {
        assert_eq!(device_depth("/dev/disk12"), 0);
        assert_eq!(device_depth("/dev/disk12s3"), 1);
        assert_eq!(device_depth("/dev/disk12s3s1"), 2);
        assert_eq!(
            whole_device_from("/dev/disk12s3"),
            Some("/dev/disk12".into())
        );
        assert_eq!(whole_device_from("/dev/disk"), None);
        assert_eq!(whole_device_from("/dev/not-a-disk"), None);

        let invalid = br#"<plist><dict><key>system-entities</key><array>
            <dict><key>dev-entry</key><string>/dev/not-a-disk</string>
            <key>volume-kind</key><string>apfs</string></dict>
            </array></dict></plist>"#;
        assert!(matches!(
            parse_attachment_plist(invalid),
            Err(ApfsError::InvalidAttachmentPlist(message))
                if message == "invalid APFS device"
        ));
    }

    #[test]
    fn clonefile_errors_classify_existing_destination_and_other_io() {
        let destination = classify_clone_error(
            Path::new("main.asif"),
            Path::new("session.asif"),
            io::Error::from_raw_os_error(17),
        );
        assert!(matches!(
            destination,
            CloneFileError::DestinationExists { destination }
                if destination == Path::new("session.asif")
        ));

        let other = classify_clone_error(
            Path::new("main.asif"),
            Path::new("session.asif"),
            io::Error::new(io::ErrorKind::PermissionDenied, "denied"),
        );
        assert!(matches!(
            &other,
            CloneFileError::Io {
                source_path,
                destination_path,
                source,
            } if source_path == Path::new("main.asif")
                && destination_path == Path::new("session.asif")
                && source.kind() == io::ErrorKind::PermissionDenied
        ));
        assert_eq!(
            std::error::Error::source(&other).unwrap().to_string(),
            "denied"
        );
    }

    #[test]
    fn clonefile_reports_a_missing_source_without_creating_destination() {
        let source = temp_path("missing-clone-source", ImageFormat::Asif.extension());
        let destination = temp_path("missing-clone-destination", ImageFormat::Asif.extension());
        let error = clonefile_native(&source, &destination).unwrap_err();
        #[cfg(target_os = "macos")]
        assert!(matches!(error, CloneFileError::Io { .. }));
        #[cfg(not(target_os = "macos"))]
        assert!(matches!(error, CloneFileError::UnsupportedPlatform));
        assert!(!destination.exists());
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
