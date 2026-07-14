use std::fmt;

use serde::{Deserialize, Serialize};

/// Stable cowshed outcome taxonomy shared by the core API and CLI.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ErrorCode {
    Internal,
    Usage,
    NotFound,
    Conflict,
    EnvironmentMissing,
    SandboxDenied,
    Integrity,
}

impl ErrorCode {
    pub const fn exit_code(self) -> u8 {
        match self {
            Self::Internal => 1,
            Self::Usage => 2,
            Self::NotFound => 3,
            Self::Conflict => 4,
            Self::EnvironmentMissing => 5,
            Self::SandboxDenied => 6,
            Self::Integrity => 7,
        }
    }

    pub const fn exec_wrapper_exit_code(self) -> u8 {
        match self {
            Self::Internal => 100,
            Self::Usage => 101,
            Self::NotFound => 102,
            Self::Conflict => 103,
            Self::EnvironmentMissing => 104,
            Self::SandboxDenied => 105,
            Self::Integrity => 106,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Internal => "internal",
            Self::Usage => "usage",
            Self::NotFound => "not-found",
            Self::Conflict => "conflict",
            Self::EnvironmentMissing => "environment-missing",
            Self::SandboxDenied => "sandbox-denied",
            Self::Integrity => "integrity",
        }
    }
}

/// An operational error with a concrete recovery command.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CowshedError {
    pub code: ErrorCode,
    pub message: String,
    pub hint: String,
}

impl CowshedError {
    pub fn new(code: ErrorCode, message: impl Into<String>, hint: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            hint: hint.into(),
        }
    }

    pub fn usage(message: impl Into<String>, hint: impl Into<String>) -> Self {
        Self::new(ErrorCode::Usage, message, hint)
    }

    pub fn not_found(message: impl Into<String>, hint: impl Into<String>) -> Self {
        Self::new(ErrorCode::NotFound, message, hint)
    }

    pub fn conflict(message: impl Into<String>, hint: impl Into<String>) -> Self {
        Self::new(ErrorCode::Conflict, message, hint)
    }

    pub fn environment_missing(message: impl Into<String>, hint: impl Into<String>) -> Self {
        Self::new(ErrorCode::EnvironmentMissing, message, hint)
    }

    pub fn sandbox_denied(message: impl Into<String>, hint: impl Into<String>) -> Self {
        Self::new(ErrorCode::SandboxDenied, message, hint)
    }

    pub fn integrity(message: impl Into<String>, hint: impl Into<String>) -> Self {
        Self::new(ErrorCode::Integrity, message, hint)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Internal, message, "cowshed doctor --json")
    }

    pub const fn exit_code(&self) -> u8 {
        self.code.exit_code()
    }

    pub const fn exec_wrapper_exit_code(&self) -> u8 {
        self.code.exec_wrapper_exit_code()
    }
}

impl fmt::Display for CowshedError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for CowshedError {}

pub type Result<T> = std::result::Result<T, CowshedError>;

#[cfg(test)]
mod tests {
    use super::{CowshedError, ErrorCode};

    const CODES: [ErrorCode; 7] = [
        ErrorCode::Internal,
        ErrorCode::Usage,
        ErrorCode::NotFound,
        ErrorCode::Conflict,
        ErrorCode::EnvironmentMissing,
        ErrorCode::SandboxDenied,
        ErrorCode::Integrity,
    ];

    #[test]
    fn stable_exit_codes_are_frozen() {
        assert_eq!(CODES.map(ErrorCode::exit_code), [1, 2, 3, 4, 5, 6, 7]);
        assert_eq!(
            CODES.map(ErrorCode::exec_wrapper_exit_code),
            [100, 101, 102, 103, 104, 105, 106]
        );
    }

    #[test]
    fn cowshed_error_delegates_codes_and_displays_its_message() {
        let error = CowshedError::conflict("working tree changed", "retry adoption");

        assert_eq!(error.exit_code(), ErrorCode::Conflict.exit_code());
        assert_eq!(
            error.exec_wrapper_exit_code(),
            ErrorCode::Conflict.exec_wrapper_exit_code()
        );
        assert_eq!(error.to_string(), "working tree changed");
    }

    #[test]
    fn json_uses_frozen_taxonomy_spelling() {
        let error = CowshedError::environment_missing("not adopted", "cowshed adopt");
        let value = serde_json::to_value(error).expect("error serializes");
        assert_eq!(value["code"], "environment-missing");
        assert_eq!(value["message"], "not adopted");
        assert_eq!(value["hint"], "cowshed adopt");
    }

    #[test]
    fn integrity_is_a_typed_operational_failure() {
        let error = CowshedError::integrity(
            "sealed stdout digest does not match",
            "cowshed doctor --json",
        );
        let value = serde_json::to_value(&error).expect("error serializes");

        assert_eq!(error.exit_code(), 7);
        assert_eq!(error.exec_wrapper_exit_code(), 106);
        assert_eq!(value["code"], "integrity");
        assert_eq!(value["hint"], "cowshed doctor --json");
    }
}
