//! What a launch could not do: the operation, what it was done to, and the kernel's reason.

use std::fmt;
use std::io;

#[derive(Debug)]
pub(super) struct Failure {
    operation: &'static str,
    subject: String,
    cause: io::Error,
}

impl Failure {
    pub(super) fn new(
        operation: &'static str,
        subject: impl fmt::Display,
        cause: io::Error,
    ) -> Self {
        Self {
            operation,
            subject: subject.to_string(),
            cause,
        }
    }

    /// The failure of the system call that just returned an error to this thread.
    pub(super) fn last(operation: &'static str, subject: impl fmt::Display) -> Self {
        Self::new(operation, subject, io::Error::last_os_error())
    }
}

impl fmt::Display for Failure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "cannot {} {}: {}",
            self.operation, self.subject, self.cause
        )
    }
}

impl std::error::Error for Failure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.cause)
    }
}
