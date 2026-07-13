use cowshed_core::CowshedError;
use cowshed_core::api::{EmptyResult, JsonEnvelope, ResultBody};
use serde::Serialize;
use std::io::{self, Write};

fn write_serialized<W: Write, T: Serialize + ?Sized>(writer: &mut W, value: &T) -> io::Result<()> {
    serde_json::to_writer(&mut *writer, value).map_err(io::Error::other)?;
    writer.write_all(b"\n")
}

pub fn write_success_envelope<W: Write, T: ResultBody>(
    writer: &mut W,
    result: T,
) -> io::Result<()> {
    write_serialized(writer, &JsonEnvelope::success(result))
}

pub fn write_error_envelope<W: Write>(writer: &mut W, error: CowshedError) -> io::Result<()> {
    write_serialized(writer, &JsonEnvelope::<EmptyResult>::failure(error))
}

pub struct Output<W: Write, E: Write> {
    stdout: W,
    stderr: E,
    quiet: bool,
}

impl<W: Write, E: Write> Output<W, E> {
    pub const fn new(stdout: W, stderr: E, quiet: bool) -> Self {
        Self {
            stdout,
            stderr,
            quiet,
        }
    }

    /// Copies an already-machine-readable bare value or child stream verbatim.
    pub fn bare(&mut self, value: &[u8]) -> io::Result<()> {
        self.stdout.write_all(value)
    }

    /// Emits one machine-readable bare value followed by a newline.
    pub fn bare_line(&mut self, value: &[u8]) -> io::Result<()> {
        self.stdout.write_all(value)?;
        self.stdout.write_all(b"\n")
    }

    /// Emits one typed JSON record without wrapping it in the CLI envelope.
    pub fn bare_record<T: Serialize + ?Sized>(&mut self, value: &T) -> io::Result<()> {
        write_serialized(&mut self.stdout, value)
    }

    /// Emits the one frozen success envelope on stdout.
    pub fn success<T: ResultBody>(&mut self, result: T) -> io::Result<()> {
        write_success_envelope(&mut self.stdout, result)
    }

    /// Emits the one frozen error envelope on stdout.
    pub fn json_error(&mut self, error: CowshedError) -> io::Result<()> {
        write_error_envelope(&mut self.stdout, error)
    }

    /// Emits ordinary guidance on stderr unless quiet mode suppresses it.
    pub fn guidance(&mut self, message: &str) -> io::Result<()> {
        if self.quiet {
            return Ok(());
        }
        writeln!(self.stderr, "cowshed: {message}")
    }

    /// Emits an actionable next command on stderr unless quiet mode suppresses it.
    pub fn hint(&mut self, command: &str) -> io::Result<()> {
        if self.quiet {
            return Ok(());
        }
        writeln!(self.stderr, "next: {command}")
    }

    /// Emits an error on stderr. Errors are never suppressed by quiet mode.
    pub fn error(&mut self, message: &str) -> io::Result<()> {
        writeln!(self.stderr, "cowshed: {message}")
    }

    pub fn into_inner(self) -> (W, E) {
        (self.stdout, self.stderr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cowshed_core::ErrorCode;
    use cowshed_core::api::{EmptyResult, MountResult};
    use cowshed_core::metadata::WorkspaceName;
    use std::path::PathBuf;

    #[test]
    fn guidance_never_contaminates_piped_stdout() {
        let mut output = Output::new(Vec::new(), Vec::new(), false);
        output.bare_line(b"/tmp/raven").unwrap();
        output.guidance("attached workspace raven").unwrap();
        output.hint("cd \"$(cowshed path raven)\"").unwrap();
        let (stdout, stderr) = output.into_inner();
        assert_eq!(stdout, b"/tmp/raven\n");
        assert_eq!(
            stderr,
            b"cowshed: attached workspace raven\nnext: cd \"$(cowshed path raven)\"\n"
        );
    }

    #[test]
    fn quiet_suppresses_guidance_but_not_errors() {
        let mut output = Output::new(Vec::new(), Vec::new(), true);
        output.guidance("hidden").unwrap();
        output.hint("hidden").unwrap();
        output.error("still visible").unwrap();
        let (stdout, stderr) = output.into_inner();
        assert!(stdout.is_empty());
        assert_eq!(stderr, b"cowshed: still visible\n");
    }

    #[test]
    fn core_success_envelope_is_the_only_json_success_shape() {
        let mut output = Output::new(Vec::new(), Vec::new(), false);
        output
            .success(MountResult {
                workspace: WorkspaceName::new("raven").unwrap(),
                mount: PathBuf::from("/tmp/raven"),
                base_commit: None,
            })
            .unwrap();
        let (stdout, stderr) = output.into_inner();
        assert_eq!(
            stdout,
            b"{\"ok\":true,\"result\":{\"workspace\":\"raven\",\"mount\":\"/tmp/raven\"}}\n"
        );
        assert!(stderr.is_empty());
    }

    #[test]
    fn core_error_envelope_preserves_taxonomy_and_hint() {
        let mut output = Output::new(Vec::new(), Vec::new(), false);
        output
            .json_error(CowshedError::new(
                ErrorCode::NotFound,
                "workspace raven does not exist",
                "cowshed ls",
            ))
            .unwrap();
        let (stdout, stderr) = output.into_inner();
        assert_eq!(
            stdout,
            b"{\"ok\":false,\"error\":{\"code\":\"not-found\",\"message\":\"workspace raven does not exist\",\"hint\":\"cowshed ls\"}}\n"
        );
        assert!(stderr.is_empty());
    }

    #[test]
    fn empty_success_is_an_object_never_null() {
        let mut bytes = Vec::new();
        write_success_envelope(&mut bytes, EmptyResult {}).unwrap();
        assert_eq!(bytes, b"{\"ok\":true,\"result\":{}}\n");
    }
}
