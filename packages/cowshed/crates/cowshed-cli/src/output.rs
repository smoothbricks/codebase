use std::io::{self, Write};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JsonValue {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    String(String),
    Array(Vec<JsonValue>),
    Object(Vec<(String, JsonValue)>),
}

impl JsonValue {
    pub fn object(fields: impl IntoIterator<Item = (impl Into<String>, JsonValue)>) -> Self {
        Self::Object(fields.into_iter().map(|(key, value)| (key.into(), value)).collect())
    }

    pub fn array(values: impl IntoIterator<Item = JsonValue>) -> Self {
        Self::Array(values.into_iter().collect())
    }
}

impl From<&str> for JsonValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

impl From<String> for JsonValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<bool> for JsonValue {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<u64> for JsonValue {
    fn from(value: u64) -> Self {
        Self::U64(value)
    }
}

impl From<u32> for JsonValue {
    fn from(value: u32) -> Self {
        Self::U64(u64::from(value))
    }
}

impl From<i64> for JsonValue {
    fn from(value: i64) -> Self {
        Self::I64(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ErrorRecord {
    pub code: String,
    pub message: String,
    pub hint: String,
}

impl ErrorRecord {
    pub fn new(code: impl Into<String>, message: impl Into<String>, hint: impl Into<String>) -> Self {
        Self { code: code.into(), message: message.into(), hint: hint.into() }
    }
}

pub fn write_json<W: Write>(writer: &mut W, value: &JsonValue) -> io::Result<()> {
    match value {
        JsonValue::Null => writer.write_all(b"null"),
        JsonValue::Bool(true) => writer.write_all(b"true"),
        JsonValue::Bool(false) => writer.write_all(b"false"),
        JsonValue::I64(value) => write!(writer, "{value}"),
        JsonValue::U64(value) => write!(writer, "{value}"),
        JsonValue::String(value) => write_json_string(writer, value),
        JsonValue::Array(values) => {
            writer.write_all(b"[")?;
            for (index, value) in values.iter().enumerate() {
                if index != 0 { writer.write_all(b",")?; }
                write_json(writer, value)?;
            }
            writer.write_all(b"]")
        }
        JsonValue::Object(fields) => {
            writer.write_all(b"{")?;
            for (index, (key, value)) in fields.iter().enumerate() {
                if index != 0 { writer.write_all(b",")?; }
                write_json_string(writer, key)?;
                writer.write_all(b":")?;
                write_json(writer, value)?;
            }
            writer.write_all(b"}")
        }
    }
}

pub fn write_success_envelope<W: Write>(writer: &mut W, result: &JsonValue) -> io::Result<()> {
    writer.write_all(b"{\"ok\":true,\"result\":")?;
    write_json(writer, result)?;
    writer.write_all(b"}\n")
}

pub fn write_error_envelope<W: Write>(writer: &mut W, error: &ErrorRecord) -> io::Result<()> {
    writer.write_all(b"{\"ok\":false,\"error\":{\"code\":")?;
    write_json_string(writer, &error.code)?;
    writer.write_all(b",\"message\":")?;
    write_json_string(writer, &error.message)?;
    writer.write_all(b",\"hint\":")?;
    write_json_string(writer, &error.hint)?;
    writer.write_all(b"}}\n")
}

fn write_json_string<W: Write>(writer: &mut W, value: &str) -> io::Result<()> {
    writer.write_all(b"\"")?;
    for character in value.chars() {
        match character {
            '"' => writer.write_all(b"\\\"")?,
            '\\' => writer.write_all(b"\\\\")?,
            '\u{08}' => writer.write_all(b"\\b")?,
            '\u{0c}' => writer.write_all(b"\\f")?,
            '\n' => writer.write_all(b"\\n")?,
            '\r' => writer.write_all(b"\\r")?,
            '\t' => writer.write_all(b"\\t")?,
            character if character <= '\u{1f}' => write!(writer, "\\u{:04x}", character as u32)?,
            character => {
                let mut encoded = [0; 4];
                writer.write_all(character.encode_utf8(&mut encoded).as_bytes())?;
            }
        }
    }
    writer.write_all(b"\"")
}

pub struct Output<W: Write, E: Write> {
    stdout: W,
    stderr: E,
    quiet: bool,
}

impl<W: Write, E: Write> Output<W, E> {
    pub const fn new(stdout: W, stderr: E, quiet: bool) -> Self {
        Self { stdout, stderr, quiet }
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

    /// Emits one JSON record without wrapping it in the CLI envelope.
    pub fn bare_record(&mut self, value: &JsonValue) -> io::Result<()> {
        write_json(&mut self.stdout, value)?;
        self.stdout.write_all(b"\n")
    }

    /// Emits the one frozen success envelope on stdout.
    pub fn success(&mut self, result: &JsonValue) -> io::Result<()> {
        write_success_envelope(&mut self.stdout, result)
    }

    /// Emits the one frozen error envelope on stdout.
    pub fn json_error(&mut self, error: &ErrorRecord) -> io::Result<()> {
        write_error_envelope(&mut self.stdout, error)
    }

    /// Emits ordinary guidance on stderr unless quiet mode suppresses it.
    pub fn guidance(&mut self, message: &str) -> io::Result<()> {
        if self.quiet { return Ok(()); }
        writeln!(self.stderr, "cowshed: {message}")
    }

    /// Emits an actionable next command on stderr unless quiet mode suppresses it.
    pub fn hint(&mut self, command: &str) -> io::Result<()> {
        if self.quiet { return Ok(()); }
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

    #[test]
    fn guidance_never_contaminates_piped_stdout() {
        let mut output = Output::new(Vec::new(), Vec::new(), false);
        output.bare_line(b"/tmp/raven").unwrap();
        output.guidance("attached workspace raven").unwrap();
        output.hint("cd \"$(cowshed path raven)\"").unwrap();
        let (stdout, stderr) = output.into_inner();
        assert_eq!(stdout, b"/tmp/raven\n");
        assert_eq!(stderr, b"cowshed: attached workspace raven\nnext: cd \"$(cowshed path raven)\"\n");
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
    fn frozen_success_envelope_has_no_extra_top_level_keys() {
        let result = JsonValue::object([
            ("workspace", JsonValue::from("raven")),
            ("mount", JsonValue::from("/tmp/raven")),
            ("jobId", JsonValue::from(7_u64)),
        ]);
        let mut encoded = Vec::new();
        write_success_envelope(&mut encoded, &result).unwrap();
        assert_eq!(encoded, br#"{"ok":true,"result":{"workspace":"raven","mount":"/tmp/raven","jobId":7}}
"#);
    }

    #[test]
    fn frozen_error_envelope_escapes_values_and_has_exact_shape() {
        let error = ErrorRecord::new("not-found", "workspace \"raven\"\nmissing", "cowshed ls");
        let mut encoded = Vec::new();
        write_error_envelope(&mut encoded, &error).unwrap();
        assert_eq!(encoded, br#"{"ok":false,"error":{"code":"not-found","message":"workspace \"raven\"\nmissing","hint":"cowshed ls"}}
"#);
    }

    #[test]
    fn bare_records_are_not_implicitly_enveloped() {
        let mut output = Output::new(Vec::new(), Vec::new(), false);
        output.bare_record(&JsonValue::object([("name", JsonValue::from("main"))])).unwrap();
        let (stdout, stderr) = output.into_inner();
        assert_eq!(stdout, b"{\"name\":\"main\"}\n");
        assert!(stderr.is_empty());
    }
}
