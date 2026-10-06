//! The few SQLite calls carrying Nx's task database needs ([`super::carry`]), against the
//! system library macOS ships in `/usr/lib`. Nx keeps its cache index in SQLite; reading and
//! extending it through SQLite itself is what keeps a database another process wrote, its
//! write-ahead log included, consistent. A crate would add a C build or a second copy of the
//! library for eleven functions.

use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::io;
use std::path::Path;
use std::ptr::{self, NonNull};

#[repr(C)]
struct Sqlite3 {
    _private: [u8; 0],
}

#[repr(C)]
struct Sqlite3Stmt {
    _private: [u8; 0],
}

const SQLITE_OK: c_int = 0;
const SQLITE_ROW: c_int = 100;
const SQLITE_DONE: c_int = 101;
const SQLITE_OPEN_READWRITE: c_int = 0x0000_0002;
/// Each connection is used by the one thread that opened it.
const SQLITE_OPEN_NOMUTEX: c_int = 0x0000_8000;

/// `SQLITE_TRANSIENT`: SQLite copies a bound value before the call returns.
fn transient() -> Option<unsafe extern "C" fn(*mut c_void)> {
    // SAFETY: SQLite defines SQLITE_TRANSIENT as the destructor value -1 and never calls it.
    Some(unsafe { std::mem::transmute::<isize, unsafe extern "C" fn(*mut c_void)>(-1) })
}

#[link(name = "sqlite3")]
unsafe extern "C" {
    fn sqlite3_open_v2(
        filename: *const c_char,
        database: *mut *mut Sqlite3,
        flags: c_int,
        vfs: *const c_char,
    ) -> c_int;
    fn sqlite3_close_v2(database: *mut Sqlite3) -> c_int;
    fn sqlite3_errmsg(database: *mut Sqlite3) -> *const c_char;
    fn sqlite3_prepare_v2(
        database: *mut Sqlite3,
        sql: *const c_char,
        bytes: c_int,
        statement: *mut *mut Sqlite3Stmt,
        tail: *mut *const c_char,
    ) -> c_int;
    fn sqlite3_bind_text(
        statement: *mut Sqlite3Stmt,
        index: c_int,
        text: *const c_char,
        bytes: c_int,
        destructor: Option<unsafe extern "C" fn(*mut c_void)>,
    ) -> c_int;
    fn sqlite3_step(statement: *mut Sqlite3Stmt) -> c_int;
    fn sqlite3_column_text(statement: *mut Sqlite3Stmt, column: c_int) -> *const u8;
    fn sqlite3_column_bytes(statement: *mut Sqlite3Stmt, column: c_int) -> c_int;
    fn sqlite3_column_int64(statement: *mut Sqlite3Stmt, column: c_int) -> i64;
    fn sqlite3_reset(statement: *mut Sqlite3Stmt) -> c_int;
    fn sqlite3_finalize(statement: *mut Sqlite3Stmt) -> c_int;
}

/// One open database. Closing it (on drop) releases every file it opened, the attached ones
/// included.
pub struct Connection {
    database: NonNull<Sqlite3>,
}

impl Connection {
    /// Open the existing database at `path` for reading and writing; a missing file is an
    /// error, never a new database.
    pub fn open(path: &Path) -> io::Result<Self> {
        let name = c_path(path)?;
        let mut database = ptr::null_mut();
        // SAFETY: `name` outlives the call; `database` receives a handle (even on failure, which
        // is then closed below).
        let status = unsafe {
            sqlite3_open_v2(
                name.as_ptr(),
                &mut database,
                SQLITE_OPEN_READWRITE | SQLITE_OPEN_NOMUTEX,
                ptr::null(),
            )
        };
        let Some(database) = NonNull::new(database) else {
            return Err(io::Error::other(format!(
                "sqlite3_open_v2 {}: out of memory",
                path.display()
            )));
        };
        let connection = Self { database };
        if status != SQLITE_OK {
            return Err(connection.error(&format!("open {}", path.display())));
        }
        Ok(connection)
    }

    /// Prepare one SQL statement.
    pub fn prepare(&self, sql: &str) -> io::Result<Statement<'_>> {
        let text = CString::new(sql).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "SQL contains an interior NUL")
        })?;
        let mut statement = ptr::null_mut();
        // SAFETY: `text` is NUL-terminated (-1 reads to it) and outlives the call.
        let status = unsafe {
            sqlite3_prepare_v2(
                self.database.as_ptr(),
                text.as_ptr(),
                -1,
                &mut statement,
                ptr::null_mut(),
            )
        };
        match NonNull::new(statement) {
            Some(statement) if status == SQLITE_OK => Ok(Statement {
                connection: self,
                statement,
            }),
            _ => Err(self.error(sql)),
        }
    }

    /// Run `sql`, binding `parameters` to `?1`, `?2`, ..., to completion.
    pub fn execute(&self, sql: &str, parameters: &[&str]) -> io::Result<()> {
        let mut statement = self.prepare(sql)?;
        statement.bind(parameters)?;
        while statement.step()? {}
        Ok(())
    }

    fn error(&self, context: &str) -> io::Error {
        // SAFETY: the handle is open; SQLite owns the message until the next call on it.
        let message = unsafe { CStr::from_ptr(sqlite3_errmsg(self.database.as_ptr())) };
        io::Error::other(format!("sqlite: {context}: {}", message.to_string_lossy()))
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // SAFETY: every statement borrows the connection, so all are finalized by now.
        unsafe { sqlite3_close_v2(self.database.as_ptr()) };
    }
}

/// One prepared statement of a [`Connection`].
pub struct Statement<'connection> {
    connection: &'connection Connection,
    statement: NonNull<Sqlite3Stmt>,
}

impl Statement<'_> {
    /// Rewind the statement and bind `parameters` to `?1`, `?2`, ...
    pub fn bind(&mut self, parameters: &[&str]) -> io::Result<()> {
        // SAFETY: the statement is live; resetting a fresh one is a no-op.
        unsafe { sqlite3_reset(self.statement.as_ptr()) };
        for (index, value) in parameters.iter().enumerate() {
            let length = c_int::try_from(value.len()).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "SQL parameter too long")
            })?;
            let index = c_int::try_from(index + 1).expect("few parameters");
            // SAFETY: SQLite copies the bytes before returning (SQLITE_TRANSIENT).
            let status = unsafe {
                sqlite3_bind_text(
                    self.statement.as_ptr(),
                    index,
                    value.as_ptr().cast(),
                    length,
                    transient(),
                )
            };
            if status != SQLITE_OK {
                return Err(self.connection.error("bind"));
            }
        }
        Ok(())
    }

    /// Advance to the next row: `true` while one is current, `false` once the statement is done.
    pub fn step(&mut self) -> io::Result<bool> {
        // SAFETY: the statement is live.
        match unsafe { sqlite3_step(self.statement.as_ptr()) } {
            SQLITE_ROW => Ok(true),
            SQLITE_DONE => Ok(false),
            _ => Err(self.connection.error("step")),
        }
    }

    /// Column `column` of the current row as text.
    pub fn text(&self, column: c_int) -> String {
        // SAFETY: a row is current; the pointer is valid for `sqlite3_column_bytes` bytes until
        // the next step, and is copied out here.
        unsafe {
            let text = sqlite3_column_text(self.statement.as_ptr(), column);
            if text.is_null() {
                return String::new();
            }
            let length = usize::try_from(sqlite3_column_bytes(self.statement.as_ptr(), column))
                .unwrap_or_default();
            String::from_utf8_lossy(std::slice::from_raw_parts(text, length)).into_owned()
        }
    }

    /// Column `column` of the current row as an integer.
    pub fn integer(&self, column: c_int) -> i64 {
        // SAFETY: a row is current.
        unsafe { sqlite3_column_int64(self.statement.as_ptr(), column) }
    }
}

impl Drop for Statement<'_> {
    fn drop(&mut self) {
        // SAFETY: the statement is live and dropped once.
        unsafe { sqlite3_finalize(self.statement.as_ptr()) };
    }
}

pub(super) fn c_path(path: &Path) -> io::Result<CString> {
    use std::os::unix::ffi::OsStrExt as _;
    CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("path contains an interior NUL: {}", path.display()),
        )
    })
}
