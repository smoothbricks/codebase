//! A test's completed phases, reported to the bounded-exec run it runs under: the run's progress
//! channel is the Unix socket `BOUNDED_EXEC_PROGRESS_SOCKET` names (smoothbricks nx-plugin
//! `progress-channel.ts`; specs/cowshed/08_testing.md, "awake-idle-bound"). Each line written there
//! is one record, `moved`, and restarts the run's idle bound as a finished test does. Outside such a
//! run the variable is unset and nothing is reported.
//!
//! A fixture reports once after each phase that completed successfully, never when one starts,
//! waits or retries, and never on a timer: a phase that does not complete still ends the run at
//! its bound, which then names the test.

use std::ffi::OsStr;
use std::io::{Read as _, Write as _};
use std::os::unix::net::UnixStream;
use std::path::Path;

use cowshed_os::test_backstop::UnixStreamBackstop as _;

/// The variable naming the run's progress channel.
pub(crate) const PROGRESS_SOCKET_ENV: &str = "BOUNDED_EXEC_PROGRESS_SOCKET";

/// The one record the channel accepts.
const RECORD: &[u8] = b"moved\n";

/// Report that `phase` of this test completed, to the run this process runs under.
pub(crate) fn completed_phase(phase: &str) {
    if let Some(socket) = std::env::var_os(PROGRESS_SOCKET_ENV) {
        report(&socket, phase);
    }
}

/// One record on the channel at `socket`. The executor ends its side of a connection after
/// reading every record the client wrote, so the record has counted once the read here sees
/// that end. A channel that refuses is a failure of the run's reporting, said by name.
fn report(socket: &OsStr, phase: &str) {
    let sent = (|| -> std::io::Result<()> {
        let mut stream = UnixStream::connect_bounded(socket)?;
        stream.write_all(RECORD)?;
        stream.shutdown(std::net::Shutdown::Write)?;
        let mut rest = Vec::new();
        stream.read_to_end(&mut rest)?;
        Ok(())
    })();
    if let Err(error) = sent {
        panic!(
            "report the completed phase {phase} to the progress channel {} ({PROGRESS_SOCKET_ENV}): {error}",
            Path::new(socket).display()
        );
    }
    cowshed_os::eprintln!("test progress: {phase} completed");
}

#[cfg(test)]
mod tests {
    use std::io::Read as _;
    use std::os::unix::net::UnixListener;

    #[test]
    fn a_completed_phase_is_one_record_counted_before_the_report_returns() {
        let directory = std::env::temp_dir().join(format!("cs-progress-{}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();
        let socket = directory.join("p.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        // The executor's side: read a connection's every record, then end it.
        let reader = std::thread::spawn(move || {
            let mut records = Vec::new();
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().unwrap();
                let mut bytes = Vec::new();
                stream.read_to_end(&mut bytes).unwrap();
                records.push(bytes);
            }
            records
        });
        super::report(socket.as_os_str(), "Adopt");
        super::report(socket.as_os_str(), "Fork");
        assert_eq!(
            reader.join().unwrap(),
            [b"moved\n".to_vec(), b"moved\n".to_vec()]
        );
        std::fs::remove_dir_all(&directory).unwrap();
    }
}
