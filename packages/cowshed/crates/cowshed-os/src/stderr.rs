//! Diagnostic lines on stderr, each in one write.
//!
//! Every cowshed process of a host (the gateway, each workspace supervisor, their children)
//! writes stderr to one shared file, launchd's `StandardErrorPath`
//! (13_telemetry.md, "The one text-file survivor"). `std`'s `eprintln!` writes an unbuffered
//! stderr one formatted piece at a time, one `write(2)` per piece. Its lock orders threads
//! within a process but nothing across processes, so lines of concurrent processes splice
//! mid-word. Measured 2026-10-21: an install's `supervisor-recovery drain` spans and its
//! supervisors' "retires after draining" lines interleaved beyond reading. [`eprintln!`] formats
//! the whole line first and hands it to the kernel in one write, which an append-mode file
//! places whole.
//!
//! A daemon (the gateway, each workspace supervisor) runs its whole async runtime on its main
//! thread, so a line written there in place stops every request it serves until the kernel
//! takes the line. The log lives on the same APFS container as the store, and a container
//! stalled by one huge free stalls that write: measured 2026-10-22, the installed gateway's main
//! thread sat in `write(2)` on `daemon-stderr.log` for every one of 728 samples while its trash
//! thread unlinked an image, and every `cowshed fork` and `ls` on the host hung behind it. So a
//! daemon [`detach`]es its stderr first: each line goes whole onto a queue of [`QUEUE_LINES`]
//! lines, which one writer thread writes in order. A full queue drops the line and counts it,
//! and the writer's next write first says how many it dropped, so a loss is never silent.
//! [`flush`] waits for the queue before the process exits.

use std::fmt;
use std::io::{self, Write};
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::thread::JoinHandle;

/// How many lines a detached stderr holds for its writer before it drops one: a burst of a few
/// seconds of a busy daemon's diagnostics, a few hundred kilobytes at most.
pub const QUEUE_LINES: usize = 4096;

static DETACHED: OnceLock<Detached> = OnceLock::new();

/// Write `args` and a newline to stderr in one write. Like `std::eprintln!`, panics if stderr
/// cannot be written, unless this process [`detach`]ed it.
pub fn line(args: fmt::Arguments<'_>) {
    let mut text = args.to_string();
    text.push('\n');
    write(text);
}

/// Write `args` to stderr in one write, with no newline. Like `std::eprint!`, panics if stderr
/// cannot be written, unless this process [`detach`]ed it.
pub fn text(args: fmt::Arguments<'_>) {
    write(args.to_string());
}

fn write(text: String) {
    let text = match DETACHED.get() {
        Some(detached) => match detached.send(text) {
            Sent::Queued | Sent::Dropped => return,
            // Only a panic ends the writer: say the line in place, as before detaching.
            Sent::WriterGone(text) => text,
        },
        None => text,
    };
    if let Err(error) = io::stderr().lock().write_all(text.as_bytes()) {
        panic!("failed printing to stderr: {error}");
    }
}

/// From now on, queue this process's stderr lines for a writer thread of their own (see the
/// module). Refuses a second detach, and a writer thread that cannot start.
pub fn detach() -> io::Result<()> {
    let detached = Detached::spawn(io::stderr(), QUEUE_LINES)?;
    DETACHED
        .set(detached)
        .map_err(|_| io::Error::new(io::ErrorKind::AlreadyExists, "stderr is already detached"))
}

/// Wait until every line queued before this call is written, with the count of any dropped. A
/// process that never detached has nothing queued.
pub fn flush() {
    if let Some(detached) = DETACHED.get() {
        detached.flush();
    }
}

/// A queue of whole lines and the thread that writes them to its sink, in order.
struct Detached {
    queue: SyncSender<Message>,
    dropped: Arc<AtomicU64>,
    writer: Option<JoinHandle<()>>,
}

/// One line for the writer, and whom to tell once it and everything before it is written: a
/// flush is an empty line with someone to tell.
struct Message {
    text: String,
    written: Option<SyncSender<()>>,
}

/// What queueing one line did.
#[derive(Debug, PartialEq)]
enum Sent {
    Queued,
    /// The queue was full: the line is counted, not written.
    Dropped,
    WriterGone(String),
}

impl Detached {
    fn spawn(sink: impl Write + Send + 'static, lines: usize) -> io::Result<Self> {
        let (queue, received) = mpsc::sync_channel(lines);
        let dropped = Arc::new(AtomicU64::new(0));
        let counted = Arc::clone(&dropped);
        let writer = std::thread::Builder::new()
            .name("stderr".to_owned())
            .spawn(move || drain(sink, &received, &counted))?;
        Ok(Self {
            queue,
            dropped,
            writer: Some(writer),
        })
    }

    /// Queue `text` without waiting: a full queue drops it and counts it.
    fn send(&self, text: String) -> Sent {
        match self.queue.try_send(Message {
            text,
            written: None,
        }) {
            Ok(()) => Sent::Queued,
            Err(TrySendError::Full(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                Sent::Dropped
            }
            Err(TrySendError::Disconnected(message)) => Sent::WriterGone(message.text),
        }
    }

    fn flush(&self) {
        let (done, written) = mpsc::sync_channel(1);
        let flush = Message {
            text: String::new(),
            written: Some(done),
        };
        // A writer that is gone has written all it ever will: nothing is left to wait for.
        if self.queue.send(flush).is_ok() {
            // An error is a writer gone before answering: it has written all it ever will.
            written.recv().unwrap_or(());
        }
    }
}

impl Drop for Detached {
    /// Close the queue and wait for the writer to write what it holds and close its sink.
    fn drop(&mut self) {
        let (closed, _) = mpsc::sync_channel(0);
        drop(std::mem::replace(&mut self.queue, closed));
        if let Some(writer) = self.writer.take() {
            drop(writer.join());
        }
    }
}

fn drain(mut sink: impl Write, received: &Receiver<Message>, dropped: &AtomicU64) {
    for message in received {
        say_dropped(&mut sink, dropped);
        if !message.text.is_empty() && sink.write_all(message.text.as_bytes()).is_err() {
            dropped.fetch_add(1, Ordering::Relaxed);
        }
        if let Some(written) = message.written {
            // The flusher stopped waiting only if it is gone; there is nobody left to tell.
            written.send(()).unwrap_or(());
        }
    }
    say_dropped(&mut sink, dropped);
}

/// Say how many lines were dropped since the last time this was said, before the next line.
fn say_dropped(sink: &mut impl Write, dropped: &AtomicU64) {
    let count = dropped.swap(0, Ordering::Relaxed);
    if count == 0 {
        return;
    }
    let note = format!(
        "cowshed: {count} stderr line(s) dropped: the log took no writes while the queue was full\n"
    );
    if sink.write_all(note.as_bytes()).is_err() {
        dropped.fetch_add(count, Ordering::Relaxed);
    }
}

/// `std::eprintln!`, as one write: see the module.
#[macro_export]
macro_rules! eprintln {
    () => {
        $crate::stderr::line(::std::format_args!(""))
    };
    ($($arg:tt)*) => {
        $crate::stderr::line(::std::format_args!($($arg)*))
    };
}

/// `std::eprint!`, as one write: see the module.
#[macro_export]
macro_rules! eprint {
    ($($arg:tt)*) => {
        $crate::stderr::text(::std::format_args!($($arg)*))
    };
}

#[cfg(test)]
mod tests {

    use std::io::Read as _;

    use super::{Detached, Sent};

    /// A writer stuck on a log nobody reads never holds up the thread that says a line: past
    /// the queue, lines are dropped and counted, and once the log drains, the count is said
    /// before the next write, exactly. A pipe holds at most 64 KiB, far less than the lines
    /// sent, so the writer is blocked in `write(2)` while the sayer sends them all.
    #[test]
    fn a_stalled_log_drops_and_counts_lines_and_never_blocks_the_sayer() {
        let (mut log, sink) = std::io::pipe().expect("pipe");
        let detached = Detached::spawn(sink, 4).expect("writer");
        let line = format!("{}\n", "x".repeat(1023));
        let sent = 1000_u64;
        let mut dropped = 0_u64;
        for _ in 0..sent {
            if detached.send(line.clone()) == Sent::Dropped {
                dropped += 1;
            }
        }
        assert!(
            dropped > 0,
            "{sent} KiB never fit a stalled pipe and a queue of 4"
        );

        let reading = std::thread::spawn(move || {
            let mut text = String::new();
            log.read_to_string(&mut text).expect("read the log");
            text
        });
        detached.flush();
        drop(detached);
        let text = reading.join().expect("reader");

        let mut written = 0_u64;
        let mut said = 0_u64;
        for read in text.lines() {
            if read.len() == 1023 && read.bytes().all(|byte| byte == b'x') {
                written += 1;
                continue;
            }
            let count = read
                .strip_prefix("cowshed: ")
                .and_then(|rest| rest.split_once(" stderr line(s) dropped: "))
                .and_then(|(count, _)| count.parse::<u64>().ok())
                .unwrap_or_else(|| panic!("a line neither sent nor a drop count: {read:?}"));
            said += count;
        }
        assert_eq!(said, dropped, "every dropped line is counted aloud");
        assert_eq!(
            written + dropped,
            sent,
            "every line is written whole or counted"
        );
    }
    /// A line reaches the kernel as one write: formatting completes before anything is
    /// written, so a format argument that panics writes nothing at all, where `std`'s macro
    /// would already have written the text before it.
    #[test]
    fn a_line_is_formatted_whole_before_it_is_written() {
        struct Fails;
        impl std::fmt::Display for Fails {
            fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                Err(std::fmt::Error)
            }
        }
        let formatted = std::panic::catch_unwind(|| {
            crate::eprintln!("cowshed: written only whole {}", Fails);
        });
        assert!(
            formatted.is_err(),
            "a line that cannot be formatted is not written in part"
        );
    }
}
