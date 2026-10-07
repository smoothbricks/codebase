//! Pure bounded journal windows, shared by retained bytes and artifact readers.
//!
//! Raw controller tails keep first/last newline-delimited lines inside their byte
//! bound, including partial edge lines. [`WholeLineWindow`] uses the same cursor
//! and byte-window planning for consumers requiring whole lines: one lookbehind
//! byte identifies a latest window's first line, and one lookahead byte permits
//! progress through an over-long line. Planning and borrowed cuts allocate nothing;
//! [`tail_journals`] owns only the two bounded buffers supplied by its reader.

use std::num::NonZeroU32;
use std::ops::Range;

use super::{BinaryData, JobJournalCursor, JobStream, JobTail, JobTailLimits};
use crate::error::{CowshedError, Result};

#[derive(Clone, Copy, Debug)]
enum TailAnchor {
    After(u64),
    End,
}

struct TailWindow {
    anchor: TailAnchor,
    len: u64,
    range: Range<u64>,
}

impl TailWindow {
    fn new(stream: JobStream, len: u64, anchor: TailAnchor, max: u64) -> Result<Self> {
        let range = match anchor {
            TailAnchor::After(cursor) if cursor > len => {
                let name = match stream {
                    JobStream::Stdout => "stdout",
                    JobStream::Stderr => "stderr",
                };
                return Err(CowshedError::usage(
                    format!("tail cursor {cursor} is past the {len} admitted {name} bytes"),
                    "resume from a cursor a tail of this job returned",
                ));
            }
            TailAnchor::After(cursor) => cursor..len.min(cursor.saturating_add(max)),
            TailAnchor::End => len.saturating_sub(max)..len,
        };
        Ok(Self { anchor, len, range })
    }

    fn cut(self, mut bytes: Vec<u8>, lines: u32) -> StreamTail {
        match self.anchor {
            TailAnchor::After(cursor) => {
                bytes.truncate(first_lines_len(&bytes, lines));
                let next = cursor + u64::try_from(bytes.len()).unwrap_or(u64::MAX);
                StreamTail {
                    bytes,
                    next,
                    truncated: next < self.len,
                }
            }
            TailAnchor::End => {
                let skip = last_lines_start(&bytes, lines);
                bytes.drain(..skip);
                StreamTail {
                    bytes,
                    next: self.len,
                    truncated: self.range.start + u64::try_from(skip).unwrap_or(u64::MAX) > 0,
                }
            }
        }
    }
}

struct StreamTail {
    bytes: Vec<u8>,
    next: u64,
    truncated: bool,
}

/// A raw tail of two journals at one admitted snapshot. Both cursors are
/// checked before either read. The reader must return exactly each requested
/// range; a short or oversized answer is an internal error, not a valid slice.
/// This is the controller's live/sealed tail core and is also usable over an
/// independent artifact store without opening a controller.
pub fn tail_journals(
    cursor: Option<JobJournalCursor>,
    limits: JobTailLimits,
    lengths: (u64, u64),
    mut read: impl FnMut(JobStream, Range<u64>) -> Result<Vec<u8>>,
) -> Result<JobTail> {
    let max = u64::from(limits.bytes_per_stream.get());
    let lines = limits.lines_per_stream.get();
    let anchor = |position: Option<u64>| position.map_or(TailAnchor::End, TailAnchor::After);
    let stdout = TailWindow::new(
        JobStream::Stdout,
        lengths.0,
        anchor(cursor.map(|cursor| cursor.stdout)),
        max,
    )?;
    let stderr = TailWindow::new(
        JobStream::Stderr,
        lengths.1,
        anchor(cursor.map(|cursor| cursor.stderr)),
        max,
    )?;
    let stdout_bytes = read(JobStream::Stdout, stdout.range.clone())?;
    let stderr_bytes = read(JobStream::Stderr, stderr.range.clone())?;
    exact_range(&stdout.range, &stdout_bytes)?;
    exact_range(&stderr.range, &stderr_bytes)?;
    let stdout = stdout.cut(stdout_bytes, lines);
    let stderr = stderr.cut(stderr_bytes, lines);
    let data = |bytes: Vec<u8>| {
        BinaryData::new(bytes)
            .map_err(|error| CowshedError::internal(format!("tail slice is unbounded: {error}")))
    };
    Ok(JobTail {
        next: JobJournalCursor {
            stdout: stdout.next,
            stderr: stderr.next,
        },
        stdout_truncated: stdout.truncated,
        stderr_truncated: stderr.truncated,
        stdout: data(stdout.bytes)?,
        stderr: data(stderr.bytes)?,
    })
}

/// One stream's whole-line byte window. A running journal's trailing partial
/// line is withheld; an ended journal's final unterminated line is complete.
/// A line longer than the bound is cut inside it so continuation can progress.
/// Latest windows skip a partial leading line when another line begins inside
/// the window, otherwise keep their bounded bytes as an over-long line.
pub struct WholeLineWindow {
    range: Range<u64>,
    lookbehind: bool,
    max: usize,
    ended: bool,
}

impl WholeLineWindow {
    pub fn new(
        stream: JobStream,
        length: u64,
        from: Option<u64>,
        max_bytes: NonZeroU32,
        ended: bool,
    ) -> Result<Self> {
        let window = TailWindow::new(
            stream,
            length,
            from.map_or(TailAnchor::End, TailAnchor::After),
            u64::from(max_bytes.get()),
        )?;
        let max = usize::try_from(max_bytes.get())
            .map_err(|_| CowshedError::internal("tail byte bound exceeds platform range"))?;
        let lookbehind = from.is_none() && window.range.start > 0;
        let range = if lookbehind {
            window.range.start - 1..window.range.end
        } else if from.is_some() {
            window.range.start..length.min(window.range.end.saturating_add(1))
        } else {
            window.range
        };
        Ok(Self {
            range,
            lookbehind,
            max,
            ended,
        })
    }

    /// The exact range to read, including at most one boundary byte beyond
    /// the returned slice's bound. It can be fetched in controller-sized pages.
    pub fn range(&self) -> Range<u64> {
        self.range.clone()
    }

    /// Borrow the bounded whole-line slice and its starting journal cursor.
    /// The ending cursor is `from + text.len()`; no byte copy is needed.
    pub fn cut<'a>(&self, bytes: &'a [u8]) -> Result<(u64, &'a [u8])> {
        exact_range(&self.range, bytes)?;
        let (begin, overlong) = if self.lookbehind {
            match bytes.iter().take(self.max).position(|byte| *byte == b'\n') {
                Some(newline) => (newline + 1, false),
                None => (1.min(bytes.len()), true),
            }
        } else {
            (0, false)
        };
        let rest = &bytes[begin..];
        let end = if self.ended && rest.len() <= self.max {
            rest.len()
        } else {
            match rest.iter().take(self.max).rposition(|byte| *byte == b'\n') {
                Some(newline) => newline + 1,
                None if rest.len() > self.max || overlong => self.max.min(rest.len()),
                None => 0,
            }
        };
        Ok((
            self.range.start + u64::try_from(begin).unwrap_or(u64::MAX),
            &rest[..end],
        ))
    }
}

fn exact_range(range: &Range<u64>, bytes: &[u8]) -> Result<()> {
    let want = range.end - range.start;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) != want {
        return Err(CowshedError::internal(format!(
            "journal reader returned {} bytes for {}..{} ({want} required)",
            bytes.len(),
            range.start,
            range.end
        )));
    }
    Ok(())
}

fn first_lines_len(bytes: &[u8], lines: u32) -> usize {
    let mut seen = 0_u32;
    for (index, byte) in bytes.iter().enumerate() {
        if *byte == b'\n' {
            seen += 1;
            if seen == lines {
                return index + 1;
            }
        }
    }
    bytes.len()
}

fn last_lines_start(bytes: &[u8], lines: u32) -> usize {
    let Some((_, body)) = bytes.split_last() else {
        return 0;
    };
    let mut seen = 0_u32;
    for (index, byte) in body.iter().enumerate().rev() {
        if *byte == b'\n' {
            seen += 1;
            if seen == lines {
                return index + 1;
            }
        }
    }
    0
}

#[cfg(test)]
mod tests {
    use super::super::JobTailBytes;
    use super::*;

    fn limits(bytes: u32, lines: u32) -> JobTailLimits {
        JobTailLimits {
            bytes_per_stream: JobTailBytes::new(bytes).unwrap(),
            lines_per_stream: NonZeroU32::new(lines).unwrap(),
        }
    }

    fn slice(journal: &[u8], anchor: TailAnchor, limits: JobTailLimits) -> (Vec<u8>, u64, bool) {
        let window = TailWindow::new(
            JobStream::Stdout,
            u64::try_from(journal.len()).unwrap(),
            anchor,
            u64::from(limits.bytes_per_stream.get()),
        )
        .unwrap();
        let start = usize::try_from(window.range.start).unwrap();
        let end = usize::try_from(window.range.end).unwrap();
        let tail = window.cut(journal[start..end].to_vec(), limits.lines_per_stream.get());
        (tail.bytes, tail.next, tail.truncated)
    }

    #[test]
    fn the_latest_tail_keeps_the_last_lines_counting_an_unterminated_one() {
        let wide = limits(64, 2);
        assert_eq!(
            slice(b"a\nb\nc", TailAnchor::End, wide),
            (b"b\nc".to_vec(), 5, true)
        );
        assert_eq!(
            slice(b"a\nb\nc\n", TailAnchor::End, wide),
            (b"b\nc\n".to_vec(), 6, true)
        );
        assert_eq!(
            slice(b"a\nb", TailAnchor::End, wide),
            (b"a\nb".to_vec(), 3, false)
        );
        assert_eq!(slice(b"", TailAnchor::End, wide), (Vec::new(), 0, false));
        assert_eq!(
            slice(b"0123456789", TailAnchor::End, limits(4, 2)),
            (b"6789".to_vec(), 10, true)
        );
    }

    #[test]
    fn a_tail_after_a_cursor_keeps_the_first_lines_and_names_what_follows() {
        let journal = b"a\nb\nc";
        assert_eq!(
            slice(journal, TailAnchor::After(0), limits(64, 2)),
            (b"a\nb\n".to_vec(), 4, true)
        );
        assert_eq!(
            slice(journal, TailAnchor::After(4), limits(64, 2)),
            (b"c".to_vec(), 5, false)
        );
        assert_eq!(
            slice(journal, TailAnchor::After(5), limits(64, 2)),
            (Vec::new(), 5, false)
        );
        assert_eq!(
            slice(journal, TailAnchor::After(1), limits(2, 9)),
            (b"\nb".to_vec(), 3, true)
        );
    }

    #[test]
    fn a_bad_second_cursor_is_refused_before_either_stream_is_read() {
        let cursor = JobJournalCursor {
            stdout: 0,
            stderr: 6,
        };
        let error = tail_journals(Some(cursor), limits(64, 1), (5, 5), |_, _| {
            unreachable!("a refused tail reads no stream")
        })
        .err()
        .unwrap();
        assert_eq!(error.code, crate::error::ErrorCode::Usage);
        assert!(error.message.contains("stderr"), "{}", error.message);
    }

    #[test]
    fn a_cursor_past_the_admitted_bytes_is_a_usage_error() {
        let error = TailWindow::new(JobStream::Stderr, 5, TailAnchor::After(6), 64)
            .err()
            .unwrap();
        assert_eq!(error.code, crate::error::ErrorCode::Usage);
        assert!(error.message.contains("stderr"), "{}", error.message);
    }

    #[test]
    fn tail_bytes_are_positive_and_at_most_one_inline_frame() {
        assert!(JobTailBytes::new(0).is_err());
        assert!(JobTailBytes::new(65_536).is_ok());
        assert!(JobTailBytes::new(65_537).is_err());
        assert!(serde_json::from_str::<JobTailBytes>("0").is_err());
        assert!(
            serde_json::from_str::<JobTailLimits>(r#"{"bytesPerStream":1,"linesPerStream":0}"#)
                .is_err()
        );
    }

    #[test]
    fn whole_line_windows_cover_edges_partial_lines_and_long_line_progress() {
        for (journal, from, max, ended, expected, start) in [
            (
                &b"1\n2\n3\n4\n5\n"[..],
                Some(0),
                5,
                false,
                &b"1\n2\n"[..],
                0,
            ),
            (&b"1\n2\n3\n4\n5\n"[..], None, 5, false, &b"4\n5\n"[..], 6),
            (&b"1\n2\npartial"[..], Some(0), 64, false, &b"1\n2\n"[..], 0),
            (
                &b"1\n2\npartial"[..],
                Some(0),
                64,
                true,
                &b"1\n2\npartial"[..],
                0,
            ),
            (&b"0123456789"[..], Some(0), 4, false, &b"0123"[..], 0),
            (&b"0123456789"[..], None, 4, false, &b"6789"[..], 6),
            (&b"a\nb\n"[..], None, 2, false, &b"b\n"[..], 2),
            (&b"a\n"[..], Some(2), 4, false, &b""[..], 2),
            (&b""[..], None, 4, true, &b""[..], 0),
        ] {
            let window = WholeLineWindow::new(
                JobStream::Stdout,
                u64::try_from(journal.len()).unwrap(),
                from,
                NonZeroU32::new(max).unwrap(),
                ended,
            )
            .unwrap();
            let range = window.range();
            let bytes = &journal
                [usize::try_from(range.start).unwrap()..usize::try_from(range.end).unwrap()];
            assert_eq!(
                window.cut(bytes).unwrap(),
                (start, expected),
                "{journal:?} from {from:?} max {max} ended {ended}"
            );
        }
    }

    #[test]
    fn whole_line_cursors_preserve_multibyte_journal_bytes() {
        let journal = "é\n猫\nz".as_bytes();
        let mut from = 0;
        let mut collected = Vec::new();
        for expected in ["é\n", "猫\n", "z"] {
            let window = WholeLineWindow::new(
                JobStream::Stdout,
                u64::try_from(journal.len()).unwrap(),
                Some(from),
                NonZeroU32::new(4).unwrap(),
                true,
            )
            .unwrap();
            let range = window.range();
            let bytes = &journal
                [usize::try_from(range.start).unwrap()..usize::try_from(range.end).unwrap()];
            let (start, text) = window.cut(bytes).unwrap();
            assert_eq!(start, from);
            assert_eq!(text, expected.as_bytes());
            collected.extend_from_slice(text);
            from = start + u64::try_from(text.len()).unwrap();
        }
        assert_eq!(collected, journal);
        assert_eq!(from, u64::try_from(journal.len()).unwrap());
        let pending = WholeLineWindow::new(
            JobStream::Stdout,
            1,
            Some(0),
            NonZeroU32::new(4).unwrap(),
            false,
        )
        .unwrap();
        assert_eq!(pending.cut(&[0xc3]).unwrap(), (0, &b""[..]));
        let ended = WholeLineWindow::new(
            JobStream::Stdout,
            1,
            Some(0),
            NonZeroU32::new(4).unwrap(),
            true,
        )
        .unwrap();
        assert_eq!(ended.cut(&[0xc3]).unwrap(), (0, &[0xc3][..]));
    }

    #[test]
    fn both_window_readers_reject_short_or_oversized_ranges() {
        for bytes in [b"a".to_vec(), b"abc".to_vec()] {
            let error = tail_journals(None, limits(2, 1), (2, 0), |stream, _| {
                Ok(match stream {
                    JobStream::Stdout => bytes.clone(),
                    JobStream::Stderr => Vec::new(),
                })
            })
            .err()
            .unwrap();
            assert_eq!(error.code, crate::error::ErrorCode::Internal);
            let window = WholeLineWindow::new(
                JobStream::Stdout,
                2,
                Some(0),
                NonZeroU32::new(2).unwrap(),
                true,
            )
            .unwrap();
            assert_eq!(
                window.cut(&bytes).unwrap_err().code,
                crate::error::ErrorCode::Internal
            );
        }
    }
}
