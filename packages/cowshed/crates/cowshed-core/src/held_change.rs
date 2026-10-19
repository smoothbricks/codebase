//! Whether a target's version of a file already holds a workspace's change to it, judged from the
//! three-way merge `git merge-file --diff3` prints when it merges the workspace's change (`base` to
//! `here`) into the target's version.
//!
//! A clean merge that reproduces the target holds the change. A conflict alone does not prove the
//! opposite, because git conflicts on regions both sides *touched*, and a pick whose conflict a
//! lander resolved has, by construction, the target's own edits beside the workspace's in exactly
//! those regions. So each conflicted region is judged line by line: every hunk the workspace made
//! in it must be in the target's version of that region — the lines it replaced gone, the lines it
//! wrote present in order — and the merge with every region taken as the target has it must be the
//! target's version byte for byte (02_workspaces.md, `rm` step 1).

/// Conflict markers this long cannot be mistaken for the seven-character markers a tracked file may
/// itself carry; a line that still reads as a malformed block is judged unheld, never held.
pub(crate) const MARKER_SIZE: usize = 41;

/// The largest region, as base lines times workspace or target lines, aligned line by line. A
/// region past it is a rewrite, and is judged unheld rather than paid for quadratically.
const ALIGNMENT_CELLS: usize = 4_000_000;

/// What a merge says about the target's version of one file.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Verdict {
    /// The target's version holds the workspace's change.
    Held,
    /// The merge is clean, or every conflicted region is held, yet the result is not the target's
    /// version: some hunk of the workspace's is not in it.
    Missing,
    /// A conflicted region the target's version does not hold the workspace's hunks in.
    Conflict,
}

/// Judge `merged`, the `merge-file -p --diff3 --marker-size=MARKER_SIZE` output with the target's
/// version as "ours" and the workspace's as "theirs", against `target`, the target's version.
pub(crate) fn judge(merged: &[u8], target: &[u8]) -> Verdict {
    let open = marker(b'<');
    let base_marker = marker(b'|');
    let theirs_marker = marker(b'>');
    let separator = [&marker(b'=')[..], b"\n"].concat();
    let mut resolved = Vec::with_capacity(target.len());
    let mut lines = merged.split_inclusive(|byte| *byte == b'\n');
    while let Some(line) = lines.next() {
        if !is_marker(line, &open) {
            resolved.extend_from_slice(line);
            continue;
        }
        let ours = section(&mut lines, |line| is_marker(line, &base_marker));
        let base = section(&mut lines, |line| line == separator.as_slice());
        let theirs = section(&mut lines, |line| is_marker(line, &theirs_marker));
        let (Some(ours), Some(base), Some(theirs)) = (ours, base, theirs) else {
            return Verdict::Conflict;
        };
        if !region_holds(&base, &ours, &theirs) {
            return Verdict::Conflict;
        }
        for line in ours {
            resolved.extend_from_slice(line);
        }
    }
    if resolved == target {
        Verdict::Held
    } else {
        Verdict::Missing
    }
}

fn marker(byte: u8) -> [u8; MARKER_SIZE] {
    [byte; MARKER_SIZE]
}

/// A marker line is the marker alone or followed by a space and a label.
fn is_marker(line: &[u8], marker: &[u8]) -> bool {
    line.strip_prefix(marker)
        .is_some_and(|rest| matches!(rest.first(), None | Some(b' ' | b'\n')))
}

/// The lines up to the marker `ends` accepts, which is consumed; `None` when the output ends first.
fn section<'a>(
    lines: &mut impl Iterator<Item = &'a [u8]>,
    ends: impl Fn(&[u8]) -> bool,
) -> Option<Vec<&'a [u8]>> {
    let mut section = Vec::new();
    for line in lines {
        if ends(line) {
            return Some(section);
        }
        section.push(line);
    }
    None
}

/// Whether the target's lines of one conflicted region hold every hunk the workspace made there.
///
/// A workspace hunk replaces base lines `[start, end)` with new lines. It is held when none of
/// those base lines survives in the target's version of the region, and its new lines appear, in
/// order, between the target's nearest surviving base lines on either side: the place the
/// workspace wrote them. A pure deletion needs only the first.
fn region_holds(base: &[&[u8]], ours: &[&[u8]], theirs: &[&[u8]]) -> bool {
    let (Some(shed), Some(target)) = (aligned(base, theirs), aligned(base, ours)) else {
        return false;
    };
    let mut kept = vec![None; base.len()];
    for (base_line, ours_line) in target {
        kept[base_line] = Some(ours_line);
    }
    let mut previous = (0, 0);
    for (base_end, theirs_end) in shed.into_iter().chain([(base.len(), theirs.len())]) {
        let (start, written) = previous;
        previous = (base_end + 1, theirs_end + 1);
        let new = &theirs[written..theirs_end];
        if start == base_end && new.is_empty() {
            continue;
        }
        if kept[start..base_end].iter().any(Option::is_some) {
            return false;
        }
        let low = kept[..start]
            .iter()
            .rev()
            .find_map(|line| line.map(|ours_line| ours_line + 1))
            .unwrap_or(0);
        let high = kept[base_end..]
            .iter()
            .find_map(|line| *line)
            .unwrap_or(ours.len());
        if !in_order(new, &ours[low..high]) {
            return false;
        }
    }
    true
}

/// Whether every line of `needle` appears in `haystack`, in order.
fn in_order(needle: &[&[u8]], haystack: &[&[u8]]) -> bool {
    let mut remaining = haystack.iter();
    needle
        .iter()
        .all(|line| remaining.any(|candidate| candidate == line))
}

/// A longest common subsequence of `left` and `right` as index pairs in increasing order, or
/// `None` past [`ALIGNMENT_CELLS`].
fn aligned(left: &[&[u8]], right: &[&[u8]]) -> Option<Vec<(usize, usize)>> {
    let width = right.len() + 1;
    if left.len().checked_mul(right.len())? > ALIGNMENT_CELLS {
        return None;
    }
    // `lengths[i * width + j]` is the LCS length of `left[i..]` and `right[j..]`.
    let mut lengths = vec![0_u32; (left.len() + 1) * width];
    for i in (0..left.len()).rev() {
        for j in (0..right.len()).rev() {
            lengths[i * width + j] = if left[i] == right[j] {
                lengths[(i + 1) * width + j + 1] + 1
            } else {
                lengths[(i + 1) * width + j].max(lengths[i * width + j + 1])
            };
        }
    }
    let mut pairs = Vec::new();
    let (mut i, mut j) = (0, 0);
    while i < left.len() && j < right.len() {
        if left[i] == right[j] {
            pairs.push((i, j));
            i += 1;
            j += 1;
        } else if lengths[(i + 1) * width + j] >= lengths[i * width + j + 1] {
            i += 1;
        } else {
            j += 1;
        }
    }
    Some(pairs)
}

#[cfg(test)]
mod tests {
    use super::{MARKER_SIZE, Verdict, judge};

    /// The block `merge-file --diff3` prints for one conflicted region.
    fn block(ours: &str, base: &str, theirs: &str) -> String {
        let [open, middle, separator, close] =
            (*b"<|=>").map(|byte| char::from(byte).to_string().repeat(MARKER_SIZE));
        format!("{open} ours\n{ours}{middle} base\n{base}{separator}\n{theirs}{close} theirs\n")
    }

    /// The pick a lander resolved: the target edited the line beside the workspace's, so git
    /// conflicts on the region, and the target's version still holds the workspace's line.
    #[test]
    fn a_resolved_region_holding_the_workspace_line_beside_the_target_edit_is_held() {
        let merged = format!("{}c\n", block("a main\nb shed\n", "a\nb\n", "a\nb shed\n"));
        assert_eq!(
            judge(merged.as_bytes(), b"a main\nb shed\nc\n"),
            Verdict::Held
        );
    }

    /// Both sides inserted at one place, and the target kept the workspace's lines among its own.
    #[test]
    fn insertions_the_target_interleaved_with_its_own_are_held() {
        let merged = format!(
            "a\n{}d\n",
            block(
                "\"foo\": 1,\n\"baz\": 3,\n\"bar\": 2\n",
                "\"foo\": 1\n",
                "\"foo\": 1,\n\"bar\": 2\n"
            )
        );
        assert_eq!(
            judge(
                merged.as_bytes(),
                b"a\n\"foo\": 1,\n\"baz\": 3,\n\"bar\": 2\nd\n"
            ),
            Verdict::Held
        );
    }

    /// The target changed the line the workspace changed, to something else.
    #[test]
    fn a_line_both_sides_rewrote_differently_is_a_conflict() {
        let merged = block("b main\n", "b\n", "b shed\n");
        assert_eq!(judge(merged.as_bytes(), b"b main\n"), Verdict::Conflict);
    }

    /// The workspace replaced a line the target kept: its deletion is not in the target.
    #[test]
    fn a_replaced_line_the_target_kept_is_a_conflict() {
        let merged = block("a main\nb\nb shed\n", "a\nb\n", "a\nb shed\n");
        assert_eq!(
            judge(merged.as_bytes(), b"a main\nb\nb shed\n"),
            Verdict::Conflict
        );
    }

    /// The workspace's line exists in the target, but only outside the place it was written.
    #[test]
    fn a_line_found_only_outside_its_place_is_a_conflict() {
        let merged = format!("x\n{}", block("a main\n", "a\n", "a\nx\n"));
        assert_eq!(judge(merged.as_bytes(), b"x\na main\n"), Verdict::Conflict);
    }

    #[test]
    fn a_clean_merge_is_held_only_when_it_is_the_target_version() {
        assert_eq!(judge(b"a\nb\n", b"a\nb\n"), Verdict::Held);
        assert_eq!(judge(b"a\nb shed\n", b"a\nb\n"), Verdict::Missing);
    }

    /// A block that never closes is a parse git did not intend; it never reads as held.
    #[test]
    fn a_malformed_block_is_never_held() {
        let open = "<".repeat(MARKER_SIZE);
        assert_eq!(
            judge(format!("{open} ours\na\n").as_bytes(), b"a\n"),
            Verdict::Conflict
        );
    }
}
