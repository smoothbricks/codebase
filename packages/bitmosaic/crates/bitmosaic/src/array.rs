//! Rank and membership over an owned `Array` container's sorted `u16` plane.
//!
//! Halved without a branch, then counted (D19/D56 of the ordered-index
//! study). Each halving step compares one member and selects the next
//! bracket, so the loop's trip count depends on the plane's length alone;
//! 4,096 members take seven steps. The last bracket is counted over a fixed
//! [`LEAF`]-slot window that ends inside the plane, so that count has a
//! constant trip count, no early exit and no branch; on AArch64 it compiles
//! to two 32-byte loads, four 8-lane `cmhi`, three adds and one `addv`. A
//! window that starts before the bracket counts only members already known
//! to be below the target; one that ends past it, only members known not to
//! be.
//!
//! The borrowed view keeps its early-exit bisection (`wire::array_search`),
//! which won the dependent-chain cells over wire bytes.

/// Slots in the final counted window: 64 bytes, four 8-lane compares.
const LEAF: usize = 32;

/// Members below `v`, summed in `u16` lanes: at most [`LEAF`] are counted,
/// and a narrow accumulator keeps the compare lanes at the slot width
/// instead of widening each one to a word.
#[inline(always)]
fn count_below(members: &[u16], v: u16) -> usize {
    usize::from(
        members
            .iter()
            .fold(0u16, |below, &member| below + u16::from(member < v)),
    )
}

/// Members of `plane` below `v`; `plane` is sorted.
#[inline(always)]
pub(crate) fn rank(plane: &[u16], v: u16) -> usize {
    let n = plane.len();
    let Some(last_window) = n.checked_sub(LEAF) else {
        return count_below(plane, v);
    };
    // The rank lies in `lo..=lo + len`, and `lo + len <= n`.
    let (mut lo, mut len) = (0, n);
    while len > LEAF {
        let half = len / 2;
        lo = if plane[lo + half - 1] < v {
            lo + half
        } else {
            lo
        };
        len -= half;
    }
    let start = lo.min(last_window);
    let window: &[u16; LEAF] = plane[start..]
        .first_chunk()
        .expect("start <= n - LEAF, so the window ends inside the plane");
    start + count_below(window, v)
}

/// Whether `v` is a member of the sorted `plane`.
#[inline(always)]
pub(crate) fn contains(plane: &[u16], v: u16) -> bool {
    plane.get(rank(plane, v)) == Some(&v)
}

#[cfg(test)]
mod tests {
    use super::{LEAF, contains, rank};

    fn xorshift(state: &mut u64) -> u64 {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        *state
    }

    /// Every count through nine leaves, so every odd and even length the
    /// halving loop splits is met at least once, and the edges of each
    /// further halving up to `ARRAY_MAX`.
    fn sizes() -> Vec<usize> {
        let mut sizes: Vec<usize> = (0..=9 * LEAF).collect();
        sizes.extend([
            511, 512, 513, 1000, 1023, 1024, 1025, 2047, 2048, 2049, 4095, 4096,
        ]);
        sizes
    }

    fn planes(n: usize, state: &mut u64) -> Vec<Vec<u16>> {
        let spread = match n {
            0 => Vec::new(),
            1 => vec![32_768],
            _ => (0..n).map(|i| (i * 65_535 / (n - 1)) as u16).collect(),
        };
        let mut random = Vec::with_capacity(n);
        let mut seen = vec![false; 1 << 16];
        while random.len() < n {
            let v = (xorshift(state) & 0xFFFF) as u16;
            if !std::mem::replace(&mut seen[usize::from(v)], true) {
                random.push(v);
            }
        }
        random.sort_unstable();
        vec![
            spread,
            (0..n as u16).collect(),
            (65_536 - n as u32..65_536).map(|v| v as u16).collect(),
            random,
        ]
    }

    /// Exhaustive by equivalence class. Every compare the search makes is
    /// `member < v`, and for `v` anywhere in `(m[i-1], m[i]]` each such
    /// compare has one answer, so the whole search takes one path for the
    /// whole interval and only the final equality differs. Probing each
    /// member and its successor, plus both ends of the domain, therefore
    /// probes every path.
    #[test]
    fn rank_and_contains_match_bisection_on_every_path() {
        let mut state = 0x0DD5_EED5_u64;
        for n in sizes() {
            for plane in planes(n, &mut state) {
                let probes = plane
                    .iter()
                    .flat_map(|&m| [m, m.wrapping_add(1)])
                    .chain([0, u16::MAX]);
                for v in probes {
                    let want = plane.partition_point(|m| *m < v);
                    assert_eq!(rank(&plane, v), want, "rank({v}) over {n}");
                    assert_eq!(
                        contains(&plane, v),
                        plane.binary_search(&v).is_ok(),
                        "contains({v}) over {n}"
                    );
                }
            }
        }
    }
}
