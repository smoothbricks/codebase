//! Which released image the gateway's cleanup queue frees next (05_gateway.md "Deleting released
//! images", Danny tg#10788, tg#10789). A cut's time grows with the image's extent count, and only
//! the bytes an image does not share with its source come back when it is freed. So with no disk
//! pressure the queue frees the image of fewest extents first, keeping each burst of container
//! work short for the gates beside it; under pressure it frees the image that returns the most
//! unique bytes per extent first, the most space per second of work. An image whose releaser
//! recorded neither goes last, oldest first. Pure: the queue's shell reads the pressure and
//! carries the records.

use std::cmp::Ordering;
use std::num::NonZeroU64;

/// What the releaser knew of one released image when it handed it over, and when.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Pending {
    /// The order the queue heard of it: the smaller is the older.
    pub seq: u64,
    /// Bytes it does not share with its source: what freeing it returns. `None` when unknown.
    pub unique_bytes: Option<u64>,
    /// Its physical runs: what freeing it costs. `None` when unknown.
    pub extents: Option<NonZeroU64>,
}

/// Whether the store's container is under its free-space floor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Pressure {
    /// Free space is at or above the floor: keep each image's container work short.
    None,
    /// Free space is below the floor: return the most bytes per unit of work first.
    Below,
}

/// The position in `pending` of the image to free next, `None` when nothing is pending.
pub fn next(pending: &[Pending], pressure: Pressure) -> Option<usize> {
    pending
        .iter()
        .enumerate()
        .min_by(|(_, left), (_, right)| rank(left, right, pressure))
        .map(|(position, _)| position)
}

/// `left` before `right` is `Less`.
fn rank(left: &Pending, right: &Pending, pressure: Pressure) -> Ordering {
    let key = |pending: &Pending| match pressure {
        Pressure::None => pending.extents.map(|extents| (extents, 1_u64)),
        Pressure::Below => pending
            .unique_bytes
            .zip(pending.extents)
            .map(|(bytes, extents)| (extents, bytes)),
    };
    match (key(left), key(right)) {
        (Some(left_key), Some(right_key)) => cheaper(left_key, right_key, pressure),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
    .then(left.seq.cmp(&right.seq))
}

/// Of two known images as `(extents, bytes)`, the one to free first: fewest extents with no
/// pressure; under pressure the most bytes per extent, compared without division, then the fewest
/// extents.
fn cheaper(
    (left_extents, left_bytes): (NonZeroU64, u64),
    (right_extents, right_bytes): (NonZeroU64, u64),
    pressure: Pressure,
) -> Ordering {
    match pressure {
        Pressure::None => left_extents.cmp(&right_extents),
        Pressure::Below => {
            let left = u128::from(left_bytes) * u128::from(right_extents.get());
            let right = u128::from(right_bytes) * u128::from(left_extents.get());
            right.cmp(&left).then(left_extents.cmp(&right_extents))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pending(seq: u64, unique_bytes: Option<u64>, extents: Option<u64>) -> Pending {
        Pending {
            seq,
            unique_bytes,
            extents: extents.map(|extents| NonZeroU64::new(extents).expect("nonzero")),
        }
    }

    /// Each row: the pending images as `(unique bytes, extents)` in the order the queue heard of
    /// them, the pressure, and the order they are freed in.
    #[test]
    fn the_queue_frees_the_cheapest_cut_first_or_under_pressure_the_most_bytes_per_extent() {
        type Row = (
            &'static [(Option<u64>, Option<u64>)],
            Pressure,
            &'static [u64],
        );
        let rows: [Row; 8] = [
            (&[], Pressure::None, &[]),
            // No pressure: fewest extents first, whatever they return.
            (
                &[
                    (Some(900), Some(400)),
                    (Some(10), Some(5)),
                    (None, Some(50)),
                ],
                Pressure::None,
                &[1, 2, 0],
            ),
            // Under pressure: the most unique bytes per extent first.
            (
                &[
                    (Some(900), Some(400)),
                    (Some(10), Some(5)),
                    (Some(500), Some(50)),
                ],
                Pressure::Below,
                &[2, 0, 1],
            ),
            // A clone that shares every extent returns nothing, so it goes after any that frees.
            (
                &[(Some(0), Some(1)), (Some(64), Some(1_000))],
                Pressure::Below,
                &[1, 0],
            ),
            // An equal ratio: the fewer extents first.
            (
                &[(Some(200), Some(20)), (Some(100), Some(10))],
                Pressure::Below,
                &[1, 0],
            ),
            // No record goes last, oldest first, in either mode.
            (
                &[(None, None), (Some(1), Some(1)), (None, None)],
                Pressure::None,
                &[1, 0, 2],
            ),
            // Under pressure an image with no unique-byte record goes last even with its extents.
            (
                &[(None, Some(1)), (Some(1), Some(10))],
                Pressure::Below,
                &[1, 0],
            ),
            // A tie keeps the order the queue heard of them.
            (
                &[(Some(5), Some(5)), (Some(5), Some(5))],
                Pressure::None,
                &[0, 1],
            ),
        ];
        for (row, (images, pressure, order)) in rows.into_iter().enumerate() {
            let mut left: Vec<Pending> = (0_u64..)
                .zip(images)
                .map(|(seq, (bytes, extents))| pending(seq, *bytes, *extents))
                .collect();
            let mut freed = Vec::new();
            while let Some(position) = next(&left, pressure) {
                freed.push(left.remove(position).seq);
            }
            assert_eq!(freed, order, "row {row}");
        }
    }
}
