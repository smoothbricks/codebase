//! Storage backed fallibly: a host that caps linear memory gets a refusal it
//! can name instead of an abort it cannot catch.

/// `len` copies of `value` in exactly backed storage, or `None` when the
/// memory cannot be had.
pub(crate) fn filled<T: Clone>(value: T, len: usize) -> Option<Vec<T>> {
    let mut out = Vec::new();
    out.try_reserve_exact(len).ok()?;
    out.resize(len, value);
    Some(out)
}

/// A copy of `items` in exactly backed storage, or `None`.
pub(crate) fn copied<T: Clone>(items: &[T]) -> Option<Vec<T>> {
    let mut out = Vec::new();
    out.try_reserve_exact(items.len()).ok()?;
    out.extend_from_slice(items);
    Some(out)
}
