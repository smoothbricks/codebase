//! Exact deduplication: the seen-set of admitted event ids.
//!
//! An id is either in the set or it is not; there is no probabilistic
//! answer, so a discard policy can never drop an id it has not seen. Two
//! carriers hold the set, chosen per row by the id's declared namespace:
//!
//! - **Bytes** — an adaptive radix trie keyed by the id's own bytes, exactly
//!   as sent, no hash in front of it. A miss ends at the first divergent
//!   byte. The trie's value is the tx of the log entry that admitted the id,
//!   which is what makes the window a range cut and the id → tx lookup a
//!   byproduct of admission ([`SeenSet::admitted_tx`]).
//! - **Ordinal** — a compressed bitmap for signal types that declare a dense
//!   ordinal id namespace: the id is the canonical decimal of a `u32`, and
//!   the carrier answers membership only.
//!
//! The window is the redelivery horizon: an id whose admitting tx is below
//! it cannot be delivered again, so [`SeenSet::cut_below`] evicts by tx
//! range and never by recency. Both carriers keep their admissions in tx
//! order, so a cut is a pop from the front.
//!
//! Sizing is the host's, declared once per open: a ceiling on the number of
//! ids the set may hold, and an admission that would exceed it is refused
//! with a named cause rather than the ceiling growing under load. The
//! carriers start empty and grow toward the ceiling (about 200 bytes per id
//! held: trie leaf, branch share, window entry) — a set for an agent that
//! sees ten ids costs ten ids, and the ceiling is the alarm for a host that
//! never cuts its window, sized from the redelivery horizon and admitted
//! rate. The ceiling is never persisted — a checkpoint carries the set, and
//! the instance restoring it carries the ceiling.
//!
//! Admission is two-phase because the admitting tx is not known until the
//! log accepts the entry: [`SeenSet::judge`] stages a batch's new ids,
//! [`SeenSet::commit`] binds them to the tx the log assigned, and
//! [`SeenSet::abandon`] retracts them when the append did not happen. A
//! staged id is already a duplicate to the rest of its batch, and a new
//! batch cannot begin while one is staged ([`SeenSet::is_batch_open`]).

use std::collections::VecDeque;
use std::fmt;

use axroar::AxroarBuilder;
use ptmcart_core::art::ArtMap;

/// Longest id the set admits: the width of a SHA-512 digest, so any content
/// hash fits and an id that does not is not an id.
pub const MAX_ID_BYTES: usize = 64;

/// Value of a staged (judged, not yet committed) key in the trie. A real tx
/// never reaches it: the log's tx space is bounded far below.
const STAGED: u64 = u64::MAX;

/// Serialized bytes of one Bytes-namespace admission: tx, length, key bytes.
const BYTES_ENTRY_FIXED: usize = 8 + 1;
/// Serialized bytes of one Ordinal-namespace admission: tx, ordinal.
const ORDINAL_ENTRY: usize = 8 + 4;

/// What happens to a duplicate: `Latest` processes it (the newer entry
/// replaces), `Discard` refuses it (the first entry stands). The `u8` values
/// are ABI.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum CollisionPolicy {
    Latest = 0,
    Discard = 1,
}

impl CollisionPolicy {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Latest),
            1 => Some(Self::Discard),
            _ => None,
        }
    }
}

/// Which carrier judges an id: declared per signal type by the host.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IdNamespace {
    /// Any byte string up to [`MAX_ID_BYTES`].
    Bytes,
    /// The canonical decimal of a `u32`: digits only, no sign, no leading
    /// zero (except `0` itself).
    Ordinal,
}

/// An id as the trie stores it: the bytes inline, so a key costs no
/// allocation and a descent compares against the caller's slice directly.
#[derive(Clone, Copy)]
pub struct EventKey {
    len: u8,
    bytes: [u8; MAX_ID_BYTES],
}

impl EventKey {
    /// Refuses an id longer than [`MAX_ID_BYTES`] — the type is the boundary.
    pub fn new(id: &[u8]) -> Result<Self, AdmissionRefusal> {
        if id.len() > MAX_ID_BYTES {
            return Err(AdmissionRefusal::IdTooLong { len: id.len() });
        }
        let mut bytes = [0u8; MAX_ID_BYTES];
        bytes[..id.len()].copy_from_slice(id);
        Ok(Self {
            // Bounded by MAX_ID_BYTES above, which is well inside u8.
            len: id.len() as u8,
            bytes,
        })
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..usize::from(self.len)]
    }
}

impl AsRef<[u8]> for EventKey {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl PartialEq for EventKey {
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl Eq for EventKey {}

impl fmt::Debug for EventKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "EventKey({:?})",
            String::from_utf8_lossy(self.as_bytes())
        )
    }
}

/// Why an id was not admitted. Every variant names the cause; the caller
/// turns it into a refusal of the whole batch with the set untouched.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionRefusal {
    /// The id is longer than [`MAX_ID_BYTES`].
    IdTooLong { len: usize },
    /// Admitting one more id would exceed the ceiling the host declared.
    CeilingReached { ceiling: u32 },
    /// The signal type declares an ordinal namespace but the id is not the
    /// canonical decimal of a `u32`.
    NotAnOrdinal,
    /// A batch is already staged: commit or abandon it first.
    BatchOpen,
}

impl fmt::Display for AdmissionRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IdTooLong { len } => write!(
                f,
                "event id is {len} bytes; an id is at most {MAX_ID_BYTES} bytes (the SHA-512 width) — send a digest of the id, not the id"
            ),
            Self::CeilingReached { ceiling } => write!(
                f,
                "the seen-set holds its declared ceiling of {ceiling} ids; cut the window below the redelivery horizon or open the processor with a higher ceiling"
            ),
            Self::NotAnOrdinal => write!(
                f,
                "the signal type declares an ordinal id namespace but the id is not the canonical decimal of a u32 — send the ordinal, or drop the namespace declaration"
            ),
            Self::BatchOpen => write!(
                f,
                "the previous batch is still staged; commit it with the tx the log assigned, or abandon it when the append did not happen, before judging another"
            ),
        }
    }
}

/// The verdict on one id within a batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Judgment {
    /// Not in the set: staged for this batch.
    New,
    /// Already in the set. `admitted_tx` is the tx that admitted it, `None`
    /// when it was staged by this same batch or lives in the ordinal
    /// carrier, which keeps membership only.
    Duplicate { admitted_tx: Option<u64> },
}

/// Parse the canonical decimal of a `u32`: no sign, no leading zero, no
/// empty string, no overflow.
fn parse_ordinal(id: &[u8]) -> Option<u32> {
    match id {
        [] => None,
        [b'0'] => Some(0),
        [b'0', ..] => None,
        digits => {
            let mut value: u32 = 0;
            for byte in digits {
                let digit = byte.checked_sub(b'0').filter(|d| *d <= 9)?;
                value = value.checked_mul(10)?.checked_add(u32::from(digit))?;
            }
            Some(value)
        }
    }
}

/// One Bytes-namespace admission in tx order.
#[derive(Clone, Copy, Debug)]
struct BytesAdmission {
    tx: u64,
    key: EventKey,
}

/// One Ordinal-namespace admission in tx order.
#[derive(Clone, Copy, Debug)]
struct OrdinalAdmission {
    tx: u64,
    ordinal: u32,
}

/// The exact seen-set. See the module documentation for the model.
pub struct SeenSet {
    policy: CollisionPolicy,
    ceiling: u32,
    trie: ArtMap<EventKey, u64>,
    /// Bytes-namespace admissions in tx order: the window.
    bytes_window: VecDeque<BytesAdmission>,
    ordinal: AxroarBuilder,
    ordinal_len: u32,
    /// Ordinal-namespace admissions in tx order.
    ordinal_window: VecDeque<OrdinalAdmission>,
    /// Keys this batch staged as new; committed to the batch's tx, or
    /// removed on abandon.
    staged_keys: Vec<EventKey>,
    staged_ordinals: Vec<u32>,
    batch_open: bool,
    batch_total: u64,
    batch_duplicates: u64,
    /// Serialized size of the Bytes window, maintained at admission and
    /// eviction so a checkpoint never walks the window to size itself.
    bytes_window_serialized: usize,
    pub total_events: u64,
    pub duplicates_detected: u64,
}

impl fmt::Debug for SeenSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SeenSet")
            .field("policy", &self.policy)
            .field("ceiling", &self.ceiling)
            .field("bytes", &self.trie.len())
            .field("ordinals", &self.ordinal_len)
            .field("batch_open", &self.batch_open)
            .field("total_events", &self.total_events)
            .field("duplicates_detected", &self.duplicates_detected)
            .finish()
    }
}

impl SeenSet {
    /// A set that may hold at most `ceiling` ids across both carriers, judged
    /// in batches of at most `batch_rows` rows. The batch staging is
    /// reserved here so judging a batch allocates nothing; the carriers grow
    /// with what they hold.
    pub fn new(policy: CollisionPolicy, ceiling: u32, batch_rows: u32) -> Self {
        let batch_rows = batch_rows as usize;
        Self {
            policy,
            ceiling,
            trie: ArtMap::new(),
            bytes_window: VecDeque::new(),
            ordinal: AxroarBuilder::new(),
            ordinal_len: 0,
            ordinal_window: VecDeque::new(),
            staged_keys: Vec::with_capacity(batch_rows),
            staged_ordinals: Vec::with_capacity(batch_rows),
            batch_open: false,
            batch_total: 0,
            batch_duplicates: 0,
            bytes_window_serialized: 0,
            total_events: 0,
            duplicates_detected: 0,
        }
    }

    pub fn policy(&self) -> CollisionPolicy {
        self.policy
    }

    pub fn ceiling(&self) -> u32 {
        self.ceiling
    }

    /// Ids held, staged ones included.
    pub fn len(&self) -> u32 {
        // Both counts are bounded by the ceiling, a u32.
        self.trie.len() as u32 + self.ordinal_len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// True between the first `judge` of a batch and its commit or abandon.
    pub fn is_batch_open(&self) -> bool {
        self.batch_open
    }

    /// Judge one id of the current batch. The first judgment opens the batch.
    pub fn judge(
        &mut self,
        id: &[u8],
        namespace: IdNamespace,
    ) -> Result<Judgment, AdmissionRefusal> {
        let judgment = match namespace {
            IdNamespace::Bytes => self.judge_bytes(id)?,
            IdNamespace::Ordinal => self.judge_ordinal(id)?,
        };
        self.batch_open = true;
        self.batch_total += 1;
        if matches!(judgment, Judgment::Duplicate { .. }) {
            self.batch_duplicates += 1;
        }
        Ok(judgment)
    }

    /// `judge`, folded through the policy: whether the row is processed.
    pub fn should_process(
        &mut self,
        id: &[u8],
        namespace: IdNamespace,
    ) -> Result<bool, AdmissionRefusal> {
        Ok(match self.judge(id, namespace)? {
            Judgment::New => true,
            Judgment::Duplicate { .. } => self.policy == CollisionPolicy::Latest,
        })
    }

    fn judge_bytes(&mut self, id: &[u8]) -> Result<Judgment, AdmissionRefusal> {
        let key = EventKey::new(id)?;
        if let Some(tx) = self.trie.get(key.as_bytes()) {
            // The window key stays the FIRST admission under every policy:
            // `Latest` processes a redelivery whether or not the set still
            // remembers it, so refreshing the tx would only add a window
            // entry per redelivery, and `admitted_tx` names the entry that
            // admitted the id.
            return Ok(Judgment::Duplicate {
                admitted_tx: (*tx != STAGED).then_some(*tx),
            });
        }
        self.reserve_one()?;
        self.trie.insert(key, STAGED);
        self.staged_keys.push(key);
        Ok(Judgment::New)
    }

    fn judge_ordinal(&mut self, id: &[u8]) -> Result<Judgment, AdmissionRefusal> {
        let ordinal = parse_ordinal(id).ok_or(AdmissionRefusal::NotAnOrdinal)?;
        if self.ordinal.contains(ordinal) {
            return Ok(Judgment::Duplicate { admitted_tx: None });
        }
        self.reserve_one()?;
        self.ordinal.insert(ordinal);
        self.ordinal_len += 1;
        self.staged_ordinals.push(ordinal);
        Ok(Judgment::New)
    }

    fn reserve_one(&mut self) -> Result<(), AdmissionRefusal> {
        if self.len() >= self.ceiling {
            return Err(AdmissionRefusal::CeilingReached {
                ceiling: self.ceiling,
            });
        }
        Ok(())
    }

    /// Bind the staged batch to the tx the log assigned it. `tx` must not be
    /// below the newest admission: the window is tx-ordered by construction.
    pub fn commit(&mut self, tx: u64) {
        debug_assert!(tx != STAGED, "the staged sentinel is not a tx");
        debug_assert!(
            self.bytes_window.back().is_none_or(|last| last.tx <= tx)
                && self.ordinal_window.back().is_none_or(|last| last.tx <= tx),
            "a batch commits at or above the newest admission's tx",
        );
        for key in self.staged_keys.drain(..) {
            if let Some(value) = self.trie.get_mut(key.as_bytes()) {
                *value = tx;
            }
            self.bytes_window.push_back(BytesAdmission { tx, key });
            self.bytes_window_serialized += BYTES_ENTRY_FIXED + key.as_bytes().len();
        }
        for ordinal in self.staged_ordinals.drain(..) {
            self.ordinal_window
                .push_back(OrdinalAdmission { tx, ordinal });
        }
        self.total_events += self.batch_total;
        self.duplicates_detected += self.batch_duplicates;
        self.close_batch();
    }

    /// Retract the staged batch: the append did not happen, so nothing in it
    /// was admitted and the counters do not move.
    pub fn abandon(&mut self) {
        for key in self.staged_keys.drain(..) {
            self.trie.remove(key.as_bytes());
        }
        for ordinal in self.staged_ordinals.drain(..) {
            self.ordinal.remove(ordinal);
            self.ordinal_len -= 1;
        }
        self.close_batch();
    }

    fn close_batch(&mut self) {
        self.batch_open = false;
        self.batch_total = 0;
        self.batch_duplicates = 0;
    }

    /// Empty the set, keeping every reservation.
    pub fn clear(&mut self) {
        self.trie.clear();
        self.bytes_window.clear();
        self.bytes_window_serialized = 0;
        self.ordinal.clear_retaining();
        self.ordinal_len = 0;
        self.ordinal_window.clear();
        self.staged_keys.clear();
        self.staged_ordinals.clear();
        self.close_batch();
        self.total_events = 0;
        self.duplicates_detected = 0;
    }

    /// Refuse to open a batch while one is staged.
    pub fn require_no_open_batch(&self) -> Result<(), AdmissionRefusal> {
        if self.batch_open {
            return Err(AdmissionRefusal::BatchOpen);
        }
        Ok(())
    }

    /// The tx that admitted `id` in the Bytes namespace: the byproduct read.
    /// `None` for an id that is absent, staged, or in the ordinal carrier.
    pub fn admitted_tx(&self, id: &[u8]) -> Option<u64> {
        let tx = *self.trie.get(id)?;
        (tx != STAGED).then_some(tx)
    }

    /// Whether `id` in the Ordinal namespace is a member.
    pub fn contains_ordinal(&self, id: &[u8]) -> bool {
        parse_ordinal(id).is_some_and(|ordinal| self.ordinal.contains(ordinal))
    }

    /// Evict every admission whose tx is below `horizon`. Returns how many
    /// ids left the set.
    pub fn cut_below(&mut self, horizon: u64) -> u32 {
        let mut evicted = 0u32;
        while let Some(front) = self.bytes_window.front() {
            if front.tx >= horizon {
                break;
            }
            let key = front.key;
            self.bytes_window.pop_front();
            self.bytes_window_serialized -= BYTES_ENTRY_FIXED + key.as_bytes().len();
            if self.trie.remove(key.as_bytes()).is_some() {
                evicted += 1;
            }
        }
        while let Some(front) = self.ordinal_window.front() {
            if front.tx >= horizon {
                break;
            }
            let ordinal = front.ordinal;
            self.ordinal_window.pop_front();
            if self.ordinal.remove(ordinal) {
                self.ordinal_len -= 1;
                evicted += 1;
            }
        }
        evicted
    }

    /// Bytes a checkpoint of the committed set takes; staged ids are not
    /// admitted and are not written.
    pub fn checkpoint_len(&self) -> usize {
        checkpoint::HEADER_SIZE
            + self.bytes_window_serialized
            + self.ordinal_window.len() * ORDINAL_ENTRY
    }

    /// Write the committed set into `output`; `None` when it does not fit.
    pub fn checkpoint(&self, output: &mut [u8]) -> Option<usize> {
        checkpoint::serialize(self, output)
    }

    /// Replace the set's contents with a checkpoint's, keeping this
    /// instance's policy and ceiling. The window order is the tx order.
    pub fn restore(&mut self, input: &[u8]) -> Result<(), checkpoint::DeserializeError> {
        checkpoint::deserialize_into(self, input)
    }
}

/// Checkpoint bytes: the committed window in tx order, which rebuilds both
/// carriers on restore. Little-endian throughout.
///
/// ```text
/// [magic u32 "CHKP"][version u8 = 2][policy u8][pad u16]
/// [bytes_count u32][ordinal_count u32][total_events u64][duplicates u64]
/// bytes_count × [tx u64][len u8][key len bytes]
/// ordinal_count × [tx u64][ordinal u32]
/// ```
pub mod checkpoint {
    use super::{
        BYTES_ENTRY_FIXED, BytesAdmission, CollisionPolicy, EventKey, MAX_ID_BYTES, ORDINAL_ENTRY,
        OrdinalAdmission, SeenSet,
    };

    pub const CHECKPOINT_MAGIC: u32 = 0x4348_4B50; // "CHKP"
    pub const CHECKPOINT_VERSION: u8 = 2;
    pub const HEADER_SIZE: usize = 32;

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub enum DeserializeError {
        /// Not a checkpoint of this version, or truncated.
        InvalidCheckpoint,
        /// The checkpoint's policy is not this instance's.
        PolicyMismatch,
        /// The checkpoint holds more ids than this instance's ceiling.
        ExceedsCeiling,
    }

    pub fn serialize(set: &SeenSet, output: &mut [u8]) -> Option<usize> {
        let total = set.checkpoint_len();
        if output.len() < total {
            return None;
        }
        output[0..4].copy_from_slice(&CHECKPOINT_MAGIC.to_le_bytes());
        output[4] = CHECKPOINT_VERSION;
        output[5] = set.policy as u8;
        output[6..8].fill(0);
        // Both windows are bounded by the ceiling, a u32.
        output[8..12].copy_from_slice(&(set.bytes_window.len() as u32).to_le_bytes());
        output[12..16].copy_from_slice(&(set.ordinal_window.len() as u32).to_le_bytes());
        output[16..24].copy_from_slice(&set.total_events.to_le_bytes());
        output[24..32].copy_from_slice(&set.duplicates_detected.to_le_bytes());
        let mut offset = HEADER_SIZE;
        for admission in &set.bytes_window {
            let key = admission.key.as_bytes();
            output[offset..offset + 8].copy_from_slice(&admission.tx.to_le_bytes());
            output[offset + 8] = admission.key.len;
            output[offset + 9..offset + 9 + key.len()].copy_from_slice(key);
            offset += BYTES_ENTRY_FIXED + key.len();
        }
        for admission in &set.ordinal_window {
            output[offset..offset + 8].copy_from_slice(&admission.tx.to_le_bytes());
            output[offset + 8..offset + 12].copy_from_slice(&admission.ordinal.to_le_bytes());
            offset += ORDINAL_ENTRY;
        }
        debug_assert_eq!(offset, total);
        Some(total)
    }

    fn read_u32(input: &[u8], at: usize) -> Option<u32> {
        input
            .get(at..at + 4)
            .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn read_u64(input: &[u8], at: usize) -> Option<u64> {
        input
            .get(at..at + 8)
            .map(|b| u64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]))
    }

    pub fn deserialize_into(set: &mut SeenSet, input: &[u8]) -> Result<(), DeserializeError> {
        use DeserializeError::InvalidCheckpoint;
        if input.len() < HEADER_SIZE
            || read_u32(input, 0) != Some(CHECKPOINT_MAGIC)
            || input[4] != CHECKPOINT_VERSION
        {
            return Err(InvalidCheckpoint);
        }
        let policy = CollisionPolicy::from_u8(input[5]).ok_or(InvalidCheckpoint)?;
        if policy != set.policy {
            return Err(DeserializeError::PolicyMismatch);
        }
        let bytes_count = read_u32(input, 8).ok_or(InvalidCheckpoint)?;
        let ordinal_count = read_u32(input, 12).ok_or(InvalidCheckpoint)?;
        let total_events = read_u64(input, 16).ok_or(InvalidCheckpoint)?;
        let duplicates = read_u64(input, 24).ok_or(InvalidCheckpoint)?;
        if u64::from(bytes_count) + u64::from(ordinal_count) > u64::from(set.ceiling) {
            return Err(DeserializeError::ExceedsCeiling);
        }

        // Walk the bytes once before touching the set — lengths, bounds,
        // tx order — so a checkpoint that does not parse leaves the instance
        // as it was and the build below cannot fail on structure.
        let mut offset = HEADER_SIZE;
        let mut last_tx = 0u64;
        for _ in 0..bytes_count {
            let tx = read_u64(input, offset).ok_or(InvalidCheckpoint)?;
            let len = usize::from(*input.get(offset + 8).ok_or(InvalidCheckpoint)?);
            if len > MAX_ID_BYTES
                || tx < last_tx
                || tx == super::STAGED
                || input.get(offset + 9..offset + 9 + len).is_none()
            {
                return Err(InvalidCheckpoint);
            }
            last_tx = tx;
            offset += BYTES_ENTRY_FIXED + len;
        }
        last_tx = 0;
        for _ in 0..ordinal_count {
            let tx = read_u64(input, offset).ok_or(InvalidCheckpoint)?;
            if tx < last_tx || tx == super::STAGED || read_u32(input, offset + 8).is_none() {
                return Err(InvalidCheckpoint);
            }
            last_tx = tx;
            offset += ORDINAL_ENTRY;
        }
        if offset != input.len() {
            return Err(InvalidCheckpoint);
        }

        // Build into the reserved carriers. The only refusal left is an
        // ordinal listed twice, which no writer produces; it empties the set
        // because a checkpoint that lies has no restorable state.
        set.clear();
        set.total_events = total_events;
        set.duplicates_detected = duplicates;
        offset = HEADER_SIZE;
        for _ in 0..bytes_count {
            let tx = read_u64(input, offset).ok_or(InvalidCheckpoint)?;
            let len = usize::from(input[offset + 8]);
            let key = EventKey::new(&input[offset + 9..offset + 9 + len])
                .map_err(|_| InvalidCheckpoint)?;
            set.trie.insert(key, tx);
            set.bytes_window.push_back(BytesAdmission { tx, key });
            set.bytes_window_serialized += BYTES_ENTRY_FIXED + len;
            offset += BYTES_ENTRY_FIXED + len;
        }
        for _ in 0..ordinal_count {
            let tx = read_u64(input, offset).ok_or(InvalidCheckpoint)?;
            let value = read_u32(input, offset + 8).ok_or(InvalidCheckpoint)?;
            if !set.ordinal.insert(value) {
                set.clear();
                return Err(InvalidCheckpoint);
            }
            set.ordinal_len += 1;
            set.ordinal_window
                .push_back(OrdinalAdmission { tx, ordinal: value });
            offset += ORDINAL_ENTRY;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::BTreeMap;

    const B: IdNamespace = IdNamespace::Bytes;
    const O: IdNamespace = IdNamespace::Ordinal;

    #[test]
    fn discard_refuses_only_what_it_has_seen() {
        let mut set = SeenSet::new(CollisionPolicy::Discard, 8, 8);
        assert_eq!(set.should_process(b"event-001", B), Ok(true));
        assert_eq!(set.should_process(b"event-001", B), Ok(false));
        assert_eq!(set.should_process(b"event-002", B), Ok(true));
        set.commit(1);
        assert_eq!(set.total_events, 3);
        assert_eq!(set.duplicates_detected, 1);
        assert_eq!(set.should_process(b"event-001", B), Ok(false));
        assert_eq!(set.should_process(b"event-00", B), Ok(true));
        assert_eq!(set.should_process(b"event-0011", B), Ok(true));
        set.commit(2);
        assert_eq!(set.admitted_tx(b"event-001"), Some(1));
        assert_eq!(set.admitted_tx(b"event-00"), Some(2));
        assert_eq!(set.admitted_tx(b"event-0011"), Some(2));
        assert_eq!(set.admitted_tx(b"event"), None);
    }

    #[test]
    fn latest_processes_duplicates_and_keeps_the_first_admission() {
        let mut set = SeenSet::new(CollisionPolicy::Latest, 8, 8);
        assert_eq!(set.should_process(b"a", B), Ok(true));
        set.commit(1);
        assert_eq!(set.should_process(b"a", B), Ok(true));
        assert_eq!(
            set.judge(b"a", B),
            Ok(Judgment::Duplicate {
                admitted_tx: Some(1)
            })
        );
        set.commit(5);
        assert_eq!(set.duplicates_detected, 2);
        assert_eq!(set.admitted_tx(b"a"), Some(1));
        assert_eq!(set.len(), 1, "a redelivery adds no window entry");
        assert_eq!(set.cut_below(2), 1);
        assert_eq!(set.admitted_tx(b"a"), None);
        assert!(set.is_empty());
        // Past the horizon the id is new again, and Latest processes it.
        assert_eq!(set.should_process(b"a", B), Ok(true));
    }

    #[test]
    fn abandon_retracts_the_batch_and_its_counts() {
        let mut set = SeenSet::new(CollisionPolicy::Discard, 8, 8);
        assert_eq!(set.should_process(b"x", B), Ok(true));
        assert_eq!(set.should_process(b"7", O), Ok(true));
        assert!(set.is_batch_open());
        set.abandon();
        assert!(!set.is_batch_open());
        assert!(set.is_empty());
        assert_eq!(set.total_events, 0);
        assert_eq!(set.should_process(b"x", B), Ok(true));
        assert_eq!(set.should_process(b"7", O), Ok(true));
        set.commit(1);
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn a_staged_batch_blocks_the_next() {
        let mut set = SeenSet::new(CollisionPolicy::Discard, 8, 8);
        assert_eq!(set.require_no_open_batch(), Ok(()));
        set.judge(b"x", B).unwrap();
        assert_eq!(
            set.require_no_open_batch(),
            Err(AdmissionRefusal::BatchOpen)
        );
        set.commit(1);
        assert_eq!(set.require_no_open_batch(), Ok(()));
    }

    #[test]
    fn the_ceiling_refuses_at_admission_and_leaves_the_set_intact() {
        let mut set = SeenSet::new(CollisionPolicy::Discard, 2, 8);
        assert_eq!(set.judge(b"a", B), Ok(Judgment::New));
        assert_eq!(set.judge(b"1", O), Ok(Judgment::New));
        assert_eq!(
            set.judge(b"b", B),
            Err(AdmissionRefusal::CeilingReached { ceiling: 2 })
        );
        assert_eq!(
            set.judge(b"2", O),
            Err(AdmissionRefusal::CeilingReached { ceiling: 2 })
        );
        // Duplicates never need room.
        assert_eq!(
            set.judge(b"a", B),
            Ok(Judgment::Duplicate { admitted_tx: None })
        );
        set.commit(1);
        assert_eq!(set.len(), 2);
        assert_eq!(set.cut_below(2), 2);
        assert_eq!(set.judge(b"b", B), Ok(Judgment::New));
    }

    #[test]
    fn ids_longer_than_the_digest_width_are_refused_by_the_type() {
        let mut set = SeenSet::new(CollisionPolicy::Discard, 8, 8);
        let long = [b'x'; MAX_ID_BYTES + 1];
        assert_eq!(
            set.judge(&long, B),
            Err(AdmissionRefusal::IdTooLong {
                len: MAX_ID_BYTES + 1
            })
        );
        let exact = [b'x'; MAX_ID_BYTES];
        assert_eq!(set.judge(&exact, B), Ok(Judgment::New));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn ordinal_namespace_is_canonical_decimal_only() {
        let mut set = SeenSet::new(CollisionPolicy::Discard, 8, 8);
        for bad in [&b""[..], b"01", b"-1", b"1a", b"4294967296", b" 1"] {
            assert_eq!(
                set.judge(bad, O),
                Err(AdmissionRefusal::NotAnOrdinal),
                "{bad:?}"
            );
        }
        assert_eq!(set.judge(b"0", O), Ok(Judgment::New));
        assert_eq!(set.judge(b"4294967295", O), Ok(Judgment::New));
        assert_eq!(
            set.judge(b"0", O),
            Ok(Judgment::Duplicate { admitted_tx: None })
        );
        set.commit(3);
        assert!(set.contains_ordinal(b"0"));
        assert!(!set.contains_ordinal(b"1"));
        // The same digits in the Bytes namespace are a different id.
        assert_eq!(set.judge(b"0", B), Ok(Judgment::New));
        set.commit(4);
        assert_eq!(set.cut_below(4), 2);
        assert!(!set.contains_ordinal(b"0"));
        assert_eq!(set.admitted_tx(b"0"), Some(4));
    }

    #[test]
    fn checkpoint_round_trips_and_keeps_the_instance_ceiling() {
        let mut set = SeenSet::new(CollisionPolicy::Latest, 16, 8);
        set.judge(b"alpha", B).unwrap();
        set.judge(b"5", O).unwrap();
        set.commit(1);
        set.judge(b"alpha", B).unwrap();
        set.judge(b"beta", B).unwrap();
        set.commit(2);
        // A staged id is not in a checkpoint.
        set.judge(b"gamma", B).unwrap();
        let mut buffer = vec![0u8; set.checkpoint_len() + 8];
        let written = set.checkpoint(&mut buffer).unwrap();
        assert_eq!(written, set.checkpoint_len());
        set.abandon();

        let mut restored = SeenSet::new(CollisionPolicy::Latest, 16, 8);
        restored.restore(&buffer[..written]).unwrap();
        assert_eq!(restored.admitted_tx(b"alpha"), Some(1));
        assert_eq!(restored.admitted_tx(b"beta"), Some(2));
        assert_eq!(restored.admitted_tx(b"gamma"), None);
        assert!(restored.contains_ordinal(b"5"));
        assert_eq!(restored.total_events, 4);
        assert_eq!(restored.duplicates_detected, 1);
        assert_eq!(restored.checkpoint_len(), written);
        assert_eq!(restored.cut_below(2), 2);
        assert_eq!(restored.admitted_tx(b"alpha"), None);
        assert_eq!(restored.admitted_tx(b"beta"), Some(2));

        let mut tight = SeenSet::new(CollisionPolicy::Latest, 2, 8);
        assert_eq!(
            tight.restore(&buffer[..written]),
            Err(checkpoint::DeserializeError::ExceedsCeiling)
        );
        assert!(tight.is_empty());
        let mut other_policy = SeenSet::new(CollisionPolicy::Discard, 16, 8);
        assert_eq!(
            other_policy.restore(&buffer[..written]),
            Err(checkpoint::DeserializeError::PolicyMismatch)
        );
        let mut truncated = SeenSet::new(CollisionPolicy::Latest, 16, 8);
        assert_eq!(
            truncated.restore(&buffer[..written - 1]),
            Err(checkpoint::DeserializeError::InvalidCheckpoint)
        );
        assert!(truncated.is_empty());
    }

    /// Ids that share long prefixes and repeat: the shapes a trie must not
    /// confuse and a probabilistic filter would.
    fn prefix_heavy_ids() -> impl Strategy<Value = Vec<Vec<u8>>> {
        let stem = prop::collection::vec(any::<u8>(), 0..48);
        let tail = prop::collection::vec(any::<u8>(), 0..16);
        (stem, prop::collection::vec(tail, 1..64)).prop_map(|(stem, tails)| {
            tails
                .into_iter()
                .map(|tail| {
                    let mut id = stem.clone();
                    id.extend(tail);
                    id
                })
                .collect()
        })
    }

    proptest! {
        /// Under Discard, no id that has never been admitted is ever
        /// refused, and every id that has been is.
        #[test]
        fn discard_never_drops_a_new_id(batches in prop::collection::vec(prefix_heavy_ids(), 1..8)) {
            let mut set = SeenSet::new(CollisionPolicy::Discard, 1 << 16, 256);
            let mut model: BTreeMap<Vec<u8>, u64> = BTreeMap::new();
            for (index, batch) in batches.iter().enumerate() {
                let tx = index as u64 + 1;
                for id in batch {
                    let expected = !model.contains_key(id);
                    prop_assert_eq!(set.should_process(id, B).unwrap(), expected);
                    model.entry(id.clone()).or_insert(tx);
                }
                set.commit(tx);
            }
            for (id, tx) in &model {
                prop_assert_eq!(set.admitted_tx(id), Some(*tx));
            }
        }

        /// Every admitted id resolves to its admitting tx until the cut
        /// passes it, and never after.
        #[test]
        fn admitted_tx_holds_until_the_range_cut(
            batches in prop::collection::vec(prefix_heavy_ids(), 1..8),
            horizon in 0u64..10,
        ) {
            let mut set = SeenSet::new(CollisionPolicy::Discard, 1 << 16, 256);
            let mut model: BTreeMap<Vec<u8>, u64> = BTreeMap::new();
            for (index, batch) in batches.iter().enumerate() {
                let tx = index as u64 + 1;
                for id in batch {
                    set.judge(id, B).unwrap();
                    model.entry(id.clone()).or_insert(tx);
                }
                set.commit(tx);
            }
            let evicted = set.cut_below(horizon);
            let expected_evicted = model.values().filter(|tx| **tx < horizon).count();
            prop_assert_eq!(evicted as usize, expected_evicted);
            for (id, tx) in &model {
                let expected = (*tx >= horizon).then_some(*tx);
                prop_assert_eq!(set.admitted_tx(id), expected);
            }
            prop_assert_eq!(set.len() as usize, model.len() - expected_evicted);
        }

        /// The ordinal carrier agrees with a model set on membership.
        #[test]
        fn ordinal_membership_is_exact(
            batches in prop::collection::vec(prop::collection::vec(0u32..5000, 1..64), 1..6),
            horizon in 0u64..8,
        ) {
            let mut set = SeenSet::new(CollisionPolicy::Discard, 1 << 16, 256);
            let mut model: BTreeMap<u32, u64> = BTreeMap::new();
            for (index, batch) in batches.iter().enumerate() {
                let tx = index as u64 + 1;
                for ordinal in batch {
                    let id = ordinal.to_string();
                    let expected = !model.contains_key(ordinal);
                    prop_assert_eq!(set.should_process(id.as_bytes(), O).unwrap(), expected);
                    model.entry(*ordinal).or_insert(tx);
                }
                set.commit(tx);
            }
            set.cut_below(horizon);
            for (ordinal, tx) in &model {
                prop_assert_eq!(set.contains_ordinal(ordinal.to_string().as_bytes()), *tx >= horizon);
            }
        }

        /// A checkpoint restores exactly the committed set.
        #[test]
        fn checkpoint_is_lossless(batches in prop::collection::vec(prefix_heavy_ids(), 1..5)) {
            let mut set = SeenSet::new(CollisionPolicy::Latest, 1 << 16, 256);
            for (index, batch) in batches.iter().enumerate() {
                for id in batch {
                    set.judge(id, B).unwrap();
                }
                set.commit(index as u64 + 1);
            }
            let mut buffer = vec![0u8; set.checkpoint_len()];
            let written = set.checkpoint(&mut buffer).unwrap();
            let mut restored = SeenSet::new(CollisionPolicy::Latest, 1 << 16, 256);
            restored.restore(&buffer[..written]).unwrap();
            for batch in &batches {
                for id in batch {
                    prop_assert_eq!(restored.admitted_tx(id), set.admitted_tx(id));
                }
            }
            prop_assert_eq!(restored.len(), set.len());
            prop_assert_eq!(restored.checkpoint_len(), written);
        }
    }
}
