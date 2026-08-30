use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const DEFAULT_JOURNAL_BYTES: usize = 32 * 1024 * 1024;
const MAX_JOURNAL_CHUNK_BYTES: usize = 64 * 1024;
const ALT_SCREEN_SCAN_TAIL_BYTES: usize = 64;
static NEXT_STREAM_GENERATION: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RawPtyRange {
    pub(crate) start_seq: u64,
    pub(crate) data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RawPtyAttachPlan {
    pub(crate) stream_id: u64,
    pub(crate) start_seq: u64,
    pub(crate) snapshot: Option<Vec<u8>>,
}

#[derive(Debug)]
struct JournalChunk {
    start_seq: u64,
    data: Vec<u8>,
    head: usize,
}

impl JournalChunk {
    fn retained(&self) -> &[u8] {
        &self.data[self.head..]
    }

    fn retained_len(&self) -> usize {
        self.data.len().saturating_sub(self.head)
    }
}

#[derive(Debug)]
struct RawPtyStreamState {
    stream_id: u64,
    end_seq: u64,
    retained_bytes: usize,
    chunks: VecDeque<JournalChunk>,
    primary_snapshot: Option<Vec<u8>>,
    control_scan_tail: Vec<u8>,
}

#[derive(Debug)]
struct RawPtyStreamShared {
    state: Mutex<RawPtyStreamState>,
    changed: Condvar,
    subscribers: AtomicUsize,
}

/// Parser-synchronized, bounded journal of bytes read from one PTY generation.
#[derive(Debug, Clone)]
pub(crate) struct RawPtyStream {
    shared: Arc<RawPtyStreamShared>,
    max_bytes: usize,
}

/// Keeps publication notifications enabled while a raw client pump is active.
#[derive(Debug)]
pub(crate) struct RawPtySubscription {
    stream: RawPtyStream,
}

impl Drop for RawPtySubscription {
    fn drop(&mut self) {
        self.stream
            .shared
            .subscribers
            .fetch_sub(1, Ordering::AcqRel);
        self.stream.notify_waiters();
    }
}

impl RawPtyStream {
    pub(crate) fn new() -> Self {
        Self::with_capacity(DEFAULT_JOURNAL_BYTES)
    }

    fn with_capacity(max_bytes: usize) -> Self {
        let generation = NEXT_STREAM_GENERATION.fetch_add(1, Ordering::Relaxed);
        let process_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        let stream_id = process_epoch
            .rotate_left(17)
            .wrapping_add(u64::from(std::process::id()).rotate_left(32))
            .wrapping_add(generation);
        Self {
            shared: Arc::new(RawPtyStreamShared {
                state: Mutex::new(RawPtyStreamState {
                    stream_id,
                    end_seq: 0,
                    retained_bytes: 0,
                    chunks: VecDeque::new(),
                    primary_snapshot: None,
                    control_scan_tail: Vec::new(),
                }),
                changed: Condvar::new(),
                subscribers: AtomicUsize::new(0),
            }),
            max_bytes,
        }
    }

    pub(crate) fn entering_alternate_screen(&self, bytes: &[u8]) -> bool {
        let mut state = self.lock();
        let mut candidate = Vec::with_capacity(state.control_scan_tail.len() + bytes.len());
        candidate.extend_from_slice(&state.control_scan_tail);
        candidate.extend_from_slice(bytes);
        let entering = contains_alternate_screen_enter(&candidate);
        let keep = candidate.len().min(ALT_SCREEN_SCAN_TAIL_BYTES);
        state.control_scan_tail.clear();
        state
            .control_scan_tail
            .extend_from_slice(&candidate[candidate.len().saturating_sub(keep)..]);
        entering
    }

    pub(crate) fn subscribe(&self) -> RawPtySubscription {
        self.shared.subscribers.fetch_add(1, Ordering::AcqRel);
        RawPtySubscription {
            stream: self.clone(),
        }
    }

    /// Holds the stream lock while parsing and publishing, making snapshots and
    /// resume cutoffs atomic with respect to the PTY read boundary.
    pub(crate) fn process_read<T>(
        &self,
        bytes: &[u8],
        primary_snapshot: Option<Vec<u8>>,
        parse: impl FnOnce() -> T,
    ) -> T {
        let mut state = self.lock();
        if let Some(primary_snapshot) = primary_snapshot {
            state.primary_snapshot = Some(primary_snapshot);
        }
        let result = parse();
        state.append(bytes, self.max_bytes);
        drop(state);
        if self.shared.subscribers.load(Ordering::Acquire) != 0 {
            self.notify_waiters();
        }
        result
    }

    pub(crate) fn attach(
        &self,
        resume: Option<(u64, u64)>,
        snapshot: impl FnOnce(Option<&[u8]>) -> Vec<u8>,
    ) -> RawPtyAttachPlan {
        let state = self.lock();
        let earliest = state.earliest_seq();
        if let Some((stream_id, parsed_seq)) = resume {
            if stream_id == state.stream_id && parsed_seq >= earliest && parsed_seq <= state.end_seq
            {
                return RawPtyAttachPlan {
                    stream_id: state.stream_id,
                    start_seq: parsed_seq,
                    snapshot: None,
                };
            }
        }
        RawPtyAttachPlan {
            stream_id: state.stream_id,
            start_seq: state.end_seq,
            snapshot: Some(snapshot(state.primary_snapshot.as_deref())),
        }
    }

    pub(crate) fn range(
        &self,
        stream_id: u64,
        start_seq: u64,
        max_bytes: usize,
    ) -> Option<RawPtyRange> {
        let state = self.lock();
        if stream_id != state.stream_id
            || start_seq < state.earliest_seq()
            || start_seq > state.end_seq
        {
            return None;
        }
        let mut data = Vec::with_capacity(max_bytes.min(state.retained_bytes));
        for chunk in &state.chunks {
            let retained = chunk.retained();
            let chunk_end = chunk.start_seq.saturating_add(retained.len() as u64);
            if chunk_end <= start_seq {
                continue;
            }
            let offset = start_seq.saturating_sub(chunk.start_seq) as usize;
            let available = &retained[offset.min(retained.len())..];
            let take = available.len().min(max_bytes.saturating_sub(data.len()));
            data.extend_from_slice(&available[..take]);
            if data.len() == max_bytes {
                break;
            }
        }
        Some(RawPtyRange { start_seq, data })
    }

    pub(crate) fn wait_for_change(&self, timeout: Duration) {
        let state = self.lock();
        let _ = self
            .shared
            .changed
            .wait_timeout(state, timeout)
            .unwrap_or_else(|poisoned| poisoned.into_inner());
    }

    pub(crate) fn notify_waiters(&self) {
        self.shared.changed.notify_all();
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, RawPtyStreamState> {
        self.shared
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

fn contains_alternate_screen_enter(bytes: &[u8]) -> bool {
    let mut index = 0;
    while index + 3 < bytes.len() {
        if bytes[index..].starts_with(b"\x1b[?") {
            let params_start = index + 3;
            let mut end = params_start;
            while end < bytes.len() && (bytes[end].is_ascii_digit() || bytes[end] == b';') {
                end += 1;
            }
            if end < bytes.len()
                && bytes[end] == b'h'
                && bytes[params_start..end]
                    .split(|byte| *byte == b';')
                    .any(|param| matches!(param, b"47" | b"1047" | b"1049"))
            {
                return true;
            }
        }
        index += 1;
    }
    false
}

impl RawPtyStreamState {
    fn earliest_seq(&self) -> u64 {
        self.chunks
            .front()
            .map_or(self.end_seq, |chunk| chunk.start_seq)
    }

    fn append(&mut self, bytes: &[u8], max_bytes: usize) {
        if bytes.is_empty() {
            return;
        }
        let mut remaining = bytes;
        while !remaining.is_empty() {
            let start_seq = self.end_seq;
            let append_to_back = self
                .chunks
                .back()
                .is_some_and(|chunk| chunk.data.len() < MAX_JOURNAL_CHUNK_BYTES);
            if append_to_back {
                let back = self.chunks.back_mut().expect("checked journal tail");
                let take = remaining
                    .len()
                    .min(MAX_JOURNAL_CHUNK_BYTES.saturating_sub(back.data.len()));
                back.data.extend_from_slice(&remaining[..take]);
                remaining = &remaining[take..];
                self.end_seq = self.end_seq.saturating_add(take as u64);
                self.retained_bytes = self.retained_bytes.saturating_add(take);
            } else {
                let take = remaining.len().min(MAX_JOURNAL_CHUNK_BYTES);
                self.chunks.push_back(JournalChunk {
                    start_seq,
                    data: remaining[..take].to_vec(),
                    head: 0,
                });
                remaining = &remaining[take..];
                self.end_seq = self.end_seq.saturating_add(take as u64);
                self.retained_bytes = self.retained_bytes.saturating_add(take);
            }
        }
        while self.retained_bytes > max_bytes {
            let excess = self.retained_bytes - max_bytes;
            let Some(front) = self.chunks.front_mut() else {
                break;
            };
            let front_len = front.retained_len();
            if excess >= front_len {
                self.retained_bytes -= front_len;
                self.chunks.pop_front();
            } else {
                front.head += excess;
                front.start_seq = front.start_seq.saturating_add(excess as u64);
                self.retained_bytes -= excess;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordered_append_partial_ranges_and_trimming() {
        let stream = RawPtyStream::with_capacity(5);
        stream.process_read(b"abc", None, || ());
        stream.process_read(b"defg", None, || ());
        let id = stream.lock().stream_id;
        assert!(stream.range(id, 1, 4).is_none());
        assert_eq!(stream.range(id, 2, 3).expect("retained range").data, b"cde");
        assert_eq!(stream.range(id, 5, 8).expect("retained tail").data, b"fg");
    }

    #[test]
    fn exact_resume_and_generation_mismatch_choose_expected_cutoff() {
        let stream = RawPtyStream::with_capacity(8);
        stream.process_read(b"abcdef", None, || ());
        let id = stream.lock().stream_id;
        let resumed = stream.attach(Some((id, 3)), |_| panic!("resume must not snapshot"));
        assert_eq!((resumed.start_seq, resumed.snapshot), (3, None));

        let mismatch = stream.attach(Some((id + 1, 3)), |_| b"snapshot".to_vec());
        assert_eq!(mismatch.start_seq, 6);
        assert_eq!(mismatch.snapshot.as_deref(), Some(b"snapshot".as_slice()));

        stream.process_read(b"ghijkl", None, || ());
        let stale = stream.attach(Some((id, 0)), |_| b"reconstructed".to_vec());
        assert_eq!(stale.start_seq, 12);
        assert_eq!(stale.snapshot.as_deref(), Some(b"reconstructed".as_slice()));
    }

    #[test]
    fn attach_cutoff_cannot_split_parse_and_publication() {
        let stream = RawPtyStream::with_capacity(8);
        let parsed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        stream.process_read(b"live", None, || parsed.store(true, Ordering::Release));
        let plan = stream.attach(None, |_| {
            assert!(parsed.load(Ordering::Acquire));
            b"state".to_vec()
        });
        assert_eq!(plan.start_seq, 4);
        assert_eq!(
            stream
                .range(plan.stream_id, plan.start_seq, 8)
                .expect("cutoff range")
                .data,
            b""
        );
    }

    #[test]
    fn tiny_reads_are_coalesced_into_bounded_journal_chunks() {
        let stream = RawPtyStream::with_capacity(MAX_JOURNAL_CHUNK_BYTES * 2);
        for _ in 0..(MAX_JOURNAL_CHUNK_BYTES + 17) {
            stream.process_read(b"x", None, || ());
        }
        let state = stream.lock();
        assert_eq!(state.retained_bytes, MAX_JOURNAL_CHUNK_BYTES + 17);
        assert_eq!(state.chunks.len(), 2);
        assert!(state
            .chunks
            .iter()
            .all(|chunk| chunk.data.len() <= MAX_JOURNAL_CHUNK_BYTES));
    }

    #[test]
    fn steady_state_trimming_advances_chunk_head_without_shifting_bytes() {
        let stream = RawPtyStream::with_capacity(MAX_JOURNAL_CHUNK_BYTES);
        stream.process_read(&vec![b'x'; MAX_JOURNAL_CHUNK_BYTES], None, || ());
        stream.process_read(b"y", None, || ());

        let state = stream.lock();
        let front = state
            .chunks
            .front()
            .expect("partially retained front chunk");
        assert_eq!(front.head, 1);
        assert_eq!(front.data.len(), MAX_JOURNAL_CHUNK_BYTES);
        assert_eq!(front.retained_len(), MAX_JOURNAL_CHUNK_BYTES - 1);
        assert_eq!(state.retained_bytes, MAX_JOURNAL_CHUNK_BYTES);
        assert_eq!(state.chunks.len(), 2);
        let stream_id = state.stream_id;
        drop(state);

        let range = stream
            .range(stream_id, 1, MAX_JOURNAL_CHUNK_BYTES)
            .expect("retained journal range");
        assert_eq!(range.data.len(), MAX_JOURNAL_CHUNK_BYTES);
        assert_eq!(range.data.last(), Some(&b'y'));
    }

    #[test]
    fn split_alternate_screen_enter_caches_primary_snapshot_for_attach() {
        let stream = RawPtyStream::with_capacity(64);
        assert!(!stream.entering_alternate_screen(b"\x1b[?10"));
        assert!(stream.entering_alternate_screen(b"49h"));
        stream.process_read(b"49h", Some(b"primary-state".to_vec()), || ());
        let plan = stream.attach(None, |primary| primary.unwrap_or_default().to_vec());
        assert_eq!(plan.snapshot.as_deref(), Some(b"primary-state".as_slice()));
    }

    #[test]
    fn stream_generations_are_unique() {
        let first = RawPtyStream::with_capacity(8).lock().stream_id;
        let second = RawPtyStream::with_capacity(8).lock().stream_id;
        assert_ne!(first, second);
    }
}
