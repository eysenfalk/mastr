use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use crate::pane::{RawPtyAttachPlan, RawPtyStream, RawPtySubscription};
use crate::protocol::{self, RawPtyRecovery, ServerMessage};
use crate::server::client_transport::{ClientStreamWriter, ServerEvent};

const RAW_PTY_CHUNK_BYTES: usize = 64 * 1024;
const RAW_PTY_ACK_WINDOW_BYTES: u64 = 1024 * 1024;
const RAW_PTY_WAIT_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Debug)]
struct PumpState {
    start_seq: u64,
    acknowledged_seq: u64,
    sent_seq: u64,
    start_acknowledged: bool,
}

/// Cancellable control handle for one client's raw PTY delivery thread.
pub(crate) struct RawPtyPump {
    stream_id: u64,
    cancelled: Arc<AtomicBool>,
    state: Arc<(Mutex<PumpState>, Condvar)>,
    stream: RawPtyStream,
    writer: ClientStreamWriter,
}

impl RawPtyPump {
    pub(crate) fn start(
        client_id: u64,
        plan: RawPtyAttachPlan,
        stream: RawPtyStream,
        writer: ClientStreamWriter,
        server_event_tx: tokio::sync::mpsc::Sender<ServerEvent>,
    ) -> Self {
        let stream_id = plan.stream_id;
        let start_seq = plan.start_seq;
        let recovery = if plan.snapshot.is_some() {
            RawPtyRecovery::BestEffortReplay
        } else {
            RawPtyRecovery::ExactResume
        };
        let cancelled = Arc::new(AtomicBool::new(false));
        let state = Arc::new((
            Mutex::new(PumpState {
                start_seq,
                acknowledged_seq: start_seq,
                sent_seq: start_seq,
                start_acknowledged: false,
            }),
            Condvar::new(),
        ));

        let thread_cancelled = cancelled.clone();
        let thread_state = state.clone();
        let thread_stream = stream.clone();
        let thread_writer = writer.clone();
        std::thread::spawn(move || {
            let _subscription: RawPtySubscription = thread_stream.subscribe();
            let start = ServerMessage::RawPtyStreamStart {
                stream_id,
                start_seq,
                recovery,
                snapshot: plan.snapshot,
            };
            if !send_message(&thread_writer, &thread_cancelled, &start) {
                return;
            }

            let (state_lock, state_changed) = &*thread_state;
            let mut pump_state = lock_state(state_lock);
            while !thread_cancelled.load(Ordering::Acquire) && !pump_state.start_acknowledged {
                pump_state = state_changed
                    .wait(pump_state)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
            }
            drop(pump_state);

            while !thread_cancelled.load(Ordering::Acquire) {
                let (next_seq, allowance) = {
                    let pump_state = lock_state(state_lock);
                    let unacked = pump_state
                        .sent_seq
                        .saturating_sub(pump_state.acknowledged_seq);
                    (
                        pump_state.sent_seq,
                        RAW_PTY_ACK_WINDOW_BYTES.saturating_sub(unacked) as usize,
                    )
                };
                if allowance == 0 {
                    wait_for_ack_or_cancel(&thread_state, &thread_cancelled);
                    continue;
                }

                let Some(range) =
                    thread_stream.range(stream_id, next_seq, allowance.min(RAW_PTY_CHUNK_BYTES))
                else {
                    if !thread_cancelled.load(Ordering::Acquire) {
                        let _ = server_event_tx.blocking_send(ServerEvent::RawPtyStreamGap {
                            client_id,
                            stream_id,
                        });
                    }
                    return;
                };
                if range.data.is_empty() {
                    thread_stream.wait_for_change(RAW_PTY_WAIT_INTERVAL);
                    continue;
                }

                let chunk_len = range.data.len() as u64;
                {
                    let mut pump_state = lock_state(state_lock);
                    if pump_state.sent_seq != range.start_seq {
                        continue;
                    }
                    pump_state.sent_seq = pump_state.sent_seq.saturating_add(chunk_len);
                }
                let chunk = ServerMessage::RawPtyStreamChunk {
                    stream_id,
                    start_seq: range.start_seq,
                    data: range.data,
                };
                if !send_message(&thread_writer, &thread_cancelled, &chunk) {
                    return;
                }
            }
        });

        Self {
            stream_id,
            cancelled,
            state,
            stream,
            writer,
        }
    }

    pub(crate) fn stream_id(&self) -> u64 {
        self.stream_id
    }

    /// Accepts only the exact start ACK, then monotonic parser-completion ACKs
    /// that do not claim bytes which have not been sent by this pump.
    pub(crate) fn acknowledge(&self, stream_id: u64, parsed_seq: u64) -> bool {
        if stream_id != self.stream_id || self.cancelled.load(Ordering::Acquire) {
            return false;
        }
        let (state_lock, state_changed) = &*self.state;
        let mut state = lock_state(state_lock);
        if !state.start_acknowledged {
            if parsed_seq != state.start_seq {
                return false;
            }
            state.start_acknowledged = true;
        } else if parsed_seq < state.acknowledged_seq || parsed_seq > state.sent_seq {
            return false;
        }
        state.acknowledged_seq = parsed_seq;
        state_changed.notify_all();
        drop(state);
        self.stream.notify_waiters();
        true
    }

    pub(crate) fn cancel(&self) {
        if self.cancelled.swap(true, Ordering::AcqRel) {
            return;
        }
        self.writer.cancel_pending();
        self.state.1.notify_all();
        self.stream.notify_waiters();
    }
}

impl Drop for RawPtyPump {
    fn drop(&mut self) {
        self.cancel();
    }
}

fn send_message(
    writer: &ClientStreamWriter,
    cancelled: &AtomicBool,
    message: &ServerMessage,
) -> bool {
    let mut framed = Vec::new();
    if protocol::write_message(&mut framed, message).is_err() {
        return false;
    }
    writer.send(framed, cancelled).is_ok()
}

fn wait_for_ack_or_cancel(state: &Arc<(Mutex<PumpState>, Condvar)>, cancelled: &AtomicBool) {
    let (state_lock, state_changed) = &**state;
    let pump_state = lock_state(state_lock);
    if !cancelled.load(Ordering::Acquire) {
        let _ = state_changed
            .wait_timeout(pump_state, RAW_PTY_WAIT_INTERVAL)
            .unwrap_or_else(|poisoned| poisoned.into_inner());
    }
}

fn lock_state(state: &Mutex<PumpState>) -> std::sync::MutexGuard<'_, PumpState> {
    state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::client_transport::ClientWriter;

    fn decode(bytes: Vec<u8>) -> ServerMessage {
        protocol::read_message(&mut bytes.as_slice(), protocol::MAX_GRAPHICS_FRAME_SIZE)
            .expect("framed server message")
    }

    #[test]
    fn waits_for_exact_start_ack_and_sends_contiguous_chunks() {
        let stream = RawPtyStream::new();
        stream.process_read(b"before", None, || ());
        let plan = stream.attach(None, |_| b"snapshot".to_vec());
        let (control_tx, _control_rx) = std::sync::mpsc::channel();
        let (render_tx, render_rx) = std::sync::mpsc::sync_channel(8);
        let writer = ClientWriter::test_channel(control_tx, render_tx);
        let (event_tx, _event_rx) = tokio::sync::mpsc::channel(4);
        writer.begin_stream();
        let pump = RawPtyPump::start(
            7,
            plan.clone(),
            stream.clone(),
            writer.stream.clone(),
            event_tx,
        );

        assert!(matches!(
            decode(
                render_rx
                    .recv_timeout(Duration::from_secs(1))
                    .expect("start")
            ),
            ServerMessage::RawPtyStreamStart { start_seq: 6, .. }
        ));
        stream.process_read(b"live", None, || ());
        assert!(!pump.acknowledge(plan.stream_id, plan.start_seq + 1));
        assert!(render_rx.recv_timeout(Duration::from_millis(50)).is_err());
        assert!(pump.acknowledge(plan.stream_id, plan.start_seq));
        match decode(
            render_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("chunk"),
        ) {
            ServerMessage::RawPtyStreamChunk {
                start_seq, data, ..
            } => {
                assert_eq!(start_seq, plan.start_seq);
                assert_eq!(data, b"live");
            }
            other => panic!("expected raw chunk, got {other:?}"),
        }
    }

    #[test]
    fn unacknowledged_output_is_limited_to_one_mebibyte() {
        let stream = RawPtyStream::new();
        let plan = stream.attach(None, |_| Vec::new());
        let (control_tx, _control_rx) = std::sync::mpsc::channel();
        let (render_tx, render_rx) = std::sync::mpsc::sync_channel(32);
        let writer = ClientWriter::test_channel(control_tx, render_tx);
        let (event_tx, _event_rx) = tokio::sync::mpsc::channel(2);
        let pump = RawPtyPump::start(
            4,
            plan.clone(),
            stream.clone(),
            writer.stream.clone(),
            event_tx,
        );
        let _ = render_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("start");
        assert!(pump.acknowledge(plan.stream_id, plan.start_seq));
        stream.process_read(
            &vec![b'x'; RAW_PTY_ACK_WINDOW_BYTES as usize + 1],
            None,
            || (),
        );

        let mut received = 0usize;
        while received < RAW_PTY_ACK_WINDOW_BYTES as usize {
            match decode(
                render_rx
                    .recv_timeout(Duration::from_secs(1))
                    .expect("window chunk"),
            ) {
                ServerMessage::RawPtyStreamChunk { data, .. } => received += data.len(),
                other => panic!("expected raw chunk, got {other:?}"),
            }
        }
        assert_eq!(received, RAW_PTY_ACK_WINDOW_BYTES as usize);
        assert!(render_rx.recv_timeout(Duration::from_millis(50)).is_err());
    }

    #[test]
    fn cancellation_stops_delivery_and_rejects_late_ack() {
        let stream = RawPtyStream::new();
        let plan = stream.attach(None, |_| Vec::new());
        let (control_tx, _control_rx) = std::sync::mpsc::channel();
        let (render_tx, render_rx) = std::sync::mpsc::sync_channel(2);
        let writer = ClientWriter::test_channel(control_tx, render_tx);
        let (event_tx, _event_rx) = tokio::sync::mpsc::channel(2);
        let pump = RawPtyPump::start(
            4,
            plan.clone(),
            stream.clone(),
            writer.stream.clone(),
            event_tx,
        );
        let _ = render_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("start");
        pump.cancel();
        stream.process_read(b"late", None, || ());
        assert!(!pump.acknowledge(plan.stream_id, plan.start_seq));
        assert!(render_rx.recv_timeout(Duration::from_millis(50)).is_err());
    }

    #[test]
    fn stale_ack_does_not_advance_window() {
        let stream = RawPtyStream::new();
        let plan = stream.attach(None, |_| Vec::new());
        let (control_tx, _control_rx) = std::sync::mpsc::channel();
        let (render_tx, render_rx) = std::sync::mpsc::sync_channel(2);
        let writer = ClientWriter::test_channel(control_tx, render_tx);
        let (event_tx, _event_rx) = tokio::sync::mpsc::channel(2);
        let pump = RawPtyPump::start(3, plan.clone(), stream, writer.stream.clone(), event_tx);
        let _ = render_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("start");
        assert!(!pump.acknowledge(plan.stream_id + 1, plan.start_seq));
        assert!(pump.acknowledge(plan.stream_id, plan.start_seq));
        assert!(!pump.acknowledge(plan.stream_id, plan.start_seq + 1));
    }
}
