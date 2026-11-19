//! Buffers, replays, broadcasts, and atomically closes downstream output.

use bytes::Bytes;
use tokio::sync::mpsc;

#[derive(Default)]
pub struct OutputHub {
    state: parking_lot::Mutex<OutputState>,
}

#[derive(Default)]
struct OutputState {
    history: Vec<Bytes>,
    subscribers: Vec<mpsc::UnboundedSender<Bytes>>,
    closed: bool,
}

impl OutputHub {
    pub fn emit(&self, frame: Bytes) -> bool {
        let mut state = self.state.lock();
        if state.closed {
            return false;
        }
        state.history.push(frame.clone());
        state
            .subscribers
            .retain(|subscriber| subscriber.send(frame.clone()).is_ok());
        true
    }

    pub fn subscribe(&self) -> mpsc::UnboundedReceiver<Bytes> {
        let (sender, receiver) = mpsc::unbounded_channel();
        let mut state = self.state.lock();
        for frame in &state.history {
            let _ = sender.send(frame.clone());
        }
        if !state.closed {
            state.subscribers.push(sender);
        }
        receiver
    }

    /// The last end-stream frame this hub produced, if any.
    ///
    /// Used to replay a terminal frame to a client that reconnects after the
    /// owning run has already been torn down.
    pub fn terminal_frame(&self) -> Option<Bytes> {
        let state = self.state.lock();
        state
            .history
            .iter()
            .rev()
            .find(|frame| {
                frame.first().is_some_and(|flags| {
                    flags & crate::cursor::protocol::connect::END_STREAM_FLAG != 0
                })
            })
            .cloned()
    }

    pub fn close(&self) -> bool {
        let mut state = self.state.lock();
        if state.closed {
            return false;
        }
        state.closed = true;
        state.subscribers.clear();
        drop(state);
        true
    }
}
