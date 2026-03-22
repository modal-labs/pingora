// Copyright 2026 Cloudflare, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Bounded in-memory tee of request body bytes.
//!
//! Pending data is stored as a `VecDeque<Bytes>`. [`BodyForkSender::try_push`] enqueues a
//! `Bytes` handle (arc-increment, no copy); [`BodyForkReceiver::recv`] drains everything
//! currently queued.
//!
//! Two ways to close the sender:
//!
//! - **Clean EOF** — call [`BodyForkSender::finish`] (consumes self).
//!   Pending chunks are preserved for the receiver to drain.
//! - **Abort** — drop the sender without calling `finish`.
//!   Pending chunks are cleared immediately; the receiver gets `None`.

use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::Notify;

/// Error from [`BodyForkSender::try_push`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BodyForkPushError {
    /// The queue has reached `max_chunks` pending chunks.
    Full,
}

struct BodyForkState {
    pending: VecDeque<Bytes>,
    /// Set when the sender is done (either clean finish or abort).
    done: bool,
}

struct BodyForkShared {
    max_chunks: usize,
    state: Mutex<BodyForkState>,
    notify: Notify,
}

/// Send side: call [`try_push`](BodyForkSender::try_push) for each body chunk, then
/// [`finish`](BodyForkSender::finish) when the body is fully read.
/// Dropping without calling `finish` aborts the fork (clears buffered data).
pub struct BodyForkSender {
    inner: Arc<BodyForkShared>,
}

/// Receive side: [`recv`](BodyForkReceiver::recv) drains all queued chunks.
pub struct BodyForkReceiver {
    inner: Arc<BodyForkShared>,
}

/// Create a bounded body fork pair.
///
/// `max_chunks` limits the number of `Bytes` chunks that can be queued at once.
/// When the receiver drains chunks, capacity is freed for more pushes.
pub fn body_fork_pair(max_chunks: usize) -> (BodyForkSender, BodyForkReceiver) {
    let inner = Arc::new(BodyForkShared {
        max_chunks,
        state: Mutex::new(BodyForkState {
            pending: VecDeque::new(),
            done: false,
        }),
        notify: Notify::new(),
    });
    (
        BodyForkSender {
            inner: inner.clone(),
        },
        BodyForkReceiver { inner },
    )
}

impl BodyForkSender {
    /// Try to queue a body chunk. Fails with [`BodyForkPushError::Full`] if the number of
    /// pending (unconsumed) chunks has reached `max_chunks`.
    pub fn try_push(&self, chunk: Bytes) -> Result<(), BodyForkPushError> {
        let mut g = self.inner.state.lock();
        if g.pending.len() >= self.inner.max_chunks {
            return Err(BodyForkPushError::Full);
        }
        if !chunk.is_empty() {
            g.pending.push_back(chunk);
        }
        drop(g);
        self.inner.notify.notify_waiters();
        Ok(())
    }

    /// Mark the body as complete. Pending chunks are preserved for the receiver.
    /// Consumes the sender so no further pushes are possible.
    pub fn finish(self) {
        let mut g = self.inner.state.lock();
        g.done = true;
        drop(g);
        self.inner.notify.notify_waiters();
    }
}

impl Drop for BodyForkSender {
    fn drop(&mut self) {
        let mut g = self.inner.state.lock();
        if !g.done {
            // Abort: the stream is incomplete, discard partial data.
            g.pending.clear();
            g.done = true;
        }
        drop(g);
        self.inner.notify.notify_waiters();
    }
}

enum RecvPoll {
    Ready(Option<VecDeque<Bytes>>),
    Wait,
}

fn poll_recv_available(shared: &BodyForkShared) -> RecvPoll {
    let mut g = shared.state.lock();

    if !g.pending.is_empty() {
        let chunks = std::mem::take(&mut g.pending);
        return RecvPoll::Ready(Some(chunks));
    }

    if g.done {
        return RecvPoll::Ready(None);
    }

    RecvPoll::Wait
}

impl BodyForkReceiver {
    /// Receive all queued body chunks.
    ///
    /// Returns [`None`] when the stream has ended and there is no more data. Waits if the queue
    /// is empty but the sender may still produce data.
    pub async fn recv(&mut self) -> Option<VecDeque<Bytes>> {
        loop {
            let notified = self.inner.notify.notified();
            match poll_recv_available(&self.inner) {
                RecvPoll::Ready(v) => return v,
                RecvPoll::Wait => notified.await,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Flatten a VecDeque<Bytes> for easy assertion.
    fn flatten(chunks: VecDeque<Bytes>) -> Vec<u8> {
        chunks.into_iter().flat_map(|b| b.to_vec()).collect()
    }

    #[tokio::test]
    async fn push_finish_recv() {
        let (tx, mut rx) = body_fork_pair(32);
        tx.try_push(Bytes::from_static(b"a")).unwrap();
        tx.try_push(Bytes::from_static(b"bc")).unwrap();
        tx.finish();
        assert_eq!(flatten(rx.recv().await.unwrap()), b"abc");
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn max_chunks_rejects_push() {
        let (tx, mut rx) = body_fork_pair(2);
        tx.try_push(Bytes::from_static(b"ab")).unwrap();
        tx.try_push(Bytes::from_static(b"cd")).unwrap();
        assert_eq!(
            tx.try_push(Bytes::from_static(b"ef")),
            Err(BodyForkPushError::Full)
        );
        tx.finish();
        assert_eq!(flatten(rx.recv().await.unwrap()), b"abcd");
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn recv_frees_capacity() {
        let (tx, mut rx) = body_fork_pair(2);
        tx.try_push(Bytes::from_static(b"a")).unwrap();
        tx.try_push(Bytes::from_static(b"b")).unwrap();
        assert_eq!(
            tx.try_push(Bytes::from_static(b"c")),
            Err(BodyForkPushError::Full)
        );
        // Drain — frees both slots.
        let _ = rx.recv().await.unwrap();
        // Now we can push again.
        tx.try_push(Bytes::from_static(b"c")).unwrap();
        tx.try_push(Bytes::from_static(b"d")).unwrap();
        tx.finish();
        assert_eq!(flatten(rx.recv().await.unwrap()), b"cd");
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn drop_without_finish_clears_bytes() {
        let (tx, mut rx) = body_fork_pair(32);
        tx.try_push(Bytes::from_static(b"x")).unwrap();
        drop(tx);
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn finish_empty_body() {
        let (tx, mut rx) = body_fork_pair(32);
        tx.finish();
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn recv_returns_available_without_waiting_for_end() {
        let (tx, mut rx) = body_fork_pair(32);
        tx.try_push(Bytes::from_static(b"a")).unwrap();
        tx.try_push(Bytes::from_static(b"b")).unwrap();
        let chunks = rx.recv().await.unwrap();
        assert_eq!(chunks.len(), 2);
        assert_eq!(flatten(chunks), b"ab");
        tx.try_push(Bytes::from_static(b"z")).unwrap();
        tx.finish();
        assert_eq!(flatten(rx.recv().await.unwrap()), b"z");
        assert!(rx.recv().await.is_none());
    }
}
