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
//! Pending data is stored as a `VecDeque<Bytes>`. [`BodyForkSender::try_push`] optionally maps
//! and enqueues an owned `Bytes` handle; [`BodyForkReceiver::recv`] drains everything currently
//! queued.
//!
//! Two ways to close the sender:
//!
//! - **Clean EOF** — call [`BodyForkSender::finish`] (consumes self).
//!   Pending chunks are preserved for the receiver to drain.
//! - **Abort** — drop the sender without calling `finish`.
//!   Pending chunks are cleared immediately; [`BodyForkReceiver::recv_event`] reports the abort.

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
    /// The configured mapper rejected the chunk.
    Rejected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BodyForkPhase {
    Open,
    Finished,
    Aborted,
}

/// Data or completion returned by [`BodyForkReceiver::recv_event`].
#[derive(Debug, PartialEq, Eq)]
pub enum BodyForkEvent {
    /// All chunks that were queued when the receiver was polled.
    Chunks(VecDeque<Bytes>),
    /// The sender reached clean end-of-body and no queued chunks remain.
    Finished,
    /// The sender was dropped before clean end-of-body.
    Aborted,
}

struct BodyForkState {
    pending: VecDeque<Bytes>,
    phase: BodyForkPhase,
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
    mapper: Option<Box<dyn Fn(Bytes) -> Option<Bytes> + Send + Sync>>,
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
    body_fork_pair_inner(max_chunks, None)
}

/// Create a bounded body fork pair with an owned-chunk mapper.
///
/// The mapper runs before queue admission. Returning [`None`] rejects the chunk with
/// [`BodyForkPushError::Rejected`]. A successfully mapped chunk that cannot be queued is dropped
/// before [`BodyForkPushError::Full`] is returned.
pub fn body_fork_pair_with<F>(max_chunks: usize, mapper: F) -> (BodyForkSender, BodyForkReceiver)
where
    F: Fn(Bytes) -> Option<Bytes> + Send + Sync + 'static,
{
    body_fork_pair_inner(max_chunks, Some(Box::new(mapper)))
}

fn body_fork_pair_inner(
    max_chunks: usize,
    mapper: Option<Box<dyn Fn(Bytes) -> Option<Bytes> + Send + Sync>>,
) -> (BodyForkSender, BodyForkReceiver) {
    let inner = Arc::new(BodyForkShared {
        max_chunks,
        state: Mutex::new(BodyForkState {
            pending: VecDeque::new(),
            phase: BodyForkPhase::Open,
        }),
        notify: Notify::new(),
    });
    (
        BodyForkSender {
            inner: inner.clone(),
            mapper,
        },
        BodyForkReceiver { inner },
    )
}

impl BodyForkSender {
    /// Try to map and queue a body chunk.
    pub fn try_push(&self, chunk: Bytes) -> Result<(), BodyForkPushError> {
        let chunk = match self.mapper.as_ref() {
            Some(mapper) => mapper(chunk).ok_or(BodyForkPushError::Rejected)?,
            None => chunk,
        };
        if chunk.is_empty() {
            return Ok(());
        }

        let mut g = self.inner.state.lock();
        if g.pending.len() >= self.inner.max_chunks {
            return Err(BodyForkPushError::Full);
        }
        g.pending.push_back(chunk);
        drop(g);
        self.inner.notify.notify_waiters();
        Ok(())
    }

    /// Mark the body as complete. Pending chunks are preserved for the receiver.
    /// Consumes the sender so no further pushes are possible.
    pub fn finish(self) {
        let mut g = self.inner.state.lock();
        g.phase = BodyForkPhase::Finished;
        drop(g);
        self.inner.notify.notify_waiters();
    }
}

impl Drop for BodyForkSender {
    fn drop(&mut self) {
        let mut g = self.inner.state.lock();
        if g.phase == BodyForkPhase::Open {
            // Abort: the stream is incomplete, discard partial data.
            g.pending.clear();
            g.phase = BodyForkPhase::Aborted;
        }
        drop(g);
        self.inner.notify.notify_waiters();
    }
}

enum RecvPoll {
    Ready(BodyForkEvent),
    Wait,
}

fn poll_recv_available(shared: &BodyForkShared) -> RecvPoll {
    let mut g = shared.state.lock();

    if !g.pending.is_empty() {
        let chunks = std::mem::take(&mut g.pending);
        return RecvPoll::Ready(BodyForkEvent::Chunks(chunks));
    }

    match g.phase {
        BodyForkPhase::Open => RecvPoll::Wait,
        BodyForkPhase::Finished => RecvPoll::Ready(BodyForkEvent::Finished),
        BodyForkPhase::Aborted => RecvPoll::Ready(BodyForkEvent::Aborted),
    }
}

impl BodyForkReceiver {
    /// Receive queued body chunks or the explicit sender completion state.
    pub async fn recv_event(&mut self) -> BodyForkEvent {
        loop {
            let notified = self.inner.notify.notified();
            match poll_recv_available(&self.inner) {
                RecvPoll::Ready(event) => return event,
                RecvPoll::Wait => notified.await,
            }
        }
    }

    /// Receive all queued body chunks.
    ///
    /// Returns [`None`] for both clean finish and abort for backward compatibility. Call
    /// [`Self::recv_event`] when the distinction matters.
    pub async fn recv(&mut self) -> Option<VecDeque<Bytes>> {
        match self.recv_event().await {
            BodyForkEvent::Chunks(chunks) => Some(chunks),
            BodyForkEvent::Finished | BodyForkEvent::Aborted => None,
        }
    }

    /// Wait until the sender aborts.
    ///
    /// Clean finish does not resolve this future. This is intended to be selected against work
    /// that must be interrupted if the fork aborts.
    pub async fn wait_for_abort(&self) {
        loop {
            let notified = self.inner.notify.notified();
            if self.inner.state.lock().phase == BodyForkPhase::Aborted {
                return;
            }
            notified.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

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
        assert_eq!(rx.recv_event().await, BodyForkEvent::Aborted);
    }

    #[tokio::test]
    async fn finish_empty_body() {
        let (tx, mut rx) = body_fork_pair(32);
        tx.finish();
        assert_eq!(rx.recv_event().await, BodyForkEvent::Finished);
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

    struct TrackedBytes {
        bytes: Bytes,
        live_bytes: Arc<AtomicUsize>,
    }

    impl AsRef<[u8]> for TrackedBytes {
        fn as_ref(&self) -> &[u8] {
            self.bytes.as_ref()
        }
    }

    impl Drop for TrackedBytes {
        fn drop(&mut self) {
            self.live_bytes
                .fetch_sub(self.bytes.len(), Ordering::SeqCst);
        }
    }

    fn tracked_mapper(
        live_bytes: Arc<AtomicUsize>,
    ) -> impl Fn(Bytes) -> Option<Bytes> + Send + Sync {
        move |bytes| {
            live_bytes.fetch_add(bytes.len(), Ordering::SeqCst);
            Some(Bytes::from_owner(TrackedBytes {
                bytes,
                live_bytes: live_bytes.clone(),
            }))
        }
    }

    #[tokio::test]
    async fn mapper_rejection_aborts_when_sender_is_dropped() {
        let (tx, mut rx) = body_fork_pair_with(32, |_| None);
        assert_eq!(
            tx.try_push(Bytes::from_static(b"rejected")),
            Err(BodyForkPushError::Rejected)
        );
        drop(tx);
        assert_eq!(rx.recv_event().await, BodyForkEvent::Aborted);
    }

    #[tokio::test]
    async fn full_drops_mapped_chunk_immediately() {
        let live_bytes = Arc::new(AtomicUsize::new(0));
        let (tx, mut rx) = body_fork_pair_with(1, tracked_mapper(live_bytes.clone()));

        tx.try_push(Bytes::from_static(b"a")).unwrap();
        assert_eq!(live_bytes.load(Ordering::SeqCst), 1);
        assert_eq!(
            tx.try_push(Bytes::from_static(b"bc")),
            Err(BodyForkPushError::Full)
        );
        assert_eq!(
            live_bytes.load(Ordering::SeqCst),
            1,
            "the mapped chunk rejected by queue admission must be dropped"
        );

        tx.finish();
        let BodyForkEvent::Chunks(chunks) = rx.recv_event().await else {
            panic!("expected queued chunk");
        };
        drop(chunks);
        assert_eq!(live_bytes.load(Ordering::SeqCst), 0);
        assert_eq!(rx.recv_event().await, BodyForkEvent::Finished);
    }

    #[tokio::test]
    async fn mapped_bytes_live_across_drained_batch_and_refilled_queue() {
        const CHUNKS_PER_BATCH: usize = 32;

        let live_bytes = Arc::new(AtomicUsize::new(0));
        let (tx, mut rx) =
            body_fork_pair_with(CHUNKS_PER_BATCH, tracked_mapper(live_bytes.clone()));

        for _ in 0..CHUNKS_PER_BATCH {
            tx.try_push(Bytes::from_static(b"x")).unwrap();
        }
        let BodyForkEvent::Chunks(first_batch) = rx.recv_event().await else {
            panic!("expected first batch");
        };
        for _ in 0..CHUNKS_PER_BATCH {
            tx.try_push(Bytes::from_static(b"x")).unwrap();
        }
        assert_eq!(
            live_bytes.load(Ordering::SeqCst),
            2 * CHUNKS_PER_BATCH,
            "the drained batch and refilled queue retain their owners"
        );

        drop(first_batch);
        assert_eq!(live_bytes.load(Ordering::SeqCst), CHUNKS_PER_BATCH);
        drop(tx);
        assert_eq!(
            live_bytes.load(Ordering::SeqCst),
            0,
            "aborting clears the refilled queue"
        );
        assert_eq!(rx.recv_event().await, BodyForkEvent::Aborted);
    }

    #[tokio::test]
    async fn abort_wait_is_persistent() {
        let (tx, mut rx) = body_fork_pair(1);
        tx.try_push(Bytes::from_static(b"x")).unwrap();
        let BodyForkEvent::Chunks(batch) = rx.recv_event().await else {
            panic!("expected queued chunk");
        };

        drop(tx);
        tokio::time::timeout(std::time::Duration::from_secs(1), rx.wait_for_abort())
            .await
            .expect("abort recorded before waiter registration must still be observed");
        drop(batch);
        assert_eq!(rx.recv_event().await, BodyForkEvent::Aborted);
    }
}
