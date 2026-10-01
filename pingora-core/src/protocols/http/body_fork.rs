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
//! Pending data is stored as a `VecDeque<Bytes>`. [`BodyForkSender::try_push`] maps and enqueues
//! an owned `Bytes` handle; [`BodyForkReceiver::recv`] drains everything currently
//! queued.
//!
//! Two ways to close the sender:
//!
//! - **Clean EOF** — call [`BodyForkSender::finish`] (consumes self).
//!   Pending chunks are preserved for the receiver to drain.
//! - **Abort** — drop the sender without calling `finish`.
//!   Pending chunks are cleared immediately; [`BodyForkReceiver::recv`] reports the abort.

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BodyMultiForkPushError {
    Rejected,

    AllErrored(Vec<BodyForkPushError>),
}

/// Error from [`BodyForkReceiver::recv`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BodyForkAborted;

enum BodyForkState {
    Open(VecDeque<Bytes>),
    Finished(VecDeque<Bytes>),
    Aborted,
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

pub struct BodyMultiForkSender {
    mapper: Box<dyn Fn(Bytes) -> Option<Bytes> + Send + Sync>,
    senders: Mutex<Vec<BodyForkSender>>,
}

/// Create a bounded body fork pair with an owned-chunk mapper.
///
/// The mapper runs before queue admission. Returning [`None`] rejects the chunk with
/// [`BodyForkPushError::Rejected`]. A successfully mapped chunk that cannot be queued is dropped
/// before [`BodyForkPushError::Full`] is returned.
pub fn body_fork_pair_with(max_chunks: usize) -> (BodyForkSender, BodyForkReceiver) {
    let inner = Arc::new(BodyForkShared {
        max_chunks,
        state: Mutex::new(BodyForkState::Open(VecDeque::new())),
        notify: Notify::new(),
    });
    (
        BodyForkSender {
            inner: inner.clone(),
        },
        BodyForkReceiver { inner },
    )
}

pub fn body_multi_fork_pair_with<F>(
    max_chunks: usize,
    forks: usize,
    mapper: F,
) -> (BodyMultiForkSender, Vec<BodyForkReceiver>)
where
    F: Fn(Bytes) -> Option<Bytes> + Send + Sync + 'static,
{
    assert!(forks > 0, "at least one fork is required");
    let (senders, receivers) = (0..forks).map(|_| body_fork_pair_with(max_chunks)).unzip();
    (
        BodyMultiForkSender {
            mapper: Box::new(mapper),
            senders: Mutex::new(senders),
        },
        receivers,
    )
}

impl BodyMultiForkSender {
    /// Tries to push the same chunk of bytes to all forks, calling the mapping function once before pushing.
    pub fn try_push(&self, chunk: Bytes) -> Result<Vec<BodyForkPushError>, BodyMultiForkPushError> {
        let mut senders = self.senders.lock();
        let chunk = (self.mapper)(chunk).ok_or(BodyMultiForkPushError::Rejected)?;
        if chunk.is_empty() {
            return Ok(Vec::new());
        }

        let mut errs = Vec::new();
        let remaining_senders = senders
            .drain(..)
            .filter_map(|sender| {
                let res = sender.try_push(chunk.clone());
                match res {
                    Ok(_) => Some(sender),
                    Err(e) => {
                        errs.push(e);
                        None
                    }
                }
            })
            .collect::<Vec<_>>();
        *senders = remaining_senders;

        if senders.is_empty() {
            Err(BodyMultiForkPushError::AllErrored(errs))
        } else {
            Ok(errs)
        }
    }

    /// Finishes all forks.
    pub fn finish(self) {
        let mut senders = self.senders.lock();
        for sender in senders.drain(..) {
            sender.finish();
        }
    }
}

impl BodyForkSender {
    /// Try to map and queue a body chunk.
    pub fn try_push(&self, chunk: Bytes) -> Result<(), BodyForkPushError> {
        let mut g = self.inner.state.lock();
        let pending = match &mut *g {
            BodyForkState::Open(pending) => pending,
            _ => unreachable!("sender cannot be used after finish or abort"),
        };
        if pending.len() >= self.inner.max_chunks {
            return Err(BodyForkPushError::Full);
        }
        pending.push_back(chunk);
        drop(g);
        self.inner.notify.notify_waiters();
        Ok(())
    }

    /// Mark the body as complete. Pending chunks are preserved for the receiver.
    /// Consumes the sender so no further pushes are possible.
    pub fn finish(self) {
        let mut g = self.inner.state.lock();
        let pending = match &mut *g {
            BodyForkState::Open(pending) => pending,
            _ => unreachable!("sender cannot be finished after finish or abort"),
        };
        let pending = std::mem::take(pending);
        *g = BodyForkState::Finished(pending);
        drop(g);
        self.inner.notify.notify_waiters();
    }
}

impl Drop for BodyForkSender {
    fn drop(&mut self) {
        let mut g = self.inner.state.lock();
        if matches!(*g, BodyForkState::Open(_)) {
            // Abort: the stream is incomplete, discard partial data.
            *g = BodyForkState::Aborted;
        }
        drop(g);
        self.inner.notify.notify_waiters();
    }
}

enum RecvPoll {
    Ready(Result<Option<VecDeque<Bytes>>, BodyForkAborted>),
    Wait,
}

fn poll_recv_available(shared: &BodyForkShared) -> RecvPoll {
    let mut g = shared.state.lock();

    match &mut *g {
        BodyForkState::Open(pending) => {
            if pending.is_empty() {
                RecvPoll::Wait
            } else {
                RecvPoll::Ready(Ok(Some(std::mem::take(pending))))
            }
        }
        BodyForkState::Finished(pending) => {
            if pending.is_empty() {
                RecvPoll::Ready(Ok(None))
            } else {
                RecvPoll::Ready(Ok(Some(std::mem::take(pending))))
            }
        }
        BodyForkState::Aborted => RecvPoll::Ready(Err(BodyForkAborted)),
    }
}

impl BodyForkReceiver {
    /// Receive all queued body chunks.
    ///
    /// Returns [`Ok(None)`] after clean end-of-body and [`Err`] if the sender aborts.
    pub async fn recv(&mut self) -> Result<Option<VecDeque<Bytes>>, BodyForkAborted> {
        loop {
            let notified = self.inner.notify.notified();
            match poll_recv_available(&self.inner) {
                RecvPoll::Ready(result) => return result,
                RecvPoll::Wait => notified.await,
            }
        }
    }

    /// Wait until the sender aborts.
    ///
    /// Clean finish does not resolve this future. This is intended to be selected against work
    /// that must be interrupted if the fork aborts.
    pub async fn wait_for_abort(&self) {
        loop {
            let notified = self.inner.notify.notified();
            if matches!(*self.inner.state.lock(), BodyForkState::Aborted) {
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
        let (tx, mut rx) = body_fork_pair_with(32);
        tx.try_push(Bytes::from_static(b"a")).unwrap();
        tx.try_push(Bytes::from_static(b"bc")).unwrap();
        tx.finish();
        assert_eq!(flatten(rx.recv().await.unwrap().unwrap()), b"abc");
        assert!(rx.recv().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn max_chunks_rejects_push() {
        let (tx, mut rx) = body_fork_pair_with(2);
        tx.try_push(Bytes::from_static(b"ab")).unwrap();
        tx.try_push(Bytes::from_static(b"cd")).unwrap();
        assert_eq!(
            tx.try_push(Bytes::from_static(b"ef")),
            Err(BodyForkPushError::Full)
        );
        tx.finish();
        assert_eq!(flatten(rx.recv().await.unwrap().unwrap()), b"abcd");
        assert!(rx.recv().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn recv_frees_capacity() {
        let (tx, mut rx) = body_fork_pair_with(2);
        tx.try_push(Bytes::from_static(b"a")).unwrap();
        tx.try_push(Bytes::from_static(b"b")).unwrap();
        assert_eq!(
            tx.try_push(Bytes::from_static(b"c")),
            Err(BodyForkPushError::Full)
        );
        // Drain — frees both slots.
        let _ = rx.recv().await.unwrap().unwrap();
        // Now we can push again.
        tx.try_push(Bytes::from_static(b"c")).unwrap();
        tx.try_push(Bytes::from_static(b"d")).unwrap();
        tx.finish();
        assert_eq!(flatten(rx.recv().await.unwrap().unwrap()), b"cd");
        assert!(rx.recv().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn drop_without_finish_clears_bytes() {
        let (tx, mut rx) = body_fork_pair_with(32);
        tx.try_push(Bytes::from_static(b"x")).unwrap();
        drop(tx);
        assert_eq!(rx.recv().await, Err(BodyForkAborted));
    }

    #[tokio::test]
    async fn finish_empty_body() {
        let (tx, mut rx) = body_fork_pair_with(32);
        tx.finish();
        assert_eq!(rx.recv().await, Ok(None));
    }

    #[tokio::test]
    async fn recv_returns_available_without_waiting_for_end() {
        let (tx, mut rx) = body_fork_pair_with(32);
        tx.try_push(Bytes::from_static(b"a")).unwrap();
        tx.try_push(Bytes::from_static(b"b")).unwrap();
        let chunks = rx.recv().await.unwrap().unwrap();
        assert_eq!(chunks.len(), 2);
        assert_eq!(flatten(chunks), b"ab");
        tx.try_push(Bytes::from_static(b"z")).unwrap();
        tx.finish();
        assert_eq!(flatten(rx.recv().await.unwrap().unwrap()), b"z");
        assert!(rx.recv().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn multi_fork_partial_failure_keeps_healthy_fork() {
        let (tx, mut receivers) = body_multi_fork_pair_with(1, 2, Some);
        let mut healthy = receivers.remove(0);
        let mut stalled = receivers.remove(0);

        assert!(tx.try_push(Bytes::from_static(b"a")).unwrap().is_empty());
        assert_eq!(flatten(healthy.recv().await.unwrap().unwrap()), b"a");

        assert_eq!(
            tx.try_push(Bytes::from_static(b"b")),
            Ok(vec![BodyForkPushError::Full])
        );
        assert_eq!(stalled.recv().await, Err(BodyForkAborted));
        assert_eq!(flatten(healthy.recv().await.unwrap().unwrap()), b"b");

        tx.finish();
        assert_eq!(healthy.recv().await, Ok(None));
    }

    #[tokio::test]
    async fn multi_fork_errors_when_last_healthy_fork_fails() {
        let (tx, mut receivers) = body_multi_fork_pair_with(1, 2, Some);
        let mut healthy = receivers.remove(0);
        let mut stalled = receivers.remove(0);

        assert!(tx.try_push(Bytes::from_static(b"a")).unwrap().is_empty());
        assert_eq!(flatten(healthy.recv().await.unwrap().unwrap()), b"a");
        assert_eq!(
            tx.try_push(Bytes::from_static(b"b")),
            Ok(vec![BodyForkPushError::Full])
        );

        assert_eq!(
            tx.try_push(Bytes::from_static(b"c")),
            Err(BodyMultiForkPushError::AllErrored(vec![
                BodyForkPushError::Full
            ]))
        );
        assert_eq!(healthy.recv().await, Err(BodyForkAborted));
        assert_eq!(stalled.recv().await, Err(BodyForkAborted));
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
        let (tx, mut receivers) = body_multi_fork_pair_with(32, 1, |_| None);
        let mut rx = receivers.pop().unwrap();
        assert_eq!(
            tx.try_push(Bytes::from_static(b"rejected")),
            Err(BodyMultiForkPushError::Rejected)
        );
        drop(tx);
        assert_eq!(rx.recv().await, Err(BodyForkAborted));
    }

    #[tokio::test]
    async fn full_drops_failed_fork_bytes_immediately() {
        let live_bytes = Arc::new(AtomicUsize::new(0));
        let (tx, mut rx_vec) = body_multi_fork_pair_with(1, 1, tracked_mapper(live_bytes.clone()));
        let mut rx = rx_vec.pop().expect("expected one fork");

        tx.try_push(Bytes::from_static(b"a")).unwrap();
        assert_eq!(live_bytes.load(Ordering::SeqCst), 1);
        assert_eq!(
            tx.try_push(Bytes::from_static(b"bc")),
            Err(BodyMultiForkPushError::AllErrored(vec![
                BodyForkPushError::Full
            ]))
        );
        assert_eq!(
            live_bytes.load(Ordering::SeqCst),
            0,
            "aborting the failed fork must drop its queued and rejected chunks"
        );

        tx.finish();
        assert_eq!(rx.recv().await, Err(BodyForkAborted));
    }

    #[tokio::test]
    async fn mapped_bytes_live_across_drained_batch_and_refilled_queue() {
        const CHUNKS_PER_BATCH: usize = 32;

        let live_bytes = Arc::new(AtomicUsize::new(0));
        let (tx, mut rx_vec) =
            body_multi_fork_pair_with(CHUNKS_PER_BATCH, 1, tracked_mapper(live_bytes.clone()));
        let mut rx = rx_vec.pop().expect("expected one fork");

        for _ in 0..CHUNKS_PER_BATCH {
            tx.try_push(Bytes::from_static(b"x")).unwrap();
        }
        let Some(first_batch) = rx.recv().await.unwrap() else {
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
        assert_eq!(rx.recv().await, Err(BodyForkAborted));
    }

    #[tokio::test]
    async fn abort_wait_is_persistent() {
        let (tx, mut rx) = body_fork_pair_with(1);
        tx.try_push(Bytes::from_static(b"x")).unwrap();
        let Some(batch) = rx.recv().await.unwrap() else {
            panic!("expected queued chunk");
        };

        drop(tx);
        tokio::time::timeout(std::time::Duration::from_secs(1), rx.wait_for_abort())
            .await
            .expect("abort recorded before waiter registration must still be observed");
        drop(batch);
        assert_eq!(rx.recv().await, Err(BodyForkAborted));
    }
}
