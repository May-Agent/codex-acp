//! Executor-independent cooperative polling of the shared `AppServer` I/O driver.
//!
//! [`codex_sdk::AppServer`] separates its cloneable client handle from the
//! companion driver future so no async runtime is assumed. [`DriverSlot`]
//! lets any in-flight `SessionHost` or `Session` future advance the driver
//! inline without `tokio::spawn`, while cooperating with the idle background
//! pump run by [`crate::serve`] so out-of-prompt server requests are serviced
//! between turns without holding a lock across `.await` or losing wakers.

use std::fmt;
use std::future::{Future, poll_fn};
use std::pin::{Pin, pin};
use std::sync::Mutex;
use std::task::{Context, Poll};

use async_channel::{Receiver, Sender};

type BoxDriverFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

/// Synchronized slot holding the active `AppServer` I/O driver future.
pub struct DriverSlot {
    driver: Mutex<Option<BoxDriverFuture>>,
    bg_wake_tx: Sender<()>,
    bg_wake_rx: Receiver<()>,
}

impl fmt::Debug for DriverSlot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DriverSlot").finish_non_exhaustive()
    }
}

struct WakeGuard<'a> {
    tx: &'a Sender<()>,
    polled: bool,
}

impl Drop for WakeGuard<'_> {
    fn drop(&mut self) {
        if self.polled {
            let _ = self.tx.try_send(());
        }
    }
}

impl DriverSlot {
    /// Wrap a boxed `AppServer` driver future in a new [`DriverSlot`].
    pub(crate) fn new(driver: BoxDriverFuture) -> Self {
        let (bg_wake_tx, bg_wake_rx) = async_channel::bounded(1);
        Self {
            driver: Mutex::new(Some(driver)),
            bg_wake_tx,
            bg_wake_rx,
        }
    }

    fn poll_driver_once(&self, cx: &mut Context<'_>) -> Option<Poll<()>> {
        let mut guard = self.driver.try_lock().ok()?;
        let Some(driver) = guard.as_mut() else {
            return Some(Poll::Ready(()));
        };
        let poll = driver.as_mut().poll(cx);
        if poll.is_ready() {
            *guard = None;
        }
        drop(guard);
        Some(poll)
    }

    /// Poll `op` to completion while advancing the `AppServer` driver inline
    /// whenever no other poller is currently inside `driver.poll`.
    ///
    /// When `drive_until` finishes (or is dropped), it signals the background
    /// pump so the background task re-registers its own waker on the underlying
    /// stream reader.
    pub(crate) async fn drive_until<F, T>(&self, op: F) -> T
    where
        F: Future<Output = T>,
    {
        let mut op = pin!(op);
        let mut wake_guard = WakeGuard {
            tx: &self.bg_wake_tx,
            polled: false,
        };

        poll_fn(|cx| {
            if let Poll::Ready(output) = op.as_mut().poll(cx) {
                return Poll::Ready(output);
            }

            match self.poll_driver_once(cx) {
                Some(Poll::Ready(())) => {
                    wake_guard.polled = true;
                    op.as_mut().poll(cx)
                }
                Some(Poll::Pending) => {
                    wake_guard.polled = true;
                    if let Poll::Ready(output) = op.as_mut().poll(cx) {
                        return Poll::Ready(output);
                    }
                    Poll::Pending
                }
                None => Poll::Pending,
            }
        })
        .await
    }

    /// Drive the underlying `AppServer` future until it terminates cleanly.
    pub(crate) async fn drive_to_completion(&self) {
        let mut wake_stream = Box::pin(self.bg_wake_rx.clone());
        poll_fn(|cx| {
            // Drain any handoff notifications from finished `drive_until` calls
            // so `wake_stream` registers `cx` for future handoffs.
            while futures_lite::Stream::poll_next(wake_stream.as_mut(), cx) == Poll::Ready(Some(()))
            {
            }

            match self.poll_driver_once(cx) {
                Some(Poll::Ready(())) => Poll::Ready(()),
                Some(Poll::Pending) | None => Poll::Pending,
            }
        })
        .await;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures_lite::future::{or, zip};

    use super::*;

    #[tokio::test]
    async fn cancelling_drive_to_completion_preserves_driver_for_shutdown() {
        let (req_tx, req_rx) = async_channel::unbounded::<Sender<u32>>();
        let driver = Box::pin(async move {
            if let Ok(reply) = req_rx.recv().await {
                let _ = reply.send(99).await;
            }
        });
        let slot = Arc::new(DriverSlot::new(driver));

        or(slot.drive_to_completion(), async {
            futures_lite::future::yield_now().await;
        })
        .await;

        let got = slot
            .drive_until(async move {
                let (reply_tx, reply_rx) = async_channel::bounded(1);
                req_tx.send(reply_tx).await.unwrap();
                reply_rx.recv().await.unwrap()
            })
            .await;
        assert_eq!(got, 99);
        slot.drive_to_completion().await;
    }

    #[tokio::test]
    async fn concurrent_callers_and_background_pump_never_lose_wakers() {
        let (req_tx, req_rx) = async_channel::unbounded::<Sender<u32>>();
        let driver = Box::pin(async move {
            let mut count = 0_u32;
            while let Ok(reply) = req_rx.recv().await {
                count += 1;
                let _ = reply.send(count).await;
                if count == 3 {
                    break;
                }
            }
        });

        let slot = Arc::new(DriverSlot::new(driver));
        let bg_slot = Arc::clone(&slot);
        let bg_task = tokio::spawn(async move { bg_slot.drive_to_completion().await });

        let tx1 = req_tx.clone();
        let tx2 = req_tx.clone();
        let slot1 = Arc::clone(&slot);
        let slot2 = Arc::clone(&slot);

        let (r1, r2) = zip(
            slot1.drive_until(async move {
                let (reply_tx, reply_rx) = async_channel::bounded(1);
                tx1.send(reply_tx).await.unwrap();
                reply_rx.recv().await.unwrap()
            }),
            slot2.drive_until(async move {
                let (reply_tx, reply_rx) = async_channel::bounded(1);
                tx2.send(reply_tx).await.unwrap();
                reply_rx.recv().await.unwrap()
            }),
        )
        .await;
        assert_eq!(r1 + r2, 3);

        // Now verify the background pump still wakes after both `drive_until` calls exited!
        let (reply_tx, reply_rx) = async_channel::bounded(1);
        req_tx.send(reply_tx).await.unwrap();
        assert_eq!(reply_rx.recv().await.unwrap(), 3);
        bg_task.await.unwrap();
    }
}
