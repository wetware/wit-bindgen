use futures::channel::oneshot;
use std::future::{Future, IntoFuture, poll_fn};
use std::pin::{Pin, pin};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering::Relaxed};
use std::task::{Context, Poll, Waker};
use wit_bindgen::{FutureReader, spawn_local};

include!(env!("BINDINGS"));

use crate::exports::test::rust_cancel_wake::i::{Guest, Status};

struct Component;

export!(Component);

static REGISTERED: AtomicU32 = AtomicU32::new(0);
static POLLS: AtomicU32 = AtomicU32::new(0);
static DROPPED: AtomicU32 = AtomicU32::new(0);
static DELIVERED: AtomicU32 = AtomicU32::new(0);
static ROOT_DROPPED: AtomicBool = AtomicBool::new(false);
static WAITING: AtomicBool = AtomicBool::new(false);
static WAKERS: Mutex<Vec<Waker>> = Mutex::new(Vec::new());
static SLEEPER_TX: Mutex<Option<oneshot::Sender<()>>> = Mutex::new(None);
static SLEEPER_COMPLETED: AtomicBool = AtomicBool::new(false);

struct Node {
    receiver: oneshot::Receiver<()>,
    peer: Option<oneshot::Sender<()>>,
    registered: bool,
}

impl Future for Node {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        POLLS.fetch_add(1, Relaxed);
        assert!(Pin::new(&mut self.receiver).poll(cx).is_pending());
        if !self.registered {
            WAKERS.lock().unwrap().push(cx.waker().clone());
            self.registered = true;
            REGISTERED.fetch_add(1, Relaxed);
        }
        Poll::Pending
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        // Whichever node is destroyed first wakes a different, still-live
        // FuturesUnordered node. Waking only our own node would be suppressed
        // by FuturesUnordered during destruction and would miss the bug.
        if self.peer.take().unwrap().send(()).is_ok() {
            DELIVERED.fetch_add(1, Relaxed);
        }
        wake_saved_wakers();
        DROPPED.fetch_add(1, Relaxed);
    }
}

struct Root;

impl Drop for Root {
    fn drop(&mut self) {
        ROOT_DROPPED.store(true, Relaxed);
    }
}

fn wake_saved_wakers() {
    for waker in WAKERS.lock().unwrap().iter() {
        waker.wake_by_ref();
        waker.clone().wake();
    }
}

impl Guest for Component {
    async fn cancellable(checkpoint: FutureReader<()>, gate: FutureReader<()>) {
        let _root = Root;
        let (tx1, rx1) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();
        for (receiver, peer) in [(rx1, tx2), (rx2, tx1)] {
            spawn_local(Node {
                receiver,
                peer: Some(peer),
                registered: false,
            });
        }

        // The runner releases this component future only after both spawned
        // nodes have registered their wakers. Its callback clears any wakeup
        // from spawning, then parks the task on the unreleased gate.
        checkpoint.await;
        assert_eq!(REGISTERED.load(Relaxed), 2);
        let mut gate = pin!(gate.into_future());
        poll_fn(|cx| {
            POLLS.fetch_add(1, Relaxed);
            WAITING.store(true, Relaxed);
            gate.as_mut().poll(cx)
        })
        .await;
        unreachable!("the runner must cancel this task without opening the gate");
    }

    fn get_status() -> Status {
        Status {
            registered: REGISTERED.load(Relaxed),
            polls: POLLS.load(Relaxed),
            dropped: DROPPED.load(Relaxed),
            delivered: DELIVERED.load(Relaxed),
            root_dropped: ROOT_DROPPED.load(Relaxed),
            waiting: WAITING.load(Relaxed),
            sleeper_waiting: SLEEPER_TX.lock().unwrap().is_some(),
            sleeper_completed: SLEEPER_COMPLETED.load(Relaxed),
        }
    }

    fn wake_escaped() {
        for waker in std::mem::take(&mut *WAKERS.lock().unwrap()) {
            waker.wake_by_ref();
            waker.wake_by_ref();
            waker.clone().wake();
            waker.wake();
        }
    }

    async fn sleep() {
        let (tx, rx) = oneshot::channel();
        *SLEEPER_TX.lock().unwrap() = Some(tx);
        rx.await.unwrap();
        SLEEPER_COMPLETED.store(true, Relaxed);
    }

    fn wake_sleeper() {
        SLEEPER_TX.lock().unwrap().take().unwrap().send(()).unwrap();
    }
}
