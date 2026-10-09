//@ wasmtime-flags = '-Wcomponent-model-async'

include!(env!("BINDINGS"));

use crate::test::rust_cancel_wake::i::*;
use futures::task::noop_waker_ref;
use std::future::Future;
use std::pin::Pin;
use std::task::Context;
use wit_bindgen::yield_async;

struct Component;

export!(Component);

impl Guest for Component {
    async fn run() {
        // Keep an independent component task sleeping throughout cancellation.
        let sleeper = start(sleep());
        while !get_status().sleeper_waiting {
            yield_async().await;
        }

        let (checkpoint, checkpoint_rx) = wit_future::new(|| unreachable!());
        let (gate, gate_rx) = wit_future::new(|| unreachable!());
        let task = start(cancellable(checkpoint_rx, gate_rx));
        while get_status().registered != 2 {
            yield_async().await;
        }
        checkpoint.write(()).await.unwrap();
        while !get_status().waiting {
            yield_async().await;
        }

        // The checkpoint callback has now returned with the root blocked on
        // the gate, with no pending Rust wakes. Dropping the import delivers
        // actual component cancellation to that sleeping task.
        let polls = get_status().polls;
        assert_eq!(polls, 3);
        drop(task);

        let status = get_status();
        assert_eq!(status.dropped, 2);
        assert_eq!(status.delivered, 1);
        assert!(status.root_dropped);
        assert_eq!(status.polls, polls);
        gate.write(()).await.unwrap_err();

        // Wakers retained beyond the cancelled task's lifetime remain inert,
        // including repeated wake-by-reference and consuming wake calls.
        wake_escaped();
        assert_eq!(get_status().polls, polls);
        assert!(!get_status().sleeper_completed);

        // A normal Rust oneshot wake from another component task still works.
        wake_sleeper();
        sleeper.await;
        assert!(get_status().sleeper_completed);
        assert_eq!(get_status().polls, polls);
    }
}

fn start(future: impl Future<Output = ()> + 'static) -> Pin<Box<dyn Future<Output = ()>>> {
    let mut future = Box::pin(future);
    assert!(
        future
            .as_mut()
            .poll(&mut Context::from_waker(noop_waker_ref()))
            .is_pending()
    );
    future
}
