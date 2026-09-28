//! The operations engine, driven against an injected source of
//! completions — no RDMA hardware needed: the engine's connection
//! seam ([`OpSource`]) is what the real `rdma::Connection` implements
//! (see `rdma/verbs.rs`), and here a scripted fake plays that half,
//! sharing its state with the test so completions arrive on demand.

use std::any::Any;
use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::os::fd::{AsRawFd, OwnedFd, RawFd};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::thread;
use std::time::Duration;

use crate::engine::{
    thread_waker, Completion, CompletionSource, InFlight, Op, OpSource, OpTarget, Submitted,
};
use crate::os;
use crate::*;

/// The fake's state, shared between the engine thread (submitting and
/// polling, through the source) and the test (scripting completions,
/// through its own clone of the arc).
#[derive(Default)]
struct FakeState {
    /// Every submit the source saw: `(wr_id, op, addr, len,
    /// remote_addr, rkey)`.
    submitted: Vec<(u64, Op, u64, usize, u64, u32)>,
    /// Fails the next submit with this message.
    fail_submit: Option<String>,
    /// Completions to deliver, in order, one batch per poll.
    script: VecDeque<Completion>,
    /// Auto-complete every submitted operation on the next poll.
    auto: bool,
    /// Submitted-and-not-yet-completed wr_ids.
    in_flight: Vec<u64>,
    /// close() ran.
    closed: bool,
    /// poll() call count.
    polls: usize,
    /// arm() call count — the hybrid-block counter.
    arms: usize,
    /// Take this on the next submitted operation: its in-flight hold
    /// stamps the byte into the operation's buffer at finish — which
    /// is how a test tells the engine finished the hold before the
    /// buffer resolved (the resolved buffer carries the stamp).
    stamp: Option<u8>,
    /// Every hold finish, as `(stamp, ok)`.
    finishes: Vec<(Option<u8>, bool)>,
}

impl FakeState {
    /// Script a completion for `wr_id`, completing it.
    fn complete(&mut self, wr_id: u64, ok: bool, error: &str) {
        self.in_flight.retain(|&w| w != wr_id);
        self.script.push_back(Completion {
            wr_id,
            ok,
            error: error.to_owned(),
        });
    }
}

/// The fake connection: the engine's connection seam — submitting
/// operations and closing — sharing its records with the fake
/// completions of its device.
struct FakeConn(Arc<Mutex<FakeState>>);

/// The fake shared completion source: the engine's completion seam
/// for a device — polling, arming, events — around the same shared
/// state as the device's connections, with an optional event pipe.
struct FakeCompletions {
    state: Arc<Mutex<FakeState>>,
    /// The fake's completion-event channel — a pipe standing in for
    /// the real comp channel: the engine blocks on the read end
    /// between busy windows, the test fires events by writing to it.
    /// Absent: a source with no event fd — the engine then never
    /// blocks while it has anything in flight.
    event_read: Option<OwnedFd>,
}

/// One fake pair — a connection and its device's shared completion
/// source — around one shared state.
fn fake() -> (FakeConn, FakeCompletions, Arc<Mutex<FakeState>>) {
    let state = Arc::new(Mutex::new(FakeState::default()));
    (
        FakeConn(Arc::clone(&state)),
        FakeCompletions {
            state: Arc::clone(&state),
            event_read: None,
        },
        state,
    )
}

/// A fake pair with a completion-event channel; the write end is the
/// test's, to fire events with.
fn fake_with_events() -> (FakeConn, FakeCompletions, Arc<Mutex<FakeState>>, OwnedFd) {
    let (read, write) = os::pipe_pair().unwrap();
    let state = Arc::new(Mutex::new(FakeState::default()));
    (
        FakeConn(Arc::clone(&state)),
        FakeCompletions {
            state: Arc::clone(&state),
            event_read: Some(read),
        },
        state,
        write,
    )
}

/// Register a fake connection with the engine, its shared completion
/// source with it — the reader side's first connection on a device
/// does exactly this; a later connection of the same device
/// registers with `None`.
fn register(engine: &Engine, conn: FakeConn, completions: FakeCompletions) -> u32 {
    engine
        .register(Box::new(conn), Some(Box::new(completions)))
        .unwrap()
}

impl OpSource for FakeConn {
    fn submit(
        &mut self,
        op: Op,
        addr: u64,
        len: usize,
        target: OpTarget,
        wr_id: u64,
    ) -> io::Result<Box<dyn InFlight>> {
        let mut state = self.0.lock().unwrap();
        state.submitted.push((
            wr_id,
            op,
            addr,
            len,
            target.remote_addr,
            target.rkey,
        ));
        state.in_flight.push(wr_id);
        if let Some(message) = state.fail_submit.take() {
            return Err(io::Error::other(message));
        }
        if state.auto {
            state.script.push_back(Completion {
                wr_id,
                ok: true,
                error: String::new(),
            });
        }
        let stamp = state.stamp.take();
        // The hold: it records its finishes, and stamps the buffer on
        // a stamped one's — the ordering the pool's copy-out relies
        // on.
        let flight = FakeFlight {
            state: Arc::clone(&self.0),
            addr,
            stamp,
        };
        Ok(Box::new(flight))
    }

    fn close(&mut self) {
        let mut state = self.0.lock().unwrap();
        state.closed = true;
        // Mirror the real close: the queue pair's outstanding
        // operations flush as error completions.
        let flushed: Vec<_> = state
            .in_flight
            .drain(..)
            .map(|wr_id| Completion {
                wr_id,
                ok: false,
                error: "flushed by close".to_owned(),
            })
            .collect();
        state.script.extend(flushed);
    }
}

/// The fake connection's in-flight hold: it records its finishes and,
/// when the state stamped it at submit, writes the stamp byte into the
/// operation's buffer at a successful finish — mirroring the real
/// pool's copy-out, which is what pins the engine's ordering: the
/// hold finishes before the buffer resolves.
struct FakeFlight {
    state: Arc<Mutex<FakeState>>,
    /// The operation's buffer, parked in the engine's slot — valid,
    /// owned, and untouched from submission to resolution.
    addr: u64,
    stamp: Option<u8>,
}

impl InFlight for FakeFlight {
    fn finish(&mut self, ok: bool) {
        self.state.lock().unwrap().finishes.push((self.stamp, ok));
        if let (true, Some(stamp)) = (ok, self.stamp) {
            // Safe like the pool's copy-out is: the buffer is the
            // slot's, until it resolves right after this.
            unsafe { *(self.addr as *mut u8) = stamp };
        }
    }
}

impl CompletionSource for FakeCompletions {
    fn poll(&mut self, out: &mut [Completion]) -> io::Result<usize> {
        let mut state = self.state.lock().unwrap();
        state.polls += 1;
        let mut count = 0;
        while count < out.len() {
            let Some(completion) = state.script.pop_front() else {
                break;
            };
            state.in_flight.retain(|&wr_id| wr_id != completion.wr_id);
            out[count] = completion;
            count += 1;
        }
        Ok(count)
    }

    fn event_fd(&self) -> Option<RawFd> {
        self.event_read.as_ref().map(|fd| fd.as_raw_fd())
    }

    fn arm(&mut self) -> io::Result<()> {
        self.state.lock().unwrap().arms += 1;
        Ok(())
    }

    fn consume_events(&mut self) -> io::Result<()> {
        // The readable event channel drains: every byte the test
        // wrote is one fired event.
        if let Some(read) = &self.event_read {
            os::drain(read.as_raw_fd());
        }
        Ok(())
    }
}

/// A `block_on` that parks the awaiting thread between wakes — so a
/// resolution genuinely exercises the engine's waker path (the wake
/// unparks, the poll completes) instead of spinning it away. The
/// waker itself is the engine's own ([`crate::engine::thread_waker`],
/// behind `OpHandle::wait`).
fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    let waker = thread_waker();
    let mut cx = Context::from_waker(&waker);
    loop {
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(output) => return output,
            Poll::Pending => thread::park(),
        }
    }
}

/// The error of a failed operation — its success type (the returned,
/// type-erased buffer) is not `Debug`, so `unwrap_err` will not do.
fn expect_err(result: io::Result<Box<dyn Any + Send>>) -> io::Error {
    match result {
        Ok(_) => panic!("the operation should have failed"),
        Err(error) => error,
    }
}

/// The buffer of a completed operation.
fn expect_ok(result: io::Result<Box<dyn Any + Send>>) -> Box<dyn Any + Send> {
    match result {
        Ok(buffer) => buffer,
        Err(error) => panic!("the operation should have succeeded: {error}"),
    }
}

/// Wait for the engine to catch up with the test's expectations: the
/// engine thread runs concurrently, so state it should have observed
/// (a submit it must have dispatched, a script it must have drained)
/// appears within a bounded spin.
fn wait_until(probe: impl Fn() -> bool) {
    for _ in 0..10_000 {
        if probe() {
            return;
        }
        thread::sleep(Duration::from_millis(1));
    }
    panic!("the engine never caught up with the test");
}

fn target() -> OpTarget {
    OpTarget {
        remote_addr: 0x1000,
        rkey: 7,
    }
}

fn submit(
    engine: &Engine,
    conn: u32,
    op: Op,
) -> crate::engine::OpHandle {
    engine
        .submit(conn, op, target(), Submitted::new(vec![0u8; 16]))
        .unwrap()
}

#[test]
fn engines_complete_a_submitted_read() {
    let engine = Engine::new();
    let (conn_source, completions, state) = fake();
    state.lock().unwrap().auto = true;
    let conn = register(&engine, conn_source, completions);

    let buffer = vec![0u8; 16];
    let addr = buffer.as_ptr() as u64;
    let handle = engine
        .submit(conn, Op::Read, target(), Submitted::new(buffer))
        .unwrap();

    let returned = block_on(handle)
        .unwrap()
        .downcast::<Vec<u8>>()
        .unwrap();
    assert_eq!(returned.len(), 16);

    // The source saw the read exactly as submitted: the operation,
    // the buffer's address, its size, and the target. The lock ends
    // before the shutdown: the engine thread takes it to close the
    // source, and the shutdown joins the engine thread.
    {
        let state = state.lock().unwrap();
        let &(_, op, seen_addr, len, remote_addr, rkey) = state.submitted.first().unwrap();
        assert_eq!(
            (op, seen_addr, len, remote_addr, rkey),
            (Op::Read, addr, 16, 0x1000, 7)
        );
    }
    engine.shutdown();
    assert!(state.lock().unwrap().closed);
}

#[test]
fn ops_complete_in_completion_order_not_submission_order() {
    // The engine's defining property: operations A and B wait at the
    // same time, polled by the same thread, and B completing first
    // resolves B while A stays pending — the poller drains queues,
    // it never waits on one operation.
    let engine = Engine::new();
    let (conn_source, completions, state) = fake();
    let conn = register(&engine, conn_source, completions);

    let mut a = std::pin::pin!(submit(&engine, conn, Op::Read));
    let b = submit(&engine, conn, Op::Write);
    wait_until(|| state.lock().unwrap().in_flight.len() == 2);
    let (a_id, b_id) = {
        let state = state.lock().unwrap();
        (state.submitted[0].0, state.submitted[1].0)
    };

    let waker = thread_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(a.as_mut().poll(&mut cx).is_pending());

    // B completes first; A does not.
    state.lock().unwrap().complete(b_id, true, "");
    let returned = block_on(b).unwrap().downcast::<Vec<u8>>().unwrap();
    assert_eq!(returned.len(), 16);
    assert!(a.as_mut().poll(&mut cx).is_pending());

    state.lock().unwrap().complete(a_id, true, "");
    let returned = loop {
        match a.as_mut().poll(&mut cx) {
            Poll::Ready(result) => break result.unwrap(),
            Poll::Pending => thread::park_timeout(Duration::from_millis(1)),
        }
    };
    assert_eq!(returned.downcast::<Vec<u8>>().unwrap().len(), 16);

    engine.shutdown();
}

#[test]
fn submit_failures_fail_the_operation() {
    let engine = Engine::new();
    let (conn_source, completions, state) = fake();
    state.lock().unwrap().fail_submit = Some("no dice".to_owned());
    let conn = register(&engine, conn_source, completions);

    let error = expect_err(block_on(submit(&engine, conn, Op::Read)));
    assert!(error.to_string().contains("no dice"));

    engine.shutdown();
}

#[test]
fn failed_completions_fail_the_operation() {
    let engine = Engine::new();
    let (conn_source, completions, state) = fake();
    let conn = register(&engine, conn_source, completions);

    let handle = submit(&engine, conn, Op::Read);
    wait_until(|| state.lock().unwrap().in_flight.len() == 1);
    let wr_id = state.lock().unwrap().in_flight[0];
    state
        .lock()
        .unwrap()
        .complete(wr_id, false, "RDMA operation failed: remote access error");

    let error = expect_err(block_on(handle));
    assert!(error.to_string().contains("remote access error"));

    engine.shutdown();
}

#[test]
fn holds_finish_before_the_buffer_resolves() {
    // The pool's contract, pinned: the in-flight hold finishes — its
    // bytes land in the operation's buffer — before the buffer
    // resolves to its handle. A stamped fake hold writes into the
    // buffer at finish; the resolved buffer carrying the stamp is
    // the proof of the order.
    let engine = Engine::new();
    let (conn_source, completions, state) = fake();
    state.lock().unwrap().stamp = Some(42);
    state.lock().unwrap().auto = true;
    let conn = register(&engine, conn_source, completions);

    let buffer = expect_ok(block_on(submit(&engine, conn, Op::Read)));
    assert_eq!(
        buffer.downcast::<Vec<u8>>().unwrap().as_slice(),
        &[42, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    );
    assert_eq!(
        state.lock().unwrap().finishes.as_slice(),
        &[(Some(42), true)]
    );

    engine.shutdown();
}

#[test]
fn failed_operations_finish_without_the_copy() {
    // The other half of the contract: a failed operation still
    // finishes its hold — recorded, and the copy-out skipped (nothing
    // would ever read the buffer).
    let engine = Engine::new();
    let (conn_source, completions, state) = fake();
    state.lock().unwrap().stamp = Some(42);
    let conn = register(&engine, conn_source, completions);

    let handle = submit(&engine, conn, Op::Read);
    wait_until(|| state.lock().unwrap().in_flight.len() == 1);
    let wr_id = state.lock().unwrap().in_flight[0];
    state.lock().unwrap().complete(wr_id, false, "no dice");

    let error = expect_err(block_on(handle));
    assert!(error.to_string().contains("no dice"));
    assert_eq!(
        state.lock().unwrap().finishes.as_slice(),
        &[(Some(42), false)]
    );

    engine.shutdown();
}

#[test]
fn dropped_handles_release_their_operations() {
    // Two operations in flight, both handles dropped: the engine must
    // still process the completions and free the slots — proven by
    // the shutdown joining (a leaked in-flight slot would hang the
    // drain).
    let engine = Engine::new();
    let (conn_source, completions, state) = fake();
    let conn = register(&engine, conn_source, completions);

    let a = submit(&engine, conn, Op::Read);
    let b = submit(&engine, conn, Op::Write);
    wait_until(|| state.lock().unwrap().in_flight.len() == 2);
    let ids: Vec<u64> = state
        .lock()
        .unwrap()
        .submitted
        .iter()
        .map(|&(wr_id, ..)| wr_id)
        .collect();
    drop((a, b));

    {
        let mut state = state.lock().unwrap();
        state.complete(ids[0], true, "");
        state.complete(ids[1], true, "");
    }
    wait_until(|| state.lock().unwrap().script.is_empty());

    engine.shutdown(); // joins: nothing was leaked as in-flight
}

#[test]
fn destroying_a_connection_flushes_pending_operations() {
    let engine = Engine::new();
    let (conn_source, completions, state) = fake();
    let conn = register(&engine, conn_source, completions);

    let a = submit(&engine, conn, Op::Read);
    let b = submit(&engine, conn, Op::Write);
    wait_until(|| state.lock().unwrap().in_flight.len() == 2);
    engine.destroy(conn).unwrap();

    for handle in [a, b] {
        let error = expect_err(block_on(handle));
        assert!(error.to_string().contains("flush"));
    }
    assert!(state.lock().unwrap().closed);

    // The engine takes a new connection afterwards, on the same id
    // sequence.
    let (conn_source, completions, state) = fake();
    state.lock().unwrap().auto = true;
    let conn = register(&engine, conn_source, completions);
    let returned = block_on(submit(&engine, conn, Op::Read))
        .unwrap()
        .downcast::<Vec<u8>>()
        .unwrap();
    assert_eq!(returned.len(), 16);

    engine.shutdown();
}

#[test]
fn shutdown_fails_pending_operations_and_is_idempotent() {
    let engine = Engine::new();
    let (conn_source, completions, state) = fake();
    let conn = register(&engine, conn_source, completions);

    let handle = submit(&engine, conn, Op::Read);
    wait_until(|| state.lock().unwrap().in_flight.len() == 1);

    // Shutdown closes the source — the flush resolves the pending
    // operation — and joins the engine thread.
    engine.shutdown();
    let error = expect_err(block_on(handle));
    assert!(error.to_string().contains("flush"));

    // Idempotent, and a submitter after shutdown fails instead of
    // queueing into a dead engine.
    engine.shutdown();
    assert!(engine.submit(conn, Op::Read, target(), Submitted::new(vec![0u8; 8])).is_err());
}

#[test]
fn submitting_to_an_unknown_connection_fails_at_submission() {
    let engine = Engine::new();
    let error = engine
        .submit(99, Op::Read, target(), Submitted::new(vec![0u8; 8]))
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
}

#[test]
fn zero_length_operations_fail_at_submission() {
    let engine = Engine::new();
    let (conn_source, completions, _) = fake();
    let conn = register(&engine, conn_source, completions);

    let error = engine
        .submit(conn, Op::Read, target(), Submitted::new(Vec::<u8>::new()))
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);

    engine.shutdown();
}

#[test]
fn every_connection_is_polled() {
    // Two pairs — the two-device case: two completion sources, one
    // operation each; the rotation polls both, so both resolve.
    let engine = Engine::new();
    let (conn_a, completions_a, state_a) = fake();
    let (conn_b, completions_b, state_b) = fake();
    state_a.lock().unwrap().auto = true;
    state_b.lock().unwrap().auto = true;
    let a = register(&engine, conn_a, completions_a);
    let b = register(&engine, conn_b, completions_b);

    let (returned_a, returned_b) = (
        block_on(submit(&engine, a, Op::Read)),
        block_on(submit(&engine, b, Op::Write)),
    );
    assert_eq!(
        (
            returned_a.unwrap().downcast::<Vec<u8>>().unwrap().len(),
            returned_b.unwrap().downcast::<Vec<u8>>().unwrap().len(),
        ),
        (16, 16)
    );

    wait_until(|| state_a.lock().unwrap().polls >= 1 && state_b.lock().unwrap().polls >= 1);
    engine.shutdown();
}

#[test]
fn connections_of_one_device_share_one_completion_source() {
    // The shared-completions case — the reader side's flow: the first
    // connection on a device registers with its shared completion
    // source, every later connection of the device registers with
    // `None`, and both drain through the one source.
    let engine = Engine::new();
    let (conn_a, completions, state) = fake();
    state.lock().unwrap().auto = true;
    let a = register(&engine, conn_a, completions);

    // A later connection of the same device: no completion source of
    // its own — the shared one covers it.
    let second = FakeConn(Arc::clone(&state));
    let b = engine.register(Box::new(second), None).unwrap();

    let (returned_a, returned_b) = (
        block_on(submit(&engine, a, Op::Read)),
        block_on(submit(&engine, b, Op::Write)),
    );
    assert_eq!(
        (
            returned_a.unwrap().downcast::<Vec<u8>>().unwrap().len(),
            returned_b.unwrap().downcast::<Vec<u8>>().unwrap().len(),
        ),
        (16, 16)
    );
    // Both connections' operations ran through the one pair's state:
    // two submissions recorded, both drained.
    assert_eq!(state.lock().unwrap().submitted.len(), 2);

    engine.shutdown();
}

#[test]
fn slot_ids_differ_across_reuse() {
    // Three rounds of submit-and-complete on one slot pool: the
    // generation packing makes every wr_id distinct.
    let engine = Engine::new();
    let (conn_source, completions, state) = fake();
    state.lock().unwrap().auto = true;
    let conn = register(&engine, conn_source, completions);

    let mut seen = Vec::new();
    for _ in 0..3 {
        block_on(submit(&engine, conn, Op::Read)).unwrap();
        seen.push(state.lock().unwrap().submitted.last().unwrap().0);
    }
    let unique: std::collections::HashSet<u64> = seen.iter().copied().collect();
    assert_eq!(unique.len(), seen.len());

    engine.shutdown();
}

#[test]
fn engines_block_on_events_when_in_flight_and_idle() {
    // The hybrid idle: an operation is in flight but nothing completes,
    // so after the busy window the engine arms the source's completion
    // events and blocks — nothing resolves until the test fires one,
    // and then it does.
    let engine = Engine::new();
    let (conn_source, completions, state, events) = fake_with_events();
    let conn = register(&engine, conn_source, completions);

    let handle = submit(&engine, conn, Op::Read);
    // The engine dispatched the operation, spun its busy window out,
    // and armed the completion events — that is what it blocks on.
    wait_until(|| state.lock().unwrap().arms >= 1);

    // Fire the event: script the completion and write the byte (what
    // the real channel does to its fd when the armed queue completes).
    let wr_id = state.lock().unwrap().submitted[0].0;
    state.lock().unwrap().complete(wr_id, true, "");
    os::wake(events.as_raw_fd());

    let returned = block_on(handle)
        .unwrap()
        .downcast::<Vec<u8>>()
        .unwrap();
    assert_eq!(returned.len(), 16);

    engine.shutdown();
}

#[test]
fn submissions_wake_a_hybrid_blocked_engine() {
    // The wake channel still works under the hybrid block: an
    // operation submitted while the engine rests on the events wakes
    // it — the command is dispatched without any event firing.
    let engine = Engine::new();
    let (conn_source, completions, state, events) = fake_with_events();
    let conn = register(&engine, conn_source, completions);

    let a = submit(&engine, conn, Op::Read);
    wait_until(|| state.lock().unwrap().arms >= 1);

    // Submitted while the engine is (about to be) blocked on events.
    let b = submit(&engine, conn, Op::Write);
    wait_until(|| state.lock().unwrap().submitted.len() == 2);

    let ids: Vec<u64> = state
        .lock()
        .unwrap()
        .submitted
        .iter()
        .map(|&(wr_id, ..)| wr_id)
        .collect();
    {
        let mut state = state.lock().unwrap();
        state.complete(ids[0], true, "");
        state.complete(ids[1], true, "");
    }
    os::wake(events.as_raw_fd());

    for handle in [a, b] {
        assert_eq!(
            block_on(handle).unwrap().downcast::<Vec<u8>>().unwrap().len(),
            16
        );
    }

    engine.shutdown();
}

#[test]
fn pure_busy_engines_never_block() {
    // `Duration::MAX`: the dedicated-core case — the engine never arms
    // and never blocks while anything is in flight; completions arrive
    // through the busy sweep.
    let engine = Engine::new().with_busy_window(Duration::MAX);
    let (conn_source, completions, state, _events) = fake_with_events();
    let conn = register(&engine, conn_source, completions);

    let handle = submit(&engine, conn, Op::Read);
    thread::sleep(Duration::from_millis(20));
    assert_eq!(state.lock().unwrap().arms, 0);

    let wr_id = state.lock().unwrap().submitted[0].0;
    state.lock().unwrap().complete(wr_id, true, "");
    let returned = block_on(handle)
        .unwrap()
        .downcast::<Vec<u8>>()
        .unwrap();
    assert_eq!(returned.len(), 16);

    engine.shutdown();
}

#[test]
fn engines_pin_to_available_cpus_and_reject_unavailable_ones() {
    let cpus = os::current_thread_cpus().unwrap();
    let Some(&cpu) = cpus.first() else {
        return;
    };

    let engine = Engine::on_cpu(cpu).unwrap();
    assert_eq!(engine.cpu(), Some(cpu));

    // A pinned engine works like an unpinned one.
    let (conn_source, completions, state) = fake();
    state.lock().unwrap().auto = true;
    let conn = register(&engine, conn_source, completions);
    let returned = block_on(submit(&engine, conn, Op::Read))
        .unwrap()
        .downcast::<Vec<u8>>()
        .unwrap();
    assert_eq!(returned.len(), 16);
    engine.shutdown();

    if let Some(blocked) = (0..1024u32).find(|cpu| !cpus.contains(cpu)) {
        assert!(Engine::on_cpu(blocked).is_err());
    }
}

#[test]
fn engine_pieces_cross_threads() {
    // The threading stance, pinned at compile time: an engine is
    // cloneable across threads, and an operation handle is `Send` —
    // awaitable anywhere, however it was submitted.
    fn assert_send<T: Send>() {}
    assert_send::<Engine>();
    assert_send::<crate::engine::OpHandle>();
}
