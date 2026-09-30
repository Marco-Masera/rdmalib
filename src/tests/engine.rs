//! The operations engine, driven against an injected source of
//! completions — no RDMA hardware needed: the engine's connection
//! seam ([`OpSource`]) is what the real `rdma::Connection` implements
//! (see `rdma/verbs.rs`), and here a scripted fake plays that half,
//! sharing its state with the test so completions arrive on demand.

use std::any::Any;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::os::fd::{AsRawFd, OwnedFd, RawFd};
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::thread;
use std::time::Duration;

use crate::engine::{
    Completion, CompletionSource, InFlight, Op, OpSource, OpTarget, Submitted, thread_waker,
};
use crate::os;
use crate::*;

/// The fake's state, shared between the engine thread (submitting and
/// polling, through the source) and the test (scripting completions,
/// through its own clone of the arc).
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
    /// Registered buffers, as `(addr, len)` in registration order.
    registered_buffers: Vec<(u64, usize)>,
    /// Deregistered buffers, as `(addr, len)` in deregistration
    /// order — each after the engine dispatched everything queued
    /// before it.
    deregistered_buffers: Vec<(u64, usize)>,
    /// poll() call count.
    polls: usize,
    /// arm() call count — the hybrid-block counter.
    arms: usize,
    /// The fake connection's send-queue depth — the bound of
    /// submissions. Defaulted to 16 (the old nominal depth), set
    /// lower by the bound's tests.
    depth: usize,
    /// Take this on the next submitted operation: its in-flight hold
    /// stamps the byte into the operation's buffer at finish — which
    /// is how a test tells the engine finished the hold before the
    /// buffer resolved (the resolved buffer carries the stamp).
    stamp: Option<u8>,
    /// Every hold finish, as `(stamp, ok)`.
    finishes: Vec<(Option<u8>, bool)>,
}

impl Default for FakeState {
    fn default() -> Self {
        Self {
            submitted: Vec::new(),
            fail_submit: None,
            script: VecDeque::new(),
            auto: false,
            in_flight: Vec::new(),
            closed: false,
            registered_buffers: Vec::new(),
            deregistered_buffers: Vec::new(),
            polls: 0,
            arms: 0,
            // The old nominal send-queue depth: enough for every
            // test that does not exercise the bound.
            depth: 16,
            stamp: None,
            finishes: Vec::new(),
        }
    }
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
fn register(
    engine: &Engine,
    conn: FakeConn,
    completions: FakeCompletions,
) -> crate::engine::ConnRef {
    engine
        .register(Box::new(conn), Some(Box::new(completions)))
        .unwrap()
}

impl OpSource for FakeConn {
    fn depth(&self) -> usize {
        self.0.lock().unwrap().depth
    }

    fn submit(
        &mut self,
        op: Op,
        addr: u64,
        len: usize,
        target: OpTarget,
        wr_id: u64,
    ) -> io::Result<Box<dyn InFlight>> {
        let mut state = self.0.lock().unwrap();
        state
            .submitted
            .push((wr_id, op, addr, len, target.remote_addr, target.rkey));
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

    fn register_buffer(&mut self, addr: u64, len: usize) -> io::Result<()> {
        self.0.lock().unwrap().registered_buffers.push((addr, len));
        Ok(())
    }

    fn deregister_buffer(&mut self, addr: u64, len: usize, payload: Box<dyn Any + Send>) {
        // The payload drops with the call — the real half holds it
        // until the device is done, which the fake has no device for.
        drop(payload);
        self.0
            .lock()
            .unwrap()
            .deregistered_buffers
            .push((addr, len));
    }

    fn submit_registered(
        &mut self,
        op: Op,
        addr: u64,
        len: usize,
        target: OpTarget,
        wr_id: u64,
    ) -> io::Result<()> {
        // Recorded exactly like an owned submit — the source's
        // covering lookup is the real half's business.
        let mut state = self.0.lock().unwrap();
        state
            .submitted
            .push((wr_id, op, addr, len, target.remote_addr, target.rkey));
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
        Ok(())
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
fn expect_err(result: io::Result<crate::engine::Outcome>) -> io::Error {
    match result {
        Ok(_) => panic!("the operation should have failed"),
        Err(error) => error,
    }
}

/// The buffer of a completed operation.
fn expect_ok(result: io::Result<crate::engine::Outcome>) -> Box<dyn Any + Send> {
    match result {
        Ok(outcome) => buffer_of(outcome),
        Err(error) => panic!("the operation should have succeeded: {error}"),
    }
}

/// The buffer an owned operation resolves with, for the tests'
/// downcasts — a registered-buffer operation resolves with nothing
/// (`Outcome::Unit`), which no owned-operation test expects.
fn buffer_of(outcome: crate::engine::Outcome) -> Box<dyn Any + Send> {
    match outcome {
        crate::engine::Outcome::Buffer(buffer) => buffer,
        crate::engine::Outcome::Unit => {
            unreachable!("an owned operation resolves with its buffer")
        }
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

fn submit(engine: &Engine, conn: &crate::engine::ConnRef, op: Op) -> crate::engine::OpHandle {
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
        .submit(&conn, Op::Read, target(), Submitted::new(buffer))
        .unwrap();

    let returned = buffer_of(block_on(handle).unwrap())
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

    let mut a = std::pin::pin!(submit(&engine, &conn, Op::Read));
    let b = submit(&engine, &conn, Op::Write);
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
    let returned = buffer_of(block_on(b).unwrap())
        .downcast::<Vec<u8>>()
        .unwrap();
    assert_eq!(returned.len(), 16);
    assert!(a.as_mut().poll(&mut cx).is_pending());

    state.lock().unwrap().complete(a_id, true, "");
    let returned = loop {
        match a.as_mut().poll(&mut cx) {
            Poll::Ready(result) => break result.unwrap(),
            Poll::Pending => thread::park_timeout(Duration::from_millis(1)),
        }
    };
    assert_eq!(buffer_of(returned).downcast::<Vec<u8>>().unwrap().len(), 16);

    engine.shutdown();
}

#[test]
fn submit_failures_fail_the_operation() {
    let engine = Engine::new();
    let (conn_source, completions, state) = fake();
    state.lock().unwrap().fail_submit = Some("no dice".to_owned());
    let conn = register(&engine, conn_source, completions);

    let error = expect_err(block_on(submit(&engine, &conn, Op::Read)));
    assert!(error.to_string().contains("no dice"));

    engine.shutdown();
}

#[test]
fn failed_completions_fail_the_operation() {
    let engine = Engine::new();
    let (conn_source, completions, state) = fake();
    let conn = register(&engine, conn_source, completions);

    let handle = submit(&engine, &conn, Op::Read);
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

    let buffer = expect_ok(block_on(submit(&engine, &conn, Op::Read)));
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

    let handle = submit(&engine, &conn, Op::Read);
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

    let a = submit(&engine, &conn, Op::Read);
    let b = submit(&engine, &conn, Op::Write);
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

    let a = submit(&engine, &conn, Op::Read);
    let b = submit(&engine, &conn, Op::Write);
    wait_until(|| state.lock().unwrap().in_flight.len() == 2);
    engine.destroy(&conn).unwrap();

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
    let returned = buffer_of(block_on(submit(&engine, &conn, Op::Read)).unwrap())
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

    let handle = submit(&engine, &conn, Op::Read);
    wait_until(|| state.lock().unwrap().in_flight.len() == 1);

    // Shutdown closes the source — the flush resolves the pending
    // operation — and joins the engine thread.
    engine.shutdown();
    let error = expect_err(block_on(handle));
    assert!(error.to_string().contains("flush"));

    // Idempotent, and a submitter after shutdown fails instead of
    // queueing into a dead engine.
    engine.shutdown();
    assert!(
        engine
            .submit(&conn, Op::Read, target(), Submitted::new(vec![0u8; 8]))
            .is_err()
    );
}

#[test]
fn submitting_to_an_unknown_connection_fails_at_submission() {
    let engine = Engine::new();
    let error = engine
        .submit(
            &crate::engine::ConnRef::unknown(99),
            Op::Read,
            target(),
            Submitted::new(vec![0u8; 8]),
        )
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
}

#[test]
fn zero_length_operations_fail_at_submission() {
    let engine = Engine::new();
    let (conn_source, completions, _) = fake();
    let conn = register(&engine, conn_source, completions);

    let error = engine
        .submit(&conn, Op::Read, target(), Submitted::new(Vec::<u8>::new()))
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);

    engine.shutdown();
}

#[test]
fn submissions_past_a_connections_depth_fail_at_submission() {
    // The send-queue bound, eager: with the fake's depth at 2, the
    // third submission fails on the spot — the first two still run
    // and complete, the queue pair never sees an over-deep post.
    let engine = Engine::new();
    let (conn_source, completions, state) = fake();
    state.lock().unwrap().depth = 2;
    state.lock().unwrap().auto = true;
    let conn = register(&engine, conn_source, completions);

    let (a, b) = (
        submit(&engine, &conn, Op::Read),
        submit(&engine, &conn, Op::Read),
    );
    let error = engine
        .submit(&conn, Op::Write, target(), Submitted::new(vec![0u8; 16]))
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert!(error.to_string().contains("send queue is full"));
    assert!(error.to_string().contains("max_send_wr is 2"));

    assert_eq!(
        (
            expect_ok(block_on(a)).downcast::<Vec<u8>>().unwrap().len(),
            expect_ok(block_on(b)).downcast::<Vec<u8>>().unwrap().len(),
        ),
        (16, 16)
    );

    engine.shutdown();
}

#[test]
fn completions_free_the_send_queue_slots() {
    // The bound follows completions, not submissions: depth 1 — the
    // second submission fails while the first is in flight, passes
    // again once the first completed.
    let engine = Engine::new();
    let (conn_source, completions, state) = fake();
    state.lock().unwrap().depth = 1;
    let conn = register(&engine, conn_source, completions);

    let first = submit(&engine, &conn, Op::Read);
    let error = engine
        .submit(&conn, Op::Write, target(), Submitted::new(vec![0u8; 16]))
        .unwrap_err();
    assert!(error.to_string().contains("send queue is full"));

    // The wr_id is taken under one lock at a time — a nested
    // `state.lock()` inside the `complete` call's arguments would
    // deadlock on the non-reentrant mutex.
    wait_until(|| state.lock().unwrap().in_flight.len() == 1);
    let wr_id = state.lock().unwrap().in_flight[0];
    state.lock().unwrap().complete(wr_id, true, "");
    assert_eq!(
        expect_ok(block_on(first))
            .downcast::<Vec<u8>>()
            .unwrap()
            .len(),
        16
    );

    // The completion freed the slot: the next submission passes.
    let second = submit(&engine, &conn, Op::Read);
    wait_until(|| state.lock().unwrap().in_flight.len() == 1);
    let wr_id = state.lock().unwrap().in_flight[0];
    state.lock().unwrap().complete(wr_id, true, "");
    assert_eq!(
        expect_ok(block_on(second))
            .downcast::<Vec<u8>>()
            .unwrap()
            .len(),
        16
    );

    engine.shutdown();
}

#[test]
fn destroyed_connections_reject_submissions() {
    // The eager end of the destroyed-connection path: once a
    // destroyed connection is reaped (nothing of it left anywhere —
    // the source itself is gone, provable by the arc's refcount),
    // submissions to it fail on the spot instead of riding the
    // mailbox into the engine's "gone or closing" error.
    let engine = Engine::new();
    let (conn_source, completions, state) = fake();
    let conn = register(&engine, conn_source, completions);

    engine.destroy(&conn).unwrap();
    // The reaped connection's source is gone: only the test's own
    // handle and the engine's completion source still hold the
    // state's arc (destroy tears the connection down, not the
    // device's completion source).
    wait_until(|| Arc::strong_count(&state) == 2);

    let error = engine
        .submit(&conn, Op::Read, target(), Submitted::new(vec![0u8; 16]))
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains(&format!("connection {} is gone", conn.id))
    );
    assert!(error.to_string().contains("destroyed"));

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
        block_on(submit(&engine, &a, Op::Read)),
        block_on(submit(&engine, &b, Op::Write)),
    );
    assert_eq!(
        (
            buffer_of(returned_a.unwrap())
                .downcast::<Vec<u8>>()
                .unwrap()
                .len(),
            buffer_of(returned_b.unwrap())
                .downcast::<Vec<u8>>()
                .unwrap()
                .len(),
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
        block_on(submit(&engine, &a, Op::Read)),
        block_on(submit(&engine, &b, Op::Write)),
    );
    assert_eq!(
        (
            buffer_of(returned_a.unwrap())
                .downcast::<Vec<u8>>()
                .unwrap()
                .len(),
            buffer_of(returned_b.unwrap())
                .downcast::<Vec<u8>>()
                .unwrap()
                .len(),
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
        block_on(submit(&engine, &conn, Op::Read)).unwrap();
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

    let handle = submit(&engine, &conn, Op::Read);
    // The engine dispatched the operation, spun its busy window out,
    // and armed the completion events — that is what it blocks on.
    wait_until(|| state.lock().unwrap().arms >= 1);

    // Fire the event: script the completion and write the byte (what
    // the real channel does to its fd when the armed queue completes).
    let wr_id = state.lock().unwrap().submitted[0].0;
    state.lock().unwrap().complete(wr_id, true, "");
    os::wake(events.as_raw_fd());

    let returned = buffer_of(block_on(handle).unwrap())
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

    let a = submit(&engine, &conn, Op::Read);
    wait_until(|| state.lock().unwrap().arms >= 1);

    // Submitted while the engine is (about to be) blocked on events.
    let b = submit(&engine, &conn, Op::Write);
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
            buffer_of(block_on(handle).unwrap())
                .downcast::<Vec<u8>>()
                .unwrap()
                .len(),
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

    let handle = submit(&engine, &conn, Op::Read);
    thread::sleep(Duration::from_millis(20));
    assert_eq!(state.lock().unwrap().arms, 0);

    let wr_id = state.lock().unwrap().submitted[0].0;
    state.lock().unwrap().complete(wr_id, true, "");
    let returned = buffer_of(block_on(handle).unwrap())
        .downcast::<Vec<u8>>()
        .unwrap();
    assert_eq!(returned.len(), 16);

    engine.shutdown();
}

#[test]
fn idle_engines_keep_sweeping_within_the_busy_window() {
    // The window gates the idle block too: with nothing in flight,
    // the engine keeps sweeping for the window after the last
    // progress instead of resting on the wake channel — a submit
    // that arrives within it (the next op of a sequential
    // submit-and-wait loop) then needs no wake at all.
    let engine = Engine::new().with_busy_window(Duration::from_secs(60));
    let (conn_source, completions, state) = fake();
    state.lock().unwrap().auto = true;
    let conn = register(&engine, conn_source, completions);

    let returned = buffer_of(block_on(submit(&engine, &conn, Op::Read)).unwrap())
        .downcast::<Vec<u8>>()
        .unwrap();
    assert_eq!(returned.len(), 16);

    // Everything is resolved and the engine is idle — and still
    // sweeping: the polls keep coming, and nothing was ever armed
    // (arming belongs to the hybrid block, which needs flights).
    let before = state.lock().unwrap().polls;
    thread::sleep(Duration::from_millis(20));
    let after = state.lock().unwrap().polls;
    assert!(
        after > before,
        "the idle engine stopped sweeping within the busy window"
    );
    assert_eq!(state.lock().unwrap().arms, 0);

    engine.shutdown();
}

#[test]
fn zero_window_engines_block_when_idle_and_still_wake() {
    // `Duration::ZERO` keeps the old behavior: with nothing in
    // flight, the engine rests on the wake channel instead of
    // spinning — and a submit that arrives while it rests still
    // completes (the sleeping protocol wakes it for the command).
    let engine = Engine::new().with_busy_window(Duration::ZERO);
    let (conn_source, completions, state) = fake();
    state.lock().unwrap().auto = true;
    let conn = register(&engine, conn_source, completions);

    let returned = buffer_of(block_on(submit(&engine, &conn, Op::Read)).unwrap())
        .downcast::<Vec<u8>>()
        .unwrap();
    assert_eq!(returned.len(), 16);

    // The engine settles: with everything resolved, nothing in
    // flight, and no window to spin out, its sweeps stop (it rests
    // on the wake channel).
    let mut stable = 0;
    let mut polls = state.lock().unwrap().polls;
    for _ in 0..50 {
        thread::sleep(Duration::from_millis(1));
        let next = state.lock().unwrap().polls;
        if next == polls {
            stable += 1;
            if stable >= 10 {
                break;
            }
        } else {
            stable = 0;
            polls = next;
        }
    }
    assert!(
        stable >= 10,
        "the zero-window engine never settled into its idle block"
    );

    // A submit while it rests still completes: the sleeping protocol
    // wakes the engine for the mailbox's command.
    let returned = buffer_of(block_on(submit(&engine, &conn, Op::Write)).unwrap())
        .downcast::<Vec<u8>>()
        .unwrap();
    assert_eq!(returned.len(), 16);

    engine.shutdown();
}

#[test]
fn wait_spins_within_the_busy_window_and_resolves() {
    // `wait` mirrors the engine's blocking rule, with the busy window
    // as the spin budget: within it the waiter spins — a fast
    // operation then completes with no park cycle at all, which is
    // what takes the park/unpark round trip out of a blocking wait.
    // A wide window keeps the waiter on the spin side, however long
    // the test machine takes to turn the fake around.
    let engine = Engine::new().with_busy_window(Duration::from_secs(60));
    let (conn_source, completions, _state) = fake();
    _state.lock().unwrap().auto = true;
    let conn = register(&engine, conn_source, completions);

    let returned = submit(&engine, &conn, Op::Read)
        .wait()
        .map(buffer_of)
        .unwrap()
        .downcast::<Vec<u8>>()
        .unwrap();
    assert_eq!(returned.len(), 16);

    engine.shutdown();
}

#[test]
fn wait_parks_past_the_busy_window_and_still_resolves() {
    // `Duration::ZERO`: the budget is spent at once, so the waiter
    // parks immediately — the old behavior — and the engine's waker
    // still resolves it (a wake during a spin leaves an unpark
    // token, so the spin-to-park transition cannot miss one either).
    let engine = Engine::new().with_busy_window(Duration::ZERO);
    let (conn_source, completions, _state) = fake();
    _state.lock().unwrap().auto = true;
    let conn = register(&engine, conn_source, completions);

    let returned = submit(&engine, &conn, Op::Read)
        .wait()
        .map(buffer_of)
        .unwrap()
        .downcast::<Vec<u8>>()
        .unwrap();
    assert_eq!(returned.len(), 16);

    engine.shutdown();
}

#[test]
fn wait_survives_the_reuse_of_a_resolved_slot() {
    // The lockless fast path rides slot state: a resolved slot is
    // freed and reused (the free list is LIFO — the same index), and
    // the next operation's wait must see only that operation's
    // lifecycle. A stale `resolved` would resolve the second wait
    // out of thin air; a stale `done` flag would only park it early
    // (the waker still resolves it — a latency bug, not a
    // correctness one). The probe is the wait itself: the second
    // wait must not resolve before its own completion, and must
    // resolve right after it.
    let engine = Engine::new().with_busy_window(Duration::from_secs(60));
    let (conn_source, completions, state) = fake();
    let conn = register(&engine, conn_source, completions);

    // The first operation: submitted, completed by hand — its wait
    // resolves through the fast path, and its slot frees.
    let one = submit(&engine, &conn, Op::Read);
    wait_until(|| !state.lock().unwrap().submitted.is_empty());
    let first = state.lock().unwrap().submitted[0].0;
    state.lock().unwrap().complete(first, true, "");
    assert_eq!(
        one.wait()
            .map(buffer_of)
            .unwrap()
            .downcast::<Vec<u8>>()
            .unwrap()
            .len(),
        16
    );

    // The second operation takes the freed slot and is not
    // completed: nothing may resolve its wait.
    let two = submit(&engine, &conn, Op::Read);
    wait_until(|| state.lock().unwrap().submitted.len() == 2);
    let (signalled, resolved) = std::sync::mpsc::channel();
    let waiter = thread::spawn(move || {
        let result = two.wait();
        signalled.send(()).unwrap();
        result
    });
    assert!(
        resolved.recv_timeout(Duration::from_millis(100)).is_err(),
        "the second operation resolved before its own completion"
    );

    // Completed by hand: the wait resolves, through the reused
    // slot's own state.
    let second = state.lock().unwrap().submitted[1].0;
    state.lock().unwrap().complete(second, true, "");
    assert_eq!(
        buffer_of(waiter.join().unwrap().unwrap())
            .downcast::<Vec<u8>>()
            .unwrap()
            .len(),
        16
    );

    engine.shutdown();
}

#[test]
#[ignore = "a diagnostic, not an assertion: prints the engine machinery's per-op round trip against the fakes (no verbs, no network)"]
fn machinery_round_trip_floor() {
    // What a submit-and-wait pays for the engine machinery alone: the
    // locks, the allocations, the two thread handoffs, and the
    // iteration boundaries — everything but the verbs post/poll and
    // the wire. The fake auto-completes at dispatch, so the loop is
    // the pure machinery path: submit, mailbox hop, dispatch, the
    // sweep's same-iteration completion poll, resolve, waiter hop.
    // The numbers are machine- and placement-specific (pin both
    // threads like the benchmarks do for a stable one; `--nocapture`
    // to see this print).
    let engine = Engine::new();
    let (conn_source, completions, state) = fake();
    state.lock().unwrap().auto = true;
    let conn = register(&engine, conn_source, completions);

    // Warm up: the mailbox, the slot, the waker, and the engine's
    // iteration all settle.
    for _ in 0..10_000 {
        let _ = submit(&engine, &conn, Op::Read).wait();
    }
    let iters: u32 = std::env::var("RDMALIB_MACHINERY_ITERS")
        .ok()
        .and_then(|iters| iters.parse().ok())
        .unwrap_or(100_000);
    let mut buffer = vec![0u8; 16];
    let mut samples = Vec::with_capacity(iters as usize);
    for _ in 0..iters {
        let t0 = std::time::Instant::now();
        buffer = *submit(&engine, &conn, Op::Read)
            .wait()
            .map(buffer_of)
            .unwrap()
            .downcast::<Vec<u8>>()
            .unwrap();
        samples.push(t0.elapsed());
        std::hint::black_box(&buffer);
    }
    samples.sort();
    let at = |quantile: f64| samples[(samples.len() as f64 * quantile) as usize];
    println!(
        "machinery round trip over {} ops: min {:?} p50 {:?} p90 {:?}",
        iters,
        samples.first().unwrap(),
        at(0.5),
        at(0.9)
    );

    engine.shutdown();
}

#[test]
fn op_trace_records_complete_timelines_only() {
    // The operation timeline: five stamps per operation, four
    // segments per record, and nothing for an incomplete timeline —
    // a failed dispatch never stamps the post pair, so its operation
    // resolves (with the error) without recording.
    //
    // On: every waited operation records; off (the default): none.
    let engine = Engine::new().with_op_trace(true);
    let (conn_source, completions, state) = fake();
    state.lock().unwrap().auto = true;
    let conn = register(&engine, conn_source, completions);

    let _ = submit(&engine, &conn, Op::Read).wait();
    let samples = engine.op_trace_samples();
    assert_eq!(samples.len(), 1, "the traced operation recorded");
    let [submit_to_post, post, post_to_complete, complete_to_done] = samples[0];
    // Sanity, not timing: the segments are causal (submit happens
    // before the post, the post before the completion's resolution,
    // the resolution before the waiter's poll), non-negative by
    // construction, and bounded by a second.
    for segment in [submit_to_post, post, post_to_complete, complete_to_done] {
        assert!(segment < 1_000_000_000, "segment {segment} ns is not sane");
    }
    assert!(submit_to_post + post + post_to_complete + complete_to_done > 0);

    // A failed dispatch: the post pair is never stamped, the
    // operation resolves with the error, and nothing records.
    {
        let mut state = state.lock().unwrap();
        state.fail_submit = Some("fail the next submit".to_owned());
    }
    let error = expect_err(submit(&engine, &conn, Op::Read).wait());
    assert_eq!(error.to_string(), "fail the next submit");
    assert_eq!(
        engine.op_trace_samples().len(),
        1,
        "the failed dispatch recorded nothing"
    );

    engine.shutdown();
}

#[test]
fn registered_buffers_post_directly_and_resolve_with_nothing() {
    // The registered-buffer operation: the post runs against the
    // registered address itself — no pool slice in between, nothing
    // parked in the slot — and the operation resolves with nothing
    // back (the buffer was the user's all along).
    let engine = Engine::new();
    let (conn_source, completions, state) = fake();
    state.lock().unwrap().auto = true;
    let conn = register(&engine, conn_source, completions);

    // A live buffer, registered through the engine: the registration
    // lands on the engine thread before the call returns.
    let memory = [0u8; 16];
    let addr = memory.as_ptr() as u64;
    engine.register_buffer(&conn, addr, 16).unwrap();
    assert_eq!(state.lock().unwrap().registered_buffers, [(addr, 16)]);

    let outcome = engine
        .submit_registered(&conn, Op::Read, target(), addr, 16)
        .unwrap()
        .wait()
        .unwrap();
    assert!(matches!(outcome, crate::engine::Outcome::Unit));

    // The post saw the registered address itself, exactly as
    // submitted. (Scoped: the guard must not live across the
    // shutdown below — the engine thread's teardown locks the fake
    // too.)
    {
        let state = state.lock().unwrap();
        let &(_, op, seen_addr, len, remote_addr, rkey) = state.submitted.first().unwrap();
        assert_eq!(
            (op, seen_addr, len, remote_addr, rkey),
            (Op::Read, addr, 16, 0x1000, 7)
        );
    }

    engine.shutdown();
}

#[test]
fn a_deregistration_lands_after_the_operations_queued_before_it() {
    // The mailbox is FIFO: a deregistration queued after an
    // operation reaches the source only once the operation was
    // posted, so the handed-over memory has nothing of itself in
    // flight through this engine's queues (the real half's `EBUSY`
    // machinery holds whatever is still on the wire).
    let engine = Engine::new();
    let (conn_source, completions, state) = fake();
    let conn = register(&engine, conn_source, completions);

    let memory = [0u8; 16];
    let addr = memory.as_ptr() as u64;
    engine.register_buffer(&conn, addr, 16).unwrap();

    // An operation on the registered buffer, not completed yet, then
    // the deregistration — the memory hands over with it — then the
    // completion, by hand.
    let op = engine
        .submit_registered(&conn, Op::Read, target(), addr, 16)
        .unwrap();
    engine
        .deregister_buffer(&conn, addr, 16, Box::new(memory))
        .unwrap();
    wait_until(|| !state.lock().unwrap().submitted.is_empty());
    let wr_id = state.lock().unwrap().submitted[0].0;
    state.lock().unwrap().complete(wr_id, true, "");
    assert!(matches!(op.wait().unwrap(), crate::engine::Outcome::Unit));

    // The deregistration reached the source, after the post it was
    // queued behind.
    wait_until(|| !state.lock().unwrap().deregistered_buffers.is_empty());
    assert_eq!(state.lock().unwrap().deregistered_buffers, [(addr, 16)]);
    assert_eq!(state.lock().unwrap().registered_buffers, [(addr, 16)]);

    engine.shutdown();
}

#[test]
fn readers_register_buffers_and_operate_on_them_in_place() {
    // The reader-side registered-buffer path, end to end against the
    // fakes: the provider registers a group's buffer (through the
    // group's engine connection), the region reads into it in place —
    // the post runs against the registered address, nothing parked,
    // nothing copied — writes from it, the borrow ending with each
    // wait, and the handle's drop deregisters, handing the memory
    // over.
    let engine = Engine::new();
    let (conn_source, completions, state) = fake();
    state.lock().unwrap().auto = true;
    let conn = engine
        .register(Box::new(conn_source), Some(Box::new(completions)))
        .unwrap();

    // A provider whose group-0 session's connection is the fake,
    // registered with the engine, and a region of that group.
    let provider =
        RemoteMemoryProvider::with_engine(RemoteMemoryProviderAddr::default(), engine.clone());
    let connection = {
        let mut sessions = provider.sessions.borrow_mut();
        let session = sessions
            .entry(0)
            .or_insert_with(|| crate::readers::GroupSession {
                channel: None,
                connection: Rc::new(RefCell::new(crate::readers::GroupConnection::empty(
                    engine.clone(),
                ))),
            });
        session.connection.borrow_mut().conn = Some(conn);
        Rc::clone(&session.connection)
    };
    let region = RemoteMemoryRegion {
        connection,
        remote_addr: 0x2000,
        size: 4096,
        rkey: 7,
        group: 0,
        elem_size: 1,
        elem_align: 1,
        elem_type: "u8".into(),
    };

    // Register a buffer of the group and read into it, in place.
    let mut rbuf = provider.register_buffer(0, vec![0u8; 32]).unwrap();
    let (addr, size) = state.lock().unwrap().registered_buffers[0];
    assert_eq!(size, 32);

    region.read_into_rbuf(8, &mut rbuf).unwrap().wait().unwrap();
    {
        // The post ran against the registered address directly — the
        // remote end at the region's offset — and resolved with the
        // buffer in place (the fake auto-completes).
        let state = state.lock().unwrap();
        let &(_, op, seen_addr, len, remote_addr, rkey) = state.submitted.first().unwrap();
        assert_eq!((op, seen_addr, len), (Op::Read, addr, 32));
        assert_eq!((remote_addr, rkey), (0x2008, 7));
    }

    // Between operations the buffer is the reader's again; a write
    // from it posts the same registered memory.
    rbuf.as_mut_slice()[0] = 9;
    region.write_rbuf(8, &mut rbuf).unwrap().wait().unwrap();
    assert_eq!(rbuf.len(), 32);
    assert_eq!(state.lock().unwrap().submitted.len(), 2);

    // A region of another group cannot see the registration: the
    // mismatch is rejected eagerly.
    let other_group = RemoteMemoryRegion {
        connection: Rc::new(RefCell::new(crate::readers::GroupConnection::empty(
            engine.clone(),
        ))),
        remote_addr: 0x2000,
        size: 4096,
        rkey: 7,
        group: 5,
        elem_size: 1,
        elem_align: 1,
        elem_type: "u8".into(),
    };
    let rejected = other_group.read_into_rbuf(0, &mut rbuf);
    let rejected = match rejected {
        Err(error) => error.to_string(),
        Ok(_) => panic!("a group-mismatched region accepted the buffer"),
    };
    assert_eq!(
        rejected,
        "the buffer is registered with group 0, the region is in group 5 — the buffer's registration is not visible to the region's connection"
    );

    // The handle's drop deregisters, handing the memory over.
    drop(rbuf);
    wait_until(|| !state.lock().unwrap().deregistered_buffers.is_empty());
    assert_eq!(state.lock().unwrap().deregistered_buffers, [(addr, size)]);

    engine.shutdown();
}

#[test]
fn concurrent_submissions_complete_through_the_phase_machinery() {
    // The lock-free slab under a real race storm: many threads at
    // once — submitting (the Treiber pop), waiting (the phase take,
    // the waker dance), and abandoning (the Abandoned transition, the
    // engine's resolve-side cleanup) — against one engine and one
    // connection. Every accepted operation completes with its own
    // sixteen bytes back, every abandoned one leaves nothing of
    // itself behind, and the machinery never wedges: the drain at
    // shutdown completes and the connection is reaped clean.
    const THREADS: usize = 8;
    const OPS: usize = 200;

    let engine = Engine::new();
    let (conn_source, completions, state) = fake();
    state.lock().unwrap().auto = true;
    let conn = register(&engine, conn_source, completions);

    let results: Vec<_> = (0..THREADS)
        .map(|thread| {
            let engine = engine.clone();
            let conn = conn.clone();
            thread::spawn(move || {
                let mut waited = 0;
                for op in 0..OPS {
                    let handle = submit(
                        &engine,
                        &conn,
                        if op % 2 == 0 { Op::Read } else { Op::Write },
                    );
                    if thread % 2 == 0 || op % 3 != 0 {
                        // This thread waits for it: the buffer comes
                        // back, its full length, whatever the phase
                        // dance did with the slot underneath.
                        let buffer = buffer_of(handle.wait().expect("the operation completes"))
                            .downcast::<Vec<u8>>()
                            .expect("the operation's buffer back");
                        assert_eq!(buffer.len(), 16);
                        waited += 1;
                    } else {
                        // Abandon it mid-flight: the engine resolves
                        // it, frees the slot, and drops the buffer.
                        drop(handle);
                    }
                }
                waited
            })
        })
        .collect();
    let mut total_waited = 0;
    for result in results {
        total_waited += result.join().expect("no thread panicked");
    }

    // Everything submitted reached the source — nothing was lost to
    // the free list, the phases, or the storm.
    assert_eq!(state.lock().unwrap().submitted.len(), THREADS * OPS);
    assert!(total_waited > 0);

    // The connection tears down clean: its flush resolves nothing
    // (auto-completions resolved everything), the sweep reaps it,
    // and the shutdown drains without a wedged slot stopping it.
    engine.destroy(&conn).unwrap();
    engine.shutdown();
}

#[test]
fn the_send_queue_bound_holds_under_concurrent_submission() {
    // The bound's atomic fetch-add-revert dance under a storm: four
    // threads against a depth of four, every submit either stays
    // (and completes — the buffer back) or fails eagerly with the
    // full-queue error, never a wedge and never a wrong error.
    const THREADS: usize = 4;
    const OPS: usize = 500;
    const DEPTH: usize = 4;

    let engine = Engine::new();
    let (conn_source, completions, state) = fake();
    {
        let mut state = state.lock().unwrap();
        state.auto = true;
        state.depth = DEPTH;
    }
    let conn = register(&engine, conn_source, completions);

    let results: Vec<_> = (0..THREADS)
        .map(|_| {
            let engine = engine.clone();
            let conn = conn.clone();
            thread::spawn(move || {
                let mut stayed = 0;
                let mut rejected = 0;
                for _ in 0..OPS {
                    // The raw submit (the test's helper unwraps):
                    // the bound is eager, the failure comes from
                    // the submit itself, before the handle exists.
                    let handle =
                        engine.submit(&conn, Op::Read, target(), Submitted::new(vec![0u8; 16]));
                    let handle = match handle {
                        Ok(handle) => handle,
                        Err(error) => {
                            assert!(
                                error.to_string().contains("send queue is full"),
                                "the only eager rejection is the full queue, not {error}"
                            );
                            rejected += 1;
                            continue;
                        }
                    };
                    assert_eq!(
                        buffer_of(handle.wait().expect("the accepted operation completes"))
                            .downcast::<Vec<u8>>()
                            .expect("the operation's buffer back")
                            .len(),
                        16
                    );
                    stayed += 1;
                }
                (stayed, rejected)
            })
        })
        .collect();
    let mut stayed = 0;
    let mut rejected = 0;
    for result in results {
        let (s, r) = result.join().expect("no thread panicked");
        stayed += s;
        rejected += r;
    }

    // Every operation was accounted for exactly once, and the
    // accepted ones completed — the bound never admitted a stuck op.
    assert_eq!(stayed + rejected, THREADS * OPS);
    assert_eq!(state.lock().unwrap().submitted.len(), stayed);
    // A connection at its depth still serves: the next sequential
    // submit-and-wait passes through the same ticket.
    let handle = engine.submit(&conn, Op::Read, target(), Submitted::new(vec![0u8; 16]));
    assert!(handle.is_ok());
    drop(handle);
    engine.shutdown();
}

#[test]
fn op_trace_is_off_by_default() {
    // The default engine (no `RDMALIB_OP_TRACE=1` in the
    // environment, the test suite's) records nothing: an untraced
    // operation pays no stamps and no records.
    let engine = Engine::new();
    let (conn_source, completions, state) = fake();
    state.lock().unwrap().auto = true;
    let conn = register(&engine, conn_source, completions);

    let _ = submit(&engine, &conn, Op::Read).wait();
    assert_eq!(engine.op_trace_samples().len(), 0);

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
    let returned = buffer_of(block_on(submit(&engine, &conn, Op::Read)).unwrap())
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
