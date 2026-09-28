//! The operations engine: one library-owned thread that posts and
//! polls every reader's one-sided operations.
//!
//! Design and decisions: `docs/async_engine.md`. In short —
//!
//! - submission is eager and mailbox-based: the checks run on the
//!   submitting thread, the buffer is parked in an operation slot,
//!   and a command is queued; every verbs call on the operation path
//!   runs on the engine thread, whose connection seam is the
//!   [`OpSource`] trait (implemented for `rdma::Connection` in
//!   `rdma/verbs.rs`, and for injected fakes in the engine's tests);
//! - polling is queue-level, never operation-level: the engine sweeps
//!   the registered connections round-robin, a bounded batch per
//!   connection per sweep, and routes every completion by the wr_id
//!   its slot was packed into — any number of operations wait with
//!   interleaved polling from this one thread, which is what makes
//!   CPU pinning ([`Engine::on_cpu`]) meaningful;
//! - operation handles ([`OpHandle`]) are plain `std` futures over
//!   the slots: await, join, select, or drop them — abandonment is
//!   safe, the slot owns the buffer and its registration until the
//!   completion is processed;
//! - the loop is hybrid: busy while operations are in flight, blocked
//!   on its wake pipe when idle (completion-channel events join the
//!   block later, per the design doc's step plan).

use std::any::Any;
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::io;
use std::os::fd::{AsRawFd, OwnedFd, RawFd};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::os;

/// Completions drained per completion source per sweep — the shared
/// queue's batch; the verbs half of the seam batches to the same
/// size.
pub(crate) const SWEEP_BATCH: usize = 16;

/// Which side of a one-sided operation to run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Op {
    Read,
    Write,
}

impl Op {
    /// The operation's name, for error messages.
    pub(crate) fn name(&self) -> &'static str {
        match self {
            Op::Read => "read",
            Op::Write => "write",
        }
    }
}

/// Where a one-sided operation lands: the address of the remote
/// region (plus the offset already applied) and the rkey that
/// authorizes access to it.
#[derive(Clone, Copy)]
pub(crate) struct OpTarget {
    pub(crate) remote_addr: u64,
    pub(crate) rkey: u32,
}

/// The local half of an operation: the buffer, moved in — the slot
/// owns it until the operation resolves — with the heap address and
/// byte size its `Vec` had, captured before the type erasure (moving
/// a `Vec` never moves its heap allocation, so the address stays
/// valid however the box travels).
pub(crate) struct Submitted {
    pub(crate) buffer: Box<dyn Any + Send>,
    pub(crate) addr: u64,
    pub(crate) len: usize,
}

impl Submitted {
    /// Capture a `Vec`'s heap address and byte size, then erase it.
    pub(crate) fn new<T: Any + Send>(vec: Vec<T>) -> Self {
        let addr = vec.as_ptr() as u64;
        let len = std::mem::size_of_val(vec.as_slice());
        Self {
            buffer: Box::new(vec),
            addr,
            len,
        }
    }
}

/// One completed operation, as the engine routes it: the wr_id the
/// operation was posted with, and its outcome (the error string is
/// empty when `ok`). The seam's poll fills these; the verbs half
/// writes a complete error message, and so do the fakes.
pub(crate) struct Completion {
    pub(crate) wr_id: u64,
    pub(crate) ok: bool,
    pub(crate) error: String,
}

/// The in-flight half of a submitted operation, as the connection
/// that posted it holds its local bytes: a registration, or a slice
/// lent out of the connection's pooled registration (see
/// `rdma/verbs.rs`). The engine holds it from the post until the
/// completion is processed, then calls [`InFlight::finish`] — still
/// on the engine thread — and releases it.
pub(crate) trait InFlight: Send {
    /// The operation's completion arrived, with its outcome. Copy
    /// out what the device wrote (a read that ran through pooled
    /// memory), release what the half holds — whatever that is —
    /// here. Called exactly once, before the operation's buffer
    /// resolves; a drop without a finish (an engine bug, or a slot
    /// vanished between post and completion) releases without the
    /// copy.
    fn finish(&mut self, ok: bool);
}

/// The trivial hold: nothing to release, nothing to copy. The
/// engine's fakes post it.
impl InFlight for () {
    fn finish(&mut self, _ok: bool) {}
}

/// The connection seam the engine drives: posting operations and
/// tearing the connection down. One per registered connection — the
/// real one is `rdma::Connection` (see `rdma/verbs.rs`); the engine's
/// tests inject fakes.
///
/// `depth` is the connection's send-queue depth — the operations that
/// may be in flight on it at once. Read once, at registration, to
/// bound submissions: the engine never posts more than it.
/// `submit` posts the operation over the local memory at
/// `addr`..`addr+len` — registered, or pooled, however the half
/// below chooses to hold it — tagged `wr_id`, and returns the
/// in-flight hold of that memory, which the engine keeps until the
/// completion is processed. `close` starts teardown: the outstanding
/// operations flush as error completions, still arriving through the
/// shared completion source, until the connection is drained and
/// dropped.
pub(crate) trait OpSource: Send {
    fn depth(&self) -> usize;
    fn submit(
        &mut self,
        op: Op,
        addr: u64,
        len: usize,
        target: OpTarget,
        wr_id: u64,
    ) -> io::Result<Box<dyn InFlight>>;
    fn close(&mut self);
}

/// The completion half of the engine: one per (engine, device) —
/// every connection on the device posts its completions into it
/// ([`crate::rdma::SharedCompletions`] is the real one; the engine's
/// tests inject fakes), so one poll drains them all, interleaved in
/// arrival order.
///
/// The hybrid-idle half: `event_fd` is the descriptor that becomes
/// readable when the source's queue fires a completion event (none:
/// the engine may not block while this source has anything in
/// flight); `arm` arranges for the next completion ADDED afterwards
/// to fire it — one already sitting in the queue does not, so the
/// engine polls once more after arming before it blocks;
/// `consume_events` handles one fired event (poll(2) said the
/// descriptor was readable first — a get without a pending event
/// would block).
pub(crate) trait CompletionSource: Send {
    fn poll(&mut self, out: &mut [Completion]) -> io::Result<usize>;
    fn event_fd(&self) -> Option<RawFd>;
    fn arm(&mut self) -> io::Result<()>;
    fn consume_events(&mut self) -> io::Result<()>;
}

/// The commands the engine's mailbox carries, FIFO: a session's
/// `Register` precedes everything submitted for its connection, and
/// its `Destroy` precedes anything submitted after the teardown
/// began.
enum Command {
    /// Take a connection over: the engine thread owns the source —
    /// every verbs call on its operation path runs there — from here
    /// on. With a completion source (the first connection on a new
    /// device, whose shared queue it posts into): the engine keeps
    /// and polls it — one per (engine, device).
    Register {
        conn: u32,
        source: Box<dyn OpSource>,
        completions: Option<Box<dyn CompletionSource>>,
    },
    /// Tear a connection down: close it (its outstanding operations
    /// flush as error completions), then drop the source, on the
    /// engine thread.
    Destroy { conn: u32 },
    /// Post one operation. The buffer is already parked in the slot
    /// `wr_id` names; the source registers the memory and posts.
    Post {
        wr_id: u64,
        conn: u32,
        op: Op,
        target: OpTarget,
        addr: u64,
        len: usize,
    },
}

/// The engine's mailbox: the queued commands, and the flag that ends
/// it. Once dead, nothing can be pushed — a submitter fails instead —
/// which is what makes the final drain of a shutdown complete.
struct Mailbox {
    dead: bool,
    queue: VecDeque<Command>,
}

/// The in-flight half of an operation's slot: the buffer (from
/// submission to resolution) and the in-flight hold of its local
/// memory (from posting to resolution — finished and dropped here).
struct Flight {
    buffer: Box<dyn Any + Send>,
    token: Option<Box<dyn InFlight>>,
}

/// One operation's state. Addressed by slot index and generation —
/// packed as the wr_id the completion is routed by.
struct Slot {
    /// Bumped every time the slot is freed, so a wr_id from a
    /// previous reuse never matches.
    generation: u32,
    conn: u32,
    /// While the operation is in flight — including the window
    /// between submission and the engine's dispatch (the buffer is
    /// parked here before the command is even queued).
    flight: Option<Flight>,
    /// Set by the engine when the completion is processed; taken by
    /// the handle.
    resolved: Option<io::Result<Box<dyn Any + Send>>>,
    /// The awaiting handle's waker, if it polled while pending.
    waker: Option<Waker>,
    /// Whether a handle still claims the result. A handle dropped
    /// before resolution leaves the engine to complete the operation
    /// anyway and free the slot (dropping the buffer itself).
    claimed: bool,
}

/// One registered connection's load: its send-queue depth — the
/// operations that may be in flight on it at once — and how many are
/// in flight right now. The submission bound runs on it: a submit
/// past the depth fails on the spot, eagerly, instead of a post
/// failing on the engine thread when the device's queue is full (the
/// `ibv_post_send` `ENOMEM` of a send queue out of work requests).
struct ConnLoad {
    depth: u32,
    in_flight: u32,
}

/// The engine's operation slots. Allocation packs
/// `(index << 32) | generation` into the wr_id the completion is
/// routed by; freeing bumps the generation. The registered
/// connections' loads ride along, under the same lock: the submit
/// bound is a check-and-increment of the load, so no two submissions
/// can slip past the depth of one connection.
struct Slab {
    slots: Vec<Slot>,
    free: Vec<u32>,
    conn_loads: HashMap<u32, ConnLoad>,
}

impl Slab {
    /// Park `buffer` for connection `conn`, returning the packed slot
    /// id.
    fn alloc(&mut self, conn: u32, buffer: Box<dyn Any + Send>) -> u64 {
        let index = match self.free.pop() {
            Some(index) => index,
            None => {
                self.slots.push(Slot {
                    generation: 0,
                    conn,
                    flight: None,
                    resolved: None,
                    waker: None,
                    claimed: true,
                });
                (self.slots.len() - 1) as u32
            }
        };
        let slot = &mut self.slots[index as usize];
        slot.conn = conn;
        slot.flight = Some(Flight {
            buffer,
            token: None,
        });
        slot.resolved = None;
        slot.waker = None;
        slot.claimed = true;
        (index as u64) << 32 | slot.generation as u64
    }

    /// Release a slot — dropping whatever it still holds (an
    /// unclaimed result, an abandoned waker) — and mark it reusable,
    /// generation bumped so stale wr_ids stop matching.
    fn free(&mut self, index: u32) {
        let slot = &mut self.slots[index as usize];
        slot.flight = None;
        slot.resolved = None;
        slot.waker = None;
        slot.generation = slot.generation.wrapping_add(1);
        self.free.push(index);
    }

    /// The slot a packed wr_id names, if its generation matches.
    fn slot_of_mut(&mut self, wr_id: u64) -> Option<&mut Slot> {
        let index = (wr_id >> 32) as usize;
        let generation = wr_id as u32;
        let slot = self.slots.get_mut(index)?;
        (slot.generation == generation).then_some(slot)
    }

    /// Whether `conn` has an operation in flight.
    fn has_flights(&self, conn: u32) -> bool {
        self.slots
            .iter()
            .any(|slot| slot.conn == conn && slot.flight.is_some())
    }

    /// Every in-flight operation's packed wr_id.
    fn all_flights(&self) -> Vec<u64> {
        self.slots
            .iter()
            .enumerate()
            .filter(|(_, slot)| slot.flight.is_some())
            .map(|(index, slot)| (index as u64) << 32 | slot.generation as u64)
            .collect()
    }

    /// Whether any operation is in flight.
    fn any_flights(&self) -> bool {
        self.slots.iter().any(|slot| slot.flight.is_some())
    }
}

/// The engine's shared state: everything the submitting threads and
/// the awaiting handles touch (kept alive by either), and the knobs
/// of the engine thread's loop. The verbs objects never come here —
/// they live on the engine thread — so nothing in this struct needs
/// more than plain data and locks.
struct EngineShared {
    cpu: Option<u32>,
    shutdown: AtomicBool,
    /// Set while the engine thread blocks on its wake pipe. Stored
    /// before the engine checks the mailbox, so a command pushed
    /// after the check sees the flag and writes the wake byte —
    /// pushed before it, the check finds the command. No wake is
    /// ever missed.
    sleeping: AtomicBool,
    next_conn: AtomicU32,
    mailbox: Mutex<Mailbox>,
    slab: Mutex<Slab>,
    wake_write: OwnedFd,
    /// The busy window, in flight with nothing completing, before the
    /// engine arms the completion events and blocks on them; the knob
    /// of [`Engine::with_busy_window`]. A mutex, not a plain field:
    /// the builder may turn it while the engine runs.
    busy_window: Mutex<Duration>,
}

/// How long a default engine busy-polls while operations are in
/// flight but nothing completes, before arming the completion events
/// and blocking on them — sized to cover a round trip, so short
/// operations never pay the arm-and-block cost.
const DEFAULT_BUSY_WINDOW: Duration = Duration::from_micros(100);

/// The engine's controlling half: the shared state and the engine
/// thread's join handle. Dropping the last one stops the engine.
struct EngineControl {
    state: Arc<EngineShared>,
    thread: Mutex<Option<JoinHandle<()>>>,
}

impl EngineControl {
    /// Stop the engine: no new commands (the mailbox dies with the
    /// final drain), everything posted resolves (the closed
    /// connections flush their operations as error completions),
    /// everything still queued resolves as an error — then join the
    /// engine thread. Idempotent.
    fn stop(&self) {
        self.state.shutdown.store(true, Ordering::Release);
        // The engine may be blocked on its wake pipe; the byte is
        // written unconditionally, unlike a submission's wake (which
        // checks `sleeping` — a busy engine needs no waking).
        os::wake(self.state.wake_write.as_raw_fd());
        if let Some(handle) = self.thread.lock().unwrap().take() {
            let _ = handle.join();
        }
    }
}

impl Drop for EngineControl {
    fn drop(&mut self) {
        self.stop();
    }
}

/// The operations engine: one library-owned thread that posts and
/// polls the one-sided operations of every connection registered with
/// it. Design and decisions: `docs/async_engine.md`.
///
/// Clone the handle to share one engine — one polling thread, one
/// CPU — among several controllers, and hand the clones to their
/// constructors. The engine lives as long as any handle or any
/// in-flight operation; the last one out drains (every pending
/// operation resolves, every connection is torn down, on the engine
/// thread) and stops the thread.
///
/// ```rust
/// # use rdmalib::Engine;
/// let engine = Engine::new();
/// // ... the reader side registers its connections with the engine
/// // and submits its operations through it ...
/// engine.shutdown();
/// ```
#[derive(Clone)]
pub struct Engine {
    control: Arc<EngineControl>,
}

impl Default for Engine {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The pinned CPU tells engines apart; the shared state behind
        // the arc (queues, slots, raw descriptors) has no useful face.
        f.debug_struct("Engine").field("cpu", &self.cpu()).finish()
    }
}

impl Engine {
    /// Start an unpinned engine: its thread runs wherever the OS
    /// schedules it — busy-polling while operations are in flight,
    /// blocked on its wake channel when idle.
    pub fn new() -> Self {
        Self::start(None)
    }

    /// Start an engine pinned to `cpu`: its thread pins itself there
    /// before entering the loop, so all the posting and polling of
    /// every operation submitted to this engine runs on one CPU. The
    /// CPU is validated against this process's affinity mask here;
    /// an unusable one fails before the thread starts.
    pub fn on_cpu(cpu: u32) -> io::Result<Self> {
        os::ensure_cpu_available(cpu)?;
        Ok(Self::start(Some(cpu)))
    }

    /// The CPU this engine's thread is pinned to, if any.
    pub fn cpu(&self) -> Option<u32> {
        self.control.state.cpu
    }

    /// Set the busy window: how long the engine busy-polls while
    /// operations are in flight but nothing completes, before arming
    /// the completion events and blocking on them (the hybrid idle).
    ///
    /// `Duration::ZERO` blocks as soon as a sweep finds nothing;
    /// `Duration::MAX` never blocks while anything is in flight —
    /// pure busy-polling, the dedicated-core case. The default is
    /// sized to cover a round trip.
    pub fn with_busy_window(self, window: Duration) -> Self {
        *self.control.state.busy_window.lock().unwrap() = window;
        self
    }

    /// Stop the engine: see [`EngineControl::stop`]. Idempotent — and
    /// it runs anyway when the last handle drops, if it never ran.
    pub fn shutdown(&self) {
        self.control.stop();
    }

    fn start(cpu: Option<u32>) -> Self {
        let (wake_read, wake_write) =
            os::pipe_pair().expect("cannot create the engine's wake pipe");
        let state = Arc::new(EngineShared {
            cpu,
            shutdown: AtomicBool::new(false),
            sleeping: AtomicBool::new(false),
            next_conn: AtomicU32::new(0),
            mailbox: Mutex::new(Mailbox {
                dead: false,
                queue: VecDeque::new(),
            }),
            slab: Mutex::new(Slab {
                slots: Vec::new(),
                free: Vec::new(),
                conn_loads: HashMap::new(),
            }),
            wake_write,
            busy_window: Mutex::new(DEFAULT_BUSY_WINDOW),
        });
        let control = Arc::new(EngineControl {
            state: Arc::clone(&state),
            thread: Mutex::new(None),
        });
        let core = EngineCore {
            state,
            wake_read,
            sources: HashMap::new(),
            completions: Vec::new(),
            cursor: 0,
            closing_all: false,
        };
        let handle = thread::spawn(move || core.run());
        *control.thread.lock().unwrap() = Some(handle);
        Self { control }
    }

    /// Register a connection with the engine: from here the engine
    /// thread owns it — every verbs call on its operation path runs
    /// there. Returns the connection id its operations are submitted
    /// with.
    ///
    /// `completions`, given with the first connection on a device,
    /// is the device's shared completion source — every connection on
    /// the device (registered after it with `None`) posts its
    /// completions into it, and the engine polls it: one poll site
    /// per device. Connections on another device bring their own
    /// shared source; the engine polls each, round-robin.
    pub(crate) fn register(
        &self,
        source: Box<dyn OpSource>,
        completions: Option<Box<dyn CompletionSource>>,
    ) -> io::Result<u32> {
        // The connection's send-queue depth, read here on the
        // registering thread before the source moves to the engine:
        // the submission bound needs it before the engine could have
        // dispatched the registration.
        let depth = source.depth() as u32;
        let conn = self
            .control
            .state
            .next_conn
            .fetch_add(1, Ordering::Relaxed);
        self.push(Command::Register {
            conn,
            source,
            completions,
        })?;
        // The load enters before the id returns, so the caller's very
        // first submission on it already finds the bound in place.
        self.control
            .state
            .slab
            .lock()
            .unwrap()
            .conn_loads
            .insert(conn, ConnLoad { depth, in_flight: 0 });
        Ok(conn)
    }

    /// Tear a registered connection down: close it — its outstanding
    /// operations flush as error completions, resolving their handles
    /// — then drop it, on the engine thread.
    pub(crate) fn destroy(&self, conn: u32) -> io::Result<()> {
        self.push(Command::Destroy { conn })
    }

    /// The connection's send-queue depth — the operations that may be
    /// in flight on it at once; `None` once the connection is gone.
    /// The bound [`Self::submit`] enforces, read back for the
    /// reader-side accessors.
    pub(crate) fn in_flight_limit(&self, conn: u32) -> Option<usize> {
        self.control
            .state
            .slab
            .lock()
            .unwrap()
            .conn_loads
            .get(&conn)
            .map(|load| load.depth as usize)
    }

    /// Submit one operation, eagerly: the checks run here — a
    /// zero-length operation or a never-registered connection fails
    /// on the spot — the buffer is parked in its slot, and the engine
    /// thread registers it, posts it, and resolves the returned
    /// handle with the completion's outcome (the buffer comes back
    /// with it on success). A connection torn down in the meantime
    /// resolves the handle with an error instead.
    ///
    /// Dropping the handle abandons the operation: it still completes
    /// (or flushes), and the engine frees the buffer.
    pub(crate) fn submit(
        &self,
        conn: u32,
        op: Op,
        target: OpTarget,
        submitted: Submitted,
    ) -> io::Result<OpHandle> {
        let state = &self.control.state;
        if submitted.len == 0 {
            return Err(invalid(format!("cannot {} zero bytes", op.name())));
        }
        if conn >= state.next_conn.load(Ordering::Relaxed) {
            return Err(invalid(format!(
                "connection {conn} was never registered with the engine"
            )));
        }
        let wr_id = {
            let mut slab = state.slab.lock().unwrap();
            let Some(load) = slab.conn_loads.get_mut(&conn) else {
                // The id was once registered (the never-registered
                // are caught above) but its connection was destroyed
                // and reaped: nothing of it is left to post through.
                return Err(invalid(format!(
                    "connection {conn} is gone (destroyed, and its operations flushed)"
                )));
            };
            if load.in_flight >= load.depth {
                // Eager, like every submission check: the send queue
                // is full — await in-flight operations first. (This
                // is the old C++ library's `Exceeded rdma completion
                // queue size` guard, at the async seam.)
                return Err(invalid(format!(
                    "connection {conn}'s send queue is full: {} operations in flight, its max_send_wr is {} — await in-flight operations before submitting more",
                    load.in_flight, load.depth
                )));
            }
            load.in_flight += 1;
            slab.alloc(conn, submitted.buffer)
        };
        if let Err(error) = self.push(Command::Post {
            wr_id,
            conn,
            op,
            target,
            addr: submitted.addr,
            len: submitted.len,
        }) {
            // The command never queued (the engine is shut down): the
            // parked buffer is released here, on the submitting thread,
            // and the load's in-flight count gives its slot back.
            let mut slab = state.slab.lock().unwrap();
            if let Some(load) = slab.conn_loads.get_mut(&conn) {
                load.in_flight = load.in_flight.saturating_sub(1);
            }
            slab.free((wr_id >> 32) as u32);
            return Err(error);
        }
        Ok(OpHandle {
            state: Arc::clone(state),
            slot: wr_id,
        })
    }

    /// Push a command, waking a blocked engine: the `sleeping` flag
    /// was stored before the engine last looked at the mailbox, so a
    /// push after that sees it (and writes the wake byte); a push
    /// before it left the command in the mailbox for the engine's
    /// next drain.
    fn push(&self, command: Command) -> io::Result<()> {
        let state = &self.control.state;
        {
            let mut mailbox = state.mailbox.lock().unwrap();
            if mailbox.dead {
                return Err(io::Error::other("the engine is shut down"));
            }
            mailbox.queue.push_back(command);
        }
        if state.sleeping.load(Ordering::Acquire) {
            os::wake(state.wake_write.as_raw_fd());
        }
        Ok(())
    }
}

/// The future of one submitted operation: it resolves with the
/// operation's outcome — the buffer back on success (the same `Vec`
/// it was submitted as, via [`Submitted::new`]), the completion's (or
/// the submission's) error otherwise.
///
/// `Send`, so it can be awaited — spawned, selected, joined — on any
/// executor thread; only submission is bound to the submitting
/// thread. Dropping it before resolution abandons the operation,
/// safely: the engine completes it anyway and frees the buffer.
pub(crate) struct OpHandle {
    state: Arc<EngineShared>,
    slot: u64,
}

impl std::fmt::Debug for OpHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The packed slot id is enough to tell operations apart.
        f.debug_struct("OpHandle").field("slot", &self.slot).finish()
    }
}

impl Future for OpHandle {
    type Output = io::Result<Box<dyn Any + Send>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let index = (self.slot >> 32) as u32;
        let generation = self.slot as u32;
        let mut slab = self.state.slab.lock().unwrap();
        let Some(slot) = slab
            .slots
            .get_mut(index as usize)
            .filter(|slot| slot.generation == generation)
        else {
            // Unreachable by construction (a live handle holds its slot
            // until it resolves); resolved as an error, not a panic —
            // futures must not panic.
            return Poll::Ready(Err(io::Error::other(
                "the operation's slot is gone — an engine bug",
            )));
        };
        // Resolution happens under this same lock, so checking for it
        // before storing the waker cannot miss a wake.
        if let Some(resolved) = slot.resolved.take() {
            slab.free(index);
            return Poll::Ready(resolved);
        }
        slot.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl Drop for OpHandle {
    fn drop(&mut self) {
        let index = (self.slot >> 32) as u32;
        let generation = self.slot as u32;
        let mut slab = self.state.slab.lock().unwrap();
        let Some(slot) = slab.slots.get_mut(index as usize) else {
            return;
        };
        if slot.generation != generation {
            return;
        }
        // Claim released: the engine completes the operation, frees
        // the slot, and drops the buffer — unless it already
        // resolved, in which case dropping the result is the
        // abandoning handle's job.
        slot.claimed = false;
        if slot.resolved.is_some() {
            slab.free(index);
        }
    }
}

impl OpHandle {
    /// Block until the operation resolves — the join for callers
    /// without an executor (the library embeds none; with one, await
    /// the future instead). The engine thread does the waiting work;
    /// this thread parks until the engine's wake. Operations waited
    /// in turn still overlap: they complete concurrently, on the
    /// engine thread.
    pub(crate) fn wait(self) -> io::Result<Box<dyn Any + Send>> {
        let mut future = std::pin::pin!(self);
        let waker = thread_waker();
        let mut cx = Context::from_waker(&waker);
        loop {
            match future.as_mut().poll(&mut cx) {
                Poll::Ready(result) => return result,
                Poll::Pending => thread::park(),
            }
        }
    }
}

/// A waker that unparks the current thread — the park/wake pair
/// behind [`OpHandle::wait`] and the engine tests' `block_on`. The
/// four vtable functions manage the `Arc<Thread>` below the raw
/// pointer, per `RawWaker`'s contract: `clone` bumps the count, `wake`
/// and `drop` consume one, `wake_by_ref` borrows.
pub(crate) fn thread_waker() -> Waker {
    fn clone(data: *const ()) -> RawWaker {
        unsafe { Arc::increment_strong_count(data as *const thread::Thread) };
        RawWaker::new(data, &VTABLE)
    }
    unsafe fn wake(data: *const ()) {
        unsafe { Arc::from_raw(data as *const thread::Thread) }.unpark();
    }
    unsafe fn wake_by_ref(data: *const ()) {
        unsafe { (*(data as *const thread::Thread)).unpark() };
    }
    unsafe fn drop_waker(data: *const ()) {
        unsafe { drop(Arc::from_raw(data as *const thread::Thread)) };
    }
    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop_waker);
    // SAFETY: the vtable functions above manage the arc moved below
    // the pointer.
    unsafe {
        Waker::from_raw(RawWaker::new(
            Arc::into_raw(Arc::new(thread::current())) as *const (),
            &VTABLE,
        ))
    }
}

/// One registered connection, as the engine thread owns it.
struct SourceEntry {
    source: Box<dyn OpSource>,
    /// Closed (by a destroy, a failed completion source, or
    /// shutdown): nothing more is posted through it; its flush
    /// resolves what was, then it drops.
    closing: bool,
}

/// The engine thread's own state: the registered connections — their
/// sources (the verbs objects) owned here, torn down here — the
/// completion sources (one shared queue per device), the sweep
/// rotation, and the wake channel's read end. The `Arc` it holds is
/// released only when the loop exits, drained.
struct EngineCore {
    state: Arc<EngineShared>,
    wake_read: OwnedFd,
    sources: HashMap<u32, SourceEntry>,
    /// The completion sources, one per (engine, device): each drains
    /// every connection on its device, interleaved — one poll site
    /// per device, rotated by `cursor` (one, in the common
    /// single-device case).
    completions: Vec<Box<dyn CompletionSource>>,
    cursor: usize,
    /// Shutdown seen: the sources were closed once, now draining.
    closing_all: bool,
}

impl EngineCore {
    /// The engine's loop: take and dispatch the commands, sweep the
    /// completion sources, and either step the drain-then-exit of a
    /// shutdown or rest — busy while operations are completing (the
    /// busy window), blocked on the completion events when they are
    /// not (the hybrid idle), blocked on the wake channel when
    /// nothing is in flight at all.
    fn run(mut self) {
        // Pin this thread to its CPU first, when one was requested:
        // the CPU was validated before the thread started, on the
        // spawning thread's inherited mask; a failure (a race against
        // the process's affinity changing) leaves the loop unpinned
        // but working, with no one left to report to.
        if let Some(cpu) = self.state.cpu {
            let _ = os::pin_current_thread(cpu);
        }
        let mut last_progress = Instant::now();
        while !self.shutdown_step() {
            if self.take_commands() > 0 {
                last_progress = Instant::now();
            }
            let completed = self.sweep();
            if completed > 0 {
                last_progress = Instant::now();
            }
            if self.state.shutdown.load(Ordering::Acquire) {
                continue; // draining: keep sweeping until the flush resolves
            }
            if !self.any_flights() {
                self.block_until_wake(&[]);
            } else if last_progress.elapsed()
                < *self.state.busy_window.lock().unwrap()
            {
                continue; // the busy window: nothing to rest on yet
            } else {
                self.hybrid_block();
                last_progress = Instant::now();
            }
        }
        // Drained: the sources (any verbs objects) drop here, on the
        // engine thread, and with them the last `Arc` of the shared
        // state — unless operation handles outlive the engine, which
        // keep only their slots' resolved plain data.
    }

    /// Take every queued command and dispatch it, FIFO — the order
    /// the submitters pushed them in. Returns how many.
    fn take_commands(&mut self) -> usize {
        let commands = {
            let mut mailbox = self.state.mailbox.lock().unwrap();
            std::mem::take(&mut mailbox.queue)
        };
        let count = commands.len();
        for command in commands {
            self.dispatch(command);
        }
        count
    }

    fn dispatch(&mut self, command: Command) {
        match command {
            Command::Register {
                conn,
                source,
                completions,
            } => {
                self.sources.insert(
                    conn,
                    SourceEntry {
                        source,
                        closing: false,
                    },
                );
                if let Some(completions) = completions {
                    self.completions.push(completions);
                }
            }
            Command::Destroy { conn } => self.begin_destroy(conn),
            Command::Post {
                wr_id,
                conn,
                op,
                target,
                addr,
                len,
            } => {
                let posted = match self.sources.get_mut(&conn) {
                    Some(entry) if !entry.closing => {
                        entry.source.submit(op, addr, len, target, wr_id)
                    }
                    _ => Err(io::Error::other(format!(
                        "connection {conn} is gone or closing"
                    ))),
                };
                match posted {
                    Ok(token) => {
                        let mut slab = self.state.slab.lock().unwrap();
                        if let Some(slot) = slab.slot_of_mut(wr_id) {
                            // The hold of the local memory (the
                            // registration, or the pooled slice)
                            // lives in the slot until the completion
                            // finishes it.
                            if let Some(flight) = slot.flight.as_mut() {
                                flight.token = Some(token);
                            }
                        }
                        // A vanished slot (an engine bug — the handle
                        // dropped mid-flight does not free it) drops
                        // the token here: the hold releases without a
                        // finish.
                    }
                    Err(error) => self.resolve(wr_id, Err(error)),
                }
            }
        }
    }

    /// One completion sweep: every completion source, in rotation, a
    /// bounded batch of completions each — one source per (engine,
    /// device), draining every connection on its device
    /// interleaved, so a single-device engine has a single poll site.
    /// A source whose poll fails has every in-flight operation
    /// resolved with the error, everything closed, and itself
    /// dropped. Returns how many completions it routed.
    fn sweep(&mut self) -> usize {
        if self.completions.is_empty() {
            self.reap_closing();
            return 0;
        }
        self.cursor %= self.completions.len();
        let mut out: [Completion; SWEEP_BATCH] = std::array::from_fn(|_| Completion {
            wr_id: 0,
            ok: false,
            error: String::new(),
        });
        let mut routed = 0;
        let mut failed: Vec<(usize, io::Error)> = Vec::new();
        for _ in 0..self.completions.len() {
            let index = self.cursor;
            self.cursor = (self.cursor + 1) % self.completions.len();
            let polled = self.completions[index].poll(&mut out);
            match polled {
                Ok(count) => {
                    for completion in &out[..count] {
                        self.on_completion(completion);
                    }
                    routed += count;
                }
                Err(error) => failed.push((index, error)),
            }
        }
        self.fail_many_completions(failed);
        self.reap_closing();
        routed
    }

    /// Route one completion to its slot.
    fn on_completion(&mut self, completion: &Completion) {
        let outcome = if completion.ok {
            Ok(())
        } else {
            Err(io::Error::other(completion.error.clone()))
        };
        self.resolve(completion.wr_id, outcome);
    }

    /// Resolve the operation a wr_id names: finish the hold of its
    /// local memory (a pooled read's bytes copy into the buffer
    /// here, and a registration releases — both on the engine
    /// thread, before the buffer resolves), store the outcome — the
    /// buffer back on success, dropped on failure — and free the
    /// slot when no handle claims it. The claiming handle is woken
    /// outside the slab lock: a waker runs user code.
    fn resolve(&mut self, wr_id: u64, outcome: Result<(), io::Error>) {
        let index = (wr_id >> 32) as u32;
        let mut waker = None;
        {
            let mut slab = self.state.slab.lock().unwrap();
            let Some(slot) = slab.slot_of_mut(wr_id) else {
                return; // stale: a reused slot never matches this generation
            };
            let conn = slot.conn;
            let Some(flight) = slot.flight.take() else {
                return; // already resolved (a duplicate completion)
            };
            let ok = outcome.is_ok();
            let mut flight = flight;
            if let Some(token) = flight.token.as_mut() {
                // Before the buffer resolves: a pooling hold copies
                // the device's bytes into it here, so the handle (or
                // the buffer's drop, unclaimed) sees the read.
                token.finish(ok);
            }
            slot.resolved = Some(match outcome {
                Ok(()) => Ok(flight.buffer),
                Err(error) => {
                    drop(flight.buffer);
                    Err(error)
                }
            });
            if slot.claimed {
                waker = slot.waker.take();
            } else {
                slab.free(index);
            }
            // The work request completed — the send-queue slot is
            // free again, whether the handle claims the result or
            // the slot frees above.
            if let Some(load) = slab.conn_loads.get_mut(&conn) {
                load.in_flight = load.in_flight.saturating_sub(1);
            }
        }
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// A completion source whose polling failed: the queue it drains
    /// is broken, so everything in flight anywhere resolves with the
    /// error (nothing else would drain it), every connection closes
    /// (nothing more posts into an unpollled queue), and the source
    /// drops. (An `io::Error` is not `Clone`: the message carries on.)
    fn fail_completions(&mut self, index: usize, error: io::Error) {
        if index < self.completions.len() {
            self.completions.remove(index);
        }
        for entry in self.sources.values_mut() {
            if !entry.closing {
                entry.closing = true;
                entry.source.close();
            }
        }
        let message = error.to_string();
        let flights = self.all_flights();
        for wr_id in flights {
            self.resolve(wr_id, Err(io::Error::other(message.clone())));
        }
    }

    /// Fail several completion sources: the indices are processed
    /// highest-first, so an earlier removal does not shift a later
    /// index.
    fn fail_many_completions(&mut self, failed: Vec<(usize, io::Error)>) {
        let mut failed = failed;
        failed.sort_by_key(|(index, _)| std::cmp::Reverse(*index));
        for (index, error) in failed {
            self.fail_completions(index, error);
        }
    }

    /// Tear one connection down: close it — its outstanding operations
    /// flush as error completions — and let the sweep reap it once
    /// nothing of its is in flight (a source with nothing in flight is
    /// reaped by the very next sweep).
    fn begin_destroy(&mut self, conn: u32) {
        let Some(entry) = self.sources.get_mut(&conn) else {
            return;
        };
        if entry.closing {
            return;
        }
        entry.closing = true;
        entry.source.close();
    }

    /// Drop the closed connections with nothing in flight — their
    /// verbs teardown runs here, on the engine thread — and with them
    /// their loads: nothing of the connection is left to bound.
    fn reap_closing(&mut self) {
        let reaped: Vec<u32> = self
            .sources
            .iter()
            .filter(|(_, entry)| entry.closing)
            .map(|(&conn, _)| conn)
            .filter(|&conn| !self.has_flights(conn))
            .collect();
        for conn in &reaped {
            self.sources.remove(conn);
            self.state.slab.lock().unwrap().conn_loads.remove(conn);
        }
    }

    /// One drain-then-exit step of a shutdown: nothing to do while
    /// the flag is clear; once set, the sources close (once) and
    /// their flush resolves what was in flight; when nothing is, the
    /// mailbox dies (after which no command can be pushed — a
    /// submitter fails instead), and the sources drop. Returns
    /// whether the loop is done.
    fn shutdown_step(&mut self) -> bool {
        if !self.state.shutdown.load(Ordering::Acquire) {
            return false;
        }
        if !self.closing_all {
            self.closing_all = true;
            for entry in self.sources.values_mut() {
                if !entry.closing {
                    entry.closing = true;
                    entry.source.close();
                }
            }
        }
        if self.any_flights() {
            return false; // keep sweeping: the flush resolves them
        }
        {
            let mut mailbox = self.state.mailbox.lock().unwrap();
            if !mailbox.queue.is_empty() {
                return false; // more commands: the next loop dispatches them
            }
            mailbox.dead = true;
        }
        // Everything drained: the connections and the completion
        // sources drop here, on the engine thread.
        self.sources.clear();
        self.completions.clear();
        true
    }

    /// The hybrid idle: operations are in flight, but nothing has
    /// completed within the busy window. Arm every completion source,
    /// drain what raced in (an event fires only for a completion
    /// ADDED after its arming — one that landed between the sweep and
    /// the arming would otherwise never wake the block), and block on
    /// the events and the wake channel until something happens.
    fn hybrid_block(&mut self) {
        // Arm first: completions arriving after this fire the event.
        let mut failed: Vec<(usize, io::Error)> = Vec::new();
        for (index, source) in self.completions.iter_mut().enumerate() {
            if let Err(error) = source.arm() {
                failed.push((index, error));
            }
        }
        self.fail_many_completions(failed);
        // Then drain what landed between the sweep and the arming.
        if self.sweep() > 0 {
            return; // progress: back to the busy window
        }
        // A source without an event fd (a fake without a channel)
        // would never wake the block — spin on, conservatively, if
        // any such source is registered.
        if self.completions.iter().any(|source| source.event_fd().is_none()) {
            return;
        }
        let events: Vec<RawFd> = self
            .completions
            .iter()
            .filter_map(|source| source.event_fd())
            .collect();
        self.block_until_wake(&events);
    }

    /// Block on the wake channel (and the completion events of
    /// `events`, one per completion source, in the hybrid idle) until
    /// a submitter (or a shutdown) wakes the engine, or a completion
    /// event fires. The `sleeping` protocol: the flag is stored
    /// before the mailbox is checked, so a command pushed after the
    /// check sees the flag and writes the wake byte; a command pushed
    /// before it left the command in the mailbox for the check to
    /// find.
    fn block_until_wake(&mut self, events: &[RawFd]) {
        self.state.sleeping.store(true, Ordering::Release);
        let has_commands = {
            let mailbox = self.state.mailbox.lock().unwrap();
            !mailbox.queue.is_empty()
        };
        let mut signalled: Vec<usize> = Vec::new();
        if !has_commands {
            let mut fds: Vec<os::PollFd> = std::iter::once(os::PollFd {
                fd: self.wake_read.as_raw_fd(),
                events: os::POLLIN,
                revents: 0,
            })
            .chain(events.iter().map(|&fd| os::PollFd {
                fd,
                events: os::POLLIN,
                revents: 0,
            }))
            .collect();
            // Infinite timeout: shutdown and every submitter write the
            // wake byte, every armed completion fires its event; EINTR
            // retries inside.
            let _ = os::poll_fds(&mut fds, -1);
            if fds[0].revents & os::POLLIN != 0 {
                os::drain(self.wake_read.as_raw_fd());
            }
            for (index, _) in events.iter().enumerate() {
                if fds[index + 1].revents & os::POLLIN != 0 {
                    signalled.push(index);
                }
            }
        }
        self.state.sleeping.store(false, Ordering::Release);
        // The completion events are consumed outside every lock —
        // verbs calls — and a source whose consumption fails is
        // failed like a poll failure. The next sweep drains the
        // completions the events announce.
        // Highest-first: an earlier failure's removal would shift a
        // later index.
        signalled.sort_unstable_by_key(|&index| std::cmp::Reverse(index));
        let mut failed: Vec<(usize, io::Error)> = Vec::new();
        for index in signalled {
            if let Some(error) = self
                .completions
                .get_mut(index)
                .and_then(|source| source.consume_events().err())
            {
                failed.push((index, error));
            }
        }
        self.fail_many_completions(failed);
    }

    /// Whether any operation is in flight anywhere — the busy/block
    /// decision, and the shutdown drain.
    fn any_flights(&self) -> bool {
        self.state.slab.lock().unwrap().any_flights()
    }

    fn has_flights(&self, conn: u32) -> bool {
        self.state.slab.lock().unwrap().has_flights(conn)
    }

    /// Every in-flight operation's packed wr_id, anywhere.
    fn all_flights(&self) -> Vec<u64> {
        self.state.slab.lock().unwrap().all_flights()
    }
}

fn invalid(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, msg.into())
}
