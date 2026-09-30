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
//! - the loop is hybrid: busy within the busy window of the last
//!   progress — a command taken or a completion routed, operations in
//!   flight or not, so a submit-and-wait loop never pays a
//!   block-and-wake cycle — then blocked on its wake pipe when idle
//!   and on the completion-channel events while operations are in
//!   flight.

use std::any::Any;
use std::cell::UnsafeCell;
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::io;
use std::os::fd::{AsRawFd, OwnedFd, RawFd};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU8, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
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
///
/// The registered-buffer half: `register_buffer` registers the
/// memory at `addr`..`addr+len` with the connection's domain, once —
/// a registered buffer the engine's operations then post against
/// directly, through `submit_registered`: no pooled copy, no
/// per-operation registration, no hold — the registration outlives
/// the operation. `deregister_buffer` deregisters it, handed the
/// registered memory's owner (`payload`) to release only once the
/// device is done with it — a deregistration that must wait for
/// in-flight operations keeps the pair until a later attempt frees
/// it (the memory never drops while the device can still touch it).
/// All on the engine thread.
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
    fn register_buffer(&mut self, addr: u64, len: usize) -> io::Result<()>;
    fn deregister_buffer(&mut self, addr: u64, len: usize, payload: Box<dyn Any + Send>);
    fn submit_registered(
        &mut self,
        op: Op,
        addr: u64,
        len: usize,
        target: OpTarget,
        wr_id: u64,
    ) -> io::Result<()>;
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
    /// and polls it — one per (engine, device). The connection's load
    /// rides along: the engine resolves by it, the ticket by which
    /// submissions bound holds it too.
    Register {
        conn: u32,
        load: Arc<ConnLoad>,
        source: Box<dyn OpSource>,
        completions: Option<Box<dyn CompletionSource>>,
    },
    /// Tear a connection down: close it (its outstanding operations
    /// flush as error completions), then drop the source, on the
    /// engine thread.
    Destroy { conn: u32 },
    /// Post one operation. The buffer is already parked in the slot
    /// `wr_id` names — an owned buffer the source registers or pools
    /// (`registered` false), or nothing at all (`registered` true:
    /// the local memory is a registered buffer, posted directly by
    /// [`OpSource::submit_registered`], nothing to hold and nothing
    /// to park — the slot keeps only the wr_id).
    Post {
        wr_id: u64,
        conn: u32,
        op: Op,
        target: OpTarget,
        addr: u64,
        len: usize,
        registered: bool,
    },
    /// Register a buffer with the connection's domain (see
    /// [`OpSource::register_buffer`]) and reply: the registering
    /// thread blocks on `reply` until the engine thread answers, so
    /// the registration is in place before the call returns.
    RegisterBuffer {
        conn: u32,
        addr: u64,
        len: usize,
        reply: mpsc::Sender<io::Result<()>>,
    },
    /// Deregister a registered buffer (see
    /// [`OpSource::deregister_buffer`]), handing its memory's owner
    /// over: the engine thread releases it only once the device is
    /// done with it. Fire and forget — the payload never comes back.
    DeregisterBuffer {
        conn: u32,
        addr: u64,
        len: usize,
        payload: Box<dyn Any + Send>,
    },
}

/// The engine's mailbox: the queued commands, and the flag that ends
/// it. Once dead, nothing can be pushed — a submitter fails instead —
/// which is what makes the final drain of a shutdown complete.
struct Mailbox {
    dead: bool,
    queue: VecDeque<Command>,
}

/// What one submitted operation runs over.
enum Flight {
    /// An owned buffer: parked in the slot from submission to
    /// resolution (dropped handles never race the device), with the
    /// in-flight hold of its local memory from posting to resolution.
    Owned {
        buffer: Box<dyn Any + Send>,
        token: Option<Box<dyn InFlight>>,
    },
    /// A registered buffer: the user's memory, registered once with
    /// the connection's domain — the engine posts against the
    /// registration directly, so nothing is parked and nothing is
    /// held: the registration outlives the operation. The borrow
    /// checker holds the buffer for the operation's future; a dropped
    /// future abandons the operation (the engine completes it anyway)
    /// with the device free to touch the memory until then.
    Registered,
}

/// What a resolved operation resolves with: the owned buffer back
/// (see [`Flight::Owned`]) or nothing (see [`Flight::Registered`] —
/// the user's buffer was theirs all along).
pub(crate) enum Outcome {
    Buffer(Box<dyn Any + Send>),
    Unit,
}

/// One operation's timeline stamps — set only while tracing is on
/// (`Engine::with_op_trace` or `RDMALIB_OP_TRACE=1`), each under the
/// slab lock the stamper's path already holds, so a traced operation
/// pays nothing but the clock reads: no lock, no allocation, no
/// atomic. Incomplete stamps skip the record (a failed dispatch, a
/// resolution nobody waited for); untraced slots keep stale stamps,
/// but only a traced path ever reads them.
#[derive(Clone, Copy, Default)]
struct OpTraceStamps {
    /// The submitter returned from `submit`.
    submit: Option<Instant>,
    /// The engine entered / left the source's post path (the pool
    /// lend, the copy, `ibv_post_send`).
    post_start: Option<Instant>,
    post_end: Option<Instant>,
    /// The engine began resolving the completion.
    complete: Option<Instant>,
}

/// The operation timeline report: what one traced operation's
/// submit-to-resolution cost is made of, segment by segment, printed
/// once at the engine's shutdown. The segments are the four gaps
/// between the five stamps of [`OpTraceStamps`] — the waiter stamps
/// the fifth (`done`, its final poll) and records, so every record is
/// a complete operation:
///
/// - **submit → post**: the mailbox hop, the engine's iteration
///   boundary, the dispatch — the first cross-thread handoff;
/// - **post**: the source's post path — the pool lend and copy, the
///   verbs `ibv_post_send`;
/// - **post → complete**: the wire, the NIC, and the poll that
///   detected the completion — where the perftest-equivalent time
///   lives;
/// - **complete → done**: the resolve, the wake, the waiter's hop and
///   final poll — the second cross-thread handoff.
struct OpTrace {
    samples: Vec<[u128; 4]>,
}

impl OpTrace {
    fn record(&mut self, segments: [u128; 4]) {
        self.samples.push(segments);
    }

    /// The report: count and min/p50/p90/max per segment, ns — the
    /// decomposition of a sequential operation's fixed cost.
    fn report(&self) {
        if self.samples.is_empty() {
            return;
        }
        let percentile = |mut sorted: Vec<u128>, quantile: f64| -> u128 {
            sorted.sort_unstable();
            sorted[(sorted.len() as f64 * quantile) as usize % sorted.len()]
        };
        let column =
            |segment: usize| -> Vec<u128> { self.samples.iter().map(|it| it[segment]).collect() };
        let names = ["submit→post", "post", "post→complete", "complete→done"];
        println!(
            "[op trace] {} operations, segments in ns:",
            self.samples.len()
        );
        for (segment, name) in names.iter().enumerate() {
            let column = column(segment);
            let min = column.iter().copied().min().unwrap();
            let p50 = percentile(column.clone(), 0.5);
            let p90 = percentile(column.clone(), 0.9);
            let max = column.iter().copied().max().unwrap();
            println!(
                "[op trace]   {name:<16} min {min:>9}  p50 {p50:>9}  p90 {p90:>9}  max {max:>9}"
            );
        }
        let totals: Vec<u128> = self.samples.iter().map(|it| it.iter().sum()).collect();
        println!(
            "[op trace]   {:<16} min {:>9}  p50 {:>9}  p90 {:>9}  max {:>9}",
            "total",
            totals.iter().copied().min().unwrap(),
            percentile(totals.clone(), 0.5),
            percentile(totals.clone(), 0.9),
            totals.iter().copied().max().unwrap(),
        );
    }
}

/// The four segments of one operation's timeline, `None` unless all
/// five stamps are present.
fn trace_segments(stamps: &OpTraceStamps, done: Instant) -> Option<[u128; 4]> {
    Some([
        stamps.post_start?.saturating_duration_since(stamps.submit?),
        stamps
            .post_end?
            .saturating_duration_since(stamps.post_start?),
        stamps.complete?.saturating_duration_since(stamps.post_end?),
        done.saturating_duration_since(stamps.complete?),
    ])
    .map(|[a, b, c, d]| [a.as_nanos(), b.as_nanos(), c.as_nanos(), d.as_nanos()])
}

/// A slot's life, one phase at a time. The phases carry the
/// ownership of the slot's plain fields (below): every transition but
/// the waiter's and the abandoning dropper's runs on the engine
/// thread, and each one releases the fields the next phase's owner
/// reads — the whole lock-free protocol hangs off this word.
///
/// - `Empty` — on the free list. Owned by the list; the popping
///   submitter takes ownership with the pop. Set by the freeing
///   thread before the slot is pushed back.
/// - `Submitted` — parked and queued. The submitter wrote the
///   payload and handed the slot over (release) — through the
///   mailbox's lock, for the engine, or the returned handle, for the
///   waiter — so the phase needs no handoff of its own: the engine
///   parks the post's hold into the flight here, and the resolve
///   stores the outcome before leaving it.
/// - `Waiting` — a handle polled and registered a waker (under the
///   slot's waker lock). The engine takes the waker at resolve.
/// - `Resolved` — the engine stored the outcome and set the done
///   flag, and expects the claiming handle to take the outcome and
///   free the slot.
/// - `Abandoned` — the handle dropped before resolution; the engine
///   finishes the hold, drops the outcome, and frees the slot.
const PHASE_EMPTY: u8 = 0;
const PHASE_SUBMITTED: u8 = 1;
const PHASE_WAITING: u8 = 2;
const PHASE_RESOLVED: u8 = 3;
const PHASE_ABANDONED: u8 = 4;

/// One operation's state. Addressed by slot index and generation —
/// packed as the wr_id the completion is routed by.
///
/// The atomic word is the lock: `generation` (bumped every time the
/// slot is freed, so a wr_id from a previous reuse never matches)
/// and `phase` (the slot's life, above) govern every plain field in
/// `data`, and `done` is the waiter's lockless fast-path view of
/// "resolved". Everything else — the payload the phases own — sits
/// behind one `UnsafeCell`, written only by the phase's owner, so
/// no lock guards it: the owner hands it over with the phase
/// transition's release, the next owner reads it after the matching
/// acquire. That argument is why `Sync` is asserted below.
struct Slot {
    /// Bumped every time the slot is freed, so a wr_id from a
    /// previous reuse never matches.
    generation: AtomicU32,
    /// The slot's life — see the phase constants above.
    phase: AtomicU8,
    /// The waker field's spin lock: storing a waker (a waiter) and
    /// taking one (the engine at resolve, the abandoning dropper)
    /// serialize through it, so a re-poll's waker replace cannot
    /// race the engine's take.
    waker_lock: AtomicBool,
    /// While `Empty`: the next slot's index on the free list. The
    /// generation the head's tag carries rides in the slot's
    /// `generation`, stable while the slot sits on the list.
    free_next: AtomicU32,
    /// Set (released) once the outcome is stored, cleared by the
    /// next life's submitter — the handle's lockless fast-path view
    /// of "done": a spinning `wait` loads this without any lock, and
    /// takes the one final poll only once it sees `true`. An `Arc`,
    /// not a plain field: the handle must reach the flag without
    /// indexing the slot table, which a concurrent chunk allocation
    /// can extend — while the `Arc`'s allocation is stable for as
    /// long as anyone holds it. One allocation per *slot*, not per
    /// operation: reused slots keep theirs.
    done: Arc<AtomicBool>,
    /// Everything the phases own, mutated through shared references
    /// by the protocol above.
    data: UnsafeCell<SlotData>,
}

/// The slot's payload — the fields the phase protocol owns. Which
/// thread may touch which field when, as a table:
///
/// - `conn`, `flight`, `trace.submit`: written by the submitter
///   between its pop and its phase-`Submitted` store (sole access —
///   a popped slot is exclusively the submitter's); read by the
///   engine after the mailbox's handoff, or (for the scans) after an
///   acquire load of a live phase.
/// - `flight`'s parked hold, `trace`'s post stamps, `trace.complete`:
///   written by the engine thread alone, between seeing the command
///   and resolving.
/// - `resolved` (with `done`, above): written by the engine before
///   its phase-`Resolved` transition; read by the claiming handle
///   after the phase (or the flag) says so, exclusively — the
///   claiming handle is one thread, and it also frees the slot.
/// - `waker`: written under `waker_lock`; taken under it (by the
///   engine at resolve, by the abandoning dropper of a waiting
///   slot) or from exclusive phase ownership.
struct SlotData {
    conn: u32,
    /// While the operation is in flight — including the window
    /// between submission and the engine's dispatch (the buffer is
    /// parked here before the command is even queued).
    flight: Option<Flight>,
    /// Set by the engine when the completion is processed; taken by
    /// the handle. [`Outcome::Buffer`] for an owned buffer's
    /// operation, [`Outcome::Unit`] for a registered buffer's.
    resolved: Option<io::Result<Outcome>>,
    /// The awaiting handle's waker, if it polled while pending.
    waker: Option<Waker>,
    /// The operation's timeline stamps (see [`OpTraceStamps`]):
    /// written only on traced paths, reset by the next life's
    /// submitter only while tracing is on — an untraced operation
    /// never touches them.
    trace: OpTraceStamps,
}

// SAFETY: `Slot` is shared as `&Slab` across the submitting threads,
// the engine thread, and the handles' threads, and it mutates its
// plain fields through that shared reference — the phase protocol is
// the lock that makes it sound, as the table on `SlotData` spells
// out: every field has exactly one owner at a time (the submitter
// between pop and the `Submitted` release, the engine between the
// handoff and the `Resolved`/`Abandoned` transition, the claiming
// handle at `Resolved`, the free list at `Empty`), ownership moves
// only through release/acquire transitions of `phase` or the
// generation-tagged free list's CAS chain, and the waker field
// serializes through `waker_lock`. The atomics are always accessed
// atomically; the `done` `Arc`'s pointer is written once at the
// chunk's construction, before the chunk is visible to anyone.
unsafe impl Sync for Slot {}

impl Slot {
    fn new() -> Self {
        Self {
            generation: AtomicU32::new(0),
            phase: AtomicU8::new(PHASE_EMPTY),
            waker_lock: AtomicBool::new(false),
            free_next: AtomicU32::new(NIL_INDEX),
            done: Arc::new(AtomicBool::new(false)),
            data: UnsafeCell::new(SlotData {
                conn: 0,
                flight: None,
                resolved: None,
                waker: None,
                trace: OpTraceStamps::default(),
            }),
        }
    }

    /// The payload, exclusive by the phase protocol — see the table
    /// on `SlotData` for whose it is when.
    #[inline(always)]
    fn data(&self) -> &SlotData {
        // SAFETY: the caller owns the slot's current phase, per the
        // table on `SlotData` — the protocol guarantees no other
        // thread touches the payload concurrently.
        unsafe { &*self.data.get() }
    }

    /// The payload, mutably — same protocol as [`Self::data`]:
    /// through the shared reference the `UnsafeCell` below is the
    /// one legal way (the phase protocol is the lock; see the
    /// safety argument on `unsafe impl Sync for Slot`).
    #[allow(clippy::mut_from_ref)] // the UnsafeCell handoff is the point
    #[inline(always)]
    fn data_mut(&self) -> &mut SlotData {
        // SAFETY: as `Self::data`: the caller owns the phase, and no
        // other thread touches the payload concurrently.
        unsafe { &mut *self.data.get() }
    }
}

/// One registered connection's load: its send-queue depth — the
/// operations that may be in flight on it at once — how many are in
/// flight right now, and whether the connection is still alive. The
/// submission bound runs on it: a submit past the depth fails on the
/// spot, eagerly, instead of a post failing on the engine thread
/// when the device's queue is full (the `ibv_post_send` `ENOMEM` of
/// a send queue out of work requests). Shared through the
/// connection's ticket ([`ConnRef`]) by every submitting thread and
/// by the engine's resolves — all through atomics, no lock.
pub(crate) struct ConnLoad {
    depth: u32,
    in_flight: AtomicU32,
    live: AtomicBool,
}

impl ConnLoad {
    /// The send-queue bound of a connection of the given depth.
    fn new(depth: u32) -> Arc<Self> {
        Arc::new(Self {
            depth,
            in_flight: AtomicU32::new(0),
            live: AtomicBool::new(true),
        })
    }
}

/// A registered connection's engine identity — the ticket every
/// submission bounds by and every command rides with: the id, and
/// the load it shares with the engine. Handed out once by
/// [`Engine::register`], held by the connection's owner (the
/// reader side's session), so the submission bound is two atomic
/// loads and a fetch — no map, no lock, on the operation's path.
/// (Cheap to clone: the id and one `Arc` reference.)
#[derive(Clone)]
pub(crate) struct ConnRef {
    pub(crate) id: u32,
    load: Arc<ConnLoad>,
}

impl std::fmt::Debug for ConnRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The id is enough to tell connections apart.
        f.debug_struct("ConnRef").field("id", &self.id).finish()
    }
}

impl ConnRef {
    /// The send-queue bound the ticket's connection runs under.
    pub(crate) fn depth(&self) -> u32 {
        self.load.depth
    }

    /// Whether the ticket's connection is still registered with the
    /// engine — false once its destroy landed (or the engine's
    /// teardown closed it).
    pub(crate) fn is_live(&self) -> bool {
        self.load.live.load(Ordering::Acquire)
    }

    /// A ticket no engine ever handed out — the id fails every
    /// submission bound as "never registered". For the eager-check
    /// tests.
    #[cfg(test)]
    pub(crate) fn unknown(id: u32) -> Self {
        Self {
            id,
            load: ConnLoad::new(16),
        }
    }
}

/// The NIL of the free list's packed head: no index, no generation.
const NIL_HEAD: u64 = u64::MAX;
/// The NIL of a free-list node's next pointer (no next slot).
const NIL_INDEX: u32 = u32::MAX;

/// Slots per chunk of the slot table.
const CHUNK_SLOTS: usize = 512;
/// Slot-table chunks — the slot ceiling, 32 768 slots. Every
/// in-flight operation holds exactly one slot, and the eager
/// send-queue bound caps those at the connections' depths, so the
/// ceiling is a hoard's allowance: slots awaiting a claiming handle
/// past the depth-bounded in-flight ones.
const CHUNKS: usize = 64;

/// The engine's operation slots: a fixed table of append-only
/// chunks — the table never reallocates, so every access through a
/// shared reference is sound while a concurrent submitter extends
/// it — addressed by slot index, and a generation-tagged Treiber
/// free list threaded through the `Empty` slots. The tags make the
/// list ABA-safe: a head tag is `(index, generation)`, the
/// generation bumps with every free, and a slot can sit second in
/// line only while the tagged head above it stands — so a pop's
/// read of its next cannot go stale while the CAS succeeds.
///
/// The allocation packs `(index << 32) | generation` into the wr_id
/// the completion is routed by; the phase word on each slot carries
/// the lock-free protocol (see the phase constants above).
struct Slab {
    /// Append-only: entries only ever go null → claimed, in order
    /// (a claimer takes the first null), and a chunk is visible only
    /// once spliced — an acquire load of a non-null pointer pairs
    /// the claimer's release CAS.
    chunks: [AtomicPtr<[Slot; CHUNK_SLOTS]>; CHUNKS],
    /// The free list's packed head: `(index << 32) | generation`.
    free_head: AtomicU64,
}

impl Slab {
    fn new() -> Self {
        Self {
            chunks: std::array::from_fn(|_| AtomicPtr::new(std::ptr::null_mut())),
            free_head: AtomicU64::new(NIL_HEAD),
        }
    }

    /// The slot a global index names — every access goes through the
    /// chunk table, whose entries, once visible, never move.
    #[inline(always)]
    fn slot_at(&self, index: u32) -> Option<&Slot> {
        let chunk = self
            .chunks
            .get((index as usize) / CHUNK_SLOTS)?
            .load(Ordering::Acquire);
        if chunk.is_null() {
            return None;
        }
        // SAFETY: the pointer is a live chunk — non-null entries are
        // released only at the engine's death, after every handle
        // and every engine reference has dropped — and the index is
        // within the chunk by the division above.
        let slots: &[Slot; CHUNK_SLOTS] = unsafe { &*chunk };
        Some(&slots[(index as usize) % CHUNK_SLOTS])
    }

    /// Pop a slot off the free list: `(index, generation)` — the
    /// popper owns the slot's payload exclusively until it releases
    /// the phase. `None` when the list ran dry: the caller grows the
    /// table (or fails past its ceiling).
    fn pop(&self) -> Option<(u32, u32)> {
        let mut head = self.free_head.load(Ordering::Acquire);
        loop {
            if head == NIL_HEAD {
                return None;
            }
            let index = (head >> 32) as u32;
            let generation = head as u32;
            let Some(slot) = self.slot_at(index) else {
                return None; // unreachable: a head tag is a live slot
            };
            // The popped slot's next: stable while the tagged head
            // stands (see the struct's ABA argument).
            let next = slot.free_next.load(Ordering::Acquire);
            let next_head = if next == NIL_INDEX {
                NIL_HEAD
            } else {
                // The next node's generation, stable while it sits
                // on the list — it bumps only at a free, and a slot
                // on the list is not free-able.
                let next_gen = self
                    .slot_at(next)
                    .expect("a free-list node names a live slot")
                    .generation
                    .load(Ordering::Acquire);
                (next as u64) << 32 | next_gen as u64
            };
            match self.free_head.compare_exchange(
                head,
                next_head,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some((index, generation)),
                Err(current) => head = current, // a racing pop or push: retry
            }
        }
    }

    /// Push a freed slot — generation already bumped, phase already
    /// `Empty` — onto the free list.
    fn push(&self, index: u32, generation: u32) {
        let mut head = self.free_head.load(Ordering::Acquire);
        loop {
            self.slot_at(index)
                .expect("a freed slot is a live slot")
                .free_next
                .store(
                    if head == NIL_HEAD {
                        NIL_INDEX
                    } else {
                        (head >> 32) as u32
                    },
                    Ordering::Release,
                );
            match self.free_head.compare_exchange(
                head,
                (index as u64) << 32 | generation as u64,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(current) => head = current, // a racing pop or push: retry
            }
        }
    }

    /// Recycle a slot its owner is done with — the generation bumps
    /// (stale wr_ids stop matching), the phase returns to `Empty`,
    /// and the slot re-enters the free list. The caller owns the
    /// slot's phase exclusively (a claiming handle at `Resolved`,
    /// the engine at `Abandoned`).
    fn recycle(&self, slot: &Slot, index: u32) {
        let generation = slot
            .generation
            .fetch_add(1, Ordering::AcqRel)
            .wrapping_add(1);
        slot.phase.store(PHASE_EMPTY, Ordering::Release);
        self.push(index, generation);
    }

    /// Grow the slot table by one chunk: claim the first open entry
    /// (the losers of the claim race drop their chunk — it was
    /// never visible), build the slots, and splice them onto the
    /// free list as one chain. The caller retries its pop after.
    /// `None` at the table's ceiling.
    fn grow(&self) -> bool {
        // The first open entry.
        let mut position = None;
        for (candidate, entry) in self.chunks.iter().enumerate() {
            if entry.load(Ordering::Acquire).is_null() {
                position = Some(candidate);
                break;
            }
        }
        let Some(position) = position else {
            return false; // the ceiling: every chunk is claimed
        };
        let chunk: Box<[Slot; CHUNK_SLOTS]> = Box::new(std::array::from_fn(|_| Slot::new()));
        let raw = Box::into_raw(chunk);
        if self.chunks[position]
            .compare_exchange(
                std::ptr::null_mut(),
                raw,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            // A racing grower claimed this entry first: our chunk was
            // never visible to anyone — take it back and let the
            // caller's retry find theirs.
            // SAFETY: the CAS above failed, so `raw` never escaped —
            // nobody else can hold it.
            unsafe { drop(Box::from_raw(raw)) };
            return true;
        }
        // Splice: the chunk's slots, chained onto the current head,
        // published by one CAS. The slots' generations are 0 — the
        // slots are fresh — and their phases are `Empty` from
        // construction.
        let base = (position * CHUNK_SLOTS) as u32;
        let first = base;
        // SAFETY: `raw` was just published at `position` by the
        // successful CAS above — it is a live chunk, and it is the
        // one `base` indexes into.
        let slots: &[Slot; CHUNK_SLOTS] = unsafe { &*raw };
        let mut head = self.free_head.load(Ordering::Acquire);
        loop {
            slots[CHUNK_SLOTS - 1].free_next.store(
                if head == NIL_HEAD {
                    NIL_INDEX
                } else {
                    (head >> 32) as u32
                },
                Ordering::Release,
            );
            for (chained, slot) in slots.iter().enumerate().take(CHUNK_SLOTS - 1) {
                slot.free_next
                    .store(base + chained as u32 + 1, Ordering::Release);
            }
            match self.free_head.compare_exchange(
                head,
                (first as u64) << 32, // generation 0: fresh slots
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(current) => head = current, // a racing pop or push: retry
            }
        }
    }

    /// Pop a slot, growing the table once if the list ran dry — the
    /// submitter's allocation. `None` at the table's ceiling (a
    /// hoard of outcomes nobody claims — resolve or await some).
    fn acquire(&self) -> Option<(u32, u32)> {
        let popped = self.pop();
        if popped.is_some() {
            return popped;
        }
        if !self.grow() {
            return None;
        }
        self.pop()
    }

    /// Whether any operation is in flight anywhere — a live phase
    /// that is not yet resolved. The engine thread's view, after its
    /// mailbox drain: everything queued is visible.
    fn any_flights(&self) -> bool {
        self.flights(None)
    }

    /// Whether `conn` has an operation in flight.
    fn has_flights(&self, conn: u32) -> bool {
        self.flights(Some(conn))
    }

    /// Every in-flight operation's packed wr_id — the engine's
    /// everything-must-resolve sweeps (a failed completion source, a
    /// shutdown).
    fn all_flights(&self) -> Vec<u64> {
        let mut flights = Vec::new();
        self.scan(|slot, index| {
            if matches!(
                slot.phase.load(Ordering::Acquire),
                PHASE_SUBMITTED | PHASE_WAITING | PHASE_ABANDONED
            ) {
                flights.push((index as u64) << 32 | slot.generation.load(Ordering::Acquire) as u64);
            }
        });
        flights
    }

    /// The shared scan: is any operation in flight — anywhere, or
    /// on `conn`. The payload read runs only for live-phase slots,
    /// whose payloads a phase release published and this acquire
    /// pairs (an `Empty` slot's payload belongs to its popper, and is
    /// never touched here).
    fn flights(&self, conn: Option<u32>) -> bool {
        let mut found = false;
        self.scan(|slot, _| {
            if found {
                return;
            }
            if !matches!(
                slot.phase.load(Ordering::Acquire),
                PHASE_SUBMITTED | PHASE_WAITING | PHASE_ABANDONED
            ) {
                return; // not in flight: the payload is not ours to read
            }
            if conn.is_some_and(|conn| slot.data().conn != conn) {
                return;
            }
            found = true;
        });
        found
    }

    /// Walk every live chunk's slots, engine-side.
    fn scan(&self, mut visit: impl FnMut(&Slot, u32)) {
        for (chunk_index, entry) in self.chunks.iter().enumerate() {
            let chunk = entry.load(Ordering::Acquire);
            if chunk.is_null() {
                break; // chunks claim in order: past the first null, none
            }
            // SAFETY: a non-null entry is a live chunk (see
            // `slot_at`).
            let slots: &[Slot; CHUNK_SLOTS] = unsafe { &*chunk };
            for (in_chunk, slot) in slots.iter().enumerate() {
                visit(slot, (chunk_index * CHUNK_SLOTS + in_chunk) as u32);
            }
        }
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
    /// Whether the mailbox holds commands — set under its lock by
    /// every push, cleared under it by the drain that takes them —
    /// so the engine's spin can check the mailbox with one load,
    /// without the lock: a taken-command push necessarily ran first
    /// (and set it again), so the invariant "pending or empty" holds
    /// at every lock release and no command is ever missed by
    /// trusting the fast check.
    pending: AtomicBool,
    mailbox: Mutex<Mailbox>,
    /// The operation slots — lock-free (see [`Slab`]): the phase
    /// protocol and the generation-tagged free list are the locks,
    /// so the sequential operation's submit, dispatch, resolve, and
    /// take touch no mutex at all.
    slab: Slab,
    wake_write: OwnedFd,
    /// The busy window since the last progress — a command taken or a
    /// completion routed, operations in flight or not — before the
    /// engine blocks (on the completion events while operations are
    /// in flight, on the wake channel when idle); the knob of
    /// [`Engine::with_busy_window`]. A mutex, not a plain field:
    /// the builder may turn it while the engine runs.
    busy_window: Mutex<Duration>,
    /// Whether the operation timeline is on (`RDMALIB_OP_TRACE=1` or
    /// [`Engine::with_op_trace`]) — every traced path checks this
    /// first, so an untraced operation pays nothing: no stamp, no
    /// record. Relaxed: it is set before operations run, and a
    /// racing turn-on simply starts stamping mid-stream.
    op_trace: AtomicBool,
    /// The collected operation timelines — present always (one mutex
    /// and one empty `Vec` per engine), touched only when
    /// [`Self::op_trace`] is on: the stamps ride the slab lock, and
    /// the record takes this mutex alone (never nested with the
    /// slab's).
    trace: Mutex<OpTrace>,
}

impl Drop for EngineShared {
    fn drop(&mut self) {
        // The slot table's chunks, freed once nobody holds the state
        // — no handle, no engine, no in-flight operation — so no
        // slot is live and every slot is droppable as it stands.
        for chunk in &mut self.slab.chunks {
            let raw = *chunk.get_mut();
            if !raw.is_null() {
                // SAFETY: every non-null entry was created by
                // `Slab::grow`'s `Box::into_raw` and published once —
                // and this drop runs only when the last `Arc` to
                // the state is gone, after which no access of any
                // kind can follow.
                unsafe { drop(Box::from_raw(raw)) };
            }
        }
    }
}

/// How long a default engine busy-polls after the last progress — a
/// command taken or a completion routed, operations in flight or not
/// — before it blocks: on the completion events (armed first) while
/// operations are in flight, on the wake channel when idle. Sized to
/// cover a round trip and the gap between sequential operations, so
/// neither a short operation nor a submit-and-wait loop ever pays a
/// block-and-wake cycle.
const DEFAULT_BUSY_WINDOW: Duration = Duration::from_micros(256);

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
        // The engine thread is joined, so the timeline is complete —
        // the report of what the traced operations' cost was made of.
        if self.state.op_trace.load(Ordering::Relaxed) {
            self.state.trace.lock().unwrap().report();
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
    /// schedules it — busy-polling within the busy window of the last
    /// progress, blocked on its wake channel when idle.
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

    /// Set the busy window: how long the engine busy-polls after the
    /// last progress — a command taken or a completion routed,
    /// operations in flight or not — before it blocks: on the
    /// completion events (armed first, the hybrid idle) while
    /// operations are in flight, on the wake channel when idle.
    ///
    /// `Duration::ZERO` blocks as soon as a sweep finds nothing;
    /// `Duration::MAX` never blocks at all — pure busy-polling, the
    /// dedicated-core case. The default is sized to cover a round
    /// trip and the gap between sequential operations, so a
    /// submit-and-wait loop never pays a block-and-wake cycle.
    pub fn with_busy_window(self, window: Duration) -> Self {
        *self.control.state.busy_window.lock().unwrap() = window;
        self
    }

    /// Turn the operation timeline on or off — what a traced
    /// operation's cost is made of, segment by segment, printed at
    /// the engine's shutdown (the default comes from
    /// `RDMALIB_OP_TRACE=1` in the environment). Set before the
    /// operations whose timeline matters: a racing turn-on simply
    /// starts stamping mid-stream, and only complete five-stamp
    /// operations record.
    pub fn with_op_trace(self, on: bool) -> Self {
        self.control.state.op_trace.store(on, Ordering::Relaxed);
        self
    }

    /// The collected operation timelines, one `[submit→post, post,
    /// post→complete, complete→done]` (ns) per traced operation —
    /// what the shutdown report prints, for tests and tools.
    pub fn op_trace_samples(&self) -> Vec<[u128; 4]> {
        self.control.state.trace.lock().unwrap().samples.clone()
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
            pending: AtomicBool::new(false),
            mailbox: Mutex::new(Mailbox {
                dead: false,
                queue: VecDeque::new(),
            }),
            slab: Slab::new(),
            wake_write,
            busy_window: Mutex::new(DEFAULT_BUSY_WINDOW),
            op_trace: AtomicBool::new(std::env::var("RDMALIB_OP_TRACE").as_deref() == Ok("1")),
            trace: Mutex::new(OpTrace {
                samples: Vec::new(),
            }),
        });
        let control = Arc::new(EngineControl {
            state: Arc::clone(&state),
            thread: Mutex::new(None),
        });
        let core = EngineCore {
            state,
            wake_read,
            sources: HashMap::new(),
            loads: HashMap::new(),
            completions: Vec::new(),
            cursor: 0,
            closing_all: false,
            batch: std::array::from_fn(|_| Completion {
                wr_id: 0,
                ok: false,
                error: String::new(),
            }),
            scratch: VecDeque::new(),
        };
        let handle = thread::spawn(move || core.run());
        *control.thread.lock().unwrap() = Some(handle);
        Self { control }
    }

    /// Register a connection with the engine: from here the engine
    /// thread owns it — every verbs call on its operation path runs
    /// there. Returns the connection's ticket — the id its
    /// operations ride with and the load every submission bounds
    /// against (see [`ConnRef`]).
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
    ) -> io::Result<ConnRef> {
        // The connection's send-queue depth, read here on the
        // registering thread before the source moves to the engine:
        // the submission bound needs it before the engine could have
        // dispatched the registration.
        let depth = source.depth() as u32;
        let conn = self.control.state.next_conn.fetch_add(1, Ordering::Relaxed);
        // The load rides the command and the ticket both: the engine
        // resolves by it, and every submission bounds by it — no
        // registry, no lock, on any operation's path.
        let load = ConnLoad::new(depth);
        self.push(Command::Register {
            conn,
            load: Arc::clone(&load),
            source,
            completions,
        })?;
        // The ticket hands the bound to the caller: the caller's very
        // first submission on it already finds the bound in place —
        // the load is alive here, before the id ever returns.
        Ok(ConnRef { id: conn, load })
    }

    /// Tear a registered connection down: close it — its outstanding
    /// operations flush as error completions, resolving their handles
    /// — then drop it, on the engine thread. The ticket's
    /// submissions fail from the engine's teardown on: the load's
    /// `live` flag clears with the destroy, and anything queued
    /// before it fails at dispatch, behind it in the mailbox.
    pub(crate) fn destroy(&self, conn: &ConnRef) -> io::Result<()> {
        self.push(Command::Destroy { conn: conn.id })
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
        conn: &ConnRef,
        op: Op,
        target: OpTarget,
        submitted: Submitted,
    ) -> io::Result<OpHandle> {
        if submitted.len == 0 {
            return Err(invalid(format!("cannot {} zero bytes", op.name())));
        }
        let (wr_id, done) = self.park(
            conn,
            Flight::Owned {
                buffer: submitted.buffer,
                token: None,
            },
        )?;
        self.post(
            conn,
            op,
            target,
            submitted.addr,
            submitted.len,
            false,
            wr_id,
            done,
        )
    }

    /// Post one operation on a registered buffer — the user's memory,
    /// registered once with the connection's domain (see
    /// [`Self::register_buffer`]): the engine posts against the
    /// registration directly, so nothing is parked (the slot keeps
    /// only the wr_id) and nothing is held — no allocation, no copy,
    /// no pool slice on the operation's path. The caller keeps the
    /// buffer borrowed for the returned future's lifetime; dropping
    /// the future abandons the operation, the device free to touch
    /// the memory until the completion.
    pub(crate) fn submit_registered(
        &self,
        conn: &ConnRef,
        op: Op,
        target: OpTarget,
        addr: u64,
        len: usize,
    ) -> io::Result<OpHandle> {
        if len == 0 {
            return Err(invalid(format!("cannot {} zero bytes", op.name())));
        }
        let (wr_id, done) = self.park(conn, Flight::Registered)?;
        self.post(conn, op, target, addr, len, true, wr_id, done)
    }

    /// The common submit half — lockless: the connection checks run
    /// on the ticket (the id and the load it carries — two atomic
    /// loads and the in-flight fetch), and the slot allocation pops
    /// the generation-tagged free list (see [`Slab`]). The popped
    /// slot is exclusively the submitter's until the phase release:
    /// the payload is written first, the phase published last.
    fn park(&self, conn: &ConnRef, flight: Flight) -> io::Result<(u64, Arc<AtomicBool>)> {
        let state = &self.control.state;
        if conn.id >= state.next_conn.load(Ordering::Relaxed) {
            return Err(invalid(format!(
                "connection {} was never registered with the engine",
                conn.id
            )));
        }
        if !conn.load.live.load(Ordering::Acquire) {
            return Err(invalid(format!(
                "connection {} is gone (destroyed, and its operations flushed)",
                conn.id
            )));
        }
        // The eager send-queue bound, atomic: every submit that
        // stays saw a count below the depth before its own
        // increment, so at most `depth` operations stay — the old
        // C++ library's `Exceeded rdma completion queue size`
        // guard, at the async seam. A transient overshoot (a racing
        // submit past the depth) reverts itself on the spot.
        let old = conn.load.in_flight.fetch_add(1, Ordering::AcqRel);
        if old >= conn.load.depth {
            conn.load.in_flight.fetch_sub(1, Ordering::AcqRel);
            return Err(invalid(format!(
                "connection {}'s send queue is full: {} operations in flight, its max_send_wr is {} — await in-flight operations before submitting more",
                conn.id,
                old,
                conn.depth()
            )));
        }
        // The slot: popped off the free list — exclusively ours
        // until the phase release below.
        let Some((index, generation)) = state.slab.acquire() else {
            conn.load.in_flight.fetch_sub(1, Ordering::AcqRel);
            return Err(invalid(format!(
                "the engine's operation slots are exhausted ({} slots) — resolve or await operations before hoarding more",
                CHUNKS * CHUNK_SLOTS
            )));
        };
        let slot = state
            .slab
            .slot_at(index)
            .expect("a popped slot index is a live slot");
        let traced = state.op_trace.load(Ordering::Relaxed);
        {
            // The payload, written under exclusive pop ownership.
            let data = slot.data_mut();
            data.conn = conn.id;
            data.flight = Some(flight);
            data.resolved = None; // the previous life's taker left it empty
            data.waker = None; // likewise
            if traced {
                // The timeline's first stamp — the operation is
                // about to be parked and queued.
                data.trace = OpTraceStamps::default();
                data.trace.submit = Some(Instant::now());
            }
        }
        // The previous life of this slot set `done` at its
        // resolution — clear it for this one, before the phase
        // release publishes everything above.
        slot.done.store(false, Ordering::Release);
        // Published: the phase release hands the payload to whoever
        // sees the command (the engine, through the mailbox's lock)
        // or the handle (the waiter, through the returned handle).
        slot.phase.store(PHASE_SUBMITTED, Ordering::Release);
        let wr_id = (index as u64) << 32 | generation as u64;
        // The handle's fast-path flag, cloned while the slot is
        // still exclusively ours: one reference count, no
        // allocation.
        let done = Arc::clone(&slot.done);
        Ok((wr_id, done))
    }

    /// The common submit tail: queue the post command and hand back
    /// the handle. A queue failure (the engine is shut down) gives
    /// the load's in-flight count and the slot back, on the
    /// submitting thread — the command never left, so the slot
    /// never became visible to the engine, and it is still
    /// exclusively ours to recycle.
    #[allow(clippy::too_many_arguments)] // the submit it factors is this wide
    fn post(
        &self,
        conn: &ConnRef,
        op: Op,
        target: OpTarget,
        addr: u64,
        len: usize,
        registered: bool,
        wr_id: u64,
        done: Arc<AtomicBool>,
    ) -> io::Result<OpHandle> {
        let state = &self.control.state;
        if let Err(error) = self.push(Command::Post {
            wr_id,
            conn: conn.id,
            op,
            target,
            addr,
            len,
            registered,
        }) {
            let index = (wr_id >> 32) as u32;
            let slot = state
                .slab
                .slot_at(index)
                .expect("a parked slot index is a live slot");
            drop(slot.data_mut().flight.take());
            conn.load.in_flight.fetch_sub(1, Ordering::AcqRel);
            state.slab.recycle(slot, index);
            return Err(error);
        }
        Ok(OpHandle {
            state: Arc::clone(state),
            slot: wr_id,
            done,
        })
    }

    /// Register the memory at `addr`..`addr+len` with `conn`'s
    /// domain — a registered buffer the engine's operations post
    /// against directly (see [`Self::submit_registered`]). The call
    /// blocks until the engine thread answers, so the registration is
    /// in place before it returns.
    pub(crate) fn register_buffer(&self, conn: &ConnRef, addr: u64, len: usize) -> io::Result<()> {
        let (reply, done) = mpsc::channel();
        self.push(Command::RegisterBuffer {
            conn: conn.id,
            addr,
            len,
            reply,
        })?;
        done.recv().unwrap_or_else(|_| {
            Err(io::Error::other(
                "the engine dropped the registration reply before answering",
            ))
        })
    }

    /// Deregister a registered buffer, handing its memory's owner
    /// (`payload`) to the engine: it is released only once the device
    /// is done with it — fire and forget, the payload never comes
    /// back.
    pub(crate) fn deregister_buffer(
        &self,
        conn: &ConnRef,
        addr: u64,
        len: usize,
        payload: Box<dyn Any + Send>,
    ) -> io::Result<()> {
        self.push(Command::DeregisterBuffer {
            conn: conn.id,
            addr,
            len,
            payload,
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
        // Set under the lock the engine's drain also holds: the
        // drain takes the commands and clears the flag in one
        // critical section, so a push racing the drain either lands
        // in the taken batch or sets the flag after it was cleared
        // — either way the engine sees it.
        state.pending.store(true, Ordering::Release);
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
/// safely: the engine completes it anyway and frees the parked
/// buffer (a registered-buffer operation parks nothing — its
/// memory is the user's, and the device may touch it until the
/// completion).
pub(crate) struct OpHandle {
    state: Arc<EngineShared>,
    slot: u64,
    /// The slot's lockless done flag (see [`Slot::done`]): cloned at
    /// submission, so `wait`'s spin observes the resolution without
    /// taking the slab lock — one reference count per operation,
    /// against a mutex pair and a waker clone per *iteration* the
    /// lockless spin replaces.
    done: Arc<AtomicBool>,
}

impl std::fmt::Debug for OpHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The packed slot id is enough to tell operations apart.
        f.debug_struct("OpHandle")
            .field("slot", &self.slot)
            .finish()
    }
}

impl Future for OpHandle {
    type Output = io::Result<Outcome>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let index = (self.slot >> 32) as u32;
        let generation = self.slot as u32;
        let Some(slot) = self.state.slab.slot_at(index) else {
            // Unreachable by construction (a live handle holds its slot
            // until it resolves); resolved as an error, not a panic —
            // futures must not panic.
            return Poll::Ready(Err(io::Error::other(
                "the operation's slot is gone — an engine bug",
            )));
        };
        if slot.generation.load(Ordering::Acquire) != generation {
            // Likewise.
            return Poll::Ready(Err(io::Error::other(
                "the operation's slot is gone — an engine bug",
            )));
        }
        let engine_bug = || {
            Poll::Ready(Err(io::Error::other(
                "the operation's slot is gone — an engine bug",
            )))
        };
        loop {
            let phase = slot.phase.load(Ordering::Acquire);
            match phase {
                PHASE_RESOLVED => {
                    // The claiming handle is exclusively the
                    // outcome's now (the engine is done with the
                    // slot at this phase): take it, with the
                    // timeline's stamps traveling out, and recycle
                    // the slot.
                    let resolved = slot.data_mut().resolved.take().unwrap_or_else(|| {
                        Err(io::Error::other(
                            "a resolved slot holds no outcome — an engine bug",
                        ))
                    });
                    let stamps = slot.data().trace;
                    self.state.slab.recycle(slot, index);
                    // The timeline's fifth stamp (this poll is the
                    // waiter's `done`) and its record — outside every
                    // lock, so the trace's mutex never nests under
                    // another. An incomplete timeline (a failed
                    // dispatch, nobody waiting) records nothing.
                    if self.state.op_trace.load(Ordering::Relaxed)
                        && let Some(segments) = trace_segments(&stamps, Instant::now())
                    {
                        self.state.trace.lock().unwrap().record(segments);
                    }
                    return Poll::Ready(resolved);
                }
                PHASE_SUBMITTED => {
                    // Store the waker (under the slot's waker lock —
                    // the engine's take serializes through the same
                    // one), then re-check the phase: a resolve that
                    // raced the store is taken here, not slept
                    // through.
                    store_waker(slot, cx.waker().clone());
                    if slot.phase.load(Ordering::Acquire) != PHASE_SUBMITTED {
                        continue; // resolved while storing: take it above
                    }
                    match slot.phase.compare_exchange(
                        PHASE_SUBMITTED,
                        PHASE_WAITING,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        // The transition's release publishes the
                        // waker to the engine's resolve: it takes the
                        // waker only at `Waiting`.
                        Ok(_) => return Poll::Pending,
                        // Resolved between the re-check and the
                        // transition: the retry takes the outcome —
                        // the engine, still at `Submitted` when it
                        // loaded the phase, took no waker and woke
                        // nobody.
                        Err(_) => continue,
                    }
                }
                PHASE_WAITING => {
                    // A re-poll: the waker replaces under the lock,
                    // and the re-check after the store keeps a
                    // resolve that raced it from being slept
                    // through.
                    store_waker(slot, cx.waker().clone());
                    if slot.phase.load(Ordering::Acquire) != PHASE_WAITING {
                        continue;
                    }
                    return Poll::Pending;
                }
                _ => return engine_bug(),
            }
        }
    }
}

impl Drop for OpHandle {
    fn drop(&mut self) {
        let index = (self.slot >> 32) as u32;
        let generation = self.slot as u32;
        let Some(slot) = self.state.slab.slot_at(index) else {
            return;
        };
        if slot.generation.load(Ordering::Acquire) != generation {
            return; // stale — a recycled slot never matches this generation
        }
        loop {
            let phase = slot.phase.load(Ordering::Acquire);
            match phase {
                PHASE_SUBMITTED | PHASE_WAITING => {
                    // Claim released before resolution: the engine
                    // completes the operation anyway, frees the slot,
                    // and drops the buffer — after this transition
                    // tells it nobody claims the outcome.
                    match slot.phase.compare_exchange(
                        phase,
                        PHASE_ABANDONED,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => {
                            if phase == PHASE_WAITING {
                                // The waker is this handle's own —
                                // stored by its own poll — so it is
                                // this drop's to take back (under the
                                // lock: the engine's take at resolve
                                // runs through the same one) and drop.
                                drop(take_waker(slot));
                            }
                            return;
                        }
                        // The engine resolved mid-race: the retry
                        // takes the outcome and frees the slot here —
                        // the claiming handle is gone.
                        Err(_) => continue,
                    }
                }
                PHASE_RESOLVED => {
                    // Claim released after the resolution: the
                    // outcome was this handle's to take, so it is
                    // this drop's to drop, and the slot's recycle
                    // too. (The timeline records nothing — nobody
                    // waited for it.)
                    drop(slot.data_mut().resolved.take());
                    self.state.slab.recycle(slot, index);
                    return;
                }
                _ => return, // stale or mid-transition — nothing of ours left
            }
        }
    }
}

impl OpHandle {
    /// Block until the operation resolves — the join for callers
    /// without an executor (the library embeds none; with one, await
    /// the future instead). Hybrid, like the engine's own loop: the
    /// busy window is the spin budget — within it this thread spins
    /// (a fast operation then completes with no park cycle at all;
    /// the park/unpark cycle is most of a blocking round trip's
    /// cost), past it this thread parks until the engine's wake. No
    /// wake can be missed: the poll that runs when the spin gives up
    /// checks `resolved` and registers the waker under the same slab
    /// lock the engine resolves under — either the engine ran first
    /// (the check finds the result) or it runs after (it finds the
    /// waker and wakes this thread, token or park alike). Operations
    /// waited in turn still overlap: they complete concurrently, on
    /// the engine thread.
    ///
    /// The spin itself is lockless: it loads the slot's `done` flag,
    /// not a poll — a pending poll is a slab mutex pair and a waker
    /// clone, far too much to pay per spin iteration, and it hammers
    /// the very lock the engine resolves under. `done` set means the
    /// final poll finds `resolved` under the lock, so the fast path
    /// costs one poll per operation.
    pub(crate) fn wait(self) -> io::Result<Outcome> {
        // The budget: the busy window from now. `checked_add`
        // overflows on `Duration::MAX` — `None` then, which never
        // parks: pure spin, the dedicated-core case; `Duration::
        // ZERO` parks at once — the old behavior, like the engine's
        // own blocking rule.
        let budget = Instant::now().checked_add(*self.state.busy_window.lock().unwrap());
        let mut future = std::pin::pin!(self);
        let waker = thread_waker();
        let mut cx = Context::from_waker(&waker);
        loop {
            if let Some(deadline) = budget {
                // The clock, thinned: one read per sixty-four spins —
                // a `now()` per iteration would double the cost of a
                // spin at load-and-pause granularity, and the window
                // overshoot it buys (at most one check's worth) is
                // noise against the window's scale. The first check
                // runs at spin zero, so a spent budget (`ZERO`) parks
                // at once.
                let mut spins: u32 = 0;
                while !future.as_ref().done() {
                    if (spins == 0 || spins & 0x3f == 0) && Instant::now() >= deadline {
                        break;
                    }
                    spins = spins.wrapping_add(1);
                    std::hint::spin_loop();
                }
            } else {
                // `Duration::MAX`: no budget to check — pure spin on
                // the flag, forever if need be.
                while !future.as_ref().done() {
                    std::hint::spin_loop();
                }
            }
            match future.as_mut().poll(&mut cx) {
                Poll::Ready(result) => return result,
                // The budget spent (or a spurious wake): the waker
                // is registered, so park until the engine's wake.
                Poll::Pending => thread::park(),
            }
        }
    }

    /// The lockless fast-path view of "resolved" — see [`Slot::done`].
    fn done(&self) -> bool {
        self.done.load(Ordering::Acquire)
    }
}

// A waker that unparks the current thread — the park/wake pair
// behind [`OpHandle::wait`] and the engine tests' `block_on`. The
// four vtable functions manage the `Arc<Thread>` below the raw
// pointer, per `RawWaker`'s contract: `clone` bumps the count, `wake`
// and `drop` consume one, `wake_by_ref` borrows.
//
// Cached per thread: constructing one allocates the `Arc<Thread>`,
// and a per-`wait` construction would make that an allocation pair
// per *operation* — the count changes on each clone instead, and the
// allocation happens once per thread, on its first wait.
thread_local! {
    static THREAD_WAKER: Waker = // SAFETY: the vtable functions below
        // manage the arc moved under the pointer: `clone` bumps the
        // count, `wake` and `drop_waker` consume one, `wake_by_ref`
        // borrows.
        unsafe {
            Waker::from_raw(RawWaker::new(
                Arc::into_raw(Arc::new(thread::current())) as *const (),
                &VTABLE,
            ))
        };
}

pub(crate) fn thread_waker() -> Waker {
    THREAD_WAKER.with(|waker| waker.clone())
}

/// The vtable behind [`thread_waker`]'s raw pointer — module scope,
/// so the per-thread cache above can name it.
static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop_waker);

unsafe fn clone(data: *const ()) -> RawWaker {
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

/// The waker field's spin lock, taken by [`store_waker`] and
/// [`take_waker`]: uncontended, two atomics — and only ever touched on
/// the waiting paths, never on the fast one.
fn spin_waker_lock(slot: &Slot) {
    while slot
        .waker_lock
        .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
        .is_err()
    {
        std::hint::spin_loop();
    }
}

/// Store a waker into a slot — the waiter's half of the waker dance:
/// under the lock, so the engine's take (or a re-poll's replace)
/// serializes against it.
fn store_waker(slot: &Slot, waker: Waker) {
    spin_waker_lock(slot);
    slot.data_mut().waker = Some(waker);
    slot.waker_lock.store(false, Ordering::Release);
}

/// Take a slot's waker — the engine's half (at resolve) and the
/// abandoning dropper's (of its own waiting slot).
fn take_waker(slot: &Slot) -> Option<Waker> {
    spin_waker_lock(slot);
    let waker = slot.data_mut().waker.take();
    slot.waker_lock.store(false, Ordering::Release);
    waker
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
    /// The registered connections' loads — the engine-thread half
    /// of the tickets the submitters hold: the resolve hands the
    /// send-queue slot back through it, the destroy clears its
    /// `live` flag. Engine-thread-only, no lock.
    loads: HashMap<u32, Arc<ConnLoad>>,
    /// The completion sources, one per (engine, device): each drains
    /// every connection on its device, interleaved — one poll site
    /// per device, rotated by `cursor` (one, in the common
    /// single-device case).
    completions: Vec<Box<dyn CompletionSource>>,
    cursor: usize,
    /// Shutdown seen: the sources were closed once, now draining.
    closing_all: bool,
    /// The sweep's completion scratch, one bounded batch, hoisted and
    /// reused across sweeps: the loop's iterations are the two
    /// handoff granularities a sequential operation pays, so they
    /// carry no rebuilds — a poll fills `[0..count]` fully, and a
    /// taken entry goes back as an empty.
    batch: [Completion; SWEEP_BATCH],
    /// The mailbox's drain scratch: the queued commands swap into
    /// this, and are dispatched out of it, so both sides of the swap
    /// keep their allocations — a take-and-drop (what the swap
    /// replaces) frees the queue's buffer every round and re-mallocs
    /// it on the next push, an allocation pair per batch that the
    /// loop's steady state has no use for.
    scratch: VecDeque<Command>,
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
        // The busy window as a spin deadline, cached: recomputed on
        // progress only — a command taken, a completion routed — so
        // an iteration's own work is a mailbox check (one load), a
        // poll, and no clock read but one in sixteen. `None`
        // (`Duration::MAX`: `checked_add` overflows) never rests.
        let mut spin_until = self.spin_deadline();
        let mut window_checks: u32 = 0;
        while !self.shutdown_step() {
            if self.take_commands() > 0 {
                spin_until = self.spin_deadline();
            }
            let completed = self.sweep();
            if completed > 0 {
                spin_until = self.spin_deadline();
            }
            if self.state.shutdown.load(Ordering::Acquire) {
                continue; // draining: keep sweeping until the flush resolves
            }
            // The clock, thinned: the window check reads the clock
            // once per sixteen iterations — a `now()` per iteration
            // costs a vDSO round trip against each iteration's
            // single load and poll, and the overshoot it buys (at
            // most fifteen iterations' worth of spinning past the
            // window) only keeps the loop on its fast side longer.
            // The first check runs after the first iteration, so a
            // spent window (`ZERO`) parks without extra work.
            window_checks = window_checks.wrapping_add(1);
            if window_checks != 1 && window_checks & 0xF != 0 {
                continue;
            }
            if spin_until.is_none_or(|until| Instant::now() < until) {
                // The busy window: the time since the last progress —
                // a command taken or a completion routed — is unspent,
                // operations in flight or not. A submit that arrives
                // while the engine spins here finds its command taken
                // by the next sweep, with no wake cycle to pay: this is
                // what keeps a submit-and-wait loop off the wake
                // channel.
                continue;
            }
            if !self.any_flights() {
                // Idle: the window is spent and nothing is in flight —
                // rest on the wake channel alone (no completion events
                // are armed while nothing is in flight).
                self.block_until_wake(&[]);
                spin_until = self.spin_deadline();
            } else {
                // In flight: the window is spent — arm the completion
                // events and rest on them and the wake channel.
                self.hybrid_block();
                spin_until = self.spin_deadline();
            }
        }
        // Drained: the sources (any verbs objects) drop here, on the
        // engine thread, and with them the last `Arc` of the shared
        // state — unless operation handles outlive the engine, which
        // keep only their slots' resolved plain data.
    }

    /// The spin deadline of one progress event: the busy window from
    /// now — re-read here, once per progress, so a builder turning
    /// the knob live takes effect at the next progress — or `None`
    /// when the window overflows `Instant` (`Duration::MAX`: never
    /// rest).
    fn spin_deadline(&self) -> Option<Instant> {
        Instant::now().checked_add(*self.state.busy_window.lock().unwrap())
    }

    /// Take every queued command and dispatch it, FIFO — the order
    /// the submitters pushed them in. Returns how many.
    fn take_commands(&mut self) -> usize {
        // The fast check: one load, no lock — the common spin
        // iteration (nothing queued) costs an acquire load, not a
        // mutex pair. `pending` is held to "set by every push,
        // cleared by the drain that takes them, both under the
        // mailbox's lock", so a false here means empty at some
        // lock release since — and any push after that release
        // re-set it.
        if !self.state.pending.load(Ordering::Acquire) {
            return 0;
        }
        let state = Arc::clone(&self.state);
        let count;
        {
            let mut mailbox = state.mailbox.lock().unwrap();
            // Swap, not take: the scratch below keeps its allocation
            // for the next round, and the mailbox keeps the drained
            // batch's — a take would leave an empty queue in the
            // mailbox whose next push re-mallocs, and free the
            // taken one when it drops at scope end: an allocation
            // pair per batch. The flag clears in the same critical
            // section, per the invariant above.
            std::mem::swap(&mut mailbox.queue, &mut self.scratch);
            state.pending.store(false, Ordering::Release);
            count = self.scratch.len();
        }
        while let Some(command) = self.scratch.pop_front() {
            self.dispatch(command);
        }
        count
    }

    fn dispatch(&mut self, command: Command) {
        match command {
            Command::Register {
                conn,
                load,
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
                self.loads.insert(conn, load);
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
                registered,
            } => {
                let traced = self.state.op_trace.load(Ordering::Relaxed);
                let post_start = traced.then(Instant::now);
                let posted = match self.sources.get_mut(&conn) {
                    Some(entry) if !entry.closing => {
                        if registered {
                            // A registered buffer: the post runs
                            // against the registration directly — no
                            // hold comes back, nothing is parked.
                            entry
                                .source
                                .submit_registered(op, addr, len, target, wr_id)
                                .map(|()| None)
                        } else {
                            entry.source.submit(op, addr, len, target, wr_id).map(Some)
                        }
                    }
                    _ => Err(io::Error::other(format!(
                        "connection {conn} is gone or closing"
                    ))),
                };
                let post_end = traced.then(Instant::now);
                match posted {
                    Ok(token) => {
                        let index = (wr_id >> 32) as u32;
                        let generation = wr_id as u32;
                        if let Some(slot) =
                            self.state.slab.slot_at(index).filter(|slot| {
                                slot.generation.load(Ordering::Acquire) == generation
                            })
                        {
                            // The payload is the engine's now — the
                            // mailbox's handoff — so the hold of the
                            // local memory (the registration, or the
                            // pooled slice) parks here, into the
                            // flight, until the completion finishes
                            // it. A registered buffer has none (the
                            // registration outlives the operation).
                            let data = slot.data_mut();
                            if let Some(Flight::Owned { token: held, .. }) = data.flight.as_mut() {
                                *held = token;
                            }
                            // The timeline's post stamps, captured
                            // around the source's post path, stored
                            // before the payload stays engine-owned
                            // until the resolve.
                            if let (Some(start), Some(end)) = (post_start, post_end) {
                                data.trace.post_start = Some(start);
                                data.trace.post_end = Some(end);
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
            Command::RegisterBuffer {
                conn,
                addr,
                len,
                reply,
            } => {
                // The source owns the registration (every verbs call
                // on the engine thread); the reply releases the
                // registering thread, so it must answer in every
                // case.
                let answer = match self.sources.get_mut(&conn) {
                    Some(entry) if !entry.closing => entry.source.register_buffer(addr, len),
                    _ => Err(io::Error::other(format!(
                        "connection {conn} is gone or closing"
                    ))),
                };
                let _ = reply.send(answer);
            }
            Command::DeregisterBuffer {
                conn,
                addr,
                len,
                payload,
            } => {
                // The payload (the registered memory's owner) hands
                // over to the source when it has the registration —
                // its EBUSY machinery keeps the memory alive until
                // the device is done — and drops here when it does
                // not (a destroyed source dropped its queue pair
                // only after every operation flushed, so nothing can
                // still touch the memory).
                match self.sources.get_mut(&conn) {
                    Some(entry) => {
                        entry.source.deregister_buffer(addr, len, payload);
                    }
                    None => drop(payload),
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
    ///
    /// The batch is the hoisted scratch (`self.batch`): an iteration
    /// of this loop is a handoff granularity a sequential operation
    /// pays, so it carries no rebuilds — the poll fills
    /// `[0..count]` fully, and each taken routing moves its entry
    /// out (an empty goes back).
    fn sweep(&mut self) -> usize {
        if self.completions.is_empty() {
            self.reap_closing();
            return 0;
        }
        self.cursor %= self.completions.len();
        let mut routed = 0;
        let mut failed: Vec<(usize, io::Error)> = Vec::new();
        for _ in 0..self.completions.len() {
            let index = self.cursor;
            self.cursor = (self.cursor + 1) % self.completions.len();
            let polled = self.completions[index].poll(&mut self.batch);
            match polled {
                Ok(count) => {
                    for i in 0..count {
                        let completion = std::mem::replace(
                            &mut self.batch[i],
                            Completion {
                                wr_id: 0,
                                ok: false,
                                error: String::new(),
                            },
                        );
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

    /// Route one completion to its slot — by value, so the error
    /// message moves through instead of cloning.
    fn on_completion(&mut self, completion: Completion) {
        let outcome = if completion.ok {
            Ok(())
        } else {
            Err(io::Error::other(completion.error))
        };
        self.resolve(completion.wr_id, outcome);
    }

    /// Resolve the operation a wr_id names, lockless: finish the
    /// hold of its local memory (a pooled read's bytes copy into the
    /// buffer here, and a registration releases — both on the engine
    /// thread, before the buffer resolves), store the outcome — the
    /// buffer back on success, dropped on failure — and hand the slot
    /// over with the phase transition (see the phase constants
    /// above). A handle that already dropped it (`Abandoned`) has
    /// the engine clean up instead: the hold finishes, the outcome
    /// drops, the slot recycles. A handle that polled and parked a
    /// waker (`Waiting`) gets it taken and woken — outside the
    /// waker lock, a waker runs user code.
    fn resolve(&mut self, wr_id: u64, outcome: Result<(), io::Error>) {
        let index = (wr_id >> 32) as u32;
        let generation = wr_id as u32;
        let Some(slot) = self.state.slab.slot_at(index) else {
            return; // stale: a recycled slot never matches this generation
        };
        if slot.generation.load(Ordering::Acquire) != generation {
            return; // likewise
        }
        let traced = self.state.op_trace.load(Ordering::Relaxed);
        let conn;
        let mut resolved;
        {
            let data = slot.data_mut();
            if traced {
                // The timeline's fourth stamp: the completion is
                // being resolved now.
                data.trace.complete = Some(Instant::now());
            }
            let Some(flight) = data.flight.take() else {
                return; // already resolved (a duplicate completion)
            };
            conn = data.conn;
            let ok = outcome.is_ok();
            resolved = match flight {
                Flight::Owned { buffer, token } => {
                    if let Some(mut token) = token {
                        // Before the buffer resolves: a pooling hold
                        // copies the device's bytes into it here, so
                        // the handle (or the buffer's drop, unclaimed)
                        // sees the read.
                        token.finish(ok);
                    }
                    match outcome {
                        Ok(()) => Ok(Outcome::Buffer(buffer)),
                        Err(error) => {
                            drop(buffer);
                            Err(error)
                        }
                    }
                }
                // A registered buffer: nothing was parked, nothing
                // comes back — the user's buffer was theirs all
                // along.
                Flight::Registered => match outcome {
                    Ok(()) => Ok(Outcome::Unit),
                    Err(error) => Err(error),
                },
            };
        }
        let mut waker = None;
        loop {
            let phase = slot.phase.load(Ordering::Acquire);
            match phase {
                PHASE_SUBMITTED | PHASE_WAITING => {
                    if phase == PHASE_WAITING {
                        // The waker the parked handle left: taken
                        // under the lock, so a re-poll's replace
                        // cannot race this take.
                        waker = take_waker(slot);
                    }
                    // Store, then transition: the transition's
                    // release publishes the outcome and the done
                    // flag to whoever acquire-loads the phase (or
                    // the flag).
                    slot.data_mut().resolved = Some(resolved);
                    slot.done.store(true, Ordering::Release);
                    match slot.phase.compare_exchange(
                        phase,
                        PHASE_RESOLVED,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => break, // the claiming handle takes it
                        Err(_) => {
                            // The abandoning dropper won the race:
                            // take the outcome back — unobserved,
                            // only a successful transition ever
                            // published it — and clean up on the
                            // retry.
                            resolved = slot
                                .data_mut()
                                .resolved
                                .take()
                                .expect("stored above, unobserved");
                            continue;
                        }
                    }
                }
                PHASE_ABANDONED => {
                    // Nobody claims the outcome: it drops here, the
                    // hold already finished above, and the slot is
                    // the engine's to recycle.
                    drop(resolved);
                    drop(waker.take()); // dropped, not woken: nobody waits
                    self.state.slab.recycle(slot, index);
                    break;
                }
                _ => {
                    // Unreachable by the protocol (a live flight is
                    // Submitted, Waiting, or Abandoned) — defensive:
                    // the outcome drops, the send-queue slot is
                    // still the engine's to hand back below.
                    drop(resolved);
                    break;
                }
            }
        }
        // The work request completed — the send-queue slot is free
        // again, whether the handle claims the result or the slot
        // freed above.
        if let Some(load) = self.loads.get(&conn) {
            load.in_flight.fetch_sub(1, Ordering::AcqRel);
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
    /// reaped by the very next sweep). The load's `live` flag clears
    /// here, on the engine thread, so every submission through the
    /// connection's ticket after the destroy fails eagerly — and
    /// everything queued before it, ahead of the destroy in the
    /// mailbox, still dispatches.
    fn begin_destroy(&mut self, conn: u32) {
        if let Some(load) = self.loads.get(&conn) {
            load.live.store(false, Ordering::Release);
        }
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
            // The load goes with the source: nothing of the
            // connection resolves here anymore. (The tickets out
            // there keep their `Arc` — their submissions fail on
            // the dead `live` flag, which is all it is to them.)
            self.loads.remove(conn);
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
        if self
            .completions
            .iter()
            .any(|source| source.event_fd().is_none())
        {
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
        self.state.slab.any_flights()
    }

    fn has_flights(&self, conn: u32) -> bool {
        self.state.slab.has_flights(conn)
    }

    /// Every in-flight operation's packed wr_id, anywhere.
    fn all_flights(&self) -> Vec<u64> {
        self.state.slab.all_flights()
    }
}

fn invalid(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, msg.into())
}
