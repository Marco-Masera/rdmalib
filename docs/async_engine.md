# Async operations engine — design

Status: agreed design; implementation in the step order below. Steps
0–11 are done: the affinity FFI; the provider service-thread pinning;
the wake channel (`pipe`/`poll(2)`); the engine itself (mailbox,
slab, waker-driven op handles, idle blocking on the wake channel,
drain-then-exit); the verbs `post`/`poll` split behind the seam
(implemented for `rdma::Connection`/`rdma::SharedCompletions` in
`rdma/verbs.rs`, and for injected fakes in `src/tests/engine.rs` —
the engine's tests run without hardware); the reader integration —
`update` hands the group's connection to the engine
(`with_engine`/default own-unpinned), `RemoteMemoryRegion` submits
owned-buffer operations eagerly (`read_async` … `write_typed_async`)
returning `RegionOp` futures (`RegionOp::wait` is the executor-less
join), and the synchronous op API is deleted (verbs `read`/`write`/
`one_sided`/`POLL_TIMEOUT` and the reader-side sync calls); the
hybrid in-flight idle — per-device completion event channels, the
arm-before-block dance with `poll(2)` on the events and the wake
channel, and the busy-window knob (`Engine::with_busy_window`,
`Duration::MAX` = pure busy for the dedicated-core case); the shared
completion queue — one CQ per (engine, device)
(`rdma::SharedCompletions`, `Connection::connect_shared`): every
connection of the device posts into it, so the engine's sweep polls
one source per device (a single poll site, in the common
single-device case) instead of one queue per connection, and the
per-connection round-robin retires; and the registered-buffer
recycling — one pooled registration per connection (`POOL_ENTRY`/
`POOL_SLOTS`, one registration total, created lazily): operations
at most `POOL_ENTRY` bytes bounce through pooled slices (a copy in
at post for a write, a copy out at the hold's finish for a read)
instead of a registration per operation; larger ones, and ones
that find the pool fully lent out, register directly, exactly as
before the pool. Over the send-queue depth: the hardware's caps
queried (`ibv_query_device`, once per device's shared queue —
`max_qp_wr`, `max_cqe`), the knob
(`RemoteMemoryProvider::set_max_send_wr`, default 128), the
reject-with-the-numbers check at `update()` (the device's
`max_qp_wr`, the shared queue's completion budget — the sum rule),
and the eager submission bound (a submit past the connection's
depth fails on the spot, with the numbers in the message); and the
window gating every block — the busy window since the last progress
(a command taken or a completion routed) defers the idle block on
the wake channel too, not only the hybrid one, so a strict
submit-and-wait loop finds the engine hot and pays no wake for it
(the first benchmarks measured ~24 µs per sequential op: two
block-and-wake cycles, the engine's and the waiter's); the default
window raised to 256 µs accordingly.

Goals and invariants not restated here live in `AGENTS.md`; this doc
extends them with the operations path: one-sided reads and writes as
async operations, polled by one pinned, library-owned engine thread.

## Goals

- One-sided RDMA reads and writes as async operations: submit N, await
  all, await a subset, attach callbacks — with the standard
  async/await machinery of any executor the user chooses (e.g. Tokio).
  The library provides submission and completion plumbing only; the
  more orchestration logic stays in the user application, the better.
- CPU pinning: the user creates an `Engine` on a specific CPU and hands
  it to the controllers; a shared engine pins the polling of everything
  attached to it to that one CPU.
- One poller per engine: while operations A and B are waiting, the same
  CPU polls for both, interleaved — the poller drains queues and never
  waits for one particular operation.
- Zero dependencies: futures and wakers are `std`; the library embeds
  no async runtime and works with any executor.

## Decisions (agreed)

1. **Async-only op API.** The synchronous read/write calls are removed;
   blocking users await the op (the overhead is minimal). One verbs op
   path instead of two.
2. **Eager submission.** `read_async`/`write_async` submit at call
   time — checks, buffer registration, WR post all happen on
   submission, errors fail at submit — and return `io::Result<Op>`: the
   op handle is purely a completion claim. Rust futures are lazy, so
   lazy submission would break "submit N, then await".
3. **Owned buffers, both directions.** Writes take ownership of a
   `Vec`; reads allocate (or take a `Vec` to fill) and hand it back on
   completion. The op slot owns the buffer and its registration from
   submission to completion, so a dropped or `mem::forget`-ten handle
   can never race the device — no borrowed-buffer unsoundness, and
   abandonment is free.
4. **Round-robin CQ polling first**, shared-CQ-per-engine later (see
   steps): the engine sweeps the registered per-connection completion
   queues, a bounded batch per CQ per sweep, so no stream starves
   another.
5. **Engine is explicit and shareable.** `Engine::on_cpu(n)` (pinned) /
   `Engine::new()` (unpinned); passed to controller constructors. A
   `RemoteMemoryProvider` without an engine creates its own, unpinned.
6. **Hybrid polling is mandatory.** A bounded busy-poll window after
   the last progress — a command taken or a completion routed, work in
   flight or not (this is where the pinned-CPU latency win lives, and
   what keeps a sequential submit-and-wait loop off the wake channel)
   — then fd-blocking — otherwise every default engine would burn a
   core.
7. **No engine-side timeouts.** Every posted WR completes (a torn-down
   connection flushes its outstanding WRs as error completions), so the
   current skip-stale-completions hack is deleted. Timeouts are user
   composition (`tokio::time::timeout` or similar); a late completion
   resolves into an abandoned slot and is dropped.
8. **Mailbox submission: all op-path verbs calls run on the engine
   thread.** QPs are not thread-safe and verbs handles are `Send` but
   not `Sync` — moving posting, registration, and polling onto one
   owned thread keeps that stance and makes it stronger, not weaker.

## Architecture

### Engine

- `Engine` handle: `Arc<EngineShared>`, `Clone`; handing it to several
  controllers attaches them to one polling thread.
- `EngineShared` (the `Arc`'d control block): the CPU id, the mailbox
  (`Mutex<VecDeque<Command>>`), the op slab (`Mutex<Vec<Slot>>`), the
  wake-pipe write end, the shutdown flag, and the join handle (for
  explicit stop and last-drop).
- The engine **thread** owns the verbs objects — `HashMap<conn_id,
  Connection>` and the CQ sweep list — and holds only a
  `Weak<EngineShared>`: when the upgrade fails, every handle and every
  op claim is gone, so it drains and exits. This avoids the
  Arc-self-reference deadlock and gives "alive while anything uses it"
  semantics.
- Commands: `Register(Connection)` (from a reader session's `update`),
  `Destroy(conn_id)` (from a session's teardown), `Post { slot, conn_id,
  opcode, remote_addr, rkey }` (from op submission). The wake pipe is
  written on submit and on shutdown; the engine blocks on it (plus the
  comp channels) in hybrid idle.

### Ops, slots, wr_ids

- Submission (on the provider's home thread): bounds/layout/no-session
  checks → the send-queue bound (a submit past the connection's
  depth — the operations in flight on it — fails here, eagerly, with
  the numbers in the message; the load, `depth`/`in_flight`, lives in
  the slab state, checked-and-incremented under its one lock) →
  allocate a slab slot `(index, generation)` → move the buffer into
  the slot → push `Post` → wake the pipe. The bound follows
  completions: the count drops at resolve, whether or not the handle
  claims the result. This is the old C++ library's
  `Exceeded rdma completion queue size` guard (`ops.cpp`), at the
  async seam.
- `wr_id = (slot_index << 32) | generation` — engine-wide unique, so
  completions route without consulting any connection state; the
  per-connection `next_wr_id` counter disappears.
- The engine, on `Post`: takes the buffer out of the slot, holds its
  local bytes for the flight, builds the WR with the packed wr_id,
  `post_send`s it. The hold is the connection's choice (the
  `OpSource` seam): a slice of its pooled registration — a copy in
  at post for a write — for an operation at most `POOL_ENTRY` bytes
  (the pool registers once, in the connection's domain, and reuses);
  a direct registration in the connection's protection domain
  otherwise (every large operation, and any that finds the pool
  fully lent out). The slot holds the buffer plus the hold while in
  flight.
- The engine, on a completion: decodes the wr_id; on a generation
  match it finishes the hold (a pooled read's bytes copy into the
  buffer here, still on the engine thread, before the buffer
  resolves — this order is the pool's correctness), releases it,
  resolves the slot (`Ok(buffer)` / `Err(status)`), collects the
  waker — and wakes only after releasing the slab lock, since
  wakers run user code. If the slot is unclaimed (the handle was
  dropped), the engine drops the buffer itself.
- The op handle implements `Future` over the slot (`std::task::Waker`):
  poll → take the result if resolved, else register the waker, re-check
  (the engine may resolve in between), return `Pending`. Handles are
  `Send`, so awaiting can happen on any executor thread.
- Handle drop mid-flight marks the slot unclaimed; the engine still
  completes, frees the buffer, deregisters. `mem::forget` of a handle
  leaks that op's buffer and its engine reference — the standard,
  acceptable cost.
- Typed ops erase the buffer the way the provider side already does
  (`Owner = Rc<RefCell<Box<dyn Any>>>` in `src/providers.rs`): slots
  hold `Box<dyn Any + Send>`, handles downcast on pickup — never a
  transmute of the `Vec` itself.

### Send-queue depth, and its bound

Each connection posts with a send-queue depth — the operations that
may be in flight on it at once. The engine enforces it at
submission, eagerly: a submit past the depth fails on the spot
(`InvalidInput`, the numbers in the message), instead of the device
rejecting the post (`ibv_post_send` `ENOMEM` of a send queue out of
work requests) and the failure surfacing as the op's completion.
The bound follows completions: the count drops at resolve, whether
or not the handle claims the result, so the slot frees as work
completes, not as handles drop.

Where the depth comes from:

- The knob: `RemoteMemoryProvider::set_max_send_wr(n)` (default
  `DEFAULT_MAX_SEND_WR = 128`), applied to the sessions established
  after the call. `RemoteMemoryRegion::max_in_flight()` reads the
  effective bound back.

```rust
let provider = RemoteMemoryProvider::with_engine(addr, engine.clone());
provider.set_max_send_wr(512);           // default: 128
provider.update(0)?;                      // may reject — see below
let region = provider.get_remote_mr(&catalog[0], Some(0))?;

// Submit at most this many before awaiting some —
// a submit past the depth fails, eagerly, with the numbers:
let ops: Vec<_> = (0..region.max_in_flight())
    .map(|i| region.read_async(i * 4096, 4096))
    .collect::<io::Result<Vec<_>>>()?;
let error = region.read_async(0, 4096).unwrap_err(); // "send queue is full: …"
```

- The hardware: `ibv_query_device` at the device's shared-queue
  creation (`rdma::SharedCompletions`), once per (engine, device) —
  `max_qp_wr` (the per-QP work-request ceiling) and `max_cqe` (the
  CQ entry ceiling) are stored on the queue;
  `RemoteMemoryProvider::device_max_qp_wr()` exposes the ceiling
  once a session has run.
- The check, at `update()` — the earliest point the hardware is
  known — rejects (never silently clamps) a request past either cap,
  with both numbers in the message: the device's `max_qp_wr`, and
  the shared queue's remaining completion budget.
- The budget: the depths of a device's live connections must sum
  under its shared queue's depth (a simultaneous completion burst
  from every connection must fit), or the queue overruns
  (`IBV_EVENT_CQ_ERR` — resolved loudly by the engine's completion
  failure path). A connection lends at connect and returns its share
  at drop, under one mutex, so the budget is exact at any moment.
  The queue is sized once per device — `max(SHARED_CQ_SIZE = 1024,
  the first connection's request)`, capped by `max_cqe` — because a
  CQ cannot grow while queue pairs feed it: a later, larger request
  than the first one's must fit the remaining budget.

### Poll loop

- Sweep of the completion sources, one per (engine, device), rotated
  across devices only — a single poll site in the common
  single-device case; bounded batch per source per sweep; process
  completions; sweep again.
- Busy-poll within the window since the last progress (configurable;
  the dedicated-core/HPC case can ask for pure busy) — operations in
  flight or not: a submit that arrives while the engine spins finds
  its command taken by the next sweep, so a sequential
  submit-and-wait loop never pays a block-and-wake cycle.
- The executor-less wait mirrors the same rule: `wait` spins within
  the window before its first park — a fast operation then completes
  with no park cycle at all — and a wake that lands during the spin
  leaves an unpark token, so the spin-to-park transition cannot miss
  one.
- Window spent, nothing in flight: fd-block on the wake pipe alone.
- Window spent, operations in flight: hybrid fd-block —
  1. arm every CQ (`ibv_req_notify_cq`) **before** blocking, or the
     engine sleeps on a completion it already holds;
  2. `poll(2)` on [wake-pipe read end, per-device comp-channel fds];
  3. on a comp-channel event: `ibv_get_cq_event`, `ibv_ack_cq_events`,
     then drain that CQ to empty (events coalesce; more completions
     may have landed since arming — the ack-then-drain pattern from
     the man page);
  4. on the wake pipe: drain the pipe, take the mailbox commands, back
     to busy.
- New FFI, each verified against upstream headers and pinned with
  tests per the verbs FFI rules: `sched_setaffinity`, `pipe`,
  `poll` (done — `src/os.rs`), `ibv_create_comp_channel`,
  `ibv_get_cq_event`, `ibv_ack_cq_events`, `ibv_destroy_comp_channel`
  (done — real symbols in `rdma/verbs.rs`), and
  `ibv_req_notify_cq` (done — `static inline` in `verbs.h`, so it
  dispatches through the device ops table at index 12, like
  `post_send`/`poll_cq`).

### Lifecycle

- `Engine::on_cpu(n)`: the engine thread pins itself at startup
  (`sched_setaffinity(0, …)`; affinity is a per-thread attribute).
  Construction fails cleanly on an unusable CPU.
- `RemoteMemoryProvider::new(addr)`: own unpinned engine;
  `RemoteMemoryProvider::with_engine(addr, engine)`: shared.
  `update` stays synchronous on the home thread (TCP handshake,
  `rdma_cm` connect, resource creation — rare, already synchronous)
  and then hands the `Connection` to the engine by mailbox; the reader
  side's `GroupConnection` slot shrinks to `{engine, conn_id}` — the
  user thread holds no verbs op objects at all.
- `SharedMemoryRegionProvider`: accepts an `Engine` for its affinity
  only (it issues no one-sided ops); the service thread pins itself.
  `with_cpu(addr, cpu)` exists today; `with_engine` maps
  `engine.cpu()` onto it. Session threads (update-wait) stay unpinned.
- Shutdown: last `Arc` drop or explicit `Engine::shutdown` → stop
  taking commands, then **drain-then-exit**: disconnect (outstanding
  WRs flush as error completions), resolve every slot, deregister
  every MR, only then destroy the connections and their protection
  domains — the same drop-order discipline as the provider
  (`Drop for SharedMemoryRegionProvider`).

### What gets deleted (step 2)

- `RemoteMemoryRegion::read`/`read_into`/`read_typed`/
  `read_into_typed`/`write`/`write_typed` (src/readers.rs) — replaced
  by the async equivalents.
- `Connection::read`/`write` public API (src/rdma/verbs.rs) — the post
  and poll halves become engine-internal (`pub(crate)`).
- `POLL_TIMEOUT` and the skip-stale-wr_id loop (verbs.rs:883-910).
- Connection-flavored `register`/`register_addr` — engine-internal
  (the protection-domain versions stay; the provider uses those).
- The `rdma/mod.rs` example, rewritten on the async API.

### Send/Sync stance

- All op-path verbs calls run on the engine thread: the verbs handles
  keep `Send`, still need no `Sync`.
- Op handles are `Send` (`EngineShared` is `Mutex`es, pipes, atomics,
  and `Box<dyn Any + Send>`).
- `RemoteMemoryProvider`/`RemoteMemoryRegion` stay `Rc`/`!Send`:
  submission from the controller's home thread, awaiting from anywhere
  (`tokio::spawn` needs only the op handle to be `Send`). A full
  `Send` reader-side redesign is an open question, not a requirement.

## API sketch

```rust
let engine = Engine::on_cpu(2)?;               // pinned engine thread
let provider = RemoteMemoryProvider::with_engine(addr, engine.clone());
provider.update(group)?;
let region = provider.get_remote_mr(&meta, Some(group)).unwrap();

// Submit N jobs eagerly, then await however the application likes:
// join_all, JoinSet, select! on a subset, timeouts, callbacks via
// spawn — user-side machinery, user-side dependencies.
let ops = (0..n)
    .map(|i| region.read_async(i * 4096, 4096))
    .collect::<io::Result<Vec<_>>>()?;
let buffers = futures::future::join_all(ops).await; // Vec<io::Result<Vec<u8>>>

// Writes hand the buffer back on completion (recyclable later).
let op = region.write_typed_async(0, vec![42u64; 64])?;
let returned = op.await?;

// Without an executor: block on the op — `wait` is the join (the
// engine thread does the waiting work; this thread spins within the
// busy window — a fast op completes with no park cycle — and parks
// past it).
let buffer = region.read_async(0, 4096)?.wait()?;
```

## Testing strategy

- Inner (src/tests/, no hardware): the mailbox, slab, waker, and
  lifetime machinery — the risky part — tested against an injected
  completion source: the engine's poll step goes through a seam so
  tests can fake completions (on demand or after N polls) without any
  device. Teardown choreography (drain-then-exit, MR-before-PD order)
  tested the same way.
- Outer (tests/, cluster): the existing read/write integration tests
  move to the async API; the `#[ignore]` gating and the
  `scripts/test_runner.py` deploy flow are untouched.
- FFI: every new declaration gets constants/layout tests, like the
  verbs mirror tests at the bottom of verbs.rs (the affinity mask
  mirror is tested in step 0).

## Implementation steps

0. **(done)** This doc; `src/os.rs` CPU-affinity FFI + tests;
   `SharedMemoryRegionProvider::with_cpu` + service-thread
   self-pinning at `serve()`.
1. **(done)** verbs: `one_sided` split into a post (given a wr_id)
   and a poll half, joined behind the `OpSource` seam; engine
   (`src/engine.rs`): mailbox, slab, waker-driven op handles,
   round-robin busy sweep, idle blocking on the wake channel
   (`pipe`/`poll(2)` landed here, ahead of the hybrid step),
   drain-then-exit; inner tests against an injected completion source
   (`src/tests/engine.rs`).
2. **(done)** Reader integration: `update` hands connections to the
   engine (`Engine::with_engine` constructors, default
   own-unpinned engine); async ops on `RemoteMemoryRegion`
   (`Submitted`-owned buffers, eager submit, `RegionOp::wait` as the
   executor-less join); sync op API and timeout machinery deleted
   (the list above); `rdma/mod.rs` example rewritten; inner
   e2e/remote tests reworked; the outer cluster tests migrated
   (user-approved, mechanically).
3. **(done)** Hybrid in-flight idle: per-connection comp channels
   (the `ibv_create_cq` channel argument), req_notify +
   `poll(2)` + get/ack event dance; the busy-window knob
   (`Engine::with_busy_window`; `Duration::MAX` = pure busy).
4. **(done)** Shared CQ per engine + batched drain (SWEEP_BATCH=16);
   the per-connection round-robin sweep retires — the engine polls
   one completion source per (engine, device)
   (`rdma::SharedCompletions`, `Connection::connect_shared`), one in
   the common single-device case, rotated only across devices.
5. **(done)** Registered-buffer recycling: one pooled registration
   per connection (`POOL_ENTRY`-byte slices, `POOL_SLOTS` of them,
   one `ibv_reg_mr` total, created lazily) — operations at most
   `POOL_ENTRY` bytes bounce through it (a copy in at post for a
   write, a copy out at finish for a read) instead of a
   registration per operation; larger ones, and ones that find the
   pool fully lent out, register directly. The seam holds: the
   engine's in-flight hold (`InFlight`, per operation) is finished
   — copy out, release — before the operation's buffer resolves.
 6. **(done)** The send-queue depth, its hardware caps, its knob, and
    its eager submission bound (see "Send-queue depth, and its
    bound" above): `ibv_query_device` per device
    (`max_qp_wr`/`max_cqe`), `set_max_send_wr` (default 128),
    reject-with-the-numbers at `update()`, the bound at submit.
 7. **(done)** The window gates idle blocking too: one knob
    (`busy_window`) covers every block — the engine keeps sweeping
    for the window after the last progress even with nothing in
    flight, so a strict submit-then-wait loop (the first benchmarks
    measured ~24 µs per sequential op: two block-and-wake cycles,
    the engine's and the waiter's — the engine's half was the
    idle block between ops) finds the engine hot and pays no wake
    for it. `Duration::ZERO` keeps the old immediate-idle-block
    behavior; `Duration::MAX` never blocks at all, idle or not. The
    default raised to 256 µs — a round trip plus the gap between
    sequential operations.
 8. **(done)** The waiter mirrors the engine's rule, and the spin
     got cheap. `OpHandle::wait`/`RegionOp::wait`: the busy window
     is the spin budget — within it the waiting thread spins (a
     fast operation completes with no park cycle at all; the
     park/unpark cycle is most of a blocking round trip's cost), a
     wake during the spin leaves an unpark token, and past the
     budget the thread parks as before; `Duration::MAX` never parks,
     `Duration::ZERO` parks at once. The loop's iteration, the two
     handoff granularities a sequential op pays, lost its
     per-iteration rebuilds: the spin deadline is cached per progress
     event (no per-iteration window lock or clock pair), the sweep's
     completion batch is hoisted and reused, and the verbs poll's
     work-completion scratch is uninitialized (the queue writes
     `[0..ret]` fully) instead of a per-poll 1 KiB zeroing. Measured
     on the first benchmark cluster (2008-era Xeon L5420 nodes,
     engine and waiter pinned as L2 mates): sequential 8–64 B ops
     ~24 µs → ~7 µs p50 (~6.5 µs min) park-default — `wait` and the
     spin-wait floor converge; the remaining ~4 µs over same-thread
     perftest (~3 µs) is the engine-thread architecture on that
     hardware (two handoffs + submit-side work), with an
     environment tail (~+11 µs on ~10–35% of spin-burned samples:
     the descheduler, not the library — a parked waiter does not
     pay it).
 9. **(done)** The hot paths are lockless, allocation-free, and —
     proven by syscall counting — run entirely in user space while
     both threads are within their busy windows. The audit that
     followed the step-8 numbers (a fixed ~5 µs of per-op cost over
     the wire on newer nodes) itemized what a sequential
     submit-and-wait still paid: a slab mutex pair and a waker
     clone *per spin iteration* of `wait`'s poll loop (which also
     contended the very lock the engine resolves under), a
     `mem::take` of the mailbox queue that freed its buffer every
     drain and re-malloc'd it on the next push (an allocation pair
     per batch), a fresh `Arc<Thread>` waker per `wait` (another
     pair per operation), and one clock read per engine iteration.
     The fixes, in the same order: the slot carries a `done` flag —
     an `Arc<AtomicBool>`, one allocation per *slot* not per
     operation, cloned into the handle at submission (the handle
     cannot index into the slab's `Vec`, which a concurrent
     submission's push can reallocate — the `Arc`'s allocation is
     stable), reset at allocation and set at resolution — and
     `wait` spins on that one acquire load (the clock checked once
     per sixty-four spins), taking the lock only for the single
     final poll, which no resolution can race: the check and the
     waker registration share the engine's critical section; the
     mailbox drains by swapping with a hoisted scratch queue (both
     sides keep their allocations), behind a `pending` flag set
     under the lock by every push and cleared by the drain — so an
     idle iteration is one load, not a mutex pair; the waker is
     cached per thread (one `Arc<Thread>` per thread, refcounts
     thereafter). Measured: sequential 8–64 B ops on the L5420 pair
     ~7 µs → **~6.1–6.3 µs p50 with mean ≈ p90** (the environment
     tail vanished — the spin-poll's lock churn was what attracted
     the descheduler); 16-op concurrent groups 43.9 → 39.9 µs
     (read) and 37.0 → 33.3 µs (write) — sixteen waiters no longer
      hammer the slab; `strace -c` over a 3200-op run counts 50
      `write`s, 5 `futex`es — all startup, shutdown, and logging:
      **zero syscalls per operation**. The machinery alone: ~1.5 µs
      p50 on a modern dev machine by the fake-source diagnostic
      (`machinery_round_trip_floor`), and 1.4–1.8 µs by the
      operation timeline on the real path (step 10) — CPU-bound and
      clock-scaled, as designed. What is left for the follow-on
      (the registered buffers below, then a lock-free submit/resolve
      path if needed): the type-erased `Box<dyn Any>` per operation
      and the pooled slice's lend/return mutex pairs — the last
       allocation pair and the last three lock pairs a sequential
       operation pays.
 10. **(done)** The engine's own clock thinned, the operation
     timeline made visible — and the "verbs-path residual" it
     revealed dissolved into a measurement artifact. The run loop's
     window check had read the clock every iteration (a vDSO round
     trip against each iteration's single load and poll): thinned
     to once per sixteen, the L5420 pair's sequential reads dropped
     6.3 → **5.9 µs p50** from that alone. The timeline trace
     (`RDMALIB_OP_TRACE=1`, `Engine::with_op_trace`): five stamps
     per operation — submitted, post entered, post left, completion
     resolved, waiter done — each written under the slab lock the
     stamper's path already holds, recorded by the waiter's final
     poll outside it, printed at shutdown as percentiles of the
     four segments. L5420 pair: submit→post 0.52 µs, post 0.49 µs,
     wire 3.8 µs, complete→done 0.81 µs. EPYC pair (Broadcom 10G
     RoCE, governor-throttled): submit→post 0.34 µs, post 0.65 µs,
     wire 18.8 µs, complete→done 0.41 µs — machinery 1.4 µs against
     perftest's self-reported 10.7 µs at 64 B, an apparent ~8 µs
     residual that inline data, a channel-less CQ, and an
     empty-poll probe (30 ns) each failed to explain. The
     explanation was perftest itself: it converts cycles to µs
     with the wrong rate — on that pair a frequency it detects as
     conflicting ("Conflicting CPU frequency values detected …
     CPU Frequency is not max", printed every iteration), and
     run-difference wall-clock (100 000 iterations minus 5 000)
     gives **21.6 µs per perftest op, not 10.7**. Our sequential
     op there is 20.8 µs p50 — under single-thread parity,
     machinery 1.4 µs. The same check on the L5420 pair (fixed
     clocks, no warning printed — perftest stays silent there)
     found it mis-scaled there too: self-reported 2.28 µs at 64 B,
     run-difference real **4.63 µs** (0.920 s at 100 000 minus
     0.480 s at 5 000). Our own numbers are honest: an
     external-clock difference (1.1 M iterations against 10 000,
     timed from another machine) gives 5.9 ± 0.3 µs real against
     the 6.32 µs our CLOCK_MONOTONIC histogram reports. The
      verdict: our 64 B sequential write there is 6.32 µs — the
      real baseline 4.63 plus **1.7 µs of machinery**, exactly the
      traced segments (0.52 + 0.63 + 0.65), the wire window at
      parity (our post→complete 4.17 against perftest's real
      loop minus its post); the EPYC pair under parity outright.
      The fixed cost above the wire is the machinery — 1.4–1.8 µs
      at these clocks — and no perftest self-reported µs is a
      baseline anywhere: run-difference wall-clock, or the
      timeline trace, every time.

 11. **(done)** Registered buffers — the zero-copy operation path
      (the section below): `RBuf<T>` handles
      (`RemoteMemoryProvider::register_buffer`), operations by
      mutable borrow (`read_into_rbuf`/`write_rbuf`), the engine's
      registered-op plumbing (`submit_registered` posts against the
      registration directly — no pool slice, no hold, no
      `Box<dyn Any>` on the operation's path; registrations and
      deregistrations are mailbox commands, the deregistration
      carrying the handed-over memory to the engine thread, an
      `EBUSY` verbs-side parked until a later submit drains it).
      Measured on the first cluster at 64 B: write 6.40 → **5.92
      µs p50**, read 6.14 → **5.76 µs p50** — ~0.4–0.5 µs, the
      pool's copy and the buffer's move through the slot gone.

## Registered buffers (step 11)

The measured follow-on to the pool (the last of the open questions
below): a user-facing registered-buffer API — the old C++ library's
zero-copy model, brought to the engine's threading rules. Done: the
`RBuf` handle, the reader-side API, the engine's
registered-operation plumbing, tests, and the benchmarks' zero-copy
singles.

- `RBuf<T>`: a buffer the user owns and the engine registers, once,
  in the group's connection domain — created by
  `RemoteMemoryProvider::register_buffer(group, vec)` (a blocking
  round-trip through the mailbox: every verbs call stays on the
  engine thread, per the design's invariant). The handle keeps the
  registration alive until dropped; the drop deregisters through
  the mailbox, handing the buffer's memory back — a deregistration
  queued behind in-flight operations of the connection lands after
  them (the mailbox is FIFO), and a registration verbs-side busy
  (`EBUSY` — an operation still on the wire) parks until a later
  submit drains it, so the memory hands over with nothing of itself
  in flight.
- Operations take it by mutable borrow: `read_into_rbuf(offset, &mut
  rbuf)` / `write_rbuf(offset, &mut rbuf)` — no allocation, no
  type-erasure boxing, no pool copy; the engine posts directly
  against the registration (`submit_registered`: the covering
  lookup over the connection's registrations — disjoint live
  allocations make it unambiguous), and the slot holds only the
  wr_id (no `InFlight` hold: the registration outlives the
  operation). The future (`RBufOp`) keeps the buffer's exclusive
  borrow and resolves with nothing back; dropping it abandons the
  operation — the device may touch the memory until the completion,
  so keep the future alive (or await a later operation on the same
  group) before reusing a dropped operation's buffer.
- The wins, measured on the first cluster at 64 B (the zero-copy
  counterparts of the owned-path singles, same nodes, same payload):
  write **6.40 → 5.92 µs p50**, read **6.14 → 5.76 µs p50** —
  ~0.4–0.5 µs, the pool's copy, its lend/borrow, and the buffer's
  move through the slot gone; larger reused buffers save the copy
  in proportion. What it does not remove: the two engine-thread
  handoffs (submit → post, completion → wake) — the sequential
  small-op floor stays the engine-thread architecture's (~5.9 µs
  p50 on these nodes, of which machinery ~1.4 µs over perftest's
  real wall-clock there — 4.63 µs at 64 B — while the EPYC pair
  measured under parity outright; step 10). Same-thread posting —
  perftest's model — would break the `!Sync` verbs stance and the
  one-poller design, and is not planned.
- Testing: the registration commands through the mailbox (inner,
  against the fakes — a registered operation posts the registered
  address and resolves with nothing; a deregistration lands after
  the operations queued before it), the reader-side path end to
  end against the fakes (register, read into, write from, the
  group-mismatch rejection, the drop's deregistration), and the
  benchmarks' `single_{read,write}_rbuf` for the real half.

## Tuning for latency

The engine's own costs are small and user-space-only (steps 9
and 10),
but they sit *on top of* whatever the machine is doing — placement,
clocks, and interrupts decide most of the per-op cost above the wire.
The rules, in order of measured impact:

- **Pin the engine and the waiting thread together, to cores that
  share an L2/LLC.** The two threads hand off twice per operation
  (submit → post, completion → wake) and share the mailbox and slab
  locks across every handoff; on every architecture measured, a lock
  line that stays inside one cache translates in tens of
  nanoseconds, one that crosses a socket costs microseconds. The
  library pins the engine (`Engine::on_cpu`); the waiting thread is
  pinned by the caller (the benchmarks expose `--engine-cpu`/
  `--client-cpu`, defaulting to the first cluster's placement — find
  the machine's own with its topology: `lscpu` for siblings and
  NUMA, `/proc/interrupts` for where the NIC's completion vectors
  land). Keep *both* off the NIC's completion-interrupt cores — an
  ISR stealing the engine's core mid-poll costs more than any cache
  crossing. Measured on the first cluster: same-L2 beat a private
  core by ~2.7 µs/op; on the EPYC pair the same-CCX default was the
  best of the placements tried.
- **Fix the CPU frequency — twice over.** A node whose governor
  parks its cores at base frequency pays ~2× on every CPU-side
  cost — the machinery is CPU-bound (the fake-measured floor of
  step 9: ~1.5 µs p50 at modern clocks; ~2× at half the clocks),
  and a frequency-scaled node also breaks perftest's own µs
  (it warns — "Conflicting CPU frequency values detected", every
  iteration — and under-reported the EPYC pair's wire latency by
  ~2×: 10.7 self-reported vs 21.6 measured). **Take every baseline
  by wall-clock difference (a long run minus a short one), never
  by perftest's self-reported µs, anywhere:** the fixed-clock
  L5420 pair, with no warning printed, was mis-scaled the same
  ~2× (2.28 self-reported vs 4.63 measured). Our own numbers are
  CLOCK_MONOTONIC and verified honest against an external clock
  (5.9 ± 0.3 µs real vs 6.32 reported). `cpupower
  frequency-set -g performance` (root) fixes the real cost; no
  library change can.
- **Prefer the hybrid `wait` (the default) to pure spin.** The busy
  window is the spin budget: within it a wait spins (no park cycle),
  past it the thread parks. Pure spinning (`busy_window =
  MAX` or the benchmarks' `--wait spin`) is the floor on a
  *dedicated, isolated* core only — on a general-purpose core the
  descheduler eventually preempts the spinner and the +11 µs tail
  appears; the parked waiter never pays it, so the hybrid mode's
  mean tracks its p50. Isolated cores (`isolcpus`, `nohz_full`) are
  the environment's half of making pure spin honest.
- **Trust the defaults otherwise**: the 256 µs busy window covers a
  round trip plus the gap between sequential operations, and the
  send-queue depth bound keeps submission eager. Both are knobs the
  benchmark matrix exercises (`with_busy_window`,
  `set_max_send_wr`), not values to tune per machine.

The diagnostic for all of the above: the benchmarks' latency
histograms (mean drifting above p50 = a preemption/environment tail,
not library cost; mean ≈ p90 = the machine is quiet); the operation
timeline (`RDMALIB_OP_TRACE=1` on any benchmark) splitting each op
into mailbox hop, verbs post, wire, and wake — the wire segment
against a wall-clock-difference baseline is the whole truth about
the environment; and the engine's own floor without the wire — the
`machinery_round_trip_floor` test (ignored: run it scoped to the
library binary, never with the cluster-test deploy flags) reporting
what submit → handoff → post → resolve → wake costs on that
machine's clocks, against the fake source.

## Open questions

- Engine spawn time: at construction (simple, one blocked thread per
  default engine) vs. lazily at the first `Register` (fewer idle
  threads). Hybrid idle makes both cheap; lean simple.
- Engine dedup per CPU: two controllers given `Engine::on_cpu(n)`
  share by construction; two *separately created* ones land two
  spinning threads on one core. A per-CPU registry could dedup —
  decide when someone hits it.
- Should session threads (update-wait) pin to the engine's CPU too?
- QP liveness against a stalled remote: defaults accept indefinite
  stalls; engine-side rnr/ack timeout policy is a knob to consider.
- CQ depth: dynamic since step 6 — `max(SHARED_CQ_SIZE = 1024, the
  first connection's request)`, capped by the device's `max_cqe`,
  with the live connections' depths kept under it by the lend/return
  budget. The residual knob: the 1024 floor is a fixed guess, and
  the budget is per shared queue (one per device) — a per-engine
  CQ-budget knob could follow.
- Pool sizing: `POOL_ENTRY = 128 KiB` / `POOL_SLOTS = 32` (4 MiB
  pinned per connection, created lazily) are fixed guesses — the
  copy-vs-registration crossover, per device, is measurable; knobs
  to expose once someone measures. Two known follow-on wins: inline
  data for tiny writes (no local registration nor pool slice at
  all), and a user-facing registered-buffer (zero-copy) API for
  large reused buffers — the latter done (step 11; the registered
  path skips the pool entirely, so the crossover only matters for
  callers that keep using the owned-buffer API).
- A `Send` reader-side redesign (multi-thread submission) — only if
  user demand appears.
