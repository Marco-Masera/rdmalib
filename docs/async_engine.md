# Async operations engine — design

Status: agreed design; implementation in the step order below. Steps
0–5 are done: the affinity FFI; the provider service-thread pinning;
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
before the pool.

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
6. **Hybrid polling is mandatory.** A bounded busy-poll window while
   work is pending (this is where the pinned-CPU latency win lives),
   then fd-blocking when idle — otherwise every default engine would
   burn a core.
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
  checks → allocate a slab slot `(index, generation)` → move the buffer
  into the slot → push `Post` → wake the pipe.
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

### Poll loop

- Round-robin sweep: each registered CQ in rotation, bounded batch per
  CQ per sweep; process completions; sweep again.
- While slots are pending: busy-poll within a bounded window
  (configurable; the dedicated-core/HPC case can ask for pure busy).
- Idle: hybrid fd-block —
  1. arm every CQ (`ibv_req_notify_cq`) **before** blocking, or the
     engine sleeps on a completion it already holds;
  2. `poll(2)` on [wake-pipe read end, per-connection comp-channel
     fds];
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
// engine thread still does the waiting work; this thread parks).
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
- CQ depth: `SHARED_CQ_SIZE = 256` — one shared queue per (engine,
  device) now bounds the outstanding ops of every connection on the
  device at once; a real sizing knob and an overrun policy remain
  open.
- Pool sizing: `POOL_ENTRY = 128 KiB` / `POOL_SLOTS = 32` (4 MiB
  pinned per connection, created lazily) are fixed guesses — the
  copy-vs-registration crossover, per device, is measurable; knobs
  to expose once someone measures. Two known follow-on wins: inline
  data for tiny writes (no local registration nor pool slice at
  all), and a user-facing registered-buffer API (zero-copy, the
  old C++ library's model) for large reused buffers.
- A `Send` reader-side redesign (multi-thread submission) — only if
  user demand appears.
