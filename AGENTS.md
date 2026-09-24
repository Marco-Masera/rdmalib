# AGENTS.md

## Project

`rdmalib` — one crate, Rust 2024 edition, **zero dependencies by design**: the RDMA FFI is hand-written, do not add `*-sys` crates. Layout:

- `src/lib.rs` — high-level API: client side (`RemoteMemoryProvider`), server side (`SharedMemoryRegionProvider`). Must stay free of verbs specifics.
- `src/rdma/` — low-level RDMA (`verbs.rs`: libibverbs + rdma_cm FFI). Confined to its own module so the implementation can be swapped for a different technology; `rdma/mod.rs` is the re-export seam.
- `old_cpp_lib/` — older C++ implementation of the same ideas; read for semantics (`conn.cpp`, `remote_process/ops.cpp`), but it does more (TCP sync, pre-allocated buffers) — don't copy blindly.
- `src/tests/` — unit tests (in-crate, they use private internals; not `tests/`).

## Commands

- Host fast loop (no linking, always works): `cargo check`, `cargo clippy --all-targets`.
- Run the tests: `./podman_build/link_test.sh [cargo-test-args…]` (e.g. `…/link_test.sh verbs_struct` for one test). Uses **podman, not docker**; builds/reuses the `rdmalib-dev` image from `podman_build/Containerfile`.
- The dev host has **no RDMA stack**: no librdmacm at all, a truncated runtime-only `libibverbs.so.1`, no `/dev/infiniband`, no root. Plain `cargo test` on the host fails to link — that is expected; don't try to fix it, run tests through the script.
- Hardware-dependent tests are `#[ignore]`-gated and meant for the remote HPC (`cargo test -- --ignored` there). Local and container runs must stay green without hardware.
- Edition 2024 needs rustc >= 1.85 — that's why the Containerfile installs rustup instead of the distro rustc.

## FFI rules (`src/rdma/verbs.rs`)

- No verbs headers exist on this host: verify layouts/constants against upstream rdma-core headers (`libibverbs/verbs.h`, `librdmacm/rdma_cma.h` on GitHub) instead of trusting memory.
- Known traps already hit: `ibv_post_send`/`ibv_poll_cq` are `static inline` in `verbs.h` (NOT library symbols) — they dispatch through `ibv_context->ops` (`post_send` at index 25, `poll_cq` at 11); `struct ibv_mr` has a `handle` field before `lkey`/`rkey`; `IBV_SEND_SIGNALED = 1<<1`; `RDMA_PS_TCP = 0x0106`.
- The Rust structs mirror only the leading C fields; the tests at the bottom of `verbs.rs` pin sizes/offsets with `offset_of!` — keep them in sync when touching the mirrors.
- Dead-stripping can hide link errors: an FFI typo only surfaces when a test actually calls the path.

## Design invariants (don't "simplify" these away)

- A **group id identifies a set of readers with the same access**: one (remote_addr, size, rkey) tuple per (region, group), so every reader in a group shares one rkey. Distinct groups exist so the owner can grant/revoke a set of readers at once (revocation is future work). Tuples are created lazily per (region, group) in `get_shared_mr`.
- Groups are implemented with a **shared per-group protection domain** (`rdma::ProtectionDomain`, `Listener::accept_into`): all of a group's connections create their QPs in it, so one registration per (region, group) serves every reader of the group. Verbs rkeys only work through QPs in the PD the memory was registered in — that is why the domain, not the connection, owns registrations.
- Tuple creation must **not touch the buffer** (the app may hold a `borrow_mut`): that is why registration uses `ProtectionDomain::register_addr(addr, size)` with the address captured at `register()` time, not a slice. Buffers are never resized after registration — the handle API only exposes slices.
- The remote side connects **lazily on the first read**, so `RemoteMemoryProvider::new` stays infallible and tests run without hardware.
- Drop order matters: ibv registrations (tuples) must be deregistered before their connections (PDs) are destroyed — `Drop for SharedMemoryRegionProvider` clears the tuples map first; don't reorder fields/teardown.
- Typed sharing (`register_typed`, typed handle borrows, `read_typed`) goes through the **`RemoteSafe` unsafe trait** (`src/pod.rs`): the implementer asserts every bit pattern is a valid `T` (`#[repr(C)]`, no bool/char/enums/refs — remote writers produce arbitrary bytes). Buffers are stored **type-erased** (`Rc<RefCell<Box<dyn Any>>>`, shared entry↔handle) and borrows downcast to `&[T]` — never transmute the `Vec` itself (wrong-layout dealloc). Verbs only ever sees the byte (addr, size) captured at registration. Type agreement between the separately compiled sides is the user's responsibility; reader-side typed reads still check bounds and remote-address alignment before connecting.

## Not implemented yet

TCP metadata exchange (`update()` is a no-op; the remote caches stay empty until then; it should also carry the element type/layout so typed mismatches can be detected), IPv6, write/atomic verbs, async post+poll reads, reuse of registered destination buffers for reads.
