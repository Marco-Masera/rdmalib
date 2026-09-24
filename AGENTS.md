# AGENTS.md

## Project

`rdmalib` — one crate, Rust 2024 edition, **zero dependencies by design**: the RDMA FFI is hand-written, do not add `*-sys` crates. Layout:

- `src/lib.rs` — high-level API: client side (`RemoteMemoryProvider`), server side (`SharedMemoryRegionProvider`). Must stay free of verbs specifics.
- `src/meta.rs` — the metadata channel: the TCP wire protocol (framing + messages) and `Channel`, plain `std::net` — the zero-dependency TCP choice, deliberate. No RDMA knowledge; both high-level sides drive it.
- `src/rdma/` — low-level RDMA (`verbs.rs`: libibverbs + rdma_cm FFI). Confined to its own module so the implementation can be swapped for a different technology; `rdma/mod.rs` is the re-export seam.
- `old_cpp_lib/` — older C++ implementation of the same ideas; read for semantics (`conn.cpp`, `remote_process/ops.cpp`), but it does more (TCP sync, pre-allocated buffers) — don't copy blindly.
- `src/tests/` — unit tests (in-crate, they use private internals; not `tests/`). **Agent-owned**: run them to verify changes, modify them freely — they work locally, without RDMA hardware.
- `tests/` — cluster integration tests (public API only), deployed to real nodes by `scripts/test_runner.py` (see below). `tests/common/mod.rs` is the shared helper module, `tests/_test_config.json` the machine-specific cluster config. **User-owned**: do not execute or modify them unless explicitly asked.

## Commands

- Host fast loop (no linking, always works): `cargo check`, `cargo clippy --all-targets`.
- Run the unit tests (`src/tests/`): `./podman_build/link_test.sh [cargo-test-args…]` (e.g. `…/link_test.sh verbs_struct` for one test). Uses **podman, not docker**; builds/reuses the `rdmalib-dev` image from `podman_build/Containerfile`.
- The dev host has **no RDMA stack**: no librdmacm at all, a truncated runtime-only `libibverbs.so.1`, no `/dev/infiniband`, no root. Plain `cargo test` on the host fails to link — that is expected; don't try to fix it, run tests through the script.
- Hardware-dependent tests are `#[ignore]`-gated and meant for the remote HPC (`cargo test -- --ignored` there). Local and container runs must stay green without hardware.
- Edition 2024 needs rustc >= 1.85 — that's why the Containerfile installs rustup instead of the distro rustc.

## Cluster integration tests (`tests/`)

- **Ownership**: these are the user's tests. Do not execute them — never forward `--ignored`/`--include-ignored`, that is the runner's deploy trigger — and do not modify anything under `tests/` unless explicitly asked. Verify changes with the `src/tests/` unit tests (`link_test.sh` without the flag) instead.

- `.cargo/config.toml` routes **every** test binary through `scripts/test_runner.py`. A binary listed in `tests/_test_config.json` (key = test name, e.g. `read_test`) is a cluster test: the runner rsyncs the binary to that test's nodes and launches it on all of them concurrently over ssh, streaming per-node output. Everything else — the lib unit tests, or a cluster test when no `--ignored`/`--include-ignored` flag was forwarded — is exec'd locally, untouched: that is what keeps plain `cargo test` / `link_test.sh` green (and working at all) on machines without a cluster.
- Cluster tests are `#[ignore]`-gated (they need RDMA hardware); run one via `./podman_build/link_test.sh --test read_test -- --ignored` — the flag both selects them and is the runner's deploy trigger. In that mode the container is only the *builder* (`cargo test --no-run`, artifacts persisted under `target-cluster/`, gitignored) and `scripts/test_runner.py` then runs **on the host**: the container has no ssh config/keys for the node hostnames, the host does. Without `--test <name>`, every cluster test in the config is deployed.
- The runner passes the layout to each remote process via env vars: `RDMALIB_TEST_NODES` = comma-separated `ip:tcp_port:rdma_port` (one per deployed node, ports = `base_ports` + position, IPv4 only) and `RDMALIB_TEST_MY_NODE` = that process's index into the list. `tests/common/mod.rs` parses them (`TestCluster::from_env()`) and builds provider/reader addresses per node. Deployed runs get `--nocapture` appended automatically: libtest otherwise captures (and drops) the stdout of passing tests, and the whole point of deploying is streaming the nodes' output.

## FFI rules (`src/rdma/verbs.rs`)

- No verbs headers exist on this host: verify layouts/constants against upstream rdma-core headers (`libibverbs/verbs.h`, `librdmacm/rdma_cma.h` on GitHub) instead of trusting memory.
- Known traps already hit: `ibv_post_send`/`ibv_poll_cq` are `static inline` in `verbs.h` (NOT library symbols) — they dispatch through `ibv_context->ops` (`post_send` at index 25, `poll_cq` at 11); `struct ibv_mr` has a `handle` field before `lkey`/`rkey`; `IBV_SEND_SIGNALED = 1<<1`; `RDMA_PS_TCP = 0x0106`.
- The Rust structs mirror only the leading C fields; the tests at the bottom of `verbs.rs` pin sizes/offsets with `offset_of!` — keep them in sync when touching the mirrors.
- Dead-stripping can hide link errors: an FFI typo only surfaces when a test actually calls the path.

## Design invariants (don't "simplify" these away)

- A **group id identifies a set of readers with the same access**: one (remote_addr, size, rkey) tuple per (region, group), so every reader in a group shares one rkey. Distinct groups exist so the owner can grant/revoke a set of readers at once (revocation is future work). Tuples are created lazily per (region, group) in `get_shared_mr`.
- Groups are implemented with a **shared per-group protection domain** (`rdma::ProtectionDomain`, `Listener::accept_into`): all of a group's connections create their QPs in it, so one registration per (region, group) serves every reader of the group. Verbs rkeys only work through QPs in the PD the memory was registered in — that is why the domain, not the connection, owns registrations.
- Tuple creation must **not touch the buffer** (the app may hold a `borrow_mut`): that is why registration uses `ProtectionDomain::register_addr(addr, size)` with the address captured at `register()` time, not a slice. Buffers are never resized after registration — the handle API only exposes slices. The provider's state is split accordingly: `Shared` (catalog of plain `RegionInfo` data + groups + tuples, `Arc<Mutex<…>>`) is shared with the service thread, while the buffer owners (`owners`, `Rc`) stay confined to the application thread.
- A **session** pairs one TCP connection with one RDMA connection: reader sends `Hello`, then `WantGroup { group }`, then connects over RDMA; the provider accepts it into the group's PD (creating the group on first use) and replies with the catalog + the group's tuples. Group membership is **reader-declared** (v1; owner-side policy/revocation is future work). The reader keeps the TCP channel open; `update(group)` re-requests over it, or opens a fresh session if the exchange failed. Each group has its **own RDMA connection** on the reader side (rkeys only work through QPs in the group's PD — a single shared connection was a latent multi-group bug).
- The remote side connects **lazily on the first `update(group)`**, so `RemoteMemoryProvider::new` stays infallible and tests run without hardware; region reads go through the group's connection slot and fail (not panic) when no session has run.
- Threading stance: the verbs handles are `Send` (not `Sync`; `ProtectionDomain` moved from `Rc` to `Arc` and `PdInner` is `Send + Sync` with a documented safety argument) so `Shared` can cross threads; the `Arc<Mutex>` locks are taken one at a time, **never nested**. The service thread owns both listeners and runs the **rendezvous serially** (an `rdma_cm` listener cannot be shared across session threads without connection-pairing logic); each open session's update-wait runs on its own thread. This is also what keeps future CPU pinning viable: all library work stays on library-owned threads.
- Drop order matters: `Drop for SharedMemoryRegionProvider` stops the service first (shutdown flag + join — the accept loop is nonblocking/polled because closing a listener does not wake a blocked accept on Linux), then clears the tuples map, so ibv registrations are deregistered before their connections (PDs) are destroyed; don't reorder fields/teardown.
- Typed sharing (`register_typed`, typed handle borrows, `read_typed`) goes through the **`RemoteSafe` unsafe trait** (`src/pod.rs`): the implementer asserts every bit pattern is a valid `T` (`#[repr(C)]`, no bool/char/enums/refs — remote writers produce arbitrary bytes). Buffers are stored **type-erased** (`Rc<RefCell<Box<dyn Any>>>`, shared entry↔handle) and borrows downcast to `&[T]` — never transmute the `Vec` itself (wrong-layout dealloc). Verbs only ever sees the byte (addr, size) captured at registration. The element layout (size, alignment, type name) travels with the catalog and `read_typed` rejects a mismatched `T` — a **layout** check only; type identity between the separately compiled sides remains the user's responsibility. Reader-side typed reads also check bounds and remote-address alignment before the device is touched.

## Not implemented yet

Revocation (groups exist for it; reserved message ids in `src/meta.rs` leave room) and any owner-side group policy (membership is reader-declared), a timeout on the RDMA accept inside the session rendezvous (a reader that sends `WantGroup` but never connects stalls later readers' rendezvous), reaping of dead reader connections from a group's `connections`, IPv6, write/atomic verbs, async post+poll reads, reuse of registered destination buffers for reads.
