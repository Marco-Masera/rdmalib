//! Low-level RDMA commands.
//!
//! The primitive operations the rest of the library is built on:
//!
//! - connection creation, via [`Connection::connect`] (reader side)
//!   and [`Listener::bind`] + [`Listener::accept`] /
//!   [`Listener::accept_into`] (provider side; connections sharing a
//!   [`ProtectionDomain`] share the rkeys of the memory registered in
//!   it, which is how a group of readers gets shared access);
//! - tuple creation, via [`ProtectionDomain::register`] /
//!   [`ProtectionDomain::register_addr`], which registers a local
//!   buffer for remote access and produces the (remote_addr, size,
//!   rkey) tuple remote readers need;
//! - shared completion queues, via [`SharedCompletions`] and
//!   [`Connection::connect_shared`]: one queue per (engine, device),
//!   every connection of the device posting into it, so the
//!   operations engine drains them all from one poll site;
//!   and
//! - the connection's pooled registration (private to
//!   `Connection::submit`): one allocation, registered once, its
//!   slices lent to small operations — the engine's in-flight holds
//!   — instead of a registration per operation.
//!
//! One-sided reads and writes are not commands of a connection
//! anymore: they run through the operations engine
//! ([`crate::Engine`], see `docs/async_engine.md`), which owns every
//! connection registered with it and drives it through the engine's
//! connection seam. The high-level reader side
//! ([`crate::RemoteMemoryProvider`], [`crate::RemoteMemoryRegion`])
//! does that driving; this module is the setup half — connections,
//! domains, and tuples.
//!
//! The implementation is technology-specific and confined to a single
//! module; the current one is built on libibverbs and rdma_cm.
//! Supporting a different technology means writing a sibling module
//! and re-exporting it from here, leaving the rest of the library
//! untouched.
//!
//! # Example
//!
//! ```no_run
//! use rdmalib::rdma::{Connection, Listener};
//!
//! // Provider: listen for readers and share a buffer's tuple.
//! let listener = Listener::bind("10.0.0.1:18515").unwrap();
//! let conn = listener.accept().unwrap();
//! let buffer = vec![0u8; 4096];
//! let pd = conn.protection_domain();
//! let mr = pd.register(&buffer).unwrap();
//! let tuple = mr.tuple(); // (remote_addr, size, rkey) for the reader
//!
//! // Reader: connect — then operate through the engine (the
//! // high-level reader side wires this; in practice the tuple
//! // travels over the metadata channel, and the operations are
//! // `RemoteMemoryRegion`'s async reads and writes).
//! let conn = Connection::connect("10.0.0.1:18515").unwrap();
//! ```

mod verbs;

pub use verbs::{Connection, Listener, MemoryRegion, ProtectionDomain, SharedCompletions};
