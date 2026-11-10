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
//!   [`ProtectionDomain::register_addr`] (or the [`Connection`]
//!   equivalents), which registers a local buffer for remote access
//!   and produces the (remote_addr, size, rkey) tuple remote readers
//!   need;
//! - one-sided reads and writes, via [`Connection::read`] and
//!   [`Connection::write`] — and [`Connection::write_inline`], which
//!   posts small writes straight out of the caller's memory, no
//!   registration needed, within the budget the device granted the
//!   connection ([`Connection::inline_cap`]);
//! - revocable remote access, via [`ProtectionDomain::alloc_mw`] and
//!   [`Connection::bind_mw`] / [`Connection::unbind_mw`]: a type 1
//!   [`MemoryWindow`] binds over registered memory under a fresh
//!   rkey, and unbinding (a zero-length bind) revokes the grant —
//!   from the unbind's completion, no access under the previous
//!   binding is served, in-flight requests included. A later bind
//!   re-grants under another fresh rkey, usable from any queue pair
//!   of the domain. The rkey is served only after the bind's
//!   completion, per the verbs contract.
//!
//! Failed completions are classified: a remote access violation
//! ([`is_remote_access`] — the failure a revoked access reads as),
//! the remote operation error ([`is_remote_op`] — the same
//! revocation aborting an operation it had already admitted), and
//! a flush ([`is_flushed`] — the failed queue pair's goodbye) can
//! be told apart from everything else, which is what a consumer
//! acting on permissions needs.
//!
//! Registrations keep their protection domain alive (they hold a
//! handle of it), so a [`MemoryRegion`] may outlive the connection it
//! was registered through — its keys keep working through the
//! domain's other connections, and its deregistration never races
//! the domain's destruction.
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
//! let mr = conn.register(&buffer).unwrap();
//! let tuple = mr.tuple(); // (remote_addr, size, rkey) for the reader
//!
//! // Reader: connect and read into a locally registered buffer.
//! // (In practice the tuple travels over the metadata channel — see
//! // the high-level API.)
//! let conn = Connection::connect("10.0.0.1:18515").unwrap();
//! let dst = vec![0u8; 4096];
//! let dst_mr = conn.register(&dst).unwrap();
//! conn.read(&dst_mr, tuple.0, tuple.2, 4096).unwrap();
//!
//! // Writing back works the same way, source buffer first.
//! let src = vec![0u8; 4096];
//! let src_mr = conn.register(&src).unwrap();
//! conn.write(&src_mr, tuple.0, tuple.2, 4096).unwrap();
//! ```

mod verbs;

pub use verbs::{
    CompletionError, CompletionStatus, Connection, Listener, MemoryRegion, MemoryWindow,
    ProtectionDomain, is_flushed, is_remote_access, is_remote_op,
};
pub(crate) use verbs::poll_readable;
