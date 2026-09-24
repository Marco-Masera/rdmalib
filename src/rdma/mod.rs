//! Low-level RDMA commands.
//!
//! The primitive operations the rest of the library is built on:
//!
//! - connection creation, via [`Connection::connect`] (reader side)
//!   and [`Listener::bind`] + [`Listener::accept`] /
//!   [`Listener::accept_into`] (provider side; connections sharing a
//!   [`ProtectionDomain`] share the rkeys of the memory registered in
//!   it, which is how a group of readers gets shared access);
//! - tuple creation, via [`ProtectionDomain::register`] (or
//!   [`Connection::register`] on a connection's domain), which
//!   registers a local buffer for remote access and produces the
//!   (remote_addr, size, rkey) tuple remote readers need;
//! - one-sided reads, via [`Connection::read`].
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
//! // (In practice the tuple travels over the metadata channel, which
//! // is not implemented yet.)
//! let conn = Connection::connect("10.0.0.1:18515").unwrap();
//! let dst = vec![0u8; 4096];
//! let dst_mr = conn.register(&dst).unwrap();
//! conn.read(&dst_mr, tuple.0, tuple.2, 4096).unwrap();
//! ```

mod verbs;

pub use verbs::{Connection, Listener, MemoryRegion, ProtectionDomain};
