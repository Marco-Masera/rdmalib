//! rdmalib: a Rust RDMA library.
//!
//! The low-level RDMA commands live in [`rdma`]; the high-level parts
//! are built on them. Client side (accessing remote regions):
//! [`RemoteMemorySession`]; server side (offering local regions to
//! remote readers): [`SharedMemoryProvider`]. Regions are shared
//! as raw bytes or, without copying, as vectors of any [`RemoteSafe`]
//! type — types whose every bit pattern is valid, since remote
//! machines write arbitrary bytes into them. Reads, writes, and tuple
//! creation use the low-level layer; the metadata exchange between the
//! two sides travels over TCP — [`RemoteMemorySession::join_and_fetch`]
//! runs a group's session on the reader side, and the provider serves it
//! from a background thread ([`SharedMemoryProvider::serve`]).

pub mod rdma;

mod clients;
mod meta;
mod pod;
mod providers;

pub use clients::{
    Catalog, GetRegionError, RegionNotFound, RegisteredBuffer, RemoteMemoryProviderAddr,
    RemoteMemoryRegion, RemoteMemoryRegionMetadata, RemoteMemorySession,
};
pub use pod::RemoteSafe;
pub use providers::{
    SharedMemoryProvider, SharedMemoryProviderAddr, SharedMemoryRegionHandle,
    SharedMemoryRegionMetadata,
};
pub use rdma::{is_flushed, is_remote_access, is_remote_op};

#[cfg(test)]
mod tests;
