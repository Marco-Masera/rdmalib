//! rdmalib: a Rust RDMA library.
//!
//! The low-level RDMA commands live in [`rdma`]; the high-level parts
//! are built on them. Client side (accessing remote regions):
//! [`RemoteMemoryProvider`]; server side (offering local regions to
//! remote readers): [`SharedMemoryRegionProvider`]. Regions are shared
//! as raw bytes or, without copying, as vectors of any [`RemoteSafe`]
//! type — types whose every bit pattern is valid, since remote
//! machines write arbitrary bytes into them. Reads and tuple creation
//! use the low-level layer; the metadata exchange between the two
//! sides travels over TCP — [`RemoteMemoryProvider::update`] runs a
//! group's session on the reader side, and the provider serves it
//! from a background thread ([`SharedMemoryRegionProvider::serve`]).

pub mod rdma;

mod meta;
mod pod;
mod providers;
mod readers;

pub use pod::RemoteSafe;
pub use providers::{
    SharedMemoryRegionHandle, SharedMemoryRegionMetadata, SharedMemoryRegionProvider,
    SharedMemoryRegionProviderAddr,
};
pub use readers::{
    RemoteMemoryProvider, RemoteMemoryProviderAddr, RemoteMemoryRegion,
    RemoteMemoryRegionMetadata,
};

#[cfg(test)]
mod tests;
