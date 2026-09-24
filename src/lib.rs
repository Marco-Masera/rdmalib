//! rdmalib: a Rust RDMA library.
//!
//! The low-level RDMA commands live in [`rdma`]; the high-level parts
//! are built on them. Client side (accessing remote regions):
//! [`RemoteMemoryProvider`]; server side (offering local regions to
//! remote readers): [`SharedMemoryRegionProvider`]. Regions are shared
//! as raw bytes or, without copying, as vectors of any [`RemoteSafe`]
//! type — types whose every bit pattern is valid, since remote
//! machines write arbitrary bytes into them. Reads and tuple creation
//! use the low-level layer, and the metadata exchange over TCP is
//! still to come.

pub mod rdma;

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
