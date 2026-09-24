//! Client side: handles to memory regions exported by a remote machine.
//!
//! [`RemoteMemoryProvider`] connects to a remote
//! [`SharedMemoryRegionProvider`](crate::SharedMemoryRegionProvider) and
//! hands out [`RemoteMemoryRegion`]s to read from. The metadata exchange
//! over TCP is not implemented yet.

use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::rc::Rc;

use crate::pod::zeroed_vec;
use crate::rdma;
use crate::RemoteSafe;

/// Data used to reach a remote machine running a memory provider.
///
/// Built by the client and handed to [`RemoteMemoryProvider::new`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct RemoteMemoryProviderAddr {
    /// Address of the remote machine (IP or hostname).
    pub address: String,
    /// RDMA port the remote memory provider serves memory on.
    pub rdma_port: u16,
    /// TCP port the remote uses for the metadata exchange.
    pub tcp_port: u16,
}

impl RemoteMemoryProviderAddr {
    /// Describe a remote memory provider by address and ports.
    pub fn new(address: impl Into<String>, rdma_port: u16, tcp_port: u16) -> Self {
        Self {
            address: address.into(),
            rdma_port,
            tcp_port,
        }
    }
}

/// Handle to the memory regions exported by a remote machine.
///
/// Constructed from a [`RemoteMemoryProviderAddr`]. The RDMA connection
/// to the remote is established on the first read; the metadata
/// exchange over the TCP channel is not implemented yet, so the region
/// caches stay empty until then.
#[derive(Debug)]
pub struct RemoteMemoryProvider {
    /// The lazily established RDMA connection, shared with the regions
    /// handed out by [`Self::get_remote_mr`].
    pub(crate) connection: Rc<RefCell<LazyConnection>>,
    /// Catalog of the remote's regions, as advertised by the remote.
    pub(crate) metadata: RefCell<Vec<RemoteMemoryRegionMetadata>>,
    /// Active regions by (name, id); a region may expose several groups.
    /// `RefCell` allows `update` to refresh the caches through `&self`.
    pub(crate) regions: RefCell<HashMap<(String, u32), Vec<CachedRegion>>>,
}

/// The RDMA connection to the remote machine, established on first use.
#[derive(Debug)]
pub(crate) struct LazyConnection {
    /// Where to connect to.
    addr: RemoteMemoryProviderAddr,
    connection: Option<rdma::Connection>,
}

impl LazyConnection {
    /// The connection, connecting on first use: the RDMA port carries
    /// the memory traffic.
    fn get(&mut self) -> io::Result<&rdma::Connection> {
        if self.connection.is_none() {
            self.connection = Some(rdma::Connection::connect((
                self.addr.address.as_str(),
                self.addr.rdma_port,
            ))?);
        }
        Ok(self.connection.as_ref().expect("just connected"))
    }
}

/// A region's (remote_addr, size, rkey) tuple for one group, as cached
/// from the metadata exchange.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CachedRegion {
    pub(crate) remote_addr: u64,
    pub(crate) size: usize,
    pub(crate) rkey: u32,
    pub(crate) group: u32,
}

/// Metadata describing a memory region exported by a remote machine.
///
/// Returned by [`RemoteMemoryProvider::get_remote_mr_metadata`]; pass it
/// back to [`RemoteMemoryProvider::get_remote_mr`] to obtain the active
/// region.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RemoteMemoryRegionMetadata {
    /// Human-readable name of the region.
    pub name: String,
    /// Identifier of the region on the remote machine.
    pub id: u32,
    /// Size of the region, in bytes.
    pub size: usize,
}

/// An active memory region on the remote machine, as returned by
/// [`RemoteMemoryProvider::get_remote_mr`].
///
/// Reads go through the RDMA connection of the provider the region
/// came from, established on first use — as bytes ([`Self::read`],
/// [`Self::read_into`]) or as elements of any [`RemoteSafe`] type
/// ([`Self::read_typed`], [`Self::read_into_typed`]), provided that is
/// the type the sharing side registered the region as.
#[derive(Debug, Clone)]
pub struct RemoteMemoryRegion {
    /// The provider's lazily connected RDMA connection.
    pub(crate) connection: Rc<RefCell<LazyConnection>>,
    /// Address of the region in the remote machine's address space.
    pub remote_addr: u64,
    /// Size of the region, in bytes.
    pub size: usize,
    /// Remote key required to access the region with RDMA.
    pub rkey: u32,
    /// Group id this region belongs to (the group used at lookup time).
    pub group: u32,
}

impl RemoteMemoryRegion {
    /// Read `size` bytes starting at `offset` from the remote region,
    /// allocating and returning a new buffer with the data.
    ///
    /// Connects to the remote machine on first use. The read must stay
    /// within the region: `offset + size` may not exceed the region's
    /// size.
    ///
    /// TODO: every read registers and deregisters its buffer; reuse
    /// registered memory once performance matters.
    pub fn read(&self, offset: u64, size: usize) -> io::Result<Vec<u8>> {
        let mut buffer = vec![0u8; size];
        self.read_into(offset, size, &mut buffer)?;
        Ok(buffer)
    }

    /// Read `size` bytes starting at `offset` into the start of `buf`,
    /// overwriting its first `size` bytes without allocating new memory.
    ///
    /// Connects to the remote machine on first use. `size` may not
    /// exceed `buf.len()`, and the read must stay within the region.
    pub fn read_into(&self, offset: u64, size: usize, buf: &mut [u8]) -> io::Result<()> {
        if size > buf.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("read of {size} bytes exceeds the buffer of {} bytes", buf.len()),
            ));
        }
        if offset.saturating_add(size as u64) > self.size as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "read of {size} bytes at offset {offset} exceeds the region of {} bytes",
                    self.size
                ),
            ));
        }

        let mut slot = self.connection.borrow_mut();
        let connection = slot.get()?;
        let mr = connection.register_addr(buf.as_mut_ptr() as u64, size)?;
        connection.read(&mr, self.remote_addr + offset, self.rkey, size)
    }

    /// Read `count` elements starting at byte `offset` from the remote
    /// region, allocating and returning a new `Vec` of them.
    ///
    /// `T` must be the element type the sharing side registered the
    /// region as; the compiler cannot enforce agreement between
    /// separately compiled applications, so a mismatched `T` reads
    /// garbage without failing. The read must stay within the region,
    /// and `offset` must be a multiple of `T`'s alignment. Connects to
    /// the remote machine on first use.
    ///
    /// TODO: every read registers and deregisters its buffer; reuse
    /// registered memory once performance matters.
    pub fn read_typed<T: RemoteSafe>(&self, offset: u64, count: usize) -> io::Result<Vec<T>> {
        let mut buffer = zeroed_vec(count);
        self.read_into_typed(offset, &mut buffer)?;
        Ok(buffer)
    }

    /// Read `buf.len()` elements starting at byte `offset` into `buf`,
    /// without allocating new memory.
    ///
    /// The same `T`, bounds, and alignment rules as
    /// [`Self::read_typed`]; a partial read is a shorter `buf` slice.
    pub fn read_into_typed<T: RemoteSafe>(&self, offset: u64, buf: &mut [T]) -> io::Result<()> {
        let size = size_of_val(buf);
        if offset.saturating_add(size as u64) > self.size as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "read of {} elements of {} bytes at offset {offset} exceeds the region of {} bytes",
                    buf.len(),
                    size_of::<T>(),
                    self.size
                ),
            ));
        }
        if !(self.remote_addr + offset).is_multiple_of(align_of::<T>() as u64) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "read at offset {offset} is misaligned for elements of alignment {}",
                    align_of::<T>()
                ),
            ));
        }

        let mut slot = self.connection.borrow_mut();
        let connection = slot.get()?;
        let mr = connection.register_addr(buf.as_mut_ptr() as u64, size)?;
        connection.read(&mr, self.remote_addr + offset, self.rkey, size)
    }
}

impl RemoteMemoryProvider {
    /// Prepare a provider for the remote machine described by `addr`.
    ///
    /// The RDMA connection is established on the first read; the
    /// initial metadata exchange (over TCP) is not implemented yet.
    pub fn new(addr: RemoteMemoryProviderAddr) -> Self {
        Self {
            connection: Rc::new(RefCell::new(LazyConnection {
                addr,
                connection: None,
            })),
            metadata: RefCell::new(Vec::new()),
            regions: RefCell::new(HashMap::new()),
        }
    }

    /// Metadata of the remote memory regions known to this provider.
    pub fn get_remote_mr_metadata(&self) -> Vec<RemoteMemoryRegionMetadata> {
        self.metadata.borrow().clone()
    }

    /// Refresh the region metadata from the remote machine.
    ///
    /// TODO: re-query the remote and replace the cached metadata.
    pub fn update(&self) {
        // Connection not implemented yet; keep the current metadata.
    }

    /// Look up a remote memory region by its metadata.
    ///
    /// `group` selects a specific group; `None` picks any available region
    /// (currently the first known one). The returned region reports the
    /// group id that was used.
    pub fn get_remote_mr(
        &self,
        metadata: &RemoteMemoryRegionMetadata,
        group: Option<u32>,
    ) -> Option<RemoteMemoryRegion> {
        let regions = self.regions.borrow();
        let cached = regions
            .get(&(metadata.name.clone(), metadata.id))?
            .iter()
            .find(|region| group.is_none_or(|g| region.group == g))?;
        Some(RemoteMemoryRegion {
            connection: Rc::clone(&self.connection),
            remote_addr: cached.remote_addr,
            size: cached.size,
            rkey: cached.rkey,
            group: cached.group,
        })
    }
}
