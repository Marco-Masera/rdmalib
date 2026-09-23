//! rdmalib: a Rust RDMA library.
//!
//! The low-level RDMA commands live in [`rdma`]; the high-level
//! providers (client side: [`RemoteMemoryProvider`], server side:
//! [`SharedMemoryRegionProvider`]) are built on them: reads and tuple
//! creation use the low-level layer, and the metadata exchange over
//! TCP is still to come.

pub mod rdma;

use std::cell::{Cell, Ref, RefCell, RefMut};
use std::collections::HashMap;
use std::io;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::rc::Rc;

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
    connection: Rc<RefCell<LazyConnection>>,
    /// Catalog of the remote's regions, as advertised by the remote.
    metadata: RefCell<Vec<RemoteMemoryRegionMetadata>>,
    /// Active regions by (name, id); a region may expose several groups.
    /// `RefCell` allows `update` to refresh the caches through `&self`.
    regions: RefCell<HashMap<(String, u32), Vec<CachedRegion>>>,
}

/// The RDMA connection to the remote machine, established on first use.
#[derive(Debug)]
struct LazyConnection {
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
struct CachedRegion {
    remote_addr: u64,
    size: usize,
    rkey: u32,
    group: u32,
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
/// came from, established on first use.
#[derive(Debug, Clone)]
pub struct RemoteMemoryRegion {
    /// The provider's lazily connected RDMA connection.
    connection: Rc<RefCell<LazyConnection>>,
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
        let mr = connection.register(&buf[..size])?;
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

/// A memory region to be shared with remote machines.
///
/// Owns the region's buffer. Registering the metadata with a
/// [`SharedMemoryRegionProvider`] moves the buffer into the provider; a
/// handle is returned for continued access.
#[derive(Debug, Clone)]
pub struct SharedMemoryRegionMetadata {
    /// Name of the region. Names need not be unique - an ID will be set upon registering
    pub name: String,
    /// The region's buffer.
    pub buffer: Vec<u8>,
}

impl SharedMemoryRegionMetadata {
    /// Create region metadata from a name and its buffer.
    pub fn new(name: impl Into<String>, buffer: Vec<u8>) -> Self {
        Self {
            name: name.into(),
            buffer,
        }
    }
}

/// Connection parameters of this node's memory provider service.
///
/// Built by the node sharing memory and handed to
/// [`SharedMemoryRegionProvider::new`]: the RDMA port is bound when the
/// first reader is accepted, and the TCP port will be used to exchange
/// metadata with remote clients (not implemented yet).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SharedMemoryRegionProviderAddr {
    /// RDMA port the provider serves memory on.
    pub rdma_port: u16,
    /// Desired TCP port for the metadata exchange.
    pub tcp_port: u16,
}

impl SharedMemoryRegionProviderAddr {
    /// Describe this provider's RDMA and TCP ports.
    pub fn new(rdma_port: u16, tcp_port: u16) -> Self {
        Self {
            rdma_port,
            tcp_port,
        }
    }
}

/// Serves the memory regions shared with remote machines.
///
/// Constructed from a [`SharedMemoryRegionProviderAddr`]. Readers
/// connect to the RDMA port and are accepted one at a time with
/// [`Self::accept`], each getting its own group. Regions are
/// registered with [`Self::register`], which takes ownership of their
/// buffer and assigns them an id. (remote_addr, size, rkey) tuples are
/// created lazily by [`Self::get_shared_mr`], one per (region, group)
/// pair: the region's buffer is registered on the group's connection,
/// so the rkey is a real one. The TCP channel for the metadata
/// exchange does not exist yet.
#[derive(Debug)]
pub struct SharedMemoryRegionProvider {
    addr: SharedMemoryRegionProviderAddr,
    /// Tuples by (region id, group): the registration of each region's
    /// buffer on a reader's connection, kept alive until the provider
    /// is dropped.
    tuples: RefCell<HashMap<(u32, u32), rdma::MemoryRegion>>,
    /// Registered regions by id. This lock guards only the catalog:
    /// buffers live in per-region RefCells shared with the handles, so a
    /// buffer can be borrowed while the provider keeps working.
    regions: RefCell<HashMap<u32, RegionEntry>>,
    /// Accepted reader connections by group id.
    connections: RefCell<HashMap<u32, Rc<rdma::Connection>>>,
    /// The listener, bound on the first [`Self::accept`].
    listener: RefCell<Option<rdma::Listener>>,
    next_id: Cell<u32>,
    next_group: Cell<u32>,
}

/// Handle to a region registered with a [`SharedMemoryRegionProvider`].
///
/// Grants read and write access to the region's buffer via [`Self::borrow`]
/// and [`Self::borrow_mut`]. The usual `RefCell` rule applies: only one
/// borrow of a given buffer can be active at a time. The provider keeps
/// working while a buffer is borrowed.
#[derive(Debug)]
pub struct SharedMemoryRegionHandle {
    id: u32,
    /// Shared with the provider's region entry; keeps the buffer alive.
    buffer: Rc<RefCell<Vec<u8>>>,
}

/// Internal bookkeeping for one registered region.
#[derive(Debug)]
struct RegionEntry {
    // Exposed once the metadata exchange is implemented.
    #[allow(dead_code)]
    name: String,
    /// Keeps the provider's ownership of the buffer alive; never read
    /// directly. The buffer is never resized after registration, so
    /// the address captured below stays valid.
    #[allow(dead_code)]
    buffer: Rc<RefCell<Vec<u8>>>,
    /// Captured at registration so that tuples can be created later
    /// without touching the buffer: the application may hold a borrow of
    /// it at that moment.
    remote_addr: u64,
    /// Size of the region's buffer, in bytes.
    size: usize,
}

impl SharedMemoryRegionProvider {
    /// Create a provider serving memory over `addr`'s RDMA port and
    /// exchanging metadata over its TCP port.
    pub fn new(addr: SharedMemoryRegionProviderAddr) -> Self {
        Self {
            addr,
            tuples: RefCell::new(HashMap::new()),
            regions: RefCell::new(HashMap::new()),
            connections: RefCell::new(HashMap::new()),
            listener: RefCell::new(None),
            next_id: Cell::new(0),
            next_group: Cell::new(0),
        }
    }

    /// Accept a connection from a remote reader, and assign it a fresh
    /// group id.
    ///
    /// Blocks until a reader connects. The listener is bound on the
    /// first call, on the provider's RDMA port with a wildcard address
    /// (across the node's RDMA devices). The returned group identifies
    /// the reader in [`Self::get_shared_mr`] requests.
    ///
    /// TODO: once the metadata exchange is implemented, readers arrive
    /// with their metadata requests over the TCP channel.
    pub fn accept(&self) -> io::Result<u32> {
        if self.listener.borrow().is_none() {
            let listener = rdma::Listener::bind(SocketAddrV4::new(
                Ipv4Addr::UNSPECIFIED,
                self.addr.rdma_port,
            ))?;
            *self.listener.borrow_mut() = Some(listener);
        }
        let connection = self
            .listener
            .borrow()
            .as_ref()
            .expect("the listener is bound")
            .accept()?;
        let group = self.next_group.get();
        self.next_group.set(group + 1);
        self.connections
            .borrow_mut()
            .insert(group, Rc::new(connection));
        Ok(group)
    }

    /// Take ownership of `metadata` and assign the region a fresh id.
    ///
    /// Returns a handle for continued access to the buffer; the assigned
    /// id is available as [`SharedMemoryRegionHandle::id`]. No tuples are
    /// created here: they materialize on the first
    /// [`Self::get_shared_mr`] request for each (region, group) pair.
    pub fn register(&self, metadata: SharedMemoryRegionMetadata) -> SharedMemoryRegionHandle {
        let id = self.next_id.get();
        self.next_id.set(id + 1);

        let buffer = Rc::new(RefCell::new(metadata.buffer));
        let (remote_addr, size) = {
            let borrowed = buffer.borrow();
            (borrowed.as_ptr() as u64, borrowed.len())
        };

        let handle = SharedMemoryRegionHandle {
            id,
            buffer: Rc::clone(&buffer),
        };
        self.regions.borrow_mut().insert(
            id,
            RegionEntry {
                name: metadata.name,
                buffer,
                remote_addr,
                size,
            },
        );
        handle
    }

    /// The (remote_addr, size, rkey) tuple of region `id` for `group`,
    /// created on first request.
    ///
    /// Creating a tuple registers the region's buffer on the group's
    /// connection: the rkey is a real one, usable by that reader, and
    /// the registration is kept alive for as long as the provider is.
    /// Repeated requests for the same region and group return the same
    /// tuple; registration errors are reported as `Err`; unknown region
    /// ids or groups return `Ok(None)`.
    ///
    /// This is the data that will be served through the TCP channel.
    /// The buffer is never touched here: its address and size were
    /// captured at registration time, so the application may hold a
    /// borrow of the buffer while tuples are created and served.
    pub fn get_shared_mr(&self, id: u32, group: u32) -> io::Result<Option<(u64, usize, u32)>> {
        if let Some(mr) = self.tuples.borrow().get(&(id, group)) {
            return Ok(Some((mr.addr(), mr.size(), mr.rkey())));
        }

        let Some(connection) = self.connections.borrow().get(&group).cloned() else {
            return Ok(None);
        };
        let Some((remote_addr, size)) = self
            .regions
            .borrow()
            .get(&id)
            .map(|entry| (entry.remote_addr, entry.size))
        else {
            return Ok(None);
        };

        let mr = connection.register_addr(remote_addr, size)?;
        let tuple = (mr.addr(), mr.size(), mr.rkey());
        self.tuples.borrow_mut().insert((id, group), mr);
        Ok(Some(tuple))
    }
}

impl Drop for SharedMemoryRegionProvider {
    /// Deregister the tuples while the buffers they pin and the
    /// connections (whose protection domains the registrations belong
    /// to) are still alive.
    fn drop(&mut self) {
        self.tuples.borrow_mut().clear();
    }
}

impl SharedMemoryRegionHandle {
    /// The id the provider assigned to this region.
    pub fn id(&self) -> u32 {
        self.id
    }

    /// Immutably borrow the buffer.
    pub fn borrow(&self) -> Ref<'_, [u8]> {
        Ref::map(self.buffer.borrow(), Vec::as_slice)
    }

    /// Mutably borrow the buffer to read and modify the region's data.
    pub fn borrow_mut(&self) -> RefMut<'_, [u8]> {
        RefMut::map(self.buffer.borrow_mut(), Vec::as_mut_slice)
    }
}

#[cfg(test)]
mod tests;
