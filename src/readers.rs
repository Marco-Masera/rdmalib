//! Client side: handles to memory regions exported by a remote machine.
//!
//! [`RemoteMemoryProvider`] connects to a remote
//! [`SharedMemoryRegionProvider`](crate::SharedMemoryRegionProvider)
//! and hands out [`RemoteMemoryRegion`]s to read from and write to.
//! [`RemoteMemoryProvider::update`] runs a group's session over the
//! metadata channel — greeting, joining the group, connecting over
//! RDMA (which the remote accepts into the group's protection
//! domain), and receiving the region catalog and the group's tuples.

use std::any::type_name;
use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::mem::{align_of, size_of};
use std::rc::Rc;

use crate::meta::{Channel, Message, TupleDesc, PROTOCOL_VERSION};
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
/// Constructed from a [`RemoteMemoryProviderAddr`];
/// [`Self::update`] runs a group's session with the remote — the RDMA
/// connection of the group is established as part of it, so nothing
/// connects until the first update, keeping construction infallible.
#[derive(Debug)]
pub struct RemoteMemoryProvider {
    /// Where to reach the remote machine.
    addr: RemoteMemoryProviderAddr,
    /// Sessions by group, established by [`Self::update`]: the
    /// metadata channel, kept open for re-requests, and the group's
    /// RDMA connection.
    pub(crate) sessions: RefCell<HashMap<u32, GroupSession>>,
    /// Catalog of the remote's regions, as advertised by the remote.
    pub(crate) metadata: RefCell<Vec<RemoteMemoryRegionMetadata>>,
    /// Active regions by (name, id); a region may expose several
    /// groups.
    pub(crate) regions: RefCell<HashMap<(String, u32), Vec<CachedRegion>>>,
}

/// The reader's session with one group of the remote: the metadata
/// channel (re-requesting the catalog and the tuples is its whole
/// job) and the group's RDMA connection, shared with the regions
/// handed out for the group.
#[derive(Debug)]
pub(crate) struct GroupSession {
    /// The metadata channel, connected by the group's first
    /// [`RemoteMemoryProvider::update`]; dropped when an exchange
    /// fails, so the next update opens a fresh session.
    pub(crate) channel: Option<Channel>,
    /// The group's RDMA connection, established by the session's
    /// rendezvous; the group's regions read through it.
    pub(crate) connection: Rc<RefCell<GroupConnection>>,
}

/// The RDMA connection of one group: established by the group's
/// session, read by the group's regions.
#[derive(Debug)]
pub(crate) struct GroupConnection {
    pub(crate) connection: Option<rdma::Connection>,
}

impl GroupConnection {
    /// A slot no session has filled yet: reads through it fail — a
    /// successful [`RemoteMemoryProvider::update`] of the group is
    /// needed first.
    pub(crate) fn empty() -> Self {
        Self { connection: None }
    }

    /// The connection, or an error explaining why there is none.
    pub(crate) fn get(&self) -> io::Result<&rdma::Connection> {
        self.connection.as_ref().ok_or_else(|| {
            io::Error::other("the group has no RDMA connection: a successful update is required first")
        })
    }
}

/// A region's (remote_addr, size, rkey) tuple for one group, as cached
/// from the metadata exchange, with the element layout the region is
/// shared as.
#[derive(Debug, Clone)]
pub(crate) struct CachedRegion {
    pub(crate) remote_addr: u64,
    pub(crate) size: usize,
    pub(crate) rkey: u32,
    pub(crate) group: u32,
    /// `size_of` of the element type the region is shared as.
    pub(crate) elem_size: u32,
    /// `align_of` of the element type the region is shared as.
    pub(crate) elem_align: u32,
    /// `std::any::type_name` of the element type, for diagnostics.
    pub(crate) elem_type: String,
}

/// Metadata describing a memory region exported by a remote machine.
///
/// Returned by [`RemoteMemoryProvider::get_remote_mr_metadata`]; pass it
/// back to [`RemoteMemoryProvider::get_remote_mr`] to obtain the active
/// region. The element layout is what the sharing side registered the
/// region as: a typed read whose `T` does not match it fails instead
/// of reading garbage.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RemoteMemoryRegionMetadata {
    /// Human-readable name of the region.
    pub name: String,
    /// Identifier of the region on the remote machine.
    pub id: u32,
    /// Size of the region, in bytes.
    pub size: usize,
    /// `size_of` of the element type the region is shared as.
    pub elem_size: u32,
    /// `align_of` of the element type the region is shared as.
    pub elem_align: u32,
    /// `std::any::type_name` of the element type, for diagnostics.
    pub elem_type: String,
}

/// An active memory region on the remote machine, as returned by
/// [`RemoteMemoryProvider::get_remote_mr`].
///
/// Reads and writes go through the RDMA connection of the region's
/// group — established by that group's
/// [`RemoteMemoryProvider::update`] — as bytes ([`Self::read`],
/// [`Self::read_into`], [`Self::write`]) or as elements of any
/// [`RemoteSafe`] type ([`Self::read_typed`], [`Self::read_into_typed`],
/// [`Self::write_typed`]), provided that is the type the sharing side
/// registered the region as.
#[derive(Debug, Clone)]
pub struct RemoteMemoryRegion {
    /// The group's RDMA connection slot, shared with the provider's
    /// session for the group.
    pub(crate) connection: Rc<RefCell<GroupConnection>>,
    /// Address of the region in the remote machine's address space.
    pub remote_addr: u64,
    /// Size of the region, in bytes.
    pub size: usize,
    /// Remote key required to access the region with RDMA.
    pub rkey: u32,
    /// Group id this region belongs to (the group used at lookup time).
    pub group: u32,
    /// `size_of` of the element type the region is shared as.
    pub(crate) elem_size: u32,
    /// `align_of` of the element type the region is shared as.
    pub(crate) elem_align: u32,
    /// `std::any::type_name` of the element type, for diagnostics.
    pub(crate) elem_type: String,
}

impl RemoteMemoryRegion {
    /// Read `size` bytes starting at `offset` from the remote region,
    /// allocating and returning a new buffer with the data.
    ///
    /// The read must stay within the region: `offset + size` may not
    /// exceed the region's size.
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
    /// `size` may not exceed `buf.len()`, and the read must stay within
    /// the region.
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

        let slot = self.connection.borrow();
        let connection = slot.get()?;
        let mr = connection.register_addr(buf.as_mut_ptr() as u64, size)?;
        connection.read(&mr, self.remote_addr + offset, self.rkey, size)
    }

    /// Read `count` elements starting at element `offset` from the
    /// remote region, allocating and returning a new `Vec` of them.
    ///
    /// `T` must be the element type the sharing side registered the
    /// region as: its size and alignment travel with the region, and a
    /// `T` that does not match fails here instead of reading garbage —
    /// though type identity across separately compiled applications
    /// still cannot be verified, only the layout. Both `offset` and
    /// `count` are in elements of `T`; the read must stay within the
    /// region. For byte offsets, use [`Self::read`].
    ///
    /// TODO: every read registers and deregisters its buffer; reuse
    /// registered memory once performance matters.
    pub fn read_typed<T: RemoteSafe>(&self, offset: u64, count: usize) -> io::Result<Vec<T>> {
        let mut buffer = zeroed_vec(count);
        self.read_into_typed(offset, &mut buffer)?;
        Ok(buffer)
    }

    /// Read `buf.len()` elements starting at element `offset` into
    /// `buf`, without allocating new memory.
    ///
    /// The same `T`, bounds, and element-unit rules as
    /// [`Self::read_typed`]; a partial read is a shorter `buf` slice.
    pub fn read_into_typed<T: RemoteSafe>(&self, offset: u64, buf: &mut [T]) -> io::Result<()> {
        self.check_elem::<T>()?;
        let elem = size_of::<T>() as u64;
        let byte_offset = offset.saturating_mul(elem);
        let size = size_of_val(buf);
        if byte_offset.saturating_add(size as u64) > self.size as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "read of {} elements at element offset {offset} exceeds the region of {} elements of {} bytes",
                    buf.len(),
                    self.size / size_of::<T>(),
                    size_of::<T>()
                ),
            ));
        }
        if !(self.remote_addr + byte_offset).is_multiple_of(align_of::<T>() as u64) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "read at element offset {offset} is misaligned for elements of alignment {}",
                    align_of::<T>()
                ),
            ));
        }

        let slot = self.connection.borrow();
        let connection = slot.get()?;
        let mr = connection.register_addr(buf.as_mut_ptr() as u64, size)?;
        connection.read(&mr, self.remote_addr + byte_offset, self.rkey, size)
    }

    /// Write `buf` to the remote region starting at byte `offset`,
    /// modifying the remote memory in place.
    ///
    /// The write must stay within the region: `offset + buf.len()` may
    /// not exceed the region's size.
    ///
    /// TODO: every write registers and deregisters its buffer; reuse
    /// registered memory once performance matters.
    pub fn write(&self, offset: u64, buf: &[u8]) -> io::Result<()> {
        if offset.saturating_add(buf.len() as u64) > self.size as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "write of {} bytes at offset {offset} exceeds the region of {} bytes",
                    buf.len(),
                    self.size
                ),
            ));
        }

        let slot = self.connection.borrow();
        let connection = slot.get()?;
        let mr = connection.register_addr(buf.as_ptr() as u64, buf.len())?;
        connection.write(&mr, self.remote_addr + offset, self.rkey, buf.len())
    }

    /// Write `buf` to the remote region starting at element `offset`,
    /// modifying the remote memory in place.
    ///
    /// The same `T` and element-unit rules as [`Self::read_typed`]:
    /// `T` must be the element type the sharing side registered the
    /// region as, and `offset` and `buf.len()` are in elements of `T`;
    /// the write must stay within the region. For byte offsets, use
    /// [`Self::write`].
    ///
    /// TODO: every write registers and deregisters its buffer; reuse
    /// registered memory once performance matters.
    pub fn write_typed<T: RemoteSafe>(&self, offset: u64, buf: &[T]) -> io::Result<()> {
        self.check_elem::<T>()?;
        let elem = size_of::<T>() as u64;
        let byte_offset = offset.saturating_mul(elem);
        let size = size_of_val(buf);
        if byte_offset.saturating_add(size as u64) > self.size as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "write of {} elements at element offset {offset} exceeds the region of {} elements of {} bytes",
                    buf.len(),
                    self.size / size_of::<T>(),
                    size_of::<T>()
                ),
            ));
        }
        if !(self.remote_addr + byte_offset).is_multiple_of(align_of::<T>() as u64) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "write at element offset {offset} is misaligned for elements of alignment {}",
                    align_of::<T>()
                ),
            ));
        }

        let slot = self.connection.borrow();
        let connection = slot.get()?;
        let mr = connection.register_addr(buf.as_ptr() as u64, size)?;
        connection.write(&mr, self.remote_addr + byte_offset, self.rkey, size)
    }

    /// Reject a `T` whose layout is not the layout the region is
    /// shared as.
    fn check_elem<T: RemoteSafe>(&self) -> io::Result<()> {
        if size_of::<T>() as u32 == self.elem_size && align_of::<T>() as u32 == self.elem_align {
            return Ok(());
        }
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "the region is shared as elements of {} (size {}, alignment {}), not {} (size {}, alignment {})",
                self.elem_type,
                self.elem_size,
                self.elem_align,
                type_name::<T>(),
                size_of::<T>(),
                align_of::<T>()
            ),
        ))
    }
}

impl RemoteMemoryProvider {
    /// Prepare a provider for the remote machine described by `addr`.
    ///
    /// Nothing connects here: the first [`Self::update`] runs a
    /// session — the metadata exchange and the group's RDMA
    /// connection.
    pub fn new(addr: RemoteMemoryProviderAddr) -> Self {
        Self {
            addr,
            sessions: RefCell::new(HashMap::new()),
            metadata: RefCell::new(Vec::new()),
            regions: RefCell::new(HashMap::new()),
        }
    }

    /// Metadata of the remote memory regions known to this provider.
    pub fn get_remote_mr_metadata(&self) -> Vec<RemoteMemoryRegionMetadata> {
        self.metadata.borrow().clone()
    }

    /// Refresh the region metadata from the remote machine, for the
    /// tuples of `group`.
    ///
    /// Runs (or reuses) the group's session over the metadata channel:
    /// a fresh session greets the remote, joins the group, and
    /// connects over RDMA — the remote accepts that connection into
    /// the group's protection domain — then receives the region
    /// catalog and the group's tuples, which fill
    /// [`Self::get_remote_mr_metadata`] and the regions
    /// [`Self::get_remote_mr`] hands out. A repeated call re-requests
    /// both over the open channel, picking up regions the remote
    /// registered since; if that exchange fails, the channel is
    /// dropped and the next call runs a fresh session.
    pub fn update(&self, group: u32) -> io::Result<()> {
        let mut sessions = self.sessions.borrow_mut();
        let session = sessions.entry(group).or_insert_with(|| GroupSession {
            channel: None,
            connection: Rc::new(RefCell::new(GroupConnection::empty())),
        });

        if let Some(channel) = session.channel.as_mut() {
            match refresh(channel) {
                Ok(exchange) => self.ingest(group, exchange),
                Err(e) => {
                    // The channel is dead — the remote went away or
                    // the exchange broke — so the next update opens a
                    // fresh session.
                    session.channel = None;
                    Err(e)
                }
            }
        } else {
            let mut channel = Channel::connect((self.addr.address.as_str(), self.addr.tcp_port))?;
            channel.send(&Message::Hello {
                version: PROTOCOL_VERSION,
            })?;
            match channel.receive()? {
                Message::Welcome { version } if version == PROTOCOL_VERSION => {}
                Message::Welcome { version } => {
                    return Err(io::Error::other(format!(
                        "the remote speaks protocol version {version}, not {PROTOCOL_VERSION}"
                    )));
                }
                other => {
                    return Err(io::Error::other(format!(
                        "expected a greeting reply, got {other:?}"
                    )));
                }
            }
            channel.send(&Message::WantGroup { group })?;

            // The rendezvous: the remote, having read the group join,
            // is accepting into the group's protection domain — this
            // connection, which the group's regions read through.
            let connection =
                rdma::Connection::connect((self.addr.address.as_str(), self.addr.rdma_port))?;
            session.connection.borrow_mut().connection = Some(connection);

            let exchange = receive_exchange(&mut channel)?;
            session.channel = Some(channel);
            self.ingest(group, exchange)
        }
    }

    /// Look up a remote memory region by its metadata.
    ///
    /// `group` selects a specific group; `None` picks any available region
    /// (the first known one). The returned region reports the group id
    /// that was used, and reads go through that group's RDMA
    /// connection.
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
        // The group's connection slot: the session's when one exists,
        // a fresh empty one otherwise (reads through it fail: an
        // update of the group is needed first).
        let connection = self
            .sessions
            .borrow()
            .get(&cached.group)
            .map(|session| Rc::clone(&session.connection))
            .unwrap_or_else(|| Rc::new(RefCell::new(GroupConnection::empty())));
        Some(RemoteMemoryRegion {
            connection,
            remote_addr: cached.remote_addr,
            size: cached.size,
            rkey: cached.rkey,
            group: cached.group,
            elem_size: cached.elem_size,
            elem_align: cached.elem_align,
            elem_type: cached.elem_type.clone(),
        })
    }

    /// Fill the caches from the exchange of `group`'s session.
    fn ingest(&self, group: u32, exchange: (Message, Message)) -> io::Result<()> {
        let (Message::Metadata { regions }, Message::Tuples { group: served, tuples }) = exchange
        else {
            return Err(io::Error::other(
                "the remote replied with something other than the catalog and the tuples",
            ));
        };
        if served != group {
            return Err(io::Error::other(format!(
                "the remote served group {served}, not {group}"
            )));
        }

        // The catalog is complete: it replaces the cached metadata.
        *self.metadata.borrow_mut() = regions
            .iter()
            .map(|region| RemoteMemoryRegionMetadata {
                name: region.name.clone(),
                id: region.id,
                size: region.size as usize,
                elem_size: region.elem_size,
                elem_align: region.elem_align,
                elem_type: region.elem_type.clone(),
            })
            .collect();

        let tuples_by_id: HashMap<u32, TupleDesc> = tuples
            .iter()
            .copied()
            .map(|tuple| (tuple.region_id, tuple))
            .collect();
        let mut active = self.regions.borrow_mut();
        for region in &regions {
            let entry = active
                .entry((region.name.clone(), region.id))
                .or_default();
            entry.retain(|cached| cached.group != group);
            if let Some(tuple) = tuples_by_id.get(&region.id) {
                entry.push(CachedRegion {
                    remote_addr: tuple.remote_addr,
                    size: tuple.size as usize,
                    rkey: tuple.rkey,
                    group,
                    elem_size: region.elem_size,
                    elem_align: region.elem_align,
                    elem_type: region.elem_type.clone(),
                });
            }
        }
        // Regions gone from the catalog are gone from the cache.
        let live: Vec<(String, u32)> = regions
            .iter()
            .map(|region| (region.name.clone(), region.id))
            .collect();
        active.retain(|key, _| live.contains(key));
        Ok(())
    }
}

/// Receive the two-message reply of an exchange: the catalog, then
/// the tuples.
fn receive_exchange(channel: &mut Channel) -> io::Result<(Message, Message)> {
    let catalog = channel.receive()?;
    let tuples = channel.receive()?;
    Ok((catalog, tuples))
}

/// Re-request the catalog and the tuples over an open session.
fn refresh(channel: &mut Channel) -> io::Result<(Message, Message)> {
    channel.send(&Message::Update)?;
    receive_exchange(channel)
}
