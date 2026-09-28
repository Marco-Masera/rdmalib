//! Client side: handles to memory regions exported by a remote machine.
//!
//! [`RemoteMemoryProvider`] connects to a remote
//! [`SharedMemoryRegionProvider`](crate::SharedMemoryRegionProvider)
//! and hands out [`RemoteMemoryRegion`]s to read from and write to.
//! [`RemoteMemoryProvider::update`] runs a group's session over the
//! metadata channel — greeting, joining the group, connecting over
//! RDMA (which the remote accepts into the group's protection
//! domain), and receiving the region catalog and the group's tuples.
//!
//! One-sided operations are async, through the operations engine
//! ([`Engine`], see `docs/async_engine.md`): submitting registers the
//! buffer, posts the operation, and parks the buffer in the engine's
//! slot — everything on the engine's thread, interleaved polling of
//! every waiting operation from one optionally CPU-pinned thread. The
//! returned [`RegionOp`] is an ordinary future: await it, join them,
//! select them, or [`RegionOp::wait`] it without an executor.

use std::any::type_name;
use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::marker::PhantomData;
use std::mem::{align_of, size_of};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use crate::engine::{CompletionSource, Op, OpHandle, OpTarget, Submitted};
use crate::meta::{Channel, Message, TupleDesc, PROTOCOL_VERSION};
use crate::pod::zeroed_vec;
use crate::rdma;
use crate::{Engine, RemoteSafe};

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
/// The provider's operations run through its [`Engine`]: the one
/// handed to [`Self::with_engine`], or an own unpinned one.
#[derive(Debug)]
pub struct RemoteMemoryProvider {
    /// Where to reach the remote machine.
    addr: RemoteMemoryProviderAddr,
    /// The engine that posts and polls every operation: the one the
    /// provider was built with, or its own unpinned default.
    engine: Engine,
    /// The shared completion queues, one per device the provider's
    /// connections landed on — every connection of a device posts
    /// its completions into its device's queue, and the engine drains
    /// each queue from one poll site (see `docs/async_engine.md`).
    completions: RefCell<Vec<rdma::SharedCompletions>>,
    /// Sessions by group, established by [`Self::update`]: the
    /// metadata channel, kept open for re-requests, and the group's
    /// engine-registered RDMA connection.
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
    /// The group's RDMA connection slot, filled by the session's
    /// rendezvous; the group's regions operate through it.
    pub(crate) connection: Rc<RefCell<GroupConnection>>,
}

/// The RDMA connection of one group: the engine the connection was
/// registered with, and its id there. Filled by a session's
/// rendezvous; empty otherwise, and operating through it fails until
/// a successful [`RemoteMemoryProvider::update`] of the group.
#[derive(Debug)]
pub(crate) struct GroupConnection {
    /// The engine the group's connection is registered with — the
    /// provider's, whatever it was built with.
    pub(crate) engine: Engine,
    /// The connection id the engine assigned; absent with no
    /// session.
    pub(crate) conn: Option<u32>,
}

impl GroupConnection {
    /// A slot no session has filled yet: operations through it fail
    /// — a successful [`RemoteMemoryProvider::update`] of the group
    /// is needed first.
    pub(crate) fn empty(engine: Engine) -> Self {
        Self {
            engine,
            conn: None,
        }
    }

    /// The group's engine-registered connection id, or an error
    /// explaining why there is none.
    pub(crate) fn get(&self) -> io::Result<u32> {
        self.conn.ok_or_else(|| {
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
/// Operations go through the operations engine of the region's group
/// — the group's connection was registered with it by that group's
/// [`RemoteMemoryProvider::update`] — as bytes ([`Self::read_async`],
/// [`Self::read_into_async`], [`Self::write_async`]) or as elements
/// of any [`RemoteSafe`] type ([`Self::read_typed_async`],
/// [`Self::read_into_typed_async`], [`Self::write_typed_async`]),
/// provided that is the type the sharing side registered the region
/// as. Each submission is eager — the checks and the buffer handover
/// happen at the call — and returns a [`RegionOp`] future that
/// resolves with the operation's outcome: the buffer back, filled
/// (a read) or as submitted (a write), on success.
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
    /// exceed the region's size. The buffer is owned by the operation
    /// until it resolves, so a dropped operation never races the
    /// device.
    ///
    pub fn read_async(&self, offset: u64, size: usize) -> io::Result<RegionOp<u8>> {
        let buffer = vec![0u8; size];
        self.read_into_async(offset, buffer)
    }

    /// Read the whole of `buffer`'s worth of bytes starting at
    /// `offset`, overwriting it without allocating new memory; the
    /// filled buffer resolves with the operation.
    ///
    /// The read must stay within the region: `offset +
    /// buffer.len()` may not exceed the region's size.
    ///
    pub fn read_into_async(&self, offset: u64, buffer: Vec<u8>) -> io::Result<RegionOp<u8>> {
        if offset.saturating_add(buffer.len() as u64) > self.size as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "read of {} bytes at offset {offset} exceeds the region of {} bytes",
                    buffer.len(),
                    self.size
                ),
            ));
        }
        self.post_op(Op::Read, offset, Submitted::new(buffer))
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
    /// region. For byte offsets, use [`Self::read_async`].
    ///
    pub fn read_typed_async<T: RemoteSafe>(
        &self,
        offset: u64,
        count: usize,
    ) -> io::Result<RegionOp<T>> {
        let buffer = zeroed_vec(count);
        self.read_into_typed_async(offset, buffer)
    }

    /// Read the whole of `buffer`'s worth of elements starting at
    /// element `offset`, overwriting it without allocating new
    /// memory; the filled buffer resolves with the operation.
    ///
    /// The same `T`, bounds, and element-unit rules as
    /// [`Self::read_typed_async`].
    ///
    pub fn read_into_typed_async<T: RemoteSafe>(
        &self,
        offset: u64,
        buffer: Vec<T>,
    ) -> io::Result<RegionOp<T>> {
        let byte_offset = self.check_elem_layout::<T>(offset, buffer.len())?;
        self.post_op(Op::Read, byte_offset, Submitted::new(buffer))
    }

    /// Write the whole of `buffer` to the remote region starting at
    /// byte `offset`, modifying the remote memory in place.
    ///
    /// The write must stay within the region: `offset +
    /// buffer.len()` may not exceed the region's size. The buffer is
    /// owned by the operation until it resolves — handed back on
    /// success — so a dropped operation never races the device.
    ///
    pub fn write_async(&self, offset: u64, buffer: Vec<u8>) -> io::Result<RegionOp<u8>> {
        if offset.saturating_add(buffer.len() as u64) > self.size as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "write of {} bytes at offset {offset} exceeds the region of {} bytes",
                    buffer.len(),
                    self.size
                ),
            ));
        }
        self.post_op(Op::Write, offset, Submitted::new(buffer))
    }

    /// Write the whole of `buffer` to the remote region starting at
    /// element `offset`, modifying the remote memory in place.
    ///
    /// The same `T` and element-unit rules as
    /// [`Self::read_typed_async`]: `T` must be the element type the
    /// sharing side registered the region as, and `offset` and
    /// `buffer.len()` are in elements of `T`; the write must stay
    /// within the region. For byte offsets, use [`Self::write_async`].
    ///
    pub fn write_typed_async<T: RemoteSafe>(
        &self,
        offset: u64,
        buffer: Vec<T>,
    ) -> io::Result<RegionOp<T>> {
        let byte_offset = self.check_elem_layout::<T>(offset, buffer.len())?;
        self.post_op(Op::Write, byte_offset, Submitted::new(buffer))
    }

    /// Submit an operation of `buffer.len()` bytes at `offset`
    /// through the group's engine-registered connection.
    ///
    /// The bounds and layout checks ran in the callers; what is left
    /// is the connection check — no session, no operation — and the
    /// eager engine submission, from this thread.
    fn post_op<T: RemoteSafe>(
        &self,
        op: Op,
        offset: u64,
        submitted: Submitted,
    ) -> io::Result<RegionOp<T>> {
        let slot = self.connection.borrow();
        let handle = slot.engine.submit(
            slot.get()?,
            op,
            OpTarget {
                remote_addr: self.remote_addr + offset,
                rkey: self.rkey,
            },
            submitted,
        )?;
        Ok(RegionOp {
            handle,
            _view: PhantomData,
        })
    }

    /// Reject a `T` whose layout is not the layout the region is
    /// shared as, and translate element `offset` + `count` into the
    /// checked byte offset they name.
    fn check_elem_layout<T: RemoteSafe>(&self, offset: u64, count: usize) -> io::Result<u64> {
        self.check_elem::<T>()?;
        let elem = size_of::<T>() as u64;
        let byte_offset = offset.saturating_mul(elem);
        let len = count as u64 * elem;
        if byte_offset.saturating_add(len) > self.size as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "operation of {} elements at element offset {offset} exceeds the region of {} elements of {} bytes",
                    count,
                    self.size / size_of::<T>(),
                    size_of::<T>()
                ),
            ));
        }
        if !(self.remote_addr + byte_offset).is_multiple_of(align_of::<T>() as u64) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "operation at element offset {offset} is misaligned for elements of alignment {}",
                    align_of::<T>()
                ),
            ));
        }
        Ok(byte_offset)
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
            )
        ))
    }
}

/// The future of one submitted region operation: it resolves with the
/// operation's outcome — the buffer back on success (filled, for a
/// read; as submitted, for a write), the completion's (or the
/// submission's) error otherwise.
///
/// An ordinary `std` future, so any executor works: await it, join
/// several, select among them, attach callbacks by spawning — or,
/// without an executor, block on [`Self::wait`]. It is `Send`, so it
/// can be awaited from any thread; only the submission it came from
/// was bound to the submitting thread.
///
/// Dropping it before resolution abandons the operation, safely: the
/// engine completes it anyway and frees the buffer.
#[derive(Debug)]
pub struct RegionOp<T = u8> {
    /// The engine-level operation future.
    handle: OpHandle,
    /// The element type the buffer comes back as; never owned
    /// (`fn() -> T` keeps it covariant in `T`).
    _view: PhantomData<fn() -> T>,
}

impl<T: RemoteSafe> Future for RegionOp<T> {
    type Output = io::Result<Vec<T>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // RegionOp is Unpin (its handle is), so the pin never holds.
        let this = self.get_mut();
        match Future::poll(Pin::new(&mut this.handle), cx) {
            Poll::Ready(Ok(buffer)) => match buffer.downcast::<Vec<T>>() {
                Ok(buffer) => Poll::Ready(Ok(*buffer)),
                // Unreachable by construction: the buffer was erased
                // from a `Vec<T>` at submission.
                Err(_) => Poll::Ready(Err(io::Error::other(
                    "the engine returned a buffer of the wrong element type — an engine bug",
                ))),
            },
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T: RemoteSafe> RegionOp<T> {
    /// Block until the operation resolves — the join for callers
    /// without an executor (with one, await instead). The engine
    /// thread does the waiting work; this thread parks until the
    /// engine's wake. Operations waited in turn still overlap: they
    /// complete concurrently, on the engine thread.
    pub fn wait(self) -> io::Result<Vec<T>> {
        let buffer = self.handle.wait()?;
        match buffer.downcast::<Vec<T>>() {
            Ok(buffer) => Ok(*buffer),
            // Unreachable by construction — see the future's poll.
            Err(_) => Err(io::Error::other(
                "the engine returned a buffer of the wrong element type — an engine bug",
            )),
        }
    }
}

impl RemoteMemoryProvider {
    /// Prepare a provider for the remote machine described by `addr`,
    /// running its operations on an own, unpinned engine.
    ///
    /// Nothing connects here: the first [`Self::update`] runs a
    /// session — the metadata exchange and the group's RDMA
    /// connection. To share one engine (and its CPU) among several
    /// providers, or to pin the engine to a CPU, build with
    /// [`Self::with_engine`] instead.
    pub fn new(addr: RemoteMemoryProviderAddr) -> Self {
        Self::with_engine(addr, Engine::new())
    }

    /// Prepare a provider for the remote machine described by `addr`,
    /// running its operations on `engine`.
    ///
    /// Nothing connects here: the first [`Self::update`] runs a
    /// session — the metadata exchange and the group's RDMA
    /// connection — and registers that connection with the engine, so
    /// every operation of every region of this provider posts and
    /// polls on the engine's thread: share one engine among several
    /// providers to share one polling thread (and, when the engine is
    /// [`Engine::on_cpu`]-pinned, one CPU).
    pub fn with_engine(addr: RemoteMemoryProviderAddr, engine: Engine) -> Self {
        Self {
            addr,
            engine,
            completions: RefCell::new(Vec::new()),
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
    /// [`Self::get_remote_mr`] hands out. The connection is registered
    /// with the engine (a previous one, from an earlier session of the
    /// group, is torn down there), so the group's operations run
    /// through the engine's thread from here on. A repeated call
    /// re-requests the catalog and the tuples over the open channel,
    /// picking up regions the remote registered since; if that
    /// exchange fails, the channel is dropped and the next call runs
    /// a fresh session.
    pub fn update(&self, group: u32) -> io::Result<()> {
        let mut sessions = self.sessions.borrow_mut();
        let session = sessions.entry(group).or_insert_with(|| GroupSession {
            channel: None,
            connection: Rc::new(RefCell::new(GroupConnection::empty(self.engine.clone()))),
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
            // connection, whose operations the engine drives. The
            // connection posts its completions into a shared
            // completion queue, one per device: the first connection
            // on a device creates it and registers it with the engine
            // (every later connection of the device reuses it — one
            // poll site for them all); a previous session's
            // connection of the group is torn down on the engine
            // thread, its outstanding operations flushing to their
            // handles.
            let (connection, new_completions) = rdma::Connection::connect_shared(
                (self.addr.address.as_str(), self.addr.rdma_port),
                &self.completions.borrow(),
            )?;
            let engine_completions = match new_completions {
                Some(shared) => {
                    self.completions.borrow_mut().push(shared.clone());
                    Some(Box::new(shared) as Box<dyn CompletionSource>)
                }
                None => None,
            };
            let conn = self
                .engine
                .register(Box::new(connection), engine_completions)?;
            {
                let mut slot = session.connection.borrow_mut();
                if let Some(old) = slot.conn.replace(conn) {
                    self.engine.destroy(old)?;
                }
            }

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
        // a fresh empty one otherwise (operations through it fail: an
        // update of the group is needed first) — on the provider's
        // engine either way.
        let connection = self
            .sessions
            .borrow()
            .get(&cached.group)
            .map(|session| Rc::clone(&session.connection))
            .unwrap_or_else(|| {
                Rc::new(RefCell::new(GroupConnection::empty(self.engine.clone())))
            });
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
