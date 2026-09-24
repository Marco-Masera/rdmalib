//! Server side: memory regions shared with remote machines.
//!
//! [`SharedMemoryRegionProvider`] owns the registered regions, assigning
//! each a fresh id and handing out a [`SharedMemoryRegionHandle`] for
//! continued access. Regions are shared as bytes ([`Self::register`])
//! or as a `Vec` of any [`crate::RemoteSafe`] type
//! ([`Self::register_typed`]) — either way without copying: the buffer
//! is moved in, and only its address and size travel further. Readers
//! are accepted into groups that share a protection domain.
//! [`Self::serve`] starts the metadata service: a background thread
//! accepting readers over TCP, each joining a group of the reader's
//! choice and connecting over RDMA into the group's protection
//! domain, then receiving the region catalog and the group's tuples.

use std::any::{type_name, Any};
use std::cell::{Cell, Ref, RefCell, RefMut};
use std::collections::HashMap;
use std::io;
use std::marker::PhantomData;
use std::mem::{align_of, size_of};
use std::net::{Ipv4Addr, SocketAddrV4, TcpListener, TcpStream};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::meta::{Channel, Message, RegionDesc, TupleDesc, PROTOCOL_VERSION};
use crate::rdma;
use crate::RemoteSafe;

/// A memory region to be shared with remote machines, as raw bytes.
///
/// Owns the region's buffer. Registering the metadata with a
/// [`SharedMemoryRegionProvider`] moves the buffer into the provider; a
/// handle is returned for continued access. For a `Vec` of some other
/// [`RemoteSafe`] element type, use
/// [`SharedMemoryRegionProvider::register_typed`]. Deliberately not
/// `Clone`: cloning would duplicate the region's memory.
#[derive(Debug)]
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
/// [`SharedMemoryRegionProvider::new`]:
/// [`SharedMemoryRegionProvider::serve`] binds both ports — the TCP
/// port carries the metadata exchange, the RDMA port the memory
/// traffic.
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

/// A registered region's buffer, type-erased: the `Vec<u8>` or
/// `Vec<T>` moved in at registration, kept alive and dropped
/// correctly until the provider goes away. Never read directly —
/// borrows go through the `Rc` clone the handle holds.
type Owner = Rc<RefCell<Box<dyn Any>>>;

/// Serves the memory regions shared with remote machines.
///
/// Constructed from a [`SharedMemoryRegionProviderAddr`]. Regions are
/// registered with [`Self::register`] (a `Vec<u8>`) or
/// [`Self::register_typed`] (a `Vec` of any [`RemoteSafe`] type):
/// each takes ownership of the buffer without copying it and assigns
/// the region an id. [`Self::serve`] starts the metadata service on a
/// background thread; a reader's session there greets over TCP,
/// joins the group of the reader's choosing, and connects over RDMA —
/// accepted into the group's protection domain, created with the
/// group's first reader, so every reader of the group shares the
/// rkey of each tuple. (remote_addr, size, rkey) tuples are created
/// lazily by [`Self::get_shared_mr`], one per (region, group) pair,
/// and served with the catalog; readers re-request them over their
/// open session at any time.
#[derive(Debug)]
pub struct SharedMemoryRegionProvider {
    addr: SharedMemoryRegionProviderAddr,
    /// The state shared with the service thread: everything the
    /// metadata channel serves, and everything it needs to serve it.
    shared: Shared,
    /// The registered regions' buffers by id, shared with the handles
    /// and confined to the application thread (`Rc`): the service
    /// works from the address and size captured in the catalog and
    /// never touches a buffer.
    owners: RefCell<HashMap<u32, Owner>>,
    /// The running service, after [`Self::serve`].
    service: RefCell<Option<ServiceThread>>,
    next_id: Cell<u32>,
}

/// Handle to a region registered with a [`SharedMemoryRegionProvider`].
///
/// Grants read and write access to the region's buffer via [`Self::borrow`]
/// and [`Self::borrow_mut`] — as `T`s for a region registered with
/// [`SharedMemoryRegionProvider::register_typed`], as bytes for one
/// registered with [`SharedMemoryRegionProvider::register`] (then `T`
/// is `u8`). The usual `RefCell` rule applies: only one borrow of a
/// given buffer can be active at a time. The provider keeps working
/// while a buffer is borrowed; it co-owns the buffer and keeps it
/// alive (and registered) for as long as it lives, so the handle
/// grants access, not ownership.
#[derive(Debug)]
pub struct SharedMemoryRegionHandle<T = u8> {
    id: u32,
    /// Shared with the provider's region entry; keeps the buffer alive.
    owner: Owner,
    /// The element type the buffer is viewed as. The handle never
    /// owns a `T`; `fn() -> T` keeps it covariant in `T`.
    _view: PhantomData<fn() -> T>,
}

/// A region's entry in the catalog the metadata channel serves: what
/// the readers see, plus the byte address and size tuple creation
/// works from. Plain data, deliberately without the buffer itself:
/// tuples are created from the address captured at registration, so
/// the service never touches a buffer the application may be
/// borrowing.
#[derive(Debug, Clone)]
struct RegionInfo {
    name: String,
    /// The region's buffer address, in bytes, captured at
    /// registration.
    remote_addr: u64,
    /// The region's buffer size, in bytes.
    size: usize,
    /// `size_of` of the element type the region is shared as.
    elem_size: u32,
    /// `align_of` of the element type the region is shared as.
    elem_align: u32,
    /// `std::any::type_name` of the element type, for diagnostics.
    elem_type: String,
}

/// Internal bookkeeping for one group of readers: they share the
/// group's protection domain, so they share the rkeys of the tuples
/// created for the group.
#[derive(Debug)]
struct GroupEntry {
    /// The group's protection domain, created with the group's first
    /// accepted connection; the queue pairs of all the group's
    /// connections live in it.
    pd: rdma::ProtectionDomain,
    /// The group's accepted reader connections.
    connections: Vec<rdma::Connection>,
}

/// The provider state shared with the service and session threads:
/// the catalog they serve, the groups they accept readers into, and
/// the tuples they create for those groups.
#[derive(Debug, Clone)]
struct Shared {
    /// The region catalog by id.
    catalog: Arc<Mutex<HashMap<u32, RegionInfo>>>,
    /// Reader groups by group id.
    groups: Arc<Mutex<HashMap<u32, GroupEntry>>>,
    /// Tuples by (region, group): the registration of each region's
    /// buffer in a group's protection domain, kept alive until the
    /// provider is dropped.
    tuples: Arc<Mutex<HashMap<(u32, u32), rdma::MemoryRegion>>>,
}

impl SharedMemoryRegionProvider {
    /// Create a provider serving memory over `addr`'s RDMA port and
    /// exchanging metadata over its TCP port, once [`Self::serve`] is
    /// called.
    pub fn new(addr: SharedMemoryRegionProviderAddr) -> Self {
        Self {
            addr,
            shared: Shared {
                catalog: Arc::new(Mutex::new(HashMap::new())),
                groups: Arc::new(Mutex::new(HashMap::new())),
                tuples: Arc::new(Mutex::new(HashMap::new())),
            },
            owners: RefCell::new(HashMap::new()),
            service: RefCell::new(None),
            next_id: Cell::new(0),
        }
    }

    /// Bind the provider's ports and serve readers in the background.
    ///
    /// The metadata listener binds the TCP port on every IPv4
    /// interface; the RDMA listener binds the RDMA port with a
    /// wildcard address, across the node's RDMA devices. Both stay
    /// bound until the provider is dropped. A reader's session: it
    /// greets over TCP, joins the group of its choosing, and connects
    /// over RDMA, which the service accepts into that group's
    /// protection domain — the group, and its domain, are created by
    /// the group's first reader. The reader then receives the region
    /// catalog and the group's tuples, and may re-request them over
    /// its open session at any time. Regions registered before or
    /// after this call are served alike.
    ///
    /// The service runs one rendezvous at a time; each open session
    /// then waits for its reader's requests on a thread of its own,
    /// so one connected reader never blocks another's join.
    ///
    /// TODO: a reader that joins a group but never connects its RDMA
    /// side stalls the rendezvous of the readers behind it — there is
    /// no timeout on the RDMA accept yet; bound it by polling the
    /// event channel if that matters.
    pub fn serve(&self) -> io::Result<()> {
        if self.service.borrow().is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the provider is already serving",
            ));
        }
        let tcp = TcpListener::bind((Ipv4Addr::UNSPECIFIED, self.addr.tcp_port))?;
        let rdma_listener = rdma::Listener::bind(SocketAddrV4::new(
            Ipv4Addr::UNSPECIFIED,
            self.addr.rdma_port,
        ))?;

        let shutdown = Arc::new(AtomicBool::new(false));
        let service = Service {
            shutdown: Arc::clone(&shutdown),
            tcp,
            rdma: rdma_listener,
            shared: self.shared.clone(),
            sessions: Vec::new(),
        };
        let handle = thread::spawn(move || service.run());
        *self.service.borrow_mut() = Some(ServiceThread { handle, shutdown });
        Ok(())
    }

    /// Take ownership of `metadata` and assign the region a fresh id.
    ///
    /// The byte-flavored entry point; see [`Self::register_typed`] for
    /// sharing a `Vec` of some other element type. Returns a handle
    /// for continued access to the buffer; the assigned id is
    /// available as [`SharedMemoryRegionHandle::id`]. No tuples are
    /// created here: they materialize when a reader's session is
    /// served, one per (region, group) pair.
    pub fn register(&self, metadata: SharedMemoryRegionMetadata) -> SharedMemoryRegionHandle {
        self.register_typed(metadata.name, metadata.buffer)
    }

    /// Take ownership of a `Vec` of `T`s and assign the region a fresh
    /// id, sharing it without copying: the buffer's heap allocation
    /// does not move with the value, and remote readers access it in
    /// place.
    ///
    /// `T` must be the type the readers read the region as: its size
    /// and alignment travel with the region's metadata, so the reader
    /// side rejects a `T` that does not match them — but type identity
    /// across separately compiled applications cannot be verified, so
    /// a same-sized, same-aligned `T` still reads whatever bytes the
    /// sharing side meant by its type. Returns a handle granting
    /// access to the buffer as a `&[T]`; tuples materialize lazily as
    /// for [`Self::register`], from the byte address and size captured
    /// here, so the buffer is never touched by tuple creation.
    ///
    /// # Panics
    ///
    /// Panics if `T` is zero-sized: such a region has no bytes to
    /// share.
    pub fn register_typed<T: RemoteSafe>(
        &self,
        name: impl Into<String>,
        buffer: Vec<T>,
    ) -> SharedMemoryRegionHandle<T> {
        assert!(
            size_of::<T>() > 0,
            "cannot share a Vec of zero-sized elements"
        );
        let id = self.next_id.get();
        self.next_id.set(id + 1);

        let remote_addr = buffer.as_ptr() as u64;
        let size = buffer.len() * size_of::<T>();
        let owner: Owner = Rc::new(RefCell::new(Box::new(buffer)));

        let handle = SharedMemoryRegionHandle {
            id,
            owner: Rc::clone(&owner),
            _view: PhantomData,
        };
        self.owners.borrow_mut().insert(id, Rc::clone(&owner));
        self.shared.catalog.lock().unwrap().insert(
            id,
            RegionInfo {
                name: name.into(),
                remote_addr,
                size,
                elem_size: size_of::<T>() as u32,
                elem_align: align_of::<T>() as u32,
                elem_type: type_name::<T>().to_owned(),
            },
        );
        handle
    }

    /// The (remote_addr, size, rkey) tuple of region `id` for `group`,
    /// created on first request.
    ///
    /// Creating a tuple registers the region's buffer in the group's
    /// protection domain: the rkey is a real one, usable by every
    /// reader of the group, and the registration is kept alive for as
    /// long as the provider is. Repeated requests for the same region
    /// and group return the same tuple; registration errors are
    /// reported as `Err`; unknown region ids or groups return
    /// `Ok(None)`.
    ///
    /// This is the data the metadata service serves over TCP; the
    /// service calls it while serving a reader's session, and it
    /// stays available for inspection. Unknown region ids or groups
    /// return `Ok(None)`; registration errors are reported as `Err`.
    /// The buffer is never touched here: its address and size were
    /// captured at registration time, so the application may hold a
    /// borrow of the buffer while tuples are created and served.
    pub fn get_shared_mr(&self, id: u32, group: u32) -> io::Result<Option<(u64, usize, u32)>> {
        self.shared.tuple_of(id, group)
    }
}

impl Drop for SharedMemoryRegionProvider {
    /// Stop the service (and with it the open sessions), then
    /// deregister the tuples while the buffers they pin and the
    /// groups (whose protection domains the registrations belong to)
    /// are still alive. A reader mid-exchange can hold the service
    /// thread for up to one message timeout.
    fn drop(&mut self) {
        if let Some(service) = self.service.borrow_mut().take() {
            service.stop();
        }
        self.shared.tuples.lock().unwrap().clear();
    }
}

impl<T> SharedMemoryRegionHandle<T> {
    /// The id the provider assigned to this region.
    pub fn id(&self) -> u32 {
        self.id
    }
}

impl<T: RemoteSafe> SharedMemoryRegionHandle<T> {
    /// Immutably borrow the buffer as a slice of `T`s.
    ///
    /// Values may change under the borrow without notice: remote
    /// readers of the region write to the same memory. The `RefCell`
    /// rule synchronizes local access only, never the device.
    pub fn borrow(&self) -> Ref<'_, [T]> {
        Ref::map(self.owner.borrow(), |owner| {
            owner
                .downcast_ref::<Vec<T>>()
                .expect("the handle's type matches its registration")
                .as_slice()
        })
    }

    /// Mutably borrow the buffer as a slice of `T`s, to read and
    /// modify the region's data.
    pub fn borrow_mut(&self) -> RefMut<'_, [T]> {
        RefMut::map(self.owner.borrow_mut(), |owner| {
            owner
                .downcast_mut::<Vec<T>>()
                .expect("the handle's type matches its registration")
                .as_mut_slice()
        })
    }
}

impl Shared {
    /// The (remote_addr, size, rkey) tuple of region `id` for `group`,
    /// created on first request.
    ///
    /// Creating a tuple registers the region's buffer in the group's
    /// protection domain: the rkey is a real one, usable by every
    /// reader of the group, and the registration is kept alive for as
    /// long as the provider is. Repeated requests for the same region
    /// and group return the same tuple. The buffer is never touched:
    /// its address and size were captured at registration time. The
    /// locks are taken one at a time, never nested.
    fn tuple_of(&self, id: u32, group: u32) -> io::Result<Option<(u64, usize, u32)>> {
        if let Some(mr) = self.tuples.lock().unwrap().get(&(id, group)) {
            return Ok(Some((mr.addr(), mr.size(), mr.rkey())));
        }

        let Some(pd) = self
            .groups
            .lock()
            .unwrap()
            .get(&group)
            .map(|entry| entry.pd.clone())
        else {
            return Ok(None);
        };
        let Some((remote_addr, size)) = self
            .catalog
            .lock()
            .unwrap()
            .get(&id)
            .map(|entry| (entry.remote_addr, entry.size))
        else {
            return Ok(None);
        };

        let mr = pd.register_addr(remote_addr, size)?;
        let tuple = (mr.addr(), mr.size(), mr.rkey());
        self.tuples.lock().unwrap().insert((id, group), mr);
        Ok(Some(tuple))
    }

    /// The catalog and the tuples of `group`, as the two messages of a
    /// session's exchange: every registered region, each with its
    /// tuple for the group, created lazily here.
    fn catalog_and_tuples(&self, group: u32) -> (Message, Message) {
        // A snapshot: the lock is released before tuple creation,
        // which takes the same lock itself.
        let catalog: Vec<(u32, RegionInfo)> = {
            let locked = self.catalog.lock().unwrap();
            locked
                .iter()
                .map(|(&id, info)| (id, info.clone()))
                .collect()
        };

        let mut regions = Vec::with_capacity(catalog.len());
        let mut tuples = Vec::with_capacity(catalog.len());
        for (id, info) in catalog {
            regions.push(RegionDesc {
                id,
                name: info.name,
                size: info.size as u64,
                elem_size: info.elem_size,
                elem_align: info.elem_align,
                elem_type: info.elem_type,
            });
            if let Ok(Some((remote_addr, size, rkey))) = self.tuple_of(id, group) {
                tuples.push(TupleDesc {
                    region_id: id,
                    remote_addr,
                    size: size as u64,
                    rkey,
                });
            }
        }
        (Message::Metadata { regions }, Message::Tuples { group, tuples })
    }
}

/// How often the service thread re-checks shutdown between accept
/// attempts.
const SERVICE_POLL: Duration = Duration::from_millis(100);

/// The running service, joined when the provider drops.
#[derive(Debug)]
struct ServiceThread {
    handle: JoinHandle<()>,
    shutdown: Arc<AtomicBool>,
}

impl ServiceThread {
    /// Signal the service to stop and wait for it (and its open
    /// sessions) to end.
    fn stop(self) {
        self.shutdown.store(true, Ordering::Relaxed);
        let _ = self.handle.join();
    }
}

/// The metadata service, started by
/// [`SharedMemoryRegionProvider::serve`] on a thread of its own: it
/// owns both listeners and runs the readers' rendezvous, one at a
/// time.
struct Service {
    /// Signals the service and its sessions to stop; the provider's
    /// drop sets it.
    shutdown: Arc<AtomicBool>,
    /// The metadata listener, on the provider's TCP port.
    tcp: TcpListener,
    /// The RDMA listener, on the provider's RDMA port; every
    /// rendezvous is accepted through it, on this thread only.
    rdma: rdma::Listener,
    /// The provider state the sessions serve.
    shared: Shared,
    /// The update halves of the accepted sessions, on their own
    /// threads.
    sessions: Vec<JoinHandle<()>>,
}

impl Service {
    /// Accept readers until shutdown, one rendezvous at a time.
    fn run(mut self) {
        // Polling accept: closing a listener does not reliably wake a
        // blocked accept on Linux, but the flag is checked between
        // attempts.
        let _ = self.tcp.set_nonblocking(true);
        while !self.shutdown.load(Ordering::Relaxed) {
            match self.tcp.accept() {
                Ok((stream, _)) => {
                    let _ = stream.set_nonblocking(false);
                    let _ = self.handshake(stream);
                }
                Err(_) => thread::sleep(SERVICE_POLL),
            }
            self.sessions.retain(|handle| !handle.is_finished());
        }
        for session in self.sessions.drain(..) {
            let _ = session.join();
        }
    }

    /// The serial half of one reader's session: greet, join a group,
    /// accept the reader's RDMA connection into the group's domain,
    /// and serve the catalog and the tuples. The channel then moves
    /// to the session's own thread, waiting for the reader's update
    /// requests, so the next reader's rendezvous can start at once.
    fn handshake(&mut self, stream: TcpStream) -> io::Result<()> {
        let mut channel = Channel::new(stream)?;
        match channel.receive()? {
            Message::Hello { version } if version == PROTOCOL_VERSION => {
                channel.send(&Message::Welcome { version })?;
            }
            Message::Hello { version } => {
                return Err(io::Error::other(format!(
                    "the reader speaks protocol version {version}, not {PROTOCOL_VERSION}"
                )));
            }
            other => {
                return Err(io::Error::other(format!(
                    "expected a greeting, got {other:?}"
                )));
            }
        }
        let group = match channel.receive()? {
            Message::WantGroup { group } => group,
            other => {
                return Err(io::Error::other(format!(
                    "expected a group join, got {other:?}"
                )));
            }
        };

        // The rendezvous: the reader's RDMA connection follows its
        // group join; accept it into the group's domain, creating the
        // group (and the domain) on first use. The domain handle is
        // cloned before the blocking accept, so no lock is held while
        // it waits.
        let group_pd = self
            .shared
            .groups
            .lock()
            .unwrap()
            .get(&group)
            .map(|entry| entry.pd.clone());
        let connection = match group_pd {
            Some(pd) => self.rdma.accept_into(&pd)?,
            None => self.rdma.accept()?,
        };
        self.shared
            .groups
            .lock()
            .unwrap()
            .entry(group)
            .or_insert_with(|| GroupEntry {
                pd: connection.protection_domain(),
                connections: Vec::new(),
            })
            .connections
            .push(connection);

        let (catalog, tuples) = self.shared.catalog_and_tuples(group);
        channel.send(&catalog)?;
        channel.send(&tuples)?;

        let session = SessionThread {
            shutdown: Arc::clone(&self.shutdown),
            channel,
            group,
            shared: self.shared.clone(),
        };
        self.sessions
            .push(thread::spawn(move || session.wait_for_updates()));
        Ok(())
    }
}

/// The open session of one accepted reader: its channel, kept for
/// update requests — and, in the future, provider-side notifications
/// — on a thread of its own.
struct SessionThread {
    shutdown: Arc<AtomicBool>,
    channel: Channel,
    group: u32,
    shared: Shared,
}

impl SessionThread {
    /// Serve the reader's update requests until the reader hangs up
    /// or the provider is dropped. A session waiting on a silent
    /// reader costs at most one message timeout at shutdown.
    fn wait_for_updates(mut self) {
        while !self.shutdown.load(Ordering::Relaxed) {
            let (catalog, tuples) = match self.channel.receive() {
                Ok(Message::Update) => self.shared.catalog_and_tuples(self.group),
                Ok(_) | Err(_) => return,
            };
            if self.channel.send(&catalog).is_err() || self.channel.send(&tuples).is_err() {
                return;
            }
        }
    }
}
