//! Server side: memory regions shared with remote machines.
//!
//! [`SharedMemoryRegionProvider`] owns the registered regions, assigning
//! each a fresh id and handing out a [`SharedMemoryRegionHandle`] for
//! continued access. Regions are shared as bytes ([`Self::register`])
//! or as a `Vec` of any [`crate::RemoteSafe`] type
//! ([`Self::register_typed`]) — either way without copying: the buffer
//! is moved in, and only its address and size travel further. Readers
//! are accepted into groups that share a protection domain. The
//! metadata exchange over TCP is not implemented yet.

use std::any::Any;
use std::cell::{Cell, Ref, RefCell, RefMut};
use std::collections::HashMap;
use std::io;
use std::marker::PhantomData;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::rc::Rc;

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
/// connect to the RDMA port and are accepted one at a time into a
/// group of the owner's choosing with [`Self::accept`]: the readers of
/// a group share one protection domain, and with it the rkey of each
/// tuple. Regions are registered with [`Self::register`] (a
/// `Vec<u8>`) or [`Self::register_typed`] (a `Vec` of any
/// [`RemoteSafe`] type); each takes ownership of the buffer without
/// copying it and assigns the region an id. (remote_addr, size, rkey)
/// tuples are created lazily by [`Self::get_shared_mr`], one per
/// (region, group) pair, and shared by all of the group's readers.
/// The TCP channel for the metadata exchange does not exist yet.
#[derive(Debug)]
pub struct SharedMemoryRegionProvider {
    addr: SharedMemoryRegionProviderAddr,
    /// Tuples by (region id, group): the registration of each region's
    /// buffer in a group's protection domain, kept alive until the
    /// provider is dropped.
    tuples: RefCell<HashMap<(u32, u32), rdma::MemoryRegion>>,
    /// Registered regions by id. This lock guards only the catalog:
    /// buffers live in per-region RefCells shared with the handles, so a
    /// buffer can be borrowed while the provider keeps working.
    regions: RefCell<HashMap<u32, RegionEntry>>,
    /// Accepted reader groups by group id.
    groups: RefCell<HashMap<u32, GroupEntry>>,
    /// The listener, bound on the first [`Self::accept`].
    listener: RefCell<Option<rdma::Listener>>,
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
    owner: Rc<RefCell<Box<dyn Any>>>,
    /// The element type the buffer is viewed as. The handle never
    /// owns a `T`; `fn() -> T` keeps it covariant in `T`.
    _view: PhantomData<fn() -> T>,
}

/// Internal bookkeeping for one registered region.
#[derive(Debug)]
struct RegionEntry {
    // Exposed once the metadata exchange is implemented.
    #[allow(dead_code)]
    name: String,
    /// The region's buffer, type-erased: the `Vec<u8>` or `Vec<T>`
    /// moved in at registration, kept alive and dropped correctly
    /// until the provider goes away. Never read directly — borrows go
    /// through the handle's `Rc` clone of it.
    #[allow(dead_code)]
    owner: Rc<RefCell<Box<dyn Any>>>,
    /// Captured at registration so that tuples can be created later
    /// without touching the buffer: the application may hold a borrow of
    /// it at that moment. In bytes, whatever the element type.
    remote_addr: u64,
    /// Size of the region's buffer, in bytes.
    size: usize,
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

impl SharedMemoryRegionProvider {
    /// Create a provider serving memory over `addr`'s RDMA port and
    /// exchanging metadata over its TCP port.
    pub fn new(addr: SharedMemoryRegionProviderAddr) -> Self {
        Self {
            addr,
            tuples: RefCell::new(HashMap::new()),
            regions: RefCell::new(HashMap::new()),
            groups: RefCell::new(HashMap::new()),
            listener: RefCell::new(None),
            next_id: Cell::new(0),
        }
    }

    /// Accept a connection from a remote reader into `group`, creating
    /// the group on first use.
    ///
    /// Blocks until a reader connects. The group's protection domain
    /// is created with its first connection and shared by all of its
    /// connections, so every reader in the group can use the tuples
    /// created for it. The listener is bound on the first call, on the
    /// provider's RDMA port with a wildcard address (across the node's
    /// RDMA devices); a reader arriving on a different device than its
    /// group's domain is rejected.
    ///
    /// TODO: once the metadata exchange is implemented, readers arrive
    /// with their metadata requests over the TCP channel.
    pub fn accept(&self, group: u32) -> io::Result<()> {
        if self.listener.borrow().is_none() {
            let listener = rdma::Listener::bind(SocketAddrV4::new(
                Ipv4Addr::UNSPECIFIED,
                self.addr.rdma_port,
            ))?;
            *self.listener.borrow_mut() = Some(listener);
        }

        // The group's domain, if the group already has readers.
        let group_pd = self.groups.borrow().get(&group).map(|g| g.pd.clone());
        let connection = {
            let listener = self.listener.borrow();
            let listener = listener.as_ref().expect("the listener is bound");
            match group_pd {
                Some(pd) => listener.accept_into(&pd)?,
                None => listener.accept()?,
            }
        };

        let mut groups = self.groups.borrow_mut();
        groups
            .entry(group)
            .or_insert_with(|| GroupEntry {
                pd: connection.protection_domain(),
                connections: Vec::new(),
            })
            .connections
            .push(connection);
        Ok(())
    }

    /// Take ownership of `metadata` and assign the region a fresh id.
    ///
    /// The byte-flavored entry point; see [`Self::register_typed`] for
    /// sharing a `Vec` of some other element type. Returns a handle
    /// for continued access to the buffer; the assigned id is
    /// available as [`SharedMemoryRegionHandle::id`]. No tuples are
    /// created here: they materialize on the first
    /// [`Self::get_shared_mr`] request for each (region, group) pair.
    pub fn register(&self, metadata: SharedMemoryRegionMetadata) -> SharedMemoryRegionHandle {
        self.register_typed(metadata.name, metadata.buffer)
    }

    /// Take ownership of a `Vec` of `T`s and assign the region a fresh
    /// id, sharing it without copying: the buffer's heap allocation
    /// does not move with the value, and remote readers access it in
    /// place.
    ///
    /// `T` must be the type the readers read the region as — the
    /// compiler cannot enforce agreement between separately compiled
    /// applications, so choosing differently on the two sides reads
    /// garbage without failing. Returns a handle granting access to
    /// the buffer as a `&[T]`; tuples materialize lazily as for
    /// [`Self::register`], from the byte address and size captured
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
        let owner: Rc<RefCell<Box<dyn Any>>> = Rc::new(RefCell::new(Box::new(buffer)));

        let handle = SharedMemoryRegionHandle {
            id,
            owner: Rc::clone(&owner),
            _view: PhantomData,
        };
        self.regions.borrow_mut().insert(
            id,
            RegionEntry {
                name: name.into(),
                owner,
                remote_addr,
                size,
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
    /// This is the data that will be served through the TCP channel.
    /// The buffer is never touched here: its address and size were
    /// captured at registration time, so the application may hold a
    /// borrow of the buffer while tuples are created and served.
    pub fn get_shared_mr(&self, id: u32, group: u32) -> io::Result<Option<(u64, usize, u32)>> {
        if let Some(mr) = self.tuples.borrow().get(&(id, group)) {
            return Ok(Some((mr.addr(), mr.size(), mr.rkey())));
        }

        let Some(pd) = self
            .groups
            .borrow()
            .get(&group)
            .map(|entry| entry.pd.clone())
        else {
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

        let mr = pd.register_addr(remote_addr, size)?;
        let tuple = (mr.addr(), mr.size(), mr.rkey());
        self.tuples.borrow_mut().insert((id, group), mr);
        Ok(Some(tuple))
    }
}

impl Drop for SharedMemoryRegionProvider {
    /// Deregister the tuples while the buffers they pin and the
    /// groups (whose protection domains the registrations belong to)
    /// are still alive.
    fn drop(&mut self) {
        self.tuples.borrow_mut().clear();
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
