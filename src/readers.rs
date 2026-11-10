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
//! No data operation registers memory per call: registering and
//! deregistering cost kernel round trips and page pinning, an order
//! of magnitude more than the operation itself. Instead, every group
//! connection owns a registered **staging pool** (sized by
//! [`RemoteMemoryProvider::with_staging_capacity`]) that the byte and
//! typed paths bounce through — one local copy per operation, in
//! pool-sized chunks for larger ones — and small writes travel
//! inline, straight out of the caller's memory, needing nothing at
//! all. The zero-copy path is a buffer registered once for the
//! group, a [`RegisteredBuffer`]: reads and writes through it touch
//! neither the pool nor the registration path.

use std::any::type_name;
use std::cell::{Cell, Ref, RefCell};
use std::collections::HashMap;
use std::fmt;
use std::io;
use std::mem::{align_of, size_of};
use std::rc::Rc;

use crate::meta::{Channel, Message, TupleDesc, PROTOCOL_VERSION};
use crate::rdma;
use crate::RemoteSafe;

/// Default capacity of a group connection's staging pool, in bytes:
/// 1 MiB. Operations larger than the pool travel in pool-sized
/// chunks; a reader that knows it will issue larger operations can
/// raise the capacity with
/// [`RemoteMemoryProvider::with_staging_capacity`] or
/// [`RemoteMemoryProvider::set_staging_capacity`].
const DEFAULT_STAGING_CAPACITY: usize = 1 << 20;

/// A staging capacity to use: at least one byte (an empty pool
/// cannot be registered) and at most what one RDMA operation can
/// address (the scatter entry length is 32-bit).
fn clamp_staging_capacity(bytes: usize) -> usize {
    bytes.clamp(1, u32::MAX as usize)
}

/// The error a data operation reports when its group has no RDMA
/// connection: a session — a successful
/// [`RemoteMemoryProvider::update`] — is a prerequisite.
fn no_connection() -> io::Error {
    io::Error::other(
        "the group has no RDMA connection: a successful update is required first",
    )
}

fn invalid(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, msg.into())
}

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
    /// The staging-pool capacity of the group connections, applied
    /// when a group's pool is (re)created; see
    /// [`Self::set_staging_capacity`].
    staging_capacity: Cell<usize>,
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

/// One group connection's staging pool: a buffer registered in the
/// connection's protection domain once, on first use, and reused by
/// every unregistered read and write through the connection. This is
/// what keeps steady-state operations off the registration path:
/// registering and deregistering per operation costs kernel round
/// trips and page pinning — tens of microseconds against the ~2 µs
/// of the operation itself — and the pool pays it once per
/// connection.
#[derive(Debug)]
pub(crate) struct Staging {
    /// The registration, in the connection's protection domain;
    /// declared first so it is dropped — deregistered — before the
    /// buffer it pins.
    pub(crate) mr: rdma::MemoryRegion,
    /// The staging buffer. Never resized or moved after the
    /// registration pins its address.
    pub(crate) buf: Vec<u8>,
}

/// The RDMA connection of one group: established by the group's
/// session, read by the group's regions, and the owner of the
/// staging pool those regions' unregistered reads and writes bounce
/// through.
#[derive(Debug)]
pub(crate) struct GroupConnection {
    /// The staging pool, created lazily by the first unregistered
    /// read or write through this connection (inline writes never
    /// need it). Declared before `connection` so the pool — its
    /// registration — is dropped while the connection, owner of the
    /// protection domain it is registered in, is still alive.
    pub(crate) staging: Option<Staging>,
    pub(crate) connection: Option<rdma::Connection>,
    /// The capacity the pool is created with, from the provider's
    /// configuration at creation time.
    pub(crate) capacity: usize,
    /// Bumped whenever the connection is replaced (by a fresh session
    /// in [`RemoteMemoryProvider::update`]): the staging pool is
    /// dropped with the old connection, and registered buffers see
    /// the bump and re-register into the new connection's domain.
    pub(crate) generation: u32,
}

impl GroupConnection {
    /// A slot no session has filled yet: reads through it fail — a
    /// successful [`RemoteMemoryProvider::update`] of the group is
    /// needed first.
    pub(crate) fn empty() -> Self {
        Self::with_capacity(DEFAULT_STAGING_CAPACITY)
    }

    /// An empty slot whose staging pool, once created, holds
    /// `capacity` bytes.
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            staging: None,
            connection: None,
            capacity: clamp_staging_capacity(capacity),
            generation: 0,
        }
    }

    /// The connection, or an error explaining why there is none.
    pub(crate) fn get(&self) -> io::Result<&rdma::Connection> {
        self.connection.as_ref().ok_or_else(no_connection)
    }

    /// Create the staging pool: a `capacity`-byte buffer registered
    /// in the connection's protection domain, once per connection.
    pub(crate) fn create_staging(
        connection: &rdma::Connection,
        capacity: usize,
    ) -> io::Result<Staging> {
        let buf = vec![0u8; capacity];
        let mr = connection.register_addr(buf.as_ptr() as u64, buf.len())?;
        Ok(Staging { mr, buf })
    }

    /// Read `len` bytes at `remote_addr` of the remote machine,
    /// accessed with `rkey`, into the `len` writable bytes at `dst`.
    ///
    /// The read lands in the staging pool and is copied to `dst`, in
    /// pool-sized chunks when `len` exceeds the pool.
    ///
    /// # Safety
    ///
    /// The caller guarantees `len` writable bytes at `dst` for the
    /// duration of the call.
    pub(crate) fn read_into_ptr(
        &mut self,
        dst: *mut u8,
        len: usize,
        remote_addr: u64,
        rkey: u32,
    ) -> io::Result<()> {
        // Split borrows: the loop posts through the connection and
        // reads the pool in the same iteration.
        let Self {
            staging,
            connection,
            capacity,
            ..
        } = self;
        // No connection first, then the zero-length rejection the low
        // level layer applies — the failure order of the operations
        // before the staging pool existed.
        let conn = connection.as_ref().ok_or_else(no_connection)?;
        if len == 0 {
            return Err(invalid("cannot read zero bytes"));
        }
        if staging.is_none() {
            *staging = Some(Self::create_staging(conn, *capacity)?);
        }
        let stage = staging
            .as_ref()
            .expect("the staging pool was just created");
        let capacity = stage.buf.len();
        let mut done = 0;
        while done < len {
            let chunk = (len - done).min(capacity);
            conn.read(&stage.mr, remote_addr + done as u64, rkey, chunk)?;
            // SAFETY: the caller's `dst` spans `len` writable bytes,
            // and the read just filled the first `chunk` bytes of the
            // pool.
            unsafe {
                std::ptr::copy_nonoverlapping(stage.buf.as_ptr(), dst.add(done), chunk)
            };
            done += chunk;
        }
        Ok(())
    }

    /// Write the `len` readable bytes at `src` to `remote_addr` of
    /// the remote machine, accessed with `rkey`.
    ///
    /// Writes within the connection's inline budget travel straight
    /// from the caller's memory — no registration, no copy; larger
    /// ones copy through the staging pool, in pool-sized chunks.
    ///
    /// # Safety
    ///
    /// The caller guarantees `len` readable bytes at `src` for the
    /// duration of the call.
    pub(crate) fn write_from_ptr(
        &mut self,
        src: *const u8,
        len: usize,
        remote_addr: u64,
        rkey: u32,
    ) -> io::Result<()> {
        let Self {
            staging,
            connection,
            capacity,
            ..
        } = self;
        let conn = connection.as_ref().ok_or_else(no_connection)?;
        if len == 0 {
            return Err(invalid("cannot write zero bytes"));
        }
        // Inline: the device reads the payload out of the caller's
        // memory while the write is posted.
        if len <= conn.inline_cap() {
            return conn.write_inline(src as u64, remote_addr, rkey, len);
        }
        if staging.is_none() {
            *staging = Some(Self::create_staging(conn, *capacity)?);
        }
        let stage = staging
            .as_mut()
            .expect("the staging pool was just created");
        let capacity = stage.buf.len();
        let mut done = 0;
        while done < len {
            let chunk = (len - done).min(capacity);
            // SAFETY: the caller's `src` spans `len` readable bytes.
            unsafe {
                std::ptr::copy_nonoverlapping(src.add(done), stage.buf.as_mut_ptr(), chunk)
            };
            conn.write(&stage.mr, remote_addr + done as u64, rkey, chunk)?;
            done += chunk;
        }
        Ok(())
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
/// registered the region as. None of them register memory per call:
/// the byte and typed paths copy through the group connection's
/// staging pool (small writes skip even that and travel inline), and
/// the registered paths — [`Self::read_into_registered`] and
/// [`Self::write_from_registered`], with a [`RegisteredBuffer`] —
/// move data with no copy at all.
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
    /// exceed the region's size. The data lands in the group
    /// connection's staging pool and is copied into the returned
    /// buffer; the zero-copy path — no staging, no copy — is
    /// [`Self::read_into_registered`] with a [`RegisteredBuffer`].
    pub fn read(&self, offset: u64, size: usize) -> io::Result<Vec<u8>> {
        if offset.saturating_add(size as u64) > self.size as u64 {
            return Err(invalid(format!(
                "read of {size} bytes at offset {offset} exceeds the region of {} bytes",
                self.size
            )));
        }
        let mut buffer = Vec::with_capacity(size);
        let mut slot = self.connection.borrow_mut();
        // SAFETY: `buffer`'s allocation spans `size` writable bytes
        // (its capacity).
        slot.read_into_ptr(
            buffer.as_mut_ptr(),
            size,
            self.remote_addr + offset,
            self.rkey,
        )?;
        // SAFETY: on success the device filled all `size` bytes of
        // the buffer, and every byte pattern is a valid `u8`.
        unsafe { buffer.set_len(size) };
        Ok(buffer)
    }

    /// Read `size` bytes starting at `offset` into the start of `buf`,
    /// overwriting its first `size` bytes without allocating new memory.
    ///
    /// `size` may not exceed `buf.len()`, and the read must stay within
    /// the region. The data is staged (see [`Self::read`]) and copied
    /// into `buf`.
    pub fn read_into(&self, offset: u64, size: usize, buf: &mut [u8]) -> io::Result<()> {
        if size > buf.len() {
            return Err(invalid(format!(
                "read of {size} bytes exceeds the buffer of {} bytes",
                buf.len()
            )));
        }
        if offset.saturating_add(size as u64) > self.size as u64 {
            return Err(invalid(format!(
                "read of {size} bytes at offset {offset} exceeds the region of {} bytes",
                self.size
            )));
        }
        let mut slot = self.connection.borrow_mut();
        // SAFETY: `buf` spans `size` (checked above) writable bytes.
        slot.read_into_ptr(buf.as_mut_ptr(), size, self.remote_addr + offset, self.rkey)
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
    /// The data is staged (see [`Self::read`]) and copied into the
    /// returned buffer.
    pub fn read_typed<T: RemoteSafe>(&self, offset: u64, count: usize) -> io::Result<Vec<T>> {
        let (byte_offset, byte_len) = self.typed_span::<T>("read", offset, count)?;
        let mut buffer = Vec::with_capacity(count);
        let mut slot = self.connection.borrow_mut();
        // SAFETY: `buffer`'s allocation spans `count` elements of
        // `T` (its capacity) — `byte_len` writable bytes.
        slot.read_into_ptr(
            buffer.as_mut_ptr() as *mut u8,
            byte_len,
            self.remote_addr + byte_offset,
            self.rkey,
        )?;
        // SAFETY: on success the device filled all `byte_len` bytes
        // of the buffer, and every bit pattern is a valid `T` by the
        // `RemoteSafe` contract.
        unsafe { buffer.set_len(count) };
        Ok(buffer)
    }

    /// Read `buf.len()` elements starting at element `offset` into
    /// `buf`, without allocating new memory.
    ///
    /// The same `T`, bounds, and element-unit rules as
    /// [`Self::read_typed`]; a partial read is a shorter `buf` slice.
    /// The data is staged (see [`Self::read`]) and copied into `buf`.
    pub fn read_into_typed<T: RemoteSafe>(&self, offset: u64, buf: &mut [T]) -> io::Result<()> {
        let (byte_offset, byte_len) = self.typed_span::<T>("read", offset, buf.len())?;
        let mut slot = self.connection.borrow_mut();
        // SAFETY: `buf` is `buf.len()` valid `T`s — hence `byte_len`
        // writable bytes — and the engine treats it as raw memory
        // only.
        slot.read_into_ptr(
            buf.as_mut_ptr() as *mut u8,
            byte_len,
            self.remote_addr + byte_offset,
            self.rkey,
        )
    }

    /// Write `buf` to the remote region starting at byte `offset`,
    /// modifying the remote memory in place.
    ///
    /// The write must stay within the region: `offset + buf.len()` may
    /// not exceed the region's size. Writes within the connection's
    /// inline budget travel straight from `buf` — no registration, no
    /// copy; larger ones copy through the group connection's staging
    /// pool, in pool-sized chunks.
    pub fn write(&self, offset: u64, buf: &[u8]) -> io::Result<()> {
        if offset.saturating_add(buf.len() as u64) > self.size as u64 {
            return Err(invalid(format!(
                "write of {} bytes at offset {offset} exceeds the region of {} bytes",
                buf.len(),
                self.size
            )));
        }
        let mut slot = self.connection.borrow_mut();
        // SAFETY: `buf` spans `buf.len()` readable bytes.
        slot.write_from_ptr(buf.as_ptr(), buf.len(), self.remote_addr + offset, self.rkey)
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
    /// Small writes travel inline (see [`Self::write`]); larger ones
    /// are staged.
    pub fn write_typed<T: RemoteSafe>(&self, offset: u64, buf: &[T]) -> io::Result<()> {
        let (byte_offset, byte_len) = self.typed_span::<T>("write", offset, buf.len())?;
        let mut slot = self.connection.borrow_mut();
        // SAFETY: `buf` is `buf.len()` valid `T`s — hence `byte_len`
        // readable bytes — and the engine treats it as raw memory
        // only.
        slot.write_from_ptr(
            buf.as_ptr() as *const u8,
            byte_len,
            self.remote_addr + byte_offset,
            self.rkey,
        )
    }

    /// Read `count` elements starting at element `offset` from the
    /// remote region into the start of `buf`, a buffer registered for
    /// the region's group: zero-copy — the device writes straight
    /// into the buffer, with no staging step and no per-call
    /// registration.
    ///
    /// The same `T`, element-unit, and layout rules as
    /// [`Self::read_into_typed`]; `buf` must belong to the region's
    /// group and `count` may not exceed its length.
    pub fn read_into_registered<T: RemoteSafe>(
        &self,
        offset: u64,
        count: usize,
        buf: &mut RegisteredBuffer<T>,
    ) -> io::Result<()> {
        let (byte_offset, byte_len) = self.registered_span::<T>("read", offset, count, buf)?;
        let mr = buf.registered()?;
        let slot = buf.connection.borrow_mut();
        let conn = slot.get()?;
        conn.read(&mr, self.remote_addr + byte_offset, self.rkey, byte_len)
    }

    /// Write the first `count` elements of `buf` to the remote region
    /// starting at element `offset`, modifying the remote memory in
    /// place: zero-copy, as [`Self::read_into_registered`] reads.
    ///
    /// The same `T`, element-unit, layout, group, and count rules as
    /// there. Small writes travel inline — the device reads the
    /// payload out of the buffer while the write is posted — which
    /// saves the device one trip to the buffer's memory.
    pub fn write_from_registered<T: RemoteSafe>(
        &self,
        offset: u64,
        count: usize,
        buf: &RegisteredBuffer<T>,
    ) -> io::Result<()> {
        let (byte_offset, byte_len) = self.registered_span::<T>("write", offset, count, buf)?;
        let mr = buf.registered()?;
        let slot = buf.connection.borrow_mut();
        let conn = slot.get()?;
        if byte_len <= conn.inline_cap() {
            conn.write_inline(
                buf.buf.as_ptr() as u64,
                self.remote_addr + byte_offset,
                self.rkey,
                byte_len,
            )
        } else {
            conn.write(&mr, self.remote_addr + byte_offset, self.rkey, byte_len)
        }
    }

    /// The checks the registered-buffer operations share: the buffer
    /// belongs to the region's group, `count` fits it, and the span
    /// of `offset`..`offset + count` elements of `T` is the region's
    /// to access — returned as its byte offset and length.
    fn registered_span<T: RemoteSafe>(
        &self,
        op: &str,
        offset: u64,
        count: usize,
        buf: &RegisteredBuffer<T>,
    ) -> io::Result<(u64, usize)> {
        if buf.group != self.group {
            return Err(invalid(format!(
                "the buffer is registered for group {}, not the region's group {}",
                buf.group, self.group
            )));
        }
        if count > buf.buf.len() {
            return Err(invalid(format!(
                "{op} of {count} elements exceeds the registered buffer of {} elements",
                buf.buf.len()
            )));
        }
        self.typed_span::<T>(op, offset, count)
    }

    /// The byte offset and length of the span of `count` elements of
    /// `T` starting at element `offset`, after the checks every typed
    /// operation of the region makes: `T` matches the registered
    /// layout, the span lies within the region, and its start is
    /// aligned for `T`. `op` names the operation in error messages.
    fn typed_span<T: RemoteSafe>(&self, op: &str, offset: u64, count: usize) -> io::Result<(u64, usize)> {
        self.check_elem::<T>()?;
        let elem = size_of::<T>();
        let byte_offset = offset.saturating_mul(elem as u64);
        let Some(byte_len) = count.checked_mul(elem) else {
            return Err(invalid(format!(
                "{op} of {count} elements of {elem} bytes overflows the length"
            )));
        };
        if byte_offset.saturating_add(byte_len as u64) > self.size as u64 {
            return Err(invalid(format!(
                "{op} of {count} elements at element offset {offset} exceeds the region of {} elements of {elem} bytes",
                self.size / elem
            )));
        }
        if !(self.remote_addr + byte_offset).is_multiple_of(align_of::<T>() as u64) {
            return Err(invalid(format!(
                "{op} at element offset {offset} is misaligned for elements of alignment {}",
                align_of::<T>()
            )));
        }
        Ok((byte_offset, byte_len))
    }

    /// Reject a `T` whose layout is not the layout the region is
    /// shared as.
    fn check_elem<T: RemoteSafe>(&self) -> io::Result<()> {
        if size_of::<T>() as u32 == self.elem_size && align_of::<T>() as u32 == self.elem_align {
            return Ok(());
        }
        Err(invalid(format!(
            "the region is shared as elements of {} (size {}, alignment {}), not {} (size {}, alignment {})",
            self.elem_type,
            self.elem_size,
            self.elem_align,
            type_name::<T>(),
            size_of::<T>(),
            align_of::<T>()
        )))
    }
}

/// A buffer registered once for the RDMA connection of one group: the
/// zero-copy data path.
///
/// Created by [`RemoteMemoryProvider::register_buffer`] or
/// [`RemoteMemoryProvider::register_buffer_typed`] after the group's
/// session has run, and read and written through a
/// [`RemoteMemoryRegion`] of the same group with
/// [`RemoteMemoryRegion::read_into_registered`] and
/// [`RemoteMemoryRegion::write_from_registered`]: the device moves
/// data straight between the region and this buffer — no per-call
/// registration, no staging copy.
///
/// The buffer never moves or reallocates: the registration pins its
/// address. If the group's connection is replaced — a fresh session
/// after the old one failed, run by
/// [`RemoteMemoryProvider::update`] — the registration is remade in
/// the new connection's protection domain on the next use. `T`
/// follows the library's element rules: a [`RemoteSafe`] type,
/// matching the layout the sharing side registered the region as.
pub struct RegisteredBuffer<T = u8> {
    /// The group whose connection the buffer is registered in.
    pub(crate) group: u32,
    /// The group's connection slot, shared with the provider's
    /// session for the group.
    pub(crate) connection: Rc<RefCell<GroupConnection>>,
    /// The group generation the registration was made in; a
    /// connection replacement bumps the group's, which is what makes
    /// the next use re-register.
    pub(crate) generation: Cell<u32>,
    /// The registration, in the group connection's protection
    /// domain; absent until first use.
    pub(crate) mr: RefCell<Option<rdma::MemoryRegion>>,
    /// The buffer.
    pub(crate) buf: Vec<T>,
}

impl<T: RemoteSafe> RegisteredBuffer<T> {
    /// The group the buffer is registered for.
    pub fn group(&self) -> u32 {
        self.group
    }

    /// The buffer's element count.
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    /// Whether the buffer has no elements.
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// The buffer's elements, for reading.
    pub fn as_slice(&self) -> &[T] {
        &self.buf
    }

    /// The buffer's elements, for writing — e.g. filling the buffer
    /// before a [`RemoteMemoryRegion::write_from_registered`]. The
    /// registration pins memory, not contents: mutation is safe.
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        &mut self.buf
    }

    /// The registration in the group connection's current protection
    /// domain, remade if the connection was replaced (or never
    /// established) since the last one. The returned borrow outlives
    /// this call; the group's connection cell is borrowed only
    /// inside.
    fn registered(&self) -> io::Result<Ref<'_, rdma::MemoryRegion>> {
        let slot = self.connection.borrow_mut();
        if self.mr.borrow().is_none() || self.generation.get() != slot.generation {
            // Deregister while the old domain is still alive — the
            // registration itself keeps it alive until this returns —
            // then register the buffer, pinned at its address, in the
            // current connection's domain.
            *self.mr.borrow_mut() = None;
            let conn = slot.get()?;
            let len = self.buf.len() * size_of::<T>();
            let mr = conn.register_addr(self.buf.as_ptr() as u64, len)?;
            *self.mr.borrow_mut() = Some(mr);
            self.generation.set(slot.generation);
        }
        drop(slot);
        Ok(Ref::map(self.mr.borrow(), |mr| {
            mr.as_ref().expect("the registration was just made")
        }))
    }
}

impl<T> fmt::Debug for RegisteredBuffer<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RegisteredBuffer")
            .field("group", &self.group)
            .field("len", &self.buf.len())
            .field("generation", &self.generation)
            .field("registered", &self.mr.borrow().is_some())
            .finish()
    }
}

impl RemoteMemoryProvider {
    /// Prepare a provider for the remote machine described by `addr`.
    ///
    /// Nothing connects here: the first [`Self::update`] runs a
    /// session — the metadata exchange and the group's RDMA
    /// connection. Group connections get staging pools of 1 MiB;
    /// tune that with [`Self::with_staging_capacity`].
    pub fn new(addr: RemoteMemoryProviderAddr) -> Self {
        Self {
            addr,
            sessions: RefCell::new(HashMap::new()),
            metadata: RefCell::new(Vec::new()),
            regions: RefCell::new(HashMap::new()),
            staging_capacity: Cell::new(DEFAULT_STAGING_CAPACITY),
        }
    }

    /// Builder-style: set the staging-pool capacity of the group
    /// connections, in bytes (1 MiB by default).
    ///
    /// A group's pool is created lazily by the first read or write
    /// through the group, at the capacity current then; operations
    /// larger than the pool are chunked through it in pool-sized
    /// pieces, so the capacity trades memory for fewer chunks on
    /// large operations. See also [`Self::set_staging_capacity`],
    /// which also affects pools that already exist.
    pub fn with_staging_capacity(self, bytes: usize) -> Self {
        self.staging_capacity.set(clamp_staging_capacity(bytes));
        self
    }

    /// The staging-pool capacity of the group connections, in bytes.
    pub fn staging_capacity(&self) -> usize {
        self.staging_capacity.get()
    }

    /// Change the staging-pool capacity, in bytes, for every group.
    ///
    /// Existing pools are dropped — deregistered — and re-created
    /// lazily by the next operation through them, at the new size;
    /// groups that have not run a session yet pick the capacity up
    /// when they do.
    pub fn set_staging_capacity(&self, bytes: usize) {
        let capacity = clamp_staging_capacity(bytes);
        self.staging_capacity.set(capacity);
        for session in self.sessions.borrow().values() {
            let mut slot = session.connection.borrow_mut();
            slot.capacity = capacity;
            slot.staging = None;
        }
    }

    /// The inline budget the device granted the group's RDMA
    /// connection, in bytes.
    ///
    /// Writes of at most this many bytes travel inline, straight out
    /// of the caller's memory; larger ones copy through the group
    /// connection's staging pool (see [`RemoteMemoryRegion::write`]).
    /// The budget is granted when the group's session runs its
    /// rendezvous, so a group that has not updated yet has none to
    /// report.
    pub fn inline_cap(&self, group: u32) -> io::Result<usize> {
        let connection = self
            .sessions
            .borrow()
            .get(&group)
            .map(|session| Rc::clone(&session.connection))
            .ok_or_else(|| {
                io::Error::other(
                    "the group has no session: a successful update is required first",
                )
            })?;
        let slot = connection.borrow();
        let conn = slot.get()?;
        Ok(conn.inline_cap())
    }

    /// Register `buffer` for the connection of `group`, once: the
    /// zero-copy data path.
    ///
    /// The returned [`RegisteredBuffer`] is read and written through
    /// the group's regions with
    /// [`RemoteMemoryRegion::read_into_registered`] and
    /// [`RemoteMemoryRegion::write_from_registered`] — the device
    /// moves data straight between the region and the buffer, with no
    /// per-call registration and no staging copy. The group's session
    /// must have run — a successful [`Self::update`]: the
    /// registration is made in the group connection's protection
    /// domain, which the session's rendezvous creates. An empty
    /// buffer cannot be registered. For a buffer of another element
    /// type, see [`Self::register_buffer_typed`].
    pub fn register_buffer(&self, group: u32, buffer: Vec<u8>) -> io::Result<RegisteredBuffer> {
        self.register_buffer_typed(group, buffer)
    }

    /// Register a `Vec` of `T`s for the connection of `group`, once:
    /// as [`Self::register_buffer`], for the typed element rules —
    /// `T` must be the element type the region is shared as, as
    /// everywhere in the library.
    pub fn register_buffer_typed<T: RemoteSafe>(
        &self,
        group: u32,
        buffer: Vec<T>,
    ) -> io::Result<RegisteredBuffer<T>> {
        if buffer.is_empty() {
            return Err(invalid("cannot register an empty buffer"));
        }
        let connection = self
            .sessions
            .borrow()
            .get(&group)
            .map(|session| Rc::clone(&session.connection))
            .ok_or_else(|| {
                io::Error::other(
                    "the group has no session: a successful update is required first",
                )
            })?;
        let slot = connection.borrow_mut();
        let conn = slot.get()?;
        let len = buffer.len() * size_of::<T>();
        let mr = conn.register_addr(buffer.as_ptr() as u64, len)?;
        let generation = slot.generation;
        drop(slot);
        Ok(RegisteredBuffer {
            group,
            connection,
            generation: Cell::new(generation),
            mr: RefCell::new(Some(mr)),
            buf: buffer,
        })
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
    /// dropped and the next call runs a fresh session. (The rkeys a
    /// re-request picks up are the *current* ones — the fresh rkey of
    /// a re-granted windowed region travels this path.)
    ///
    /// A fresh session replaces the group's RDMA connection: the
    /// connection's staging pool is dropped with it, and registered
    /// buffers re-register into the new connection's protection
    /// domain on their next use. [`Self::reconnect`] forces this
    /// fresh-session path — the recovery for a group whose connection
    /// has failed, which a healthy channel would otherwise never
    /// replace.
    pub fn update(&self, group: u32) -> io::Result<()> {
        let mut sessions = self.sessions.borrow_mut();
        let session = sessions.entry(group).or_insert_with(|| GroupSession {
            channel: None,
            connection: Rc::new(RefCell::new(GroupConnection::with_capacity(
                self.staging_capacity.get(),
            ))),
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
            // connection, which the group's regions read through. The
            // group announced in the connect's private data is what
            // the remote matches its accept against.
            let connection = rdma::Connection::connect_group(
                (self.addr.address.as_str(), self.addr.rdma_port),
                group,
            )?;
            {
                let mut slot = session.connection.borrow_mut();
                // The new connection has a protection domain of its
                // own: drop the staging pool — its registration
                // belongs to the old domain — while the old
                // connection is still alive to own that domain, then
                // swap in the new connection and bump the generation,
                // which is what makes registered buffers re-register
                // on their next use.
                slot.staging = None;
                slot.connection = Some(connection);
                slot.capacity = self.staging_capacity.get();
                slot.generation = slot.generation.wrapping_add(1);
            }

            let exchange = receive_exchange(&mut channel)?;
            session.channel = Some(channel);
            self.ingest(group, exchange)
        }
    }

    /// Forget `group`'s open session, so the next [`Self::update`]
    /// establishes a fresh one from scratch.
    ///
    /// A fresh session is a new metadata exchange *and* a new RDMA
    /// connection: the group's connection is replaced (the staging
    /// pool is dropped with it and registered buffers re-register on
    /// their next use, as in any connection replacement), and the
    /// remote accepts into the group's protection domain, so the
    /// group's tuples keep working over the new connection.
    ///
    /// The recovery path for a group whose connection has *failed*
    /// rather than merely gone stale: a queue pair error leaves the
    /// connection unusable, while a healthy metadata channel would
    /// make [`Self::update`] merely refresh the tuples over it —
    /// never touching the connection. Forgetting the channel here
    /// forces the fresh session that replaces the connection.
    pub fn reconnect(&self, group: u32) {
        let mut sessions = self.sessions.borrow_mut();
        if let Some(session) = sessions.get_mut(&group) {
            session.channel = None;
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
/// the tuples. A `ServeError` reply — the remote cannot serve this
/// exchange — becomes the session's error, carrying the remote's own
/// reason with it.
fn receive_exchange(channel: &mut Channel) -> io::Result<(Message, Message)> {
    let catalog = served(channel.receive()?)?;
    let tuples = served(channel.receive()?)?;
    Ok((catalog, tuples))
}

/// A reply that is not the remote's refusal to serve; the refusal
/// becomes the error, with the remote's reason.
fn served(message: Message) -> io::Result<Message> {
    match message {
        Message::ServeError { message } => Err(io::Error::other(format!(
            "the remote cannot serve this exchange: {message}"
        ))),
        exchange => Ok(exchange),
    }
}

/// Re-request the catalog and the tuples over an open session.
fn refresh(channel: &mut Channel) -> io::Result<(Message, Message)> {
    channel.send(&Message::Update)?;
    receive_exchange(channel)
}
