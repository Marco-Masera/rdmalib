//! libibverbs + rdma_cm implementation of the low-level RDMA commands.
//!
//! The structures below declare only the fields this implementation
//! accesses, mirroring the layout of the installed `infiniband/verbs.h`
//! and `rdma/rdma_cma.h` (ABI-stable up to the declared fields);
//! undeclared trailing fields are never touched. Linking requires
//! `libibverbs` and `librdmacm`.
//!
//! The handle types are `Send` but deliberately not `Sync`: the
//! wrapped C objects have no thread affinity, so a handle can move
//! between threads, while exclusive ownership keeps a single object
//! used by one thread at a time.

use std::ffi::{c_char, c_int, c_uint, c_void, CStr};
use std::fmt;
use std::io;
use std::net::{SocketAddr, SocketAddrV4, ToSocketAddrs};
use std::os::fd::RawFd;
use std::sync::{Arc, Mutex};

use crate::engine::{Completion, CompletionSource, InFlight, Op, OpSource, OpTarget, SWEEP_BATCH};

/// RDMA port space used by the connections: TCP-style, reliable.
const RDMA_PS_TCP: c_int = 0x0106;

/// Timeout of the address and route resolution, in milliseconds.
const RESOLVE_TIMEOUT_MS: c_int = 2000;

/// Depth of a connection's own completion queue; the queue pair is
/// created with as many send work requests.
const CQ_SIZE: c_int = 16;

/// The floor of a shared completion queue's depth, in entries — the
/// depth the first connection on a device creates the queue with
/// (raised to that connection's request if larger, capped by the
/// device's `max_cqe`): a few default-depth connections then share
/// one queue without budget rejects. Completion entries are host
/// memory, so the floor is cheap.
const SHARED_CQ_SIZE: usize = 1024;

/// Byte size of one pooled operation slice: operations at most this
/// large run through the connection's pooled registration — copied
/// in at post for a write, copied out at finish for a read — instead
/// of a registration per operation. Larger operations register
/// directly (the copy would cost more than the registration it
/// saves).
const POOL_ENTRY: usize = 128 * 1024;

/// Pooled slices per connection: the ceiling on operations that run
/// through the pooled registration at once. An operation that finds
/// every slice lent out registers directly (the pool never blocks
/// the engine thread).
const POOL_SLOTS: usize = 32;

/// Connection requests the listener keeps pending.
const LISTEN_BACKLOG: c_int = 16;

/// Address family of [`SockaddrIn`].
const AF_INET: u16 = 2;

// QP type and work request constants (from `infiniband/verbs.h`):
// `IBV_WR_RDMA_WRITE` is the first entry of `enum ibv_wr_opcode`,
// `IBV_WR_RDMA_READ` the fifth.
const IBV_QPT_RC: c_int = 2;
const IBV_WR_RDMA_WRITE: c_int = 0;
const IBV_WR_RDMA_READ: c_int = 4;
const IBV_SEND_SIGNALED: c_uint = 1 << 1;

// MR access flags (from `infiniband/verbs.h`): the remote reader may
// read and write, and future atomic operations are allowed.
const IBV_ACCESS_LOCAL_WRITE: c_int = 1;
const IBV_ACCESS_REMOTE_WRITE: c_int = 1 << 1;
const IBV_ACCESS_REMOTE_READ: c_int = 1 << 2;
const IBV_ACCESS_REMOTE_ATOMIC: c_int = 1 << 3;
const IBV_WC_SUCCESS: c_int = 0;

// Connection manager event codes (from `rdma/rdma_cma.h`).
const RDMA_CM_EVENT_ADDR_RESOLVED: c_int = 0;
const RDMA_CM_EVENT_ROUTE_RESOLVED: c_int = 2;
const RDMA_CM_EVENT_CONNECT_REQUEST: c_int = 4;
const RDMA_CM_EVENT_ESTABLISHED: c_int = 9;

// The structs below mirror the C layouts; some of their fields are
// never read from Rust and exist only to keep the offsets of the
// accessed ones correct.

/// A queue pair (`struct ibv_qp`); only the leading field is
/// accessed, to dispatch commands through the device's ops table.
#[repr(C)]
struct IbvQp {
    context: *mut IbvContext,
}

/// A completion queue (`struct ibv_cq`); only the leading field is
/// accessed, to dispatch commands through the device's ops table.
#[repr(C)]
struct IbvCq {
    context: *mut IbvContext,
}

/// A completion event channel (`struct ibv_comp_channel`); the `fd` is
/// what the engine blocks on — readable when the channel fires a
/// completion event.
#[repr(C)]
#[allow(dead_code)]
struct IbvCompChannel {
    context: *mut IbvContext,
    fd: c_int,
    refcnt: c_int,
}

/// `ibv_post_send`, `ibv_poll_cq`, and `ibv_req_notify_cq` are static
/// inline calls in `infiniband/verbs.h`, not library symbols: they
/// dispatch through the command table of the device handle.
type PostSendFn =
    unsafe extern "C" fn(*mut IbvQp, *mut IbvSendWr, *mut *mut IbvSendWr) -> c_int;
type PollCqFn = unsafe extern "C" fn(*mut IbvCq, c_int, *mut IbvWc) -> c_int;
type ReqNotifyCqFn = unsafe extern "C" fn(*mut IbvCq, c_int) -> c_int;

/// The command-dispatch table of a device (`struct
/// ibv_context_ops`, leading fields only); the unused entries are
/// layout placeholders.
#[repr(C)]
#[allow(dead_code)]
struct IbvContextOps {
    _compat_query_device: *mut c_void,
    _compat_query_port: *mut c_void,
    _compat_alloc_pd: *mut c_void,
    _compat_dealloc_pd: *mut c_void,
    _compat_reg_mr: *mut c_void,
    _compat_rereg_mr: *mut c_void,
    _compat_dereg_mr: *mut c_void,
    alloc_mw: *mut c_void,
    bind_mw: *mut c_void,
    dealloc_mw: *mut c_void,
    _compat_create_cq: *mut c_void,
    poll_cq: PollCqFn,
    req_notify_cq: ReqNotifyCqFn,
    _compat_cq_event: *mut c_void,
    _compat_resize_cq: *mut c_void,
    _compat_destroy_cq: *mut c_void,
    _compat_create_srq: *mut c_void,
    _compat_modify_srq: *mut c_void,
    _compat_query_srq: *mut c_void,
    _compat_destroy_srq: *mut c_void,
    post_srq_recv: *mut c_void,
    _compat_create_qp: *mut c_void,
    _compat_query_qp: *mut c_void,
    _compat_modify_qp: *mut c_void,
    _compat_destroy_qp: *mut c_void,
    post_send: PostSendFn,
}

/// A device handle (`struct ibv_context`); only the leading fields
/// are accessed.
#[repr(C)]
#[allow(dead_code)]
struct IbvContext {
    device: *mut c_void,
    ops: IbvContextOps,
}

/// Opaque protection domain (`struct ibv_pd`).
#[repr(C)]
struct IbvPd {
    _private: [u8; 0],
}

/// Opaque shared receive queue (`struct ibv_srq`).
#[repr(C)]
struct IbvSrq {
    _private: [u8; 0],
}

/// Opaque connection manager event channel
/// (`struct rdma_event_channel`).
#[repr(C)]
struct RdmaEventChannel {
    _private: [u8; 0],
}

/// A registered memory region (`struct ibv_mr`).
#[repr(C)]
#[allow(dead_code)]
struct IbvMr {
    context: *mut IbvContext,
    pd: *mut IbvPd,
    addr: *mut c_void,
    length: usize,
    handle: u32,
    lkey: u32,
    rkey: u32,
}

/// A scatter/gather entry (`struct ibv_sge`).
#[repr(C)]
#[derive(Clone, Copy)]
#[allow(dead_code)]
struct IbvSge {
    addr: u64,
    length: u32,
    lkey: u32,
}

/// The rdma branch of a send work request (`struct ibv_rdma_wr`).
#[repr(C)]
#[derive(Clone, Copy)]
#[allow(dead_code)]
struct IbvRdmaWr {
    remote_addr: u64,
    rkey: u32,
}

/// The operation-specific branch of a send work request (the `wr`
/// union of `struct ibv_send_wr`); only the rdma variant is used.
#[repr(C)]
#[allow(dead_code)]
union SendWrOp {
    rdma: IbvRdmaWr,
}

/// A send work request (`struct ibv_send_wr`).
#[repr(C)]
#[allow(dead_code)]
struct IbvSendWr {
    wr_id: u64,
    next: *mut IbvSendWr,
    sg_list: *mut IbvSge,
    num_sge: c_int,
    opcode: c_int,
    send_flags: c_uint,
    imm_data: u32,
    wr: SendWrOp,
}

/// A completion (`struct ibv_wc`).
#[repr(C)]
#[allow(dead_code)]
struct IbvWc {
    wr_id: u64,
    status: c_int,
    opcode: c_int,
    vendor_err: u32,
    byte_len: u32,
    imm_data: u32,
    qp_num: u32,
    src_qp: u32,
    wc_flags: c_uint,
    pkey_index: u16,
    slid: u16,
    sl: u8,
    dlid_path_bits: u8,
}

/// Queue pair capabilities (`struct ibv_qp_cap`).
#[repr(C)]
#[derive(Clone, Copy)]
#[allow(dead_code)]
struct IbvQpCap {
    max_send_wr: u32,
    max_recv_wr: u32,
    max_send_sge: u32,
    max_recv_sge: u32,
    max_inline_data: u32,
}

/// Queue pair creation attributes (`struct ibv_qp_init_attr`).
#[repr(C)]
#[allow(dead_code)]
struct IbvQpInitAttr {
    qp_context: *mut c_void,
    send_cq: *mut IbvCq,
    recv_cq: *mut IbvCq,
    srq: *mut IbvSrq,
    cap: IbvQpCap,
    qp_type: c_int,
    sq_sig_all: c_int,
}

/// Device attributes (`struct ibv_device_attr`), the leading fields
/// through `max_cqe` — the caps the queue sizing needs (`max_qp_wr`,
/// the per-queue-pair work-request ceiling, is the send-queue depth
/// cap; `max_cqe` the completion-queue entry cap) plus everything
/// before them the kernel writes over. The rest of the struct the
/// kernel writes too, so the mirror carries it as unread tail and
/// must stay full-size (232 bytes): a short buffer would have the
/// kernel write past it.
#[repr(C)]
struct IbvDeviceAttr {
    fw_ver: [u8; 64],
    node_guid: u64,
    sys_image_guid: u64,
    max_mr_size: u64,
    page_size_cap: u64,
    vendor_id: u32,
    vendor_part_id: u32,
    hw_ver: u32,
    max_qp: i32,
    max_qp_wr: i32,
    device_cap_flags: u32,
    max_sge: i32,
    max_sge_rd: i32,
    max_cq: i32,
    max_cqe: i32,
    /// The fields past `max_cqe`, written by the query, read by no
    /// one here.
    _tail: [u8; 232 - 136],
}

/// A communication identifier (`struct rdma_cm_id`); only the leading
/// fields accessed by this implementation are declared.
#[repr(C)]
#[allow(dead_code)]
struct RdmaCmId {
    verbs: *mut IbvContext,
    channel: *mut RdmaEventChannel,
    context: *mut c_void,
    qp: *mut IbvQp,
}

/// A connection manager event (`struct rdma_cm_event`); the trailing
/// `param` union is never accessed.
#[repr(C)]
#[allow(dead_code)]
struct RdmaCmEvent {
    id: *mut RdmaCmId,
    listen_id: *mut RdmaCmId,
    event: c_int,
    status: c_int,
}

/// Connection parameters (`struct rdma_conn_param`).
#[repr(C)]
#[allow(dead_code)]
struct RdmaConnParam {
    private_data: *const c_void,
    private_data_len: u8,
    responder_resources: u8,
    initiator_depth: u8,
    flow_control: u8,
    retry_count: u8,
    rnr_retry_count: u8,
    srq: u8,
    qp_num: u32,
}

/// An IPv4 socket address (`struct sockaddr_in`).
#[repr(C)]
#[allow(dead_code)]
struct SockaddrIn {
    sin_family: u16,
    /// Port, in network byte order.
    sin_port: u16,
    sin_addr: [u8; 4],
    sin_zero: [u8; 8],
}

impl From<SocketAddrV4> for SockaddrIn {
    fn from(addr: SocketAddrV4) -> Self {
        Self {
            sin_family: AF_INET,
            sin_port: addr.port().to_be(),
            sin_addr: addr.ip().octets(),
            sin_zero: [0; 8],
        }
    }
}

#[link(name = "ibverbs")]
unsafe extern "C" {
    fn ibv_alloc_pd(context: *mut IbvContext) -> *mut IbvPd;
    fn ibv_dealloc_pd(pd: *mut IbvPd) -> c_int;
    fn ibv_create_cq(
        context: *mut IbvContext,
        cqe: c_int,
        cq_context: *mut c_void,
        channel: *mut IbvCompChannel,
        comp_vector: c_int,
    ) -> *mut IbvCq;
    fn ibv_destroy_cq(cq: *mut IbvCq) -> c_int;
    fn ibv_create_comp_channel(context: *mut IbvContext) -> *mut IbvCompChannel;
    fn ibv_destroy_comp_channel(channel: *mut IbvCompChannel) -> c_int;
    fn ibv_get_cq_event(
        channel: *mut IbvCompChannel,
        cq: *mut *mut IbvCq,
        cq_context: *mut *mut c_void,
    ) -> c_int;
    fn ibv_ack_cq_events(cq: *mut IbvCq, nevents: c_uint);
    fn ibv_reg_mr(pd: *mut IbvPd, addr: *mut c_void, length: usize, access: c_int) -> *mut IbvMr;
    fn ibv_dereg_mr(mr: *mut IbvMr) -> c_int;
    fn ibv_query_device(
        context: *mut IbvContext,
        device_attr: *mut IbvDeviceAttr,
    ) -> c_int;
    fn ibv_wc_status_str(status: c_int) -> *const c_char;
}

#[link(name = "rdmacm")]
unsafe extern "C" {
    fn rdma_create_event_channel() -> *mut RdmaEventChannel;
    fn rdma_destroy_event_channel(channel: *mut RdmaEventChannel);
    fn rdma_create_id(
        channel: *mut RdmaEventChannel,
        id: *mut *mut RdmaCmId,
        context: *mut c_void,
        ps: c_int,
    ) -> c_int;
    fn rdma_destroy_id(id: *mut RdmaCmId) -> c_int;
    fn rdma_bind_addr(id: *mut RdmaCmId, addr: *mut SockaddrIn) -> c_int;
    fn rdma_listen(id: *mut RdmaCmId, backlog: c_int) -> c_int;
    fn rdma_get_cm_event(channel: *mut RdmaEventChannel, event: *mut *mut RdmaCmEvent) -> c_int;
    fn rdma_ack_cm_event(event: *mut RdmaCmEvent) -> c_int;
    fn rdma_resolve_addr(
        id: *mut RdmaCmId,
        src: *mut SockaddrIn,
        dst: *mut SockaddrIn,
        timeout_ms: c_int,
    ) -> c_int;
    fn rdma_resolve_route(id: *mut RdmaCmId, timeout_ms: c_int) -> c_int;
    fn rdma_create_qp(id: *mut RdmaCmId, pd: *mut IbvPd, attr: *mut IbvQpInitAttr) -> c_int;
    fn rdma_destroy_qp(id: *mut RdmaCmId);
    fn rdma_connect(id: *mut RdmaCmId, conn_param: *mut RdmaConnParam) -> c_int;
    fn rdma_accept(id: *mut RdmaCmId, conn_param: *mut RdmaConnParam) -> c_int;
    fn rdma_disconnect(id: *mut RdmaCmId) -> c_int;
    fn rdma_event_str(event: c_int) -> *const c_char;
}

/// Turn a non-zero C return value into an `io::Error`.
fn check(call: &str, ret: c_int) -> io::Result<()> {
    if ret == 0 {
        Ok(())
    } else {
        Err(io::Error::other(format!(
            "{call} failed: {}",
            io::Error::from_raw_os_error(ret)
        )))
    }
}

/// Turn a null C pointer into an `io::Error`.
fn check_ptr<T>(call: &str, ptr: *mut T) -> io::Result<*mut T> {
    if ptr.is_null() {
        Err(io::Error::other(format!(
            "{call} failed: {}",
            io::Error::last_os_error()
        )))
    } else {
        Ok(ptr)
    }
}

/// Query the device's attributes — the caps the queue sizing runs
/// on: `max_qp_wr`, the per-queue-pair work-request ceiling every
/// send-queue depth is checked against, and `max_cqe`, the
/// completion-queue entry ceiling a queue's depth clamps to. The
/// rest of the attributes the query writes, the mirror carries as
/// unread tail. The return is 0, or the value of `errno` — `check`'s
/// convention exactly.
fn query_device(context: *mut IbvContext) -> io::Result<IbvDeviceAttr> {
    let mut attr: IbvDeviceAttr = unsafe { std::mem::zeroed() };
    check(
        "ibv_query_device",
        unsafe { ibv_query_device(context, &mut attr) },
    )?;
    Ok(attr)
}

fn invalid(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, msg.into())
}

/// Name of a connection manager event, for error messages.
fn event_name(event: c_int) -> String {
    unsafe { CStr::from_ptr(rdma_event_str(event)) }
        .to_string_lossy()
        .into_owned()
}

/// Name of a completion status, for error messages.
fn status_str(status: c_int) -> String {
    unsafe { CStr::from_ptr(ibv_wc_status_str(status)) }
        .to_string_lossy()
        .into_owned()
}

/// Resolve `addr` to an IPv4 socket address, the first of the
/// addresses it names.
///
/// TODO: IPv6 support, and iterating over all resolved addresses.
fn resolve_v4<A: ToSocketAddrs>(addr: A) -> io::Result<SocketAddrV4> {
    let first = addr
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| invalid("the address did not resolve to a socket address"))?;
    match first {
        SocketAddr::V4(v4) => Ok(v4),
        SocketAddr::V6(_) => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "IPv6 addresses are not supported yet",
        )),
    }
}

/// Connection parameters, mirroring the choices of the C library:
/// a few outstanding one-sided operations, generous connect retries.
fn conn_param() -> RdmaConnParam {
    RdmaConnParam {
        private_data: std::ptr::null(),
        private_data_len: 0,
        responder_resources: 3,
        initiator_depth: 3,
        flow_control: 0,
        retry_count: 7,
        rnr_retry_count: 0,
        srq: 0,
        qp_num: 0,
    }
}

/// Block on `channel` until `expected` arrives, acknowledging it.
fn wait_event(channel: *mut RdmaEventChannel, expected: c_int, step: &str) -> io::Result<()> {
    let mut event: *mut RdmaCmEvent = std::ptr::null_mut();
    check(step, unsafe { rdma_get_cm_event(channel, &mut event) })?;
    let (event_type, status) = unsafe { ((*event).event, (*event).status) };
    let ack = unsafe { rdma_ack_cm_event(event) };
    if event_type != expected || status != 0 {
        return Err(io::Error::other(format!(
            "{step}: expected {}, got {} (status {status})",
            event_name(expected),
            event_name(event_type)
        )));
    }
    check("rdma_ack_cm_event", ack)
}

/// A locally registered memory region.
///
/// Registering a buffer on a [`Connection`] pins it for RDMA access
/// from that connection: the tuple (remote_addr, size, rkey) tells
/// remote readers how to reach it, and `lkey` authorizes local
/// operations, such as the destination buffer of a read or the source
/// of a write.
///
/// Dropping the region deregisters it. Safety contracts: the buffer
/// must outlive the region without moving or being resized, and the
/// region must be dropped before its connection.
pub struct MemoryRegion {
    mr: *mut IbvMr,
    /// The connection's protection domain, checked by read operations.
    pd: *mut IbvPd,
    addr: u64,
    size: usize,
    lkey: u32,
    rkey: u32,
}

// SAFETY: the wrapped registration has no thread affinity, so moving
// it to another thread is sound; access stays serialized by
// ownership, which is why `Sync` is deliberately not implemented.
unsafe impl Send for MemoryRegion {}

impl MemoryRegion {
    /// Address of the region in this machine's address space, as seen
    /// by remote readers.
    pub fn addr(&self) -> u64 {
        self.addr
    }

    /// Size of the region, in bytes.
    pub fn size(&self) -> usize {
        self.size
    }

    /// Local key: authorizes local operations on this region.
    pub fn lkey(&self) -> u32 {
        self.lkey
    }

    /// Remote key: what remote readers need to access this region.
    pub fn rkey(&self) -> u32 {
        self.rkey
    }

    /// The (remote_addr, size, rkey) tuple to hand to remote readers.
    pub fn tuple(&self) -> (u64, usize, u32) {
        (self.addr, self.size, self.rkey)
    }
}

impl Drop for MemoryRegion {
    fn drop(&mut self) {
        unsafe { ibv_dereg_mr(self.mr) };
    }
}

impl fmt::Debug for MemoryRegion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryRegion")
            .field("addr", &self.addr)
            .field("size", &self.size)
            .field("lkey", &self.lkey)
            .field("rkey", &self.rkey)
            .finish()
    }
}

/// A protection domain: the verbs scope in which memory regions are
/// registered and queue pairs are created.
///
/// Connections created in the same domain share the rkeys of the
/// memory registered in it: that is how a group of readers gets
/// access to the same memory with one registration. A domain belongs
/// to a single device; accepting a connection through another
/// device's listener into it fails.
///
/// Cloning a handle shares the domain; it is destroyed once the last
/// handle, connection, and registration are gone. Safety contract:
/// registrations ([`MemoryRegion`]) must be dropped before the last
/// handle.
#[derive(Clone)]
pub struct ProtectionDomain {
    inner: Arc<PdInner>,
}

struct PdInner {
    context: *mut IbvContext,
    pd: *mut IbvPd,
}

// SAFETY: the wrapped domain has no thread affinity, and the verbs
// API permits concurrent use of one protection domain from several
// threads; destruction stays exclusive, by the `Arc` refcount. This
// is what lets clones of the handle live on different threads — and
// why the handle must stay on `Arc`, whose atomic refcount that
// relies on.
unsafe impl Send for PdInner {}
unsafe impl Sync for PdInner {}

impl ProtectionDomain {
    /// Allocate a domain on `context`'s device.
    fn alloc(context: *mut IbvContext) -> io::Result<Self> {
        let pd = check_ptr("ibv_alloc_pd", unsafe { ibv_alloc_pd(context) })?;
        Ok(Self {
            inner: Arc::new(PdInner { context, pd }),
        })
    }

    /// The raw handle, for the FFI calls of this module.
    fn raw(&self) -> *mut IbvPd {
        self.inner.pd
    }

    /// The device the domain belongs to.
    fn context(&self) -> *mut IbvContext {
        self.inner.context
    }

    /// Register the memory at `addr`..`addr + size` in this protection
    /// domain.
    ///
    /// Returns the registered [`MemoryRegion`], as [`Self::register`].
    /// Use this for memory whose address and size are tracked
    /// separately from a Rust borrow, as the high-level providers do:
    /// the memory must stay valid and unmoved while the region is
    /// alive, but no Rust reference to it needs to exist while it is
    /// being registered. The rkey is usable through every connection
    /// of this domain.
    pub fn register_addr(&self, addr: u64, size: usize) -> io::Result<MemoryRegion> {
        if size == 0 {
            return Err(invalid("cannot register an empty region"));
        }
        let mr = check_ptr(
            "ibv_reg_mr",
            unsafe {
                ibv_reg_mr(
                    self.inner.pd,
                    addr as *mut c_void,
                    size,
                    IBV_ACCESS_LOCAL_WRITE
                        | IBV_ACCESS_REMOTE_WRITE
                        | IBV_ACCESS_REMOTE_READ
                        | IBV_ACCESS_REMOTE_ATOMIC,
                )
            },
        )?;
        let (lkey, rkey) = unsafe { ((*mr).lkey, (*mr).rkey) };
        Ok(MemoryRegion {
            mr,
            pd: self.inner.pd,
            addr,
            size,
            lkey,
            rkey,
        })
    }

    /// Register `buffer` in this protection domain.
    ///
    /// Returns the registered [`MemoryRegion`]: its [`MemoryRegion::tuple`]
    /// is what remote readers need to access the buffer remotely; its
    /// [`MemoryRegion::lkey`] is what local operations require. The
    /// buffer may be modified remotely at any time while registered,
    /// through every connection of this domain.
    ///
    /// The buffer must outlive the returned region without moving or
    /// being resized: drop the region first.
    pub fn register(&self, buffer: &[u8]) -> io::Result<MemoryRegion> {
        self.register_addr(buffer.as_ptr() as u64, buffer.len())
    }
}

impl Drop for PdInner {
    fn drop(&mut self) {
        unsafe { ibv_dealloc_pd(self.pd) };
    }
}

impl fmt::Debug for ProtectionDomain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The raw handle is enough to tell domains apart.
        f.debug_struct("ProtectionDomain")
            .field("pd", &self.inner.pd)
            .finish()
    }
}

/// An established RDMA connection to a remote machine.
///
/// Created by [`Connection::connect`] or accepted by a [`Listener`].
/// Each connection belongs to a [`ProtectionDomain`]: the rkeys of
/// the memory registered in it work through the domain's connections,
/// so connections sharing a domain (a group of readers) share access.
///
/// Operations are synchronous; like every handle of this module the
/// type is `Send` but not `Sync`.
pub struct Connection {
    /// The communication identifier.
    id: *mut RdmaCmId,
    /// Resources, created during the handshake; the queue-less/null
    /// state lets a connection whose setup failed drop cleanly.
    cq: *mut IbvCq,
    /// The protection domain, shared with the connection's group;
    /// absent until the resources are created.
    pd: Option<ProtectionDomain>,
    /// The completion event channel of the connection's completion
    /// queue, for the engine's hybrid idle: its `fd` is what the
    /// engine blocks on between busy windows. Absent until the
    /// resources are created — and absent with a shared completion
    /// queue, which owns its own.
    comp_channel: *mut IbvCompChannel,
    /// Event channel owned by this connection; accepted connections
    /// share the listener's channel and leave this null.
    event_channel: *mut RdmaEventChannel,
    /// The shared completion queue this connection posts into, when it
    /// was created for one ([`Self::connect_shared`]). Holds the queue
    /// alive until this queue pair is destroyed; without one, the
    /// connection owns its queue and channel itself.
    shared: Option<SharedCompletions>,
    /// The send-queue depth this connection posts with — the
    /// operations that may be in flight on it at once. Set at resource
    /// creation; the send-queue bound of the engine's submissions
    /// (see `docs/async_engine.md`) reads it through
    /// [`Self::depth`].
    max_send_wr: usize,
    /// The connection's pooled registration, created lazily at the
    /// first pooled operation (see [`Pool`]); absent until then.
    pool: Option<Pool>,
}

// SAFETY: the wrapped connection has no thread affinity (its domain
// handle is `Send`), so moving it to another thread is sound; access
// stays serialized by ownership, and `Sync` is deliberately not
// implemented — concurrent use of one connection is the engine's
// business, through the `OpSource` seam, not the caller's.
unsafe impl Send for Connection {}

impl Connection {
    /// Connect to the remote machine listening at `addr`.
    ///
    /// Resolves the address and route, creates the connection
    /// resources — the connection's own completion queue — and
    /// establishes the connection. `max_send_wr` is the send-queue
    /// depth it posts with, the operations that may be in flight on
    /// it at once. For the engine's shared queue, use
    /// [`Self::connect_shared`].
    pub fn connect<A: ToSocketAddrs>(addr: A, max_send_wr: usize) -> io::Result<Self> {
        let mut dst = SockaddrIn::from(resolve_v4(addr)?);
        let event_channel =
            check_ptr("rdma_create_event_channel", unsafe { rdma_create_event_channel() })?;
        let mut conn = Self {
            id: std::ptr::null_mut(),
            cq: std::ptr::null_mut(),
            pd: None,
            comp_channel: std::ptr::null_mut(),
            event_channel,
            shared: None,
            max_send_wr: 0,
            pool: None,
        };
        conn.connect_to(&mut dst, &[], max_send_wr)?;
        Ok(conn)
    }

    /// Connect to the remote machine listening at `addr`, posting the
    /// connection's completions into a shared completion queue.
    ///
    /// The queue: the first of `shared` that lives on the
    /// connection's device; none does (or none was given), a fresh one
    /// for that device, returned so the caller can register it with
    /// the engine and keep it for the device's later connections.
    /// Every connection of one engine on one device shares one queue
    /// that way, so the engine's one poll drains them all,
    /// interleaved in arrival order (see `docs/async_engine.md`).
    ///
    /// `max_send_wr` is the send-queue depth the connection posts
    /// with — the operations that may be in flight on it at once. It
    /// is checked here, against the device's `max_qp_wr` and the
    /// shared queue's remaining completion budget (the sum rule:
    /// every live connection's depth sums under the queue's depth),
    /// and a request past either is rejected — surfacing at the
    /// caller's `update` — with both numbers in the message.
    pub fn connect_shared<A: ToSocketAddrs>(
        addr: A,
        shared: &[SharedCompletions],
        max_send_wr: usize,
    ) -> io::Result<(Self, Option<SharedCompletions>)> {
        let mut dst = SockaddrIn::from(resolve_v4(addr)?);
        let event_channel =
            check_ptr("rdma_create_event_channel", unsafe { rdma_create_event_channel() })?;
        let mut conn = Self {
            id: std::ptr::null_mut(),
            cq: std::ptr::null_mut(),
            pd: None,
            comp_channel: std::ptr::null_mut(),
            event_channel,
            shared: None,
            max_send_wr: 0,
            pool: None,
        };
        let new_shared = conn.connect_to(&mut dst, shared, max_send_wr)?;
        Ok((conn, new_shared))
    }

    /// Establish the connection; on failure the partially built `conn`
    /// is dropped by the caller, releasing the resources created so
    /// far. Completes into the first of `shared` on this connection's
    /// device, or a fresh one it creates (and returns) for it.
    /// `max_send_wr` is the checked send-queue depth — see
    /// [`Self::connect_shared`].
    fn connect_to(
        &mut self,
        dst: &mut SockaddrIn,
        shared: &[SharedCompletions],
        max_send_wr: usize,
    ) -> io::Result<Option<SharedCompletions>> {
        check(
            "rdma_create_id",
            unsafe {
                rdma_create_id(
                    self.event_channel,
                    &mut self.id,
                    std::ptr::null_mut(),
                    RDMA_PS_TCP,
                )
            },
        )?;
        check(
            "rdma_resolve_addr",
            unsafe { rdma_resolve_addr(self.id, std::ptr::null_mut(), dst, RESOLVE_TIMEOUT_MS) },
        )?;
        wait_event(
            self.event_channel,
            RDMA_CM_EVENT_ADDR_RESOLVED,
            "rdma_resolve_addr",
        )?;
        check(
            "rdma_resolve_route",
            unsafe { rdma_resolve_route(self.id, RESOLVE_TIMEOUT_MS) },
        )?;
        wait_event(
            self.event_channel,
            RDMA_CM_EVENT_ROUTE_RESOLVED,
            "rdma_resolve_route",
        )?;
        let context = unsafe { (*self.id).verbs };
        let pd = ProtectionDomain::alloc(context)?;
        // The device is known only now, after the route resolved: pick
        // the shared queue living on it, or create one for it — sized
        // by this connection's request (the queue cannot grow after
        // creation, so the first connection's request sizes it).
        let (shared, new) = match shared.iter().find(|sc| sc.context() == context) {
            Some(sc) => (Some(sc.clone()), None),
            None => {
                let sc = SharedCompletions::on_device(context, max_send_wr)?;
                (Some(sc.clone()), Some(sc))
            }
        };
        // The send-queue depth, lent out of the queue's completion
        // budget — the caps of the lend (the device's `max_qp_wr`, the
        // budget) reject a request past either here.
        shared.as_ref().unwrap().lend(max_send_wr)?;
        self.create_resources(pd, shared.as_ref(), max_send_wr)?;
        let mut param = conn_param();
        check("rdma_connect", unsafe { rdma_connect(self.id, &mut param) })?;
        wait_event(self.event_channel, RDMA_CM_EVENT_ESTABLISHED, "rdma_connect")?;
        Ok(new)
    }

    /// Create the completion queue and the queue pair on this
    /// connection's device, in `pd`.
    ///
    /// With a `shared` queue, the queue pair posts its completions
    /// into it (kept alive by the connection) and the connection owns
    /// no queue of its own; without one, it creates its own queue and
    /// its completion event channel — the `fd` a poller could block
    /// on between busy windows.
    fn create_resources(
        &mut self,
        pd: ProtectionDomain,
        shared: Option<&SharedCompletions>,
        max_send_wr: usize,
    ) -> io::Result<()> {
        // The shared queue first, before anything that can fail: the
        // connection returns its lent send-queue depth to the queue's
        // budget in `Drop`, through this field — a failure after the
        // lend must not skip it. (A failure before it tears the whole
        // fresh queue down instead, lending nothing that survives.)
        self.shared = shared.cloned();
        let cq = match shared {
            Some(sc) => sc.raw_cq(),
            None => {
                let context = unsafe { (*self.id).verbs };
                let comp_channel = check_ptr(
                    "ibv_create_comp_channel",
                    unsafe { ibv_create_comp_channel(context) },
                )?;
                let cq = check_ptr(
                    "ibv_create_cq",
                    unsafe { ibv_create_cq(context, CQ_SIZE, std::ptr::null_mut(), comp_channel, 0) },
                )?;
                self.comp_channel = comp_channel;
                self.cq = cq;
                cq
            }
        };

        // A reliable connection signaling every send, like the C
        // library: each read completion is then always reported.
        // `max_send_wr` is the send-queue depth — the operations that
        // may be in flight at once, bounded by the lend that checked
        // it against the device and the queue's budget; the receive
        // side never posts (one-sided operations only), so its depth
        // stays nominal.
        let mut attr = IbvQpInitAttr {
            qp_context: std::ptr::null_mut(),
            send_cq: cq,
            recv_cq: cq,
            srq: std::ptr::null_mut(),
            cap: IbvQpCap {
                max_send_wr: max_send_wr as u32,
                max_recv_wr: CQ_SIZE as u32,
                max_send_sge: 1,
                max_recv_sge: 1,
                max_inline_data: 0,
            },
            qp_type: IBV_QPT_RC,
            sq_sig_all: 1,
        };
        check(
            "rdma_create_qp",
            unsafe { rdma_create_qp(self.id, pd.raw(), &mut attr) },
        )?;
        self.pd = Some(pd);
        self.max_send_wr = max_send_wr;
        Ok(())
    }

    /// The connection's protection domain: memory registered in it is
    /// accessible through this connection and any other connection
    /// created in the same domain (see [`Listener::accept_into`]).
    pub fn protection_domain(&self) -> ProtectionDomain {
        self.pd.clone().expect("the connection is established")
    }

    /// The send-queue depth this connection posts with — the
    /// operations that may be in flight on it at once. The engine's
    /// submission bound runs on it (see `docs/async_engine.md`).
    pub fn depth(&self) -> usize {
        self.max_send_wr
    }

    /// Register the memory at `addr`..`addr + size` in this
    /// connection's protection domain.
    ///
    /// As [`ProtectionDomain::register_addr`], for buffers whose
    /// address and size are tracked separately from a Rust borrow:
    /// the memory must stay valid and unmoved while the region is
    /// alive, but no Rust reference to it needs to exist while it is
    /// being registered or read into. This is the engine's op-buffer
    /// path, through the `OpSource` seam below — one-sided operations
    /// run through the engine (see `docs/async_engine.md`), so a
    /// connection offers no read/write calls of its own.
    fn register_addr(&self, addr: u64, size: usize) -> io::Result<MemoryRegion> {
        self.protection_domain().register_addr(addr, size)
    }

    /// Post one one-sided RDMA operation tagged `wr_id`, without
    /// waiting for its completion.
    ///
    /// The checks of the operation path live here: the zero-length
    /// and out-of-bounds checks, and the protection-domain check.
    /// `opcode` picks the operation ([`IBV_WR_RDMA_READ`] or
    /// [`IBV_WR_RDMA_WRITE`]); `op` names it in error messages. The
    /// first `len` bytes of `mr` at `offset` are the operation's
    /// local end — the destination of a read, the source of a write;
    /// a pooled operation's slice names its offset into the pool's
    /// registration, a directly registered one a 0 into its own — and
    /// `remote_addr`, accessed with `rkey`, its remote end; `mr` must
    /// be registered in this connection's protection domain. The
    /// completion is polled by the engine, and its `wr_id` is how
    /// the poller tells the posted operations apart.
    #[allow(clippy::too_many_arguments)] // the verbs call it mirrors is this wide
    fn post(
        &self,
        op: &str,
        opcode: c_int,
        mr: &MemoryRegion,
        offset: usize,
        remote_addr: u64,
        rkey: u32,
        len: usize,
        wr_id: u64,
    ) -> io::Result<()> {
        if len == 0 {
            return Err(invalid(format!("cannot {op} zero bytes")));
        }
        if offset.saturating_add(len) > mr.size {
            return Err(invalid(format!(
                "{op} of {len} bytes at offset {offset} exceeds the registered region of {} bytes",
                mr.size
            )));
        }
        let pd = self.pd.as_ref().expect("the connection is established");
        if mr.pd != pd.raw() {
            return Err(invalid(
                "the memory region is registered in another protection domain",
            ));
        }

        let mut sge = IbvSge {
            addr: mr.addr + offset as u64,
            length: len as u32,
            lkey: mr.lkey,
        };
        let mut send_wr = IbvSendWr {
            wr_id,
            next: std::ptr::null_mut(),
            sg_list: &mut sge,
            num_sge: 1,
            opcode,
            send_flags: IBV_SEND_SIGNALED,
            imm_data: 0,
            wr: SendWrOp {
                rdma: IbvRdmaWr { remote_addr, rkey },
            },
        };
        let mut bad_wr: *mut IbvSendWr = std::ptr::null_mut();
        // ibv_post_send is a static inline call in verbs.h, dispatched
        // through the device's command table.
        let qp = unsafe { (*self.id).qp };
        let post_send = unsafe { (*(*qp).context).ops.post_send };
        check(
            "ibv_post_send",
            unsafe { post_send(qp, &mut send_wr, &mut bad_wr) },
        )
    }

}

impl Drop for Connection {
    fn drop(&mut self) {
        // Best effort: disconnected or never-connected ids return an
        // error, which is ignored here. The shared protection domain
        // (a struct field) drops after this, deallocating the domain
        // once its last connection and registration are gone. The
        // shared completion queue (a struct field too) also drops
        // after this — after the queue pair above is destroyed, which
        // is the order the queue requires.
        unsafe {
            if !self.id.is_null() {
                if !(*self.id).qp.is_null() {
                    rdma_disconnect(self.id);
                    rdma_destroy_qp(self.id);
                }
                rdma_destroy_id(self.id);
            }
            // Only the connection's own queue: a shared one is owned
            // by the SharedCompletions the field holds.
            if self.shared.is_none() {
                if !self.cq.is_null() {
                    ibv_destroy_cq(self.cq);
                }
                if !self.comp_channel.is_null() {
                    ibv_destroy_comp_channel(self.comp_channel);
                }
            }
            if !self.event_channel.is_null() {
                rdma_destroy_event_channel(self.event_channel);
            }
        }
        // The lent send-queue depth returns only now, after the queue
        // pair is destroyed: its work requests can complete no more,
        // so its share of the shared queue's completion budget is
        // free for the next connection of the device.
        if let Some(sc) = &self.shared {
            sc.return_budget(self.max_send_wr);
        }
    }
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The raw identifier is enough to tell connections apart.
        f.debug_struct("Connection").field("id", &self.id).finish()
    }
}

/// The verbs half of the operations engine (src/engine.rs): every
/// registered connection reaches the engine as an [`OpSource`], and
/// the engine drives it through this seam — posting and closing — so
/// the engine itself knows no verbs and runs, tested, against
/// injected fakes instead of hardware. Completions arrive through the
/// [`SharedCompletions`] the connection posts into, as a
/// [`CompletionSource`].
impl OpSource for Connection {
    fn depth(&self) -> usize {
        self.max_send_wr
    }

    fn submit(
        &mut self,
        op: Op,
        addr: u64,
        len: usize,
        target: OpTarget,
        wr_id: u64,
    ) -> io::Result<Box<dyn InFlight>> {
        let opcode = match op {
            Op::Read => IBV_WR_RDMA_READ,
            Op::Write => IBV_WR_RDMA_WRITE,
        };
        // The pooled path first: an operation at most [`POOL_ENTRY`]
        // bytes runs through a slice of the connection's pooled
        // registration — a copy at post for a write, a copy at finish
        // for a read — reusing the one registration for every such
        // operation instead of registering each one's buffer.
        if let Some(pooled) = self.pooled(op, addr, len)? {
            self.post(
                op.name(),
                opcode,
                &pooled.pool.mr,
                pooled.offset,
                target.remote_addr,
                target.rkey,
                len,
                wr_id,
            )?;
            return Ok(pooled.flight);
        }
        // The direct path: register the operation's local memory in
        // this connection's domain, post the operation, and hand the
        // registration back as the in-flight hold: the engine keeps
        // it until the completion is processed — its drop
        // deregisters, so the memory is released only after the
        // device is done with it.
        let mr = self.register_addr(addr, len)?;
        self.post(
            op.name(),
            opcode,
            &mr,
            0,
            target.remote_addr,
            target.rkey,
            len,
            wr_id,
        )?;
        Ok(Box::new(mr))
    }

    fn close(&mut self) {
        // Best effort: the queue pair moves to the error state and
        // its outstanding operations flush as error completions, which
        // the engine resolves; an error (an id never fully connected)
        // is ignored — the drop that follows tears it down anyway.
        unsafe { rdma_disconnect(self.id) };
    }
}

impl Connection {
    /// The pooled path of a submitted operation, when one fits: a
    /// slice of the connection's pooled registration lent out for it,
    /// with its offset into the registration (the post reads the
    /// local end from there) and the in-flight hold that returns it.
    /// A write's bytes are copied into the slice here; a read's come
    /// back at the hold's finish. Returns `None` — the operation
    /// registers directly — when it is larger than [`POOL_ENTRY`] or
    /// the pool is fully lent out (the pool never blocks the engine
    /// thread).
    fn pooled(&mut self, op: Op, addr: u64, len: usize) -> io::Result<Option<Pooled>> {
        if len == 0 || len > POOL_ENTRY {
            return Ok(None);
        }
        let pool = match self.pool.as_ref() {
            Some(pool) => Arc::clone(&pool.inner),
            None => {
                // The first pooled operation creates the pool: one
                // allocation, registered once, reused for every
                // pooled operation of this connection from now on.
                let pool = Pool::new(&self.protection_domain())?;
                let inner = Arc::clone(&pool.inner);
                self.pool = Some(pool);
                inner
            }
        };
        let Some(entry) = pool.free.lock().unwrap().pop() else {
            return Ok(None); // fully lent out: register directly
        };
        let offset = entry as usize * POOL_ENTRY;
        if matches!(op, Op::Write) {
            // The device reads the bytes from the pool, so they must
            // be here before the post. Safe: the operation's buffer
            // is parked in the engine's slot, owned and untouched
            // from submission to resolution, and its heap never
            // moves — the same lifetime guarantee the direct
            // registration relies on.
            unsafe {
                std::ptr::copy_nonoverlapping(
                    addr as *const u8,
                    (pool.mr.addr as *mut u8).add(offset),
                    len,
                );
            }
        }
        let flight = PooledFlight {
            pool: Arc::clone(&pool),
            entry: Some(entry),
            addr,
            len,
            read: matches!(op, Op::Read),
        };
        Ok(Some(Pooled {
            pool,
            offset,
            flight: Box::new(flight),
        }))
    }
}

/// What a pooled submission runs with: the pool (its registration is
/// the post's local end), the lent slice's byte offset into it, and
/// the in-flight hold that returns the slice.
struct Pooled {
    pool: Arc<PoolInner>,
    offset: usize,
    flight: Box<dyn InFlight>,
}

/// The pooled registration of one connection: one contiguous
/// allocation, registered once, its [`POOL_ENTRY`]-byte slices lent
/// to operations one at a time — a registration per operation
/// replaced by a copy in (a write) and a copy out (a read), at a
/// fraction of the cost.
struct Pool {
    inner: Arc<PoolInner>,
}

struct PoolInner {
    /// The registration of the whole pool. First field: it must
    /// deregister, here on the engine thread, before the allocation
    /// it pins (`memory`, below) drops.
    mr: MemoryRegion,
    /// The pooled allocation, pinned by `mr` — never resized or
    /// moved while the registration lives. Read at creation only: it
    /// exists to own — and, when the last holder drops, free — the
    /// memory the registration pins, not to be read through.
    #[allow(dead_code)] // owning is its whole job
    memory: Vec<u8>,
    /// The free slices, as their indices — lent from the end, so the
    /// first operations share cache lines early in the pool.
    free: Mutex<Vec<u32>>,
}

// SAFETY: nothing here runs verbs calls through shared references —
// a shared `PoolInner` reads its registration's captured fields
// (addr, lkey) and the free list, both plain data behind the
// `Mutex` or never written after creation — and the one verbs call
// on the registration (its deregister) happens exactly once, in
// `Drop`, under the `Arc`'s exclusive final reference. This is what
// lets in-flight holds of a connection's pool live on the engine
// thread while the connection itself stays on it too.
unsafe impl Send for PoolInner {}
unsafe impl Sync for PoolInner {}

impl Pool {
    fn new(pd: &ProtectionDomain) -> io::Result<Self> {
        let memory = vec![0u8; POOL_SLOTS * POOL_ENTRY];
        let mr = pd.register_addr(memory.as_ptr() as u64, memory.len())?;
        // The registration and the allocation agree — the registration's
        // captured address is what every lent slice is offset from.
        debug_assert_eq!(mr.addr, memory.as_ptr() as u64);
        let free = Mutex::new((0..POOL_SLOTS as u32).rev().collect());
        Ok(Self {
            inner: Arc::new(PoolInner { mr, memory, free }),
        })
    }
}

/// The in-flight hold of a pooled operation: the lent slice, copied
/// back and returned at finish, plus the operation's own buffer
/// address, copied into at finish on a successful read.
struct PooledFlight {
    pool: Arc<PoolInner>,
    entry: Option<u32>,
    addr: u64,
    len: usize,
    read: bool,
}

impl InFlight for PooledFlight {
    fn finish(&mut self, ok: bool) {
        let Some(entry) = self.entry else {
            return; // already finished (a duplicate completion)
        };
        if self.read && ok {
            // The device wrote into the pool; the operation's buffer
            // — the engine slot's `Vec`'s heap, valid and untouched
            // until it resolves right after this — takes the bytes
            // here, before its handle sees them.
            unsafe {
                std::ptr::copy_nonoverlapping(
                    (self.pool.mr.addr as *const u8).add(entry as usize * POOL_ENTRY),
                    self.addr as *mut u8,
                    self.len,
                );
            }
        }
        self.entry = None;
        self.pool.free.lock().unwrap().push(entry);
    }
}

impl Drop for PooledFlight {
    fn drop(&mut self) {
        // Defensive only: every resolved path finishes first, and a
        // finish already returned the slice. A drop without one (an
        // engine bug, or a slot vanished between post and completion)
        // still returns the slice — without the copy, the outcome is
        // gone — so a leak degrades the pool, never the safety.
        if let Some(entry) = self.entry.take() {
            self.pool.free.lock().unwrap().push(entry);
        }
    }
}

/// The in-flight hold of a directly registered operation: the
/// registration itself. Finishing has nothing to do; the drop
/// deregisters, which is exactly the release.
impl InFlight for MemoryRegion {
    fn finish(&mut self, _ok: bool) {}
}

/// A shared completion queue with its completion event channel, on one
/// device: the completion half of one engine — every connection
/// created for it ([`Connection::connect_shared`]) posts its
/// completions into the queue, so one poll drains them all,
/// interleaved in arrival order. One per (engine, device): a
/// connection landing on another device gets a queue of its own.
///
/// Cloning a handle shares the queue; it is destroyed once the last
/// handle, connection, and engine registration are gone — after
/// every queue pair posting into it. Safety contract: no queue pair
/// of a connection may outlive the last handle.
#[derive(Clone)]
pub struct SharedCompletions {
    inner: Arc<ScInner>,
}

struct ScInner {
    /// The device the queue lives on — the connection-creation match.
    context: *mut IbvContext,
    cq: *mut IbvCq,
    channel: *mut IbvCompChannel,
    /// The device's per-queue-pair work-request ceiling, queried once
    /// at creation — the hardware cap every connection's requested
    /// send-queue depth is checked against (see [`SharedCompletions::lend`]).
    max_qp_wr: u32,
    /// The device's completion-queue entry ceiling, likewise — the
    /// queue's own depth was clamped to it at creation. Read at
    /// creation only; kept for diagnostics.
    #[allow(dead_code)]
    max_cqe: u32,
    /// The completion budget, `(lent, depth)`: the sum of the send-queue
    /// depths lent to live connections, under the queue's depth — the
    /// sum rule, so a simultaneous completion burst from every
    /// connection cannot overrun the queue. Behind a mutex: a
    /// connection lends on its owner's thread, a dead one returns its
    /// depth on the engine thread (its drop).
    budget: Mutex<(usize, usize)>,
}

// SAFETY: the wrapped queue has no thread affinity, and the verbs API
// permits a CQ to be referenced (queue pairs created on it) from
// several threads; polling stays on the engine thread, destruction
// exclusive by the `Arc` refcount. The rest is plain data behind the
// budget mutex. This is what lets clones of the handle live on
// different threads — and why the handle must stay on `Arc`, whose
// atomic refcount that relies on.
unsafe impl Send for ScInner {}
unsafe impl Sync for ScInner {}

impl SharedCompletions {
    /// A shared queue on `context`'s device, `min_depth` entries deep
    /// — at least [`SHARED_CQ_SIZE`], at least the first connection's
    /// request, never past the device's `max_cqe` (queried here, once
    /// per queue) — with its caps remembered for the later
    /// connections' depth checks.
    fn on_device(context: *mut IbvContext, min_depth: usize) -> io::Result<Self> {
        let attr = query_device(context)?;
        let depth = SHARED_CQ_SIZE.max(min_depth).min(attr.max_cqe.max(0) as usize);
        let channel = check_ptr("ibv_create_comp_channel", unsafe {
            ibv_create_comp_channel(context)
        })?;
        let cq = check_ptr(
            "ibv_create_cq",
            unsafe { ibv_create_cq(context, depth as c_int, std::ptr::null_mut(), channel, 0) },
        )?;
        Ok(Self {
            inner: Arc::new(ScInner {
                context,
                cq,
                channel,
                max_qp_wr: attr.max_qp_wr.max(0) as u32,
                max_cqe: attr.max_cqe.max(0) as u32,
                budget: Mutex::new((0, depth)),
            }),
        })
    }

    /// The device the queue lives on.
    fn context(&self) -> *mut IbvContext {
        self.inner.context
    }

    /// The raw queue, for the queue pair of a connection created for
    /// this shared queue.
    fn raw_cq(&self) -> *mut IbvCq {
        self.inner.cq
    }

    /// The device's per-queue-pair work-request ceiling — the
    /// hardware cap of every send-queue depth lent from this queue's
    /// device.
    pub fn max_qp_wr(&self) -> u32 {
        self.inner.max_qp_wr
    }

    /// Lend `request` entries of the queue's completion budget to a
    /// new connection — the send-queue depth it posts with. Two caps,
    /// a request past either rejected with both numbers named: the
    /// device's `max_qp_wr`, and the budget remaining after the
    /// depths lent to the device's live connections (the sum rule:
    /// every live connection's send-queue depth must sum under the
    /// queue's depth, or a simultaneous completion burst overruns
    /// it).
    fn lend(&self, request: usize) -> io::Result<()> {
        if request > self.inner.max_qp_wr as usize {
            return Err(invalid(format!(
                "max_send_wr {request} exceeds the device's max_qp_wr {}",
                self.inner.max_qp_wr
            )));
        }
        let mut budget = self.inner.budget.lock().unwrap();
        let remaining = budget.1.saturating_sub(budget.0);
        if request > remaining {
            return Err(invalid(format!(
                "max_send_wr {request} exceeds the shared completion queue's remaining budget: queue depth {}, already lent {}, remaining {remaining}",
                budget.1, budget.0
            )));
        }
        budget.0 += request;
        Ok(())
    }

    /// Return a dead connection's lent depth to the queue's budget —
    /// its share is free again once its queue pair is gone.
    fn return_budget(&self, depth: usize) {
        let mut budget = self.inner.budget.lock().unwrap();
        budget.0 = budget.0.saturating_sub(depth);
    }
}

impl Drop for ScInner {
    fn drop(&mut self) {
        // The queue before its channel; a queue with unacknowledged
        // events would wait for them (the engine acknowledges
        // one-to-one).
        unsafe {
            ibv_destroy_cq(self.cq);
            ibv_destroy_comp_channel(self.channel);
        }
    }
}

impl fmt::Debug for SharedCompletions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The raw queue is enough to tell shared queues apart.
        f.debug_struct("SharedCompletions")
            .field("cq", &self.inner.cq)
            .finish()
    }
}

/// The completion half of the operations engine: the shared queue
/// polls — every connection on its device, interleaved — and its
/// event channel is what the engine blocks on between busy windows.
impl CompletionSource for SharedCompletions {
    fn poll(&mut self, out: &mut [Completion]) -> io::Result<usize> {
        // A fixed batch of raw completions, at most the engine's sweep
        // batch (SWEEP_BATCH) per poll, from every connection posting
        // into the shared queue.
        let mut wc: [IbvWc; SWEEP_BATCH] = unsafe { std::mem::zeroed() };
        let want = out.len().min(SWEEP_BATCH);
        // ibv_poll_cq is a static inline call in verbs.h, dispatched
        // through the device's command table; a negative return is the
        // errno.
        let poll_cq = unsafe { (*(*self.inner.cq).context).ops.poll_cq };
        let ret = unsafe { poll_cq(self.inner.cq, want as c_int, wc.as_mut_ptr()) };
        if ret < 0 {
            return Err(io::Error::other(format!(
                "ibv_poll_cq failed: {}",
                io::Error::from_raw_os_error(-ret)
            )));
        }
        for i in 0..ret as usize {
            let ok = wc[i].status == IBV_WC_SUCCESS;
            out[i] = Completion {
                wr_id: wc[i].wr_id,
                ok,
                error: if ok {
                    String::new()
                } else {
                    format!(
                        "RDMA operation failed: {} (vendor error {})",
                        status_str(wc[i].status),
                        wc[i].vendor_err
                    )
                },
            };
        }
        Ok(ret as usize)
    }

    fn event_fd(&self) -> Option<RawFd> {
        // The completion channel's fd: readable when the armed queue
        // fires a completion event.
        Some(unsafe { (*self.inner.channel).fd })
    }

    fn arm(&mut self) -> io::Result<()> {
        // ibv_req_notify_cq is a static inline call in verbs.h,
        // dispatched through the device's command table, like
        // ibv_post_send; a negative return is the errno. The event
        // fires when a completion is ADDED after this — one already
        // sitting in the queue does not — which is why the engine
        // polls once more before blocking.
        let req_notify = unsafe { (*(*self.inner.cq).context).ops.req_notify_cq };
        check("ibv_req_notify_cq", unsafe { req_notify(self.inner.cq, 0) })
    }

    fn consume_events(&mut self) -> io::Result<()> {
        // poll(2) said the channel is readable, so one event is
        // pending; leftovers keep the channel readable, and the next
        // block's poll returns at once — so exactly one get per
        // readable poll, never a draining loop: a get without a
        // pending event blocks.
        let mut cq: *mut IbvCq = std::ptr::null_mut();
        let mut cq_context: *mut c_void = std::ptr::null_mut();
        let ret = unsafe { ibv_get_cq_event(self.inner.channel, &mut cq, &mut cq_context) };
        if ret != 0 {
            return Err(io::Error::other(format!(
                "ibv_get_cq_event failed: {}",
                io::Error::last_os_error()
            )));
        }
        // One ack per successful get, one-to-one: destroying the
        // queue waits for every event to be acknowledged.
        unsafe { ibv_ack_cq_events(cq, 1) };
        Ok(())
    }
}

/// A listening RDMA endpoint.
///
/// Binds to a local address and port, and accepts [`Connection`]s,
/// one per remote reader.
pub struct Listener {
    id: *mut RdmaCmId,
    event_channel: *mut RdmaEventChannel,
}

// SAFETY: the wrapped listener has no thread affinity, so moving it
// to another thread is sound; accepting remains serialized by
// ownership (`Sync` deliberately not implemented).
unsafe impl Send for Listener {}

impl Listener {
    /// Bind to `addr` and listen for RDMA connection requests.
    ///
    /// A specific address selects the RDMA device of that interface; a
    /// wildcard address listens across devices. Accepted connections
    /// create their resources on the device they arrive on.
    pub fn bind<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        let mut sin = SockaddrIn::from(resolve_v4(addr)?);
        let event_channel =
            check_ptr("rdma_create_event_channel", unsafe { rdma_create_event_channel() })?;
        let mut id: *mut RdmaCmId = std::ptr::null_mut();
        check(
            "rdma_create_id",
            unsafe {
                rdma_create_id(
                    event_channel,
                    &mut id,
                    std::ptr::null_mut(),
                    RDMA_PS_TCP,
                )
            },
        )?;
        let result = check("rdma_bind_addr", unsafe { rdma_bind_addr(id, &mut sin) })
            .and_then(|()| check("rdma_listen", unsafe { rdma_listen(id, LISTEN_BACKLOG) }));
        match result {
            Ok(()) => Ok(Self { id, event_channel }),
            Err(e) => {
                unsafe {
                    rdma_destroy_id(id);
                    rdma_destroy_event_channel(event_channel);
                }
                Err(e)
            }
        }
    }

    /// Block until a connection request arrives, and accept it,
    /// creating the connection's own protection domain.
    ///
    /// Creates the connection resources on the requested device, then
    /// accepts. The established event of an accepted connection is
    /// reported on this listener's event channel.
    pub fn accept(&self) -> io::Result<Connection> {
        self.accept_impl(None)
    }

    /// Block until a connection request arrives, and accept it into
    /// the shared protection domain `pd`.
    ///
    /// The queue pair is created in `pd`, so the rkeys of the memory
    /// registered in it work through this connection too: this is how
    /// the connections of one group share access. Fails, releasing
    /// the connection, if it arrives on a different device than the
    /// one `pd` belongs to.
    pub fn accept_into(&self, pd: &ProtectionDomain) -> io::Result<Connection> {
        self.accept_impl(Some(pd))
    }

    fn accept_impl(&self, pd: Option<&ProtectionDomain>) -> io::Result<Connection> {
        let mut event: *mut RdmaCmEvent = std::ptr::null_mut();
        check(
            "rdma_get_cm_event",
            unsafe { rdma_get_cm_event(self.event_channel, &mut event) },
        )?;
        let (child, event_type, status) = unsafe { ((*event).id, (*event).event, (*event).status) };
        let ack = unsafe { rdma_ack_cm_event(event) };
        if event_type != RDMA_CM_EVENT_CONNECT_REQUEST || status != 0 {
            return Err(io::Error::other(format!(
                "expected a connection request, got {} (status {status})",
                event_name(event_type)
            )));
        }
        check("rdma_ack_cm_event", ack)?;

        let mut conn = Connection {
            id: child,
            cq: std::ptr::null_mut(),
            pd: None,
            comp_channel: std::ptr::null_mut(),
            // The child id shares this listener's event channel, which
            // this connection must not destroy.
            event_channel: std::ptr::null_mut(),
            shared: None,
            max_send_wr: 0,
            pool: None,
        };

        let context = unsafe { (*child).verbs };
        let pd = match pd {
            // Dropping conn here releases the rejected connection.
            Some(pd) if pd.context() != context => {
                return Err(io::Error::other(
                    "rdma accept_into: the connection arrived on a different device than the protection domain",
                ));
            }
            Some(pd) => pd.clone(),
            None => ProtectionDomain::alloc(context)?,
        };
        // The provider side never posts one-sided operations, so its
        // connections own their (inert) queues: no shared queue here,
        // and a nominal send-queue depth — the bound never bites.
        conn.create_resources(pd, None, CQ_SIZE as usize)?;
        let mut param = conn_param();
        check("rdma_accept", unsafe { rdma_accept(conn.id, &mut param) })?;
        wait_event(self.event_channel, RDMA_CM_EVENT_ESTABLISHED, "rdma_accept")?;
        Ok(conn)
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        unsafe {
            rdma_destroy_id(self.id);
            rdma_destroy_event_channel(self.event_channel);
        }
    }
}

impl fmt::Debug for Listener {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The raw identifier is enough to tell listeners apart.
        f.debug_struct("Listener").field("id", &self.id).finish()
    }
}

#[cfg(test)]
mod tests {
    use std::mem::{offset_of, size_of, align_of};

    use super::*;

    /// The structs above mirror the C layouts of `infiniband/verbs.h`
    /// and `rdma/rdma_cma.h` up to the declared fields; these
    /// assertions pin the Rust side of the contract, including the
    /// offsets of every field the implementation touches. They only
    /// inspect types, so the C libraries are not needed to run them.
    #[test]
    fn verbs_struct_layouts() {
        assert_eq!(size_of::<IbvSge>(), 16);
        assert_eq!(align_of::<IbvSge>(), 8);
        assert_eq!(size_of::<IbvMr>(), 48);
        assert_eq!(offset_of!(IbvMr, handle), 32);
        assert_eq!(offset_of!(IbvMr, lkey), 36);
        assert_eq!(offset_of!(IbvMr, rkey), 40);
        assert_eq!(size_of::<IbvSendWr>(), 56);
        assert_eq!(offset_of!(IbvSendWr, send_flags), 32);
        assert_eq!(offset_of!(IbvSendWr, wr), 40);
        assert_eq!(size_of::<IbvWc>(), 48);
        assert_eq!(offset_of!(IbvWc, status), 8);
        assert_eq!(size_of::<IbvQpCap>(), 20);
        assert_eq!(size_of::<IbvQpInitAttr>(), 64);
        // The device attributes: the mirror must be full-size (the
        // kernel writes the whole struct), with the caps the queue
        // sizing reads at the offsets the header's field order gives
        // — fw_ver[64], five u64s, three u32s, then max_qp at 108 and
        // max_qp_wr at 112, then max_cq at 128 and max_cqe at 132.
        assert_eq!(size_of::<IbvDeviceAttr>(), 232);
        assert_eq!(offset_of!(IbvDeviceAttr, node_guid), 64);
        assert_eq!(offset_of!(IbvDeviceAttr, vendor_id), 96);
        assert_eq!(offset_of!(IbvDeviceAttr, max_qp), 108);
        assert_eq!(offset_of!(IbvDeviceAttr, max_qp_wr), 112);
        assert_eq!(offset_of!(IbvDeviceAttr, device_cap_flags), 116);
        assert_eq!(offset_of!(IbvDeviceAttr, max_cq), 128);
        assert_eq!(offset_of!(IbvDeviceAttr, max_cqe), 132);
        assert_eq!(size_of::<IbvQp>(), 8);
        assert_eq!(offset_of!(IbvQp, context), 0);
        assert_eq!(size_of::<IbvCq>(), 8);
        assert_eq!(offset_of!(IbvCq, context), 0);
        // The completion event channel: the context pointer, then the
        // fd the engine blocks on.
        assert_eq!(size_of::<IbvCompChannel>(), 16);
        assert_eq!(offset_of!(IbvCompChannel, fd), 8);
        // The ops table: every entry is a pointer, with poll_cq at
        // index 11, req_notify_cq at 12, and post_send at 25.
        assert_eq!(size_of::<IbvContextOps>(), 208);
        assert_eq!(offset_of!(IbvContext, ops), 8);
        assert_eq!(offset_of!(IbvContextOps, poll_cq), 88);
        assert_eq!(offset_of!(IbvContextOps, req_notify_cq), 96);
        assert_eq!(offset_of!(IbvContextOps, post_send), 200);
        assert_eq!(size_of::<SockaddrIn>(), 16);
        assert_eq!(offset_of!(SockaddrIn, sin_port), 2);
        assert_eq!(offset_of!(SockaddrIn, sin_addr), 4);
    }

    #[test]
    fn cm_struct_layouts() {
        assert_eq!(size_of::<RdmaConnParam>(), 24);
        assert_eq!(size_of::<RdmaCmEvent>(), 24);
        assert_eq!(offset_of!(RdmaCmEvent, id), 0);
        assert_eq!(offset_of!(RdmaCmEvent, event), 16);
        assert_eq!(offset_of!(RdmaCmEvent, status), 20);
        assert_eq!(size_of::<RdmaCmId>(), 32);
        assert_eq!(offset_of!(RdmaCmId, verbs), 0);
        assert_eq!(offset_of!(RdmaCmId, channel), 8);
        assert_eq!(offset_of!(RdmaCmId, qp), 24);
    }
}
