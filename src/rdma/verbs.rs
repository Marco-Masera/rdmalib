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

use std::cell::Cell;
use std::ffi::{c_char, c_int, c_uint, c_void, CStr};
use std::fmt;
use std::io;
use std::net::{SocketAddr, SocketAddrV4, ToSocketAddrs};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// RDMA port space used by the connections: TCP-style, reliable.
const RDMA_PS_TCP: c_int = 0x0106;

/// Timeout of the address and route resolution, in milliseconds.
const RESOLVE_TIMEOUT_MS: c_int = 2000;

/// How long [`Connection::read`] waits for the read completion.
const POLL_TIMEOUT: Duration = Duration::from_secs(10);

/// Depth of the per-connection completion queue; the queue pair is
/// created with as many send work requests.
const CQ_SIZE: c_int = 16;

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
// Send flags (from `enum ibv_send_flags`): every work request is
// signaled, and small writes may carry their payload inline — the
// device copies it out of the caller's memory while the request is
// posted, so it needs no registration (its `lkey` is ignored).
const IBV_SEND_SIGNALED: c_uint = 1 << 1;
const IBV_SEND_INLINE: c_uint = 1 << 3;

/// Inline budgets a new queue pair asks for, most preferred first.
///
/// The verbs create path writes the budget the device actually
/// granted back into the queue pair attributes (`cap.max_inline_data`
/// is an in/out parameter: both libibverbs' `ibv_cmd_create_qp` and
/// the providers — e.g. mlx5's `mlx5_calc_sq_size` — update it on
/// return), so the first candidate the device accepts settles the
/// connection's [`Connection::inline_cap`] regardless of clamping. A
/// device that rejects an oversized request is stepped down; 0 is the
/// verbs default every device accepts, so a failure there is a real
/// error.
///
/// The top candidate is the mlx5 ceiling, the largest budget its
/// queue pairs are granted at all: `mlx5_calc_send_wqe` fits a send
/// work request into the device's 976-byte `max_sq_desc_sz` in
/// 64-byte blocks, so asking for 924 bytes builds a 960-byte work
/// request — the largest that fits — and grants 924 back (960 minus
/// the 36 bytes of control, remote-address, and inline segment
/// headers). Asking for 220 grants exactly 220; a top candidate
/// below the device ceiling reads back normally, so the shortfall
/// only shows as the sizes between the two taking the staging pool
/// instead of riding inline.
const INLINE_CANDIDATES: &[u32] = &[924, 220, 96, 0];

/// The remote access a window bind grants by default: read and
/// write — the access the memory served through windows needs, and
/// no more. Atomic access is deliberately not granted: nothing
/// served through a window uses it, and a grant is only as good as
/// what the port can serve — a RoCE port carries no atomic
/// operations, where a plain registration merely records the flags
/// and lets the ops fail at use time. A different grant is one bind
/// away: the access flags are the bind's, not the backing
/// registration's.
const WINDOW_ACCESS: c_int = IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;

// MR access flags (from `infiniband/verbs.h`): the remote reader may
// read and write, and future atomic operations are allowed.
const IBV_ACCESS_LOCAL_WRITE: c_int = 1;
const IBV_ACCESS_REMOTE_WRITE: c_int = 1 << 1;
const IBV_ACCESS_REMOTE_READ: c_int = 1 << 2;
const IBV_ACCESS_REMOTE_ATOMIC: c_int = 1 << 3;
// The permission a memory window's bind demands of its *target*: a
// bind over an MR registered without it is refused by the device —
// the bind's CQE fails with IBV_WC_MW_BIND_ERR ("memory management
// operation error"; on CX-3 Pro the vendor syndrome is 94) and the
// posting queue pair goes to ERROR. (`register_window_target`
// exists for this.)
const IBV_ACCESS_MW_BIND: c_int = 1 << 4;
// Completion statuses (from `enum ibv_wc_status`), besides success:
// the flush ends a failed queue pair's stream, the remote access
// violation is the memory-protection "no" — the failure a revoked
// (or otherwise invalid) access reads as.
const IBV_WC_SUCCESS: c_int = 0;
const IBV_WC_WR_FLUSH_ERR: c_int = 5;
const IBV_WC_REM_ACCESS_ERR: c_int = 10;
// The type of memory window to allocate: type 1 — the kind scoped
// to the protection domain rather than a queue pair number, so
// every queue pair of the domain can use the window's rkey and a
// replacement connection of the group keeps working. (Binding type 1
// windows is the `ibv_bind_mw` inline's path.)
const IBV_MW_TYPE_1: c_int = 1;

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

/// `ibv_post_send` and `ibv_poll_cq` are static inline calls in
/// `infiniband/verbs.h`, not library symbols: they dispatch through
/// the command table of the device handle.
type PostSendFn =
    unsafe extern "C" fn(*mut IbvQp, *mut IbvSendWr, *mut *mut IbvSendWr) -> c_int;
type PollCqFn = unsafe extern "C" fn(*mut IbvCq, c_int, *mut IbvWc) -> c_int;
type AllocMwFn = unsafe extern "C" fn(*mut IbvPd, c_int) -> *mut IbvMw;
type BindMwFn = unsafe extern "C" fn(*mut IbvQp, *mut IbvMw, *mut IbvMwBind) -> c_int;
type DeallocMwFn = unsafe extern "C" fn(*mut IbvMw) -> c_int;

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
    // Memory window support is provider-optional — the header's
    // inlines null-check these — so a device without it fails
    // loudly instead of binding nothing.
    alloc_mw: Option<AllocMwFn>,
    bind_mw: Option<BindMwFn>,
    dealloc_mw: Option<DeallocMwFn>,
    _compat_create_cq: *mut c_void,
    poll_cq: PollCqFn,
    req_notify_cq: *mut c_void,
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

/// A memory window (`struct ibv_mw`); the `rkey` field is both read
/// and written — a posted bind eagerly stores the *next* rkey in it,
/// and the failure contract is to restore the previous one.
#[repr(C)]
#[allow(dead_code)]
struct IbvMw {
    context: *mut IbvContext,
    pd: *mut IbvPd,
    rkey: u32,
    handle: u32,
    /// `enum ibv_mw_type`; written by allocation, never read from
    /// Rust (only type 1 is allocated).
    mw_type: c_int,
}

/// A bind's target (`struct ibv_mw_bind_info`): the registered memory
/// the window binds over, its range, and the access the binding
/// grants.
#[repr(C)]
#[derive(Clone, Copy)]
#[allow(dead_code)]
struct IbvMwBindInfo {
    mr: *mut IbvMr,
    addr: u64,
    length: u64,
    /// Access flags (`enum ibv_access_flags`), `unsigned int`.
    mw_access_flags: c_uint,
}

/// A bind request (`struct ibv_mw_bind`): the work request id the
/// bind's completion carries, the send flags, and the target.
#[repr(C)]
#[derive(Clone, Copy)]
#[allow(dead_code)]
struct IbvMwBind {
    wr_id: u64,
    /// `unsigned int`; from `enum ibv_send_flags`.
    send_flags: c_uint,
    bind_info: IbvMwBindInfo,
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
        channel: *mut c_void,
        comp_vector: c_int,
    ) -> *mut IbvCq;
    fn ibv_destroy_cq(cq: *mut IbvCq) -> c_int;
    fn ibv_reg_mr(pd: *mut IbvPd, addr: *mut c_void, length: usize, access: c_int) -> *mut IbvMr;
    fn ibv_dereg_mr(mr: *mut IbvMr) -> c_int;
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

/// The verdict of a failed completion, classified for consumers
/// that act on the failure mode: a revoked grant reads as a remote
/// access violation, and everything on a failed queue pair as a
/// flush.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionStatus {
    /// The remote's memory protection rejected the operation: its
    /// rkey was not honored. For an access whose grant was revoked
    /// this is the no-permission signal; the failed queue pair then
    /// completes its remaining work as [`Self::Flushed`].
    RemoteAccess,
    /// The operation flushed: its queue pair has failed and been
    /// abandoned, so nothing more can be posted through it.
    Flushed,
    /// Anything else the device can report.
    Other,
}

impl CompletionStatus {
    fn of(status: c_int) -> Self {
        match status {
            IBV_WC_REM_ACCESS_ERR => CompletionStatus::RemoteAccess,
            IBV_WC_WR_FLUSH_ERR => CompletionStatus::Flushed,
            _ => CompletionStatus::Other,
        }
    }
}

/// The error of one failed completion: the classified verdict, with
/// the device's own description for the log.
#[derive(Debug)]
pub struct CompletionError {
    /// The classified verdict.
    pub status: CompletionStatus,
    /// What failed — `"read"`, `"write"`, `"bind the window"`, ...
    pub op: String,
    /// The device's description: status name and vendor error.
    pub detail: String,
}

impl CompletionError {
    fn of_wc(op: &str, wc: &IbvWc) -> Self {
        CompletionError {
            status: CompletionStatus::of(wc.status),
            op: op.to_owned(),
            detail: format!(
                "{} (vendor error {})",
                status_str(wc.status),
                wc.vendor_err
            ),
        }
    }
}

impl fmt::Display for CompletionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RDMA {} failed: {}", self.op, self.detail)
    }
}

impl std::error::Error for CompletionError {}

/// The classified verdict inside `err`, when it is a completion
/// error.
fn completion_status(err: &io::Error) -> Option<CompletionStatus> {
    err.get_ref()
        .and_then(|cause| cause.downcast_ref::<CompletionError>())
        .map(|error| error.status)
}

/// Whether `err` is the remote's memory protection refusing an
/// operation — the failure a revoked (or otherwise invalid) access
/// reads as.
pub fn is_remote_access(err: &io::Error) -> bool {
    completion_status(err) == Some(CompletionStatus::RemoteAccess)
}

/// Whether `err` is a flush: the operation's queue pair has failed
/// and its outstanding work was abandoned — a replacement
/// connection is required for anything further.
pub fn is_flushed(err: &io::Error) -> bool {
    completion_status(err) == Some(CompletionStatus::Flushed)
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
/// Dropping the region deregisters it. Safety contract: the buffer
/// must outlive the region without moving or being resized. The
/// region keeps its protection domain alive (it holds a handle to
/// it), so it may outlive the connection it was registered through —
/// its keys then keep working through the domain's remaining
/// connections; registering again is required only once the
/// connection's domain is replaced.
pub struct MemoryRegion {
    mr: *mut IbvMr,
    /// The domain the memory is registered in; checked by the
    /// operations, and kept alive until this handle is dropped, so
    /// the deregistration below never races the domain's destruction.
    pd: ProtectionDomain,
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

/// A type 1 memory window: a revocable, re-grantable grant of remote
/// access to a range of a registered memory region, allocated in a
/// [`ProtectionDomain`].
///
/// Where a [`MemoryRegion`]'s rkey is granted when registered and
/// lives as long as the registration, a window *binds* over
/// registered memory: [`Connection::bind_mw`] grants remote access
/// to a range under a fresh rkey; [`Connection::unbind_mw`] revokes
/// the grant (binding the window to zero length — the rkey stops
/// being honored, including for requests in flight, which fail with
/// a protection violation); a later bind grants again under another
/// fresh rkey. Every bind rotates the rkey (its low 8 bits), so a
/// re-granted window never serves under the rkey a revoked binding
/// did.
///
/// A bind is a work request posted through a queue pair of the
/// window's protection domain, and takes effect at its completion —
/// the verbs contract forbids transferring the new rkey to a remote
/// before that completion, which is why this type's rkey is updated
/// only then. Once bound, the window's rkey is usable from every
/// queue pair of the domain, so a group whose connection is
/// replaced keeps its access.
pub struct MemoryWindow {
    mw: *mut IbvMw,
    /// The domain the window was allocated in, kept alive for the
    /// window's lifetime, as registrations keep theirs.
    pd: ProtectionDomain,
    /// The rkey of the latest *confirmed* binding. The window
    /// struct's own field changes at post time — before the bind is
    /// certain — and is restored there on a failed completion; this
    /// copy moves only at the completion.
    rkey: Cell<u32>,
}

// SAFETY: like a registration, the wrapped window has no thread
// affinity, so moving it to another thread is sound; access stays
// serialized by ownership (`Sync` deliberately not implemented).
unsafe impl Send for MemoryWindow {}

impl MemoryWindow {
    /// The rkey of the window's latest confirmed binding — what a
    /// remote needs to access the bound range. Until a first
    /// confirmed bind it is the allocated (unbound) key, honored by
    /// no device.
    pub fn rkey(&self) -> u32 {
        self.rkey.get()
    }
}

impl Drop for MemoryWindow {
    fn drop(&mut self) {
        // Best effort: deallocating a *bound* window is improper
        // (unbind it first), and unbinding needs a live queue pair
        // of the domain — unavailable at teardown. A failed dealloc
        // leaks the window object to the process's exit rather than
        // risk anything at drop time.
        let dealloc = unsafe { (*self.pd.context()).ops.dealloc_mw };
        if let Some(dealloc) = dealloc {
            unsafe {
                let _ = dealloc(self.mw);
            }
        }
    }
}

impl fmt::Debug for MemoryWindow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryWindow")
            .field("mw", &self.mw)
            .field("rkey", &self.rkey.get())
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
/// handle, connection, and registration are gone. Registrations
/// ([`MemoryRegion`]) hold handles of it, so they keep the domain
/// alive until they are dropped — their deregistration always
/// happens while the domain is still there, whatever the drop order
/// of connections and regions around them.
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
        self.register_access(
            addr,
            size,
            IBV_ACCESS_LOCAL_WRITE
                | IBV_ACCESS_REMOTE_WRITE
                | IBV_ACCESS_REMOTE_READ
                | IBV_ACCESS_REMOTE_ATOMIC,
        )
    }

    /// Register a memory range as a **window target**: as
    /// [`Self::register_addr`] does, and additionally carrying
    /// `IBV_ACCESS_MW_BIND` — the permission a
    /// [`Connection::bind_mw`] demands of its target.
    ///
    /// A bind over an ordinary registration is refused by the
    /// device: the bind's CQE fails with `IBV_WC_MW_BIND_ERR`
    /// ("memory management operation error") and the posting queue
    /// pair goes to ERROR, flushing everything after it. Shared
    /// regions served through windows (see
    /// `SharedMemoryRegionProvider::serve_through_windows`) register
    /// their backing this way.
    pub fn register_window_target(&self, addr: u64, size: usize) -> io::Result<MemoryRegion> {
        self.register_access(
            addr,
            size,
            IBV_ACCESS_LOCAL_WRITE
                | IBV_ACCESS_REMOTE_WRITE
                | IBV_ACCESS_REMOTE_READ
                | IBV_ACCESS_REMOTE_ATOMIC
                | IBV_ACCESS_MW_BIND,
        )
    }

    /// The registration both entry points share.
    fn register_access(&self, addr: u64, size: usize, access: c_int) -> io::Result<MemoryRegion> {
        if size == 0 {
            return Err(invalid("cannot register an empty region"));
        }
        let mr = check_ptr(
            "ibv_reg_mr",
            unsafe { ibv_reg_mr(self.inner.pd, addr as *mut c_void, size, access) },
        )?;
        let (lkey, rkey) = unsafe { ((*mr).lkey, (*mr).rkey) };
        Ok(MemoryRegion {
            mr,
            pd: self.clone(),
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

    /// Allocate a type 1 memory window in this protection domain.
    ///
    /// The window grants no access until it is bound over a
    /// registered range with [`Connection::bind_mw`] — through a
    /// queue pair of this domain — and its rkey changes with every
    /// binding (see [`MemoryWindow`]). A device without memory
    /// window support fails here, loudly, instead of handing out a
    /// window nothing can bind.
    pub fn alloc_mw(&self) -> io::Result<MemoryWindow> {
        let alloc = unsafe { (*self.context()).ops.alloc_mw };
        let alloc = alloc.ok_or_else(|| {
            io::Error::other("ibv_alloc_mw failed: the device supports no memory windows")
        })?;
        let mw = check_ptr("ibv_alloc_mw", unsafe { alloc(self.raw(), IBV_MW_TYPE_1) })?;
        let rkey = unsafe { (*mw).rkey };
        Ok(MemoryWindow {
            mw,
            pd: self.clone(),
            rkey: Cell::new(rkey),
        })
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
    /// Event channel owned by this connection; accepted connections
    /// share the listener's channel and leave this null.
    event_channel: *mut RdmaEventChannel,
    /// The inline budget the device granted the queue pair: writes of
    /// at most this many bytes can be posted with [`IBV_SEND_INLINE`],
    /// straight from the caller's memory.
    inline_cap: u32,
    /// Work request ids, to match completions in [`Connection::read`].
    next_wr_id: Cell<u64>,
}

// SAFETY: the wrapped connection has no thread affinity (its `Cell`
// is `Send`, its domain handle now is too), so moving it to another
// thread is sound; access stays serialized by ownership, and `Sync`
// is deliberately not implemented — concurrent reads of one
// connection are not part of the contract.
unsafe impl Send for Connection {}

impl Connection {
    /// Connect to the remote machine listening at `addr`.
    ///
    /// Resolves the address and route, creates the connection
    /// resources, and establishes the connection.
    pub fn connect<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        let mut dst = SockaddrIn::from(resolve_v4(addr)?);
        let event_channel =
            check_ptr("rdma_create_event_channel", unsafe { rdma_create_event_channel() })?;
        let mut conn = Self {
            id: std::ptr::null_mut(),
            cq: std::ptr::null_mut(),
            pd: None,
            event_channel,
            inline_cap: 0,
            next_wr_id: Cell::new(0),
        };
        conn.connect_to(&mut dst)?;
        Ok(conn)
    }

    /// Establish the connection; on failure the partially built `conn`
    /// is dropped by the caller, releasing the resources created so far.
    fn connect_to(&mut self, dst: &mut SockaddrIn) -> io::Result<()> {
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
        self.create_resources(pd)?;
        let mut param = conn_param();
        check("rdma_connect", unsafe { rdma_connect(self.id, &mut param) })?;
        wait_event(self.event_channel, RDMA_CM_EVENT_ESTABLISHED, "rdma_connect")
    }

    /// Create the completion queue and the queue pair on this
    /// connection's device, in `pd`.
    fn create_resources(&mut self, pd: ProtectionDomain) -> io::Result<()> {
        let context = unsafe { (*self.id).verbs };
        let cq = check_ptr(
            "ibv_create_cq",
            unsafe { ibv_create_cq(context, CQ_SIZE, std::ptr::null_mut(), std::ptr::null_mut(), 0) },
        )?;
        self.cq = cq;

        // A reliable connection signaling every send, like the C
        // library: each read completion is then always reported. The
        // inline budget is probed: the first candidate the device
        // accepts wins, and the budget the device granted is written
        // back into the attributes (see [`INLINE_CANDIDATES`]).
        let mut attr = IbvQpInitAttr {
            qp_context: std::ptr::null_mut(),
            send_cq: cq,
            recv_cq: cq,
            srq: std::ptr::null_mut(),
            cap: IbvQpCap {
                max_send_wr: CQ_SIZE as u32,
                max_recv_wr: CQ_SIZE as u32,
                max_send_sge: 1,
                max_recv_sge: 1,
                max_inline_data: 0,
            },
            qp_type: IBV_QPT_RC,
            sq_sig_all: 1,
        };
        for &candidate in INLINE_CANDIDATES {
            attr.cap.max_inline_data = candidate;
            let ret = unsafe { rdma_create_qp(self.id, pd.raw(), &mut attr) };
            if ret == 0 {
                self.inline_cap = attr.cap.max_inline_data;
                break;
            }
            if candidate == 0 {
                // 0 is the verbs default every device accepts: the
                // failure is a real create error, not a probe miss.
                check("rdma_create_qp", ret)?;
            }
        }
        self.pd = Some(pd);
        Ok(())
    }

    /// The connection's protection domain: memory registered in it is
    /// accessible through this connection and any other connection
    /// created in the same domain (see [`Listener::accept_into`]).
    pub fn protection_domain(&self) -> ProtectionDomain {
        self.pd.clone().expect("the connection is established")
    }

    /// Register `buffer` for RDMA access in this connection's
    /// protection domain.
    ///
    /// Returns the registered [`MemoryRegion`]: its [`MemoryRegion::tuple`]
    /// is what remote readers need to access the buffer remotely; its
    /// [`MemoryRegion::lkey`] is what local operations require. The
    /// buffer may be modified remotely at any time while registered,
    /// through every connection of the domain.
    ///
    /// The buffer must outlive the returned region without moving or
    /// being resized: drop the region first.
    pub fn register(&self, buffer: &[u8]) -> io::Result<MemoryRegion> {
        self.protection_domain().register(buffer)
    }

    /// Register the memory at `addr`..`addr + size` in this
    /// connection's protection domain.
    ///
    /// As [`ProtectionDomain::register_addr`], for buffers whose
    /// address and size are tracked separately from a Rust borrow:
    /// the memory must stay valid and unmoved while the region is
    /// alive, but no Rust reference to it needs to exist while it is
    /// being registered or read into.
    pub fn register_addr(&self, addr: u64, size: usize) -> io::Result<MemoryRegion> {
        self.protection_domain().register_addr(addr, size)
    }

    /// Read `len` bytes at `remote_addr` of the remote machine,
    /// accessed with `rkey`, into the first `len` bytes of `mr`, a
    /// region registered in this connection's protection domain.
    ///
    /// Synchronous: blocks until the read completes or
    /// [`POLL_TIMEOUT`] elapses. Out-of-bounds reads are detected by
    /// the remote machine and fail with a remote access error.
    pub fn read(&self, mr: &MemoryRegion, remote_addr: u64, rkey: u32, len: usize) -> io::Result<()> {
        self.one_sided("read", IBV_WR_RDMA_READ, mr, remote_addr, rkey, len)
    }

    /// Write the first `len` bytes of `mr`, a region registered in
    /// this connection's protection domain, to `remote_addr` of the
    /// remote machine, accessed with `rkey`.
    ///
    /// Synchronous: blocks until the write completes or
    /// [`POLL_TIMEOUT`] elapses. The remote memory must be registered
    /// for remote writes — every region the high-level providers
    /// share is — and out-of-bounds writes are detected by the remote
    /// machine and fail with a remote access error.
    pub fn write(&self, mr: &MemoryRegion, remote_addr: u64, rkey: u32, len: usize) -> io::Result<()> {
        self.one_sided("write", IBV_WR_RDMA_WRITE, mr, remote_addr, rkey, len)
    }

    /// The inline budget the device granted this connection's queue
    /// pair, in bytes.
    ///
    /// Writes of at most this many bytes can be posted with
    /// [`Self::write_inline`], straight out of the caller's memory.
    pub fn inline_cap(&self) -> usize {
        self.inline_cap as usize
    }

    /// Write `len` bytes of unregistered memory at `local_addr` to
    /// `remote_addr` of the remote machine, accessed with `rkey`.
    ///
    /// The payload is inlined into the work request: the device
    /// copies it out of the caller's memory while the request is
    /// being posted, so the memory needs no registration (the
    /// scatter entry's `lkey` is ignored when inline). `len` must not
    /// exceed the connection's inline budget ([`Self::inline_cap`]) —
    /// larger writes belong on [`Self::write`], with a registered
    /// buffer.
    ///
    /// Synchronous like [`Self::read`] and [`Self::write`]. The
    /// remote memory must be registered for remote writes — every
    /// region the high-level providers share is.
    pub fn write_inline(
        &self,
        local_addr: u64,
        remote_addr: u64,
        rkey: u32,
        len: usize,
    ) -> io::Result<()> {
        if len == 0 {
            return Err(invalid("cannot write zero bytes"));
        }
        if len > self.inline_cap as usize {
            return Err(invalid(format!(
                "inline write of {len} bytes exceeds the inline budget of {} bytes",
                self.inline_cap
            )));
        }
        let mut sge = IbvSge {
            addr: local_addr,
            length: len as u32,
            lkey: 0,
        };
        let mut send_wr = IbvSendWr {
            wr_id: self.next_wr_id.replace(self.next_wr_id.get() + 1),
            next: std::ptr::null_mut(),
            sg_list: &mut sge,
            num_sge: 1,
            opcode: IBV_WR_RDMA_WRITE,
            send_flags: IBV_SEND_SIGNALED | IBV_SEND_INLINE,
            imm_data: 0,
            wr: SendWrOp {
                rdma: IbvRdmaWr { remote_addr, rkey },
            },
        };
        self.post_and_wait("write", &mut send_wr)
    }

    /// Bind `window` over `mr`'s `addr`..`addr + len`, granting
    /// remote access to that range under a fresh rkey, and wait for
    /// the bind's completion.
    ///
    /// The window and `mr` must belong to this connection's
    /// protection domain, and `mr` must be a window-target
    /// registration ([`ProtectionDomain::register_window_target`]):
    /// a bind over an ordinary registration is refused by the device
    /// — its CQE fails with `IBV_WC_MW_BIND_ERR` and the queue pair
    /// goes to ERROR. The access granted is the module's default
    /// (see [`WINDOW_ACCESS`]). The grant takes effect at
    /// the bind's completion — requests already in flight under the
    /// previous binding fail with a protection violation from that
    /// point — so the returned rkey is safe to hand to a remote:
    /// the verbs contract forbids transferring it before the
    /// completion, and waiting for it is exactly what this does.
    /// The rkey rotates on every bind; the one a previous binding
    /// was served under stays dead.
    ///
    /// A bind through a queue pair that has failed does not take
    /// effect — it is flushed with the queue pair, reported as an
    /// error, and any previous binding stands.
    pub fn bind_mw(
        &self,
        window: &MemoryWindow,
        mr: &MemoryRegion,
        addr: u64,
        len: u64,
    ) -> io::Result<u32> {
        self.bind_mw_inner(window, mr, addr, len, WINDOW_ACCESS)
    }

    /// Revoke every remote access `window` grants: bind it to zero
    /// length and wait for the completion, from which no access
    /// under the previous binding is served — not even a request
    /// already in flight, which fails with a protection violation.
    ///
    /// The binding dies, not the window: a later
    /// [`Self::bind_mw`] grants again under a fresh rkey, usable
    /// from any queue pair of the domain. Revoking through a queue
    /// pair that has failed does not take effect (the bind is
    /// flushed with it) and is reported as an error — the previous
    /// binding stands until a bind through a live queue pair of the
    /// domain completes.
    pub fn unbind_mw(&self, window: &MemoryWindow, mr: &MemoryRegion) -> io::Result<()> {
        self.bind_mw_inner(window, mr, mr.addr(), 0, 0).map(|_| ())
    }

    /// The bind both window operations share: the same domain checks
    /// the verbs inline makes, then post through the device's bind
    /// entry point and wait for the bind's own completion.
    fn bind_mw_inner(
        &self,
        window: &MemoryWindow,
        mr: &MemoryRegion,
        addr: u64,
        len: u64,
        access: c_int,
    ) -> io::Result<u32> {
        let pd = self.pd.as_ref().expect("the connection is established");
        if !Arc::ptr_eq(&window.pd.inner, &pd.inner) {
            return Err(invalid(
                "the memory window was allocated in another protection domain",
            ));
        }
        if !Arc::ptr_eq(&mr.pd.inner, &pd.inner) {
            return Err(invalid(
                "the memory region is registered in another protection domain",
            ));
        }
        let bind = unsafe { (*(*self.id).verbs).ops.bind_mw };
        let bind =
            bind.ok_or_else(|| io::Error::other("ibv_bind_mw failed: the device supports no memory windows"))?;

        let wr_id = self.next_wr_id.replace(self.next_wr_id.get() + 1);
        let mut request = IbvMwBind {
            wr_id,
            send_flags: IBV_SEND_SIGNALED,
            bind_info: IbvMwBindInfo {
                mr: mr.mr,
                addr,
                length: len,
                mw_access_flags: access as c_uint,
            },
        };

        // A posted bind eagerly writes the *next* rkey into the
        // window; the binding takes effect only at the completion,
        // and the failure contract is to restore the previous key.
        // The served rkey (`window.rkey`) moves only on the
        // completion — the no-transfer-before-CQE rule.
        let previous = window.rkey.get();
        check(
            "ibv_bind_mw",
            unsafe { bind((*self.id).qp, window.mw, &mut request) },
        )?;
        match self.wait_completion("bind the window", wr_id) {
            Ok(()) => {
                let fresh = unsafe { (*window.mw).rkey };
                window.rkey.set(fresh);
                Ok(fresh)
            }
            Err(e) => {
                // The bind never took effect: the previous binding
                // stands, and the window's eager rkey goes back.
                unsafe { (*window.mw).rkey = previous };
                Err(e)
            }
        }
    }

    /// Post one one-sided RDMA operation and wait for its completion.
    ///
    /// `opcode` picks the operation ([`IBV_WR_RDMA_READ`] or
    /// [`IBV_WR_RDMA_WRITE`]); `op` names it in error messages. The
    /// first `len` bytes of `mr` are the operation's local end — the
    /// destination of a read, the source of a write — and
    /// `remote_addr`, accessed with `rkey`, its remote end; `mr` must
    /// be registered in this connection's protection domain.
    fn one_sided(
        &self,
        op: &str,
        opcode: c_int,
        mr: &MemoryRegion,
        remote_addr: u64,
        rkey: u32,
        len: usize,
    ) -> io::Result<()> {
        if len == 0 {
            return Err(invalid(format!("cannot {op} zero bytes")));
        }
        if len > u32::MAX as usize {
            return Err(invalid(format!(
                "{op} of {len} bytes exceeds the 4 GiB addressable by a single operation; split it"
            )));
        }
        if len > mr.size {
            return Err(invalid(format!(
                "{op} of {len} bytes exceeds the registered region of {} bytes",
                mr.size
            )));
        }
        let pd = self.pd.as_ref().expect("the connection is established");
        if !Arc::ptr_eq(&mr.pd.inner, &pd.inner) {
            return Err(invalid(
                "the memory region is registered in another protection domain",
            ));
        }

        let mut sge = IbvSge {
            addr: mr.addr,
            length: len as u32,
            lkey: mr.lkey,
        };
        let mut send_wr = IbvSendWr {
            wr_id: self.next_wr_id.replace(self.next_wr_id.get() + 1),
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
        self.post_and_wait(op, &mut send_wr)
    }

    /// Post one work request and wait for its completion.
    ///
    /// The request is identified by its work request id, which stale
    /// completions (e.g. left by an operation that timed out) do not
    /// match; those are skipped.
    fn post_and_wait(&self, op: &str, wr: &mut IbvSendWr) -> io::Result<()> {
        // ibv_post_send is a static inline call in verbs.h, dispatched
        // through the device's command table.
        let mut bad_wr: *mut IbvSendWr = std::ptr::null_mut();
        let qp = unsafe { (*self.id).qp };
        let post_send = unsafe { (*(*qp).context).ops.post_send };
        check("ibv_post_send", unsafe { post_send(qp, wr, &mut bad_wr) })?;
        self.wait_completion(op, wr.wr_id)
    }

    /// Wait for the completion of work request `wr_id`, skipping
    /// stale completions others left. The failure of a completed
    /// work request is classified (see [`CompletionStatus`]); every
    /// work request of this connection is signaled, so one always
    /// arrives — or the deadline does.
    fn wait_completion(&self, op: &str, wr_id: u64) -> io::Result<()> {
        // Spin on the completion queue: polling is user-space work on
        // every verbs device, which is what the latency wants.
        // ibv_poll_cq is a static inline call in verbs.h, dispatched
        // like ibv_post_send.
        let poll_cq = unsafe { (*(*self.cq).context).ops.poll_cq };
        let deadline = Instant::now() + POLL_TIMEOUT;
        let mut wc: IbvWc = unsafe { std::mem::zeroed() };
        let mut polls: u32 = 0;
        loop {
            let ret = unsafe { poll_cq(self.cq, 1, &mut wc) };
            if ret < 0 {
                check("ibv_poll_cq", -ret)?;
            }
            if ret == 1 && wc.wr_id == wr_id {
                if wc.status != IBV_WC_SUCCESS {
                    return Err(io::Error::other(CompletionError::of_wc(op, &wc)));
                }
                return Ok(());
            }
            // Reading the clock is a vDSO call next to the few
            // instructions of a poll, so the deadline is only checked
            // every so often; waiting for it never delays a
            // completion — the queue is polled on every iteration.
            polls = polls.wrapping_add(1);
            if polls & 127 == 0 && Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("timed out waiting for the RDMA {op} completion"),
                ));
            }
        }
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // Best effort: disconnected or never-connected ids return an
        // error, which is ignored here. The shared protection domain
        // (a struct field) drops after this, deallocating the domain
        // once its last connection and registration are gone.
        unsafe {
            if !self.id.is_null() {
                if !(*self.id).qp.is_null() {
                    rdma_disconnect(self.id);
                    rdma_destroy_qp(self.id);
                }
                rdma_destroy_id(self.id);
            }
            if !self.cq.is_null() {
                ibv_destroy_cq(self.cq);
            }
            if !self.event_channel.is_null() {
                rdma_destroy_event_channel(self.event_channel);
            }
        }
    }
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The raw identifier is enough to tell connections apart; the
        // inline budget says what small writes can do through it.
        f.debug_struct("Connection")
            .field("id", &self.id)
            .field("inline_cap", &self.inline_cap)
            .finish()
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
            // The child id shares this listener's event channel, which
            // this connection must not destroy.
            event_channel: std::ptr::null_mut(),
            inline_cap: 0,
            next_wr_id: Cell::new(0),
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
        conn.create_resources(pd)?;
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

    /// The access and completion-status constants the FFI relies on,
    /// pinned against `infiniband/verbs.h` — the values matter even
    /// where the layouts do not (`IBV_ACCESS_MW_BIND` is the
    /// permission a window bind demands of its target; the two
    /// statuses are what the classification keys on).
    #[test]
    fn verbs_constants() {
        assert_eq!(IBV_ACCESS_LOCAL_WRITE, 1);
        assert_eq!(IBV_ACCESS_REMOTE_WRITE, 1 << 1);
        assert_eq!(IBV_ACCESS_REMOTE_READ, 1 << 2);
        assert_eq!(IBV_ACCESS_REMOTE_ATOMIC, 1 << 3);
        assert_eq!(IBV_ACCESS_MW_BIND, 1 << 4);
        assert_eq!(IBV_WC_WR_FLUSH_ERR, 5);
        assert_eq!(IBV_WC_REM_ACCESS_ERR, 10);
    }

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
        // The memory window mirrors: `struct ibv_mw` (rkey before
        // handle and type), the bind info, and the bind request.
        assert_eq!(size_of::<IbvMw>(), 32);
        assert_eq!(offset_of!(IbvMw, rkey), 16);
        assert_eq!(offset_of!(IbvMw, handle), 20);
        assert_eq!(offset_of!(IbvMw, mw_type), 24);
        assert_eq!(size_of::<IbvMwBindInfo>(), 32);
        assert_eq!(offset_of!(IbvMwBindInfo, mr), 0);
        assert_eq!(offset_of!(IbvMwBindInfo, addr), 8);
        assert_eq!(offset_of!(IbvMwBindInfo, length), 16);
        assert_eq!(offset_of!(IbvMwBindInfo, mw_access_flags), 24);
        assert_eq!(size_of::<IbvMwBind>(), 48);
        assert_eq!(offset_of!(IbvMwBind, send_flags), 8);
        assert_eq!(offset_of!(IbvMwBind, bind_info), 16);
        assert_eq!(size_of::<IbvSendWr>(), 56);
        assert_eq!(offset_of!(IbvSendWr, send_flags), 32);
        assert_eq!(offset_of!(IbvSendWr, wr), 40);
        assert_eq!(size_of::<IbvWc>(), 48);
        assert_eq!(offset_of!(IbvWc, status), 8);
        assert_eq!(size_of::<IbvQpCap>(), 20);
        assert_eq!(size_of::<IbvQpInitAttr>(), 64);
        assert_eq!(size_of::<IbvQp>(), 8);
        assert_eq!(offset_of!(IbvQp, context), 0);
        assert_eq!(size_of::<IbvCq>(), 8);
        assert_eq!(offset_of!(IbvCq, context), 0);
        // The ops table: every entry is a pointer, with poll_cq at
        // index 11 and post_send at index 25.
        assert_eq!(size_of::<IbvContextOps>(), 208);
        assert_eq!(offset_of!(IbvContext, ops), 8);
        assert_eq!(offset_of!(IbvContextOps, poll_cq), 88);
        assert_eq!(offset_of!(IbvContextOps, post_send), 200);
        // The window entries the bind path dispatches through:
        // alloc_mw at index 7, bind_mw 8, dealloc_mw 9.
        assert_eq!(offset_of!(IbvContextOps, alloc_mw), 56);
        assert_eq!(offset_of!(IbvContextOps, bind_mw), 64);
        assert_eq!(offset_of!(IbvContextOps, dealloc_mw), 72);
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
