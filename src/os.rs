//! The operating-system interface the library needs, hand-declared
//! like the RDMA FFI: the project takes no dependencies, so the few
//! glibc facilities beyond `std` live here directly, each verified
//! against the glibc headers and man pages the way the verbs
//! declarations in [`crate::rdma`] are verified against upstream
//! rdma-core.
//!
//! CPU affinity: [`pin_current_thread`] pins the calling thread to one
//! CPU and [`current_thread_cpus`] reports the CPUs available to it —
//! the plumbing the async operations engine (see `docs/async_engine.md`)
//! uses to pin its polling loop, and the provider's service thread
//! uses at `serve` time. Affinity is a per-thread attribute; pinning
//! the calling thread (`pid 0`) never affects the process's other
//! threads.
//!
//! The wake channel: [`pipe_pair`], [`wake`], [`drain`], and
//! [`poll_fds`] are the engine's idle half — a submitter tells a
//! blocked engine there is work with one written byte, and the engine
//! waits for it (and, later, for completion-channel events) with
//! `poll(2)`.

use std::ffi::{c_int, c_short, c_void};
use std::io;
use std::os::fd::{FromRawFd, OwnedFd, RawFd};

/// glibc's `cpu_set_t` is a fixed 128-byte bit mask (`CPU_SETSIZE`
/// 1024): a `[u64; 16]` mirrors it, and CPU ids beyond 1023 cannot be
/// addressed by it. Systems whose kernel affinity mask exceeds 1024
/// CPUs would need a dynamically sized mask (`CPU_ALLOC`) — far beyond
/// any machine this library targets.
const CPU_SET_BYTES: usize = 128;
/// The highest CPU id the fixed-size affinity mask can address.
const CPU_SETSIZE: u32 = 1024;
/// The affinity mask itself: 1024 bits as 16 `unsigned long`s.
type CpuSet = [u64; CPU_SET_BYTES / 8];

// Real glibc symbols (verified against sched.h and the
// sched_setaffinity(2), poll(2), pipe(7), read(2)/write(2) man
// pages): affinity is a per-thread attribute, `pid` 0 addresses the
// calling thread, poll's `nfds_t` is an unsigned long and its
// timeout is milliseconds (negative: infinite), and — unlike the
// libibverbs inline wrappers, which return the errno directly —
// these functions report failure with `-1` and leave the reason in
// errno.
unsafe extern "C" {
    fn sched_setaffinity(pid: c_int, cpusetsize: usize, mask: *const CpuSet) -> c_int;
    fn sched_getaffinity(pid: c_int, cpusetsize: usize, mask: *mut CpuSet) -> c_int;
    fn pipe(fds: *mut c_int) -> c_int;
    fn read(fd: c_int, buf: *mut c_void, count: usize) -> isize;
    fn write(fd: c_int, buf: *const c_void, count: usize) -> isize;
    fn poll(fds: *mut PollFd, nfds: u64, timeout: c_int) -> c_int;
}

/// Pin the calling thread to `cpu`.
///
/// Fails without an OS call if `cpu` cannot be addressed by the
/// fixed-size mask, and with the OS error otherwise (the CPU does not
/// exist, or is not permitted to this process, e.g. by a cpuset
/// cgroup). A failed call leaves the thread's affinity unchanged.
pub(crate) fn pin_current_thread(cpu: u32) -> io::Result<()> {
    if cpu >= CPU_SETSIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "cpu id {cpu} exceeds the affinity mask's limit of {}",
                CPU_SETSIZE - 1
            ),
        ));
    }
    let mut set: CpuSet = [0; CPU_SET_BYTES / 8];
    set[(cpu / 64) as usize] |= 1u64 << (cpu % 64);
    check("sched_setaffinity", unsafe {
        sched_setaffinity(0, CPU_SET_BYTES, &set)
    })
}

/// The CPUs the calling thread may run on, ascending by id.
///
/// Reads the thread's affinity mask — the CPUs permitted to this
/// process, e.g. by a cpuset cgroup — which is what a thread spawned
/// by this one inherits, so a CPU in the list is one a spawned thread
/// can pin to.
pub(crate) fn current_thread_cpus() -> io::Result<Vec<u32>> {
    let mut set: CpuSet = [0; CPU_SET_BYTES / 8];
    check("sched_getaffinity", unsafe {
        sched_getaffinity(0, CPU_SET_BYTES, &mut set)
    })?;
    Ok((0..CPU_SETSIZE)
        .filter(|&cpu| set[(cpu / 64) as usize] & (1u64 << (cpu % 64)) != 0)
        .collect())
}

/// Report whether a thread spawned by the calling thread can pin to
/// `cpu`, before anything is spawned onto it.
///
/// Checks the calling thread's mask — the mask a spawned thread
/// inherits — so this is a side-effect-free pre-flight for
/// [`pin_current_thread`] in a freshly spawned thread, failing with
/// `InvalidInput` for a CPU the process may not use.
pub(crate) fn ensure_cpu_available(cpu: u32) -> io::Result<()> {
    if cpu >= CPU_SETSIZE {
        return Err(invalid(format!(
            "cpu id {cpu} exceeds the affinity mask's limit of {}",
            CPU_SETSIZE - 1
        )));
    }
    let cpus = current_thread_cpus()?;
    if cpus.contains(&cpu) {
        return Ok(());
    }
    Err(invalid(format!(
        "cpu {cpu} is not available to this process (its affinity mask allows {cpus:?})"
    )))
}

/// Turn a glibc-style failure (`-1`, reason in errno) into an
/// `io::Error` — unlike the verbs inline wrappers, which return the
/// errno directly and are checked accordingly in `rdma/verbs.rs`.
fn check(call: &str, ret: c_int) -> io::Result<()> {
    if ret == 0 {
        Ok(())
    } else {
        Err(io::Error::other(format!(
            "{call} failed: {}",
            io::Error::last_os_error()
        )))
    }
}

fn invalid(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, msg.into())
}

/// `POLLIN` — the only poll event the engine waits for.
pub(crate) const POLLIN: c_short = 0x001;

/// glibc's `struct pollfd`, mirrored: the descriptor watched, the
/// events to watch for, and the events the kernel reported (filled
/// by [`poll_fds`]).
#[repr(C)]
#[derive(Clone, Copy)]
pub(crate) struct PollFd {
    pub(crate) fd: c_int,
    pub(crate) events: c_short,
    pub(crate) revents: c_short,
}

/// A pipe's two ends, (read, write): the engine's wake channel. The
/// write end is kept where commands are pushed; the read end is the
/// engine thread's, blocked on with [`poll_fds`] when idle and
/// drained with [`drain`] after every wake.
pub(crate) fn pipe_pair() -> io::Result<(OwnedFd, OwnedFd)> {
    let mut fds = [0 as c_int; 2];
    check("pipe", unsafe { pipe(fds.as_mut_ptr()) })?;
    // SAFETY: a successful pipe call filled both descriptors, and
    // they are new — owned by nothing else.
    Ok(unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) })
}

/// Write one wake byte to a wake channel's write end.
///
/// Best effort by design: a full pipe means the reader already has
/// wake bytes it has not drained — the wake is pending. An `EINTR`
/// retries: a wake lost to a signal would be a missed submission.
pub(crate) fn wake(write_fd: RawFd) {
    let byte = 1u8;
    loop {
        let written = unsafe { write(write_fd, &byte as *const u8 as *const c_void, 1) };
        if written == 1 {
            return;
        }
        if io::Error::last_os_error().kind() != io::ErrorKind::Interrupted {
            return;
        }
    }
}

/// Read and discard the wake bytes of a wake channel's read end known
/// readable (poll said `POLLIN` first — the read never blocks),
/// returning how many were drained. A short or failed read returns
/// what it got (0 on failure).
pub(crate) fn drain(read_fd: RawFd) -> usize {
    let mut bytes = [0u8; 64];
    let read = unsafe { read(read_fd, bytes.as_mut_ptr() as *mut c_void, bytes.len()) };
    read.max(0) as usize
}

/// poll(2): wait until one of `fds` reports an event, `timeout_ms`
/// milliseconds at most (negative: forever; zero: not at all).
///
/// Returns how many descriptors have events in their `revents`. An
/// `EINTR` retries — a readiness lost to a signal would be a lost
/// wake.
pub(crate) fn poll_fds(fds: &mut [PollFd], timeout_ms: c_int) -> io::Result<usize> {
    loop {
        let ret = unsafe { poll(fds.as_mut_ptr(), fds.len() as u64, timeout_ms) };
        if ret >= 0 {
            return Ok(ret as usize);
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(io::Error::other(format!("poll failed: {error}")));
        }
    }
}
