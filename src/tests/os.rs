//! CPU affinity and the wake channel, the `src/os.rs` plumbing behind
//! [`SharedMemoryRegionProvider::with_cpu`] and the async operations
//! engine (see `docs/async_engine.md`).

use std::io;
use std::mem::{offset_of, size_of};
use std::os::fd::AsRawFd;
use std::thread;
use std::time::Duration;

use crate::os;
use crate::*;

#[test]
fn pinning_restricts_the_calling_thread() {
    // The test harness runs each test on its own thread, so pinning
    // here cannot disturb any other test.
    let cpus = os::current_thread_cpus().unwrap();
    let Some(&cpu) = cpus.first() else {
        // An empty affinity mask cannot happen, but if the OS ever
        // reports one there is nothing to pin to.
        return;
    };

    os::pin_current_thread(cpu).unwrap();

    assert_eq!(os::current_thread_cpus().unwrap(), vec![cpu]);
}

#[test]
fn unavailable_cpus_fail() {
    // A CPU the process may not use (outside its affinity mask, e.g.
    // by a cpuset cgroup) fails to pin; unless this process somehow
    // may use every addressable CPU.
    let available = os::current_thread_cpus().unwrap();
    let Some(blocked) = (0..1024u32).find(|cpu| !available.contains(cpu)) else {
        return;
    };

    assert!(os::pin_current_thread(blocked).is_err());
    // The failed call left the calling thread's mask untouched.
    assert_eq!(os::current_thread_cpus().unwrap(), available);
}

#[test]
fn cpu_ids_beyond_the_mask_fail_without_an_os_call() {
    // glibc's cpu_set_t addresses 1024 CPUs: id 1024 (and beyond) is
    // rejected by the library, before any FFI.
    for bad in [1024, u32::MAX] {
        let err = os::pin_current_thread(bad).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(err.to_string().contains("affinity mask"));
    }

    // The pre-flight helper rejects the same ids the same way.
    assert_eq!(
        os::ensure_cpu_available(1024).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
}

#[test]
fn ensure_cpu_available_reports_permitted_cpus() {
    let available = os::current_thread_cpus().unwrap();
    let Some(&cpu) = available.first() else {
        return;
    };

    os::ensure_cpu_available(cpu).unwrap();
    if let Some(blocked) = (0..1024u32).find(|cpu| !available.contains(cpu)) {
        assert!(os::ensure_cpu_available(blocked).is_err());
    }
}

#[test]
fn provider_validates_the_service_cpu_before_serving() {
    // A provider built for a CPU the process may not use fails at
    // serve, before binding any port — no RDMA stack needed to check
    // that.
    let available = os::current_thread_cpus().unwrap();
    let Some(blocked) = (0..1024u32).find(|cpu| !available.contains(cpu)) else {
        return;
    };

    let provider = SharedMemoryRegionProvider::with_cpu(
        SharedMemoryRegionProviderAddr::new(18515, 9125),
        blocked,
    );
    let err = provider.serve().unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    assert!(err.to_string().contains("affinity mask"));
}

#[test]
fn pollfd_layout_is_pinned() {
    // glibc's struct pollfd: an int and two shorts, on LP64 an 8-byte
    // struct with the events at 4 and the revents at 6.
    assert_eq!(size_of::<os::PollFd>(), 8);
    assert_eq!(offset_of!(os::PollFd, fd), 0);
    assert_eq!(offset_of!(os::PollFd, events), 4);
    assert_eq!(offset_of!(os::PollFd, revents), 6);
    assert_eq!(os::POLLIN, 0x001);
}

#[test]
fn wake_channels_carry_and_drain_wakes() {
    let (read, write) = os::pipe_pair().unwrap();
    os::wake(write.as_raw_fd());
    os::wake(write.as_raw_fd());
    os::wake(write.as_raw_fd());

    let mut fds = [os::PollFd {
        fd: read.as_raw_fd(),
        events: os::POLLIN,
        revents: 0,
    }];
    // The writes made the read end readable; a bounded poll agrees.
    assert!(os::poll_fds(&mut fds, 100).unwrap() >= 1);
    assert!(fds[0].revents & os::POLLIN != 0);
    // All three wakes drain at once.
    assert_eq!(os::drain(read.as_raw_fd()), 3);
    // Drained, a zero-timeout poll reports nothing ready.
    assert_eq!(os::poll_fds(&mut fds, 0).unwrap(), 0);
}

#[test]
fn poll_blocks_until_woken() {
    // An infinite poll returns only when another thread wakes the
    // channel — a broken one (returning early) cannot pass this
    // bounded-time test, and a lost wake hangs the runner's timeout.
    let (read, write) = os::pipe_pair().unwrap();
    let waker = thread::spawn(move || {
        thread::sleep(Duration::from_millis(20));
        os::wake(write.as_raw_fd());
    });

    let mut fds = [os::PollFd {
        fd: read.as_raw_fd(),
        events: os::POLLIN,
        revents: 0,
    }];
    assert!(os::poll_fds(&mut fds, -1).unwrap() >= 1);
    assert!(fds[0].revents & os::POLLIN != 0);
    waker.join().unwrap();
}
