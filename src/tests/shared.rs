use crate::*;

fn provider() -> SharedMemoryRegionProvider {
    SharedMemoryRegionProvider::new(SharedMemoryRegionProviderAddr::new(18515, 9125))
}

#[test]
fn register_assigns_sequential_ids() {
    let provider = provider();
    let a = provider.register(SharedMemoryRegionMetadata::new("heap", vec![0; 4]));
    let b = provider.register(SharedMemoryRegionMetadata::new("heap", vec![0; 4]));
    // Same name, distinct ids.
    assert_eq!((a.id(), b.id()), (0, 1));
}

#[test]
fn tuples_need_an_accepted_connection() {
    let provider = provider();
    let handle = provider.register(SharedMemoryRegionMetadata::new("heap", vec![0u8; 4096]));

    // No reader has been accepted, so no group exists: no tuple is
    // served. (Creating real tuples requires RDMA hardware, exercised
    // on the cluster machines: the tuple of a region and group is the
    // registration of the region's buffer on the group's connection.)
    assert!(provider.get_shared_mr(handle.id(), 0).unwrap().is_none());
    assert!(provider.get_shared_mr(99, 0).unwrap().is_none());
}

#[test]
fn buffer_read_write_via_handle() {
    let provider = provider();
    let handle = provider.register(SharedMemoryRegionMetadata::new("heap", vec![1, 2, 3, 4]));

    assert_eq!(*handle.borrow(), [1, 2, 3, 4]);
    handle.borrow_mut()[0] = 9;
    assert_eq!(*handle.borrow(), [9, 2, 3, 4]);
}

#[test]
fn provider_usable_while_buffer_borrowed() {
    let provider = provider();
    let a = provider.register(SharedMemoryRegionMetadata::new("a", vec![0; 8]));

    let mut buf = a.borrow_mut();
    buf[0] = 1;
    // Tuples are served without touching the buffers — their address
    // and size were captured at registration — so the provider keeps
    // working while the application holds a mutable borrow: it
    // registers regions and serves tuple lookups. (No reader has been
    // accepted, so no tuple is available yet.)
    let b = provider.register(SharedMemoryRegionMetadata::new("b", vec![0; 8]));
    assert!(provider.get_shared_mr(b.id(), 0).unwrap().is_none());
    assert_eq!(*b.borrow(), [0; 8]);
    drop(buf);

    assert_eq!(a.borrow()[0], 1);
}
