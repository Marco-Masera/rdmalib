use crate::*;

fn provider() -> SharedMemoryRegionProvider {
    SharedMemoryRegionProvider::new(SharedMemoryRegionProviderAddr::new(18515, 9125))
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
struct Reading {
    timestamp: u64,
    value: f32,
    flags: u32,
}

crate::impl_remote_safe!(Reading);

#[test]
fn register_assigns_sequential_ids() {
    let provider = provider();
    let a = provider.register(SharedMemoryRegionMetadata::new("heap", vec![0; 4]));
    let b = provider.register_typed("counters", vec![0u32; 4]);
    // Same name, distinct ids — the id sequence is shared by the byte
    // and the typed entry points.
    assert_eq!((a.id(), b.id()), (0, 1));
}

#[test]
fn tuples_need_an_accepted_group() {
    let provider = provider();
    let handle = provider.register(SharedMemoryRegionMetadata::new("heap", vec![0u8; 4096]));

    // No reader has been accepted into group 0, so no group exists
    // yet: no tuple is served. (Creating real tuples requires RDMA
    // hardware, exercised on the cluster machines: the tuple of a
    // region and group is the registration of the region's buffer in
    // the group's protection domain, shared by all of the group's
    // readers.)
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

#[test]
fn typed_regions_are_shared_without_copying() {
    let provider = provider();
    let readings = vec![
        Reading {
            timestamp: 1,
            value: 1.5,
            flags: 0,
        },
        Reading {
            timestamp: 2,
            value: 2.5,
            flags: 8,
        },
    ];
    let expected = readings.clone();
    let before = readings.as_ptr();

    let handle = provider.register_typed("readings", readings);

    // The Vec was moved, not copied: the same heap allocation.
    assert_eq!(handle.borrow().as_ptr(), before);
    assert_eq!(&*handle.borrow(), expected.as_slice());

    handle.borrow_mut()[1].value = 9.0;
    assert_eq!(handle.borrow()[1].value, 9.0);
}

#[test]
fn typed_regions_serve_tuples_like_byte_regions() {
    let provider = provider();
    let handle = provider.register_typed::<u32>("counters", vec![7, 7, 7, 7]);

    // Tuples are built from the captured byte address and size, so a
    // 16-byte Vec of four u32s is served exactly like a byte region.
    // (No reader has been accepted into group 0, so no tuple exists.)
    assert!(provider.get_shared_mr(handle.id(), 0).unwrap().is_none());
}

#[test]
#[should_panic(expected = "zero-sized")]
fn zero_sized_elements_are_rejected() {
    #[derive(Clone, Copy)]
    struct Empty;
    crate::impl_remote_safe!(Empty);

    let provider = provider();
    let _ = provider.register_typed("empty", vec![Empty; 3]);
}

#[test]
fn the_served_catalog_is_sorted_by_id() {
    use crate::providers::{ordered_catalog, RegionInfo};
    use std::collections::HashMap;

    // A catalog as scrambled as a `HashMap` can leave it: ids out of
    // order, non-contiguous. The serving order is the id order —
    // registration order — whatever the map's iteration order
    // happens to be (it is randomized per process, which is the bug
    // this pins: a served order taken from the iteration order flips
    // per run).
    let info = |name: &str| RegionInfo {
        name: name.to_owned(),
        remote_addr: 0,
        size: 0,
        elem_size: 1,
        elem_align: 1,
        elem_type: "u8".to_owned(),
    };
    let mut catalog = HashMap::new();
    catalog.insert(2, info("c"));
    catalog.insert(0, info("a"));
    catalog.insert(5, info("f"));
    catalog.insert(1, info("b"));
    let served: Vec<u32> = ordered_catalog(&catalog).iter().map(|&(id, _)| id).collect();
    assert_eq!(served, [0, 1, 2, 5]);
}

#[test]
fn the_exchange_serves_the_catalog_in_registration_order() {
    use crate::meta::Message;

    let provider = provider();
    // Registered out of name order, so the id order is not the name
    // order: what a session is served is the registration order.
    // (No reader has been accepted into group 0, so no group exists,
    // so no tuple is created — the exchange runs without hardware.)
    let b = provider.register(SharedMemoryRegionMetadata::new("b", vec![0; 8]));
    let a = provider.register(SharedMemoryRegionMetadata::new("a", vec![0; 8]));

    let (catalog, tuples) = provider.served_exchange(0);
    let Message::Metadata { regions } = catalog else {
        panic!("the exchange serves the catalog first");
    };
    let Message::Tuples { group, tuples } = tuples else {
        panic!("the exchange serves the tuples second");
    };
    assert_eq!(group, 0);
    assert!(tuples.is_empty());
    assert_eq!(
        regions
            .iter()
            .map(|region| (region.id, region.name.as_str()))
            .collect::<Vec<_>>(),
        [(b.id(), "b"), (a.id(), "a")]
    );
}
