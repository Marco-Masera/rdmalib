use std::io;
use std::rc::Rc;

use crate::readers::CachedRegion;
use crate::*;

fn metadata(name: &str, id: u32, size: usize) -> RemoteMemoryRegionMetadata {
    RemoteMemoryRegionMetadata {
        name: name.into(),
        id,
        size,
    }
}

#[test]
fn provider_starts_empty() {
    let provider = RemoteMemoryProvider::new(RemoteMemoryProviderAddr::default());
    assert!(provider.get_remote_mr_metadata().is_empty());
    assert!(provider.get_remote_mr(&metadata("missing", 0, 0), None).is_none());
    provider.update();
}

#[test]
fn lookup_by_metadata_and_group() {
    let provider = RemoteMemoryProvider::new(RemoteMemoryProviderAddr::default());
    let heap = metadata("heap", 7, 12288);
    provider.metadata.borrow_mut().push(heap.clone());
    provider.regions.borrow_mut().insert(
        (heap.name.clone(), heap.id),
        vec![
            CachedRegion {
                remote_addr: 0x1000,
                size: 4096,
                rkey: 1,
                group: 0,
            },
            CachedRegion {
                remote_addr: 0x2000,
                size: 8192,
                rkey: 2,
                group: 3,
            },
        ],
    );

    let any = provider.get_remote_mr(&heap, None).unwrap();
    assert_eq!(any.group, 0);

    let grouped = provider.get_remote_mr(&heap, Some(3)).unwrap();
    assert_eq!(
        (grouped.remote_addr, grouped.size, grouped.rkey, grouped.group),
        (0x2000, 8192, 2, 3)
    );

    assert!(provider.get_remote_mr(&heap, Some(7)).is_none());
    // Same name, different id: a different region.
    assert!(provider.get_remote_mr(&metadata("heap", 8, 12288), None).is_none());
    assert_eq!(provider.get_remote_mr_metadata(), vec![heap]);
}

#[test]
fn read_bounds_are_checked_before_connecting() {
    let provider = RemoteMemoryProvider::new(RemoteMemoryProviderAddr::new("127.0.0.1", 1, 2));
    let region = RemoteMemoryRegion {
        connection: Rc::clone(&provider.connection),
        remote_addr: 0x1000,
        size: 4096,
        rkey: 1,
        group: 0,
    };
    let mut buf = [0u8; 8];

    // Beyond the region's end.
    assert!(region.read_into(4090, 8, &mut buf).is_err());
    // Larger than the destination buffer.
    assert!(region.read_into(0, 16, &mut buf).is_err());
    // The allocating read checks the region bounds too. All of these
    // fail before any connection is attempted.
    assert!(region.read(4090, 8).is_err());
}

#[test]
fn typed_read_bounds_and_alignment_are_checked_before_connecting() {
    let provider = RemoteMemoryProvider::new(RemoteMemoryProviderAddr::new("127.0.0.1", 1, 2));
    let region = RemoteMemoryRegion {
        connection: Rc::clone(&provider.connection),
        remote_addr: 0x1000,
        size: 4096,
        rkey: 1,
        group: 0,
    };

    // 8 bytes of u32s at offset 4090 exceed the 4096-byte region.
    assert_eq!(
        region.read_typed::<u32>(4090, 2).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    // Offset 2 is misaligned for u32 elements at a 0x1000 region.
    assert_eq!(
        region.read_typed::<u32>(2, 1).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    // A whole-buffer typed read of 2048 u32s exceeds the region.
    let mut buf = [0u32; 2048];
    assert_eq!(
        region.read_into_typed(0, &mut buf).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );

    // In bounds and aligned, the checks pass — this one fails later,
    // connecting: there is no RDMA stack here and nothing listening
    // there, but the error is no longer an input error.
    assert_ne!(
        region.read_typed::<u32>(0, 4).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
}
