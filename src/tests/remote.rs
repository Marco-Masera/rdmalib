use std::cell::RefCell;
use std::io;
use std::rc::Rc;

use crate::readers::{CachedRegion, GroupConnection};
use crate::*;

fn metadata(name: &str, id: u32, size: usize) -> RemoteMemoryRegionMetadata {
    RemoteMemoryRegionMetadata {
        name: name.into(),
        id,
        size,
        elem_size: 1,
        elem_align: 1,
        elem_type: "u8".into(),
    }
}

/// A region of 4096 bytes at 0x1000, shared as elements of the given
/// layout, with no session behind it: its reads fail for want of the
/// group's RDMA connection, not for bad input.
fn region(elem_size: u32, elem_align: u32, elem_type: &str) -> RemoteMemoryRegion {
    RemoteMemoryRegion {
        connection: Rc::new(RefCell::new(GroupConnection::empty())),
        remote_addr: 0x1000,
        size: 4096,
        rkey: 1,
        group: 0,
        elem_size,
        elem_align,
        elem_type: elem_type.into(),
    }
}

#[test]
fn provider_starts_empty() {
    let provider = RemoteMemoryProvider::new(RemoteMemoryProviderAddr::default());
    assert!(provider.get_remote_mr_metadata().is_empty());
    assert!(provider.get_remote_mr(&metadata("missing", 0, 0), None).is_none());
    // Nothing connects until an update, and an update without a
    // reachable remote fails here.
    assert!(provider.update(0).is_err());
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
                elem_size: 1,
                elem_align: 1,
                elem_type: "u8".into(),
            },
            CachedRegion {
                remote_addr: 0x2000,
                size: 8192,
                rkey: 2,
                group: 3,
                elem_size: 8,
                elem_align: 8,
                elem_type: "u64".into(),
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
    assert_eq!((grouped.elem_size, grouped.elem_align), (8, 8));

    assert!(provider.get_remote_mr(&heap, Some(7)).is_none());
    // Same name, different id: a different region.
    assert!(provider.get_remote_mr(&metadata("heap", 8, 12288), None).is_none());
    assert_eq!(provider.get_remote_mr_metadata(), vec![heap]);
}

#[test]
fn read_bounds_are_checked_before_connecting() {
    let region = region(1, 1, "u8");
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
    let region = region(4, 4, "u32");

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

    // In bounds, aligned, and of the registered element layout — this
    // one fails later, for want of the group's connection: there is
    // no RDMA stack here and no session has run, but the error is no
    // longer an input error.
    assert_ne!(
        region.read_typed::<u32>(0, 4).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
}

#[test]
fn typed_mismatch_with_the_registered_layout_is_rejected() {
    // A region shared as u64s.
    let region = region(8, 8, "u64");

    // A u32 view of it: layout mismatch, rejected before any read is
    // attempted — with both type names in the error.
    let err = region.read_typed::<u32>(0, 2).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    let msg = err.to_string();
    assert!(msg.contains("u64"), "{msg}");
    assert!(msg.contains("u32"), "{msg}");

    let mut buf = [0u32; 2];
    assert_eq!(
        region.read_into_typed(0, &mut buf).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    // The matching type still passes the layout check.
    assert_ne!(
        region.read_typed::<u64>(0, 1).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
}
