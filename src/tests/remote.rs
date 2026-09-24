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

    // 2 u32s at element offset 1023 (bytes 4092..4100) exceed the
    // 4096-byte region.
    assert_eq!(
        region.read_typed::<u32>(1023, 2).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    // A misaligned region base (0x1002 here) is rejected for u32
    // elements: element offsets themselves are always aligned.
    let misaligned = RemoteMemoryRegion {
        connection: Rc::new(RefCell::new(GroupConnection::empty())),
        remote_addr: 0x1002,
        size: 4096,
        rkey: 1,
        group: 0,
        elem_size: 4,
        elem_align: 4,
        elem_type: "u32".into(),
    };
    assert_eq!(
        misaligned.read_typed::<u32>(0, 1).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    // A whole-buffer typed read of 2048 u32s exceeds the region.
    let mut buf = [0u32; 2048];
    assert_eq!(
        region.read_into_typed(0, &mut buf).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    // An element offset past the region's end, even with nothing to
    // read. (An empty read at the end offset, 1024, stays in bounds —
    // like `vec[1024..1024]` — and only fails later, for want of the
    // connection.)
    assert_eq!(
        region.read_typed::<u32>(1025, 0).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );

    // In bounds and of the registered element layout — this one fails
    // later, for want of the group's connection: there is no RDMA
    // stack here and no session has run, but the error is no longer
    // an input error. Element offset 1 = bytes 4..8.
    assert_ne!(
        region.read_typed::<u32>(1, 1).unwrap_err().kind(),
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

#[test]
fn write_bounds_are_checked_before_connecting() {
    let region = region(1, 1, "u8");

    // Beyond the region's end.
    assert_eq!(
        region.write(4090, &[0u8; 8]).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    // In bounds, but no session has run: the failure is for want of
    // the group's connection, not bad input.
    assert_ne!(
        region.write(0, &[0u8; 16]).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    // An empty write at the end offset stays in bounds — like
    // `vec[4096..4096]` — and fails later, for want of the connection.
    assert_ne!(
        region.write(4096, &[]).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
}

#[test]
fn typed_write_bounds_and_alignment_are_checked_before_connecting() {
    let region = region(4, 4, "u32");

    // 2 u32s at element offset 1023 (bytes 4092..4100) exceed the
    // 4096-byte region.
    assert_eq!(
        region.write_typed::<u32>(1023, &[0u32; 2]).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    // A misaligned region base (0x1002 here) is rejected for u32
    // elements: element offsets themselves are always aligned.
    let misaligned = RemoteMemoryRegion {
        connection: Rc::new(RefCell::new(GroupConnection::empty())),
        remote_addr: 0x1002,
        size: 4096,
        rkey: 1,
        group: 0,
        elem_size: 4,
        elem_align: 4,
        elem_type: "u32".into(),
    };
    assert_eq!(
        misaligned.write_typed::<u32>(0, &[0u32; 1]).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    // A whole-buffer typed write of 2048 u32s exceeds the region.
    assert_eq!(
        region.write_typed::<u32>(0, &[0u32; 2048]).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    // An element offset past the region's end, even with nothing to
    // write. (An empty write at the end offset, 1024, stays in bounds
    // — like `vec[1024..1024]` — and fails later, for want of the
    // connection.)
    assert_eq!(
        region.write_typed::<u32>(1025, &[]).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert_ne!(
        region.write_typed::<u32>(1024, &[]).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );

    // In bounds and of the registered element layout — this one fails
    // later, for want of the group's connection: there is no RDMA
    // stack here and no session has run, but the error is no longer
    // an input error. Element offset 1 = bytes 4..8.
    assert_ne!(
        region.write_typed::<u32>(1, &[0u32; 1]).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
}

#[test]
fn typed_write_mismatch_with_the_registered_layout_is_rejected() {
    // A region shared as u64s.
    let region = region(8, 8, "u64");

    // A u32 view of it: layout mismatch, rejected before any write is
    // attempted — with both type names in the error.
    let err = region.write_typed::<u32>(0, &[0u32; 2]).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    let msg = err.to_string();
    assert!(msg.contains("u64"), "{msg}");
    assert!(msg.contains("u32"), "{msg}");
    // The matching type passes the layout check and fails later, for
    // want of the connection.
    assert_ne!(
        region.write_typed::<u64>(0, &[0u64; 1]).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
}
