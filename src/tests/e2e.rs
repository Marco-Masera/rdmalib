//! The full flow over the real stack: sharing a region, exchanging
//! its metadata over TCP, and reading it back over RDMA. These need
//! RDMA hardware — run them on a cluster node with
//! `cargo test -- --ignored`.

use crate::*;

/// Ports of the sharing side; adjust to the node if taken.
const RDMA_PORT: u16 = 18515;
const TCP_PORT: u16 = 9917;

#[test]
#[ignore = "needs an RDMA device"]
fn reader_reads_what_the_owner_shares() {
    // Owner: share a region and start the metadata service.
    let owner = SharedMemoryRegionProvider::new(SharedMemoryRegionProviderAddr::new(
        RDMA_PORT,
        TCP_PORT,
    ));
    let handle = owner.register_typed("readings", vec![42u64, 43, 44]);
    owner.serve().unwrap();

    // Reader: join group 0 and read the region back, element by
    // element over RDMA.
    let reader = RemoteMemoryProvider::new(RemoteMemoryProviderAddr::new(
        "localhost",
        RDMA_PORT,
        TCP_PORT,
    ));
    reader.update(0).unwrap();
    let catalog = reader.get_remote_mr_metadata();
    assert_eq!(catalog.len(), 1);
    assert_eq!((catalog[0].name.as_str(), catalog[0].id), ("readings", handle.id()));
    assert_eq!((catalog[0].elem_size, catalog[0].elem_align, catalog[0].elem_type.as_str()), (8, 8, "u64"));

    let region = reader.get_remote_mr(&catalog[0], Some(0)).unwrap();
    assert_eq!(region.read_typed::<u64>(0, 3).unwrap(), vec![42, 43, 44]);
    // The byte view of the same region: the first element's bytes.
    assert_eq!(region.read(0, 8).unwrap(), 42u64.to_ne_bytes());
    // A typed read of a mismatched layout is rejected before the
    // device is touched.
    assert_eq!(
        region.read_typed::<u32>(0, 2).unwrap_err().kind(),
        std::io::ErrorKind::InvalidInput
    );

    // A region registered after the session started is picked up by
    // the next update, over the open channel.
    let late = owner.register_typed("counters", vec![7u32, 8]);
    reader.update(0).unwrap();
    let catalog = reader.get_remote_mr_metadata();
    assert_eq!(catalog.len(), 2);
    let late_meta = catalog
        .iter()
        .find(|metadata| metadata.id == late.id())
        .unwrap();
    let late_region = reader.get_remote_mr(late_meta, Some(0)).unwrap();
    assert_eq!(late_region.read_typed::<u32>(0, 2).unwrap(), vec![7, 8]);

    // The owner keeps writing; the reader sees it.
    handle.borrow_mut()[1] = 99;
    assert_eq!(region.read_typed::<u64>(1, 1).unwrap(), vec![99]);
}

/// The write path of the same flow: the reader writes into the shared
/// region over RDMA, and both sides see the bytes. Needs RDMA
/// hardware — run with `cargo test -- --ignored`.
#[test]
#[ignore = "needs an RDMA device"]
fn reader_writes_what_the_owner_shares() {
    // Owner: share a region and start the metadata service. Ports of
    // its own, so this runs back-to-back with the read test above.
    const RDMA_PORT: u16 = 18516;
    const TCP_PORT: u16 = 9918;
    let owner = SharedMemoryRegionProvider::new(SharedMemoryRegionProviderAddr::new(
        RDMA_PORT,
        TCP_PORT,
    ));
    let handle = owner.register_typed("commands", vec![0u64, 0, 0, 0]);
    owner.serve().unwrap();

    // Reader: join group 0 and write into the region, element-wise
    // over RDMA.
    let reader = RemoteMemoryProvider::new(RemoteMemoryProviderAddr::new(
        "localhost",
        RDMA_PORT,
        TCP_PORT,
    ));
    reader.update(0).unwrap();
    let catalog = reader.get_remote_mr_metadata();
    let region = reader.get_remote_mr(&catalog[0], Some(0)).unwrap();

    region.write_typed::<u64>(1, &[7, 8]).unwrap();
    // The owner sees the written elements...
    assert_eq!(&*handle.borrow(), &[0, 7, 8, 0]);
    // ...and so does the reader, reading them back.
    assert_eq!(region.read_typed::<u64>(1, 2).unwrap(), vec![7, 8]);

    // The byte view: a raw write of the first element's bytes.
    region.write(0, &13u64.to_ne_bytes()).unwrap();
    assert_eq!(&*handle.borrow(), &[13, 7, 8, 0]);
    assert_eq!(region.read(0, 8).unwrap(), 13u64.to_ne_bytes());

    // Bounds and layout are checked before the device is touched.
    assert_eq!(
        region.write_typed::<u64>(3, &[0u64; 2]).unwrap_err().kind(),
        std::io::ErrorKind::InvalidInput
    );
    assert_eq!(
        region.write_typed::<u32>(0, &[0u32; 2]).unwrap_err().kind(),
        std::io::ErrorKind::InvalidInput
    );
    assert_eq!(
        region.write(31, &[0u8; 8]).unwrap_err().kind(),
        std::io::ErrorKind::InvalidInput
    );
}
