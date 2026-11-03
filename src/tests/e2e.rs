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

/// The staged and registered data paths: unregistered reads and
/// writes through the group connection's staging pool — including a
/// read larger than the pool, which travels in chunks — the
/// zero-copy registered-buffer path, and a path-by-size latency sweep
/// attributing per-operation overhead on this hardware: the staged
/// read with and without the allocation, the staged write (inline
/// within the granted budget, through the pool above it), and the
/// zero-copy registered read and write. Needs RDMA hardware — run
/// with `cargo test -- --ignored`.
#[test]
#[ignore = "needs an RDMA device"]
fn staged_and_registered_transfers() {
    // Ports of their own, so this runs back-to-back with the tests
    // above.
    const RDMA_PORT: u16 = 18517;
    const TCP_PORT: u16 = 9919;
    // Larger than the default 1 MiB staging pool: whole-region
    // operations travel in chunks.
    const BULK_ELEMS: usize = 640 * 1024;

    let owner = SharedMemoryRegionProvider::new(SharedMemoryRegionProviderAddr::new(
        RDMA_PORT,
        TCP_PORT,
    ));
    let bulk: Vec<u32> = (0..BULK_ELEMS as u32).collect();
    let bulk_handle = owner.register_typed("bulk", bulk.clone());
    let small_handle = owner.register_typed("small", vec![1u64, 2, 3]);
    // A byte-shared region for the latency sweep: shared as u8s, its
    // byte API exercises every data path without the element-layout
    // checks rejecting the u8 views of it.
    owner.register_typed("sweep", vec![0u8; 8192]);
    owner.serve().unwrap();

    let reader = RemoteMemoryProvider::new(RemoteMemoryProviderAddr::new(
        "localhost",
        RDMA_PORT,
        TCP_PORT,
    ));
    reader.update(0).unwrap();
    let catalog = reader.get_remote_mr_metadata();
    let by_name = |name: &str| {
        catalog
            .iter()
            .find(|region| region.name == name)
            .unwrap()
            .clone()
    };
    let bulk_region = reader.get_remote_mr(&by_name("bulk"), Some(0)).unwrap();
    let small_region = reader.get_remote_mr(&by_name("small"), Some(0)).unwrap();

    // Staged reads: a chunked whole-region one, then slices.
    let whole = bulk_region.read_typed::<u32>(0, BULK_ELEMS).unwrap();
    assert_eq!(whole, bulk);
    assert_eq!(
        bulk_region.read_typed::<u32>(100, 4).unwrap(),
        vec![100, 101, 102, 103]
    );
    assert_eq!(bulk_region.read(8, 4).unwrap(), 2u32.to_ne_bytes());

    // Staged writes: an inline-sized one and a chunked one.
    bulk_region.write_typed::<u32>(0, &[42, 43]).unwrap();
    {
        let owned = bulk_handle.borrow();
        assert_eq!(&owned[..2], &[42, 43]);
    }
    let pattern = vec![0xdead_beefu32; BULK_ELEMS];
    bulk_region.write_typed::<u32>(0, &pattern).unwrap();
    assert_eq!(bulk_handle.borrow()[BULK_ELEMS - 1], 0xdead_beef);

    // Registered buffers: zero-copy in both directions.
    let mut small_buf = reader.register_buffer_typed(0, vec![0u64; 3]).unwrap();
    small_region
        .read_into_registered(0, 3, &mut small_buf)
        .unwrap();
    assert_eq!(small_buf.as_slice(), &[1, 2, 3]);
    small_buf.as_mut_slice()[1] = 99;
    small_region
        .write_from_registered(0, 3, &small_buf)
        .unwrap();
    assert_eq!(&*small_handle.borrow(), &[1, 99, 3]);

    let mut bulk_buf = reader
        .register_buffer_typed(0, vec![0u32; BULK_ELEMS])
        .unwrap();
    bulk_region
        .read_into_registered(0, BULK_ELEMS, &mut bulk_buf)
        .unwrap();
    assert_eq!(bulk_buf.as_slice(), &pattern[..]);

    // A path-by-size latency sweep over the byte API of the
    // byte-shared region: which of the library's paths costs what on
    // this hardware. One process over loopback RDMA — pin it
    // (`numactl --cpunodebind=… --membind=…`, the HCA's node) for
    // stable absolutes; the ratios between the columns are the point
    // and hold either way. The `read` column pays the allocation its
    // API promises, the `write` column switches from the inline path
    // to the pool at the granted budget, and the registered columns
    // are the zero-copy floor.
    let sweep_region = reader.get_remote_mr(&by_name("sweep"), Some(0)).unwrap();
    let mut sweep_buf = reader.register_buffer(0, vec![0u8; 4096]).unwrap();
    let mut scratch = vec![0u8; 4096];

    fn timed(mut op: impl FnMut(), iterations: usize) -> f64 {
        // Warm the path out of the timing, then average the loop.
        op();
        let start = std::time::Instant::now();
        for _ in 0..iterations {
            op();
        }
        start.elapsed().as_nanos() as f64 / iterations as f64
    }

    const SWEEP_ITERATIONS: usize = 1000;
    let inline = reader.inline_cap(0).unwrap();
    println!("inline budget granted: {inline} bytes (ns/op, one process over loopback RDMA):");
    println!(
        "{:>6}  {:>9}  {:>9}  {:>9}  {:>9}  {:>9}  write path",
        "size", "read", "read_into", "write", "read_reg", "write_reg"
    );
    for size in [8usize, 64, 220, 256, 512, 924, 1024, 4096] {
        let payload = vec![0xA5u8; size];
        let read = timed(
            || {
                sweep_region.read(0, size).unwrap();
            },
            SWEEP_ITERATIONS,
        );
        let read_into = timed(
            || sweep_region.read_into(0, size, &mut scratch).unwrap(),
            SWEEP_ITERATIONS,
        );
        let write = timed(|| sweep_region.write(0, &payload).unwrap(), SWEEP_ITERATIONS);
        let read_reg = timed(
            || sweep_region.read_into_registered(0, size, &mut sweep_buf).unwrap(),
            SWEEP_ITERATIONS,
        );
        let write_reg = timed(
            || sweep_region.write_from_registered(0, size, &sweep_buf).unwrap(),
            SWEEP_ITERATIONS,
        );
        let path = if size <= inline { "inline" } else { "pool" };
        println!(
            "{size:>6}  {read:>9.0}  {read_into:>9.0}  {write:>9.0}  {read_reg:>9.0}  {write_reg:>9.0}  {path}"
        );
    }
}

/// A registered buffer survives the group's connection being replaced:
/// when the owner goes away, the reader's next update opens a fresh
/// session — a new RDMA connection, a new protection domain — and the
/// buffer re-registers into it on its next use. Needs RDMA hardware —
/// run with `cargo test -- --ignored`.
#[test]
#[ignore = "needs an RDMA device"]
fn registered_buffers_survive_a_reconnect() {
    const RDMA_PORT: u16 = 18518;
    const TCP_PORT: u16 = 9920;
    let addr = SharedMemoryRegionProviderAddr::new(RDMA_PORT, TCP_PORT);

    let owner = SharedMemoryRegionProvider::new(addr);
    owner.register_typed("values", vec![7u64, 8, 9]);
    owner.serve().unwrap();

    let reader = RemoteMemoryProvider::new(RemoteMemoryProviderAddr::new(
        "localhost",
        RDMA_PORT,
        TCP_PORT,
    ));
    reader.update(0).unwrap();
    let catalog = reader.get_remote_mr_metadata();
    let region = reader.get_remote_mr(&catalog[0], Some(0)).unwrap();
    let mut buffer = reader.register_buffer_typed(0, vec![0u64; 3]).unwrap();
    region.read_into_registered(0, 3, &mut buffer).unwrap();
    assert_eq!(buffer.as_slice(), &[7, 8, 9]);

    // The owner goes away, and comes back with different data. The
    // reader's open channel is dead: the next update fails and drops
    // it, and the one after opens a fresh session — with a new RDMA
    // connection and protection domain.
    drop(owner);
    let owner = SharedMemoryRegionProvider::new(addr);
    let handle = owner.register_typed("values", vec![70u64, 80, 90]);
    owner.serve().unwrap();
    assert!(reader.update(0).is_err());
    reader.update(0).unwrap();

    // The buffer, registered in the old connection's domain,
    // re-registers on use — and works through the new one.
    let catalog = reader.get_remote_mr_metadata();
    let region = reader.get_remote_mr(&catalog[0], Some(0)).unwrap();
    region.read_into_registered(0, 3, &mut buffer).unwrap();
    assert_eq!(buffer.as_slice(), &[70, 80, 90]);
    region.write_from_registered(0, 3, &buffer).unwrap();
    assert_eq!(&*handle.borrow(), &[70, 80, 90]);
}
