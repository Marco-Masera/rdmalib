// Cluster integration test: the runner (scripts/test_runner.py) deploys
// this binary to the nodes configured for `byte_read_test` in
// tests/_test_config.json and tells each process the cluster layout via
// environment variables (parsed in tests/common/mod.rs).
//
// The byte-flavored sibling of read_test.rs: node0 shares a raw byte
// buffer (no element type), the readers read it back — whole, and in
// slices at byte offsets — and node0 rewrites part of it for a second
// round of reads.

mod common;

#[test]
#[ignore = "cluster test: ./podman_build/link_test.sh --test byte_read_test -- --ignored"]
fn test_public_api() {
    let cluster = common::TestCluster::from_env();

    println!(
        "I am node {} of {}: {}",
        cluster.my_node,
        cluster.len(),
        cluster.me()
    );
    for peer in cluster.others() {
        println!("peer: {peer}");
    }
    assert!(!cluster.is_empty());
    assert!(cluster.my_node < cluster.len());

    let _provider_here = cluster.me().provider_addr();
    let _reader_of_first = cluster.node(0).remote_addr();

    // Simple raw byte read/write test: what node0 writes as bytes, the
    // readers read as bytes.
    // Barrier across the deployed nodes: without it the readers race
    // node0's serve() (nothing binds before it) and their TCP connect
    // is refused. Node0 also has to outlive the readers' session, so
    // it only tears the provider down after their final barrier.
    let mut synch = common::GlobalSynch::new(&cluster);
    if cluster.my_node == 0 {
        // Writer
        let owner =
            rdmalib::SharedMemoryRegionProvider::new(rdmalib::SharedMemoryRegionProviderAddr::new(
                cluster.me().rdma_port,
                cluster.me().tcp_port,
            )); // RDMA, TCP ports
        let payload: Vec<u8> = (0..64u8).collect();
        let handle = owner.register(rdmalib::SharedMemoryRegionMetadata::new("raw", payload));
        owner.serve().unwrap();
        // The provider is listening: let the readers connect.
        synch.synch().unwrap();
        // Hold the provider (and its registered regions) alive until
        // the readers are through: they enter this barrier afterwards.
        synch.synch().unwrap();

        //Modify the buffer
        let mut buf = handle.borrow_mut();
        buf[0..8].copy_from_slice(&[0xAA; 8]);
        synch.synch().unwrap();
        synch.synch().unwrap();
    } else {
        // Reader
        let writer = cluster.node(0);
        let reader = rdmalib::RemoteMemoryProvider::new(rdmalib::RemoteMemoryProviderAddr::new(
            writer.ip.clone(),
            writer.rdma_port,
            writer.tcp_port,
        ));
        let expected: Vec<u8> = (0..64u8).collect();
        // Node0 is serving now.
        synch.synch().unwrap();
        reader.update(0).unwrap(); // session: greet, join group 0, RDMA rendezvous, tuples
        let catalog = reader.get_remote_mr_metadata();
        let region = reader.get_remote_mr(&catalog[0], Some(0)).unwrap();
        // The whole region...
        let whole = region.read(0, expected.len()).unwrap();
        assert_eq!(whole, expected);
        // ...and a slice of it at a byte offset, into a buffer we own.
        let mut slice = vec![0u8; 8];
        region.read_into(16, 8, &mut slice).unwrap();
        assert_eq!(slice, expected[16..24]);
        println!("Done!");
        // Done reading: let node0 tear the provider down.
        synch.synch().unwrap();
        synch.synch().unwrap();
        // Node0 has rewritten the first 8 bytes; the rest is untouched.
        let prefix = region.read(0, 8).unwrap();
        assert_eq!(prefix, vec![0xAA; 8]);
        let rest = region.read(8, 56).unwrap();
        assert_eq!(rest, expected[8..]);
        println!("Done 2!");
        synch.synch().unwrap();
    }
}
