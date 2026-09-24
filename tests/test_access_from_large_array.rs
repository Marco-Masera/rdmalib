// Cluster integration test: the runner (scripts/test_runner.py) deploys
// this binary to the nodes configured for `read_test` in
// tests/_test_config.json and tells each process the cluster layout via
// environment variables (parsed in tests/common/mod.rs).

mod common;

#[repr(C)]
#[derive(Clone, Copy)]
struct Test {
    x: usize,
    y: usize,
    z: f64,
}

#[test]
#[ignore = "cluster test: ./podman_build/link_test.sh --test read_test -- --ignored"]
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

    // Simple read/write test
    rdmalib::impl_remote_safe!(Test);
    // Barrier across the deployed nodes: without it the readers race
    // node0's serve() (nothing binds before it) and their TCP connect
    // is refused. Node0 also has to outlive the readers' session, so
    // it only tears the provider down after their second barrier.
    let mut synch = common::GlobalSynch::new(&cluster);
    if cluster.my_node == 0 {
        // Writer
        let owner =
            rdmalib::SharedMemoryRegionProvider::new(rdmalib::SharedMemoryRegionProviderAddr::new(
                cluster.me().rdma_port,
                cluster.me().tcp_port,
            )); // RDMA, TCP ports

        let buffer: Vec<Test> = (0..1000)
            .map(|i| Test {
                x: i,
                y: i * 2,
                z: i as f64,
            })
            .collect();

        let handle = owner.register_typed("tests", buffer);
        let result = owner.serve().unwrap();
        // The provider is listening: let the readers connect.
        synch.synch().unwrap();
        // Hold the provider (and its registered regions) alive until
        // the readers are through: they enter this barrier afterwards.
        synch.synch().unwrap();

        //Modify the buffer
        let mut buf = handle.borrow_mut();
        buf[0].x = 1000;
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
        // Node0 is serving now.
        synch.synch().unwrap();
        reader.update(0).unwrap(); // session: greet, join group 0, RDMA rendezvous, tuples
        let catalog = reader.get_remote_mr_metadata();
        let region = reader.get_remote_mr(&catalog[0], Some(0)).unwrap();

        let smaller_buffer = region.read_typed::<Test>(100, 150).unwrap();
        assert_eq!(smaller_buffer.len(), 150);
        for (i, entry) in smaller_buffer.iter().enumerate() {
            assert_eq!(entry.x, 100 + i);
            assert_eq!(entry.y, (100 + i) * 2)
        }
        println!("Done!");
        // Done reading: let node0 tear the provider down.
        synch.synch().unwrap();
        synch.synch().unwrap();
        let tests2 = region.read_typed::<Test>(0, 1).unwrap();
        assert_eq!(tests2[0].x, 1000);
        println!("Done 2!");
        synch.synch().unwrap();
    }
}
