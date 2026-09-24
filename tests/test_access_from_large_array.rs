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
    // First, read from the environment to get info on ports and addresses
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

    // Register our Application-level data structure (Test) into the library
    rdmalib::impl_remote_safe!(Test);

    // Barrier across the deployed nodes
    let mut synch = common::GlobalSynch::new(&cluster);

    if cluster.my_node == 0 {
        // ==============================================
        // Writer node

        // Create the shared memory Provider. It's the manager object
        let owner =
            rdmalib::SharedMemoryRegionProvider::new(rdmalib::SharedMemoryRegionProviderAddr::new(
                cluster.me().rdma_port,
                cluster.me().tcp_port,
            )); // RDMA, TCP ports

        // instantiate a buffer of 1000 elements of type Test
        let buffer: Vec<Test> = (0..1000)
            .map(|i| Test {
                x: i,
                y: i * 2,
                z: i as f64,
            })
            .collect();

        // Assign this memory region to the memory Provider. Now it owns it.
        let handle = owner.register_typed("tests", buffer);
        let result = owner.serve().unwrap();

        // Wait for the reader
        synch.synch().unwrap();
        // Hold the provider (and its registered regions) alive until
        // the readers are through: they enter this barrier afterwards.
        synch.synch().unwrap();

        //Modify the buffer, then reader will read it again
        // Since the Manager owns the original vector, has to ask for a borrow
        let mut buffer = handle.borrow_mut();
        // Can access the borrowed buffer in read and write - it's just local memory
        buffer[0].x = 1000;
        //Wait for the reader
        synch.synch().unwrap();
        synch.synch().unwrap();
    } else {
        // ==============================================
        // Reader node

        // Establish a connection to the writer
        let writer_info = cluster.node(0);
        let reader = rdmalib::RemoteMemoryProvider::new(rdmalib::RemoteMemoryProviderAddr::new(
            writer_info.ip.clone(),
            writer_info.rdma_port,
            writer_info.tcp_port,
        ));

        //wait for writer to be ready
        synch.synch().unwrap();

        // This gets metadata from the remote writer
        reader.update(0).unwrap();
        // Catalog: set of regions shared by the writer - only one in this example
        let catalog = reader.get_remote_mr_metadata();
        // Get the region associated to the area. Some(0) is a self-assigned group for permissions
        let region = reader.get_remote_mr(&catalog[0], Some(0)).unwrap();

        // Read only 150 elements out of the 1000, from 100 to 149
        let smaller_buffer = region.read_typed::<Test>(100, 150).unwrap();
        // check correctness
        assert_eq!(smaller_buffer.len(), 150);
        for (i, entry) in smaller_buffer.iter().enumerate() {
            assert_eq!(entry.x, 100 + i);
            assert_eq!(entry.y, (100 + i) * 2)
        }
        println!("Done!");

        // Done reading: let node0 modify the data in his local memory
        synch.synch().unwrap();
        synch.synch().unwrap();

        // Read again, this time position 0, which has been modified by writer
        let tests2 = region.read_typed::<Test>(0, 1).unwrap();
        assert_eq!(tests2[0].x, 1000);
        println!("Done 2!");
        synch.synch().unwrap();
    }
}
