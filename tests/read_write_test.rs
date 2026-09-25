/*
 * Read Write Test
 *  - One writer and one reader-writer setting
 *  - The reader accesses the remote memory as an array of Structs of type Test
 *  - Switch roles: The reader then modifies the memory and send the write over rdma
 *  - The original writer becomes the reader and checks changes in its local memory
 */

mod common;

#[repr(C)]
#[derive(Clone, Copy)]
struct Test {
    v: usize,
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
        let handle =
            owner.register_typed("tests", vec![Test { v: 1 }, Test { v: 2 }, Test { v: 3 }]);
        let _ = owner.serve().unwrap();
        // The provider is listening: let the readers connect.
        synch.synch().unwrap();
        // Hold the provider (and its registered regions) alive until
        // the readers are through: they enter this barrier afterwards.
        synch.synch().unwrap();

        //Read my own buffer
        let mut buf = handle.borrow();
        assert_eq!(buf[1].v, 1000);
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
        let tests = region.read_typed::<Test>(0, 3).unwrap(); // [Test { v: 1 }, Test { v: 2 }, Test { v: 3 }]
        assert_eq!(tests.iter().map(|t| t.v).collect::<Vec<_>>(), vec![1, 2, 3]);
        println!("Done!");
        // Done reading

        // Now set one element remotely
        let modify_buf = Test { v: 1000 };
        let _ = region.write_typed(1, &vec![modify_buf]);
        synch.synch().unwrap();
    }
}
