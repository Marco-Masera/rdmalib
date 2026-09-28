/*
    Tests the Async capabilities of the library.
    The writer instantiates two buffers.
    Step 1:
        The reader issues N read requests toward them, and wait for all together.
    Step 2:
        The reader mixes read and write requests.
*/

use std::io;

use crate::common::GlobalSynch;
use crate::common::TestCluster;

#[repr(C)]
#[derive(Clone, Copy)]
struct Test {
    original_buf: usize,
    v: usize,
}

mod common;

const BUF_SIZE: usize = 30;

async fn reader_test(cluster: TestCluster, mut synch: GlobalSynch) {
    let engine = rdmalib::Engine::on_cpu(0).unwrap();
    let writer_info = cluster.node(0);
    let reader = rdmalib::RemoteMemoryProvider::with_engine(
        rdmalib::RemoteMemoryProviderAddr::new(
            writer_info.ip.clone(),
            writer_info.rdma_port,
            writer_info.tcp_port,
        ),
        engine.clone(),
    );

    println!("Step 1 - Running Reader function");

    // Wait for the writer to be ready
    synch.synch().unwrap();

    reader.update(0).unwrap();
    let catalog = reader.get_remote_mr_metadata();
    let region1 = reader.get_remote_mr(&catalog[0], Some(0)).unwrap();
    let region2 = reader.get_remote_mr(&catalog[1], Some(0)).unwrap();

    let ops1 = (0..BUF_SIZE)
        .map(|i| region1.read_typed_async::<Test>(i as u64, 1))
        .collect::<io::Result<Vec<rdmalib::RegionOp<Test>>>>()
        .unwrap();
    let ops2 = (0..BUF_SIZE)
        .map(|i| region2.read_typed_async::<Test>(i as u64, 1))
        .collect::<io::Result<Vec<rdmalib::RegionOp<Test>>>>()
        .unwrap();
    let mut ops = ops1;
    ops.extend(ops2);

    // All of both regions' reads, awaited as one batch.
    let buffers = futures::future::join_all(ops).await;

    // The extraction: every op resolves with its own filled buffer —
    // the one element it read. The writer stamped each element with
    // its value and the buffer it lives in, so both are checked:
    // region1's reads carry `original_buf: 0`, region2's `1`.
    for (i, result) in buffers.iter().enumerate() {
        let value = result.as_ref().unwrap()[0];
        let (region, element) = if i < BUF_SIZE {
            (0, i)
        } else {
            (1, i - BUF_SIZE)
        };
        assert_eq!(value.original_buf, region);
        assert_eq!(value.v, element);
    }

    synch.synch().unwrap();
    println!("Step 1 - Reader function finished");

    println!("Step 2 - Write test");
    //Write in region 1
    let ops = (0..BUF_SIZE)
        .map(|i| {
            region1.write_typed_async::<Test>(
                i as u64,
                vec![Test {
                    v: i,
                    original_buf: 2,
                }],
            )
        })
        .collect::<io::Result<Vec<rdmalib::RegionOp<Test>>>>()
        .unwrap();
    let buffers = futures::future::join_all(ops).await;
    println!("Step 2 - Write test finished");
    synch.synch().unwrap();
}

#[test]
#[ignore = "cluster test: ./podman_build/link_test.sh --test read_asyc_multiple -- --ignored"]
fn test_public_api() {
    let cluster = common::TestCluster::from_env();
    assert!(!cluster.is_empty());
    assert!(cluster.my_node < cluster.len());

    let _provider_here = cluster.me().provider_addr();
    let _reader_of_first = cluster.node(0).remote_addr();

    rdmalib::impl_remote_safe!(Test);

    let mut synch = common::GlobalSynch::new(&cluster);
    if cluster.my_node == 0 {
        // Writer
        let owner =
            rdmalib::SharedMemoryRegionProvider::new(rdmalib::SharedMemoryRegionProviderAddr::new(
                cluster.me().rdma_port,
                cluster.me().tcp_port,
            )); // RDMA, TCP ports
        let buffer1: Vec<Test> = (0..BUF_SIZE)
            .map(|i| Test {
                v: i,
                original_buf: 0,
            })
            .collect();
        let buffer2: Vec<Test> = (0..BUF_SIZE)
            .map(|i| Test {
                v: i,
                original_buf: 1,
            })
            .collect();
        let handle1 = owner.register_typed("buf1", buffer1);
        let handle2 = owner.register_typed("buf2", buffer2);
        let result = owner.serve().unwrap();

        // Wait for the reader to be ready
        synch.synch().unwrap();
        // Wait for the reader to finish step 1
        synch.synch().unwrap();
        synch.synch().unwrap();

        println!("Owner: test changes");
        let borrowed = handle1.borrow();
        for entry in borrowed.iter() {
            assert_eq!(2, entry.original_buf);
        }
    } else {
        futures::executor::block_on(reader_test(cluster, synch))
    }
}
