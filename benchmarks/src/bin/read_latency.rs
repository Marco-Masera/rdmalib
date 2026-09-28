use std::time::Instant;
use clap::Parser;
use rdmalib::{RemoteMemoryProvider, SharedMemoryRegionProvider};
use rdmalib_benchmarks::{ClusterContext, GlobalSynch, LatencyHistogram};

#[derive(Parser, Debug)]
#[command(name = "read_latency", about = "Benchmark one-sided RDMA read round-trip latency")]
struct Args {
    /// Number of measured read operations
    #[arg(short, long, default_value_t = 100_000)]
    iters: usize,

    /// Payload size in bytes per read
    #[arg(short, long, default_value_t = 64)]
    size: usize,

    /// Number of warm-up operations before measurement starts
    #[arg(short, long, default_value_t = 1_000)]
    warmup: usize,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let cluster = ClusterContext::from_env();
    let mut synch = GlobalSynch::new(&cluster);

    const REGION_NAME: &str = "bench_region";
    // Allocate buffer large enough for the requested payload size (at least 4KB).
    let buffer_size = args.size.max(4096);

    if cluster.is_server() {
        println!("[node {}] starting server (provider)", cluster.my_node);
        let owner = SharedMemoryRegionProvider::new(cluster.me().provider_addr());
        let _handle = owner.register_typed(REGION_NAME, vec![0xABu8; buffer_size]);
        owner.serve()?;

        println!("[node {}] server listening, entering sync barrier 1 (ready)...", cluster.my_node);
        synch.synch()?;

        println!("[node {}] server waiting for benchmark completion (sync barrier 2)...", cluster.my_node);
        synch.synch()?;

        println!("[node {}] benchmark completed, server shutting down.", cluster.my_node);
    } else {
        println!("[node {}] client waiting for server to be ready...", cluster.my_node);
        synch.synch()?;

        let server_node = cluster.node(0);
        let reader = RemoteMemoryProvider::new(server_node.remote_addr());

        println!("[node {}] connecting to server group 0...", cluster.my_node);
        reader.update(0)?;

        let catalog = reader.get_remote_mr_metadata();
        let region_meta = catalog
            .iter()
            .find(|desc| desc.name == REGION_NAME)
            .ok_or("shared region not found in catalog")?;
        let region = reader
            .get_remote_mr(region_meta, Some(0))
            .ok_or("failed to get remote MR for region")?;

        if args.warmup > 0 {
            println!("[node {}] running {} warm-up iterations...", cluster.my_node, args.warmup);
            for _ in 0..args.warmup {
                let _ = region.read_async(0, args.size)?.wait()?;
            }
        }

        println!(
            "[node {}] running {} measured read iterations (payload: {} B)...",
            cluster.my_node, args.iters, args.size
        );
        let mut hist = LatencyHistogram::new();
        let start_all = Instant::now();

        for _ in 0..args.iters {
            let t0 = Instant::now();
            let _ = region.read_async(0, args.size)?.wait()?;
            hist.record(t0.elapsed());
        }

        let total_time = start_all.elapsed();
        hist.print_summary("RDMA Read Latency", total_time, args.size);

        println!("[node {}] notifying server that client finished...", cluster.my_node);
        synch.synch()?;
    }

    Ok(())
}
