use clap::Parser;
use rdmalib_benchmarks::{
    DEFAULT_CLIENT_CPU, DEFAULT_ENGINE_CPU, OpKind, Placement, run_concurrent_typed,
};

#[derive(Parser, Debug)]
#[command(
    name = "concurrent_write_typed",
    about = "Concurrent Typed Writes (1 entry of Word8 per op, wait all together)"
)]
struct Args {
    /// Number of concurrent operations (e.g. 16 or 32)
    #[arg(short = 'c', long, default_value_t = 16)]
    concurrency: usize,

    /// Number of measured iterations
    #[arg(short = 'n', long, default_value_t = 10_000)]
    iters: usize,

    /// Number of warm-up iterations
    #[arg(short = 'w', long, default_value_t = 100)]
    warmup: usize,

    /// Optional path to store the list of N measured latencies in milliseconds (JSON)
    #[arg(short = 'o', long)]
    output: Option<String>,

    /// CPU to pin the engine thread to (default: the first benchmark cluster's placement — see docs/async_engine.md)
    #[arg(long = "engine-cpu", default_value_t = DEFAULT_ENGINE_CPU)]
    engine_cpu: u32,

    /// CPU to pin the client (submitting and waiting) thread to (default: the engine's L2 mate there)
    #[arg(long = "client-cpu", default_value_t = DEFAULT_CLIENT_CPU)]
    client_cpu: u32,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    if args.concurrency == 0 {
        eprintln!("Error: --concurrency must be at least 1");
        std::process::exit(1);
    }
    run_concurrent_typed(
        OpKind::Write,
        args.concurrency,
        args.iters,
        args.warmup,
        args.output,
        Placement {
            engine_cpu: args.engine_cpu,
            client_cpu: args.client_cpu,
        },
    )
}
