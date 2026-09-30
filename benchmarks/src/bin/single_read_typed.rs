use clap::Parser;
use rdmalib_benchmarks::{
    DEFAULT_CLIENT_CPU, DEFAULT_ENGINE_CPU, OpKind, Placement, WaitMode, run_single_typed,
};

#[derive(Parser, Debug)]
#[command(
    name = "single_read_typed",
    about = "Benchmark 3: Single Read, multiple words, typed (8-word struct)"
)]
struct Args {
    /// Number of 8-word entries per read (e.g. 1, 4, 16, 32)
    #[arg(short = 'e', long, default_value_t = 1)]
    entries: usize,

    /// Number of measured iterations
    #[arg(short = 'n', long, default_value_t = 500)]
    iters: usize,

    /// Number of warm-up iterations
    #[arg(short = 'w', long, default_value_t = 100)]
    warmup: usize,

    /// Optional path to store the list of N measured latencies in milliseconds (JSON)
    #[arg(short = 'o', long)]
    output: Option<String>,

    /// How the client waits for each operation: parked (blocking) or spinning (busy-poll floor)
    #[arg(long = "wait", value_enum, default_value = "park")]
    wait_mode: WaitMode,

    /// CPU to pin the engine thread to (default: the first benchmark cluster's placement — see docs/async_engine.md)
    #[arg(long = "engine-cpu", default_value_t = DEFAULT_ENGINE_CPU)]
    engine_cpu: u32,

    /// CPU to pin the client (submitting and waiting) thread to (default: the engine's L2 mate there)
    #[arg(long = "client-cpu", default_value_t = DEFAULT_CLIENT_CPU)]
    client_cpu: u32,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    if args.entries == 0 {
        eprintln!("Error: --entries must be at least 1");
        std::process::exit(1);
    }
    run_single_typed(
        OpKind::Read,
        args.entries,
        args.iters,
        args.warmup,
        args.output,
        args.wait_mode,
        Placement {
            engine_cpu: args.engine_cpu,
            client_cpu: args.client_cpu,
        },
    )
}
