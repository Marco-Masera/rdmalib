use clap::Parser;
use rdmalib_benchmarks::{
    DEFAULT_CLIENT_CPU, DEFAULT_ENGINE_CPU, OpKind, Placement, WaitMode, run_single_untyped,
};

#[derive(Parser, Debug)]
#[command(
    name = "single_read_untyped",
    about = "Benchmark 1: Single Read, single word (8 B), untyped"
)]
struct Args {
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
    run_single_untyped(
        OpKind::Read,
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
