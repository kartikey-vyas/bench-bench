use std::path::PathBuf;

use clap::Parser;
use rust_client::{run_benchmark, Config};

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, default_value = "config/workload.smoke.json")]
    config: PathBuf,
    #[arg(long = "output-dir")]
    output_dir: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();
    let config = Config::from_path(args.config)?;
    let result = run_benchmark(config, args.output_dir).await?;
    println!(
        "rust requests/s={:.2} chunks/s={:.2} failures={}",
        result.summary.requests_per_second,
        result.summary.chunks_per_second,
        result.summary.failed_requests
    );
    Ok(())
}
