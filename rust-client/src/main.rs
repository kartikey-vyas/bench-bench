use std::path::PathBuf;

use clap::Parser;
use rust_client::{run_benchmark_with_client, ClientKind, Config};

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, default_value = "config/workload.smoke.json")]
    config: PathBuf,
    #[arg(long = "output-dir")]
    output_dir: Option<PathBuf>,
    #[arg(long, default_value = "reqwest")]
    client: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();
    let config = Config::from_path(args.config)?;
    let client_kind = ClientKind::parse(&args.client)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
    let result = run_benchmark_with_client(config, args.output_dir, client_kind).await?;
    println!(
        "rust/{} requests/s={:.2} chunks/s={:.2} efficiency={:.3} failures={} incomplete={}",
        result.implementation,
        result.summary.requests_per_second,
        result.summary.chunks_per_second,
        result.summary.efficiency,
        result.summary.failed_requests,
        result.summary.incomplete_requests
    );
    Ok(())
}
