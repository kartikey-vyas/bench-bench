use std::net::SocketAddr;

use clap::Parser;
use server_rust::app;
use tokio::net::TcpListener;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:8080")]
    bind: SocketAddr,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let listener = TcpListener::bind(args.bind).await?;
    println!("synthetic OpenAI-style server listening on http://{}", args.bind);
    axum::serve(listener, app()).await?;
    Ok(())
}
