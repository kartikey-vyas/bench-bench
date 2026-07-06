use std::{net::SocketAddr, time::Duration};

use clap::Parser;
use hyper::body::Incoming;
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto,
};
use server_rust::app;
use tokio::net::TcpListener;
use tower::Service;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:8080")]
    bind: SocketAddr,
    /// Tokio worker threads for the server runtime. Defaults to all cores.
    #[arg(long)]
    worker_threads: Option<usize>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    if let Some(threads) = args.worker_threads {
        builder.worker_threads(threads);
    }
    builder.build()?.block_on(serve(args))
}

async fn serve(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(args.bind).await?;
    let threads = args
        .worker_threads
        .map_or_else(|| "all cores".to_string(), |threads| threads.to_string());
    println!(
        "synthetic OpenAI-style server listening on http://{} (worker threads: {threads})",
        args.bind
    );
    let router = app();

    loop {
        let (socket, _remote) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(error) => {
                // Transient accept failures (e.g. fd exhaustion) must not kill the
                // server mid-benchmark; back off briefly instead of hot-spinning.
                eprintln!("accept error: {error}");
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }
        };
        if let Err(error) = socket.set_nodelay(true) {
            eprintln!("set_nodelay error: {error}");
            continue;
        }
        let service = router.clone();
        tokio::spawn(async move {
            let io = TokioIo::new(socket);
            let hyper_service =
                hyper::service::service_fn(move |request: hyper::Request<Incoming>| {
                    service.clone().call(request)
                });
            let _ = auto::Builder::new(TokioExecutor::new())
                .serve_connection(io, hyper_service)
                .await;
        });
    }
}
