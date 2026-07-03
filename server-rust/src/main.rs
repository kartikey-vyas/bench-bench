use std::net::SocketAddr;

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
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let listener = TcpListener::bind(args.bind).await?;
    println!("synthetic OpenAI-style server listening on http://{}", args.bind);
    let router = app();

    loop {
        let (socket, _remote) = listener.accept().await?;
        // Nagle + delayed ACK would distort millisecond-scale SSE pacing.
        socket.set_nodelay(true)?;
        let service = router.clone();
        tokio::spawn(async move {
            let io = TokioIo::new(socket);
            let hyper_service = hyper::service::service_fn(
                move |request: hyper::Request<Incoming>| service.clone().call(request),
            );
            let _ = auto::Builder::new(TokioExecutor::new())
                .serve_connection(io, hyper_service)
                .await;
        });
    }
}
