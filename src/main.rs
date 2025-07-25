mod api;
mod db;
mod worker;

use anyhow::{Result, anyhow};
use clap::Parser;
use tracing_subscriber::{fmt::layer, layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from("debug"))
        .with(
            layer()
                .with_target(false)
                .with_line_number(true)
                .with_file(true),
        )
        .init();

    if let Err(e) = serve(Args::parse()).await {
        tracing::error!("FATAL: Exiting | {e}");
    }
}

async fn serve(args: Args) -> Result<()> {
    match args.mode.as_str() {
        "api" => {
            let addr = args
                .worker_addr
                .unwrap_or_else(|| unreachable!("Clap shouldn't allow missing worker_addr"));

            api::serve(&addr).await
        }
        "worker" => worker::serve().await,
        _ => Err(anyhow!("Invalid mode {:?}", args.mode)),
    }
}

#[derive(Parser)]
#[command(about = "Rinha 2025")]
struct Args {
    #[arg(short = 'm', value_parser = ["api", "worker"])]
    mode: String,

    #[arg(short = 'w', required_if_eq("mode", "api"))]
    worker_addr: Option<String>,
}
