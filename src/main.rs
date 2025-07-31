mod api;
mod data;
mod db;
mod worker;

use anyhow::{Result, anyhow};
use clap::Parser;
use once_cell::sync::Lazy;
use tokio::signal::unix::SignalKind;
use tracing_subscriber::{
    EnvFilter,
    fmt::{format::FmtSpan, layer},
    layer::SubscriberExt,
    util::SubscriberInitExt,
};

#[tokio::main]
#[tracing::instrument(skip_all)]
async fn main() {
    init_tracing().expect("Configure tracing");

    tokio::select! {
        _ = serve(Args::parse()) => {},
        _ = signal(SignalKind::interrupt()) => {},
        _ = signal(SignalKind::terminate()) => {},
    }
}

fn init_tracing() -> Result<()> {
    dotenvy::dotenv().ok();

    let filter = EnvFilter::try_from_default_env().or_else(|_| EnvFilter::try_new("info"))?;

    let fmt = layer()
        .json()
        .with_target(false)
        .with_line_number(true)
        .with_file(true)
        .with_thread_ids(true)
        .with_level(true)
        .with_current_span(true)
        .with_span_events(FmtSpan::CLOSE);

    tracing_subscriber::registry().with(filter).with(fmt).init();

    Ok(())
}

async fn signal(kind: SignalKind) {
    tokio::signal::unix::signal(kind)
        .expect("failed to install signal handler")
        .recv()
        .await;
}

async fn serve(args: Args) {
    let result = match args.mode.as_str() {
        "api" => api::serve().await,
        "worker" => worker::serve().await,
        _ => Err(anyhow!("Invalid mode {:?}", args.mode)),
    };

    if let Err(err) = result {
        tracing::error!(?err, "FATAL: Exiting");
    }
}

#[derive(Parser)]
#[command(about = "Rinha 2025")]
struct Args {
    #[arg(short = 'm', value_parser = ["api", "worker"], help = "The mode in which the binary will run")]
    mode: String,
}

pub static WORKER_SOCKET: Lazy<String> =
    Lazy::new(|| std::env::var("WORKER_SOCKET").unwrap_or("/uds/rinha.sock".to_string()));
