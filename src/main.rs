mod api;
mod data;
mod db;
mod worker;

use anyhow::{Result, anyhow};
use clap::Parser;
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
    let args = Args::parse();

    init_tracing().expect("Configure tracing");

    tokio::select! {
        _ = serve(args) => {},
        _ = signal(SignalKind::interrupt()) => {},
        _ = signal(SignalKind::terminate()) => {},
    }
}

fn init_tracing() -> Result<()> {
    let filter =
        EnvFilter::try_from_default_env().or_else(|_| EnvFilter::try_new("warn,tarpc=warn"))?;

    let fmt = layer()
        .json()
        .with_target(false)
        .with_line_number(true)
        .with_file(true)
        .with_thread_ids(true)
        .with_level(true)
        .with_current_span(true)
        .with_span_events(FmtSpan::FULL);

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
        "api" => api::serve(args.port).await,
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
    #[arg(
        short = 'p',
        default_value_t = 80,
        help = "The port in which the api will bind"
    )]
    port: u16,
    #[arg(short = 'm', value_parser = ["api", "worker"], help = "The mode in which the binary will run")]
    mode: String,
}
