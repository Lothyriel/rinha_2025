mod api;
mod db;
mod worker;

use anyhow::{Result, anyhow};
use clap::Parser;
use reqwest::Url;
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

    init_tracing(&args).expect("Configure tracing");

    tokio::select! {
        _ = serve(args) => {},
        _ = signal(SignalKind::interrupt()) => {},
        _ = signal(SignalKind::terminate()) => {},
    }
}

fn init_tracing(args: &Args) -> Result<()> {
    let loki_addr = args.loki_addr.as_deref().unwrap_or("http://loki:3100");

    let (loki_layer, task) = tracing_loki::builder()
        .label("app", &args.mode)?
        .build_url(Url::parse(loki_addr)?)?;

    tokio::spawn(task);

    let fmt = layer()
        .json()
        .with_target(false)
        .with_line_number(true)
        .with_file(true)
        .with_thread_ids(true)
        .with_level(true)
        .with_current_span(true)
        .with_span_events(FmtSpan::CLOSE);

    tracing_subscriber::registry()
        .with(EnvFilter::new(format!("{},tarpc=warn", args.log_level)))
        .with(fmt)
        .with(loki_layer)
        .init();

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
        "api" => {
            let addr = args
                .worker_addr
                .unwrap_or_else(|| unreachable!("Clap shouldn't allow missing worker_addr"));

            api::serve(args.port, &addr).await
        }
        "worker" => worker::serve(args.port).await,
        _ => Err(anyhow!("Invalid mode {:?}", args.mode)),
    };

    if let Err(err) = result {
        tracing::error!(?err, "FATAL: Exiting");
    }
}

#[derive(Parser)]
#[command(about = "Rinha 2025")]
struct Args {
    #[arg(short = 'l', default_value = "info", value_parser = ["error", "warn", "info", "debug", "trace"])]
    log_level: String,
    #[arg(short = 'p', default_value_t = 80)]
    port: u16,
    #[arg(short = 'm', value_parser = ["api", "worker"])]
    mode: String,
    #[arg(long = "loki")]
    loki_addr: Option<String>,
    #[arg(short = 'w', required_if_eq("mode", "api"))]
    worker_addr: Option<String>,
}
