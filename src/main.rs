mod api;
mod db;
mod worker;

use anyhow::anyhow;
use clap::Parser;
use tokio::signal::unix::SignalKind;
use tracing_subscriber::{fmt::layer, layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
#[tracing::instrument(skip_all)]
async fn main() {
    let args = Args::parse();

    let env_filter = format!("{},tarpc=warn", args.log_level);

    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from(env_filter))
        .with(
            layer()
                .with_target(false)
                .with_line_number(true)
                .with_file(true)
                .with_thread_ids(true)
                .with_level(true)
                .with_ansi(args.ansi),
        )
        .init();

    let interrupt = signal(SignalKind::interrupt());
    let terminate = signal(SignalKind::terminate());

    let serve = serve(args);

    tokio::select! {
        _ = serve => {},
        _ = interrupt => {},
        _ = terminate => {},
    }
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

    if let Err(e) = result {
        tracing::error!("FATAL: Exiting|err:{e}");
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
    #[arg(short = 'w', required_if_eq("mode", "api"))]
    worker_addr: Option<String>,
    #[arg(long = "ansi", default_value_t = false)]
    ansi: bool,
}
