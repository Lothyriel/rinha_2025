mod api;
mod db;
mod worker;

use anyhow::{Result, anyhow};
use clap::Parser;
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

    if let Err(e) = serve(args).await {
        tracing::error!("FATAL: Exiting|err:{e}");
    }
}

async fn serve(args: Args) -> Result<()> {
    match args.mode.as_str() {
        "api" => {
            let addr = args
                .worker_addr
                .unwrap_or_else(|| unreachable!("Clap shouldn't allow missing worker_addr"));

            api::serve(args.port, &addr).await
        }
        "worker" => worker::serve(args.port).await,
        _ => Err(anyhow!("Invalid mode {:?}", args.mode)),
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
