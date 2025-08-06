mod api;
mod data;
mod db;
mod lb;
mod worker;

use std::{fs::Permissions, os::unix::fs::PermissionsExt, time::Duration};

use anyhow::{Result, anyhow};
use clap::Parser;
use metrics_util::Quantile;
use once_cell::sync::Lazy;
use tokio::net::UnixListener;
use tracing_subscriber::{EnvFilter, fmt::layer, layer::SubscriberExt, util::SubscriberInitExt};

fn main() {
    init_tracing();

    init_metrics();

    serve(Args::parse())
}

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn init_metrics() {
    let mut recorder = metrics_printer::PrintRecorder::default();

    recorder
        .do_print_metadata()
        .set_print_interval(Duration::from_secs(15))
        .select_quantiles(Box::new([
            Quantile::new(0.50),
            Quantile::new(0.99),
            Quantile::new(1.0),
        ]));

    recorder.install().expect("register recorder");
}

fn init_tracing() {
    dotenvy::dotenv().ok();

    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info"))
        .expect("valid level");

    let fmt = layer()
        .with_target(false)
        .with_line_number(true)
        .with_file(true)
        .with_thread_ids(true)
        .with_level(true)
        .with_ansi(false);

    tracing_subscriber::registry().with(filter).with(fmt).init();
}

fn serve(args: Args) {
    let result = match args.mode.as_str() {
        "api" => api::serve(),
        "worker" => worker::serve(),
        "lb" => lb::serve(),
        _ => Err(anyhow!("Invalid mode {:?}", args.mode)),
    };

    if let Err(err) = result {
        tracing::error!(?err, "FATAL: Exiting");
    }
}

#[derive(Parser)]
#[command(about = "Rinha 2025")]
struct Args {
    #[arg(short = 'm', value_parser = ["api", "worker", "lb"], help = "The mode in which the binary will run")]
    mode: String,
}

static WORKER_SOCKET: Lazy<String> =
    Lazy::new(|| std::env::var("WORKER_SOCKET").unwrap_or("./worker.sock".to_string()));

fn bind_unix_socket(file: &str) -> Result<UnixListener> {
    std::fs::remove_file(file).ok();

    let listener = UnixListener::bind(file)?;

    let permissions = Permissions::from_mode(0o666);
    std::fs::set_permissions(file, permissions)?;

    Ok(listener)
}
