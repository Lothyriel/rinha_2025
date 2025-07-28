mod api;
mod db;
mod worker;

use std::collections::HashMap;

use anyhow::{Result, anyhow};
use clap::Parser;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::{SpanExporter, WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::trace::SdkTracerProvider;
use tokio::signal::unix::SignalKind;
use tracing_opentelemetry::OpenTelemetryLayer;
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
    let filter =
        EnvFilter::try_from_default_env().or_else(|_| EnvFilter::try_new("info,tarpc=warn"))?;

    let fmt = layer()
        .json()
        .with_target(false)
        .with_line_number(true)
        .with_file(true)
        .with_thread_ids(true)
        .with_level(true)
        .with_current_span(true)
        .with_span_events(FmtSpan::CLOSE);

    let openobserve_addr = args.oo_addr.as_deref().unwrap_or("http://openobserve:5080");

    let headers = HashMap::from([(
        "authorization".to_string(),
        "Basic YWRtaW5AYWRtaW4uY29tOlh6cEQ3YThad2FKQTVBaDk=".to_string(),
    )]);

    let exporter = SpanExporter::builder()
        .with_http()
        .with_endpoint(openobserve_addr)
        .with_headers(headers)
        .build()?;

    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .build();

    let otel_layer = OpenTelemetryLayer::new(provider.tracer("rinha"));

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt)
        .with(otel_layer)
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
    #[arg(short = 'p', default_value_t = 80)]
    port: u16,
    #[arg(short = 'm', value_parser = ["api", "worker"])]
    mode: String,
    #[arg(short = 'o')]
    oo_addr: Option<String>,
    #[arg(short = 'w', required_if_eq("mode", "api"))]
    worker_addr: Option<String>,
}
