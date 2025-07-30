pub mod payment;
pub mod summary;
mod util;

use std::{net::Ipv4Addr, time::Duration};

use anyhow::Result;
use axum::{Router, http::Request, routing};
use tokio::net::{TcpListener, UnixListener};

#[tracing::instrument(skip_all)]
pub async fn serve() -> Result<()> {
    tracing::info!("Starting API");

    let layer = tower_http::trace::TraceLayer::new_for_http()
        .make_span_with(|request: &Request<_>| {
            tracing::info_span!(
                "http",
                method = %request.method(),
                uri = %request.uri(),
                version = ?request.version(),
            )
        })
        .on_request(|request: &Request<_>, _: &tracing::Span| {
            tracing::info!(method = ?request.method(), url = ?request.uri(), "req");
        })
        .on_response(
            |response: &axum::http::Response<_>, latency: Duration, _: &tracing::Span| {
                tracing::info!(status = ?response.status(), ?latency, "res");
            },
        );

    let app = Router::new()
        .route("/payments", routing::post(payment::create))
        .route("/payments-summary", routing::get(summary::get))
        .route("/purge-payments", routing::post(util::purge_db))
        .layer(layer);

    match std::env::var("NGINX_SOCKET").ok() {
        Some(f) => {
            std::fs::remove_file(&f).ok();
            tracing::info!("Binding to unix socket on {f}");
            let socket = UnixListener::bind(f)?;

            axum::serve(socket, app).await?
        }
        None => {
            let socket = TcpListener::bind((Ipv4Addr::UNSPECIFIED, 8080)).await?;
            axum::serve(socket, app).await?;
        }
    };

    Ok(())
}
