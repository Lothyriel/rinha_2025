pub mod payment;
pub mod summary;
mod util;

use std::{net::Ipv4Addr, time::Duration};

use anyhow::Result;
use axum::{Router, http::Request, routing};

#[tracing::instrument(skip_all)]
pub async fn serve(port: u16) -> Result<()> {
    tracing::info!("Starting API on port {port}");

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

    let addr = (Ipv4Addr::UNSPECIFIED, port);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
