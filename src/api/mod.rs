pub mod payment;
pub mod summary;
mod util;

use std::{net::Ipv4Addr, time::Duration};

use anyhow::Result;
use axum::{Router, http::Request, routing};
use tokio::net::TcpListener;

use crate::bind_unix_socket;

pub async fn serve() -> Result<()> {
    tracing::info!("starting API");

    let layer = tower_http::trace::TraceLayer::new_for_http()
        .on_request(|request: &Request<_>, _: &tracing::Span| {
            tracing::debug!(method = ?request.method(), url = ?request.uri(), "req");
        })
        .on_response(
            |response: &axum::http::Response<_>, latency: Duration, _: &tracing::Span| {
                tracing::debug!(status = ?response.status(), ?latency, "res");
            },
        );

    let app = Router::new()
        .route("/payments", routing::post(payment::create))
        .route("/payments-summary", routing::get(summary::get))
        .route("/purge-payments", routing::post(util::purge_db))
        .layer(layer);

    match std::env::var("NGINX_SOCKET").ok() {
        Some(f) => {
            let listener = bind_unix_socket(&f)?;
            tracing::info!("binded to unix socket on {f}");

            axum::serve(listener, app).await?
        }
        None => {
            const PORT: u16 = 9999;
            let socket = TcpListener::bind((Ipv4Addr::UNSPECIFIED, PORT)).await?;
            tracing::info!("binding to network socket on {PORT}");
            axum::serve(socket, app).await?;
        }
    };

    Ok(())
}
