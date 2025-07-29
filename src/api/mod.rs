mod payment;
mod summary;

use std::{net::Ipv4Addr, time::Duration};

use anyhow::Result;
use axum::{
    Router,
    http::{Request, StatusCode},
    routing,
};

use crate::{db, worker::rpc};

#[tracing::instrument(skip_all)]
pub async fn serve(port: u16, worker_addr: &str) -> Result<()> {
    tracing::info!("Starting API on port {port}");

    let pool = db::read_pool()?;

    let client = rpc::client(worker_addr).await?;

    let state = Data { pool, client };

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
        .route("/purge-payments", routing::post(purge_db))
        .layer(layer)
        .with_state(state);

    let addr = (Ipv4Addr::UNSPECIFIED, port);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

#[tracing::instrument(skip_all)]
async fn purge_db() -> StatusCode {
    fn purge() -> Result<()> {
        let pool = db::write_pool()?;

        let conn = pool.get()?;

        db::purge(&conn)?;

        Ok(())
    }

    match purge() {
        Ok(_) => tracing::info!("DB purged"),
        Err(err) => tracing::error!(?err),
    }

    StatusCode::OK
}

#[derive(Clone, Debug)]
struct Data {
    pool: db::Pool,
    client: rpc::PaymentServiceClient,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PaymentRequest {
    pub correlation_id: String,
    pub amount: f32,
}
