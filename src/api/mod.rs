mod payment;
mod summary;

use anyhow::Result;
use axum::{Router, routing};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;

use crate::{db::init_pool, worker::rpc};

pub async fn serve(worker_addr: &str) -> Result<()> {
    tracing::info!("Starting API...");

    let pool = init_pool(10)?;

    let client = rpc::client(worker_addr).await?;

    let state = Data { pool, client };

    let app = Router::new()
        .route("/payments", routing::post(payment::create))
        .route("/payments-summary", routing::get(summary::get))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:80").await?;
    axum::serve(listener, app).await?;

    Ok(())
}

#[derive(Clone)]
struct Data {
    pool: Pool<SqliteConnectionManager>,
    client: rpc::PaymentServiceClient,
}
