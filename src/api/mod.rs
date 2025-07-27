mod payment;
mod summary;

use std::net::Ipv4Addr;

use anyhow::Result;
use axum::{Router, http::StatusCode, routing};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;

use crate::{db, worker::rpc};

#[tracing::instrument(skip_all)]
pub async fn serve(port: u16, worker_addr: &str) -> Result<()> {
    tracing::info!("Starting API on port {port}");

    let pool = db::read_pool()?;

    let client = rpc::client(worker_addr).await?;

    let state = Data { pool, client };

    let app = Router::new()
        .route("/payments", routing::post(payment::create))
        .route("/purge-payments", routing::post(purge_db))
        .route("/payments-summary", routing::get(summary::get))
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
        Err(e) => tracing::error!("{e}"),
    }

    StatusCode::OK
}

#[derive(Clone, Debug)]
struct Data {
    pool: Pool<SqliteConnectionManager>,
    client: rpc::PaymentServiceClient,
}
