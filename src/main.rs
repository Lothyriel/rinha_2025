use std::time::Duration;

use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;

mod api;
mod worker;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let mode = std::env::args().nth(1);

    let result = match mode.as_deref() {
        Some("api") => api::serve().await,
        Some("worker") => worker::serve().await,
        m => panic!("Invalid mode {m:?}"),
    };

    result.expect("Failed to run app");
}

const DB_FILE: &str = "/sqlite/rinha.db";

pub fn get_db_pool(max: u32) -> Pool<SqliteConnectionManager> {
    let manager = SqliteConnectionManager::file(DB_FILE);

    Pool::builder()
        .connection_timeout(Duration::from_secs(1))
        .max_size(max)
        .build(manager)
        .expect("Unable to create pool")
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct Payment {
    correlation_id: String,
    amount: rust_decimal::Decimal,
}
