use axum::{Json, extract::State};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

use crate::{Data, Payment};

#[derive(serde::Deserialize)]
struct SummaryQuery {
    from: DateTime<Utc>,
    to: DateTime<Utc>,
}

pub async fn get(State(data): State<Data>, Json(payment): Json<Payment>) -> Json<Summary> {
    todo!()
}

#[derive(serde::Serialize)]
pub struct Summary {
    default: ProcessedData,
    fallback: ProcessedData,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct ProcessedData {
    total_requests: usize,
    total_amount: Decimal,
}
