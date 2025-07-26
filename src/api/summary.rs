use anyhow::Result;
use axum::{
    Json,
    extract::{Query, State},
};
use chrono::{DateTime, Utc};
use rust_decimal::{Decimal, dec};
use tokio::time::Instant;

use crate::{api::Data, db};

#[derive(serde::Deserialize)]
pub struct SummaryQuery {
    from: DateTime<Utc>,
    to: DateTime<Utc>,
}

pub async fn get(State(data): State<Data>, Query(query): Query<SummaryQuery>) -> Json<Summary> {
    let start = Instant::now();

    let summary = get_summary(data, query).expect("Should get summary");

    tracing::info!("pp_payments_summary_http_time: {:?}", start.elapsed());

    Json(summary)
}

fn get_summary(data: Data, query: SummaryQuery) -> Result<Summary> {
    let conn = data.pool.get()?;

    let query = (query.from.timestamp_millis(), query.to.timestamp_millis());

    let payments = db::get_payments(&conn, query)?;

    let summary = payments.iter().fold([(0, 0), (0, 0)], |mut acc, p| {
        match p.processor_id {
            1 => inc(&mut acc[0], p.amount),
            2 => inc(&mut acc[1], p.amount),
            id => unreachable!("processor_id {{{id}}} should not exist"),
        };

        acc
    });

    Ok(Summary {
        default: build(summary[0]),
        fallback: build(summary[1]),
    })
}

fn inc(acc: &mut (u64, u64), amount: u64) {
    acc.0 += 1;
    acc.1 += amount;
}

fn build((total_requests, total_amount): (u64, u64)) -> ProcessedData {
    ProcessedData {
        total_requests,
        total_amount: Decimal::from(total_amount) / dec!(100),
    }
}

#[derive(serde::Serialize)]
pub struct Summary {
    default: ProcessedData,
    fallback: ProcessedData,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct ProcessedData {
    total_requests: u64,
    total_amount: Decimal,
}
