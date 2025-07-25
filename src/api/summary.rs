use axum::{
    Json,
    extract::{Query, State},
};
use chrono::{DateTime, Utc};
use rusqlite::{Connection, params};
use rust_decimal::{Decimal, dec};

use crate::api::Data;

#[derive(serde::Deserialize)]
pub struct SummaryQuery {
    from: DateTime<Utc>,
    to: DateTime<Utc>,
}

pub async fn get(State(data): State<Data>, Query(query): Query<SummaryQuery>) -> Json<Summary> {
    let conn = data.pool.get().unwrap();

    let payments = get_payments(&conn, query).expect("Failed to get payments");

    let summary = payments.iter().fold([(0, 0), (0, 0)], |mut acc, p| {
        match p.processor_id {
            1 => inc(&mut acc[0], p),
            2 => inc(&mut acc[1], p),
            id => unreachable!("processor_id {{{id}}} should not exist"),
        };

        acc
    });

    let summary = Summary {
        default: build(summary[0]),
        fallback: build(summary[1]),
    };

    Json(summary)
}

fn inc(acc: &mut (u64, u64), tx: &CompletedPayment) {
    acc.0 += 1;
    acc.1 += tx.amount;
}

fn build((total_requests, total_amount): (u64, u64)) -> ProcessedData {
    ProcessedData {
        total_requests,
        total_amount: Decimal::from(total_amount) / dec!(100),
    }
}

fn get_payments(
    conn: &Connection,
    query: SummaryQuery,
) -> Result<Vec<CompletedPayment>, rusqlite::Error> {
    let mut stmt = conn
        .prepare("SELECT processor_id, amount FROM payments WHERE requested_at BETWEEN ? AND ?;")?;

    let (from, to) = (query.from.timestamp_millis(), query.to.timestamp_millis());

    stmt.query_map(params![from, to], |row| {
        Ok(CompletedPayment {
            processor_id: row.get(0)?,
            amount: row.get(1)?,
        })
    })?
    .collect()
}

struct CompletedPayment {
    processor_id: u8,
    amount: u64,
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
