use std::time::Duration;

use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use reqwest::{Client, StatusCode};
use rust_decimal::{Decimal, dec};

use crate::{db, worker::Payment};

pub async fn handle(
    pool: Pool<SqliteConnectionManager>,
    client: &Client,
    payment: Payment,
) -> Result<()> {
    let now = Utc::now();

    let amount = payment.amount;

    let payment = ProcessorPayment {
        requested_at: now,
        amount: Decimal::from(amount) / dec!(100),
        correlation_id: payment.correlation_id,
    };

    let processor_id = process(client, &payment).await;

    let conn = pool.get()?;

    db::insert_payment(&conn, now.timestamp_millis(), amount, processor_id)?;

    Ok(())
}

const PAYMENT_PROCESSORS: [(u8, &str); 2] = [
    (1, "http://payment-processor-default:8080"),
    (2, "http://payment-processor-fallback:8080"),
];

#[tracing::instrument(skip_all)]
async fn process(client: &Client, payment: &ProcessorPayment) -> u8 {
    //todo: this needs to be handled way better
    //todo: map and use the GET /payments/service-health

    loop {
        for (id, uri) in PAYMENT_PROCESSORS {
            let result = send(uri, client, payment).await;

            match result {
                Ok(_) => return id,
                Err(e) => {
                    tracing::warn!("pp_payments_err: {e}");
                    continue;
                }
            }
        }

        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

#[tracing::instrument(skip_all)]
async fn send(uri: &str, client: &Client, payment: &ProcessorPayment) -> anyhow::Result<()> {
    let res = client
        .post(format!("{uri}/payments"))
        .json(payment)
        .send()
        .await?;

    let status = res.status();

    tracing::debug!("pp_payments_status: {status}");

    match res.status() {
        StatusCode::OK => Ok(()),
        _ => Err(anyhow!("{status}")),
    }
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct ProcessorPayment {
    requested_at: DateTime<Utc>,
    amount: Decimal,
    correlation_id: String,
}
