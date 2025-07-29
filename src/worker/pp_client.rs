use std::time::Duration;

use anyhow::{Result, anyhow};
use reqwest::{Client, StatusCode};

use crate::worker::{Payment, ProcessorPayment};

#[tracing::instrument(skip_all)]
pub async fn create(client: &Client, payment: ProcessorPayment) -> Payment {
    let processor_id = send_to_processor(client, &payment).await;

    let amount = payment.amount * 100.0;

    Payment {
        amount: amount as u64,
        requested_at: payment.requested_at.timestamp_millis(),
        processor_id,
    }
}

const PAYMENT_PROCESSORS: [(u8, &str); 2] = [
    (1, "http://payment-processor-default:8080"),
    (2, "http://payment-processor-fallback:8080"),
];

#[tracing::instrument(skip_all)]
async fn send_to_processor(client: &Client, payment: &ProcessorPayment) -> u8 {
    //todo: this needs to be handled way better
    //todo: map and use the GET /payments/service-health

    loop {
        for (id, uri) in PAYMENT_PROCESSORS {
            for _ in 0..5 {
                let result = send(uri, client, payment).await;

                match result {
                    Ok(_) => return id,
                    Err(err) => {
                        tracing::warn!(?err, "pp_payments_err");
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        continue;
                    }
                }
            }
        }
    }
}

#[tracing::instrument(skip_all)]
async fn send(uri: &str, client: &Client, payment: &ProcessorPayment) -> Result<()> {
    let res = client
        .post(format!("{uri}/payments"))
        .json(payment)
        .send()
        .await?;

    let status = res.status();

    tracing::debug!(pp_payments_status = ?status);

    match res.status() {
        StatusCode::OK => Ok(()),
        _ => Err(anyhow!("{status}")),
    }
}
