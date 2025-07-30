use std::time::Duration;

use anyhow::{Result, anyhow};
use reqwest::{Client, StatusCode};

use crate::data::ProcessorPaymentRequest;

const PAYMENT_PROCESSORS: [(u8, &str); 2] = [
    (1, "http://payment-processor-default:8080"),
    (2, "http://payment-processor-fallback:8080"),
];

#[tracing::instrument(skip_all)]
pub async fn send(client: &Client, payment: &ProcessorPaymentRequest) -> u8 {
    //todo: this needs to be handled way better
    //todo: map and use the GET /payments/service-health

    loop {
        for (id, uri) in PAYMENT_PROCESSORS {
            for i in 0..5 {
                tracing::info!(pp_id = id, retry = i, "sending to payment-processor");
                let result = http_send(uri, client, payment).await;

                match result {
                    Ok(_) => return id,
                    Err(err) => {
                        tracing::info!(?err, "pp_payments_err");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        }
    }
}

#[tracing::instrument(skip_all)]
async fn http_send(uri: &str, client: &Client, payment: &ProcessorPaymentRequest) -> Result<()> {
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
