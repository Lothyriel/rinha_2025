use anyhow::{Result, anyhow};
use chrono::Utc;
use reqwest::{Client, StatusCode};

use crate::{
    api::payment,
    data::{Payment, ProcessorPaymentRequest},
};

const PAYMENT_PROCESSORS: [(u8, &str); 2] = [
    (1, "http://payment-processor-default:8080"),
    (2, "http://payment-processor-fallback:8080"),
];

#[tracing::instrument(skip_all)]
pub async fn send(client: &Client, payment: payment::Request) -> Result<Payment> {
    //todo: this needs to be handled way better
    //todo: map and use the GET /payments/service-health

    let payment = ProcessorPaymentRequest {
        requested_at: Utc::now(),
        amount: payment.amount,
        correlation_id: payment.correlation_id,
    };

    //for (id, uri) in PAYMENT_PROCESSORS {
    let (id, uri) = PAYMENT_PROCESSORS[0];

    tracing::info!(pp_id = id, "sending to payment-processor");

    http_send(uri, client, &payment).await?;

    let amount = payment.amount * 100.0;

    let payment = Payment {
        amount: amount as u64,
        requested_at: payment.requested_at.timestamp_micros(),
        processor_id: id,
    };

    Ok(payment)
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
