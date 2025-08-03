use std::time::Instant;

use anyhow::{Result, anyhow};
use chrono::Utc;
use metrics::Unit;
use once_cell::sync::Lazy;
use reqwest::{Client, StatusCode};

use crate::{
    api::payment,
    data::{Payment, ProcessorPaymentRequest},
};

pub static PAYMENT_PROCESSORS: Lazy<[(u8, String); 2]> = Lazy::new(|| {
    let default = std::env::var("PROCESSOR_DEFAULT")
        .unwrap_or("http://payment-processor-default:8080".to_string());

    let fallback = std::env::var("PROCESSOR_FALLBACK")
        .unwrap_or("http://payment-processor-fallback:8080".to_string());

    [(1, default), (2, fallback)]
});

pub async fn send(client: &Client, payment: payment::Request) -> Result<Payment> {
    //todo: this needs to be handled way better
    //todo: map and use the GET /payments/service-health

    let payment = ProcessorPaymentRequest {
        requested_at: Utc::now(),
        amount: payment.amount,
        correlation_id: payment.correlation_id,
    };

    //for (id, uri) in PAYMENT_PROCESSORS {
    let (id, uri) = &(*PAYMENT_PROCESSORS)[0];

    tracing::debug!(pp_id = id, "sending to payment-processor");

    http_send(uri, client, &payment).await?;

    let amount = payment.amount * 100.0;

    let payment = Payment {
        amount: amount as u64,
        requested_at: payment.requested_at.timestamp_micros(),
        processor_id: *id,
    };

    Ok(payment)
}

async fn http_send(uri: &str, client: &Client, payment: &ProcessorPaymentRequest) -> Result<()> {
    let now = Instant::now();

    let res = client
        .post(format!("{uri}/payments"))
        .json(payment)
        .send()
        .await?;

    let status = res.status();

    tracing::debug!(pp_payments_status = ?status);

    let res = match res.status() {
        StatusCode::OK => Ok(()),
        _ => Err(anyhow!("{status}")),
    };

    metrics::describe_histogram!("pp_http", Unit::Microseconds, "payment processor http time");
    metrics::histogram!("pp_http").record(now.elapsed().as_micros() as f64);

    res
}
