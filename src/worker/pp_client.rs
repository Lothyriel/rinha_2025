use std::{
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Result, anyhow};
use chrono::Utc;
use metrics::Unit;
use reqwest::{Client, StatusCode};

use crate::{
    api::payment,
    data::{Payment, ProcessorPaymentRequest},
    db,
};

pub struct PaymentsManager {
    default: PaymentProcesorClient,
    fallback: PaymentProcesorClient,
    store: db::Store,
    micros_cutout: u32,
}

impl PaymentsManager {
    pub fn new(
        default: &str,
        fallback: &str,
        micros_cutout: u32,
        store: db::Store,
        client: &Client,
    ) -> Arc<Self> {
        Arc::new(Self {
            default: PaymentProcesorClient::new(0, default, client.clone()),
            fallback: PaymentProcesorClient::new(1, fallback, client.clone()),
            store,
            micros_cutout,
        })
    }

    pub async fn send(&self, req: payment::Request) -> Result<()> {
        let client = self.get_client();

        let payment = client.send(req).await?;

        self.store.insert(payment).await;

        Ok(())
    }

    pub fn start(self: &Arc<Self>, secs: u64) {
        let m = self.clone();

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(secs)).await;
                m.reset();
            }
        });
    }

    fn reset(&self) {
        let default = self.default.latency.swap(0, Ordering::Relaxed);
        let fallback = self.fallback.latency.swap(0, Ordering::Relaxed);

        tracing::info!("default: {default} | fallback: {fallback}");
    }

    fn get_client(&self) -> &PaymentProcesorClient {
        let default = self.default.latency.load(Ordering::Relaxed);
        let fallback = self.fallback.latency.load(Ordering::Relaxed);

        if default <= fallback + self.micros_cutout {
            &self.default
        } else {
            &self.fallback
        }
    }
}

struct PaymentProcesorClient {
    id: u8,
    client: Client,
    payments_url: String,
    latency: AtomicU32,
}

impl PaymentProcesorClient {
    fn new(id: u8, host: &str, client: Client) -> Self {
        Self {
            payments_url: format!("{host}/payments"),
            latency: AtomicU32::new(0),
            client,
            id,
        }
    }

    async fn send(&self, payment: payment::Request) -> Result<Payment> {
        let payment = ProcessorPaymentRequest {
            requested_at: Utc::now(),
            amount: payment.amount,
            correlation_id: payment.correlation_id,
        };

        tracing::trace!(pp_id = self.id, "sending to payment-processor");

        let now = Instant::now();

        let result = self.http_send(&payment).await;

        let elapsed = now.elapsed().as_micros();

        let latency = match result {
            Ok(_) => elapsed,
            Err(_) => u128::MAX,
        };

        self.latency.store(latency as u32, Ordering::Relaxed);

        metrics::describe_histogram!("pp_http", Unit::Microseconds, "payment processor http time");
        metrics::histogram!("pp_http").record(elapsed as f64);

        result?;

        let amount = payment.amount * 100.0;

        let payment = Payment {
            amount: amount as u64,
            requested_at: payment.requested_at.timestamp_micros(),
            processor_id: self.id,
        };

        Ok(payment)
    }

    async fn http_send(&self, payment: &ProcessorPaymentRequest) -> Result<()> {
        let res = self
            .client
            .post(&self.payments_url)
            .json(payment)
            .send()
            .await?;

        let status = res.status();

        tracing::debug!(pp_payments_status = ?status);

        match status {
            StatusCode::OK => Ok(()),
            _ => Err(anyhow!("{status}")),
        }
    }
}
