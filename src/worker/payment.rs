use chrono::Utc;
use reqwest::Client;

use crate::{
    api::payment,
    data::{Payment, ProcessorPaymentRequest},
    worker::{Sender, pp_client},
};

pub async fn process(sender: Sender, client: Client, payment: payment::Request) {
    tracing::info!(payment.correlation_id, "rpc_recv");

    let payment = ProcessorPaymentRequest {
        requested_at: Utc::now(),
        amount: payment.amount,
        correlation_id: payment.correlation_id,
    };

    let processor_id = pp_client::send(&client, &payment).await;

    let amount = payment.amount * 100.0;

    let payment = Payment {
        amount: amount as u64,
        requested_at: payment.requested_at.timestamp_millis(),
        processor_id,
    };

    tracing::info!("mpsc_send");

    sender
        .send(payment)
        .expect("Consumer should not have dropped")
}
