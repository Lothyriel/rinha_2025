use anyhow::Result;
use axum::{Json, extract::State, http::StatusCode};
use rust_decimal::{dec, prelude::ToPrimitive};
use tarpc::context;
use tokio::time::Instant;

use crate::{api::Data, worker::Payment};

pub async fn create(State(data): State<Data>, Json(payment): Json<PaymentRequest>) -> StatusCode {
    let start = Instant::now();

    tracing::debug!("rpc_send: {}", payment.correlation_id);

    if let Err(e) = send(data, payment).await {
        tracing::error!("rpc_send: {e}");
    }

    tracing::info!("Payment processed in {:?}", start.elapsed());
    StatusCode::OK
}

async fn send(data: Data, payment: PaymentRequest) -> Result<()> {
    let cents = payment.amount / dec!(100);

    let payment = Payment {
        amount: cents.to_u64().expect("Valid u64 repr"),
        correlation_id: payment.correlation_id,
    };

    data.client.process(context::current(), payment).await?;

    Ok(())
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PaymentRequest {
    correlation_id: String,
    amount: rust_decimal::Decimal,
}
