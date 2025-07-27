use axum::{Json, extract::State, http::StatusCode};
use rust_decimal::{dec, prelude::ToPrimitive};
use tarpc::context;
use tokio::time::Instant;

use crate::{api::Data, worker::Payment};

#[tracing::instrument(skip_all)]
pub async fn create(State(data): State<Data>, Json(payment): Json<PaymentRequest>) -> StatusCode {
    let start = Instant::now();

    tracing::info!("rpc_send: {}", payment.correlation_id);

    tokio::spawn(send(data, payment));

    tracing::info!("pp_payments_http_time: {:?}", start.elapsed());

    StatusCode::OK
}

#[tracing::instrument(skip_all)]
async fn send(data: Data, payment: PaymentRequest) {
    let cents = payment.amount * dec!(100);

    let payment = Payment {
        amount: cents.to_u64().expect("Valid u64 repr"),
        correlation_id: payment.correlation_id,
    };

    if let Err(e) = data.client.process(context::current(), payment).await {
        tracing::error!("rpc_send: {e}");
    }
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PaymentRequest {
    correlation_id: String,
    amount: rust_decimal::Decimal,
}
