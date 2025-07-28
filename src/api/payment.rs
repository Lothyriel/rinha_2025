use axum::{Json, extract::State, http::StatusCode};
use tarpc::context;

use crate::{api::Data, worker::Payment};

#[tracing::instrument(skip_all)]
pub async fn create(State(data): State<Data>, Json(payment): Json<PaymentRequest>) -> StatusCode {
    tokio::spawn(send(data, payment));

    StatusCode::OK
}

#[tracing::instrument(skip_all)]
async fn send(data: Data, payment: PaymentRequest) {
    let cents = payment.amount * 100.0;

    let payment = Payment {
        amount: cents as u64,
        correlation_id: payment.correlation_id,
    };

    tracing::info!(payment.correlation_id, "rpc_send");
    match data.client.process(context::current(), payment).await {
        Ok(_) => {}
        Err(err) => tracing::error!(?err, "rpc_send"),
    }
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PaymentRequest {
    correlation_id: String,
    amount: f32,
}
