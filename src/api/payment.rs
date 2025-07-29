use axum::{Json, extract::State, http::StatusCode};
use tarpc::context;

use crate::api::{Data, PaymentRequest};

#[tracing::instrument(skip_all)]
pub async fn create(State(data): State<Data>, Json(payment): Json<PaymentRequest>) -> StatusCode {
    tokio::spawn(send(data, payment));

    StatusCode::OK
}

#[tracing::instrument(skip_all)]
async fn send(data: Data, payment: PaymentRequest) {
    tracing::info!(payment.correlation_id, "rpc_send");

    match data.client.process(context::current(), payment).await {
        Ok(_) => {}
        Err(err) => tracing::error!(?err, "rpc_send"),
    }
}
