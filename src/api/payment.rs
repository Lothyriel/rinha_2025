use axum::{Json, extract::State, http::StatusCode};
use tarpc::context;
use tokio::time::Instant;

use crate::{Payment, api::Data};

pub async fn create(State(data): State<Data>, Json(payment): Json<Payment>) -> StatusCode {
    let start = Instant::now();

    data.client
        .process(context::current(), payment)
        .await
        .expect("Client failed to send RPC message");

    tracing::debug!("Payment processed in {:?}", start.elapsed());
    StatusCode::OK
}
