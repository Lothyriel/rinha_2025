use std::sync::atomic::Ordering;

use axum::{Json, Router, extract::State, http::StatusCode, routing};

use crate::{ACTIVE_IDX, Data, Payment};

pub async fn create(State(data): State<Data>, Json(payment): Json<Payment>) -> StatusCode {
    let active_url = &data.urls[ACTIVE_IDX.load(Ordering::Acquire)];

    StatusCode::OK
}
