use std::time::Instant;

use anyhow::Result;
use axum::{Json, http::StatusCode};
use metrics::Unit;
use tokio::{io::AsyncWriteExt, net::UnixStream};

use crate::{WORKER_SOCKET, data, worker::WorkerRequest};

pub async fn create(Json(payment): Json<Request>) -> StatusCode {
    let now = Instant::now();

    tokio::spawn(async {
        if let Err(err) = send(payment).await {
            tracing::error!(?err, "uds_send_err /payments");
        }
    });

    metrics::describe_histogram!("http.post", Unit::Microseconds, "http handler time");
    metrics::histogram!("http.post").record(now.elapsed().as_micros() as f64);

    StatusCode::OK
}

async fn send(payment: Request) -> Result<()> {
    tracing::debug!(payment.correlation_id, "uds_send");

    let mut socket = UnixStream::connect(&*WORKER_SOCKET).await?;

    let mut buf = [0u8; 64];
    let n = data::encode(WorkerRequest::Payment(payment), &mut buf);

    socket.write_all(&buf[..n]).await?;

    Ok(())
}

#[derive(Debug, serde::Deserialize, serde::Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Request {
    pub correlation_id: String,
    pub amount: f32,
}
