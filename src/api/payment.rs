use anyhow::Result;
use axum::{Json, http::StatusCode};
use tokio::{io::AsyncWriteExt, net::UnixStream};

use crate::{
    data,
    worker::{UDS_PATH, WorkerRequest},
};

#[tracing::instrument(skip_all)]
pub async fn create(Json(payment): Json<Request>) -> StatusCode {
    tokio::spawn(async { send(payment).await.expect("uds_send /payments") });

    StatusCode::OK
}

#[tracing::instrument(skip_all)]
async fn send(payment: Request) -> Result<()> {
    tracing::info!(payment.correlation_id, "uds_send");

    let mut socket = UnixStream::connect(UDS_PATH).await?;

    let mut buf = [0u8; 64];
    let n = data::encode(WorkerRequest::Payment(payment), &mut buf);

    socket.write_all(&buf[..n]).await?;

    Ok(())
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Request {
    pub correlation_id: String,
    pub amount: f32,
}
