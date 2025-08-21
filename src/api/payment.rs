use anyhow::Result;
use tokio::net::UnixStream;

use crate::{data, worker::WorkerRequest};

pub async fn send(socket: &mut UnixStream, buf: &mut [u8]) -> Result<()> {
    let payment: Request = serde_json::from_slice(buf)?;

    tracing::trace!(payment.correlation_id, "uds_send");

    let req = WorkerRequest::Payment(payment);

    data::send(req, buf, socket).await
}

#[derive(Debug, serde::Deserialize, serde::Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Request {
    pub correlation_id: String,
    pub amount: f32,
}
