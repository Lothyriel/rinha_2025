use anyhow::Result;
use tokio::{io::AsyncWriteExt, net::UnixStream};

use crate::{data, worker::WorkerRequest};

pub async fn send(socket: &mut UnixStream, buf: &mut [u8]) -> Result<()> {
    let payment: Request = serde_json::from_slice(buf)?;

    tracing::trace!(payment.correlation_id, "uds_send");

    let n = data::encode(WorkerRequest::Payment(payment), buf);

    socket.write_all(&buf[..n]).await?;

    Ok(())
}

#[derive(Debug, serde::Deserialize, serde::Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Request {
    pub correlation_id: String,
    pub amount: f32,
}
