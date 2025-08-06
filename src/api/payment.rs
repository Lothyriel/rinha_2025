use anyhow::Result;
use tokio::{io::AsyncWriteExt, net::UnixStream};

use crate::{WORKER_SOCKET, data, worker::WorkerRequest};

pub async fn send(buf: &mut [u8], payment: Request) -> Result<()> {
    tracing::trace!(payment.correlation_id, "uds_send");

    let mut socket = UnixStream::connect(&*WORKER_SOCKET).await?;

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
