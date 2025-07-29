use anyhow::Result;
use axum::http::StatusCode;
use tokio::{io::AsyncWriteExt, net::UnixStream};

use crate::{
    data,
    worker::{UDS_PATH, WorkerRequest},
};

#[tracing::instrument(skip_all)]
pub async fn purge_db() -> StatusCode {
    purge().await.expect("purge_db uds_send");

    StatusCode::OK
}

#[tracing::instrument(skip_all)]
async fn purge() -> Result<()> {
    let mut socket = UnixStream::connect(UDS_PATH).await?;

    let mut buf = [0u8; 32];
    let n = data::encode(WorkerRequest::PurgeDb, &mut buf);

    socket.write_all(&buf[..n]).await?;

    Ok(())
}
