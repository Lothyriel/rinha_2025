use anyhow::Result;
use axum::http::StatusCode;
use tokio::{io::AsyncWriteExt, net::UnixStream};

use crate::{WORKER_SOCKET, data, worker::WorkerRequest};

pub async fn purge_db() -> StatusCode {
    match purge().await {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

async fn purge() -> Result<()> {
    let mut socket = UnixStream::connect(&*WORKER_SOCKET).await?;

    let mut buf = [0u8; 32];
    let n = data::encode(WorkerRequest::PurgeDb, &mut buf);

    socket.write_all(&buf[..n]).await?;

    Ok(())
}
