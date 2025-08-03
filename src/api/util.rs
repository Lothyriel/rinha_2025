use anyhow::Result;
use tokio::{io::AsyncWriteExt, net::UnixStream};

use crate::{WORKER_SOCKET, data, worker::WorkerRequest};

pub async fn purge() -> Result<()> {
    let mut socket = UnixStream::connect(&*WORKER_SOCKET).await?;

    let mut buf = [0u8; 32];
    let n = data::encode(WorkerRequest::PurgeDb, &mut buf);

    socket.write_all(&buf[..n]).await?;

    Ok(())
}
