use anyhow::Result;
use tokio::{io::AsyncWriteExt, net::UnixStream};

use crate::{data, worker::WorkerRequest};

pub async fn purge(socket: &mut UnixStream, buf: &mut [u8]) -> Result<()> {
    let n = data::encode(WorkerRequest::PurgeDb, buf);

    socket.write_all(&buf[..n]).await?;

    Ok(())
}
