use anyhow::Result;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
};

use crate::{WORKER_SOCKET, data, worker::WorkerRequest};

pub async fn get_summary(query: (i64, i64), buf: &mut [u8]) -> Result<usize> {
    let mut socket = UnixStream::connect(&*WORKER_SOCKET).await?;
    tracing::debug!("connected to unix socket on {}", *WORKER_SOCKET);

    let n = data::encode(WorkerRequest::Summary(query), buf);
    tracing::debug!("writing to {}", *WORKER_SOCKET);
    socket.write_all(&buf[..n]).await?;

    tracing::debug!("reading from {}", *WORKER_SOCKET);

    let n = socket.read(buf).await?;

    Ok(n)
}

pub struct Summary {
    pub default: ProcessedData,
    pub fallback: ProcessedData,
}

impl Summary {
    pub fn new(summary: [(u64, u64); 2]) -> Self {
        Summary {
            default: ProcessedData::new(summary[0]),
            fallback: ProcessedData::new(summary[1]),
        }
    }
}

pub struct ProcessedData {
    pub count: u64,
    pub amount: f32,
}

impl ProcessedData {
    pub fn new((requests, amount): (u64, u64)) -> Self {
        ProcessedData {
            count: requests,
            amount: amount as f32 / 100.0,
        }
    }
}
