use anyhow::Result;
use std::io::{Cursor, Write};
use tokio::{io::AsyncWriteExt, net::UnixStream};

use crate::db;

pub async fn process(
    mut socket: UnixStream,
    store: db::Store,
    query: (i64, i64),
    buf: &mut [u8],
) -> Result<()> {
    tracing::debug!("handling get_summary");

    let summary = store.get(query).await;

    let n = build_payload(buf, summary.default.count, summary.default.amount)?;

    socket.write_all(&buf[..n]).await?;

    Ok(())
}

fn build_payload(buf: &mut [u8], count: u64, amount: f32) -> Result<usize> {
    let mut writer = Cursor::new(buf);

    write!(
        writer,
        r#"{{"default":{{"totalRequests":{count},"totalAmount":{amount}}},"fallback":{{"totalRequests":0,"totalAmount":0.0}}}}"#,
    )?;

    Ok(writer.position() as usize)
}
