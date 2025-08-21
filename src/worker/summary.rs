use std::io::{Cursor, Write};

use anyhow::Result;
use tokio::net::UnixStream;

use crate::{api::summary::Summary, data, db};

pub async fn process(
    socket: &mut UnixStream,
    store: &db::Store,
    query: (i64, i64),
    buf: &mut [u8],
) -> Result<()> {
    tracing::trace!("handling get_summary");

    let summary = store.get(query).await;

    let n = build_payload(buf, summary)?;

    data::send_bytes(buf, n, socket).await
}

fn build_payload(buf: &mut [u8], Summary { default, fallback }: Summary) -> Result<usize> {
    let mut writer = Cursor::new(buf);

    write!(
        writer,
        r#"{{"default":{{"totalRequests":{},"totalAmount":{}}},"fallback":{{"totalRequests":{},"totalAmount":{}}}}}"#,
        default.count, default.amount, fallback.count, fallback.amount
    )?;

    Ok(writer.position() as usize)
}
