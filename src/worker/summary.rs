use anyhow::Result;
use tokio::{io::AsyncWriteExt, net::UnixStream};

use crate::{data, db};

pub async fn process(
    mut socket: UnixStream,
    store: db::Store,
    mut buf: [u8; 64],
    query: (i64, i64),
) -> Result<()> {
    tracing::debug!("handling get_summary");

    let summary = store.get(query);

    let n = data::encode(summary, &mut buf);

    socket.write_all(&buf[..n]).await?;

    Ok(())
}
