use anyhow::Result;
use tokio::{io::AsyncWriteExt, net::UnixStream};

use crate::{api::summary::Summary, data, db};

pub async fn process(
    mut socket: UnixStream,
    pool: db::Pool,
    mut buf: [u8; 64],
    query: (i64, i64),
) -> Result<()> {
    tracing::debug!("handling get_summary");

    let summary = get(pool, query)?;

    let n = data::encode(summary, &mut buf);

    socket.write_all(&buf[..n]).await?;

    Ok(())
}

#[tracing::instrument(skip_all)]
fn get(pool: db::Pool, query: (i64, i64)) -> Result<Summary> {
    let payments = {
        let conn = pool.get()?;
        db::get_payments(&conn, query)?
    };

    let summary = payments.iter().fold([(0, 0), (0, 0)], |mut acc, p| {
        match p.processor_id {
            1 => inc(&mut acc[0], p.amount),
            2 => inc(&mut acc[1], p.amount),
            id => unreachable!("processor_id {{{id}}} should not exist"),
        };

        acc
    });

    Ok(Summary::new(summary))
}

fn inc(acc: &mut (u64, u64), amount: u64) {
    acc.0 += 1;
    acc.1 += amount;
}
