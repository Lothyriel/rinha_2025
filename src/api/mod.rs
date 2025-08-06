pub mod payment;
pub mod summary;
mod util;

use std::io::IoSlice;

use anyhow::Result;
use chrono::DateTime;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
};

use crate::{bind_unix_socket, data};

pub const API_SOCK: &str = "./api.sock";

#[tokio::main]
pub async fn serve() -> Result<()> {
    tracing::info!("starting API");

    let socket = data::get_api_n()
        .map(data::get_api_socket_name)
        .unwrap_or(API_SOCK.to_string());

    let listener = bind_unix_socket(&socket)?;
    tracing::info!("binded to unix socket on {socket}");

    loop {
        let (socket, _) = listener.accept().await?;

        tokio::spawn(async {
            if let Err(err) = handle_http(socket).await {
                tracing::error!(?err, "http_err");
            }
        });
    }
}

async fn handle_http(mut socket: UnixStream) -> Result<()> {
    let mut buf = [0u8; 512];

    loop {
        let n = socket.read(&mut buf).await?;

        if n == 0 {
            return Ok(());
        }

        match buf[0] {
            // [G]ET /payments-summary
            b'G' => {
                let n = get_summary(&mut buf).await?;
                let body_len = n.to_string();

                let res = &[
                    IoSlice::new(b"HTTP/1.1 200 OK\r\nContent-Length: "),
                    IoSlice::new(body_len.as_bytes()),
                    IoSlice::new(b"\r\n\r\n"),
                    IoSlice::new(&buf[..n]),
                ];

                _ = socket.write_vectored(res).await?;
            }
            // [P]OST
            b'P' => match buf[7] {
                // POST /p[a]yments
                b'a' => handle_payment(&mut buf).await?,
                // POST /p[u]rge-payments
                b'u' => {
                    util::purge().await?;
                }
                _ => {
                    tracing::warn!("Invalid request {:?}", std::str::from_utf8(&buf));
                }
            },
            _ => {
                tracing::warn!("Invalid request {:?}", std::str::from_utf8(&buf));
            }
        }
    }
}

async fn handle_payment(buf: &mut [u8]) -> Result<(), anyhow::Error> {
    let start = buf
        .iter()
        .position(|&b| b == b'{')
        .expect("find json start");

    let end = buf.iter().rposition(|&b| b == b'}').expect("find json end");

    let req = &buf[start..end + 1];
    let req = serde_json::from_slice(req)?;

    payment::send(buf, req).await?;

    Ok(())
}

const DISTANT_FUTURE: DateTime<chrono::Utc> = DateTime::from_timestamp_nanos(i64::MAX);

async fn get_summary(buf: &mut [u8]) -> Result<usize> {
    const RFC_3339_SIZE: usize = 24;
    const TO_OFFSET: usize = 27 + RFC_3339_SIZE + 2 + 2;

    let from = &buf[27..27 + RFC_3339_SIZE];
    let to = &buf[TO_OFFSET..TO_OFFSET + RFC_3339_SIZE];

    let from = std::str::from_utf8(from)?;
    let to = std::str::from_utf8(to)?;

    let from = DateTime::parse_from_rfc3339(from).unwrap_or_default();
    let to = DateTime::parse_from_rfc3339(to).unwrap_or(DISTANT_FUTURE.into());

    let query = (from.timestamp_micros(), to.timestamp_micros());

    summary::get_summary(query, buf).await
}
