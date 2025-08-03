pub mod payment;
pub mod summary;
mod util;

use std::{net::Ipv4Addr, time::Instant};

use anyhow::Result;
use chrono::DateTime;
use metrics::Unit;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpListener,
};

use crate::{api::summary::Summary, bind_unix_socket};

pub async fn serve() -> Result<()> {
    tracing::info!("starting API");

    match std::env::var("NGINX_SOCKET").ok() {
        Some(f) => {
            let listener = bind_unix_socket(&f)?;
            tracing::info!("binded to unix socket on {f}");

            loop {
                let (socket, _) = listener.accept().await?;
                tokio::spawn(async {
                    if let Err(err) = handle_http(socket).await {
                        tracing::error!(?err, "http_err");
                    }
                });
            }
        }
        None => {
            const PORT: u16 = 9999;
            let listener = TcpListener::bind((Ipv4Addr::UNSPECIFIED, PORT)).await?;
            tracing::info!("binding to network socket on {PORT}");

            loop {
                let (socket, _) = listener.accept().await?;
                tokio::spawn(async {
                    if let Err(err) = handle_http(socket).await {
                        tracing::error!(?err, "http_err");
                    }
                });
            }
        }
    };
}

const EMPTY_RES: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";

async fn handle_http<T: AsyncRead + AsyncWrite + Unpin>(mut socket: T) -> Result<()> {
    let mut buf = [0; 512];

    let n = socket.read(&mut buf).await?;

    if n == 0 {
        return Ok(());
    }

    loop {
        match buf[0] {
            // [G]ET /payments-summary
            b'G' => {
                let now = Instant::now();

                let summary = get_summary(buf).await?;

                let body = serde_json::to_string(&summary)?;
                let res = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}",
                    body.len(),
                    body
                );

                socket.write_all(res.as_bytes()).await?;

                metrics::describe_histogram!("http.get", Unit::Microseconds, "http handler time");
                metrics::histogram!("http.get").record(now.elapsed().as_micros() as f64);
            }
            // [P]OST
            b'P' => match buf[7] {
                // POST /p[a]yments
                b'a' => {
                    let now = Instant::now();

                    socket.write_all(EMPTY_RES).await?;

                    metrics::describe_histogram!(
                        "http.post",
                        Unit::Microseconds,
                        "http handler time"
                    );
                    metrics::histogram!("http.post").record(now.elapsed().as_micros() as f64);

                    handle_payment(buf).await?;
                }
                // POST /p[u]rge-payments
                b'u' => {
                    socket.write_all(EMPTY_RES).await?;

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

async fn handle_payment(buf: [u8; 512]) -> Result<(), anyhow::Error> {
    let start = buf
        .iter()
        .position(|&b| b == b'{')
        .expect("find json start");
    let end = buf.iter().rposition(|&b| b == b'}').expect("find json end");
    let req = &buf[start..end + 1];
    let req = serde_json::from_slice(req)?;
    payment::send(req).await?;
    Ok(())
}

const DISTANT_FUTURE: DateTime<chrono::Utc> = DateTime::from_timestamp_nanos(i64::MAX);

async fn get_summary(buf: [u8; 512]) -> Result<Summary, anyhow::Error> {
    const RFC_3339_SIZE: usize = 24;
    const TO_OFFSET: usize = 27 + RFC_3339_SIZE + 2 + 2;

    let from = &buf[27..27 + RFC_3339_SIZE];
    let to = &buf[TO_OFFSET..TO_OFFSET + RFC_3339_SIZE];

    let from = std::str::from_utf8(from)?;
    let to = std::str::from_utf8(to)?;

    let from = DateTime::parse_from_rfc3339(from).unwrap_or_default();
    let to = DateTime::parse_from_rfc3339(to).unwrap_or(DISTANT_FUTURE.into());

    let query = (from.timestamp_micros(), to.timestamp_micros());

    summary::get_summary(query).await
}
