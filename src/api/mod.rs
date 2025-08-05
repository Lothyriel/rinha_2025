pub mod payment;
pub mod summary;
mod util;

use std::{io::IoSlice, net::Ipv4Addr, time::Instant};

use anyhow::Result;
use chrono::DateTime;
use metrics::Unit;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpListener,
};

use crate::bind_unix_socket;

pub async fn serve() -> Result<()> {
    tracing::info!("starting API");

    let (tx, rx) = flume::unbounded();

    tokio::spawn(async move {
        if let Err(err) = consumer(rx).await {
            tracing::error!(?err, "http_consumer");
        }
    });

    match std::env::var("NGINX_SOCKET").ok() {
        Some(f) => {
            let listener = bind_unix_socket(&f)?;
            tracing::info!("binded to unix socket on {f}");

            loop {
                let tx = tx.clone();
                let (socket, _) = listener.accept().await?;

                tokio::spawn(async {
                    if let Err(err) = handle_http(socket, tx).await {
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
                let tx = tx.clone();
                let (socket, _) = listener.accept().await?;

                tokio::spawn(async {
                    if let Err(err) = handle_http(socket, tx).await {
                        tracing::error!(?err, "http_err");
                    }
                });
            }
        }
    };
}

async fn consumer(rx: flume::Receiver<[u8; 512]>) -> Result<()> {
    loop {
        let mut buf = rx.recv_async().await?;

        handle_payment(&mut buf).await?
    }
}

const OK_RES: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";

async fn handle_http<T: AsyncRead + AsyncWrite + Unpin>(
    mut socket: T,
    tx: flume::Sender<[u8; 512]>,
) -> Result<()> {
    let mut buf = [0u8; 512];

    loop {
        let n = socket.read(&mut buf).await?;

        if n == 0 {
            socket.shutdown().await?;
            return Ok(());
        }

        match buf[0] {
            // [G]ET /payments-summary
            b'G' => {
                let now = Instant::now();

                let n = get_summary(&mut buf).await?;
                let body_len = n.to_string();

                let res = &[
                    IoSlice::new(b"HTTP/1.1 200 OK\r\nContent-Length: "),
                    IoSlice::new(body_len.as_bytes()),
                    IoSlice::new(b"\r\n\r\n"),
                    IoSlice::new(&buf[..n]),
                ];

                _ = socket.write_vectored(res).await?;

                metrics::describe_histogram!("http.get", Unit::Microseconds, "http handler time");
                metrics::histogram!("http.get").record(now.elapsed().as_micros() as f64);
            }
            // [P]OST
            b'P' => match buf[7] {
                // POST /p[a]yments
                b'a' => {
                    let now = Instant::now();

                    socket.write_all(OK_RES).await?;

                    metrics::describe_histogram!(
                        "http.post",
                        Unit::Microseconds,
                        "http handler time"
                    );
                    metrics::histogram!("http.post").record(now.elapsed().as_micros() as f64);

                    tx.send_async(buf).await?;
                }
                // POST /p[u]rge-payments
                b'u' => {
                    socket.write_all(OK_RES).await?;

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
