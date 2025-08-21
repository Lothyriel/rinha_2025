pub mod payment;
pub mod summary;

use std::{io::IoSlice, time::Instant};

use anyhow::Result;
use metrics::Unit;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
};

use crate::{bind_unix_socket, data, get_worker_socket, worker::WorkerRequest};

#[tokio::main(flavor = "current_thread")]
pub async fn serve() -> Result<()> {
    tracing::info!("starting API");

    let socket = std::env::var("API_N")
        .map(|n| format!("/var/run/api{n}.sock"))
        .unwrap_or("./api.sock".to_string());

    let listener = bind_unix_socket(&socket)?;
    tracing::info!("binded to unix socket on {socket}");

    loop {
        let (socket, _) = listener.accept().await?;

        let counter = metrics::counter!("http.conn");
        counter.increment(1);

        tokio::spawn(async {
            if let Err(err) = handle_http(socket).await {
                tracing::error!(?err, "http_err");
            }
        });
    }
}

async fn handle_http(mut client: UnixStream) -> Result<()> {
    let mut buf = [0u8; 512];

    let socket = get_worker_socket();
    let mut worker = UnixStream::connect(socket).await?;

    loop {
        let n = client.read(&mut buf).await?;
        let now = Instant::now();

        if n == 0 {
            return Ok(());
        }

        match (buf[0], buf[7]) {
            // [G]ET /payments-summary
            (b'G', _) => {
                let n = summary::get_summary(&mut worker, &mut buf).await?;
                let body_len = n.to_string();

                let res = &[
                    IoSlice::new(b"HTTP/1.1 200 OK\r\nContent-Length: "),
                    IoSlice::new(body_len.as_bytes()),
                    IoSlice::new(b"\r\n\r\n"),
                    IoSlice::new(&buf[..n]),
                ];

                _ = client.write_vectored(res).await?;

                metrics::describe_histogram!("http.get", Unit::Microseconds, "http handler time");
                metrics::histogram!("http.get").record(now.elapsed().as_micros() as f64);
            }
            // POST /p[a]yments
            (b'P', b'a') => {
                send_ok(&mut client).await?;

                metrics::describe_histogram!("http.post", Unit::Microseconds, "http handler time");

                metrics::histogram!("http.post").record(now.elapsed().as_micros() as f64);

                handle_payment(&mut worker, &mut buf).await?
            }
            // POST /p[u]rge-payments
            (b'P', b'u') => {
                data::send(WorkerRequest::PurgeDb, &mut buf, &mut worker).await?;
                send_ok(&mut client).await?;
            }
            _ => {
                tracing::warn!("Invalid request {:?}", std::str::from_utf8(&buf));
            }
        }
    }
}

async fn send_ok(socket: &mut UnixStream) -> Result<()> {
    socket
        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
        .await?;

    Ok(())
}

async fn handle_payment(socket: &mut UnixStream, buf: &mut [u8]) -> Result<(), anyhow::Error> {
    let start = buf
        .iter()
        .position(|&b| b == b'{')
        .expect("find json start");

    let end = buf.iter().rposition(|&b| b == b'}').expect("find json end");

    payment::send(socket, &mut buf[start..end + 1]).await
}
