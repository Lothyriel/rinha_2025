use anyhow::Result;
use metrics::Unit;
use once_cell::sync::Lazy;
use std::net::Ipv4Addr;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Instant;
use tokio_uring::buf::BoundedBuf;
use tokio_uring::buf::fixed::FixedBufRegistry;
use tokio_uring::net::UnixStream;
use tokio_uring::net::{TcpListener, TcpStream};

use crate::api;
use crate::data;

static BACKENDS: Lazy<Vec<String>> = Lazy::new(|| match data::get_api_n() {
    Some(n) => (0..n).map(data::get_api_socket_name).collect(),
    None => vec![api::API_SOCK.to_string()],
});

const OK_RES: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
const BUFFER_POOL_SIZE: usize = 1024;
const BUFFER_SIZE: usize = 512;

static CONN_COUNT: AtomicUsize = AtomicUsize::new(0);

pub fn serve() -> Result<()> {
    tokio_uring::start(async {
        if let Err(err) = start().await {
            tracing::error!(?err, "lb_main");
        }
    });

    Ok(())
}

async fn start() -> Result<()> {
    let port = 9999;

    tracing::info!("starting lb on port {port}");

    let listener = TcpListener::bind((Ipv4Addr::UNSPECIFIED, port).into())?;

    tracing::info!("api instances {:?}", *BACKENDS);

    let buffers = std::iter::repeat_with(|| Vec::with_capacity(BUFFER_SIZE)).take(BUFFER_POOL_SIZE);

    tracing::info!("allocating {BUFFER_POOL_SIZE} {BUFFER_SIZE}B buffers");

    let registry = FixedBufRegistry::new(buffers);

    registry.register()?;

    loop {
        let registry = registry.clone();
        let (client, _) = listener.accept().await?;
        client.set_nodelay(true)?;

        tokio_uring::spawn(async move {
            if let Err(err) = handle_connection(registry, client).await {
                tracing::error!(?err, "handle_conn");
            }
        });
    }
}

async fn handle_connection(registry: FixedBufRegistry<Vec<u8>>, tcp: TcpStream) -> Result<()> {
    let c = CONN_COUNT.fetch_add(1, Ordering::Relaxed);
    let backend = &BACKENDS[c % BACKENDS.len()];

    let unix = UnixStream::connect(backend).await?;

    let mut buffer = registry
        .check_out(c % BUFFER_POOL_SIZE)
        .ok_or_else(|| anyhow::anyhow!("buf {c} unavailable"))?;

    loop {
        let (r, buf) = tcp.read_fixed(buffer).await;
        let now = Instant::now();
        let n = r?;

        if n == 0 {
            return Ok(());
        }

        buffer = match buf[0] {
            // [G]ET /payments-summary
            b'G' => {
                let (r, buf) = unix.write_all(buf.slice(..n)).await;
                let buf = buf.into_inner();
                r?;

                let (r, buf) = unix.read(buf).await;
                let n = r?;

                let (r, buf) = tcp.write_fixed_all(buf.slice(..n)).await;
                r?;

                metrics::describe_histogram!("http.get", Unit::Microseconds, "http handler time");
                metrics::histogram!("http.get").record(now.elapsed().as_micros() as f64);

                buf.into_inner()
            }
            // [P]OST
            b'P' => {
                {
                    let (r, _) = tcp.write_all(OK_RES).await;
                    r?;
                }

                metrics::describe_histogram!("http.post", Unit::Microseconds, "http handler time");
                metrics::histogram!("http.post").record(now.elapsed().as_micros() as f64);

                let (r, buf) = unix.write_all(buf.slice(..n)).await;
                r?;

                buf.into_inner()
            }
            _ => {
                tracing::warn!("Invalid request {:?}", std::str::from_utf8(&buf[..n]));
                buf
            }
        }
    }
}
