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
const BUFFER_POOL_SIZE: usize = 10_000;

static CONN_COUNT: AtomicUsize = AtomicUsize::new(0);
static REQ_COUNT: AtomicUsize = AtomicUsize::new(0);

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

    let registry = FixedBufRegistry::new(std::iter::repeat_n(vec![0; 512], BUFFER_POOL_SIZE));

    registry.register()?;

    loop {
        let registry = registry.clone();
        let (client, _) = listener.accept().await?;

        tokio_uring::spawn(async move {
            if let Err(err) = handle_connection(registry, client).await {
                tracing::error!(?err, "handle_conn");
            }
        });
    }
}

async fn handle_connection(registry: FixedBufRegistry<Vec<u8>>, tcp: TcpStream) -> Result<()> {
    let backend = &BACKENDS[CONN_COUNT.fetch_add(1, Ordering::Relaxed) % BACKENDS.len()];

    let unix = UnixStream::connect(backend).await?;

    loop {
        let buf = registry
            .check_out(REQ_COUNT.fetch_add(1, Ordering::AcqRel) % BUFFER_POOL_SIZE)
            .ok_or_else(|| anyhow::anyhow!("buf unavailable"))?;

        let (r, buf) = tcp.read_fixed(buf).await;
        let now = Instant::now();
        let n = r?;

        if n == 0 {
            return Ok(());
        }

        match buf[0] {
            // [G]ET /payments-summary
            b'G' => {
                let (r, buf) = unix.write_all(buf.slice(..n)).await;
                let buf = buf.into_inner();
                r?;

                let (r, buf) = unix.read(buf).await;
                let n = r?;

                let (r, _) = tcp.write_fixed_all(buf.slice(..n)).await;
                r?;

                metrics::describe_histogram!("http.get", Unit::Microseconds, "http handler time");
                metrics::histogram!("http.get").record(now.elapsed().as_micros() as f64);
            }
            // [P]OST
            b'P' => {
                {
                    let (r, _) = tcp.write_all(OK_RES).await;
                    r?;
                }

                metrics::describe_histogram!("http.post", Unit::Microseconds, "http handler time");
                metrics::histogram!("http.post").record(now.elapsed().as_micros() as f64);

                let (r, _) = unix.write_all(buf.slice(..n)).await;
                r?;
            }
            _ => {
                tracing::warn!("Invalid request {:?}", std::str::from_utf8(&buf));
            }
        }
    }
}
