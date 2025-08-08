mod payment;
mod pp_client;
mod summary;

use std::time::Instant;

use anyhow::Result;
use metrics::Unit;
use once_cell::sync::Lazy;
use reqwest::Client;
use tokio::{io::AsyncReadExt, net::UnixStream};

use crate::{WORKER_SOCKET, api, bind_unix_socket, data, db};

// maybe tweak this value?
static HTTP_WORKERS: Lazy<u8> = Lazy::new(|| {
    std::env::var("HTTP_WORKERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2)
});

#[tokio::main(flavor = "current_thread")]
pub async fn serve() -> Result<()> {
    tracing::info!("starting worker");

    let store = db::Store::new();

    let req_tx = start_http_workers(store.clone());

    uds_listen(req_tx, store).await
}

fn start_http_workers(store: db::Store) -> Sender {
    let (tx, rx) = flume::unbounded();

    tracing::info!("starting payment_req_consumer");
    let client = Client::new();

    for _ in 0..*HTTP_WORKERS {
        let worker = start_http_worker(store.clone(), tx.clone(), rx.clone(), client.clone());
        tokio::spawn(async {
            if let Err(err) = worker.await {
                tracing::error!(?err, "http_worker_err")
            }
        });
    }

    tx
}

async fn start_http_worker(
    store: db::Store,
    tx: Sender,
    rx: Receiver,
    client: Client,
) -> Result<()> {
    loop {
        let req = rx.recv_async().await?;
        let result = payment::process(store.clone(), client.clone(), req.clone()).await;

        if let Err(err) = result {
            tracing::debug!(?err, "pp_client_err");
            tx.send_async(req).await?;
        }
    }
}

async fn uds_listen(tx: Sender, store: db::Store) -> Result<()> {
    let listener = bind_unix_socket(&WORKER_SOCKET)?;

    tracing::info!("listening on {}", *WORKER_SOCKET);

    loop {
        let tx = tx.clone();
        let store = store.clone();

        let (socket, _) = listener.accept().await?;
        tracing::debug!("accepted unix socket connection");

        tokio::spawn(async {
            if let Err(err) = handle_uds(tx, socket, store).await {
                tracing::error!(err = ?err, "handle_uds");
            }
        });
    }
}

async fn handle_uds(tx: Sender, mut socket: UnixStream, store: db::Store) -> Result<()> {
    let now = Instant::now();

    let mut buf = [0u8; 128];

    let n = socket.read(&mut buf).await?;

    let req = data::decode(&buf[..n]);

    match req {
        WorkerRequest::Summary(query) => summary::process(socket, store, query, &mut buf).await?,
        WorkerRequest::Payment(req) => {
            tracing::trace!("sending to req_channel");
            tx.send_async(req).await?;
        }
        WorkerRequest::PurgeDb => purge_db(store).await?,
    }

    metrics::describe_histogram!("uds.handle", Unit::Microseconds, "http handler time");
    metrics::histogram!("uds.handle").record(now.elapsed().as_micros() as f64);

    Ok(())
}

async fn purge_db(store: db::Store) -> Result<()> {
    store.purge().await;

    tracing::info!("db purged");

    Ok(())
}

#[derive(serde::Serialize, serde::Deserialize)]
pub enum WorkerRequest {
    Summary((i64, i64)),
    Payment(api::payment::Request),
    PurgeDb,
}

type Sender = flume::Sender<api::payment::Request>;
type Receiver = flume::Receiver<api::payment::Request>;
