mod pp_client;
mod summary;

use std::sync::Arc;

use anyhow::Result;
use reqwest::Client;
use tokio::net::UnixStream;

use crate::{
    api, bind_unix_socket, data, db, get_worker_socket, worker::pp_client::PaymentsManager,
};

#[tokio::main(flavor = "current_thread")]
pub async fn serve() -> Result<()> {
    tracing::info!("starting worker");

    let store = db::Store::new();

    let req_tx = start_http_workers(store.clone());

    uds_listen(req_tx, store).await
}

fn start_http_workers(store: db::Store) -> Sender {
    let (tx, rx) = flume::unbounded();

    let http_workers = std::env::var("HTTP_WORKERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(16);

    let default = std::env::var("PROCESSOR_DEFAULT")
        .unwrap_or("http://payment-processor-default:8080".to_string());

    let fallback = std::env::var("PROCESSOR_FALLBACK")
        .unwrap_or("http://payment-processor-fallback:8080".to_string());

    let micros_cutout = std::env::var("PROCESSOR_CUTOUT")
        .ok()
        .and_then(|c| c.parse().ok())
        .unwrap_or(100_000); //100ms

    let reset_timeout = std::env::var("RESET_TIMEOUT")
        .ok()
        .and_then(|c| c.parse().ok())
        .unwrap_or(6);

    let manager = PaymentsManager::new(&default, &fallback, micros_cutout, store, &Client::new());

    manager.start(reset_timeout);

    tracing::info!("starting {http_workers} http_workers");
    for _ in 0..http_workers {
        let worker = start_http_worker(manager.clone(), tx.clone(), rx.clone());
        tokio::spawn(async {
            if let Err(err) = worker.await {
                tracing::error!(?err, "http_worker_err")
            }
        });
    }

    tx
}

async fn start_http_worker(manager: Arc<PaymentsManager>, tx: Sender, rx: Receiver) -> Result<()> {
    loop {
        let req = rx.recv_async().await?;

        let result = manager.send(req.clone()).await;

        if let Err(err) = result {
            tracing::debug!(?err, "pp_client_err");
            tx.send_async(req).await?;
        }
    }
}

async fn uds_listen(tx: Sender, store: db::Store) -> Result<()> {
    let socket = get_worker_socket();
    let listener = bind_unix_socket(&socket)?;

    tracing::info!("listening on {}", &socket);

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

async fn handle_uds(tx: Sender, stream: UnixStream, store: db::Store) -> Result<()> {
    let mut stream = data::FramedStream::new(stream);

    loop {
        let n = stream.read().await?;

        if n == 0 {
            return Ok(());
        }

        while let Some(req) = stream.next()? {
            match req {
                WorkerRequest::Summary(query) => {
                    let (socket, buf) = stream.inner();
                    summary::process(socket, &store, query, buf).await?
                }
                WorkerRequest::Payment(req) => {
                    tracing::trace!("sending to req_channel");
                    tx.send_async(req).await?;
                }
                WorkerRequest::PurgeDb => purge_db(&store).await?,
            }
        }
    }
}

async fn purge_db(store: &db::Store) -> Result<()> {
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
