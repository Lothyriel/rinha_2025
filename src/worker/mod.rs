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

pub async fn serve() -> Result<()> {
    tracing::info!("starting worker");

    let store = db::Store::new();

    let db_tx = start_db_consumer(store.clone());

    let req_tx = start_http_workers(db_tx);

    uds_listen(req_tx, store).await
}

fn start_db_consumer(store: db::Store) -> PaymentTx {
    let (tx, rx) = flume::unbounded();

    tracing::info!("starting db_consumer");

    tokio::spawn(async move {
        if let Err(err) = handle_completed_payments(store, rx).await {
            tracing::error!(?err, "db_consumer_err");
        }
    });

    tx
}

fn start_http_workers(payment_tx: PaymentTx) -> RequestTx {
    let (tx, rx) = flume::unbounded();

    tracing::info!("starting payment_req_consumer");
    let client = Client::new();

    for _ in 0..*HTTP_WORKERS {
        let worker = start_http_worker(payment_tx.clone(), tx.clone(), rx.clone(), client.clone());
        tokio::spawn(async {
            if let Err(err) = worker.await {
                tracing::error!(?err, "http_worker_err")
            }
        });
    }

    tx
}

async fn start_http_worker(
    payment_tx: PaymentTx,
    tx: RequestTx,
    rx: RequestRx,
    client: Client,
) -> Result<()> {
    loop {
        let client = client.clone();
        let payment_tx = payment_tx.clone();

        let req = rx.recv_async().await?;
        let result = payment::process(payment_tx, client, req.clone()).await;

        if let Err(err) = result {
            tracing::debug!(?err, "pp_client_err");
            tx.send_async(req).await?;
        }
    }
}

async fn uds_listen(tx: RequestTx, store: db::Store) -> Result<()> {
    let listener = bind_unix_socket(&WORKER_SOCKET)?;

    tracing::info!("listening on {}", &*WORKER_SOCKET);

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

async fn handle_uds(tx: RequestTx, mut socket: UnixStream, store: db::Store) -> Result<()> {
    let now = Instant::now();

    let mut buf = [0u8; 128];

    let n = socket.read(&mut buf).await?;

    let req = data::decode(&buf[..n]);

    match req {
        WorkerRequest::Summary(query) => summary::process(socket, store, query, &mut buf).await?,
        WorkerRequest::Payment(req) => {
            tracing::debug!("sending to req_channel");
            tx.send_async(req).await?;
        }
        WorkerRequest::PurgeDb => purge_db(store)?,
    }

    metrics::describe_histogram!("uds.handle", Unit::Microseconds, "http handler time");
    metrics::histogram!("uds.handle").record(now.elapsed().as_micros() as f64);

    Ok(())
}

async fn handle_completed_payments(writer: db::Store, rx: PaymentRx) -> Result<()> {
    loop {
        let payment = rx.recv_async().await?;

        tracing::debug!("completed_payments_recv");

        writer.insert(payment);
    }
}

fn purge_db(store: db::Store) -> Result<()> {
    store.purge();

    tracing::info!("db purged");

    Ok(())
}

#[derive(serde::Serialize, serde::Deserialize)]
pub enum WorkerRequest {
    Summary((i64, i64)),
    Payment(api::payment::Request),
    PurgeDb,
}

type PaymentTx = flume::Sender<data::Payment>;
type PaymentRx = flume::Receiver<data::Payment>;

type RequestTx = flume::Sender<api::payment::Request>;
type RequestRx = flume::Receiver<api::payment::Request>;
