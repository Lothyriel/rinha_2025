mod payment;
mod pp_client;
mod summary;

use anyhow::Result;
use reqwest::Client;
use tokio::{io::AsyncReadExt, net::UnixStream};

use crate::{WORKER_SOCKET, api, bind_unix_socket, data, db};

#[tracing::instrument(skip_all)]
pub async fn serve() -> Result<()> {
    tracing::info!("Starting worker");

    db::init_db()?;

    let db_tx = start_db_consumer();

    _ = start_http_workers(db_tx);

    uds_listen().await
}

fn start_db_consumer() -> PaymentTx {
    let (tx, rx) = crossbeam::channel::unbounded();

    tracing::info!("Starting db_consumer");

    tokio::spawn(async move {
        if let Err(err) = handle_completed_payments(rx).await {
            tracing::error!(?err, "db_consumer_err");
        }
    });

    tx
}

fn start_http_workers(payment_tx: PaymentTx) -> RequestTx {
    let (tx, rx) = crossbeam::channel::unbounded();

    tracing::info!("Starting payment_req_consumer");
    let client = Client::new();

    // maybe tweak this value?
    const HTTP_WORKERS: usize = 8;

    for _ in 0..HTTP_WORKERS {
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

        let req = rx.recv()?;
        let result = payment::process(payment_tx, client, req.clone()).await;

        if let Err(err) = result {
            tracing::debug!(?err, "pp_client_err");
            tx.send(req)?;
        }
    }
}

async fn uds_listen() -> Result<()> {
    let listener = bind_unix_socket(&WORKER_SOCKET)?;

    tracing::info!("listening on {}", &*WORKER_SOCKET);

    loop {
        let (socket, _) = listener.accept().await?;
        tracing::debug!("accepted unix socket connection");

        tokio::spawn(async {
            if let Err(err) = handle_uds(/*tx,*/ socket).await {
                tracing::error!(err = ?err, "handle_uds");
            }
        });
    }
}

#[tracing::instrument(skip_all)]
async fn handle_uds(mut socket: UnixStream) -> Result<()> {
    let mut buf = [0u8; 64];

    let n = socket.read(&mut buf).await?;

    let req = data::decode(&buf[..n]);

    match req {
        WorkerRequest::Summary(query) => summary::process(socket, buf, query).await?,
        WorkerRequest::Payment(_) => {
            tracing::debug!("sending to req_channel");
        }
        WorkerRequest::PurgeDb => purge_db()?,
    }

    Ok(())
}

#[tracing::instrument(skip_all)]
async fn handle_completed_payments(rx: PaymentRx) -> Result<()> {
    const BATCH_SIZE: usize = 100;
    let mut buffer = Vec::with_capacity(BATCH_SIZE);

    loop {
        let payment = rx.recv()?;

        tracing::debug!("completed_payments_recv");

        if buffer.len() < BATCH_SIZE {
            buffer.push(payment);
            continue;
        }

        tracing::info!("db_write_flush");

        db::insert_payment(&buffer)?;

        buffer.clear();
    }
}

fn purge_db() -> Result<()> {
    db::purge()?;

    tracing::info!("DB purged");

    Ok(())
}

#[derive(serde::Serialize, serde::Deserialize)]
pub enum WorkerRequest {
    Summary((i64, i64)),
    Payment(api::payment::Request),
    PurgeDb,
}

type PaymentTx = crossbeam::channel::Sender<data::Payment>;
type PaymentRx = crossbeam::channel::Receiver<data::Payment>;

type RequestTx = crossbeam::channel::Sender<api::payment::Request>;
type RequestRx = crossbeam::channel::Receiver<api::payment::Request>;
