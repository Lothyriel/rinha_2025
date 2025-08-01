mod payment;
mod pp_client;
mod summary;

use anyhow::Result;
use reqwest::Client;
use tokio::{io::AsyncReadExt, net::UnixStream};

use crate::{WORKER_SOCKET, api, bind_unix_socket, data, db};

#[tracing::instrument(skip_all)]
pub async fn serve() -> Result<()> {
    tracing::info!("starting worker");

    let writer = {
        let pool = db::write_pool()?;
        let conn = pool.get()?;
        db::init_db(&conn)?;
        pool
    };

    let db_tx = start_db_consumer(writer);

    let req_tx = start_http_workers(db_tx);

    uds_listen(req_tx).await
}

fn start_db_consumer(pool: db::Pool) -> PaymentTx {
    let (tx, rx) = flume::unbounded();

    tracing::info!("starting db_consumer");

    tokio::spawn(async move {
        if let Err(err) = handle_completed_payments(pool, rx).await {
            tracing::error!(?err, "db_consumer_err");
        }
    });

    tx
}

fn start_http_workers(payment_tx: PaymentTx) -> RequestTx {
    let (tx, rx) = flume::unbounded();

    tracing::info!("starting payment_req_consumer");
    let client = Client::new();

    // maybe tweak this value?
    const HTTP_WORKERS: usize = 4;

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

        let req = rx.recv_async().await?;
        let result = payment::process(payment_tx, client, req.clone()).await;

        if let Err(err) = result {
            tracing::debug!(?err, "pp_client_err");
            tx.send_async(req).await?;
        }
    }
}

async fn uds_listen(tx: RequestTx) -> Result<()> {
    let listener = bind_unix_socket(&WORKER_SOCKET)?;
    let reader = db::read_pool()?;

    tracing::info!("listening on {}", &*WORKER_SOCKET);

    loop {
        let reader = reader.clone();
        let tx = tx.clone();
        let (socket, _) = listener.accept().await?;
        tracing::debug!("accepted unix socket connection");

        tokio::spawn(async {
            if let Err(err) = handle_uds(tx, socket, reader).await {
                tracing::error!(err = ?err, "handle_uds");
            }
        });
    }
}

#[tracing::instrument(skip_all)]
async fn handle_uds(tx: RequestTx, mut socket: UnixStream, pool: db::Pool) -> Result<()> {
    let mut buf = [0u8; 64];

    let n = socket.read(&mut buf).await?;

    let req = data::decode(&buf[..n]);

    match req {
        WorkerRequest::Summary(query) => summary::process(socket, pool, buf, query).await?,
        WorkerRequest::Payment(req) => {
            tracing::debug!("sending to req_channel");
            tx.send_async(req).await?;
        }
        WorkerRequest::PurgeDb => purge_db()?,
    }

    Ok(())
}

#[tracing::instrument(skip_all)]
async fn handle_completed_payments(writer: db::Pool, rx: PaymentRx) -> Result<()> {
    let conn = writer.get()?;

    loop {
        let payment = rx.recv_async().await?;

        tracing::debug!("completed_payments_recv");

        db::insert_payment(&conn, &payment)?;
    }
}

fn purge_db() -> Result<()> {
    let writer = db::write_pool()?;

    let conn = writer.get()?;

    db::purge(&conn)?;

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
