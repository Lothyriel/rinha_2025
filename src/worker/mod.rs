mod payment;
mod pp_client;
mod summary;

use anyhow::Result;
use reqwest::Client;
use tokio::{
    io::AsyncReadExt,
    net::{UnixListener, UnixStream},
};

use crate::{api, data, db};

#[tracing::instrument(skip_all)]
pub async fn serve() -> Result<()> {
    tracing::info!("Starting worker");

    db::init_db()?;

    let db_tx = start_db_consumer();

    let req_tx = start_payments_request_consumer(db_tx);

    uds_listen(req_tx).await
}

fn start_db_consumer() -> PaymentTx {
    let (tx, rx) = crossbeam::channel::unbounded();

    tracing::info!("Starting crossbeam consumer");

    tokio::spawn(async move {
        if let Err(err) = handle_completed_payment(rx).await {
            tracing::error!(?err, "crossbeam_err");
        }
    });

    tx
}

fn start_payments_request_consumer(payment_tx: PaymentTx) -> RequestTx {
    let (tx, rx): (RequestTx, _) = crossbeam::channel::unbounded();

    tracing::info!("Starting payments_request consumer");
    let client = Client::new();

    const HTTP_WORKERS: usize = 8;

    // maybe tweak this value?
    for _ in 0..HTTP_WORKERS {
        let client = client.clone();
        let payment_tx = payment_tx.clone();
        let rx = rx.clone();
        let tx = tx.clone();

        tokio::spawn(async move {
            match rx.recv() {
                Ok(req) => {
                    let result = payment::process(payment_tx, client, req.clone()).await;

                    if result.is_err() {
                        tx.send(req).expect("requeue request");
                    }
                }
                Err(err) => tracing::error!(?err, "crossbeam_err"),
            }
        });
    }

    tx
}

#[tracing::instrument(skip_all)]
async fn handle_completed_payment(rx: PaymentRx) -> Result<()> {
    const BATCH_SIZE: usize = 100;
    let mut buffer = Vec::with_capacity(BATCH_SIZE);

    loop {
        let payment = rx.recv()?;

        if buffer.len() < BATCH_SIZE {
            buffer.push(payment);
            continue;
        }

        tracing::info!("crossbeam_recv");

        db::insert_payment(&buffer)?;

        buffer.clear();
    }
}

pub const UDS_PATH: &str = "/var/run/rinha.sock";

async fn uds_listen(tx: RequestTx) -> Result<()> {
    std::fs::remove_file(UDS_PATH).ok();
    let listener = UnixListener::bind(UDS_PATH)?;
    tracing::info!("listening on {}", UDS_PATH);

    loop {
        let (socket, _) = listener.accept().await?;
        let tx = tx.clone();

        tokio::spawn(async {
            if let Err(err) = handle_uds(tx, socket).await {
                tracing::error!(err = ?err, "handle_uds");
            }
        });
    }
}

#[tracing::instrument(skip_all)]
async fn handle_uds(tx: RequestTx, mut socket: UnixStream) -> Result<()> {
    let mut buf = [0u8; 64];

    let n = socket.read(&mut buf).await?;

    match data::decode(&buf[..n]) {
        WorkerRequest::Summary(query) => summary::process(socket, buf, query).await?,
        WorkerRequest::Payment(req) => tx.send(req)?,
        WorkerRequest::PurgeDb => purge_db()?,
    }

    Ok(())
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
