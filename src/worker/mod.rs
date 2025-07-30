mod payment;
mod pp_client;
mod summary;

use anyhow::Result;
use reqwest::Client;
use tokio::{
    io::AsyncReadExt,
    net::{UnixListener, UnixStream},
    sync::mpsc,
};

use crate::{api, data, db};

#[tracing::instrument(skip_all)]
pub async fn serve() -> Result<()> {
    tracing::info!("Starting worker");

    db::init_db()?;

    let sender = start_consumer();

    uds_listen(sender).await
}

fn start_consumer() -> Sender {
    let (tx, mut rx): (Sender, Receiver) = mpsc::unbounded_channel();

    tracing::info!("Starting mpsc consumer");

    const BATCH_SIZE: usize = 100;

    let mut buffer = Vec::with_capacity(BATCH_SIZE);

    tokio::spawn(async move {
        loop {
            rx.recv_many(&mut buffer, BATCH_SIZE).await;

            let result = handle_mpsc(&buffer).await;

            buffer.clear();
            if let Err(er) = result {
                tracing::error!(?er, "mpsc_err");
            }
        }
    });

    tx
}

pub const UDS_PATH: &str = "/var/run/rinha.sock";

async fn uds_listen(sender: Sender) -> Result<()> {
    std::fs::remove_file(UDS_PATH).ok();
    let listener = UnixListener::bind(UDS_PATH)?;
    tracing::info!("listening on {}", UDS_PATH);

    let client = Client::new();

    loop {
        let sender = sender.clone();
        let client = client.clone();

        let (socket, _) = listener.accept().await?;

        tokio::spawn(async {
            if let Err(err) = handle_uds(sender, client, socket).await {
                tracing::error!(err = ?err, "handle_uds");
            }
        });
    }
}

#[tracing::instrument(skip_all)]
async fn handle_uds(sender: Sender, client: Client, mut socket: UnixStream) -> Result<()> {
    let mut buf = [0u8; 64];

    let n = socket.read(&mut buf).await?;

    match data::decode(&buf[..n]) {
        WorkerRequest::Summary(query) => summary::process(socket, buf, query).await?,
        WorkerRequest::Payment(req) => payment::process(sender, client, req).await,
        WorkerRequest::PurgeDb => purge_db()?,
    }

    Ok(())
}

fn purge_db() -> Result<()> {
    db::purge()?;

    tracing::info!("DB purged");

    Ok(())
}

#[tracing::instrument(skip_all)]
async fn handle_mpsc(payments: &[data::Payment]) -> Result<()> {
    tracing::info!("mpsc_recv");

    db::insert_payment(payments)?;

    Ok(())
}

#[derive(serde::Serialize, serde::Deserialize)]
pub enum WorkerRequest {
    Summary((i64, i64)),
    Payment(api::payment::Request),
    PurgeDb,
}

pub type Sender = mpsc::UnboundedSender<data::Payment>;
type Receiver = mpsc::UnboundedReceiver<data::Payment>;
