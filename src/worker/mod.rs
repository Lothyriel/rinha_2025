mod pp_client;
pub mod rpc;

use std::net::Ipv4Addr;

use anyhow::Result;
use chrono::{DateTime, Utc};
use futures::{StreamExt, future};
use reqwest::Client;
use tarpc::{serde_transport::tcp, server::Channel, tokio_serde::formats};
use tokio::sync::mpsc;

use crate::{
    db,
    worker::rpc::{PaymentService, PaymentWorker},
};

pub struct Payment {
    pub amount: u64,
    pub requested_at: i64,
    pub processor_id: u8,
}

#[tracing::instrument(skip_all)]
pub async fn serve(port: u16) -> Result<()> {
    tracing::info!("Starting worker on port {port}");

    let addr = (Ipv4Addr::UNSPECIFIED, port);
    let listener = tcp::listen(&addr, formats::Bincode::default).await?;

    let pool = db::write_pool()?;
    {
        let conn = pool.get()?;
        db::init_db(&conn)?;
    }

    let client = Client::new();

    let (tx, rx): (Sender, Receiver) = mpsc::unbounded_channel();
    start_consumer(rx, pool);

    listener
        .filter_map(|r| future::ready(r.ok()))
        .map(tarpc::server::BaseChannel::with_defaults)
        .inspect(|c| tracing::debug!(rpc_connect_addr = ?c.transport().peer_addr()))
        .for_each(|channel| async {
            let server = PaymentWorker {
                sender: tx.clone(),
                client: client.clone(),
            };

            tokio::spawn(async {
                channel
                    .execute(server.serve())
                    .for_each(|f| async {
                        tokio::spawn(f);
                    })
                    .await;

                tracing::debug!("PaymentServer disconnected");
            });
        })
        .await;

    Ok(())
}

#[tracing::instrument(skip_all)]
fn start_consumer(rx: Receiver, pool: db::Pool) {
    tracing::info!("Starting mpsc consumer");
    tokio::spawn(consumer(rx, pool));
}

#[tracing::instrument(skip_all)]
async fn consumer(mut rx: Receiver, pool: db::Pool) {
    while let Some(payment) = rx.recv().await {
        let result = handle(payment, pool.clone()).await;

        if let Err(er) = result {
            tracing::error!(?er, "mpsc_err");
        }
    }
}

#[tracing::instrument(skip_all)]
async fn handle(payment: Payment, pool: db::Pool) -> Result<()> {
    tracing::info!("mpsc_recv");

    let conn = pool.get()?;

    db::insert_payment(&conn, payment)?;

    Ok(())
}

pub type Sender = mpsc::UnboundedSender<Payment>;
type Receiver = mpsc::UnboundedReceiver<Payment>;

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProcessorPayment {
    pub requested_at: DateTime<Utc>,
    pub amount: f32,
    pub correlation_id: String,
}
