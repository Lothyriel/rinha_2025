mod processor;
pub mod rpc;

use std::net::Ipv4Addr;

use anyhow::Result;
use futures::{StreamExt, future};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use reqwest::Client;
use tarpc::{
    serde_transport::tcp,
    server::{self, Channel},
    tokio_serde::formats,
};
use tokio::sync::mpsc;

use crate::{
    db,
    worker::rpc::{PaymentService, PaymentWorker},
};

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct Payment {
    pub amount: u64,
    pub correlation_id: String,
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

    let (tx, rx): (Sender, Receiver) = mpsc::unbounded_channel();
    start_consumer(rx, pool);

    listener
        .filter_map(|r| future::ready(r.ok()))
        .map(server::BaseChannel::with_defaults)
        .inspect(|c| tracing::debug!("RPC connect on {:?}", c.transport().peer_addr()))
        .for_each(|channel| async {
            let server = PaymentWorker(tx.clone());

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
fn start_consumer(rx: Receiver, pool: Pool<SqliteConnectionManager>) {
    tracing::info!("Starting mpsc consumer");
    tokio::spawn(consumer(rx, pool));
}

#[tracing::instrument(skip_all)]
async fn consumer(mut rx: Receiver, pool: Pool<SqliteConnectionManager>) {
    let client = Client::new();

    while let Some(payment) = rx.recv().await {
        tracing::info!("mpsc_recv: {}", payment.correlation_id);

        let result = processor::handle(pool.clone(), &client, payment).await;

        if let Err(e) = result {
            tracing::error!("mpsc_err: {e}");
        }
    }
}

pub type Sender = mpsc::UnboundedSender<Payment>;
type Receiver = mpsc::UnboundedReceiver<Payment>;
