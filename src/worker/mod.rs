mod processor;
pub mod rpc;

use std::net::{IpAddr, Ipv4Addr};

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

pub async fn serve() -> Result<()> {
    let server_addr = (IpAddr::V4(Ipv4Addr::UNSPECIFIED), 80);
    let listener = tcp::listen(&server_addr, formats::Bincode::default).await?;

    tracing::info!("Starting worker...");

    let pool = db::init_pool(1)?;
    let conn = pool.get()?;

    db::init_db(&conn)?;

    let (tx, rx): (Sender, Receiver) = mpsc::unbounded_channel();

    tracing::info!("Starting mpsc consumer");
    tokio::spawn(start_consumer(rx, pool));

    tracing::info!("RPC listening on {}", listener.local_addr());
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

async fn start_consumer(mut rx: Receiver, pool: Pool<SqliteConnectionManager>) {
    let client = Client::new();

    while let Some(payment) = rx.recv().await {
        tracing::debug!("{} : mpsc_recv", payment.correlation_id);

        let result = processor::handle(pool.clone(), &client, payment).await;

        if let Err(e) = result {
            tracing::error!("CONSUMER: {e}");
        }
    }
}

pub type Sender = mpsc::UnboundedSender<Payment>;
pub type Receiver = mpsc::UnboundedReceiver<Payment>;
