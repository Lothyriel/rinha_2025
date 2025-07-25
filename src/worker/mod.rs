use std::{
    future,
    net::{IpAddr, Ipv6Addr},
};

use anyhow::{Result, anyhow};
use chrono::Utc;
use futures::StreamExt;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use reqwest::{Client, StatusCode};
use rusqlite::{Connection, params};
use rust_decimal::{dec, prelude::ToPrimitive};
use tarpc::{
    client, context,
    serde_transport::tcp::connect,
    server::{self, Channel},
    tokio_serde::formats::Bincode,
};
use tokio::sync::mpsc;

use crate::{Payment, get_db_pool};

pub async fn client(addr: &str) -> Result<PaymentsClient> {
    let transport = connect(addr, Bincode::default);

    let client = PaymentsClient::new(client::Config::default(), transport.await?).spawn();

    Ok(client)
}

pub async fn serve() -> Result<()> {
    let server_addr = (IpAddr::V6(Ipv6Addr::LOCALHOST), 80);

    let listener = tarpc::serde_transport::tcp::listen(&server_addr, Bincode::default).await?;
    tracing::info!("RPC listening on port {}", listener.local_addr().port());

    let (tx, rx): (Sender, Receiver) = mpsc::unbounded_channel();

    tokio::spawn(start_consumer(rx));

    listener
        .filter_map(|r| future::ready(r.ok()))
        .map(server::BaseChannel::with_defaults)
        .inspect(|c| tracing::info!("RPC connect on {:?}", c.transport().peer_addr()))
        .for_each(|channel| {
            let server = PaymentServer(tx.clone());
            channel.execute(server.serve()).for_each(spawn)
        })
        .await;

    Ok(())
}

async fn start_consumer(mut rx: Receiver) {
    let client = Client::new();
    let pool = get_db_pool(1);

    while let Some(payment) = rx.recv().await {
        process(pool.clone(), &client, payment).await
    }
}

type Sender = mpsc::UnboundedSender<Payment>;
type Receiver = mpsc::UnboundedReceiver<Payment>;

async fn spawn(fut: impl Future<Output = ()> + Send + 'static) {
    tokio::spawn(fut);
}

#[tarpc::service]
pub trait Payments {
    async fn process(payment: Payment);
}

#[derive(Clone)]
struct PaymentServer(Sender);

impl Payments for PaymentServer {
    async fn process(self, _: context::Context, payment: Payment) {
        self.0.send(payment).expect("Should send to mpsc")
    }
}

async fn process(pool: Pool<SqliteConnectionManager>, client: &Client, payment: Payment) {
    let now = Utc::now().timestamp_millis();

    let processor_id = process_payment(client, &payment).await;

    let conn = pool.get().expect("Should get connection from the pool");

    let amount = payment.amount * dec!(100);

    insert_payment(
        &conn,
        now,
        amount.to_u64().expect("Valid u64"),
        processor_id,
    );
}

const PAYMENT_PROCESSORS: [(u8, &str); 2] = [
    (1, "http://payment-processor-default:8080"),
    (2, "http://payment-processor-fallback:8080"),
];

async fn process_payment(client: &Client, payment: &Payment) -> u8 {
    for (id, uri) in PAYMENT_PROCESSORS {
        let result = send(uri, client, payment).await;

        match result {
            Ok(_) => return id,
            Err(_) => continue,
        }
    }

    panic!("Neither processor is working ATM")
}

async fn send(uri: &str, client: &Client, payment: &Payment) -> anyhow::Result<()> {
    let res = client.post(uri).json(payment).send().await?;

    tracing::info!("Response status: {}", res.status());

    match res.status() {
        StatusCode::OK => Ok(()),
        _ => Err(anyhow!("{}", res.text().await?)),
    }
}

fn insert_payment(conn: &Connection, requested_at: i64, amount: u64, id: u8) {
    conn.execute(
        "INSERT INTO payments (requested_at, amount, processor_id) VALUES (?, ?, ?)",
        params![requested_at, amount, id],
    )
    .expect("Failed to increment stats");
}
