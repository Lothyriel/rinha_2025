use anyhow::Result;
use tarpc::{client, context, serde_transport::tcp, tokio_serde::formats};

use crate::worker::{Payment, Sender};

pub async fn client(addr: &str) -> Result<PaymentServiceClient> {
    let transport = tcp::connect(addr, formats::Bincode::default);

    let client = PaymentServiceClient::new(client::Config::default(), transport.await?).spawn();

    Ok(client)
}

#[tarpc::service]
pub trait PaymentService {
    async fn process(payment: Payment);
}

#[derive(Clone, Debug)]
pub struct PaymentWorker(pub Sender);

impl PaymentService for PaymentWorker {
    #[tracing::instrument]
    async fn process(self, _: context::Context, payment: Payment) {
        tracing::info!("rpc_recv: {}", payment.correlation_id);

        self.0
            .send(payment)
            .expect("Consumer should not have dropped")
    }
}
