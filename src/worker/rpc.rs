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

#[derive(Clone)]
pub struct PaymentWorker(pub Sender);

impl PaymentService for PaymentWorker {
    async fn process(self, _: context::Context, payment: Payment) {
        tracing::debug!("{} : rpc_recv", payment.correlation_id);
        self.0.send(payment).expect("Should send to mpsc")
    }
}
