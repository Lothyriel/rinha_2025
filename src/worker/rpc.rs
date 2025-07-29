use anyhow::Result;
use chrono::Utc;
use reqwest::Client;
use tarpc::{client, context, serde_transport::tcp, tokio_serde::formats};

use crate::{
    api::PaymentRequest,
    worker::{
        ProcessorPayment, Sender,
        pp_client::{self},
    },
};

pub async fn client(addr: &str) -> Result<PaymentServiceClient> {
    let transport = tcp::connect(addr, formats::Bincode::default);

    let client = PaymentServiceClient::new(client::Config::default(), transport.await?).spawn();

    Ok(client)
}

#[tarpc::service]
pub trait PaymentService {
    async fn process(payment: PaymentRequest);
}

#[derive(Clone, Debug)]
pub struct PaymentWorker {
    pub sender: Sender,
    pub client: Client,
}

impl PaymentService for PaymentWorker {
    #[tracing::instrument(skip_all)]
    async fn process(self, _: context::Context, payment: PaymentRequest) {
        tracing::info!(payment.correlation_id, "rpc_recv");

        let payment = ProcessorPayment {
            requested_at: Utc::now(),
            amount: payment.amount,
            correlation_id: payment.correlation_id,
        };

        let payment = pp_client::create(&self.client, payment).await;

        tracing::info!("mpsc_send");
        self.sender
            .send(payment)
            .expect("Consumer should not have dropped")
    }
}
