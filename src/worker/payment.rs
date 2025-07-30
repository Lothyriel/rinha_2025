use anyhow::Result;
use reqwest::Client;

use crate::{
    api::payment,
    worker::{PaymentTx, pp_client},
};

pub async fn process(
    payment_tx: PaymentTx,
    client: Client,
    payment: payment::Request,
) -> Result<()> {
    tracing::info!(payment.correlation_id, "rpc_recv");

    let payment = pp_client::send(&client, payment).await?;

    tracing::info!("crossbeam_send");

    payment_tx.send(payment)?;

    Ok(())
}
