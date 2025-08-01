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
    let payment = pp_client::send(&client, payment).await?;

    tracing::debug!("payment_tx_send");
    payment_tx.send_async(payment).await?;

    Ok(())
}
