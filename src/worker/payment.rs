use anyhow::Result;
use reqwest::Client;

use crate::{api::payment, db, worker::pp_client};

pub async fn process(store: db::Store, client: Client, payment: payment::Request) -> Result<()> {
    let payment = pp_client::send(&client, payment).await?;

    tracing::trace!("payment_tx_send");

    store.insert(payment).await;

    Ok(())
}
