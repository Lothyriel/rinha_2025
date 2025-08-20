use std::{sync::Arc, time::Instant};

use metrics::Unit;
use tokio::sync::RwLock;

use crate::{api::summary::Summary, data::Payment};

#[derive(Clone)]
pub struct Store {
    payments: Arc<RwLock<Vec<Payment>>>,
}

impl Store {
    pub fn new() -> Self {
        Self {
            payments: Arc::new(RwLock::new(Vec::with_capacity(100_000))),
        }
    }

    pub async fn insert(&self, payment: Payment) {
        let now = Instant::now();

        {
            self.payments.write().await.push(payment);
        }

        metrics::describe_histogram!("db.insert", Unit::Nanoseconds, "db insert time");
        metrics::histogram!("db.insert").record(now.elapsed().as_nanos() as f64);
    }

    pub async fn get(&self, (from, to): (i64, i64)) -> Summary {
        let now = Instant::now();

        let summary = {
            let payments = self.payments.read().await;

            let start = payments
                .binary_search_by_key(&from, |p| p.requested_at)
                .unwrap_or_else(|pos| pos);

            let end = payments
                .binary_search_by_key(&to, |p| p.requested_at)
                .map_or_else(|pos| pos, |pos| pos + 1);

            payments[start..end]
                .iter()
                .fold([(0, 0), (0, 0)], |mut acc, p| {
                    acc[p.processor_id as usize].0 += 1;
                    acc[p.processor_id as usize].1 += p.amount;
                    acc
                })
        };

        metrics::describe_histogram!("db.select", Unit::Nanoseconds, "db query time");
        metrics::histogram!("db.select").record(now.elapsed().as_nanos() as f64);

        Summary::new([summary[0], summary[1]])
    }

    pub async fn purge(&self) {
        self.payments.write().await.clear();
    }
}
