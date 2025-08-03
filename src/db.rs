use std::{sync::Arc, time::Instant};

use metrics::Unit;
use parking_lot::RwLock;

use crate::{api::summary::Summary, data::Payment};

#[derive(Clone)]
pub struct Store {
    payments: Arc<RwLock<Vec<Payment>>>,
}

impl Store {
    pub fn new() -> Self {
        Self {
            payments: Arc::new(RwLock::new(Vec::with_capacity(20_000))),
        }
    }

    pub fn insert(&self, payment: Payment) {
        let now = Instant::now();

        {
            self.payments.write().push(payment);
        }

        metrics::describe_histogram!("db.insert", Unit::Microseconds, "db insert time");
        metrics::histogram!("db.insert").record(now.elapsed().as_micros() as f64);
    }

    pub fn get(&self, (from, to): (i64, i64)) -> Summary {
        let now = Instant::now();

        let summary = {
            let payments = self.payments.read();

            let start = payments
                .binary_search_by_key(&from, |p| p.requested_at)
                .unwrap_or_else(|pos| pos);

            let end = payments
                .binary_search_by_key(&to, |p| p.requested_at)
                .map_or_else(|pos| pos, |pos| pos + 1);

            payments[start..end].iter().fold((0, 0), |mut acc, p| {
                acc.0 += 1;
                acc.1 += p.amount;
                acc
            })
        };

        metrics::describe_histogram!("db.select", Unit::Microseconds, "db query time");
        metrics::histogram!("db.select").record(now.elapsed().as_micros() as f64);

        Summary::new([summary, (0, 0)])
    }

    pub fn purge(&self) {
        self.payments.write().clear();
    }
}
