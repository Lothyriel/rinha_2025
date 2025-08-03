use std::time::{Duration, Instant};

use metrics::Unit;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{Connection, OpenFlags, Result, params};

use crate::data::Payment;

const DB_FILE: &str = "file:memdb1?mode=memory&cache=shared";

pub type Pool = r2d2::Pool<SqliteConnectionManager>;

pub fn write_pool() -> Result<Pool, r2d2::Error> {
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE;

    init_pool(1, flags)
}

pub fn read_pool() -> Result<Pool, r2d2::Error> {
    init_pool(8, OpenFlags::SQLITE_OPEN_READ_ONLY)
}

fn init_pool(max: u32, flags: OpenFlags) -> Result<Pool, r2d2::Error> {
    let manager = SqliteConnectionManager::file(DB_FILE)
        .with_flags(flags)
        .with_init(|conn| {
            conn.pragma_update(None, "journal_mode", "WAL")?;
            conn.pragma_update(None, "synchronous", "OFF")?;
            conn.pragma_update(None, "temp_store", "memory")?;
            conn.pragma_update(None, "foreign_keys", "false")?;

            Ok(())
        });

    Pool::builder()
        .connection_timeout(Duration::from_millis(100))
        .max_size(max)
        .build(manager)
}

pub fn init_db(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            requested_at INTEGER NOT NULL,
            amount INTEGER NOT NULL,
            processor_id INTEGER NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_payments_requested_at ON payments(requested_at);
    "#,
    )
}

pub fn insert_payment(conn: &Connection, p: &Payment) -> Result<()> {
    let now = Instant::now();

    let mut stmt = conn.prepare_cached(
        "INSERT INTO payments (requested_at, amount, processor_id) VALUES (?, ?, ?)",
    )?;

    stmt.execute(params![p.requested_at, p.amount, p.processor_id])?;

    metrics::describe_histogram!("db.insert", Unit::Microseconds, "db insert time");
    metrics::histogram!("db.insert").record(now.elapsed().as_micros() as f64);

    Ok(())
}

pub fn get_payments(conn: &Connection, (from, to): (i64, i64)) -> Result<Vec<PaymentDto>> {
    let now = Instant::now();

    let mut stmt = conn.prepare_cached(
        "SELECT processor_id, amount FROM payments WHERE requested_at BETWEEN ? AND ?;",
    )?;

    let query_map = stmt.query_map(params![from, to], |row| {
        Ok(PaymentDto {
            processor_id: row.get(0)?,
            amount: row.get(1)?,
        })
    })?;

    let result = query_map.collect();

    metrics::describe_histogram!("db.select", Unit::Microseconds, "db query time");
    metrics::histogram!("db.select").record(now.elapsed().as_micros() as f64);

    result
}

pub fn purge(conn: &Connection) -> Result<()> {
    conn.execute_batch("DELETE FROM payments;")
}

pub struct PaymentDto {
    pub processor_id: u8,
    pub amount: u64,
}
