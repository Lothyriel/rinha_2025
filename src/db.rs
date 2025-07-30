use rusqlite::{Connection, OpenFlags, Result, params};

use crate::data::Payment;

const DB_FILE: &str = "./rinha.db";
const MMAP_SIZE: &str = "67108864";

fn writer() -> Result<Connection> {
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE;

    get_conn(flags)
}

fn reader() -> Result<Connection> {
    get_conn(OpenFlags::SQLITE_OPEN_READ_ONLY)
}

fn get_conn(flags: OpenFlags) -> Result<Connection> {
    let conn = rusqlite::Connection::open_with_flags(DB_FILE, flags)?;

    conn.pragma_update(None, "cache", "shared")?;
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    conn.pragma_update(None, "temp_store", "memory")?;
    conn.pragma_update(None, "foreign_keys", "false")?;
    conn.pragma_update(None, "mmap_size", MMAP_SIZE)?;

    Ok(conn)
}

pub fn init_db() -> Result<()> {
    let conn = writer()?;

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

#[tracing::instrument(skip_all)]
pub fn insert_payment(p: Payment) -> Result<()> {
    let conn = writer()?;

    conn.execute(
        "INSERT INTO payments (requested_at, amount, processor_id) VALUES (?, ?, ?)",
        params![p.requested_at, p.amount, p.processor_id],
    )?;

    Ok(())
}

#[tracing::instrument(skip_all)]
pub fn get_payments((from, to): (i64, i64)) -> Result<Vec<PaymentDto>> {
    let conn = reader()?;

    let mut stmt = conn.prepare_cached(
        "SELECT processor_id, amount FROM payments WHERE requested_at BETWEEN ? AND ?;",
    )?;

    let query_map = stmt.query_map(params![from, to], |row| {
        Ok(PaymentDto {
            processor_id: row.get(0)?,
            amount: row.get(1)?,
        })
    })?;

    query_map.collect()
}

pub fn purge() -> Result<()> {
    writer()?.execute_batch("DELETE FROM payments;")
}

pub struct PaymentDto {
    pub processor_id: u8,
    pub amount: u64,
}
