use std::time::Duration;

use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{Connection, Result, params};

const DB_FILE: &str = "/sqlite/rinha.db";

pub fn init_pool(max: u32) -> Result<Pool<SqliteConnectionManager>, r2d2::Error> {
    let manager = SqliteConnectionManager::file(DB_FILE);

    Pool::builder()
        .connection_timeout(Duration::from_secs(1))
        .max_size(max)
        .build(manager)
}

pub fn init_db(conn: &Connection) -> Result<()> {
    let sql = r#"CREATE TABLE IF NOT EXISTS payments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    requested_at INTEGER NOT NULL,
                    amount INTEGER NOT NULL,
                    processor_id INTEGER NOT NULL
                );"#;

    conn.execute_batch(sql)?;

    Ok(())
}

pub fn insert_payment(conn: &Connection, requested_at: i64, amount: u64, id: u8) -> Result<()> {
    conn.execute(
        "INSERT INTO payments (requested_at, amount, processor_id) VALUES (?, ?, ?)",
        params![requested_at, amount, id],
    )?;

    Ok(())
}

pub fn get_payments(
    conn: &Connection,
    (from, to): (i64, i64),
) -> Result<Vec<CompletedPayment>, rusqlite::Error> {
    let mut stmt = conn
        .prepare("SELECT processor_id, amount FROM payments WHERE requested_at BETWEEN ? AND ?;")?;

    stmt.query_map(params![from, to], |row| {
        Ok(CompletedPayment {
            processor_id: row.get(0)?,
            amount: row.get(1)?,
        })
    })?
    .collect()
}

pub struct CompletedPayment {
    pub processor_id: u8,
    pub amount: u64,
}
