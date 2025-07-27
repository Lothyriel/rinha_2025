use std::time::Duration;

use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{Connection, OpenFlags, Result, params};

const DB_FILE: &str = "./data/rinha.db";
const MMAP_SIZE: &str = "67108864";

type Pool = r2d2::Pool<SqliteConnectionManager>;

pub fn write_pool() -> Result<Pool, r2d2::Error> {
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE;

    init_pool(1, flags)
}

pub fn read_pool() -> Result<Pool, r2d2::Error> {
    init_pool(5, OpenFlags::SQLITE_OPEN_READ_ONLY)
}

fn init_pool(max: u32, flags: OpenFlags) -> Result<Pool, r2d2::Error> {
    let manager = SqliteConnectionManager::file(DB_FILE)
        .with_flags(flags)
        .with_init(|conn| {
            conn.pragma_update(None, "cache", "shared")?;
            conn.pragma_update(None, "journal_mode", "WAL")?;
            conn.pragma_update(None, "synchronous", "NORMAL")?;
            conn.pragma_update(None, "temp_store", "memory")?;
            conn.pragma_update(None, "foreign_keys", "false")?;
            conn.pragma_update(None, "mmap_size", MMAP_SIZE)?;

            Ok(())
        });

    Pool::builder()
        .connection_timeout(Duration::from_secs(1))
        .max_size(max)
        .build(manager)
}

pub fn init_db(conn: &Connection) -> Result<()> {
    let sql = r#"
    CREATE TABLE IF NOT EXISTS payments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        requested_at INTEGER NOT NULL,
        amount INTEGER NOT NULL,
        processor_id INTEGER NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_payments_requested_at ON payments(requested_at);
    "#;

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

pub fn get_payments(conn: &Connection, (from, to): (i64, i64)) -> Result<Vec<CompletedPayment>> {
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
