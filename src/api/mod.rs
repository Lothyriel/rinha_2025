pub mod payment;
pub mod summary;
mod util;

use std::io::IoSlice;

use anyhow::Result;
use chrono::DateTime;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
};

use crate::{bind_unix_socket, data};

pub const API_SOCK: &str = "./api.sock";

#[tokio::main]
pub async fn serve() -> Result<()> {
    tracing::info!("starting API");

    let socket = data::get_api_n()
        .map(data::get_api_socket_name)
        .unwrap_or(API_SOCK.to_string());

    let listener = bind_unix_socket(&socket)?;
    tracing::info!("binded to unix socket on {socket}");

    loop {
        let (socket, _) = listener.accept().await?;

        let counter = metrics::counter!("http.conn");
        counter.increment(1);

        tokio::spawn(async {
            if let Err(err) = handle_http(socket).await {
                tracing::error!(?err, "http_err");
            }
        });
    }
}

async fn handle_http(mut socket: UnixStream) -> Result<()> {
    let mut buf = [0u8; 512];

    loop {
        let n = socket.read(&mut buf).await?;

        let counter = metrics::counter!("http.req");
        counter.increment(1);

        if n == 0 {
            return Ok(());
        }

        match buf[0] {
            // [G]ET /payments-summary
            b'G' => {
                let n = get_summary(&mut buf).await?;
                let body_len = n.to_string();

                let res = &[
                    IoSlice::new(b"HTTP/1.1 200 OK\r\nContent-Length: "),
                    IoSlice::new(body_len.as_bytes()),
                    IoSlice::new(b"\r\n\r\n"),
                    IoSlice::new(&buf[..n]),
                ];

                _ = socket.write_vectored(res).await?;
            }
            // [P]OST
            b'P' => match buf[7] {
                // POST /p[a]yments
                b'a' => handle_payment(&mut buf).await?,
                // POST /p[u]rge-payments
                b'u' => {
                    util::purge().await?;
                }
                _ => {
                    tracing::warn!("Invalid request {:?}", std::str::from_utf8(&buf));
                }
            },
            _ => {
                tracing::warn!("Invalid request {:?}", std::str::from_utf8(&buf));
            }
        }
    }
}

async fn handle_payment(buf: &mut [u8]) -> Result<(), anyhow::Error> {
    let start = buf
        .iter()
        .position(|&b| b == b'{')
        .expect("find json start");

    let end = buf.iter().rposition(|&b| b == b'}').expect("find json end");

    let req = &buf[start..end + 1];
    let req = serde_json::from_slice(req)?;

    payment::send(buf, req).await?;

    Ok(())
}

async fn get_summary(buf: &mut [u8]) -> Result<usize> {
    let query = get_query(buf)?;

    summary::get_summary(query, buf).await
}

fn get_query(buf: &[u8]) -> Result<(i64, i64)> {
    const DISTANT_FUTURE: DateTime<chrono::Utc> = DateTime::from_timestamp_nanos(i64::MAX);

    const RFC_3339_SIZE: usize = "2001-04-27T15:30:05.000Z".len();
    const QUERY_STRING_OFFSET: usize = "GET /payments-summary".len();

    const FROM_FIELD_SIZE: usize = "?from=".len();
    const FROM_START: usize = QUERY_STRING_OFFSET + FROM_FIELD_SIZE;
    const FROM_END: usize = FROM_START + RFC_3339_SIZE;

    const TO_FIELD_SIZE: usize = "&to=".len();
    const TO_START: usize = FROM_END + TO_FIELD_SIZE;
    const TO_END: usize = TO_START + RFC_3339_SIZE;

    let from = &buf[FROM_START..FROM_END];
    let to = &buf[TO_START..TO_END];

    let from = std::str::from_utf8(from)?;
    let to = std::str::from_utf8(to)?;

    let from = DateTime::parse_from_rfc3339(from).unwrap_or_default();
    let to = DateTime::parse_from_rfc3339(to).unwrap_or(DISTANT_FUTURE.into());

    Ok((from.timestamp_micros(), to.timestamp_micros()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Utc};

    #[test]
    fn test_add() {
        let input =
            b"GET /payments-summary?from=2001-04-27T12:30:00.000Z&to=2025-05-27T15:37:50.000Z";

        let date = NaiveDate::from_ymd_opt(2001, 4, 27).expect("valid date");
        let time = NaiveTime::from_hms_opt(12, 30, 0).expect("valid time");
        let datetime = NaiveDateTime::new(date, time);

        let from: DateTime<Utc> = DateTime::from_naive_utc_and_offset(datetime, Utc);

        let date = NaiveDate::from_ymd_opt(2025, 5, 27).expect("valid date");
        let time = NaiveTime::from_hms_opt(15, 37, 50).expect("valid time");
        let datetime = NaiveDateTime::new(date, time);

        let to: DateTime<Utc> = DateTime::from_naive_utc_and_offset(datetime, Utc);

        let result = get_query(input).expect("get query");

        assert_eq!(result, (from.timestamp_micros(), to.timestamp_micros()));
    }
}
