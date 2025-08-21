use anyhow::Result;
use chrono::DateTime;
use tokio::{io::AsyncReadExt, net::UnixStream};

use crate::{data, worker::WorkerRequest};

pub async fn get_summary(socket: &mut UnixStream, buf: &mut [u8]) -> Result<usize> {
    let query = get_query(buf)?;

    let req = WorkerRequest::Summary(query);

    data::send(req, buf, socket).await?;

    let n = socket.read(buf).await?;

    Ok(n)
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

pub struct Summary {
    pub default: ProcessedData,
    pub fallback: ProcessedData,
}

impl Summary {
    pub fn new(summary: [(u64, u64); 2]) -> Self {
        Summary {
            default: ProcessedData::new(summary[0]),
            fallback: ProcessedData::new(summary[1]),
        }
    }
}

pub struct ProcessedData {
    pub count: u64,
    pub amount: f32,
}

impl ProcessedData {
    pub fn new((requests, amount): (u64, u64)) -> Self {
        ProcessedData {
            count: requests,
            amount: amount as f32 / 100.0,
        }
    }
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
