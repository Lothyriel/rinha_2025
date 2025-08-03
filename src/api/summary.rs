use std::time::Instant;

use anyhow::Result;
use axum::{Json, response::IntoResponse};
use axum_extra::extract::OptionalQuery;
use chrono::{DateTime, Utc};
use metrics::Unit;
use reqwest::StatusCode;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
};

use crate::{WORKER_SOCKET, data, worker::WorkerRequest};

#[derive(serde::Deserialize)]
pub struct SummaryQuery {
    from: DateTime<Utc>,
    to: DateTime<Utc>,
}

pub async fn get(OptionalQuery(query): OptionalQuery<SummaryQuery>) -> impl IntoResponse {
    let now = Instant::now();

    let query = if let Some(query) = query {
        (query.from.timestamp_micros(), query.to.timestamp_micros())
    } else {
        (0, i64::MAX)
    };

    let res = match get_summary(query).await {
        Ok(s) => Json(s).into_response(),
        Err(err) => {
            tracing::error!(?err, "http_get_summary");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    };

    metrics::describe_histogram!("http.get", Unit::Microseconds, "http handler time");
    metrics::histogram!("http.get").record(now.elapsed().as_micros() as f64);

    res
}

async fn get_summary(query: (i64, i64)) -> Result<Summary> {
    let mut socket = UnixStream::connect(&*WORKER_SOCKET).await?;
    tracing::debug!("connected to unix socket on {}", *WORKER_SOCKET);

    let mut buf = [0u8; 32];
    let n = data::encode(WorkerRequest::Summary(query), &mut buf);
    tracing::debug!("writing to {}", *WORKER_SOCKET);
    socket.write_all(&buf[..n]).await?;

    tracing::debug!("reading from {}", *WORKER_SOCKET);
    let n = socket.read(&mut buf).await?;
    let res = data::decode(&buf[..n]);

    Ok(res)
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct Summary {
    default: ProcessedData,
    fallback: ProcessedData,
}

impl Summary {
    pub fn new(summary: [(u64, u64); 2]) -> Self {
        Summary {
            default: ProcessedData::new(summary[0]),
            fallback: ProcessedData::new(summary[1]),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProcessedData {
    total_requests: u64,
    total_amount: f32,
}

impl ProcessedData {
    pub fn new((requests, amount): (u64, u64)) -> Self {
        ProcessedData {
            total_requests: requests,
            total_amount: amount as f32 / 100.0,
        }
    }
}
