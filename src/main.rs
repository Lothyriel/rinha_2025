mod payment;
mod summary;

use std::sync::{Arc, atomic::AtomicUsize};

use axum::{Router, routing};
use http_body_util::Empty;
use hyper::{Request, Uri, body::Bytes, client::conn};
use rust_decimal::Decimal;
use tokio::net::TcpStream;
use tokio_postgres::{Client, NoTls};

macro_rules! get_var {
    ($name:expr) => {
        std::env::var($name).expect(concat!("Environment variable not set: ", $name))
    };
}

static ACTIVE_IDX: AtomicUsize = AtomicUsize::new(0);

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let urls: [Uri; 2] = [
        Uri::from_static("http://payment-processor-default:8080"),
        Uri::from_static("htt://payment-processor-fallback:8080"),
    ];

    let db_client = get_db_client().await.expect("DB Client");

    let state = Arc::new(InnerData { urls, db_client });

    let app = Router::new()
        .route("/payments", routing::post(payment::create))
        .route("/payments-summary", routing::get(summary::get))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:80").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn get_db_client() -> Result<tokio_postgres::Client, tokio_postgres::Error> {
    let user = get_var!("PG_USER");
    let pass = get_var!("PG_PASS");
    let host = get_var!("PG_HOST");
    let database = get_var!("PG_DATABASE");

    let conn_str = format!("postgres://{user}:{pass}@{host}/{database}");

    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!("DB Connection error: {}", e);
        }
    });

    Ok(client)
}

async fn api(uri: &Uri) {
    let authority = uri.authority().expect("Authority").as_str();
    let stream = TcpStream::connect(authority).await.expect("TCP Connect");

    let io = hyper_util::rt::TokioIo::new(stream);

    let (mut sender, conn) = conn::http1::handshake(io).await.expect("Handshake");

    tokio::task::spawn(async move {
        if let Err(err) = conn.await {
            tracing::error!("Connection failed: {:?}", err);
        }
    });

    let req = Request::builder()
        .uri(uri)
        .header(hyper::header::HOST, authority)
        .body(Empty::<Bytes>::new())
        .expect("Valid request");

    let res = sender.send_request(req).await.expect("Send request");

    tracing::info!("Response status: {}", res.status());
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct Payment {
    correlation_id: String,
    amount: Decimal,
}

type Data = Arc<InnerData>;

struct InnerData {
    urls: [Uri; 2],
    db_client: Client,
}
