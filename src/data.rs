use bincode::config::*;
use chrono::{DateTime, Utc};

type BincodeConfig = Configuration<LittleEndian, Fixint, NoLimit>;
const CONFIG: BincodeConfig = bincode::config::standard().with_fixed_int_encoding();

pub fn get_api_socket_name(i: i32) -> String {
    format!("/var/run/api{i}.sock")
}

pub fn get_api_n() -> Option<i32> {
    std::env::var("API_N").ok().and_then(|n| n.parse().ok())
}

pub fn encode<S: serde::Serialize>(input: S, buf: &mut [u8]) -> usize {
    bincode::serde::encode_into_slice(&input, buf, CONFIG).expect("Failed to encode")
}

pub fn decode<D: serde::de::DeserializeOwned>(input: &[u8]) -> D {
    let (o, _) = bincode::serde::borrow_decode_from_slice(input, CONFIG).expect("Failed to decode");

    o
}

pub struct Payment {
    pub amount: u64,
    pub requested_at: i64,
    #[allow(dead_code)]
    pub processor_id: u8,
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProcessorPaymentRequest {
    pub requested_at: DateTime<Utc>,
    pub amount: f32,
    pub correlation_id: String,
}
