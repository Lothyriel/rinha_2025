use anyhow::Result;
use bincode::{
    config::*,
    error::{DecodeError, EncodeError},
};
use chrono::{DateTime, Utc};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

type BincodeConfig = Configuration<LittleEndian, Fixint, NoLimit>;
const CONFIG: BincodeConfig = bincode::config::standard().with_fixed_int_encoding();

fn encode<S: serde::Serialize>(input: S, buf: &mut [u8]) -> Result<usize, EncodeError> {
    bincode::serde::encode_into_slice(&input, buf, CONFIG)
}

fn decode<D: serde::de::DeserializeOwned>(input: &[u8]) -> Result<D, DecodeError> {
    let (o, _) = bincode::serde::borrow_decode_from_slice(input, CONFIG)?;

    Ok(o)
}
const SIZE: usize = std::mem::size_of::<usize>();

pub async fn send<T: serde::Serialize, S: AsyncWriteExt + Unpin>(
    payload: T,
    buf: &mut [u8],
    stream: &mut S,
) -> Result<()> {
    let n = encode(payload, &mut buf[SIZE..])?;

    buf[..SIZE].copy_from_slice(&n.to_be_bytes());

    stream.write_all(&buf[..n + SIZE]).await?;

    Ok(())
}

pub struct FramedStream<S: AsyncReadExt + Unpin> {
    buf: [u8; 1024],
    stream: S,
    offset: usize,
}

impl<S: AsyncReadExt + Unpin> FramedStream<S> {
    pub fn new(stream: S) -> Self {
        Self {
            buf: [0u8; 1024],
            stream,
            offset: 0,
        }
    }

    pub async fn read(&mut self) -> Result<usize> {
        let read = self.stream.read(&mut self.buf).await?;

        if read == 0 {
            return Ok(0);
        }

        Ok(read)
    }

    pub fn next<T: serde::de::DeserializeOwned>(&mut self) -> Result<Option<T>> {
        if self.offset + SIZE > self.buf.len() {
            // unix sockets should not have partial reads
            panic!("buffer overflow!");
        }

        let payload_size =
            usize::from_be_bytes(self.buf[self.offset..self.offset + SIZE].try_into()?);

        if payload_size == 0 {
            self.offset = 0;
            self.buf.fill(0);
            return Ok(None);
        }

        let payload_offset = self.offset + SIZE;
        let payload = decode(&self.buf[payload_offset..payload_offset + payload_size])?;

        self.offset = payload_offset + payload_size;

        Ok(Some(payload))
    }

    pub fn inner(&mut self) -> &mut S {
        &mut self.stream
    }
}

pub struct Payment {
    pub amount: u64,
    pub requested_at: i64,
    pub processor_id: u8,
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProcessorPaymentRequest {
    pub requested_at: DateTime<Utc>,
    pub amount: f32,
    pub correlation_id: String,
}
