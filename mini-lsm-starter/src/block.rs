mod builder;
mod iterator;

use crate::key::KeyVec;
pub use builder::BlockBuilder;
use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
    pub(crate) first_key: KeyVec,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output

    pub fn estimated_size(&self) -> usize {
        self.data.len() + self.offsets.len() * 2 + 2
    }

    pub fn encode(&self) -> Bytes {
        let mut buf = self.data.clone();
        let _: Vec<()> = self
            .offsets
            .iter()
            .map(|offset| buf.put_u16(*offset))
            .collect();
        buf.put_u16(self.offsets.len() as u16);
        Bytes::from(buf)
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let mut last_two = &data[data.len() - 2..];
        let num = last_two.get_u16();

        let offset_buf_len = (num * 2) as usize;
        let buf_end = data.len() - 2;

        let entries = data[..data.len() - 2 - offset_buf_len].to_vec();
        let mut buf = &data[buf_end - offset_buf_len..buf_end];
        let mut offsets = Vec::new();
        for _ in 0..num {
            offsets.push(buf.get_u16());
        }

        let mut first_key_len_buf = &data[2..4];
        let first_key_len = first_key_len_buf.get_u16() as usize;
        let first_key_buf = &data[4..4 + first_key_len];
        let first_key = KeyVec::from_vec(first_key_buf.to_vec());

        Block {
            data: entries,
            offsets,
            first_key,
        }
    }
}
