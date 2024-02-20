mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
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
        let mut buf = data;
        let mut data_len = 0;
        for _ in 0..num {
            let key_len = buf.get_u16();
            buf = &buf[key_len as usize..];
            let value_len = buf.get_u16();
            buf = &buf[value_len as usize..];
            data_len += key_len + value_len + 4;
        }

        let entries = data[..data_len as usize].to_vec();
        let mut offsets = Vec::new();
        for _ in 0..num {
            offsets.push(buf.get_u16());
        }

        Block {
            data: entries,
            offsets,
        }
    }
}
