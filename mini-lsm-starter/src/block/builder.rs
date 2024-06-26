use std::io::Write;

use bytes::BufMut;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        BlockBuilder {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    fn esize(&self) -> usize {
        self.offsets.len() * 2 + self.data.len() + 2
    }

    fn get_overlap_metric(&self, key: KeySlice) -> (u16, u16) {
        if self.first_key.is_empty() {
            (0, key.len() as u16)
        } else {
            let num = self
                .first_key
                .raw_ref()
                .iter()
                .zip(key.raw_ref().iter())
                .take_while(|&(a, b)| a == b)
                .count() as u16;
            (num, key.len() as u16 - num)
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let key_len = key.len();
        let value_len = value.len();
        let total_len = key_len + value_len + 4;

        if (!self.is_empty()) && (total_len + self.esize() + 2 > self.block_size) {
            return false;
        }

        self.offsets.push(self.data.len() as u16);
        let (overlap_num, rest_num) = self.get_overlap_metric(key);

        self.data.put_u16(overlap_num);
        self.data.put_u16(rest_num);
        let _ = self
            .data
            .write(&key.raw_ref()[overlap_num as usize..])
            .unwrap();
        self.data.put_u16(value_len as u16);
        let _ = self.data.write(value).unwrap();

        if self.first_key.is_empty() {
            self.first_key.append(key.raw_ref());
        }

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
            first_key: self.first_key,
        }
    }
}
