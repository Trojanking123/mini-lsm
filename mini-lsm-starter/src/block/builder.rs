#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

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

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let key_len = key.len();
        let value_len = value.len();
        let total_len = key_len + value_len + 4;

        if (!self.is_empty()) && (total_len + self.esize() + 2 > self.block_size) {
            return false;
        }
        self.data.put_u16(key_len as u16);
        let _ = self.data.write(key.raw_ref()).unwrap();
        self.data.put_u16(value_len as u16);
        let _ = self.data.write(value).unwrap();
        self.offsets.push(total_len as u16);

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
        }
    }
}
