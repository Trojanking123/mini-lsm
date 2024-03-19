use std::sync::Arc;

use crate::key::{KeySlice, KeyVec};

use super::Block;
use bytes::Buf;
/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the value range from the block
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let key = block.first_key.clone();
        let start = 4 + key.len();
        let buf = block.data.as_slice();
        let mut buf = &buf[start..];
        let value_len = buf.get_u16() as usize;
        let value_range = (start + 2, start + 2 + value_len);
        Self {
            block,
            key: key.clone(),
            value_range,
            idx: 0,
            first_key: key,
        }
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let target_key = key;
        let num = block.offsets.len();
        let mut buf = block.data.as_slice();
        for i in 00..num {
            let overlap_key_len = buf.get_u16() as usize;
            let reset_key_len = buf.get_u16() as usize;

            let rest_key = &buf[..reset_key_len];
            buf = &buf[reset_key_len..];

            let full_key = [&block.first_key.raw_ref()[..overlap_key_len], &rest_key].concat();

            let value_len = buf.get_u16() as usize;
            buf = &buf[value_len..];

            if &full_key[..] >= target_key.raw_ref() {
                let first_key = KeyVec::from_vec(full_key);
                let offset = block.offsets[i] as usize;
                return BlockIterator {
                    block,
                    key: first_key.clone(),
                    value_range: (
                        (offset + reset_key_len + 6),
                        (offset + reset_key_len + 6 + value_len),
                    ),
                    idx: i,
                    first_key,
                };
            }
        }

        let mut iter = BlockIterator::new(block);
        iter.idx = num;
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        KeySlice::from_slice(self.key.raw_ref())
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        let (s, e) = self.value_range;
        &self.block.data.as_slice()[s..e]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key().is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        let target = KeySlice::from_slice(self.first_key.raw_ref());
        *self = BlockIterator::create_and_seek_to_key(self.block.clone(), target);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        if self.idx + 1 >= self.block.offsets.len() {
            self.key = KeyVec::new();
            self.value_range = (0, 0);
        } else {
            let (_, end) = self.value_range;
            let mut buf = &self.block.data.as_slice()[end..];

            let overlap_key_len = buf.get_u16() as usize;
            let reset_key_len = buf.get_u16() as usize;
            let rest_key = &buf[..reset_key_len];
            buf = &buf[reset_key_len..];

            let full_key = [
                &self.block.first_key.raw_ref()[..overlap_key_len],
                &rest_key,
            ]
            .concat();

            let value_len = buf.get_u16() as usize;
            let offset = self.block.offsets[self.idx + 1] as usize;
            let value_range = (
                offset + reset_key_len + 6,
                offset + reset_key_len + 6 + value_len,
            );
            self.key = KeyVec::from_vec(full_key);
            self.value_range = value_range;
            self.idx += 1;
        }
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        *self = BlockIterator::create_and_seek_to_key(self.block.clone(), key);
    }
}
