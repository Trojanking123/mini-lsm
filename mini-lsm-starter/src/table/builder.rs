use std::sync::Arc;
use std::{io::Write, path::Path};

use anyhow::Result;
use bytes::{BufMut, Bytes};
use nom::AsBytes;

use super::bloom::Bloom;
use super::{BlockMeta, SsTable};
use crate::key::{Key, KeyBytes};
use crate::table::FileObject;
use crate::{block::BlockBuilder, key::KeySlice, lsm_storage::BlockCache};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    key_hash_vec: Vec<u32>,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        SsTableBuilder {
            builder: BlockBuilder::new(block_size),
            first_key: Vec::new(),
            last_key: Vec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
            key_hash_vec: vec![],
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if !self.builder.add(key, value) {
            let old = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
            let _ = self.builder.add(key, value);
            let block = old.build();

            let meta = BlockMeta {
                offset: self.data.len(),
                first_key: KeyBytes::from_bytes(Bytes::from(self.first_key.clone())),
                last_key: KeyBytes::from_bytes(Bytes::from(self.last_key.clone())),
            };

            let res = block.encode();
            let _ = self.data.write(res.as_bytes()).unwrap();
            self.meta.push(meta);
            self.first_key = Vec::new()
        }
        self.key_hash_vec
            .push(farmhash::fingerprint32(key.raw_ref()));
        if self.first_key.is_empty() {
            self.first_key = key.raw_ref().to_vec();
        }

        self.last_key = key.raw_ref().to_vec();
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        let mut data = self.data;
        let mut meta_vec = self.meta;
        if !self.first_key.is_empty() {
            let block = self.builder.build();
            let meta = BlockMeta {
                offset: data.len(),
                first_key: KeyBytes::from_bytes(Bytes::from(self.first_key.clone())),
                last_key: KeyBytes::from_bytes(Bytes::from(self.last_key.clone())),
            };
            let res = block.encode();
            let _ = data.write(res.as_bytes()).unwrap();
            meta_vec.push(meta);
        }

        let offset = data.len();

        BlockMeta::encode_block_meta(&meta_vec, &mut data);
        data.put_u32(offset as u32);

        let bloom_offset = data.len();
        let bits_per_key = Bloom::bloom_bits_per_key(self.key_hash_vec.len(), 0.01);
        let bloom = Bloom::build_from_key_hashes(&self.key_hash_vec, bits_per_key);
        let bloom_data = bloom.encode(&mut data);
        data.put_u32(bloom_offset as u32);

        let first_key = meta_vec.first().unwrap().first_key.clone();
        let last_key = meta_vec.last().unwrap().last_key.clone();

        let obj = FileObject::create(path.as_ref(), data).unwrap();
        let table = SsTable {
            file: obj,
            block_meta: meta_vec,
            block_meta_offset: offset,
            id,
            block_cache,
            first_key,
            last_key,
            max_ts: 0,
            bloom: Some(bloom),
        };
        Ok(table)
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        let cache = BlockCache::new(1000);
        self.build(0, Some(Arc::new(cache)), path)
    }
}
