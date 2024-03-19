#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

pub(crate) mod bloom;
mod builder;
mod iterator;

use std::fs::File;
use std::io::Write;
use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut};
pub use iterator::SsTableIterator;
use nom::AsBytes;

use crate::block::Block;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        for item in block_meta.iter() {
            buf.put_u64(item.offset as u64);
            let first_key_len = item.first_key.len() as u16;
            let last_key_len = item.last_key.len() as u16;

            buf.put_u16(first_key_len);
            let _ = buf.write(item.first_key.raw_ref()).unwrap();
            buf.put_u16(last_key_len);
            let _ = buf.write(item.last_key.raw_ref()).unwrap();
        }
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(buf: impl Buf) -> Vec<BlockMeta> {
        let mut buf = buf;
        let mut metas = Vec::new();
        while buf.remaining() != 0 {
            let offset = buf.get_u64() as usize;
            let first_key_len = buf.get_u16() as usize;
            let first_key = buf.copy_to_bytes(first_key_len);
            let last_key_len = buf.get_u16() as usize;
            let last_key = buf.copy_to_bytes(last_key_len);
            let meta = BlockMeta {
                offset,
                first_key: KeyBytes::from_bytes(first_key),
                last_key: KeyBytes::from_bytes(last_key),
            };
            metas.push(meta)
        }

        metas
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        let cache = BlockCache::new(1000);
        Self::open(0, Some(Arc::new(cache)), file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let len = file.size() as usize;
        let buf = file.read(0, len as u64).unwrap();

        let mut last_four = &buf[(len - 4)..];
        let bloom_offset = last_four.get_u32() as usize;
        let bloom_buf = &buf[bloom_offset..len - 4];
        let bloom_num = bloom_buf.len();
        let bloom = Bloom::decode(bloom_buf)?;

        let mut last_meta_four = &buf[(len - 8 - bloom_num)..(len - 4 - bloom_num)];
        let meta_offset = last_meta_four.get_u32() as usize;
        let meta_buf = &buf[meta_offset..(len - 8 - bloom_num)];
        let metas = BlockMeta::decode_block_meta(meta_buf);
        let first_key = metas.first().unwrap().first_key.clone();
        let last_key = metas.last().unwrap().last_key.clone();
        let sst = SsTable {
            file,
            block_meta: metas,
            block_meta_offset: meta_offset,
            id,
            block_cache,
            first_key,
            last_key,
            bloom: Some(bloom),
            max_ts: 0,
        };

        Ok(sst)
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        if block_idx >= self.num_of_blocks() {
            return Err(anyhow::Error::msg("block idx overflowed"));
        }
        let start = self.block_meta[block_idx].offset as u64;
        let end = if block_idx + 1 == self.num_of_blocks() {
            self.block_meta_offset as u64
        } else {
            self.block_meta[block_idx + 1].offset as u64
        };
        let buf = self.file.read(start, end - start)?;
        let block = Block::decode(buf.as_bytes());
        let arc_block = Arc::new(block);
        Ok(arc_block.clone())
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        let key = (self.sst_id(), block_idx);
        if self.block_cache.is_some() {
            let cache = self.block_cache.as_deref().unwrap();
            let res = match cache.get(&key) {
                Some(v) => Ok(v.clone()),
                None => {
                    let block = self.read_block(block_idx)?;
                    cache.insert(key, block.clone());
                    Ok(block.clone())
                }
            };
            return res;
        }
        let block = self.read_block(block_idx)?;
        Ok(block)
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        unimplemented!()
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }

    pub fn is_overlap(&self, lower: &Bound<&[u8]>, upper: &Bound<&[u8]>) -> bool {
        match upper {
            Bound::Included(b) if b.as_bytes() < self.first_key.raw_ref() => {
                return false;
            }
            Bound::Excluded(b) if b.as_bytes() <= self.first_key.raw_ref() => {
                return false;
            }
            _ => {}
        }

        match lower {
            Bound::Included(b) if b.as_bytes() > self.last_key.raw_ref() => {
                return false;
            }
            Bound::Excluded(b) if b.as_bytes() >= self.last_key.raw_ref() => {
                return false;
            }
            _ => {}
        }

        true
    }

    pub fn may_contain(&self, key: KeySlice) -> bool {
        if self.bloom.is_some() {
            let bloom = self.bloom.as_ref().unwrap();
            return bloom.may_contain(farmhash::fingerprint32(key.raw_ref()));
        }
        true
    }
}
