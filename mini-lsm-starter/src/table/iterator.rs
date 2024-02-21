use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;

use super::{BlockMeta, SsTable};
use crate::{
    block::BlockIterator,
    iterators::StorageIterator,
    key::{KeyBytes, KeySlice},
};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

fn hit_block(meta: &[BlockMeta], i: usize, key: KeySlice) -> i32 {
    let kb = KeyBytes::from_bytes(Bytes::from(key.raw_ref().to_vec()));

    if i == 0 {
        if kb <= meta.first().unwrap().last_key {
            return 0;
        } else {
            return 1;
        }
    }
    if kb > meta[i - 1].last_key && kb <= meta[i].last_key {
        0
    } else if kb <= meta[i - 1].last_key {
        return -1;
    } else {
        return 1;
    }
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let block = table.read_block_cached(0)?;
        let blk_iter = BlockIterator::create_and_seek_to_first(block);
        let iter = SsTableIterator {
            table,
            blk_iter,
            blk_idx: 0,
        };
        Ok(iter)
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        *self = SsTableIterator::create_and_seek_to_first(self.table.clone())?;
        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let num = table.num_of_blocks();
        let mut low = 0;
        let mut high = num - 1;
        let mut res = -1;
        while low <= high {
            let mid = (low + high) / 2;
            let ans = hit_block(&table.block_meta, mid, key);
            if ans == 0 {
                res = mid as i64;
                break;
            } else if ans < 0 {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }

        if res < 0 {
            res = num as i64 - 1;
        }
        let block = table.read_block_cached(res as usize)?;
        let blk_iter = BlockIterator::create_and_seek_to_key(block, key);
        let iter = SsTableIterator {
            table,
            blk_idx: res as usize,
            blk_iter,
        };
        Ok(iter)
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        *self = SsTableIterator::create_and_seek_to_key(self.table.clone(), key)?;
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        self.blk_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        self.blk_iter.next();
        if !self.blk_iter.is_valid() {
            if let Ok(blk) = self.table.read_block_cached(self.blk_idx + 1) {
                self.blk_iter = BlockIterator::create_and_seek_to_first(blk);
                self.blk_idx += 1;
            }
        }
        Ok(())
    }
}
