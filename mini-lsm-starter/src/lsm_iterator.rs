#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::ops::Bound;

use anyhow::Result;
use bytes::Bytes;
use nom::AsBytes;

use crate::{
    iterators::{
        merge_iterator::MergeIterator, two_merge_iterator::TwoMergeIterator, StorageIterator,
    },
    mem_table::MemTableIterator,
    table::SsTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
//type LsmIteratorInner = MergeIterator<MemTableIterator>;
type LsmIteratorInner =
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    end_bound: Bound<Bytes>,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner, end: Bound<Bytes>) -> Result<Self> {
        let mut iter = iter;
        loop {
            if iter.is_valid() {
                if iter.value().is_empty() {
                    match iter.next() {
                        Ok(_) => {
                            continue;
                        }
                        Err(e) => {
                            break;
                        }
                    }
                } else {
                    break;
                }
            } else {
                break;
            }
        }
        Ok(Self {
            inner: iter,
            end_bound: end,
        })
    }
}

fn key_to_bound(key: &[u8]) -> Bound<Bytes> {
    let bytes = Bytes::from(key.to_vec());
    Bound::Included(bytes)
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        let res = self.inner.is_valid();
        if res {
            let res2 = match self.end_bound.as_ref() {
                Bound::Unbounded => true,
                Bound::Excluded(bound) => self.key() < bound.as_bytes(),
                Bound::Included(bound) => self.key() <= bound.as_bytes(),
            };
            return res && res2;
        }
        res
    }

    fn key(&self) -> &[u8] {
        self.inner.key().raw_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        //self.inner.next()
        let mut res;
        loop {
            res = self.inner.next();
            match res {
                Ok(_) => {
                    if !self.inner.is_valid() {
                        return Ok(());
                    }
                    if self.value().is_empty() {
                        continue;
                    } else {
                        return Ok(());
                    }
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a> = I::KeyType<'a> where Self: 'a;

    fn is_valid(&self) -> bool {
        (!self.has_errored) && self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.is_valid() {
            match self.iter.next() {
                Ok(_) => return Ok(()),
                Err(_) => {
                    self.has_errored = true;
                    return Err(anyhow::Error::msg("fsdfasdf"));
                }
            }
        }
        if self.has_errored {
            Err(anyhow::Error::msg("abcd"))
        } else {
            Ok(())
        }
    }
}
