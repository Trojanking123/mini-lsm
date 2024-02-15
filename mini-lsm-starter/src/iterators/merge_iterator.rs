#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::{self};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;

#[derive(Clone)]
struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap() == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    #[allow(clippy::non_canonical_partial_ord_impl)]
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        match self.1.key().cmp(&other.1.key()) {
            cmp::Ordering::Greater => Some(cmp::Ordering::Greater),
            cmp::Ordering::Less => Some(cmp::Ordering::Less),
            cmp::Ordering::Equal => self.0.partial_cmp(&other.0),
        }
        .map(|x| x.reverse())
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut i = 0;
        let mut iters = iters;
        let total_len = iters.len();
        let mut heap = BinaryHeap::new();
        while let Some(item) = iters.pop() {
            if item.is_valid() {
                let hw = HeapWrapper(total_len - i - 1, item);
                heap.push(hw);
                i += 1;
            }
        }
        let cur = heap.pop();
        let merged_iter = MergeIterator {
            iters: heap,
            current: cur,
        };
        merged_iter
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        !self.current.as_ref().is_none()
    }

    fn next(&mut self) -> Result<()> {
        let mut cur = std::mem::replace(&mut self.current, None).unwrap();
        while let Some(mut it) = self.iters.peek_mut() {
            if it.1.key() == cur.1.key() {
                match it.1.next() {
                    Err(e) => return Err(e),
                    Ok(_) => {}
                };
                if !it.1.is_valid() {
                    PeekMut::pop(it);
                }
            } else if it.1.key() > cur.1.key() {
                break;
            }
        }

        match cur.1.next() {
            Err(e) => return Err(e),
            Ok(_) => {}
        };
        if cur.1.is_valid() {
            self.iters.push(cur);
        }

        let cur = self.iters.pop();
        let _ = std::mem::replace(&mut self.current, cur);
        Ok(())
    }
}
