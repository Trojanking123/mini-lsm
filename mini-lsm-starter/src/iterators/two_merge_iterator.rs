#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::Ordering;

use anyhow::Result;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    which: u8,
    // Add fields as need
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    pub fn create(a: A, b: B) -> Result<Self> {
        let which = match (a.is_valid(), b.is_valid()) {
            (true, true) => match a.key().cmp(&b.key()) {
                Ordering::Greater => 2,
                Ordering::Equal => 1,
                Ordering::Less => 1,
            },
            (true, false) => 1,
            (false, true) => 2,
            (false, false) => 0,
        };

        let iter = TwoMergeIterator { a, b, which };

        Ok(iter)
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        match self.which {
            0 => self.a.key(),
            1 => self.a.key(),
            2 => self.b.key(),
            _ => unreachable!("invalid which"),
        }
    }

    fn value(&self) -> &[u8] {
        match self.which {
            0 => self.a.value(),
            1 => self.a.value(),
            2 => self.b.value(),
            _ => unreachable!("invalid which"),
        }
    }

    fn is_valid(&self) -> bool {
        matches!(self.which, 1..=2)
    }

    fn next(&mut self) -> Result<()> {
        match self.which {
            1 => {
                if self.b.is_valid() && self.a.key() == self.b.key() {
                    self.b.next()?;
                }
                self.a.next()?
            }
            2 => {
                if self.a.is_valid() && self.a.key() == self.b.key() {
                    self.a.next()?;
                }
                self.b.next()?
            }
            _ => return Err(anyhow::Error::msg("invalid next which")),
        };

        let which = match (self.a.is_valid(), self.b.is_valid()) {
            (true, true) => match self.a.key().cmp(&self.b.key()) {
                Ordering::Greater => 1,
                Ordering::Equal => 1,
                Ordering::Less => 2,
            },
            (true, false) => 1,
            (false, true) => 2,
            (false, false) => 0,
        };
        //dbg!(which);
        self.which = which;
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.a.num_active_iterators() + self.b.num_active_iterators()
    }
}
