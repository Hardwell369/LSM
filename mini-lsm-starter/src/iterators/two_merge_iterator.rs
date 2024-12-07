#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use anyhow::Result;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    // indicate which iterator is currently being read
    is_a: bool,
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    pub fn create(a: A, b: B) -> Result<Self> {
        if !a.is_valid() {
            return Ok(Self { a, b, is_a: false });
        }
        if !b.is_valid() {
            return Ok(Self { a, b, is_a: true });
        }
        let is_a = a.key() <= b.key();
        Ok(Self { a, b, is_a })
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        if self.is_a {
            self.a.key()
        } else {
            self.b.key()
        }
    }

    fn value(&self) -> &[u8] {
        if self.is_a {
            self.a.value()
        } else {
            self.b.value()
        }
    }

    fn is_valid(&self) -> bool {
        if self.is_a {
            self.a.is_valid()
        } else {
            self.b.is_valid()
        }
    }

    fn next(&mut self) -> Result<()> {
        // skip the same key
        if self.a.is_valid() && self.b.is_valid() && self.a.key() == self.b.key() {
            self.b.next()?;
        }
        // move the iterator
        if self.is_a {
            self.a.next()?;
        } else {
            self.b.next()?;
        }
        // update is_a
        if !self.a.is_valid() {
            self.is_a = false;
        } else if !self.b.is_valid() {
            self.is_a = true;
        } else {
            self.is_a = self.a.key() <= self.b.key();
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        let a_is_valid = self.a.is_valid();
        let b_is_valid = self.b.is_valid();

        if a_is_valid && b_is_valid {
            self.a.num_active_iterators() + self.b.num_active_iterators()
        } else if a_is_valid && !b_is_valid {
            self.a.num_active_iterators()
        } else if !a_is_valid && b_is_valid {
            self.b.num_active_iterators()
        } else {
            0
        }
    }
}
