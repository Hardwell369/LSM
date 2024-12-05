#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
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
        let mut iter = Self::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        KeySlice::from_slice(&self.key.raw_ref()[..])
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.idx = 0;
        self.update_key_value_range();
        self.first_key = self.key.clone();
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        if self.idx + 1 >= self.block.offsets.len() {
            self.key = KeyVec::new();
            return;
        }
        self.idx += 1;
        self.update_key_value_range();
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        if key.raw_ref() <= self.first_key.raw_ref() {
            self.seek_to_first();
            return;
        }
        let mut left = 0;
        let mut right = self.block.offsets.len() - 1;
        while left < right {
            let mid = (right + left) / 2;
            let mid_key = self.get_key_at(mid);
            if mid_key.raw_ref() < key.raw_ref() {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        if left >= self.block.offsets.len() {
            self.key = KeyVec::new();
            return;
        }
        self.idx = left;
        self.update_key_value_range();
    }

    /// Updates the current key and value range based on the current index.
    fn update_key_value_range(&mut self) {
        let entries_range_start = self.block.offsets[self.idx] as usize;
        let key_len = self.get_u16_at(entries_range_start);
        self.key = KeyVec::from_vec(
            self.block.data[entries_range_start + 2..entries_range_start + 2 + key_len as usize]
                .to_vec(),
        );
        let value_len = self.get_u16_at(entries_range_start + 2 + key_len as usize);
        let entries_range_end = entries_range_start + 2 + key_len as usize + 2 + value_len as usize;
        self.value_range = (
            entries_range_start + 2 + key_len as usize + 2,
            entries_range_end,
        );
    }

    /// Gets the key at the specified index.
    fn get_key_at(&self, index: usize) -> KeyVec {
        let entries_range_start = self.block.offsets[index] as usize;
        let key_len = self.get_u16_at(entries_range_start);
        KeyVec::from_vec(
            self.block.data[entries_range_start + 2..entries_range_start + 2 + key_len as usize]
                .to_vec(),
        )
    }

    /// Reads a u16 value from the block data at the specified position.
    fn get_u16_at(&self, pos: usize) -> u16 {
        ((self.block.data[pos] as u16) << 8) | (self.block.data[pos + 1] as u16)
    }
}
