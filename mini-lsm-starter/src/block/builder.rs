#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use bytes::Bytes;
use serde_json::to_vec;

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
        Self {
            offsets: vec![],
            data: vec![],
            block_size,
            first_key: KeyVec::default(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        if self.is_full(key, value) {
            return false;
        }

        if self.is_empty() {
            self.first_key = key.to_key_vec();
        }

        let key_len = key.len() as u16;
        let value_len = value.len() as u16;
        self.offsets.push(self.data.len() as u16);
        self.data.extend_from_slice(&key_len.to_be_bytes());
        self.data.extend_from_slice(key.raw_ref());
        self.data.extend_from_slice(&value_len.to_be_bytes());
        self.data.extend_from_slice(value);
        true
    }

    // 检查block是否已满
    fn is_full(&self, key: KeySlice, value: &[u8]) -> bool {
        // 如果block为空，直接返回false
        // 即第一次添加key-value时，不会判断是否超过block size
        if self.is_empty() {
            return false;
        }
        // 2 * self.offsets.len() 是因为每个offset是u16，占2个字节
        // key.len() + value.len() + 4 是因为key和value的本身长度加上固定4个字节的key_len与value_len
        self.data.len() + self.offsets.len() * 2 + key.len() + value.len() + 4 > self.block_size
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn first_key(&self) -> Bytes {
        if self.is_empty() {
            Bytes::new()
        } else {
            let offset = self.offsets.first().copied().unwrap_or(0) as usize;
            let key_len = u16::from_be_bytes([self.data[offset], self.data[offset + 1]]) as usize;
            Bytes::copy_from_slice(&self.data[offset + 2..offset + 2 + key_len])
        }
    }

    pub fn last_key(&self) -> Bytes {
        if self.is_empty() {
            Bytes::new()
        } else {
            let offset = self.offsets.last().copied().unwrap_or(0) as usize;
            let key_len = u16::from_be_bytes([self.data[offset], self.data[offset + 1]]) as usize;
            Bytes::copy_from_slice(&self.data[offset + 2..offset + 2 + key_len])
        }
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
