use bytes::Bytes;

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
            // The first key in the block.
            // overlap_size = 0 (u16) | rest_key_len (u16) | key (rest_key_len) | value_len (u16) | value (value_len)
            self.first_key = key.to_key_vec();
        }

        let overlap_size = if self.is_empty() {
            0
        } else {
            self.compute_key_prefix_size(key)
        };
        let rest_key_len = (key.len() - overlap_size) as u16;
        let value_len = value.len() as u16;
        self.offsets.push(self.data.len() as u16);
        // data: key_overlap_len (u16) | rest_key_len (u16) | key (rest_key_len) | value_len (u16) | value (value_len)
        self.data
            .extend_from_slice(&(overlap_size as u16).to_be_bytes());
        self.data.extend_from_slice(&rest_key_len.to_be_bytes());
        self.data.extend_from_slice(&key.raw_ref()[overlap_size..]);
        self.data.extend_from_slice(&value_len.to_be_bytes());
        self.data.extend_from_slice(value);
        true
    }

    fn compute_key_prefix_size(&self, key: KeySlice) -> usize {
        let mut prefix_size = 0;
        loop {
            if prefix_size >= self.first_key.len() || prefix_size >= key.len() {
                break;
            }
            if self.first_key.raw_ref()[prefix_size] != key.raw_ref()[prefix_size] {
                break;
            }
            prefix_size += 1;
        }
        prefix_size
    }

    // 检查block是否已满
    fn is_full(&self, key: KeySlice, value: &[u8]) -> bool {
        // 如果block为空，直接返回false
        // 即第一次添加key-value时，不会判断是否超过block size
        if self.is_empty() {
            return false;
        }
        // 2 * self.offsets.len() 是因为每个offset是u16，占2个字节
        let overlap_size = self.compute_key_prefix_size(key);
        let rest_key_len = key.len() - overlap_size;
        self.data.len() + self.offsets.len() * 2 + 4 + rest_key_len + 2 + value.len()
            > self.block_size
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
            let key_len =
                u16::from_be_bytes([self.data[offset + 2], self.data[offset + 3]]) as usize;
            Bytes::copy_from_slice(&self.data[offset + 4..offset + 4 + key_len])
        }
    }

    pub fn last_key(&self) -> Bytes {
        if self.is_empty() {
            Bytes::new()
        } else if self.offsets.len() == 1 {
            self.first_key.raw_ref().to_vec().into()
        } else {
            let offset = self.offsets.last().copied().unwrap_or(0) as usize;
            let overlap_size =
                u16::from_be_bytes([self.data[offset], self.data[offset + 1]]) as usize;
            let rest_key_len =
                u16::from_be_bytes([self.data[offset + 2], self.data[offset + 3]]) as usize;
            let rest_key =
                Bytes::copy_from_slice(&self.data[offset + 4..offset + 4 + rest_key_len]);
            self.first_key.raw_ref()[..overlap_size]
                .iter()
                .chain(rest_key.iter())
                .copied()
                .collect()
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
