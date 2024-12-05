#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::Bytes;
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        // 格式：data | offsets | num_of_entries
        let mut encoded: Vec<u8> = vec![];
        let num_of_entries = self.offsets.len() as u16;
        encoded.extend_from_slice(&self.data[..]);
        for offset in &self.offsets {
            encoded.extend_from_slice(&offset.to_be_bytes());
        }
        encoded.extend_from_slice(&num_of_entries.to_be_bytes());
        Bytes::from(encoded)
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let data_len = data.len();
        let num_of_entries = u16::from_be_bytes([data[data_len - 2], data[data_len - 1]]);
        // 存储entries所用的空间长度 （总空间长度 - num_of_entries所用长度 - offsets所用长度）
        let entries_len = data_len - 2 - num_of_entries as usize * 2;

        let entries = data[..entries_len].to_vec();
        let mut offsets = Vec::with_capacity(num_of_entries as usize);

        for i in (entries_len..data_len - 2).step_by(2) {
            offsets.push(u16::from_be_bytes([data[i], data[i + 1]]));
        }

        Self {
            data: entries,
            offsets,
        }
    }
}
