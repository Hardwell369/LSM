mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::Bytes;
use crc32fast;
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    pub fn encode(&self) -> Bytes {
        // 格式：data | offsets(num_of_entries  *2) | num_of_entries(u16) | checksum(u32)
        let mut encoded: Vec<u8> = vec![];
        let num_of_entries = self.offsets.len() as u16;
        encoded.extend_from_slice(&self.data[..]);
        for offset in &self.offsets {
            encoded.extend_from_slice(&offset.to_be_bytes());
        }
        encoded.extend_from_slice(&num_of_entries.to_be_bytes());
        let checksum = crc32fast::hash(&encoded);
        encoded.extend_from_slice(&checksum.to_be_bytes());
        Bytes::from(encoded)
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let data_len = data.len();
        // 读取checksum
        let checksum = u32::from_be_bytes(data[data_len - 4..].try_into().unwrap());
        // 计算checksum
        let checksum_calculated = crc32fast::hash(&data[..data_len - 4]);
        if checksum != checksum_calculated {
            panic!("Checksum error while Block decoding");
        }
        // 读取 num_of_entries
        let num_of_entries =
            u16::from_be_bytes(data[data_len - 6..data_len - 4].try_into().unwrap());
        // 计算entries和offsets的长度
        let offset_len = num_of_entries as usize * 2;
        let entries_len = data_len - 6 - offset_len;

        let entries = data[..entries_len].to_vec();
        let offsets = (entries_len..data_len - 6)
            .step_by(2)
            .map(|i| u16::from_be_bytes(data[i..i + 2].try_into().unwrap()))
            .collect();

        Self {
            data: entries,
            offsets,
        }
    }
}
