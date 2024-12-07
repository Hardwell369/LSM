pub(crate) mod bloom;
mod builder;
mod iterator;

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Ok, Result};
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut};
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        // buf: len (4 bytes)
        buf.put_u32(block_meta.len() as u32);
        // buf: offset (4 bytes) | first_key_len (2 bytes) | first_key | last_key_len (2 bytes) | last_key
        for meta in block_meta {
            buf.put_u32(meta.offset as u32);
            buf.put_u16(meta.first_key.len() as u16);
            buf.extend_from_slice(meta.first_key.raw_ref());
            buf.put_u16(meta.last_key.len() as u16);
            buf.extend_from_slice(meta.last_key.raw_ref());
        }
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: &[u8]) -> Vec<BlockMeta> {
        let num_of_block_meta = buf.get_u32() as usize;
        let mut res = Vec::with_capacity(num_of_block_meta);
        for _ in 0..num_of_block_meta {
            let offset = buf.get_u32() as usize;
            let first_key_len = buf.get_u16() as usize;
            let first_key = buf.copy_to_bytes(first_key_len);
            let last_key_len = buf.get_u16() as usize;
            let last_key = buf.copy_to_bytes(last_key_len);
            res.push(BlockMeta {
                offset,
                first_key: KeyBytes::from_bytes(first_key),
                last_key: KeyBytes::from_bytes(last_key),
            });
        }
        res
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    // SST File: data blocks | meta blocks | block meta offset(u32) | bloom filter | bloom filter offset(u32)
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        /* decode bloom filter */
        let mut file_size = file.size();
        let bloom_filter_offset = (&file.read(file_size - 4, 4)?[..]).get_u32() as usize;
        let bloom_filter = if bloom_filter_offset > 0 {
            let bloom_filter_buf = &file.read(
                bloom_filter_offset as u64,
                file_size - bloom_filter_offset as u64 - 4,
            )?;
            Some(Bloom::decode(bloom_filter_buf)?)
        } else {
            None
        };
        /* decode block meta */
        file_size = bloom_filter_offset as u64;
        let block_meta_offset = (&file.read(file_size - 4, 4)?[..]).get_u32() as usize;
        let block_meta_len = file_size - block_meta_offset as u64 - 4;
        let meta_buf = &file.read(block_meta_offset as u64, block_meta_len)?[..];
        let block_meta = BlockMeta::decode_block_meta(meta_buf);
        let first_key = block_meta
            .first()
            .map(|x| x.first_key.clone())
            .unwrap_or_default();
        let last_key = block_meta
            .last()
            .map(|x| x.last_key.clone())
            .unwrap_or_default();

        Ok(Self {
            file,
            block_meta,
            block_meta_offset,
            id,
            block_cache,
            first_key,
            last_key,
            bloom: bloom_filter,
            max_ts: 0,
        })
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        if block_idx >= self.block_meta.len() {
            return Err(anyhow::anyhow!("Block index out of range"));
        }
        let meta = &self.block_meta[block_idx];
        let start_offset = meta.offset;
        let mut end_offset = self.block_meta_offset;
        if block_idx + 1 < self.block_meta.len() {
            let next_meta = &self.block_meta[block_idx + 1];
            end_offset = next_meta.offset;
        }
        let data = self
            .file
            .read(start_offset as u64, end_offset as u64 - start_offset as u64)?;
        Ok(Arc::new(Block::decode(&data[..])))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        // cache: (sst_id, block_idx) -> block
        // try_get_with: 根据(sst_id, block_idx)查找缓存，如果没有则调用闭包函数，然后将函数运行的结果存入缓存
        if let Some(cache) = &self.block_cache {
            if let std::result::Result::Ok(block) =
                cache.try_get_with((self.sst_id(), block_idx), || self.read_block(block_idx))
            {
                return Ok(block);
            }
        }
        let block = self.read_block(block_idx)?;
        Ok(block)
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    // 该函数可能会返回一个不存在的索引，该索引通常是最后一个数据块的索引+1
    // 在调用时，需要在外部判断是否越界
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        // 二分查找
        let mut left = 0;
        let mut right = self.block_meta.len() - 1;
        while left < right {
            let mid = (left + right) / 2;
            let mid_key = &self.block_meta[mid].first_key;
            if mid_key.as_key_slice() < key {
                if key <= self.block_meta[mid].last_key.as_key_slice() {
                    return mid;
                }
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        // let left_key = &self.block_meta[left].first_key;
        let right_key = &self.block_meta[left].last_key;
        if key > right_key.as_key_slice() {
            left + 1
        } else {
            left
        }
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
}
