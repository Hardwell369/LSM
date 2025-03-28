use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::{BufMut, Bytes};

use self::bloom::Bloom;

use super::{bloom, BlockMeta, SsTable};
use crate::{
    block::BlockBuilder,
    key::{KeyBytes, KeySlice, KeyVec},
    lsm_storage::BlockCache,
    table::FileObject,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: KeyVec,
    last_key: KeyVec,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    hash_keys: Vec<u32>, // hash keys for bloom filter
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: KeyVec::new(),
            last_key: KeyVec::new(),
            data: vec![],
            meta: vec![],
            hash_keys: vec![],
            block_size,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if self.first_key.is_empty() {
            self.first_key.set_from_slice(key);
        }

        if !self.builder.add(key, value) {
            self.finish_block();
            let _ = self.builder.add(key, value);
            self.first_key.set_from_slice(key);
        }

        self.last_key.set_from_slice(key);
        self.hash_keys.push(farmhash::fingerprint32(key.key_ref()));
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        // Finish the last block
        if !self.builder.is_empty() {
            self.finish_block();
        }
        // SST File: data blocks (each block end with checksum[u32]) | meta blocks | block meta offset(u32) | bloom filter | bloom filter offset(u32)
        /* data blocks */
        let mut buf = self.data;
        /* meta blocks */
        let block_meta_offset = buf.len();
        BlockMeta::encode_block_meta(&self.meta, &mut buf);
        buf.put_u32(block_meta_offset as u32);
        /* bloom filter */
        let bloom_filter_offset = buf.len();
        let bloom_filter = bloom::Bloom::build_from_key_hashes(
            self.hash_keys.as_slice(),
            Bloom::bloom_bits_per_key(self.hash_keys.len(), 0.01),
        );
        bloom_filter.encode(&mut buf);
        buf.put_u32(bloom_filter_offset as u32);

        /* write to disk and generate SST file */
        let file = FileObject::create(path.as_ref(), buf)?;

        Ok(SsTable {
            file,
            block_meta_offset,
            id,
            block_cache,
            first_key: self.meta.first().unwrap().first_key.clone(),
            last_key: self.meta.last().unwrap().last_key.clone(),
            block_meta: self.meta,
            bloom: Some(bloom_filter),
            max_ts: 0,
        })
    }

    fn finish_block(&mut self) {
        let block =
            std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size)).build();
        self.meta.push(BlockMeta {
            offset: self.data.len(),
            first_key: std::mem::take(&mut self.first_key).into_key_bytes(),
            last_key: std::mem::take(&mut self.last_key).into_key_bytes(),
        });
        self.data.extend_from_slice(&block.encode());
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
