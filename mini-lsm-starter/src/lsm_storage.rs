#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::collections::HashMap;
use std::ops::{Bound, Deref};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::{Ok, Result};
use bytes::Bytes;
use nom::Err;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::StorageIterator;
use crate::iterators::{merge_iterator::MergeIterator, two_merge_iterator::TwoMergeIterator};
use crate::key::KeySlice;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::Manifest;
use crate::mem_table::MemTable;
use crate::mvcc::LsmMvccInner;
use crate::table::SsTable;
use crate::table::SsTableIterator;

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        unimplemented!()
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        let state = LsmStorageState::create(&options);

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache: Arc::new(BlockCache::new(1024)),
            next_sst_id: AtomicUsize::new(1),
            compaction_controller,
            manifest: None,
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, _key: &[u8]) -> Result<Option<Bytes>> {
        // 从内存中查找
        {
            let state = self.state.read();
            // 从memtable中查找
            if let Some(value) = state.memtable.get(_key) {
                if value.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(value));
            }
            // 从imm_memtable（由新到旧排序）中查找
            for imm_memtable in state.imm_memtables.iter() {
                if let Some(value) = imm_memtable.get(_key) {
                    if value.is_empty() {
                        return Ok(None);
                    }
                    return Ok(Some(value));
                }
            }
        }
        // 从磁盘中查找
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };
        // 从l0_sstables（由新到旧排序）中查找
        for sst_id in snapshot.l0_sstables.iter() {
            let sst = snapshot.sstables.get(sst_id).unwrap().clone();
            let first_key = sst.first_key().as_key_slice().raw_ref();
            let last_key = sst.last_key().as_key_slice().raw_ref();
            // 判断用户指定的key是否在sstable的范围内
            // 只有在范围内才进一步去寻找该key
            if _key >= first_key && _key <= last_key {
                let sst_iter =
                    SsTableIterator::create_and_seek_to_key(sst, KeySlice::from_slice(_key))?;
                if sst_iter.is_valid() && sst_iter.key().raw_ref() == _key {
                    if sst_iter.value().is_empty() {
                        return Ok(None);
                    }
                    return Ok(Some(Bytes::copy_from_slice(sst_iter.value())));
                }
            }
        }
        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        if self.get_approximate_size() + _key.len() + _value.len() > self.options.target_sst_size {
            // 添加唯一的写锁，防止其他线程同时执行freeze操作
            let state_lock = self.state_lock.lock();
            // 重新进行判定，因为在获取唯一锁的过程中，可能有其他线程已经执行了freeze操作
            if self.get_approximate_size() + _key.len() + _value.len()
                > self.options.target_sst_size
            {
                self.force_freeze_memtable(&state_lock)?;
            }
        }
        // 确定不需要freeze memtable后，再次获取读锁，执行put操作
        let state = self.state.read();
        state.memtable.put(_key, _value)
    }

    fn get_approximate_size(&self) -> usize {
        // 获取读锁，在读取完memtable的大小后，立即释放读锁，避免阻塞其他要执行freeze操作的线程
        let state = self.state.read();
        state.memtable.approximate_size()
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, _key: &[u8]) -> Result<()> {
        self.put(_key, &[])
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        unimplemented!()
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let new_mem_table = Arc::new(MemTable::create(self.next_sst_id()));
        {
            let mut state = self.state.write();
            let mut state_copy = state.as_ref().clone();
            let old_memtable = std::mem::replace(&mut state_copy.memtable, new_mem_table.clone());
            state_copy.imm_memtables.insert(0, old_memtable);
            *state = Arc::new(state_copy);
        }
        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        _lower: Bound<&[u8]>,
        _upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        // 依据范围，获取memtable和imm_memtable的迭代器
        let mut mem_iters = Vec::new();
        {
            let state = self.state.read();
            let memtable_iter = state.memtable.scan(_lower, _upper);
            mem_iters.push(Box::new(memtable_iter));
            for imm_memtable in state.imm_memtables.iter() {
                let imm_memtable_iter = imm_memtable.scan(_lower, _upper);
                mem_iters.push(Box::new(imm_memtable_iter));
            }
        }

        // 依据范围，获取l0_sstables的迭代器
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };
        let mut sst_iters = Vec::with_capacity(snapshot.l0_sstables.len());
        for sst_id in snapshot.l0_sstables.iter() {
            // 判断每一个sstable是否与用户指定的范围有交集
            // 如果没有交集，则不需要创建迭代器，直接跳过
            // 如果有交集，则创建迭代器，且根据用户指定的范围进行seek操作
            let sst = snapshot.sstables.get(sst_id).unwrap().clone();
            if self.range_overlap(
                _lower,
                _upper,
                sst.first_key().as_key_slice(),
                sst.last_key().as_key_slice(),
            ) {
                let sst_iter = match _lower {
                    Bound::Included(key) => {
                        SsTableIterator::create_and_seek_to_key(sst, KeySlice::from_slice(key))?
                    }
                    Bound::Excluded(key) => {
                        let mut iter = SsTableIterator::create_and_seek_to_key(
                            sst,
                            KeySlice::from_slice(key),
                        )?;
                        if iter.is_valid() && iter.key().raw_ref() == key {
                            iter.next()?;
                        }
                        iter
                    }
                    Bound::Unbounded => SsTableIterator::create_and_seek_to_first(sst)?,
                };
                sst_iters.push(Box::new(sst_iter));
            }
        }

        let memtable_merge_iter = MergeIterator::create(mem_iters);
        let sst_merge_iter = MergeIterator::create(sst_iters);
        let lsm_inner_iter = TwoMergeIterator::create(memtable_merge_iter, sst_merge_iter)?;
        let end_bound = _upper.map(|b| Bytes::copy_from_slice(b));
        let lsm_iter = LsmIterator::new(lsm_inner_iter, end_bound)?;
        Ok(FusedIterator::new(lsm_iter))
    }

    // 判断用户指定的范围（lower, upper）是否与Sstable的范围是否有交集
    fn range_overlap(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        sst_first_key: KeySlice,
        sst_last_key: KeySlice,
    ) -> bool {
        match upper {
            Bound::Excluded(key) if key <= sst_first_key.raw_ref() => return false,
            Bound::Included(key) if key < sst_first_key.raw_ref() => return false,
            _ => {}
        }
        match lower {
            Bound::Excluded(key) if key >= sst_last_key.raw_ref() => return false,
            Bound::Included(key) if key > sst_last_key.raw_ref() => return false,
            _ => {}
        }
        true
    }
}
