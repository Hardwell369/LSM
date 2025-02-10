#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

mod leveled;
mod simple_leveled;
mod tiered;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Ok, Result};
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::StorageIterator;
use crate::key::Key;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::manifest::ManifestRecord;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact(&self, _task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = {
            let state = self.state.read();
            state.clone()
        };
        match _task {
            CompactionTask::Leveled(task) => {
                if task.upper_level.is_none() {
                    let new_task = CompactionTask::ForceFullCompaction {
                        l0_sstables: task.upper_level_sst_ids.clone(),
                        l1_sstables: task.lower_level_sst_ids.clone(),
                    };
                    return self.compact(&new_task);
                }
                // create concat iterator for upper level and lower level
                let mut upper_ssts = Vec::with_capacity(task.upper_level_sst_ids.len());
                for sst_id in task.upper_level_sst_ids.iter() {
                    upper_ssts.push(snapshot.sstables.get(sst_id).unwrap().clone());
                }
                let upper_concat_iter = SstConcatIterator::create_and_seek_to_first(upper_ssts)?;
                let mut lower_ssts = Vec::with_capacity(task.lower_level_sst_ids.len());
                for sst_id in task.lower_level_sst_ids.iter() {
                    lower_ssts.push(snapshot.sstables.get(sst_id).unwrap().clone());
                }
                let lower_concat_iter = SstConcatIterator::create_and_seek_to_first(lower_ssts)?;
                // merge upper and lower level
                self.merge_two_level(upper_concat_iter, lower_concat_iter)
            }
            CompactionTask::Tiered(task) => {
                let mut iters = Vec::with_capacity(task.tiers.len());
                for (_, sst_ids) in task.tiers.iter() {
                    let mut ssts = Vec::with_capacity(sst_ids.len());
                    for sst_id in sst_ids.iter() {
                        ssts.push(snapshot.sstables.get(sst_id).unwrap().clone());
                    }
                    iters.push(Box::new(SstConcatIterator::create_and_seek_to_first(ssts)?));
                }
                let merge_iter = MergeIterator::create(iters);
                self.merge_two_level(
                    merge_iter,
                    SstConcatIterator::create_and_seek_to_first(vec![])?,
                )
            }
            CompactionTask::Simple(task) => {
                if task.upper_level.is_none() {
                    let new_task = CompactionTask::ForceFullCompaction {
                        l0_sstables: task.upper_level_sst_ids.clone(),
                        l1_sstables: task.lower_level_sst_ids.clone(),
                    };
                    return self.compact(&new_task);
                }
                // create concat iterator for upper level and lower level
                let mut upper_ssts = Vec::with_capacity(task.upper_level_sst_ids.len());
                for sst_id in task.upper_level_sst_ids.iter() {
                    upper_ssts.push(snapshot.sstables.get(sst_id).unwrap().clone());
                }
                let upper_concat_iter = SstConcatIterator::create_and_seek_to_first(upper_ssts)?;
                let mut lower_ssts = Vec::with_capacity(task.lower_level_sst_ids.len());
                for sst_id in task.lower_level_sst_ids.iter() {
                    lower_ssts.push(snapshot.sstables.get(sst_id).unwrap().clone());
                }
                let lower_concat_iter = SstConcatIterator::create_and_seek_to_first(lower_ssts)?;
                // merge upper and lower level
                self.merge_two_level(upper_concat_iter, lower_concat_iter)
            }
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                // create merge iterator for L0
                let mut l0_iters = Vec::with_capacity(l0_sstables.len());
                for sst_id in l0_sstables.iter() {
                    l0_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                        snapshot.sstables.get(sst_id).unwrap().clone(),
                    )?));
                }
                let l0_merge_iter = MergeIterator::create(l0_iters);
                // create concat iterator for L1
                let mut l1_ssts = Vec::with_capacity(l1_sstables.len());
                for sst_id in l1_sstables.iter() {
                    l1_ssts.push(snapshot.sstables.get(sst_id).unwrap().clone());
                }
                let l1_concat_iter = SstConcatIterator::create_and_seek_to_first(l1_ssts)?;
                // merge L0 and L1
                self.merge_two_level(l0_merge_iter, l1_concat_iter)
            }
        }
    }

    // merge two level sstables, and return the new sstables
    fn merge_two_level<U, L>(
        &self,
        mut upper_level_iter: U,
        mut lower_level_iter: L,
    ) -> Result<Vec<Arc<SsTable>>>
    where
        U: for<'a> StorageIterator<KeyType<'a> = Key<&'a [u8]>>,
        L: for<'a> StorageIterator<KeyType<'a> = Key<&'a [u8]>>,
    {
        let mut result = Vec::new();
        let mut builder = SsTableBuilder::new(self.options.block_size);
        while upper_level_iter.is_valid() || lower_level_iter.is_valid() {
            if upper_level_iter.is_valid() && lower_level_iter.is_valid() {
                if upper_level_iter.key() <= lower_level_iter.key() {
                    let key = upper_level_iter.key();
                    let value = upper_level_iter.value();
                    if !value.is_empty() {
                        builder.add(key, value);
                    }
                    if upper_level_iter.key() == lower_level_iter.key() {
                        lower_level_iter.next()?;
                    }
                    upper_level_iter.next()?;
                } else {
                    let key = lower_level_iter.key();
                    let value = lower_level_iter.value();
                    if !value.is_empty() {
                        builder.add(key, value);
                    }
                    lower_level_iter.next()?;
                }
            } else if upper_level_iter.is_valid() {
                let key = upper_level_iter.key();
                let value = upper_level_iter.value();
                if !value.is_empty() {
                    builder.add(key, value);
                }
                upper_level_iter.next()?;
            } else {
                let key = lower_level_iter.key();
                let value = lower_level_iter.value();
                if !value.is_empty() {
                    builder.add(key, value);
                }
                lower_level_iter.next()?;
            }
            // if the builder is full, write to a new sstable
            if builder.estimated_size() >= self.options.target_sst_size {
                let new_sst_id = self.next_sst_id();
                let new_sst = builder.build(new_sst_id, None, self.path_of_sst(new_sst_id))?;
                result.push(Arc::new(new_sst));
                builder = SsTableBuilder::new(self.options.block_size);
            }
        }
        // write the remaining data to a new sstable
        let new_sst_id = self.next_sst_id();
        let new_sst = builder.build(new_sst_id, None, self.path_of_sst(new_sst_id))?;
        result.push(Arc::new(new_sst));
        Ok(result)
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let snapshot = {
            let state = self.state.read();
            state.clone()
        };

        // Generate a compaction task that compacts all SSTables in L0 and L1
        let l0_ssts = snapshot.l0_sstables.clone();
        let l1_ssts = snapshot.levels[0].1.clone();
        let compact_task = CompactionTask::ForceFullCompaction {
            l0_sstables: l0_ssts.clone(),
            l1_sstables: l1_ssts.clone(),
        };

        // do compaction
        let new_ssts = self.compact(&compact_task)?;

        {
            let mut state = self.state.write();
            let mut snapshot = state.as_ref().clone();

            // Remove all SSTables in L0 and L1
            for sst in l0_ssts.iter().chain(l1_ssts.iter()) {
                snapshot.sstables.remove(sst);
            }
            snapshot.l0_sstables.clear();
            snapshot.levels[0].1.clear();

            // Add new SSTables to L1, and insert into sstables
            for sst in new_ssts {
                snapshot.levels[0].1.push(sst.sst_id());
                snapshot.sstables.insert(sst.sst_id(), sst);
            }
            *state = Arc::new(snapshot);
        }
        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        let snapshot = {
            let state = self.state.read();
            state.clone()
        };
        let task = self
            .compaction_controller
            .generate_compaction_task(&snapshot);
        if let Some(task) = task {
            let output_ssts = self.compact(&task)?;
            let output = output_ssts.iter().map(|x| x.sst_id()).collect::<Vec<_>>();
            {
                let _state_lock = self.state_lock.lock();
                let mut snapshot = self.state.read().as_ref().clone();
                // add compact result
                for sst in output_ssts {
                    let _result = snapshot.sstables.insert(sst.sst_id(), sst);
                }
                // change snapshot
                let (mut snapshot, files_need_removal) = self
                    .compaction_controller
                    .apply_compaction_result(&snapshot, &task, &output, false);
                // delete old sstables
                for sst_id in &files_need_removal {
                    snapshot.sstables.remove(sst_id);
                    std::fs::remove_file(self.path_of_sst(*sst_id))?;
                }
                let mut state = self.state.write();
                *state = Arc::new(snapshot);
                drop(state);
                // write to manifest
                self.manifest
                    .as_ref()
                    .unwrap()
                    .add_record(&_state_lock, ManifestRecord::Compaction(task, output))?;
            }
        }
        self.sync_dir()?;
        Ok(())
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let num_memtables = {
            let state = self.state.read();
            1 + state.imm_memtables.len() // +1 for the current mutable memtable
        };

        if num_memtables > self.options.num_memtable_limit {
            self.force_flush_next_imm_memtable()?;
        }

        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
