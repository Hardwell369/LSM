use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn find_overlapping_ssts(
        &self,
        _snapshot: &LsmStorageState,
        _sst_ids: &[usize],
        _in_level: usize,
    ) -> Vec<usize> {
        let mut overlapping_ssts = Vec::new();
        let first_key = _sst_ids
            .iter()
            .map(|x| _snapshot.sstables.get(x).unwrap().first_key().clone())
            .min()
            .unwrap();
        let last_key = _sst_ids
            .iter()
            .map(|x| _snapshot.sstables.get(x).unwrap().last_key().clone())
            .max()
            .unwrap();
        for sst_id in _snapshot.levels[_in_level - 1].1.iter() {
            let level_sst = _snapshot.sstables.get(sst_id).unwrap();
            if last_key < level_sst.first_key().clone() {
                break;
            }
            if first_key > level_sst.last_key().clone() {
                continue;
            }
            overlapping_ssts.push(*sst_id);
        }
        overlapping_ssts
    }

    pub fn generate_compaction_task(
        &self,
        _snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        let max_level_num = self.options.max_levels;
        // 1 .. max_level_num, level0 is not included
        let mut target_level_size = vec![0; max_level_num];
        // Calculate the size of last level
        let last_level_size = _snapshot.levels[self.options.max_levels - 1]
            .1
            .iter()
            .map(|x| _snapshot.sstables.get(x).unwrap().table_size())
            .sum::<u64>() as usize;
        target_level_size[max_level_num - 1] = last_level_size;
        let mut base_level = max_level_num - 1;
        // Calculate the target size of each level based on the size of last level and level_size_multiplier
        // 每一层的目标大小是下一层的大小除以level_size_multiplier
        for i in (0..max_level_num - 2).rev() {
            if target_level_size[i + 1] < self.options.base_level_size_mb * 1024 * 1024 {
                break;
            }
            target_level_size[i] = target_level_size[i + 1] / self.options.level_size_multiplier;
            base_level = i;
        }
        // 触发条件1: l0_sstables数量大于level0_file_num_compaction_trigger
        if _snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: _snapshot.l0_sstables.clone(),
                lower_level: base_level + 1,
                lower_level_sst_ids: self.find_overlapping_ssts(
                    _snapshot,
                    &_snapshot.l0_sstables,
                    base_level + 1,
                ),
                is_lower_level_bottom_level: base_level + 1 == max_level_num,
            });
        }
        // 触发条件2： current_size / target_size > 1.0 并且当前层的ratio最大，合并当前层和下一层
        let mut current_level_size = Vec::with_capacity(max_level_num);
        for i in 0..max_level_num - 1 {
            current_level_size.push(
                _snapshot.levels[i]
                    .1
                    .iter()
                    .map(|x| _snapshot.sstables.get(x).unwrap().table_size())
                    .sum::<u64>() as usize,
            );
        }
        let mut max_ratio = 0.0;
        let mut max_ratio_level = 0;
        for i in 0..max_level_num - 1 {
            let ratio = current_level_size[i] as f64 / target_level_size[i] as f64;
            if ratio > max_ratio {
                max_ratio = ratio;
                max_ratio_level = i;
            }
        }
        max_ratio_level += 1;
        if max_ratio > 1.0 {
            return Some(LeveledCompactionTask {
                upper_level: Some(max_ratio_level),
                upper_level_sst_ids: _snapshot.levels[max_ratio_level - 1].1.clone(),
                lower_level: max_ratio_level + 1,
                lower_level_sst_ids: self.find_overlapping_ssts(
                    _snapshot,
                    &_snapshot.levels[max_ratio_level - 1].1,
                    max_ratio_level + 1,
                ),
                is_lower_level_bottom_level: max_ratio_level + 1 == max_level_num,
            });
        }
        None
    }

    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &LeveledCompactionTask,
        _output: &[usize],
        _in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = _snapshot.clone();
        let mut files_needed_removal: HashSet<usize> = HashSet::new();

        // Collect SST IDs to be removed
        files_needed_removal.extend(&_task.upper_level_sst_ids);
        files_needed_removal.extend(&_task.lower_level_sst_ids);

        // Remove SST IDs from the upper level
        if let Some(upper_level) = _task.upper_level {
            snapshot.levels[upper_level - 1]
                .1
                .retain(|x| !files_needed_removal.contains(x));
        } else {
            snapshot
                .l0_sstables
                .retain(|x| !files_needed_removal.contains(x));
        }

        // Remove SST IDs from the lower level
        snapshot.levels[_task.lower_level - 1]
            .1
            .retain(|x| !files_needed_removal.contains(x));

        // Add new SST IDs to the lower level
        snapshot.levels[_task.lower_level - 1].1.extend(_output);
        // Sort SSTs in the level (only in not recovery mode)
        if !_in_recovery {
            snapshot.levels[_task.lower_level - 1]
                .1
                .sort_by_key(|a| snapshot.sstables.get(a).unwrap().first_key().clone());
        }

        (snapshot, files_needed_removal.into_iter().collect())
    }
}
