use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
    pub max_merge_width: Option<usize>,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        _snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        // 触发条件1: 当前的tier数量大于设定的num_tiers
        let num_tiers = _snapshot.levels.len();
        if num_tiers < self.options.num_tiers {
            return None;
        }
        // 触发条件2: space amplification ratio
        // space amplification formula: all levels except the bottom level / bottom level
        let mut space_amplification = 0.0;
        for i in 0..num_tiers - 1 {
            space_amplification += _snapshot.levels[i].1.len() as f64;
        }
        space_amplification /= _snapshot.levels[num_tiers - 1].1.len() as f64;
        if space_amplification >= self.options.max_size_amplification_percent as f64 {
            return Some(TieredCompactionTask {
                tiers: _snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        };
        // 触发条件3: size ratio
        let mut prev_tiers_size_sum = 0;
        for (i, level) in _snapshot.levels.iter().enumerate() {
            if i + 1 < self.options.min_merge_width {
                prev_tiers_size_sum += level.1.len();
                continue;
            }
            let current_tier_size = level.1.len();
            let size_ratio = prev_tiers_size_sum as f64 / current_tier_size as f64;
            if size_ratio > ((self.options.size_ratio as f64 + 100.0f64) / 100.0f64) {
                let tiers = _snapshot.levels[0..=i].to_vec().clone();
                return Some(TieredCompactionTask {
                    tiers,
                    bottom_tier_included: i == num_tiers - 1,
                });
            }
            prev_tiers_size_sum += current_tier_size;
        }
        // 触发条件4: max merge width
        if _snapshot.levels.len() == self.options.num_tiers {
            return None;
        }
        let nums = _snapshot.levels.len() - self.options.num_tiers;
        let tiers = _snapshot.levels[0..=nums].to_vec().clone();
        Some(TieredCompactionTask {
            tiers,
            bottom_tier_included: false,
        })
    }

    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &TieredCompactionTask,
        _output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = _snapshot.clone();
        let tier_ids_needed_removal: Vec<usize> = _task.tiers.iter().map(|x| x.0).collect();
        let cutting_position = snapshot
            .levels
            .iter()
            .position(|x| x.0 == tier_ids_needed_removal[0])
            .expect("tier ids needed removal not found");

        snapshot.levels.splice(
            cutting_position..cutting_position + tier_ids_needed_removal.len(),
            std::iter::once((_output[0], _output.to_vec())),
        );

        let files_needed_removal: Vec<usize> =
            _task.tiers.iter().flat_map(|tier| tier.1.clone()).collect();

        (snapshot, files_needed_removal)
    }
}
