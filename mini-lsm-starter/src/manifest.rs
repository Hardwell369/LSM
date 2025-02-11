use bytes::{Buf, BufMut};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};

use crate::compact::CompactionTask;

pub struct Manifest {
    file: Arc<Mutex<File>>,
}

#[derive(Serialize, Deserialize)]
pub enum ManifestRecord {
    Flush(usize),
    NewMemtable(usize),
    Compaction(CompactionTask, Vec<usize>),
}

impl Manifest {
    pub fn create(_path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(
                OpenOptions::new()
                    .read(true)
                    .create_new(true)
                    .write(true)
                    .open(_path)
                    .context("failed to create manifest file")?,
            )),
        })
    }

    pub fn recover(_path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(_path)
            .context("failed to open manifest file")?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut buf_slice = buf.as_slice();
        let mut records = Vec::new();
        while buf_slice.has_remaining() {
            let record_size = buf_slice.get_u64() as usize;
            let record_slice = buf_slice.copy_to_bytes(record_size);
            let record = serde_json::from_slice::<ManifestRecord>(&record_slice)?;
            let checksum = buf_slice.get_u32();
            let checksum_calculated = crc32fast::hash(&record_slice);
            if checksum != checksum_calculated {
                bail!("Checksum error while Manifest decoding");
            }
            records.push(record);
        }
        Ok((
            Self {
                file: Arc::new(Mutex::new(file)),
            },
            records,
        ))
    }

    pub fn add_record(
        &self,
        _state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord,
    ) -> Result<()> {
        self.add_record_when_init(record)
    }

    pub fn add_record_when_init(&self, _record: ManifestRecord) -> Result<()> {
        // each record: record_size(u64) | record | checksum(u32)
        let mut file = self.file.lock();
        let mut buf = serde_json::to_vec(&_record)?;
        let record_size = buf.len() as u64;
        // 计算并添加 checksum
        let checksum = crc32fast::hash(&buf);
        buf.put_u32(checksum);
        file.write_all(&record_size.to_be_bytes())?;
        file.write_all(&buf)?;
        // 避免操作系统缓存，立即将数据写入磁盘
        file.sync_all()?;
        Ok(())
    }
}
