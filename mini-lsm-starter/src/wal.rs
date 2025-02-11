#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::fs::{File, OpenOptions};
use std::hash::Hasher;
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(_path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(
                OpenOptions::new()
                    .read(true)
                    .create_new(true)
                    .write(true)
                    .open(_path)
                    .context("failed to create wal file")?,
            ))),
        })
    }

    pub fn recover(_path: impl AsRef<Path>, _skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        let path = _path.as_ref();
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)
            .context("failed to open wal file")?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        // wal decode
        let mut rbuf = buf.as_slice();
        while rbuf.has_remaining() {
            let key_len = rbuf.get_u16() as usize;
            let key = rbuf.copy_to_bytes(key_len);
            let value_len = rbuf.get_u16() as usize;
            let value = rbuf.copy_to_bytes(value_len);
            let checksum = rbuf.get_u32();
            let mut hasher = crc32fast::Hasher::new();
            hasher.write_u16(key_len as u16);
            hasher.write(&key);
            hasher.write_u16(value_len as u16);
            hasher.write(&value);
            if checksum != hasher.finalize() {
                bail!("Checksum error while Wal decoding");
            }
            _skiplist.insert(key, value);
        }
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    // WAL data: key_size(2 bytes) | key | value_size(2 bytes) | value
    pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        let mut buf = Vec::new();
        let mut hasher = crc32fast::Hasher::new();
        // wal encode
        let key_len = _key.len() as u16;
        buf.put_u16(key_len);
        hasher.write_u16(key_len);
        buf.put_slice(_key);
        hasher.write(_key);
        let value_len = _value.len() as u16;
        buf.put_u16(value_len);
        hasher.write_u16(value_len);
        buf.put_slice(_value);
        hasher.write(_value);
        // add checksum
        let checksum = hasher.finalize();
        buf.put_u32(checksum);
        let mut file = self.file.lock();
        file.write_all(&buf)?;
        Ok(())
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, _data: &[(&[u8], &[u8])]) -> Result<()> {
        unimplemented!()
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_all()?;
        Ok(())
    }
}
