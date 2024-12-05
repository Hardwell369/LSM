#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::{self};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;
// use std::ops::Deref;

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut iters = iters
            .into_iter()
            .filter(|iter| iter.is_valid())
            .enumerate()
            .map(|(i, iter)| HeapWrapper(i, iter))
            .collect::<BinaryHeap<_>>();
        let current = iters.pop();
        MergeIterator { iters, current }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        !self.current.is_none() && self.current.as_ref().unwrap().1.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        let current = self.current.as_mut().unwrap();
        let key = current.1.key();
        // 移除与当前迭代器具有相同key的迭代器
        // 每次对peek_mut对象进行操作后，heap会自动重新排序
        while let Some(mut inner) = self.iters.peek_mut() {
            if inner.1.key() != key {
                break;
            }
            if let Err(e) = inner.1.next() {
                PeekMut::pop(inner);
                return Err(e);
            }
            if !inner.1.is_valid() {
                PeekMut::pop(inner);
            }
        }

        // 尝试移动当前迭代器
        current.1.next()?;

        // 如果当前迭代器不再有效，则将其替换为堆中的下一个迭代器
        if !current.1.is_valid() {
            *current = match self.iters.pop() {
                Some(inner) => inner,
                None => return Ok(()),
            };
        } else {
            // 将当前迭代器与堆中的最小迭代器进行比较
            // 如果当前迭代器大于最小迭代器，则交换两者
            if let Some(mut iter) = self.iters.peek_mut() {
                // 因为比较的结果是reverse的，所以这里使用大于号，但是实际上是小于号
                if *iter > *current {
                    std::mem::swap(&mut *iter, current);
                }
            }
        }

        Ok(())
    }
}
