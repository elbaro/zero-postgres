//! Buffer pool for reusing buffers across connections.

use std::mem::ManuallyDrop;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, LazyLock};

use crossbeam_queue::ArrayQueue;

use crate::BufferSet;

const POOL_CAPACITY: usize = 128;

/// Global buffer pool for reusing buffers across connections.
pub static GLOBAL_BUFFER_POOL: LazyLock<Arc<BufferPool>> =
    LazyLock::new(|| Arc::new(BufferPool::default()));

/// A pooled `BufferSet` that returns itself to the pool on drop.
pub struct PooledBufferSet {
    pool: Arc<BufferPool>,
    inner: ManuallyDrop<BufferSet>,
}

impl PooledBufferSet {
    fn new(pool: Arc<BufferPool>, buffer_set: BufferSet) -> Self {
        Self {
            pool,
            inner: ManuallyDrop::new(buffer_set),
        }
    }
}

impl Deref for PooledBufferSet {
    type Target = BufferSet;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for PooledBufferSet {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Drop for PooledBufferSet {
    fn drop(&mut self) {
        // SAFETY: inner is never accessed after this
        let buffer_set = unsafe { ManuallyDrop::take(&mut self.inner) };
        self.pool.return_buffer_set(buffer_set);
    }
}

/// Buffer pool for reusing `BufferSet` instances across connections.
#[derive(Debug)]
pub struct BufferPool {
    buffer_sets: ArrayQueue<BufferSet>,
}

impl BufferPool {
    /// Create a new buffer pool with the given capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer_sets: ArrayQueue::new(capacity),
        }
    }

    /// Get a buffer set from the pool, or create a new one if empty.
    pub fn get_buffer_set(self: &Arc<Self>) -> PooledBufferSet {
        let buffer_set = self.buffer_sets.pop().unwrap_or_default();
        PooledBufferSet::new(Arc::clone(self), buffer_set)
    }

    /// Return a buffer set to the pool.
    pub fn return_buffer_set(&self, mut buffer_set: BufferSet) {
        // Clear buffers but preserve capacity
        buffer_set.read_buffer.clear();
        buffer_set.write_buffer.clear();
        buffer_set.column_buffer.clear();
        buffer_set.type_byte = 0;

        // Ignore if pool is full
        let _ = self.buffer_sets.push(buffer_set);
    }
}

impl Default for BufferPool {
    fn default() -> Self {
        Self::new(POOL_CAPACITY)
    }
}
