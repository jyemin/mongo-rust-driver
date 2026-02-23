// Cursor management for FFI
//
// This module provides cursor management for iterating over MongoDB query results.
// We use the Rust driver's RawBatchCursor which handles pinning internally.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use crate::raw_batch_cursor::RawBatchCursor;

/// Manages RawBatchCursor instances for FFI.
/// Thread-safe storage of cursors indexed by handle.
pub struct CursorManager {
    next_handle: AtomicU64,
    cursors: Mutex<HashMap<u64, RawBatchCursor>>,
}

impl CursorManager {
    pub fn new() -> Self {
        Self {
            next_handle: AtomicU64::new(1), // Start at 1, 0 is invalid
            cursors: Mutex::new(HashMap::new()),
        }
    }

    /// Store a cursor and return its handle.
    pub fn store(&self, cursor: RawBatchCursor) -> u64 {
        let handle = self.next_handle.fetch_add(1, Ordering::SeqCst);
        if let Ok(mut cursors) = self.cursors.lock() {
            cursors.insert(handle, cursor);
        }
        handle
    }

    /// Take a cursor out of storage (for mutation/iteration).
    /// The caller must put it back with `put` after use.
    pub fn take(&self, handle: u64) -> Option<RawBatchCursor> {
        self.cursors.lock().ok()?.remove(&handle)
    }

    /// Put a cursor back into storage after use.
    pub fn put(&self, handle: u64, cursor: RawBatchCursor) {
        if let Ok(mut cursors) = self.cursors.lock() {
            cursors.insert(handle, cursor);
        }
    }

    /// Remove a cursor from storage permanently.
    pub fn remove(&self, handle: u64) -> Option<RawBatchCursor> {
        self.cursors.lock().ok()?.remove(&handle)
    }

    /// Check if a cursor exists.
    pub fn exists(&self, handle: u64) -> bool {
        self.cursors.lock().ok().map_or(false, |c| c.contains_key(&handle))
    }
}

impl Default for CursorManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Convert a RawBatch to bytes for FFI.
/// Returns the raw document bytes which Java can parse.
pub fn raw_batch_to_bytes(batch: &crate::raw_batch_cursor::RawBatch) -> Vec<u8> {
    // RawBatch contains the full server response, just return its bytes
    batch.as_raw_document().as_bytes().to_vec()
}

