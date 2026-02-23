// Core types for FFI boundary

use std::sync::Arc;
use super::session::FfiSessionPool;
use super::cursor::CursorManager;

/// Raw BSON bytes passed across FFI boundary
#[repr(C)]
pub struct BsonBytes {
    pub data: *const u8,
    pub len: usize,
}

/// Operation context passed across FFI boundary
/// Contains session, transaction, and retryability information
#[repr(C)]
pub struct OperationContext {
    /// Retryability: 0=None, 1=Read, 2=Write
    pub retryability: u8,
    /// Session handle from mongo_session_acquire (0 = no session)
    pub session_handle: u64,
    /// Whether the operation is in a transaction
    pub in_transaction: bool,
    /// Whether to start a new transaction
    pub start_transaction: bool,
    /// Whether afterClusterTime is set
    pub has_after_cluster_time: bool,
    /// afterClusterTime seconds component (only valid if has_after_cluster_time is true)
    pub after_cluster_time_seconds: u32,
    /// afterClusterTime increment component (only valid if has_after_cluster_time is true)
    pub after_cluster_time_increment: u32,
    /// Read concern level (nullable, null-terminated C string)
    pub read_concern_level: *const std::ffi::c_char,
}

/// Callback for single result operations
///
/// # Parameters
/// * `success` - Whether the operation succeeded
/// * `data` - BSON bytes for the result (if success=true) or error (if success=false)
pub type SingleResultCallback = extern "C" fn(success: bool, data: *const BsonBytes);

/// Callback for cursor operations
///
/// # Parameters
/// * `success` - Whether the operation succeeded
/// * `cursor_handle` - Handle to the cursor (0 on error)
/// * `exhausted` - Whether the cursor is exhausted (no more batches)
/// * `data` - BSON bytes for the batch (firstBatch or nextBatch)
pub type CursorResultCallback = extern "C" fn(
    success: bool,
    cursor_handle: u64,
    exhausted: bool,
    data: *const BsonBytes,
);

/// Callback for getMore operations
///
/// # Parameters
/// * `success` - Whether the operation succeeded
/// * `exhausted` - Whether the cursor is exhausted (no more batches)
/// * `data` - BSON bytes for the nextBatch
pub type GetMoreResultCallback = extern "C" fn(
    success: bool,
    exhausted: bool,
    data: *const BsonBytes,
);

/// Opaque client handle
/// Contains the actual MongoDB Rust driver client, session pool, and cursor manager
pub struct MongoClient {
    pub client: crate::Client,
    pub runtime: tokio::runtime::Runtime,
    pub session_pool: FfiSessionPool,
    /// Arc-wrapped for safe sharing across async tasks
    pub cursor_manager: Arc<CursorManager>,
}

// Note: When MongoClient is dropped, the runtime will be dropped automatically.
// The Tokio runtime's Drop implementation will wait for spawned tasks to complete,
// ensuring graceful shutdown of any in-flight operations.

