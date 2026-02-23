//! Shared async operations for FFI bindings.
//!
//! These functions contain the core async logic that both the C API (api.rs)
//! and JNI bindings can call. They handle command execution and return Results,
//! leaving callback invocation to the caller.

use super::core;
use super::cursor::CursorManager;
use crate::bson::RawDocumentBuf;
use crate::raw_batch_cursor::RawBatchCursor;
use crate::Client;
use crate::Retryability;
use std::sync::Arc;

/// Result of executing a cursor command - contains cursor handle, exhausted flag, and first batch bytes.
pub struct CursorCommandResult {
    pub cursor_handle: u64,
    pub exhausted: bool,
    pub first_batch_bytes: Vec<u8>,
}

/// Result of a getMore operation - contains exhausted flag and batch bytes.
pub struct GetMoreResult {
    pub exhausted: bool,
    pub batch_bytes: Vec<u8>,
}

/// Execute a simple command (non-cursor).
/// Returns the raw response bytes on success, or serialized error bytes on failure.
pub async fn execute_command(
    client: &Client,
    db_name: &str,
    command: RawDocumentBuf,
    retry: Retryability,
    skip_session_injection: bool,
) -> Result<Vec<u8>, Vec<u8>> {
    match execute_command_raw(client, db_name, command, retry, skip_session_injection).await {
        Ok(raw_doc) => Ok(raw_doc.as_bytes().to_vec()),
        Err(e) => Err(e),
    }
}

/// Execute a simple command (non-cursor) returning RawDocumentBuf directly.
/// This variant avoids the extra copy for zero-copy scenarios where the caller
/// wants to write directly to a buffer (e.g., JNI DirectByteBuffer).
pub async fn execute_command_raw(
    client: &Client,
    db_name: &str,
    command: RawDocumentBuf,
    retry: Retryability,
    skip_session_injection: bool,
) -> Result<RawDocumentBuf, Vec<u8>> {
    let db = client.database(db_name);
    let result = db
        .run_raw_command_raw(command)
        .retryability(retry)
        .skip_session_injection(skip_session_injection)
        .await;

    match result {
        Ok(raw_doc) => Ok(raw_doc),
        Err(e) => Err(core::serialize_mongodb_error(&e)),
    }
}

/// Execute a cursor-returning command.
/// Returns cursor handle, exhausted flag, and first batch bytes.
pub async fn execute_cursor_command(
    client: &Client,
    db_name: &str,
    command: RawDocumentBuf,
    retry: Retryability,
    skip_session_injection: bool,
    batch_size: Option<u32>,
    comment: Option<crate::bson::Bson>,
    external_session_info: Option<(RawDocumentBuf, Option<i64>)>,
    cursor_manager: &Arc<CursorManager>,
) -> Result<CursorCommandResult, Vec<u8>> {
    use futures_util::StreamExt;

    let db = client.database(db_name);

    let mut action = db
        .run_raw_cursor_command_raw(command)
        .retryability(retry)
        .skip_session_injection(skip_session_injection);

    if let Some(bs) = batch_size {
        action = action.batch_size(bs);
    }
    if let Some(c) = comment {
        action = action.comment(c);
    }

    let result: crate::error::Result<RawBatchCursor> = action.await;

    match result {
        Ok(mut cursor) => {
            // Set external session info for getMore operations
            if let Some((lsid, txn_number)) = external_session_info {
                cursor.set_external_session_info(lsid, txn_number);
            }

            // Get the first batch
            let first_batch = cursor.next().await;

            match first_batch {
                Some(Ok(batch)) => {
                    let exhausted = cursor.is_exhausted();
                    let cursor_handle = cursor_manager.store(cursor);
                    let first_batch_bytes = super::cursor::raw_batch_to_bytes(&batch);
                    Ok(CursorCommandResult {
                        cursor_handle,
                        exhausted,
                        first_batch_bytes,
                    })
                }
                Some(Err(e)) => Err(core::serialize_mongodb_error(&e)),
                None => {
                    // Empty result - cursor is exhausted
                    let cursor_handle = cursor_manager.store(cursor);
                    Ok(CursorCommandResult {
                        cursor_handle,
                        exhausted: true,
                        first_batch_bytes: Vec::new(),
                    })
                }
            }
        }
        Err(e) => Err(core::serialize_mongodb_error(&e)),
    }
}

/// Execute getMore on a cursor.
/// Returns exhausted flag and batch bytes.
pub async fn execute_get_more(
    cursor_manager: &Arc<CursorManager>,
    cursor_handle: u64,
) -> Result<GetMoreResult, (Vec<u8>, bool)> {
    use futures_util::StreamExt;

    let cursor = match cursor_manager.take(cursor_handle) {
        Some(c) => c,
        None => return Err((b"Cursor not found".to_vec(), false)),
    };

    let mut cursor = cursor;
    let next_batch = cursor.next().await;

    match next_batch {
        Some(Ok(batch)) => {
            let exhausted = cursor.is_exhausted();
            let batch_bytes = super::cursor::raw_batch_to_bytes(&batch);
            if !exhausted {
                cursor_manager.put(cursor_handle, cursor);
            } else {
                cursor_manager.remove(cursor_handle);
            }
            Ok(GetMoreResult {
                exhausted,
                batch_bytes,
            })
        }
        Some(Err(e)) => {
            cursor_manager.put(cursor_handle, cursor);
            Err((core::serialize_mongodb_error(&e), true))
        }
        None => {
            cursor_manager.remove(cursor_handle);
            Ok(GetMoreResult {
                exhausted: true,
                batch_bytes: Vec::new(),
            })
        }
    }
}
