// Shared core logic used by both JNI and FFI entry points.
// This module contains business logic that is independent of the FFI mechanism.

use super::session::FfiSessionPool;
use crate::bson::{doc, Bson, Document, RawDocumentBuf};

// ============================================================================
// Error Category Constants (must match RustError.CATEGORY_* in Java)
// ============================================================================

pub const ERROR_CATEGORY_COMMAND: i32 = 0;
pub const ERROR_CATEGORY_CONNECTION: i32 = 1;
pub const ERROR_CATEGORY_SERVER_SELECTION: i32 = 2;
pub const ERROR_CATEGORY_AUTHENTICATION: i32 = 3;
pub const ERROR_CATEGORY_INTERNAL: i32 = 4;

// ============================================================================
// Operation Parameters
// ============================================================================

/// Parameters for command execution, independent of FFI mechanism.
#[derive(Debug, Clone)]
pub struct OperationParams {
    pub retryability: u8, // 0=None, 1=Read, 2=Write
    pub session_handle: u64,
    pub in_transaction: bool,
    pub start_transaction: bool,
    pub has_after_cluster_time: bool,
    pub after_cluster_time_seconds: u32,
    pub after_cluster_time_increment: u32,
    pub read_concern_level: Option<String>,
}

impl Default for OperationParams {
    fn default() -> Self {
        Self {
            retryability: 0,
            session_handle: 0,
            in_transaction: false,
            start_transaction: false,
            has_after_cluster_time: false,
            after_cluster_time_seconds: 0,
            after_cluster_time_increment: 0,
            read_concern_level: None,
        }
    }
}

// ============================================================================
// Retryability Conversion
// ============================================================================

/// Convert retryability byte to MongoDB Retryability enum.
#[inline]
pub fn to_retryability(value: u8) -> crate::Retryability {
    match value {
        1 => crate::Retryability::Read,
        2 => crate::Retryability::Write,
        _ => crate::Retryability::None,
    }
}

// ============================================================================
// Error Serialization
// ============================================================================

/// Serialize a MongoDB error to structured BSON bytes.
/// Format: { _rustError: true, category, code, codeName, message, errorLabels, serverResponse? }
pub fn serialize_mongodb_error(error: &crate::error::Error) -> Vec<u8> {
    // Determine error category and extract details based on error kind
    let (category, code, code_name, message) = match error.kind.as_ref() {
        crate::error::ErrorKind::Command(cmd_err) => (
            ERROR_CATEGORY_COMMAND,
            cmd_err.code,
            cmd_err.code_name.clone(),
            cmd_err.message.clone(),
        ),
        crate::error::ErrorKind::ServerSelection { message, .. } => (
            ERROR_CATEGORY_SERVER_SELECTION,
            -1,
            String::new(),
            message.clone(),
        ),
        crate::error::ErrorKind::Authentication { message, .. } => (
            ERROR_CATEGORY_AUTHENTICATION,
            -1,
            String::new(),
            message.clone(),
        ),
        crate::error::ErrorKind::ConnectionPoolCleared { message, .. } => (
            ERROR_CATEGORY_CONNECTION,
            -1,
            String::new(),
            message.clone(),
        ),
        // I/O errors are connection errors (socket errors, network errors, etc.)
        crate::error::ErrorKind::Io(_) => (
            ERROR_CATEGORY_CONNECTION,
            -1,
            String::new(),
            error.to_string(),
        ),
        _ => (
            ERROR_CATEGORY_INTERNAL,
            -1,
            String::new(),
            error.to_string(),
        ),
    };

    // Extract labels
    let labels: Vec<Bson> = error
        .labels()
        .iter()
        .map(|s| Bson::String(s.clone()))
        .collect();

    // Build BSON document with error info
    let mut error_doc = doc! {
        "_rustError": true,
        "category": category,
        "code": code,
        "codeName": &code_name,
        "message": &message,
        "errorLabels": labels,
    };

    // Include raw server response if available (for command errors)
    if let Some(server_response) = error.server_response() {
        if let Ok(doc) = server_response.to_document() {
            error_doc.insert("serverResponse", doc);
        }
    }

    // Serialize to bytes
    if let Ok(raw) = RawDocumentBuf::from_document(&error_doc) {
        raw.as_bytes().to_vec()
    } else {
        // Fallback to plain string error
        error.to_string().into_bytes()
    }
}

// ============================================================================
// Command Preparation
// ============================================================================

/// Prepare a command by adding session and transaction fields.
/// Returns (prepared_command, has_session) where has_session indicates if session injection should be skipped.
pub fn prepare_command_with_session(
    command_bytes: Vec<u8>,
    params: &OperationParams,
    session_pool: &FfiSessionPool,
) -> Result<(RawDocumentBuf, bool), String> {
    // Parse command into a mutable Document
    let mut command_doc = Document::from_reader(&mut command_bytes.as_slice())
        .map_err(|e| format!("Failed to parse command: {}", e))?;

    let retry = to_retryability(params.retryability);
    let has_session = params.session_handle != 0;

    if has_session {
        // Get lsid from session
        if let Some(lsid_doc) = session_pool.get_session_lsid(params.session_handle) {
            command_doc.insert("lsid", lsid_doc);

            // Handle transaction or retryable write txnNumber
            if params.in_transaction {
                let txn_number = session_pool.get_txn_number(params.session_handle);
                command_doc.insert("txnNumber", Bson::Int64(txn_number as i64));

                if params.start_transaction {
                    command_doc.insert("startTransaction", true);
                    add_read_concern(&mut command_doc, params);
                }
                command_doc.insert("autocommit", false);
            } else if retry == crate::Retryability::Write {
                // Retryable write outside transaction: auto-advance txnNumber
                let txn_number = session_pool.advance_txn_number(params.session_handle);
                command_doc.insert("txnNumber", Bson::Int64(txn_number as i64));
            }
        }
    }

    // Convert back to RawDocumentBuf
    let command_raw = RawDocumentBuf::from_document(&command_doc)
        .map_err(|e| format!("Failed to serialize command: {}", e))?;

    Ok((command_raw, has_session))
}

/// Add readConcern to command if needed (for startTransaction).
fn add_read_concern(command_doc: &mut Document, params: &OperationParams) {
    let has_level = params.read_concern_level.is_some();
    let has_after = params.has_after_cluster_time;

    if has_level || has_after {
        let mut read_concern = Document::new();
        if let Some(level) = &params.read_concern_level {
            read_concern.insert("level", level.as_str());
        }
        if has_after {
            let timestamp = crate::bson::Timestamp {
                time: params.after_cluster_time_seconds,
                increment: params.after_cluster_time_increment,
            };
            read_concern.insert("afterClusterTime", timestamp);
        }
        command_doc.insert("readConcern", read_concern);
    }
}

// ============================================================================
// Cursor Command Preparation
// ============================================================================

/// Prepare a cursor command and extract external session info for getMore operations.
/// Returns (prepared_command, has_session, external_session_info).
/// external_session_info is (lsid, Option<txn_number>) for getMore to use.
pub fn prepare_cursor_command_with_session(
    command_bytes: Vec<u8>,
    params: &OperationParams,
    session_pool: &FfiSessionPool,
) -> Result<(RawDocumentBuf, bool, Option<(RawDocumentBuf, Option<i64>)>), String> {
    // Parse command into a mutable Document
    let mut command_doc = Document::from_reader(&mut command_bytes.as_slice())
        .map_err(|e| format!("Failed to parse command: {}", e))?;

    let has_session = params.session_handle != 0;

    // Store session info for getMore operations (needed for ALL sessions, not just transactions)
    let external_session_info: Option<(RawDocumentBuf, Option<i64>)> = if has_session {
        let lsid = session_pool
            .get_session_lsid(params.session_handle)
            .and_then(|lsid_doc| RawDocumentBuf::from_document(&lsid_doc).ok());
        let txn_number = if params.in_transaction {
            Some(session_pool.get_txn_number(params.session_handle) as i64)
        } else {
            None
        };
        lsid.map(|l| (l, txn_number))
    } else {
        None
    };

    if has_session {
        // Get lsid from session
        if let Some(lsid_doc) = session_pool.get_session_lsid(params.session_handle) {
            command_doc.insert("lsid", lsid_doc);

            // Handle transaction
            if params.in_transaction {
                let txn_number = session_pool.get_txn_number(params.session_handle);
                command_doc.insert("txnNumber", Bson::Int64(txn_number as i64));

                if params.start_transaction {
                    command_doc.insert("startTransaction", true);
                    add_read_concern(&mut command_doc, params);
                }
                command_doc.insert("autocommit", false);
            }
        }
    }

    // Convert back to RawDocumentBuf
    let command_raw = RawDocumentBuf::from_document(&command_doc)
        .map_err(|e| format!("Failed to serialize command: {}", e))?;

    Ok((command_raw, has_session, external_session_info))
}

/// Extract comment from a command document (for getMore operations).
pub fn extract_comment(command_bytes: &[u8]) -> Option<Bson> {
    if let Ok(doc) = Document::from_reader(&mut &command_bytes[..]) {
        doc.get("comment").cloned()
    } else {
        None
    }
}
