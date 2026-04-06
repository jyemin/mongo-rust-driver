//! FFI count operations.

#[cfg(test)]
mod tests;

use std::ffi::{c_char, c_void};

use crate::ffi::{
    client::MongoClient,
    error::Error,
    types::{Bson, BsonValue, ContextExt, OperationContext},
};

/// Callback for count results.
/// On error: `count` is 0 and `error` is non-null.
/// On success: `count` is the document count and `error` is null.
pub type CountCallback = extern "C" fn(userdata: *mut c_void, count: u64, error: *const Error);

/// Options for `count_documents`.
///
/// All pointer fields are nullable (null = not set).
#[repr(C)]
pub struct CountOptions {
    /// Collation as a serialized BSON document. Nullable.
    pub collation: *const Bson,
    /// Index hint by name. Nullable. Takes precedence over `hint_keys`.
    pub hint_name: *const c_char,
    /// Index hint by key pattern as BSON document. Nullable.
    pub hint_keys: *const Bson,
    /// Maximum number of documents to count. -1 = not set.
    pub limit: i64,
    /// Number of documents to skip. -1 = not set.
    pub skip: i64,
    /// Maximum time in milliseconds. -1 = not set.
    pub max_time_ms: i64,
    /// Comment BSON value. Nullable.
    pub comment: *const BsonValue,
}

/// Options for `estimated_document_count`.
///
/// All pointer fields are nullable (null = not set).
#[repr(C)]
pub struct EstimatedDocumentCountOptions {
    /// Maximum time in milliseconds. -1 = not set.
    pub max_time_ms: i64,
    /// Comment BSON value. Nullable.
    pub comment: *const BsonValue,
}

/// Count documents matching `filter` in the specified collection.
///
/// If `filter` is null, all documents are counted (equivalent to `{}`).
///
/// # Safety
///
/// - `client` must be a valid pointer from `mongo_client_new`
/// - `db_name`, `coll_name` must be valid null-terminated C strings
/// - `filter` may be null (counts all documents) or a valid BSON document pointer
/// - `opts` may be null (use defaults)
/// - `ctx` may be null (no session/read concern)
#[no_mangle]
pub unsafe extern "C" fn mongo_count_documents(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    filter: *const Bson,
    opts: *const CountOptions,
    callback: CountCallback,
    userdata: *mut c_void,
) {
    use crate::ffi::ops::count::{execute_count_documents, prepare_count_documents};

    let setup = (|| -> crate::error::Result<_> {
        if client.is_null() {
            return Err(crate::error::Error::invalid_argument(
                "client cannot be null",
            ));
        }
        prepare_count_documents(&(*client).client, ctx, db_name, coll_name, filter, opts)
    })();

    let (coll, filter_doc, options) = match setup {
        Ok(v) => v,
        Err(e) => {
            callback(userdata, 0, &Error::from(&e));
            return;
        }
    };

    let session_ref = ctx.session();
    let userdata_ptr = userdata as usize;
    let client_ref = &*client;
    client_ref.runtime.spawn(async move {
        let result = execute_count_documents(coll, filter_doc, options, session_ref).await;
        let userdata = userdata_ptr as *mut c_void;
        match result {
            Ok(count) => callback(userdata, count, std::ptr::null()),
            Err(e) => callback(userdata, 0, &Error::from(&e)),
        }
    });
}

/// Return an estimated document count for the specified collection.
///
/// Uses collection metadata rather than scanning documents — does not support sessions.
///
/// # Safety
///
/// - `client` must be a valid pointer from `mongo_client_new`
/// - `db_name`, `coll_name` must be valid null-terminated C strings
/// - `opts` may be null (use defaults)
/// - `ctx` may be null (no read concern/read preference)
#[no_mangle]
pub unsafe extern "C" fn mongo_estimated_document_count(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    opts: *const EstimatedDocumentCountOptions,
    callback: CountCallback,
    userdata: *mut c_void,
) {
    use crate::ffi::ops::count::{execute_estimated_document_count, prepare_estimated_document_count};

    let setup = (|| -> crate::error::Result<_> {
        if client.is_null() {
            return Err(crate::error::Error::invalid_argument(
                "client cannot be null",
            ));
        }
        prepare_estimated_document_count(&(*client).client, ctx, db_name, coll_name, opts)
    })();

    let (coll, options) = match setup {
        Ok(v) => v,
        Err(e) => {
            callback(userdata, 0, &Error::from(&e));
            return;
        }
    };

    let userdata_ptr = userdata as usize;
    let client_ref = &*client;
    client_ref.runtime.spawn(async move {
        let result = execute_estimated_document_count(coll, options).await;
        let userdata = userdata_ptr as *mut c_void;
        match result {
            Ok(count) => callback(userdata, count, std::ptr::null()),
            Err(e) => callback(userdata, 0, &Error::from(&e)),
        }
    });
}
