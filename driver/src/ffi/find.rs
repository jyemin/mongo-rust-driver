//! FFI find operations.

#[cfg(test)]
mod tests;

use std::ffi::{c_char, c_void};

use futures_util::stream::StreamExt;

use crate::ffi::{
    client::MongoClient,
    cursor::{CursorResult, FfiCursor},
    error::Error,
    types::{Bson, BsonArray, ContextExt, OperationContext},
    utils::with_err_callback,
};

/// Callback for asynchronous `mongo_find` results.
pub type FindCallback =
    extern "C" fn(userdata: *mut c_void, result: *const CursorResult, error: *const Error);

/// Find documents matching a filter.
#[no_mangle]
pub unsafe extern "C" fn mongo_find(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    filter: *const Bson,
    opts: *const FindOptions,
    callback: FindCallback,
    userdata: *mut c_void,
) {
    let (coll, filter, options) = with_err_callback!(callback, userdata, || {
        use crate::error::Error;

        if client.is_null() {
            return Err(Error::invalid_argument("client cannot be null"));
        }

        crate::ffi::ops::find::prepare_find(&(*client).client, ctx, db_name, coll_name, filter, opts)
    });

    let mut session_ref = ctx.session();
    let userdata_ptr = userdata as usize;
    let client_ref = &*client;
    client_ref.runtime.spawn(async move {
        let action = coll.find(filter).with_options(options);
        let cursor = match session_ref.as_deref_mut() {
            None => action.batch().await.map(FfiCursor::Base),
            Some(session) => action
                .session(session)
                .batch()
                .await
                .map(FfiCursor::Session),
        };
        let userdata = userdata_ptr as *mut c_void;
        let mut cursor = match cursor {
            Ok(c) => c,
            Err(e) => {
                callback(userdata, std::ptr::null(), &Error::from(&e));
                return;
            }
        };
        let first_batch = match &mut cursor {
            FfiCursor::Base(c) => c.next().await,
            FfiCursor::Session(c) => c.stream(session_ref.unwrap()).next().await,
        };

        let userdata = userdata_ptr as *mut c_void;
        with_err_callback!(callback, userdata, || {
            let exhausted = match &cursor {
                FfiCursor::Base(c) => c.is_exhausted(),
                FfiCursor::Session(c) => c.is_exhausted(),
            };

            let raw_batch;
            let _doc_ptrs;
            let first_batch = match first_batch {
                Some(raw) => {
                    raw_batch = raw?;
                    let out = BsonArray::from_batch(&raw_batch)?;
                    _doc_ptrs = out.0;
                    out.1
                }
                None => BsonArray::null(),
            };

            let cursor = if exhausted {
                std::ptr::null_mut()
            } else {
                Box::into_raw(Box::new(cursor))
            };
            let result = CursorResult {
                cursor,
                exhausted,
                first_batch,
            };
            callback(userdata, &result, std::ptr::null());
            Ok(())
        });
    });
}

/// FFI-compatible find options.
///
/// Use -1 for "not set" on integer options, null for pointer options.
/// For tri-state booleans (i8): -1 = not set, 0 = false, 1 = true.
#[repr(C)]
pub struct FindOptions {
    /// Allow disk use for sorting large result sets. Tri-state: -1 = not set, 0 = false, 1 = true.
    pub allow_disk_use: i8,
    /// Allow partial results from mongos if some shards are down. Tri-state.
    pub allow_partial_results: i8,
    /// Number of documents per batch. -1 = not set.
    pub batch_size: i32,
    /// Comment to attach to the query. Nullable BSON value wrapped in doc with empty key.
    pub comment: *const Bson,
    /// Cursor type: -1 = not set, 0 = NonTailable, 1 = Tailable, 2 = TailableAwait.
    pub cursor_type: i8,
    /// Index name hint. Nullable, takes precedence over hint_keys if set.
    pub hint_name: *const c_char,
    /// Index keys hint as BSON document. Nullable.
    pub hint_keys: *const Bson,
    /// Maximum number of documents to return. 0 = not set.
    pub limit: i64,
    /// Exclusive upper bound for a specific index. Nullable BSON document.
    pub max: *const Bson,
    /// Max time for tailable cursor to wait for new documents. -1 = not set.
    pub max_await_time_ms: i64,
    /// Maximum query execution time in milliseconds. -1 = not set.
    pub max_time_ms: i64,
    /// Inclusive lower bound for a specific index. Nullable BSON document.
    pub min: *const Bson,
    /// Prevent cursor timeout after inactivity. Tri-state.
    pub no_cursor_timeout: i8,
    /// Projection document. Nullable BSON document.
    pub projection: *const Bson,
    /// Return only index keys, not full documents. Tri-state.
    pub return_key: i8,
    /// Include record identifier in results. Tri-state.
    pub show_record_id: i8,
    /// Number of documents to skip. -1 = not set.
    pub skip: i64,
    /// Sort specification. Nullable BSON document.
    pub sort: *const Bson,
    /// Collation options. Nullable BSON document (deserialized as Collation).
    pub collation: *const Bson,
    /// Variables for use in aggregation expressions. Nullable BSON document.
    pub let_vars: *const Bson,
}

