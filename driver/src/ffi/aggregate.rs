//! FFI aggregate operations.

#[cfg(test)]
mod tests;

use std::ffi::{c_char, c_void};

use futures_util::stream::StreamExt;

use crate::ffi::{
    client::MongoClient,
    cursor::{CursorResult, FfiCursor},
    error::Error,
    types::{Bson, BsonArray, BsonValue, ContextExt, OperationContext},
    utils::with_err_callback,
};

/// Callback for asynchronous aggregate results.
pub type AggregateCallback =
    extern "C" fn(userdata: *mut c_void, result: *const CursorResult, error: *const Error);

/// FFI-compatible aggregate options.
///
/// Use -1 for "not set" on integer options, null for pointer options.
/// For tri-state booleans (i8): -1 = not set, 0 = false, 1 = true.
#[repr(C)]
pub struct AggregateOptions {
    /// Allow disk use for sorting large result sets. Tri-state: -1 = not set, 0 = false, 1 = true.
    pub allow_disk_use: i8,
    /// Number of documents per batch. -1 = not set.
    pub batch_size: i32,
    /// Opt out of document-level validation. Tri-state.
    pub bypass_document_validation: i8,
    /// Collation options. Nullable BSON document.
    pub collation: *const Bson,
    /// Comment as a BSON value. Nullable.
    pub comment: *const BsonValue,
    /// Index name hint. Nullable, takes precedence over hint_keys if set.
    pub hint_name: *const c_char,
    /// Index keys hint as BSON document. Nullable.
    pub hint_keys: *const Bson,
    /// Maximum query execution time in milliseconds. -1 = not set.
    pub max_time_ms: i64,
    /// Variables for use in aggregation expressions. Nullable BSON document.
    pub let_vars: *const Bson,
}


/// Run an aggregation pipeline on a collection.
#[no_mangle]
pub unsafe extern "C" fn mongo_aggregate_collection(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    pipeline: BsonArray,
    opts: *const AggregateOptions,
    callback: AggregateCallback,
    userdata: *mut c_void,
) {
    let (coll, pipeline_docs, options) = with_err_callback!(callback, userdata, || {
        use crate::error::Error;

        if client.is_null() {
            return Err(Error::invalid_argument("client cannot be null"));
        }

        crate::ffi::ops::aggregate::prepare_aggregate_collection(
            &(*client).client, ctx, db_name, coll_name, pipeline, opts,
        )
    });

    let mut session_ref = ctx.session();
    let userdata_ptr = userdata as usize;
    let client_ref = &*client;
    client_ref.runtime.spawn(async move {
        let action = coll.aggregate(pipeline_docs).with_options(options);
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

/// Run an aggregation pipeline on a database.
#[no_mangle]
pub unsafe extern "C" fn mongo_aggregate_database(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    pipeline: BsonArray,
    opts: *const AggregateOptions,
    callback: AggregateCallback,
    userdata: *mut c_void,
) {
    let (db, pipeline_docs, options) = with_err_callback!(callback, userdata, || {
        use crate::error::Error;

        if client.is_null() {
            return Err(Error::invalid_argument("client cannot be null"));
        }

        crate::ffi::ops::aggregate::prepare_aggregate_database(
            &(*client).client, ctx, db_name, pipeline, opts,
        )
    });

    let mut session_ref = ctx.session();
    let userdata_ptr = userdata as usize;
    let client_ref = &*client;
    client_ref.runtime.spawn(async move {
        let action = db.aggregate(pipeline_docs).with_options(options);
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
