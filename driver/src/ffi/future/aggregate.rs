//! Future-based FFI aggregate operations.

use std::ffi::c_char;

use futures_util::stream::StreamExt;

use super::{
    client::MongoClient,
    cursor::{CursorResult, FfiCursor},
    future::{FutureValue, MongoFuture},
};
use crate::ffi::{
    ops::aggregate::AggregateOptions,
    types::{BsonArray, ContextExt, OperationContext},
};

/// Helper: given a cursor, get the first batch and build a `FutureValue::Cursor`.
async unsafe fn cursor_to_future_value(
    mut cursor: FfiCursor,
    session_ref: Option<&mut crate::ClientSession>,
) -> crate::error::Result<FutureValue> {
    let first_batch = match &mut cursor {
        FfiCursor::Base(c) => c.next().await,
        FfiCursor::Session(c) => c.stream(session_ref.unwrap()).next().await,
    };

    let exhausted = match &cursor {
        FfiCursor::Base(c) => c.is_exhausted(),
        FfiCursor::Session(c) => c.is_exhausted(),
    };

    let (raw_batch, doc_ptrs, first_batch_arr) = match first_batch {
        Some(raw) => {
            let raw_batch = raw?;
            let (doc_ptrs, arr) = BsonArray::from_batch(&raw_batch)?;
            (Some(raw_batch), Some(doc_ptrs), arr)
        }
        None => (None, None, BsonArray::null()),
    };

    let cursor_ptr = if exhausted {
        std::ptr::null_mut()
    } else {
        Box::into_raw(Box::new(cursor))
    };

    let result = CursorResult {
        cursor: cursor_ptr,
        exhausted,
        first_batch: first_batch_arr,
    };

    Ok(FutureValue::Cursor {
        _raw_batch: raw_batch,
        _doc_ptrs: doc_ptrs,
        result,
    })
}

/// Run an aggregation pipeline on a collection.
///
/// Returns a `MongoFuture` that resolves to a `CursorResult` containing the
/// first batch and (if not exhausted) a cursor for subsequent batches.
///
/// # Safety
///
/// - `client` must be a valid pointer from `mongoc_future_client_new`
/// - `db_name`, `coll_name` must be valid null-terminated C strings
/// - `pipeline` must be a valid `BsonArray` of pipeline stage documents
/// - `opts` may be null (use defaults)
/// - `ctx` may be null (no session/read concern)
#[no_mangle]
pub unsafe extern "C" fn mongoc_future_aggregate_collection(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    pipeline: BsonArray,
    opts: *const AggregateOptions,
) -> *mut MongoFuture {
    let client_ref = &*client;

    let prep = (|| -> crate::error::Result<_> {
        use crate::error::Error;

        if client.is_null() {
            return Err(Error::invalid_argument("client cannot be null"));
        }

        crate::ffi::ops::aggregate::prepare_aggregate_collection(
            &client_ref.client,
            ctx,
            db_name,
            coll_name,
            pipeline,
            opts,
        )
    })();

    match prep {
        Err(e) => MongoFuture::from_error(e),
        Ok((coll, pipeline_docs, options)) => {
            let mut session_ref = ctx.session();
            let handle = client_ref.runtime.spawn(async move {
                let action = coll.aggregate(pipeline_docs).with_options(options);
                let cursor = match session_ref.as_deref_mut() {
                    None => action.batch().await.map(FfiCursor::Base),
                    Some(session) => action
                        .session(session)
                        .batch()
                        .await
                        .map(FfiCursor::Session),
                };
                let cursor = cursor?;
                cursor_to_future_value(cursor, session_ref.as_deref_mut()).await
            });
            MongoFuture::from_join_handle(&client_ref.runtime, handle)
        }
    }
}

/// Run an aggregation pipeline on a database.
///
/// Returns a `MongoFuture` that resolves to a `CursorResult` containing the
/// first batch and (if not exhausted) a cursor for subsequent batches.
///
/// # Safety
///
/// - `client` must be a valid pointer from `mongoc_future_client_new`
/// - `db_name` must be a valid null-terminated C string
/// - `pipeline` must be a valid `BsonArray` of pipeline stage documents
/// - `opts` may be null (use defaults)
/// - `ctx` may be null (no session/read concern)
#[no_mangle]
pub unsafe extern "C" fn mongoc_future_aggregate_database(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    pipeline: BsonArray,
    opts: *const AggregateOptions,
) -> *mut MongoFuture {
    let client_ref = &*client;

    let prep = (|| -> crate::error::Result<_> {
        use crate::error::Error;

        if client.is_null() {
            return Err(Error::invalid_argument("client cannot be null"));
        }

        crate::ffi::ops::aggregate::prepare_aggregate_database(
            &client_ref.client,
            ctx,
            db_name,
            pipeline,
            opts,
        )
    })();

    match prep {
        Err(e) => MongoFuture::from_error(e),
        Ok((db, pipeline_docs, options)) => {
            let mut session_ref = ctx.session();
            let handle = client_ref.runtime.spawn(async move {
                let action = db.aggregate(pipeline_docs).with_options(options);
                let cursor = match session_ref.as_deref_mut() {
                    None => action.batch().await.map(FfiCursor::Base),
                    Some(session) => action
                        .session(session)
                        .batch()
                        .await
                        .map(FfiCursor::Session),
                };
                let cursor = cursor?;
                cursor_to_future_value(cursor, session_ref.as_deref_mut()).await
            });
            MongoFuture::from_join_handle(&client_ref.runtime, handle)
        }
    }
}
