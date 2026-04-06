//! Future-based FFI find operation.

use std::ffi::c_char;

use futures_util::stream::StreamExt;

use super::{
    client::MongoClient,
    cursor::{CursorResult, FfiCursor},
    future::{FutureValue, MongoFuture},
};
use crate::ffi::{
    ops::find::FindOptions,
    types::{Bson, BsonArray, ContextExt, OperationContext},
};

/// Find documents matching a filter.
///
/// Returns a `MongoFuture` that resolves to a `CursorResult` containing the
/// first batch and (if not exhausted) a cursor for subsequent batches.
///
/// # Safety
///
/// - `client` must be a valid pointer from `mongoc_future_client_new`
/// - `db_name`, `coll_name` must be valid null-terminated C strings
/// - `filter` must be a valid BSON document pointer, or null for `{}`
/// - `opts` may be null (use defaults)
/// - `ctx` may be null (no session/read concern)
#[no_mangle]
pub unsafe extern "C" fn mongoc_future_find(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    filter: *const Bson,
    opts: *const FindOptions,
) -> *mut MongoFuture {
    let client_ref = &*client;

    let prep = (|| -> crate::error::Result<_> {
        use crate::error::Error;

        if client.is_null() {
            return Err(Error::invalid_argument("client cannot be null"));
        }

        crate::ffi::ops::find::prepare_find(
            &client_ref.client,
            ctx,
            db_name,
            coll_name,
            filter,
            opts,
        )
    })();

    match prep {
        Err(e) => MongoFuture::from_error(e),
        Ok((coll, filter_doc, options)) => {
            let mut session_ref = ctx.session();
            let handle = client_ref.runtime.spawn(async move {
                let action = coll.find(filter_doc).with_options(options);
                let cursor = match session_ref.as_deref_mut() {
                    None => action.batch().await.map(FfiCursor::Base),
                    Some(session) => action
                        .session(session)
                        .batch()
                        .await
                        .map(FfiCursor::Session),
                };
                let mut cursor = cursor?;

                let first_batch = match &mut cursor {
                    FfiCursor::Base(c) => c.next().await,
                    FfiCursor::Session(c) => {
                        c.stream(session_ref.unwrap()).next().await
                    }
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
            });
            MongoFuture::from_join_handle(&client_ref.runtime, handle)
        }
    }
}
