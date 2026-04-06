//! Future-based FFI count operations.

#[cfg(test)]
mod tests;

use std::ffi::c_char;

use super::{
    client::MongoClient,
    future::{FutureValue, MongoFuture},
};
use crate::ffi::{
    ops::count::{CountOptions, EstimatedDocumentCountOptions},
    types::{Bson, ContextExt, OperationContext},
};

/// Count documents matching `filter` in the specified collection.
///
/// Returns a `MongoFuture` that resolves to a count (u64).
///
/// # Safety
///
/// - `client` must be a valid pointer from `mongoc_future_client_new`
/// - `db_name`, `coll_name` must be valid null-terminated C strings
/// - `filter` may be null (counts all documents) or a valid BSON document pointer
/// - `opts` may be null (use defaults)
/// - `ctx` may be null (no session/read concern)
#[no_mangle]
pub unsafe extern "C" fn mongoc_future_count_documents(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    filter: *const Bson,
    opts: *const CountOptions,
) -> *mut MongoFuture {
    use crate::ffi::ops::count::{execute_count_documents, prepare_count_documents};

    let client_ref = &*client;

    let prep = (|| -> crate::error::Result<_> {
        prepare_count_documents(&client_ref.client, ctx, db_name, coll_name, filter, opts)
    })();

    match prep {
        Err(e) => MongoFuture::from_error(e),
        Ok((coll, filter_doc, options)) => {
            let session = ctx.session();
            let handle = client_ref.runtime.spawn(async move {
                let count = execute_count_documents(coll, filter_doc, options, session).await?;
                Ok(FutureValue::Count(count))
            });
            MongoFuture::from_join_handle(&client_ref.runtime, handle)
        }
    }
}

/// Return an estimated document count for the specified collection.
///
/// Returns a `MongoFuture` that resolves to a count (u64).
///
/// # Safety
///
/// - `client` must be a valid pointer from `mongoc_future_client_new`
/// - `db_name`, `coll_name` must be valid null-terminated C strings
/// - `opts` may be null (use defaults)
/// - `ctx` may be null (no read concern/read preference)
#[no_mangle]
pub unsafe extern "C" fn mongoc_future_estimated_document_count(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    opts: *const EstimatedDocumentCountOptions,
) -> *mut MongoFuture {
    use crate::ffi::ops::count::{execute_estimated_document_count, prepare_estimated_document_count};

    let client_ref = &*client;

    let prep = (|| -> crate::error::Result<_> {
        prepare_estimated_document_count(&client_ref.client, ctx, db_name, coll_name, opts)
    })();

    match prep {
        Err(e) => MongoFuture::from_error(e),
        Ok((coll, options)) => {
            let handle = client_ref.runtime.spawn(async move {
                let count = execute_estimated_document_count(coll, options).await?;
                Ok(FutureValue::Count(count))
            });
            MongoFuture::from_join_handle(&client_ref.runtime, handle)
        }
    }
}
