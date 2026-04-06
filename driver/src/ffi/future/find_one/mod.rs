//! Future-based FFI find-and-modify operations.

#[cfg(test)]
mod tests;

use std::ffi::c_char;

use super::{
    client::MongoClient,
    future::{FutureValue, MongoFuture},
};
use crate::ffi::{
    ops::find_one::{FindOneAndDeleteOptions, FindOneAndReplaceOptions, FindOneAndUpdateOptions},
    types::{Bson, BsonArray, ContextExt, OwnedBson, OperationContext},
};

/// Atomically find a document matching `filter` and delete it.
///
/// Returns a `MongoFuture` that resolves to a `Document` (or null if no match).
///
/// # Safety
///
/// - `client` must be a valid pointer from `mongoc_future_client_new`
/// - `db_name`, `coll_name` must be valid null-terminated C strings
/// - `filter` must be a valid BSON document pointer
/// - `opts` and `ctx` may be null
#[no_mangle]
pub unsafe extern "C" fn mongoc_future_find_one_and_delete(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    filter: *const Bson,
    opts: *const FindOneAndDeleteOptions,
) -> *mut MongoFuture {
    use crate::ffi::ops::find_one::{execute_find_one_and_delete, prepare_find_one_and_delete};

    let client_ref = &*client;

    let prep = (|| -> crate::error::Result<_> {
        prepare_find_one_and_delete(&client_ref.client, ctx, db_name, coll_name, filter, opts)
    })();

    match prep {
        Err(e) => MongoFuture::from_error(e),
        Ok((coll, filter_doc, options)) => {
            let session = ctx.session();
            let handle = client_ref.runtime.spawn(async move {
                let result =
                    execute_find_one_and_delete(coll, filter_doc, options, session).await?;
                Ok(FutureValue::Document(result.map(|d| OwnedBson::from_doc(&d))))
            });
            MongoFuture::from_join_handle(&client_ref.runtime, handle)
        }
    }
}

/// Atomically find a document matching `filter` and update it.
///
/// Exactly one of `update_doc` (non-null) or `update_pipeline` (non-empty) must be provided.
///
/// Returns a `MongoFuture` that resolves to a `Document` (or null if no match).
///
/// # Safety
///
/// Same safety requirements as `mongoc_future_find_one_and_delete`.
#[no_mangle]
pub unsafe extern "C" fn mongoc_future_find_one_and_update(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    filter: *const Bson,
    update_doc: *const Bson,
    update_pipeline: BsonArray,
    opts: *const FindOneAndUpdateOptions,
) -> *mut MongoFuture {
    use crate::ffi::ops::find_one::{execute_find_one_and_update, prepare_find_one_and_update};

    let client_ref = &*client;

    let prep = (|| -> crate::error::Result<_> {
        prepare_find_one_and_update(
            &client_ref.client,
            ctx,
            db_name,
            coll_name,
            filter,
            update_doc,
            update_pipeline,
            opts,
        )
    })();

    match prep {
        Err(e) => MongoFuture::from_error(e),
        Ok((coll, filter_doc, update, options)) => {
            let session = ctx.session();
            let handle = client_ref.runtime.spawn(async move {
                let result =
                    execute_find_one_and_update(coll, filter_doc, update, options, session).await?;
                Ok(FutureValue::Document(result.map(|d| OwnedBson::from_doc(&d))))
            });
            MongoFuture::from_join_handle(&client_ref.runtime, handle)
        }
    }
}

/// Atomically find a document matching `filter` and replace it with `replacement`.
///
/// Returns a `MongoFuture` that resolves to a `Document` (or null if no match).
///
/// # Safety
///
/// Same safety requirements as `mongoc_future_find_one_and_delete`.
#[no_mangle]
pub unsafe extern "C" fn mongoc_future_find_one_and_replace(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    filter: *const Bson,
    replacement: *const Bson,
    opts: *const FindOneAndReplaceOptions,
) -> *mut MongoFuture {
    use crate::ffi::ops::find_one::{execute_find_one_and_replace, prepare_find_one_and_replace};

    let client_ref = &*client;

    let prep = (|| -> crate::error::Result<_> {
        prepare_find_one_and_replace(
            &client_ref.client,
            ctx,
            db_name,
            coll_name,
            filter,
            replacement,
            opts,
        )
    })();

    match prep {
        Err(e) => MongoFuture::from_error(e),
        Ok((coll, filter_doc, replacement_doc, options)) => {
            let session = ctx.session();
            let handle = client_ref.runtime.spawn(async move {
                let result = execute_find_one_and_replace(
                    coll,
                    filter_doc,
                    replacement_doc,
                    options,
                    session,
                )
                .await?;
                Ok(FutureValue::Document(result.map(|d| OwnedBson::from_doc(&d))))
            });
            MongoFuture::from_join_handle(&client_ref.runtime, handle)
        }
    }
}
