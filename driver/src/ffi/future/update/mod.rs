//! Future-based FFI update and replace operations.

#[cfg(test)]
mod tests;

use std::ffi::c_char;

use super::{
    client::MongoClient,
    future::{FutureValue, MongoFuture},
};
use crate::ffi::{
    ops::update::{ReplaceOneOptions, UpdateOneOptions},
    types::{Bson, BsonArray, ContextExt, OperationContext},
};

/// Update up to one document matching `filter`.
///
/// Exactly one of `update` (BSON document) or `pipeline` (non-empty array) must be provided.
///
/// Returns a `MongoFuture` that resolves to an `UpdateResult`.
///
/// # Safety
///
/// - `client` must be a valid pointer from `mongoc_future_client_new`
/// - `db_name`, `coll_name` must be valid null-terminated C strings
/// - `filter` must be a valid BSON document pointer
/// - Exactly one of `update` or `pipeline` must be non-null/non-empty
/// - `opts` may be null (use defaults)
/// - `ctx` may be null (no session/write concern)
#[no_mangle]
pub unsafe extern "C" fn mongoc_future_update_one(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    filter: *const Bson,
    update: *const Bson,
    pipeline: BsonArray,
    opts: *const UpdateOneOptions,
) -> *mut MongoFuture {
    use crate::ffi::ops::update::{build_update_result, execute_update_one, prepare_update};

    let client_ref = &*client;

    let prep = (|| -> crate::error::Result<_> {
        prepare_update(
            &client_ref.client,
            ctx,
            db_name,
            coll_name,
            filter,
            update,
            &pipeline,
            opts,
        )
    })();

    match prep {
        Err(e) => MongoFuture::from_error(e),
        Ok((coll, filter_doc, modifications, options)) => {
            let session = ctx.session();
            let handle = client_ref.runtime.spawn(async move {
                let result =
                    execute_update_one(coll, filter_doc, modifications, options, session).await;
                Ok(FutureValue::Update(build_update_result(result?)?))
            });
            MongoFuture::from_join_handle(&client_ref.runtime, handle)
        }
    }
}

/// Update all documents matching `filter`.
///
/// Exactly one of `update` (BSON document) or `pipeline` (non-empty array) must be provided.
///
/// Returns a `MongoFuture` that resolves to an `UpdateResult`.
///
/// # Safety
///
/// Same safety requirements as `mongoc_future_update_one`.
#[no_mangle]
pub unsafe extern "C" fn mongoc_future_update_many(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    filter: *const Bson,
    update: *const Bson,
    pipeline: BsonArray,
    opts: *const UpdateOneOptions,
) -> *mut MongoFuture {
    use crate::ffi::ops::update::{build_update_result, execute_update_many, prepare_update};

    let client_ref = &*client;

    let prep = (|| -> crate::error::Result<_> {
        prepare_update(
            &client_ref.client,
            ctx,
            db_name,
            coll_name,
            filter,
            update,
            &pipeline,
            opts,
        )
    })();

    match prep {
        Err(e) => MongoFuture::from_error(e),
        Ok((coll, filter_doc, modifications, options)) => {
            let session = ctx.session();
            let handle = client_ref.runtime.spawn(async move {
                let result =
                    execute_update_many(coll, filter_doc, modifications, options, session).await;
                Ok(FutureValue::Update(build_update_result(result?)?))
            });
            MongoFuture::from_join_handle(&client_ref.runtime, handle)
        }
    }
}

/// Replace up to one document matching `filter` with `replacement`.
///
/// Returns a `MongoFuture` that resolves to an `UpdateResult`.
///
/// # Safety
///
/// - `client` must be a valid pointer from `mongoc_future_client_new`
/// - `db_name`, `coll_name` must be valid null-terminated C strings
/// - `filter` must be a valid BSON document pointer
/// - `replacement` must be a valid BSON document pointer
/// - `opts` may be null (use defaults)
/// - `ctx` may be null (no session/write concern)
#[no_mangle]
pub unsafe extern "C" fn mongoc_future_replace_one(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    filter: *const Bson,
    replacement: *const Bson,
    opts: *const ReplaceOneOptions,
) -> *mut MongoFuture {
    use crate::ffi::ops::update::{build_update_result, execute_replace_one, prepare_replace};

    let client_ref = &*client;

    let prep = (|| -> crate::error::Result<_> {
        prepare_replace(
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
                let result =
                    execute_replace_one(coll, filter_doc, replacement_doc, options, session).await;
                Ok(FutureValue::Update(build_update_result(result?)?))
            });
            MongoFuture::from_join_handle(&client_ref.runtime, handle)
        }
    }
}
