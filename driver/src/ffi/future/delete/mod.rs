//! Future-based FFI delete operations.

#[cfg(test)]
mod tests;

use std::ffi::c_char;

use super::{
    client::MongoClient,
    future::{FutureValue, MongoFuture},
};
use crate::ffi::{
    ops::delete::{DeleteOptions, DeleteResult},
    types::{Bson, ContextExt, OperationContext},
};

/// Delete up to one document matching `filter`.
///
/// Returns a `MongoFuture` that resolves to a `DeleteResult`.
///
/// # Safety
///
/// - `client` must be a valid pointer from `mongoc_future_client_new`
/// - `db_name`, `coll_name` must be valid null-terminated C strings
/// - `filter` must be a valid BSON document pointer
/// - `opts` may be null (use defaults)
/// - `ctx` may be null (no session/write concern)
#[no_mangle]
pub unsafe extern "C" fn mongoc_future_delete_one(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    filter: *const Bson,
    opts: *const DeleteOptions,
) -> *mut MongoFuture {
    use crate::ffi::ops::delete::{execute_delete_one, prepare_delete};

    let client_ref = &*client;

    let prep = (|| -> crate::error::Result<_> {
        prepare_delete(&client_ref.client, ctx, db_name, coll_name, filter, opts)
    })();

    match prep {
        Err(e) => MongoFuture::from_error(e),
        Ok((coll, filter_doc, options)) => {
            let session = ctx.session();
            let handle = client_ref.runtime.spawn(async move {
                let r = execute_delete_one(coll, filter_doc, options, session).await?;
                Ok(FutureValue::Delete(DeleteResult {
                    deleted_count: r.deleted_count,
                }))
            });
            MongoFuture::from_join_handle(&client_ref.runtime, handle)
        }
    }
}

/// Delete all documents matching `filter`.
///
/// Returns a `MongoFuture` that resolves to a `DeleteResult`.
///
/// # Safety
///
/// Same safety requirements as `mongoc_future_delete_one`.
#[no_mangle]
pub unsafe extern "C" fn mongoc_future_delete_many(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    filter: *const Bson,
    opts: *const DeleteOptions,
) -> *mut MongoFuture {
    use crate::ffi::ops::delete::{execute_delete_many, prepare_delete};

    let client_ref = &*client;

    let prep = (|| -> crate::error::Result<_> {
        prepare_delete(&client_ref.client, ctx, db_name, coll_name, filter, opts)
    })();

    match prep {
        Err(e) => MongoFuture::from_error(e),
        Ok((coll, filter_doc, options)) => {
            let session = ctx.session();
            let handle = client_ref.runtime.spawn(async move {
                let r = execute_delete_many(coll, filter_doc, options, session).await?;
                Ok(FutureValue::Delete(DeleteResult {
                    deleted_count: r.deleted_count,
                }))
            });
            MongoFuture::from_join_handle(&client_ref.runtime, handle)
        }
    }
}
