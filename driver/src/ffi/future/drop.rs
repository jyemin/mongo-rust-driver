//! Future-based FFI drop operations.

use std::os::raw::c_char;

use super::{
    client::MongoClient,
    future::{FutureValue, MongoFuture},
};
use crate::ffi::types::{ContextExt, OperationContext};

/// Drop a database asynchronously.
///
/// Returns a `MongoFuture` that resolves to void.
///
/// # Safety
///
/// - `client` must be a valid pointer from `mongoc_future_client_new`
/// - `db_name` must be a valid null-terminated C string
/// - `context` may be null or a valid pointer to an OperationContext
#[no_mangle]
pub unsafe extern "C" fn mongoc_future_drop_database(
    client: *mut MongoClient,
    context: *mut OperationContext,
    db_name: *const c_char,
) -> *mut MongoFuture {
    use crate::ffi::ops::drop::{execute_drop_database, prepare_drop_database};

    let client_ref = &*client;

    let prep = (|| -> crate::error::Result<_> {
        prepare_drop_database(&client_ref.client, db_name)
    })();

    match prep {
        Err(e) => MongoFuture::from_error(e),
        Ok(db) => {
            let session = context.session();
            let write_concern = context.write_concern();
            let handle = client_ref.runtime.spawn(async move {
                execute_drop_database(db, session, write_concern).await?;
                Ok(FutureValue::Void)
            });
            MongoFuture::from_join_handle(&client_ref.runtime, handle)
        }
    }
}

/// Drop a collection asynchronously.
///
/// Returns a `MongoFuture` that resolves to void.
///
/// # Safety
///
/// - `client` must be a valid pointer from `mongoc_future_client_new`
/// - `db_name`, `coll_name` must be valid null-terminated C strings
/// - `context` may be null or a valid pointer to an OperationContext
#[no_mangle]
pub unsafe extern "C" fn mongoc_future_drop_collection(
    client: *mut MongoClient,
    context: *mut OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
) -> *mut MongoFuture {
    use crate::ffi::ops::drop::{execute_drop_collection, prepare_drop_collection};

    let client_ref = &*client;

    let prep = (|| -> crate::error::Result<_> {
        prepare_drop_collection(&client_ref.client, db_name, coll_name)
    })();

    match prep {
        Err(e) => MongoFuture::from_error(e),
        Ok(coll) => {
            let session = context.session();
            let write_concern = context.write_concern();
            let handle = client_ref.runtime.spawn(async move {
                execute_drop_collection(coll, session, write_concern).await?;
                Ok(FutureValue::Void)
            });
            MongoFuture::from_join_handle(&client_ref.runtime, handle)
        }
    }
}
