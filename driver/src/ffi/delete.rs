//! FFI delete operations.

#[cfg(test)]
mod tests;

use std::ffi::{c_char, c_void};

use crate::ffi::{
    client::MongoClient,
    error::Error,
    types::{Bson, BsonValue, ContextExt, OperationContext},
    utils::with_err_callback,
};

/// Callback for delete operation results.
pub type DeleteCallback = extern "C" fn(
    userdata: *mut c_void,
    result: *const DeleteResult,
    error: *const Error,
);

/// Result of a delete_one or delete_many operation.
#[repr(C)]
pub struct DeleteResult {
    pub deleted_count: u64,
}

/// Options for delete operations.
///
/// All pointer fields are nullable (null = not set).
/// `write_concern` comes from `OperationContext`, not this struct.
#[repr(C)]
pub struct DeleteOptions {
    /// Collation as a serialized BSON document. Nullable.
    pub collation: *const Bson,
    /// Index hint by name. Nullable. Takes precedence over `hint_keys`.
    pub hint_name: *const c_char,
    /// Index hint by key pattern as BSON document. Nullable.
    pub hint_keys: *const Bson,
    /// Variables for MQL expressions (`$let`). Nullable BSON document.
    pub let_vars: *const Bson,
    /// Comment BSON value. Nullable.
    pub comment: *const BsonValue,
}

/// Delete up to one document matching `filter`.
///
/// # Safety
///
/// - `client` must be a valid pointer from `mongo_client_new`
/// - `db_name`, `coll_name` must be valid null-terminated C strings
/// - `filter` must be a valid BSON document pointer
/// - `opts` may be null (use defaults)
/// - `ctx` may be null (no session/write concern)
#[no_mangle]
pub unsafe extern "C" fn mongo_delete_one(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    filter: *const Bson,
    opts: *const DeleteOptions,
    callback: DeleteCallback,
    userdata: *mut c_void,
) {
    let (coll, filter_doc, options) = with_err_callback!(callback, userdata, || {
        use crate::error::Error;
        if client.is_null() {
            return Err(Error::invalid_argument("client cannot be null"));
        }
        super::ops::delete::prepare_delete(
            &(*client).client,
            ctx,
            db_name,
            coll_name,
            filter,
            opts,
        )
    });

    let session_ref = ctx.session();
    let userdata_ptr = userdata as usize;
    let client_ref = &*client;
    client_ref.runtime.spawn(async move {
        let result =
            super::ops::delete::execute_delete_one(coll, filter_doc, options, session_ref).await;

        let userdata = userdata_ptr as *mut c_void;
        with_err_callback!(callback, userdata, || {
            let r = result?;
            callback(
                userdata,
                &DeleteResult {
                    deleted_count: r.deleted_count,
                },
                std::ptr::null(),
            );
            Ok(())
        });
    });
}

/// Delete all documents matching `filter`.
///
/// # Safety
///
/// Same safety requirements as `mongo_delete_one`.
#[no_mangle]
pub unsafe extern "C" fn mongo_delete_many(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    filter: *const Bson,
    opts: *const DeleteOptions,
    callback: DeleteCallback,
    userdata: *mut c_void,
) {
    let (coll, filter_doc, options) = with_err_callback!(callback, userdata, || {
        use crate::error::Error;
        if client.is_null() {
            return Err(Error::invalid_argument("client cannot be null"));
        }
        super::ops::delete::prepare_delete(
            &(*client).client,
            ctx,
            db_name,
            coll_name,
            filter,
            opts,
        )
    });

    let session_ref = ctx.session();
    let userdata_ptr = userdata as usize;
    let client_ref = &*client;
    client_ref.runtime.spawn(async move {
        let result =
            super::ops::delete::execute_delete_many(coll, filter_doc, options, session_ref).await;

        let userdata = userdata_ptr as *mut c_void;
        with_err_callback!(callback, userdata, || {
            let r = result?;
            callback(
                userdata,
                &DeleteResult {
                    deleted_count: r.deleted_count,
                },
                std::ptr::null(),
            );
            Ok(())
        });
    });
}
