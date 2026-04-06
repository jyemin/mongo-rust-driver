//! FFI update and replace operations.
#[cfg(test)]
mod tests;

use std::ffi::{c_char, c_void};

use super::client::MongoClient;
use crate::ffi::{
    error::Error,
    ops::update::{ReplaceOneOptions, UpdateOneOptions, UpdateResult},
    types::{Bson, BsonArray, ContextExt, OperationContext},
    utils::with_err_callback,
};

/// Callback for update/replace operation results.
pub type UpdateCallback = extern "C" fn(
    userdata: *mut c_void,
    result: *const UpdateResult,
    error: *const Error,
);

/// Update up to one document matching `filter`.
///
/// Exactly one of `update` (BSON document) or `pipeline` (non-empty array) must be provided.
///
/// # Safety
///
/// - `client` must be a valid pointer from `mongo_client_new`
/// - `db_name`, `coll_name` must be valid null-terminated C strings
/// - `filter` must be a valid BSON document pointer
/// - Exactly one of `update` or `pipeline` must be non-null/non-empty
/// - `opts` may be null (use defaults)
/// - `ctx` may be null (no session/write concern)
#[no_mangle]
pub unsafe extern "C" fn mongo_update_one(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    filter: *const Bson,
    update: *const Bson,
    pipeline: BsonArray,
    opts: *const UpdateOneOptions,
    callback: UpdateCallback,
    userdata: *mut c_void,
) {
    let (coll, filter_doc, modifications, options) = with_err_callback!(callback, userdata, || {
        use crate::error::Error;
        if client.is_null() {
            return Err(Error::invalid_argument("client cannot be null"));
        }
        crate::ffi::ops::update::prepare_update(
            &(*client).client,
            ctx,
            db_name,
            coll_name,
            filter,
            update,
            &pipeline,
            opts,
        )
    });

    let session_ref = ctx.session();
    let userdata_ptr = userdata as usize;
    let client_ref = &*client;
    client_ref.runtime.spawn(async move {
        let result = crate::ffi::ops::update::execute_update_one(
            coll,
            filter_doc,
            modifications,
            options,
            session_ref,
        )
        .await;

        let userdata = userdata_ptr as *mut c_void;
        with_err_callback!(callback, userdata, || {
            let out = crate::ffi::ops::update::build_update_result(result?)?;
            callback(userdata, &out, std::ptr::null());
            Ok(())
        });
    });
}

/// Update all documents matching `filter`.
///
/// Exactly one of `update` (BSON document) or `pipeline` (non-empty array) must be provided.
///
/// # Safety
///
/// Same safety requirements as `mongo_update_one`.
#[no_mangle]
pub unsafe extern "C" fn mongo_update_many(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    filter: *const Bson,
    update: *const Bson,
    pipeline: BsonArray,
    opts: *const UpdateOneOptions,
    callback: UpdateCallback,
    userdata: *mut c_void,
) {
    let (coll, filter_doc, modifications, options) = with_err_callback!(callback, userdata, || {
        use crate::error::Error;
        if client.is_null() {
            return Err(Error::invalid_argument("client cannot be null"));
        }
        crate::ffi::ops::update::prepare_update(
            &(*client).client,
            ctx,
            db_name,
            coll_name,
            filter,
            update,
            &pipeline,
            opts,
        )
    });

    let session_ref = ctx.session();
    let userdata_ptr = userdata as usize;
    let client_ref = &*client;
    client_ref.runtime.spawn(async move {
        let result = crate::ffi::ops::update::execute_update_many(
            coll,
            filter_doc,
            modifications,
            options,
            session_ref,
        )
        .await;

        let userdata = userdata_ptr as *mut c_void;
        with_err_callback!(callback, userdata, || {
            let out = crate::ffi::ops::update::build_update_result(result?)?;
            callback(userdata, &out, std::ptr::null());
            Ok(())
        });
    });
}

/// Replace up to one document matching `filter` with `replacement`.
///
/// # Safety
///
/// - `client` must be a valid pointer from `mongo_client_new`
/// - `db_name`, `coll_name` must be valid null-terminated C strings
/// - `filter` must be a valid BSON document pointer
/// - `replacement` must be a valid BSON document pointer
/// - `opts` may be null (use defaults)
/// - `ctx` may be null (no session/write concern)
#[no_mangle]
pub unsafe extern "C" fn mongo_replace_one(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    filter: *const Bson,
    replacement: *const Bson,
    opts: *const ReplaceOneOptions,
    callback: UpdateCallback,
    userdata: *mut c_void,
) {
    let (coll, filter_doc, replacement_doc, options) =
        with_err_callback!(callback, userdata, || {
            use crate::error::Error;
            if client.is_null() {
                return Err(Error::invalid_argument("client cannot be null"));
            }
            crate::ffi::ops::update::prepare_replace(
                &(*client).client,
                ctx,
                db_name,
                coll_name,
                filter,
                replacement,
                opts,
            )
        });

    let session_ref = ctx.session();
    let userdata_ptr = userdata as usize;
    let client_ref = &*client;
    client_ref.runtime.spawn(async move {
        let result = crate::ffi::ops::update::execute_replace_one(
            coll,
            filter_doc,
            replacement_doc,
            options,
            session_ref,
        )
        .await;

        let userdata = userdata_ptr as *mut c_void;
        with_err_callback!(callback, userdata, || {
            let out = crate::ffi::ops::update::build_update_result(result?)?;
            callback(userdata, &out, std::ptr::null());
            Ok(())
        });
    });
}
