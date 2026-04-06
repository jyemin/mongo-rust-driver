//! FFI find-and-modify operations.

#[cfg(test)]
mod tests;

use std::ffi::{c_char, c_void};

use super::client::MongoClient;
use crate::ffi::{
    error::Error,
    ops::find_one::{FindOneAndDeleteOptions, FindOneAndReplaceOptions, FindOneAndUpdateOptions},
    types::{Bson, BsonArray, ContextExt, OwnedBson, OperationContext},
    utils::with_err_callback,
};

/// Callback for find-and-modify results.
/// `result` is null when no document matched (success with no match) or on error.
/// Check `error` to distinguish the two cases.
pub type FindOneCallback = extern "C" fn(
    userdata: *mut c_void,
    result: *const OwnedBson,
    error: *const Error,
);


/// Atomically find a document matching `filter` and delete it.
///
/// Callback receives null result when no document matched.
///
/// # Safety
///
/// - `client` must be a valid pointer from `mongo_client_new`
/// - `db_name`, `coll_name` must be valid null-terminated C strings
/// - `filter` must be a valid BSON document pointer
/// - `opts` and `ctx` may be null
#[no_mangle]
pub unsafe extern "C" fn mongo_find_one_and_delete(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    filter: *const Bson,
    opts: *const FindOneAndDeleteOptions,
    callback: FindOneCallback,
    userdata: *mut c_void,
) {
    let (coll, filter_doc, options) = with_err_callback!(callback, userdata, || {
        use crate::error::Error;
        if client.is_null() {
            return Err(Error::invalid_argument("client cannot be null"));
        }
        crate::ffi::ops::find_one::prepare_find_one_and_delete(
            &(*client).client, ctx, db_name, coll_name, filter, opts,
        )
    });

    let session_ref = ctx.session();
    let userdata_ptr = userdata as usize;
    let client_ref = &*client;
    client_ref.runtime.spawn(async move {
        let result = crate::ffi::ops::find_one::execute_find_one_and_delete(
            coll, filter_doc, options, session_ref,
        )
        .await;

        let userdata = userdata_ptr as *mut c_void;
        with_err_callback!(callback, userdata, || {
            match result? {
                None => callback(userdata, std::ptr::null(), std::ptr::null()),
                Some(doc) => callback(userdata, &OwnedBson::from_doc(&doc), std::ptr::null()),
            }
            Ok(())
        });
    });
}

/// Atomically find a document matching `filter` and update it.
///
/// Exactly one of `update_doc` (non-null) or `update_pipeline` (non-empty) must be provided.
/// Callback receives null result when no document matched.
///
/// # Safety
///
/// Same safety requirements as `mongo_find_one_and_delete`.
#[no_mangle]
pub unsafe extern "C" fn mongo_find_one_and_update(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    filter: *const Bson,
    update_doc: *const Bson,
    update_pipeline: BsonArray,
    opts: *const FindOneAndUpdateOptions,
    callback: FindOneCallback,
    userdata: *mut c_void,
) {
    let (coll, filter_doc, update, options) = with_err_callback!(callback, userdata, || {
        use crate::error::Error;
        if client.is_null() {
            return Err(Error::invalid_argument("client cannot be null"));
        }
        crate::ffi::ops::find_one::prepare_find_one_and_update(
            &(*client).client, ctx, db_name, coll_name, filter, update_doc, update_pipeline, opts,
        )
    });

    let session_ref = ctx.session();
    let userdata_ptr = userdata as usize;
    let client_ref = &*client;
    client_ref.runtime.spawn(async move {
        let result = crate::ffi::ops::find_one::execute_find_one_and_update(
            coll, filter_doc, update, options, session_ref,
        )
        .await;

        let userdata = userdata_ptr as *mut c_void;
        with_err_callback!(callback, userdata, || {
            match result? {
                None => callback(userdata, std::ptr::null(), std::ptr::null()),
                Some(doc) => callback(userdata, &OwnedBson::from_doc(&doc), std::ptr::null()),
            }
            Ok(())
        });
    });
}

/// Atomically find a document matching `filter` and replace it with `replacement`.
///
/// Callback receives null result when no document matched.
///
/// # Safety
///
/// Same safety requirements as `mongo_find_one_and_delete`.
#[no_mangle]
pub unsafe extern "C" fn mongo_find_one_and_replace(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    filter: *const Bson,
    replacement: *const Bson,
    opts: *const FindOneAndReplaceOptions,
    callback: FindOneCallback,
    userdata: *mut c_void,
) {
    let (coll, filter_doc, replacement_doc, options) = with_err_callback!(callback, userdata, || {
        use crate::error::Error;
        if client.is_null() {
            return Err(Error::invalid_argument("client cannot be null"));
        }
        crate::ffi::ops::find_one::prepare_find_one_and_replace(
            &(*client).client, ctx, db_name, coll_name, filter, replacement, opts,
        )
    });

    let session_ref = ctx.session();
    let userdata_ptr = userdata as usize;
    let client_ref = &*client;
    client_ref.runtime.spawn(async move {
        let result = crate::ffi::ops::find_one::execute_find_one_and_replace(
            coll, filter_doc, replacement_doc, options, session_ref,
        )
        .await;

        let userdata = userdata_ptr as *mut c_void;
        with_err_callback!(callback, userdata, || {
            match result? {
                None => callback(userdata, std::ptr::null(), std::ptr::null()),
                Some(doc) => callback(userdata, &OwnedBson::from_doc(&doc), std::ptr::null()),
            }
            Ok(())
        });
    });
}
