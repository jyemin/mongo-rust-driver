//! FFI insert operations.
//!
//! This module provides C-compatible APIs for document insertion.

#[cfg(test)]
mod tests;

use std::ffi::{c_char, c_void};

use super::client::MongoClient;
use crate::ffi::{
    error::Error,
    ops::insert::{InsertManyResult, InsertOneResult, InsertedId},
    types::{Bson, BsonArray, BsonValue, ContextExt, OperationContext, OwnedBsonValue},
    utils::with_err_callback,
};

/// Callback type for insert_one results.
///
/// - `userdata`: The userdata pointer passed to `mongo_insert_one`
/// - `result`: The insert result (null on error)
/// - `error`: Error details (null on success)
///
/// Result and error pointers are valid only during callback invocation.
pub type InsertOneCallback = extern "C" fn(
    userdata: *mut c_void,
    result: *const InsertOneResult, // null on error
    error: *const Error,            // null on success
);

/// Insert a single document asynchronously.
///
/// # Safety
///
/// - `client` must be a valid pointer returned from `mongo_client_new`
/// - `db_name` must be a valid null-terminated C string
/// - `coll_name` must be a valid null-terminated C string
/// - `document` must be a valid pointer to a BSON document
/// - `ctx` can be null (no session/options) or a valid pointer to OperationContext
/// - `comment` can be null or a valid pointer to a BsonValue
/// - `callback` must be a valid function pointer
/// - `userdata` can be any value and will be passed to the callback
#[no_mangle]
pub unsafe extern "C" fn mongo_insert_one(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    document: *const Bson,
    bypass_document_validation: i8, // -1 = None, 0 = false, 1 = true
    comment: *const BsonValue,
    callback: InsertOneCallback,
    userdata: *mut c_void,
) {
    let (coll, raw_doc, options) = with_err_callback!(callback, userdata, || {
        use crate::error::Error;
        if client.is_null() {
            return Err(Error::invalid_argument("client cannot be null"));
        }
        crate::ffi::ops::insert::prepare_insert_one(
            &(*client).client,
            ctx,
            db_name,
            coll_name,
            document,
            bypass_document_validation,
            comment,
        )
    });

    let session_ref = ctx.session();
    let userdata_ptr = userdata as usize;
    let client_ref = &*client;
    client_ref.runtime.spawn(async move {
        let result =
            crate::ffi::ops::insert::execute_insert_one(coll, raw_doc, options, session_ref).await;

        let userdata = userdata_ptr as *mut c_void;
        with_err_callback!(callback, userdata, || {
            let result = result?;
            let owned_id = OwnedBsonValue::from_bson(&result.inserted_id)?;
            let out = InsertOneResult {
                inserted_id: owned_id,
            };
            callback(userdata, &out, std::ptr::null());
            Ok(())
        });
    });
}

/// Callback for `mongo_insert_many` results.
pub type InsertManyCallback = extern "C" fn(
    userdata: *mut c_void,
    result: *const InsertManyResult, // null on error
    error: *const Error,             // null on success
);

/// Insert multiple documents into a collection.
#[no_mangle]
pub unsafe extern "C" fn mongo_insert_many(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    documents: BsonArray,
    // options
    bypass_document_validation: i8, // -1 = None, 0 = false, 1 = true
    ordered: bool,
    comment: *const BsonValue,
    // result
    callback: InsertManyCallback,
    userdata: *mut c_void,
) {
    let (coll, options) = with_err_callback!(callback, userdata, || {
        use crate::error::Error;
        if client.is_null() {
            return Err(Error::invalid_argument("client cannot be null"));
        }
        crate::ffi::ops::insert::prepare_insert_many(
            &(*client).client,
            ctx,
            db_name,
            coll_name,
            &documents,
            bypass_document_validation,
            ordered,
            comment,
        )
    });

    let session_ref = ctx.session();
    let userdata_ptr = userdata as usize;
    let client_ref = &*client;
    client_ref.runtime.spawn(async move {
        let result =
            crate::ffi::ops::insert::execute_insert_many(coll, documents, options, session_ref).await;

        let userdata = userdata_ptr as *mut c_void;
        let (_inserted_arr, result) = with_err_callback!(callback, userdata, || {
            let crate::results::InsertManyResult { inserted_ids } = result?;
            let mut inserted_arr = vec![];
            for (index, id) in inserted_ids {
                inserted_arr.push(InsertedId {
                    index,
                    id: OwnedBsonValue::from_bson(&id)?,
                });
            }
            let result = InsertManyResult {
                inserted_ids: inserted_arr.as_ptr(),
                inserted_ids_len: inserted_arr.len(),
            };
            Ok((inserted_arr, result))
        });
        callback(userdata, &result, std::ptr::null());
    });
}
