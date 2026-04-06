//! FFI drop operations.
//!
//! This module provides C-compatible APIs for dropping databases and collections.

#[cfg(test)]
mod tests;

use std::{ffi::c_void, os::raw::c_char};

use super::client::MongoClient;
use crate::ffi::{
    error::Error,
    types::{ContextExt, OperationContext},
    utils::with_void_err_callback,
};

/// Callback type for drop operations.
///
/// - `userdata`: The userdata pointer passed to the drop function
/// - `error`: Error details (null on success)
pub type DropCallback = extern "C" fn(userdata: *mut c_void, error: *const Error);

/// Drop a database asynchronously.
///
/// # Safety
///
/// - `client` must be a valid pointer returned from `mongo_client_new`
/// - `db_name` must be a valid null-terminated C string
/// - `callback` must be a valid function pointer
/// - `userdata` can be any value and will be passed to the callback
#[no_mangle]
pub unsafe extern "C" fn mongo_drop_database(
    client: *mut MongoClient,
    context: *mut OperationContext,
    db_name: *const c_char,
    callback: DropCallback,
    userdata: *mut c_void,
) {
    use crate::ffi::ops::drop::{execute_drop_database, prepare_drop_database};

    let db = with_void_err_callback!(callback, userdata, || {
        if client.is_null() {
            return Err(crate::error::Error::invalid_argument(
                "client cannot be null",
            ));
        }
        prepare_drop_database(&(*client).client, db_name)
    });

    let client_ref = &*client;
    let userdata_ptr = userdata as usize;
    let session_ref = context.session();
    let write_concern = context.write_concern();
    client_ref.runtime.spawn(async move {
        let result = execute_drop_database(db, session_ref, write_concern).await;

        let userdata = userdata_ptr as *mut c_void;
        match result {
            Ok(()) => callback(userdata, std::ptr::null()),
            Err(e) => callback(userdata, &crate::ffi::error::Error::from(&e)),
        }
    });
}

/// Drop a collection asynchronously.
///
/// # Safety
///
/// - `client` must be a valid pointer returned from `mongo_client_new`
/// - `db_name` must be a valid null-terminated C string
/// - `coll_name` must be a valid null-terminated C string
/// - `callback` must be a valid function pointer
/// - `userdata` can be any value and will be passed to the callback
#[no_mangle]
pub unsafe extern "C" fn mongo_drop_collection(
    client: *mut MongoClient,
    context: *mut OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    callback: DropCallback,
    userdata: *mut c_void,
) {
    use crate::ffi::ops::drop::{execute_drop_collection, prepare_drop_collection};

    let coll = with_void_err_callback!(callback, userdata, || {
        if client.is_null() {
            return Err(crate::error::Error::invalid_argument(
                "client cannot be null",
            ));
        }
        prepare_drop_collection(&(*client).client, db_name, coll_name)
    });

    let client_ref = &*client;
    let userdata_ptr = userdata as usize;
    let session_ref = context.session();
    let write_concern = context.write_concern();
    client_ref.runtime.spawn(async move {
        let result = execute_drop_collection(coll, session_ref, write_concern).await;

        let userdata = userdata_ptr as *mut c_void;
        match result {
            Ok(()) => callback(userdata, std::ptr::null()),
            Err(e) => callback(userdata, &crate::ffi::error::Error::from(&e)),
        }
    });
}
