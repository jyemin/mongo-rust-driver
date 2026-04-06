//! FFI distinct operation.

#[cfg(test)]
mod tests;

use std::ffi::{c_char, c_void};

use super::client::MongoClient;
use crate::ffi::{
    error::Error,
    ops::distinct::{DistinctOptions, DistinctResult},
    types::{Bson, ContextExt, OwnedBsonValue, OperationContext},
};

/// Callback for distinct results.
/// On error: `result` is null and `error` is non-null.
/// On success: `result` is non-null and `error` is null.
pub type DistinctCallback =
    extern "C" fn(userdata: *mut c_void, result: *const DistinctResult, error: *const Error);

/// Find the distinct values for a specified field across a collection.
///
/// If `filter` is null, all documents are considered (equivalent to `{}`).
///
/// # Safety
///
/// - `client` must be a valid pointer from `mongo_client_new`
/// - `db_name`, `coll_name`, `field_name` must be valid null-terminated C strings
/// - `filter` may be null (considers all documents) or a valid BSON document pointer
/// - `opts` may be null (use defaults)
/// - `ctx` may be null (no session/read concern)
#[no_mangle]
pub unsafe extern "C" fn mongo_distinct(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    field_name: *const c_char,
    filter: *const Bson,
    opts: *const DistinctOptions,
    callback: DistinctCallback,
    userdata: *mut c_void,
) {
    use crate::ffi::ops::distinct::{execute_distinct, prepare_distinct};

    let setup = (|| -> crate::error::Result<_> {
        if client.is_null() {
            return Err(crate::error::Error::invalid_argument(
                "client cannot be null",
            ));
        }
        prepare_distinct(&(*client).client, ctx, db_name, coll_name, field_name, filter, opts)
    })();

    let (coll, field, filter_doc, options) = match setup {
        Ok(v) => v,
        Err(e) => {
            callback(userdata, std::ptr::null(), &Error::from(&e));
            return;
        }
    };

    let session_ref = ctx.session();
    let userdata_ptr = userdata as usize;
    let client_ref = &*client;
    client_ref.runtime.spawn(async move {
        let result = execute_distinct(coll, field, filter_doc, options, session_ref).await;

        let userdata = userdata_ptr as *mut c_void;
        match result {
            Ok(values) => {
                let owned: Result<Vec<OwnedBsonValue>, _> = values
                    .iter()
                    .map(|v| OwnedBsonValue::from_bson(v))
                    .collect();
                match owned {
                    Ok(owned_values) => {
                        let out = DistinctResult {
                            values: owned_values.as_ptr(),
                            len: owned_values.len(),
                        };
                        callback(userdata, &out, std::ptr::null());
                    }
                    Err(e) => {
                        callback(userdata, std::ptr::null(), &Error::from(&e));
                    }
                }
            }
            Err(e) => {
                callback(userdata, std::ptr::null(), &Error::from(&e));
            }
        }
    });
}
