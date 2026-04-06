//! FFI command operations.
//!
//! This module provides C-compatible APIs for database commands.

#[cfg(test)]
mod tests;

use std::{ffi::c_void, os::raw::c_char};

use crate::ffi::utils::with_err_callback;

use super::{
    client::MongoClient,
    error::Error,
    types::{Bson, ContextExt, OperationContext, OwnedBson},
};

/// Callback type for run_command results.
///
/// - `userdata`: The userdata pointer passed to `mongo_run_command`
/// - `result`: The command result as a BSON document (null on error)
/// - `error`: Error details (null on success)
///
/// Result and error pointers are valid only during callback invocation.
/// Copy any data you need before returning from the callback.
pub type RunCommandCallback =
    extern "C" fn(userdata: *mut c_void, result: *const OwnedBson, error: *const Error);

/// Run a database command asynchronously.
///
/// # Safety
///
/// - `client` must be a valid pointer returned from `mongo_client_new`
/// - `db_name` must be a valid null-terminated C string
/// - `command` must be a valid pointer to a BSON document
/// - `command` must remain valid until the callback is invoked
/// - `session` can be null (no session) or a valid pointer to a Session
/// - `callback` must be a valid function pointer
/// - `userdata` can be any value and will be passed to the callback
///
/// The `read_preference_mode` parameter:
/// - 0 = Primary
/// - 1 = PrimaryPreferred
/// - 2 = Secondary
/// - 3 = SecondaryPreferred
/// - 4 = Nearest
/// - 255 = Not set (use default)
#[no_mangle]
pub unsafe extern "C" fn mongo_run_command(
    client: *mut MongoClient,
    context: *mut OperationContext,
    db_name: *const c_char,
    command: *const Bson,
    callback: RunCommandCallback,
    userdata: *mut c_void,
) {
    use crate::ffi::ops::command::{execute_run_command, prepare_run_command};

    let (db, options, command_doc) = with_err_callback!(callback, userdata, || {
        if client.is_null() {
            return Err(crate::error::Error::invalid_argument(
                "client cannot be null",
            ));
        }
        prepare_run_command(&(*client).client, context, db_name, command)
    });

    let userdata_ptr = userdata as usize;
    let session_ref = context.session();
    let client_ref = &*client;
    client_ref.runtime.spawn(async move {
        let result = execute_run_command(db, options, command_doc, session_ref).await;

        let userdata = userdata_ptr as *mut c_void;
        with_err_callback!(callback, userdata, || {
            let result = result?;
            callback(userdata, &OwnedBson::from_doc(&result), std::ptr::null());
            Ok(())
        });
    });
}
