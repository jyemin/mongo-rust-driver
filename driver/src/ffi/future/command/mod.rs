//! Future-based FFI command operations.

#[cfg(test)]
mod tests;

use std::os::raw::c_char;

use super::{
    client::MongoClient,
    future::{FutureValue, MongoFuture},
};
use crate::ffi::types::{Bson, ContextExt, OwnedBson, OperationContext};

/// Run a database command asynchronously.
///
/// Returns a `MongoFuture` that resolves to a `Document`.
///
/// # Safety
///
/// - `client` must be a valid pointer from `mongoc_future_client_new`
/// - `db_name` must be a valid null-terminated C string
/// - `command` must be a valid pointer to a BSON document
/// - `context` may be null or a valid pointer to an OperationContext
#[no_mangle]
pub unsafe extern "C" fn mongoc_future_run_command(
    client: *mut MongoClient,
    context: *mut OperationContext,
    db_name: *const c_char,
    command: *const Bson,
) -> *mut MongoFuture {
    use crate::ffi::ops::command::{execute_run_command, prepare_run_command};

    let client_ref = &*client;

    let prep = (|| -> crate::error::Result<_> {
        prepare_run_command(&client_ref.client, context, db_name, command)
    })();

    match prep {
        Err(e) => MongoFuture::from_error(e),
        Ok((db, options, command_doc)) => {
            let session = context.session();
            let handle = client_ref.runtime.spawn(async move {
                let result = execute_run_command(db, options, command_doc, session).await?;
                Ok(FutureValue::Document(Some(OwnedBson::from_doc(&result))))
            });
            MongoFuture::from_join_handle(&client_ref.runtime, handle)
        }
    }
}
