//! Future-based FFI distinct operation.

use std::ffi::c_char;

use super::{
    client::MongoClient,
    future::{FutureValue, MongoFuture},
};
use crate::ffi::{
    ops::distinct::{DistinctOptions, DistinctResult},
    types::{Bson, ContextExt, OwnedBsonValue, OperationContext},
};

/// Find the distinct values for a specified field across a collection.
///
/// Returns a `MongoFuture` that resolves to a `DistinctResult`.
///
/// # Safety
///
/// - `client` must be a valid pointer from `mongoc_future_client_new`
/// - `db_name`, `coll_name`, `field_name` must be valid null-terminated C strings
/// - `filter` may be null (considers all documents) or a valid BSON document pointer
/// - `opts` may be null (use defaults)
/// - `ctx` may be null (no session/read concern)
#[no_mangle]
pub unsafe extern "C" fn mongoc_future_distinct(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    field_name: *const c_char,
    filter: *const Bson,
    opts: *const DistinctOptions,
) -> *mut MongoFuture {
    use crate::ffi::ops::distinct::{execute_distinct, prepare_distinct};

    let client_ref = &*client;

    let prep = (|| -> crate::error::Result<_> {
        prepare_distinct(
            &client_ref.client,
            ctx,
            db_name,
            coll_name,
            field_name,
            filter,
            opts,
        )
    })();

    match prep {
        Err(e) => MongoFuture::from_error(e),
        Ok((coll, field, filter_doc, options)) => {
            let session = ctx.session();
            let handle = client_ref.runtime.spawn(async move {
                let values = execute_distinct(coll, field, filter_doc, options, session).await?;
                let owned: Vec<OwnedBsonValue> = values
                    .iter()
                    .map(|v| OwnedBsonValue::from_bson(v))
                    .collect::<crate::error::Result<_>>()?;
                let result = DistinctResult {
                    values: owned.as_ptr(),
                    len: owned.len(),
                };
                Ok(FutureValue::Distinct(owned, result))
            });
            MongoFuture::from_join_handle(&client_ref.runtime, handle)
        }
    }
}
