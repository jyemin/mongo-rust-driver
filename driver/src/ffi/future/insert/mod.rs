//! Future-based FFI insert operations.

#[cfg(test)]
mod tests;

use std::ffi::c_char;

use super::{
    client::MongoClient,
    future::{FutureValue, MongoFuture},
};
use crate::ffi::{
    ops::insert::{InsertManyResult, InsertOneResult, InsertedId},
    types::{Bson, BsonArray, BsonValue, ContextExt, OperationContext, OwnedBsonValue},
};

/// Insert a single document asynchronously.
///
/// Returns a `MongoFuture` that resolves to an `InsertOneResult`.
///
/// # Safety
///
/// - `client` must be a valid pointer from `mongoc_future_client_new`
/// - `db_name`, `coll_name` must be valid null-terminated C strings
/// - `document` must be a valid pointer to a BSON document
/// - `ctx` may be null (no session/options)
/// - `comment` may be null or a valid pointer to a BsonValue
#[no_mangle]
pub unsafe extern "C" fn mongoc_future_insert_one(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    document: *const Bson,
    bypass_document_validation: i8,
    comment: *const BsonValue,
) -> *mut MongoFuture {
    use crate::ffi::ops::insert::{execute_insert_one, prepare_insert_one};

    let client_ref = &*client;

    let prep = (|| -> crate::error::Result<_> {
        prepare_insert_one(
            &client_ref.client,
            ctx,
            db_name,
            coll_name,
            document,
            bypass_document_validation,
            comment,
        )
    })();

    match prep {
        Err(e) => MongoFuture::from_error(e),
        Ok((coll, raw_doc, options)) => {
            let session = ctx.session();
            let handle = client_ref.runtime.spawn(async move {
                let result = execute_insert_one(coll, raw_doc, options, session).await?;
                let owned_id = OwnedBsonValue::from_bson(&result.inserted_id)?;
                Ok(FutureValue::InsertOne(InsertOneResult {
                    inserted_id: owned_id,
                }))
            });
            MongoFuture::from_join_handle(&client_ref.runtime, handle)
        }
    }
}

/// Insert multiple documents into a collection.
///
/// Returns a `MongoFuture` that resolves to an `InsertManyResult`.
///
/// # Safety
///
/// - `client` must be a valid pointer from `mongoc_future_client_new`
/// - `db_name`, `coll_name` must be valid null-terminated C strings
/// - `documents` must be a valid BsonArray
/// - `ctx` may be null (no session/options)
/// - `comment` may be null or a valid pointer to a BsonValue
#[no_mangle]
pub unsafe extern "C" fn mongoc_future_insert_many(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    documents: BsonArray,
    bypass_document_validation: i8,
    ordered: bool,
    comment: *const BsonValue,
) -> *mut MongoFuture {
    use crate::ffi::ops::insert::{execute_insert_many, prepare_insert_many};

    let client_ref = &*client;

    let prep = (|| -> crate::error::Result<_> {
        prepare_insert_many(
            &client_ref.client,
            ctx,
            db_name,
            coll_name,
            &documents,
            bypass_document_validation,
            ordered,
            comment,
        )
    })();

    match prep {
        Err(e) => MongoFuture::from_error(e),
        Ok((coll, options)) => {
            let session = ctx.session();
            let handle = client_ref.runtime.spawn(async move {
                let crate::results::InsertManyResult { inserted_ids } =
                    execute_insert_many(coll, documents, options, session).await?;
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
                Ok(FutureValue::InsertMany(inserted_arr, result))
            });
            MongoFuture::from_join_handle(&client_ref.runtime, handle)
        }
    }
}
