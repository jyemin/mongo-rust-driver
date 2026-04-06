//! Future-based FFI cursor definitions.

use futures_util::stream::StreamExt;

use crate::{
    ffi::types::BsonArray,
    raw_batch_cursor::{RawBatchCursor, SessionRawBatchCursor},
    ClientSession,
};

use super::{
    client::MongoClient,
    future::{FutureValue, MongoFuture},
};

/// A handle used to request batches of results from the server.
// Not named `Cursor` because cbindgen gets confused between this and `crate::Cursor`; renamed back
// to `Cursor` in cbindgen.toml.
#[allow(missing_docs)]
pub enum FfiCursor {
    Base(RawBatchCursor),
    Session(SessionRawBatchCursor),
}

/// Common result for all cursor-returning operations.
#[repr(C)]
pub struct CursorResult {
    /// null if exhausted with single batch
    pub cursor: *mut FfiCursor,
    /// true if no more batches (cursor already closed)
    pub exhausted: bool,
    /// raw BSON array of documents from initial response
    pub first_batch: BsonArray,
}

// Safety: CursorResult contains raw pointers that are only accessed from the
// single FFI thread that owns the client and its runtime.
unsafe impl Send for CursorResult {}

/// Get more results from a cursor.
///
/// Returns a `MongoFuture` that resolves to a `GetMore` result. The caller
/// must tick the client runtime to drive the operation.
///
/// # Safety
///
/// - `client` must be a valid pointer from `mongoc_future_client_new`
/// - `cursor` must be a valid pointer from a previous find/aggregate result
/// - `session` must be null for cursors created without a session, or a valid
///   session pointer for cursors created with one
#[no_mangle]
pub unsafe extern "C" fn mongoc_future_cursor_get_more(
    client: *mut MongoClient,
    cursor: *mut FfiCursor,
    session: *mut ClientSession,
) -> *mut MongoFuture {
    let validate = || -> crate::error::Result<()> {
        use crate::error::Error;

        if client.is_null() {
            return Err(Error::invalid_argument("client cannot be null"));
        }
        if cursor.is_null() {
            return Err(Error::invalid_argument("cursor cannot be null"));
        }
        let cursor = &*cursor;
        match cursor {
            FfiCursor::Base(_) => {
                if !session.is_null() {
                    return Err(Error::invalid_argument(
                        "cursors created without a session must not be iterated with one",
                    ));
                }
            }
            FfiCursor::Session(_) => {
                if session.is_null() {
                    return Err(Error::invalid_argument(
                        "cursors created with a session must be iterated with that session",
                    ));
                }
            }
        }
        Ok(())
    };

    if let Err(e) = validate() {
        return MongoFuture::from_error(e);
    }

    let client_ref = &*client;
    let cursor_ptr = cursor as usize;
    let session_ptr = session as usize;

    let handle = client_ref.runtime.spawn(async move {
        let cursor = cursor_ptr as *mut FfiCursor;
        let session = session_ptr as *mut ClientSession;

        let (batch, exhausted) = match &mut *cursor {
            FfiCursor::Base(c) => (c.next().await, c.is_exhausted()),
            FfiCursor::Session(c) => (
                c.stream(&mut *session).next().await,
                c.is_exhausted(),
            ),
        };

        let batch = batch.ok_or_else(|| {
            crate::error::Error::invalid_response(
                "no batch returned for unexhausted cursor",
            )
        })??;

        let (doc_ptrs, data) = BsonArray::from_batch(&batch)?;

        Ok(FutureValue::GetMore {
            exhausted,
            _raw_batch: Some(batch),
            _doc_ptrs: Some(doc_ptrs),
            data,
        })
    });

    MongoFuture::from_join_handle(&client_ref.runtime, handle)
}

/// Close and free a cursor synchronously.
///
/// # Safety
///
/// `cursor` must be a valid pointer from a previous find/aggregate result, or null.
#[no_mangle]
pub unsafe extern "C" fn mongoc_future_cursor_close(cursor: *mut FfiCursor) {
    if !cursor.is_null() {
        drop(Box::from_raw(cursor));
    }
}
