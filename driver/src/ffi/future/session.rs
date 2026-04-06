//! Future-based FFI session management.

use super::{
    client::MongoClient,
    future::{FutureValue, MongoFuture},
};
use crate::ffi::{
    error::{Error, InvalidArgumentError},
    ops::session::{SessionOptions, TransactionOptions, parse_session_options, parse_transaction_options},
    types::ClientSession,
};

/// Start a new session.
///
/// This is a synchronous operation that blocks on the client's runtime.
/// Returns a session handle on success, or null on error.
///
/// # Safety
///
/// - `client` must be a valid pointer to a MongoClient.
/// - `options` can be null (use defaults) or a valid pointer to SessionOptions.
/// - `error_out` can be null (errors ignored) or a valid pointer to an error pointer.
#[no_mangle]
pub unsafe extern "C" fn mongoc_future_session_start(
    client: *mut MongoClient,
    options: *const SessionOptions,
    error_out: *mut *mut Error,
) -> *mut ClientSession {
    if client.is_null() {
        if !error_out.is_null() {
            *error_out =
                Box::into_raw(Box::new(InvalidArgumentError::new("client is null").into()));
        }
        return std::ptr::null_mut();
    }

    let client_ref = &*client;

    let session_options = match parse_session_options(options) {
        Ok(opts) => opts,
        Err(e) => {
            if !error_out.is_null() {
                *error_out = Box::into_raw(Box::new(e));
            }
            return std::ptr::null_mut();
        }
    };

    let rust_client = client_ref.client.clone();
    let session_result = client_ref.runtime.block_on(async {
        rust_client
            .start_session()
            .with_options(session_options)
            .await
    });

    match session_result {
        Ok(session) => Box::into_raw(Box::new(session)),
        Err(e) => {
            if !error_out.is_null() {
                *error_out = Box::into_raw(Box::new(Error::from(&e)));
            }
            std::ptr::null_mut()
        }
    }
}

/// End a session.
///
/// The session handle becomes invalid after this call.
///
/// # Safety
///
/// - `session` must be a valid pointer to a Session, or null (no-op).
#[no_mangle]
pub unsafe extern "C" fn mongoc_future_session_end(session: *mut ClientSession) {
    if !session.is_null() {
        let _ = Box::from_raw(session);
    }
}

/// Start a transaction on the session.
///
/// Returns a `MongoFuture` that resolves to void.
///
/// # Safety
///
/// - `client` must be a valid pointer to a MongoClient.
/// - `session` must be a valid pointer to a Session.
/// - `options` can be null (use defaults) or a valid pointer to TransactionOptions.
#[no_mangle]
pub unsafe extern "C" fn mongoc_future_session_start_transaction(
    client: *mut MongoClient,
    session: *mut ClientSession,
    options: *const TransactionOptions,
) -> *mut MongoFuture {
    if client.is_null() {
        return MongoFuture::from_error(crate::error::Error::invalid_argument("client is null"));
    }
    if session.is_null() {
        return MongoFuture::from_error(crate::error::Error::invalid_argument("session is null"));
    }

    let tx_options = match parse_transaction_options(options) {
        Ok(opts) => opts,
        Err(_e) => {
            return MongoFuture::from_error(crate::error::Error::invalid_argument(
                "invalid transaction options",
            ));
        }
    };

    let client_ref = &*client;
    let session_ref = &mut *session;

    let handle = client_ref.runtime.spawn(async move {
        session_ref
            .start_transaction()
            .with_options(tx_options)
            .await?;
        Ok(FutureValue::Void)
    });
    MongoFuture::from_join_handle(&client_ref.runtime, handle)
}

/// Commit the current transaction.
///
/// Returns a `MongoFuture` that resolves to void.
///
/// # Safety
///
/// - `client` must be a valid pointer to a MongoClient.
/// - `session` must be a valid pointer to a Session with an active transaction.
#[no_mangle]
pub unsafe extern "C" fn mongoc_future_session_commit_transaction(
    client: *mut MongoClient,
    session: *mut ClientSession,
) -> *mut MongoFuture {
    if client.is_null() {
        return MongoFuture::from_error(crate::error::Error::invalid_argument("client is null"));
    }
    if session.is_null() {
        return MongoFuture::from_error(crate::error::Error::invalid_argument("session is null"));
    }

    let client_ref = &*client;
    let session_ref = &mut *session;

    let handle = client_ref.runtime.spawn(async move {
        session_ref.commit_transaction().await?;
        Ok(FutureValue::Void)
    });
    MongoFuture::from_join_handle(&client_ref.runtime, handle)
}

/// Abort the current transaction.
///
/// Returns a `MongoFuture` that resolves to void.
///
/// # Safety
///
/// - `client` must be a valid pointer to a MongoClient.
/// - `session` must be a valid pointer to a Session with an active transaction.
#[no_mangle]
pub unsafe extern "C" fn mongoc_future_session_abort_transaction(
    client: *mut MongoClient,
    session: *mut ClientSession,
) -> *mut MongoFuture {
    if client.is_null() {
        return MongoFuture::from_error(crate::error::Error::invalid_argument("client is null"));
    }
    if session.is_null() {
        return MongoFuture::from_error(crate::error::Error::invalid_argument("session is null"));
    }

    let client_ref = &*client;
    let session_ref = &mut *session;

    let handle = client_ref.runtime.spawn(async move {
        session_ref.abort_transaction().await?;
        Ok(FutureValue::Void)
    });
    MongoFuture::from_join_handle(&client_ref.runtime, handle)
}
