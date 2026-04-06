//! FFI session management for the MongoDB Rust driver.
//!
//! Sessions are opaque pointers (`*mut Session`) backed by real `ClientSession` objects.
//! Transaction state is managed entirely within the session.

#[cfg(test)]
mod tests;

use std::ffi::c_void;

use super::client::MongoClient;
use crate::ffi::{
    error::{Error, InvalidArgumentError},
    ops::session::{SessionOptions, TransactionOptions, parse_session_options, parse_transaction_options},
    types::ClientSession,
};

/// Callback type for transaction operations (start, commit, abort).
pub type TransactionCallback = extern "C" fn(userdata: *mut c_void, error: *const Error);

/// Start a new session.
///
/// Returns a session handle on success, or null on error.
///
/// # Safety
///
/// - `client` must be a valid pointer to a MongoClient.
/// - `options` can be null (use defaults) or a valid pointer to SessionOptionsFFI.
/// - `error_out` can be null (errors ignored) or a valid pointer to an error pointer.
#[no_mangle]
pub unsafe extern "C" fn mongo_session_start(
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

    // Parse session options
    let session_options = match parse_session_options(options) {
        Ok(opts) => opts,
        Err(e) => {
            if !error_out.is_null() {
                *error_out = Box::into_raw(Box::new(e));
            }
            return std::ptr::null_mut();
        }
    };

    // Create the session synchronously using the runtime
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
pub unsafe extern "C" fn mongo_session_end(session: *mut ClientSession) {
    if !session.is_null() {
        let _ = Box::from_raw(session);
    }
}

/// Start a transaction on the session.
///
/// # Safety
///
/// - `client` must be a valid pointer to a MongoClient.
/// - `session` must be a valid pointer to a Session.
/// - `options` can be null (use defaults) or a valid pointer to TransactionOptionsFFI.
/// - `callback` will be invoked when the operation completes.
/// - `userdata` is passed to the callback.
#[no_mangle]
pub unsafe extern "C" fn mongo_session_start_transaction(
    client: *mut MongoClient,
    session: *mut ClientSession,
    options: *const TransactionOptions,
    callback: TransactionCallback,
    userdata: *mut c_void,
) {
    if client.is_null() {
        let error = InvalidArgumentError::new("client is null").into();
        callback(userdata, &error);
        return;
    }

    if session.is_null() {
        let error = InvalidArgumentError::new("session is null").into();
        callback(userdata, &error);
        return;
    }

    // Parse transaction options
    let tx_options = match parse_transaction_options(options) {
        Ok(opts) => opts,
        Err(e) => {
            callback(userdata, &e);
            return;
        }
    };

    let client_ref = &*client;
    let session_ref = &mut *session;
    let userdata_ptr = userdata as usize;

    client_ref.runtime.spawn(async move {
        let result = session_ref
            .start_transaction()
            .with_options(tx_options)
            .await;
        let userdata = userdata_ptr as *mut c_void;

        match result {
            Ok(()) => callback(userdata, std::ptr::null()),
            Err(e) => {
                let error = Error::from(&e);
                callback(userdata, &error);
            }
        }
    });
}

/// Commit the current transaction.
///
/// # Safety
///
/// - `client` must be a valid pointer to a MongoClient.
/// - `session` must be a valid pointer to a Session with an active transaction.
/// - `callback` will be invoked when the operation completes.
/// - `userdata` is passed to the callback.
#[no_mangle]
pub unsafe extern "C" fn mongo_session_commit_transaction(
    client: *mut MongoClient,
    session: *mut ClientSession,
    callback: TransactionCallback,
    userdata: *mut c_void,
) {
    if client.is_null() {
        let error = InvalidArgumentError::new("client is null").into();
        callback(userdata, &error);
        return;
    }

    if session.is_null() {
        let error = InvalidArgumentError::new("session is null").into();
        callback(userdata, &error);
        return;
    }

    let client_ref = &*client;
    let session_ref = &mut *session;
    let userdata_ptr = userdata as usize;

    client_ref.runtime.spawn(async move {
        let result = session_ref.commit_transaction().await;
        let userdata = userdata_ptr as *mut c_void;

        match result {
            Ok(()) => callback(userdata, std::ptr::null()),
            Err(e) => {
                let error = Error::from(&e);
                callback(userdata, &error);
            }
        }
    });
}

/// Abort the current transaction.
///
/// # Safety
///
/// - `client` must be a valid pointer to a MongoClient.
/// - `session` must be a valid pointer to a Session with an active transaction.
/// - `callback` will be invoked when the operation completes.
/// - `userdata` is passed to the callback.
#[no_mangle]
pub unsafe extern "C" fn mongo_session_abort_transaction(
    client: *mut MongoClient,
    session: *mut ClientSession,
    callback: TransactionCallback,
    userdata: *mut c_void,
) {
    if client.is_null() {
        let error = InvalidArgumentError::new("client is null").into();
        callback(userdata, &error);
        return;
    }

    if session.is_null() {
        let error = InvalidArgumentError::new("session is null").into();
        callback(userdata, &error);
        return;
    }

    let client_ref = &*client;
    let session_ref = &mut *session;
    let userdata_ptr = userdata as usize;

    client_ref.runtime.spawn(async move {
        let result = session_ref.abort_transaction().await;
        let userdata = userdata_ptr as *mut c_void;

        match result {
            Ok(()) => callback(userdata, std::ptr::null()),
            Err(e) => {
                let error = Error::from(&e);
                callback(userdata, &error);
            }
        }
    });
}
