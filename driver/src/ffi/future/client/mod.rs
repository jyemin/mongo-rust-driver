//! Future-based FFI client.
//!
//! Each `MongoClient` owns a per-client `current_thread` tokio runtime. The
//! caller drives the runtime by calling `mongoc_future_client_tick()`. Async
//! operations return `MongoFuture` handles that resolve as the runtime is
//! ticked.

#[cfg(test)]
mod tests;

use crate::{
    ffi::{
        error::Error,
        event::{build_command_event_handler, MongoCommandEventHandler},
        ops::client::build_client_options,
        types::{AuthSettings, ConnectionSettings, TlsSettings},
    },
    Client,
};

use super::future::{FutureValue, MongoFuture};

/// Opaque client type for the future-based FFI surface.
///
/// Owns both the driver `Client` and a `current_thread` tokio runtime.
pub struct MongoClient {
    pub(super) client: Client,
    pub(super) runtime: tokio::runtime::Runtime,
}

/// Create a new future-based MongoClient.
///
/// Returns a pointer to the client on success, null on error. If the function
/// returns null and `error_out` is non-null, `*error_out` is set to a
/// heap-allocated `Error` that must be freed with `error_free()`.
///
/// # Safety
///
/// - `connection_settings` must be a valid pointer to a ConnectionSettings struct
/// - `auth_settings` can be null or a valid pointer to an AuthSettings struct
/// - `tls_settings` can be null or a valid pointer to a TlsSettings struct
/// - `command_event_handler` can be null (no monitoring) or a valid pointer
/// - `error_out` can be null or a valid pointer to store error information
/// - All C string pointers in the settings structs must be valid null-terminated strings
#[no_mangle]
pub unsafe extern "C" fn mongoc_future_client_new(
    connection_settings: *const ConnectionSettings,
    auth_settings: *const AuthSettings,
    tls_settings: *const TlsSettings,
    command_event_handler: *const MongoCommandEventHandler,
    error_out: *mut *mut Error,
) -> *mut MongoClient {
    // Build the current_thread runtime
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            if !error_out.is_null() {
                let err = crate::error::Error::invalid_argument(format!(
                    "failed to create tokio runtime: {}",
                    e
                ));
                *error_out = Box::into_raw(Box::new(Error::from(&err)));
            }
            return std::ptr::null_mut();
        }
    };

    let result = build_client_options(connection_settings, auth_settings, tls_settings);

    match result {
        Ok(mut options) => {
            if !command_event_handler.is_null() {
                options.command_event_handler =
                    Some(build_command_event_handler(&*command_event_handler));
            }

            // Client::with_options spawns tasks, so it needs a runtime context
            let _guard = runtime.enter();

            match Client::with_options(options) {
                Ok(client) => {
                    let inner = MongoClient { client, runtime };
                    Box::into_raw(Box::new(inner))
                }
                Err(e) => {
                    if !error_out.is_null() {
                        *error_out = Box::into_raw(Box::new(Error::from(&e)));
                    }
                    std::ptr::null_mut()
                }
            }
        }
        Err(e) => {
            if !error_out.is_null() {
                *error_out = Box::into_raw(Box::new(Error::from(&e)));
            }
            std::ptr::null_mut()
        }
    }
}

/// Tick the client's tokio runtime.
///
/// This drives all pending async work forward by one turn of the event loop.
/// The caller should call this in a loop, checking futures for completion
/// between ticks.
///
/// # Safety
///
/// `client` must be a valid pointer returned by `mongoc_future_client_new`.
#[no_mangle]
pub unsafe extern "C" fn mongoc_future_client_tick(client: *mut MongoClient) {
    if client.is_null() {
        return;
    }
    let client = &*client;
    client
        .runtime
        .block_on(async { tokio::task::yield_now().await });
}

/// Begin asynchronous shutdown of the client.
///
/// Returns a `MongoFuture` that resolves to void when shutdown is complete.
/// The client must remain alive (for ticking) until the returned future
/// resolves. After the future completes, call `mongoc_future_client_free()`
/// to release the client.
///
/// # Safety
///
/// - `client` must be a valid pointer returned by `mongoc_future_client_new`.
/// - No new operations may be issued after this call.
/// - The caller must continue to call `mongoc_future_client_tick()` until
///   the returned future is finished.
#[no_mangle]
pub unsafe extern "C" fn mongoc_future_client_destroy(
    client: *mut MongoClient,
) -> *mut MongoFuture {
    if client.is_null() {
        return MongoFuture::from_value(FutureValue::Void);
    }

    let client_ref = &*client;
    let client_clone = client_ref.client.clone();
    let handle = client_ref.runtime.spawn(async move {
        client_clone.shutdown().immediate(true).await;
        Ok(FutureValue::Void)
    });
    MongoFuture::from_join_handle(&client_ref.runtime, handle)
}

/// Free a client after shutdown is complete.
///
/// This releases the client and its runtime. Only call this after the
/// future returned by `mongoc_future_client_destroy()` has resolved.
///
/// # Safety
///
/// - `client` must be a valid pointer returned by `mongoc_future_client_new`,
///   or null.
/// - The shutdown future must have already completed.
/// - The client must not be used after this call.
#[no_mangle]
pub unsafe extern "C" fn mongoc_future_client_free(client: *mut MongoClient) {
    if !client.is_null() {
        drop(Box::from_raw(client));
    }
}
