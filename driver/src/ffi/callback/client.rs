//! FFI client implementation.
//!
//! This module provides the C-compatible API for creating and destroying MongoDB clients.

#[cfg(test)]
mod tests;

use std::{ffi::c_void, sync::Arc};

use tokio::runtime::Runtime;

use crate::Client;

use crate::ffi::{
    error::Error,
    event::{build_command_event_handler, MongoCommandEventHandler},
    ops::client::build_client_options,
    types::{AuthSettings, ConnectionSettings, TlsSettings},
};

use super::runtime::acquire_runtime;

/// Opaque pointer type for MongoClient.
///
/// This wraps the Rust Client along with a reference to the shared global Tokio runtime.
pub struct MongoClient {
    pub(crate) client: Client,
    pub(crate) runtime: Arc<Runtime>,
}

/// Create a new MongoClient. Returns pointer on success, null on error.
///
/// # Safety
///
/// - `connection_settings` must be a valid pointer to a ConnectionSettings struct
/// - `auth_settings` can be null or a valid pointer to an AuthSettings struct
/// - `tls_settings` can be null or a valid pointer to a TlsSettings struct
/// - `command_event_handler` can be null (no monitoring) or a valid pointer to a MongoCommandEventHandler
/// - `error_out` can be null or a valid pointer to store error information
/// - All C string pointers in the settings structs must be valid null-terminated strings
///
/// If the function returns null and `error_out` is not null, `*error_out` will be set to
/// a pointer to an Error that must be freed with `error_free()`.
#[no_mangle]
pub unsafe extern "C" fn mongo_client_new(
    connection_settings: *const ConnectionSettings,
    auth_settings: *const AuthSettings,
    tls_settings: *const TlsSettings,
    command_event_handler: *const MongoCommandEventHandler,
    error_out: *mut *mut Error,
) -> *mut MongoClient {
    let result = build_client_options(connection_settings, auth_settings, tls_settings);

    match result {
        Ok(mut options) => {
            if !command_event_handler.is_null() {
                options.command_event_handler =
                    Some(build_command_event_handler(&*command_event_handler));
            }

            let runtime = acquire_runtime();

            // Client::with_options spawns tasks, so it needs a runtime context
            let _guard = runtime.enter();

            match Client::with_options(options) {
                Ok(client) => {
                    let inner = MongoClient { client, runtime };
                    Box::into_raw(Box::new(inner)) as *mut MongoClient
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

/// Callback invoked when client destruction is complete.
///
/// After this callback fires, no more event callbacks (command, SDAM, CMAP) will be invoked
/// for this client. The caller may safely free event handler function pointers.
pub type DestroyCallback = extern "C" fn(userdata: *mut c_void);

/// Destroy a MongoClient asynchronously.
///
/// This function returns immediately. The client is logically dead after this call — no new
/// operations may be issued. Background cleanup runs asynchronously on the shared Tokio
/// runtime:
///
///   1. `endSessions` is sent for all pooled server sessions
///   2. SDAM topology monitors are shut down
///   3. `callback` is invoked to signal that all cleanup is complete
///
/// After the callback fires, no more event callbacks (command, SDAM, CMAP) will be delivered
/// for this client. It is then safe for the caller to free event handler function pointers.
///
/// # Safety
///
/// - `client` must be a valid pointer returned from `mongo_client_new`
/// - `client` must not be used after this call
/// - This function must only be called once per client
/// - `callback` must be a valid function pointer that remains valid until invoked
#[no_mangle]
pub unsafe extern "C" fn mongo_client_destroy(
    client: *mut MongoClient,
    callback: DestroyCallback,
    userdata: *mut c_void,
) {
    if client.is_null() {
        callback(userdata);
        return;
    }

    let MongoClient {
        client: rust_client,
        runtime,
    } = *Box::from_raw(client);

    // We need the runtime to stay alive until the shutdown task completes and the callback
    // fires. But we can't drop the last Arc<Runtime> from within an async task (tokio panics:
    // "Cannot drop a runtime in a context where blocking is not allowed").
    // Solution: spawn a std::thread that holds the runtime Arc and waits on a plain sync
    // channel for the async task to signal completion, then drops the Arc from a non-async
    // context.
    let (done_tx, done_rx) = std::sync::mpsc::sync_channel::<()>(0);
    let userdata_ptr = userdata as usize;
    runtime.spawn(async move {
        rust_client.shutdown().immediate(true).await;

        let userdata = userdata_ptr as *mut c_void;
        callback(userdata);

        let _ = done_tx.send(());
    });

    std::thread::spawn(move || {
        let _ = done_rx.recv();
        // runtime Arc is dropped here, outside any async context.
        drop(runtime);
    });
}

