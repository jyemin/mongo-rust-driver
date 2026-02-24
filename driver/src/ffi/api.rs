// FFI functions exposed to Java via JNA/FFM

use super::events::{create_command_event_handler, CommandEventCallback};
#[cfg(feature = "tracing-unstable")]
use super::logging::{init_logging, update_log_levels, LogCallback};
use super::settings::*;
use super::types::*;
use crate::options::{ClientOptions, Credential, ServerAddress};
use std::ffi::CStr;
use std::os::raw::c_char;

/// Wrapper to make *mut c_void Send+Sync for use across async boundaries
/// Safety: The caller must ensure the userdata pointer remains valid
/// for the lifetime of the async operation.
#[derive(Clone, Copy)]
struct SendSyncUserdata(*mut std::ffi::c_void);
unsafe impl Send for SendSyncUserdata {}
unsafe impl Sync for SendSyncUserdata {}

impl SendSyncUserdata {
    /// Get the raw pointer - use this to avoid capturing the raw pointer in async blocks
    fn ptr(&self) -> *mut std::ffi::c_void {
        self.0
    }
}

// ============================================================================
// Session Management Functions (for FFM)
// ============================================================================

/// Acquire a session from the client's session pool.
/// Returns a session handle (non-zero), or 0 on error.
#[no_mangle]
pub extern "C" fn mongo_session_acquire(client: *mut MongoClient) -> u64 {
    if client.is_null() {
        return 0;
    }
    let mongo_client = unsafe { &*client };
    mongo_client.session_pool.acquire()
}

/// Release a session back to the client's session pool.
#[no_mangle]
pub extern "C" fn mongo_session_release(client: *mut MongoClient, session_handle: u64) {
    if client.is_null() || session_handle == 0 {
        return;
    }
    let mongo_client = unsafe { &*client };
    mongo_client.session_pool.release(session_handle);
}

/// Get the transaction number for a session.
#[no_mangle]
pub extern "C" fn mongo_session_get_txn_number(
    client: *mut MongoClient,
    session_handle: u64,
) -> i64 {
    if client.is_null() || session_handle == 0 {
        return 0;
    }
    let mongo_client = unsafe { &*client };
    mongo_client.session_pool.get_txn_number(session_handle) as i64
}

/// Advance the transaction number and return the new value.
#[no_mangle]
pub extern "C" fn mongo_session_advance_txn_number(
    client: *mut MongoClient,
    session_handle: u64,
) -> i64 {
    if client.is_null() || session_handle == 0 {
        return 0;
    }
    let mongo_client = unsafe { &*client };
    mongo_client.session_pool.advance_txn_number(session_handle) as i64
}

/// Mark a session as dirty (should not be returned to pool).
#[no_mangle]
pub extern "C" fn mongo_session_mark_dirty(client: *mut MongoClient, session_handle: u64) {
    if client.is_null() || session_handle == 0 {
        return;
    }
    let mongo_client = unsafe { &*client };
    mongo_client.session_pool.mark_dirty(session_handle);
}

/// Get the session lsid as BSON bytes.
/// Caller must call mongo_free_bytes on the returned data when done.
/// Returns BsonBytes with null data if session not found.
#[no_mangle]
pub extern "C" fn mongo_session_get_lsid(
    client: *mut MongoClient,
    session_handle: u64,
) -> BsonBytes {
    if client.is_null() || session_handle == 0 {
        return BsonBytes {
            data: std::ptr::null(),
            len: 0,
        };
    }
    let mongo_client = unsafe { &*client };

    if let Some(lsid_doc) = mongo_client.session_pool.get_session_lsid(session_handle) {
        // Serialize the lsid document to BSON bytes
        let mut bytes = Vec::new();
        if lsid_doc.to_writer(&mut bytes).is_ok() {
            let len = bytes.len();
            let data = bytes.as_ptr();
            std::mem::forget(bytes); // Prevent deallocation - caller must free
            return BsonBytes { data, len };
        }
    }
    BsonBytes {
        data: std::ptr::null(),
        len: 0,
    }
}

/// Free bytes allocated by Rust (e.g., from mongo_session_get_lsid).
#[no_mangle]
pub extern "C" fn mongo_free_bytes(data: *mut u8, len: usize) {
    if !data.is_null() && len > 0 {
        unsafe {
            let _ = Vec::from_raw_parts(data, len, len);
            // Vec is dropped here, freeing the memory
        }
    }
}

/// Initialize logging globally with a callback and per-component log levels.
/// This should be called once at first client creation.
///
/// Log levels: 0 = DEBUG, 1 = INFO, 2 = WARN (effectively disabled)
///
/// NOTE: userdata is passed through to the callback for closure context
/// NOTE: callback MUST be the last parameter for JNA compatibility
#[cfg(feature = "tracing-unstable")]
#[no_mangle]
pub extern "C" fn mongo_init_logging(
    command_level: i32,
    connection_level: i32,
    server_selection_level: i32,
    topology_level: i32,
    userdata: *mut std::ffi::c_void,
    callback: LogCallback,
) {
    init_logging(
        userdata,
        callback,
        command_level,
        connection_level,
        server_selection_level,
        topology_level,
    );
}

/// Update log levels at runtime.
/// Can be called periodically to refresh levels from SLF4J.
///
/// Log levels: 0 = DEBUG, 1 = INFO, 2 = WARN (effectively disabled)
///
/// NOTE: Due to tracing limitations, the filter is fixed at init time.
/// This function is provided for API completeness.
#[cfg(feature = "tracing-unstable")]
#[no_mangle]
pub extern "C" fn mongo_update_log_levels(
    command_level: i32,
    connection_level: i32,
    server_selection_level: i32,
    topology_level: i32,
) {
    update_log_levels(
        command_level,
        connection_level,
        server_selection_level,
        topology_level,
    );
}

/// Create a new MongoDB client with settings structs
/// NOTE: All settings are passed by reference (pointer) for JNA compatibility
/// NOTE: Callbacks must be the LAST parameters for JNA compatibility
/// NOTE: Logging is now initialized globally via mongo_init_logging()
/// NOTE: max_document_length controls log truncation per-client
/// NOTE: command_event_callback can be NULL if no event monitoring is needed
/// NOTE: command_event_userdata is passed through to command_event_callback (for closure context)
#[no_mangle]
pub extern "C" fn mongo_client_new(
    connection_settings: *const ConnectionSettings,
    auth_settings: *const AuthSettings,
    tls_settings: *const TlsSettings,
    max_document_length: i32,
    command_event_userdata: *mut std::ffi::c_void,
    command_event_callback: CommandEventCallback,
) -> *mut MongoClient {
    if connection_settings.is_null() {
        return std::ptr::null_mut();
    }

    // Create a Tokio runtime for async operations
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(_) => return std::ptr::null_mut(),
    };

    // Build ClientOptions from settings
    let client_options = unsafe {
        match build_client_options(
            connection_settings,
            auth_settings,
            tls_settings,
            max_document_length,
            command_event_userdata,
            command_event_callback,
        ) {
            Ok(opts) => opts,
            Err(_) => return std::ptr::null_mut(),
        }
    };

    // Create client within the runtime context
    let client = runtime.block_on(async { crate::Client::with_options(client_options) });

    let client = match client {
        Ok(c) => c,
        Err(_) => return std::ptr::null_mut(),
    };

    let mongo_client = Box::new(MongoClient {
        client,
        runtime,
        session_pool: super::session::FfiSessionPool::new(),
        cursor_manager: std::sync::Arc::new(super::cursor::CursorManager::new()),
    });
    Box::into_raw(mongo_client)
}

/// Build ClientOptions from settings structs
/// Note: command_event_callback is CommandEventCallback (which is Option<fn>) - None means no callback
/// Note: command_event_userdata is passed through to the callback for closure context
unsafe fn build_client_options(
    connection_settings: *const ConnectionSettings,
    auth_settings: *const AuthSettings,
    tls_settings: *const TlsSettings,
    max_document_length: i32,
    command_event_userdata: *mut std::ffi::c_void,
    command_event_callback: CommandEventCallback,
) -> Result<ClientOptions, Box<dyn std::error::Error>> {
    let conn_settings = &*connection_settings;

    // Start with default options
    let mut options = ClientOptions::default();

    // Wire up command event handler if callback was provided (CommandEventCallback is Option<fn>)
    if let Some(callback) = command_event_callback {
        options.command_event_handler =
            Some(create_command_event_handler(command_event_userdata, callback));
    }

    // Apply connection settings
    let hosts = conn_settings.get_hosts();
    options.hosts = hosts
        .iter()
        .filter_map(|h| h.parse::<ServerAddress>().ok())
        .collect();

    if let Some(direct) = ConnectionSettings::get_optional_bool(conn_settings.direct_connection) {
        options.direct_connection = Some(direct);
    }

    if let Some(repl_set) = ConnectionSettings::get_optional_string(conn_settings.repl_set_name) {
        options.repl_set_name = Some(repl_set);
    }

    if let Some(srv_service) =
        ConnectionSettings::get_optional_string(conn_settings.srv_service_name)
    {
        options.srv_service_name = Some(srv_service);
    }

    if conn_settings.srv_max_hosts > 0 {
        options.srv_max_hosts = Some(conn_settings.srv_max_hosts);
    }

    if let Some(timeout) =
        ConnectionSettings::get_optional_duration_ms(conn_settings.server_selection_timeout_ms)
    {
        options.server_selection_timeout = Some(timeout);
    }

    if let Some(threshold) =
        ConnectionSettings::get_optional_duration_ms(conn_settings.local_threshold_ms)
    {
        options.local_threshold = Some(threshold);
    }

    if let Some(load_balanced) = ConnectionSettings::get_optional_bool(conn_settings.load_balanced)
    {
        options.load_balanced = Some(load_balanced);
    }

    // Apply max document length for tracing/logging truncation
    #[cfg(feature = "tracing-unstable")]
    if max_document_length > 0 {
        options.tracing_max_document_length_bytes = Some(max_document_length as usize);
    }
    #[cfg(not(feature = "tracing-unstable"))]
    let _ = max_document_length; // Silence unused warning

    // Apply authentication settings
    if !auth_settings.is_null() {
        let auth = &*auth_settings;
        if auth.is_configured() {
            // For now, just create a basic credential with username/password
            // The typed builder pattern makes conditional building difficult
            // TODO: Improve this to handle all credential fields properly
            if let (Some(username), Some(password)) = (
                ConnectionSettings::get_optional_string(auth.username),
                ConnectionSettings::get_optional_string(auth.password),
            ) {
                let credential = Credential::builder()
                    .username(username)
                    .password(password)
                    .build();
                options.credential = Some(credential);
            }
        }
    }

    // Apply TLS settings
    if !tls_settings.is_null() {
        let tls = &*tls_settings;
        if tls.is_enabled() {
            use crate::options::Tls;

            let mut tls_opts = Tls::Enabled(Default::default());

            if let Tls::Enabled(ref mut opts) = tls_opts {
                if let Some(allow_invalid_certs) =
                    ConnectionSettings::get_optional_bool(tls.allow_invalid_certificates)
                {
                    opts.allow_invalid_certificates = Some(allow_invalid_certs);
                }

                // Note: allow_invalid_hostnames is not available in TlsOptions in this version
                // of the Rust driver. It may be added in a future version.

                if let Some(ca_file) = ConnectionSettings::get_optional_string(tls.ca_file_path) {
                    opts.ca_file_path = Some(ca_file.into());
                }

                if let Some(cert_key_file) =
                    ConnectionSettings::get_optional_string(tls.cert_key_file_path)
                {
                    opts.cert_key_file_path = Some(cert_key_file.into());
                }
            }

            options.tls = Some(tls_opts);
        }
    }

    Ok(options)
}

/// Destroy a MongoDB client
#[no_mangle]
pub extern "C" fn mongo_client_destroy(client: *mut MongoClient) {
    if !client.is_null() {
        unsafe {
            let mongo_client = Box::from_raw(client);
            // Shutdown the runtime gracefully, waiting up to 5 seconds for tasks to complete
            mongo_client
                .runtime
                .shutdown_timeout(std::time::Duration::from_secs(5));
            // mongo_client.client is dropped here
        }
    }
}

// ============================================================================
// Execute Command with Session Handle
// ============================================================================

use super::core::{self, OperationParams};
use super::ops;

/// Execute a command using session handle (matches JNI executeCommandAsync).
/// Rust looks up the session by handle and manages lsid/txnNumber internally.
///
/// Parameters:
/// - client: The MongoClient pointer
/// - database: Database name (null-terminated C string)
/// - command: BSON command bytes
/// - context: Operation context with session, transaction, and retryability info
/// - userdata: Opaque pointer passed through to callback (for closure context)
/// - callback: Callback to invoke with result
#[no_mangle]
pub extern "C" fn mongo_execute_command(
    client: *mut MongoClient,
    database: *const c_char,
    command: *const BsonBytes,
    context: *const OperationContext,
    userdata: *mut std::ffi::c_void,
    callback: SingleResultCallback,
) {
    if client.is_null() || database.is_null() || command.is_null() {
        let error_msg = b"Invalid parameters";
        let error_bytes = BsonBytes {
            data: error_msg.as_ptr(),
            len: error_msg.len(),
        };
        callback(userdata, false, &error_bytes);
        return;
    }

    let mongo_client = unsafe { &*client };

    // Parse database name
    let db_name = unsafe { CStr::from_ptr(database).to_string_lossy().into_owned() };

    // Get raw BSON command bytes
    let command_bytes = unsafe {
        let bson_bytes = &*command;
        if bson_bytes.data.is_null() || bson_bytes.len == 0 {
            let error_msg = b"Empty command";
            let error_bytes = BsonBytes {
                data: error_msg.as_ptr(),
                len: error_msg.len(),
            };
            callback(userdata, false, &error_bytes);
            return;
        }
        std::slice::from_raw_parts(bson_bytes.data, bson_bytes.len).to_vec()
    };

    // Build operation params from context
    let params = context_to_params(context);

    // Prepare command with session/txn fields using shared core logic
    let (command_raw, has_session) = match core::prepare_command_with_session(
        command_bytes,
        &params,
        &mongo_client.session_pool,
    ) {
        Ok(result) => result,
        Err(e) => {
            let error_bytes = e.as_bytes();
            let bson_bytes = BsonBytes {
                data: error_bytes.as_ptr(),
                len: error_bytes.len(),
            };
            callback(userdata, false, &bson_bytes);
            return;
        }
    };

    let retry = core::to_retryability(params.retryability);
    let client_clone = mongo_client.client.clone();

    // Wrap userdata for Send+Sync across async boundary
    let ud = SendSyncUserdata(userdata);

    // Spawn async task using shared ops function
    mongo_client.runtime.spawn(async move {
        let result =
            ops::execute_command(&client_clone, &db_name, command_raw, retry, has_session).await;
        match result {
            Ok(bytes) => {
                let bson_bytes = BsonBytes {
                    data: bytes.as_ptr(),
                    len: bytes.len(),
                };
                callback(ud.ptr(), true, &bson_bytes);
            }
            Err(error_bytes) => {
                let bson_bytes = BsonBytes {
                    data: error_bytes.as_ptr(),
                    len: error_bytes.len(),
                };
                callback(ud.ptr(), false, &bson_bytes);
            }
        }
    });
}

/// Convert OperationContext pointer to OperationParams.
fn context_to_params(context: *const OperationContext) -> OperationParams {
    if context.is_null() {
        return OperationParams::default();
    }

    let ctx = unsafe { &*context };
    let read_concern_level = if ctx.read_concern_level.is_null() {
        None
    } else {
        Some(unsafe {
            CStr::from_ptr(ctx.read_concern_level)
                .to_string_lossy()
                .into_owned()
        })
    };

    OperationParams {
        retryability: ctx.retryability,
        session_handle: ctx.session_handle,
        in_transaction: ctx.in_transaction,
        start_transaction: ctx.start_transaction,
        has_after_cluster_time: ctx.has_after_cluster_time,
        after_cluster_time_seconds: ctx.after_cluster_time_seconds,
        after_cluster_time_increment: ctx.after_cluster_time_increment,
        read_concern_level,
    }
}

// ============================================================================
// Cursor Operations (for FFM)
// ============================================================================

/// Execute a cursor-returning command (find, aggregate, listCollections, etc.)
/// Returns cursor handle, exhausted flag, and first batch via callback.
///
/// Parameters:
/// - client: The MongoClient pointer
/// - database: Database name (null-terminated C string)
/// - command: BSON command bytes
/// - batch_size: Batch size for getMore operations (0 = use server default)
/// - context: Operation context with session, transaction, and retryability info
/// - userdata: Opaque pointer passed through to callback (for closure context)
/// - callback: Callback to invoke with result
#[no_mangle]
pub extern "C" fn mongo_execute_cursor_command(
    client: *mut MongoClient,
    database: *const c_char,
    command: *const BsonBytes,
    batch_size: i32,
    context: *const OperationContext,
    userdata: *mut std::ffi::c_void,
    callback: CursorResultCallback,
) {
    if client.is_null() || database.is_null() || command.is_null() {
        let error_msg = b"Invalid parameters";
        let error_bytes = BsonBytes {
            data: error_msg.as_ptr(),
            len: error_msg.len(),
        };
        callback(userdata, false, 0, false, &error_bytes);
        return;
    }

    let mongo_client = unsafe { &*client };

    // Parse database name
    let db_name = unsafe { CStr::from_ptr(database).to_string_lossy().into_owned() };

    // Get raw BSON command bytes
    let command_bytes = unsafe {
        let bson_bytes = &*command;
        if bson_bytes.data.is_null() || bson_bytes.len == 0 {
            let error_msg = b"Empty command";
            let error_bytes = BsonBytes {
                data: error_msg.as_ptr(),
                len: error_msg.len(),
            };
            callback(userdata, false, 0, false, &error_bytes);
            return;
        }
        std::slice::from_raw_parts(bson_bytes.data, bson_bytes.len).to_vec()
    };

    // Build operation params from context
    let params = context_to_params(context);

    // Extract comment from command before preparing (for getMore operations)
    let comment = core::extract_comment(&command_bytes);

    // Prepare cursor command with session/txn fields using shared core logic
    let (command_raw, has_session, external_session_info) =
        match core::prepare_cursor_command_with_session(
            command_bytes,
            &params,
            &mongo_client.session_pool,
        ) {
            Ok(result) => result,
            Err(e) => {
                let error_bytes = e.as_bytes();
                let bson_bytes = BsonBytes {
                    data: error_bytes.as_ptr(),
                    len: error_bytes.len(),
                };
                callback(userdata, false, 0, false, &bson_bytes);
                return;
            }
        };

    let retry = core::to_retryability(params.retryability);
    let batch_size_opt: Option<u32> = if batch_size > 0 {
        Some(batch_size as u32)
    } else {
        None
    };
    let client_clone = mongo_client.client.clone();
    let cursor_manager = mongo_client.cursor_manager.clone();

    // Wrap userdata for Send+Sync across async boundary
    let ud = SendSyncUserdata(userdata);

    // Spawn async task using shared ops function
    mongo_client.runtime.spawn(async move {
        let result = ops::execute_cursor_command(
            &client_clone,
            &db_name,
            command_raw,
            retry,
            has_session,
            batch_size_opt,
            comment,
            external_session_info,
            &cursor_manager,
        )
        .await;

        match result {
            Ok(r) => {
                let bson_bytes = BsonBytes {
                    data: r.first_batch_bytes.as_ptr(),
                    len: r.first_batch_bytes.len(),
                };
                callback(ud.ptr(), true, r.cursor_handle, r.exhausted, &bson_bytes);
            }
            Err(error_bytes) => {
                let bson_bytes = BsonBytes {
                    data: error_bytes.as_ptr(),
                    len: error_bytes.len(),
                };
                callback(ud.ptr(), false, 0, false, &bson_bytes);
            }
        }
    });
}

/// Execute getMore on a cursor (get next batch).
/// Returns exhausted flag and next batch via callback.
///
/// Parameters:
/// - client: The MongoClient pointer
/// - cursor_handle: Handle to the cursor
/// - userdata: Opaque pointer passed through to callback (for closure context)
/// - callback: Callback to invoke with result
#[no_mangle]
pub extern "C" fn mongo_cursor_get_more(
    client: *mut MongoClient,
    cursor_handle: u64,
    userdata: *mut std::ffi::c_void,
    callback: GetMoreResultCallback,
) {
    if client.is_null() {
        let error_msg = b"Client is null";
        let error_bytes = BsonBytes {
            data: error_msg.as_ptr(),
            len: error_msg.len(),
        };
        callback(userdata, false, false, &error_bytes);
        return;
    }

    let mongo_client = unsafe { &*client };
    let cursor_manager = mongo_client.cursor_manager.clone();

    // Wrap userdata for Send+Sync across async boundary
    let ud = SendSyncUserdata(userdata);

    // Spawn async task using shared ops function
    mongo_client.runtime.spawn(async move {
        let result = ops::execute_get_more(&cursor_manager, cursor_handle).await;

        match result {
            Ok(r) => {
                let bson_bytes = BsonBytes {
                    data: r.batch_bytes.as_ptr(),
                    len: r.batch_bytes.len(),
                };
                callback(ud.ptr(), true, r.exhausted, &bson_bytes);
            }
            Err((error_bytes, _cursor_valid)) => {
                let bson_bytes = BsonBytes {
                    data: error_bytes.as_ptr(),
                    len: error_bytes.len(),
                };
                callback(ud.ptr(), false, false, &bson_bytes);
            }
        }
    });
}

/// Close a cursor and clean up resources.
/// The RawBatchCursor's Drop impl handles killCursors automatically.
///
/// Parameters:
/// - client: The MongoClient pointer
/// - cursor_handle: Handle to the cursor
/// - userdata: Opaque pointer passed through to callback (for closure context)
/// - callback: Callback to invoke on completion
#[no_mangle]
pub extern "C" fn mongo_cursor_close(
    client: *mut MongoClient,
    cursor_handle: u64,
    userdata: *mut std::ffi::c_void,
    callback: SingleResultCallback,
) {
    if client.is_null() {
        let error_msg = b"Client is null";
        let error_bytes = BsonBytes {
            data: error_msg.as_ptr(),
            len: error_msg.len(),
        };
        callback(userdata, false, &error_bytes);
        return;
    }

    let mongo_client = unsafe { &*client };

    // Remove cursor from manager - Drop will send killCursors if needed
    let _cursor = mongo_client.cursor_manager.remove(cursor_handle);

    // The cursor is dropped here, which triggers killCursors if not exhausted
    // This happens synchronously via the RawBatchCursor Drop impl

    // Invoke success callback
    let empty = BsonBytes {
        data: std::ptr::null(),
        len: 0,
    };
    callback(userdata, true, &empty);
}
