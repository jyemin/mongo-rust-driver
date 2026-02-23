//! FFI (Foreign Function Interface) support for language bindings.
//!
//! This module provides the canonical C API for binding the MongoDB Rust driver
//! to other languages (Java, Node.js, Python, etc.) via FFI.
//!
//! ## Components
//!
//! - [`types`]: Core FFI types (BsonBytes, MongoClient, callbacks)
//! - [`settings`]: Client configuration types (ConnectionSettings, AuthSettings, etc.)
//! - [`session`]: FFI-level session management (lsid, txnNumber tracking)
//! - [`cursor`]: Cursor management for iterating query results
//! - [`events`]: Command event types and handlers
//! - [`logging`]: Logging integration (tracing to callbacks) - requires `tracing-unstable` feature
//! - [`core`]: Shared business logic for command preparation and error handling
//! - [`ops`]: Shared async operations for command execution
//! - [`api`]: C ABI entry points (extern "C" functions)

pub mod api;
pub mod core;
pub mod cursor;
pub mod events;
#[cfg(feature = "tracing-unstable")]
pub mod logging;
pub mod ops;
pub mod session;
pub mod settings;
pub mod types;

// Re-export commonly used types for convenience
pub use core::{
    extract_comment, prepare_command_with_session, prepare_cursor_command_with_session,
    serialize_mongodb_error, to_retryability, OperationParams, ERROR_CATEGORY_AUTHENTICATION,
    ERROR_CATEGORY_COMMAND, ERROR_CATEGORY_CONNECTION, ERROR_CATEGORY_INTERNAL,
    ERROR_CATEGORY_SERVER_SELECTION,
};
pub use cursor::CursorManager;
pub use events::{
    create_command_event_handler, CommandEventCallback, CommandEventType, FfiCommandEvent,
};
#[cfg(feature = "tracing-unstable")]
pub use logging::{
    init_logging, init_logging_with_jni_callback, update_log_levels, FfiLogEvent, JniLogCallback,
    LogCallback,
};
pub use ops::{
    execute_command, execute_command_raw, execute_cursor_command, execute_get_more,
    CursorCommandResult, GetMoreResult,
};
pub use session::FfiSessionPool;
pub use settings::{AuthSettings, ConnectionSettings, TlsSettings};
pub use types::{
    BsonBytes, CursorResultCallback, GetMoreResultCallback, MongoClient, OperationContext,
    SingleResultCallback,
};
