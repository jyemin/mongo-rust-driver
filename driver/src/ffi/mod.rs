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

pub mod types;
pub mod settings;
pub mod session;
pub mod cursor;
pub mod events;
#[cfg(feature = "tracing-unstable")]
pub mod logging;
pub mod core;
pub mod ops;
pub mod api;

// Re-export commonly used types for convenience
pub use types::{BsonBytes, OperationContext, MongoClient, SingleResultCallback, CursorResultCallback, GetMoreResultCallback};
pub use settings::{ConnectionSettings, AuthSettings, TlsSettings};
pub use session::FfiSessionPool;
pub use cursor::CursorManager;
pub use events::{CommandEventType, CommandEventCallback, FfiCommandEvent, create_command_event_handler};
#[cfg(feature = "tracing-unstable")]
pub use logging::{LogCallback, JniLogCallback, FfiLogEvent, init_logging, init_logging_with_jni_callback, update_log_levels};
pub use core::{
    OperationParams,
    ERROR_CATEGORY_COMMAND,
    ERROR_CATEGORY_CONNECTION,
    ERROR_CATEGORY_SERVER_SELECTION,
    ERROR_CATEGORY_AUTHENTICATION,
    ERROR_CATEGORY_INTERNAL,
    to_retryability,
    serialize_mongodb_error,
    prepare_command_with_session,
    prepare_cursor_command_with_session,
    extract_comment,
};
pub use ops::{execute_command, execute_command_raw, execute_cursor_command, execute_get_more, CursorCommandResult, GetMoreResult};
