// Logging integration - bridges Rust tracing to Java SLF4J via callbacks
//
// Log events are filtered based on Java's SLF4J configuration (per-component levels).
// The filter is configured once globally via init_logging() on first client creation.
// Levels can be updated at runtime via update_log_levels().

use std::ffi::CString;
use std::os::raw::c_char;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Once, RwLock};

use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;

/// Log level constants matching Java's LogMessage.Level
pub const LOG_LEVEL_DEBUG: i32 = 0;
pub const LOG_LEVEL_INFO: i32 = 1;

/// A single log field (name/value pair)
#[repr(C)]
pub struct FfiLogField {
    pub name: *const c_char,
    pub value: *const c_char,
}

/// Log event structure passed to Java callback
#[repr(C)]
pub struct FfiLogEvent {
    /// Log level: 0=DEBUG, 1=INFO
    pub level: i32,
    /// Tracing target: "crate::command", "crate::connection", etc.
    pub target: *const c_char,
    /// Message identifier: "Command started", "Connection pool created", etc.
    pub message: *const c_char,
    /// Number of fields
    pub field_count: i32,
    /// Array of fields
    pub fields: *const FfiLogField,
}

/// Callback type for log events (C FFI / JNA)
/// Java implements this and passes to Rust via init_logging()
/// The userdata parameter is passed through from init_logging for closure context
pub type LogCallback = extern "C" fn(userdata: *mut std::ffi::c_void, event: *const FfiLogEvent);

/// Callback type for log events (JNI)
/// Function pointer that takes Rust strings and invokes Java via JNI
pub type JniLogCallback =
    fn(level: i32, target: &str, message: &str, field_names: &[&str], field_values: &[&str]);

/// Wrapper to make *mut c_void Send+Sync for use across threads
/// Safety: The caller must ensure the userdata pointer remains valid
/// for the lifetime of the logging system.
#[derive(Clone, Copy)]
struct SendSyncUserdata(*mut std::ffi::c_void);
unsafe impl Send for SendSyncUserdata {}
unsafe impl Sync for SendSyncUserdata {}

/// Global C FFI log callback (set once via init_logging)
static GLOBAL_LOG_CALLBACK: RwLock<Option<LogCallback>> = RwLock::new(None);

/// Global C FFI log callback userdata (set once via init_logging)
static GLOBAL_LOG_USERDATA: RwLock<Option<SendSyncUserdata>> = RwLock::new(None);

/// Global JNI log callback (set once via init_logging_with_jni_callback)
static GLOBAL_JNI_LOG_CALLBACK: RwLock<Option<JniLogCallback>> = RwLock::new(None);

/// Fast atomic flag to avoid RwLock reads in the hot path.
/// Set to true when ANY callback is registered.
static HAS_ANY_CALLBACK: AtomicBool = AtomicBool::new(false);

/// Set the global C FFI log callback (called from init_logging FFI)
pub fn set_global_log_callback(userdata: *mut std::ffi::c_void, callback: LogCallback) {
    if let Ok(mut cb) = GLOBAL_LOG_CALLBACK.write() {
        *cb = Some(callback);
    }
    if let Ok(mut ud) = GLOBAL_LOG_USERDATA.write() {
        *ud = Some(SendSyncUserdata(userdata));
    }
    HAS_ANY_CALLBACK.store(true, Ordering::Release);
}

/// Set the global JNI log callback
fn set_global_jni_log_callback(callback: JniLogCallback) {
    if let Ok(mut cb) = GLOBAL_JNI_LOG_CALLBACK.write() {
        *cb = Some(callback);
        HAS_ANY_CALLBACK.store(true, Ordering::Release);
    }
}

/// Check if any callback is registered (fast atomic check)
fn has_callback() -> bool {
    HAS_ANY_CALLBACK.load(Ordering::Acquire)
}

/// Get the global C FFI callback and userdata for invoking
fn get_callback_with_userdata() -> Option<(*mut std::ffi::c_void, LogCallback)> {
    let callback = if let Ok(cb) = GLOBAL_LOG_CALLBACK.read() {
        *cb
    } else {
        None
    }?;
    let userdata = if let Ok(ud) = GLOBAL_LOG_USERDATA.read() {
        ud.map(|w| w.0)
    } else {
        None
    }
    .unwrap_or(std::ptr::null_mut());
    Some((userdata, callback))
}

/// Get the global JNI callback for invoking
fn get_jni_callback() -> Option<JniLogCallback> {
    if let Ok(cb) = GLOBAL_JNI_LOG_CALLBACK.read() {
        *cb
    } else {
        None
    }
}

/// Send a log event to the global callback (either C FFI or JNI)
fn send_log_event(level: i32, target: &str, message: &str, fields: &[(&str, &str)]) {
    // Try JNI callback first (simpler - no C string conversion needed)
    if let Some(jni_callback) = get_jni_callback() {
        let field_names: Vec<&str> = fields.iter().map(|(n, _)| *n).collect();
        let field_values: Vec<&str> = fields.iter().map(|(_, v)| *v).collect();
        jni_callback(level, target, message, &field_names, &field_values);
        return;
    }

    // Fall back to C FFI callback
    let (userdata, callback) = match get_callback_with_userdata() {
        Some(cb) => cb,
        None => return,
    };

    // Convert strings to C strings
    let target_cstr = match CString::new(target) {
        Ok(s) => s,
        Err(_) => return,
    };
    let message_cstr = match CString::new(message) {
        Ok(s) => s,
        Err(_) => return,
    };

    // Convert fields to FFI structures
    let field_cstrings: Vec<(CString, CString)> = fields
        .iter()
        .filter_map(|(n, v)| Some((CString::new(*n).ok()?, CString::new(*v).ok()?)))
        .collect();

    let ffi_fields: Vec<FfiLogField> = field_cstrings
        .iter()
        .map(|(n, v)| FfiLogField {
            name: n.as_ptr(),
            value: v.as_ptr(),
        })
        .collect();

    let event = FfiLogEvent {
        level,
        target: target_cstr.as_ptr(),
        message: message_cstr.as_ptr(),
        field_count: ffi_fields.len() as i32,
        fields: ffi_fields.as_ptr(),
    };

    // Invoke the global callback with userdata
    callback(userdata, &event);
}

// ============================================================================
// Tracing Layer Implementation
// ============================================================================

/// Field visitor that collects field name/value pairs as strings
struct FieldVisitor {
    fields: Vec<(String, String)>,
    message: Option<String>,
}

impl FieldVisitor {
    fn new() -> Self {
        Self {
            fields: Vec::new(),
            message: None,
        }
    }
}

impl Visit for FieldVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        let name = field.name();
        let value_str = format!("{:?}", value);

        if name == "message" {
            self.message = Some(value_str.trim_matches('"').to_string());
        } else {
            self.fields.push((name.to_string(), value_str));
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        let name = field.name();
        if name == "message" {
            self.message = Some(value.to_string());
        } else {
            self.fields.push((name.to_string(), value.to_string()));
        }
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.fields
            .push((field.name().to_string(), value.to_string()));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.fields
            .push((field.name().to_string(), value.to_string()));
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.fields
            .push((field.name().to_string(), value.to_string()));
    }
}

/// Tracing layer that bridges to Java callbacks
///
/// This layer intercepts tracing events from the Rust driver (targets: crate::*)
/// and forwards them to the global Java callback via `send_log_event`.
pub struct FfiTracingLayer;

/// Get the configured log level for a given tracing target.
/// Returns the level (0=DEBUG, 1=INFO, 2=WARN) for the component.
fn get_configured_level_for_target(target: &str) -> i32 {
    let levels = match CURRENT_LOG_LEVELS.read() {
        Ok(levels) => *levels,
        Err(_) => return 1, // Default to INFO on lock failure
    };

    // Match target to component index
    // levels[0]=command, [1]=connection, [2]=server_selection, [3]=topology
    // Tracing targets are "mongodb::*" (the crate name)
    if target.starts_with("mongodb::command") {
        levels[0]
    } else if target.starts_with("mongodb::connection") {
        levels[1]
    } else if target.starts_with("mongodb::server_selection") {
        levels[2]
    } else if target.starts_with("mongodb::topology") {
        levels[3]
    } else {
        // Unknown mongodb::* target - default to INFO
        1
    }
}

impl<S> Layer<S> for FfiTracingLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let metadata = event.metadata();
        let target = metadata.target();

        // Only process mongodb crate events (tracing target is "mongodb::*")
        if !target.starts_with("mongodb") {
            return;
        }

        // Quick check if callback is registered (avoid work if no one's listening)
        if !has_callback() {
            return;
        }

        // Convert tracing level to our level constants
        let event_level = match *metadata.level() {
            Level::DEBUG | Level::TRACE => LOG_LEVEL_DEBUG,
            Level::INFO | Level::WARN | Level::ERROR => LOG_LEVEL_INFO,
        };

        // Runtime level filtering: skip if event is more verbose than configured level.
        // Lower level number = more verbose (DEBUG=0, INFO=1, WARN=2)
        // Skip if event_level < configured_level (e.g., DEBUG event when INFO is configured)
        let configured_level = get_configured_level_for_target(target);
        if event_level < configured_level {
            return;
        }

        // Visit all fields
        let mut visitor = FieldVisitor::new();
        event.record(&mut visitor);

        // Use the message field or fall back to event name
        let message = visitor.message.as_deref().unwrap_or("");

        // Convert to slice of references for send_log_event
        let field_refs: Vec<(&str, &str)> = visitor
            .fields
            .iter()
            .map(|(n, v)| (n.as_str(), v.as_str()))
            .collect();

        send_log_event(event_level, target, message, &field_refs);
    }
}

/// Initialize the global tracing subscriber with our FFI layer and level filtering.
/// This is called once during library initialization via init_logging().
static INIT_TRACING: Once = Once::new();

/// Store the current log levels (can be updated at runtime via update_log_levels)
/// Order: command, connection, server_selection, topology
/// Levels: 0 = DEBUG, 1 = INFO, 2 = WARN (effectively disabled)
static CURRENT_LOG_LEVELS: RwLock<[i32; 4]> = RwLock::new([1, 1, 1, 1]); // Default: INFO for all

/// Store the current log levels in the global state.
fn store_log_levels(
    command_level: i32,
    connection_level: i32,
    server_selection_level: i32,
    topology_level: i32,
) {
    if let Ok(mut levels) = CURRENT_LOG_LEVELS.write() {
        *levels = [
            command_level,
            connection_level,
            server_selection_level,
            topology_level,
        ];
    }
}

/// Initialize logging globally with a callback and per-component levels.
/// This should be called once at first client creation.
/// The callback receives structured log events; levels control filtering.
///
/// Note: We use EnvFilter set to DEBUG for all crate::* targets, then do
/// runtime filtering in FfiTracingLayer based on CURRENT_LOG_LEVELS.
/// This allows levels to be updated at runtime via update_log_levels().
///
/// The userdata parameter is passed through to the callback for closure context.
pub fn init_logging(
    userdata: *mut std::ffi::c_void,
    callback: LogCallback,
    command_level: i32,
    connection_level: i32,
    server_selection_level: i32,
    topology_level: i32,
) {
    // Set the global callback with userdata
    set_global_log_callback(userdata, callback);

    // Store the initial log levels for runtime filtering
    store_log_levels(
        command_level,
        connection_level,
        server_selection_level,
        topology_level,
    );

    // Initialize the tracing subscriber (only runs once)
    INIT_TRACING.call_once(|| {
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::util::SubscriberInitExt;
        use tracing_subscriber::EnvFilter;

        // Use DEBUG for all crate::* targets - our layer does the actual filtering.
        // This allows runtime level changes without reinitializing the subscriber.
        let env_filter = EnvFilter::new("mongodb=debug");

        // Create a registry with the filter and our FFI layer
        let subscriber = tracing_subscriber::registry()
            .with(env_filter)
            .with(FfiTracingLayer);

        // Set as global default (ignore error if already set)
        let _ = subscriber.try_init();
    });
}

/// Initialize logging with a JNI callback instead of C FFI callback.
/// This is used when JNI is the default FFI, to avoid loading both JNA and JNI libraries.
pub fn init_logging_with_jni_callback(
    callback: Option<JniLogCallback>,
    command_level: i32,
    connection_level: i32,
    server_selection_level: i32,
    topology_level: i32,
) {
    // Set the global JNI callback (if provided)
    if let Some(cb) = callback {
        set_global_jni_log_callback(cb);
    }

    // Store the initial log levels for runtime filtering
    store_log_levels(
        command_level,
        connection_level,
        server_selection_level,
        topology_level,
    );

    // Initialize the tracing subscriber (only runs once)
    INIT_TRACING.call_once(|| {
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::util::SubscriberInitExt;
        use tracing_subscriber::EnvFilter;

        // Use DEBUG for all crate::* targets - our layer does the actual filtering.
        // This allows runtime level changes without reinitializing the subscriber.
        let env_filter = EnvFilter::new("mongodb=debug");

        // Create a registry with the filter and our FFI layer
        let subscriber = tracing_subscriber::registry()
            .with(env_filter)
            .with(FfiTracingLayer);

        // Set as global default (ignore error if already set)
        let _ = subscriber.try_init();
    });
}

/// Update the log levels at runtime.
/// This allows Java to refresh levels from SLF4J periodically.
///
/// The levels are stored in CURRENT_LOG_LEVELS and checked by FfiTracingLayer
/// on each event, so changes take effect immediately.
pub fn update_log_levels(
    command_level: i32,
    connection_level: i32,
    server_selection_level: i32,
    topology_level: i32,
) {
    store_log_levels(
        command_level,
        connection_level,
        server_selection_level,
        topology_level,
    );
}
