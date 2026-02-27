# FFI Operations Layer Design

## Problem Statement

The current FFI approach uses raw command execution (`RunCommandRaw`), where language drivers:
1. Build complete commands as BSON
2. Send to Rust for execution
3. Receive raw BSON responses

This creates abstraction leakage:
- **WriteConcernError retry**: `RunCommandRaw` doesn't check for `writeConcernError` in responses, so retry logic doesn't trigger
- **Bulk write batching**: Drivers must implement batching based on server limits
- **Version-specific fields**: Drivers must know when to include/exclude fields
- **Change stream resumability**: Drivers must handle resume tokens and reconnection
- **Transaction state tracking**: Language drivers must track transaction state and pass it to every operation

## Design Principles

1. **Options as FFI structs, not BSON** - Type-safe contracts, no serialization overhead
2. **Documents/filters/pipelines as BSON** - User data stays as raw BSON
3. **Results as FFI structs** - Except embedded documents which stay as BSON
4. **Async with callbacks** - Operations spawn onto Tokio, invoke callback on completion
5. **Opaque session handles** - Sessions are fully managed in Rust, including transaction state

## Benefits

1. **Spec logic in Rust only** - Retry, batching, version checks handled internally
2. **Type-safe contracts** - FFI structs make the API explicit
3. **No BSON overhead for options** - Direct struct passing
4. **Language drivers become thin wrappers** - Map API to FFI, parse results
5. **No driver changes required** - FFI uses existing `execute_operation` with real `ClientSession`

## Sessions

Sessions are **opaque handles** (`u64`) backed by real `ClientSession` objects stored in Rust.
Transaction state is managed entirely within the session - language drivers don't track it.

### Session Options

```rust
#[repr(C)]
pub struct SessionOptionsFFI {
    /// Causal consistency. -1 = not set (use default), 0 = false, 1 = true
    pub causal_consistency: i8,

    /// Snapshot reads. -1 = not set, 0 = false, 1 = true
    pub snapshot: i8,

    /// Default transaction options (applied when starting transactions)
    pub default_transaction_options: *const TransactionOptionsFFI,  // nullable
}

#[repr(C)]
pub struct TransactionOptionsFFI {
    /// Read concern level (null-terminated string, nullable)
    pub read_concern_level: *const c_char,

    /// Write concern w value. -1 = not set, 0 = unacknowledged, 1+ = w value
    pub write_concern_w: i32,
    /// Write concern w tag (for w:"majority", null-terminated, nullable)
    pub write_concern_w_tag: *const c_char,
    /// Write concern journal. -1 = not set, 0 = false, 1 = true
    pub write_concern_j: i8,
    /// Write concern wtimeout in milliseconds. -1 = not set
    pub write_concern_w_timeout_ms: i64,

    /// Read preference mode. 0=primary, 1=primaryPreferred, 2=secondary, etc.
    pub read_preference_mode: u8,

    /// Max commit time in milliseconds. -1 = not set
    pub max_commit_time_ms: i64,
}
```

### Session Lifecycle

```rust
/// Start a new session. Returns session handle (non-zero), or 0 on error.
pub extern "C" fn mongo_session_start(
    client: *mut MongoClient,
    options: *const SessionOptionsFFI,  // nullable
) -> u64

/// End a session. The session handle becomes invalid after this call.
pub extern "C" fn mongo_session_end(
    client: *mut MongoClient,
    session_handle: u64,
)
```

### Transaction Control

Transaction methods are on the session, not standalone operations. This keeps transaction state opaque.

```rust
pub type TransactionCallback = extern "C" fn(
    userdata: *mut c_void,
    error: *const ErrorFFI,  // null on success
);

/// Start a transaction on the session.
pub extern "C" fn mongo_session_start_transaction(
    client: *mut MongoClient,
    session_handle: u64,
    options: *const TransactionOptionsFFI,  // nullable
    callback: TransactionCallback,
    userdata: *mut c_void,
)

/// Commit the current transaction.
pub extern "C" fn mongo_session_commit_transaction(
    client: *mut MongoClient,
    session_handle: u64,
    callback: TransactionCallback,
    userdata: *mut c_void,
)

/// Abort the current transaction.
pub extern "C" fn mongo_session_abort_transaction(
    client: *mut MongoClient,
    session_handle: u64,
    callback: TransactionCallback,
    userdata: *mut c_void,
)
```

### Why Opaque Sessions?

| Aspect | External State (Old) | Opaque Sessions (New) |
|--------|---------------------|----------------------|
| **Transaction state** | Language driver tracks | Rust `ClientSession` manages |
| **State sync** | Must pass state every operation | Not needed |
| **Commit retry** | Driver must handle | `commit_transaction()` handles internally |
| **Error labels** | Driver interprets | Rust applies automatically |
| **Driver changes** | New executor method needed | **None** - uses existing APIs |

## Read Preference

Read preferences are **opaque handles** (`u64`). Create once, reuse across operations.

```rust
#[repr(C)]
pub struct ReadPreferenceOptionsFFI {
    /// Mode: 0=primary, 1=primaryPreferred, 2=secondary, 3=secondaryPreferred, 4=nearest
    pub mode: u8,

    /// Tag sets as raw BSON array, nullable. Example: [{"dc": "east"}, {"dc": "west"}]
    pub tags: *const u8,
    pub tags_len: usize,

    /// Max staleness in seconds. -1 = not set
    pub max_staleness_seconds: i64,

    /// Hedge options as raw BSON document, nullable. Example: {"enabled": true}
    pub hedge: *const u8,
    pub hedge_len: usize,
}

/// Create a read preference. Returns handle (non-zero), or 0 on error.
pub extern "C" fn mongo_read_preference_create(
    client: *mut MongoClient,
    options: *const ReadPreferenceOptionsFFI,
) -> u64

/// Destroy a read preference handle.
pub extern "C" fn mongo_read_preference_destroy(
    client: *mut MongoClient,
    handle: u64,
)
```

## Write Concern

Write concerns are **opaque handles** (`u64`). Create once, reuse across operations.

```rust
#[repr(C)]
pub struct WriteConcernOptionsFFI {
    /// W value. -1 = not set, 0 = unacknowledged, 1+ = w value
    /// Use w_tag for string values like "majority"
    pub w: i32,

    /// W tag (for w:"majority" etc), null-terminated, nullable
    /// If set, w field is ignored
    pub w_tag: *const c_char,

    /// Journal. -1 = not set, 0 = false, 1 = true
    pub journal: i8,

    /// Write timeout in milliseconds. -1 = not set
    pub w_timeout_ms: i64,
}

/// Create a write concern. Returns handle (non-zero), or 0 on error.
pub extern "C" fn mongo_write_concern_create(
    client: *mut MongoClient,
    options: *const WriteConcernOptionsFFI,
) -> u64

/// Destroy a write concern handle.
pub extern "C" fn mongo_write_concern_destroy(
    client: *mut MongoClient,
    handle: u64,
)
```

## Read Concern

Read concerns are **opaque handles** (`u64`). Create once, reuse across operations.

```rust
#[repr(C)]
pub struct ReadConcernOptionsFFI {
    /// Level: null-terminated string (e.g., "local", "majority", "snapshot", "linearizable")
    pub level: *const c_char,
}

/// Create a read concern. Returns handle (non-zero), or 0 on error.
pub extern "C" fn mongo_read_concern_create(
    client: *mut MongoClient,
    options: *const ReadConcernOptionsFFI,
) -> u64

/// Destroy a read concern handle.
pub extern "C" fn mongo_read_concern_destroy(
    client: *mut MongoClient,
    handle: u64,
)
```

## Operation Context

Passed to every operation. Contains handles for session, read preference, write concern, and read concern.

```rust
#[repr(C)]
pub struct OperationContext {
    /// Session handle (0 = no session)
    pub session_handle: u64,

    /// Read preference handle (0 = use default/inherit from session)
    pub read_preference_handle: u64,

    /// Write concern handle (0 = use default/inherit from session)
    pub write_concern_handle: u64,

    /// Read concern handle (0 = use default/inherit from session)
    pub read_concern_handle: u64,

    /// Timeout in milliseconds (CSOT). -1 = not set (use client default)
    pub timeout_ms: i64,
}
```

## Errors

Errors use a tagged union. Language drivers switch on `error_type` to produce idiomatic exceptions.

### Server Errors

```rust
#[repr(C)]
pub struct ServerError {
    pub code: i32,
    pub code_name: *const c_char,     // null-terminated
    pub message: *const c_char,       // null-terminated
    pub labels: *const *const c_char, // error labels (RetryableWriteError, etc.)
    pub labels_len: usize,
}

#[repr(C)]
pub struct WriteError {
    pub index: u32,
    pub code: i32,
    pub message: *const c_char,
}

#[repr(C)]
pub struct WriteConcernError {
    pub code: i32,
    pub message: *const c_char,
    pub labels: *const *const c_char,
    pub labels_len: usize,
}

#[repr(C)]
pub struct BulkWriteError {
    pub write_errors: *const WriteError,
    pub write_errors_len: usize,
    pub write_concern_error: *const WriteConcernError,  // nullable
    pub partial_result: *const BulkWriteResult,         // nullable
}
```

### Client Errors

```rust
#[repr(C)]
pub struct IoError {
    pub message: *const c_char,
}

#[repr(C)]
pub struct ServerSelectionError {
    pub message: *const c_char,
    pub timeout_ms: i64,
}

#[repr(C)]
pub struct TimeoutError {
    pub message: *const c_char,
    pub timeout_ms: i64,
}

#[repr(C)]
pub struct AuthError {
    pub message: *const c_char,
}

#[repr(C)]
pub struct InvalidArgumentError {
    pub message: *const c_char,
}
```

### ErrorFFI (Tagged Union)

```rust
#[repr(C)]
pub struct ErrorFFI {
    pub error_type: u8,  // 0=Server, 1=BulkWrite, 2=Io, 3=ServerSelection, 4=Timeout, 5=Auth, 6=InvalidArgument
    pub error: ErrorUnion,
}

#[repr(C)]
pub union ErrorUnion {
    pub server: *const ServerError,
    pub bulk_write: *const BulkWriteError,
    pub io: *const IoError,
    pub server_selection: *const ServerSelectionError,
    pub timeout: *const TimeoutError,
    pub auth: *const AuthError,
    pub invalid_argument: *const InvalidArgumentError,
}
```

## Results

### Write Results

```rust
#[repr(C)]
pub struct InsertOneResult {
    pub inserted_id: *const u8,      // Raw BSON value
    pub inserted_id_len: usize,
}

#[repr(C)]
pub struct UpdateResult {
    pub matched_count: u64,
    pub modified_count: u64,
    pub upserted_id: *const u8,      // Raw BSON value, nullable
    pub upserted_id_len: usize,
}

#[repr(C)]
pub struct DeleteResult {
    pub deleted_count: u64,
}

#[repr(C)]
pub struct BulkWriteResult {
    pub inserted_count: u64,
    pub matched_count: u64,
    pub modified_count: u64,
    pub deleted_count: u64,
    pub upserted_count: u64,
    pub inserted_ids: *const u8,     // Raw BSON document {index: id, ...}
    pub inserted_ids_len: usize,
    pub upserted_ids: *const u8,     // Raw BSON document {index: id, ...}
    pub upserted_ids_len: usize,
}
```

### Cursor Results

```rust
#[repr(C)]
pub struct FindResult {
    pub cursor_handle: u64,
    pub server_address: *const c_char,
    pub server_port: u16,
}

#[repr(C)]
pub struct ChangeStreamResult {
    pub cursor_handle: u64,
    pub server_address: *const c_char,
    pub server_port: u16,
}
```

## Callbacks

```rust
pub type InsertOneCallback = extern "C" fn(
    userdata: *mut c_void,
    result: *const InsertOneResult,  // null on error
    error: *const ErrorFFI,          // null on success
);

pub type UpdateCallback = extern "C" fn(
    userdata: *mut c_void,
    result: *const UpdateResult,
    error: *const ErrorFFI,
);

pub type FindCallback = extern "C" fn(
    userdata: *mut c_void,
    result: *const FindResult,
    error: *const ErrorFFI,
);

pub type BulkWriteCallback = extern "C" fn(
    userdata: *mut c_void,
    result: *const BulkWriteResult,
    error: *const ErrorFFI,
);
```

## Cursors

Cursors are managed via handles. Operations that return cursors (find, aggregate, watch) return a `cursor_handle` in their result struct.

```rust
/// Get more results from a cursor (async).
pub extern "C" fn mongo_cursor_get_more(
    client: *mut MongoClient,
    cursor_handle: u64,
    userdata: *mut c_void,
    callback: GetMoreResultCallback,
)

/// Close a cursor (async).
pub extern "C" fn mongo_cursor_close(
    client: *mut MongoClient,
    cursor_handle: u64,
    userdata: *mut c_void,
    callback: SingleResultCallback,
)

pub type GetMoreResultCallback = extern "C" fn(
    userdata: *mut c_void,
    success: bool,
    exhausted: bool,      // true if no more batches
    data: *const BsonBytes,
);
```

## Operations

All operations take a `MongoClient*` as the first parameter. This provides access to:
- The Tokio runtime for async execution
- The session pool for resolving session handles
- The cursor manager for cursor handles

### Collection Namespace

Operations use database name and collection name strings rather than handles, for simplicity:

```rust
/// Insert a single document (async)
pub extern "C" fn ffi_insert_one(
    client: *const MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,           // null-terminated
    coll_name: *const c_char,         // null-terminated
    document: *const u8,
    document_len: usize,
    // Operation-specific options
    bypass_document_validation: i8,   // -1 = None, 0 = false, 1 = true
    comment: *const u8,               // Raw BSON value, nullable
    comment_len: usize,
    // Async callback
    callback: InsertOneCallback,
    userdata: *mut c_void,
)

/// Update documents matching a filter (async)
pub extern "C" fn ffi_update_one(
    client: *const MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    filter: *const u8,
    filter_len: usize,
    update: *const u8,
    update_len: usize,
    // Options
    upsert: i8,
    array_filters: *const u8,         // Raw BSON array, nullable
    array_filters_len: usize,
    hint: *const u8,                  // Raw BSON (string or document), nullable
    hint_len: usize,
    collation: *const u8,             // Raw BSON document, nullable
    collation_len: usize,
    // Async callback
    callback: UpdateCallback,
    userdata: *mut c_void,
)

/// Find documents (async) - returns cursor handle for iteration
pub extern "C" fn ffi_find(
    client: *const MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    filter: *const u8,
    filter_len: usize,
    // Options
    projection: *const u8,            // nullable
    projection_len: usize,
    sort: *const u8,                  // nullable
    sort_len: usize,
    limit: i64,                       // -1 = not set
    skip: i64,                        // -1 = not set
    batch_size: i32,                  // -1 = not set
    // Async callback
    callback: FindCallback,
    userdata: *mut c_void,
)

/// Watch for changes (async) - iteration uses mongo_cursor_next
pub extern "C" fn ffi_watch(
    client: *const MongoClient,
    ctx: *const OperationContext,
    target_type: u8,                  // 0=client, 1=database, 2=collection
    db_name: *const c_char,           // nullable for client-level watch
    coll_name: *const c_char,         // nullable for database/client-level watch
    pipeline: *const u8,              // Raw BSON array
    pipeline_len: usize,
    // Options
    full_document: u8,                // 0=default, 1=updateLookup, etc.
    start_after: *const u8,           // nullable
    start_after_len: usize,
    // Async callback
    callback: ChangeStreamCallback,
    userdata: *mut c_void,
)

/// Bulk write (async)
pub extern "C" fn ffi_bulk_write(
    client: *const MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    models: *const WriteModel,
    models_len: usize,
    // Options
    ordered: i8,                      // -1=not set, 0=false, 1=true
    bypass_document_validation: i8,
    // Async callback
    callback: BulkWriteCallback,
    userdata: *mut c_void,
)
```

### Bulk Write Models

```rust
#[repr(C)]
pub struct InsertOneModel {
    pub document: *const u8,
    pub document_len: usize,
}

#[repr(C)]
pub struct UpdateOneModel {
    pub filter: *const u8,
    pub filter_len: usize,
    pub update: *const u8,
    pub update_len: usize,
    pub upsert: i8,                   // -1=not set, 0=false, 1=true
    pub array_filters: *const u8,     // nullable
    pub array_filters_len: usize,
    pub collation: *const u8,         // nullable
    pub collation_len: usize,
    pub hint: *const u8,              // nullable
    pub hint_len: usize,
}

#[repr(C)]
pub struct WriteModel {
    pub model_type: u8,               // 0=insertOne, 1=updateOne, 2=updateMany, etc.
    pub model: WriteModelUnion,       // tagged union
}
```

### Operations to Implement

| Category | Operations |
|----------|------------|
| **Session** | `mongo_session_start`, `mongo_session_end`, `mongo_session_start_transaction`, `mongo_session_commit_transaction`, `mongo_session_abort_transaction` |
| **Read Preference** | `mongo_read_preference_create`, `mongo_read_preference_destroy` |
| **Write Concern** | `mongo_write_concern_create`, `mongo_write_concern_destroy` |
| **Read Concern** | `mongo_read_concern_create`, `mongo_read_concern_destroy` |
| **CRUD** | insert_one, insert_many, update_one, update_many, replace_one, delete_one, delete_many |
| **Find** | find, find_one, find_one_and_update, find_one_and_replace, find_one_and_delete |
| **Aggregate** | aggregate, count_documents, estimated_document_count, distinct |
| **Bulk** | bulk_write (collection), client_bulk_write |
| **Index** | create_indexes, drop_index, list_indexes |
| **Collection** | create_collection, drop_collection, rename_collection, list_collections |
| **Database** | drop_database, list_databases, run_command |
| **Change Stream** | watch (collection, database, client) |
| **Cursor** | cursor_next, cursor_close |

**Notes:**
- Transaction operations (`start_transaction`, `commit_transaction`, `abort_transaction`) are **session methods**, not standalone operations.
- Read preference, write concern, and read concern handles can be created once and reused across multiple operations.
