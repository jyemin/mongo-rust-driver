# FFI Operations Layer Implementation Plan

## Overview

This plan details the implementation of a typed FFI operations layer for the MongoDB Rust driver. The current FFI implementation uses raw command execution (`RunCommandRaw`) where language drivers build complete BSON commands. The new design provides typed operations (`ffi_insert_one`, `ffi_find`, etc.) where:

- **Options** are passed as FFI structs (not BSON) - type-safe contracts
- **Documents/filters** stay as raw BSON - user data unchanged
- **Results** are FFI structs - typed responses
- **Rust handles all spec logic** - retry, batching, version checks
- **Opaque handles** for sessions, read preferences, write concerns, read concerns

### Goals
1. Move spec logic (retry, batching, version checks) entirely to Rust
2. Provide type-safe FFI contracts for all CRUD operations
3. Maintain backward compatibility with existing `mongo_execute_command` API
4. Enable incremental migration - each phase independently testable

### Scope Boundaries
- **Included**: CRUD operations, aggregate, bulk write, cursors, transactions, indexes, collection/database management
- **Excluded**: GridFS, client-side encryption, search indexes (future phases)

---

## Driver Integration

### Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│  Language Driver (Java/Python/etc.)                                      │
│  - Thin wrapper over FFI                                                 │
│  - No transaction state tracking (opaque in Rust)                        │
│  - No spec logic (handled by Rust)                                       │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  FFI Layer (driver/src/ffi/)                                             │
│  - Typed operations (ffi_insert_one, ffi_find, etc.)                     │
│  - Session pool storing real ClientSession objects                       │
│  - Handle pools for read preference, write concern, read concern         │
│  - Transaction methods on sessions                                       │
│  - Cursor management                                                     │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  Driver Core (unchanged)                                                 │
│  - client.execute_operation(op, &mut session)                            │
│  - Full spec compliance: retry, error handling, transactions             │
└─────────────────────────────────────────────────────────────────────────┘
```

### Key Design Decision: Opaque Sessions with Real ClientSession

The FFI session pool stores **real `ClientSession` objects** behind opaque handles. This eliminates the need for driver core changes and provides full spec compliance automatically.

**Session Pool Implementation:**
- `DashMap<u64, Arc<Mutex<ClientSession>>>` - Maps handles to real sessions
- Transaction state managed by `ClientSession` internally
- Operations lock the session, call `execute_operation(&mut session)`, release
- No external session state synchronization needed

**Benefits:**
- No driver core changes required
- Full transaction support (start/commit/abort handled by real session)
- Automatic commit retry (handled by `ClientSession::commit_transaction`)
- Error labels applied automatically

### Integration Flow

```
┌─────────────────────────────────────────────────────────────────────────┐
│  FFI Entry Point (ffi_insert_one)                                        │
│  1. Validate FFI parameters                                              │
│  2. Parse BSON document bytes → RawDocumentBuf                           │
│  3. Resolve handles: session, read_pref, write_concern, read_concern     │
│  4. Create Insert operation with resolved options                        │
│  5. Spawn async task                                                     │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  Async Task                                                              │
│  1. Lock session (if session_handle != 0)                                │
│  2. Call client.execute_operation(op, &mut session)                      │
│  3. Release session lock                                                 │
│  4. Convert result to FFI struct                                         │
│  5. Invoke callback                                                      │
└─────────────────────────────────────────────────────────────────────────┘
```

### No Driver Core Changes Required

Because we store real `ClientSession` objects and use the existing `execute_operation` method:
- All retry logic works automatically
- Transaction state transitions handled by `ClientSession`
- WriteConcernError retry works via typed operations
- Error labels applied by executor

---

## Prerequisites

1. **Existing FFI Infrastructure** (already implemented):
   - `MongoClient` with runtime, cursor manager
   - `CursorManager` for cursor handle management
   - `BsonBytes` for raw BSON transfer
   - Callback-based async execution pattern

2. **Dependencies** (no new external dependencies required):
   - All operations use existing `driver/src/operation/` implementations
   - `tokio::sync::Mutex` for session locking
   - `dashmap` for handle storage (already a dependency)

---

## Phase 0: Raw Document Support in Operations

The driver's operation layer is **partially raw-friendly**. Some operations accept `RawDocumentBuf`,
others require typed `Document` and convert internally. For FFI efficiency, we should add raw
constructors to avoid Document → RawDocumentBuf → Document round-trips.

### Current State - Full Analysis

| Operation | Filter/Query | Document/Pipeline | Status |
|-----------|--------------|-------------------|--------|
| **Insert** | N/A | `Vec<&RawDocument>` | ✅ Ready |
| **Update** | `Document` | `RawDocumentBuf` (replace) | ⚠️ Filter needs raw |
| **Delete** | `Document` | N/A | ❌ Filter needs raw |
| **Find** | `Document` | N/A | ❌ Filter needs raw |
| **FindAndModify** | `Document` | `RawDocumentBuf` | ⚠️ Query needs raw |
| **Aggregate** | N/A | `Vec<Document>` | ❌ Pipeline needs raw |
| **CountDocuments** | `Document` | N/A | ❌ Uses Aggregate internally |
| **Distinct** | `Document` (filter) | N/A | ❌ Filter needs raw |
| **RunCommand** | N/A | `RawDocumentBuf` | ✅ Ready |
| **RunCommandRaw** | N/A | `RawDocumentBuf` | ✅ Ready |
| **GetMore** | N/A | N/A | ✅ Ready (no user docs) |
| **ListDatabases** | N/A | N/A | ✅ Ready (no user docs) |
| **ListCollections** | `Document` (filter in options) | N/A | ⚠️ Filter in options |
| **CreateIndexes** | N/A | `Vec<IndexModel>` (keys: Document) | ⚠️ Keys are Document |
| **DropIndexes** | N/A | N/A | ✅ Ready (no user docs) |
| **BulkWrite (client)** | `Document` (in WriteModel) | `Document` (in WriteModel) | ❌ WriteModel uses Document |

### WriteModel Analysis (Client Bulk Write)

The `WriteModel` enum used by client-level bulk write contains typed `Document` fields:

```rust
pub struct InsertOneModel {
    pub document: Document,        // ❌ Needs RawDocumentBuf
}
pub struct UpdateOneModel {
    pub filter: Document,          // ❌ Needs RawDocumentBuf
    pub update: UpdateModifications,  // Already supports raw via RawBson
}
pub struct ReplaceOneModel {
    pub filter: Document,          // ❌ Needs RawDocumentBuf
    pub replacement: Document,     // ❌ Needs RawDocumentBuf
}
pub struct DeleteOneModel {
    pub filter: Document,          // ❌ Needs RawDocumentBuf
}
```

### Changes Required

Add `_raw` constructor variants to operations that accept `RawDocumentBuf` instead of `Document`:

#### 0.1 Find - Add raw filter support

**File:** `driver/src/operation/find.rs`

```rust
// Existing
pub(crate) struct Find {
    target: Collection<Document>,
    filter: Document,  // ❌ Typed
    options: Option<Box<FindOptions>>,
}

// Add enum to support both
pub(crate) enum FilterDoc {
    Typed(Document),
    Raw(RawDocumentBuf),
}

pub(crate) struct Find {
    target: Collection<Document>,
    filter: FilterDoc,  // ✅ Either typed or raw
    options: Option<Box<FindOptions>>,
}

impl Find {
    // Existing constructor (for backward compat)
    pub(crate) fn new(..., filter: Document, ...) -> Self { ... }

    // New raw constructor (for FFI)
    pub(crate) fn new_raw(..., filter: RawDocumentBuf, ...) -> Self { ... }
}
```

In `build()`, use the filter directly if raw:
```rust
fn build(&mut self, ...) -> Result<Command> {
    // ...
    let raw_filter = match &self.filter {
        FilterDoc::Typed(doc) => RawDocumentBuf::try_from(doc)?,
        FilterDoc::Raw(raw) => raw.clone(),  // Zero-copy if we take ownership
    };
    body.append(cstr!("filter"), raw_filter);
    // ...
}
```

#### 0.2 Delete - Add raw filter support

**File:** `driver/src/operation/delete.rs`

Same pattern as Find. Currently uses `doc!` macro which creates typed Document.
Refactor to build raw command directly.

```rust
pub(crate) struct Delete {
    target: Collection<Document>,
    filter: FilterDoc,  // Change from Document
    // ...
}

impl Delete {
    pub(crate) fn new_raw(
        target: Collection<Document>,
        filter: RawDocumentBuf,
        limit: Option<u32>,
        options: Option<DeleteOptions>,
    ) -> Self { ... }
}
```

#### 0.3 Update - Add raw filter support

**File:** `driver/src/operation/update.rs`

Filter is currently `Document`. Add raw variant:

```rust
pub(crate) struct Update {
    target: Collection<Document>,
    filter: FilterDoc,  // Change from Document
    update: UpdateOrReplace,
    // ...
}

impl Update {
    pub(crate) fn with_update_raw(
        target: Collection<Document>,
        filter: RawDocumentBuf,
        update: UpdateModifications,
        multi: bool,
        options: Option<UpdateOptions>,
    ) -> Self { ... }
}
```

#### 0.4 Aggregate - Add raw pipeline support

**File:** `driver/src/operation/aggregate.rs`

Pipeline is currently `Vec<Document>`. Add raw variant:

```rust
pub(crate) enum Pipeline {
    Typed(Vec<Document>),
    Raw(RawArrayBuf),
}

pub(crate) struct Aggregate {
    target: OperationTarget,
    pipeline: Pipeline,
    options: Option<AggregateOptions>,
}

impl Aggregate {
    pub(crate) fn new_raw(
        target: OperationTarget,
        pipeline: RawArrayBuf,
        options: Option<AggregateOptions>,
    ) -> Self { ... }
}
```

#### 0.5 FindAndModify - Add raw query support

**File:** `driver/src/operation/find_and_modify.rs`

Query is currently `Document`. The update/replacement already supports `RawDocumentBuf`.

```rust
pub(crate) struct FindAndModify {
    // ...
    query: FilterDoc,  // Change from Document
    // ...
}
```

#### 0.6 CountDocuments - Uses Aggregate

**File:** `driver/src/operation/count_documents.rs`

`CountDocuments` wraps `Aggregate` internally. Once Aggregate supports raw pipelines,
we can add a `new_raw()` constructor that builds a raw `$match` stage.

```rust
impl CountDocuments {
    pub(crate) fn new_raw(
        coll: &Collection<Document>,
        filter: RawDocumentBuf,  // Raw filter
        options: Option<CountOptions>,
    ) -> Result<Self> {
        // Build $match stage with raw filter
        let match_stage = rawdoc! { "$match": filter };
        // ... build raw pipeline
    }
}
```

#### 0.7 Distinct - Add raw filter support

**File:** `driver/src/operation/distinct.rs`

Filter is currently `Option<Document>`.

```rust
pub(crate) struct Distinct {
    // ...
    filter: Option<FilterDoc>,  // Change from Option<Document>
    // ...
}
```

#### 0.8 Client Bulk Write - Add raw support to WriteModel

**File:** `driver/src/client/options/bulk_write.rs`

The `WriteModel` enum contains `Document` fields throughout. Add raw variants:

```rust
/// Enum to hold either typed or raw document
pub(crate) enum DocOrRaw {
    Typed(Document),
    Raw(RawDocumentBuf),
}

impl From<Document> for DocOrRaw {
    fn from(doc: Document) -> Self { Self::Typed(doc) }
}

impl From<RawDocumentBuf> for DocOrRaw {
    fn from(raw: RawDocumentBuf) -> Self { Self::Raw(raw) }
}

// Update models to use DocOrRaw internally
pub struct InsertOneModel {
    pub namespace: Namespace,
    pub(crate) document: DocOrRaw,  // Was: Document
}

impl InsertOneModel {
    pub fn new(namespace: Namespace, document: Document) -> Self { ... }
    pub fn new_raw(namespace: Namespace, document: RawDocumentBuf) -> Self { ... }
}

pub struct UpdateOneModel {
    pub namespace: Namespace,
    pub(crate) filter: DocOrRaw,      // Was: Document
    pub update: UpdateModifications,   // Already supports raw via RawBson
    // ... other fields
}

impl UpdateOneModel {
    pub fn new(..., filter: Document, ...) -> Self { ... }
    pub fn new_raw(..., filter: RawDocumentBuf, ...) -> Self { ... }
}

pub struct ReplaceOneModel {
    pub namespace: Namespace,
    pub(crate) filter: DocOrRaw,       // Was: Document
    pub(crate) replacement: DocOrRaw,  // Was: Document
    // ...
}

pub struct DeleteOneModel {
    pub namespace: Namespace,
    pub(crate) filter: DocOrRaw,  // Was: Document
    // ...
}

// Same pattern for UpdateManyModel, DeleteManyModel
```

**Serialization:** Update the serde `Serialize` impl to handle `DocOrRaw`:

```rust
impl Serialize for DocOrRaw {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            DocOrRaw::Typed(doc) => doc.serialize(serializer),
            DocOrRaw::Raw(raw) => raw.serialize(serializer),
        }
    }
}
```

#### 0.9 ListCollections - Filter in options

**File:** `driver/src/db/options.rs`

The filter is inside `ListCollectionsOptions`:

```rust
pub struct ListCollectionsOptions {
    pub filter: Option<Document>,  // Change to Option<DocOrRaw>
    // ...
}
```

#### 0.10 CreateIndexes - Keys in IndexModel

**File:** `driver/src/index.rs`

The `IndexModel` has `keys: Document`:

```rust
pub struct IndexModel {
    pub keys: Document,  // Change to DocOrRaw
    // ...
}

impl IndexModel {
    pub fn new_raw(keys: RawDocumentBuf, options: Option<IndexOptions>) -> Self { ... }
}
```

### Implementation Approach

Two options:

**Option A: FilterDoc enum (shown above)**
- Add enum that holds either `Document` or `RawDocumentBuf`
- Minimal changes to struct layout
- Small runtime branch in `build()`

**Option B: Generic over filter type**
- `Find<F: IntoRawFilter>` with trait
- More type-safe but larger API surface change
- Affects action layer

**Recommendation:** Option A is simpler and sufficient for FFI needs.

### Files Changed in Phase 0

| File | Changes |
|------|---------|
| `driver/src/operation/find.rs` | Add `FilterDoc` enum, `new_raw()` constructor |
| `driver/src/operation/delete.rs` | Add `FilterDoc`, `new_raw()` |
| `driver/src/operation/update.rs` | Add `FilterDoc`, `with_update_raw()` |
| `driver/src/operation/aggregate.rs` | Add `Pipeline` enum, `new_raw()` |
| `driver/src/operation/find_and_modify.rs` | Add `FilterDoc` for query |
| `driver/src/operation/count_documents.rs` | Add `new_raw()` with raw filter |
| `driver/src/operation/distinct.rs` | Add `FilterDoc` for filter |
| `driver/src/operation/mod.rs` | Add `FilterDoc`, `Pipeline`, `DocOrRaw` types |
| `driver/src/client/options/bulk_write.rs` | Add `DocOrRaw` to WriteModel variants, `new_raw()` constructors |
| `driver/src/db/options.rs` | Add `DocOrRaw` for ListCollections filter |
| `driver/src/index.rs` | Add `DocOrRaw` for IndexModel keys, `new_raw()` |

### Testing Strategy - Phase 0
- Unit tests: verify raw constructors produce identical commands to typed constructors
- Roundtrip tests: `Document` → `RawDocumentBuf` → operation → command bytes should match
- Integration tests: operations with raw filters work against real MongoDB

---

## Phase 1: Handle Pools & Core FFI Types

**Complexity**: Medium

This phase creates the handle pool infrastructure for sessions, read preferences, write concerns,
and read concerns, plus the core error and result types.

### 1.1 Create `driver/src/ffi/handles.rs` - Handle Pool Infrastructure

```rust
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Mutex;

/// Generic handle pool for storing objects behind opaque u64 handles.
pub struct HandlePool<T> {
    items: DashMap<u64, T>,
    next_handle: AtomicU64,
}

impl<T> HandlePool<T> {
    pub fn new() -> Self {
        Self {
            items: DashMap::new(),
            next_handle: AtomicU64::new(1), // 0 reserved for "none"
        }
    }

    pub fn insert(&self, item: T) -> u64 {
        let handle = self.next_handle.fetch_add(1, Ordering::SeqCst);
        self.items.insert(handle, item);
        handle
    }

    pub fn get(&self, handle: u64) -> Option<dashmap::mapref::one::Ref<'_, u64, T>> {
        self.items.get(&handle)
    }

    pub fn remove(&self, handle: u64) -> Option<T> {
        self.items.remove(&handle).map(|(_, v)| v)
    }
}
```

### 1.2 Create `driver/src/ffi/session.rs` - Session Pool with Real ClientSession

```rust
use crate::client::session::ClientSession;
use tokio::sync::Mutex;

/// Pool storing real ClientSession objects behind opaque handles.
pub struct FfiSessionPool {
    sessions: DashMap<u64, Arc<Mutex<ClientSession>>>,
    next_handle: AtomicU64,
}

impl FfiSessionPool {
    pub fn new() -> Self { ... }

    /// Start a new session, return handle.
    pub async fn start_session(
        &self,
        client: &Client,
        options: Option<SessionOptions>,
    ) -> Result<u64> {
        let session = client.start_session().with_options(options).await?;
        let handle = self.next_handle.fetch_add(1, Ordering::SeqCst);
        self.sessions.insert(handle, Arc::new(Mutex::new(session)));
        Ok(handle)
    }

    /// End a session.
    pub fn end_session(&self, handle: u64) {
        self.sessions.remove(&handle);
    }

    /// Get session for use in operation.
    pub fn get(&self, handle: u64) -> Option<Arc<Mutex<ClientSession>>> {
        self.sessions.get(&handle).map(|r| r.clone())
    }
}

// FFI exports
#[no_mangle]
pub extern "C" fn mongo_session_start(
    client: *mut MongoClient,
    options: *const SessionOptionsFFI,
) -> u64 { ... }

#[no_mangle]
pub extern "C" fn mongo_session_end(
    client: *mut MongoClient,
    session_handle: u64,
) { ... }
```

### 1.3 Create `driver/src/ffi/concerns.rs` - Read/Write Concern Handle Pools

```rust
use crate::options::{ReadConcern, ReadPreference, WriteConcern};

/// Pool for read preference handles.
pub type ReadPreferencePool = HandlePool<ReadPreference>;

/// Pool for write concern handles.
pub type WriteConcernPool = HandlePool<WriteConcern>;

/// Pool for read concern handles.
pub type ReadConcernPool = HandlePool<ReadConcern>;

// FFI exports for read preference
#[no_mangle]
pub extern "C" fn mongo_read_preference_create(
    client: *mut MongoClient,
    options: *const ReadPreferenceOptionsFFI,
) -> u64 { ... }

#[no_mangle]
pub extern "C" fn mongo_read_preference_destroy(
    client: *mut MongoClient,
    handle: u64,
) { ... }

// FFI exports for write concern
#[no_mangle]
pub extern "C" fn mongo_write_concern_create(
    client: *mut MongoClient,
    options: *const WriteConcernOptionsFFI,
) -> u64 { ... }

#[no_mangle]
pub extern "C" fn mongo_write_concern_destroy(
    client: *mut MongoClient,
    handle: u64,
) { ... }

// FFI exports for read concern
#[no_mangle]
pub extern "C" fn mongo_read_concern_create(
    client: *mut MongoClient,
    options: *const ReadConcernOptionsFFI,
) -> u64 { ... }

#[no_mangle]
pub extern "C" fn mongo_read_concern_destroy(
    client: *mut MongoClient,
    handle: u64,
) { ... }
```

### 1.4 Update `driver/src/ffi/client.rs` - Add Handle Pools to MongoClient

```rust
pub struct MongoClient {
    pub(crate) inner: Client,
    pub(crate) runtime: tokio::runtime::Handle,
    pub(crate) session_pool: FfiSessionPool,
    pub(crate) cursor_manager: CursorManager,
    // NEW: Handle pools for concerns
    pub(crate) read_preference_pool: ReadPreferencePool,
    pub(crate) write_concern_pool: WriteConcernPool,
    pub(crate) read_concern_pool: ReadConcernPool,
}
```

### 1.5 Create `driver/src/ffi/types.rs` - OperationContext with Handles

```rust
/// Operation context using handles for all reusable objects.
#[repr(C)]
pub struct OperationContext {
    /// Session handle (0 = no session)
    pub session_handle: u64,

    /// Read preference handle (0 = use default)
    pub read_preference_handle: u64,

    /// Write concern handle (0 = use default)
    pub write_concern_handle: u64,

    /// Read concern handle (0 = use default)
    pub read_concern_handle: u64,

    /// Timeout in milliseconds (CSOT). -1 = not set
    pub timeout_ms: i64,
}
```

### 1.6 Create `driver/src/ffi/error.rs` - Error Types

(Error types as defined in design document - ServerErrorFFI, WriteErrorFFI, etc.)

### 1.7 Create `driver/src/ffi/results.rs` - Result Types

(Result types as defined in design document - InsertOneResultFFI, UpdateResultFFI, etc.)

### Files Changed in Phase 1
| File | Action |
|------|--------|
| `driver/src/ffi/handles.rs` | Create |
| `driver/src/ffi/session.rs` | Rewrite (use real ClientSession) |
| `driver/src/ffi/concerns.rs` | Create |
| `driver/src/ffi/types.rs` | Rewrite (use handles) |
| `driver/src/ffi/error.rs` | Create |
| `driver/src/ffi/results.rs` | Create |
| `driver/src/ffi/client.rs` | Modify (add handle pools) |
| `driver/src/ffi/mod.rs` | Modify (add exports) |

### Testing Strategy - Phase 1
- Unit tests for handle pool operations (insert, get, remove)
- Integration tests for session start/end with real ClientSession
- Unit tests for read preference/write concern/read concern handle creation
- Unit tests for error and result type conversions
- Memory safety tests (valgrind/ASAN)

---

## Phase 2: Core CRUD Operations

**Complexity**: Medium

Operations use `db_name` and `coll_name` strings (null-terminated `*const c_char`) to specify the namespace,
rather than opaque collection handles. This is simpler and matches the design document.

### 2.1 Create `driver/src/ffi/callbacks.rs` - Typed Callbacks

```rust
// Typed callbacks for each operation

pub type InsertOneCallback = extern "C" fn(
    userdata: *mut c_void,
    result: *const InsertOneResultFFI,  // null on error
    error: *const ErrorFFI,              // null on success
);

pub type InsertManyCallback = extern "C" fn(
    userdata: *mut c_void,
    result: *const InsertManyResultFFI,
    error: *const ErrorFFI,
);

pub type UpdateCallback = extern "C" fn(
    userdata: *mut c_void,
    result: *const UpdateResultFFI,
    error: *const ErrorFFI,
);

pub type DeleteCallback = extern "C" fn(
    userdata: *mut c_void,
    result: *const DeleteResultFFI,
    error: *const ErrorFFI,
);

pub type FindCallback = extern "C" fn(
    userdata: *mut c_void,
    result: *const FindResultFFI,
    error: *const ErrorFFI,
);

pub type AggregateCallback = extern "C" fn(
    userdata: *mut c_void,
    result: *const AggregateResultFFI,
    error: *const ErrorFFI,
);

pub type BulkWriteCallback = extern "C" fn(
    userdata: *mut c_void,
    result: *const BulkWriteResultFFI,
    error: *const ErrorFFI,
);

// Simple success/error callback for void operations
pub type VoidCallback = extern "C" fn(
    userdata: *mut c_void,
    error: *const ErrorFFI,  // null on success
);
```

### 2.2 Create `driver/src/ffi/operations/mod.rs` - Operations Module

```rust
//! FFI Operations Layer
//!
//! Typed operations that leverage the driver's Operation trait implementations.

mod insert;
mod update;
mod delete;
mod find;

pub use insert::*;
pub use update::*;
pub use delete::*;
pub use find::*;
```

### 2.3 Create `driver/src/ffi/operations/insert.rs` - Insert Operations

```rust
use crate::ffi::*;
use crate::operation::insert::Insert;

/// Insert options passed across FFI boundary.
#[repr(C)]
pub struct InsertOneOptionsFFI {
    pub bypass_document_validation: i8,  // -1 = None, 0 = false, 1 = true
    pub comment: *const u8,              // Raw BSON value, nullable
    pub comment_len: usize,
}

/// Insert a single document (async).
///
/// # Safety
/// All pointers must be valid for the duration of the call.
/// The callback will be invoked exactly once with either a result or an error.
#[no_mangle]
pub extern "C" fn ffi_insert_one(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,              // null-terminated
    coll_name: *const c_char,            // null-terminated
    document: *const u8,
    document_len: usize,
    options: *const InsertOneOptionsFFI,
    callback: InsertOneCallback,
    userdata: *mut c_void,
) {
    // 1. Validate parameters
    // 2. Parse document from BSON bytes
    // 3. Resolve handles from ctx: session, read_pref, write_concern, read_concern
    // 4. Build InsertManyOptions from FFI options + resolved handles
    // 5. Create Insert operation
    // 6. Spawn async task:
    //    a. Lock session if ctx.session_handle != 0
    //    b. Call client.execute_operation(insert_op, &mut session)
    //    c. Convert result to InsertOneResultFFI
    //    d. Invoke callback
}

/// Insert multiple documents (async).
#[no_mangle]
pub extern "C" fn ffi_insert_many(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    documents: *const u8,              // Raw BSON array of documents
    documents_len: usize,
    options: *const InsertManyOptionsFFI,
    callback: InsertManyCallback,
    userdata: *mut c_void,
) { ... }
```

### 2.4 Create `driver/src/ffi/operations/update.rs` - Update Operations

```rust
#[repr(C)]
pub struct UpdateOptionsFFI {
    pub upsert: i8,                      // -1=not set, 0=false, 1=true
    pub bypass_document_validation: i8,
    pub array_filters: *const u8,        // Raw BSON array, nullable
    pub array_filters_len: usize,
    pub hint: *const u8,                 // Raw BSON (string or document), nullable
    pub hint_len: usize,
    pub collation: *const u8,            // Raw BSON document, nullable
    pub collation_len: usize,
    pub comment: *const u8,
    pub comment_len: usize,
    pub let_vars: *const u8,             // Raw BSON document, nullable
    pub let_vars_len: usize,
}

#[no_mangle]
pub extern "C" fn ffi_update_one(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    filter: *const u8,
    filter_len: usize,
    update: *const u8,                   // Raw BSON (document or array for pipeline)
    update_len: usize,
    options: *const UpdateOptionsFFI,
    callback: UpdateCallback,
    userdata: *mut c_void,
) { ... }

#[no_mangle]
pub extern "C" fn ffi_update_many(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    filter: *const u8,
    filter_len: usize,
    update: *const u8,
    update_len: usize,
    options: *const UpdateOptionsFFI,
    callback: UpdateCallback,
    userdata: *mut c_void,
) { ... }

#[no_mangle]
pub extern "C" fn ffi_replace_one(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    filter: *const u8,
    filter_len: usize,
    replacement: *const u8,
    replacement_len: usize,
    options: *const UpdateOptionsFFI,
    callback: UpdateCallback,
    userdata: *mut c_void,
) { ... }
```

### 2.5 Create `driver/src/ffi/operations/delete.rs` - Delete Operations

```rust
#[repr(C)]
pub struct DeleteOptionsFFI {
    pub collation: *const u8,
    pub collation_len: usize,
    pub hint: *const u8,
    pub hint_len: usize,
    pub comment: *const u8,
    pub comment_len: usize,
    pub let_vars: *const u8,
    pub let_vars_len: usize,
}

#[no_mangle]
pub extern "C" fn ffi_delete_one(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    filter: *const u8,
    filter_len: usize,
    options: *const DeleteOptionsFFI,
    callback: DeleteCallback,
    userdata: *mut c_void,
) { ... }

#[no_mangle]
pub extern "C" fn ffi_delete_many(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    filter: *const u8,
    filter_len: usize,
    options: *const DeleteOptionsFFI,
    callback: DeleteCallback,
    userdata: *mut c_void,
) { ... }
```

### 2.6 Create `driver/src/ffi/operations/find.rs` - Find Operations

```rust
#[repr(C)]
pub struct FindOptionsFFI {
    pub projection: *const u8,            // nullable
    pub projection_len: usize,
    pub sort: *const u8,                  // nullable
    pub sort_len: usize,
    pub limit: i64,                       // -1 = not set
    pub skip: i64,                        // -1 = not set
    pub batch_size: i32,                  // -1 = not set
    pub hint: *const u8,
    pub hint_len: usize,
    pub collation: *const u8,
    pub collation_len: usize,
    pub comment: *const u8,
    pub comment_len: usize,
    pub max_time_ms: i64,                 // -1 = not set
    pub allow_partial_results: i8,        // -1=not set, 0=false, 1=true
    pub cursor_type: u8,                  // 0=non-tailable, 1=tailable, 2=tailable_await
    pub no_cursor_timeout: i8,
    pub allow_disk_use: i8,
    pub show_record_id: i8,
    pub return_key: i8,
    pub min: *const u8,
    pub min_len: usize,
    pub max: *const u8,
    pub max_len: usize,
}

#[no_mangle]
pub extern "C" fn ffi_find(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    filter: *const u8,
    filter_len: usize,
    options: *const FindOptionsFFI,
    callback: FindCallback,
    userdata: *mut c_void,
) { ... }

/// Find a single document - convenience wrapper.
#[no_mangle]
pub extern "C" fn ffi_find_one(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    filter: *const u8,
    filter_len: usize,
    options: *const FindOptionsFFI,
    callback: SingleDocCallback,  // Returns single document or null
    userdata: *mut c_void,
) { ... }
```

### Files Changed in Phase 2
| File | Action |
|------|--------|
| `driver/src/ffi/callbacks.rs` | Create |
| `driver/src/ffi/operations/mod.rs` | Create |
| `driver/src/ffi/operations/insert.rs` | Create |
| `driver/src/ffi/operations/update.rs` | Create |
| `driver/src/ffi/operations/delete.rs` | Create |
| `driver/src/ffi/operations/find.rs` | Create |
| `driver/src/ffi/mod.rs` | Modify (add exports) |

### Testing Strategy - Phase 2
- Integration tests using mock MongoDB or embedded MongoDB
- Test each operation with valid inputs
- Test error handling for invalid inputs
- Test session and handle resolution (read_pref, write_concern, read_concern)
- Test callback invocation semantics

---

## Phase 3: Aggregate, Count, Distinct & Cursor Operations

**Complexity**: Medium

### 3.1 Create `driver/src/ffi/operations/aggregate.rs`

```rust
#[repr(C)]
pub struct AggregateOptionsFFI {
    pub batch_size: i32,                  // -1 = not set
    pub allow_disk_use: i8,
    pub max_time_ms: i64,
    pub bypass_document_validation: i8,
    pub collation: *const u8,
    pub collation_len: usize,
    pub comment: *const u8,
    pub comment_len: usize,
    pub hint: *const u8,
    pub hint_len: usize,
    pub let_vars: *const u8,
    pub let_vars_len: usize,
}

#[no_mangle]
pub extern "C" fn ffi_aggregate(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,             // nullable for database-level aggregation
    pipeline: *const u8,                  // Raw BSON array
    pipeline_len: usize,
    options: *const AggregateOptionsFFI,
    callback: AggregateCallback,
    userdata: *mut c_void,
) { ... }
```

### 3.2 Create `driver/src/ffi/operations/count.rs`

```rust
#[repr(C)]
pub struct CountOptionsFFI {
    pub limit: i64,
    pub skip: i64,
    pub max_time_ms: i64,
    pub hint: *const u8,
    pub hint_len: usize,
    pub collation: *const u8,
    pub collation_len: usize,
    pub comment: *const u8,
    pub comment_len: usize,
}

pub type CountCallback = extern "C" fn(
    userdata: *mut c_void,
    count: i64,                           // -1 on error
    error: *const ErrorFFI,
);

#[no_mangle]
pub extern "C" fn ffi_count_documents(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    filter: *const u8,
    filter_len: usize,
    options: *const CountOptionsFFI,
    callback: CountCallback,
    userdata: *mut c_void,
) { ... }

#[no_mangle]
pub extern "C" fn ffi_estimated_document_count(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    options: *const CountOptionsFFI,  // Only max_time_ms and comment used
    callback: CountCallback,
    userdata: *mut c_void,
) { ... }
```

### 3.3 Create `driver/src/ffi/operations/distinct.rs`

```rust
pub type DistinctCallback = extern "C" fn(
    userdata: *mut c_void,
    values: *const u8,                    // Raw BSON array
    values_len: usize,
    error: *const ErrorFFI,
);

#[no_mangle]
pub extern "C" fn ffi_distinct(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    field_name: *const c_char,
    filter: *const u8,                    // nullable
    filter_len: usize,
    options: *const DistinctOptionsFFI,
    callback: DistinctCallback,
    userdata: *mut c_void,
) { ... }
```

### 3.4 Enhance Cursor Operations in `driver/src/ffi/operations/cursor.rs`

```rust
/// Enhanced cursor getMore with typed callback
#[no_mangle]
pub extern "C" fn ffi_cursor_next(
    client: *mut MongoClient,
    cursor_handle: u64,
    userdata: *mut c_void,
    callback: CursorNextCallback,
) { ... }

pub type CursorNextCallback = extern "C" fn(
    userdata: *mut c_void,
    exhausted: bool,
    batch: *const u8,                     // Raw BSON array of documents
    batch_len: usize,
    error: *const ErrorFFI,
);

/// Close cursor with typed callback
#[no_mangle]
pub extern "C" fn ffi_cursor_close(
    client: *mut MongoClient,
    cursor_handle: u64,
    userdata: *mut c_void,
    callback: VoidCallback,
) { ... }
```

### Files Changed in Phase 3
| File | Action |
|------|--------|
| `driver/src/ffi/operations/aggregate.rs` | Create |
| `driver/src/ffi/operations/count.rs` | Create |
| `driver/src/ffi/operations/distinct.rs` | Create |
| `driver/src/ffi/operations/cursor.rs` | Create |
| `driver/src/ffi/operations/mod.rs` | Modify |

---

## Phase 4: Find-and-Modify Operations

**Complexity**: Medium

### 4.1 Create `driver/src/ffi/operations/find_and_modify.rs`

```rust
#[repr(C)]
pub struct FindOneAndUpdateOptionsFFI {
    pub projection: *const u8,
    pub projection_len: usize,
    pub sort: *const u8,
    pub sort_len: usize,
    pub upsert: i8,
    pub return_document: u8,              // 0=before, 1=after
    pub bypass_document_validation: i8,
    pub array_filters: *const u8,
    pub array_filters_len: usize,
    pub hint: *const u8,
    pub hint_len: usize,
    pub collation: *const u8,
    pub collation_len: usize,
    pub max_time_ms: i64,
    pub let_vars: *const u8,
    pub let_vars_len: usize,
    pub comment: *const u8,
    pub comment_len: usize,
}

pub type FindOneAndModifyCallback = extern "C" fn(
    userdata: *mut c_void,
    document: *const u8,                  // Raw BSON document, null if not found
    document_len: usize,
    error: *const ErrorFFI,
);

#[no_mangle]
pub extern "C" fn ffi_find_one_and_update(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    filter: *const u8,
    filter_len: usize,
    update: *const u8,
    update_len: usize,
    options: *const FindOneAndUpdateOptionsFFI,
    callback: FindOneAndModifyCallback,
    userdata: *mut c_void,
) { ... }

#[no_mangle]
pub extern "C" fn ffi_find_one_and_replace(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    filter: *const u8,
    filter_len: usize,
    replacement: *const u8,
    replacement_len: usize,
    options: *const FindOneAndReplaceOptionsFFI,
    callback: FindOneAndModifyCallback,
    userdata: *mut c_void,
) { ... }

#[no_mangle]
pub extern "C" fn ffi_find_one_and_delete(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    filter: *const u8,
    filter_len: usize,
    options: *const FindOneAndDeleteOptionsFFI,
    callback: FindOneAndModifyCallback,
    userdata: *mut c_void,
) { ... }
```

---

## Phase 5: Bulk Write Operations

**Complexity**: High

### 5.1 Create `driver/src/ffi/operations/bulk_write.rs`

```rust
// Write model types (tagged union)
pub const WRITE_MODEL_INSERT_ONE: u8 = 0;
pub const WRITE_MODEL_UPDATE_ONE: u8 = 1;
pub const WRITE_MODEL_UPDATE_MANY: u8 = 2;
pub const WRITE_MODEL_REPLACE_ONE: u8 = 3;
pub const WRITE_MODEL_DELETE_ONE: u8 = 4;
pub const WRITE_MODEL_DELETE_MANY: u8 = 5;

#[repr(C)]
pub struct InsertOneModelFFI {
    pub document: *const u8,
    pub document_len: usize,
}

#[repr(C)]
pub struct UpdateOneModelFFI {
    pub filter: *const u8,
    pub filter_len: usize,
    pub update: *const u8,
    pub update_len: usize,
    pub upsert: i8,
    pub array_filters: *const u8,
    pub array_filters_len: usize,
    pub collation: *const u8,
    pub collation_len: usize,
    pub hint: *const u8,
    pub hint_len: usize,
}

#[repr(C)]
pub struct DeleteOneModelFFI {
    pub filter: *const u8,
    pub filter_len: usize,
    pub collation: *const u8,
    pub collation_len: usize,
    pub hint: *const u8,
    pub hint_len: usize,
}

#[repr(C)]
pub union WriteModelUnionFFI {
    pub insert_one: InsertOneModelFFI,
    pub update_one: UpdateOneModelFFI,
    pub update_many: UpdateOneModelFFI,
    pub replace_one: UpdateOneModelFFI,
    pub delete_one: DeleteOneModelFFI,
    pub delete_many: DeleteOneModelFFI,
}

#[repr(C)]
pub struct WriteModelFFI {
    pub model_type: u8,
    pub namespace: CollectionHandle,      // For client bulk write
    pub model: WriteModelUnionFFI,
}

#[repr(C)]
pub struct BulkWriteOptionsFFI {
    pub ordered: i8,
    pub bypass_document_validation: i8,
    pub comment: *const u8,
    pub comment_len: usize,
    pub let_vars: *const u8,
    pub let_vars_len: usize,
}

/// Collection-level bulk write
#[no_mangle]
pub extern "C" fn ffi_bulk_write(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    models: *const WriteModelFFI,
    models_len: usize,
    options: *const BulkWriteOptionsFFI,
    callback: BulkWriteCallback,
    userdata: *mut c_void,
) { ... }

/// Client-level bulk write (MongoDB 8.0+)
#[no_mangle]
pub extern "C" fn ffi_client_bulk_write(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    models: *const NamespacedWriteModelFFI,  // Each model includes db_name + coll_name
    models_len: usize,
    options: *const BulkWriteOptionsFFI,
    callback: BulkWriteCallback,
    userdata: *mut c_void,
) { ... }
```

---

## Phase 6: Index & Collection Management

**Complexity**: Medium

### 6.1 Create `driver/src/ffi/operations/indexes.rs`

```rust
#[repr(C)]
pub struct IndexModelFFI {
    pub keys: *const u8,                  // Raw BSON document
    pub keys_len: usize,
    pub name: *const c_char,              // nullable
    pub unique: i8,
    pub sparse: i8,
    pub background: i8,
    pub expire_after_seconds: i64,        // -1 = not set
    pub partial_filter_expression: *const u8,
    pub partial_filter_expression_len: usize,
    pub collation: *const u8,
    pub collation_len: usize,
    pub hidden: i8,
}

pub type CreateIndexesCallback = extern "C" fn(
    userdata: *mut c_void,
    index_names: *const *const c_char,    // Array of index names
    index_names_len: usize,
    error: *const ErrorFFI,
);

#[no_mangle]
pub extern "C" fn ffi_create_indexes(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    indexes: *const IndexModelFFI,
    indexes_len: usize,
    options: *const CreateIndexOptionsFFI,
    callback: CreateIndexesCallback,
    userdata: *mut c_void,
) { ... }

#[no_mangle]
pub extern "C" fn ffi_drop_index(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    index_name: *const c_char,
    options: *const DropIndexOptionsFFI,
    callback: VoidCallback,
    userdata: *mut c_void,
) { ... }

#[no_mangle]
pub extern "C" fn ffi_list_indexes(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    callback: FindCallback,               // Returns cursor
    userdata: *mut c_void,
) { ... }
```

### 6.2 Create `driver/src/ffi/operations/collection_mgmt.rs`

```rust
#[no_mangle]
pub extern "C" fn ffi_create_collection(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    options: *const CreateCollectionOptionsFFI,
    callback: VoidCallback,
    userdata: *mut c_void,
) { ... }

#[no_mangle]
pub extern "C" fn ffi_drop_collection(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    options: *const DropCollectionOptionsFFI,
    callback: VoidCallback,
    userdata: *mut c_void,
) { ... }

#[no_mangle]
pub extern "C" fn ffi_rename_collection(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    new_coll_name: *const c_char,
    drop_target: bool,
    callback: VoidCallback,
    userdata: *mut c_void,
) { ... }

#[no_mangle]
pub extern "C" fn ffi_list_collections(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    filter: *const u8,                    // nullable
    filter_len: usize,
    callback: FindCallback,               // Returns cursor
    userdata: *mut c_void,
) { ... }
```

### 6.3 Create `driver/src/ffi/operations/database_mgmt.rs`

```rust
#[no_mangle]
pub extern "C" fn ffi_drop_database(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    options: *const DropDatabaseOptionsFFI,
    callback: VoidCallback,
    userdata: *mut c_void,
) { ... }

#[no_mangle]
pub extern "C" fn ffi_list_databases(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    filter: *const u8,                    // nullable
    filter_len: usize,
    callback: ListDatabasesCallback,
    userdata: *mut c_void,
) { ... }

#[no_mangle]
pub extern "C" fn ffi_run_command(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    db_name: *const c_char,
    command: *const u8,
    command_len: usize,
    callback: SingleDocCallback,
    userdata: *mut c_void,
) { ... }
```

---

## Phase 7: Change Streams

**Complexity**: Medium

Note: Transaction operations (`start_transaction`, `commit_transaction`, `abort_transaction`)
are implemented in Phase 1 as **session methods** in `driver/src/ffi/session.rs`, not as
standalone operations. This keeps transaction state opaque within the session.

### 7.1 Create `driver/src/ffi/operations/watch.rs`

```rust
pub const FULL_DOCUMENT_DEFAULT: u8 = 0;
pub const FULL_DOCUMENT_UPDATE_LOOKUP: u8 = 1;
pub const FULL_DOCUMENT_WHEN_AVAILABLE: u8 = 2;
pub const FULL_DOCUMENT_REQUIRED: u8 = 3;

pub const FULL_DOCUMENT_BEFORE_CHANGE_OFF: u8 = 0;
pub const FULL_DOCUMENT_BEFORE_CHANGE_WHEN_AVAILABLE: u8 = 1;
pub const FULL_DOCUMENT_BEFORE_CHANGE_REQUIRED: u8 = 2;

#[repr(C)]
pub struct ChangeStreamOptionsFFI {
    pub full_document: u8,
    pub full_document_before_change: u8,
    pub resume_after: *const u8,          // Raw BSON document, nullable
    pub resume_after_len: usize,
    pub start_after: *const u8,
    pub start_after_len: usize,
    pub start_at_operation_time: i8,      // -1=not set, otherwise use timestamps below
    pub start_at_time_seconds: u32,
    pub start_at_time_increment: u32,
    pub batch_size: i32,
    pub max_await_time_ms: i64,
    pub collation: *const u8,
    pub collation_len: usize,
    pub comment: *const u8,
    pub comment_len: usize,
}

#[no_mangle]
pub extern "C" fn ffi_watch(
    client: *mut MongoClient,
    ctx: *const OperationContext,
    target_type: u8,                      // 0=client, 1=database, 2=collection
    db_name: *const c_char,               // nullable for client-level
    coll_name: *const c_char,             // nullable for db/client-level
    pipeline: *const u8,
    pipeline_len: usize,
    options: *const ChangeStreamOptionsFFI,
    userdata: *mut c_void,
    callback: ChangeStreamCallback,
) { ... }

pub type ChangeStreamCallback = extern "C" fn(
    userdata: *mut c_void,
    cursor_handle: u64,                   // Change stream uses cursor API
    error: *const ErrorFFI,
);
```

### 7.2 Transaction Methods (Already in Phase 1)

Transaction methods are part of the session API in `driver/src/ffi/session.rs`:

```rust
// These are implemented in Phase 1 as session methods

pub type TransactionCallback = extern "C" fn(
    userdata: *mut c_void,
    error: *const ErrorFFI,
);

#[no_mangle]
pub extern "C" fn mongo_session_start_transaction(
    client: *mut MongoClient,
    session_handle: u64,
    options: *const TransactionOptionsFFI,
    callback: TransactionCallback,
    userdata: *mut c_void,
) { ... }

#[no_mangle]
pub extern "C" fn mongo_session_commit_transaction(
    client: *mut MongoClient,
    session_handle: u64,
    callback: TransactionCallback,
    userdata: *mut c_void,
) { ... }

#[no_mangle]
pub extern "C" fn mongo_session_abort_transaction(
    client: *mut MongoClient,
    session_handle: u64,
    callback: TransactionCallback,
    userdata: *mut c_void,
) { ... }
```

The transaction methods work by:
1. Locking the session (`Arc<Mutex<ClientSession>>`)
2. Calling the real `ClientSession::start_transaction()`, `commit_transaction()`, or `abort_transaction()`
3. The driver handles all transaction state, retry logic, and error labels automatically

---

## File Changes Summary

### New Files
| File | Phase | Description |
|------|-------|-------------|
| `driver/src/ffi/handles.rs` | 1 | Generic handle pool infrastructure |
| `driver/src/ffi/session.rs` | 1 | Session pool with real ClientSession + transaction methods |
| `driver/src/ffi/concerns.rs` | 1 | Read preference, write concern, read concern handle pools |
| `driver/src/ffi/error.rs` | 1 | Error types (ServerErrorFFI, etc.) |
| `driver/src/ffi/results.rs` | 1 | Result types (InsertOneResultFFI, etc.) |
| `driver/src/ffi/callbacks.rs` | 2 | Callback type definitions |
| `driver/src/ffi/operations/mod.rs` | 2 | Operations module |
| `driver/src/ffi/operations/insert.rs` | 2 | insert_one, insert_many |
| `driver/src/ffi/operations/update.rs` | 2 | update_one, update_many, replace_one |
| `driver/src/ffi/operations/delete.rs` | 2 | delete_one, delete_many |
| `driver/src/ffi/operations/find.rs` | 2 | find, find_one |
| `driver/src/ffi/operations/aggregate.rs` | 3 | aggregate |
| `driver/src/ffi/operations/count.rs` | 3 | count_documents, estimated_document_count |
| `driver/src/ffi/operations/distinct.rs` | 3 | distinct |
| `driver/src/ffi/operations/cursor.rs` | 3 | cursor_next, cursor_close |
| `driver/src/ffi/operations/find_and_modify.rs` | 4 | find_one_and_update, etc. |
| `driver/src/ffi/operations/bulk_write.rs` | 5 | bulk_write, client_bulk_write |
| `driver/src/ffi/operations/indexes.rs` | 6 | create_indexes, drop_index, list_indexes |
| `driver/src/ffi/operations/collection_mgmt.rs` | 6 | create/drop/rename/list collections |
| `driver/src/ffi/operations/database_mgmt.rs` | 6 | drop_database, list_databases, run_command |
| `driver/src/ffi/operations/watch.rs` | 7 | Change streams |

### Modified Files (FFI Layer)
| File | Phase | Changes |
|------|-------|---------|
| `driver/src/ffi/client.rs` | 1 | Add handle pools (session, read_pref, write_concern, read_concern) |
| `driver/src/ffi/types.rs` | 1 | Rewrite OperationContext to use handles |
| `driver/src/ffi/mod.rs` | 1 | Add module exports |
| `driver/src/ffi/api.rs` | 2+ | Keep existing API, add deprecation comments |

### Modified Files (Driver Core - Phase 0)
| File | Changes |
|------|---------|
| `driver/src/operation/find.rs` | Add `FilterDoc` enum, `new_raw()` constructor |
| `driver/src/operation/delete.rs` | Add `FilterDoc`, `new_raw()` |
| `driver/src/operation/update.rs` | Add `FilterDoc`, `with_update_raw()` |
| `driver/src/operation/aggregate.rs` | Add `Pipeline` enum, `new_raw()` |
| `driver/src/operation/find_and_modify.rs` | Add `FilterDoc` for query |
| `driver/src/operation/count_documents.rs` | Add `new_raw()` with raw filter |
| `driver/src/operation/distinct.rs` | Add `FilterDoc` for filter |
| `driver/src/operation/mod.rs` | Add `FilterDoc`, `Pipeline`, `DocOrRaw` types |
| `driver/src/client/options/bulk_write.rs` | Add `DocOrRaw` to WriteModel variants |
| `driver/src/db/options.rs` | Add `DocOrRaw` for ListCollections filter |
| `driver/src/index.rs` | Add `DocOrRaw` for IndexModel keys |

### Unchanged
The opaque session design with real `ClientSession` objects means:
- No changes to `driver/src/client/executor.rs`
- No changes to `driver/src/client/session.rs`

---

## Testing Strategy

### Unit Tests
- Test handle pool operations (insert, get, remove)
- Test all FFI type conversions (ErrorFFI, ResultFFI types)
- Test option parsing from FFI structs
- Memory safety with ASAN/Valgrind

### Integration Tests
- Test session start/end with real ClientSession
- Test transaction flow (start → operations → commit/abort)
- Test each operation against a real MongoDB instance
- Test error handling and propagation

### Property-Based Tests
- Use proptest to generate random inputs
- Verify no panics across FFI boundary
- Verify memory is properly freed

### Stress Tests
- Concurrent operation execution with shared sessions
- Memory leak detection over long runs
- Callback ordering guarantees

---

## Migration & Compatibility

### Backward Compatibility
- **Existing API preserved**: `mongo_execute_command`, `mongo_execute_cursor_command`, `mongo_cursor_get_more`, `mongo_cursor_close` remain unchanged
- **Gradual adoption**: Language drivers can migrate one operation at a time
- **Feature flag**: Consider `ffi-typed-ops` feature flag during development

### Migration Path for Language Drivers
1. **Phase 1**: Adopt new session API with transaction support, create handle pools
2. **Phase 2-3**: Migrate CRUD operations one at a time, test each
3. **Phase 4-6**: Migrate remaining operations
4. **Phase 7**: Migrate change streams

### Deprecation Timeline
- Phase 1-3: New API coexists with old
- Phase 4+: Mark `mongo_execute_command` as deprecated for operations with typed equivalents
- Future: Remove deprecated APIs in major version bump

---

## Estimated Effort Summary

With AI-assisted development, each phase can typically be completed in a single working session.

| Phase | Description | Dependencies |
|-------|-------------|--------------|
| 0 | Raw Document Support in Operations | None |
| 1 | Handle Pools, Sessions, Concerns, Error/Result Types | None |
| 2 | Core CRUD Operations | Phase 0, Phase 1 |
| 3 | Aggregate, Count, Distinct, Cursor | Phase 0, Phase 2 |
| 4 | Find-and-Modify Operations | Phase 2 |
| 5 | Bulk Write Operations | Phase 2 |
| 6 | Index & Collection Management | Phase 2 |
| 7 | Change Streams | Phase 3 |

Phase 0 and Phase 1 can be done in parallel. Phases 2-6 can be parallelized after both are complete.

---

## Rollback Plan

Each phase is independently deployable. To rollback:

1. **Feature flag**: Disable `ffi-typed-ops` feature
2. **Version pin**: Language drivers pin to previous version
3. **API revert**: Remove new exports from `mod.rs`, keep existing code

No data migration required - this is purely API changes.

---

## Complexity Assessment

**Overall Complexity**: **Medium-High**

Key challenges:
1. **Memory management across FFI boundary** - Must carefully track ownership
2. **Error conversion** - MongoDB errors have complex structure
3. **Bulk write models** - Tagged unions require careful handling
4. **Change stream resumability** - Needs careful cursor management
5. **Session locking** - Must handle concurrent access to sessions

Mitigations:
1. Comprehensive unit tests for memory management
2. Clear ownership documentation
3. Helper functions for tagged union construction
4. Reuse existing cursor infrastructure
5. Use `Arc<Mutex<ClientSession>>` for safe concurrent session access
6. **No driver core changes** - Reduces risk significantly

