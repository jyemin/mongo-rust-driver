//! `MongoFuture` — an opaque handle wrapping a tokio `JoinHandle`.
//!
//! The future never drives the runtime itself. The caller must tick the owning
//! client's runtime to make progress. Typed getters extract the result once the
//! future is finished.

use std::ptr;

use tokio::task::JoinHandle;

use crate::ffi::{
    error::Error,
    ops::{
        delete::DeleteResult,
        distinct::DistinctResult,
        insert::{InsertManyResult, InsertOneResult, InsertedId},
        update::UpdateResult,
    },
    types::{BsonArray, OwnedBson, OwnedBsonValue},
};

use super::cursor::CursorResult;

/// The resolved value of a future.
pub(super) enum FutureValue {
    Void,
    Count(u64),
    InsertOne(InsertOneResult),
    InsertMany(Vec<InsertedId>, InsertManyResult),
    Update(UpdateResult),
    Delete(DeleteResult),
    Document(Option<OwnedBson>),
    Distinct(Vec<OwnedBsonValue>, DistinctResult),
    Cursor {
        _raw_batch: Option<crate::raw_batch_cursor::RawBatch>,
        _doc_ptrs: Option<Vec<*const u8>>,
        result: CursorResult,
    },
    GetMore {
        exhausted: bool,
        _raw_batch: Option<crate::raw_batch_cursor::RawBatch>,
        _doc_ptrs: Option<Vec<*const u8>>,
        data: BsonArray,
    },
}

// Safety: FutureValue contains FFI types with raw pointers (e.g. InsertManyResult,
// DistinctResult) that are not automatically Send. These pointers are only accessed
// from the single FFI thread that owns the client and its runtime.
unsafe impl Send for FutureValue {}

/// Opaque future handle returned by async FFI operations.
///
/// The caller must tick the owning client's runtime via `mongoc_future_client_tick()`
/// to make progress. Use `mongoc_future_is_finished()` to poll for completion,
/// then call the appropriate typed getter to extract the result.
///
/// The future (and any cached results) remain valid until `mongoc_future_destroy()`
/// is called.
pub struct MongoFuture {
    /// Pointer to the runtime, used only for resolving completed handles.
    /// The runtime is owned by the client and outlives this future.
    runtime: *const tokio::runtime::Runtime,
    /// The pending join handle, consumed when the result is resolved.
    handle: Option<JoinHandle<Result<FutureValue, crate::error::Error>>>,
    /// Cached result, set once the handle completes.
    result: Option<Result<FutureValue, crate::error::Error>>,
}

// Safety: MongoFuture is only accessed from the FFI thread that owns the client.
// The runtime pointer is valid for the lifetime of the client.
unsafe impl Send for MongoFuture {}

impl MongoFuture {
    /// Create a future from a `JoinHandle` and a reference to the runtime.
    ///
    /// The runtime reference must remain valid for the lifetime of this future.
    pub(super) fn from_join_handle(
        runtime: &tokio::runtime::Runtime,
        handle: JoinHandle<Result<FutureValue, crate::error::Error>>,
    ) -> *mut Self {
        Box::into_raw(Box::new(Self {
            runtime: runtime as *const _,
            handle: Some(handle),
            result: None,
        }))
    }

    /// Create a future that is immediately resolved with an error.
    pub(super) fn from_error(e: crate::error::Error) -> *mut Self {
        Box::into_raw(Box::new(Self {
            runtime: ptr::null(),
            handle: None,
            result: Some(Err(e)),
        }))
    }

    /// Create a future that is immediately resolved with a value.
    pub(super) fn from_value(v: FutureValue) -> *mut Self {
        Box::into_raw(Box::new(Self {
            runtime: ptr::null(),
            handle: None,
            result: Some(Ok(v)),
        }))
    }

    /// Try to resolve the handle if it is finished. If the task has completed,
    /// the result is cached and subsequent calls are no-ops.
    fn try_resolve(&mut self) {
        if self.result.is_some() {
            return;
        }
        if let Some(ref handle) = self.handle {
            if !handle.is_finished() {
                return;
            }
        }
        // The handle is finished — extract the result.
        if let Some(handle) = self.handle.take() {
            // Safety: we checked is_finished() above, so block_on will return
            // immediately. The runtime pointer is valid because the client
            // (which owns the runtime) must outlive this future.
            let runtime = unsafe { &*self.runtime };
            self.result = Some(match runtime.block_on(handle) {
                Ok(r) => r,
                Err(join_err) => Err(crate::error::Error::invalid_argument(format!(
                    "task panicked: {}",
                    join_err
                ))),
            });
        }
    }

    /// Write an error to the out-pointer if the result is `Err`.
    /// Returns `true` if the result is `Ok`, `false` if `Err`.
    fn write_error(&self, error_out: *mut *mut Error) -> bool {
        match &self.result {
            Some(Ok(_)) => true,
            Some(Err(e)) => {
                if !error_out.is_null() {
                    unsafe {
                        *error_out = Box::into_raw(Box::new(Error::from(e)));
                    }
                }
                false
            }
            None => {
                // Not yet resolved — caller must tick and wait for is_finished
                if !error_out.is_null() {
                    let e = crate::error::Error::invalid_argument(
                        "future is not yet finished; call tick() and is_finished() first",
                    );
                    unsafe {
                        *error_out = Box::into_raw(Box::new(Error::from(&e)));
                    }
                }
                false
            }
        }
    }
}

// ---------------------------------------------------------------------------
// FFI functions
// ---------------------------------------------------------------------------

/// Check whether the future has completed.
///
/// Returns `true` if the result is available (either success or error).
/// The caller should call `mongoc_future_client_tick()` to drive progress
/// before checking this.
///
/// # Safety
///
/// `future` must be a valid pointer returned by an FFI operation.
#[no_mangle]
pub unsafe extern "C" fn mongoc_future_is_finished(future: *const MongoFuture) -> bool {
    if future.is_null() {
        return true;
    }
    // Cast away const — try_resolve is an internal cache mutation, not
    // externally observable state change.
    let future = &mut *(future as *mut MongoFuture);
    future.try_resolve();
    future.result.is_some()
}

/// Free a future and any cached results.
///
/// # Safety
///
/// `future` must be a valid pointer returned by an FFI operation, or null.
/// After this call, the pointer is invalid.
#[no_mangle]
pub unsafe extern "C" fn mongoc_future_destroy(future: *mut MongoFuture) {
    if !future.is_null() {
        drop(Box::from_raw(future));
    }
}

/// Get the result of a void future (e.g. drop, shutdown).
///
/// Returns `true` on success, `false` on error. If `false` and `error_out`
/// is non-null, `*error_out` is set to a heap-allocated `Error` that must
/// be freed with `error_free()`.
///
/// The future must be finished (`mongoc_future_is_finished` returns true).
///
/// # Safety
///
/// - `future` must be a valid pointer returned by an FFI operation.
/// - `error_out` may be null or a valid pointer.
#[no_mangle]
pub unsafe extern "C" fn mongoc_future_get_void(
    future: *mut MongoFuture,
    error_out: *mut *mut Error,
) -> bool {
    if future.is_null() {
        return true;
    }
    let future = &mut *future;
    future.try_resolve();
    future.write_error(error_out)
}

/// Get the count result from a future.
///
/// On success, writes the count to `*count_out` and returns `true`.
/// On error, returns `false` and optionally writes to `*error_out`.
///
/// # Safety
///
/// - `future` must be a valid pointer.
/// - `count_out` must be a valid pointer to a `u64`.
/// - `error_out` may be null or a valid pointer.
#[no_mangle]
pub unsafe extern "C" fn mongoc_future_get_count(
    future: *mut MongoFuture,
    count_out: *mut u64,
    error_out: *mut *mut Error,
) -> bool {
    if future.is_null() {
        return false;
    }
    let future = &mut *future;
    future.try_resolve();
    if !future.write_error(error_out) {
        return false;
    }
    if let Some(Ok(FutureValue::Count(count))) = &future.result {
        if !count_out.is_null() {
            *count_out = *count;
        }
        true
    } else {
        write_type_mismatch_error(error_out, "count");
        false
    }
}

/// Get the insert_one result from a future.
///
/// On success, writes a pointer to the cached `InsertOneResult` to `*result_out`
/// and returns `true`. The pointer is valid until the future is destroyed.
///
/// # Safety
///
/// - `future` must be a valid pointer.
/// - `result_out` must be a valid pointer.
/// - `error_out` may be null or a valid pointer.
#[no_mangle]
pub unsafe extern "C" fn mongoc_future_get_insert_one(
    future: *mut MongoFuture,
    result_out: *mut *const InsertOneResult,
    error_out: *mut *mut Error,
) -> bool {
    if future.is_null() {
        return false;
    }
    let future = &mut *future;
    future.try_resolve();
    if !future.write_error(error_out) {
        return false;
    }
    if let Some(Ok(FutureValue::InsertOne(ref r))) = future.result {
        if !result_out.is_null() {
            *result_out = r as *const InsertOneResult;
        }
        true
    } else {
        write_type_mismatch_error(error_out, "insert_one");
        false
    }
}

/// Get the insert_many result from a future.
///
/// On success, writes a pointer to the cached `InsertManyResult` to `*result_out`
/// and returns `true`. The pointer is valid until the future is destroyed.
///
/// # Safety
///
/// - `future` must be a valid pointer.
/// - `result_out` must be a valid pointer.
/// - `error_out` may be null or a valid pointer.
#[no_mangle]
pub unsafe extern "C" fn mongoc_future_get_insert_many(
    future: *mut MongoFuture,
    result_out: *mut *const InsertManyResult,
    error_out: *mut *mut Error,
) -> bool {
    if future.is_null() {
        return false;
    }
    let future = &mut *future;
    future.try_resolve();
    if !future.write_error(error_out) {
        return false;
    }
    if let Some(Ok(FutureValue::InsertMany(_, ref r))) = future.result {
        if !result_out.is_null() {
            *result_out = r as *const InsertManyResult;
        }
        true
    } else {
        write_type_mismatch_error(error_out, "insert_many");
        false
    }
}

/// Get the update/replace result from a future.
///
/// On success, writes a pointer to the cached `UpdateResult` to `*result_out`
/// and returns `true`. The pointer is valid until the future is destroyed.
///
/// # Safety
///
/// - `future` must be a valid pointer.
/// - `result_out` must be a valid pointer.
/// - `error_out` may be null or a valid pointer.
#[no_mangle]
pub unsafe extern "C" fn mongoc_future_get_update(
    future: *mut MongoFuture,
    result_out: *mut *const UpdateResult,
    error_out: *mut *mut Error,
) -> bool {
    if future.is_null() {
        return false;
    }
    let future = &mut *future;
    future.try_resolve();
    if !future.write_error(error_out) {
        return false;
    }
    if let Some(Ok(FutureValue::Update(ref r))) = future.result {
        if !result_out.is_null() {
            *result_out = r as *const UpdateResult;
        }
        true
    } else {
        write_type_mismatch_error(error_out, "update");
        false
    }
}

/// Get the delete result from a future.
///
/// On success, writes a pointer to the cached `DeleteResult` to `*result_out`
/// and returns `true`. The pointer is valid until the future is destroyed.
///
/// # Safety
///
/// - `future` must be a valid pointer.
/// - `result_out` must be a valid pointer.
/// - `error_out` may be null or a valid pointer.
#[no_mangle]
pub unsafe extern "C" fn mongoc_future_get_delete(
    future: *mut MongoFuture,
    result_out: *mut *const DeleteResult,
    error_out: *mut *mut Error,
) -> bool {
    if future.is_null() {
        return false;
    }
    let future = &mut *future;
    future.try_resolve();
    if !future.write_error(error_out) {
        return false;
    }
    if let Some(Ok(FutureValue::Delete(ref r))) = future.result {
        if !result_out.is_null() {
            *result_out = r as *const DeleteResult;
        }
        true
    } else {
        write_type_mismatch_error(error_out, "delete");
        false
    }
}

/// Get the document result from a future (e.g. find_one, run_command).
///
/// On success, writes a pointer to the cached `OwnedBson` to `*result_out`
/// and returns `true`. If the operation succeeded but no document matched,
/// `*result_out` is set to null. The pointer (if non-null) is valid until
/// the future is destroyed.
///
/// # Safety
///
/// - `future` must be a valid pointer.
/// - `result_out` must be a valid pointer.
/// - `error_out` may be null or a valid pointer.
#[no_mangle]
pub unsafe extern "C" fn mongoc_future_get_document(
    future: *mut MongoFuture,
    result_out: *mut *const OwnedBson,
    error_out: *mut *mut Error,
) -> bool {
    if future.is_null() {
        return false;
    }
    let future = &mut *future;
    future.try_resolve();
    if !future.write_error(error_out) {
        return false;
    }
    if let Some(Ok(FutureValue::Document(ref opt))) = future.result {
        if !result_out.is_null() {
            match opt {
                Some(ref b) => *result_out = b as *const OwnedBson,
                None => *result_out = ptr::null(),
            }
        }
        true
    } else {
        write_type_mismatch_error(error_out, "document");
        false
    }
}

/// Get the distinct result from a future.
///
/// On success, writes a pointer to the cached `DistinctResult` to `*result_out`
/// and returns `true`. The pointer is valid until the future is destroyed.
///
/// # Safety
///
/// - `future` must be a valid pointer.
/// - `result_out` must be a valid pointer.
/// - `error_out` may be null or a valid pointer.
#[no_mangle]
pub unsafe extern "C" fn mongoc_future_get_distinct(
    future: *mut MongoFuture,
    result_out: *mut *const DistinctResult,
    error_out: *mut *mut Error,
) -> bool {
    if future.is_null() {
        return false;
    }
    let future = &mut *future;
    future.try_resolve();
    if !future.write_error(error_out) {
        return false;
    }
    if let Some(Ok(FutureValue::Distinct(_, ref r))) = future.result {
        if !result_out.is_null() {
            *result_out = r as *const DistinctResult;
        }
        true
    } else {
        write_type_mismatch_error(error_out, "distinct");
        false
    }
}

/// Get the cursor result from a future (e.g. find, aggregate).
///
/// On success, writes a pointer to the cached `CursorResult` to `*result_out`
/// and returns `true`. The pointer is valid until the future is destroyed.
///
/// # Safety
///
/// - `future` must be a valid pointer.
/// - `result_out` must be a valid pointer.
/// - `error_out` may be null or a valid pointer.
#[no_mangle]
pub unsafe extern "C" fn mongoc_future_get_cursor(
    future: *mut MongoFuture,
    result_out: *mut *const CursorResult,
    error_out: *mut *mut Error,
) -> bool {
    if future.is_null() {
        return false;
    }
    let future = &mut *future;
    future.try_resolve();
    if !future.write_error(error_out) {
        return false;
    }
    if let Some(Ok(FutureValue::Cursor { ref result, .. })) = future.result {
        if !result_out.is_null() {
            *result_out = result as *const CursorResult;
        }
        true
    } else {
        write_type_mismatch_error(error_out, "cursor");
        false
    }
}

/// Get the get_more result from a future.
///
/// On success, writes the exhausted flag and batch data to the out-pointers
/// and returns `true`. The batch data is valid until the future is destroyed.
///
/// # Safety
///
/// - `future` must be a valid pointer.
/// - `exhausted_out` must be a valid pointer to a `bool`.
/// - `data_out` must be a valid pointer to a `BsonArray`.
/// - `error_out` may be null or a valid pointer.
#[no_mangle]
pub unsafe extern "C" fn mongoc_future_get_more(
    future: *mut MongoFuture,
    exhausted_out: *mut bool,
    data_out: *mut BsonArray,
    error_out: *mut *mut Error,
) -> bool {
    if future.is_null() {
        return false;
    }
    let future = &mut *future;
    future.try_resolve();
    if !future.write_error(error_out) {
        return false;
    }
    if let Some(Ok(FutureValue::GetMore {
        exhausted,
        ref data,
        ..
    })) = future.result
    {
        if !exhausted_out.is_null() {
            *exhausted_out = exhausted;
        }
        if !data_out.is_null() {
            *data_out = BsonArray {
                data: data.data,
                len: data.len,
            };
        }
        true
    } else {
        write_type_mismatch_error(error_out, "get_more");
        false
    }
}

/// Write a type-mismatch error to the out-pointer.
unsafe fn write_type_mismatch_error(error_out: *mut *mut Error, expected: &str) {
    if !error_out.is_null() {
        let e = crate::error::Error::invalid_argument(format!(
            "future does not contain a {} result",
            expected
        ));
        *error_out = Box::into_raw(Box::new(Error::from(&e)));
    }
}
