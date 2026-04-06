//! Shared find operation logic.

use std::ffi::c_char;

use crate::{
    bson::Document,
    coll::Collection,
    error::Result,
    ffi::{
        types::{Bson, ContextExt, OperationContext},
        utils::{c_char_to_str, c_char_to_string, i64_to_duration_ms, i8_to_option_bool},
    },
    options::{CursorType, Hint, SelectionCriteria},
};

/// FFI-compatible find options.
///
/// Use -1 for "not set" on integer options, null for pointer options.
/// For tri-state booleans (i8): -1 = not set, 0 = false, 1 = true.
#[repr(C)]
pub struct FindOptions {
    /// Allow disk use for sorting large result sets. Tri-state: -1 = not set, 0 = false, 1 = true.
    pub allow_disk_use: i8,
    /// Allow partial results from mongos if some shards are down. Tri-state.
    pub allow_partial_results: i8,
    /// Number of documents per batch. -1 = not set.
    pub batch_size: i32,
    /// Comment to attach to the query. Nullable BSON value wrapped in doc with empty key.
    pub comment: *const Bson,
    /// Cursor type: -1 = not set, 0 = NonTailable, 1 = Tailable, 2 = TailableAwait.
    pub cursor_type: i8,
    /// Index name hint. Nullable, takes precedence over hint_keys if set.
    pub hint_name: *const c_char,
    /// Index keys hint as BSON document. Nullable.
    pub hint_keys: *const Bson,
    /// Maximum number of documents to return. 0 = not set.
    pub limit: i64,
    /// Exclusive upper bound for a specific index. Nullable BSON document.
    pub max: *const Bson,
    /// Max time for tailable cursor to wait for new documents. -1 = not set.
    pub max_await_time_ms: i64,
    /// Maximum query execution time in milliseconds. -1 = not set.
    pub max_time_ms: i64,
    /// Inclusive lower bound for a specific index. Nullable BSON document.
    pub min: *const Bson,
    /// Prevent cursor timeout after inactivity. Tri-state.
    pub no_cursor_timeout: i8,
    /// Projection document. Nullable BSON document.
    pub projection: *const Bson,
    /// Return only index keys, not full documents. Tri-state.
    pub return_key: i8,
    /// Include record identifier in results. Tri-state.
    pub show_record_id: i8,
    /// Number of documents to skip. -1 = not set.
    pub skip: i64,
    /// Sort specification. Nullable BSON document.
    pub sort: *const Bson,
    /// Collation options. Nullable BSON document (deserialized as Collation).
    pub collation: *const Bson,
    /// Variables for use in aggregation expressions. Nullable BSON document.
    pub let_vars: *const Bson,
}

// --- Option parsing (moved from ffi/find.rs) ---

/// Parse FFI FindOptions into driver FindOptions.
pub(crate) unsafe fn parse_find_options(
    opts: *const FindOptions,
    ctx: *const OperationContext,
) -> Result<crate::options::FindOptions> {
    let mut options = crate::options::FindOptions::default();

    // Always set context-derived options
    options.read_concern = ctx.read_concern();
    options.selection_criteria = ctx.read_preference().map(SelectionCriteria::ReadPreference);

    if opts.is_null() {
        return Ok(options);
    }

    let opts = &*opts;

    options.allow_disk_use = i8_to_option_bool(opts.allow_disk_use);
    options.allow_partial_results = i8_to_option_bool(opts.allow_partial_results);
    options.batch_size = if opts.batch_size >= 0 {
        Some(opts.batch_size as u32)
    } else {
        None
    };

    // Parse comment (BSON value wrapped in doc with empty key)
    if !opts.comment.is_null() {
        let comment_bson = (*opts.comment).as_raw_doc()?;
        let comment_doc: Document = comment_bson.try_into()?;
        options.comment = comment_doc.get("").map(|v| v.clone());
    }

    // Parse cursor_type
    options.cursor_type = match opts.cursor_type {
        0 => Some(CursorType::NonTailable),
        1 => Some(CursorType::Tailable),
        2 => Some(CursorType::TailableAwait),
        _ => None,
    };

    // Parse hint (name takes precedence over keys)
    options.hint = if let Some(name) = c_char_to_string(opts.hint_name)? {
        Some(Hint::Name(name))
    } else if let Some(keys) = Bson::to_doc(opts.hint_keys)? {
        Some(Hint::Keys(keys))
    } else {
        None
    };

    options.limit = if opts.limit != 0 {
        Some(opts.limit)
    } else {
        None
    };
    options.max = Bson::to_doc(opts.max)?;
    options.max_await_time = i64_to_duration_ms(opts.max_await_time_ms);
    options.max_time = i64_to_duration_ms(opts.max_time_ms);
    options.min = Bson::to_doc(opts.min)?;
    options.no_cursor_timeout = i8_to_option_bool(opts.no_cursor_timeout);
    options.projection = Bson::to_doc(opts.projection)?;
    options.return_key = i8_to_option_bool(opts.return_key);
    options.show_record_id = i8_to_option_bool(opts.show_record_id);
    options.skip = if opts.skip >= 0 {
        Some(opts.skip as u64)
    } else {
        None
    };
    options.sort = Bson::to_doc(opts.sort)?;

    // Parse collation from BSON document
    if let Some(doc) = Bson::to_doc(opts.collation)? {
        options.collation = Some(crate::bson_compat::deserialize_from_document(doc)?);
    }

    options.let_vars = Bson::to_doc(opts.let_vars)?;

    Ok(options)
}

// --- Prepare function ---

pub(crate) unsafe fn prepare_find(
    client: &crate::Client,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    filter: *const Bson,
    opts: *const FindOptions,
) -> Result<(Collection<Document>, Document, crate::options::FindOptions)> {
    use crate::error::Error;

    let db = c_char_to_str(db_name)?
        .ok_or_else(|| Error::invalid_argument("db_name cannot be null"))?;
    let coll_name_str = c_char_to_str(coll_name)?
        .ok_or_else(|| Error::invalid_argument("coll_name cannot be null"))?;
    let coll = client.database(db).collection::<Document>(coll_name_str);

    let filter_doc: Document = if filter.is_null() {
        crate::bson::doc! {}
    } else {
        (&*filter).as_raw_doc()?.try_into()?
    };

    let options = parse_find_options(opts, ctx)?;

    Ok((coll, filter_doc, options))
}
