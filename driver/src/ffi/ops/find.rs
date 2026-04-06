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

// --- Option parsing (moved from ffi/find.rs) ---

/// Parse FFI FindOptions into driver FindOptions.
pub(crate) unsafe fn parse_find_options(
    opts: *const crate::ffi::find::FindOptions,
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
    opts: *const crate::ffi::find::FindOptions,
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
