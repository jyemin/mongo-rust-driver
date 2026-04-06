//! Shared aggregate operation logic.

use std::ffi::c_char;

use crate::{
    bson::Document,
    coll::Collection,
    db::Database,
    error::Result,
    ffi::{
        types::{Bson, BsonArray, BsonValue, ContextExt, OperationContext},
        utils::{c_char_to_str, c_char_to_string, i64_to_duration_ms, i8_to_option_bool},
    },
    options::{Hint, SelectionCriteria},
};

/// FFI-compatible aggregate options.
///
/// Use -1 for "not set" on integer options, null for pointer options.
/// For tri-state booleans (i8): -1 = not set, 0 = false, 1 = true.
#[repr(C)]
pub struct AggregateOptions {
    /// Allow disk use for sorting large result sets. Tri-state: -1 = not set, 0 = false, 1 = true.
    pub allow_disk_use: i8,
    /// Number of documents per batch. -1 = not set.
    pub batch_size: i32,
    /// Opt out of document-level validation. Tri-state.
    pub bypass_document_validation: i8,
    /// Collation options. Nullable BSON document.
    pub collation: *const Bson,
    /// Comment as a BSON value. Nullable.
    pub comment: *const BsonValue,
    /// Index name hint. Nullable, takes precedence over hint_keys if set.
    pub hint_name: *const c_char,
    /// Index keys hint as BSON document. Nullable.
    pub hint_keys: *const Bson,
    /// Maximum query execution time in milliseconds. -1 = not set.
    pub max_time_ms: i64,
    /// Variables for use in aggregation expressions. Nullable BSON document.
    pub let_vars: *const Bson,
}

// --- Helper functions (moved from ffi/aggregate.rs) ---

/// Build a pipeline (Vec<Document>) from a BsonArray of BSON documents.
pub(crate) unsafe fn pipeline_from_array(
    pipeline: BsonArray,
) -> Result<Vec<Document>> {
    if pipeline.data.is_null() || pipeline.len == 0 {
        return Ok(vec![]);
    }
    let ptrs = std::slice::from_raw_parts(pipeline.data, pipeline.len);
    let mut docs = Vec::with_capacity(pipeline.len);
    for &ptr in ptrs {
        // Each pointer is a BSON document; read the 4-byte length prefix to get total size
        let len_slice = std::slice::from_raw_parts(ptr, 4);
        let len = i32::from_le_bytes(len_slice.try_into().unwrap()) as usize;
        let doc_bytes = std::slice::from_raw_parts(ptr, len);
        let raw_doc = crate::bson::RawDocument::from_bytes(doc_bytes)?;
        let doc: Document = raw_doc.try_into()?;
        docs.push(doc);
    }
    Ok(docs)
}

/// Parse FFI AggregateOptions into driver AggregateOptions.
pub(crate) unsafe fn parse_aggregate_options(
    opts: *const AggregateOptions,
    ctx: *const OperationContext,
) -> Result<crate::coll::options::AggregateOptions> {
    let mut options = crate::coll::options::AggregateOptions::default();

    // Always set context-derived options
    options.read_concern = ctx.read_concern();
    options.write_concern = ctx.write_concern();
    options.selection_criteria = ctx.read_preference().map(SelectionCriteria::ReadPreference);

    if opts.is_null() {
        return Ok(options);
    }

    let opts = &*opts;

    options.allow_disk_use = i8_to_option_bool(opts.allow_disk_use);
    options.batch_size = if opts.batch_size >= 0 {
        Some(opts.batch_size as u32)
    } else {
        None
    };
    options.bypass_document_validation = i8_to_option_bool(opts.bypass_document_validation);

    // Parse collation from BSON document
    if let Some(doc) = Bson::to_doc(opts.collation)? {
        options.collation = Some(crate::bson_compat::deserialize_from_document(doc)?);
    }

    // Parse comment (BsonValue)
    if !opts.comment.is_null() {
        options.comment = (&*opts.comment).to_bson()?;
    }

    // Parse hint (name takes precedence over keys)
    options.hint = if let Some(name) = c_char_to_string(opts.hint_name)? {
        Some(Hint::Name(name))
    } else if let Some(keys) = Bson::to_doc(opts.hint_keys)? {
        Some(Hint::Keys(keys))
    } else {
        None
    };

    options.max_time = i64_to_duration_ms(opts.max_time_ms);
    options.let_vars = Bson::to_doc(opts.let_vars)?;

    Ok(options)
}

// --- Prepare functions ---

pub(crate) unsafe fn prepare_aggregate_collection(
    client: &crate::Client,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    pipeline: BsonArray,
    opts: *const AggregateOptions,
) -> Result<(
    Collection<Document>,
    Vec<Document>,
    crate::coll::options::AggregateOptions,
)> {
    use crate::error::Error;

    let db_name_str = c_char_to_str(db_name)?
        .ok_or_else(|| Error::invalid_argument("db_name cannot be null"))?;
    let coll_name_str = c_char_to_str(coll_name)?
        .ok_or_else(|| Error::invalid_argument("coll_name cannot be null"))?;
    let coll = client
        .database(db_name_str)
        .collection::<Document>(coll_name_str);

    let pipeline_docs = pipeline_from_array(pipeline)?;
    let options = parse_aggregate_options(opts, ctx)?;

    Ok((coll, pipeline_docs, options))
}

pub(crate) unsafe fn prepare_aggregate_database(
    client: &crate::Client,
    ctx: *const OperationContext,
    db_name: *const c_char,
    pipeline: BsonArray,
    opts: *const AggregateOptions,
) -> Result<(Database, Vec<Document>, crate::coll::options::AggregateOptions)> {
    use crate::error::Error;

    let db_name_str = c_char_to_str(db_name)?
        .ok_or_else(|| Error::invalid_argument("db_name cannot be null"))?;
    let db = client.database(db_name_str);

    let pipeline_docs = pipeline_from_array(pipeline)?;
    let options = parse_aggregate_options(opts, ctx)?;

    Ok((db, pipeline_docs, options))
}
