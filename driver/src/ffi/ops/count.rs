//! Shared count operation logic.

use std::ffi::c_char;

use crate::{
    bson::Document,
    coll::Collection,
    error::Result,
    ffi::{
        types::{Bson, BsonValue, ContextExt, OperationContext},
        utils::{c_char_to_str, c_char_to_string, i64_to_duration_ms},
    },
    ClientSession,
};

/// Options for `count_documents`.
///
/// All pointer fields are nullable (null = not set).
#[repr(C)]
pub struct CountOptions {
    /// Collation as a serialized BSON document. Nullable.
    pub collation: *const Bson,
    /// Index hint by name. Nullable. Takes precedence over `hint_keys`.
    pub hint_name: *const c_char,
    /// Index hint by key pattern as BSON document. Nullable.
    pub hint_keys: *const Bson,
    /// Maximum number of documents to count. -1 = not set.
    pub limit: i64,
    /// Number of documents to skip. -1 = not set.
    pub skip: i64,
    /// Maximum time in milliseconds. -1 = not set.
    pub max_time_ms: i64,
    /// Comment BSON value. Nullable.
    pub comment: *const BsonValue,
}

/// Options for `estimated_document_count`.
///
/// All pointer fields are nullable (null = not set).
#[repr(C)]
pub struct EstimatedDocumentCountOptions {
    /// Maximum time in milliseconds. -1 = not set.
    pub max_time_ms: i64,
    /// Comment BSON value. Nullable.
    pub comment: *const BsonValue,
}

// --- Option parsing (moved from ffi/count.rs) ---

pub(crate) unsafe fn parse_count_options(
    opts: *const CountOptions,
    ctx: *const OperationContext,
) -> Result<crate::coll::options::CountOptions> {
    let mut options = crate::coll::options::CountOptions::default();
    options.read_concern = ctx.read_concern();
    options.selection_criteria = ctx
        .read_preference()
        .map(crate::options::SelectionCriteria::ReadPreference);

    if opts.is_null() {
        return Ok(options);
    }
    let opts = &*opts;

    options.hint = if let Some(name) = c_char_to_string(opts.hint_name)? {
        Some(crate::options::Hint::Name(name))
    } else if let Some(keys) = Bson::to_doc(opts.hint_keys)? {
        Some(crate::options::Hint::Keys(keys))
    } else {
        None
    };

    if let Some(doc) = Bson::to_doc(opts.collation)? {
        options.collation = Some(crate::bson_compat::deserialize_from_document(doc)?);
    }

    if opts.limit >= 0 {
        options.limit = Some(opts.limit as u64);
    }

    if opts.skip >= 0 {
        options.skip = Some(opts.skip as u64);
    }

    options.max_time = i64_to_duration_ms(opts.max_time_ms);

    if !opts.comment.is_null() {
        options.comment = (&*opts.comment).to_bson()?;
    }

    Ok(options)
}

pub(crate) unsafe fn parse_estimated_options(
    opts: *const EstimatedDocumentCountOptions,
    ctx: *const OperationContext,
) -> Result<crate::coll::options::EstimatedDocumentCountOptions> {
    let mut options = crate::coll::options::EstimatedDocumentCountOptions::default();
    options.read_concern = ctx.read_concern();
    options.selection_criteria = ctx
        .read_preference()
        .map(crate::options::SelectionCriteria::ReadPreference);

    if opts.is_null() {
        return Ok(options);
    }
    let opts = &*opts;

    options.max_time = i64_to_duration_ms(opts.max_time_ms);

    if !opts.comment.is_null() {
        options.comment = (&*opts.comment).to_bson()?;
    }

    Ok(options)
}

// --- Prepare functions ---

pub(crate) unsafe fn prepare_count_documents(
    client: &crate::Client,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    filter: *const Bson,
    opts: *const CountOptions,
) -> Result<(Collection<Document>, Document, crate::coll::options::CountOptions)> {
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
    let options = parse_count_options(opts, ctx)?;
    Ok((coll, filter_doc, options))
}

pub(crate) async fn execute_count_documents(
    coll: Collection<Document>,
    filter_doc: Document,
    options: crate::coll::options::CountOptions,
    session: Option<&mut ClientSession>,
) -> Result<u64> {
    let mut action = coll.count_documents(filter_doc).with_options(options);
    if let Some(session) = session {
        action = action.session(session);
    }
    action.await
}

pub(crate) unsafe fn prepare_estimated_document_count(
    client: &crate::Client,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    opts: *const EstimatedDocumentCountOptions,
) -> Result<(Collection<Document>, crate::coll::options::EstimatedDocumentCountOptions)> {
    use crate::error::Error;

    let db = c_char_to_str(db_name)?
        .ok_or_else(|| Error::invalid_argument("db_name cannot be null"))?;
    let coll_name_str = c_char_to_str(coll_name)?
        .ok_or_else(|| Error::invalid_argument("coll_name cannot be null"))?;
    let coll = client.database(db).collection::<Document>(coll_name_str);
    let options = parse_estimated_options(opts, ctx)?;
    Ok((coll, options))
}

pub(crate) async fn execute_estimated_document_count(
    coll: Collection<Document>,
    options: crate::coll::options::EstimatedDocumentCountOptions,
) -> Result<u64> {
    coll.estimated_document_count().with_options(options).await
}
