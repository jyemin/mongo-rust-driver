//! Shared distinct operation logic.

use std::ffi::c_char;

use crate::{
    bson::Document,
    coll::Collection,
    error::Result,
    ffi::{
        types::{Bson, BsonValue, ContextExt, OperationContext, OwnedBsonValue},
        utils::{c_char_to_str, c_char_to_string, i64_to_duration_ms},
    },
    ClientSession,
};

/// Result for `mongo_distinct`.
///
/// On success, `values` points to an array of `len` BSON values.
/// The memory is owned by the driver and valid only for the duration of the callback.
#[repr(C)]
pub struct DistinctResult {
    /// Pointer to an array of distinct values.
    pub values: *const OwnedBsonValue,
    /// Number of values in the array.
    pub len: usize,
}

/// Options for `mongo_distinct`.
///
/// All pointer fields are nullable (null = not set).
#[repr(C)]
pub struct DistinctOptions {
    /// Collation as a serialized BSON document. Nullable.
    pub collation: *const Bson,
    /// Index hint by name. Nullable. Takes precedence over `hint_keys`.
    pub hint_name: *const c_char,
    /// Index hint by key pattern as BSON document. Nullable.
    pub hint_keys: *const Bson,
    /// Maximum time in milliseconds. -1 = not set.
    pub max_time_ms: i64,
    /// Comment BSON value. Nullable.
    pub comment: *const BsonValue,
}

// --- Option parsing (moved from ffi/distinct.rs) ---

pub(crate) unsafe fn parse_distinct_options(
    opts: *const DistinctOptions,
    ctx: *const OperationContext,
) -> Result<crate::coll::options::DistinctOptions> {
    let mut options = crate::coll::options::DistinctOptions::default();
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

    options.max_time = i64_to_duration_ms(opts.max_time_ms);

    if !opts.comment.is_null() {
        options.comment = (&*opts.comment).to_bson()?;
    }

    Ok(options)
}

// --- Prepare / execute functions ---

pub(crate) unsafe fn prepare_distinct(
    client: &crate::Client,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    field_name: *const c_char,
    filter: *const Bson,
    opts: *const DistinctOptions,
) -> Result<(
    Collection<Document>,
    String,
    Document,
    crate::coll::options::DistinctOptions,
)> {
    use crate::error::Error;

    let db = c_char_to_str(db_name)?
        .ok_or_else(|| Error::invalid_argument("db_name cannot be null"))?;
    let coll_name_str = c_char_to_str(coll_name)?
        .ok_or_else(|| Error::invalid_argument("coll_name cannot be null"))?;
    let field = c_char_to_str(field_name)?
        .ok_or_else(|| Error::invalid_argument("field_name cannot be null"))?;
    let coll = client
        .database(db)
        .collection::<Document>(coll_name_str);
    let filter_doc: Document = if filter.is_null() {
        crate::bson::doc! {}
    } else {
        (&*filter).as_raw_doc()?.try_into()?
    };
    let options = parse_distinct_options(opts, ctx)?;
    Ok((coll, field.to_string(), filter_doc, options))
}

pub(crate) async fn execute_distinct(
    coll: Collection<Document>,
    field: String,
    filter_doc: Document,
    options: crate::coll::options::DistinctOptions,
    session: Option<&mut ClientSession>,
) -> Result<Vec<crate::bson::Bson>> {
    let mut action = coll.distinct(field, filter_doc).with_options(options);
    if let Some(session) = session {
        action = action.session(session);
    }
    action.await
}
