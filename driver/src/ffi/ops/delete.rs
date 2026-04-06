//! Shared delete operation logic.

use std::ffi::c_char;

use crate::{
    bson::Document,
    coll::Collection,
    error::Result,
    ffi::{
        types::{Bson, BsonValue, ContextExt, OperationContext},
        utils::{c_char_to_str, c_char_to_string},
    },
    ClientSession,
};

/// Result of a delete_one or delete_many operation.
#[repr(C)]
pub struct DeleteResult {
    pub deleted_count: u64,
}

/// Options for delete operations.
///
/// All pointer fields are nullable (null = not set).
/// `write_concern` comes from `OperationContext`, not this struct.
#[repr(C)]
pub struct DeleteOptions {
    /// Collation as a serialized BSON document. Nullable.
    pub collation: *const Bson,
    /// Index hint by name. Nullable. Takes precedence over `hint_keys`.
    pub hint_name: *const c_char,
    /// Index hint by key pattern as BSON document. Nullable.
    pub hint_keys: *const Bson,
    /// Variables for MQL expressions (`$let`). Nullable BSON document.
    pub let_vars: *const Bson,
    /// Comment BSON value. Nullable.
    pub comment: *const BsonValue,
}

// --- Option parsing (moved from ffi/delete.rs) ---

pub(crate) unsafe fn parse_delete_options(
    opts: *const DeleteOptions,
    ctx: *const OperationContext,
) -> Result<crate::coll::options::DeleteOptions> {
    let mut options = crate::coll::options::DeleteOptions::default();
    options.write_concern = ctx.write_concern();

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

    options.let_vars = Bson::to_doc(opts.let_vars)?;

    if !opts.comment.is_null() {
        options.comment = (&*opts.comment).to_bson()?;
    }

    Ok(options)
}

// --- Prepare function ---

pub(crate) unsafe fn prepare_delete(
    client: &crate::Client,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    filter: *const Bson,
    opts: *const DeleteOptions,
) -> Result<(Collection<Document>, Document, crate::coll::options::DeleteOptions)> {
    use crate::error::Error;

    if filter.is_null() {
        return Err(Error::invalid_argument("filter cannot be null"));
    }
    let db = c_char_to_str(db_name)?
        .ok_or_else(|| Error::invalid_argument("db_name cannot be null"))?;
    let coll_name_str = c_char_to_str(coll_name)?
        .ok_or_else(|| Error::invalid_argument("coll_name cannot be null"))?;
    let coll = client
        .database(db)
        .collection::<Document>(coll_name_str);
    let filter_doc: Document = (&*filter).as_raw_doc()?.try_into()?;
    let options = parse_delete_options(opts, ctx)?;
    Ok((coll, filter_doc, options))
}

// --- Execute functions ---

pub(crate) async fn execute_delete_one(
    coll: Collection<Document>,
    filter_doc: Document,
    options: crate::coll::options::DeleteOptions,
    session: Option<&mut ClientSession>,
) -> Result<crate::results::DeleteResult> {
    let mut action = coll.delete_one(filter_doc).with_options(options);
    if let Some(session) = session {
        action = action.session(session);
    }
    action.await
}

pub(crate) async fn execute_delete_many(
    coll: Collection<Document>,
    filter_doc: Document,
    options: crate::coll::options::DeleteOptions,
    session: Option<&mut ClientSession>,
) -> Result<crate::results::DeleteResult> {
    let mut action = coll.delete_many(filter_doc).with_options(options);
    if let Some(session) = session {
        action = action.session(session);
    }
    action.await
}
