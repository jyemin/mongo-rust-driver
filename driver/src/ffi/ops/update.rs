//! Shared update and replace operation logic.

use std::ffi::c_char;

use crate::{
    bson::Document,
    coll::{
        options::{ReplaceOptions, UpdateModifications, UpdateOptions},
        Collection,
    },
    error::Result,
    ffi::{
        types::{Bson, BsonArray, ContextExt, OperationContext, OwnedBsonValue},
        utils::{c_char_to_str, c_char_to_string},
    },
    ClientSession,
};

// --- Option parsing (moved from ffi/update.rs) ---

pub(crate) unsafe fn parse_update_options(
    opts: *const crate::ffi::update::UpdateOneOptions,
    ctx: *const OperationContext,
) -> Result<UpdateOptions> {
    use crate::ffi::utils::i8_to_option_bool;

    let mut options = UpdateOptions::default();
    options.write_concern = ctx.write_concern();

    if opts.is_null() {
        return Ok(options);
    }
    let opts = &*opts;

    options.upsert = i8_to_option_bool(opts.upsert);
    options.bypass_document_validation = i8_to_option_bool(opts.bypass_document_validation);

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

    options.sort = Bson::to_doc(opts.sort)?;

    if let Some(af_doc) = Bson::to_doc(opts.array_filters)? {
        // array_filters is passed as a BSON document wrapping an array: {"": [...]}
        // Extract the array from the first field
        let mut filters = vec![];
        for (_, val) in &af_doc {
            if let crate::bson::Bson::Array(arr) = val {
                for item in arr {
                    if let crate::bson::Bson::Document(d) = item {
                        filters.push(d.clone());
                    }
                }
            }
        }
        if !filters.is_empty() {
            options.array_filters = Some(filters);
        }
    }

    Ok(options)
}

pub(crate) unsafe fn parse_replace_options(
    opts: *const crate::ffi::update::ReplaceOneOptions,
    ctx: *const OperationContext,
) -> Result<ReplaceOptions> {
    use crate::ffi::utils::i8_to_option_bool;

    let mut options = ReplaceOptions::default();
    options.write_concern = ctx.write_concern();

    if opts.is_null() {
        return Ok(options);
    }
    let opts = &*opts;

    options.upsert = i8_to_option_bool(opts.upsert);
    options.bypass_document_validation = i8_to_option_bool(opts.bypass_document_validation);

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

    options.sort = Bson::to_doc(opts.sort)?;

    Ok(options)
}

// --- Result building ---

pub(crate) fn build_update_result(
    r: crate::results::UpdateResult,
) -> Result<crate::ffi::update::UpdateResult> {
    let upserted_id = match r.upserted_id {
        Some(id) => OwnedBsonValue::from_bson(&id)?,
        None => OwnedBsonValue::null(),
    };
    Ok(crate::ffi::update::UpdateResult {
        matched_count: r.matched_count,
        modified_count: r.modified_count,
        upserted_id,
    })
}

// --- Prepare functions ---

pub(crate) unsafe fn prepare_update(
    client: &crate::Client,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    filter: *const Bson,
    update: *const Bson,
    pipeline: &BsonArray,
    opts: *const crate::ffi::update::UpdateOneOptions,
) -> Result<(Collection<Document>, Document, UpdateModifications, UpdateOptions)> {
    use crate::error::Error;

    if filter.is_null() {
        return Err(Error::invalid_argument("filter cannot be null"));
    }
    let update_set = !update.is_null();
    let pipeline_set = !pipeline.is_empty();
    if update_set && pipeline_set {
        return Err(Error::invalid_argument(
            "only one of update document or pipeline may be provided",
        ));
    }
    if !update_set && !pipeline_set {
        return Err(Error::invalid_argument(
            "one of update document or pipeline must be provided",
        ));
    }
    let db = c_char_to_str(db_name)?
        .ok_or_else(|| Error::invalid_argument("db_name cannot be null"))?;
    let coll_name_str = c_char_to_str(coll_name)?
        .ok_or_else(|| Error::invalid_argument("coll_name cannot be null"))?;
    let coll = client
        .database(db)
        .collection::<Document>(coll_name_str);
    let filter_doc: Document = (&*filter).as_raw_doc()?.try_into()?;
    let modifications = if update_set {
        let update_doc: Document = (&*update).as_raw_doc()?.try_into()?;
        UpdateModifications::Document(update_doc)
    } else {
        let raw_docs = pipeline.to_raw_docs();
        let mut docs = vec![];
        for raw in raw_docs {
            docs.push(Document::try_from(raw)?);
        }
        UpdateModifications::Pipeline(docs)
    };
    let options = parse_update_options(opts, ctx)?;
    Ok((coll, filter_doc, modifications, options))
}

pub(crate) unsafe fn prepare_replace(
    client: &crate::Client,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    filter: *const Bson,
    replacement: *const Bson,
    opts: *const crate::ffi::update::ReplaceOneOptions,
) -> Result<(Collection<Document>, Document, Document, ReplaceOptions)> {
    use crate::error::Error;

    if filter.is_null() {
        return Err(Error::invalid_argument("filter cannot be null"));
    }
    if replacement.is_null() {
        return Err(Error::invalid_argument("replacement cannot be null"));
    }
    let db = c_char_to_str(db_name)?
        .ok_or_else(|| Error::invalid_argument("db_name cannot be null"))?;
    let coll_name_str = c_char_to_str(coll_name)?
        .ok_or_else(|| Error::invalid_argument("coll_name cannot be null"))?;
    let coll = client
        .database(db)
        .collection::<Document>(coll_name_str);
    let filter_doc: Document = (&*filter).as_raw_doc()?.try_into()?;
    let replacement_doc: Document = (&*replacement).as_raw_doc()?.try_into()?;
    let options = parse_replace_options(opts, ctx)?;
    Ok((coll, filter_doc, replacement_doc, options))
}

// --- Execute functions ---

pub(crate) async fn execute_update_one(
    coll: Collection<Document>,
    filter_doc: Document,
    modifications: UpdateModifications,
    options: UpdateOptions,
    session: Option<&mut ClientSession>,
) -> Result<crate::results::UpdateResult> {
    let mut action = coll
        .update_one(filter_doc, modifications)
        .with_options(options);
    if let Some(session) = session {
        action = action.session(session);
    }
    action.await
}

pub(crate) async fn execute_update_many(
    coll: Collection<Document>,
    filter_doc: Document,
    modifications: UpdateModifications,
    options: UpdateOptions,
    session: Option<&mut ClientSession>,
) -> Result<crate::results::UpdateResult> {
    let mut action = coll
        .update_many(filter_doc, modifications)
        .with_options(options);
    if let Some(session) = session {
        action = action.session(session);
    }
    action.await
}

pub(crate) async fn execute_replace_one(
    coll: Collection<Document>,
    filter_doc: Document,
    replacement_doc: Document,
    options: ReplaceOptions,
    session: Option<&mut ClientSession>,
) -> Result<crate::results::UpdateResult> {
    let mut action = coll
        .replace_one(filter_doc, replacement_doc)
        .with_options(options);
    if let Some(session) = session {
        action = action.session(session);
    }
    action.await
}
