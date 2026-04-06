//! Shared insert operation logic.

use std::ffi::c_char;

use crate::{
    bson::Document,
    coll::{options::InsertOneOptions, Collection},
    error::Result,
    ffi::{
        types::{BsonArray, BsonValue, ContextExt, OperationContext},
        utils::c_char_to_str,
    },
    ClientSession,
};

// --- Prepare functions ---

pub(crate) unsafe fn prepare_insert_one(
    client: &crate::Client,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    document: *const crate::ffi::types::Bson,
    bypass_document_validation: i8,
    comment: *const BsonValue,
) -> Result<(Collection<Document>, &'static crate::bson::RawDocument, InsertOneOptions)> {
    use crate::error::Error;

    if document.is_null() {
        return Err(Error::invalid_argument("document cannot be null"));
    }

    let db_name_str = c_char_to_str(db_name)?
        .ok_or_else(|| Error::invalid_argument("db_name cannot be null"))?;
    let coll_name_str = c_char_to_str(coll_name)?
        .ok_or_else(|| Error::invalid_argument("coll_name cannot be null"))?;
    let coll = client
        .database(db_name_str)
        .collection::<Document>(coll_name_str);

    let doc_bson = &*document;
    let raw_doc = doc_bson.as_raw_doc()?;

    let mut options = InsertOneOptions::default();
    if bypass_document_validation >= 0 {
        options.bypass_document_validation = Some(bypass_document_validation != 0);
    }
    if !comment.is_null() {
        let comment_val = &*comment;
        options.comment = comment_val.to_bson()?;
    }
    options.write_concern = ctx.write_concern();

    Ok((coll, raw_doc, options))
}

pub(crate) async fn execute_insert_one(
    coll: Collection<Document>,
    raw_doc: &crate::bson::RawDocument,
    options: InsertOneOptions,
    session: Option<&mut ClientSession>,
) -> Result<crate::results::InsertOneResult> {
    coll.insert_one_raw(raw_doc, Some(options), session).await
}

pub(crate) unsafe fn prepare_insert_many(
    client: &crate::Client,
    ctx: *const OperationContext,
    db_name: *const c_char,
    coll_name: *const c_char,
    documents: &BsonArray,
    bypass_document_validation: i8,
    ordered: bool,
    comment: *const BsonValue,
) -> Result<(Collection<Document>, crate::options::InsertManyOptions)> {
    use crate::error::Error;

    if documents.is_empty() {
        return Err(Error::invalid_argument("documents cannot be empty"));
    }

    let db_name_str = c_char_to_str(db_name)?
        .ok_or_else(|| Error::invalid_argument("db_name cannot be null"))?;
    let coll_name_str = c_char_to_str(coll_name)?
        .ok_or_else(|| Error::invalid_argument("coll_name cannot be null"))?;
    let coll = client
        .database(db_name_str)
        .collection::<Document>(coll_name_str);

    let mut options = crate::options::InsertManyOptions::default();
    if bypass_document_validation >= 0 {
        options.bypass_document_validation = Some(bypass_document_validation != 0);
    }
    options.ordered = Some(ordered);
    if !comment.is_null() {
        let comment_val = &*comment;
        options.comment = comment_val.to_bson()?;
    }
    options.write_concern = ctx.write_concern();

    Ok((coll, options))
}

pub(crate) async unsafe fn execute_insert_many(
    coll: Collection<Document>,
    documents: BsonArray,
    options: crate::options::InsertManyOptions,
    session: Option<&mut ClientSession>,
) -> Result<crate::results::InsertManyResult> {
    let raw_docs = documents.to_raw_docs();
    coll.insert_many_raw(&raw_docs, Some(options), session)
        .await
}
