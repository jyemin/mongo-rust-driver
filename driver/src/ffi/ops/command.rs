//! Shared command operation logic.

use std::ffi::c_char;

use crate::{
    bson::{Document, RawDocumentBuf},
    db::Database,
    error::Result,
    ffi::{
        types::{Bson, ContextExt, OperationContext},
        utils::c_char_to_str,
    },
    options::RunCommandOptions,
    ClientSession,
};

// --- Prepare / execute functions ---

pub(crate) unsafe fn prepare_run_command(
    client: &crate::Client,
    ctx: *mut OperationContext,
    db_name: *const c_char,
    command: *const Bson,
) -> Result<(Database, RunCommandOptions, RawDocumentBuf)> {
    use crate::error::Error;

    if command.is_null() {
        return Err(Error::invalid_argument("command cannot be null"));
    }

    let db_name_str = c_char_to_str(db_name)?
        .ok_or_else(|| Error::invalid_argument("db_name cannot be null"))?;
    let db = client.database(db_name_str);

    let mut options = RunCommandOptions::default();
    options.selection_criteria = ctx
        .read_preference()
        .map(crate::selection_criteria::SelectionCriteria::ReadPreference);

    let command_bson = &*command;
    let command_bytes = std::slice::from_raw_parts(command_bson.data, command_bson.len);
    let command_doc = RawDocumentBuf::from_bytes(command_bytes.to_vec())?;

    Ok((db, options, command_doc))
}

pub(crate) async fn execute_run_command(
    db: Database,
    options: RunCommandOptions,
    command_doc: RawDocumentBuf,
    session: Option<&mut ClientSession>,
) -> Result<Document> {
    let mut action = db.run_raw_command(command_doc).with_options(options);
    if let Some(session) = session {
        action = action.session(session);
    }
    action.await
}
