//! Shared drop operation logic.

use std::ffi::c_char;

use crate::{
    bson::RawDocumentBuf,
    coll::Collection,
    db::Database,
    error::Result,
    ffi::utils::c_char_to_str,
    options::WriteConcern,
    ClientSession,
};

// --- Prepare / execute functions ---

pub(crate) unsafe fn prepare_drop_database(
    client: &crate::Client,
    db_name: *const c_char,
) -> Result<Database> {
    use crate::error::Error;

    let db_name_str = c_char_to_str(db_name)?
        .ok_or_else(|| Error::invalid_argument("db_name cannot be null"))?;
    Ok(client.database(db_name_str))
}

pub(crate) async fn execute_drop_database(
    db: Database,
    session: Option<&mut ClientSession>,
    write_concern: Option<WriteConcern>,
) -> Result<()> {
    let mut action = db.drop();
    if let Some(session) = session {
        action = action.session(session);
    }
    if let Some(wc) = write_concern {
        action = action.write_concern(wc);
    }
    action.await
}

pub(crate) unsafe fn prepare_drop_collection(
    client: &crate::Client,
    db_name: *const c_char,
    coll_name: *const c_char,
) -> Result<Collection<RawDocumentBuf>> {
    use crate::error::Error;

    let db_name_str = c_char_to_str(db_name)?
        .ok_or_else(|| Error::invalid_argument("db_name cannot be null"))?;
    let coll_name_str = c_char_to_str(coll_name)?
        .ok_or_else(|| Error::invalid_argument("coll_name cannot be null"))?;
    Ok(client
        .database(db_name_str)
        .collection::<RawDocumentBuf>(coll_name_str))
}

pub(crate) async fn execute_drop_collection(
    coll: Collection<RawDocumentBuf>,
    session: Option<&mut ClientSession>,
    write_concern: Option<WriteConcern>,
) -> Result<()> {
    let mut action = coll.drop();
    if let Some(session) = session {
        action = action.session(session);
    }
    if let Some(wc) = write_concern {
        action = action.write_concern(wc);
    }
    action.await
}
