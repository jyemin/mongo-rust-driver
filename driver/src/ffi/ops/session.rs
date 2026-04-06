//! Shared session option parsing logic.

use crate::ffi::{
    error::{Error, InvalidArgumentError},
    utils::{c_char_to_string, i64_to_duration_ms, parse_read_preference_mode},
};

/// Parse FFI transaction options into Rust TransactionOptions.
///
/// # Safety
///
/// `options` must be null or a valid pointer to a `TransactionOptions`.
pub(crate) unsafe fn parse_transaction_options(
    options: *const crate::ffi::session::TransactionOptions,
) -> Result<Option<crate::options::TransactionOptions>, Error> {
    if options.is_null() {
        return Ok(None);
    }

    let opts = &*options;
    let mut tx_options = crate::options::TransactionOptions::default();

    // Parse read concern
    if !opts.read_concern_level.is_null() {
        let level = c_char_to_string(opts.read_concern_level)
            .map_err(|e| Error::from(InvalidArgumentError::new(&e.to_string())))?;
        if let Some(level_str) = level {
            tx_options.read_concern = Some(crate::options::ReadConcern::custom(level_str));
        }
    }

    // Parse write concern
    let w = if opts.write_concern_w >= 0 {
        Some(crate::options::Acknowledgment::from(
            opts.write_concern_w as u32,
        ))
    } else if !opts.write_concern_w_tag.is_null() {
        let tag = c_char_to_string(opts.write_concern_w_tag)
            .map_err(|e| Error::from(InvalidArgumentError::new(&e.to_string())))?;
        tag.map(crate::options::Acknowledgment::Custom)
    } else {
        None
    };

    let journal = if opts.write_concern_j >= 0 {
        Some(opts.write_concern_j != 0)
    } else {
        None
    };

    let w_timeout = i64_to_duration_ms(opts.write_concern_w_timeout_ms);

    if w.is_some() || journal.is_some() || w_timeout.is_some() {
        tx_options.write_concern = Some(crate::options::WriteConcern {
            w,
            w_timeout,
            journal,
        });
    }

    // Parse read preference
    let read_pref = parse_read_preference_mode(opts.read_preference_mode)
        .map_err(|e| Error::from(InvalidArgumentError::new(&e.to_string())))?;
    if let Some(rp) = read_pref {
        tx_options.selection_criteria =
            Some(crate::selection_criteria::SelectionCriteria::ReadPreference(rp));
    }

    // Parse max commit time
    tx_options.max_commit_time = i64_to_duration_ms(opts.max_commit_time_ms);

    Ok(Some(tx_options))
}

/// Parse FFI session options into Rust SessionOptions.
///
/// # Safety
///
/// `options` must be null or a valid pointer to a `SessionOptions`.
pub(crate) unsafe fn parse_session_options(
    options: *const crate::ffi::session::SessionOptions,
) -> Result<Option<crate::options::SessionOptions>, Error> {
    if options.is_null() {
        return Ok(None);
    }

    let opts = &*options;
    let mut session_options = crate::options::SessionOptions::default();

    // Parse causal consistency
    if opts.causal_consistency >= 0 {
        session_options.causal_consistency = Some(opts.causal_consistency != 0);
    }

    // Parse snapshot
    if opts.snapshot >= 0 {
        session_options.snapshot = Some(opts.snapshot != 0);
    }

    // Parse default transaction options
    session_options.default_transaction_options =
        parse_transaction_options(opts.default_transaction_options)?;

    Ok(Some(session_options))
}
