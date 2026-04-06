//! Shared operation logic for FFI surfaces.
//!
//! Each operation has a `prepare_*` function (synchronous validation and option
//! building) and an `execute_*` function (the async driver call). Both the
//! callback and future FFI surfaces call these shared functions.

pub(crate) mod aggregate;
pub(crate) mod client;
pub(crate) mod command;
pub(crate) mod count;
pub(crate) mod delete;
pub(crate) mod distinct;
pub(crate) mod drop;
pub(crate) mod find;
pub(crate) mod find_one;
pub(crate) mod insert;
pub(crate) mod session;
pub(crate) mod update;
