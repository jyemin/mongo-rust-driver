//! Callback-based FFI surface.
//!
//! This module provides the C-compatible API that uses callbacks for
//! async operation results. It uses a shared multi-threaded tokio runtime.

pub mod client;
pub mod cursor;
pub(crate) mod runtime;
pub mod aggregate;
pub mod command;
pub mod count;
pub mod delete;
pub mod distinct;
pub mod drop;
pub mod find;
pub mod find_one;
pub mod insert;
pub mod session;
pub mod update;
