//! FFI (Foreign Function Interface) layer for the MongoDB Rust driver.
//!
//! Shared types, error handling, and utilities used by both the callback
//! and future-based FFI surfaces.

pub mod error;
pub mod event;
pub mod types;
pub(crate) mod utils;
pub(crate) mod ops;

#[cfg(feature = "ffi")]
pub mod callback;
