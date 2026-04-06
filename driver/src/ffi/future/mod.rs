//! Future-based FFI surface.
//!
//! Returns opaque future handles instead of using callbacks. Uses a per-client
//! `current_thread` tokio runtime that the caller drives via `tick()`.

pub mod aggregate;
pub mod client;
pub mod command;
pub mod count;
pub mod cursor;
pub mod delete;
pub mod distinct;
pub mod drop;
pub mod find;
pub mod find_one;
pub mod future;
pub mod insert;
pub mod session;
pub mod update;
