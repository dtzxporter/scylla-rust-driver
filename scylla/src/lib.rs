//! Async CQL driver for Rust, optimized for Scylla.

#[macro_use]
extern crate anyhow;

#[macro_use]
pub mod macros;

pub mod frame;
pub mod transport;
pub mod statement;

pub use statement::query;
pub use statement::prepared_statement;
