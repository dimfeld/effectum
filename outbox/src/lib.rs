#![warn(missing_docs)]

//! Outbox pattern implementation for Effectum
//! This is not ready for use yet.

#[cfg(feature = "postgres")]
pub mod postgres;

#[cfg(feature = "postgres")]
pub use postgres::*;
