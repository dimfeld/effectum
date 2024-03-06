#![warn(missing_docs)]

//! Outbox pattern implementation for Effectum

#[cfg(feature = "postgres")]
pub mod postgres;

#[cfg(feature = "postgres")]
pub use postgres::*;
