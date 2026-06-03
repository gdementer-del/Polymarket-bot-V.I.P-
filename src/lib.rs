//! Low-latency Polymarket research and paper-trading toolkit.
//!
//! The application combines Polymarket order books with exchange and oracle
//! feeds for controlled strategy experiments across short crypto windows.

pub mod config;
pub mod error;
pub mod models;
pub mod services;

pub use crate::config::{AppConfig, BotMode};
pub use crate::error::{AppError, Result};
