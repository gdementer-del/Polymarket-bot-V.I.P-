//! Polymarket BTC 5-minute bundle-trading bot.
//!
//! The strategy scans `BTC up/down 5m` markets and looks for cases where
//! the best ask for `Up` plus the best ask for `Down` is below `1.00`.

pub mod config;
pub mod error;
pub mod models;
pub mod services;

pub use crate::config::{AppConfig, BotMode};
pub use crate::error::{AppError, Result};
