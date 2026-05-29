//! Error types for the Polymarket bot.

use std::path::PathBuf;

use reqwest::StatusCode;
use thiserror::Error;

/// Result alias used throughout the crate.
pub type Result<T> = std::result::Result<T, AppError>;

/// Application error.
#[derive(Debug, Error)]
pub enum AppError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("HTTP client error: {0}")]
    HttpClient(#[from] reqwest::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("HTTP {code}: {body}")]
    HttpStatus { code: StatusCode, body: String },

    #[error("failed to read config {path}: {source}")]
    ConfigRead {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("failed to parse config {path}: {source}")]
    ConfigParse {
        path: PathBuf,
        #[source]
        source: Box<toml::de::Error>,
    },

    #[error("invalid config: {0}")]
    InvalidConfig(&'static str),

    #[error("invalid live auth settings: {0}")]
    InvalidLiveAuth(String),

    #[error("invalid market data: {0}")]
    InvalidMarket(String),

    #[error("required environment variable `{0}` is not set")]
    MissingEnvVar(String),

    #[error("interactive secret input is unavailable; set `{0}` through an environment variable")]
    InteractiveInputUnavailable(String),

    #[error("live trading is blocked in this jurisdiction: {country}/{region}")]
    Geoblocked { country: String, region: String },

    #[error("live execution error: {0}")]
    LiveExecution(String),

    #[error("SDK error: {0}")]
    Sdk(String),
}
