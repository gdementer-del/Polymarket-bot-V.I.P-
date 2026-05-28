//! Error types for the Polymarket bot.

use std::path::PathBuf;

use reqwest::StatusCode;
use thiserror::Error;

/// Result alias used throughout the crate.
pub type Result<T> = std::result::Result<T, AppError>;

/// Application error.
#[derive(Debug, Error)]
pub enum AppError {
    #[error("ошибка ввода-вывода: {0}")]
    Io(#[from] std::io::Error),

    #[error("ошибка HTTP-клиента: {0}")]
    HttpClient(#[from] reqwest::Error),

    #[error("ошибка JSON: {0}")]
    Json(#[from] serde_json::Error),

    #[error("HTTP {code}: {body}")]
    HttpStatus { code: StatusCode, body: String },

    #[error("не удалось прочитать конфиг {path}: {source}")]
    ConfigRead {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("не удалось разобрать конфиг {path}: {source}")]
    ConfigParse {
        path: PathBuf,
        #[source]
        source: Box<toml::de::Error>,
    },

    #[error("некорректная конфигурация: {0}")]
    InvalidConfig(&'static str),

    #[error("некорректные настройки live-аутентификации: {0}")]
    InvalidLiveAuth(String),

    #[error("некорректные данные рынка: {0}")]
    InvalidMarket(String),

    #[error("не задана обязательная переменная окружения `{0}`")]
    MissingEnvVar(String),

    #[error("интерактивный ввод секрета недоступен; задайте `{0}` через переменную окружения")]
    InteractiveInputUnavailable(String),

    #[error("live-торговля заблокирована в этой юрисдикции: {country}/{region}")]
    Geoblocked { country: String, region: String },

    #[error("ошибка live-исполнения: {0}")]
    LiveExecution(String),

    #[error("ошибка SDK: {0}")]
    Sdk(String),
}
