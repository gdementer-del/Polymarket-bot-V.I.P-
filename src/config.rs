//! Configuration types and CLI parsing.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand, ValueEnum};
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::error::{AppError, Result};
use crate::models::MarketTarget;

const DEFAULT_CONFIG_PATH: &str = "config.example.toml";
const DEFAULT_GAMMA_BASE_URL: &str = "https://gamma-api.polymarket.com";
const DEFAULT_CLOB_BASE_URL: &str = "https://clob.polymarket.com";
const DEFAULT_DATA_API_BASE_URL: &str = "https://data-api.polymarket.com";
const DEFAULT_GEOBLOCK_URL: &str = "https://polymarket.com/api/geoblock";
const DEFAULT_BINANCE_BASE_URL: &str = "https://api.binance.com";
const DEFAULT_BINANCE_WS_BASE_URL: &str = "wss://stream.binance.com:9443/ws";
const DEFAULT_COINBASE_WS_BASE_URL: &str = "wss://ws-feed.exchange.coinbase.com";
const DEFAULT_POLYMARKET_RTDS_WS_URL: &str = "wss://ws-live-data.polymarket.com";
const DEFAULT_POLYBACKTEST_BASE_URL: &str = "https://api.polybacktest.com";
const DEFAULT_REFRESH_SECS: u64 = 5;
const DEFAULT_BONEREAPER_WALLET: &str = "0xeebde7a0e019a63e6b476eb425505b7b3e6eba30";

/// CLI-точка входа для бота.
#[derive(Debug, Parser)]
#[command(author, version, about = "Торговый бот для BTC 5m рынков Polymarket")]
pub struct Cli {
    /// Путь к TOML-конфигу.
    #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
    pub config: PathBuf,

    #[command(subcommand)]
    pub command: Command,
}

/// Набор команд бота.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Просканировать активные BTC 5m рынки и вывести лучшие возможности.
    Scan {
        /// Сколько возможностей вывести.
        #[arg(long, default_value_t = 10)]
        top: usize,
    },
    /// Показать активные BTC 5m рынки и их текущие котировки.
    Markets {
        /// Сколько рынков вывести.
        #[arg(long, default_value_t = 20)]
        top: usize,
        /// Обновлять вывод в цикле.
        #[arg(long)]
        watch: bool,
        /// Интервал обновления в секундах.
        #[arg(long, default_value_t = DEFAULT_REFRESH_SECS)]
        refresh_secs: u64,
        /// Ограничить число циклов обновления. Удобно для smoke test.
        #[arg(long)]
        cycles: Option<usize>,
    },
    /// Открыть терминальный дашборд со сводкой рынков и сигналов.
    Dashboard {
        /// Сколько строк рынков выводить.
        #[arg(long, default_value_t = 12)]
        top: usize,
        /// Интервал обновления в секундах.
        #[arg(long, default_value_t = DEFAULT_REFRESH_SECS)]
        refresh_secs: u64,
        /// Ограничить число циклов обновления. Удобно для smoke test.
        #[arg(long)]
        cycles: Option<usize>,
    },
    /// Вывести аналитику по локальному журналу исполнений.
    Backtest {
        #[arg(long, default_value_t = 30)]
        windows_per_target: usize,
        #[arg(long, default_value_t = 1)]
        entry_minutes: u32,
        #[arg(long, default_value_t = 10)]
        top: usize,
        #[arg(long)]
        target: Option<MarketTarget>,
    },
    /// Прогнать стратегию по историческим snapshot'ам `PolyBackTest`.
    #[command(name = "polybacktest", alias = "poly-backtest")]
    PolyBacktest {
        #[arg(long, default_value_t = 30)]
        windows_per_target: usize,
        #[arg(long, default_value_t = 1)]
        entry_minutes: u32,
        #[arg(long, default_value_t = 10)]
        top: usize,
        #[arg(long)]
        target: Option<MarketTarget>,
    },
    /// Sweep safe strategy thresholds against `PolyBackTest` and rank variants.
    #[command(name = "polybacktest-tune", alias = "poly-tune")]
    PolyBacktestTune {
        #[arg(long, default_value_t = 30)]
        windows_per_target: usize,
        #[arg(long, value_delimiter = ',', default_value = "1,2,3")]
        entry_minutes: Vec<u32>,
        #[arg(long, default_value_t = 10)]
        top: usize,
        #[arg(long)]
        target: Option<MarketTarget>,
        #[arg(long, value_delimiter = ',')]
        variants: Vec<String>,
        #[arg(long)]
        max_variants: Option<usize>,
    },
    Analytics {
        /// Максимальное число последних записей журнала для анализа.
        #[arg(long)]
        limit: Option<usize>,
    },
    /// Проверить live-учётные данные Polymarket без выставления ордеров.
    #[command(name = "paper-report", alias = "paper_report")]
    PaperReport {
        #[arg(long)]
        limit: Option<usize>,
    },
    /// Показать ленту paper-сделок с событиями открытия и закрытия.
    #[command(name = "paper-trades", alias = "paper_trades")]
    PaperTrades {
        #[arg(long)]
        limit: Option<usize>,
        /// Show only paper trade events recorded at or after this timestamp.
        ///
        /// Accepts RFC3339/UTC (`2026-05-06T03:49:26Z`) or local time
        /// (`2026-05-06 10:49:26`).
        #[arg(long)]
        since: Option<String>,
    },
    /// Show paper trade quality grouped by entry ask, timing, gap, and MFE/MAE.
    #[command(name = "paper-quality", alias = "paper_quality")]
    PaperQuality {
        #[arg(long)]
        limit: Option<usize>,
        /// Show only closed trades whose open/close records are after this timestamp.
        #[arg(long)]
        since: Option<String>,
    },
    /// Summarize one paper run by timestamp, including PnL and rejection reasons.
    #[command(name = "paper-run-summary", alias = "paper_run_summary")]
    PaperRunSummary {
        /// Start timestamp of the run. Accepts RFC3339/UTC or local time.
        #[arg(long)]
        since: Option<String>,
        /// Limit loaded journal rows when --since is omitted.
        #[arg(long)]
        limit: Option<usize>,
        /// Number of top near-miss and close-category rows to print.
        #[arg(long, default_value_t = 10)]
        top: usize,
    },
    /// Показать текущие открытые позиции paper-режима.
    #[command(name = "paper-positions", alias = "paper_positions")]
    PaperPositions,
    AuthCheck,
    /// Watch wallet activity from Polymarket Data API.
    #[command(name = "follow-wallet", alias = "follow")]
    FollowWallet {
        /// Wallet address to monitor.
        #[arg(long, default_value = DEFAULT_BONEREAPER_WALLET)]
        wallet: String,
        /// Number of latest activity records fetched per poll.
        #[arg(long, default_value_t = 80)]
        limit: usize,
        /// Poll interval in seconds.
        #[arg(long, default_value_t = 8)]
        refresh_secs: u64,
        /// Filter to BTC 5m windows only.
        #[arg(long, default_value_t = true)]
        btc_only: bool,
        /// Optional max number of polling cycles.
        #[arg(long)]
        cycles: Option<usize>,
    },
    /// Watch wallet activity and persist enriched market snapshots for each trade.
    #[command(name = "follow-wallet-record", alias = "follow-record")]
    FollowWalletRecord {
        /// Wallet address to monitor.
        #[arg(long, default_value = DEFAULT_BONEREAPER_WALLET)]
        wallet: String,
        /// Number of latest activity records fetched per poll.
        #[arg(long, default_value_t = 80)]
        limit: usize,
        /// Poll interval in seconds.
        #[arg(long, default_value_t = 8)]
        refresh_secs: u64,
        /// Filter to BTC 5m windows only.
        #[arg(long, default_value_t = true)]
        btc_only: bool,
        /// Optional max number of polling cycles.
        #[arg(long)]
        cycles: Option<usize>,
        /// Optional path to the JSONL file written for captured trades.
        #[arg(long)]
        output: Option<PathBuf>,
    },
    /// Show summary report for enriched wallet activity records.
    #[command(name = "follow-wallet-report", alias = "follow-report")]
    FollowWalletReport {
        /// Optional path to the JSONL file written by `follow-wallet-record`.
        #[arg(long)]
        input: Option<PathBuf>,
        /// Show only the latest N records in the report.
        #[arg(long)]
        limit: Option<usize>,
        /// Number of top windows/slugs to show.
        #[arg(long, default_value_t = 10)]
        top: usize,
    },
    /// Show replay-oriented inventory report for enriched wallet activity records.
    #[command(name = "follow-wallet-replay-report", alias = "follow-replay")]
    FollowWalletReplayReport {
        /// Optional path to the JSONL file written by `follow-wallet-record`.
        #[arg(long)]
        input: Option<PathBuf>,
        /// Show only the latest N records in the report.
        #[arg(long)]
        limit: Option<usize>,
        /// Number of top windows/slugs to show.
        #[arg(long, default_value_t = 10)]
        top: usize,
    },
    /// Show event-by-event replay timeline for one wallet activity window.
    #[command(name = "follow-wallet-replay-window", alias = "follow-window")]
    FollowWalletReplayWindow {
        /// Optional path to the JSONL file written by `follow-wallet-record`.
        #[arg(long)]
        input: Option<PathBuf>,
        /// Show only the latest N records in the source log before building the replay.
        #[arg(long)]
        limit: Option<usize>,
        /// Specific slug to inspect. If omitted, the highest-volume replay window is used.
        #[arg(long)]
        slug: Option<String>,
        /// Number of replay events to print.
        #[arg(long, default_value_t = 25)]
        events: usize,
    },
    /// Export replay dataset with window timelines and alerts to JSON.
    #[command(name = "follow-wallet-replay-export", alias = "follow-export")]
    FollowWalletReplayExport {
        /// Optional path to the JSONL file written by `follow-wallet-record`.
        #[arg(long)]
        input: Option<PathBuf>,
        /// Show only the latest N records in the source log before building the replay.
        #[arg(long)]
        limit: Option<usize>,
        /// Optional explicit output path for exported replay JSON.
        #[arg(long)]
        output: Option<PathBuf>,
    },
    /// Simulate inventory caps and cooldown rules on exported replay dataset.
    #[command(name = "follow-wallet-replay-simulate", alias = "follow-sim")]
    FollowWalletReplaySimulate {
        /// Path to replay export JSON produced by `follow-wallet-replay-export`.
        #[arg(long)]
        input: Option<PathBuf>,
        /// Max allowed gross inventory shares per window.
        #[arg(long, default_value_t = 4000)]
        max_gross_window_shares: u32,
        /// Max allowed directional delta shares per window.
        #[arg(long, default_value_t = 3000)]
        max_directional_delta_shares: u32,
        /// Cooldown length in seconds after adverse event.
        #[arg(long, default_value_t = 120)]
        cooldown_secs: i64,
        /// Trigger cooldown on replay cooldown-candidate alerts.
        #[arg(long, default_value_t = true)]
        trigger_on_cooldown_alert: bool,
        /// Trigger cooldown on late-window-expansion alerts.
        #[arg(long, default_value_t = true)]
        trigger_on_late_expansion: bool,
    },
    /// Compare multiple replay export JSON files side by side.
    #[command(name = "follow-wallet-research-compare", alias = "follow-compare")]
    FollowWalletResearchCompare {
        /// Replay export JSON files to compare.
        #[arg(long, required = true, num_args = 1..)]
        inputs: Vec<PathBuf>,
        /// Number of top sessions to highlight.
        #[arg(long, default_value_t = 10)]
        top: usize,
    },
    /// Sweep gross/delta/cooldown grids across replay exports and rank v4 risk profiles.
    #[command(name = "follow-wallet-replay-autotune", alias = "follow-autotune")]
    FollowWalletReplayAutotune {
        /// Replay export JSON files to tune against.
        #[arg(long, required = true, num_args = 1..)]
        inputs: Vec<PathBuf>,
        /// Candidate gross inventory caps per window, comma-separated.
        #[arg(long, value_delimiter = ',', default_values_t = [2500_u32, 3000, 4000, 5000])]
        gross_values: Vec<u32>,
        /// Candidate directional delta caps per window, comma-separated.
        #[arg(long, value_delimiter = ',', default_values_t = [1500_u32, 2000, 2500, 3000, 3500])]
        delta_values: Vec<u32>,
        /// Candidate cooldown values in seconds, comma-separated.
        #[arg(long, value_delimiter = ',', default_values_t = [60_i64, 120, 180])]
        cooldown_values: Vec<i64>,
        /// Number of top candidate profiles to show.
        #[arg(long, default_value_t = 12)]
        top: usize,
    },
    /// Derive heuristic alert thresholds from replay export sessions.
    #[command(name = "follow-wallet-alert-calibrate", alias = "follow-calibrate")]
    FollowWalletAlertCalibrate {
        /// Replay export JSON files used for threshold calibration.
        #[arg(long, required = true, num_args = 1..)]
        inputs: Vec<PathBuf>,
    },
    /// Запустить бота один раз или в цикле.
    Run {
        /// Переопределить режим исполнения из конфига.
        #[arg(long)]
        mode: Option<BotMode>,
        /// Выполнить один цикл и завершиться.
        #[arg(long)]
        once: bool,
        /// Stop after this many seconds, flushing paper journals before exit.
        #[arg(long)]
        max_runtime_secs: Option<u64>,
        /// After max runtime, stop new entries and keep managing paper positions until flat.
        #[arg(long)]
        drain_open_positions: bool,
        /// Maximum extra seconds to spend in drain mode.
        #[arg(long, default_value_t = 600)]
        max_drain_secs: u64,
    },
}

/// Режим исполнения.
#[derive(Debug, Clone, Copy, Eq, PartialEq, ValueEnum, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BotMode {
    /// Имитация торговли без отправки ордеров.
    Paper,
    /// Отправка реальных ордеров через официальный Polymarket Rust SDK.
    Live,
}

/// Full application configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub http: HttpConfig,
    pub strategy: StrategyConfig,
    pub run: RunConfig,
    pub live: LiveConfig,
    #[serde(default)]
    pub polybacktest: PolyBacktestConfig,
    pub storage: StorageConfig,
}

impl AppConfig {
    /// Load config from disk and apply environment overrides.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read, parsed, or validated.
    pub fn load(path: &Path) -> Result<Self> {
        let raw = fs::read_to_string(path).map_err(|source| AppError::ConfigRead {
            path: path.to_path_buf(),
            source,
        })?;
        let mut config: AppConfig =
            toml::from_str(&raw).map_err(|source| AppError::ConfigParse {
                path: path.to_path_buf(),
                source: Box::new(source),
            })?;
        config.apply_env_overrides();
        config.validate()?;
        Ok(config)
    }

    fn apply_env_overrides(&mut self) {
        if let Ok(mode) = env::var("POLY_BOT_MODE") {
            self.run.mode = match mode.as_str() {
                "paper" => BotMode::Paper,
                "live" => BotMode::Live,
                _ => self.run.mode,
            };
        }

        if let Ok(top_n) = env::var("POLY_BOT_EXECUTE_TOP_N")
            && let Ok(parsed) = top_n.parse::<usize>()
        {
            self.run.execute_top_n = parsed.max(1);
        }

        if let Ok(revalidate) = env::var("POLY_BOT_REVALIDATE_BEFORE_EXECUTE") {
            let normalized = revalidate.trim().to_ascii_lowercase();
            self.run.revalidate_before_execute =
                matches!(normalized.as_str(), "1" | "true" | "yes" | "on");
        }

        if let Ok(signature_type) = env::var("POLYMARKET_SIGNATURE_TYPE") {
            self.live.signature_type = match signature_type.as_str() {
                "eoa" => LiveSignatureType::Eoa,
                "proxy" => LiveSignatureType::Proxy,
                "gnosis_safe" => LiveSignatureType::GnosisSafe,
                _ => self.live.signature_type,
            };
        }

        if let Ok(funder_address) = env::var("POLYMARKET_FUNDER_ADDRESS") {
            let trimmed = funder_address.trim();
            self.live.funder_address = if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_owned())
            };
        }
    }

    #[allow(clippy::too_many_lines)]
    pub(crate) fn validate(&self) -> Result<()> {
        if self.strategy.max_markets == 0 {
            return Err(AppError::InvalidConfig(
                "strategy.max_markets должен быть больше нуля",
            ));
        }

        if self.strategy.market_targets.is_empty() {
            return Err(AppError::InvalidConfig(
                "strategy.market_targets не должен быть пустым",
            ));
        }

        if self.run.execute_top_n == 0 {
            return Err(AppError::InvalidConfig(
                "run.execute_top_n должен быть больше нуля",
            ));
        }

        if self.http.coinbase_max_source_disagreement_bps < Decimal::ZERO
            || self.http.coinbase_max_spread_bps < Decimal::ZERO
        {
            return Err(AppError::InvalidConfig(
                "http.coinbase_max_source_disagreement_bps and http.coinbase_max_spread_bps must be non-negative",
            ));
        }

        if self.strategy.enable_bonereaper_state_v2 && self.strategy.enable_codex_sentinel_v1 {
            return Err(AppError::InvalidConfig(
                "strategy.enable_bonereaper_state_v2 and strategy.enable_codex_sentinel_v1 cannot both be enabled",
            ));
        }

        if self.strategy.codex_scalp_probe_v1_raw_ablation_enabled
            && self.run.mode != BotMode::Paper
        {
            return Err(AppError::InvalidConfig(
                "strategy.codex_scalp_probe_v1_raw_ablation_enabled is paper-only",
            ));
        }
        if self.strategy.codex_scalp_probe_v1_raw_light_enabled
            && !self.strategy.codex_scalp_probe_v1_raw_ablation_enabled
        {
            return Err(AppError::InvalidConfig(
                "strategy.codex_scalp_probe_v1_raw_light_enabled requires raw ablation mode",
            ));
        }

        if self.run.polymarket_stream.book_staleness_ms <= 0 {
            return Err(AppError::InvalidConfig(
                "run.polymarket_stream.book_staleness_ms must be positive",
            ));
        }

        if self.run.chainlink_oracle.enabled
            && (self.run.chainlink_oracle.max_quote_age_ms <= 0
                || self.run.chainlink_oracle.max_window_open_lag_ms < 0
                || self.run.chainlink_oracle.max_settlement_close_lag_ms < 0)
        {
            return Err(AppError::InvalidConfig(
                "run.chainlink_oracle age/lag settings must be non-negative, and max_quote_age_ms must be positive",
            ));
        }

        if self
            .run
            .paper_starting_balance_usdc
            .is_some_and(|value| value < Decimal::ZERO)
        {
            return Err(AppError::InvalidConfig(
                "run.paper_starting_balance_usdc must not be negative",
            ));
        }

        if self.run.scale_in.min_price_improvement < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "run.scale_in.min_price_improvement must not be negative",
            ));
        }

        if self.strategy.directional_micro_burst_weight < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "strategy.directional_micro_burst_weight must not be negative",
            ));
        }

        if self.strategy.micro_breakout_min_spot_move_1s_bps < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "strategy.micro_breakout_min_spot_move_1s_bps must not be negative",
            ));
        }

        if self.strategy.micro_breakout_signal_burst_multiplier < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "strategy.micro_breakout_signal_burst_multiplier must not be negative",
            ));
        }

        if self.strategy.micro_breakout_expensive_entry_price < Decimal::ZERO
            || self.strategy.micro_breakout_expensive_entry_price > Decimal::ONE
        {
            return Err(AppError::InvalidConfig(
                "strategy.micro_breakout_expensive_entry_price must be in range [0, 1]",
            ));
        }

        if self
            .strategy
            .micro_breakout_strong_signal_min_spot_move_1s_bps
            < Decimal::ZERO
        {
            return Err(AppError::InvalidConfig(
                "strategy.micro_breakout_strong_signal_min_spot_move_1s_bps must not be negative",
            ));
        }

        if self.strategy.micro_breakout_max_burst_to_micro_ratio < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "strategy.micro_breakout_max_burst_to_micro_ratio must not be negative",
            ));
        }

        if self.strategy.micro_breakout_target_cross_min_gap_bps < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "strategy.micro_breakout_target_cross_min_gap_bps must not be negative",
            ));
        }

        if self.strategy.micro_breakout_target_cross_signal_boost_bps < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "strategy.micro_breakout_target_cross_signal_boost_bps must not be negative",
            ));
        }

        if self.strategy.micro_breakout_max_average_price_drift < Decimal::ZERO
            || self.strategy.micro_breakout_max_average_price_drift > Decimal::ONE
        {
            return Err(AppError::InvalidConfig(
                "strategy.micro_breakout_max_average_price_drift must be in range [0, 1]",
            ));
        }

        if self.strategy.target_state_min_target_gap_bps < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "strategy.target_state_min_target_gap_bps must not be negative",
            ));
        }

        if self.strategy.target_state_min_spot_move_15s_bps < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "strategy.target_state_min_spot_move_15s_bps must not be negative",
            ));
        }

        if self.strategy.target_state_min_aligned_flow_bps < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "strategy.target_state_min_aligned_flow_bps must not be negative",
            ));
        }

        if self.strategy.target_state_max_entry_price < Decimal::ZERO
            || self.strategy.target_state_max_entry_price > Decimal::ONE
        {
            return Err(AppError::InvalidConfig(
                "strategy.target_state_max_entry_price must be in range [0, 1]",
            ));
        }

        if self.run.scale_in.min_impulse_improvement_bps < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "run.scale_in.min_impulse_improvement_bps must not be negative",
            ));
        }

        if self.run.adaptive_regime.safe_max_entries_per_cycle == 0 {
            return Err(AppError::InvalidConfig(
                "run.adaptive_regime.safe_max_entries_per_cycle должен быть больше нуля",
            ));
        }

        if self.run.early_exit.min_hold_secs < 0 {
            return Err(AppError::InvalidConfig(
                "run.early_exit.min_hold_secs must not be negative",
            ));
        }

        if self.run.early_exit.min_take_profit_usdc < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "run.early_exit.min_take_profit_usdc must not be negative",
            ));
        }

        if self.run.early_exit.min_expected_profit_capture_ratio < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "run.early_exit.min_expected_profit_capture_ratio must not be negative",
            ));
        }

        if self.run.early_exit.max_loss_usdc < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "run.early_exit.max_loss_usdc must not be negative",
            ));
        }

        if self.run.early_exit.profit_lock_min_profit_usdc < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "run.early_exit.profit_lock_min_profit_usdc must not be negative",
            ));
        }

        if self.run.early_exit.profit_lock_partial_close_ratio <= Decimal::ZERO
            || self.run.early_exit.profit_lock_partial_close_ratio >= Decimal::ONE
        {
            return Err(AppError::InvalidConfig(
                "run.early_exit.profit_lock_partial_close_ratio must be in range (0, 1)",
            ));
        }

        if self.run.early_exit.reversal_min_5s_bps < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "run.early_exit.reversal_min_5s_bps must not be negative",
            ));
        }

        if self
            .run
            .early_exit
            .bonereaper_state_v2_stop_loss_min_15s_bps
            < Decimal::ZERO
        {
            return Err(AppError::InvalidConfig(
                "run.early_exit.bonereaper_state_v2_stop_loss_min_15s_bps must not be negative",
            ));
        }

        if self.run.early_exit.bonereaper_state_v2_reversal_min_15s_bps < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "run.early_exit.bonereaper_state_v2_reversal_min_15s_bps must not be negative",
            ));
        }

        if self.run.early_exit.micro_breakout_fail_fast_1s_bps < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "run.early_exit.micro_breakout_fail_fast_1s_bps must not be negative",
            ));
        }

        if self.run.early_exit.micro_breakout_fail_fast_15s_bps < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "run.early_exit.micro_breakout_fail_fast_15s_bps must not be negative",
            ));
        }

        if self
            .run
            .early_exit
            .micro_breakout_fail_fast_profit_buffer_usdc
            < Decimal::ZERO
        {
            return Err(AppError::InvalidConfig(
                "run.early_exit.micro_breakout_fail_fast_profit_buffer_usdc must not be negative",
            ));
        }

        if self.run.early_exit.peak_exit_min_profit_usdc < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "run.early_exit.peak_exit_min_profit_usdc must not be negative",
            ));
        }

        if self.run.early_exit.peak_exit_min_primary_ask_price < Decimal::ZERO
            || self.run.early_exit.peak_exit_min_primary_ask_price > Decimal::ONE
        {
            return Err(AppError::InvalidConfig(
                "run.early_exit.peak_exit_min_primary_ask_price must be in range [0, 1]",
            ));
        }

        if self.run.early_exit.peak_exit_partial_close_ratio <= Decimal::ZERO
            || self.run.early_exit.peak_exit_partial_close_ratio >= Decimal::ONE
        {
            return Err(AppError::InvalidConfig(
                "run.early_exit.peak_exit_partial_close_ratio must be in range (0, 1)",
            ));
        }

        if self.run.early_exit.exhaustion_exit_min_profit_usdc < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "run.early_exit.exhaustion_exit_min_profit_usdc must not be negative",
            ));
        }

        if self.run.early_exit.directional_partial_close_ratio <= Decimal::ZERO
            || self.run.early_exit.directional_partial_close_ratio >= Decimal::ONE
        {
            return Err(AppError::InvalidConfig(
                "run.early_exit.directional_partial_close_ratio must be in range (0, 1)",
            ));
        }

        if self.run.early_exit.directional_partial_reversal_5s_bps < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "run.early_exit.directional_partial_reversal_5s_bps must not be negative",
            ));
        }

        if self.run.early_exit.directional_partial_reversal_15s_bps < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "run.early_exit.directional_partial_reversal_15s_bps must not be negative",
            ));
        }

        if self.run.early_exit.micro_breakout_partial_close_ratio <= Decimal::ZERO
            || self.run.early_exit.micro_breakout_partial_close_ratio >= Decimal::ONE
        {
            return Err(AppError::InvalidConfig(
                "run.early_exit.micro_breakout_partial_close_ratio must be in range (0, 1)",
            ));
        }

        if self.run.early_exit.micro_breakout_partial_reversal_5s_bps < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "run.early_exit.micro_breakout_partial_reversal_5s_bps must not be negative",
            ));
        }

        if self.run.early_exit.micro_breakout_partial_reversal_15s_bps < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "run.early_exit.micro_breakout_partial_reversal_15s_bps must not be negative",
            ));
        }

        if self.run.early_exit.stop_and_reverse_size_ratio <= Decimal::ZERO
            || self.run.early_exit.stop_and_reverse_size_ratio > Decimal::ONE
        {
            return Err(AppError::InvalidConfig(
                "run.early_exit.stop_and_reverse_size_ratio must be in range (0, 1]",
            ));
        }

        if self.run.early_exit.stop_and_reverse_min_seconds_left < 0 {
            return Err(AppError::InvalidConfig(
                "run.early_exit.stop_and_reverse_min_seconds_left must not be negative",
            ));
        }

        if self.run.early_exit.scalp_take_profit_price_delta < Decimal::ZERO
            || self.run.early_exit.scalp_take_profit_price_delta > Decimal::ONE
        {
            return Err(AppError::InvalidConfig(
                "run.early_exit.scalp_take_profit_price_delta must be in range [0, 1]",
            ));
        }

        if self.run.early_exit.scalp_stop_loss_price_delta < Decimal::ZERO
            || self.run.early_exit.scalp_stop_loss_price_delta > Decimal::ONE
        {
            return Err(AppError::InvalidConfig(
                "run.early_exit.scalp_stop_loss_price_delta must be in range [0, 1]",
            ));
        }

        if self.run.early_exit.scalp_time_stop_secs < 0 {
            return Err(AppError::InvalidConfig(
                "run.early_exit.scalp_time_stop_secs must not be negative",
            ));
        }

        if self.run.early_exit.scalp_invalidation_min_loss_usdc < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "run.early_exit.scalp_invalidation_min_loss_usdc must not be negative",
            ));
        }

        if self.run.early_exit.scalp_invalidation_opposite_gap_bps < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "run.early_exit.scalp_invalidation_opposite_gap_bps must not be negative",
            ));
        }

        if self.run.early_exit.scalp_invalidation_opposite_5s_bps < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "run.early_exit.scalp_invalidation_opposite_5s_bps must not be negative",
            ));
        }

        if self.run.early_exit.scalp_exit_enabled
            && self.run.early_exit.scalp_take_profit_price_delta <= Decimal::ZERO
            && self.run.early_exit.scalp_stop_loss_price_delta <= Decimal::ZERO
            && self.run.early_exit.scalp_time_stop_secs <= 0
            && !self.run.early_exit.scalp_invalidation_exit_enabled
        {
            return Err(AppError::InvalidConfig(
                "run.early_exit scalp exit requires take-profit, stop-loss, time-stop, or signal invalidation",
            ));
        }

        if self.run.early_exit.near_expiry_secs < 0 {
            return Err(AppError::InvalidConfig(
                "run.early_exit.near_expiry_secs must not be negative",
            ));
        }

        if self.run.adaptive_regime.safe_max_bundle_cost <= Decimal::ZERO
            || self.run.adaptive_regime.safe_max_bundle_cost >= Decimal::from(2_u32)
        {
            return Err(AppError::InvalidConfig(
                "run.adaptive_regime.safe_max_bundle_cost должен быть в диапазоне (0, 2)",
            ));
        }

        if self.run.adaptive_regime.aggressive_max_bundle_cost <= Decimal::ZERO
            || self.run.adaptive_regime.aggressive_max_bundle_cost >= Decimal::from(2_u32)
        {
            return Err(AppError::InvalidConfig(
                "run.adaptive_regime.aggressive_max_bundle_cost должен быть в диапазоне (0, 2)",
            ));
        }

        if self.run.risk.max_daily_loss_usdc < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "run.risk.max_daily_loss_usdc не должен быть отрицательным",
            ));
        }

        if self.run.risk.max_session_loss_usdc < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "run.risk.max_session_loss_usdc не должен быть отрицательным",
            ));
        }
        if self.run.risk.max_open_notional_usdc < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "run.risk.max_open_notional_usdc must not be negative",
            ));
        }
        if self.run.risk.max_unrealized_loss_usdc < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "run.risk.max_unrealized_loss_usdc must not be negative",
            ));
        }
        if self.run.risk.apply_in_live_mode {
            return Err(AppError::InvalidConfig(
                "run.risk.apply_in_live_mode is disabled until live position reconciliation is implemented",
            ));
        }

        if self.run.risk.cooldown_cycles == 0
            && (self.run.risk.max_daily_loss_usdc > Decimal::ZERO
                || self.run.risk.max_session_loss_usdc > Decimal::ZERO
                || self.run.risk.max_open_notional_usdc > Decimal::ZERO
                || self.run.risk.max_unrealized_loss_usdc > Decimal::ZERO
                || self.run.risk.max_consecutive_losses > 0)
        {
            return Err(AppError::InvalidConfig(
                "run.risk.cooldown_cycles должен быть больше нуля, если risk-лимиты включены",
            ));
        }

        if self.run.pnl_ratchet.enabled {
            if self.run.pnl_ratchet.base_notional_usdc <= Decimal::ZERO
                || self.run.pnl_ratchet.protect_notional_usdc <= Decimal::ZERO
                || self.run.pnl_ratchet.profit_unlock_usdc < Decimal::ZERO
            {
                return Err(AppError::InvalidConfig(
                    "run.pnl_ratchet notionals must be positive and profit_unlock_usdc must be non-negative",
                ));
            }

            if self.run.pnl_ratchet.protect_notional_usdc > self.run.pnl_ratchet.base_notional_usdc
            {
                return Err(AppError::InvalidConfig(
                    "run.pnl_ratchet.protect_notional_usdc must be <= base_notional_usdc",
                ));
            }
        }

        if self.run.v4_inventory.enabled {
            if self.run.v4_inventory.max_gross_inventory_shares_per_window <= Decimal::ZERO {
                return Err(AppError::InvalidConfig(
                    "run.v4_inventory.max_gross_inventory_shares_per_window must be positive",
                ));
            }

            if self
                .run
                .v4_inventory
                .max_directional_delta_shares_per_window
                <= Decimal::ZERO
            {
                return Err(AppError::InvalidConfig(
                    "run.v4_inventory.max_directional_delta_shares_per_window must be positive",
                ));
            }

            if self.run.v4_inventory.cooldown_secs <= 0 {
                return Err(AppError::InvalidConfig(
                    "run.v4_inventory.cooldown_secs must be positive when v4 inventory overlay is enabled",
                ));
            }

            if self.run.v4_inventory.max_window_spent_usdc < Decimal::ZERO {
                return Err(AppError::InvalidConfig(
                    "run.v4_inventory.max_window_spent_usdc must not be negative",
                ));
            }
        }

        if self.strategy.max_bundle_notional_usdc <= Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "strategy.max_bundle_notional_usdc должен быть положительным",
            ));
        }

        if self.strategy.max_directional_notional_usdc <= Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "strategy.max_directional_notional_usdc должен быть положительным",
            ));
        }

        if self.strategy.directional_soft_entry_min_notional_usdc <= Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "strategy.directional_soft_entry_min_notional_usdc должен быть положительным",
            ));
        }

        if self.strategy.directional_soft_entry_max_notional_usdc <= Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "strategy.directional_soft_entry_max_notional_usdc должен быть положительным",
            ));
        }

        if self.strategy.directional_soft_entry_min_notional_usdc
            > self.strategy.directional_soft_entry_max_notional_usdc
        {
            return Err(AppError::InvalidConfig(
                "strategy.directional_soft_entry_min_notional_usdc должен быть не больше strategy.directional_soft_entry_max_notional_usdc",
            ));
        }

        if self.strategy.directional_soft_entry_max_notional_usdc
            > self.strategy.max_directional_notional_usdc
        {
            return Err(AppError::InvalidConfig(
                "strategy.directional_soft_entry_max_notional_usdc должен быть не больше strategy.max_directional_notional_usdc",
            ));
        }

        if self.strategy.micro_breakout_min_spot_move_5s_bps < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "strategy.micro_breakout_min_spot_move_5s_bps должен быть неотрицательным",
            ));
        }

        if self.strategy.micro_breakout_target_cross_min_gap_bps < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "strategy.micro_breakout_target_cross_min_gap_bps должен быть неотрицательным",
            ));
        }

        if self.strategy.micro_breakout_target_cross_signal_boost_bps < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "strategy.micro_breakout_target_cross_signal_boost_bps должен быть неотрицательным",
            ));
        }

        if self.strategy.micro_breakout_weak_notional_usdc < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "strategy.micro_breakout_weak_notional_usdc не должен быть отрицательным",
            ));
        }

        if self.strategy.micro_breakout_normal_notional_usdc
            < self.strategy.micro_breakout_weak_notional_usdc
        {
            return Err(AppError::InvalidConfig(
                "strategy.micro_breakout_normal_notional_usdc должен быть не меньше strategy.micro_breakout_weak_notional_usdc",
            ));
        }

        if self.strategy.micro_breakout_normal_notional_usdc
            > self.strategy.max_directional_notional_usdc
        {
            return Err(AppError::InvalidConfig(
                "strategy.micro_breakout_normal_notional_usdc должен быть не больше strategy.max_directional_notional_usdc",
            ));
        }

        if self.strategy.micro_breakout_strong_notional_usdc
            < self.strategy.micro_breakout_normal_notional_usdc
        {
            return Err(AppError::InvalidConfig(
                "strategy.micro_breakout_strong_notional_usdc должен быть не меньше strategy.micro_breakout_normal_notional_usdc",
            ));
        }

        if self.strategy.micro_breakout_strong_notional_usdc
            > self.strategy.max_directional_notional_usdc
        {
            return Err(AppError::InvalidConfig(
                "strategy.micro_breakout_strong_notional_usdc должен быть не больше strategy.max_directional_notional_usdc",
            ));
        }

        if self.strategy.target_state_normal_notional_usdc <= Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "strategy.target_state_normal_notional_usdc must be positive",
            ));
        }

        if self.strategy.target_state_strong_notional_usdc
            < self.strategy.target_state_normal_notional_usdc
        {
            return Err(AppError::InvalidConfig(
                "strategy.target_state_strong_notional_usdc must not be smaller than strategy.target_state_normal_notional_usdc",
            ));
        }

        if self.strategy.target_state_strong_notional_usdc
            > self.strategy.max_directional_notional_usdc
        {
            return Err(AppError::InvalidConfig(
                "strategy.target_state_strong_notional_usdc must not exceed strategy.max_directional_notional_usdc",
            ));
        }

        if self.strategy.target_state_strong_gap_bps < self.strategy.target_state_min_target_gap_bps
        {
            return Err(AppError::InvalidConfig(
                "strategy.target_state_strong_gap_bps must not be smaller than strategy.target_state_min_target_gap_bps",
            ));
        }

        if self.strategy.micro_breakout_expensive_entry_price <= Decimal::ZERO
            || self.strategy.micro_breakout_expensive_entry_price >= Decimal::ONE
        {
            return Err(AppError::InvalidConfig(
                "strategy.micro_breakout_expensive_entry_price должен быть в диапазоне (0, 1)",
            ));
        }

        if self.strategy.micro_breakout_full_size_max_entry_price <= Decimal::ZERO
            || self.strategy.micro_breakout_full_size_max_entry_price >= Decimal::ONE
        {
            return Err(AppError::InvalidConfig(
                "strategy.micro_breakout_full_size_max_entry_price должен быть в диапазоне (0, 1)",
            ));
        }

        if self.strategy.micro_breakout_full_size_max_entry_price
            > self.strategy.micro_breakout_expensive_entry_price
        {
            return Err(AppError::InvalidConfig(
                "strategy.micro_breakout_full_size_max_entry_price должен быть не больше strategy.micro_breakout_expensive_entry_price",
            ));
        }

        if self
            .strategy
            .micro_breakout_strong_signal_min_spot_move_5s_bps
            < Decimal::ZERO
        {
            return Err(AppError::InvalidConfig(
                "strategy.micro_breakout_strong_signal_min_spot_move_5s_bps должен быть неотрицательным",
            ));
        }

        if self
            .strategy
            .micro_breakout_strong_signal_min_spot_move_15s_bps
            < Decimal::ZERO
        {
            return Err(AppError::InvalidConfig(
                "strategy.micro_breakout_strong_signal_min_spot_move_15s_bps должен быть неотрицательным",
            ));
        }

        if self.strategy.micro_breakout_min_elapsed_window_secs < 0
            || self.strategy.micro_breakout_min_elapsed_window_secs > 300
        {
            return Err(AppError::InvalidConfig(
                "strategy.micro_breakout_min_elapsed_window_secs должен быть в диапазоне [0, 300]",
            ));
        }

        if self.strategy.target_state_min_elapsed_window_secs < 0
            || self.strategy.target_state_min_elapsed_window_secs > 300
        {
            return Err(AppError::InvalidConfig(
                "strategy.target_state_min_elapsed_window_secs must be in range [0, 300]",
            ));
        }

        if self.strategy.target_state_max_seconds_left < 0
            || self.strategy.target_state_max_seconds_left > self.strategy.max_seconds_left
        {
            return Err(AppError::InvalidConfig(
                "strategy.target_state_max_seconds_left must be in range [0, strategy.max_seconds_left]",
            ));
        }

        if self.strategy.bonereaper_state_min_elapsed_window_secs < 0
            || self.strategy.bonereaper_state_min_elapsed_window_secs > 300
        {
            return Err(AppError::InvalidConfig(
                "strategy.bonereaper_state_min_elapsed_window_secs must be in range [0, 300]",
            ));
        }

        if self.strategy.bonereaper_state_max_seconds_left < 0
            || self.strategy.bonereaper_state_max_seconds_left > self.strategy.max_seconds_left
        {
            return Err(AppError::InvalidConfig(
                "strategy.bonereaper_state_max_seconds_left must be in range [0, strategy.max_seconds_left]",
            ));
        }

        if self.strategy.bonereaper_state_min_target_gap_bps < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "strategy.bonereaper_state_min_target_gap_bps must be non-negative",
            ));
        }

        if self.strategy.bonereaper_state_min_spot_move_15s_bps < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "strategy.bonereaper_state_min_spot_move_15s_bps must be non-negative",
            ));
        }

        if self.strategy.bonereaper_state_min_spot_move_5s_bps < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "strategy.bonereaper_state_min_spot_move_5s_bps must be non-negative",
            ));
        }

        if self.strategy.bonereaper_state_max_entry_price <= Decimal::ZERO
            || self.strategy.bonereaper_state_max_entry_price >= Decimal::ONE
        {
            return Err(AppError::InvalidConfig(
                "strategy.bonereaper_state_max_entry_price must be in range (0, 1)",
            ));
        }

        if self.strategy.bonereaper_state_v2_min_elapsed_window_secs < 0
            || self.strategy.bonereaper_state_v2_min_elapsed_window_secs > 300
        {
            return Err(AppError::InvalidConfig(
                "strategy.bonereaper_state_v2_min_elapsed_window_secs must be in range [0, 300]",
            ));
        }

        if self.strategy.bonereaper_state_v2_max_seconds_left < 0
            || self.strategy.bonereaper_state_v2_max_seconds_left > self.strategy.max_seconds_left
        {
            return Err(AppError::InvalidConfig(
                "strategy.bonereaper_state_v2_max_seconds_left must be in range [0, strategy.max_seconds_left]",
            ));
        }

        if self.strategy.bonereaper_state_v2_min_seconds_left < 0
            || self.strategy.bonereaper_state_v2_min_seconds_left
                > self.strategy.bonereaper_state_v2_max_seconds_left
        {
            return Err(AppError::InvalidConfig(
                "strategy.bonereaper_state_v2_min_seconds_left must be in range [0, strategy.bonereaper_state_v2_max_seconds_left]",
            ));
        }

        if self.strategy.bonereaper_state_v2_bias_min_target_gap_bps < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "strategy.bonereaper_state_v2_bias_min_target_gap_bps must be non-negative",
            ));
        }

        if self.strategy.bonereaper_state_v2_flip_max_target_gap_bps < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "strategy.bonereaper_state_v2_flip_max_target_gap_bps must be non-negative",
            ));
        }

        if self.strategy.bonereaper_state_v2_min_spot_move_15s_bps < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "strategy.bonereaper_state_v2_min_spot_move_15s_bps must be non-negative",
            ));
        }

        if self.strategy.bonereaper_state_v2_min_spot_move_5s_bps < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "strategy.bonereaper_state_v2_min_spot_move_5s_bps must be non-negative",
            ));
        }

        if self.strategy.bonereaper_state_v2_min_aligned_flow_bps < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "strategy.bonereaper_state_v2_min_aligned_flow_bps must be non-negative",
            ));
        }

        if self.strategy.bonereaper_state_v2_max_entry_price <= Decimal::ZERO
            || self.strategy.bonereaper_state_v2_max_entry_price >= Decimal::ONE
        {
            return Err(AppError::InvalidConfig(
                "strategy.bonereaper_state_v2_max_entry_price must be in range (0, 1)",
            ));
        }

        if self.strategy.bonereaper_state_v2_max_fair_price <= Decimal::ZERO
            || self.strategy.bonereaper_state_v2_max_fair_price >= Decimal::ONE
        {
            return Err(AppError::InvalidConfig(
                "strategy.bonereaper_state_v2_max_fair_price must be in range (0, 1)",
            ));
        }

        if self.strategy.bonereaper_state_v2_probe_notional_usdc < Decimal::ZERO
            || self.strategy.bonereaper_state_v2_normal_notional_usdc < Decimal::ZERO
            || self.strategy.bonereaper_state_v2_strong_notional_usdc < Decimal::ZERO
        {
            return Err(AppError::InvalidConfig(
                "strategy.bonereaper_state_v2 notionals must be non-negative",
            ));
        }

        if self.strategy.bonereaper_state_v2_strong_gap_bps < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "strategy.bonereaper_state_v2_strong_gap_bps must be non-negative",
            ));
        }

        if self.strategy.bonereaper_state_v2_strong_flow_bps < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "strategy.bonereaper_state_v2_strong_flow_bps must be non-negative",
            ));
        }

        if self.strategy.bonereaper_state_v2_min_expected_profit_usdc < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "strategy.bonereaper_state_v2_min_expected_profit_usdc must be non-negative",
            ));
        }

        if self.strategy.bonereaper_state_v2_max_counter_1s_bps < Decimal::ZERO
            || self.strategy.bonereaper_state_v2_max_counter_5s_bps < Decimal::ZERO
        {
            return Err(AppError::InvalidConfig(
                "strategy.bonereaper_state_v2 counter-move limits must be non-negative",
            ));
        }

        if self
            .strategy
            .bonereaper_state_v2_early_window_max_seconds_left
            < 0
            || self
                .strategy
                .bonereaper_state_v2_early_window_max_seconds_left
                > self.strategy.bonereaper_state_v2_max_seconds_left
        {
            return Err(AppError::InvalidConfig(
                "strategy.bonereaper_state_v2_early_window_max_seconds_left must be in range [0, strategy.bonereaper_state_v2_max_seconds_left]",
            ));
        }

        if self.strategy.bonereaper_state_v2_early_window_min_fresh_bps < Decimal::ZERO
            || self.strategy.bonereaper_state_v2_early_window_min_swing_bps < Decimal::ZERO
            || self
                .strategy
                .bonereaper_state_v2_early_window_min_signal_bps
                < Decimal::ZERO
        {
            return Err(AppError::InvalidConfig(
                "strategy.bonereaper_state_v2 early-window thresholds must be non-negative",
            ));
        }

        if self
            .strategy
            .bonereaper_state_v2_high_gap_min_target_gap_bps
            < Decimal::ZERO
            || self.strategy.bonereaper_state_v2_high_gap_min_fresh_bps < Decimal::ZERO
            || self.strategy.bonereaper_state_v2_high_gap_min_swing_bps < Decimal::ZERO
            || self.strategy.bonereaper_state_v2_high_gap_min_signal_bps < Decimal::ZERO
        {
            return Err(AppError::InvalidConfig(
                "strategy.bonereaper_state_v2 high-gap thresholds must be non-negative",
            ));
        }

        if self.strategy.bonereaper_state_v2_high_gap_max_entry_price <= Decimal::ZERO
            || self.strategy.bonereaper_state_v2_high_gap_max_entry_price >= Decimal::ONE
        {
            return Err(AppError::InvalidConfig(
                "strategy.bonereaper_state_v2_high_gap_max_entry_price must be in range (0, 1)",
            ));
        }

        if self.strategy.bonereaper_state_v2_mid_gap_min_target_gap_bps < Decimal::ZERO
            || self.strategy.bonereaper_state_v2_mid_gap_max_target_gap_bps < Decimal::ZERO
            || self.strategy.bonereaper_state_v2_mid_gap_min_fresh_bps < Decimal::ZERO
            || self.strategy.bonereaper_state_v2_mid_gap_min_signal_bps < Decimal::ZERO
            || self.strategy.bonereaper_state_v2_mid_gap_min_flow_bps < Decimal::ZERO
        {
            return Err(AppError::InvalidConfig(
                "strategy.bonereaper_state_v2 mid-gap thresholds must be non-negative",
            ));
        }

        if self.strategy.bonereaper_state_v2_mid_gap_guard_enabled
            && self.strategy.bonereaper_state_v2_mid_gap_max_target_gap_bps
                <= self.strategy.bonereaper_state_v2_mid_gap_min_target_gap_bps
        {
            return Err(AppError::InvalidConfig(
                "strategy.bonereaper_state_v2_mid_gap_max_target_gap_bps must be greater than strategy.bonereaper_state_v2_mid_gap_min_target_gap_bps",
            ));
        }

        if self.strategy.bonereaper_state_v2_mid_gap_max_entry_price <= Decimal::ZERO
            || self.strategy.bonereaper_state_v2_mid_gap_max_entry_price >= Decimal::ONE
        {
            return Err(AppError::InvalidConfig(
                "strategy.bonereaper_state_v2_mid_gap_max_entry_price must be in range (0, 1)",
            ));
        }

        if self.strategy.bonereaper_state_v2_mid_gap_min_seconds_left < 0
            || self.strategy.bonereaper_state_v2_mid_gap_min_seconds_left
                > self.strategy.bonereaper_state_v2_max_seconds_left
        {
            return Err(AppError::InvalidConfig(
                "strategy.bonereaper_state_v2_mid_gap_min_seconds_left must be in range [0, strategy.bonereaper_state_v2_max_seconds_left]",
            ));
        }

        if self.strategy.bonereaper_state_v2_low_gap_max_target_gap_bps < Decimal::ZERO
            || self
                .strategy
                .bonereaper_state_v2_low_gap_allow_min_fresh_bps
                < Decimal::ZERO
            || self
                .strategy
                .bonereaper_state_v2_low_gap_allow_min_signal_bps
                < Decimal::ZERO
            || self.strategy.bonereaper_state_v2_low_gap_allow_min_flow_bps < Decimal::ZERO
        {
            return Err(AppError::InvalidConfig(
                "strategy.bonereaper_state_v2 low-gap thresholds must be non-negative",
            ));
        }

        if self.strategy.bonereaper_state_v2_low_gap_guard_enabled
            && self.strategy.bonereaper_state_v2_low_gap_max_target_gap_bps <= Decimal::ZERO
        {
            return Err(AppError::InvalidConfig(
                "strategy.bonereaper_state_v2_low_gap_max_target_gap_bps must be positive when the guard is enabled",
            ));
        }

        if self.strategy.bonereaper_state_v2_low_gap_max_entry_price <= Decimal::ZERO
            || self.strategy.bonereaper_state_v2_low_gap_max_entry_price >= Decimal::ONE
        {
            return Err(AppError::InvalidConfig(
                "strategy.bonereaper_state_v2_low_gap_max_entry_price must be in range (0, 1)",
            ));
        }

        if self.strategy.bonereaper_state_v2_low_gap_min_seconds_left < 0
            || self.strategy.bonereaper_state_v2_low_gap_min_seconds_left
                > self.strategy.bonereaper_state_v2_max_seconds_left
        {
            return Err(AppError::InvalidConfig(
                "strategy.bonereaper_state_v2_low_gap_min_seconds_left must be in range [0, strategy.bonereaper_state_v2_max_seconds_left]",
            ));
        }

        if self
            .strategy
            .bonereaper_state_v2_early_expensive_min_seconds_left
            < 0
            || self
                .strategy
                .bonereaper_state_v2_early_expensive_min_seconds_left
                > self.strategy.bonereaper_state_v2_max_seconds_left
        {
            return Err(AppError::InvalidConfig(
                "strategy.bonereaper_state_v2_early_expensive_min_seconds_left must be in range [0, strategy.bonereaper_state_v2_max_seconds_left]",
            ));
        }

        if self
            .strategy
            .bonereaper_state_v2_early_expensive_entry_price
            <= Decimal::ZERO
            || self
                .strategy
                .bonereaper_state_v2_early_expensive_entry_price
                >= Decimal::ONE
        {
            return Err(AppError::InvalidConfig(
                "strategy.bonereaper_state_v2_early_expensive_entry_price must be in range (0, 1)",
            ));
        }

        if self
            .strategy
            .bonereaper_state_v2_early_expensive_allow_min_target_gap_bps
            < Decimal::ZERO
            || self
                .strategy
                .bonereaper_state_v2_early_expensive_allow_min_fresh_bps
                < Decimal::ZERO
            || self
                .strategy
                .bonereaper_state_v2_early_expensive_allow_min_signal_bps
                < Decimal::ZERO
            || self
                .strategy
                .bonereaper_state_v2_early_expensive_allow_min_flow_bps
                < Decimal::ZERO
        {
            return Err(AppError::InvalidConfig(
                "strategy.bonereaper_state_v2 early-expensive thresholds must be non-negative",
            ));
        }

        if self.strategy.enable_codex_sentinel_v1 {
            self.validate_codex_sentinel_v1()?;
        }

        if self.strategy.enable_codex_scalp_probe_v1 {
            self.validate_codex_scalp_probe_v1()?;
        }

        if self.strategy.directional_strong_signal_min_spot_move_5s_bps < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "strategy.directional_strong_signal_min_spot_move_5s_bps должен быть неотрицательным",
            ));
        }

        if self.strategy.directional_strong_signal_min_trade_flow_bps < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "strategy.directional_strong_signal_min_trade_flow_bps должен быть неотрицательным",
            ));
        }

        if self.strategy.directional_soft_entry_signal_window_bps < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "strategy.directional_soft_entry_signal_window_bps должен быть неотрицательным",
            ));
        }

        if self.strategy.min_seconds_left < 0
            || self.strategy.max_seconds_left <= self.strategy.min_seconds_left
        {
            return Err(AppError::InvalidConfig(
                "strategy.max_seconds_left должен быть больше strategy.min_seconds_left",
            ));
        }

        let largest_trade_limit = self
            .strategy
            .max_bundle_notional_usdc
            .max(self.strategy.max_directional_notional_usdc);
        if self.strategy.max_market_notional_usdc < largest_trade_limit {
            return Err(AppError::InvalidConfig(
                "strategy.max_market_notional_usdc должен быть не меньше максимального размера одной сделки",
            ));
        }

        if self.strategy.directional_max_entry_price <= Decimal::ZERO
            || self.strategy.directional_max_entry_price >= Decimal::ONE
        {
            return Err(AppError::InvalidConfig(
                "strategy.directional_max_entry_price должен быть в диапазоне (0, 1)",
            ));
        }

        if self.strategy.directional_max_fair_price <= Decimal::new(50, 2)
            || self.strategy.directional_max_fair_price > Decimal::ONE
        {
            return Err(AppError::InvalidConfig(
                "strategy.directional_max_fair_price должен быть в диапазоне (0.50, 1.00]",
            ));
        }

        if self.strategy.directional_projection_cap_multiplier < Decimal::ONE {
            return Err(AppError::InvalidConfig(
                "strategy.directional_projection_cap_multiplier должен быть не меньше 1",
            ));
        }

        if self.strategy.directional_trade_flow_weight < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "strategy.directional_trade_flow_weight должен быть неотрицательным",
            ));
        }

        if self.strategy.directional_execution_slippage_bps > 2_500 {
            return Err(AppError::InvalidConfig(
                "strategy.directional_execution_slippage_bps не должен превышать 2500 bps",
            ));
        }

        if self.strategy.tail_hedge_ratio < Decimal::ZERO
            || self.strategy.tail_hedge_ratio > Decimal::ONE
        {
            return Err(AppError::InvalidConfig(
                "strategy.tail_hedge_ratio должен быть в диапазоне [0, 1]",
            ));
        }

        if self.strategy.tail_hedge_max_opposite_price <= Decimal::ZERO
            || self.strategy.tail_hedge_max_opposite_price >= Decimal::ONE
        {
            return Err(AppError::InvalidConfig(
                "strategy.tail_hedge_max_opposite_price должен быть в диапазоне (0, 1)",
            ));
        }

        if self.strategy.tail_hedge_max_bundle_cost <= Decimal::ZERO
            || self.strategy.tail_hedge_max_bundle_cost >= Decimal::from(2_u32)
        {
            return Err(AppError::InvalidConfig(
                "strategy.tail_hedge_max_bundle_cost должен быть в диапазоне (0, 2)",
            ));
        }

        if self.strategy.tail_hedge_open_window_secs < 0
            || self.strategy.tail_hedge_open_window_secs > 300
        {
            return Err(AppError::InvalidConfig(
                "strategy.tail_hedge_open_window_secs должен быть в диапазоне [0, 300]",
            ));
        }

        Ok(())
    }

    fn validate_codex_sentinel_v1(&self) -> Result<()> {
        if self
            .strategy
            .codex_sentinel_v1_stale_micro_max_confirmation_bps
            < Decimal::ZERO
            || self.strategy.codex_sentinel_v1_stale_micro_min_signal_bps < Decimal::ZERO
            || self.strategy.codex_sentinel_v1_stale_micro_min_flow_bps < Decimal::ZERO
            || self.strategy.codex_sentinel_v1_stale_micro_min_swing_bps < Decimal::ZERO
            || self
                .strategy
                .codex_sentinel_v1_stale_micro_min_target_gap_bps
                < Decimal::ZERO
            || self
                .strategy
                .codex_sentinel_v1_stale_micro_discount_min_signal_bps
                < Decimal::ZERO
            || self
                .strategy
                .codex_sentinel_v1_stale_micro_discount_min_flow_bps
                < Decimal::ZERO
        {
            return Err(AppError::InvalidConfig(
                "strategy.codex_sentinel_v1 stale-micro thresholds must be non-negative",
            ));
        }

        if self
            .strategy
            .codex_sentinel_v1_stale_micro_discount_max_entry_price
            <= Decimal::ZERO
            || self
                .strategy
                .codex_sentinel_v1_stale_micro_discount_max_entry_price
                >= Decimal::ONE
            || self
                .strategy
                .codex_sentinel_v1_stale_micro_max_non_discount_entry_price
                <= Decimal::ZERO
            || self
                .strategy
                .codex_sentinel_v1_stale_micro_max_non_discount_entry_price
                >= Decimal::ONE
        {
            return Err(AppError::InvalidConfig(
                "strategy.codex_sentinel_v1 stale-micro entry prices must be in range (0, 1)",
            ));
        }

        if self.strategy.codex_sentinel_v1_expensive_min_micro_bps < Decimal::ZERO
            || self.strategy.codex_sentinel_v1_expensive_min_swing_bps < Decimal::ZERO
        {
            return Err(AppError::InvalidConfig(
                "strategy.codex_sentinel_v1 expensive-entry thresholds must be non-negative",
            ));
        }

        if self.strategy.codex_sentinel_v1_expensive_entry_price <= Decimal::ZERO
            || self.strategy.codex_sentinel_v1_expensive_entry_price >= Decimal::ONE
        {
            return Err(AppError::InvalidConfig(
                "strategy.codex_sentinel_v1_expensive_entry_price must be in range (0, 1)",
            ));
        }

        if self.strategy.codex_sentinel_v1_premium_entry_price <= Decimal::ZERO
            || self.strategy.codex_sentinel_v1_premium_entry_price >= Decimal::ONE
        {
            return Err(AppError::InvalidConfig(
                "strategy.codex_sentinel_v1_premium_entry_price must be in range (0, 1)",
            ));
        }

        if self.strategy.codex_sentinel_v1_premium_min_signal_bps < Decimal::ZERO
            || self.strategy.codex_sentinel_v1_premium_min_flow_bps < Decimal::ZERO
            || self.strategy.codex_sentinel_v1_premium_min_fresh_bps < Decimal::ZERO
        {
            return Err(AppError::InvalidConfig(
                "strategy.codex_sentinel_v1 premium-entry thresholds must be non-negative",
            ));
        }

        if self.strategy.codex_sentinel_v1_max_live_quote_age_ms < 0 {
            return Err(AppError::InvalidConfig(
                "strategy.codex_sentinel_v1_max_live_quote_age_ms must not be negative",
            ));
        }

        if self
            .strategy
            .codex_sentinel_v1_aggressive_continuation_max_entry_price
            <= Decimal::ZERO
            || self
                .strategy
                .codex_sentinel_v1_aggressive_continuation_max_entry_price
                >= Decimal::ONE
        {
            return Err(AppError::InvalidConfig(
                "strategy.codex_sentinel_v1_aggressive_continuation_max_entry_price must be in range (0, 1)",
            ));
        }

        if self
            .strategy
            .codex_sentinel_v1_aggressive_continuation_min_target_gap_bps
            < Decimal::ZERO
            || self
                .strategy
                .codex_sentinel_v1_aggressive_continuation_min_signal_bps
                < Decimal::ZERO
            || self
                .strategy
                .codex_sentinel_v1_aggressive_continuation_min_flow_bps
                < Decimal::ZERO
            || self
                .strategy
                .codex_sentinel_v1_aggressive_continuation_min_fresh_bps
                < Decimal::ZERO
            || self
                .strategy
                .codex_sentinel_v1_aggressive_continuation_min_swing_bps
                < Decimal::ZERO
        {
            return Err(AppError::InvalidConfig(
                "strategy.codex_sentinel_v1 aggressive-continuation thresholds must be non-negative",
            ));
        }

        if self
            .strategy
            .codex_sentinel_v1_aggressive_continuation_max_quote_age_ms
            < 0
        {
            return Err(AppError::InvalidConfig(
                "strategy.codex_sentinel_v1_aggressive_continuation_max_quote_age_ms must not be negative",
            ));
        }

        if self.strategy.codex_breakout_v1_max_entry_price <= Decimal::ZERO
            || self.strategy.codex_breakout_v1_max_entry_price >= Decimal::ONE
        {
            return Err(AppError::InvalidConfig(
                "strategy.codex_breakout_v1_max_entry_price must be in range (0, 1)",
            ));
        }

        if self.strategy.codex_breakout_v1_max_book_age_ms < 0 {
            return Err(AppError::InvalidConfig(
                "strategy.codex_breakout_v1_max_book_age_ms must not be negative",
            ));
        }

        if self.strategy.codex_breakout_v1_required && !self.strategy.codex_breakout_v1_enabled {
            return Err(AppError::InvalidConfig(
                "strategy.codex_breakout_v1_required requires codex_breakout_v1_enabled",
            ));
        }

        if self.strategy.codex_breakout_v1_max_spread_bps < Decimal::ZERO
            || self.strategy.codex_breakout_v1_min_score_bps < Decimal::ZERO
            || self.strategy.codex_breakout_v1_min_depth_imbalance_bps < Decimal::ZERO
            || self.strategy.codex_breakout_v1_min_microprice_bps < Decimal::ZERO
            || self.strategy.codex_breakout_v1_min_fresh_bps < Decimal::ZERO
            || self.strategy.codex_breakout_v1_min_target_gap_bps < Decimal::ZERO
            || self.strategy.codex_breakout_v1_min_signal_bps < Decimal::ZERO
            || self.strategy.codex_breakout_v1_min_flow_bps < Decimal::ZERO
        {
            return Err(AppError::InvalidConfig(
                "strategy.codex_breakout_v1 thresholds must be non-negative",
            ));
        }

        if self
            .strategy
            .codex_sentinel_v1_discount_value_max_entry_price
            <= Decimal::ZERO
            || self
                .strategy
                .codex_sentinel_v1_discount_value_max_entry_price
                >= Decimal::ONE
        {
            return Err(AppError::InvalidConfig(
                "strategy.codex_sentinel_v1_discount_value_max_entry_price must be in range (0, 1)",
            ));
        }

        if self
            .strategy
            .codex_sentinel_v1_discount_value_max_book_age_ms
            < 0
        {
            return Err(AppError::InvalidConfig(
                "strategy.codex_sentinel_v1_discount_value_max_book_age_ms must not be negative",
            ));
        }

        if self
            .strategy
            .codex_sentinel_v1_discount_value_max_exchange_spread_bps
            < Decimal::ZERO
            || self
                .strategy
                .codex_sentinel_v1_discount_value_min_target_gap_bps
                < Decimal::ZERO
            || self.strategy.codex_sentinel_v1_discount_value_min_fresh_bps < Decimal::ZERO
            || self.strategy.codex_sentinel_v1_discount_value_min_swing_bps < Decimal::ZERO
            || self
                .strategy
                .codex_sentinel_v1_discount_value_min_signal_bps
                < Decimal::ZERO
            || self.strategy.codex_sentinel_v1_discount_value_min_flow_bps < Decimal::ZERO
            || self
                .strategy
                .codex_sentinel_v1_discount_value_min_top_imbalance_bps
                < Decimal::ZERO
            || self
                .strategy
                .codex_sentinel_v1_discount_value_min_depth_imbalance_bps
                < Decimal::ZERO
            || self
                .strategy
                .codex_sentinel_v1_discount_value_min_microprice_bps
                < Decimal::ZERO
        {
            return Err(AppError::InvalidConfig(
                "strategy.codex_sentinel_v1 discount-value thresholds must be non-negative",
            ));
        }

        if self.strategy.codex_sentinel_v1_max_entry_spread <= Decimal::ZERO
            || self.strategy.codex_sentinel_v1_max_entry_spread >= Decimal::ONE
        {
            return Err(AppError::InvalidConfig(
                "strategy.codex_sentinel_v1_max_entry_spread must be in range (0, 1)",
            ));
        }

        if self.strategy.codex_sentinel_v1_no_chase_entry_price <= Decimal::ZERO
            || self.strategy.codex_sentinel_v1_no_chase_entry_price >= Decimal::ONE
        {
            return Err(AppError::InvalidConfig(
                "strategy.codex_sentinel_v1_no_chase_entry_price must be in range (0, 1)",
            ));
        }

        if self.strategy.codex_sentinel_v1_no_chase_min_seconds_left < 0
            || self.strategy.codex_sentinel_v1_no_chase_min_seconds_left
                > self.strategy.bonereaper_state_v2_max_seconds_left
        {
            return Err(AppError::InvalidConfig(
                "strategy.codex_sentinel_v1_no_chase_min_seconds_left must be in range [0, bonereaper_state_v2_max_seconds_left]",
            ));
        }

        if self
            .strategy
            .codex_sentinel_v1_no_chase_allow_min_target_gap_bps
            < Decimal::ZERO
            || self.strategy.codex_sentinel_v1_no_chase_allow_min_fresh_bps < Decimal::ZERO
            || self
                .strategy
                .codex_sentinel_v1_no_chase_allow_min_signal_bps
                < Decimal::ZERO
            || self.strategy.codex_sentinel_v1_no_chase_allow_min_flow_bps < Decimal::ZERO
        {
            return Err(AppError::InvalidConfig(
                "strategy.codex_sentinel_v1 no-chase thresholds must be non-negative",
            ));
        }

        if self
            .strategy
            .codex_sentinel_v1_quality_floor_min_target_gap_bps
            < Decimal::ZERO
            || self
                .strategy
                .codex_sentinel_v1_quality_floor_mid_gap_max_bps
                < Decimal::ZERO
            || self
                .strategy
                .codex_sentinel_v1_quality_floor_mid_gap_min_signal_bps
                < Decimal::ZERO
            || self
                .strategy
                .codex_sentinel_v1_quality_floor_mid_gap_min_flow_bps
                < Decimal::ZERO
        {
            return Err(AppError::InvalidConfig(
                "strategy.codex_sentinel_v1 quality-floor thresholds must be non-negative",
            ));
        }

        if self.strategy.codex_sentinel_v1_mid_gap_premium_entry_price <= Decimal::ZERO
            || self.strategy.codex_sentinel_v1_mid_gap_premium_entry_price >= Decimal::ONE
        {
            return Err(AppError::InvalidConfig(
                "strategy.codex_sentinel_v1_mid_gap_premium_entry_price must be in range (0, 1)",
            ));
        }

        if self
            .strategy
            .codex_sentinel_v1_mid_gap_premium_min_target_gap_bps
            < Decimal::ZERO
            || self
                .strategy
                .codex_sentinel_v1_mid_gap_premium_max_target_gap_bps
                < Decimal::ZERO
            || self
                .strategy
                .codex_sentinel_v1_mid_gap_premium_min_signal_bps
                < Decimal::ZERO
            || self.strategy.codex_sentinel_v1_mid_gap_premium_min_flow_bps < Decimal::ZERO
            || self
                .strategy
                .codex_sentinel_v1_mid_gap_premium_min_fresh_bps
                < Decimal::ZERO
        {
            return Err(AppError::InvalidConfig(
                "strategy.codex_sentinel_v1 mid-gap premium thresholds must be non-negative",
            ));
        }

        if self
            .strategy
            .codex_sentinel_v1_mid_gap_premium_guard_enabled
            && self
                .strategy
                .codex_sentinel_v1_mid_gap_premium_max_target_gap_bps
                < self
                    .strategy
                    .codex_sentinel_v1_mid_gap_premium_min_target_gap_bps
        {
            return Err(AppError::InvalidConfig(
                "strategy.codex_sentinel_v1_mid_gap_premium_max_target_gap_bps must be >= min_target_gap_bps when enabled",
            ));
        }

        if self.strategy.codex_sentinel_v1_attack_notional_usdc < Decimal::ZERO
            || self.strategy.codex_sentinel_v1_attack_min_signal_bps < Decimal::ZERO
            || self.strategy.codex_sentinel_v1_attack_min_flow_bps < Decimal::ZERO
            || self.strategy.codex_sentinel_v1_attack_min_confirmation_bps < Decimal::ZERO
        {
            return Err(AppError::InvalidConfig(
                "strategy.codex_sentinel_v1 attack-size thresholds must be non-negative",
            ));
        }

        if self.strategy.codex_sentinel_v1_attack_max_entry_price <= Decimal::ZERO
            || self.strategy.codex_sentinel_v1_attack_max_entry_price >= Decimal::ONE
        {
            return Err(AppError::InvalidConfig(
                "strategy.codex_sentinel_v1_attack_max_entry_price must be in range (0, 1)",
            ));
        }

        if self.strategy.codex_sentinel_v1_attack_size_enabled
            && self.strategy.codex_sentinel_v1_attack_notional_usdc <= Decimal::ZERO
        {
            return Err(AppError::InvalidConfig(
                "strategy.codex_sentinel_v1_attack_notional_usdc must be positive when attack sizing is enabled",
            ));
        }

        if self.strategy.codex_sentinel_v1_bad_window_min_score < Decimal::ZERO
            || self.strategy.codex_sentinel_v1_bad_window_min_score > Decimal::from(100_u32)
            || self.strategy.codex_sentinel_v1_confidence_min_score < Decimal::ZERO
            || self.strategy.codex_sentinel_v1_confidence_min_score > Decimal::from(100_u32)
        {
            return Err(AppError::InvalidConfig(
                "strategy.codex_sentinel_v1 confidence scores must be in range [0, 100]",
            ));
        }

        if self.strategy.codex_sentinel_v1_confidence_max_multiplier < Decimal::ONE {
            return Err(AppError::InvalidConfig(
                "strategy.codex_sentinel_v1_confidence_max_multiplier must be >= 1",
            ));
        }

        if self.strategy.codex_sentinel_v1_low_flow_max_flow_bps < Decimal::ZERO
            || self
                .strategy
                .codex_sentinel_v1_low_flow_allow_min_signal_bps
                < Decimal::ZERO
            || self.strategy.codex_sentinel_v1_low_flow_allow_min_fresh_bps < Decimal::ZERO
            || self.strategy.codex_sentinel_v1_low_flow_allow_min_swing_bps < Decimal::ZERO
        {
            return Err(AppError::InvalidConfig(
                "strategy.codex_sentinel_v1 low-flow thresholds must be non-negative",
            ));
        }

        if self
            .strategy
            .codex_sentinel_v1_low_flow_allow_max_entry_price
            <= Decimal::ZERO
            || self
                .strategy
                .codex_sentinel_v1_low_flow_allow_max_entry_price
                >= Decimal::ONE
        {
            return Err(AppError::InvalidConfig(
                "strategy.codex_sentinel_v1_low_flow_allow_max_entry_price must be in range (0, 1)",
            ));
        }

        if self.strategy.codex_sentinel_v1_counter_burst_min_bps < Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "strategy.codex_sentinel_v1_counter_burst_min_bps must be non-negative",
            ));
        }

        if self
            .strategy
            .codex_sentinel_v1_counter_burst_max_entry_price
            <= Decimal::ZERO
            || self
                .strategy
                .codex_sentinel_v1_counter_burst_max_entry_price
                >= Decimal::ONE
        {
            return Err(AppError::InvalidConfig(
                "strategy.codex_sentinel_v1_counter_burst_max_entry_price must be in range (0, 1)",
            ));
        }

        if self.strategy.codex_sentinel_v1_late_entry_max_entry_price <= Decimal::ZERO
            || self.strategy.codex_sentinel_v1_late_entry_max_entry_price >= Decimal::ONE
        {
            return Err(AppError::InvalidConfig(
                "strategy.codex_sentinel_v1_late_entry_max_entry_price must be in range (0, 1)",
            ));
        }

        if self.strategy.codex_sentinel_v1_late_entry_min_seconds_left < 0
            || self.strategy.codex_sentinel_v1_late_entry_min_seconds_left
                > self.strategy.bonereaper_state_v2_min_seconds_left
        {
            return Err(AppError::InvalidConfig(
                "strategy.codex_sentinel_v1_late_entry_min_seconds_left must be between 0 and strategy.bonereaper_state_v2_min_seconds_left",
            ));
        }

        if self.strategy.codex_sentinel_v1_late_entry_min_signal_bps < Decimal::ZERO
            || self.strategy.codex_sentinel_v1_late_entry_min_fresh_bps < Decimal::ZERO
            || self.strategy.codex_sentinel_v1_late_entry_min_flow_bps < Decimal::ZERO
            || self
                .strategy
                .codex_sentinel_v1_late_entry_min_target_gap_bps
                < Decimal::ZERO
        {
            return Err(AppError::InvalidConfig(
                "strategy.codex_sentinel_v1 late-entry thresholds must be non-negative",
            ));
        }

        if self.strategy.codex_sentinel_v1_late_window_max_entry_price <= Decimal::ZERO
            || self.strategy.codex_sentinel_v1_late_window_max_entry_price >= Decimal::ONE
        {
            return Err(AppError::InvalidConfig(
                "strategy.codex_sentinel_v1_late_window_max_entry_price must be in range (0, 1)",
            ));
        }

        if self.strategy.codex_sentinel_v1_late_window_max_seconds_left < 0
            || self.strategy.codex_sentinel_v1_late_window_max_seconds_left
                > self.strategy.bonereaper_state_v2_max_seconds_left
        {
            return Err(AppError::InvalidConfig(
                "strategy.codex_sentinel_v1_late_window_max_seconds_left must be in range [0, bonereaper_state_v2_max_seconds_left]",
            ));
        }

        if self
            .strategy
            .codex_sentinel_v1_late_window_allow_min_signal_bps
            < Decimal::ZERO
            || self
                .strategy
                .codex_sentinel_v1_late_window_allow_min_fresh_bps
                < Decimal::ZERO
            || self
                .strategy
                .codex_sentinel_v1_late_window_allow_min_flow_bps
                < Decimal::ZERO
            || self
                .strategy
                .codex_sentinel_v1_late_window_allow_min_target_gap_bps
                < Decimal::ZERO
        {
            return Err(AppError::InvalidConfig(
                "strategy.codex_sentinel_v1 late-window value thresholds must be non-negative",
            ));
        }

        Ok(())
    }

    fn validate_codex_scalp_probe_v1(&self) -> Result<()> {
        if self.strategy.codex_scalp_probe_v1_min_entry_price <= Decimal::ZERO
            || self.strategy.codex_scalp_probe_v1_min_entry_price >= Decimal::ONE
            || self.strategy.codex_scalp_probe_v1_max_entry_price <= Decimal::ZERO
            || self.strategy.codex_scalp_probe_v1_max_entry_price >= Decimal::ONE
            || self.strategy.codex_scalp_probe_v1_min_entry_price
                > self.strategy.codex_scalp_probe_v1_max_entry_price
        {
            return Err(AppError::InvalidConfig(
                "strategy.codex_scalp_probe_v1 entry price range must be within (0, 1) and min <= max",
            ));
        }

        if self.strategy.codex_scalp_probe_v1_max_entry_spread <= Decimal::ZERO
            || self.strategy.codex_scalp_probe_v1_max_entry_spread >= Decimal::ONE
        {
            return Err(AppError::InvalidConfig(
                "strategy.codex_scalp_probe_v1_max_entry_spread must be in range (0, 1)",
            ));
        }

        if self.strategy.codex_scalp_probe_v1_min_elapsed_window_secs < 0
            || self.strategy.codex_scalp_probe_v1_max_seconds_left < 0
            || self.strategy.codex_scalp_probe_v1_min_seconds_left < 0
            || self.strategy.codex_scalp_probe_v1_min_seconds_left
                > self.strategy.codex_scalp_probe_v1_max_seconds_left
        {
            return Err(AppError::InvalidConfig(
                "strategy.codex_scalp_probe_v1 timing must be non-negative and min_seconds_left <= max_seconds_left",
            ));
        }

        if self.strategy.codex_scalp_probe_v1_max_book_age_ms < 0 {
            return Err(AppError::InvalidConfig(
                "strategy.codex_scalp_probe_v1_max_book_age_ms must not be negative",
            ));
        }

        if self.strategy.codex_scalp_probe_v1_max_exchange_spread_bps < Decimal::ZERO
            || self.strategy.codex_scalp_probe_v1_min_target_gap_bps < Decimal::ZERO
            || self.strategy.codex_scalp_probe_v1_min_fresh_bps < Decimal::ZERO
            || self.strategy.codex_scalp_probe_v1_min_signal_bps < Decimal::ZERO
            || self.strategy.codex_scalp_probe_v1_min_flow_bps < Decimal::ZERO
            || self.strategy.codex_scalp_probe_v1_min_top_imbalance_bps < Decimal::ZERO
            || self.strategy.codex_scalp_probe_v1_min_depth_imbalance_bps < Decimal::ZERO
            || self.strategy.codex_scalp_probe_v1_min_radar_score_bps < Decimal::ZERO
            || self.strategy.codex_scalp_probe_v1_min_expected_profit_usdc < Decimal::ZERO
        {
            return Err(AppError::InvalidConfig(
                "strategy.codex_scalp_probe_v1 thresholds must be non-negative",
            ));
        }

        if self.strategy.codex_scalp_probe_v1_notional_usdc <= Decimal::ZERO {
            return Err(AppError::InvalidConfig(
                "strategy.codex_scalp_probe_v1_notional_usdc must be positive",
            ));
        }

        if self.strategy.codex_scalp_probe_v1_bnb_pressure_enabled {
            if self
                .strategy
                .codex_scalp_probe_v1_bnb_pressure_max_entry_price
                <= Decimal::ZERO
                || self
                    .strategy
                    .codex_scalp_probe_v1_bnb_pressure_max_entry_price
                    >= Decimal::ONE
                || self
                    .strategy
                    .codex_scalp_probe_v1_bnb_pressure_max_entry_price
                    < self.strategy.codex_scalp_probe_v1_min_entry_price
            {
                return Err(AppError::InvalidConfig(
                    "strategy.codex_scalp_probe_v1_bnb_pressure_max_entry_price must be within (0, 1) and >= min entry price",
                ));
            }

            if self
                .strategy
                .codex_scalp_probe_v1_bnb_pressure_max_book_age_ms
                < 0
            {
                return Err(AppError::InvalidConfig(
                    "strategy.codex_scalp_probe_v1_bnb_pressure_max_book_age_ms must not be negative",
                ));
            }

            if self
                .strategy
                .codex_scalp_probe_v1_bnb_pressure_min_target_gap_bps
                < Decimal::ZERO
                || self
                    .strategy
                    .codex_scalp_probe_v1_bnb_pressure_min_fresh_bps
                    < Decimal::ZERO
                || self
                    .strategy
                    .codex_scalp_probe_v1_bnb_pressure_min_top_imbalance_bps
                    < Decimal::ZERO
                || self
                    .strategy
                    .codex_scalp_probe_v1_bnb_pressure_min_depth_imbalance_bps
                    < Decimal::ZERO
                || self
                    .strategy
                    .codex_scalp_probe_v1_bnb_pressure_min_expected_profit_usdc
                    < Decimal::ZERO
            {
                return Err(AppError::InvalidConfig(
                    "strategy.codex_scalp_probe_v1_bnb_pressure thresholds must be non-negative",
                ));
            }
        }

        Ok(())
    }
}

/// HTTP settings.
#[derive(Debug, Clone, Deserialize)]
pub struct HttpConfig {
    #[serde(default = "default_gamma_base_url")]
    pub gamma_base_url: String,
    #[serde(default = "default_clob_base_url")]
    pub clob_base_url: String,
    #[serde(default = "default_data_api_base_url")]
    pub data_api_base_url: String,
    #[serde(default = "default_geoblock_url")]
    pub geoblock_url: String,
    #[serde(default = "default_binance_base_url")]
    pub binance_base_url: String,
    #[serde(default = "default_binance_ws_base_url")]
    pub binance_ws_base_url: String,
    #[serde(default)]
    pub coinbase_market_data_enabled: bool,
    #[serde(default = "default_coinbase_ws_base_url")]
    pub coinbase_ws_base_url: String,
    #[serde(default = "default_polymarket_rtds_ws_url")]
    pub polymarket_rtds_ws_url: String,
    #[serde(default = "default_coinbase_max_source_disagreement_bps")]
    pub coinbase_max_source_disagreement_bps: Decimal,
    #[serde(default = "default_coinbase_max_spread_bps")]
    pub coinbase_max_spread_bps: Decimal,
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
    #[serde(default = "default_page_size")]
    pub page_size: usize,
    #[serde(default = "default_books_batch_size")]
    pub books_batch_size: usize,
}

/// Strategy tuning.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Deserialize)]
pub struct StrategyConfig {
    pub market_targets: Vec<MarketTarget>,
    #[serde(default = "default_enable_bundle")]
    pub enable_bundle: bool,
    pub min_edge_bps: u32,
    pub assumed_fee_bps: u32,
    pub min_spot_move_bps: u32,
    pub min_liquidity_usdc: Decimal,
    pub min_top_of_book_shares: Decimal,
    pub max_bundle_notional_usdc: Decimal,
    pub max_market_notional_usdc: Decimal,
    pub min_seconds_left: i64,
    pub max_seconds_left: i64,
    pub min_minutes_to_expiry: i64,
    pub max_markets: usize,
    pub enable_directional: bool,
    pub directional_min_spot_move_bps: u32,
    #[serde(default = "default_directional_min_signal_bps")]
    pub directional_min_signal_bps: u32,
    #[serde(default = "default_directional_min_velocity_bps_per_minute")]
    pub directional_min_velocity_bps_per_minute: u32,
    #[serde(default = "default_directional_soft_entry_min_notional_usdc")]
    pub directional_soft_entry_min_notional_usdc: Decimal,
    #[serde(default = "default_directional_soft_entry_max_notional_usdc")]
    pub directional_soft_entry_max_notional_usdc: Decimal,
    #[serde(default = "default_directional_strong_signal_min_spot_move_5s_bps")]
    pub directional_strong_signal_min_spot_move_5s_bps: Decimal,
    #[serde(default = "default_directional_strong_signal_min_trade_flow_bps")]
    pub directional_strong_signal_min_trade_flow_bps: Decimal,
    #[serde(default = "default_directional_soft_entry_signal_window_bps")]
    pub directional_soft_entry_signal_window_bps: Decimal,
    pub directional_min_model_edge_bps: u32,
    pub directional_confidence_bps_per_spot_bps: u32,
    #[serde(default = "default_directional_projection_cap_multiplier")]
    pub directional_projection_cap_multiplier: Decimal,
    #[serde(default = "default_directional_trade_flow_weight")]
    pub directional_trade_flow_weight: Decimal,
    #[serde(default = "default_directional_micro_signal_weight")]
    pub directional_micro_signal_weight: Decimal,
    #[serde(default = "default_directional_micro_burst_weight")]
    pub directional_micro_burst_weight: Decimal,
    #[serde(default = "default_directional_require_hedge_for_soft_entry")]
    pub directional_require_hedge_for_soft_entry: bool,
    #[serde(default = "default_enable_micro_breakout")]
    pub enable_micro_breakout: bool,
    #[serde(default = "default_micro_breakout_min_spot_move_bps")]
    pub micro_breakout_min_spot_move_bps: u32,
    #[serde(default = "default_micro_breakout_min_spot_move_5s_bps")]
    pub micro_breakout_min_spot_move_5s_bps: Decimal,
    #[serde(default = "default_micro_breakout_min_spot_move_1s_bps")]
    pub micro_breakout_min_spot_move_1s_bps: Decimal,
    #[serde(default = "default_micro_breakout_min_signal_bps")]
    pub micro_breakout_min_signal_bps: u32,
    #[serde(default = "default_micro_breakout_signal_boost_multiplier")]
    pub micro_breakout_signal_boost_multiplier: Decimal,
    #[serde(default = "default_micro_breakout_signal_burst_multiplier")]
    pub micro_breakout_signal_burst_multiplier: Decimal,
    #[serde(default = "default_micro_breakout_max_burst_to_micro_ratio")]
    pub micro_breakout_max_burst_to_micro_ratio: Decimal,
    #[serde(default = "default_micro_breakout_target_cross_min_gap_bps")]
    pub micro_breakout_target_cross_min_gap_bps: Decimal,
    #[serde(default = "default_micro_breakout_target_cross_signal_boost_bps")]
    pub micro_breakout_target_cross_signal_boost_bps: Decimal,
    #[serde(default = "default_micro_breakout_max_entry_price")]
    pub micro_breakout_max_entry_price: Decimal,
    #[serde(default = "default_micro_breakout_max_average_price_drift")]
    pub micro_breakout_max_average_price_drift: Decimal,
    #[serde(default = "default_micro_breakout_min_elapsed_window_secs")]
    pub micro_breakout_min_elapsed_window_secs: i64,
    #[serde(default = "default_micro_breakout_weak_notional_usdc")]
    pub micro_breakout_weak_notional_usdc: Decimal,
    #[serde(default = "default_micro_breakout_normal_notional_usdc")]
    pub micro_breakout_normal_notional_usdc: Decimal,
    #[serde(default = "default_micro_breakout_strong_notional_usdc")]
    pub micro_breakout_strong_notional_usdc: Decimal,
    #[serde(default = "default_micro_breakout_expensive_entry_price")]
    pub micro_breakout_expensive_entry_price: Decimal,
    #[serde(default = "default_micro_breakout_expensive_entry_requires_strong_tier")]
    pub micro_breakout_expensive_entry_requires_strong_tier: bool,
    #[serde(default = "default_micro_breakout_full_size_max_entry_price")]
    pub micro_breakout_full_size_max_entry_price: Decimal,
    #[serde(default = "default_micro_breakout_strong_signal_min_spot_move_5s_bps")]
    pub micro_breakout_strong_signal_min_spot_move_5s_bps: Decimal,
    #[serde(default = "default_micro_breakout_strong_signal_min_spot_move_1s_bps")]
    pub micro_breakout_strong_signal_min_spot_move_1s_bps: Decimal,
    #[serde(default = "default_micro_breakout_strong_signal_min_spot_move_15s_bps")]
    pub micro_breakout_strong_signal_min_spot_move_15s_bps: Decimal,
    #[serde(default = "default_enable_target_state_v1")]
    pub enable_target_state_v1: bool,
    #[serde(default = "default_target_state_min_elapsed_window_secs")]
    pub target_state_min_elapsed_window_secs: i64,
    #[serde(default = "default_target_state_max_seconds_left")]
    pub target_state_max_seconds_left: i64,
    #[serde(default = "default_target_state_min_target_gap_bps")]
    pub target_state_min_target_gap_bps: Decimal,
    #[serde(default = "default_target_state_min_signal_bps")]
    pub target_state_min_signal_bps: u32,
    #[serde(default = "default_target_state_min_spot_move_15s_bps")]
    pub target_state_min_spot_move_15s_bps: Decimal,
    #[serde(default = "default_target_state_min_aligned_flow_bps")]
    pub target_state_min_aligned_flow_bps: Decimal,
    #[serde(default = "default_target_state_max_entry_price")]
    pub target_state_max_entry_price: Decimal,
    #[serde(default = "default_target_state_normal_notional_usdc")]
    pub target_state_normal_notional_usdc: Decimal,
    #[serde(default = "default_target_state_strong_notional_usdc")]
    pub target_state_strong_notional_usdc: Decimal,
    #[serde(default = "default_target_state_strong_gap_bps")]
    pub target_state_strong_gap_bps: Decimal,
    #[serde(default = "default_enable_bonereaper_state_v1")]
    pub enable_bonereaper_state_v1: bool,
    #[serde(default = "default_bonereaper_state_min_elapsed_window_secs")]
    pub bonereaper_state_min_elapsed_window_secs: i64,
    #[serde(default = "default_bonereaper_state_max_seconds_left")]
    pub bonereaper_state_max_seconds_left: i64,
    #[serde(default = "default_bonereaper_state_min_target_gap_bps")]
    pub bonereaper_state_min_target_gap_bps: Decimal,
    #[serde(default = "default_bonereaper_state_min_signal_bps")]
    pub bonereaper_state_min_signal_bps: u32,
    #[serde(default = "default_bonereaper_state_min_spot_move_15s_bps")]
    pub bonereaper_state_min_spot_move_15s_bps: Decimal,
    #[serde(default = "default_bonereaper_state_min_spot_move_5s_bps")]
    pub bonereaper_state_min_spot_move_5s_bps: Decimal,
    #[serde(default = "default_bonereaper_state_min_aligned_flow_bps")]
    pub bonereaper_state_min_aligned_flow_bps: Decimal,
    #[serde(default = "default_bonereaper_state_max_entry_price")]
    pub bonereaper_state_max_entry_price: Decimal,
    #[serde(default = "default_bonereaper_state_normal_notional_usdc")]
    pub bonereaper_state_normal_notional_usdc: Decimal,
    #[serde(default = "default_bonereaper_state_strong_notional_usdc")]
    pub bonereaper_state_strong_notional_usdc: Decimal,
    #[serde(default = "default_bonereaper_state_strong_gap_bps")]
    pub bonereaper_state_strong_gap_bps: Decimal,
    #[serde(default = "default_bonereaper_state_strong_flow_bps")]
    pub bonereaper_state_strong_flow_bps: Decimal,
    #[serde(default = "default_enable_bonereaper_state_v2")]
    pub enable_bonereaper_state_v2: bool,
    #[serde(default = "default_enable_bonereaper_state_guarded")]
    pub enable_bonereaper_state_guarded: bool,
    #[serde(default = "default_enable_codex_sentinel_v1")]
    pub enable_codex_sentinel_v1: bool,
    #[serde(default = "default_codex_sentinel_v1_mid_signal_guard_enabled")]
    pub codex_sentinel_v1_mid_signal_guard_enabled: bool,
    #[serde(default = "default_codex_sentinel_v1_mid_signal_min_bps")]
    pub codex_sentinel_v1_mid_signal_min_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_mid_signal_max_bps")]
    pub codex_sentinel_v1_mid_signal_max_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_mid_signal_min_confirmation_bps")]
    pub codex_sentinel_v1_mid_signal_min_confirmation_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_max_entry_price")]
    pub codex_sentinel_v1_max_entry_price: Decimal,
    #[serde(default = "default_codex_sentinel_v1_live_quote_age_guard_enabled")]
    pub codex_sentinel_v1_live_quote_age_guard_enabled: bool,
    #[serde(default = "default_codex_sentinel_v1_max_live_quote_age_ms")]
    pub codex_sentinel_v1_max_live_quote_age_ms: i64,
    #[serde(default = "default_codex_sentinel_v1_entry_spread_guard_enabled")]
    pub codex_sentinel_v1_entry_spread_guard_enabled: bool,
    #[serde(default = "default_codex_sentinel_v1_max_entry_spread")]
    pub codex_sentinel_v1_max_entry_spread: Decimal,
    #[serde(default = "default_codex_sentinel_v1_stale_micro_guard_enabled")]
    pub codex_sentinel_v1_stale_micro_guard_enabled: bool,
    #[serde(default = "default_codex_sentinel_v1_stale_micro_max_confirmation_bps")]
    pub codex_sentinel_v1_stale_micro_max_confirmation_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_stale_micro_discount_max_entry_price")]
    pub codex_sentinel_v1_stale_micro_discount_max_entry_price: Decimal,
    #[serde(default = "default_codex_sentinel_v1_stale_micro_discount_min_signal_bps")]
    pub codex_sentinel_v1_stale_micro_discount_min_signal_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_stale_micro_discount_min_flow_bps")]
    pub codex_sentinel_v1_stale_micro_discount_min_flow_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_stale_micro_min_signal_bps")]
    pub codex_sentinel_v1_stale_micro_min_signal_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_stale_micro_min_flow_bps")]
    pub codex_sentinel_v1_stale_micro_min_flow_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_stale_micro_max_non_discount_entry_price")]
    pub codex_sentinel_v1_stale_micro_max_non_discount_entry_price: Decimal,
    #[serde(default = "default_codex_sentinel_v1_stale_micro_min_swing_bps")]
    pub codex_sentinel_v1_stale_micro_min_swing_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_stale_micro_min_target_gap_bps")]
    pub codex_sentinel_v1_stale_micro_min_target_gap_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_expensive_entry_guard_enabled")]
    pub codex_sentinel_v1_expensive_entry_guard_enabled: bool,
    #[serde(default = "default_codex_sentinel_v1_expensive_entry_price")]
    pub codex_sentinel_v1_expensive_entry_price: Decimal,
    #[serde(default = "default_codex_sentinel_v1_expensive_min_micro_bps")]
    pub codex_sentinel_v1_expensive_min_micro_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_expensive_min_swing_bps")]
    pub codex_sentinel_v1_expensive_min_swing_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_premium_entry_guard_enabled")]
    pub codex_sentinel_v1_premium_entry_guard_enabled: bool,
    #[serde(default = "default_codex_sentinel_v1_premium_entry_price")]
    pub codex_sentinel_v1_premium_entry_price: Decimal,
    #[serde(default = "default_codex_sentinel_v1_premium_min_signal_bps")]
    pub codex_sentinel_v1_premium_min_signal_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_premium_min_flow_bps")]
    pub codex_sentinel_v1_premium_min_flow_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_premium_min_fresh_bps")]
    pub codex_sentinel_v1_premium_min_fresh_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_aggressive_continuation_enabled")]
    pub codex_sentinel_v1_aggressive_continuation_enabled: bool,
    #[serde(default = "default_codex_sentinel_v1_aggressive_continuation_max_entry_price")]
    pub codex_sentinel_v1_aggressive_continuation_max_entry_price: Decimal,
    #[serde(default = "default_codex_sentinel_v1_aggressive_continuation_min_target_gap_bps")]
    pub codex_sentinel_v1_aggressive_continuation_min_target_gap_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_aggressive_continuation_min_signal_bps")]
    pub codex_sentinel_v1_aggressive_continuation_min_signal_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_aggressive_continuation_min_flow_bps")]
    pub codex_sentinel_v1_aggressive_continuation_min_flow_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_aggressive_continuation_min_fresh_bps")]
    pub codex_sentinel_v1_aggressive_continuation_min_fresh_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_aggressive_continuation_min_swing_bps")]
    pub codex_sentinel_v1_aggressive_continuation_min_swing_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_aggressive_continuation_max_quote_age_ms")]
    pub codex_sentinel_v1_aggressive_continuation_max_quote_age_ms: i64,
    #[serde(default = "default_codex_breakout_v1_enabled")]
    pub codex_breakout_v1_enabled: bool,
    #[serde(default = "default_codex_breakout_v1_required")]
    pub codex_breakout_v1_required: bool,
    #[serde(default = "default_codex_breakout_v1_max_entry_price")]
    pub codex_breakout_v1_max_entry_price: Decimal,
    #[serde(default = "default_codex_breakout_v1_max_book_age_ms")]
    pub codex_breakout_v1_max_book_age_ms: i64,
    #[serde(default = "default_codex_breakout_v1_max_spread_bps")]
    pub codex_breakout_v1_max_spread_bps: Decimal,
    #[serde(default = "default_codex_breakout_v1_min_score_bps")]
    pub codex_breakout_v1_min_score_bps: Decimal,
    #[serde(default = "default_codex_breakout_v1_min_depth_imbalance_bps")]
    pub codex_breakout_v1_min_depth_imbalance_bps: Decimal,
    #[serde(default = "default_codex_breakout_v1_min_microprice_bps")]
    pub codex_breakout_v1_min_microprice_bps: Decimal,
    #[serde(default = "default_codex_breakout_v1_min_fresh_bps")]
    pub codex_breakout_v1_min_fresh_bps: Decimal,
    #[serde(default = "default_codex_breakout_v1_min_target_gap_bps")]
    pub codex_breakout_v1_min_target_gap_bps: Decimal,
    #[serde(default = "default_codex_breakout_v1_min_signal_bps")]
    pub codex_breakout_v1_min_signal_bps: Decimal,
    #[serde(default = "default_codex_breakout_v1_min_flow_bps")]
    pub codex_breakout_v1_min_flow_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_discount_value_lane_enabled")]
    pub codex_sentinel_v1_discount_value_lane_enabled: bool,
    #[serde(default = "default_codex_sentinel_v1_discount_value_max_entry_price")]
    pub codex_sentinel_v1_discount_value_max_entry_price: Decimal,
    #[serde(default = "default_codex_sentinel_v1_discount_value_max_book_age_ms")]
    pub codex_sentinel_v1_discount_value_max_book_age_ms: i64,
    #[serde(default = "default_codex_sentinel_v1_discount_value_max_exchange_spread_bps")]
    pub codex_sentinel_v1_discount_value_max_exchange_spread_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_discount_value_min_target_gap_bps")]
    pub codex_sentinel_v1_discount_value_min_target_gap_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_discount_value_min_fresh_bps")]
    pub codex_sentinel_v1_discount_value_min_fresh_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_discount_value_min_swing_bps")]
    pub codex_sentinel_v1_discount_value_min_swing_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_discount_value_min_signal_bps")]
    pub codex_sentinel_v1_discount_value_min_signal_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_discount_value_min_flow_bps")]
    pub codex_sentinel_v1_discount_value_min_flow_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_discount_value_min_top_imbalance_bps")]
    pub codex_sentinel_v1_discount_value_min_top_imbalance_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_discount_value_min_depth_imbalance_bps")]
    pub codex_sentinel_v1_discount_value_min_depth_imbalance_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_discount_value_min_microprice_bps")]
    pub codex_sentinel_v1_discount_value_min_microprice_bps: Decimal,
    #[serde(default = "default_enable_codex_scalp_probe_v1")]
    pub enable_codex_scalp_probe_v1: bool,
    #[serde(default = "default_codex_scalp_probe_v1_raw_ablation_enabled")]
    pub codex_scalp_probe_v1_raw_ablation_enabled: bool,
    #[serde(default = "default_codex_scalp_probe_v1_raw_light_enabled")]
    pub codex_scalp_probe_v1_raw_light_enabled: bool,
    #[serde(default = "default_codex_scalp_probe_v1_min_entry_price")]
    pub codex_scalp_probe_v1_min_entry_price: Decimal,
    #[serde(default = "default_codex_scalp_probe_v1_max_entry_price")]
    pub codex_scalp_probe_v1_max_entry_price: Decimal,
    #[serde(default = "default_codex_scalp_probe_v1_max_entry_spread")]
    pub codex_scalp_probe_v1_max_entry_spread: Decimal,
    #[serde(default = "default_codex_scalp_probe_v1_min_elapsed_window_secs")]
    pub codex_scalp_probe_v1_min_elapsed_window_secs: i64,
    #[serde(default = "default_codex_scalp_probe_v1_max_seconds_left")]
    pub codex_scalp_probe_v1_max_seconds_left: i64,
    #[serde(default = "default_codex_scalp_probe_v1_min_seconds_left")]
    pub codex_scalp_probe_v1_min_seconds_left: i64,
    #[serde(default = "default_codex_scalp_probe_v1_max_book_age_ms")]
    pub codex_scalp_probe_v1_max_book_age_ms: i64,
    #[serde(default = "default_codex_scalp_probe_v1_max_exchange_spread_bps")]
    pub codex_scalp_probe_v1_max_exchange_spread_bps: Decimal,
    #[serde(default = "default_codex_scalp_probe_v1_min_target_gap_bps")]
    pub codex_scalp_probe_v1_min_target_gap_bps: Decimal,
    #[serde(default = "default_codex_scalp_probe_v1_min_fresh_bps")]
    pub codex_scalp_probe_v1_min_fresh_bps: Decimal,
    #[serde(default = "default_codex_scalp_probe_v1_min_signal_bps")]
    pub codex_scalp_probe_v1_min_signal_bps: Decimal,
    #[serde(default = "default_codex_scalp_probe_v1_min_flow_bps")]
    pub codex_scalp_probe_v1_min_flow_bps: Decimal,
    #[serde(default = "default_codex_scalp_probe_v1_min_top_imbalance_bps")]
    pub codex_scalp_probe_v1_min_top_imbalance_bps: Decimal,
    #[serde(default = "default_codex_scalp_probe_v1_min_depth_imbalance_bps")]
    pub codex_scalp_probe_v1_min_depth_imbalance_bps: Decimal,
    #[serde(default = "default_codex_scalp_probe_v1_min_radar_score_bps")]
    pub codex_scalp_probe_v1_min_radar_score_bps: Decimal,
    #[serde(default = "default_codex_scalp_probe_v1_notional_usdc")]
    pub codex_scalp_probe_v1_notional_usdc: Decimal,
    #[serde(default = "default_codex_scalp_probe_v1_min_expected_profit_usdc")]
    pub codex_scalp_probe_v1_min_expected_profit_usdc: Decimal,
    #[serde(default = "default_codex_scalp_probe_v1_bnb_pressure_enabled")]
    pub codex_scalp_probe_v1_bnb_pressure_enabled: bool,
    #[serde(default = "default_codex_scalp_probe_v1_bnb_pressure_max_entry_price")]
    pub codex_scalp_probe_v1_bnb_pressure_max_entry_price: Decimal,
    #[serde(default = "default_codex_scalp_probe_v1_bnb_pressure_max_book_age_ms")]
    pub codex_scalp_probe_v1_bnb_pressure_max_book_age_ms: i64,
    #[serde(default = "default_codex_scalp_probe_v1_bnb_pressure_min_target_gap_bps")]
    pub codex_scalp_probe_v1_bnb_pressure_min_target_gap_bps: Decimal,
    #[serde(default = "default_codex_scalp_probe_v1_bnb_pressure_min_fresh_bps")]
    pub codex_scalp_probe_v1_bnb_pressure_min_fresh_bps: Decimal,
    #[serde(default = "default_codex_scalp_probe_v1_bnb_pressure_min_top_imbalance_bps")]
    pub codex_scalp_probe_v1_bnb_pressure_min_top_imbalance_bps: Decimal,
    #[serde(default = "default_codex_scalp_probe_v1_bnb_pressure_min_depth_imbalance_bps")]
    pub codex_scalp_probe_v1_bnb_pressure_min_depth_imbalance_bps: Decimal,
    #[serde(default = "default_codex_scalp_probe_v1_bnb_pressure_min_expected_profit_usdc")]
    pub codex_scalp_probe_v1_bnb_pressure_min_expected_profit_usdc: Decimal,
    #[serde(default = "default_codex_sentinel_v1_no_chase_guard_enabled")]
    pub codex_sentinel_v1_no_chase_guard_enabled: bool,
    #[serde(default = "default_codex_sentinel_v1_no_chase_entry_price")]
    pub codex_sentinel_v1_no_chase_entry_price: Decimal,
    #[serde(default = "default_codex_sentinel_v1_no_chase_min_seconds_left")]
    pub codex_sentinel_v1_no_chase_min_seconds_left: i64,
    #[serde(default = "default_codex_sentinel_v1_no_chase_allow_min_target_gap_bps")]
    pub codex_sentinel_v1_no_chase_allow_min_target_gap_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_no_chase_allow_min_fresh_bps")]
    pub codex_sentinel_v1_no_chase_allow_min_fresh_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_no_chase_allow_min_signal_bps")]
    pub codex_sentinel_v1_no_chase_allow_min_signal_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_no_chase_allow_min_flow_bps")]
    pub codex_sentinel_v1_no_chase_allow_min_flow_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_quality_floor_enabled")]
    pub codex_sentinel_v1_quality_floor_enabled: bool,
    #[serde(default = "default_codex_sentinel_v1_quality_floor_min_target_gap_bps")]
    pub codex_sentinel_v1_quality_floor_min_target_gap_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_quality_floor_mid_gap_max_bps")]
    pub codex_sentinel_v1_quality_floor_mid_gap_max_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_quality_floor_mid_gap_min_signal_bps")]
    pub codex_sentinel_v1_quality_floor_mid_gap_min_signal_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_quality_floor_mid_gap_min_flow_bps")]
    pub codex_sentinel_v1_quality_floor_mid_gap_min_flow_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_mid_gap_premium_guard_enabled")]
    pub codex_sentinel_v1_mid_gap_premium_guard_enabled: bool,
    #[serde(default = "default_codex_sentinel_v1_mid_gap_premium_min_target_gap_bps")]
    pub codex_sentinel_v1_mid_gap_premium_min_target_gap_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_mid_gap_premium_max_target_gap_bps")]
    pub codex_sentinel_v1_mid_gap_premium_max_target_gap_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_mid_gap_premium_entry_price")]
    pub codex_sentinel_v1_mid_gap_premium_entry_price: Decimal,
    #[serde(default = "default_codex_sentinel_v1_mid_gap_premium_min_signal_bps")]
    pub codex_sentinel_v1_mid_gap_premium_min_signal_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_mid_gap_premium_min_flow_bps")]
    pub codex_sentinel_v1_mid_gap_premium_min_flow_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_mid_gap_premium_min_fresh_bps")]
    pub codex_sentinel_v1_mid_gap_premium_min_fresh_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_attack_size_enabled")]
    pub codex_sentinel_v1_attack_size_enabled: bool,
    #[serde(default = "default_codex_sentinel_v1_attack_notional_usdc")]
    pub codex_sentinel_v1_attack_notional_usdc: Decimal,
    #[serde(default = "default_codex_sentinel_v1_attack_min_signal_bps")]
    pub codex_sentinel_v1_attack_min_signal_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_attack_min_flow_bps")]
    pub codex_sentinel_v1_attack_min_flow_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_attack_min_confirmation_bps")]
    pub codex_sentinel_v1_attack_min_confirmation_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_attack_max_entry_price")]
    pub codex_sentinel_v1_attack_max_entry_price: Decimal,
    #[serde(default = "default_codex_sentinel_v1_bad_window_guard_enabled")]
    pub codex_sentinel_v1_bad_window_guard_enabled: bool,
    #[serde(default = "default_codex_sentinel_v1_bad_window_min_score")]
    pub codex_sentinel_v1_bad_window_min_score: Decimal,
    #[serde(default = "default_codex_sentinel_v1_confidence_sizing_enabled")]
    pub codex_sentinel_v1_confidence_sizing_enabled: bool,
    #[serde(default = "default_codex_sentinel_v1_confidence_min_score")]
    pub codex_sentinel_v1_confidence_min_score: Decimal,
    #[serde(default = "default_codex_sentinel_v1_confidence_max_multiplier")]
    pub codex_sentinel_v1_confidence_max_multiplier: Decimal,
    #[serde(default = "default_codex_sentinel_v1_low_flow_guard_enabled")]
    pub codex_sentinel_v1_low_flow_guard_enabled: bool,
    #[serde(default = "default_codex_sentinel_v1_low_flow_max_flow_bps")]
    pub codex_sentinel_v1_low_flow_max_flow_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_low_flow_allow_min_signal_bps")]
    pub codex_sentinel_v1_low_flow_allow_min_signal_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_low_flow_allow_min_fresh_bps")]
    pub codex_sentinel_v1_low_flow_allow_min_fresh_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_low_flow_allow_min_swing_bps")]
    pub codex_sentinel_v1_low_flow_allow_min_swing_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_low_flow_allow_max_entry_price")]
    pub codex_sentinel_v1_low_flow_allow_max_entry_price: Decimal,
    #[serde(default = "default_codex_sentinel_v1_counter_burst_guard_enabled")]
    pub codex_sentinel_v1_counter_burst_guard_enabled: bool,
    #[serde(default = "default_codex_sentinel_v1_counter_burst_min_bps")]
    pub codex_sentinel_v1_counter_burst_min_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_counter_burst_max_entry_price")]
    pub codex_sentinel_v1_counter_burst_max_entry_price: Decimal,
    #[serde(default = "default_codex_sentinel_v1_late_entry_override_enabled")]
    pub codex_sentinel_v1_late_entry_override_enabled: bool,
    #[serde(default = "default_codex_sentinel_v1_late_entry_min_seconds_left")]
    pub codex_sentinel_v1_late_entry_min_seconds_left: i64,
    #[serde(default = "default_codex_sentinel_v1_late_entry_max_entry_price")]
    pub codex_sentinel_v1_late_entry_max_entry_price: Decimal,
    #[serde(default = "default_codex_sentinel_v1_late_entry_min_signal_bps")]
    pub codex_sentinel_v1_late_entry_min_signal_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_late_entry_min_fresh_bps")]
    pub codex_sentinel_v1_late_entry_min_fresh_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_late_entry_min_flow_bps")]
    pub codex_sentinel_v1_late_entry_min_flow_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_late_entry_min_target_gap_bps")]
    pub codex_sentinel_v1_late_entry_min_target_gap_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_late_window_value_guard_enabled")]
    pub codex_sentinel_v1_late_window_value_guard_enabled: bool,
    #[serde(default = "default_codex_sentinel_v1_late_window_max_seconds_left")]
    pub codex_sentinel_v1_late_window_max_seconds_left: i64,
    #[serde(default = "default_codex_sentinel_v1_late_window_max_entry_price")]
    pub codex_sentinel_v1_late_window_max_entry_price: Decimal,
    #[serde(default = "default_codex_sentinel_v1_late_window_allow_min_signal_bps")]
    pub codex_sentinel_v1_late_window_allow_min_signal_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_late_window_allow_min_fresh_bps")]
    pub codex_sentinel_v1_late_window_allow_min_fresh_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_late_window_allow_min_flow_bps")]
    pub codex_sentinel_v1_late_window_allow_min_flow_bps: Decimal,
    #[serde(default = "default_codex_sentinel_v1_late_window_allow_min_target_gap_bps")]
    pub codex_sentinel_v1_late_window_allow_min_target_gap_bps: Decimal,
    #[serde(default = "default_bonereaper_state_v2_min_elapsed_window_secs")]
    pub bonereaper_state_v2_min_elapsed_window_secs: i64,
    #[serde(default = "default_bonereaper_state_v2_max_seconds_left")]
    pub bonereaper_state_v2_max_seconds_left: i64,
    #[serde(default = "default_bonereaper_state_v2_min_seconds_left")]
    pub bonereaper_state_v2_min_seconds_left: i64,
    #[serde(default = "default_bonereaper_state_v2_bias_min_target_gap_bps")]
    pub bonereaper_state_v2_bias_min_target_gap_bps: Decimal,
    #[serde(default = "default_bonereaper_state_v2_flip_max_target_gap_bps")]
    pub bonereaper_state_v2_flip_max_target_gap_bps: Decimal,
    #[serde(default = "default_bonereaper_state_v2_min_signal_bps")]
    pub bonereaper_state_v2_min_signal_bps: u32,
    #[serde(default = "default_bonereaper_state_v2_min_spot_move_15s_bps")]
    pub bonereaper_state_v2_min_spot_move_15s_bps: Decimal,
    #[serde(default = "default_bonereaper_state_v2_min_spot_move_5s_bps")]
    pub bonereaper_state_v2_min_spot_move_5s_bps: Decimal,
    #[serde(default = "default_bonereaper_state_v2_min_aligned_flow_bps")]
    pub bonereaper_state_v2_min_aligned_flow_bps: Decimal,
    #[serde(default = "default_bonereaper_state_v2_max_entry_price")]
    pub bonereaper_state_v2_max_entry_price: Decimal,
    #[serde(default = "default_bonereaper_state_v2_max_fair_price")]
    pub bonereaper_state_v2_max_fair_price: Decimal,
    #[serde(default = "default_bonereaper_state_v2_probe_notional_usdc")]
    pub bonereaper_state_v2_probe_notional_usdc: Decimal,
    #[serde(default = "default_bonereaper_state_v2_normal_notional_usdc")]
    pub bonereaper_state_v2_normal_notional_usdc: Decimal,
    #[serde(default = "default_bonereaper_state_v2_strong_notional_usdc")]
    pub bonereaper_state_v2_strong_notional_usdc: Decimal,
    #[serde(default = "default_bonereaper_state_v2_strong_gap_bps")]
    pub bonereaper_state_v2_strong_gap_bps: Decimal,
    #[serde(default = "default_bonereaper_state_v2_strong_flow_bps")]
    pub bonereaper_state_v2_strong_flow_bps: Decimal,
    #[serde(default = "default_bonereaper_state_v2_min_expected_profit_usdc")]
    pub bonereaper_state_v2_min_expected_profit_usdc: Decimal,
    #[serde(default = "default_bonereaper_state_v2_micro_alignment_guard_enabled")]
    pub bonereaper_state_v2_micro_alignment_guard_enabled: bool,
    #[serde(default = "default_bonereaper_state_v2_max_counter_1s_bps")]
    pub bonereaper_state_v2_max_counter_1s_bps: Decimal,
    #[serde(default = "default_bonereaper_state_v2_max_counter_5s_bps")]
    pub bonereaper_state_v2_max_counter_5s_bps: Decimal,
    #[serde(default = "default_bonereaper_state_v2_early_window_guard_enabled")]
    pub bonereaper_state_v2_early_window_guard_enabled: bool,
    #[serde(default = "default_bonereaper_state_v2_early_window_max_seconds_left")]
    pub bonereaper_state_v2_early_window_max_seconds_left: i64,
    #[serde(default = "default_bonereaper_state_v2_early_window_min_fresh_bps")]
    pub bonereaper_state_v2_early_window_min_fresh_bps: Decimal,
    #[serde(default = "default_bonereaper_state_v2_early_window_min_swing_bps")]
    pub bonereaper_state_v2_early_window_min_swing_bps: Decimal,
    #[serde(default = "default_bonereaper_state_v2_early_window_min_signal_bps")]
    pub bonereaper_state_v2_early_window_min_signal_bps: Decimal,
    #[serde(default = "default_bonereaper_state_v2_high_gap_guard_enabled")]
    pub bonereaper_state_v2_high_gap_guard_enabled: bool,
    #[serde(default = "default_bonereaper_state_v2_high_gap_min_target_gap_bps")]
    pub bonereaper_state_v2_high_gap_min_target_gap_bps: Decimal,
    #[serde(default = "default_bonereaper_state_v2_high_gap_max_entry_price")]
    pub bonereaper_state_v2_high_gap_max_entry_price: Decimal,
    #[serde(default = "default_bonereaper_state_v2_high_gap_min_fresh_bps")]
    pub bonereaper_state_v2_high_gap_min_fresh_bps: Decimal,
    #[serde(default = "default_bonereaper_state_v2_high_gap_min_swing_bps")]
    pub bonereaper_state_v2_high_gap_min_swing_bps: Decimal,
    #[serde(default = "default_bonereaper_state_v2_high_gap_min_signal_bps")]
    pub bonereaper_state_v2_high_gap_min_signal_bps: Decimal,
    #[serde(default = "default_bonereaper_state_v2_mid_gap_guard_enabled")]
    pub bonereaper_state_v2_mid_gap_guard_enabled: bool,
    #[serde(default = "default_bonereaper_state_v2_mid_gap_min_target_gap_bps")]
    pub bonereaper_state_v2_mid_gap_min_target_gap_bps: Decimal,
    #[serde(default = "default_bonereaper_state_v2_mid_gap_max_target_gap_bps")]
    pub bonereaper_state_v2_mid_gap_max_target_gap_bps: Decimal,
    #[serde(default = "default_bonereaper_state_v2_mid_gap_max_entry_price")]
    pub bonereaper_state_v2_mid_gap_max_entry_price: Decimal,
    #[serde(default = "default_bonereaper_state_v2_mid_gap_min_seconds_left")]
    pub bonereaper_state_v2_mid_gap_min_seconds_left: i64,
    #[serde(default = "default_bonereaper_state_v2_mid_gap_min_fresh_bps")]
    pub bonereaper_state_v2_mid_gap_min_fresh_bps: Decimal,
    #[serde(default = "default_bonereaper_state_v2_mid_gap_min_signal_bps")]
    pub bonereaper_state_v2_mid_gap_min_signal_bps: Decimal,
    #[serde(default = "default_bonereaper_state_v2_mid_gap_min_flow_bps")]
    pub bonereaper_state_v2_mid_gap_min_flow_bps: Decimal,
    #[serde(default = "default_bonereaper_state_v2_low_gap_guard_enabled")]
    pub bonereaper_state_v2_low_gap_guard_enabled: bool,
    #[serde(default = "default_bonereaper_state_v2_low_gap_max_target_gap_bps")]
    pub bonereaper_state_v2_low_gap_max_target_gap_bps: Decimal,
    #[serde(default = "default_bonereaper_state_v2_low_gap_max_entry_price")]
    pub bonereaper_state_v2_low_gap_max_entry_price: Decimal,
    #[serde(default = "default_bonereaper_state_v2_low_gap_min_seconds_left")]
    pub bonereaper_state_v2_low_gap_min_seconds_left: i64,
    #[serde(default = "default_bonereaper_state_v2_low_gap_allow_min_fresh_bps")]
    pub bonereaper_state_v2_low_gap_allow_min_fresh_bps: Decimal,
    #[serde(default = "default_bonereaper_state_v2_low_gap_allow_min_signal_bps")]
    pub bonereaper_state_v2_low_gap_allow_min_signal_bps: Decimal,
    #[serde(default = "default_bonereaper_state_v2_low_gap_allow_min_flow_bps")]
    pub bonereaper_state_v2_low_gap_allow_min_flow_bps: Decimal,
    #[serde(default = "default_bonereaper_state_v2_early_expensive_guard_enabled")]
    pub bonereaper_state_v2_early_expensive_guard_enabled: bool,
    #[serde(default = "default_bonereaper_state_v2_early_expensive_min_seconds_left")]
    pub bonereaper_state_v2_early_expensive_min_seconds_left: i64,
    #[serde(default = "default_bonereaper_state_v2_early_expensive_entry_price")]
    pub bonereaper_state_v2_early_expensive_entry_price: Decimal,
    #[serde(default = "default_bonereaper_state_v2_early_expensive_allow_min_target_gap_bps")]
    pub bonereaper_state_v2_early_expensive_allow_min_target_gap_bps: Decimal,
    #[serde(default = "default_bonereaper_state_v2_early_expensive_allow_min_fresh_bps")]
    pub bonereaper_state_v2_early_expensive_allow_min_fresh_bps: Decimal,
    #[serde(default = "default_bonereaper_state_v2_early_expensive_allow_min_signal_bps")]
    pub bonereaper_state_v2_early_expensive_allow_min_signal_bps: Decimal,
    #[serde(default = "default_bonereaper_state_v2_early_expensive_allow_min_flow_bps")]
    pub bonereaper_state_v2_early_expensive_allow_min_flow_bps: Decimal,
    #[serde(default = "default_directional_execution_slippage_bps")]
    pub directional_execution_slippage_bps: u32,
    pub directional_max_fair_price: Decimal,
    pub directional_max_entry_price: Decimal,
    pub max_directional_notional_usdc: Decimal,
    pub enable_tail_hedge: bool,
    pub tail_hedge_ratio: Decimal,
    #[serde(default = "default_tail_hedge_min_spot_move_bps")]
    pub tail_hedge_min_spot_move_bps: u32,
    #[serde(default = "default_tail_hedge_min_signal_bps")]
    pub tail_hedge_min_signal_bps: u32,
    #[serde(default = "default_tail_hedge_min_velocity_bps_per_minute")]
    pub tail_hedge_min_velocity_bps_per_minute: u32,
    pub tail_hedge_max_opposite_price: Decimal,
    pub tail_hedge_max_bundle_cost: Decimal,
    pub tail_hedge_open_window_secs: i64,
}

/// Runtime settings.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Deserialize)]
pub struct RunConfig {
    pub mode: BotMode,
    pub poll_interval_secs: u64,
    #[serde(default)]
    pub paper_starting_balance_usdc: Option<Decimal>,
    #[serde(default)]
    pub paper_start_mode: Option<PaperStartMode>,
    #[serde(default = "default_restore_paper_state_on_start")]
    pub restore_paper_state_on_start: bool,
    #[serde(default = "default_reactive_run")]
    pub reactive: bool,
    #[serde(default = "default_reactive_debounce_ms")]
    pub reactive_debounce_ms: u64,
    #[serde(default = "default_reactive_idle_secs")]
    pub reactive_idle_secs: u64,
    #[serde(default = "default_allow_repeat_entries_same_window")]
    pub allow_repeat_entries_same_window: bool,
    #[serde(default = "default_repeat_entry_min_interval_ms")]
    pub repeat_entry_min_interval_ms: u64,
    #[serde(default = "default_revalidate_before_execute")]
    pub revalidate_before_execute: bool,
    #[serde(default)]
    pub polymarket_stream: PolymarketStreamConfig,
    #[serde(default)]
    pub chainlink_oracle: ChainlinkOracleConfig,
    pub execute_top_n: usize,
    #[serde(default)]
    pub scale_in: ScaleInConfig,
    #[serde(default)]
    pub adaptive_regime: AdaptiveRegimeConfig,
    #[serde(default)]
    pub pnl_ratchet: PnlRatchetConfig,
    #[serde(default)]
    pub risk: RiskControlConfig,
    #[serde(default)]
    pub v4_inventory: V4InventoryConfig,
    #[serde(default)]
    pub early_exit: EarlyExitConfig,
}

impl RunConfig {
    #[must_use]
    pub fn effective_paper_start_mode(&self) -> PaperStartMode {
        self.paper_start_mode.unwrap_or({
            if self.restore_paper_state_on_start {
                PaperStartMode::Resume
            } else {
                PaperStartMode::Isolated
            }
        })
    }

    #[must_use]
    pub fn should_restore_paper_state_on_start(&self) -> bool {
        matches!(self.effective_paper_start_mode(), PaperStartMode::Resume)
    }

    #[must_use]
    pub fn should_seed_risk_from_history(&self) -> bool {
        matches!(self.effective_paper_start_mode(), PaperStartMode::Resume)
            && self.risk.seed_from_history
    }
}

/// Startup behavior for paper-mode sessions.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PaperStartMode {
    Resume,
    Isolated,
}

impl PaperStartMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Resume => "resume",
            Self::Isolated => "isolated",
        }
    }
}

/// Polymarket live-stream settings for orderbook/trade ingestion.
#[derive(Debug, Clone, Deserialize)]
pub struct PolymarketStreamConfig {
    #[serde(default = "default_polymarket_stream_enabled")]
    pub enabled: bool,
    #[serde(default = "default_polymarket_stream_book_staleness_ms")]
    pub book_staleness_ms: i64,
    #[serde(default = "default_polymarket_stream_rest_fallback_enabled")]
    pub rest_fallback_enabled: bool,
    #[serde(default = "default_polymarket_stream_backfill_trade_flow")]
    pub backfill_trade_flow: bool,
}

impl Default for PolymarketStreamConfig {
    fn default() -> Self {
        Self {
            enabled: default_polymarket_stream_enabled(),
            book_staleness_ms: default_polymarket_stream_book_staleness_ms(),
            rest_fallback_enabled: default_polymarket_stream_rest_fallback_enabled(),
            backfill_trade_flow: default_polymarket_stream_backfill_trade_flow(),
        }
    }
}

/// Polymarket RTDS Chainlink oracle-price settings.
#[derive(Debug, Clone, Copy, Deserialize)]
pub struct ChainlinkOracleConfig {
    #[serde(default = "default_chainlink_oracle_enabled")]
    pub enabled: bool,
    #[serde(default = "default_chainlink_oracle_max_quote_age_ms")]
    pub max_quote_age_ms: i64,
    #[serde(default = "default_chainlink_oracle_max_window_open_lag_ms")]
    pub max_window_open_lag_ms: i64,
    #[serde(default = "default_chainlink_oracle_max_settlement_close_lag_ms")]
    pub max_settlement_close_lag_ms: i64,
}

impl Default for ChainlinkOracleConfig {
    fn default() -> Self {
        Self {
            enabled: default_chainlink_oracle_enabled(),
            max_quote_age_ms: default_chainlink_oracle_max_quote_age_ms(),
            max_window_open_lag_ms: default_chainlink_oracle_max_window_open_lag_ms(),
            max_settlement_close_lag_ms: default_chainlink_oracle_max_settlement_close_lag_ms(),
        }
    }
}

/// Optional rules that constrain repeated entries in the same market window.
#[derive(Debug, Clone, Deserialize)]
pub struct ScaleInConfig {
    #[serde(default = "default_scale_in_enabled")]
    pub enabled: bool,
    #[serde(default = "default_scale_in_max_additional_entries_per_window")]
    pub max_additional_entries_per_window: u32,
    #[serde(default = "default_scale_in_min_price_improvement")]
    pub min_price_improvement: Decimal,
    #[serde(default = "default_scale_in_require_stronger_binance_impulse")]
    pub require_stronger_binance_impulse: bool,
    #[serde(default = "default_scale_in_min_impulse_improvement_bps")]
    pub min_impulse_improvement_bps: Decimal,
}

impl Default for ScaleInConfig {
    fn default() -> Self {
        Self {
            enabled: default_scale_in_enabled(),
            max_additional_entries_per_window: default_scale_in_max_additional_entries_per_window(),
            min_price_improvement: default_scale_in_min_price_improvement(),
            require_stronger_binance_impulse: default_scale_in_require_stronger_binance_impulse(),
            min_impulse_improvement_bps: default_scale_in_min_impulse_improvement_bps(),
        }
    }
}

/// Runtime strategy-selection mode used by adaptive execution.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeRegime {
    Safe,
    Aggressive,
}

impl RuntimeRegime {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Safe => "safe",
            Self::Aggressive => "aggressive",
        }
    }
}

/// Dynamic mode switching for runtime execution.
#[derive(Debug, Clone, Deserialize)]
pub struct AdaptiveRegimeConfig {
    #[serde(default = "default_adaptive_regime_enabled")]
    pub enabled: bool,
    #[serde(default = "default_aggressive_min_spot_move_bps")]
    pub aggressive_min_spot_move_bps: u32,
    #[serde(default = "default_aggressive_max_bundle_cost")]
    pub aggressive_max_bundle_cost: Decimal,
    #[serde(default = "default_safe_max_bundle_cost")]
    pub safe_max_bundle_cost: Decimal,
    #[serde(default = "default_safe_bundle_only")]
    pub safe_bundle_only: bool,
    #[serde(default = "default_safe_max_entries_per_cycle")]
    pub safe_max_entries_per_cycle: usize,
}

impl Default for AdaptiveRegimeConfig {
    fn default() -> Self {
        Self {
            enabled: default_adaptive_regime_enabled(),
            aggressive_min_spot_move_bps: default_aggressive_min_spot_move_bps(),
            aggressive_max_bundle_cost: default_aggressive_max_bundle_cost(),
            safe_max_bundle_cost: default_safe_max_bundle_cost(),
            safe_bundle_only: default_safe_bundle_only(),
            safe_max_entries_per_cycle: default_safe_max_entries_per_cycle(),
        }
    }
}

/// PnL-aware notional caps that keep aggression behind realized profit.
#[derive(Debug, Clone, Deserialize)]
pub struct PnlRatchetConfig {
    #[serde(default = "default_pnl_ratchet_enabled")]
    pub enabled: bool,
    #[serde(default = "default_pnl_ratchet_apply_to_codex_sentinel_only")]
    pub apply_to_codex_sentinel_only: bool,
    #[serde(default = "default_pnl_ratchet_base_notional_usdc")]
    pub base_notional_usdc: Decimal,
    #[serde(default = "default_pnl_ratchet_protect_notional_usdc")]
    pub protect_notional_usdc: Decimal,
    #[serde(default = "default_pnl_ratchet_profit_unlock_usdc")]
    pub profit_unlock_usdc: Decimal,
    #[serde(default = "default_pnl_ratchet_protect_after_consecutive_losses")]
    pub protect_after_consecutive_losses: u32,
}

impl Default for PnlRatchetConfig {
    fn default() -> Self {
        Self {
            enabled: default_pnl_ratchet_enabled(),
            apply_to_codex_sentinel_only: default_pnl_ratchet_apply_to_codex_sentinel_only(),
            base_notional_usdc: default_pnl_ratchet_base_notional_usdc(),
            protect_notional_usdc: default_pnl_ratchet_protect_notional_usdc(),
            profit_unlock_usdc: default_pnl_ratchet_profit_unlock_usdc(),
            protect_after_consecutive_losses: default_pnl_ratchet_protect_after_consecutive_losses(
            ),
        }
    }
}

/// Risk limits that can temporarily halt new entries.
#[derive(Debug, Clone, Deserialize)]
pub struct RiskControlConfig {
    #[serde(default = "default_max_daily_loss_usdc")]
    pub max_daily_loss_usdc: Decimal,
    #[serde(default = "default_max_session_loss_usdc")]
    pub max_session_loss_usdc: Decimal,
    #[serde(default = "default_max_open_notional_usdc")]
    pub max_open_notional_usdc: Decimal,
    #[serde(default = "default_max_unrealized_loss_usdc")]
    pub max_unrealized_loss_usdc: Decimal,
    #[serde(default = "default_max_consecutive_losses")]
    pub max_consecutive_losses: u32,
    #[serde(default = "default_risk_cooldown_cycles")]
    pub cooldown_cycles: usize,
    #[serde(default = "default_risk_apply_in_live_mode")]
    pub apply_in_live_mode: bool,
    #[serde(default = "default_risk_seed_from_history")]
    pub seed_from_history: bool,
    #[serde(default = "default_risk_reset_daily_on_start")]
    pub reset_daily_on_start: bool,
}

impl Default for RiskControlConfig {
    fn default() -> Self {
        Self {
            max_daily_loss_usdc: default_max_daily_loss_usdc(),
            max_session_loss_usdc: default_max_session_loss_usdc(),
            max_open_notional_usdc: default_max_open_notional_usdc(),
            max_unrealized_loss_usdc: default_max_unrealized_loss_usdc(),
            max_consecutive_losses: default_max_consecutive_losses(),
            cooldown_cycles: default_risk_cooldown_cycles(),
            apply_in_live_mode: default_risk_apply_in_live_mode(),
            seed_from_history: default_risk_seed_from_history(),
            reset_daily_on_start: default_risk_reset_daily_on_start(),
        }
    }
}

/// Inventory-aware runtime guardrails for the first `v4` paper loop.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Deserialize)]
pub struct V4InventoryConfig {
    #[serde(default = "default_v4_inventory_enabled")]
    pub enabled: bool,
    #[serde(default = "default_v4_inventory_max_gross_inventory_shares_per_window")]
    pub max_gross_inventory_shares_per_window: Decimal,
    #[serde(default = "default_v4_inventory_max_directional_delta_shares_per_window")]
    pub max_directional_delta_shares_per_window: Decimal,
    #[serde(default = "default_v4_inventory_max_window_spent_usdc")]
    pub max_window_spent_usdc: Decimal,
    #[serde(default = "default_v4_inventory_max_entries_per_window")]
    pub max_entries_per_window: u32,
    #[serde(default = "default_v4_inventory_cooldown_secs")]
    pub cooldown_secs: i64,
    #[serde(default = "default_v4_inventory_cooldown_on_stop_loss")]
    pub cooldown_on_stop_loss: bool,
    #[serde(default = "default_v4_inventory_cooldown_on_reversal")]
    pub cooldown_on_reversal: bool,
    #[serde(default = "default_v4_inventory_cooldown_on_partial_reversal")]
    pub cooldown_on_partial_reversal: bool,
}

impl Default for V4InventoryConfig {
    fn default() -> Self {
        Self {
            enabled: default_v4_inventory_enabled(),
            max_gross_inventory_shares_per_window:
                default_v4_inventory_max_gross_inventory_shares_per_window(),
            max_directional_delta_shares_per_window:
                default_v4_inventory_max_directional_delta_shares_per_window(),
            max_window_spent_usdc: default_v4_inventory_max_window_spent_usdc(),
            max_entries_per_window: default_v4_inventory_max_entries_per_window(),
            cooldown_secs: default_v4_inventory_cooldown_secs(),
            cooldown_on_stop_loss: default_v4_inventory_cooldown_on_stop_loss(),
            cooldown_on_reversal: default_v4_inventory_cooldown_on_reversal(),
            cooldown_on_partial_reversal: default_v4_inventory_cooldown_on_partial_reversal(),
        }
    }
}

/// Early-exit rules for paper/live position management.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Deserialize)]
pub struct EarlyExitConfig {
    #[serde(default = "default_early_exit_enabled")]
    pub enabled: bool,
    #[serde(default = "default_early_exit_min_hold_secs")]
    pub min_hold_secs: i64,
    #[serde(default = "default_early_exit_min_take_profit_usdc")]
    pub min_take_profit_usdc: Decimal,
    #[serde(default = "default_early_exit_min_expected_profit_capture_ratio")]
    pub min_expected_profit_capture_ratio: Decimal,
    #[serde(default = "default_early_exit_max_loss_usdc")]
    pub max_loss_usdc: Decimal,
    #[serde(default = "default_early_exit_profit_lock_partial_close_enabled")]
    pub profit_lock_partial_close_enabled: bool,
    #[serde(default = "default_early_exit_profit_lock_partial_close_ratio")]
    pub profit_lock_partial_close_ratio: Decimal,
    #[serde(default = "default_early_exit_profit_lock_min_profit_usdc")]
    pub profit_lock_min_profit_usdc: Decimal,
    #[serde(default = "default_early_exit_reversal_min_5s_bps")]
    pub reversal_min_5s_bps: Decimal,
    #[serde(default = "default_early_exit_bonereaper_state_v2_stop_loss_min_15s_bps")]
    pub bonereaper_state_v2_stop_loss_min_15s_bps: Decimal,
    #[serde(default = "default_early_exit_bonereaper_state_v2_reversal_min_15s_bps")]
    pub bonereaper_state_v2_reversal_min_15s_bps: Decimal,
    #[serde(default = "default_early_exit_directional_partial_reversal_enabled")]
    pub directional_partial_reversal_enabled: bool,
    #[serde(default = "default_early_exit_directional_partial_close_ratio")]
    pub directional_partial_close_ratio: Decimal,
    #[serde(default = "default_early_exit_directional_partial_reversal_5s_bps")]
    pub directional_partial_reversal_5s_bps: Decimal,
    #[serde(default = "default_early_exit_directional_partial_reversal_15s_bps")]
    pub directional_partial_reversal_15s_bps: Decimal,
    #[serde(default = "default_early_exit_micro_breakout_partial_reversal_enabled")]
    pub micro_breakout_partial_reversal_enabled: bool,
    #[serde(default = "default_early_exit_micro_breakout_partial_close_ratio")]
    pub micro_breakout_partial_close_ratio: Decimal,
    #[serde(default = "default_early_exit_micro_breakout_partial_reversal_5s_bps")]
    pub micro_breakout_partial_reversal_5s_bps: Decimal,
    #[serde(default = "default_early_exit_micro_breakout_partial_reversal_15s_bps")]
    pub micro_breakout_partial_reversal_15s_bps: Decimal,
    #[serde(default = "default_early_exit_micro_breakout_fail_fast_1s_bps")]
    pub micro_breakout_fail_fast_1s_bps: Decimal,
    #[serde(default = "default_early_exit_micro_breakout_fail_fast_15s_bps")]
    pub micro_breakout_fail_fast_15s_bps: Decimal,
    #[serde(default = "default_early_exit_micro_breakout_fail_fast_profit_buffer_usdc")]
    pub micro_breakout_fail_fast_profit_buffer_usdc: Decimal,
    #[serde(default = "default_early_exit_peak_exit_enabled")]
    pub peak_exit_enabled: bool,
    #[serde(default = "default_early_exit_peak_exit_partial_close_enabled")]
    pub peak_exit_partial_close_enabled: bool,
    #[serde(default = "default_early_exit_peak_exit_partial_close_ratio")]
    pub peak_exit_partial_close_ratio: Decimal,
    #[serde(default = "default_early_exit_peak_exit_min_profit_usdc")]
    pub peak_exit_min_profit_usdc: Decimal,
    #[serde(default = "default_early_exit_peak_exit_min_primary_ask_price")]
    pub peak_exit_min_primary_ask_price: Decimal,
    #[serde(default = "default_early_exit_peak_exit_max_aligned_1s_bps")]
    pub peak_exit_max_aligned_1s_bps: Decimal,
    #[serde(default = "default_early_exit_peak_exit_max_aligned_5s_bps")]
    pub peak_exit_max_aligned_5s_bps: Decimal,
    #[serde(default = "default_early_exit_peak_exit_max_acceleration_bps")]
    pub peak_exit_max_acceleration_bps: Decimal,
    #[serde(default = "default_early_exit_exhaustion_exit_enabled")]
    pub exhaustion_exit_enabled: bool,
    #[serde(default = "default_early_exit_exhaustion_exit_min_profit_usdc")]
    pub exhaustion_exit_min_profit_usdc: Decimal,
    #[serde(default = "default_early_exit_exhaustion_exit_max_aligned_1s_bps")]
    pub exhaustion_exit_max_aligned_1s_bps: Decimal,
    #[serde(default = "default_early_exit_exhaustion_exit_max_aligned_5s_bps")]
    pub exhaustion_exit_max_aligned_5s_bps: Decimal,
    #[serde(default = "default_early_exit_exhaustion_exit_max_aligned_15s_bps")]
    pub exhaustion_exit_max_aligned_15s_bps: Decimal,
    #[serde(default = "default_early_exit_exhaustion_exit_max_acceleration_bps")]
    pub exhaustion_exit_max_acceleration_bps: Decimal,
    #[serde(default = "default_early_exit_stop_and_reverse_enabled")]
    pub stop_and_reverse_enabled: bool,
    #[serde(default = "default_early_exit_stop_and_reverse_on_stop_loss")]
    pub stop_and_reverse_on_stop_loss: bool,
    #[serde(default = "default_early_exit_stop_and_reverse_size_ratio")]
    pub stop_and_reverse_size_ratio: Decimal,
    #[serde(default = "default_early_exit_stop_and_reverse_min_seconds_left")]
    pub stop_and_reverse_min_seconds_left: i64,
    #[serde(default = "default_early_exit_scalp_exit_enabled")]
    pub scalp_exit_enabled: bool,
    #[serde(default = "default_early_exit_scalp_exit_apply_to_codex_sentinel_only")]
    pub scalp_exit_apply_to_codex_sentinel_only: bool,
    #[serde(default = "default_early_exit_scalp_take_profit_price_delta")]
    pub scalp_take_profit_price_delta: Decimal,
    #[serde(default = "default_early_exit_scalp_stop_loss_price_delta")]
    pub scalp_stop_loss_price_delta: Decimal,
    #[serde(default = "default_early_exit_scalp_time_stop_secs")]
    pub scalp_time_stop_secs: i64,
    #[serde(default = "default_early_exit_scalp_invalidation_exit_enabled")]
    pub scalp_invalidation_exit_enabled: bool,
    #[serde(default = "default_early_exit_scalp_invalidation_min_loss_usdc")]
    pub scalp_invalidation_min_loss_usdc: Decimal,
    #[serde(default = "default_early_exit_scalp_invalidation_opposite_gap_bps")]
    pub scalp_invalidation_opposite_gap_bps: Decimal,
    #[serde(default = "default_early_exit_scalp_invalidation_opposite_5s_bps")]
    pub scalp_invalidation_opposite_5s_bps: Decimal,
    #[serde(default = "default_early_exit_near_expiry_secs")]
    pub near_expiry_secs: i64,
}

impl Default for EarlyExitConfig {
    fn default() -> Self {
        Self {
            enabled: default_early_exit_enabled(),
            min_hold_secs: default_early_exit_min_hold_secs(),
            min_take_profit_usdc: default_early_exit_min_take_profit_usdc(),
            min_expected_profit_capture_ratio: default_early_exit_min_expected_profit_capture_ratio(
            ),
            max_loss_usdc: default_early_exit_max_loss_usdc(),
            profit_lock_partial_close_enabled: default_early_exit_profit_lock_partial_close_enabled(
            ),
            profit_lock_partial_close_ratio: default_early_exit_profit_lock_partial_close_ratio(),
            profit_lock_min_profit_usdc: default_early_exit_profit_lock_min_profit_usdc(),
            reversal_min_5s_bps: default_early_exit_reversal_min_5s_bps(),
            bonereaper_state_v2_stop_loss_min_15s_bps:
                default_early_exit_bonereaper_state_v2_stop_loss_min_15s_bps(),
            bonereaper_state_v2_reversal_min_15s_bps:
                default_early_exit_bonereaper_state_v2_reversal_min_15s_bps(),
            directional_partial_reversal_enabled:
                default_early_exit_directional_partial_reversal_enabled(),
            directional_partial_close_ratio: default_early_exit_directional_partial_close_ratio(),
            directional_partial_reversal_5s_bps:
                default_early_exit_directional_partial_reversal_5s_bps(),
            directional_partial_reversal_15s_bps:
                default_early_exit_directional_partial_reversal_15s_bps(),
            micro_breakout_partial_reversal_enabled:
                default_early_exit_micro_breakout_partial_reversal_enabled(),
            micro_breakout_partial_close_ratio:
                default_early_exit_micro_breakout_partial_close_ratio(),
            micro_breakout_partial_reversal_5s_bps:
                default_early_exit_micro_breakout_partial_reversal_5s_bps(),
            micro_breakout_partial_reversal_15s_bps:
                default_early_exit_micro_breakout_partial_reversal_15s_bps(),
            micro_breakout_fail_fast_1s_bps: default_early_exit_micro_breakout_fail_fast_1s_bps(),
            micro_breakout_fail_fast_15s_bps: default_early_exit_micro_breakout_fail_fast_15s_bps(),
            micro_breakout_fail_fast_profit_buffer_usdc:
                default_early_exit_micro_breakout_fail_fast_profit_buffer_usdc(),
            peak_exit_enabled: default_early_exit_peak_exit_enabled(),
            peak_exit_partial_close_enabled: default_early_exit_peak_exit_partial_close_enabled(),
            peak_exit_partial_close_ratio: default_early_exit_peak_exit_partial_close_ratio(),
            peak_exit_min_profit_usdc: default_early_exit_peak_exit_min_profit_usdc(),
            peak_exit_min_primary_ask_price: default_early_exit_peak_exit_min_primary_ask_price(),
            peak_exit_max_aligned_1s_bps: default_early_exit_peak_exit_max_aligned_1s_bps(),
            peak_exit_max_aligned_5s_bps: default_early_exit_peak_exit_max_aligned_5s_bps(),
            peak_exit_max_acceleration_bps: default_early_exit_peak_exit_max_acceleration_bps(),
            exhaustion_exit_enabled: default_early_exit_exhaustion_exit_enabled(),
            exhaustion_exit_min_profit_usdc: default_early_exit_exhaustion_exit_min_profit_usdc(),
            exhaustion_exit_max_aligned_1s_bps:
                default_early_exit_exhaustion_exit_max_aligned_1s_bps(),
            exhaustion_exit_max_aligned_5s_bps:
                default_early_exit_exhaustion_exit_max_aligned_5s_bps(),
            exhaustion_exit_max_aligned_15s_bps:
                default_early_exit_exhaustion_exit_max_aligned_15s_bps(),
            exhaustion_exit_max_acceleration_bps:
                default_early_exit_exhaustion_exit_max_acceleration_bps(),
            stop_and_reverse_enabled: default_early_exit_stop_and_reverse_enabled(),
            stop_and_reverse_on_stop_loss: default_early_exit_stop_and_reverse_on_stop_loss(),
            stop_and_reverse_size_ratio: default_early_exit_stop_and_reverse_size_ratio(),
            stop_and_reverse_min_seconds_left: default_early_exit_stop_and_reverse_min_seconds_left(
            ),
            scalp_exit_enabled: default_early_exit_scalp_exit_enabled(),
            scalp_exit_apply_to_codex_sentinel_only:
                default_early_exit_scalp_exit_apply_to_codex_sentinel_only(),
            scalp_take_profit_price_delta: default_early_exit_scalp_take_profit_price_delta(),
            scalp_stop_loss_price_delta: default_early_exit_scalp_stop_loss_price_delta(),
            scalp_time_stop_secs: default_early_exit_scalp_time_stop_secs(),
            scalp_invalidation_exit_enabled: default_early_exit_scalp_invalidation_exit_enabled(),
            scalp_invalidation_min_loss_usdc: default_early_exit_scalp_invalidation_min_loss_usdc(),
            scalp_invalidation_opposite_gap_bps:
                default_early_exit_scalp_invalidation_opposite_gap_bps(),
            scalp_invalidation_opposite_5s_bps:
                default_early_exit_scalp_invalidation_opposite_5s_bps(),
            near_expiry_secs: default_early_exit_near_expiry_secs(),
        }
    }
}

/// Live trading settings.
#[derive(Debug, Clone, Deserialize)]
pub struct LiveConfig {
    #[serde(default = "default_private_key_env")]
    pub private_key_env: String,
    #[serde(default = "default_api_key_env")]
    pub api_key_env: String,
    #[serde(default = "default_api_secret_env")]
    pub api_secret_env: String,
    #[serde(default = "default_api_passphrase_env")]
    pub api_passphrase_env: String,
    #[serde(default = "default_prompt_for_secrets")]
    pub prompt_for_secrets: bool,
    #[serde(default = "default_live_signature_type")]
    pub signature_type: LiveSignatureType,
    #[serde(default)]
    pub funder_address: Option<String>,
}

/// `PolyBackTest` integration settings.
#[derive(Debug, Clone, Deserialize)]
pub struct PolyBacktestConfig {
    #[serde(default = "default_polybacktest_base_url")]
    pub base_url: String,
    #[serde(default = "default_polybacktest_api_key_env")]
    pub api_key_env: String,
    #[serde(default = "default_prompt_for_polybacktest_api_key")]
    pub prompt_for_api_key: bool,
    #[serde(default = "default_polybacktest_snapshot_page_limit")]
    pub snapshot_page_limit: usize,
    #[serde(default = "default_polybacktest_include_orderbook")]
    pub include_orderbook: bool,
    #[serde(default = "default_polybacktest_cache_enabled")]
    pub cache_enabled: bool,
    #[serde(default = "default_polybacktest_cache_dir")]
    pub cache_dir: PathBuf,
}

impl Default for PolyBacktestConfig {
    fn default() -> Self {
        Self {
            base_url: default_polybacktest_base_url(),
            api_key_env: default_polybacktest_api_key_env(),
            prompt_for_api_key: default_prompt_for_polybacktest_api_key(),
            snapshot_page_limit: default_polybacktest_snapshot_page_limit(),
            include_orderbook: default_polybacktest_include_orderbook(),
            cache_enabled: default_polybacktest_cache_enabled(),
            cache_dir: default_polybacktest_cache_dir(),
        }
    }
}

/// Signature type used when signing Polymarket orders.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LiveSignatureType {
    /// Sign as a regular externally owned account.
    Eoa,
    /// Sign as a Polymarket proxy wallet.
    Proxy,
    /// Sign as a Gnosis Safe wallet.
    GnosisSafe,
}

/// Persistent storage settings.
#[derive(Debug, Clone, Deserialize)]
pub struct StorageConfig {
    #[serde(default = "default_state_dir")]
    pub state_dir: PathBuf,
    #[serde(default = "default_execution_journal_filename")]
    pub execution_journal_filename: String,
    #[serde(default = "default_pnl_snapshot_filename")]
    pub pnl_snapshot_filename: String,
    #[serde(default = "default_paper_cycle_journal_filename")]
    pub paper_cycle_journal_filename: String,
    #[serde(default = "default_paper_cycle_journal_sample_secs")]
    pub paper_cycle_journal_sample_secs: u64,
    #[serde(default = "default_paper_cycle_journal_max_bytes")]
    pub paper_cycle_journal_max_bytes: u64,
    #[serde(default = "default_paper_cycle_journal_max_rotated_files")]
    pub paper_cycle_journal_max_rotated_files: usize,
    #[serde(default = "default_paper_trade_journal_filename")]
    pub paper_trade_journal_filename: String,
}

fn default_gamma_base_url() -> String {
    DEFAULT_GAMMA_BASE_URL.to_owned()
}

fn default_clob_base_url() -> String {
    DEFAULT_CLOB_BASE_URL.to_owned()
}

fn default_data_api_base_url() -> String {
    DEFAULT_DATA_API_BASE_URL.to_owned()
}

fn default_geoblock_url() -> String {
    DEFAULT_GEOBLOCK_URL.to_owned()
}

fn default_binance_base_url() -> String {
    DEFAULT_BINANCE_BASE_URL.to_owned()
}

fn default_binance_ws_base_url() -> String {
    DEFAULT_BINANCE_WS_BASE_URL.to_owned()
}

fn default_coinbase_ws_base_url() -> String {
    DEFAULT_COINBASE_WS_BASE_URL.to_owned()
}

fn default_polymarket_rtds_ws_url() -> String {
    DEFAULT_POLYMARKET_RTDS_WS_URL.to_owned()
}

fn default_coinbase_max_source_disagreement_bps() -> Decimal {
    Decimal::new(25, 0)
}

fn default_coinbase_max_spread_bps() -> Decimal {
    Decimal::new(5, 0)
}

fn default_polybacktest_base_url() -> String {
    DEFAULT_POLYBACKTEST_BASE_URL.to_owned()
}

const fn default_timeout_secs() -> u64 {
    15
}

const fn default_page_size() -> usize {
    200
}

const fn default_books_batch_size() -> usize {
    100
}

fn default_private_key_env() -> String {
    "POLYMARKET_PRIVATE_KEY".to_owned()
}

fn default_api_key_env() -> String {
    "POLYMARKET_API_KEY".to_owned()
}

fn default_api_secret_env() -> String {
    "POLYMARKET_API_SECRET".to_owned()
}

fn default_api_passphrase_env() -> String {
    "POLYMARKET_API_PASSPHRASE".to_owned()
}

fn default_polybacktest_api_key_env() -> String {
    "POLYBACKTEST_API_KEY".to_owned()
}

const fn default_prompt_for_secrets() -> bool {
    true
}

const fn default_enable_bundle() -> bool {
    true
}

const fn default_directional_min_velocity_bps_per_minute() -> u32 {
    6
}

const fn default_directional_min_signal_bps() -> u32 {
    12
}

fn default_directional_soft_entry_min_notional_usdc() -> Decimal {
    Decimal::new(25, 0)
}

fn default_directional_soft_entry_max_notional_usdc() -> Decimal {
    Decimal::new(35, 0)
}

fn default_directional_strong_signal_min_spot_move_5s_bps() -> Decimal {
    Decimal::new(15, 1)
}

fn default_directional_strong_signal_min_trade_flow_bps() -> Decimal {
    Decimal::new(15, 1)
}

fn default_directional_soft_entry_signal_window_bps() -> Decimal {
    Decimal::new(60, 1)
}

fn default_directional_projection_cap_multiplier() -> Decimal {
    Decimal::new(20, 1)
}

fn default_directional_trade_flow_weight() -> Decimal {
    Decimal::new(35, 2)
}

fn default_directional_micro_signal_weight() -> Decimal {
    Decimal::new(50, 2)
}

fn default_directional_micro_burst_weight() -> Decimal {
    Decimal::ZERO
}

const fn default_directional_require_hedge_for_soft_entry() -> bool {
    false
}

const fn default_enable_micro_breakout() -> bool {
    true
}

const fn default_micro_breakout_min_spot_move_bps() -> u32 {
    2
}

fn default_micro_breakout_min_spot_move_5s_bps() -> Decimal {
    Decimal::new(10, 1)
}

fn default_micro_breakout_min_spot_move_1s_bps() -> Decimal {
    Decimal::ZERO
}

const fn default_micro_breakout_min_signal_bps() -> u32 {
    4
}

fn default_micro_breakout_signal_boost_multiplier() -> Decimal {
    Decimal::new(20, 1)
}

fn default_micro_breakout_signal_burst_multiplier() -> Decimal {
    Decimal::ZERO
}

fn default_micro_breakout_max_burst_to_micro_ratio() -> Decimal {
    Decimal::ZERO
}

fn default_micro_breakout_target_cross_min_gap_bps() -> Decimal {
    Decimal::ZERO
}

fn default_micro_breakout_target_cross_signal_boost_bps() -> Decimal {
    Decimal::ZERO
}

fn default_micro_breakout_max_entry_price() -> Decimal {
    Decimal::new(92, 2)
}

fn default_micro_breakout_max_average_price_drift() -> Decimal {
    Decimal::ZERO
}

const fn default_micro_breakout_min_elapsed_window_secs() -> i64 {
    0
}

fn default_micro_breakout_weak_notional_usdc() -> Decimal {
    Decimal::new(5, 0)
}

fn default_micro_breakout_normal_notional_usdc() -> Decimal {
    Decimal::new(7, 0)
}

fn default_micro_breakout_strong_notional_usdc() -> Decimal {
    Decimal::new(10, 0)
}

fn default_micro_breakout_expensive_entry_price() -> Decimal {
    Decimal::new(75, 2)
}

const fn default_micro_breakout_expensive_entry_requires_strong_tier() -> bool {
    false
}

fn default_micro_breakout_full_size_max_entry_price() -> Decimal {
    Decimal::new(70, 2)
}

fn default_micro_breakout_strong_signal_min_spot_move_5s_bps() -> Decimal {
    Decimal::new(15, 1)
}

fn default_micro_breakout_strong_signal_min_spot_move_1s_bps() -> Decimal {
    Decimal::ZERO
}

fn default_micro_breakout_strong_signal_min_spot_move_15s_bps() -> Decimal {
    Decimal::new(15, 1)
}

const fn default_enable_target_state_v1() -> bool {
    false
}

const fn default_target_state_min_elapsed_window_secs() -> i64 {
    120
}

const fn default_target_state_max_seconds_left() -> i64 {
    150
}

fn default_target_state_min_target_gap_bps() -> Decimal {
    Decimal::new(5, 0)
}

const fn default_target_state_min_signal_bps() -> u32 {
    10
}

fn default_target_state_min_spot_move_15s_bps() -> Decimal {
    Decimal::new(8, 1)
}

fn default_target_state_min_aligned_flow_bps() -> Decimal {
    Decimal::new(2, 1)
}

fn default_target_state_max_entry_price() -> Decimal {
    Decimal::new(74, 2)
}

fn default_target_state_normal_notional_usdc() -> Decimal {
    Decimal::new(12, 0)
}

fn default_target_state_strong_notional_usdc() -> Decimal {
    Decimal::new(20, 0)
}

fn default_target_state_strong_gap_bps() -> Decimal {
    Decimal::new(12, 0)
}

const fn default_enable_bonereaper_state_v1() -> bool {
    false
}

const fn default_bonereaper_state_min_elapsed_window_secs() -> i64 {
    20
}

const fn default_bonereaper_state_max_seconds_left() -> i64 {
    270
}

fn default_bonereaper_state_min_target_gap_bps() -> Decimal {
    Decimal::new(2, 0)
}

const fn default_bonereaper_state_min_signal_bps() -> u32 {
    6
}

fn default_bonereaper_state_min_spot_move_15s_bps() -> Decimal {
    Decimal::new(4, 1)
}

fn default_bonereaper_state_min_spot_move_5s_bps() -> Decimal {
    Decimal::new(2, 1)
}

fn default_bonereaper_state_min_aligned_flow_bps() -> Decimal {
    Decimal::ZERO
}

fn default_bonereaper_state_max_entry_price() -> Decimal {
    Decimal::new(86, 2)
}

fn default_bonereaper_state_normal_notional_usdc() -> Decimal {
    Decimal::new(15, 0)
}

fn default_bonereaper_state_strong_notional_usdc() -> Decimal {
    Decimal::new(25, 0)
}

fn default_bonereaper_state_strong_gap_bps() -> Decimal {
    Decimal::new(8, 0)
}

fn default_bonereaper_state_strong_flow_bps() -> Decimal {
    Decimal::new(10, 1)
}

const fn default_enable_bonereaper_state_v2() -> bool {
    false
}

const fn default_enable_bonereaper_state_guarded() -> bool {
    false
}

const fn default_enable_codex_sentinel_v1() -> bool {
    false
}

const fn default_codex_sentinel_v1_mid_signal_guard_enabled() -> bool {
    true
}

fn default_codex_sentinel_v1_mid_signal_min_bps() -> Decimal {
    Decimal::new(28, 1)
}

fn default_codex_sentinel_v1_mid_signal_max_bps() -> Decimal {
    Decimal::new(36, 1)
}

fn default_codex_sentinel_v1_mid_signal_min_confirmation_bps() -> Decimal {
    Decimal::new(5, 2)
}

fn default_codex_sentinel_v1_max_entry_price() -> Decimal {
    Decimal::new(76, 2)
}

const fn default_codex_sentinel_v1_live_quote_age_guard_enabled() -> bool {
    false
}

const fn default_codex_sentinel_v1_max_live_quote_age_ms() -> i64 {
    1_000
}

const fn default_codex_sentinel_v1_entry_spread_guard_enabled() -> bool {
    false
}

fn default_codex_sentinel_v1_max_entry_spread() -> Decimal {
    Decimal::new(8, 2)
}

const fn default_codex_sentinel_v1_stale_micro_guard_enabled() -> bool {
    true
}

fn default_codex_sentinel_v1_stale_micro_max_confirmation_bps() -> Decimal {
    Decimal::new(5, 2)
}

fn default_codex_sentinel_v1_stale_micro_discount_max_entry_price() -> Decimal {
    Decimal::new(55, 2)
}

fn default_codex_sentinel_v1_stale_micro_discount_min_signal_bps() -> Decimal {
    Decimal::from(450)
}

fn default_codex_sentinel_v1_stale_micro_discount_min_flow_bps() -> Decimal {
    Decimal::from(700)
}

fn default_codex_sentinel_v1_stale_micro_min_signal_bps() -> Decimal {
    Decimal::from(800)
}

fn default_codex_sentinel_v1_stale_micro_min_flow_bps() -> Decimal {
    Decimal::from(1400)
}

fn default_codex_sentinel_v1_stale_micro_max_non_discount_entry_price() -> Decimal {
    Decimal::new(65, 2)
}

fn default_codex_sentinel_v1_stale_micro_min_swing_bps() -> Decimal {
    Decimal::new(75, 2)
}

fn default_codex_sentinel_v1_stale_micro_min_target_gap_bps() -> Decimal {
    Decimal::new(75, 2)
}

const fn default_codex_sentinel_v1_expensive_entry_guard_enabled() -> bool {
    true
}

fn default_codex_sentinel_v1_expensive_entry_price() -> Decimal {
    Decimal::new(65, 2)
}

fn default_codex_sentinel_v1_expensive_min_micro_bps() -> Decimal {
    Decimal::new(125, 2)
}

fn default_codex_sentinel_v1_expensive_min_swing_bps() -> Decimal {
    Decimal::new(125, 2)
}

const fn default_codex_sentinel_v1_premium_entry_guard_enabled() -> bool {
    false
}

fn default_codex_sentinel_v1_premium_entry_price() -> Decimal {
    Decimal::new(55, 2)
}

fn default_codex_sentinel_v1_premium_min_signal_bps() -> Decimal {
    Decimal::from(800_u32)
}

fn default_codex_sentinel_v1_premium_min_flow_bps() -> Decimal {
    Decimal::from(1400_u32)
}

fn default_codex_sentinel_v1_premium_min_fresh_bps() -> Decimal {
    Decimal::new(125, 2)
}

const fn default_codex_sentinel_v1_aggressive_continuation_enabled() -> bool {
    false
}

fn default_codex_sentinel_v1_aggressive_continuation_max_entry_price() -> Decimal {
    Decimal::new(62, 2)
}

fn default_codex_sentinel_v1_aggressive_continuation_min_target_gap_bps() -> Decimal {
    Decimal::new(600, 2)
}

fn default_codex_sentinel_v1_aggressive_continuation_min_signal_bps() -> Decimal {
    Decimal::from(1_500_u32)
}

fn default_codex_sentinel_v1_aggressive_continuation_min_flow_bps() -> Decimal {
    Decimal::from(2_200_u32)
}

fn default_codex_sentinel_v1_aggressive_continuation_min_fresh_bps() -> Decimal {
    Decimal::new(350, 2)
}

fn default_codex_sentinel_v1_aggressive_continuation_min_swing_bps() -> Decimal {
    Decimal::new(350, 2)
}

const fn default_codex_sentinel_v1_aggressive_continuation_max_quote_age_ms() -> i64 {
    750
}

const fn default_codex_breakout_v1_enabled() -> bool {
    false
}

const fn default_codex_breakout_v1_required() -> bool {
    false
}

fn default_codex_breakout_v1_max_entry_price() -> Decimal {
    Decimal::new(58, 2)
}

const fn default_codex_breakout_v1_max_book_age_ms() -> i64 {
    750
}

fn default_codex_breakout_v1_max_spread_bps() -> Decimal {
    Decimal::new(20, 2)
}

fn default_codex_breakout_v1_min_score_bps() -> Decimal {
    Decimal::from(3_000_u32)
}

fn default_codex_breakout_v1_min_depth_imbalance_bps() -> Decimal {
    Decimal::from(1_800_u32)
}

fn default_codex_breakout_v1_min_microprice_bps() -> Decimal {
    Decimal::new(3, 4)
}

fn default_codex_breakout_v1_min_fresh_bps() -> Decimal {
    Decimal::new(100, 2)
}

fn default_codex_breakout_v1_min_target_gap_bps() -> Decimal {
    Decimal::new(100, 2)
}

fn default_codex_breakout_v1_min_signal_bps() -> Decimal {
    Decimal::ZERO
}

fn default_codex_breakout_v1_min_flow_bps() -> Decimal {
    Decimal::ZERO
}

const fn default_codex_sentinel_v1_discount_value_lane_enabled() -> bool {
    false
}

fn default_codex_sentinel_v1_discount_value_max_entry_price() -> Decimal {
    Decimal::new(50, 2)
}

const fn default_codex_sentinel_v1_discount_value_max_book_age_ms() -> i64 {
    750
}

fn default_codex_sentinel_v1_discount_value_max_exchange_spread_bps() -> Decimal {
    Decimal::new(300, 2)
}

fn default_codex_sentinel_v1_discount_value_min_target_gap_bps() -> Decimal {
    Decimal::new(120, 2)
}

fn default_codex_sentinel_v1_discount_value_min_fresh_bps() -> Decimal {
    Decimal::new(125, 2)
}

fn default_codex_sentinel_v1_discount_value_min_swing_bps() -> Decimal {
    Decimal::new(100, 2)
}

fn default_codex_sentinel_v1_discount_value_min_signal_bps() -> Decimal {
    Decimal::from(650_u32)
}

fn default_codex_sentinel_v1_discount_value_min_flow_bps() -> Decimal {
    Decimal::from(700_u32)
}

fn default_codex_sentinel_v1_discount_value_min_top_imbalance_bps() -> Decimal {
    Decimal::from(500_u32)
}

fn default_codex_sentinel_v1_discount_value_min_depth_imbalance_bps() -> Decimal {
    Decimal::from(700_u32)
}

fn default_codex_sentinel_v1_discount_value_min_microprice_bps() -> Decimal {
    Decimal::new(3, 4)
}

const fn default_enable_codex_scalp_probe_v1() -> bool {
    false
}

const fn default_codex_scalp_probe_v1_raw_ablation_enabled() -> bool {
    false
}

const fn default_codex_scalp_probe_v1_raw_light_enabled() -> bool {
    false
}

fn default_codex_scalp_probe_v1_min_entry_price() -> Decimal {
    Decimal::new(45, 2)
}

fn default_codex_scalp_probe_v1_max_entry_price() -> Decimal {
    Decimal::new(56, 2)
}

fn default_codex_scalp_probe_v1_max_entry_spread() -> Decimal {
    Decimal::new(10, 2)
}

const fn default_codex_scalp_probe_v1_min_elapsed_window_secs() -> i64 {
    8
}

const fn default_codex_scalp_probe_v1_max_seconds_left() -> i64 {
    270
}

const fn default_codex_scalp_probe_v1_min_seconds_left() -> i64 {
    210
}

const fn default_codex_scalp_probe_v1_max_book_age_ms() -> i64 {
    750
}

fn default_codex_scalp_probe_v1_max_exchange_spread_bps() -> Decimal {
    Decimal::new(3, 0)
}

fn default_codex_scalp_probe_v1_min_target_gap_bps() -> Decimal {
    Decimal::new(150, 2)
}

fn default_codex_scalp_probe_v1_min_fresh_bps() -> Decimal {
    Decimal::new(70, 2)
}

fn default_codex_scalp_probe_v1_min_signal_bps() -> Decimal {
    Decimal::from(450_u32)
}

fn default_codex_scalp_probe_v1_min_flow_bps() -> Decimal {
    Decimal::from(700_u32)
}

fn default_codex_scalp_probe_v1_min_top_imbalance_bps() -> Decimal {
    Decimal::from(900_u32)
}

fn default_codex_scalp_probe_v1_min_depth_imbalance_bps() -> Decimal {
    Decimal::from(1_000_u32)
}

fn default_codex_scalp_probe_v1_min_radar_score_bps() -> Decimal {
    Decimal::from(2_200_u32)
}

fn default_codex_scalp_probe_v1_notional_usdc() -> Decimal {
    Decimal::new(3, 0)
}

fn default_codex_scalp_probe_v1_min_expected_profit_usdc() -> Decimal {
    Decimal::new(12, 2)
}

const fn default_codex_scalp_probe_v1_bnb_pressure_enabled() -> bool {
    false
}

fn default_codex_scalp_probe_v1_bnb_pressure_max_entry_price() -> Decimal {
    Decimal::new(58, 2)
}

const fn default_codex_scalp_probe_v1_bnb_pressure_max_book_age_ms() -> i64 {
    400
}

fn default_codex_scalp_probe_v1_bnb_pressure_min_target_gap_bps() -> Decimal {
    Decimal::new(70, 2)
}

fn default_codex_scalp_probe_v1_bnb_pressure_min_fresh_bps() -> Decimal {
    Decimal::new(10, 2)
}

fn default_codex_scalp_probe_v1_bnb_pressure_min_top_imbalance_bps() -> Decimal {
    Decimal::from(1_500_u32)
}

fn default_codex_scalp_probe_v1_bnb_pressure_min_depth_imbalance_bps() -> Decimal {
    Decimal::from(1_300_u32)
}

fn default_codex_scalp_probe_v1_bnb_pressure_min_expected_profit_usdc() -> Decimal {
    Decimal::new(5, 2)
}

const fn default_codex_sentinel_v1_no_chase_guard_enabled() -> bool {
    false
}

fn default_codex_sentinel_v1_no_chase_entry_price() -> Decimal {
    Decimal::new(62, 2)
}

const fn default_codex_sentinel_v1_no_chase_min_seconds_left() -> i64 {
    240
}

fn default_codex_sentinel_v1_no_chase_allow_min_target_gap_bps() -> Decimal {
    Decimal::new(800, 2)
}

fn default_codex_sentinel_v1_no_chase_allow_min_fresh_bps() -> Decimal {
    Decimal::new(400, 2)
}

fn default_codex_sentinel_v1_no_chase_allow_min_signal_bps() -> Decimal {
    Decimal::from(2_500_u32)
}

fn default_codex_sentinel_v1_no_chase_allow_min_flow_bps() -> Decimal {
    Decimal::from(4_000_u32)
}

const fn default_codex_sentinel_v1_quality_floor_enabled() -> bool {
    false
}

fn default_codex_sentinel_v1_quality_floor_min_target_gap_bps() -> Decimal {
    Decimal::ZERO
}

fn default_codex_sentinel_v1_quality_floor_mid_gap_max_bps() -> Decimal {
    Decimal::ZERO
}

fn default_codex_sentinel_v1_quality_floor_mid_gap_min_signal_bps() -> Decimal {
    Decimal::ZERO
}

fn default_codex_sentinel_v1_quality_floor_mid_gap_min_flow_bps() -> Decimal {
    Decimal::ZERO
}

const fn default_codex_sentinel_v1_mid_gap_premium_guard_enabled() -> bool {
    false
}

fn default_codex_sentinel_v1_mid_gap_premium_min_target_gap_bps() -> Decimal {
    Decimal::new(150, 2)
}

fn default_codex_sentinel_v1_mid_gap_premium_max_target_gap_bps() -> Decimal {
    Decimal::new(300, 2)
}

fn default_codex_sentinel_v1_mid_gap_premium_entry_price() -> Decimal {
    Decimal::new(56, 2)
}

fn default_codex_sentinel_v1_mid_gap_premium_min_signal_bps() -> Decimal {
    Decimal::from(800_u32)
}

fn default_codex_sentinel_v1_mid_gap_premium_min_flow_bps() -> Decimal {
    Decimal::from(1200_u32)
}

fn default_codex_sentinel_v1_mid_gap_premium_min_fresh_bps() -> Decimal {
    Decimal::new(125, 2)
}

const fn default_codex_sentinel_v1_attack_size_enabled() -> bool {
    false
}

fn default_codex_sentinel_v1_attack_notional_usdc() -> Decimal {
    Decimal::new(10, 0)
}

fn default_codex_sentinel_v1_attack_min_signal_bps() -> Decimal {
    Decimal::new(650, 0)
}

fn default_codex_sentinel_v1_attack_min_flow_bps() -> Decimal {
    Decimal::new(700, 0)
}

fn default_codex_sentinel_v1_attack_min_confirmation_bps() -> Decimal {
    Decimal::new(5, 1)
}

fn default_codex_sentinel_v1_attack_max_entry_price() -> Decimal {
    Decimal::new(60, 2)
}

const fn default_codex_sentinel_v1_bad_window_guard_enabled() -> bool {
    false
}

fn default_codex_sentinel_v1_bad_window_min_score() -> Decimal {
    Decimal::new(32, 0)
}

const fn default_codex_sentinel_v1_confidence_sizing_enabled() -> bool {
    false
}

fn default_codex_sentinel_v1_confidence_min_score() -> Decimal {
    Decimal::new(40, 0)
}

fn default_codex_sentinel_v1_confidence_max_multiplier() -> Decimal {
    Decimal::ONE
}

const fn default_codex_sentinel_v1_low_flow_guard_enabled() -> bool {
    false
}

fn default_codex_sentinel_v1_low_flow_max_flow_bps() -> Decimal {
    Decimal::from(100_u32)
}

fn default_codex_sentinel_v1_low_flow_allow_min_signal_bps() -> Decimal {
    Decimal::from(40_u32)
}

fn default_codex_sentinel_v1_low_flow_allow_min_fresh_bps() -> Decimal {
    Decimal::new(300, 2)
}

fn default_codex_sentinel_v1_low_flow_allow_min_swing_bps() -> Decimal {
    Decimal::new(300, 2)
}

fn default_codex_sentinel_v1_low_flow_allow_max_entry_price() -> Decimal {
    Decimal::new(58, 2)
}

const fn default_codex_sentinel_v1_counter_burst_guard_enabled() -> bool {
    false
}

fn default_codex_sentinel_v1_counter_burst_min_bps() -> Decimal {
    Decimal::new(75, 2)
}

fn default_codex_sentinel_v1_counter_burst_max_entry_price() -> Decimal {
    Decimal::new(55, 2)
}

const fn default_codex_sentinel_v1_late_entry_override_enabled() -> bool {
    false
}

const fn default_codex_sentinel_v1_late_entry_min_seconds_left() -> i64 {
    60
}

fn default_codex_sentinel_v1_late_entry_max_entry_price() -> Decimal {
    Decimal::new(62, 2)
}

fn default_codex_sentinel_v1_late_entry_min_signal_bps() -> Decimal {
    Decimal::new(850, 0)
}

fn default_codex_sentinel_v1_late_entry_min_fresh_bps() -> Decimal {
    Decimal::new(150, 2)
}

fn default_codex_sentinel_v1_late_entry_min_flow_bps() -> Decimal {
    Decimal::ZERO
}

fn default_codex_sentinel_v1_late_entry_min_target_gap_bps() -> Decimal {
    Decimal::new(150, 2)
}

const fn default_codex_sentinel_v1_late_window_value_guard_enabled() -> bool {
    false
}

const fn default_codex_sentinel_v1_late_window_max_seconds_left() -> i64 {
    180
}

fn default_codex_sentinel_v1_late_window_max_entry_price() -> Decimal {
    Decimal::new(62, 2)
}

fn default_codex_sentinel_v1_late_window_allow_min_signal_bps() -> Decimal {
    Decimal::from(1_600_u32)
}

fn default_codex_sentinel_v1_late_window_allow_min_fresh_bps() -> Decimal {
    Decimal::new(200, 2)
}

fn default_codex_sentinel_v1_late_window_allow_min_flow_bps() -> Decimal {
    Decimal::from(2_500_u32)
}

fn default_codex_sentinel_v1_late_window_allow_min_target_gap_bps() -> Decimal {
    Decimal::new(300, 2)
}

const fn default_bonereaper_state_v2_min_elapsed_window_secs() -> i64 {
    8
}

const fn default_bonereaper_state_v2_max_seconds_left() -> i64 {
    295
}

const fn default_bonereaper_state_v2_min_seconds_left() -> i64 {
    0
}

fn default_bonereaper_state_v2_bias_min_target_gap_bps() -> Decimal {
    Decimal::new(15, 1)
}

fn default_bonereaper_state_v2_flip_max_target_gap_bps() -> Decimal {
    Decimal::new(3, 0)
}

const fn default_bonereaper_state_v2_min_signal_bps() -> u32 {
    4
}

fn default_bonereaper_state_v2_min_spot_move_15s_bps() -> Decimal {
    Decimal::new(1, 1)
}

fn default_bonereaper_state_v2_min_spot_move_5s_bps() -> Decimal {
    Decimal::new(5, 2)
}

fn default_bonereaper_state_v2_min_aligned_flow_bps() -> Decimal {
    Decimal::ZERO
}

fn default_bonereaper_state_v2_max_entry_price() -> Decimal {
    Decimal::new(88, 2)
}

fn default_bonereaper_state_v2_max_fair_price() -> Decimal {
    Decimal::new(99, 2)
}

fn default_bonereaper_state_v2_probe_notional_usdc() -> Decimal {
    Decimal::new(8, 0)
}

fn default_bonereaper_state_v2_normal_notional_usdc() -> Decimal {
    Decimal::new(15, 0)
}

fn default_bonereaper_state_v2_strong_notional_usdc() -> Decimal {
    Decimal::new(25, 0)
}

fn default_bonereaper_state_v2_strong_gap_bps() -> Decimal {
    Decimal::new(45, 1)
}

fn default_bonereaper_state_v2_strong_flow_bps() -> Decimal {
    Decimal::new(6, 1)
}

fn default_bonereaper_state_v2_min_expected_profit_usdc() -> Decimal {
    Decimal::ZERO
}

const fn default_bonereaper_state_v2_micro_alignment_guard_enabled() -> bool {
    false
}

fn default_bonereaper_state_v2_max_counter_1s_bps() -> Decimal {
    Decimal::ZERO
}

fn default_bonereaper_state_v2_max_counter_5s_bps() -> Decimal {
    Decimal::ZERO
}

const fn default_bonereaper_state_v2_early_window_guard_enabled() -> bool {
    false
}

const fn default_bonereaper_state_v2_early_window_max_seconds_left() -> i64 {
    240
}

fn default_bonereaper_state_v2_early_window_min_fresh_bps() -> Decimal {
    Decimal::new(75, 2)
}

fn default_bonereaper_state_v2_early_window_min_swing_bps() -> Decimal {
    Decimal::new(75, 2)
}

fn default_bonereaper_state_v2_early_window_min_signal_bps() -> Decimal {
    Decimal::from(800_u32)
}

const fn default_bonereaper_state_v2_high_gap_guard_enabled() -> bool {
    false
}

fn default_bonereaper_state_v2_high_gap_min_target_gap_bps() -> Decimal {
    Decimal::new(300, 2)
}

fn default_bonereaper_state_v2_high_gap_max_entry_price() -> Decimal {
    Decimal::new(56, 2)
}

fn default_bonereaper_state_v2_high_gap_min_fresh_bps() -> Decimal {
    Decimal::new(125, 2)
}

fn default_bonereaper_state_v2_high_gap_min_swing_bps() -> Decimal {
    Decimal::new(125, 2)
}

fn default_bonereaper_state_v2_high_gap_min_signal_bps() -> Decimal {
    Decimal::from(1_000_u32)
}

const fn default_bonereaper_state_v2_mid_gap_guard_enabled() -> bool {
    false
}

fn default_bonereaper_state_v2_mid_gap_min_target_gap_bps() -> Decimal {
    Decimal::new(150, 2)
}

fn default_bonereaper_state_v2_mid_gap_max_target_gap_bps() -> Decimal {
    Decimal::new(300, 2)
}

fn default_bonereaper_state_v2_mid_gap_max_entry_price() -> Decimal {
    Decimal::new(50, 2)
}

const fn default_bonereaper_state_v2_mid_gap_min_seconds_left() -> i64 {
    120
}

fn default_bonereaper_state_v2_mid_gap_min_fresh_bps() -> Decimal {
    Decimal::new(125, 2)
}

fn default_bonereaper_state_v2_mid_gap_min_signal_bps() -> Decimal {
    Decimal::from(800_u32)
}

fn default_bonereaper_state_v2_mid_gap_min_flow_bps() -> Decimal {
    Decimal::from(1_500_u32)
}

const fn default_bonereaper_state_v2_low_gap_guard_enabled() -> bool {
    false
}

fn default_bonereaper_state_v2_low_gap_max_target_gap_bps() -> Decimal {
    Decimal::new(150, 2)
}

fn default_bonereaper_state_v2_low_gap_max_entry_price() -> Decimal {
    Decimal::new(45, 2)
}

const fn default_bonereaper_state_v2_low_gap_min_seconds_left() -> i64 {
    120
}

fn default_bonereaper_state_v2_low_gap_allow_min_fresh_bps() -> Decimal {
    Decimal::new(150, 2)
}

fn default_bonereaper_state_v2_low_gap_allow_min_signal_bps() -> Decimal {
    Decimal::from(2_000_u32)
}

fn default_bonereaper_state_v2_low_gap_allow_min_flow_bps() -> Decimal {
    Decimal::from(3_000_u32)
}

const fn default_bonereaper_state_v2_early_expensive_guard_enabled() -> bool {
    false
}

const fn default_bonereaper_state_v2_early_expensive_min_seconds_left() -> i64 {
    240
}

fn default_bonereaper_state_v2_early_expensive_entry_price() -> Decimal {
    Decimal::new(56, 2)
}

fn default_bonereaper_state_v2_early_expensive_allow_min_target_gap_bps() -> Decimal {
    Decimal::new(300, 2)
}

fn default_bonereaper_state_v2_early_expensive_allow_min_fresh_bps() -> Decimal {
    Decimal::new(200, 2)
}

fn default_bonereaper_state_v2_early_expensive_allow_min_signal_bps() -> Decimal {
    Decimal::from(2_500_u32)
}

fn default_bonereaper_state_v2_early_expensive_allow_min_flow_bps() -> Decimal {
    Decimal::from(4_000_u32)
}

const fn default_directional_execution_slippage_bps() -> u32 {
    20
}

const fn default_reactive_run() -> bool {
    false
}

const fn default_restore_paper_state_on_start() -> bool {
    true
}

const fn default_reactive_debounce_ms() -> u64 {
    750
}

const fn default_reactive_idle_secs() -> u64 {
    15
}

const fn default_allow_repeat_entries_same_window() -> bool {
    false
}

const fn default_repeat_entry_min_interval_ms() -> u64 {
    0
}

const fn default_revalidate_before_execute() -> bool {
    true
}

const fn default_polymarket_stream_enabled() -> bool {
    false
}

const fn default_polymarket_stream_book_staleness_ms() -> i64 {
    8_000
}

const fn default_polymarket_stream_rest_fallback_enabled() -> bool {
    true
}

const fn default_polymarket_stream_backfill_trade_flow() -> bool {
    true
}

const fn default_chainlink_oracle_enabled() -> bool {
    false
}

const fn default_chainlink_oracle_max_quote_age_ms() -> i64 {
    2_500
}

const fn default_chainlink_oracle_max_window_open_lag_ms() -> i64 {
    3_000
}

const fn default_chainlink_oracle_max_settlement_close_lag_ms() -> i64 {
    5_000
}

const fn default_scale_in_enabled() -> bool {
    false
}

const fn default_scale_in_max_additional_entries_per_window() -> u32 {
    1
}

fn default_scale_in_min_price_improvement() -> Decimal {
    Decimal::new(2, 2)
}

const fn default_scale_in_require_stronger_binance_impulse() -> bool {
    true
}

fn default_scale_in_min_impulse_improvement_bps() -> Decimal {
    Decimal::new(5, 1)
}

const fn default_adaptive_regime_enabled() -> bool {
    true
}

const fn default_aggressive_min_spot_move_bps() -> u32 {
    25
}

fn default_aggressive_max_bundle_cost() -> Decimal {
    Decimal::new(102, 2)
}

fn default_safe_max_bundle_cost() -> Decimal {
    Decimal::new(995, 3)
}

const fn default_safe_bundle_only() -> bool {
    true
}

const fn default_safe_max_entries_per_cycle() -> usize {
    1
}

const fn default_pnl_ratchet_enabled() -> bool {
    false
}

const fn default_pnl_ratchet_apply_to_codex_sentinel_only() -> bool {
    true
}

fn default_pnl_ratchet_base_notional_usdc() -> Decimal {
    Decimal::new(6, 0)
}

fn default_pnl_ratchet_protect_notional_usdc() -> Decimal {
    Decimal::new(4, 0)
}

fn default_pnl_ratchet_profit_unlock_usdc() -> Decimal {
    Decimal::new(2, 0)
}

const fn default_pnl_ratchet_protect_after_consecutive_losses() -> u32 {
    1
}

fn default_max_daily_loss_usdc() -> Decimal {
    Decimal::new(30, 0)
}

fn default_max_session_loss_usdc() -> Decimal {
    Decimal::new(50, 0)
}

fn default_max_open_notional_usdc() -> Decimal {
    Decimal::ZERO
}

fn default_max_unrealized_loss_usdc() -> Decimal {
    Decimal::ZERO
}

const fn default_max_consecutive_losses() -> u32 {
    3
}

const fn default_risk_cooldown_cycles() -> usize {
    3
}

const fn default_risk_apply_in_live_mode() -> bool {
    false
}

const fn default_risk_seed_from_history() -> bool {
    true
}

const fn default_risk_reset_daily_on_start() -> bool {
    false
}

const fn default_v4_inventory_enabled() -> bool {
    false
}

fn default_v4_inventory_max_gross_inventory_shares_per_window() -> Decimal {
    Decimal::new(5000, 0)
}

fn default_v4_inventory_max_directional_delta_shares_per_window() -> Decimal {
    Decimal::new(3000, 0)
}

fn default_v4_inventory_max_window_spent_usdc() -> Decimal {
    Decimal::ZERO
}

const fn default_v4_inventory_max_entries_per_window() -> u32 {
    0
}

const fn default_v4_inventory_cooldown_secs() -> i64 {
    180
}

const fn default_v4_inventory_cooldown_on_stop_loss() -> bool {
    true
}

const fn default_v4_inventory_cooldown_on_reversal() -> bool {
    true
}

const fn default_v4_inventory_cooldown_on_partial_reversal() -> bool {
    true
}

const fn default_early_exit_enabled() -> bool {
    true
}

const fn default_early_exit_min_hold_secs() -> i64 {
    3
}

fn default_early_exit_min_take_profit_usdc() -> Decimal {
    Decimal::new(75, 2)
}

fn default_early_exit_min_expected_profit_capture_ratio() -> Decimal {
    Decimal::new(55, 2)
}

fn default_early_exit_max_loss_usdc() -> Decimal {
    Decimal::new(150, 2)
}

const fn default_early_exit_profit_lock_partial_close_enabled() -> bool {
    false
}

fn default_early_exit_profit_lock_partial_close_ratio() -> Decimal {
    Decimal::new(65, 2)
}

fn default_early_exit_profit_lock_min_profit_usdc() -> Decimal {
    Decimal::new(75, 2)
}

fn default_early_exit_reversal_min_5s_bps() -> Decimal {
    Decimal::new(2, 0)
}

fn default_early_exit_bonereaper_state_v2_stop_loss_min_15s_bps() -> Decimal {
    Decimal::ZERO
}

fn default_early_exit_bonereaper_state_v2_reversal_min_15s_bps() -> Decimal {
    Decimal::ZERO
}

const fn default_early_exit_directional_partial_reversal_enabled() -> bool {
    true
}

fn default_early_exit_directional_partial_close_ratio() -> Decimal {
    Decimal::new(70, 2)
}

fn default_early_exit_directional_partial_reversal_5s_bps() -> Decimal {
    Decimal::new(25, 1)
}

fn default_early_exit_directional_partial_reversal_15s_bps() -> Decimal {
    Decimal::new(10, 1)
}

const fn default_early_exit_micro_breakout_partial_reversal_enabled() -> bool {
    true
}

fn default_early_exit_micro_breakout_partial_close_ratio() -> Decimal {
    Decimal::new(70, 2)
}

fn default_early_exit_micro_breakout_partial_reversal_5s_bps() -> Decimal {
    Decimal::new(20, 1)
}

fn default_early_exit_micro_breakout_partial_reversal_15s_bps() -> Decimal {
    Decimal::new(10, 1)
}

fn default_early_exit_micro_breakout_fail_fast_1s_bps() -> Decimal {
    Decimal::ZERO
}

fn default_early_exit_micro_breakout_fail_fast_15s_bps() -> Decimal {
    Decimal::ZERO
}

fn default_early_exit_micro_breakout_fail_fast_profit_buffer_usdc() -> Decimal {
    Decimal::ZERO
}

const fn default_early_exit_peak_exit_enabled() -> bool {
    false
}

const fn default_early_exit_peak_exit_partial_close_enabled() -> bool {
    false
}

fn default_early_exit_peak_exit_partial_close_ratio() -> Decimal {
    Decimal::new(65, 2)
}

fn default_early_exit_peak_exit_min_profit_usdc() -> Decimal {
    Decimal::new(1, 0)
}

fn default_early_exit_peak_exit_min_primary_ask_price() -> Decimal {
    Decimal::new(72, 2)
}

fn default_early_exit_peak_exit_max_aligned_1s_bps() -> Decimal {
    Decimal::new(3, 1)
}

fn default_early_exit_peak_exit_max_aligned_5s_bps() -> Decimal {
    Decimal::new(8, 1)
}

fn default_early_exit_peak_exit_max_acceleration_bps() -> Decimal {
    Decimal::new(1, 1)
}

const fn default_early_exit_exhaustion_exit_enabled() -> bool {
    false
}

fn default_early_exit_exhaustion_exit_min_profit_usdc() -> Decimal {
    Decimal::new(5, 1)
}

fn default_early_exit_exhaustion_exit_max_aligned_1s_bps() -> Decimal {
    Decimal::ZERO
}

fn default_early_exit_exhaustion_exit_max_aligned_5s_bps() -> Decimal {
    Decimal::new(4, 1)
}

fn default_early_exit_exhaustion_exit_max_aligned_15s_bps() -> Decimal {
    Decimal::new(8, 1)
}

fn default_early_exit_exhaustion_exit_max_acceleration_bps() -> Decimal {
    Decimal::ZERO
}

const fn default_early_exit_stop_and_reverse_enabled() -> bool {
    false
}

const fn default_early_exit_stop_and_reverse_on_stop_loss() -> bool {
    false
}

fn default_early_exit_stop_and_reverse_size_ratio() -> Decimal {
    Decimal::new(50, 2)
}

const fn default_early_exit_stop_and_reverse_min_seconds_left() -> i64 {
    75
}

const fn default_early_exit_scalp_exit_enabled() -> bool {
    false
}

const fn default_early_exit_scalp_exit_apply_to_codex_sentinel_only() -> bool {
    true
}

fn default_early_exit_scalp_take_profit_price_delta() -> Decimal {
    Decimal::new(8, 2)
}

fn default_early_exit_scalp_stop_loss_price_delta() -> Decimal {
    Decimal::new(5, 2)
}

const fn default_early_exit_scalp_time_stop_secs() -> i64 {
    45
}

const fn default_early_exit_scalp_invalidation_exit_enabled() -> bool {
    false
}

fn default_early_exit_scalp_invalidation_min_loss_usdc() -> Decimal {
    Decimal::new(1, 0)
}

fn default_early_exit_scalp_invalidation_opposite_gap_bps() -> Decimal {
    Decimal::new(1, 0)
}

fn default_early_exit_scalp_invalidation_opposite_5s_bps() -> Decimal {
    Decimal::new(5, 0)
}

const fn default_early_exit_near_expiry_secs() -> i64 {
    20
}

const fn default_tail_hedge_min_spot_move_bps() -> u32 {
    10
}

const fn default_tail_hedge_min_signal_bps() -> u32 {
    14
}

const fn default_tail_hedge_min_velocity_bps_per_minute() -> u32 {
    6
}

const fn default_prompt_for_polybacktest_api_key() -> bool {
    true
}

const fn default_polybacktest_snapshot_page_limit() -> usize {
    250
}

const fn default_polybacktest_include_orderbook() -> bool {
    true
}

const fn default_polybacktest_cache_enabled() -> bool {
    true
}

fn default_polybacktest_cache_dir() -> PathBuf {
    PathBuf::from("state/polybacktest-cache")
}

const fn default_live_signature_type() -> LiveSignatureType {
    LiveSignatureType::Proxy
}

fn default_state_dir() -> PathBuf {
    PathBuf::from("state")
}

fn default_execution_journal_filename() -> String {
    "trade_journal.jsonl".to_owned()
}

fn default_pnl_snapshot_filename() -> String {
    "pnl_snapshot.json".to_owned()
}

fn default_paper_cycle_journal_filename() -> String {
    "paper_cycle_journal.jsonl".to_owned()
}

const fn default_paper_cycle_journal_sample_secs() -> u64 {
    1
}

const fn default_paper_cycle_journal_max_bytes() -> u64 {
    16 * 1024 * 1024
}

const fn default_paper_cycle_journal_max_rotated_files() -> usize {
    1
}

fn default_paper_trade_journal_filename() -> String {
    "paper_trade_journal.jsonl".to_owned()
}

#[cfg(test)]
mod tests {
    use crate::models::MarketTarget;

    use super::AppConfig;
    use rust_decimal::Decimal;

    #[test]
    fn codex_sentinel_config_validates_without_legacy_v2() {
        let raw = include_str!("../config.codex-sentinel.toml");
        let config: AppConfig = toml::from_str(raw).expect("fixture config should parse");

        config
            .validate()
            .expect("codex sentinel config should keep legacy v2 disabled");
    }

    #[test]
    fn codex_scalp_config_validates_runtime_scalp_exit() {
        let raw = include_str!("../config.codex-scalp-v1.toml");
        let config: AppConfig = toml::from_str(raw).expect("fixture config should parse");

        assert!(
            config.run.early_exit.scalp_exit_enabled,
            "scalp profile should enable runtime scalp exits"
        );
        assert_eq!(
            config.run.early_exit.scalp_take_profit_price_delta,
            Decimal::new(8, 2)
        );
        assert_eq!(
            config.run.early_exit.scalp_stop_loss_price_delta,
            Decimal::new(4, 2)
        );
        assert_eq!(config.run.early_exit.min_hold_secs, 3);
        assert_eq!(config.run.early_exit.scalp_time_stop_secs, 35);
        assert!(
            config
                .strategy
                .codex_sentinel_v1_premium_entry_guard_enabled,
            "scalp profile should require fresh confirmation before chasing premium entries"
        );
        assert!(
            config.strategy.codex_sentinel_v1_entry_spread_guard_enabled,
            "scalp profile should reject entries with too much immediate exit spread"
        );
        assert!(
            config
                .strategy
                .codex_sentinel_v1_live_quote_age_guard_enabled,
            "scalp profile should reject stale live quote entries after stale high-gap losses"
        );
        assert_eq!(config.strategy.codex_sentinel_v1_max_live_quote_age_ms, 750);
        assert_eq!(
            config.strategy.codex_sentinel_v1_max_entry_spread,
            Decimal::new(5, 2)
        );
        assert!(
            config.strategy.codex_sentinel_v1_attack_size_enabled,
            "scalp profile should size up only confirmed discount attacks"
        );
        assert!(
            config.strategy.codex_sentinel_v1_confidence_sizing_enabled,
            "scalp profile should scale high-confidence attacks without changing the base strategy"
        );
        assert!(
            config.strategy.codex_sentinel_v1_low_flow_guard_enabled,
            "scalp profile should reject weak momentum when Polymarket flow is absent"
        );
        assert_eq!(
            config
                .strategy
                .codex_sentinel_v1_low_flow_allow_min_swing_bps,
            Decimal::new(300, 2)
        );
        assert_eq!(
            config.strategy.codex_sentinel_v1_attack_max_entry_price,
            Decimal::new(56, 2),
            "scalp profile should size up only value entries after expensive bucket losses"
        );
        assert_eq!(
            config.strategy.codex_sentinel_v1_premium_entry_price,
            Decimal::new(56, 2),
            "scalp profile should treat >0.56 as a premium chase"
        );
        assert_eq!(
            config.strategy.codex_sentinel_v1_premium_min_signal_bps,
            Decimal::from(1600)
        );
        assert_eq!(
            config.strategy.codex_sentinel_v1_premium_min_flow_bps,
            Decimal::from(2500)
        );
        assert!(
            config
                .strategy
                .codex_sentinel_v1_aggressive_continuation_enabled,
            "scalp profile should reopen premium entries only for fresh high-gap continuations"
        );
        assert_eq!(
            config
                .strategy
                .codex_sentinel_v1_aggressive_continuation_max_entry_price,
            Decimal::new(62, 2)
        );
        assert_eq!(
            config
                .strategy
                .codex_sentinel_v1_aggressive_continuation_min_target_gap_bps,
            Decimal::new(600, 2)
        );
        assert_eq!(
            config
                .strategy
                .codex_sentinel_v1_aggressive_continuation_min_flow_bps,
            Decimal::from(2200)
        );
        assert!(
            config.strategy.codex_breakout_v1_enabled,
            "scalp profile should enable orderbook-pressure breakout anticipation"
        );
        assert_eq!(
            config.strategy.market_targets,
            vec![
                MarketTarget::Btc5m,
                MarketTarget::Eth5m,
                MarketTarget::Sol5m,
                MarketTarget::Xrp5m,
                MarketTarget::Bnb5m,
            ],
            "active scalp profile should scan all supported 5m crypto windows with tiny radar probes"
        );
        assert!(
            config.strategy.codex_breakout_v1_required,
            "scalp profile should require orderbook-pressure confirmation before entries"
        );
        assert_eq!(
            config.strategy.codex_breakout_v1_max_entry_price,
            Decimal::new(58, 2)
        );
        assert_eq!(
            config.strategy.codex_breakout_v1_min_depth_imbalance_bps,
            Decimal::from(1800)
        );
        assert_eq!(
            config.strategy.codex_breakout_v1_min_microprice_bps,
            Decimal::new(3, 4),
            "BTC L2 microprice is measured in true bps, so the threshold must be sub-bp"
        );
        assert_eq!(
            config.strategy.codex_breakout_v1_min_signal_bps,
            Decimal::from(650),
            "strict breakout entries should reject weak Sentinel signal strength"
        );
        assert_eq!(
            config.strategy.codex_breakout_v1_min_flow_bps,
            Decimal::from(700),
            "strict breakout entries should reject weak trade-flow confirmation"
        );
        assert_eq!(config.strategy.codex_breakout_v1_max_book_age_ms, 750);
        assert!(
            config
                .strategy
                .codex_sentinel_v1_discount_value_lane_enabled,
            "scalp profile should allow only cheap, strongly confirmed value entries to bypass strict breakout"
        );
        assert_eq!(
            config
                .strategy
                .codex_sentinel_v1_discount_value_max_entry_price,
            Decimal::new(50, 2)
        );
        assert_eq!(
            config
                .strategy
                .codex_sentinel_v1_discount_value_min_fresh_bps,
            Decimal::new(125, 2)
        );
        assert_eq!(
            config
                .strategy
                .codex_sentinel_v1_discount_value_min_signal_bps,
            Decimal::from(650)
        );
        assert_eq!(
            config
                .strategy
                .codex_sentinel_v1_discount_value_min_depth_imbalance_bps,
            Decimal::from(700)
        );
        assert_eq!(
            config
                .strategy
                .codex_sentinel_v1_discount_value_min_microprice_bps,
            Decimal::new(3, 4)
        );
        assert!(
            config.strategy.enable_codex_scalp_probe_v1,
            "active scalp profile should enable tiny radar probes to test orderbook breakout anticipation"
        );
        assert!(
            !config.strategy.codex_scalp_probe_v1_raw_ablation_enabled,
            "production scalp profile must keep raw ablation disabled"
        );
        assert_eq!(
            config.strategy.codex_scalp_probe_v1_min_entry_price,
            Decimal::new(45, 2)
        );
        assert_eq!(
            config.strategy.codex_scalp_probe_v1_max_entry_price,
            Decimal::new(54, 2)
        );
        assert_eq!(
            config.strategy.codex_scalp_probe_v1_max_book_age_ms, 250,
            "probe entries should use fresher exchange orderbook pressure when accepting earlier breakouts"
        );
        assert_eq!(
            config.strategy.codex_scalp_probe_v1_min_target_gap_bps,
            Decimal::new(60, 2),
            "probe entries should be allowed to anticipate low-gap breakouts only with stronger book pressure"
        );
        assert_eq!(
            config.strategy.codex_scalp_probe_v1_min_fresh_bps,
            Decimal::new(15, 2)
        );
        assert_eq!(
            config.strategy.codex_scalp_probe_v1_min_top_imbalance_bps,
            Decimal::from(1000)
        );
        assert_eq!(
            config.strategy.codex_scalp_probe_v1_min_radar_score_bps,
            Decimal::from(1650),
            "probe entries should require a combined orderbook-density radar score"
        );
        assert_eq!(
            config.strategy.codex_scalp_probe_v1_notional_usdc,
            Decimal::new(2, 0),
            "probe entries should stay tiny while we validate low-gap breakout anticipation"
        );
        assert!(
            !config.strategy.codex_scalp_probe_v1_bnb_pressure_enabled,
            "BNB pressure scout should remain opt-in because the last run produced no trades"
        );
        assert_eq!(
            config
                .strategy
                .codex_scalp_probe_v1_bnb_pressure_max_entry_price,
            Decimal::new(58, 2),
            "BNB pressure scout can chase slightly higher only when book pressure is strong"
        );
        assert_eq!(
            config
                .strategy
                .codex_scalp_probe_v1_bnb_pressure_max_book_age_ms,
            400
        );
        assert_eq!(
            config
                .strategy
                .codex_scalp_probe_v1_bnb_pressure_min_top_imbalance_bps,
            Decimal::from(1500)
        );
        assert_eq!(
            config
                .strategy
                .codex_scalp_probe_v1_bnb_pressure_min_expected_profit_usdc,
            Decimal::new(5, 2)
        );
        assert_eq!(
            config
                .strategy
                .codex_sentinel_v1_stale_micro_max_non_discount_entry_price,
            Decimal::new(58, 2)
        );
        assert_eq!(
            config.strategy.codex_sentinel_v1_expensive_entry_price,
            Decimal::new(56, 2),
            "scalp profile should require monster confirmation for entries above the profitable value bucket"
        );
        assert!(
            config
                .strategy
                .codex_sentinel_v1_mid_gap_premium_guard_enabled,
            "scalp profile should protect the weak mid-gap premium bucket"
        );
        assert_eq!(
            config
                .strategy
                .codex_sentinel_v1_mid_gap_premium_entry_price,
            Decimal::new(56, 2)
        );

        config
            .validate()
            .expect("codex scalp config should validate runtime scalp exit controls");
    }

    #[test]
    fn codex_scalp_raw_config_is_paper_only_ablation() {
        let raw = include_str!("../config.codex-scalp-v1-raw.toml");
        let config: AppConfig = toml::from_str(raw).expect("fixture config should parse");

        assert_eq!(config.run.mode, super::BotMode::Paper);
        assert!(config.strategy.enable_codex_scalp_probe_v1);
        assert!(config.strategy.codex_scalp_probe_v1_raw_ablation_enabled);
        assert!(!config.strategy.codex_scalp_probe_v1_raw_light_enabled);
        assert!(!config.strategy.enable_codex_sentinel_v1);
        assert!(!config.run.early_exit.enabled);
        assert!(!config.run.v4_inventory.enabled);
        assert_eq!(config.run.risk.max_open_notional_usdc, Decimal::ZERO);
        assert_eq!(
            config.storage.state_dir.to_string_lossy().as_ref(),
            "state/codex-scalp-v1-raw"
        );

        config
            .validate()
            .expect("raw scalp ablation config should validate in paper mode");
    }

    #[test]
    fn codex_scalp_raw_light_config_validates_market_specific_probe() {
        let raw = include_str!("../config.codex-scalp-v1-raw-light-v2.toml");
        let config: AppConfig = toml::from_str(raw).expect("fixture config should parse");

        assert_eq!(config.run.mode, super::BotMode::Paper);
        assert!(config.strategy.enable_codex_scalp_probe_v1);
        assert!(config.strategy.codex_scalp_probe_v1_raw_ablation_enabled);
        assert!(config.strategy.codex_scalp_probe_v1_raw_light_enabled);
        assert!(!config.run.allow_repeat_entries_same_window);
        assert!(config.run.v4_inventory.enabled);
        assert!(config.run.polymarket_stream.rest_fallback_enabled);
        assert_eq!(config.run.v4_inventory.max_entries_per_window, 1);
        assert_eq!(
            config.storage.state_dir.to_string_lossy().as_ref(),
            "state/codex-scalp-v1-raw-light-v2"
        );

        config
            .validate()
            .expect("raw-light scalp config should validate in paper mode");
    }

    #[test]
    fn codex_scalp_raw_light_v3_config_targets_multi_asset_scalp_bucket() {
        let raw = include_str!("../config.codex-scalp-v1-raw-light-v3.toml");
        let config: AppConfig = toml::from_str(raw).expect("fixture config should parse");

        assert_eq!(config.run.mode, super::BotMode::Paper);
        assert_eq!(
            config.strategy.market_targets,
            vec![
                MarketTarget::Btc5m,
                MarketTarget::Eth5m,
                MarketTarget::Sol5m,
                MarketTarget::Xrp5m
            ]
        );
        assert!(config.strategy.codex_scalp_probe_v1_raw_ablation_enabled);
        assert!(config.strategy.codex_scalp_probe_v1_raw_light_enabled);
        assert_eq!(
            config.strategy.codex_scalp_probe_v1_min_entry_price,
            Decimal::new(45, 2)
        );
        assert_eq!(
            config.strategy.codex_scalp_probe_v1_max_entry_price,
            Decimal::new(68, 2)
        );
        assert_eq!(config.strategy.codex_scalp_probe_v1_min_seconds_left, 60);
        assert_eq!(config.strategy.codex_scalp_probe_v1_max_seconds_left, 295);
        assert_eq!(
            config.strategy.codex_scalp_probe_v1_notional_usdc,
            Decimal::new(30, 0)
        );
        assert_eq!(config.run.reactive_debounce_ms, 0);
        assert!(config.run.allow_repeat_entries_same_window);
        assert_eq!(config.run.repeat_entry_min_interval_ms, 500);
        assert!(!config.run.revalidate_before_execute);
        assert_eq!(config.run.execute_top_n, 2);
        assert!(config.run.early_exit.enabled);
        assert!(config.run.early_exit.scalp_exit_enabled);
        assert_eq!(
            config.run.early_exit.scalp_take_profit_price_delta,
            Decimal::new(7, 2)
        );
        assert_eq!(
            config.run.early_exit.scalp_stop_loss_price_delta,
            Decimal::ZERO
        );
        assert_eq!(config.run.early_exit.scalp_time_stop_secs, 20);
        assert!(!config.run.early_exit.scalp_invalidation_exit_enabled);
        assert_eq!(
            config.run.early_exit.scalp_invalidation_min_loss_usdc,
            Decimal::new(45, 2)
        );
        assert_eq!(
            config.run.early_exit.scalp_invalidation_opposite_gap_bps,
            Decimal::new(50, 2)
        );
        assert_eq!(
            config.run.early_exit.scalp_invalidation_opposite_5s_bps,
            Decimal::new(3, 0)
        );
        assert_eq!(config.run.early_exit.near_expiry_secs, 15);
        assert!(!config.run.early_exit.directional_partial_reversal_enabled);
        assert!(!config.run.early_exit.stop_and_reverse_enabled);
        assert_eq!(config.run.polymarket_stream.book_staleness_ms, 1_200);
        assert!(!config.run.polymarket_stream.rest_fallback_enabled);
        assert!(!config.run.v4_inventory.enabled);
        assert_eq!(
            config.storage.state_dir.to_string_lossy().as_ref(),
            "state/codex-scalp-v1-raw-light-v3"
        );

        config
            .validate()
            .expect("raw-light v3 config should validate in paper mode");
    }

    #[test]
    fn codex_scalp_raw_light_requires_raw_ablation() {
        let raw = include_str!("../config.codex-scalp-v1-raw-light-v2.toml").replace(
            "codex_scalp_probe_v1_raw_ablation_enabled = true",
            "codex_scalp_probe_v1_raw_ablation_enabled = false",
        );
        let config: AppConfig = toml::from_str(&raw).expect("fixture config should parse");
        let err = config
            .validate()
            .expect_err("raw-light mode must be an ablation sub-mode");

        assert!(
            err.to_string()
                .contains("codex_scalp_probe_v1_raw_light_enabled requires raw ablation mode")
        );
    }

    #[test]
    fn codex_scalp_raw_ablation_is_rejected_for_live_mode() {
        let raw = include_str!("../config.codex-scalp-v1-raw.toml")
            .replace("mode = \"paper\"", "mode = \"live\"");
        let config: AppConfig = toml::from_str(&raw).expect("fixture config should parse");
        let err = config
            .validate()
            .expect_err("raw ablation must not be allowed for live mode");

        assert!(
            err.to_string()
                .contains("codex_scalp_probe_v1_raw_ablation_enabled is paper-only")
        );
    }

    #[test]
    fn codex_scalp_boost_config_validates_pnl_sizing_controls() {
        let raw = include_str!("../config.codex-scalp-v1-boost.toml");
        let config: AppConfig = toml::from_str(raw).expect("fixture config should parse");

        assert_eq!(
            config.strategy.codex_sentinel_v1_attack_notional_usdc,
            Decimal::new(12, 0)
        );
        assert_eq!(
            config.strategy.codex_sentinel_v1_confidence_max_multiplier,
            Decimal::new(135, 2)
        );
        assert_eq!(
            config.strategy.bonereaper_state_v2_normal_notional_usdc,
            Decimal::new(4, 0)
        );
        assert!(
            config
                .strategy
                .codex_sentinel_v1_mid_gap_premium_guard_enabled,
            "boost profile should keep quality filters while increasing only selected notional"
        );
        assert_eq!(
            config
                .strategy
                .codex_sentinel_v1_mid_gap_premium_min_signal_bps,
            Decimal::from(800)
        );
        assert_eq!(
            config.run.paper_starting_balance_usdc,
            Some(Decimal::new(100, 0))
        );
        assert_eq!(
            config.run.v4_inventory.max_window_spent_usdc,
            Decimal::new(18, 0)
        );
        assert!(
            config
                .storage
                .state_dir
                .to_string_lossy()
                .contains("codex-scalp-v1-boost"),
            "boost profile should write to its own state directory"
        );

        config
            .validate()
            .expect("codex scalp boost config should validate isolated pnl sizing controls");
    }

    #[test]
    fn codex_v4_champion_config_limits_repeat_window_risk() {
        let raw = include_str!("../config.codex-v4-champion.toml");
        let config: AppConfig = toml::from_str(raw).expect("champion config should parse");

        assert!(config.run.allow_repeat_entries_same_window);
        assert_eq!(config.run.v4_inventory.max_entries_per_window, 2);
        assert_eq!(
            config.run.v4_inventory.max_window_spent_usdc,
            Decimal::from(20)
        );
        assert!(config.run.risk.max_session_loss_usdc > Decimal::ZERO);

        config
            .validate()
            .expect("champion config should validate risk caps");
    }

    #[test]
    fn config_rejects_codex_sentinel_with_legacy_v2_enabled() {
        let raw = include_str!("../config.codex-sentinel.toml").replace(
            "enable_bonereaper_state_v2 = false",
            "enable_bonereaper_state_v2 = true",
        );
        let config: AppConfig = toml::from_str(&raw).expect("fixture config should parse");
        let error = config
            .validate()
            .expect_err("codex sentinel and legacy v2 must be mutually exclusive");

        assert!(
            error
                .to_string()
                .contains("enable_bonereaper_state_v2 and strategy.enable_codex_sentinel_v1")
        );
    }

    #[test]
    fn config_rejects_live_risk_mode_until_live_reconciliation_exists() {
        let raw = include_str!("../config.codex-scalp-v1.toml")
            .replace("apply_in_live_mode = false", "apply_in_live_mode = true");
        let config: AppConfig = toml::from_str(&raw).expect("fixture config should parse");
        let error = config
            .validate()
            .expect_err("live risk mode must fail closed until it has live state");

        assert!(
            error
                .to_string()
                .contains("apply_in_live_mode is disabled until live position reconciliation")
        );
    }
}
