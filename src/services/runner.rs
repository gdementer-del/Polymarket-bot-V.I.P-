//! CLI orchestration and runtime loop.

mod cycle_summary;

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Write as _;
use std::fs::{self, OpenOptions};
use std::io::{self, IsTerminal, Write};
use std::path::Path;
use std::time::{Duration as StdDuration, Instant};

use chrono::{DateTime, Duration as ChronoDuration, Local, NaiveDateTime, TimeZone, Utc};
use crossterm::cursor::MoveTo;
use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use crossterm::execute;
use crossterm::terminal::{Clear, ClearType, disable_raw_mode, enable_raw_mode};
use futures_util::future::join_all;
use rust_decimal::Decimal;
use tokio::sync::watch;
use tokio::time::{Duration, sleep, timeout};
use tracing::{debug, info, warn};

use crate::config::{
    AppConfig, BotMode, Cli, Command, EarlyExitConfig, PnlRatchetConfig, RiskControlConfig,
    RuntimeRegime, V4InventoryConfig,
};
use crate::error::{AppError, Result};
use crate::models::{
    BinaryMarket, BookFillLevel, ExecutionReport, MarketTarget, Opportunity, OpportunityKind,
    OrderBook, PaperOutcomeSide, PaperPosition, PaperPositionLeg, PaperState,
};

use self::cycle_summary::{
    RuntimeCurrentMarketSummary, RuntimeLatencyMetrics, RuntimeSnapshotSummary,
    build_paper_cycle_entry, build_runtime_snapshot_summary,
};
use super::analytics::AnalyticsReport;
use super::backtest::{BacktestReport, BacktestRunner, BacktestSignal};
use super::binance::{
    BinanceClient, BinanceTriggerEvent, BinanceTriggerSource, BtcFiveMinuteContext,
    LiveMarketDataHealth, MarketWindowResolution, WindowDirection,
};
use super::coinbase::CoinbaseClient;
use super::execution::{
    AuthCheckReport, LiveExecutor, MAX_MARK_TO_MARKET_BID_LEVELS, PaperCloseReport, PaperCostModel,
    PaperExecutor, TradeExecutor, mark_to_market_payout_for_legs,
};
use super::inventory::{
    InventoryReplaySimulationConfig, export_wallet_inventory_replay_dataset,
    export_wallet_inventory_replay_dataset_path_auto, show_wallet_inventory_replay_report,
    show_wallet_inventory_replay_report_path, show_wallet_inventory_replay_simulation,
    show_wallet_inventory_replay_simulation_path, show_wallet_inventory_replay_window_report,
    show_wallet_inventory_replay_window_report_path,
};
use super::journal::{
    JournalEntry, JournalStore, PaperCycleEntry, PaperJournalWriter, PaperReportMemory,
    PaperTradeAction, PaperTradeEntry, PnlSnapshot,
};
use super::labels::{
    outcome_label_is_down, outcome_label_is_up, wallet_side_is_buy_label, wallet_side_is_sell_label,
};
use super::market_data::{
    MarketDataClient, ProfileActivityRecord, TradeFlowSummary, TradeFlowWindow,
};
use super::polybacktest::PolyBacktestRunner;
use super::research::{
    show_replay_alert_calibration, show_replay_export_autotune, show_replay_export_comparison,
};
use super::strategy::{BundleArbitrageStrategy, NearMiss};
use super::text::sanitize_legacy_mojibake;
use super::wallet_activity::{
    DEFAULT_WALLET_ACTIVITY_RECORD_FILENAME, WalletActivitySnapshotRecord,
    load_wallet_activity_records,
};

const MARKET_DATA_WARMUP_TIMEOUT_MS: u64 = 12_000;
const MARKET_DATA_WARMUP_POLL_MS: u64 = 100;
const MARKET_DATA_WARMUP_MAX_QUOTE_AGE_MS: i64 = 2_000;
const MARKET_DATA_WARMUP_MAX_DEPTH_AGE_MS: i64 = 2_000;

#[derive(Debug, Clone)]
struct RuntimeTriggerEvent {
    symbol: String,
    event_time_ms: i64,
    received_time_ms: i64,
    price: Decimal,
    source: String,
}

#[derive(Debug, Clone, Copy)]
struct FollowWalletRecordOptions<'a> {
    wallet: &'a str,
    limit: usize,
    refresh_secs: u64,
    btc_only: bool,
    cycles: Option<usize>,
    output: Option<&'a Path>,
}

impl RuntimeTriggerEvent {
    fn from_binance(event: BinanceTriggerEvent) -> Self {
        let source = match event.source {
            BinanceTriggerSource::CoinbaseTicker => "Coinbase::Ticker".to_owned(),
            BinanceTriggerSource::CoinbaseLevel2 => "Coinbase::Level2".to_owned(),
            _ => format!("Binance::{:?}", event.source),
        };
        Self {
            symbol: event.symbol,
            event_time_ms: event.event_time_ms,
            received_time_ms: event.received_time_ms,
            price: event.price,
            source,
        }
    }

    fn from_polymarket(revision: u64) -> Self {
        let now_ms = Utc::now().timestamp_millis();
        Self {
            symbol: "POLYMARKET".to_owned(),
            event_time_ms: now_ms,
            received_time_ms: now_ms,
            price: Decimal::ZERO,
            source: format!("Polymarket::{revision}"),
        }
    }

    fn event_age_ms(&self) -> i64 {
        Utc::now()
            .timestamp_millis()
            .saturating_sub(self.event_time_ms)
    }

    fn received_age_ms(&self) -> i64 {
        Utc::now()
            .timestamp_millis()
            .saturating_sub(self.received_time_ms)
    }
}

/// Run the CLI command.
///
/// # Errors
///
/// Returns an error if config loading, market fetching, or execution fails.
#[allow(clippy::too_many_lines)]
pub async fn run_cli(cli: Cli) -> Result<()> {
    match &cli.command {
        Command::FollowWalletReport {
            input: Some(input),
            limit,
            top,
        } => return show_wallet_activity_report_path(input, *limit, *top),
        Command::FollowWalletReplayReport {
            input: Some(input),
            limit,
            top,
        } => return show_wallet_inventory_replay_report_path(input, *limit, *top),
        Command::FollowWalletReplayWindow {
            input: Some(input),
            limit,
            slug,
            events,
        } => {
            return show_wallet_inventory_replay_window_report_path(
                input,
                *limit,
                slug.as_deref(),
                *events,
            );
        }
        Command::FollowWalletReplayExport {
            input: Some(input),
            limit,
            output,
        } => {
            return export_wallet_inventory_replay_dataset_path_auto(
                input,
                *limit,
                output.as_deref(),
            );
        }
        Command::FollowWalletReplaySimulate {
            input: Some(input),
            max_gross_window_shares,
            max_directional_delta_shares,
            cooldown_secs,
            trigger_on_cooldown_alert,
            trigger_on_late_expansion,
        } => {
            return show_wallet_inventory_replay_simulation_path(
                input,
                InventoryReplaySimulationConfig {
                    max_gross_window_shares: Decimal::from(*max_gross_window_shares),
                    max_directional_delta_shares: Decimal::from(*max_directional_delta_shares),
                    cooldown_secs: *cooldown_secs,
                    trigger_on_cooldown_alert: *trigger_on_cooldown_alert,
                    trigger_on_late_expansion: *trigger_on_late_expansion,
                },
            );
        }
        Command::FollowWalletResearchCompare { inputs, top } => {
            return show_replay_export_comparison(inputs, *top);
        }
        Command::FollowWalletReplayAutotune {
            inputs,
            gross_values,
            delta_values,
            cooldown_values,
            top,
        } => {
            return show_replay_export_autotune(
                inputs,
                gross_values,
                delta_values,
                cooldown_values,
                *top,
            );
        }
        Command::FollowWalletAlertCalibrate { inputs } => {
            return show_replay_alert_calibration(inputs);
        }
        _ => {}
    }

    let config = AppConfig::load(&cli.config)?;
    let data_client = MarketDataClient::new(config.http.clone())?;
    let binance_client = BinanceClient::new(
        config.http.binance_base_url.clone(),
        config.http.binance_ws_base_url.clone(),
        config.http.timeout_secs,
    )?;
    let coinbase_client = config.http.coinbase_market_data_enabled.then(|| {
        CoinbaseClient::new(
            config.http.coinbase_ws_base_url.clone(),
            binance_client.clone(),
            config.http.coinbase_max_source_disagreement_bps,
            config.http.coinbase_max_spread_bps,
        )
    });
    let strategy = BundleArbitrageStrategy::new(config.strategy.clone());

    match cli.command {
        Command::Scan { top } => {
            scan_once(&config, &data_client, &binance_client, &strategy, top).await
        }
        Command::Markets {
            top,
            watch,
            refresh_secs,
            cycles,
        } => {
            if watch {
                watch_markets(
                    &config,
                    &data_client,
                    &binance_client,
                    &strategy,
                    top,
                    refresh_secs,
                    cycles,
                )
                .await
            } else {
                show_markets(&config, &data_client, &binance_client, &strategy, top).await
            }
        }
        Command::Dashboard {
            top,
            refresh_secs,
            cycles,
        } => {
            run_dashboard(
                &config,
                &data_client,
                &binance_client,
                &strategy,
                top,
                refresh_secs,
                cycles,
            )
            .await
        }
        Command::Backtest {
            windows_per_target,
            entry_minutes,
            top,
            target,
        } => {
            show_backtest(
                &config,
                &data_client,
                &binance_client,
                &strategy,
                windows_per_target,
                entry_minutes,
                top,
                target,
            )
            .await
        }
        Command::PolyBacktest {
            windows_per_target,
            entry_minutes,
            top,
            target,
        } => {
            show_polybacktest(
                &config,
                &binance_client,
                &strategy,
                windows_per_target,
                entry_minutes,
                top,
                target,
            )
            .await
        }
        Command::PolyBacktestTune {
            windows_per_target,
            entry_minutes,
            top,
            target,
            variants,
            max_variants,
        } => {
            show_polybacktest_tune(
                &config,
                &binance_client,
                PolyBacktestTuneOptions {
                    windows_per_target,
                    entry_minutes: &entry_minutes,
                    top,
                    target,
                    variant_filter: &variants,
                    max_variants,
                },
            )
            .await
        }
        Command::Analytics { limit } => show_analytics(&config, &binance_client, limit).await,
        Command::PaperReport { limit } => show_paper_report(&config, limit),
        Command::PaperTrades { limit, since } => {
            show_paper_trades(&config, limit, since.as_deref())
        }
        Command::PaperQuality { limit, since } => {
            show_paper_quality(&config, limit, since.as_deref())
        }
        Command::PaperRunSummary { since, limit, top } => {
            show_paper_run_summary(&config, since.as_deref(), limit, top)
        }
        Command::PaperPositions => show_paper_positions(&config),
        Command::AuthCheck => auth_check(&config, &data_client).await,
        Command::FollowWallet {
            wallet,
            limit,
            refresh_secs,
            btc_only,
            cycles,
        } => {
            follow_wallet_activity(&data_client, &wallet, limit, refresh_secs, btc_only, cycles)
                .await
        }
        Command::FollowWalletRecord {
            wallet,
            limit,
            refresh_secs,
            btc_only,
            cycles,
            output,
        } => {
            follow_wallet_activity_recorded(
                &config,
                &data_client,
                &binance_client,
                FollowWalletRecordOptions {
                    wallet: &wallet,
                    limit,
                    refresh_secs,
                    btc_only,
                    cycles,
                    output: output.as_deref(),
                },
            )
            .await
        }
        Command::FollowWalletReport { input, limit, top } => {
            show_wallet_activity_report(&config, input.as_deref(), limit, top)
        }
        Command::FollowWalletReplayReport { input, limit, top } => {
            show_wallet_inventory_replay_report(&config, input.as_deref(), limit, top)
        }
        Command::FollowWalletReplayWindow {
            input,
            limit,
            slug,
            events,
        } => show_wallet_inventory_replay_window_report(
            &config,
            input.as_deref(),
            limit,
            slug.as_deref(),
            events,
        ),
        Command::FollowWalletReplayExport {
            input,
            limit,
            output,
        } => export_wallet_inventory_replay_dataset(
            &config,
            input.as_deref(),
            limit,
            output.as_deref(),
        ),
        Command::FollowWalletReplaySimulate {
            input,
            max_gross_window_shares,
            max_directional_delta_shares,
            cooldown_secs,
            trigger_on_cooldown_alert,
            trigger_on_late_expansion,
        } => show_wallet_inventory_replay_simulation(
            &config,
            input.as_deref(),
            InventoryReplaySimulationConfig {
                max_gross_window_shares: Decimal::from(max_gross_window_shares),
                max_directional_delta_shares: Decimal::from(max_directional_delta_shares),
                cooldown_secs,
                trigger_on_cooldown_alert,
                trigger_on_late_expansion,
            },
        ),
        Command::FollowWalletResearchCompare { inputs, top } => {
            show_replay_export_comparison(&inputs, top)
        }
        Command::FollowWalletReplayAutotune {
            inputs,
            gross_values,
            delta_values,
            cooldown_values,
            top,
        } => show_replay_export_autotune(
            &inputs,
            &gross_values,
            &delta_values,
            &cooldown_values,
            top,
        ),
        Command::FollowWalletAlertCalibrate { inputs } => show_replay_alert_calibration(&inputs),
        Command::Run {
            mode,
            once,
            max_runtime_secs,
            drain_open_positions,
            max_drain_secs,
        } => {
            run_loop(
                &config,
                &data_client,
                &binance_client,
                coinbase_client.as_ref(),
                &strategy,
                RunLoopControl {
                    mode_override: mode,
                    once,
                    max_runtime_secs,
                    drain_open_positions,
                    max_drain_secs,
                },
            )
            .await
        }
    }
}

async fn scan_once(
    config: &AppConfig,
    data_client: &MarketDataClient,
    binance_client: &BinanceClient,
    strategy: &BundleArbitrageStrategy,
    top: usize,
) -> Result<()> {
    let analysis = collect_analysis_frame(
        config,
        data_client,
        binance_client,
        strategy,
        &HashMap::new(),
    )
    .await?;
    let AnalysisFrame {
        opportunities,
        near_misses,
        ..
    } = analysis;

    if opportunities.is_empty() {
        if !near_misses.is_empty() {
            info!("\n{}", render_near_miss_table(&near_misses, top.min(6)));
        }
        info!("no strategy opportunities found");
    } else {
        info!(
            total = opportunities.len(),
            shown = top.min(opportunities.len()),
            "strategy opportunities found"
        );
        info!("\n{}", render_opportunity_table_v2(&opportunities, top));
    }

    Ok(())
}

async fn show_markets(
    config: &AppConfig,
    data_client: &MarketDataClient,
    binance_client: &BinanceClient,
    strategy: &BundleArbitrageStrategy,
    top: usize,
) -> Result<()> {
    let analysis = collect_analysis_frame(
        config,
        data_client,
        binance_client,
        strategy,
        &HashMap::new(),
    )
    .await?;
    let views = analysis.views;

    if views.is_empty() {
        info!("no markets found");
        return Ok(());
    }

    let live_windows = views.iter().filter(|view| view.phase.is_live()).count();
    let upcoming_windows = views.iter().filter(|view| view.phase.is_upcoming()).count();
    let strategy_fit = views.iter().filter(|view| view.strategy_fit).count();

    info!(
        total_markets = views.len(),
        live_windows,
        upcoming_windows,
        strategy_fit,
        shown = top.min(views.len()),
        "analytics summary"
    );

    if let Some(current_window) = views.iter().find(|view| view.phase.is_live()) {
        info!(
            slug = %current_window.slug,
            question = %current_window.question,
            starts_at = ?current_window.window_start,
            ends_at = ?current_window.window_end,
            seconds_left = current_window.seconds_left,
            price = %current_window.current_price,
            target_price = %current_window.target_price,
            target_source = %current_window.target_price_source,
            target_gap_bps = %current_window.target_gap_bps,
            up_ask = %current_window.up_ask,
            down_ask = %current_window.down_ask,
            bundle_cost = %current_window.bundle_cost,
            spot_move_bps = %current_window.spot_move_bps,
            spot_move_5s_bps = %current_window.spot_move_5s_bps,
            dominant_outcome = %current_window.dominant_outcome,
            strategy_fit = current_window.strategy_fit,
            "analytics dominant-outcome bucket"
        );
    }

    if let Some(next_window) = views.iter().find(|view| view.phase.is_upcoming()) {
        info!(
            slug = %next_window.slug,
            question = %next_window.question,
            starts_at = ?next_window.window_start,
            ends_at = ?next_window.window_end,
            seconds_to_start = next_window.seconds_to_start,
            up_ask = %next_window.up_ask,
            down_ask = %next_window.down_ask,
            bundle_cost = %next_window.bundle_cost,
            "analytics actual-outcome bucket"
        );
    }

    info!("\n{}", render_market_table_v2(&views, top));
    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct RunLoopControl {
    mode_override: Option<BotMode>,
    once: bool,
    max_runtime_secs: Option<u64>,
    drain_open_positions: bool,
    max_drain_secs: u64,
}

fn controlled_run_trigger_wait_timeout(
    once: bool,
    max_runtime: Option<StdDuration>,
    run_started_at: Instant,
    drain_started_at: Option<Instant>,
    max_drain: StdDuration,
) -> Option<StdDuration> {
    if once {
        return None;
    }

    if let Some(drain_started_at) = drain_started_at {
        return Some(
            max_drain
                .checked_sub(drain_started_at.elapsed())
                .unwrap_or(StdDuration::ZERO),
        );
    }

    max_runtime.map(|max_runtime| {
        max_runtime
            .checked_sub(run_started_at.elapsed())
            .unwrap_or(StdDuration::ZERO)
    })
}

#[allow(clippy::too_many_lines)]
async fn run_loop(
    config: &AppConfig,
    data_client: &MarketDataClient,
    binance_client: &BinanceClient,
    coinbase_client: Option<&CoinbaseClient>,
    strategy: &BundleArbitrageStrategy,
    control: RunLoopControl,
) -> Result<()> {
    let mode = control.mode_override.unwrap_or(config.run.mode);
    let max_runtime = control.max_runtime_secs.map(StdDuration::from_secs);
    let max_drain = StdDuration::from_secs(control.max_drain_secs.max(1));
    let run_started_at = Instant::now();
    let mut drain_started_at: Option<Instant> = None;
    let journal = JournalStore::new(&config.storage)?;
    let paper_journal = (mode == BotMode::Paper).then(|| journal.spawn_paper_writer());
    let paper_cycle_journal_sample_secs = config.storage.paper_cycle_journal_sample_secs.max(1);
    let mut last_paper_cycle_journal_append_at: Option<DateTime<Utc>> = None;
    let PaperRuntimeBootstrap {
        mut journal_snapshot,
        mut executed_market_slugs,
        paper,
        mut risk_tracker,
    } = bootstrap_paper_runtime(config, &journal, mode)?;
    let mut reversed_market_slugs = HashSet::<String>::new();
    let mut v4_inventory_tracker = V4InventoryTracker::default();
    let mut repeat_entry_throttle = HashMap::<String, Instant>::new();
    let mut trigger_rx = config
        .run
        .reactive
        .then(|| binance_client.subscribe_triggers());
    let mut polymarket_trigger_rx = (config.run.reactive && config.run.polymarket_stream.enabled)
        .then(|| data_client.subscribe_market_triggers());

    start_runtime_market_streams(config, data_client, binance_client, coinbase_client);
    prewarm_runtime_polymarket_stream(config, data_client).await;
    warm_up_runtime_market_data(config, binance_client).await;

    if mode == BotMode::Live {
        let geo = data_client.geoblock_status().await?;
        if geo.blocked {
            return Err(AppError::Geoblocked {
                country: geo.country,
                region: geo.region,
            });
        }
    }

    let live = if mode == BotMode::Live {
        Some(LiveExecutor::new(&config.http.clob_base_url, &config.live).await?)
    } else {
        None
    };

    let mut first_cycle = true;
    let mut reactive_snapshot_cache = ReactiveMarketSnapshotCache::default();
    loop {
        risk_tracker.advance_cycle_cooldown();
        let trigger_wait_timeout = controlled_run_trigger_wait_timeout(
            control.once,
            max_runtime,
            run_started_at,
            drain_started_at,
            max_drain,
        );
        let trigger =
            if first_cycle || trigger_wait_timeout.is_some_and(|timeout| timeout.is_zero()) {
                None
            } else {
                let trigger = wait_for_runtime_trigger(
                    &config.run,
                    trigger_rx.as_mut(),
                    polymarket_trigger_rx.as_mut(),
                    trigger_wait_timeout,
                )
                .await;
                if config.run.reactive {
                    log_runtime_trigger(trigger.as_ref(), config);
                }
                trigger
            };
        first_cycle = false;

        if mode == BotMode::Paper {
            let closed_positions = settle_resolved_paper_positions(
                config,
                &journal,
                binance_client,
                &paper,
                &executed_market_slugs,
                &mut journal_snapshot,
                paper_journal.as_ref(),
            )
            .await?;
            risk_tracker.observe_closed_positions(&closed_positions);
            v4_inventory_tracker
                .observe_closed_positions(&config.run.v4_inventory, &closed_positions);
        }

        let cycle_started_at = Instant::now();
        let mut latency = RuntimeLatencyMetrics::default();
        let mut early_closed_positions = Vec::new();
        let mut paper_state = if mode == BotMode::Paper {
            Some(paper.snapshot().await)
        } else {
            None
        };
        if mode == BotMode::Paper
            && let Some(state) = paper_state.as_ref()
            && !state.open_positions.is_empty()
        {
            let open_position_slugs = state.open_positions.keys().cloned().collect::<Vec<_>>();
            let exit_snapshot_started_at = Instant::now();
            let exit_snapshot = match collect_exit_market_snapshot(
                config,
                data_client,
                binance_client,
                &open_position_slugs,
                &mut reactive_snapshot_cache,
            )
            .await
            {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    warn!(
                        error = %error,
                        "exit- ;"
                    );
                    continue;
                }
            };
            latency.exit_snapshot_ms = duration_ms_u64(exit_snapshot_started_at.elapsed());
            let early_exit_started_at = Instant::now();
            early_closed_positions = close_paper_positions_early(
                &journal,
                &paper,
                &exit_snapshot,
                &config.run.early_exit,
                paper_cost_model_from_config(config),
                &executed_market_slugs,
                &mut journal_snapshot,
                paper_journal.as_ref(),
            )
            .await?;
            latency.early_exit_eval_ms = duration_ms_u64(early_exit_started_at.elapsed());
            risk_tracker.observe_closed_positions(&early_closed_positions);
            v4_inventory_tracker
                .observe_closed_positions(&config.run.v4_inventory, &early_closed_positions);
            paper_state = Some(paper.snapshot().await);
        }

        let exposure = paper_state
            .as_ref()
            .map_or_else(HashMap::new, |state| state.market_notional.clone());
        let open_position_slugs = paper_state
            .as_ref()
            .map(|state| state.open_positions.keys().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        if !control.once
            && let Some(max_runtime) = max_runtime
            && run_started_at.elapsed() >= max_runtime
            && drain_started_at.is_none()
        {
            let open_positions = paper_state
                .as_ref()
                .map_or(0, |state| state.open_positions.len());
            if control.drain_open_positions && mode == BotMode::Paper && open_positions > 0 {
                drain_started_at = Some(Instant::now());
                info!(
                    open_positions,
                    max_drain_secs = control.max_drain_secs,
                    "controlled run duration reached; entering drain mode"
                );
            } else {
                info!(
                    max_runtime_secs = control.max_runtime_secs.unwrap_or_default(),
                    "controlled run duration reached; shutting down cleanly"
                );
                break;
            }
        }
        let (runtime_snapshot, analysis, analysis_timing) = match collect_runtime_analysis_frame(
            config,
            data_client,
            binance_client,
            strategy,
            &exposure,
            trigger.as_ref(),
            &open_position_slugs,
            &mut reactive_snapshot_cache,
        )
        .await
        {
            Ok(frame) => frame,
            Err(error) => {
                warn!(
                    error = %error,
                    "runtime- ;"
                );
                continue;
            }
        };
        if let Some(trigger) = trigger.as_ref() {
            latency.trigger_event_to_snapshot_ms =
                Some(non_negative_i64_to_u64(trigger.event_age_ms()));
            latency.trigger_received_to_snapshot_ms =
                Some(non_negative_i64_to_u64(trigger.received_age_ms()));
        }
        latency.runtime_snapshot_ms = analysis_timing.snapshot_ms;
        latency.analysis_ms = analysis_timing.analysis_ms;
        let AnalysisFrame {
            views: _views,
            opportunities,
            near_misses,
        } = analysis;
        let mut stop_and_reverse = Vec::new();
        if mode == BotMode::Paper {
            stop_and_reverse = collect_stop_and_reverse_opportunities(
                config,
                &early_closed_positions,
                &opportunities,
                &reversed_market_slugs,
            );
            if !stop_and_reverse.is_empty() {
                info!(
                    count = stop_and_reverse.len(),
                    top_slug = stop_and_reverse
                        .first()
                        .map_or("-", |entry| entry.slug.as_str()),
                    "stop-and-reverse"
                );
            }
        }
        let selection_started_at = Instant::now();
        let RegimeSelection {
            regime,
            reason: regime_reason,
            selected: regime_selected,
        } = select_opportunities_for_regime(config, &opportunities, &executed_market_slugs);
        let stop_and_reverse_slugs = stop_and_reverse
            .iter()
            .map(|opportunity| opportunity.slug.clone())
            .collect::<HashSet<_>>();
        let mut selected = stop_and_reverse;
        selected.extend(
            regime_selected
                .into_iter()
                .filter(|opportunity| !stop_and_reverse_slugs.contains(&opportunity.slug)),
        );
        if drain_started_at.is_some() && !selected.is_empty() {
            info!(
                skipped_entries = selected.len(),
                "controlled run drain mode active; skipping new entries"
            );
            selected.clear();
        }
        let selected_count = selected.len();
        let risk_active = should_apply_risk_limits(mode, &config.run.risk);
        let paper_state_before_exec = paper.snapshot().await;
        let risk_context = build_runtime_risk_context(
            &paper_state_before_exec,
            config.run.paper_starting_balance_usdc,
            &runtime_snapshot.books,
            paper_cost_model_from_config(config),
        );
        let mut risk_block_reason = if risk_active {
            risk_tracker.evaluate_and_arm_limits(&config.run.risk, risk_context)
        } else {
            None
        };
        if risk_active && risk_tracker.is_blocked() {
            if risk_block_reason.is_none() {
                risk_block_reason = risk_tracker.current_block_reason();
            }
            selected.clear();
            if let Some(reason) = &risk_block_reason {
                warn!(
                    reason = %reason,
                    daily_realized_profit = %risk_tracker.daily_realized_profit.round_dp(4),
                    consecutive_losses = risk_tracker.consecutive_losses,
                    cooldown_cycles_left = risk_tracker.cooldown_remaining_cycles,
                    ": risk kill-switch"
                );
            }
        }
        latency.selection_ms = duration_ms_u64(selection_started_at.elapsed());
        let mut executed_count = 0_usize;
        let execution_started_at = Instant::now();
        let mut revalidation_ms = 0_u64;

        if opportunities.is_empty() {
            debug!("no opportunities in current cycle");
        } else {
            if selected.is_empty() {
                debug!("opportunities found but none selected");
            }

            for selected_opportunity in selected {
                let paper_snapshot = paper.snapshot().await;
                let current_exposure = paper_snapshot.market_notional.clone();
                let opportunity = if config.run.revalidate_before_execute {
                    let revalidation_started_at = Instant::now();
                    let Some(refreshed) = revalidate_selected_opportunity(RevalidationRequest {
                        mode,
                        config,
                        data_client,
                        binance_client,
                        strategy,
                        runtime_snapshot: &runtime_snapshot,
                        candidate: &selected_opportunity,
                        market_notional: &current_exposure,
                    })
                    .await?
                    else {
                        revalidation_ms = revalidation_ms
                            .saturating_add(duration_ms_u64(revalidation_started_at.elapsed()));
                        warn!(
                            slug = %selected_opportunity.slug,
                            ","
                        );
                        continue;
                    };
                    revalidation_ms = revalidation_ms
                        .saturating_add(duration_ms_u64(revalidation_started_at.elapsed()));
                    refreshed
                } else {
                    selected_opportunity
                };

                let pre_ratchet_required_usdc = opportunity.required_usdc;
                let Some(opportunity) = apply_pnl_ratchet_to_opportunity(
                    &config.run.pnl_ratchet,
                    &risk_tracker,
                    paper_snapshot.total_realized_profit,
                    &opportunity,
                    config.strategy.min_top_of_book_shares,
                ) else {
                    info!(
                        slug = %opportunity.slug,
                        kind = opportunity.kind.as_str(),
                        "pnl-ratchet skipped entry because scaled size fell below minimum shares"
                    );
                    continue;
                };
                if opportunity.required_usdc < pre_ratchet_required_usdc {
                    info!(
                        slug = %opportunity.slug,
                        kind = opportunity.kind.as_str(),
                        from_usdc = %pre_ratchet_required_usdc.round_dp(4),
                        to_usdc = %opportunity.required_usdc.round_dp(4),
                        "pnl-ratchet capped entry notional"
                    );
                }

                if let Some(reason) = repeated_entry_block_reason(
                    config,
                    &paper_snapshot,
                    &executed_market_slugs,
                    &opportunity,
                ) {
                    info!(
                        slug = %opportunity.slug,
                        kind = opportunity.kind.as_str(),
                        reason = %reason,
                        "repeat-entry guard skipped candidate"
                    );
                    continue;
                }

                let repeat_entry_observed_at = Instant::now();
                if let Some(reason) = repeat_entry_throttle_block_reason(
                    config,
                    &repeat_entry_throttle,
                    &opportunity,
                    repeat_entry_observed_at,
                ) {
                    debug!(
                        slug = %opportunity.slug,
                        kind = opportunity.kind.as_str(),
                        reason = %reason,
                        "repeat-entry throttle skipped candidate"
                    );
                    continue;
                }

                if risk_active
                    && let Some(reason) = projected_open_notional_block_reason_with_costs(
                        &config.run.risk,
                        paper_cost_model_from_config(config),
                        &paper_snapshot,
                        &opportunity,
                    )
                {
                    warn!(
                        slug = %opportunity.slug,
                        kind = opportunity.kind.as_str(),
                        required_usdc = %opportunity.required_usdc.round_dp(4),
                        reason = %reason,
                        "entry blocked by projected open notional guard"
                    );
                    continue;
                }

                if let Some(reason) = v4_inventory_block_reason(
                    &config.run.v4_inventory,
                    &mut v4_inventory_tracker,
                    &paper_snapshot,
                    &opportunity,
                ) {
                    info!(
                        slug = %opportunity.slug,
                        kind = opportunity.kind.as_str(),
                        reason = %reason,
                        "v4 inventory overlay skipped entry"
                    );
                    continue;
                }

                if let Some(reason) = paper_cash_block_reason(config, &paper_snapshot, &opportunity)
                {
                    warn!(
                        slug = %opportunity.slug,
                        kind = opportunity.kind.as_str(),
                        required_usdc = %opportunity.required_usdc.round_dp(4),
                        reason = %reason,
                        "entry blocked by paper cash guard"
                    );
                    continue;
                }

                let report = match mode {
                    BotMode::Paper => execute_and_log(&paper, &opportunity).await?,
                    BotMode::Live => {
                        let Some(live_executor) = live.as_ref() else {
                            return Err(AppError::LiveExecution("live-".to_owned()));
                        };
                        execute_and_log(live_executor, &opportunity).await?
                    }
                };
                if mode == BotMode::Paper && config.run.v4_inventory.enabled {
                    v4_inventory_tracker.observe_opened_opportunity(&opportunity);
                }
                let paper_state = if mode == BotMode::Paper {
                    paper.snapshot().await
                } else {
                    paper.record_fill(&opportunity).await
                };
                if mode == BotMode::Paper {
                    let open_trade = build_paper_open_trade(&opportunity, &report);
                    if let Some(writer) = paper_journal.as_ref() {
                        writer.record_trade(open_trade)?;
                    } else {
                        journal.record_paper_trade(&open_trade)?;
                    }
                    log_paper_open(&opportunity, &report, &paper_state);
                }
                executed_market_slugs.insert(opportunity.slug.clone());
                if stop_and_reverse_slugs.contains(&opportunity.slug) {
                    reversed_market_slugs.insert(opportunity.slug.clone());
                }
                journal.record_execution_in_place(
                    &mut journal_snapshot,
                    &opportunity,
                    &report,
                    &paper_state,
                    &executed_market_slugs,
                )?;
                record_repeat_entry_throttle(
                    &mut repeat_entry_throttle,
                    &opportunity,
                    repeat_entry_observed_at,
                );
                flush_paper_journal_if_needed(paper_journal.as_ref())?;
                executed_count += 1;
                info!(
                    mode = %report.mode,
                    signal_kind = opportunity.kind.as_str(),
                    spent_usdc = %report.spent_usdc,
                    expected_profit = %report.expected_profit,
                    shares = %report.shares,
                    execution_count = journal_snapshot.execution_count,
                    unique_market_windows = journal_snapshot.executed_market_slugs.len(),
                    slug = %opportunity.slug,
                    question = %report.question,
                    "paper execution recorded"
                );
            }
        }
        latency.revalidation_ms = revalidation_ms;
        latency.execution_ms = duration_ms_u64(execution_started_at.elapsed());
        latency.cycle_total_ms = duration_ms_u64(cycle_started_at.elapsed());
        let runtime_summary =
            build_runtime_snapshot_summary(&runtime_snapshot, &opportunities, latency);
        log_live_cycle_snapshot(config, &runtime_summary, &opportunities, &near_misses);

        let mut open_positions_after_cycle = None;
        if mode == BotMode::Paper {
            let paper_state = paper.snapshot().await;
            let open_position_count = paper_state.open_positions.len();
            open_positions_after_cycle = Some(open_position_count);
            let cycle_entry = build_paper_cycle_entry(
                config,
                &runtime_snapshot,
                &runtime_summary,
                &opportunities,
                &near_misses,
                selected_count,
                executed_count,
                &paper_state,
                regime,
                regime_reason.as_deref(),
                risk_block_reason.as_deref(),
                &risk_tracker,
            );
            let append_cycle = should_append_paper_cycle_journal(
                &cycle_entry,
                last_paper_cycle_journal_append_at,
                paper_cycle_journal_sample_secs,
            );
            if append_cycle {
                last_paper_cycle_journal_append_at = Some(cycle_entry.recorded_at);
            }
            if let Some(writer) = paper_journal.as_ref() {
                if append_cycle {
                    writer.record_cycle(cycle_entry.clone())?;
                } else {
                    writer.record_cycle_latest(cycle_entry.clone())?;
                }
            } else if append_cycle {
                journal.record_paper_cycle(&cycle_entry)?;
            } else {
                journal.record_paper_cycle_latest(&cycle_entry)?;
            }
            log_paper_cycle_summary(&cycle_entry);

            if let Some(drain_started_at) = drain_started_at {
                if open_position_count == 0 {
                    info!("controlled run drain mode completed; no open paper positions remain");
                    break;
                }
                if drain_started_at.elapsed() >= max_drain {
                    warn!(
                        open_positions = open_position_count,
                        max_drain_secs = control.max_drain_secs,
                        "controlled run drain mode limit reached; shutting down with open paper positions"
                    );
                    break;
                }
            }
        }

        if control.once {
            break;
        }
        if drain_started_at.is_none()
            && max_runtime.is_some_and(|duration| run_started_at.elapsed() >= duration)
        {
            let open_positions = open_positions_after_cycle.unwrap_or_default();
            if control.drain_open_positions && mode == BotMode::Paper && open_positions > 0 {
                drain_started_at = Some(Instant::now());
                info!(
                    open_positions,
                    max_drain_secs = control.max_drain_secs,
                    "controlled run duration reached after cycle; entering drain mode"
                );
            } else {
                info!(
                    max_runtime_secs = control.max_runtime_secs.unwrap_or_default(),
                    "controlled run duration reached; shutting down cleanly"
                );
                break;
            }
        }
    }

    if let Some(writer) = paper_journal {
        writer.shutdown()?;
    }

    Ok(())
}

struct PaperRuntimeBootstrap {
    journal_snapshot: PnlSnapshot,
    executed_market_slugs: HashSet<String>,
    paper: PaperExecutor,
    risk_tracker: RiskTracker,
}

fn bootstrap_paper_runtime(
    config: &AppConfig,
    journal: &JournalStore,
    mode: BotMode,
) -> Result<PaperRuntimeBootstrap> {
    let restore_paper_state = config.run.should_restore_paper_state_on_start();
    let seed_risk_from_history = config.run.should_seed_risk_from_history();
    let paper_start_mode = config.run.effective_paper_start_mode();
    let journal_snapshot = if mode == BotMode::Paper && !restore_paper_state {
        info!("paper-state");
        PnlSnapshot::default()
    } else {
        journal.load_snapshot()?
    };
    let executed_market_slugs = journal_snapshot.executed_market_slugs.clone();
    if restore_paper_state && journal_snapshot.execution_count > 0 {
        info!(
            paper_start_mode = paper_start_mode.as_str(),
            execution_count = journal_snapshot.execution_count,
            total_spent_usdc = %journal_snapshot.paper_state.total_spent_usdc,
            total_expected_profit = %journal_snapshot.paper_state.total_expected_profit,
            unique_market_windows = executed_market_slugs.len(),
            "restored paper runtime state"
        );
    }

    let session_start_realized_profit = journal_snapshot.paper_state.total_realized_profit;
    let paper_cost_model = paper_cost_model_from_config(config);
    let paper =
        PaperExecutor::with_state_and_costs(journal_snapshot.paper_state.clone(), paper_cost_model);
    let mut risk_seed = if seed_risk_from_history {
        load_risk_seed_from_trades(journal)?
    } else {
        RiskSeed::default()
    };
    if config.run.risk.reset_daily_on_start {
        risk_seed.daily_realized_profit = Decimal::ZERO;
        risk_seed.consecutive_losses = 0;
    }
    info!(
        paper_start_mode = paper_start_mode.as_str(),
        seed_from_history = seed_risk_from_history,
        reset_daily_on_start = config.run.risk.reset_daily_on_start,
        seeded_daily_realized_profit = %risk_seed.daily_realized_profit.round_dp(4),
        seeded_consecutive_losses = risk_seed.consecutive_losses,
        "-"
    );
    let risk_tracker = RiskTracker::new(
        session_start_realized_profit,
        risk_seed.daily_realized_profit,
        risk_seed.consecutive_losses,
    );

    Ok(PaperRuntimeBootstrap {
        journal_snapshot,
        executed_market_slugs,
        paper,
        risk_tracker,
    })
}

fn paper_cost_model_from_config(config: &AppConfig) -> PaperCostModel {
    PaperCostModel::new(config.strategy.assumed_fee_bps, 0)
}

fn paper_open_notional(state: &PaperState) -> Decimal {
    state
        .market_notional
        .values()
        .copied()
        .sum::<Decimal>()
        .round_dp(6)
}

fn paper_cash(starting_balance: Decimal, state: &PaperState) -> Decimal {
    (starting_balance - paper_open_notional(state) + state.total_realized_profit).round_dp(6)
}

fn paper_cash_block_reason(
    config: &AppConfig,
    paper_state: &PaperState,
    opportunity: &Opportunity,
) -> Option<String> {
    paper_cash_block_reason_with_costs(
        config.run.paper_starting_balance_usdc,
        paper_cost_model_from_config(config),
        paper_state,
        opportunity,
    )
}

fn paper_cash_block_reason_with_costs(
    paper_starting_balance_usdc: Option<Decimal>,
    cost_model: PaperCostModel,
    paper_state: &PaperState,
    opportunity: &Opportunity,
) -> Option<String> {
    let starting_balance = paper_starting_balance_usdc?;
    let available_cash = paper_cash(starting_balance, paper_state);
    let required_cash = cost_model
        .gross_entry_spend(opportunity.required_usdc)
        .round_dp(6);
    if required_cash <= available_cash {
        return None;
    }

    Some(format!(
        "insufficient paper cash: required {} USDC incl. costs, available {} USDC",
        required_cash.round_dp(4),
        available_cash.round_dp(4)
    ))
}

fn projected_open_notional_block_reason_with_costs(
    risk: &RiskControlConfig,
    cost_model: PaperCostModel,
    paper_state: &PaperState,
    opportunity: &Opportunity,
) -> Option<String> {
    if risk.max_open_notional_usdc <= Decimal::ZERO {
        return None;
    }

    let current_open_notional = paper_open_notional(paper_state);
    let entry_notional = cost_model
        .gross_entry_spend(opportunity.required_usdc)
        .round_dp(6);
    let projected_open_notional = (current_open_notional + entry_notional).round_dp(6);
    if projected_open_notional < risk.max_open_notional_usdc {
        return None;
    }

    Some(format!(
        "projected open notional limit reached: {} >= {} USDC (current {}, entry incl. costs {})",
        projected_open_notional.round_dp(4),
        risk.max_open_notional_usdc.round_dp(4),
        current_open_notional.round_dp(4),
        entry_notional.round_dp(4)
    ))
}

#[derive(Debug, Clone, Copy)]
struct RuntimeRiskContext {
    total_realized_profit: Decimal,
    open_notional: Decimal,
    unrealized_profit: Decimal,
    paper_cash: Option<Decimal>,
}

fn build_runtime_risk_context(
    paper_state: &PaperState,
    paper_starting_balance_usdc: Option<Decimal>,
    books: &HashMap<String, OrderBook>,
    paper_cost_model: PaperCostModel,
) -> RuntimeRiskContext {
    let open_notional = paper_open_notional(paper_state);
    let unrealized_profit = paper_unrealized_profit(paper_state, books, paper_cost_model);
    let paper_cash = paper_starting_balance_usdc.map(|starting_balance| {
        (starting_balance - open_notional + paper_state.total_realized_profit).round_dp(6)
    });

    RuntimeRiskContext {
        total_realized_profit: paper_state.total_realized_profit,
        open_notional,
        unrealized_profit,
        paper_cash,
    }
}

fn paper_unrealized_profit(
    paper_state: &PaperState,
    books: &HashMap<String, OrderBook>,
    paper_cost_model: PaperCostModel,
) -> Decimal {
    paper_state
        .open_positions
        .values()
        .map(|position| paper_position_net_mark_to_market_profit(position, books, paper_cost_model))
        .sum::<Decimal>()
        .round_dp(6)
}

fn start_runtime_market_streams(
    config: &AppConfig,
    data_client: &MarketDataClient,
    binance_client: &BinanceClient,
    coinbase_client: Option<&CoinbaseClient>,
) {
    for symbol in configured_binance_symbols(config) {
        if binance_client.start_trade_stream(symbol) {
            info!(symbol, "websocket- Binance");
        } else {
            warn!("websocket- Binance");
        }
    }

    if let Some(coinbase_client) = coinbase_client {
        for target in configured_market_targets(config) {
            if coinbase_client.start_ticker_stream(target) {
                info!(
                    product_id = target.coinbase_product_id(),
                    symbol = target.binance_symbol(),
                    "started Coinbase ticker stream as secondary live market data"
                );
            } else {
                warn!(
                    product_id = target.coinbase_product_id(),
                    "failed to start Coinbase ticker stream"
                );
            }
        }
    }

    if config.run.chainlink_oracle.enabled {
        let targets = configured_market_targets(config);
        if binance_client.start_chainlink_oracle_stream(
            config.http.polymarket_rtds_ws_url.clone(),
            &targets,
            config.run.chainlink_oracle,
        ) {
            info!("started Polymarket RTDS Chainlink oracle stream");
        } else {
            warn!("failed to start Polymarket RTDS Chainlink oracle stream");
        }
    }

    if config.run.polymarket_stream.enabled {
        data_client.ensure_market_stream_started();
        info!("websocket- Polymarket");
    }
}

async fn prewarm_runtime_polymarket_stream(config: &AppConfig, data_client: &MarketDataClient) {
    if !config.run.polymarket_stream.enabled {
        return;
    }

    match fetch_current_live_markets(data_client, &config.strategy.market_targets).await {
        Ok(markets) if markets.is_empty() => {
            warn!("Polymarket stream prewarm found no current live markets");
        }
        Ok(markets) => {
            let market_count = markets.len();
            data_client.register_live_markets(&markets).await;
            info!(
                markets = market_count,
                "prewarmed Polymarket live stream subscriptions"
            );
        }
        Err(error) => {
            warn!(
                error = %error,
                "Polymarket stream prewarm failed; reactive loop will retry from hot path"
            );
        }
    }
}

async fn warm_up_runtime_market_data(config: &AppConfig, binance_client: &BinanceClient) {
    let symbols = configured_binance_symbols(config);
    if symbols.is_empty() {
        return;
    }

    let started_at = Instant::now();
    loop {
        let latest_health = binance_client
            .live_market_data_health_for_symbols(&symbols)
            .await;
        let quotes_ready = latest_health
            .iter()
            .all(|health| health.has_fresh_quote(MARKET_DATA_WARMUP_MAX_QUOTE_AGE_MS));
        let depth_ready = latest_health
            .iter()
            .all(|health| health.has_fresh_depth(MARKET_DATA_WARMUP_MAX_DEPTH_AGE_MS));

        if quotes_ready && depth_ready {
            info!(
                symbols = symbols.len(),
                elapsed_ms = started_at.elapsed().as_millis(),
                health = %format_live_market_data_health(&latest_health),
                "runtime market-data warmup ready"
            );
            return;
        }

        if started_at.elapsed() >= Duration::from_millis(MARKET_DATA_WARMUP_TIMEOUT_MS) {
            warn!(
                symbols = symbols.len(),
                elapsed_ms = started_at.elapsed().as_millis(),
                quotes_ready,
                depth_ready,
                health = %format_live_market_data_health(&latest_health),
                "runtime market-data warmup incomplete; continuing with strategy-side freshness guards"
            );
            return;
        }

        sleep(Duration::from_millis(MARKET_DATA_WARMUP_POLL_MS)).await;
    }
}

fn format_live_market_data_health(health: &[LiveMarketDataHealth]) -> String {
    health
        .iter()
        .map(|entry| {
            format!(
                "{}:quote={} recv_age_ms={} points={} depth_age_ms={}",
                entry.symbol,
                entry.quote_source.unwrap_or("-"),
                entry
                    .quote_received_age_ms
                    .map_or_else(|| "-".to_owned(), |age_ms| age_ms.to_string()),
                entry.quote_points,
                entry
                    .depth_age_ms
                    .map_or_else(|| "-".to_owned(), |age_ms| age_ms.to_string())
            )
        })
        .collect::<Vec<_>>()
        .join(";")
}

#[derive(Debug, Clone)]
struct RegimeSelection {
    regime: RuntimeRegime,
    reason: Option<String>,
    selected: Vec<Opportunity>,
}

fn select_opportunities_for_regime(
    config: &AppConfig,
    opportunities: &[Opportunity],
    executed_market_slugs: &HashSet<String>,
) -> RegimeSelection {
    let base = opportunities
        .iter()
        .filter(|opportunity| {
            config.run.allow_repeat_entries_same_window
                || !executed_market_slugs.contains(&opportunity.slug)
        })
        .cloned()
        .collect::<Vec<_>>();

    if base.is_empty() {
        let adaptive_enabled = config.run.adaptive_regime.enabled;
        return RegimeSelection {
            regime: if adaptive_enabled {
                RuntimeRegime::Safe
            } else {
                RuntimeRegime::Aggressive
            },
            reason: Some(if adaptive_enabled {
                "-".to_owned()
            } else {
                "adaptive regime disabled".to_owned()
            }),
            selected: Vec::new(),
        };
    }

    if !config.run.adaptive_regime.enabled {
        return RegimeSelection {
            regime: RuntimeRegime::Aggressive,
            reason: Some("adaptive regime disabled".to_owned()),
            selected: base.into_iter().take(config.run.execute_top_n).collect(),
        };
    }

    let strongest_spot_move = opportunities
        .iter()
        .map(|entry| entry.spot_move_bps.abs())
        .max()
        .unwrap_or(Decimal::ZERO);
    let cheapest_bundle = opportunities
        .iter()
        .map(|entry| entry.bundle_cost)
        .min()
        .unwrap_or(Decimal::from(2_u32));
    let aggressive = strongest_spot_move
        >= Decimal::from(config.run.adaptive_regime.aggressive_min_spot_move_bps)
        && cheapest_bundle <= config.run.adaptive_regime.aggressive_max_bundle_cost;

    if aggressive {
        let reason = format!(
            "aggressive: spot {} bps >= {} bps bundle {} <= {}",
            strongest_spot_move.round_dp(2),
            config.run.adaptive_regime.aggressive_min_spot_move_bps,
            cheapest_bundle.round_dp(4),
            config
                .run
                .adaptive_regime
                .aggressive_max_bundle_cost
                .round_dp(4)
        );
        return RegimeSelection {
            regime: RuntimeRegime::Aggressive,
            reason: Some(reason),
            selected: base.into_iter().take(config.run.execute_top_n).collect(),
        };
    }

    let safe_limit = config
        .run
        .adaptive_regime
        .safe_max_entries_per_cycle
        .min(config.run.execute_top_n)
        .max(1);
    let selected = base
        .into_iter()
        .filter(|entry| {
            if entry.bundle_cost > config.run.adaptive_regime.safe_max_bundle_cost {
                return false;
            }
            if config.run.adaptive_regime.safe_bundle_only {
                entry.kind == OpportunityKind::BundleArbitrage
            } else {
                true
            }
        })
        .take(safe_limit)
        .collect::<Vec<_>>();
    let safe_mode = if config.run.adaptive_regime.safe_bundle_only {
        "safe(bundle-only)"
    } else {
        "safe"
    };
    let reason = format!(
        "{safe_mode}: spot {} bps, bundle {} > aggressive-threshold",
        strongest_spot_move.round_dp(2),
        cheapest_bundle.round_dp(4)
    );
    RegimeSelection {
        regime: RuntimeRegime::Safe,
        reason: Some(reason),
        selected,
    }
}

fn repeated_entry_block_reason(
    config: &AppConfig,
    paper_state: &PaperState,
    executed_market_slugs: &HashSet<String>,
    opportunity: &Opportunity,
) -> Option<String> {
    let Some(existing_position) = paper_state.open_positions.get(&opportunity.slug) else {
        if !config.run.allow_repeat_entries_same_window
            && executed_market_slugs.contains(&opportunity.slug)
        {
            return Some("repeat entry skipped because window was already traded".to_owned());
        }
        return None;
    };

    if !config.run.allow_repeat_entries_same_window {
        return Some("repeat entry skipped because an open position already exists".to_owned());
    }

    if !config.run.scale_in.enabled {
        return Some(
            "repeat entry skipped because scale-in is disabled while a position is open".to_owned(),
        );
    }

    let max_total_entries =
        1_u32.saturating_add(config.run.scale_in.max_additional_entries_per_window);
    let entry_count = existing_position.entry_count.max(1);
    if entry_count >= max_total_entries {
        return Some(format!(": {entry_count} {max_total_entries}"));
    }

    if !position_matches_opportunity(existing_position, opportunity) {
        return Some(",".to_owned());
    }

    let signal_upgrade = allows_signal_upgrade(config, existing_position, opportunity);
    let current_reference_price = entry_reference_price(opportunity);
    let best_reference_price = position_best_entry_reference_price(existing_position);
    let price_improvement = (best_reference_price - current_reference_price).round_dp(4);
    if best_reference_price > Decimal::ZERO
        && price_improvement < config.run.scale_in.min_price_improvement
        && !signal_upgrade
    {
        return Some(format!(
            "{}: {}",
            config.run.scale_in.min_price_improvement.round_dp(4),
            price_improvement.round_dp(4)
        ));
    }

    if config.run.scale_in.require_stronger_binance_impulse {
        let previous_impulse = existing_position.spot_move_bps_at_entry.abs();
        let current_impulse = opportunity.spot_move_bps.abs();
        let impulse_improvement = (current_impulse - previous_impulse).round_dp(4);
        if impulse_improvement < config.run.scale_in.min_impulse_improvement_bps {
            return Some(format!(
                "Binance {} bps: {} bps",
                config.run.scale_in.min_impulse_improvement_bps.round_dp(4),
                impulse_improvement.round_dp(4)
            ));
        }
    }

    None
}

fn repeat_entry_throttle_block_reason(
    config: &AppConfig,
    last_entry_at: &HashMap<String, Instant>,
    opportunity: &Opportunity,
    now: Instant,
) -> Option<String> {
    let min_interval_ms = config.run.repeat_entry_min_interval_ms;
    if min_interval_ms == 0 || !config.run.allow_repeat_entries_same_window {
        return None;
    }

    let key = repeat_entry_throttle_key(opportunity);
    let previous_entry_at = last_entry_at.get(&key)?;

    let min_interval = StdDuration::from_millis(min_interval_ms);
    let elapsed = now.saturating_duration_since(*previous_entry_at);
    if elapsed >= min_interval {
        return None;
    }

    let remaining = min_interval.saturating_sub(elapsed);
    Some(format!(
        "repeat entry throttled for {min_interval_ms}ms ({}ms remaining)",
        remaining.as_millis()
    ))
}

fn record_repeat_entry_throttle(
    last_entry_at: &mut HashMap<String, Instant>,
    opportunity: &Opportunity,
    now: Instant,
) {
    if !opportunity.slug.is_empty() {
        last_entry_at.insert(repeat_entry_throttle_key(opportunity), now);
    }
}

fn repeat_entry_throttle_key(opportunity: &Opportunity) -> String {
    let side = if opportunity.primary_outcome_token_id.is_empty() {
        opportunity.primary_outcome_label.as_str()
    } else {
        opportunity.primary_outcome_token_id.as_str()
    };
    format!("{}|{}|{side}", opportunity.slug, opportunity.kind.as_str())
}

fn v4_inventory_block_reason(
    overlay: &V4InventoryConfig,
    tracker: &mut V4InventoryTracker,
    paper_state: &PaperState,
    opportunity: &Opportunity,
) -> Option<String> {
    if !overlay.enabled
        || !(is_directional_kind(opportunity.kind) || is_micro_breakout_kind(opportunity.kind))
    {
        return None;
    }

    if let Some(reason) = tracker.active_cooldown_reason(&opportunity.slug) {
        return Some(reason);
    }

    let current = current_slug_inventory_exposure(paper_state, &opportunity.slug);
    let addition = opportunity_inventory_exposure(opportunity);
    let post_fill = current.add(addition);
    let post_fill_spent =
        (tracker.opened_spent_for_slug(&opportunity.slug) + opportunity.required_usdc).round_dp(6);
    let post_fill_entries = tracker
        .opened_entries_for_slug(&opportunity.slug)
        .saturating_add(1);

    if overlay.max_window_spent_usdc > Decimal::ZERO
        && post_fill_spent > overlay.max_window_spent_usdc
    {
        return Some(format!(
            "v4 window spent cap exceeded: post-fill {} > {} USDC",
            post_fill_spent.round_dp(4),
            overlay.max_window_spent_usdc.round_dp(4)
        ));
    }

    if overlay.max_entries_per_window > 0 && post_fill_entries > overlay.max_entries_per_window {
        return Some(format!(
            "v4 entry-count cap exceeded: post-fill {} > {} entries",
            post_fill_entries, overlay.max_entries_per_window
        ));
    }

    if overlay.max_gross_inventory_shares_per_window > Decimal::ZERO
        && post_fill.gross > overlay.max_gross_inventory_shares_per_window
    {
        return Some(format!(
            "v4 gross inventory cap exceeded: post-fill {} > {} shares",
            post_fill.gross.round_dp(4),
            overlay.max_gross_inventory_shares_per_window.round_dp(4)
        ));
    }

    if overlay.max_directional_delta_shares_per_window > Decimal::ZERO
        && post_fill.delta > overlay.max_directional_delta_shares_per_window
    {
        return Some(format!(
            "v4 directional delta cap exceeded: post-fill {} > {} shares",
            post_fill.delta.round_dp(4),
            overlay.max_directional_delta_shares_per_window.round_dp(4)
        ));
    }

    None
}

fn current_slug_inventory_exposure(paper_state: &PaperState, slug: &str) -> InventoryExposure {
    paper_state.open_positions.get(slug).map_or_else(
        InventoryExposure::default,
        paper_position_inventory_exposure,
    )
}

fn paper_position_inventory_exposure(position: &PaperPosition) -> InventoryExposure {
    let (up_shares, down_shares) = paper_position_side_totals(&position.legs);
    InventoryExposure::from_side_totals(up_shares, down_shares)
}

fn paper_position_side_totals(legs: &[crate::models::PaperPositionLeg]) -> (Decimal, Decimal) {
    let mut up_shares = Decimal::ZERO;
    let mut down_shares = Decimal::ZERO;
    for leg in legs {
        match leg.side {
            PaperOutcomeSide::Up => up_shares += leg.shares,
            PaperOutcomeSide::Down => down_shares += leg.shares,
            PaperOutcomeSide::Unknown => {}
        }
    }
    (up_shares.round_dp(6), down_shares.round_dp(6))
}

fn opportunity_inventory_exposure(opportunity: &Opportunity) -> InventoryExposure {
    match opportunity.kind {
        OpportunityKind::BundleArbitrage => {
            let first_leg_side = PaperOutcomeSide::from_label(&opportunity.outcome_a_label);
            let second_leg_side = PaperOutcomeSide::from_label(&opportunity.outcome_b_label);
            let mut up_shares = Decimal::ZERO;
            let mut down_shares = Decimal::ZERO;
            add_side_shares(
                &mut up_shares,
                &mut down_shares,
                first_leg_side,
                opportunity.tradable_shares,
            );
            add_side_shares(
                &mut up_shares,
                &mut down_shares,
                second_leg_side,
                opportunity.tradable_shares,
            );
            InventoryExposure::from_side_totals(up_shares, down_shares)
        }
        OpportunityKind::DirectionalMomentum
        | OpportunityKind::TargetStateV1
        | OpportunityKind::BonereaperStateV1
        | OpportunityKind::BonereaperStateV2
        | OpportunityKind::BonereaperStateGuarded
        | OpportunityKind::CodexSentinelV1
        | OpportunityKind::CodexScalpProbeV1
        | OpportunityKind::MicroBreakout => {
            let mut up_shares = Decimal::ZERO;
            let mut down_shares = Decimal::ZERO;
            add_side_shares(
                &mut up_shares,
                &mut down_shares,
                PaperOutcomeSide::from_label(&opportunity.primary_outcome_label),
                opportunity.tradable_shares,
            );
            InventoryExposure::from_side_totals(up_shares, down_shares)
        }
        OpportunityKind::DirectionalMomentumHedged => {
            let mut up_shares = Decimal::ZERO;
            let mut down_shares = Decimal::ZERO;
            add_side_shares(
                &mut up_shares,
                &mut down_shares,
                PaperOutcomeSide::from_label(&opportunity.primary_outcome_label),
                opportunity.tradable_shares,
            );
            if let Some(label) = opportunity.hedge_outcome_label.as_deref() {
                add_side_shares(
                    &mut up_shares,
                    &mut down_shares,
                    PaperOutcomeSide::from_label(label),
                    opportunity.hedge_shares,
                );
            }
            InventoryExposure::from_side_totals(up_shares, down_shares)
        }
    }
}

fn add_side_shares(
    up_shares: &mut Decimal,
    down_shares: &mut Decimal,
    side: PaperOutcomeSide,
    shares: Decimal,
) {
    match side {
        PaperOutcomeSide::Up => *up_shares += shares,
        PaperOutcomeSide::Down => *down_shares += shares,
        PaperOutcomeSide::Unknown => {}
    }
}

fn position_matches_opportunity(position: &PaperPosition, opportunity: &Opportunity) -> bool {
    match opportunity.kind {
        OpportunityKind::BundleArbitrage => true,
        OpportunityKind::DirectionalMomentum
        | OpportunityKind::TargetStateV1
        | OpportunityKind::BonereaperStateV1
        | OpportunityKind::BonereaperStateV2
        | OpportunityKind::BonereaperStateGuarded
        | OpportunityKind::CodexSentinelV1
        | OpportunityKind::CodexScalpProbeV1
        | OpportunityKind::MicroBreakout => position
            .legs
            .iter()
            .any(|leg| leg.token_id == opportunity.primary_outcome_token_id),
        OpportunityKind::DirectionalMomentumHedged => position
            .legs
            .iter()
            .any(|leg| leg.token_id == opportunity.primary_outcome_token_id),
    }
}

fn allows_signal_upgrade(
    config: &AppConfig,
    existing_position: &PaperPosition,
    opportunity: &Opportunity,
) -> bool {
    is_directional_kind(opportunity.kind)
        && existing_position.entry_count <= 1
        && existing_position.spent_usdc <= config.strategy.directional_soft_entry_min_notional_usdc
        && opportunity.required_usdc >= config.strategy.directional_soft_entry_min_notional_usdc
        && opportunity.spot_move_bps.abs() >= existing_position.spot_move_bps_at_entry.abs()
        && position_matches_opportunity(existing_position, opportunity)
}

fn entry_reference_price(opportunity: &Opportunity) -> Decimal {
    match opportunity.kind {
        OpportunityKind::BundleArbitrage => opportunity.bundle_cost,
        OpportunityKind::DirectionalMomentum
        | OpportunityKind::TargetStateV1
        | OpportunityKind::BonereaperStateV1
        | OpportunityKind::BonereaperStateV2
        | OpportunityKind::BonereaperStateGuarded
        | OpportunityKind::CodexSentinelV1
        | OpportunityKind::CodexScalpProbeV1
        | OpportunityKind::MicroBreakout => opportunity.primary_outcome_ask_price,
        OpportunityKind::DirectionalMomentumHedged => {
            opportunity.primary_outcome_ask_price
                + opportunity.hedge_outcome_ask_price.unwrap_or(Decimal::ZERO)
        }
    }
}

fn position_best_entry_reference_price(position: &PaperPosition) -> Decimal {
    if position.best_entry_reference_price > Decimal::ZERO {
        return position.best_entry_reference_price;
    }

    position
        .legs
        .iter()
        .map(|leg| leg.entry_price)
        .sum::<Decimal>()
}

fn collect_stop_and_reverse_opportunities(
    config: &AppConfig,
    close_reports: &[PaperCloseReport],
    opportunities: &[Opportunity],
    reversed_market_slugs: &HashSet<String>,
) -> Vec<Opportunity> {
    if !config.run.early_exit.stop_and_reverse_enabled {
        return Vec::new();
    }

    close_reports
        .iter()
        .filter(|report| is_directional_kind(report.kind) || is_micro_breakout_kind(report.kind))
        .filter(|report| is_stop_and_reverse_reason(&report.close_reason, &config.run.early_exit))
        .filter(|report| !reversed_market_slugs.contains(&report.slug))
        .filter_map(|report| {
            let previous_side = PaperOutcomeSide::from_label(&report.dominant_outcome_at_entry);
            let reverse = opportunities.iter().find(|opportunity| {
                opportunity.slug == report.slug
                    && PaperOutcomeSide::from_label(&opportunity.primary_outcome_label)
                        != previous_side
                    && previous_side != PaperOutcomeSide::Unknown
                    && opportunity.seconds_left
                        >= config.run.early_exit.stop_and_reverse_min_seconds_left
            })?;
            let max_notional =
                (report.spent_usdc * config.run.early_exit.stop_and_reverse_size_ratio).round_dp(6);
            scale_opportunity_to_notional(
                reverse,
                max_notional,
                config.strategy.min_top_of_book_shares,
                " | stop-and-reverse",
            )
        })
        .collect()
}

fn is_stop_and_reverse_reason(reason: &str, exit_config: &EarlyExitConfig) -> bool {
    reason.contains("reversal")
        || (exit_config.stop_and_reverse_on_stop_loss && reason.contains("stop-loss"))
}

fn apply_pnl_ratchet_to_opportunity(
    ratchet: &PnlRatchetConfig,
    risk_tracker: &RiskTracker,
    current_total_realized_profit: Decimal,
    opportunity: &Opportunity,
    min_shares: Decimal,
) -> Option<Opportunity> {
    if !ratchet.enabled {
        return Some(opportunity.clone());
    }

    if ratchet.apply_to_codex_sentinel_only && opportunity.kind != OpportunityKind::CodexSentinelV1
    {
        return Some(opportunity.clone());
    }

    let session_realized_profit =
        risk_tracker.session_realized_profit(current_total_realized_profit);
    let protect_after_loss = ratchet.protect_after_consecutive_losses > 0
        && risk_tracker.consecutive_losses >= ratchet.protect_after_consecutive_losses;

    let (target_notional, reason) = if protect_after_loss {
        (ratchet.protect_notional_usdc, "after-loss protection")
    } else if session_realized_profit < Decimal::ZERO {
        (ratchet.protect_notional_usdc, "negative-session protection")
    } else if session_realized_profit < ratchet.profit_unlock_usdc {
        (ratchet.base_notional_usdc, "profit-lock base cap")
    } else {
        return Some(opportunity.clone());
    };

    if opportunity.required_usdc <= target_notional {
        return Some(opportunity.clone());
    }

    let note_suffix = format!(
        " | pnl-ratchet {reason}: cap {} USDC",
        target_notional.round_dp(4)
    );
    scale_opportunity_to_notional(opportunity, target_notional, min_shares, &note_suffix)
}

fn scale_opportunity_to_notional(
    opportunity: &Opportunity,
    target_notional: Decimal,
    min_shares: Decimal,
    note_suffix: &str,
) -> Option<Opportunity> {
    if target_notional <= Decimal::ZERO || opportunity.required_usdc <= Decimal::ZERO {
        return None;
    }

    let scale = (target_notional / opportunity.required_usdc).min(Decimal::ONE);
    if scale <= Decimal::ZERO {
        return None;
    }

    let tradable_shares = (opportunity.tradable_shares * scale).round_dp(6);
    if tradable_shares < min_shares {
        return None;
    }

    let mut scaled = opportunity.clone();
    scaled.tradable_shares = tradable_shares;
    scaled.required_usdc = (opportunity.required_usdc * scale).round_dp(6);
    scaled.expected_payout = (opportunity.expected_payout * scale).round_dp(6);
    scaled.expected_profit = (opportunity.expected_profit * scale).round_dp(6);
    scaled.hedge_shares = (opportunity.hedge_shares * scale).round_dp(6);
    scaled.primary_fill_levels = scale_fill_levels(&opportunity.primary_fill_levels, scale);
    scaled.hedge_fill_levels = scale_fill_levels(&opportunity.hedge_fill_levels, scale);
    scaled.note = format!("{}{note_suffix}", opportunity.note);
    Some(scaled)
}

fn scale_fill_levels(levels: &[BookFillLevel], scale: Decimal) -> Vec<BookFillLevel> {
    levels
        .iter()
        .filter_map(|level| {
            let shares = (level.shares * scale).round_dp(6);
            (shares > Decimal::ZERO).then_some(BookFillLevel {
                price: level.price,
                shares,
            })
        })
        .collect()
}

#[derive(Debug, Clone, Copy, Default)]
struct RiskSeed {
    daily_realized_profit: Decimal,
    consecutive_losses: u32,
}

fn load_risk_seed_from_trades(journal: &JournalStore) -> Result<RiskSeed> {
    let trades = journal.load_paper_trades(None)?;
    let today = Utc::now().date_naive();
    let mut daily_realized_profit = Decimal::ZERO;
    let close_profits = trades
        .iter()
        .filter(|entry| entry.action == PaperTradeAction::Close)
        .filter_map(|entry| {
            entry
                .realized_profit_usdc
                .map(|profit| (entry.recorded_at, profit))
        })
        .collect::<Vec<_>>();

    for (recorded_at, profit) in &close_profits {
        if recorded_at.date_naive() == today {
            daily_realized_profit += *profit;
        }
    }

    let mut consecutive_losses = 0_u32;
    for (_recorded_at, profit) in close_profits.iter().rev() {
        if *profit < Decimal::ZERO {
            consecutive_losses = consecutive_losses.saturating_add(1);
        } else {
            break;
        }
    }

    Ok(RiskSeed {
        daily_realized_profit: daily_realized_profit.round_dp(6),
        consecutive_losses,
    })
}

#[derive(Debug, Clone)]
struct RiskTracker {
    session_start_realized_profit: Decimal,
    daily_realized_profit: Decimal,
    consecutive_losses: u32,
    cooldown_remaining_cycles: usize,
    current_day_utc: chrono::NaiveDate,
    last_block_reason: Option<String>,
}

impl RiskTracker {
    fn new(
        session_start_realized_profit: Decimal,
        daily_realized_profit: Decimal,
        consecutive_losses: u32,
    ) -> Self {
        Self {
            session_start_realized_profit,
            daily_realized_profit,
            consecutive_losses,
            cooldown_remaining_cycles: 0,
            current_day_utc: Utc::now().date_naive(),
            last_block_reason: None,
        }
    }

    fn advance_cycle_cooldown(&mut self) {
        self.refresh_day_boundary();
        if self.cooldown_remaining_cycles > 0 {
            self.cooldown_remaining_cycles -= 1;
            if self.cooldown_remaining_cycles == 0 {
                self.last_block_reason = None;
            }
        }
    }

    fn observe_closed_positions(&mut self, reports: &[PaperCloseReport]) {
        self.refresh_day_boundary();
        for report in reports {
            self.daily_realized_profit += report.realized_profit_usdc;
            if report.realized_profit_usdc < Decimal::ZERO {
                self.consecutive_losses = self.consecutive_losses.saturating_add(1);
            } else if report.realized_profit_usdc > Decimal::ZERO {
                self.consecutive_losses = 0;
            }
        }
        self.daily_realized_profit = self.daily_realized_profit.round_dp(6);
    }

    fn evaluate_and_arm_limits(
        &mut self,
        risk: &RiskControlConfig,
        context: RuntimeRiskContext,
    ) -> Option<String> {
        self.refresh_day_boundary();
        let session_realized_profit =
            (context.total_realized_profit - self.session_start_realized_profit).round_dp(6);
        let session_equity_profit =
            (session_realized_profit + context.unrealized_profit).round_dp(6);
        let daily_equity_profit =
            (self.daily_realized_profit + context.unrealized_profit).round_dp(6);
        let maybe_reason = if context.paper_cash.is_some_and(|cash| cash < Decimal::ZERO) {
            Some(format!(
                "paper cash is negative: {} USDC",
                context.paper_cash.unwrap_or_default().round_dp(4)
            ))
        } else if risk.max_open_notional_usdc > Decimal::ZERO
            && context.open_notional >= risk.max_open_notional_usdc
        {
            Some(format!(
                "open notional limit reached: {} >= {} USDC",
                context.open_notional.round_dp(4),
                risk.max_open_notional_usdc.round_dp(4)
            ))
        } else if risk.max_unrealized_loss_usdc > Decimal::ZERO
            && context.unrealized_profit <= -risk.max_unrealized_loss_usdc
        {
            Some(format!(
                "unrealized loss limit reached: {} <= -{} USDC",
                context.unrealized_profit.round_dp(4),
                risk.max_unrealized_loss_usdc.round_dp(4)
            ))
        } else if risk.max_daily_loss_usdc > Decimal::ZERO
            && daily_equity_profit <= -risk.max_daily_loss_usdc
        {
            Some(format!(
                ": {} <= -{} USDC",
                daily_equity_profit.round_dp(4),
                risk.max_daily_loss_usdc.round_dp(4)
            ))
        } else if risk.max_session_loss_usdc > Decimal::ZERO
            && session_equity_profit <= -risk.max_session_loss_usdc
        {
            Some(format!(
                ": {} <= -{} USDC",
                session_equity_profit.round_dp(4),
                risk.max_session_loss_usdc.round_dp(4)
            ))
        } else if risk.max_consecutive_losses > 0
            && self.consecutive_losses >= risk.max_consecutive_losses
        {
            Some(format!(
                ": {} >= {}",
                self.consecutive_losses, risk.max_consecutive_losses
            ))
        } else {
            None
        };

        if let Some(reason) = maybe_reason
            && self.cooldown_remaining_cycles == 0
        {
            self.cooldown_remaining_cycles = risk.cooldown_cycles.max(1);
            self.last_block_reason = Some(reason.clone());
            return Some(reason);
        }
        None
    }

    fn session_realized_profit(&self, current_total_realized_profit: Decimal) -> Decimal {
        (current_total_realized_profit - self.session_start_realized_profit).round_dp(6)
    }

    const fn is_blocked(&self) -> bool {
        self.cooldown_remaining_cycles > 0
    }

    fn current_block_reason(&self) -> Option<String> {
        if self.is_blocked() {
            self.last_block_reason.clone()
        } else {
            None
        }
    }

    fn refresh_day_boundary(&mut self) {
        let today = Utc::now().date_naive();
        if today != self.current_day_utc {
            self.current_day_utc = today;
            self.daily_realized_profit = Decimal::ZERO;
            self.consecutive_losses = 0;
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct InventoryExposure {
    gross: Decimal,
    delta: Decimal,
    up: Decimal,
    down: Decimal,
}

impl InventoryExposure {
    fn from_side_totals(up_shares: Decimal, down_shares: Decimal) -> Self {
        let gross = (up_shares + down_shares).round_dp(6);
        let delta = (up_shares - down_shares).abs().round_dp(6);
        Self {
            gross,
            delta,
            up: up_shares.round_dp(6),
            down: down_shares.round_dp(6),
        }
    }

    fn add(self, other: Self) -> Self {
        Self::from_side_totals(self.up + other.up, self.down + other.down)
    }
}

#[derive(Debug, Clone)]
struct V4InventoryCooldownState {
    until: DateTime<Utc>,
    trigger_reason: String,
}

#[derive(Debug, Clone, Default)]
#[allow(clippy::struct_field_names)]
struct V4InventoryTracker {
    cooldowns_by_slug: HashMap<String, V4InventoryCooldownState>,
    opened_spent_by_slug: HashMap<String, Decimal>,
    opened_entries_by_slug: HashMap<String, u32>,
}

impl V4InventoryTracker {
    fn prune_expired(&mut self) {
        let now = Utc::now();
        self.cooldowns_by_slug.retain(|_, state| state.until > now);
    }

    fn active_cooldown_reason(&mut self, slug: &str) -> Option<String> {
        self.prune_expired();
        let state = self.cooldowns_by_slug.get(slug)?;
        Some(format!(
            "v4 inventory cooldown active until {} ({})",
            state.until.to_rfc3339(),
            state.trigger_reason
        ))
    }

    fn observe_closed_positions(
        &mut self,
        config: &V4InventoryConfig,
        reports: &[PaperCloseReport],
    ) {
        if !config.enabled || config.cooldown_secs <= 0 {
            return;
        }

        self.prune_expired();
        for report in reports {
            let close_reason = report.close_reason.to_ascii_lowercase();
            let should_cooldown = (config.cooldown_on_partial_reversal
                && close_reason.contains("partial-reversal"))
                || (config.cooldown_on_stop_loss && close_reason.contains("stop-loss"))
                || (config.cooldown_on_reversal
                    && close_reason.contains("reversal")
                    && !close_reason.contains("partial-reversal"));
            if !should_cooldown {
                continue;
            }

            let until = report.closed_at + ChronoDuration::seconds(config.cooldown_secs.max(1));
            self.cooldowns_by_slug.insert(
                report.slug.clone(),
                V4InventoryCooldownState {
                    until,
                    trigger_reason: classify_close_reason(&report.close_reason).to_owned(),
                },
            );
        }
    }

    fn observe_opened_opportunity(&mut self, opportunity: &Opportunity) {
        let spent = self
            .opened_spent_by_slug
            .entry(opportunity.slug.clone())
            .or_default();
        *spent = (*spent + opportunity.required_usdc).round_dp(6);

        let entries = self
            .opened_entries_by_slug
            .entry(opportunity.slug.clone())
            .or_default();
        *entries = entries.saturating_add(1);
    }

    fn opened_spent_for_slug(&self, slug: &str) -> Decimal {
        self.opened_spent_by_slug
            .get(slug)
            .copied()
            .unwrap_or(Decimal::ZERO)
            .round_dp(6)
    }

    fn opened_entries_for_slug(&self, slug: &str) -> u32 {
        self.opened_entries_by_slug.get(slug).copied().unwrap_or(0)
    }
}

fn should_apply_risk_limits(mode: BotMode, risk: &RiskControlConfig) -> bool {
    let limits_enabled = risk.max_daily_loss_usdc > Decimal::ZERO
        || risk.max_session_loss_usdc > Decimal::ZERO
        || risk.max_open_notional_usdc > Decimal::ZERO
        || risk.max_unrealized_loss_usdc > Decimal::ZERO
        || risk.max_consecutive_losses > 0;
    if !limits_enabled {
        return false;
    }

    match mode {
        BotMode::Paper => true,
        BotMode::Live => risk.apply_in_live_mode,
    }
}

async fn auth_check(config: &AppConfig, data_client: &MarketDataClient) -> Result<()> {
    let geo = data_client.geoblock_status().await?;
    if geo.blocked {
        return Err(AppError::Geoblocked {
            country: geo.country,
            region: geo.region,
        });
    }

    let report = LiveExecutor::auth_check(&config.http.clob_base_url, &config.live).await?;
    log_auth_check(&report);
    Ok(())
}

async fn follow_wallet_activity(
    data_client: &MarketDataClient,
    wallet: &str,
    limit: usize,
    refresh_secs: u64,
    btc_only: bool,
    cycles: Option<usize>,
) -> Result<()> {
    let poll_limit = limit.max(1);
    let poll_interval_secs = refresh_secs.max(1);
    let poll_interval = Duration::from_secs(poll_interval_secs);
    let mut cycle_index = 0_usize;
    let mut seen = HashSet::<String>::new();

    info!(
        wallet,
        poll_limit,
        poll_interval_secs,
        btc_only,
        cycles = ?cycles,
        "Polymarket"
    );

    loop {
        cycle_index += 1;
        let mut entries = data_client
            .fetch_profile_activity(wallet, poll_limit, 0)
            .await?;
        entries.sort_by(|left, right| {
            left.timestamp
                .cmp(&right.timestamp)
                .then_with(|| left.dedupe_key().cmp(&right.dedupe_key()))
        });
        if btc_only {
            entries.retain(ProfileActivityRecord::is_btc_5m);
        }

        if cycle_index == 1 {
            if entries.is_empty() {
                info!(wallet, "wallet activity monitor found no initial entries");
            } else {
                let snapshot = &entries[entries.len().saturating_sub(12)..];
                for entry in snapshot {
                    info!("{}", render_wallet_activity("snapshot", entry));
                }
                for entry in entries {
                    seen.insert(entry.dedupe_key());
                }
            }
        } else {
            let mut new_entries = Vec::new();
            for entry in entries {
                if seen.insert(entry.dedupe_key()) {
                    new_entries.push(entry);
                }
            }

            if new_entries.is_empty() {
                info!(
                    cycle = cycle_index,
                    "wallet activity monitor found no new entries"
                );
            } else {
                info!(
                    cycle = cycle_index,
                    new_entries = new_entries.len(),
                    "wallet activity monitor found new entries"
                );
                for entry in &new_entries {
                    info!("{}", render_wallet_activity("new", entry));
                }
            }
        }

        if let Some(max_cycles) = cycles
            && cycle_index >= max_cycles
        {
            info!(
                cycle = cycle_index,
                "wallet activity monitor reached cycle limit"
            );
            break;
        }

        sleep(poll_interval).await;
    }

    Ok(())
}

#[derive(Debug, Clone, Default)]
struct WalletActivitySlugProgressState {
    trade_index: u64,
    cumulative_usdc: Decimal,
    last_trade_timestamp: Option<i64>,
    net_up_shares: Decimal,
    net_down_shares: Decimal,
}

#[allow(clippy::too_many_lines)]
async fn follow_wallet_activity_recorded(
    config: &AppConfig,
    data_client: &MarketDataClient,
    binance_client: &BinanceClient,
    options: FollowWalletRecordOptions<'_>,
) -> Result<()> {
    let output_path = options.output.map_or_else(
        || {
            config
                .storage
                .state_dir
                .join(DEFAULT_WALLET_ACTIVITY_RECORD_FILENAME)
        },
        Path::to_path_buf,
    );
    ensure_parent_dir(&output_path)?;

    let poll_limit = options.limit.max(1);
    let poll_interval_secs = options.refresh_secs.max(1);
    let poll_interval = Duration::from_secs(poll_interval_secs);
    let mut cycle_index = 0_usize;
    let mut seen = HashSet::<String>::new();
    let existing_records = load_wallet_activity_records(&output_path, None)?;
    let mut slug_progress = seed_wallet_progress_states(&existing_records);

    info!(
        wallet = options.wallet,
        poll_limit,
        poll_interval_secs,
        btc_only = options.btc_only,
        output = %output_path.display(),
        cycles = ?options.cycles,
        "Polymarket"
    );

    loop {
        cycle_index += 1;
        let mut entries = data_client
            .fetch_profile_activity(options.wallet, poll_limit, 0)
            .await?;
        entries.sort_by(|left, right| {
            left.timestamp
                .cmp(&right.timestamp)
                .then_with(|| left.dedupe_key().cmp(&right.dedupe_key()))
        });
        if options.btc_only {
            entries.retain(ProfileActivityRecord::is_btc_5m);
        }

        let mut cycle_records = Vec::new();
        if cycle_index == 1 {
            let snapshot = &entries[entries.len().saturating_sub(12)..];
            for entry in snapshot {
                let mut record = capture_wallet_activity_snapshot(
                    data_client,
                    binance_client,
                    options.wallet,
                    "snapshot",
                    entry,
                )
                .await?;
                apply_wallet_progress_metrics(&mut record, &mut slug_progress);
                info!("{}", render_wallet_activity_snapshot(&record));
                cycle_records.push(record);
            }
            for entry in entries {
                seen.insert(entry.dedupe_key());
            }
        } else {
            let mut new_entries = Vec::new();
            for entry in entries {
                if seen.insert(entry.dedupe_key()) {
                    new_entries.push(entry);
                }
            }

            if new_entries.is_empty() {
                info!(
                    cycle = cycle_index,
                    "wallet snapshot monitor found no new entries"
                );
            } else {
                info!(
                    cycle = cycle_index,
                    new_entries = new_entries.len(),
                    "wallet snapshot monitor found new entries"
                );
                for entry in &new_entries {
                    let mut record = capture_wallet_activity_snapshot(
                        data_client,
                        binance_client,
                        options.wallet,
                        "new",
                        entry,
                    )
                    .await?;
                    apply_wallet_progress_metrics(&mut record, &mut slug_progress);
                    info!("{}", render_wallet_activity_snapshot(&record));
                    cycle_records.push(record);
                }
            }
        }

        for record in &cycle_records {
            append_wallet_activity_snapshot(&output_path, record)?;
            append_wallet_activity_snapshot_csv(&output_path, record)?;
        }

        if let Some(max_cycles) = options.cycles
            && cycle_index >= max_cycles
        {
            info!(
                cycle = cycle_index,
                "wallet snapshot monitor reached cycle limit"
            );
            break;
        }

        sleep(poll_interval).await;
    }

    Ok(())
}

fn default_wallet_activity_record_path(config: &AppConfig) -> std::path::PathBuf {
    config
        .storage
        .state_dir
        .join(DEFAULT_WALLET_ACTIVITY_RECORD_FILENAME)
}

#[allow(clippy::too_many_lines)]
async fn capture_wallet_activity_snapshot(
    data_client: &MarketDataClient,
    binance_client: &BinanceClient,
    wallet: &str,
    label: &str,
    entry: &ProfileActivityRecord,
) -> Result<WalletActivitySnapshotRecord> {
    let now = Utc::now();
    let mut record = WalletActivitySnapshotRecord {
        recorded_at: now,
        label: label.to_owned(),
        wallet: wallet.to_owned(),
        activity_timestamp: entry.timestamp,
        activity_type: entry.activity_type.clone(),
        slug: entry.slug.clone(),
        question: None,
        market_target: None,
        window_start_ts: None,
        window_secs: None,
        minutes_window: None,
        seconds_since_window_start: None,
        window_progress_pct: None,
        seconds_left_at_observed: None,
        side: entry.side.clone(),
        outcome: entry.outcome.clone(),
        activity_price: entry.price,
        usdc_size: entry.usdc_size,
        transaction_hash: entry.transaction_hash.clone(),
        binance_symbol: None,
        binance_price: None,
        target_price: None,
        target_price_source: None,
        polymarket_final_reference_price: None,
        target_gap_bps: None,
        dominant_outcome: None,
        spot_move_bps: None,
        spot_move_1s_bps: None,
        spot_move_5s_bps: None,
        spot_move_15s_bps: None,
        micro_acceleration_bps: None,
        up_ask: None,
        up_bid: None,
        down_ask: None,
        down_bid: None,
        bundle_cost: None,
        selected_outcome_ask: None,
        selected_outcome_bid: None,
        selected_outcome_mid: None,
        opposite_outcome_ask: None,
        opposite_outcome_bid: None,
        opposite_outcome_mid: None,
        implied_up_mid: None,
        implied_down_mid: None,
        up_display_price_estimate: None,
        down_display_price_estimate: None,
        selected_outcome_display_price_estimate: None,
        selected_trade_discount_to_ask_bps: None,
        selected_trade_discount_to_mid_bps: None,
        selected_outcome_spread_bps: None,
        selected_vs_opposite_mid_bps: None,
        selected_share_of_bundle_pct: None,
        trade_index_in_slug: None,
        seconds_since_previous_trade_same_slug: None,
        usdc_cumulative_same_slug: None,
        dominant_outcome_after_trade: None,
        selected_trade_vs_display_bps: None,
    };

    let market = data_client
        .fetch_supported_market_by_slug(&entry.slug)
        .await?
        .or(data_client
            .fetch_historical_market_by_slug(&entry.slug)
            .await?);
    let Some(market) = market else {
        return Ok(record);
    };

    record.question = Some(market.question.clone());
    record.market_target = market.target().map(|target| target.as_key().to_owned());
    record.window_start_ts = market.window_start_ts();
    record.window_secs = market.window_secs();
    record.minutes_window = market.window_secs().map(|secs| secs / 60);
    record.seconds_since_window_start = market
        .window_start_ts()
        .map(|start_ts| now.timestamp().saturating_sub(start_ts).max(0));
    record.window_progress_pct = match (record.seconds_since_window_start, market.window_secs()) {
        (Some(elapsed), Some(window_secs)) if window_secs > 0 => Some(
            (Decimal::from(elapsed.min(window_secs)) / Decimal::from(window_secs)
                * Decimal::from(100_u32))
            .round_dp(4),
        ),
        _ => None,
    };
    record.polymarket_final_reference_price = market.final_reference_price;
    record.seconds_left_at_observed = match (market.window_start_ts(), market.window_secs()) {
        (Some(start_ts), Some(window_secs)) => Some(
            (start_ts + window_secs)
                .saturating_sub(now.timestamp())
                .max(0),
        ),
        _ => None,
    };

    if let Some(target) = market.target() {
        record.binance_symbol = Some(target.binance_symbol().to_owned());
        let _ = binance_client.start_trade_stream(target.binance_symbol());
    }

    data_client.ensure_market_stream_started();
    data_client
        .register_live_markets(std::slice::from_ref(&market))
        .await;

    let token_ids = vec![
        market.outcome_a_token_id.clone(),
        market.outcome_b_token_id.clone(),
    ];
    let books = data_client
        .fetch_order_books_live_first(&token_ids, 1_500)
        .await?;
    let up_book = market
        .token_for_outcome("up")
        .and_then(|token_id| books.get(token_id));
    let down_book = market
        .token_for_outcome("down")
        .and_then(|token_id| books.get(token_id));

    record.up_ask = up_book.and_then(best_ask_price);
    record.up_bid = up_book.and_then(best_bid_price);
    record.down_ask = down_book.and_then(best_ask_price);
    record.down_bid = down_book.and_then(best_bid_price);
    record.bundle_cost = match (record.up_ask, record.down_ask) {
        (Some(up_ask), Some(down_ask)) => Some((up_ask + down_ask).round_dp(6)),
        _ => None,
    };
    record.implied_up_mid = midpoint_price(record.up_bid, record.up_ask);
    record.implied_down_mid = midpoint_price(record.down_bid, record.down_ask);
    record.up_display_price_estimate = display_price_estimate(
        record.up_bid,
        record.up_ask,
        if outcome_side_is_up(&record.outcome) {
            record.activity_price
        } else {
            None
        },
    );
    record.down_display_price_estimate = display_price_estimate(
        record.down_bid,
        record.down_ask,
        if outcome_side_is_down(&record.outcome) {
            record.activity_price
        } else {
            None
        },
    );
    record.selected_outcome_ask = selected_outcome_ask(&record);
    record.selected_outcome_bid = selected_outcome_bid(&record);
    record.selected_outcome_mid =
        midpoint_price(record.selected_outcome_bid, record.selected_outcome_ask);
    record.opposite_outcome_ask = opposite_outcome_ask(&record);
    record.opposite_outcome_bid = opposite_outcome_bid(&record);
    record.opposite_outcome_mid =
        midpoint_price(record.opposite_outcome_bid, record.opposite_outcome_ask);
    record.selected_outcome_display_price_estimate = if outcome_side_is_up(&record.outcome) {
        record.up_display_price_estimate
    } else if outcome_side_is_down(&record.outcome) {
        record.down_display_price_estimate
    } else {
        None
    };
    record.selected_trade_discount_to_ask_bps =
        match (record.activity_price, record.selected_outcome_ask) {
            (Some(activity_price), Some(selected_ask)) if selected_ask > Decimal::ZERO => Some(
                ((selected_ask - activity_price) / selected_ask * Decimal::from(10_000_u32))
                    .round_dp(4),
            ),
            _ => None,
        };
    record.selected_trade_discount_to_mid_bps =
        match (record.activity_price, record.selected_outcome_mid) {
            (Some(activity_price), Some(selected_mid)) if selected_mid > Decimal::ZERO => Some(
                ((selected_mid - activity_price) / selected_mid * Decimal::from(10_000_u32))
                    .round_dp(4),
            ),
            _ => None,
        };
    record.selected_outcome_spread_bps = match (
        record.selected_outcome_bid,
        record.selected_outcome_ask,
        record.selected_outcome_mid,
    ) {
        (Some(selected_bid), Some(selected_ask), Some(selected_mid_price))
            if selected_mid_price > Decimal::ZERO =>
        {
            Some(
                ((selected_ask - selected_bid) / selected_mid_price * Decimal::from(10_000_u32))
                    .round_dp(4),
            )
        }
        _ => None,
    };
    record.selected_vs_opposite_mid_bps = match (
        record.selected_outcome_mid,
        record.opposite_outcome_mid,
        record.selected_outcome_display_price_estimate,
    ) {
        (Some(selected_mid), Some(opposite_mid), Some(display_price))
            if display_price > Decimal::ZERO =>
        {
            Some(
                ((selected_mid - opposite_mid) / display_price * Decimal::from(10_000_u32))
                    .round_dp(4),
            )
        }
        _ => None,
    };
    record.selected_share_of_bundle_pct = match (record.selected_outcome_ask, record.bundle_cost) {
        (Some(selected_ask), Some(bundle_cost)) if bundle_cost > Decimal::ZERO => {
            Some((selected_ask / bundle_cost * Decimal::from(100_u32)).round_dp(4))
        }
        _ => None,
    };

    let now_ts = now.timestamp();
    let live_context = if market
        .window_start_ts()
        .zip(market.window_secs())
        .is_some_and(|(start_ts, window_secs)| {
            entry.timestamp >= now_ts.saturating_sub(5) && now_ts < start_ts + window_secs
        }) {
        binance_client.market_context(&market).await?
    } else {
        None
    };
    let historical_context = if live_context.is_none() {
        binance_client
            .context_for_slug_at_timestamp(&entry.slug, entry.timestamp)
            .await?
    } else {
        None
    };

    if let Some(context) = live_context.or(historical_context) {
        record.binance_price = Some(context.current_spot_price);
        record.target_price = Some(context.target_price);
        record.target_price_source = Some(context.target_price_source.as_str().to_owned());
        record.target_gap_bps = Some(context.target_gap_bps);
        record.dominant_outcome = Some(context.dominant_outcome);
        record.spot_move_bps = Some(context.spot_move_bps);
        record.spot_move_1s_bps = Some(context.spot_move_1s_bps);
        record.spot_move_5s_bps = Some(context.spot_move_5s_bps);
        record.spot_move_15s_bps = Some(context.spot_move_15s_bps);
        record.micro_acceleration_bps = Some(context.micro_acceleration_bps);
        record.seconds_left_at_observed = Some(context.seconds_left.max(0));
    }

    Ok(record)
}

fn ensure_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)?;
    }
    Ok(())
}

fn append_wallet_activity_snapshot(
    path: &Path,
    record: &WalletActivitySnapshotRecord,
) -> Result<()> {
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    serde_json::to_writer(&mut file, record)?;
    file.write_all(b"\n")?;
    Ok(())
}

fn append_wallet_activity_snapshot_csv(
    jsonl_path: &Path,
    record: &WalletActivitySnapshotRecord,
) -> Result<()> {
    let csv_path = jsonl_path.with_extension("csv");
    let file_exists = csv_path.exists();
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(csv_path)?;
    if !file_exists {
        file.write_all(wallet_activity_csv_header().as_bytes())?;
        file.write_all(b"\n")?;
    }
    file.write_all(wallet_activity_csv_row(record).as_bytes())?;
    file.write_all(b"\n")?;
    Ok(())
}

fn seed_wallet_progress_states(
    records: &[WalletActivitySnapshotRecord],
) -> HashMap<String, WalletActivitySlugProgressState> {
    let mut states = HashMap::new();
    let mut seeded_records = records.to_vec();
    seeded_records.sort_by(|left, right| {
        left.activity_timestamp
            .cmp(&right.activity_timestamp)
            .then_with(|| left.recorded_at.cmp(&right.recorded_at))
            .then_with(|| left.transaction_hash.cmp(&right.transaction_hash))
    });
    for record in &mut seeded_records {
        apply_wallet_progress_metrics(record, &mut states);
    }
    states
}

fn apply_wallet_progress_metrics(
    record: &mut WalletActivitySnapshotRecord,
    states: &mut HashMap<String, WalletActivitySlugProgressState>,
) {
    let state = states.entry(record.slug.clone()).or_default();
    state.trade_index = state.trade_index.saturating_add(1);
    record.trade_index_in_slug = Some(state.trade_index);
    record.seconds_since_previous_trade_same_slug = state
        .last_trade_timestamp
        .map(|previous_ts| (record.activity_timestamp - previous_ts).max(0));

    if let Some(usdc_size) = record.usdc_size {
        state.cumulative_usdc += usdc_size;
    }
    record.usdc_cumulative_same_slug = Some(state.cumulative_usdc.round_dp(6));

    let shares = inferred_shares(record).unwrap_or(Decimal::ZERO);
    let is_buy = wallet_side_is_buy(&record.side);
    let is_sell = wallet_side_is_sell(&record.side);

    if outcome_side_is_up(&record.outcome) {
        if is_sell {
            state.net_up_shares -= shares;
        } else if is_buy {
            state.net_up_shares += shares;
        }
    } else if outcome_side_is_down(&record.outcome) {
        if is_sell {
            state.net_down_shares -= shares;
        } else if is_buy {
            state.net_down_shares += shares;
        }
    }

    let dominant = dominant_outcome_from_net_shares(state.net_up_shares, state.net_down_shares);
    record.dominant_outcome_after_trade = if dominant.is_empty() {
        None
    } else {
        Some(dominant)
    };
    record.selected_trade_vs_display_bps = match (
        record.activity_price,
        record.selected_outcome_display_price_estimate,
    ) {
        (Some(activity_price), Some(display_price)) if display_price > Decimal::ZERO => Some(
            ((display_price - activity_price) / display_price * Decimal::from(10_000_u32))
                .round_dp(4),
        ),
        _ => None,
    };

    state.last_trade_timestamp = Some(record.activity_timestamp);
}

fn wallet_activity_csv_header() -> &'static str {
    "recorded_at,label,wallet,activity_timestamp,activity_type,slug,question,market_target,window_start_ts,window_secs,minutes_window,seconds_since_window_start,window_progress_pct,seconds_left_at_observed,side,outcome,activity_price,usdc_size,transaction_hash,binance_symbol,binance_price,target_price,target_price_source,polymarket_final_reference_price,target_gap_bps,dominant_outcome,spot_move_bps,spot_move_1s_bps,spot_move_5s_bps,spot_move_15s_bps,micro_acceleration_bps,up_ask,up_bid,down_ask,down_bid,bundle_cost,selected_outcome_ask,selected_outcome_bid,selected_outcome_mid,opposite_outcome_ask,opposite_outcome_bid,opposite_outcome_mid,implied_up_mid,implied_down_mid,up_display_price_estimate,down_display_price_estimate,selected_outcome_display_price_estimate,selected_trade_discount_to_ask_bps,selected_trade_discount_to_mid_bps,selected_outcome_spread_bps,selected_vs_opposite_mid_bps,selected_share_of_bundle_pct,trade_index_in_slug,seconds_since_previous_trade_same_slug,usdc_cumulative_same_slug,dominant_outcome_after_trade,selected_trade_vs_display_bps"
}

fn wallet_activity_csv_row(record: &WalletActivitySnapshotRecord) -> String {
    [
        csv_cell(record.recorded_at.to_rfc3339()),
        csv_cell(&record.label),
        csv_cell(&record.wallet),
        csv_cell(record.activity_timestamp.to_string()),
        csv_cell(&record.activity_type),
        csv_cell(&record.slug),
        csv_cell(record.question.as_deref().unwrap_or("")),
        csv_cell(record.market_target.as_deref().unwrap_or("")),
        csv_cell(option_i64_to_string(record.window_start_ts)),
        csv_cell(option_i64_to_string(record.window_secs)),
        csv_cell(option_i64_to_string(record.minutes_window)),
        csv_cell(option_i64_to_string(record.seconds_since_window_start)),
        csv_cell(option_decimal_to_string(record.window_progress_pct)),
        csv_cell(option_i64_to_string(record.seconds_left_at_observed)),
        csv_cell(&record.side),
        csv_cell(&record.outcome),
        csv_cell(option_decimal_to_string(record.activity_price)),
        csv_cell(option_decimal_to_string(record.usdc_size)),
        csv_cell(&record.transaction_hash),
        csv_cell(record.binance_symbol.as_deref().unwrap_or("")),
        csv_cell(option_decimal_to_string(record.binance_price)),
        csv_cell(option_decimal_to_string(record.target_price)),
        csv_cell(record.target_price_source.as_deref().unwrap_or("")),
        csv_cell(option_decimal_to_string(
            record.polymarket_final_reference_price,
        )),
        csv_cell(option_decimal_to_string(record.target_gap_bps)),
        csv_cell(record.dominant_outcome.as_deref().unwrap_or("")),
        csv_cell(option_decimal_to_string(record.spot_move_bps)),
        csv_cell(option_decimal_to_string(record.spot_move_1s_bps)),
        csv_cell(option_decimal_to_string(record.spot_move_5s_bps)),
        csv_cell(option_decimal_to_string(record.spot_move_15s_bps)),
        csv_cell(option_decimal_to_string(record.micro_acceleration_bps)),
        csv_cell(option_decimal_to_string(record.up_ask)),
        csv_cell(option_decimal_to_string(record.up_bid)),
        csv_cell(option_decimal_to_string(record.down_ask)),
        csv_cell(option_decimal_to_string(record.down_bid)),
        csv_cell(option_decimal_to_string(record.bundle_cost)),
        csv_cell(option_decimal_to_string(record.selected_outcome_ask)),
        csv_cell(option_decimal_to_string(record.selected_outcome_bid)),
        csv_cell(option_decimal_to_string(record.selected_outcome_mid)),
        csv_cell(option_decimal_to_string(record.opposite_outcome_ask)),
        csv_cell(option_decimal_to_string(record.opposite_outcome_bid)),
        csv_cell(option_decimal_to_string(record.opposite_outcome_mid)),
        csv_cell(option_decimal_to_string(record.implied_up_mid)),
        csv_cell(option_decimal_to_string(record.implied_down_mid)),
        csv_cell(option_decimal_to_string(record.up_display_price_estimate)),
        csv_cell(option_decimal_to_string(record.down_display_price_estimate)),
        csv_cell(option_decimal_to_string(
            record.selected_outcome_display_price_estimate,
        )),
        csv_cell(option_decimal_to_string(
            record.selected_trade_discount_to_ask_bps,
        )),
        csv_cell(option_decimal_to_string(
            record.selected_trade_discount_to_mid_bps,
        )),
        csv_cell(option_decimal_to_string(record.selected_outcome_spread_bps)),
        csv_cell(option_decimal_to_string(
            record.selected_vs_opposite_mid_bps,
        )),
        csv_cell(option_decimal_to_string(
            record.selected_share_of_bundle_pct,
        )),
        csv_cell(option_u64_to_string(record.trade_index_in_slug)),
        csv_cell(option_i64_to_string(
            record.seconds_since_previous_trade_same_slug,
        )),
        csv_cell(option_decimal_to_string(record.usdc_cumulative_same_slug)),
        csv_cell(record.dominant_outcome_after_trade.as_deref().unwrap_or("")),
        csv_cell(option_decimal_to_string(
            record.selected_trade_vs_display_bps,
        )),
    ]
    .join(",")
}

fn csv_cell(value: impl AsRef<str>) -> String {
    let normalized = sanitize_legacy_mojibake(value.as_ref()).replace(['\r', '\n'], " ");
    let escaped = normalized.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

fn option_decimal_to_string(value: Option<Decimal>) -> String {
    value.map_or_else(String::new, |decimal| decimal.round_dp(6).to_string())
}

fn option_i64_to_string(value: Option<i64>) -> String {
    value.map_or_else(String::new, |number| number.to_string())
}

fn option_u64_to_string(value: Option<u64>) -> String {
    value.map_or_else(String::new, |number| number.to_string())
}

fn show_wallet_activity_report(
    config: &AppConfig,
    input: Option<&Path>,
    limit: Option<usize>,
    top: usize,
) -> Result<()> {
    let path = input.map_or_else(
        || default_wallet_activity_record_path(config),
        Path::to_path_buf,
    );
    show_wallet_activity_report_path(&path, limit, top)
}

fn show_wallet_activity_report_path(path: &Path, limit: Option<usize>, top: usize) -> Result<()> {
    let records = load_wallet_activity_records(path, limit)?;

    if records.is_empty() {
        info!(path = %path.display(), "wallet activity report has no records");
        return Ok(());
    }

    let top = top.max(1);
    info!(
        path = %path.display(),
        records = records.len(),
        "enriched wallet activity"
    );
    info!("\n{}", render_wallet_activity_summary(&records, top));
    Ok(())
}

#[derive(Default)]
struct WalletActivityAggregate {
    trades: usize,
    total_usdc: Decimal,
}

#[derive(Default)]
struct WalletActivityMarketAggregate {
    trades: usize,
    total_usdc: Decimal,
    sum_activity_price: Decimal,
    activity_price_count: usize,
    sum_binance_price: Decimal,
    binance_price_count: usize,
    sum_up_ask: Decimal,
    up_ask_count: usize,
    sum_down_ask: Decimal,
    down_ask_count: usize,
}

#[derive(Default)]
struct WalletActivityPatternAggregate {
    trades: usize,
    total_usdc: Decimal,
    selected_ask_sum: Decimal,
    selected_ask_count: usize,
    aux_sum: Decimal,
    aux_count: usize,
}

#[derive(Debug, Clone)]
struct WalletActivitySlugStreak {
    slug: String,
    outcome: String,
    trades: usize,
    started_at: i64,
    ended_at: i64,
    total_usdc: Decimal,
}

#[derive(Debug, Clone)]
struct WalletActivityPositionSummary {
    slug: String,
    started_at: i64,
    ended_at: i64,
    trades: usize,
    buy_trades: usize,
    sell_trades: usize,
    gross_usdc: Decimal,
    avg_usdc_per_trade: Decimal,
    duration_secs: i64,
    avg_seconds_between_trades: Option<Decimal>,
    up_shares_net: Decimal,
    down_shares_net: Decimal,
    dominant_end_outcome: String,
    dominant_switches: usize,
    first_target_gap_bps: Option<Decimal>,
    last_target_gap_bps: Option<Decimal>,
    max_abs_target_gap_bps: Option<Decimal>,
    first_seconds_left: Option<i64>,
    last_seconds_left: Option<i64>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum WalletExecutionHeuristic {
    MakerLike,
    Neutral,
    CrossedOrStale,
    Unknown,
}

#[derive(Debug, Clone, Default)]
struct WalletBonereaperForensics {
    total_windows: usize,
    two_sided_windows: usize,
    one_sided_windows: usize,
    gross_inventory_shares: Decimal,
    net_directional_delta_shares: Decimal,
    balanced_windows: usize,
    maker_like_trades: usize,
    neutral_execution_trades: usize,
    crossed_or_stale_trades: usize,
    unknown_execution_trades: usize,
    micro_window_trades: usize,
    micro_window_usdc: Decimal,
    btc_trades: usize,
    eth_trades: usize,
}

#[allow(clippy::too_many_lines)]
fn render_wallet_activity_summary(records: &[WalletActivitySnapshotRecord], top: usize) -> String {
    let mut output = String::new();
    let first_ts = records.first().map_or_else(
        || "-".to_owned(),
        |record| format_unix_secs_local(record.activity_timestamp),
    );
    let last_ts = records.last().map_or_else(
        || "-".to_owned(),
        |record| format_unix_secs_local(record.activity_timestamp),
    );

    let total_usdc = records
        .iter()
        .filter_map(|record| record.usdc_size)
        .fold(Decimal::ZERO, |sum, value| sum + value);
    let unique_slugs = records
        .iter()
        .map(|record| record.slug.as_str())
        .collect::<HashSet<_>>()
        .len();

    let _ = writeln!(output, ": {}", records.len());
    let _ = writeln!(output, ": {first_ts} -> {last_ts}");
    let _ = writeln!(output, ": {unique_slugs}");
    let _ = writeln!(output, "USDC: {}", total_usdc.round_dp(4));

    let mut by_side = BTreeMap::<String, WalletActivityAggregate>::new();
    let mut by_activity_type = BTreeMap::<String, WalletActivityAggregate>::new();
    let mut by_outcome = BTreeMap::<String, WalletActivityAggregate>::new();
    let mut by_minutes = BTreeMap::<i64, WalletActivityAggregate>::new();
    let mut by_seconds_left_bucket = BTreeMap::<String, WalletActivityPatternAggregate>::new();
    let mut by_target_gap_bucket = BTreeMap::<String, WalletActivityPatternAggregate>::new();
    let mut by_selected_ask_bucket = BTreeMap::<String, WalletActivityPatternAggregate>::new();
    let mut by_target_alignment = BTreeMap::<String, WalletActivityPatternAggregate>::new();
    let mut by_progress_bucket = BTreeMap::<String, WalletActivityPatternAggregate>::new();
    let mut by_discount_bucket = BTreeMap::<String, WalletActivityPatternAggregate>::new();
    let mut by_spread_bucket = BTreeMap::<String, WalletActivityPatternAggregate>::new();
    let mut by_trade_size_bucket = BTreeMap::<String, WalletActivityPatternAggregate>::new();
    let mut by_slug = HashMap::<String, WalletActivityMarketAggregate>::new();

    for record in records {
        let side_key = if record.side.is_empty() {
            "-".to_owned()
        } else {
            record.side.clone()
        };
        let outcome_key = if record.outcome.is_empty() {
            "-".to_owned()
        } else {
            record.outcome.clone()
        };
        let activity_type_key = if record.activity_type.is_empty() {
            "-".to_owned()
        } else {
            record.activity_type.clone()
        };
        let usdc = record.usdc_size.unwrap_or(Decimal::ZERO);

        let side_entry = by_side.entry(side_key).or_default();
        side_entry.trades += 1;
        side_entry.total_usdc += usdc;

        let activity_type_entry = by_activity_type.entry(activity_type_key).or_default();
        activity_type_entry.trades += 1;
        activity_type_entry.total_usdc += usdc;

        let outcome_entry = by_outcome.entry(outcome_key).or_default();
        outcome_entry.trades += 1;
        outcome_entry.total_usdc += usdc;

        if let Some(minutes) = record.minutes_window {
            let minute_entry = by_minutes.entry(minutes).or_default();
            minute_entry.trades += 1;
            minute_entry.total_usdc += usdc;
        }

        let selected_ask = selected_outcome_ask(record);
        let seconds_bucket = seconds_left_bucket_label(record.seconds_left_at_observed);
        let seconds_entry = by_seconds_left_bucket.entry(seconds_bucket).or_default();
        seconds_entry.trades += 1;
        seconds_entry.total_usdc += usdc;
        if let Some(selected_ask) = selected_ask {
            seconds_entry.selected_ask_sum += selected_ask;
            seconds_entry.selected_ask_count += 1;
        }

        let gap_bucket = target_gap_bucket_label(record.target_gap_bps);
        let gap_entry = by_target_gap_bucket.entry(gap_bucket).or_default();
        gap_entry.trades += 1;
        gap_entry.total_usdc += usdc;
        if let Some(selected_ask) = selected_ask {
            gap_entry.selected_ask_sum += selected_ask;
            gap_entry.selected_ask_count += 1;
        }

        let ask_bucket = selected_ask_bucket_label(selected_ask);
        let ask_entry = by_selected_ask_bucket.entry(ask_bucket).or_default();
        ask_entry.trades += 1;
        ask_entry.total_usdc += usdc;
        if let Some(selected_ask) = selected_ask {
            ask_entry.selected_ask_sum += selected_ask;
            ask_entry.selected_ask_count += 1;
        }

        let alignment_bucket = target_alignment_label(record);
        let alignment_entry = by_target_alignment.entry(alignment_bucket).or_default();
        alignment_entry.trades += 1;
        alignment_entry.total_usdc += usdc;
        if let Some(selected_ask) = selected_ask {
            alignment_entry.selected_ask_sum += selected_ask;
            alignment_entry.selected_ask_count += 1;
        }

        let progress_bucket = window_progress_bucket_label(record.window_progress_pct);
        let progress_entry = by_progress_bucket.entry(progress_bucket).or_default();
        progress_entry.trades += 1;
        progress_entry.total_usdc += usdc;
        if let Some(progress) = record.window_progress_pct {
            progress_entry.aux_sum += progress;
            progress_entry.aux_count += 1;
        }

        let discount_bucket = discount_bucket_label(record.selected_trade_discount_to_ask_bps);
        let discount_entry = by_discount_bucket.entry(discount_bucket).or_default();
        discount_entry.trades += 1;
        discount_entry.total_usdc += usdc;
        if let Some(discount) = record.selected_trade_discount_to_ask_bps {
            discount_entry.aux_sum += discount;
            discount_entry.aux_count += 1;
        }

        let spread_bucket = spread_bucket_label(record.selected_outcome_spread_bps);
        let spread_entry = by_spread_bucket.entry(spread_bucket).or_default();
        spread_entry.trades += 1;
        spread_entry.total_usdc += usdc;
        if let Some(spread) = record.selected_outcome_spread_bps {
            spread_entry.aux_sum += spread;
            spread_entry.aux_count += 1;
        }

        let trade_size_bucket = trade_size_bucket_label(record.usdc_size);
        let trade_size_aggregate = by_trade_size_bucket.entry(trade_size_bucket).or_default();
        trade_size_aggregate.trades += 1;
        trade_size_aggregate.total_usdc += usdc;
        if let Some(mid_gap) = record.selected_vs_opposite_mid_bps {
            trade_size_aggregate.aux_sum += mid_gap;
            trade_size_aggregate.aux_count += 1;
        }

        let market_entry = by_slug.entry(record.slug.clone()).or_default();
        market_entry.trades += 1;
        market_entry.total_usdc += usdc;
        if let Some(price) = record.activity_price {
            market_entry.sum_activity_price += price;
            market_entry.activity_price_count += 1;
        }
        if let Some(price) = record.binance_price {
            market_entry.sum_binance_price += price;
            market_entry.binance_price_count += 1;
        }
        if let Some(price) = record.up_ask {
            market_entry.sum_up_ask += price;
            market_entry.up_ask_count += 1;
        }
        if let Some(price) = record.down_ask {
            market_entry.sum_down_ask += price;
            market_entry.down_ask_count += 1;
        }
    }

    let _ = writeln!(output);
    let _ = writeln!(output, "side:");
    for (side, aggregate) in by_side {
        let _ = writeln!(
            output,
            "  {side}: trades={} usdc={}",
            aggregate.trades,
            aggregate.total_usdc.round_dp(4)
        );
    }

    let _ = writeln!(output);
    let _ = writeln!(output, "activity_type:");
    for (activity_type, aggregate) in by_activity_type {
        let _ = writeln!(
            output,
            "  {activity_type}: trades={} usdc={}",
            aggregate.trades,
            aggregate.total_usdc.round_dp(4)
        );
    }

    let _ = writeln!(output);
    let _ = writeln!(output, "outcome:");
    for (outcome, aggregate) in by_outcome {
        let _ = writeln!(
            output,
            "  {outcome}: trades={} usdc={}",
            aggregate.trades,
            aggregate.total_usdc.round_dp(4)
        );
    }

    let _ = writeln!(output);
    let _ = writeln!(output, "Summary:");
    for (minutes, aggregate) in by_minutes {
        let _ = writeln!(
            output,
            "  {minutes}m: trades={} usdc={}",
            aggregate.trades,
            aggregate.total_usdc.round_dp(4)
        );
    }

    let _ = writeln!(output);
    let _ = writeln!(output, ":");
    for (bucket, aggregate) in by_seconds_left_bucket {
        let _ = writeln!(
            output,
            "  {bucket}: trades={} usdc={} avg_selected_ask={}",
            aggregate.trades,
            aggregate.total_usdc.round_dp(4),
            average_decimal(aggregate.selected_ask_sum, aggregate.selected_ask_count)
        );
    }

    let _ = writeln!(output);
    let _ = writeln!(output, "target_gap_bps:");
    for (bucket, aggregate) in by_target_gap_bucket {
        let _ = writeln!(
            output,
            "  {bucket}: trades={} usdc={} avg_selected_ask={}",
            aggregate.trades,
            aggregate.total_usdc.round_dp(4),
            average_decimal(aggregate.selected_ask_sum, aggregate.selected_ask_count)
        );
    }

    let _ = writeln!(output);
    let _ = writeln!(output, ":");
    for (bucket, aggregate) in by_selected_ask_bucket {
        let _ = writeln!(
            output,
            "  {bucket}: trades={} usdc={}",
            aggregate.trades,
            aggregate.total_usdc.round_dp(4)
        );
    }

    let _ = writeln!(output);
    let _ = writeln!(output, "target_gap:");
    for (bucket, aggregate) in by_target_alignment {
        let _ = writeln!(
            output,
            "  {bucket}: trades={} usdc={} avg_selected_ask={}",
            aggregate.trades,
            aggregate.total_usdc.round_dp(4),
            average_decimal(aggregate.selected_ask_sum, aggregate.selected_ask_count)
        );
    }

    let _ = writeln!(output);
    let _ = writeln!(output, ":");
    for (bucket, aggregate) in by_progress_bucket {
        let _ = writeln!(
            output,
            "  {bucket}: trades={} usdc={} avg_progress_pct={}",
            aggregate.trades,
            aggregate.total_usdc.round_dp(4),
            average_decimal(aggregate.aux_sum, aggregate.aux_count)
        );
    }

    let _ = writeln!(output);
    let _ = writeln!(output, "discount ask:");
    for (bucket, aggregate) in by_discount_bucket {
        let _ = writeln!(
            output,
            "  {bucket}: trades={} usdc={} avg_discount_bps={}",
            aggregate.trades,
            aggregate.total_usdc.round_dp(4),
            average_decimal(aggregate.aux_sum, aggregate.aux_count)
        );
    }

    let _ = writeln!(output);
    let _ = writeln!(output, "spread :");
    for (bucket, aggregate) in by_spread_bucket {
        let _ = writeln!(
            output,
            "  {bucket}: trades={} usdc={} avg_spread_bps={}",
            aggregate.trades,
            aggregate.total_usdc.round_dp(4),
            average_decimal(aggregate.aux_sum, aggregate.aux_count)
        );
    }

    let _ = writeln!(output);
    let _ = writeln!(output, ":");
    for (bucket, aggregate) in by_trade_size_bucket {
        let _ = writeln!(
            output,
            "  {bucket}: trades={} usdc={} avg_selected_vs_opposite_mid_bps={}",
            aggregate.trades,
            aggregate.total_usdc.round_dp(4),
            average_decimal(aggregate.aux_sum, aggregate.aux_count)
        );
    }

    let mut top_slugs = by_slug.into_iter().collect::<Vec<_>>();
    top_slugs.sort_by(|left, right| {
        right
            .1
            .trades
            .cmp(&left.1.trades)
            .then_with(|| right.1.total_usdc.cmp(&left.1.total_usdc))
    });

    let _ = writeln!(output);
    let _ = writeln!(output, ":");
    for (slug, aggregate) in top_slugs.into_iter().take(top) {
        let avg_trade_price =
            average_decimal(aggregate.sum_activity_price, aggregate.activity_price_count);
        let avg_binance =
            average_decimal(aggregate.sum_binance_price, aggregate.binance_price_count);
        let avg_up_ask = average_decimal(aggregate.sum_up_ask, aggregate.up_ask_count);
        let avg_down_ask = average_decimal(aggregate.sum_down_ask, aggregate.down_ask_count);
        let _ = writeln!(
            output,
            "  {slug}: trades={} usdc={} avg_trade={} avg_binance={} avg_up_ask={} avg_down_ask={}",
            aggregate.trades,
            aggregate.total_usdc.round_dp(4),
            avg_trade_price,
            avg_binance,
            avg_up_ask,
            avg_down_ask
        );
    }

    let streaks = compute_wallet_slug_streaks(records);
    if !streaks.is_empty() {
        let repeated_trade_count = streaks
            .iter()
            .filter(|streak| streak.trades > 1)
            .map(|streak| streak.trades)
            .sum::<usize>();
        let repeated_streaks = streaks.iter().filter(|streak| streak.trades > 1).count();
        let _ = writeln!(output);
        let _ = writeln!(
            output,
            "slug: repeated_streaks={repeated_streaks} repeated_trades={repeated_trade_count}",
        );
        let _ = writeln!(output, "slug:");

        let mut top_streaks = streaks;
        top_streaks.sort_by(|left, right| {
            right
                .trades
                .cmp(&left.trades)
                .then_with(|| right.total_usdc.cmp(&left.total_usdc))
                .then_with(|| left.started_at.cmp(&right.started_at))
        });

        for streak in top_streaks.into_iter().take(top) {
            let _ = writeln!(
                output,
                "  {} | outcome={} | trades={} | usdc={} | {} -> {}",
                streak.slug,
                if streak.outcome.is_empty() {
                    "-"
                } else {
                    streak.outcome.as_str()
                },
                streak.trades,
                streak.total_usdc.round_dp(4),
                format_unix_secs_local(streak.started_at),
                format_unix_secs_local(streak.ended_at)
            );
        }
    }

    let position_summaries = compute_wallet_position_summaries(records);
    if !position_summaries.is_empty() {
        let bonereaper_forensics =
            compute_wallet_bonereaper_forensics(records, &position_summaries);
        let total_execution_trades = bonereaper_forensics.maker_like_trades
            + bonereaper_forensics.neutral_execution_trades
            + bonereaper_forensics.crossed_or_stale_trades
            + bonereaper_forensics.unknown_execution_trades;
        let inventory_balance_ratio = inventory_balance_ratio_string(
            bonereaper_forensics.gross_inventory_shares,
            bonereaper_forensics.net_directional_delta_shares,
        );
        let micro_share_pct = if records.is_empty() {
            Decimal::ZERO
        } else {
            (Decimal::from(bonereaper_forensics.micro_window_trades as u64)
                / Decimal::from(records.len() as u64)
                * Decimal::from(100_u32))
            .round_dp(2)
        };
        let two_sided_pct = if bonereaper_forensics.total_windows == 0 {
            Decimal::ZERO
        } else {
            (Decimal::from(bonereaper_forensics.two_sided_windows as u64)
                / Decimal::from(bonereaper_forensics.total_windows as u64)
                * Decimal::from(100_u32))
            .round_dp(2)
        };

        let _ = writeln!(output);
        let _ = writeln!(output, "Bonereaper-style forensic profile (heuristic):");
        let _ = writeln!(
            output,
            "  micro-window trades={} / {} ({}%) usdc={}",
            bonereaper_forensics.micro_window_trades,
            records.len(),
            micro_share_pct,
            bonereaper_forensics.micro_window_usdc.round_dp(4),
        );
        let _ = writeln!(
            output,
            "  btc_trades={} eth_trades={}",
            bonereaper_forensics.btc_trades, bonereaper_forensics.eth_trades
        );
        let _ = writeln!(
            output,
            "  two-sided residual windows={} / {} ({}%) balanced_windows={}",
            bonereaper_forensics.two_sided_windows,
            bonereaper_forensics.total_windows,
            two_sided_pct,
            bonereaper_forensics.balanced_windows
        );
        let _ = writeln!(
            output,
            "  gross_inventory_shares={} net_directional_delta_shares={} inventory_balance={}",
            bonereaper_forensics.gross_inventory_shares.round_dp(4),
            bonereaper_forensics
                .net_directional_delta_shares
                .round_dp(4),
            inventory_balance_ratio
        );
        let _ = writeln!(
            output,
            "  execution_heuristic: maker-like={} neutral={} crossed-or-stale={} unknown={} total={}",
            bonereaper_forensics.maker_like_trades,
            bonereaper_forensics.neutral_execution_trades,
            bonereaper_forensics.crossed_or_stale_trades,
            bonereaper_forensics.unknown_execution_trades,
            total_execution_trades
        );

        let mut top_positions = position_summaries;
        top_positions.sort_by(|left, right| {
            right
                .gross_usdc
                .cmp(&left.gross_usdc)
                .then_with(|| right.trades.cmp(&left.trades))
                .then_with(|| left.started_at.cmp(&right.started_at))
        });

        let _ = writeln!(output);
        let _ = writeln!(output, "- slug:");
        for summary in top_positions.into_iter().take(top) {
            let _ = writeln!(
                output,
                "  {} | trades={} buy={} sell={} gross_usdc={} avg_trade={} duration={}s avg_spacing={} | net_up_shares={} net_down_shares={} | end={} switches={} | first_gap={} last_gap={} max_abs_gap={} | first_left={} last_left={} | {} -> {}",
                summary.slug,
                summary.trades,
                summary.buy_trades,
                summary.sell_trades,
                summary.gross_usdc.round_dp(4),
                summary.avg_usdc_per_trade.round_dp(4),
                summary.duration_secs,
                summary
                    .avg_seconds_between_trades
                    .map_or_else(|| "-".to_owned(), |value| value.round_dp(2).to_string()),
                summary.up_shares_net.round_dp(4),
                summary.down_shares_net.round_dp(4),
                if summary.dominant_end_outcome.is_empty() {
                    "-"
                } else {
                    summary.dominant_end_outcome.as_str()
                },
                summary.dominant_switches,
                summary
                    .first_target_gap_bps
                    .map_or_else(|| "-".to_owned(), |value| value.round_dp(2).to_string()),
                summary
                    .last_target_gap_bps
                    .map_or_else(|| "-".to_owned(), |value| value.round_dp(2).to_string()),
                summary
                    .max_abs_target_gap_bps
                    .map_or_else(|| "-".to_owned(), |value| value.round_dp(2).to_string()),
                summary
                    .first_seconds_left
                    .map_or_else(|| "-".to_owned(), |value| value.to_string()),
                summary
                    .last_seconds_left
                    .map_or_else(|| "-".to_owned(), |value| value.to_string()),
                format_unix_secs_local(summary.started_at),
                format_unix_secs_local(summary.ended_at),
            );
        }
    }

    sanitize_legacy_mojibake(&output)
}

fn average_decimal(sum: Decimal, count: usize) -> String {
    if count == 0 {
        "-".to_owned()
    } else {
        (sum / Decimal::from(count as u64)).round_dp(6).to_string()
    }
}

fn selected_outcome_ask(record: &WalletActivitySnapshotRecord) -> Option<Decimal> {
    if outcome_side_is_up(&record.outcome) {
        record.up_ask
    } else if outcome_side_is_down(&record.outcome) {
        record.down_ask
    } else {
        None
    }
}

fn selected_outcome_bid(record: &WalletActivitySnapshotRecord) -> Option<Decimal> {
    if outcome_side_is_up(&record.outcome) {
        record.up_bid
    } else if outcome_side_is_down(&record.outcome) {
        record.down_bid
    } else {
        None
    }
}

fn opposite_outcome_ask(record: &WalletActivitySnapshotRecord) -> Option<Decimal> {
    if outcome_side_is_up(&record.outcome) {
        record.down_ask
    } else if outcome_side_is_down(&record.outcome) {
        record.up_ask
    } else {
        None
    }
}

fn opposite_outcome_bid(record: &WalletActivitySnapshotRecord) -> Option<Decimal> {
    if outcome_side_is_up(&record.outcome) {
        record.down_bid
    } else if outcome_side_is_down(&record.outcome) {
        record.up_bid
    } else {
        None
    }
}

fn midpoint_price(bid: Option<Decimal>, ask: Option<Decimal>) -> Option<Decimal> {
    match (bid, ask) {
        (Some(bid), Some(ask)) => Some(((bid + ask) / Decimal::TWO).round_dp(6)),
        _ => None,
    }
}

fn display_price_estimate(
    bid: Option<Decimal>,
    ask: Option<Decimal>,
    last_trade_price: Option<Decimal>,
) -> Option<Decimal> {
    match (bid, ask) {
        (Some(bid), Some(ask)) => {
            let spread = ask - bid;
            if spread > Decimal::new(10, 2) {
                last_trade_price.or_else(|| midpoint_price(Some(bid), Some(ask)))
            } else {
                midpoint_price(Some(bid), Some(ask))
            }
        }
        _ => last_trade_price,
    }
}

fn outcome_side_is_up(value: &str) -> bool {
    outcome_label_is_up(value)
}

fn outcome_side_is_down(value: &str) -> bool {
    outcome_label_is_down(value)
}

fn seconds_left_bucket_label(seconds_left: Option<i64>) -> String {
    match seconds_left.unwrap_or(-1) {
        ..=-1 => "unknown".to_owned(),
        0..=29 => "0-29s".to_owned(),
        30..=59 => "30-59s".to_owned(),
        60..=89 => "60-89s".to_owned(),
        90..=119 => "90-119s".to_owned(),
        120..=179 => "120-179s".to_owned(),
        _ => "180s+".to_owned(),
    }
}

fn target_gap_bucket_label(target_gap_bps: Option<Decimal>) -> String {
    let Some(target_gap_bps) = target_gap_bps else {
        return "unknown".to_owned();
    };
    let abs_gap = target_gap_bps.abs();
    if abs_gap < Decimal::from(2_u32) {
        "<2bps".to_owned()
    } else if abs_gap < Decimal::from(5_u32) {
        "2-5bps".to_owned()
    } else if abs_gap < Decimal::from(10_u32) {
        "5-10bps".to_owned()
    } else if abs_gap < Decimal::from(20_u32) {
        "10-20bps".to_owned()
    } else {
        "20bps+".to_owned()
    }
}

fn selected_ask_bucket_label(selected_ask: Option<Decimal>) -> String {
    let Some(selected_ask) = selected_ask else {
        return "unknown".to_owned();
    };
    if selected_ask < Decimal::new(50, 2) {
        "<0.50".to_owned()
    } else if selected_ask < Decimal::new(60, 2) {
        "0.50-0.59".to_owned()
    } else if selected_ask < Decimal::new(70, 2) {
        "0.60-0.69".to_owned()
    } else if selected_ask < Decimal::new(80, 2) {
        "0.70-0.79".to_owned()
    } else {
        "0.80+".to_owned()
    }
}

fn window_progress_bucket_label(window_progress_pct: Option<Decimal>) -> String {
    let Some(window_progress_pct) = window_progress_pct else {
        return "unknown".to_owned();
    };
    if window_progress_pct < Decimal::from(20_u32) {
        "0-19%".to_owned()
    } else if window_progress_pct < Decimal::from(40_u32) {
        "20-39%".to_owned()
    } else if window_progress_pct < Decimal::from(60_u32) {
        "40-59%".to_owned()
    } else if window_progress_pct < Decimal::from(80_u32) {
        "60-79%".to_owned()
    } else {
        "80-100%".to_owned()
    }
}

fn discount_bucket_label(discount_bps: Option<Decimal>) -> String {
    let Some(discount_bps) = discount_bps else {
        return "unknown".to_owned();
    };
    if discount_bps < Decimal::ZERO {
        "above_ask".to_owned()
    } else if discount_bps < Decimal::from(25_u32) {
        "0-24bps".to_owned()
    } else if discount_bps < Decimal::from(100_u32) {
        "25-99bps".to_owned()
    } else if discount_bps < Decimal::from(250_u32) {
        "100-249bps".to_owned()
    } else {
        "250bps+".to_owned()
    }
}

fn spread_bucket_label(spread_bps: Option<Decimal>) -> String {
    let Some(spread_bps) = spread_bps else {
        return "unknown".to_owned();
    };
    if spread_bps < Decimal::from(100_u32) {
        "<100bps".to_owned()
    } else if spread_bps < Decimal::from(300_u32) {
        "100-299bps".to_owned()
    } else if spread_bps < Decimal::from(600_u32) {
        "300-599bps".to_owned()
    } else {
        "600bps+".to_owned()
    }
}

fn trade_size_bucket_label(usdc_size: Option<Decimal>) -> String {
    let Some(usdc_size) = usdc_size else {
        return "unknown".to_owned();
    };
    if usdc_size < Decimal::from(10_u32) {
        "<10".to_owned()
    } else if usdc_size < Decimal::from(50_u32) {
        "10-49".to_owned()
    } else if usdc_size < Decimal::from(100_u32) {
        "50-99".to_owned()
    } else if usdc_size < Decimal::from(250_u32) {
        "100-249".to_owned()
    } else if usdc_size < Decimal::from(500_u32) {
        "250-499".to_owned()
    } else {
        "500+".to_owned()
    }
}

fn target_alignment_label(record: &WalletActivitySnapshotRecord) -> String {
    let Some(target_gap_bps) = record.target_gap_bps else {
        return "unknown".to_owned();
    };
    if target_gap_bps == Decimal::ZERO {
        return "flat_target_gap".to_owned();
    }

    let outcome = record.outcome.as_str();
    let with_gap = (target_gap_bps > Decimal::ZERO && outcome_side_is_up(outcome))
        || (target_gap_bps < Decimal::ZERO && outcome_side_is_down(outcome));
    let against_gap = (target_gap_bps > Decimal::ZERO && outcome_side_is_down(outcome))
        || (target_gap_bps < Decimal::ZERO && outcome_side_is_up(outcome));

    if with_gap {
        "with_target_gap".to_owned()
    } else if against_gap {
        "against_target_gap".to_owned()
    } else {
        "unknown".to_owned()
    }
}

fn compute_wallet_slug_streaks(
    records: &[WalletActivitySnapshotRecord],
) -> Vec<WalletActivitySlugStreak> {
    let mut streaks = Vec::new();
    let mut current: Option<WalletActivitySlugStreak> = None;

    for record in records {
        let usdc = record.usdc_size.unwrap_or(Decimal::ZERO);
        match current.as_mut() {
            Some(streak) if streak.slug == record.slug && streak.outcome == record.outcome => {
                streak.trades += 1;
                streak.total_usdc += usdc;
                streak.ended_at = record.activity_timestamp;
            }
            Some(_) => {
                if let Some(finished) = current.take() {
                    streaks.push(finished);
                }
                current = Some(WalletActivitySlugStreak {
                    slug: record.slug.clone(),
                    outcome: record.outcome.clone(),
                    trades: 1,
                    started_at: record.activity_timestamp,
                    ended_at: record.activity_timestamp,
                    total_usdc: usdc,
                });
            }
            None => {
                current = Some(WalletActivitySlugStreak {
                    slug: record.slug.clone(),
                    outcome: record.outcome.clone(),
                    trades: 1,
                    started_at: record.activity_timestamp,
                    ended_at: record.activity_timestamp,
                    total_usdc: usdc,
                });
            }
        }
    }

    if let Some(finished) = current {
        streaks.push(finished);
    }

    streaks
}

#[allow(clippy::too_many_lines)]
fn compute_wallet_position_summaries(
    records: &[WalletActivitySnapshotRecord],
) -> Vec<WalletActivityPositionSummary> {
    let mut by_slug = BTreeMap::<String, Vec<&WalletActivitySnapshotRecord>>::new();
    for record in records {
        by_slug.entry(record.slug.clone()).or_default().push(record);
    }

    let mut summaries = Vec::new();
    for (slug, mut slug_records) in by_slug {
        slug_records.sort_by(|left, right| {
            left.activity_timestamp
                .cmp(&right.activity_timestamp)
                .then_with(|| left.recorded_at.cmp(&right.recorded_at))
        });

        let mut buy_trades = 0_usize;
        let mut sell_trades = 0_usize;
        let mut gross_usdc = Decimal::ZERO;
        let mut up_shares_net = Decimal::ZERO;
        let mut down_shares_net = Decimal::ZERO;
        let mut dominant_switches = 0_usize;
        let mut previous_dominant = String::new();
        let mut previous_trade_ts: Option<i64> = None;
        let mut trade_spacing_sum = Decimal::ZERO;
        let mut trade_spacing_count = 0_usize;

        for record in &slug_records {
            let usdc = record.usdc_size.unwrap_or(Decimal::ZERO);
            gross_usdc += usdc;
            let shares = inferred_shares(record).unwrap_or(Decimal::ZERO);
            let is_buy = wallet_side_is_buy(&record.side);
            let is_sell = wallet_side_is_sell(&record.side);

            if is_buy {
                buy_trades += 1;
            } else if is_sell {
                sell_trades += 1;
            }

            if outcome_side_is_up(&record.outcome) {
                if is_sell {
                    up_shares_net -= shares;
                } else {
                    up_shares_net += shares;
                }
            } else if outcome_side_is_down(&record.outcome) {
                if is_sell {
                    down_shares_net -= shares;
                } else {
                    down_shares_net += shares;
                }
            }

            let dominant = dominant_outcome_from_net_shares(up_shares_net, down_shares_net);
            if !previous_dominant.is_empty()
                && !dominant.is_empty()
                && previous_dominant != dominant
            {
                dominant_switches += 1;
            }
            if !dominant.is_empty() {
                previous_dominant = dominant;
            }

            if let Some(previous_ts) = previous_trade_ts {
                let delta_secs = (record.activity_timestamp - previous_ts).max(0);
                trade_spacing_sum += Decimal::from(delta_secs);
                trade_spacing_count += 1;
            }
            previous_trade_ts = Some(record.activity_timestamp);
        }

        let started_at = slug_records
            .first()
            .map_or(0, |record| record.activity_timestamp);
        let ended_at = slug_records
            .last()
            .map_or(0, |record| record.activity_timestamp);
        let dominant_end_outcome = dominant_outcome_from_net_shares(up_shares_net, down_shares_net);
        let avg_usdc_per_trade = if slug_records.is_empty() {
            Decimal::ZERO
        } else {
            (gross_usdc / Decimal::from(slug_records.len() as u64)).round_dp(6)
        };
        let duration_secs = ended_at.saturating_sub(started_at);
        let avg_seconds_between_trades = if trade_spacing_count == 0 {
            None
        } else {
            Some((trade_spacing_sum / Decimal::from(trade_spacing_count as u64)).round_dp(4))
        };
        let first_target_gap_bps = slug_records
            .first()
            .and_then(|record| record.target_gap_bps);
        let last_target_gap_bps = slug_records.last().and_then(|record| record.target_gap_bps);
        let max_abs_target_gap_bps = slug_records
            .iter()
            .filter_map(|record| record.target_gap_bps.map(|value| value.abs()))
            .max();
        let first_seconds_left = slug_records
            .first()
            .and_then(|record| record.seconds_left_at_observed);
        let last_seconds_left = slug_records
            .last()
            .and_then(|record| record.seconds_left_at_observed);

        summaries.push(WalletActivityPositionSummary {
            slug,
            started_at,
            ended_at,
            trades: slug_records.len(),
            buy_trades,
            sell_trades,
            gross_usdc,
            avg_usdc_per_trade,
            duration_secs,
            avg_seconds_between_trades,
            up_shares_net,
            down_shares_net,
            dominant_end_outcome,
            dominant_switches,
            first_target_gap_bps,
            last_target_gap_bps,
            max_abs_target_gap_bps,
            first_seconds_left,
            last_seconds_left,
        });
    }

    summaries
}

fn compute_wallet_bonereaper_forensics(
    records: &[WalletActivitySnapshotRecord],
    position_summaries: &[WalletActivityPositionSummary],
) -> WalletBonereaperForensics {
    let mut forensics = WalletBonereaperForensics {
        total_windows: position_summaries.len(),
        ..WalletBonereaperForensics::default()
    };

    for record in records {
        let usdc = record.usdc_size.unwrap_or(Decimal::ZERO);
        if is_micro_window_record(record) {
            forensics.micro_window_trades += 1;
            forensics.micro_window_usdc += usdc;
        }
        if record.slug.starts_with("btc-updown-") {
            forensics.btc_trades += 1;
        } else if record.slug.starts_with("eth-updown-") {
            forensics.eth_trades += 1;
        }

        match execution_heuristic(record.selected_trade_discount_to_ask_bps) {
            WalletExecutionHeuristic::MakerLike => forensics.maker_like_trades += 1,
            WalletExecutionHeuristic::Neutral => forensics.neutral_execution_trades += 1,
            WalletExecutionHeuristic::CrossedOrStale => {
                forensics.crossed_or_stale_trades += 1;
            }
            WalletExecutionHeuristic::Unknown => forensics.unknown_execution_trades += 1,
        }
    }

    for summary in position_summaries {
        let up_abs = summary.up_shares_net.abs();
        let down_abs = summary.down_shares_net.abs();
        let gross_inventory = up_abs + down_abs;
        let directional_delta = (summary.up_shares_net - summary.down_shares_net).abs();

        forensics.gross_inventory_shares += gross_inventory;
        forensics.net_directional_delta_shares += directional_delta;

        if summary.up_shares_net > Decimal::ZERO && summary.down_shares_net > Decimal::ZERO {
            forensics.two_sided_windows += 1;
        } else {
            forensics.one_sided_windows += 1;
        }

        if gross_inventory > Decimal::ZERO
            && directional_delta <= (gross_inventory * Decimal::new(20, 2)).round_dp(8)
        {
            forensics.balanced_windows += 1;
        }
    }

    forensics
}

fn execution_heuristic(discount_to_ask_bps: Option<Decimal>) -> WalletExecutionHeuristic {
    match discount_to_ask_bps {
        Some(discount) if discount >= Decimal::from(15_u32) => WalletExecutionHeuristic::MakerLike,
        Some(discount) if discount <= Decimal::from(-15_i32) => {
            WalletExecutionHeuristic::CrossedOrStale
        }
        Some(_) => WalletExecutionHeuristic::Neutral,
        None => WalletExecutionHeuristic::Unknown,
    }
}

fn inventory_balance_ratio_string(gross_inventory: Decimal, directional_delta: Decimal) -> String {
    if gross_inventory <= Decimal::ZERO {
        "-".to_owned()
    } else {
        let hedged_share = ((gross_inventory - directional_delta.max(Decimal::ZERO))
            / gross_inventory
            * Decimal::from(100_u32))
        .round_dp(2);
        format!("{hedged_share}% hedged")
    }
}

fn is_micro_window_record(record: &WalletActivitySnapshotRecord) -> bool {
    record
        .minutes_window
        .is_some_and(|minutes| minutes > 0 && minutes <= 30)
        || record.slug.starts_with("btc-updown-")
        || record.slug.starts_with("eth-updown-")
        || record.slug.starts_with("sol-updown-")
        || record.slug.starts_with("xrp-updown-")
        || record.slug.starts_with("bnb-updown-")
}

fn inferred_shares(record: &WalletActivitySnapshotRecord) -> Option<Decimal> {
    match (record.usdc_size, record.activity_price) {
        (Some(usdc_size), Some(activity_price)) if activity_price > Decimal::ZERO => {
            Some((usdc_size / activity_price).round_dp(8))
        }
        _ => None,
    }
}

fn dominant_outcome_from_net_shares(up_shares: Decimal, down_shares: Decimal) -> String {
    match up_shares.cmp(&down_shares) {
        std::cmp::Ordering::Greater if up_shares > Decimal::ZERO => "Up".to_owned(),
        std::cmp::Ordering::Less if down_shares > Decimal::ZERO => "Down".to_owned(),
        _ => String::new(),
    }
}

fn wallet_side_is_buy(value: &str) -> bool {
    wallet_side_is_buy_label(value)
}

fn wallet_side_is_sell(value: &str) -> bool {
    wallet_side_is_sell_label(value)
}

fn best_ask_price(book: &OrderBook) -> Option<Decimal> {
    book.best_ask().map(|level| level.price)
}

fn best_bid_price(book: &OrderBook) -> Option<Decimal> {
    book.best_bid().map(|level| level.price)
}

fn render_wallet_activity_snapshot(record: &WalletActivitySnapshotRecord) -> String {
    let activity_time = format_unix_secs_local(record.activity_timestamp);
    let price = record
        .activity_price
        .map_or_else(|| "-".to_owned(), |value| value.round_dp(5).to_string());
    let usdc = record
        .usdc_size
        .map_or_else(|| "-".to_owned(), |value| value.round_dp(4).to_string());
    let binance = record
        .binance_price
        .map_or_else(|| "-".to_owned(), |value| value.round_dp(2).to_string());
    let target = record
        .target_price
        .map_or_else(|| "-".to_owned(), |value| value.round_dp(2).to_string());
    let up = match (record.up_bid, record.up_ask) {
        (Some(bid), Some(ask)) => format!("{}/{}", bid.round_dp(4), ask.round_dp(4)),
        _ => "-".to_owned(),
    };
    let down = match (record.down_bid, record.down_ask) {
        (Some(bid), Some(ask)) => format!("{}/{}", bid.round_dp(4), ask.round_dp(4)),
        _ => "-".to_owned(),
    };
    let selected = match (
        record.selected_outcome_bid,
        record.selected_outcome_ask,
        record.selected_outcome_mid,
    ) {
        (Some(bid), Some(ask), Some(mid)) => {
            format!(
                "{}/{} mid={}",
                bid.round_dp(4),
                ask.round_dp(4),
                mid.round_dp(4)
            )
        }
        _ => "-".to_owned(),
    };
    let discount = record
        .selected_trade_discount_to_ask_bps
        .map_or_else(|| "-".to_owned(), |value| value.round_dp(2).to_string());
    let discount_to_mid = record
        .selected_trade_discount_to_mid_bps
        .map_or_else(|| "-".to_owned(), |value| value.round_dp(2).to_string());
    let spread = record
        .selected_outcome_spread_bps
        .map_or_else(|| "-".to_owned(), |value| value.round_dp(2).to_string());
    let display = record
        .selected_outcome_display_price_estimate
        .map_or_else(|| "-".to_owned(), |value| value.round_dp(4).to_string());
    let progress = record
        .window_progress_pct
        .map_or_else(|| "-".to_owned(), |value| value.round_dp(2).to_string());
    let rel_mid = record
        .selected_vs_opposite_mid_bps
        .map_or_else(|| "-".to_owned(), |value| value.round_dp(2).to_string());
    let bundle_share = record
        .selected_share_of_bundle_pct
        .map_or_else(|| "-".to_owned(), |value| value.round_dp(2).to_string());
    let trade_index = record
        .trade_index_in_slug
        .map_or_else(|| "-".to_owned(), |value| value.to_string());
    let since_prev = record
        .seconds_since_previous_trade_same_slug
        .map_or_else(|| "-".to_owned(), |value| value.to_string());
    let cumulative_usdc = record
        .usdc_cumulative_same_slug
        .map_or_else(|| "-".to_owned(), |value| value.round_dp(4).to_string());
    let dominant_after = record
        .dominant_outcome_after_trade
        .as_deref()
        .unwrap_or("-");
    let trade_vs_display = record
        .selected_trade_vs_display_bps
        .map_or_else(|| "-".to_owned(), |value| value.round_dp(2).to_string());

    format!(
        "[{label}] {activity_time} | {slug} | {side} {outcome} | trade={price} usdc={usdc} idx={trade_index} dt_prev={since_prev}s cum_usdc={cumulative_usdc} dom_after={dominant_after} | binance={binance} target={target} | up(bid/ask)={up} down(bid/ask)={down} | selected={selected} display_est={display} discount_to_ask_bps={discount} discount_to_mid_bps={discount_to_mid} trade_vs_display_bps={trade_vs_display} spread_bps={spread} rel_mid_bps={rel_mid} bundle_share_pct={bundle_share} progress_pct={progress} | left={seconds_left}s",
        label = record.label,
        slug = record.slug,
        side = if record.side.is_empty() {
            "-"
        } else {
            record.side.as_str()
        },
        outcome = if record.outcome.is_empty() {
            "-"
        } else {
            record.outcome.as_str()
        },
        seconds_left = record
            .seconds_left_at_observed
            .map_or_else(|| "-".to_owned(), |value| value.to_string()),
    )
}

fn render_wallet_activity(label: &str, entry: &ProfileActivityRecord) -> String {
    let timestamp = format_unix_secs_local(entry.timestamp);
    let side = if entry.side.is_empty() {
        "-"
    } else {
        entry.side.as_str()
    };
    let outcome = if entry.outcome.is_empty() {
        "-"
    } else {
        entry.outcome.as_str()
    };
    let price = entry
        .price
        .map_or_else(|| "-".to_owned(), |value| value.round_dp(5).to_string());
    let usdc_size = entry
        .usdc_size
        .map_or_else(|| "-".to_owned(), |value| value.round_dp(4).to_string());
    let tx_hash = if entry.transaction_hash.len() <= 14 {
        entry.transaction_hash.as_str()
    } else {
        &entry.transaction_hash[..14]
    };

    format!(
        "[{label}] {timestamp} | {slug} | {side} {outcome} | price={price} | usdc={usdc_size} | tx={tx_hash}",
        slug = entry.slug
    )
}

fn format_unix_secs_local(timestamp: i64) -> String {
    DateTime::<Utc>::from_timestamp(timestamp, 0).map_or_else(
        || timestamp.to_string(),
        |value| {
            value
                .with_timezone(&Local)
                .format("%Y-%m-%d %H:%M:%S %z")
                .to_string()
        },
    )
}

fn show_paper_report(config: &AppConfig, limit: Option<usize>) -> Result<()> {
    let journal = JournalStore::new(&config.storage)?;
    let previous_memory = journal.load_paper_report_memory()?;
    let cycles = journal.load_paper_cycles(limit)?;
    let snapshot = journal.load_snapshot()?;

    let total_cycles = cycles.len();
    let cycles_with_signals = cycles
        .iter()
        .filter(|entry| entry.opportunity_count > 0)
        .count();
    let cycles_with_exec = cycles
        .iter()
        .filter(|entry| entry.executed_count > 0)
        .count();
    let safe_cycles = cycles
        .iter()
        .filter(|entry| entry.regime.as_deref() == Some(RuntimeRegime::Safe.as_str()))
        .count();
    let aggressive_cycles = cycles
        .iter()
        .filter(|entry| entry.regime.as_deref() == Some(RuntimeRegime::Aggressive.as_str()))
        .count();
    let risk_blocked_cycles = cycles.iter().filter(|entry| entry.risk_blocked).count();
    let total_open_notional = snapshot
        .paper_state
        .market_notional
        .values()
        .copied()
        .sum::<Decimal>()
        .round_dp(4);
    let total_spent = snapshot.paper_state.total_spent_usdc.round_dp(4);
    let total_expected = snapshot.paper_state.total_expected_profit.round_dp(4);
    let total_realized = snapshot.paper_state.total_realized_profit.round_dp(4);
    let open_positions = snapshot.paper_state.open_positions.len();

    println!(
        "Paper report: cycles={total_cycles} signals={cycles_with_signals} executed={cycles_with_exec} safe={safe_cycles} aggressive={aggressive_cycles} risk_blocked={risk_blocked_cycles} open_positions={open_positions} open_notional={total_open_notional} spent={total_spent} expected_profit={total_expected} realized_profit={total_realized}"
    );

    let current_memory = PaperReportMemory {
        recorded_at: Utc::now(),
        total_cycles,
        cycles_with_exec,
        risk_blocked_cycles,
        total_realized_profit: total_realized,
        open_positions,
        total_open_notional,
        total_spent_usdc: total_spent,
        total_expected_profit: total_expected,
    };
    if let Some(previous) = previous_memory {
        let total_cycles_delta =
            usize_delta_to_i64(current_memory.total_cycles, previous.total_cycles);
        let executed_cycles_delta =
            usize_delta_to_i64(current_memory.cycles_with_exec, previous.cycles_with_exec);
        let risk_blocked_cycles_delta = usize_delta_to_i64(
            current_memory.risk_blocked_cycles,
            previous.risk_blocked_cycles,
        );
        let open_positions_delta =
            usize_delta_to_i64(current_memory.open_positions, previous.open_positions);
        println!(
            "Delta since previous report ({}): realized_profit={} cycles={} executed={} risk_blocked={} open_positions={} open_notional={}",
            previous
                .recorded_at
                .with_timezone(&Local)
                .format("%Y-%m-%d %H:%M:%S"),
            (current_memory.total_realized_profit - previous.total_realized_profit).round_dp(4),
            total_cycles_delta,
            executed_cycles_delta,
            risk_blocked_cycles_delta,
            open_positions_delta,
            (current_memory.total_open_notional - previous.total_open_notional).round_dp(4),
        );
    } else {
        println!("Paper report baseline saved.");
    }
    journal.save_paper_report_memory(&current_memory)?;

    if cycles.is_empty() {
        println!("Paper cycle journal is empty.");
    } else {
        println!("\n{}", render_paper_cycle_table(&cycles));
    }
    Ok(())
}

fn usize_delta_to_i64(current: usize, previous: usize) -> i64 {
    let current_i128 = i128::try_from(current).unwrap_or(i128::MAX);
    let previous_i128 = i128::try_from(previous).unwrap_or(i128::MAX);
    let delta = current_i128 - previous_i128;
    if delta > i128::from(i64::MAX) {
        i64::MAX
    } else if delta < i128::from(i64::MIN) {
        i64::MIN
    } else {
        i64::try_from(delta).unwrap_or_else(|_| {
            if delta.is_negative() {
                i64::MIN
            } else {
                i64::MAX
            }
        })
    }
}

fn parse_since_filter(since: Option<&str>) -> Result<Option<DateTime<Utc>>> {
    let Some(raw) = since.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };

    if let Ok(timestamp) = DateTime::parse_from_rfc3339(raw) {
        return Ok(Some(timestamp.with_timezone(&Utc)));
    }

    let parsed_local = NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S")
        .or_else(|_| NaiveDateTime::parse_from_str(raw, "%Y-%m-%dT%H:%M:%S"));
    if let Ok(local_naive) = parsed_local
        && let Some(local_timestamp) = Local.from_local_datetime(&local_naive).single()
    {
        return Ok(Some(local_timestamp.with_timezone(&Utc)));
    }

    Err(AppError::InvalidMarket(format!(
        "invalid --since timestamp `{raw}`; use RFC3339 like 2026-05-06T03:49:26Z or local time like 2026-05-06 10:49:26"
    )))
}

fn limit_for_since_load(limit: Option<usize>, since: Option<DateTime<Utc>>) -> Option<usize> {
    if since.is_some() { None } else { limit }
}

fn filter_paper_trades_since(
    trades: Vec<PaperTradeEntry>,
    since: Option<DateTime<Utc>>,
) -> Vec<PaperTradeEntry> {
    match since {
        Some(start) => trades
            .into_iter()
            .filter(|trade| trade.recorded_at >= start)
            .collect(),
        None => trades,
    }
}

fn filter_paper_cycles_since(
    cycles: Vec<PaperCycleEntry>,
    since: Option<DateTime<Utc>>,
) -> Vec<PaperCycleEntry> {
    match since {
        Some(start) => cycles
            .into_iter()
            .filter(|cycle| cycle.recorded_at >= start)
            .collect(),
        None => cycles,
    }
}

fn limit_recent_paper_trades(
    mut trades: Vec<PaperTradeEntry>,
    limit: Option<usize>,
) -> Vec<PaperTradeEntry> {
    if let Some(limit) = limit
        && trades.len() > limit
    {
        let drain_len = trades.len() - limit;
        trades.drain(..drain_len);
    }
    trades
}

fn since_label(since: Option<DateTime<Utc>>) -> String {
    since.map_or_else(
        || "all".to_owned(),
        |start| {
            format!(
                "since {}",
                start.with_timezone(&Local).format("%Y-%m-%d %H:%M:%S")
            )
        },
    )
}

fn show_paper_trades(config: &AppConfig, limit: Option<usize>, since: Option<&str>) -> Result<()> {
    let journal = JournalStore::new(&config.storage)?;
    let since_filter = parse_since_filter(since)?;
    let trades = journal.load_paper_trades(limit_for_since_load(limit, since_filter))?;
    let trades = limit_recent_paper_trades(filter_paper_trades_since(trades, since_filter), limit);
    if trades.is_empty() {
        println!("Paper trade journal is empty for the selected range.");
        return Ok(());
    }

    let post_trade = build_post_trade_report(&trades);

    println!(
        "Paper trades: range={} events={} open={} close={} wins={} losses={} realized_profit={} win_rate_pct={} expectancy={} profit_factor={} max_drawdown={}",
        since_label(since_filter),
        trades.len(),
        post_trade.open_count,
        post_trade.close_count,
        post_trade.win_count,
        post_trade.loss_count,
        post_trade.realized_profit.round_dp(4),
        post_trade.win_rate_pct.round_dp(2),
        post_trade.expectancy.round_dp(4),
        post_trade.profit_factor.round_dp(4),
        post_trade.max_drawdown.round_dp(4),
    );
    println!("\n{}", render_post_trade_report(&post_trade));
    println!("\n{}", render_paper_trade_table(&trades));
    Ok(())
}

fn show_paper_quality(config: &AppConfig, limit: Option<usize>, since: Option<&str>) -> Result<()> {
    let journal = JournalStore::new(&config.storage)?;
    let since_filter = parse_since_filter(since)?;
    let trades = filter_paper_trades_since(journal.load_paper_trades(None)?, since_filter);
    if trades.is_empty() {
        println!("Paper trade journal is empty for the selected range.");
        return Ok(());
    }

    let cycles = filter_paper_cycles_since(journal.load_paper_cycles(None)?, since_filter);
    let closed_trades = pair_paper_quality_trades(&trades, &cycles);
    if closed_trades.is_empty() {
        println!("Paper quality report has no closed trades in the selected range yet.");
        return Ok(());
    }

    let total_closed = closed_trades.len();
    let displayed_trades = limit_recent_quality_trades(closed_trades, limit);
    let report = build_paper_quality_report(&displayed_trades);

    println!(
        "Paper quality: range={} closed_total={} displayed={} wins={} losses={} realized_profit={} expectancy={} win_rate_pct={} avg_mfe={} avg_mae={}",
        since_label(since_filter),
        total_closed,
        report.close_count,
        report.win_count,
        report.loss_count,
        format_signed_decimal(report.realized_profit),
        format_signed_decimal(report.expectancy),
        report.win_rate_pct.round_dp(2),
        format_option_decimal(report.avg_mfe_usdc, 4),
        format_option_decimal(report.avg_mae_usdc, 4),
    );
    println!(
        "\n{}",
        render_paper_quality_buckets("By entry ask", &report.by_entry_ask)
    );
    println!(
        "\n{}",
        render_paper_quality_buckets("By seconds left", &report.by_seconds_left)
    );
    println!(
        "\n{}",
        render_paper_quality_buckets("By abs target gap", &report.by_target_gap)
    );
    println!("\n{}", render_paper_quality_table(&displayed_trades));
    Ok(())
}

fn show_paper_run_summary(
    config: &AppConfig,
    since: Option<&str>,
    limit: Option<usize>,
    top: usize,
) -> Result<()> {
    let journal = JournalStore::new(&config.storage)?;
    let since_filter = parse_since_filter(since)?;
    let trades = filter_paper_trades_since(
        journal.load_paper_trades(limit_for_since_load(limit, since_filter))?,
        since_filter,
    );
    let cycles = filter_paper_cycles_since(
        journal.load_paper_cycles(limit_for_since_load(limit, since_filter))?,
        since_filter,
    );
    let post_trade = build_post_trade_report(&trades);
    let quality_trades = pair_paper_quality_trades(&trades, &cycles);
    let quality_report =
        (!quality_trades.is_empty()).then(|| build_paper_quality_report(&quality_trades));
    let latency_report = build_paper_latency_report(&cycles);

    let cycles_with_signals = cycles
        .iter()
        .filter(|cycle| cycle.opportunity_count > 0)
        .count();
    let cycles_with_exec = cycles
        .iter()
        .filter(|cycle| cycle.executed_count > 0)
        .count();
    let cycles_with_near_miss = cycles
        .iter()
        .filter(|cycle| cycle.near_miss_count > 0)
        .count();
    let near_miss_events = cycles
        .iter()
        .map(|cycle| cycle.near_miss_count)
        .sum::<usize>();
    let risk_blocked_cycles = cycles.iter().filter(|cycle| cycle.risk_blocked).count();
    let last_cycle = cycles.last().map_or_else(
        || "-".to_owned(),
        |cycle| {
            cycle
                .recorded_at
                .with_timezone(&Local)
                .format("%Y-%m-%d %H:%M:%S")
                .to_string()
        },
    );

    println!(
        "Paper run summary: range={} trade_events={} open={} close={} wins={} losses={} realized_profit={} win_rate_pct={} expectancy={} profit_factor={} max_drawdown={} avg_mfe={} avg_mae={}",
        since_label(since_filter),
        trades.len(),
        post_trade.open_count,
        post_trade.close_count,
        post_trade.win_count,
        post_trade.loss_count,
        format_signed_decimal(post_trade.realized_profit),
        post_trade.win_rate_pct.round_dp(2),
        format_signed_decimal(post_trade.expectancy),
        post_trade.profit_factor.round_dp(4),
        post_trade.max_drawdown.round_dp(4),
        quality_report.as_ref().map_or_else(
            || "-".to_owned(),
            |report| format_option_decimal(report.avg_mfe_usdc, 4)
        ),
        quality_report.as_ref().map_or_else(
            || "-".to_owned(),
            |report| format_option_decimal(report.avg_mae_usdc, 4)
        ),
    );
    println!(
        "Paper run cycles: cycles={} signals={} exec_cycles={} near_miss_cycles={} near_miss_events={} risk_blocked={} last_cycle={}",
        cycles.len(),
        cycles_with_signals,
        cycles_with_exec,
        cycles_with_near_miss,
        near_miss_events,
        risk_blocked_cycles,
        last_cycle,
    );
    println!(
        "Paper run latency: trigger_samples={} avg_trigger_received_to_snapshot_ms={} p95_trigger_received_to_snapshot_ms={} avg_runtime_snapshot_ms={} p95_runtime_snapshot_ms={} avg_analysis_ms={} avg_selection_ms={} avg_revalidation_ms={} avg_execution_ms={} avg_cycle_total_ms={} p95_cycle_total_ms={}",
        latency_report.trigger_received_to_snapshot_count,
        format_option_u64(latency_report.avg_trigger_received_to_snapshot_ms),
        format_option_u64(latency_report.p95_trigger_received_to_snapshot_ms),
        format_option_u64(latency_report.avg_runtime_snapshot_ms),
        format_option_u64(latency_report.p95_runtime_snapshot_ms),
        format_option_u64(latency_report.avg_analysis_ms),
        format_option_u64(latency_report.avg_selection_ms),
        format_option_u64(latency_report.avg_revalidation_ms),
        format_option_u64(latency_report.avg_execution_ms),
        format_option_u64(latency_report.avg_cycle_total_ms),
        format_option_u64(latency_report.p95_cycle_total_ms),
    );

    println!(
        "\n{}",
        render_count_table(
            "Top close categories",
            &paper_close_category_counts(&trades),
            top,
        )
    );
    println!(
        "\n{}",
        render_count_table(
            "Top near-miss reasons",
            &paper_near_miss_reason_counts(&cycles),
            top,
        )
    );
    println!(
        "\n{}",
        render_count_table(
            "Top near-miss assets",
            &paper_near_miss_asset_counts(&cycles),
            top,
        )
    );
    println!(
        "\n{}",
        render_count_table(
            "Top near-miss asset + ask buckets",
            &paper_near_miss_asset_ask_bucket_counts(&cycles),
            top,
        )
    );
    println!(
        "\n{}",
        render_near_miss_quality_table(
            "Top near-miss quality buckets",
            &paper_near_miss_quality_buckets(&cycles),
            top,
        )
    );

    Ok(())
}

#[derive(Debug, Clone, Default)]
struct PaperLatencyReport {
    trigger_received_to_snapshot_count: usize,
    avg_trigger_received_to_snapshot_ms: Option<u64>,
    p95_trigger_received_to_snapshot_ms: Option<u64>,
    avg_runtime_snapshot_ms: Option<u64>,
    p95_runtime_snapshot_ms: Option<u64>,
    avg_analysis_ms: Option<u64>,
    avg_selection_ms: Option<u64>,
    avg_revalidation_ms: Option<u64>,
    avg_execution_ms: Option<u64>,
    avg_cycle_total_ms: Option<u64>,
    p95_cycle_total_ms: Option<u64>,
}

fn build_paper_latency_report(cycles: &[PaperCycleEntry]) -> PaperLatencyReport {
    let trigger_received_to_snapshot = cycles
        .iter()
        .filter_map(|cycle| cycle.latency.trigger_received_to_snapshot_ms)
        .collect::<Vec<_>>();
    let runtime_snapshot = cycles
        .iter()
        .map(|cycle| cycle.latency.runtime_snapshot_ms)
        .filter(|value| *value > 0)
        .collect::<Vec<_>>();
    let analysis = cycles
        .iter()
        .map(|cycle| cycle.latency.analysis_ms)
        .filter(|value| *value > 0)
        .collect::<Vec<_>>();
    let selection = cycles
        .iter()
        .map(|cycle| cycle.latency.selection_ms)
        .filter(|value| *value > 0)
        .collect::<Vec<_>>();
    let revalidation = cycles
        .iter()
        .map(|cycle| cycle.latency.revalidation_ms)
        .filter(|value| *value > 0)
        .collect::<Vec<_>>();
    let execution = cycles
        .iter()
        .map(|cycle| cycle.latency.execution_ms)
        .filter(|value| *value > 0)
        .collect::<Vec<_>>();
    let cycle_total = cycles
        .iter()
        .map(|cycle| cycle.latency.cycle_total_ms)
        .filter(|value| *value > 0)
        .collect::<Vec<_>>();

    PaperLatencyReport {
        trigger_received_to_snapshot_count: trigger_received_to_snapshot.len(),
        avg_trigger_received_to_snapshot_ms: average_u64(&trigger_received_to_snapshot),
        p95_trigger_received_to_snapshot_ms: percentile_u64(trigger_received_to_snapshot, 95),
        avg_runtime_snapshot_ms: average_u64(&runtime_snapshot),
        p95_runtime_snapshot_ms: percentile_u64(runtime_snapshot, 95),
        avg_analysis_ms: average_u64(&analysis),
        avg_selection_ms: average_u64(&selection),
        avg_revalidation_ms: average_u64(&revalidation),
        avg_execution_ms: average_u64(&execution),
        avg_cycle_total_ms: average_u64(&cycle_total),
        p95_cycle_total_ms: percentile_u64(cycle_total, 95),
    }
}

fn average_u64(values: &[u64]) -> Option<u64> {
    let count = u64::try_from(values.len()).ok()?;
    if count == 0 {
        return None;
    }
    Some(values.iter().copied().sum::<u64>() / count)
}

fn percentile_u64(mut values: Vec<u64>, percentile: usize) -> Option<u64> {
    if values.is_empty() {
        return None;
    }
    values.sort_unstable();
    let last_index = values.len().saturating_sub(1);
    let index = last_index.saturating_mul(percentile).div_ceil(100);
    values.get(index).copied()
}

fn format_option_u64(value: Option<u64>) -> String {
    value.map_or_else(|| "-".to_owned(), |value| value.to_string())
}

fn paper_close_category_counts(trades: &[PaperTradeEntry]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::<String, usize>::new();
    for trade in trades
        .iter()
        .filter(|trade| trade.action == PaperTradeAction::Close)
    {
        let label = trade
            .close_category
            .as_deref()
            .unwrap_or("unknown")
            .to_owned();
        *counts.entry(label).or_default() += 1;
    }
    counts
}

fn paper_near_miss_reason_counts(cycles: &[PaperCycleEntry]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::<String, usize>::new();
    for cycle in cycles {
        if let Some(reason) = cycle.top_near_miss_reason.as_deref() {
            *counts.entry(reason.to_owned()).or_default() += 1;
        }
    }
    counts
}

fn paper_near_miss_asset_counts(cycles: &[PaperCycleEntry]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::<String, usize>::new();
    for cycle in cycles
        .iter()
        .filter(|cycle| cycle.top_near_miss_reason.is_some())
    {
        *counts
            .entry(cycle_near_miss_asset_label(cycle).to_owned())
            .or_default() += 1;
    }
    counts
}

fn paper_near_miss_asset_ask_bucket_counts(cycles: &[PaperCycleEntry]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::<String, usize>::new();
    for cycle in cycles
        .iter()
        .filter(|cycle| cycle.top_near_miss_reason.is_some())
    {
        let ask = cycle_directional_ask(cycle);
        let key = format!(
            "{} | {}",
            cycle_near_miss_asset_label(cycle),
            near_miss_ask_bucket_label(ask),
        );
        *counts.entry(key).or_default() += 1;
    }
    counts
}

#[derive(Debug, Clone, Default)]
struct NearMissQualityStats {
    count: usize,
    ask_sum: Decimal,
    ask_count: usize,
    gap_abs_sum: Decimal,
    gap_abs_count: usize,
    fresh_abs_sum: Decimal,
    fresh_abs_count: usize,
    top_imbalance_abs_sum: Decimal,
    top_imbalance_abs_count: usize,
    depth_abs_sum: Decimal,
    depth_abs_count: usize,
    book_age_ms_sum: i64,
    book_age_ms_count: usize,
}

impl NearMissQualityStats {
    fn record(&mut self, cycle: &PaperCycleEntry) {
        self.count += 1;
        if let Some(ask) = cycle_directional_ask(cycle) {
            self.ask_sum += ask;
            self.ask_count += 1;
        }
        if let Some(gap_abs) = cycle_near_miss_target_gap_abs_bps(cycle) {
            self.gap_abs_sum += gap_abs;
            self.gap_abs_count += 1;
        }
        if let Some(fresh_abs) = cycle_near_miss_fresh_signal_abs_bps(cycle) {
            self.fresh_abs_sum += fresh_abs;
            self.fresh_abs_count += 1;
        }
        if let Some(top_imbalance_abs) = cycle_near_miss_top_imbalance_abs_bps(cycle) {
            self.top_imbalance_abs_sum += top_imbalance_abs;
            self.top_imbalance_abs_count += 1;
        }
        if let Some(depth_abs) = cycle_near_miss_depth_abs_bps(cycle) {
            self.depth_abs_sum += depth_abs;
            self.depth_abs_count += 1;
        }
        if let Some(book_age_ms) = cycle_near_miss_book_age_ms(cycle) {
            self.book_age_ms_sum += book_age_ms;
            self.book_age_ms_count += 1;
        }
    }

    fn avg_ask(&self) -> Option<Decimal> {
        average_decimal_option(self.ask_sum, self.ask_count)
    }

    fn avg_gap_abs(&self) -> Option<Decimal> {
        average_decimal_option(self.gap_abs_sum, self.gap_abs_count)
    }

    fn avg_fresh_abs(&self) -> Option<Decimal> {
        average_decimal_option(self.fresh_abs_sum, self.fresh_abs_count)
    }

    fn avg_top_imbalance_abs(&self) -> Option<Decimal> {
        average_decimal_option(self.top_imbalance_abs_sum, self.top_imbalance_abs_count)
    }

    fn avg_depth_abs(&self) -> Option<Decimal> {
        average_decimal_option(self.depth_abs_sum, self.depth_abs_count)
    }

    fn avg_book_age_ms(&self) -> Option<Decimal> {
        average_i64(self.book_age_ms_sum, self.book_age_ms_count)
    }
}

fn paper_near_miss_quality_buckets(
    cycles: &[PaperCycleEntry],
) -> BTreeMap<(String, String, String), NearMissQualityStats> {
    let mut buckets = BTreeMap::<(String, String, String), NearMissQualityStats>::new();
    for cycle in cycles {
        let Some(reason) = cycle.top_near_miss_reason.as_deref() else {
            continue;
        };
        let ask = cycle_directional_ask(cycle);
        let key = (
            cycle_near_miss_asset_label(cycle).to_owned(),
            near_miss_ask_bucket_label(ask).to_owned(),
            reason.to_owned(),
        );
        buckets.entry(key).or_default().record(cycle);
    }
    buckets
}

fn render_near_miss_quality_table(
    title: &str,
    buckets: &BTreeMap<(String, String, String), NearMissQualityStats>,
    top: usize,
) -> String {
    let mut output = String::new();
    let _ = writeln!(output, "{title}:");
    if buckets.is_empty() || top == 0 {
        let _ = writeln!(output, "none");
        return output;
    }

    let mut rows = buckets.iter().collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        right
            .1
            .count
            .cmp(&left.1.count)
            .then_with(|| left.0.cmp(right.0))
    });
    let _ = writeln!(
        output,
        "{:<6} {:<5} {:<11} {:>7} {:>8} {:>8} {:>9} {:>9} {:>8} Reason",
        "Count",
        "Asset",
        "AskBucket",
        "AvgAsk",
        "AvgGap",
        "AvgFresh",
        "AvgTopOb",
        "AvgDepth",
        "BookMs",
    );
    let _ = writeln!(output, "{}", "-".repeat(132));
    for ((asset, ask_bucket, reason), stats) in rows.into_iter().take(top) {
        let _ = writeln!(
            output,
            "{:<6} {:<5} {:<11} {:>7} {:>8} {:>8} {:>9} {:>9} {:>8} {}",
            stats.count,
            asset,
            ask_bucket,
            format_option_decimal(stats.avg_ask(), 4),
            format_option_decimal(stats.avg_gap_abs(), 2),
            format_option_decimal(stats.avg_fresh_abs(), 2),
            format_option_decimal(stats.avg_top_imbalance_abs(), 2),
            format_option_decimal(stats.avg_depth_abs(), 2),
            format_option_decimal(stats.avg_book_age_ms(), 0),
            truncate_text_v2(reason, 44),
        );
    }
    output
}

fn cycle_near_miss_asset_label(cycle: &PaperCycleEntry) -> &'static str {
    cycle
        .top_near_miss_slug
        .as_deref()
        .or(cycle.current_market_slug.as_deref())
        .map_or("UNKNOWN", market_slug_asset_label)
}

fn market_slug_asset_label(slug: &str) -> &'static str {
    if slug.starts_with("btc-updown-") {
        "BTC"
    } else if slug.starts_with("eth-updown-") {
        "ETH"
    } else if slug.starts_with("sol-updown-") {
        "SOL"
    } else if slug.starts_with("xrp-updown-") {
        "XRP"
    } else if slug.starts_with("bnb-updown-") {
        "BNB"
    } else {
        "UNKNOWN"
    }
}

fn cycle_directional_ask(cycle: &PaperCycleEntry) -> Option<Decimal> {
    if let Some(primary_ask) = parse_cycle_decimal(cycle.top_near_miss_primary_ask.as_deref()) {
        return Some(primary_ask);
    }

    let target_gap = parse_cycle_decimal(cycle.current_market_target_gap_bps.as_deref());
    let ask = if target_gap.is_some_and(|value| value < Decimal::ZERO) {
        cycle.current_market_down_ask.as_deref()
    } else {
        cycle.current_market_up_ask.as_deref()
    };
    parse_cycle_decimal(ask)
}

fn near_miss_ask_bucket_label(ask: Option<Decimal>) -> &'static str {
    match ask {
        Some(value) if value < Decimal::new(45, 2) => "<0.45",
        Some(value) if value < Decimal::new(50, 2) => "0.45-0.50",
        Some(value) if value < Decimal::new(56, 2) => "0.50-0.56",
        Some(value) if value < Decimal::new(60, 2) => "0.56-0.60",
        Some(_) => ">=0.60",
        None => "unknown",
    }
}

fn cycle_near_miss_target_gap_abs_bps(cycle: &PaperCycleEntry) -> Option<Decimal> {
    parse_cycle_decimal(cycle.top_near_miss_target_gap_bps.as_deref())
        .or_else(|| parse_cycle_decimal(cycle.current_market_target_gap_bps.as_deref()))
        .map(decimal_abs)
}

fn cycle_near_miss_fresh_signal_abs_bps(cycle: &PaperCycleEntry) -> Option<Decimal> {
    let exact = [
        cycle.top_near_miss_spot_move_1s_bps.as_deref(),
        cycle.top_near_miss_spot_move_5s_bps.as_deref(),
        cycle.top_near_miss_spot_move_15s_bps.as_deref(),
        cycle.top_near_miss_micro_acceleration_bps.as_deref(),
    ];
    let fallback = [
        cycle.current_market_spot_move_1s_bps.as_deref(),
        cycle.current_market_spot_move_5s_bps.as_deref(),
        cycle.current_market_spot_move_15s_bps.as_deref(),
        cycle.current_market_micro_acceleration_bps.as_deref(),
    ];

    max_abs_decimal(exact).or_else(|| max_abs_decimal(fallback))
}

fn cycle_near_miss_top_imbalance_abs_bps(cycle: &PaperCycleEntry) -> Option<Decimal> {
    parse_cycle_decimal(
        cycle
            .top_near_miss_exchange_book_top_imbalance_bps
            .as_deref(),
    )
    .or_else(|| {
        parse_cycle_decimal(
            cycle
                .current_market_exchange_book_top_imbalance_bps
                .as_deref(),
        )
    })
    .map(decimal_abs)
}

fn cycle_near_miss_depth_abs_bps(cycle: &PaperCycleEntry) -> Option<Decimal> {
    parse_cycle_decimal(
        cycle
            .top_near_miss_exchange_book_depth_imbalance_bps
            .as_deref(),
    )
    .or_else(|| {
        parse_cycle_decimal(
            cycle
                .current_market_exchange_book_depth_imbalance_bps
                .as_deref(),
        )
    })
    .map(decimal_abs)
}

fn cycle_near_miss_book_age_ms(cycle: &PaperCycleEntry) -> Option<i64> {
    cycle
        .top_near_miss_exchange_book_age_ms
        .or(cycle.current_market_exchange_book_age_ms)
        .filter(|book_age_ms| *book_age_ms >= 0)
}

fn max_abs_decimal(values: [Option<&str>; 4]) -> Option<Decimal> {
    values
        .into_iter()
        .flatten()
        .filter_map(|value| parse_cycle_decimal(Some(value)))
        .map(decimal_abs)
        .fold(None, |best, value| match best {
            Some(best) if best >= value => Some(best),
            _ => Some(value),
        })
}

fn parse_cycle_decimal(value: Option<&str>) -> Option<Decimal> {
    value.and_then(|value| value.parse::<Decimal>().ok())
}

fn decimal_abs(value: Decimal) -> Decimal {
    if value < Decimal::ZERO { -value } else { value }
}

fn average_decimal_option(sum: Decimal, count: usize) -> Option<Decimal> {
    (count > 0).then(|| sum / Decimal::from(count as u64))
}

fn average_i64(sum: i64, count: usize) -> Option<Decimal> {
    (count > 0).then(|| Decimal::from(sum) / Decimal::from(count as u64))
}

fn render_count_table(title: &str, counts: &BTreeMap<String, usize>, top: usize) -> String {
    let mut output = String::new();
    let _ = writeln!(output, "{title}:");
    if counts.is_empty() || top == 0 {
        let _ = writeln!(output, "none");
        return output;
    }

    let mut rows = counts.iter().collect::<Vec<_>>();
    rows.sort_by(|left, right| right.1.cmp(left.1).then_with(|| left.0.cmp(right.0)));
    let _ = writeln!(output, "{:<6} Reason", "Count");
    let _ = writeln!(output, "{}", "-".repeat(88));
    for (reason, count) in rows.into_iter().take(top) {
        let _ = writeln!(output, "{:<6} {}", count, truncate_text_v2(reason, 76));
    }
    output
}

#[derive(Debug, Clone)]
struct PaperQualityTrade {
    open: PaperTradeEntry,
    close: PaperTradeEntry,
    mfe_usdc: Option<Decimal>,
    mae_usdc: Option<Decimal>,
}

#[derive(Debug, Clone, Default)]
struct PaperQualityBucketStats {
    close_count: usize,
    win_count: usize,
    loss_count: usize,
    realized_profit: Decimal,
    mfe_sum: Decimal,
    mfe_count: usize,
    mae_sum: Decimal,
    mae_count: usize,
}

impl PaperQualityBucketStats {
    fn record(&mut self, trade: &PaperQualityTrade) {
        self.close_count += 1;
        let pnl = trade.close.realized_profit_usdc.unwrap_or_default();
        self.realized_profit += pnl;
        if pnl > Decimal::ZERO {
            self.win_count += 1;
        } else if pnl < Decimal::ZERO {
            self.loss_count += 1;
        }
        if let Some(mfe) = trade.mfe_usdc {
            self.mfe_sum += mfe;
            self.mfe_count += 1;
        }
        if let Some(mae) = trade.mae_usdc {
            self.mae_sum += mae;
            self.mae_count += 1;
        }
    }

    fn expectancy(&self) -> Decimal {
        if self.close_count == 0 {
            Decimal::ZERO
        } else {
            (self.realized_profit / Decimal::from(self.close_count as u64)).round_dp(6)
        }
    }

    fn win_rate_pct(&self) -> Decimal {
        if self.close_count == 0 {
            Decimal::ZERO
        } else {
            (Decimal::from(self.win_count as u64) / Decimal::from(self.close_count as u64)
                * Decimal::from(100_u32))
            .round_dp(4)
        }
    }

    fn avg_mfe_usdc(&self) -> Option<Decimal> {
        average_decimal_count(self.mfe_sum, self.mfe_count)
    }

    fn avg_mae_usdc(&self) -> Option<Decimal> {
        average_decimal_count(self.mae_sum, self.mae_count)
    }
}

#[derive(Debug, Clone)]
struct PaperQualityReport {
    close_count: usize,
    win_count: usize,
    loss_count: usize,
    realized_profit: Decimal,
    expectancy: Decimal,
    win_rate_pct: Decimal,
    avg_mfe_usdc: Option<Decimal>,
    avg_mae_usdc: Option<Decimal>,
    by_entry_ask: BTreeMap<String, PaperQualityBucketStats>,
    by_seconds_left: BTreeMap<String, PaperQualityBucketStats>,
    by_target_gap: BTreeMap<String, PaperQualityBucketStats>,
}

fn pair_paper_quality_trades(
    trades: &[PaperTradeEntry],
    cycles: &[PaperCycleEntry],
) -> Vec<PaperQualityTrade> {
    let mut open_history = HashMap::<String, Vec<PaperTradeEntry>>::new();
    let mut paired = Vec::new();

    for trade in trades {
        match trade.action {
            PaperTradeAction::Open => {
                open_history
                    .entry(trade.slug.clone())
                    .or_default()
                    .push(trade.clone());
            }
            PaperTradeAction::Close => {
                if trade.realized_profit_usdc.is_none() {
                    continue;
                }
                if let Some(open) = latest_paper_open_for_close(&open_history, trade) {
                    let (mfe_usdc, mae_usdc) =
                        paper_quality_mfe_mae_for_trade(cycles, &open, trade);
                    paired.push(PaperQualityTrade {
                        open,
                        close: trade.clone(),
                        mfe_usdc,
                        mae_usdc,
                    });
                }
            }
        }
    }

    paired
}

fn latest_paper_open_for_close(
    open_history: &HashMap<String, Vec<PaperTradeEntry>>,
    close: &PaperTradeEntry,
) -> Option<PaperTradeEntry> {
    open_history.get(&close.slug).and_then(|opens| {
        opens
            .iter()
            .rev()
            .find(|open| open.recorded_at <= close.recorded_at)
            .cloned()
    })
}

fn paper_quality_mfe_mae_for_trade(
    cycles: &[PaperCycleEntry],
    open: &PaperTradeEntry,
    close: &PaperTradeEntry,
) -> (Option<Decimal>, Option<Decimal>) {
    let mut mfe_usdc = None;
    let mut mae_usdc = None;

    for cycle in cycles {
        if cycle.recorded_at < open.recorded_at || cycle.recorded_at > close.recorded_at {
            continue;
        }
        if cycle.worst_open_slug.as_deref() != Some(open.slug.as_str()) {
            continue;
        }
        let Some(mtm_profit) = cycle
            .worst_open_mtm_profit_usdc
            .as_deref()
            .and_then(|value| value.parse::<Decimal>().ok())
        else {
            continue;
        };

        mfe_usdc = Some(mfe_usdc.map_or(mtm_profit, |value: Decimal| value.max(mtm_profit)));
        mae_usdc = Some(mae_usdc.map_or(mtm_profit, |value: Decimal| value.min(mtm_profit)));
    }

    (mfe_usdc, mae_usdc)
}

fn limit_recent_quality_trades(
    mut trades: Vec<PaperQualityTrade>,
    limit: Option<usize>,
) -> Vec<PaperQualityTrade> {
    if let Some(limit) = limit
        && trades.len() > limit
    {
        let drain_len = trades.len() - limit;
        trades.drain(..drain_len);
    }
    trades
}

fn build_paper_quality_report(trades: &[PaperQualityTrade]) -> PaperQualityReport {
    let mut total = PaperQualityBucketStats::default();
    let mut by_entry_ask = BTreeMap::<String, PaperQualityBucketStats>::new();
    let mut by_seconds_left = BTreeMap::<String, PaperQualityBucketStats>::new();
    let mut by_target_gap = BTreeMap::<String, PaperQualityBucketStats>::new();

    for trade in trades {
        total.record(trade);
        by_entry_ask
            .entry(paper_quality_entry_ask_bucket(trade.open.primary_outcome_ask_price).to_owned())
            .or_default()
            .record(trade);
        by_seconds_left
            .entry(paper_quality_seconds_bucket(trade.open.seconds_left_at_entry).to_owned())
            .or_default()
            .record(trade);
        by_target_gap
            .entry(paper_quality_target_gap_bucket(trade.open.target_gap_bps).to_owned())
            .or_default()
            .record(trade);
    }

    PaperQualityReport {
        close_count: total.close_count,
        win_count: total.win_count,
        loss_count: total.loss_count,
        realized_profit: total.realized_profit.round_dp(6),
        expectancy: total.expectancy(),
        win_rate_pct: total.win_rate_pct(),
        avg_mfe_usdc: total.avg_mfe_usdc(),
        avg_mae_usdc: total.avg_mae_usdc(),
        by_entry_ask,
        by_seconds_left,
        by_target_gap,
    }
}

fn average_decimal_count(sum: Decimal, count: usize) -> Option<Decimal> {
    (count > 0).then(|| (sum / Decimal::from(count as u64)).round_dp(6))
}

fn paper_quality_entry_ask_bucket(value: Option<Decimal>) -> &'static str {
    match value {
        None => "unknown",
        Some(price) if price <= Decimal::new(56, 2) => "ask <= 0.56",
        Some(price) if price <= Decimal::new(62, 2) => "ask 0.56-0.62",
        Some(price) if price <= Decimal::new(68, 2) => "ask 0.62-0.68",
        Some(_) => "ask > 0.68",
    }
}

fn paper_quality_seconds_bucket(value: Option<i64>) -> &'static str {
    match value {
        None => "unknown",
        Some(seconds_left) if seconds_left > 270 => "sec > 270",
        Some(seconds_left) if seconds_left > 240 => "sec 241-270",
        Some(seconds_left) if seconds_left >= 180 => "sec 180-240",
        Some(_) => "sec < 180",
    }
}

fn paper_quality_target_gap_bucket(value: Option<Decimal>) -> &'static str {
    match value.map(|gap| gap.abs()) {
        None => "unknown",
        Some(gap) if gap < Decimal::new(150, 2) => "gap < 1.50",
        Some(gap) if gap < Decimal::new(300, 2) => "gap 1.50-3.00",
        Some(gap) if gap < Decimal::new(600, 2) => "gap 3.00-6.00",
        Some(_) => "gap >= 6.00",
    }
}

fn render_paper_quality_buckets(
    title: &str,
    buckets: &BTreeMap<String, PaperQualityBucketStats>,
) -> String {
    let mut output = String::new();
    let _ = writeln!(output, "{title}:");
    let _ = writeln!(
        output,
        "{:<18} {:>6} {:>6} {:>6} {:>10} {:>10} {:>10} {:>10}",
        "Bucket", "Close", "Win", "Loss", "PnL", "Exp", "AvgMFE", "AvgMAE"
    );
    let _ = writeln!(output, "{}", "-".repeat(86));
    for (bucket, stats) in buckets {
        let _ = writeln!(
            output,
            "{:<18} {:>6} {:>6} {:>6} {:>10} {:>10} {:>10} {:>10}",
            bucket,
            stats.close_count,
            stats.win_count,
            stats.loss_count,
            format_signed_decimal(stats.realized_profit.round_dp(4)),
            format_signed_decimal(stats.expectancy().round_dp(4)),
            format_option_decimal(stats.avg_mfe_usdc(), 4),
            format_option_decimal(stats.avg_mae_usdc(), 4)
        );
    }
    output
}

fn render_paper_quality_table(trades: &[PaperQualityTrade]) -> String {
    let mut output = String::new();
    let _ = writeln!(
        output,
        "{:<3} {:<19} {:>7} {:>5} {:>8} {:>8} {:>8} {:>8} {:>8} {:>10} {:>10} {:>10} {:<18} Slug",
        "#",
        "EntryTime",
        "Ask",
        "Sec",
        "Gap",
        "1s",
        "5s",
        "Sig",
        "Flow",
        "PnL",
        "MFE",
        "MAE",
        "Close"
    );
    let _ = writeln!(output, "{}", "-".repeat(154));

    for (index, trade) in trades.iter().enumerate() {
        let close_label = trade
            .close
            .close_category
            .as_deref()
            .unwrap_or(trade.close.note.as_str());
        let _ = writeln!(
            output,
            "{:<3} {:<19} {:>7} {:>5} {:>8} {:>8} {:>8} {:>8} {:>8} {:>10} {:>10} {:>10} {:<18} {}",
            index + 1,
            trade
                .open
                .recorded_at
                .with_timezone(&Local)
                .format("%Y-%m-%d %H:%M:%S"),
            format_option_decimal(trade.open.primary_outcome_ask_price, 4),
            trade
                .open
                .seconds_left_at_entry
                .map_or_else(|| "-".to_owned(), |value| value.to_string()),
            format_option_decimal(trade.open.target_gap_bps, 2),
            format_option_decimal(trade.open.spot_move_1s_bps, 2),
            format_option_decimal(trade.open.spot_move_5s_bps, 2),
            format_option_decimal(trade.open.signal_strength_bps, 2),
            format_option_decimal(trade.open.aligned_trade_flow_bps, 2),
            trade.close.realized_profit_usdc.map_or_else(
                || "-".to_owned(),
                |value| format_signed_decimal(value.round_dp(4))
            ),
            format_option_decimal(trade.mfe_usdc, 4),
            format_option_decimal(trade.mae_usdc, 4),
            truncate_text_v2(close_label, 18),
            trade.open.slug
        );
    }

    output
}

fn format_option_decimal(value: Option<Decimal>, dp: u32) -> String {
    value.map_or_else(
        || "-".to_owned(),
        |decimal| decimal.round_dp(dp).to_string(),
    )
}

#[derive(Debug, Clone, Default)]
struct KindPostTradeStats {
    close_count: usize,
    win_count: usize,
    loss_count: usize,
    realized_profit: Decimal,
}

#[derive(Debug, Clone)]
struct PostTradeReport {
    open_count: usize,
    close_count: usize,
    win_count: usize,
    loss_count: usize,
    realized_profit: Decimal,
    win_rate_pct: Decimal,
    expectancy: Decimal,
    profit_factor: Decimal,
    max_drawdown: Decimal,
    by_kind: BTreeMap<String, KindPostTradeStats>,
}

fn build_post_trade_report(trades: &[PaperTradeEntry]) -> PostTradeReport {
    let open_count = trades
        .iter()
        .filter(|entry| entry.action == PaperTradeAction::Open)
        .count();
    let close_entries = trades
        .iter()
        .filter(|entry| entry.action == PaperTradeAction::Close)
        .filter_map(|entry| {
            entry
                .realized_profit_usdc
                .map(|profit| (entry.kind.as_str(), profit))
        })
        .collect::<Vec<_>>();
    let close_count = close_entries.len();

    let mut realized_profit = Decimal::ZERO;
    let mut gross_profit = Decimal::ZERO;
    let mut gross_loss_abs = Decimal::ZERO;
    let mut running_profit = Decimal::ZERO;
    let mut peak_profit = Decimal::ZERO;
    let mut max_drawdown = Decimal::ZERO;
    let mut by_kind = BTreeMap::<String, KindPostTradeStats>::new();
    let mut win_count = 0_usize;
    let mut loss_count = 0_usize;

    for (kind, profit) in close_entries {
        realized_profit += profit;
        running_profit += profit;
        if running_profit > peak_profit {
            peak_profit = running_profit;
        }
        let drawdown = peak_profit - running_profit;
        if drawdown > max_drawdown {
            max_drawdown = drawdown;
        }

        if profit > Decimal::ZERO {
            win_count += 1;
            gross_profit += profit;
        } else if profit < Decimal::ZERO {
            loss_count += 1;
            gross_loss_abs += -profit;
        }

        let stats = by_kind.entry(kind.to_owned()).or_default();
        stats.close_count += 1;
        stats.realized_profit += profit;
        if profit > Decimal::ZERO {
            stats.win_count += 1;
        } else if profit < Decimal::ZERO {
            stats.loss_count += 1;
        }
    }

    let win_rate_pct = if close_count == 0 {
        Decimal::ZERO
    } else {
        (Decimal::from(win_count as u64) / Decimal::from(close_count as u64)
            * Decimal::from(100_u32))
        .round_dp(4)
    };
    let expectancy = if close_count == 0 {
        Decimal::ZERO
    } else {
        (realized_profit / Decimal::from(close_count as u64)).round_dp(6)
    };
    let profit_factor = if gross_loss_abs <= Decimal::ZERO {
        if gross_profit <= Decimal::ZERO {
            Decimal::ZERO
        } else {
            Decimal::new(9999, 0)
        }
    } else {
        (gross_profit / gross_loss_abs).round_dp(6)
    };

    PostTradeReport {
        open_count,
        close_count,
        win_count,
        loss_count,
        realized_profit: realized_profit.round_dp(6),
        win_rate_pct,
        expectancy,
        profit_factor,
        max_drawdown: max_drawdown.round_dp(6),
        by_kind,
    }
}

fn render_post_trade_report(report: &PostTradeReport) -> String {
    let mut output = String::new();
    let _ = writeln!(output, "Post-trade :");
    let _ = writeln!(
        output,
        "Open: {} | Close: {} | Win: {} | Loss: {} | WinRate: {}% | Expectancy: {} | ProfitFactor: {} | MaxDD: {} | Realized: {}",
        report.open_count,
        report.close_count,
        report.win_count,
        report.loss_count,
        report.win_rate_pct.round_dp(2),
        report.expectancy.round_dp(4),
        report.profit_factor.round_dp(4),
        report.max_drawdown.round_dp(4),
        report.realized_profit.round_dp(4)
    );

    if report.by_kind.is_empty() {
        return output;
    }

    let _ = writeln!(output, "\\n :");
    let _ = writeln!(
        output,
        "{:<12} {:>6} {:>6} {:>6} {:>9} {:>9}",
        "Kind", "Close", "Win", "Loss", "WinRate", "PnL"
    );
    let _ = writeln!(output, "{}", "-".repeat(58));
    for (kind, stats) in &report.by_kind {
        let win_rate = if stats.close_count == 0 {
            Decimal::ZERO
        } else {
            (Decimal::from(stats.win_count as u64) / Decimal::from(stats.close_count as u64)
                * Decimal::from(100_u32))
            .round_dp(2)
        };
        let _ = writeln!(
            output,
            "{:<12} {:>6} {:>6} {:>6} {:>8}% {:>9}",
            kind,
            stats.close_count,
            stats.win_count,
            stats.loss_count,
            win_rate,
            stats.realized_profit.round_dp(4)
        );
    }

    output
}

fn show_paper_positions(config: &AppConfig) -> Result<()> {
    let journal = JournalStore::new(&config.storage)?;
    let snapshot = journal.load_snapshot()?;
    let positions = snapshot
        .paper_state
        .open_positions
        .values()
        .cloned()
        .collect::<Vec<_>>();
    if positions.is_empty() {
        println!("No open paper positions.");
        return Ok(());
    }

    let open_notional = positions
        .iter()
        .map(|position| position.spent_usdc)
        .sum::<Decimal>()
        .round_dp(4);
    println!(
        "Paper positions: open_positions={} open_notional={}",
        positions.len(),
        open_notional
    );
    println!("\n{}", render_paper_position_table(&positions));
    Ok(())
}

async fn settle_resolved_paper_positions(
    config: &AppConfig,
    journal: &JournalStore,
    binance_client: &BinanceClient,
    paper: &PaperExecutor,
    executed_market_slugs: &HashSet<String>,
    journal_snapshot: &mut PnlSnapshot,
    paper_journal: Option<&PaperJournalWriter>,
) -> Result<Vec<PaperCloseReport>> {
    let snapshot = paper.snapshot().await;
    if snapshot.open_positions.is_empty() {
        return Ok(Vec::new());
    }

    let open_slugs = snapshot.open_positions.keys().cloned().collect::<Vec<_>>();
    let mut close_reports = Vec::new();
    for slug in open_slugs {
        let resolution = if should_use_fast_paper_settlement(config) {
            match binance_client.resolution_from_slug_live_cache(&slug).await {
                Some(resolution) => resolution,
                None => continue,
            }
        } else {
            match binance_client.resolution_from_slug(&slug).await {
                Ok(Some(resolution)) => resolution,
                Ok(None) => continue,
                Err(error) => {
                    warn!(
                        slug = %slug,
                        error = %error,
                        "paper settlement lookup failed; keeping position open for retry"
                    );
                    continue;
                }
            }
        };
        let Some(close_report) = paper
            .close_position(&slug, &resolution, WINDOW_SETTLEMENT_REASON)
            .await
        else {
            continue;
        };
        close_reports.push(close_report);
    }

    if close_reports.is_empty() {
        return Ok(Vec::new());
    }

    let paper_state = paper.snapshot().await;
    journal.update_snapshot_in_place(journal_snapshot, &paper_state, executed_market_slugs)?;
    for close_report in &close_reports {
        let close_trade = build_paper_close_trade(close_report);
        if let Some(writer) = paper_journal {
            writer.record_trade(close_trade)?;
        } else {
            journal.record_paper_trade(&close_trade)?;
        }
        log_paper_close(close_report, &paper_state);
    }
    flush_paper_journal_if_needed(paper_journal)?;

    Ok(close_reports)
}

fn should_use_fast_paper_settlement(config: &AppConfig) -> bool {
    config.run.reactive && should_use_live_only_polymarket_books(config)
}

#[allow(clippy::too_many_arguments)]
async fn close_paper_positions_early(
    journal: &JournalStore,
    paper: &PaperExecutor,
    snapshot: &MarketSnapshot,
    exit_config: &EarlyExitConfig,
    paper_cost_model: PaperCostModel,
    executed_market_slugs: &HashSet<String>,
    journal_snapshot: &mut PnlSnapshot,
    paper_journal: Option<&PaperJournalWriter>,
) -> Result<Vec<PaperCloseReport>> {
    if !exit_config.enabled {
        return Ok(Vec::new());
    }

    let paper_state = paper.snapshot().await;
    if paper_state.open_positions.is_empty() {
        return Ok(Vec::new());
    }

    let mut candidates = Vec::new();
    for position in paper_state.open_positions.values() {
        let Some(context) = snapshot.contexts.get(&position.slug) else {
            continue;
        };
        if let Some((reason, close_fraction)) = directional_partial_exit_plan(
            position,
            context,
            &snapshot.books,
            exit_config,
            paper_cost_model,
        ) {
            candidates.push((position.slug.clone(), reason, Some(close_fraction)));
            continue;
        }

        if let Some((reason, close_fraction)) = micro_breakout_partial_exit_plan(
            position,
            context,
            &snapshot.books,
            exit_config,
            paper_cost_model,
        ) {
            candidates.push((position.slug.clone(), reason, Some(close_fraction)));
            continue;
        }

        if let Some((reason, close_fraction)) = profit_lock_partial_exit_plan(
            position,
            context,
            &snapshot.books,
            exit_config,
            paper_cost_model,
        ) {
            candidates.push((position.slug.clone(), reason, Some(close_fraction)));
            continue;
        }

        if let Some((reason, close_fraction)) = peak_exit_partial_plan(
            position,
            context,
            &snapshot.books,
            exit_config,
            paper_cost_model,
        ) {
            candidates.push((position.slug.clone(), reason, Some(close_fraction)));
            continue;
        }

        let Some(reason) = early_exit_reason(
            position,
            context,
            &snapshot.books,
            exit_config,
            paper_cost_model,
        ) else {
            continue;
        };
        candidates.push((position.slug.clone(), reason, None));
    }

    if candidates.is_empty() {
        return Ok(Vec::new());
    }

    let mut close_reports = Vec::new();
    for (slug, reason, close_fraction) in candidates {
        let close_report = match close_fraction {
            Some(fraction) => {
                paper
                    .close_position_mark_to_market_partial(
                        &slug,
                        &snapshot.books,
                        &reason,
                        fraction,
                    )
                    .await
            }
            None => {
                paper
                    .close_position_mark_to_market(&slug, &snapshot.books, &reason)
                    .await
            }
        };
        let Some(close_report) = close_report else {
            continue;
        };
        close_reports.push(close_report);
    }

    if close_reports.is_empty() {
        return Ok(Vec::new());
    }

    let paper_state = paper.snapshot().await;
    journal.update_snapshot_in_place(journal_snapshot, &paper_state, executed_market_slugs)?;
    for close_report in &close_reports {
        let close_trade = build_paper_close_trade(close_report);
        if let Some(writer) = paper_journal {
            writer.record_trade(close_trade)?;
        } else {
            journal.record_paper_trade(&close_trade)?;
        }
        log_paper_close(close_report, &paper_state);
    }
    flush_paper_journal_if_needed(paper_journal)?;

    Ok(close_reports)
}

fn flush_paper_journal_if_needed(paper_journal: Option<&PaperJournalWriter>) -> Result<()> {
    if let Some(writer) = paper_journal {
        writer.flush()?;
    }
    Ok(())
}

fn early_exit_reason(
    position: &PaperPosition,
    context: &BtcFiveMinuteContext,
    books: &HashMap<String, OrderBook>,
    exit_config: &EarlyExitConfig,
    paper_cost_model: PaperCostModel,
) -> Option<String> {
    if matches!(position.kind, OpportunityKind::BundleArbitrage) {
        return None;
    }

    let held_seconds = Utc::now()
        .signed_duration_since(position.opened_at)
        .num_seconds()
        .max(0);
    if held_seconds < exit_config.min_hold_secs {
        return None;
    }

    let primary_side = paper_position_primary_side(position);
    if primary_side == PaperOutcomeSide::Unknown {
        return None;
    }

    let mark_to_market_profit =
        paper_position_net_mark_to_market_profit(position, books, paper_cost_model);
    if let Some(reason) =
        hard_stop_loss_reason(context, exit_config, primary_side, mark_to_market_profit)
    {
        return Some(reason);
    }

    if let Some(reason) = scalp_exit_reason(
        position,
        context,
        books,
        exit_config,
        primary_side,
        held_seconds,
        mark_to_market_profit,
    ) {
        return Some(reason);
    }

    if let Some(reason) = peak_or_exhaustion_exit_reason(
        position,
        context,
        books,
        exit_config,
        primary_side,
        mark_to_market_profit,
    ) {
        return Some(reason);
    }

    let aligned_micro_move_bps = aligned_move_bps(context.spot_move_5s_bps, primary_side);
    let aligned_momentum_move_bps = aligned_move_bps(context.spot_move_15s_bps, primary_side);

    if exit_config.max_loss_usdc > Decimal::ZERO
        && !is_micro_breakout_kind(position.kind)
        && mark_to_market_profit <= -exit_config.max_loss_usdc
        && aligned_micro_move_bps <= -exit_config.reversal_min_5s_bps
        && (!is_bonereaper_state_v2_kind(position.kind)
            || exit_config.bonereaper_state_v2_stop_loss_min_15s_bps <= Decimal::ZERO
            || aligned_momentum_move_bps <= -exit_config.bonereaper_state_v2_stop_loss_min_15s_bps)
        && (!is_bonereaper_state_guarded_kind(position.kind)
            || aligned_momentum_move_bps <= -exit_config.directional_partial_reversal_15s_bps)
    {
        return Some(format!(
            "early-exit stop-loss: mtm {} USDC, aligned 5s {} bps, aligned 15s {} bps",
            mark_to_market_profit.round_dp(4),
            aligned_micro_move_bps.round_dp(2),
            aligned_momentum_move_bps.round_dp(2)
        ));
    }

    let capture_threshold = position.expected_profit_usdc
        * exit_config
            .min_expected_profit_capture_ratio
            .max(Decimal::ZERO);
    let take_profit_threshold = exit_config.min_take_profit_usdc.max(capture_threshold);
    if mark_to_market_profit >= take_profit_threshold && aligned_micro_move_bps <= Decimal::ZERO {
        return Some(format!(
            "early-exit take-profit: mtm {} USDC captured, aligned 5s {} bps",
            mark_to_market_profit.round_dp(4),
            aligned_micro_move_bps.round_dp(2)
        ));
    }

    if exit_config.near_expiry_secs > 0
        && context.seconds_left <= exit_config.near_expiry_secs
        && mark_to_market_profit > Decimal::ZERO
        && aligned_micro_move_bps <= Decimal::ZERO
    {
        return Some(format!(
            "early-exit time-decay: {}s left, mtm {} USDC",
            context.seconds_left,
            mark_to_market_profit.round_dp(4)
        ));
    }

    if aligned_micro_move_bps <= -exit_config.reversal_min_5s_bps
        && aligned_momentum_move_bps <= Decimal::ZERO
        && (!is_bonereaper_state_v2_kind(position.kind)
            || exit_config.bonereaper_state_v2_reversal_min_15s_bps <= Decimal::ZERO
            || aligned_momentum_move_bps <= -exit_config.bonereaper_state_v2_reversal_min_15s_bps)
    {
        if is_partial_reversal_enabled(position.kind, exit_config)
            && position.partial_reversal_exits == 0
        {
            return None;
        }

        return Some(format!(
            "early-exit reversal: aligned 5s {} bps, aligned 15s {} bps, mtm {} USDC",
            aligned_micro_move_bps.round_dp(2),
            aligned_momentum_move_bps.round_dp(2),
            mark_to_market_profit.round_dp(4)
        ));
    }

    None
}

fn is_partial_reversal_enabled(kind: OpportunityKind, exit_config: &EarlyExitConfig) -> bool {
    (is_directional_kind(kind) && exit_config.directional_partial_reversal_enabled)
        || (is_micro_breakout_kind(kind) && exit_config.micro_breakout_partial_reversal_enabled)
}

fn directional_partial_exit_plan(
    position: &PaperPosition,
    context: &BtcFiveMinuteContext,
    books: &HashMap<String, OrderBook>,
    exit_config: &EarlyExitConfig,
    paper_cost_model: PaperCostModel,
) -> Option<(String, Decimal)> {
    if !exit_config.directional_partial_reversal_enabled
        || !is_directional_kind(position.kind)
        || position.partial_reversal_exits > 0
    {
        return None;
    }

    let held_seconds = Utc::now()
        .signed_duration_since(position.opened_at)
        .num_seconds()
        .max(0);
    if held_seconds < exit_config.min_hold_secs {
        return None;
    }

    if exit_config.near_expiry_secs > 0 && context.seconds_left <= exit_config.near_expiry_secs {
        return None;
    }

    let primary_side = paper_position_primary_side(position);
    if primary_side == PaperOutcomeSide::Unknown {
        return None;
    }

    let aligned_micro_move_bps = aligned_move_bps(context.spot_move_5s_bps, primary_side);
    let aligned_momentum_move_bps = aligned_move_bps(context.spot_move_15s_bps, primary_side);
    if aligned_micro_move_bps > -exit_config.directional_partial_reversal_5s_bps
        || aligned_momentum_move_bps > -exit_config.directional_partial_reversal_15s_bps
    {
        return None;
    }

    let mark_to_market_profit =
        paper_position_net_mark_to_market_profit(position, books, paper_cost_model);
    if let Some(reason) =
        hard_stop_loss_reason(context, exit_config, primary_side, mark_to_market_profit)
    {
        return Some((reason, Decimal::ONE));
    }
    Some((
        format!(
            "early-exit partial-reversal: close {}%, aligned 5s {} bps, aligned 15s {} bps, mtm {} USDC",
            (exit_config.directional_partial_close_ratio * Decimal::new(100, 0)).round_dp(1),
            aligned_micro_move_bps.round_dp(2),
            aligned_momentum_move_bps.round_dp(2),
            mark_to_market_profit.round_dp(4)
        ),
        exit_config.directional_partial_close_ratio,
    ))
}

fn micro_breakout_partial_exit_plan(
    position: &PaperPosition,
    context: &BtcFiveMinuteContext,
    books: &HashMap<String, OrderBook>,
    exit_config: &EarlyExitConfig,
    paper_cost_model: PaperCostModel,
) -> Option<(String, Decimal)> {
    if !exit_config.micro_breakout_partial_reversal_enabled
        || !is_micro_breakout_kind(position.kind)
        || position.partial_reversal_exits > 0
    {
        return None;
    }

    let held_seconds = Utc::now()
        .signed_duration_since(position.opened_at)
        .num_seconds()
        .max(0);
    if held_seconds < exit_config.min_hold_secs {
        return None;
    }

    let primary_side = paper_position_primary_side(position);
    if primary_side == PaperOutcomeSide::Unknown {
        return None;
    }

    let mark_to_market_profit =
        paper_position_net_mark_to_market_profit(position, books, paper_cost_model);
    if let Some(reason) =
        hard_stop_loss_reason(context, exit_config, primary_side, mark_to_market_profit)
    {
        return Some((reason, Decimal::ONE));
    }

    let aligned_burst_move_bps = aligned_move_bps(context.spot_move_1s_bps, primary_side);
    let aligned_micro_move_bps = aligned_move_bps(context.spot_move_5s_bps, primary_side);
    let aligned_momentum_move_bps = aligned_move_bps(context.spot_move_15s_bps, primary_side);
    if aligned_micro_move_bps > -exit_config.micro_breakout_partial_reversal_5s_bps
        || aligned_momentum_move_bps > -exit_config.micro_breakout_partial_reversal_15s_bps
    {
        return None;
    }

    let burst_fail_fast_enabled = exit_config.micro_breakout_fail_fast_1s_bps > Decimal::ZERO;
    let burst_fail_fast_close = burst_fail_fast_enabled
        && aligned_burst_move_bps <= -exit_config.micro_breakout_fail_fast_1s_bps
        && aligned_micro_move_bps <= -exit_config.micro_breakout_partial_reversal_5s_bps
        && (exit_config.micro_breakout_fail_fast_15s_bps <= Decimal::ZERO
            || aligned_momentum_move_bps <= -exit_config.micro_breakout_fail_fast_15s_bps)
        && mark_to_market_profit <= exit_config.micro_breakout_fail_fast_profit_buffer_usdc;
    let fail_fast_close = burst_fail_fast_close
        || (mark_to_market_profit <= Decimal::ZERO
            && aligned_micro_move_bps <= -exit_config.reversal_min_5s_bps
            && aligned_momentum_move_bps <= -exit_config.micro_breakout_partial_reversal_15s_bps);
    let close_fraction = if fail_fast_close {
        Decimal::ONE
    } else {
        exit_config.micro_breakout_partial_close_ratio
    };
    Some((
        format!(
            "early-exit partial-reversal (micro): close {}%, aligned 1s {} bps, aligned 5s {} bps, aligned 15s {} bps, mtm {} USDC",
            (close_fraction * Decimal::new(100, 0)).round_dp(1),
            aligned_burst_move_bps.round_dp(2),
            aligned_micro_move_bps.round_dp(2),
            aligned_momentum_move_bps.round_dp(2),
            mark_to_market_profit.round_dp(4)
        ),
        close_fraction,
    ))
}

fn profit_lock_partial_exit_plan(
    position: &PaperPosition,
    context: &BtcFiveMinuteContext,
    books: &HashMap<String, OrderBook>,
    exit_config: &EarlyExitConfig,
    paper_cost_model: PaperCostModel,
) -> Option<(String, Decimal)> {
    if !exit_config.profit_lock_partial_close_enabled
        || !is_directional_kind(position.kind)
        || position.partial_reversal_exits > 0
    {
        return None;
    }

    let held_seconds = Utc::now()
        .signed_duration_since(position.opened_at)
        .num_seconds()
        .max(0);
    if held_seconds < exit_config.min_hold_secs {
        return None;
    }

    if exit_config.near_expiry_secs > 0 && context.seconds_left <= exit_config.near_expiry_secs {
        return None;
    }

    let mark_to_market_profit =
        paper_position_net_mark_to_market_profit(position, books, paper_cost_model);
    if mark_to_market_profit < exit_config.profit_lock_min_profit_usdc {
        return None;
    }

    let primary_side = paper_position_primary_side(position);
    if primary_side == PaperOutcomeSide::Unknown {
        return None;
    }

    let aligned_1s_bps = aligned_move_bps(context.spot_move_1s_bps, primary_side);
    let aligned_5s_bps = aligned_move_bps(context.spot_move_5s_bps, primary_side);
    let aligned_momentum_bps = aligned_move_bps(context.spot_move_15s_bps, primary_side);
    Some((
        format!(
            "early-exit partial profit-lock: close {}%, mtm {} USDC, aligned 1s {} bps, aligned 5s {} bps, aligned 15s {} bps",
            (exit_config.profit_lock_partial_close_ratio * Decimal::new(100, 0)).round_dp(1),
            mark_to_market_profit.round_dp(4),
            aligned_1s_bps.round_dp(2),
            aligned_5s_bps.round_dp(2),
            aligned_momentum_bps.round_dp(2),
        ),
        exit_config.profit_lock_partial_close_ratio,
    ))
}

fn peak_exit_partial_plan(
    position: &PaperPosition,
    context: &BtcFiveMinuteContext,
    books: &HashMap<String, OrderBook>,
    exit_config: &EarlyExitConfig,
    paper_cost_model: PaperCostModel,
) -> Option<(String, Decimal)> {
    if !exit_config.peak_exit_enabled
        || !exit_config.peak_exit_partial_close_enabled
        || !is_directional_kind(position.kind)
        || position.partial_reversal_exits > 0
    {
        return None;
    }

    let held_seconds = Utc::now()
        .signed_duration_since(position.opened_at)
        .num_seconds()
        .max(0);
    if held_seconds < exit_config.min_hold_secs {
        return None;
    }

    if exit_config.near_expiry_secs > 0 && context.seconds_left <= exit_config.near_expiry_secs {
        return None;
    }

    let primary_side = paper_position_primary_side(position);
    if primary_side == PaperOutcomeSide::Unknown {
        return None;
    }

    let mark_to_market_profit =
        paper_position_net_mark_to_market_profit(position, books, paper_cost_model);
    if mark_to_market_profit < exit_config.peak_exit_min_profit_usdc {
        return None;
    }

    let aligned_1s_bps = aligned_move_bps(context.spot_move_1s_bps, primary_side);
    let aligned_5s_bps = aligned_move_bps(context.spot_move_5s_bps, primary_side);
    let aligned_acceleration_bps = aligned_move_bps(context.micro_acceleration_bps, primary_side);
    let primary_ask_price = paper_position_primary_book(position, books, primary_side)
        .and_then(OrderBook::best_ask)
        .map(|level| level.price.round_dp(4));

    if primary_ask_price.is_none_or(|price| price < exit_config.peak_exit_min_primary_ask_price)
        || aligned_1s_bps > exit_config.peak_exit_max_aligned_1s_bps
        || aligned_5s_bps > exit_config.peak_exit_max_aligned_5s_bps
        || aligned_acceleration_bps > exit_config.peak_exit_max_acceleration_bps
    {
        return None;
    }

    Some((
        format!(
            "early-exit partial peak-exit: close {}%, mtm {} USDC, ask {}, aligned 1s {} bps, aligned 5s {} bps, acceleration {} bps",
            (exit_config.peak_exit_partial_close_ratio * Decimal::new(100, 0)).round_dp(1),
            mark_to_market_profit.round_dp(4),
            display_optional_decimal(primary_ask_price),
            aligned_1s_bps.round_dp(2),
            aligned_5s_bps.round_dp(2),
            aligned_acceleration_bps.round_dp(2),
        ),
        exit_config.peak_exit_partial_close_ratio,
    ))
}

const fn is_directional_kind(kind: OpportunityKind) -> bool {
    matches!(
        kind,
        OpportunityKind::DirectionalMomentum
            | OpportunityKind::DirectionalMomentumHedged
            | OpportunityKind::TargetStateV1
            | OpportunityKind::BonereaperStateV1
            | OpportunityKind::BonereaperStateV2
            | OpportunityKind::BonereaperStateGuarded
            | OpportunityKind::CodexSentinelV1
            | OpportunityKind::CodexScalpProbeV1
    )
}

const fn is_bonereaper_state_v2_kind(kind: OpportunityKind) -> bool {
    matches!(
        kind,
        OpportunityKind::BonereaperStateV2
            | OpportunityKind::CodexSentinelV1
            | OpportunityKind::CodexScalpProbeV1
    )
}

const fn is_bonereaper_state_guarded_kind(kind: OpportunityKind) -> bool {
    matches!(kind, OpportunityKind::BonereaperStateGuarded)
}

const fn is_micro_breakout_kind(kind: OpportunityKind) -> bool {
    matches!(kind, OpportunityKind::MicroBreakout)
}

fn paper_position_mark_to_market_payout(
    position: &PaperPosition,
    books: &HashMap<String, OrderBook>,
) -> Decimal {
    mark_to_market_payout_for_legs(&position.legs, books)
}

fn paper_position_net_mark_to_market_profit(
    position: &PaperPosition,
    books: &HashMap<String, OrderBook>,
    paper_cost_model: PaperCostModel,
) -> Decimal {
    let gross_payout = paper_position_mark_to_market_payout(position, books).round_dp(6);
    let (net_payout, _, _) = paper_cost_model.net_exit_payout(gross_payout);
    (net_payout - position.spent_usdc).round_dp(6)
}

fn hard_stop_loss_reason(
    context: &BtcFiveMinuteContext,
    exit_config: &EarlyExitConfig,
    primary_side: PaperOutcomeSide,
    mark_to_market_profit: Decimal,
) -> Option<String> {
    if exit_config.max_loss_usdc <= Decimal::ZERO
        || mark_to_market_profit > -exit_config.max_loss_usdc
    {
        return None;
    }

    let aligned_burst_move_bps = aligned_move_bps(context.spot_move_1s_bps, primary_side);
    let aligned_micro_move_bps = aligned_move_bps(context.spot_move_5s_bps, primary_side);
    let aligned_momentum_move_bps = aligned_move_bps(context.spot_move_15s_bps, primary_side);
    Some(format!(
        "early-exit hard stop-loss: net mtm {} USDC, aligned 1s {} bps, aligned 5s {} bps, aligned 15s {} bps",
        mark_to_market_profit.round_dp(4),
        aligned_burst_move_bps.round_dp(2),
        aligned_micro_move_bps.round_dp(2),
        aligned_momentum_move_bps.round_dp(2)
    ))
}

fn scalp_exit_reason(
    position: &PaperPosition,
    context: &BtcFiveMinuteContext,
    books: &HashMap<String, OrderBook>,
    exit_config: &EarlyExitConfig,
    primary_side: PaperOutcomeSide,
    held_seconds: i64,
    mark_to_market_profit: Decimal,
) -> Option<String> {
    if !exit_config.scalp_exit_enabled
        || matches!(position.kind, OpportunityKind::BundleArbitrage)
        || (exit_config.scalp_exit_apply_to_codex_sentinel_only
            && !matches!(
                position.kind,
                OpportunityKind::CodexSentinelV1 | OpportunityKind::CodexScalpProbeV1
            ))
    {
        return None;
    }

    let (entry_price, exit_price) =
        paper_position_primary_entry_and_exit_price(position, books, primary_side)?;
    let price_delta = (exit_price - entry_price).round_dp(6);

    if exit_config.scalp_take_profit_price_delta > Decimal::ZERO
        && price_delta >= exit_config.scalp_take_profit_price_delta
        && mark_to_market_profit > Decimal::ZERO
    {
        return Some(format!(
            "early-exit scalp take-profit: entry {}, exit {}, delta {}, mtm {} USDC, held {}s",
            entry_price.round_dp(4),
            exit_price.round_dp(4),
            price_delta.round_dp(4),
            mark_to_market_profit.round_dp(4),
            held_seconds
        ));
    }

    if exit_config.near_expiry_secs > 0 && context.seconds_left <= exit_config.near_expiry_secs {
        return Some(format!(
            "early-exit scalp near-expiry: {}s left, entry {}, exit {}, delta {}, mtm {} USDC, held {}s",
            context.seconds_left,
            entry_price.round_dp(4),
            exit_price.round_dp(4),
            price_delta.round_dp(4),
            mark_to_market_profit.round_dp(4),
            held_seconds
        ));
    }

    let aligned_gap_bps = aligned_move_bps(context.target_gap_bps, primary_side);
    let aligned_5s_bps = aligned_move_bps(context.spot_move_5s_bps, primary_side);
    if exit_config.scalp_invalidation_exit_enabled
        && mark_to_market_profit <= -exit_config.scalp_invalidation_min_loss_usdc
        && aligned_gap_bps <= -exit_config.scalp_invalidation_opposite_gap_bps
        && aligned_5s_bps <= -exit_config.scalp_invalidation_opposite_5s_bps
    {
        return Some(format!(
            "early-exit scalp signal-invalidation: entry {}, exit {}, delta {}, mtm {} USDC, aligned gap {} bps, aligned 5s {} bps, held {}s",
            entry_price.round_dp(4),
            exit_price.round_dp(4),
            price_delta.round_dp(4),
            mark_to_market_profit.round_dp(4),
            aligned_gap_bps.round_dp(2),
            aligned_5s_bps.round_dp(2),
            held_seconds
        ));
    }

    if exit_config.scalp_stop_loss_price_delta > Decimal::ZERO
        && price_delta <= -exit_config.scalp_stop_loss_price_delta
    {
        return Some(format!(
            "early-exit scalp stop-loss: entry {}, exit {}, delta {}, mtm {} USDC, held {}s",
            entry_price.round_dp(4),
            exit_price.round_dp(4),
            price_delta.round_dp(4),
            mark_to_market_profit.round_dp(4),
            held_seconds
        ));
    }

    if exit_config.scalp_time_stop_secs > 0 && held_seconds >= exit_config.scalp_time_stop_secs {
        return Some(format!(
            "early-exit scalp time-stop: entry {}, exit {}, delta {}, mtm {} USDC, held {}s",
            entry_price.round_dp(4),
            exit_price.round_dp(4),
            price_delta.round_dp(4),
            mark_to_market_profit.round_dp(4),
            held_seconds
        ));
    }

    None
}

fn paper_position_primary_entry_and_exit_price(
    position: &PaperPosition,
    books: &HashMap<String, OrderBook>,
    primary_side: PaperOutcomeSide,
) -> Option<(Decimal, Decimal)> {
    let leg = paper_position_primary_leg(position, primary_side)?;
    if leg.shares <= Decimal::ZERO {
        return None;
    }

    let book = books.get(&leg.token_id)?;
    let coverable_bid_shares = book
        .bids
        .iter()
        .rev()
        .take(MAX_MARK_TO_MARKET_BID_LEVELS)
        .filter(|level| level.price > Decimal::ZERO && level.size > Decimal::ZERO)
        .map(|level| level.size)
        .sum::<Decimal>();
    if coverable_bid_shares < leg.shares {
        return None;
    }

    let gross_payout = mark_to_market_payout_for_legs(std::slice::from_ref(leg), books);
    if gross_payout <= Decimal::ZERO {
        return None;
    }

    Some((leg.entry_price, (gross_payout / leg.shares).round_dp(6)))
}

fn paper_position_primary_leg(
    position: &PaperPosition,
    primary_side: PaperOutcomeSide,
) -> Option<&PaperPositionLeg> {
    position
        .legs
        .iter()
        .filter(|leg| leg.side == primary_side)
        .max_by(|left, right| {
            left.shares
                .partial_cmp(&right.shares)
                .unwrap_or(Ordering::Equal)
        })
}

fn peak_or_exhaustion_exit_reason(
    position: &PaperPosition,
    context: &BtcFiveMinuteContext,
    books: &HashMap<String, OrderBook>,
    exit_config: &EarlyExitConfig,
    primary_side: PaperOutcomeSide,
    mark_to_market_profit: Decimal,
) -> Option<String> {
    if (!exit_config.peak_exit_enabled && !exit_config.exhaustion_exit_enabled)
        || matches!(position.kind, OpportunityKind::BundleArbitrage)
    {
        return None;
    }

    let aligned_1s_bps = aligned_move_bps(context.spot_move_1s_bps, primary_side);
    let aligned_5s_bps = aligned_move_bps(context.spot_move_5s_bps, primary_side);
    let aligned_momentum_bps = aligned_move_bps(context.spot_move_15s_bps, primary_side);
    let aligned_acceleration_bps = aligned_move_bps(context.micro_acceleration_bps, primary_side);
    let primary_ask_price = paper_position_primary_book(position, books, primary_side)
        .and_then(OrderBook::best_ask)
        .map(|level| level.price.round_dp(4));

    if exit_config.peak_exit_enabled
        && mark_to_market_profit >= exit_config.peak_exit_min_profit_usdc
        && primary_ask_price
            .is_some_and(|price| price >= exit_config.peak_exit_min_primary_ask_price)
        && aligned_1s_bps <= exit_config.peak_exit_max_aligned_1s_bps
        && aligned_5s_bps <= exit_config.peak_exit_max_aligned_5s_bps
        && aligned_acceleration_bps <= exit_config.peak_exit_max_acceleration_bps
    {
        return Some(format!(
            "early-exit peak-exit: mtm {} USDC, ask {}, aligned 1s {} bps, aligned 5s {} bps, acceleration {} bps",
            mark_to_market_profit.round_dp(4),
            display_optional_decimal(primary_ask_price),
            aligned_1s_bps.round_dp(2),
            aligned_5s_bps.round_dp(2),
            aligned_acceleration_bps.round_dp(2),
        ));
    }

    if exit_config.exhaustion_exit_enabled
        && mark_to_market_profit >= exit_config.exhaustion_exit_min_profit_usdc
        && aligned_1s_bps <= exit_config.exhaustion_exit_max_aligned_1s_bps
        && aligned_5s_bps <= exit_config.exhaustion_exit_max_aligned_5s_bps
        && aligned_momentum_bps <= exit_config.exhaustion_exit_max_aligned_15s_bps
        && aligned_acceleration_bps <= exit_config.exhaustion_exit_max_acceleration_bps
    {
        return Some(format!(
            "early-exit exhaustion: mtm {} USDC, aligned 1s {} bps, aligned 5s {} bps, aligned 15s {} bps, acceleration {} bps",
            mark_to_market_profit.round_dp(4),
            aligned_1s_bps.round_dp(2),
            aligned_5s_bps.round_dp(2),
            aligned_momentum_bps.round_dp(2),
            aligned_acceleration_bps.round_dp(2),
        ));
    }

    None
}

fn paper_position_primary_book<'a>(
    position: &PaperPosition,
    books: &'a HashMap<String, OrderBook>,
    primary_side: PaperOutcomeSide,
) -> Option<&'a OrderBook> {
    position
        .legs
        .iter()
        .filter(|leg| leg.side == primary_side)
        .max_by(|left, right| {
            left.shares
                .partial_cmp(&right.shares)
                .unwrap_or(Ordering::Equal)
        })
        .and_then(|leg| books.get(&leg.token_id))
}

fn summarize_worst_open_position(
    paper_state: &PaperState,
    contexts: &HashMap<String, BtcFiveMinuteContext>,
    books: &HashMap<String, OrderBook>,
    exit_config: &EarlyExitConfig,
    paper_cost_model: PaperCostModel,
) -> Option<WorstOpenPositionSummary> {
    paper_state
        .open_positions
        .values()
        .filter_map(|position| {
            let context = contexts.get(&position.slug)?;
            let primary_side = paper_position_primary_side(position);
            if primary_side == PaperOutcomeSide::Unknown {
                return None;
            }

            let mark_to_market_profit =
                paper_position_net_mark_to_market_profit(position, books, paper_cost_model);
            let aligned_1s_bps = aligned_move_bps(context.spot_move_1s_bps, primary_side);
            let aligned_5s_bps = aligned_move_bps(context.spot_move_5s_bps, primary_side);
            let aligned_momentum_bps = aligned_move_bps(context.spot_move_15s_bps, primary_side);
            let stop_loss_hit = if is_micro_breakout_kind(position.kind) {
                exit_config.max_loss_usdc > Decimal::ZERO
                    && mark_to_market_profit <= -exit_config.max_loss_usdc
            } else {
                exit_config.max_loss_usdc > Decimal::ZERO
                    && mark_to_market_profit <= -exit_config.max_loss_usdc
                    && aligned_5s_bps <= -exit_config.reversal_min_5s_bps
            };

            Some(WorstOpenPositionSummary {
                slug: position.slug.clone(),
                mark_to_market_profit,
                stop_loss_hit,
                aligned_1s_bps,
                aligned_5s_bps,
                aligned_15s_bps: aligned_momentum_bps,
            })
        })
        .min_by(|left, right| {
            left.mark_to_market_profit
                .partial_cmp(&right.mark_to_market_profit)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
}

fn paper_position_primary_side(position: &PaperPosition) -> PaperOutcomeSide {
    let label_side = PaperOutcomeSide::from_label(&position.dominant_outcome_at_entry);
    if label_side != PaperOutcomeSide::Unknown {
        return label_side;
    }

    position
        .legs
        .iter()
        .filter(|leg| leg.side != PaperOutcomeSide::Unknown)
        .max_by(|left, right| {
            left.shares
                .partial_cmp(&right.shares)
                .unwrap_or(Ordering::Equal)
        })
        .map_or(PaperOutcomeSide::Unknown, |leg| leg.side)
}

fn aligned_move_bps(move_bps: Decimal, side: PaperOutcomeSide) -> Decimal {
    match side {
        PaperOutcomeSide::Up => move_bps,
        PaperOutcomeSide::Down => -move_bps,
        PaperOutcomeSide::Unknown => Decimal::ZERO,
    }
}

fn build_paper_open_trade(opportunity: &Opportunity, report: &ExecutionReport) -> PaperTradeEntry {
    PaperTradeEntry {
        recorded_at: Utc::now(),
        action: PaperTradeAction::Open,
        slug: opportunity.slug.clone(),
        condition_id: opportunity.condition_id.clone(),
        question: opportunity.question.clone(),
        kind: opportunity.kind,
        spent_usdc: report.spent_usdc.round_dp(6),
        expected_profit_usdc: Some(report.expected_profit.round_dp(6)),
        realized_payout_usdc: None,
        realized_profit_usdc: None,
        dominant_outcome: Some(opportunity.dominant_outcome.clone()),
        actual_outcome: None,
        holding_seconds: None,
        close_category: None,
        current_spot_price: Some(opportunity.current_spot_price.round_dp(4)),
        target_price: Some(opportunity.target_price.round_dp(4)),
        target_gap_bps: Some(opportunity.target_gap_bps.round_dp(2)),
        spot_move_bps: Some(opportunity.spot_move_bps.round_dp(2)),
        spot_move_1s_bps: Some(opportunity.spot_move_1s_bps.round_dp(2)),
        spot_move_5s_bps: Some(opportunity.spot_move_5s_bps.round_dp(2)),
        spot_move_15s_bps: Some(opportunity.spot_move_15s_bps.round_dp(2)),
        micro_acceleration_bps: Some(opportunity.micro_acceleration_bps.round_dp(2)),
        signal_strength_bps: Some(opportunity.signal_strength_bps.round_dp(2)),
        aligned_trade_flow_bps: Some(opportunity.aligned_trade_flow_bps.round_dp(2)),
        primary_outcome_ask_price: Some(opportunity.primary_outcome_ask_price.round_dp(4)),
        signal_tier: (!opportunity.signal_tier.is_empty()).then(|| opportunity.signal_tier.clone()),
        target_cross_label: (!opportunity.target_cross_label.is_empty())
            .then(|| opportunity.target_cross_label.clone()),
        seconds_left_at_entry: Some(opportunity.seconds_left),
        note: format!(
            "{} | spent_with_costs {} | spot {} | 1s {} | 5s {} | 15s {} | ask {} | signal {} | flow {} | target_gap {}",
            opportunity.kind.as_str(),
            report.spent_usdc.round_dp(4),
            opportunity.spot_move_bps.round_dp(2),
            opportunity.spot_move_1s_bps.round_dp(2),
            opportunity.spot_move_5s_bps.round_dp(2),
            opportunity.spot_move_15s_bps.round_dp(2),
            opportunity.primary_outcome_ask_price.round_dp(4),
            opportunity.signal_strength_bps.round_dp(2),
            opportunity.aligned_trade_flow_bps.round_dp(2),
            opportunity.target_gap_bps.round_dp(2),
        ),
    }
}

fn build_paper_close_trade(report: &PaperCloseReport) -> PaperTradeEntry {
    PaperTradeEntry {
        recorded_at: report.closed_at,
        action: PaperTradeAction::Close,
        slug: report.slug.clone(),
        condition_id: report.condition_id.clone(),
        question: report.question.clone(),
        kind: report.kind,
        spent_usdc: report.spent_usdc.round_dp(6),
        expected_profit_usdc: None,
        realized_payout_usdc: Some(report.realized_payout_usdc.round_dp(6)),
        realized_profit_usdc: Some(report.realized_profit_usdc.round_dp(6)),
        dominant_outcome: None,
        actual_outcome: (!is_early_exit_reason(&report.close_reason))
            .then(|| report.actual_outcome.as_str().to_owned()),
        holding_seconds: Some(report.holding_seconds),
        close_category: Some(classify_close_reason(&report.close_reason).to_owned()),
        current_spot_price: None,
        target_price: None,
        target_gap_bps: None,
        spot_move_bps: None,
        spot_move_1s_bps: None,
        spot_move_5s_bps: None,
        spot_move_15s_bps: None,
        micro_acceleration_bps: None,
        signal_strength_bps: None,
        aligned_trade_flow_bps: None,
        primary_outcome_ask_price: None,
        signal_tier: None,
        target_cross_label: None,
        seconds_left_at_entry: None,
        note: report.close_reason.clone(),
    }
}

fn is_early_exit_reason(reason: &str) -> bool {
    reason.starts_with("early-exit")
}

const WINDOW_SETTLEMENT_REASON: &str = "window-settlement: position resolved at settlement";

fn classify_close_reason(reason: &str) -> &'static str {
    if reason.starts_with(WINDOW_SETTLEMENT_REASON)
        || reason.to_ascii_lowercase().contains("settlement")
    {
        "window_settlement"
    } else if reason.starts_with("early-exit hard stop-loss")
        || reason.starts_with("early-exit micro hard stop-loss")
    {
        "early_exit_hard_stop_loss"
    } else if reason.starts_with("early-exit scalp take-profit") {
        "early_exit_scalp_take_profit"
    } else if reason.starts_with("early-exit scalp stop-loss") {
        "early_exit_scalp_stop_loss"
    } else if reason.starts_with("early-exit scalp signal-invalidation") {
        "early_exit_scalp_signal_invalidation"
    } else if reason.starts_with("early-exit scalp near-expiry") {
        "early_exit_scalp_near_expiry"
    } else if reason.starts_with("early-exit scalp time-stop") {
        "early_exit_scalp_time_stop"
    } else if reason.starts_with("early-exit stop-loss") {
        "early_exit_stop_loss"
    } else if reason.starts_with("early-exit peak-exit") {
        "early_exit_peak_exit"
    } else if reason.starts_with("early-exit exhaustion") {
        "early_exit_exhaustion"
    } else if reason.starts_with("early-exit partial-reversal") {
        "early_exit_partial_reversal"
    } else if reason.starts_with("early-exit take-profit") {
        "early_exit_take_profit"
    } else if reason.starts_with("early-exit time-decay") {
        "early_exit_time_decay"
    } else if reason.starts_with("early-exit reversal") {
        "early_exit_reversal"
    } else if reason.starts_with("early-exit") {
        "early_exit_other"
    } else {
        "other"
    }
}

fn log_paper_open(opportunity: &Opportunity, report: &ExecutionReport, paper_state: &PaperState) {
    let window_position = paper_state.open_positions.get(&opportunity.slug);
    let window_spent_usdc = window_position.map_or(report.spent_usdc.round_dp(4), |position| {
        position.spent_usdc.round_dp(4)
    });
    let window_leg_count = window_position.map_or(0, |position| position.legs.len());
    let window_entry_count = window_position.map_or(0, |position| position.entry_count.max(1));
    info!(
        slug = %opportunity.slug,
        kind = opportunity.kind.as_str(),
        spent_usdc = %report.spent_usdc,
        shares = %report.shares,
        window_spent_usdc = %window_spent_usdc,
        window_leg_count,
        window_entry_count,
        open_positions = paper_state.open_positions.len(),
        open_notional = %paper_state.market_notional.values().copied().sum::<Decimal>().round_dp(4),
        question = %report.question,
        "paper position opened"
    );
}

fn log_paper_close(report: &PaperCloseReport, paper_state: &PaperState) {
    let actual_outcome = if is_early_exit_reason(&report.close_reason) {
        "mark-to-market"
    } else {
        report.actual_outcome.as_str()
    };
    info!(
        slug = %report.slug,
        kind = report.kind.as_str(),
        actual_outcome,
        payout_usdc = %report.realized_payout_usdc,
        realized_profit_usdc = %report.realized_profit_usdc,
        holding_seconds = report.holding_seconds,
        open_positions = paper_state.open_positions.len(),
        total_realized_profit = %paper_state.total_realized_profit.round_dp(4),
        question = %report.question,
        "paper position closed"
    );
}

fn should_append_paper_cycle_journal(
    entry: &PaperCycleEntry,
    last_append_at: Option<DateTime<Utc>>,
    sample_secs: u64,
) -> bool {
    should_append_paper_cycle_journal_fields(
        entry.recorded_at,
        entry.selected_count,
        entry.executed_count,
        entry.risk_blocked,
        last_append_at,
        sample_secs,
    )
}

fn should_append_paper_cycle_journal_fields(
    recorded_at: DateTime<Utc>,
    selected_count: usize,
    executed_count: usize,
    risk_blocked: bool,
    last_append_at: Option<DateTime<Utc>>,
    sample_secs: u64,
) -> bool {
    if selected_count > 0 || executed_count > 0 || risk_blocked {
        return true;
    }

    let Some(last_append_at) = last_append_at else {
        return true;
    };

    let sample_secs = i64::try_from(sample_secs.max(1)).unwrap_or(i64::MAX);
    recorded_at.signed_duration_since(last_append_at) >= ChronoDuration::seconds(sample_secs)
}

fn log_paper_cycle_summary(entry: &PaperCycleEntry) {
    debug!(
        markets = entry.total_markets,
        live_markets = entry.live_markets,
        fit = entry.strategy_fit_count,
        opportunities = entry.opportunity_count,
        near_miss = entry.near_miss_count,
        selected = entry.selected_count,
        executed = entry.executed_count,
        open_notional = %entry.open_notional,
        live_slug = entry.current_market_slug.as_deref().unwrap_or("-"),
        live_price = entry.current_market_price.as_deref().unwrap_or("-"),
        live_spot_source = entry.current_market_spot_source.as_deref().unwrap_or("-"),
        live_spot_event_age_ms = entry.current_market_spot_event_age_ms,
        live_spot_received_age_ms = entry.current_market_spot_received_age_ms,
        live_spot_quote_points = entry.current_market_spot_quote_points,
        live_spot = entry.current_market_spot_move_bps.as_deref().unwrap_or("-"),
        live_spot_5s = entry.current_market_spot_move_5s_bps.as_deref().unwrap_or("-"),
        live_up = entry.current_market_up_ask.as_deref().unwrap_or("-"),
        live_down = entry.current_market_down_ask.as_deref().unwrap_or("-"),
        top_signal = entry.top_opportunity_slug.as_deref().unwrap_or("-"),
        top_near_miss = entry.top_near_miss_slug.as_deref().unwrap_or("-"),
        regime = entry.regime.as_deref().unwrap_or("-"),
        risk_blocked = entry.risk_blocked,
        risk_reason = entry.risk_reason.as_deref().unwrap_or("-"),
        worst_open_slug = entry.worst_open_slug.as_deref().unwrap_or("-"),
        worst_open_mtm = entry
            .worst_open_mtm_profit_usdc
            .as_deref()
            .unwrap_or("-"),
        worst_open_stop = entry
            .worst_open_stop_loss_hit
            .map_or("-", yes_no_ru),
        worst_open_1s = entry
            .worst_open_aligned_1s_bps
            .as_deref()
            .unwrap_or("-"),
        worst_open_5s = entry
            .worst_open_aligned_5s_bps
            .as_deref()
            .unwrap_or("-"),
        worst_open_15s = entry
            .worst_open_aligned_15s_bps
            .as_deref()
            .unwrap_or("-"),
        daily_realized_profit = %entry.daily_realized_profit.round_dp(4),
        session_realized_profit = %entry.session_realized_profit.round_dp(4),
        consecutive_losses = entry.consecutive_losses,
        latency_trigger_event_to_snapshot_ms = entry.latency.trigger_event_to_snapshot_ms,
        latency_trigger_received_to_snapshot_ms = entry.latency.trigger_received_to_snapshot_ms,
        latency_runtime_snapshot_ms = entry.latency.runtime_snapshot_ms,
        latency_analysis_ms = entry.latency.analysis_ms,
        latency_selection_ms = entry.latency.selection_ms,
        latency_revalidation_ms = entry.latency.revalidation_ms,
        latency_execution_ms = entry.latency.execution_ms,
        latency_cycle_total_ms = entry.latency.cycle_total_ms,
        decision = entry.decision_reason.as_deref().unwrap_or("-"),
        "paper cycle summary"
    );
}

fn log_live_cycle_snapshot(
    config: &AppConfig,
    runtime_summary: &RuntimeSnapshotSummary,
    opportunities: &[Opportunity],
    near_misses: &[NearMiss],
) {
    let Some(current_market) = runtime_summary.current_market.as_ref() else {
        return;
    };

    debug!(
        slug = %current_market.slug,
        target = %current_market.target_label,
        seconds_left = current_market.seconds_left,
        price = %current_market.current_price,
        price_source = %current_market.current_price_source,
        price_event_age_ms = current_market.current_price_event_age_ms,
        price_received_age_ms = current_market.current_price_received_age_ms,
        price_quote_points = current_market.current_price_quote_points,
        target_price = %current_market.target_price,
        target_source = %current_market.target_price_source,
        target_gap_bps = %current_market.target_gap_bps,
        ref_5s_price = %current_market.micro_reference_price,
        spot_move_bps = %current_market.spot_move_bps,
        spot_move_5s_bps = %current_market.spot_move_5s_bps,
        up_ask = %current_market.up_ask,
        down_ask = %current_market.down_ask,
        bundle_cost = %current_market.bundle_cost,
        direction = %current_market.dominant_outcome,
        fit = current_market.status.strategy_fit,
        data_health = runtime_summary.data_health_reason.as_deref().unwrap_or("healthy"),
        trigger_event_to_snapshot_ms = runtime_summary.latency.trigger_event_to_snapshot_ms,
        trigger_received_to_snapshot_ms = runtime_summary.latency.trigger_received_to_snapshot_ms,
        exit_snapshot_ms = runtime_summary.latency.exit_snapshot_ms,
        early_exit_eval_ms = runtime_summary.latency.early_exit_eval_ms,
        runtime_snapshot_ms = runtime_summary.latency.runtime_snapshot_ms,
        analysis_ms = runtime_summary.latency.analysis_ms,
        selection_ms = runtime_summary.latency.selection_ms,
        revalidation_ms = runtime_summary.latency.revalidation_ms,
        execution_ms = runtime_summary.latency.execution_ms,
        cycle_total_ms = runtime_summary.latency.cycle_total_ms,
        "runtime market snapshot"
    );

    if let Some(opportunity) = opportunities.first() {
        debug!(
            slug = %opportunity.slug,
            kind = opportunity.kind.as_str(),
            edge_bps = opportunity.edge_bps,
            ask = %opportunity.primary_outcome_ask_price,
            expected_profit = %opportunity.expected_profit,
            note = %opportunity.note,
            "runtime opportunity candidate"
        );
    } else if let Some(near_miss) = near_misses.first() {
        debug!(
            slug = %near_miss.slug,
            kind = near_miss.kind.as_str(),
            gap = %near_miss.shortfall_label,
            ask = %near_miss
                .primary_outcome_ask_price
                .map_or_else(|| "-".to_owned(), |value| value.round_dp(4).to_string()),
            reason = %near_miss.reason,
            "runtime near-miss candidate"
        );
    } else {
        debug!(
            reason = %explain_no_near_miss_runtime(config, current_market),
            "runtime market has no candidate"
        );
    }
}

fn explain_no_near_miss_runtime(
    config: &AppConfig,
    current_market: &RuntimeCurrentMarketSummary,
) -> String {
    let spot_abs = parse_display_decimal(&current_market.spot_move_bps)
        .map_or(Decimal::ZERO, |value| value.abs());
    let bundle_cost = parse_display_decimal(&current_market.bundle_cost);
    let directional_ask = directional_ask_from_runtime_summary(current_market);

    if config.strategy.enable_directional
        && spot_abs < Decimal::from(config.strategy.directional_min_spot_move_bps)
    {
        return format!(
            "abs(spot) {} bps directional_min_spot_move_bps {}",
            current_market.spot_move_bps, config.strategy.directional_min_spot_move_bps
        );
    }

    if config.strategy.enable_bundle && spot_abs < Decimal::from(config.strategy.min_spot_move_bps)
    {
        return format!(
            "abs(spot) {} bps bundle min_spot_move_bps {}",
            current_market.spot_move_bps, config.strategy.min_spot_move_bps
        );
    }

    if config.strategy.enable_bundle
        && let Some(bundle_cost) = bundle_cost
        && bundle_cost > Decimal::ONE
    {
        return format!(
            "bundle_cost {} is above bundle threshold",
            current_market.bundle_cost
        );
    }

    if config.strategy.enable_directional
        && let Some(directional_ask) = directional_ask
        && directional_ask > config.strategy.directional_max_entry_price
    {
        return format!(
            "ask {} directional_max_entry_price {}",
            directional_ask.round_dp(4),
            config.strategy.directional_max_entry_price.round_dp(4)
        );
    }

    if config.strategy.enable_directional {
        return format!(
            "directional scan: spot {} | 5s {} | up/down {}/{}",
            current_market.spot_move_bps,
            current_market.spot_move_5s_bps,
            current_market.up_ask,
            current_market.down_ask
        );
    }

    format!(
        "scan: spot {} | 5s {} | up/down {}/{}",
        current_market.spot_move_bps,
        current_market.spot_move_5s_bps,
        current_market.up_ask,
        current_market.down_ask
    )
}

fn directional_ask_from_runtime_summary(
    current_market: &RuntimeCurrentMarketSummary,
) -> Option<Decimal> {
    if current_market.dominant_outcome == "Up" {
        parse_display_decimal(&current_market.up_ask)
    } else if current_market.dominant_outcome == "Down" {
        parse_display_decimal(&current_market.down_ask)
    } else {
        None
    }
}

fn parse_display_decimal(value: &str) -> Option<Decimal> {
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed == "/" {
        None
    } else {
        trimmed.parse::<Decimal>().ok()
    }
}

#[allow(clippy::too_many_arguments)]
async fn show_backtest(
    config: &AppConfig,
    data_client: &MarketDataClient,
    binance_client: &BinanceClient,
    _strategy: &BundleArbitrageStrategy,
    windows_per_target: usize,
    entry_minutes: u32,
    top: usize,
    target: Option<crate::models::MarketTarget>,
) -> Result<()> {
    let scoped_config = config_for_backtest_target(config, target);
    let scoped_strategy = BundleArbitrageStrategy::new(scoped_config.strategy.clone());
    let runner = BacktestRunner::new(
        &scoped_config,
        data_client,
        binance_client,
        &scoped_strategy,
    );
    let report = runner.run(windows_per_target, entry_minutes).await?;

    log_backtest_report(&report, top);
    println!("{}", render_backtest_report(&report, top, "backtest"));
    Ok(())
}

async fn show_polybacktest(
    config: &AppConfig,
    binance_client: &BinanceClient,
    _strategy: &BundleArbitrageStrategy,
    windows_per_target: usize,
    entry_minutes: u32,
    top: usize,
    target: Option<crate::models::MarketTarget>,
) -> Result<()> {
    let scoped_config = config_for_backtest_target(config, target);
    let scoped_strategy = BundleArbitrageStrategy::new(scoped_config.strategy.clone());
    let runner = PolyBacktestRunner::new(&scoped_config, &scoped_strategy, binance_client)?;
    let report = runner.run(windows_per_target, entry_minutes).await?;

    log_backtest_report(&report, top);
    println!("{}", render_backtest_report(&report, top, "PolyBackTest"));
    Ok(())
}

struct PolyBacktestTuneOptions<'a> {
    windows_per_target: usize,
    entry_minutes: &'a [u32],
    top: usize,
    target: Option<crate::models::MarketTarget>,
    variant_filter: &'a [String],
    max_variants: Option<usize>,
}

async fn show_polybacktest_tune(
    config: &AppConfig,
    binance_client: &BinanceClient,
    options: PolyBacktestTuneOptions<'_>,
) -> Result<()> {
    if options.entry_minutes.is_empty() {
        return Err(AppError::InvalidConfig(
            "polybacktest-tune requires at least one entry minute",
        ));
    }

    let mut entry_grid = options.entry_minutes.to_vec();
    entry_grid.sort_unstable();
    entry_grid.dedup();

    let base_config = config_for_backtest_target(config, options.target);
    let mut variants = polybacktest_tune_variants();
    if !options.variant_filter.is_empty() {
        let requested = options
            .variant_filter
            .iter()
            .map(|name| name.trim().to_ascii_lowercase())
            .filter(|name| !name.is_empty())
            .collect::<HashSet<_>>();
        variants.retain(|variant| requested.contains(&variant.name.to_ascii_lowercase()));
        if variants.is_empty() {
            return Err(AppError::InvalidConfig(
                "polybacktest-tune variant filter did not match any known variant",
            ));
        }
    }
    let variant_limit = options
        .max_variants
        .unwrap_or(variants.len())
        .clamp(1, variants.len());
    let mut results = Vec::new();
    let base_strategy = BundleArbitrageStrategy::new(base_config.strategy.clone());
    let base_runner = PolyBacktestRunner::new(&base_config, &base_strategy, binance_client)?;
    let datasets = base_runner
        .prepare_datasets(options.windows_per_target, &entry_grid)
        .await?;

    for variant in variants.iter().take(variant_limit) {
        for dataset in &datasets {
            let mut tuned_config = base_config.clone();
            (variant.apply)(&mut tuned_config);
            tuned_config.validate()?;

            let tuned_strategy = BundleArbitrageStrategy::new(tuned_config.strategy.clone());
            let runner = PolyBacktestRunner::new(&tuned_config, &tuned_strategy, binance_client)?;
            let report = runner.run_prepared(dataset);

            results.push(PolyBacktestTuneResult::from_report(
                variant.name,
                variant.description,
                &report,
            ));
        }
    }

    results.sort_by(|left, right| {
        right
            .realized_profit
            .cmp(&left.realized_profit)
            .then_with(|| right.signal_count.cmp(&left.signal_count))
            .then_with(|| right.expected_profit.cmp(&left.expected_profit))
            .then_with(|| left.variant_name.cmp(right.variant_name))
            .then_with(|| left.entry_minutes.cmp(&right.entry_minutes))
    });

    println!(
        "{}",
        render_polybacktest_tune_report(&results, options.top, options.windows_per_target)
    );
    Ok(())
}

async fn show_analytics(
    config: &AppConfig,
    binance_client: &BinanceClient,
    limit: Option<usize>,
) -> Result<()> {
    let journal = JournalStore::new(&config.storage)?;
    let snapshot = journal.load_snapshot()?;
    let entries = journal.load_entries(limit)?;
    let resolutions = load_resolutions(binance_client, &entries).await?;
    let report = AnalyticsReport::from_entries(&entries, &snapshot, &resolutions);

    info!(
        execution_count_total = report.execution_count_total,
        execution_count_sampled = report.execution_count_sampled,
        unique_market_windows = report.unique_market_windows,
        current_open_notional = %report.current_open_notional,
        total_spent_usdc = %report.total_spent_usdc,
        total_expected_profit = %report.total_expected_profit,
        realized_profit_resolved = %report.realized_profit_resolved,
        average_edge_bps = %report.average_edge_bps,
        average_spot_move_bps = %report.average_spot_move_bps,
        average_bundle_cost = %report.average_bundle_cost,
        average_realized_move_bps = %report.average_realized_move_bps,
        resolved_execution_count = report.resolved_execution_count,
        pending_resolution_count = report.pending_resolution_count,
        signal_accuracy_pct = %report.signal_accuracy_pct,
        last_execution_at = ?report.last_execution_at,
        "analytics summary"
    );

    for (dominant_outcome, count) in &report.dominant_outcome_distribution {
        info!(
            dominant_outcome = %dominant_outcome,
            executions = *count,
            "analytics dominant-outcome bucket"
        );
    }

    for (actual_outcome, count) in &report.actual_outcome_distribution {
        info!(
            actual_outcome = %actual_outcome,
            executions = *count,
            "analytics actual-outcome bucket"
        );
    }

    for entry in entries.iter().rev().take(5) {
        let resolution = resolutions.get(&entry.opportunity.slug);
        info!(
            recorded_at = %entry.recorded_at,
            slug = %entry.opportunity.slug,
            edge_bps = entry.opportunity.edge_bps,
            spot_move_bps = %entry.opportunity.spot_move_bps,
            actual_outcome = resolution.map_or("unresolved", |resolution| resolution.actual_outcome.as_str()),
            realized_move_bps = %resolution.map_or(Decimal::ZERO, |resolution| resolution.realized_move_bps),
            spent_usdc = %entry.report.spent_usdc,
            question = %entry.report.question,
            "analytics recent execution"
        );
    }

    Ok(())
}

fn log_auth_check(report: &AuthCheckReport) {
    info!(
        api_status = %report.api_status,
        wallet_address = %report.wallet_address,
        api_key = %report.api_key,
        credential_mode = %report.credential_mode,
        signature_type = %report.signature_type,
        funder_mode = %report.funder_mode,
        funder_address = ?report.funder_address,
        "live-"
    );
}

fn log_backtest_report(report: &BacktestReport, top: usize) {
    info!(
        entry_minutes = report.entry_minutes,
        targets = report.summaries.len(),
        signals = report.signals.len(),
        near_misses = report.near_misses.len(),
        "backtest"
    );

    for summary in &report.summaries {
        info!(
            target = summary.target.label(),
            sampled_windows = summary.sampled_windows,
            signal_count = summary.signal_count,
            near_miss_count = summary.near_miss_count,
            resolved_signal_count = summary.resolved_signal_count,
            expected_profit = %summary.expected_profit,
            realized_profit = %summary.realized_profit,
            signal_accuracy_pct = %summary.signal_accuracy_pct,
            "backtest"
        );
    }

    for signal in report.signals.iter().take(top) {
        info!(
            target = signal.target.label(),
            slug = %signal.slug,
            kind = signal.kind.as_str(),
            spot_move_bps = %signal.spot_move_bps,
            bundle_cost = %signal.bundle_cost,
            expected_profit = %signal.expected_profit,
            realized_profit = %signal.realized_profit,
            actual_outcome = signal.actual_outcome.as_str(),
            dominant_outcome = %signal.dominant_outcome,
            question = %signal.question,
            "polybacktest signal"
        );
    }

    for near_miss in report.near_misses.iter().take(top) {
        info!(
            target = near_miss.target.label(),
            slug = %near_miss.slug,
            kind = near_miss.kind.as_str(),
            spot_move_bps = %near_miss.spot_move_bps,
            shortfall_bps = near_miss.shortfall_bps,
            shortfall_label = %near_miss.shortfall_label,
            reason = %near_miss.reason,
            dominant_outcome = %near_miss.dominant_outcome,
            question = %near_miss.question,
            "-"
        );
    }
}

fn config_for_backtest_target(
    config: &AppConfig,
    target: Option<crate::models::MarketTarget>,
) -> AppConfig {
    let mut scoped = config.clone();
    scoped.strategy.min_minutes_to_expiry = i64::MIN;
    if let Some(target) = target {
        scoped.strategy.market_targets = vec![target];
    }
    scoped
}

#[derive(Debug, Clone, Copy)]
struct PolyBacktestTuneVariant {
    name: &'static str,
    description: &'static str,
    apply: fn(&mut AppConfig),
}

#[derive(Debug, Clone)]
struct PolyBacktestTuneResult {
    variant_name: &'static str,
    description: &'static str,
    entry_minutes: u32,
    sampled_windows: usize,
    signal_count: usize,
    near_miss_count: usize,
    win_count: usize,
    hit_rate_pct: Decimal,
    expected_profit: Decimal,
    realized_profit: Decimal,
    top_near_miss_reason: String,
}

impl PolyBacktestTuneResult {
    fn from_report(
        variant_name: &'static str,
        description: &'static str,
        report: &BacktestReport,
    ) -> Self {
        let sampled_windows = report
            .summaries
            .iter()
            .map(|summary| summary.sampled_windows)
            .sum();
        let signal_count = report.signals.len();
        let near_miss_count = report.near_misses.len();
        let win_count = report
            .signals
            .iter()
            .filter(|signal| backtest_signal_matches_actual(signal))
            .count();
        let expected_profit = report
            .summaries
            .iter()
            .fold(Decimal::ZERO, |total, summary| {
                total + summary.expected_profit
            })
            .round_dp(6);
        let realized_profit = report
            .summaries
            .iter()
            .fold(Decimal::ZERO, |total, summary| {
                total + summary.realized_profit
            })
            .round_dp(6);

        Self {
            variant_name,
            description,
            entry_minutes: report.entry_minutes,
            sampled_windows,
            signal_count,
            near_miss_count,
            win_count,
            hit_rate_pct: percentage(win_count, signal_count),
            expected_profit,
            realized_profit,
            top_near_miss_reason: top_near_miss_reason(report),
        }
    }
}

fn polybacktest_tune_variants() -> Vec<PolyBacktestTuneVariant> {
    vec![
        PolyBacktestTuneVariant {
            name: "baseline",
            description: "current config unchanged",
            apply: tune_baseline,
        },
        PolyBacktestTuneVariant {
            name: "strict-flow-450",
            description: "restore v2-style taker-flow floor",
            apply: tune_strict_flow_450,
        },
        PolyBacktestTuneVariant {
            name: "late-window-90",
            description: "allow entries until 90 seconds left",
            apply: tune_late_window_90,
        },
        PolyBacktestTuneVariant {
            name: "max-entry-70",
            description: "slightly wider entry price cap with premium guards intact",
            apply: tune_max_entry_70,
        },
        PolyBacktestTuneVariant {
            name: "premium-lite",
            description: "softer premium-entry quality floor",
            apply: tune_premium_lite,
        },
        PolyBacktestTuneVariant {
            name: "stale-lite",
            description: "softer stale-micro confirmation floor",
            apply: tune_stale_lite,
        },
        PolyBacktestTuneVariant {
            name: "premium-momentum",
            description: "allow premium entries only with realistic fresh momentum",
            apply: tune_premium_momentum,
        },
        PolyBacktestTuneVariant {
            name: "wide-momentum-80",
            description: "allow asks up to 0.80 only with strong fresh momentum",
            apply: tune_wide_momentum_80,
        },
        PolyBacktestTuneVariant {
            name: "wide-momentum-85",
            description: "allow asks up to 0.85 only with very strong fresh momentum",
            apply: tune_wide_momentum_85,
        },
        PolyBacktestTuneVariant {
            name: "stale-momentum",
            description: "allow stale micro entries when 15s swing confirms",
            apply: tune_stale_momentum,
        },
        PolyBacktestTuneVariant {
            name: "signal-1",
            description: "lower state signal threshold from 2 bps to 1 bps",
            apply: tune_signal_1,
        },
        PolyBacktestTuneVariant {
            name: "signal-0",
            description: "allow zero-floor state signal for diagnostics",
            apply: tune_signal_0,
        },
        PolyBacktestTuneVariant {
            name: "profit-floor-30",
            description: "lower minimum modeled expected profit to 0.30 USDC",
            apply: tune_profit_floor_30,
        },
        PolyBacktestTuneVariant {
            name: "profit-floor-0",
            description: "disable minimum modeled expected profit floor",
            apply: tune_profit_floor_0,
        },
        PolyBacktestTuneVariant {
            name: "micro-probe",
            description: "enable small-size micro-breakout with strict entry price",
            apply: tune_micro_probe,
        },
        PolyBacktestTuneVariant {
            name: "micro-balanced",
            description: "enable balanced micro-breakout for fresh Binance impulses",
            apply: tune_micro_balanced,
        },
        PolyBacktestTuneVariant {
            name: "micro-aggressive",
            description: "enable higher-frequency micro-breakout with capped notional",
            apply: tune_micro_aggressive,
        },
        PolyBacktestTuneVariant {
            name: "micro-only-balanced",
            description: "disable Sentinel and test balanced micro-breakout alone",
            apply: tune_micro_only_balanced,
        },
        PolyBacktestTuneVariant {
            name: "micro-only-aggr",
            description: "disable Sentinel and test aggressive micro-breakout alone",
            apply: tune_micro_only_aggressive,
        },
        PolyBacktestTuneVariant {
            name: "micro-only-discount",
            description: "disable Sentinel and test cheap micro entries with very small size",
            apply: tune_micro_only_discount,
        },
        PolyBacktestTuneVariant {
            name: "hot-combo",
            description: "combine later window, wider cap, and softer premium/stale guards",
            apply: tune_hot_combo,
        },
        PolyBacktestTuneVariant {
            name: "hot-combo-profit",
            description: "hot-combo plus 0.30 USDC expected-profit floor",
            apply: tune_hot_combo_profit,
        },
        PolyBacktestTuneVariant {
            name: "hot-momentum",
            description: "combine hot config with realistic premium/stale momentum gates",
            apply: tune_hot_momentum,
        },
        PolyBacktestTuneVariant {
            name: "late-momentum-60",
            description: "allow late Sentinel entries only with strong fresh momentum",
            apply: tune_late_momentum_60,
        },
        PolyBacktestTuneVariant {
            name: "sentinel-micro",
            description: "current Sentinel plus balanced micro-breakout fallback",
            apply: tune_sentinel_micro,
        },
    ]
}

fn tune_baseline(_config: &mut AppConfig) {}

fn tune_strict_flow_450(config: &mut AppConfig) {
    config.strategy.bonereaper_state_v2_min_aligned_flow_bps = Decimal::from(450_u32);
}

fn tune_late_window_90(config: &mut AppConfig) {
    config.strategy.bonereaper_state_v2_min_seconds_left = 90;
}

fn tune_max_entry_70(config: &mut AppConfig) {
    config.strategy.codex_sentinel_v1_max_entry_price = Decimal::new(70, 2);
    config.strategy.codex_sentinel_v1_expensive_entry_price = Decimal::new(68, 2);
}

fn tune_premium_lite(config: &mut AppConfig) {
    config.strategy.codex_sentinel_v1_premium_entry_price = Decimal::new(58, 2);
    config.strategy.codex_sentinel_v1_premium_min_signal_bps = Decimal::from(500_u32);
    config.strategy.codex_sentinel_v1_premium_min_flow_bps = Decimal::from(800_u32);
    config.strategy.codex_sentinel_v1_premium_min_fresh_bps = Decimal::new(75, 2);
}

fn tune_stale_lite(config: &mut AppConfig) {
    config
        .strategy
        .codex_sentinel_v1_stale_micro_discount_min_signal_bps = Decimal::from(300_u32);
    config
        .strategy
        .codex_sentinel_v1_stale_micro_discount_min_flow_bps = Decimal::from(400_u32);
    config.strategy.codex_sentinel_v1_stale_micro_min_signal_bps = Decimal::from(500_u32);
    config.strategy.codex_sentinel_v1_stale_micro_min_flow_bps = Decimal::from(800_u32);
    config.strategy.codex_sentinel_v1_stale_micro_min_swing_bps = Decimal::new(50, 2);
    config
        .strategy
        .codex_sentinel_v1_stale_micro_min_target_gap_bps = Decimal::new(50, 2);
}

fn tune_premium_momentum(config: &mut AppConfig) {
    config.strategy.codex_sentinel_v1_max_entry_price = Decimal::new(70, 2);
    config.strategy.codex_sentinel_v1_expensive_entry_price = Decimal::new(68, 2);
    config.strategy.codex_sentinel_v1_expensive_min_micro_bps = Decimal::new(100, 2);
    config.strategy.codex_sentinel_v1_expensive_min_swing_bps = Decimal::new(100, 2);
    config.strategy.codex_sentinel_v1_premium_entry_price = Decimal::new(56, 2);
    config.strategy.codex_sentinel_v1_premium_min_signal_bps = Decimal::new(650, 2);
    config.strategy.codex_sentinel_v1_premium_min_flow_bps = Decimal::ZERO;
    config.strategy.codex_sentinel_v1_premium_min_fresh_bps = Decimal::new(100, 2);
}

fn tune_wide_momentum_80(config: &mut AppConfig) {
    tune_premium_momentum(config);
    config.strategy.codex_sentinel_v1_max_entry_price = Decimal::new(80, 2);
    config.strategy.codex_sentinel_v1_expensive_entry_price = Decimal::new(72, 2);
    config.strategy.codex_sentinel_v1_expensive_min_micro_bps = Decimal::new(200, 2);
    config.strategy.codex_sentinel_v1_expensive_min_swing_bps = Decimal::new(200, 2);
    config.strategy.codex_sentinel_v1_premium_min_signal_bps = Decimal::new(800, 2);
    config.strategy.codex_sentinel_v1_premium_min_fresh_bps = Decimal::new(150, 2);
    config.strategy.bonereaper_state_v2_min_expected_profit_usdc = Decimal::new(30, 2);
}

fn tune_wide_momentum_85(config: &mut AppConfig) {
    tune_premium_momentum(config);
    config.strategy.codex_sentinel_v1_max_entry_price = Decimal::new(85, 2);
    config.strategy.codex_sentinel_v1_expensive_entry_price = Decimal::new(76, 2);
    config.strategy.codex_sentinel_v1_expensive_min_micro_bps = Decimal::new(300, 2);
    config.strategy.codex_sentinel_v1_expensive_min_swing_bps = Decimal::new(300, 2);
    config.strategy.codex_sentinel_v1_premium_min_signal_bps = Decimal::new(950, 2);
    config.strategy.codex_sentinel_v1_premium_min_fresh_bps = Decimal::new(250, 2);
    config.strategy.bonereaper_state_v2_min_expected_profit_usdc = Decimal::new(20, 2);
}

fn tune_stale_momentum(config: &mut AppConfig) {
    config
        .strategy
        .codex_sentinel_v1_stale_micro_discount_min_signal_bps = Decimal::new(350, 2);
    config
        .strategy
        .codex_sentinel_v1_stale_micro_discount_min_flow_bps = Decimal::ZERO;
    config.strategy.codex_sentinel_v1_stale_micro_min_signal_bps = Decimal::new(550, 2);
    config.strategy.codex_sentinel_v1_stale_micro_min_flow_bps = Decimal::ZERO;
    config.strategy.codex_sentinel_v1_stale_micro_min_swing_bps = Decimal::new(45, 2);
    config
        .strategy
        .codex_sentinel_v1_stale_micro_min_target_gap_bps = Decimal::new(45, 2);
}

fn tune_signal_1(config: &mut AppConfig) {
    config.strategy.bonereaper_state_v2_min_signal_bps = 1;
}

fn tune_signal_0(config: &mut AppConfig) {
    config.strategy.bonereaper_state_v2_min_signal_bps = 0;
}

fn tune_profit_floor_30(config: &mut AppConfig) {
    config.strategy.bonereaper_state_v2_min_expected_profit_usdc = Decimal::new(30, 2);
}

fn tune_profit_floor_0(config: &mut AppConfig) {
    config.strategy.bonereaper_state_v2_min_expected_profit_usdc = Decimal::ZERO;
}

fn tune_micro_probe(config: &mut AppConfig) {
    config.strategy.enable_micro_breakout = true;
    config.strategy.micro_breakout_min_spot_move_bps = 2;
    config.strategy.micro_breakout_min_spot_move_5s_bps = Decimal::new(10, 1);
    config.strategy.micro_breakout_min_spot_move_1s_bps = Decimal::new(10, 2);
    config.strategy.micro_breakout_min_signal_bps = 3;
    config.strategy.micro_breakout_max_entry_price = Decimal::new(62, 2);
    config.strategy.micro_breakout_expensive_entry_price = Decimal::new(58, 2);
    config
        .strategy
        .micro_breakout_expensive_entry_requires_strong_tier = true;
    config.strategy.micro_breakout_max_average_price_drift = Decimal::new(15, 3);
    config.strategy.micro_breakout_weak_notional_usdc = Decimal::new(4, 0);
    config.strategy.micro_breakout_normal_notional_usdc = Decimal::new(6, 0);
    config.strategy.micro_breakout_strong_notional_usdc = Decimal::new(8, 0);
    config.strategy.micro_breakout_full_size_max_entry_price = Decimal::new(55, 2);
}

fn tune_micro_balanced(config: &mut AppConfig) {
    config.strategy.enable_micro_breakout = true;
    config.strategy.micro_breakout_min_spot_move_bps = 1;
    config.strategy.micro_breakout_min_spot_move_5s_bps = Decimal::new(75, 2);
    config.strategy.micro_breakout_min_spot_move_1s_bps = Decimal::new(8, 2);
    config.strategy.micro_breakout_min_signal_bps = 2;
    config.strategy.micro_breakout_max_entry_price = Decimal::new(68, 2);
    config.strategy.micro_breakout_expensive_entry_price = Decimal::new(62, 2);
    config
        .strategy
        .micro_breakout_expensive_entry_requires_strong_tier = true;
    config.strategy.micro_breakout_max_average_price_drift = Decimal::new(20, 3);
    config.strategy.micro_breakout_weak_notional_usdc = Decimal::new(4, 0);
    config.strategy.micro_breakout_normal_notional_usdc = Decimal::new(7, 0);
    config.strategy.micro_breakout_strong_notional_usdc = Decimal::new(10, 0);
    config.strategy.micro_breakout_full_size_max_entry_price = Decimal::new(58, 2);
}

fn tune_micro_aggressive(config: &mut AppConfig) {
    config.strategy.enable_micro_breakout = true;
    config.strategy.micro_breakout_min_spot_move_bps = 1;
    config.strategy.micro_breakout_min_spot_move_5s_bps = Decimal::new(50, 2);
    config.strategy.micro_breakout_min_spot_move_1s_bps = Decimal::new(5, 2);
    config.strategy.micro_breakout_min_signal_bps = 1;
    config.strategy.micro_breakout_max_entry_price = Decimal::new(72, 2);
    config.strategy.micro_breakout_expensive_entry_price = Decimal::new(66, 2);
    config
        .strategy
        .micro_breakout_expensive_entry_requires_strong_tier = true;
    config.strategy.micro_breakout_max_average_price_drift = Decimal::new(25, 3);
    config.strategy.micro_breakout_weak_notional_usdc = Decimal::new(3, 0);
    config.strategy.micro_breakout_normal_notional_usdc = Decimal::new(6, 0);
    config.strategy.micro_breakout_strong_notional_usdc = Decimal::new(9, 0);
    config.strategy.micro_breakout_full_size_max_entry_price = Decimal::new(60, 2);
}

fn tune_micro_only_balanced(config: &mut AppConfig) {
    config.strategy.enable_codex_sentinel_v1 = false;
    config.strategy.enable_bonereaper_state_v2 = false;
    config.strategy.enable_bonereaper_state_guarded = false;
    tune_micro_balanced(config);
}

fn tune_micro_only_aggressive(config: &mut AppConfig) {
    config.strategy.enable_codex_sentinel_v1 = false;
    config.strategy.enable_bonereaper_state_v2 = false;
    config.strategy.enable_bonereaper_state_guarded = false;
    tune_micro_aggressive(config);
}

fn tune_micro_only_discount(config: &mut AppConfig) {
    config.strategy.enable_codex_sentinel_v1 = false;
    config.strategy.enable_bonereaper_state_v2 = false;
    config.strategy.enable_bonereaper_state_guarded = false;
    config.strategy.enable_micro_breakout = true;
    config.strategy.micro_breakout_min_spot_move_bps = 0;
    config.strategy.micro_breakout_min_spot_move_5s_bps = Decimal::new(10, 2);
    config.strategy.micro_breakout_min_spot_move_1s_bps = Decimal::ZERO;
    config.strategy.micro_breakout_min_signal_bps = 1;
    config.strategy.micro_breakout_max_entry_price = Decimal::new(55, 2);
    config.strategy.micro_breakout_expensive_entry_price = Decimal::new(55, 2);
    config
        .strategy
        .micro_breakout_expensive_entry_requires_strong_tier = true;
    config.strategy.micro_breakout_max_average_price_drift = Decimal::new(10, 3);
    config.strategy.micro_breakout_weak_notional_usdc = Decimal::new(2, 0);
    config.strategy.micro_breakout_normal_notional_usdc = Decimal::new(3, 0);
    config.strategy.micro_breakout_strong_notional_usdc = Decimal::new(4, 0);
    config.strategy.micro_breakout_full_size_max_entry_price = Decimal::new(50, 2);
}

fn tune_hot_combo(config: &mut AppConfig) {
    tune_late_window_90(config);
    tune_max_entry_70(config);
    tune_premium_lite(config);
    tune_stale_lite(config);
}

fn tune_hot_combo_profit(config: &mut AppConfig) {
    tune_hot_combo(config);
    tune_profit_floor_30(config);
}

fn tune_hot_momentum(config: &mut AppConfig) {
    tune_late_window_90(config);
    tune_signal_1(config);
    tune_profit_floor_30(config);
    tune_premium_momentum(config);
    tune_stale_momentum(config);
    tune_late_momentum_60(config);
}

fn tune_late_momentum_60(config: &mut AppConfig) {
    config
        .strategy
        .codex_sentinel_v1_late_entry_override_enabled = true;
    config
        .strategy
        .codex_sentinel_v1_late_entry_min_seconds_left = 60;
    config.strategy.codex_sentinel_v1_late_entry_max_entry_price = Decimal::new(62, 2);
    config.strategy.codex_sentinel_v1_late_entry_min_signal_bps = Decimal::from(850_u32);
    config.strategy.codex_sentinel_v1_late_entry_min_fresh_bps = Decimal::new(150, 2);
    config.strategy.codex_sentinel_v1_late_entry_min_flow_bps = Decimal::ZERO;
    config
        .strategy
        .codex_sentinel_v1_late_entry_min_target_gap_bps = Decimal::new(150, 2);
}

fn tune_sentinel_micro(config: &mut AppConfig) {
    tune_profit_floor_30(config);
    tune_micro_balanced(config);
}

fn backtest_signal_matches_actual(signal: &BacktestSignal) -> bool {
    if signal.scalp_exit.is_some() {
        return signal.realized_profit >= Decimal::ZERO;
    }

    match signal.actual_outcome {
        WindowDirection::Up => outcome_label_is_up(&signal.primary_outcome_label),
        WindowDirection::Down => outcome_label_is_down(&signal.primary_outcome_label),
        WindowDirection::Flat => false,
    }
}

fn percentage(numerator: usize, denominator: usize) -> Decimal {
    if denominator == 0 {
        return Decimal::ZERO;
    }

    (Decimal::from(numerator as u64) / Decimal::from(denominator as u64) * Decimal::from(100_u32))
        .round_dp(2)
}

fn top_near_miss_reason(report: &BacktestReport) -> String {
    let mut counts = BTreeMap::<String, usize>::new();
    for near_miss in &report.near_misses {
        *counts.entry(near_miss.reason.clone()).or_default() += 1;
    }

    counts
        .into_iter()
        .max_by(|left, right| left.1.cmp(&right.1).then_with(|| right.0.cmp(&left.0)))
        .map_or_else(
            || "none".to_owned(),
            |(reason, count)| format!("{count}x {}", truncate_table_cell(&reason, 42)),
        )
}

fn truncate_table_cell(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_owned();
    }

    let mut truncated = value
        .chars()
        .take(max_chars.saturating_sub(3))
        .collect::<String>();
    truncated.push_str("...");
    truncated
}

fn render_polybacktest_tune_report(
    results: &[PolyBacktestTuneResult],
    top: usize,
    windows_per_target: usize,
) -> String {
    let mut output = String::new();
    let shown = top.min(results.len());

    let _ = writeln!(output, "PolyBackTest Autotune");
    let _ = writeln!(
        output,
        "windows/entry: {} | tested rows: {} | shown: {}",
        windows_per_target,
        results.len(),
        shown
    );
    let _ = writeln!(output);
    let _ = writeln!(
        output,
        "{:<4} {:<18} {:>5} {:>7} {:>7} {:>7} {:>5} {:>7} {:>10} {:>10}  top near-miss",
        "#",
        "variant",
        "entry",
        "windows",
        "signals",
        "near",
        "wins",
        "hit%",
        "expected",
        "realized"
    );
    let _ = writeln!(output, "{}", "-".repeat(126));

    for (index, result) in results.iter().take(shown).enumerate() {
        let _ = writeln!(
            output,
            "{:<4} {:<18} {:>5} {:>7} {:>7} {:>7} {:>5} {:>6}% {:>10} {:>10}  {}",
            index + 1,
            truncate_table_cell(result.variant_name, 18),
            result.entry_minutes,
            result.sampled_windows,
            result.signal_count,
            result.near_miss_count,
            result.win_count,
            result.hit_rate_pct,
            result.expected_profit.round_dp(4),
            result.realized_profit.round_dp(4),
            result.top_near_miss_reason,
        );
    }

    if shown > 0 {
        let _ = writeln!(output);
        let _ = writeln!(output, "Variant notes:");
        let mut seen_variants = HashSet::new();
        for result in results.iter().take(shown) {
            if !seen_variants.insert(result.variant_name) {
                continue;
            }
            let _ = writeln!(output, "- {}: {}", result.variant_name, result.description);
        }
    }

    output
}

#[allow(clippy::too_many_lines)]
fn render_backtest_report(report: &BacktestReport, top: usize, title: &str) -> String {
    let mut output = String::new();
    let total_sampled_windows = report
        .summaries
        .iter()
        .map(|summary| summary.sampled_windows)
        .sum::<usize>();
    let total_realized_profit = report
        .summaries
        .iter()
        .fold(Decimal::ZERO, |total, summary| {
            total + summary.realized_profit
        })
        .round_dp(6);
    let total_expected_profit = report
        .summaries
        .iter()
        .fold(Decimal::ZERO, |total, summary| {
            total + summary.expected_profit
        })
        .round_dp(6);
    let scalp_exit_count = report
        .signals
        .iter()
        .filter(|signal| signal.scalp_exit.is_some())
        .count();
    let total_scalp_realized_profit = report
        .signals
        .iter()
        .filter_map(|signal| signal.scalp_exit.as_ref())
        .fold(Decimal::ZERO, |total, scalp_exit| {
            total + scalp_exit.realized_profit
        })
        .round_dp(6);
    let scalp_wins = report
        .signals
        .iter()
        .filter_map(|signal| signal.scalp_exit.as_ref())
        .filter(|scalp_exit| scalp_exit.realized_profit > Decimal::ZERO)
        .count();
    let mut scalp_reason_counts = BTreeMap::<String, usize>::new();
    for scalp_exit in report
        .signals
        .iter()
        .filter_map(|signal| signal.scalp_exit.as_ref())
    {
        *scalp_reason_counts
            .entry(scalp_exit.exit_reason.clone())
            .or_default() += 1;
    }

    let _ = writeln!(output, "{title}");
    let _ = writeln!(
        output,
        "{} . | : {} | : {} | : {} | Near-miss: {} | Expected: {} | Realized: {}",
        report.entry_minutes,
        report.summaries.len(),
        total_sampled_windows,
        report.signals.len(),
        report.near_misses.len(),
        total_expected_profit,
        total_realized_profit
    );
    if scalp_exit_count > 0 {
        let reason_summary = scalp_reason_counts
            .iter()
            .map(|(reason, count)| format!("{reason}:{count}"))
            .collect::<Vec<_>>()
            .join(", ");
        let _ = writeln!(
            output,
            "Scalp exit model | Signals: {} | Realized: {} | WinRate: {}% | Reasons: {}",
            scalp_exit_count,
            total_scalp_realized_profit,
            percentage(scalp_wins, scalp_exit_count).round_dp(2),
            reason_summary
        );
    }
    let _ = writeln!(output);
    let _ = writeln!(output, ":");
    let _ = writeln!(
        output,
        "{:<10} {:>8} {:>8} {:>10} {:>10} {:>10} {:>9}",
        "Target", "Windows", "Signals", "NearMiss", "Expected", "Realized", "HitRate"
    );
    let _ = writeln!(output, "{}", "-".repeat(74));

    for summary in &report.summaries {
        let _ = writeln!(
            output,
            "{:<10} {:>8} {:>8} {:>10} {:>10} {:>10} {:>8}%",
            summary.target.label(),
            summary.sampled_windows,
            summary.signal_count,
            summary.near_miss_count,
            summary.expected_profit.round_dp(4),
            summary.realized_profit.round_dp(4),
            summary.signal_accuracy_pct.round_dp(2)
        );
    }

    let _ = writeln!(output);
    let _ = writeln!(output, "Signals:");
    if report.signals.is_empty() {
        let _ = writeln!(output, "none");
    } else {
        let _ = writeln!(
            output,
            "{:<3} {:<10} {:<11} {:>8} {:>8} {:>9} {:>10} {:<8} {:<8} Slug",
            "#", "Target", "Kind", "Spot", "Cost", "USDC", "Realized", "Actual", "Signal"
        );
        let _ = writeln!(output, "{}", "-".repeat(110));

        for (index, signal) in report.signals.iter().take(top).enumerate() {
            let _ = writeln!(
                output,
                "{:<3} {:<10} {:<11} {:>8} {:>8} {:>9} {:>10} {:<8} {:<8} {}",
                index + 1,
                signal.target.label(),
                signal.kind.as_str(),
                signal.spot_move_bps.round_dp(2),
                signal.bundle_cost.round_dp(4),
                signal.required_usdc.round_dp(4),
                signal.realized_profit.round_dp(4),
                signal.actual_outcome.as_str(),
                truncate_text_v2(&signal.dominant_outcome, 8),
                signal.slug
            );
        }
    }

    let _ = writeln!(output, "\\nNear-misses:");
    if report.near_misses.is_empty() {
        let _ = writeln!(output, "none");
        append_backtest_signals_csv(&mut output, report);
        return output;
    }

    let _ = writeln!(
        output,
        "{:<3} {:<10} {:<11} {:>8} {:>8} {:>7} {:<34} Slug",
        "#", "Target", "Kind", "Gap", "Spot", "Ask", "Reason"
    );
    let _ = writeln!(output, "{}", "-".repeat(108));

    for (index, near_miss) in report.near_misses.iter().take(top).enumerate() {
        let ask = near_miss
            .primary_outcome_ask_price
            .map_or_else(|| "-".to_owned(), |value| value.round_dp(4).to_string());
        let _ = writeln!(
            output,
            "{:<3} {:<10} {:<11} {:>8} {:>8} {:>7} {:<34} {}",
            index + 1,
            near_miss.target.label(),
            near_miss.kind.as_str(),
            near_miss.shortfall_label,
            near_miss.spot_move_bps.round_dp(2),
            ask,
            truncate_text_v2(&near_miss.reason, 34),
            near_miss.slug
        );
    }

    append_backtest_signals_csv(&mut output, report);
    output
}

fn append_backtest_signals_csv(output: &mut String, report: &BacktestReport) {
    let _ = writeln!(output, "\nSIGNALS_CSV_BEGIN");
    output.push_str(&render_backtest_signals_csv(report));
    let _ = writeln!(output, "SIGNALS_CSV_END");
    let _ = writeln!(output, "\nNEAR_MISSES_CSV_BEGIN");
    output.push_str(&render_backtest_near_misses_csv(report));
    let _ = writeln!(output, "NEAR_MISSES_CSV_END");
}

fn render_backtest_signals_csv(report: &BacktestReport) -> String {
    let mut csv = String::from(
        "target,slug,kind,seconds_left,primary_outcome,actual_outcome,correct,ask,avg_cost,required_usdc,shares,expected_payout,expected_profit,realized_profit,scalp_exit_reason,scalp_hold_secs,scalp_exit_price,scalp_gross_payout,scalp_realized_profit,scalp_max_favorable_price,scalp_max_adverse_price,edge_per_share,edge_bps,spot_move_bps,spot_move_1s_bps,spot_move_5s_bps,spot_move_15s_bps,micro_acceleration_bps,target_gap_bps,signal_strength_bps,aligned_trade_flow_bps,signal_tier,target_cross,note\n",
    );

    for signal in &report.signals {
        let scalp_exit = signal.scalp_exit.as_ref();
        let row = [
            csv_cell(signal.target.label()),
            csv_cell(&signal.slug),
            csv_cell(signal.kind.as_str()),
            csv_cell(signal.seconds_left.to_string()),
            csv_cell(&signal.primary_outcome_label),
            csv_cell(window_direction_code(signal.actual_outcome)),
            csv_cell(backtest_signal_was_correct(signal).to_string()),
            csv_cell(signal.primary_outcome_ask_price.round_dp(6).to_string()),
            csv_cell(signal.bundle_cost.round_dp(6).to_string()),
            csv_cell(signal.required_usdc.round_dp(6).to_string()),
            csv_cell(signal.tradable_shares.round_dp(6).to_string()),
            csv_cell(signal.expected_payout.round_dp(6).to_string()),
            csv_cell(signal.expected_profit.round_dp(6).to_string()),
            csv_cell(signal.realized_profit.round_dp(6).to_string()),
            csv_cell(
                scalp_exit
                    .map(|exit| exit.exit_reason.clone())
                    .unwrap_or_default(),
            ),
            csv_cell(scalp_exit.map_or_else(String::new, |exit| exit.hold_secs.to_string())),
            csv_cell(
                scalp_exit.map_or_else(String::new, |exit| exit.exit_price.round_dp(6).to_string()),
            ),
            csv_cell(scalp_exit.map_or_else(String::new, |exit| {
                exit.gross_payout.round_dp(6).to_string()
            })),
            csv_cell(scalp_exit.map_or_else(String::new, |exit| {
                exit.realized_profit.round_dp(6).to_string()
            })),
            csv_cell(scalp_exit.map_or_else(String::new, |exit| {
                exit.max_favorable_price.round_dp(6).to_string()
            })),
            csv_cell(scalp_exit.map_or_else(String::new, |exit| {
                exit.max_adverse_price.round_dp(6).to_string()
            })),
            csv_cell(signal.edge_per_share.round_dp(6).to_string()),
            csv_cell(signal.edge_bps.to_string()),
            csv_cell(signal.spot_move_bps.round_dp(6).to_string()),
            csv_cell(signal.spot_move_1s_bps.round_dp(6).to_string()),
            csv_cell(signal.spot_move_5s_bps.round_dp(6).to_string()),
            csv_cell(signal.spot_move_15s_bps.round_dp(6).to_string()),
            csv_cell(signal.micro_acceleration_bps.round_dp(6).to_string()),
            csv_cell(signal.target_gap_bps.round_dp(6).to_string()),
            csv_cell(signal.signal_strength_bps.round_dp(6).to_string()),
            csv_cell(signal.aligned_trade_flow_bps.round_dp(6).to_string()),
            csv_cell(&signal.signal_tier),
            csv_cell(&signal.target_cross_label),
            csv_cell(&signal.note),
        ]
        .join(",");
        csv.push_str(&row);
        csv.push('\n');
    }

    csv
}

fn render_backtest_near_misses_csv(report: &BacktestReport) -> String {
    let mut csv = String::from(
        "target,slug,kind,seconds_left,dominant_outcome,primary_outcome,ask,bundle_cost,spot_move_bps,shortfall_bps,shortfall_label,reason,question\n",
    );

    for near_miss in &report.near_misses {
        let row = [
            csv_cell(near_miss.target.label()),
            csv_cell(&near_miss.slug),
            csv_cell(near_miss.kind.as_str()),
            csv_cell(near_miss.seconds_left.to_string()),
            csv_cell(&near_miss.dominant_outcome),
            csv_cell(&near_miss.primary_outcome_label),
            csv_cell(
                near_miss
                    .primary_outcome_ask_price
                    .map_or_else(String::new, |value| value.round_dp(6).to_string()),
            ),
            csv_cell(
                near_miss
                    .bundle_cost
                    .map_or_else(String::new, |value| value.round_dp(6).to_string()),
            ),
            csv_cell(near_miss.spot_move_bps.round_dp(6).to_string()),
            csv_cell(near_miss.shortfall_bps.to_string()),
            csv_cell(&near_miss.shortfall_label),
            csv_cell(&near_miss.reason),
            csv_cell(&near_miss.question),
        ]
        .join(",");
        csv.push_str(&row);
        csv.push('\n');
    }

    csv
}

fn backtest_signal_was_correct(signal: &BacktestSignal) -> bool {
    if signal.scalp_exit.is_some() {
        return signal.realized_profit >= Decimal::ZERO;
    }

    if signal.kind == OpportunityKind::BundleArbitrage {
        return signal.realized_profit >= Decimal::ZERO;
    }

    matches!(
        (
            PaperOutcomeSide::from_label(&signal.primary_outcome_label),
            signal.actual_outcome,
        ),
        (PaperOutcomeSide::Up, WindowDirection::Up)
            | (PaperOutcomeSide::Down, WindowDirection::Down)
    )
}

const fn window_direction_code(direction: WindowDirection) -> &'static str {
    match direction {
        WindowDirection::Up => "up",
        WindowDirection::Down => "down",
        WindowDirection::Flat => "flat",
    }
}

fn configured_binance_symbols(config: &AppConfig) -> Vec<&'static str> {
    let mut symbols = Vec::with_capacity(config.strategy.market_targets.len());
    for target in &config.strategy.market_targets {
        let symbol = target.binance_symbol();
        if !symbols.contains(&symbol) {
            symbols.push(symbol);
        }
    }
    symbols
}

fn configured_market_targets(config: &AppConfig) -> Vec<crate::models::MarketTarget> {
    let mut targets = Vec::with_capacity(config.strategy.market_targets.len());
    for target in &config.strategy.market_targets {
        if !targets.contains(target) {
            targets.push(*target);
        }
    }
    targets
}

async fn load_resolutions(
    binance_client: &BinanceClient,
    entries: &[JournalEntry],
) -> Result<HashMap<String, MarketWindowResolution>> {
    let unique_slugs = entries
        .iter()
        .map(|entry| entry.opportunity.slug.clone())
        .collect::<HashSet<_>>();
    let mut resolutions = HashMap::with_capacity(unique_slugs.len());

    for slug in unique_slugs {
        if let Some(resolution) = binance_client.resolution_from_slug(&slug).await? {
            resolutions.insert(slug, resolution);
        }
    }

    Ok(resolutions)
}

async fn execute_and_log(
    executor: &impl TradeExecutor,
    opportunity: &Opportunity,
) -> Result<ExecutionReport> {
    info!(
        edge_bps = opportunity.edge_bps,
        dominant_outcome = %opportunity.dominant_outcome,
        spot_move_bps = %opportunity.spot_move_bps,
        spent_usdc = %opportunity.required_usdc,
        expected_profit = %opportunity.expected_profit,
        question = %opportunity.question,
        "paper opportunity execution"
    );
    executor.execute(opportunity).await
}

#[allow(dead_code)]
async fn collect_opportunities(
    config: &AppConfig,
    data_client: &MarketDataClient,
    binance_client: &BinanceClient,
    strategy: &BundleArbitrageStrategy,
    market_notional: &HashMap<String, Decimal>,
) -> Result<Vec<Opportunity>> {
    Ok(collect_analysis_frame(
        config,
        data_client,
        binance_client,
        strategy,
        market_notional,
    )
    .await?
    .opportunities)
}

#[derive(Debug)]
struct MarketSnapshot {
    markets: Vec<BinaryMarket>,
    books: HashMap<String, OrderBook>,
    contexts: HashMap<String, BtcFiveMinuteContext>,
    trade_flows: HashMap<String, TradeFlowSummary>,
}

#[derive(Debug, Clone)]
struct CachedReactiveMarketComponents {
    market: BinaryMarket,
    books: HashMap<String, OrderBook>,
    trade_flow: Option<TradeFlowSummary>,
    cached_at: Instant,
}

#[derive(Debug, Default)]
struct ReactiveMarketSnapshotCache {
    entries: HashMap<String, CachedReactiveMarketComponents>,
}

#[derive(Debug, Clone, Copy, Default)]
struct RuntimeAnalysisTiming {
    snapshot_ms: u64,
    analysis_ms: u64,
}

#[derive(Debug, Clone)]
struct WorstOpenPositionSummary {
    slug: String,
    mark_to_market_profit: Decimal,
    stop_loss_hit: bool,
    aligned_1s_bps: Decimal,
    aligned_5s_bps: Decimal,
    aligned_15s_bps: Decimal,
}

#[derive(Debug)]
struct AnalysisFrame {
    views: Vec<BtcFiveMinuteMarketView>,
    opportunities: Vec<Opportunity>,
    near_misses: Vec<NearMiss>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum AnalysisDetail {
    Full,
    RuntimeFast,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum MarketPhase {
    Live,
    Upcoming,
    Settled,
    MissingContext,
    UnknownWindow,
}

impl MarketPhase {
    const fn label(self) -> &'static str {
        match self {
            Self::Live => "live",
            Self::Upcoming => "upcoming",
            Self::Settled => "settled",
            Self::MissingContext => "missing_context",
            Self::UnknownWindow => "unknown_window",
        }
    }

    const fn sort_key(self) -> u8 {
        match self {
            Self::Live => 0,
            Self::Upcoming => 1,
            Self::Settled => 2,
            Self::MissingContext => 3,
            Self::UnknownWindow => 4,
        }
    }

    const fn is_live(self) -> bool {
        matches!(self, Self::Live)
    }

    const fn is_upcoming(self) -> bool {
        matches!(self, Self::Upcoming)
    }
}

#[derive(Debug)]
struct BtcFiveMinuteMarketView {
    target_label: String,
    slug: String,
    question: String,
    phase: MarketPhase,
    window_start: Option<DateTime<Utc>>,
    window_end: Option<DateTime<Utc>>,
    seconds_to_start: i64,
    seconds_left: i64,
    liquidity_usdc: Decimal,
    up_ask: String,
    down_ask: String,
    bundle_cost: String,
    current_price: String,
    target_price: String,
    target_price_source: String,
    target_gap_bps: String,
    raw_edge_bps: String,
    spot_move_bps: String,
    spot_move_5s_bps: String,
    spot_move_15s_bps: String,
    micro_acceleration_bps: String,
    dominant_outcome: String,
    strategy_fit: bool,
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone)]
struct ScreenState {
    top: usize,
    show_signals: bool,
    show_near_misses: bool,
    show_positions: bool,
    show_trades: bool,
    show_help: bool,
}

impl ScreenState {
    const fn new(top: usize) -> Self {
        Self {
            top,
            show_signals: true,
            show_near_misses: true,
            show_positions: true,
            show_trades: true,
            show_help: true,
        }
    }

    fn increase_top(&mut self) {
        self.top = self.top.saturating_add(1).min(50);
    }

    fn decrease_top(&mut self) {
        self.top = self.top.saturating_sub(1).max(1);
    }
}

#[derive(Debug)]
struct TerminalSession {
    interactive: bool,
}

impl TerminalSession {
    fn start(enable_hotkeys: bool) -> Result<Self> {
        let interactive = enable_hotkeys && io::stdin().is_terminal() && io::stdout().is_terminal();
        if interactive {
            enable_raw_mode()?;
        }

        Ok(Self { interactive })
    }
}

impl Drop for TerminalSession {
    fn drop(&mut self) {
        if self.interactive {
            let _ = disable_raw_mode();
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum ScreenAction {
    Continue,
    Quit,
}

#[allow(dead_code)]
async fn collect_market_views(
    config: &AppConfig,
    data_client: &MarketDataClient,
    binance_client: &BinanceClient,
    strategy: &BundleArbitrageStrategy,
) -> Result<Vec<BtcFiveMinuteMarketView>> {
    Ok(collect_analysis_frame(
        config,
        data_client,
        binance_client,
        strategy,
        &HashMap::new(),
    )
    .await?
    .views)
}

async fn collect_analysis_frame(
    config: &AppConfig,
    data_client: &MarketDataClient,
    binance_client: &BinanceClient,
    strategy: &BundleArbitrageStrategy,
    market_notional: &HashMap<String, Decimal>,
) -> Result<AnalysisFrame> {
    let snapshot = collect_market_snapshot(config, data_client, binance_client).await?;
    Ok(analyze_market_snapshot(
        config,
        strategy,
        &snapshot,
        market_notional,
        AnalysisDetail::Full,
    ))
}

#[allow(clippy::too_many_arguments)]
async fn collect_runtime_analysis_frame(
    config: &AppConfig,
    data_client: &MarketDataClient,
    binance_client: &BinanceClient,
    strategy: &BundleArbitrageStrategy,
    market_notional: &HashMap<String, Decimal>,
    trigger: Option<&RuntimeTriggerEvent>,
    open_position_slugs: &[String],
    reactive_snapshot_cache: &mut ReactiveMarketSnapshotCache,
) -> Result<(MarketSnapshot, AnalysisFrame, RuntimeAnalysisTiming)> {
    if config.run.reactive {
        let snapshot_started_at = Instant::now();
        let snapshot = collect_reactive_market_snapshot(
            config,
            data_client,
            binance_client,
            trigger,
            open_position_slugs,
            reactive_snapshot_cache,
        )
        .await?;
        let snapshot_ms = duration_ms_u64(snapshot_started_at.elapsed());
        let analysis_started_at = Instant::now();
        let analysis = analyze_market_snapshot(
            config,
            strategy,
            &snapshot,
            market_notional,
            AnalysisDetail::RuntimeFast,
        );
        Ok((
            snapshot,
            analysis,
            RuntimeAnalysisTiming {
                snapshot_ms,
                analysis_ms: duration_ms_u64(analysis_started_at.elapsed()),
            },
        ))
    } else {
        let snapshot_started_at = Instant::now();
        let snapshot = collect_market_snapshot(config, data_client, binance_client).await?;
        let snapshot_ms = duration_ms_u64(snapshot_started_at.elapsed());
        let analysis_started_at = Instant::now();
        let analysis = analyze_market_snapshot(
            config,
            strategy,
            &snapshot,
            market_notional,
            AnalysisDetail::Full,
        );
        Ok((
            snapshot,
            analysis,
            RuntimeAnalysisTiming {
                snapshot_ms,
                analysis_ms: duration_ms_u64(analysis_started_at.elapsed()),
            },
        ))
    }
}

async fn wait_for_runtime_trigger(
    run_config: &crate::config::RunConfig,
    trigger_rx: Option<&mut watch::Receiver<Option<BinanceTriggerEvent>>>,
    polymarket_trigger_rx: Option<&mut watch::Receiver<u64>>,
    max_wait: Option<Duration>,
) -> Option<RuntimeTriggerEvent> {
    if max_wait.is_some_and(|timeout| timeout.is_zero()) {
        return None;
    }

    if !run_config.reactive {
        let poll_wait = Duration::from_secs(run_config.poll_interval_secs.max(1));
        sleep(max_wait.map_or(poll_wait, |timeout| poll_wait.min(timeout))).await;
        return None;
    }

    if trigger_rx.is_none() && polymarket_trigger_rx.is_none() {
        let poll_wait = Duration::from_secs(run_config.poll_interval_secs.max(1));
        sleep(max_wait.map_or(poll_wait, |timeout| poll_wait.min(timeout))).await;
        return None;
    }

    let mut trigger_rx = trigger_rx;
    let mut polymarket_trigger_rx = polymarket_trigger_rx;

    let event_wait = if run_config.reactive_idle_secs == 0 {
        max_wait
    } else {
        let idle_wait = Duration::from_secs(run_config.reactive_idle_secs.max(1));
        Some(max_wait.map_or(idle_wait, |timeout| idle_wait.min(timeout)))
    };

    let mut event = if let Some(event_wait) = event_wait {
        timeout(
            event_wait,
            wait_for_next_runtime_event(&mut trigger_rx, &mut polymarket_trigger_rx),
        )
        .await
        .ok()
        .flatten()?
    } else {
        wait_for_next_runtime_event(&mut trigger_rx, &mut polymarket_trigger_rx).await?
    };

    if run_config.reactive_debounce_ms > 0 {
        sleep(Duration::from_millis(run_config.reactive_debounce_ms)).await;
    }

    if let Some(rx) = trigger_rx.as_mut() {
        while rx.has_changed().unwrap_or(false) {
            if rx.changed().await.is_err() {
                break;
            }
            if let Some(binance_event) = rx.borrow_and_update().clone() {
                event = RuntimeTriggerEvent::from_binance(binance_event);
            }
        }
    }
    if let Some(rx) = polymarket_trigger_rx.as_mut() {
        while rx.has_changed().unwrap_or(false) {
            if rx.changed().await.is_err() {
                break;
            }
            event = RuntimeTriggerEvent::from_polymarket(*rx.borrow_and_update());
        }
    }

    Some(event)
}

async fn wait_for_next_runtime_event(
    trigger_rx: &mut Option<&mut watch::Receiver<Option<BinanceTriggerEvent>>>,
    polymarket_trigger_rx: &mut Option<&mut watch::Receiver<u64>>,
) -> Option<RuntimeTriggerEvent> {
    match (trigger_rx.as_mut(), polymarket_trigger_rx.as_mut()) {
        (Some(binance_rx), Some(poly_rx)) => {
            tokio::select! {
                changed = binance_rx.changed() => {
                    if changed.is_err() {
                        return None;
                    }
                    binance_rx
                        .borrow_and_update()
                        .clone()
                        .map(RuntimeTriggerEvent::from_binance)
                }
                changed = poly_rx.changed() => {
                    if changed.is_err() {
                        return None;
                    }
                    Some(RuntimeTriggerEvent::from_polymarket(
                        *poly_rx.borrow_and_update(),
                    ))
                }
            }
        }
        (Some(binance_rx), None) => {
            if binance_rx.changed().await.is_err() {
                return None;
            }
            binance_rx
                .borrow_and_update()
                .clone()
                .map(RuntimeTriggerEvent::from_binance)
        }
        (None, Some(poly_rx)) => {
            if poly_rx.changed().await.is_err() {
                return None;
            }
            Some(RuntimeTriggerEvent::from_polymarket(
                *poly_rx.borrow_and_update(),
            ))
        }
        (None, None) => None,
    }
}

fn log_runtime_trigger(trigger: Option<&RuntimeTriggerEvent>, config: &AppConfig) {
    if let Some(trigger) = trigger {
        debug!(
            symbol = %trigger.symbol,
            event_time_ms = trigger.event_time_ms,
            event_age_ms = trigger.event_age_ms(),
            received_age_ms = trigger.received_age_ms(),
            price = %trigger.price,
            reactive_debounce_ms = config.run.reactive_debounce_ms,
            source = %trigger.source,
            "reactive trigger received"
        );
    } else {
        debug!(
            reactive_idle_secs = config.run.reactive_idle_secs,
            ", fallback-"
        );
    }
}

fn duration_ms_u64(duration: StdDuration) -> u64 {
    let millis = duration.as_millis();
    if millis == 0 && duration > StdDuration::ZERO {
        1
    } else {
        u64::try_from(millis).unwrap_or(u64::MAX)
    }
}

fn non_negative_i64_to_u64(value: i64) -> u64 {
    u64::try_from(value.max(0)).unwrap_or(u64::MAX)
}

async fn collect_market_snapshot(
    config: &AppConfig,
    data_client: &MarketDataClient,
    binance_client: &BinanceClient,
) -> Result<MarketSnapshot> {
    let markets = data_client
        .fetch_target_markets(&config.strategy.market_targets, config.strategy.max_markets)
        .await?;
    collect_market_snapshot_for_markets(config, markets, data_client, binance_client).await
}

async fn collect_reactive_market_snapshot(
    config: &AppConfig,
    data_client: &MarketDataClient,
    binance_client: &BinanceClient,
    trigger: Option<&RuntimeTriggerEvent>,
    open_position_slugs: &[String],
    reactive_snapshot_cache: &mut ReactiveMarketSnapshotCache,
) -> Result<MarketSnapshot> {
    let lookup_targets =
        runtime_market_targets_for_trigger(&config.strategy.market_targets, trigger);
    let runtime_markets = fetch_current_live_markets(data_client, &lookup_targets).await?;
    if config.run.polymarket_stream.enabled {
        data_client.register_live_markets(&runtime_markets).await;
    }
    let mut live_markets = filter_markets_for_runtime_trigger(runtime_markets, trigger);
    if should_use_live_only_polymarket_books(config) {
        let observed_ts = data_client.current_server_time_secs_fast().await;
        live_markets.retain(|market| market_window_is_live_at(market, observed_ts));
    }
    let markets = merge_markets_by_slug(
        live_markets,
        fetch_supported_markets_for_slugs(data_client, open_position_slugs).await?,
    );
    let reactive_cache = if reactive_component_cache_allowed(trigger) {
        Some(reactive_snapshot_cache)
    } else {
        None
    };
    collect_market_snapshot_for_markets_with_options(
        config,
        markets,
        data_client,
        binance_client,
        true,
        config.run.polymarket_stream.rest_fallback_enabled,
        reactive_cache,
    )
    .await
}

fn reactive_component_cache_allowed(trigger: Option<&RuntimeTriggerEvent>) -> bool {
    trigger.is_none_or(|trigger| !trigger.source.starts_with("Polymarket::"))
}

fn should_use_live_only_polymarket_books(config: &AppConfig) -> bool {
    config.run.polymarket_stream.enabled && !config.run.polymarket_stream.rest_fallback_enabled
}

fn market_window_is_live_at(market: &BinaryMarket, observed_ts: i64) -> bool {
    let Some(start_ts) = market.window_start_ts() else {
        return false;
    };
    let Some(window_secs) = market.window_secs() else {
        return false;
    };

    observed_ts >= start_ts && observed_ts < start_ts.saturating_add(window_secs)
}

fn filter_markets_for_runtime_trigger(
    markets: Vec<BinaryMarket>,
    trigger: Option<&RuntimeTriggerEvent>,
) -> Vec<BinaryMarket> {
    let Some(trigger) = trigger else {
        return markets;
    };
    if !trigger.source.starts_with("Binance::") && !trigger.source.starts_with("Coinbase::") {
        return markets;
    }

    let normalized_symbol = trigger.symbol.to_ascii_uppercase();
    let filtered = markets
        .iter()
        .filter(|market| {
            market.target().is_some_and(|target| {
                target
                    .binance_symbol()
                    .eq_ignore_ascii_case(normalized_symbol.as_str())
            })
        })
        .cloned()
        .collect::<Vec<_>>();
    if filtered.is_empty() {
        markets
    } else {
        filtered
    }
}

async fn fetch_current_live_markets(
    data_client: &MarketDataClient,
    targets: &[MarketTarget],
) -> Result<Vec<BinaryMarket>> {
    data_client.fetch_cached_current_live_markets(targets).await
}

fn runtime_market_targets_for_trigger(
    configured_targets: &[MarketTarget],
    trigger: Option<&RuntimeTriggerEvent>,
) -> Vec<MarketTarget> {
    let configured = dedupe_market_targets(configured_targets);
    let Some(trigger) = trigger else {
        return configured;
    };
    if !trigger.source.starts_with("Binance::") && !trigger.source.starts_with("Coinbase::") {
        return configured;
    }

    let normalized_symbol = trigger.symbol.to_ascii_uppercase();
    let filtered = configured
        .iter()
        .copied()
        .filter(|target| {
            target
                .binance_symbol()
                .eq_ignore_ascii_case(normalized_symbol.as_str())
        })
        .collect::<Vec<_>>();
    if filtered.is_empty() {
        configured
    } else {
        filtered
    }
}

fn dedupe_market_targets(targets: &[MarketTarget]) -> Vec<MarketTarget> {
    let mut deduped = Vec::with_capacity(targets.len());
    for target in targets {
        if !deduped.contains(target) {
            deduped.push(*target);
        }
    }
    deduped
}

async fn collect_market_snapshot_for_markets(
    config: &AppConfig,
    markets: Vec<BinaryMarket>,
    data_client: &MarketDataClient,
    binance_client: &BinanceClient,
) -> Result<MarketSnapshot> {
    collect_market_snapshot_for_markets_with_options(
        config,
        markets,
        data_client,
        binance_client,
        true,
        true,
        None,
    )
    .await
}

async fn collect_exit_market_snapshot(
    config: &AppConfig,
    data_client: &MarketDataClient,
    binance_client: &BinanceClient,
    open_position_slugs: &[String],
    reactive_snapshot_cache: &mut ReactiveMarketSnapshotCache,
) -> Result<MarketSnapshot> {
    let markets = fetch_supported_markets_for_slugs(data_client, open_position_slugs).await?;
    collect_market_snapshot_for_markets_with_options(
        config,
        markets,
        data_client,
        binance_client,
        false,
        true,
        Some(reactive_snapshot_cache),
    )
    .await
}

#[allow(clippy::too_many_lines)]
async fn collect_market_snapshot_for_markets_with_options(
    config: &AppConfig,
    markets: Vec<BinaryMarket>,
    data_client: &MarketDataClient,
    binance_client: &BinanceClient,
    include_trade_flows: bool,
    allow_rest_orderbook_fallback: bool,
    reactive_snapshot_cache: Option<&mut ReactiveMarketSnapshotCache>,
) -> Result<MarketSnapshot> {
    if let Some(cache) = reactive_snapshot_cache {
        return collect_market_snapshot_for_markets_cached(
            config,
            markets,
            data_client,
            binance_client,
            include_trade_flows,
            allow_rest_orderbook_fallback,
            cache,
        )
        .await;
    }
    let _ = reactive_snapshot_cache;
    fetch_fresh_market_snapshot_for_markets(
        config,
        markets,
        data_client,
        binance_client,
        include_trade_flows,
        allow_rest_orderbook_fallback,
    )
    .await
}

async fn collect_market_snapshot_for_markets_cached(
    config: &AppConfig,
    markets: Vec<BinaryMarket>,
    data_client: &MarketDataClient,
    binance_client: &BinanceClient,
    include_trade_flows: bool,
    allow_rest_orderbook_fallback: bool,
    reactive_snapshot_cache: &mut ReactiveMarketSnapshotCache,
) -> Result<MarketSnapshot> {
    const REACTIVE_COMPONENT_TTL_MS: u128 = 500;

    let observed_ts = data_client.current_server_time_secs_fast().await;
    let contexts =
        fetch_binance_contexts_for_markets(config, &markets, binance_client, observed_ts).await?;

    let desired_slugs = markets
        .iter()
        .map(|market| market.slug.clone())
        .collect::<HashSet<_>>();
    reactive_snapshot_cache
        .entries
        .retain(|slug, _| desired_slugs.contains(slug));

    let mut missing_markets = Vec::new();
    for market in &markets {
        let is_fresh = reactive_snapshot_cache
            .entries
            .get(&market.slug)
            .is_some_and(|entry| {
                entry.cached_at.elapsed().as_millis() <= REACTIVE_COMPONENT_TTL_MS
                    && (!include_trade_flows || entry.trade_flow.is_some())
            });
        if !is_fresh {
            missing_markets.push(market.clone());
        }
    }

    if !missing_markets.is_empty() {
        let (fresh_books, fresh_trade_flows) = fetch_market_books_and_trade_flows(
            config,
            &missing_markets,
            data_client,
            &contexts,
            include_trade_flows,
            allow_rest_orderbook_fallback,
        )
        .await?;
        let refreshed_at = Instant::now();
        for market in &missing_markets {
            let mut market_books = HashMap::with_capacity(2);
            for token_id in [&market.outcome_a_token_id, &market.outcome_b_token_id] {
                if let Some(book) = fresh_books.get(token_id) {
                    market_books.insert(token_id.clone(), book.clone());
                }
            }
            reactive_snapshot_cache.entries.insert(
                market.slug.clone(),
                CachedReactiveMarketComponents {
                    market: market.clone(),
                    books: market_books,
                    trade_flow: fresh_trade_flows.get(&market.slug).copied(),
                    cached_at: refreshed_at,
                },
            );
        }
    }

    let mut snapshot = MarketSnapshot {
        markets: Vec::with_capacity(markets.len()),
        books: HashMap::with_capacity(markets.len().saturating_mul(2)),
        contexts: HashMap::with_capacity(markets.len()),
        trade_flows: HashMap::with_capacity(markets.len()),
    };
    for market in markets {
        if let Some(entry) = reactive_snapshot_cache.entries.get(&market.slug) {
            snapshot.markets.push(entry.market.clone());
            snapshot.books.extend(entry.books.clone());
            if let Some(context) = contexts.get(&entry.market.slug) {
                snapshot
                    .contexts
                    .insert(entry.market.slug.clone(), context.clone());
            }
            if let Some(trade_flow) = entry.trade_flow {
                snapshot
                    .trade_flows
                    .insert(entry.market.slug.clone(), trade_flow);
            }
        } else {
            snapshot.markets.push(market);
        }
    }

    Ok(snapshot)
}

async fn fetch_binance_contexts_for_markets(
    config: &AppConfig,
    markets: &[BinaryMarket],
    binance_client: &BinanceClient,
    observed_ts: i64,
) -> Result<HashMap<String, BtcFiveMinuteContext>> {
    let context_results = join_all(markets.iter().map(|market| async move {
        (
            market.slug.clone(),
            market.target(),
            binance_client
                .market_context_at_timestamp(market, observed_ts)
                .await,
        )
    }))
    .await;
    let mut contexts = HashMap::<String, BtcFiveMinuteContext>::with_capacity(markets.len());
    for (slug, target, context_result) in context_results {
        match context_result {
            Ok(Some(context)) => {
                contexts.insert(slug, context);
            }
            Ok(None) => {}
            Err(AppError::InvalidMarket(message)) if message.contains("Binance") => {
                debug!(
                    slug = %slug,
                    target = ?target,
                    reason = %message,
                    "skipping market context after transient Binance data gap"
                );
            }
            Err(error) => return Err(error),
        }
    }
    debug!(contexts = contexts.len(), "Binance");
    log_market_context_source_health(config, &contexts);
    Ok(contexts)
}

fn log_market_context_source_health(
    config: &AppConfig,
    contexts: &HashMap<String, BtcFiveMinuteContext>,
) {
    let mut binance_trade = 0_usize;
    let mut coinbase_ticker = 0_usize;
    let mut chainlink_rtds = 0_usize;
    let mut binance_rest_latest = 0_usize;
    let mut binance_rest_1m_fallback = 0_usize;
    let mut other_source = 0_usize;
    let mut missing_quote_age = 0_usize;
    let mut stale_quote = 0_usize;
    let mut missing_book = 0_usize;
    let mut stale_book = 0_usize;
    let max_quote_age_ms = config.strategy.codex_sentinel_v1_max_live_quote_age_ms;
    let max_book_age_ms = config.strategy.codex_scalp_probe_v1_max_book_age_ms;

    for context in contexts.values() {
        match context.current_spot_source.as_str() {
            "Binance::Trade" => binance_trade += 1,
            "Coinbase::Ticker" => coinbase_ticker += 1,
            "Chainlink::RTDS" => chainlink_rtds += 1,
            "Binance::RestLatest" => binance_rest_latest += 1,
            "Binance::Rest1mFallback" => binance_rest_1m_fallback += 1,
            _ => other_source += 1,
        }

        match context.current_spot_received_age_ms {
            Some(age_ms) if age_ms >= 0 && age_ms <= max_quote_age_ms => {}
            Some(_) => stale_quote += 1,
            None => missing_quote_age += 1,
        }

        match context.exchange_book_age_ms {
            Some(age_ms) if age_ms >= 0 && age_ms <= max_book_age_ms => {}
            Some(_) => stale_book += 1,
            None => missing_book += 1,
        }
    }

    if !contexts.is_empty() {
        debug!(
            contexts = contexts.len(),
            binance_trade,
            coinbase_ticker,
            chainlink_rtds,
            binance_rest_latest,
            binance_rest_1m_fallback,
            other_source,
            missing_quote_age,
            stale_quote,
            missing_book,
            stale_book,
            max_quote_age_ms,
            max_book_age_ms,
            "runtime market-data context source mix"
        );
    }
}

async fn fetch_market_books_and_trade_flows(
    config: &AppConfig,
    markets: &[BinaryMarket],
    data_client: &MarketDataClient,
    contexts: &HashMap<String, BtcFiveMinuteContext>,
    include_trade_flows: bool,
    allow_rest_orderbook_fallback: bool,
) -> Result<(
    HashMap<String, OrderBook>,
    HashMap<String, TradeFlowSummary>,
)> {
    let token_ids = markets
        .iter()
        .flat_map(|market| {
            [
                market.outcome_a_token_id.clone(),
                market.outcome_b_token_id.clone(),
            ]
        })
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();

    if config.run.polymarket_stream.enabled {
        data_client.register_live_markets(markets).await;
    }

    let books = if config.run.polymarket_stream.enabled {
        data_client
            .fetch_order_books_live_first_with_fallback(
                &token_ids,
                config.run.polymarket_stream.book_staleness_ms,
                allow_rest_orderbook_fallback,
            )
            .await?
    } else {
        data_client.fetch_order_books(&token_ids).await?
    };
    debug!(books = books.len(), "order books loaded");

    log_market_context_source_health(config, contexts);

    let trade_flows = if include_trade_flows {
        let trade_flow_windows = markets
            .iter()
            .filter_map(|market| {
                let start_ts = market.window_start_ts()?;
                let context = contexts.get(&market.slug)?;
                let window_secs = market.window_secs().unwrap_or(300);
                let quote_ts = start_ts + (window_secs - context.seconds_left).max(0);
                Some(TradeFlowWindow {
                    slug: market.slug.clone(),
                    condition_id: market.condition_id.clone(),
                    start_ts_ms: start_ts.saturating_mul(1000),
                    end_ts_ms: quote_ts.saturating_mul(1000),
                })
            })
            .collect::<Vec<_>>();
        if config.run.polymarket_stream.enabled {
            data_client
                .fetch_trade_flow_summaries_live_first(&trade_flow_windows, false)
                .await?
        } else {
            data_client
                .fetch_trade_flow_summaries(&trade_flow_windows)
                .await?
        }
    } else {
        HashMap::new()
    };
    debug!(trade_flows = trade_flows.len(), "trade-flow Polymarket");

    Ok((books, trade_flows))
}

#[allow(clippy::too_many_lines)]
async fn fetch_fresh_market_snapshot_for_markets(
    config: &AppConfig,
    markets: Vec<BinaryMarket>,
    data_client: &MarketDataClient,
    binance_client: &BinanceClient,
    include_trade_flows: bool,
    allow_rest_orderbook_fallback: bool,
) -> Result<MarketSnapshot> {
    let observed_ts = data_client.current_server_time_secs_fast().await;
    debug!(markets = markets.len(), "fresh market snapshot started");

    let token_ids = markets
        .iter()
        .flat_map(|market| {
            [
                market.outcome_a_token_id.clone(),
                market.outcome_b_token_id.clone(),
            ]
        })
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();

    if config.run.polymarket_stream.enabled {
        data_client.register_live_markets(&markets).await;
    }

    let books = if config.run.polymarket_stream.enabled {
        data_client
            .fetch_order_books_live_first_with_fallback(
                &token_ids,
                config.run.polymarket_stream.book_staleness_ms,
                allow_rest_orderbook_fallback,
            )
            .await?
    } else {
        data_client.fetch_order_books(&token_ids).await?
    };
    debug!(
        books = books.len(),
        "fresh market snapshot order books loaded"
    );

    let context_results = join_all(markets.iter().map(|market| async move {
        (
            market.slug.clone(),
            market.target(),
            binance_client
                .market_context_at_timestamp(market, observed_ts)
                .await,
        )
    }))
    .await;
    let mut contexts = HashMap::<String, BtcFiveMinuteContext>::with_capacity(markets.len());
    for (slug, target, context_result) in context_results {
        match context_result {
            Ok(Some(context)) => {
                contexts.insert(slug, context);
            }
            Ok(None) => {}
            Err(AppError::InvalidMarket(message)) if message.contains("Binance") => {
                debug!(
                    slug = %slug,
                    target = ?target,
                    reason = %message,
                    "skipping market context after transient Binance data gap"
                );
            }
            Err(error) => return Err(error),
        }
    }
    debug!(contexts = contexts.len(), "Binance");

    let trade_flows = if include_trade_flows {
        let trade_flow_windows = markets
            .iter()
            .filter_map(|market| {
                let start_ts = market.window_start_ts()?;
                let context = contexts.get(&market.slug)?;
                let window_secs = market.window_secs().unwrap_or(300);
                let quote_ts = start_ts + (window_secs - context.seconds_left).max(0);
                Some(TradeFlowWindow {
                    slug: market.slug.clone(),
                    condition_id: market.condition_id.clone(),
                    start_ts_ms: start_ts.saturating_mul(1000),
                    end_ts_ms: quote_ts.saturating_mul(1000),
                })
            })
            .collect::<Vec<_>>();
        if config.run.polymarket_stream.enabled {
            // In the reactive hot path we prefer fresh live flow over repeated REST backfill.
            data_client
                .fetch_trade_flow_summaries_live_first(&trade_flow_windows, false)
                .await?
        } else {
            data_client
                .fetch_trade_flow_summaries(&trade_flow_windows)
                .await?
        }
    } else {
        HashMap::new()
    };
    debug!(trade_flows = trade_flows.len(), "trade-flow Polymarket");

    Ok(MarketSnapshot {
        markets,
        books,
        contexts,
        trade_flows,
    })
}

async fn fetch_supported_markets_for_slugs(
    data_client: &MarketDataClient,
    slugs: &[String],
) -> Result<Vec<BinaryMarket>> {
    let slug_results = join_all(
        slugs
            .iter()
            .map(|slug| async move { data_client.fetch_supported_market_by_slug(slug).await }),
    )
    .await;
    let mut markets = Vec::with_capacity(slugs.len());
    for result in slug_results {
        if let Some(market) = result? {
            markets.push(market);
        }
    }
    Ok(markets)
}

fn merge_markets_by_slug(
    primary: Vec<BinaryMarket>,
    secondary: Vec<BinaryMarket>,
) -> Vec<BinaryMarket> {
    let mut merged = Vec::with_capacity(primary.len() + secondary.len());
    let mut seen = HashSet::with_capacity(primary.len() + secondary.len());
    for market in primary.into_iter().chain(secondary) {
        if seen.insert(market.slug.clone()) {
            merged.push(market);
        }
    }
    merged
}

struct RevalidationRequest<'a> {
    mode: BotMode,
    config: &'a AppConfig,
    data_client: &'a MarketDataClient,
    binance_client: &'a BinanceClient,
    strategy: &'a BundleArbitrageStrategy,
    runtime_snapshot: &'a MarketSnapshot,
    candidate: &'a Opportunity,
    market_notional: &'a HashMap<String, Decimal>,
}

struct RevalidationNearMissLog<'a> {
    strategy: &'a BundleArbitrageStrategy,
    candidate: &'a Opportunity,
    market: &'a BinaryMarket,
    books: &'a HashMap<String, OrderBook>,
    market_notional: &'a HashMap<String, Decimal>,
    contexts: &'a HashMap<String, BtcFiveMinuteContext>,
    trade_flows: &'a HashMap<String, TradeFlowSummary>,
    source: &'static str,
}

async fn revalidate_selected_opportunity(
    request: RevalidationRequest<'_>,
) -> Result<Option<Opportunity>> {
    if should_use_in_memory_revalidate(request.mode, request.config) {
        return Ok(revalidate_opportunity_from_snapshot(
            request.strategy,
            request.runtime_snapshot,
            request.candidate,
            request.market_notional,
        ));
    }

    revalidate_opportunity(
        request.config,
        request.data_client,
        request.binance_client,
        request.strategy,
        request.candidate,
        request.market_notional,
    )
    .await
}

fn should_use_in_memory_revalidate(mode: BotMode, config: &AppConfig) -> bool {
    mode == BotMode::Paper && config.run.reactive && should_use_live_only_polymarket_books(config)
}

fn revalidate_opportunity_from_snapshot(
    strategy: &BundleArbitrageStrategy,
    snapshot: &MarketSnapshot,
    candidate: &Opportunity,
    market_notional: &HashMap<String, Decimal>,
) -> Option<Opportunity> {
    let market = snapshot
        .markets
        .iter()
        .find(|market| market.slug == candidate.slug)?;
    let context = snapshot.contexts.get(&candidate.slug)?;
    let mut books = HashMap::with_capacity(2);
    for token_id in [&market.outcome_a_token_id, &market.outcome_b_token_id] {
        books.insert(token_id.clone(), snapshot.books.get(token_id)?.clone());
    }
    let mut contexts = HashMap::with_capacity(1);
    contexts.insert(market.slug.clone(), context.clone());
    let mut trade_flows = HashMap::with_capacity(1);
    if let Some(trade_flow) = snapshot.trade_flows.get(&market.slug) {
        trade_flows.insert(market.slug.clone(), *trade_flow);
    }

    let refreshed = strategy
        .find_opportunities(
            std::slice::from_ref(market),
            &books,
            market_notional,
            &contexts,
            &trade_flows,
        )
        .into_iter()
        .next();

    if refreshed.is_none() {
        log_revalidation_near_miss(RevalidationNearMissLog {
            strategy,
            candidate,
            market,
            books: &books,
            market_notional,
            contexts: &contexts,
            trade_flows: &trade_flows,
            source: "in-memory",
        });
    }

    refreshed
}

fn log_revalidation_near_miss(input: RevalidationNearMissLog<'_>) {
    let revalidation_near_miss = input
        .strategy
        .find_near_misses(
            std::slice::from_ref(input.market),
            input.books,
            input.market_notional,
            input.contexts,
            input.trade_flows,
            1,
        )
        .into_iter()
        .next();

    match revalidation_near_miss {
        Some(near_miss) => warn!(
            source = input.source,
            slug = %input.candidate.slug,
            candidate_kind = input.candidate.kind.as_str(),
            candidate_edge_bps = input.candidate.edge_bps,
            candidate_required_usdc = %input.candidate.required_usdc.round_dp(4),
            candidate_primary_ask = %input.candidate.primary_outcome_ask_price.round_dp(4),
            candidate_signal_strength_bps = %input.candidate.signal_strength_bps.round_dp(2),
            near_miss_kind = near_miss.kind.as_str(),
            near_miss_reason = %near_miss.reason,
            near_miss_primary_ask = ?near_miss.primary_outcome_ask_price.map(|price| price.round_dp(4)),
            near_miss_target_gap_bps = %near_miss.target_gap_bps.round_dp(2),
            near_miss_shortfall = %near_miss.shortfall_label,
            "revalidated opportunity disappeared before execution"
        ),
        None => warn!(
            source = input.source,
            slug = %input.candidate.slug,
            candidate_kind = input.candidate.kind.as_str(),
            candidate_edge_bps = input.candidate.edge_bps,
            candidate_required_usdc = %input.candidate.required_usdc.round_dp(4),
            candidate_primary_ask = %input.candidate.primary_outcome_ask_price.round_dp(4),
            candidate_signal_strength_bps = %input.candidate.signal_strength_bps.round_dp(2),
            "revalidated opportunity disappeared before execution without near-miss detail"
        ),
    }
}

async fn revalidate_opportunity(
    config: &AppConfig,
    data_client: &MarketDataClient,
    binance_client: &BinanceClient,
    strategy: &BundleArbitrageStrategy,
    candidate: &Opportunity,
    market_notional: &HashMap<String, Decimal>,
) -> Result<Option<Opportunity>> {
    let Some(market) = data_client
        .fetch_supported_market_by_slug(&candidate.slug)
        .await?
    else {
        return Ok(None);
    };

    let token_ids = vec![
        market.outcome_a_token_id.clone(),
        market.outcome_b_token_id.clone(),
    ];
    if config.run.polymarket_stream.enabled {
        data_client
            .register_live_markets(std::slice::from_ref(&market))
            .await;
    }
    let books = if config.run.polymarket_stream.enabled {
        data_client
            .fetch_order_books_live_first_with_fallback(
                &token_ids,
                config.run.polymarket_stream.book_staleness_ms,
                config.run.polymarket_stream.rest_fallback_enabled,
            )
            .await?
    } else {
        data_client.fetch_order_books(&token_ids).await?
    };

    let observed_ts = data_client.current_server_time_secs_fast().await;
    let context = match binance_client
        .market_context_at_timestamp(&market, observed_ts)
        .await
    {
        Ok(Some(context)) => context,
        Ok(None) => return Ok(None),
        Err(AppError::InvalidMarket(message)) if message.contains("Binance") => {
            debug!(
                slug = %market.slug,
                reason = %message,
                "skipping revalidation after transient Binance data gap"
            );
            return Ok(None);
        }
        Err(error) => return Err(error),
    };

    let mut contexts = HashMap::with_capacity(1);
    contexts.insert(market.slug.clone(), context);
    let trade_flow_windows = market
        .window_start_ts()
        .and_then(|start_ts| {
            contexts.get(&market.slug).map(|context| TradeFlowWindow {
                slug: market.slug.clone(),
                condition_id: market.condition_id.clone(),
                start_ts_ms: start_ts.saturating_mul(1000),
                end_ts_ms: (start_ts
                    + (market.window_secs().unwrap_or(300) - context.seconds_left).max(0))
                .saturating_mul(1000),
            })
        })
        .into_iter()
        .collect::<Vec<_>>();
    let trade_flows = if config.run.polymarket_stream.enabled {
        // Revalidation should confirm freshness, not stall on historical backfill.
        data_client
            .fetch_trade_flow_summaries_live_first(&trade_flow_windows, false)
            .await?
    } else {
        data_client
            .fetch_trade_flow_summaries(&trade_flow_windows)
            .await?
    };

    let refreshed = strategy
        .find_opportunities(
            std::slice::from_ref(&market),
            &books,
            market_notional,
            &contexts,
            &trade_flows,
        )
        .into_iter()
        .next();

    if refreshed.is_none() {
        let revalidation_near_miss = strategy
            .find_near_misses(
                std::slice::from_ref(&market),
                &books,
                market_notional,
                &contexts,
                &trade_flows,
                1,
            )
            .into_iter()
            .next();

        match revalidation_near_miss {
            Some(near_miss) => warn!(
                slug = %candidate.slug,
                candidate_kind = candidate.kind.as_str(),
                candidate_edge_bps = candidate.edge_bps,
                candidate_required_usdc = %candidate.required_usdc.round_dp(4),
                candidate_primary_ask = %candidate.primary_outcome_ask_price.round_dp(4),
                candidate_signal_strength_bps = %candidate.signal_strength_bps.round_dp(2),
                near_miss_kind = near_miss.kind.as_str(),
                near_miss_reason = %near_miss.reason,
                near_miss_primary_ask = ?near_miss.primary_outcome_ask_price.map(|price| price.round_dp(4)),
                near_miss_target_gap_bps = %near_miss.target_gap_bps.round_dp(2),
                near_miss_shortfall = %near_miss.shortfall_label,
                "revalidated opportunity disappeared before execution"
            ),
            None => warn!(
                slug = %candidate.slug,
                candidate_kind = candidate.kind.as_str(),
                candidate_edge_bps = candidate.edge_bps,
                candidate_required_usdc = %candidate.required_usdc.round_dp(4),
                candidate_primary_ask = %candidate.primary_outcome_ask_price.round_dp(4),
                candidate_signal_strength_bps = %candidate.signal_strength_bps.round_dp(2),
                "revalidated opportunity disappeared before execution without a near-miss explanation"
            ),
        }
    }

    if let Some(opportunity) = refreshed.as_ref()
        && (opportunity.kind != candidate.kind
            || opportunity.edge_bps != candidate.edge_bps
            || opportunity.required_usdc != candidate.required_usdc)
    {
        warn!(
            slug = %opportunity.slug,
            previous_kind = candidate.kind.as_str(),
            refreshed_kind = opportunity.kind.as_str(),
            previous_edge_bps = candidate.edge_bps,
            refreshed_edge_bps = opportunity.edge_bps,
            previous_required_usdc = %candidate.required_usdc,
            refreshed_required_usdc = %opportunity.required_usdc,
            "revalidation refreshed opportunity"
        );
    }

    Ok(refreshed)
}

fn analyze_market_snapshot(
    config: &AppConfig,
    strategy: &BundleArbitrageStrategy,
    snapshot: &MarketSnapshot,
    market_notional: &HashMap<String, Decimal>,
    detail: AnalysisDetail,
) -> AnalysisFrame {
    let opportunities = strategy.find_opportunities(
        &snapshot.markets,
        &snapshot.books,
        market_notional,
        &snapshot.contexts,
        &snapshot.trade_flows,
    );
    let near_misses = if detail == AnalysisDetail::Full || opportunities.is_empty() {
        strategy.find_near_misses(
            &snapshot.markets,
            &snapshot.books,
            market_notional,
            &snapshot.contexts,
            &snapshot.trade_flows,
            config.strategy.max_markets.min(12),
        )
    } else {
        Vec::new()
    };
    let opportunity_by_slug = opportunities
        .iter()
        .cloned()
        .map(|opportunity| (opportunity.slug.clone(), opportunity))
        .collect::<HashMap<_, _>>();
    let views = if detail == AnalysisDetail::Full {
        let mut views = snapshot
            .markets
            .iter()
            .map(|market| {
                build_market_view(
                    config,
                    market,
                    &snapshot.books,
                    &snapshot.contexts,
                    &opportunity_by_slug,
                )
            })
            .collect::<Vec<_>>();
        views.sort_by(compare_market_views);
        views
    } else {
        Vec::new()
    };

    AnalysisFrame {
        views,
        opportunities,
        near_misses,
    }
}

fn build_market_view(
    config: &AppConfig,
    market: &BinaryMarket,
    books: &HashMap<String, OrderBook>,
    contexts: &HashMap<String, BtcFiveMinuteContext>,
    opportunities: &HashMap<String, Opportunity>,
) -> BtcFiveMinuteMarketView {
    let window_start_ts = market.window_start_ts();
    let window_secs = market.window_secs().unwrap_or(300);
    let target_label = market
        .target()
        .map_or_else(|| "n/a".to_owned(), |target| target.label().to_owned());
    let up_ask = market
        .token_for_outcome("up")
        .and_then(|token_id| books.get(token_id))
        .and_then(OrderBook::best_ask)
        .map(|level| level.price);
    let down_ask = market
        .token_for_outcome("down")
        .and_then(|token_id| books.get(token_id))
        .and_then(OrderBook::best_ask)
        .map(|level| level.price);
    let bundle_cost = up_ask
        .zip(down_ask)
        .map(|(up_ask, down_ask)| (up_ask + down_ask).round_dp(6));
    let raw_edge_bps = bundle_cost.map(|bundle_cost| {
        ((Decimal::ONE
            - (bundle_cost
                + Decimal::from(config.strategy.assumed_fee_bps) / Decimal::from(10_000_u32)))
            * Decimal::from(10_000_u32))
        .round_dp(2)
    });
    let context = contexts.get(&market.slug);
    let (phase, seconds_to_start, seconds_left) =
        classify_market_phase(window_start_ts, window_secs, context);

    BtcFiveMinuteMarketView {
        target_label,
        slug: market.slug.clone(),
        question: market.question.clone(),
        phase,
        window_start: window_start_ts.and_then(|timestamp| DateTime::from_timestamp(timestamp, 0)),
        window_end: window_start_ts
            .and_then(|timestamp| DateTime::from_timestamp(timestamp + window_secs, 0)),
        seconds_to_start,
        seconds_left,
        liquidity_usdc: market.liquidity_usdc,
        up_ask: display_optional_decimal(up_ask),
        down_ask: display_optional_decimal(down_ask),
        bundle_cost: display_optional_decimal(bundle_cost),
        target_price: context.map_or_else(
            || "/".to_owned(),
            |context| context.target_price.round_dp(4).to_string(),
        ),
        target_price_source: context.map_or_else(
            || "/".to_owned(),
            |context| context.target_price_source.as_str().to_owned(),
        ),
        target_gap_bps: context.map_or_else(
            || "/".to_owned(),
            |context| context.target_gap_bps.round_dp(2).to_string(),
        ),
        current_price: context.map_or_else(
            || "/".to_owned(),
            |context| context.current_spot_price.round_dp(4).to_string(),
        ),
        raw_edge_bps: display_optional_decimal(raw_edge_bps),
        spot_move_bps: context.map_or_else(
            || "/".to_owned(),
            |context| context.spot_move_bps.to_string(),
        ),
        spot_move_5s_bps: context.map_or_else(
            || "/".to_owned(),
            |context| context.spot_move_5s_bps.to_string(),
        ),
        dominant_outcome: context.map_or_else(
            || "/".to_owned(),
            |context| context.dominant_outcome.clone(),
        ),
        spot_move_15s_bps: context.map_or_else(
            || "/".to_owned(),
            |context| context.spot_move_15s_bps.to_string(),
        ),
        micro_acceleration_bps: context.map_or_else(
            || "/".to_owned(),
            |context| context.micro_acceleration_bps.to_string(),
        ),
        strategy_fit: opportunities.contains_key(&market.slug),
    }
}

fn classify_market_phase(
    start_ts: Option<i64>,
    window_secs: i64,
    context: Option<&BtcFiveMinuteContext>,
) -> (MarketPhase, i64, i64) {
    let now = Utc::now().timestamp();
    match (start_ts, context) {
        (_, Some(context)) => (MarketPhase::Live, 0, context.seconds_left),
        (Some(start_ts), None) if now < start_ts => (MarketPhase::Upcoming, start_ts - now, 0),
        (Some(start_ts), None) if now >= start_ts + window_secs => (MarketPhase::Settled, 0, 0),
        (Some(_), None) => (MarketPhase::MissingContext, 0, 0),
        (None, None) => (MarketPhase::UnknownWindow, 0, 0),
    }
}

fn compare_market_views(
    left: &BtcFiveMinuteMarketView,
    right: &BtcFiveMinuteMarketView,
) -> Ordering {
    left.phase
        .sort_key()
        .cmp(&right.phase.sort_key())
        .then_with(|| left.window_start.cmp(&right.window_start))
        .then_with(|| right.liquidity_usdc.cmp(&left.liquidity_usdc))
}

fn display_optional_decimal(value: Option<Decimal>) -> String {
    if value.is_none() {
        return "n/a".to_owned();
    }

    value.map_or_else(|| "n/a".to_owned(), |value| value.to_string())
}

#[allow(dead_code)]
fn render_market_table(views: &[BtcFiveMinuteMarketView], top: usize) -> String {
    let displayed = views.iter().take(top).collect::<Vec<_>>();
    let current_slug = displayed
        .iter()
        .find(|view| view.phase.is_live())
        .map(|view| view.slug.as_str());
    let next_slug = displayed
        .iter()
        .find(|view| view.phase.is_upcoming())
        .map(|view| view.slug.as_str());

    let mut output = String::new();
    let _ = writeln!(
        output,
        "{:<3} {:<8} {:<12} {:<11} {:<10} {:>6} {:>6} {:>7} {:>9} {:>9} {:<8} {:<4} Slug",
        "#", "Tag", "Phase", "UTC", "Timer", "Up", "Down", "Bundle", "Edge", "Spot", "Dir", "Fit"
    );
    let _ = writeln!(output, "{}", "-".repeat(116));

    for (index, view) in displayed.iter().enumerate() {
        let _ = writeln!(
            output,
            "{:<3} {:<8} {:<12} {:<11} {:<10} {:>6} {:>6} {:>7} {:>9} {:>9} {:<8} {:<4} {}",
            index + 1,
            market_tag(view, current_slug, next_slug),
            view.phase.label(),
            format_window(view),
            format_timer(view),
            view.up_ask,
            view.down_ask,
            view.bundle_cost,
            view.raw_edge_bps,
            view.spot_move_bps,
            truncate_text(&view.dominant_outcome, 8),
            yes_no(view.strategy_fit),
            view.slug
        );
    }

    output
}

fn render_opportunity_table(opportunities: &[Opportunity], top: usize) -> String {
    let mut output = String::new();
    let _ = writeln!(
        output,
        "{:<3} {:>8} {:>9} {:>8} {:>9} {:>10} {:>6} {:<9} Slug",
        "#", "Edge", "Spot", "Shares", "USDC", "Profit", "Secs", "Dir"
    );
    let _ = writeln!(output, "{}", "-".repeat(88));

    for (index, opportunity) in opportunities.iter().take(top).enumerate() {
        let _ = writeln!(
            output,
            "{:<3} {:>8} {:>9} {:>8} {:>9} {:>10} {:>6} {:<9} {}",
            index + 1,
            opportunity.edge_bps,
            opportunity.spot_move_bps.round_dp(2),
            opportunity.tradable_shares,
            opportunity.required_usdc,
            opportunity.expected_profit,
            opportunity.seconds_left,
            truncate_text(&opportunity.dominant_outcome, 9),
            opportunity.slug
        );
    }

    output
}

#[allow(dead_code)]
fn market_tag<'a>(
    view: &BtcFiveMinuteMarketView,
    current_slug: Option<&'a str>,
    next_slug: Option<&'a str>,
) -> &'static str {
    if view.strategy_fit {
        "FIT"
    } else if current_slug == Some(view.slug.as_str()) {
        "LIVE"
    } else if next_slug == Some(view.slug.as_str()) {
        "NEXT"
    } else {
        ""
    }
}

#[allow(dead_code)]
fn format_window(view: &BtcFiveMinuteMarketView) -> String {
    match (view.window_start, view.window_end) {
        (Some(start), Some(end)) => format!("{}-{}", start.format("%H:%M"), end.format("%H:%M")),
        _ => "n/a".to_owned(),
    }
}

#[allow(dead_code)]
fn format_timer(view: &BtcFiveMinuteMarketView) -> String {
    if view.phase.is_live() {
        format!("{}s", view.seconds_left)
    } else if view.phase.is_upcoming() {
        format!("{}s", view.seconds_to_start)
    } else {
        "-".to_owned()
    }
}

fn truncate_text(value: &str, max_chars: usize) -> String {
    let value = sanitize_legacy_mojibake(value);
    let chars = value.chars().collect::<Vec<_>>();
    if chars.len() <= max_chars {
        return value;
    }

    let mut truncated = chars.into_iter().take(max_chars).collect::<String>();
    if max_chars > 1 {
        let _removed = truncated.pop();
        truncated.push_str("...");
    }
    truncated
}

async fn watch_markets(
    config: &AppConfig,
    data_client: &MarketDataClient,
    binance_client: &BinanceClient,
    strategy: &BundleArbitrageStrategy,
    top: usize,
    refresh_secs: u64,
    cycles: Option<usize>,
) -> Result<()> {
    let terminal = TerminalSession::start(cycles.is_none())?;
    let mut state = ScreenState::new(top.max(1));
    let mut iteration = 0_usize;

    loop {
        let analysis = collect_analysis_frame(
            config,
            data_client,
            binance_client,
            strategy,
            &HashMap::new(),
        )
        .await?;

        render_full_screen_v2(&render_market_watch_screen_v2(
            &analysis.views,
            &analysis.opportunities,
            &analysis.near_misses,
            &state,
            iteration + 1,
            terminal.interactive,
        ))?;

        iteration += 1;
        if cycles.is_some_and(|max_cycles| iteration >= max_cycles) {
            break;
        }

        if terminal.interactive {
            if wait_for_screen_action(refresh_secs, &mut state)? == ScreenAction::Quit {
                break;
            }
        } else {
            sleep(Duration::from_secs(refresh_secs.max(1))).await;
        }
    }

    Ok(())
}

async fn run_dashboard(
    config: &AppConfig,
    data_client: &MarketDataClient,
    binance_client: &BinanceClient,
    strategy: &BundleArbitrageStrategy,
    top: usize,
    refresh_secs: u64,
    cycles: Option<usize>,
) -> Result<()> {
    const DASHBOARD_RECENT_TRADES_LIMIT: usize = 20;

    let journal = JournalStore::new(&config.storage)?;
    let terminal = TerminalSession::start(cycles.is_none())?;
    let mut state = ScreenState::new(top.max(1));
    let mut iteration = 0_usize;

    loop {
        let analysis = collect_analysis_frame(
            config,
            data_client,
            binance_client,
            strategy,
            &HashMap::new(),
        )
        .await?;
        let snapshot = journal.load_snapshot()?;
        let recent_trades = journal.load_paper_trades(Some(DASHBOARD_RECENT_TRADES_LIMIT))?;

        render_full_screen_v2(&render_dashboard_screen_v2(
            &analysis.views,
            &analysis.opportunities,
            &analysis.near_misses,
            &snapshot,
            &recent_trades,
            config.run.paper_starting_balance_usdc,
            &state,
            iteration + 1,
            terminal.interactive,
        ))?;

        iteration += 1;
        if cycles.is_some_and(|max_cycles| iteration >= max_cycles) {
            break;
        }

        if terminal.interactive {
            if wait_for_screen_action(refresh_secs, &mut state)? == ScreenAction::Quit {
                break;
            }
        } else {
            sleep(Duration::from_secs(refresh_secs.max(1))).await;
        }
    }

    Ok(())
}

#[allow(dead_code)]
fn render_market_watch_screen(
    views: &[BtcFiveMinuteMarketView],
    opportunities: &[Opportunity],
    top: usize,
    iteration: usize,
) -> String {
    let mut output = String::new();
    let _ = writeln!(output, "Fast Markets Watch");
    let _ = writeln!(
        output,
        "Time: {} | Iteration: {} | Markets: {} | Signals: {}",
        Local::now().format("%Y-%m-%d %H:%M:%S"),
        iteration,
        views.len(),
        opportunities.len()
    );
    let _ = writeln!(output);
    let _ = write!(output, "{}", render_market_table(views, top));
    if !opportunities.is_empty() {
        let _ = writeln!(output);
        let _ = writeln!(output, "Signals:");
        let _ = write!(
            output,
            "{}",
            render_opportunity_table(opportunities, top.min(6))
        );
    }
    output
}

#[allow(dead_code)]
fn render_dashboard_screen(
    views: &[BtcFiveMinuteMarketView],
    opportunities: &[Opportunity],
    snapshot: &PnlSnapshot,
    top: usize,
    iteration: usize,
) -> String {
    let current_window = views.iter().find(|view| view.phase.is_live());
    let next_window = views.iter().find(|view| view.phase.is_upcoming());

    let mut output = String::new();
    let _ = writeln!(output, "Fast Markets Dashboard");
    let _ = writeln!(
        output,
        "Time: {} | Iteration: {}",
        Local::now().format("%Y-%m-%d %H:%M:%S"),
        iteration
    );
    let _ = writeln!(
        output,
        "Executions: {} | Spent: {} | Expected profit: {} | Open risk: {}",
        snapshot.execution_count,
        snapshot.paper_state.total_spent_usdc.round_dp(4),
        snapshot.paper_state.total_expected_profit.round_dp(4),
        snapshot
            .paper_state
            .market_notional
            .values()
            .copied()
            .sum::<Decimal>()
            .round_dp(4)
    );
    if let Some(current_window) = current_window {
        let _ = writeln!(
            output,
            "Current: {} | timer {} | spot {} | direction {} | fit {}",
            current_window.slug,
            format_timer_v2(current_window),
            current_window.spot_move_bps,
            current_window.dominant_outcome,
            yes_no(current_window.strategy_fit)
        );
    }
    if let Some(next_window) = next_window {
        let _ = writeln!(
            output,
            "Next: {} | starts in {}",
            next_window.slug,
            format_timer_v2(next_window)
        );
    }
    let _ = writeln!(output);
    let _ = writeln!(output, "Signals:");
    if opportunities.is_empty() {
        let _ = writeln!(output, "none");
    } else {
        let _ = write!(
            output,
            "{}",
            render_opportunity_table(opportunities, top.min(6))
        );
    }
    let _ = writeln!(output);
    let _ = writeln!(output, "Markets:");
    let _ = write!(output, "{}", render_market_table(views, top));
    output
}

#[allow(dead_code)]
fn render_full_screen(content: &str) -> Result<()> {
    let mut stdout = io::stdout();
    stdout.write_all(b"\x1b[2J\x1b[H")?;
    let content = sanitize_legacy_mojibake(content);
    stdout.write_all(content.as_bytes())?;
    stdout.flush()?;
    Ok(())
}

#[allow(dead_code)]
fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

fn render_market_watch_screen_v2(
    views: &[BtcFiveMinuteMarketView],
    opportunities: &[Opportunity],
    near_misses: &[NearMiss],
    state: &ScreenState,
    iteration: usize,
    interactive: bool,
) -> String {
    let mut output = String::new();
    let _ = writeln!(output, "Fast Markets Watch");
    let _ = writeln!(
        output,
        "Time: {} | Iteration: {} | Markets: {} | Signals: {} | Near-miss: {}",
        Local::now().format("%Y-%m-%d %H:%M:%S"),
        iteration,
        views.len(),
        opportunities.len(),
        near_misses.len()
    );
    let _ = writeln!(output, "{}", render_controls_line(interactive, state));
    if state.show_help {
        let _ = writeln!(output, "{}", render_help_block());
    }
    let _ = writeln!(output);
    let _ = write!(output, "{}", render_market_table_v2(views, state.top));

    if state.show_signals {
        let _ = writeln!(output);
        let _ = writeln!(output, "Signals:");
        if opportunities.is_empty() {
            let _ = writeln!(output, "none");
        } else {
            let _ = write!(
                output,
                "{}",
                render_opportunity_table_v2(opportunities, state.top.min(6))
            );
        }
    }

    if state.show_near_misses {
        let _ = writeln!(output);
        let _ = writeln!(output, "Near-misses:");
        if near_misses.is_empty() {
            let _ = writeln!(output, "none");
        } else {
            let _ = write!(
                output,
                "{}",
                render_near_miss_table(near_misses, state.top.min(6))
            );
        }
    }

    output
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn render_dashboard_screen_v2(
    views: &[BtcFiveMinuteMarketView],
    opportunities: &[Opportunity],
    near_misses: &[NearMiss],
    snapshot: &PnlSnapshot,
    recent_trades: &[PaperTradeEntry],
    paper_starting_balance_usdc: Option<Decimal>,
    state: &ScreenState,
    iteration: usize,
    interactive: bool,
) -> String {
    let current_window = views.iter().find(|view| view.phase.is_live());
    let next_window = views.iter().find(|view| view.phase.is_upcoming());
    let open_notional = snapshot
        .paper_state
        .market_notional
        .values()
        .copied()
        .sum::<Decimal>()
        .round_dp(4);
    let total_spent = snapshot.paper_state.total_spent_usdc.round_dp(4);
    let total_fees = snapshot.paper_state.total_fees_usdc.round_dp(4);
    let total_slippage = snapshot.paper_state.total_slippage_cost_usdc.round_dp(4);
    let total_realized_profit = snapshot.paper_state.total_realized_profit.round_dp(4);
    let total_expected_profit = snapshot.paper_state.total_expected_profit.round_dp(4);
    let open_positions = snapshot.paper_state.open_positions.len();
    let closed_positions = snapshot.paper_state.closed_position_count;

    let mut output = String::new();
    let _ = writeln!(output, "Fast Markets Dashboard");
    let _ = writeln!(
        output,
        "Time: {} | Iteration: {} | Signals: {} | Near-miss: {}",
        Local::now().format("%Y-%m-%d %H:%M:%S"),
        iteration,
        opportunities.len(),
        near_misses.len()
    );
    let _ = writeln!(output, "{}", render_controls_line(interactive, state));
    if state.show_help {
        let _ = writeln!(output, "{}", render_help_block());
    }
    let _ = writeln!(
        output,
        "Executions: {} | Total spent: {} | Expected profit: {} | Open risk: {}",
        snapshot.execution_count,
        snapshot.paper_state.total_spent_usdc.round_dp(4),
        snapshot.paper_state.total_expected_profit.round_dp(4),
        snapshot
            .paper_state
            .market_notional
            .values()
            .copied()
            .sum::<Decimal>()
            .round_dp(4)
    );
    if let Some(current_window) = current_window {
        let _ = writeln!(
            output,
            "Current: {} | timer {} | px {} | spot {} | 5s {} | up/down {}/{} | direction {} | fit {}",
            current_window.slug,
            format_timer(current_window),
            current_window.current_price,
            current_window.spot_move_bps,
            current_window.spot_move_5s_bps,
            current_window.up_ask,
            current_window.down_ask,
            current_window.dominant_outcome,
            yes_no_ru(current_window.strategy_fit)
        );
        let _ = writeln!(
            output,
            "Micro structure | 5s {} | 15s {} | accel {}",
            current_window.spot_move_5s_bps,
            current_window.spot_move_15s_bps,
            current_window.micro_acceleration_bps
        );
    }
    let _ = writeln!(
        output,
        "Paper balance | Realized PnL: {} | Open notional: {} | Open expected PnL: {}",
        format_signed_decimal(total_realized_profit),
        open_notional,
        format_signed_decimal(total_expected_profit),
    );
    let _ = writeln!(
        output,
        "Paper stats | Executions: {} | Open positions: {} | Closed positions: {} | Total spent: {} | Fees: {} | Slippage: {}",
        snapshot.execution_count,
        open_positions,
        closed_positions,
        total_spent,
        total_fees,
        total_slippage,
    );
    if let Some(starting_balance) = paper_starting_balance_usdc {
        let paper_cash = (starting_balance - open_notional + total_realized_profit).round_dp(4);
        let paper_equity = (starting_balance + total_realized_profit).round_dp(4);
        let _ = writeln!(
            output,
            "Paper funds | Start: {} | Cash now: {} | Equity now: {}",
            starting_balance.round_dp(4),
            paper_cash,
            paper_equity
        );
    }
    if let Some(next_window) = next_window {
        let _ = writeln!(
            output,
            "Next: {} | starts in {}",
            next_window.slug,
            format_timer(next_window)
        );
    }

    if state.show_positions {
        let mut positions = snapshot
            .paper_state
            .open_positions
            .values()
            .cloned()
            .collect::<Vec<_>>();
        positions.sort_by(|left, right| right.opened_at.cmp(&left.opened_at));
        let _ = writeln!(output);
        let _ = writeln!(output, "Open positions:");
        if positions.is_empty() {
            let _ = writeln!(output, "none");
        } else {
            let positions_to_show = positions.len().min(state.top.min(8));
            let _ = write!(
                output,
                "{}",
                render_paper_position_table(&positions[..positions_to_show])
            );
        }
    }

    if state.show_trades {
        let mut trades = recent_trades.to_vec();
        trades.sort_by(|left, right| right.recorded_at.cmp(&left.recorded_at));
        let _ = writeln!(output);
        let _ = writeln!(output, "Recent trades:");
        if trades.is_empty() {
            let _ = writeln!(output, "none");
        } else {
            let trades_to_show = trades.len().min(state.top.min(10));
            let _ = write!(
                output,
                "{}",
                render_paper_trade_table(&trades[..trades_to_show])
            );
        }
    }

    if state.show_signals {
        let _ = writeln!(output);
        let _ = writeln!(output, "Signals:");
        if opportunities.is_empty() {
            let _ = writeln!(output, "none");
        } else {
            let _ = write!(
                output,
                "{}",
                render_opportunity_table_v2(opportunities, state.top.min(6))
            );
        }
    }

    if state.show_near_misses {
        let _ = writeln!(output);
        let _ = writeln!(output, "Near-misses:");
        if near_misses.is_empty() {
            let _ = writeln!(output, "none");
        } else {
            let _ = write!(
                output,
                "{}",
                render_near_miss_table(near_misses, state.top.min(6))
            );
        }
    }

    let _ = writeln!(output);
    let _ = writeln!(output, "Markets:");
    let _ = write!(output, "{}", render_market_table_v2(views, state.top));
    output
}

fn render_opportunity_table_v2(opportunities: &[Opportunity], top: usize) -> String {
    let mut output = String::new();
    let _ = writeln!(
        output,
        "{:<3} {:<11} {:>6} {:>8} {:>7} {:>8} {:>9} {:>10} {:>6} {:<8} Slug",
        "#", "Kind", "Edge", "Spot", "Ask", "Shares", "USDC", "Profit", "Secs", "Side"
    );
    let _ = writeln!(output, "{}", "-".repeat(108));

    for (index, opportunity) in opportunities.iter().take(top).enumerate() {
        let _ = writeln!(
            output,
            "{:<3} {:<11} {:>6} {:>8} {:>7} {:>8} {:>9} {:>10} {:>6} {:<8} {}",
            index + 1,
            opportunity.kind.as_str(),
            opportunity.edge_bps,
            opportunity.spot_move_bps.round_dp(2),
            opportunity.primary_outcome_ask_price.round_dp(4),
            opportunity.tradable_shares,
            opportunity.required_usdc,
            opportunity.expected_profit,
            opportunity.seconds_left,
            truncate_text_v2(&opportunity.primary_outcome_label, 8),
            opportunity.slug
        );
    }

    output
}

fn render_near_miss_table(near_misses: &[NearMiss], top: usize) -> String {
    let mut output = String::new();
    let _ = writeln!(
        output,
        "{:<3} {:<11} {:>8} {:>8} {:>7} {:>6} {:<34} Slug",
        "#", "Kind", "Gap", "Spot", "Ask", "Secs", "Reason"
    );
    let _ = writeln!(output, "{}", "-".repeat(120));

    for (index, near_miss) in near_misses.iter().take(top).enumerate() {
        let ask = near_miss
            .primary_outcome_ask_price
            .map_or_else(|| "-".to_owned(), |value| value.round_dp(4).to_string());
        let _ = writeln!(
            output,
            "{:<3} {:<11} {:>8} {:>8} {:>7} {:>6} {:<34} {}",
            index + 1,
            near_miss.kind.as_str(),
            near_miss.shortfall_label,
            near_miss.spot_move_bps.round_dp(2),
            ask,
            near_miss.seconds_left,
            truncate_text_v2(&near_miss.reason, 34),
            near_miss.slug
        );
    }

    output
}

fn render_paper_cycle_table(cycles: &[PaperCycleEntry]) -> String {
    let mut output = String::new();
    let _ = writeln!(
        output,
        "{:<3} {:<19} {:<16} {:<10} {:<5} {:>8} {:>8} {:>8} {:>6} {:>7} {:>7} {:>7} {:<32}",
        "#",
        "Time",
        "LiveSlug",
        "Regime",
        "Risk",
        "Spot",
        "5s",
        "Price",
        "Fit",
        "Opps",
        "Miss",
        "Exec",
        "Decision"
    );
    let _ = writeln!(output, "{}", "-".repeat(154));

    for (index, cycle) in cycles.iter().enumerate() {
        let _ = writeln!(
            output,
            "{:<3} {:<19} {:<16} {:<10} {:<5} {:>8} {:>8} {:>8} {:>6} {:>7} {:>7} {:>7} {:<32}",
            index + 1,
            cycle
                .recorded_at
                .with_timezone(&Local)
                .format("%Y-%m-%d %H:%M:%S"),
            truncate_text_v2(cycle.current_market_slug.as_deref().unwrap_or("-"), 16),
            truncate_text_v2(cycle.regime.as_deref().unwrap_or("-"), 10),
            if cycle.risk_blocked { "Y" } else { "-" },
            cycle.current_market_spot_move_bps.as_deref().unwrap_or("-"),
            cycle
                .current_market_spot_move_5s_bps
                .as_deref()
                .unwrap_or("-"),
            cycle.current_market_price.as_deref().unwrap_or("-"),
            cycle.current_market_fit.map_or("-", yes_no_ru),
            cycle.opportunity_count,
            cycle.near_miss_count,
            cycle.executed_count,
            truncate_text_v2(cycle.decision_reason.as_deref().unwrap_or("-"), 32),
        );
    }

    output
}

fn render_paper_trade_table(trades: &[PaperTradeEntry]) -> String {
    let mut output = String::new();
    let _ = writeln!(
        output,
        "{:<3} {:<19} {:<6} {:<11} {:>9} {:>9} {:>9} {:>7} {:<10} {:<34} Slug",
        "#", "Time", "Act", "Kind", "Spend", "Payout", "PnL", "Hold", "Outcome", "Reason"
    );
    let _ = writeln!(output, "{}", "-".repeat(148));

    for (index, trade) in trades.iter().enumerate() {
        let pnl_label = match trade.action {
            PaperTradeAction::Open => trade
                .expected_profit_usdc
                .map_or_else(|| "-".to_owned(), |value| format!("~{}", value.round_dp(4))),
            PaperTradeAction::Close => trade.realized_profit_usdc.map_or_else(
                || "-".to_owned(),
                |value| format_signed_decimal(value.round_dp(4)),
            ),
        };
        let outcome_label = trade
            .actual_outcome
            .as_deref()
            .or(trade.dominant_outcome.as_deref())
            .unwrap_or("-");

        let _ = writeln!(
            output,
            "{:<3} {:<19} {:<6} {:<11} {:>9} {:>9} {:>9} {:>7} {:<10} {:<34} {}",
            index + 1,
            trade
                .recorded_at
                .with_timezone(&Local)
                .format("%Y-%m-%d %H:%M:%S"),
            trade.action.as_str(),
            trade.kind.as_str(),
            trade.spent_usdc.round_dp(4),
            trade
                .realized_payout_usdc
                .map_or_else(|| "-".to_owned(), |value| value.round_dp(4).to_string()),
            pnl_label,
            trade
                .holding_seconds
                .map_or_else(|| "-".to_owned(), |value| value.to_string()),
            truncate_text_v2(outcome_label, 10),
            truncate_text_v2(&trade.note, 34),
            trade.slug
        );
    }

    output
}

fn render_paper_position_table(positions: &[PaperPosition]) -> String {
    let mut output = String::new();
    let _ = writeln!(
        output,
        "{:<3} {:<19} {:<19} {:<11} {:>9} {:>9} {:<10} {:<24} Slug",
        "#", "OpenedAt", "CloseAt", "Kind", "Spend", "ExpPnL", "Outcome", "Legs"
    );
    let _ = writeln!(output, "{}", "-".repeat(140));

    for (index, position) in positions.iter().enumerate() {
        let _ = writeln!(
            output,
            "{:<3} {:<19} {:<19} {:<11} {:>9} {:>9} {:<10} {:<24} {}",
            index + 1,
            position
                .opened_at
                .with_timezone(&Local)
                .format("%Y-%m-%d %H:%M:%S"),
            position.scheduled_close_at.map_or_else(
                || "-".to_owned(),
                |value| value
                    .with_timezone(&Local)
                    .format("%Y-%m-%d %H:%M:%S")
                    .to_string()
            ),
            position.kind.as_str(),
            position.spent_usdc.round_dp(4),
            position.expected_profit_usdc.round_dp(4),
            truncate_text_v2(&position.dominant_outcome_at_entry, 10),
            truncate_text_v2(&render_position_legs_summary(position), 24),
            position.slug
        );
    }

    output
}

fn render_position_legs_summary(position: &PaperPosition) -> String {
    position
        .legs
        .iter()
        .map(|leg| format!("{}@{}", leg.label, leg.entry_price.round_dp(4)))
        .collect::<Vec<_>>()
        .join(" + ")
}

fn render_controls_line(interactive: bool, state: &ScreenState) -> String {
    if interactive {
        format!(
            "Hotkeys: q quit | r refresh | j/k top ({}) | s signals {} | n near-miss {} | p positions {} | t trades {} | h help {}",
            state.top,
            yes_no_ru(state.show_signals),
            yes_no_ru(state.show_near_misses),
            yes_no_ru(state.show_positions),
            yes_no_ru(state.show_trades),
            yes_no_ru(state.show_help),
        )
    } else {
        format!(
            "Non-interactive | top {} | signals {} | near-miss {} | positions {} | trades {}",
            state.top,
            yes_no_ru(state.show_signals),
            yes_no_ru(state.show_near_misses),
            yes_no_ru(state.show_positions),
            yes_no_ru(state.show_trades),
        )
    }
}

fn render_help_block() -> &'static str {
    "Controls: q/Esc quit, r refresh, j/k change row limit, s signals, n near-misses, p positions, t trades, h help."
}

fn render_full_screen_v2(content: &str) -> Result<()> {
    let mut stdout = io::stdout();
    execute!(stdout, Clear(ClearType::All), MoveTo(0, 0))?;
    let content = sanitize_legacy_mojibake(content);
    stdout.write_all(content.as_bytes())?;
    stdout.flush()?;
    Ok(())
}

fn wait_for_screen_action(refresh_secs: u64, state: &mut ScreenState) -> Result<ScreenAction> {
    tokio::task::block_in_place(|| {
        if !event::poll(StdDuration::from_secs(refresh_secs.max(1)))? {
            return Ok(ScreenAction::Continue);
        }

        loop {
            match event::read()? {
                Event::Resize(_, _) => return Ok(ScreenAction::Continue),
                Event::Key(key)
                    if matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) =>
                {
                    return Ok(apply_key_binding(state, key.code));
                }
                _ => {}
            }
        }
    })
}

fn apply_key_binding(state: &mut ScreenState, code: KeyCode) -> ScreenAction {
    match code {
        KeyCode::Esc | KeyCode::Char('q') => ScreenAction::Quit,
        KeyCode::Char('j' | '+' | '=') => {
            state.increase_top();
            ScreenAction::Continue
        }
        KeyCode::Char('k' | '-') => {
            state.decrease_top();
            ScreenAction::Continue
        }
        KeyCode::Char('s') => {
            state.show_signals = !state.show_signals;
            ScreenAction::Continue
        }
        KeyCode::Char('n') => {
            state.show_near_misses = !state.show_near_misses;
            ScreenAction::Continue
        }
        KeyCode::Char('p') => {
            state.show_positions = !state.show_positions;
            ScreenAction::Continue
        }
        KeyCode::Char('t') => {
            state.show_trades = !state.show_trades;
            ScreenAction::Continue
        }
        KeyCode::Char('h' | '?') => {
            state.show_help = !state.show_help;
            ScreenAction::Continue
        }
        _ => ScreenAction::Continue,
    }
}

fn yes_no_ru(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

fn format_signed_decimal(value: Decimal) -> String {
    let rounded = value.round_dp(4);
    if rounded > Decimal::ZERO {
        format!("+{rounded}")
    } else {
        rounded.to_string()
    }
}

fn render_market_table_v2(views: &[BtcFiveMinuteMarketView], top: usize) -> String {
    let displayed = views.iter().take(top).collect::<Vec<_>>();
    let current_slug = displayed
        .iter()
        .find(|view| view.phase.is_live())
        .map(|view| view.slug.as_str());
    let next_slug = displayed
        .iter()
        .find(|view| view.phase.is_upcoming())
        .map(|view| view.slug.as_str());

    let mut output = String::new();
    let _ = writeln!(
        output,
        "{:<3} {:<8} {:<12} {:<11} {:<10} {:>10} {:>8} {:>8} {:>6} {:>6} {:<8} {:<4} Slug",
        "#", "Tag", "Phase", "UTC", "Timer", "Price", "Spot", "5s", "Up", "Down", "Dir", "Fit"
    );
    let _ = writeln!(output, "{}", "-".repeat(122));

    for (index, view) in displayed.iter().enumerate() {
        let _ = writeln!(
            output,
            "{:<3} {:<8} {:<12} {:<11} {:<10} {:>10} {:>8} {:>8} {:>6} {:>6} {:<8} {:<4} {}",
            index + 1,
            market_tag_v2(view, current_slug, next_slug),
            market_phase_label_v2(view.phase),
            format_window_v2(view),
            format_timer_v2(view),
            view.current_price,
            view.spot_move_bps,
            view.spot_move_5s_bps,
            view.up_ask,
            view.down_ask,
            truncate_text_v2(&view.dominant_outcome, 8),
            yes_no_ru(view.strategy_fit),
            view.slug
        );
    }

    output
}

fn market_tag_v2<'a>(
    view: &'a BtcFiveMinuteMarketView,
    current_slug: Option<&'a str>,
    next_slug: Option<&'a str>,
) -> &'a str {
    if view.strategy_fit {
        "FIT"
    } else if current_slug == Some(view.slug.as_str()) {
        "LIVE"
    } else if next_slug == Some(view.slug.as_str()) {
        "NEXT"
    } else {
        view.target_label.as_str()
    }
}

fn market_phase_label_v2(phase: MarketPhase) -> &'static str {
    match phase {
        MarketPhase::Live => "live",
        MarketPhase::Upcoming => "upcoming",
        MarketPhase::Settled => "settled",
        MarketPhase::MissingContext => "missing_context",
        MarketPhase::UnknownWindow => "unknown_window",
    }
}

fn format_window_v2(view: &BtcFiveMinuteMarketView) -> String {
    match (view.window_start, view.window_end) {
        (Some(start), Some(end)) => format!("{}-{}", start.format("%H:%M"), end.format("%H:%M")),
        _ => "n/a".to_owned(),
    }
}

fn format_timer_v2(view: &BtcFiveMinuteMarketView) -> String {
    if view.phase.is_live() {
        format!("{}s", view.seconds_left)
    } else if view.phase.is_upcoming() {
        format!("{}s", view.seconds_to_start)
    } else {
        "-".to_owned()
    }
}

fn truncate_text_v2(value: &str, max_chars: usize) -> String {
    let value = sanitize_legacy_mojibake(value);
    let char_count = value.chars().count();
    if char_count <= max_chars {
        return value;
    }

    if max_chars <= 3 {
        return value.chars().take(max_chars).collect::<String>();
    }

    let mut truncated = value
        .chars()
        .take(max_chars.saturating_sub(3))
        .collect::<String>();
    if max_chars > 3 {
        truncated.push_str("...");
    }
    truncated
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use std::collections::HashMap;
    use std::time::{Duration as StdDuration, Instant};

    use chrono::Utc;

    use super::*;
    use crate::models::{BookLevel, MarketTarget, PaperPositionLeg, TargetPriceSource};
    use crate::services::binance::WindowDirection;

    fn decimal(value: &str) -> Decimal {
        value.parse().expect("decimal literal should parse")
    }

    #[test]
    fn paper_cycle_append_respects_sampling_when_position_is_open() {
        let last_append_at = Utc::now();
        let before_sample = last_append_at + chrono::Duration::milliseconds(500);
        let at_sample = last_append_at + chrono::Duration::seconds(1);

        assert!(!should_append_paper_cycle_journal_fields(
            before_sample,
            0,
            0,
            false,
            Some(last_append_at),
            1,
        ));
        assert!(should_append_paper_cycle_journal_fields(
            at_sample,
            0,
            0,
            false,
            Some(last_append_at),
            1,
        ));
        assert!(should_append_paper_cycle_journal_fields(
            before_sample,
            1,
            0,
            false,
            Some(last_append_at),
            1,
        ));
        assert!(should_append_paper_cycle_journal_fields(
            before_sample,
            0,
            1,
            false,
            Some(last_append_at),
            1,
        ));
    }

    #[test]
    fn controlled_run_wait_timeout_enforces_runtime_and_drain_deadlines() {
        let now = Instant::now();

        assert_eq!(
            controlled_run_trigger_wait_timeout(
                true,
                Some(StdDuration::from_secs(10)),
                now,
                None,
                StdDuration::from_secs(5),
            ),
            None
        );
        assert_eq!(
            controlled_run_trigger_wait_timeout(
                false,
                Some(StdDuration::from_secs(10)),
                now - StdDuration::from_secs(11),
                None,
                StdDuration::from_secs(5),
            ),
            Some(StdDuration::ZERO)
        );

        let runtime_remaining = controlled_run_trigger_wait_timeout(
            false,
            Some(StdDuration::from_secs(10)),
            now - StdDuration::from_secs(3),
            None,
            StdDuration::from_secs(5),
        )
        .expect("runtime should provide a bounded wait");
        assert!(runtime_remaining <= StdDuration::from_secs(7));
        assert!(runtime_remaining > StdDuration::from_secs(6));

        let drain_remaining = controlled_run_trigger_wait_timeout(
            false,
            Some(StdDuration::from_secs(10)),
            now - StdDuration::from_secs(20),
            Some(now - StdDuration::from_secs(2)),
            StdDuration::from_secs(5),
        )
        .expect("drain should provide a bounded wait");
        assert!(drain_remaining <= StdDuration::from_secs(3));
        assert!(drain_remaining > StdDuration::from_secs(2));
    }

    #[test]
    fn near_miss_asset_label_detects_supported_assets() {
        assert_eq!(market_slug_asset_label("btc-updown-5m-1"), "BTC");
        assert_eq!(market_slug_asset_label("eth-updown-5m-1"), "ETH");
        assert_eq!(market_slug_asset_label("sol-updown-5m-1"), "SOL");
        assert_eq!(market_slug_asset_label("xrp-updown-5m-1"), "XRP");
        assert_eq!(market_slug_asset_label("bnb-updown-5m-1"), "BNB");
        assert_eq!(market_slug_asset_label("doge-updown-5m-1"), "UNKNOWN");
    }

    #[test]
    fn near_miss_ask_bucket_label_groups_entry_prices() {
        assert_eq!(near_miss_ask_bucket_label(Some(decimal("0.44"))), "<0.45");
        assert_eq!(
            near_miss_ask_bucket_label(Some(decimal("0.49"))),
            "0.45-0.50"
        );
        assert_eq!(
            near_miss_ask_bucket_label(Some(decimal("0.55"))),
            "0.50-0.56"
        );
        assert_eq!(
            near_miss_ask_bucket_label(Some(decimal("0.59"))),
            "0.56-0.60"
        );
        assert_eq!(near_miss_ask_bucket_label(Some(decimal("0.60"))), ">=0.60");
        assert_eq!(near_miss_ask_bucket_label(None), "unknown");
    }

    fn test_v4_overlay() -> V4InventoryConfig {
        V4InventoryConfig {
            enabled: true,
            max_gross_inventory_shares_per_window: decimal("10"),
            max_directional_delta_shares_per_window: decimal("10"),
            max_window_spent_usdc: Decimal::ZERO,
            max_entries_per_window: 0,
            cooldown_secs: 180,
            cooldown_on_stop_loss: true,
            cooldown_on_reversal: true,
            cooldown_on_partial_reversal: true,
        }
    }

    fn test_opportunity(slug: &str, shares: &str, label: &str) -> Opportunity {
        Opportunity {
            kind: OpportunityKind::BonereaperStateV2,
            condition_id: format!("condition-{slug}"),
            slug: slug.to_owned(),
            question: "Test window".to_owned(),
            outcome_a_label: "Up".to_owned(),
            outcome_a_token_id: "up-token".to_owned(),
            outcome_b_label: "Down".to_owned(),
            outcome_b_token_id: "down-token".to_owned(),
            liquidity_usdc: decimal("1000"),
            outcome_a_ask_price: decimal("0.52"),
            outcome_b_ask_price: decimal("0.48"),
            bundle_cost: decimal("1.00"),
            net_bundle_cost: decimal("1.00"),
            edge_per_share: decimal("0.01"),
            edge_bps: 10,
            tradable_shares: decimal(shares),
            required_usdc: decimal("5"),
            expected_payout: decimal("6"),
            expected_profit: decimal("1"),
            interval_open_price: decimal("65000"),
            target_price: decimal("65050"),
            target_price_source: TargetPriceSource::BinanceWindowOpenFallback,
            target_gap_bps: decimal("5"),
            current_spot_price: decimal("65060"),
            spot_move_bps: decimal("8"),
            spot_move_1s_bps: decimal("0.5"),
            spot_move_5s_bps: decimal("1.0"),
            spot_move_15s_bps: decimal("1.5"),
            micro_acceleration_bps: decimal("0.1"),
            micro_burst_reference_price: decimal("65058"),
            micro_reference_price: decimal("65054"),
            signal_strength_bps: decimal("7"),
            aligned_trade_flow_bps: decimal("1.2"),
            signal_tier: "normal".to_owned(),
            target_cross_label: String::new(),
            dominant_outcome: label.to_owned(),
            primary_outcome_label: label.to_owned(),
            primary_outcome_token_id: format!("{slug}-{label}-token"),
            primary_outcome_ask_price: decimal("0.52"),
            primary_fill_levels: Vec::new(),
            hedge_outcome_label: None,
            hedge_outcome_token_id: None,
            hedge_outcome_ask_price: None,
            hedge_fill_levels: Vec::new(),
            hedge_shares: Decimal::ZERO,
            seconds_left: 120,
            note: "test".to_owned(),
        }
    }

    #[test]
    fn repeat_entry_throttle_blocks_same_market_side_until_interval() {
        let raw = include_str!("../../config.codex-scalp-v1-raw-light-v3.toml");
        let config: AppConfig = toml::from_str(raw).expect("fixture config should parse");
        let opportunity = test_opportunity("btc-updown-5m-throttle", "10", "Down");
        let mut throttle = HashMap::<String, Instant>::new();
        let first_entry_at = Instant::now();

        assert!(
            repeat_entry_throttle_block_reason(&config, &throttle, &opportunity, first_entry_at)
                .is_none()
        );
        record_repeat_entry_throttle(&mut throttle, &opportunity, first_entry_at);

        let blocked = repeat_entry_throttle_block_reason(
            &config,
            &throttle,
            &opportunity,
            first_entry_at + StdDuration::from_millis(250),
        )
        .expect("same side should be throttled inside the configured interval");
        assert!(blocked.contains("repeat entry throttled"));

        assert!(
            repeat_entry_throttle_block_reason(
                &config,
                &throttle,
                &opportunity,
                first_entry_at + StdDuration::from_millis(500),
            )
            .is_none()
        );
    }

    #[test]
    fn runtime_market_targets_follow_exchange_trigger_symbol() {
        let configured = vec![
            MarketTarget::Btc5m,
            MarketTarget::Eth5m,
            MarketTarget::Sol5m,
            MarketTarget::Xrp5m,
        ];
        let trigger = RuntimeTriggerEvent {
            symbol: "ETHUSDT".to_owned(),
            event_time_ms: 1_000,
            received_time_ms: 1_001,
            price: decimal("3500"),
            source: "Binance::Trade".to_owned(),
        };

        assert_eq!(
            runtime_market_targets_for_trigger(&configured, Some(&trigger)),
            vec![MarketTarget::Eth5m]
        );
    }

    #[test]
    fn runtime_market_targets_keep_all_for_polymarket_trigger() {
        let configured = vec![MarketTarget::Btc5m, MarketTarget::Eth5m];
        let trigger = RuntimeTriggerEvent {
            symbol: String::new(),
            event_time_ms: 1_000,
            received_time_ms: 1_001,
            price: Decimal::ZERO,
            source: "Polymarket::Book".to_owned(),
        };

        assert_eq!(
            runtime_market_targets_for_trigger(&configured, Some(&trigger)),
            configured
        );
    }

    #[test]
    fn repeat_entry_blocks_scale_in_when_scale_in_is_disabled() {
        let raw = include_str!("../../config.codex-scalp-v1-raw-light-v3.toml");
        let config: AppConfig = toml::from_str(raw).expect("fixture config should parse");
        assert!(config.run.allow_repeat_entries_same_window);
        assert!(!config.run.scale_in.enabled);

        let opportunity = test_opportunity("btc-updown-5m-scale-disabled", "10", "Down");
        let mut paper_state = PaperState::default();
        paper_state.open_positions.insert(
            opportunity.slug.clone(),
            test_position(&opportunity.slug, "0", "10"),
        );

        let reason =
            repeated_entry_block_reason(&config, &paper_state, &HashSet::new(), &opportunity)
                .expect("open position should block implicit scale-in");
        assert!(reason.contains("scale-in is disabled"));
    }

    #[test]
    fn repeat_entry_throttle_does_not_block_opposite_side() {
        let raw = include_str!("../../config.codex-scalp-v1-raw-light-v3.toml");
        let config: AppConfig = toml::from_str(raw).expect("fixture config should parse");
        let down = test_opportunity("btc-updown-5m-throttle-side", "10", "Down");
        let mut up = test_opportunity("btc-updown-5m-throttle-side", "10", "Up");
        up.primary_outcome_token_id = "up-token-different".to_owned();
        let mut throttle = HashMap::<String, Instant>::new();
        let first_entry_at = Instant::now();

        record_repeat_entry_throttle(&mut throttle, &down, first_entry_at);

        assert!(
            repeat_entry_throttle_block_reason(
                &config,
                &throttle,
                &up,
                first_entry_at + StdDuration::from_millis(100),
            )
            .is_none()
        );
    }

    #[test]
    fn paper_cash_guard_blocks_entry_when_fee_would_overdraw_bankroll() {
        let mut paper_state = PaperState::default();
        paper_state
            .market_notional
            .insert("condition-open".to_owned(), decimal("45"));
        let opportunity = test_opportunity("btc-updown-5m-cash", "10", "Up");

        let reason = paper_cash_block_reason_with_costs(
            Some(decimal("50")),
            PaperCostModel::new(100, 0),
            &paper_state,
            &opportunity,
        )
        .expect("fee-inclusive spend should exceed free cash");

        assert!(reason.contains("insufficient paper cash"));
    }

    #[test]
    fn projected_open_notional_guard_blocks_entry_before_cap_is_exceeded() {
        let mut risk = RiskControlConfig::default();
        risk.max_open_notional_usdc = decimal("100");
        let mut paper_state = PaperState::default();
        paper_state
            .market_notional
            .insert("condition-open".to_owned(), decimal("95"));
        let mut opportunity = test_opportunity("btc-updown-5m-projected-cap", "10", "Up");
        opportunity.required_usdc = decimal("8");

        let reason = projected_open_notional_block_reason_with_costs(
            &risk,
            PaperCostModel::new(0, 0),
            &paper_state,
            &opportunity,
        )
        .expect("projected open notional should hit the risk cap");

        assert!(reason.contains("projected open notional limit reached"));
    }

    #[test]
    fn projected_open_notional_guard_counts_fee_inclusive_entry_spend() {
        let mut risk = RiskControlConfig::default();
        risk.max_open_notional_usdc = decimal("100");
        let mut paper_state = PaperState::default();
        paper_state
            .market_notional
            .insert("condition-open".to_owned(), decimal("94.99"));
        let opportunity = test_opportunity("btc-updown-5m-projected-fee", "10", "Up");

        let reason = projected_open_notional_block_reason_with_costs(
            &risk,
            PaperCostModel::new(100, 0),
            &paper_state,
            &opportunity,
        )
        .expect("fee-inclusive projected notional should hit the risk cap");

        assert!(reason.contains("entry incl. costs"));
    }

    #[test]
    fn risk_tracker_blocks_on_open_notional_limit() {
        let mut risk = RiskControlConfig::default();
        risk.max_open_notional_usdc = decimal("50");
        let mut tracker = RiskTracker::new(Decimal::ZERO, Decimal::ZERO, 0);

        let reason = tracker
            .evaluate_and_arm_limits(
                &risk,
                RuntimeRiskContext {
                    total_realized_profit: Decimal::ZERO,
                    open_notional: decimal("50"),
                    unrealized_profit: Decimal::ZERO,
                    paper_cash: Some(decimal("10")),
                },
            )
            .expect("open notional cap should arm the kill switch");

        assert!(reason.contains("open notional limit reached"));
        assert!(tracker.is_blocked());
    }

    #[test]
    fn risk_limits_are_active_when_only_open_notional_cap_is_enabled() {
        let mut risk = RiskControlConfig::default();
        risk.max_open_notional_usdc = decimal("50");

        assert!(should_apply_risk_limits(BotMode::Paper, &risk));
    }

    #[test]
    fn risk_tracker_blocks_on_unrealized_loss() {
        let mut risk = RiskControlConfig::default();
        risk.max_unrealized_loss_usdc = decimal("3");
        let mut tracker = RiskTracker::new(Decimal::ZERO, Decimal::ZERO, 0);

        let reason = tracker
            .evaluate_and_arm_limits(
                &risk,
                RuntimeRiskContext {
                    total_realized_profit: Decimal::ZERO,
                    open_notional: decimal("10"),
                    unrealized_profit: decimal("-3.01"),
                    paper_cash: Some(decimal("40")),
                },
            )
            .expect("unrealized loss cap should arm the kill switch");

        assert!(reason.contains("unrealized loss limit reached"));
        assert!(tracker.is_blocked());
    }

    fn test_pnl_ratchet() -> PnlRatchetConfig {
        PnlRatchetConfig {
            enabled: true,
            apply_to_codex_sentinel_only: true,
            base_notional_usdc: decimal("6"),
            protect_notional_usdc: decimal("4"),
            profit_unlock_usdc: decimal("2"),
            protect_after_consecutive_losses: 1,
        }
    }

    fn test_large_codex_opportunity() -> Opportunity {
        let mut opportunity = test_opportunity("btc-updown-5m-ratchet", "20", "Up");
        opportunity.kind = OpportunityKind::CodexSentinelV1;
        opportunity.required_usdc = decimal("10");
        opportunity.expected_payout = decimal("12");
        opportunity.expected_profit = decimal("2");
        opportunity.primary_outcome_ask_price = decimal("0.50");
        opportunity.signal_tier = "attack".to_owned();
        opportunity
    }

    #[test]
    fn pnl_ratchet_caps_codex_attack_size_before_profit_unlock() {
        let ratchet = test_pnl_ratchet();
        let tracker = RiskTracker::new(Decimal::ZERO, Decimal::ZERO, 0);
        let opportunity = test_large_codex_opportunity();

        let scaled = apply_pnl_ratchet_to_opportunity(
            &ratchet,
            &tracker,
            Decimal::ZERO,
            &opportunity,
            decimal("1"),
        )
        .expect("base cap should keep enough shares");

        assert_eq!(scaled.required_usdc, decimal("6.000000"));
        assert_eq!(scaled.tradable_shares, decimal("12.000000"));
        assert!(scaled.note.contains("profit-lock base cap"));
    }

    #[test]
    fn pnl_ratchet_protects_after_consecutive_loss() {
        let ratchet = test_pnl_ratchet();
        let tracker = RiskTracker::new(Decimal::ZERO, Decimal::ZERO, 1);
        let opportunity = test_large_codex_opportunity();

        let scaled = apply_pnl_ratchet_to_opportunity(
            &ratchet,
            &tracker,
            decimal("-0.12"),
            &opportunity,
            decimal("1"),
        )
        .expect("protect cap should keep enough shares");

        assert_eq!(scaled.required_usdc, decimal("4.000000"));
        assert_eq!(scaled.tradable_shares, decimal("8.000000"));
        assert!(scaled.note.contains("after-loss protection"));
    }

    #[test]
    fn pnl_ratchet_allows_attack_size_after_profit_unlock() {
        let ratchet = test_pnl_ratchet();
        let tracker = RiskTracker::new(Decimal::ZERO, Decimal::ZERO, 0);
        let opportunity = test_large_codex_opportunity();

        let scaled = apply_pnl_ratchet_to_opportunity(
            &ratchet,
            &tracker,
            decimal("2.10"),
            &opportunity,
            decimal("1"),
        )
        .expect("unlocked attack should pass through");

        assert_eq!(scaled.required_usdc, opportunity.required_usdc);
        assert_eq!(scaled.tradable_shares, opportunity.tradable_shares);
    }

    fn test_position(slug: &str, up_shares: &str, down_shares: &str) -> PaperPosition {
        let mut legs = Vec::new();
        if decimal(up_shares) > Decimal::ZERO {
            legs.push(PaperPositionLeg {
                label: "Up".to_owned(),
                side: PaperOutcomeSide::Up,
                token_id: format!("{slug}-up"),
                shares: decimal(up_shares),
                entry_price: decimal("0.50"),
            });
        }
        if decimal(down_shares) > Decimal::ZERO {
            legs.push(PaperPositionLeg {
                label: "Down".to_owned(),
                side: PaperOutcomeSide::Down,
                token_id: format!("{slug}-down"),
                shares: decimal(down_shares),
                entry_price: decimal("0.50"),
            });
        }
        PaperPosition {
            opened_at: Utc::now(),
            scheduled_close_at: None,
            condition_id: format!("condition-{slug}"),
            slug: slug.to_owned(),
            question: "Test window".to_owned(),
            kind: OpportunityKind::BonereaperStateV2,
            dominant_outcome_at_entry: "Up".to_owned(),
            spot_move_bps_at_entry: decimal("5"),
            spent_usdc: decimal("5"),
            expected_profit_usdc: decimal("1"),
            entry_count: 1,
            partial_reversal_exits: 0,
            best_entry_reference_price: decimal("0.50"),
            legs,
        }
    }

    fn test_book(token_id: &str, bid_price: &str, bid_size: &str) -> OrderBook {
        OrderBook {
            asset_id: token_id.to_owned(),
            bids: vec![BookLevel {
                price: decimal(bid_price),
                size: decimal(bid_size),
            }],
            asks: Vec::new(),
            min_order_size: None,
            tick_size: None,
        }
    }

    fn test_peak_book(token_id: &str, bid_price: &str, ask_price: &str) -> OrderBook {
        OrderBook {
            asset_id: token_id.to_owned(),
            bids: vec![BookLevel {
                price: decimal(bid_price),
                size: decimal("10"),
            }],
            asks: vec![BookLevel {
                price: decimal(ask_price),
                size: decimal("10"),
            }],
            min_order_size: None,
            tick_size: None,
        }
    }

    fn test_context() -> BtcFiveMinuteContext {
        BtcFiveMinuteContext {
            target: MarketTarget::Btc5m,
            interval_open_price: decimal("100"),
            target_price: decimal("100"),
            target_price_source: TargetPriceSource::BinanceWindowOpenFallback,
            target_gap_bps: Decimal::ZERO,
            current_spot_price: decimal("100"),
            current_spot_source: "test-fixture".to_owned(),
            current_spot_event_age_ms: None,
            current_spot_received_age_ms: None,
            current_spot_quote_points: None,
            exchange_book_age_ms: None,
            exchange_book_top_imbalance_bps: Decimal::ZERO,
            exchange_book_depth_imbalance_bps: Decimal::ZERO,
            exchange_book_microprice_bps: Decimal::ZERO,
            exchange_book_spread_bps: Decimal::ZERO,
            micro_burst_reference_price: decimal("100"),
            micro_reference_price: decimal("100"),
            spot_move_bps: Decimal::ZERO,
            spot_move_1s_bps: decimal("0.1"),
            spot_move_5s_bps: decimal("0.2"),
            spot_move_15s_bps: decimal("0.3"),
            micro_acceleration_bps: Decimal::ZERO,
            dominant_outcome: "Up".to_owned(),
            seconds_left: 180,
        }
    }

    #[test]
    fn hard_stop_loss_ignores_momentum_confirmation() {
        let mut exit_config = EarlyExitConfig::default();
        exit_config.max_loss_usdc = decimal("0.60");

        let reason = hard_stop_loss_reason(
            &test_context(),
            &exit_config,
            PaperOutcomeSide::Up,
            decimal("-0.61"),
        )
        .expect("hard stop should trigger from net loss alone");

        assert!(reason.contains("hard stop-loss"));
    }

    #[test]
    fn paper_mark_to_market_profit_uses_net_exit_payout() {
        let slug = "btc-updown-5m-net";
        let position = test_position(slug, "10", "0");
        let mut books = HashMap::new();
        books.insert(
            format!("{slug}-up"),
            test_book(&format!("{slug}-up"), "0.60", "10"),
        );

        let gross_profit = (paper_position_mark_to_market_payout(&position, &books)
            - position.spent_usdc)
            .round_dp(6);
        let net_profit = paper_position_net_mark_to_market_profit(
            &position,
            &books,
            PaperCostModel::new(100, 0),
        );

        assert_eq!(gross_profit, decimal("1.000000"));
        assert_eq!(net_profit, decimal("0.940000"));
    }

    #[test]
    fn scalp_exit_reason_takes_profit_from_executable_bid_delta() {
        let slug = "btc-updown-5m-scalp-tp";
        let mut position = test_position(slug, "10", "0");
        position.kind = OpportunityKind::CodexSentinelV1;
        position.opened_at = Utc::now() - chrono::Duration::seconds(10);

        let mut books = HashMap::new();
        books.insert(
            format!("{slug}-up"),
            test_book(&format!("{slug}-up"), "0.58", "10"),
        );

        let mut exit_config = EarlyExitConfig::default();
        exit_config.scalp_exit_enabled = true;
        exit_config.scalp_take_profit_price_delta = decimal("0.08");
        exit_config.scalp_stop_loss_price_delta = decimal("0.05");
        exit_config.scalp_time_stop_secs = 45;
        exit_config.max_loss_usdc = Decimal::ZERO;
        exit_config.min_hold_secs = 1;

        let reason = early_exit_reason(
            &position,
            &test_context(),
            &books,
            &exit_config,
            PaperCostModel::new(0, 0),
        )
        .expect("scalp take-profit should trigger on executable bid jump");

        assert!(reason.starts_with("early-exit scalp take-profit"));
    }

    #[test]
    fn scalp_exit_reason_waits_when_take_profit_delta_has_negative_net_mtm() {
        let slug = "btc-updown-5m-scalp-negative-net-tp";
        let mut position = test_position(slug, "10", "0");
        position.kind = OpportunityKind::CodexSentinelV1;
        position.opened_at = Utc::now() - chrono::Duration::seconds(10);
        position.spent_usdc = decimal("7");

        let mut books = HashMap::new();
        books.insert(
            format!("{slug}-up"),
            test_book(&format!("{slug}-up"), "0.58", "10"),
        );

        let mut exit_config = EarlyExitConfig::default();
        exit_config.scalp_exit_enabled = true;
        exit_config.scalp_take_profit_price_delta = decimal("0.08");
        exit_config.scalp_stop_loss_price_delta = Decimal::ZERO;
        exit_config.scalp_time_stop_secs = 45;
        exit_config.max_loss_usdc = Decimal::ZERO;
        exit_config.min_hold_secs = 1;

        let reason = early_exit_reason(
            &position,
            &test_context(),
            &books,
            &exit_config,
            PaperCostModel::new(0, 0),
        );

        assert!(
            reason.is_none(),
            "scalp take-profit should wait when full-position net MTM is still negative"
        );
    }

    #[test]
    fn scalp_exit_reason_closes_when_signal_is_invalidated() {
        let slug = "btc-updown-5m-scalp-invalidated";
        let mut position = test_position(slug, "0", "10");
        position.kind = OpportunityKind::CodexScalpProbeV1;
        position.dominant_outcome_at_entry = "Down".to_owned();
        position.opened_at = Utc::now() - chrono::Duration::seconds(20);
        position.spent_usdc = decimal("5");

        let mut books = HashMap::new();
        books.insert(
            format!("{slug}-down"),
            test_book(&format!("{slug}-down"), "0.38", "10"),
        );

        let mut context = test_context();
        context.target_gap_bps = decimal("1.25");
        context.spot_move_5s_bps = decimal("6.00");
        context.dominant_outcome = "Up".to_owned();

        let mut exit_config = EarlyExitConfig::default();
        exit_config.scalp_exit_enabled = true;
        exit_config.scalp_take_profit_price_delta = decimal("0.08");
        exit_config.scalp_stop_loss_price_delta = Decimal::ZERO;
        exit_config.scalp_time_stop_secs = 0;
        exit_config.scalp_invalidation_exit_enabled = true;
        exit_config.scalp_invalidation_min_loss_usdc = decimal("1.00");
        exit_config.scalp_invalidation_opposite_gap_bps = decimal("1.00");
        exit_config.scalp_invalidation_opposite_5s_bps = decimal("5.00");
        exit_config.max_loss_usdc = Decimal::ZERO;
        exit_config.min_hold_secs = 1;

        assert_eq!(
            paper_position_primary_side(&position),
            PaperOutcomeSide::Down
        );
        assert_eq!(
            paper_position_net_mark_to_market_profit(&position, &books, PaperCostModel::new(0, 0)),
            decimal("-1.200000")
        );

        let reason = early_exit_reason(
            &position,
            &context,
            &books,
            &exit_config,
            PaperCostModel::new(0, 0),
        )
        .expect("scalp position should close when the original signal is invalidated");

        assert!(reason.starts_with("early-exit scalp signal-invalidation"));
    }

    #[test]
    fn scalp_exit_reason_stops_loss_from_executable_bid_delta() {
        let slug = "btc-updown-5m-scalp-sl";
        let mut position = test_position(slug, "10", "0");
        position.kind = OpportunityKind::CodexSentinelV1;
        position.opened_at = Utc::now() - chrono::Duration::seconds(10);

        let mut books = HashMap::new();
        books.insert(
            format!("{slug}-up"),
            test_book(&format!("{slug}-up"), "0.44", "10"),
        );

        let mut exit_config = EarlyExitConfig::default();
        exit_config.scalp_exit_enabled = true;
        exit_config.scalp_take_profit_price_delta = decimal("0.08");
        exit_config.scalp_stop_loss_price_delta = decimal("0.05");
        exit_config.scalp_time_stop_secs = 45;
        exit_config.max_loss_usdc = decimal("10");
        exit_config.min_hold_secs = 1;

        let reason = early_exit_reason(
            &position,
            &test_context(),
            &books,
            &exit_config,
            PaperCostModel::new(0, 0),
        )
        .expect("scalp stop-loss should trigger on executable bid drop");

        assert!(reason.starts_with("early-exit scalp stop-loss"));
    }

    #[test]
    fn scalp_exit_reason_time_stops_stale_position() {
        let slug = "btc-updown-5m-scalp-time";
        let mut position = test_position(slug, "10", "0");
        position.kind = OpportunityKind::CodexSentinelV1;
        position.opened_at = Utc::now() - chrono::Duration::seconds(46);

        let mut books = HashMap::new();
        books.insert(
            format!("{slug}-up"),
            test_book(&format!("{slug}-up"), "0.51", "10"),
        );

        let mut exit_config = EarlyExitConfig::default();
        exit_config.scalp_exit_enabled = true;
        exit_config.scalp_take_profit_price_delta = decimal("0.08");
        exit_config.scalp_stop_loss_price_delta = decimal("0.05");
        exit_config.scalp_time_stop_secs = 45;
        exit_config.max_loss_usdc = Decimal::ZERO;
        exit_config.min_hold_secs = 1;

        let reason = early_exit_reason(
            &position,
            &test_context(),
            &books,
            &exit_config,
            PaperCostModel::new(0, 0),
        )
        .expect("scalp time-stop should close stale positions");

        assert!(reason.starts_with("early-exit scalp time-stop"));
    }

    #[test]
    fn scalp_exit_reason_closes_before_settlement_window() {
        let slug = "btc-updown-5m-scalp-near-expiry";
        let mut position = test_position(slug, "10", "0");
        position.kind = OpportunityKind::CodexScalpProbeV1;
        position.opened_at = Utc::now() - chrono::Duration::seconds(20);

        let mut books = HashMap::new();
        books.insert(
            format!("{slug}-up"),
            test_book(&format!("{slug}-up"), "0.46", "10"),
        );

        let mut context = test_context();
        context.seconds_left = 12;

        let mut exit_config = EarlyExitConfig::default();
        exit_config.scalp_exit_enabled = true;
        exit_config.scalp_take_profit_price_delta = decimal("0.08");
        exit_config.scalp_stop_loss_price_delta = Decimal::ZERO;
        exit_config.scalp_time_stop_secs = 45;
        exit_config.near_expiry_secs = 15;
        exit_config.max_loss_usdc = Decimal::ZERO;
        exit_config.min_hold_secs = 1;

        let reason = early_exit_reason(
            &position,
            &context,
            &books,
            &exit_config,
            PaperCostModel::new(0, 0),
        )
        .expect("scalp position should close before settlement when near expiry");

        assert!(reason.starts_with("early-exit scalp near-expiry"));
    }

    #[test]
    fn scalp_exit_reason_waits_when_bid_depth_cannot_cover_position() {
        let slug = "btc-updown-5m-scalp-depth";
        let mut position = test_position(slug, "10", "0");
        position.kind = OpportunityKind::CodexSentinelV1;
        position.opened_at = Utc::now() - chrono::Duration::seconds(10);

        let mut books = HashMap::new();
        books.insert(
            format!("{slug}-up"),
            test_book(&format!("{slug}-up"), "0.60", "2"),
        );

        let mut exit_config = EarlyExitConfig::default();
        exit_config.scalp_exit_enabled = true;
        exit_config.scalp_take_profit_price_delta = decimal("0.08");
        exit_config.scalp_stop_loss_price_delta = decimal("0.05");
        exit_config.scalp_time_stop_secs = 45;
        exit_config.max_loss_usdc = Decimal::ZERO;
        exit_config.min_hold_secs = 1;

        let reason = early_exit_reason(
            &position,
            &test_context(),
            &books,
            &exit_config,
            PaperCostModel::new(0, 0),
        );

        assert!(
            reason.is_none(),
            "scalp exit should not mark a full close when bid depth cannot cover the position"
        );
    }

    #[test]
    fn peak_exit_partial_plan_keeps_runner_after_profit_capture() {
        let slug = "btc-updown-5m-peak";
        let mut position = test_position(slug, "10", "0");
        position.opened_at = Utc::now() - chrono::Duration::seconds(10);

        let mut books = HashMap::new();
        books.insert(
            format!("{slug}-up"),
            test_peak_book(&format!("{slug}-up"), "0.70", "0.73"),
        );

        let mut exit_config = EarlyExitConfig::default();
        exit_config.peak_exit_enabled = true;
        exit_config.peak_exit_partial_close_enabled = true;
        exit_config.peak_exit_partial_close_ratio = decimal("0.65");
        exit_config.peak_exit_min_profit_usdc = decimal("0.80");
        exit_config.peak_exit_min_primary_ask_price = decimal("0.72");
        exit_config.min_hold_secs = 1;

        let (reason, fraction) = peak_exit_partial_plan(
            &position,
            &test_context(),
            &books,
            &exit_config,
            PaperCostModel::new(0, 0),
        )
        .expect("profitable stalled peak should close only a partial");

        assert_eq!(fraction, decimal("0.65"));
        assert!(reason.contains("partial peak-exit"));
    }

    #[test]
    fn profit_lock_partial_plan_banks_fast_mark_to_market_gain() {
        let slug = "btc-updown-5m-profit-lock";
        let mut position = test_position(slug, "10", "0");
        position.opened_at = Utc::now() - chrono::Duration::seconds(10);

        let mut books = HashMap::new();
        books.insert(
            format!("{slug}-up"),
            test_book(&format!("{slug}-up"), "0.56", "10"),
        );

        let mut exit_config = EarlyExitConfig::default();
        exit_config.profit_lock_partial_close_enabled = true;
        exit_config.profit_lock_partial_close_ratio = decimal("0.65");
        exit_config.profit_lock_min_profit_usdc = decimal("0.55");
        exit_config.min_hold_secs = 1;

        let (reason, fraction) = profit_lock_partial_exit_plan(
            &position,
            &test_context(),
            &books,
            &exit_config,
            PaperCostModel::new(0, 0),
        )
        .expect("fast profitable mtm should bank a partial before reversal");

        assert_eq!(fraction, decimal("0.65"));
        assert!(reason.contains("partial profit-lock"));
    }

    #[test]
    fn v4_inventory_overlay_blocks_when_post_fill_exceeds_gross_cap() {
        let overlay = test_v4_overlay();
        let mut tracker = V4InventoryTracker::default();
        let slug = "btc-updown-5m-test";
        let mut paper_state = PaperState::default();
        paper_state
            .open_positions
            .insert(slug.to_owned(), test_position(slug, "8", "0"));
        let opportunity = test_opportunity(slug, "5", "Up");

        let reason = v4_inventory_block_reason(&overlay, &mut tracker, &paper_state, &opportunity)
            .expect("gross cap should block");

        assert!(reason.contains("gross inventory cap exceeded"));
    }

    #[test]
    fn v4_inventory_overlay_blocks_when_slug_is_on_cooldown() {
        let overlay = test_v4_overlay();
        let mut tracker = V4InventoryTracker::default();
        let slug = "btc-updown-5m-cooldown";
        let reports = vec![PaperCloseReport {
            closed_at: Utc::now(),
            slug: slug.to_owned(),
            condition_id: "condition".to_owned(),
            question: "Test".to_owned(),
            kind: OpportunityKind::BonereaperStateV2,
            dominant_outcome_at_entry: "Up".to_owned(),
            actual_outcome: WindowDirection::Flat,
            realized_payout_usdc: decimal("4"),
            realized_profit_usdc: decimal("-1"),
            close_reason: "early-exit stop-loss: mtm -1.0 USDC".to_owned(),
            holding_seconds: 5,
            spent_usdc: decimal("5"),
        }];
        tracker.observe_closed_positions(&overlay, &reports);

        let opportunity = test_opportunity(slug, "2", "Up");
        let reason =
            v4_inventory_block_reason(&overlay, &mut tracker, &PaperState::default(), &opportunity)
                .expect("cooldown should block");

        assert!(reason.contains("cooldown active"));
    }

    #[test]
    fn v4_inventory_overlay_blocks_when_post_fill_exceeds_directional_delta_cap() {
        let mut overlay = test_v4_overlay();
        overlay.max_gross_inventory_shares_per_window = decimal("20");
        overlay.max_directional_delta_shares_per_window = decimal("6");
        let mut tracker = V4InventoryTracker::default();
        let slug = "btc-updown-5m-delta";
        let mut paper_state = PaperState::default();
        paper_state
            .open_positions
            .insert(slug.to_owned(), test_position(slug, "4", "2"));
        let opportunity = test_opportunity(slug, "5", "Up");

        let reason = v4_inventory_block_reason(&overlay, &mut tracker, &paper_state, &opportunity)
            .expect("delta cap should block");

        assert!(reason.contains("directional delta cap exceeded"));
    }

    #[test]
    fn v4_inventory_overlay_blocks_when_post_fill_exceeds_window_spent_cap() {
        let mut overlay = test_v4_overlay();
        overlay.max_gross_inventory_shares_per_window = decimal("100");
        overlay.max_directional_delta_shares_per_window = decimal("100");
        overlay.max_window_spent_usdc = decimal("12");

        let mut tracker = V4InventoryTracker::default();
        let slug = "btc-updown-5m-spent";
        tracker.observe_opened_opportunity(&test_opportunity(slug, "4", "Up"));
        tracker.observe_opened_opportunity(&test_opportunity(slug, "4", "Up"));
        let opportunity = test_opportunity(slug, "4", "Up");

        let reason =
            v4_inventory_block_reason(&overlay, &mut tracker, &PaperState::default(), &opportunity)
                .expect("window spent cap should block");

        assert!(reason.contains("window spent cap exceeded"));
    }

    #[test]
    fn v4_inventory_overlay_blocks_when_post_fill_exceeds_entry_count_cap() {
        let mut overlay = test_v4_overlay();
        overlay.max_gross_inventory_shares_per_window = decimal("100");
        overlay.max_directional_delta_shares_per_window = decimal("100");
        overlay.max_entries_per_window = 2;

        let mut tracker = V4InventoryTracker::default();
        let slug = "btc-updown-5m-entries";
        tracker.observe_opened_opportunity(&test_opportunity(slug, "4", "Up"));
        tracker.observe_opened_opportunity(&test_opportunity(slug, "4", "Up"));
        let opportunity = test_opportunity(slug, "4", "Up");

        let reason =
            v4_inventory_block_reason(&overlay, &mut tracker, &PaperState::default(), &opportunity)
                .expect("entry count cap should block");

        assert!(reason.contains("entry-count cap exceeded"));
    }

    #[test]
    fn v4_inventory_overlay_applies_to_guarded_bonereaper() {
        let mut overlay = test_v4_overlay();
        overlay.max_gross_inventory_shares_per_window = decimal("10");
        let mut tracker = V4InventoryTracker::default();
        let slug = "btc-updown-5m-guarded";
        let mut paper_state = PaperState::default();
        paper_state
            .open_positions
            .insert(slug.to_owned(), test_position(slug, "8", "0"));
        let mut opportunity = test_opportunity(slug, "5", "Up");
        opportunity.kind = OpportunityKind::BonereaperStateGuarded;

        let reason = v4_inventory_block_reason(&overlay, &mut tracker, &paper_state, &opportunity)
            .expect("guarded bonereaper should be protected by v4 inventory caps");

        assert!(reason.contains("gross inventory cap exceeded"));
    }

    #[test]
    fn classify_close_reason_detects_machine_readable_window_settlement() {
        assert_eq!(
            classify_close_reason(WINDOW_SETTLEMENT_REASON),
            "window_settlement"
        );
    }

    #[test]
    fn classify_close_reason_detects_english_settlement_phrase() {
        assert_eq!(
            classify_close_reason("position resolved at settlement close"),
            "window_settlement"
        );
    }

    #[test]
    fn classify_close_reason_detects_hard_stop_loss() {
        assert_eq!(
            classify_close_reason("early-exit hard stop-loss: net mtm -1.0 USDC"),
            "early_exit_hard_stop_loss"
        );
    }

    #[test]
    fn classify_close_reason_detects_scalp_exits() {
        assert_eq!(
            classify_close_reason("early-exit scalp take-profit: entry 0.5000"),
            "early_exit_scalp_take_profit"
        );
        assert_eq!(
            classify_close_reason("early-exit scalp stop-loss: entry 0.5000"),
            "early_exit_scalp_stop_loss"
        );
        assert_eq!(
            classify_close_reason("early-exit scalp signal-invalidation: entry 0.5000"),
            "early_exit_scalp_signal_invalidation"
        );
        assert_eq!(
            classify_close_reason("early-exit scalp near-expiry: 12s left"),
            "early_exit_scalp_near_expiry"
        );
        assert_eq!(
            classify_close_reason("early-exit scalp time-stop: entry 0.5000"),
            "early_exit_scalp_time_stop"
        );
    }
}
