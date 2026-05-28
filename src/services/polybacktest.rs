//! Historical strategy backtesting via the `PolyBackTest` API.

use std::collections::{HashMap, HashSet};
use std::env;
use std::fs;
use std::hash::{Hash, Hasher};
use std::io::{self, IsTerminal, Write};
use std::path::PathBuf;
use std::time::Duration;
use std::{collections::hash_map::DefaultHasher, time::SystemTime};

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use futures_util::stream::{self, StreamExt};
use reqwest::{Client, Response, StatusCode};
use rust_decimal::Decimal;
use serde_json::Value;
use tokio::time::{sleep, timeout};
use tracing::{info, warn};

use crate::config::{AppConfig, PolyBacktestConfig, V4InventoryConfig};
use crate::error::{AppError, Result};
use crate::models::{
    BinaryMarket, BookLevel, MarketTarget, Opportunity, OpportunityKind, OrderBook,
    PaperOutcomeSide,
};

use super::backtest::{
    BacktestNearMiss, BacktestReport, BacktestSignal, BacktestTargetSummary, ScalpExitReport,
};
use super::binance::{BinanceClient, MarketWindowContext, WindowDirection};
use super::labels::{outcome_label_is_down, outcome_label_is_flat, outcome_label_is_up};
use super::market_data::{MarketDataClient, TradeFlowSummary, TradeFlowWindow};
use super::strategy::{BundleArbitrageStrategy, NearMiss};

const FALLBACK_ORDERBOOK_SIZE: u32 = 1_000;
const MAX_RATE_LIMIT_RETRIES: u32 = 5;
const DEFAULT_RETRY_AFTER_SECS: u64 = 2;
const MAX_RETRY_AFTER_SECS: u64 = 20;
const MAX_TIMEOUT_RETRIES: u32 = 3;
const DEFAULT_TIMEOUT_RETRY_SECS: u64 = 3;
const SNAPSHOT_MIN_GAP_SECS: i64 = 5;
const SNAPSHOT_MAX_SAMPLES_PER_WINDOW: usize = 120;
const MARKET_PROCESS_TIMEOUT_MIN_SECS: u64 = 20;
const MARKET_PROCESS_TIMEOUT_MAX_SECS: u64 = 45;
const MARKET_CACHE_MAX_AGE_SECS: u64 = 300;
const MARKET_PAGE_LIMIT: usize = 100;
const MARKET_PREPARE_CONCURRENCY: usize = 6;
const MARKET_PREPARE_CONCURRENCY_MAX: usize = 24;
const MARKET_PREPARE_CONCURRENCY_ENV: &str = "POLYBACKTEST_MARKET_PREPARE_CONCURRENCY";
const TARGET_PREPARE_CONCURRENCY: usize = 2;
const TARGET_PREPARE_CONCURRENCY_MAX: usize = 8;
const TARGET_PREPARE_CONCURRENCY_ENV: &str = "POLYBACKTEST_TARGET_PREPARE_CONCURRENCY";
const SCALP_TIME_STOP_SECS: i64 = 45;

/// Runner that evaluates the local strategy using historical `PolyBackTest` snapshots.
pub struct PolyBacktestRunner<'a> {
    app_config: &'a AppConfig,
    strategy: &'a BundleArbitrageStrategy,
    binance_client: &'a BinanceClient,
    market_data_client: MarketDataClient,
    poly_client: PolyBacktestClient,
}

/// Historical market data loaded once and reusable across strategy variants.
#[derive(Debug, Clone)]
pub struct PolyBacktestDataset {
    entry_minutes: u32,
    targets: Vec<PreparedTargetBacktestData>,
}

#[derive(Debug, Clone)]
struct PreparedTargetBacktestData {
    target: MarketTarget,
    markets: Vec<PreparedMarketBacktestData>,
}

#[derive(Debug, Clone)]
struct PreparedMarketBacktestData {
    target: MarketTarget,
    market: PolyBacktestMarket,
    binary_market: BinaryMarket,
    snapshots: Vec<PolyBacktestSnapshot>,
    actual_outcome: WindowDirection,
    trade_flows: HashMap<String, TradeFlowSummary>,
    context_cache: HashMap<i64, MarketWindowContext>,
}

impl<'a> PolyBacktestRunner<'a> {
    /// Create a new PolyBackTest-powered historical runner.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP client cannot be built or the API key is missing.
    pub fn new(
        app_config: &'a AppConfig,
        strategy: &'a BundleArbitrageStrategy,
        binance_client: &'a BinanceClient,
    ) -> Result<Self> {
        let market_data_client = MarketDataClient::new(app_config.http.clone())?;
        let poly_client =
            PolyBacktestClient::new(&app_config.polybacktest, app_config.http.timeout_secs)?;

        Ok(Self {
            app_config,
            strategy,
            binance_client,
            market_data_client,
            poly_client,
        })
    }

    /// Run the configured strategy against recent resolved windows from `PolyBackTest`.
    ///
    /// # Errors
    ///
    /// Returns an error if fetching markets, snapshots, or Binance reference data fails.
    pub async fn run(
        &self,
        windows_per_target: usize,
        entry_minutes: u32,
    ) -> Result<BacktestReport> {
        let dataset = self
            .prepare_dataset(windows_per_target, entry_minutes)
            .await?;
        Ok(self.run_prepared(&dataset))
    }

    /// Load historical snapshots, Binance contexts, resolutions, and trade flow once.
    ///
    /// # Errors
    ///
    /// Returns an error if listing target markets fails.
    pub async fn prepare_dataset(
        &self,
        windows_per_target: usize,
        entry_minutes: u32,
    ) -> Result<PolyBacktestDataset> {
        let entry_offset_secs = i64::from(entry_minutes) * 60;
        let unique_targets = unique_targets(&self.app_config.strategy.market_targets);
        let target_concurrency = target_prepare_concurrency(unique_targets.len());
        let mut target_results = stream::iter(unique_targets.into_iter().enumerate())
            .map(|(index, target)| async move {
                (
                    index,
                    self.prepare_target_dataset(target, windows_per_target, entry_offset_secs)
                        .await,
                )
            })
            .buffer_unordered(target_concurrency)
            .collect::<Vec<_>>()
            .await;

        target_results.sort_by_key(|(index, _)| *index);
        let mut targets = Vec::with_capacity(target_results.len());
        for (_index, target_result) in target_results {
            targets.push(target_result?);
        }

        Ok(PolyBacktestDataset {
            entry_minutes,
            targets,
        })
    }

    /// Load reusable historical data for multiple entry offsets in one pass.
    ///
    /// # Errors
    ///
    /// Returns an error if listing target markets fails.
    pub async fn prepare_datasets(
        &self,
        windows_per_target: usize,
        entry_minutes_values: &[u32],
    ) -> Result<Vec<PolyBacktestDataset>> {
        let mut entry_minutes = entry_minutes_values.to_vec();
        entry_minutes.sort_unstable();
        entry_minutes.dedup();
        if entry_minutes.is_empty() {
            return Ok(Vec::new());
        }

        let entries = entry_minutes
            .iter()
            .map(|entry_minutes| (*entry_minutes, i64::from(*entry_minutes) * 60))
            .collect::<Vec<_>>();
        let mut targets_by_entry = entry_minutes
            .iter()
            .map(|entry_minutes| (*entry_minutes, Vec::new()))
            .collect::<HashMap<_, _>>();

        let unique_targets = unique_targets(&self.app_config.strategy.market_targets);
        let target_concurrency = target_prepare_concurrency(unique_targets.len());
        let mut target_results = stream::iter(unique_targets.into_iter().enumerate())
            .map(|(index, target)| {
                let entries = &entries;
                async move {
                    (
                        index,
                        self.prepare_target_datasets(target, windows_per_target, entries)
                            .await,
                    )
                }
            })
            .buffer_unordered(target_concurrency)
            .collect::<Vec<_>>()
            .await;

        target_results.sort_by_key(|(index, _)| *index);
        for (_index, target_result) in target_results {
            for (entry_minutes, target_data) in target_result? {
                targets_by_entry
                    .entry(entry_minutes)
                    .or_default()
                    .push(target_data);
            }
        }

        Ok(entry_minutes
            .into_iter()
            .map(|entry_minutes| PolyBacktestDataset {
                entry_minutes,
                targets: targets_by_entry.remove(&entry_minutes).unwrap_or_default(),
            })
            .collect())
    }

    /// Evaluate the current strategy/config against a preloaded dataset.
    #[must_use]
    pub fn run_prepared(&self, dataset: &PolyBacktestDataset) -> BacktestReport {
        let mut summaries = Vec::new();
        let mut signals = Vec::new();
        let mut near_misses = Vec::new();

        for target_data in &dataset.targets {
            let target_report = self.evaluate_prepared_target(target_data);
            summaries.push(target_report.summary);
            signals.extend(target_report.signals);
            near_misses.extend(target_report.near_misses);
        }

        signals.sort_by(|left, right| {
            right
                .realized_profit
                .cmp(&left.realized_profit)
                .then_with(|| right.expected_profit.cmp(&left.expected_profit))
        });

        BacktestReport {
            entry_minutes: dataset.entry_minutes,
            summaries,
            signals,
            near_misses,
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn prepare_target_dataset(
        &self,
        target: MarketTarget,
        windows_per_target: usize,
        entry_offset_secs: i64,
    ) -> Result<PreparedTargetBacktestData> {
        let markets = self
            .poly_client
            .list_markets(target, windows_per_target)
            .await?;

        let total_markets = markets.len();
        let trade_flows_by_slug = match self
            .fetch_target_trade_flow_summaries(&markets, entry_offset_secs)
            .await
        {
            Ok(trade_flows) => trade_flows,
            Err(error) => {
                warn!(
                    target = target.label(),
                    error = %error,
                    "PolyBackTest trade-flow prefetch failed; continuing without trade-flow summaries"
                );
                HashMap::new()
            }
        };
        let market_timeout = self.market_process_timeout();
        let concurrency = market_prepare_concurrency(total_markets);
        let mut prepared_results = stream::iter(markets.iter().enumerate())
            .map(|(index, market)| {
                let trade_flows_by_slug = &trade_flows_by_slug;
                async move {
                    let window_number = index.saturating_add(1);
                    info!(
                        target = target.label(),
                        slug = %market.slug,
                        window = window_number,
                        total = total_markets,
                        timeout_secs = market_timeout.as_secs(),
                        concurrency,
                        "PolyBackTest window start"
                    );

                    let result = timeout(
                        market_timeout,
                        self.prepare_market_data(
                            target,
                            market,
                            entry_offset_secs,
                            trade_flows_by_slug,
                        ),
                    )
                    .await;

                    let prepared_market = match result {
                        Ok(Ok(Some(prepared_market))) => {
                            info!(
                                target = target.label(),
                                slug = %market.slug,
                                window = window_number,
                                total = total_markets,
                                "PolyBackTest window prepared"
                            );
                            Some(prepared_market)
                        }
                        Ok(Ok(None)) => {
                            info!(
                                target = target.label(),
                                slug = %market.slug,
                                window = window_number,
                                total = total_markets,
                                "PolyBackTest window skipped"
                            );
                            None
                        }
                        Ok(Err(error)) => {
                            warn!(
                                target = target.label(),
                                slug = %market.slug,
                                window = window_number,
                                total = total_markets,
                                error = %error,
                                "PolyBackTest window failed; skipping"
                            );
                            None
                        }
                        Err(_) => {
                            warn!(
                                target = target.label(),
                                slug = %market.slug,
                                window = window_number,
                                total = total_markets,
                                timeout_secs = market_timeout.as_secs(),
                                "PolyBackTest window timed out; skipping"
                            );
                            None
                        }
                    };

                    (index, prepared_market)
                }
            })
            .buffer_unordered(concurrency)
            .collect::<Vec<_>>()
            .await;

        prepared_results.sort_by_key(|(index, _)| *index);
        let prepared_markets = prepared_results
            .into_iter()
            .filter_map(|(_, prepared_market)| prepared_market)
            .collect();

        Ok(PreparedTargetBacktestData {
            target,
            markets: prepared_markets,
        })
    }

    async fn fetch_target_trade_flow_summaries(
        &self,
        markets: &[PolyBacktestMarket],
        entry_offset_secs: i64,
    ) -> Result<HashMap<String, TradeFlowSummary>> {
        let windows = markets
            .iter()
            .filter_map(|market| {
                let entry_time = market.start_time + ChronoDuration::seconds(entry_offset_secs);
                (entry_time < market.end_time).then(|| TradeFlowWindow {
                    slug: market.slug.clone(),
                    condition_id: market.market_id.clone(),
                    start_ts_ms: market.start_time.timestamp_millis(),
                    end_ts_ms: entry_time.timestamp_millis(),
                })
            })
            .collect::<Vec<_>>();

        self.market_data_client
            .fetch_trade_flow_summaries(&windows)
            .await
    }

    #[allow(clippy::too_many_lines)]
    async fn prepare_target_datasets(
        &self,
        target: MarketTarget,
        windows_per_target: usize,
        entries: &[(u32, i64)],
    ) -> Result<Vec<(u32, PreparedTargetBacktestData)>> {
        let markets = self
            .poly_client
            .list_markets(target, windows_per_target)
            .await?;
        let total_markets = markets.len();
        let mut trade_flows_by_entry = HashMap::new();

        for (entry_minutes, entry_offset_secs) in entries {
            let trade_flows = match self
                .fetch_target_trade_flow_summaries(&markets, *entry_offset_secs)
                .await
            {
                Ok(trade_flows) => trade_flows,
                Err(error) => {
                    warn!(
                        target = target.label(),
                        entry_minutes,
                        error = %error,
                        "PolyBackTest trade-flow prefetch failed; continuing without trade-flow summaries"
                    );
                    HashMap::new()
                }
            };
            trade_flows_by_entry.insert(*entry_minutes, trade_flows);
        }

        let mut prepared_by_entry = entries
            .iter()
            .map(|(entry_minutes, _)| (*entry_minutes, Vec::new()))
            .collect::<HashMap<_, _>>();
        let market_timeout = self.market_process_timeout();
        let concurrency = market_prepare_concurrency(total_markets);
        let mut prepared_results = stream::iter(markets.iter().enumerate())
            .map(|(index, market)| {
                let trade_flows_by_entry = &trade_flows_by_entry;
                async move {
                    let window_number = index.saturating_add(1);
                    info!(
                        target = target.label(),
                        slug = %market.slug,
                        window = window_number,
                        total = total_markets,
                        timeout_secs = market_timeout.as_secs(),
                        concurrency,
                        "PolyBackTest multi-entry window start"
                    );

                    let result = timeout(
                        market_timeout,
                        self.prepare_market_data_for_entries(
                            target,
                            market,
                            entries,
                            trade_flows_by_entry,
                        ),
                    )
                    .await;

                    let prepared_markets = match result {
                        Ok(Ok(prepared_markets)) if !prepared_markets.is_empty() => {
                            let prepared_entry_count = prepared_markets.len();
                            info!(
                                target = target.label(),
                                slug = %market.slug,
                                window = window_number,
                                total = total_markets,
                                prepared_entry_count,
                                "PolyBackTest multi-entry window prepared"
                            );
                            prepared_markets
                        }
                        Ok(Ok(_)) => {
                            info!(
                                target = target.label(),
                                slug = %market.slug,
                                window = window_number,
                                total = total_markets,
                                "PolyBackTest multi-entry window skipped"
                            );
                            Vec::new()
                        }
                        Ok(Err(error)) => {
                            warn!(
                                target = target.label(),
                                slug = %market.slug,
                                window = window_number,
                                total = total_markets,
                                error = %error,
                                "PolyBackTest multi-entry window failed; skipping"
                            );
                            Vec::new()
                        }
                        Err(_) => {
                            warn!(
                                target = target.label(),
                                slug = %market.slug,
                                window = window_number,
                                total = total_markets,
                                timeout_secs = market_timeout.as_secs(),
                                "PolyBackTest multi-entry window timed out; skipping"
                            );
                            Vec::new()
                        }
                    };

                    (index, prepared_markets)
                }
            })
            .buffer_unordered(concurrency)
            .collect::<Vec<_>>()
            .await;

        prepared_results.sort_by_key(|(index, _)| *index);
        for (_index, prepared_markets) in prepared_results {
            for (entry_minutes, prepared_market) in prepared_markets {
                prepared_by_entry
                    .entry(entry_minutes)
                    .or_default()
                    .push(prepared_market);
            }
        }

        Ok(entries
            .iter()
            .map(|(entry_minutes, _)| {
                (
                    *entry_minutes,
                    PreparedTargetBacktestData {
                        target,
                        markets: prepared_by_entry.remove(entry_minutes).unwrap_or_default(),
                    },
                )
            })
            .collect())
    }

    async fn prepare_market_data_for_entries(
        &self,
        target: MarketTarget,
        market: &PolyBacktestMarket,
        entries: &[(u32, i64)],
        trade_flows_by_entry: &HashMap<u32, HashMap<String, TradeFlowSummary>>,
    ) -> Result<Vec<(u32, PreparedMarketBacktestData)>> {
        let Some(earliest_offset_secs) = entries
            .iter()
            .map(|(_entry_minutes, entry_offset_secs)| *entry_offset_secs)
            .min()
        else {
            return Ok(Vec::new());
        };
        let earliest_entry_time = market.start_time + ChronoDuration::seconds(earliest_offset_secs);
        if earliest_entry_time >= market.end_time {
            return Ok(Vec::new());
        }

        let snapshots = self
            .poly_client
            .fetch_snapshots_in_window(
                target,
                &market.market_id,
                earliest_entry_time,
                market.end_time,
            )
            .await?;
        if snapshots.is_empty() {
            return Ok(Vec::new());
        }

        let Some(resolution) = self
            .binance_client
            .resolution_from_slug(&market.slug)
            .await?
        else {
            return Ok(Vec::new());
        };

        let binary_market = market.to_binary_market();
        let mut snapshots_by_entry = Vec::new();
        let mut elapsed_secs_values = Vec::new();

        for (entry_minutes, entry_offset_secs) in entries {
            let entry_time = market.start_time + ChronoDuration::seconds(*entry_offset_secs);
            if entry_time >= market.end_time {
                continue;
            }
            let entry_snapshots = downsample_snapshots(
                snapshots
                    .iter()
                    .filter(|snapshot| {
                        snapshot.timestamp >= entry_time && snapshot.timestamp < market.end_time
                    })
                    .cloned()
                    .collect(),
            );
            if entry_snapshots.is_empty() {
                continue;
            }
            elapsed_secs_values.extend(entry_snapshots.iter().map(|snapshot| {
                (snapshot.timestamp.timestamp() - market.start_time.timestamp()).max(0)
            }));
            snapshots_by_entry.push((*entry_minutes, entry_snapshots));
        }

        if snapshots_by_entry.is_empty() {
            return Ok(Vec::new());
        }

        let context_cache = self
            .binance_client
            .historical_contexts_from_slug(&market.slug, &elapsed_secs_values)
            .await?;
        if context_cache.is_empty() {
            return Ok(Vec::new());
        }

        Ok(snapshots_by_entry
            .into_iter()
            .map(|(entry_minutes, entry_snapshots)| {
                let trade_flows = trade_flows_by_entry
                    .get(&entry_minutes)
                    .and_then(|trade_flows| trade_flows.get(&binary_market.slug))
                    .copied()
                    .map(|summary| HashMap::from([(binary_market.slug.clone(), summary)]))
                    .unwrap_or_default();

                (
                    entry_minutes,
                    PreparedMarketBacktestData {
                        target,
                        market: market.clone(),
                        binary_market: binary_market.clone(),
                        snapshots: entry_snapshots,
                        actual_outcome: resolution.actual_outcome,
                        trade_flows,
                        context_cache: context_cache.clone(),
                    },
                )
            })
            .collect())
    }

    fn evaluate_prepared_target(
        &self,
        target_data: &PreparedTargetBacktestData,
    ) -> TargetBacktestResult {
        let mut signals = Vec::new();
        let mut near_misses = Vec::new();

        for market_data in &target_data.markets {
            let market_result = self.evaluate_prepared_market(market_data);
            signals.extend(market_result.signals);
            if let Some(near_miss) = market_result.near_miss {
                near_misses.push(near_miss);
            }
        }

        let signal_count = signals.len();
        let near_miss_count = near_misses.len();
        let realized_profit = signals
            .iter()
            .fold(Decimal::ZERO, |total, signal| {
                total + signal.realized_profit
            })
            .round_dp(6);
        let expected_profit = signals
            .iter()
            .fold(Decimal::ZERO, |total, signal| {
                total + signal.expected_profit
            })
            .round_dp(6);
        let accurate_signals = signals
            .iter()
            .filter(|signal| polybacktest_signal_was_successful(signal))
            .count();

        TargetBacktestResult {
            summary: BacktestTargetSummary {
                target: target_data.target,
                sampled_windows: target_data.markets.len(),
                signal_count,
                near_miss_count,
                resolved_signal_count: signal_count,
                realized_profit,
                expected_profit,
                signal_accuracy_pct: percentage_or_zero(accurate_signals, signal_count),
            },
            signals,
            near_misses,
        }
    }

    fn market_process_timeout(&self) -> Duration {
        Duration::from_secs(self.app_config.http.timeout_secs.saturating_mul(5).clamp(
            MARKET_PROCESS_TIMEOUT_MIN_SECS,
            MARKET_PROCESS_TIMEOUT_MAX_SECS,
        ))
    }

    async fn prepare_market_data(
        &self,
        target: MarketTarget,
        market: &PolyBacktestMarket,
        entry_offset_secs: i64,
        trade_flows_by_slug: &HashMap<String, TradeFlowSummary>,
    ) -> Result<Option<PreparedMarketBacktestData>> {
        let entry_time = market.start_time + ChronoDuration::seconds(entry_offset_secs);
        if entry_time >= market.end_time {
            return Ok(None);
        }

        let snapshots = self
            .poly_client
            .fetch_snapshots_in_window(target, &market.market_id, entry_time, market.end_time)
            .await?;
        if snapshots.is_empty() {
            return Ok(None);
        }
        let snapshots = downsample_snapshots(snapshots);
        if snapshots.is_empty() {
            return Ok(None);
        }

        let Some(resolution) = self
            .binance_client
            .resolution_from_slug(&market.slug)
            .await?
        else {
            return Ok(None);
        };

        let binary_market = market.to_binary_market();
        let trade_flows = trade_flows_by_slug
            .get(&binary_market.slug)
            .copied()
            .map(|summary| HashMap::from([(binary_market.slug.clone(), summary)]))
            .unwrap_or_default();

        let elapsed_secs_values = snapshots
            .iter()
            .filter(|snapshot| snapshot.timestamp < market.end_time)
            .map(|snapshot| (snapshot.timestamp.timestamp() - market.start_time.timestamp()).max(0))
            .collect::<Vec<_>>();
        let context_cache = self
            .binance_client
            .historical_contexts_from_slug(&market.slug, &elapsed_secs_values)
            .await?;
        if context_cache.is_empty() {
            return Ok(None);
        }

        Ok(Some(PreparedMarketBacktestData {
            target,
            market: market.clone(),
            binary_market,
            snapshots,
            actual_outcome: resolution.actual_outcome,
            trade_flows,
            context_cache,
        }))
    }

    #[allow(clippy::too_many_lines)]
    fn evaluate_prepared_market(
        &self,
        market_data: &PreparedMarketBacktestData,
    ) -> ProcessedMarketResult {
        let mut signals = Vec::new();
        let mut market_notional = HashMap::<String, Decimal>::new();
        let mut v4_inventory = PolyBacktestV4InventoryState::default();
        let mut market_emitted_signal = false;
        let mut market_near_miss = None;

        for (snapshot_index, snapshot) in market_data.snapshots.iter().enumerate() {
            if snapshot.timestamp >= market_data.market.end_time {
                continue;
            }
            if market_emitted_signal && !self.app_config.run.allow_repeat_entries_same_window {
                break;
            }

            let elapsed_secs =
                (snapshot.timestamp.timestamp() - market_data.market.start_time.timestamp()).max(0);
            let context_key =
                elapsed_secs.clamp(0, market_data.target.window_secs().saturating_sub(1));
            let Some(mut context) = market_data.context_cache.get(&context_key).cloned() else {
                continue;
            };
            context.seconds_left =
                (market_data.market.end_time.timestamp() - snapshot.timestamp.timestamp()).max(0);

            let books = snapshot.to_order_books(
                &market_data.binary_market,
                self.app_config
                    .strategy
                    .min_top_of_book_shares
                    .max(Decimal::from(FALLBACK_ORDERBOOK_SIZE)),
            );
            let contexts = HashMap::from([(market_data.binary_market.slug.clone(), context)]);

            let opportunities = self.strategy.find_opportunities(
                std::slice::from_ref(&market_data.binary_market),
                &books,
                &market_notional,
                &contexts,
                &market_data.trade_flows,
            );

            if let Some(opportunity) = opportunities.into_iter().next() {
                if let Some(reason) = polybacktest_v4_block_reason(
                    &self.app_config.run.v4_inventory,
                    &v4_inventory,
                    &opportunity,
                ) {
                    if market_near_miss.is_none() {
                        market_near_miss = Some(near_miss_from_blocked_opportunity(
                            market_data.target,
                            &opportunity,
                            reason,
                        ));
                    }
                    continue;
                }

                market_emitted_signal = true;
                let allocated = market_notional
                    .entry(opportunity.condition_id.clone())
                    .or_insert(Decimal::ZERO);
                *allocated += opportunity.required_usdc;
                let scalp_exit = scalp_exit_for_opportunity(
                    &opportunity,
                    &market_data.snapshots[snapshot_index..],
                    snapshot.timestamp,
                    self.app_config.strategy.assumed_fee_bps,
                );
                v4_inventory.observe_opened(&opportunity);
                signals.push(signal_from_opportunity(
                    market_data.target,
                    opportunity,
                    market_data.actual_outcome,
                    scalp_exit,
                ));
                continue;
            }

            if market_near_miss.is_none() {
                market_near_miss = self
                    .strategy
                    .find_near_misses(
                        std::slice::from_ref(&market_data.binary_market),
                        &books,
                        &market_notional,
                        &contexts,
                        &market_data.trade_flows,
                        1,
                    )
                    .into_iter()
                    .next()
                    .map(|near_miss| near_miss_from_report(market_data.target, near_miss));
            }
        }

        if market_emitted_signal {
            market_near_miss = None;
        }

        ProcessedMarketResult {
            signals,
            near_miss: market_near_miss,
        }
    }
}

#[derive(Debug)]
struct TargetBacktestResult {
    summary: BacktestTargetSummary,
    signals: Vec<BacktestSignal>,
    near_misses: Vec<BacktestNearMiss>,
}

#[derive(Debug)]
struct ProcessedMarketResult {
    signals: Vec<BacktestSignal>,
    near_miss: Option<BacktestNearMiss>,
}

#[derive(Debug, Clone, Default)]
#[allow(clippy::struct_field_names)]
struct PolyBacktestV4InventoryState {
    spent_by_slug: HashMap<String, Decimal>,
    entries_by_slug: HashMap<String, u32>,
    up_shares_by_slug: HashMap<String, Decimal>,
    down_shares_by_slug: HashMap<String, Decimal>,
}

impl PolyBacktestV4InventoryState {
    fn observe_opened(&mut self, opportunity: &Opportunity) {
        let spent = self
            .spent_by_slug
            .entry(opportunity.slug.clone())
            .or_default();
        *spent = (*spent + opportunity.required_usdc).round_dp(6);

        let entries = self
            .entries_by_slug
            .entry(opportunity.slug.clone())
            .or_default();
        *entries = entries.saturating_add(1);

        let (up_shares, down_shares) = opportunity_side_shares(opportunity);
        let up = self
            .up_shares_by_slug
            .entry(opportunity.slug.clone())
            .or_default();
        *up = (*up + up_shares).round_dp(6);
        let down = self
            .down_shares_by_slug
            .entry(opportunity.slug.clone())
            .or_default();
        *down = (*down + down_shares).round_dp(6);
    }

    fn spent_for_slug(&self, slug: &str) -> Decimal {
        self.spent_by_slug
            .get(slug)
            .copied()
            .unwrap_or(Decimal::ZERO)
    }

    fn entries_for_slug(&self, slug: &str) -> u32 {
        self.entries_by_slug.get(slug).copied().unwrap_or(0)
    }

    fn side_shares_for_slug(&self, slug: &str) -> (Decimal, Decimal) {
        (
            self.up_shares_by_slug
                .get(slug)
                .copied()
                .unwrap_or(Decimal::ZERO),
            self.down_shares_by_slug
                .get(slug)
                .copied()
                .unwrap_or(Decimal::ZERO),
        )
    }
}

#[derive(Debug, Clone)]
struct PolyBacktestClient {
    http: Client,
    base_url: String,
    api_key: String,
    api_key_env: String,
    snapshot_page_limit: usize,
    include_orderbook: bool,
    cache_dir: Option<PathBuf>,
}

impl PolyBacktestClient {
    fn new(config: &PolyBacktestConfig, timeout_secs: u64) -> Result<Self> {
        let api_key = resolve_polybacktest_api_key(config)?;
        let http = Client::builder()
            .timeout(Duration::from_secs(timeout_secs))
            .user_agent("polymarket_mvp/0.1.0")
            .build()?;

        Ok(Self {
            http,
            base_url: config.base_url.clone(),
            api_key,
            api_key_env: config.api_key_env.clone(),
            snapshot_page_limit: config.snapshot_page_limit.clamp(10, 1000),
            include_orderbook: config.include_orderbook,
            cache_dir: config.cache_enabled.then(|| config.cache_dir.clone()),
        })
    }

    async fn list_markets(
        &self,
        target: MarketTarget,
        limit: usize,
    ) -> Result<Vec<PolyBacktestMarket>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let mut offset = 0_usize;
        let mut collected = Vec::new();
        let mut seen_slugs = HashSet::new();

        while collected.len() < limit {
            let page_limit = (limit - collected.len()).min(MARKET_PAGE_LIMIT);
            let response = self.fetch_markets_json(target, page_limit, offset).await?;

            let markets = response
                .get("markets")
                .and_then(Value::as_array)
                .ok_or_else(|| {
                    AppError::InvalidMarket("PolyBackTest не вернул массив markets".to_owned())
                })?;
            let raw_page_len = markets.len();
            if raw_page_len == 0 {
                break;
            }

            for value in markets {
                let Some(market) = PolyBacktestMarket::from_value(value)? else {
                    continue;
                };
                if seen_slugs.insert(market.slug.clone()) {
                    collected.push(market);
                }
                if collected.len() >= limit {
                    break;
                }
            }

            if raw_page_len < page_limit {
                break;
            }
            offset = offset.saturating_add(raw_page_len);
        }

        Ok(collected)
    }

    async fn fetch_snapshots_in_window(
        &self,
        target: MarketTarget,
        market_id: &str,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<PolyBacktestSnapshot>> {
        let mut offset = 0_usize;
        let mut snapshots = Vec::new();

        loop {
            let response = self
                .fetch_snapshots_json(target, market_id, start_time, end_time, offset)
                .await?;

            let page = response
                .get("snapshots")
                .and_then(Value::as_array)
                .ok_or_else(|| {
                    AppError::InvalidMarket("PolyBackTest не вернул массив snapshots".to_owned())
                })?;

            if page.is_empty() {
                break;
            }

            let page_snapshots = page
                .iter()
                .filter_map(PolyBacktestSnapshot::from_value)
                .collect::<Vec<_>>();
            snapshots.extend(page_snapshots);

            if page.len() < self.snapshot_page_limit {
                break;
            }
            offset += self.snapshot_page_limit;
        }

        snapshots.sort_by_key(|snapshot| snapshot.timestamp);
        snapshots.dedup_by_key(|snapshot| snapshot.timestamp);
        Ok(snapshots)
    }

    async fn fetch_markets_json(
        &self,
        target: MarketTarget,
        limit: usize,
        offset: usize,
    ) -> Result<Value> {
        let mut attempt = 0_u32;

        loop {
            let url = markets_url(&self.base_url, target, limit, offset);
            let cache_key = format!("markets:{url}");
            let max_cache_age = self
                .has_api_key()
                .then_some(Duration::from_secs(MARKET_CACHE_MAX_AGE_SECS));
            if let Some(cached) = self.read_cached_json(&cache_key, max_cache_age) {
                return Ok(cached);
            }
            let api_key = self.api_key_or_err()?;
            let response = match self
                .http
                .get(url)
                .header("Authorization", format!("Bearer {api_key}"))
                .header("X-API-Key", api_key)
                .send()
                .await
            {
                Ok(response) => response,
                Err(error) if error.is_timeout() && attempt < MAX_TIMEOUT_RETRIES => {
                    sleep(timeout_retry_delay(attempt)).await;
                    attempt += 1;
                    continue;
                }
                Err(error) => return Err(error.into()),
            };

            if response.status() == StatusCode::TOO_MANY_REQUESTS
                && attempt < MAX_RATE_LIMIT_RETRIES
            {
                sleep(retry_delay(&response, attempt)).await;
                attempt += 1;
                continue;
            }

            let parsed = parse_json_response(response).await?;
            self.write_cached_json(&cache_key, &parsed);
            return Ok(parsed);
        }
    }

    async fn fetch_snapshots_json(
        &self,
        target: MarketTarget,
        market_id: &str,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        offset: usize,
    ) -> Result<Value> {
        let mut attempt = 0_u32;

        loop {
            let url = format!(
                "{}/v2/markets/{market_id}/snapshots?coin={}",
                self.base_url.trim_end_matches('/'),
                coin_param(target),
            );
            let cache_key = format!(
                "snapshots:{url}:start={}:end={}:limit={}:offset={offset}:orderbook={}",
                start_time.to_rfc3339(),
                end_time.to_rfc3339(),
                self.snapshot_page_limit,
                self.include_orderbook
            );
            if let Some(cached) = self.read_cached_json(&cache_key, None) {
                return Ok(cached);
            }
            let api_key = self.api_key_or_err()?;
            let response = match self
                .http
                .get(url)
                .header("Authorization", format!("Bearer {api_key}"))
                .header("X-API-Key", api_key)
                .query(&[
                    ("start_time", start_time.to_rfc3339()),
                    ("end_time", end_time.to_rfc3339()),
                    ("limit", self.snapshot_page_limit.to_string()),
                    ("offset", offset.to_string()),
                    ("include_orderbook", self.include_orderbook.to_string()),
                ])
                .send()
                .await
            {
                Ok(response) => response,
                Err(error) if error.is_timeout() && attempt < MAX_TIMEOUT_RETRIES => {
                    sleep(timeout_retry_delay(attempt)).await;
                    attempt += 1;
                    continue;
                }
                Err(error) => return Err(error.into()),
            };

            if response.status() == StatusCode::TOO_MANY_REQUESTS
                && attempt < MAX_RATE_LIMIT_RETRIES
            {
                sleep(retry_delay(&response, attempt)).await;
                attempt += 1;
                continue;
            }

            let parsed = parse_json_response(response).await?;
            self.write_cached_json(&cache_key, &parsed);
            return Ok(parsed);
        }
    }

    fn read_cached_json(&self, key: &str, max_age: Option<Duration>) -> Option<Value> {
        let path = self.cache_path(key)?;
        if let Some(max_age) = max_age {
            let modified = fs::metadata(&path).ok()?.modified().ok()?;
            let age = SystemTime::now().duration_since(modified).ok()?;
            if age > max_age {
                return None;
            }
        }

        let bytes = fs::read(path).ok()?;
        serde_json::from_slice::<Value>(&bytes).ok()
    }

    fn write_cached_json(&self, key: &str, value: &Value) {
        let Some(path) = self.cache_path(key) else {
            return;
        };
        let Some(parent) = path.parent() else {
            return;
        };
        if fs::create_dir_all(parent).is_err() {
            return;
        }
        let Ok(bytes) = serde_json::to_vec(value) else {
            return;
        };
        let nonce = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_or(0, |duration| duration.as_nanos());
        let tmp_path = path.with_extension(format!("json.tmp-{}-{nonce}", std::process::id()));

        if fs::write(&tmp_path, bytes).is_err() {
            let _ = fs::remove_file(&tmp_path);
            return;
        }

        #[cfg(windows)]
        if path.exists() {
            let _ = fs::remove_file(&path);
        }

        if fs::rename(&tmp_path, &path).is_err() {
            let _ = fs::remove_file(&tmp_path);
        }
    }

    fn cache_path(&self, key: &str) -> Option<PathBuf> {
        let cache_dir = self.cache_dir.as_ref()?;
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        Some(cache_dir.join(format!("{:016x}.json", hasher.finish())))
    }

    fn has_api_key(&self) -> bool {
        !self.api_key.is_empty()
    }

    fn api_key_or_err(&self) -> Result<&str> {
        if self.api_key.is_empty() {
            return Err(AppError::MissingEnvVar(self.api_key_env.clone()));
        }
        Ok(&self.api_key)
    }
}

fn markets_url(base_url: &str, target: MarketTarget, limit: usize, offset: usize) -> String {
    format!(
        "{}/v2/markets?coin={}&market_type={}&resolved=true&limit={}&offset={offset}",
        base_url.trim_end_matches('/'),
        coin_param(target),
        market_type_param(target),
        limit.min(MARKET_PAGE_LIMIT),
    )
}

fn market_prepare_concurrency(total_markets: usize) -> usize {
    total_markets.clamp(
        1,
        env_usize_or_default(
            MARKET_PREPARE_CONCURRENCY_ENV,
            MARKET_PREPARE_CONCURRENCY,
            MARKET_PREPARE_CONCURRENCY_MAX,
        ),
    )
}

fn target_prepare_concurrency(total_targets: usize) -> usize {
    total_targets.clamp(
        1,
        env_usize_or_default(
            TARGET_PREPARE_CONCURRENCY_ENV,
            TARGET_PREPARE_CONCURRENCY,
            TARGET_PREPARE_CONCURRENCY_MAX,
        ),
    )
}

fn env_usize_or_default(env_name: &str, default_value: usize, max_value: usize) -> usize {
    env::var(env_name)
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .map_or(default_value, |value| value.min(max_value))
}

async fn parse_json_response(response: Response) -> Result<Value> {
    let status = response.status();
    if status.is_success() {
        return response.json::<Value>().await.map_err(AppError::from);
    }

    let body = response.text().await.unwrap_or_default();
    Err(AppError::HttpStatus { code: status, body })
}

fn retry_delay(response: &Response, attempt: u32) -> Duration {
    let header_delay = response
        .headers()
        .get("Retry-After")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(DEFAULT_RETRY_AFTER_SECS)
        .min(MAX_RETRY_AFTER_SECS);
    let backoff_multiplier = 2_u64.saturating_pow(attempt);
    Duration::from_secs(
        header_delay
            .saturating_mul(backoff_multiplier)
            .min(MAX_RETRY_AFTER_SECS),
    )
}

fn timeout_retry_delay(attempt: u32) -> Duration {
    let backoff_multiplier = 2_u64.saturating_pow(attempt);
    Duration::from_secs(
        DEFAULT_TIMEOUT_RETRY_SECS
            .saturating_mul(backoff_multiplier)
            .min(MAX_RETRY_AFTER_SECS),
    )
}

#[derive(Debug, Clone)]
struct PolyBacktestMarket {
    market_id: String,
    slug: String,
    question: String,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
    liquidity_usdc: Decimal,
}

impl PolyBacktestMarket {
    fn from_value(value: &Value) -> Result<Option<Self>> {
        let Some(slug) = extract_string(value, &["slug"]) else {
            return Ok(None);
        };
        if MarketTarget::from_slug(&slug).is_none() {
            return Ok(None);
        }

        let market_id = extract_string(value, &["market_id", "id"]).ok_or_else(|| {
            AppError::InvalidMarket("PolyBackTest market_id отсутствует".to_owned())
        })?;
        let start_time =
            extract_datetime(value, &["start_time", "startTime"]).ok_or_else(|| {
                AppError::InvalidMarket("PolyBackTest start_time отсутствует".to_owned())
            })?;
        let end_time = extract_datetime(value, &["end_time", "endTime"]).ok_or_else(|| {
            AppError::InvalidMarket("PolyBackTest end_time отсутствует".to_owned())
        })?;

        Ok(Some(Self {
            market_id,
            slug: slug.clone(),
            question: extract_string(value, &["question", "title", "event_title"]).unwrap_or(slug),
            start_time,
            end_time,
            liquidity_usdc: extract_decimal(
                value,
                &["final_liquidity", "liquidity", "current_liquidity"],
            )
            .unwrap_or(Decimal::ZERO),
        }))
    }

    fn to_binary_market(&self) -> BinaryMarket {
        BinaryMarket {
            condition_id: self.market_id.clone(),
            slug: self.slug.clone(),
            question: self.question.clone(),
            outcome_a_label: "Up".to_owned(),
            outcome_a_token_id: format!("{}:up", self.market_id),
            outcome_b_label: "Down".to_owned(),
            outcome_b_token_id: format!("{}:down", self.market_id),
            end_date: Some(self.end_time),
            liquidity_usdc: self.liquidity_usdc,
            target_price: None,
            target_price_source: None,
            final_reference_price: None,
        }
    }
}

#[derive(Debug, Clone)]
struct PolyBacktestSnapshot {
    timestamp: DateTime<Utc>,
    up_ask_price: Decimal,
    up_ask_size: Decimal,
    down_ask_price: Decimal,
    down_ask_size: Decimal,
}

impl PolyBacktestSnapshot {
    fn from_value(value: &Value) -> Option<Self> {
        let timestamp = extract_datetime(
            value,
            &["timestamp", "ts", "created_at", "snapshot_time", "time"],
        )?;

        let up_orderbook = extract_best_ask(
            value,
            &[
                &["orderbook_up", "asks"],
                &["up_orderbook", "asks"],
                &["orderbook", "up", "asks"],
                &["up", "asks"],
            ],
        );
        let down_orderbook = extract_best_ask(
            value,
            &[
                &["orderbook_down", "asks"],
                &["down_orderbook", "asks"],
                &["orderbook", "down", "asks"],
                &["down", "asks"],
            ],
        );

        let up_ask_price = up_orderbook.map(|(price, _size)| price).or_else(|| {
            extract_decimal(
                value,
                &[
                    "up_ask",
                    "best_ask_up",
                    "ask_up",
                    "up_best_ask",
                    "up_price",
                    "price_up",
                    "mid_price_up",
                    "up_mid_price",
                ],
            )
        });
        let down_ask_price = down_orderbook.map(|(price, _size)| price).or_else(|| {
            extract_decimal(
                value,
                &[
                    "down_ask",
                    "best_ask_down",
                    "ask_down",
                    "down_best_ask",
                    "down_price",
                    "price_down",
                    "mid_price_down",
                    "down_mid_price",
                ],
            )
        });

        let (Some(up_ask_price), Some(down_ask_price)) = (up_ask_price, down_ask_price) else {
            return None;
        };

        Some(Self {
            timestamp,
            up_ask_price,
            up_ask_size: up_orderbook.map_or_else(
                || Decimal::from(FALLBACK_ORDERBOOK_SIZE),
                |(_price, size)| size,
            ),
            down_ask_price,
            down_ask_size: down_orderbook.map_or_else(
                || Decimal::from(FALLBACK_ORDERBOOK_SIZE),
                |(_price, size)| size,
            ),
        })
    }

    fn to_order_books(
        &self,
        market: &BinaryMarket,
        default_size: Decimal,
    ) -> std::collections::HashMap<String, OrderBook> {
        std::collections::HashMap::from([
            (
                market.outcome_a_token_id.clone(),
                OrderBook {
                    asset_id: market.outcome_a_token_id.clone(),
                    bids: Vec::new(),
                    asks: vec![BookLevel {
                        price: self.up_ask_price,
                        size: self.up_ask_size.max(default_size),
                    }],
                    min_order_size: None,
                    tick_size: None,
                },
            ),
            (
                market.outcome_b_token_id.clone(),
                OrderBook {
                    asset_id: market.outcome_b_token_id.clone(),
                    bids: Vec::new(),
                    asks: vec![BookLevel {
                        price: self.down_ask_price,
                        size: self.down_ask_size.max(default_size),
                    }],
                    min_order_size: None,
                    tick_size: None,
                },
            ),
        ])
    }
}

fn signal_from_opportunity(
    target: MarketTarget,
    opportunity: Opportunity,
    actual_outcome: WindowDirection,
    scalp_exit: Option<ScalpExitReport>,
) -> BacktestSignal {
    let realized_payout = match opportunity.kind {
        OpportunityKind::BundleArbitrage => opportunity.tradable_shares,
        OpportunityKind::DirectionalMomentum
        | OpportunityKind::TargetStateV1
        | OpportunityKind::BonereaperStateV1
        | OpportunityKind::BonereaperStateV2
        | OpportunityKind::BonereaperStateGuarded
        | OpportunityKind::CodexSentinelV1
        | OpportunityKind::CodexScalpProbeV1
        | OpportunityKind::MicroBreakout => {
            if outcome_label_matches_direction(&opportunity.primary_outcome_label, actual_outcome) {
                opportunity.tradable_shares
            } else {
                Decimal::ZERO
            }
        }
        OpportunityKind::DirectionalMomentumHedged => {
            let primary = if outcome_label_matches_direction(
                &opportunity.primary_outcome_label,
                actual_outcome,
            ) {
                opportunity.tradable_shares
            } else {
                Decimal::ZERO
            };
            let hedge = if opportunity
                .hedge_outcome_label
                .as_deref()
                .is_some_and(|label| outcome_label_matches_direction(label, actual_outcome))
            {
                opportunity.hedge_shares
            } else {
                Decimal::ZERO
            };
            primary + hedge
        }
    };

    let settlement_realized_profit = (realized_payout - opportunity.required_usdc).round_dp(6);
    let realized_profit = scalp_exit
        .as_ref()
        .map_or(settlement_realized_profit, |exit| exit.realized_profit);

    BacktestSignal {
        target,
        slug: opportunity.slug,
        question: opportunity.question,
        kind: opportunity.kind,
        seconds_left: opportunity.seconds_left,
        primary_outcome_label: opportunity.primary_outcome_label,
        primary_outcome_ask_price: opportunity.primary_outcome_ask_price,
        spot_move_bps: opportunity.spot_move_bps,
        spot_move_1s_bps: opportunity.spot_move_1s_bps,
        spot_move_5s_bps: opportunity.spot_move_5s_bps,
        spot_move_15s_bps: opportunity.spot_move_15s_bps,
        micro_acceleration_bps: opportunity.micro_acceleration_bps,
        target_gap_bps: opportunity.target_gap_bps,
        signal_strength_bps: opportunity.signal_strength_bps,
        aligned_trade_flow_bps: opportunity.aligned_trade_flow_bps,
        signal_tier: opportunity.signal_tier,
        target_cross_label: opportunity.target_cross_label,
        bundle_cost: opportunity.bundle_cost,
        net_bundle_cost: opportunity.net_bundle_cost,
        edge_per_share: opportunity.edge_per_share,
        edge_bps: opportunity.edge_bps,
        tradable_shares: opportunity.tradable_shares,
        required_usdc: opportunity.required_usdc,
        expected_payout: opportunity.expected_payout,
        expected_profit: opportunity.expected_profit,
        realized_profit,
        scalp_exit,
        actual_outcome,
        dominant_outcome: opportunity.dominant_outcome,
        note: opportunity.note,
    }
}

fn polybacktest_signal_was_successful(signal: &BacktestSignal) -> bool {
    if signal.scalp_exit.is_some() {
        return signal.realized_profit >= Decimal::ZERO;
    }

    outcome_label_matches_direction(&signal.primary_outcome_label, signal.actual_outcome)
}

fn scalp_exit_for_opportunity(
    opportunity: &Opportunity,
    snapshots: &[PolyBacktestSnapshot],
    entry_time: DateTime<Utc>,
    fee_bps: u32,
) -> Option<ScalpExitReport> {
    if matches!(
        opportunity.kind,
        OpportunityKind::BundleArbitrage | OpportunityKind::DirectionalMomentumHedged
    ) {
        return None;
    }
    let side = PaperOutcomeSide::from_label(&opportunity.primary_outcome_label);
    if side == PaperOutcomeSide::Unknown || snapshots.is_empty() {
        return None;
    }

    let entry_price = opportunity.primary_outcome_ask_price;
    let take_profit_price = (entry_price + scalp_take_profit_delta()).min(Decimal::ONE);
    let stop_loss_price = (entry_price - scalp_stop_loss_delta()).max(Decimal::ZERO);
    let mut max_favorable_price = Decimal::ZERO;
    let mut max_adverse_price = Decimal::ONE;
    let mut fallback_exit = None;

    for snapshot in snapshots {
        let hold_secs = (snapshot.timestamp.timestamp() - entry_time.timestamp()).max(0);
        let exit_price = implied_exit_bid(snapshot, side);
        max_favorable_price = max_favorable_price.max(exit_price);
        max_adverse_price = max_adverse_price.min(exit_price);

        if hold_secs == 0 {
            continue;
        }

        fallback_exit = Some((hold_secs, exit_price, "last_snapshot"));

        if exit_price >= take_profit_price {
            return Some(build_scalp_exit_report(
                "take_profit",
                hold_secs,
                exit_price,
                opportunity,
                fee_bps,
                max_favorable_price,
                max_adverse_price,
            ));
        }
        if exit_price <= stop_loss_price {
            return Some(build_scalp_exit_report(
                "stop_loss",
                hold_secs,
                exit_price,
                opportunity,
                fee_bps,
                max_favorable_price,
                max_adverse_price,
            ));
        }
        if hold_secs >= SCALP_TIME_STOP_SECS {
            return Some(build_scalp_exit_report(
                "time_stop",
                hold_secs,
                exit_price,
                opportunity,
                fee_bps,
                max_favorable_price,
                max_adverse_price,
            ));
        }
    }

    fallback_exit.map(|(hold_secs, exit_price, reason)| {
        build_scalp_exit_report(
            reason,
            hold_secs,
            exit_price,
            opportunity,
            fee_bps,
            max_favorable_price,
            max_adverse_price,
        )
    })
}

fn build_scalp_exit_report(
    reason: &str,
    hold_secs: i64,
    exit_price: Decimal,
    opportunity: &Opportunity,
    fee_bps: u32,
    max_favorable_price: Decimal,
    max_adverse_price: Decimal,
) -> ScalpExitReport {
    let gross_payout = (opportunity.tradable_shares * exit_price).round_dp(6);
    let entry_fee = cost_from_bps(opportunity.required_usdc, fee_bps);
    let exit_fee = cost_from_bps(gross_payout, fee_bps);
    let realized_profit =
        (gross_payout - opportunity.required_usdc - entry_fee - exit_fee).round_dp(6);

    ScalpExitReport {
        exit_reason: reason.to_owned(),
        hold_secs,
        exit_price: exit_price.round_dp(6),
        gross_payout,
        realized_profit,
        max_favorable_price: max_favorable_price.round_dp(6),
        max_adverse_price: max_adverse_price.round_dp(6),
    }
}

fn implied_exit_bid(snapshot: &PolyBacktestSnapshot, side: PaperOutcomeSide) -> Decimal {
    match side {
        // PolyBackTest snapshots currently expose asks. For binary outcomes, the opposite ask gives
        // a conservative executable close proxy: sell Up near 1 - Down ask, and vice versa.
        PaperOutcomeSide::Up => {
            (Decimal::ONE - snapshot.down_ask_price).clamp(Decimal::ZERO, Decimal::ONE)
        }
        PaperOutcomeSide::Down => {
            (Decimal::ONE - snapshot.up_ask_price).clamp(Decimal::ZERO, Decimal::ONE)
        }
        PaperOutcomeSide::Unknown => Decimal::ZERO,
    }
}

fn scalp_take_profit_delta() -> Decimal {
    Decimal::new(8, 2)
}

fn scalp_stop_loss_delta() -> Decimal {
    Decimal::new(5, 2)
}

fn cost_from_bps(notional_usdc: Decimal, bps: u32) -> Decimal {
    if bps == 0 || notional_usdc <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    (notional_usdc * Decimal::from(bps) / Decimal::from(10_000_u32)).round_dp(6)
}

fn polybacktest_v4_block_reason(
    overlay: &V4InventoryConfig,
    state: &PolyBacktestV4InventoryState,
    opportunity: &Opportunity,
) -> Option<String> {
    if !overlay.enabled || !is_polybacktest_v4_inventory_kind(opportunity.kind) {
        return None;
    }

    let post_fill_spent =
        (state.spent_for_slug(&opportunity.slug) + opportunity.required_usdc).round_dp(6);
    let post_fill_entries = state.entries_for_slug(&opportunity.slug).saturating_add(1);
    let (current_up, current_down) = state.side_shares_for_slug(&opportunity.slug);
    let (addition_up, addition_down) = opportunity_side_shares(opportunity);
    let post_up = (current_up + addition_up).round_dp(6);
    let post_down = (current_down + addition_down).round_dp(6);
    let post_gross = (post_up + post_down).round_dp(6);
    let post_delta = (post_up - post_down).abs().round_dp(6);

    if overlay.max_window_spent_usdc > Decimal::ZERO
        && post_fill_spent > overlay.max_window_spent_usdc
    {
        return Some(format!(
            "v4 window spent cap exceeded in polybacktest: post-fill {} > {} USDC",
            post_fill_spent.round_dp(4),
            overlay.max_window_spent_usdc.round_dp(4)
        ));
    }

    if overlay.max_entries_per_window > 0 && post_fill_entries > overlay.max_entries_per_window {
        return Some(format!(
            "v4 entry-count cap exceeded in polybacktest: post-fill {} > {} entries",
            post_fill_entries, overlay.max_entries_per_window
        ));
    }

    if overlay.max_gross_inventory_shares_per_window > Decimal::ZERO
        && post_gross > overlay.max_gross_inventory_shares_per_window
    {
        return Some(format!(
            "v4 gross inventory cap exceeded in polybacktest: post-fill {} > {} shares",
            post_gross.round_dp(4),
            overlay.max_gross_inventory_shares_per_window.round_dp(4)
        ));
    }

    if overlay.max_directional_delta_shares_per_window > Decimal::ZERO
        && post_delta > overlay.max_directional_delta_shares_per_window
    {
        return Some(format!(
            "v4 directional delta cap exceeded in polybacktest: post-fill {} > {} shares",
            post_delta.round_dp(4),
            overlay.max_directional_delta_shares_per_window.round_dp(4)
        ));
    }

    None
}

const fn is_polybacktest_v4_inventory_kind(kind: OpportunityKind) -> bool {
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
            | OpportunityKind::MicroBreakout
    )
}

fn opportunity_side_shares(opportunity: &Opportunity) -> (Decimal, Decimal) {
    let mut up_shares = Decimal::ZERO;
    let mut down_shares = Decimal::ZERO;
    add_label_shares(
        &mut up_shares,
        &mut down_shares,
        &opportunity.primary_outcome_label,
        opportunity.tradable_shares,
    );
    if let Some(hedge_label) = &opportunity.hedge_outcome_label {
        add_label_shares(
            &mut up_shares,
            &mut down_shares,
            hedge_label,
            opportunity.hedge_shares,
        );
    }
    (up_shares.round_dp(6), down_shares.round_dp(6))
}

fn add_label_shares(
    up_shares: &mut Decimal,
    down_shares: &mut Decimal,
    label: &str,
    shares: Decimal,
) {
    match PaperOutcomeSide::from_label(label) {
        PaperOutcomeSide::Up => *up_shares += shares,
        PaperOutcomeSide::Down => *down_shares += shares,
        PaperOutcomeSide::Unknown => {}
    }
}

fn near_miss_from_blocked_opportunity(
    target: MarketTarget,
    opportunity: &Opportunity,
    reason: String,
) -> BacktestNearMiss {
    BacktestNearMiss {
        target,
        slug: opportunity.slug.clone(),
        question: opportunity.question.clone(),
        kind: opportunity.kind,
        dominant_outcome: opportunity.dominant_outcome.clone(),
        primary_outcome_label: opportunity.primary_outcome_label.clone(),
        primary_outcome_ask_price: Some(opportunity.primary_outcome_ask_price),
        bundle_cost: Some(opportunity.bundle_cost),
        spot_move_bps: opportunity.spot_move_bps,
        seconds_left: opportunity.seconds_left,
        shortfall_bps: 0,
        shortfall_label: "v4_inventory".to_owned(),
        reason,
    }
}

fn near_miss_from_report(target: MarketTarget, near_miss: NearMiss) -> BacktestNearMiss {
    BacktestNearMiss {
        target,
        slug: near_miss.slug,
        question: near_miss.question,
        kind: near_miss.kind,
        dominant_outcome: near_miss.dominant_outcome,
        primary_outcome_label: near_miss.primary_outcome_label,
        primary_outcome_ask_price: near_miss.primary_outcome_ask_price,
        bundle_cost: near_miss.bundle_cost,
        spot_move_bps: near_miss.spot_move_bps,
        seconds_left: near_miss.seconds_left,
        shortfall_bps: near_miss.shortfall_bps,
        shortfall_label: near_miss.shortfall_label,
        reason: near_miss.reason,
    }
}

fn outcome_label_matches_direction(label: &str, actual_outcome: WindowDirection) -> bool {
    match actual_outcome {
        WindowDirection::Up => outcome_label_is_up(label),
        WindowDirection::Down => outcome_label_is_down(label),
        WindowDirection::Flat => outcome_label_is_flat(label),
    }
}

fn downsample_snapshots(mut snapshots: Vec<PolyBacktestSnapshot>) -> Vec<PolyBacktestSnapshot> {
    if snapshots.is_empty() {
        return snapshots;
    }

    snapshots.sort_by_key(|snapshot| snapshot.timestamp);
    snapshots.dedup_by_key(|snapshot| snapshot.timestamp);
    if snapshots.is_empty() {
        return snapshots;
    }

    let mut last_timestamp = None;
    let mut filtered = Vec::with_capacity(snapshots.len());
    for snapshot in snapshots {
        let current_timestamp = snapshot.timestamp.timestamp();
        let should_keep = match last_timestamp {
            Some(previous_timestamp) => {
                current_timestamp - previous_timestamp >= SNAPSHOT_MIN_GAP_SECS
            }
            None => true,
        };
        if should_keep {
            last_timestamp = Some(current_timestamp);
            filtered.push(snapshot);
        }
    }

    if filtered.len() <= SNAPSHOT_MAX_SAMPLES_PER_WINDOW {
        return filtered;
    }

    let step = filtered
        .len()
        .div_ceil(SNAPSHOT_MAX_SAMPLES_PER_WINDOW)
        .max(1);
    let mut sampled = filtered.iter().cloned().step_by(step).collect::<Vec<_>>();

    if let Some(last) = filtered.last()
        && sampled
            .last()
            .is_none_or(|snapshot| snapshot.timestamp != last.timestamp)
    {
        sampled.push(last.clone());
    }

    sampled.truncate(SNAPSHOT_MAX_SAMPLES_PER_WINDOW);
    sampled
}

fn coin_param(target: MarketTarget) -> &'static str {
    match target {
        MarketTarget::Btc5m | MarketTarget::Btc15m => "btc",
        MarketTarget::Eth5m | MarketTarget::Eth15m => "eth",
        MarketTarget::Sol5m => "sol",
        MarketTarget::Xrp5m => "xrp",
        MarketTarget::Bnb5m => "bnb",
    }
}

fn market_type_param(target: MarketTarget) -> &'static str {
    match target {
        MarketTarget::Btc5m
        | MarketTarget::Eth5m
        | MarketTarget::Sol5m
        | MarketTarget::Xrp5m
        | MarketTarget::Bnb5m => "5m",
        MarketTarget::Btc15m | MarketTarget::Eth15m => "15m",
    }
}

fn unique_targets(targets: &[MarketTarget]) -> Vec<MarketTarget> {
    let mut unique = Vec::with_capacity(targets.len());
    for target in targets {
        if !unique.contains(target) {
            unique.push(*target);
        }
    }
    unique
}

fn percentage_or_zero(numerator: usize, denominator: usize) -> Decimal {
    if denominator == 0 {
        Decimal::ZERO
    } else {
        (Decimal::from(numerator as u64) / Decimal::from(denominator as u64)
            * Decimal::from(100_u32))
        .round_dp(4)
    }
}

fn extract_string(value: &Value, fields: &[&str]) -> Option<String> {
    fields.iter().find_map(|field| match value.get(*field) {
        Some(Value::String(text)) if !text.trim().is_empty() => Some(text.trim().to_owned()),
        Some(inner) if inner.is_number() => Some(inner.to_string()),
        _ => None,
    })
}

fn extract_decimal(value: &Value, fields: &[&str]) -> Option<Decimal> {
    fields
        .iter()
        .find_map(|field| value.get(*field))
        .and_then(decimal_from_value)
}

fn extract_datetime(value: &Value, fields: &[&str]) -> Option<DateTime<Utc>> {
    for field in fields {
        let Some(raw) = value.get(*field) else {
            continue;
        };
        if let Some(parsed) = datetime_from_value(raw) {
            return Some(parsed);
        }
    }
    None
}

fn extract_best_ask(value: &Value, paths: &[&[&str]]) -> Option<(Decimal, Decimal)> {
    for path in paths {
        let Some(asks) = lookup_path(value, path).and_then(Value::as_array) else {
            continue;
        };
        let best = asks
            .iter()
            .filter_map(|level| {
                let price = extract_decimal(level, &["price", "p"])?;
                let size = extract_decimal(level, &["size", "s"]).unwrap_or(Decimal::ONE);
                Some((price, size))
            })
            .min_by(|left, right| left.0.cmp(&right.0));
        if best.is_some() {
            return best;
        }
    }
    None
}

fn lookup_path<'a>(value: &'a Value, path: &[&str]) -> Option<&'a Value> {
    let mut current = value;
    for segment in path {
        current = current.get(*segment)?;
    }
    Some(current)
}

fn decimal_from_value(value: &Value) -> Option<Decimal> {
    match value {
        Value::String(text) => text.parse::<Decimal>().ok(),
        Value::Number(number) => number.to_string().parse::<Decimal>().ok(),
        _ => None,
    }
}

fn datetime_from_value(value: &Value) -> Option<DateTime<Utc>> {
    match value {
        Value::String(text) => DateTime::parse_from_rfc3339(text)
            .ok()
            .map(|parsed| parsed.with_timezone(&Utc)),
        Value::Number(number) => number.as_i64().and_then(timestamp_to_datetime),
        _ => None,
    }
}

fn timestamp_to_datetime(timestamp: i64) -> Option<DateTime<Utc>> {
    if timestamp > 10_000_000_000 {
        DateTime::from_timestamp_millis(timestamp)
    } else {
        DateTime::from_timestamp(timestamp, 0)
    }
}

fn resolve_polybacktest_api_key(config: &PolyBacktestConfig) -> Result<String> {
    if let Some(value) = read_env_secret(&config.api_key_env) {
        return Ok(value);
    }

    if !config.prompt_for_api_key {
        return if config.cache_enabled {
            Ok(String::new())
        } else {
            Err(AppError::MissingEnvVar(config.api_key_env.clone()))
        };
    }

    if !io::stdin().is_terminal() {
        return if config.cache_enabled {
            Ok(String::new())
        } else {
            Err(AppError::InteractiveInputUnavailable(
                config.api_key_env.clone(),
            ))
        };
    }

    prompt_line(
        "API key PolyBackTest (можно вставить прямо сюда): ",
        &config.api_key_env,
    )
}

fn prompt_line(prompt: &str, env_name: &str) -> Result<String> {
    let mut stdout = io::stdout();
    stdout.write_all(prompt.as_bytes())?;
    stdout.flush()?;

    let mut input = String::new();
    let bytes_read = io::stdin().read_line(&mut input)?;
    if bytes_read == 0 {
        return Err(AppError::InteractiveInputUnavailable(env_name.to_owned()));
    }
    let trimmed = input.trim().to_owned();
    if trimmed.is_empty() {
        return Err(AppError::MissingEnvVar(env_name.to_owned()));
    }

    Ok(trimmed)
}

fn read_env_secret(env_name: &str) -> Option<String> {
    env::var(env_name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::TargetPriceSource;

    fn decimal(value: &str) -> Decimal {
        value.parse().expect("decimal literal should parse")
    }

    #[test]
    fn markets_url_uses_offset_and_caps_page_limit() {
        let url = markets_url(
            "https://api.polybacktest.com/",
            MarketTarget::Btc5m,
            500,
            200,
        );

        assert_eq!(
            url,
            "https://api.polybacktest.com/v2/markets?coin=btc&market_type=5m&resolved=true&limit=100&offset=200"
        );
    }

    #[test]
    fn scalp_exit_model_takes_profit_on_implied_bid_jump() {
        let mut opportunity = test_opportunity("btc-updown-5m-scalp-tp", "10", "Up");
        opportunity.primary_outcome_ask_price = decimal("0.40");
        opportunity.required_usdc = decimal("4.00");
        opportunity.tradable_shares = decimal("10");
        let entry_time = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let snapshots = vec![
            test_snapshot(entry_time, "0.40", "0.60"),
            test_snapshot(entry_time + ChronoDuration::seconds(10), "0.48", "0.52"),
        ];

        let exit = scalp_exit_for_opportunity(&opportunity, &snapshots, entry_time, 0)
            .expect("scalp exit should be simulated");

        assert_eq!(exit.exit_reason, "take_profit");
        assert_eq!(exit.hold_secs, 10);
        assert_eq!(exit.exit_price, decimal("0.48"));
        assert_eq!(exit.realized_profit, decimal("0.800000"));
    }

    #[test]
    fn scalp_exit_model_stops_loss_when_implied_bid_breaks() {
        let mut opportunity = test_opportunity("btc-updown-5m-scalp-sl", "10", "Up");
        opportunity.primary_outcome_ask_price = decimal("0.50");
        opportunity.required_usdc = decimal("5.00");
        opportunity.tradable_shares = decimal("10");
        let entry_time = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let snapshots = vec![
            test_snapshot(entry_time, "0.50", "0.50"),
            test_snapshot(entry_time + ChronoDuration::seconds(4), "0.44", "0.56"),
        ];

        let exit = scalp_exit_for_opportunity(&opportunity, &snapshots, entry_time, 0)
            .expect("scalp exit should be simulated");

        assert_eq!(exit.exit_reason, "stop_loss");
        assert_eq!(exit.hold_secs, 4);
        assert_eq!(exit.exit_price, decimal("0.44"));
        assert_eq!(exit.realized_profit, decimal("-0.600000"));
    }

    #[test]
    fn polybacktest_signal_uses_scalp_realized_profit_when_exit_is_simulated() {
        let mut opportunity = test_opportunity("btc-updown-5m-scalp-effective-pnl", "10", "Up");
        opportunity.required_usdc = decimal("4.00");
        opportunity.tradable_shares = decimal("10");
        let scalp_exit = ScalpExitReport {
            exit_reason: "take_profit".to_owned(),
            hold_secs: 5,
            exit_price: decimal("0.48"),
            gross_payout: decimal("4.80"),
            realized_profit: decimal("0.800000"),
            max_favorable_price: decimal("0.48"),
            max_adverse_price: decimal("0.40"),
        };

        let signal = signal_from_opportunity(
            MarketTarget::Btc5m,
            opportunity,
            WindowDirection::Down,
            Some(scalp_exit),
        );

        assert_eq!(signal.realized_profit, decimal("0.800000"));
        assert!(polybacktest_signal_was_successful(&signal));
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
            net_bundle_cost: decimal("0.52"),
            edge_per_share: decimal("0.03"),
            edge_bps: 30,
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
            seconds_left: 240,
            note: "test".to_owned(),
        }
    }

    fn test_snapshot(
        timestamp: DateTime<Utc>,
        up_ask_price: &str,
        down_ask_price: &str,
    ) -> PolyBacktestSnapshot {
        PolyBacktestSnapshot {
            timestamp,
            up_ask_price: decimal(up_ask_price),
            up_ask_size: decimal("1000"),
            down_ask_price: decimal(down_ask_price),
            down_ask_size: decimal("1000"),
        }
    }

    #[test]
    fn polybacktest_v4_overlay_blocks_entry_count_like_runner() {
        let mut overlay = test_v4_overlay();
        overlay.max_entries_per_window = 2;
        overlay.max_gross_inventory_shares_per_window = decimal("100");
        overlay.max_directional_delta_shares_per_window = decimal("100");
        let mut state = PolyBacktestV4InventoryState::default();
        let opportunity = test_opportunity("btc-updown-5m-test", "3", "Up");

        state.observe_opened(&opportunity);
        state.observe_opened(&opportunity);
        let reason = polybacktest_v4_block_reason(&overlay, &state, &opportunity)
            .expect("third entry should be blocked");

        assert!(reason.contains("entry-count cap exceeded"));
    }

    #[test]
    fn polybacktest_v4_overlay_blocks_guarded_bonereaper_gross_cap() {
        let overlay = test_v4_overlay();
        let mut state = PolyBacktestV4InventoryState::default();
        let mut first = test_opportunity("btc-updown-5m-guarded", "8", "Up");
        first.kind = OpportunityKind::BonereaperStateGuarded;
        let mut second = test_opportunity("btc-updown-5m-guarded", "5", "Up");
        second.kind = OpportunityKind::BonereaperStateGuarded;

        state.observe_opened(&first);
        let reason = polybacktest_v4_block_reason(&overlay, &state, &second)
            .expect("guarded bonereaper should respect gross cap in polybacktest");

        assert!(reason.contains("gross inventory cap exceeded"));
    }
}
