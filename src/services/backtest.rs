//! Approximate historical backtesting for supported fast Polymarket markets.

use std::collections::HashMap;

use chrono::Utc;
use rust_decimal::Decimal;

use crate::config::AppConfig;
use crate::error::Result;
use crate::models::{
    BinaryMarket, BookLevel, MarketTarget, Opportunity, OpportunityKind, OrderBook,
};

use super::binance::{BinanceClient, BtcFiveMinuteContext, WindowDirection};
use super::labels::{outcome_label_is_down, outcome_label_is_flat, outcome_label_is_up};
use super::market_data::{MarketDataClient, PriceHistoryPoint, TradeFlowWindow};
use super::strategy::{BundleArbitrageStrategy, NearMiss};

/// Aggregated backtest output across all configured market targets.
#[derive(Debug, Clone)]
pub struct BacktestReport {
    pub entry_minutes: u32,
    pub summaries: Vec<BacktestTargetSummary>,
    pub signals: Vec<BacktestSignal>,
    pub near_misses: Vec<BacktestNearMiss>,
}

/// Summary for one market family.
#[derive(Debug, Clone)]
pub struct BacktestTargetSummary {
    pub target: MarketTarget,
    pub sampled_windows: usize,
    pub signal_count: usize,
    pub near_miss_count: usize,
    pub resolved_signal_count: usize,
    pub realized_profit: Decimal,
    pub expected_profit: Decimal,
    pub signal_accuracy_pct: Decimal,
}

/// One historical signal emitted by the strategy.
#[derive(Debug, Clone)]
pub struct BacktestSignal {
    pub target: MarketTarget,
    pub slug: String,
    pub question: String,
    pub kind: OpportunityKind,
    pub seconds_left: i64,
    pub primary_outcome_label: String,
    pub primary_outcome_ask_price: Decimal,
    pub spot_move_bps: Decimal,
    pub spot_move_1s_bps: Decimal,
    pub spot_move_5s_bps: Decimal,
    pub spot_move_15s_bps: Decimal,
    pub micro_acceleration_bps: Decimal,
    pub target_gap_bps: Decimal,
    pub signal_strength_bps: Decimal,
    pub aligned_trade_flow_bps: Decimal,
    pub signal_tier: String,
    pub target_cross_label: String,
    pub bundle_cost: Decimal,
    pub net_bundle_cost: Decimal,
    pub edge_per_share: Decimal,
    pub edge_bps: u32,
    pub tradable_shares: Decimal,
    pub required_usdc: Decimal,
    pub expected_payout: Decimal,
    pub expected_profit: Decimal,
    pub realized_profit: Decimal,
    pub scalp_exit: Option<ScalpExitReport>,
    pub actual_outcome: WindowDirection,
    pub dominant_outcome: String,
    pub note: String,
}

/// Simulated early-exit result for a fast scalp instead of holding until resolution.
#[derive(Debug, Clone)]
pub struct ScalpExitReport {
    pub exit_reason: String,
    pub hold_secs: i64,
    pub exit_price: Decimal,
    pub gross_payout: Decimal,
    pub realized_profit: Decimal,
    pub max_favorable_price: Decimal,
    pub max_adverse_price: Decimal,
}

/// One historical market that nearly emitted a signal.
#[derive(Debug, Clone)]
pub struct BacktestNearMiss {
    pub target: MarketTarget,
    pub slug: String,
    pub question: String,
    pub kind: OpportunityKind,
    pub dominant_outcome: String,
    pub primary_outcome_label: String,
    pub primary_outcome_ask_price: Option<Decimal>,
    pub bundle_cost: Option<Decimal>,
    pub spot_move_bps: Decimal,
    pub seconds_left: i64,
    pub shortfall_bps: u32,
    pub shortfall_label: String,
    pub reason: String,
}

/// Backtest runner built on top of the live strategy implementation.
#[derive(Debug)]
pub struct BacktestRunner<'a> {
    config: &'a AppConfig,
    data_client: &'a MarketDataClient,
    binance_client: &'a BinanceClient,
    strategy: &'a BundleArbitrageStrategy,
}

impl<'a> BacktestRunner<'a> {
    /// Create a new backtest runner.
    #[must_use]
    pub const fn new(
        config: &'a AppConfig,
        data_client: &'a MarketDataClient,
        binance_client: &'a BinanceClient,
        strategy: &'a BundleArbitrageStrategy,
    ) -> Self {
        Self {
            config,
            data_client,
            binance_client,
            strategy,
        }
    }

    /// Execute an approximate historical backtest for the configured targets.
    ///
    /// # Errors
    ///
    /// Returns an error if historical market, price-history, or Binance data requests fail.
    pub async fn run(
        &self,
        windows_per_target: usize,
        entry_minutes: u32,
    ) -> Result<BacktestReport> {
        let entry_offset_secs = i64::from(entry_minutes).saturating_mul(60);
        let targets = dedupe_targets(&self.config.strategy.market_targets);
        let mut summaries = Vec::with_capacity(targets.len());
        let mut signals = Vec::new();
        let mut near_misses = Vec::new();

        for target in targets {
            let target_report = self
                .run_for_target(target, windows_per_target, entry_offset_secs)
                .await?;
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

        Ok(BacktestReport {
            entry_minutes,
            summaries,
            signals,
            near_misses,
        })
    }

    #[allow(clippy::too_many_lines)]
    async fn run_for_target(
        &self,
        target: MarketTarget,
        windows_per_target: usize,
        entry_offset_secs: i64,
    ) -> Result<TargetBacktestResult> {
        let mut sampled_windows = 0_usize;
        let mut signals = Vec::new();
        let mut near_misses = Vec::new();

        for slug in historical_window_slugs(target, windows_per_target) {
            let Some(market) = self
                .data_client
                .fetch_historical_market_by_slug(&slug)
                .await?
            else {
                continue;
            };
            let Some(start_ts) = market.window_start_ts() else {
                continue;
            };
            let window_secs = target.window_secs();
            if entry_offset_secs >= window_secs {
                continue;
            }

            let Some(context) = self
                .binance_client
                .historical_context_from_slug(&slug, entry_offset_secs)
                .await?
            else {
                continue;
            };
            let Some(resolution) = self.binance_client.resolution_from_slug(&slug).await? else {
                continue;
            };
            let Some(books) = self
                .synthetic_books_at_entry(&market, start_ts, entry_offset_secs)
                .await?
            else {
                continue;
            };

            sampled_windows += 1;

            let mut contexts = HashMap::<String, BtcFiveMinuteContext>::with_capacity(1);
            contexts.insert(slug.clone(), context);
            let trade_flows = self
                .data_client
                .fetch_trade_flow_summaries(&[TradeFlowWindow {
                    slug: market.slug.clone(),
                    condition_id: market.condition_id.clone(),
                    start_ts_ms: start_ts.saturating_mul(1000),
                    end_ts_ms: (start_ts + entry_offset_secs).saturating_mul(1000),
                }])
                .await?;
            let opportunities = self.strategy.find_opportunities(
                std::slice::from_ref(&market),
                &books,
                &HashMap::new(),
                &contexts,
                &trade_flows,
            );

            if let Some(opportunity) = opportunities.into_iter().next() {
                signals.push(build_signal_report(
                    target,
                    opportunity,
                    resolution.actual_outcome,
                ));
            } else if let Some(near_miss) = self
                .strategy
                .find_near_misses(
                    std::slice::from_ref(&market),
                    &books,
                    &HashMap::new(),
                    &contexts,
                    &trade_flows,
                    1,
                )
                .into_iter()
                .next()
            {
                near_misses.push(build_near_miss_report(target, near_miss));
            }
        }

        let signal_count = signals.len();
        let near_miss_count = near_misses.len();
        let resolved_signal_count = signal_count;
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
            .filter(|signal| {
                outcome_label_matches_direction(&signal.dominant_outcome, signal.actual_outcome)
            })
            .count();

        Ok(TargetBacktestResult {
            summary: BacktestTargetSummary {
                target,
                sampled_windows,
                signal_count,
                near_miss_count,
                resolved_signal_count,
                realized_profit,
                expected_profit,
                signal_accuracy_pct: percentage_or_zero(accurate_signals, resolved_signal_count),
            },
            signals,
            near_misses,
        })
    }

    async fn synthetic_books_at_entry(
        &self,
        market: &BinaryMarket,
        start_ts: i64,
        entry_offset_secs: i64,
    ) -> Result<Option<HashMap<String, OrderBook>>> {
        let entry_ts_ms = (start_ts + entry_offset_secs) * 1000;
        let history_end_ms = (start_ts + market.window_secs().unwrap_or(300)) * 1000;

        let Some(up_token) = market.token_for_outcome("up") else {
            return Ok(None);
        };
        let Some(down_token) = market.token_for_outcome("down") else {
            return Ok(None);
        };

        let up_history = self
            .data_client
            .fetch_price_history(up_token, start_ts * 1000, history_end_ms, "all")
            .await?;
        let down_history = self
            .data_client
            .fetch_price_history(down_token, start_ts * 1000, history_end_ms, "all")
            .await?;

        let Some(up_price) = pick_entry_price(&up_history, entry_ts_ms) else {
            return Ok(None);
        };
        let Some(down_price) = pick_entry_price(&down_history, entry_ts_ms) else {
            return Ok(None);
        };

        let depth = self
            .config
            .strategy
            .min_top_of_book_shares
            .max(Decimal::from(250_u32));
        let books = HashMap::from([
            (
                up_token.to_owned(),
                synthetic_order_book(up_token, up_price, depth),
            ),
            (
                down_token.to_owned(),
                synthetic_order_book(down_token, down_price, depth),
            ),
        ]);

        Ok(Some(books))
    }
}

#[derive(Debug)]
struct TargetBacktestResult {
    summary: BacktestTargetSummary,
    signals: Vec<BacktestSignal>,
    near_misses: Vec<BacktestNearMiss>,
}

fn build_signal_report(
    target: MarketTarget,
    opportunity: Opportunity,
    actual_outcome: WindowDirection,
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
    let realized_profit = (realized_payout - opportunity.required_usdc).round_dp(6);

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
        scalp_exit: None,
        actual_outcome,
        dominant_outcome: opportunity.dominant_outcome,
        note: opportunity.note,
    }
}

fn build_near_miss_report(target: MarketTarget, near_miss: NearMiss) -> BacktestNearMiss {
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

fn synthetic_order_book(token_id: &str, ask_price: Decimal, size: Decimal) -> OrderBook {
    OrderBook {
        asset_id: token_id.to_owned(),
        bids: Vec::new(),
        asks: vec![BookLevel {
            price: ask_price,
            size,
        }],
        min_order_size: None,
        tick_size: None,
    }
}

fn pick_entry_price(history: &[PriceHistoryPoint], entry_ts_ms: i64) -> Option<Decimal> {
    history
        .iter()
        .find(|point| point.timestamp_ms >= entry_ts_ms)
        .or_else(|| {
            history
                .iter()
                .rev()
                .find(|point| point.timestamp_ms <= entry_ts_ms)
        })
        .map(|point| point.price)
}

fn historical_window_slugs(target: MarketTarget, windows_per_target: usize) -> Vec<String> {
    let now_ts = Utc::now().timestamp();
    let window_secs = target.window_secs();
    let current_window_start = now_ts - now_ts.rem_euclid(window_secs);
    let latest_completed_start = current_window_start - window_secs;

    (0..windows_per_target)
        .map(|index| {
            format!(
                "{}{}",
                target.slug_prefix(),
                latest_completed_start - i64::try_from(index).unwrap_or(0) * window_secs
            )
        })
        .collect()
}

fn dedupe_targets(targets: &[MarketTarget]) -> Vec<MarketTarget> {
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
