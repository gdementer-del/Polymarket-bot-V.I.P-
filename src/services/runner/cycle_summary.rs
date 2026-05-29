use std::collections::HashSet;

use chrono::Utc;
use rust_decimal::Decimal;

use crate::config::{AppConfig, RuntimeRegime};
use crate::models::{Opportunity, OrderBook, PaperState};

use super::super::journal::{
    PaperCycleCurrentMarketHealth, PaperCycleEntry, PaperCycleLatencyMetrics,
};
use super::super::strategy::NearMiss;
use super::{
    MarketSnapshot, RiskTracker, display_optional_decimal, explain_no_near_miss_runtime,
    paper_cost_model_from_config, summarize_worst_open_position, yes_no_ru,
};

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone)]
pub(super) struct RuntimeCurrentMarketHealth {
    missing_context: bool,
    missing_up_ask: bool,
    missing_down_ask: bool,
    missing_bundle_cost: bool,
    missing_directional_ask: bool,
}

impl RuntimeCurrentMarketHealth {
    #[allow(clippy::fn_params_excessive_bools)]
    pub(super) fn from_quote_state(
        missing_up_ask: bool,
        missing_down_ask: bool,
        missing_bundle_cost: bool,
        missing_directional_ask: bool,
    ) -> Self {
        Self {
            missing_context: false,
            missing_up_ask,
            missing_down_ask,
            missing_bundle_cost,
            missing_directional_ask,
        }
    }

    pub(super) fn missing_context() -> Self {
        Self {
            missing_context: true,
            missing_up_ask: false,
            missing_down_ask: false,
            missing_bundle_cost: false,
            missing_directional_ask: false,
        }
    }

    fn issues(&self) -> Vec<&'static str> {
        let mut issues = Vec::new();
        if self.missing_context {
            issues.push("current_market_context_unavailable");
        }
        if self.missing_up_ask {
            issues.push("missing_up_ask");
        }
        if self.missing_down_ask {
            issues.push("missing_down_ask");
        }
        if self.missing_bundle_cost {
            issues.push("missing_bundle_cost");
        }
        if self.missing_directional_ask {
            issues.push("missing_directional_ask");
        }
        issues
    }

    fn reason(&self) -> Option<String> {
        let issues = self.issues();
        (!issues.is_empty()).then(|| issues.join(","))
    }
}

#[derive(Debug, Clone)]
pub(super) struct RuntimeCurrentMarketStatus {
    pub(super) strategy_fit: bool,
    pub(super) health: RuntimeCurrentMarketHealth,
}

#[derive(Debug, Clone)]
pub(super) struct RuntimeCurrentMarketSummary {
    pub(super) target_label: String,
    pub(super) slug: String,
    pub(super) seconds_left: i64,
    pub(super) current_price: String,
    pub(super) current_price_source: String,
    pub(super) current_price_event_age_ms: Option<i64>,
    pub(super) current_price_received_age_ms: Option<i64>,
    pub(super) current_price_quote_points: Option<usize>,
    pub(super) exchange_book_age_ms: Option<i64>,
    pub(super) exchange_book_top_imbalance_bps: String,
    pub(super) exchange_book_depth_imbalance_bps: String,
    pub(super) exchange_book_microprice_bps: String,
    pub(super) exchange_book_spread_bps: String,
    pub(super) target_price: String,
    pub(super) target_price_source: String,
    pub(super) target_gap_bps: String,
    pub(super) micro_reference_price: String,
    pub(super) spot_move_bps: String,
    pub(super) spot_move_1s_bps: String,
    pub(super) spot_move_5s_bps: String,
    pub(super) spot_move_15s_bps: String,
    pub(super) micro_acceleration_bps: String,
    pub(super) up_ask: String,
    pub(super) down_ask: String,
    pub(super) bundle_cost: String,
    pub(super) dominant_outcome: String,
    pub(super) status: RuntimeCurrentMarketStatus,
}

#[derive(Debug, Clone)]
pub(super) struct RuntimeSnapshotSummary {
    pub(super) total_markets: usize,
    pub(super) live_markets: usize,
    pub(super) strategy_fit_count: usize,
    pub(super) current_market: Option<RuntimeCurrentMarketSummary>,
    pub(super) data_health_reason: Option<String>,
    pub(super) latency: RuntimeLatencyMetrics,
}

#[allow(clippy::struct_field_names)]
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct RuntimeLatencyMetrics {
    pub(super) trigger_event_to_snapshot_ms: Option<u64>,
    pub(super) trigger_received_to_snapshot_ms: Option<u64>,
    pub(super) exit_snapshot_ms: u64,
    pub(super) early_exit_eval_ms: u64,
    pub(super) runtime_snapshot_ms: u64,
    pub(super) analysis_ms: u64,
    pub(super) selection_ms: u64,
    pub(super) revalidation_ms: u64,
    pub(super) execution_ms: u64,
    pub(super) cycle_total_ms: u64,
}

impl From<RuntimeCurrentMarketHealth> for PaperCycleCurrentMarketHealth {
    fn from(value: RuntimeCurrentMarketHealth) -> Self {
        Self {
            missing_context: value.missing_context,
            missing_up_ask: value.missing_up_ask,
            missing_down_ask: value.missing_down_ask,
            missing_bundle_cost: value.missing_bundle_cost,
            missing_directional_ask: value.missing_directional_ask,
        }
    }
}

impl From<RuntimeLatencyMetrics> for PaperCycleLatencyMetrics {
    fn from(value: RuntimeLatencyMetrics) -> Self {
        Self {
            trigger_event_to_snapshot_ms: value.trigger_event_to_snapshot_ms,
            trigger_received_to_snapshot_ms: value.trigger_received_to_snapshot_ms,
            exit_snapshot_ms: value.exit_snapshot_ms,
            early_exit_eval_ms: value.early_exit_eval_ms,
            runtime_snapshot_ms: value.runtime_snapshot_ms,
            analysis_ms: value.analysis_ms,
            selection_ms: value.selection_ms,
            revalidation_ms: value.revalidation_ms,
            execution_ms: value.execution_ms,
            cycle_total_ms: value.cycle_total_ms,
        }
    }
}

pub(super) fn build_runtime_snapshot_summary(
    snapshot: &MarketSnapshot,
    opportunities: &[Opportunity],
    latency: RuntimeLatencyMetrics,
) -> RuntimeSnapshotSummary {
    let strategy_fit_slugs = opportunities
        .iter()
        .map(|opportunity| opportunity.slug.as_str())
        .collect::<HashSet<_>>();
    let current_market = snapshot.markets.iter().find_map(|market| {
        let context = snapshot.contexts.get(&market.slug)?;
        let up_ask = market
            .token_for_outcome("up")
            .and_then(|token_id| snapshot.books.get(token_id))
            .and_then(OrderBook::best_ask)
            .map(|level| level.price);
        let down_ask = market
            .token_for_outcome("down")
            .and_then(|token_id| snapshot.books.get(token_id))
            .and_then(OrderBook::best_ask)
            .map(|level| level.price);
        let bundle_cost = up_ask
            .zip(down_ask)
            .map(|(up_price, down_price)| (up_price + down_price).round_dp(6));
        let dominant_outcome = context.dominant_outcome.clone();
        let missing_directional_ask = if dominant_outcome == "Up" {
            up_ask.is_none()
        } else if dominant_outcome == "Down" {
            down_ask.is_none()
        } else {
            true
        };

        Some(RuntimeCurrentMarketSummary {
            target_label: market
                .target()
                .map_or_else(|| "n/a".to_owned(), |target| target.label().to_owned()),
            slug: market.slug.clone(),
            seconds_left: context.seconds_left,
            current_price: context.current_spot_price.round_dp(4).to_string(),
            current_price_source: context.current_spot_source.clone(),
            current_price_event_age_ms: context.current_spot_event_age_ms,
            current_price_received_age_ms: context.current_spot_received_age_ms,
            current_price_quote_points: context.current_spot_quote_points,
            exchange_book_age_ms: context.exchange_book_age_ms,
            exchange_book_top_imbalance_bps: context
                .exchange_book_top_imbalance_bps
                .round_dp(2)
                .to_string(),
            exchange_book_depth_imbalance_bps: context
                .exchange_book_depth_imbalance_bps
                .round_dp(2)
                .to_string(),
            exchange_book_microprice_bps: context
                .exchange_book_microprice_bps
                .round_dp(4)
                .to_string(),
            exchange_book_spread_bps: context.exchange_book_spread_bps.round_dp(4).to_string(),
            target_price: context.target_price.round_dp(4).to_string(),
            target_price_source: context.target_price_source.as_str().to_owned(),
            target_gap_bps: context.target_gap_bps.round_dp(2).to_string(),
            micro_reference_price: context.micro_reference_price.round_dp(4).to_string(),
            spot_move_bps: context.spot_move_bps.to_string(),
            spot_move_1s_bps: context.spot_move_1s_bps.to_string(),
            spot_move_5s_bps: context.spot_move_5s_bps.to_string(),
            spot_move_15s_bps: context.spot_move_15s_bps.to_string(),
            micro_acceleration_bps: context.micro_acceleration_bps.round_dp(2).to_string(),
            up_ask: display_optional_decimal(up_ask),
            down_ask: display_optional_decimal(down_ask),
            bundle_cost: display_optional_decimal(bundle_cost),
            dominant_outcome,
            status: RuntimeCurrentMarketStatus {
                strategy_fit: strategy_fit_slugs.contains(market.slug.as_str()),
                health: RuntimeCurrentMarketHealth::from_quote_state(
                    up_ask.is_none(),
                    down_ask.is_none(),
                    bundle_cost.is_none(),
                    missing_directional_ask,
                ),
            },
        })
    });

    let data_health_reason = if snapshot.markets.is_empty() {
        Some("no_markets_in_snapshot".to_owned())
    } else if snapshot.contexts.is_empty() {
        Some("binance_context_unavailable".to_owned())
    } else if let Some(current_market) = current_market.as_ref() {
        current_market.status.health.reason()
    } else {
        RuntimeCurrentMarketHealth::missing_context().reason()
    };

    RuntimeSnapshotSummary {
        total_markets: snapshot.markets.len(),
        live_markets: snapshot.contexts.len(),
        strategy_fit_count: strategy_fit_slugs.len(),
        current_market,
        data_health_reason,
        latency,
    }
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_lines)]
pub(super) fn build_paper_cycle_entry(
    config: &AppConfig,
    snapshot: &MarketSnapshot,
    runtime_summary: &RuntimeSnapshotSummary,
    opportunities: &[Opportunity],
    near_misses: &[NearMiss],
    selected_count: usize,
    executed_count: usize,
    paper_state: &PaperState,
    regime: RuntimeRegime,
    regime_reason: Option<&str>,
    risk_block_reason: Option<&str>,
    risk_tracker: &RiskTracker,
) -> PaperCycleEntry {
    let current_market = runtime_summary.current_market.as_ref();
    let worst_open_position = summarize_worst_open_position(
        paper_state,
        &snapshot.contexts,
        &snapshot.books,
        &config.run.early_exit,
        paper_cost_model_from_config(config),
    );
    let base_reason = if let Some(opportunity) = opportunities.first() {
        Some(format!(
            "signal {} | edge {} | spot {} | ask {} | usdc {}",
            opportunity.kind.as_str(),
            opportunity.edge_bps,
            opportunity.spot_move_bps.round_dp(2),
            opportunity.primary_outcome_ask_price.round_dp(4),
            opportunity.required_usdc.round_dp(4)
        ))
    } else if let Some(near_miss) = near_misses.first() {
        Some(format!(
            "near {} | gap {} | {}",
            near_miss.kind.as_str(),
            near_miss.shortfall_label,
            near_miss.reason
        ))
    } else {
        current_market.map(|market| explain_no_near_miss_runtime(config, market))
    };
    let worst_open_reason = worst_open_position.as_ref().map(|position| {
        format!(
            " | open_mtm {} {} stop={}",
            position.slug,
            position.mark_to_market_profit.round_dp(4),
            yes_no_ru(position.stop_loss_hit)
        )
    });
    let data_health_suffix = runtime_summary
        .data_health_reason
        .as_ref()
        .map(|value| format!(" | data: {value}"))
        .unwrap_or_default();
    let decision_reason = Some(format!(
        "regime={} | {}{}{}{}",
        regime.as_str(),
        base_reason.unwrap_or_else(|| "no signal".to_owned()),
        worst_open_reason.unwrap_or_default(),
        regime_reason.map_or_else(String::new, |value| format!(" | mode: {value}")),
        risk_block_reason.map_or_else(String::new, |value| format!(" | risk: {value}"))
    ));
    let decision_reason = decision_reason.map(|value| format!("{value}{data_health_suffix}"));
    let open_notional = paper_state
        .market_notional
        .values()
        .copied()
        .sum::<Decimal>()
        .round_dp(6);
    let top_near_miss = near_misses.first();

    PaperCycleEntry {
        recorded_at: Utc::now(),
        total_markets: runtime_summary.total_markets,
        live_markets: runtime_summary.live_markets,
        strategy_fit_count: runtime_summary.strategy_fit_count,
        opportunity_count: opportunities.len(),
        near_miss_count: near_misses.len(),
        selected_count,
        executed_count,
        open_notional,
        total_spent_usdc: paper_state.total_spent_usdc.round_dp(6),
        total_expected_profit: paper_state.total_expected_profit.round_dp(6),
        top_opportunity_slug: opportunities.first().map(|entry| entry.slug.clone()),
        top_opportunity_kind: opportunities
            .first()
            .map(|entry| entry.kind.as_str().to_owned()),
        top_opportunity_edge_bps: opportunities.first().map(|entry| entry.edge_bps),
        top_opportunity_required_usdc: opportunities
            .first()
            .map(|entry| entry.required_usdc.round_dp(4).to_string()),
        top_opportunity_expected_profit_usdc: opportunities
            .first()
            .map(|entry| entry.expected_profit.round_dp(4).to_string()),
        top_opportunity_signal_strength_bps: opportunities
            .first()
            .map(|entry| entry.signal_strength_bps.round_dp(2).to_string()),
        top_opportunity_target_gap_bps: opportunities
            .first()
            .map(|entry| entry.target_gap_bps.round_dp(2).to_string()),
        top_opportunity_primary_ask: opportunities
            .first()
            .map(|entry| entry.primary_outcome_ask_price.round_dp(4).to_string()),
        top_opportunity_signal_tier: opportunities
            .first()
            .and_then(|entry| (!entry.signal_tier.is_empty()).then(|| entry.signal_tier.clone())),
        top_opportunity_target_cross_label: opportunities.first().and_then(|entry| {
            (!entry.target_cross_label.is_empty()).then(|| entry.target_cross_label.clone())
        }),
        top_near_miss_slug: top_near_miss.map(|entry| entry.slug.clone()),
        top_near_miss_reason: top_near_miss.map(|entry| entry.reason.clone()),
        top_near_miss_primary_ask: top_near_miss
            .and_then(|entry| entry.primary_outcome_ask_price)
            .map(|value| value.round_dp(4).to_string()),
        top_near_miss_bundle_cost: top_near_miss
            .and_then(|entry| entry.bundle_cost)
            .map(|value| value.round_dp(4).to_string()),
        top_near_miss_target_gap_bps: top_near_miss
            .map(|entry| entry.target_gap_bps.round_dp(2).to_string()),
        top_near_miss_spot_move_bps: top_near_miss
            .map(|entry| entry.spot_move_bps.round_dp(2).to_string()),
        top_near_miss_spot_move_1s_bps: top_near_miss
            .map(|entry| entry.spot_move_1s_bps.round_dp(2).to_string()),
        top_near_miss_spot_move_5s_bps: top_near_miss
            .map(|entry| entry.spot_move_5s_bps.round_dp(2).to_string()),
        top_near_miss_spot_move_15s_bps: top_near_miss
            .map(|entry| entry.spot_move_15s_bps.round_dp(2).to_string()),
        top_near_miss_micro_acceleration_bps: top_near_miss
            .map(|entry| entry.micro_acceleration_bps.round_dp(2).to_string()),
        top_near_miss_exchange_book_age_ms: top_near_miss
            .and_then(|entry| entry.exchange_book_age_ms),
        top_near_miss_exchange_book_top_imbalance_bps: top_near_miss.map(|entry| {
            entry
                .exchange_book_top_imbalance_bps
                .round_dp(2)
                .to_string()
        }),
        top_near_miss_exchange_book_depth_imbalance_bps: top_near_miss.map(|entry| {
            entry
                .exchange_book_depth_imbalance_bps
                .round_dp(2)
                .to_string()
        }),
        top_near_miss_shortfall_bps: top_near_miss.map(|entry| entry.shortfall_bps),
        top_near_miss_shortfall_label: top_near_miss.map(|entry| entry.shortfall_label.clone()),
        current_market_slug: current_market.map(|market| market.slug.clone()),
        current_market_seconds_left: current_market.map(|market| market.seconds_left),
        current_market_spot_move_bps: current_market.map(|market| market.spot_move_bps.clone()),
        current_market_spot_move_1s_bps: current_market
            .map(|market| market.spot_move_1s_bps.clone()),
        current_market_spot_move_5s_bps: current_market
            .map(|market| market.spot_move_5s_bps.clone()),
        current_market_spot_move_15s_bps: current_market
            .map(|market| market.spot_move_15s_bps.clone()),
        current_market_micro_acceleration_bps: current_market
            .map(|market| market.micro_acceleration_bps.clone()),
        current_market_price: current_market.map(|market| market.current_price.clone()),
        current_market_spot_source: current_market
            .map(|market| market.current_price_source.clone()),
        current_market_spot_event_age_ms: current_market
            .and_then(|market| market.current_price_event_age_ms),
        current_market_spot_received_age_ms: current_market
            .and_then(|market| market.current_price_received_age_ms),
        current_market_spot_quote_points: current_market
            .and_then(|market| market.current_price_quote_points),
        current_market_exchange_book_age_ms: current_market
            .and_then(|market| market.exchange_book_age_ms),
        current_market_exchange_book_top_imbalance_bps: current_market
            .map(|market| market.exchange_book_top_imbalance_bps.clone()),
        current_market_exchange_book_depth_imbalance_bps: current_market
            .map(|market| market.exchange_book_depth_imbalance_bps.clone()),
        current_market_exchange_book_microprice_bps: current_market
            .map(|market| market.exchange_book_microprice_bps.clone()),
        current_market_exchange_book_spread_bps: current_market
            .map(|market| market.exchange_book_spread_bps.clone()),
        current_market_target_price: current_market.map(|market| market.target_price.clone()),
        current_market_target_price_source: current_market
            .map(|market| market.target_price_source.clone()),
        current_market_target_gap_bps: current_market.map(|market| market.target_gap_bps.clone()),
        current_market_up_ask: current_market.map(|market| market.up_ask.clone()),
        current_market_down_ask: current_market.map(|market| market.down_ask.clone()),
        current_market_bundle_cost: current_market.map(|market| market.bundle_cost.clone()),
        current_market_direction: current_market.map(|market| market.dominant_outcome.clone()),
        current_market_fit: current_market.map(|market| market.status.strategy_fit),
        current_market_health: current_market
            .map_or_else(RuntimeCurrentMarketHealth::missing_context, |market| {
                market.status.health.clone()
            })
            .into(),
        data_health_reason: runtime_summary.data_health_reason.clone(),
        decision_reason,
        regime: Some(regime.as_str().to_owned()),
        risk_blocked: risk_block_reason.is_some() || risk_tracker.is_blocked(),
        risk_reason: risk_block_reason.map(str::to_owned),
        daily_realized_profit: risk_tracker.daily_realized_profit.round_dp(6),
        session_realized_profit: risk_tracker
            .session_realized_profit(paper_state.total_realized_profit)
            .round_dp(6),
        consecutive_losses: risk_tracker.consecutive_losses,
        worst_open_slug: worst_open_position
            .as_ref()
            .map(|position| position.slug.clone()),
        worst_open_mtm_profit_usdc: worst_open_position
            .as_ref()
            .map(|position| position.mark_to_market_profit.round_dp(4).to_string()),
        worst_open_stop_loss_hit: worst_open_position
            .as_ref()
            .map(|position| position.stop_loss_hit),
        worst_open_aligned_1s_bps: worst_open_position
            .as_ref()
            .map(|position| position.aligned_1s_bps.round_dp(2).to_string()),
        worst_open_aligned_5s_bps: worst_open_position
            .as_ref()
            .map(|position| position.aligned_5s_bps.round_dp(2).to_string()),
        worst_open_aligned_15s_bps: worst_open_position
            .as_ref()
            .map(|position| position.aligned_15s_bps.round_dp(2).to_string()),
        latency: runtime_summary.latency.into(),
    }
}
