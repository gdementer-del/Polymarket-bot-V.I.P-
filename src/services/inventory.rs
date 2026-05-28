//! Replay-style inventory analytics for enriched wallet activity logs.
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::missing_errors_doc,
    clippy::too_many_lines
)]

use std::cmp::Reverse;
use std::collections::{BTreeMap, VecDeque};
use std::fmt::Write as _;
use std::fs;
use std::path::Path;

use chrono::{DateTime, Local, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::config::AppConfig;
use crate::error::Result;

use super::labels::{
    outcome_label_is_down, outcome_label_is_up, wallet_side_is_buy_label, wallet_side_is_sell_label,
};
use super::wallet_activity::{
    DEFAULT_WALLET_ACTIVITY_RECORD_FILENAME, WalletActivitySnapshotRecord,
    load_wallet_activity_records,
};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub enum ExecutionHeuristic {
    MakerLike,
    Neutral,
    CrossedOrStale,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InventoryReplayDataset {
    pub windows: Vec<InventoryReplayWindow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InventoryReplayWindow {
    pub slug: String,
    pub started_at: i64,
    pub ended_at: i64,
    pub steps: Vec<InventoryReplayStep>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InventoryReplayStep {
    pub activity_timestamp: i64,
    pub activity_type: String,
    pub side: String,
    pub outcome: String,
    pub usdc_size: Decimal,
    pub inferred_shares: Option<Decimal>,
    pub net_up_shares: Decimal,
    pub net_down_shares: Decimal,
    pub gross_inventory_shares: Decimal,
    pub directional_delta_shares: Decimal,
    pub hedged_share_pct: Option<Decimal>,
    pub dominant_outcome: String,
    pub execution_heuristic: ExecutionHeuristic,
    pub selected_trade_discount_to_ask_bps: Option<Decimal>,
    pub target_gap_bps: Option<Decimal>,
    pub seconds_left_at_observed: Option<i64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct InventoryReplayAlertThresholds {
    pub min_alert_gross_inventory_shares: Decimal,
    pub imbalance_max_hedged_share_pct: Decimal,
    pub severe_imbalance_max_hedged_share_pct: Decimal,
    pub late_window_seconds_left_max: i64,
    pub late_window_expansion_min_gross_shares: Decimal,
    pub late_window_expansion_min_step_growth_shares: Decimal,
    pub adverse_execution_cluster_window: usize,
    pub adverse_execution_cluster_min_crossed: usize,
}

impl Default for InventoryReplayAlertThresholds {
    fn default() -> Self {
        Self {
            min_alert_gross_inventory_shares: Decimal::from(1_000_u32),
            imbalance_max_hedged_share_pct: Decimal::from(20_u32),
            severe_imbalance_max_hedged_share_pct: Decimal::from(10_u32),
            late_window_seconds_left_max: 12,
            late_window_expansion_min_gross_shares: Decimal::from(1_500_u32),
            late_window_expansion_min_step_growth_shares: Decimal::from(250_u32),
            adverse_execution_cluster_window: 5,
            adverse_execution_cluster_min_crossed: 3,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub enum InventoryReplayAlertKind {
    InventoryImbalance,
    LateWindowExpansion,
    AdverseExecutionCluster,
    CooldownCandidate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InventoryReplayAlert {
    pub kind: InventoryReplayAlertKind,
    pub activity_timestamp: i64,
    pub gross_inventory_shares: Decimal,
    pub directional_delta_shares: Decimal,
    pub hedged_share_pct: Option<Decimal>,
    pub seconds_left_at_observed: Option<i64>,
    pub note: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InventoryReplayWindowExport {
    pub slug: String,
    pub started_at: i64,
    pub ended_at: i64,
    pub trade_events: usize,
    pub redeem_events: usize,
    pub trade_volume_usdc: Decimal,
    pub redeem_volume_usdc: Decimal,
    pub peak_gross_inventory_shares: Decimal,
    pub final_gross_inventory_shares: Decimal,
    pub final_directional_delta_shares: Decimal,
    pub final_hedged_share_pct: Option<Decimal>,
    pub alerts: Vec<InventoryReplayAlert>,
    pub steps: Vec<InventoryReplayStep>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InventoryReplayExport {
    pub source_path: String,
    pub source_records: usize,
    pub thresholds: InventoryReplayAlertThresholds,
    pub generated_at: DateTime<Utc>,
    pub windows: Vec<InventoryReplayWindowExport>,
}

#[derive(Debug, Clone, Copy)]
pub struct InventoryReplaySimulationConfig {
    pub max_gross_window_shares: Decimal,
    pub max_directional_delta_shares: Decimal,
    pub cooldown_secs: i64,
    pub trigger_on_cooldown_alert: bool,
    pub trigger_on_late_expansion: bool,
}

#[derive(Debug, Clone)]
pub struct InventoryReplaySimulationSummary {
    pub windows: usize,
    pub total_trade_events: usize,
    pub accepted_trade_events: usize,
    pub blocked_by_gross_cap: usize,
    pub blocked_by_directional_cap: usize,
    pub blocked_by_cooldown: usize,
    pub cooldown_activations: usize,
    pub impacted_windows: usize,
    pub accepted_alert_events: usize,
    pub accepted_alert_steps: usize,
    pub accepted_alert_windows: usize,
    pub accepted_cooldown_alerts: usize,
    pub accepted_cooldown_steps: usize,
    pub accepted_late_alerts: usize,
    pub accepted_late_steps: usize,
    pub accepted_crossed_cluster_alerts: usize,
    pub accepted_crossed_cluster_steps: usize,
}

#[derive(Debug, Clone)]
pub struct WindowSimulationResult {
    pub slug: String,
    pub trade_events: usize,
    pub accepted_trade_events: usize,
    pub blocked_by_gross_cap: usize,
    pub blocked_by_directional_cap: usize,
    pub blocked_by_cooldown: usize,
    pub cooldown_activations: usize,
}

impl InventoryReplayDataset {
    #[must_use]
    pub fn from_records(records: &[WalletActivitySnapshotRecord]) -> Self {
        let mut sorted_records = records.to_vec();
        sorted_records.sort_by(|left, right| {
            left.activity_timestamp
                .cmp(&right.activity_timestamp)
                .then_with(|| left.recorded_at.cmp(&right.recorded_at))
                .then_with(|| left.transaction_hash.cmp(&right.transaction_hash))
        });

        let mut by_slug = BTreeMap::<String, InventoryReplayWindow>::new();
        for record in &sorted_records {
            let window =
                by_slug
                    .entry(record.slug.clone())
                    .or_insert_with(|| InventoryReplayWindow {
                        slug: record.slug.clone(),
                        started_at: record.activity_timestamp,
                        ended_at: record.activity_timestamp,
                        steps: Vec::new(),
                    });
            window.started_at = window.started_at.min(record.activity_timestamp);
            window.ended_at = window.ended_at.max(record.activity_timestamp);

            let previous_step = window.steps.last();
            let previous_up = previous_step.map_or(Decimal::ZERO, |step| step.net_up_shares);
            let previous_down = previous_step.map_or(Decimal::ZERO, |step| step.net_down_shares);

            let activity_type = normalized_activity_type(&record.activity_type);
            let usdc_size = record.usdc_size.unwrap_or(Decimal::ZERO);
            let inferred_shares = inferred_shares(record);

            let (next_up, next_down, execution_heuristic) = if activity_type == "REDEEM" {
                (previous_up, previous_down, ExecutionHeuristic::Unknown)
            } else {
                let mut next_up = previous_up;
                let mut next_down = previous_down;
                let shares = inferred_shares.unwrap_or(Decimal::ZERO);
                if outcome_side_is_up(&record.outcome) {
                    if wallet_side_is_sell(&record.side) {
                        next_up -= shares;
                    } else if wallet_side_is_buy(&record.side) {
                        next_up += shares;
                    }
                } else if outcome_side_is_down(&record.outcome) {
                    if wallet_side_is_sell(&record.side) {
                        next_down -= shares;
                    } else if wallet_side_is_buy(&record.side) {
                        next_down += shares;
                    }
                }
                (
                    next_up,
                    next_down,
                    execution_heuristic(record.selected_trade_discount_to_ask_bps),
                )
            };

            let gross_inventory_shares = next_up.abs() + next_down.abs();
            let directional_delta_shares = (next_up - next_down).abs();

            window.steps.push(InventoryReplayStep {
                activity_timestamp: record.activity_timestamp,
                activity_type,
                side: record.side.clone(),
                outcome: record.outcome.clone(),
                usdc_size,
                inferred_shares,
                net_up_shares: next_up,
                net_down_shares: next_down,
                gross_inventory_shares,
                directional_delta_shares,
                hedged_share_pct: hedged_share_pct(
                    gross_inventory_shares,
                    directional_delta_shares,
                ),
                dominant_outcome: dominant_outcome_from_net_shares(next_up, next_down),
                execution_heuristic,
                selected_trade_discount_to_ask_bps: record.selected_trade_discount_to_ask_bps,
                target_gap_bps: record.target_gap_bps,
                seconds_left_at_observed: record.seconds_left_at_observed,
            });
        }

        Self {
            windows: by_slug.into_values().collect(),
        }
    }

    #[must_use]
    pub fn highest_trade_volume_window(&self) -> Option<&InventoryReplayWindow> {
        self.windows.iter().max_by(|left, right| {
            left.trade_volume_usdc()
                .cmp(&right.trade_volume_usdc())
                .then_with(|| left.steps.len().cmp(&right.steps.len()))
        })
    }

    #[must_use]
    pub fn find_window(&self, slug: &str) -> Option<&InventoryReplayWindow> {
        self.windows.iter().find(|window| window.slug == slug)
    }
}

impl InventoryReplayWindow {
    #[must_use]
    pub fn trade_volume_usdc(&self) -> Decimal {
        self.steps
            .iter()
            .filter(|step| step.activity_type != "REDEEM")
            .fold(Decimal::ZERO, |sum, step| sum + step.usdc_size)
    }

    #[must_use]
    pub fn redeem_volume_usdc(&self) -> Decimal {
        self.steps
            .iter()
            .filter(|step| step.activity_type == "REDEEM")
            .fold(Decimal::ZERO, |sum, step| sum + step.usdc_size)
    }

    #[must_use]
    pub fn trade_events(&self) -> usize {
        self.steps
            .iter()
            .filter(|step| step.activity_type != "REDEEM")
            .count()
    }

    #[must_use]
    pub fn redeem_events(&self) -> usize {
        self.steps
            .iter()
            .filter(|step| step.activity_type == "REDEEM")
            .count()
    }

    #[must_use]
    pub fn peak_gross_inventory_shares(&self) -> Decimal {
        self.steps
            .iter()
            .map(|step| step.gross_inventory_shares)
            .max()
            .unwrap_or(Decimal::ZERO)
    }

    #[must_use]
    pub fn final_step(&self) -> Option<&InventoryReplayStep> {
        self.steps.last()
    }

    #[must_use]
    pub fn alerts(&self, thresholds: InventoryReplayAlertThresholds) -> Vec<InventoryReplayAlert> {
        replay_alerts_from_steps(&self.steps, thresholds)
    }
}

impl InventoryReplayWindowExport {
    #[must_use]
    pub fn alerts_with_thresholds(
        &self,
        thresholds: InventoryReplayAlertThresholds,
    ) -> Vec<InventoryReplayAlert> {
        replay_alerts_from_steps(&self.steps, thresholds)
    }
}

#[must_use]
pub fn replay_alerts_from_steps(
    steps: &[InventoryReplayStep],
    thresholds: InventoryReplayAlertThresholds,
) -> Vec<InventoryReplayAlert> {
    let mut alerts = Vec::new();
    let mut imbalance_count = 0_usize;
    let mut late_expansion_count = 0_usize;
    let mut adverse_cluster_count = 0_usize;
    let mut severe_imbalance_steps = 0_usize;
    let mut imbalance_active = false;
    let mut late_expansion_active = false;
    let mut crossed_cluster_active = false;

    for (index, step) in steps.iter().enumerate() {
        if step.activity_type == "REDEEM" {
            continue;
        }

        let previous_gross = if index == 0 {
            Decimal::ZERO
        } else {
            steps[index - 1].gross_inventory_shares
        };
        let gross_growth = (step.gross_inventory_shares - previous_gross).max(Decimal::ZERO);

        let imbalance_gross_gate = thresholds
            .min_alert_gross_inventory_shares
            .max(Decimal::from(1_000_u32));
        let imbalance_hedged_gate = thresholds
            .imbalance_max_hedged_share_pct
            .min(Decimal::from(6_u32));
        let imbalance_condition = step.gross_inventory_shares >= imbalance_gross_gate
            && step
                .hedged_share_pct
                .is_some_and(|hedged| hedged <= imbalance_hedged_gate);
        let severe_imbalance_condition = step.gross_inventory_shares >= imbalance_gross_gate
            && step
                .hedged_share_pct
                .is_some_and(|hedged| hedged <= thresholds.severe_imbalance_max_hedged_share_pct);

        if severe_imbalance_condition {
            severe_imbalance_steps += 1;
        }

        if imbalance_condition && !imbalance_active {
            imbalance_count += 1;
            alerts.push(InventoryReplayAlert {
                kind: InventoryReplayAlertKind::InventoryImbalance,
                activity_timestamp: step.activity_timestamp,
                gross_inventory_shares: step.gross_inventory_shares,
                directional_delta_shares: step.directional_delta_shares,
                hedged_share_pct: step.hedged_share_pct,
                seconds_left_at_observed: step.seconds_left_at_observed,
                note: format!(
                    "hedged_share={}%, dominant={}, gross={}",
                    option_decimal_string(step.hedged_share_pct),
                    empty_dash(&step.dominant_outcome),
                    step.gross_inventory_shares.round_dp(2)
                ),
            });
        }
        imbalance_active = imbalance_condition;

        let late_seconds_gate = thresholds.late_window_seconds_left_max.clamp(4, 5);
        let late_growth_gate = thresholds
            .late_window_expansion_min_step_growth_shares
            .max(Decimal::from(150_u32));
        let late_directional_condition = step.gross_inventory_shares > Decimal::ZERO
            && step.directional_delta_shares
                >= (step.gross_inventory_shares * Decimal::new(25, 2)).round_dp(8);
        let late_expansion_condition = step
            .seconds_left_at_observed
            .is_some_and(|left| left <= late_seconds_gate)
            && step.gross_inventory_shares >= thresholds.late_window_expansion_min_gross_shares
            && gross_growth >= late_growth_gate
            && late_directional_condition;

        if late_expansion_condition && !late_expansion_active {
            late_expansion_count += 1;
            alerts.push(InventoryReplayAlert {
                kind: InventoryReplayAlertKind::LateWindowExpansion,
                activity_timestamp: step.activity_timestamp,
                gross_inventory_shares: step.gross_inventory_shares,
                directional_delta_shares: step.directional_delta_shares,
                hedged_share_pct: step.hedged_share_pct,
                seconds_left_at_observed: step.seconds_left_at_observed,
                note: format!(
                    "gross_growth={} shares near close",
                    gross_growth.round_dp(2)
                ),
            });
        }
        late_expansion_active = late_expansion_condition;

        let crossed_cluster_condition = if let Some(crossed_count) =
            recent_crossed_cluster_count_from_steps(steps, index, thresholds)
        {
            let gross_gate = thresholds
                .min_alert_gross_inventory_shares
                .max(Decimal::from(1_000_u32));
            let material_directional_pressure = step.gross_inventory_shares > Decimal::ZERO
                && step.directional_delta_shares
                    >= (step.gross_inventory_shares * Decimal::new(40, 2)).round_dp(8);
            let inventory_exposed = step
                .hedged_share_pct
                .is_none_or(|hedged| hedged <= Decimal::from(60_u32));
            let late_window_pressure = step.seconds_left_at_observed.is_some_and(|left| {
                left <= thresholds.late_window_seconds_left_max.saturating_mul(6)
            });
            crossed_count >= thresholds.adverse_execution_cluster_min_crossed
                && step.gross_inventory_shares >= gross_gate
                && ((material_directional_pressure && inventory_exposed) || late_window_pressure)
        } else {
            false
        };

        if crossed_cluster_condition && !crossed_cluster_active {
            let crossed_count = recent_crossed_cluster_count_from_steps(steps, index, thresholds)
                .unwrap_or_default();
            adverse_cluster_count += 1;
            alerts.push(InventoryReplayAlert {
                kind: InventoryReplayAlertKind::AdverseExecutionCluster,
                activity_timestamp: step.activity_timestamp,
                gross_inventory_shares: step.gross_inventory_shares,
                directional_delta_shares: step.directional_delta_shares,
                hedged_share_pct: step.hedged_share_pct,
                seconds_left_at_observed: step.seconds_left_at_observed,
                note: format!(
                    "{crossed_count} crossed/stale prints in last {} trades; directional_pressure={} late_pressure={}",
                    thresholds.adverse_execution_cluster_window,
                    step.directional_delta_shares.round_dp(2),
                    step.seconds_left_at_observed.unwrap_or_default()
                ),
            });
        }
        crossed_cluster_active = crossed_cluster_condition;
    }

    if let Some(final_step) = steps.last()
        && final_step.activity_type != "REDEEM"
        && final_step.gross_inventory_shares >= thresholds.min_alert_gross_inventory_shares
        && final_step
            .hedged_share_pct
            .is_some_and(|hedged| hedged <= thresholds.severe_imbalance_max_hedged_share_pct)
        && (late_expansion_count > 0
            || adverse_cluster_count > 1
            || imbalance_count > 1
            || severe_imbalance_steps > 1)
    {
        alerts.push(InventoryReplayAlert {
            kind: InventoryReplayAlertKind::CooldownCandidate,
            activity_timestamp: final_step.activity_timestamp,
            gross_inventory_shares: final_step.gross_inventory_shares,
            directional_delta_shares: final_step.directional_delta_shares,
            hedged_share_pct: final_step.hedged_share_pct,
            seconds_left_at_observed: final_step.seconds_left_at_observed,
            note: format!(
                "late_expansion={late_expansion_count}, adverse_clusters={adverse_cluster_count}, imbalance_events={imbalance_count}, severe_imbalance_steps={severe_imbalance_steps}"
            ),
        });
    }

    alerts
}

fn recent_crossed_cluster_count_from_steps(
    steps: &[InventoryReplayStep],
    index: usize,
    thresholds: InventoryReplayAlertThresholds,
) -> Option<usize> {
    if thresholds.adverse_execution_cluster_window == 0 {
        return None;
    }
    let start = index
        .saturating_add(1)
        .saturating_sub(thresholds.adverse_execution_cluster_window);
    let recent = steps[start..=index]
        .iter()
        .filter(|step| step.activity_type != "REDEEM")
        .count();
    if recent < thresholds.adverse_execution_cluster_window {
        return None;
    }
    Some(
        steps[start..=index]
            .iter()
            .filter(|step| {
                step.activity_type != "REDEEM"
                    && step.execution_heuristic == ExecutionHeuristic::CrossedOrStale
            })
            .count(),
    )
}

#[must_use]
pub fn calibrate_inventory_replay_alert_thresholds(
    exports: &[InventoryReplayExport],
) -> InventoryReplayAlertThresholds {
    let mut imbalance_gross = Vec::new();
    let mut imbalance_hedged = Vec::new();
    let mut severe_hedged = Vec::new();
    let mut late_seconds_left = Vec::new();
    let mut late_gross = Vec::new();
    let mut late_growth = Vec::new();
    let mut cluster_maxima = Vec::new();

    for export in exports {
        for window in &export.windows {
            let mut previous_gross = Decimal::ZERO;
            let mut rolling_crossed = 0_usize;
            let mut recent = VecDeque::new();
            for step in &window.steps {
                if step.activity_type == "REDEEM" {
                    continue;
                }

                if let Some(hedged) = step.hedged_share_pct
                    && step.gross_inventory_shares > Decimal::ZERO
                {
                    if hedged <= Decimal::from(25_u32) {
                        imbalance_gross.push(step.gross_inventory_shares);
                        imbalance_hedged.push(hedged);
                    }
                    if hedged <= Decimal::from(10_u32) {
                        severe_hedged.push(hedged);
                    }
                }

                let growth = (step.gross_inventory_shares - previous_gross).max(Decimal::ZERO);
                previous_gross = step.gross_inventory_shares;
                if let Some(left) = step.seconds_left_at_observed
                    && left <= 20
                    && growth > Decimal::ZERO
                {
                    late_seconds_left.push(left);
                    late_gross.push(step.gross_inventory_shares);
                    late_growth.push(growth);
                }

                recent.push_back(step.execution_heuristic == ExecutionHeuristic::CrossedOrStale);
                if *recent.back().unwrap_or(&false) {
                    rolling_crossed += 1;
                }
                if recent.len() > 5 && recent.pop_front().unwrap_or(false) {
                    rolling_crossed = rolling_crossed.saturating_sub(1);
                }
                if window.trade_events >= 20 {
                    cluster_maxima.push(rolling_crossed);
                }
            }
        }
    }

    let min_alert_gross_inventory_shares = round_decimal_to_step(
        decimal_quantile(&imbalance_gross, 0.50).unwrap_or(Decimal::from(1_000_u32)),
        Decimal::from(100_u32),
    )
    .max(Decimal::from(500_u32));

    let imbalance_max_hedged_share_pct = decimal_quantile(&imbalance_hedged, 0.75)
        .unwrap_or(Decimal::from(20_u32))
        .round_dp(2)
        .clamp(Decimal::from(5_u32), Decimal::from(20_u32));

    let severe_candidate = decimal_quantile(&severe_hedged, 0.50)
        .unwrap_or(Decimal::from(10_u32))
        .round_dp(2)
        .max(Decimal::from(1_u32));
    let severe_imbalance_max_hedged_share_pct = severe_candidate
        .min((imbalance_max_hedged_share_pct - Decimal::from(1_u32)).max(Decimal::from(1_u32)));

    let late_window_seconds_left_max = i64_quantile(&late_seconds_left, 0.75)
        .unwrap_or(12)
        .clamp(4, 12);

    let late_window_expansion_min_gross_shares = round_decimal_to_step(
        decimal_quantile(&late_gross, 0.50).unwrap_or(Decimal::from(1_500_u32)),
        Decimal::from(100_u32),
    )
    .max(Decimal::from(1_000_u32));

    let significant_late_growth = late_growth
        .iter()
        .copied()
        .filter(|growth| *growth >= Decimal::from(25_u32))
        .collect::<Vec<_>>();
    let late_window_expansion_min_step_growth_shares = round_decimal_to_step(
        decimal_quantile(&significant_late_growth, 0.50)
            .or_else(|| decimal_quantile(&late_growth, 0.75))
            .unwrap_or(Decimal::from(250_u32)),
        Decimal::from(25_u32),
    )
    .max(Decimal::from(25_u32));

    InventoryReplayAlertThresholds {
        min_alert_gross_inventory_shares,
        imbalance_max_hedged_share_pct,
        severe_imbalance_max_hedged_share_pct,
        late_window_seconds_left_max,
        late_window_expansion_min_gross_shares,
        late_window_expansion_min_step_growth_shares,
        adverse_execution_cluster_window: 5,
        adverse_execution_cluster_min_crossed: usize_quantile(&cluster_maxima, 0.50)
            .unwrap_or(3)
            .clamp(2, 5),
    }
}

#[derive(Debug, Clone)]
struct ReplayWindowSummary {
    slug: String,
    started_at: i64,
    ended_at: i64,
    trade_events: usize,
    redeem_events: usize,
    trade_usdc: Decimal,
    redeem_usdc: Decimal,
    buy_trades: usize,
    sell_trades: usize,
    net_up_shares: Decimal,
    net_down_shares: Decimal,
    max_gross_inventory_shares: Decimal,
    max_directional_delta_shares: Decimal,
    maker_like_trades: usize,
    neutral_execution_trades: usize,
    crossed_or_stale_trades: usize,
    unknown_execution_trades: usize,
    dominant_switches: usize,
    previous_dominant: String,
    last_trade_ts: Option<i64>,
    trade_spacing_sum_secs: Decimal,
    trade_spacing_count: usize,
}

impl ReplayWindowSummary {
    fn new(slug: String, timestamp: i64) -> Self {
        Self {
            slug,
            started_at: timestamp,
            ended_at: timestamp,
            trade_events: 0,
            redeem_events: 0,
            trade_usdc: Decimal::ZERO,
            redeem_usdc: Decimal::ZERO,
            buy_trades: 0,
            sell_trades: 0,
            net_up_shares: Decimal::ZERO,
            net_down_shares: Decimal::ZERO,
            max_gross_inventory_shares: Decimal::ZERO,
            max_directional_delta_shares: Decimal::ZERO,
            maker_like_trades: 0,
            neutral_execution_trades: 0,
            crossed_or_stale_trades: 0,
            unknown_execution_trades: 0,
            dominant_switches: 0,
            previous_dominant: String::new(),
            last_trade_ts: None,
            trade_spacing_sum_secs: Decimal::ZERO,
            trade_spacing_count: 0,
        }
    }

    fn final_gross_inventory_shares(&self) -> Decimal {
        self.net_up_shares.abs() + self.net_down_shares.abs()
    }

    fn final_directional_delta_shares(&self) -> Decimal {
        (self.net_up_shares - self.net_down_shares).abs()
    }

    fn hedged_share_pct(&self) -> Option<Decimal> {
        hedged_share_pct(
            self.final_gross_inventory_shares(),
            self.final_directional_delta_shares(),
        )
    }

    fn avg_seconds_between_trades(&self) -> Option<Decimal> {
        if self.trade_spacing_count == 0 {
            None
        } else {
            Some(
                (self.trade_spacing_sum_secs / Decimal::from(self.trade_spacing_count as u64))
                    .round_dp(4),
            )
        }
    }

    fn maker_like_share_pct(&self) -> Option<Decimal> {
        if self.trade_events == 0 {
            None
        } else {
            Some(
                (Decimal::from(self.maker_like_trades as u64)
                    / Decimal::from(self.trade_events as u64)
                    * Decimal::from(100_u32))
                .round_dp(2),
            )
        }
    }

    fn is_two_sided(&self) -> bool {
        self.net_up_shares > Decimal::ZERO && self.net_down_shares > Decimal::ZERO
    }

    fn is_balanced(&self) -> bool {
        let gross = self.final_gross_inventory_shares();
        gross > Decimal::ZERO
            && self.final_directional_delta_shares() <= (gross * Decimal::new(20, 2)).round_dp(8)
    }
}

pub fn show_wallet_inventory_replay_report(
    config: &AppConfig,
    input: Option<&Path>,
    limit: Option<usize>,
    top: usize,
) -> Result<()> {
    let path = input.map_or_else(
        || {
            config
                .storage
                .state_dir
                .join(DEFAULT_WALLET_ACTIVITY_RECORD_FILENAME)
        },
        Path::to_path_buf,
    );
    show_wallet_inventory_replay_report_path(&path, limit, top)
}

pub fn show_wallet_inventory_replay_report_path(
    path: &Path,
    limit: Option<usize>,
    top: usize,
) -> Result<()> {
    let mut records = load_wallet_activity_records(path, limit)?;
    if records.is_empty() {
        info!(path = %path.display(), "в replay-журнале wallet activity пока нет записей");
        return Ok(());
    }

    records.sort_by(|left, right| {
        left.activity_timestamp
            .cmp(&right.activity_timestamp)
            .then_with(|| left.recorded_at.cmp(&right.recorded_at))
            .then_with(|| left.transaction_hash.cmp(&right.transaction_hash))
    });

    info!(
        path = %path.display(),
        records = records.len(),
        "загружен replay inventory report для enriched wallet activity"
    );
    info!(
        "\n{}",
        render_wallet_inventory_replay_report(&records, top.max(1))
    );
    Ok(())
}

pub fn show_wallet_inventory_replay_window_report(
    config: &AppConfig,
    input: Option<&Path>,
    limit: Option<usize>,
    slug: Option<&str>,
    events: usize,
) -> Result<()> {
    let path = input.map_or_else(
        || {
            config
                .storage
                .state_dir
                .join(DEFAULT_WALLET_ACTIVITY_RECORD_FILENAME)
        },
        Path::to_path_buf,
    );
    show_wallet_inventory_replay_window_report_path(&path, limit, slug, events)
}

pub fn show_wallet_inventory_replay_window_report_path(
    path: &Path,
    limit: Option<usize>,
    slug: Option<&str>,
    events: usize,
) -> Result<()> {
    let records = load_wallet_activity_records(path, limit)?;
    if records.is_empty() {
        info!(path = %path.display(), "в replay-журнале wallet activity пока нет записей");
        return Ok(());
    }

    let dataset = InventoryReplayDataset::from_records(&records);
    let window = if let Some(slug) = slug {
        dataset.find_window(slug)
    } else {
        dataset.highest_trade_volume_window()
    };

    let Some(window) = window else {
        info!(path = %path.display(), "подходящее replay-окно не найдено");
        return Ok(());
    };

    info!(
        path = %path.display(),
        slug = window.slug,
        events = window.steps.len(),
        "загружен replay timeline для enriched wallet activity"
    );
    info!(
        "\n{}",
        render_wallet_inventory_window_report(window, events.max(1))
    );
    Ok(())
}

pub fn export_wallet_inventory_replay_dataset(
    config: &AppConfig,
    input: Option<&Path>,
    limit: Option<usize>,
    output: Option<&Path>,
) -> Result<()> {
    let input_path = input.map_or_else(
        || {
            config
                .storage
                .state_dir
                .join(DEFAULT_WALLET_ACTIVITY_RECORD_FILENAME)
        },
        Path::to_path_buf,
    );
    let output_path = output.map_or_else(
        || default_replay_export_path(&input_path),
        Path::to_path_buf,
    );
    export_wallet_inventory_replay_dataset_path(&input_path, limit, &output_path)
}

pub fn export_wallet_inventory_replay_dataset_path(
    input_path: &Path,
    limit: Option<usize>,
    output_path: &Path,
) -> Result<()> {
    let records = load_wallet_activity_records(input_path, limit)?;
    if records.is_empty() {
        info!(path = %input_path.display(), "в replay-журнале wallet activity пока нет записей");
        return Ok(());
    }

    let dataset = InventoryReplayDataset::from_records(&records);
    let mut export = InventoryReplayExport {
        source_path: input_path.display().to_string(),
        source_records: records.len(),
        thresholds: InventoryReplayAlertThresholds::default(),
        generated_at: Utc::now(),
        windows: dataset
            .windows
            .iter()
            .map(|window| InventoryReplayWindowExport {
                slug: window.slug.clone(),
                started_at: window.started_at,
                ended_at: window.ended_at,
                trade_events: window.trade_events(),
                redeem_events: window.redeem_events(),
                trade_volume_usdc: window.trade_volume_usdc(),
                redeem_volume_usdc: window.redeem_volume_usdc(),
                peak_gross_inventory_shares: window.peak_gross_inventory_shares(),
                final_gross_inventory_shares: window
                    .final_step()
                    .map_or(Decimal::ZERO, |step| step.gross_inventory_shares),
                final_directional_delta_shares: window
                    .final_step()
                    .map_or(Decimal::ZERO, |step| step.directional_delta_shares),
                final_hedged_share_pct: window.final_step().and_then(|step| step.hedged_share_pct),
                alerts: Vec::new(),
                steps: window.steps.clone(),
            })
            .collect(),
    };

    let thresholds = calibrate_inventory_replay_alert_thresholds(std::slice::from_ref(&export));
    export.thresholds = thresholds;
    for window in &mut export.windows {
        window.alerts = window.alerts_with_thresholds(thresholds);
    }

    if let Some(parent) = output_path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)?;
    }
    fs::write(output_path, serde_json::to_vec_pretty(&export)?)?;
    info!(
        input = %input_path.display(),
        output = %output_path.display(),
        windows = export.windows.len(),
        "экспортирован replay dataset с inventory alerts"
    );
    Ok(())
}

pub fn export_wallet_inventory_replay_dataset_path_auto(
    input_path: &Path,
    limit: Option<usize>,
    output: Option<&Path>,
) -> Result<()> {
    let output_path =
        output.map_or_else(|| default_replay_export_path(input_path), Path::to_path_buf);
    export_wallet_inventory_replay_dataset_path(input_path, limit, &output_path)
}

pub fn load_inventory_replay_export(path: &Path) -> Result<InventoryReplayExport> {
    let raw = fs::read(path)?;
    Ok(serde_json::from_slice(&raw)?)
}

pub fn show_wallet_inventory_replay_simulation(
    config: &AppConfig,
    input: Option<&Path>,
    simulation: InventoryReplaySimulationConfig,
) -> Result<()> {
    let input_path = input.map_or_else(
        || {
            config
                .storage
                .state_dir
                .join("wallet_activity_replay_export.json")
        },
        Path::to_path_buf,
    );
    show_wallet_inventory_replay_simulation_path(&input_path, simulation)
}

pub fn show_wallet_inventory_replay_simulation_path(
    input_path: &Path,
    simulation: InventoryReplaySimulationConfig,
) -> Result<()> {
    let export = load_inventory_replay_export(input_path)?;
    let (summary, windows) = simulate_inventory_caps(&export, simulation);
    info!(
        input = %input_path.display(),
        windows = summary.windows,
        "загружен replay cap/cooldown simulator"
    );
    info!(
        "\n{}",
        render_inventory_replay_simulation_report(&summary, &windows, simulation)
    );
    Ok(())
}

fn render_wallet_inventory_replay_report(
    records: &[WalletActivitySnapshotRecord],
    top: usize,
) -> String {
    let mut output = String::new();
    let summaries = build_replay_window_summaries(records);
    let dataset = InventoryReplayDataset::from_records(records);
    let thresholds = InventoryReplayAlertThresholds::default();
    let window_alerts = dataset
        .windows
        .iter()
        .map(|window| (window.slug.clone(), window.alerts(thresholds)))
        .collect::<Vec<_>>();
    let trade_events: usize = summaries.iter().map(|summary| summary.trade_events).sum();
    let redeem_events: usize = summaries.iter().map(|summary| summary.redeem_events).sum();
    let trade_usdc = summaries
        .iter()
        .fold(Decimal::ZERO, |sum, summary| sum + summary.trade_usdc);
    let redeem_usdc = summaries
        .iter()
        .fold(Decimal::ZERO, |sum, summary| sum + summary.redeem_usdc);
    let final_gross_inventory = summaries.iter().fold(Decimal::ZERO, |sum, summary| {
        sum + summary.final_gross_inventory_shares()
    });
    let peak_gross_inventory = summaries.iter().fold(Decimal::ZERO, |sum, summary| {
        sum + summary.max_gross_inventory_shares
    });
    let final_directional_delta = summaries.iter().fold(Decimal::ZERO, |sum, summary| {
        sum + summary.final_directional_delta_shares()
    });
    let windows_with_redeem = summaries
        .iter()
        .filter(|summary| summary.redeem_events > 0)
        .count();
    let two_sided_windows = summaries
        .iter()
        .filter(|summary| summary.is_two_sided())
        .count();
    let balanced_windows = summaries
        .iter()
        .filter(|summary| summary.is_balanced())
        .count();
    let maker_like_trades = summaries
        .iter()
        .map(|summary| summary.maker_like_trades)
        .sum::<usize>();
    let crossed_or_stale_trades = summaries
        .iter()
        .map(|summary| summary.crossed_or_stale_trades)
        .sum::<usize>();
    let neutral_trades = summaries
        .iter()
        .map(|summary| summary.neutral_execution_trades)
        .sum::<usize>();
    let unknown_trades = summaries
        .iter()
        .map(|summary| summary.unknown_execution_trades)
        .sum::<usize>();
    let total_alerts = window_alerts
        .iter()
        .map(|(_, alerts)| alerts.len())
        .sum::<usize>();
    let imbalance_alerts = window_alerts
        .iter()
        .flat_map(|(_, alerts)| alerts.iter())
        .filter(|alert| alert.kind == InventoryReplayAlertKind::InventoryImbalance)
        .count();
    let late_expansion_alerts = window_alerts
        .iter()
        .flat_map(|(_, alerts)| alerts.iter())
        .filter(|alert| alert.kind == InventoryReplayAlertKind::LateWindowExpansion)
        .count();
    let adverse_cluster_alerts = window_alerts
        .iter()
        .flat_map(|(_, alerts)| alerts.iter())
        .filter(|alert| alert.kind == InventoryReplayAlertKind::AdverseExecutionCluster)
        .count();
    let cooldown_alerts = window_alerts
        .iter()
        .flat_map(|(_, alerts)| alerts.iter())
        .filter(|alert| alert.kind == InventoryReplayAlertKind::CooldownCandidate)
        .count();

    let first_ts = records.first().map_or_else(
        || "-".to_owned(),
        |record| format_unix_secs_local(record.activity_timestamp),
    );
    let last_ts = records.last().map_or_else(
        || "-".to_owned(),
        |record| format_unix_secs_local(record.activity_timestamp),
    );

    let _ = writeln!(output, "Replay inventory report");
    let _ = writeln!(output, "Период: {first_ts} -> {last_ts}");
    let _ = writeln!(output, "Окон: {}", summaries.len());
    let _ = writeln!(output, "TRADE событий: {trade_events}");
    let _ = writeln!(output, "REDEEM событий: {redeem_events}");
    let _ = writeln!(output, "Trade volume USDC: {}", trade_usdc.round_dp(4));
    let _ = writeln!(output, "Redeem volume USDC: {}", redeem_usdc.round_dp(4));
    let _ = writeln!(output, "Окон с redeem: {windows_with_redeem}");
    let _ = writeln!(output, "Двухсторонних окон на выходе: {two_sided_windows}");
    let _ = writeln!(
        output,
        "Сбалансированных окон на выходе: {balanced_windows}"
    );
    let _ = writeln!(
        output,
        "Final gross inventory shares: {}",
        final_gross_inventory.round_dp(4)
    );
    let _ = writeln!(
        output,
        "Peak gross inventory shares: {}",
        peak_gross_inventory.round_dp(4)
    );
    let _ = writeln!(
        output,
        "Final directional delta shares: {}",
        final_directional_delta.round_dp(4)
    );
    let _ = writeln!(
        output,
        "Хеджированная доля final inventory: {}",
        inventory_balance_ratio_string(final_gross_inventory, final_directional_delta)
    );
    let _ = writeln!(output, "Replay alerts: {total_alerts}");

    let execution_total =
        maker_like_trades + crossed_or_stale_trades + neutral_trades + unknown_trades;
    let _ = writeln!(output);
    let _ = writeln!(output, "Execution mix");
    let _ = writeln!(
        output,
        "- maker-like: {} ({})",
        maker_like_trades,
        share_pct_string(maker_like_trades, execution_total)
    );
    let _ = writeln!(
        output,
        "- crossed-or-stale: {} ({})",
        crossed_or_stale_trades,
        share_pct_string(crossed_or_stale_trades, execution_total)
    );
    let _ = writeln!(
        output,
        "- neutral: {} ({})",
        neutral_trades,
        share_pct_string(neutral_trades, execution_total)
    );
    let _ = writeln!(
        output,
        "- unknown: {} ({})",
        unknown_trades,
        share_pct_string(unknown_trades, execution_total)
    );

    let _ = writeln!(output);
    let _ = writeln!(output, "Alert mix");
    let _ = writeln!(output, "- inventory-imbalance: {imbalance_alerts}");
    let _ = writeln!(output, "- late-window-expansion: {late_expansion_alerts}");
    let _ = writeln!(
        output,
        "- adverse-execution-cluster: {adverse_cluster_alerts}"
    );
    let _ = writeln!(output, "- cooldown-candidate: {cooldown_alerts}");

    let mut top_by_trade_usdc = summaries.clone();
    top_by_trade_usdc.sort_by_key(|summary| Reverse(summary.trade_usdc));
    let _ = writeln!(output);
    let _ = writeln!(output, "Top windows by trade volume");
    let _ = writeln!(
        output,
        "slug | events | trade_usdc | redeem | final_gross | peak_gross | delta | hedged | maker | switches | avg_gap_s"
    );
    for summary in top_by_trade_usdc.iter().take(top) {
        let _ = writeln!(
            output,
            "{} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {}",
            summary.slug,
            summary.trade_events,
            summary.trade_usdc.round_dp(4),
            summary.redeem_events,
            summary.final_gross_inventory_shares().round_dp(4),
            summary.max_gross_inventory_shares.round_dp(4),
            summary.final_directional_delta_shares().round_dp(4),
            option_decimal_string(summary.hedged_share_pct()),
            option_decimal_string(summary.maker_like_share_pct()),
            summary.dominant_switches,
            option_decimal_string(summary.avg_seconds_between_trades()),
        );
    }

    let mut top_by_peak_inventory = summaries.clone();
    top_by_peak_inventory.sort_by_key(|summary| Reverse(summary.max_gross_inventory_shares));
    let _ = writeln!(output);
    let _ = writeln!(output, "Top windows by peak gross inventory");
    let _ = writeln!(
        output,
        "slug | buys | sells | up_shares | down_shares | final_dom | peak_gross | final_delta | started | ended"
    );
    for summary in top_by_peak_inventory.iter().take(top) {
        let _ = writeln!(
            output,
            "{} | {} | {} | {} | {} | {} | {} | {} | {} | {}",
            summary.slug,
            summary.buy_trades,
            summary.sell_trades,
            summary.net_up_shares.round_dp(4),
            summary.net_down_shares.round_dp(4),
            dominant_outcome_from_net_shares(summary.net_up_shares, summary.net_down_shares),
            summary.max_gross_inventory_shares.round_dp(4),
            summary.final_directional_delta_shares().round_dp(4),
            format_unix_secs_short(summary.started_at),
            format_unix_secs_short(summary.ended_at),
        );
    }

    let mut top_alert_windows = dataset
        .windows
        .iter()
        .map(|window| {
            let alerts = window.alerts(thresholds);
            (window, alerts)
        })
        .collect::<Vec<_>>();
    top_alert_windows.sort_by_key(|(_, alerts)| Reverse(alerts.len()));
    let _ = writeln!(output);
    let _ = writeln!(output, "Top windows by replay alerts");
    let _ = writeln!(
        output,
        "slug | alerts | cooldown | imbalance | late | crossed | final_hedged"
    );
    for (window, alerts) in top_alert_windows
        .iter()
        .filter(|(_, alerts)| !alerts.is_empty())
        .take(top)
    {
        let cooldown = alerts
            .iter()
            .filter(|alert| alert.kind == InventoryReplayAlertKind::CooldownCandidate)
            .count();
        let imbalance = alerts
            .iter()
            .filter(|alert| alert.kind == InventoryReplayAlertKind::InventoryImbalance)
            .count();
        let late = alerts
            .iter()
            .filter(|alert| alert.kind == InventoryReplayAlertKind::LateWindowExpansion)
            .count();
        let crossed = alerts
            .iter()
            .filter(|alert| alert.kind == InventoryReplayAlertKind::AdverseExecutionCluster)
            .count();
        let _ = writeln!(
            output,
            "{} | {} | {} | {} | {} | {} | {}",
            window.slug,
            alerts.len(),
            cooldown,
            imbalance,
            late,
            crossed,
            option_decimal_string(window.final_step().and_then(|step| step.hedged_share_pct)),
        );
    }

    output
}

#[must_use]
pub fn simulate_inventory_caps(
    export: &InventoryReplayExport,
    simulation: InventoryReplaySimulationConfig,
) -> (
    InventoryReplaySimulationSummary,
    Vec<WindowSimulationResult>,
) {
    let mut results = Vec::new();
    let mut summary = InventoryReplaySimulationSummary {
        windows: export.windows.len(),
        total_trade_events: 0,
        accepted_trade_events: 0,
        blocked_by_gross_cap: 0,
        blocked_by_directional_cap: 0,
        blocked_by_cooldown: 0,
        cooldown_activations: 0,
        impacted_windows: 0,
        accepted_alert_events: 0,
        accepted_alert_steps: 0,
        accepted_alert_windows: 0,
        accepted_cooldown_alerts: 0,
        accepted_cooldown_steps: 0,
        accepted_late_alerts: 0,
        accepted_late_steps: 0,
        accepted_crossed_cluster_alerts: 0,
        accepted_crossed_cluster_steps: 0,
    };

    for window in &export.windows {
        let window_alerts = window.alerts_with_thresholds(export.thresholds);
        let mut cooldown_until: Option<i64> = None;
        let mut window_had_accepted_alert_step = false;
        let mut result = WindowSimulationResult {
            slug: window.slug.clone(),
            trade_events: 0,
            accepted_trade_events: 0,
            blocked_by_gross_cap: 0,
            blocked_by_directional_cap: 0,
            blocked_by_cooldown: 0,
            cooldown_activations: 0,
        };

        for step in &window.steps {
            if step.activity_type == "REDEEM" {
                continue;
            }
            result.trade_events += 1;
            summary.total_trade_events += 1;

            if cooldown_until.is_some_and(|until| step.activity_timestamp < until) {
                result.blocked_by_cooldown += 1;
                summary.blocked_by_cooldown += 1;
                continue;
            }

            if step.gross_inventory_shares > simulation.max_gross_window_shares {
                result.blocked_by_gross_cap += 1;
                summary.blocked_by_gross_cap += 1;
                if simulation.cooldown_secs > 0 {
                    cooldown_until = Some(step.activity_timestamp + simulation.cooldown_secs);
                    result.cooldown_activations += 1;
                    summary.cooldown_activations += 1;
                }
                continue;
            }

            if step.directional_delta_shares > simulation.max_directional_delta_shares {
                result.blocked_by_directional_cap += 1;
                summary.blocked_by_directional_cap += 1;
                if simulation.cooldown_secs > 0 {
                    cooldown_until = Some(step.activity_timestamp + simulation.cooldown_secs);
                    result.cooldown_activations += 1;
                    summary.cooldown_activations += 1;
                }
                continue;
            }

            result.accepted_trade_events += 1;
            summary.accepted_trade_events += 1;

            let step_alerts = window_alerts
                .iter()
                .filter(|alert| alert.activity_timestamp == step.activity_timestamp)
                .collect::<Vec<_>>();
            if !step_alerts.is_empty() {
                summary.accepted_alert_events += step_alerts.len();
                summary.accepted_alert_steps += 1;
                window_had_accepted_alert_step = true;
                let has_cooldown = step_alerts
                    .iter()
                    .any(|alert| alert.kind == InventoryReplayAlertKind::CooldownCandidate);
                let has_late = step_alerts
                    .iter()
                    .any(|alert| alert.kind == InventoryReplayAlertKind::LateWindowExpansion);
                let has_crossed = step_alerts
                    .iter()
                    .any(|alert| alert.kind == InventoryReplayAlertKind::AdverseExecutionCluster);
                summary.accepted_cooldown_alerts += step_alerts
                    .iter()
                    .filter(|alert| alert.kind == InventoryReplayAlertKind::CooldownCandidate)
                    .count();
                summary.accepted_cooldown_steps += usize::from(has_cooldown);
                summary.accepted_late_alerts += step_alerts
                    .iter()
                    .filter(|alert| alert.kind == InventoryReplayAlertKind::LateWindowExpansion)
                    .count();
                summary.accepted_late_steps += usize::from(has_late);
                summary.accepted_crossed_cluster_alerts += step_alerts
                    .iter()
                    .filter(|alert| alert.kind == InventoryReplayAlertKind::AdverseExecutionCluster)
                    .count();
                summary.accepted_crossed_cluster_steps += usize::from(has_crossed);
            }

            if simulation.cooldown_secs > 0 {
                let has_cooldown_alert = simulation.trigger_on_cooldown_alert
                    && step_alerts
                        .iter()
                        .any(|alert| alert.kind == InventoryReplayAlertKind::CooldownCandidate);
                let has_late_alert = simulation.trigger_on_late_expansion
                    && step_alerts
                        .iter()
                        .any(|alert| alert.kind == InventoryReplayAlertKind::LateWindowExpansion);
                if has_cooldown_alert || has_late_alert {
                    cooldown_until = Some(step.activity_timestamp + simulation.cooldown_secs);
                    result.cooldown_activations += 1;
                    summary.cooldown_activations += 1;
                }
            }
        }

        if result.blocked_by_gross_cap > 0
            || result.blocked_by_directional_cap > 0
            || result.blocked_by_cooldown > 0
        {
            summary.impacted_windows += 1;
        }
        if window_had_accepted_alert_step {
            summary.accepted_alert_windows += 1;
        }
        results.push(result);
    }

    (summary, results)
}

fn render_inventory_replay_simulation_report(
    summary: &InventoryReplaySimulationSummary,
    windows: &[WindowSimulationResult],
    simulation: InventoryReplaySimulationConfig,
) -> String {
    let mut output = String::new();
    let accepted_share = if summary.total_trade_events == 0 {
        None
    } else {
        Some(
            (Decimal::from(summary.accepted_trade_events as u64)
                / Decimal::from(summary.total_trade_events as u64)
                * Decimal::from(100_u32))
            .round_dp(2),
        )
    };

    let _ = writeln!(output, "Replay cap/cooldown simulation");
    let _ = writeln!(
        output,
        "Caps: gross<={} shares, delta<={} shares, cooldown={}s",
        simulation.max_gross_window_shares.round_dp(2),
        simulation.max_directional_delta_shares.round_dp(2),
        simulation.cooldown_secs
    );
    let _ = writeln!(output, "Окон: {}", summary.windows);
    let _ = writeln!(output, "Trade events: {}", summary.total_trade_events);
    let _ = writeln!(
        output,
        "Accepted trades: {} ({})",
        summary.accepted_trade_events,
        option_decimal_string(accepted_share)
    );
    let _ = writeln!(
        output,
        "Blocked by gross cap: {}",
        summary.blocked_by_gross_cap
    );
    let _ = writeln!(
        output,
        "Blocked by directional cap: {}",
        summary.blocked_by_directional_cap
    );
    let _ = writeln!(
        output,
        "Blocked by cooldown: {}",
        summary.blocked_by_cooldown
    );
    let _ = writeln!(
        output,
        "Cooldown activations: {}",
        summary.cooldown_activations
    );
    let _ = writeln!(output, "Impacted windows: {}", summary.impacted_windows);
    let _ = writeln!(
        output,
        "Accepted alert events: {}",
        summary.accepted_alert_events
    );
    let _ = writeln!(
        output,
        "Accepted alert steps: {}",
        summary.accepted_alert_steps
    );
    let _ = writeln!(
        output,
        "Accepted alert windows: {}",
        summary.accepted_alert_windows
    );
    let _ = writeln!(
        output,
        "Accepted cooldown alerts: {}",
        summary.accepted_cooldown_alerts
    );
    let _ = writeln!(
        output,
        "Accepted cooldown steps: {}",
        summary.accepted_cooldown_steps
    );
    let _ = writeln!(
        output,
        "Accepted late alerts: {}",
        summary.accepted_late_alerts
    );
    let _ = writeln!(
        output,
        "Accepted late steps: {}",
        summary.accepted_late_steps
    );
    let _ = writeln!(
        output,
        "Accepted crossed clusters: {}",
        summary.accepted_crossed_cluster_alerts
    );
    let _ = writeln!(
        output,
        "Accepted crossed-cluster steps: {}",
        summary.accepted_crossed_cluster_steps
    );

    let mut top_impacted = windows.to_vec();
    top_impacted.sort_by_key(|window| {
        Reverse(
            window.blocked_by_gross_cap
                + window.blocked_by_directional_cap
                + window.blocked_by_cooldown,
        )
    });
    let _ = writeln!(output);
    let _ = writeln!(output, "Most impacted windows");
    let _ = writeln!(
        output,
        "slug | trades | accepted | gross_block | delta_block | cooldown_block | cooldowns"
    );
    for window in top_impacted
        .iter()
        .filter(|window| {
            window.blocked_by_gross_cap
                + window.blocked_by_directional_cap
                + window.blocked_by_cooldown
                > 0
        })
        .take(10)
    {
        let _ = writeln!(
            output,
            "{} | {} | {} | {} | {} | {} | {}",
            window.slug,
            window.trade_events,
            window.accepted_trade_events,
            window.blocked_by_gross_cap,
            window.blocked_by_directional_cap,
            window.blocked_by_cooldown,
            window.cooldown_activations,
        );
    }

    output
}

fn render_wallet_inventory_window_report(
    window: &InventoryReplayWindow,
    event_limit: usize,
) -> String {
    let mut output = String::new();
    let final_step = window.final_step();
    let alerts = window.alerts(InventoryReplayAlertThresholds::default());

    let _ = writeln!(output, "Replay window");
    let _ = writeln!(output, "Slug: {}", window.slug);
    let _ = writeln!(
        output,
        "Период: {} -> {}",
        format_unix_secs_local(window.started_at),
        format_unix_secs_local(window.ended_at)
    );
    let _ = writeln!(output, "TRADE событий: {}", window.trade_events());
    let _ = writeln!(output, "REDEEM событий: {}", window.redeem_events());
    let _ = writeln!(
        output,
        "Trade volume USDC: {}",
        window.trade_volume_usdc().round_dp(4)
    );
    let _ = writeln!(
        output,
        "Redeem volume USDC: {}",
        window.redeem_volume_usdc().round_dp(4)
    );
    let _ = writeln!(
        output,
        "Peak gross inventory shares: {}",
        window.peak_gross_inventory_shares().round_dp(4)
    );
    if let Some(final_step) = final_step {
        let _ = writeln!(
            output,
            "Final gross inventory shares: {}",
            final_step.gross_inventory_shares.round_dp(4)
        );
        let _ = writeln!(
            output,
            "Final directional delta shares: {}",
            final_step.directional_delta_shares.round_dp(4)
        );
        let _ = writeln!(
            output,
            "Final hedged share: {}",
            option_decimal_string(final_step.hedged_share_pct)
        );
        let _ = writeln!(
            output,
            "Final dominant outcome: {}",
            empty_dash(&final_step.dominant_outcome)
        );
    }

    let _ = writeln!(output, "Replay alerts: {}", alerts.len());
    if !alerts.is_empty() {
        let _ = writeln!(output);
        let _ = writeln!(output, "Alerts");
        let _ = writeln!(
            output,
            "time | kind | gross | delta | hedged | left_s | note"
        );
        for alert in &alerts {
            let _ = writeln!(
                output,
                "{} | {} | {} | {} | {} | {} | {}",
                format_unix_secs_short(alert.activity_timestamp),
                replay_alert_kind_label(alert.kind),
                alert.gross_inventory_shares.round_dp(4),
                alert.directional_delta_shares.round_dp(4),
                option_decimal_string(alert.hedged_share_pct),
                option_i64_string(alert.seconds_left_at_observed),
                alert.note,
            );
        }
    }

    let mut steps_to_show = window.steps.iter().collect::<Vec<_>>();
    if steps_to_show.len() > event_limit {
        let keep_from = steps_to_show.len().saturating_sub(event_limit);
        steps_to_show = steps_to_show.split_off(keep_from);
    }

    let _ = writeln!(output);
    let _ = writeln!(output, "Timeline");
    let _ = writeln!(
        output,
        "time | type | side | outcome | usdc | shares | gross | delta | hedged | dom | exec | disc_bps | gap_bps | left_s"
    );
    for step in steps_to_show {
        let _ = writeln!(
            output,
            "{} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {}",
            format_unix_secs_short(step.activity_timestamp),
            step.activity_type,
            empty_dash(&step.side),
            empty_dash(&step.outcome),
            step.usdc_size.round_dp(4),
            option_decimal_string(step.inferred_shares),
            step.gross_inventory_shares.round_dp(4),
            step.directional_delta_shares.round_dp(4),
            option_decimal_string(step.hedged_share_pct),
            empty_dash(&step.dominant_outcome),
            execution_heuristic_label(step.execution_heuristic),
            option_decimal_string(step.selected_trade_discount_to_ask_bps),
            option_decimal_string(step.target_gap_bps),
            option_i64_string(step.seconds_left_at_observed),
        );
    }

    output
}

fn decimal_quantile(values: &[Decimal], q: f64) -> Option<Decimal> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let index = ((sorted.len() - 1) as f64 * q).round() as usize;
    sorted.get(index).copied()
}

fn i64_quantile(values: &[i64], q: f64) -> Option<i64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let index = ((sorted.len() - 1) as f64 * q).round() as usize;
    sorted.get(index).copied()
}

fn usize_quantile(values: &[usize], q: f64) -> Option<usize> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let index = ((sorted.len() - 1) as f64 * q).round() as usize;
    sorted.get(index).copied()
}

fn round_decimal_to_step(value: Decimal, step: Decimal) -> Decimal {
    if step <= Decimal::ZERO {
        value
    } else {
        ((value / step).round() * step).round_dp(2)
    }
}

fn default_replay_export_path(input_path: &Path) -> std::path::PathBuf {
    input_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("wallet_activity_replay_export.json")
}

fn build_replay_window_summaries(
    records: &[WalletActivitySnapshotRecord],
) -> Vec<ReplayWindowSummary> {
    let mut by_slug = BTreeMap::<String, ReplayWindowSummary>::new();

    for record in records {
        let summary = by_slug.entry(record.slug.clone()).or_insert_with(|| {
            ReplayWindowSummary::new(record.slug.clone(), record.activity_timestamp)
        });
        summary.started_at = summary.started_at.min(record.activity_timestamp);
        summary.ended_at = summary.ended_at.max(record.activity_timestamp);

        let activity_type = normalized_activity_type(&record.activity_type);
        let usdc = record.usdc_size.unwrap_or(Decimal::ZERO);
        if activity_type == "REDEEM" {
            summary.redeem_events += 1;
            summary.redeem_usdc += usdc;
            continue;
        }

        summary.trade_events += 1;
        summary.trade_usdc += usdc;
        if wallet_side_is_buy(&record.side) {
            summary.buy_trades += 1;
        } else if wallet_side_is_sell(&record.side) {
            summary.sell_trades += 1;
        }

        let shares = inferred_shares(record).unwrap_or(Decimal::ZERO);
        if outcome_side_is_up(&record.outcome) {
            if wallet_side_is_sell(&record.side) {
                summary.net_up_shares -= shares;
            } else if wallet_side_is_buy(&record.side) {
                summary.net_up_shares += shares;
            }
        } else if outcome_side_is_down(&record.outcome) {
            if wallet_side_is_sell(&record.side) {
                summary.net_down_shares -= shares;
            } else if wallet_side_is_buy(&record.side) {
                summary.net_down_shares += shares;
            }
        }

        let gross_inventory = summary.final_gross_inventory_shares();
        let directional_delta = summary.final_directional_delta_shares();
        summary.max_gross_inventory_shares =
            summary.max_gross_inventory_shares.max(gross_inventory);
        summary.max_directional_delta_shares =
            summary.max_directional_delta_shares.max(directional_delta);

        let dominant =
            dominant_outcome_from_net_shares(summary.net_up_shares, summary.net_down_shares);
        if !summary.previous_dominant.is_empty()
            && !dominant.is_empty()
            && summary.previous_dominant != dominant
        {
            summary.dominant_switches += 1;
        }
        if !dominant.is_empty() {
            summary.previous_dominant = dominant;
        }

        if let Some(previous_ts) = summary.last_trade_ts {
            let delta_secs = (record.activity_timestamp - previous_ts).max(0);
            summary.trade_spacing_sum_secs += Decimal::from(delta_secs);
            summary.trade_spacing_count += 1;
        }
        summary.last_trade_ts = Some(record.activity_timestamp);

        match execution_heuristic(record.selected_trade_discount_to_ask_bps) {
            ExecutionHeuristic::MakerLike => summary.maker_like_trades += 1,
            ExecutionHeuristic::Neutral => summary.neutral_execution_trades += 1,
            ExecutionHeuristic::CrossedOrStale => summary.crossed_or_stale_trades += 1,
            ExecutionHeuristic::Unknown => summary.unknown_execution_trades += 1,
        }
    }

    by_slug.into_values().collect()
}

fn execution_heuristic(discount_to_ask_bps: Option<Decimal>) -> ExecutionHeuristic {
    match discount_to_ask_bps {
        Some(discount) if discount >= Decimal::from(15_u32) => ExecutionHeuristic::MakerLike,
        Some(discount) if discount <= Decimal::from(-15_i32) => ExecutionHeuristic::CrossedOrStale,
        Some(_) => ExecutionHeuristic::Neutral,
        None => ExecutionHeuristic::Unknown,
    }
}

fn normalized_activity_type(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        "TRADE".to_owned()
    } else {
        trimmed.to_ascii_uppercase()
    }
}

fn inventory_balance_ratio_string(gross_inventory: Decimal, directional_delta: Decimal) -> String {
    hedged_share_pct(gross_inventory, directional_delta)
        .map_or_else(|| "-".to_owned(), |value| format!("{}%", value.round_dp(2)))
}

fn hedged_share_pct(gross_inventory: Decimal, directional_delta: Decimal) -> Option<Decimal> {
    if gross_inventory <= Decimal::ZERO {
        None
    } else {
        Some(
            ((gross_inventory - directional_delta.max(Decimal::ZERO)) / gross_inventory
                * Decimal::from(100_u32))
            .round_dp(2),
        )
    }
}

fn share_pct_string(part: usize, total: usize) -> String {
    if total == 0 {
        "-".to_owned()
    } else {
        (Decimal::from(part as u64) / Decimal::from(total as u64) * Decimal::from(100_u32))
            .round_dp(2)
            .to_string()
            + "%"
    }
}

fn option_decimal_string(value: Option<Decimal>) -> String {
    value.map_or_else(|| "-".to_owned(), |value| value.round_dp(4).to_string())
}

fn option_i64_string(value: Option<i64>) -> String {
    value.map_or_else(|| "-".to_owned(), |value| value.to_string())
}

fn empty_dash(value: &str) -> &str {
    if value.is_empty() { "-" } else { value }
}

fn execution_heuristic_label(value: ExecutionHeuristic) -> &'static str {
    match value {
        ExecutionHeuristic::MakerLike => "maker",
        ExecutionHeuristic::Neutral => "neutral",
        ExecutionHeuristic::CrossedOrStale => "crossed",
        ExecutionHeuristic::Unknown => "unknown",
    }
}

fn replay_alert_kind_label(value: InventoryReplayAlertKind) -> &'static str {
    match value {
        InventoryReplayAlertKind::InventoryImbalance => "imbalance",
        InventoryReplayAlertKind::LateWindowExpansion => "late_expansion",
        InventoryReplayAlertKind::AdverseExecutionCluster => "crossed_cluster",
        InventoryReplayAlertKind::CooldownCandidate => "cooldown",
    }
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

fn outcome_side_is_up(value: &str) -> bool {
    outcome_label_is_up(value)
}

fn outcome_side_is_down(value: &str) -> bool {
    outcome_label_is_down(value)
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

fn format_unix_secs_short(timestamp: i64) -> String {
    DateTime::<Utc>::from_timestamp(timestamp, 0).map_or_else(
        || timestamp.to_string(),
        |value| value.with_timezone(&Local).format("%H:%M:%S").to_string(),
    )
}
