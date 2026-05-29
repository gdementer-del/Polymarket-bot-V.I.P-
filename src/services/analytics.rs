//! Analytics derived from the local execution journal.

use std::collections::{BTreeMap, HashMap};

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

use super::binance::{BtcFiveMinuteResolution, WindowDirection};
use super::journal::{JournalEntry, PnlSnapshot};
use super::labels::{outcome_label_is_down, outcome_label_is_flat, outcome_label_is_up};

/// Aggregated execution metrics for the local trading journal.
#[derive(Debug, Clone)]
pub struct AnalyticsReport {
    pub execution_count_total: u64,
    pub execution_count_sampled: usize,
    pub unique_market_windows: usize,
    pub current_open_notional: Decimal,
    pub total_spent_usdc: Decimal,
    pub total_expected_profit: Decimal,
    pub realized_profit_resolved: Decimal,
    pub average_edge_bps: Decimal,
    pub average_spot_move_bps: Decimal,
    pub average_bundle_cost: Decimal,
    pub average_realized_move_bps: Decimal,
    pub resolved_execution_count: usize,
    pub pending_resolution_count: usize,
    pub signal_accuracy_pct: Decimal,
    pub dominant_outcome_distribution: BTreeMap<String, usize>,
    pub actual_outcome_distribution: BTreeMap<String, usize>,
    pub last_execution_at: Option<DateTime<Utc>>,
}

impl AnalyticsReport {
    /// Build analytics from journal entries, persisted state, and resolved window outcomes.
    #[must_use]
    pub fn from_entries(
        entries: &[JournalEntry],
        snapshot: &PnlSnapshot,
        resolutions: &HashMap<String, BtcFiveMinuteResolution>,
    ) -> Self {
        let execution_count_sampled = entries.len();
        let sampled_count_decimal = Decimal::from(execution_count_sampled as u64);
        let total_edge_bps = entries.iter().fold(Decimal::ZERO, |total, entry| {
            total + Decimal::from(entry.opportunity.edge_bps)
        });
        let total_spot_move_bps = entries.iter().fold(Decimal::ZERO, |total, entry| {
            total + entry.opportunity.spot_move_bps.abs()
        });
        let total_bundle_cost = entries.iter().fold(Decimal::ZERO, |total, entry| {
            total + entry.opportunity.bundle_cost
        });

        let dominant_outcome_distribution = entries.iter().fold(
            BTreeMap::<String, usize>::new(),
            |mut distribution, entry| {
                *distribution
                    .entry(entry.opportunity.dominant_outcome.clone())
                    .or_default() += 1;
                distribution
            },
        );

        let (
            resolved_execution_count,
            signal_match_count,
            total_realized_move_bps,
            realized_profit_resolved,
            actual_outcome_distribution,
        ) = entries.iter().fold(
            (
                0_usize,
                0_usize,
                Decimal::ZERO,
                Decimal::ZERO,
                BTreeMap::<String, usize>::new(),
            ),
            |(
                resolved_execution_count,
                signal_match_count,
                total_realized_move_bps,
                realized_profit_resolved,
                mut actual_outcome_distribution,
            ),
             entry| {
                let Some(resolution) = resolutions.get(&entry.opportunity.slug) else {
                    return (
                        resolved_execution_count,
                        signal_match_count,
                        total_realized_move_bps,
                        realized_profit_resolved,
                        actual_outcome_distribution,
                    );
                };

                *actual_outcome_distribution
                    .entry(resolution.actual_outcome.as_str().to_owned())
                    .or_default() += 1;

                (
                    resolved_execution_count + 1,
                    signal_match_count
                        + usize::from(outcome_label_matches_direction(
                            &entry.opportunity.dominant_outcome,
                            resolution.actual_outcome,
                        )),
                    total_realized_move_bps + resolution.realized_move_bps.abs(),
                    realized_profit_resolved + entry.report.expected_profit,
                    actual_outcome_distribution,
                )
            },
        );

        let last_execution_at = entries.iter().map(|entry| entry.recorded_at).max();
        let current_open_notional = snapshot
            .paper_state
            .market_notional
            .values()
            .copied()
            .sum::<Decimal>();
        let resolved_count_decimal = Decimal::from(resolved_execution_count as u64);

        Self {
            execution_count_total: snapshot.execution_count,
            execution_count_sampled,
            unique_market_windows: snapshot.executed_market_slugs.len(),
            current_open_notional,
            total_spent_usdc: snapshot.paper_state.total_spent_usdc,
            total_expected_profit: snapshot.paper_state.total_expected_profit,
            realized_profit_resolved: realized_profit_resolved.round_dp(6),
            average_edge_bps: average_or_zero(total_edge_bps, sampled_count_decimal),
            average_spot_move_bps: average_or_zero(total_spot_move_bps, sampled_count_decimal),
            average_bundle_cost: average_or_zero(total_bundle_cost, sampled_count_decimal),
            average_realized_move_bps: average_or_zero(
                total_realized_move_bps,
                resolved_count_decimal,
            ),
            resolved_execution_count,
            pending_resolution_count: execution_count_sampled
                .saturating_sub(resolved_execution_count),
            signal_accuracy_pct: percentage_or_zero(signal_match_count, resolved_execution_count),
            dominant_outcome_distribution,
            actual_outcome_distribution,
            last_execution_at,
        }
    }
}

fn average_or_zero(total: Decimal, count: Decimal) -> Decimal {
    if count.is_zero() {
        Decimal::ZERO
    } else {
        (total / count).round_dp(4)
    }
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

fn outcome_label_matches_direction(label: &str, actual_outcome: WindowDirection) -> bool {
    match actual_outcome {
        WindowDirection::Up => outcome_label_is_up(label),
        WindowDirection::Down => outcome_label_is_down(label),
        WindowDirection::Flat => outcome_label_is_flat(label),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use chrono::Utc;
    use rust_decimal::Decimal;

    use crate::models::MarketTarget;
    use crate::models::{ExecutionReport, Opportunity, OpportunityKind, PaperState};
    use crate::services::binance::{BtcFiveMinuteResolution, WindowDirection};

    use super::{AnalyticsReport, JournalEntry, PnlSnapshot};

    fn build_entry(slug: &str, dominant_outcome: &str, edge_bps: u32) -> JournalEntry {
        JournalEntry {
            recorded_at: Utc::now(),
            opportunity: Opportunity {
                kind: OpportunityKind::BundleArbitrage,
                condition_id: slug.to_owned(),
                slug: slug.to_owned(),
                question: slug.to_owned(),
                outcome_a_label: "Up".to_owned(),
                outcome_a_token_id: "up-token".to_owned(),
                outcome_b_label: "Down".to_owned(),
                outcome_b_token_id: "down-token".to_owned(),
                liquidity_usdc: Decimal::from(1_000_u32),
                outcome_a_ask_price: Decimal::new(46, 2),
                outcome_b_ask_price: Decimal::new(47, 2),
                bundle_cost: Decimal::new(93, 2),
                net_bundle_cost: Decimal::new(93, 2),
                edge_per_share: Decimal::new(7, 2),
                edge_bps,
                tradable_shares: Decimal::new(10, 0),
                required_usdc: Decimal::new(93, 1),
                expected_payout: Decimal::new(10, 0),
                expected_profit: Decimal::new(7, 1),
                interval_open_price: Decimal::from(67_000_u32),
                target_price: Decimal::from(67_000_u32),
                target_price_source: crate::models::TargetPriceSource::BinanceWindowOpenFallback,
                target_gap_bps: Decimal::new(75, 1),
                current_spot_price: Decimal::from(67_050_u32),
                spot_move_bps: Decimal::new(75, 1),
                spot_move_1s_bps: Decimal::ZERO,
                spot_move_5s_bps: Decimal::ZERO,
                spot_move_15s_bps: Decimal::ZERO,
                micro_acceleration_bps: Decimal::ZERO,
                micro_burst_reference_price: Decimal::from(67_050_u32),
                micro_reference_price: Decimal::from(67_050_u32),
                signal_strength_bps: Decimal::ZERO,
                aligned_trade_flow_bps: Decimal::ZERO,
                signal_tier: "bundle".to_owned(),
                target_cross_label: "none".to_owned(),
                dominant_outcome: dominant_outcome.to_owned(),
                primary_outcome_label: dominant_outcome.to_owned(),
                primary_outcome_token_id: "up-token".to_owned(),
                primary_outcome_ask_price: Decimal::new(46, 2),
                primary_fill_levels: Vec::new(),
                hedge_outcome_label: None,
                hedge_outcome_token_id: None,
                hedge_outcome_ask_price: None,
                hedge_fill_levels: Vec::new(),
                hedge_shares: Decimal::ZERO,
                seconds_left: 120,
                note: "test".to_owned(),
            },
            report: ExecutionReport {
                mode: "paper".to_owned(),
                action: "open".to_owned(),
                slug: slug.to_owned(),
                condition_id: slug.to_owned(),
                question: slug.to_owned(),
                shares: Decimal::new(10, 0),
                spent_usdc: Decimal::new(93, 1),
                expected_profit: Decimal::new(7, 1),
                details: "paper fill".to_owned(),
            },
            paper_state: PaperState::default(),
        }
    }

    #[test]
    fn analytics_report_computes_realized_metrics() {
        let entries = vec![
            build_entry("btc-updown-5m-1", "Up", 700),
            build_entry("btc-updown-5m-2", "Down", 500),
        ];
        let snapshot = PnlSnapshot {
            updated_at: Utc::now(),
            execution_count: 2,
            paper_state: PaperState {
                market_notional: HashMap::from([
                    ("btc-updown-5m-1".to_owned(), Decimal::new(93, 1)),
                    ("btc-updown-5m-2".to_owned(), Decimal::new(93, 1)),
                ]),
                open_positions: HashMap::new(),
                total_spent_usdc: Decimal::new(186, 1),
                total_fees_usdc: Decimal::ZERO,
                total_slippage_cost_usdc: Decimal::ZERO,
                total_expected_profit: Decimal::new(14, 1),
                total_realized_payout: Decimal::ZERO,
                total_realized_profit: Decimal::ZERO,
                closed_position_count: 0,
            },
            executed_market_slugs: HashSet::from([
                "btc-updown-5m-1".to_owned(),
                "btc-updown-5m-2".to_owned(),
            ]),
        };
        let resolutions = HashMap::from([
            (
                "btc-updown-5m-1".to_owned(),
                BtcFiveMinuteResolution {
                    target: MarketTarget::Btc5m,
                    start_price: Decimal::from(67_000_u32),
                    end_price: Decimal::from(67_050_u32),
                    realized_move_bps: Decimal::new(75, 1),
                    actual_outcome: WindowDirection::Up,
                    resolved_at_ms: 1,
                },
            ),
            (
                "btc-updown-5m-2".to_owned(),
                BtcFiveMinuteResolution {
                    target: MarketTarget::Btc5m,
                    start_price: Decimal::from(67_050_u32),
                    end_price: Decimal::from(67_000_u32),
                    realized_move_bps: Decimal::new(75, 1),
                    actual_outcome: WindowDirection::Down,
                    resolved_at_ms: 2,
                },
            ),
        ]);

        let report = AnalyticsReport::from_entries(&entries, &snapshot, &resolutions);

        assert_eq!(report.execution_count_total, 2);
        assert_eq!(report.execution_count_sampled, 2);
        assert_eq!(report.unique_market_windows, 2);
        assert_eq!(report.average_edge_bps, Decimal::from(600_u32));
        assert_eq!(report.average_spot_move_bps, Decimal::new(75, 1));
        assert_eq!(report.average_realized_move_bps, Decimal::new(75, 1));
        assert_eq!(report.resolved_execution_count, 2);
        assert_eq!(report.pending_resolution_count, 0);
        assert_eq!(report.signal_accuracy_pct, Decimal::from(100_u32));
        assert_eq!(report.realized_profit_resolved, Decimal::new(14, 1));
        assert_eq!(report.actual_outcome_distribution["Down"], 1);
        assert_eq!(report.actual_outcome_distribution["Up"], 1);
    }
}
