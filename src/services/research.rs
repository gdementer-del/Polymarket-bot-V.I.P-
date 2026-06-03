//! Comparative research helpers for replay export sessions.
#![allow(clippy::missing_errors_doc, clippy::too_many_lines)]

use std::cmp::Reverse;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use rust_decimal::Decimal;
use tracing::info;

use crate::error::Result;

use super::inventory::{
    InventoryReplayAlertKind, InventoryReplayExport, InventoryReplaySimulationConfig,
    InventoryReplaySimulationSummary, calibrate_inventory_replay_alert_thresholds,
    load_inventory_replay_export, simulate_inventory_caps,
};

#[derive(Debug, Clone)]
struct ReplaySessionComparison {
    label: String,
    windows: usize,
    trade_events: usize,
    redeem_events: usize,
    trade_volume_usdc: Decimal,
    redeem_volume_usdc: Decimal,
    final_gross_inventory_shares: Decimal,
    final_directional_delta_shares: Decimal,
    final_hedged_share_pct: Option<Decimal>,
    maker_like_share_pct: Option<Decimal>,
    crossed_or_stale_share_pct: Option<Decimal>,
    alert_count: usize,
    cooldown_candidates: usize,
    late_expansions: usize,
    adverse_clusters: usize,
    balanced_windows: usize,
    two_sided_windows: usize,
}

#[derive(Debug, Clone)]
struct ReplayAutotuneCandidate {
    gross_cap: Decimal,
    delta_cap: Decimal,
    cooldown_secs: i64,
    sessions: usize,
    total_windows: usize,
    total_trades: usize,
    accepted_trades: usize,
    blocked_trades: usize,
    impacted_windows: usize,
    cooldown_activations: usize,
    accepted_alert_events: usize,
    accepted_alert_steps: usize,
    accepted_alert_windows: usize,
    accepted_cooldown_alerts: usize,
    accepted_cooldown_steps: usize,
    accepted_late_alerts: usize,
    accepted_late_steps: usize,
    accepted_crossed_cluster_alerts: usize,
    accepted_crossed_cluster_steps: usize,
    score: Decimal,
}

#[derive(Debug, Clone)]
struct ReplayAlertCalibration {
    sessions: usize,
    imbalance_samples: usize,
    severe_imbalance_samples: usize,
    late_growth_samples: usize,
    crossed_window_samples: usize,
    recommended_thresholds: super::inventory::InventoryReplayAlertThresholds,
}

pub fn show_replay_export_comparison(inputs: &[PathBuf], top: usize) -> Result<()> {
    let mut sessions = Vec::new();
    for input in inputs {
        let export = load_inventory_replay_export(input)?;
        sessions.push(summarize_export(input, &export));
    }
    info!(
        sessions = sessions.len(),
        "loaded multiple replay-export sessions for comparison"
    );
    info!(
        "\n{}",
        render_replay_export_comparison(&sessions, top.max(1))
    );
    Ok(())
}

pub fn show_replay_export_autotune(
    inputs: &[PathBuf],
    gross_values: &[u32],
    delta_values: &[u32],
    cooldown_values: &[i64],
    top: usize,
) -> Result<()> {
    let exports = inputs
        .iter()
        .map(|input| load_inventory_replay_export(input).map(|export| (input, export)))
        .collect::<Result<Vec<_>>>()?;
    let calibration_inputs = exports
        .iter()
        .map(|(_, export)| export.clone())
        .collect::<Vec<_>>();
    let calibrated_thresholds = calibrate_inventory_replay_alert_thresholds(&calibration_inputs);
    let mut candidates = Vec::new();

    for &gross in gross_values {
        for &delta in delta_values {
            if delta > gross {
                continue;
            }
            for &cooldown in cooldown_values {
                let mut candidate = ReplayAutotuneCandidate {
                    gross_cap: Decimal::from(gross),
                    delta_cap: Decimal::from(delta),
                    cooldown_secs: cooldown,
                    sessions: exports.len(),
                    total_windows: 0,
                    total_trades: 0,
                    accepted_trades: 0,
                    blocked_trades: 0,
                    impacted_windows: 0,
                    cooldown_activations: 0,
                    accepted_alert_events: 0,
                    accepted_alert_steps: 0,
                    accepted_alert_windows: 0,
                    accepted_cooldown_alerts: 0,
                    accepted_cooldown_steps: 0,
                    accepted_late_alerts: 0,
                    accepted_late_steps: 0,
                    accepted_crossed_cluster_alerts: 0,
                    accepted_crossed_cluster_steps: 0,
                    score: Decimal::ZERO,
                };

                for (_, export) in &exports {
                    let mut calibrated_export = export.clone();
                    calibrated_export.thresholds = calibrated_thresholds;
                    let (summary, _) = simulate_inventory_caps(
                        &calibrated_export,
                        InventoryReplaySimulationConfig {
                            max_gross_window_shares: Decimal::from(gross),
                            max_directional_delta_shares: Decimal::from(delta),
                            cooldown_secs: cooldown,
                            trigger_on_cooldown_alert: true,
                            trigger_on_late_expansion: true,
                        },
                    );
                    accumulate_candidate(&mut candidate, &summary);
                }

                candidate.score = score_candidate(&candidate);
                candidates.push(candidate);
            }
        }
    }

    candidates.sort_by(|left, right| {
        right.score.cmp(&left.score).then_with(|| {
            right
                .accepted_share_pct()
                .cmp(&left.accepted_share_pct())
                .then_with(|| {
                    left.accepted_alert_step_share_pct()
                        .cmp(&right.accepted_alert_step_share_pct())
                })
        })
    });

    info!(
        sessions = exports.len(),
        candidates = candidates.len(),
        "completed v4 replay autotune grid search"
    );
    info!(
        "\n{}",
        render_replay_autotune_report(&candidates, top.max(1))
    );
    Ok(())
}

pub fn show_replay_alert_calibration(inputs: &[PathBuf]) -> Result<()> {
    let exports = inputs
        .iter()
        .map(|input| load_inventory_replay_export(input))
        .collect::<Result<Vec<_>>>()?;
    let (imbalance_samples, severe_imbalance_samples, late_growth_samples, crossed_window_samples) =
        replay_alert_calibration_sample_counts(&exports);
    let calibration = ReplayAlertCalibration {
        sessions: exports.len(),
        imbalance_samples,
        severe_imbalance_samples,
        late_growth_samples,
        crossed_window_samples,
        recommended_thresholds: calibrate_inventory_replay_alert_thresholds(&exports),
    };
    info!(
        sessions = calibration.sessions,
        "completed heuristic replay alert-threshold calibration"
    );
    info!("\n{}", render_alert_calibration_report(&calibration));
    Ok(())
}

fn summarize_export(path: &Path, export: &InventoryReplayExport) -> ReplaySessionComparison {
    let trade_events = export
        .windows
        .iter()
        .map(|window| window.trade_events)
        .sum::<usize>();
    let redeem_events = export
        .windows
        .iter()
        .map(|window| window.redeem_events)
        .sum::<usize>();
    let trade_volume_usdc = export
        .windows
        .iter()
        .fold(Decimal::ZERO, |sum, window| sum + window.trade_volume_usdc);
    let redeem_volume_usdc = export
        .windows
        .iter()
        .fold(Decimal::ZERO, |sum, window| sum + window.redeem_volume_usdc);
    let final_gross_inventory_shares = export.windows.iter().fold(Decimal::ZERO, |sum, window| {
        sum + window.final_gross_inventory_shares
    });
    let final_directional_delta_shares =
        export.windows.iter().fold(Decimal::ZERO, |sum, window| {
            sum + window.final_directional_delta_shares
        });
    let final_hedged_share_pct = if final_gross_inventory_shares <= Decimal::ZERO {
        None
    } else {
        Some(
            ((final_gross_inventory_shares - final_directional_delta_shares.max(Decimal::ZERO))
                / final_gross_inventory_shares
                * Decimal::from(100_u32))
            .round_dp(2),
        )
    };

    let mut maker_like = 0_usize;
    let mut crossed = 0_usize;
    let mut priced_steps = 0_usize;
    let mut alert_count = 0_usize;
    let mut cooldown_candidates = 0_usize;
    let mut late_expansions = 0_usize;
    let mut adverse_clusters = 0_usize;
    let mut balanced_windows = 0_usize;
    let mut two_sided_windows = 0_usize;
    for window in &export.windows {
        let alerts = window.alerts_with_thresholds(export.thresholds);
        if window.final_gross_inventory_shares > Decimal::ZERO
            && window.final_directional_delta_shares
                <= (window.final_gross_inventory_shares * Decimal::new(20, 2)).round_dp(8)
        {
            balanced_windows += 1;
        }
        let final_up = window
            .steps
            .last()
            .map_or(Decimal::ZERO, |step| step.net_up_shares);
        let final_down = window
            .steps
            .last()
            .map_or(Decimal::ZERO, |step| step.net_down_shares);
        if final_up > Decimal::ZERO && final_down > Decimal::ZERO {
            two_sided_windows += 1;
        }

        for step in &window.steps {
            if step.activity_type == "REDEEM" {
                continue;
            }
            priced_steps += 1;
            match step.execution_heuristic {
                super::inventory::ExecutionHeuristic::MakerLike => maker_like += 1,
                super::inventory::ExecutionHeuristic::CrossedOrStale => crossed += 1,
                super::inventory::ExecutionHeuristic::Neutral
                | super::inventory::ExecutionHeuristic::Unknown => {}
            }
        }
        alert_count += alerts.len();
        cooldown_candidates += alerts
            .iter()
            .filter(|alert| alert.kind == InventoryReplayAlertKind::CooldownCandidate)
            .count();
        late_expansions += alerts
            .iter()
            .filter(|alert| alert.kind == InventoryReplayAlertKind::LateWindowExpansion)
            .count();
        adverse_clusters += alerts
            .iter()
            .filter(|alert| alert.kind == InventoryReplayAlertKind::AdverseExecutionCluster)
            .count();
    }

    let maker_like_share_pct = if priced_steps == 0 {
        None
    } else {
        Some(
            (Decimal::from(maker_like as u64) / Decimal::from(priced_steps as u64)
                * Decimal::from(100_u32))
            .round_dp(2),
        )
    };
    let crossed_or_stale_share_pct = if priced_steps == 0 {
        None
    } else {
        Some(
            (Decimal::from(crossed as u64) / Decimal::from(priced_steps as u64)
                * Decimal::from(100_u32))
            .round_dp(2),
        )
    };

    ReplaySessionComparison {
        label: path.parent().and_then(|path| path.file_name()).map_or_else(
            || path.display().to_string(),
            |name| name.to_string_lossy().into_owned(),
        ),
        windows: export.windows.len(),
        trade_events,
        redeem_events,
        trade_volume_usdc,
        redeem_volume_usdc,
        final_gross_inventory_shares,
        final_directional_delta_shares,
        final_hedged_share_pct,
        maker_like_share_pct,
        crossed_or_stale_share_pct,
        alert_count,
        cooldown_candidates,
        late_expansions,
        adverse_clusters,
        balanced_windows,
        two_sided_windows,
    }
}

fn render_replay_export_comparison(sessions: &[ReplaySessionComparison], top: usize) -> String {
    let mut output = String::new();
    let _ = writeln!(output, "Replay export comparison");
    let _ = writeln!(output, "Sessions: {}", sessions.len());
    let _ = writeln!(output);
    let _ = writeln!(
        output,
        "session | windows | trades | redeem | trade_usdc | redeem_usdc | hedged | maker | crossed | alerts | cooldowns"
    );
    for session in sessions {
        let _ = writeln!(
            output,
            "{} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {}",
            session.label,
            session.windows,
            session.trade_events,
            session.redeem_events,
            session.trade_volume_usdc.round_dp(4),
            session.redeem_volume_usdc.round_dp(4),
            option_decimal_string(session.final_hedged_share_pct),
            option_decimal_string(session.maker_like_share_pct),
            option_decimal_string(session.crossed_or_stale_share_pct),
            session.alert_count,
            session.cooldown_candidates,
        );
    }

    let mut top_by_volume = sessions.to_vec();
    top_by_volume.sort_by_key(|session| Reverse(session.trade_volume_usdc));
    let _ = writeln!(output);
    let _ = writeln!(output, "Top sessions by trade volume");
    for session in top_by_volume.iter().take(top) {
        let _ = writeln!(
            output,
            "- {}: volume={} windows={} hedged={} balanced={}/{} two_sided={} gross={} delta={}",
            session.label,
            session.trade_volume_usdc.round_dp(4),
            session.windows,
            option_decimal_string(session.final_hedged_share_pct),
            session.balanced_windows,
            session.windows,
            session.two_sided_windows,
            session.final_gross_inventory_shares.round_dp(2),
            session.final_directional_delta_shares.round_dp(2),
        );
    }

    let mut top_by_alerts = sessions.to_vec();
    top_by_alerts.sort_by_key(|session| Reverse(session.alert_count));
    let _ = writeln!(output);
    let _ = writeln!(output, "Top sessions by alerts");
    for session in top_by_alerts.iter().take(top) {
        let _ = writeln!(
            output,
            "- {}: alerts={} cooldowns={} late={} crossed_clusters={}",
            session.label,
            session.alert_count,
            session.cooldown_candidates,
            session.late_expansions,
            session.adverse_clusters,
        );
    }

    output
}

impl ReplayAutotuneCandidate {
    fn accepted_share_pct(&self) -> Decimal {
        if self.total_trades == 0 {
            Decimal::ZERO
        } else {
            (Decimal::from(self.accepted_trades as u64) / Decimal::from(self.total_trades as u64)
                * Decimal::from(100_u32))
            .round_dp(2)
        }
    }

    fn impacted_window_share_pct(&self) -> Decimal {
        if self.total_windows == 0 {
            Decimal::ZERO
        } else {
            let denominator = Decimal::from(self.total_windows as u64);
            (Decimal::from(self.impacted_windows as u64) / denominator * Decimal::from(100_u32))
                .round_dp(2)
        }
    }

    fn accepted_alert_step_share_pct(&self) -> Decimal {
        if self.accepted_trades == 0 {
            Decimal::ZERO
        } else {
            (Decimal::from(self.accepted_alert_steps as u64)
                / Decimal::from(self.accepted_trades as u64)
                * Decimal::from(100_u32))
            .round_dp(2)
        }
    }

    fn accepted_alert_window_share_pct(&self) -> Decimal {
        if self.total_windows == 0 {
            Decimal::ZERO
        } else {
            (Decimal::from(self.accepted_alert_windows as u64)
                / Decimal::from(self.total_windows as u64)
                * Decimal::from(100_u32))
            .round_dp(2)
        }
    }
}

fn accumulate_candidate(
    candidate: &mut ReplayAutotuneCandidate,
    summary: &InventoryReplaySimulationSummary,
) {
    candidate.total_windows += summary.windows;
    candidate.total_trades += summary.total_trade_events;
    candidate.accepted_trades += summary.accepted_trade_events;
    candidate.blocked_trades += summary.blocked_by_gross_cap
        + summary.blocked_by_directional_cap
        + summary.blocked_by_cooldown;
    candidate.impacted_windows += summary.impacted_windows;
    candidate.cooldown_activations += summary.cooldown_activations;
    candidate.accepted_alert_events += summary.accepted_alert_events;
    candidate.accepted_alert_steps += summary.accepted_alert_steps;
    candidate.accepted_alert_windows += summary.accepted_alert_windows;
    candidate.accepted_cooldown_alerts += summary.accepted_cooldown_alerts;
    candidate.accepted_cooldown_steps += summary.accepted_cooldown_steps;
    candidate.accepted_late_alerts += summary.accepted_late_alerts;
    candidate.accepted_late_steps += summary.accepted_late_steps;
    candidate.accepted_crossed_cluster_alerts += summary.accepted_crossed_cluster_alerts;
    candidate.accepted_crossed_cluster_steps += summary.accepted_crossed_cluster_steps;
}

fn score_candidate(candidate: &ReplayAutotuneCandidate) -> Decimal {
    let accepted_share = candidate.accepted_share_pct();
    let accepted_alert_step_share = candidate.accepted_alert_step_share_pct();
    let accepted_alert_window_share = candidate.accepted_alert_window_share_pct();
    let accepted_late_share = if candidate.accepted_trades == 0 {
        Decimal::ZERO
    } else {
        (Decimal::from(candidate.accepted_late_steps as u64)
            / Decimal::from(candidate.accepted_trades as u64)
            * Decimal::from(100_u32))
        .round_dp(2)
    };
    let accepted_cooldown_share = if candidate.accepted_trades == 0 {
        Decimal::ZERO
    } else {
        (Decimal::from(candidate.accepted_cooldown_steps as u64)
            / Decimal::from(candidate.accepted_trades as u64)
            * Decimal::from(100_u32))
        .round_dp(2)
    };
    let accepted_crossed_share = if candidate.accepted_trades == 0 {
        Decimal::ZERO
    } else {
        (Decimal::from(candidate.accepted_crossed_cluster_steps as u64)
            / Decimal::from(candidate.accepted_trades as u64)
            * Decimal::from(100_u32))
        .round_dp(2)
    };
    let impacted_window_share = candidate.impacted_window_share_pct();
    let blocked_share = if candidate.total_trades == 0 {
        Decimal::ZERO
    } else {
        (Decimal::from(candidate.blocked_trades as u64)
            / Decimal::from(candidate.total_trades as u64)
            * Decimal::from(100_u32))
        .round_dp(2)
    };
    let cooldown_penalty = if candidate.total_trades == 0 {
        Decimal::ZERO
    } else {
        (Decimal::from(candidate.cooldown_activations as u64)
            / Decimal::from(candidate.total_trades as u64)
            * Decimal::from(100_u32))
        .round_dp(4)
    };

    (accepted_share
        - accepted_alert_step_share * Decimal::new(6, 1)
        - accepted_alert_window_share * Decimal::new(2, 1)
        - accepted_late_share * Decimal::new(9, 1)
        - accepted_cooldown_share * Decimal::new(12, 1)
        - accepted_crossed_share * Decimal::new(25, 2)
        - impacted_window_share * Decimal::new(35, 2)
        - blocked_share * Decimal::new(15, 2)
        - cooldown_penalty * Decimal::new(1, 0))
    .round_dp(4)
}

fn render_replay_autotune_report(candidates: &[ReplayAutotuneCandidate], top: usize) -> String {
    let mut output = String::new();
    let _ = writeln!(output, "Replay autotune");
    let _ = writeln!(output, "Candidates: {}", candidates.len());
    let _ = writeln!(output);
    let _ = writeln!(
        output,
        "gross | delta | cooldown | score | accept% | alert_step% | alert_window% | late% | cooldown% | crossed% | impacted_windows% | cooldowns"
    );
    for candidate in candidates.iter().take(top) {
        let late_share = if candidate.accepted_trades == 0 {
            Decimal::ZERO
        } else {
            (Decimal::from(candidate.accepted_late_steps as u64)
                / Decimal::from(candidate.accepted_trades as u64)
                * Decimal::from(100_u32))
            .round_dp(2)
        };
        let cooldown_share = if candidate.accepted_trades == 0 {
            Decimal::ZERO
        } else {
            (Decimal::from(candidate.accepted_cooldown_steps as u64)
                / Decimal::from(candidate.accepted_trades as u64)
                * Decimal::from(100_u32))
            .round_dp(2)
        };
        let crossed_share = if candidate.accepted_trades == 0 {
            Decimal::ZERO
        } else {
            (Decimal::from(candidate.accepted_crossed_cluster_steps as u64)
                / Decimal::from(candidate.accepted_trades as u64)
                * Decimal::from(100_u32))
            .round_dp(2)
        };
        let _ = writeln!(
            output,
            "{} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {}",
            candidate.gross_cap.round_dp(2),
            candidate.delta_cap.round_dp(2),
            candidate.cooldown_secs,
            candidate.score.round_dp(4),
            candidate.accepted_share_pct(),
            candidate.accepted_alert_step_share_pct(),
            candidate.accepted_alert_window_share_pct(),
            late_share,
            cooldown_share,
            crossed_share,
            candidate.impacted_window_share_pct(),
            candidate.cooldown_activations,
        );
    }

    if let Some(best) = candidates.first() {
        let _ = writeln!(output);
        let _ = writeln!(output, "Best heuristic profile");
        let _ = writeln!(output, "- sessions: {}", best.sessions);
        let _ = writeln!(output, "- gross cap: {} shares", best.gross_cap.round_dp(2));
        let _ = writeln!(
            output,
            "- directional delta cap: {} shares",
            best.delta_cap.round_dp(2)
        );
        let _ = writeln!(output, "- cooldown: {}s", best.cooldown_secs);
        let _ = writeln!(output, "- accepted trades: {}%", best.accepted_share_pct());
        let _ = writeln!(
            output,
            "- accepted alert-step leakage: {}%",
            best.accepted_alert_step_share_pct()
        );
        let _ = writeln!(
            output,
            "- accepted alert-window leakage: {}%",
            best.accepted_alert_window_share_pct()
        );
        let _ = writeln!(
            output,
            "- impacted windows: {}%",
            best.impacted_window_share_pct()
        );
        let _ = writeln!(output, "- heuristic score: {}", best.score.round_dp(4));
    }

    output
}

fn replay_alert_calibration_sample_counts(
    exports: &[InventoryReplayExport],
) -> (usize, usize, usize, usize) {
    let mut imbalance_gross = Vec::new();
    let mut severe_hedged = Vec::new();
    let mut late_growth = Vec::new();
    let mut cluster_maxima = Vec::new();

    for export in exports {
        for window in &export.windows {
            let mut previous_gross = Decimal::ZERO;
            let mut rolling_crossed = 0_usize;
            let mut recent = std::collections::VecDeque::new();
            for step in &window.steps {
                if step.activity_type == "REDEEM" {
                    continue;
                }

                if let Some(hedged) = step.hedged_share_pct
                    && step.gross_inventory_shares > Decimal::ZERO
                {
                    if hedged <= Decimal::from(25_u32) {
                        imbalance_gross.push(step.gross_inventory_shares);
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
                    late_growth.push(growth);
                }

                recent.push_back(
                    step.execution_heuristic
                        == super::inventory::ExecutionHeuristic::CrossedOrStale,
                );
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
    (
        imbalance_gross.len(),
        severe_hedged.len(),
        late_growth.len(),
        cluster_maxima.len(),
    )
}

fn render_alert_calibration_report(calibration: &ReplayAlertCalibration) -> String {
    let mut output = String::new();
    let thresholds = calibration.recommended_thresholds;
    let _ = writeln!(output, "Replay alert calibration");
    let _ = writeln!(output, "Sessions: {}", calibration.sessions);
    let _ = writeln!(
        output,
        "Imbalance samples: {}",
        calibration.imbalance_samples
    );
    let _ = writeln!(
        output,
        "Severe imbalance samples: {}",
        calibration.severe_imbalance_samples
    );
    let _ = writeln!(
        output,
        "Late growth samples: {}",
        calibration.late_growth_samples
    );
    let _ = writeln!(
        output,
        "Crossed-window samples: {}",
        calibration.crossed_window_samples
    );
    let _ = writeln!(output);
    let _ = writeln!(output, "Recommended thresholds");
    let _ = writeln!(
        output,
        "- min_alert_gross_inventory_shares: {}",
        thresholds.min_alert_gross_inventory_shares.round_dp(2)
    );
    let _ = writeln!(
        output,
        "- imbalance_max_hedged_share_pct: {}",
        thresholds.imbalance_max_hedged_share_pct.round_dp(2)
    );
    let _ = writeln!(
        output,
        "- severe_imbalance_max_hedged_share_pct: {}",
        thresholds.severe_imbalance_max_hedged_share_pct.round_dp(2)
    );
    let _ = writeln!(
        output,
        "- late_window_seconds_left_max: {}",
        thresholds.late_window_seconds_left_max
    );
    let _ = writeln!(
        output,
        "- late_window_expansion_min_gross_shares: {}",
        thresholds
            .late_window_expansion_min_gross_shares
            .round_dp(2)
    );
    let _ = writeln!(
        output,
        "- late_window_expansion_min_step_growth_shares: {}",
        thresholds
            .late_window_expansion_min_step_growth_shares
            .round_dp(2)
    );
    let _ = writeln!(
        output,
        "- adverse_execution_cluster_window: {}",
        thresholds.adverse_execution_cluster_window
    );
    let _ = writeln!(
        output,
        "- adverse_execution_cluster_min_crossed: {}",
        thresholds.adverse_execution_cluster_min_crossed
    );
    output
}

fn option_decimal_string(value: Option<Decimal>) -> String {
    value.map_or_else(|| "-".to_owned(), |value| value.round_dp(2).to_string())
}
