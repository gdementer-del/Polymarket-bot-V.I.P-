use std::fmt::Write as _;
use std::sync::mpsc::{SyncSender, TrySendError, sync_channel};
use std::thread;
use std::time::{Duration as StdDuration, Instant};

use chrono::Local;
use tracing::warn;

use crate::services::journal::PaperCycleEntry;

use super::{format_signed_decimal, render_full_screen_v2, truncate_text_v2, yes_no_ru};

const PAPER_RUN_DASHBOARD_REFRESH: StdDuration = StdDuration::from_secs(1);

#[derive(Debug, Clone)]
struct PaperRunDashboardFrame {
    entry: PaperCycleEntry,
    elapsed_secs: u64,
    remaining_secs: Option<u64>,
    drain_mode: bool,
    total_executions: usize,
}

#[derive(Debug)]
pub(super) struct PaperRunDashboardWriter {
    tx: Option<SyncSender<PaperRunDashboardFrame>>,
    handle: Option<thread::JoinHandle<()>>,
    last_enqueue_at: Option<Instant>,
    total_executions: usize,
}

impl PaperRunDashboardWriter {
    pub(super) fn spawn() -> Self {
        let (tx, rx) = sync_channel::<PaperRunDashboardFrame>(1);
        let handle = thread::spawn(move || {
            while let Ok(frame) = rx.recv() {
                if let Err(error) =
                    render_full_screen_v2(&render_paper_run_dashboard_screen(&frame))
                {
                    warn!(error = %error, "failed to redraw paper-run status dashboard");
                }
            }
        });

        Self {
            tx: Some(tx),
            handle: Some(handle),
            last_enqueue_at: None,
            total_executions: 0,
        }
    }

    pub(super) fn observe_cycle(
        &mut self,
        entry: &PaperCycleEntry,
        run_started_at: Instant,
        max_runtime: Option<StdDuration>,
        drain_mode: bool,
    ) {
        self.total_executions = self.total_executions.saturating_add(entry.executed_count);
        let now = Instant::now();
        let refresh_due = self.last_enqueue_at.is_none_or(|last_enqueue_at| {
            now.duration_since(last_enqueue_at) >= PAPER_RUN_DASHBOARD_REFRESH
        });
        if !refresh_due && entry.executed_count == 0 {
            return;
        }

        let elapsed = run_started_at.elapsed();
        let frame = PaperRunDashboardFrame {
            entry: entry.clone(),
            elapsed_secs: elapsed.as_secs(),
            remaining_secs: max_runtime.map(|duration| duration.saturating_sub(elapsed).as_secs()),
            drain_mode,
            total_executions: self.total_executions,
        };

        let Some(tx) = self.tx.as_ref() else {
            return;
        };
        match tx.try_send(frame) {
            Ok(()) => self.last_enqueue_at = Some(now),
            Err(TrySendError::Full(_)) => {}
            Err(TrySendError::Disconnected(_)) => {
                self.tx = None;
                warn!("paper-run status dashboard stopped unexpectedly");
            }
        }
    }
}

impl Drop for PaperRunDashboardWriter {
    fn drop(&mut self) {
        drop(self.tx.take());
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn render_paper_run_dashboard_screen(frame: &PaperRunDashboardFrame) -> String {
    let entry = &frame.entry;
    let remaining = frame
        .remaining_secs
        .map_or_else(|| "unbounded".to_owned(), format_dashboard_duration);
    let run_phase = if frame.drain_mode {
        "draining"
    } else {
        "active"
    };

    let mut output = String::new();
    let _ = writeln!(output, "Controlled Paper Run Dashboard");
    let _ = writeln!(
        output,
        "Press Ctrl+C to stop. The strategy remains event-driven; this view refreshes at most once per second."
    );
    let _ = writeln!(output);
    let _ = writeln!(
        output,
        "Time: {} | Phase: {} | Elapsed: {} | Remaining: {}",
        Local::now().format("%Y-%m-%d %H:%M:%S"),
        run_phase,
        format_dashboard_duration(frame.elapsed_secs),
        remaining
    );
    let _ = writeln!(
        output,
        "Run: {} | Cycles: {} | Executions: {} | Trigger: {}",
        entry.run_id,
        entry.processed_cycle_count,
        frame.total_executions,
        entry.trigger_source.as_deref().unwrap_or("startup")
    );
    let _ = writeln!(
        output,
        "Markets: total {} | live {} | fit {} | opportunities {} | near-miss {} | selected {} | executed this cycle {}",
        entry.total_markets,
        entry.live_markets,
        entry.strategy_fit_count,
        entry.opportunity_count,
        entry.near_miss_count,
        entry.selected_count,
        entry.executed_count
    );
    let _ = writeln!(
        output,
        "Paper PnL: {} | Open notional: {} | Total spent: {} | Expected profit: {}",
        format_signed_decimal(entry.session_realized_profit),
        entry.open_notional.round_dp(4),
        entry.total_spent_usdc.round_dp(4),
        format_signed_decimal(entry.total_expected_profit)
    );
    let _ = writeln!(output);
    let _ = writeln!(
        output,
        "Current: {} | timer {}s | px {} | source {} | fit {}",
        entry.current_market_slug.as_deref().unwrap_or("-"),
        entry.current_market_seconds_left.unwrap_or_default(),
        entry.current_market_price.as_deref().unwrap_or("-"),
        entry.current_market_spot_source.as_deref().unwrap_or("-"),
        entry.current_market_fit.map_or("-", yes_no_ru)
    );
    let _ = writeln!(
        output,
        "Signal: gap {} | spot {} | 1s {} | 5s {} | 15s {} | up/down {}/{}",
        entry
            .current_market_target_gap_bps
            .as_deref()
            .unwrap_or("-"),
        entry.current_market_spot_move_bps.as_deref().unwrap_or("-"),
        entry
            .current_market_spot_move_1s_bps
            .as_deref()
            .unwrap_or("-"),
        entry
            .current_market_spot_move_5s_bps
            .as_deref()
            .unwrap_or("-"),
        entry
            .current_market_spot_move_15s_bps
            .as_deref()
            .unwrap_or("-"),
        entry.current_market_up_ask.as_deref().unwrap_or("-"),
        entry.current_market_down_ask.as_deref().unwrap_or("-")
    );
    let _ = writeln!(
        output,
        "Latency: trigger->snapshot {} | snapshot {}ms | analysis {}ms | cycle {}ms",
        entry
            .latency
            .trigger_received_to_snapshot_ms
            .map_or_else(|| "-".to_owned(), |value| format!("{value}ms")),
        entry.latency.runtime_snapshot_ms,
        entry.latency.analysis_ms,
        entry.latency.cycle_total_ms
    );
    let _ = writeln!(
        output,
        "Data: {}",
        entry.data_health_reason.as_deref().unwrap_or("healthy")
    );
    let _ = writeln!(
        output,
        "Decision: {}",
        entry.decision_reason.as_deref().unwrap_or("-")
    );
    if let Some(reason) = entry.top_near_miss_reason.as_deref() {
        let _ = writeln!(output, "Top near-miss: {}", truncate_text_v2(reason, 120));
    }
    if let Some(slug) = entry.worst_open_slug.as_deref() {
        let _ = writeln!(
            output,
            "Worst open: {} | MTM {}",
            slug,
            entry.worst_open_mtm_profit_usdc.as_deref().unwrap_or("-")
        );
    }
    output
}

fn format_dashboard_duration(total_secs: u64) -> String {
    let hours = total_secs / 3_600;
    let minutes = total_secs % 3_600 / 60;
    let seconds = total_secs % 60;
    format!("{hours:02}:{minutes:02}:{seconds:02}")
}

#[cfg(test)]
mod tests {
    use super::format_dashboard_duration;

    #[test]
    fn paper_run_dashboard_formats_elapsed_time() {
        assert_eq!(format_dashboard_duration(0), "00:00:00");
        assert_eq!(format_dashboard_duration(65), "00:01:05");
        assert_eq!(format_dashboard_duration(3_661), "01:01:01");
    }
}
