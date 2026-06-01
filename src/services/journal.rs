//! Persistent execution journal and paper-state snapshots.

use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, ErrorKind, Write};
use std::path::PathBuf;
use std::sync::mpsc::{self, Sender as StdSender};
use std::thread;
use std::time::UNIX_EPOCH;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tracing::warn;

use crate::config::StorageConfig;
use crate::error::{AppError, Result};
use crate::models::{ExecutionReport, Opportunity, OpportunityKind, PaperState};

/// On-disk storage for executions and recovered paper state.
#[allow(clippy::struct_field_names)]
#[derive(Debug, Clone)]
pub struct JournalStore {
    execution_journal_path: PathBuf,
    pnl_snapshot_path: PathBuf,
    paper_cycle_journal_path: PathBuf,
    paper_trade_journal_path: PathBuf,
    paper_cycle_latest_path: PathBuf,
    paper_report_memory_path: PathBuf,
    paper_cycle_journal_max_bytes: u64,
    paper_cycle_journal_max_rotated_files: usize,
}

/// Restored state snapshot used to resume trading after restart.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PnlSnapshot {
    #[serde(default)]
    pub run_id: String,
    pub updated_at: DateTime<Utc>,
    pub execution_count: u64,
    pub paper_state: PaperState,
    pub executed_market_slugs: HashSet<String>,
}

impl Default for PnlSnapshot {
    fn default() -> Self {
        Self {
            run_id: String::new(),
            updated_at: Utc::now(),
            execution_count: 0,
            paper_state: PaperState::default(),
            executed_market_slugs: HashSet::new(),
        }
    }
}

/// A single persisted execution entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalEntry {
    #[serde(default)]
    pub run_id: String,
    pub recorded_at: DateTime<Utc>,
    pub opportunity: Opportunity,
    pub report: ExecutionReport,
    pub paper_state: PaperState,
}

/// Open/close event in paper mode.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PaperTradeAction {
    Open,
    Close,
}

impl PaperTradeAction {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Open => "OPEN",
            Self::Close => "CLOSE",
        }
    }
}

/// One persisted paper trade event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperTradeEntry {
    #[serde(default)]
    pub run_id: String,
    #[serde(default)]
    pub position_id: String,
    pub recorded_at: DateTime<Utc>,
    pub action: PaperTradeAction,
    pub slug: String,
    pub condition_id: String,
    pub question: String,
    pub kind: OpportunityKind,
    pub spent_usdc: rust_decimal::Decimal,
    pub expected_profit_usdc: Option<rust_decimal::Decimal>,
    pub realized_payout_usdc: Option<rust_decimal::Decimal>,
    pub realized_profit_usdc: Option<rust_decimal::Decimal>,
    pub dominant_outcome: Option<String>,
    pub actual_outcome: Option<String>,
    pub holding_seconds: Option<i64>,
    #[serde(default)]
    pub close_category: Option<String>,
    #[serde(default)]
    pub current_spot_price: Option<rust_decimal::Decimal>,
    #[serde(default)]
    pub target_price: Option<rust_decimal::Decimal>,
    #[serde(default)]
    pub target_gap_bps: Option<rust_decimal::Decimal>,
    #[serde(default)]
    pub spot_move_bps: Option<rust_decimal::Decimal>,
    #[serde(default)]
    pub spot_move_1s_bps: Option<rust_decimal::Decimal>,
    #[serde(default)]
    pub spot_move_5s_bps: Option<rust_decimal::Decimal>,
    #[serde(default)]
    pub spot_move_15s_bps: Option<rust_decimal::Decimal>,
    #[serde(default)]
    pub micro_acceleration_bps: Option<rust_decimal::Decimal>,
    #[serde(default)]
    pub signal_strength_bps: Option<rust_decimal::Decimal>,
    #[serde(default)]
    pub aligned_trade_flow_bps: Option<rust_decimal::Decimal>,
    #[serde(default)]
    pub primary_outcome_ask_price: Option<rust_decimal::Decimal>,
    #[serde(default)]
    pub signal_tier: Option<String>,
    #[serde(default)]
    pub target_cross_label: Option<String>,
    #[serde(default)]
    pub seconds_left_at_entry: Option<i64>,
    pub note: String,
}

/// Summary of one paper-mode market scan cycle.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PaperCycleCurrentMarketHealth {
    #[serde(default, rename = "current_market_missing_context")]
    pub missing_context: bool,
    #[serde(default, rename = "current_market_missing_up_ask")]
    pub missing_up_ask: bool,
    #[serde(default, rename = "current_market_missing_down_ask")]
    pub missing_down_ask: bool,
    #[serde(default, rename = "current_market_missing_bundle_cost")]
    pub missing_bundle_cost: bool,
    #[serde(default, rename = "current_market_missing_directional_ask")]
    pub missing_directional_ask: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PaperCycleLatencyMetrics {
    #[serde(default, rename = "latency_trigger_event_to_snapshot_ms")]
    pub trigger_event_to_snapshot_ms: Option<u64>,
    #[serde(default, rename = "latency_trigger_received_to_snapshot_ms")]
    pub trigger_received_to_snapshot_ms: Option<u64>,
    #[serde(default, rename = "latency_exit_snapshot_ms")]
    pub exit_snapshot_ms: u64,
    #[serde(default, rename = "latency_early_exit_eval_ms")]
    pub early_exit_eval_ms: u64,
    #[serde(default, rename = "latency_runtime_snapshot_ms")]
    pub runtime_snapshot_ms: u64,
    #[serde(default, rename = "latency_analysis_ms")]
    pub analysis_ms: u64,
    #[serde(default, rename = "latency_selection_ms")]
    pub selection_ms: u64,
    #[serde(default, rename = "latency_revalidation_ms")]
    pub revalidation_ms: u64,
    #[serde(default, rename = "latency_execution_ms")]
    pub execution_ms: u64,
    #[serde(default, rename = "latency_persistence_enqueue_ms")]
    pub persistence_enqueue_ms: u64,
    #[serde(default, rename = "latency_cycle_total_ms")]
    pub cycle_total_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperCycleEntry {
    #[serde(default)]
    pub run_id: String,
    pub recorded_at: DateTime<Utc>,
    #[serde(default)]
    pub trigger_source: Option<String>,
    pub total_markets: usize,
    pub live_markets: usize,
    pub strategy_fit_count: usize,
    pub opportunity_count: usize,
    pub near_miss_count: usize,
    pub selected_count: usize,
    pub executed_count: usize,
    pub open_notional: rust_decimal::Decimal,
    pub total_spent_usdc: rust_decimal::Decimal,
    pub total_expected_profit: rust_decimal::Decimal,
    pub top_opportunity_slug: Option<String>,
    pub top_opportunity_kind: Option<String>,
    pub top_opportunity_edge_bps: Option<u32>,
    #[serde(default)]
    pub top_opportunity_required_usdc: Option<String>,
    #[serde(default)]
    pub top_opportunity_expected_profit_usdc: Option<String>,
    #[serde(default)]
    pub top_opportunity_signal_strength_bps: Option<String>,
    #[serde(default)]
    pub top_opportunity_target_gap_bps: Option<String>,
    #[serde(default)]
    pub top_opportunity_primary_ask: Option<String>,
    #[serde(default)]
    pub top_opportunity_signal_tier: Option<String>,
    #[serde(default)]
    pub top_opportunity_target_cross_label: Option<String>,
    pub top_near_miss_slug: Option<String>,
    pub top_near_miss_reason: Option<String>,
    #[serde(default)]
    pub top_near_miss_primary_ask: Option<String>,
    #[serde(default)]
    pub top_near_miss_bundle_cost: Option<String>,
    #[serde(default)]
    pub top_near_miss_target_gap_bps: Option<String>,
    #[serde(default)]
    pub top_near_miss_spot_move_bps: Option<String>,
    #[serde(default)]
    pub top_near_miss_spot_move_1s_bps: Option<String>,
    #[serde(default)]
    pub top_near_miss_spot_move_5s_bps: Option<String>,
    #[serde(default)]
    pub top_near_miss_spot_move_15s_bps: Option<String>,
    #[serde(default)]
    pub top_near_miss_micro_acceleration_bps: Option<String>,
    #[serde(default)]
    pub top_near_miss_exchange_book_age_ms: Option<i64>,
    #[serde(default)]
    pub top_near_miss_exchange_book_top_imbalance_bps: Option<String>,
    #[serde(default)]
    pub top_near_miss_exchange_book_depth_imbalance_bps: Option<String>,
    #[serde(default)]
    pub top_near_miss_shortfall_bps: Option<u32>,
    #[serde(default)]
    pub top_near_miss_shortfall_label: Option<String>,
    pub current_market_slug: Option<String>,
    #[serde(default)]
    pub current_market_seconds_left: Option<i64>,
    pub current_market_spot_move_bps: Option<String>,
    #[serde(default)]
    pub current_market_spot_move_1s_bps: Option<String>,
    pub current_market_spot_move_5s_bps: Option<String>,
    #[serde(default)]
    pub current_market_spot_move_15s_bps: Option<String>,
    #[serde(default)]
    pub current_market_micro_acceleration_bps: Option<String>,
    pub current_market_price: Option<String>,
    #[serde(default)]
    pub current_market_spot_source: Option<String>,
    #[serde(default)]
    pub current_market_spot_event_age_ms: Option<i64>,
    #[serde(default)]
    pub current_market_spot_received_age_ms: Option<i64>,
    #[serde(default)]
    pub current_market_spot_quote_points: Option<usize>,
    #[serde(default)]
    pub current_market_exchange_book_age_ms: Option<i64>,
    #[serde(default)]
    pub current_market_exchange_book_top_imbalance_bps: Option<String>,
    #[serde(default)]
    pub current_market_exchange_book_depth_imbalance_bps: Option<String>,
    #[serde(default)]
    pub current_market_exchange_book_microprice_bps: Option<String>,
    #[serde(default)]
    pub current_market_exchange_book_spread_bps: Option<String>,
    #[serde(default)]
    pub current_market_target_price: Option<String>,
    #[serde(default)]
    pub current_market_target_price_source: Option<String>,
    #[serde(default)]
    pub current_market_target_gap_bps: Option<String>,
    pub current_market_up_ask: Option<String>,
    pub current_market_down_ask: Option<String>,
    pub current_market_bundle_cost: Option<String>,
    pub current_market_direction: Option<String>,
    pub current_market_fit: Option<bool>,
    #[serde(flatten)]
    pub current_market_health: PaperCycleCurrentMarketHealth,
    #[serde(default)]
    pub data_health_reason: Option<String>,
    pub decision_reason: Option<String>,
    #[serde(default)]
    pub regime: Option<String>,
    #[serde(default)]
    pub risk_blocked: bool,
    #[serde(default)]
    pub risk_reason: Option<String>,
    #[serde(default)]
    pub daily_realized_profit: rust_decimal::Decimal,
    #[serde(default)]
    pub session_realized_profit: rust_decimal::Decimal,
    #[serde(default)]
    pub consecutive_losses: u32,
    #[serde(default)]
    pub worst_open_slug: Option<String>,
    #[serde(default)]
    pub worst_open_mtm_profit_usdc: Option<String>,
    #[serde(default)]
    pub worst_open_stop_loss_hit: Option<bool>,
    #[serde(default)]
    pub worst_open_aligned_1s_bps: Option<String>,
    #[serde(default)]
    pub worst_open_aligned_5s_bps: Option<String>,
    #[serde(default)]
    pub worst_open_aligned_15s_bps: Option<String>,
    #[serde(flatten)]
    pub latency: PaperCycleLatencyMetrics,
}

/// Persisted paper-report baseline used to show deltas between runs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperReportMemory {
    pub recorded_at: DateTime<Utc>,
    pub total_cycles: usize,
    pub cycles_with_exec: usize,
    pub risk_blocked_cycles: usize,
    pub total_realized_profit: rust_decimal::Decimal,
    pub open_positions: usize,
    pub total_open_notional: rust_decimal::Decimal,
    pub total_spent_usdc: rust_decimal::Decimal,
    pub total_expected_profit: rust_decimal::Decimal,
}

type PaperJournalAck = StdSender<std::result::Result<(), String>>;

enum PaperJournalCommand {
    Execution {
        snapshot: Box<PnlSnapshot>,
        entry: Box<JournalEntry>,
    },
    Snapshot(Box<PnlSnapshot>),
    Trade(Box<PaperTradeEntry>),
    Cycle(Box<PaperCycleEntry>),
    CycleLatest(Box<PaperCycleEntry>),
    Flush(PaperJournalAck),
    Shutdown(PaperJournalAck),
}

pub struct PaperJournalWriter {
    tx: StdSender<PaperJournalCommand>,
    handle: Option<thread::JoinHandle<()>>,
}

impl PaperJournalWriter {
    /// Queue an execution journal append and snapshot refresh without blocking on disk IO.
    ///
    /// # Errors
    ///
    /// Returns an error if the writer thread is no longer available.
    pub fn record_execution(&self, snapshot: PnlSnapshot, entry: JournalEntry) -> Result<()> {
        self.send(PaperJournalCommand::Execution {
            snapshot: Box::new(snapshot),
            entry: Box::new(entry),
        })
    }

    /// Queue a snapshot-only refresh without blocking on disk IO.
    ///
    /// # Errors
    ///
    /// Returns an error if the writer thread is no longer available.
    pub fn record_snapshot(&self, snapshot: PnlSnapshot) -> Result<()> {
        self.send(PaperJournalCommand::Snapshot(Box::new(snapshot)))
    }

    /// Queue a paper trade write without blocking the runtime hot path on disk IO.
    ///
    /// # Errors
    ///
    /// Returns an error if the writer thread is no longer available.
    pub fn record_trade(&self, entry: PaperTradeEntry) -> Result<()> {
        self.send(PaperJournalCommand::Trade(Box::new(entry)))
    }

    /// Queue a paper cycle write without blocking the runtime hot path on disk IO.
    ///
    /// # Errors
    ///
    /// Returns an error if the writer thread is no longer available.
    pub fn record_cycle(&self, entry: PaperCycleEntry) -> Result<()> {
        self.send(PaperJournalCommand::Cycle(Box::new(entry)))
    }

    /// Queue a latest-cycle refresh without appending to the high-volume JSONL journal.
    ///
    /// # Errors
    ///
    /// Returns an error if the writer thread is no longer available.
    pub fn record_cycle_latest(&self, entry: PaperCycleEntry) -> Result<()> {
        self.send(PaperJournalCommand::CycleLatest(Box::new(entry)))
    }

    /// Wait until all queued writes are persisted.
    ///
    /// # Errors
    ///
    /// Returns an error if the writer thread is no longer available.
    pub fn flush(&self) -> Result<()> {
        let (ack_tx, ack_rx) = mpsc::channel();
        self.send(PaperJournalCommand::Flush(ack_tx))?;
        ack_rx
            .recv()
            .map_err(writer_recv_error)?
            .map_err(|error| writer_persist_error(&error))
    }

    /// Stop the writer thread after flushing queued writes.
    ///
    /// # Errors
    ///
    /// Returns an error if the writer thread cannot be contacted.
    pub fn shutdown(mut self) -> Result<()> {
        self.shutdown_inner()
    }

    fn shutdown_inner(&mut self) -> Result<()> {
        if self.handle.is_none() {
            return Ok(());
        }
        let (ack_tx, ack_rx) = mpsc::channel();
        self.send(PaperJournalCommand::Shutdown(ack_tx))?;
        ack_rx
            .recv()
            .map_err(writer_recv_error)?
            .map_err(|error| writer_persist_error(&error))?;
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
        Ok(())
    }

    fn send(&self, command: PaperJournalCommand) -> Result<()> {
        self.tx.send(command).map_err(writer_channel_error)
    }
}

impl Drop for PaperJournalWriter {
    fn drop(&mut self) {
        if self.handle.is_none() {
            return;
        }

        if let Err(error) = self.shutdown_inner() {
            warn!(
                error = %error,
                "не удалось корректно завершить background writer paper journal"
            );
            if let Some(handle) = self.handle.take() {
                let _ = handle.join();
            }
        }
    }
}

impl JournalStore {
    /// Create a new journal store and ensure its directory exists.
    ///
    /// # Errors
    ///
    /// Returns an error if the storage directory cannot be created.
    pub fn new(storage: &StorageConfig) -> Result<Self> {
        fs::create_dir_all(&storage.state_dir)?;

        Ok(Self {
            execution_journal_path: storage.state_dir.join(&storage.execution_journal_filename),
            pnl_snapshot_path: storage.state_dir.join(&storage.pnl_snapshot_filename),
            paper_cycle_journal_path: storage
                .state_dir
                .join(&storage.paper_cycle_journal_filename),
            paper_trade_journal_path: storage
                .state_dir
                .join(&storage.paper_trade_journal_filename),
            paper_cycle_latest_path: storage.state_dir.join("paper_cycle_latest.json"),
            paper_report_memory_path: storage.state_dir.join("paper_report_memory.json"),
            paper_cycle_journal_max_bytes: storage.paper_cycle_journal_max_bytes,
            paper_cycle_journal_max_rotated_files: storage.paper_cycle_journal_max_rotated_files,
        })
    }

    /// Spawn a background writer for high-frequency paper trade/cycle journal writes.
    #[must_use]
    pub fn spawn_paper_writer(&self) -> PaperJournalWriter {
        let (tx, rx) = mpsc::channel();
        let store = self.clone();
        let handle = thread::spawn(move || {
            let mut last_error: Option<String> = None;
            while let Ok(command) = rx.recv() {
                match command {
                    PaperJournalCommand::Execution { snapshot, entry } => {
                        if let Err(error) = store.persist_execution(&snapshot, &entry) {
                            last_error = Some(error.to_string());
                            warn!(error = %error, "failed to asynchronously persist execution");
                        }
                    }
                    PaperJournalCommand::Snapshot(snapshot) => {
                        if let Err(error) = store.persist_snapshot(&snapshot) {
                            last_error = Some(error.to_string());
                            warn!(error = %error, "failed to asynchronously persist paper snapshot");
                        }
                    }
                    PaperJournalCommand::Trade(entry) => {
                        if let Err(error) = store.record_paper_trade(&entry) {
                            last_error = Some(error.to_string());
                            warn!(error = %error, "не удалось асинхронно записать paper trade");
                        }
                    }
                    PaperJournalCommand::Cycle(entry) => {
                        if let Err(error) = store.record_paper_cycle(&entry) {
                            last_error = Some(error.to_string());
                            warn!(error = %error, "не удалось асинхронно записать paper cycle");
                        }
                    }
                    PaperJournalCommand::CycleLatest(entry) => {
                        if let Err(error) = store.record_paper_cycle_latest(&entry) {
                            last_error = Some(error.to_string());
                            warn!(error = %error, "failed to asynchronously refresh latest paper cycle");
                        }
                    }
                    PaperJournalCommand::Flush(ack_tx) => {
                        let _ = ack_tx.send(paper_writer_ack(last_error.as_deref()));
                    }
                    PaperJournalCommand::Shutdown(ack_tx) => {
                        let _ = ack_tx.send(paper_writer_ack(last_error.as_deref()));
                        break;
                    }
                }
            }
        });

        PaperJournalWriter {
            tx,
            handle: Some(handle),
        }
    }

    /// Load the last persisted snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error if the snapshot file exists but cannot be read.
    /// Falls back to the append-only journal when the summary snapshot is corrupt.
    pub fn load_snapshot(&self) -> Result<PnlSnapshot> {
        if !self.pnl_snapshot_path.exists() {
            return self.default_snapshot();
        }

        let raw = fs::read_to_string(&self.pnl_snapshot_path)?;
        if raw.trim().is_empty() {
            return self.default_snapshot();
        }

        match serde_json::from_str(&raw) {
            Ok(snapshot) => Ok(snapshot),
            Err(error) => {
                warn!(
                    error = %error,
                    path = %self.pnl_snapshot_path.display(),
                    "pnl snapshot is corrupt, recovering state from execution journal"
                );
                self.default_snapshot()
            }
        }
    }

    /// Append an execution to the JSONL journal and refresh the summary snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error if either the journal or snapshot cannot be written.
    pub fn record_execution(
        &self,
        opportunity: &Opportunity,
        report: &ExecutionReport,
        paper_state: &PaperState,
        executed_market_slugs: &HashSet<String>,
    ) -> Result<PnlSnapshot> {
        let mut snapshot = self.load_snapshot()?;
        self.record_execution_in_place(
            &mut snapshot,
            opportunity,
            report,
            paper_state,
            executed_market_slugs,
        )?;
        Ok(snapshot)
    }

    /// Append an execution and update an already-loaded snapshot.
    ///
    /// This avoids re-reading `pnl_snapshot.json` in the runtime hot path.
    ///
    /// # Errors
    ///
    /// Returns an error if either the journal or snapshot cannot be written.
    pub fn record_execution_in_place(
        &self,
        snapshot: &mut PnlSnapshot,
        opportunity: &Opportunity,
        report: &ExecutionReport,
        paper_state: &PaperState,
        executed_market_slugs: &HashSet<String>,
    ) -> Result<()> {
        let entry = Self::prepare_execution_in_place(
            snapshot,
            opportunity,
            report,
            paper_state,
            executed_market_slugs,
        );
        self.persist_execution(snapshot, &entry)
    }

    /// Update an in-memory snapshot and build its execution entry without disk IO.
    pub fn prepare_execution_in_place(
        snapshot: &mut PnlSnapshot,
        opportunity: &Opportunity,
        report: &ExecutionReport,
        paper_state: &PaperState,
        executed_market_slugs: &HashSet<String>,
    ) -> JournalEntry {
        snapshot.execution_count += 1;
        snapshot.updated_at = Utc::now();
        snapshot.paper_state = paper_state.clone();
        snapshot
            .executed_market_slugs
            .clone_from(executed_market_slugs);

        JournalEntry {
            run_id: snapshot.run_id.clone(),
            recorded_at: snapshot.updated_at,
            opportunity: opportunity.clone(),
            report: report.clone(),
            paper_state: paper_state.clone(),
        }
    }

    fn persist_execution(&self, snapshot: &PnlSnapshot, entry: &JournalEntry) -> Result<()> {
        let mut journal = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.execution_journal_path)?;
        serde_json::to_writer(&mut journal, entry)?;
        journal.write_all(b"\n")?;

        self.persist_snapshot(snapshot)
    }

    /// Persist snapshot-only changes without appending a new execution entry.
    ///
    /// # Errors
    ///
    /// Returns an error if the snapshot cannot be written.
    pub fn update_snapshot(
        &self,
        paper_state: &PaperState,
        executed_market_slugs: &HashSet<String>,
    ) -> Result<PnlSnapshot> {
        let mut snapshot = self.load_snapshot()?;
        self.update_snapshot_in_place(&mut snapshot, paper_state, executed_market_slugs)?;
        Ok(snapshot)
    }

    /// Persist snapshot-only changes into an already-loaded snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error if the snapshot cannot be written.
    pub fn update_snapshot_in_place(
        &self,
        snapshot: &mut PnlSnapshot,
        paper_state: &PaperState,
        executed_market_slugs: &HashSet<String>,
    ) -> Result<()> {
        Self::prepare_snapshot_update_in_place(snapshot, paper_state, executed_market_slugs);
        self.persist_snapshot(snapshot)
    }

    /// Update an already-loaded snapshot without touching disk.
    pub fn prepare_snapshot_update_in_place(
        snapshot: &mut PnlSnapshot,
        paper_state: &PaperState,
        executed_market_slugs: &HashSet<String>,
    ) {
        snapshot.updated_at = Utc::now();
        snapshot.paper_state = paper_state.clone();
        snapshot
            .executed_market_slugs
            .clone_from(executed_market_slugs);
    }

    fn persist_snapshot(&self, snapshot: &PnlSnapshot) -> Result<()> {
        let encoded = serde_json::to_vec(snapshot)?;
        fs::write(&self.pnl_snapshot_path, encoded)?;
        Ok(())
    }

    /// Load journal entries from disk.
    ///
    /// # Errors
    ///
    /// Returns an error if the journal exists but cannot be read or parsed.
    pub fn load_entries(&self, limit: Option<usize>) -> Result<Vec<JournalEntry>> {
        load_jsonl_records(&self.execution_journal_path, limit)
    }

    /// Append one paper open/close event to the JSONL journal.
    ///
    /// # Errors
    ///
    /// Returns an error if the journal cannot be written.
    pub fn record_paper_trade(&self, entry: &PaperTradeEntry) -> Result<()> {
        let mut journal = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.paper_trade_journal_path)?;
        serde_json::to_writer(&mut journal, entry)?;
        journal.write_all(b"\n")?;
        Ok(())
    }

    /// Load persisted paper trade events from disk.
    ///
    /// # Errors
    ///
    /// Returns an error if the journal exists but cannot be read or parsed.
    pub fn load_paper_trades(&self, limit: Option<usize>) -> Result<Vec<PaperTradeEntry>> {
        load_jsonl_records(&self.paper_trade_journal_path, limit)
    }

    /// Append one paper-mode cycle summary to the JSONL journal.
    ///
    /// # Errors
    ///
    /// Returns an error if the journal cannot be written.
    pub fn record_paper_cycle(&self, entry: &PaperCycleEntry) -> Result<()> {
        self.rotate_paper_cycle_journal_if_needed()?;
        let mut journal = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.paper_cycle_journal_path)?;
        serde_json::to_writer(&mut journal, entry)?;
        journal.write_all(b"\n")?;
        self.record_paper_cycle_latest(entry)?;
        Ok(())
    }

    fn rotate_paper_cycle_journal_if_needed(&self) -> Result<()> {
        if self.paper_cycle_journal_max_bytes == 0 {
            return Ok(());
        }

        let metadata = match fs::metadata(&self.paper_cycle_journal_path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        if metadata.len() < self.paper_cycle_journal_max_bytes {
            return Ok(());
        }

        let file_name = self
            .paper_cycle_journal_path
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("paper_cycle_journal.jsonl");
        let nonce = Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or_else(|| Utc::now().timestamp_micros().saturating_mul(1_000));
        let rotated_path = self
            .paper_cycle_journal_path
            .with_file_name(format!("{file_name}.rotated-{nonce}"));

        fs::rename(&self.paper_cycle_journal_path, rotated_path)?;
        self.prune_paper_cycle_journal_rotations()
    }

    fn prune_paper_cycle_journal_rotations(&self) -> Result<()> {
        let Some(directory) = self.paper_cycle_journal_path.parent() else {
            return Ok(());
        };
        let Some(file_name) = self
            .paper_cycle_journal_path
            .file_name()
            .and_then(|value| value.to_str())
        else {
            return Ok(());
        };
        let prefix = format!("{file_name}.rotated-");

        let mut rotations = fs::read_dir(directory)?
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let name = entry.file_name();
                let name = name.to_str()?;
                if !name.starts_with(&prefix) {
                    return None;
                }
                let modified_at = entry
                    .metadata()
                    .and_then(|metadata| metadata.modified())
                    .unwrap_or(UNIX_EPOCH);
                Some((modified_at, entry.path()))
            })
            .collect::<Vec<_>>();
        rotations.sort_by_key(|(modified_at, _path)| *modified_at);

        let remove_count = rotations
            .len()
            .saturating_sub(self.paper_cycle_journal_max_rotated_files);
        for (_modified_at, path) in rotations.into_iter().take(remove_count) {
            fs::remove_file(path)?;
        }

        Ok(())
    }

    /// Refresh the latest paper-mode cycle summary without growing the JSONL journal.
    ///
    /// # Errors
    ///
    /// Returns an error if the latest-cycle file cannot be written.
    pub fn record_paper_cycle_latest(&self, entry: &PaperCycleEntry) -> Result<()> {
        let encoded = serde_json::to_vec(entry)?;
        fs::write(&self.paper_cycle_latest_path, encoded)?;
        Ok(())
    }

    /// Load the latest paper-mode cycle summary.
    ///
    /// # Errors
    ///
    /// Returns an error if the latest-cycle file cannot be parsed.
    pub fn load_latest_paper_cycle(&self) -> Result<Option<PaperCycleEntry>> {
        if !self.paper_cycle_latest_path.exists() {
            return Ok(None);
        }

        let raw = fs::read_to_string(&self.paper_cycle_latest_path)?;
        if raw.trim().is_empty() {
            return Ok(None);
        }

        let entry = serde_json::from_str::<PaperCycleEntry>(&raw)?;
        Ok(Some(entry))
    }

    /// Load the last persisted paper-report baseline.
    ///
    /// # Errors
    ///
    /// Returns an error if the file exists but cannot be parsed.
    pub fn load_paper_report_memory(&self) -> Result<Option<PaperReportMemory>> {
        if !self.paper_report_memory_path.exists() {
            return Ok(None);
        }

        let raw = fs::read_to_string(&self.paper_report_memory_path)?;
        if raw.trim().is_empty() {
            return Ok(None);
        }

        let memory = serde_json::from_str::<PaperReportMemory>(&raw)?;
        Ok(Some(memory))
    }

    /// Persist paper-report baseline.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be written.
    pub fn save_paper_report_memory(&self, memory: &PaperReportMemory) -> Result<()> {
        let encoded = serde_json::to_vec_pretty(memory)?;
        fs::write(&self.paper_report_memory_path, encoded)?;
        Ok(())
    }

    /// Load persisted paper-mode cycle summaries from disk.
    ///
    /// # Errors
    ///
    /// Returns an error if the cycle journal exists but cannot be read or parsed.
    pub fn load_paper_cycles(&self, limit: Option<usize>) -> Result<Vec<PaperCycleEntry>> {
        let mut entries = Vec::new();
        for path in self.paper_cycle_journal_paths()? {
            entries.extend(load_jsonl_records(&path, None)?);
        }
        retain_last_records(&mut entries, limit);
        Ok(entries)
    }

    fn paper_cycle_journal_paths(&self) -> Result<Vec<PathBuf>> {
        let Some(directory) = self.paper_cycle_journal_path.parent() else {
            return Ok(vec![self.paper_cycle_journal_path.clone()]);
        };
        let Some(file_name) = self
            .paper_cycle_journal_path
            .file_name()
            .and_then(|value| value.to_str())
        else {
            return Ok(vec![self.paper_cycle_journal_path.clone()]);
        };
        let prefix = format!("{file_name}.rotated-");
        let mut rotations = fs::read_dir(directory)?
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let name = entry.file_name();
                let name = name.to_str()?;
                name.starts_with(&prefix).then(|| entry.path())
            })
            .collect::<Vec<_>>();
        rotations.sort();
        if self.paper_cycle_journal_path.exists() {
            rotations.push(self.paper_cycle_journal_path.clone());
        }
        Ok(rotations)
    }

    fn default_snapshot(&self) -> Result<PnlSnapshot> {
        let entries = self.load_entries(None)?;
        let mut snapshot = PnlSnapshot {
            execution_count: entries.len() as u64,
            executed_market_slugs: entries
                .iter()
                .map(|entry| entry.opportunity.slug.clone())
                .collect(),
            ..PnlSnapshot::default()
        };

        if let Some(last_entry) = entries.last() {
            snapshot.run_id.clone_from(&last_entry.run_id);
            snapshot.updated_at = last_entry.recorded_at;
            snapshot.paper_state = last_entry.paper_state.clone();
        }

        Ok(snapshot)
    }
}

fn load_jsonl_records<T>(path: &PathBuf, limit: Option<usize>) -> Result<Vec<T>>
where
    T: DeserializeOwned,
{
    if !path.exists() {
        return Ok(Vec::new());
    }

    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let lines = reader.lines().collect::<std::io::Result<Vec<_>>>()?;
    let last_nonempty_index = lines.iter().rposition(|line| !line.trim().is_empty());
    let mut entries = Vec::new();

    for (index, line) in lines.into_iter().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str(&line) {
            Ok(entry) => entries.push(entry),
            Err(error) if Some(index) == last_nonempty_index => {
                warn!(
                    error = %error,
                    line = index + 1,
                    path = %path.display(),
                    "skipping truncated tail record in jsonl journal"
                );
            }
            Err(error) => return Err(error.into()),
        }
    }

    retain_last_records(&mut entries, limit);
    Ok(entries)
}

fn retain_last_records<T>(entries: &mut Vec<T>, limit: Option<usize>) {
    if let Some(limit) = limit
        && entries.len() > limit
    {
        let drain_len = entries.len() - limit;
        entries.drain(..drain_len);
    }
}

fn writer_channel_error<T>(_error: mpsc::SendError<T>) -> AppError {
    AppError::Io(std::io::Error::new(
        std::io::ErrorKind::BrokenPipe,
        "paper journal writer is not available",
    ))
}

fn writer_recv_error(error: mpsc::RecvError) -> AppError {
    AppError::Io(std::io::Error::new(std::io::ErrorKind::BrokenPipe, error))
}

fn writer_persist_error(error: &str) -> AppError {
    AppError::Io(std::io::Error::other(format!(
        "paper journal writer failed to persist queued writes: {error}"
    )))
}

fn paper_writer_ack(error: Option<&str>) -> std::result::Result<(), String> {
    error.map_or(Ok(()), |error| Err(error.to_owned()))
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::fs;
    use std::io::Write;
    use std::path::PathBuf;

    use rust_decimal::Decimal;

    use crate::config::StorageConfig;
    use crate::models::{ExecutionReport, Opportunity, OpportunityKind, PaperState};

    use super::{
        JournalStore, PaperCycleCurrentMarketHealth, PaperCycleEntry, PaperCycleLatencyMetrics,
        PaperReportMemory, PnlSnapshot,
    };

    fn temp_state_dir() -> PathBuf {
        let unique = format!(
            "polymarket-mvp-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock should be after unix epoch")
                .as_nanos()
        );
        std::env::temp_dir().join(unique)
    }

    #[test]
    fn paper_writer_ack_surfaces_persist_error() {
        let ack = super::paper_writer_ack(Some("disk is read-only"));

        assert_eq!(ack, Err("disk is read-only".to_owned()));
    }

    #[test]
    fn paper_writer_persists_run_snapshot_without_trade() {
        let state_dir = temp_state_dir();
        let store = JournalStore::new(&build_storage_config(state_dir.clone()))
            .expect("journal store should initialize");
        let writer = store.spawn_paper_writer();
        let snapshot = PnlSnapshot {
            run_id: "paper-empty-run".to_owned(),
            ..PnlSnapshot::default()
        };

        writer
            .record_snapshot(snapshot)
            .expect("snapshot write should queue");
        writer
            .shutdown()
            .expect("writer should drain queued snapshot");

        let restored = store
            .load_snapshot()
            .expect("queued snapshot should deserialize");
        assert_eq!(restored.run_id, "paper-empty-run");
        assert_eq!(restored.execution_count, 0);

        fs::remove_dir_all(state_dir).expect("temporary test directory should be removable");
    }

    fn build_storage_config(state_dir: PathBuf) -> StorageConfig {
        StorageConfig {
            state_dir,
            execution_journal_filename: "journal.jsonl".to_owned(),
            pnl_snapshot_filename: "snapshot.json".to_owned(),
            paper_cycle_journal_filename: "paper_cycles.jsonl".to_owned(),
            paper_cycle_journal_sample_secs: 1,
            paper_cycle_journal_max_bytes: 16 * 1024 * 1024,
            paper_cycle_journal_max_rotated_files: 1,
            paper_trade_journal_filename: "paper_trades.jsonl".to_owned(),
        }
    }

    fn build_opportunity() -> Opportunity {
        Opportunity {
            kind: OpportunityKind::BundleArbitrage,
            condition_id: "condition-1".to_owned(),
            slug: "btc-updown-5m-1772375100".to_owned(),
            question: "Will BTC finish higher in 5m?".to_owned(),
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
            edge_bps: 700,
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
            dominant_outcome: "Up".to_owned(),
            primary_outcome_label: "Up".to_owned(),
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
        }
    }

    #[test]
    fn journal_persists_and_restores_snapshot() {
        let state_dir = temp_state_dir();
        let store = JournalStore::new(&build_storage_config(state_dir.clone()))
            .expect("journal store should initialize");
        let paper_state = PaperState {
            market_notional: [("condition-1".to_owned(), Decimal::new(93, 1))]
                .into_iter()
                .collect(),
            open_positions: Default::default(),
            total_spent_usdc: Decimal::new(93, 1),
            total_fees_usdc: Decimal::ZERO,
            total_slippage_cost_usdc: Decimal::ZERO,
            total_expected_profit: Decimal::new(7, 1),
            total_realized_payout: Decimal::ZERO,
            total_realized_profit: Decimal::ZERO,
            closed_position_count: 0,
        };
        let report = ExecutionReport {
            mode: "paper".to_owned(),
            action: "open".to_owned(),
            slug: "btc-updown-5m-1772375100".to_owned(),
            condition_id: "condition-1".to_owned(),
            question: "Will BTC finish higher in 5m?".to_owned(),
            shares: Decimal::new(10, 0),
            spent_usdc: Decimal::new(93, 1),
            expected_profit: Decimal::new(7, 1),
            details: "paper fill".to_owned(),
        };

        store
            .record_execution(
                &build_opportunity(),
                &report,
                &paper_state,
                &HashSet::from(["btc-updown-5m-1772375100".to_owned()]),
            )
            .expect("journal write should succeed");
        let restored = store
            .load_snapshot()
            .expect("snapshot should deserialize successfully");
        let entries = store
            .load_entries(None)
            .expect("journal entries should deserialize successfully");

        assert_eq!(restored.execution_count, 1);
        assert_eq!(restored.paper_state.total_spent_usdc, Decimal::new(93, 1));
        assert_eq!(
            restored.paper_state.total_expected_profit,
            Decimal::new(7, 1)
        );
        assert!(
            restored
                .executed_market_slugs
                .contains("btc-updown-5m-1772375100")
        );
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].opportunity.slug, "btc-updown-5m-1772375100");

        fs::remove_dir_all(state_dir).expect("temporary test directory should be removable");
    }

    #[test]
    fn journal_recovers_snapshot_from_journal_when_snapshot_is_corrupt() {
        let state_dir = temp_state_dir();
        let store = JournalStore::new(&build_storage_config(state_dir.clone()))
            .expect("journal store should initialize");
        let paper_state = PaperState {
            market_notional: [("condition-1".to_owned(), Decimal::new(93, 1))]
                .into_iter()
                .collect(),
            open_positions: Default::default(),
            total_spent_usdc: Decimal::new(93, 1),
            total_fees_usdc: Decimal::ZERO,
            total_slippage_cost_usdc: Decimal::ZERO,
            total_expected_profit: Decimal::new(7, 1),
            total_realized_payout: Decimal::ZERO,
            total_realized_profit: Decimal::ZERO,
            closed_position_count: 0,
        };
        let report = ExecutionReport {
            mode: "paper".to_owned(),
            action: "open".to_owned(),
            slug: "btc-updown-5m-1772375100".to_owned(),
            condition_id: "condition-1".to_owned(),
            question: "Will BTC finish higher in 5m?".to_owned(),
            shares: Decimal::new(10, 0),
            spent_usdc: Decimal::new(93, 1),
            expected_profit: Decimal::new(7, 1),
            details: "paper fill".to_owned(),
        };

        store
            .record_execution(
                &build_opportunity(),
                &report,
                &paper_state,
                &HashSet::from(["btc-updown-5m-1772375100".to_owned()]),
            )
            .expect("journal write should succeed");
        fs::write(state_dir.join("snapshot.json"), b"{broken")
            .expect("snapshot corruption fixture should be writable");

        let restored = store
            .load_snapshot()
            .expect("snapshot should recover from execution journal");

        assert_eq!(restored.execution_count, 1);
        assert_eq!(restored.paper_state.total_spent_usdc, Decimal::new(93, 1));
        assert!(
            restored
                .executed_market_slugs
                .contains("btc-updown-5m-1772375100")
        );

        fs::remove_dir_all(state_dir).expect("temporary test directory should be removable");
    }

    #[test]
    fn journal_skips_truncated_tail_record_but_keeps_prior_entries() {
        let state_dir = temp_state_dir();
        let store = JournalStore::new(&build_storage_config(state_dir.clone()))
            .expect("journal store should initialize");
        let paper_state = PaperState {
            market_notional: [("condition-1".to_owned(), Decimal::new(93, 1))]
                .into_iter()
                .collect(),
            open_positions: Default::default(),
            total_spent_usdc: Decimal::new(93, 1),
            total_fees_usdc: Decimal::ZERO,
            total_slippage_cost_usdc: Decimal::ZERO,
            total_expected_profit: Decimal::new(7, 1),
            total_realized_payout: Decimal::ZERO,
            total_realized_profit: Decimal::ZERO,
            closed_position_count: 0,
        };
        let report = ExecutionReport {
            mode: "paper".to_owned(),
            action: "open".to_owned(),
            slug: "btc-updown-5m-1772375100".to_owned(),
            condition_id: "condition-1".to_owned(),
            question: "Will BTC finish higher in 5m?".to_owned(),
            shares: Decimal::new(10, 0),
            spent_usdc: Decimal::new(93, 1),
            expected_profit: Decimal::new(7, 1),
            details: "paper fill".to_owned(),
        };

        store
            .record_execution(
                &build_opportunity(),
                &report,
                &paper_state,
                &HashSet::from(["btc-updown-5m-1772375100".to_owned()]),
            )
            .expect("journal write should succeed");
        let mut journal = fs::OpenOptions::new()
            .append(true)
            .open(state_dir.join("journal.jsonl"))
            .expect("journal fixture should open");
        journal
            .write_all(b"{truncated tail")
            .expect("truncated tail fixture should be writable");

        let entries = store
            .load_entries(None)
            .expect("truncated tail should be skipped");

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].opportunity.slug, "btc-updown-5m-1772375100");

        fs::remove_dir_all(state_dir).expect("temporary test directory should be removable");
    }

    #[test]
    fn journal_errors_on_corrupt_middle_record() {
        let state_dir = temp_state_dir();
        let store = JournalStore::new(&build_storage_config(state_dir.clone()))
            .expect("journal store should initialize");
        let paper_state = PaperState {
            market_notional: Default::default(),
            open_positions: Default::default(),
            total_spent_usdc: Decimal::ZERO,
            total_fees_usdc: Decimal::ZERO,
            total_slippage_cost_usdc: Decimal::ZERO,
            total_expected_profit: Decimal::ZERO,
            total_realized_payout: Decimal::ZERO,
            total_realized_profit: Decimal::ZERO,
            closed_position_count: 0,
        };
        let report = ExecutionReport {
            mode: "paper".to_owned(),
            action: "open".to_owned(),
            slug: "btc-updown-5m-1772375100".to_owned(),
            condition_id: "condition-1".to_owned(),
            question: "Will BTC finish higher in 5m?".to_owned(),
            shares: Decimal::new(10, 0),
            spent_usdc: Decimal::new(93, 1),
            expected_profit: Decimal::new(7, 1),
            details: "paper fill".to_owned(),
        };

        store
            .record_execution(
                &build_opportunity(),
                &report,
                &paper_state,
                &HashSet::from(["btc-updown-5m-1772375100".to_owned()]),
            )
            .expect("journal write should succeed");
        let journal_path = state_dir.join("journal.jsonl");
        let original = fs::read_to_string(&journal_path).expect("journal fixture should read");
        fs::write(&journal_path, format!("{{corrupt middle\n{original}"))
            .expect("corrupt middle fixture should be writable");

        assert!(store.load_entries(None).is_err());

        fs::remove_dir_all(state_dir).expect("temporary test directory should be removable");
    }

    #[test]
    fn journal_persists_and_reads_paper_cycles() {
        let state_dir = temp_state_dir();
        let mut storage = build_storage_config(state_dir.clone());
        storage.paper_cycle_journal_max_bytes = 1;
        let store = JournalStore::new(&storage).expect("journal store should initialize");
        let entry = PaperCycleEntry {
            run_id: "test-run".to_owned(),
            recorded_at: chrono::Utc::now(),
            trigger_source: Some("Polymarket::WS".to_owned()),
            total_markets: 12,
            live_markets: 1,
            strategy_fit_count: 2,
            opportunity_count: 1,
            near_miss_count: 3,
            selected_count: 1,
            executed_count: 1,
            open_notional: Decimal::new(30, 0),
            total_spent_usdc: Decimal::new(30, 0),
            total_expected_profit: Decimal::new(3, 0),
            top_opportunity_slug: Some("btc-updown-5m-1".to_owned()),
            top_opportunity_kind: Some("directional".to_owned()),
            top_opportunity_edge_bps: Some(44),
            top_opportunity_required_usdc: Some("20.0".to_owned()),
            top_opportunity_expected_profit_usdc: Some("3.4".to_owned()),
            top_opportunity_signal_strength_bps: Some("8.2".to_owned()),
            top_opportunity_target_gap_bps: Some("12.4".to_owned()),
            top_opportunity_primary_ask: Some("0.56".to_owned()),
            top_opportunity_signal_tier: Some("strong".to_owned()),
            top_opportunity_target_cross_label: Some("1s".to_owned()),
            top_near_miss_slug: Some("btc-updown-5m-2".to_owned()),
            top_near_miss_reason: Some("spot слабее порога".to_owned()),
            top_near_miss_primary_ask: Some("0.61".to_owned()),
            top_near_miss_bundle_cost: Some("1.02".to_owned()),
            top_near_miss_target_gap_bps: Some("10.5".to_owned()),
            top_near_miss_spot_move_bps: Some("9.7".to_owned()),
            top_near_miss_spot_move_1s_bps: Some("1.0".to_owned()),
            top_near_miss_spot_move_5s_bps: Some("2.0".to_owned()),
            top_near_miss_spot_move_15s_bps: Some("4.0".to_owned()),
            top_near_miss_micro_acceleration_bps: Some("0.5".to_owned()),
            top_near_miss_exchange_book_age_ms: Some(8),
            top_near_miss_exchange_book_top_imbalance_bps: Some("2100".to_owned()),
            top_near_miss_exchange_book_depth_imbalance_bps: Some("1700".to_owned()),
            top_near_miss_shortfall_bps: Some(2),
            top_near_miss_shortfall_label: Some("2 bps".to_owned()),
            current_market_slug: Some("btc-updown-5m-1".to_owned()),
            current_market_seconds_left: Some(120),
            current_market_spot_move_bps: Some("12.4".to_owned()),
            current_market_spot_move_1s_bps: Some("1.2".to_owned()),
            current_market_spot_move_5s_bps: Some("3.1".to_owned()),
            current_market_spot_move_15s_bps: Some("5.8".to_owned()),
            current_market_micro_acceleration_bps: Some("0.9".to_owned()),
            current_market_price: Some("87123.4".to_owned()),
            current_market_spot_source: Some("Coinbase::Ticker".to_owned()),
            current_market_spot_event_age_ms: Some(612),
            current_market_spot_received_age_ms: Some(4),
            current_market_spot_quote_points: Some(42),
            current_market_exchange_book_age_ms: Some(9),
            current_market_exchange_book_top_imbalance_bps: Some("2200".to_owned()),
            current_market_exchange_book_depth_imbalance_bps: Some("1800".to_owned()),
            current_market_exchange_book_microprice_bps: Some("0.5".to_owned()),
            current_market_exchange_book_spread_bps: Some("0.2".to_owned()),
            current_market_target_price: Some("87100.0".to_owned()),
            current_market_target_price_source: Some("binance_fallback".to_owned()),
            current_market_target_gap_bps: Some("12.4".to_owned()),
            current_market_up_ask: Some("0.56".to_owned()),
            current_market_down_ask: Some("0.45".to_owned()),
            current_market_bundle_cost: Some("1.01".to_owned()),
            current_market_direction: Some("Рост".to_owned()),
            current_market_fit: Some(false),
            decision_reason: Some("почти directional | gap 2 bps".to_owned()),
            current_market_health: PaperCycleCurrentMarketHealth::default(),
            data_health_reason: Some("healthy".to_owned()),
            regime: Some("safe".to_owned()),
            risk_blocked: false,
            risk_reason: None,
            daily_realized_profit: rust_decimal::Decimal::new(1, 0),
            session_realized_profit: rust_decimal::Decimal::new(2, 0),
            consecutive_losses: 0,
            worst_open_slug: Some("btc-updown-5m-3".to_owned()),
            worst_open_mtm_profit_usdc: Some("-1.25".to_owned()),
            worst_open_stop_loss_hit: Some(true),
            worst_open_aligned_1s_bps: Some("-2.4".to_owned()),
            worst_open_aligned_5s_bps: Some("-1.9".to_owned()),
            worst_open_aligned_15s_bps: Some("-0.8".to_owned()),
            latency: PaperCycleLatencyMetrics {
                trigger_event_to_snapshot_ms: Some(18),
                trigger_received_to_snapshot_ms: Some(9),
                exit_snapshot_ms: 3,
                early_exit_eval_ms: 1,
                runtime_snapshot_ms: 12,
                analysis_ms: 4,
                selection_ms: 2,
                revalidation_ms: 1,
                execution_ms: 5,
                persistence_enqueue_ms: 1,
                cycle_total_ms: 27,
            },
        };

        store
            .record_paper_cycle(&entry)
            .expect("paper cycle write should succeed");
        let mut second_entry = entry.clone();
        second_entry.run_id = "test-run-2".to_owned();
        second_entry.opportunity_count = 2;
        second_entry.top_opportunity_slug = Some("btc-updown-5m-2".to_owned());
        store
            .record_paper_cycle(&second_entry)
            .expect("second paper cycle write should rotate the first entry");
        let loaded = store
            .load_paper_cycles(Some(10))
            .expect("paper cycles should deserialize successfully");

        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].opportunity_count, 1);
        assert_eq!(loaded[0].trigger_source.as_deref(), Some("Polymarket::WS"));
        assert_eq!(
            loaded[0].top_opportunity_slug.as_deref(),
            Some("btc-updown-5m-1")
        );
        assert_eq!(loaded[0].top_near_miss_primary_ask.as_deref(), Some("0.61"));
        assert_eq!(
            loaded[0].top_near_miss_target_gap_bps.as_deref(),
            Some("10.5")
        );
        assert_eq!(loaded[1].opportunity_count, 2);
        assert_eq!(
            loaded[1].top_opportunity_slug.as_deref(),
            Some("btc-updown-5m-2")
        );
        let limited = store
            .load_paper_cycles(Some(1))
            .expect("paper cycle limit should apply across active and rotated journals");
        assert_eq!(limited.len(), 1);
        assert_eq!(limited[0].run_id, "test-run-2");
        let latest = store
            .load_latest_paper_cycle()
            .expect("latest paper cycle should deserialize successfully");
        assert!(latest.is_some());
        assert_eq!(
            latest
                .as_ref()
                .and_then(|cycle| cycle.top_opportunity_slug.as_deref()),
            Some("btc-updown-5m-2")
        );

        fs::remove_dir_all(state_dir).expect("temporary test directory should be removable");
    }

    #[test]
    fn paper_cycle_journal_rotation_keeps_active_file_small() {
        let state_dir = temp_state_dir();
        let mut storage = build_storage_config(state_dir.clone());
        storage.paper_cycle_journal_max_bytes = 4;
        storage.paper_cycle_journal_max_rotated_files = 1;
        let store = JournalStore::new(&storage).expect("journal store should initialize");
        let journal_path = state_dir.join("paper_cycles.jsonl");

        fs::write(&journal_path, b"first").expect("oversized journal fixture should be writable");
        store
            .rotate_paper_cycle_journal_if_needed()
            .expect("oversized journal should rotate");
        assert!(!journal_path.exists());

        fs::write(&journal_path, b"again").expect("second oversized journal fixture should write");
        store
            .rotate_paper_cycle_journal_if_needed()
            .expect("second oversized journal should rotate and prune");

        let rotated_count = fs::read_dir(&state_dir)
            .expect("state dir should read")
            .filter_map(std::result::Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.starts_with("paper_cycles.jsonl.rotated-"))
            })
            .count();
        assert_eq!(rotated_count, 1);

        fs::remove_dir_all(state_dir).expect("temporary test directory should be removable");
    }

    #[test]
    fn journal_persists_and_reads_paper_report_memory() {
        let state_dir = temp_state_dir();
        let store = JournalStore::new(&build_storage_config(state_dir.clone()))
            .expect("journal store should initialize");
        let memory = PaperReportMemory {
            recorded_at: chrono::Utc::now(),
            total_cycles: 42,
            cycles_with_exec: 9,
            risk_blocked_cycles: 3,
            total_realized_profit: Decimal::new(125, 1),
            open_positions: 2,
            total_open_notional: Decimal::new(750, 1),
            total_spent_usdc: Decimal::new(1280, 1),
            total_expected_profit: Decimal::new(210, 1),
        };

        store
            .save_paper_report_memory(&memory)
            .expect("paper report memory should persist");
        let restored = store
            .load_paper_report_memory()
            .expect("paper report memory should deserialize");

        assert!(restored.is_some());
        let restored = restored.expect("memory should exist");
        assert_eq!(restored.total_cycles, 42);
        assert_eq!(restored.cycles_with_exec, 9);
        assert_eq!(restored.total_realized_profit, Decimal::new(125, 1));

        fs::remove_dir_all(state_dir).expect("temporary test directory should be removable");
    }
}
