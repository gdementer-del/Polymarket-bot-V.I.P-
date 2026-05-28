//! Shared wallet activity snapshot DTOs and loaders.

use std::fs;
use std::io::{BufRead, BufReader};
use std::path::Path;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::error::Result;

pub const DEFAULT_WALLET_ACTIVITY_RECORD_FILENAME: &str = "wallet_activity_enriched.jsonl";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletActivitySnapshotRecord {
    pub recorded_at: DateTime<Utc>,
    pub label: String,
    pub wallet: String,
    pub activity_timestamp: i64,
    #[serde(default)]
    pub activity_type: String,
    pub slug: String,
    pub question: Option<String>,
    pub market_target: Option<String>,
    pub window_start_ts: Option<i64>,
    pub window_secs: Option<i64>,
    pub minutes_window: Option<i64>,
    pub seconds_since_window_start: Option<i64>,
    pub window_progress_pct: Option<Decimal>,
    pub seconds_left_at_observed: Option<i64>,
    pub side: String,
    pub outcome: String,
    pub activity_price: Option<Decimal>,
    pub usdc_size: Option<Decimal>,
    pub transaction_hash: String,
    pub binance_symbol: Option<String>,
    pub binance_price: Option<Decimal>,
    pub target_price: Option<Decimal>,
    pub target_price_source: Option<String>,
    pub polymarket_final_reference_price: Option<Decimal>,
    pub target_gap_bps: Option<Decimal>,
    pub dominant_outcome: Option<String>,
    pub spot_move_bps: Option<Decimal>,
    pub spot_move_1s_bps: Option<Decimal>,
    pub spot_move_5s_bps: Option<Decimal>,
    pub spot_move_15s_bps: Option<Decimal>,
    pub micro_acceleration_bps: Option<Decimal>,
    pub up_ask: Option<Decimal>,
    pub up_bid: Option<Decimal>,
    pub down_ask: Option<Decimal>,
    pub down_bid: Option<Decimal>,
    pub bundle_cost: Option<Decimal>,
    pub selected_outcome_ask: Option<Decimal>,
    pub selected_outcome_bid: Option<Decimal>,
    pub selected_outcome_mid: Option<Decimal>,
    pub opposite_outcome_ask: Option<Decimal>,
    pub opposite_outcome_bid: Option<Decimal>,
    pub opposite_outcome_mid: Option<Decimal>,
    pub implied_up_mid: Option<Decimal>,
    pub implied_down_mid: Option<Decimal>,
    pub up_display_price_estimate: Option<Decimal>,
    pub down_display_price_estimate: Option<Decimal>,
    pub selected_outcome_display_price_estimate: Option<Decimal>,
    pub selected_trade_discount_to_ask_bps: Option<Decimal>,
    pub selected_trade_discount_to_mid_bps: Option<Decimal>,
    pub selected_outcome_spread_bps: Option<Decimal>,
    pub selected_vs_opposite_mid_bps: Option<Decimal>,
    pub selected_share_of_bundle_pct: Option<Decimal>,
    pub trade_index_in_slug: Option<u64>,
    pub seconds_since_previous_trade_same_slug: Option<i64>,
    pub usdc_cumulative_same_slug: Option<Decimal>,
    pub dominant_outcome_after_trade: Option<String>,
    pub selected_trade_vs_display_bps: Option<Decimal>,
}

/// Load enriched wallet activity rows from a JSONL file.
///
/// # Errors
///
/// Returns an error if the file cannot be read or a JSONL row cannot be parsed.
pub fn load_wallet_activity_records(
    path: &Path,
    limit: Option<usize>,
) -> Result<Vec<WalletActivitySnapshotRecord>> {
    if !path.exists() {
        return Ok(Vec::new());
    }

    let file = fs::File::open(path)?;
    let reader = BufReader::new(file);
    let mut records = Vec::new();
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        records.push(serde_json::from_str::<WalletActivitySnapshotRecord>(&line)?);
    }

    if let Some(limit) = limit
        && records.len() > limit
    {
        let keep_from = records.len().saturating_sub(limit);
        records = records.split_off(keep_from);
    }

    Ok(records)
}
