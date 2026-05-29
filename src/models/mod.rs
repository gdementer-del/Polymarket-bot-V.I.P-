//! Domain models and API payloads.

use std::collections::HashMap;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::de::{self, Deserializer};
use serde::{Deserialize, Serialize};

use crate::error::{AppError, Result};

/// Supported fast Polymarket market families.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub enum MarketTarget {
    Btc5m,
    Btc15m,
    Eth5m,
    Eth15m,
    Sol5m,
    Xrp5m,
    Bnb5m,
}

impl MarketTarget {
    /// Return the canonical config key.
    #[must_use]
    pub const fn as_key(self) -> &'static str {
        match self {
            Self::Btc5m => "btc-5m",
            Self::Btc15m => "btc-15m",
            Self::Eth5m => "eth-5m",
            Self::Eth15m => "eth-15m",
            Self::Sol5m => "sol-5m",
            Self::Xrp5m => "xrp-5m",
            Self::Bnb5m => "bnb-5m",
        }
    }

    /// Return the human-readable label.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Btc5m => "BTC 5m",
            Self::Btc15m => "BTC 15m",
            Self::Eth5m => "ETH 5m",
            Self::Eth15m => "ETH 15m",
            Self::Sol5m => "SOL 5m",
            Self::Xrp5m => "XRP 5m",
            Self::Bnb5m => "BNB 5m",
        }
    }

    /// Return the Polymarket slug prefix.
    #[must_use]
    pub const fn slug_prefix(self) -> &'static str {
        match self {
            Self::Btc5m => "btc-updown-5m-",
            Self::Btc15m => "btc-updown-15m-",
            Self::Eth5m => "eth-updown-5m-",
            Self::Eth15m => "eth-updown-15m-",
            Self::Sol5m => "sol-updown-5m-",
            Self::Xrp5m => "xrp-updown-5m-",
            Self::Bnb5m => "bnb-updown-5m-",
        }
    }

    /// Return the Binance spot symbol used for this target.
    #[must_use]
    pub const fn binance_symbol(self) -> &'static str {
        match self {
            Self::Btc5m | Self::Btc15m => "BTCUSDT",
            Self::Eth5m | Self::Eth15m => "ETHUSDT",
            Self::Sol5m => "SOLUSDT",
            Self::Xrp5m => "XRPUSDT",
            Self::Bnb5m => "BNBUSDT",
        }
    }

    /// Return the Coinbase Exchange product used as a secondary live price source.
    #[must_use]
    pub const fn coinbase_product_id(self) -> &'static str {
        match self {
            Self::Btc5m | Self::Btc15m => "BTC-USD",
            Self::Eth5m | Self::Eth15m => "ETH-USD",
            Self::Sol5m => "SOL-USD",
            Self::Xrp5m => "XRP-USD",
            Self::Bnb5m => "BNB-USD",
        }
    }

    /// Return the Polymarket RTDS Chainlink symbol when the public stream supports it.
    #[must_use]
    pub const fn polymarket_chainlink_symbol(self) -> Option<&'static str> {
        match self {
            Self::Btc5m | Self::Btc15m => Some("btc/usd"),
            Self::Eth5m | Self::Eth15m => Some("eth/usd"),
            Self::Sol5m => Some("sol/usd"),
            Self::Xrp5m => Some("xrp/usd"),
            Self::Bnb5m => None,
        }
    }

    /// Return the window length in seconds.
    #[must_use]
    pub const fn window_secs(self) -> i64 {
        match self {
            Self::Btc5m | Self::Eth5m | Self::Sol5m | Self::Xrp5m | Self::Bnb5m => 300,
            Self::Btc15m | Self::Eth15m => 900,
        }
    }

    /// Return the normalized window start timestamp for an arbitrary moment.
    #[must_use]
    pub fn window_start_ts_at(self, timestamp_secs: i64) -> i64 {
        let window_secs = self.window_secs();
        timestamp_secs.div_euclid(window_secs) * window_secs
    }

    /// Build the canonical Polymarket slug for the given window start.
    #[must_use]
    pub fn slug_for_window_start(self, start_ts: i64) -> String {
        format!("{}{}", self.slug_prefix(), start_ts)
    }

    /// Build the canonical slug for the currently active live window.
    #[must_use]
    pub fn live_slug_at(self, timestamp_secs: i64) -> String {
        self.slug_for_window_start(self.window_start_ts_at(timestamp_secs))
    }

    /// Parse the target from a Polymarket slug.
    #[must_use]
    pub fn from_slug(slug: &str) -> Option<Self> {
        [
            Self::Btc5m,
            Self::Btc15m,
            Self::Eth5m,
            Self::Eth15m,
            Self::Sol5m,
            Self::Xrp5m,
            Self::Bnb5m,
        ]
        .into_iter()
        .find(|target| slug.starts_with(target.slug_prefix()))
    }
}

impl From<MarketTarget> for String {
    fn from(value: MarketTarget) -> Self {
        value.as_key().to_owned()
    }
}

impl TryFrom<String> for MarketTarget {
    type Error = String;

    fn try_from(value: String) -> std::result::Result<Self, Self::Error> {
        Self::from_str(&value)
    }
}

impl FromStr for MarketTarget {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "btc-5m" => Ok(Self::Btc5m),
            "btc-15m" => Ok(Self::Btc15m),
            "eth-5m" => Ok(Self::Eth5m),
            "eth-15m" => Ok(Self::Eth15m),
            "sol-5m" => Ok(Self::Sol5m),
            "xrp-5m" => Ok(Self::Xrp5m),
            "bnb-5m" => Ok(Self::Bnb5m),
            _ => Err(format!("неподдерживаемая цель рынка `{value}`")),
        }
    }
}

/// Source used to determine the market target price.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetPriceSource {
    PolymarketEventMetadata,
    ChainlinkRtdsWindowOpen,
    #[default]
    BinanceWindowOpenFallback,
}

impl TargetPriceSource {
    /// Return a compact label for logs and dashboards.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PolymarketEventMetadata => "polymarket",
            Self::ChainlinkRtdsWindowOpen => "chainlink_rtds",
            Self::BinanceWindowOpenFallback => "binance_fallback",
        }
    }

    /// Return `true` when the target price came from an explicit oracle/market source.
    #[must_use]
    pub const fn is_explicit(self) -> bool {
        matches!(
            self,
            Self::PolymarketEventMetadata | Self::ChainlinkRtdsWindowOpen
        )
    }
}

/// A normalized binary Polymarket market.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BinaryMarket {
    pub condition_id: String,
    pub slug: String,
    pub question: String,
    pub outcome_a_label: String,
    pub outcome_a_token_id: String,
    pub outcome_b_label: String,
    pub outcome_b_token_id: String,
    pub end_date: Option<DateTime<Utc>>,
    pub liquidity_usdc: Decimal,
    pub target_price: Option<Decimal>,
    pub target_price_source: Option<TargetPriceSource>,
    pub final_reference_price: Option<Decimal>,
}

impl BinaryMarket {
    /// Return the token ID for an outcome label, matched case-insensitively.
    #[must_use]
    pub fn token_for_outcome(&self, label: &str) -> Option<&str> {
        if self.outcome_a_label.eq_ignore_ascii_case(label) {
            Some(&self.outcome_a_token_id)
        } else if self.outcome_b_label.eq_ignore_ascii_case(label) {
            Some(&self.outcome_b_token_id)
        } else {
            None
        }
    }

    /// Return the parsed supported market family.
    #[must_use]
    pub fn target(&self) -> Option<MarketTarget> {
        MarketTarget::from_slug(&self.slug)
    }

    /// Return `true` if this market is a BTC 5-minute up/down market.
    #[must_use]
    pub fn is_btc_5m_market(&self) -> bool {
        self.target() == Some(MarketTarget::Btc5m)
            && self.token_for_outcome("up").is_some()
            && self.token_for_outcome("down").is_some()
    }

    /// Return `true` if this market belongs to any supported family.
    #[must_use]
    pub fn is_supported_target_market(&self) -> bool {
        self.target().is_some()
            && self.token_for_outcome("up").is_some()
            && self.token_for_outcome("down").is_some()
    }

    /// Parse the market open time from its slug.
    #[must_use]
    pub fn window_start_ts(&self) -> Option<i64> {
        let target = self.target()?;
        self.slug
            .strip_prefix(target.slug_prefix())?
            .parse::<i64>()
            .ok()
    }

    /// Return the market window length in seconds.
    #[must_use]
    pub fn window_secs(&self) -> Option<i64> {
        self.target().map(MarketTarget::window_secs)
    }

    /// Parse the BTC 5-minute market open time from its slug.
    #[must_use]
    pub fn btc_5m_window_start_ts(&self) -> Option<i64> {
        if !self.is_btc_5m_market() {
            return None;
        }

        self.window_start_ts()
    }
}

/// Order book level.
#[derive(Debug, Clone, Deserialize, Eq, PartialEq)]
pub struct BookLevel {
    #[serde(deserialize_with = "decimal_from_any")]
    pub price: Decimal,
    #[serde(deserialize_with = "decimal_from_any")]
    pub size: Decimal,
}

/// Order book snapshot.
#[derive(Debug, Clone, Deserialize, Eq, PartialEq)]
pub struct OrderBook {
    #[serde(alias = "asset_id")]
    pub asset_id: String,
    pub bids: Vec<BookLevel>,
    pub asks: Vec<BookLevel>,
    #[serde(default, deserialize_with = "option_decimal_from_any")]
    pub min_order_size: Option<Decimal>,
    #[serde(default, deserialize_with = "option_decimal_from_any")]
    pub tick_size: Option<Decimal>,
}

impl OrderBook {
    /// Return the best ask if present.
    #[must_use]
    pub fn best_ask(&self) -> Option<&BookLevel> {
        self.asks.last()
    }

    /// Return the best bid if present.
    #[must_use]
    pub fn best_bid(&self) -> Option<&BookLevel> {
        self.bids.last()
    }
}

/// Opportunity returned by the strategy scanner.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OpportunityKind {
    BundleArbitrage,
    DirectionalMomentum,
    MicroBreakout,
    TargetStateV1,
    BonereaperStateV1,
    BonereaperStateV2,
    BonereaperStateGuarded,
    CodexSentinelV1,
    CodexScalpProbeV1,
    DirectionalMomentumHedged,
}

impl OpportunityKind {
    /// Return the short label used in logs and dashboards.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BundleArbitrage => "bundle",
            Self::DirectionalMomentum => "directional",
            Self::MicroBreakout => "micro-breakout",
            Self::TargetStateV1 => "target-state-v1",
            Self::BonereaperStateV1 => "bonereaper-state-v1",
            Self::BonereaperStateV2 => "bonereaper-state-v2",
            Self::BonereaperStateGuarded => "bonereaper-state-guarded",
            Self::CodexSentinelV1 => "codex-sentinel-v1",
            Self::CodexScalpProbeV1 => "codex-scalp-probe-v1",
            Self::DirectionalMomentumHedged => "dir+hedge",
        }
    }
}

/// Planned execution slice for one order-book level.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct BookFillLevel {
    pub price: Decimal,
    pub shares: Decimal,
}

/// Opportunity returned by the strategy scanner.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Opportunity {
    pub kind: OpportunityKind,
    pub condition_id: String,
    pub slug: String,
    pub question: String,
    pub outcome_a_label: String,
    pub outcome_a_token_id: String,
    pub outcome_b_label: String,
    pub outcome_b_token_id: String,
    pub liquidity_usdc: Decimal,
    pub outcome_a_ask_price: Decimal,
    pub outcome_b_ask_price: Decimal,
    pub bundle_cost: Decimal,
    pub net_bundle_cost: Decimal,
    pub edge_per_share: Decimal,
    pub edge_bps: u32,
    pub tradable_shares: Decimal,
    pub required_usdc: Decimal,
    pub expected_payout: Decimal,
    pub expected_profit: Decimal,
    pub interval_open_price: Decimal,
    #[serde(default)]
    pub target_price: Decimal,
    #[serde(default)]
    pub target_price_source: TargetPriceSource,
    #[serde(default)]
    pub target_gap_bps: Decimal,
    pub current_spot_price: Decimal,
    pub spot_move_bps: Decimal,
    #[serde(default)]
    pub spot_move_1s_bps: Decimal,
    #[serde(default)]
    pub spot_move_5s_bps: Decimal,
    #[serde(default)]
    pub spot_move_15s_bps: Decimal,
    #[serde(default)]
    pub micro_acceleration_bps: Decimal,
    #[serde(default)]
    pub micro_burst_reference_price: Decimal,
    #[serde(default)]
    pub micro_reference_price: Decimal,
    #[serde(default)]
    pub signal_strength_bps: Decimal,
    #[serde(default)]
    pub aligned_trade_flow_bps: Decimal,
    #[serde(default)]
    pub signal_tier: String,
    #[serde(default)]
    pub target_cross_label: String,
    pub dominant_outcome: String,
    pub primary_outcome_label: String,
    pub primary_outcome_token_id: String,
    pub primary_outcome_ask_price: Decimal,
    #[serde(default)]
    pub primary_fill_levels: Vec<BookFillLevel>,
    pub hedge_outcome_label: Option<String>,
    pub hedge_outcome_token_id: Option<String>,
    pub hedge_outcome_ask_price: Option<Decimal>,
    #[serde(default)]
    pub hedge_fill_levels: Vec<BookFillLevel>,
    pub hedge_shares: Decimal,
    pub seconds_left: i64,
    pub note: String,
}

/// Execution report for paper/live modes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionReport {
    pub mode: String,
    pub action: String,
    pub slug: String,
    pub condition_id: String,
    pub question: String,
    pub shares: Decimal,
    pub spent_usdc: Decimal,
    pub expected_profit: Decimal,
    pub details: String,
}

/// Outcome side for fast up/down markets.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PaperOutcomeSide {
    Up,
    Down,
    Unknown,
}

const PAPER_OUTCOME_UP_LABEL_RU_LOWER: &str = "\u{440}\u{43e}\u{441}\u{442}";
const PAPER_OUTCOME_DOWN_LABEL_RU_LOWER: &str = "\u{43f}\u{430}\u{434}\u{435}\u{43d}\u{438}\u{435}";

impl PaperOutcomeSide {
    /// Parse a market outcome label into a normalized side.
    #[must_use]
    pub fn from_label(label: &str) -> Self {
        match label.trim().to_lowercase().as_str() {
            "up" | PAPER_OUTCOME_UP_LABEL_RU_LOWER => Self::Up,
            "down" | PAPER_OUTCOME_DOWN_LABEL_RU_LOWER => Self::Down,
            _ => Self::Unknown,
        }
    }

    /// Return a compact ASCII label for logs and journals.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Up => "Up",
            Self::Down => "Down",
            Self::Unknown => "Unknown",
        }
    }
}

/// One simulated leg of a paper position.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperPositionLeg {
    pub label: String,
    pub side: PaperOutcomeSide,
    pub token_id: String,
    pub shares: Decimal,
    pub entry_price: Decimal,
}

/// Open paper position tracked between entry and auto-settlement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperPosition {
    pub opened_at: DateTime<Utc>,
    pub scheduled_close_at: Option<DateTime<Utc>>,
    pub condition_id: String,
    pub slug: String,
    pub question: String,
    pub kind: OpportunityKind,
    pub dominant_outcome_at_entry: String,
    pub spot_move_bps_at_entry: Decimal,
    pub spent_usdc: Decimal,
    pub expected_profit_usdc: Decimal,
    #[serde(default = "default_paper_position_entry_count")]
    pub entry_count: u32,
    #[serde(default)]
    pub partial_reversal_exits: u32,
    #[serde(default)]
    pub best_entry_reference_price: Decimal,
    pub legs: Vec<PaperPositionLeg>,
}

/// Current paper position summary.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct PaperState {
    pub market_notional: HashMap<String, Decimal>,
    pub open_positions: HashMap<String, PaperPosition>,
    pub total_spent_usdc: Decimal,
    pub total_fees_usdc: Decimal,
    pub total_slippage_cost_usdc: Decimal,
    pub total_expected_profit: Decimal,
    pub total_realized_payout: Decimal,
    pub total_realized_profit: Decimal,
    pub closed_position_count: u64,
}

const fn default_paper_position_entry_count() -> u32 {
    1
}

/// Gamma API market payload.
#[derive(Debug, Clone, Deserialize)]
pub struct GammaMarket {
    #[serde(alias = "conditionId")]
    pub condition_id: String,
    #[serde(default, alias = "slug")]
    pub slug: String,
    #[serde(default, alias = "question", alias = "title")]
    pub question: String,
    #[serde(default)]
    pub active: bool,
    #[serde(default)]
    pub closed: bool,
    #[serde(default)]
    pub archived: Option<bool>,
    #[serde(default, alias = "endDate", alias = "end_date")]
    pub end_date: Option<String>,
    #[serde(
        default,
        alias = "liquidityNum",
        deserialize_with = "option_decimal_from_any"
    )]
    pub liquidity_num: Option<Decimal>,
    #[serde(default, alias = "clobTokenIds")]
    pub clob_token_ids: Option<String>,
    #[serde(default)]
    pub outcomes: Option<String>,
    #[serde(default, alias = "negRisk")]
    pub neg_risk: Option<bool>,
    #[serde(default)]
    pub events: Vec<GammaEvent>,
}

/// Nested Gamma event payload.
#[derive(Debug, Clone, Deserialize)]
pub struct GammaEvent {
    #[serde(default, alias = "eventMetadata")]
    pub event_metadata: Option<GammaEventMetadata>,
}

/// Explicit market reference prices published by Polymarket.
#[derive(Debug, Clone, Deserialize)]
pub struct GammaEventMetadata {
    #[serde(
        default,
        alias = "priceToBeat",
        deserialize_with = "option_decimal_from_any"
    )]
    pub price_to_beat: Option<Decimal>,
    #[serde(
        default,
        alias = "finalPrice",
        deserialize_with = "option_decimal_from_any"
    )]
    pub final_price: Option<Decimal>,
}

impl GammaMarket {
    /// Convert a raw Gamma market into a normalized binary market.
    ///
    /// # Errors
    ///
    /// Returns an error if embedded JSON fields or the end date are malformed.
    pub fn into_binary_market(self) -> Result<Option<BinaryMarket>> {
        self.into_binary_market_with_options(true)
    }

    /// Convert a raw Gamma market into a normalized binary market without
    /// requiring the market to remain active.
    ///
    /// # Errors
    ///
    /// Returns an error if embedded JSON fields or the end date are malformed.
    pub fn into_binary_market_any_state(self) -> Result<Option<BinaryMarket>> {
        self.into_binary_market_with_options(false)
    }

    fn into_binary_market_with_options(self, require_active: bool) -> Result<Option<BinaryMarket>> {
        if self.archived.unwrap_or(false) || self.neg_risk == Some(true) {
            return Ok(None);
        }

        if require_active && (!self.active || self.closed) {
            return Ok(None);
        }

        let token_ids: Vec<String> = Self::parse_json_array("clobTokenIds", self.clob_token_ids)?;
        let outcomes: Vec<String> = Self::parse_json_array("outcomes", self.outcomes)?;

        if token_ids.len() != 2 || outcomes.len() != 2 {
            return Ok(None);
        }

        let end_date = self
            .end_date
            .as_deref()
            .map(parse_datetime_utc)
            .transpose()
            .map_err(|value| {
                AppError::InvalidMarket(format!("некорректная дата завершения `{value}`"))
            })?;

        let event_metadata = self
            .events
            .iter()
            .find_map(|event| event.event_metadata.as_ref());
        let target_price = event_metadata.and_then(|metadata| metadata.price_to_beat);
        let final_reference_price = event_metadata.and_then(|metadata| metadata.final_price);
        let target_price_source = target_price.map(|_| TargetPriceSource::PolymarketEventMetadata);

        Ok(Some(BinaryMarket {
            condition_id: self.condition_id,
            slug: self.slug,
            question: self.question,
            outcome_a_label: outcomes[0].clone(),
            outcome_a_token_id: token_ids[0].clone(),
            outcome_b_label: outcomes[1].clone(),
            outcome_b_token_id: token_ids[1].clone(),
            end_date,
            liquidity_usdc: self.liquidity_num.unwrap_or(Decimal::ZERO),
            target_price,
            target_price_source,
            final_reference_price,
        }))
    }

    fn parse_json_array(field: &'static str, value: Option<String>) -> Result<Vec<String>> {
        value
            .map(|raw| serde_json::from_str::<Vec<String>>(&raw))
            .transpose()?
            .ok_or_else(|| AppError::InvalidMarket(format!("отсутствует поле `{field}`")))
    }
}

/// Body for the batched books endpoint.
#[derive(Debug, Clone, Serialize)]
pub struct BooksRequestItem<'a> {
    pub token_id: &'a str,
}

/// Geoblock response from `polymarket.com/api/geoblock`.
#[derive(Debug, Clone, Deserialize)]
pub struct GeoblockResponse {
    pub blocked: bool,
    pub country: String,
    pub region: String,
}

fn parse_datetime_utc(value: &str) -> std::result::Result<DateTime<Utc>, chrono::ParseError> {
    DateTime::parse_from_rfc3339(value).map(|parsed| parsed.with_timezone(&Utc))
}

fn decimal_from_any<'de, D>(deserializer: D) -> std::result::Result<Decimal, D::Error>
where
    D: Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    decimal_from_value(&value).ok_or_else(|| de::Error::custom("некорректное decimal-значение"))
}

fn option_decimal_from_any<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<Decimal>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    if value.is_null() {
        Ok(None)
    } else {
        decimal_from_value(&value)
            .map(Some)
            .ok_or_else(|| de::Error::custom("некорректное decimal-значение"))
    }
}

fn decimal_from_value(value: &serde_json::Value) -> Option<Decimal> {
    match value {
        serde_json::Value::String(inner) => inner.parse::<Decimal>().ok(),
        serde_json::Value::Number(inner) => inner.to_string().parse::<Decimal>().ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::{
        BinaryMarket, BookLevel, GammaEvent, GammaEventMetadata, GammaMarket, MarketTarget,
        OrderBook, TargetPriceSource,
    };

    #[test]
    fn binary_market_parses_yes_no_pairs() {
        let market = GammaMarket {
            condition_id: "cond-1".to_owned(),
            slug: "test-market".to_owned(),
            question: "Will this compile?".to_owned(),
            active: true,
            closed: false,
            archived: Some(false),
            end_date: Some("2026-05-01T00:00:00Z".to_owned()),
            liquidity_num: Some("1000".parse().expect("decimal literal is valid")),
            clob_token_ids: Some("[\"yes-token\",\"no-token\"]".to_owned()),
            outcomes: Some("[\"Yes\",\"No\"]".to_owned()),
            neg_risk: Some(false),
            events: Vec::new(),
        };

        let normalized = market
            .into_binary_market()
            .expect("market should parse")
            .expect("market should stay binary");

        assert_eq!(normalized.outcome_a_label, "Yes");
        assert_eq!(normalized.outcome_a_token_id, "yes-token");
        assert_eq!(normalized.outcome_b_label, "No");
        assert_eq!(normalized.outcome_b_token_id, "no-token");
    }

    #[test]
    fn btc_5m_slug_parses_start_timestamp() {
        let market = BinaryMarket {
            condition_id: "cond-1".to_owned(),
            slug: "btc-updown-5m-1772375100".to_owned(),
            question: "BTC 5m".to_owned(),
            outcome_a_label: "Up".to_owned(),
            outcome_a_token_id: "up-token".to_owned(),
            outcome_b_label: "Down".to_owned(),
            outcome_b_token_id: "down-token".to_owned(),
            end_date: None,
            liquidity_usdc: Decimal::ZERO,
            target_price: None,
            target_price_source: None,
            final_reference_price: None,
        };

        assert!(market.is_btc_5m_market());
        assert_eq!(market.btc_5m_window_start_ts(), Some(1_772_375_100));
    }

    #[test]
    fn alt_5m_targets_use_live_exchange_symbols() {
        let cases = [
            (
                "sol-updown-5m-1772375100",
                MarketTarget::Sol5m,
                "sol-5m",
                "SOLUSDT",
                "SOL-USD",
            ),
            (
                "xrp-updown-5m-1772375100",
                MarketTarget::Xrp5m,
                "xrp-5m",
                "XRPUSDT",
                "XRP-USD",
            ),
            (
                "bnb-updown-5m-1772375100",
                MarketTarget::Bnb5m,
                "bnb-5m",
                "BNBUSDT",
                "BNB-USD",
            ),
        ];

        for (slug, expected, key, binance_symbol, coinbase_product) in cases {
            let target = MarketTarget::from_slug(slug).expect("5m slug should be supported");
            assert_eq!(target, expected);
            assert_eq!(target.as_key(), key);
            assert_eq!(target.binance_symbol(), binance_symbol);
            assert_eq!(target.coinbase_product_id(), coinbase_product);
            assert_eq!(target.window_secs(), 300);
        }
    }

    #[test]
    fn order_book_best_prices_use_top_of_book_from_sorted_levels() {
        let book = OrderBook {
            asset_id: "asset".to_owned(),
            bids: vec![
                BookLevel {
                    price: Decimal::new(10, 2),
                    size: Decimal::ONE,
                },
                BookLevel {
                    price: Decimal::new(25, 2),
                    size: Decimal::ONE,
                },
            ],
            asks: vec![
                BookLevel {
                    price: Decimal::new(99, 2),
                    size: Decimal::ONE,
                },
                BookLevel {
                    price: Decimal::new(52, 2),
                    size: Decimal::ONE,
                },
            ],
            min_order_size: None,
            tick_size: None,
        };

        assert_eq!(
            book.best_bid().map(|level| level.price),
            Some(Decimal::new(25, 2))
        );
        assert_eq!(
            book.best_ask().map(|level| level.price),
            Some(Decimal::new(52, 2))
        );
    }

    #[test]
    fn gamma_market_keeps_explicit_target_price() {
        let market = GammaMarket {
            condition_id: "cond-1".to_owned(),
            slug: "btc-updown-5m-1772375100".to_owned(),
            question: "BTC 5m".to_owned(),
            active: true,
            closed: false,
            archived: Some(false),
            end_date: Some("2026-05-01T00:00:00Z".to_owned()),
            liquidity_num: Some("1000".parse().expect("decimal literal is valid")),
            clob_token_ids: Some("[\"up-token\",\"down-token\"]".to_owned()),
            outcomes: Some("[\"Up\",\"Down\"]".to_owned()),
            neg_risk: Some(false),
            events: vec![GammaEvent {
                event_metadata: Some(GammaEventMetadata {
                    price_to_beat: Some(Decimal::from(67_123_u32)),
                    final_price: Some(Decimal::from(67_456_u32)),
                }),
            }],
        };

        let normalized = market
            .into_binary_market()
            .expect("market should parse")
            .expect("market should stay binary");

        assert_eq!(normalized.target_price, Some(Decimal::from(67_123_u32)));
        assert_eq!(
            normalized.final_reference_price,
            Some(Decimal::from(67_456_u32))
        );
        assert_eq!(
            normalized.target_price_source,
            Some(TargetPriceSource::PolymarketEventMetadata)
        );
    }
}
