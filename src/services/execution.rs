//! Paper and live execution backends.

use std::env;
use std::io::{self, IsTerminal, Write};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use tokio::sync::Mutex;

use crate::config::{LiveConfig, LiveSignatureType};
use crate::error::{AppError, Result};
use crate::models::{
    BookFillLevel, ExecutionReport, Opportunity, OpportunityKind, OrderBook, PaperOutcomeSide,
    PaperPosition, PaperPositionLeg, PaperState,
};

use super::binance::{MarketWindowResolution, WindowDirection};

pub(crate) const MAX_MARK_TO_MARKET_BID_LEVELS: usize = 3;
const BPS_DENOMINATOR: u32 = 10_000;
static PAPER_POSITION_SEQUENCE: AtomicU64 = AtomicU64::new(1);

/// Execution abstraction used by the runner.
#[async_trait]
pub trait TradeExecutor: Send + Sync {
    /// Execute a bundle opportunity.
    async fn execute(&self, opportunity: &Opportunity) -> Result<ExecutionReport>;
}

/// In-memory paper executor.
#[derive(Debug, Clone, Default)]
pub struct PaperExecutor {
    state: Arc<Mutex<PaperState>>,
    cost_model: PaperCostModel,
}

/// Conservative paper execution costs applied to simulated fills.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub struct PaperCostModel {
    fee_bps: u32,
    slippage_bps: u32,
}

#[derive(Debug, Clone, Copy)]
struct PaperEntryAccounting {
    total_spent: Decimal,
    fee: Decimal,
    slippage_cost: Decimal,
    expected_profit: Decimal,
}

impl PaperCostModel {
    /// Build a cost model from basis-point fee and slippage assumptions.
    #[must_use]
    pub const fn new(fee_bps: u32, slippage_bps: u32) -> Self {
        Self {
            fee_bps,
            slippage_bps,
        }
    }

    /// Total cash debited for an entry notional after modeled costs.
    #[must_use]
    pub fn gross_entry_spend(self, notional_usdc: Decimal) -> Decimal {
        (notional_usdc + self.fee_for(notional_usdc) + self.slippage_for(notional_usdc)).round_dp(6)
    }

    fn entry_accounting(self, opportunity: &Opportunity) -> PaperEntryAccounting {
        let fee = self.fee_for(opportunity.required_usdc);
        let slippage_cost = self.slippage_for(opportunity.required_usdc);
        let total_spent = (opportunity.required_usdc + fee + slippage_cost).round_dp(6);
        let expected_profit = (opportunity.expected_payout - total_spent).round_dp(6);
        PaperEntryAccounting {
            total_spent,
            fee,
            slippage_cost,
            expected_profit,
        }
    }

    /// Net cash received after applying modeled exit costs.
    ///
    /// The returned tuple is `(net_payout, fee, slippage_cost)`.
    #[must_use]
    pub fn net_exit_payout(self, gross_payout_usdc: Decimal) -> (Decimal, Decimal, Decimal) {
        let fee_usdc = self.fee_for(gross_payout_usdc);
        let slippage_usdc = self.slippage_for(gross_payout_usdc);
        let net_payout_usdc = (gross_payout_usdc - fee_usdc - slippage_usdc)
            .max(Decimal::ZERO)
            .round_dp(6);
        (net_payout_usdc, fee_usdc, slippage_usdc)
    }

    fn fee_for(self, notional_usdc: Decimal) -> Decimal {
        cost_from_bps(notional_usdc, self.fee_bps)
    }

    fn slippage_for(self, notional_usdc: Decimal) -> Decimal {
        cost_from_bps(notional_usdc, self.slippage_bps)
    }
}

fn cost_from_bps(notional_usdc: Decimal, bps: u32) -> Decimal {
    if bps == 0 || notional_usdc <= Decimal::ZERO {
        return Decimal::ZERO;
    }

    (notional_usdc * Decimal::from(bps) / Decimal::from(BPS_DENOMINATOR)).round_dp(6)
}

/// Result of auto-settling one paper position.
#[derive(Debug, Clone)]
pub struct PaperCloseReport {
    pub position_id: String,
    pub closed_at: DateTime<Utc>,
    pub slug: String,
    pub condition_id: String,
    pub question: String,
    pub kind: OpportunityKind,
    pub dominant_outcome_at_entry: String,
    pub primary_outcome_at_entry: String,
    pub actual_outcome: WindowDirection,
    pub realized_payout_usdc: Decimal,
    pub realized_profit_usdc: Decimal,
    pub close_reason: String,
    pub holding_seconds: i64,
    pub spent_usdc: Decimal,
}

impl PaperExecutor {
    /// Create a new paper executor.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a paper executor from previously persisted state.
    #[must_use]
    pub fn with_state(state: PaperState) -> Self {
        Self::with_state_and_costs(state, PaperCostModel::default())
    }

    /// Create a paper executor from state plus explicit execution costs.
    #[must_use]
    pub fn with_state_and_costs(state: PaperState, cost_model: PaperCostModel) -> Self {
        Self {
            state: Arc::new(Mutex::new(state)),
            cost_model,
        }
    }

    /// Apply a successful fill to the in-memory risk state.
    pub async fn record_fill(&self, opportunity: &Opportunity) -> PaperState {
        let mut state = self.state.lock().await;
        apply_fill(&mut state, opportunity, self.cost_model);
        state.clone()
    }

    /// Auto-settle one paper position once the underlying window is resolved.
    pub async fn close_position(
        &self,
        slug: &str,
        resolution: &MarketWindowResolution,
        close_reason: &str,
    ) -> Option<PaperCloseReport> {
        let mut state = self.state.lock().await;
        let position = state.open_positions.remove(slug)?;
        let closed_at = Utc::now();
        let primary_outcome_at_entry = paper_position_primary_outcome_label(&position);
        let payout = realized_payout(&position, resolution.actual_outcome).round_dp(6);
        let realized_profit = (payout - position.spent_usdc).round_dp(6);
        state.market_notional.remove(&position.condition_id);
        state.total_realized_payout += payout;
        state.total_realized_profit += realized_profit;
        state.closed_position_count += 1;

        Some(PaperCloseReport {
            position_id: position.position_id,
            closed_at,
            slug: position.slug,
            condition_id: position.condition_id,
            question: position.question,
            kind: position.kind,
            dominant_outcome_at_entry: position.dominant_outcome_at_entry,
            primary_outcome_at_entry,
            actual_outcome: resolution.actual_outcome,
            realized_payout_usdc: payout,
            realized_profit_usdc: realized_profit,
            close_reason: close_reason.to_owned(),
            holding_seconds: closed_at
                .signed_duration_since(position.opened_at)
                .num_seconds()
                .max(0),
            spent_usdc: position.spent_usdc,
        })
    }

    /// Close a paper position against current market bids before the window settles.
    pub async fn close_position_mark_to_market(
        &self,
        slug: &str,
        books: &std::collections::HashMap<String, OrderBook>,
        close_reason: &str,
    ) -> Option<PaperCloseReport> {
        let mut state = self.state.lock().await;
        let position = state.open_positions.remove(slug)?;
        let closed_at = Utc::now();
        let primary_outcome_at_entry = paper_position_primary_outcome_label(&position);
        let gross_payout = mark_to_market_payout(&position, books).round_dp(6);
        let (payout, exit_fee, exit_slippage) = self.cost_model.net_exit_payout(gross_payout);
        state.market_notional.remove(&position.condition_id);
        state.total_realized_payout += payout;
        state.total_fees_usdc += exit_fee;
        state.total_slippage_cost_usdc += exit_slippage;
        let realized_profit = (payout - position.spent_usdc).round_dp(6);
        state.total_realized_profit += realized_profit;
        state.closed_position_count += 1;

        Some(PaperCloseReport {
            position_id: position.position_id,
            closed_at,
            slug: position.slug,
            condition_id: position.condition_id,
            question: position.question,
            kind: position.kind,
            dominant_outcome_at_entry: position.dominant_outcome_at_entry,
            primary_outcome_at_entry,
            actual_outcome: WindowDirection::Flat,
            realized_payout_usdc: payout,
            realized_profit_usdc: realized_profit,
            close_reason: close_reason.to_owned(),
            holding_seconds: closed_at
                .signed_duration_since(position.opened_at)
                .num_seconds()
                .max(0),
            spent_usdc: position.spent_usdc,
        })
    }

    /// Partially close a paper position against current market bids.
    ///
    /// The remaining fraction stays open and keeps its original entry metadata.
    pub async fn close_position_mark_to_market_partial(
        &self,
        slug: &str,
        books: &std::collections::HashMap<String, OrderBook>,
        close_reason: &str,
        close_fraction: Decimal,
    ) -> Option<PaperCloseReport> {
        let fraction = close_fraction.max(Decimal::ZERO).min(Decimal::ONE);
        if fraction <= Decimal::ZERO {
            return None;
        }

        if fraction >= Decimal::ONE {
            return self
                .close_position_mark_to_market(slug, books, close_reason)
                .await;
        }

        let mut state = self.state.lock().await;
        let mut position = state.open_positions.remove(slug)?;
        let closed_at = Utc::now();
        let primary_outcome_at_entry = paper_position_primary_outcome_label(&position);
        let original_spent = position.spent_usdc;
        let closed_spent = (original_spent * fraction).round_dp(6);
        if closed_spent <= Decimal::ZERO {
            state.open_positions.insert(slug.to_owned(), position);
            return None;
        }
        let expected_profit_closed = (position.expected_profit_usdc * fraction).round_dp(6);

        let mut closed_legs = Vec::with_capacity(position.legs.len());
        for leg in &mut position.legs {
            let closed_shares = (leg.shares * fraction).round_dp(6).min(leg.shares);
            if closed_shares <= Decimal::ZERO {
                continue;
            }
            closed_legs.push(PaperPositionLeg {
                label: leg.label.clone(),
                side: leg.side,
                token_id: leg.token_id.clone(),
                shares: closed_shares,
                entry_price: leg.entry_price,
            });
            leg.shares = (leg.shares - closed_shares).round_dp(6);
        }

        position.legs.retain(|leg| leg.shares > Decimal::ZERO);

        let gross_payout = mark_to_market_payout_for_legs(&closed_legs, books).round_dp(6);
        let (payout, exit_fee, exit_slippage) = self.cost_model.net_exit_payout(gross_payout);
        let realized_profit = (payout - closed_spent).round_dp(6);
        state.total_realized_payout += payout;
        state.total_fees_usdc += exit_fee;
        state.total_slippage_cost_usdc += exit_slippage;
        state.total_realized_profit += realized_profit;

        if let Some(market_notional) = state.market_notional.get_mut(&position.condition_id) {
            *market_notional = (*market_notional - closed_spent)
                .max(Decimal::ZERO)
                .round_dp(6);
            if *market_notional <= Decimal::ZERO {
                state.market_notional.remove(&position.condition_id);
            }
        }

        let remaining_spent = (original_spent - closed_spent).round_dp(6);
        let is_fully_closed = remaining_spent <= Decimal::ZERO || position.legs.is_empty();
        if is_fully_closed {
            state.closed_position_count += 1;
        } else {
            position.spent_usdc = remaining_spent;
            position.expected_profit_usdc =
                (position.expected_profit_usdc - expected_profit_closed).max(Decimal::ZERO);
            position.partial_reversal_exits = position.partial_reversal_exits.saturating_add(1);
            state
                .open_positions
                .insert(slug.to_owned(), position.clone());
        }

        Some(PaperCloseReport {
            position_id: position.position_id.clone(),
            closed_at,
            slug: position.slug.clone(),
            condition_id: position.condition_id.clone(),
            question: position.question.clone(),
            kind: position.kind,
            dominant_outcome_at_entry: position.dominant_outcome_at_entry.clone(),
            primary_outcome_at_entry,
            actual_outcome: WindowDirection::Flat,
            realized_payout_usdc: payout,
            realized_profit_usdc: realized_profit,
            close_reason: close_reason.to_owned(),
            holding_seconds: closed_at
                .signed_duration_since(position.opened_at)
                .num_seconds()
                .max(0),
            spent_usdc: closed_spent,
        })
    }

    /// Snapshot current simulated state.
    pub async fn snapshot(&self) -> PaperState {
        self.state.lock().await.clone()
    }
}

#[async_trait]
impl TradeExecutor for PaperExecutor {
    async fn execute(&self, opportunity: &Opportunity) -> Result<ExecutionReport> {
        let mut state = self.state.lock().await;
        let accounting = self.cost_model.entry_accounting(opportunity);
        apply_fill_with_accounting(&mut state, opportunity, accounting);
        let details = match opportunity.kind {
            OpportunityKind::BundleArbitrage => format!(
                "paper bundle execution {}@{} {}@{}",
                opportunity.outcome_a_label,
                opportunity.outcome_a_ask_price,
                opportunity.outcome_b_label,
                opportunity.outcome_b_ask_price
            ),
            OpportunityKind::DirectionalMomentum
            | OpportunityKind::TargetStateV1
            | OpportunityKind::BonereaperStateV1
            | OpportunityKind::BonereaperStateV2
            | OpportunityKind::BonereaperStateGuarded
            | OpportunityKind::CodexSentinelV1
            | OpportunityKind::CodexScalpProbeV1 => format!(
                "paper directional execution {}@{}",
                opportunity.primary_outcome_label, opportunity.primary_outcome_ask_price
            ),
            OpportunityKind::MicroBreakout => format!(
                "paper micro-breakout execution {}@{}",
                opportunity.primary_outcome_label, opportunity.primary_outcome_ask_price
            ),
            OpportunityKind::DirectionalMomentumHedged => format!(
                "paper dir+hedge execution {}@{} + {}@{}",
                opportunity.primary_outcome_label,
                opportunity.primary_outcome_ask_price,
                opportunity
                    .hedge_outcome_label
                    .as_deref()
                    .unwrap_or("hedge"),
                opportunity.hedge_outcome_ask_price.unwrap_or(Decimal::ZERO)
            ),
        };
        Ok(ExecutionReport {
            mode: "paper".to_owned(),
            action: "open".to_owned(),
            slug: opportunity.slug.clone(),
            condition_id: opportunity.condition_id.clone(),
            question: opportunity.question.clone(),
            shares: opportunity.tradable_shares,
            spent_usdc: accounting.total_spent,
            expected_profit: accounting.expected_profit,
            details,
        })
    }
}

fn apply_fill(state: &mut PaperState, opportunity: &Opportunity, cost_model: PaperCostModel) {
    let accounting = cost_model.entry_accounting(opportunity);
    apply_fill_with_accounting(state, opportunity, accounting);
}

fn apply_fill_with_accounting(
    state: &mut PaperState,
    opportunity: &Opportunity,
    accounting: PaperEntryAccounting,
) {
    let market_entry = state
        .market_notional
        .entry(opportunity.condition_id.clone())
        .or_default();
    *market_entry += accounting.total_spent;
    let new_position = build_paper_position(opportunity, Utc::now(), accounting);
    if let Some(existing) = state.open_positions.get_mut(&opportunity.slug) {
        merge_paper_position(existing, new_position);
    } else {
        state
            .open_positions
            .insert(opportunity.slug.clone(), new_position);
    }
    state.total_spent_usdc += accounting.total_spent;
    state.total_fees_usdc += accounting.fee;
    state.total_slippage_cost_usdc += accounting.slippage_cost;
    state.total_expected_profit += accounting.expected_profit;
}

fn build_paper_position(
    opportunity: &Opportunity,
    opened_at: DateTime<Utc>,
    accounting: PaperEntryAccounting,
) -> PaperPosition {
    let mut legs = Vec::with_capacity(2);
    match opportunity.kind {
        OpportunityKind::BundleArbitrage => {
            legs.push(PaperPositionLeg {
                label: opportunity.outcome_a_label.clone(),
                side: PaperOutcomeSide::from_label(&opportunity.outcome_a_label),
                token_id: opportunity.outcome_a_token_id.clone(),
                shares: opportunity.tradable_shares,
                entry_price: opportunity.outcome_a_ask_price,
            });
            legs.push(PaperPositionLeg {
                label: opportunity.outcome_b_label.clone(),
                side: PaperOutcomeSide::from_label(&opportunity.outcome_b_label),
                token_id: opportunity.outcome_b_token_id.clone(),
                shares: opportunity.tradable_shares,
                entry_price: opportunity.outcome_b_ask_price,
            });
        }
        OpportunityKind::DirectionalMomentum
        | OpportunityKind::TargetStateV1
        | OpportunityKind::BonereaperStateV1
        | OpportunityKind::BonereaperStateV2
        | OpportunityKind::BonereaperStateGuarded
        | OpportunityKind::CodexSentinelV1
        | OpportunityKind::CodexScalpProbeV1
        | OpportunityKind::MicroBreakout => {
            legs.push(PaperPositionLeg {
                label: opportunity.primary_outcome_label.clone(),
                side: PaperOutcomeSide::from_label(&opportunity.primary_outcome_label),
                token_id: opportunity.primary_outcome_token_id.clone(),
                shares: opportunity.tradable_shares,
                entry_price: average_fill_price(
                    &opportunity.primary_fill_levels,
                    opportunity.primary_outcome_ask_price,
                ),
            });
        }
        OpportunityKind::DirectionalMomentumHedged => {
            legs.push(PaperPositionLeg {
                label: opportunity.primary_outcome_label.clone(),
                side: PaperOutcomeSide::from_label(&opportunity.primary_outcome_label),
                token_id: opportunity.primary_outcome_token_id.clone(),
                shares: opportunity.tradable_shares,
                entry_price: average_fill_price(
                    &opportunity.primary_fill_levels,
                    opportunity.primary_outcome_ask_price,
                ),
            });
            if let (Some(label), Some(token_id), Some(entry_price)) = (
                opportunity.hedge_outcome_label.as_ref(),
                opportunity.hedge_outcome_token_id.as_ref(),
                opportunity.hedge_outcome_ask_price,
            ) {
                legs.push(PaperPositionLeg {
                    label: label.clone(),
                    side: PaperOutcomeSide::from_label(label),
                    token_id: token_id.clone(),
                    shares: opportunity.hedge_shares,
                    entry_price: average_fill_price(&opportunity.hedge_fill_levels, entry_price),
                });
            }
        }
    }
    PaperPosition {
        position_id: next_paper_position_id(opened_at),
        opened_at,
        scheduled_close_at: scheduled_close_at_for_slug(&opportunity.slug),
        condition_id: opportunity.condition_id.clone(),
        slug: opportunity.slug.clone(),
        question: opportunity.question.clone(),
        kind: opportunity.kind,
        dominant_outcome_at_entry: opportunity.dominant_outcome.clone(),
        spot_move_bps_at_entry: opportunity.spot_move_bps,
        spent_usdc: accounting.total_spent,
        expected_profit_usdc: accounting.expected_profit,
        entry_count: 1,
        partial_reversal_exits: 0,
        best_entry_reference_price: paper_entry_reference_price(opportunity),
        legs,
    }
}

fn next_paper_position_id(opened_at: DateTime<Utc>) -> String {
    format!(
        "paper-pos-{}-{}",
        opened_at.timestamp_micros(),
        PAPER_POSITION_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    )
}

fn merge_paper_position(existing: &mut PaperPosition, addition: PaperPosition) {
    existing.spent_usdc += addition.spent_usdc;
    existing.expected_profit_usdc += addition.expected_profit_usdc;
    existing.spot_move_bps_at_entry = addition.spot_move_bps_at_entry;
    existing.dominant_outcome_at_entry = addition.dominant_outcome_at_entry;
    existing.entry_count = existing
        .entry_count
        .saturating_add(addition.entry_count.max(1));
    existing.best_entry_reference_price = merged_best_entry_reference_price(
        existing.best_entry_reference_price,
        addition.best_entry_reference_price,
    );
    existing.scheduled_close_at =
        earliest_close_at(existing.scheduled_close_at, addition.scheduled_close_at);

    for leg in addition.legs {
        merge_paper_leg(&mut existing.legs, leg);
    }
}

fn paper_entry_reference_price(opportunity: &Opportunity) -> Decimal {
    match opportunity.kind {
        OpportunityKind::BundleArbitrage => opportunity.bundle_cost,
        OpportunityKind::DirectionalMomentum
        | OpportunityKind::TargetStateV1
        | OpportunityKind::BonereaperStateV1
        | OpportunityKind::BonereaperStateV2
        | OpportunityKind::BonereaperStateGuarded
        | OpportunityKind::CodexSentinelV1
        | OpportunityKind::CodexScalpProbeV1
        | OpportunityKind::MicroBreakout => opportunity.primary_outcome_ask_price,
        OpportunityKind::DirectionalMomentumHedged => {
            opportunity.primary_outcome_ask_price
                + opportunity.hedge_outcome_ask_price.unwrap_or(Decimal::ZERO)
        }
    }
}

fn average_fill_price(levels: &[BookFillLevel], fallback_price: Decimal) -> Decimal {
    let total_shares = levels.iter().map(|level| level.shares).sum::<Decimal>();
    if total_shares <= Decimal::ZERO {
        return fallback_price;
    }

    let total_cost = levels
        .iter()
        .map(|level| level.price * level.shares)
        .sum::<Decimal>();
    if total_cost <= Decimal::ZERO {
        fallback_price
    } else {
        (total_cost / total_shares).round_dp(6)
    }
}

fn merged_best_entry_reference_price(current: Decimal, next: Decimal) -> Decimal {
    match (current > Decimal::ZERO, next > Decimal::ZERO) {
        (true, true) => current.min(next),
        (true, false) => current,
        (false, true) => next,
        (false, false) => Decimal::ZERO,
    }
}

fn merge_paper_leg(legs: &mut Vec<PaperPositionLeg>, addition: PaperPositionLeg) {
    if let Some(existing) = legs.iter_mut().find(|leg| {
        leg.token_id == addition.token_id
            && leg.side == addition.side
            && leg.label == addition.label
    }) {
        let combined_shares = existing.shares + addition.shares;
        if combined_shares > Decimal::ZERO {
            let weighted_cost =
                (existing.entry_price * existing.shares) + (addition.entry_price * addition.shares);
            existing.entry_price = (weighted_cost / combined_shares).round_dp(6);
        }
        existing.shares = combined_shares.round_dp(6);
    } else {
        legs.push(addition);
    }
}

fn earliest_close_at(
    current: Option<DateTime<Utc>>,
    next: Option<DateTime<Utc>>,
) -> Option<DateTime<Utc>> {
    match (current, next) {
        (Some(current), Some(next)) => Some(current.min(next)),
        (Some(current), None) => Some(current),
        (None, Some(next)) => Some(next),
        (None, None) => None,
    }
}

fn scheduled_close_at_for_slug(slug: &str) -> Option<DateTime<Utc>> {
    let target = crate::models::MarketTarget::from_slug(slug)?;
    let start_ts = slug
        .strip_prefix(target.slug_prefix())?
        .parse::<i64>()
        .ok()?;
    DateTime::from_timestamp(start_ts + target.window_secs(), 0)
}

fn paper_position_primary_outcome_label(position: &PaperPosition) -> String {
    position
        .legs
        .iter()
        .filter(|leg| leg.side != PaperOutcomeSide::Unknown)
        .max_by(|left, right| {
            left.shares
                .partial_cmp(&right.shares)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map_or_else(
            || position.dominant_outcome_at_entry.clone(),
            |leg| leg.label.clone(),
        )
}

fn realized_payout(position: &PaperPosition, actual_outcome: WindowDirection) -> Decimal {
    let winning_side = match actual_outcome {
        WindowDirection::Up => PaperOutcomeSide::Up,
        WindowDirection::Down => PaperOutcomeSide::Down,
        WindowDirection::Flat => PaperOutcomeSide::Unknown,
    };

    position
        .legs
        .iter()
        .filter(|leg| leg.side == winning_side)
        .map(|leg| leg.shares)
        .sum::<Decimal>()
}

fn mark_to_market_payout(
    position: &PaperPosition,
    books: &std::collections::HashMap<String, OrderBook>,
) -> Decimal {
    mark_to_market_payout_for_legs(&position.legs, books)
}

pub(crate) fn mark_to_market_payout_for_legs(
    legs: &[PaperPositionLeg],
    books: &std::collections::HashMap<String, OrderBook>,
) -> Decimal {
    legs.iter()
        .map(|leg| {
            books.get(&leg.token_id).map_or(Decimal::ZERO, |book| {
                mark_to_market_revenue_for_shares(book, leg.shares)
            })
        })
        .sum::<Decimal>()
        .round_dp(6)
}

fn mark_to_market_revenue_for_shares(book: &OrderBook, shares: Decimal) -> Decimal {
    let mut remaining_shares = shares.max(Decimal::ZERO);
    let mut revenue = Decimal::ZERO;

    for level in book.bids.iter().rev().take(MAX_MARK_TO_MARKET_BID_LEVELS) {
        if remaining_shares <= Decimal::ZERO || level.size <= Decimal::ZERO {
            break;
        }

        let fill_shares = remaining_shares.min(level.size).round_dp(6);
        if fill_shares <= Decimal::ZERO {
            continue;
        }

        revenue += (fill_shares * level.price).round_dp(6);
        remaining_shares = (remaining_shares - fill_shares).max(Decimal::ZERO);
    }

    revenue.round_dp(6)
}

/// Live executor backed by the official Polymarket Rust SDK.
pub struct LiveExecutor {
    inner: LiveSdk,
}

impl LiveExecutor {
    /// Build a new live executor.
    ///
    /// # Errors
    ///
    /// Returns an error if the private key is missing or SDK authentication fails.
    pub async fn new(clob_base_url: &str, live_config: &LiveConfig) -> Result<Self> {
        Ok(Self {
            inner: LiveSdk::connect(clob_base_url, live_config).await?,
        })
    }

    /// Validate live credentials without placing any orders.
    ///
    /// # Errors
    ///
    /// Returns an error if authentication fails or the authenticated API probes fail.
    pub async fn auth_check(
        clob_base_url: &str,
        live_config: &LiveConfig,
    ) -> Result<AuthCheckReport> {
        LiveSdk::connect(clob_base_url, live_config)
            .await?
            .auth_check()
            .await
    }
}

#[async_trait]
impl TradeExecutor for LiveExecutor {
    async fn execute(&self, opportunity: &Opportunity) -> Result<ExecutionReport> {
        self.inner.execute_bundle(opportunity).await
    }
}

/// Safe summary of a live-auth verification.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AuthCheckReport {
    /// Result returned by the CLOB health endpoint.
    pub api_status: String,
    /// Wallet address used to authenticate.
    pub wallet_address: String,
    /// Active L2 API key UUID used by the SDK.
    pub api_key: String,
    /// Whether credentials were supplied directly or derived automatically.
    pub credential_mode: &'static str,
    /// Signature type selected for the live client.
    pub signature_type: &'static str,
    /// How the live funder address is determined.
    pub funder_mode: &'static str,
    /// Explicit funder address when configured.
    pub funder_address: Option<String>,
}

struct LiveSdk {
    client: polymarket_client_sdk::clob::Client<
        polymarket_client_sdk::auth::state::Authenticated<polymarket_client_sdk::auth::Normal>,
    >,
    private_key: String,
    auth_context: LiveAuthContext,
}

impl LiveSdk {
    async fn connect(clob_base_url: &str, live_config: &LiveConfig) -> Result<Self> {
        use polymarket_client_sdk::POLYGON;
        use polymarket_client_sdk::auth::Signer as _;
        use polymarket_client_sdk::clob::{Client, Config};

        let resolved_auth = resolve_live_auth(live_config)?;
        let signer = polymarket_client_sdk::auth::LocalSigner::from_str(&resolved_auth.private_key)
            .map_err(|error| AppError::Sdk(error.to_string()))?
            .with_chain_id(Some(POLYGON));

        let mut authentication_builder = Client::new(clob_base_url, Config::default())
            .map_err(|error| AppError::Sdk(error.to_string()))?
            .authentication_builder(&signer)
            .signature_type(resolved_auth.signature_type);

        if let Some(credentials) = resolved_auth.credentials {
            authentication_builder = authentication_builder.credentials(credentials);
        }

        if let Some(funder) = resolved_auth.funder {
            authentication_builder = authentication_builder.funder(funder);
        }

        let client = authentication_builder
            .authenticate()
            .await
            .map_err(|error| AppError::Sdk(error.to_string()))?;

        Ok(Self {
            client,
            private_key: resolved_auth.private_key,
            auth_context: LiveAuthContext {
                credential_mode: resolved_auth.credential_mode,
                signature_type: resolved_auth.signature_type_label,
                funder_mode: resolved_auth.funder_mode,
                funder_address: resolved_auth.funder_address,
            },
        })
    }

    async fn execute_bundle(&self, opportunity: &Opportunity) -> Result<ExecutionReport> {
        match opportunity.kind {
            OpportunityKind::BundleArbitrage | OpportunityKind::DirectionalMomentumHedged => {
                return Err(AppError::LiveExecution(
                    "multi-leg live execution is disabled until reconciliation is implemented"
                        .to_owned(),
                ));
            }
            OpportunityKind::DirectionalMomentum
            | OpportunityKind::TargetStateV1
            | OpportunityKind::BonereaperStateV1
            | OpportunityKind::BonereaperStateV2
            | OpportunityKind::BonereaperStateGuarded
            | OpportunityKind::CodexSentinelV1
            | OpportunityKind::CodexScalpProbeV1
            | OpportunityKind::MicroBreakout => {
                self.execute_directional(opportunity).await?;
            }
        }
        let details = match opportunity.kind {
            OpportunityKind::BundleArbitrage => format!(
                "live FOK execution {}@{} {}@{}",
                opportunity.outcome_a_label,
                opportunity.outcome_a_ask_price,
                opportunity.outcome_b_label,
                opportunity.outcome_b_ask_price
            ),
            OpportunityKind::DirectionalMomentum
            | OpportunityKind::TargetStateV1
            | OpportunityKind::BonereaperStateV1
            | OpportunityKind::BonereaperStateV2
            | OpportunityKind::BonereaperStateGuarded
            | OpportunityKind::CodexSentinelV1
            | OpportunityKind::CodexScalpProbeV1 => format!(
                "live FOK execution {}@{}",
                opportunity.primary_outcome_label, opportunity.primary_outcome_ask_price
            ),
            OpportunityKind::MicroBreakout => format!(
                "live FOK execution micro-breakout {}@{}",
                opportunity.primary_outcome_label, opportunity.primary_outcome_ask_price
            ),
            OpportunityKind::DirectionalMomentumHedged => format!(
                "live FOK execution {}@{} + {}@{}",
                opportunity.primary_outcome_label,
                opportunity.primary_outcome_ask_price,
                opportunity
                    .hedge_outcome_label
                    .as_deref()
                    .unwrap_or("hedge"),
                opportunity.hedge_outcome_ask_price.unwrap_or(Decimal::ZERO)
            ),
        };
        Ok(ExecutionReport {
            mode: "live".to_owned(),
            action: "open".to_owned(),
            slug: opportunity.slug.clone(),
            condition_id: opportunity.condition_id.clone(),
            question: opportunity.question.clone(),
            shares: opportunity.tradable_shares,
            spent_usdc: opportunity.required_usdc,
            expected_profit: opportunity.expected_profit,
            details,
        })
    }

    async fn auth_check(&self) -> Result<AuthCheckReport> {
        let api_status = self
            .client
            .ok()
            .await
            .map_err(|error| AppError::Sdk(error.to_string()))?;
        drop(
            self.client
                .api_keys()
                .await
                .map_err(|error| AppError::Sdk(error.to_string()))?,
        );

        Ok(AuthCheckReport {
            api_status,
            wallet_address: self.client.address().to_string(),
            api_key: self.client.credentials().key().to_string(),
            credential_mode: self.auth_context.credential_mode.as_str(),
            signature_type: self.auth_context.signature_type,
            funder_mode: self.auth_context.funder_mode.as_str(),
            funder_address: self.auth_context.funder_address.clone(),
        })
    }

    async fn buy_leg(&self, token_id: &str, ask_price: Decimal, shares: Decimal) -> Result<()> {
        use polymarket_client_sdk::POLYGON;
        use polymarket_client_sdk::auth::Signer as _;
        use polymarket_client_sdk::clob::types::{Amount, OrderType, Side};
        use polymarket_client_sdk::types::U256;

        let notional = shares * ask_price;
        let sdk_amount = polymarket_client_sdk::types::Decimal::from_str(&notional.to_string())
            .map_err(|error| AppError::Sdk(error.to_string()))?;
        let sdk_price = polymarket_client_sdk::types::Decimal::from_str(&ask_price.to_string())
            .map_err(|error| AppError::Sdk(error.to_string()))?;
        let token_id =
            U256::from_str(token_id).map_err(|error| AppError::Sdk(error.to_string()))?;
        let signer = polymarket_client_sdk::auth::LocalSigner::from_str(&self.private_key)
            .map_err(|error| AppError::Sdk(error.to_string()))?
            .with_chain_id(Some(POLYGON));

        let order = self
            .client
            .market_order()
            .token_id(token_id)
            .amount(Amount::usdc(sdk_amount).map_err(|error| AppError::Sdk(error.to_string()))?)
            .side(Side::Buy)
            .order_type(OrderType::FOK)
            .price(sdk_price)
            .build()
            .await
            .map_err(|error| AppError::Sdk(error.to_string()))?;

        let signed_order = self
            .client
            .sign(&signer, order)
            .await
            .map_err(|error| AppError::Sdk(error.to_string()))?;

        self.client
            .post_order(signed_order)
            .await
            .map_err(|error| AppError::LiveExecution(error.to_string()))?;

        Ok(())
    }

    async fn execute_directional(&self, opportunity: &Opportunity) -> Result<()> {
        self.execute_fill_plan(
            &opportunity.primary_outcome_token_id,
            opportunity.primary_outcome_ask_price,
            opportunity.tradable_shares,
            &opportunity.primary_fill_levels,
        )
        .await
    }

    async fn execute_fill_plan(
        &self,
        token_id: &str,
        fallback_ask_price: Decimal,
        total_shares: Decimal,
        fill_levels: &[BookFillLevel],
    ) -> Result<()> {
        if fill_levels.is_empty() {
            return self
                .buy_leg(token_id, fallback_ask_price, total_shares)
                .await;
        }

        for level in fill_levels {
            if level.shares <= Decimal::ZERO {
                continue;
            }
            self.buy_leg(token_id, level.price, level.shares).await?;
        }

        Ok(())
    }
}

struct ResolvedLiveAuth {
    private_key: String,
    credentials: Option<polymarket_client_sdk::auth::Credentials>,
    credential_mode: LiveCredentialsMode,
    signature_type: polymarket_client_sdk::clob::types::SignatureType,
    signature_type_label: &'static str,
    funder: Option<polymarket_client_sdk::types::Address>,
    funder_mode: LiveFunderMode,
    funder_address: Option<String>,
}

struct ResolvedCredentials {
    credentials: Option<polymarket_client_sdk::auth::Credentials>,
    mode: LiveCredentialsMode,
}

struct LiveAuthContext {
    credential_mode: LiveCredentialsMode,
    signature_type: &'static str,
    funder_mode: LiveFunderMode,
    funder_address: Option<String>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum LiveCredentialsMode {
    Provided,
    Derived,
}

impl LiveCredentialsMode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Provided => "заданы вручную",
            Self::Derived => "получены автоматически",
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum LiveFunderMode {
    NotUsed,
    Explicit,
    AutoDerived,
}

impl LiveFunderMode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::NotUsed => "не используется",
            Self::Explicit => "задан явно",
            Self::AutoDerived => "вычислен автоматически",
        }
    }
}

fn resolve_live_auth(live_config: &LiveConfig) -> Result<ResolvedLiveAuth> {
    use polymarket_client_sdk::clob::types::SignatureType;
    use polymarket_client_sdk::types::Address;

    let private_key = resolve_required_secret(
        &live_config.private_key_env,
        live_config.prompt_for_secrets,
        "Приватный ключ Polymarket (обязателен для подписи live-ордеров): ",
    )?;

    let resolved_credentials = resolve_optional_credentials(live_config)?;
    let funder = match live_config
        .funder_address
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        Some(value) => Some(Address::from_str(value).map_err(|error| {
            AppError::InvalidLiveAuth(format!("некорректный адрес funder: {error}"))
        })?),
        None => None,
    };

    let (signature_type, signature_type_label) = match live_config.signature_type {
        LiveSignatureType::Eoa => (SignatureType::Eoa, "eoa"),
        LiveSignatureType::Proxy => (SignatureType::Proxy, "proxy"),
        LiveSignatureType::GnosisSafe => (SignatureType::GnosisSafe, "gnosis_safe"),
    };
    let (funder_mode, funder_address) = match (funder.as_ref(), signature_type) {
        (Some(address), _) => (LiveFunderMode::Explicit, Some(address.to_string())),
        (None, SignatureType::Proxy | SignatureType::GnosisSafe) => {
            (LiveFunderMode::AutoDerived, None)
        }
        (None, _) => (LiveFunderMode::NotUsed, None),
    };

    Ok(ResolvedLiveAuth {
        private_key,
        credentials: resolved_credentials.credentials,
        credential_mode: resolved_credentials.mode,
        signature_type,
        signature_type_label,
        funder,
        funder_mode,
        funder_address,
    })
}

fn resolve_optional_credentials(live_config: &LiveConfig) -> Result<ResolvedCredentials> {
    use polymarket_client_sdk::auth::{ApiKey, Credentials};

    let mut api_key = read_env_secret(&live_config.api_key_env);
    let mut api_secret = read_env_secret(&live_config.api_secret_env);
    let mut api_passphrase = read_env_secret(&live_config.api_passphrase_env);

    let has_any = api_key.is_some() || api_secret.is_some() || api_passphrase.is_some();
    if !has_any && !live_config.prompt_for_secrets {
        return Ok(ResolvedCredentials {
            credentials: None,
            mode: LiveCredentialsMode::Derived,
        });
    }

    if api_key.is_none()
        && api_secret.is_none()
        && api_passphrase.is_none()
        && live_config.prompt_for_secrets
    {
        let input = prompt_line(
            "API key Polymarket (необязательно, нажмите Enter для авто-derive из приватного ключа): ",
            &live_config.api_key_env,
        )?;
        if !input.is_empty() {
            api_key = Some(input);
            api_secret = Some(resolve_required_secret(
                &live_config.api_secret_env,
                true,
                "API secret Polymarket: ",
            )?);
            api_passphrase = Some(resolve_required_secret(
                &live_config.api_passphrase_env,
                true,
                "API passphrase Polymarket: ",
            )?);
        }
    } else if has_any {
        if api_key.is_none() {
            api_key = Some(resolve_required_secret(
                &live_config.api_key_env,
                live_config.prompt_for_secrets,
                "API key Polymarket: ",
            )?);
        }
        if api_secret.is_none() {
            api_secret = Some(resolve_required_secret(
                &live_config.api_secret_env,
                live_config.prompt_for_secrets,
                "API secret Polymarket: ",
            )?);
        }
        if api_passphrase.is_none() {
            api_passphrase = Some(resolve_required_secret(
                &live_config.api_passphrase_env,
                live_config.prompt_for_secrets,
                "API passphrase Polymarket: ",
            )?);
        }
    }

    match (api_key, api_secret, api_passphrase) {
        (Some(api_key), Some(api_secret), Some(api_passphrase)) => {
            let api_key = ApiKey::from_str(&api_key).map_err(|error| {
                AppError::InvalidLiveAuth(format!("некорректный UUID API key: {error}"))
            })?;
            Ok(ResolvedCredentials {
                credentials: Some(Credentials::new(api_key, api_secret, api_passphrase)),
                mode: LiveCredentialsMode::Provided,
            })
        }
        (None, None, None) => Ok(ResolvedCredentials {
            credentials: None,
            mode: LiveCredentialsMode::Derived,
        }),
        _ => Err(AppError::InvalidLiveAuth(
            "учётные данные API Polymarket должны включать api key, secret и passphrase одновременно"
                .to_owned(),
        )),
    }
}

fn resolve_required_secret(
    env_name: &str,
    prompt_for_secrets: bool,
    prompt: &str,
) -> Result<String> {
    if let Some(value) = read_env_secret(env_name) {
        return Ok(value);
    }

    if !prompt_for_secrets {
        return Err(AppError::MissingEnvVar(env_name.to_owned()));
    }

    ensure_interactive(env_name)?;
    let value = rpassword::prompt_password(prompt)?;
    let trimmed = value.trim().to_owned();
    if trimmed.is_empty() {
        return Err(AppError::MissingEnvVar(env_name.to_owned()));
    }

    Ok(trimmed)
}

fn prompt_line(prompt: &str, env_name: &str) -> Result<String> {
    ensure_interactive(env_name)?;

    let mut stdout = io::stdout();
    stdout.write_all(prompt.as_bytes())?;
    stdout.flush()?;

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    Ok(input.trim().to_owned())
}

fn read_env_secret(env_name: &str) -> Option<String> {
    env::var(env_name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn ensure_interactive(env_name: &str) -> Result<()> {
    if io::stdin().is_terminal() {
        Ok(())
    } else {
        Err(AppError::InteractiveInputUnavailable(env_name.to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use rust_decimal::Decimal;

    use crate::models::{Opportunity, OpportunityKind, PaperState};
    use crate::services::binance::{MarketWindowResolution, WindowDirection};

    use super::{PaperCostModel, PaperExecutor};

    fn sample_opportunity() -> Opportunity {
        Opportunity {
            kind: OpportunityKind::DirectionalMomentum,
            condition_id: "condition-1".to_owned(),
            slug: "btc-updown-5m-1775221500".to_owned(),
            question: "Will BTC finish higher in 5m?".to_owned(),
            outcome_a_label: "Up".to_owned(),
            outcome_a_token_id: "up-token".to_owned(),
            outcome_b_label: "Down".to_owned(),
            outcome_b_token_id: "down-token".to_owned(),
            liquidity_usdc: Decimal::from(1_000_u32),
            outcome_a_ask_price: Decimal::new(46, 2),
            outcome_b_ask_price: Decimal::new(54, 2),
            bundle_cost: Decimal::ONE,
            net_bundle_cost: Decimal::ONE,
            edge_per_share: Decimal::new(8, 2),
            edge_bps: 80,
            tradable_shares: Decimal::new(10, 0),
            required_usdc: Decimal::new(460, 2),
            expected_payout: Decimal::new(10, 0),
            expected_profit: Decimal::new(54, 2),
            interval_open_price: Decimal::from(66_500_u32),
            target_price: Decimal::from(66_500_u32),
            target_price_source: crate::models::TargetPriceSource::BinanceWindowOpenFallback,
            target_gap_bps: Decimal::new(15, 0),
            current_spot_price: Decimal::from(66_600_u32),
            spot_move_bps: Decimal::new(15, 0),
            spot_move_1s_bps: Decimal::ZERO,
            spot_move_5s_bps: Decimal::ZERO,
            spot_move_15s_bps: Decimal::ZERO,
            micro_acceleration_bps: Decimal::ZERO,
            micro_burst_reference_price: Decimal::from(66_600_u32),
            micro_reference_price: Decimal::from(66_600_u32),
            signal_strength_bps: Decimal::new(15, 0),
            aligned_trade_flow_bps: Decimal::ZERO,
            signal_tier: "soft".to_owned(),
            target_cross_label: "none".to_owned(),
            dominant_outcome: "Рост".to_owned(),
            primary_outcome_label: "Рост".to_owned(),
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

    #[tokio::test]
    async fn paper_execute_opens_position() {
        let executor = PaperExecutor::new();
        let opportunity = sample_opportunity();

        let _report = crate::services::execution::TradeExecutor::execute(&executor, &opportunity)
            .await
            .expect("paper execute should succeed");
        let snapshot = executor.snapshot().await;

        assert_eq!(snapshot.open_positions.len(), 1);
        assert_eq!(
            snapshot.market_notional.get(&opportunity.condition_id),
            Some(&opportunity.required_usdc)
        );
        let position = snapshot
            .open_positions
            .get(&opportunity.slug)
            .expect("position should exist");
        assert_eq!(position.entry_count, 1);
        assert_eq!(
            position.best_entry_reference_price,
            opportunity.primary_outcome_ask_price
        );
    }

    #[tokio::test]
    async fn paper_execute_debits_modeled_fee_from_profit() {
        let executor =
            PaperExecutor::with_state_and_costs(PaperState::default(), PaperCostModel::new(100, 0));
        let opportunity = sample_opportunity();

        let report = crate::services::execution::TradeExecutor::execute(&executor, &opportunity)
            .await
            .expect("paper execute should succeed");
        let snapshot = executor.snapshot().await;
        let fee = Decimal::new(46, 3);
        let gross_spent = opportunity.required_usdc + fee;

        assert_eq!(report.spent_usdc, gross_spent);
        assert_eq!(
            report.expected_profit,
            opportunity.expected_payout - gross_spent
        );
        assert_eq!(snapshot.total_fees_usdc, fee);
        assert_eq!(snapshot.total_spent_usdc, gross_spent);
        assert_eq!(
            snapshot.market_notional.get(&opportunity.condition_id),
            Some(&gross_spent)
        );
    }

    #[tokio::test]
    async fn paper_execute_tracks_best_price_after_second_fill() {
        let executor = PaperExecutor::new();
        let first = sample_opportunity();
        let mut second = sample_opportunity();
        second.primary_outcome_ask_price = Decimal::new(41, 2);

        let _report = crate::services::execution::TradeExecutor::execute(&executor, &first)
            .await
            .expect("first fill should succeed");
        let _report = crate::services::execution::TradeExecutor::execute(&executor, &second)
            .await
            .expect("second fill should succeed");
        let snapshot = executor.snapshot().await;
        let position = snapshot
            .open_positions
            .get(&first.slug)
            .expect("position should exist");

        assert_eq!(position.entry_count, 2);
        assert_eq!(position.best_entry_reference_price, Decimal::new(41, 2));
    }

    #[tokio::test]
    async fn paper_close_position_realizes_profit() {
        let executor = PaperExecutor::new();
        let opportunity = sample_opportunity();
        let _report = crate::services::execution::TradeExecutor::execute(&executor, &opportunity)
            .await
            .expect("paper execute should succeed");

        let resolution = MarketWindowResolution {
            target: crate::models::MarketTarget::Btc5m,
            start_price: Decimal::from(66_500_u32),
            end_price: Decimal::from(66_700_u32),
            realized_move_bps: Decimal::new(30, 0),
            actual_outcome: WindowDirection::Up,
            resolved_at_ms: Utc::now().timestamp_millis(),
        };

        let close_report = executor
            .close_position(
                &opportunity.slug,
                &resolution,
                "окно завершилось, позиция зачтена по факту",
            )
            .await
            .expect("position should close");
        let snapshot = executor.snapshot().await;

        assert!(snapshot.open_positions.is_empty());
        assert_eq!(close_report.realized_payout_usdc, Decimal::new(10, 0));
        assert_eq!(
            close_report.realized_profit_usdc,
            Decimal::new(10, 0) - opportunity.required_usdc
        );
        assert_eq!(
            close_report.primary_outcome_at_entry,
            opportunity.primary_outcome_label
        );
    }
}
