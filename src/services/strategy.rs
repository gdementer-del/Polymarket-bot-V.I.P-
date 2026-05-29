//! Strategy implementation for Polymarket BTC 5-minute markets.

use std::cmp::Ordering;
use std::collections::HashMap;

use chrono::Utc;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

use crate::config::StrategyConfig;
use crate::models::{
    BinaryMarket, BookFillLevel, MarketTarget, Opportunity, OpportunityKind, OrderBook,
};

use super::binance::BtcFiveMinuteContext;
use super::market_data::TradeFlowSummary;
use super::text::{contains_legacy_mojibake, sanitize_legacy_mojibake};

const BPS_FIXED_SCALE: i64 = 1_000;
const MAX_BOOK_SWEEP_LEVELS: usize = 3;

/// Strategy engine for BTC 5-minute Polymarket markets.
#[derive(Debug, Clone)]
pub struct BundleArbitrageStrategy {
    config: StrategyConfig,
}
// Disabled duplicate test scaffold kept only for patch recovery.
/*
    fn strategy_enables_opening_tail_hedge_for_directional_signal() {
        let strategy = BundleArbitrageStrategy::new(strategy_config());
        let market = market();

        let mut books = HashMap::new();
        books.insert(
            "up-token".to_owned(),
            OrderBook {
                asset_id: "up-token".to_owned(),
                bids: Vec::new(),
                asks: vec![BookLevel {
                    price: decimal("0.56"),
                    size: decimal("100"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );
        books.insert(
            "down-token".to_owned(),
            OrderBook {
                asset_id: "down-token".to_owned(),
                bids: Vec::new(),
                asks: vec![BookLevel {
                    price: decimal("0.22"),
                    size: decimal("40"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );

        let mut contexts = HashMap::new();
        contexts.insert(
            market.slug.clone(),
            BtcFiveMinuteContext {
                interval_open_price: decimal("67000"),
                current_spot_price: decimal("67210"),
                current_spot_source: "test-fixture".to_owned(),
                current_spot_event_age_ms: None,
                current_spot_received_age_ms: None,
                current_spot_quote_points: None,
                exchange_book_age_ms: None,
                exchange_book_top_imbalance_bps: Decimal::ZERO,
                exchange_book_depth_imbalance_bps: Decimal::ZERO,
                exchange_book_microprice_bps: Decimal::ZERO,
                exchange_book_spread_bps: Decimal::ZERO,
                micro_reference_price: decimal("67210"),
                spot_move_bps: decimal("31.34"),
                spot_move_1s_bps: Decimal::ZERO,
                spot_move_5s_bps: Decimal::ZERO,
                dominant_outcome: "Up".to_owned(),
                seconds_left: 295,
            },
        );

        let opportunities =
            strategy.find_opportunities(&[market], &books, &HashMap::new(), &contexts);
        assert_eq!(opportunities.len(), 1);
        assert_eq!(
            opportunities[0].kind,
            OpportunityKind::DirectionalMomentumHedged
        );
        assert_eq!(opportunities[0].primary_outcome_label, "Up");
        assert_eq!(opportunities[0].hedge_outcome_label.as_deref(), Some("Down"));
        assert!(opportunities[0].hedge_shares > Decimal::ZERO);
    }
*/

/// A market that is close to producing a valid trade signal.
#[derive(Debug, Clone)]
pub struct NearMiss {
    pub kind: OpportunityKind,
    pub slug: String,
    pub question: String,
    pub dominant_outcome: String,
    pub primary_outcome_label: String,
    pub primary_outcome_ask_price: Option<Decimal>,
    pub bundle_cost: Option<Decimal>,
    pub target_gap_bps: Decimal,
    pub spot_move_bps: Decimal,
    pub spot_move_1s_bps: Decimal,
    pub spot_move_5s_bps: Decimal,
    pub spot_move_15s_bps: Decimal,
    pub micro_acceleration_bps: Decimal,
    pub exchange_book_age_ms: Option<i64>,
    pub exchange_book_top_imbalance_bps: Decimal,
    pub exchange_book_depth_imbalance_bps: Decimal,
    pub seconds_left: i64,
    pub shortfall_bps: u32,
    pub shortfall_label: String,
    pub reason: String,
}

#[derive(Debug, Clone, Copy)]
struct MarketQuotes<'a> {
    up_token: &'a str,
    down_token: &'a str,
    up_ask_price: Decimal,
    up_ask_size: Decimal,
    down_ask_price: Decimal,
    down_ask_size: Decimal,
}

#[derive(Debug, Clone, Copy)]
struct PrimaryQuote<'a> {
    label: &'a str,
    token_id: &'a str,
    ask_price: Decimal,
    ask_size: Decimal,
}

#[derive(Debug, Clone, Copy)]
struct MarketBookRefs<'a> {
    up: &'a OrderBook,
    down: &'a OrderBook,
}

#[derive(Debug, Clone)]
struct BookFillPlan {
    levels: Vec<BookFillLevel>,
    total_shares: Decimal,
    total_cost: Decimal,
    average_price: Decimal,
}

#[derive(Debug, Clone)]
struct ScoredNearMiss {
    near_miss: NearMiss,
    priority_rank: u8,
}

#[derive(Debug, Default)]
struct MarketOpportunitySet {
    bundle: Option<Opportunity>,
    directional: Option<Opportunity>,
    target_state: Option<Opportunity>,
    bonereaper_state: Option<Opportunity>,
    bonereaper_state_v2: Option<Opportunity>,
    bonereaper_state_guarded: Option<Opportunity>,
    codex_sentinel_v1: Option<Opportunity>,
    codex_scalp_probe_v1: Option<Opportunity>,
    micro_breakout: Option<Opportunity>,
}

impl MarketOpportunitySet {
    fn any(&self) -> bool {
        self.bundle.is_some()
            || self.directional.is_some()
            || self.target_state.is_some()
            || self.bonereaper_state.is_some()
            || self.bonereaper_state_v2.is_some()
            || self.bonereaper_state_guarded.is_some()
            || self.codex_sentinel_v1.is_some()
            || self.codex_scalp_probe_v1.is_some()
            || self.micro_breakout.is_some()
    }

    fn best(self) -> Option<Opportunity> {
        [
            self.bundle,
            self.directional,
            self.target_state,
            self.bonereaper_state,
            self.bonereaper_state_v2,
            self.bonereaper_state_guarded,
            self.codex_sentinel_v1,
            self.codex_scalp_probe_v1,
            self.micro_breakout,
        ]
        .into_iter()
        .flatten()
        .max_by(compare_opportunities)
    }
}

#[derive(Debug, Default)]
struct MarketNearMissSet {
    bundle: Option<ScoredNearMiss>,
    directional: Option<ScoredNearMiss>,
    target_state: Option<ScoredNearMiss>,
    bonereaper_state: Option<ScoredNearMiss>,
    bonereaper_state_v2: Option<ScoredNearMiss>,
    bonereaper_state_guarded: Option<ScoredNearMiss>,
    codex_sentinel_v1: Option<ScoredNearMiss>,
    codex_scalp_probe_v1: Option<ScoredNearMiss>,
    micro_breakout: Option<ScoredNearMiss>,
}

impl MarketNearMissSet {
    fn best(self) -> Option<ScoredNearMiss> {
        [
            self.bundle,
            self.directional,
            self.target_state,
            self.bonereaper_state,
            self.bonereaper_state_v2,
            self.bonereaper_state_guarded,
            self.codex_sentinel_v1,
            self.codex_scalp_probe_v1,
            self.micro_breakout,
        ]
        .into_iter()
        .flatten()
        .min_by(compare_near_misses)
    }
}

impl BundleArbitrageStrategy {
    /// Create a new strategy.
    #[must_use]
    pub fn new(config: StrategyConfig) -> Self {
        Self { config }
    }

    /// Scan markets and return opportunities sorted by score descending.
    #[must_use]
    pub fn find_opportunities(
        &self,
        markets: &[BinaryMarket],
        books: &HashMap<String, OrderBook>,
        market_notional: &HashMap<String, Decimal>,
        binance_contexts: &HashMap<String, BtcFiveMinuteContext>,
        trade_flows: &HashMap<String, TradeFlowSummary>,
    ) -> Vec<Opportunity> {
        let mut opportunities = markets
            .iter()
            .filter_map(|market| {
                self.evaluate_market(
                    market,
                    books,
                    market_notional,
                    binance_contexts,
                    trade_flows,
                )
            })
            .collect::<Vec<_>>();

        opportunities
            .iter_mut()
            .for_each(sanitize_opportunity_diagnostics);
        opportunities.sort_by(|left, right| compare_opportunities(right, left));
        opportunities
    }

    /// Return markets that are close to producing a trade signal.
    #[must_use]
    pub fn find_near_misses(
        &self,
        markets: &[BinaryMarket],
        books: &HashMap<String, OrderBook>,
        market_notional: &HashMap<String, Decimal>,
        binance_contexts: &HashMap<String, BtcFiveMinuteContext>,
        trade_flows: &HashMap<String, TradeFlowSummary>,
        limit: usize,
    ) -> Vec<NearMiss> {
        let mut near_misses = markets
            .iter()
            .filter_map(|market| {
                self.evaluate_market_near_miss(
                    market,
                    books,
                    market_notional,
                    binance_contexts,
                    trade_flows,
                )
            })
            .collect::<Vec<_>>();

        near_misses.sort_by(compare_near_misses);
        near_misses
            .into_iter()
            .map(|entry| entry.near_miss)
            .take(limit)
            .collect()
    }

    fn evaluate_market(
        &self,
        market: &BinaryMarket,
        books: &HashMap<String, OrderBook>,
        market_notional: &HashMap<String, Decimal>,
        binance_contexts: &HashMap<String, BtcFiveMinuteContext>,
        trade_flows: &HashMap<String, TradeFlowSummary>,
    ) -> Option<Opportunity> {
        let (context, quotes, book_refs, available_market_capacity, trade_flow) = self
            .market_inputs(
                market,
                books,
                market_notional,
                binance_contexts,
                trade_flows,
            )?;

        self.evaluate_market_opportunities(
            market,
            context,
            quotes,
            book_refs,
            available_market_capacity,
            trade_flow,
        )
        .best()
    }

    fn evaluate_market_near_miss(
        &self,
        market: &BinaryMarket,
        books: &HashMap<String, OrderBook>,
        market_notional: &HashMap<String, Decimal>,
        binance_contexts: &HashMap<String, BtcFiveMinuteContext>,
        trade_flows: &HashMap<String, TradeFlowSummary>,
    ) -> Option<ScoredNearMiss> {
        let (context, quotes, book_refs, available_market_capacity, trade_flow) = self
            .market_inputs(
                market,
                books,
                market_notional,
                binance_contexts,
                trade_flows,
            )?;

        if self
            .evaluate_market_opportunities(
                market,
                context,
                quotes,
                book_refs,
                available_market_capacity,
                trade_flow,
            )
            .any()
        {
            return None;
        }

        self.evaluate_market_near_miss_set(
            market,
            context,
            quotes,
            book_refs,
            available_market_capacity,
            trade_flow,
        )
        .best()
    }

    fn evaluate_market_opportunities(
        &self,
        market: &BinaryMarket,
        context: &BtcFiveMinuteContext,
        quotes: MarketQuotes<'_>,
        book_refs: MarketBookRefs<'_>,
        available_market_capacity: Decimal,
        trade_flow: Option<&TradeFlowSummary>,
    ) -> MarketOpportunitySet {
        MarketOpportunitySet {
            bundle: self.evaluate_bundle_candidate(
                market,
                context,
                quotes,
                available_market_capacity,
            ),
            directional: self.evaluate_directional_candidate(
                market,
                context,
                quotes,
                book_refs,
                available_market_capacity,
                trade_flow,
            ),
            target_state: self.evaluate_target_state_candidate(
                market,
                context,
                quotes,
                book_refs,
                available_market_capacity,
                trade_flow,
            ),
            bonereaper_state: self.evaluate_bonereaper_state_candidate(
                market,
                context,
                quotes,
                book_refs,
                available_market_capacity,
                trade_flow,
            ),
            bonereaper_state_v2: self.evaluate_bonereaper_state_v2_candidate(
                market,
                context,
                quotes,
                book_refs,
                available_market_capacity,
                trade_flow,
            ),
            bonereaper_state_guarded: self.evaluate_bonereaper_state_guarded_candidate(
                market,
                context,
                quotes,
                book_refs,
                available_market_capacity,
                trade_flow,
            ),
            codex_sentinel_v1: self.evaluate_codex_sentinel_v1_candidate(
                market,
                context,
                quotes,
                book_refs,
                available_market_capacity,
                trade_flow,
            ),
            codex_scalp_probe_v1: self.evaluate_codex_scalp_probe_v1_candidate(
                market,
                context,
                quotes,
                book_refs,
                available_market_capacity,
                trade_flow,
            ),
            micro_breakout: self.evaluate_micro_breakout_candidate(
                market,
                context,
                quotes,
                book_refs,
                available_market_capacity,
                trade_flow,
            ),
        }
    }

    fn evaluate_market_near_miss_set(
        &self,
        market: &BinaryMarket,
        context: &BtcFiveMinuteContext,
        quotes: MarketQuotes<'_>,
        book_refs: MarketBookRefs<'_>,
        available_market_capacity: Decimal,
        trade_flow: Option<&TradeFlowSummary>,
    ) -> MarketNearMissSet {
        MarketNearMissSet {
            bundle: self.evaluate_bundle_near_miss(
                market,
                context,
                quotes,
                available_market_capacity,
            ),
            directional: self.evaluate_directional_near_miss(
                market,
                context,
                quotes,
                available_market_capacity,
                trade_flow,
            ),
            target_state: self.evaluate_target_state_near_miss(
                market,
                context,
                quotes,
                available_market_capacity,
                trade_flow,
            ),
            bonereaper_state: self.evaluate_bonereaper_state_near_miss(
                market,
                context,
                quotes,
                available_market_capacity,
                trade_flow,
            ),
            bonereaper_state_v2: self.evaluate_bonereaper_state_v2_near_miss(
                market,
                context,
                quotes,
                book_refs,
                available_market_capacity,
                trade_flow,
            ),
            bonereaper_state_guarded: self.evaluate_bonereaper_state_guarded_near_miss(
                market,
                context,
                quotes,
                book_refs,
                available_market_capacity,
                trade_flow,
            ),
            codex_sentinel_v1: self.evaluate_codex_sentinel_v1_near_miss(
                market,
                context,
                quotes,
                book_refs,
                available_market_capacity,
                trade_flow,
            ),
            codex_scalp_probe_v1: self.evaluate_codex_scalp_probe_v1_near_miss(
                market,
                context,
                quotes,
                book_refs,
                available_market_capacity,
                trade_flow,
            ),
            micro_breakout: self.evaluate_micro_breakout_near_miss(
                market,
                context,
                quotes,
                available_market_capacity,
                trade_flow,
            ),
        }
    }

    fn market_inputs<'a>(
        &self,
        market: &'a BinaryMarket,
        books: &'a HashMap<String, OrderBook>,
        market_notional: &HashMap<String, Decimal>,
        binance_contexts: &'a HashMap<String, BtcFiveMinuteContext>,
        trade_flows: &'a HashMap<String, TradeFlowSummary>,
    ) -> Option<(
        &'a BtcFiveMinuteContext,
        MarketQuotes<'a>,
        MarketBookRefs<'a>,
        Decimal,
        Option<&'a TradeFlowSummary>,
    )> {
        if !market
            .target()
            .is_some_and(|target| self.config.market_targets.contains(&target))
            || market.liquidity_usdc < self.config.min_liquidity_usdc
            || !self.has_time_to_expiry(market)
        {
            return None;
        }

        let context = binance_contexts.get(&market.slug)?;
        if context.seconds_left < self.config.min_seconds_left
            || context.seconds_left > self.config.max_seconds_left
        {
            return None;
        }

        let up_token = market.token_for_outcome("up")?;
        let down_token = market.token_for_outcome("down")?;
        let up_book = books.get(up_token)?;
        let down_book = books.get(down_token)?;
        let up_ask = up_book.best_ask()?;
        let down_ask = down_book.best_ask()?;

        let available_market_capacity = self.available_market_capacity(market_notional, market);
        let quotes = MarketQuotes {
            up_token,
            down_token,
            up_ask_price: up_ask.price,
            up_ask_size: up_ask.size,
            down_ask_price: down_ask.price,
            down_ask_size: down_ask.size,
        };
        let book_refs = MarketBookRefs {
            up: up_book,
            down: down_book,
        };

        let trade_flow = trade_flows.get(&market.slug);

        Some((
            context,
            quotes,
            book_refs,
            available_market_capacity,
            trade_flow,
        ))
    }

    fn evaluate_bundle_candidate(
        &self,
        market: &BinaryMarket,
        context: &BtcFiveMinuteContext,
        quotes: MarketQuotes<'_>,
        available_market_capacity: Decimal,
    ) -> Option<Opportunity> {
        if !self.config.enable_bundle {
            return None;
        }

        if context.spot_move_bps.abs() < Decimal::from(self.config.min_spot_move_bps) {
            return None;
        }

        if !is_valid_binary_price(quotes.up_ask_price)
            || !is_valid_binary_price(quotes.down_ask_price)
        {
            return None;
        }

        let gross_bundle_cost = quotes.up_ask_price + quotes.down_ask_price;
        let fee_buffer = self.fee_buffer();
        let net_bundle_cost = gross_bundle_cost + fee_buffer;
        let edge_per_share = Decimal::ONE - net_bundle_cost;
        if edge_per_share <= Decimal::ZERO {
            return None;
        }

        let edge_bps = decimal_to_bps(edge_per_share);
        if edge_bps < self.config.min_edge_bps {
            return None;
        }

        let max_notional = self
            .config
            .max_bundle_notional_usdc
            .min(available_market_capacity);
        if max_notional <= Decimal::ZERO {
            return None;
        }

        let depth_shares = quotes.up_ask_size.min(quotes.down_ask_size);
        if depth_shares < self.config.min_top_of_book_shares {
            return None;
        }

        let affordable_shares = max_notional / gross_bundle_cost;
        let tradable_shares = depth_shares.min(affordable_shares).round_dp(4);
        if tradable_shares < self.config.min_top_of_book_shares {
            return None;
        }

        let required_usdc = (tradable_shares * gross_bundle_cost).round_dp(6);
        let expected_payout = tradable_shares.round_dp(6);
        let expected_profit = (tradable_shares * edge_per_share).round_dp(6);
        let primary = primary_quote(context, quotes);

        Some(Opportunity {
            kind: OpportunityKind::BundleArbitrage,
            condition_id: market.condition_id.clone(),
            slug: market.slug.clone(),
            question: market.question.clone(),
            outcome_a_label: "Up".to_owned(),
            outcome_a_token_id: quotes.up_token.to_owned(),
            outcome_b_label: "Down".to_owned(),
            outcome_b_token_id: quotes.down_token.to_owned(),
            liquidity_usdc: market.liquidity_usdc,
            outcome_a_ask_price: quotes.up_ask_price,
            outcome_b_ask_price: quotes.down_ask_price,
            bundle_cost: gross_bundle_cost.round_dp(6),
            net_bundle_cost: net_bundle_cost.round_dp(6),
            edge_per_share: edge_per_share.round_dp(6),
            edge_bps,
            tradable_shares,
            required_usdc,
            expected_payout,
            expected_profit,
            interval_open_price: context.interval_open_price,
            target_price: context.target_price,
            target_price_source: context.target_price_source,
            target_gap_bps: context.target_gap_bps,
            current_spot_price: context.current_spot_price,
            spot_move_bps: context.spot_move_bps,
            spot_move_1s_bps: context.spot_move_1s_bps,
            spot_move_5s_bps: context.spot_move_5s_bps,
            spot_move_15s_bps: context.spot_move_15s_bps,
            micro_acceleration_bps: context.micro_acceleration_bps,
            micro_burst_reference_price: context.micro_burst_reference_price,
            micro_reference_price: context.micro_reference_price,
            signal_strength_bps: Decimal::ZERO,
            aligned_trade_flow_bps: Decimal::ZERO,
            signal_tier: "bundle".to_owned(),
            target_cross_label: "none".to_owned(),
            dominant_outcome: context.dominant_outcome.clone(),
            primary_outcome_label: context.dominant_outcome.clone(),
            primary_outcome_token_id: primary.token_id.to_owned(),
            primary_outcome_ask_price: primary.ask_price,
            primary_fill_levels: Vec::new(),
            hedge_outcome_label: None,
            hedge_outcome_token_id: None,
            hedge_outcome_ask_price: None,
            hedge_fill_levels: Vec::new(),
            hedge_shares: Decimal::ZERO,
            seconds_left: context.seconds_left,
            note: format!(
                "bundle: Binance {} {} bps 5m {}. , ask .",
                context.target.binance_symbol(),
                context.spot_move_bps.round_dp(2),
                context.interval_open_price.round_dp(2),
            ),
        })
    }

    #[allow(clippy::too_many_lines)]
    fn evaluate_directional_candidate(
        &self,
        market: &BinaryMarket,
        context: &BtcFiveMinuteContext,
        quotes: MarketQuotes<'_>,
        book_refs: MarketBookRefs<'_>,
        available_market_capacity: Decimal,
        trade_flow: Option<&TradeFlowSummary>,
    ) -> Option<Opportunity> {
        if !self.config.enable_directional {
            return None;
        }

        let spot_move_fixed_abs = bps_fixed_abs(context.spot_move_bps);
        if spot_move_fixed_abs < u32_bps_to_fixed_abs(self.config.directional_min_spot_move_bps) {
            return None;
        }
        let spot_move_abs = context.spot_move_bps.abs();

        let velocity_bps_per_minute = directional_velocity_bps_per_minute(context);
        if velocity_bps_per_minute
            < Decimal::from(self.config.directional_min_velocity_bps_per_minute)
        {
            return None;
        }

        let signal_strength_bps =
            directional_effective_signal_bps(context, trade_flow, &self.config);
        if signal_strength_bps < Decimal::from(self.config.directional_min_signal_bps) {
            return None;
        }

        let primary = primary_quote(context, quotes);
        let hedge = secondary_quote(context, quotes);
        let primary_book = primary_order_book(primary, book_refs);

        if !is_valid_binary_price(primary.ask_price)
            || primary.ask_price > self.config.directional_max_entry_price
        {
            return None;
        }

        let signal_anchor_price =
            directional_signal_anchor_price(signal_strength_bps, &self.config);
        let hedge_ratio = self.directional_tail_hedge_ratio(
            context,
            primary,
            hedge,
            signal_strength_bps,
            velocity_bps_per_minute,
        );
        let strong_entry_signal =
            directional_is_strong_entry_signal(context, trade_flow, &self.config);
        if self.config.directional_require_hedge_for_soft_entry
            && hedge_ratio <= Decimal::ZERO
            && !strong_entry_signal
        {
            return None;
        }
        let execution_slippage =
            Decimal::from(self.config.directional_execution_slippage_bps) / bps_denominator();
        let execution_anchor_price = (signal_anchor_price + execution_slippage).min(Decimal::ONE);
        let fee_buffer = self.fee_buffer() * (Decimal::ONE + hedge_ratio);
        let effective_unit_cost = primary.ask_price + (hedge_ratio * hedge.ask_price);
        let effective_unit_payout =
            execution_anchor_price + (hedge_ratio * (Decimal::ONE - execution_anchor_price));
        let edge_per_share = effective_unit_payout - effective_unit_cost - fee_buffer;
        if edge_per_share <= Decimal::ZERO {
            return None;
        }

        let edge_bps = decimal_to_bps(edge_per_share);
        if edge_bps < self.config.directional_min_model_edge_bps {
            return None;
        }

        let max_notional = directional_entry_notional_cap(
            available_market_capacity,
            signal_strength_bps,
            strong_entry_signal,
            &self.config,
        );
        if max_notional <= Decimal::ZERO {
            return None;
        }

        let hedge_depth_limit = if hedge_ratio > Decimal::ZERO {
            hedge.ask_size / hedge_ratio
        } else {
            Decimal::MAX
        };
        let affordable_shares = max_notional / effective_unit_cost;
        let target_primary_shares = hedge_depth_limit.min(affordable_shares).round_dp(6);
        let mut primary_fill = build_buy_fill_plan_for_shares(
            primary_book,
            target_primary_shares,
            self.config.directional_max_entry_price,
        )?;
        let mut tradable_shares = primary_fill.total_shares.round_dp(4);
        if tradable_shares < self.config.min_top_of_book_shares {
            return None;
        }

        let mut hedge_shares = (tradable_shares * hedge_ratio).round_dp(4);
        let mut required_usdc =
            (primary_fill.total_cost + (hedge_shares * hedge.ask_price)).round_dp(6);
        if required_usdc > max_notional {
            let scale = (max_notional / required_usdc).min(Decimal::ONE);
            primary_fill = build_buy_fill_plan_for_shares(
                primary_book,
                (tradable_shares * scale).round_dp(6),
                self.config.directional_max_entry_price,
            )?;
            tradable_shares = primary_fill.total_shares.round_dp(4);
            if tradable_shares < self.config.min_top_of_book_shares {
                return None;
            }
            hedge_shares = (tradable_shares * hedge_ratio).round_dp(4);
            required_usdc =
                (primary_fill.total_cost + (hedge_shares * hedge.ask_price)).round_dp(6);
        }

        let primary_effective_price = primary_fill.average_price;
        let effective_unit_cost = primary_effective_price + (hedge_ratio * hedge.ask_price);
        let edge_per_share = effective_unit_payout - effective_unit_cost - fee_buffer;
        if edge_per_share <= Decimal::ZERO {
            return None;
        }
        let edge_bps = decimal_to_bps(edge_per_share);
        if edge_bps < self.config.directional_min_model_edge_bps {
            return None;
        }

        let expected_payout = (tradable_shares * effective_unit_payout).round_dp(6);
        let expected_profit = (tradable_shares * edge_per_share).round_dp(6);
        let kind = if hedge_shares > Decimal::ZERO {
            OpportunityKind::DirectionalMomentumHedged
        } else {
            OpportunityKind::DirectionalMomentum
        };

        Some(Opportunity {
            kind,
            condition_id: market.condition_id.clone(),
            slug: market.slug.clone(),
            question: market.question.clone(),
            outcome_a_label: "Up".to_owned(),
            outcome_a_token_id: quotes.up_token.to_owned(),
            outcome_b_label: "Down".to_owned(),
            outcome_b_token_id: quotes.down_token.to_owned(),
            liquidity_usdc: market.liquidity_usdc,
            outcome_a_ask_price: quotes.up_ask_price,
            outcome_b_ask_price: quotes.down_ask_price,
            bundle_cost: (quotes.up_ask_price + quotes.down_ask_price).round_dp(6),
            net_bundle_cost: effective_unit_cost.round_dp(6),
            edge_per_share: edge_per_share.round_dp(6),
            edge_bps,
            tradable_shares,
            required_usdc,
            expected_payout,
            expected_profit,
            interval_open_price: context.interval_open_price,
            target_price: context.target_price,
            target_price_source: context.target_price_source,
            target_gap_bps: context.target_gap_bps,
            current_spot_price: context.current_spot_price,
            spot_move_bps: context.spot_move_bps,
            spot_move_1s_bps: context.spot_move_1s_bps,
            spot_move_5s_bps: context.spot_move_5s_bps,
            spot_move_15s_bps: context.spot_move_15s_bps,
            micro_acceleration_bps: context.micro_acceleration_bps,
            micro_burst_reference_price: context.micro_burst_reference_price,
            micro_reference_price: context.micro_reference_price,
            signal_strength_bps: signal_strength_bps.round_dp(6),
            aligned_trade_flow_bps: aligned_trade_flow_bps(context, trade_flow).round_dp(6),
            signal_tier: if strong_entry_signal {
                "strong".to_owned()
            } else {
                "soft".to_owned()
            },
            target_cross_label: recent_target_cross(context, &self.config)
                .label()
                .to_owned(),
            dominant_outcome: context.dominant_outcome.clone(),
            primary_outcome_label: primary.label.to_owned(),
            primary_outcome_token_id: primary.token_id.to_owned(),
            primary_outcome_ask_price: primary_effective_price.round_dp(6),
            primary_fill_levels: primary_fill.levels,
            hedge_outcome_label: (hedge_shares > Decimal::ZERO).then(|| hedge.label.to_owned()),
            hedge_outcome_token_id: (hedge_shares > Decimal::ZERO)
                .then(|| hedge.token_id.to_owned()),
            hedge_outcome_ask_price: (hedge_shares > Decimal::ZERO).then_some(hedge.ask_price),
            hedge_fill_levels: Vec::new(),
            hedge_shares,
            seconds_left: context.seconds_left,
            note: format!(
                "directional(binance-first): Binance {} {} {} bps, signal {} bps. {} ask {}, signal anchor {}, execution slack {} bps, hedge_ratio {}.",
                context.target.binance_symbol(),
                context.dominant_outcome,
                spot_move_abs.round_dp(2),
                signal_strength_bps.round_dp(2),
                primary.label,
                primary_effective_price.round_dp(4),
                signal_anchor_price.round_dp(4),
                self.config.directional_execution_slippage_bps,
                hedge_ratio.round_dp(2),
            ),
        })
    }

    #[allow(clippy::too_many_lines)]
    fn evaluate_target_state_candidate(
        &self,
        market: &BinaryMarket,
        context: &BtcFiveMinuteContext,
        quotes: MarketQuotes<'_>,
        book_refs: MarketBookRefs<'_>,
        available_market_capacity: Decimal,
        trade_flow: Option<&TradeFlowSummary>,
    ) -> Option<Opportunity> {
        if !self.config.enable_target_state_v1 {
            return None;
        }

        if elapsed_window_secs(context) < self.config.target_state_min_elapsed_window_secs
            || context.seconds_left > self.config.target_state_max_seconds_left
        {
            return None;
        }

        let target_gap_abs = context.target_gap_bps.abs();
        if target_gap_abs < self.config.target_state_min_target_gap_bps
            || !moves_align(context.target_gap_bps, context.spot_move_15s_bps)
            || context.spot_move_15s_bps.abs() < self.config.target_state_min_spot_move_15s_bps
        {
            return None;
        }

        let aligned_flow_bps = aligned_trade_flow_bps(context, trade_flow);
        if aligned_flow_bps < self.config.target_state_min_aligned_flow_bps {
            return None;
        }

        let primary = primary_quote(context, quotes);
        let primary_book = primary_order_book(primary, book_refs);
        if !is_valid_binary_price(primary.ask_price)
            || primary.ask_price > self.config.target_state_max_entry_price
        {
            return None;
        }

        let signal_strength_bps = target_state_signal_bps(context, trade_flow, &self.config);
        if signal_strength_bps < Decimal::from(self.config.target_state_min_signal_bps) {
            return None;
        }

        let signal_tier = target_state_signal_tier(context, trade_flow, &self.config);
        let max_notional =
            target_state_entry_notional_cap(available_market_capacity, signal_tier, &self.config);
        if max_notional <= Decimal::ZERO {
            return None;
        }

        let primary_fill = build_buy_fill_plan_for_notional(
            primary_book,
            max_notional,
            sweep_average_price_limit(primary.ask_price, self.config.target_state_max_entry_price),
        )?;
        let tradable_shares = primary_fill.total_shares.round_dp(4);
        if tradable_shares < self.config.min_top_of_book_shares {
            return None;
        }

        let effective_unit_cost = primary_fill.average_price;
        let execution_anchor_price =
            target_state_signal_anchor_price(context, signal_strength_bps, &self.config);
        let edge_per_share = execution_anchor_price - effective_unit_cost - self.fee_buffer();
        if edge_per_share <= Decimal::ZERO {
            return None;
        }

        let edge_bps = decimal_to_bps(edge_per_share);
        if edge_bps < self.config.directional_min_model_edge_bps {
            return None;
        }

        let expected_payout = (tradable_shares * execution_anchor_price).round_dp(6);
        let expected_profit = (tradable_shares * edge_per_share).round_dp(6);
        let target_cross = recent_target_cross(context, &self.config);

        Some(Opportunity {
            kind: OpportunityKind::TargetStateV1,
            condition_id: market.condition_id.clone(),
            slug: market.slug.clone(),
            question: market.question.clone(),
            outcome_a_label: "Up".to_owned(),
            outcome_a_token_id: quotes.up_token.to_owned(),
            outcome_b_label: "Down".to_owned(),
            outcome_b_token_id: quotes.down_token.to_owned(),
            liquidity_usdc: market.liquidity_usdc,
            outcome_a_ask_price: quotes.up_ask_price,
            outcome_b_ask_price: quotes.down_ask_price,
            bundle_cost: (quotes.up_ask_price + quotes.down_ask_price).round_dp(6),
            net_bundle_cost: effective_unit_cost.round_dp(6),
            edge_per_share: edge_per_share.round_dp(6),
            edge_bps,
            tradable_shares,
            required_usdc: primary_fill.total_cost.round_dp(6),
            expected_payout,
            expected_profit,
            interval_open_price: context.interval_open_price,
            target_price: context.target_price,
            target_price_source: context.target_price_source,
            target_gap_bps: context.target_gap_bps,
            current_spot_price: context.current_spot_price,
            spot_move_bps: context.spot_move_bps,
            spot_move_1s_bps: context.spot_move_1s_bps,
            spot_move_5s_bps: context.spot_move_5s_bps,
            spot_move_15s_bps: context.spot_move_15s_bps,
            micro_acceleration_bps: context.micro_acceleration_bps,
            micro_burst_reference_price: context.micro_burst_reference_price,
            micro_reference_price: context.micro_reference_price,
            signal_strength_bps: signal_strength_bps.round_dp(6),
            aligned_trade_flow_bps: aligned_flow_bps.round_dp(6),
            signal_tier: signal_tier.as_str().to_owned(),
            target_cross_label: target_cross.label().to_owned(),
            dominant_outcome: context.dominant_outcome.clone(),
            primary_outcome_label: primary.label.to_owned(),
            primary_outcome_token_id: primary.token_id.to_owned(),
            primary_outcome_ask_price: effective_unit_cost.round_dp(6),
            primary_fill_levels: primary_fill.levels,
            hedge_outcome_label: None,
            hedge_outcome_token_id: None,
            hedge_outcome_ask_price: None,
            hedge_fill_levels: Vec::new(),
            hedge_shares: Decimal::ZERO,
            seconds_left: context.seconds_left,
            note: format!(
                "target-state-v1({}): price already {} target by {} bps, 15s {} bps, 5s {} bps, flow {} bps, target-cross {}. Taking {} at ask {}, signal {} bps.",
                signal_tier.as_str(),
                primary.label,
                target_gap_abs.round_dp(2),
                context.spot_move_15s_bps.abs().round_dp(2),
                context.spot_move_5s_bps.abs().round_dp(2),
                aligned_flow_bps.round_dp(2),
                target_cross.label(),
                primary.label,
                effective_unit_cost.round_dp(4),
                signal_strength_bps.round_dp(2),
            ),
        })
    }

    #[allow(clippy::too_many_lines)]
    fn evaluate_bonereaper_state_candidate(
        &self,
        market: &BinaryMarket,
        context: &BtcFiveMinuteContext,
        quotes: MarketQuotes<'_>,
        book_refs: MarketBookRefs<'_>,
        available_market_capacity: Decimal,
        trade_flow: Option<&TradeFlowSummary>,
    ) -> Option<Opportunity> {
        if !self.config.enable_bonereaper_state_v1 {
            return None;
        }

        if elapsed_window_secs(context) < self.config.bonereaper_state_min_elapsed_window_secs
            || context.seconds_left > self.config.bonereaper_state_max_seconds_left
        {
            return None;
        }

        let up_side = primary_side_from_context(context);
        let target_gap_abs = context.target_gap_bps.abs();
        let aligned_micro_bps = aligned_move_bps(context.spot_move_5s_bps, up_side);
        let aligned_swing_bps = aligned_move_bps(context.spot_move_15s_bps, up_side);
        let aligned_flow_bps = aligned_trade_flow_bps(context, trade_flow);

        if target_gap_abs < self.config.bonereaper_state_min_target_gap_bps
            || aligned_swing_bps < self.config.bonereaper_state_min_spot_move_15s_bps
            || aligned_micro_bps < self.config.bonereaper_state_min_spot_move_5s_bps
            || aligned_flow_bps < self.config.bonereaper_state_min_aligned_flow_bps
        {
            return None;
        }

        let primary = primary_quote(context, quotes);
        let primary_book = primary_order_book(primary, book_refs);
        if !is_valid_binary_price(primary.ask_price)
            || primary.ask_price > self.config.bonereaper_state_max_entry_price
        {
            return None;
        }

        let signal_strength_bps = bonereaper_state_signal_bps(context, trade_flow, &self.config);
        if signal_strength_bps < Decimal::from(self.config.bonereaper_state_min_signal_bps) {
            return None;
        }

        let signal_tier = bonereaper_state_signal_tier(context, trade_flow, &self.config);
        let max_notional = bonereaper_state_entry_notional_cap(
            available_market_capacity,
            signal_tier,
            &self.config,
        );
        if max_notional <= Decimal::ZERO {
            return None;
        }

        let primary_fill = build_buy_fill_plan_for_notional(
            primary_book,
            max_notional,
            sweep_average_price_limit(
                primary.ask_price,
                self.config.bonereaper_state_max_entry_price,
            ),
        )?;
        let tradable_shares = primary_fill.total_shares.round_dp(4);
        if tradable_shares < self.config.min_top_of_book_shares {
            return None;
        }

        let effective_unit_cost = primary_fill.average_price;
        let execution_anchor_price = bonereaper_state_signal_anchor_price(
            context,
            signal_strength_bps,
            self.config.directional_max_fair_price,
            &self.config,
        );
        let edge_per_share = execution_anchor_price - effective_unit_cost - self.fee_buffer();
        if edge_per_share <= Decimal::ZERO {
            return None;
        }

        let edge_bps = decimal_to_bps(edge_per_share);
        if edge_bps < self.config.directional_min_model_edge_bps {
            return None;
        }

        let expected_payout = (tradable_shares * execution_anchor_price).round_dp(6);
        let expected_profit = (tradable_shares * edge_per_share).round_dp(6);
        let target_cross = recent_target_cross(context, &self.config);

        Some(Opportunity {
            kind: OpportunityKind::BonereaperStateV1,
            condition_id: market.condition_id.clone(),
            slug: market.slug.clone(),
            question: market.question.clone(),
            outcome_a_label: "Up".to_owned(),
            outcome_a_token_id: quotes.up_token.to_owned(),
            outcome_b_label: "Down".to_owned(),
            outcome_b_token_id: quotes.down_token.to_owned(),
            liquidity_usdc: market.liquidity_usdc,
            outcome_a_ask_price: quotes.up_ask_price,
            outcome_b_ask_price: quotes.down_ask_price,
            bundle_cost: (quotes.up_ask_price + quotes.down_ask_price).round_dp(6),
            net_bundle_cost: effective_unit_cost.round_dp(6),
            edge_per_share: edge_per_share.round_dp(6),
            edge_bps,
            tradable_shares,
            required_usdc: primary_fill.total_cost.round_dp(6),
            expected_payout,
            expected_profit,
            interval_open_price: context.interval_open_price,
            target_price: context.target_price,
            target_price_source: context.target_price_source,
            target_gap_bps: context.target_gap_bps,
            current_spot_price: context.current_spot_price,
            spot_move_bps: context.spot_move_bps,
            spot_move_1s_bps: context.spot_move_1s_bps,
            spot_move_5s_bps: context.spot_move_5s_bps,
            spot_move_15s_bps: context.spot_move_15s_bps,
            micro_acceleration_bps: context.micro_acceleration_bps,
            micro_burst_reference_price: context.micro_burst_reference_price,
            micro_reference_price: context.micro_reference_price,
            signal_strength_bps: signal_strength_bps.round_dp(6),
            aligned_trade_flow_bps: aligned_flow_bps.round_dp(6),
            signal_tier: signal_tier.as_str().to_owned(),
            target_cross_label: target_cross.label().to_owned(),
            dominant_outcome: context.dominant_outcome.clone(),
            primary_outcome_label: primary.label.to_owned(),
            primary_outcome_token_id: primary.token_id.to_owned(),
            primary_outcome_ask_price: effective_unit_cost.round_dp(6),
            primary_fill_levels: primary_fill.levels,
            hedge_outcome_label: None,
            hedge_outcome_token_id: None,
            hedge_outcome_ask_price: None,
            hedge_fill_levels: Vec::new(),
            hedge_shares: Decimal::ZERO,
            seconds_left: context.seconds_left,
            note: format!(
                "bonereaper-state-v1({}): target gap {} bps, aligned 5s {} bps, aligned 15s {} bps, flow {} bps, target-cross {}. Taking {} at ask {}, signal {} bps.",
                signal_tier.as_str(),
                target_gap_abs.round_dp(2),
                aligned_micro_bps.round_dp(2),
                aligned_swing_bps.round_dp(2),
                aligned_flow_bps.round_dp(2),
                target_cross.label(),
                primary.label,
                effective_unit_cost.round_dp(4),
                signal_strength_bps.round_dp(2),
            ),
        })
    }

    #[allow(clippy::too_many_lines)]
    fn evaluate_bonereaper_state_v2_candidate(
        &self,
        market: &BinaryMarket,
        context: &BtcFiveMinuteContext,
        quotes: MarketQuotes<'_>,
        book_refs: MarketBookRefs<'_>,
        available_market_capacity: Decimal,
        trade_flow: Option<&TradeFlowSummary>,
    ) -> Option<Opportunity> {
        if !self.config.enable_bonereaper_state_v2 {
            return None;
        }

        if elapsed_window_secs(context) < self.config.bonereaper_state_v2_min_elapsed_window_secs
            || context.seconds_left > self.config.bonereaper_state_v2_max_seconds_left
        {
            return None;
        }

        let decision = bonereaper_state_v2_decision(context, trade_flow, &self.config, false)?;
        self.build_bonereaper_state_v2_variant_opportunity(
            market,
            context,
            quotes,
            book_refs,
            available_market_capacity,
            decision,
            OpportunityKind::BonereaperStateV2,
        )
    }

    #[allow(clippy::too_many_lines)]
    fn evaluate_bonereaper_state_guarded_candidate(
        &self,
        market: &BinaryMarket,
        context: &BtcFiveMinuteContext,
        quotes: MarketQuotes<'_>,
        book_refs: MarketBookRefs<'_>,
        available_market_capacity: Decimal,
        trade_flow: Option<&TradeFlowSummary>,
    ) -> Option<Opportunity> {
        if !self.config.enable_bonereaper_state_guarded {
            return None;
        }

        if elapsed_window_secs(context) < self.config.bonereaper_state_v2_min_elapsed_window_secs
            || context.seconds_left > self.config.bonereaper_state_v2_max_seconds_left
            || context.seconds_left < self.config.bonereaper_state_v2_min_seconds_left
        {
            return None;
        }

        let decision = bonereaper_state_v2_decision(context, trade_flow, &self.config, true)?;
        self.build_bonereaper_state_v2_variant_opportunity(
            market,
            context,
            quotes,
            book_refs,
            available_market_capacity,
            decision,
            OpportunityKind::BonereaperStateGuarded,
        )
    }

    #[allow(clippy::too_many_lines)]
    fn evaluate_codex_sentinel_v1_candidate(
        &self,
        market: &BinaryMarket,
        context: &BtcFiveMinuteContext,
        quotes: MarketQuotes<'_>,
        book_refs: MarketBookRefs<'_>,
        available_market_capacity: Decimal,
        trade_flow: Option<&TradeFlowSummary>,
    ) -> Option<Opportunity> {
        if !self.config.enable_codex_sentinel_v1
            || !codex_sentinel_v1_target_allowed(context.target)
        {
            return None;
        }

        if elapsed_window_secs(context) < self.config.bonereaper_state_v2_min_elapsed_window_secs
            || context.seconds_left > self.config.bonereaper_state_v2_max_seconds_left
            || context.seconds_left < self.config.bonereaper_state_v2_min_seconds_left
        {
            return None;
        }

        // Sentinel is the guarded v2 profile: early probes must not ignore flow quality.
        let decision = bonereaper_state_v2_decision(context, trade_flow, &self.config, true)?;
        let primary = quote_for_side(quotes, decision.up_side);
        let primary_book = order_book_for_side(decision.up_side, book_refs);
        let breakout_allows =
            codex_breakout_v1_allows(context, &decision, primary.ask_price, &self.config);
        let discount_value_allows = codex_sentinel_v1_discount_value_lane_allows(
            context,
            &decision,
            primary.ask_price,
            &self.config,
        );
        if self.config.codex_breakout_v1_required && !(breakout_allows || discount_value_allows) {
            return None;
        }
        if codex_sentinel_v1_mid_signal_guard_blocks(context, &decision, &self.config)
            && !(breakout_allows || discount_value_allows)
        {
            return None;
        }
        if codex_sentinel_v1_entry_spread_guard_blocks(
            primary.ask_price,
            primary_book,
            &self.config,
        ) {
            return None;
        }
        if codex_sentinel_v1_live_quote_age_guard_blocks(context, &self.config) {
            return None;
        }
        if context.seconds_left < self.config.bonereaper_state_v2_min_seconds_left
            && !codex_sentinel_v1_late_entry_override_allows(
                context,
                &decision,
                primary.ask_price,
                &self.config,
            )
        {
            return None;
        }
        if codex_sentinel_v1_bad_window_guard_blocks(
            context,
            &decision,
            primary.ask_price,
            &self.config,
        ) {
            return None;
        }
        if codex_sentinel_v1_stale_micro_guard_blocks(
            context,
            &decision,
            primary.ask_price,
            &self.config,
        ) && !(breakout_allows || discount_value_allows)
        {
            return None;
        }
        if codex_sentinel_v1_quality_floor_blocks(context, &decision, &self.config) {
            return None;
        }
        if codex_sentinel_v1_low_flow_guard_blocks(
            context,
            &decision,
            primary.ask_price,
            &self.config,
        ) && !(breakout_allows || discount_value_allows)
        {
            return None;
        }
        if codex_sentinel_v1_no_chase_guard_blocks(
            context,
            &decision,
            primary.ask_price,
            &self.config,
        ) {
            return None;
        }
        if codex_sentinel_v1_late_window_value_guard_blocks(
            context,
            &decision,
            primary.ask_price,
            &self.config,
        ) {
            return None;
        }
        let aggressive_continuation_allows = codex_sentinel_v1_aggressive_continuation_allows(
            context,
            &decision,
            primary.ask_price,
            &self.config,
        );
        if codex_sentinel_v1_premium_entry_guard_blocks(
            context,
            &decision,
            primary.ask_price,
            &self.config,
        ) && !aggressive_continuation_allows
        {
            return None;
        }
        if codex_sentinel_v1_expensive_entry_guard_blocks(
            context,
            &decision,
            primary.ask_price,
            &self.config,
        ) && !aggressive_continuation_allows
        {
            return None;
        }

        self.build_bonereaper_state_v2_variant_opportunity(
            market,
            context,
            quotes,
            book_refs,
            available_market_capacity,
            decision,
            OpportunityKind::CodexSentinelV1,
        )
    }

    #[allow(clippy::too_many_lines)]
    fn evaluate_codex_scalp_probe_v1_candidate(
        &self,
        market: &BinaryMarket,
        context: &BtcFiveMinuteContext,
        quotes: MarketQuotes<'_>,
        book_refs: MarketBookRefs<'_>,
        available_market_capacity: Decimal,
        trade_flow: Option<&TradeFlowSummary>,
    ) -> Option<Opportunity> {
        if !self.config.enable_codex_scalp_probe_v1
            || !codex_scalp_probe_v1_target_allowed(context.target)
        {
            return None;
        }

        let raw_ablation = self.config.codex_scalp_probe_v1_raw_ablation_enabled;
        if !raw_ablation {
            if elapsed_window_secs(context)
                < self.config.codex_scalp_probe_v1_min_elapsed_window_secs
                || context.seconds_left > self.config.codex_scalp_probe_v1_max_seconds_left
                || context.seconds_left < self.config.codex_scalp_probe_v1_min_seconds_left
            {
                return None;
            }

            if !codex_sentinel_v1_has_fresh_live_spot(
                context,
                self.config.codex_sentinel_v1_max_live_quote_age_ms,
            ) {
                return None;
            }
        }

        let decision = bonereaper_state_v2_decision(context, trade_flow, &self.config, true)?;
        let primary = quote_for_side(quotes, decision.up_side);
        let primary_book = order_book_for_side(decision.up_side, book_refs);
        if !codex_scalp_probe_v1_allows(
            context,
            &decision,
            primary.ask_price,
            primary_book,
            &self.config,
        ) {
            return None;
        }

        self.build_bonereaper_state_v2_variant_opportunity(
            market,
            context,
            quotes,
            book_refs,
            available_market_capacity,
            decision,
            OpportunityKind::CodexScalpProbeV1,
        )
    }

    #[allow(clippy::too_many_lines, clippy::too_many_arguments)]
    fn build_bonereaper_state_v2_variant_opportunity(
        &self,
        market: &BinaryMarket,
        context: &BtcFiveMinuteContext,
        quotes: MarketQuotes<'_>,
        book_refs: MarketBookRefs<'_>,
        available_market_capacity: Decimal,
        decision: BonereaperStateV2Decision,
        kind: OpportunityKind,
    ) -> Option<Opportunity> {
        let primary = quote_for_side(quotes, decision.up_side);
        let primary_book = order_book_for_side(decision.up_side, book_refs);
        let max_entry_price = bonereaper_state_v2_variant_max_entry_price(kind, &self.config);
        let raw_scalp_ablation = kind == OpportunityKind::CodexScalpProbeV1
            && self.config.codex_scalp_probe_v1_raw_ablation_enabled;
        if !is_valid_binary_price(primary.ask_price)
            || (!raw_scalp_ablation && primary.ask_price > max_entry_price)
        {
            return None;
        }
        if !matches!(
            kind,
            OpportunityKind::CodexSentinelV1 | OpportunityKind::CodexScalpProbeV1
        ) && bonereaper_state_v2_quality_guard_block(
            context,
            &decision,
            primary.ask_price,
            &self.config,
        )
        .is_some()
        {
            return None;
        }
        let codex_confidence_score = (kind == OpportunityKind::CodexSentinelV1).then(|| {
            codex_sentinel_v1_confidence_score(context, &decision, primary.ask_price, &self.config)
        });

        let max_notional = bonereaper_state_v2_entry_notional_cap(
            available_market_capacity,
            decision.signal_tier,
            &self.config,
        );
        let max_notional = match kind {
            OpportunityKind::CodexSentinelV1 => codex_sentinel_v1_entry_notional_cap(
                available_market_capacity,
                context,
                decision,
                primary.ask_price,
                codex_confidence_score.unwrap_or_default(),
                &self.config,
            ),
            OpportunityKind::CodexScalpProbeV1 => self
                .config
                .codex_scalp_probe_v1_notional_usdc
                .min(available_market_capacity)
                .round_dp(6),
            _ => max_notional,
        };
        if max_notional <= Decimal::ZERO {
            return None;
        }

        let primary_fill = build_buy_fill_plan_for_notional(
            primary_book,
            max_notional,
            sweep_average_price_limit(primary.ask_price, max_entry_price),
        )?;
        let tradable_shares = primary_fill.total_shares.round_dp(4);
        if tradable_shares < self.config.min_top_of_book_shares {
            return None;
        }

        let signal_strength_bps = decision.signal_strength_bps;
        let effective_unit_cost = primary_fill.average_price;
        let execution_anchor_price = bonereaper_state_signal_anchor_price(
            context,
            signal_strength_bps,
            self.config.bonereaper_state_v2_max_fair_price,
            &self.config,
        );
        let edge_per_share = execution_anchor_price - effective_unit_cost - self.fee_buffer();
        if !raw_scalp_ablation && edge_per_share <= Decimal::ZERO {
            return None;
        }

        let edge_bps = decimal_to_bps(edge_per_share);
        if !raw_scalp_ablation && edge_bps < self.config.directional_min_model_edge_bps {
            return None;
        }

        let expected_payout = (tradable_shares * execution_anchor_price).round_dp(6);
        let expected_profit = (tradable_shares * edge_per_share).round_dp(6);
        let min_expected_profit = if kind == OpportunityKind::CodexScalpProbeV1 {
            codex_scalp_probe_v1_min_expected_profit_usdc(
                context,
                &decision,
                primary.ask_price,
                primary_book,
                &self.config,
            )
        } else {
            self.config.bonereaper_state_v2_min_expected_profit_usdc
        };
        if !raw_scalp_ablation && expected_profit < min_expected_profit {
            return None;
        }
        let target_cross = recent_target_cross(context, &self.config);
        let regime = if decision.counter_bias {
            "contested-flip"
        } else {
            "with-gap"
        };
        let confidence_note = codex_confidence_score.map_or_else(String::new, |score| {
            format!(", confidence {}/100", score.round_dp(1))
        });
        let breakout_note =
            if matches!(
                kind,
                OpportunityKind::CodexSentinelV1 | OpportunityKind::CodexScalpProbeV1
            ) && codex_breakout_v1_allows(context, &decision, primary.ask_price, &self.config)
            {
                let aligned_depth_bps =
                    aligned_move_bps(context.exchange_book_depth_imbalance_bps, decision.up_side);
                let aligned_top_bps =
                    aligned_move_bps(context.exchange_book_top_imbalance_bps, decision.up_side);
                let aligned_microprice_bps =
                    aligned_move_bps(context.exchange_book_microprice_bps, decision.up_side);
                let breakout_score = codex_breakout_v1_score_bps(
                    aligned_depth_bps,
                    aligned_top_bps,
                    aligned_microprice_bps,
                    codex_sentinel_v1_fresh_confirmation_bps(context, &decision),
                    context.target_gap_bps.abs(),
                );
                format!(", breakout_score {}", breakout_score.round_dp(1))
            } else {
                String::new()
            };
        let discount_value_note = if kind == OpportunityKind::CodexSentinelV1
            && codex_sentinel_v1_discount_value_lane_allows(
                context,
                &decision,
                primary.ask_price,
                &self.config,
            ) {
            ", discount_value_lane".to_owned()
        } else {
            String::new()
        };
        let scalp_radar_note = if kind == OpportunityKind::CodexScalpProbeV1 {
            let aligned_top_bps =
                aligned_move_bps(context.exchange_book_top_imbalance_bps, decision.up_side);
            let aligned_depth_bps =
                aligned_move_bps(context.exchange_book_depth_imbalance_bps, decision.up_side);
            let fresh_confirmation_bps =
                codex_scalp_probe_v1_fresh_confirmation_bps(context, &decision, &self.config);
            let radar_score = codex_scalp_probe_v1_radar_score_bps(
                context,
                &decision,
                aligned_top_bps,
                aligned_depth_bps,
                fresh_confirmation_bps,
            );
            format!(", scalp_radar {}", radar_score.round_dp(1))
        } else {
            String::new()
        };

        Some(Opportunity {
            kind,
            condition_id: market.condition_id.clone(),
            slug: market.slug.clone(),
            question: market.question.clone(),
            outcome_a_label: "Up".to_owned(),
            outcome_a_token_id: quotes.up_token.to_owned(),
            outcome_b_label: "Down".to_owned(),
            outcome_b_token_id: quotes.down_token.to_owned(),
            liquidity_usdc: market.liquidity_usdc,
            outcome_a_ask_price: quotes.up_ask_price,
            outcome_b_ask_price: quotes.down_ask_price,
            bundle_cost: (quotes.up_ask_price + quotes.down_ask_price).round_dp(6),
            net_bundle_cost: effective_unit_cost.round_dp(6),
            edge_per_share: edge_per_share.round_dp(6),
            edge_bps,
            tradable_shares,
            required_usdc: primary_fill.total_cost.round_dp(6),
            expected_payout,
            expected_profit,
            interval_open_price: context.interval_open_price,
            target_price: context.target_price,
            target_price_source: context.target_price_source,
            target_gap_bps: context.target_gap_bps,
            current_spot_price: context.current_spot_price,
            spot_move_bps: context.spot_move_bps,
            spot_move_1s_bps: context.spot_move_1s_bps,
            spot_move_5s_bps: context.spot_move_5s_bps,
            spot_move_15s_bps: context.spot_move_15s_bps,
            micro_acceleration_bps: context.micro_acceleration_bps,
            micro_burst_reference_price: context.micro_burst_reference_price,
            micro_reference_price: context.micro_reference_price,
            signal_strength_bps: signal_strength_bps.round_dp(6),
            aligned_trade_flow_bps: decision.aligned_flow_bps.round_dp(6),
            signal_tier: decision.signal_tier.as_str().to_owned(),
            target_cross_label: target_cross.label().to_owned(),
            dominant_outcome: context.dominant_outcome.clone(),
            primary_outcome_label: primary.label.to_owned(),
            primary_outcome_token_id: primary.token_id.to_owned(),
            primary_outcome_ask_price: effective_unit_cost.round_dp(6),
            primary_fill_levels: primary_fill.levels,
            hedge_outcome_label: None,
            hedge_outcome_token_id: None,
            hedge_outcome_ask_price: None,
            hedge_fill_levels: Vec::new(),
            hedge_shares: Decimal::ZERO,
            seconds_left: context.seconds_left,
            note: format!(
                "{}{}({}/{regime}): gap {} bps, aligned 5s {} bps, aligned 15s {} bps, flow {} bps, target-cross {}{}{}{}{}. Taking {} at ask {}, signal {} bps.",
                kind.as_str(),
                if raw_scalp_ablation {
                    "-raw-ablation"
                } else {
                    ""
                },
                decision.signal_tier.as_str(),
                context.target_gap_bps.round_dp(2),
                decision.aligned_micro_bps.round_dp(2),
                decision.aligned_swing_bps.round_dp(2),
                decision.aligned_flow_bps.round_dp(2),
                target_cross.label(),
                confidence_note,
                breakout_note,
                discount_value_note,
                scalp_radar_note,
                primary.label,
                effective_unit_cost.round_dp(4),
                signal_strength_bps.round_dp(2),
            ),
        })
    }

    #[allow(clippy::too_many_lines)]
    fn evaluate_micro_breakout_candidate(
        &self,
        market: &BinaryMarket,
        context: &BtcFiveMinuteContext,
        quotes: MarketQuotes<'_>,
        book_refs: MarketBookRefs<'_>,
        available_market_capacity: Decimal,
        trade_flow: Option<&TradeFlowSummary>,
    ) -> Option<Opportunity> {
        if !self.config.enable_micro_breakout {
            return None;
        }

        if elapsed_window_secs(context) < self.config.micro_breakout_min_elapsed_window_secs {
            return None;
        }

        let spot_move_fixed = bps_to_fixed(context.spot_move_bps);
        let micro_move_fixed = bps_to_fixed(context.spot_move_5s_bps);
        let target_cross = recent_target_cross(context, &self.config);
        let burst_confirmed = has_aligned_micro_burst(context, &self.config);
        let burst_or_cross_confirmed = burst_confirmed || target_cross.is_active();
        if fixed_abs(spot_move_fixed)
            < u32_bps_to_fixed_abs(self.config.micro_breakout_min_spot_move_bps)
            || (fixed_abs(micro_move_fixed)
                < bps_threshold_to_fixed_abs(self.config.micro_breakout_min_spot_move_5s_bps)
                && !burst_or_cross_confirmed)
            || !fixed_moves_align(spot_move_fixed, micro_move_fixed)
        {
            return None;
        }
        if !burst_or_cross_confirmed {
            return None;
        }
        let micro_move_abs = context.spot_move_5s_bps.abs();

        let primary = primary_quote(context, quotes);
        let primary_book = primary_order_book(primary, book_refs);
        if !is_valid_binary_price(primary.ask_price)
            || primary.ask_price > self.config.micro_breakout_max_entry_price
        {
            return None;
        }

        let fifteen_second_momentum_confirmed =
            has_aligned_fifteen_second_momentum(context, &self.config);
        let positive_acceleration_confirmed = has_aligned_positive_acceleration(context);
        let aligned_flow_bps = aligned_trade_flow_bps(context, trade_flow);
        if !fifteen_second_momentum_confirmed
            || !positive_acceleration_confirmed
            || aligned_flow_bps <= Decimal::ZERO
        {
            return None;
        }

        let target_cross_boost_bps = if target_cross.is_active() {
            self.config.micro_breakout_target_cross_signal_boost_bps
        } else {
            Decimal::ZERO
        };
        let signal_strength_bps = (micro_breakout_signal_bps(context, trade_flow, &self.config)
            + target_cross_boost_bps)
            .round_dp(6);
        if signal_strength_bps < Decimal::from(self.config.micro_breakout_min_signal_bps) {
            return None;
        }
        let signal_tier = micro_breakout_signal_tier(context, trade_flow, &self.config);
        if self
            .config
            .micro_breakout_expensive_entry_requires_strong_tier
            && primary.ask_price > self.config.micro_breakout_expensive_entry_price
            && signal_tier != MicroBreakoutSignalTier::Strong
        {
            return None;
        }
        let full_size_allowed = micro_breakout_full_size_allowed(
            primary.ask_price,
            micro_move_abs,
            aligned_flow_bps,
            positive_acceleration_confirmed,
            signal_tier,
            &self.config,
        );

        let signal_anchor_price =
            directional_signal_anchor_price(signal_strength_bps, &self.config);
        let execution_slippage =
            Decimal::from(self.config.directional_execution_slippage_bps) / bps_denominator();
        let execution_anchor_price = (signal_anchor_price + execution_slippage).min(Decimal::ONE);
        let preliminary_edge_per_share =
            execution_anchor_price - primary.ask_price - self.fee_buffer();
        if preliminary_edge_per_share <= Decimal::ZERO {
            return None;
        }

        let max_notional = micro_breakout_entry_notional_cap(
            available_market_capacity,
            primary.ask_price,
            signal_tier,
            full_size_allowed,
            &self.config,
        );
        if max_notional <= Decimal::ZERO {
            return None;
        }

        let max_average_price = if full_size_allowed {
            sweep_average_price_limit(
                primary.ask_price,
                self.config.micro_breakout_full_size_max_entry_price,
            )
        } else {
            sweep_average_price_limit(
                primary.ask_price,
                self.config
                    .micro_breakout_expensive_entry_price
                    .min(self.config.micro_breakout_max_entry_price),
            )
        };
        let primary_fill =
            build_buy_fill_plan_for_notional(primary_book, max_notional, max_average_price)?;
        let tradable_shares = primary_fill.total_shares.round_dp(4);
        if tradable_shares < self.config.min_top_of_book_shares {
            return None;
        }

        let effective_unit_cost = primary_fill.average_price;
        let average_price_drift = saturating_sub(effective_unit_cost, primary.ask_price);
        if self.config.micro_breakout_max_average_price_drift > Decimal::ZERO
            && average_price_drift > self.config.micro_breakout_max_average_price_drift
        {
            return None;
        }
        let edge_per_share = execution_anchor_price - effective_unit_cost - self.fee_buffer();
        if edge_per_share <= Decimal::ZERO {
            return None;
        }

        let edge_bps = decimal_to_bps(edge_per_share);
        if edge_bps < self.config.directional_min_model_edge_bps {
            return None;
        }

        let required_usdc = primary_fill.total_cost.round_dp(6);
        let expected_payout = (tradable_shares * execution_anchor_price).round_dp(6);
        let expected_profit = (tradable_shares * edge_per_share).round_dp(6);

        Some(Opportunity {
            kind: OpportunityKind::MicroBreakout,
            condition_id: market.condition_id.clone(),
            slug: market.slug.clone(),
            question: market.question.clone(),
            outcome_a_label: "Up".to_owned(),
            outcome_a_token_id: quotes.up_token.to_owned(),
            outcome_b_label: "Down".to_owned(),
            outcome_b_token_id: quotes.down_token.to_owned(),
            liquidity_usdc: market.liquidity_usdc,
            outcome_a_ask_price: quotes.up_ask_price,
            outcome_b_ask_price: quotes.down_ask_price,
            bundle_cost: (quotes.up_ask_price + quotes.down_ask_price).round_dp(6),
            net_bundle_cost: effective_unit_cost.round_dp(6),
            edge_per_share: edge_per_share.round_dp(6),
            edge_bps,
            tradable_shares,
            required_usdc,
            expected_payout,
            expected_profit,
            interval_open_price: context.interval_open_price,
            target_price: context.target_price,
            target_price_source: context.target_price_source,
            target_gap_bps: context.target_gap_bps,
            current_spot_price: context.current_spot_price,
            spot_move_bps: context.spot_move_bps,
            spot_move_1s_bps: context.spot_move_1s_bps,
            spot_move_5s_bps: context.spot_move_5s_bps,
            spot_move_15s_bps: context.spot_move_15s_bps,
            micro_acceleration_bps: context.micro_acceleration_bps,
            micro_burst_reference_price: context.micro_burst_reference_price,
            micro_reference_price: context.micro_reference_price,
            signal_strength_bps: signal_strength_bps.round_dp(6),
            aligned_trade_flow_bps: aligned_flow_bps.round_dp(6),
            signal_tier: signal_tier.as_str().to_owned(),
            target_cross_label: target_cross.label().to_owned(),
            dominant_outcome: context.dominant_outcome.clone(),
            primary_outcome_label: primary.label.to_owned(),
            primary_outcome_token_id: primary.token_id.to_owned(),
            primary_outcome_ask_price: effective_unit_cost.round_dp(6),
            primary_fill_levels: primary_fill.levels,
            hedge_outcome_label: None,
            hedge_outcome_token_id: None,
            hedge_outcome_ask_price: None,
            hedge_fill_levels: Vec::new(),
            hedge_shares: Decimal::ZERO,
            seconds_left: context.seconds_left,
            note: format!(
                "micro-breakout({}): Binance 1s burst {} bps, 5s impulse {} bps, 15s impulse {} bps, acceleration {} bps, aligned flow {} bps, target-cross {} (+{} bps). Taking {} at ask {}, signal {} bps, cap {} USDC.",
                signal_tier.as_str(),
                context.spot_move_1s_bps.abs().round_dp(2),
                micro_move_abs.round_dp(2),
                context.spot_move_15s_bps.abs().round_dp(2),
                context.micro_acceleration_bps.round_dp(2),
                aligned_flow_bps.round_dp(2),
                target_cross.label(),
                target_cross_boost_bps.round_dp(2),
                primary.label,
                effective_unit_cost.round_dp(4),
                signal_strength_bps.round_dp(2),
                max_notional.round_dp(4),
            ),
        })
    }

    #[allow(clippy::too_many_lines)]
    fn evaluate_bundle_near_miss(
        &self,
        market: &BinaryMarket,
        context: &BtcFiveMinuteContext,
        quotes: MarketQuotes<'_>,
        available_market_capacity: Decimal,
    ) -> Option<ScoredNearMiss> {
        if !self.config.enable_bundle {
            return None;
        }

        if !is_valid_binary_price(quotes.up_ask_price)
            || !is_valid_binary_price(quotes.down_ask_price)
        {
            return None;
        }

        let spot_move_abs = context.spot_move_bps.abs();
        if spot_move_abs < Decimal::from(self.config.min_spot_move_bps) {
            let shortfall_bps =
                whole_bps_shortfall(spot_move_abs, Decimal::from(self.config.min_spot_move_bps));
            return Some(scored_near_miss(
                market,
                context,
                OpportunityKind::BundleArbitrage,
                context.dominant_outcome.clone(),
                None,
                Some((quotes.up_ask_price + quotes.down_ask_price).round_dp(6)),
                shortfall_bps,
                format!("{shortfall_bps} bps"),
                "Binance bundle".to_owned(),
                1,
            ));
        }

        let depth_shares = quotes.up_ask_size.min(quotes.down_ask_size);
        if depth_shares < self.config.min_top_of_book_shares {
            let missing_shares = saturating_sub(self.config.min_top_of_book_shares, depth_shares);
            return Some(scored_near_miss(
                market,
                context,
                OpportunityKind::BundleArbitrage,
                context.dominant_outcome.clone(),
                None,
                Some((quotes.up_ask_price + quotes.down_ask_price).round_dp(6)),
                decimal_to_whole_units(missing_shares),
                format!("{} shares", missing_shares.round_dp(2)),
                "bundle".to_owned(),
                3,
            ));
        }

        let gross_bundle_cost = quotes.up_ask_price + quotes.down_ask_price;
        let net_bundle_cost = gross_bundle_cost + self.fee_buffer();
        let edge_per_share = Decimal::ONE - net_bundle_cost;
        if edge_per_share <= Decimal::ZERO {
            let shortfall_bps = decimal_to_bps_ceil(net_bundle_cost - Decimal::ONE);
            return Some(scored_near_miss(
                market,
                context,
                OpportunityKind::BundleArbitrage,
                context.dominant_outcome.clone(),
                None,
                Some(gross_bundle_cost.round_dp(6)),
                shortfall_bps,
                format!("{shortfall_bps} bps"),
                "bundle fee".to_owned(),
                0,
            ));
        }

        let edge_bps = decimal_to_bps(edge_per_share);
        if edge_bps < self.config.min_edge_bps {
            let shortfall_bps = self.config.min_edge_bps - edge_bps;
            return Some(scored_near_miss(
                market,
                context,
                OpportunityKind::BundleArbitrage,
                context.dominant_outcome.clone(),
                None,
                Some(gross_bundle_cost.round_dp(6)),
                shortfall_bps,
                format!("{shortfall_bps} bps"),
                "edge bundle".to_owned(),
                0,
            ));
        }

        let max_notional = self
            .config
            .max_bundle_notional_usdc
            .min(available_market_capacity);
        if max_notional <= Decimal::ZERO {
            return Some(scored_near_miss(
                market,
                context,
                OpportunityKind::BundleArbitrage,
                context.dominant_outcome.clone(),
                None,
                Some(gross_bundle_cost.round_dp(6)),
                0,
                "risk-limit".to_owned(),
                "bundle market risk limit reached".to_owned(),
                4,
            ));
        }

        let affordable_shares = max_notional / gross_bundle_cost;
        let tradable_shares = depth_shares.min(affordable_shares).round_dp(4);
        if tradable_shares < self.config.min_top_of_book_shares {
            let missing_shares =
                saturating_sub(self.config.min_top_of_book_shares, tradable_shares);
            return Some(scored_near_miss(
                market,
                context,
                OpportunityKind::BundleArbitrage,
                context.dominant_outcome.clone(),
                None,
                Some(gross_bundle_cost.round_dp(6)),
                decimal_to_whole_units(missing_shares),
                format!("{} shares", missing_shares.round_dp(2)),
                "notional bundle".to_owned(),
                3,
            ));
        }

        None
    }

    #[allow(clippy::too_many_lines)]
    fn evaluate_directional_near_miss(
        &self,
        market: &BinaryMarket,
        context: &BtcFiveMinuteContext,
        quotes: MarketQuotes<'_>,
        available_market_capacity: Decimal,
        trade_flow: Option<&TradeFlowSummary>,
    ) -> Option<ScoredNearMiss> {
        if !self.config.enable_directional {
            return None;
        }

        let spot_move_abs = context.spot_move_bps.abs();
        let primary = primary_quote(context, quotes);
        let hedge = secondary_quote(context, quotes);
        let mut hedge_kind = OpportunityKind::DirectionalMomentum;

        if spot_move_abs < Decimal::from(self.config.directional_min_spot_move_bps) {
            let shortfall_bps = whole_bps_shortfall(
                spot_move_abs,
                Decimal::from(self.config.directional_min_spot_move_bps),
            );
            return Some(scored_near_miss(
                market,
                context,
                hedge_kind,
                primary.label.to_owned(),
                Some(primary.ask_price.round_dp(6)),
                Some((quotes.up_ask_price + quotes.down_ask_price).round_dp(6)),
                shortfall_bps,
                format!("{shortfall_bps} bps"),
                "Binance directional".to_owned(),
                1,
            ));
        }

        let velocity_bps_per_minute = directional_velocity_bps_per_minute(context);
        if velocity_bps_per_minute
            < Decimal::from(self.config.directional_min_velocity_bps_per_minute)
        {
            let shortfall_bps = whole_bps_shortfall(
                velocity_bps_per_minute,
                Decimal::from(self.config.directional_min_velocity_bps_per_minute),
            );
            return Some(scored_near_miss(
                market,
                context,
                hedge_kind,
                primary.label.to_owned(),
                Some(primary.ask_price.round_dp(6)),
                Some((quotes.up_ask_price + quotes.down_ask_price).round_dp(6)),
                shortfall_bps,
                format!("{shortfall_bps} bps/min"),
                "Binance directional".to_owned(),
                1,
            ));
        }

        let signal_strength_bps =
            directional_effective_signal_bps(context, trade_flow, &self.config);
        let hedge_ratio = self.directional_tail_hedge_ratio(
            context,
            primary,
            hedge,
            signal_strength_bps,
            velocity_bps_per_minute,
        );
        let strong_entry_signal =
            directional_is_strong_entry_signal(context, trade_flow, &self.config);
        if hedge_ratio > Decimal::ZERO {
            hedge_kind = OpportunityKind::DirectionalMomentumHedged;
        }
        if signal_strength_bps < Decimal::from(self.config.directional_min_signal_bps) {
            let shortfall_bps = whole_bps_shortfall(
                signal_strength_bps,
                Decimal::from(self.config.directional_min_signal_bps),
            );
            return Some(scored_near_miss(
                market,
                context,
                hedge_kind,
                primary.label.to_owned(),
                Some(primary.ask_price.round_dp(6)),
                Some((quotes.up_ask_price + quotes.down_ask_price).round_dp(6)),
                shortfall_bps,
                format!("{shortfall_bps} bps"),
                "Binance/flow directional".to_owned(),
                1,
            ));
        }

        if self.config.directional_require_hedge_for_soft_entry
            && hedge_ratio <= Decimal::ZERO
            && !strong_entry_signal
        {
            return Some(scored_near_miss(
                market,
                context,
                hedge_kind,
                primary.label.to_owned(),
                Some(primary.ask_price.round_dp(6)),
                Some((quotes.up_ask_price + quotes.down_ask_price).round_dp(6)),
                1,
                "hedge".to_owned(),
                "plain directional hedge strong-confirmation".to_owned(),
                1,
            ));
        }

        if !is_valid_binary_price(primary.ask_price) {
            return None;
        }

        if primary.ask_price > self.config.directional_max_entry_price {
            let shortfall_bps =
                decimal_to_bps_ceil(primary.ask_price - self.config.directional_max_entry_price);
            return Some(scored_near_miss(
                market,
                context,
                hedge_kind,
                primary.label.to_owned(),
                Some(primary.ask_price.round_dp(6)),
                Some((quotes.up_ask_price + quotes.down_ask_price).round_dp(6)),
                shortfall_bps,
                format!("{shortfall_bps} bps"),
                "directional ask".to_owned(),
                0,
            ));
        }

        if primary.ask_size < self.config.min_top_of_book_shares {
            let missing_shares =
                saturating_sub(self.config.min_top_of_book_shares, primary.ask_size);
            return Some(scored_near_miss(
                market,
                context,
                hedge_kind,
                primary.label.to_owned(),
                Some(primary.ask_price.round_dp(6)),
                Some((quotes.up_ask_price + quotes.down_ask_price).round_dp(6)),
                decimal_to_whole_units(missing_shares),
                format!("{} shares", missing_shares.round_dp(2)),
                "directional".to_owned(),
                3,
            ));
        }

        let signal_anchor_price =
            directional_signal_anchor_price(signal_strength_bps, &self.config);
        let execution_slippage =
            Decimal::from(self.config.directional_execution_slippage_bps) / bps_denominator();
        let execution_anchor_price = (signal_anchor_price + execution_slippage).min(Decimal::ONE);
        let effective_unit_cost = primary.ask_price + (hedge_ratio * hedge.ask_price);
        let effective_unit_payout =
            execution_anchor_price + (hedge_ratio * (Decimal::ONE - execution_anchor_price));
        let edge_per_share = effective_unit_payout
            - effective_unit_cost
            - self.fee_buffer() * (Decimal::ONE + hedge_ratio);
        if edge_per_share <= Decimal::ZERO {
            let shortfall_bps = decimal_to_bps_ceil(
                effective_unit_cost + self.fee_buffer() - effective_unit_payout,
            );
            return Some(scored_near_miss(
                market,
                context,
                hedge_kind,
                primary.label.to_owned(),
                Some(primary.ask_price.round_dp(6)),
                Some((quotes.up_ask_price + quotes.down_ask_price).round_dp(6)),
                shortfall_bps,
                format!("{shortfall_bps} bps"),
                "Polymarket Binance".to_owned(),
                0,
            ));
        }

        let edge_bps = decimal_to_bps(edge_per_share);
        if edge_bps < self.config.directional_min_model_edge_bps {
            let shortfall_bps = self.config.directional_min_model_edge_bps - edge_bps;
            return Some(scored_near_miss(
                market,
                context,
                hedge_kind,
                primary.label.to_owned(),
                Some(primary.ask_price.round_dp(6)),
                Some((quotes.up_ask_price + quotes.down_ask_price).round_dp(6)),
                shortfall_bps,
                format!("{shortfall_bps} bps"),
                "edge directional".to_owned(),
                0,
            ));
        }

        let max_notional = directional_entry_notional_cap(
            available_market_capacity,
            signal_strength_bps,
            strong_entry_signal,
            &self.config,
        );
        if max_notional <= Decimal::ZERO {
            return Some(scored_near_miss(
                market,
                context,
                hedge_kind,
                primary.label.to_owned(),
                Some(primary.ask_price.round_dp(6)),
                Some((quotes.up_ask_price + quotes.down_ask_price).round_dp(6)),
                0,
                "risk-limit".to_owned(),
                "directional market risk limit reached".to_owned(),
                4,
            ));
        }

        let hedge_depth_limit = if hedge_ratio > Decimal::ZERO {
            hedge.ask_size / hedge_ratio
        } else {
            primary.ask_size
        };
        let affordable_shares = max_notional / effective_unit_cost;
        let tradable_shares = primary
            .ask_size
            .min(hedge_depth_limit)
            .min(affordable_shares)
            .round_dp(4);
        if tradable_shares < self.config.min_top_of_book_shares {
            let missing_shares =
                saturating_sub(self.config.min_top_of_book_shares, tradable_shares);
            return Some(scored_near_miss(
                market,
                context,
                hedge_kind,
                primary.label.to_owned(),
                Some(primary.ask_price.round_dp(6)),
                Some((quotes.up_ask_price + quotes.down_ask_price).round_dp(6)),
                decimal_to_whole_units(missing_shares),
                format!("{} shares", missing_shares.round_dp(2)),
                "notional directional".to_owned(),
                3,
            ));
        }

        None
    }

    #[allow(clippy::too_many_lines)]
    fn evaluate_target_state_near_miss(
        &self,
        market: &BinaryMarket,
        context: &BtcFiveMinuteContext,
        quotes: MarketQuotes<'_>,
        available_market_capacity: Decimal,
        trade_flow: Option<&TradeFlowSummary>,
    ) -> Option<ScoredNearMiss> {
        if !self.config.enable_target_state_v1 {
            return None;
        }

        let primary = primary_quote(context, quotes);
        let target_gap_abs = context.target_gap_bps.abs();
        if elapsed_window_secs(context) < self.config.target_state_min_elapsed_window_secs {
            let shortfall_secs =
                self.config.target_state_min_elapsed_window_secs - elapsed_window_secs(context);
            return Some(scored_near_miss(
                market,
                context,
                OpportunityKind::TargetStateV1,
                primary.label.to_owned(),
                Some(primary.ask_price.round_dp(6)),
                Some((quotes.up_ask_price + quotes.down_ask_price).round_dp(6)),
                u32::try_from(shortfall_secs).unwrap_or(u32::MAX),
                format!("{shortfall_secs} secs"),
                "target-state-v1".to_owned(),
                1,
            ));
        }

        if context.seconds_left > self.config.target_state_max_seconds_left {
            let shortfall_secs = context.seconds_left - self.config.target_state_max_seconds_left;
            return Some(scored_near_miss(
                market,
                context,
                OpportunityKind::TargetStateV1,
                primary.label.to_owned(),
                Some(primary.ask_price.round_dp(6)),
                Some((quotes.up_ask_price + quotes.down_ask_price).round_dp(6)),
                u32::try_from(shortfall_secs).unwrap_or(u32::MAX),
                format!("{shortfall_secs} secs"),
                "target-state-v1".to_owned(),
                1,
            ));
        }

        if target_gap_abs < self.config.target_state_min_target_gap_bps {
            let shortfall_bps =
                whole_bps_shortfall(target_gap_abs, self.config.target_state_min_target_gap_bps);
            return Some(scored_near_miss(
                market,
                context,
                OpportunityKind::TargetStateV1,
                primary.label.to_owned(),
                Some(primary.ask_price.round_dp(6)),
                Some((quotes.up_ask_price + quotes.down_ask_price).round_dp(6)),
                shortfall_bps,
                format!("{shortfall_bps} bps"),
                "Binance target target-state-v1".to_owned(),
                1,
            ));
        }

        if !moves_align(context.target_gap_bps, context.spot_move_15s_bps) {
            return Some(scored_near_miss(
                market,
                context,
                OpportunityKind::TargetStateV1,
                primary.label.to_owned(),
                Some(primary.ask_price.round_dp(6)),
                Some((quotes.up_ask_price + quotes.down_ask_price).round_dp(6)),
                1,
                "alignment".to_owned(),
                "15s- Binance target".to_owned(),
                1,
            ));
        }

        if context.spot_move_15s_bps.abs() < self.config.target_state_min_spot_move_15s_bps {
            let shortfall_bps = whole_bps_shortfall(
                context.spot_move_15s_bps.abs(),
                self.config.target_state_min_spot_move_15s_bps,
            );
            return Some(scored_near_miss(
                market,
                context,
                OpportunityKind::TargetStateV1,
                primary.label.to_owned(),
                Some(primary.ask_price.round_dp(6)),
                Some((quotes.up_ask_price + quotes.down_ask_price).round_dp(6)),
                shortfall_bps,
                format!("{shortfall_bps} bps"),
                "15s- target".to_owned(),
                1,
            ));
        }

        let aligned_flow_bps = aligned_trade_flow_bps(context, trade_flow);
        if aligned_flow_bps < self.config.target_state_min_aligned_flow_bps {
            let shortfall_bps = whole_bps_shortfall(
                aligned_flow_bps.max(Decimal::ZERO),
                self.config.target_state_min_aligned_flow_bps,
            );
            return Some(scored_near_miss(
                market,
                context,
                OpportunityKind::TargetStateV1,
                primary.label.to_owned(),
                Some(primary.ask_price.round_dp(6)),
                Some((quotes.up_ask_price + quotes.down_ask_price).round_dp(6)),
                shortfall_bps,
                format!("{shortfall_bps} bps"),
                "trade-flow target-state".to_owned(),
                1,
            ));
        }

        if !is_valid_binary_price(primary.ask_price) {
            return None;
        }

        if primary.ask_price > self.config.target_state_max_entry_price {
            let shortfall_bps =
                decimal_to_bps_ceil(primary.ask_price - self.config.target_state_max_entry_price);
            return Some(scored_near_miss(
                market,
                context,
                OpportunityKind::TargetStateV1,
                primary.label.to_owned(),
                Some(primary.ask_price.round_dp(6)),
                Some((quotes.up_ask_price + quotes.down_ask_price).round_dp(6)),
                shortfall_bps,
                format!("{shortfall_bps} bps"),
                "ask target-state-v1".to_owned(),
                0,
            ));
        }

        let signal_strength_bps = target_state_signal_bps(context, trade_flow, &self.config);
        if signal_strength_bps < Decimal::from(self.config.target_state_min_signal_bps) {
            let shortfall_bps = whole_bps_shortfall(
                signal_strength_bps,
                Decimal::from(self.config.target_state_min_signal_bps),
            );
            return Some(scored_near_miss(
                market,
                context,
                OpportunityKind::TargetStateV1,
                primary.label.to_owned(),
                Some(primary.ask_price.round_dp(6)),
                Some((quotes.up_ask_price + quotes.down_ask_price).round_dp(6)),
                shortfall_bps,
                format!("{shortfall_bps} bps"),
                "signal-strength target-state".to_owned(),
                1,
            ));
        }

        if target_state_entry_notional_cap(
            available_market_capacity,
            target_state_signal_tier(context, trade_flow, &self.config),
            &self.config,
        ) <= Decimal::ZERO
        {
            return Some(scored_near_miss(
                market,
                context,
                OpportunityKind::TargetStateV1,
                primary.label.to_owned(),
                Some(primary.ask_price.round_dp(6)),
                Some((quotes.up_ask_price + quotes.down_ask_price).round_dp(6)),
                0,
                "risk-limit".to_owned(),
                "target-state market risk limit reached".to_owned(),
                4,
            ));
        }

        None
    }

    #[allow(clippy::too_many_lines)]
    fn evaluate_bonereaper_state_near_miss(
        &self,
        market: &BinaryMarket,
        context: &BtcFiveMinuteContext,
        quotes: MarketQuotes<'_>,
        available_market_capacity: Decimal,
        trade_flow: Option<&TradeFlowSummary>,
    ) -> Option<ScoredNearMiss> {
        if !self.config.enable_bonereaper_state_v1 {
            return None;
        }

        if elapsed_window_secs(context) < self.config.bonereaper_state_min_elapsed_window_secs {
            let shortfall_secs =
                self.config.bonereaper_state_min_elapsed_window_secs - elapsed_window_secs(context);
            return Some(scored_near_miss(
                market,
                context,
                OpportunityKind::BonereaperStateV1,
                context.dominant_outcome.clone(),
                None,
                None,
                non_negative_u32(shortfall_secs),
                format!("{shortfall_secs}s"),
                "bonereaper-state-v1".to_owned(),
                1,
            ));
        }

        if context.seconds_left > self.config.bonereaper_state_max_seconds_left {
            let shortfall_secs =
                context.seconds_left - self.config.bonereaper_state_max_seconds_left;
            return Some(scored_near_miss(
                market,
                context,
                OpportunityKind::BonereaperStateV1,
                context.dominant_outcome.clone(),
                None,
                None,
                non_negative_u32(shortfall_secs),
                format!("{shortfall_secs}s"),
                "bonereaper-state-v1".to_owned(),
                1,
            ));
        }

        let target_gap_abs = context.target_gap_bps.abs();
        if target_gap_abs < self.config.bonereaper_state_min_target_gap_bps {
            let shortfall_bps = whole_bps_shortfall(
                target_gap_abs,
                self.config.bonereaper_state_min_target_gap_bps,
            );
            return Some(scored_near_miss(
                market,
                context,
                OpportunityKind::BonereaperStateV1,
                context.dominant_outcome.clone(),
                Some(primary_quote(context, quotes).ask_price),
                None,
                shortfall_bps,
                format!("{shortfall_bps} bps"),
                "target gap bonereaper-state-v1".to_owned(),
                2,
            ));
        }

        let up_side = primary_side_from_context(context);
        let aligned_swing_bps = aligned_move_bps(context.spot_move_15s_bps, up_side);
        if aligned_swing_bps < self.config.bonereaper_state_min_spot_move_15s_bps {
            let shortfall_bps = whole_bps_shortfall(
                aligned_swing_bps,
                self.config.bonereaper_state_min_spot_move_15s_bps,
            );
            return Some(scored_near_miss(
                market,
                context,
                OpportunityKind::BonereaperStateV1,
                context.dominant_outcome.clone(),
                Some(primary_quote(context, quotes).ask_price),
                None,
                shortfall_bps,
                format!("{shortfall_bps} bps"),
                "15s- bonereaper-state-v1".to_owned(),
                1,
            ));
        }

        let aligned_micro_bps = aligned_move_bps(context.spot_move_5s_bps, up_side);
        if aligned_micro_bps < self.config.bonereaper_state_min_spot_move_5s_bps {
            let shortfall_bps = whole_bps_shortfall(
                aligned_micro_bps,
                self.config.bonereaper_state_min_spot_move_5s_bps,
            );
            return Some(scored_near_miss(
                market,
                context,
                OpportunityKind::BonereaperStateV1,
                context.dominant_outcome.clone(),
                Some(primary_quote(context, quotes).ask_price),
                None,
                shortfall_bps,
                format!("{shortfall_bps} bps"),
                "5s- bonereaper-state-v1".to_owned(),
                1,
            ));
        }

        let primary = primary_quote(context, quotes);
        if primary.ask_price > self.config.bonereaper_state_max_entry_price {
            let shortfall_bps = decimal_to_bps_ceil(
                primary.ask_price - self.config.bonereaper_state_max_entry_price,
            );
            return Some(scored_near_miss(
                market,
                context,
                OpportunityKind::BonereaperStateV1,
                primary.label.to_owned(),
                Some(primary.ask_price),
                None,
                shortfall_bps,
                format!("{shortfall_bps} bps"),
                "bonereaper-state-v1".to_owned(),
                0,
            ));
        }

        if bonereaper_state_entry_notional_cap(
            available_market_capacity,
            bonereaper_state_signal_tier(context, trade_flow, &self.config),
            &self.config,
        ) <= Decimal::ZERO
        {
            return Some(scored_near_miss(
                market,
                context,
                OpportunityKind::BonereaperStateV1,
                primary.label.to_owned(),
                Some(primary.ask_price),
                None,
                0,
                "risk-limit".to_owned(),
                "bonereaper-state-v1 market risk limit reached".to_owned(),
                4,
            ));
        }

        let signal_strength_bps = bonereaper_state_signal_bps(context, trade_flow, &self.config);
        if signal_strength_bps < Decimal::from(self.config.bonereaper_state_min_signal_bps) {
            let shortfall_bps = whole_bps_shortfall(
                signal_strength_bps,
                Decimal::from(self.config.bonereaper_state_min_signal_bps),
            );
            return Some(scored_near_miss(
                market,
                context,
                OpportunityKind::BonereaperStateV1,
                primary.label.to_owned(),
                Some(primary.ask_price),
                None,
                shortfall_bps,
                format!("{shortfall_bps} bps"),
                "state-signal bonereaper-state-v1".to_owned(),
                0,
            ));
        }

        None
    }

    #[allow(clippy::too_many_lines)]
    fn evaluate_bonereaper_state_v2_near_miss(
        &self,
        market: &BinaryMarket,
        context: &BtcFiveMinuteContext,
        quotes: MarketQuotes<'_>,
        book_refs: MarketBookRefs<'_>,
        available_market_capacity: Decimal,
        trade_flow: Option<&TradeFlowSummary>,
    ) -> Option<ScoredNearMiss> {
        self.evaluate_bonereaper_state_v2_variant_near_miss(
            market,
            context,
            quotes,
            book_refs,
            available_market_capacity,
            trade_flow,
            OpportunityKind::BonereaperStateV2,
            self.config.enable_bonereaper_state_v2,
        )
    }

    #[allow(clippy::too_many_lines)]
    fn evaluate_bonereaper_state_guarded_near_miss(
        &self,
        market: &BinaryMarket,
        context: &BtcFiveMinuteContext,
        quotes: MarketQuotes<'_>,
        book_refs: MarketBookRefs<'_>,
        available_market_capacity: Decimal,
        trade_flow: Option<&TradeFlowSummary>,
    ) -> Option<ScoredNearMiss> {
        self.evaluate_bonereaper_state_v2_variant_near_miss(
            market,
            context,
            quotes,
            book_refs,
            available_market_capacity,
            trade_flow,
            OpportunityKind::BonereaperStateGuarded,
            self.config.enable_bonereaper_state_guarded,
        )
    }

    #[allow(clippy::too_many_lines)]
    fn evaluate_codex_sentinel_v1_near_miss(
        &self,
        market: &BinaryMarket,
        context: &BtcFiveMinuteContext,
        quotes: MarketQuotes<'_>,
        book_refs: MarketBookRefs<'_>,
        available_market_capacity: Decimal,
        trade_flow: Option<&TradeFlowSummary>,
    ) -> Option<ScoredNearMiss> {
        if !self.config.enable_codex_sentinel_v1
            || !codex_sentinel_v1_target_allowed(context.target)
        {
            return None;
        }

        self.evaluate_bonereaper_state_v2_variant_near_miss(
            market,
            context,
            quotes,
            book_refs,
            available_market_capacity,
            trade_flow,
            OpportunityKind::CodexSentinelV1,
            true,
        )
    }

    #[allow(clippy::too_many_lines)]
    fn evaluate_codex_scalp_probe_v1_near_miss(
        &self,
        market: &BinaryMarket,
        context: &BtcFiveMinuteContext,
        quotes: MarketQuotes<'_>,
        book_refs: MarketBookRefs<'_>,
        available_market_capacity: Decimal,
        trade_flow: Option<&TradeFlowSummary>,
    ) -> Option<ScoredNearMiss> {
        if !self.config.enable_codex_scalp_probe_v1
            || !codex_scalp_probe_v1_target_allowed(context.target)
        {
            return None;
        }

        let kind = OpportunityKind::CodexScalpProbeV1;
        let label = kind.as_str();

        if elapsed_window_secs(context) < self.config.codex_scalp_probe_v1_min_elapsed_window_secs {
            let shortfall_secs = self.config.codex_scalp_probe_v1_min_elapsed_window_secs
                - elapsed_window_secs(context);
            return Some(scored_near_miss(
                market,
                context,
                kind,
                context.dominant_outcome.clone(),
                None,
                None,
                non_negative_u32(shortfall_secs),
                format!("{shortfall_secs}s"),
                format!("window is still too young for {label}"),
                1,
            ));
        }

        if context.seconds_left > self.config.codex_scalp_probe_v1_max_seconds_left {
            let shortfall_secs =
                context.seconds_left - self.config.codex_scalp_probe_v1_max_seconds_left;
            return Some(scored_near_miss(
                market,
                context,
                kind,
                context.dominant_outcome.clone(),
                None,
                None,
                non_negative_u32(shortfall_secs),
                format!("{shortfall_secs}s"),
                format!("it is still too early in the window for {label}"),
                1,
            ));
        }

        if context.seconds_left < self.config.codex_scalp_probe_v1_min_seconds_left {
            let late_secs =
                self.config.codex_scalp_probe_v1_min_seconds_left - context.seconds_left;
            return Some(scored_near_miss(
                market,
                context,
                kind,
                context.dominant_outcome.clone(),
                None,
                None,
                non_negative_u32(late_secs),
                format!("{late_secs}s"),
                format!("it is already too late in the window for {label}"),
                1,
            ));
        }

        if !codex_sentinel_v1_has_fresh_live_spot(
            context,
            self.config.codex_sentinel_v1_max_live_quote_age_ms,
        ) {
            let primary = primary_quote(context, quotes);
            return Some(scored_near_miss(
                market,
                context,
                kind,
                primary.label.to_owned(),
                Some(primary.ask_price),
                None,
                0,
                "stale quote".to_owned(),
                format!("live spot quote is stale for {label}"),
                0,
            ));
        }

        let decision = match bonereaper_state_v2_decision(context, trade_flow, &self.config, true) {
            Some(decision) => decision,
            None => {
                let primary = primary_quote(context, quotes);
                return Some(scored_near_miss(
                    market,
                    context,
                    kind,
                    primary.label.to_owned(),
                    Some(primary.ask_price),
                    None,
                    0,
                    "signal".to_owned(),
                    format!("state signal is still below threshold for {label}"),
                    1,
                ));
            }
        };

        let primary = quote_for_side(quotes, decision.up_side);
        let primary_book = order_book_for_side(decision.up_side, book_refs);
        let raw_light_mode = codex_scalp_probe_v1_raw_light_mode(&self.config);
        let raw_light_profile =
            raw_light_mode.then(|| codex_scalp_probe_v1_raw_light_profile(context.target));
        let use_bnb_pressure_thresholds = !raw_light_mode
            && self.config.codex_scalp_probe_v1_bnb_pressure_enabled
            && context.target == MarketTarget::Bnb5m;
        let min_entry_price = raw_light_profile.map_or(
            self.config.codex_scalp_probe_v1_min_entry_price,
            |profile| profile.min_entry_price,
        );
        let max_entry_price = if use_bnb_pressure_thresholds {
            self.config
                .codex_scalp_probe_v1_bnb_pressure_max_entry_price
        } else {
            raw_light_profile.map_or(
                self.config.codex_scalp_probe_v1_max_entry_price,
                |profile| profile.max_entry_price,
            )
        };
        let max_book_age_ms = if use_bnb_pressure_thresholds {
            self.config
                .codex_scalp_probe_v1_bnb_pressure_max_book_age_ms
        } else {
            self.config.codex_scalp_probe_v1_max_book_age_ms
        };
        let min_top_imbalance_bps = if use_bnb_pressure_thresholds {
            self.config
                .codex_scalp_probe_v1_bnb_pressure_min_top_imbalance_bps
        } else {
            self.config.codex_scalp_probe_v1_min_top_imbalance_bps
        };
        let min_depth_imbalance_bps = if use_bnb_pressure_thresholds {
            self.config
                .codex_scalp_probe_v1_bnb_pressure_min_depth_imbalance_bps
        } else {
            self.config.codex_scalp_probe_v1_min_depth_imbalance_bps
        };
        let min_target_gap_bps = if use_bnb_pressure_thresholds {
            self.config
                .codex_scalp_probe_v1_bnb_pressure_min_target_gap_bps
        } else {
            self.config.codex_scalp_probe_v1_min_target_gap_bps
        };
        let min_fresh_bps = if use_bnb_pressure_thresholds {
            self.config.codex_scalp_probe_v1_bnb_pressure_min_fresh_bps
        } else {
            self.config.codex_scalp_probe_v1_min_fresh_bps
        };

        if self
            .config
            .codex_scalp_probe_v1_notional_usdc
            .min(available_market_capacity)
            <= Decimal::ZERO
        {
            return Some(scored_near_miss(
                market,
                context,
                kind,
                primary.label.to_owned(),
                Some(primary.ask_price),
                None,
                0,
                "limit".to_owned(),
                "risk limit for this market is already exhausted".to_owned(),
                4,
            ));
        }

        if primary.ask_price < min_entry_price {
            let shortfall_bps = decimal_to_bps_ceil(min_entry_price - primary.ask_price);
            return Some(scored_near_miss(
                market,
                context,
                kind,
                primary.label.to_owned(),
                Some(primary.ask_price),
                None,
                shortfall_bps,
                format!("{shortfall_bps} bps"),
                format!("entry price is below the tested value band for {label}"),
                2,
            ));
        }

        if primary.ask_price > max_entry_price {
            let shortfall_bps = decimal_to_bps_ceil(primary.ask_price - max_entry_price);
            return Some(scored_near_miss(
                market,
                context,
                kind,
                primary.label.to_owned(),
                Some(primary.ask_price),
                None,
                shortfall_bps,
                format!("{shortfall_bps} bps"),
                format!("entry price is already too expensive for {label}"),
                0,
            ));
        }

        let Some(entry_spread) = codex_sentinel_v1_entry_spread(primary.ask_price, primary_book)
        else {
            return Some(scored_near_miss(
                market,
                context,
                kind,
                primary.label.to_owned(),
                Some(primary.ask_price),
                None,
                0,
                "missing bid".to_owned(),
                format!("entry spread is unknown for {label}"),
                0,
            ));
        };
        if entry_spread > self.config.codex_scalp_probe_v1_max_entry_spread {
            let shortfall_bps = decimal_to_bps_ceil(
                entry_spread - self.config.codex_scalp_probe_v1_max_entry_spread,
            );
            return Some(scored_near_miss(
                market,
                context,
                kind,
                primary.label.to_owned(),
                Some(primary.ask_price),
                None,
                shortfall_bps,
                format!("spread {}", entry_spread.round_dp(4)),
                format!("entry spread is too wide for {label}"),
                0,
            ));
        }

        if !codex_sentinel_v1_has_fresh_live_spot(
            context,
            self.config.codex_sentinel_v1_max_live_quote_age_ms,
        ) {
            return Some(scored_near_miss(
                market,
                context,
                kind,
                primary.label.to_owned(),
                Some(primary.ask_price),
                None,
                0,
                "stale quote".to_owned(),
                format!("live spot quote is stale for {label}"),
                0,
            ));
        }

        let Some(book_age_ms) = context.exchange_book_age_ms else {
            return Some(scored_near_miss(
                market,
                context,
                kind,
                primary.label.to_owned(),
                Some(primary.ask_price),
                None,
                0,
                "missing book".to_owned(),
                format!("exchange orderbook is missing for {label}"),
                0,
            ));
        };
        if book_age_ms < 0 || book_age_ms > max_book_age_ms {
            let shortfall_ms = book_age_ms - max_book_age_ms;
            return Some(scored_near_miss(
                market,
                context,
                kind,
                primary.label.to_owned(),
                Some(primary.ask_price),
                None,
                non_negative_u32(shortfall_ms),
                format!("{book_age_ms} ms"),
                format!("exchange orderbook is stale for {label}"),
                0,
            ));
        }

        let aligned_top_bps =
            aligned_move_bps(context.exchange_book_top_imbalance_bps, decision.up_side);
        let aligned_depth_bps =
            aligned_move_bps(context.exchange_book_depth_imbalance_bps, decision.up_side);
        let fresh_confirmation_bps =
            codex_scalp_probe_v1_fresh_confirmation_bps(context, &decision, &self.config);
        let radar = CodexScalpProbeRadar {
            score_bps: codex_scalp_probe_v1_radar_score_bps(
                context,
                &decision,
                aligned_top_bps,
                aligned_depth_bps,
                fresh_confirmation_bps,
            ),
            aligned_top_bps,
            aligned_depth_bps,
            aligned_microprice_bps: aligned_move_bps(
                context.exchange_book_microprice_bps,
                decision.up_side,
            ),
            aligned_burst_bps: aligned_move_bps(context.spot_move_1s_bps, decision.up_side),
            fresh_confirmation_bps,
        };
        let thresholds = CodexScalpProbeThresholds {
            min_target_gap_bps,
            min_fresh_bps,
            min_top_imbalance_bps,
            min_depth_imbalance_bps,
        };
        let high_pressure_override =
            codex_scalp_probe_v1_high_pressure_override_allows(radar, thresholds, &self.config);

        if context.exchange_book_spread_bps
            > self.config.codex_scalp_probe_v1_max_exchange_spread_bps
        {
            let shortfall_bps = whole_bps_shortfall(
                context.exchange_book_spread_bps,
                self.config.codex_scalp_probe_v1_max_exchange_spread_bps,
            );
            return Some(scored_near_miss(
                market,
                context,
                kind,
                primary.label.to_owned(),
                Some(primary.ask_price),
                None,
                shortfall_bps,
                format!(
                    "exchange spread {}",
                    context.exchange_book_spread_bps.round_dp(2)
                ),
                format!("exchange spread is too wide for {label}"),
                0,
            ));
        }

        if raw_light_mode {
            let profile = raw_light_profile
                .unwrap_or_else(|| codex_scalp_probe_v1_raw_light_profile(context.target));
            let target_gap_abs = context.target_gap_bps.abs();
            if target_gap_abs > profile.max_target_gap_bps {
                let shortfall_bps =
                    decimal_to_bps_ceil(target_gap_abs - profile.max_target_gap_bps);
                return Some(scored_near_miss(
                    market,
                    context,
                    kind,
                    primary.label.to_owned(),
                    Some(primary.ask_price),
                    None,
                    shortfall_bps,
                    format!("gap {}", target_gap_abs.round_dp(2)),
                    format!("target gap is too extended for scalp continuation in {label}"),
                    0,
                ));
            }
            let aligned_micro_or_swing = decision
                .aligned_micro_bps
                .max(decision.aligned_swing_bps)
                .max(radar.fresh_confirmation_bps);
            let normal_lane = primary.ask_price <= profile.max_entry_price
                && decision.signal_strength_bps >= profile.min_signal_bps
                && target_gap_abs >= profile.min_target_gap_bps
                && radar.fresh_confirmation_bps >= profile.min_fresh_bps
                && decision.aligned_micro_bps >= profile.min_aligned_micro_bps
                && decision.aligned_swing_bps >= profile.min_aligned_swing_bps
                && aligned_top_bps >= profile.min_top_imbalance_bps
                && aligned_depth_bps >= profile.min_depth_imbalance_bps;
            let cheap_lottery_lane = primary.ask_price <= profile.cheap_entry_price
                && decision.signal_strength_bps >= profile.cheap_min_signal_bps
                && target_gap_abs >= profile.cheap_min_target_gap_bps
                && aligned_micro_or_swing >= profile.cheap_min_aligned_bps
                && aligned_top_bps >= Decimal::ZERO
                && aligned_depth_bps >= Decimal::ZERO;

            if normal_lane || cheap_lottery_lane {
                return Some(scored_near_miss(
                    market,
                    context,
                    kind,
                    primary.label.to_owned(),
                    Some(primary.ask_price),
                    None,
                    0,
                    "expected profit".to_owned(),
                    format!("expected profit is still too small for {label}"),
                    0,
                ));
            }

            if primary.ask_price > profile.max_entry_price {
                let shortfall_bps =
                    decimal_to_bps_ceil(primary.ask_price - profile.max_entry_price);
                return Some(scored_near_miss(
                    market,
                    context,
                    kind,
                    primary.label.to_owned(),
                    Some(primary.ask_price),
                    None,
                    shortfall_bps,
                    format!("{shortfall_bps} bps"),
                    format!("entry price is already too expensive for {label}"),
                    0,
                ));
            }

            if primary.ask_price > profile.cheap_entry_price
                && (aligned_top_bps < profile.min_top_imbalance_bps
                    || aligned_depth_bps < profile.min_depth_imbalance_bps)
            {
                return Some(scored_near_miss(
                    market,
                    context,
                    kind,
                    primary.label.to_owned(),
                    Some(primary.ask_price),
                    None,
                    decimal_to_whole_units(aligned_top_bps.max(aligned_depth_bps)),
                    format!(
                        "top {}, depth {}",
                        aligned_top_bps.round_dp(1),
                        aligned_depth_bps.round_dp(1)
                    ),
                    format!("orderbook pressure is too weak for {label}"),
                    0,
                ));
            }

            return Some(scored_near_miss(
                market,
                context,
                kind,
                primary.label.to_owned(),
                Some(primary.ask_price),
                None,
                decimal_to_whole_units(decision.signal_strength_bps),
                format!(
                    "gap {}, fresh {}, micro {}, swing {}, signal {}, top {}, depth {}",
                    target_gap_abs.round_dp(2),
                    radar.fresh_confirmation_bps.round_dp(2),
                    decision.aligned_micro_bps.round_dp(2),
                    decision.aligned_swing_bps.round_dp(2),
                    decision.signal_strength_bps.round_dp(1),
                    aligned_top_bps.round_dp(1),
                    aligned_depth_bps.round_dp(1)
                ),
                format!("signal quality is too weak for {label}"),
                0,
            ));
        }

        if radar.score_bps < self.config.codex_scalp_probe_v1_min_radar_score_bps {
            let shortfall_bps = whole_bps_shortfall(
                radar.score_bps,
                self.config.codex_scalp_probe_v1_min_radar_score_bps,
            );
            return Some(scored_near_miss(
                market,
                context,
                kind,
                primary.label.to_owned(),
                Some(primary.ask_price),
                None,
                shortfall_bps,
                format!(
                    "radar {}, top {}, depth {}",
                    radar.score_bps.round_dp(1),
                    radar.aligned_top_bps.round_dp(1),
                    radar.aligned_depth_bps.round_dp(1)
                ),
                format!("scalp radar score is too low for {label}"),
                0,
            ));
        }

        if aligned_top_bps < min_top_imbalance_bps || aligned_depth_bps < min_depth_imbalance_bps {
            return Some(scored_near_miss(
                market,
                context,
                kind,
                primary.label.to_owned(),
                Some(primary.ask_price),
                None,
                decimal_to_whole_units(aligned_top_bps.max(aligned_depth_bps)),
                format!(
                    "top {}, depth {}",
                    aligned_top_bps.round_dp(1),
                    aligned_depth_bps.round_dp(1)
                ),
                format!("orderbook pressure is too weak for {label}"),
                0,
            ));
        }

        let target_gap_ok =
            context.target_gap_bps.abs() >= min_target_gap_bps || high_pressure_override;
        let flow_ok = decision.aligned_flow_bps >= self.config.codex_scalp_probe_v1_min_flow_bps
            || high_pressure_override;
        if !target_gap_ok
            || radar.fresh_confirmation_bps < min_fresh_bps
            || decision.signal_strength_bps < self.config.codex_scalp_probe_v1_min_signal_bps
            || !flow_ok
        {
            return Some(scored_near_miss(
                market,
                context,
                kind,
                primary.label.to_owned(),
                Some(primary.ask_price),
                None,
                decimal_to_whole_units(decision.signal_strength_bps),
                format!(
                    "gap {}, fresh {}, signal {}, flow {}, radar {}",
                    context.target_gap_bps.abs().round_dp(2),
                    radar.fresh_confirmation_bps.round_dp(2),
                    decision.signal_strength_bps.round_dp(1),
                    decision.aligned_flow_bps.round_dp(1),
                    radar.score_bps.round_dp(1)
                ),
                format!("signal quality is too weak for {label}"),
                0,
            ));
        }

        Some(scored_near_miss(
            market,
            context,
            kind,
            primary.label.to_owned(),
            Some(primary.ask_price),
            None,
            0,
            "expected profit".to_owned(),
            format!("expected profit is still too small for {label}"),
            0,
        ))
    }

    #[allow(clippy::too_many_lines, clippy::too_many_arguments)]
    fn evaluate_bonereaper_state_v2_variant_near_miss(
        &self,
        market: &BinaryMarket,
        context: &BtcFiveMinuteContext,
        quotes: MarketQuotes<'_>,
        book_refs: MarketBookRefs<'_>,
        available_market_capacity: Decimal,
        trade_flow: Option<&TradeFlowSummary>,
        kind: OpportunityKind,
        enabled: bool,
    ) -> Option<ScoredNearMiss> {
        if !enabled {
            return None;
        }

        let label = kind.as_str();

        if elapsed_window_secs(context) < self.config.bonereaper_state_v2_min_elapsed_window_secs {
            let shortfall_secs = self.config.bonereaper_state_v2_min_elapsed_window_secs
                - elapsed_window_secs(context);
            return Some(scored_near_miss(
                market,
                context,
                kind,
                context.dominant_outcome.clone(),
                None,
                None,
                non_negative_u32(shortfall_secs),
                format!("{shortfall_secs}s"),
                format!("window is still too young for {label}"),
                1,
            ));
        }

        if context.seconds_left > self.config.bonereaper_state_v2_max_seconds_left {
            let shortfall_secs =
                context.seconds_left - self.config.bonereaper_state_v2_max_seconds_left;
            return Some(scored_near_miss(
                market,
                context,
                kind,
                context.dominant_outcome.clone(),
                None,
                None,
                non_negative_u32(shortfall_secs),
                format!("{shortfall_secs}s"),
                format!("it is still too early in the window for {label}"),
                1,
            ));
        }

        if context.seconds_left < self.config.bonereaper_state_v2_min_seconds_left
            && kind != OpportunityKind::CodexSentinelV1
        {
            let late_secs = self.config.bonereaper_state_v2_min_seconds_left - context.seconds_left;
            return Some(scored_near_miss(
                market,
                context,
                kind,
                context.dominant_outcome.clone(),
                None,
                None,
                non_negative_u32(late_secs),
                format!("{late_secs}s"),
                format!("it is already too late in the window for {label}"),
                1,
            ));
        }

        let target_side = primary_side_from_context(context);
        let target_metrics = bonereaper_state_v2_side_metrics(context, trade_flow, target_side);
        let counter_metrics = bonereaper_state_v2_side_metrics(context, trade_flow, !target_side);
        let preferred_side =
            if counter_metrics.signal_strength_bps > target_metrics.signal_strength_bps {
                !target_side
            } else {
                target_side
            };
        let preferred_metrics = if preferred_side == target_side {
            target_metrics
        } else {
            counter_metrics
        };
        let preferred_quote = quote_for_side(quotes, preferred_side);
        let target_gap_abs = context.target_gap_bps.abs();

        if bonereaper_state_v2_entry_notional_cap(
            available_market_capacity,
            BonereaperStateV2SignalTier::Probe,
            &self.config,
        ) <= Decimal::ZERO
        {
            return Some(scored_near_miss(
                market,
                context,
                kind,
                preferred_quote.label.to_owned(),
                Some(preferred_quote.ask_price),
                None,
                0,
                "limit".to_owned(),
                "risk limit for this market is already exhausted".to_owned(),
                4,
            ));
        }

        let max_entry_price = bonereaper_state_v2_variant_max_entry_price(kind, &self.config);
        if preferred_quote.ask_price > max_entry_price {
            let shortfall_bps = decimal_to_bps_ceil(preferred_quote.ask_price - max_entry_price);
            return Some(scored_near_miss(
                market,
                context,
                kind,
                preferred_quote.label.to_owned(),
                Some(preferred_quote.ask_price),
                None,
                shortfall_bps,
                format!("{shortfall_bps} bps"),
                format!("entry price is already too expensive for {label}"),
                0,
            ));
        }

        let preferred_book = order_book_for_side(preferred_side, book_refs);
        if kind == OpportunityKind::CodexSentinelV1
            && codex_sentinel_v1_entry_spread_guard_blocks(
                preferred_quote.ask_price,
                preferred_book,
                &self.config,
            )
        {
            let spread = codex_sentinel_v1_entry_spread(preferred_quote.ask_price, preferred_book);
            let shortfall_bps = spread.map_or(0, |spread| {
                decimal_to_bps_ceil(saturating_sub(
                    spread,
                    self.config.codex_sentinel_v1_max_entry_spread,
                ))
            });
            let distance = spread.map_or_else(
                || "missing bid".to_owned(),
                |spread| format!("spread {}", spread.round_dp(4)),
            );
            return Some(scored_near_miss(
                market,
                context,
                kind,
                preferred_quote.label.to_owned(),
                Some(preferred_quote.ask_price),
                None,
                shortfall_bps,
                distance,
                format!("entry spread is too wide for {label}"),
                0,
            ));
        }

        let lower_gap_threshold = self
            .config
            .bonereaper_state_v2_bias_min_target_gap_bps
            .min(self.config.bonereaper_state_v2_flip_max_target_gap_bps);
        let upper_gap_threshold = self
            .config
            .bonereaper_state_v2_bias_min_target_gap_bps
            .max(self.config.bonereaper_state_v2_flip_max_target_gap_bps);
        if lower_gap_threshold < upper_gap_threshold
            && target_gap_abs > lower_gap_threshold
            && target_gap_abs < upper_gap_threshold
        {
            let shortfall_bps = whole_bps_shortfall(target_gap_abs, upper_gap_threshold);
            return Some(scored_near_miss(
                market,
                context,
                kind,
                preferred_quote.label.to_owned(),
                Some(preferred_quote.ask_price),
                None,
                shortfall_bps,
                format!("{shortfall_bps} bps"),
                format!(
                    "window gap is stuck between directional and contested-flip thresholds for {label}"
                ),
                2,
            ));
        }

        if preferred_metrics.aligned_swing_bps
            < self.config.bonereaper_state_v2_min_spot_move_15s_bps
        {
            let shortfall_bps = whole_bps_shortfall(
                preferred_metrics.aligned_swing_bps,
                self.config.bonereaper_state_v2_min_spot_move_15s_bps,
            );
            return Some(scored_near_miss(
                market,
                context,
                kind,
                preferred_quote.label.to_owned(),
                Some(preferred_quote.ask_price),
                None,
                shortfall_bps,
                format!("{shortfall_bps} bps"),
                format!("15s confirmation is still too weak for {label}"),
                1,
            ));
        }

        if preferred_metrics.aligned_micro_bps
            < self.config.bonereaper_state_v2_min_spot_move_5s_bps
        {
            let shortfall_bps = whole_bps_shortfall(
                preferred_metrics.aligned_micro_bps,
                self.config.bonereaper_state_v2_min_spot_move_5s_bps,
            );
            return Some(scored_near_miss(
                market,
                context,
                kind,
                preferred_quote.label.to_owned(),
                Some(preferred_quote.ask_price),
                None,
                shortfall_bps,
                format!("{shortfall_bps} bps"),
                format!("5s confirmation is still too weak for {label}"),
                1,
            ));
        }

        if preferred_metrics.aligned_flow_bps < self.config.bonereaper_state_v2_min_aligned_flow_bps
        {
            let shortfall_bps = whole_bps_shortfall(
                preferred_metrics.aligned_flow_bps,
                self.config.bonereaper_state_v2_min_aligned_flow_bps,
            );
            return Some(scored_near_miss(
                market,
                context,
                kind,
                preferred_quote.label.to_owned(),
                Some(preferred_quote.ask_price),
                None,
                shortfall_bps,
                format!("{shortfall_bps} bps"),
                format!("aligned trade flow is still too weak for {label}"),
                1,
            ));
        }

        if preferred_metrics.signal_strength_bps
            < Decimal::from(self.config.bonereaper_state_v2_min_signal_bps)
        {
            let shortfall_bps = whole_bps_shortfall(
                preferred_metrics.signal_strength_bps,
                Decimal::from(self.config.bonereaper_state_v2_min_signal_bps),
            );
            return Some(scored_near_miss(
                market,
                context,
                kind,
                preferred_quote.label.to_owned(),
                Some(preferred_quote.ask_price),
                None,
                shortfall_bps,
                format!("{shortfall_bps} bps"),
                format!("state signal is still below threshold for {label}"),
                0,
            ));
        }

        let signal_tier = bonereaper_state_v2_signal_tier(
            context,
            preferred_metrics,
            preferred_side,
            &self.config,
        );
        let guard_decision = BonereaperStateV2Decision {
            up_side: preferred_side,
            aligned_micro_bps: preferred_metrics.aligned_micro_bps,
            aligned_swing_bps: preferred_metrics.aligned_swing_bps,
            aligned_flow_bps: preferred_metrics.aligned_flow_bps,
            signal_strength_bps: preferred_metrics.signal_strength_bps,
            signal_tier,
            counter_bias: preferred_side != target_side,
        };
        let breakout_allows = kind == OpportunityKind::CodexSentinelV1
            && codex_breakout_v1_allows(
                context,
                &guard_decision,
                preferred_quote.ask_price,
                &self.config,
            );
        let discount_value_allows = kind == OpportunityKind::CodexSentinelV1
            && codex_sentinel_v1_discount_value_lane_allows(
                context,
                &guard_decision,
                preferred_quote.ask_price,
                &self.config,
            );
        if kind == OpportunityKind::CodexSentinelV1
            && self.config.codex_breakout_v1_required
            && !(breakout_allows || discount_value_allows)
        {
            let book_age = context
                .exchange_book_age_ms
                .map_or_else(|| "missing book".to_owned(), |age| format!("{age}ms"));
            let aligned_depth_bps =
                aligned_move_bps(context.exchange_book_depth_imbalance_bps, preferred_side);
            let aligned_top_bps =
                aligned_move_bps(context.exchange_book_top_imbalance_bps, preferred_side);
            let aligned_microprice_bps =
                aligned_move_bps(context.exchange_book_microprice_bps, preferred_side);
            let breakout_score = codex_breakout_v1_score_bps(
                aligned_depth_bps,
                aligned_top_bps,
                aligned_microprice_bps,
                codex_sentinel_v1_fresh_confirmation_bps(context, &guard_decision),
                context.target_gap_bps.abs(),
            );
            return Some(scored_near_miss(
                market,
                context,
                kind,
                preferred_quote.label.to_owned(),
                Some(preferred_quote.ask_price),
                None,
                decimal_to_whole_units(breakout_score),
                book_age,
                format!(
                    "orderbook breakout or discount-value confirmation is not strong/fresh enough for {label}"
                ),
                1,
            ));
        }
        let quality_block = (kind != OpportunityKind::CodexSentinelV1)
            .then(|| {
                bonereaper_state_v2_quality_guard_block(
                    context,
                    &guard_decision,
                    preferred_quote.ask_price,
                    &self.config,
                )
            })
            .flatten();
        if let Some(block) = quality_block {
            return Some(scored_near_miss(
                market,
                context,
                kind,
                preferred_quote.label.to_owned(),
                Some(preferred_quote.ask_price),
                None,
                decimal_to_whole_units(preferred_metrics.signal_strength_bps),
                format!("{} bps", preferred_metrics.signal_strength_bps.round_dp(2)),
                format!("{} for {label}", block.reason()),
                block.priority_rank(),
            ));
        }
        if kind == OpportunityKind::CodexSentinelV1
            && context.seconds_left < self.config.bonereaper_state_v2_min_seconds_left
            && !codex_sentinel_v1_late_entry_override_allows(
                context,
                &guard_decision,
                preferred_quote.ask_price,
                &self.config,
            )
        {
            let late_secs = self.config.bonereaper_state_v2_min_seconds_left - context.seconds_left;
            return Some(scored_near_miss(
                market,
                context,
                kind,
                preferred_quote.label.to_owned(),
                Some(preferred_quote.ask_price),
                None,
                non_negative_u32(late_secs),
                format!("{late_secs}s"),
                format!("late entry needs stronger fresh momentum for {label}"),
                0,
            ));
        }
        if kind == OpportunityKind::CodexSentinelV1
            && codex_sentinel_v1_mid_signal_guard_blocks(context, &guard_decision, &self.config)
            && !(breakout_allows || discount_value_allows)
        {
            return Some(scored_near_miss(
                market,
                context,
                kind,
                preferred_quote.label.to_owned(),
                Some(preferred_quote.ask_price),
                None,
                decimal_to_whole_units(preferred_metrics.signal_strength_bps),
                format!("{} bps", preferred_metrics.signal_strength_bps.round_dp(2)),
                format!("mid-signal needs fresh confirmation for {label}"),
                0,
            ));
        }
        if kind == OpportunityKind::CodexSentinelV1
            && codex_sentinel_v1_live_quote_age_guard_blocks(context, &self.config)
        {
            let quote_age_ms = codex_sentinel_v1_live_quote_age_ms(context).unwrap_or_default();
            return Some(scored_near_miss(
                market,
                context,
                kind,
                preferred_quote.label.to_owned(),
                Some(preferred_quote.ask_price),
                None,
                u32::try_from(quote_age_ms.max(0)).unwrap_or_default(),
                format!("{quote_age_ms}ms"),
                format!("live quote is too stale for {label}"),
                1,
            ));
        }
        if kind == OpportunityKind::CodexSentinelV1
            && codex_sentinel_v1_stale_micro_guard_blocks(
                context,
                &guard_decision,
                preferred_quote.ask_price,
                &self.config,
            )
            && !(breakout_allows || discount_value_allows)
        {
            return Some(scored_near_miss(
                market,
                context,
                kind,
                preferred_quote.label.to_owned(),
                Some(preferred_quote.ask_price),
                None,
                decimal_to_whole_units(preferred_metrics.signal_strength_bps),
                format!("{} bps", preferred_metrics.signal_strength_bps.round_dp(2)),
                format!("stale 1s/5s signal needs discount or stronger flow for {label}"),
                0,
            ));
        }
        if kind == OpportunityKind::CodexSentinelV1
            && codex_sentinel_v1_quality_floor_blocks(context, &guard_decision, &self.config)
        {
            return Some(scored_near_miss(
                market,
                context,
                kind,
                preferred_quote.label.to_owned(),
                Some(preferred_quote.ask_price),
                None,
                decimal_to_whole_units(context.target_gap_bps.abs()),
                format!("{} bps", context.target_gap_bps.abs().round_dp(2)),
                format!("Codex Sentinel quality floor rejected weak gap/signal for {label}"),
                1,
            ));
        }
        if kind == OpportunityKind::CodexSentinelV1
            && codex_sentinel_v1_low_flow_guard_blocks(
                context,
                &guard_decision,
                preferred_quote.ask_price,
                &self.config,
            )
            && !(breakout_allows || discount_value_allows)
        {
            return Some(scored_near_miss(
                market,
                context,
                kind,
                preferred_quote.label.to_owned(),
                Some(preferred_quote.ask_price),
                None,
                decimal_to_whole_units(preferred_metrics.signal_strength_bps),
                format!("{} bps", preferred_metrics.signal_strength_bps.round_dp(2)),
                format!("low-flow entry needs stronger momentum quality for {label}"),
                1,
            ));
        }
        if kind == OpportunityKind::CodexSentinelV1
            && codex_sentinel_v1_mid_gap_premium_guard_blocks(
                context,
                &guard_decision,
                preferred_quote.ask_price,
                &self.config,
            )
        {
            return Some(scored_near_miss(
                market,
                context,
                kind,
                preferred_quote.label.to_owned(),
                Some(preferred_quote.ask_price),
                None,
                decimal_to_whole_units(preferred_metrics.signal_strength_bps),
                format!("{} bps", preferred_metrics.signal_strength_bps.round_dp(2)),
                format!("mid-gap premium entry needs stronger quality for {label}"),
                1,
            ));
        }
        if kind == OpportunityKind::CodexSentinelV1
            && codex_sentinel_v1_no_chase_guard_blocks(
                context,
                &guard_decision,
                preferred_quote.ask_price,
                &self.config,
            )
        {
            return Some(scored_near_miss(
                market,
                context,
                kind,
                preferred_quote.label.to_owned(),
                Some(preferred_quote.ask_price),
                None,
                decimal_to_whole_units(preferred_metrics.signal_strength_bps),
                format!("{} bps", preferred_metrics.signal_strength_bps.round_dp(2)),
                format!("early premium chase needs extreme confirmation for {label}"),
                1,
            ));
        }
        if kind == OpportunityKind::CodexSentinelV1
            && codex_sentinel_v1_late_window_value_guard_blocks(
                context,
                &guard_decision,
                preferred_quote.ask_price,
                &self.config,
            )
        {
            return Some(scored_near_miss(
                market,
                context,
                kind,
                preferred_quote.label.to_owned(),
                Some(preferred_quote.ask_price),
                None,
                decimal_to_whole_units(preferred_metrics.signal_strength_bps),
                format!("{} bps", preferred_metrics.signal_strength_bps.round_dp(2)),
                format!("late-window entry needs cheaper ask or extreme quality for {label}"),
                1,
            ));
        }
        let aggressive_continuation_allows = kind == OpportunityKind::CodexSentinelV1
            && codex_sentinel_v1_aggressive_continuation_allows(
                context,
                &guard_decision,
                preferred_quote.ask_price,
                &self.config,
            );
        if kind == OpportunityKind::CodexSentinelV1
            && codex_sentinel_v1_premium_entry_guard_blocks(
                context,
                &guard_decision,
                preferred_quote.ask_price,
                &self.config,
            )
            && !aggressive_continuation_allows
        {
            return Some(scored_near_miss(
                market,
                context,
                kind,
                preferred_quote.label.to_owned(),
                Some(preferred_quote.ask_price),
                None,
                decimal_to_whole_units(preferred_metrics.signal_strength_bps),
                format!("{} bps", preferred_metrics.signal_strength_bps.round_dp(2)),
                format!("premium entry needs stronger signal/flow/fresh confirmation for {label}"),
                0,
            ));
        }
        if kind == OpportunityKind::CodexSentinelV1
            && codex_sentinel_v1_expensive_entry_guard_blocks(
                context,
                &guard_decision,
                preferred_quote.ask_price,
                &self.config,
            )
            && !aggressive_continuation_allows
        {
            return Some(scored_near_miss(
                market,
                context,
                kind,
                preferred_quote.label.to_owned(),
                Some(preferred_quote.ask_price),
                None,
                decimal_to_whole_units(preferred_metrics.signal_strength_bps),
                format!("{} bps", preferred_metrics.signal_strength_bps.round_dp(2)),
                format!("expensive entry needs stronger fresh Binance confirmation for {label}"),
                0,
            ));
        }

        let expected_profit_floor = self.config.bonereaper_state_v2_min_expected_profit_usdc;
        if expected_profit_floor > Decimal::ZERO && preferred_quote.ask_price > Decimal::ZERO {
            let approx_notional = bonereaper_state_v2_entry_notional_cap(
                available_market_capacity,
                signal_tier,
                &self.config,
            );
            let approx_anchor_price = bonereaper_state_signal_anchor_price(
                context,
                preferred_metrics.signal_strength_bps,
                self.config.bonereaper_state_v2_max_fair_price,
                &self.config,
            );
            let approx_edge_per_share =
                approx_anchor_price - preferred_quote.ask_price - self.fee_buffer();
            if approx_notional > Decimal::ZERO && approx_edge_per_share > Decimal::ZERO {
                let approx_shares = (approx_notional / preferred_quote.ask_price).round_dp(6);
                let approx_expected_profit = (approx_shares * approx_edge_per_share).round_dp(6);
                if approx_expected_profit < expected_profit_floor {
                    let shortfall_usdc =
                        (expected_profit_floor - approx_expected_profit).round_dp(4);
                    return Some(scored_near_miss(
                        market,
                        context,
                        kind,
                        preferred_quote.label.to_owned(),
                        Some(preferred_quote.ask_price),
                        None,
                        0,
                        format!("{shortfall_usdc} usdc"),
                        format!("expected profit is still too small for {label}"),
                        0,
                    ));
                }
            }
        }

        None
    }

    #[allow(clippy::too_many_lines)]
    fn evaluate_micro_breakout_near_miss(
        &self,
        market: &BinaryMarket,
        context: &BtcFiveMinuteContext,
        quotes: MarketQuotes<'_>,
        available_market_capacity: Decimal,
        trade_flow: Option<&TradeFlowSummary>,
    ) -> Option<ScoredNearMiss> {
        if !self.config.enable_micro_breakout {
            return None;
        }

        let spot_move_abs = context.spot_move_bps.abs();
        let micro_move_abs = context.spot_move_5s_bps.abs();
        let primary = primary_quote(context, quotes);

        if micro_move_abs < self.config.micro_breakout_min_spot_move_5s_bps {
            let shortfall_bps = whole_bps_shortfall(
                micro_move_abs,
                self.config.micro_breakout_min_spot_move_5s_bps,
            );
            return Some(scored_near_miss(
                market,
                context,
                OpportunityKind::MicroBreakout,
                primary.label.to_owned(),
                Some(primary.ask_price.round_dp(6)),
                Some((quotes.up_ask_price + quotes.down_ask_price).round_dp(6)),
                shortfall_bps,
                format!("{shortfall_bps} bps"),
                "5s- Binance micro-breakout".to_owned(),
                1,
            ));
        }

        if spot_move_abs < Decimal::from(self.config.micro_breakout_min_spot_move_bps) {
            let shortfall_bps = whole_bps_shortfall(
                spot_move_abs,
                Decimal::from(self.config.micro_breakout_min_spot_move_bps),
            );
            return Some(scored_near_miss(
                market,
                context,
                OpportunityKind::MicroBreakout,
                primary.label.to_owned(),
                Some(primary.ask_price.round_dp(6)),
                Some((quotes.up_ask_price + quotes.down_ask_price).round_dp(6)),
                shortfall_bps,
                format!("{shortfall_bps} bps"),
                "Binance 5s- micro-breakout".to_owned(),
                1,
            ));
        }

        if !moves_align(context.spot_move_bps, context.spot_move_5s_bps) {
            return Some(scored_near_miss(
                market,
                context,
                OpportunityKind::MicroBreakout,
                primary.label.to_owned(),
                Some(primary.ask_price.round_dp(6)),
                Some((quotes.up_ask_price + quotes.down_ask_price).round_dp(6)),
                1,
                "alignment".to_owned(),
                "5s- Binance".to_owned(),
                1,
            ));
        }

        let fifteen_second_move_abs = context.spot_move_15s_bps.abs();
        if fifteen_second_move_abs < self.config.micro_breakout_min_spot_move_5s_bps {
            let shortfall_bps = whole_bps_shortfall(
                fifteen_second_move_abs,
                self.config.micro_breakout_min_spot_move_5s_bps,
            );
            return Some(scored_near_miss(
                market,
                context,
                OpportunityKind::MicroBreakout,
                primary.label.to_owned(),
                Some(primary.ask_price.round_dp(6)),
                Some((quotes.up_ask_price + quotes.down_ask_price).round_dp(6)),
                shortfall_bps,
                format!("{shortfall_bps} bps"),
                "15s-momentum Binance micro-breakout".to_owned(),
                1,
            ));
        }

        if !moves_align(context.spot_move_bps, context.spot_move_15s_bps) {
            return Some(scored_near_miss(
                market,
                context,
                OpportunityKind::MicroBreakout,
                primary.label.to_owned(),
                Some(primary.ask_price.round_dp(6)),
                Some((quotes.up_ask_price + quotes.down_ask_price).round_dp(6)),
                1,
                "alignment".to_owned(),
                "15s-momentum Binance".to_owned(),
                1,
            ));
        }

        let acceleration_abs = context.micro_acceleration_bps.abs();
        if acceleration_abs <= Decimal::ZERO {
            return Some(scored_near_miss(
                market,
                context,
                OpportunityKind::MicroBreakout,
                primary.label.to_owned(),
                Some(primary.ask_price.round_dp(6)),
                Some((quotes.up_ask_price + quotes.down_ask_price).round_dp(6)),
                1,
                "acceleration".to_owned(),
                "Binance".to_owned(),
                1,
            ));
        }

        if !moves_align(context.spot_move_bps, context.micro_acceleration_bps) {
            return Some(scored_near_miss(
                market,
                context,
                OpportunityKind::MicroBreakout,
                primary.label.to_owned(),
                Some(primary.ask_price.round_dp(6)),
                Some((quotes.up_ask_price + quotes.down_ask_price).round_dp(6)),
                1,
                "acceleration-alignment".to_owned(),
                "Binance".to_owned(),
                1,
            ));
        }

        let aligned_flow_bps = aligned_trade_flow_bps(context, trade_flow);
        if aligned_flow_bps <= Decimal::ZERO {
            let shortfall_bps =
                whole_bps_shortfall(aligned_flow_bps.max(Decimal::ZERO), Decimal::ONE);
            return Some(scored_near_miss(
                market,
                context,
                OpportunityKind::MicroBreakout,
                primary.label.to_owned(),
                Some(primary.ask_price.round_dp(6)),
                Some((quotes.up_ask_price + quotes.down_ask_price).round_dp(6)),
                shortfall_bps,
                format!("{shortfall_bps} bps"),
                "trade-flow Polymarket micro-breakout".to_owned(),
                1,
            ));
        }

        let signal_strength_bps = micro_breakout_signal_bps(context, trade_flow, &self.config);
        if signal_strength_bps < Decimal::from(self.config.micro_breakout_min_signal_bps) {
            let shortfall_bps = whole_bps_shortfall(
                signal_strength_bps,
                Decimal::from(self.config.micro_breakout_min_signal_bps),
            );
            return Some(scored_near_miss(
                market,
                context,
                OpportunityKind::MicroBreakout,
                primary.label.to_owned(),
                Some(primary.ask_price.round_dp(6)),
                Some((quotes.up_ask_price + quotes.down_ask_price).round_dp(6)),
                shortfall_bps,
                format!("{shortfall_bps} bps"),
                "micro-breakout".to_owned(),
                1,
            ));
        }

        if !is_valid_binary_price(primary.ask_price) {
            return None;
        }

        if primary.ask_price > self.config.micro_breakout_max_entry_price {
            let shortfall_bps =
                decimal_to_bps_ceil(primary.ask_price - self.config.micro_breakout_max_entry_price);
            return Some(scored_near_miss(
                market,
                context,
                OpportunityKind::MicroBreakout,
                primary.label.to_owned(),
                Some(primary.ask_price.round_dp(6)),
                Some((quotes.up_ask_price + quotes.down_ask_price).round_dp(6)),
                shortfall_bps,
                format!("{shortfall_bps} bps"),
                "ask micro-breakout".to_owned(),
                0,
            ));
        }

        if primary.ask_size < self.config.min_top_of_book_shares {
            let missing_shares =
                saturating_sub(self.config.min_top_of_book_shares, primary.ask_size);
            return Some(scored_near_miss(
                market,
                context,
                OpportunityKind::MicroBreakout,
                primary.label.to_owned(),
                Some(primary.ask_price.round_dp(6)),
                Some((quotes.up_ask_price + quotes.down_ask_price).round_dp(6)),
                decimal_to_whole_units(missing_shares),
                format!("{} shares", missing_shares.round_dp(2)),
                "micro-breakout".to_owned(),
                3,
            ));
        }

        let signal_anchor_price =
            directional_signal_anchor_price(signal_strength_bps, &self.config);
        let execution_slippage =
            Decimal::from(self.config.directional_execution_slippage_bps) / bps_denominator();
        let execution_anchor_price = (signal_anchor_price + execution_slippage).min(Decimal::ONE);
        let edge_per_share = execution_anchor_price - primary.ask_price - self.fee_buffer();
        if edge_per_share <= Decimal::ZERO {
            let shortfall_bps =
                decimal_to_bps_ceil(primary.ask_price + self.fee_buffer() - execution_anchor_price);
            return Some(scored_near_miss(
                market,
                context,
                OpportunityKind::MicroBreakout,
                primary.label.to_owned(),
                Some(primary.ask_price.round_dp(6)),
                Some((quotes.up_ask_price + quotes.down_ask_price).round_dp(6)),
                shortfall_bps,
                format!("{shortfall_bps} bps"),
                "payout micro-breakout ask".to_owned(),
                0,
            ));
        }

        let signal_tier = micro_breakout_signal_tier(context, trade_flow, &self.config);
        let positive_acceleration_confirmed = has_aligned_positive_acceleration(context);
        let max_notional = micro_breakout_entry_notional_cap(
            available_market_capacity,
            primary.ask_price,
            signal_tier,
            micro_breakout_full_size_allowed(
                primary.ask_price,
                micro_move_abs,
                aligned_flow_bps,
                positive_acceleration_confirmed,
                signal_tier,
                &self.config,
            ),
            &self.config,
        );
        if max_notional <= Decimal::ZERO {
            return Some(scored_near_miss(
                market,
                context,
                OpportunityKind::MicroBreakout,
                primary.label.to_owned(),
                Some(primary.ask_price.round_dp(6)),
                Some((quotes.up_ask_price + quotes.down_ask_price).round_dp(6)),
                0,
                "risk-limit".to_owned(),
                "micro-breakout market risk limit reached".to_owned(),
                4,
            ));
        }

        let affordable_shares = max_notional / primary.ask_price;
        let tradable_shares = primary.ask_size.min(affordable_shares).round_dp(4);
        if tradable_shares < self.config.min_top_of_book_shares {
            let missing_shares =
                saturating_sub(self.config.min_top_of_book_shares, tradable_shares);
            return Some(scored_near_miss(
                market,
                context,
                OpportunityKind::MicroBreakout,
                primary.label.to_owned(),
                Some(primary.ask_price.round_dp(6)),
                Some((quotes.up_ask_price + quotes.down_ask_price).round_dp(6)),
                decimal_to_whole_units(missing_shares),
                format!("{} shares", missing_shares.round_dp(2)),
                "notional micro-breakout".to_owned(),
                3,
            ));
        }

        None
    }

    fn available_market_capacity(
        &self,
        market_notional: &HashMap<String, Decimal>,
        market: &BinaryMarket,
    ) -> Decimal {
        let already_allocated = *market_notional
            .get(&market.condition_id)
            .unwrap_or(&Decimal::ZERO);
        saturating_sub(self.config.max_market_notional_usdc, already_allocated)
    }

    fn fee_buffer(&self) -> Decimal {
        Decimal::from(self.config.assumed_fee_bps) / bps_denominator()
    }

    fn directional_tail_hedge_ratio(
        &self,
        context: &BtcFiveMinuteContext,
        primary: PrimaryQuote<'_>,
        hedge: PrimaryQuote<'_>,
        signal_strength_bps: Decimal,
        velocity_bps_per_minute: Decimal,
    ) -> Decimal {
        if !self.config.enable_tail_hedge
            || self.config.tail_hedge_ratio <= Decimal::ZERO
            || self.config.tail_hedge_open_window_secs == 0
        {
            return Decimal::ZERO;
        }

        let spot_move_abs = context.spot_move_bps.abs();
        if spot_move_abs < Decimal::from(self.config.tail_hedge_min_spot_move_bps)
            || signal_strength_bps < Decimal::from(self.config.tail_hedge_min_signal_bps)
            || velocity_bps_per_minute
                < Decimal::from(self.config.tail_hedge_min_velocity_bps_per_minute)
        {
            return Decimal::ZERO;
        }

        let elapsed_window_secs = (300_i64 - context.seconds_left).max(0);
        if elapsed_window_secs > self.config.tail_hedge_open_window_secs {
            return Decimal::ZERO;
        }

        if primary.label == hedge.label
            || !is_valid_binary_price(hedge.ask_price)
            || hedge.ask_price > self.config.tail_hedge_max_opposite_price
            || primary.ask_price + hedge.ask_price > self.config.tail_hedge_max_bundle_cost
        {
            return Decimal::ZERO;
        }

        self.config.tail_hedge_ratio
    }

    fn has_time_to_expiry(&self, market: &BinaryMarket) -> bool {
        market.end_date.is_none_or(|end_date| {
            end_date.signed_duration_since(Utc::now()).num_minutes()
                >= self.config.min_minutes_to_expiry
        })
    }

    /*
    fn strategy_enables_opening_tail_hedge_for_directional_signal() {
        let strategy = BundleArbitrageStrategy::new(strategy_config());
        let market = market();

        let mut books = HashMap::new();
        books.insert(
            "up-token".to_owned(),
            OrderBook {
                asset_id: "up-token".to_owned(),
                bids: Vec::new(),
                asks: vec![BookLevel {
                    price: decimal("0.56"),
                    size: decimal("100"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );
        books.insert(
            "down-token".to_owned(),
            OrderBook {
                asset_id: "down-token".to_owned(),
                bids: Vec::new(),
                asks: vec![BookLevel {
                    price: decimal("0.22"),
                    size: decimal("40"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );

        let mut contexts = HashMap::new();
        contexts.insert(
            market.slug.clone(),
            BtcFiveMinuteContext {
                target: MarketTarget::Btc5m,
                interval_open_price: decimal("67000"),
                current_spot_price: decimal("67210"),
                current_spot_source: "test-fixture".to_owned(),
                current_spot_event_age_ms: None,
                current_spot_received_age_ms: None,
                current_spot_quote_points: None,
                exchange_book_age_ms: None,
                exchange_book_top_imbalance_bps: Decimal::ZERO,
                exchange_book_depth_imbalance_bps: Decimal::ZERO,
                exchange_book_microprice_bps: Decimal::ZERO,
                exchange_book_spread_bps: Decimal::ZERO,
                micro_reference_price: decimal("67210"),
                spot_move_bps: decimal("31.34"),
                spot_move_5s_bps: Decimal::ZERO,
                dominant_outcome: "Up".to_owned(),
                seconds_left: 295,
            },
        );

        let opportunities =
            strategy.find_opportunities(&[market], &books, &HashMap::new(), &contexts);
        assert_eq!(opportunities.len(), 1);
        assert_eq!(
            opportunities[0].kind,
            OpportunityKind::DirectionalMomentumHedged
        );
        assert_eq!(opportunities[0].primary_outcome_label, "Up");
        assert_eq!(opportunities[0].hedge_outcome_label.as_deref(), Some("Down"));
        assert!(opportunities[0].hedge_shares > Decimal::ZERO);
    }
    */
}

fn compare_opportunities(left: &Opportunity, right: &Opportunity) -> Ordering {
    left.edge_bps
        .cmp(&right.edge_bps)
        .then_with(|| left.expected_profit.cmp(&right.expected_profit))
        .then_with(|| opportunity_kind_rank(left.kind).cmp(&opportunity_kind_rank(right.kind)))
}

fn quote_for_side(quotes: MarketQuotes<'_>, up_side: bool) -> PrimaryQuote<'_> {
    if up_side {
        PrimaryQuote {
            label: "Up",
            token_id: quotes.up_token,
            ask_price: quotes.up_ask_price,
            ask_size: quotes.up_ask_size,
        }
    } else {
        PrimaryQuote {
            label: "Down",
            token_id: quotes.down_token,
            ask_price: quotes.down_ask_price,
            ask_size: quotes.down_ask_size,
        }
    }
}

fn primary_quote<'a>(context: &BtcFiveMinuteContext, quotes: MarketQuotes<'a>) -> PrimaryQuote<'a> {
    quote_for_side(quotes, is_up_outcome(context))
}

fn secondary_quote<'a>(
    context: &BtcFiveMinuteContext,
    quotes: MarketQuotes<'a>,
) -> PrimaryQuote<'a> {
    quote_for_side(quotes, !is_up_outcome(context))
}

fn opportunity_kind_rank(kind: OpportunityKind) -> u8 {
    match kind {
        OpportunityKind::BundleArbitrage => 5,
        OpportunityKind::BonereaperStateV2
        | OpportunityKind::BonereaperStateGuarded
        | OpportunityKind::CodexSentinelV1
        | OpportunityKind::CodexScalpProbeV1 => 4,
        OpportunityKind::BonereaperStateV1 => 3,
        OpportunityKind::TargetStateV1 => 2,
        OpportunityKind::MicroBreakout | OpportunityKind::DirectionalMomentumHedged => 1,
        OpportunityKind::DirectionalMomentum => 0,
    }
}

fn compare_near_misses(left: &ScoredNearMiss, right: &ScoredNearMiss) -> Ordering {
    left.priority_rank
        .cmp(&right.priority_rank)
        .then_with(|| {
            left.near_miss
                .shortfall_bps
                .cmp(&right.near_miss.shortfall_bps)
        })
        .then_with(|| {
            left.near_miss
                .seconds_left
                .cmp(&right.near_miss.seconds_left)
        })
}

#[allow(clippy::too_many_arguments)]
fn scored_near_miss(
    market: &BinaryMarket,
    context: &BtcFiveMinuteContext,
    kind: OpportunityKind,
    primary_outcome_label: String,
    primary_outcome_ask_price: Option<Decimal>,
    bundle_cost: Option<Decimal>,
    shortfall_bps: u32,
    shortfall_label: String,
    reason: String,
    priority_rank: u8,
) -> ScoredNearMiss {
    let clean_shortfall_label = sanitize_legacy_mojibake(&shortfall_label);
    let clean_reason = clean_near_miss_reason(kind, &reason, &clean_shortfall_label);
    let clean_primary_label =
        clean_strategy_outcome_label(&primary_outcome_label, aligned_outcome_label(context));

    ScoredNearMiss {
        near_miss: NearMiss {
            kind,
            slug: market.slug.clone(),
            question: market.question.clone(),
            dominant_outcome: aligned_outcome_label(context).to_owned(),
            primary_outcome_label: clean_primary_label,
            primary_outcome_ask_price,
            bundle_cost,
            target_gap_bps: context.target_gap_bps,
            spot_move_bps: context.spot_move_bps,
            spot_move_1s_bps: context.spot_move_1s_bps,
            spot_move_5s_bps: context.spot_move_5s_bps,
            spot_move_15s_bps: context.spot_move_15s_bps,
            micro_acceleration_bps: context.micro_acceleration_bps,
            exchange_book_age_ms: context.exchange_book_age_ms,
            exchange_book_top_imbalance_bps: context.exchange_book_top_imbalance_bps,
            exchange_book_depth_imbalance_bps: context.exchange_book_depth_imbalance_bps,
            seconds_left: context.seconds_left,
            shortfall_bps,
            shortfall_label: clean_shortfall_label,
            reason: clean_reason,
        },
        priority_rank,
    }
}

fn sanitize_opportunity_diagnostics(opportunity: &mut Opportunity) {
    let inferred_outcome = if opportunity.current_spot_price >= opportunity.target_price {
        "Up"
    } else {
        "Down"
    };

    opportunity.dominant_outcome =
        clean_strategy_outcome_label(&opportunity.dominant_outcome, inferred_outcome);
    opportunity.primary_outcome_label =
        clean_strategy_outcome_label(&opportunity.primary_outcome_label, inferred_outcome);
    opportunity.outcome_a_label = clean_strategy_outcome_label(&opportunity.outcome_a_label, "Up");
    opportunity.outcome_b_label =
        clean_strategy_outcome_label(&opportunity.outcome_b_label, "Down");
    if let Some(label) = &mut opportunity.hedge_outcome_label {
        *label = clean_strategy_outcome_label(label, "Down");
    }

    if contains_legacy_mojibake(&opportunity.note) {
        opportunity.note = format!(
            "{} signal: selected {} at ask {}; spot move {} bps; 5s {} bps; 15s {} bps; target gap {} bps; signal {} bps; size {} USDC.",
            opportunity.kind.as_str(),
            opportunity.primary_outcome_label,
            opportunity.primary_outcome_ask_price.round_dp(4),
            opportunity.spot_move_bps.round_dp(2),
            opportunity.spot_move_5s_bps.round_dp(2),
            opportunity.spot_move_15s_bps.round_dp(2),
            opportunity.target_gap_bps.round_dp(2),
            opportunity.signal_strength_bps.round_dp(2),
            opportunity.required_usdc.round_dp(4),
        );
    } else {
        opportunity.note = sanitize_legacy_mojibake(&opportunity.note);
    }
}

fn clean_strategy_outcome_label(label: &str, fallback: &str) -> String {
    let cleaned = sanitize_legacy_mojibake(label);
    if contains_legacy_mojibake(label) || cleaned == "[encoding-corrupt-text]" {
        fallback.to_owned()
    } else {
        cleaned
    }
}

fn clean_near_miss_reason(
    kind: OpportunityKind,
    reason: &str,
    clean_shortfall_label: &str,
) -> String {
    let cleaned = sanitize_legacy_mojibake(reason);
    if contains_legacy_mojibake(reason) && cleaned == "[encoding-corrupt-text]" {
        format!(
            "{} guard rejected setup; shortfall {}",
            kind.as_str(),
            clean_shortfall_label
        )
    } else {
        cleaned
    }
}

fn bps_denominator() -> Decimal {
    Decimal::from(10_000_u32)
}

fn decimal_to_bps(value: Decimal) -> u32 {
    (value * bps_denominator()).floor().to_u32().unwrap_or(0)
}

fn decimal_to_bps_ceil(value: Decimal) -> u32 {
    (value.max(Decimal::ZERO) * bps_denominator())
        .ceil()
        .to_u32()
        .unwrap_or(u32::MAX)
}

fn decimal_to_whole_units(value: Decimal) -> u32 {
    value.max(Decimal::ZERO).ceil().to_u32().unwrap_or(u32::MAX)
}

fn whole_bps_shortfall(actual: Decimal, threshold: Decimal) -> u32 {
    saturating_sub(threshold, actual)
        .ceil()
        .to_u32()
        .unwrap_or(0)
}

fn saturating_sub(left: Decimal, right: Decimal) -> Decimal {
    let difference = left - right;
    if difference.is_sign_negative() {
        Decimal::ZERO
    } else {
        difference
    }
}

fn primary_order_book<'a>(primary: PrimaryQuote<'a>, books: MarketBookRefs<'a>) -> &'a OrderBook {
    if primary.token_id == books.up.asset_id {
        books.up
    } else {
        books.down
    }
}

fn order_book_for_side(up_side: bool, books: MarketBookRefs<'_>) -> &OrderBook {
    if up_side { books.up } else { books.down }
}

fn build_buy_fill_plan_for_notional(
    book: &OrderBook,
    max_notional: Decimal,
    max_average_price: Decimal,
) -> Option<BookFillPlan> {
    if max_notional <= Decimal::ZERO {
        return None;
    }

    let max_price = max_average_price.max(Decimal::ZERO);
    let mut remaining_notional = max_notional;
    let mut total_shares = Decimal::ZERO;
    let mut total_cost = Decimal::ZERO;
    let mut levels = Vec::with_capacity(MAX_BOOK_SWEEP_LEVELS);

    for level in book.asks.iter().rev().take(MAX_BOOK_SWEEP_LEVELS) {
        if remaining_notional <= Decimal::ZERO
            || level.size <= Decimal::ZERO
            || !is_valid_binary_price(level.price)
        {
            break;
        }

        let allowed_shares = if level.price > max_price {
            max_shares_with_average_cap(total_shares, total_cost, level.price, max_price)
        } else {
            Decimal::MAX
        };
        if allowed_shares <= Decimal::ZERO {
            break;
        }

        let fill_shares = level
            .size
            .min(remaining_notional / level.price)
            .min(allowed_shares)
            .round_dp(6);
        if fill_shares <= Decimal::ZERO {
            continue;
        }

        let fill_cost = (fill_shares * level.price).round_dp(6);
        total_shares = (total_shares + fill_shares).round_dp(6);
        total_cost = (total_cost + fill_cost).round_dp(6);
        remaining_notional = (remaining_notional - fill_cost).max(Decimal::ZERO);
        levels.push(BookFillLevel {
            price: level.price.round_dp(6),
            shares: fill_shares,
        });
    }

    finalize_fill_plan(levels, total_shares, total_cost)
}

fn build_buy_fill_plan_for_shares(
    book: &OrderBook,
    target_shares: Decimal,
    max_average_price: Decimal,
) -> Option<BookFillPlan> {
    if target_shares <= Decimal::ZERO {
        return None;
    }

    let max_price = max_average_price.max(Decimal::ZERO);
    let mut remaining_shares = target_shares;
    let mut total_shares = Decimal::ZERO;
    let mut total_cost = Decimal::ZERO;
    let mut levels = Vec::with_capacity(MAX_BOOK_SWEEP_LEVELS);

    for level in book.asks.iter().rev().take(MAX_BOOK_SWEEP_LEVELS) {
        if remaining_shares <= Decimal::ZERO
            || level.size <= Decimal::ZERO
            || !is_valid_binary_price(level.price)
        {
            break;
        }

        let allowed_shares = if level.price > max_price {
            max_shares_with_average_cap(total_shares, total_cost, level.price, max_price)
        } else {
            Decimal::MAX
        };
        if allowed_shares <= Decimal::ZERO {
            break;
        }

        let fill_shares = level
            .size
            .min(remaining_shares)
            .min(allowed_shares)
            .round_dp(6);
        if fill_shares <= Decimal::ZERO {
            continue;
        }

        let fill_cost = (fill_shares * level.price).round_dp(6);
        total_shares = (total_shares + fill_shares).round_dp(6);
        total_cost = (total_cost + fill_cost).round_dp(6);
        remaining_shares = (remaining_shares - fill_shares).max(Decimal::ZERO);
        levels.push(BookFillLevel {
            price: level.price.round_dp(6),
            shares: fill_shares,
        });
    }

    finalize_fill_plan(levels, total_shares, total_cost)
}

fn finalize_fill_plan(
    levels: Vec<BookFillLevel>,
    total_shares: Decimal,
    total_cost: Decimal,
) -> Option<BookFillPlan> {
    if total_shares <= Decimal::ZERO || total_cost <= Decimal::ZERO {
        return None;
    }

    Some(BookFillPlan {
        average_price: (total_cost / total_shares).round_dp(6),
        total_shares: total_shares.round_dp(6),
        total_cost: total_cost.round_dp(6),
        levels,
    })
}

fn max_shares_with_average_cap(
    current_shares: Decimal,
    current_cost: Decimal,
    next_price: Decimal,
    max_average_price: Decimal,
) -> Decimal {
    if next_price <= max_average_price {
        return Decimal::MAX;
    }

    let numerator = (max_average_price * current_shares) - current_cost;
    let denominator = next_price - max_average_price;
    if numerator <= Decimal::ZERO || denominator <= Decimal::ZERO {
        Decimal::ZERO
    } else {
        (numerator / denominator).round_dp(6)
    }
}

fn sweep_average_price_limit(reference_price: Decimal, ceiling_price: Decimal) -> Decimal {
    (reference_price + Decimal::new(3, 2))
        .min(ceiling_price)
        .round_dp(6)
}

fn is_valid_binary_price(price: Decimal) -> bool {
    price > Decimal::ZERO && price < Decimal::ONE
}

fn is_up_outcome(context: &BtcFiveMinuteContext) -> bool {
    context.current_spot_price >= context.target_price
}

fn primary_side_from_context(context: &BtcFiveMinuteContext) -> bool {
    is_up_outcome(context)
}

fn non_negative_u32(value: i64) -> u32 {
    u32::try_from(value.max(0)).unwrap_or(u32::MAX)
}

fn aligned_move_bps(move_bps: Decimal, up_side: bool) -> Decimal {
    if up_side { move_bps } else { -move_bps }
}

fn aligned_outcome_label(context: &BtcFiveMinuteContext) -> &'static str {
    if is_up_outcome(context) { "Up" } else { "Down" }
}

fn directional_signal_anchor_price(
    signal_strength_bps: Decimal,
    config: &StrategyConfig,
) -> Decimal {
    let confidence_gain = (signal_strength_bps
        * Decimal::from(config.directional_confidence_bps_per_spot_bps)
        / bps_denominator())
    .round_dp(6);
    (Decimal::new(50, 2) + confidence_gain).min(config.directional_max_fair_price)
}

fn directional_velocity_bps_per_minute(context: &BtcFiveMinuteContext) -> Decimal {
    let elapsed_window_secs = elapsed_window_secs(context);
    if elapsed_window_secs <= 0 {
        return Decimal::ZERO;
    }

    (context.spot_move_bps.abs() * Decimal::from(60_i64) / Decimal::from(elapsed_window_secs))
        .round_dp(4)
}

fn directional_effective_signal_bps(
    context: &BtcFiveMinuteContext,
    trade_flow: Option<&TradeFlowSummary>,
    config: &StrategyConfig,
) -> Decimal {
    let aligned_outcome_label = aligned_outcome_label(context);
    let spot_move_abs = context.spot_move_bps.abs();
    let elapsed_window_secs = elapsed_window_secs(context);
    let projected_final_move = if elapsed_window_secs <= 0 {
        spot_move_abs
    } else {
        (spot_move_abs * Decimal::from(context.target.window_secs())
            / Decimal::from(elapsed_window_secs))
        .round_dp(6)
    };
    let capped_projection =
        (spot_move_abs * config.directional_projection_cap_multiplier).round_dp(6);
    let projected_signal = projected_final_move
        .max(spot_move_abs)
        .min(capped_projection);
    let flow_adjustment = trade_flow.map_or(Decimal::ZERO, |flow| {
        (flow.aligned_imbalance_bps(aligned_outcome_label) * config.directional_trade_flow_weight)
            .round_dp(6)
    });
    let micro_adjustment = directional_micro_signal_adjustment(context, config);

    (projected_signal + flow_adjustment + micro_adjustment).max(Decimal::ZERO)
}

fn directional_entry_notional_cap(
    available_market_capacity: Decimal,
    signal_strength_bps: Decimal,
    strong_entry_signal: bool,
    config: &StrategyConfig,
) -> Decimal {
    let full_cap = config
        .max_directional_notional_usdc
        .min(available_market_capacity);
    if full_cap <= Decimal::ZERO {
        return Decimal::ZERO;
    }

    if strong_entry_signal {
        return full_cap;
    }

    let soft_min = config
        .directional_soft_entry_min_notional_usdc
        .min(config.directional_soft_entry_max_notional_usdc)
        .min(full_cap);
    let soft_max = config
        .directional_soft_entry_max_notional_usdc
        .max(config.directional_soft_entry_min_notional_usdc)
        .min(full_cap)
        .max(soft_min);

    let min_signal = Decimal::from(config.directional_min_signal_bps);
    let signal_excess = (signal_strength_bps - min_signal).max(Decimal::ZERO);
    let signal_window = if config.directional_soft_entry_signal_window_bps > Decimal::ZERO {
        config.directional_soft_entry_signal_window_bps
    } else {
        Decimal::ONE
    };
    let signal_progress = (signal_excess / signal_window).min(Decimal::ONE);
    (soft_min + (soft_max - soft_min) * signal_progress).round_dp(6)
}

fn directional_is_strong_entry_signal(
    context: &BtcFiveMinuteContext,
    trade_flow: Option<&TradeFlowSummary>,
    config: &StrategyConfig,
) -> bool {
    if !moves_align(context.spot_move_bps, context.spot_move_5s_bps) {
        return false;
    }

    if bps_fixed_abs(context.spot_move_5s_bps)
        < bps_threshold_to_fixed_abs(config.directional_strong_signal_min_spot_move_5s_bps)
    {
        return false;
    }

    if config.directional_micro_burst_weight > Decimal::ZERO
        && !has_aligned_micro_burst(context, config)
    {
        return false;
    }

    trade_flow.map_or(Decimal::ZERO, |flow| {
        flow.aligned_imbalance_bps(aligned_outcome_label(context))
    }) >= config.directional_strong_signal_min_trade_flow_bps
}

fn target_state_entry_notional_cap(
    available_market_capacity: Decimal,
    signal_tier: TargetStateSignalTier,
    config: &StrategyConfig,
) -> Decimal {
    let tier_cap = match signal_tier {
        TargetStateSignalTier::Normal => config.target_state_normal_notional_usdc,
        TargetStateSignalTier::Strong => config.target_state_strong_notional_usdc,
    };
    tier_cap.min(available_market_capacity).round_dp(6)
}

fn target_state_signal_tier(
    context: &BtcFiveMinuteContext,
    trade_flow: Option<&TradeFlowSummary>,
    config: &StrategyConfig,
) -> TargetStateSignalTier {
    let target_cross = recent_target_cross(context, config);
    let target_gap_abs = context.target_gap_bps.abs();
    let aligned_flow_bps = aligned_trade_flow_bps(context, trade_flow);
    let aligned_one_second = moves_align(context.target_gap_bps, context.spot_move_1s_bps);

    if target_gap_abs >= config.target_state_strong_gap_bps
        && aligned_flow_bps >= config.target_state_min_aligned_flow_bps
        && (target_cross.is_active() || aligned_one_second)
    {
        TargetStateSignalTier::Strong
    } else {
        TargetStateSignalTier::Normal
    }
}

fn target_state_signal_bps(
    context: &BtcFiveMinuteContext,
    trade_flow: Option<&TradeFlowSummary>,
    config: &StrategyConfig,
) -> Decimal {
    let target_gap_abs = context.target_gap_bps.abs();
    let persistence_bonus = (context.spot_move_15s_bps.abs() * Decimal::new(60, 2)).round_dp(6);
    let micro_bonus = (context.spot_move_5s_bps.abs() * Decimal::new(35, 2)).round_dp(6);
    let flow_bonus = (aligned_trade_flow_bps(context, trade_flow)
        * config.directional_trade_flow_weight)
        .round_dp(6);
    let acceleration_bonus = if has_aligned_positive_acceleration(context)
        && moves_align(context.target_gap_bps, context.micro_acceleration_bps)
    {
        context.micro_acceleration_bps.abs().min(Decimal::new(2, 0))
    } else {
        Decimal::ZERO
    };
    let cross_bonus = if recent_target_cross(context, config).is_active() {
        config
            .micro_breakout_target_cross_signal_boost_bps
            .max(Decimal::ONE)
    } else {
        Decimal::ZERO
    };
    let time_bonus =
        (Decimal::from(elapsed_window_secs(context)) / Decimal::from(200_i64)).round_dp(6);

    (target_gap_abs
        + persistence_bonus
        + micro_bonus
        + flow_bonus.max(Decimal::ZERO)
        + acceleration_bonus
        + cross_bonus
        + time_bonus)
        .round_dp(6)
}

fn target_state_signal_anchor_price(
    context: &BtcFiveMinuteContext,
    signal_strength_bps: Decimal,
    config: &StrategyConfig,
) -> Decimal {
    let elapsed_ratio = Decimal::from(elapsed_window_secs(context))
        / Decimal::from(context.target.window_secs().max(1));
    let time_confidence_bonus = (elapsed_ratio * Decimal::new(2, 2)).round_dp(6);
    (directional_signal_anchor_price(signal_strength_bps, config) + time_confidence_bonus)
        .min(config.directional_max_fair_price)
        .round_dp(6)
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum TargetStateSignalTier {
    Normal,
    Strong,
}

impl TargetStateSignalTier {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Strong => "strong",
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum BonereaperStateSignalTier {
    Normal,
    Strong,
}

impl BonereaperStateSignalTier {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Strong => "strong",
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum BonereaperStateV2SignalTier {
    Probe,
    Normal,
    Strong,
}

impl BonereaperStateV2SignalTier {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Probe => "probe",
            Self::Normal => "normal",
            Self::Strong => "strong",
        }
    }
}

#[allow(clippy::struct_field_names)]
#[derive(Debug, Clone, Copy)]
struct BonereaperStateV2SideMetrics {
    aligned_micro_bps: Decimal,
    aligned_swing_bps: Decimal,
    aligned_flow_bps: Decimal,
    signal_strength_bps: Decimal,
}

#[derive(Debug, Clone, Copy)]
struct BonereaperStateV2Decision {
    up_side: bool,
    aligned_micro_bps: Decimal,
    aligned_swing_bps: Decimal,
    aligned_flow_bps: Decimal,
    signal_strength_bps: Decimal,
    signal_tier: BonereaperStateV2SignalTier,
    counter_bias: bool,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum BonereaperStateV2QualityBlock {
    CounterMicro,
    EarlyWindow,
    EarlyExpensive,
    LowGap,
    HighGap,
    MidGap,
}

impl BonereaperStateV2QualityBlock {
    const fn reason(self) -> &'static str {
        match self {
            Self::CounterMicro => "fresh 1s/5s momentum is against the selected side",
            Self::EarlyWindow => "early-window entry needs stronger fresh confirmation",
            Self::EarlyExpensive => "early expensive entry needs extreme confirmation",
            Self::LowGap => "low-gap entry needs deeper value or extreme confirmation",
            Self::HighGap => "high-gap chase needs discount and stronger confirmation",
            Self::MidGap => "mid-gap entry needs discount, flow, and fresh confirmation",
        }
    }

    const fn priority_rank(self) -> u8 {
        match self {
            Self::CounterMicro => 0,
            Self::EarlyWindow
            | Self::EarlyExpensive
            | Self::LowGap
            | Self::HighGap
            | Self::MidGap => 1,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum MicroBreakoutSignalTier {
    Weak,
    Normal,
    Strong,
}

impl MicroBreakoutSignalTier {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Weak => "weak",
            Self::Normal => "normal",
            Self::Strong => "strong",
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum RecentTargetCross {
    None,
    OneSecond,
    FiveSecond,
}

impl RecentTargetCross {
    const fn is_active(self) -> bool {
        !matches!(self, Self::None)
    }

    const fn label(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::OneSecond => "1s",
            Self::FiveSecond => "5s",
        }
    }
}

fn micro_breakout_signal_tier(
    context: &BtcFiveMinuteContext,
    trade_flow: Option<&TradeFlowSummary>,
    config: &StrategyConfig,
) -> MicroBreakoutSignalTier {
    let target_cross = recent_target_cross(context, config);
    let burst_feature_enabled = micro_burst_feature_enabled(config);
    let five_second_move_fixed_abs = bps_fixed_abs(context.spot_move_5s_bps);
    let one_second_move_fixed_abs = bps_fixed_abs(context.spot_move_1s_bps);
    let fifteen_second_move_fixed_abs = bps_fixed_abs(context.spot_move_15s_bps);
    let strong_five_second_fixed_abs =
        bps_threshold_to_fixed_abs(config.micro_breakout_strong_signal_min_spot_move_5s_bps);
    let strong_one_second_fixed_abs =
        bps_threshold_to_fixed_abs(config.micro_breakout_strong_signal_min_spot_move_1s_bps);
    let strong_fifteen_second_fixed_abs =
        bps_threshold_to_fixed_abs(config.micro_breakout_strong_signal_min_spot_move_15s_bps);
    let fifteen_seconds_confirm = has_aligned_fifteen_second_momentum(context, config);
    let aligned_flow_bps = aligned_trade_flow_bps(context, trade_flow);
    let acceleration_confirmed = has_aligned_positive_acceleration(context);
    let burst_confirmed = if burst_feature_enabled {
        has_aligned_micro_burst(context, config)
    } else {
        true
    };
    let strong_burst_enabled =
        config.micro_breakout_strong_signal_min_spot_move_1s_bps > Decimal::ZERO;
    let strong_burst_confirmed = strong_burst_enabled
        && one_second_move_fixed_abs >= strong_one_second_fixed_abs
        && moves_align(context.spot_move_bps, context.spot_move_1s_bps)
        && has_persistent_micro_burst(context, config);
    let quality_confirmed = fifteen_seconds_confirm
        && acceleration_confirmed
        && aligned_flow_bps > Decimal::ZERO
        && (burst_confirmed || target_cross.is_active());

    if quality_confirmed
        && five_second_move_fixed_abs >= strong_five_second_fixed_abs
        && (!strong_burst_enabled
            || strong_burst_confirmed
            || target_cross == RecentTargetCross::OneSecond)
        && fifteen_second_move_fixed_abs >= strong_fifteen_second_fixed_abs
        && aligned_flow_bps >= config.directional_strong_signal_min_trade_flow_bps
    {
        MicroBreakoutSignalTier::Strong
    } else if quality_confirmed
        && (five_second_move_fixed_abs >= strong_five_second_fixed_abs
            || fifteen_second_move_fixed_abs >= strong_fifteen_second_fixed_abs
            || strong_burst_confirmed
            || target_cross.is_active())
    {
        MicroBreakoutSignalTier::Normal
    } else {
        MicroBreakoutSignalTier::Weak
    }
}

fn bonereaper_state_entry_notional_cap(
    available_market_capacity: Decimal,
    signal_tier: BonereaperStateSignalTier,
    config: &StrategyConfig,
) -> Decimal {
    let tier_cap = match signal_tier {
        BonereaperStateSignalTier::Normal => config.bonereaper_state_normal_notional_usdc,
        BonereaperStateSignalTier::Strong => config.bonereaper_state_strong_notional_usdc,
    };
    tier_cap.min(available_market_capacity).round_dp(6)
}

fn bonereaper_state_signal_tier(
    context: &BtcFiveMinuteContext,
    trade_flow: Option<&TradeFlowSummary>,
    config: &StrategyConfig,
) -> BonereaperStateSignalTier {
    let aligned_flow_bps = aligned_trade_flow_bps(context, trade_flow);
    let aligned_micro_bps =
        aligned_move_bps(context.spot_move_5s_bps, primary_side_from_context(context));
    let target_cross = recent_target_cross(context, config);

    if context.target_gap_bps.abs() >= config.bonereaper_state_strong_gap_bps
        && aligned_flow_bps >= config.bonereaper_state_strong_flow_bps
        && aligned_micro_bps >= config.bonereaper_state_min_spot_move_5s_bps
        && (target_cross.is_active() || has_aligned_positive_acceleration(context))
    {
        BonereaperStateSignalTier::Strong
    } else {
        BonereaperStateSignalTier::Normal
    }
}

fn bonereaper_state_signal_bps(
    context: &BtcFiveMinuteContext,
    trade_flow: Option<&TradeFlowSummary>,
    config: &StrategyConfig,
) -> Decimal {
    let up_side = primary_side_from_context(context);
    let target_gap_abs = context.target_gap_bps.abs();
    let aligned_micro_bps = aligned_move_bps(context.spot_move_5s_bps, up_side);
    let aligned_swing_bps = aligned_move_bps(context.spot_move_15s_bps, up_side);
    let aligned_burst_bps = aligned_move_bps(context.spot_move_1s_bps, up_side);
    let aligned_flow_bps = aligned_trade_flow_bps(context, trade_flow).max(Decimal::ZERO);
    let acceleration_bonus = if has_aligned_positive_acceleration(context) {
        context.micro_acceleration_bps.abs().min(Decimal::new(2, 0))
    } else {
        Decimal::ZERO
    };
    let cross_bonus = if recent_target_cross(context, config).is_active() {
        Decimal::ONE
    } else {
        Decimal::ZERO
    };
    let elapsed_bonus = (Decimal::from(elapsed_window_secs(context)) / Decimal::from(120_i64))
        .min(Decimal::new(2, 0));

    (target_gap_abs
        + (aligned_swing_bps * Decimal::new(9, 1))
        + (aligned_micro_bps * Decimal::new(5, 1))
        + (aligned_burst_bps.max(Decimal::ZERO) * Decimal::new(2, 1))
        + (aligned_flow_bps * config.directional_trade_flow_weight)
        + acceleration_bonus
        + cross_bonus
        + elapsed_bonus)
        .round_dp(6)
}

fn bonereaper_state_v2_entry_notional_cap(
    available_market_capacity: Decimal,
    signal_tier: BonereaperStateV2SignalTier,
    config: &StrategyConfig,
) -> Decimal {
    let tier_cap = match signal_tier {
        BonereaperStateV2SignalTier::Probe => config.bonereaper_state_v2_probe_notional_usdc,
        BonereaperStateV2SignalTier::Normal => config.bonereaper_state_v2_normal_notional_usdc,
        BonereaperStateV2SignalTier::Strong => config.bonereaper_state_v2_strong_notional_usdc,
    };
    tier_cap.min(available_market_capacity).round_dp(6)
}

fn codex_sentinel_v1_entry_notional_cap(
    available_market_capacity: Decimal,
    context: &BtcFiveMinuteContext,
    decision: BonereaperStateV2Decision,
    primary_ask_price: Decimal,
    confidence_score: Decimal,
    config: &StrategyConfig,
) -> Decimal {
    let tier_cap = bonereaper_state_v2_entry_notional_cap(
        available_market_capacity,
        decision.signal_tier,
        config,
    );
    let mut capped_notional = tier_cap;

    let aligned_burst_bps = aligned_move_bps(context.spot_move_1s_bps, decision.up_side);
    let aligned_acceleration_bps =
        aligned_move_bps(context.micro_acceleration_bps, decision.up_side);
    let fresh_confirmation = decision
        .aligned_micro_bps
        .max(aligned_burst_bps)
        .max(aligned_acceleration_bps);
    let attack_quality = primary_ask_price <= config.codex_sentinel_v1_attack_max_entry_price
        && decision.signal_strength_bps >= config.codex_sentinel_v1_attack_min_signal_bps
        && decision.aligned_flow_bps >= config.codex_sentinel_v1_attack_min_flow_bps
        && fresh_confirmation >= config.codex_sentinel_v1_attack_min_confirmation_bps;

    if config.codex_sentinel_v1_attack_size_enabled && attack_quality {
        capped_notional = config
            .codex_sentinel_v1_attack_notional_usdc
            .max(capped_notional);
    }

    codex_sentinel_v1_confidence_sized_notional_cap(
        capped_notional,
        available_market_capacity,
        confidence_score,
        config,
    )
}

fn codex_sentinel_v1_confidence_sized_notional_cap(
    base_notional: Decimal,
    available_market_capacity: Decimal,
    confidence_score: Decimal,
    config: &StrategyConfig,
) -> Decimal {
    if !config.codex_sentinel_v1_confidence_sizing_enabled
        || config.codex_sentinel_v1_confidence_max_multiplier <= Decimal::ONE
        || confidence_score < config.codex_sentinel_v1_confidence_min_score
    {
        return base_notional.min(available_market_capacity).round_dp(6);
    }

    let score_headroom =
        (Decimal::from(100_u32) - config.codex_sentinel_v1_confidence_min_score).max(Decimal::ONE);
    let score_ratio = ((confidence_score - config.codex_sentinel_v1_confidence_min_score)
        / score_headroom)
        .clamp(Decimal::ZERO, Decimal::ONE);
    let multiplier = Decimal::ONE
        + ((config.codex_sentinel_v1_confidence_max_multiplier - Decimal::ONE) * score_ratio);

    (base_notional * multiplier)
        .min(available_market_capacity)
        .round_dp(6)
}

fn bonereaper_state_v2_variant_max_entry_price(
    kind: OpportunityKind,
    config: &StrategyConfig,
) -> Decimal {
    match kind {
        OpportunityKind::CodexSentinelV1 => config.codex_sentinel_v1_max_entry_price,
        OpportunityKind::CodexScalpProbeV1 => config.codex_scalp_probe_v1_max_entry_price,
        _ => config.bonereaper_state_v2_max_entry_price,
    }
}

fn bonereaper_state_v2_side_metrics(
    context: &BtcFiveMinuteContext,
    trade_flow: Option<&TradeFlowSummary>,
    up_side: bool,
) -> BonereaperStateV2SideMetrics {
    let target_gap_component = aligned_move_bps(context.target_gap_bps, up_side).max(Decimal::ZERO);
    let aligned_micro_bps = aligned_move_bps(context.spot_move_5s_bps, up_side);
    let aligned_swing_bps = aligned_move_bps(context.spot_move_15s_bps, up_side);
    let aligned_burst_bps = aligned_move_bps(context.spot_move_1s_bps, up_side);
    let aligned_flow_bps = aligned_trade_flow_bps(context, trade_flow)
        * if up_side == primary_side_from_context(context) {
            Decimal::ONE
        } else {
            -Decimal::ONE
        };
    let acceleration_bonus = aligned_move_bps(context.micro_acceleration_bps, up_side)
        .max(Decimal::ZERO)
        .min(Decimal::new(2, 0));
    let elapsed_bonus =
        (Decimal::from(elapsed_window_secs(context)) / Decimal::from(150_i64)).min(Decimal::ONE);

    BonereaperStateV2SideMetrics {
        aligned_micro_bps,
        aligned_swing_bps,
        aligned_flow_bps,
        signal_strength_bps: (target_gap_component
            + (aligned_swing_bps.max(Decimal::ZERO) * Decimal::new(8, 1))
            + (aligned_micro_bps.max(Decimal::ZERO) * Decimal::new(5, 1))
            + (aligned_burst_bps.max(Decimal::ZERO) * Decimal::new(2, 1))
            + (aligned_flow_bps.max(Decimal::ZERO) * Decimal::new(6, 1))
            + acceleration_bonus
            + elapsed_bonus)
            .round_dp(6),
    }
}

fn bonereaper_state_v2_signal_tier(
    context: &BtcFiveMinuteContext,
    metrics: BonereaperStateV2SideMetrics,
    up_side: bool,
    config: &StrategyConfig,
) -> BonereaperStateV2SignalTier {
    let with_target_gap = up_side == primary_side_from_context(context);
    let target_cross = recent_target_cross(context, config);
    let acceleration_ok = aligned_move_bps(context.micro_acceleration_bps, up_side) > Decimal::ZERO;

    if with_target_gap
        && context.target_gap_bps.abs() >= config.bonereaper_state_v2_strong_gap_bps
        && metrics.aligned_flow_bps >= config.bonereaper_state_v2_strong_flow_bps
        && metrics.aligned_micro_bps >= config.bonereaper_state_v2_min_spot_move_5s_bps
        && metrics.aligned_swing_bps >= config.bonereaper_state_v2_min_spot_move_15s_bps
        && (target_cross.is_active() || acceleration_ok)
    {
        BonereaperStateV2SignalTier::Strong
    } else if with_target_gap {
        BonereaperStateV2SignalTier::Normal
    } else {
        BonereaperStateV2SignalTier::Probe
    }
}

fn bonereaper_state_v2_quality_guard_block(
    context: &BtcFiveMinuteContext,
    decision: &BonereaperStateV2Decision,
    primary_ask_price: Decimal,
    config: &StrategyConfig,
) -> Option<BonereaperStateV2QualityBlock> {
    let aligned_burst_bps = aligned_move_bps(context.spot_move_1s_bps, decision.up_side);

    if config.bonereaper_state_v2_micro_alignment_guard_enabled
        && (aligned_burst_bps < -config.bonereaper_state_v2_max_counter_1s_bps
            || decision.aligned_micro_bps < -config.bonereaper_state_v2_max_counter_5s_bps)
    {
        return Some(BonereaperStateV2QualityBlock::CounterMicro);
    }

    let fresh_confirmation =
        bonereaper_state_v2_fresh_confirmation_bps(context, decision, aligned_burst_bps);
    if config.bonereaper_state_v2_early_window_guard_enabled
        && context.seconds_left > config.bonereaper_state_v2_early_window_max_seconds_left
    {
        let early_quality = fresh_confirmation
            >= config.bonereaper_state_v2_early_window_min_fresh_bps
            && decision.aligned_swing_bps >= config.bonereaper_state_v2_early_window_min_swing_bps
            && decision.signal_strength_bps
                >= config.bonereaper_state_v2_early_window_min_signal_bps;
        if !early_quality {
            return Some(BonereaperStateV2QualityBlock::EarlyWindow);
        }
    }

    if config.bonereaper_state_v2_early_expensive_guard_enabled
        && context.seconds_left >= config.bonereaper_state_v2_early_expensive_min_seconds_left
        && primary_ask_price > config.bonereaper_state_v2_early_expensive_entry_price
    {
        let extreme_quality = context.target_gap_bps.abs()
            >= config.bonereaper_state_v2_early_expensive_allow_min_target_gap_bps
            && fresh_confirmation >= config.bonereaper_state_v2_early_expensive_allow_min_fresh_bps
            && decision.signal_strength_bps
                >= config.bonereaper_state_v2_early_expensive_allow_min_signal_bps
            && decision.aligned_flow_bps
                >= config.bonereaper_state_v2_early_expensive_allow_min_flow_bps;
        if !extreme_quality {
            return Some(BonereaperStateV2QualityBlock::EarlyExpensive);
        }
    }

    if config.bonereaper_state_v2_low_gap_guard_enabled {
        let target_gap_abs = context.target_gap_bps.abs();
        let is_low_gap = target_gap_abs < config.bonereaper_state_v2_low_gap_max_target_gap_bps;
        if is_low_gap {
            let mature_enough =
                context.seconds_left >= config.bonereaper_state_v2_low_gap_min_seconds_left;
            let low_gap_value_entry = mature_enough
                && primary_ask_price <= config.bonereaper_state_v2_low_gap_max_entry_price;
            let low_gap_extreme_quality = mature_enough
                && fresh_confirmation >= config.bonereaper_state_v2_low_gap_allow_min_fresh_bps
                && decision.signal_strength_bps
                    >= config.bonereaper_state_v2_low_gap_allow_min_signal_bps
                && decision.aligned_flow_bps
                    >= config.bonereaper_state_v2_low_gap_allow_min_flow_bps;
            if !(low_gap_value_entry || low_gap_extreme_quality) {
                return Some(BonereaperStateV2QualityBlock::LowGap);
            }
        }
    }

    if config.bonereaper_state_v2_mid_gap_guard_enabled && !decision.counter_bias {
        let target_gap_abs = context.target_gap_bps.abs();
        let is_mid_gap = target_gap_abs >= config.bonereaper_state_v2_mid_gap_min_target_gap_bps
            && target_gap_abs <= config.bonereaper_state_v2_mid_gap_max_target_gap_bps;
        if is_mid_gap {
            let mid_gap_quality = primary_ask_price
                <= config.bonereaper_state_v2_mid_gap_max_entry_price
                && context.seconds_left >= config.bonereaper_state_v2_mid_gap_min_seconds_left
                && fresh_confirmation >= config.bonereaper_state_v2_mid_gap_min_fresh_bps
                && decision.signal_strength_bps
                    >= config.bonereaper_state_v2_mid_gap_min_signal_bps
                && decision.aligned_flow_bps >= config.bonereaper_state_v2_mid_gap_min_flow_bps;
            if !mid_gap_quality {
                return Some(BonereaperStateV2QualityBlock::MidGap);
            }
        }
    }

    if config.bonereaper_state_v2_high_gap_guard_enabled
        && !decision.counter_bias
        && context.target_gap_bps.abs() >= config.bonereaper_state_v2_high_gap_min_target_gap_bps
    {
        let high_gap_quality = primary_ask_price
            <= config.bonereaper_state_v2_high_gap_max_entry_price
            && fresh_confirmation >= config.bonereaper_state_v2_high_gap_min_fresh_bps
            && decision.aligned_swing_bps >= config.bonereaper_state_v2_high_gap_min_swing_bps
            && decision.signal_strength_bps >= config.bonereaper_state_v2_high_gap_min_signal_bps;
        if !high_gap_quality {
            return Some(BonereaperStateV2QualityBlock::HighGap);
        }
    }

    None
}

fn bonereaper_state_v2_fresh_confirmation_bps(
    context: &BtcFiveMinuteContext,
    decision: &BonereaperStateV2Decision,
    aligned_burst_bps: Decimal,
) -> Decimal {
    decision
        .aligned_micro_bps
        .max(aligned_burst_bps)
        .max(aligned_move_bps(
            context.micro_acceleration_bps,
            decision.up_side,
        ))
        .max(Decimal::ZERO)
}

fn codex_sentinel_v1_mid_signal_guard_blocks(
    context: &BtcFiveMinuteContext,
    decision: &BonereaperStateV2Decision,
    config: &StrategyConfig,
) -> bool {
    if !config.codex_sentinel_v1_mid_signal_guard_enabled {
        return false;
    }

    let signal_strength = decision.signal_strength_bps.abs();
    if signal_strength < config.codex_sentinel_v1_mid_signal_min_bps
        || signal_strength > config.codex_sentinel_v1_mid_signal_max_bps
    {
        return false;
    }

    let min_confirmation = config.codex_sentinel_v1_mid_signal_min_confirmation_bps;
    let aligned_acceleration_bps =
        aligned_move_bps(context.micro_acceleration_bps, decision.up_side);
    let has_fresh_confirmation = decision.aligned_micro_bps > min_confirmation
        || decision.aligned_swing_bps > min_confirmation
        || decision.aligned_flow_bps > min_confirmation
        || aligned_acceleration_bps > min_confirmation
        || recent_target_cross(context, config).is_active();

    !has_fresh_confirmation
}

fn codex_sentinel_v1_stale_micro_guard_blocks(
    context: &BtcFiveMinuteContext,
    decision: &BonereaperStateV2Decision,
    primary_ask_price: Decimal,
    config: &StrategyConfig,
) -> bool {
    if !config.codex_sentinel_v1_stale_micro_guard_enabled {
        return false;
    }

    let max_confirmation = config.codex_sentinel_v1_stale_micro_max_confirmation_bps;
    let aligned_burst_bps = aligned_move_bps(context.spot_move_1s_bps, decision.up_side);
    let micro_is_stale =
        decision.aligned_micro_bps <= max_confirmation && aligned_burst_bps <= max_confirmation;
    if !micro_is_stale {
        return false;
    }

    let swing_confirmed = decision.aligned_swing_bps
        >= config.codex_sentinel_v1_stale_micro_min_swing_bps
        && context.target_gap_bps.abs() >= config.codex_sentinel_v1_stale_micro_min_target_gap_bps;
    let strong_enough_without_fresh_micro = primary_ask_price
        <= config.codex_sentinel_v1_stale_micro_max_non_discount_entry_price
        && swing_confirmed
        && decision.signal_strength_bps >= config.codex_sentinel_v1_stale_micro_min_signal_bps
        && decision.aligned_flow_bps >= config.codex_sentinel_v1_stale_micro_min_flow_bps;
    let discounted_enough_without_fresh_micro = primary_ask_price
        <= config.codex_sentinel_v1_stale_micro_discount_max_entry_price
        && swing_confirmed
        && decision.signal_strength_bps
            >= config.codex_sentinel_v1_stale_micro_discount_min_signal_bps
        && decision.aligned_flow_bps >= config.codex_sentinel_v1_stale_micro_discount_min_flow_bps;

    !(strong_enough_without_fresh_micro || discounted_enough_without_fresh_micro)
}

fn codex_sentinel_v1_expensive_entry_guard_blocks(
    context: &BtcFiveMinuteContext,
    decision: &BonereaperStateV2Decision,
    primary_ask_price: Decimal,
    config: &StrategyConfig,
) -> bool {
    if !config.codex_sentinel_v1_expensive_entry_guard_enabled
        || primary_ask_price < config.codex_sentinel_v1_expensive_entry_price
    {
        return false;
    }

    let aligned_burst_bps = aligned_move_bps(context.spot_move_1s_bps, decision.up_side);
    let aligned_acceleration_bps =
        aligned_move_bps(context.micro_acceleration_bps, decision.up_side);
    let fresh_micro_confirmation = decision
        .aligned_micro_bps
        .max(aligned_burst_bps)
        .max(aligned_acceleration_bps);

    fresh_micro_confirmation < config.codex_sentinel_v1_expensive_min_micro_bps
        || decision.aligned_swing_bps < config.codex_sentinel_v1_expensive_min_swing_bps
}

fn codex_sentinel_v1_entry_spread_guard_blocks(
    primary_ask_price: Decimal,
    primary_book: &OrderBook,
    config: &StrategyConfig,
) -> bool {
    if !config.codex_sentinel_v1_entry_spread_guard_enabled {
        return false;
    }

    match codex_sentinel_v1_entry_spread(primary_ask_price, primary_book) {
        Some(spread) => spread > config.codex_sentinel_v1_max_entry_spread,
        None => true,
    }
}

fn codex_sentinel_v1_entry_spread(
    primary_ask_price: Decimal,
    primary_book: &OrderBook,
) -> Option<Decimal> {
    let best_bid = primary_book.best_bid()?;
    if !is_valid_binary_price(primary_ask_price)
        || !is_valid_binary_price(best_bid.price)
        || best_bid.size <= Decimal::ZERO
    {
        return None;
    }

    Some(saturating_sub(primary_ask_price, best_bid.price))
}

fn codex_sentinel_v1_premium_entry_guard_blocks(
    context: &BtcFiveMinuteContext,
    decision: &BonereaperStateV2Decision,
    primary_ask_price: Decimal,
    config: &StrategyConfig,
) -> bool {
    if !config.codex_sentinel_v1_premium_entry_guard_enabled
        || primary_ask_price < config.codex_sentinel_v1_premium_entry_price
    {
        return false;
    }

    codex_sentinel_v1_fresh_confirmation_bps(context, decision)
        < config.codex_sentinel_v1_premium_min_fresh_bps
        || decision.signal_strength_bps < config.codex_sentinel_v1_premium_min_signal_bps
        || decision.aligned_flow_bps < config.codex_sentinel_v1_premium_min_flow_bps
}

fn codex_sentinel_v1_live_quote_age_guard_blocks(
    context: &BtcFiveMinuteContext,
    config: &StrategyConfig,
) -> bool {
    config.codex_sentinel_v1_live_quote_age_guard_enabled
        && !codex_sentinel_v1_has_fresh_live_spot(
            context,
            config.codex_sentinel_v1_max_live_quote_age_ms,
        )
}

fn codex_sentinel_v1_aggressive_continuation_allows(
    context: &BtcFiveMinuteContext,
    decision: &BonereaperStateV2Decision,
    primary_ask_price: Decimal,
    config: &StrategyConfig,
) -> bool {
    if !config.codex_sentinel_v1_aggressive_continuation_enabled
        || primary_ask_price > config.codex_sentinel_v1_aggressive_continuation_max_entry_price
        || !codex_sentinel_v1_has_fresh_live_spot(
            context,
            config.codex_sentinel_v1_aggressive_continuation_max_quote_age_ms,
        )
    {
        return false;
    }

    context.target_gap_bps.abs()
        >= config.codex_sentinel_v1_aggressive_continuation_min_target_gap_bps
        && decision.signal_strength_bps
            >= config.codex_sentinel_v1_aggressive_continuation_min_signal_bps
        && decision.aligned_flow_bps
            >= config.codex_sentinel_v1_aggressive_continuation_min_flow_bps
        && codex_sentinel_v1_fresh_confirmation_bps(context, decision)
            >= config.codex_sentinel_v1_aggressive_continuation_min_fresh_bps
        && decision.aligned_swing_bps
            >= config.codex_sentinel_v1_aggressive_continuation_min_swing_bps
}

fn codex_sentinel_v1_target_allowed(target: MarketTarget) -> bool {
    matches!(target, MarketTarget::Btc5m)
}

fn codex_scalp_probe_v1_target_allowed(target: MarketTarget) -> bool {
    matches!(
        target,
        MarketTarget::Btc5m
            | MarketTarget::Eth5m
            | MarketTarget::Sol5m
            | MarketTarget::Xrp5m
            | MarketTarget::Bnb5m
    )
}

#[derive(Debug, Clone, Copy)]
struct CodexScalpProbeRadar {
    score_bps: Decimal,
    aligned_top_bps: Decimal,
    aligned_depth_bps: Decimal,
    aligned_microprice_bps: Decimal,
    aligned_burst_bps: Decimal,
    fresh_confirmation_bps: Decimal,
}

#[derive(Debug, Clone, Copy)]
struct CodexScalpProbeThresholds {
    min_target_gap_bps: Decimal,
    min_fresh_bps: Decimal,
    min_top_imbalance_bps: Decimal,
    min_depth_imbalance_bps: Decimal,
}

fn codex_scalp_probe_v1_allows(
    context: &BtcFiveMinuteContext,
    decision: &BonereaperStateV2Decision,
    primary_ask_price: Decimal,
    primary_book: &OrderBook,
    config: &StrategyConfig,
) -> bool {
    if config.codex_scalp_probe_v1_raw_ablation_enabled {
        if codex_scalp_probe_v1_raw_light_mode(config) {
            return codex_scalp_probe_v1_raw_light_allows(
                context,
                decision,
                primary_ask_price,
                primary_book,
                config,
            );
        }
        return codex_scalp_probe_v1_target_allowed(context.target)
            && is_valid_binary_price(primary_ask_price);
    }

    codex_scalp_probe_v1_standard_allows(context, decision, primary_ask_price, primary_book, config)
        || codex_scalp_probe_v1_bnb_pressure_allows(
            context,
            decision,
            primary_ask_price,
            primary_book,
            config,
        )
}

fn codex_scalp_probe_v1_standard_allows(
    context: &BtcFiveMinuteContext,
    decision: &BonereaperStateV2Decision,
    primary_ask_price: Decimal,
    primary_book: &OrderBook,
    config: &StrategyConfig,
) -> bool {
    if !codex_scalp_probe_v1_target_allowed(context.target)
        || !is_valid_binary_price(primary_ask_price)
        || primary_ask_price < config.codex_scalp_probe_v1_min_entry_price
        || primary_ask_price > config.codex_scalp_probe_v1_max_entry_price
    {
        return false;
    }

    let Some(entry_spread) = codex_sentinel_v1_entry_spread(primary_ask_price, primary_book) else {
        return false;
    };
    if entry_spread > config.codex_scalp_probe_v1_max_entry_spread {
        return false;
    }

    if !codex_sentinel_v1_has_fresh_live_spot(
        context,
        config.codex_sentinel_v1_max_live_quote_age_ms,
    ) {
        return false;
    }

    let Some(book_age_ms) = context.exchange_book_age_ms else {
        return false;
    };
    if book_age_ms < 0 || book_age_ms > config.codex_scalp_probe_v1_max_book_age_ms {
        return false;
    }

    let aligned_top_bps =
        aligned_move_bps(context.exchange_book_top_imbalance_bps, decision.up_side);
    let aligned_depth_bps =
        aligned_move_bps(context.exchange_book_depth_imbalance_bps, decision.up_side);
    let thresholds = CodexScalpProbeThresholds {
        min_target_gap_bps: config.codex_scalp_probe_v1_min_target_gap_bps,
        min_fresh_bps: config.codex_scalp_probe_v1_min_fresh_bps,
        min_top_imbalance_bps: config.codex_scalp_probe_v1_min_top_imbalance_bps,
        min_depth_imbalance_bps: config.codex_scalp_probe_v1_min_depth_imbalance_bps,
    };

    context.exchange_book_spread_bps <= config.codex_scalp_probe_v1_max_exchange_spread_bps
        && codex_scalp_probe_v1_quality_allows(
            context,
            decision,
            CodexScalpProbeRadar {
                score_bps: codex_scalp_probe_v1_radar_score_bps(
                    context,
                    decision,
                    aligned_top_bps,
                    aligned_depth_bps,
                    codex_sentinel_v1_fresh_confirmation_bps(context, decision),
                ),
                aligned_top_bps,
                aligned_depth_bps,
                aligned_microprice_bps: aligned_move_bps(
                    context.exchange_book_microprice_bps,
                    decision.up_side,
                ),
                aligned_burst_bps: aligned_move_bps(context.spot_move_1s_bps, decision.up_side),
                fresh_confirmation_bps: codex_sentinel_v1_fresh_confirmation_bps(context, decision),
            },
            thresholds,
            config,
        )
}

#[derive(Debug, Clone, Copy)]
struct CodexScalpProbeRawLightProfile {
    min_entry_price: Decimal,
    max_entry_price: Decimal,
    max_target_gap_bps: Decimal,
    cheap_entry_price: Decimal,
    min_signal_bps: Decimal,
    min_target_gap_bps: Decimal,
    min_fresh_bps: Decimal,
    min_aligned_micro_bps: Decimal,
    min_aligned_swing_bps: Decimal,
    min_top_imbalance_bps: Decimal,
    min_depth_imbalance_bps: Decimal,
    cheap_min_signal_bps: Decimal,
    cheap_min_target_gap_bps: Decimal,
    cheap_min_aligned_bps: Decimal,
}

#[allow(clippy::too_many_lines)]
fn codex_scalp_probe_v1_raw_light_profile(target: MarketTarget) -> CodexScalpProbeRawLightProfile {
    match target {
        MarketTarget::Btc5m => CodexScalpProbeRawLightProfile {
            min_entry_price: Decimal::new(50, 2),
            max_entry_price: Decimal::new(68, 2),
            max_target_gap_bps: Decimal::new(1200, 2),
            cheap_entry_price: Decimal::new(12, 2),
            min_signal_bps: Decimal::from(12_u32),
            min_target_gap_bps: Decimal::new(400, 2),
            min_fresh_bps: Decimal::new(30, 2),
            min_aligned_micro_bps: Decimal::new(350, 2),
            min_aligned_swing_bps: Decimal::new(350, 2),
            min_top_imbalance_bps: Decimal::from(500_u32),
            min_depth_imbalance_bps: Decimal::from(500_u32),
            cheap_min_signal_bps: Decimal::from(15_u32),
            cheap_min_target_gap_bps: Decimal::new(300, 2),
            cheap_min_aligned_bps: Decimal::new(400, 2),
        },
        MarketTarget::Eth5m => CodexScalpProbeRawLightProfile {
            min_entry_price: Decimal::new(45, 2),
            max_entry_price: Decimal::new(62, 2),
            max_target_gap_bps: Decimal::new(1400, 2),
            cheap_entry_price: Decimal::new(10, 2),
            min_signal_bps: Decimal::from(22_u32),
            min_target_gap_bps: Decimal::new(800, 2),
            min_fresh_bps: Decimal::ONE,
            min_aligned_micro_bps: Decimal::new(600, 2),
            min_aligned_swing_bps: Decimal::new(600, 2),
            min_top_imbalance_bps: Decimal::from(1_500_u32),
            min_depth_imbalance_bps: Decimal::from(1_500_u32),
            cheap_min_signal_bps: Decimal::from(24_u32),
            cheap_min_target_gap_bps: Decimal::new(600, 2),
            cheap_min_aligned_bps: Decimal::new(700, 2),
        },
        MarketTarget::Sol5m => CodexScalpProbeRawLightProfile {
            min_entry_price: Decimal::new(45, 2),
            max_entry_price: Decimal::new(62, 2),
            max_target_gap_bps: Decimal::new(1400, 2),
            cheap_entry_price: Decimal::new(10, 2),
            min_signal_bps: Decimal::from(20_u32),
            min_target_gap_bps: Decimal::new(800, 2),
            min_fresh_bps: Decimal::ONE,
            min_aligned_micro_bps: Decimal::new(600, 2),
            min_aligned_swing_bps: Decimal::new(600, 2),
            min_top_imbalance_bps: Decimal::from(1_500_u32),
            min_depth_imbalance_bps: Decimal::from(1_500_u32),
            cheap_min_signal_bps: Decimal::from(22_u32),
            cheap_min_target_gap_bps: Decimal::new(600, 2),
            cheap_min_aligned_bps: Decimal::new(700, 2),
        },
        MarketTarget::Xrp5m => CodexScalpProbeRawLightProfile {
            min_entry_price: Decimal::new(40, 2),
            max_entry_price: Decimal::new(58, 2),
            max_target_gap_bps: Decimal::new(1200, 2),
            cheap_entry_price: Decimal::new(10, 2),
            min_signal_bps: Decimal::from(22_u32),
            min_target_gap_bps: Decimal::new(700, 2),
            min_fresh_bps: Decimal::new(80, 2),
            min_aligned_micro_bps: Decimal::new(650, 2),
            min_aligned_swing_bps: Decimal::new(700, 2),
            min_top_imbalance_bps: Decimal::from(1_500_u32),
            min_depth_imbalance_bps: Decimal::from(1_500_u32),
            cheap_min_signal_bps: Decimal::from(22_u32),
            cheap_min_target_gap_bps: Decimal::new(500, 2),
            cheap_min_aligned_bps: Decimal::new(700, 2),
        },
        MarketTarget::Bnb5m => CodexScalpProbeRawLightProfile {
            min_entry_price: Decimal::new(45, 2),
            max_entry_price: Decimal::new(62, 2),
            max_target_gap_bps: Decimal::new(1000, 2),
            cheap_entry_price: Decimal::new(10, 2),
            min_signal_bps: Decimal::from(8_u32),
            min_target_gap_bps: Decimal::new(400, 2),
            min_fresh_bps: Decimal::new(40, 2),
            min_aligned_micro_bps: Decimal::new(150, 2),
            min_aligned_swing_bps: Decimal::new(300, 2),
            min_top_imbalance_bps: Decimal::from(1_500_u32),
            min_depth_imbalance_bps: Decimal::from(1_300_u32),
            cheap_min_signal_bps: Decimal::from(10_u32),
            cheap_min_target_gap_bps: Decimal::new(350, 2),
            cheap_min_aligned_bps: Decimal::new(250, 2),
        },
        MarketTarget::Btc15m | MarketTarget::Eth15m => CodexScalpProbeRawLightProfile {
            min_entry_price: Decimal::MAX,
            max_entry_price: Decimal::ZERO,
            max_target_gap_bps: Decimal::ZERO,
            cheap_entry_price: Decimal::ZERO,
            min_signal_bps: Decimal::MAX,
            min_target_gap_bps: Decimal::MAX,
            min_fresh_bps: Decimal::MAX,
            min_aligned_micro_bps: Decimal::MAX,
            min_aligned_swing_bps: Decimal::MAX,
            min_top_imbalance_bps: Decimal::MAX,
            min_depth_imbalance_bps: Decimal::MAX,
            cheap_min_signal_bps: Decimal::MAX,
            cheap_min_target_gap_bps: Decimal::MAX,
            cheap_min_aligned_bps: Decimal::MAX,
        },
    }
}

fn codex_scalp_probe_v1_raw_light_allows(
    context: &BtcFiveMinuteContext,
    decision: &BonereaperStateV2Decision,
    primary_ask_price: Decimal,
    primary_book: &OrderBook,
    config: &StrategyConfig,
) -> bool {
    if !codex_scalp_probe_v1_target_allowed(context.target)
        || !is_valid_binary_price(primary_ask_price)
    {
        return false;
    }

    let profile = codex_scalp_probe_v1_raw_light_profile(context.target);
    if primary_ask_price < profile.min_entry_price || primary_ask_price > profile.max_entry_price {
        return false;
    }

    if elapsed_window_secs(context) < config.codex_scalp_probe_v1_min_elapsed_window_secs
        || context.seconds_left > config.codex_scalp_probe_v1_max_seconds_left
        || context.seconds_left < config.codex_scalp_probe_v1_min_seconds_left
    {
        return false;
    }

    if !codex_sentinel_v1_has_fresh_live_spot(
        context,
        config.codex_sentinel_v1_max_live_quote_age_ms,
    ) {
        return false;
    }

    let Some(entry_spread) = codex_sentinel_v1_entry_spread(primary_ask_price, primary_book) else {
        return false;
    };
    if entry_spread > config.codex_scalp_probe_v1_max_entry_spread {
        return false;
    }

    let Some(book_age_ms) = context.exchange_book_age_ms else {
        return false;
    };
    if book_age_ms < 0 || book_age_ms > config.codex_scalp_probe_v1_max_book_age_ms {
        return false;
    }

    if context.exchange_book_spread_bps > config.codex_scalp_probe_v1_max_exchange_spread_bps {
        return false;
    }

    let aligned_top_bps =
        aligned_move_bps(context.exchange_book_top_imbalance_bps, decision.up_side);
    let aligned_depth_bps =
        aligned_move_bps(context.exchange_book_depth_imbalance_bps, decision.up_side);
    let fresh_confirmation_bps = codex_scalp_probe_v1_live_burst_bps(context, decision);
    let target_gap_abs = context.target_gap_bps.abs();
    if target_gap_abs > profile.max_target_gap_bps {
        return false;
    }
    let aligned_micro_or_swing = decision
        .aligned_micro_bps
        .max(decision.aligned_swing_bps)
        .max(fresh_confirmation_bps);

    let normal_lane = primary_ask_price <= profile.max_entry_price
        && decision.signal_strength_bps >= profile.min_signal_bps
        && target_gap_abs >= profile.min_target_gap_bps
        && fresh_confirmation_bps >= profile.min_fresh_bps
        && decision.aligned_micro_bps >= profile.min_aligned_micro_bps
        && decision.aligned_swing_bps >= profile.min_aligned_swing_bps
        && aligned_top_bps >= profile.min_top_imbalance_bps
        && aligned_depth_bps >= profile.min_depth_imbalance_bps;

    let cheap_lottery_lane = primary_ask_price <= profile.cheap_entry_price
        && decision.signal_strength_bps >= profile.cheap_min_signal_bps
        && target_gap_abs >= profile.cheap_min_target_gap_bps
        && aligned_micro_or_swing >= profile.cheap_min_aligned_bps
        && aligned_top_bps >= Decimal::ZERO
        && aligned_depth_bps >= Decimal::ZERO;

    normal_lane || cheap_lottery_lane
}

fn codex_scalp_probe_v1_raw_light_mode(config: &StrategyConfig) -> bool {
    config.codex_scalp_probe_v1_raw_ablation_enabled
        && config.codex_scalp_probe_v1_raw_light_enabled
}

fn codex_scalp_probe_v1_fresh_confirmation_bps(
    context: &BtcFiveMinuteContext,
    decision: &BonereaperStateV2Decision,
    config: &StrategyConfig,
) -> Decimal {
    if codex_scalp_probe_v1_raw_light_mode(config) {
        codex_scalp_probe_v1_live_burst_bps(context, decision)
    } else {
        codex_sentinel_v1_fresh_confirmation_bps(context, decision)
    }
}

fn codex_scalp_probe_v1_live_burst_bps(
    context: &BtcFiveMinuteContext,
    decision: &BonereaperStateV2Decision,
) -> Decimal {
    aligned_move_bps(context.spot_move_1s_bps, decision.up_side)
        .max(aligned_move_bps(
            context.micro_acceleration_bps,
            decision.up_side,
        ))
        .max(Decimal::ZERO)
}

fn codex_scalp_probe_v1_bnb_pressure_allows(
    context: &BtcFiveMinuteContext,
    decision: &BonereaperStateV2Decision,
    primary_ask_price: Decimal,
    primary_book: &OrderBook,
    config: &StrategyConfig,
) -> bool {
    if !config.codex_scalp_probe_v1_bnb_pressure_enabled
        || context.target != MarketTarget::Bnb5m
        || !is_valid_binary_price(primary_ask_price)
        || primary_ask_price < config.codex_scalp_probe_v1_min_entry_price
        || primary_ask_price > config.codex_scalp_probe_v1_bnb_pressure_max_entry_price
    {
        return false;
    }

    let Some(entry_spread) = codex_sentinel_v1_entry_spread(primary_ask_price, primary_book) else {
        return false;
    };
    if entry_spread > config.codex_scalp_probe_v1_max_entry_spread {
        return false;
    }

    if !codex_sentinel_v1_has_fresh_live_spot(
        context,
        config.codex_sentinel_v1_max_live_quote_age_ms,
    ) {
        return false;
    }

    let Some(book_age_ms) = context.exchange_book_age_ms else {
        return false;
    };
    if book_age_ms < 0 || book_age_ms > config.codex_scalp_probe_v1_bnb_pressure_max_book_age_ms {
        return false;
    }

    let aligned_top_bps =
        aligned_move_bps(context.exchange_book_top_imbalance_bps, decision.up_side);
    let aligned_depth_bps =
        aligned_move_bps(context.exchange_book_depth_imbalance_bps, decision.up_side);
    let thresholds = CodexScalpProbeThresholds {
        min_target_gap_bps: config.codex_scalp_probe_v1_bnb_pressure_min_target_gap_bps,
        min_fresh_bps: config.codex_scalp_probe_v1_bnb_pressure_min_fresh_bps,
        min_top_imbalance_bps: config.codex_scalp_probe_v1_bnb_pressure_min_top_imbalance_bps,
        min_depth_imbalance_bps: config.codex_scalp_probe_v1_bnb_pressure_min_depth_imbalance_bps,
    };

    context.exchange_book_spread_bps <= config.codex_scalp_probe_v1_max_exchange_spread_bps
        && codex_scalp_probe_v1_quality_allows(
            context,
            decision,
            CodexScalpProbeRadar {
                score_bps: codex_scalp_probe_v1_radar_score_bps(
                    context,
                    decision,
                    aligned_top_bps,
                    aligned_depth_bps,
                    codex_sentinel_v1_fresh_confirmation_bps(context, decision),
                ),
                aligned_top_bps,
                aligned_depth_bps,
                aligned_microprice_bps: aligned_move_bps(
                    context.exchange_book_microprice_bps,
                    decision.up_side,
                ),
                aligned_burst_bps: aligned_move_bps(context.spot_move_1s_bps, decision.up_side),
                fresh_confirmation_bps: codex_sentinel_v1_fresh_confirmation_bps(context, decision),
            },
            thresholds,
            config,
        )
}

fn codex_scalp_probe_v1_quality_allows(
    context: &BtcFiveMinuteContext,
    decision: &BonereaperStateV2Decision,
    radar: CodexScalpProbeRadar,
    thresholds: CodexScalpProbeThresholds,
    config: &StrategyConfig,
) -> bool {
    let pressure_ok = codex_scalp_probe_v1_pressure_ok(radar, thresholds);
    let high_pressure_override =
        codex_scalp_probe_v1_high_pressure_override_allows(radar, thresholds, config);
    let target_gap_ok =
        context.target_gap_bps.abs() >= thresholds.min_target_gap_bps || high_pressure_override;
    let flow_ok = decision.aligned_flow_bps >= config.codex_scalp_probe_v1_min_flow_bps
        || high_pressure_override;

    radar.score_bps >= config.codex_scalp_probe_v1_min_radar_score_bps
        && pressure_ok
        && target_gap_ok
        && radar.fresh_confirmation_bps >= thresholds.min_fresh_bps
        && decision.signal_strength_bps >= config.codex_scalp_probe_v1_min_signal_bps
        && flow_ok
}

fn codex_scalp_probe_v1_pressure_ok(
    radar: CodexScalpProbeRadar,
    thresholds: CodexScalpProbeThresholds,
) -> bool {
    radar.aligned_top_bps >= thresholds.min_top_imbalance_bps
        && radar.aligned_depth_bps >= thresholds.min_depth_imbalance_bps
}

fn codex_scalp_probe_v1_high_pressure_override_allows(
    radar: CodexScalpProbeRadar,
    thresholds: CodexScalpProbeThresholds,
    config: &StrategyConfig,
) -> bool {
    radar.score_bps >= config.codex_scalp_probe_v1_min_radar_score_bps * Decimal::new(125, 2)
        && codex_scalp_probe_v1_pressure_ok(radar, thresholds)
        && radar.aligned_burst_bps >= Decimal::ZERO
        && radar.aligned_microprice_bps >= config.codex_breakout_v1_min_microprice_bps
}

fn codex_scalp_probe_v1_radar_score_bps(
    context: &BtcFiveMinuteContext,
    decision: &BonereaperStateV2Decision,
    aligned_top_bps: Decimal,
    aligned_depth_bps: Decimal,
    fresh_confirmation_bps: Decimal,
) -> Decimal {
    let aligned_microprice_bps =
        aligned_move_bps(context.exchange_book_microprice_bps, decision.up_side);
    let aligned_burst_bps = aligned_move_bps(context.spot_move_1s_bps, decision.up_side);
    let exchange_spread_penalty =
        context.exchange_book_spread_bps.max(Decimal::ZERO) * Decimal::from(50_u32);

    (aligned_depth_bps.max(Decimal::ZERO)
        + (aligned_top_bps.max(Decimal::ZERO) / Decimal::from(2_u32))
        + (aligned_microprice_bps.max(Decimal::ZERO) * Decimal::from(500_u32))
        + (fresh_confirmation_bps.max(Decimal::ZERO) * Decimal::from(50_u32))
        + (context.target_gap_bps.abs().max(Decimal::ZERO) * Decimal::from(50_u32))
        + (aligned_burst_bps.max(Decimal::ZERO) * Decimal::from(80_u32))
        + (decision.aligned_swing_bps.max(Decimal::ZERO) * Decimal::from(30_u32))
        + (decision.aligned_flow_bps.max(Decimal::ZERO) / Decimal::from(4_u32))
        - exchange_spread_penalty)
        .max(Decimal::ZERO)
        .round_dp(4)
}

fn codex_scalp_probe_v1_min_expected_profit_usdc(
    context: &BtcFiveMinuteContext,
    decision: &BonereaperStateV2Decision,
    primary_ask_price: Decimal,
    primary_book: &OrderBook,
    config: &StrategyConfig,
) -> Decimal {
    if codex_scalp_probe_v1_bnb_pressure_allows(
        context,
        decision,
        primary_ask_price,
        primary_book,
        config,
    ) {
        config.codex_scalp_probe_v1_bnb_pressure_min_expected_profit_usdc
    } else {
        config.codex_scalp_probe_v1_min_expected_profit_usdc
    }
}

fn codex_breakout_v1_allows(
    context: &BtcFiveMinuteContext,
    decision: &BonereaperStateV2Decision,
    primary_ask_price: Decimal,
    config: &StrategyConfig,
) -> bool {
    if !config.codex_breakout_v1_enabled
        || primary_ask_price > config.codex_breakout_v1_max_entry_price
        || !codex_sentinel_v1_has_fresh_live_spot(
            context,
            config.codex_sentinel_v1_max_live_quote_age_ms,
        )
    {
        return false;
    }

    let Some(book_age_ms) = context.exchange_book_age_ms else {
        return false;
    };
    if book_age_ms < 0 || book_age_ms > config.codex_breakout_v1_max_book_age_ms {
        return false;
    }

    let aligned_depth_bps =
        aligned_move_bps(context.exchange_book_depth_imbalance_bps, decision.up_side);
    let aligned_top_bps =
        aligned_move_bps(context.exchange_book_top_imbalance_bps, decision.up_side);
    let aligned_microprice_bps =
        aligned_move_bps(context.exchange_book_microprice_bps, decision.up_side);
    let fresh_confirmation = codex_sentinel_v1_fresh_confirmation_bps(context, decision);

    if context.exchange_book_spread_bps > config.codex_breakout_v1_max_spread_bps
        || aligned_depth_bps < config.codex_breakout_v1_min_depth_imbalance_bps
        || aligned_microprice_bps < config.codex_breakout_v1_min_microprice_bps
        || fresh_confirmation < config.codex_breakout_v1_min_fresh_bps
        || context.target_gap_bps.abs() < config.codex_breakout_v1_min_target_gap_bps
        || decision.signal_strength_bps < config.codex_breakout_v1_min_signal_bps
        || decision.aligned_flow_bps < config.codex_breakout_v1_min_flow_bps
    {
        return false;
    }

    codex_breakout_v1_score_bps(
        aligned_depth_bps,
        aligned_top_bps,
        aligned_microprice_bps,
        fresh_confirmation,
        context.target_gap_bps.abs(),
    ) >= config.codex_breakout_v1_min_score_bps
}

fn codex_sentinel_v1_discount_value_lane_allows(
    context: &BtcFiveMinuteContext,
    decision: &BonereaperStateV2Decision,
    primary_ask_price: Decimal,
    config: &StrategyConfig,
) -> bool {
    if !config.codex_sentinel_v1_discount_value_lane_enabled
        || decision.counter_bias
        || !is_valid_binary_price(primary_ask_price)
        || primary_ask_price > config.codex_sentinel_v1_discount_value_max_entry_price
        || !codex_sentinel_v1_has_fresh_live_spot(
            context,
            config.codex_sentinel_v1_max_live_quote_age_ms,
        )
    {
        return false;
    }

    let Some(book_age_ms) = context.exchange_book_age_ms else {
        return false;
    };
    if book_age_ms < 0 || book_age_ms > config.codex_sentinel_v1_discount_value_max_book_age_ms {
        return false;
    }

    let aligned_top_bps =
        aligned_move_bps(context.exchange_book_top_imbalance_bps, decision.up_side);
    let aligned_depth_bps =
        aligned_move_bps(context.exchange_book_depth_imbalance_bps, decision.up_side);
    let aligned_microprice_bps =
        aligned_move_bps(context.exchange_book_microprice_bps, decision.up_side);
    let aligned_burst_bps = aligned_move_bps(context.spot_move_1s_bps, decision.up_side);

    context.exchange_book_spread_bps
        <= config.codex_sentinel_v1_discount_value_max_exchange_spread_bps
        && aligned_burst_bps >= Decimal::ZERO
        && context.target_gap_bps.abs()
            >= config.codex_sentinel_v1_discount_value_min_target_gap_bps
        && codex_sentinel_v1_fresh_confirmation_bps(context, decision)
            >= config.codex_sentinel_v1_discount_value_min_fresh_bps
        && decision.aligned_swing_bps >= config.codex_sentinel_v1_discount_value_min_swing_bps
        && decision.signal_strength_bps >= config.codex_sentinel_v1_discount_value_min_signal_bps
        && decision.aligned_flow_bps >= config.codex_sentinel_v1_discount_value_min_flow_bps
        && aligned_top_bps >= config.codex_sentinel_v1_discount_value_min_top_imbalance_bps
        && aligned_depth_bps >= config.codex_sentinel_v1_discount_value_min_depth_imbalance_bps
        && aligned_microprice_bps >= config.codex_sentinel_v1_discount_value_min_microprice_bps
}

fn codex_breakout_v1_score_bps(
    aligned_depth_bps: Decimal,
    aligned_top_bps: Decimal,
    aligned_microprice_bps: Decimal,
    fresh_confirmation_bps: Decimal,
    target_gap_abs_bps: Decimal,
) -> Decimal {
    (aligned_depth_bps.max(Decimal::ZERO)
        + (aligned_top_bps.max(Decimal::ZERO) / Decimal::from(2_u32))
        + (aligned_microprice_bps.max(Decimal::ZERO) * Decimal::from(500_u32))
        + (fresh_confirmation_bps.max(Decimal::ZERO) * Decimal::from(50_u32))
        + (target_gap_abs_bps.max(Decimal::ZERO) * Decimal::from(50_u32)))
    .round_dp(4)
}

fn codex_sentinel_v1_has_fresh_live_spot(
    context: &BtcFiveMinuteContext,
    max_quote_age_ms: i64,
) -> bool {
    if max_quote_age_ms < 0 {
        return false;
    }

    match context.current_spot_source.as_str() {
        "Coinbase::Ticker" | "Binance::Trade" => {}
        _ => return false,
    }

    let Some(quote_age_ms) = codex_sentinel_v1_live_quote_age_ms(context) else {
        return false;
    };

    quote_age_ms >= 0 && quote_age_ms <= max_quote_age_ms
}

fn codex_sentinel_v1_live_quote_age_ms(context: &BtcFiveMinuteContext) -> Option<i64> {
    context
        .current_spot_received_age_ms
        .or(context.current_spot_event_age_ms)
}

fn codex_sentinel_v1_no_chase_guard_blocks(
    context: &BtcFiveMinuteContext,
    decision: &BonereaperStateV2Decision,
    primary_ask_price: Decimal,
    config: &StrategyConfig,
) -> bool {
    if !config.codex_sentinel_v1_no_chase_guard_enabled
        || primary_ask_price <= config.codex_sentinel_v1_no_chase_entry_price
        || context.seconds_left < config.codex_sentinel_v1_no_chase_min_seconds_left
    {
        return false;
    }

    let fresh_confirmation = codex_sentinel_v1_fresh_confirmation_bps(context, decision);
    let extreme_quality = context.target_gap_bps.abs()
        >= config.codex_sentinel_v1_no_chase_allow_min_target_gap_bps
        && fresh_confirmation >= config.codex_sentinel_v1_no_chase_allow_min_fresh_bps
        && decision.signal_strength_bps >= config.codex_sentinel_v1_no_chase_allow_min_signal_bps
        && decision.aligned_flow_bps >= config.codex_sentinel_v1_no_chase_allow_min_flow_bps;

    !extreme_quality
}

fn codex_sentinel_v1_late_window_value_guard_blocks(
    context: &BtcFiveMinuteContext,
    decision: &BonereaperStateV2Decision,
    primary_ask_price: Decimal,
    config: &StrategyConfig,
) -> bool {
    if !config.codex_sentinel_v1_late_window_value_guard_enabled
        || context.seconds_left > config.codex_sentinel_v1_late_window_max_seconds_left
        || primary_ask_price <= config.codex_sentinel_v1_late_window_max_entry_price
    {
        return false;
    }

    let fresh_confirmation = codex_sentinel_v1_fresh_confirmation_bps(context, decision);
    let extreme_quality = context.target_gap_bps.abs()
        >= config.codex_sentinel_v1_late_window_allow_min_target_gap_bps
        && fresh_confirmation >= config.codex_sentinel_v1_late_window_allow_min_fresh_bps
        && decision.signal_strength_bps
            >= config.codex_sentinel_v1_late_window_allow_min_signal_bps
        && decision.aligned_flow_bps >= config.codex_sentinel_v1_late_window_allow_min_flow_bps;

    !extreme_quality
}

fn codex_sentinel_v1_quality_floor_blocks(
    context: &BtcFiveMinuteContext,
    decision: &BonereaperStateV2Decision,
    config: &StrategyConfig,
) -> bool {
    if !config.codex_sentinel_v1_quality_floor_enabled {
        return false;
    }

    let target_gap_abs = context.target_gap_bps.abs();
    if target_gap_abs < config.codex_sentinel_v1_quality_floor_min_target_gap_bps {
        return true;
    }

    let mid_gap_guard_enabled = config.codex_sentinel_v1_quality_floor_mid_gap_max_bps
        > config.codex_sentinel_v1_quality_floor_min_target_gap_bps;
    if !mid_gap_guard_enabled
        || target_gap_abs > config.codex_sentinel_v1_quality_floor_mid_gap_max_bps
    {
        return false;
    }

    decision.signal_strength_bps < config.codex_sentinel_v1_quality_floor_mid_gap_min_signal_bps
        || decision.aligned_flow_bps < config.codex_sentinel_v1_quality_floor_mid_gap_min_flow_bps
}

fn codex_sentinel_v1_low_flow_guard_blocks(
    context: &BtcFiveMinuteContext,
    decision: &BonereaperStateV2Decision,
    primary_ask_price: Decimal,
    config: &StrategyConfig,
) -> bool {
    if !config.codex_sentinel_v1_low_flow_guard_enabled
        || decision.aligned_flow_bps > config.codex_sentinel_v1_low_flow_max_flow_bps
    {
        return false;
    }

    primary_ask_price > config.codex_sentinel_v1_low_flow_allow_max_entry_price
        || decision.signal_strength_bps < config.codex_sentinel_v1_low_flow_allow_min_signal_bps
        || codex_sentinel_v1_fresh_confirmation_bps(context, decision)
            < config.codex_sentinel_v1_low_flow_allow_min_fresh_bps
        || decision.aligned_swing_bps < config.codex_sentinel_v1_low_flow_allow_min_swing_bps
}

fn codex_sentinel_v1_mid_gap_premium_guard_blocks(
    context: &BtcFiveMinuteContext,
    decision: &BonereaperStateV2Decision,
    primary_ask_price: Decimal,
    config: &StrategyConfig,
) -> bool {
    if !config.codex_sentinel_v1_mid_gap_premium_guard_enabled
        || primary_ask_price < config.codex_sentinel_v1_mid_gap_premium_entry_price
    {
        return false;
    }

    let target_gap_abs = context.target_gap_bps.abs();
    if target_gap_abs < config.codex_sentinel_v1_mid_gap_premium_min_target_gap_bps
        || target_gap_abs > config.codex_sentinel_v1_mid_gap_premium_max_target_gap_bps
    {
        return false;
    }

    let fresh_confirmation = codex_sentinel_v1_fresh_confirmation_bps(context, decision);
    let premium_quality = fresh_confirmation
        >= config.codex_sentinel_v1_mid_gap_premium_min_fresh_bps
        && decision.signal_strength_bps >= config.codex_sentinel_v1_mid_gap_premium_min_signal_bps
        && decision.aligned_flow_bps >= config.codex_sentinel_v1_mid_gap_premium_min_flow_bps;

    !premium_quality
}

fn codex_sentinel_v1_late_entry_override_allows(
    context: &BtcFiveMinuteContext,
    decision: &BonereaperStateV2Decision,
    primary_ask_price: Decimal,
    config: &StrategyConfig,
) -> bool {
    if !config.codex_sentinel_v1_late_entry_override_enabled {
        return false;
    }
    if context.seconds_left >= config.bonereaper_state_v2_min_seconds_left {
        return true;
    }
    if context.seconds_left < config.codex_sentinel_v1_late_entry_min_seconds_left {
        return false;
    }

    primary_ask_price <= config.codex_sentinel_v1_late_entry_max_entry_price
        && decision.signal_strength_bps >= config.codex_sentinel_v1_late_entry_min_signal_bps
        && codex_sentinel_v1_fresh_confirmation_bps(context, decision)
            >= config.codex_sentinel_v1_late_entry_min_fresh_bps
        && decision.aligned_flow_bps >= config.codex_sentinel_v1_late_entry_min_flow_bps
        && context.target_gap_bps.abs() >= config.codex_sentinel_v1_late_entry_min_target_gap_bps
}

fn codex_sentinel_v1_bad_window_guard_blocks(
    context: &BtcFiveMinuteContext,
    decision: &BonereaperStateV2Decision,
    primary_ask_price: Decimal,
    config: &StrategyConfig,
) -> bool {
    codex_sentinel_v1_counter_burst_guard_blocks(context, decision, primary_ask_price, config)
        || (config.codex_sentinel_v1_bad_window_guard_enabled
            && codex_sentinel_v1_confidence_score(context, decision, primary_ask_price, config)
                < config.codex_sentinel_v1_bad_window_min_score)
}

fn codex_sentinel_v1_counter_burst_guard_blocks(
    context: &BtcFiveMinuteContext,
    decision: &BonereaperStateV2Decision,
    primary_ask_price: Decimal,
    config: &StrategyConfig,
) -> bool {
    if !config.codex_sentinel_v1_counter_burst_guard_enabled
        || recent_target_cross(context, config).is_active()
        || primary_ask_price <= config.codex_sentinel_v1_counter_burst_max_entry_price
    {
        return false;
    }

    let aligned_burst_bps = aligned_move_bps(context.spot_move_1s_bps, decision.up_side);
    aligned_burst_bps <= -config.codex_sentinel_v1_counter_burst_min_bps
}

fn codex_sentinel_v1_confidence_score(
    context: &BtcFiveMinuteContext,
    decision: &BonereaperStateV2Decision,
    primary_ask_price: Decimal,
    config: &StrategyConfig,
) -> Decimal {
    let fresh_confirmation = codex_sentinel_v1_fresh_confirmation_bps(context, decision);
    let signal_points =
        confidence_component(decision.signal_strength_bps, Decimal::from(1_200_u32), 35);
    let flow_points = confidence_component(decision.aligned_flow_bps, Decimal::from(1_800_u32), 20);
    let fresh_points = confidence_component(fresh_confirmation, Decimal::new(25, 1), 20);
    let swing_points = confidence_component(decision.aligned_swing_bps, Decimal::new(30, 1), 15);
    let cross_bonus = if recent_target_cross(context, config).is_active() {
        Decimal::new(5, 0)
    } else {
        Decimal::ZERO
    };

    (signal_points
        + flow_points
        + fresh_points
        + swing_points
        + codex_sentinel_v1_entry_price_quality_score(primary_ask_price, config)
        + cross_bonus)
        .min(Decimal::from(100_u32))
        .round_dp(6)
}

fn codex_sentinel_v1_fresh_confirmation_bps(
    context: &BtcFiveMinuteContext,
    decision: &BonereaperStateV2Decision,
) -> Decimal {
    decision
        .aligned_micro_bps
        .max(aligned_move_bps(context.spot_move_1s_bps, decision.up_side))
        .max(aligned_move_bps(
            context.micro_acceleration_bps,
            decision.up_side,
        ))
        .max(Decimal::ZERO)
}

fn confidence_component(value: Decimal, full_value: Decimal, max_points: u32) -> Decimal {
    if full_value <= Decimal::ZERO || value <= Decimal::ZERO {
        return Decimal::ZERO;
    }

    ((value.min(full_value) / full_value) * Decimal::from(max_points)).round_dp(6)
}

fn codex_sentinel_v1_entry_price_quality_score(
    primary_ask_price: Decimal,
    config: &StrategyConfig,
) -> Decimal {
    if primary_ask_price <= config.codex_sentinel_v1_attack_max_entry_price {
        Decimal::new(10, 0)
    } else if primary_ask_price <= Decimal::new(65, 2) {
        Decimal::new(6, 0)
    } else if primary_ask_price <= config.codex_sentinel_v1_expensive_entry_price {
        Decimal::new(4, 0)
    } else if primary_ask_price <= config.codex_sentinel_v1_max_entry_price {
        Decimal::new(2, 0)
    } else {
        Decimal::ZERO
    }
}

fn bonereaper_state_v2_decision(
    context: &BtcFiveMinuteContext,
    trade_flow: Option<&TradeFlowSummary>,
    config: &StrategyConfig,
    require_probe_flow_floor: bool,
) -> Option<BonereaperStateV2Decision> {
    let target_side = primary_side_from_context(context);
    let target_gap_abs = context.target_gap_bps.abs();
    let target_metrics = bonereaper_state_v2_side_metrics(context, trade_flow, target_side);
    let counter_metrics = bonereaper_state_v2_side_metrics(context, trade_flow, !target_side);
    let min_signal = Decimal::from(config.bonereaper_state_v2_min_signal_bps);

    let target_ready = target_gap_abs >= config.bonereaper_state_v2_bias_min_target_gap_bps
        && target_metrics.aligned_swing_bps >= config.bonereaper_state_v2_min_spot_move_15s_bps
        && target_metrics.aligned_micro_bps >= config.bonereaper_state_v2_min_spot_move_5s_bps
        && target_metrics.aligned_flow_bps >= config.bonereaper_state_v2_min_aligned_flow_bps
        && target_metrics.signal_strength_bps >= min_signal;

    let counter_ready = target_gap_abs <= config.bonereaper_state_v2_flip_max_target_gap_bps
        && counter_metrics.aligned_swing_bps >= config.bonereaper_state_v2_min_spot_move_15s_bps
        && counter_metrics.aligned_micro_bps >= config.bonereaper_state_v2_min_spot_move_5s_bps
        && counter_metrics.aligned_flow_bps >= config.bonereaper_state_v2_min_aligned_flow_bps
        && counter_metrics.signal_strength_bps >= min_signal
        && counter_metrics.signal_strength_bps > target_metrics.signal_strength_bps + Decimal::ONE;

    let early_probe_ready = !target_ready
        && !counter_ready
        && target_gap_abs
            >= (config.bonereaper_state_v2_bias_min_target_gap_bps / Decimal::new(2, 0))
        && target_metrics.aligned_swing_bps >= config.bonereaper_state_v2_min_spot_move_15s_bps
        && target_metrics.aligned_micro_bps >= config.bonereaper_state_v2_min_spot_move_5s_bps
        && (!require_probe_flow_floor
            || target_metrics.aligned_flow_bps >= config.bonereaper_state_v2_min_aligned_flow_bps)
        && target_metrics.signal_strength_bps >= min_signal;

    let (up_side, metrics, counter_bias) = if target_ready || early_probe_ready {
        (target_side, target_metrics, false)
    } else if counter_ready {
        (!target_side, counter_metrics, true)
    } else {
        return None;
    };

    Some(BonereaperStateV2Decision {
        up_side,
        aligned_micro_bps: metrics.aligned_micro_bps,
        aligned_swing_bps: metrics.aligned_swing_bps,
        aligned_flow_bps: metrics.aligned_flow_bps,
        signal_strength_bps: metrics.signal_strength_bps,
        signal_tier: bonereaper_state_v2_signal_tier(context, metrics, up_side, config),
        counter_bias,
    })
}

fn bonereaper_state_signal_anchor_price(
    context: &BtcFiveMinuteContext,
    signal_strength_bps: Decimal,
    max_fair_price: Decimal,
    config: &StrategyConfig,
) -> Decimal {
    let elapsed_ratio = Decimal::from(elapsed_window_secs(context))
        / Decimal::from(context.target.window_secs().max(1));
    let state_confidence_bonus = (elapsed_ratio * Decimal::new(25, 3)).round_dp(6);
    (directional_signal_anchor_price(signal_strength_bps, config) + state_confidence_bonus)
        .min(max_fair_price)
        .round_dp(6)
}

fn micro_breakout_entry_notional_cap(
    available_market_capacity: Decimal,
    primary_ask_price: Decimal,
    signal_tier: MicroBreakoutSignalTier,
    full_size_allowed: bool,
    config: &StrategyConfig,
) -> Decimal {
    let full_cap = config
        .max_directional_notional_usdc
        .min(available_market_capacity);
    if full_cap <= Decimal::ZERO {
        return Decimal::ZERO;
    }

    let configured_cap = match signal_tier {
        MicroBreakoutSignalTier::Strong if full_size_allowed => full_cap,
        MicroBreakoutSignalTier::Strong
            if primary_ask_price <= config.micro_breakout_expensive_entry_price =>
        {
            config.micro_breakout_strong_notional_usdc
        }
        MicroBreakoutSignalTier::Normal
            if primary_ask_price <= config.micro_breakout_expensive_entry_price =>
        {
            config.micro_breakout_normal_notional_usdc
        }
        MicroBreakoutSignalTier::Normal
        | MicroBreakoutSignalTier::Weak
        | MicroBreakoutSignalTier::Strong => config.micro_breakout_weak_notional_usdc,
    };

    configured_cap.min(full_cap).round_dp(6)
}

fn has_aligned_fifteen_second_momentum(
    context: &BtcFiveMinuteContext,
    config: &StrategyConfig,
) -> bool {
    moves_align(context.spot_move_bps, context.spot_move_15s_bps)
        && bps_fixed_abs(context.spot_move_15s_bps)
            >= bps_threshold_to_fixed_abs(config.micro_breakout_min_spot_move_5s_bps)
}

fn has_aligned_positive_acceleration(context: &BtcFiveMinuteContext) -> bool {
    bps_fixed_abs(context.micro_acceleration_bps) > 0
        && moves_align(context.spot_move_bps, context.micro_acceleration_bps)
}

fn micro_burst_feature_enabled(config: &StrategyConfig) -> bool {
    config.micro_breakout_min_spot_move_1s_bps > Decimal::ZERO
        || config.micro_breakout_signal_burst_multiplier > Decimal::ZERO
        || config.micro_breakout_strong_signal_min_spot_move_1s_bps > Decimal::ZERO
        || config.micro_breakout_max_burst_to_micro_ratio > Decimal::ZERO
}

fn has_aligned_micro_burst(context: &BtcFiveMinuteContext, config: &StrategyConfig) -> bool {
    if config.micro_breakout_min_spot_move_1s_bps <= Decimal::ZERO
        && config.micro_breakout_max_burst_to_micro_ratio <= Decimal::ZERO
    {
        return true;
    }

    moves_align(context.spot_move_bps, context.spot_move_1s_bps)
        && has_persistent_micro_burst(context, config)
        && (config.micro_breakout_min_spot_move_1s_bps <= Decimal::ZERO
            || bps_fixed_abs(context.spot_move_1s_bps)
                >= bps_threshold_to_fixed_abs(config.micro_breakout_min_spot_move_1s_bps))
}

fn has_persistent_micro_burst(context: &BtcFiveMinuteContext, config: &StrategyConfig) -> bool {
    if config.micro_breakout_max_burst_to_micro_ratio <= Decimal::ZERO {
        return true;
    }

    let burst_move_abs = context.spot_move_1s_bps.abs();
    let micro_move_abs = context.spot_move_5s_bps.abs();
    burst_move_abs > Decimal::ZERO
        && micro_move_abs > Decimal::ZERO
        && moves_align(context.spot_move_1s_bps, context.spot_move_5s_bps)
        && burst_move_abs
            <= (micro_move_abs * config.micro_breakout_max_burst_to_micro_ratio).round_dp(6)
}

fn recent_target_cross(
    context: &BtcFiveMinuteContext,
    config: &StrategyConfig,
) -> RecentTargetCross {
    if config.micro_breakout_target_cross_min_gap_bps <= Decimal::ZERO
        || context.target_gap_bps.abs() < config.micro_breakout_target_cross_min_gap_bps
    {
        return RecentTargetCross::None;
    }

    if crossed_target(
        context.micro_burst_reference_price,
        context.current_spot_price,
        context.target_price,
    ) && moves_align(context.target_gap_bps, context.spot_move_1s_bps)
    {
        RecentTargetCross::OneSecond
    } else if crossed_target(
        context.micro_reference_price,
        context.current_spot_price,
        context.target_price,
    ) && moves_align(context.target_gap_bps, context.spot_move_5s_bps)
    {
        RecentTargetCross::FiveSecond
    } else {
        RecentTargetCross::None
    }
}

fn crossed_target(reference_price: Decimal, current_price: Decimal, target_price: Decimal) -> bool {
    match current_price.cmp(&target_price) {
        Ordering::Greater => reference_price <= target_price,
        Ordering::Less => reference_price >= target_price,
        Ordering::Equal => false,
    }
}

fn aligned_trade_flow_bps(
    context: &BtcFiveMinuteContext,
    trade_flow: Option<&TradeFlowSummary>,
) -> Decimal {
    trade_flow.map_or(Decimal::ZERO, |flow| {
        flow.aligned_imbalance_bps(aligned_outcome_label(context))
    })
}

fn micro_breakout_full_size_allowed(
    primary_ask_price: Decimal,
    five_second_move_abs: Decimal,
    aligned_flow_bps: Decimal,
    positive_acceleration_confirmed: bool,
    signal_tier: MicroBreakoutSignalTier,
    config: &StrategyConfig,
) -> bool {
    signal_tier == MicroBreakoutSignalTier::Strong
        && primary_ask_price <= config.micro_breakout_full_size_max_entry_price
        && five_second_move_abs > Decimal::ZERO
        && positive_acceleration_confirmed
        && aligned_flow_bps >= config.directional_strong_signal_min_trade_flow_bps
}

fn micro_breakout_signal_bps(
    context: &BtcFiveMinuteContext,
    trade_flow: Option<&TradeFlowSummary>,
    config: &StrategyConfig,
) -> Decimal {
    let aligned_outcome_label = aligned_outcome_label(context);
    let spot_move_abs = context.spot_move_bps.abs();
    let burst_move_abs = context.spot_move_1s_bps.abs();
    let micro_move_abs = context.spot_move_5s_bps.abs();
    let flow_adjustment = trade_flow.map_or(Decimal::ZERO, |flow| {
        (flow.aligned_imbalance_bps(aligned_outcome_label) * config.directional_trade_flow_weight)
            .round_dp(6)
    });

    let boosted_micro_move =
        (micro_move_abs * config.micro_breakout_signal_boost_multiplier).round_dp(6);
    let boosted_burst_move =
        (burst_move_abs * config.micro_breakout_signal_burst_multiplier).round_dp(6);
    let alignment_penalty = if moves_align(context.spot_move_bps, context.spot_move_5s_bps) {
        Decimal::ZERO
    } else {
        boosted_micro_move
    };
    let burst_alignment_penalty = if moves_align(context.spot_move_bps, context.spot_move_1s_bps) {
        Decimal::ZERO
    } else {
        boosted_burst_move
    };

    (spot_move_abs + boosted_micro_move + boosted_burst_move + flow_adjustment
        - alignment_penalty
        - burst_alignment_penalty)
        .max(Decimal::ZERO)
}

fn elapsed_window_secs(context: &BtcFiveMinuteContext) -> i64 {
    (context.target.window_secs() - context.seconds_left).clamp(0, context.target.window_secs())
}

fn directional_micro_signal_adjustment(
    context: &BtcFiveMinuteContext,
    config: &StrategyConfig,
) -> Decimal {
    let micro_move_abs = context.spot_move_5s_bps.abs();
    let burst_move_abs = context.spot_move_1s_bps.abs();
    let weighted_micro_move = if micro_move_abs == Decimal::ZERO
        || config.directional_micro_signal_weight == Decimal::ZERO
    {
        Decimal::ZERO
    } else {
        let weighted = (micro_move_abs * config.directional_micro_signal_weight).round_dp(6);
        if moves_align(context.spot_move_bps, context.spot_move_5s_bps) {
            weighted
        } else {
            -weighted
        }
    };
    let weighted_burst_move = if burst_move_abs == Decimal::ZERO
        || config.directional_micro_burst_weight == Decimal::ZERO
    {
        Decimal::ZERO
    } else {
        let weighted = (burst_move_abs * config.directional_micro_burst_weight).round_dp(6);
        if moves_align(context.spot_move_bps, context.spot_move_1s_bps) {
            weighted
        } else {
            -weighted
        }
    };

    weighted_micro_move + weighted_burst_move
}

fn moves_align(window_move_bps: Decimal, micro_move_bps: Decimal) -> bool {
    fixed_moves_align(bps_to_fixed(window_move_bps), bps_to_fixed(micro_move_bps))
}

fn fixed_moves_align(window_move_bps: i64, micro_move_bps: i64) -> bool {
    window_move_bps == 0
        || micro_move_bps == 0
        || (window_move_bps > 0 && micro_move_bps > 0)
        || (window_move_bps < 0 && micro_move_bps < 0)
}

fn bps_fixed_abs(value: Decimal) -> i64 {
    fixed_abs(bps_to_fixed(value))
}

fn fixed_abs(value: i64) -> i64 {
    value.checked_abs().unwrap_or(i64::MAX)
}

fn u32_bps_to_fixed_abs(value: u32) -> i64 {
    i64::from(value).saturating_mul(BPS_FIXED_SCALE)
}

fn bps_threshold_to_fixed_abs(value: Decimal) -> i64 {
    fixed_abs(bps_to_fixed(value))
}

fn bps_to_fixed(value: Decimal) -> i64 {
    decimal_to_fixed(value, BPS_FIXED_SCALE)
}

fn decimal_to_fixed(value: Decimal, scale: i64) -> i64 {
    let scaled = (value * Decimal::from(scale)).round();
    scaled.to_i64().unwrap_or_else(|| {
        if scaled.is_sign_negative() {
            i64::MIN
        } else {
            i64::MAX
        }
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::{Duration, Utc};
    use rust_decimal::Decimal;

    use crate::config::StrategyConfig;
    use crate::models::{
        BinaryMarket, BookLevel, MarketTarget, OpportunityKind, OrderBook, TargetPriceSource,
    };
    use crate::services::binance::BtcFiveMinuteContext;

    use super::{
        BonereaperStateV2Decision, BonereaperStateV2QualityBlock, BonereaperStateV2SignalTier,
        BundleArbitrageStrategy, RecentTargetCross, TradeFlowSummary,
        bonereaper_state_v2_quality_guard_block, codex_breakout_v1_allows,
        codex_scalp_probe_v1_allows, codex_scalp_probe_v1_min_expected_profit_usdc,
        codex_sentinel_v1_aggressive_continuation_allows,
        codex_sentinel_v1_bad_window_guard_blocks, codex_sentinel_v1_confidence_score,
        codex_sentinel_v1_discount_value_lane_allows, codex_sentinel_v1_entry_notional_cap,
        codex_sentinel_v1_entry_spread_guard_blocks,
        codex_sentinel_v1_expensive_entry_guard_blocks,
        codex_sentinel_v1_late_entry_override_allows,
        codex_sentinel_v1_late_window_value_guard_blocks,
        codex_sentinel_v1_live_quote_age_guard_blocks, codex_sentinel_v1_low_flow_guard_blocks,
        codex_sentinel_v1_mid_gap_premium_guard_blocks, codex_sentinel_v1_no_chase_guard_blocks,
        codex_sentinel_v1_premium_entry_guard_blocks, codex_sentinel_v1_quality_floor_blocks,
        codex_sentinel_v1_stale_micro_guard_blocks, codex_sentinel_v1_target_allowed,
        recent_target_cross,
    };

    fn decimal(value: &str) -> Decimal {
        value.parse().expect("decimal literal is valid")
    }

    fn strategy_config() -> StrategyConfig {
        StrategyConfig {
            market_targets: vec![MarketTarget::Btc5m],
            enable_bundle: true,
            min_edge_bps: 10,
            assumed_fee_bps: 0,
            min_spot_move_bps: 5,
            min_liquidity_usdc: decimal("100"),
            min_top_of_book_shares: decimal("10"),
            max_bundle_notional_usdc: decimal("100"),
            max_directional_notional_usdc: decimal("75"),
            max_market_notional_usdc: decimal("200"),
            min_seconds_left: 20,
            max_seconds_left: 299,
            min_minutes_to_expiry: 15,
            max_markets: 10,
            enable_directional: true,
            directional_min_spot_move_bps: 12,
            directional_min_signal_bps: 16,
            directional_min_velocity_bps_per_minute: 6,
            directional_soft_entry_min_notional_usdc: decimal("25"),
            directional_soft_entry_max_notional_usdc: decimal("35"),
            directional_strong_signal_min_spot_move_5s_bps: decimal("1.5"),
            directional_strong_signal_min_trade_flow_bps: decimal("1.5"),
            directional_soft_entry_signal_window_bps: decimal("6.0"),
            directional_min_model_edge_bps: 15,
            directional_confidence_bps_per_spot_bps: 25,
            directional_projection_cap_multiplier: decimal("2.0"),
            directional_trade_flow_weight: decimal("0.35"),
            directional_micro_signal_weight: decimal("0.50"),
            directional_micro_burst_weight: Decimal::ZERO,
            directional_require_hedge_for_soft_entry: false,
            enable_micro_breakout: true,
            micro_breakout_min_spot_move_bps: 2,
            micro_breakout_min_spot_move_5s_bps: decimal("1.0"),
            micro_breakout_min_spot_move_1s_bps: Decimal::ZERO,
            micro_breakout_min_signal_bps: 4,
            micro_breakout_signal_boost_multiplier: decimal("2.0"),
            micro_breakout_signal_burst_multiplier: Decimal::ZERO,
            micro_breakout_max_burst_to_micro_ratio: Decimal::ZERO,
            micro_breakout_target_cross_min_gap_bps: Decimal::ZERO,
            micro_breakout_target_cross_signal_boost_bps: Decimal::ZERO,
            micro_breakout_max_entry_price: decimal("0.68"),
            micro_breakout_max_average_price_drift: Decimal::ZERO,
            micro_breakout_min_elapsed_window_secs: 0,
            micro_breakout_weak_notional_usdc: decimal("5"),
            micro_breakout_normal_notional_usdc: decimal("7"),
            micro_breakout_strong_notional_usdc: decimal("10"),
            micro_breakout_expensive_entry_price: decimal("0.75"),
            micro_breakout_expensive_entry_requires_strong_tier: false,
            micro_breakout_full_size_max_entry_price: decimal("0.70"),
            micro_breakout_strong_signal_min_spot_move_5s_bps: decimal("1.5"),
            micro_breakout_strong_signal_min_spot_move_1s_bps: Decimal::ZERO,
            micro_breakout_strong_signal_min_spot_move_15s_bps: decimal("1.5"),
            enable_target_state_v1: false,
            target_state_min_elapsed_window_secs: 120,
            target_state_max_seconds_left: 150,
            target_state_min_target_gap_bps: decimal("5"),
            target_state_min_signal_bps: 10,
            target_state_min_spot_move_15s_bps: decimal("0.8"),
            target_state_min_aligned_flow_bps: decimal("0.2"),
            target_state_max_entry_price: decimal("0.74"),
            target_state_normal_notional_usdc: decimal("12"),
            target_state_strong_notional_usdc: decimal("20"),
            target_state_strong_gap_bps: decimal("12"),
            enable_bonereaper_state_v1: false,
            bonereaper_state_min_elapsed_window_secs: 10,
            bonereaper_state_max_seconds_left: 290,
            bonereaper_state_min_target_gap_bps: decimal("1.5"),
            bonereaper_state_min_signal_bps: 4,
            bonereaper_state_min_spot_move_15s_bps: decimal("0.10"),
            bonereaper_state_min_spot_move_5s_bps: decimal("0.05"),
            bonereaper_state_min_aligned_flow_bps: Decimal::ZERO,
            bonereaper_state_max_entry_price: decimal("0.85"),
            bonereaper_state_normal_notional_usdc: decimal("12"),
            bonereaper_state_strong_notional_usdc: decimal("18"),
            bonereaper_state_strong_gap_bps: decimal("4.5"),
            bonereaper_state_strong_flow_bps: decimal("0.6"),
            enable_bonereaper_state_v2: false,
            enable_bonereaper_state_guarded: false,
            enable_codex_sentinel_v1: false,
            codex_sentinel_v1_mid_signal_guard_enabled: true,
            codex_sentinel_v1_mid_signal_min_bps: decimal("2.8"),
            codex_sentinel_v1_mid_signal_max_bps: decimal("3.6"),
            codex_sentinel_v1_mid_signal_min_confirmation_bps: decimal("0.05"),
            codex_sentinel_v1_max_entry_price: decimal("0.76"),
            codex_sentinel_v1_live_quote_age_guard_enabled: false,
            codex_sentinel_v1_max_live_quote_age_ms: 1000,
            codex_sentinel_v1_entry_spread_guard_enabled: false,
            codex_sentinel_v1_max_entry_spread: decimal("0.08"),
            codex_sentinel_v1_stale_micro_guard_enabled: true,
            codex_sentinel_v1_stale_micro_max_confirmation_bps: decimal("0.05"),
            codex_sentinel_v1_stale_micro_discount_max_entry_price: decimal("0.55"),
            codex_sentinel_v1_stale_micro_discount_min_signal_bps: Decimal::from(450),
            codex_sentinel_v1_stale_micro_discount_min_flow_bps: Decimal::from(700),
            codex_sentinel_v1_stale_micro_min_signal_bps: Decimal::from(800),
            codex_sentinel_v1_stale_micro_min_flow_bps: Decimal::from(1400),
            codex_sentinel_v1_stale_micro_max_non_discount_entry_price: decimal("0.65"),
            codex_sentinel_v1_stale_micro_min_swing_bps: decimal("0.75"),
            codex_sentinel_v1_stale_micro_min_target_gap_bps: decimal("0.75"),
            codex_sentinel_v1_expensive_entry_guard_enabled: true,
            codex_sentinel_v1_expensive_entry_price: decimal("0.65"),
            codex_sentinel_v1_expensive_min_micro_bps: decimal("1.25"),
            codex_sentinel_v1_expensive_min_swing_bps: decimal("1.25"),
            codex_sentinel_v1_premium_entry_guard_enabled: false,
            codex_sentinel_v1_premium_entry_price: decimal("0.55"),
            codex_sentinel_v1_premium_min_signal_bps: Decimal::from(800),
            codex_sentinel_v1_premium_min_flow_bps: Decimal::from(1400),
            codex_sentinel_v1_premium_min_fresh_bps: decimal("1.25"),
            codex_sentinel_v1_aggressive_continuation_enabled: false,
            codex_sentinel_v1_aggressive_continuation_max_entry_price: decimal("0.62"),
            codex_sentinel_v1_aggressive_continuation_min_target_gap_bps: decimal("6.00"),
            codex_sentinel_v1_aggressive_continuation_min_signal_bps: Decimal::from(1500),
            codex_sentinel_v1_aggressive_continuation_min_flow_bps: Decimal::from(2200),
            codex_sentinel_v1_aggressive_continuation_min_fresh_bps: decimal("3.50"),
            codex_sentinel_v1_aggressive_continuation_min_swing_bps: decimal("3.50"),
            codex_sentinel_v1_aggressive_continuation_max_quote_age_ms: 750,
            codex_breakout_v1_enabled: false,
            codex_breakout_v1_required: false,
            codex_breakout_v1_max_entry_price: decimal("0.58"),
            codex_breakout_v1_max_book_age_ms: 750,
            codex_breakout_v1_max_spread_bps: decimal("0.20"),
            codex_breakout_v1_min_score_bps: Decimal::from(3000),
            codex_breakout_v1_min_depth_imbalance_bps: Decimal::from(1800),
            codex_breakout_v1_min_microprice_bps: decimal("0.0003"),
            codex_breakout_v1_min_fresh_bps: decimal("1.00"),
            codex_breakout_v1_min_target_gap_bps: decimal("1.00"),
            codex_breakout_v1_min_signal_bps: Decimal::ZERO,
            codex_breakout_v1_min_flow_bps: Decimal::ZERO,
            codex_sentinel_v1_discount_value_lane_enabled: false,
            codex_sentinel_v1_discount_value_max_entry_price: decimal("0.50"),
            codex_sentinel_v1_discount_value_max_book_age_ms: 750,
            codex_sentinel_v1_discount_value_max_exchange_spread_bps: decimal("3.00"),
            codex_sentinel_v1_discount_value_min_target_gap_bps: decimal("1.20"),
            codex_sentinel_v1_discount_value_min_fresh_bps: decimal("1.25"),
            codex_sentinel_v1_discount_value_min_swing_bps: decimal("1.00"),
            codex_sentinel_v1_discount_value_min_signal_bps: Decimal::from(650),
            codex_sentinel_v1_discount_value_min_flow_bps: Decimal::from(700),
            codex_sentinel_v1_discount_value_min_top_imbalance_bps: Decimal::from(500),
            codex_sentinel_v1_discount_value_min_depth_imbalance_bps: Decimal::from(700),
            codex_sentinel_v1_discount_value_min_microprice_bps: decimal("0.0003"),
            enable_codex_scalp_probe_v1: false,
            codex_scalp_probe_v1_raw_ablation_enabled: false,
            codex_scalp_probe_v1_raw_light_enabled: false,
            codex_scalp_probe_v1_min_entry_price: decimal("0.45"),
            codex_scalp_probe_v1_max_entry_price: decimal("0.56"),
            codex_scalp_probe_v1_max_entry_spread: decimal("0.10"),
            codex_scalp_probe_v1_min_elapsed_window_secs: 8,
            codex_scalp_probe_v1_max_seconds_left: 270,
            codex_scalp_probe_v1_min_seconds_left: 210,
            codex_scalp_probe_v1_max_book_age_ms: 750,
            codex_scalp_probe_v1_max_exchange_spread_bps: decimal("3.00"),
            codex_scalp_probe_v1_min_target_gap_bps: decimal("1.50"),
            codex_scalp_probe_v1_min_fresh_bps: decimal("0.70"),
            codex_scalp_probe_v1_min_signal_bps: Decimal::from(450),
            codex_scalp_probe_v1_min_flow_bps: Decimal::from(700),
            codex_scalp_probe_v1_min_top_imbalance_bps: Decimal::from(900),
            codex_scalp_probe_v1_min_depth_imbalance_bps: Decimal::from(1000),
            codex_scalp_probe_v1_min_radar_score_bps: Decimal::from(2200),
            codex_scalp_probe_v1_notional_usdc: decimal("3"),
            codex_scalp_probe_v1_min_expected_profit_usdc: decimal("0.12"),
            codex_scalp_probe_v1_bnb_pressure_enabled: false,
            codex_scalp_probe_v1_bnb_pressure_max_entry_price: decimal("0.58"),
            codex_scalp_probe_v1_bnb_pressure_max_book_age_ms: 400,
            codex_scalp_probe_v1_bnb_pressure_min_target_gap_bps: decimal("0.70"),
            codex_scalp_probe_v1_bnb_pressure_min_fresh_bps: decimal("0.10"),
            codex_scalp_probe_v1_bnb_pressure_min_top_imbalance_bps: Decimal::from(1500),
            codex_scalp_probe_v1_bnb_pressure_min_depth_imbalance_bps: Decimal::from(1300),
            codex_scalp_probe_v1_bnb_pressure_min_expected_profit_usdc: decimal("0.05"),
            codex_sentinel_v1_no_chase_guard_enabled: false,
            codex_sentinel_v1_no_chase_entry_price: decimal("0.62"),
            codex_sentinel_v1_no_chase_min_seconds_left: 240,
            codex_sentinel_v1_no_chase_allow_min_target_gap_bps: decimal("8.00"),
            codex_sentinel_v1_no_chase_allow_min_fresh_bps: decimal("4.00"),
            codex_sentinel_v1_no_chase_allow_min_signal_bps: Decimal::from(2500),
            codex_sentinel_v1_no_chase_allow_min_flow_bps: Decimal::from(4000),
            codex_sentinel_v1_quality_floor_enabled: false,
            codex_sentinel_v1_quality_floor_min_target_gap_bps: Decimal::ZERO,
            codex_sentinel_v1_quality_floor_mid_gap_max_bps: Decimal::ZERO,
            codex_sentinel_v1_quality_floor_mid_gap_min_signal_bps: Decimal::ZERO,
            codex_sentinel_v1_quality_floor_mid_gap_min_flow_bps: Decimal::ZERO,
            codex_sentinel_v1_mid_gap_premium_guard_enabled: false,
            codex_sentinel_v1_mid_gap_premium_min_target_gap_bps: decimal("1.50"),
            codex_sentinel_v1_mid_gap_premium_max_target_gap_bps: decimal("3.00"),
            codex_sentinel_v1_mid_gap_premium_entry_price: decimal("0.56"),
            codex_sentinel_v1_mid_gap_premium_min_signal_bps: Decimal::from(800),
            codex_sentinel_v1_mid_gap_premium_min_flow_bps: Decimal::from(1200),
            codex_sentinel_v1_mid_gap_premium_min_fresh_bps: decimal("1.25"),
            codex_sentinel_v1_attack_size_enabled: false,
            codex_sentinel_v1_attack_notional_usdc: decimal("10"),
            codex_sentinel_v1_attack_min_signal_bps: Decimal::from(650),
            codex_sentinel_v1_attack_min_flow_bps: Decimal::from(700),
            codex_sentinel_v1_attack_min_confirmation_bps: decimal("0.5"),
            codex_sentinel_v1_attack_max_entry_price: decimal("0.60"),
            codex_sentinel_v1_bad_window_guard_enabled: false,
            codex_sentinel_v1_bad_window_min_score: Decimal::from(32),
            codex_sentinel_v1_confidence_sizing_enabled: false,
            codex_sentinel_v1_confidence_min_score: Decimal::from(40),
            codex_sentinel_v1_confidence_max_multiplier: Decimal::ONE,
            codex_sentinel_v1_low_flow_guard_enabled: false,
            codex_sentinel_v1_low_flow_max_flow_bps: Decimal::from(100),
            codex_sentinel_v1_low_flow_allow_min_signal_bps: Decimal::from(40),
            codex_sentinel_v1_low_flow_allow_min_fresh_bps: decimal("3.00"),
            codex_sentinel_v1_low_flow_allow_min_swing_bps: decimal("3.00"),
            codex_sentinel_v1_low_flow_allow_max_entry_price: decimal("0.58"),
            codex_sentinel_v1_counter_burst_guard_enabled: false,
            codex_sentinel_v1_counter_burst_min_bps: decimal("0.75"),
            codex_sentinel_v1_counter_burst_max_entry_price: decimal("0.55"),
            codex_sentinel_v1_late_entry_override_enabled: false,
            codex_sentinel_v1_late_entry_min_seconds_left: 60,
            codex_sentinel_v1_late_entry_max_entry_price: decimal("0.62"),
            codex_sentinel_v1_late_entry_min_signal_bps: Decimal::from(850),
            codex_sentinel_v1_late_entry_min_fresh_bps: decimal("1.50"),
            codex_sentinel_v1_late_entry_min_flow_bps: Decimal::ZERO,
            codex_sentinel_v1_late_entry_min_target_gap_bps: decimal("1.50"),
            codex_sentinel_v1_late_window_value_guard_enabled: false,
            codex_sentinel_v1_late_window_max_seconds_left: 180,
            codex_sentinel_v1_late_window_max_entry_price: decimal("0.62"),
            codex_sentinel_v1_late_window_allow_min_signal_bps: Decimal::from(1600),
            codex_sentinel_v1_late_window_allow_min_fresh_bps: decimal("2.00"),
            codex_sentinel_v1_late_window_allow_min_flow_bps: Decimal::from(2500),
            codex_sentinel_v1_late_window_allow_min_target_gap_bps: decimal("3.00"),
            bonereaper_state_v2_min_elapsed_window_secs: 8,
            bonereaper_state_v2_max_seconds_left: 290,
            bonereaper_state_v2_min_seconds_left: 0,
            bonereaper_state_v2_bias_min_target_gap_bps: decimal("1.4"),
            bonereaper_state_v2_flip_max_target_gap_bps: decimal("2.6"),
            bonereaper_state_v2_min_signal_bps: 4,
            bonereaper_state_v2_min_spot_move_15s_bps: decimal("0.10"),
            bonereaper_state_v2_min_spot_move_5s_bps: decimal("0.05"),
            bonereaper_state_v2_min_aligned_flow_bps: Decimal::ZERO,
            bonereaper_state_v2_max_entry_price: decimal("0.88"),
            bonereaper_state_v2_max_fair_price: decimal("0.99"),
            bonereaper_state_v2_probe_notional_usdc: decimal("8"),
            bonereaper_state_v2_normal_notional_usdc: decimal("15"),
            bonereaper_state_v2_strong_notional_usdc: decimal("25"),
            bonereaper_state_v2_strong_gap_bps: decimal("4.5"),
            bonereaper_state_v2_strong_flow_bps: decimal("0.6"),
            bonereaper_state_v2_min_expected_profit_usdc: Decimal::ZERO,
            bonereaper_state_v2_micro_alignment_guard_enabled: false,
            bonereaper_state_v2_max_counter_1s_bps: Decimal::ZERO,
            bonereaper_state_v2_max_counter_5s_bps: Decimal::ZERO,
            bonereaper_state_v2_early_window_guard_enabled: false,
            bonereaper_state_v2_early_window_max_seconds_left: 240,
            bonereaper_state_v2_early_window_min_fresh_bps: decimal("0.75"),
            bonereaper_state_v2_early_window_min_swing_bps: decimal("0.75"),
            bonereaper_state_v2_early_window_min_signal_bps: Decimal::from(800),
            bonereaper_state_v2_high_gap_guard_enabled: false,
            bonereaper_state_v2_high_gap_min_target_gap_bps: decimal("3.00"),
            bonereaper_state_v2_high_gap_max_entry_price: decimal("0.56"),
            bonereaper_state_v2_high_gap_min_fresh_bps: decimal("1.25"),
            bonereaper_state_v2_high_gap_min_swing_bps: decimal("1.25"),
            bonereaper_state_v2_high_gap_min_signal_bps: Decimal::from(1000),
            bonereaper_state_v2_mid_gap_guard_enabled: false,
            bonereaper_state_v2_mid_gap_min_target_gap_bps: decimal("1.50"),
            bonereaper_state_v2_mid_gap_max_target_gap_bps: decimal("3.00"),
            bonereaper_state_v2_mid_gap_max_entry_price: decimal("0.50"),
            bonereaper_state_v2_mid_gap_min_seconds_left: 120,
            bonereaper_state_v2_mid_gap_min_fresh_bps: decimal("1.25"),
            bonereaper_state_v2_mid_gap_min_signal_bps: Decimal::from(800),
            bonereaper_state_v2_mid_gap_min_flow_bps: Decimal::from(1500),
            bonereaper_state_v2_low_gap_guard_enabled: false,
            bonereaper_state_v2_low_gap_max_target_gap_bps: decimal("1.50"),
            bonereaper_state_v2_low_gap_max_entry_price: decimal("0.45"),
            bonereaper_state_v2_low_gap_min_seconds_left: 120,
            bonereaper_state_v2_low_gap_allow_min_fresh_bps: decimal("1.50"),
            bonereaper_state_v2_low_gap_allow_min_signal_bps: Decimal::from(2000),
            bonereaper_state_v2_low_gap_allow_min_flow_bps: Decimal::from(3000),
            bonereaper_state_v2_early_expensive_guard_enabled: false,
            bonereaper_state_v2_early_expensive_min_seconds_left: 240,
            bonereaper_state_v2_early_expensive_entry_price: decimal("0.56"),
            bonereaper_state_v2_early_expensive_allow_min_target_gap_bps: decimal("3.00"),
            bonereaper_state_v2_early_expensive_allow_min_fresh_bps: decimal("2.00"),
            bonereaper_state_v2_early_expensive_allow_min_signal_bps: Decimal::from(2500),
            bonereaper_state_v2_early_expensive_allow_min_flow_bps: Decimal::from(4000),
            directional_execution_slippage_bps: 15,
            directional_max_fair_price: decimal("0.72"),
            directional_max_entry_price: decimal("0.60"),
            enable_tail_hedge: true,
            tail_hedge_ratio: decimal("0.25"),
            tail_hedge_min_spot_move_bps: 10,
            tail_hedge_min_signal_bps: 14,
            tail_hedge_min_velocity_bps_per_minute: 6,
            tail_hedge_max_opposite_price: decimal("0.25"),
            tail_hedge_max_bundle_cost: decimal("0.99"),
            tail_hedge_open_window_secs: 20,
        }
    }

    fn test_codex_decision(
        aligned_micro_bps: &str,
        aligned_swing_bps: &str,
    ) -> BonereaperStateV2Decision {
        BonereaperStateV2Decision {
            up_side: true,
            aligned_micro_bps: decimal(aligned_micro_bps),
            aligned_swing_bps: decimal(aligned_swing_bps),
            aligned_flow_bps: Decimal::from(5_000),
            signal_strength_bps: Decimal::from(5_000),
            signal_tier: BonereaperStateV2SignalTier::Normal,
            counter_bias: false,
        }
    }

    fn test_codex_context(target_gap_bps: &str, spot_move_1s_bps: &str) -> BtcFiveMinuteContext {
        BtcFiveMinuteContext {
            target: MarketTarget::Btc5m,
            interval_open_price: decimal("100"),
            target_price: decimal("100"),
            target_price_source: TargetPriceSource::BinanceWindowOpenFallback,
            target_gap_bps: decimal(target_gap_bps),
            current_spot_price: decimal("100"),
            current_spot_source: "test-fixture".to_owned(),
            current_spot_event_age_ms: None,
            current_spot_received_age_ms: None,
            current_spot_quote_points: None,
            exchange_book_age_ms: None,
            exchange_book_top_imbalance_bps: Decimal::ZERO,
            exchange_book_depth_imbalance_bps: Decimal::ZERO,
            exchange_book_microprice_bps: Decimal::ZERO,
            exchange_book_spread_bps: Decimal::ZERO,
            micro_burst_reference_price: decimal("100"),
            micro_reference_price: decimal("100"),
            spot_move_bps: decimal(target_gap_bps),
            spot_move_1s_bps: decimal(spot_move_1s_bps),
            spot_move_5s_bps: Decimal::ZERO,
            spot_move_15s_bps: decimal("1.00"),
            micro_acceleration_bps: Decimal::ZERO,
            dominant_outcome: "Up".to_owned(),
            seconds_left: 240,
        }
    }

    #[test]
    fn bonereaper_state_v2_quality_guard_blocks_counter_micro_burst() {
        let mut config = strategy_config();
        config.bonereaper_state_v2_micro_alignment_guard_enabled = true;
        config.bonereaper_state_v2_max_counter_1s_bps = decimal("0.30");
        config.bonereaper_state_v2_max_counter_5s_bps = decimal("0.10");
        let decision = test_codex_decision("0.40", "1.00");
        let context = test_codex_context("2.00", "-0.45");

        assert_eq!(
            bonereaper_state_v2_quality_guard_block(&context, &decision, decimal("0.55"), &config),
            Some(BonereaperStateV2QualityBlock::CounterMicro)
        );
    }

    #[test]
    fn bonereaper_state_v2_quality_guard_blocks_weak_early_window() {
        let mut config = strategy_config();
        config.bonereaper_state_v2_early_window_guard_enabled = true;
        config.bonereaper_state_v2_early_window_max_seconds_left = 240;
        config.bonereaper_state_v2_early_window_min_fresh_bps = decimal("0.75");
        config.bonereaper_state_v2_early_window_min_swing_bps = decimal("0.75");
        config.bonereaper_state_v2_early_window_min_signal_bps = Decimal::from(800);
        let decision = BonereaperStateV2Decision {
            signal_strength_bps: Decimal::from(900),
            ..test_codex_decision("0.30", "1.00")
        };
        let mut context = test_codex_context("2.00", "0.20");
        context.seconds_left = 255;

        assert_eq!(
            bonereaper_state_v2_quality_guard_block(&context, &decision, decimal("0.55"), &config),
            Some(BonereaperStateV2QualityBlock::EarlyWindow)
        );
    }

    #[test]
    fn bonereaper_state_v2_quality_guard_blocks_high_gap_chase() {
        let mut config = strategy_config();
        config.bonereaper_state_v2_high_gap_guard_enabled = true;
        config.bonereaper_state_v2_high_gap_min_target_gap_bps = decimal("3.00");
        config.bonereaper_state_v2_high_gap_max_entry_price = decimal("0.56");
        config.bonereaper_state_v2_high_gap_min_fresh_bps = decimal("1.25");
        config.bonereaper_state_v2_high_gap_min_swing_bps = decimal("1.25");
        config.bonereaper_state_v2_high_gap_min_signal_bps = Decimal::from(1000);
        let decision = BonereaperStateV2Decision {
            signal_strength_bps: Decimal::from(1_200),
            ..test_codex_decision("1.30", "1.30")
        };
        let context = test_codex_context("3.20", "1.30");

        assert_eq!(
            bonereaper_state_v2_quality_guard_block(&context, &decision, decimal("0.58"), &config),
            Some(BonereaperStateV2QualityBlock::HighGap)
        );
    }

    #[test]
    fn bonereaper_state_v2_quality_guard_blocks_mid_gap_without_discount() {
        let mut config = strategy_config();
        config.bonereaper_state_v2_mid_gap_guard_enabled = true;
        let decision = BonereaperStateV2Decision {
            signal_strength_bps: Decimal::from(1_200),
            aligned_flow_bps: Decimal::from(2_000),
            ..test_codex_decision("1.50", "1.50")
        };
        let mut context = test_codex_context("2.20", "1.50");
        context.seconds_left = 180;

        assert_eq!(
            bonereaper_state_v2_quality_guard_block(&context, &decision, decimal("0.54"), &config),
            Some(BonereaperStateV2QualityBlock::MidGap)
        );
    }

    #[test]
    fn bonereaper_state_v2_quality_guard_blocks_late_mid_gap() {
        let mut config = strategy_config();
        config.bonereaper_state_v2_mid_gap_guard_enabled = true;
        let decision = BonereaperStateV2Decision {
            signal_strength_bps: Decimal::from(1_200),
            aligned_flow_bps: Decimal::from(2_000),
            ..test_codex_decision("1.50", "1.50")
        };
        let mut context = test_codex_context("2.20", "1.50");
        context.seconds_left = 73;

        assert_eq!(
            bonereaper_state_v2_quality_guard_block(&context, &decision, decimal("0.48"), &config),
            Some(BonereaperStateV2QualityBlock::MidGap)
        );
    }

    #[test]
    fn bonereaper_state_v2_quality_guard_allows_high_quality_mid_gap_value() {
        let mut config = strategy_config();
        config.bonereaper_state_v2_mid_gap_guard_enabled = true;
        let decision = BonereaperStateV2Decision {
            signal_strength_bps: Decimal::from(1_200),
            aligned_flow_bps: Decimal::from(2_000),
            ..test_codex_decision("1.50", "1.50")
        };
        let mut context = test_codex_context("2.20", "1.50");
        context.seconds_left = 180;

        assert_eq!(
            bonereaper_state_v2_quality_guard_block(&context, &decision, decimal("0.48"), &config),
            None
        );
    }

    #[test]
    fn bonereaper_state_v2_quality_guard_blocks_early_expensive_average_signal() {
        let mut config = strategy_config();
        config.bonereaper_state_v2_early_expensive_guard_enabled = true;
        let decision = BonereaperStateV2Decision {
            signal_strength_bps: Decimal::from(1_777),
            aligned_flow_bps: Decimal::from(2_954),
            ..test_codex_decision("1.44", "1.44")
        };
        let mut context = test_codex_context("1.44", "1.44");
        context.seconds_left = 255;

        assert_eq!(
            bonereaper_state_v2_quality_guard_block(&context, &decision, decimal("0.57"), &config),
            Some(BonereaperStateV2QualityBlock::EarlyExpensive)
        );
    }

    #[test]
    fn bonereaper_state_v2_quality_guard_allows_early_expensive_extreme_signal() {
        let mut config = strategy_config();
        config.bonereaper_state_v2_early_expensive_guard_enabled = true;
        let decision = BonereaperStateV2Decision {
            signal_strength_bps: Decimal::from(3_000),
            aligned_flow_bps: Decimal::from(4_500),
            ..test_codex_decision("2.20", "2.20")
        };
        let mut context = test_codex_context("3.20", "2.20");
        context.seconds_left = 255;

        assert_eq!(
            bonereaper_state_v2_quality_guard_block(&context, &decision, decimal("0.57"), &config),
            None
        );
    }

    #[test]
    fn bonereaper_state_v2_quality_guard_blocks_average_low_gap_entry() {
        let mut config = strategy_config();
        config.bonereaper_state_v2_low_gap_guard_enabled = true;
        let decision = BonereaperStateV2Decision {
            signal_strength_bps: Decimal::from(900),
            aligned_flow_bps: Decimal::from(1_500),
            ..test_codex_decision("0.20", "0.60")
        };
        let mut context = test_codex_context("0.95", "0.20");
        context.seconds_left = 180;

        assert_eq!(
            bonereaper_state_v2_quality_guard_block(&context, &decision, decimal("0.51"), &config),
            Some(BonereaperStateV2QualityBlock::LowGap)
        );
    }

    #[test]
    fn bonereaper_state_v2_quality_guard_blocks_late_low_gap_value_reentry() {
        let mut config = strategy_config();
        config.bonereaper_state_v2_low_gap_guard_enabled = true;
        let decision = BonereaperStateV2Decision {
            signal_strength_bps: Decimal::from(1_500),
            aligned_flow_bps: Decimal::from(2_500),
            ..test_codex_decision("0.60", "0.60")
        };
        let mut context = test_codex_context("0.95", "0.60");
        context.seconds_left = 116;

        assert_eq!(
            bonereaper_state_v2_quality_guard_block(&context, &decision, decimal("0.37"), &config),
            Some(BonereaperStateV2QualityBlock::LowGap)
        );
    }

    #[test]
    fn bonereaper_state_v2_quality_guard_blocks_counter_bias_low_gap_chop() {
        let mut config = strategy_config();
        config.bonereaper_state_v2_low_gap_guard_enabled = true;
        let decision = BonereaperStateV2Decision {
            signal_strength_bps: Decimal::from(980),
            aligned_flow_bps: Decimal::from(1_700),
            counter_bias: true,
            ..test_codex_decision("1.20", "1.20")
        };
        let mut context = test_codex_context("-1.24", "-1.24");
        context.seconds_left = 244;

        assert_eq!(
            bonereaper_state_v2_quality_guard_block(&context, &decision, decimal("0.50"), &config),
            Some(BonereaperStateV2QualityBlock::LowGap)
        );
    }

    #[test]
    fn bonereaper_state_v2_quality_guard_allows_discounted_low_gap_value() {
        let mut config = strategy_config();
        config.bonereaper_state_v2_low_gap_guard_enabled = true;
        let decision = BonereaperStateV2Decision {
            signal_strength_bps: Decimal::from(700),
            aligned_flow_bps: Decimal::from(900),
            ..test_codex_decision("0.20", "0.30")
        };
        let mut context = test_codex_context("0.40", "0.20");
        context.seconds_left = 195;

        assert_eq!(
            bonereaper_state_v2_quality_guard_block(&context, &decision, decimal("0.43"), &config),
            None
        );
    }

    #[test]
    fn bonereaper_state_v2_quality_guard_allows_extreme_low_gap_signal() {
        let mut config = strategy_config();
        config.bonereaper_state_v2_low_gap_guard_enabled = true;
        let decision = BonereaperStateV2Decision {
            signal_strength_bps: Decimal::from(2_200),
            aligned_flow_bps: Decimal::from(3_500),
            ..test_codex_decision("1.60", "1.60")
        };
        let mut context = test_codex_context("1.20", "1.60");
        context.seconds_left = 180;

        assert_eq!(
            bonereaper_state_v2_quality_guard_block(&context, &decision, decimal("0.52"), &config),
            None
        );
    }

    #[test]
    fn bonereaper_state_v2_quality_guard_allows_confirmed_high_gap_value() {
        let mut config = strategy_config();
        config.bonereaper_state_v2_micro_alignment_guard_enabled = true;
        config.bonereaper_state_v2_early_window_guard_enabled = true;
        config.bonereaper_state_v2_high_gap_guard_enabled = true;
        let decision = BonereaperStateV2Decision {
            signal_strength_bps: Decimal::from(1_500),
            ..test_codex_decision("1.40", "1.40")
        };
        let mut context = test_codex_context("3.20", "1.40");
        context.seconds_left = 250;

        assert_eq!(
            bonereaper_state_v2_quality_guard_block(&context, &decision, decimal("0.54"), &config),
            None
        );
    }

    #[test]
    fn codex_sentinel_v1_blocks_stale_micro_without_swing_confirmation() {
        let mut config = strategy_config();
        config.codex_sentinel_v1_stale_micro_min_swing_bps = decimal("0.75");
        config.codex_sentinel_v1_stale_micro_min_target_gap_bps = decimal("0.75");
        let decision = test_codex_decision("0", "0");
        let context = test_codex_context("1.00", "0");

        assert!(codex_sentinel_v1_stale_micro_guard_blocks(
            &context,
            &decision,
            decimal("0.53"),
            &config,
        ));
    }

    #[test]
    fn codex_sentinel_v1_allows_stale_micro_with_swing_confirmation_and_fair_price() {
        let mut config = strategy_config();
        config.codex_sentinel_v1_stale_micro_min_swing_bps = decimal("0.75");
        config.codex_sentinel_v1_stale_micro_min_target_gap_bps = decimal("0.75");
        config.codex_sentinel_v1_stale_micro_max_non_discount_entry_price = decimal("0.65");
        let decision = test_codex_decision("0", "0.85");
        let context = test_codex_context("0.85", "0");

        assert!(!codex_sentinel_v1_stale_micro_guard_blocks(
            &context,
            &decision,
            decimal("0.64"),
            &config,
        ));
    }

    #[test]
    fn codex_sentinel_v1_blocks_expensive_entry_without_fresh_confirmation() {
        let mut config = strategy_config();
        config.codex_sentinel_v1_expensive_entry_price = decimal("0.65");
        config.codex_sentinel_v1_expensive_min_micro_bps = decimal("1.25");
        config.codex_sentinel_v1_expensive_min_swing_bps = decimal("1.25");
        let decision = test_codex_decision("1.11", "1.11");
        let context = test_codex_context("2.38", "0");

        assert!(codex_sentinel_v1_expensive_entry_guard_blocks(
            &context,
            &decision,
            decimal("0.68"),
            &config,
        ));
    }

    #[test]
    fn codex_sentinel_v1_blocks_premium_entry_without_strong_flow() {
        let mut config = strategy_config();
        config.codex_sentinel_v1_premium_entry_guard_enabled = true;
        config.codex_sentinel_v1_premium_entry_price = decimal("0.55");
        config.codex_sentinel_v1_premium_min_signal_bps = Decimal::from(800);
        config.codex_sentinel_v1_premium_min_flow_bps = Decimal::from(1400);
        config.codex_sentinel_v1_premium_min_fresh_bps = decimal("1.25");
        let mut decision = test_codex_decision("1.33", "1.33");
        decision.signal_strength_bps = Decimal::from(745);
        decision.aligned_flow_bps = Decimal::from(1234);
        let context = test_codex_context("1.33", "-1.33");

        assert!(codex_sentinel_v1_premium_entry_guard_blocks(
            &context,
            &decision,
            decimal("0.56"),
            &config,
        ));
    }

    #[test]
    fn codex_sentinel_v1_allows_premium_entry_with_strong_quality() {
        let mut config = strategy_config();
        config.codex_sentinel_v1_premium_entry_guard_enabled = true;
        config.codex_sentinel_v1_premium_entry_price = decimal("0.55");
        config.codex_sentinel_v1_premium_min_signal_bps = Decimal::from(800);
        config.codex_sentinel_v1_premium_min_flow_bps = Decimal::from(1400);
        config.codex_sentinel_v1_premium_min_fresh_bps = decimal("1.25");
        let mut decision = test_codex_decision("1.50", "2.00");
        decision.signal_strength_bps = Decimal::from(900);
        decision.aligned_flow_bps = Decimal::from(1500);
        let context = test_codex_context("1.50", "-1.50");

        assert!(!codex_sentinel_v1_premium_entry_guard_blocks(
            &context,
            &decision,
            decimal("0.56"),
            &config,
        ));
    }

    #[test]
    fn codex_sentinel_v1_value_only_gate_blocks_expensive_non_monster_entry() {
        let mut config = strategy_config();
        config.codex_sentinel_v1_premium_entry_guard_enabled = true;
        config.codex_sentinel_v1_premium_entry_price = decimal("0.56");
        config.codex_sentinel_v1_premium_min_signal_bps = Decimal::from(1600);
        config.codex_sentinel_v1_premium_min_flow_bps = Decimal::from(2500);
        config.codex_sentinel_v1_premium_min_fresh_bps = decimal("4.00");
        let mut decision = test_codex_decision("4.10", "4.74");
        decision.signal_strength_bps = Decimal::from(1036);
        decision.aligned_flow_bps = Decimal::from(1705);
        let context = test_codex_context("4.24", "5.49");

        assert!(codex_sentinel_v1_premium_entry_guard_blocks(
            &context,
            &decision,
            decimal("0.67"),
            &config,
        ));
    }

    #[test]
    fn codex_sentinel_v1_value_only_gate_allows_monster_premium_entry() {
        let mut config = strategy_config();
        config.codex_sentinel_v1_premium_entry_guard_enabled = true;
        config.codex_sentinel_v1_premium_entry_price = decimal("0.56");
        config.codex_sentinel_v1_premium_min_signal_bps = Decimal::from(1600);
        config.codex_sentinel_v1_premium_min_flow_bps = Decimal::from(2500);
        config.codex_sentinel_v1_premium_min_fresh_bps = decimal("4.00");
        let mut decision = test_codex_decision("4.30", "4.40");
        decision.signal_strength_bps = Decimal::from(1700);
        decision.aligned_flow_bps = Decimal::from(2600);
        let context = test_codex_context("4.50", "4.20");

        assert!(!codex_sentinel_v1_premium_entry_guard_blocks(
            &context,
            &decision,
            decimal("0.57"),
            &config,
        ));
    }

    #[test]
    fn codex_sentinel_v1_aggressive_continuation_bypasses_premium_only_for_fresh_high_gap() {
        let mut config = strategy_config();
        config.codex_sentinel_v1_premium_entry_guard_enabled = true;
        config.codex_sentinel_v1_premium_entry_price = decimal("0.56");
        config.codex_sentinel_v1_premium_min_signal_bps = Decimal::from(1600);
        config.codex_sentinel_v1_premium_min_flow_bps = Decimal::from(2500);
        config.codex_sentinel_v1_premium_min_fresh_bps = decimal("4.00");
        config.codex_sentinel_v1_expensive_entry_guard_enabled = true;
        config.codex_sentinel_v1_expensive_entry_price = decimal("0.56");
        config.codex_sentinel_v1_expensive_min_micro_bps = decimal("4.00");
        config.codex_sentinel_v1_expensive_min_swing_bps = decimal("4.00");
        config.codex_sentinel_v1_aggressive_continuation_enabled = true;
        config.codex_sentinel_v1_aggressive_continuation_max_entry_price = decimal("0.62");
        config.codex_sentinel_v1_aggressive_continuation_min_target_gap_bps = decimal("6.00");
        config.codex_sentinel_v1_aggressive_continuation_min_signal_bps = Decimal::from(1500);
        config.codex_sentinel_v1_aggressive_continuation_min_flow_bps = Decimal::from(2200);
        config.codex_sentinel_v1_aggressive_continuation_min_fresh_bps = decimal("3.50");
        config.codex_sentinel_v1_aggressive_continuation_min_swing_bps = decimal("3.50");
        config.codex_sentinel_v1_aggressive_continuation_max_quote_age_ms = 750;

        let mut context = test_codex_context("6.50", "3.60");
        context.current_spot_source = "Coinbase::Ticker".to_owned();
        context.current_spot_event_age_ms = Some(400);
        context.current_spot_received_age_ms = Some(180);
        let mut decision = test_codex_decision("3.60", "4.10");
        decision.signal_strength_bps = Decimal::from(1500);
        decision.aligned_flow_bps = Decimal::from(2300);

        assert!(codex_sentinel_v1_premium_entry_guard_blocks(
            &context,
            &decision,
            decimal("0.60"),
            &config,
        ));
        assert!(codex_sentinel_v1_expensive_entry_guard_blocks(
            &context,
            &decision,
            decimal("0.60"),
            &config,
        ));
        assert!(codex_sentinel_v1_aggressive_continuation_allows(
            &context,
            &decision,
            decimal("0.60"),
            &config,
        ));
    }

    #[test]
    fn codex_sentinel_v1_blocks_threshold_premium_even_when_breakout_allows() {
        let mut config = strategy_config();
        config.codex_sentinel_v1_premium_entry_guard_enabled = true;
        config.codex_sentinel_v1_premium_entry_price = decimal("0.56");
        config.codex_sentinel_v1_premium_min_signal_bps = Decimal::from(1600);
        config.codex_sentinel_v1_premium_min_flow_bps = Decimal::from(2500);
        config.codex_sentinel_v1_premium_min_fresh_bps = decimal("4.00");
        config.codex_breakout_v1_enabled = true;
        config.codex_breakout_v1_max_entry_price = decimal("0.58");
        config.codex_breakout_v1_max_book_age_ms = 750;
        config.codex_breakout_v1_max_spread_bps = decimal("2.00");
        config.codex_breakout_v1_min_score_bps = Decimal::from(3000);
        config.codex_breakout_v1_min_depth_imbalance_bps = Decimal::from(1800);
        config.codex_breakout_v1_min_microprice_bps = decimal("0.0003");
        config.codex_breakout_v1_min_fresh_bps = decimal("1.00");
        config.codex_breakout_v1_min_target_gap_bps = decimal("1.00");
        config.codex_breakout_v1_min_signal_bps = Decimal::ZERO;
        config.codex_breakout_v1_min_flow_bps = Decimal::ZERO;

        let mut context = test_codex_context("3.44", "4.01");
        context.current_spot_source = "Binance::Trade".to_owned();
        context.current_spot_received_age_ms = Some(30);
        context.exchange_book_age_ms = Some(51);
        context.exchange_book_depth_imbalance_bps = Decimal::from(9810);
        context.exchange_book_top_imbalance_bps = Decimal::from(9865);
        context.exchange_book_microprice_bps = decimal("0.55");
        context.exchange_book_spread_bps = decimal("0.10");
        let mut decision = test_codex_decision("4.01", "1.50");
        decision.signal_strength_bps = Decimal::from(921);
        decision.aligned_flow_bps = Decimal::from(1515);

        assert!(codex_breakout_v1_allows(
            &context,
            &decision,
            decimal("0.56"),
            &config,
        ));
        assert!(codex_sentinel_v1_premium_entry_guard_blocks(
            &context,
            &decision,
            decimal("0.56"),
            &config,
        ));
    }

    #[test]
    fn codex_sentinel_v1_strategy_blocks_breakout_premium_chase() {
        let mut config = strategy_config();
        config.enable_bundle = false;
        config.enable_directional = false;
        config.enable_micro_breakout = false;
        config.enable_target_state_v1 = false;
        config.enable_bonereaper_state_v2 = false;
        config.enable_codex_sentinel_v1 = true;
        config.min_top_of_book_shares = decimal("5");
        config.bonereaper_state_v2_min_elapsed_window_secs = 8;
        config.bonereaper_state_v2_min_seconds_left = 210;
        config.bonereaper_state_v2_max_seconds_left = 270;
        config.bonereaper_state_v2_bias_min_target_gap_bps = decimal("1.20");
        config.bonereaper_state_v2_flip_max_target_gap_bps = decimal("1.20");
        config.bonereaper_state_v2_min_signal_bps = 4;
        config.bonereaper_state_v2_min_spot_move_15s_bps = decimal("0.10");
        config.bonereaper_state_v2_min_spot_move_5s_bps = decimal("0.05");
        config.bonereaper_state_v2_min_aligned_flow_bps = Decimal::ZERO;
        config.bonereaper_state_v2_max_entry_price = decimal("0.76");
        config.bonereaper_state_v2_max_fair_price = decimal("0.86");
        config.bonereaper_state_v2_normal_notional_usdc = decimal("9");
        config.bonereaper_state_v2_min_expected_profit_usdc = decimal("0.30");
        config.codex_sentinel_v1_mid_signal_guard_enabled = true;
        config.codex_sentinel_v1_mid_signal_min_bps = decimal("2.8");
        config.codex_sentinel_v1_mid_signal_max_bps = decimal("3.6");
        config.codex_sentinel_v1_mid_signal_min_confirmation_bps = decimal("0.05");
        config.codex_sentinel_v1_premium_entry_guard_enabled = true;
        config.codex_sentinel_v1_premium_entry_price = decimal("0.56");
        config.codex_sentinel_v1_premium_min_signal_bps = Decimal::from(1600);
        config.codex_sentinel_v1_premium_min_flow_bps = Decimal::from(2500);
        config.codex_sentinel_v1_premium_min_fresh_bps = decimal("4.00");
        config.codex_sentinel_v1_expensive_entry_guard_enabled = true;
        config.codex_sentinel_v1_expensive_entry_price = decimal("0.56");
        config.codex_sentinel_v1_expensive_min_micro_bps = decimal("4.00");
        config.codex_sentinel_v1_expensive_min_swing_bps = decimal("4.00");
        config.codex_breakout_v1_enabled = true;
        config.codex_breakout_v1_required = true;
        config.codex_breakout_v1_max_entry_price = decimal("0.58");
        config.codex_breakout_v1_max_book_age_ms = 750;
        config.codex_breakout_v1_max_spread_bps = decimal("2.00");
        config.codex_breakout_v1_min_score_bps = Decimal::from(3000);
        config.codex_breakout_v1_min_depth_imbalance_bps = Decimal::from(1800);
        config.codex_breakout_v1_min_microprice_bps = decimal("0.0003");
        config.codex_breakout_v1_min_fresh_bps = decimal("1.00");
        config.codex_breakout_v1_min_target_gap_bps = decimal("1.00");
        config.codex_breakout_v1_min_signal_bps = Decimal::from(650);
        config.codex_breakout_v1_min_flow_bps = Decimal::from(700);
        let strategy = BundleArbitrageStrategy::new(config);
        let market = market();

        let mut books = HashMap::new();
        books.insert(
            "up-token".to_owned(),
            OrderBook {
                asset_id: "up-token".to_owned(),
                bids: vec![BookLevel {
                    price: decimal("0.55"),
                    size: decimal("150"),
                }],
                asks: vec![BookLevel {
                    price: decimal("0.56"),
                    size: decimal("150"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );
        books.insert(
            "down-token".to_owned(),
            OrderBook {
                asset_id: "down-token".to_owned(),
                bids: vec![BookLevel {
                    price: decimal("0.43"),
                    size: decimal("150"),
                }],
                asks: vec![BookLevel {
                    price: decimal("0.45"),
                    size: decimal("150"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );

        let mut contexts = HashMap::new();
        contexts.insert(
            market.slug.clone(),
            BtcFiveMinuteContext {
                target: MarketTarget::Btc5m,
                interval_open_price: decimal("67000"),
                target_price: decimal("67000"),
                target_price_source: TargetPriceSource::PolymarketEventMetadata,
                target_gap_bps: decimal("3.44"),
                current_spot_price: decimal("67023.05"),
                current_spot_source: "Binance::Trade".to_owned(),
                current_spot_event_age_ms: Some(95),
                current_spot_received_age_ms: Some(30),
                current_spot_quote_points: None,
                exchange_book_age_ms: Some(51),
                exchange_book_top_imbalance_bps: Decimal::from(9865),
                exchange_book_depth_imbalance_bps: Decimal::from(9810),
                exchange_book_microprice_bps: decimal("0.55"),
                exchange_book_spread_bps: decimal("0.10"),
                micro_burst_reference_price: decimal("66996.18"),
                micro_reference_price: decimal("66996.18"),
                spot_move_bps: decimal("3.44"),
                spot_move_1s_bps: decimal("4.01"),
                spot_move_5s_bps: decimal("4.01"),
                spot_move_15s_bps: decimal("1.50"),
                micro_acceleration_bps: Decimal::ZERO,
                dominant_outcome: "Up".to_owned(),
                seconds_left: 240,
            },
        );

        let mut trade_flows = HashMap::new();
        trade_flows.insert(
            market.slug.clone(),
            TradeFlowSummary {
                signed_up_imbalance_bps: decimal("1515"),
                trade_count: 12,
                ..TradeFlowSummary::default()
            },
        );

        let opportunities = strategy.find_opportunities(
            std::slice::from_ref(&market),
            &books,
            &HashMap::new(),
            &contexts,
            &trade_flows,
        );
        assert!(opportunities.is_empty());

        let misses = strategy.find_near_misses(
            std::slice::from_ref(&market),
            &books,
            &HashMap::new(),
            &contexts,
            &trade_flows,
            1,
        );
        assert_eq!(misses.len(), 1);
        assert!(
            misses[0].reason.contains("premium entry"),
            "near miss should explain the premium-price guard, got: {}",
            misses[0].reason
        );
    }

    #[test]
    fn codex_sentinel_v1_aggressive_continuation_rejects_stale_or_too_expensive_chase() {
        let mut config = strategy_config();
        config.codex_sentinel_v1_aggressive_continuation_enabled = true;
        config.codex_sentinel_v1_aggressive_continuation_max_entry_price = decimal("0.62");
        config.codex_sentinel_v1_aggressive_continuation_min_target_gap_bps = decimal("6.00");
        config.codex_sentinel_v1_aggressive_continuation_min_signal_bps = Decimal::from(1500);
        config.codex_sentinel_v1_aggressive_continuation_min_flow_bps = Decimal::from(2200);
        config.codex_sentinel_v1_aggressive_continuation_min_fresh_bps = decimal("3.50");
        config.codex_sentinel_v1_aggressive_continuation_min_swing_bps = decimal("3.50");
        config.codex_sentinel_v1_aggressive_continuation_max_quote_age_ms = 750;

        let mut context = test_codex_context("6.50", "3.60");
        context.current_spot_source = "Coinbase::Ticker".to_owned();
        context.current_spot_event_age_ms = Some(900);
        context.current_spot_received_age_ms = Some(900);
        let mut decision = test_codex_decision("3.60", "4.10");
        decision.signal_strength_bps = Decimal::from(1500);
        decision.aligned_flow_bps = Decimal::from(2300);

        assert!(!codex_sentinel_v1_aggressive_continuation_allows(
            &context,
            &decision,
            decimal("0.60"),
            &config,
        ));

        context.current_spot_event_age_ms = Some(400);
        context.current_spot_received_age_ms = Some(180);
        assert!(!codex_sentinel_v1_aggressive_continuation_allows(
            &context,
            &decision,
            decimal("0.63"),
            &config,
        ));
    }

    #[test]
    fn codex_breakout_v1_allows_fresh_aligned_book_pressure() {
        let mut config = strategy_config();
        config.codex_breakout_v1_enabled = true;
        config.codex_breakout_v1_max_entry_price = decimal("0.58");
        config.codex_breakout_v1_max_book_age_ms = 750;
        config.codex_breakout_v1_max_spread_bps = decimal("2.00");
        config.codex_breakout_v1_min_score_bps = Decimal::from(3000);
        config.codex_breakout_v1_min_depth_imbalance_bps = Decimal::from(1800);
        config.codex_breakout_v1_min_microprice_bps = decimal("0.0003");
        config.codex_breakout_v1_min_fresh_bps = decimal("1.00");
        config.codex_breakout_v1_min_target_gap_bps = decimal("1.00");
        config.codex_sentinel_v1_live_quote_age_guard_enabled = true;
        config.codex_sentinel_v1_max_live_quote_age_ms = 750;
        let mut context = test_codex_context("1.80", "1.25");
        context.current_spot_source = "Binance::Trade".to_owned();
        context.current_spot_received_age_ms = Some(30);
        context.exchange_book_age_ms = Some(25);
        context.exchange_book_depth_imbalance_bps = Decimal::from(2_100);
        context.exchange_book_top_imbalance_bps = Decimal::from(2_600);
        context.exchange_book_microprice_bps = decimal("0.55");
        context.exchange_book_spread_bps = decimal("0.30");
        let decision = test_codex_decision("1.25", "1.20");

        assert!(codex_breakout_v1_allows(
            &context,
            &decision,
            decimal("0.56"),
            &config,
        ));
    }

    #[test]
    fn codex_breakout_v1_rejects_stale_or_opposing_book_pressure() {
        let mut config = strategy_config();
        config.codex_breakout_v1_enabled = true;
        config.codex_breakout_v1_max_entry_price = decimal("0.58");
        config.codex_breakout_v1_max_book_age_ms = 750;
        config.codex_breakout_v1_max_spread_bps = decimal("2.00");
        config.codex_breakout_v1_min_score_bps = Decimal::from(3000);
        config.codex_breakout_v1_min_depth_imbalance_bps = Decimal::from(1800);
        config.codex_breakout_v1_min_microprice_bps = decimal("0.0003");
        config.codex_breakout_v1_min_fresh_bps = decimal("1.00");
        config.codex_breakout_v1_min_target_gap_bps = decimal("1.00");
        config.codex_sentinel_v1_live_quote_age_guard_enabled = true;
        config.codex_sentinel_v1_max_live_quote_age_ms = 750;
        let mut context = test_codex_context("1.80", "1.25");
        context.current_spot_source = "Binance::Trade".to_owned();
        context.current_spot_received_age_ms = Some(30);
        context.exchange_book_age_ms = Some(900);
        context.exchange_book_depth_imbalance_bps = Decimal::from(2_100);
        context.exchange_book_microprice_bps = decimal("0.55");
        context.exchange_book_spread_bps = decimal("0.30");
        let decision = test_codex_decision("1.25", "1.20");

        assert!(!codex_breakout_v1_allows(
            &context,
            &decision,
            decimal("0.56"),
            &config,
        ));

        context.exchange_book_age_ms = Some(25);
        context.exchange_book_depth_imbalance_bps = Decimal::from(-2_100);
        context.exchange_book_microprice_bps = decimal("-0.55");
        assert!(!codex_breakout_v1_allows(
            &context,
            &decision,
            decimal("0.56"),
            &config,
        ));
    }

    #[test]
    fn codex_scalp_probe_v1_allows_alt_value_setup_with_book_pressure() {
        let config = strategy_config();
        let mut context = test_codex_context("1.80", "0.90");
        context.target = MarketTarget::Xrp5m;
        context.current_spot_source = "Binance::Trade".to_owned();
        context.current_spot_received_age_ms = Some(25);
        context.exchange_book_age_ms = Some(30);
        context.exchange_book_top_imbalance_bps = Decimal::from(1_100);
        context.exchange_book_depth_imbalance_bps = Decimal::from(1_300);
        context.exchange_book_spread_bps = decimal("0.40");
        let mut decision = test_codex_decision("0.90", "1.20");
        decision.aligned_flow_bps = Decimal::from(750);
        decision.signal_strength_bps = Decimal::from(500);
        let book = OrderBook {
            asset_id: "up".to_owned(),
            bids: vec![BookLevel {
                price: decimal("0.48"),
                size: decimal("100"),
            }],
            asks: vec![BookLevel {
                price: decimal("0.52"),
                size: decimal("100"),
            }],
            min_order_size: None,
            tick_size: None,
        };

        assert!(codex_scalp_probe_v1_allows(
            &context,
            &decision,
            decimal("0.52"),
            &book,
            &config,
        ));
    }

    #[test]
    fn codex_scalp_probe_v1_radar_allows_low_gap_when_book_pressure_is_extreme() {
        let config = strategy_config();
        let mut context = test_codex_context("0.20", "0.30");
        context.target = MarketTarget::Eth5m;
        context.current_spot_source = "Binance::Trade".to_owned();
        context.current_spot_received_age_ms = Some(25);
        context.exchange_book_age_ms = Some(30);
        context.exchange_book_top_imbalance_bps = Decimal::from(2_500);
        context.exchange_book_depth_imbalance_bps = Decimal::from(2_500);
        context.exchange_book_microprice_bps = decimal("0.0004");
        context.exchange_book_spread_bps = decimal("0.40");
        let mut decision = test_codex_decision("0.90", "1.20");
        decision.aligned_flow_bps = Decimal::from(100);
        decision.signal_strength_bps = Decimal::from(500);
        let book = OrderBook {
            asset_id: "up".to_owned(),
            bids: vec![BookLevel {
                price: decimal("0.48"),
                size: decimal("100"),
            }],
            asks: vec![BookLevel {
                price: decimal("0.52"),
                size: decimal("100"),
            }],
            min_order_size: None,
            tick_size: None,
        };

        assert!(codex_scalp_probe_v1_allows(
            &context,
            &decision,
            decimal("0.49"),
            &book,
            &config,
        ));
    }

    #[test]
    fn codex_scalp_probe_v1_allows_btc_but_rejects_weak_book_pressure() {
        let config = strategy_config();
        let mut context = test_codex_context("1.80", "0.90");
        context.current_spot_source = "Binance::Trade".to_owned();
        context.current_spot_received_age_ms = Some(25);
        context.exchange_book_age_ms = Some(30);
        context.exchange_book_top_imbalance_bps = Decimal::from(1_100);
        context.exchange_book_depth_imbalance_bps = Decimal::from(1_300);
        context.exchange_book_spread_bps = decimal("0.40");
        let mut decision = test_codex_decision("0.90", "1.20");
        decision.aligned_flow_bps = Decimal::from(750);
        decision.signal_strength_bps = Decimal::from(500);
        let book = OrderBook {
            asset_id: "up".to_owned(),
            bids: vec![BookLevel {
                price: decimal("0.48"),
                size: decimal("100"),
            }],
            asks: vec![BookLevel {
                price: decimal("0.52"),
                size: decimal("100"),
            }],
            min_order_size: None,
            tick_size: None,
        };

        assert!(codex_scalp_probe_v1_allows(
            &context,
            &decision,
            decimal("0.52"),
            &book,
            &config,
        ));

        context.target = MarketTarget::Sol5m;
        context.exchange_book_depth_imbalance_bps = Decimal::from(200);
        assert!(!codex_scalp_probe_v1_allows(
            &context,
            &decision,
            decimal("0.49"),
            &book,
            &config,
        ));
    }

    #[test]
    fn codex_scalp_probe_v1_raw_ablation_bypasses_quality_guards() {
        let mut config = strategy_config();
        config.codex_scalp_probe_v1_raw_ablation_enabled = true;
        let mut context = test_codex_context("0.10", "-0.40");
        context.target = MarketTarget::Eth5m;
        context.current_spot_source = "Binance::Trade".to_owned();
        context.current_spot_received_age_ms = Some(4_000);
        context.exchange_book_age_ms = Some(5_000);
        context.exchange_book_top_imbalance_bps = Decimal::from(-300);
        context.exchange_book_depth_imbalance_bps = Decimal::from(-300);
        context.exchange_book_spread_bps = decimal("15.00");
        let mut decision = test_codex_decision("-0.40", "-0.40");
        decision.aligned_flow_bps = Decimal::ZERO;
        decision.signal_strength_bps = Decimal::ZERO;
        let book = OrderBook {
            asset_id: "up".to_owned(),
            bids: vec![BookLevel {
                price: decimal("0.45"),
                size: decimal("100"),
            }],
            asks: vec![BookLevel {
                price: decimal("0.92"),
                size: decimal("100"),
            }],
            min_order_size: None,
            tick_size: None,
        };

        assert!(codex_scalp_probe_v1_allows(
            &context,
            &decision,
            decimal("0.92"),
            &book,
            &config,
        ));
    }

    #[test]
    fn codex_scalp_probe_v1_raw_light_keeps_btc_breakout_but_blocks_weak_alt() {
        let mut config = strategy_config();
        config.codex_scalp_probe_v1_raw_ablation_enabled = true;
        config.codex_scalp_probe_v1_raw_light_enabled = true;
        config.codex_sentinel_v1_max_live_quote_age_ms = 3_000;
        config.codex_scalp_probe_v1_max_book_age_ms = 1_000;
        config.codex_scalp_probe_v1_max_entry_spread = decimal("0.20");
        config.codex_scalp_probe_v1_max_exchange_spread_bps = decimal("10.00");

        let mut context = test_codex_context("5.00", "5.00");
        context.target = MarketTarget::Btc5m;
        context.current_spot_source = "Binance::Trade".to_owned();
        context.current_spot_received_age_ms = Some(25);
        context.exchange_book_age_ms = Some(30);
        context.exchange_book_top_imbalance_bps = Decimal::from(1_000);
        context.exchange_book_depth_imbalance_bps = Decimal::from(1_000);
        context.exchange_book_spread_bps = decimal("0.40");
        let mut decision = test_codex_decision("5.00", "5.00");
        decision.signal_strength_bps = Decimal::from(16);
        let book = OrderBook {
            asset_id: "up".to_owned(),
            bids: vec![BookLevel {
                price: decimal("0.50"),
                size: decimal("100"),
            }],
            asks: vec![BookLevel {
                price: decimal("0.52"),
                size: decimal("100"),
            }],
            min_order_size: None,
            tick_size: None,
        };

        assert!(codex_scalp_probe_v1_allows(
            &context,
            &decision,
            decimal("0.52"),
            &book,
            &config,
        ));

        context.target = MarketTarget::Sol5m;
        context.target_gap_bps = decimal("4.00");
        context.exchange_book_top_imbalance_bps = Decimal::from(300);
        context.exchange_book_depth_imbalance_bps = Decimal::from(300);
        decision.signal_strength_bps = Decimal::from(12);
        assert!(!codex_scalp_probe_v1_allows(
            &context,
            &decision,
            decimal("0.52"),
            &book,
            &config,
        ));
    }

    #[test]
    fn codex_scalp_probe_v1_raw_light_requires_live_burst_not_stale_5s_only() {
        let mut config = strategy_config();
        config.codex_scalp_probe_v1_raw_ablation_enabled = true;
        config.codex_scalp_probe_v1_raw_light_enabled = true;
        config.codex_scalp_probe_v1_min_entry_price = decimal("0.56");
        config.codex_scalp_probe_v1_max_entry_price = decimal("0.68");
        config.codex_scalp_probe_v1_min_seconds_left = 180;
        config.codex_scalp_probe_v1_max_seconds_left = 295;
        config.codex_sentinel_v1_max_live_quote_age_ms = 3_000;
        config.codex_scalp_probe_v1_max_book_age_ms = 1_000;
        config.codex_scalp_probe_v1_max_entry_spread = decimal("0.20");
        config.codex_scalp_probe_v1_max_exchange_spread_bps = decimal("10.00");

        let mut context = test_codex_context("8.00", "0.00");
        context.current_spot_source = "Binance::Trade".to_owned();
        context.current_spot_received_age_ms = Some(25);
        context.exchange_book_age_ms = Some(30);
        context.exchange_book_top_imbalance_bps = Decimal::from(1_200);
        context.exchange_book_depth_imbalance_bps = Decimal::from(1_200);
        context.exchange_book_spread_bps = decimal("0.40");
        context.seconds_left = 275;
        let mut decision = test_codex_decision("14.00", "14.00");
        decision.signal_strength_bps = Decimal::from(450);
        let book = OrderBook {
            asset_id: "up".to_owned(),
            bids: vec![BookLevel {
                price: decimal("0.58"),
                size: decimal("100"),
            }],
            asks: vec![BookLevel {
                price: decimal("0.60"),
                size: decimal("100"),
            }],
            min_order_size: None,
            tick_size: None,
        };

        assert!(!codex_scalp_probe_v1_allows(
            &context,
            &decision,
            decimal("0.60"),
            &book,
            &config,
        ));

        context.spot_move_1s_bps = decimal("0.35");
        assert!(codex_scalp_probe_v1_allows(
            &context,
            &decision,
            decimal("0.60"),
            &book,
            &config,
        ));
    }

    #[test]
    fn codex_scalp_probe_v1_near_miss_uses_raw_light_live_burst_thresholds() {
        let mut config = strategy_config();
        config.enable_bundle = false;
        config.enable_directional = false;
        config.enable_micro_breakout = false;
        config.enable_target_state_v1 = false;
        config.enable_bonereaper_state_v2 = false;
        config.enable_bonereaper_state_guarded = false;
        config.enable_codex_sentinel_v1 = false;
        config.enable_codex_scalp_probe_v1 = true;
        config.codex_scalp_probe_v1_raw_ablation_enabled = true;
        config.codex_scalp_probe_v1_raw_light_enabled = true;
        config.codex_scalp_probe_v1_min_entry_price = decimal("0.56");
        config.codex_scalp_probe_v1_max_entry_price = decimal("0.68");
        config.codex_scalp_probe_v1_min_elapsed_window_secs = 0;
        config.codex_scalp_probe_v1_max_seconds_left = 295;
        config.codex_scalp_probe_v1_min_seconds_left = 180;
        config.codex_scalp_probe_v1_max_book_age_ms = 1_000;
        config.codex_scalp_probe_v1_max_exchange_spread_bps = decimal("10.00");
        config.codex_scalp_probe_v1_min_target_gap_bps = Decimal::ZERO;
        config.codex_scalp_probe_v1_min_fresh_bps = Decimal::ZERO;
        config.codex_scalp_probe_v1_min_signal_bps = Decimal::ZERO;
        config.codex_scalp_probe_v1_min_flow_bps = Decimal::ZERO;
        config.codex_scalp_probe_v1_min_top_imbalance_bps = Decimal::ZERO;
        config.codex_scalp_probe_v1_min_depth_imbalance_bps = Decimal::ZERO;
        config.codex_scalp_probe_v1_min_radar_score_bps = Decimal::ZERO;
        config.codex_sentinel_v1_max_live_quote_age_ms = 3_000;
        config.bonereaper_state_v2_bias_min_target_gap_bps = Decimal::ZERO;
        config.bonereaper_state_v2_flip_max_target_gap_bps = Decimal::from(999);
        config.bonereaper_state_v2_min_signal_bps = 0;
        config.bonereaper_state_v2_min_spot_move_15s_bps = Decimal::ZERO;
        config.bonereaper_state_v2_min_spot_move_5s_bps = Decimal::ZERO;
        config.bonereaper_state_v2_min_aligned_flow_bps = Decimal::ZERO;

        let strategy = BundleArbitrageStrategy::new(config);
        let market = market();
        let mut books = HashMap::new();
        books.insert(
            "up-token".to_owned(),
            OrderBook {
                asset_id: "up-token".to_owned(),
                bids: vec![BookLevel {
                    price: decimal("0.58"),
                    size: decimal("150"),
                }],
                asks: vec![BookLevel {
                    price: decimal("0.60"),
                    size: decimal("150"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );
        books.insert(
            "down-token".to_owned(),
            OrderBook {
                asset_id: "down-token".to_owned(),
                bids: vec![BookLevel {
                    price: decimal("0.39"),
                    size: decimal("150"),
                }],
                asks: vec![BookLevel {
                    price: decimal("0.41"),
                    size: decimal("150"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );

        let mut context = test_codex_context("8.00", "0.00");
        context.current_spot_source = "Binance::Trade".to_owned();
        context.current_spot_received_age_ms = Some(25);
        context.exchange_book_age_ms = Some(30);
        context.exchange_book_top_imbalance_bps = Decimal::from(1_200);
        context.exchange_book_depth_imbalance_bps = Decimal::from(1_200);
        context.exchange_book_spread_bps = decimal("0.40");
        context.spot_move_5s_bps = decimal("14.00");
        context.spot_move_15s_bps = decimal("14.00");
        context.seconds_left = 275;
        let mut contexts = HashMap::new();
        contexts.insert(market.slug.clone(), context);

        let mut trade_flows = HashMap::new();
        trade_flows.insert(
            market.slug.clone(),
            TradeFlowSummary {
                signed_up_imbalance_bps: Decimal::from(700),
                trade_count: 12,
                ..TradeFlowSummary::default()
            },
        );

        let misses = strategy.find_near_misses(
            std::slice::from_ref(&market),
            &books,
            &HashMap::new(),
            &contexts,
            &trade_flows,
            1,
        );

        assert_eq!(misses.len(), 1);
        assert_eq!(misses[0].kind, OpportunityKind::CodexScalpProbeV1);
        assert!(
            misses[0].reason.contains("signal quality is too weak"),
            "raw-light near miss must report the live-burst gate, got: {}",
            misses[0].reason
        );
        assert!(
            misses[0].shortfall_label.contains("fresh 0.00"),
            "raw-light near miss must not reuse stale 5s momentum as freshness, got: {}",
            misses[0].shortfall_label
        );
    }

    #[test]
    fn codex_scalp_probe_v1_raw_light_respects_configured_v3_entry_window() {
        let mut config = strategy_config();
        config.codex_scalp_probe_v1_raw_ablation_enabled = true;
        config.codex_scalp_probe_v1_raw_light_enabled = true;
        config.codex_scalp_probe_v1_min_entry_price = decimal("0.56");
        config.codex_scalp_probe_v1_max_entry_price = decimal("0.68");
        config.codex_scalp_probe_v1_min_seconds_left = 240;
        config.codex_scalp_probe_v1_max_seconds_left = 290;
        config.codex_sentinel_v1_max_live_quote_age_ms = 3_000;
        config.codex_scalp_probe_v1_max_book_age_ms = 1_000;
        config.codex_scalp_probe_v1_max_entry_spread = decimal("0.20");
        config.codex_scalp_probe_v1_max_exchange_spread_bps = decimal("10.00");

        let mut context = test_codex_context("8.00", "5.00");
        context.target = MarketTarget::Btc5m;
        context.current_spot_source = "Binance::Trade".to_owned();
        context.current_spot_received_age_ms = Some(25);
        context.exchange_book_age_ms = Some(30);
        context.exchange_book_top_imbalance_bps = Decimal::from(1_200);
        context.exchange_book_depth_imbalance_bps = Decimal::from(1_200);
        context.exchange_book_spread_bps = decimal("0.40");
        context.seconds_left = 270;
        let mut decision = test_codex_decision("5.00", "5.00");
        decision.signal_strength_bps = Decimal::from(24);
        let book = OrderBook {
            asset_id: "up".to_owned(),
            bids: vec![BookLevel {
                price: decimal("0.57"),
                size: decimal("100"),
            }],
            asks: vec![BookLevel {
                price: decimal("0.59"),
                size: decimal("100"),
            }],
            min_order_size: None,
            tick_size: None,
        };

        assert!(codex_scalp_probe_v1_allows(
            &context,
            &decision,
            decimal("0.59"),
            &book,
            &config,
        ));
        assert!(!codex_scalp_probe_v1_allows(
            &context,
            &decision,
            decimal("0.49"),
            &book,
            &config,
        ));

        context.seconds_left = 200;
        assert!(!codex_scalp_probe_v1_allows(
            &context,
            &decision,
            decimal("0.59"),
            &book,
            &config,
        ));
    }

    #[test]
    fn codex_scalp_probe_v1_bnb_pressure_allows_stronger_bnb_bucket() {
        let mut config = strategy_config();
        config.codex_scalp_probe_v1_bnb_pressure_enabled = true;
        let mut context = test_codex_context("0.80", "0.12");
        context.target = MarketTarget::Bnb5m;
        context.current_spot_source = "Coinbase::Ticker".to_owned();
        context.current_spot_received_age_ms = Some(20);
        context.exchange_book_age_ms = Some(340);
        context.exchange_book_top_imbalance_bps = Decimal::from(1_550);
        context.exchange_book_depth_imbalance_bps = Decimal::from(1_350);
        context.exchange_book_spread_bps = decimal("0.40");
        let mut decision = test_codex_decision("0.12", "0.80");
        decision.aligned_flow_bps = Decimal::from(750);
        decision.signal_strength_bps = Decimal::from(500);
        let book = OrderBook {
            asset_id: "up".to_owned(),
            bids: vec![BookLevel {
                price: decimal("0.53"),
                size: decimal("100"),
            }],
            asks: vec![BookLevel {
                price: decimal("0.57"),
                size: decimal("100"),
            }],
            min_order_size: None,
            tick_size: None,
        };

        assert!(codex_scalp_probe_v1_allows(
            &context,
            &decision,
            decimal("0.57"),
            &book,
            &config,
        ));
        assert_eq!(
            codex_scalp_probe_v1_min_expected_profit_usdc(
                &context,
                &decision,
                decimal("0.57"),
                &book,
                &config,
            ),
            decimal("0.05")
        );
    }

    #[test]
    fn codex_scalp_probe_v1_bnb_pressure_does_not_relax_other_assets() {
        let mut config = strategy_config();
        config.codex_scalp_probe_v1_bnb_pressure_enabled = true;
        let mut context = test_codex_context("0.80", "0.12");
        context.target = MarketTarget::Xrp5m;
        context.current_spot_source = "Coinbase::Ticker".to_owned();
        context.current_spot_received_age_ms = Some(20);
        context.exchange_book_age_ms = Some(340);
        context.exchange_book_top_imbalance_bps = Decimal::from(1_550);
        context.exchange_book_depth_imbalance_bps = Decimal::from(1_350);
        context.exchange_book_spread_bps = decimal("0.40");
        let mut decision = test_codex_decision("0.12", "0.80");
        decision.aligned_flow_bps = Decimal::from(500);
        decision.signal_strength_bps = Decimal::from(400);
        let book = OrderBook {
            asset_id: "up".to_owned(),
            bids: vec![BookLevel {
                price: decimal("0.53"),
                size: decimal("100"),
            }],
            asks: vec![BookLevel {
                price: decimal("0.57"),
                size: decimal("100"),
            }],
            min_order_size: None,
            tick_size: None,
        };

        assert!(!codex_scalp_probe_v1_allows(
            &context,
            &decision,
            decimal("0.57"),
            &book,
            &config,
        ));
    }

    #[test]
    fn codex_sentinel_v1_live_quote_age_guard_allows_recently_received_high_gap_value_entry() {
        let mut config = strategy_config();
        config.codex_sentinel_v1_live_quote_age_guard_enabled = true;
        config.codex_sentinel_v1_max_live_quote_age_ms = 750;
        let mut context = test_codex_context("6.50", "3.60");
        context.current_spot_source = "Coinbase::Ticker".to_owned();
        context.current_spot_event_age_ms = Some(856);
        context.current_spot_received_age_ms = Some(9);

        assert!(!codex_sentinel_v1_live_quote_age_guard_blocks(
            &context, &config,
        ));
    }

    #[test]
    fn codex_sentinel_v1_live_quote_age_guard_blocks_stale_received_high_gap_value_entry() {
        let mut config = strategy_config();
        config.codex_sentinel_v1_live_quote_age_guard_enabled = true;
        config.codex_sentinel_v1_max_live_quote_age_ms = 750;
        let mut context = test_codex_context("6.50", "3.60");
        context.current_spot_source = "Coinbase::Ticker".to_owned();
        context.current_spot_event_age_ms = Some(900);
        context.current_spot_received_age_ms = Some(900);

        assert!(codex_sentinel_v1_live_quote_age_guard_blocks(
            &context, &config,
        ));
    }

    #[test]
    fn codex_sentinel_v1_live_quote_age_guard_allows_fresh_live_quote_entry() {
        let mut config = strategy_config();
        config.codex_sentinel_v1_live_quote_age_guard_enabled = true;
        config.codex_sentinel_v1_max_live_quote_age_ms = 750;
        let mut context = test_codex_context("6.50", "3.60");
        context.current_spot_source = "Coinbase::Ticker".to_owned();
        context.current_spot_event_age_ms = Some(433);
        context.current_spot_received_age_ms = Some(40);

        assert!(!codex_sentinel_v1_live_quote_age_guard_blocks(
            &context, &config,
        ));
    }

    #[test]
    fn codex_sentinel_v1_blocks_wide_entry_spread_for_scalp() {
        let mut config = strategy_config();
        config.codex_sentinel_v1_entry_spread_guard_enabled = true;
        config.codex_sentinel_v1_max_entry_spread = decimal("0.05");
        let book = OrderBook {
            asset_id: "up".to_owned(),
            bids: vec![BookLevel {
                price: decimal("0.52"),
                size: decimal("100"),
            }],
            asks: vec![BookLevel {
                price: decimal("0.60"),
                size: decimal("100"),
            }],
            min_order_size: None,
            tick_size: None,
        };

        assert!(codex_sentinel_v1_entry_spread_guard_blocks(
            decimal("0.60"),
            &book,
            &config,
        ));
    }

    #[test]
    fn codex_sentinel_v1_allows_tight_entry_spread_for_scalp() {
        let mut config = strategy_config();
        config.codex_sentinel_v1_entry_spread_guard_enabled = true;
        config.codex_sentinel_v1_max_entry_spread = decimal("0.05");
        let book = OrderBook {
            asset_id: "up".to_owned(),
            bids: vec![BookLevel {
                price: decimal("0.56"),
                size: decimal("100"),
            }],
            asks: vec![BookLevel {
                price: decimal("0.60"),
                size: decimal("100"),
            }],
            min_order_size: None,
            tick_size: None,
        };

        assert!(!codex_sentinel_v1_entry_spread_guard_blocks(
            decimal("0.60"),
            &book,
            &config,
        ));
    }

    #[test]
    fn codex_sentinel_v1_blocks_early_premium_chase_without_extreme_quality() {
        let mut config = strategy_config();
        config.codex_sentinel_v1_no_chase_guard_enabled = true;
        config.codex_sentinel_v1_no_chase_entry_price = decimal("0.62");
        config.codex_sentinel_v1_no_chase_min_seconds_left = 240;
        config.codex_sentinel_v1_no_chase_allow_min_target_gap_bps = decimal("8.00");
        config.codex_sentinel_v1_no_chase_allow_min_fresh_bps = decimal("4.00");
        config.codex_sentinel_v1_no_chase_allow_min_signal_bps = Decimal::from(2500);
        config.codex_sentinel_v1_no_chase_allow_min_flow_bps = Decimal::from(4000);
        let mut decision = test_codex_decision("2.00", "2.00");
        decision.signal_strength_bps = Decimal::from(2300);
        decision.aligned_flow_bps = Decimal::from(3900);
        let mut context = test_codex_context("6.80", "2.00");
        context.seconds_left = 255;

        assert!(codex_sentinel_v1_no_chase_guard_blocks(
            &context,
            &decision,
            decimal("0.69"),
            &config,
        ));
    }

    #[test]
    fn codex_sentinel_v1_allows_early_premium_chase_with_extreme_quality() {
        let mut config = strategy_config();
        config.codex_sentinel_v1_no_chase_guard_enabled = true;
        config.codex_sentinel_v1_no_chase_entry_price = decimal("0.62");
        config.codex_sentinel_v1_no_chase_min_seconds_left = 240;
        config.codex_sentinel_v1_no_chase_allow_min_target_gap_bps = decimal("8.00");
        config.codex_sentinel_v1_no_chase_allow_min_fresh_bps = decimal("4.00");
        config.codex_sentinel_v1_no_chase_allow_min_signal_bps = Decimal::from(2500);
        config.codex_sentinel_v1_no_chase_allow_min_flow_bps = Decimal::from(4000);
        let mut decision = test_codex_decision("4.25", "4.25");
        decision.signal_strength_bps = Decimal::from(2600);
        decision.aligned_flow_bps = Decimal::from(4200);
        let mut context = test_codex_context("8.50", "4.25");
        context.seconds_left = 255;

        assert!(!codex_sentinel_v1_no_chase_guard_blocks(
            &context,
            &decision,
            decimal("0.69"),
            &config,
        ));
    }

    #[test]
    fn codex_sentinel_v1_quality_floor_blocks_tiny_gap_even_with_flow() {
        let mut config = strategy_config();
        config.codex_sentinel_v1_quality_floor_enabled = true;
        config.codex_sentinel_v1_quality_floor_min_target_gap_bps = decimal("1.50");
        config.codex_sentinel_v1_quality_floor_mid_gap_max_bps = decimal("3.00");
        config.codex_sentinel_v1_quality_floor_mid_gap_min_signal_bps = Decimal::from(1500);
        config.codex_sentinel_v1_quality_floor_mid_gap_min_flow_bps = Decimal::from(2000);
        let mut decision = test_codex_decision("0", "0.63");
        decision.signal_strength_bps = Decimal::from(1800);
        decision.aligned_flow_bps = Decimal::from(3000);
        let context = test_codex_context("0.58", "0");

        assert!(codex_sentinel_v1_quality_floor_blocks(
            &context, &decision, &config,
        ));
    }

    #[test]
    fn codex_sentinel_v1_quality_floor_blocks_mid_gap_with_weak_signal() {
        let mut config = strategy_config();
        config.codex_sentinel_v1_quality_floor_enabled = true;
        config.codex_sentinel_v1_quality_floor_min_target_gap_bps = decimal("1.50");
        config.codex_sentinel_v1_quality_floor_mid_gap_max_bps = decimal("3.00");
        config.codex_sentinel_v1_quality_floor_mid_gap_min_signal_bps = Decimal::from(1500);
        config.codex_sentinel_v1_quality_floor_mid_gap_min_flow_bps = Decimal::from(2000);
        let mut decision = test_codex_decision("0", "1.34");
        decision.signal_strength_bps = Decimal::from(777);
        decision.aligned_flow_bps = Decimal::from(1287);
        let context = test_codex_context("2.20", "0");

        assert!(codex_sentinel_v1_quality_floor_blocks(
            &context, &decision, &config,
        ));
    }

    #[test]
    fn codex_sentinel_v1_quality_floor_allows_mid_gap_with_strong_quality() {
        let mut config = strategy_config();
        config.codex_sentinel_v1_quality_floor_enabled = true;
        config.codex_sentinel_v1_quality_floor_min_target_gap_bps = decimal("1.50");
        config.codex_sentinel_v1_quality_floor_mid_gap_max_bps = decimal("3.00");
        config.codex_sentinel_v1_quality_floor_mid_gap_min_signal_bps = Decimal::from(1500);
        config.codex_sentinel_v1_quality_floor_mid_gap_min_flow_bps = Decimal::from(2000);
        let mut decision = test_codex_decision("0.15", "1.38");
        decision.signal_strength_bps = Decimal::from(2141);
        decision.aligned_flow_bps = Decimal::from(3558);
        let context = test_codex_context("2.62", "-0.15");

        assert!(!codex_sentinel_v1_quality_floor_blocks(
            &context, &decision, &config,
        ));
    }

    #[test]
    fn codex_sentinel_v1_mid_gap_premium_guard_blocks_weak_chase() {
        let mut config = strategy_config();
        config.codex_sentinel_v1_mid_gap_premium_guard_enabled = true;
        config.codex_sentinel_v1_mid_gap_premium_entry_price = decimal("0.56");
        config.codex_sentinel_v1_mid_gap_premium_min_target_gap_bps = decimal("1.50");
        config.codex_sentinel_v1_mid_gap_premium_max_target_gap_bps = decimal("3.00");
        config.codex_sentinel_v1_mid_gap_premium_min_signal_bps = Decimal::from(800);
        config.codex_sentinel_v1_mid_gap_premium_min_flow_bps = Decimal::from(1200);
        config.codex_sentinel_v1_mid_gap_premium_min_fresh_bps = decimal("1.25");
        let mut decision = test_codex_decision("0.68", "0.68");
        decision.signal_strength_bps = Decimal::from(625);
        decision.aligned_flow_bps = Decimal::from(1035);
        let context = test_codex_context("1.74", "0.68");

        assert!(codex_sentinel_v1_mid_gap_premium_guard_blocks(
            &context,
            &decision,
            decimal("0.58"),
            &config,
        ));
    }

    #[test]
    fn codex_sentinel_v1_mid_gap_premium_guard_allows_confirmed_value() {
        let mut config = strategy_config();
        config.codex_sentinel_v1_mid_gap_premium_guard_enabled = true;
        config.codex_sentinel_v1_mid_gap_premium_entry_price = decimal("0.56");
        config.codex_sentinel_v1_mid_gap_premium_min_target_gap_bps = decimal("1.50");
        config.codex_sentinel_v1_mid_gap_premium_max_target_gap_bps = decimal("3.00");
        config.codex_sentinel_v1_mid_gap_premium_min_signal_bps = Decimal::from(800);
        config.codex_sentinel_v1_mid_gap_premium_min_flow_bps = Decimal::from(1200);
        config.codex_sentinel_v1_mid_gap_premium_min_fresh_bps = decimal("1.25");
        let mut decision = test_codex_decision("1.92", "1.92");
        decision.signal_strength_bps = Decimal::from(878);
        decision.aligned_flow_bps = Decimal::from(1449);
        let context = test_codex_context("2.89", "1.92");

        assert!(!codex_sentinel_v1_mid_gap_premium_guard_blocks(
            &context,
            &decision,
            decimal("0.60"),
            &config,
        ));
    }

    #[test]
    fn codex_sentinel_v1_mid_gap_premium_guard_ignores_high_gap_scalps() {
        let mut config = strategy_config();
        config.codex_sentinel_v1_mid_gap_premium_guard_enabled = true;
        config.codex_sentinel_v1_mid_gap_premium_entry_price = decimal("0.56");
        config.codex_sentinel_v1_mid_gap_premium_min_target_gap_bps = decimal("1.50");
        config.codex_sentinel_v1_mid_gap_premium_max_target_gap_bps = decimal("3.00");
        let mut decision = test_codex_decision("0.20", "0.20");
        decision.signal_strength_bps = Decimal::from(150);
        decision.aligned_flow_bps = Decimal::from(250);
        let context = test_codex_context("3.42", "0.20");

        assert!(!codex_sentinel_v1_mid_gap_premium_guard_blocks(
            &context,
            &decision,
            decimal("0.62"),
            &config,
        ));
    }

    #[test]
    fn codex_sentinel_v1_late_override_requires_fresh_quality() {
        let mut config = strategy_config();
        config.codex_sentinel_v1_late_entry_override_enabled = true;
        config.bonereaper_state_v2_min_seconds_left = 90;
        config.codex_sentinel_v1_late_entry_min_seconds_left = 60;
        config.codex_sentinel_v1_late_entry_max_entry_price = decimal("0.62");
        config.codex_sentinel_v1_late_entry_min_signal_bps = Decimal::from(850);
        config.codex_sentinel_v1_late_entry_min_fresh_bps = decimal("1.50");
        config.codex_sentinel_v1_late_entry_min_target_gap_bps = decimal("1.50");
        let mut decision = test_codex_decision("0.10", "1.60");
        decision.signal_strength_bps = Decimal::from(900);
        let mut context = test_codex_context("1.80", "0.10");
        context.seconds_left = 75;

        assert!(!codex_sentinel_v1_late_entry_override_allows(
            &context,
            &decision,
            decimal("0.60"),
            &config,
        ));
    }

    #[test]
    fn codex_sentinel_v1_late_override_allows_high_quality_momentum() {
        let mut config = strategy_config();
        config.codex_sentinel_v1_late_entry_override_enabled = true;
        config.bonereaper_state_v2_min_seconds_left = 90;
        config.codex_sentinel_v1_late_entry_min_seconds_left = 60;
        config.codex_sentinel_v1_late_entry_max_entry_price = decimal("0.62");
        config.codex_sentinel_v1_late_entry_min_signal_bps = Decimal::from(850);
        config.codex_sentinel_v1_late_entry_min_fresh_bps = decimal("1.50");
        config.codex_sentinel_v1_late_entry_min_target_gap_bps = decimal("1.50");
        let mut decision = test_codex_decision("1.60", "1.80");
        decision.signal_strength_bps = Decimal::from(900);
        let mut context = test_codex_context("1.80", "1.60");
        context.seconds_left = 75;

        assert!(codex_sentinel_v1_late_entry_override_allows(
            &context,
            &decision,
            decimal("0.60"),
            &config,
        ));
    }

    #[test]
    fn codex_sentinel_v1_late_window_value_guard_blocks_expensive_average_setup() {
        let mut config = strategy_config();
        config.codex_sentinel_v1_late_window_value_guard_enabled = true;
        config.codex_sentinel_v1_late_window_max_seconds_left = 180;
        config.codex_sentinel_v1_late_window_max_entry_price = decimal("0.62");
        config.codex_sentinel_v1_late_window_allow_min_signal_bps = Decimal::from(1600);
        config.codex_sentinel_v1_late_window_allow_min_fresh_bps = decimal("2.00");
        config.codex_sentinel_v1_late_window_allow_min_flow_bps = Decimal::from(2500);
        config.codex_sentinel_v1_late_window_allow_min_target_gap_bps = decimal("3.00");
        let mut decision = test_codex_decision("1.30", "3.79");
        decision.signal_strength_bps = Decimal::from(656);
        decision.aligned_flow_bps = Decimal::from(1082);
        let mut context = test_codex_context("3.79", "1.30");
        context.seconds_left = 91;

        assert!(codex_sentinel_v1_late_window_value_guard_blocks(
            &context,
            &decision,
            decimal("0.66"),
            &config,
        ));
    }

    #[test]
    fn codex_sentinel_v1_late_window_value_guard_allows_discount_entry() {
        let mut config = strategy_config();
        config.codex_sentinel_v1_late_window_value_guard_enabled = true;
        config.codex_sentinel_v1_late_window_max_seconds_left = 180;
        config.codex_sentinel_v1_late_window_max_entry_price = decimal("0.62");
        let decision = test_codex_decision("0.50", "2.00");
        let mut context = test_codex_context("2.00", "0.50");
        context.seconds_left = 120;

        assert!(!codex_sentinel_v1_late_window_value_guard_blocks(
            &context,
            &decision,
            decimal("0.60"),
            &config,
        ));
    }

    #[test]
    fn codex_sentinel_v1_late_window_value_guard_allows_extreme_quality() {
        let mut config = strategy_config();
        config.codex_sentinel_v1_late_window_value_guard_enabled = true;
        config.codex_sentinel_v1_late_window_max_seconds_left = 180;
        config.codex_sentinel_v1_late_window_max_entry_price = decimal("0.62");
        config.codex_sentinel_v1_late_window_allow_min_signal_bps = Decimal::from(1600);
        config.codex_sentinel_v1_late_window_allow_min_fresh_bps = decimal("2.00");
        config.codex_sentinel_v1_late_window_allow_min_flow_bps = Decimal::from(2500);
        config.codex_sentinel_v1_late_window_allow_min_target_gap_bps = decimal("3.00");
        let mut decision = test_codex_decision("2.40", "3.40");
        decision.signal_strength_bps = Decimal::from(1800);
        decision.aligned_flow_bps = Decimal::from(2800);
        let mut context = test_codex_context("3.40", "2.40");
        context.seconds_left = 120;

        assert!(!codex_sentinel_v1_late_window_value_guard_blocks(
            &context,
            &decision,
            decimal("0.66"),
            &config,
        ));
    }

    fn market() -> BinaryMarket {
        BinaryMarket {
            condition_id: "cond-1".to_owned(),
            slug: "btc-updown-5m-1772375100".to_owned(),
            question: "Edge market?".to_owned(),
            outcome_a_label: "Up".to_owned(),
            outcome_a_token_id: "up-token".to_owned(),
            outcome_b_label: "Down".to_owned(),
            outcome_b_token_id: "down-token".to_owned(),
            end_date: Some(Utc::now() + Duration::minutes(60)),
            liquidity_usdc: decimal("5000"),
            target_price: None,
            target_price_source: None,
            final_reference_price: None,
        }
    }

    #[test]
    fn strategy_finds_positive_bundle_edge() {
        let strategy = BundleArbitrageStrategy::new(strategy_config());
        let market = market();

        let mut books = HashMap::new();
        books.insert(
            "up-token".to_owned(),
            OrderBook {
                asset_id: "up-token".to_owned(),
                bids: Vec::new(),
                asks: vec![BookLevel {
                    price: decimal("0.46"),
                    size: decimal("50"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );
        books.insert(
            "down-token".to_owned(),
            OrderBook {
                asset_id: "down-token".to_owned(),
                bids: Vec::new(),
                asks: vec![BookLevel {
                    price: decimal("0.47"),
                    size: decimal("50"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );

        let mut contexts = HashMap::new();
        contexts.insert(
            market.slug.clone(),
            BtcFiveMinuteContext {
                target: MarketTarget::Btc5m,
                interval_open_price: decimal("67000"),
                target_price: decimal("67000"),
                target_price_source: crate::models::TargetPriceSource::BinanceWindowOpenFallback,
                target_gap_bps: decimal("8.95"),
                current_spot_price: decimal("67060"),
                current_spot_source: "test-fixture".to_owned(),
                current_spot_event_age_ms: None,
                current_spot_received_age_ms: None,
                current_spot_quote_points: None,
                exchange_book_age_ms: None,
                exchange_book_top_imbalance_bps: Decimal::ZERO,
                exchange_book_depth_imbalance_bps: Decimal::ZERO,
                exchange_book_microprice_bps: Decimal::ZERO,
                exchange_book_spread_bps: Decimal::ZERO,
                micro_burst_reference_price: decimal("67060"),
                micro_reference_price: decimal("67060"),
                spot_move_bps: decimal("8.95"),
                spot_move_1s_bps: Decimal::ZERO,
                spot_move_5s_bps: Decimal::ZERO,
                spot_move_15s_bps: Decimal::ZERO,
                micro_acceleration_bps: decimal("0.8"),
                dominant_outcome: "Up".to_owned(),
                seconds_left: 120,
            },
        );

        let mut trade_flows = HashMap::new();
        trade_flows.insert(
            market.slug.clone(),
            TradeFlowSummary {
                signed_up_imbalance_bps: decimal("2.1"),
                trade_count: 5,
                ..TradeFlowSummary::default()
            },
        );

        let opportunities = strategy.find_opportunities(
            &[market],
            &books,
            &HashMap::new(),
            &contexts,
            &trade_flows,
        );
        assert_eq!(opportunities.len(), 1);
        assert_eq!(opportunities[0].kind, OpportunityKind::BundleArbitrage);
        assert_eq!(opportunities[0].edge_bps, 700);
        assert_eq!(opportunities[0].primary_outcome_label, "Up");
        if opportunities[0].primary_outcome_label != "Up" {
            assert_eq!(opportunities[0].primary_outcome_label, "Up");
        }
    }

    #[test]
    fn strategy_requires_minimum_spot_move_for_bundle() {
        let mut config = strategy_config();
        config.enable_directional = false;
        config.min_spot_move_bps = 15;
        let strategy = BundleArbitrageStrategy::new(config);
        let market = market();

        let mut books = HashMap::new();
        books.insert(
            "up-token".to_owned(),
            OrderBook {
                asset_id: "up-token".to_owned(),
                bids: Vec::new(),
                asks: vec![BookLevel {
                    price: decimal("0.46"),
                    size: decimal("50"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );
        books.insert(
            "down-token".to_owned(),
            OrderBook {
                asset_id: "down-token".to_owned(),
                bids: Vec::new(),
                asks: vec![BookLevel {
                    price: decimal("0.47"),
                    size: decimal("50"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );

        let mut contexts = HashMap::new();
        contexts.insert(
            market.slug.clone(),
            BtcFiveMinuteContext {
                target: MarketTarget::Btc5m,
                interval_open_price: decimal("67000"),
                target_price: decimal("67000"),
                target_price_source: crate::models::TargetPriceSource::BinanceWindowOpenFallback,
                target_gap_bps: decimal("5.97"),
                current_spot_price: decimal("67040"),
                current_spot_source: "test-fixture".to_owned(),
                current_spot_event_age_ms: None,
                current_spot_received_age_ms: None,
                current_spot_quote_points: None,
                exchange_book_age_ms: None,
                exchange_book_top_imbalance_bps: Decimal::ZERO,
                exchange_book_depth_imbalance_bps: Decimal::ZERO,
                exchange_book_microprice_bps: Decimal::ZERO,
                exchange_book_spread_bps: Decimal::ZERO,
                micro_burst_reference_price: decimal("67040"),
                micro_reference_price: decimal("67040"),
                spot_move_bps: decimal("5.97"),
                spot_move_1s_bps: Decimal::ZERO,
                spot_move_5s_bps: Decimal::ZERO,
                spot_move_15s_bps: Decimal::ZERO,
                micro_acceleration_bps: Decimal::ZERO,
                dominant_outcome: "Up".to_owned(),
                seconds_left: 120,
            },
        );

        let opportunities = strategy.find_opportunities(
            &[market],
            &books,
            &HashMap::new(),
            &contexts,
            &HashMap::new(),
        );
        assert!(opportunities.is_empty());
    }

    #[test]
    fn strategy_falls_back_to_directional_signal() {
        let strategy = BundleArbitrageStrategy::new(strategy_config());
        let market = market();

        let mut books = HashMap::new();
        books.insert(
            "up-token".to_owned(),
            OrderBook {
                asset_id: "up-token".to_owned(),
                bids: vec![BookLevel {
                    price: decimal("0.51"),
                    size: decimal("100"),
                }],
                asks: vec![
                    BookLevel {
                        price: decimal("0.99"),
                        size: decimal("1000"),
                    },
                    BookLevel {
                        price: decimal("0.54"),
                        size: decimal("120"),
                    },
                ],
                min_order_size: None,
                tick_size: None,
            },
        );
        books.insert(
            "down-token".to_owned(),
            OrderBook {
                asset_id: "down-token".to_owned(),
                bids: vec![BookLevel {
                    price: decimal("0.45"),
                    size: decimal("100"),
                }],
                asks: vec![
                    BookLevel {
                        price: decimal("0.99"),
                        size: decimal("1000"),
                    },
                    BookLevel {
                        price: decimal("0.48"),
                        size: decimal("120"),
                    },
                ],
                min_order_size: None,
                tick_size: None,
            },
        );

        let mut contexts = HashMap::new();
        contexts.insert(
            market.slug.clone(),
            BtcFiveMinuteContext {
                target: MarketTarget::Btc5m,
                interval_open_price: decimal("67000"),
                target_price: decimal("67000"),
                target_price_source: crate::models::TargetPriceSource::BinanceWindowOpenFallback,
                target_gap_bps: decimal("20.90"),
                current_spot_price: decimal("67140"),
                current_spot_source: "test-fixture".to_owned(),
                current_spot_event_age_ms: None,
                current_spot_received_age_ms: None,
                current_spot_quote_points: None,
                exchange_book_age_ms: None,
                exchange_book_top_imbalance_bps: Decimal::ZERO,
                exchange_book_depth_imbalance_bps: Decimal::ZERO,
                exchange_book_microprice_bps: Decimal::ZERO,
                exchange_book_spread_bps: Decimal::ZERO,
                micro_burst_reference_price: decimal("67140"),
                micro_reference_price: decimal("67140"),
                spot_move_bps: decimal("20.90"),
                spot_move_1s_bps: Decimal::ZERO,
                spot_move_5s_bps: Decimal::ZERO,
                spot_move_15s_bps: Decimal::ZERO,
                micro_acceleration_bps: Decimal::ZERO,
                dominant_outcome: "Up".to_owned(),
                seconds_left: 160,
            },
        );

        let opportunities = strategy.find_opportunities(
            &[market],
            &books,
            &HashMap::new(),
            &contexts,
            &HashMap::new(),
        );
        assert_eq!(opportunities.len(), 1);
        assert_eq!(opportunities[0].kind, OpportunityKind::DirectionalMomentum);
        assert_eq!(opportunities[0].primary_outcome_label, "Up");
        assert_eq!(opportunities[0].primary_outcome_ask_price, decimal("0.54"));
        assert!(opportunities[0].edge_bps >= 15);
    }

    #[test]
    fn strategy_can_open_micro_breakout_signal() {
        let mut config = strategy_config();
        config.enable_bundle = false;
        config.directional_min_velocity_bps_per_minute = 99;
        config.directional_min_signal_bps = 20;
        config.micro_breakout_min_spot_move_bps = 2;
        config.micro_breakout_min_spot_move_5s_bps = decimal("1.0");
        config.micro_breakout_min_signal_bps = 5;
        config.micro_breakout_signal_boost_multiplier = decimal("2.0");
        config.micro_breakout_max_entry_price = decimal("0.70");
        let strategy = BundleArbitrageStrategy::new(config);
        let market = market();

        let mut books = HashMap::new();
        books.insert(
            "up-token".to_owned(),
            OrderBook {
                asset_id: "up-token".to_owned(),
                bids: vec![BookLevel {
                    price: decimal("0.50"),
                    size: decimal("100"),
                }],
                asks: vec![BookLevel {
                    price: decimal("0.46"),
                    size: decimal("80"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );
        books.insert(
            "down-token".to_owned(),
            OrderBook {
                asset_id: "down-token".to_owned(),
                bids: vec![BookLevel {
                    price: decimal("0.40"),
                    size: decimal("100"),
                }],
                asks: vec![BookLevel {
                    price: decimal("0.44"),
                    size: decimal("90"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );

        let mut contexts = HashMap::new();
        contexts.insert(
            market.slug.clone(),
            BtcFiveMinuteContext {
                target: MarketTarget::Btc5m,
                interval_open_price: decimal("67000"),
                target_price: decimal("67000"),
                target_price_source: crate::models::TargetPriceSource::BinanceWindowOpenFallback,
                target_gap_bps: decimal("4.1791"),
                current_spot_price: decimal("67028"),
                current_spot_source: "test-fixture".to_owned(),
                current_spot_event_age_ms: None,
                current_spot_received_age_ms: None,
                current_spot_quote_points: None,
                exchange_book_age_ms: None,
                exchange_book_top_imbalance_bps: Decimal::ZERO,
                exchange_book_depth_imbalance_bps: Decimal::ZERO,
                exchange_book_microprice_bps: Decimal::ZERO,
                exchange_book_spread_bps: Decimal::ZERO,
                micro_burst_reference_price: decimal("67022"),
                micro_reference_price: decimal("67012"),
                spot_move_bps: decimal("4.1791"),
                spot_move_1s_bps: decimal("0.9"),
                spot_move_5s_bps: decimal("2.3877"),
                spot_move_15s_bps: decimal("2.3877"),
                micro_acceleration_bps: decimal("0.8"),
                dominant_outcome: "Up".to_owned(),
                seconds_left: 240,
            },
        );

        let mut trade_flows = HashMap::new();
        trade_flows.insert(
            market.slug.clone(),
            TradeFlowSummary {
                signed_up_imbalance_bps: decimal("2.1"),
                trade_count: 5,
                ..TradeFlowSummary::default()
            },
        );

        let opportunities = strategy.find_opportunities(
            &[market],
            &books,
            &HashMap::new(),
            &contexts,
            &trade_flows,
        );
        assert_eq!(opportunities.len(), 1);
        assert_eq!(opportunities[0].kind, OpportunityKind::MicroBreakout);
        assert_eq!(opportunities[0].primary_outcome_label, "Up");
        assert!(opportunities[0].expected_profit > Decimal::ZERO);
    }

    #[test]
    fn strategy_blocks_micro_breakout_too_early_in_window() {
        let mut config = strategy_config();
        config.enable_bundle = false;
        config.directional_min_velocity_bps_per_minute = 99;
        config.directional_min_signal_bps = 20;
        config.micro_breakout_min_spot_move_bps = 2;
        config.micro_breakout_min_spot_move_5s_bps = decimal("1.0");
        config.micro_breakout_min_signal_bps = 5;
        config.micro_breakout_signal_boost_multiplier = decimal("2.0");
        config.micro_breakout_max_entry_price = decimal("0.70");
        config.micro_breakout_min_elapsed_window_secs = 61;
        let strategy = BundleArbitrageStrategy::new(config);
        let market = market();

        let mut books = HashMap::new();
        books.insert(
            "up-token".to_owned(),
            OrderBook {
                asset_id: "up-token".to_owned(),
                bids: vec![BookLevel {
                    price: decimal("0.50"),
                    size: decimal("100"),
                }],
                asks: vec![BookLevel {
                    price: decimal("0.46"),
                    size: decimal("80"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );
        books.insert(
            "down-token".to_owned(),
            OrderBook {
                asset_id: "down-token".to_owned(),
                bids: vec![BookLevel {
                    price: decimal("0.40"),
                    size: decimal("100"),
                }],
                asks: vec![BookLevel {
                    price: decimal("0.44"),
                    size: decimal("90"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );

        let mut contexts = HashMap::new();
        contexts.insert(
            market.slug.clone(),
            BtcFiveMinuteContext {
                target: MarketTarget::Btc5m,
                interval_open_price: decimal("67000"),
                target_price: decimal("67000"),
                target_price_source: crate::models::TargetPriceSource::BinanceWindowOpenFallback,
                target_gap_bps: decimal("4.1791"),
                current_spot_price: decimal("67028"),
                current_spot_source: "test-fixture".to_owned(),
                current_spot_event_age_ms: None,
                current_spot_received_age_ms: None,
                current_spot_quote_points: None,
                exchange_book_age_ms: None,
                exchange_book_top_imbalance_bps: Decimal::ZERO,
                exchange_book_depth_imbalance_bps: Decimal::ZERO,
                exchange_book_microprice_bps: Decimal::ZERO,
                exchange_book_spread_bps: Decimal::ZERO,
                micro_burst_reference_price: decimal("67022"),
                micro_reference_price: decimal("67012"),
                spot_move_bps: decimal("4.1791"),
                spot_move_1s_bps: decimal("0.9"),
                spot_move_5s_bps: decimal("2.3877"),
                spot_move_15s_bps: decimal("2.3877"),
                micro_acceleration_bps: decimal("0.8"),
                dominant_outcome: "Up".to_owned(),
                seconds_left: 240,
            },
        );

        let mut trade_flows = HashMap::new();
        trade_flows.insert(
            market.slug.clone(),
            TradeFlowSummary {
                signed_up_imbalance_bps: decimal("2.1"),
                trade_count: 5,
                ..TradeFlowSummary::default()
            },
        );

        let opportunities = strategy.find_opportunities(
            &[market],
            &books,
            &HashMap::new(),
            &contexts,
            &trade_flows,
        );
        assert!(opportunities.is_empty());
    }

    #[test]
    fn strategy_can_unlock_micro_breakout_from_one_second_burst() {
        let mut config = strategy_config();
        config.enable_bundle = false;
        config.directional_min_velocity_bps_per_minute = 99;
        config.directional_min_signal_bps = 99;
        config.micro_breakout_min_spot_move_bps = 1;
        config.micro_breakout_min_spot_move_5s_bps = decimal("1.5");
        config.micro_breakout_min_spot_move_1s_bps = decimal("0.6");
        config.micro_breakout_min_signal_bps = 1;
        config.micro_breakout_signal_boost_multiplier = decimal("2.0");
        config.micro_breakout_signal_burst_multiplier = decimal("1.4");
        config.micro_breakout_strong_signal_min_spot_move_1s_bps = decimal("0.9");
        config.micro_breakout_max_entry_price = decimal("0.70");
        let strategy = BundleArbitrageStrategy::new(config);
        let market = market();

        let mut books = HashMap::new();
        books.insert(
            "up-token".to_owned(),
            OrderBook {
                asset_id: "up-token".to_owned(),
                bids: vec![BookLevel {
                    price: decimal("0.50"),
                    size: decimal("100"),
                }],
                asks: vec![BookLevel {
                    price: decimal("0.46"),
                    size: decimal("80"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );
        books.insert(
            "down-token".to_owned(),
            OrderBook {
                asset_id: "down-token".to_owned(),
                bids: vec![BookLevel {
                    price: decimal("0.40"),
                    size: decimal("100"),
                }],
                asks: vec![BookLevel {
                    price: decimal("0.44"),
                    size: decimal("90"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );

        let mut contexts = HashMap::new();
        contexts.insert(
            market.slug.clone(),
            BtcFiveMinuteContext {
                target: MarketTarget::Btc5m,
                interval_open_price: decimal("67000"),
                target_price: decimal("67000"),
                target_price_source: crate::models::TargetPriceSource::BinanceWindowOpenFallback,
                target_gap_bps: decimal("4.0"),
                current_spot_price: decimal("67027"),
                current_spot_source: "test-fixture".to_owned(),
                current_spot_event_age_ms: None,
                current_spot_received_age_ms: None,
                current_spot_quote_points: None,
                exchange_book_age_ms: None,
                exchange_book_top_imbalance_bps: Decimal::ZERO,
                exchange_book_depth_imbalance_bps: Decimal::ZERO,
                exchange_book_microprice_bps: Decimal::ZERO,
                exchange_book_spread_bps: Decimal::ZERO,
                micro_burst_reference_price: decimal("67021"),
                micro_reference_price: decimal("67020"),
                spot_move_bps: decimal("4.0"),
                spot_move_1s_bps: decimal("0.8"),
                spot_move_5s_bps: decimal("1.2"),
                spot_move_15s_bps: decimal("1.7"),
                micro_acceleration_bps: decimal("0.5"),
                dominant_outcome: "Up".to_owned(),
                seconds_left: 210,
            },
        );

        let mut trade_flows = HashMap::new();
        trade_flows.insert(
            market.slug.clone(),
            TradeFlowSummary {
                signed_up_imbalance_bps: decimal("1.8"),
                trade_count: 4,
                ..TradeFlowSummary::default()
            },
        );

        let opportunities = strategy.find_opportunities(
            &[market],
            &books,
            &HashMap::new(),
            &contexts,
            &trade_flows,
        );
        assert_eq!(opportunities.len(), 1);
        assert_eq!(opportunities[0].kind, OpportunityKind::MicroBreakout);
        assert_eq!(opportunities[0].primary_outcome_label, "Up");
    }

    #[test]
    fn detects_recent_target_cross_from_one_second_reference() {
        let mut config = strategy_config();
        config.micro_breakout_target_cross_min_gap_bps = decimal("1.0");

        let context = BtcFiveMinuteContext {
            target: MarketTarget::Btc5m,
            interval_open_price: decimal("67000"),
            target_price: decimal("67000"),
            target_price_source: TargetPriceSource::PolymarketEventMetadata,
            target_gap_bps: decimal("1.79"),
            current_spot_price: decimal("67012"),
            current_spot_source: "test-fixture".to_owned(),
            current_spot_event_age_ms: None,
            current_spot_received_age_ms: None,
            current_spot_quote_points: None,
            exchange_book_age_ms: None,
            exchange_book_top_imbalance_bps: Decimal::ZERO,
            exchange_book_depth_imbalance_bps: Decimal::ZERO,
            exchange_book_microprice_bps: Decimal::ZERO,
            exchange_book_spread_bps: Decimal::ZERO,
            micro_burst_reference_price: decimal("66998"),
            micro_reference_price: decimal("66990"),
            spot_move_bps: decimal("1.79"),
            spot_move_1s_bps: decimal("2.09"),
            spot_move_5s_bps: decimal("3.28"),
            spot_move_15s_bps: decimal("3.00"),
            micro_acceleration_bps: decimal("0.5"),
            dominant_outcome: "Up".to_owned(),
            seconds_left: 180,
        };

        assert_eq!(
            recent_target_cross(&context, &config),
            RecentTargetCross::OneSecond
        );
    }

    #[test]
    fn strategy_blocks_micro_breakout_when_burst_looks_like_spike() {
        let mut config = strategy_config();
        config.enable_bundle = false;
        config.directional_min_velocity_bps_per_minute = 99;
        config.directional_min_signal_bps = 99;
        config.micro_breakout_min_spot_move_bps = 1;
        config.micro_breakout_min_spot_move_5s_bps = decimal("1.0");
        config.micro_breakout_min_spot_move_1s_bps = decimal("0.6");
        config.micro_breakout_min_signal_bps = 1;
        config.micro_breakout_signal_boost_multiplier = decimal("2.0");
        config.micro_breakout_signal_burst_multiplier = decimal("1.4");
        config.micro_breakout_max_burst_to_micro_ratio = decimal("1.10");
        config.micro_breakout_max_entry_price = decimal("0.70");
        let strategy = BundleArbitrageStrategy::new(config);
        let market = market();

        let mut books = HashMap::new();
        books.insert(
            "up-token".to_owned(),
            OrderBook {
                asset_id: "up-token".to_owned(),
                bids: vec![BookLevel {
                    price: decimal("0.50"),
                    size: decimal("100"),
                }],
                asks: vec![BookLevel {
                    price: decimal("0.46"),
                    size: decimal("80"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );
        books.insert(
            "down-token".to_owned(),
            OrderBook {
                asset_id: "down-token".to_owned(),
                bids: vec![BookLevel {
                    price: decimal("0.40"),
                    size: decimal("100"),
                }],
                asks: vec![BookLevel {
                    price: decimal("0.44"),
                    size: decimal("90"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );

        let mut contexts = HashMap::new();
        contexts.insert(
            market.slug.clone(),
            BtcFiveMinuteContext {
                target: MarketTarget::Btc5m,
                interval_open_price: decimal("67000"),
                target_price: decimal("67000"),
                target_price_source: crate::models::TargetPriceSource::BinanceWindowOpenFallback,
                target_gap_bps: decimal("4.0"),
                current_spot_price: decimal("67027"),
                current_spot_source: "test-fixture".to_owned(),
                current_spot_event_age_ms: None,
                current_spot_received_age_ms: None,
                current_spot_quote_points: None,
                exchange_book_age_ms: None,
                exchange_book_top_imbalance_bps: Decimal::ZERO,
                exchange_book_depth_imbalance_bps: Decimal::ZERO,
                exchange_book_microprice_bps: Decimal::ZERO,
                exchange_book_spread_bps: Decimal::ZERO,
                micro_burst_reference_price: decimal("67016"),
                micro_reference_price: decimal("67020"),
                spot_move_bps: decimal("4.0"),
                spot_move_1s_bps: decimal("1.6"),
                spot_move_5s_bps: decimal("1.2"),
                spot_move_15s_bps: decimal("1.8"),
                micro_acceleration_bps: decimal("0.5"),
                dominant_outcome: "Up".to_owned(),
                seconds_left: 210,
            },
        );

        let mut trade_flows = HashMap::new();
        trade_flows.insert(
            market.slug.clone(),
            TradeFlowSummary {
                signed_up_imbalance_bps: decimal("1.8"),
                trade_count: 4,
                ..TradeFlowSummary::default()
            },
        );

        let opportunities = strategy.find_opportunities(
            &[market],
            &books,
            &HashMap::new(),
            &contexts,
            &trade_flows,
        );
        assert!(opportunities.is_empty());
    }

    #[test]
    fn strategy_blocks_micro_breakout_when_fill_drift_is_too_large() {
        let mut config = strategy_config();
        config.enable_bundle = false;
        config.directional_min_velocity_bps_per_minute = 99;
        config.directional_min_signal_bps = 99;
        config.directional_confidence_bps_per_spot_bps = 420;
        config.directional_max_fair_price = decimal("0.95");
        config.micro_breakout_max_entry_price = decimal("0.90");
        config.micro_breakout_weak_notional_usdc = decimal("20");
        config.micro_breakout_normal_notional_usdc = decimal("20");
        config.micro_breakout_strong_notional_usdc = decimal("20");
        config.micro_breakout_max_average_price_drift = decimal("0.01");
        config.max_directional_notional_usdc = decimal("20");
        let strategy = BundleArbitrageStrategy::new(config);
        let market = market();

        let mut books = HashMap::new();
        books.insert(
            "up-token".to_owned(),
            OrderBook {
                asset_id: "up-token".to_owned(),
                bids: vec![BookLevel {
                    price: decimal("0.61"),
                    size: decimal("200"),
                }],
                asks: vec![
                    BookLevel {
                        price: decimal("0.65"),
                        size: decimal("200"),
                    },
                    BookLevel {
                        price: decimal("0.62"),
                        size: decimal("2"),
                    },
                ],
                min_order_size: None,
                tick_size: None,
            },
        );
        books.insert(
            "down-token".to_owned(),
            OrderBook {
                asset_id: "down-token".to_owned(),
                bids: vec![BookLevel {
                    price: decimal("0.28"),
                    size: decimal("200"),
                }],
                asks: vec![BookLevel {
                    price: decimal("0.30"),
                    size: decimal("200"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );

        let mut contexts = HashMap::new();
        contexts.insert(
            market.slug.clone(),
            BtcFiveMinuteContext {
                target: MarketTarget::Btc5m,
                interval_open_price: decimal("66500"),
                current_spot_price: decimal("66600"),
                current_spot_source: "test-fixture".to_owned(),
                current_spot_event_age_ms: None,
                current_spot_received_age_ms: None,
                current_spot_quote_points: None,
                exchange_book_age_ms: None,
                exchange_book_top_imbalance_bps: Decimal::ZERO,
                exchange_book_depth_imbalance_bps: Decimal::ZERO,
                exchange_book_microprice_bps: Decimal::ZERO,
                exchange_book_spread_bps: Decimal::ZERO,
                micro_burst_reference_price: decimal("66590"),
                micro_reference_price: decimal("66580"),
                target_price: decimal("66500"),
                target_price_source: TargetPriceSource::PolymarketEventMetadata,
                target_gap_bps: decimal("15"),
                spot_move_bps: decimal("3.2"),
                spot_move_1s_bps: decimal("0.7"),
                spot_move_5s_bps: decimal("2.6"),
                spot_move_15s_bps: decimal("2.7"),
                micro_acceleration_bps: decimal("0.9"),
                dominant_outcome: "Up".to_owned(),
                seconds_left: 220,
            },
        );

        let mut trade_flows = HashMap::new();
        trade_flows.insert(
            market.slug.clone(),
            TradeFlowSummary {
                up_pressure_notional: decimal("2000"),
                down_pressure_notional: decimal("600"),
                total_pressure_notional: decimal("2600"),
                signed_up_imbalance_bps: decimal("3.1"),
                trade_count: 18,
            },
        );

        let opportunities = strategy.find_opportunities(
            &[market],
            &books,
            &HashMap::new(),
            &contexts,
            &trade_flows,
        );
        assert!(opportunities.is_empty());
    }

    #[test]
    fn strategy_uses_strong_micro_ladder_before_full_size() {
        let mut config = strategy_config();
        config.enable_bundle = false;
        config.directional_min_velocity_bps_per_minute = 99;
        config.directional_min_signal_bps = 99;
        config.directional_confidence_bps_per_spot_bps = 420;
        config.directional_max_fair_price = decimal("0.95");
        config.micro_breakout_max_entry_price = decimal("0.90");
        config.micro_breakout_weak_notional_usdc = decimal("10");
        config.micro_breakout_normal_notional_usdc = decimal("20");
        config.micro_breakout_strong_notional_usdc = decimal("35");
        config.micro_breakout_expensive_entry_price = decimal("0.72");
        config.micro_breakout_full_size_max_entry_price = decimal("0.64");
        config.max_directional_notional_usdc = decimal("50");
        let strategy = BundleArbitrageStrategy::new(config);
        let market = market();

        let mut books = HashMap::new();
        books.insert(
            "up-token".to_owned(),
            OrderBook {
                asset_id: "up-token".to_owned(),
                bids: vec![BookLevel {
                    price: decimal("0.64"),
                    size: decimal("200"),
                }],
                asks: vec![BookLevel {
                    price: decimal("0.68"),
                    size: decimal("200"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );
        books.insert(
            "down-token".to_owned(),
            OrderBook {
                asset_id: "down-token".to_owned(),
                bids: vec![BookLevel {
                    price: decimal("0.28"),
                    size: decimal("200"),
                }],
                asks: vec![BookLevel {
                    price: decimal("0.30"),
                    size: decimal("200"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );

        let mut contexts = HashMap::new();
        contexts.insert(
            market.slug.clone(),
            BtcFiveMinuteContext {
                target: MarketTarget::Btc5m,
                interval_open_price: decimal("66500"),
                current_spot_price: decimal("66600"),
                current_spot_source: "test-fixture".to_owned(),
                current_spot_event_age_ms: None,
                current_spot_received_age_ms: None,
                current_spot_quote_points: None,
                exchange_book_age_ms: None,
                exchange_book_top_imbalance_bps: Decimal::ZERO,
                exchange_book_depth_imbalance_bps: Decimal::ZERO,
                exchange_book_microprice_bps: Decimal::ZERO,
                exchange_book_spread_bps: Decimal::ZERO,
                micro_burst_reference_price: decimal("66590"),
                micro_reference_price: decimal("66580"),
                target_price: decimal("66500"),
                target_price_source: TargetPriceSource::PolymarketEventMetadata,
                target_gap_bps: decimal("15"),
                spot_move_bps: decimal("3.2"),
                spot_move_1s_bps: decimal("0.7"),
                spot_move_5s_bps: decimal("2.6"),
                spot_move_15s_bps: decimal("2.7"),
                micro_acceleration_bps: decimal("0.9"),
                dominant_outcome: "Up".to_owned(),
                seconds_left: 220,
            },
        );

        let mut trade_flows = HashMap::new();
        trade_flows.insert(
            market.slug.clone(),
            TradeFlowSummary {
                up_pressure_notional: decimal("2000"),
                down_pressure_notional: decimal("600"),
                total_pressure_notional: decimal("2600"),
                signed_up_imbalance_bps: decimal("3.1"),
                trade_count: 18,
            },
        );

        let opportunities = strategy.find_opportunities(
            &[market],
            &books,
            &HashMap::new(),
            &contexts,
            &trade_flows,
        );
        assert_eq!(opportunities.len(), 1);
        assert_eq!(opportunities[0].kind, OpportunityKind::MicroBreakout);
        assert_eq!(opportunities[0].required_usdc.round_dp(2), decimal("35.00"));
    }

    #[test]
    fn strategy_reports_micro_breakout_near_miss_when_directional_is_disabled() {
        let mut config = strategy_config();
        config.enable_bundle = false;
        config.enable_directional = false;
        config.enable_tail_hedge = false;
        config.enable_micro_breakout = true;
        config.micro_breakout_min_spot_move_bps = 1;
        config.micro_breakout_min_spot_move_1s_bps = decimal("0.10");
        config.micro_breakout_min_spot_move_5s_bps = decimal("1.20");
        config.micro_breakout_min_signal_bps = 2;
        config.micro_breakout_min_elapsed_window_secs = 4;
        config.micro_breakout_max_entry_price = decimal("0.84");
        config.micro_breakout_max_average_price_drift = decimal("0.020");
        config.micro_breakout_weak_notional_usdc = decimal("10");
        config.micro_breakout_normal_notional_usdc = decimal("20");
        config.micro_breakout_strong_notional_usdc = decimal("35");
        config.micro_breakout_expensive_entry_price = decimal("0.72");
        config.micro_breakout_full_size_max_entry_price = decimal("0.64");
        let strategy = BundleArbitrageStrategy::new(config);
        let market = market();

        let mut books = HashMap::new();
        books.insert(
            "up-token".to_owned(),
            OrderBook {
                asset_id: "up-token".to_owned(),
                bids: vec![BookLevel {
                    price: decimal("0.64"),
                    size: decimal("200"),
                }],
                asks: vec![BookLevel {
                    price: decimal("0.68"),
                    size: decimal("200"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );
        books.insert(
            "down-token".to_owned(),
            OrderBook {
                asset_id: "down-token".to_owned(),
                bids: vec![BookLevel {
                    price: decimal("0.28"),
                    size: decimal("200"),
                }],
                asks: vec![BookLevel {
                    price: decimal("0.30"),
                    size: decimal("200"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );

        let mut contexts = HashMap::new();
        contexts.insert(
            market.slug.clone(),
            BtcFiveMinuteContext {
                target: MarketTarget::Btc5m,
                interval_open_price: decimal("66500"),
                current_spot_price: decimal("66600"),
                current_spot_source: "test-fixture".to_owned(),
                current_spot_event_age_ms: None,
                current_spot_received_age_ms: None,
                current_spot_quote_points: None,
                exchange_book_age_ms: None,
                exchange_book_top_imbalance_bps: Decimal::ZERO,
                exchange_book_depth_imbalance_bps: Decimal::ZERO,
                exchange_book_microprice_bps: Decimal::ZERO,
                exchange_book_spread_bps: Decimal::ZERO,
                micro_burst_reference_price: decimal("66590"),
                micro_reference_price: decimal("66580"),
                target_price: decimal("66500"),
                target_price_source: TargetPriceSource::PolymarketEventMetadata,
                target_gap_bps: decimal("15"),
                spot_move_bps: decimal("3.2"),
                spot_move_1s_bps: decimal("0.2"),
                spot_move_5s_bps: decimal("0.5"),
                spot_move_15s_bps: decimal("2.7"),
                micro_acceleration_bps: decimal("0.9"),
                dominant_outcome: "Up".to_owned(),
                seconds_left: 220,
            },
        );

        let mut trade_flows = HashMap::new();
        trade_flows.insert(
            market.slug.clone(),
            TradeFlowSummary {
                up_pressure_notional: decimal("2000"),
                down_pressure_notional: decimal("600"),
                total_pressure_notional: decimal("2600"),
                signed_up_imbalance_bps: decimal("3.1"),
                trade_count: 18,
            },
        );

        let misses = strategy.find_near_misses(
            &[market],
            &books,
            &HashMap::new(),
            &contexts,
            &trade_flows,
            3,
        );
        assert_eq!(misses.len(), 1);
        assert_eq!(misses[0].kind, OpportunityKind::MicroBreakout);
        assert!(misses[0].reason.contains("5s"));
    }

    #[test]
    fn strategy_blocks_plain_directional_soft_entry_without_hedge() {
        let mut config = strategy_config();
        config.enable_bundle = false;
        config.enable_tail_hedge = false;
        config.directional_require_hedge_for_soft_entry = true;
        config.directional_min_spot_move_bps = 10;
        config.directional_min_velocity_bps_per_minute = 0;
        config.directional_min_signal_bps = 10;
        config.directional_strong_signal_min_spot_move_5s_bps = decimal("3.0");
        config.directional_strong_signal_min_trade_flow_bps = decimal("3.0");
        config.directional_max_entry_price = decimal("0.60");
        config.enable_micro_breakout = false;
        let strategy = BundleArbitrageStrategy::new(config);
        let market = market();

        let mut books = HashMap::new();
        books.insert(
            "up-token".to_owned(),
            OrderBook {
                asset_id: "up-token".to_owned(),
                bids: vec![BookLevel {
                    price: decimal("0.52"),
                    size: decimal("150"),
                }],
                asks: vec![BookLevel {
                    price: decimal("0.54"),
                    size: decimal("150"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );
        books.insert(
            "down-token".to_owned(),
            OrderBook {
                asset_id: "down-token".to_owned(),
                bids: vec![BookLevel {
                    price: decimal("0.43"),
                    size: decimal("150"),
                }],
                asks: vec![BookLevel {
                    price: decimal("0.45"),
                    size: decimal("150"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );

        let mut contexts = HashMap::new();
        contexts.insert(
            market.slug.clone(),
            BtcFiveMinuteContext {
                target: MarketTarget::Btc5m,
                interval_open_price: decimal("66500"),
                current_spot_price: decimal("66620"),
                current_spot_source: "test-fixture".to_owned(),
                current_spot_event_age_ms: None,
                current_spot_received_age_ms: None,
                current_spot_quote_points: None,
                exchange_book_age_ms: None,
                exchange_book_top_imbalance_bps: Decimal::ZERO,
                exchange_book_depth_imbalance_bps: Decimal::ZERO,
                exchange_book_microprice_bps: Decimal::ZERO,
                exchange_book_spread_bps: Decimal::ZERO,
                micro_burst_reference_price: decimal("66612"),
                micro_reference_price: decimal("66600"),
                target_price: decimal("66500"),
                target_price_source: TargetPriceSource::PolymarketEventMetadata,
                target_gap_bps: decimal("18"),
                spot_move_bps: decimal("18"),
                spot_move_1s_bps: decimal("0.6"),
                spot_move_5s_bps: decimal("1.6"),
                spot_move_15s_bps: decimal("1.9"),
                micro_acceleration_bps: decimal("0.4"),
                dominant_outcome: "Up".to_owned(),
                seconds_left: 210,
            },
        );

        let mut trade_flows = HashMap::new();
        trade_flows.insert(
            market.slug.clone(),
            TradeFlowSummary {
                up_pressure_notional: decimal("1600"),
                down_pressure_notional: decimal("900"),
                total_pressure_notional: decimal("2500"),
                signed_up_imbalance_bps: decimal("1.8"),
                trade_count: 14,
            },
        );

        let opportunities = strategy.find_opportunities(
            std::slice::from_ref(&market),
            &books,
            &HashMap::new(),
            &contexts,
            &trade_flows,
        );
        assert!(opportunities.is_empty());

        let misses = strategy.find_near_misses(
            &[market],
            &books,
            &HashMap::new(),
            &contexts,
            &trade_flows,
            3,
        );
        assert_eq!(misses.len(), 1);
        assert_eq!(misses[0].kind, OpportunityKind::DirectionalMomentum);
        assert!(misses[0].reason.contains("hedge"));
    }

    #[test]
    fn strategy_reports_bundle_near_miss_when_edge_is_below_threshold() {
        let mut config = strategy_config();
        config.enable_directional = false;
        config.min_edge_bps = 80;
        let strategy = BundleArbitrageStrategy::new(config);
        let market = market();

        let mut books = HashMap::new();
        books.insert(
            "up-token".to_owned(),
            OrderBook {
                asset_id: "up-token".to_owned(),
                bids: Vec::new(),
                asks: vec![BookLevel {
                    price: decimal("0.48"),
                    size: decimal("40"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );
        books.insert(
            "down-token".to_owned(),
            OrderBook {
                asset_id: "down-token".to_owned(),
                bids: Vec::new(),
                asks: vec![BookLevel {
                    price: decimal("0.515"),
                    size: decimal("40"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );

        let mut contexts = HashMap::new();
        contexts.insert(
            market.slug.clone(),
            BtcFiveMinuteContext {
                target: MarketTarget::Btc5m,
                interval_open_price: decimal("67000"),
                target_price: decimal("67000"),
                target_price_source: crate::models::TargetPriceSource::BinanceWindowOpenFallback,
                target_gap_bps: decimal("11.94"),
                current_spot_price: decimal("67080"),
                current_spot_source: "test-fixture".to_owned(),
                current_spot_event_age_ms: None,
                current_spot_received_age_ms: None,
                current_spot_quote_points: None,
                exchange_book_age_ms: None,
                exchange_book_top_imbalance_bps: Decimal::ZERO,
                exchange_book_depth_imbalance_bps: Decimal::ZERO,
                exchange_book_microprice_bps: Decimal::ZERO,
                exchange_book_spread_bps: Decimal::ZERO,
                micro_burst_reference_price: decimal("67080"),
                micro_reference_price: decimal("67080"),
                spot_move_bps: decimal("11.94"),
                spot_move_1s_bps: Decimal::ZERO,
                spot_move_5s_bps: Decimal::ZERO,
                spot_move_15s_bps: Decimal::ZERO,
                micro_acceleration_bps: Decimal::ZERO,
                dominant_outcome: "Up".to_owned(),
                seconds_left: 120,
            },
        );

        let misses = strategy.find_near_misses(
            &[market],
            &books,
            &HashMap::new(),
            &contexts,
            &HashMap::new(),
            3,
        );
        assert_eq!(misses.len(), 1);
        assert_eq!(misses[0].kind, OpportunityKind::BundleArbitrage);
        assert!(misses[0].reason.contains("edge"));
        assert!(misses[0].shortfall_bps > 0);
    }

    #[test]
    fn strategy_reports_directional_near_miss_when_ask_is_too_expensive() {
        let strategy = BundleArbitrageStrategy::new(strategy_config());
        let market = market();

        let mut books = HashMap::new();
        books.insert(
            "up-token".to_owned(),
            OrderBook {
                asset_id: "up-token".to_owned(),
                bids: Vec::new(),
                asks: vec![BookLevel {
                    price: decimal("0.64"),
                    size: decimal("120"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );
        books.insert(
            "down-token".to_owned(),
            OrderBook {
                asset_id: "down-token".to_owned(),
                bids: Vec::new(),
                asks: vec![BookLevel {
                    price: decimal("0.41"),
                    size: decimal("120"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );

        let mut contexts = HashMap::new();
        contexts.insert(
            market.slug.clone(),
            BtcFiveMinuteContext {
                target: MarketTarget::Btc5m,
                interval_open_price: decimal("67000"),
                target_price: decimal("67000"),
                target_price_source: crate::models::TargetPriceSource::BinanceWindowOpenFallback,
                target_gap_bps: decimal("20.90"),
                current_spot_price: decimal("67140"),
                current_spot_source: "test-fixture".to_owned(),
                current_spot_event_age_ms: None,
                current_spot_received_age_ms: None,
                current_spot_quote_points: None,
                exchange_book_age_ms: None,
                exchange_book_top_imbalance_bps: Decimal::ZERO,
                exchange_book_depth_imbalance_bps: Decimal::ZERO,
                exchange_book_microprice_bps: Decimal::ZERO,
                exchange_book_spread_bps: Decimal::ZERO,
                micro_burst_reference_price: decimal("67140"),
                micro_reference_price: decimal("67140"),
                spot_move_bps: decimal("20.90"),
                spot_move_1s_bps: Decimal::ZERO,
                spot_move_5s_bps: Decimal::ZERO,
                spot_move_15s_bps: Decimal::ZERO,
                micro_acceleration_bps: Decimal::ZERO,
                dominant_outcome: "Up".to_owned(),
                seconds_left: 160,
            },
        );

        let misses = strategy.find_near_misses(
            &[market],
            &books,
            &HashMap::new(),
            &contexts,
            &HashMap::new(),
            3,
        );
        assert_eq!(misses.len(), 1);
        assert_eq!(misses[0].kind, OpportunityKind::DirectionalMomentum);
        assert!(misses[0].reason.contains("ask") || misses[0].reason.contains("expensive"));
        assert!(misses[0].reason.is_ascii());
        assert!(misses[0].primary_outcome_ask_price.is_some());
    }

    #[test]
    fn strategy_enables_opening_tail_hedge_for_directional_signal() {
        let mut config = strategy_config();
        config.min_spot_move_bps = 40;
        let strategy = BundleArbitrageStrategy::new(config);
        let market = market();

        let mut books = HashMap::new();
        books.insert(
            "up-token".to_owned(),
            OrderBook {
                asset_id: "up-token".to_owned(),
                bids: Vec::new(),
                asks: vec![BookLevel {
                    price: decimal("0.56"),
                    size: decimal("100"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );
        books.insert(
            "down-token".to_owned(),
            OrderBook {
                asset_id: "down-token".to_owned(),
                bids: Vec::new(),
                asks: vec![BookLevel {
                    price: decimal("0.22"),
                    size: decimal("40"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );

        let mut contexts = HashMap::new();
        contexts.insert(
            market.slug.clone(),
            BtcFiveMinuteContext {
                target: MarketTarget::Btc5m,
                interval_open_price: decimal("67000"),
                target_price: decimal("67000"),
                target_price_source: crate::models::TargetPriceSource::BinanceWindowOpenFallback,
                target_gap_bps: decimal("31.34"),
                current_spot_price: decimal("67210"),
                current_spot_source: "test-fixture".to_owned(),
                current_spot_event_age_ms: None,
                current_spot_received_age_ms: None,
                current_spot_quote_points: None,
                exchange_book_age_ms: None,
                exchange_book_top_imbalance_bps: Decimal::ZERO,
                exchange_book_depth_imbalance_bps: Decimal::ZERO,
                exchange_book_microprice_bps: Decimal::ZERO,
                exchange_book_spread_bps: Decimal::ZERO,
                micro_burst_reference_price: decimal("67210"),
                micro_reference_price: decimal("67210"),
                spot_move_bps: decimal("31.34"),
                spot_move_1s_bps: Decimal::ZERO,
                spot_move_5s_bps: Decimal::ZERO,
                spot_move_15s_bps: Decimal::ZERO,
                micro_acceleration_bps: Decimal::ZERO,
                dominant_outcome: "Up".to_owned(),
                seconds_left: 295,
            },
        );

        let opportunities = strategy.find_opportunities(
            &[market],
            &books,
            &HashMap::new(),
            &contexts,
            &HashMap::new(),
        );
        assert_eq!(opportunities.len(), 1);
        assert_eq!(
            opportunities[0].kind,
            OpportunityKind::DirectionalMomentumHedged
        );
        assert_eq!(opportunities[0].primary_outcome_label, "Up");
        assert_eq!(
            opportunities[0].hedge_outcome_label.as_deref(),
            Some("Down")
        );
        assert!(opportunities[0].hedge_shares > Decimal::ZERO);
    }

    #[test]
    fn aligned_trade_flow_can_unlock_directional_signal() {
        let mut config = strategy_config();
        config.enable_bundle = false;
        config.directional_min_spot_move_bps = 18;
        config.directional_min_velocity_bps_per_minute = 18;
        config.directional_min_model_edge_bps = 15;
        config.directional_trade_flow_weight = decimal("0.50");
        config.directional_max_entry_price = decimal("0.60");
        config.directional_max_fair_price = decimal("0.68");
        let strategy = BundleArbitrageStrategy::new(config);
        let market = market();

        let mut books = HashMap::new();
        books.insert(
            "up-token".to_owned(),
            OrderBook {
                asset_id: "up-token".to_owned(),
                bids: Vec::new(),
                asks: vec![BookLevel {
                    price: decimal("0.595"),
                    size: decimal("150"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );
        books.insert(
            "down-token".to_owned(),
            OrderBook {
                asset_id: "down-token".to_owned(),
                bids: Vec::new(),
                asks: vec![BookLevel {
                    price: decimal("0.44"),
                    size: decimal("150"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );

        let mut contexts = HashMap::new();
        contexts.insert(
            market.slug.clone(),
            BtcFiveMinuteContext {
                target: MarketTarget::Btc5m,
                interval_open_price: decimal("67000"),
                target_price: decimal("67000"),
                target_price_source: crate::models::TargetPriceSource::BinanceWindowOpenFallback,
                target_gap_bps: decimal("18.06"),
                current_spot_price: decimal("67121"),
                current_spot_source: "test-fixture".to_owned(),
                current_spot_event_age_ms: None,
                current_spot_received_age_ms: None,
                current_spot_quote_points: None,
                exchange_book_age_ms: None,
                exchange_book_top_imbalance_bps: Decimal::ZERO,
                exchange_book_depth_imbalance_bps: Decimal::ZERO,
                exchange_book_microprice_bps: Decimal::ZERO,
                exchange_book_spread_bps: Decimal::ZERO,
                micro_burst_reference_price: decimal("67121"),
                micro_reference_price: decimal("67121"),
                spot_move_bps: decimal("18.06"),
                spot_move_1s_bps: Decimal::ZERO,
                spot_move_5s_bps: Decimal::ZERO,
                spot_move_15s_bps: Decimal::ZERO,
                micro_acceleration_bps: Decimal::ZERO,
                dominant_outcome: "Up".to_owned(),
                seconds_left: 240,
            },
        );

        let without_flow = strategy.find_opportunities(
            std::slice::from_ref(&market),
            &books,
            &HashMap::new(),
            &contexts,
            &HashMap::new(),
        );
        assert!(without_flow.is_empty());

        let mut trade_flows = HashMap::new();
        trade_flows.insert(
            market.slug.clone(),
            TradeFlowSummary {
                up_pressure_notional: decimal("1800"),
                down_pressure_notional: decimal("400"),
                total_pressure_notional: decimal("2200"),
                signed_up_imbalance_bps: decimal("6363.636364"),
                trade_count: 14,
            },
        );

        let with_flow = strategy.find_opportunities(
            std::slice::from_ref(&market),
            &books,
            &HashMap::new(),
            &contexts,
            &trade_flows,
        );
        assert_eq!(with_flow.len(), 1);
        assert_eq!(with_flow[0].kind, OpportunityKind::DirectionalMomentum);
        assert_eq!(with_flow[0].primary_outcome_label, "Up");
        assert!(with_flow[0].edge_bps >= 15);
    }

    #[test]
    fn target_state_v1_finds_persistent_target_side_edge() {
        let mut config = strategy_config();
        config.enable_bundle = false;
        config.enable_directional = false;
        config.enable_micro_breakout = false;
        config.enable_target_state_v1 = true;
        config.directional_min_model_edge_bps = 0;
        config.directional_max_fair_price = decimal("0.79");
        config.target_state_min_elapsed_window_secs = 120;
        config.target_state_max_seconds_left = 150;
        config.target_state_min_target_gap_bps = decimal("8");
        config.target_state_min_signal_bps = 12;
        config.target_state_min_spot_move_15s_bps = decimal("1.4");
        config.target_state_min_aligned_flow_bps = decimal("0.5");
        config.target_state_max_entry_price = decimal("0.70");
        config.target_state_normal_notional_usdc = decimal("12");
        config.target_state_strong_notional_usdc = decimal("20");
        config.target_state_strong_gap_bps = decimal("15");
        let strategy = BundleArbitrageStrategy::new(config);
        let market = market();

        let mut books = HashMap::new();
        books.insert(
            "up-token".to_owned(),
            OrderBook {
                asset_id: "up-token".to_owned(),
                bids: Vec::new(),
                asks: vec![BookLevel {
                    price: decimal("0.63"),
                    size: decimal("150"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );
        books.insert(
            "down-token".to_owned(),
            OrderBook {
                asset_id: "down-token".to_owned(),
                bids: Vec::new(),
                asks: vec![BookLevel {
                    price: decimal("0.38"),
                    size: decimal("150"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );

        let mut contexts = HashMap::new();
        contexts.insert(
            market.slug.clone(),
            BtcFiveMinuteContext {
                target: MarketTarget::Btc5m,
                interval_open_price: decimal("67000"),
                target_price: decimal("67000"),
                target_price_source: TargetPriceSource::PolymarketEventMetadata,
                target_gap_bps: decimal("16.42"),
                current_spot_price: decimal("67110"),
                current_spot_source: "test-fixture".to_owned(),
                current_spot_event_age_ms: None,
                current_spot_received_age_ms: None,
                current_spot_quote_points: None,
                exchange_book_age_ms: None,
                exchange_book_top_imbalance_bps: Decimal::ZERO,
                exchange_book_depth_imbalance_bps: Decimal::ZERO,
                exchange_book_microprice_bps: Decimal::ZERO,
                exchange_book_spread_bps: Decimal::ZERO,
                micro_burst_reference_price: decimal("67098"),
                micro_reference_price: decimal("67062"),
                spot_move_bps: decimal("16.42"),
                spot_move_1s_bps: decimal("0.8"),
                spot_move_5s_bps: decimal("1.6"),
                spot_move_15s_bps: decimal("2.1"),
                micro_acceleration_bps: decimal("0.7"),
                dominant_outcome: "Up".to_owned(),
                seconds_left: 120,
            },
        );

        let mut trade_flows = HashMap::new();
        trade_flows.insert(
            market.slug.clone(),
            TradeFlowSummary {
                up_pressure_notional: decimal("2200"),
                down_pressure_notional: decimal("500"),
                total_pressure_notional: decimal("2700"),
                signed_up_imbalance_bps: decimal("6296.296296"),
                trade_count: 18,
            },
        );

        let opportunities = strategy.find_opportunities(
            std::slice::from_ref(&market),
            &books,
            &HashMap::new(),
            &contexts,
            &trade_flows,
        );
        assert_eq!(opportunities.len(), 1);
        assert_eq!(opportunities[0].kind, OpportunityKind::TargetStateV1);
        assert_eq!(opportunities[0].primary_outcome_label, "Up");
        assert_eq!(opportunities[0].signal_tier, "strong");
        assert!(opportunities[0].expected_profit > Decimal::ZERO);
    }

    #[test]
    fn codex_sentinel_v1_reuses_calibrated_state_v2_signal_path() {
        let mut config = strategy_config();
        config.enable_bundle = false;
        config.enable_directional = false;
        config.enable_micro_breakout = false;
        config.enable_target_state_v1 = false;
        config.enable_bonereaper_state_v2 = false;
        config.enable_codex_sentinel_v1 = true;
        config.min_top_of_book_shares = decimal("5");
        config.bonereaper_state_v2_min_seconds_left = 210;
        config.bonereaper_state_v2_max_seconds_left = 270;
        config.bonereaper_state_v2_bias_min_target_gap_bps = decimal("1.2");
        config.bonereaper_state_v2_flip_max_target_gap_bps = decimal("1.2");
        config.bonereaper_state_v2_min_signal_bps = 2;
        config.bonereaper_state_v2_min_spot_move_15s_bps = Decimal::ZERO;
        config.bonereaper_state_v2_min_spot_move_5s_bps = Decimal::ZERO;
        config.bonereaper_state_v2_max_entry_price = decimal("0.76");
        config.bonereaper_state_v2_max_fair_price = decimal("0.86");
        config.bonereaper_state_v2_normal_notional_usdc = decimal("4");
        config.bonereaper_state_v2_min_expected_profit_usdc = decimal("0.30");
        config.directional_min_model_edge_bps = 0;
        let strategy = BundleArbitrageStrategy::new(config.clone());
        let market = market();

        let mut books = HashMap::new();
        books.insert(
            "up-token".to_owned(),
            OrderBook {
                asset_id: "up-token".to_owned(),
                bids: Vec::new(),
                asks: vec![BookLevel {
                    price: decimal("0.65"),
                    size: decimal("150"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );
        books.insert(
            "down-token".to_owned(),
            OrderBook {
                asset_id: "down-token".to_owned(),
                bids: Vec::new(),
                asks: vec![BookLevel {
                    price: decimal("0.36"),
                    size: decimal("150"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );

        let mut contexts = HashMap::new();
        contexts.insert(
            market.slug.clone(),
            BtcFiveMinuteContext {
                target: MarketTarget::Btc5m,
                interval_open_price: decimal("67000"),
                target_price: decimal("67000"),
                target_price_source: TargetPriceSource::PolymarketEventMetadata,
                target_gap_bps: decimal("-2.2447"),
                current_spot_price: decimal("66984.96"),
                current_spot_source: "test-fixture".to_owned(),
                current_spot_event_age_ms: None,
                current_spot_received_age_ms: None,
                current_spot_quote_points: None,
                exchange_book_age_ms: None,
                exchange_book_top_imbalance_bps: Decimal::ZERO,
                exchange_book_depth_imbalance_bps: Decimal::ZERO,
                exchange_book_microprice_bps: Decimal::ZERO,
                exchange_book_spread_bps: Decimal::ZERO,
                micro_burst_reference_price: decimal("66984.96"),
                micro_reference_price: decimal("66984.96"),
                spot_move_bps: decimal("-2.2447"),
                spot_move_1s_bps: Decimal::ZERO,
                spot_move_5s_bps: Decimal::ZERO,
                spot_move_15s_bps: decimal("-1.00"),
                micro_acceleration_bps: Decimal::ZERO,
                dominant_outcome: "Down".to_owned(),
                seconds_left: 235,
            },
        );

        let mut trade_flows = HashMap::new();
        trade_flows.insert(
            market.slug.clone(),
            TradeFlowSummary {
                signed_up_imbalance_bps: decimal("-2200"),
                trade_count: 8,
                ..TradeFlowSummary::default()
            },
        );

        let opportunities = strategy.find_opportunities(
            std::slice::from_ref(&market),
            &books,
            &HashMap::new(),
            &contexts,
            &trade_flows,
        );
        assert_eq!(opportunities.len(), 1);
        assert_eq!(opportunities[0].kind, OpportunityKind::CodexSentinelV1);
        assert_eq!(opportunities[0].primary_outcome_label, "Down");
        assert!(opportunities[0].note.contains("codex-sentinel-v1"));
        assert!(opportunities[0].expected_profit >= decimal("0.30"));

        let mut required_config = config;
        required_config.codex_breakout_v1_enabled = true;
        required_config.codex_breakout_v1_required = true;
        let required_strategy = BundleArbitrageStrategy::new(required_config);
        let required_opportunities = required_strategy.find_opportunities(
            std::slice::from_ref(&market),
            &books,
            &HashMap::new(),
            &contexts,
            &trade_flows,
        );
        assert!(required_opportunities.is_empty());

        let misses = required_strategy.find_near_misses(
            std::slice::from_ref(&market),
            &books,
            &HashMap::new(),
            &contexts,
            &trade_flows,
            1,
        );
        assert_eq!(misses.len(), 1);
        assert_eq!(misses[0].kind, OpportunityKind::CodexSentinelV1);
        assert!(
            misses[0]
                .reason
                .contains("orderbook breakout or discount-value confirmation"),
            "near miss should explain strict depth gate, got: {}",
            misses[0].reason
        );
    }

    #[test]
    fn codex_sentinel_v1_target_gate_is_btc_only() {
        assert!(codex_sentinel_v1_target_allowed(MarketTarget::Btc5m));
        for target in [
            MarketTarget::Eth5m,
            MarketTarget::Sol5m,
            MarketTarget::Xrp5m,
            MarketTarget::Bnb5m,
        ] {
            assert!(!codex_sentinel_v1_target_allowed(target));
        }
    }

    #[test]
    fn codex_sentinel_v1_discount_value_lane_allows_only_cheap_confirmed_value() {
        let mut config = strategy_config();
        config.codex_sentinel_v1_discount_value_lane_enabled = true;
        config.codex_sentinel_v1_max_live_quote_age_ms = 750;
        let mut decision = BonereaperStateV2Decision {
            aligned_flow_bps: Decimal::from(900),
            signal_strength_bps: Decimal::from(800),
            ..test_codex_decision("1.40", "1.20")
        };
        let mut context = test_codex_context("1.40", "1.35");
        context.current_spot_source = "Binance::Trade".to_owned();
        context.current_spot_received_age_ms = Some(120);
        context.exchange_book_age_ms = Some(100);
        context.exchange_book_spread_bps = decimal("2.00");
        context.exchange_book_top_imbalance_bps = Decimal::from(600);
        context.exchange_book_depth_imbalance_bps = Decimal::from(800);
        context.exchange_book_microprice_bps = decimal("0.0004");

        assert!(codex_sentinel_v1_discount_value_lane_allows(
            &context,
            &decision,
            decimal("0.49"),
            &config,
        ));
        assert!(!codex_sentinel_v1_discount_value_lane_allows(
            &context,
            &decision,
            decimal("0.51"),
            &config,
        ));

        decision.counter_bias = true;
        assert!(!codex_sentinel_v1_discount_value_lane_allows(
            &context,
            &decision,
            decimal("0.49"),
            &config,
        ));
    }

    #[test]
    fn codex_sentinel_v1_discount_value_lane_rejects_counter_burst_or_weak_microprice() {
        let mut config = strategy_config();
        config.codex_sentinel_v1_discount_value_lane_enabled = true;
        let decision = BonereaperStateV2Decision {
            aligned_flow_bps: Decimal::from(3_137),
            signal_strength_bps: Decimal::from(1_898),
            up_side: false,
            ..test_codex_decision("7.79", "6.22")
        };
        let mut context = test_codex_context("-5.10", "0.22");
        context.current_spot_source = "Coinbase::Ticker".to_owned();
        context.current_spot_received_age_ms = Some(22);
        context.exchange_book_age_ms = Some(50);
        context.exchange_book_spread_bps = decimal("0.0013");
        context.exchange_book_top_imbalance_bps = decimal("-1887.83");
        context.exchange_book_depth_imbalance_bps = decimal("-1878.22");
        context.exchange_book_microprice_bps = decimal("-0.0001");

        assert!(!codex_sentinel_v1_discount_value_lane_allows(
            &context,
            &decision,
            decimal("0.41"),
            &config,
        ));

        context.spot_move_1s_bps = decimal("-0.22");
        assert!(!codex_sentinel_v1_discount_value_lane_allows(
            &context,
            &decision,
            decimal("0.41"),
            &config,
        ));

        context.exchange_book_microprice_bps = decimal("-0.0004");
        assert!(codex_sentinel_v1_discount_value_lane_allows(
            &context,
            &decision,
            decimal("0.41"),
            &config,
        ));
    }

    #[test]
    fn codex_sentinel_v1_discount_value_lane_bypasses_strict_breakout_for_cheap_value() {
        let mut config = strategy_config();
        config.enable_bundle = false;
        config.enable_directional = false;
        config.enable_micro_breakout = false;
        config.enable_target_state_v1 = false;
        config.enable_bonereaper_state_v2 = false;
        config.enable_codex_sentinel_v1 = true;
        config.min_top_of_book_shares = decimal("5");
        config.bonereaper_state_v2_min_seconds_left = 210;
        config.bonereaper_state_v2_max_seconds_left = 270;
        config.bonereaper_state_v2_bias_min_target_gap_bps = decimal("1.2");
        config.bonereaper_state_v2_flip_max_target_gap_bps = decimal("1.2");
        config.bonereaper_state_v2_min_signal_bps = 2;
        config.bonereaper_state_v2_min_spot_move_15s_bps = Decimal::ZERO;
        config.bonereaper_state_v2_min_spot_move_5s_bps = Decimal::ZERO;
        config.bonereaper_state_v2_min_aligned_flow_bps = Decimal::ZERO;
        config.bonereaper_state_v2_max_entry_price = decimal("0.76");
        config.bonereaper_state_v2_max_fair_price = decimal("0.86");
        config.bonereaper_state_v2_normal_notional_usdc = decimal("4");
        config.bonereaper_state_v2_min_expected_profit_usdc = Decimal::ZERO;
        config.codex_sentinel_v1_max_entry_price = decimal("0.67");
        config.codex_sentinel_v1_live_quote_age_guard_enabled = true;
        config.codex_sentinel_v1_max_live_quote_age_ms = 750;
        config.codex_sentinel_v1_entry_spread_guard_enabled = true;
        config.codex_sentinel_v1_max_entry_spread = decimal("0.05");
        config.codex_breakout_v1_enabled = true;
        config.codex_breakout_v1_required = true;
        config.codex_breakout_v1_min_depth_imbalance_bps = Decimal::from(1800);
        config.codex_breakout_v1_min_score_bps = Decimal::from(3000);
        config.codex_sentinel_v1_discount_value_lane_enabled = true;
        config.codex_sentinel_v1_discount_value_max_entry_price = decimal("0.50");
        config.directional_min_model_edge_bps = 0;
        config.directional_confidence_bps_per_spot_bps = 420;
        let strategy = BundleArbitrageStrategy::new(config);
        let market = market();

        let mut books = HashMap::new();
        books.insert(
            "up-token".to_owned(),
            OrderBook {
                asset_id: "up-token".to_owned(),
                bids: vec![BookLevel {
                    price: decimal("0.47"),
                    size: decimal("150"),
                }],
                asks: vec![BookLevel {
                    price: decimal("0.49"),
                    size: decimal("150"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );
        books.insert(
            "down-token".to_owned(),
            OrderBook {
                asset_id: "down-token".to_owned(),
                bids: vec![BookLevel {
                    price: decimal("0.50"),
                    size: decimal("150"),
                }],
                asks: vec![BookLevel {
                    price: decimal("0.52"),
                    size: decimal("150"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );

        let mut contexts = HashMap::new();
        contexts.insert(
            market.slug.clone(),
            BtcFiveMinuteContext {
                target: MarketTarget::Btc5m,
                interval_open_price: decimal("67000"),
                target_price: decimal("67000"),
                target_price_source: TargetPriceSource::PolymarketEventMetadata,
                target_gap_bps: decimal("1.40"),
                current_spot_price: decimal("67009.38"),
                current_spot_source: "Binance::Trade".to_owned(),
                current_spot_event_age_ms: Some(110),
                current_spot_received_age_ms: Some(110),
                current_spot_quote_points: Some(20),
                exchange_book_age_ms: Some(120),
                exchange_book_top_imbalance_bps: Decimal::from(600),
                exchange_book_depth_imbalance_bps: Decimal::from(800),
                exchange_book_microprice_bps: decimal("0.0004"),
                exchange_book_spread_bps: decimal("2.00"),
                micro_burst_reference_price: decimal("67000.67"),
                micro_reference_price: decimal("67000.67"),
                spot_move_bps: decimal("1.40"),
                spot_move_1s_bps: decimal("1.30"),
                spot_move_5s_bps: decimal("1.30"),
                spot_move_15s_bps: decimal("1.20"),
                micro_acceleration_bps: decimal("0.10"),
                dominant_outcome: "Up".to_owned(),
                seconds_left: 235,
            },
        );

        let mut trade_flows = HashMap::new();
        trade_flows.insert(
            market.slug.clone(),
            TradeFlowSummary {
                signed_up_imbalance_bps: Decimal::from(1200),
                trade_count: 12,
                ..TradeFlowSummary::default()
            },
        );

        let opportunities = strategy.find_opportunities(
            std::slice::from_ref(&market),
            &books,
            &HashMap::new(),
            &contexts,
            &trade_flows,
        );

        assert_eq!(opportunities.len(), 1);
        assert_eq!(opportunities[0].kind, OpportunityKind::CodexSentinelV1);
        assert_eq!(opportunities[0].primary_outcome_label, "Up");
        assert!(
            opportunities[0].note.contains("discount_value_lane"),
            "opportunity note should explain the strict-breakout bypass, got: {}",
            opportunities[0].note
        );
    }

    #[test]
    fn codex_sentinel_v1_rejects_early_probe_against_flow_floor() {
        let mut config = strategy_config();
        config.enable_bundle = false;
        config.enable_directional = false;
        config.enable_micro_breakout = false;
        config.enable_target_state_v1 = false;
        config.enable_bonereaper_state_v2 = false;
        config.enable_codex_sentinel_v1 = true;
        config.min_top_of_book_shares = decimal("5");
        config.bonereaper_state_v2_min_seconds_left = 210;
        config.bonereaper_state_v2_max_seconds_left = 270;
        config.bonereaper_state_v2_bias_min_target_gap_bps = decimal("1.2");
        config.bonereaper_state_v2_flip_max_target_gap_bps = decimal("1.2");
        config.bonereaper_state_v2_min_signal_bps = 2;
        config.bonereaper_state_v2_min_spot_move_15s_bps = Decimal::ZERO;
        config.bonereaper_state_v2_min_spot_move_5s_bps = Decimal::ZERO;
        config.bonereaper_state_v2_min_aligned_flow_bps = Decimal::ZERO;
        config.bonereaper_state_v2_max_entry_price = decimal("0.76");
        config.bonereaper_state_v2_max_fair_price = decimal("0.86");
        config.bonereaper_state_v2_normal_notional_usdc = decimal("4");
        config.bonereaper_state_v2_min_expected_profit_usdc = Decimal::ZERO;
        config.directional_min_model_edge_bps = 0;
        config.directional_confidence_bps_per_spot_bps = 420;
        let market = market();

        let mut books = HashMap::new();
        books.insert(
            "up-token".to_owned(),
            OrderBook {
                asset_id: "up-token".to_owned(),
                bids: Vec::new(),
                asks: vec![BookLevel {
                    price: decimal("0.53"),
                    size: decimal("150"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );
        books.insert(
            "down-token".to_owned(),
            OrderBook {
                asset_id: "down-token".to_owned(),
                bids: Vec::new(),
                asks: vec![BookLevel {
                    price: decimal("0.48"),
                    size: decimal("150"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );

        let mut contexts = HashMap::new();
        contexts.insert(
            market.slug.clone(),
            BtcFiveMinuteContext {
                target: MarketTarget::Btc5m,
                interval_open_price: decimal("67000"),
                target_price: decimal("67000"),
                target_price_source: TargetPriceSource::PolymarketEventMetadata,
                target_gap_bps: decimal("1.28"),
                current_spot_price: decimal("67008.58"),
                current_spot_source: "test-fixture".to_owned(),
                current_spot_event_age_ms: None,
                current_spot_received_age_ms: None,
                current_spot_quote_points: None,
                exchange_book_age_ms: None,
                exchange_book_top_imbalance_bps: Decimal::ZERO,
                exchange_book_depth_imbalance_bps: Decimal::ZERO,
                exchange_book_microprice_bps: Decimal::ZERO,
                exchange_book_spread_bps: Decimal::ZERO,
                micro_burst_reference_price: decimal("67008.58"),
                micro_reference_price: decimal("67008.58"),
                spot_move_bps: decimal("1.28"),
                spot_move_1s_bps: Decimal::ZERO,
                spot_move_5s_bps: Decimal::ZERO,
                spot_move_15s_bps: decimal("0.80"),
                micro_acceleration_bps: Decimal::ZERO,
                dominant_outcome: "Up".to_owned(),
                seconds_left: 235,
            },
        );

        let mut trade_flows = HashMap::new();
        trade_flows.insert(
            market.slug.clone(),
            TradeFlowSummary {
                signed_up_imbalance_bps: decimal("-0.20"),
                trade_count: 4,
                ..TradeFlowSummary::default()
            },
        );

        let sentinel_strategy = BundleArbitrageStrategy::new(config.clone());
        let sentinel_opportunities = sentinel_strategy.find_opportunities(
            std::slice::from_ref(&market),
            &books,
            &HashMap::new(),
            &contexts,
            &trade_flows,
        );
        assert!(sentinel_opportunities.is_empty());

        let mut legacy_v2_config = config;
        legacy_v2_config.enable_bonereaper_state_v2 = true;
        legacy_v2_config.enable_codex_sentinel_v1 = false;
        let legacy_v2_strategy = BundleArbitrageStrategy::new(legacy_v2_config);
        let legacy_v2_opportunities = legacy_v2_strategy.find_opportunities(
            std::slice::from_ref(&market),
            &books,
            &HashMap::new(),
            &contexts,
            &trade_flows,
        );
        assert_eq!(legacy_v2_opportunities.len(), 1);
        assert_eq!(
            legacy_v2_opportunities[0].kind,
            OpportunityKind::BonereaperStateV2
        );
    }

    #[test]
    fn codex_sentinel_v1_blocks_stale_micro_without_discount_or_strong_flow() {
        let mut config = strategy_config();
        config.enable_bundle = false;
        config.enable_directional = false;
        config.enable_micro_breakout = false;
        config.enable_target_state_v1 = false;
        config.enable_bonereaper_state_v2 = false;
        config.enable_codex_sentinel_v1 = true;
        config.min_top_of_book_shares = decimal("5");
        config.bonereaper_state_v2_min_seconds_left = 210;
        config.bonereaper_state_v2_max_seconds_left = 270;
        config.bonereaper_state_v2_bias_min_target_gap_bps = decimal("1.2");
        config.bonereaper_state_v2_flip_max_target_gap_bps = decimal("1.2");
        config.bonereaper_state_v2_min_signal_bps = 2;
        config.bonereaper_state_v2_min_spot_move_15s_bps = Decimal::ZERO;
        config.bonereaper_state_v2_min_spot_move_5s_bps = Decimal::ZERO;
        config.bonereaper_state_v2_min_aligned_flow_bps = Decimal::ZERO;
        config.bonereaper_state_v2_max_entry_price = decimal("0.76");
        config.bonereaper_state_v2_max_fair_price = decimal("0.86");
        config.bonereaper_state_v2_normal_notional_usdc = decimal("4");
        config.bonereaper_state_v2_min_expected_profit_usdc = decimal("0.75");
        config.directional_min_model_edge_bps = 0;
        config.directional_confidence_bps_per_spot_bps = 420;
        let strategy = BundleArbitrageStrategy::new(config);
        let market = market();

        let mut books = HashMap::new();
        books.insert(
            "up-token".to_owned(),
            OrderBook {
                asset_id: "up-token".to_owned(),
                bids: Vec::new(),
                asks: vec![BookLevel {
                    price: decimal("0.39"),
                    size: decimal("150"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );
        books.insert(
            "down-token".to_owned(),
            OrderBook {
                asset_id: "down-token".to_owned(),
                bids: Vec::new(),
                asks: vec![BookLevel {
                    price: decimal("0.42"),
                    size: decimal("150"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );

        let mut contexts = HashMap::new();
        contexts.insert(
            market.slug.clone(),
            BtcFiveMinuteContext {
                target: MarketTarget::Btc5m,
                interval_open_price: decimal("76850.18"),
                target_price: decimal("76850.18"),
                target_price_source: TargetPriceSource::PolymarketEventMetadata,
                target_gap_bps: decimal("2.23"),
                current_spot_price: decimal("76867.32"),
                current_spot_source: "test-fixture".to_owned(),
                current_spot_event_age_ms: None,
                current_spot_received_age_ms: None,
                current_spot_quote_points: None,
                exchange_book_age_ms: None,
                exchange_book_top_imbalance_bps: Decimal::ZERO,
                exchange_book_depth_imbalance_bps: Decimal::ZERO,
                exchange_book_microprice_bps: Decimal::ZERO,
                exchange_book_spread_bps: Decimal::ZERO,
                micro_burst_reference_price: decimal("76867.32"),
                micro_reference_price: decimal("76867.32"),
                spot_move_bps: decimal("2.23"),
                spot_move_1s_bps: Decimal::ZERO,
                spot_move_5s_bps: Decimal::ZERO,
                spot_move_15s_bps: decimal("1.13"),
                micro_acceleration_bps: Decimal::ZERO,
                dominant_outcome: "Up".to_owned(),
                seconds_left: 267,
            },
        );

        let mut trade_flows = HashMap::new();
        trade_flows.insert(
            market.slug.clone(),
            TradeFlowSummary {
                signed_up_imbalance_bps: decimal("723.84"),
                trade_count: 6,
                ..TradeFlowSummary::default()
            },
        );

        let opportunities = strategy.find_opportunities(
            std::slice::from_ref(&market),
            &books,
            &HashMap::new(),
            &contexts,
            &trade_flows,
        );
        assert!(opportunities.is_empty());

        let misses = strategy.find_near_misses(
            std::slice::from_ref(&market),
            &books,
            &HashMap::new(),
            &contexts,
            &trade_flows,
            1,
        );
        assert_eq!(misses.len(), 1);
        assert_eq!(misses[0].kind, OpportunityKind::CodexSentinelV1);
        assert!(misses[0].reason.contains("stale 1s/5s signal"));
    }

    #[test]
    fn codex_sentinel_v1_blocks_micro_dust_counter_flip_without_discount_or_strong_flow() {
        let mut config = strategy_config();
        config.enable_bundle = false;
        config.enable_directional = false;
        config.enable_micro_breakout = false;
        config.enable_target_state_v1 = false;
        config.enable_bonereaper_state_v2 = false;
        config.enable_codex_sentinel_v1 = true;
        config.min_top_of_book_shares = decimal("5");
        config.bonereaper_state_v2_min_seconds_left = 210;
        config.bonereaper_state_v2_max_seconds_left = 270;
        config.bonereaper_state_v2_bias_min_target_gap_bps = decimal("1.1");
        config.bonereaper_state_v2_flip_max_target_gap_bps = decimal("1.1");
        config.bonereaper_state_v2_min_signal_bps = 2;
        config.bonereaper_state_v2_min_spot_move_15s_bps = Decimal::ZERO;
        config.bonereaper_state_v2_min_spot_move_5s_bps = Decimal::ZERO;
        config.bonereaper_state_v2_min_aligned_flow_bps = Decimal::ZERO;
        config.bonereaper_state_v2_max_entry_price = decimal("0.70");
        config.bonereaper_state_v2_max_fair_price = decimal("0.89");
        config.bonereaper_state_v2_probe_notional_usdc = decimal("4");
        config.bonereaper_state_v2_min_expected_profit_usdc = decimal("0.75");
        config.codex_sentinel_v1_max_entry_price = decimal("0.70");
        config.codex_sentinel_v1_stale_micro_max_confirmation_bps = decimal("0.05");
        config.codex_sentinel_v1_stale_micro_discount_max_entry_price = decimal("0.60");
        config.codex_sentinel_v1_stale_micro_discount_min_signal_bps = Decimal::from(450);
        config.codex_sentinel_v1_stale_micro_discount_min_flow_bps = Decimal::from(700);
        config.codex_sentinel_v1_stale_micro_min_signal_bps = Decimal::from(650);
        config.codex_sentinel_v1_stale_micro_min_flow_bps = Decimal::from(1000);
        config.directional_min_model_edge_bps = 0;
        config.directional_confidence_bps_per_spot_bps = 420;

        let strategy = BundleArbitrageStrategy::new(config);
        let market = market();
        let mut books = HashMap::new();
        books.insert(
            "up-token".to_owned(),
            OrderBook {
                asset_id: "up-token".to_owned(),
                bids: Vec::new(),
                asks: vec![BookLevel {
                    price: decimal("0.50"),
                    size: decimal("150"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );
        books.insert(
            "down-token".to_owned(),
            OrderBook {
                asset_id: "down-token".to_owned(),
                bids: Vec::new(),
                asks: vec![BookLevel {
                    price: decimal("0.51"),
                    size: decimal("150"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );

        let mut contexts = HashMap::new();
        contexts.insert(
            market.slug.clone(),
            BtcFiveMinuteContext {
                target: MarketTarget::Btc5m,
                interval_open_price: decimal("76622.47"),
                target_price: decimal("76622.47"),
                target_price_source: TargetPriceSource::BinanceWindowOpenFallback,
                target_gap_bps: decimal("-0.3067"),
                current_spot_price: decimal("76620.12"),
                current_spot_source: "test-fixture".to_owned(),
                current_spot_event_age_ms: None,
                current_spot_received_age_ms: None,
                current_spot_quote_points: None,
                exchange_book_age_ms: None,
                exchange_book_top_imbalance_bps: Decimal::ZERO,
                exchange_book_depth_imbalance_bps: Decimal::ZERO,
                exchange_book_microprice_bps: Decimal::ZERO,
                exchange_book_spread_bps: Decimal::ZERO,
                micro_burst_reference_price: decimal("76620.11"),
                micro_reference_price: decimal("76620.12"),
                spot_move_bps: decimal("-0.3067"),
                spot_move_1s_bps: decimal("0.0013"),
                spot_move_5s_bps: Decimal::ZERO,
                spot_move_15s_bps: decimal("0.0013"),
                micro_acceleration_bps: decimal("-0.0004"),
                dominant_outcome: "Down".to_owned(),
                seconds_left: 247,
            },
        );

        let mut trade_flows = HashMap::new();
        trade_flows.insert(
            market.slug.clone(),
            TradeFlowSummary {
                signed_up_imbalance_bps: decimal("637.3659"),
                trade_count: 6,
                ..TradeFlowSummary::default()
            },
        );

        let opportunities = strategy.find_opportunities(
            std::slice::from_ref(&market),
            &books,
            &HashMap::new(),
            &contexts,
            &trade_flows,
        );
        assert!(opportunities.is_empty());

        let misses = strategy.find_near_misses(
            std::slice::from_ref(&market),
            &books,
            &HashMap::new(),
            &contexts,
            &trade_flows,
            1,
        );
        assert_eq!(misses.len(), 1);
        assert_eq!(misses[0].kind, OpportunityKind::CodexSentinelV1);
        assert!(misses[0].reason.contains("stale 1s/5s signal"));
    }

    #[test]
    fn codex_sentinel_v1_attack_size_uses_larger_notional_for_confirmed_flow() {
        let mut config = strategy_config();
        config.enable_bundle = false;
        config.enable_directional = false;
        config.enable_micro_breakout = false;
        config.enable_target_state_v1 = false;
        config.enable_bonereaper_state_v2 = false;
        config.enable_codex_sentinel_v1 = true;
        config.min_top_of_book_shares = decimal("5");
        config.bonereaper_state_v2_min_seconds_left = 210;
        config.bonereaper_state_v2_max_seconds_left = 270;
        config.bonereaper_state_v2_bias_min_target_gap_bps = decimal("1.1");
        config.bonereaper_state_v2_flip_max_target_gap_bps = decimal("1.1");
        config.bonereaper_state_v2_min_signal_bps = 2;
        config.bonereaper_state_v2_min_spot_move_15s_bps = Decimal::ZERO;
        config.bonereaper_state_v2_min_spot_move_5s_bps = Decimal::ZERO;
        config.bonereaper_state_v2_min_aligned_flow_bps = Decimal::ZERO;
        config.bonereaper_state_v2_max_entry_price = decimal("0.70");
        config.bonereaper_state_v2_max_fair_price = decimal("0.89");
        config.bonereaper_state_v2_normal_notional_usdc = decimal("6");
        config.bonereaper_state_v2_min_expected_profit_usdc = decimal("0.75");
        config.codex_sentinel_v1_max_entry_price = decimal("0.70");
        config.codex_sentinel_v1_attack_size_enabled = true;
        config.codex_sentinel_v1_attack_notional_usdc = decimal("10");
        config.codex_sentinel_v1_attack_min_signal_bps = Decimal::from(650);
        config.codex_sentinel_v1_attack_min_flow_bps = Decimal::from(700);
        config.codex_sentinel_v1_attack_min_confirmation_bps = decimal("0.50");
        config.codex_sentinel_v1_attack_max_entry_price = decimal("0.60");
        config.directional_min_model_edge_bps = 0;
        config.directional_confidence_bps_per_spot_bps = 420;

        let strategy = BundleArbitrageStrategy::new(config);
        let market = market();
        let mut books = HashMap::new();
        books.insert(
            "up-token".to_owned(),
            OrderBook {
                asset_id: "up-token".to_owned(),
                bids: Vec::new(),
                asks: vec![BookLevel {
                    price: decimal("0.50"),
                    size: decimal("150"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );
        books.insert(
            "down-token".to_owned(),
            OrderBook {
                asset_id: "down-token".to_owned(),
                bids: Vec::new(),
                asks: vec![BookLevel {
                    price: decimal("0.51"),
                    size: decimal("150"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );

        let mut contexts = HashMap::new();
        contexts.insert(
            market.slug.clone(),
            BtcFiveMinuteContext {
                target: MarketTarget::Btc5m,
                interval_open_price: decimal("76622.47"),
                target_price: decimal("76622.47"),
                target_price_source: TargetPriceSource::BinanceWindowOpenFallback,
                target_gap_bps: decimal("1.20"),
                current_spot_price: decimal("76631.66"),
                current_spot_source: "test-fixture".to_owned(),
                current_spot_event_age_ms: None,
                current_spot_received_age_ms: None,
                current_spot_quote_points: None,
                exchange_book_age_ms: None,
                exchange_book_top_imbalance_bps: Decimal::ZERO,
                exchange_book_depth_imbalance_bps: Decimal::ZERO,
                exchange_book_microprice_bps: Decimal::ZERO,
                exchange_book_spread_bps: Decimal::ZERO,
                micro_burst_reference_price: decimal("76625.53"),
                micro_reference_price: decimal("76625.53"),
                spot_move_bps: decimal("1.20"),
                spot_move_1s_bps: decimal("0.80"),
                spot_move_5s_bps: decimal("0.80"),
                spot_move_15s_bps: decimal("0.80"),
                micro_acceleration_bps: decimal("0.05"),
                dominant_outcome: "Up".to_owned(),
                seconds_left: 247,
            },
        );

        let mut trade_flows = HashMap::new();
        trade_flows.insert(
            market.slug.clone(),
            TradeFlowSummary {
                signed_up_imbalance_bps: Decimal::from(1100),
                trade_count: 10,
                ..TradeFlowSummary::default()
            },
        );

        let opportunities = strategy.find_opportunities(
            std::slice::from_ref(&market),
            &books,
            &HashMap::new(),
            &contexts,
            &trade_flows,
        );

        assert_eq!(opportunities.len(), 1);
        assert_eq!(opportunities[0].kind, OpportunityKind::CodexSentinelV1);
        assert_eq!(opportunities[0].required_usdc, decimal("10.000000"));
    }

    #[test]
    fn codex_sentinel_v1_attack_size_requires_fresh_confirmation() {
        let mut config = strategy_config();
        config.bonereaper_state_v2_normal_notional_usdc = decimal("6");
        config.codex_sentinel_v1_attack_size_enabled = true;
        config.codex_sentinel_v1_attack_notional_usdc = decimal("10");
        config.codex_sentinel_v1_attack_min_signal_bps = Decimal::from(650);
        config.codex_sentinel_v1_attack_min_flow_bps = Decimal::from(700);
        config.codex_sentinel_v1_attack_min_confirmation_bps = decimal("0.50");
        config.codex_sentinel_v1_attack_max_entry_price = decimal("0.60");
        let mut decision = test_codex_decision("0", "2.23");
        decision.signal_tier = BonereaperStateV2SignalTier::Normal;
        let context = test_codex_context("2.23", "0");

        let notional = codex_sentinel_v1_entry_notional_cap(
            decimal("100"),
            &context,
            decision,
            decimal("0.56"),
            codex_sentinel_v1_confidence_score(&context, &decision, decimal("0.56"), &config),
            &config,
        );

        assert_eq!(notional, decimal("6.000000"));
    }

    #[test]
    fn codex_sentinel_v1_bad_window_guard_blocks_low_confidence_chop() {
        let mut config = strategy_config();
        config.codex_sentinel_v1_bad_window_guard_enabled = true;
        config.codex_sentinel_v1_bad_window_min_score = Decimal::from(35);
        let mut decision = test_codex_decision("0.02", "0.10");
        decision.aligned_flow_bps = Decimal::ZERO;
        decision.signal_strength_bps = decimal("3.0");
        let context = test_codex_context("1.10", "0.01");

        assert!(codex_sentinel_v1_bad_window_guard_blocks(
            &context,
            &decision,
            decimal("0.69"),
            &config,
        ));
    }

    #[test]
    fn codex_sentinel_v1_bad_window_guard_allows_high_confidence_signal() {
        let mut config = strategy_config();
        config.codex_sentinel_v1_bad_window_guard_enabled = true;
        config.codex_sentinel_v1_bad_window_min_score = Decimal::from(35);
        let mut decision = test_codex_decision("2.50", "3.00");
        decision.aligned_flow_bps = Decimal::from(1_800);
        decision.signal_strength_bps = Decimal::from(1_200);
        let context = test_codex_context("3.00", "2.50");

        assert!(!codex_sentinel_v1_bad_window_guard_blocks(
            &context,
            &decision,
            decimal("0.58"),
            &config,
        ));
    }

    #[test]
    fn codex_sentinel_v1_bad_window_guard_blocks_counter_burst_above_discount() {
        let mut config = strategy_config();
        config.codex_sentinel_v1_counter_burst_guard_enabled = true;
        config.codex_sentinel_v1_counter_burst_min_bps = decimal("0.75");
        config.codex_sentinel_v1_counter_burst_max_entry_price = decimal("0.55");
        let mut decision = test_codex_decision("1.78", "4.47");
        decision.aligned_flow_bps = Decimal::from(1_366);
        decision.signal_strength_bps = Decimal::from(829);
        let context = test_codex_context("4.54", "-1.20");

        assert!(codex_sentinel_v1_bad_window_guard_blocks(
            &context,
            &decision,
            decimal("0.59"),
            &config,
        ));
    }

    #[test]
    fn codex_sentinel_v1_bad_window_guard_allows_discounted_counter_burst() {
        let mut config = strategy_config();
        config.codex_sentinel_v1_counter_burst_guard_enabled = true;
        config.codex_sentinel_v1_counter_burst_min_bps = decimal("0.75");
        config.codex_sentinel_v1_counter_burst_max_entry_price = decimal("0.55");
        let mut decision = test_codex_decision("1.78", "4.47");
        decision.aligned_flow_bps = Decimal::from(1_366);
        decision.signal_strength_bps = Decimal::from(829);
        let context = test_codex_context("4.54", "-1.20");

        assert!(!codex_sentinel_v1_bad_window_guard_blocks(
            &context,
            &decision,
            decimal("0.53"),
            &config,
        ));
    }

    #[test]
    fn codex_sentinel_v1_low_flow_guard_blocks_weak_momentum_probe() {
        let mut config = strategy_config();
        config.codex_sentinel_v1_low_flow_guard_enabled = true;
        config.codex_sentinel_v1_low_flow_max_flow_bps = Decimal::from(100);
        config.codex_sentinel_v1_low_flow_allow_min_signal_bps = Decimal::from(40);
        config.codex_sentinel_v1_low_flow_allow_min_fresh_bps = decimal("3.00");
        config.codex_sentinel_v1_low_flow_allow_min_swing_bps = decimal("3.00");
        config.codex_sentinel_v1_low_flow_allow_max_entry_price = decimal("0.58");
        let mut decision = test_codex_decision("2.82", "1.60");
        decision.aligned_flow_bps = Decimal::ZERO;
        decision.signal_strength_bps = decimal("8.71");
        let context = test_codex_context("3.77", "0.00");

        assert!(codex_sentinel_v1_low_flow_guard_blocks(
            &context,
            &decision,
            decimal("0.49"),
            &config,
        ));
    }

    #[test]
    fn codex_sentinel_v1_low_flow_guard_allows_strong_discount_momentum() {
        let mut config = strategy_config();
        config.codex_sentinel_v1_low_flow_guard_enabled = true;
        config.codex_sentinel_v1_low_flow_max_flow_bps = Decimal::from(100);
        config.codex_sentinel_v1_low_flow_allow_min_signal_bps = Decimal::from(40);
        config.codex_sentinel_v1_low_flow_allow_min_fresh_bps = decimal("3.00");
        config.codex_sentinel_v1_low_flow_allow_min_swing_bps = decimal("3.00");
        config.codex_sentinel_v1_low_flow_allow_max_entry_price = decimal("0.58");
        let mut decision = test_codex_decision("3.15", "4.00");
        decision.aligned_flow_bps = decimal("62.63");
        decision.signal_strength_bps = decimal("48.01");
        let context = test_codex_context("3.20", "0.00");

        assert!(!codex_sentinel_v1_low_flow_guard_blocks(
            &context,
            &decision,
            decimal("0.56"),
            &config,
        ));
    }

    #[test]
    fn codex_sentinel_v1_confidence_sizing_scales_high_quality_signal() {
        let mut config = strategy_config();
        config.bonereaper_state_v2_normal_notional_usdc = decimal("6");
        config.codex_sentinel_v1_confidence_sizing_enabled = true;
        config.codex_sentinel_v1_confidence_min_score = Decimal::from(40);
        config.codex_sentinel_v1_confidence_max_multiplier = decimal("1.50");
        let mut decision = test_codex_decision("2.50", "3.00");
        decision.aligned_flow_bps = Decimal::from(1_800);
        decision.signal_strength_bps = Decimal::from(1_200);
        let context = test_codex_context("3.00", "2.50");
        let confidence_score =
            codex_sentinel_v1_confidence_score(&context, &decision, decimal("0.56"), &config);

        let notional = codex_sentinel_v1_entry_notional_cap(
            decimal("100"),
            &context,
            decision,
            decimal("0.56"),
            confidence_score,
            &config,
        );

        assert_eq!(confidence_score, Decimal::from(100));
        assert_eq!(notional, decimal("9.000000"));
    }

    #[test]
    fn codex_sentinel_v1_blocks_unconfirmed_mid_signal_chop() {
        let mut config = strategy_config();
        config.enable_bundle = false;
        config.enable_directional = false;
        config.enable_micro_breakout = false;
        config.enable_target_state_v1 = false;
        config.enable_bonereaper_state_v2 = false;
        config.enable_codex_sentinel_v1 = true;
        config.bonereaper_state_v2_min_seconds_left = 210;
        config.bonereaper_state_v2_max_seconds_left = 270;
        config.bonereaper_state_v2_bias_min_target_gap_bps = decimal("1.2");
        config.bonereaper_state_v2_flip_max_target_gap_bps = decimal("1.2");
        config.bonereaper_state_v2_min_signal_bps = 2;
        config.bonereaper_state_v2_min_spot_move_15s_bps = Decimal::ZERO;
        config.bonereaper_state_v2_min_spot_move_5s_bps = Decimal::ZERO;
        config.bonereaper_state_v2_max_entry_price = decimal("0.76");
        config.bonereaper_state_v2_max_fair_price = decimal("0.86");
        config.bonereaper_state_v2_normal_notional_usdc = decimal("4");
        config.bonereaper_state_v2_min_expected_profit_usdc = decimal("0.30");
        config.directional_min_model_edge_bps = 0;
        let strategy = BundleArbitrageStrategy::new(config);
        let market = market();

        let mut books = HashMap::new();
        books.insert(
            "up-token".to_owned(),
            OrderBook {
                asset_id: "up-token".to_owned(),
                bids: Vec::new(),
                asks: vec![BookLevel {
                    price: decimal("0.45"),
                    size: decimal("150"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );
        books.insert(
            "down-token".to_owned(),
            OrderBook {
                asset_id: "down-token".to_owned(),
                bids: Vec::new(),
                asks: vec![BookLevel {
                    price: decimal("0.56"),
                    size: decimal("150"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );

        let mut contexts = HashMap::new();
        contexts.insert(
            market.slug.clone(),
            BtcFiveMinuteContext {
                target: MarketTarget::Btc5m,
                interval_open_price: decimal("67000"),
                target_price: decimal("67000"),
                target_price_source: TargetPriceSource::PolymarketEventMetadata,
                target_gap_bps: decimal("2.9031"),
                current_spot_price: decimal("67019.45"),
                current_spot_source: "test-fixture".to_owned(),
                current_spot_event_age_ms: None,
                current_spot_received_age_ms: None,
                current_spot_quote_points: None,
                exchange_book_age_ms: None,
                exchange_book_top_imbalance_bps: Decimal::ZERO,
                exchange_book_depth_imbalance_bps: Decimal::ZERO,
                exchange_book_microprice_bps: Decimal::ZERO,
                exchange_book_spread_bps: Decimal::ZERO,
                micro_burst_reference_price: decimal("67019.45"),
                micro_reference_price: decimal("67019.45"),
                spot_move_bps: decimal("2.9031"),
                spot_move_1s_bps: Decimal::ZERO,
                spot_move_5s_bps: Decimal::ZERO,
                spot_move_15s_bps: Decimal::ZERO,
                micro_acceleration_bps: Decimal::ZERO,
                dominant_outcome: "Up".to_owned(),
                seconds_left: 235,
            },
        );

        let opportunities = strategy.find_opportunities(
            std::slice::from_ref(&market),
            &books,
            &HashMap::new(),
            &contexts,
            &HashMap::new(),
        );
        assert!(opportunities.is_empty());

        let misses = strategy.find_near_misses(
            std::slice::from_ref(&market),
            &books,
            &HashMap::new(),
            &contexts,
            &HashMap::new(),
            1,
        );
        assert_eq!(misses.len(), 1);
        assert_eq!(misses[0].kind, OpportunityKind::CodexSentinelV1);
        assert!(
            misses[0]
                .reason
                .contains("mid-signal needs fresh confirmation")
        );
    }

    #[test]
    fn target_state_v1_reports_near_miss_when_window_is_too_early() {
        let mut config = strategy_config();
        config.enable_bundle = false;
        config.enable_directional = false;
        config.enable_micro_breakout = false;
        config.enable_target_state_v1 = true;
        config.target_state_min_elapsed_window_secs = 150;
        config.target_state_max_seconds_left = 180;
        config.target_state_min_target_gap_bps = decimal("8");
        config.target_state_min_signal_bps = 10;
        config.target_state_min_spot_move_15s_bps = decimal("1.2");
        config.target_state_min_aligned_flow_bps = decimal("0.5");
        config.target_state_max_entry_price = decimal("0.72");
        let strategy = BundleArbitrageStrategy::new(config);
        let market = market();

        let mut books = HashMap::new();
        books.insert(
            "up-token".to_owned(),
            OrderBook {
                asset_id: "up-token".to_owned(),
                bids: Vec::new(),
                asks: vec![BookLevel {
                    price: decimal("0.61"),
                    size: decimal("150"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );
        books.insert(
            "down-token".to_owned(),
            OrderBook {
                asset_id: "down-token".to_owned(),
                bids: Vec::new(),
                asks: vec![BookLevel {
                    price: decimal("0.40"),
                    size: decimal("150"),
                }],
                min_order_size: None,
                tick_size: None,
            },
        );

        let mut contexts = HashMap::new();
        contexts.insert(
            market.slug.clone(),
            BtcFiveMinuteContext {
                target: MarketTarget::Btc5m,
                interval_open_price: decimal("67000"),
                target_price: decimal("67000"),
                target_price_source: TargetPriceSource::PolymarketEventMetadata,
                target_gap_bps: decimal("12.00"),
                current_spot_price: decimal("67080"),
                current_spot_source: "test-fixture".to_owned(),
                current_spot_event_age_ms: None,
                current_spot_received_age_ms: None,
                current_spot_quote_points: None,
                exchange_book_age_ms: None,
                exchange_book_top_imbalance_bps: Decimal::ZERO,
                exchange_book_depth_imbalance_bps: Decimal::ZERO,
                exchange_book_microprice_bps: Decimal::ZERO,
                exchange_book_spread_bps: Decimal::ZERO,
                micro_burst_reference_price: decimal("67070"),
                micro_reference_price: decimal("67040"),
                spot_move_bps: decimal("12.00"),
                spot_move_1s_bps: decimal("0.5"),
                spot_move_5s_bps: decimal("1.3"),
                spot_move_15s_bps: decimal("1.8"),
                micro_acceleration_bps: decimal("0.4"),
                dominant_outcome: "Up".to_owned(),
                seconds_left: 210,
            },
        );

        let mut trade_flows = HashMap::new();
        trade_flows.insert(
            market.slug.clone(),
            TradeFlowSummary {
                up_pressure_notional: decimal("1800"),
                down_pressure_notional: decimal("700"),
                total_pressure_notional: decimal("2500"),
                signed_up_imbalance_bps: decimal("4400"),
                trade_count: 16,
            },
        );

        let opportunities = strategy.find_opportunities(
            std::slice::from_ref(&market),
            &books,
            &HashMap::new(),
            &contexts,
            &trade_flows,
        );
        assert!(opportunities.is_empty());

        let misses = strategy.find_near_misses(
            std::slice::from_ref(&market),
            &books,
            &HashMap::new(),
            &contexts,
            &trade_flows,
            5,
        );
        assert!(!misses.is_empty());
        assert_eq!(misses[0].kind, OpportunityKind::TargetStateV1);
    }
}
