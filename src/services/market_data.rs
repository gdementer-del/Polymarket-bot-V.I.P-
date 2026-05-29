//! Polymarket HTTP clients used for market discovery, price history, and order book snapshots.

use std::cmp::Reverse;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::time::Duration;

use chrono::Utc;
use futures_util::SinkExt;
use futures_util::stream::{self, SplitSink, StreamExt};
use reqwest::{Client, StatusCode};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde::de::{self, Deserializer};
use tokio::net::TcpStream;
use tokio::sync::{RwLock, watch};
use tokio::time::sleep;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tracing::{debug, info, warn};

use crate::config::HttpConfig;
use crate::error::{AppError, Result};
use crate::models::{
    BinaryMarket, BookLevel, BooksRequestItem, GammaMarket, GeoblockResponse, MarketTarget,
    OrderBook,
};

use super::labels::{
    outcome_label_is_down, outcome_label_is_up, wallet_side_is_buy_label, wallet_side_is_sell_label,
};

const MARKET_LOOKUP_CONCURRENCY: usize = 16;
const MARKET_LOOKUP_MULTIPLIER: usize = 3;
const MARKET_LOOKUP_CAP: usize = 120;
const TRADE_FLOW_BATCH_SIZE: usize = 20;
const TRADE_FLOW_PAGE_LIMIT: usize = 500;
const HTTP_RETRY_ATTEMPTS: u8 = 3;
const HTTP_RETRY_DELAY_MS: u64 = 400;
const POLYMARKET_MARKET_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const POLYMARKET_MARKET_WS_RECONNECT_DELAY_MS: u64 = 300;
const LIVE_TRADE_RETENTION_MS: i64 = 60 * 60 * 1_000;
const MARKET_BY_SLUG_CACHE_TTL_MS: i64 = 5 * 60 * 1_000;
const LIVE_MARKET_SET_TTL_MS: i64 = 10_000;
const SERVER_TIME_TTL_MS: i64 = 30_000;
const MARKET_BY_SLUG_CACHE_MAX_ENTRIES: usize = 512;
const LIVE_SUBSCRIPTION_PAST_GRACE_SECS: i64 = 60;
const LIVE_SUBSCRIPTION_FUTURE_WINDOWS: i64 = 2;

/// Lightweight historical price point for one Polymarket token.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct PriceHistoryPoint {
    pub timestamp_ms: i64,
    pub price: Decimal,
}

/// Time-bounded trade-flow lookup window for one market.
#[derive(Debug, Clone)]
pub struct TradeFlowWindow {
    pub slug: String,
    pub condition_id: String,
    pub start_ts_ms: i64,
    pub end_ts_ms: i64,
}

/// Aggregated recent directional pressure from Polymarket trades.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Default)]
pub struct TradeFlowSummary {
    pub up_pressure_notional: Decimal,
    pub down_pressure_notional: Decimal,
    pub total_pressure_notional: Decimal,
    pub signed_up_imbalance_bps: Decimal,
    pub trade_count: usize,
}

impl TradeFlowSummary {
    /// Return directional pressure aligned with the Binance-dominant side.
    #[must_use]
    pub fn aligned_imbalance_bps(self, dominant_outcome: &str) -> Decimal {
        if outcome_label_is_up(dominant_outcome) {
            self.signed_up_imbalance_bps
        } else if outcome_label_is_down(dominant_outcome) {
            -self.signed_up_imbalance_bps
        } else {
            Decimal::ZERO
        }
    }
}

#[derive(Debug, Clone)]
struct LiveAssetMeta {
    slug: String,
    condition_id: String,
    is_up_outcome: bool,
}

#[derive(Debug, Clone)]
struct CachedOrderBook {
    book: OrderBook,
    updated_at_ms: i64,
}

#[derive(Debug, Clone)]
struct CachedMarketBySlug {
    market: BinaryMarket,
    cached_at_ms: i64,
}

#[derive(Debug, Clone, Copy)]
struct CachedServerTime {
    server_ts: i64,
    cached_at_ms: i64,
}

#[derive(Debug, Clone)]
struct CachedLiveMarketSet {
    markets: Vec<BinaryMarket>,
    cached_at_ms: i64,
}

#[derive(Debug, Clone, Copy)]
struct LiveTradeEvent {
    timestamp_ms: i64,
    up_pressure_notional: Decimal,
    down_pressure_notional: Decimal,
}

#[derive(Debug, Default)]
struct PolymarketLiveState {
    desired_assets: HashSet<String>,
    asset_meta: HashMap<String, LiveAssetMeta>,
    books: HashMap<String, CachedOrderBook>,
    trade_events_by_slug: HashMap<String, VecDeque<LiveTradeEvent>>,
    slug_stream_started_at_ms: HashMap<String, i64>,
}

#[derive(Debug)]
struct TradeFlowLiveSnapshot {
    live_summaries: HashMap<String, TradeFlowSummary>,
    backfill_windows: Vec<TradeFlowWindow>,
}

#[derive(Debug)]
struct PolymarketLiveStream {
    state: Arc<RwLock<PolymarketLiveState>>,
    subscription_revision_tx: watch::Sender<u64>,
    market_update_tx: watch::Sender<u64>,
    started: AtomicBool,
}

impl PolymarketLiveStream {
    fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(PolymarketLiveState::default())),
            subscription_revision_tx: watch::channel(0_u64).0,
            market_update_tx: watch::channel(0_u64).0,
            started: AtomicBool::new(false),
        }
    }

    fn ensure_started(&self) {
        if self.started.swap(true, AtomicOrdering::AcqRel) {
            return;
        }

        let ws_url = POLYMARKET_MARKET_WS_URL.to_owned();
        let state = Arc::clone(&self.state);
        let revision_rx = self.subscription_revision_tx.subscribe();
        let market_update_tx = self.market_update_tx.clone();

        std::mem::drop(tokio::spawn(async move {
            run_polymarket_market_stream_loop(state, revision_rx, market_update_tx, ws_url).await;
        }));
    }

    fn subscribe_market_updates(&self) -> watch::Receiver<u64> {
        self.market_update_tx.subscribe()
    }

    async fn register_markets(&self, markets: &[BinaryMarket]) {
        if markets.is_empty() {
            return;
        }

        let mut incoming_asset_meta = HashMap::with_capacity(markets.len().saturating_mul(2));
        let mut incoming_slugs = HashSet::with_capacity(markets.len());
        let now_ms = Utc::now().timestamp_millis();
        let now_ts = now_ms.div_euclid(1_000);

        for market in markets {
            let up_a = outcome_label_is_up(&market.outcome_a_label);
            let up_b = outcome_label_is_up(&market.outcome_b_label);

            incoming_slugs.insert(market.slug.clone());

            incoming_asset_meta.insert(
                market.outcome_a_token_id.clone(),
                LiveAssetMeta {
                    slug: market.slug.clone(),
                    condition_id: market.condition_id.clone(),
                    is_up_outcome: up_a,
                },
            );
            incoming_asset_meta.insert(
                market.outcome_b_token_id.clone(),
                LiveAssetMeta {
                    slug: market.slug.clone(),
                    condition_id: market.condition_id.clone(),
                    is_up_outcome: up_b,
                },
            );
        }

        let mut state = self.state.write().await;
        let previous_desired_assets = state.desired_assets.clone();
        state.asset_meta.extend(incoming_asset_meta);

        let live_slugs = state
            .asset_meta
            .values()
            .map(|meta| meta.slug.clone())
            .filter(|slug| {
                incoming_slugs.contains(slug) || live_subscription_slug_is_relevant(slug, now_ts)
            })
            .collect::<HashSet<_>>();

        state
            .asset_meta
            .retain(|_, meta| live_slugs.contains(&meta.slug));
        state.desired_assets = state.asset_meta.keys().cloned().collect();
        let desired_assets_snapshot = state.desired_assets.clone();
        state
            .books
            .retain(|asset_id, _| desired_assets_snapshot.contains(asset_id));
        state
            .trade_events_by_slug
            .retain(|slug, _| live_slugs.contains(slug));
        state
            .slug_stream_started_at_ms
            .retain(|slug, _| live_slugs.contains(slug));
        for slug in incoming_slugs {
            state
                .slug_stream_started_at_ms
                .entry(slug)
                .or_insert(now_ms);
        }
        let desired_changed = previous_desired_assets != state.desired_assets;
        drop(state);

        if desired_changed {
            self.subscription_revision_tx
                .send_modify(|revision| *revision = revision.saturating_add(1));
        }
    }

    async fn books_for_assets(
        &self,
        token_ids: &[String],
        max_staleness_ms: i64,
    ) -> HashMap<String, OrderBook> {
        if token_ids.is_empty() {
            return HashMap::new();
        }

        let now_ms = Utc::now().timestamp_millis();
        let state = self.state.read().await;
        token_ids
            .iter()
            .filter_map(|token_id| {
                let cached = state.books.get(token_id)?;
                if now_ms.saturating_sub(cached.updated_at_ms) > max_staleness_ms {
                    return None;
                }

                Some((token_id.clone(), cached.book.clone()))
            })
            .collect()
    }

    async fn trade_flow_snapshot(
        &self,
        windows: &[TradeFlowWindow],
        backfill_trade_flow: bool,
    ) -> TradeFlowLiveSnapshot {
        if windows.is_empty() {
            return TradeFlowLiveSnapshot {
                live_summaries: HashMap::new(),
                backfill_windows: Vec::new(),
            };
        }

        let state = self.state.read().await;
        let mut live_summaries = HashMap::with_capacity(windows.len());
        let mut backfill_windows = Vec::new();

        for window in windows {
            let Some(stream_started_at_ms) =
                state.slug_stream_started_at_ms.get(&window.slug).copied()
            else {
                backfill_windows.push(window.clone());
                continue;
            };

            let live_start_ts_ms = window.start_ts_ms.max(stream_started_at_ms);
            if backfill_trade_flow && window.start_ts_ms < live_start_ts_ms {
                backfill_windows.push(TradeFlowWindow {
                    slug: window.slug.clone(),
                    condition_id: window.condition_id.clone(),
                    start_ts_ms: window.start_ts_ms,
                    end_ts_ms: live_start_ts_ms.saturating_sub(1),
                });
            }

            if live_start_ts_ms > window.end_ts_ms {
                continue;
            }

            let mut summary = TradeFlowSummary::default();
            if let Some(events) = state.trade_events_by_slug.get(&window.slug) {
                for event in events {
                    if event.timestamp_ms < live_start_ts_ms
                        || event.timestamp_ms > window.end_ts_ms
                    {
                        continue;
                    }

                    summary.up_pressure_notional += event.up_pressure_notional;
                    summary.down_pressure_notional += event.down_pressure_notional;
                    summary.total_pressure_notional +=
                        event.up_pressure_notional + event.down_pressure_notional;
                    summary.trade_count += 1;
                }
            }

            if summary.total_pressure_notional > Decimal::ZERO {
                summary.signed_up_imbalance_bps = ((summary.up_pressure_notional
                    - summary.down_pressure_notional)
                    / summary.total_pressure_notional
                    * Decimal::from(10_000_u32))
                .round_dp(4);
            }

            if summary.trade_count > 0 {
                live_summaries.insert(window.slug.clone(), summary);
            }
        }

        TradeFlowLiveSnapshot {
            live_summaries,
            backfill_windows,
        }
    }
}

/// Public profile activity record from Polymarket Data API.
#[derive(Debug, Clone, Deserialize)]
pub struct ProfileActivityRecord {
    #[serde(default, alias = "proxyWallet")]
    pub proxy_wallet: String,
    #[serde(default)]
    pub timestamp: i64,
    #[serde(default, alias = "type")]
    pub activity_type: String,
    #[serde(default)]
    pub slug: String,
    #[serde(default)]
    pub outcome: String,
    #[serde(default)]
    pub side: String,
    #[serde(default, alias = "price", deserialize_with = "option_decimal_from_any")]
    pub price: Option<Decimal>,
    #[serde(
        default,
        alias = "usdcSize",
        alias = "usdc_size",
        deserialize_with = "option_decimal_from_any"
    )]
    pub usdc_size: Option<Decimal>,
    #[serde(default, alias = "transactionHash")]
    pub transaction_hash: String,
}

impl ProfileActivityRecord {
    /// Returns true when the activity belongs to BTC 5-minute markets.
    #[must_use]
    pub fn is_btc_5m(&self) -> bool {
        self.slug.starts_with("btc-updown-5m-")
    }

    /// Stable unique key for de-duplicating activity events.
    #[must_use]
    pub fn dedupe_key(&self) -> String {
        if !self.transaction_hash.is_empty() {
            return self.transaction_hash.clone();
        }

        format!(
            "{}:{}:{}:{}:{}",
            self.proxy_wallet, self.timestamp, self.slug, self.side, self.outcome
        )
    }
}

/// Read-only Polymarket data client.
#[derive(Debug, Clone)]
pub struct MarketDataClient {
    http: Client,
    http_config: HttpConfig,
    live_stream: Arc<PolymarketLiveStream>,
    market_by_slug_cache: Arc<RwLock<HashMap<String, CachedMarketBySlug>>>,
    server_time_cache: Arc<RwLock<Option<CachedServerTime>>>,
    live_market_set_cache: Arc<RwLock<HashMap<String, CachedLiveMarketSet>>>,
}

impl MarketDataClient {
    /// Create a new client from config.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying HTTP client cannot be built.
    pub fn new(http_config: HttpConfig) -> Result<Self> {
        let http = Client::builder()
            .timeout(Duration::from_secs(http_config.timeout_secs))
            .user_agent("polymarket_mvp/0.1.0")
            .build()?;
        Ok(Self {
            http,
            http_config,
            live_stream: Arc::new(PolymarketLiveStream::new()),
            market_by_slug_cache: Arc::new(RwLock::new(HashMap::new())),
            server_time_cache: Arc::new(RwLock::new(None)),
            live_market_set_cache: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Ensure Polymarket market WebSocket stream is running.
    pub fn ensure_market_stream_started(&self) {
        self.live_stream.ensure_started();
    }

    /// Subscribe to lightweight Polymarket market-data update events.
    #[must_use]
    pub fn subscribe_market_triggers(&self) -> watch::Receiver<u64> {
        self.live_stream.subscribe_market_updates()
    }

    /// Register current markets for live orderbook/trade subscription.
    pub async fn register_live_markets(&self, markets: &[BinaryMarket]) {
        if markets.is_empty() {
            return;
        }

        self.live_stream.register_markets(markets).await;
    }

    /// Fetch active binary markets, normalized into `BinaryMarket`.
    ///
    /// # Errors
    ///
    /// Returns an error if the Gamma API request fails or returns invalid JSON.
    pub async fn fetch_active_binary_markets(
        &self,
        max_markets: usize,
    ) -> Result<Vec<BinaryMarket>> {
        let mut markets = Vec::new();
        let mut offset = 0usize;

        while markets.len() < max_markets {
            let limit = self
                .http_config
                .page_size
                .min(max_markets.saturating_sub(markets.len()));

            let response = self
                .http
                .get(format!("{}/markets", self.http_config.gamma_base_url))
                .query(&[
                    ("active", "true"),
                    ("closed", "false"),
                    ("limit", &limit.to_string()),
                    ("offset", &offset.to_string()),
                ])
                .send()
                .await?
                .error_for_status()?
                .json::<Vec<GammaMarket>>()
                .await?;

            let response_len = response.len();

            for raw in response {
                if let Some(market) = raw.into_binary_market()? {
                    markets.push(market);
                }
                if markets.len() >= max_markets {
                    break;
                }
            }

            if response_len < limit {
                break;
            }

            offset += limit;
        }

        Ok(markets)
    }

    /// Fetch active supported fast markets for the requested targets.
    ///
    /// # Errors
    ///
    /// Returns an error if the Gamma API requests fail.
    pub async fn fetch_target_markets(
        &self,
        targets: &[MarketTarget],
        max_markets: usize,
    ) -> Result<Vec<BinaryMarket>> {
        if max_markets == 0 || targets.is_empty() {
            return Ok(Vec::new());
        }

        let unique_targets = dedupe_targets(targets);
        let target_count = unique_targets.len().max(1);
        let per_target = max_markets
            .div_ceil(target_count)
            .clamp(8, MARKET_LOOKUP_CAP);
        let now_ts = self.current_server_time_secs_fast().await;

        let candidate_slugs = unique_targets
            .iter()
            .flat_map(|target| generate_target_market_slugs(*target, per_target, now_ts))
            .collect::<Vec<_>>();

        let fetched_markets = stream::iter(candidate_slugs.into_iter().map(|slug| {
            let client = self.clone();
            async move { client.fetch_market_by_slug_with_options(&slug, true).await }
        }))
        .buffer_unordered(MARKET_LOOKUP_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;

        let mut markets = fetched_markets
            .into_iter()
            .filter_map(|result| match result {
                Ok(Some(market)) => Some(Ok(market)),
                Ok(None) => None,
                Err(error) => Some(Err(error)),
            })
            .collect::<Result<Vec<_>>>()?;

        markets.retain(|market| {
            market
                .target()
                .is_some_and(|target| unique_targets.contains(&target))
                && market.is_supported_target_market()
        });
        markets.sort_by(|left, right| compare_markets(right, left, now_ts));
        markets.truncate(max_markets);

        Ok(markets)
    }

    /// Fetch active BTC 5-minute markets.
    ///
    /// # Errors
    ///
    /// Returns an error if the Gamma API requests fail.
    pub async fn fetch_btc_5m_markets(&self, max_markets: usize) -> Result<Vec<BinaryMarket>> {
        self.fetch_target_markets(&[MarketTarget::Btc5m], max_markets)
            .await
    }

    /// Fetch the currently live supported markets for the requested targets
    /// using a short-lived in-memory cache.
    ///
    /// # Errors
    ///
    /// Returns an error if the Gamma API requests fail.
    pub async fn fetch_cached_current_live_markets(
        &self,
        targets: &[MarketTarget],
    ) -> Result<Vec<BinaryMarket>> {
        if targets.is_empty() {
            return Ok(Vec::new());
        }

        let key = live_market_cache_key(targets);
        let local_now_ms = Utc::now().timestamp_millis();
        if let Some(cached) = {
            let cache = self.live_market_set_cache.read().await;
            cache.get(&key).cloned()
        } && local_now_ms.saturating_sub(cached.cached_at_ms) <= LIVE_MARKET_SET_TTL_MS
        {
            return Ok(cached.markets);
        }

        let now_ts = self.current_server_time_secs_fast().await;
        let unique_targets = dedupe_targets(targets);
        let mut seen = HashSet::with_capacity(unique_targets.len().saturating_mul(2));
        let mut candidate_slugs = Vec::with_capacity(unique_targets.len().saturating_mul(2));
        for target in unique_targets {
            let current_start_ts = target.window_start_ts_at(now_ts);
            let candidate_starts = [
                current_start_ts,
                current_start_ts.saturating_add(target.window_secs()),
            ];

            for start_ts in candidate_starts {
                let slug = target.slug_for_window_start(start_ts);
                if !seen.insert(slug.clone()) {
                    continue;
                }

                candidate_slugs.push(slug);
            }
        }

        let fetched_markets = stream::iter(candidate_slugs.into_iter().map(|slug| {
            let client = self.clone();
            async move { client.fetch_supported_market_by_slug(&slug).await }
        }))
        .buffer_unordered(MARKET_LOOKUP_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;
        let mut markets = Vec::with_capacity(fetched_markets.len());
        for result in fetched_markets {
            if let Some(market) = result? {
                markets.push(market);
            }
        }
        markets.sort_by(|left, right| left.slug.cmp(&right.slug));

        let mut cache = self.live_market_set_cache.write().await;
        cache.insert(
            key,
            CachedLiveMarketSet {
                markets: markets.clone(),
                cached_at_ms: local_now_ms,
            },
        );
        cache.retain(|_, entry| {
            local_now_ms.saturating_sub(entry.cached_at_ms) <= LIVE_MARKET_SET_TTL_MS
        });

        Ok(markets)
    }

    /// Read the current Polymarket/Clob server time in unix seconds.
    ///
    /// # Errors
    ///
    /// Returns an error if the CLOB time endpoint returns an invalid response.
    pub async fn current_server_time_secs(&self) -> Result<i64> {
        let local_now_ms = Utc::now().timestamp_millis();
        if let Some(cached) = *self.server_time_cache.read().await
            && local_now_ms.saturating_sub(cached.cached_at_ms) <= SERVER_TIME_TTL_MS
        {
            return Ok(project_cached_server_time_secs(cached, local_now_ms));
        }

        let server_ts = self.fetch_server_time_secs().await?;
        let local_now_ms = Utc::now().timestamp_millis();
        *self.server_time_cache.write().await = Some(CachedServerTime {
            server_ts,
            cached_at_ms: local_now_ms,
        });
        Ok(server_ts)
    }

    /// Estimate current CLOB/server time without making a network request.
    ///
    /// Reactive paper scans should not block on `/time`; if we have a server
    /// sample we project it forward, otherwise local UTC is close enough for
    /// selecting 5-minute windows.
    pub async fn current_server_time_secs_fast(&self) -> i64 {
        let local_now_ms = Utc::now().timestamp_millis();
        if let Some(cached) = *self.server_time_cache.read().await {
            return project_cached_server_time_secs(cached, local_now_ms);
        }

        local_now_ms.div_euclid(1_000)
    }

    /// Fetch a single active supported market by slug.
    ///
    /// # Errors
    ///
    /// Returns an error if the Gamma API request fails.
    pub async fn fetch_supported_market_by_slug(&self, slug: &str) -> Result<Option<BinaryMarket>> {
        self.fetch_market_by_slug_with_options(slug, true).await
    }

    /// Fetch a single active BTC 5-minute market by slug.
    ///
    /// # Errors
    ///
    /// Returns an error if the Gamma API request fails.
    pub async fn fetch_btc_5m_market_by_slug(&self, slug: &str) -> Result<Option<BinaryMarket>> {
        self.fetch_supported_market_by_slug(slug).await
    }

    /// Fetch a historical supported market by slug, even if it is already closed.
    ///
    /// # Errors
    ///
    /// Returns an error if the Gamma API request fails.
    pub async fn fetch_historical_market_by_slug(
        &self,
        slug: &str,
    ) -> Result<Option<BinaryMarket>> {
        self.fetch_market_by_slug_with_options(slug, false).await
    }

    /// Fetch order books for token IDs in batches.
    ///
    /// # Errors
    ///
    /// Returns an error if the CLOB books endpoint request fails or returns invalid JSON.
    pub async fn fetch_order_books(
        &self,
        token_ids: &[String],
    ) -> Result<HashMap<String, OrderBook>> {
        let mut books = HashMap::with_capacity(token_ids.len());

        for batch in token_ids.chunks(self.http_config.books_batch_size.max(1)) {
            let body: Vec<BooksRequestItem<'_>> = batch
                .iter()
                .map(String::as_str)
                .map(|token_id| BooksRequestItem { token_id })
                .collect();

            let response = self
                .request_with_retry("books", || {
                    self.http
                        .post(format!("{}/books", self.http_config.clob_base_url))
                        .json(&body)
                })
                .await?
                .json::<Vec<OrderBook>>()
                .await?;

            books.extend(response.into_iter().map(|mut book| {
                normalize_book_sides(&mut book);
                (book.asset_id.clone(), book)
            }));
        }

        Ok(books)
    }

    /// Fetch order books preferring live WebSocket cache with REST fallback.
    ///
    /// # Errors
    ///
    /// Returns an error if the REST fallback request fails.
    pub async fn fetch_order_books_live_first(
        &self,
        token_ids: &[String],
        max_staleness_ms: i64,
    ) -> Result<HashMap<String, OrderBook>> {
        self.fetch_order_books_live_first_with_fallback(token_ids, max_staleness_ms, true)
            .await
    }

    /// Fetch order books preferring live WebSocket cache with optional REST fallback.
    ///
    /// # Errors
    ///
    /// Returns an error if the REST fallback request fails.
    pub async fn fetch_order_books_live_first_with_fallback(
        &self,
        token_ids: &[String],
        max_staleness_ms: i64,
        allow_rest_fallback: bool,
    ) -> Result<HashMap<String, OrderBook>> {
        if token_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let mut books = self
            .live_stream
            .books_for_assets(token_ids, max_staleness_ms.max(1))
            .await;

        let missing_ids = token_ids
            .iter()
            .filter(|token_id| !books.contains_key((*token_id).as_str()))
            .cloned()
            .collect::<Vec<_>>();

        let rest_count = missing_ids.len();
        if allow_rest_fallback && !missing_ids.is_empty() {
            books.extend(self.fetch_order_books(&missing_ids).await?);
        }

        let final_missing_ids = token_ids
            .iter()
            .filter(|token_id| !books.contains_key((*token_id).as_str()))
            .collect::<Vec<_>>();
        let empty_ask_count = token_ids
            .iter()
            .filter_map(|token_id| books.get(token_id))
            .filter(|book| book.best_ask().is_none())
            .count();
        let empty_bid_count = token_ids
            .iter()
            .filter_map(|token_id| books.get(token_id))
            .filter(|book| book.best_bid().is_none())
            .count();

        debug!(
            requested_books = token_ids.len(),
            live_books = token_ids.len().saturating_sub(rest_count),
            rest_books = if allow_rest_fallback { rest_count } else { 0 },
            skipped_rest_books = if allow_rest_fallback { 0 } else { rest_count },
            final_books = books.len(),
            final_missing_books = final_missing_ids.len(),
            empty_ask_books = empty_ask_count,
            empty_bid_books = empty_bid_count,
            "orderbook loaded in live-first mode"
        );
        if !final_missing_ids.is_empty() && allow_rest_fallback && books.is_empty() {
            debug!(
                requested_books = token_ids.len(),
                final_books = books.len(),
                final_missing_books = final_missing_ids.len(),
                empty_ask_books = empty_ask_count,
                empty_bid_books = empty_bid_count,
                missing_token_sample = final_missing_ids
                    .first()
                    .map_or("-", |token_id| token_id.as_str()),
                "orderbook unavailable after live/REST lookup"
            );
        } else if !final_missing_ids.is_empty() && allow_rest_fallback {
            warn!(
                requested_books = token_ids.len(),
                final_books = books.len(),
                final_missing_books = final_missing_ids.len(),
                empty_ask_books = empty_ask_count,
                empty_bid_books = empty_bid_count,
                missing_token_sample = final_missing_ids
                    .first()
                    .map_or("-", |token_id| token_id.as_str()),
                "orderbook load returned incomplete books"
            );
        } else if !final_missing_ids.is_empty() {
            debug!(
                requested_books = token_ids.len(),
                final_books = books.len(),
                final_missing_books = final_missing_ids.len(),
                empty_ask_books = empty_ask_count,
                empty_bid_books = empty_bid_count,
                missing_token_sample = final_missing_ids
                    .first()
                    .map_or("-", |token_id| token_id.as_str()),
                "live-only orderbook cache is not ready yet"
            );
        } else if empty_ask_count > 0 || empty_bid_count > 0 {
            debug!(
                requested_books = token_ids.len(),
                final_books = books.len(),
                empty_ask_books = empty_ask_count,
                empty_bid_books = empty_bid_count,
                "orderbook load returned books with temporarily empty sides"
            );
        }

        Ok(books)
    }

    /// Fetch historical price points for a single market token.
    ///
    /// # Errors
    ///
    /// Returns an error if the CLOB price-history endpoint fails.
    pub async fn fetch_price_history(
        &self,
        token_id: &str,
        start_ts_ms: i64,
        end_ts_ms: i64,
        interval: &str,
    ) -> Result<Vec<PriceHistoryPoint>> {
        let response = self
            .request_with_retry("prices-history", || {
                self.http
                    .get(format!("{}/prices-history", self.http_config.clob_base_url))
                    .query(&[
                        ("market", token_id.to_owned()),
                        ("startTs", start_ts_ms.to_string()),
                        ("endTs", end_ts_ms.to_string()),
                        ("interval", interval.to_owned()),
                        ("fidelity", "10".to_owned()),
                    ])
            })
            .await?
            .json::<PriceHistoryResponse>()
            .await?;

        Ok(response
            .history
            .into_iter()
            .map(|point| PriceHistoryPoint {
                timestamp_ms: point.timestamp_ms,
                price: point.price,
            })
            .collect())
    }

    /// Fetch recent public trade-flow summaries for the provided market windows.
    ///
    /// # Errors
    ///
    /// Returns an error if the Polymarket Data API request fails.
    pub async fn fetch_trade_flow_summaries(
        &self,
        windows: &[TradeFlowWindow],
    ) -> Result<HashMap<String, TradeFlowSummary>> {
        if windows.is_empty() {
            return Ok(HashMap::new());
        }

        let mut summaries = HashMap::with_capacity(windows.len());
        let mut windows_by_condition = HashMap::with_capacity(windows.len());
        for window in windows {
            summaries.insert(window.slug.clone(), TradeFlowSummary::default());
            windows_by_condition.insert(window.condition_id.clone(), window);
        }

        let condition_ids = windows_by_condition.keys().cloned().collect::<Vec<_>>();
        for batch in condition_ids.chunks(TRADE_FLOW_BATCH_SIZE.max(1)) {
            let market_query = batch.join(",");
            let trades = self.fetch_trade_records(&market_query).await?;

            for trade in trades {
                let Some(condition_id) = trade.condition_id() else {
                    continue;
                };
                let Some(window) = windows_by_condition.get(condition_id) else {
                    continue;
                };
                let timestamp_ms = normalize_timestamp_ms(trade.timestamp_ms);
                if timestamp_ms < window.start_ts_ms || timestamp_ms > window.end_ts_ms {
                    continue;
                }

                let Some((up_pressure, down_pressure)) = trade.pressure_notional() else {
                    continue;
                };

                if let Some(summary) = summaries.get_mut(&window.slug) {
                    summary.up_pressure_notional += up_pressure;
                    summary.down_pressure_notional += down_pressure;
                    summary.total_pressure_notional += up_pressure + down_pressure;
                    summary.trade_count += 1;
                }
            }
        }

        for summary in summaries.values_mut() {
            if summary.total_pressure_notional > Decimal::ZERO {
                summary.signed_up_imbalance_bps = ((summary.up_pressure_notional
                    - summary.down_pressure_notional)
                    / summary.total_pressure_notional
                    * Decimal::from(10_000_u32))
                .round_dp(4);
            }
        }

        Ok(summaries
            .into_iter()
            .filter(|(_slug, summary)| summary.trade_count > 0)
            .collect())
    }

    async fn fetch_trade_records(&self, market_query: &str) -> Result<Vec<TradeRecord>> {
        let mut attempt = 1_u8;

        loop {
            let response = self
                .request_with_retry("trades", || {
                    self.http
                        .get(format!("{}/trades", self.http_config.data_api_base_url))
                        .query(&[
                            ("market", market_query.to_owned()),
                            ("limit", TRADE_FLOW_PAGE_LIMIT.to_string()),
                            ("offset", "0".to_owned()),
                            ("takerOnly", "true".to_owned()),
                        ])
                })
                .await?;

            match response.json::<Vec<TradeRecord>>().await {
                Ok(trades) => return Ok(trades),
                Err(error) if should_retry_http_error(&error) && attempt < HTTP_RETRY_ATTEMPTS => {
                    warn!(attempt, error = %error, "retrying trades response body decode");
                }
                Err(error) => return Err(error.into()),
            }

            tokio::time::sleep(Duration::from_millis(HTTP_RETRY_DELAY_MS)).await;
            attempt += 1;
        }
    }

    /// Fetch trade-flow summaries preferring live WebSocket trades with REST backfill.
    ///
    /// # Errors
    ///
    /// Returns an error if the REST backfill request fails.
    pub async fn fetch_trade_flow_summaries_live_first(
        &self,
        windows: &[TradeFlowWindow],
        backfill_trade_flow: bool,
    ) -> Result<HashMap<String, TradeFlowSummary>> {
        if windows.is_empty() {
            return Ok(HashMap::new());
        }

        let snapshot = self
            .live_stream
            .trade_flow_snapshot(windows, backfill_trade_flow)
            .await;
        let mut merged = snapshot.live_summaries;
        if !snapshot.backfill_windows.is_empty() {
            let rest_summaries = self
                .fetch_trade_flow_summaries(&snapshot.backfill_windows)
                .await?;
            for (slug, summary) in rest_summaries {
                match merged.get_mut(&slug) {
                    Some(existing) => {
                        existing.up_pressure_notional += summary.up_pressure_notional;
                        existing.down_pressure_notional += summary.down_pressure_notional;
                        existing.total_pressure_notional += summary.total_pressure_notional;
                        existing.trade_count += summary.trade_count;
                        if existing.total_pressure_notional > Decimal::ZERO {
                            existing.signed_up_imbalance_bps = ((existing.up_pressure_notional
                                - existing.down_pressure_notional)
                                / existing.total_pressure_notional
                                * Decimal::from(10_000_u32))
                            .round_dp(4);
                        }
                    }
                    None => {
                        merged.insert(slug, summary);
                    }
                }
            }
        }

        debug!(
            requested_windows = windows.len(),
            live_windows = merged.len(),
            backfill_windows = snapshot.backfill_windows.len(),
            "trade-flow loaded in live-first mode"
        );

        Ok(merged)
    }

    /// Fetch recent account activity from Polymarket Data API.
    ///
    /// # Errors
    ///
    /// Returns an error if the activity endpoint request fails.
    pub async fn fetch_profile_activity(
        &self,
        wallet: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<ProfileActivityRecord>> {
        let query = [
            ("user".to_owned(), wallet.to_owned()),
            ("limit".to_owned(), limit.max(1).to_string()),
            ("offset".to_owned(), offset.to_string()),
        ];

        self.request_with_retry("activity", || {
            self.http
                .get(format!("{}/activity", self.http_config.data_api_base_url))
                .query(&query)
        })
        .await?
        .json::<Vec<ProfileActivityRecord>>()
        .await
        .map_err(Into::into)
    }

    /// Check geographic eligibility before live order placement.
    ///
    /// # Errors
    ///
    /// Returns an error if the geoblock endpoint request fails.
    pub async fn geoblock_status(&self) -> Result<GeoblockResponse> {
        self.http
            .get(&self.http_config.geoblock_url)
            .send()
            .await?
            .error_for_status()?
            .json::<GeoblockResponse>()
            .await
            .map_err(Into::into)
    }

    async fn fetch_market_by_slug_with_options(
        &self,
        slug: &str,
        require_active: bool,
    ) -> Result<Option<BinaryMarket>> {
        let cache_key = market_slug_cache_key(slug, require_active);
        let now_ms = Utc::now().timestamp_millis();
        if let Some(cached_market) = {
            let cache = self.market_by_slug_cache.read().await;
            cache.get(&cache_key).and_then(|entry| {
                let age_ms = now_ms.saturating_sub(entry.cached_at_ms);
                (age_ms <= MARKET_BY_SLUG_CACHE_TTL_MS).then(|| entry.market.clone())
            })
        } {
            return Ok(Some(cached_market));
        }

        let market = match self
            .request_with_retry("market-by-slug", || {
                self.http.get(format!(
                    "{}/markets/slug/{}",
                    self.http_config.gamma_base_url, slug
                ))
            })
            .await
        {
            Ok(response) => response.json::<GammaMarket>().await?,
            Err(AppError::HttpStatus {
                code: StatusCode::NOT_FOUND,
                ..
            }) => return Ok(None),
            Err(error) => return Err(error),
        };

        let normalized = if require_active {
            market.into_binary_market()?
        } else {
            market.into_binary_market_any_state()?
        };
        let Some(normalized_market) = normalized.filter(BinaryMarket::is_supported_target_market)
        else {
            return Ok(None);
        };

        {
            let mut cache = self.market_by_slug_cache.write().await;
            cache.insert(
                cache_key,
                CachedMarketBySlug {
                    market: normalized_market.clone(),
                    cached_at_ms: now_ms,
                },
            );
            if cache.len() > MARKET_BY_SLUG_CACHE_MAX_ENTRIES {
                cache.retain(|_, entry| {
                    now_ms.saturating_sub(entry.cached_at_ms) <= MARKET_BY_SLUG_CACHE_TTL_MS
                });
                if cache.len() > MARKET_BY_SLUG_CACHE_MAX_ENTRIES {
                    cache.clear();
                }
            }
        }

        Ok(Some(normalized_market))
    }

    async fn fetch_server_time_secs(&self) -> Result<i64> {
        let raw = self
            .request_with_retry("clob-time", || {
                self.http
                    .get(format!("{}/time", self.http_config.clob_base_url))
            })
            .await?
            .text()
            .await?;

        let trimmed = raw.trim().trim_matches('"');
        trimmed.parse::<i64>().map_err(|error| {
            AppError::InvalidMarket(format!(
                "Не удалось распарсить серверное время Polymarket '{trimmed}': {error}"
            ))
        })
    }

    async fn request_with_retry(
        &self,
        label: &'static str,
        mut build: impl FnMut() -> reqwest::RequestBuilder,
    ) -> Result<reqwest::Response> {
        let mut attempt = 1_u8;

        loop {
            let request = build().send().await;
            match request {
                Ok(response) if response.status() == StatusCode::NOT_FOUND => {
                    return Err(AppError::HttpStatus {
                        code: StatusCode::NOT_FOUND,
                        body: format!("{label} returned 404"),
                    });
                }
                Ok(response) => match response.error_for_status() {
                    Ok(response) => return Ok(response),
                    Err(error)
                        if should_retry_http_error(&error) && attempt < HTTP_RETRY_ATTEMPTS =>
                    {
                        warn!(attempt, error = %error, "retrying HTTP response for {label}");
                    }
                    Err(error) => {
                        let status = error.status().unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
                        let body = error.to_string();
                        return Err(AppError::HttpStatus { code: status, body });
                    }
                },
                Err(error) if should_retry_http_error(&error) && attempt < HTTP_RETRY_ATTEMPTS => {
                    warn!(attempt, error = %error, "retrying HTTP request for {label}");
                }
                Err(error) => return Err(error.into()),
            }

            tokio::time::sleep(Duration::from_millis(HTTP_RETRY_DELAY_MS)).await;
            attempt += 1;
        }
    }
}

#[derive(Debug, Deserialize)]
struct PriceHistoryResponse {
    history: Vec<PriceHistoryPointResponse>,
}

#[derive(Debug, Deserialize)]
struct PriceHistoryPointResponse {
    #[serde(rename = "t")]
    timestamp_ms: i64,
    #[serde(rename = "p", deserialize_with = "decimal_from_any")]
    price: Decimal,
}

#[derive(Debug, Deserialize)]
struct TradeRecord {
    #[serde(default, alias = "market")]
    market_id: Option<String>,
    #[serde(default, alias = "conditionId", alias = "condition_id")]
    condition_id: Option<String>,
    #[serde(default, alias = "side")]
    side: Option<String>,
    #[serde(default, alias = "outcome")]
    outcome: Option<String>,
    #[serde(
        default,
        alias = "size",
        alias = "quantity",
        deserialize_with = "option_decimal_from_any"
    )]
    size: Option<Decimal>,
    #[serde(default, alias = "price", deserialize_with = "option_decimal_from_any")]
    price: Option<Decimal>,
    #[serde(default, alias = "timestamp", alias = "createdAt")]
    timestamp_ms: Option<i64>,
}

impl TradeRecord {
    fn condition_id(&self) -> Option<&str> {
        self.condition_id.as_deref().or(self.market_id.as_deref())
    }

    fn pressure_notional(&self) -> Option<(Decimal, Decimal)> {
        let outcome = self.outcome.as_deref()?;
        let size = self.size?;
        let price = self.price?;
        let notional = (size * price).round_dp(8);
        if notional <= Decimal::ZERO {
            return None;
        }

        let trade_side = self.side.as_deref().unwrap_or("buy");
        let is_buy = wallet_side_is_buy_label(trade_side);
        let is_sell = wallet_side_is_sell_label(trade_side);
        if !is_buy && !is_sell {
            return None;
        }

        if outcome_label_is_up(outcome) {
            if is_buy {
                Some((notional, Decimal::ZERO))
            } else {
                Some((Decimal::ZERO, notional))
            }
        } else if outcome_label_is_down(outcome) {
            if is_buy {
                Some((Decimal::ZERO, notional))
            } else {
                Some((notional, Decimal::ZERO))
            }
        } else {
            None
        }
    }
}

fn dedupe_targets(targets: &[MarketTarget]) -> Vec<MarketTarget> {
    let mut unique = Vec::with_capacity(targets.len());
    for target in targets {
        if !unique.contains(target) {
            unique.push(*target);
        }
    }
    unique
}

fn live_market_cache_key(targets: &[MarketTarget]) -> String {
    let mut unique = dedupe_targets(targets);
    unique.sort_by_key(|target| target.as_key());
    unique
        .into_iter()
        .map(MarketTarget::as_key)
        .collect::<Vec<_>>()
        .join("|")
}

fn live_subscription_slug_is_relevant(slug: &str, now_ts: i64) -> bool {
    let Some((target, start_ts)) = parse_supported_window_slug(slug) else {
        return false;
    };
    let window_secs = target.window_secs();
    let end_ts = start_ts.saturating_add(window_secs);
    let latest_future_start = target
        .window_start_ts_at(now_ts)
        .saturating_add(window_secs.saturating_mul(LIVE_SUBSCRIPTION_FUTURE_WINDOWS));

    now_ts < end_ts.saturating_add(LIVE_SUBSCRIPTION_PAST_GRACE_SECS)
        && start_ts <= latest_future_start
}

fn parse_supported_window_slug(slug: &str) -> Option<(MarketTarget, i64)> {
    let target = MarketTarget::from_slug(slug)?;
    let start_ts = slug
        .strip_prefix(target.slug_prefix())?
        .parse::<i64>()
        .ok()?;
    Some((target, start_ts))
}

fn compare_markets(left: &BinaryMarket, right: &BinaryMarket, now_ts: i64) -> std::cmp::Ordering {
    market_priority_key(left, now_ts)
        .cmp(&market_priority_key(right, now_ts))
        .then_with(|| left.slug.cmp(&right.slug))
}

fn market_priority_key(
    market: &BinaryMarket,
    now_ts: i64,
) -> (u8, i64, Reverse<i64>, &'static str) {
    let Some(target) = market.target() else {
        return (3, i64::MAX, Reverse(i64::MIN), "");
    };
    let Some(start_ts) = market.window_start_ts() else {
        return (3, i64::MAX, Reverse(i64::MIN), target.label());
    };

    let end_ts = start_ts + target.window_secs();
    if start_ts <= now_ts && now_ts < end_ts {
        return (0, now_ts - start_ts, Reverse(start_ts), target.label());
    }

    if now_ts < start_ts {
        return (1, start_ts - now_ts, Reverse(start_ts), target.label());
    }

    (2, now_ts - end_ts, Reverse(start_ts), target.label())
}

fn generate_target_market_slugs(
    target: MarketTarget,
    max_markets: usize,
    now_ts: i64,
) -> Vec<String> {
    let lookup_limit = max_markets
        .saturating_mul(MARKET_LOOKUP_MULTIPLIER)
        .max(max_markets)
        .min(MARKET_LOOKUP_CAP);
    let window_secs = target.window_secs();
    let current_window_start = now_ts - now_ts.rem_euclid(window_secs);

    let mut offsets = Vec::with_capacity(lookup_limit);
    offsets.push(0_i64);
    let mut step = 1_i64;
    while offsets.len() < lookup_limit {
        offsets.push(step);
        if offsets.len() < lookup_limit {
            offsets.push(-step);
        }
        step += 1;
    }

    let slugs = offsets
        .into_iter()
        .map(|offset| {
            format!(
                "{}{}",
                target.slug_prefix(),
                current_window_start + offset * window_secs
            )
        })
        .collect::<Vec<_>>();

    prioritize_target_event_slugs_at(target, slugs, max_markets, now_ts)
}

fn prioritize_target_event_slugs_at(
    target: MarketTarget,
    mut event_slugs: Vec<String>,
    max_markets: usize,
    now_ts: i64,
) -> Vec<String> {
    event_slugs.retain(|slug| slug.starts_with(target.slug_prefix()));
    event_slugs.sort_by(|left, right| {
        target_slug_priority(target, left, now_ts)
            .cmp(&target_slug_priority(target, right, now_ts))
            .then_with(|| left.cmp(right))
    });
    event_slugs.dedup();

    let lookup_limit = max_markets
        .saturating_mul(MARKET_LOOKUP_MULTIPLIER)
        .max(max_markets)
        .min(MARKET_LOOKUP_CAP);
    event_slugs.truncate(lookup_limit.min(event_slugs.len()));
    event_slugs
}

fn target_slug_priority(target: MarketTarget, slug: &str, now_ts: i64) -> (u8, i64, Reverse<i64>) {
    let Some(start_ts) = target_start_ts_from_slug(target, slug) else {
        return (3, i64::MAX, Reverse(i64::MIN));
    };

    let end_ts = start_ts + target.window_secs();
    if start_ts <= now_ts && now_ts < end_ts {
        return (0, now_ts - start_ts, Reverse(start_ts));
    }

    if now_ts < start_ts {
        return (1, start_ts - now_ts, Reverse(start_ts));
    }

    (2, now_ts - end_ts, Reverse(start_ts))
}

fn target_start_ts_from_slug(target: MarketTarget, slug: &str) -> Option<i64> {
    slug.strip_prefix(target.slug_prefix())?.parse::<i64>().ok()
}

fn should_retry_http_error(error: &reqwest::Error) -> bool {
    error.is_timeout()
        || error.is_connect()
        || matches!(
            error.status(),
            Some(status)
                if status.is_server_error()
                    || status == StatusCode::TOO_MANY_REQUESTS
        )
}

fn project_cached_server_time_secs(cached: CachedServerTime, local_now_ms: i64) -> i64 {
    let elapsed_secs = local_now_ms.saturating_sub(cached.cached_at_ms) / 1_000;
    cached.server_ts.saturating_add(elapsed_secs)
}

fn normalize_timestamp_ms(timestamp_ms: Option<i64>) -> i64 {
    match timestamp_ms {
        Some(value) if value > 10_000_000_000 => value,
        Some(value) => value.saturating_mul(1000),
        None => 0,
    }
}

fn market_slug_cache_key(slug: &str, require_active: bool) -> String {
    format!("{slug}:{require_active}")
}

type MarketSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;
type MarketSocketWriter = SplitSink<MarketSocket, Message>;

async fn run_polymarket_market_stream_loop(
    state: Arc<RwLock<PolymarketLiveState>>,
    mut revision_rx: watch::Receiver<u64>,
    market_update_tx: watch::Sender<u64>,
    ws_url: String,
) {
    loop {
        let assets = {
            let state = state.read().await;
            state.desired_assets.iter().cloned().collect::<Vec<_>>()
        };

        if assets.is_empty() {
            if revision_rx.changed().await.is_err() {
                return;
            }
            continue;
        }

        let ws_connection = connect_async(&ws_url).await;
        let (socket, _) = match ws_connection {
            Ok(connection) => connection,
            Err(error) => {
                warn!(error = %error, "failed to connect Polymarket market websocket");
                sleep(Duration::from_millis(
                    POLYMARKET_MARKET_WS_RECONNECT_DELAY_MS,
                ))
                .await;
                continue;
            }
        };
        info!(
            assets = assets.len(),
            endpoint = %ws_url,
            "connected Polymarket market websocket"
        );

        let (mut writer, mut reader) = socket.split();
        if let Err(error) = subscribe_market_assets(&mut writer, &assets).await {
            warn!(error = %error, "failed to subscribe to Polymarket market websocket");
            sleep(Duration::from_millis(
                POLYMARKET_MARKET_WS_RECONNECT_DELAY_MS,
            ))
            .await;
            continue;
        }

        loop {
            tokio::select! {
                changed = revision_rx.changed() => {
                    if changed.is_err() {
                        return;
                    }

                    let desired_assets = {
                        let state = state.read().await;
                        state.desired_assets.iter().cloned().collect::<Vec<_>>()
                    };

                    if desired_assets != assets {
                        info!(
                            old_assets = assets.len(),
                            new_assets = desired_assets.len(),
                            "Polymarket market websocket subscriptions changed; reconnecting stream"
                        );
                        break;
                    }
                }
                message = reader.next() => {
                    let Some(message) = message else {
                        warn!("Polymarket market websocket closed by peer");
                        break;
                    };

                    match message {
                        Ok(Message::Text(payload)) => {
                            process_market_ws_payload(
                                &state,
                                &market_update_tx,
                                payload.as_ref(),
                            ).await;
                        }
                        Ok(Message::Binary(payload)) => {
                            if let Ok(text) = std::str::from_utf8(&payload) {
                                process_market_ws_payload(&state, &market_update_tx, text).await;
                            }
                        }
                        Ok(Message::Ping(payload)) => {
                            if writer.send(Message::Pong(payload)).await.is_err() {
                                break;
                            }
                        }
                        Ok(Message::Close(_)) => break,
                        Ok(_) => {}
                        Err(error) => {
                            warn!(error = %error, "error while reading Polymarket market websocket frame");
                            break;
                        }
                    }
                }
            }
        }

        sleep(Duration::from_millis(
            POLYMARKET_MARKET_WS_RECONNECT_DELAY_MS,
        ))
        .await;
    }
}

async fn subscribe_market_assets(writer: &mut MarketSocketWriter, assets: &[String]) -> Result<()> {
    let payload = serde_json::json!({
        "assets_ids": assets,
        "type": "market",
        "custom_feature_enabled": true
    });
    writer
        .send(Message::Text(payload.to_string().into()))
        .await
        .map_err(|error| {
            AppError::LiveExecution(format!("market websocket subscription failed: {error}"))
        })
}

async fn process_market_ws_payload(
    state: &Arc<RwLock<PolymarketLiveState>>,
    market_update_tx: &watch::Sender<u64>,
    payload: &str,
) {
    let Ok(value) = serde_json::from_str::<MarketWsPayload>(payload) else {
        return;
    };

    match value {
        MarketWsPayload::Batch(events) => {
            for event in events {
                process_market_ws_event(state, market_update_tx, event).await;
            }
        }
        MarketWsPayload::Single(event) => {
            process_market_ws_event(state, market_update_tx, event).await;
        }
    }
}

async fn process_market_ws_event(
    state: &Arc<RwLock<PolymarketLiveState>>,
    market_update_tx: &watch::Sender<u64>,
    event: MarketWsEvent,
) {
    let updated = match event {
        MarketWsEvent::Book(event) => apply_book_event(state, event).await,
        MarketWsEvent::PriceChange(event) => apply_price_change_event(state, event).await,
        MarketWsEvent::LastTrade(event) => apply_last_trade_event(state, event).await,
        MarketWsEvent::TickSize(event) => apply_tick_size_event(state, event).await,
        MarketWsEvent::Other => false,
    };

    if updated {
        market_update_tx.send_modify(|revision| *revision = revision.saturating_add(1));
    }
}

async fn apply_book_event(
    state: &Arc<RwLock<PolymarketLiveState>>,
    event: MarketWsBookEvent,
) -> bool {
    if event.asset_id.is_empty() {
        return false;
    }

    let timestamp_ms = normalize_timestamp_ms(event.timestamp_ms);
    let mut book = OrderBook {
        asset_id: event.asset_id.clone(),
        bids: event.bids,
        asks: event.asks,
        min_order_size: None,
        tick_size: None,
    };
    normalize_book_sides(&mut book);
    let mut state = state.write().await;
    state.books.insert(
        event.asset_id,
        CachedOrderBook {
            book,
            updated_at_ms: timestamp_ms.max(Utc::now().timestamp_millis()),
        },
    );
    true
}

async fn apply_price_change_event(
    state: &Arc<RwLock<PolymarketLiveState>>,
    event: MarketWsPriceChangeEvent,
) -> bool {
    let timestamp_ms =
        normalize_timestamp_ms(event.timestamp_ms).max(Utc::now().timestamp_millis());
    let mut state = state.write().await;
    let mut updated = false;

    for change in event.price_changes {
        if change.asset_id.is_empty() {
            continue;
        }

        let cached = state
            .books
            .entry(change.asset_id.clone())
            .or_insert_with(|| CachedOrderBook {
                book: OrderBook {
                    asset_id: change.asset_id.clone(),
                    bids: Vec::new(),
                    asks: Vec::new(),
                    min_order_size: None,
                    tick_size: None,
                },
                updated_at_ms: timestamp_ms,
            });

        let side = change.side.unwrap_or_default();
        if wallet_side_is_buy_label(&side) {
            apply_price_level_change(&mut cached.book.bids, change.price, change.size);
        } else if wallet_side_is_sell_label(&side) {
            apply_price_level_change(&mut cached.book.asks, change.price, change.size);
        } else {
            continue;
        }
        normalize_book_sides(&mut cached.book);
        cached.updated_at_ms = timestamp_ms;
        updated = true;
    }

    updated
}

async fn apply_tick_size_event(
    state: &Arc<RwLock<PolymarketLiveState>>,
    event: MarketWsTickSizeEvent,
) -> bool {
    if event.asset_id.is_empty() {
        return false;
    }

    let timestamp_ms =
        normalize_timestamp_ms(event.timestamp_ms).max(Utc::now().timestamp_millis());
    let mut state = state.write().await;
    let cached = state
        .books
        .entry(event.asset_id.clone())
        .or_insert_with(|| CachedOrderBook {
            book: OrderBook {
                asset_id: event.asset_id.clone(),
                bids: Vec::new(),
                asks: Vec::new(),
                min_order_size: None,
                tick_size: event.new_tick_size,
            },
            updated_at_ms: timestamp_ms,
        });
    cached.book.tick_size = event.new_tick_size;
    cached.updated_at_ms = timestamp_ms;
    true
}

async fn apply_last_trade_event(
    state: &Arc<RwLock<PolymarketLiveState>>,
    event: MarketWsLastTradeEvent,
) -> bool {
    if event.asset_id.is_empty() {
        return false;
    }

    let notional = (event.price * event.size).round_dp(8);
    if notional <= Decimal::ZERO {
        return false;
    }

    let timestamp_ms =
        normalize_timestamp_ms(event.timestamp_ms).max(Utc::now().timestamp_millis());
    let mut state = state.write().await;
    let Some(meta) = state.asset_meta.get(&event.asset_id).cloned() else {
        return false;
    };
    if !event.market.is_empty() && meta.condition_id != event.market {
        return false;
    }

    let side = event.side.as_deref().unwrap_or("buy");
    let is_buy = wallet_side_is_buy_label(side);
    if !is_buy && !wallet_side_is_sell_label(side) {
        return false;
    }
    let (up_pressure_notional, down_pressure_notional) = match (meta.is_up_outcome, is_buy) {
        (true, true) | (false, false) => (notional, Decimal::ZERO),
        (true, false) | (false, true) => (Decimal::ZERO, notional),
    };

    let queue = state
        .trade_events_by_slug
        .entry(meta.slug)
        .or_insert_with(|| VecDeque::with_capacity(256));
    queue.push_back(LiveTradeEvent {
        timestamp_ms,
        up_pressure_notional,
        down_pressure_notional,
    });

    let oldest_allowed = timestamp_ms.saturating_sub(LIVE_TRADE_RETENTION_MS);
    while queue
        .front()
        .is_some_and(|entry| entry.timestamp_ms < oldest_allowed)
    {
        queue.pop_front();
    }
    true
}

fn apply_price_level_change(levels: &mut Vec<BookLevel>, price: Decimal, size: Decimal) {
    if size <= Decimal::ZERO {
        levels.retain(|level| level.price != price);
        return;
    }

    if let Some(level) = levels.iter_mut().find(|level| level.price == price) {
        level.size = size;
    } else {
        levels.push(BookLevel { price, size });
    }
}

fn normalize_book_sides(book: &mut OrderBook) {
    book.bids
        .sort_by(|left, right| left.price.cmp(&right.price));
    book.asks
        .sort_by(|left, right| right.price.cmp(&left.price));
}

fn option_i64_from_any<'de, D>(deserializer: D) -> std::result::Result<Option<i64>, D::Error>
where
    D: Deserializer<'de>,
{
    match serde_json::Value::deserialize(deserializer)? {
        serde_json::Value::Null => Ok(None),
        serde_json::Value::String(value) => value
            .parse::<i64>()
            .map(Some)
            .map_err(|_| de::Error::custom("invalid i64 string")),
        serde_json::Value::Number(value) => value
            .as_i64()
            .map(Some)
            .ok_or_else(|| de::Error::custom("invalid i64 number")),
        _ => Err(de::Error::custom("invalid i64 value")),
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum MarketWsPayload {
    Batch(Vec<MarketWsEvent>),
    Single(MarketWsEvent),
}

#[derive(Debug, Deserialize)]
#[serde(tag = "event_type")]
enum MarketWsEvent {
    #[serde(rename = "book")]
    Book(MarketWsBookEvent),
    #[serde(rename = "price_change")]
    PriceChange(MarketWsPriceChangeEvent),
    #[serde(rename = "last_trade_price")]
    LastTrade(MarketWsLastTradeEvent),
    #[serde(rename = "tick_size_change")]
    TickSize(MarketWsTickSizeEvent),
    #[serde(other)]
    Other,
}

#[derive(Debug, Deserialize)]
struct MarketWsBookEvent {
    #[serde(default)]
    asset_id: String,
    #[serde(default)]
    bids: Vec<BookLevel>,
    #[serde(default)]
    asks: Vec<BookLevel>,
    #[serde(default, deserialize_with = "option_i64_from_any")]
    timestamp_ms: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct MarketWsPriceChangeEvent {
    #[serde(default)]
    price_changes: Vec<MarketWsPriceChange>,
    #[serde(default, deserialize_with = "option_i64_from_any")]
    timestamp_ms: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct MarketWsPriceChange {
    #[serde(default)]
    asset_id: String,
    #[serde(deserialize_with = "decimal_from_any")]
    price: Decimal,
    #[serde(deserialize_with = "decimal_from_any")]
    size: Decimal,
    #[serde(default)]
    side: Option<String>,
}

#[derive(Debug, Deserialize)]
struct MarketWsLastTradeEvent {
    #[serde(default)]
    market: String,
    #[serde(default)]
    asset_id: String,
    #[serde(default)]
    side: Option<String>,
    #[serde(deserialize_with = "decimal_from_any")]
    price: Decimal,
    #[serde(deserialize_with = "decimal_from_any")]
    size: Decimal,
    #[serde(default, deserialize_with = "option_i64_from_any")]
    timestamp_ms: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct MarketWsTickSizeEvent {
    #[serde(default)]
    asset_id: String,
    #[serde(default, deserialize_with = "option_decimal_from_any")]
    new_tick_size: Option<Decimal>,
    #[serde(default, deserialize_with = "option_i64_from_any")]
    timestamp_ms: Option<i64>,
}

fn decimal_from_any<'de, D>(deserializer: D) -> std::result::Result<Decimal, D::Error>
where
    D: Deserializer<'de>,
{
    match serde_json::Value::deserialize(deserializer)? {
        serde_json::Value::String(value) => value
            .parse::<Decimal>()
            .map_err(|_| de::Error::custom("invalid decimal string")),
        serde_json::Value::Number(value) => value
            .to_string()
            .parse::<Decimal>()
            .map_err(|_| de::Error::custom("invalid decimal number")),
        _ => Err(de::Error::custom("invalid decimal value")),
    }
}

fn option_decimal_from_any<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<Decimal>, D::Error>
where
    D: Deserializer<'de>,
{
    match serde_json::Value::deserialize(deserializer)? {
        serde_json::Value::Null => Ok(None),
        serde_json::Value::String(value) => value
            .parse::<Decimal>()
            .map(Some)
            .map_err(|_| de::Error::custom("invalid decimal string")),
        serde_json::Value::Number(value) => value
            .to_string()
            .parse::<Decimal>()
            .map(Some)
            .map_err(|_| de::Error::custom("invalid decimal number")),
        _ => Err(de::Error::custom("invalid decimal value")),
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use crate::models::{BookLevel, MarketTarget, OrderBook};

    use super::{
        CachedServerTime, ProfileActivityRecord, TradeFlowSummary, TradeRecord,
        generate_target_market_slugs, live_subscription_slug_is_relevant, normalize_book_sides,
        parse_supported_window_slug, prioritize_target_event_slugs_at,
        project_cached_server_time_secs,
    };

    #[test]
    fn prioritize_target_event_slugs_prefers_live_and_nearby_windows() {
        let sorted = prioritize_target_event_slugs_at(
            MarketTarget::Btc15m,
            vec![
                "btc-updown-15m-1775077200".to_owned(),
                "btc-updown-15m-1775078100".to_owned(),
                "btc-updown-15m-1775079000".to_owned(),
                "eth-updown-15m-1775078100".to_owned(),
                "btc-updown-15m-1775076300".to_owned(),
            ],
            2,
            1_775_078_450,
        );

        assert_eq!(
            sorted,
            vec![
                "btc-updown-15m-1775078100".to_owned(),
                "btc-updown-15m-1775079000".to_owned(),
                "btc-updown-15m-1775077200".to_owned(),
                "btc-updown-15m-1775076300".to_owned(),
            ]
        );
    }

    #[test]
    fn generate_target_market_slugs_starts_from_current_window() {
        let slugs = generate_target_market_slugs(MarketTarget::Eth5m, 3, 1_775_079_450);

        assert_eq!(
            slugs,
            vec![
                "eth-updown-5m-1775079300".to_owned(),
                "eth-updown-5m-1775079600".to_owned(),
                "eth-updown-5m-1775079900".to_owned(),
                "eth-updown-5m-1775080200".to_owned(),
                "eth-updown-5m-1775080500".to_owned(),
                "eth-updown-5m-1775079000".to_owned(),
                "eth-updown-5m-1775078700".to_owned(),
                "eth-updown-5m-1775078400".to_owned(),
                "eth-updown-5m-1775078100".to_owned(),
            ]
        );

        let sol_slugs = generate_target_market_slugs(MarketTarget::Sol5m, 2, 1_775_079_450);
        assert!(
            sol_slugs
                .iter()
                .all(|slug| slug.starts_with("sol-updown-5m-")),
            "SOL lookup should use Polymarket's sol-updown-5m slug family"
        );
        let xrp_slugs = generate_target_market_slugs(MarketTarget::Xrp5m, 2, 1_775_079_450);
        assert!(
            xrp_slugs
                .iter()
                .all(|slug| slug.starts_with("xrp-updown-5m-")),
            "XRP lookup should use Polymarket's xrp-updown-5m slug family"
        );
        let bnb_slugs = generate_target_market_slugs(MarketTarget::Bnb5m, 2, 1_775_079_450);
        assert!(
            bnb_slugs
                .iter()
                .all(|slug| slug.starts_with("bnb-updown-5m-")),
            "BNB lookup should use Polymarket's bnb-updown-5m slug family"
        );
    }

    #[test]
    fn cached_server_time_advances_with_local_elapsed_time() {
        let cached = CachedServerTime {
            server_ts: 1_775_079_450,
            cached_at_ms: 10_000,
        };

        assert_eq!(
            project_cached_server_time_secs(cached, 10_999),
            1_775_079_450
        );
        assert_eq!(
            project_cached_server_time_secs(cached, 12_250),
            1_775_079_452
        );
    }

    #[test]
    fn live_subscription_slug_relevance_keeps_current_and_next_windows() {
        let now_ts = 1_779_471_123;
        let current_start = MarketTarget::Btc5m.window_start_ts_at(now_ts);
        let current = MarketTarget::Btc5m.slug_for_window_start(current_start);
        let next = MarketTarget::Btc5m.slug_for_window_start(current_start + 300);
        let old = MarketTarget::Btc5m.slug_for_window_start(current_start - 600);
        let far_future = MarketTarget::Btc5m.slug_for_window_start(current_start + 900);

        assert!(live_subscription_slug_is_relevant(&current, now_ts));
        assert!(live_subscription_slug_is_relevant(&next, now_ts));
        assert!(!live_subscription_slug_is_relevant(&old, now_ts));
        assert!(!live_subscription_slug_is_relevant(&far_future, now_ts));
    }

    #[test]
    fn parse_supported_window_slug_parses_target_and_start() {
        assert_eq!(
            parse_supported_window_slug("btc-updown-5m-1779471000"),
            Some((MarketTarget::Btc5m, 1_779_471_000))
        );
        assert_eq!(parse_supported_window_slug("unknown-1779471000"), None);
    }

    #[test]
    fn deserialize_profile_activity_record_from_data_api_payload() {
        let payload = r#"{
            "proxyWallet": "0xeebde7a0e019a63e6b476eb425505b7b3e6eba30",
            "timestamp": 1775232713,
            "slug": "btc-updown-5m-1775232600",
            "outcome": "Up",
            "side": "BUY",
            "price": 0.7175,
            "usdcSize": 14.35,
            "transactionHash": "0xabc"
        }"#;

        let record = serde_json::from_str::<ProfileActivityRecord>(payload)
            .expect("valid profile activity payload");

        assert!(record.is_btc_5m());
        assert_eq!(record.side, "BUY");
        assert_eq!(record.outcome, "Up");
        assert_eq!(record.price, Some(Decimal::new(7175, 4)));
        assert_eq!(record.usdc_size, Some(Decimal::new(1435, 2)));
        assert_eq!(record.dedupe_key(), "0xabc");
    }

    #[test]
    fn trade_flow_pressure_accepts_russian_labels() {
        let up_buy = TradeRecord {
            market_id: Some("condition".to_owned()),
            condition_id: None,
            side: Some("Покупка".to_owned()),
            outcome: Some("Рост".to_owned()),
            size: Some(Decimal::from(10_u32)),
            price: Some(Decimal::new(40, 2)),
            timestamp_ms: Some(1),
        };
        let down_sell = TradeRecord {
            market_id: Some("condition".to_owned()),
            condition_id: None,
            side: Some("Продажа".to_owned()),
            outcome: Some("Падение".to_owned()),
            size: Some(Decimal::from(10_u32)),
            price: Some(Decimal::new(40, 2)),
            timestamp_ms: Some(1),
        };

        assert_eq!(
            up_buy.pressure_notional(),
            Some((Decimal::from(4_u32), Decimal::ZERO))
        );
        assert_eq!(
            down_sell.pressure_notional(),
            Some((Decimal::from(4_u32), Decimal::ZERO))
        );
        assert_eq!(
            TradeFlowSummary {
                signed_up_imbalance_bps: Decimal::from(25_u32),
                ..TradeFlowSummary::default()
            }
            .aligned_imbalance_bps("Рост"),
            Decimal::from(25_u32)
        );
        assert_eq!(
            TradeFlowSummary {
                signed_up_imbalance_bps: Decimal::from(25_u32),
                ..TradeFlowSummary::default()
            }
            .aligned_imbalance_bps("unknown"),
            Decimal::ZERO
        );
    }

    #[test]
    fn normalize_book_sides_keeps_best_prices_at_tail() {
        let mut book = OrderBook {
            asset_id: "token".to_owned(),
            bids: vec![
                BookLevel {
                    price: Decimal::new(55, 2),
                    size: Decimal::ONE,
                },
                BookLevel {
                    price: Decimal::new(65, 2),
                    size: Decimal::ONE,
                },
                BookLevel {
                    price: Decimal::new(60, 2),
                    size: Decimal::ONE,
                },
            ],
            asks: vec![
                BookLevel {
                    price: Decimal::new(34, 2),
                    size: Decimal::ONE,
                },
                BookLevel {
                    price: Decimal::new(16, 2),
                    size: Decimal::ONE,
                },
                BookLevel {
                    price: Decimal::new(25, 2),
                    size: Decimal::ONE,
                },
            ],
            min_order_size: None,
            tick_size: None,
        };

        normalize_book_sides(&mut book);

        assert_eq!(
            book.best_bid().map(|level| level.price),
            Some(Decimal::new(65, 2))
        );
        assert_eq!(
            book.best_ask().map(|level| level.price),
            Some(Decimal::new(16, 2))
        );
    }
}
