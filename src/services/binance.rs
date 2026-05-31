//! Binance market-data client for supported fast window markets.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use futures_util::StreamExt;
use reqwest::Client;
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::sync::{RwLock, watch};
use tokio::time::sleep;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, info, warn};

use crate::config::ChainlinkOracleConfig;
use crate::error::{AppError, Result};
use crate::models::{BinaryMarket, MarketTarget, TargetPriceSource};

use super::chainlink::ChainlinkOracleCache;

const MAX_CACHED_QUOTE_AGE_MS: i64 = 10_000;
const MICRO_BURST_PRICE_WINDOW_MS: i64 = 1_000;
const MICRO_PRICE_WINDOW_MS: i64 = 5_000;
const MOMENTUM_PRICE_WINDOW_MS: i64 = 15_000;
const QUOTE_HISTORY_RETENTION_MS: i64 = 30_000;
const STREAM_RECONNECT_DELAY_MS: u64 = 300;
const LIVE_SNAPSHOT_MAX_QUOTE_LAG_SECS: i64 = 5;
const SETTLEMENT_CACHE_CLOSE_LOOKBACK_MS: i64 = 5_000;
const SETTLEMENT_CACHE_CLOSE_LOOKAHEAD_MS: i64 = 1_000;
const INTERVAL_OPEN_CACHE_GRACE_SECS: i64 = 15 * 60;
const INTERVAL_OPEN_CACHE_MAX_ENTRIES: usize = 2_048;
const INTERVAL_OPEN_CACHE_TARGETS: [MarketTarget; 7] = [
    MarketTarget::Btc5m,
    MarketTarget::Btc15m,
    MarketTarget::Eth5m,
    MarketTarget::Eth15m,
    MarketTarget::Sol5m,
    MarketTarget::Xrp5m,
    MarketTarget::Bnb5m,
];
const HISTORICAL_KLINE_CACHE_MAX_ENTRIES: usize = 4_096;

/// Binance-derived context for a supported Polymarket window market.
#[derive(Debug, Clone)]
pub struct MarketWindowContext {
    pub target: MarketTarget,
    pub interval_open_price: Decimal,
    pub target_price: Decimal,
    pub target_price_source: TargetPriceSource,
    pub target_gap_bps: Decimal,
    pub current_spot_price: Decimal,
    pub current_spot_source: String,
    pub current_spot_event_age_ms: Option<i64>,
    pub current_spot_received_age_ms: Option<i64>,
    pub current_spot_quote_points: Option<usize>,
    pub exchange_book_age_ms: Option<i64>,
    pub exchange_book_top_imbalance_bps: Decimal,
    pub exchange_book_depth_imbalance_bps: Decimal,
    pub exchange_book_microprice_bps: Decimal,
    pub exchange_book_spread_bps: Decimal,
    pub micro_burst_reference_price: Decimal,
    pub micro_reference_price: Decimal,
    pub spot_move_bps: Decimal,
    pub spot_move_1s_bps: Decimal,
    pub spot_move_5s_bps: Decimal,
    pub spot_move_15s_bps: Decimal,
    pub micro_acceleration_bps: Decimal,
    pub dominant_outcome: String,
    pub seconds_left: i64,
}

/// Backward-compatible alias for older code paths.
pub type BtcFiveMinuteContext = MarketWindowContext;

/// Realized direction of a finished market window.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum WindowDirection {
    Up,
    Down,
    Flat,
}

impl WindowDirection {
    /// Return the display label used in logs and analytics.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Up => "Up",
            Self::Down => "Down",
            Self::Flat => "Flat",
        }
    }
}

/// Historical resolution for a supported window market.
#[derive(Debug, Clone)]
pub struct MarketWindowResolution {
    pub target: MarketTarget,
    pub start_price: Decimal,
    pub end_price: Decimal,
    pub realized_move_bps: Decimal,
    pub actual_outcome: WindowDirection,
    pub resolved_at_ms: i64,
}

/// Backward-compatible alias for older code paths.
pub type BtcFiveMinuteResolution = MarketWindowResolution;

#[derive(Debug, Clone, Copy)]
struct LiveSpotQuote {
    event_time_ms: i64,
    received_time_ms: i64,
    price: Decimal,
    source: SpotQuoteSource,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum SpotQuoteSource {
    BinanceTrade,
    CoinbaseTicker,
}

impl SpotQuoteSource {
    const fn as_str(self) -> &'static str {
        match self {
            Self::BinanceTrade => "Binance::Trade",
            Self::CoinbaseTicker => "Coinbase::Ticker",
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct LiveSecondKline {
    open_time_ms: i64,
    close_time_ms: i64,
    open: Decimal,
}

#[derive(Debug, Clone, Copy)]
struct LiveBookLevel {
    price: Decimal,
    size: Decimal,
}

#[derive(Debug, Clone)]
struct LiveBookDepthSnapshot {
    received_time_ms: i64,
    bids: Vec<LiveBookLevel>,
    asks: Vec<LiveBookLevel>,
}

#[derive(Debug, Clone, Copy, Default)]
struct ExchangeBookPressureSnapshot {
    received_time_ms: i64,
    top_imbalance_bps: Decimal,
    depth_imbalance_bps: Decimal,
    microprice_bps: Decimal,
    spread_bps: Decimal,
}

#[derive(Debug, Clone, Copy)]
struct CachedIntervalOpenPrice {
    price: Decimal,
    expires_at_ts: i64,
}

#[derive(Debug, Clone)]
struct OneMinuteKline {
    open_time_ms: i64,
    open: Decimal,
    close: Decimal,
    close_time_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct HistoricalKlineCacheKey {
    symbol: String,
    interval: String,
    start_time_ms: i64,
    limit: usize,
}

impl HistoricalKlineCacheKey {
    fn new(symbol: &str, interval: &str, start_time_ms: i64, limit: usize) -> Self {
        Self {
            symbol: normalize_symbol(symbol),
            interval: interval.to_owned(),
            start_time_ms,
            limit,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct HistoricalMicroPriceSnapshot {
    current: Decimal,
    burst_reference: Decimal,
    micro_reference: Decimal,
    momentum_reference: Decimal,
}

#[derive(Debug, Clone, Copy)]
struct LivePriceSnapshot {
    current_spot_price: Decimal,
    current_spot_source: Option<SpotQuoteSource>,
    latest_quote_event_time_ms: Option<i64>,
    latest_quote_received_time_ms: Option<i64>,
    quote_points: usize,
    exchange_book_pressure: Option<ExchangeBookPressureSnapshot>,
    micro_burst_reference_price: Decimal,
    micro_reference_price: Decimal,
    spot_move_1s_bps: Decimal,
    spot_move_5s_bps: Decimal,
    spot_move_15s_bps: Decimal,
    micro_acceleration_bps: Decimal,
}

impl LivePriceSnapshot {
    const fn current_spot_source_label(self) -> &'static str {
        match self.current_spot_source {
            Some(source) => source.as_str(),
            None => "Binance::RestLatest",
        }
    }

    fn current_spot_event_age_ms(self, now_ms: i64) -> Option<i64> {
        self.latest_quote_event_time_ms
            .map(|event_time_ms| now_ms.saturating_sub(event_time_ms))
    }

    fn current_spot_received_age_ms(self, now_ms: i64) -> Option<i64> {
        self.latest_quote_received_time_ms
            .map(|received_time_ms| now_ms.saturating_sub(received_time_ms))
    }

    fn exchange_book_age_ms(self, now_ms: i64) -> Option<i64> {
        self.exchange_book_pressure
            .map(|snapshot| now_ms.saturating_sub(snapshot.received_time_ms))
    }
}

#[derive(Debug, Clone, Copy)]
struct LiveSignalState {
    latest_quote_time_ms: i64,
    updated_at_ms: i64,
    snapshot: LivePriceSnapshot,
}

#[derive(Debug, Clone, Copy, Default)]
struct LiveQuoteReferenceSnapshot {
    latest_quote: Option<LiveSpotQuote>,
    micro_burst_reference_price: Option<Decimal>,
    micro_reference_price: Option<Decimal>,
    momentum_reference_price: Option<Decimal>,
    quote_points: usize,
}

/// Lightweight reactive signal emitted when Binance updates the live market stream.
#[derive(Debug, Clone)]
pub struct BinanceTriggerEvent {
    pub symbol: String,
    pub event_time_ms: i64,
    pub received_time_ms: i64,
    pub price: Decimal,
    pub source: BinanceTriggerSource,
}

/// Source of the live Binance trigger event.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum BinanceTriggerSource {
    Trade,
    OneSecondKline,
    CoinbaseTicker,
    CoinbaseLevel2,
    Depth,
}

/// Current readiness of the live exchange streams for one Binance symbol.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct LiveMarketDataHealth {
    pub symbol: String,
    pub quote_source: Option<&'static str>,
    pub quote_event_age_ms: Option<i64>,
    pub quote_received_age_ms: Option<i64>,
    pub quote_points: usize,
    pub depth_age_ms: Option<i64>,
}

/// Latest live price points split by source for one Binance symbol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveSpotPriceView {
    pub symbol: String,
    pub binance_trade: Option<LiveSpotPricePoint>,
    pub coinbase_ticker: Option<LiveSpotPricePoint>,
    pub quote_points: usize,
    pub depth_age_ms: Option<i64>,
}

/// One live spot-price point with clock-lag diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LiveSpotPricePoint {
    pub price: Decimal,
    pub event_age_ms: i64,
    pub received_age_ms: i64,
}

impl LiveMarketDataHealth {
    #[must_use]
    pub fn has_fresh_quote(&self, max_age_ms: i64) -> bool {
        max_age_ms >= 0
            && self
                .quote_received_age_ms
                .is_some_and(|age_ms| age_ms >= 0 && age_ms <= max_age_ms)
    }

    #[must_use]
    pub fn has_fresh_depth(&self, max_age_ms: i64) -> bool {
        max_age_ms >= 0
            && self
                .depth_age_ms
                .is_some_and(|age_ms| age_ms >= 0 && age_ms <= max_age_ms)
    }
}

/// Lightweight Binance client with REST fallback and WebSocket quote cache.
#[derive(Debug, Clone)]
pub struct BinanceClient {
    http: Client,
    base_url: String,
    websocket_base_url: String,
    quote_cache: Arc<RwLock<HashMap<String, VecDeque<LiveSpotQuote>>>>,
    second_kline_cache: Arc<RwLock<HashMap<String, VecDeque<LiveSecondKline>>>>,
    book_depth_cache: Arc<RwLock<HashMap<String, LiveBookDepthSnapshot>>>,
    live_snapshot_cache: Arc<RwLock<HashMap<String, LiveSignalState>>>,
    interval_open_cache: Arc<RwLock<HashMap<String, CachedIntervalOpenPrice>>>,
    historical_kline_cache: Arc<RwLock<HashMap<HistoricalKlineCacheKey, Vec<OneMinuteKline>>>>,
    chainlink_oracle: ChainlinkOracleCache,
    started_symbols: Arc<Mutex<HashSet<String>>>,
    trigger_tx: watch::Sender<Option<BinanceTriggerEvent>>,
}

impl BinanceClient {
    /// Create a new Binance client.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying HTTP client cannot be built.
    pub fn new(base_url: String, websocket_base_url: String, timeout_secs: u64) -> Result<Self> {
        let http = Client::builder()
            .timeout(Duration::from_secs(timeout_secs))
            .user_agent("polymarket_mvp/0.1.0")
            .build()?;
        Ok(Self {
            http,
            base_url,
            websocket_base_url,
            quote_cache: Arc::new(RwLock::new(HashMap::new())),
            second_kline_cache: Arc::new(RwLock::new(HashMap::new())),
            book_depth_cache: Arc::new(RwLock::new(HashMap::new())),
            live_snapshot_cache: Arc::new(RwLock::new(HashMap::new())),
            interval_open_cache: Arc::new(RwLock::new(HashMap::new())),
            historical_kline_cache: Arc::new(RwLock::new(HashMap::new())),
            chainlink_oracle: ChainlinkOracleCache::default(),
            started_symbols: Arc::new(Mutex::new(HashSet::new())),
            trigger_tx: watch::channel(None).0,
        })
    }

    /// Subscribe to reactive Binance trigger events.
    #[must_use]
    pub fn subscribe_triggers(&self) -> watch::Receiver<Option<BinanceTriggerEvent>> {
        self.trigger_tx.subscribe()
    }

    /// Subscribe to reactive Polymarket Chainlink RTDS price events.
    #[must_use]
    pub fn subscribe_chainlink_triggers(
        &self,
    ) -> watch::Receiver<Option<super::chainlink::ChainlinkOracleTriggerEvent>> {
        self.chainlink_oracle.subscribe_triggers()
    }

    /// Start the Polymarket RTDS Chainlink oracle stream.
    #[must_use]
    pub fn start_chainlink_oracle_stream(
        &self,
        websocket_url: String,
        targets: &[MarketTarget],
        settings: ChainlinkOracleConfig,
    ) -> bool {
        self.chainlink_oracle
            .start_stream(websocket_url, targets, settings)
    }

    /// Snapshot live quote/depth readiness for each configured symbol.
    pub async fn live_market_data_health_for_symbols(
        &self,
        symbols: &[&str],
    ) -> Vec<LiveMarketDataHealth> {
        let now_ms = Utc::now().timestamp_millis();
        let quote_cache = self.quote_cache.read().await;
        let book_depth_cache = self.book_depth_cache.read().await;

        symbols
            .iter()
            .map(|symbol| {
                let normalized_symbol = normalize_symbol(symbol);
                let quotes = quote_cache.get(&normalized_symbol);
                let latest_quote = quotes.and_then(|quotes| quotes.back()).copied();
                let depth_snapshot = book_depth_cache.get(&normalized_symbol);

                LiveMarketDataHealth {
                    symbol: normalized_symbol,
                    quote_source: latest_quote.map(|quote| quote.source.as_str()),
                    quote_event_age_ms: latest_quote
                        .map(|quote| now_ms.saturating_sub(quote.event_time_ms)),
                    quote_received_age_ms: latest_quote
                        .map(|quote| now_ms.saturating_sub(quote.received_time_ms)),
                    quote_points: quotes.map_or(0, VecDeque::len),
                    depth_age_ms: depth_snapshot
                        .map(|snapshot| now_ms.saturating_sub(snapshot.received_time_ms)),
                }
            })
            .collect()
    }

    /// Return latest accepted Binance and Coinbase price points for each symbol.
    pub async fn live_spot_price_views(&self, symbols: &[&str]) -> Vec<LiveSpotPriceView> {
        let now_ms = Utc::now().timestamp_millis();
        let quote_cache = self.quote_cache.read().await;
        let book_depth_cache = self.book_depth_cache.read().await;

        symbols
            .iter()
            .map(|symbol| {
                let normalized_symbol = normalize_symbol(symbol);
                let quotes = quote_cache.get(&normalized_symbol);
                let binance_trade = quotes.and_then(|quotes| {
                    quotes
                        .iter()
                        .rev()
                        .find(|quote| matches!(quote.source, SpotQuoteSource::BinanceTrade))
                        .copied()
                });
                let coinbase_ticker = quotes.and_then(|quotes| {
                    quotes
                        .iter()
                        .rev()
                        .find(|quote| matches!(quote.source, SpotQuoteSource::CoinbaseTicker))
                        .copied()
                });
                let depth_snapshot = book_depth_cache.get(&normalized_symbol);

                LiveSpotPriceView {
                    symbol: normalized_symbol,
                    binance_trade: binance_trade.map(|quote| live_spot_price_point(quote, now_ms)),
                    coinbase_ticker: coinbase_ticker
                        .map(|quote| live_spot_price_point(quote, now_ms)),
                    quote_points: quotes.map_or(0, VecDeque::len),
                    depth_age_ms: depth_snapshot
                        .map(|snapshot| now_ms.saturating_sub(snapshot.received_time_ms)),
                }
            })
            .collect()
    }

    /// Return latest Polymarket Chainlink RTDS oracle quotes for configured targets.
    pub async fn chainlink_price_views(
        &self,
        targets: &[MarketTarget],
    ) -> Vec<super::chainlink::ChainlinkOraclePriceView> {
        self.chainlink_oracle.latest_price_views(targets).await
    }

    /// Start reconnecting Binance market-data streams for a symbol.
    #[must_use]
    pub fn start_trade_stream(&self, symbol: &str) -> bool {
        let normalized_symbol = normalize_symbol(symbol);
        if normalized_symbol.is_empty() {
            return false;
        }

        {
            let Ok(mut started) = self.started_symbols.lock() else {
                warn!("started_symbols mutex poisoned; пропускаем запуск потока Binance");
                return false;
            };
            if !started.insert(normalized_symbol.clone()) {
                return true;
            }
        }

        install_rustls_crypto_provider();

        let stream_url = combined_stream_url(&self.websocket_base_url, &normalized_symbol);
        let client = self.clone();

        std::mem::drop(tokio::spawn(async move {
            client
                .run_combined_stream(normalized_symbol, stream_url)
                .await;
        }));

        true
    }

    /// Build the Binance spot context for a supported window market.
    ///
    /// # Errors
    ///
    /// Returns an error if the Binance REST requests fail or return malformed JSON.
    pub async fn market_context(
        &self,
        market: &BinaryMarket,
    ) -> Result<Option<MarketWindowContext>> {
        self.market_context_at_timestamp(market, Utc::now().timestamp())
            .await
    }

    /// Build the Binance spot context for a supported window market
    /// using an explicit observed timestamp.
    ///
    /// # Errors
    ///
    /// Returns an error if the Binance REST requests fail or return malformed JSON.
    pub async fn market_context_at_timestamp(
        &self,
        market: &BinaryMarket,
        observed_ts: i64,
    ) -> Result<Option<MarketWindowContext>> {
        let Some(target) = market.target() else {
            return Ok(None);
        };
        let Some(start_ts) = market.window_start_ts() else {
            return Ok(None);
        };

        let Some(mut context) = self
            .window_context_at(target, start_ts, observed_ts)
            .await?
        else {
            return Ok(None);
        };
        let target_price = market.target_price.unwrap_or(context.interval_open_price);
        let target_price_source = market
            .target_price_source
            .unwrap_or(TargetPriceSource::BinanceWindowOpenFallback);
        context.target_price = target_price;
        context.target_price_source = target_price_source;
        context.target_gap_bps = ((context.current_spot_price - target_price) / target_price
            * Decimal::from(10_000_u32))
        .round_dp(4);
        context.dominant_outcome = dominant_outcome_label(context.current_spot_price, target_price);
        let _oracle_applied = self
            .chainlink_oracle
            .decorate_context(market, &mut context)
            .await;

        Ok(Some(context))
    }

    /// Build an approximate historical Binance context for a past market window.
    ///
    /// The `elapsed_secs` parameter describes how many seconds after market open
    /// the synthetic strategy check should happen.
    ///
    /// # Errors
    ///
    /// Returns an error if Binance historical candle queries fail or return malformed data.
    pub async fn historical_context_from_slug(
        &self,
        slug: &str,
        elapsed_secs: i64,
    ) -> Result<Option<MarketWindowContext>> {
        let Some(target) = MarketTarget::from_slug(slug) else {
            return Ok(None);
        };
        let Some(start_ts) = parse_window_start_ts(slug, target) else {
            return Ok(None);
        };

        self.window_context_at(target, start_ts, start_ts + elapsed_secs.max(0))
            .await
    }

    /// Build historical Binance contexts for multiple offsets inside one market window.
    ///
    /// This is materially cheaper than calling [`Self::historical_context_from_slug`]
    /// per snapshot because it fetches the 1-minute kline slice once and reuses it.
    ///
    /// # Errors
    ///
    /// Returns an error if Binance historical candle queries fail or return malformed data.
    pub async fn historical_contexts_from_slug(
        &self,
        slug: &str,
        elapsed_secs_values: &[i64],
    ) -> Result<HashMap<i64, MarketWindowContext>> {
        let Some(target) = MarketTarget::from_slug(slug) else {
            return Ok(HashMap::new());
        };
        let Some(start_ts) = parse_window_start_ts(slug, target) else {
            return Ok(HashMap::new());
        };

        let window_secs = target.window_secs();
        let end_ts = start_ts + window_secs;
        let mut offsets = elapsed_secs_values
            .iter()
            .map(|elapsed_secs| (*elapsed_secs).clamp(0, window_secs.saturating_sub(1)))
            .collect::<Vec<_>>();
        offsets.sort_unstable();
        offsets.dedup();
        if offsets.is_empty() {
            return Ok(HashMap::new());
        }

        let symbol = target.binance_symbol();
        let interval_open_price = self
            .interval_open_price_cached(symbol, start_ts, end_ts)
            .await?;
        let max_elapsed_secs = offsets.iter().copied().max().unwrap_or(0);
        let second_count = usize::try_from(max_elapsed_secs.max(1)).unwrap_or(0).max(1);
        let second_klines = self
            .klines(symbol, "1s", start_ts * 1000, second_count)
            .await?;
        let mut contexts = HashMap::with_capacity(offsets.len());

        for elapsed_secs in offsets {
            let quote_ts = start_ts + elapsed_secs;
            let price_snapshot =
                historical_micro_price_snapshot(&second_klines, interval_open_price, elapsed_secs)?;
            let current_spot_price = price_snapshot.current;
            let spot_move_bps = ((current_spot_price - interval_open_price) / interval_open_price
                * Decimal::from(10_000_u32))
            .round_dp(4);
            let spot_move_1s_bps =
                historical_move_bps(price_snapshot.current, price_snapshot.burst_reference);
            let spot_move_5s_bps =
                historical_move_bps(price_snapshot.current, price_snapshot.micro_reference);
            let spot_move_15s_bps =
                historical_move_bps(price_snapshot.current, price_snapshot.momentum_reference);
            let micro_acceleration_bps =
                (spot_move_5s_bps - (spot_move_15s_bps / Decimal::from(3_u32))).round_dp(4);

            contexts.insert(
                elapsed_secs,
                MarketWindowContext {
                    target,
                    interval_open_price,
                    target_price: interval_open_price,
                    target_price_source: TargetPriceSource::BinanceWindowOpenFallback,
                    target_gap_bps: spot_move_bps,
                    current_spot_price,
                    current_spot_source: "Binance::Historical1s".to_owned(),
                    current_spot_event_age_ms: None,
                    current_spot_received_age_ms: None,
                    current_spot_quote_points: None,
                    exchange_book_age_ms: None,
                    exchange_book_top_imbalance_bps: Decimal::ZERO,
                    exchange_book_depth_imbalance_bps: Decimal::ZERO,
                    exchange_book_microprice_bps: Decimal::ZERO,
                    exchange_book_spread_bps: Decimal::ZERO,
                    micro_burst_reference_price: price_snapshot.burst_reference,
                    micro_reference_price: price_snapshot.micro_reference,
                    spot_move_bps,
                    spot_move_1s_bps,
                    spot_move_5s_bps,
                    spot_move_15s_bps,
                    micro_acceleration_bps,
                    dominant_outcome: dominant_outcome_label(
                        current_spot_price,
                        interval_open_price,
                    ),
                    seconds_left: end_ts - quote_ts,
                },
            );
        }

        Ok(contexts)
    }

    /// Build the Binance context for an arbitrary observed timestamp inside the market window.
    ///
    /// # Errors
    ///
    /// Returns an error if Binance REST requests fail or return malformed data.
    pub async fn context_for_slug_at_timestamp(
        &self,
        slug: &str,
        observed_ts: i64,
    ) -> Result<Option<MarketWindowContext>> {
        let Some(target) = MarketTarget::from_slug(slug) else {
            return Ok(None);
        };
        let Some(start_ts) = parse_window_start_ts(slug, target) else {
            return Ok(None);
        };

        self.window_context_at(target, start_ts, observed_ts).await
    }

    /// Build the Binance spot context for a BTC 5-minute market.
    ///
    /// # Errors
    ///
    /// Returns an error if the Binance REST requests fail or return malformed JSON.
    pub async fn btc_5m_context(
        &self,
        _symbol: &str,
        market: &BinaryMarket,
    ) -> Result<Option<BtcFiveMinuteContext>> {
        if market.target() != Some(MarketTarget::Btc5m) {
            return Ok(None);
        }

        self.market_context(market).await
    }

    /// Resolve the realized direction of a finished supported window market.
    ///
    /// # Errors
    ///
    /// Returns an error if Binance historical candle queries fail or return malformed data.
    pub async fn resolution_from_slug(&self, slug: &str) -> Result<Option<MarketWindowResolution>> {
        if let Some(resolution) = self.chainlink_oracle.resolution_from_slug(slug).await {
            debug!(
                slug = %slug,
                start_price = %resolution.start_price,
                end_price = %resolution.end_price,
                realized_move_bps = %resolution.realized_move_bps,
                "using Chainlink RTDS cached oracle resolution"
            );
            return Ok(Some(resolution));
        }

        let Some(target) = MarketTarget::from_slug(slug) else {
            return Ok(None);
        };
        let Some(start_ts) = parse_window_start_ts(slug, target) else {
            return Ok(None);
        };

        if Utc::now().timestamp() < start_ts + target.window_secs() {
            return Ok(None);
        }

        let symbol = target.binance_symbol();
        let kline_limit = usize::try_from(target.window_secs() / 60).unwrap_or(0);
        let klines = match self
            .one_minute_klines(symbol, start_ts * 1000, kline_limit)
            .await
        {
            Ok(klines) => klines,
            Err(error) => {
                if let Some(resolution) = self.resolution_from_stream_cache(target, start_ts).await
                {
                    debug!(
                        slug = %slug,
                        error = %error,
                        "using live stream cache for settlement after Binance kline lookup failed"
                    );
                    return Ok(Some(resolution));
                }
                return Err(error);
            }
        };
        if klines.len() < kline_limit {
            if let Some(resolution) = self.resolution_from_stream_cache(target, start_ts).await {
                debug!(
                    slug = %slug,
                    returned_klines = klines.len(),
                    expected_klines = kline_limit,
                    "using live stream cache for settlement after incomplete Binance kline response"
                );
                return Ok(Some(resolution));
            }
            return Ok(None);
        }

        let Some(first_kline) = klines.first() else {
            return Ok(None);
        };
        let Some(last_kline) = klines.last() else {
            return Ok(None);
        };

        Ok(market_window_resolution_from_prices(
            target,
            first_kline.open,
            last_kline.close,
            last_kline.close_time_ms,
        ))
    }

    /// Resolve a finished window using only in-memory live caches.
    ///
    /// This is intended for the reactive paper hot path, where blocking on
    /// Binance REST after a window closes can stall new scalp decisions.
    pub async fn resolution_from_slug_live_cache(
        &self,
        slug: &str,
    ) -> Option<MarketWindowResolution> {
        if let Some(resolution) = self.chainlink_oracle.resolution_from_slug(slug).await {
            debug!(
                slug = %slug,
                start_price = %resolution.start_price,
                end_price = %resolution.end_price,
                realized_move_bps = %resolution.realized_move_bps,
                "using Chainlink RTDS cached oracle resolution"
            );
            return Some(resolution);
        }

        let target = MarketTarget::from_slug(slug)?;
        let start_ts = parse_window_start_ts(slug, target)?;
        if Utc::now().timestamp() < start_ts.saturating_add(target.window_secs()) {
            return None;
        }

        self.resolution_from_stream_cache(target, start_ts).await
    }

    async fn resolution_from_stream_cache(
        &self,
        target: MarketTarget,
        start_ts: i64,
    ) -> Option<MarketWindowResolution> {
        let symbol = normalize_symbol(target.binance_symbol());
        let start_ms = start_ts.saturating_mul(1_000);
        let end_ts = start_ts.saturating_add(target.window_secs());
        let end_ms = end_ts.saturating_mul(1_000);

        let start_price = if let Some(price) = self
            .cached_interval_open_price_from_stream(&symbol, start_ts)
            .await
        {
            price
        } else {
            let cache = self.second_kline_cache.read().await;
            cache.get(&symbol).and_then(|klines| {
                klines
                    .iter()
                    .find(|kline| kline.open_time_ms == start_ms)
                    .map(|kline| kline.open)
            })?
        };
        let close_quote = self.cached_quote_near_window_close(&symbol, end_ms).await?;

        market_window_resolution_from_prices(
            target,
            start_price,
            close_quote.price,
            close_quote.event_time_ms,
        )
    }

    async fn cached_interval_open_price_from_stream(
        &self,
        symbol: &str,
        start_ts: i64,
    ) -> Option<Decimal> {
        let cache_key = format!("{}:{start_ts}", normalize_symbol(symbol));
        let now_ts = Utc::now().timestamp();
        let cache = self.interval_open_cache.read().await;
        cache
            .get(&cache_key)
            .filter(|entry| entry.expires_at_ts >= now_ts)
            .map(|entry| entry.price)
    }

    async fn cached_quote_near_window_close(
        &self,
        symbol: &str,
        end_ms: i64,
    ) -> Option<LiveSpotQuote> {
        let cache = self.quote_cache.read().await;
        let quotes = cache.get(&normalize_symbol(symbol))?;
        quotes
            .iter()
            .filter(|quote| {
                quote.event_time_ms <= end_ms
                    && quote.event_time_ms
                        >= end_ms.saturating_sub(SETTLEMENT_CACHE_CLOSE_LOOKBACK_MS)
            })
            .max_by_key(|quote| quote.event_time_ms)
            .copied()
            .or_else(|| {
                quotes
                    .iter()
                    .filter(|quote| {
                        quote.event_time_ms > end_ms
                            && quote.event_time_ms
                                <= end_ms.saturating_add(SETTLEMENT_CACHE_CLOSE_LOOKAHEAD_MS)
                    })
                    .min_by_key(|quote| quote.event_time_ms)
                    .copied()
            })
    }

    /// Resolve the realized direction of a BTC 5-minute market window from its slug.
    ///
    /// # Errors
    ///
    /// Returns an error if Binance historical candle queries fail or return malformed data.
    pub async fn btc_5m_resolution_from_slug(
        &self,
        _symbol: &str,
        slug: &str,
    ) -> Result<Option<BtcFiveMinuteResolution>> {
        if MarketTarget::from_slug(slug) != Some(MarketTarget::Btc5m) {
            return Ok(None);
        }

        self.resolution_from_slug(slug).await
    }

    async fn window_context_at(
        &self,
        target: MarketTarget,
        start_ts: i64,
        quote_ts: i64,
    ) -> Result<Option<MarketWindowContext>> {
        let window_secs = target.window_secs();
        let end_ts = start_ts + window_secs;
        if quote_ts < start_ts || quote_ts >= end_ts {
            return Ok(None);
        }

        let symbol = target.binance_symbol();
        let interval_open_price = self
            .interval_open_price_cached(symbol, start_ts, end_ts)
            .await?;
        let now_ts = Utc::now().timestamp();
        let live_snapshot = if should_use_live_snapshot(start_ts, end_ts, quote_ts, now_ts) {
            Some(self.live_price_snapshot(symbol).await?)
        } else {
            None
        };
        let current_spot_price = match live_snapshot {
            Some(snapshot) => snapshot.current_spot_price,
            None => self.one_minute_close_at(symbol, start_ts, quote_ts).await?,
        };
        let now_ms = Utc::now().timestamp_millis();
        let current_spot_source = live_snapshot.map_or_else(
            || "Binance::Rest1mFallback".to_owned(),
            |snapshot| snapshot.current_spot_source_label().to_owned(),
        );
        let current_spot_event_age_ms =
            live_snapshot.and_then(|snapshot| snapshot.current_spot_event_age_ms(now_ms));
        let current_spot_received_age_ms =
            live_snapshot.and_then(|snapshot| snapshot.current_spot_received_age_ms(now_ms));
        let current_spot_quote_points = live_snapshot.map(|snapshot| snapshot.quote_points);
        let exchange_book_age_ms =
            live_snapshot.and_then(|snapshot| snapshot.exchange_book_age_ms(now_ms));
        let exchange_book_top_imbalance_bps = live_snapshot
            .and_then(|snapshot| snapshot.exchange_book_pressure)
            .map_or(Decimal::ZERO, |snapshot| snapshot.top_imbalance_bps);
        let exchange_book_depth_imbalance_bps = live_snapshot
            .and_then(|snapshot| snapshot.exchange_book_pressure)
            .map_or(Decimal::ZERO, |snapshot| snapshot.depth_imbalance_bps);
        let exchange_book_microprice_bps = live_snapshot
            .and_then(|snapshot| snapshot.exchange_book_pressure)
            .map_or(Decimal::ZERO, |snapshot| snapshot.microprice_bps);
        let exchange_book_spread_bps = live_snapshot
            .and_then(|snapshot| snapshot.exchange_book_pressure)
            .map_or(Decimal::ZERO, |snapshot| snapshot.spread_bps);
        let micro_reference_price = live_snapshot.map_or(current_spot_price, |snapshot| {
            snapshot.micro_reference_price
        });
        let micro_burst_reference_price = live_snapshot.map_or(current_spot_price, |snapshot| {
            snapshot.micro_burst_reference_price
        });
        let spot_move_5s_bps =
            live_snapshot.map_or(Decimal::ZERO, |snapshot| snapshot.spot_move_5s_bps);
        let spot_move_1s_bps =
            live_snapshot.map_or(Decimal::ZERO, |snapshot| snapshot.spot_move_1s_bps);
        let momentum_move_bps =
            live_snapshot.map_or(Decimal::ZERO, |snapshot| snapshot.spot_move_15s_bps);
        let micro_acceleration_bps =
            live_snapshot.map_or(Decimal::ZERO, |snapshot| snapshot.micro_acceleration_bps);
        let spot_move_bps = ((current_spot_price - interval_open_price) / interval_open_price
            * Decimal::from(10_000_u32))
        .round_dp(4);

        let dominant_outcome = if current_spot_price >= interval_open_price {
            "Up"
        } else {
            "Down"
        }
        .to_owned();

        Ok(Some(MarketWindowContext {
            target,
            interval_open_price,
            target_price: interval_open_price,
            target_price_source: TargetPriceSource::BinanceWindowOpenFallback,
            target_gap_bps: spot_move_bps,
            current_spot_price,
            current_spot_source,
            current_spot_event_age_ms,
            current_spot_received_age_ms,
            current_spot_quote_points,
            exchange_book_age_ms,
            exchange_book_top_imbalance_bps,
            exchange_book_depth_imbalance_bps,
            exchange_book_microprice_bps,
            exchange_book_spread_bps,
            micro_burst_reference_price,
            micro_reference_price,
            spot_move_bps,
            spot_move_1s_bps,
            spot_move_5s_bps,
            spot_move_15s_bps: momentum_move_bps,
            micro_acceleration_bps,
            dominant_outcome,
            seconds_left: end_ts - quote_ts,
        }))
    }

    async fn interval_open_price_cached(
        &self,
        symbol: &str,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Decimal> {
        let cache_key = format!("{symbol}:{start_ts}");
        let now_ts = Utc::now().timestamp();
        if let Some(cached) = {
            let cache = self.interval_open_cache.read().await;
            cache.get(&cache_key).copied()
        } && cached.expires_at_ts >= now_ts
        {
            return Ok(cached.price);
        }

        let price = self.one_minute_open(symbol, start_ts * 1000).await?;
        let entry = CachedIntervalOpenPrice {
            price,
            expires_at_ts: end_ts.saturating_add(INTERVAL_OPEN_CACHE_GRACE_SECS),
        };

        let mut cache = self.interval_open_cache.write().await;
        cache.insert(cache_key, entry);
        prune_interval_open_cache(&mut cache, now_ts);

        Ok(price)
    }

    async fn cache_interval_open_from_second_kline(
        &self,
        symbol: &str,
        open_time_ms: i64,
        open: Decimal,
    ) {
        let Some((cache_key, end_ts)) =
            interval_open_cache_entry_from_second_kline(symbol, open_time_ms)
        else {
            return;
        };
        let now_ts = Utc::now().timestamp();
        let entry = CachedIntervalOpenPrice {
            price: open,
            expires_at_ts: end_ts.saturating_add(INTERVAL_OPEN_CACHE_GRACE_SECS),
        };

        let mut cache = self.interval_open_cache.write().await;
        cache.insert(cache_key, entry);
        prune_interval_open_cache(&mut cache, now_ts);
    }

    async fn run_combined_stream(self, symbol: String, stream_url: String) {
        loop {
            match connect_async(&stream_url).await {
                Ok((stream, _response)) => {
                    info!(symbol = %symbol, "connected Binance combined market-data stream");
                    let (_writer, mut reader) = stream.split();

                    while let Some(next_message) = reader.next().await {
                        match next_message {
                            Ok(Message::Text(payload)) => {
                                if let Err(error) =
                                    self.handle_combined_stream_message(payload.as_ref()).await
                                {
                                    warn!(
                                        symbol = %symbol,
                                        error = %error,
                                        "failed to handle Binance combined stream message"
                                    );
                                }
                            }
                            Ok(Message::Close(frame)) => {
                                info!(symbol = %symbol, ?frame, "Binance combined stream closed");
                                break;
                            }
                            Ok(_) => {}
                            Err(error) => {
                                warn!(
                                    symbol = %symbol,
                                    error = %error,
                                    "Binance combined stream error"
                                );
                                break;
                            }
                        }
                    }
                }
                Err(error) => {
                    warn!(
                        symbol = %symbol,
                        error = %error,
                        "failed to connect Binance combined stream"
                    );
                }
            }

            sleep(Duration::from_millis(STREAM_RECONNECT_DELAY_MS)).await;
        }
    }

    async fn handle_combined_stream_message(&self, payload: &str) -> Result<()> {
        let event = serde_json::from_str::<CombinedStreamEvent>(payload)?;
        let stream = event.stream.to_ascii_lowercase();
        if let Some(symbol) = stream.strip_suffix("@trade") {
            self.update_trade_quote(symbol, &event.data.to_string())
                .await
        } else if let Some(symbol) = stream.strip_suffix("@kline_1s") {
            self.update_second_kline(symbol, &event.data.to_string())
                .await
        } else if let Some(symbol) = stream.strip_suffix("@depth5@100ms") {
            self.update_depth_snapshot(symbol, &event.data.to_string())
                .await
        } else {
            Ok(())
        }
    }

    #[allow(dead_code)]
    async fn run_trade_stream(self, symbol: String, stream_url: String) {
        loop {
            match connect_async(&stream_url).await {
                Ok((stream, _response)) => {
                    info!(symbol = %symbol, "подключен поток сделок Binance");
                    let (_writer, mut reader) = stream.split();

                    while let Some(next_message) = reader.next().await {
                        match next_message {
                            Ok(Message::Text(payload)) => {
                                if let Err(error) =
                                    self.update_trade_quote(&symbol, payload.as_ref()).await
                                {
                                    warn!(
                                        symbol = %symbol,
                                        error = %error,
                                        "не удалось обновить кешированную котировку Binance"
                                    );
                                }
                            }
                            Ok(Message::Close(frame)) => {
                                info!(symbol = %symbol, ?frame, "поток сделок Binance закрыт");
                                break;
                            }
                            Ok(_) => {}
                            Err(error) => {
                                warn!(
                                    symbol = %symbol,
                                    error = %error,
                                    "ошибка потока сделок Binance"
                                );
                                break;
                            }
                        }
                    }
                }
                Err(error) => {
                    warn!(
                        symbol = %symbol,
                        error = %error,
                        "не удалось подключиться к потоку сделок Binance"
                    );
                }
            }

            sleep(Duration::from_millis(STREAM_RECONNECT_DELAY_MS)).await;
        }
    }

    #[allow(dead_code)]
    async fn run_second_kline_stream(self, symbol: String, stream_url: String) {
        loop {
            match connect_async(&stream_url).await {
                Ok((stream, _response)) => {
                    info!(symbol = %symbol, "подключен поток 1s-свечей Binance");
                    let (_writer, mut reader) = stream.split();

                    while let Some(next_message) = reader.next().await {
                        match next_message {
                            Ok(Message::Text(payload)) => {
                                if let Err(error) =
                                    self.update_second_kline(&symbol, payload.as_ref()).await
                                {
                                    warn!(
                                        symbol = %symbol,
                                        error = %error,
                                        "не удалось обновить кеш 1s-свечей Binance"
                                    );
                                }
                            }
                            Ok(Message::Close(frame)) => {
                                info!(symbol = %symbol, ?frame, "поток 1s-свечей Binance закрыт");
                                break;
                            }
                            Ok(_) => {}
                            Err(error) => {
                                warn!(
                                    symbol = %symbol,
                                    error = %error,
                                    "ошибка потока 1s-свечей Binance"
                                );
                                break;
                            }
                        }
                    }
                }
                Err(error) => {
                    warn!(
                        symbol = %symbol,
                        error = %error,
                        "не удалось подключиться к потоку 1s-свечей Binance"
                    );
                }
            }

            sleep(Duration::from_millis(STREAM_RECONNECT_DELAY_MS)).await;
        }
    }

    #[allow(dead_code)]
    async fn run_depth_stream(self, symbol: String, stream_url: String) {
        loop {
            match connect_async(&stream_url).await {
                Ok((stream, _response)) => {
                    info!(symbol = %symbol, "connected Binance depth5 stream");
                    let (_writer, mut reader) = stream.split();

                    while let Some(next_message) = reader.next().await {
                        match next_message {
                            Ok(Message::Text(payload)) => {
                                if let Err(error) =
                                    self.update_depth_snapshot(&symbol, payload.as_ref()).await
                                {
                                    warn!(
                                        symbol = %symbol,
                                        error = %error,
                                        "failed to update Binance depth snapshot"
                                    );
                                }
                            }
                            Ok(Message::Close(frame)) => {
                                info!(symbol = %symbol, ?frame, "Binance depth5 stream closed");
                                break;
                            }
                            Ok(_) => {}
                            Err(error) => {
                                warn!(
                                    symbol = %symbol,
                                    error = %error,
                                    "Binance depth5 stream error"
                                );
                                break;
                            }
                        }
                    }
                }
                Err(error) => {
                    warn!(
                        symbol = %symbol,
                        error = %error,
                        "failed to connect Binance depth5 stream"
                    );
                }
            }

            sleep(Duration::from_millis(STREAM_RECONNECT_DELAY_MS)).await;
        }
    }

    async fn update_trade_quote(&self, symbol: &str, payload: &str) -> Result<()> {
        let trade = serde_json::from_str::<TradeStreamEvent>(payload)?;
        let price = parse_decimal("binance.trade.price", &trade.price)?;
        let normalized_symbol = normalize_symbol(symbol);
        let received_time_ms = Utc::now().timestamp_millis();
        let mut cache = self.quote_cache.write().await;
        let quotes = cache
            .entry(normalized_symbol.clone())
            .or_insert_with(VecDeque::new);
        quotes.push_back(LiveSpotQuote {
            event_time_ms: trade.event_time_ms,
            received_time_ms,
            price,
            source: SpotQuoteSource::BinanceTrade,
        });
        prune_quote_history(quotes, trade.event_time_ms);
        drop(cache);
        self.refresh_live_signal_state(&normalized_symbol, received_time_ms)
            .await;
        let _ = self.trigger_tx.send(Some(BinanceTriggerEvent {
            symbol: normalized_symbol,
            event_time_ms: trade.event_time_ms,
            received_time_ms,
            price,
            source: BinanceTriggerSource::Trade,
        }));
        Ok(())
    }

    async fn update_depth_snapshot(&self, symbol: &str, payload: &str) -> Result<()> {
        let event = serde_json::from_str::<DepthStreamEvent>(payload)?;
        let bids = parse_depth_levels("binance.depth.bid", &event.bids)?;
        let asks = parse_depth_levels("binance.depth.ask", &event.asks)?;
        if bids.is_empty() || asks.is_empty() {
            return Ok(());
        }

        let normalized_symbol = normalize_symbol(symbol);
        let received_time_ms = Utc::now().timestamp_millis();
        let snapshot = LiveBookDepthSnapshot {
            received_time_ms,
            bids,
            asks,
        };
        let pressure = exchange_book_pressure(&snapshot);
        let trigger_price = snapshot
            .bids
            .first()
            .zip(snapshot.asks.first())
            .map(|(bid, ask)| ((bid.price + ask.price) / Decimal::from(2_u32)).round_dp(8));
        let mut cache = self.book_depth_cache.write().await;
        cache.insert(normalized_symbol.clone(), snapshot);
        drop(cache);
        self.refresh_live_signal_state(&normalized_symbol, received_time_ms)
            .await;

        if let (Some(pressure), Some(price)) = (pressure, trigger_price) {
            let _ = self.trigger_tx.send(Some(BinanceTriggerEvent {
                symbol: normalized_symbol,
                event_time_ms: pressure.received_time_ms,
                received_time_ms: pressure.received_time_ms,
                price,
                source: BinanceTriggerSource::Depth,
            }));
        }

        Ok(())
    }

    /// Ingest a vetted Coinbase ticker price into the live spot quote cache.
    ///
    /// Coinbase is treated as a secondary market-data source: it can speed up the
    /// current spot view, but only while it agrees with the primary Binance stream.
    ///
    /// # Errors
    ///
    /// Returns an error if the price is invalid.
    pub async fn ingest_coinbase_ticker_quote(
        &self,
        symbol: &str,
        event_time_ms: i64,
        price: Decimal,
        max_source_disagreement_bps: Decimal,
    ) -> Result<bool> {
        if price <= Decimal::ZERO {
            return Err(AppError::InvalidMarket(format!(
                "Coinbase ticker returned non-positive price for `{symbol}`: `{price}`"
            )));
        }

        let normalized_symbol = normalize_symbol(symbol);
        let received_time_ms = Utc::now().timestamp_millis();
        {
            let mut cache = self.quote_cache.write().await;
            let quotes = cache
                .entry(normalized_symbol.clone())
                .or_insert_with(VecDeque::new);

            let Some(primary_quote) = latest_fresh_quote_from_source(
                quotes,
                SpotQuoteSource::BinanceTrade,
                received_time_ms,
                MAX_CACHED_QUOTE_AGE_MS,
            ) else {
                return Ok(false);
            };
            if event_time_ms <= primary_quote.event_time_ms {
                return Ok(false);
            }

            let source_disagreement_bps = ((price - primary_quote.price).abs()
                / primary_quote.price
                * Decimal::from(10_000_u32))
            .round_dp(4);
            if source_disagreement_bps > max_source_disagreement_bps {
                warn!(
                    symbol = %normalized_symbol,
                    coinbase_price = %price,
                    latest_price = %primary_quote.price,
                    latest_source = ?primary_quote.source,
                    source_disagreement_bps = %source_disagreement_bps,
                    max_source_disagreement_bps = %max_source_disagreement_bps,
                    "Coinbase ticker rejected because it disagrees with the primary live quote"
                );
                return Ok(false);
            }

            quotes.push_back(LiveSpotQuote {
                event_time_ms,
                received_time_ms,
                price,
                source: SpotQuoteSource::CoinbaseTicker,
            });
            prune_quote_history(quotes, event_time_ms);
        }

        self.refresh_live_signal_state(&normalized_symbol, received_time_ms)
            .await;
        let _ = self.trigger_tx.send(Some(BinanceTriggerEvent {
            symbol: normalized_symbol,
            event_time_ms,
            received_time_ms,
            price,
            source: BinanceTriggerSource::CoinbaseTicker,
        }));
        Ok(true)
    }

    /// Ingest a vetted Coinbase level-2 book into the live exchange pressure cache.
    ///
    /// Coinbase depth is a secondary signal. It can refresh depth pressure quickly,
    /// but only while its mid price agrees with the primary Binance trade stream.
    ///
    /// # Errors
    ///
    /// Returns an error if the book contains invalid levels.
    pub async fn ingest_coinbase_l2_book(
        &self,
        symbol: &str,
        event_time_ms: i64,
        bids: Vec<(Decimal, Decimal)>,
        asks: Vec<(Decimal, Decimal)>,
        max_source_disagreement_bps: Decimal,
    ) -> Result<bool> {
        let bids = coinbase_depth_levels("coinbase.l2.bid", bids)?;
        let asks = coinbase_depth_levels("coinbase.l2.ask", asks)?;
        if bids.is_empty() || asks.is_empty() {
            return Ok(false);
        }

        let best_bid = bids.first().expect("checked non-empty bids");
        let best_ask = asks.first().expect("checked non-empty asks");
        if best_ask.price <= best_bid.price {
            return Ok(false);
        }

        let mid = ((best_bid.price + best_ask.price) / Decimal::from(2_u32)).round_dp(8);
        if mid <= Decimal::ZERO {
            return Ok(false);
        }

        let normalized_symbol = normalize_symbol(symbol);
        let received_time_ms = Utc::now().timestamp_millis();
        {
            let quote_cache = self.quote_cache.read().await;
            let Some(quotes) = quote_cache.get(&normalized_symbol) else {
                return Ok(false);
            };
            let Some(primary_quote) = latest_fresh_quote_from_source(
                quotes,
                SpotQuoteSource::BinanceTrade,
                received_time_ms,
                MAX_CACHED_QUOTE_AGE_MS,
            ) else {
                return Ok(false);
            };

            let source_disagreement_bps = ((mid - primary_quote.price).abs() / primary_quote.price
                * Decimal::from(10_000_u32))
            .round_dp(4);
            if source_disagreement_bps > max_source_disagreement_bps {
                warn!(
                    symbol = %normalized_symbol,
                    coinbase_mid = %mid,
                    latest_price = %primary_quote.price,
                    source_disagreement_bps = %source_disagreement_bps,
                    max_source_disagreement_bps = %max_source_disagreement_bps,
                    "Coinbase L2 rejected because it disagrees with the primary live quote"
                );
                return Ok(false);
            }
        }

        {
            let mut depth_cache = self.book_depth_cache.write().await;
            depth_cache.insert(
                normalized_symbol.clone(),
                LiveBookDepthSnapshot {
                    received_time_ms,
                    bids,
                    asks,
                },
            );
        }

        self.refresh_live_signal_state(&normalized_symbol, received_time_ms)
            .await;
        let _ = self.trigger_tx.send(Some(BinanceTriggerEvent {
            symbol: normalized_symbol,
            event_time_ms,
            received_time_ms,
            price: mid,
            source: BinanceTriggerSource::CoinbaseLevel2,
        }));
        Ok(true)
    }

    async fn update_second_kline(&self, symbol: &str, payload: &str) -> Result<()> {
        let event = serde_json::from_str::<KlineStreamEvent>(payload)?;
        let open = parse_decimal("binance.kline_1s.open", &event.kline.open)?;
        let normalized_symbol = normalize_symbol(symbol);
        let received_time_ms = Utc::now().timestamp_millis();
        self.cache_interval_open_from_second_kline(
            &normalized_symbol,
            event.kline.open_time_ms,
            open,
        )
        .await;
        let trigger_event = BinanceTriggerEvent {
            symbol: normalized_symbol.clone(),
            event_time_ms: event.kline.close_time_ms,
            received_time_ms,
            price: open,
            source: BinanceTriggerSource::OneSecondKline,
        };

        let mut cache = self.second_kline_cache.write().await;
        let klines = cache
            .entry(normalized_symbol.clone())
            .or_insert_with(VecDeque::new);

        if let Some(last) = klines.back_mut()
            && last.open_time_ms == event.kline.open_time_ms
        {
            last.close_time_ms = event.kline.close_time_ms;
            last.open = open;
            prune_second_kline_history(klines, event.kline.close_time_ms);
            drop(cache);
            self.refresh_live_signal_state(&normalized_symbol, received_time_ms)
                .await;
            let _ = self.trigger_tx.send(Some(trigger_event));
            return Ok(());
        }

        klines.push_back(LiveSecondKline {
            open_time_ms: event.kline.open_time_ms,
            close_time_ms: event.kline.close_time_ms,
            open,
        });
        prune_second_kline_history(klines, event.kline.close_time_ms);
        drop(cache);
        self.refresh_live_signal_state(&normalized_symbol, received_time_ms)
            .await;
        let _ = self.trigger_tx.send(Some(trigger_event));
        Ok(())
    }

    async fn refresh_live_signal_state(&self, symbol: &str, now_ms: i64) {
        let symbol = normalize_symbol(symbol);
        let Some(snapshot) = self.live_signal_state_from_streams(&symbol, now_ms).await else {
            return;
        };
        let Some(latest_quote_time_ms) = snapshot.latest_quote_event_time_ms else {
            return;
        };

        let mut cache = self.live_snapshot_cache.write().await;
        cache.insert(
            symbol,
            LiveSignalState {
                latest_quote_time_ms,
                updated_at_ms: now_ms,
                snapshot,
            },
        );
    }

    async fn cached_live_signal_state(
        &self,
        symbol: &str,
        now_ms: i64,
    ) -> Option<LivePriceSnapshot> {
        let cache = self.live_snapshot_cache.read().await;
        let state = cache.get(symbol)?;
        let latest_quote_received_time_ms = state.snapshot.latest_quote_received_time_ms?;
        if state.snapshot.latest_quote_event_time_ms != Some(state.latest_quote_time_ms)
            || now_ms.saturating_sub(latest_quote_received_time_ms) > MAX_CACHED_QUOTE_AGE_MS
            || now_ms.saturating_sub(state.updated_at_ms) > MAX_CACHED_QUOTE_AGE_MS
            || state
                .snapshot
                .exchange_book_pressure
                .is_some_and(|snapshot| {
                    now_ms.saturating_sub(snapshot.received_time_ms) > MAX_CACHED_QUOTE_AGE_MS
                })
        {
            return None;
        }

        Some(state.snapshot)
    }

    async fn live_signal_state_from_streams(
        &self,
        symbol: &str,
        now_ms: i64,
    ) -> Option<LivePriceSnapshot> {
        let quote_snapshot = self.live_quote_reference_snapshot(symbol, now_ms).await;
        let latest_quote = quote_snapshot.latest_quote?;
        let current_spot_price = latest_quote.price;
        let micro_burst_reference_price = quote_snapshot
            .micro_burst_reference_price
            .unwrap_or(current_spot_price);
        let micro_reference_price = quote_snapshot
            .micro_reference_price
            .unwrap_or(current_spot_price);
        let momentum_reference_price = quote_snapshot
            .momentum_reference_price
            .unwrap_or(current_spot_price);
        let exchange_book_pressure = self
            .exchange_book_pressure_snapshot(symbol, now_ms, MAX_CACHED_QUOTE_AGE_MS)
            .await;

        Some(build_live_price_snapshot(
            current_spot_price,
            Some(latest_quote.source),
            Some(latest_quote.event_time_ms),
            Some(latest_quote.received_time_ms),
            quote_snapshot.quote_points,
            exchange_book_pressure,
            micro_burst_reference_price,
            micro_reference_price,
            momentum_reference_price,
        ))
    }

    #[allow(clippy::too_many_lines)]
    async fn live_price_snapshot(&self, symbol: &str) -> Result<LivePriceSnapshot> {
        let symbol = normalize_symbol(symbol);
        let now_ms = Utc::now().timestamp_millis();
        if let Some(snapshot) = self.cached_live_signal_state(&symbol, now_ms).await {
            return Ok(snapshot);
        }

        let quote_snapshot = self.live_quote_reference_snapshot(&symbol, now_ms).await;
        let exchange_book_pressure = self
            .exchange_book_pressure_snapshot(&symbol, now_ms, MAX_CACHED_QUOTE_AGE_MS)
            .await;
        let current_spot_price = match quote_snapshot.latest_quote {
            Some(latest_quote) => latest_quote.price,
            None => self.rest_latest_price(&symbol).await?,
        };
        let (micro_stream_reference_price, micro_rest_reference_price) = self
            .fallback_reference_prices(
                &symbol,
                quote_snapshot.micro_reference_price,
                MICRO_PRICE_WINDOW_MS,
            )
            .await;
        let (micro_burst_stream_reference_price, micro_burst_rest_reference_price) = self
            .fallback_reference_prices(
                &symbol,
                quote_snapshot.micro_burst_reference_price,
                MICRO_BURST_PRICE_WINDOW_MS,
            )
            .await;
        let (momentum_stream_reference_price, momentum_rest_reference_price) = self
            .fallback_reference_prices(
                &symbol,
                quote_snapshot.momentum_reference_price,
                MOMENTUM_PRICE_WINDOW_MS,
            )
            .await;
        let micro_reference_price = quote_snapshot
            .micro_reference_price
            .or(micro_stream_reference_price)
            .or(micro_rest_reference_price)
            .unwrap_or(current_spot_price);
        let micro_burst_reference_price = quote_snapshot
            .micro_burst_reference_price
            .or(micro_burst_stream_reference_price)
            .or(micro_burst_rest_reference_price)
            .unwrap_or(current_spot_price);
        let momentum_reference_price = quote_snapshot
            .momentum_reference_price
            .or(momentum_stream_reference_price)
            .or(momentum_rest_reference_price)
            .unwrap_or(current_spot_price);
        let spot_move_1s_bps = if micro_burst_reference_price == Decimal::ZERO {
            Decimal::ZERO
        } else {
            ((current_spot_price - micro_burst_reference_price) / micro_burst_reference_price
                * Decimal::from(10_000_u32))
            .round_dp(4)
        };
        let spot_move_5s_bps = if micro_reference_price == Decimal::ZERO {
            Decimal::ZERO
        } else {
            ((current_spot_price - micro_reference_price) / micro_reference_price
                * Decimal::from(10_000_u32))
            .round_dp(4)
        };
        let momentum_move_bps = if momentum_reference_price == Decimal::ZERO {
            Decimal::ZERO
        } else {
            ((current_spot_price - momentum_reference_price) / momentum_reference_price
                * Decimal::from(10_000_u32))
            .round_dp(4)
        };
        let micro_acceleration_bps =
            (spot_move_5s_bps - (momentum_move_bps / Decimal::from(3_u32))).round_dp(4);

        if spot_move_5s_bps == Decimal::ZERO {
            info!(
                symbol = %symbol,
                current_spot_price = %current_spot_price,
                micro_burst_reference_price = %micro_burst_reference_price,
                micro_reference_price = %micro_reference_price,
                micro_burst_quote_reference_price = ?quote_snapshot.micro_burst_reference_price.map(|value| value.to_string()),
                micro_quote_reference_price = ?quote_snapshot.micro_reference_price.map(|value| value.to_string()),
                momentum_quote_reference_price = ?quote_snapshot.momentum_reference_price.map(|value| value.to_string()),
                micro_burst_stream_reference_price = ?micro_burst_stream_reference_price.map(|value| value.to_string()),
                micro_stream_reference_price = ?micro_stream_reference_price.map(|value| value.to_string()),
                momentum_stream_reference_price = ?momentum_stream_reference_price.map(|value| value.to_string()),
                micro_burst_rest_reference_price = ?micro_burst_rest_reference_price.map(|value| value.to_string()),
                micro_rest_reference_price = ?micro_rest_reference_price.map(|value| value.to_string()),
                momentum_rest_reference_price = ?momentum_rest_reference_price.map(|value| value.to_string()),
                quote_points = quote_snapshot.quote_points,
                "5s-движение Binance равно нулю"
            );
        }

        let snapshot = LivePriceSnapshot {
            current_spot_price,
            current_spot_source: quote_snapshot.latest_quote.map(|quote| quote.source),
            latest_quote_event_time_ms: quote_snapshot
                .latest_quote
                .map(|quote| quote.event_time_ms),
            latest_quote_received_time_ms: quote_snapshot
                .latest_quote
                .map(|quote| quote.received_time_ms),
            quote_points: quote_snapshot.quote_points,
            exchange_book_pressure,
            micro_burst_reference_price,
            micro_reference_price,
            spot_move_1s_bps,
            spot_move_5s_bps,
            spot_move_15s_bps: momentum_move_bps,
            micro_acceleration_bps,
        };
        if let Some(latest_quote) = quote_snapshot.latest_quote {
            let mut cache = self.live_snapshot_cache.write().await;
            cache.insert(
                symbol,
                LiveSignalState {
                    latest_quote_time_ms: latest_quote.event_time_ms,
                    updated_at_ms: now_ms,
                    snapshot,
                },
            );
        }

        Ok(snapshot)
    }

    async fn live_quote_reference_snapshot(
        &self,
        symbol: &str,
        now_ms: i64,
    ) -> LiveQuoteReferenceSnapshot {
        let cache = self.quote_cache.read().await;
        let Some(quotes) = cache.get(symbol) else {
            return LiveQuoteReferenceSnapshot::default();
        };
        let latest_quote = quotes.back().copied().filter(|latest_quote| {
            now_ms.saturating_sub(latest_quote.received_time_ms) <= MAX_CACHED_QUOTE_AGE_MS
        });
        let micro_reference_price = latest_quote.and_then(|latest_quote| {
            reference_quote_for_window(quotes, latest_quote.event_time_ms, MICRO_PRICE_WINDOW_MS)
                .map(|reference_quote| reference_quote.price)
        });
        let micro_burst_reference_price = latest_quote.and_then(|latest_quote| {
            reference_quote_for_window(
                quotes,
                latest_quote.event_time_ms,
                MICRO_BURST_PRICE_WINDOW_MS,
            )
            .map(|reference_quote| reference_quote.price)
        });
        let momentum_reference_price = latest_quote.and_then(|latest_quote| {
            reference_quote_for_window(quotes, latest_quote.event_time_ms, MOMENTUM_PRICE_WINDOW_MS)
                .map(|reference_quote| reference_quote.price)
        });

        LiveQuoteReferenceSnapshot {
            latest_quote,
            micro_burst_reference_price,
            micro_reference_price,
            momentum_reference_price,
            quote_points: quotes.len(),
        }
    }

    async fn exchange_book_pressure_snapshot(
        &self,
        symbol: &str,
        now_ms: i64,
        max_age_ms: i64,
    ) -> Option<ExchangeBookPressureSnapshot> {
        let cache = self.book_depth_cache.read().await;
        let snapshot = cache.get(symbol)?;
        if now_ms.saturating_sub(snapshot.received_time_ms) > max_age_ms {
            return None;
        }

        exchange_book_pressure(snapshot)
    }

    async fn fallback_reference_prices(
        &self,
        symbol: &str,
        quote_reference_price: Option<Decimal>,
        window_ms: i64,
    ) -> (Option<Decimal>, Option<Decimal>) {
        if quote_reference_price.is_some() {
            return (None, None);
        }

        let stream_reference_price = self
            .reference_price_from_stream(symbol, window_ms)
            .await
            .ok();
        let rest_reference_price = if stream_reference_price.is_none() {
            self.reference_price_from_rest(symbol, window_ms).await.ok()
        } else {
            None
        };

        (stream_reference_price, rest_reference_price)
    }

    async fn rest_latest_price(&self, symbol: &str) -> Result<Decimal> {
        let response = self
            .http
            .get(format!("{}/api/v3/ticker/price", self.base_url))
            .query(&[("symbol", normalize_symbol(symbol))])
            .send()
            .await?
            .error_for_status()?
            .json::<TickerPriceResponse>()
            .await?;

        parse_decimal("binance.price", &response.price)
    }

    async fn one_minute_open(&self, symbol: &str, start_time_ms: i64) -> Result<Decimal> {
        let klines = self.one_minute_klines(symbol, start_time_ms, 1).await?;
        let Some(first_kline) = klines.first() else {
            return Err(AppError::InvalidMarket(
                "Binance не вернул свечу для начала окна".to_owned(),
            ));
        };

        Ok(first_kline.open)
    }

    async fn one_minute_close_at(
        &self,
        symbol: &str,
        start_ts: i64,
        quote_ts: i64,
    ) -> Result<Decimal> {
        if quote_ts <= start_ts {
            return self.one_minute_open(symbol, start_ts * 1000).await;
        }

        let elapsed_secs = quote_ts - start_ts;
        let minute_count = usize::try_from((elapsed_secs + 59) / 60)
            .unwrap_or(0)
            .max(1);
        let klines = self
            .one_minute_klines(symbol, start_ts * 1000, minute_count)
            .await?;
        let index = usize::try_from(((elapsed_secs - 1).max(0)) / 60).unwrap_or(0);

        klines
            .get(index)
            .or_else(|| klines.last())
            .map(|kline| kline.close)
            .ok_or_else(|| {
                AppError::InvalidMarket(
                    "Binance не вернул свечу для исторического момента окна".to_owned(),
                )
            })
    }

    async fn one_minute_klines(
        &self,
        symbol: &str,
        start_time_ms: i64,
        limit: usize,
    ) -> Result<Vec<OneMinuteKline>> {
        self.klines(symbol, "1m", start_time_ms, limit).await
    }

    async fn reference_price_from_stream(&self, symbol: &str, window_ms: i64) -> Result<Decimal> {
        let symbol = normalize_symbol(symbol);
        let snapshot = {
            let cache = self.second_kline_cache.read().await;
            cache.get(&symbol).cloned()
        };
        let Some(klines) = snapshot else {
            return Err(AppError::InvalidMarket(
                "Binance ещё не накопил 1s-свечи для 5-секундного движения".to_owned(),
            ));
        };
        let Some(latest_kline) = klines.back().copied() else {
            return Err(AppError::InvalidMarket(
                "Binance ещё не накопил 1s-свечи для 5-секундного движения".to_owned(),
            ));
        };

        reference_kline_open_price_for_window(&klines, latest_kline.open_time_ms, window_ms)
            .ok_or_else(|| {
                AppError::InvalidMarket(
                    "Binance ещё не накопил достаточно 1s-свечей для 5-секундного движения"
                        .to_owned(),
                )
            })
    }

    async fn reference_price_from_rest(&self, symbol: &str, window_ms: i64) -> Result<Decimal> {
        let interval_count = usize::try_from((window_ms / 1000) + 1).unwrap_or(0).max(2);
        let klines = self.recent_klines(symbol, "1s", interval_count).await?;
        let offset = usize::try_from(window_ms / 1000).unwrap_or(0).max(1);
        let Some(reference_kline) = klines.get(klines.len().saturating_sub(offset + 1)) else {
            return Err(AppError::InvalidMarket(
                "Binance не вернул достаточно 1s-свечей для 5-секундного движения".to_owned(),
            ));
        };

        Ok(reference_kline.open)
    }

    async fn klines(
        &self,
        symbol: &str,
        interval: &str,
        start_time_ms: i64,
        limit: usize,
    ) -> Result<Vec<OneMinuteKline>> {
        let cache_key = HistoricalKlineCacheKey::new(symbol, interval, start_time_ms, limit);
        if let Some(cached_klines) = self
            .historical_kline_cache
            .read()
            .await
            .get(&cache_key)
            .cloned()
        {
            return Ok(cached_klines);
        }

        let response = self
            .http
            .get(format!("{}/api/v3/klines", self.base_url))
            .query(&[
                ("symbol", cache_key.symbol.clone()),
                ("interval", cache_key.interval.clone()),
                ("startTime", start_time_ms.to_string()),
                ("limit", limit.to_string()),
            ])
            .send()
            .await?
            .error_for_status()?
            .json::<Vec<Vec<serde_json::Value>>>()
            .await?;

        let klines = response
            .iter()
            .map(|raw_kline| parse_one_minute_kline(raw_kline))
            .collect::<Result<Vec<_>>>()?;

        let mut cache = self.historical_kline_cache.write().await;
        if cache.len() >= HISTORICAL_KLINE_CACHE_MAX_ENTRIES
            && let Some(first_key) = cache.keys().next().cloned()
        {
            cache.remove(&first_key);
        }
        cache.insert(cache_key, klines.clone());

        Ok(klines)
    }

    async fn recent_klines(
        &self,
        symbol: &str,
        interval: &str,
        limit: usize,
    ) -> Result<Vec<OneMinuteKline>> {
        let response = self
            .http
            .get(format!("{}/api/v3/klines", self.base_url))
            .query(&[
                ("symbol", normalize_symbol(symbol)),
                ("interval", interval.to_owned()),
                ("limit", limit.to_string()),
            ])
            .send()
            .await?
            .error_for_status()?
            .json::<Vec<Vec<serde_json::Value>>>()
            .await?;

        response
            .iter()
            .map(|raw_kline| parse_one_minute_kline(raw_kline))
            .collect::<Result<Vec<_>>>()
    }
}

#[derive(Debug, Deserialize)]
struct TickerPriceResponse {
    price: String,
}

#[derive(Debug, Deserialize)]
struct TradeStreamEvent {
    #[serde(rename = "E")]
    event_time_ms: i64,
    #[serde(rename = "p")]
    price: String,
}

#[derive(Debug, Deserialize)]
struct KlineStreamEvent {
    #[serde(rename = "k")]
    kline: KlineStreamPayload,
}

#[derive(Debug, Deserialize)]
struct KlineStreamPayload {
    #[serde(rename = "t")]
    open_time_ms: i64,
    #[serde(rename = "T")]
    close_time_ms: i64,
    #[serde(rename = "o")]
    open: String,
}

#[derive(Debug, Deserialize)]
struct DepthStreamEvent {
    bids: Vec<Vec<String>>,
    asks: Vec<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct CombinedStreamEvent {
    stream: String,
    data: serde_json::Value,
}

fn combined_stream_url(websocket_base_url: &str, symbol: &str) -> String {
    let trimmed = websocket_base_url.trim_end_matches('/');
    let stream_base = trimmed.strip_suffix("/ws").map_or_else(
        || format!("{trimmed}/stream"),
        |base| format!("{base}/stream"),
    );
    let symbol = normalize_symbol(symbol).to_ascii_lowercase();
    format!("{stream_base}?streams={symbol}@trade/{symbol}@kline_1s/{symbol}@depth5@100ms")
}

fn normalize_symbol(symbol: &str) -> String {
    symbol.trim().to_ascii_uppercase()
}

fn dominant_outcome_label(current_spot_price: Decimal, target_price: Decimal) -> String {
    if current_spot_price >= target_price {
        "Up"
    } else {
        "Down"
    }
    .to_owned()
}

fn parse_depth_levels(
    field: &'static str,
    raw_levels: &[Vec<String>],
) -> Result<Vec<LiveBookLevel>> {
    raw_levels
        .iter()
        .filter_map(|level| {
            let price = level.first()?;
            let size = level.get(1)?;
            Some((price, size))
        })
        .map(|(price, size)| {
            Ok(LiveBookLevel {
                price: parse_decimal(field, price)?,
                size: parse_decimal(field, size)?,
            })
        })
        .filter(|result| {
            result.as_ref().map_or(true, |level| {
                level.price > Decimal::ZERO && level.size > Decimal::ZERO
            })
        })
        .collect()
}

fn coinbase_depth_levels(
    field: &'static str,
    raw_levels: Vec<(Decimal, Decimal)>,
) -> Result<Vec<LiveBookLevel>> {
    raw_levels
        .into_iter()
        .map(|(price, size)| {
            if price <= Decimal::ZERO || size <= Decimal::ZERO {
                return Err(AppError::InvalidMarket(format!(
                    "invalid Coinbase depth level in `{field}`: price={price} size={size}"
                )));
            }
            Ok(LiveBookLevel { price, size })
        })
        .collect()
}

fn exchange_book_pressure(
    snapshot: &LiveBookDepthSnapshot,
) -> Option<ExchangeBookPressureSnapshot> {
    let best_bid = snapshot.bids.first()?;
    let best_ask = snapshot.asks.first()?;
    if best_bid.price <= Decimal::ZERO
        || best_ask.price <= best_bid.price
        || best_bid.size <= Decimal::ZERO
        || best_ask.size <= Decimal::ZERO
    {
        return None;
    }

    let bid_depth_notional = snapshot
        .bids
        .iter()
        .map(|level| level.price * level.size)
        .sum::<Decimal>();
    let ask_depth_notional = snapshot
        .asks
        .iter()
        .map(|level| level.price * level.size)
        .sum::<Decimal>();
    let depth_total = bid_depth_notional + ask_depth_notional;
    if depth_total <= Decimal::ZERO {
        return None;
    }

    let top_bid_notional = best_bid.price * best_bid.size;
    let top_ask_notional = best_ask.price * best_ask.size;
    let top_total = top_bid_notional + top_ask_notional;
    if top_total <= Decimal::ZERO {
        return None;
    }

    let mid_price = (best_bid.price + best_ask.price) / Decimal::from(2_u32);
    if mid_price <= Decimal::ZERO {
        return None;
    }

    let microprice = ((best_ask.price * best_bid.size) + (best_bid.price * best_ask.size))
        / (best_bid.size + best_ask.size);

    Some(ExchangeBookPressureSnapshot {
        received_time_ms: snapshot.received_time_ms,
        top_imbalance_bps: ((top_bid_notional - top_ask_notional) / top_total
            * Decimal::from(10_000_u32))
        .round_dp(4),
        depth_imbalance_bps: ((bid_depth_notional - ask_depth_notional) / depth_total
            * Decimal::from(10_000_u32))
        .round_dp(4),
        microprice_bps: ((microprice - mid_price) / mid_price * Decimal::from(10_000_u32))
            .round_dp(4),
        spread_bps: ((best_ask.price - best_bid.price) / mid_price * Decimal::from(10_000_u32))
            .round_dp(4),
    })
}

#[allow(dead_code)]
fn historical_close_price_for_elapsed(
    klines: &[OneMinuteKline],
    interval_open_price: Decimal,
    elapsed_secs: i64,
) -> Result<Decimal> {
    if elapsed_secs <= 0 {
        return Ok(interval_open_price);
    }

    let index = usize::try_from(((elapsed_secs - 1).max(0)) / 60).unwrap_or(0);
    klines
        .get(index)
        .or_else(|| klines.last())
        .map(|kline| kline.close)
        .ok_or_else(|| {
            AppError::InvalidMarket(
                "Binance не вернул свечу для исторического момента окна".to_owned(),
            )
        })
}

fn historical_micro_price_snapshot(
    second_klines: &[OneMinuteKline],
    interval_open_price: Decimal,
    elapsed_secs: i64,
) -> Result<HistoricalMicroPriceSnapshot> {
    let current = historical_second_close_price_for_elapsed(
        second_klines,
        interval_open_price,
        elapsed_secs,
    )?;

    Ok(HistoricalMicroPriceSnapshot {
        current,
        burst_reference: historical_reference_price_for_elapsed(
            second_klines,
            interval_open_price,
            elapsed_secs,
            MICRO_BURST_PRICE_WINDOW_MS / 1000,
        )?,
        micro_reference: historical_reference_price_for_elapsed(
            second_klines,
            interval_open_price,
            elapsed_secs,
            MICRO_PRICE_WINDOW_MS / 1000,
        )?,
        momentum_reference: historical_reference_price_for_elapsed(
            second_klines,
            interval_open_price,
            elapsed_secs,
            MOMENTUM_PRICE_WINDOW_MS / 1000,
        )?,
    })
}

fn historical_reference_price_for_elapsed(
    second_klines: &[OneMinuteKline],
    interval_open_price: Decimal,
    elapsed_secs: i64,
    lookback_secs: i64,
) -> Result<Decimal> {
    historical_second_close_price_for_elapsed(
        second_klines,
        interval_open_price,
        elapsed_secs.saturating_sub(lookback_secs),
    )
}

fn historical_second_close_price_for_elapsed(
    second_klines: &[OneMinuteKline],
    interval_open_price: Decimal,
    elapsed_secs: i64,
) -> Result<Decimal> {
    if elapsed_secs <= 0 {
        return Ok(interval_open_price);
    }

    let Some(first_kline) = second_klines.first() else {
        return Err(AppError::InvalidMarket(
            "Binance did not return 1s klines for historical micro context".to_owned(),
        ));
    };
    let target_close_time_ms = first_kline
        .open_time_ms
        .saturating_add(elapsed_secs.saturating_mul(1_000))
        .saturating_sub(1);
    second_klines
        .iter()
        .find(|kline| kline.close_time_ms >= target_close_time_ms)
        .or_else(|| second_klines.last())
        .map(|kline| kline.close)
        .ok_or_else(|| {
            AppError::InvalidMarket(
                "Binance did not return 1s klines for historical micro context".to_owned(),
            )
        })
}

fn historical_move_bps(current: Decimal, reference: Decimal) -> Decimal {
    if reference == Decimal::ZERO {
        return Decimal::ZERO;
    }

    ((current - reference) / reference * Decimal::from(10_000_u32)).round_dp(4)
}

#[allow(clippy::too_many_arguments)]
fn build_live_price_snapshot(
    current_spot_price: Decimal,
    current_spot_source: Option<SpotQuoteSource>,
    latest_quote_event_time_ms: Option<i64>,
    latest_quote_received_time_ms: Option<i64>,
    quote_points: usize,
    exchange_book_pressure: Option<ExchangeBookPressureSnapshot>,
    micro_burst_reference_price: Decimal,
    micro_reference_price: Decimal,
    momentum_reference_price: Decimal,
) -> LivePriceSnapshot {
    let spot_move_1s_bps = historical_move_bps(current_spot_price, micro_burst_reference_price);
    let spot_move_5s_bps = historical_move_bps(current_spot_price, micro_reference_price);
    let momentum_move_bps = historical_move_bps(current_spot_price, momentum_reference_price);
    let micro_acceleration_bps =
        (spot_move_5s_bps - (momentum_move_bps / Decimal::from(3_u32))).round_dp(4);

    LivePriceSnapshot {
        current_spot_price,
        current_spot_source,
        latest_quote_event_time_ms,
        latest_quote_received_time_ms,
        quote_points,
        exchange_book_pressure,
        micro_burst_reference_price,
        micro_reference_price,
        spot_move_1s_bps,
        spot_move_5s_bps,
        spot_move_15s_bps: momentum_move_bps,
        micro_acceleration_bps,
    }
}

fn prune_quote_history(quotes: &mut VecDeque<LiveSpotQuote>, latest_event_time_ms: i64) {
    let keep_from_ms = latest_event_time_ms.saturating_sub(QUOTE_HISTORY_RETENTION_MS);
    while quotes
        .front()
        .is_some_and(|quote| quote.event_time_ms < keep_from_ms)
    {
        let _dropped = quotes.pop_front();
    }
}

fn prune_second_kline_history(klines: &mut VecDeque<LiveSecondKline>, latest_close_time_ms: i64) {
    let keep_from_ms = latest_close_time_ms.saturating_sub(QUOTE_HISTORY_RETENTION_MS);
    while klines
        .front()
        .is_some_and(|kline| kline.close_time_ms < keep_from_ms)
    {
        let _dropped = klines.pop_front();
    }
}

fn interval_open_cache_entry_from_second_kline(
    symbol: &str,
    open_time_ms: i64,
) -> Option<(String, i64)> {
    if open_time_ms < 0 || open_time_ms % 1_000 != 0 {
        return None;
    }

    let normalized_symbol = normalize_symbol(symbol);
    let open_ts = open_time_ms / 1_000;
    let latest_end_ts = INTERVAL_OPEN_CACHE_TARGETS
        .iter()
        .filter(|target| normalize_symbol(target.binance_symbol()) == normalized_symbol)
        .filter(|target| open_ts.rem_euclid(target.window_secs()) == 0)
        .map(|target| open_ts.saturating_add(target.window_secs()))
        .max()?;

    Some((format!("{normalized_symbol}:{open_ts}"), latest_end_ts))
}

fn prune_interval_open_cache(cache: &mut HashMap<String, CachedIntervalOpenPrice>, now_ts: i64) {
    if cache.len() > INTERVAL_OPEN_CACHE_MAX_ENTRIES {
        cache.retain(|_, value| value.expires_at_ts >= now_ts);
        if cache.len() > INTERVAL_OPEN_CACHE_MAX_ENTRIES {
            cache.clear();
        }
    }
}

fn market_window_resolution_from_prices(
    target: MarketTarget,
    start_price: Decimal,
    end_price: Decimal,
    resolved_at_ms: i64,
) -> Option<MarketWindowResolution> {
    if start_price <= Decimal::ZERO || end_price <= Decimal::ZERO {
        return None;
    }

    let actual_outcome = match end_price.cmp(&start_price) {
        std::cmp::Ordering::Greater => WindowDirection::Up,
        std::cmp::Ordering::Less => WindowDirection::Down,
        std::cmp::Ordering::Equal => WindowDirection::Flat,
    };
    let realized_move_bps =
        ((end_price - start_price) / start_price * Decimal::from(10_000_u32)).round_dp(4);

    Some(MarketWindowResolution {
        target,
        start_price,
        end_price,
        realized_move_bps,
        actual_outcome,
        resolved_at_ms,
    })
}

fn reference_quote_for_window(
    quotes: &VecDeque<LiveSpotQuote>,
    latest_event_time_ms: i64,
    window_ms: i64,
) -> Option<LiveSpotQuote> {
    let first_quote = quotes.front().copied()?;
    let target_ms = latest_event_time_ms.saturating_sub(window_ms);
    if latest_event_time_ms.saturating_sub(first_quote.event_time_ms) < window_ms {
        return None;
    }

    let mut selected = first_quote;
    for quote in quotes {
        if quote.event_time_ms <= target_ms {
            selected = *quote;
        } else {
            break;
        }
    }

    Some(selected)
}

fn latest_fresh_quote_from_source(
    quotes: &VecDeque<LiveSpotQuote>,
    source: SpotQuoteSource,
    now_ms: i64,
    max_age_ms: i64,
) -> Option<LiveSpotQuote> {
    quotes.iter().rev().copied().find(|quote| {
        quote.source == source && now_ms.saturating_sub(quote.received_time_ms) <= max_age_ms
    })
}

const fn live_spot_price_point(quote: LiveSpotQuote, now_ms: i64) -> LiveSpotPricePoint {
    LiveSpotPricePoint {
        price: quote.price,
        event_age_ms: now_ms.saturating_sub(quote.event_time_ms),
        received_age_ms: now_ms.saturating_sub(quote.received_time_ms),
    }
}

#[cfg(test)]
fn micro_reference_quote(
    quotes: &VecDeque<LiveSpotQuote>,
    latest_event_time_ms: i64,
) -> Option<LiveSpotQuote> {
    reference_quote_for_window(quotes, latest_event_time_ms, MICRO_PRICE_WINDOW_MS)
}

fn reference_kline_open_price_for_window(
    klines: &VecDeque<LiveSecondKline>,
    latest_open_time_ms: i64,
    window_ms: i64,
) -> Option<Decimal> {
    let target_ms = latest_open_time_ms.saturating_sub(window_ms);
    let mut selected = None;

    for kline in klines {
        if kline.open_time_ms <= target_ms {
            selected = Some(kline.open);
        } else {
            break;
        }
    }

    selected.or_else(|| klines.front().map(|kline| kline.open))
}

fn should_use_live_snapshot(start_ts: i64, end_ts: i64, quote_ts: i64, now_ts: i64) -> bool {
    let active_window_now = start_ts <= now_ts && now_ts < end_ts;
    let near_real_time_quote = quote_ts >= now_ts.saturating_sub(LIVE_SNAPSHOT_MAX_QUOTE_LAG_SECS);
    active_window_now && near_real_time_quote
}

#[cfg(test)]
fn reference_kline_open_price(
    klines: &VecDeque<LiveSecondKline>,
    latest_open_time_ms: i64,
) -> Option<Decimal> {
    reference_kline_open_price_for_window(klines, latest_open_time_ms, MICRO_PRICE_WINDOW_MS)
}

fn install_rustls_crypto_provider() {
    if rustls::crypto::CryptoProvider::get_default().is_some() {
        return;
    }

    let provider = rustls::crypto::ring::default_provider();
    let _result = provider.install_default();
}

#[cfg(test)]
fn parse_btc_5m_window_start_ts(slug: &str) -> Option<i64> {
    parse_window_start_ts(slug, MarketTarget::Btc5m)
}

fn parse_window_start_ts(slug: &str, target: MarketTarget) -> Option<i64> {
    slug.strip_prefix(target.slug_prefix())?.parse::<i64>().ok()
}

fn parse_one_minute_kline(raw_kline: &[serde_json::Value]) -> Result<OneMinuteKline> {
    let Some(open_time_ms) = raw_kline.first().and_then(serde_json::Value::as_i64) else {
        return Err(AppError::InvalidMarket(
            "Binance kline response is missing open time".to_owned(),
        ));
    };
    let Some(open_value) = raw_kline.get(1).and_then(serde_json::Value::as_str) else {
        return Err(AppError::InvalidMarket(
            "в ответе Binance по свече отсутствует цена открытия".to_owned(),
        ));
    };
    let Some(close_value) = raw_kline.get(4).and_then(serde_json::Value::as_str) else {
        return Err(AppError::InvalidMarket(
            "в ответе Binance по свече отсутствует цена закрытия".to_owned(),
        ));
    };
    let Some(close_time_ms) = raw_kline.get(6).and_then(serde_json::Value::as_i64) else {
        return Err(AppError::InvalidMarket(
            "в ответе Binance по свече отсутствует время закрытия".to_owned(),
        ));
    };

    Ok(OneMinuteKline {
        open_time_ms,
        open: parse_decimal("binance.kline.open", open_value)?,
        close: parse_decimal("binance.kline.close", close_value)?,
        close_time_ms,
    })
}

fn parse_decimal(field: &'static str, value: &str) -> Result<Decimal> {
    value.parse::<Decimal>().map_err(|_| {
        AppError::InvalidMarket(format!(
            "некорректное decimal-значение в `{field}`: `{value}`"
        ))
    })
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use rust_decimal::Decimal;

    use super::{
        BinanceClient, BinanceTriggerSource, LiveBookDepthSnapshot, LiveBookLevel, LiveSecondKline,
        LiveSpotQuote, MarketTarget, OneMinuteKline, SpotQuoteSource, combined_stream_url,
        exchange_book_pressure, historical_micro_price_snapshot, historical_move_bps,
        interval_open_cache_entry_from_second_kline, micro_reference_quote,
        parse_btc_5m_window_start_ts, parse_depth_levels, parse_window_start_ts,
        prune_quote_history, prune_second_kline_history, reference_kline_open_price,
        should_use_live_snapshot,
    };

    const fn test_quote(event_time_ms: i64, price: Decimal) -> LiveSpotQuote {
        LiveSpotQuote {
            event_time_ms,
            received_time_ms: event_time_ms,
            price,
            source: SpotQuoteSource::BinanceTrade,
        }
    }

    #[test]
    fn parse_generic_window_start_ts_works_for_supported_targets() {
        assert_eq!(
            parse_window_start_ts("btc-updown-15m-1775127600", MarketTarget::Btc15m),
            Some(1_775_127_600)
        );
        assert_eq!(
            parse_window_start_ts("eth-updown-5m-1775127900", MarketTarget::Eth5m),
            Some(1_775_127_900)
        );
    }

    #[test]
    fn parse_btc_5m_start_ts_requires_expected_prefix() {
        assert_eq!(
            parse_btc_5m_window_start_ts("btc-updown-5m-1775127900"),
            Some(1_775_127_900)
        );
        assert_eq!(
            parse_btc_5m_window_start_ts("eth-updown-5m-1775127900"),
            None
        );
    }

    #[test]
    fn micro_reference_quote_prefers_price_around_five_seconds_back() {
        let quotes = VecDeque::from([
            test_quote(1_000, Decimal::new(10_000, 2)),
            test_quote(4_000, Decimal::new(10_100, 2)),
            test_quote(6_500, Decimal::new(10_200, 2)),
            test_quote(9_500, Decimal::new(10_300, 2)),
        ]);

        let reference = micro_reference_quote(&quotes, 9_500).expect("reference quote");
        assert_eq!(reference.event_time_ms, 4_000);
    }

    #[test]
    fn historical_micro_snapshot_reconstructs_short_horizon_moves() {
        let klines = (0_i64..20)
            .map(|index| OneMinuteKline {
                open_time_ms: index * 1_000,
                open: Decimal::new(100 + index, 0),
                close: Decimal::new(100 + index, 0),
                close_time_ms: (index * 1_000) + 999,
            })
            .collect::<Vec<_>>();

        let snapshot = historical_micro_price_snapshot(&klines, Decimal::new(100, 0), 20).unwrap();

        assert_eq!(snapshot.current, Decimal::new(119, 0));
        assert_eq!(snapshot.burst_reference, Decimal::new(118, 0));
        assert_eq!(snapshot.micro_reference, Decimal::new(114, 0));
        assert_eq!(snapshot.momentum_reference, Decimal::new(104, 0));
        assert!(historical_move_bps(snapshot.current, snapshot.micro_reference) > Decimal::ZERO);
    }

    #[test]
    fn prune_quote_history_keeps_recent_trade_window_only() {
        let mut quotes = VecDeque::from([
            test_quote(1_000, Decimal::new(10_000, 2)),
            test_quote(6_000, Decimal::new(10_100, 2)),
            test_quote(18_000, Decimal::new(10_200, 2)),
        ]);

        prune_quote_history(&mut quotes, 36_000);
        assert_eq!(quotes.len(), 2);
        assert_eq!(quotes.front().map(|quote| quote.event_time_ms), Some(6_000));
    }

    #[test]
    fn exchange_book_pressure_detects_bid_depth_imbalance() {
        let snapshot = LiveBookDepthSnapshot {
            received_time_ms: 1_000,
            bids: vec![
                LiveBookLevel {
                    price: Decimal::new(10_000, 2),
                    size: Decimal::new(10, 0),
                },
                LiveBookLevel {
                    price: Decimal::new(9_999, 2),
                    size: Decimal::new(8, 0),
                },
            ],
            asks: vec![LiveBookLevel {
                price: Decimal::new(10_001, 2),
                size: Decimal::new(2, 0),
            }],
        };

        let pressure = exchange_book_pressure(&snapshot).expect("book pressure");

        assert!(pressure.depth_imbalance_bps > Decimal::from(7_000));
        assert!(pressure.top_imbalance_bps > Decimal::from(6_000));
        assert!(pressure.microprice_bps > Decimal::ZERO);
        assert!(pressure.spread_bps > Decimal::ZERO);
    }

    #[test]
    fn parse_depth_levels_ignores_malformed_or_zero_levels() {
        let raw = vec![
            vec!["100.00".to_owned(), "2.0".to_owned()],
            vec!["99.99".to_owned()],
            vec!["99.98".to_owned(), "0".to_owned()],
        ];

        let levels = parse_depth_levels("test.depth", &raw).expect("depth levels");

        assert_eq!(levels.len(), 1);
        assert_eq!(levels[0].price, Decimal::new(10_000, 2));
        assert_eq!(levels[0].size, Decimal::new(20, 1));
    }

    #[test]
    fn combined_stream_url_uses_single_binance_stream_endpoint() {
        assert_eq!(
            combined_stream_url("wss://stream.binance.com:9443/ws", "BTCUSDT"),
            "wss://stream.binance.com:9443/stream?streams=btcusdt@trade/btcusdt@kline_1s/btcusdt@depth5@100ms"
        );
    }

    #[tokio::test]
    async fn combined_stream_dispatch_updates_quote_kline_and_depth() {
        let client = BinanceClient::new(
            "https://example.invalid".to_owned(),
            "wss://example.invalid/ws".to_owned(),
            1,
        )
        .expect("client");

        client
            .handle_combined_stream_message(
                r#"{"stream":"btcusdt@trade","data":{"E":10000,"p":"100.00"}}"#,
            )
            .await
            .expect("trade message");
        client
            .handle_combined_stream_message(
                r#"{"stream":"btcusdt@kline_1s","data":{"k":{"t":10000,"T":10999,"o":"100.00"}}}"#,
            )
            .await
            .expect("kline message");
        client
            .handle_combined_stream_message(
                r#"{"stream":"btcusdt@depth5@100ms","data":{"bids":[["99.99","2"]],"asks":[["100.01","2"]]}}"#,
            )
            .await
            .expect("depth message");

        let health = client
            .live_market_data_health_for_symbols(&["BTCUSDT"])
            .await;

        assert_eq!(health.len(), 1);
        assert_eq!(health[0].quote_source, Some("Binance::Trade"));
        assert!(health[0].has_fresh_quote(10_000));
        assert!(health[0].has_fresh_depth(10_000));
        assert!(
            client
                .live_snapshot_cache
                .read()
                .await
                .contains_key("BTCUSDT")
        );
    }

    #[tokio::test]
    async fn coinbase_ticker_quote_emits_reactive_trigger() {
        let client = BinanceClient::new(
            "https://example.invalid".to_owned(),
            "wss://example.invalid/ws".to_owned(),
            1,
        )
        .expect("client");
        let mut rx = client.subscribe_triggers();
        client
            .update_trade_quote("btcusdt", r#"{"E":10000,"p":"100.00"}"#)
            .await
            .expect("seed primary binance quote");
        rx.changed().await.expect("binance trigger");

        let accepted = client
            .ingest_coinbase_ticker_quote(
                "btcusdt",
                11_000,
                Decimal::new(10_005, 2),
                Decimal::from(25_u32),
            )
            .await
            .expect("ingest coinbase quote");

        assert!(accepted);
        rx.changed().await.expect("coinbase trigger");
        let event = rx.borrow_and_update().clone().expect("trigger event");
        assert_eq!(event.symbol, "BTCUSDT");
        assert_eq!(event.event_time_ms, 11_000);
        assert!(event.received_time_ms >= event.event_time_ms);
        assert_eq!(event.price, Decimal::new(10_005, 2));
        assert_eq!(event.source, BinanceTriggerSource::CoinbaseTicker);
    }

    #[tokio::test]
    async fn coinbase_l2_book_updates_depth_pressure_and_emits_trigger() {
        let client = BinanceClient::new(
            "https://example.invalid".to_owned(),
            "wss://example.invalid/ws".to_owned(),
            1,
        )
        .expect("client");
        let mut rx = client.subscribe_triggers();
        client
            .update_trade_quote("btcusdt", r#"{"E":10000,"p":"100.00"}"#)
            .await
            .expect("seed primary binance quote");
        rx.changed().await.expect("binance trigger");

        let accepted = client
            .ingest_coinbase_l2_book(
                "btcusdt",
                11_000,
                vec![(Decimal::new(9_999, 2), Decimal::new(5, 0))],
                vec![(Decimal::new(10_001, 2), Decimal::new(1, 0))],
                Decimal::from(25_u32),
            )
            .await
            .expect("ingest coinbase l2");

        assert!(accepted);
        rx.changed().await.expect("coinbase l2 trigger");
        let event = rx.borrow_and_update().clone().expect("trigger event");
        assert_eq!(event.source, BinanceTriggerSource::CoinbaseLevel2);
        assert_eq!(event.price, Decimal::new(10_000, 2));

        let health = client
            .live_market_data_health_for_symbols(&["BTCUSDT"])
            .await;
        assert!(health[0].has_fresh_depth(10_000));
        let live_state = client.live_snapshot_cache.read().await;
        let state = live_state.get("BTCUSDT").expect("live signal state");
        let pressure = state
            .snapshot
            .exchange_book_pressure
            .expect("book pressure");
        assert!(pressure.depth_imbalance_bps > Decimal::ZERO);
    }

    #[tokio::test]
    async fn coinbase_ticker_quote_requires_fresh_primary_binance_quote() {
        let client = BinanceClient::new(
            "https://example.invalid".to_owned(),
            "wss://example.invalid/ws".to_owned(),
            1,
        )
        .expect("client");

        let accepted = client
            .ingest_coinbase_ticker_quote(
                "btcusdt",
                11_000,
                Decimal::new(10_005, 2),
                Decimal::from(25_u32),
            )
            .await
            .expect("ingest coinbase quote");

        assert!(!accepted);
    }

    #[tokio::test]
    async fn live_market_data_health_reports_fresh_trade_quote() {
        let client = BinanceClient::new(
            "https://example.invalid".to_owned(),
            "wss://example.invalid/ws".to_owned(),
            1,
        )
        .expect("client");
        client
            .update_trade_quote("btcusdt", r#"{"E":10000,"p":"100.00"}"#)
            .await
            .expect("seed primary binance quote");

        let health = client
            .live_market_data_health_for_symbols(&["BTCUSDT"])
            .await;

        assert_eq!(health.len(), 1);
        assert_eq!(health[0].symbol, "BTCUSDT");
        assert_eq!(health[0].quote_source, Some("Binance::Trade"));
        assert_eq!(health[0].quote_points, 1);
        assert!(health[0].has_fresh_quote(10_000));
        assert!(!health[0].has_fresh_depth(10_000));
    }

    #[tokio::test]
    async fn settlement_resolution_can_use_live_stream_cache() {
        let client = BinanceClient::new(
            "https://example.invalid".to_owned(),
            "wss://example.invalid/ws".to_owned(),
            1,
        )
        .expect("client");

        client
            .update_second_kline("btcusdt", r#"{"k":{"t":900000,"T":900999,"o":"100.00"}}"#)
            .await
            .expect("window-open kline");
        client
            .update_trade_quote("btcusdt", r#"{"E":1199900,"p":"101.00"}"#)
            .await
            .expect("near-close quote");

        let resolution = client
            .resolution_from_stream_cache(MarketTarget::Btc5m, 900)
            .await
            .expect("stream-cache resolution");

        assert_eq!(resolution.start_price, Decimal::new(10_000, 2));
        assert_eq!(resolution.end_price, Decimal::new(10_100, 2));
        assert_eq!(resolution.actual_outcome, super::WindowDirection::Up);
    }

    #[tokio::test]
    async fn live_cache_slug_resolution_avoids_rest_lookup() {
        let client = BinanceClient::new(
            "https://example.invalid".to_owned(),
            "wss://example.invalid/ws".to_owned(),
            1,
        )
        .expect("client");

        client
            .update_second_kline("btcusdt", r#"{"k":{"t":900000,"T":900999,"o":"100.00"}}"#)
            .await
            .expect("window-open kline");
        client
            .update_trade_quote("btcusdt", r#"{"E":1199900,"p":"99.00"}"#)
            .await
            .expect("near-close quote");

        let resolution = client
            .resolution_from_slug_live_cache("btc-updown-5m-900")
            .await
            .expect("stream-cache slug resolution");

        assert_eq!(resolution.start_price, Decimal::new(10_000, 2));
        assert_eq!(resolution.end_price, Decimal::new(9_900, 2));
        assert_eq!(resolution.actual_outcome, super::WindowDirection::Down);
    }

    #[tokio::test]
    async fn trade_quote_precomputes_live_signal_state() {
        let client = BinanceClient::new(
            "https://example.invalid".to_owned(),
            "wss://example.invalid/ws".to_owned(),
            1,
        )
        .expect("client");

        client
            .update_trade_quote("btcusdt", r#"{"E":10000,"p":"100.00"}"#)
            .await
            .expect("first quote");
        client
            .update_trade_quote("btcusdt", r#"{"E":16000,"p":"101.00"}"#)
            .await
            .expect("second quote");

        let cache = client.live_snapshot_cache.read().await;
        let state = cache.get("BTCUSDT").expect("precomputed state");

        assert_eq!(state.latest_quote_time_ms, 16_000);
        assert!(state.updated_at_ms > 0);
        assert_eq!(state.snapshot.current_spot_price, Decimal::new(10_100, 2));
        assert_eq!(state.snapshot.current_spot_source_label(), "Binance::Trade");
        assert!(state.snapshot.spot_move_1s_bps > Decimal::ZERO);
        assert!(state.snapshot.spot_move_5s_bps > Decimal::ZERO);
    }

    #[test]
    fn reference_kline_open_price_prefers_open_around_five_seconds_back() {
        let klines = VecDeque::from([
            LiveSecondKline {
                open_time_ms: 1_000,
                close_time_ms: 1_999,
                open: Decimal::new(10_000, 2),
            },
            LiveSecondKline {
                open_time_ms: 4_000,
                close_time_ms: 4_999,
                open: Decimal::new(10_100, 2),
            },
            LiveSecondKline {
                open_time_ms: 6_000,
                close_time_ms: 6_999,
                open: Decimal::new(10_200, 2),
            },
            LiveSecondKline {
                open_time_ms: 9_000,
                close_time_ms: 9_999,
                open: Decimal::new(10_300, 2),
            },
        ]);

        let reference = reference_kline_open_price(&klines, 9_000);
        assert_eq!(reference, Some(Decimal::new(10_100, 2)));
    }

    #[test]
    fn prune_second_kline_history_keeps_recent_window_only() {
        let mut klines = VecDeque::from([
            LiveSecondKline {
                open_time_ms: 1_000,
                close_time_ms: 1_999,
                open: Decimal::new(10_000, 2),
            },
            LiveSecondKline {
                open_time_ms: 6_000,
                close_time_ms: 6_999,
                open: Decimal::new(10_100, 2),
            },
            LiveSecondKline {
                open_time_ms: 18_000,
                close_time_ms: 18_999,
                open: Decimal::new(10_200, 2),
            },
        ]);

        prune_second_kline_history(&mut klines, 36_999);
        assert_eq!(klines.len(), 2);
        assert_eq!(klines.front().map(|kline| kline.open_time_ms), Some(6_000));
    }

    #[test]
    fn interval_open_cache_entry_accepts_aligned_live_second_kline() {
        let entry = interval_open_cache_entry_from_second_kline("btcusdt", 300_000);
        assert_eq!(entry, Some(("BTCUSDT:300".to_owned(), 600)));
        let sol_entry = interval_open_cache_entry_from_second_kline("solusdt", 300_000);
        assert_eq!(sol_entry, Some(("SOLUSDT:300".to_owned(), 600)));
        let xrp_entry = interval_open_cache_entry_from_second_kline("xrpusdt", 300_000);
        assert_eq!(xrp_entry, Some(("XRPUSDT:300".to_owned(), 600)));
        let bnb_entry = interval_open_cache_entry_from_second_kline("bnbusdt", 300_000);
        assert_eq!(bnb_entry, Some(("BNBUSDT:300".to_owned(), 600)));
    }

    #[test]
    fn interval_open_cache_entry_uses_longer_expiry_for_15m_boundary() {
        let entry = interval_open_cache_entry_from_second_kline("BTCUSDT", 900_000);
        assert_eq!(entry, Some(("BTCUSDT:900".to_owned(), 1_800)));
    }

    #[test]
    fn interval_open_cache_entry_rejects_unaligned_second_kline() {
        assert_eq!(
            interval_open_cache_entry_from_second_kline("BTCUSDT", 301_000),
            None
        );
        assert_eq!(
            interval_open_cache_entry_from_second_kline("DOGEUSDT", 300_000),
            None
        );
    }

    #[test]
    fn should_use_live_snapshot_for_active_window_with_small_quote_lag() {
        assert!(should_use_live_snapshot(1_000, 1_300, 1_198, 1_200));
    }

    #[test]
    fn should_use_live_snapshot_for_active_window_with_server_time_lag() {
        assert!(should_use_live_snapshot(1_000, 1_300, 1_195, 1_200));
    }

    #[test]
    fn should_not_use_live_snapshot_for_stale_quote_even_in_active_window() {
        assert!(!should_use_live_snapshot(1_000, 1_300, 1_150, 1_200));
    }

    #[test]
    fn should_not_use_live_snapshot_for_historical_window() {
        assert!(!should_use_live_snapshot(1_000, 1_300, 1_299, 1_400));
    }
}
