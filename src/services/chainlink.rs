//! Polymarket RTDS Chainlink oracle-price stream.
//!
//! Binance/Coinbase remain the fast execution signal. This cache tracks the
//! Chainlink reference price used by Polymarket crypto Up/Down markets, so live
//! gaps and paper settlement can prefer the same oracle family as the market.

use std::collections::{HashMap, HashSet, VecDeque};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::sync::{RwLock, watch};
use tokio::time::{interval, sleep};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, info, warn};

use crate::config::ChainlinkOracleConfig;
use crate::error::{AppError, Result};
use crate::models::{BinaryMarket, MarketTarget, TargetPriceSource};

use super::binance::{MarketWindowContext, MarketWindowResolution, WindowDirection};

const STREAM_RECONNECT_DELAY_MS: u64 = 300;
const RTDS_PING_INTERVAL_MS: u64 = 5_000;
const QUOTE_HISTORY_RETENTION_MS: i64 = 30 * 60 * 1_000;
const WINDOW_CACHE_GRACE_SECS: i64 = 30 * 60;
const WINDOW_CACHE_MAX_ENTRIES: usize = 4_096;
const SUPPORTED_TARGETS: [MarketTarget; 6] = [
    MarketTarget::Btc5m,
    MarketTarget::Btc15m,
    MarketTarget::Eth5m,
    MarketTarget::Eth15m,
    MarketTarget::Sol5m,
    MarketTarget::Xrp5m,
];

/// One Chainlink oracle price emitted by Polymarket RTDS.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct ChainlinkOracleQuote {
    pub event_time_ms: i64,
    pub received_time_ms: i64,
    pub price: Decimal,
}

/// Latest Chainlink RTDS oracle price for a configured market target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainlinkOraclePriceView {
    pub target: MarketTarget,
    pub symbol: Option<&'static str>,
    pub quote: Option<ChainlinkOraclePricePoint>,
}

/// One Chainlink RTDS price point with clock-lag diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChainlinkOraclePricePoint {
    pub price: Decimal,
    pub event_age_ms: i64,
    pub received_age_ms: i64,
}

/// Reactive update emitted when a fresh Chainlink RTDS price arrives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainlinkOracleTriggerEvent {
    pub symbol: String,
    pub event_time_ms: i64,
    pub received_time_ms: i64,
    pub price: Decimal,
}

#[derive(Debug, Clone, Copy, Default)]
struct ChainlinkWindowCache {
    start_quote: Option<ChainlinkOracleQuote>,
    latest_quote: Option<ChainlinkOracleQuote>,
    explicit_target_price: Option<Decimal>,
    expires_at_ts: i64,
}

#[derive(Debug, Default)]
struct ChainlinkOracleState {
    quotes_by_symbol: HashMap<String, VecDeque<ChainlinkOracleQuote>>,
    windows: HashMap<String, ChainlinkWindowCache>,
}

/// Shared cache and reconnecting stream manager for Polymarket RTDS Chainlink prices.
#[derive(Debug, Clone)]
pub struct ChainlinkOracleCache {
    state: Arc<RwLock<ChainlinkOracleState>>,
    settings: Arc<RwLock<ChainlinkOracleConfig>>,
    started: Arc<AtomicBool>,
    trigger_tx: watch::Sender<Option<ChainlinkOracleTriggerEvent>>,
}

impl Default for ChainlinkOracleCache {
    fn default() -> Self {
        Self {
            state: Arc::new(RwLock::new(ChainlinkOracleState::default())),
            settings: Arc::new(RwLock::new(ChainlinkOracleConfig::default())),
            started: Arc::new(AtomicBool::new(false)),
            trigger_tx: watch::channel(None).0,
        }
    }
}

impl ChainlinkOracleCache {
    /// Start one reconnecting Polymarket RTDS websocket for all supported configured targets.
    #[must_use]
    pub fn start_stream(
        &self,
        websocket_url: String,
        targets: &[MarketTarget],
        settings: ChainlinkOracleConfig,
    ) -> bool {
        if !settings.enabled {
            return false;
        }

        let symbols = configured_chainlink_symbols(targets);
        if symbols.is_empty() {
            warn!("Chainlink RTDS oracle enabled but no configured target has RTDS support");
            return false;
        }

        if self.started.swap(true, Ordering::AcqRel) {
            return true;
        }

        let cache = self.clone();
        std::mem::drop(tokio::spawn(async move {
            cache.configure(settings).await;
            cache.run_stream_loop(websocket_url, symbols).await;
        }));

        true
    }

    /// Subscribe to live Chainlink RTDS trigger events.
    #[must_use]
    pub fn subscribe_triggers(&self) -> watch::Receiver<Option<ChainlinkOracleTriggerEvent>> {
        self.trigger_tx.subscribe()
    }

    /// Return latest Chainlink RTDS quotes for each configured target.
    pub async fn latest_price_views(
        &self,
        targets: &[MarketTarget],
    ) -> Vec<ChainlinkOraclePriceView> {
        let now_ms = Utc::now().timestamp_millis();
        let state = self.state.read().await;

        targets
            .iter()
            .copied()
            .map(|target| {
                let symbol = target.polymarket_chainlink_symbol();
                let quote = symbol
                    .and_then(|symbol| {
                        state
                            .quotes_by_symbol
                            .get(&normalize_chainlink_symbol(symbol))
                    })
                    .and_then(|quotes| quotes.back())
                    .copied()
                    .map(|quote| ChainlinkOraclePricePoint {
                        price: quote.price,
                        event_age_ms: now_ms.saturating_sub(quote.event_time_ms),
                        received_age_ms: now_ms.saturating_sub(quote.received_time_ms),
                    });

                ChainlinkOraclePriceView {
                    target,
                    symbol,
                    quote,
                }
            })
            .collect()
    }

    async fn configure(&self, settings: ChainlinkOracleConfig) {
        *self.settings.write().await = settings;
    }

    /// Apply a fresh Chainlink oracle price to a Binance/Coinbase context.
    pub async fn decorate_context(
        &self,
        market: &BinaryMarket,
        context: &mut MarketWindowContext,
    ) -> bool {
        let settings = *self.settings.read().await;
        if !settings.enabled {
            return false;
        }

        let Some(target) = market.target() else {
            return false;
        };
        let Some(symbol) = target.polymarket_chainlink_symbol() else {
            return false;
        };
        let Some(start_ts) = market.window_start_ts() else {
            return false;
        };
        let end_ts = start_ts.saturating_add(target.window_secs());
        let Some(latest) = self
            .latest_fresh_quote(symbol, settings.max_quote_age_ms)
            .await
        else {
            return false;
        };
        if latest.event_time_ms < start_ts.saturating_mul(1_000)
            || latest.event_time_ms >= end_ts.saturating_mul(1_000)
        {
            return false;
        }

        let (target_price, target_price_source) = if let Some(target_price) = market.target_price {
            self.record_market_target_price(target, start_ts, target_price)
                .await;
            (
                target_price,
                market
                    .target_price_source
                    .unwrap_or(TargetPriceSource::PolymarketEventMetadata),
            )
        } else if let Some(open_quote) = self
            .window_open_quote(target, start_ts, settings.max_window_open_lag_ms)
            .await
        {
            (open_quote.price, TargetPriceSource::ChainlinkRtdsWindowOpen)
        } else {
            return false;
        };
        if target_price <= Decimal::ZERO || latest.price <= Decimal::ZERO {
            return false;
        }

        let target_gap_bps = move_bps(latest.price, target_price);
        context.interval_open_price = target_price;
        context.target_price = target_price;
        context.target_price_source = target_price_source;
        context.target_gap_bps = target_gap_bps;
        context.current_spot_price = latest.price;
        context.spot_move_bps = target_gap_bps;
        context.dominant_outcome = dominant_outcome_label(latest.price, target_price);
        true
    }

    /// Resolve a finished window using cached Chainlink RTDS quotes when available.
    pub async fn resolution_from_slug(&self, slug: &str) -> Option<MarketWindowResolution> {
        let settings = *self.settings.read().await;
        if !settings.enabled {
            return None;
        }

        let (target, start_ts) = target_and_start_from_slug(slug)?;
        if Utc::now().timestamp() < start_ts.saturating_add(target.window_secs()) {
            return None;
        }

        let window = {
            let state = self.state.read().await;
            state
                .windows
                .get(&window_cache_key(target, start_ts))
                .copied()
        }?;
        let start_quote = window.start_quote;
        let close_quote = window.latest_quote?;
        let start_price = if let Some(target_price) = window.explicit_target_price {
            target_price
        } else {
            let start_quote = start_quote?;
            let start_lag_ms = start_quote
                .event_time_ms
                .saturating_sub(start_ts.saturating_mul(1_000))
                .abs();
            if start_lag_ms > settings.max_window_open_lag_ms {
                return None;
            }
            start_quote.price
        };

        let end_ms = start_ts
            .saturating_add(target.window_secs())
            .saturating_mul(1_000);
        let close_lag_ms = end_ms.saturating_sub(close_quote.event_time_ms).abs();
        if close_quote.event_time_ms > end_ms || close_lag_ms > settings.max_settlement_close_lag_ms
        {
            return None;
        }

        market_window_resolution_from_prices(
            target,
            start_price,
            close_quote.price,
            close_quote.event_time_ms,
        )
    }

    async fn run_stream_loop(self, websocket_url: String, symbols: Vec<String>) {
        loop {
            match connect_async(&websocket_url).await {
                Ok((stream, _response)) => {
                    info!(
                        symbols = %symbols.join(","),
                        "connected Polymarket RTDS Chainlink oracle stream"
                    );
                    let (mut writer, mut reader) = stream.split();
                    let subscribe_message = chainlink_subscribe_message(&symbols);

                    if let Err(error) = writer
                        .send(Message::Text(subscribe_message.to_string().into()))
                        .await
                    {
                        warn!(
                            error = %error,
                            "failed to subscribe Polymarket RTDS Chainlink stream"
                        );
                        sleep(Duration::from_millis(STREAM_RECONNECT_DELAY_MS)).await;
                        continue;
                    }

                    let mut ping = interval(Duration::from_millis(RTDS_PING_INTERVAL_MS));
                    loop {
                        tokio::select! {
                            _ = ping.tick() => {
                                if let Err(error) = writer.send(Message::Ping(Vec::new().into())).await {
                                    warn!(
                                        error = %error,
                                        "failed to ping Polymarket RTDS Chainlink stream"
                                    );
                                    break;
                                }
                            }
                            next_message = reader.next() => {
                                match next_message {
                                    Some(Ok(Message::Text(payload))) => {
                                        if let Err(error) = self.handle_text_message(payload.as_ref()).await {
                                            warn!(
                                                error = %error,
                                                "failed to handle Polymarket RTDS Chainlink message"
                                            );
                                        }
                                    }
                                    Some(Ok(Message::Close(frame))) => {
                                        info!(?frame, "Polymarket RTDS Chainlink stream closed");
                                        break;
                                    }
                                    Some(Ok(_)) => {}
                                    Some(Err(error)) => {
                                        warn!(
                                            error = %error,
                                            "Polymarket RTDS Chainlink stream error"
                                        );
                                        break;
                                    }
                                    None => break,
                                }
                            }
                        }
                    }
                }
                Err(error) => {
                    warn!(
                        error = %error,
                        "failed to connect Polymarket RTDS Chainlink stream"
                    );
                }
            }

            sleep(Duration::from_millis(STREAM_RECONNECT_DELAY_MS)).await;
        }
    }

    async fn handle_text_message(&self, payload: &str) -> Result<()> {
        let payload = payload.trim();
        if payload.is_empty()
            || payload.eq_ignore_ascii_case("ping")
            || payload.eq_ignore_ascii_case("pong")
        {
            return Ok(());
        }

        let message = serde_json::from_str::<RtdsEnvelope>(payload)?;
        match message.topic.as_str() {
            "crypto_prices_chainlink" => self.handle_update_message(message).await,
            "crypto_prices" => self.handle_snapshot_message(message).await,
            _ => Ok(()),
        }
    }

    async fn handle_update_message(&self, message: RtdsEnvelope) -> Result<()> {
        let payload = serde_json::from_value::<RtdsChainlinkPricePayload>(message.payload)?;
        let symbol = normalize_chainlink_symbol(&payload.symbol);
        let Some(price) = valid_chainlink_price(&symbol, &payload.value)? else {
            return Ok(());
        };
        let received_time_ms = Utc::now().timestamp_millis();
        let event_time_ms = clamp_chainlink_event_time_ms(
            payload.timestamp.unwrap_or(message.timestamp),
            received_time_ms,
        );
        let quote = ChainlinkOracleQuote {
            event_time_ms,
            received_time_ms,
            price,
        };
        self.ingest_quote(&symbol, quote, true).await;
        Ok(())
    }

    async fn handle_snapshot_message(&self, message: RtdsEnvelope) -> Result<()> {
        let payload = serde_json::from_value::<RtdsChainlinkSnapshotPayload>(message.payload)?;
        let symbol = normalize_chainlink_symbol(&payload.symbol);
        if symbol.is_empty() {
            return Ok(());
        }

        let received_time_ms = Utc::now().timestamp_millis();
        let mut latest_quote = None;
        for point in payload.data {
            let Some(price) = valid_chainlink_price(&symbol, &point.value)? else {
                continue;
            };
            let quote = ChainlinkOracleQuote {
                event_time_ms: clamp_chainlink_event_time_ms(point.timestamp, received_time_ms),
                received_time_ms,
                price,
            };
            if latest_quote.is_none_or(|latest: ChainlinkOracleQuote| {
                quote.event_time_ms >= latest.event_time_ms
            }) {
                latest_quote = Some(quote);
            }
            self.ingest_quote(&symbol, quote, false).await;
        }

        if let Some(quote) = latest_quote {
            self.emit_quote_trigger(&symbol, quote);
        }

        Ok(())
    }

    async fn ingest_quote(&self, symbol: &str, quote: ChainlinkOracleQuote, emit_trigger: bool) {
        let symbol = normalize_chainlink_symbol(symbol);
        let mut state = self.state.write().await;
        let quotes = state
            .quotes_by_symbol
            .entry(symbol.clone())
            .or_insert_with(VecDeque::new);
        insert_chainlink_quote_ordered(quotes, quote);
        prune_quote_history(quotes, quote.event_time_ms);

        for target in SUPPORTED_TARGETS {
            if target.polymarket_chainlink_symbol() != Some(symbol.as_str()) {
                continue;
            }
            cache_window_quote(&mut state.windows, target, quote);
        }

        prune_window_cache(&mut state.windows);
        drop(state);

        if emit_trigger {
            self.emit_quote_trigger(&symbol, quote);
        }
        debug!(
            symbol = %symbol,
            price = %quote.price,
            event_time_ms = quote.event_time_ms,
            "updated Chainlink RTDS oracle cache"
        );
    }

    fn emit_quote_trigger(&self, symbol: &str, quote: ChainlinkOracleQuote) {
        let _ = self.trigger_tx.send(Some(ChainlinkOracleTriggerEvent {
            symbol: normalize_chainlink_symbol(symbol),
            event_time_ms: quote.event_time_ms,
            received_time_ms: quote.received_time_ms,
            price: quote.price,
        }));
    }

    async fn record_market_target_price(
        &self,
        target: MarketTarget,
        start_ts: i64,
        target_price: Decimal,
    ) {
        if target_price <= Decimal::ZERO {
            return;
        }

        let mut state = self.state.write().await;
        let end_ts = start_ts.saturating_add(target.window_secs());
        let entry = state
            .windows
            .entry(window_cache_key(target, start_ts))
            .or_insert_with(|| ChainlinkWindowCache {
                start_quote: None,
                latest_quote: None,
                explicit_target_price: None,
                expires_at_ts: end_ts.saturating_add(WINDOW_CACHE_GRACE_SECS),
            });
        entry.explicit_target_price = Some(target_price);
        entry.expires_at_ts = entry
            .expires_at_ts
            .max(end_ts.saturating_add(WINDOW_CACHE_GRACE_SECS));
    }

    async fn latest_fresh_quote(
        &self,
        symbol: &str,
        max_quote_age_ms: i64,
    ) -> Option<ChainlinkOracleQuote> {
        let now_ms = Utc::now().timestamp_millis();
        let state = self.state.read().await;
        state
            .quotes_by_symbol
            .get(&normalize_chainlink_symbol(symbol))?
            .back()
            .copied()
            .filter(|quote| {
                let event_age_ms = now_ms.saturating_sub(quote.event_time_ms);
                let received_age_ms = now_ms.saturating_sub(quote.received_time_ms);
                event_age_ms >= 0
                    && event_age_ms <= max_quote_age_ms
                    && received_age_ms >= 0
                    && received_age_ms <= max_quote_age_ms
            })
    }

    async fn window_open_quote(
        &self,
        target: MarketTarget,
        start_ts: i64,
        max_open_lag_ms: i64,
    ) -> Option<ChainlinkOracleQuote> {
        let state = self.state.read().await;
        let quote = state
            .windows
            .get(&window_cache_key(target, start_ts))?
            .start_quote?;
        let open_lag_ms = quote
            .event_time_ms
            .saturating_sub(start_ts.saturating_mul(1_000))
            .abs();
        (open_lag_ms <= max_open_lag_ms).then_some(quote)
    }

    #[cfg(test)]
    pub(crate) async fn ingest_test_quote(&self, symbol: &str, event_time_ms: i64, price: Decimal) {
        self.ingest_quote(
            symbol,
            ChainlinkOracleQuote {
                event_time_ms,
                received_time_ms: Utc::now().timestamp_millis(),
                price,
            },
            true,
        )
        .await;
    }

    #[cfg(test)]
    pub(crate) async fn set_test_settings(&self, settings: ChainlinkOracleConfig) {
        self.configure(settings).await;
    }
}

#[derive(Debug, Deserialize)]
struct RtdsEnvelope {
    topic: String,
    #[serde(default)]
    timestamp: i64,
    payload: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct RtdsChainlinkPricePayload {
    symbol: String,
    #[serde(default)]
    timestamp: Option<i64>,
    value: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct RtdsChainlinkSnapshotPayload {
    symbol: String,
    #[serde(default)]
    data: Vec<RtdsChainlinkSnapshotPoint>,
}

#[derive(Debug, Deserialize)]
struct RtdsChainlinkSnapshotPoint {
    timestamp: i64,
    value: serde_json::Value,
}

fn configured_chainlink_symbols(targets: &[MarketTarget]) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut symbols = Vec::new();
    for target in targets {
        let Some(symbol) = target.polymarket_chainlink_symbol() else {
            continue;
        };
        if seen.insert(symbol) {
            symbols.push(symbol.to_owned());
        }
    }
    symbols
}

fn chainlink_subscribe_message(symbols: &[String]) -> serde_json::Value {
    let subscriptions = symbols
        .iter()
        .map(|symbol| {
            serde_json::json!({
                "topic": "crypto_prices_chainlink",
                "type": "*",
                "filters": format!(r#"{{"symbol":"{}"}}"#, symbol),
            })
        })
        .collect::<Vec<_>>();

    serde_json::json!({
        "action": "subscribe",
        "subscriptions": subscriptions,
    })
}

fn cache_window_quote(
    windows: &mut HashMap<String, ChainlinkWindowCache>,
    target: MarketTarget,
    quote: ChainlinkOracleQuote,
) {
    if quote.event_time_ms < 0 {
        return;
    }

    let quote_ts = quote.event_time_ms.div_euclid(1_000);
    let start_ts = target.window_start_ts_at(quote_ts);
    let end_ts = start_ts.saturating_add(target.window_secs());
    if quote_ts < start_ts || quote_ts >= end_ts {
        return;
    }

    let entry = windows
        .entry(window_cache_key(target, start_ts))
        .or_insert_with(|| ChainlinkWindowCache {
            start_quote: None,
            latest_quote: None,
            explicit_target_price: None,
            expires_at_ts: end_ts.saturating_add(WINDOW_CACHE_GRACE_SECS),
        });

    if entry
        .start_quote
        .is_none_or(|existing| quote.event_time_ms < existing.event_time_ms)
    {
        entry.start_quote = Some(quote);
    }
    if entry
        .latest_quote
        .is_none_or(|existing| quote.event_time_ms >= existing.event_time_ms)
    {
        entry.latest_quote = Some(quote);
    }
}

fn insert_chainlink_quote_ordered(
    quotes: &mut VecDeque<ChainlinkOracleQuote>,
    quote: ChainlinkOracleQuote,
) {
    if let Some(existing_index) = quotes
        .iter()
        .position(|existing| existing.event_time_ms == quote.event_time_ms)
    {
        let existing = quotes
            .get_mut(existing_index)
            .expect("index came from the same quote deque");
        if existing.price != quote.price {
            existing.price = quote.price;
        }
        return;
    }

    let insert_at = quotes
        .iter()
        .position(|existing| existing.event_time_ms > quote.event_time_ms)
        .unwrap_or(quotes.len());
    quotes.insert(insert_at, quote);
}

fn prune_quote_history(quotes: &mut VecDeque<ChainlinkOracleQuote>, latest_event_time_ms: i64) {
    let newest_event_time_ms = quotes
        .back()
        .map_or(latest_event_time_ms, |quote| quote.event_time_ms)
        .max(latest_event_time_ms);
    let keep_from_ms = newest_event_time_ms.saturating_sub(QUOTE_HISTORY_RETENTION_MS);
    while quotes
        .front()
        .is_some_and(|quote| quote.event_time_ms < keep_from_ms)
    {
        let _dropped = quotes.pop_front();
    }
}

fn prune_window_cache(windows: &mut HashMap<String, ChainlinkWindowCache>) {
    let now_ts = Utc::now().timestamp();
    if windows.len() > WINDOW_CACHE_MAX_ENTRIES {
        windows.retain(|_, window| window.expires_at_ts >= now_ts);
        if windows.len() > WINDOW_CACHE_MAX_ENTRIES {
            windows.clear();
        }
    }
}

const fn clamp_chainlink_event_time_ms(event_time_ms: i64, received_time_ms: i64) -> i64 {
    if event_time_ms < received_time_ms {
        event_time_ms
    } else {
        received_time_ms
    }
}

fn target_and_start_from_slug(slug: &str) -> Option<(MarketTarget, i64)> {
    SUPPORTED_TARGETS.into_iter().find_map(|target| {
        slug.strip_prefix(target.slug_prefix())
            .and_then(|start| start.parse::<i64>().ok())
            .map(|start_ts| (target, start_ts))
    })
}

fn window_cache_key(target: MarketTarget, start_ts: i64) -> String {
    format!("{}:{start_ts}", target.as_key())
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

    Some(MarketWindowResolution {
        target,
        start_price,
        end_price,
        realized_move_bps: move_bps(end_price, start_price),
        actual_outcome,
        resolved_at_ms,
    })
}

fn dominant_outcome_label(current_price: Decimal, target_price: Decimal) -> String {
    if current_price >= target_price {
        "Up"
    } else {
        "Down"
    }
    .to_owned()
}

fn move_bps(current: Decimal, reference: Decimal) -> Decimal {
    if reference <= Decimal::ZERO {
        return Decimal::ZERO;
    }

    ((current - reference) / reference * Decimal::from(10_000_u32)).round_dp(4)
}

fn normalize_chainlink_symbol(symbol: &str) -> String {
    symbol.trim().to_ascii_lowercase()
}

fn decimal_from_json_value(value: &serde_json::Value) -> Option<Decimal> {
    match value {
        serde_json::Value::String(inner) => Decimal::from_str(inner).ok(),
        serde_json::Value::Number(inner) => Decimal::from_str(&inner.to_string()).ok(),
        _ => None,
    }
}

fn valid_chainlink_price(symbol: &str, value: &serde_json::Value) -> Result<Option<Decimal>> {
    if symbol.is_empty() {
        return Ok(None);
    }

    let Some(price) = decimal_from_json_value(value) else {
        return Err(AppError::InvalidMarket(format!(
            "Chainlink RTDS returned invalid price for `{symbol}`"
        )));
    };

    Ok((price > Decimal::ZERO).then_some(price))
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use crate::config::ChainlinkOracleConfig;
    use crate::models::{BinaryMarket, MarketTarget, TargetPriceSource};
    use crate::services::binance::{MarketWindowContext, WindowDirection};

    use super::ChainlinkOracleCache;

    fn decimal(value: &str) -> Decimal {
        value.parse::<Decimal>().expect("valid decimal")
    }

    fn test_settings() -> ChainlinkOracleConfig {
        ChainlinkOracleConfig {
            enabled: true,
            max_quote_age_ms: i64::MAX,
            max_window_open_lag_ms: 3_000,
            max_settlement_close_lag_ms: 5_000,
        }
    }

    fn test_market() -> BinaryMarket {
        BinaryMarket {
            condition_id: "0xabc".to_owned(),
            slug: "btc-updown-5m-900".to_owned(),
            question: "BTC Up or Down".to_owned(),
            outcome_a_label: "Up".to_owned(),
            outcome_a_token_id: "up".to_owned(),
            outcome_b_label: "Down".to_owned(),
            outcome_b_token_id: "down".to_owned(),
            end_date: None,
            liquidity_usdc: Decimal::ZERO,
            target_price: Some(decimal("100")),
            target_price_source: Some(TargetPriceSource::PolymarketEventMetadata),
            final_reference_price: None,
        }
    }

    fn test_context() -> MarketWindowContext {
        MarketWindowContext {
            target: MarketTarget::Btc5m,
            interval_open_price: decimal("99"),
            target_price: decimal("99"),
            target_price_source: TargetPriceSource::BinanceWindowOpenFallback,
            target_gap_bps: Decimal::ZERO,
            current_spot_price: decimal("99"),
            current_spot_source: "Binance::Trade".to_owned(),
            current_spot_event_age_ms: Some(1),
            current_spot_received_age_ms: Some(1),
            current_spot_quote_points: Some(1),
            exchange_book_age_ms: Some(1),
            exchange_book_top_imbalance_bps: Decimal::ZERO,
            exchange_book_depth_imbalance_bps: Decimal::ZERO,
            exchange_book_microprice_bps: Decimal::ZERO,
            exchange_book_spread_bps: Decimal::ZERO,
            micro_burst_reference_price: decimal("99"),
            micro_reference_price: decimal("99"),
            spot_move_bps: Decimal::ZERO,
            spot_move_1s_bps: Decimal::ZERO,
            spot_move_5s_bps: Decimal::ZERO,
            spot_move_15s_bps: Decimal::ZERO,
            micro_acceleration_bps: Decimal::ZERO,
            dominant_outcome: "Down".to_owned(),
            seconds_left: 250,
        }
    }

    #[tokio::test]
    async fn decorate_context_prefers_polymarket_target_with_chainlink_live_price() {
        let cache = ChainlinkOracleCache::default();
        cache.set_test_settings(test_settings()).await;
        cache
            .ingest_test_quote("btc/usd", 901_000, decimal("101.2"))
            .await;
        let mut context = test_context();

        assert!(cache.decorate_context(&test_market(), &mut context).await);

        assert_eq!(context.interval_open_price, decimal("100"));
        assert_eq!(context.target_price, decimal("100"));
        assert_eq!(
            context.target_price_source,
            TargetPriceSource::PolymarketEventMetadata
        );
        assert_eq!(context.current_spot_price, decimal("101.2"));
        assert_eq!(context.current_spot_source, "Binance::Trade");
        assert_eq!(context.current_spot_received_age_ms, Some(1));
        assert_eq!(context.current_spot_quote_points, Some(1));
        assert_eq!(context.target_gap_bps, decimal("120.0000"));
        assert_eq!(context.spot_move_bps, decimal("120.0000"));
    }

    #[tokio::test]
    async fn snapshot_message_seeds_chainlink_quote_history() {
        let cache = ChainlinkOracleCache::default();
        cache.set_test_settings(test_settings()).await;
        cache
            .handle_text_message(
                r#"{"payload":{"data":[{"timestamp":901000,"value":101.1},{"timestamp":902000,"value":101.2}],"symbol":"btc/usd"},"timestamp":902500,"topic":"crypto_prices","type":"subscribe"}"#,
            )
            .await
            .expect("snapshot parses");
        let mut context = test_context();

        assert!(cache.decorate_context(&test_market(), &mut context).await);
        assert_eq!(context.current_spot_price, decimal("101.2"));
        assert_eq!(context.current_spot_source, "Binance::Trade");
        assert_eq!(context.current_spot_quote_points, Some(1));
    }

    #[tokio::test]
    async fn out_of_order_chainlink_quote_does_not_replace_latest_price() {
        let cache = ChainlinkOracleCache::default();
        cache.set_test_settings(test_settings()).await;
        cache
            .ingest_test_quote("btc/usd", 902_000, decimal("102"))
            .await;
        cache
            .ingest_test_quote("btc/usd", 901_000, decimal("101"))
            .await;

        let views = cache.latest_price_views(&[MarketTarget::Btc5m]).await;

        assert_eq!(
            views[0].quote.map(|quote| quote.price),
            Some(decimal("102"))
        );
    }

    #[tokio::test]
    async fn replayed_chainlink_quote_does_not_become_fresh_on_receive() {
        let cache = ChainlinkOracleCache::default();
        let mut settings = test_settings();
        settings.max_quote_age_ms = 100;
        cache.set_test_settings(settings).await;
        cache
            .ingest_test_quote("btc/usd", 901_000, decimal("101"))
            .await;
        let mut context = test_context();

        assert!(!cache.decorate_context(&test_market(), &mut context).await);
    }

    #[tokio::test]
    async fn text_heartbeat_frames_are_ignored() {
        let cache = ChainlinkOracleCache::default();
        cache.set_test_settings(test_settings()).await;

        cache
            .handle_text_message("")
            .await
            .expect("empty frames are ignored");
        cache
            .handle_text_message("PONG")
            .await
            .expect("pong frames are ignored");
    }

    #[tokio::test]
    async fn resolution_uses_cached_chainlink_window_quotes() {
        let cache = ChainlinkOracleCache::default();
        cache.set_test_settings(test_settings()).await;
        cache
            .ingest_test_quote("btc/usd", 900_500, decimal("100"))
            .await;
        cache
            .ingest_test_quote("btc/usd", 1_199_000, decimal("101"))
            .await;

        let resolution = cache
            .resolution_from_slug("btc-updown-5m-900")
            .await
            .expect("resolution");

        assert_eq!(resolution.start_price, decimal("100"));
        assert_eq!(resolution.end_price, decimal("101"));
        assert_eq!(resolution.actual_outcome, WindowDirection::Up);
    }

    #[tokio::test]
    async fn resolution_can_use_polymarket_target_when_stream_started_late() {
        let cache = ChainlinkOracleCache::default();
        cache.set_test_settings(test_settings()).await;
        cache
            .ingest_test_quote("btc/usd", 910_000, decimal("100.5"))
            .await;
        let mut context = test_context();
        assert!(cache.decorate_context(&test_market(), &mut context).await);
        cache
            .ingest_test_quote("btc/usd", 1_199_000, decimal("101"))
            .await;

        let resolution = cache
            .resolution_from_slug("btc-updown-5m-900")
            .await
            .expect("resolution");

        assert_eq!(resolution.start_price, decimal("100"));
        assert_eq!(resolution.end_price, decimal("101"));
        assert_eq!(resolution.actual_outcome, WindowDirection::Up);
    }

    #[tokio::test]
    async fn resolution_rejects_late_open_quote() {
        let cache = ChainlinkOracleCache::default();
        cache.set_test_settings(test_settings()).await;
        cache
            .ingest_test_quote("btc/usd", 910_000, decimal("100"))
            .await;
        cache
            .ingest_test_quote("btc/usd", 1_199_000, decimal("101"))
            .await;

        assert!(
            cache
                .resolution_from_slug("btc-updown-5m-900")
                .await
                .is_none()
        );
    }
}
