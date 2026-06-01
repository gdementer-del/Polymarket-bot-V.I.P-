//! Coinbase Exchange live market-data sidecar.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::time::sleep;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};

use crate::error::{AppError, Result};
use crate::models::MarketTarget;

use super::binance::{BinanceClient, clamp_live_event_time_ms};

const STREAM_RECONNECT_DELAY_MS: u64 = 300;
type CoinbaseL2Level = (Decimal, Decimal);
type CoinbaseL2TopBook = (Vec<CoinbaseL2Level>, Vec<CoinbaseL2Level>);

/// Coinbase stream feeding vetted secondary quotes and L2 pressure into the primary spot cache.
#[derive(Debug, Clone)]
pub struct CoinbaseClient {
    websocket_url: String,
    binance_client: BinanceClient,
    started_products: Arc<Mutex<HashSet<String>>>,
    l2_books: Arc<Mutex<HashMap<String, CoinbaseL2Book>>>,
    latest_tickers: Arc<Mutex<HashMap<String, CoinbaseTickerPrice>>>,
    max_source_disagreement_bps: Decimal,
    max_spread_bps: Decimal,
}

/// Latest raw Coinbase ticker price accepted by Coinbase-side spread checks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoinbaseTickerPriceView {
    pub product_id: String,
    pub symbol: String,
    pub price: Decimal,
    pub event_age_ms: i64,
    pub received_age_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CoinbaseTickerPrice {
    product_id: String,
    symbol: String,
    price: Decimal,
    event_time_ms: i64,
    received_time_ms: i64,
}

impl CoinbaseClient {
    /// Create a Coinbase Exchange market-data sidecar.
    #[must_use]
    pub fn new(
        websocket_url: String,
        binance_client: BinanceClient,
        max_source_disagreement_bps: Decimal,
        max_spread_bps: Decimal,
    ) -> Self {
        Self {
            websocket_url,
            binance_client,
            started_products: Arc::new(Mutex::new(HashSet::new())),
            l2_books: Arc::new(Mutex::new(HashMap::new())),
            latest_tickers: Arc::new(Mutex::new(HashMap::new())),
            max_source_disagreement_bps,
            max_spread_bps,
        }
    }

    /// Return latest raw Coinbase ticker prices observed by this sidecar.
    #[must_use]
    pub fn latest_ticker_price_views(&self) -> Vec<CoinbaseTickerPriceView> {
        let now_ms = Utc::now().timestamp_millis();
        let Ok(tickers) = self.latest_tickers.lock() else {
            warn!("Coinbase latest_tickers mutex is poisoned; cannot read price monitor cache");
            return Vec::new();
        };

        let mut views = tickers
            .values()
            .map(|ticker| CoinbaseTickerPriceView {
                product_id: ticker.product_id.clone(),
                symbol: ticker.symbol.clone(),
                price: ticker.price,
                event_age_ms: now_ms.saturating_sub(ticker.event_time_ms),
                received_age_ms: now_ms.saturating_sub(ticker.received_time_ms),
            })
            .collect::<Vec<_>>();
        views.sort_by(|left, right| left.symbol.cmp(&right.symbol));
        views
    }

    /// Start a reconnecting Coinbase ticker stream for a supported market target.
    #[must_use]
    pub fn start_ticker_stream(&self, target: MarketTarget) -> bool {
        let product_id = target.coinbase_product_id().to_owned();
        let binance_symbol = target.binance_symbol().to_owned();
        if product_id.is_empty() || binance_symbol.is_empty() {
            return false;
        }

        {
            let Ok(mut started) = self.started_products.lock() else {
                warn!("Coinbase started_products mutex is poisoned; skipping ticker stream start");
                return false;
            };
            if !started.insert(product_id.clone()) {
                return true;
            }
        }

        install_rustls_crypto_provider();

        let client = self.clone();
        std::mem::drop(tokio::spawn(async move {
            client.run_ticker_stream(product_id, binance_symbol).await;
        }));

        true
    }

    async fn run_ticker_stream(self, product_id: String, binance_symbol: String) {
        loop {
            match connect_async(&self.websocket_url).await {
                Ok((stream, _response)) => {
                    info!(product_id = %product_id, "connected Coinbase ticker stream");
                    let (mut writer, mut reader) = stream.split();
                    let subscribe_message = serde_json::json!({
                        "type": "subscribe",
                        "product_ids": [product_id.as_str()],
                        "channels": ["ticker", "level2_batch", "heartbeat"],
                    });

                    if let Err(error) = writer
                        .send(Message::Text(subscribe_message.to_string().into()))
                        .await
                    {
                        warn!(
                            product_id = %product_id,
                            error = %error,
                            "failed to subscribe Coinbase ticker stream"
                        );
                        sleep(Duration::from_millis(STREAM_RECONNECT_DELAY_MS)).await;
                        continue;
                    }

                    while let Some(next_message) = reader.next().await {
                        match next_message {
                            Ok(Message::Text(payload)) => {
                                if let Err(error) = self
                                    .handle_text_message(
                                        &product_id,
                                        &binance_symbol,
                                        payload.as_ref(),
                                    )
                                    .await
                                {
                                    warn!(
                                        product_id = %product_id,
                                        error = %error,
                                        "failed to handle Coinbase websocket message"
                                    );
                                }
                            }
                            Ok(Message::Close(frame)) => {
                                info!(product_id = %product_id, ?frame, "Coinbase ticker stream closed");
                                break;
                            }
                            Ok(_) => {}
                            Err(error) => {
                                warn!(
                                    product_id = %product_id,
                                    error = %error,
                                    "Coinbase ticker stream error"
                                );
                                break;
                            }
                        }
                    }
                }
                Err(error) => {
                    warn!(
                        product_id = %product_id,
                        error = %error,
                        "failed to connect Coinbase ticker stream"
                    );
                }
            }

            sleep(Duration::from_millis(STREAM_RECONNECT_DELAY_MS)).await;
        }
    }

    async fn handle_text_message(
        &self,
        expected_product_id: &str,
        binance_symbol: &str,
        payload: &str,
    ) -> Result<()> {
        let envelope = serde_json::from_str::<CoinbaseEnvelope>(payload)?;
        match envelope.message_type.as_str() {
            "ticker" => {
                let ticker = serde_json::from_str::<CoinbaseTickerMessage>(payload)?;
                if !ticker.product_id.eq_ignore_ascii_case(expected_product_id) {
                    return Ok(());
                }
                let Some(selected_price) = ticker.selected_price(self.max_spread_bps)? else {
                    return Ok(());
                };
                let event_time_ms = ticker.event_time_ms();
                self.store_latest_ticker(
                    expected_product_id,
                    binance_symbol,
                    event_time_ms,
                    selected_price.price,
                );
                let _accepted = self
                    .binance_client
                    .ingest_coinbase_ticker_quote(
                        binance_symbol,
                        event_time_ms,
                        selected_price.price,
                        self.max_source_disagreement_bps,
                    )
                    .await?;
            }
            "snapshot" => {
                let snapshot = serde_json::from_str::<CoinbaseL2SnapshotMessage>(payload)?;
                if !snapshot
                    .product_id
                    .eq_ignore_ascii_case(expected_product_id)
                {
                    return Ok(());
                }
                let top_book = {
                    let Ok(mut books) = self.l2_books.lock() else {
                        warn!("Coinbase L2 book mutex is poisoned; skipping snapshot");
                        return Ok(());
                    };
                    let book = books.entry(expected_product_id.to_owned()).or_default();
                    book.apply_snapshot(&snapshot)?;
                    book.top_levels(5)
                };
                self.publish_coinbase_l2_book(binance_symbol, snapshot.event_time_ms(), top_book)
                    .await?;
            }
            "l2update" => {
                let update = serde_json::from_str::<CoinbaseL2UpdateMessage>(payload)?;
                if !update.product_id.eq_ignore_ascii_case(expected_product_id) {
                    return Ok(());
                }
                let top_book = {
                    let Ok(mut books) = self.l2_books.lock() else {
                        warn!("Coinbase L2 book mutex is poisoned; skipping update");
                        return Ok(());
                    };
                    let book = books.entry(expected_product_id.to_owned()).or_default();
                    book.apply_update(&update)?;
                    book.top_levels(5)
                };
                self.publish_coinbase_l2_book(binance_symbol, update.event_time_ms(), top_book)
                    .await?;
            }
            "heartbeat" | "subscriptions" => {}
            "error" => {
                let error = serde_json::from_str::<CoinbaseErrorMessage>(payload)?;
                warn!(
                    message = %error.message.unwrap_or_else(|| "unknown Coinbase websocket error".to_owned()),
                    "Coinbase websocket returned error"
                );
            }
            _ => {}
        }

        Ok(())
    }

    fn store_latest_ticker(
        &self,
        product_id: &str,
        binance_symbol: &str,
        event_time_ms: i64,
        price: Decimal,
    ) {
        let Ok(mut tickers) = self.latest_tickers.lock() else {
            warn!("Coinbase latest_tickers mutex is poisoned; skipping price monitor update");
            return;
        };
        let received_time_ms = Utc::now().timestamp_millis();
        let event_time_ms = clamp_live_event_time_ms(event_time_ms, received_time_ms);
        if tickers
            .get(product_id)
            .is_some_and(|ticker| ticker.event_time_ms >= event_time_ms)
        {
            return;
        }
        tickers.insert(
            product_id.to_owned(),
            CoinbaseTickerPrice {
                product_id: product_id.to_owned(),
                symbol: binance_symbol.to_owned(),
                price,
                event_time_ms,
                received_time_ms,
            },
        );
    }

    async fn publish_coinbase_l2_book(
        &self,
        binance_symbol: &str,
        event_time_ms: i64,
        top_book: Option<CoinbaseL2TopBook>,
    ) -> Result<()> {
        let Some((bids, asks)) = top_book else {
            return Ok(());
        };
        let _accepted = self
            .binance_client
            .ingest_coinbase_l2_book(
                binance_symbol,
                event_time_ms,
                bids,
                asks,
                self.max_source_disagreement_bps,
            )
            .await?;
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct CoinbaseEnvelope {
    #[serde(rename = "type")]
    message_type: String,
}

#[derive(Debug, Deserialize)]
struct CoinbaseTickerMessage {
    product_id: String,
    price: String,
    time: Option<String>,
    best_bid: Option<String>,
    best_ask: Option<String>,
}

impl CoinbaseTickerMessage {
    fn selected_price(&self, max_spread_bps: Decimal) -> Result<Option<CoinbaseSelectedPrice>> {
        let last_price = parse_decimal("coinbase.ticker.price", &self.price)?;
        let best_bid = self
            .best_bid
            .as_deref()
            .map(|value| parse_decimal("coinbase.ticker.best_bid", value))
            .transpose()?;
        let best_ask = self
            .best_ask
            .as_deref()
            .map(|value| parse_decimal("coinbase.ticker.best_ask", value))
            .transpose()?;

        let Some((bid, ask)) = best_bid.zip(best_ask) else {
            return Ok(Some(CoinbaseSelectedPrice { price: last_price }));
        };
        if bid <= Decimal::ZERO || ask <= Decimal::ZERO || ask < bid {
            return Ok(Some(CoinbaseSelectedPrice { price: last_price }));
        }

        let mid = ((bid + ask) / Decimal::from(2_u32)).round_dp(8);
        if mid <= Decimal::ZERO {
            return Ok(Some(CoinbaseSelectedPrice { price: last_price }));
        }

        let spread_bps = ((ask - bid) / mid * Decimal::from(10_000_u32)).round_dp(4);
        if spread_bps > max_spread_bps {
            return Ok(None);
        }

        Ok(Some(CoinbaseSelectedPrice { price: mid }))
    }

    fn event_time_ms(&self) -> i64 {
        self.time
            .as_deref()
            .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
            .map_or_else(
                || Utc::now().timestamp_millis(),
                |timestamp| timestamp.timestamp_millis(),
            )
    }
}

#[derive(Debug, Clone, Copy)]
struct CoinbaseSelectedPrice {
    price: Decimal,
}

#[derive(Debug, Deserialize)]
struct CoinbaseL2SnapshotMessage {
    product_id: String,
    bids: Vec<Vec<String>>,
    asks: Vec<Vec<String>>,
    time: Option<String>,
}

impl CoinbaseL2SnapshotMessage {
    fn event_time_ms(&self) -> i64 {
        coinbase_event_time_ms(self.time.as_deref())
    }
}

#[derive(Debug, Deserialize)]
struct CoinbaseL2UpdateMessage {
    product_id: String,
    changes: Vec<Vec<String>>,
    time: Option<String>,
}

impl CoinbaseL2UpdateMessage {
    fn event_time_ms(&self) -> i64 {
        coinbase_event_time_ms(self.time.as_deref())
    }
}

#[derive(Debug, Default, Clone)]
struct CoinbaseL2Book {
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
}

impl CoinbaseL2Book {
    fn apply_snapshot(&mut self, snapshot: &CoinbaseL2SnapshotMessage) -> Result<()> {
        self.bids = parse_l2_side("coinbase.l2.snapshot.bid", &snapshot.bids)?;
        self.asks = parse_l2_side("coinbase.l2.snapshot.ask", &snapshot.asks)?;
        Ok(())
    }

    fn apply_update(&mut self, update: &CoinbaseL2UpdateMessage) -> Result<()> {
        for change in &update.changes {
            let side = change.first().map(String::as_str).unwrap_or_default();
            let price = change.get(1).ok_or_else(|| {
                AppError::InvalidMarket("Coinbase L2 update missing price".to_owned())
            })?;
            let size = change.get(2).ok_or_else(|| {
                AppError::InvalidMarket("Coinbase L2 update missing size".to_owned())
            })?;
            let price = parse_decimal("coinbase.l2.update.price", price)?;
            let size = parse_decimal("coinbase.l2.update.size", size)?;
            if price <= Decimal::ZERO {
                return Err(AppError::InvalidMarket(format!(
                    "Coinbase L2 update returned non-positive price: `{price}`"
                )));
            }

            let levels = match side {
                "buy" => &mut self.bids,
                "sell" => &mut self.asks,
                _ => continue,
            };
            if size <= Decimal::ZERO {
                levels.remove(&price);
            } else {
                levels.insert(price, size);
            }
        }
        Ok(())
    }

    fn top_levels(&self, limit: usize) -> Option<CoinbaseL2TopBook> {
        let bids = self
            .bids
            .iter()
            .rev()
            .take(limit)
            .map(|(price, size)| (*price, *size))
            .collect::<Vec<_>>();
        let asks = self
            .asks
            .iter()
            .take(limit)
            .map(|(price, size)| (*price, *size))
            .collect::<Vec<_>>();

        if bids.is_empty() || asks.is_empty() {
            return None;
        }

        Some((bids, asks))
    }
}

#[derive(Debug, Deserialize)]
struct CoinbaseErrorMessage {
    message: Option<String>,
}

fn parse_l2_side(
    field: &'static str,
    raw_levels: &[Vec<String>],
) -> Result<BTreeMap<Decimal, Decimal>> {
    let mut levels = BTreeMap::new();
    for raw_level in raw_levels {
        let Some(price) = raw_level.first() else {
            continue;
        };
        let Some(size) = raw_level.get(1) else {
            continue;
        };
        let price = parse_decimal(field, price)?;
        let size = parse_decimal(field, size)?;
        if price <= Decimal::ZERO || size <= Decimal::ZERO {
            continue;
        }
        levels.insert(price, size);
    }
    Ok(levels)
}

fn coinbase_event_time_ms(value: Option<&str>) -> i64 {
    value
        .and_then(|timestamp| DateTime::parse_from_rfc3339(timestamp).ok())
        .map_or_else(
            || Utc::now().timestamp_millis(),
            |timestamp| timestamp.timestamp_millis(),
        )
}

fn parse_decimal(field: &'static str, value: &str) -> Result<Decimal> {
    value.parse::<Decimal>().map_err(|_| {
        AppError::InvalidMarket(format!("invalid decimal value in `{field}`: `{value}`"))
    })
}

fn install_rustls_crypto_provider() {
    if rustls::crypto::CryptoProvider::get_default().is_some() {
        return;
    }

    let provider = rustls::crypto::ring::default_provider();
    let _result = provider.install_default();
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use rust_decimal::Decimal;

    use super::{
        CoinbaseClient, CoinbaseL2Book, CoinbaseL2SnapshotMessage, CoinbaseL2UpdateMessage,
        CoinbaseTickerMessage,
    };
    use crate::services::binance::BinanceClient;

    #[test]
    fn selected_price_prefers_tight_bid_ask_mid() {
        let ticker = CoinbaseTickerMessage {
            product_id: "BTC-USD".to_owned(),
            price: "100.50".to_owned(),
            time: None,
            best_bid: Some("100.00".to_owned()),
            best_ask: Some("100.20".to_owned()),
        };

        let selected = ticker
            .selected_price(Decimal::new(25, 0))
            .unwrap()
            .expect("selected price");
        assert_eq!(selected.price, Decimal::new(10010, 2));
    }

    #[test]
    fn selected_price_rejects_wide_spread() {
        let ticker = CoinbaseTickerMessage {
            product_id: "BTC-USD".to_owned(),
            price: "100.50".to_owned(),
            time: None,
            best_bid: Some("100.00".to_owned()),
            best_ask: Some("102.00".to_owned()),
        };

        assert!(ticker.selected_price(Decimal::new(5, 0)).unwrap().is_none());
    }

    #[test]
    fn raw_ticker_view_does_not_report_future_event_age() {
        let binance_client = BinanceClient::new(
            "https://example.invalid".to_owned(),
            "wss://example.invalid/ws".to_owned(),
            1,
        )
        .expect("binance client");
        let client = CoinbaseClient::new(
            "wss://example.invalid".to_owned(),
            binance_client,
            Decimal::from(25_u32),
            Decimal::from(25_u32),
        );
        client.store_latest_ticker(
            "BTC-USD",
            "BTCUSDT",
            Utc::now().timestamp_millis().saturating_add(10_000),
            Decimal::new(10_000, 2),
        );

        let views = client.latest_ticker_price_views();

        assert_eq!(views.len(), 1);
        assert!(views[0].event_age_ms >= 0);
        assert!(views[0].received_age_ms >= 0);
    }

    #[test]
    fn delayed_raw_ticker_does_not_replace_newer_monitor_price() {
        let binance_client = BinanceClient::new(
            "https://example.invalid".to_owned(),
            "wss://example.invalid/ws".to_owned(),
            1,
        )
        .expect("binance client");
        let client = CoinbaseClient::new(
            "wss://example.invalid".to_owned(),
            binance_client,
            Decimal::from(25_u32),
            Decimal::from(25_u32),
        );
        let now_ms = Utc::now().timestamp_millis();
        client.store_latest_ticker(
            "BTC-USD",
            "BTCUSDT",
            now_ms.saturating_sub(100),
            Decimal::new(10_100, 2),
        );
        client.store_latest_ticker(
            "BTC-USD",
            "BTCUSDT",
            now_ms.saturating_sub(1_000),
            Decimal::new(9_900, 2),
        );

        let views = client.latest_ticker_price_views();

        assert_eq!(views.len(), 1);
        assert_eq!(views[0].price, Decimal::new(10_100, 2));
    }

    #[test]
    fn l2_book_applies_snapshot_and_update_in_book_order() {
        let snapshot = CoinbaseL2SnapshotMessage {
            product_id: "BTC-USD".to_owned(),
            bids: vec![
                vec!["100.00".to_owned(), "1.0".to_owned()],
                vec!["99.90".to_owned(), "2.0".to_owned()],
            ],
            asks: vec![
                vec!["100.10".to_owned(), "1.5".to_owned()],
                vec!["100.20".to_owned(), "3.0".to_owned()],
            ],
            time: None,
        };
        let update = CoinbaseL2UpdateMessage {
            product_id: "BTC-USD".to_owned(),
            changes: vec![
                vec!["buy".to_owned(), "100.05".to_owned(), "0.7".to_owned()],
                vec!["sell".to_owned(), "100.10".to_owned(), "0".to_owned()],
            ],
            time: None,
        };

        let mut book = CoinbaseL2Book::default();
        book.apply_snapshot(&snapshot).expect("apply snapshot");
        book.apply_update(&update).expect("apply update");
        let (bids, asks) = book.top_levels(2).expect("top levels");

        assert_eq!(bids[0], (Decimal::new(10005, 2), Decimal::new(7, 1)));
        assert_eq!(bids[1], (Decimal::new(10000, 2), Decimal::new(10, 1)));
        assert_eq!(asks[0], (Decimal::new(10020, 2), Decimal::new(30, 1)));
    }
}
