use std::collections::HashMap;
use std::fmt::Write as _;

use chrono::Local;
use rust_decimal::Decimal;
use tokio::sync::watch;
use tokio::time::{Duration, sleep};
use tracing::{info, warn};

use crate::config::AppConfig;
use crate::error::Result;
use crate::models::MarketTarget;

use super::{
    RuntimeTriggerEvent, configured_binance_symbols, configured_market_targets,
    render_full_screen_v2,
};
use crate::services::binance::{
    BinanceClient, BinanceTriggerEvent, LiveSpotPricePoint, LiveSpotPriceView,
};
use crate::services::chainlink::{ChainlinkOraclePriceView, ChainlinkOracleTriggerEvent};
use crate::services::coinbase::{CoinbaseClient, CoinbaseTickerPriceView};

fn start_price_monitor_streams(
    config: &AppConfig,
    binance_client: &BinanceClient,
    coinbase_client: Option<&CoinbaseClient>,
) {
    for symbol in configured_binance_symbols(config) {
        if binance_client.start_trade_stream(symbol) {
            info!(symbol, "started Binance price stream for price monitor");
        } else {
            warn!(
                symbol,
                "failed to start Binance price stream for price monitor"
            );
        }
    }

    if let Some(coinbase_client) = coinbase_client {
        for target in configured_market_targets(config) {
            if coinbase_client.start_ticker_stream(target) {
                info!(
                    product_id = target.coinbase_product_id(),
                    symbol = target.binance_symbol(),
                    "started Coinbase price stream for price monitor"
                );
            } else {
                warn!(
                    product_id = target.coinbase_product_id(),
                    "failed to start Coinbase price stream"
                );
            }
        }
    }

    if config.run.chainlink_oracle.enabled {
        let targets = configured_market_targets(config);
        if binance_client.start_chainlink_oracle_stream(
            config.http.polymarket_rtds_ws_url.clone(),
            &targets,
            config.run.chainlink_oracle,
        ) {
            info!("started Chainlink RTDS price stream for price monitor");
        } else {
            warn!("failed to start Chainlink RTDS price stream for price monitor");
        }
    }
}

pub(super) async fn run_price_monitor(
    config: &AppConfig,
    binance_client: &BinanceClient,
    coinbase_client: Option<&CoinbaseClient>,
    refresh_secs: u64,
    cycles: Option<usize>,
) -> Result<()> {
    let refresh_secs = refresh_secs.max(1);
    let symbols = configured_binance_symbols(config);
    let targets = configured_market_targets(config);
    start_price_monitor_streams(config, binance_client, coinbase_client);
    println!(
        "started realtime price monitor: symbols={}, targets={}, heartbeat={}s",
        symbols.len(),
        targets.len(),
        refresh_secs
    );

    let mut exchange_rx = binance_client.subscribe_triggers();
    let mut chainlink_rx = config
        .run
        .chainlink_oracle
        .enabled
        .then(|| binance_client.subscribe_chainlink_triggers());
    let heartbeat = Duration::from_secs(refresh_secs);
    let mut completed_cycles = 0_usize;
    let mut latest_event_line = None;
    loop {
        let trigger =
            wait_for_price_monitor_trigger(&mut exchange_rx, chainlink_rx.as_mut(), heartbeat)
                .await;
        let spot_views = binance_client.live_spot_price_views(&symbols).await;
        let chainlink_views = binance_client.chainlink_price_views(&targets).await;
        let coinbase_views = coinbase_client
            .map(CoinbaseClient::latest_ticker_price_views)
            .unwrap_or_default();

        if let Some(trigger) = trigger.as_ref() {
            if !is_price_monitor_display_trigger(trigger) {
                continue;
            }
            latest_event_line = Some(render_price_monitor_event_line(
                trigger,
                &targets,
                &spot_views,
                &coinbase_views,
                &chainlink_views,
            ));
        }
        render_full_screen_v2(&render_price_monitor_screen(
            &spot_views,
            &coinbase_views,
            &chainlink_views,
            latest_event_line.as_deref(),
        ))?;

        completed_cycles += 1;
        if cycles.is_some_and(|limit| completed_cycles >= limit) {
            return Ok(());
        }
    }
}

async fn wait_for_price_monitor_trigger(
    exchange_rx: &mut watch::Receiver<Option<BinanceTriggerEvent>>,
    chainlink_rx: Option<&mut watch::Receiver<Option<ChainlinkOracleTriggerEvent>>>,
    heartbeat: Duration,
) -> Option<RuntimeTriggerEvent> {
    match chainlink_rx {
        Some(chainlink_rx) => {
            tokio::select! {
                changed = exchange_rx.changed() => {
                    changed.ok()?;
                    exchange_rx
                        .borrow_and_update()
                        .clone()
                        .map(RuntimeTriggerEvent::from_binance)
                }
                changed = chainlink_rx.changed() => {
                    changed.ok()?;
                    chainlink_rx
                        .borrow_and_update()
                        .clone()
                        .map(RuntimeTriggerEvent::from_chainlink)
                }
                _ = sleep(heartbeat) => None,
            }
        }
        None => {
            tokio::select! {
                changed = exchange_rx.changed() => {
                    changed.ok()?;
                    exchange_rx
                        .borrow_and_update()
                        .clone()
                        .map(RuntimeTriggerEvent::from_binance)
                }
                _ = sleep(heartbeat) => None,
            }
        }
    }
}

fn render_price_monitor_table(
    spot_views: &[LiveSpotPriceView],
    coinbase_views: &[CoinbaseTickerPriceView],
    chainlink_views: &[ChainlinkOraclePriceView],
) -> String {
    let coinbase_by_symbol = coinbase_views
        .iter()
        .map(|view| (view.symbol.as_str(), view))
        .collect::<HashMap<_, _>>();
    let mut output = String::new();
    let _ = writeln!(
        output,
        "price monitor @ {}",
        Local::now().format("%Y-%m-%d %H:%M:%S")
    );
    let _ = writeln!(output, "Exchange feeds:");
    for view in spot_views {
        let raw_coinbase = coinbase_by_symbol.get(view.symbol.as_str()).copied();
        let raw_coinbase_gap = view.binance_trade.and_then(|binance| {
            raw_coinbase.and_then(|coinbase| price_gap_bps(coinbase.price, binance.price))
        });
        let _ = writeln!(
            output,
            "  {} | Binance={} | CoinbaseRaw={} | CoinbaseAccepted={} | cb_vs_binance={} | depth_age={} | points={}",
            view.symbol,
            format_live_spot_price_point(view.binance_trade),
            format_coinbase_ticker_price(raw_coinbase),
            format_live_spot_price_point(view.coinbase_ticker),
            format_optional_bps(raw_coinbase_gap),
            format_optional_ms(view.depth_age_ms),
            view.quote_points
        );
    }

    let _ = writeln!(output, "Chainlink RTDS:");
    for view in chainlink_views {
        let symbol = view.symbol.unwrap_or("unsupported");
        let price = view.quote.map_or_else(
            || "waiting".to_owned(),
            |quote| format_price_with_ages(quote.price, quote.event_age_ms, quote.received_age_ms),
        );
        let _ = writeln!(output, "  {} {} | {}", view.target.label(), symbol, price);
    }

    output
}

fn render_price_monitor_screen(
    spot_views: &[LiveSpotPriceView],
    coinbase_views: &[CoinbaseTickerPriceView],
    chainlink_views: &[ChainlinkOraclePriceView],
    latest_event_line: Option<&str>,
) -> String {
    let mut output = String::new();
    let _ = writeln!(output, "Realtime Price Monitor");
    let _ = writeln!(output, "Press Ctrl+C to stop. Values update in place.");
    let _ = writeln!(output);
    let _ = write!(
        output,
        "{}",
        render_price_monitor_table(spot_views, coinbase_views, chainlink_views)
    );
    let _ = writeln!(output, "Latest displayed event:");
    let _ = writeln!(output, "  {}", latest_event_line.unwrap_or("waiting"));
    output
}

fn render_price_monitor_event_line(
    trigger: &RuntimeTriggerEvent,
    targets: &[MarketTarget],
    spot_views: &[LiveSpotPriceView],
    coinbase_views: &[CoinbaseTickerPriceView],
    chainlink_views: &[ChainlinkOraclePriceView],
) -> String {
    let display_symbol = price_monitor_display_symbol(trigger, targets);
    let spot_view = spot_views
        .iter()
        .find(|view| view.symbol == display_symbol)
        .or_else(|| spot_views.iter().find(|view| view.symbol == trigger.symbol));
    let raw_coinbase = coinbase_views
        .iter()
        .find(|view| view.symbol == display_symbol)
        .or_else(|| {
            coinbase_views
                .iter()
                .find(|view| view.symbol == trigger.symbol)
        });
    let chainlink_view = price_monitor_chainlink_view(trigger, targets, chainlink_views);
    let raw_coinbase_gap = spot_view.and_then(|view| {
        view.binance_trade.and_then(|binance| {
            raw_coinbase.and_then(|coinbase| price_gap_bps(coinbase.price, binance.price))
        })
    });
    let chainlink_gap = spot_view.and_then(|view| {
        view.binance_trade.and_then(|binance| {
            chainlink_view.and_then(|chainlink| {
                chainlink
                    .quote
                    .and_then(|quote| price_gap_bps(quote.price, binance.price))
            })
        })
    });

    format!(
        "{} | {} {} event={} event_age={}ms recv_age={}ms | binance={} | cb_raw={} | cb_accepted={} | chainlink={} | cb_gap={} | chainlink_gap={} | depth={}",
        Local::now().format("%H:%M:%S%.3f"),
        trigger.source,
        trigger.symbol,
        trigger.price.round_dp(4),
        trigger.event_age_ms(),
        trigger.received_age_ms(),
        format_live_spot_price_point(spot_view.and_then(|view| view.binance_trade)),
        format_coinbase_ticker_price(raw_coinbase),
        format_live_spot_price_point(spot_view.and_then(|view| view.coinbase_ticker)),
        format_chainlink_price_view(chainlink_view),
        format_optional_bps(raw_coinbase_gap),
        format_optional_bps(chainlink_gap),
        format_optional_ms(spot_view.and_then(|view| view.depth_age_ms))
    )
}

fn is_price_monitor_display_trigger(trigger: &RuntimeTriggerEvent) -> bool {
    matches!(
        trigger.source.as_str(),
        "Binance::Trade" | "Binance::OneSecondKline" | "Coinbase::Ticker" | "Chainlink::RTDS"
    )
}

fn price_monitor_display_symbol(trigger: &RuntimeTriggerEvent, targets: &[MarketTarget]) -> String {
    if trigger.source == "Chainlink::RTDS" {
        targets
            .iter()
            .find(|target| target.polymarket_chainlink_symbol() == Some(trigger.symbol.as_str()))
            .map_or_else(
                || trigger.symbol.clone(),
                |target| target.binance_symbol().to_owned(),
            )
    } else {
        trigger.symbol.clone()
    }
}

fn price_monitor_chainlink_view<'a>(
    trigger: &RuntimeTriggerEvent,
    targets: &[MarketTarget],
    chainlink_views: &'a [ChainlinkOraclePriceView],
) -> Option<&'a ChainlinkOraclePriceView> {
    if trigger.source == "Chainlink::RTDS" {
        return chainlink_views
            .iter()
            .find(|view| view.symbol == Some(trigger.symbol.as_str()));
    }

    let target = targets
        .iter()
        .find(|target| target.binance_symbol() == trigger.symbol)?;
    chainlink_views.iter().find(|view| view.target == *target)
}

fn format_live_spot_price_point(point: Option<LiveSpotPricePoint>) -> String {
    point.map_or_else(
        || "-".to_owned(),
        |point| format_price_with_ages(point.price, point.event_age_ms, point.received_age_ms),
    )
}

fn format_coinbase_ticker_price(view: Option<&CoinbaseTickerPriceView>) -> String {
    view.map_or_else(
        || "-".to_owned(),
        |view| format_price_with_ages(view.price, view.event_age_ms, view.received_age_ms),
    )
}

fn format_chainlink_price_view(view: Option<&ChainlinkOraclePriceView>) -> String {
    view.and_then(|view| view.quote).map_or_else(
        || "-".to_owned(),
        |quote| format_price_with_ages(quote.price, quote.event_age_ms, quote.received_age_ms),
    )
}

fn format_price_with_ages(price: Decimal, event_age_ms: i64, received_age_ms: i64) -> String {
    format!(
        "{} event_age={}ms recv_age={}ms",
        price.round_dp(4),
        event_age_ms,
        received_age_ms
    )
}

fn format_optional_ms(value: Option<i64>) -> String {
    value.map_or_else(|| "-".to_owned(), |value| format!("{value}ms"))
}

fn format_optional_bps(value: Option<Decimal>) -> String {
    value.map_or_else(
        || "-".to_owned(),
        |value| format!("{} bps", value.round_dp(4)),
    )
}

fn price_gap_bps(secondary_price: Decimal, primary_price: Decimal) -> Option<Decimal> {
    (primary_price > Decimal::ZERO).then(|| {
        ((secondary_price - primary_price) / primary_price * Decimal::from(10_000_u32)).round_dp(4)
    })
}
