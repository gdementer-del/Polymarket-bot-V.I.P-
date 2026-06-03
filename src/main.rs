//! Binary entrypoint for the Polymarket bundle-arbitrage bot.

use clap::Parser;
use polymarket_mvp::config::Cli;
use polymarket_mvp::services::runner::run_cli;
use polymarket_mvp::services::text::SanitizingStderr;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("polymarket_mvp=info")),
        )
        .with_target(false)
        .with_writer(SanitizingStderr)
        .compact()
        .init();

    let cli = Cli::parse();
    run_cli(cli).await.map_err(anyhow::Error::from)
}
