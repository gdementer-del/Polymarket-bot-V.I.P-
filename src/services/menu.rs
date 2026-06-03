//! Interactive operator menu for paper trading and research commands.

use std::env;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::config::AppConfig;
use crate::error::Result;

const DEFAULT_CONFIG_PATH: &str = "config.example.toml";
const RECOMMENDED_CONFIG_PATH: &str = "config.scalp-v1-raw-light-v3.toml";

/// Open the interactive operator menu.
///
/// The menu intentionally exposes paper and research operations only. Live
/// execution remains an explicit CLI-only action to avoid accidental orders.
pub fn run(initial_config: &Path) -> Result<()> {
    let mut menu = OperatorMenu::new(initial_config);
    loop {
        print_main_menu(&menu.config);
        match prompt("Select an option")?.as_str() {
            "1" => menu.paper_menu()?,
            "2" => menu.monitoring_menu()?,
            "3" => menu.reports_menu()?,
            "4" => menu.research_menu()?,
            "5" => menu.select_config()?,
            "6" => menu.validate_config()?,
            "7" => show_cli_help()?,
            "0" | "q" | "quit" | "exit" => {
                println!("Menu closed.");
                return Ok(());
            }
            _ => print_unknown_choice(),
        }
    }
}

struct OperatorMenu {
    config: PathBuf,
}

impl OperatorMenu {
    fn new(initial_config: &Path) -> Self {
        let config = if initial_config == Path::new(DEFAULT_CONFIG_PATH)
            && Path::new(RECOMMENDED_CONFIG_PATH).is_file()
        {
            PathBuf::from(RECOMMENDED_CONFIG_PATH)
        } else {
            initial_config.to_path_buf()
        };
        Self { config }
    }

    fn paper_menu(&self) -> Result<()> {
        loop {
            print_header("Paper Runs", &self.config);
            println!("1. Quick 10-minute smoke test");
            println!("2. Controlled 30-minute paper run");
            println!("3. Controlled 60-minute paper run");
            println!("4. Set a custom duration");
            println!("5. Execute exactly one cycle");
            println!("0. Back");
            match prompt("Select an option")?.as_str() {
                "1" => self.run_paper_for_minutes(10)?,
                "2" => self.run_paper_for_minutes(30)?,
                "3" => self.run_paper_for_minutes(60)?,
                "4" => {
                    if let Some(minutes) = prompt_positive_u64("Duration in minutes")? {
                        self.run_paper_for_minutes(minutes)?;
                    }
                }
                "5" => self.run_child(args(&["run", "--mode", "paper", "--once"]))?,
                "0" | "b" | "back" => return Ok(()),
                _ => print_unknown_choice(),
            }
        }
    }

    fn monitoring_menu(&self) -> Result<()> {
        loop {
            print_header("Monitoring", &self.config);
            println!("1. Realtime prices: Binance, Coinbase, Chainlink");
            println!("2. Terminal dashboard for markets and signals");
            println!("3. Auto-refreshing active market table");
            println!("4. Run one quick opportunity scan");
            println!("0. Back");
            match prompt("Select an option")?.as_str() {
                "1" => self.run_child(args(&["price-monitor", "--refresh-secs", "1"]))?,
                "2" => self.run_child(args(&["dashboard", "--refresh-secs", "1"]))?,
                "3" => self.run_child(args(&["markets", "--watch", "--refresh-secs", "2"]))?,
                "4" => self.run_child(args(&["scan", "--top", "10"]))?,
                "0" | "b" | "back" => return Ok(()),
                _ => print_unknown_choice(),
            }
        }
    }

    fn reports_menu(&self) -> Result<()> {
        loop {
            print_header("Paper Reports", &self.config);
            println!("1. Summarize the latest paper run");
            println!("2. Show trade-quality buckets");
            println!("3. Show open and close events");
            println!("4. Show current open positions");
            println!("5. Show the general paper report");
            println!("6. Analyze the local journal");
            println!("0. Back");
            match prompt("Select an option")?.as_str() {
                "1" => self.run_child(args(&["paper-run-summary"]))?,
                "2" => self.run_child(args(&["paper-quality"]))?,
                "3" => self.run_child(args(&["paper-trades"]))?,
                "4" => self.run_child(args(&["paper-positions"]))?,
                "5" => self.run_child(args(&["paper-report"]))?,
                "6" => self.run_child(args(&["analytics"]))?,
                "0" | "b" | "back" => return Ok(()),
                _ => print_unknown_choice(),
            }
        }
    }

    fn research_menu(&self) -> Result<()> {
        loop {
            print_header("Research and Backtesting", &self.config);
            println!("1. Run a local backtest");
            println!("2. Run PolyBacktest for BTC 5m");
            println!("3. Sweep PolyBacktest parameters for BTC 5m");
            println!("4. Wallet research: record, replay, compare, and autotune");
            println!("5. Validate live credentials without sending orders");
            println!("0. Back");
            match prompt("Select an option")?.as_str() {
                "1" => self.run_child(args(&[
                    "backtest",
                    "--windows-per-target",
                    "30",
                    "--entry-minutes",
                    "1",
                    "--top",
                    "10",
                ]))?,
                "2" => self.run_child(args(&[
                    "polybacktest",
                    "--windows-per-target",
                    "30",
                    "--entry-minutes",
                    "1",
                    "--top",
                    "10",
                    "--target",
                    "btc-5m",
                ]))?,
                "3" => self.run_child(args(&[
                    "polybacktest-tune",
                    "--windows-per-target",
                    "30",
                    "--top",
                    "10",
                    "--target",
                    "btc-5m",
                ]))?,
                "4" => self.wallet_research_menu()?,
                "5" => self.run_child(args(&["auth-check"]))?,
                "0" | "b" | "back" => return Ok(()),
                _ => print_unknown_choice(),
            }
        }
    }

    fn wallet_research_menu(&self) -> Result<()> {
        loop {
            print_header("Wallet research", &self.config);
            println!("1. Monitor public Bonereaper activity");
            println!("2. Monitor another public wallet");
            println!("3. Record Bonereaper activity snapshots");
            println!("4. Summarize recorded activity");
            println!("5. Show the inventory replay report");
            println!("6. Show one replay-window timeline");
            println!("7. Export the replay dataset");
            println!("8. Simulate caps and cooldowns on a replay dataset");
            println!("9. Compare multiple replay export JSON files");
            println!("10. Autotune caps and cooldowns from replay export JSON");
            println!("11. Calibrate alert thresholds from replay export JSON");
            println!("0. Back");
            match prompt("Select an option")?.as_str() {
                "1" => self.run_child(args(&["follow-wallet", "--refresh-secs", "8"]))?,
                "2" => self.follow_custom_wallet()?,
                "3" => self.run_child(args(&["follow-wallet-record", "--refresh-secs", "8"]))?,
                "4" => self.run_child(args(&["follow-wallet-report"]))?,
                "5" => self.run_child(args(&["follow-wallet-replay-report"]))?,
                "6" => self.run_child(args(&["follow-wallet-replay-window"]))?,
                "7" => self.run_child(args(&["follow-wallet-replay-export"]))?,
                "8" => self.run_child(args(&["follow-wallet-replay-simulate"]))?,
                "9" => self.run_wallet_research_for_inputs("follow-wallet-research-compare")?,
                "10" => self.run_wallet_research_for_inputs("follow-wallet-replay-autotune")?,
                "11" => self.run_wallet_research_for_inputs("follow-wallet-alert-calibrate")?,
                "0" | "b" | "back" => return Ok(()),
                _ => print_unknown_choice(),
            }
        }
    }

    fn follow_custom_wallet(&self) -> Result<()> {
        let wallet = prompt("Public wallet address")?;
        if wallet.is_empty() {
            println!("Wallet address must not be empty.");
            return pause();
        }
        self.run_child(vec![
            "follow-wallet".to_owned(),
            "--wallet".to_owned(),
            wallet,
            "--refresh-secs".to_owned(),
            "8".to_owned(),
        ])
    }

    fn run_wallet_research_for_inputs(&self, command: &str) -> Result<()> {
        let Some(inputs) =
            prompt_path_list("JSON files separated by ; (example: runs\\a.json; runs\\b.json)")?
        else {
            return Ok(());
        };
        let mut command_args = vec![command.to_owned(), "--inputs".to_owned()];
        command_args.extend(inputs);
        self.run_child(command_args)
    }

    fn run_paper_for_minutes(&self, minutes: u64) -> Result<()> {
        self.run_child(paper_run_args(minutes))
    }

    fn select_config(&mut self) -> Result<()> {
        let profiles = config_profiles()?;
        print_header("Select Profile", &self.config);
        for (index, profile) in profiles.iter().enumerate() {
            let selected = if profile == &self.config {
                " [selected]"
            } else {
                ""
            };
            println!("{}. {}{}", index + 1, profile.display(), selected);
        }
        println!("0. Back");

        let raw = prompt("Profile number")?;
        if raw == "0" || raw.eq_ignore_ascii_case("back") {
            return Ok(());
        }
        let Some(index) = parse_index(&raw, profiles.len()) else {
            print_unknown_choice();
            return Ok(());
        };
        let candidate = &profiles[index];
        match AppConfig::load(candidate) {
            Ok(_) => {
                self.config.clone_from(candidate);
                println!("Selected profile: {}", self.config.display());
            }
            Err(error) => {
                println!("Profile was not selected because validation failed: {error}");
            }
        }
        pause()
    }

    fn validate_config(&self) -> Result<()> {
        print_header("Validate Profile", &self.config);
        match AppConfig::load(&self.config) {
            Ok(config) => {
                println!("Configuration is valid.");
                println!("Default mode: {:?}", config.run.mode);
                println!(
                    "Paper session start mode: {}",
                    config.run.effective_paper_start_mode().as_str()
                );
                println!(
                    "Configured market families: {}",
                    config.strategy.market_targets.len()
                );
            }
            Err(error) => println!("Configuration error: {error}"),
        }
        pause()
    }

    fn run_child(&self, command_args: Vec<String>) -> Result<()> {
        print_header("Run Command", &self.config);
        println!("{}", display_command(&self.config, &command_args));
        println!("Press Ctrl+C to stop a long-running command.");
        let status = Command::new(env::current_exe()?)
            .arg("--config")
            .arg(&self.config)
            .args(&command_args)
            .status()?;
        println!();
        if status.success() {
            println!("Command completed successfully.");
        } else {
            println!("Command exited with status: {status}");
        }
        pause()
    }
}

fn paper_run_args(minutes: u64) -> Vec<String> {
    args(&[
        "run",
        "--mode",
        "paper",
        "--max-runtime-secs",
        &(minutes.saturating_mul(60)).to_string(),
        "--drain-open-positions",
        "--max-drain-secs",
        "60",
        "--status-dashboard",
    ])
}

fn args(items: &[&str]) -> Vec<String> {
    items.iter().map(|item| (*item).to_owned()).collect()
}

fn config_profiles() -> Result<Vec<PathBuf>> {
    let mut profiles = fs::read_dir(".")?
        .filter_map(std::result::Result::ok)
        .map(|entry| PathBuf::from(entry.file_name()))
        .filter(|path| {
            path.is_file()
                && path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("config") && name.ends_with(".toml"))
        })
        .collect::<Vec<_>>();
    profiles.sort();
    Ok(profiles)
}

fn parse_index(raw: &str, len: usize) -> Option<usize> {
    raw.parse::<usize>()
        .ok()
        .filter(|value| (1..=len).contains(value))
        .map(|value| value - 1)
}

fn prompt_positive_u64(label: &str) -> Result<Option<u64>> {
    let raw = prompt(label)?;
    match raw.parse::<u64>() {
        Ok(value) if value > 0 => Ok(Some(value)),
        _ => {
            println!("Enter an integer greater than zero.");
            pause()?;
            Ok(None)
        }
    }
}

fn prompt_path_list(label: &str) -> Result<Option<Vec<String>>> {
    let raw = prompt(label)?;
    let paths = raw
        .split(';')
        .map(str::trim)
        .filter(|path| !path.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if paths.is_empty() {
        println!("Add at least one JSON file path.");
        pause()?;
        return Ok(None);
    }
    Ok(Some(paths))
}

fn prompt(label: &str) -> Result<String> {
    print!("{label}: ");
    io::stdout().flush()?;
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    Ok(input.trim().to_owned())
}

fn pause() -> Result<()> {
    println!();
    let _ = prompt("Press Enter to continue")?;
    Ok(())
}

fn print_main_menu(config: &Path) {
    print_header("Polymarket Operator Menu", config);
    println!("1. Run a controlled paper test");
    println!("2. Monitor prices, markets, and signals");
    println!("3. Inspect paper reports and positions");
    println!("4. Run research and backtests");
    println!("5. Select a TOML profile");
    println!("6. Validate the current profile");
    println!("7. Show CLI help");
    println!("0. Exit");
    println!();
    println!("The menu never sends live orders.");
}

fn print_header(title: &str, config: &Path) {
    println!();
    println!("============================================================");
    println!("{title}");
    println!("Profile: {}", config.display());
    println!("============================================================");
}

fn print_unknown_choice() {
    println!("Unknown option. Select a number from the list.");
}

fn show_cli_help() -> Result<()> {
    print_header("CLI Help", Path::new("-"));
    println!("Full list of direct commands:");
    println!("  polymarket_mvp.exe --help");
    println!();
    println!("Examples:");
    println!("  polymarket_mvp.exe menu");
    println!("  polymarket_mvp.exe --config config.example.toml scan --top 10");
    println!("  polymarket_mvp.exe --config config.example.toml price-monitor --refresh-secs 1");
    println!(
        "  polymarket_mvp.exe --config config.example.toml run --mode paper --max-runtime-secs 600"
    );
    println!();
    println!("Live mode is available only through an explicit manual CLI command.");
    pause()
}

fn display_command(config: &Path, command_args: &[String]) -> String {
    format!(
        "polymarket_mvp.exe --config \"{}\" {}",
        config.display(),
        command_args.join(" ")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_menu_profile_prefers_current_scalp_research_config() {
        let menu = OperatorMenu::new(Path::new(DEFAULT_CONFIG_PATH));

        if Path::new(RECOMMENDED_CONFIG_PATH).is_file() {
            assert_eq!(menu.config, Path::new(RECOMMENDED_CONFIG_PATH));
        }
    }

    #[test]
    fn paper_run_is_always_bounded_and_explicitly_paper_only() {
        assert_eq!(
            paper_run_args(30),
            args(&[
                "run",
                "--mode",
                "paper",
                "--max-runtime-secs",
                "1800",
                "--drain-open-positions",
                "--max-drain-secs",
                "60",
                "--status-dashboard",
            ])
        );
    }

    #[test]
    fn menu_indexes_are_one_based_and_range_checked() {
        assert_eq!(parse_index("1", 3), Some(0));
        assert_eq!(parse_index("3", 3), Some(2));
        assert_eq!(parse_index("0", 3), None);
        assert_eq!(parse_index("4", 3), None);
        assert_eq!(parse_index("not-a-number", 3), None);
    }

    #[test]
    fn wallet_research_inputs_preserve_windows_paths_with_spaces() {
        let paths = "runs\\first export.json; runs\\second.json"
            .split(';')
            .map(str::trim)
            .map(str::to_owned)
            .collect::<Vec<_>>();

        assert_eq!(paths, vec!["runs\\first export.json", "runs\\second.json"]);
    }
}
