//! Interactive operator menu for paper trading and research commands.

use std::env;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::config::AppConfig;
use crate::error::Result;

const DEFAULT_CONFIG_PATH: &str = "config.example.toml";
const RECOMMENDED_CONFIG_PATH: &str = "config.codex-scalp-v1-raw-light-v3.toml";

/// Open the interactive operator menu.
///
/// The menu intentionally exposes paper and research operations only. Live
/// execution remains an explicit CLI-only action to avoid accidental orders.
pub fn run(initial_config: &Path) -> Result<()> {
    let mut menu = OperatorMenu::new(initial_config);
    loop {
        print_main_menu(&menu.config);
        match prompt("Выберите пункт")?.as_str() {
            "1" => menu.paper_menu()?,
            "2" => menu.monitoring_menu()?,
            "3" => menu.reports_menu()?,
            "4" => menu.research_menu()?,
            "5" => menu.select_config()?,
            "6" => menu.validate_config()?,
            "7" => show_cli_help()?,
            "0" | "q" | "quit" | "exit" => {
                println!("Меню закрыто.");
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
            print_header("Paper-тесты", &self.config);
            println!("1. Быстрый smoke test на 10 минут");
            println!("2. Controlled paper-run на 30 минут");
            println!("3. Controlled paper-run на 60 минут");
            println!("4. Задать длительность вручную");
            println!("5. Выполнить ровно один цикл");
            println!("0. Назад");
            match prompt("Выберите пункт")?.as_str() {
                "1" => self.run_paper_for_minutes(10)?,
                "2" => self.run_paper_for_minutes(30)?,
                "3" => self.run_paper_for_minutes(60)?,
                "4" => {
                    if let Some(minutes) = prompt_positive_u64("Длительность в минутах")?
                    {
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
            print_header("Мониторинг", &self.config);
            println!("1. Цены в реальном времени: Binance, Coinbase, Chainlink");
            println!("2. Терминальный dashboard рынков и сигналов");
            println!("3. Таблица активных рынков с автообновлением");
            println!("4. Один быстрый scan возможностей");
            println!("0. Назад");
            match prompt("Выберите пункт")?.as_str() {
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
            print_header("Paper-отчёты", &self.config);
            println!("1. Итог последнего paper-run");
            println!("2. Качество сделок по bucket-ам");
            println!("3. Лента открытий и закрытий");
            println!("4. Текущие открытые позиции");
            println!("5. Общий paper-отчёт");
            println!("6. Аналитика локального журнала");
            println!("0. Назад");
            match prompt("Выберите пункт")?.as_str() {
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
            print_header("Исследования", &self.config);
            println!("1. Локальный backtest");
            println!("2. PolyBacktest для BTC 5m");
            println!("3. Sweep параметров PolyBacktest для BTC 5m");
            println!("4. Wallet research: запись, replay, compare и autotune");
            println!("5. Проверить live-учётные данные без отправки ордеров");
            println!("0. Назад");
            match prompt("Выберите пункт")?.as_str() {
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
            println!("1. Наблюдать публичную активность Bonereaper");
            println!("2. Наблюдать другой публичный кошелёк");
            println!("3. Записывать snapshots активности Bonereaper");
            println!("4. Сводка записанной активности");
            println!("5. Replay-отчёт по inventory");
            println!("6. Timeline одного replay-окна");
            println!("7. Экспортировать replay dataset");
            println!("8. Симулировать caps и cooldown на replay dataset");
            println!("9. Сравнить несколько replay export JSON");
            println!("10. Autotune caps и cooldown по replay export JSON");
            println!("11. Калибровать alert thresholds по replay export JSON");
            println!("0. Назад");
            match prompt("Выберите пункт")?.as_str() {
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
        let wallet = prompt("Публичный wallet address")?;
        if wallet.is_empty() {
            println!("Wallet address не должен быть пустым.");
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
            prompt_path_list("JSON-файлы через ; (пример: runs\\a.json; runs\\b.json)")?
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
        print_header("Выбор профиля", &self.config);
        for (index, profile) in profiles.iter().enumerate() {
            let selected = if profile == &self.config {
                " [выбран]"
            } else {
                ""
            };
            println!("{}. {}{}", index + 1, profile.display(), selected);
        }
        println!("0. Назад");

        let raw = prompt("Номер профиля")?;
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
                println!("Выбран профиль: {}", self.config.display());
            }
            Err(error) => {
                println!("Профиль не выбран: конфиг не прошёл проверку: {error}");
            }
        }
        pause()
    }

    fn validate_config(&self) -> Result<()> {
        print_header("Проверка профиля", &self.config);
        match AppConfig::load(&self.config) {
            Ok(config) => {
                println!("Конфиг корректен.");
                println!("Режим по умолчанию: {:?}", config.run.mode);
                println!(
                    "Старт paper-сессии: {}",
                    config.run.effective_paper_start_mode().as_str()
                );
                println!(
                    "Целевых семейств рынков: {}",
                    config.strategy.market_targets.len()
                );
            }
            Err(error) => println!("Ошибка конфигурации: {error}"),
        }
        pause()
    }

    fn run_child(&self, command_args: Vec<String>) -> Result<()> {
        print_header("Запуск команды", &self.config);
        println!("{}", display_command(&self.config, &command_args));
        println!("Для остановки длительного наблюдения нажмите Ctrl+C.");
        let status = Command::new(env::current_exe()?)
            .arg("--config")
            .arg(&self.config)
            .args(&command_args)
            .status()?;
        println!();
        if status.success() {
            println!("Команда завершилась успешно.");
        } else {
            println!("Команда завершилась с кодом: {status}");
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
            println!("Введите целое число больше нуля.");
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
        println!("Добавьте хотя бы один путь к JSON-файлу.");
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
    let _ = prompt("Нажмите Enter, чтобы продолжить")?;
    Ok(())
}

fn print_main_menu(config: &Path) {
    print_header("Polymarket Operator Menu", config);
    println!("1. Запустить controlled paper-тест");
    println!("2. Мониторинг цен, рынков и сигналов");
    println!("3. Paper-отчёты и позиции");
    println!("4. Исследования и backtest");
    println!("5. Выбрать TOML-профиль");
    println!("6. Проверить текущий профиль");
    println!("7. Показать CLI-справку");
    println!("0. Выход");
    println!();
    println!("Live-ордера не запускаются из меню.");
}

fn print_header(title: &str, config: &Path) {
    println!();
    println!("============================================================");
    println!("{title}");
    println!("Профиль: {}", config.display());
    println!("============================================================");
}

fn print_unknown_choice() {
    println!("Неизвестный пункт. Выберите номер из списка.");
}

fn show_cli_help() -> Result<()> {
    print_header("CLI-справка", Path::new("-"));
    println!("Полный список прямых команд:");
    println!("  polymarket_mvp.exe --help");
    println!();
    println!("Примеры:");
    println!("  polymarket_mvp.exe menu");
    println!("  polymarket_mvp.exe --config config.example.toml scan --top 10");
    println!("  polymarket_mvp.exe --config config.example.toml price-monitor --refresh-secs 1");
    println!(
        "  polymarket_mvp.exe --config config.example.toml run --mode paper --max-runtime-secs 600"
    );
    println!();
    println!("Live-режим доступен только через явную ручную CLI-команду.");
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
