# Polymarket Research Toolkit

Низколатентный Rust-инструментарий для исследования коротких crypto Up/Down рынков
Polymarket. Программа объединяет стаканы Polymarket, Binance WebSocket, Coinbase
WebSocket и публичный Polymarket Chainlink RTDS, запускает controlled paper-тесты
и формирует отчёты по результатам.

Проект предназначен прежде всего для исследований и симуляции. Положительный PnL
не гарантирован: перед любым реальным использованием нужно отдельно проверить
комиссии, проскальзывание, ликвидность и устойчивость результата вне обучающей
выборки.

## Самый простой запуск

### 1. Установите Rust

Нужен Rust `1.96.0` или новее. После установки проверьте терминал:

```powershell
rustc --version
cargo --version
```

### 2. Соберите программу

Откройте PowerShell в папке проекта:

```powershell
cargo build --release
```

Готовый файл появится здесь:

```text
target\release\polymarket_mvp.exe
```

### 3. Откройте меню

```powershell
.\target\release\polymarket_mvp.exe
```

То же самое можно вызвать явно:

```powershell
.\target\release\polymarket_mvp.exe menu
```

При запуске без аргументов меню автоматически выбирает исследовательский профиль
`config.codex-scalp-v1-raw-light-v3.toml`, если он есть рядом с программой.

## Что есть в меню

Главное меню служит безопасной операторской точкой входа:

| Раздел | Назначение |
| --- | --- |
| `1. Controlled paper-тест` | Запуск симуляции на 10, 30, 60 минут или выбранное время |
| `2. Мониторинг` | Цены Binance, Coinbase и Chainlink RTDS, dashboard рынков, watch и scan |
| `3. Paper-отчёты` | PnL запуска, качество сделок, журнал, позиции и аналитика |
| `4. Исследования` | Локальный backtest, PolyBacktest, sweep параметров и наблюдение кошелька |
| `5. Выбрать TOML-профиль` | Переключение между локальными `config*.toml` |
| `6. Проверить профиль` | Валидация настроек до запуска |
| `7. CLI-справка` | Короткая памятка для ручного управления |

Меню намеренно не запускает live-ордера одной кнопкой. Оно управляет paper-тестами,
мониторингом и исследованиями, чтобы случайный выбор пункта не отправил реальную
сделку.

## Первый paper-тест

Для проверки установки:

1. Запустите `.\target\release\polymarket_mvp.exe`.
2. Выберите `6`, чтобы проверить текущий профиль.
3. Выберите `2`, затем `1`, чтобы увидеть поток котировок в реальном времени.
4. Остановите монитор клавишами `Ctrl+C`.
5. Снова откройте меню и выберите `1`, затем `1` для smoke test на 10 минут.
6. После завершения выберите `3`, затем `1` для итоговой сводки.

Для остановки длительной команды нажмите `Ctrl+C`. Controlled paper-run завершает
новые входы по таймеру, даёт открытым paper-позициям короткое время на закрытие и
сбрасывает журналы на диск.

## Прямые CLI-команды

Меню покрывает ежедневную работу, но все операции доступны и напрямую.

```powershell
$bot = ".\target\release\polymarket_mvp.exe"
$config = "config.codex-scalp-v1-raw-light-v3.toml"
```

Поток сырых цен в реальном времени:

```powershell
& $bot --config $config price-monitor --refresh-secs 1
```

Один scan рынков:

```powershell
& $bot --config $config scan --top 10
```

Controlled paper-run на 30 минут:

```powershell
& $bot --config $config run --mode paper --max-runtime-secs 1800 --drain-open-positions --max-drain-secs 60
```

Отчёты после прогона:

```powershell
& $bot --config $config paper-run-summary
& $bot --config $config paper-quality
& $bot --config $config paper-trades
& $bot --config $config paper-positions
```

Полный перечень команд:

```powershell
& $bot --help
```

## Профили

Профили хранятся рядом с проектом в файлах `config*.toml`.

| Файл | Для чего использовать |
| --- | --- |
| `config.example.toml` | Документированный базовый шаблон |
| `config.codex-scalp-v1-raw-light-v3.toml` | Текущий быстрый multi-asset paper-профиль для controlled экспериментов |
| `config.codex-scalp-v1-raw-light-v2.toml` | Предыдущая версия raw-light для сравнений |
| `config.codex-scalp-v1-raw.toml` | Raw ablation-профиль для исследований |
| `config.codex-scalp-v1.toml` | Базовый scalp-профиль |
| `config.codex-sentinel.toml` | Более консервативный Sentinel-эксперимент |
| `config.codex-v4-champion.toml` | Исторический v4-профиль |
| `config.polybacktest-btc.toml` | Профиль для BTC PolyBacktest |

Ни один профиль не следует считать доказанно прибыльным. Сравнивайте варианты на
одинаковых временных интервалах, учитывайте число сделок и проверяйте результат
out-of-sample.

## PolyBacktest

Для облачного PolyBacktest нужен токен только в текущем терминале:

```powershell
$env:POLYBACKTEST_API_KEY = "ваш_токен"
& $bot --config $config polybacktest --windows-per-target 30 --entry-minutes 1 --top 10 --target btc-5m
```

Не записывайте реальный токен в TOML, README или git.

## Секреты и live-режим

Для paper-тестов секреты Polymarket не нужны. Если вы отдельно исследуете live API,
используйте переменные окружения текущего PowerShell:

```powershell
$env:POLYMARKET_API_KEY = "..."
```

Поддерживаемые переменные перечислены в `.env.example`. Файл `.env` исключён из
git, но бинарник не загружает его автоматически: если вы ведёте локальный `.env`,
экспортируйте значения в окружение перед запуском. Никогда не коммитьте приватный
ключ, API key, secret или passphrase. Live-исполнение оставлено CLI-only и
требует отдельного аудита перед использованием.

## Где лежат результаты

Локальные журналы и runtime-state создаются в папке `state/`. Они исключены из
git. Внутри находятся paper-сделки, циклы, PnL snapshot и состояние открытых
позиций в соответствии с выбранным TOML-профилем.

Основные команды анализа:

```powershell
& $bot --config $config paper-run-summary
& $bot --config $config paper-quality
& $bot --config $config analytics
```

## Проверка проекта

Перед публикацией или после изменения Rust-кода выполните:

```powershell
cargo fmt --all -- --check
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
cargo build --release --locked
```

## Если что-то не работает

| Симптом | Что проверить |
| --- | --- |
| `cargo` не найден | Переоткройте PowerShell после установки Rust |
| Нет котировок | Проверьте интернет, VPN/firewall и доступ к WebSocket Binance, Coinbase и Polymarket |
| Chainlink показывает `waiting` | Публичный RTDS может не выдавать каждый символ постоянно; сравните с Binance и Coinbase |
| PolyBacktest не запускается | Проверьте `$env:POLYBACKTEST_API_KEY` в текущем PowerShell |
| В меню выбран не тот профиль | Используйте пункт `5`, затем пункт `6` для проверки |
| Нужно остановить наблюдение | Нажмите `Ctrl+C` |
| Нужна подробная CLI-справка | Выполните `.\target\release\polymarket_mvp.exe --help` |

## Структура проекта

```text
src/config.rs            CLI и TOML-конфигурация
src/models/              модели рынков, стаканов и paper-состояния
src/services/menu.rs     интерактивное операторское меню
src/services/runner.rs   orchestration и runtime-loop
src/services/            market data, strategy, execution, journal и analytics
config*.toml             профили экспериментов
docs/                    исследовательские заметки и протоколы
scripts/                 вспомогательные PowerShell-скрипты
```

## Дисклеймер

Это исследовательское программное обеспечение, а не финансовая рекомендация.
Рынки прогнозов и торговые системы связаны с существенным риском. Paper PnL не
доказывает наличие устойчивого edge в реальном исполнении.
