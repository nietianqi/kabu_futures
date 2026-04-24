# kabu-futures

Python strategy framework generated from `docs/kabu_micro225_minitopix_dual_strategy.md`.

The code is intentionally safe by default:

- Microstructure mode defaults to `observe_only`.
- `python main.py` runs in `observe` execution mode by default and never sends real orders.
- `--trade-mode paper` creates simulated microstructure positions only; live `/sendorder/future` is not enabled.
- Strategy, risk, order payload, and market microstructure logic can be tested without kabu Station.

## Layout

- `src/kabu_futures/models.py`: shared market, signal, order, and feature dataclasses.
- `src/kabu_futures/indicators.py`: bar building, cumulative-volume delta handling, VWAP, EMA, ATR, opening range.
- `src/kabu_futures/strategies.py`: minute ORB, trend-pullback, trend-continuation, and micro book engines.
- `src/kabu_futures/microstructure.py`: imbalance, OFI, microprice, jump filters.
- `src/kabu_futures/multitimeframe.py`: year/month/week/day/hour regime and bias scoring.
- `src/kabu_futures/risk.py`: daily risk controls and order throttling.
- `src/kabu_futures/orders.py`: kabu futures order payload builders and synthetic OCO state.
- `src/kabu_futures/api.py`: minimal kabu Station REST client.
- `src/kabu_futures/marketdata.py`: kabu board/PUSH normalizer, optional WebSocket reader, JSONL recorder.
- `src/kabu_futures/execution.py`: micro-trade take-profit/stop-loss/time-stop manager.
- `src/kabu_futures/paper_execution.py`: paper-only controller/executor lifecycle for micro book signals.
- `src/kabu_futures/simulator.py`: book-path micro replay simulator.
- `src/kabu_futures/engine.py`: dual-layer orchestration for replay/dry-run use.

## Run Tests

```powershell
python -m unittest discover -s tests
```

## Main Entry

Default entry:

```powershell
cd D:\kabu_futures
python main.py
```

By default, `main.py` registers `NK225micro` and `TOPIXmini` on production `18080`, day and night sessions `Exchange=23,24`. It reads API settings from `config/local.json`. After registration it connects to kabu WebSocket and keeps running until you press `Ctrl+C`.

Console tick printing is off by default. Startup, heartbeat, signal, paper execution, and error events still print; full market data is still recorded to JSONL unless you change `--book-log-mode`. Microstructure signal decisions are written to JSONL as `signal_eval`, so you can inspect why a book was rejected without flooding the console.

Useful safe commands:

```powershell
python main.py --help
python main.py --test
python main.py --replay-sample
python main.py --sandbox --dry-run
python main.py --register-only
python main.py --tick-log-mode changes
```

## Paper Execution

Paper execution never sends real orders. It only reacts to tradeable `micro_book` signals.

```powershell
# Safe default: observe only, no paper position and no live order
python main.py

# Paper execution: immediate simulated fill at Signal.price
python main.py --trade-mode paper --paper-fill-model immediate

# Paper execution: conservative fill, wait until the book touches Signal.price
python main.py --trade-mode paper --paper-fill-model touch

# Replay a live log through the paper executor
python main.py --replay-sample --path logs\live_20260423_222539.jsonl --trade-mode paper --paper-fill-model touch
```

Paper events are written to the JSONL log as `paper_pending`, `paper_entry`, `paper_exit`, `paper_cancel`, and `execution_reject`. Heartbeats include `paper_position`, `paper_pending_order`, `paper_trades`, `paper_pnl_ticks`, and `paper_pnl_yen`. Live heartbeats also include `signal_eval_count`, `signal_allow_count`, and `signal_reject_count`.

## Replay JSONL Snapshots

```powershell
$env:PYTHONPATH="D:\kabu_futures\src"
python -m kabu_futures replay logs\live_20260423_222539.jsonl --config config\local.json --trade-mode paper
```

Each raw JSONL book row should contain at least:

```json
{"symbol":"NK225micro","timestamp":"2026-04-23T09:00:00+09:00","best_bid_price":50000,"best_bid_qty":100,"best_ask_price":50005,"best_ask_qty":80}
```

Current live logs use the buffered `{"kind":"book","payload":...}` format and are also supported by replay.

Notes for kabu futures data:

- `TradingVolume` is cumulative, so the bar builder converts it to per-bar incremental volume before computing VWAP and volume ratios.
- kabu can occasionally emit equal best quotes such as `BidPrice == AskPrice`; the normalizer records `market_data_error` and skips those snapshots instead of reconnecting.

## kabu API Token And Symbol Register

kabu Station does not use a fixed API key in config. The code sends your API password to `/token`, then uses the returned token as `X-API-KEY`.

You can either put the password in an environment variable, or put it in `config/local.json`. The local file is ignored by git.

`config/local.json`:

```json
{
  "api": {
    "api_password": "YOUR_KABU_API_PASSWORD",
    "api_password_env": "KABU_API_PASSWORD",
    "production_url": "http://localhost:18080/kabusapi",
    "sandbox_url": "http://localhost:18081/kabusapi"
  }
}
```

```powershell
cd D:\kabu_futures
$env:PYTHONPATH="D:\kabu_futures\src"
$env:KABU_API_PASSWORD="YOUR_KABU_API_PASSWORD"

# Sandbox 18081, day session Exchange=23
python -m kabu_futures api-register --exchange 23

# Use api_password in config/local.json
python -m kabu_futures api-register --config D:\kabu_futures\config\local.json --exchange 23

# Register day and night sessions
python -m kabu_futures api-register --exchange 23 --exchange 24

# Production 18080
python -m kabu_futures api-register --production --exchange 23
```

Relevant code:

- `src/kabu_futures/api.py`: `/token`, `/symbolname/future`, `/register` client.
- `src/kabu_futures/__main__.py`: `api-register` command.
- `config/defaults.json`: default `NK225micro`, `TOPIXmini`, URLs, and limits.
