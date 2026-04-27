# kabu-futures

Python strategy framework generated from `docs/kabu_micro225_minitopix_dual_strategy.md`.

The code is intentionally safe by default:

- Microstructure mode defaults to `observe_only`.
- `python main.py` runs in `observe` execution mode by default and never sends real orders.
- `--trade-mode paper` creates simulated paper positions only; live `/sendorder/future` is not enabled.
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
- `src/kabu_futures/paper_execution.py`: paper-only controller/executor lifecycle for micro and minute-level signals.
- `src/kabu_futures/live_execution.py`: explicitly gated kabu `/sendorder/future` executor for real futures orders.
- `src/kabu_futures/sessions.py`: JST futures session classification and new-entry gate.
- `src/kabu_futures/evolution.py`: offline replay report with signal attribution, paper PnL, and markout.
- `src/kabu_futures/tuning.py`: report-only micro parameter grid evaluation.
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

For the next research capture, use paper mode so the JSONL contains simulated entries/exits and PnL:

```powershell
python main.py --trade-mode paper --paper-fill-model immediate
```

## JST Session Gate

The strategy uses a unified JST session gate. Market data, bars, micro features, and signal diagnostics continue to be recorded in every phase, but new entries are allowed only during JPX continuous trading: `08:45-15:40 JST` and `17:00-05:55 JST`. Outside those windows, tradeable strategy signals are converted to non-tradeable `risk` signals with reason `session_not_tradeable`; existing paper positions can still exit normally.

| JST time | Phase | New entries |
| --- | --- | --- |
| 06:15-06:30 | kabu/API maintenance | No |
| 06:30-08:00 | API/login/contract preparation | No |
| 08:00-08:45 | Day preopen order acceptance | No |
| 08:45-15:40 | Day continuous trading | Yes |
| 15:40-15:45 | Day closing call | No |
| 15:45-16:45 | Between sessions | No |
| 16:45-17:00 | Night preopen order acceptance | No |
| 17:00-05:55 | Night continuous trading | Yes |
| 05:55-06:00 | Night closing call | No |
| 06:00-06:15 | Post-close processing | No |

Practical rhythm: start kabu Station after `06:30`, prepare symbols before `08:45`, stop aggressive new entries at `15:40`, prepare night session at `16:45`, trade again from `17:00`, stop new entries at `05:55`, then save/exit before `06:15`. The old `micro_engine.no_new_entry_windows_jst` field still exists for extra local blackout windows, but its default is empty because the session gate now owns the official JPX/kabu schedule.

`directional_intraday` long signals are also observe-only by default after the latest log review. They remain visible in logs as diagnostics, but do not create paper entries unless `minute_engine.directional_intraday_long_observe_only` is disabled in config.

## Paper Execution

Paper execution never sends real orders. It reacts to tradeable `micro_book` signals and minute-level signals (`minute_orb`, `minute_vwap`, `directional_intraday`) for offline/paper research.

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

Paper events are written to the JSONL log as `paper_pending`, `paper_entry`, `paper_exit`, `paper_cancel`, `execution_reject`, and `execution_skip`. In observe mode, tradeable signals produce `execution_skip` with reason `observe_mode`. V1 keeps a single active paper position across micro and minute executors. Heartbeats keep the legacy aggregate fields `paper_position`, `paper_pending_order`, `paper_trades`, `paper_pnl_ticks`, and `paper_pnl_yen`, and add split fields such as `paper_micro_position`, `paper_minute_position`, `paper_micro_trades`, and `paper_minute_trades`.

## Live Execution

Real kabu futures orders are available only when both switches are present:

```powershell
python main.py --trade-mode live --live-orders
```

Live v1 is intentionally narrow:

- supports only `micro_book` signals;
- rejects `minute_orb`, `minute_vwap`, and `directional_intraday` with `live_unsupported_signal_engine`;
- submits FAK limit entry orders through `/sendorder/future`;
- polls `/positions?product=3&symbol=...` to confirm the real position;
- uses the same micro exit logic for take-profit, stop-loss, time-stop, and feature exits;
- submits close orders through `/sendorder/future`, using `ClosePositions` when a hold ID is available;
- keeps one active live position/order path at a time.

Live events are written to JSONL as `live_order_submitted`, `live_order_error`, `live_position_detected`, `live_position_flat`, and `live_sync_error`. Heartbeats include `live_position`, `live_pending_entry`, `live_pending_exit`, `live_orders_submitted`, and `live_order_errors`.

## Offline Evolution And Tuning

Replay a JSONL log through the current strategy and paper executor, then summarize reject reasons, paper PnL, hourly behavior, and markout:

```powershell
python scripts\analyze_micro_evolution.py logs\live_20260427_145035.jsonl --config config\local.json --max-books 50000
python scripts\analyze_micro_evolution.py logs\live_20260427_145035.jsonl --output reports\micro_evolution_report.json
```

Markout is the post-entry mid-price move at fixed horizons. For a long entry it is `future_mid - entry_price`; for a short entry it is `entry_price - future_mid`. The report converts markout to ticks so it can be compared with spread, stop, and take-profit settings.

Run the conservative imbalance experiment from the latest log analysis:

```powershell
python scripts\tune_micro_params.py logs\live_20260427_145035.jsonl --config config\local.json --imbalance-grid 0.18,0.20,0.22,0.25,0.30
```

The tuner is report-only. It never overwrites `config/local.json`; use the output as a challenger candidate for review.

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
- **kabu Bid/Ask reversal**: kabu PUSH board encodes `BidPrice` as the best *sell* quote and `AskPrice` as the best *buy* quote, the opposite of standard market data conventions. `KabuBoardNormalizer` corrects this mapping so that `OrderBook.best_bid_price` is always the highest buyer price and `best_ask_price` the lowest seller price. This is also documented in `OrderBook` class docstring in `models.py`.

## Signal Flow

```
kabu WebSocket
  -> KabuBoardNormalizer.normalize()
  -> OrderBook
  -> DualStrategyEngine.on_order_book()
     -> BarBuilder (1-min) -> MinuteStrategyEngine.on_bar() -> Signal
     -> BookFeatureEngine.update() -> BookFeatures
        -> MicroStrategyEngine.evaluate_book() -> Signal
     -> MultiTimeframeScorer.score() -> MultiTimeframeScore
     -> StrategyArbiter (NT-spread + lead-lag veto) -> StrategyIntent
     -> RiskManager / OrderThrottle -> allow / reject
     -> [Signal list] -> PaperExecutionController.on_signal()
```

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
- `config/defaults.json`: default `NK225micro`, `TOPIXmini`, URLs, session schedule, and limits.

The kabu API rate limits are controlled by config: order requests default to `5/sec`, while information, margin, and symbol registration requests default to `10/sec`.
