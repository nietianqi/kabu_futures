# Log Review Playbook

This project writes structured JSONL logs so live runs can be reviewed without guessing from console output. Use this checklist after every paper/live capture before changing strategy parameters.

## Quick Commands

```powershell
# Latest full offline attribution report. This is report-only.
python scripts\analyze_micro_evolution.py logs\live_20260428_124855.jsonl --config config\local.json --output reports\micro_evolution_latest.json

# Same report with explicit FAK entry fill assumptions.
python scripts\analyze_micro_evolution.py logs\live_20260428_124855.jsonl --config config\local.json --entry-fill-slippage-grid 0,1,2 --entry-fill-latency-ms 0,100,250,500,1000 --output reports\entry_fill_latest.json

# Conservative multi-grid search. This does not write config/local.json.
python scripts\tune_micro_params.py logs\live_20260428_124855.jsonl --config config\local.json --imbalance-grid 0.18,0.20,0.22,0.25,0.30 --microprice-grid 0.10,0.12,0.15 --spread-grid 1,2 --output reports\micro_tuning_latest.json

# Fast smoke check when the log is large.
python scripts\analyze_micro_evolution.py logs\live_20260428_124855.jsonl --config config\local.json --max-books 50000 --no-regime
```

## What To Check

- `entry_diagnostics.failed_checks_top`: the best first answer to "why are trades sparse?". `imbalance_not_met`, `ofi_not_met`, `microprice_not_met`, and `spread_not_required_width` mean the micro front gates are the bottleneck.
- `entry_fill_diagnostics.observed_live_funnel`: the live FAK entry funnel. Compare `entry_submitted`, `own_fills_detected`, `expired_unfilled`, `api_errors`, `rejected`, `timeout_after_grace`, `cooldown_rejects`, and `fill_rate` before changing execution settings.
- `entry_fill_diagnostics.fak_fill_simulation`: an offline estimate for `entry_slippage_ticks` under latency assumptions. A long simulated order fills when the adjusted limit is at or above the future best ask; a short fills when it is at or below the future best bid.
- `signals.by_key` and `paper.positions[*].symbol`: confirm whether `NK225micro` and `TOPIXmini` are both producing executable `micro_book` paths. TOPIXmini uses `tick_sizes.TOPIXmini=0.25` and `tick_values_yen.TOPIXmini=250`, so do not compare raw price moves to Nikkei's 5-point tick.
- `evaluations.decisions`: the micro allow/reject rate. A very low allow rate is expected for conservative settings, but it should be explained by the diagnostics before tuning.
- `signals.by_key`: confirms which engines produced signals. Minute signals can appear in research logs while live execution remains gated to `micro_book`.
- `paper.trades`, `paper.net_pnl_ticks`, and `markout.summary`: frequency alone is not enough. Only consider candidates that improve trades without damaging PnL, average PnL, drawdown, or markout.
- `paper.execution_reject_reasons` and recorded `execution_reject`: live-only rejects such as `live_unsupported_signal_engine` are safety gates, not strategy conflicts.
- Heartbeat fields `live_order_errors`, `live_position_count`, `live_pending_entry`, `live_pending_exits`, `live_exit_retry_after`, `live_exit_blocked`, `live_exit_failure_counts`, `live_entry_failure_count`, and `live_entry_cooldown_until`: these show whether live order management is healthy.
- `market_data_skip`: occasional equal/crossed kabu quotes are normal. A sudden spike can distort feature counts and should be reviewed before trusting a tuning run.

## 2026-04-28 Findings

The 2026-04-28 logs show sparse trading is mainly a micro-entry gate issue, not strategy conflict:

- Across `217,063` books and `136,532` micro evaluations, only `16` evaluations were allowed, for an allow rate of about `0.0117%`.
- Top micro reject reasons were `imbalance_not_met` (`100,767`), `spread_not_required_width` (`30,559`), `ofi_not_met` (`2,689`), and `jump_detected` (`2,041`).
- Metadata-level failed checks were even clearer: `imbalance_not_met` (`132,721`), `ofi_not_met` (`131,889`), `microprice_not_met` (`55,544`), and `spread_not_required_width` (`31,350`).
- The latest log, `live_20260428_124855.jsonl`, ended cleanly: `live_order_errors=0`, `live_position_count=0`, `live_pending_entry=null`, and `live_pending_exits=[]`.
- `live_20260428_124855.jsonl` had `7` `live_unsupported_signal_engine` rejects, all from `minute_vwap`. This is expected while live defaults intentionally support only `micro_book`.
- The early `live_20260428_083434.jsonl` run had `7,384` `exit_order_api_error` events caused by kabu `HTTP 429` API rate-limit responses. Treat this as an execution-health red flag; later logs did not reproduce it after exit retry/backoff handling was added.

## Safe Interpretation

- Do not enable minute engines in live just because `minute_vwap` appears in logs. Treat those signals as research evidence until a separate opt-in live plan is reviewed.
- TOPIXmini is now a live-capable `micro_book` trade symbol, not only a filter. If you only want Nikkei entries for a run, set `symbols.trade=["NK225micro"]` locally before startup.
- Do not loosen live thresholds directly from one log. Run the multi-grid tuner, then require `recommendation.decision == "recommended"`, then validate in paper or tiny live size.
- Do not change `live_execution.entry_slippage_ticks` just because 0-tick FAK fills are weak. First require a clear 1-tick simulation improvement, then confirm paper PnL and markout are still acceptable, then manually opt in through `config/local.json` with tiny size.
- If kabu `HTTP 429` appears again, stop evaluating strategy quality from that run first. The order path was rate-limited, so execution health must be fixed before PnL conclusions are useful.
- If `live_position_count > 0` and `live_pending_exits` is empty for more than one heartbeat, investigate immediately because a synced live position may not have a resting TP order.
- If `live_exit_blocked` is non-empty or an event reason is `exit_order_blocked_after_retries`, the bot has stopped automatic TP resubmission for that hold ID. Check kabu Station manually before restarting or re-enabling new entries.

