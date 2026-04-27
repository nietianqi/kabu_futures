# AI 自动进化交易程序框架

## 1. 核心原则

AI 自动进化不等于让 AI 在线随意改实盘策略。当前项目采用更安全的闭环：

```text
记录行情和信号
→ 盘后复盘
→ markout/PNL 归因
→ 候选参数或候选逻辑实验
→ walk-forward 验证
→ promotion gate 审核
→ 通过后才允许进入小资金实盘
```

实盘进程只执行已经写入配置、且由人显式启动的逻辑。候选配置和报告不会自动覆盖 `config/local.json`。

## 2. 当前代码状态

当前仓库已经具备第一版 evolution MVP：

- JSONL 行情和事件记录：`src/kabu_futures/marketdata.py`
- 离线复盘和 markout：`src/kabu_futures/evolution.py`
- 参数网格评估：`src/kabu_futures/tuning.py`
- walk-forward 验证：`src/kabu_futures/walk_forward.py`
- promotion gate：`src/kabu_futures/promotion.py`
- paper micro/minute 执行：`src/kabu_futures/paper_execution.py`
- 受保护的真实下单通路：`src/kabu_futures/live_execution.py`
- JST session gate：`src/kabu_futures/sessions.py`

真实下单不是默认行为。需要显式命令：

```powershell
python main.py --real-trading
```

等价于：

```powershell
python main.py --trade-mode live --live-orders
```

Live v1 只支持 `micro_book` 信号，并且只允许一个活跃 live 仓位或挂单路径。`minute_orb`、`minute_vwap`、`directional_intraday` 不会直接实盘下单。

## 3. 2026-04-27 日志结论

最新长日志 `live_20260427_232822.jsonl` 是 observe 模式：

```text
trade_mode=observe
live_orders=false
```

因此 33 条 tradeable signal 全部是 `execution_skip: observe_mode`，不能用于评价真实成交或 paper PnL。

已有完整 pipeline 报告显示，聚合回放约 `401k` books 后：

```text
paper trades: 57
net PnL: -136 ticks
win rate: 5.26%
avg PnL: -2.39 ticks/trade
walk-forward pass rate: 0.0
promotion decision: reject
```

关键解释：

- 单纯调低 `imbalance_entry` 没解决问题。
- 0.5s 到 5s markout 为负，说明当前 micro 方向在短窗口内偏反。
- `trend_pullback_long/short` 表现很差，已默认降级为 observe-only。
- `micro_book` 方向反转只作为实验开关，默认关闭，必须先 paper/offline 验证。

## 4. 当前安全改动

根据日志分析，当前代码做了三件事：

1. `minute_vwap` 的 `trend_pullback_long/short` 默认 observe-only。
2. 新增 `micro_engine.invert_direction` 实验开关，默认 `false`。
3. kabu 的锁定/交叉盘口从 `market_data_error` 降级为 `market_data_skip`，避免把撮合瞬态误当连接错误。

建议下一轮采集先跑：

```powershell
python main.py --trade-mode paper --paper-fill-model immediate
```

确认 paper PnL、markout 和 reject 分布改善后，再考虑：

```powershell
python main.py --real-trading
```

## 5. 下一轮实验

优先实验顺序：

1. 保持 `trend_pullback_long/short` observe-only。
2. 离线测试 `micro_engine.invert_direction=true`。
3. 按 regime 拆分 micro_book markout，确认是否只在特定波动/时段有效。
4. 若候选通过 walk-forward 和 promotion gate，再进入小资金 live。

不建议：

- 只继续微调 `imbalance_entry`。
- 根据单日单方向信号直接上线。
- 让 minute 信号直接实盘。

## 6. Promotion Gate

候选版本必须至少满足：

- 样本内 PnL 优于 baseline。
- 样本外 walk-forward pass rate 达标。
- 短窗口 markout 不持续为负。
- 最大回撤没有恶化。
- 交易次数足够，不是样本偶然。
- 参数没有卡在搜索网格边界。

任一 veto 触发时，只保留报告，不写入正式配置。
