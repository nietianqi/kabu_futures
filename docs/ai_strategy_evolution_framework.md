# AI 自动进化交易程序框架

本文整理 AI 如何帮助交易程序进化。这里的“自动进化”不是让 AI 随便修改策略后直接实盘，而是把 AI 放进一个受控的研究、复盘、验证和灰度流程中，让它帮助交易系统持续改进。

核心原则：

```text
AI 自动进化 = 自动记录 -> 自动复盘 -> 自动找问题 -> 自动调参/调权重
             -> 自动小流量验证 -> 达标后替换旧版本
```

实盘策略只应该加载已经验证过的配置。进化过程应该尽量放在盘后完成，不能绕过风控直接修改线上交易逻辑。

---

## 目录

1. [核心原则](#1-核心原则)
2. [当前 kabu_futures 项目现状](#2-当前-kabu_futures-项目现状)
3. [AI 可以进化什么](#3-ai-可以进化什么)
4. [六个进化维度](#4-六个进化维度)
5. [适合当前 kabu 微观策略的进化方向](#5-适合当前-kabu-微观策略的进化方向)
6. [自动进化闭环](#6-自动进化闭环)
7. [分步骤实施路线](#7-分步骤实施路线)
8. [反模式与危险信号](#8-反模式与危险信号)
9. [MVP 版本](#9-mvp-版本)
10. [推荐项目结构](#10-推荐项目结构)
11. [不建议一开始使用强化学习](#11-不建议一开始使用强化学习)
12. [最终结论](#12-最终结论)
13. [附录 A：依赖与环境](#附录-a依赖与环境)
14. [附录 B：术语表](#附录-b术语表)

---

## 1. 核心原则

AI 自动进化交易程序时，需要先明确边界。

第一，AI 不应该直接拥有无限制的实盘修改权。它可以提出候选参数、候选信号组合、候选策略版本，但不能跳过回测、模拟盘、小资金验证和风控门槛。

第二，交易程序的进化应该依赖实盘日志和统计结果，而不是凭感觉改策略。一次正确的进化通常来自这样的过程：

```text
发现某组信号通过率太低
-> 复盘信号被拒绝的原因
-> 分析哪些信号真正有预测力
-> 调整主信号和辅助信号关系
-> 回测验证
-> 模拟盘观察
-> 小资金灰度
-> 达标后替换 champion
```

第三，核心风控不应该交给 AI 随意改。最大日亏损、断线保护、订单限速、熔断条件、最大仓位等规则，应该由人设定上限，AI 只能在受控范围内建议调整。

---

## 2. 当前 kabu_futures 项目现状

进化方法论必须对接代码现实。下面列出当前仓库已就位、可直接复用的能力，以及还缺的部分。

### 2.1 已就位的基础设施

| 能力 | 实现位置 | 说明 |
|---|---|---|
| 实时行情归一化 | `src/kabu_futures/marketdata.py` | `KabuBoardNormalizer` 修正 kabu Bid/Ask 反转语义 |
| WebSocket 接入 | `src/kabu_futures/live.py` | 含 `MarketDataSkip` 降级、`SignalEvaluationLogger` 聚合 |
| JSONL 行情记录 | `BufferedJsonlMarketRecorder` (marketdata.py) | 全 tick 持久化 |
| 离线重放 | `src/kabu_futures/replay.py` | `read_recorded_books()` 已是生成器，`replay_jsonl()` 端到端 |
| 双层策略引擎 | `src/kabu_futures/engine.py` | `DualStrategyEngine` 编排 minute/micro/NT 套利/lead-lag |
| 微观特征 | `src/kabu_futures/microstructure.py` | OFI、加权 imbalance、microprice、jump 检测 |
| Paper 执行 | `src/kabu_futures/paper_execution.py` | 支持 `micro_book` 与 minute 级信号；micro 含 immediate / touch fill model |
| 配置校验 | `src/kabu_futures/config.py` | `StrategyConfig.validate()` 在创建时调用 |
| **离线参数调优** | `src/kabu_futures/tuning.py` | `tune_micro_params()`、`evaluate_micro_config()`、`challenger_micro_engine_payload()` |
| **CLI 调优工具** | `scripts/tune_micro_params.py` | 可生成 challenger 配置但不覆盖 champion |
| 进化分析 | `scripts/analyze_micro_evolution.py` | 进化结果对比脚本 |

### 2.2 仍需补齐的部分

| 缺失能力 | 影响 | 实现位置建议 |
|---|---|---|
| HTML 每日复盘报告 | JSON 复盘已有，仍缺人工快速阅读的图表报告 | 新建 `scripts/daily_report.py` |
| 持久化 markout 字段 | `analyze_micro_log()` 已可盘后计算，live JSONL 事件中尚未直接写入字段 | 后续可在 `paper_execution.py` 事件或独立 report 中补齐 |
| Walk-forward 滚动验证 | 已支持多日志输入、滚动 train/test_days 与不足样本诊断，仍需更多完整交易日验证 | 继续用 `src/kabu_futures/walk_forward.py` 与脚本累积观察 |
| Regime 切换落地 | 基础高低波分类已就位，仍需接入调参报告和配置池 | 完善 `src/kabu_futures/regime.py` 的使用链路 |
| Champion/Challenger 自动晋升 | 基础 promotion gate 与复盘 metrics 已就位，仍需真实观察期和人工审批流程 | 完善 `src/kabu_futures/promotion.py` 与脚本 |
| 标签数据集 (用于将来 ML) | meta-confidence 模型缺训练样本 | 待 paper_trades 累积到一定量后从 JSONL 抽取 |

### 2.3 当前的紧迫问题

最近一份日志（`logs/live_20260424_132628.jsonl`）的历史诊断显示：

- 99% 以上 signal_eval 被 reject（imbalance_not_met 64.3%）
- 54 分钟仅 5 条 signal 通过策略层
- 旧版 paper executor 只处理 `micro_book`，minute 级 signal 会被 `non_micro_signal` 拒绝

这条链路已经通过 `PaperMinuteExecutor` 补上：paper 复盘现在同时覆盖 `micro_book`、`minute_orb`、`minute_vwap`、`directional_intraday`。多日志 walk-forward 与 promotion metrics 也已打通。下一步的紧迫问题不再是 executor 断流，而是继续降低策略层 reject、积累完整交易日样本，让统计结果可信。

---

## 3. AI 可以进化什么

对交易策略来说，最适合自动进化的是参数、权重、信号组合、市场状态过滤和版本淘汰机制。

| 进化对象 | 举例 | 自动化建议 |
|---|---|---|
| 信号阈值 | book imbalance 使用 0.35 还是 0.45 | 适合自动 |
| 信号权重 | Tape-OFI 权重大，还是 LOB-OFI 权重大 | 适合自动 |
| 入场条件 | book + tape 为主，其他信号辅助确认 | 适合自动 |
| 出场逻辑 | 1 tick 止盈、时间止损、Flow Flip | 适合自动 |
| 交易时间段 | 开盘后 10 秒交易还是 30 秒后交易 | 适合自动 |
| 标的筛选 | 哪些期货或股票的流动性适合交易 | 适合自动 |
| 仓位大小 | 根据近期胜率、回撤、滑点动态调整 | 半自动 |
| 核心风控 | 最大亏损、断线保护、日内熔断 | 不建议自动随意修改 |

AI 最适合做的是研究员、复盘员、参数优化员和风控审查员，而不是不受约束的实盘交易员。

---

## 4. 六个进化维度

### 4.1 从回测拟合到发现深层规律

传统策略容易把历史数据“背熟”，在回测中表现很好，但换到未来市场就失效。AI 的价值在于帮助发现更复杂的非线性关系，例如订单流、盘口深度、成交节奏和短期价格变动之间的隐藏模式。

在 tick 级或盘口级策略中，AI 可以辅助分析：

- 哪些盘口结构在未来 0.5 秒、1 秒、3 秒后有正 markout。
- 哪些信号组合只是历史偶然有效。
- 哪些特征在不同交易日和不同波动状态下仍然稳定。
- 哪些因子虽然胜率高，但扣除滑点和手续费后没有真实优势。

这里的重点不是让深度模型直接下单，而是让 AI 帮助识别更稳健的信号结构。**对 kabu 微观策略的当前阶段，深度学习不是优先项**——先用统计量（IC、markout、滑点扣减后的期望值）把信号筛干净，再考虑模型化。

### 4.2 从处理数字到理解市场事件

传统程序主要处理价格、成交量、盘口等结构化数据。AI 可以进一步处理新闻、公告、央行声明、交易所规则变化、宏观事件和舆情等非结构化信息。

对日股、期货和 kabu Station 交易系统来说，这类能力可以用于：

- 标记 BOJ、FOMC、CPI、SQ 等事件窗口。
- 在重大事件前降低微观剥头皮信号权重。
- 将新闻或公告转换成风险标签，而不是直接生成买卖信号。
- 在异常交易日自动要求更严格的滑点和回撤门槛。

这类 AI 更适合作为市场状态识别器和风险过滤器，而不是直接替代策略主体。

### 4.3 从静态策略到动态适应

市场状态会变化。趋势策略可能在震荡市失效，均值回复策略可能在单边市失效，高频微观策略也可能在流动性差、价差扩大、订单簿变薄时失效。

AI 可以帮助交易程序识别 regime：

```text
低波动趋势
高波动趋势
低波动震荡
高波动震荡
流动性正常
流动性枯竭
事件冲击
开盘/收盘异常状态
```

在当前阶段，更稳妥的做法不是直接使用强化学习控制买卖，而是先做 regime router：

```text
市场状态识别 -> 选择允许运行的策略 -> 调整信号权重 -> 调整仓位和风控预算
```

也就是说，AI 先帮助回答“当前市场适不适合交易”，再回答“应该使用哪组参数”。**起步建议二档分类（高/低波动）即可**，不要一上来就八档；用 30 分钟滚动 ATR 的分位数做切分就够。

### 4.4 从程序执行到智能执行

传统执行程序只负责按规则下单，例如信号出现后直接市价或限价委托。智能执行更关注滑点、成交概率、订单失败率和市场冲击。

AI 可以帮助执行层做这些事情：

- 判断当前适合主动吃单还是被动挂单。
- 根据盘口厚度和成交节奏调整挂单激进程度。
- 识别短时间内价差扩大、盘口撤单、流动性变薄等风险。
- 统计不同执行方式的真实滑点和成交质量。
- 对订单失败、超时、撤单重挂进行归因。

对微观 taker/scalp 策略而言，执行质量会直接决定策略是否还能赚钱。一个信号在理论上有 1 tick 优势，但如果平均滑点和手续费超过优势，策略就没有实际价值。

**当前 kabu_futures 已经支持 immediate / touch 两种 paper fill model**（见 `paper_execution.py`），可作为执行质量评估的起点。先把 markout 和实际滑点统计起来，再讨论"智能执行"。

### 4.5 从单一配置到配置池

> 注：业界常说的“群体智能”是指元模型管理上百个独立策略。**对单标的微观剥头皮策略而言这不适用**——你不会同时跑 100 个完全不同的微观策略。更现实的是“**配置池**”：同一份策略代码，多个被验证过的配置版本，根据市场状态调度。

系统中可以同时存在：

```text
保守配置 (低波动友好)
激进配置 (高波动友好)
开盘专用配置 (流动性好、噪声大)
夜盘专用配置 (流动性差、价差宽)
事件前禁用配置 (BOJ/FOMC/SQ 前)
```

调度器的角色：

- 监控每个配置版本的近期表现。
- 识别配置之间的相关性是否过高。
- 淘汰持续恶化的版本。
- 保留在不同市场状态下互补的版本。
- 根据 regime 给不同版本分配观察权重或小资金权重。

这比只优化一个全局参数集合更稳健。

### 4.6 从防御性风控到预测性风控

传统风控通常是事后触发，例如亏损达到阈值后止损。预测性风控更强调在亏损扩大前识别异常。

AI 可以帮助识别：

- 流动性突然枯竭。
- 策略胜率结构恶化。
- markout 从正变负。
- 滑点持续扩大。
- 订单失败率异常升高。
- 某些时间段或标的开始稳定亏损。
- 当前市场状态与策略历史盈利环境不一致。

预测性风控不是取消传统风控，而是在传统风控之前增加一层预警。它可以提前降仓、暂停某些标的、切换 observe_only，或者要求人工确认。

---

## 5. 适合当前 kabu 微观策略的进化方向

当前 kabu futures 项目已经有微观结构相关模块，适合先围绕盘口、订单流和 paper/replay 数据做数据驱动进化。

重点信号包括：

```text
加权盘口不平衡    (BookFeatureEngine.imbalance)
LOB-OFI           (BookFeatureEngine.ofi)
microprice        (BookFeatureEngine.microprice)
microprice tilt   (microprice_edge_ticks)
spread regime     (spread_ticks)
跳变检测          (jump_detected)
延迟监控          (latency_ms / event_gap_ms)
```

> Tape-OFI（逐笔成交流不平衡）目前未实装；如果将来加上，应作为辅助信号而非主信号。

最适合先自动进化的方向（按性价比排序）：

1. **`imbalance_entry` 阈值**——当前价值最大，因为日志显示这是 64% reject 的来源。
2. **`spread_required_ticks` 容忍度**——25% reject 来自此项。
3. **`take_profit_ticks` / `stop_loss_ticks`**——出场参数。
4. **`time_stop_seconds`**——持仓时长。
5. **`min_order_interval_seconds`**——频率与排队成本。
6. 标的级开关（NK225micro vs TOPIXmini，哪个值得开）。
7. 时段级开关（开盘 / 中午 / 收盘 / 夜盘）。

一个更现实的策略结构是：

```text
主信号：book imbalance + microprice edge
辅助信号：LOB-OFI / jump filter
过滤条件：spread、波动 regime、事件窗口、订单失败率
```

不建议一开始就把所有信号硬 AND 在一起。微观结构信号之间可能互相拮抗，尤其在 queue 市场中，盘口强弱和 OFI 的方向不一定总是同步。

### 5.1 代码资产到本文术语的对照

| 本文术语 | 对应代码 |
|---|---|
| 自动复盘 | `evolution.py` 的 `analyze_micro_log()` + `scripts/analyze_micro_evolution.py`；HTML `daily_report.py` 待补 |
| 参数搜索 | `tuning.py` 的 `tune_micro_params()` |
| Walk-forward | `walk_forward.py` 已支持多日志滚动验证；待更多完整交易日验证 |
| Challenger 配置生成 | `tuning.py` 的 `write_challenger_micro_engine()` |
| Challenger 部署 | 把生成的 JSON 放到 `config/challenger.json`，由运维手动切换 |
| Champion 配置 | `config/local.json` 或 `config/defaults.json` |
| signal_eval 复盘 | live JSONL 中 `kind: signal_eval_summary` 已聚合 |
| markout 分析 | `analyze_micro_log()` 已支持盘后计算、按 engine 归因和 promotion metrics；live 事件持久化字段待补 |
| Regime 识别 | `regime.py` 基础高低波分类已就位；待纳入调参和报告 |

---

## 6. 自动进化闭环

完整闭环可以分为七层。

```text
1. 实盘/模拟盘数据记录
        ↓
2. 特征计算
        ↓
3. 交易结果归因
        ↓
4. 参数搜索 / 模型训练
        ↓
5. Walk-forward 回测
        ↓
6. 模拟盘 / 小资金灰度
        ↓
7. 达标后替换线上策略配置
```

每天盘后，系统应该自动回答：

- 今天哪些信号赚钱？
- 哪些信号亏钱？
- 哪些时间段亏钱？
- 哪些标的不适合？
- 哪些参数组合比当前版本更稳？
- 新版本是否真的比旧版本好？
- 是否值得进入模拟盘或小资金灰度？

---

## 7. 分步骤实施路线

### 第 0 步：先解决样本不足（前置条件，不可跳过）

`PaperMinuteExecutor` 已经补齐 minute 信号的 paper 通路，但当前日志仍然 signal 数偏少，所有进化机制都需要更多有标签交易样本。必须先：

1. `tuning.py` 已能跳过非法参数组合，继续用 `invalid_combos` 观察搜索空间是否设计过宽。
2. 跑一次 `scripts\tune_micro_params.py`，评估是否应把 `imbalance_entry` 从 0.30 降到 p85 分位附近（约 0.18~0.20）。
3. 确认主程序运行模式是 `--trade-mode paper` 而非默认 `observe`。
4. 目标：每小时 signal 数从当前个位数提升到 50+，每日 paper_trades ≥ 20。

不达成第 0 步，第 1-6 步仍可运行，但统计可信度不足，不能用于自动晋升。

### 第 1 步：自动复盘系统

先把每笔交易和每次信号判断记录完整。没有稳定日志，就没有可靠进化。

**已有字段（live JSONL 已经记录）**：

```text
kind=book          : timestamp, best_bid_price/qty, best_ask_price/qty, last_price, volume, raw_symbol
kind=signal        : engine, symbol, direction, confidence, reason, metadata
kind=signal_eval_summary : reason, count, decision, candidate_direction, start_ts, end_ts
kind=paper_entry/paper_exit : entry_price, exit_price, pnl_ticks, exit_reason, holding_seconds；已覆盖 micro 与 minute paper
```

**待补字段（应在 `paper_execution.py` 退出事件里加）**：

```text
markout_500ms     : 入场后 0.5 秒 mid_price - entry_price (long) 或反向 (short)
markout_1s
markout_3s
markout_10s
session_phase    : open_30min / morning / lunch_break / afternoon / close_30min / night
spread_at_entry  : 入场瞬间 spread (tick)
slippage_ticks   : entry_price - signal_price (long) 或反向
```

markout 计算逻辑示例：

```text
做多 markout_1s = 1 秒后 mid_price - entry_price
做空 markout_1s = entry_price - 1 秒后 mid_price
```

**判读原则**：

- markout 持续为正：信号有真实预测力。
- 胜率高但 markout 差：可能只是 take_profit 卡得紧、偶然吃到利润，长期不稳。
- markout 短期正、长期负：信号是噪声反转，不是趋势。
- markout 持续为负：信号方向反了或被对手抢跑。

每日盘后生成 `reports/daily_YYYYMMDD.html`，至少含：

- 当日 signal 漏斗（产出 / 通过 / 入场 / 退出 / PnL）
- 按 reason 分组的 reject 排序
- 按 session_phase 分组的胜率与平均 markout
- 当日最赚 / 最亏 5 笔归因

### 第 2 步：信号评分系统

不要一开始让 AI 直接决定买卖。先让它输出 signal score。

示例：

```text
score =
  0.40 * book_score
+ 0.25 * microprice_score
+ 0.20 * lob_ofi_score
+ 0.10 * jump_filter_score
+ 0.05 * spread_score
```

然后比较不同候选版本：

```text
版本 A：book 40%, microprice 25%, ofi 20%, jump 10%, spread 5%
版本 B：book 30%, microprice 30%, ofi 20%, jump 15%, spread 5%
版本 C：book + microprice 必须共振 (硬 AND)，ofi 辅助
版本 D：任一信号极强 (>p95) 即可入场
```

评估指标不能只看收益，还要看：

- 净收益（扣手续费、滑点）
- Profit Factor (∑ win / ∑ |loss|)
- 最大回撤
- 单笔期望值
- 胜率
- 平均持仓时间
- 订单失败率
- 平均滑点
- 1s / 3s / 10s markout
- 交易次数

### 第 3 步：Optuna 自动调参

> **当前已有基础**：`src/kabu_futures/tuning.py` 实现了 grid search 版本。下一步是把它升级成 Optuna，搜索效率会显著提高。

搜索空间示例（注意上下界要合理）：

```text
imbalance_entry         : 0.15 ~ 0.45  (当前 0.30，日志 p99=0.32)
microprice_entry_ticks  : 0.05 ~ 0.40
spread_required_ticks   : 1 ~ 3
take_profit_ticks       : 1 ~ 4
stop_loss_ticks         : 2 ~ 6
time_stop_seconds       : 10 ~ 60
min_order_interval_seconds : 1 ~ 10
```

**目标函数（推荐）**：

```python
score = (
    net_pnl_ticks
    - 0.5 * trades                    # 惩罚过高换手
    - 2.0 * max_drawdown_ticks        # 惩罚回撤
    - 1.0 * avg_slippage_ticks * trades  # 滑点扣减
)
```

迭代顺序（与现状对接）：

```text
当前: tuning.py 的 default_micro_grid (4 维 × 4~5 档 = 100~625 组合，全跑)
下一步: 升级为 Optuna TPE，给定相同 budget (200 trials)，覆盖更细
将来: 加入分类参数（信号组合策略），用 Optuna 多目标
```

### 第 4 步：Walk-forward 验证

不能只用过去一整段历史优化一次后直接上线，这很容易过拟合。

推荐使用滚动验证：

```text
窗口 1: 第 1-10 天训练 -> 第 11 天测试
窗口 2: 第 2-11 天训练 -> 第 12 天测试
窗口 3: 第 3-12 天训练 -> 第 13 天测试
...
```

**对 kabu 微观策略（每日大量数据）的具体建议**：

| 数据量阶段 | 训练窗口 | 测试窗口 | 滚动步长 |
|---|---|---|---|
| 起步（< 20 个交易日）| 5 天 | 1 天 | 1 天 |
| 稳定（20-60 个交易日）| 10 天 | 1 天 | 1 天 |
| 成熟（> 60 个交易日）| 20 天 | 5 天 | 5 天 |

**通过门槛**：候选参数集必须在 ≥ 70% 的滚动窗口里测试段净 PnL > 0，且全部窗口的中位数 PnL 优于 baseline，才能进入 challenger。

### 第 5 步：Champion / Challenger 机制

线上版本应该分为两类：

```text
Champion：当前正式版本 (config/local.json 或 config/defaults.json)
Challenger：候选进化版本 (config/challenger.json，由 tuning.py 写出)
```

新版本不是收益高就上线。**必须满足全部硬门槛**：

| 硬门槛 | 推荐默认值 | 说明 |
|---|---|---|
| 净 PnL 优于 champion | delta > 0 | walk-forward 中位数 |
| 最大回撤 ≤ champion × 1.2 | 不能恶化 20% 以上 | |
| 每日交易数 ≥ 阈值 | ≥ 10 笔（单标的微观）| 太少则统计不可信 |
| 平均 markout_3s | > 0.5 tick | 真实预测力 |
| 平均滑点 ≤ champion × 1.1 | 不能显著恶化 | |
| 连续亏损笔数 ≤ champion × 1.2 | | |
| Walk-forward 通过率 ≥ 70% | 见第 4 步 | |
| 持续观察 ≥ 14 个交易日 | 不要 5 天就切 | |

**一票否决条件（任一触发即拒）**：

```text
单日亏损超过限制
连亏次数 ≥ 5 笔
订单失败率 > 5%
数据延迟 / 断线频繁
任一参数变化 > ±50%（防离群解，例如 imbalance_entry 从 0.30 跳到 0.10）
搜索结果落在 grid 边界（说明搜索空间设计有问题，应扩大空间重跑）
```

例如，`book_imbalance` 从 0.30 调到 0.32 可以接受；如果从 0.30 直接变成 0.45，应人工确认是否合理。

### 第 6 步：小资金验证

通过 walk-forward 与 Champion/Challenger 门槛的版本，仍**不能直接替换 champion**。推荐流程：

```text
walk-forward 通过 (≥ 70% 窗口胜出)
-> Paper trading 14 个交易日 (并行 champion 与 challenger)
-> 1 手 / 最小资金实盘 7 个交易日
-> 标准资金 14 个交易日
-> 替换 champion
```

实盘资金应该按阶段放大。每个阶段都需要检查滑点、成交质量、异常订单和回撤是否符合预期。任何一个阶段不达标，回退一档而非直接停。

---

## 8. 反模式与危险信号

进化过程中必须警惕这些情况，**遇到任一项必须人工介入**：

### 8.1 调参结果的危险信号

| 现象 | 含义 | 应对 |
|---|---|---|
| 最优参数落在 grid 边界 | 搜索空间设计错误，真实最优在外面 | 扩大搜索空间重跑，**不要直接采纳** |
| 最优参数与 baseline 差异极大（>50%）| 可能过拟合到样本特殊事件 | 换样本重测，看是否稳定 |
| 胜率 > 70% 但单笔期望 < 0.2 tick | 滑点 + 手续费会吃光优势 | 拒绝 |
| 净 PnL 为正但 max_drawdown > 5 × 单笔期望 | 偶发大单亏损被多笔小盈利掩盖 | 拒绝 |
| 交易笔数 < min_trades 阈值 | 统计不可信 | 拒绝 |
| 回测漂亮但实盘 paper 不复现 | 数据泄漏 / 撮合模型不真实 | 全面回查 |

### 8.2 进化过程的反模式

| 反模式 | 危害 | 正确做法 |
|---|---|---|
| 频繁微调参数（每天换一组）| 进化噪声压过信号，无法判断改进来源 | 至少 14 天观察期才考虑切换 |
| 一次只优化一个目标（净 PnL）| 牺牲回撤、滑点等被忽视维度 | 多目标评分函数 |
| 把进化结果直接覆盖 champion | 没有回滚能力 | Challenger 单独存储，人工或自动晋升器切换 |
| 用全样本数据做 hyperparameter selection | 严重过拟合，未来必失效 | 严格 walk-forward |
| 信任单一回测窗口的结果 | 历史样本特殊性会被误读为规律 | 多窗口、多市况、bootstrap |
| 让 LLM 直接生成策略代码替换现有 | 引入隐性 bug、数据泄漏 | LLM 只用于审查、归因、报告 |
| 用 RL 训练后直接实盘 | sim-to-real gap 很大 | 当前阶段不用 RL |

### 8.3 必须立即停止进化的情况

```text
连续 3 个交易日实盘 paper PnL 为负
连续 3 个交易日 markout 中位数 < 0
日志中 market_data_error 突然增多 (连接问题)
订单失败率 > 5% 且持续
策略胜率结构发生质变 (例如从均匀分布变成尖峰)
当前 regime 从未在历史训练数据中出现过
```

任何一项触发，立即切回 champion 上一稳定版本，停止 challenger 实验，人工排查。

---

## 9. MVP 版本

最小可行版本不需要一开始就做复杂 AI。可以先做一个盘后自动复盘和调参系统。

每天收盘后自动完成：

```text
1. 读取当天所有交易日志 (live_*.jsonl)
2. 统计每个信号组合的胜率、PnL、markout
3. 找出亏钱最多的时间段、标的、信号组合
4. 调用 tune_micro_params() 生成一组新参数
5. 写出 challenger.json，但不覆盖 champion
6. 生成 daily_report.html 供人工检查
```

> **当前已具备 JSON 复盘、markout 分析、grid search 调参、challenger 片段写出、多日志 walk-forward、promotion metrics、基础 promotion / regime 模块。** MVP 下一步主要是补 `daily_report.html`、更多完整交易日样本验证，以及更严格的 promotion 流程。

第二阶段：

```text
如果 challenger 连续 5 天 walk-forward 优于 champion，
则允许进入小资金实盘。
```

第三阶段：

```text
如果小资金实盘连续 10 个交易日稳定（满足第 7 节第 5 步全部硬门槛），
才允许升级为 champion。
```

---

## 10. 推荐项目结构

如果后续要把概念落地，可以逐步形成这样的结构。**不必立刻迁移**，可以保留现有 `src/kabu_futures/` 作为主代码，新增模块按这个分层放置。

```text
kabu_futures/
├── src/kabu_futures/        # 当前主代码（保持不动）
│   ├── live.py
│   ├── strategies.py
│   ├── microstructure.py
│   ├── paper_execution.py
│   ├── tuning.py            # 已存在
│   └── ...
├── scripts/
│   ├── tune_micro_params.py    # 已存在
│   ├── analyze_micro_evolution.py  # 已存在
│   ├── daily_report.py         # 待补
│   ├── walk_forward.py         # 已存在
│   └── promotion_check.py      # 已存在
├── research/                  # 新增（盘后分析，不参与实盘）
│   ├── markout_analyzer.py
│   ├── signal_report.py
│   └── cost_model.py
├── evolution/                 # 新增（候选生成与晋升）
│   ├── walk_forward.py
│   ├── candidate_generator.py
│   ├── strategy_ranker.py
│   └── promotion_gate.py
├── configs/
│   ├── champion.json          # 当前正式实盘
│   ├── challenger.json        # 候选（由 tuning.py 写出）
│   └── risk_limits.json       # 硬性风控
└── reports/
    ├── daily_YYYYMMDD.html
    ├── signal_report.csv
    └── evolution_log.csv
```

迁移原则：

- 实盘代码 (`live/`、`src/kabu_futures/`) 与研究代码 (`research/`、`evolution/`) 物理分离。
- 实盘只读 `configs/champion.json`，永远不读 `challenger.json`。
- challenger 上线必须经人工或 promotion_gate 显式切换。

---

## 11. 不建议一开始使用强化学习

强化学习听起来像“自动进化”，但对当前微观策略来说不是第一优先级。

主要问题：

- 样本要求高（至少需要数千笔有标签的交易记录，当前样本量仍远远不够）。
- 容易过拟合到训练区段的特殊市况。
- 回测环境和真实成交环境差距大，sim-to-real gap 经常让训练好的 policy 实盘失效。
- 滑点、盘口冲击、排队成交很难模拟准确。
- 模型解释性差，风控审查难。
- 风控很难完全交给模型。

更现实的优先级是：

```text
1. 自动复盘
2. 自动 markout 分析
3. 自动调参（Optuna）
4. 自动选择信号组合
5. 自动筛选标的和时间段
6. 自动 Champion/Challenger 灰度上线
7. (3 个月后再考虑) 简单 ML 层 (Logistic Regression / XGBoost) 作为 meta-confidence veto
8. (1 年以上) 才考虑强化学习
```

ML / RL 不是不能用，而是**当前阶段不该用**。优先做能立刻见效的事。

---

## 12. 最终结论

AI 对交易程序最大的帮助，不是让它变成一个不可控的自动交易黑箱，而是把整个策略研发流程自动化、数据化、可复盘化。

对当前 kabu futures / 微观剥头皮策略，最现实的路线是：

```text
0. 扩充有效样本 (观察 invalid_combos + imbalance 阈值降档实验 + 确认 paper 模式)
1. 盘口 / 逐笔数据记录          (已就位)
2. markout 分析                  (JSON 复盘已就位，HTML 报告待补)
3. 参数自动搜索                  (tuning.py 已就位，待升级 Optuna)
4. 信号主次关系自动评估          (待补)
5. walk-forward 验证             (基础模块已就位，待更多数据验证)
6. challenger 小资金测试         (流程待落地)
7. 达标后替换 champion           (流程待落地)
```

这才是真正可靠的自动进化：AI 不是随机改策略，而是在严格边界内帮助系统持续发现问题、生成候选改进、验证改进，并淘汰坏版本。

---

## 附录 A：依赖与环境

### A.1 Python 与核心包

```text
Python >= 3.10            (当前环境 Python 3.13)
websocket-client >= 1.8   (pyproject.toml 当前唯一运行依赖)
```

未来可选：

```text
optuna >= 3.0             (用于 Bayesian / TPE 调参)
pandas >= 2.0             (用于报表和批量复盘数据处理)
numpy >= 1.24             (用于统计计算)
scikit-learn >= 1.3       (用于未来 ML 层)
xgboost >= 2.0            (将来 meta-confidence ML 层)
plotly / matplotlib       (daily_report 可视化)
joblib                    (并行 walk-forward)
```

### A.2 硬件预估

| 任务 | CPU | 内存 | 时间预估 |
|---|---|---|---|
| 单日 replay (1 天 JSONL ~ 50MB) | 1 core | < 1 GB | 1~3 分钟 |
| 默认 grid search (4 维 ~100 组合，1 天数据) | 1 core | < 1 GB | 5~15 分钟 |
| Optuna 200 trials (1 天数据) | 1 core | < 1 GB | 15~30 分钟 |
| Walk-forward 10 窗口 × 200 trials | 4 cores 并行 | 2 GB | 1~2 小时 |
| 每日盘后报告生成 | 1 core | < 1 GB | < 1 分钟 |

个人开发机完全够用，无需服务器。

### A.3 时间预估（落地）

| 阶段 | 工作量 | 自然天 |
|---|---|---|
| 第 0 步：信号断流诊断 + 跑一次 tuner | 已有基础 | 1 天 |
| 第 1 步：HTML 自动复盘报告 | 200~400 行 | 3~5 天 |
| 第 2 步：信号评分实验 | 100~200 行 + 多次实验 | 5~10 天 |
| 第 3 步：升级 Optuna | 100 行（基于现有 tuning.py） | 1~2 天 |
| 第 4 步：walk-forward 包装 | 150 行 | 2~3 天 |
| 第 5 步：Champion/Challenger 部署 | 200 行 + 流程文档 | 3~5 天 |
| 第 6 步：小资金验证 | 不需新代码，需要 ~30 个交易日观察 | 6 周 |
| **合计代码** | **~1000 行** | **~3 周纯开发** |
| **合计含观察期** | | **~3 个月** |

---

## 附录 B：术语表

| 术语 | 含义 |
|---|---|
| **markout** | 入场后固定时间窗口的中价变动，做多即 mid(t+Δ) - entry，做空反向。判断信号真实预测力的核心指标 |
| **Champion** | 当前实盘运行的正式策略配置 |
| **Challenger** | 候选进化版本，需要通过门槛才能晋升为 Champion |
| **Walk-forward** | 滚动窗口验证，每个窗口先训练再测试，所有窗口都过关才算稳定 |
| **Regime** | 市场状态，例如低波动趋势、高波动震荡等 |
| **OFI** | Order Flow Imbalance，订单流不平衡 |
| **Imbalance** | 盘口买卖单量不平衡（带深度加权）|
| **Microprice** | 用买卖盘口数量加权的中间价，比 mid_price 对短期方向更敏感 |
| **Profit Factor** | 总盈利 / 总亏损绝对值，>1 表示盈利 |
| **Promotion Gate** | 决定 Challenger 能否晋升为 Champion 的自动化门槛 |
| **Sim-to-real gap** | 模拟环境与真实环境的差距，强化学习的最大风险来源 |
| **IC / RankIC** | 因子值与未来收益的相关性 / 排序相关性 |
| **Sharpe Ratio** | 风险调整后收益，(均值收益 - 无风险) / 波动率 |
