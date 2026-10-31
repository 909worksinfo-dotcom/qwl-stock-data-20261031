# 固定60只与存储结构

## 股票池

canonical QWL运行时从以下配置合并股票池：

```text
config/universe.yaml
config/comparables.yaml
```

必须满足：

```text
总数：60
A股：45
港股：15
核心输出股：12
同行校准股：48
```

不得在技能内维护第二份独立股票清单。始终通过 `qwl_list_fixed_60` 或 canonical QWL配置读取。

## 数据存储

```text
state/history/
```

60只日线OHLCV；`*_raw.parquet`为未复权历史，另有标准化行情文件。

```text
state/snapshots/<as_of>/<run_id>/
```

不可变运行快照、OHLCV副本、来源、证据、门禁和回放验证。

```text
state/forecast_forward/
```

前向分钟行情、日内特征和订单流捕获。

```text
.cache/<dataset>/<ak_symbol>.json
```

财务、公告、资金流、两融、互联互通、股东、分红、公司行动、卖空、研报、新闻等逐域缓存。

```text
.cache/coverage/data_coverage.json
```

60只边界、文件覆盖、记录语义和已知缺口清单。

```text
state/audit.duckdb
```

运行、快照验证、预测和结算的追加式审计账本。

```text
state/weekly_runs/<run_id>/provider_full_responses/
```

本周采集后供应商全量结果的不可变压缩快照

同一运行目录内的 `MANIFEST_SHA256_SIZE.txt` 和 `incremental_catalog.json` 记录该周所有新增文件的哈希、大小、相对路径及基础清单哈希

根目录的原有 MANIFEST 继续作为冻结基础层清单，周任务不得覆盖；完整校验由基础清单加历次增量清单共同构成

```text
state/published_versions/<原相对路径>/<run_id>.json|parquet
```

正式数据库的追加版本层，只包含当前周首次出现的记录或当前周文档版本，基础文件保持不变

```text
state/quarantine/<run_id>/<原相对路径>/
```

旧事件首次补录、既有业务键修订、未来记录和无可靠日期记录的隔离区，内容未获准进入正式历史

## 数据域与预期文件数量

| 数据域 | 预期 |
|---|---:|
| fundamentals / financial_versions / announcements / listing / suspensions / microstructure / consensus / news / earnings_calendar | 60 |
| statements / dividends / fund_flow_ths / northbound / margin / ownership_a / share_changes / repurchases_a / block_trades_a | 45 |
| statements_hk / dividends_hk / southbound / ownership_hk / hk_market_disclosures / hk_short_selling / hk_action_evidence / hk_share_snapshot | 15 |
| 龙虎榜、拆股 | 稀疏事件；仅对实际发生或适用股票存在记录 |

文件齐全只表示缓存边界完整。每个文件内部仍需根据 `availability`、`error`、`records` 和来源限制判断是否具备有效历史。
