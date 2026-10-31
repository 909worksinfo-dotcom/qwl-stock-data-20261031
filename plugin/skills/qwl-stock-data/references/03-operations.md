# MCP工具与操作流程

## 只读工具

### `qwl_data_health`

检查：

- canonical QWL路径；
- Python运行时；
- 60只股票数量和A/H分布；
- 当前覆盖清单；
- 数据目录和依赖是否存在。

### `qwl_list_fixed_60`

返回固定60只股票的名称、标准代码、市场、板块、币种、AKShare代码和腾讯代码。

### `qwl_data_coverage_status`

- `refresh=false`：读取最近一次覆盖清单；
- `refresh=true`：重新扫描全部数据域、重复记录、PIT和边界后写入清单。
- `as_of_date`：可选的目标日期，用于交易日新鲜度及历史/快照一致性门禁

### `qwl_symbol_data_status`

输入标准代码、裸代码或腾讯代码，返回该股票已有的数据文件、大小、主要键、日期范围和缺失数据域。

### `qwl_data_inventory`

返回历史行情、快照、前向捕获和各缓存目录的文件数及字节数。

### `qwl_validate_snapshot`

检查指定快照是否包含 `as_of` 之后的数据，污染快照不可回放。

## 刷新工具

### `qwl_refresh_market_data`

参数：

```text
as_of_date: YYYY-MM-DD
persist_snapshot: true
```

作用：刷新60只行情和批量快照，执行硬质量门禁，生成不可变快照。

### `qwl_refresh_fundamentals`

参数：

```text
force: false
budget_seconds: 120
```

作用：刷新60只财务指标，并归档内容变化版本。

### `qwl_refresh_financial_events`

刷新：

- A/H三张表；
- A/H分红；
- 送转和拆并股；
- 近期A/H公告；
- 行业映射；
- HKD/CNY；
- 龙虎榜、北向、两融和微观结构；
- 财务版本与PIT归一化。

### `qwl_refresh_gap_data`

刷新：

- A/H微观结构；
- A股资金流及可达历史；
- 沪深逐股和市场两融；
- 两融资格；
- 北向、南向及市场互联互通；
- 港股分红和股本快照；
- A股股本变更；
- 流量异常隔离；
- 最终覆盖审计。

已有日频缓存不再因“文件存在”直接跳过，而是先检查最新日期；需要更新时只追加新日期并隔离历史冲突

### `qwl_refresh_historical_evidence`

从指定日期回填：

- 巨潮/HKEX公告；
- 上市区间；
- 停复牌；
- 港股公司行动及PDF证据。

### `qwl_refresh_p2_data`

刷新：

- A/H股东；
- HKEX权益披露；
- 回购与大宗；
- 一致预期；
- 新闻；
- 业绩日历；
- 市场与行业基准；
- 宏观；
- 港股卖空。

### `qwl_refresh_specialized_data`

刷新：

- BYD和安克60分钟历史；
- BYD月度经营公告；
- 固定池内新能源车经营数据；
- BYD研报文本。

### `qwl_refresh_all_data`

顺序执行全部阶段。该工具可能耗时较长；每阶段独立捕获状态，单阶段失败不得伪装成整体成功。

市场阶段先安全追加 canonical history，再执行行业基准等依赖历史的派生数据；市场或最终覆盖门禁失败时顶层返回 `FAILED`

### `qwl_publish_weekly_data`

对已经完成的采集工作区执行只读发布预览

`publish=false` 只返回计划，`publish=true` 被阻断；正式写入只能经 `qwl_weekly_update` 完整验收流程

周更授权只允许增加本周版本和隔离证据，不允许发布历史修订或历史补录

### `qwl_weekly_update`

个人正式版定时任务唯一写入口

执行顺序为冻结与实际周窗口校验、读取持久化检查点、原生采集步骤增量续跑、缺口清单、候选正式视图验收、当前周版本追加、正式视图再次验收

插件或Skill冻结漂移、当前周授权缺失、安全门禁失败时不得发布；本周可恢复缺口返回 WAITING_SOURCE 或 WAITING_RETRY，按 next_retry_at 由自动化续跑

只读 `qwl_weekly_status(as_of_date, database_root)` 返回同一任务当前状态；完成只认 COMPLETE 与 formal_acceptance.passed，不能只认 PUBLISHED

## 推荐流程

### 修复后的人工只读复验

`qwl_weekly_revalidate(as_of_date, resume=false)` 对BLOCKED_PUBLICATION任务执行私有副本验收，不联网、不采集、不写正式数据库或检查点

真实安全问题仍返回BLOCKED_REVALIDATION；仅在用户另行主动授权且复验无阻断时，提供expected_checkpoint_sha和REVALIDATE_AFTER_REPAIR才能恢复同一检查点，恢复动作本身不采集、不发布

本工具不提供两融豁免、历史修订发布、模型重冻结或跨周补数权限，自动任务不得自行使用resume=true

### qfq合同修复后的市场任务恢复

`qwl_weekly_resume_market_safety` 只用于已经用户明确授权的qfq合同修复，必须同时提供原检查点SHA、当次不可变供应商快照SHA、以及固定60只离线重放的验证SHA

恢复调用本身不联网、不采集、不发布、不写正式数据库，也不把market标成成功；它只归档旧检查点和验证回执，然后让同周任务重新执行market。任何SHA、固定股票池、冻结清单或正式基线变化都必须阻断

### 日常检查

```text
qwl_data_health
qwl_refresh_market_data
qwl_data_coverage_status(refresh=false)
```

### 每周正式更新

```text
qwl_weekly_update(
  as_of_date=<最近完成交易周目标日期>,
  database_root=/Users/bytedance/Desktop/60只股票全量历史数据_20260816,
  publish=true,
  authorization=PUBLISH_CURRENT_WEEK
)
```

自动任务不得调用历史授权流程，也不得重建冻结清单

### 财报季完整刷新

```text
qwl_data_health
qwl_refresh_fundamentals
qwl_refresh_financial_events
qwl_refresh_gap_data
qwl_refresh_historical_evidence
qwl_refresh_p2_data
qwl_data_coverage_status(refresh=true)
```

### 失败处理

1. 查看工具返回的逐阶段、逐股票状态；
2. 区分 `failed`、`timeout`、`no_records`、`ineligible` 和 `discontinued`；
3. 对网络失败可在保留白名单和相同语义下重试；
4. 对监管停发、条款禁止或商业源缺失不得重试造数；
5. 最终结果必须附带 `known_irreducible_gaps`。
