---
name: qwl-stock-data
description: Collects and audits a fixed 60-stock A/H dataset with PIT and quality controls. Invoke for stock-data refresh, coverage checks, provenance, or data gaps.
---

# QWL 60只股票全量数据

工程维护版 `weekly-recovery.20261024`，运行契约仍为 `weekly-contract.20260906`；普通数据更新不得修改或重新冻结本插件

## 目标

调用 `qwl-stock-data` MCP 工具，使用原 QWL 数据层采集和审计固定60只股票的信息。该技能是数据能力，不执行交易、不修改策略和模型。

## 强制边界

1. 先调用 `qwl_data_health`，确认 canonical QWL runtime、Python环境和覆盖清单可用。
2. 调用 `qwl_list_fixed_60` 获取唯一合法股票池。不得根据用户输入新增或替换股票。
3. 所有采集必须调用本插件 MCP 工具；不要自行拼装另一套抓取脚本。
4. 每次刷新后调用 `qwl_data_coverage_status(refresh=true)`。
5. 覆盖审计必须同时报告 `freshness_complete` 和 `history_consistency_complete`，不得仅根据文件存在判定完整；旧历史冲突只有经逐日期核验不影响本周时才作为未解决警告保留，不得改称历史完整
6. 输出必须区分：
   - 已成功采集；
   - 文件存在但源数据为空；
   - 接口失败或超时；
   - 股票不适用或不具备资格；
   - 数据源停止披露；
   - 需要授权源或商业源。
7. 不得把缺失、失败、不适用或未达到披露阈值解释为零。
8. 不得声称“市场全部数据完整”。只能表述为“固定60只、已定义数据域、当前可获得来源完成并通过审计”。
9. 自动周更必须只调用 `qwl_weekly_update`，不得把普通全量刷新工具直接指向正式数据库
10. 自动周更前必须验证外部冻结清单，插件或本Skill任一受保护文件漂移时在采集前阻断
11. 正式数据库的既有文件不得被周任务改写，供应商全量结果保存为不可变压缩快照，本周新增写入 `state/published_versions`
12. 旧事件首次补录、既有业务键内容变化、未来日期和无法判定日期的记录只能进入 `state/quarantine`，自动任务不得发布
13. 历史修订、历史补录和模型重冻结只接受用户在单独任务中的明确授权，周任务的当前周发布授权不得复用
14. 周更完成必须为 `status=COMPLETE`、`complete=true`、`publication.formal_acceptance.passed=true` 且独立正式读取验收为 `FORMAL_WEEK_ACCEPTED`，仅 `PUBLISHED` 不代表验收完成
14a. 两融逐股日频与市场汇总的本周披露缺口可列入 `margin_pending` 后发布，不得填0或伪称全部维度完整；已取得两融数据的字段、数值、范围、历史保护和两融资格仍严格验收，其他数据域不豁免
15. 本周可恢复缺口进入持久化等待与续跑，不得把对话结束当成任务完成；已有进程运行时不得重复启动
16. 北京时间周六00:00至周日23:59可处理该周，运行契约不强制10:00；进入下周后，旧周缺口只报告并等待另行授权，不得固定旧日期绕过历史边界
17. 巨潮公告端点的403、非JSON和结构错误必须保留来源证据并阻断确定性盲重试；分页截断可按检查点退避重试，不能当作空窗。既往一次性HTTP授权不能用于以后各周
18. 数据库发布后独立核对固定股票池、应有交易日、成功回执与不可变快照、公告请求窗口/分页和正式版本成员SHA；无可信完整性证据的事件域不得宣称已证明没有漏报
19. 周更及本周PIT两级门禁完成后执行空间审计，仅删除不再被正式来源、检查点或隔离证据引用且有精确安全副本的文件，报告实际释放空间

## 任务路由

### 查询股票池或现有数据

1. `qwl_list_fixed_60`
2. `qwl_symbol_data_status(symbol)` 或 `qwl_data_inventory`
3. 需要完整性结论时再调用 `qwl_data_coverage_status`

### 日常行情刷新

调用 `qwl_refresh_market_data(as_of_date)`。它会：

- 对60只股票批量取得腾讯实时快照；
- 逐股取得未复权日线，失败时按A/H市场走既定兜底；
- 执行OHLCV、交易日、新鲜度、价格和汇率门禁；
- 生成不可变快照并检查是否存在 `as_of` 之后的数据。

### 财务季或公告更新

依次调用：

1. `qwl_refresh_fundamentals`
2. `qwl_refresh_financial_events`
3. `qwl_refresh_historical_evidence`
4. `qwl_data_coverage_status(refresh=true)`

### 资金、股本和微观结构更新

调用 `qwl_refresh_gap_data`，覆盖资金流、两融、互联互通、南向、分红、股本、换手率和流通市值。

### 股东、研报、新闻、宏观及港股卖空

调用 `qwl_refresh_p2_data`。

### 全量刷新

仅在用户明确要求完整刷新并接受较长耗时时调用 `qwl_refresh_all_data`。完成后必须检查每个阶段状态以及最终覆盖审计，不得只返回顶层 `OK`。

行情历史只追加新交易日，已有日期与新来源冲突时保留旧值、隔离冲突并将阶段标记为失败，禁止静默覆盖

### 个人正式版每周更新

每周自动任务调用 `qwl_weekly_update(as_of_date, publish=true, authorization="PUBLISH_CURRENT_WEEK")`

`as_of_date` 必须是最近完成交易周的最后目标日期，周窗口从该日期所在周的星期一开始

先读取 `qwl_data_health`，要求 `runtime_revision=weekly-contract.20260906` 且 `frozen_sources.ok=true`，再读取 `qwl_weekly_status` 判断是否已有运行或等待中的同周任务

日线按口径分离：raw 对应不复权快照，标准化序列只接受独立明确的 qfq 输入，禁止用 raw 填充。qfq 重叠值变化默认阻断；仅当差异全部早于本周、本周 qfq 与独立 raw 逐字段相等、周前正式收盘锚点与新 qfq 相等且本周无公司行动时，才可只追加本周原值并隔离历史差异；若本周有已及时可用的明确除净证据，qfq 差异仅出现在除净日前且复权因子稳定，允许用周前正式收盘作锚点生成 `qfq_chain_v1` 本周版本，成交量不缩放。任一条件不满足仍安全阻断

旧快照没有明确复权参考时，跨口径差异仅记为 basis_unverified，不自动认定供应商修订或历史错误，也不认证历史完整；仅本周逐日等值验证通过的旧日期差异可保留为警告，历史修订和模型重冻结仍需另行授权

财报、A股股息、股本变更、历史公告、解禁公告、日频资金流和两融按股票保存检查点；专项按子任务拆分，研报按文档URL和内容哈希保存成功凭证，市场级接口仍由原采集器批量过滤

同周已校验成功的项目不重采；旧检查点只有回执、不可变快照及当前内容核对通过才迁移，状态字符串不能代替内容验收，财报缺任一报表分项仍属待完成

所有周更采集运行于独立工作区和受限时子进程，进程组退出后才接收结果；普通单股/文档最长150秒，财报420秒、财务指标180秒、上市观测300秒，财务指标和上市观测各自每次运行合计1500秒。失败原始快照保留、失败输出不覆盖已接受工作缓存，不越过周末授权期限

财务指标与上市观测按股票恢复，行情按股票和raw/qfq分别保存成功来源快照，续跑只处理未完成项；分项成功不代表整池行情通过。新typed-v3公告业务键和SFC历史列表契约只用于新版本，不重新解释旧正式记录，可用时间未知继续隔离。已有桥接证明仅在原公式、来源、快照、正式成员SHA和零历史写入均通过时延续，不继承旧周临时授权

`WAITING_SOURCE` / `WAITING_RETRY` 不是完成；定时调度在 `next_retry_at` 之后继续调用同一入口，不允许无间隔循环或绕过安全门禁

完整性先在隔离的“基础文件+既有版本+待发布版本”视图验收，再发布并核验正式视图；读取版本层时拒绝重复业务键

`BLOCKED_*` 阻断写入并报告证据；`AUTHORIZATION_REQUIRED_CROSS_WEEK` 保留缺口等待人工授权；禁止自动改代码、清理历史、重冻结模型或修改冻结清单

若发现历史修订或旧事件首次补录，工具只写隔离证据并报告 `authorization_required=true`，不得改变既有正式文件

沪市两融仅在已知来源、精简五字段与正式共享值逐项完全一致时复用正式旧记录，不能丢弃原有字段；新日期仍须按原有完整字段验收

行业基准在PIT归一化后，从正式基础层、已发布版本和合规本周追加候选构造独立输入视图，禁止用隔离历史修订重算正式历史；以周前正式锚点续算本周，核验数值、成分、公式与输入哈希

财务归档不得去重、重算哈希或补写既有版本的PIT元数据；周任务的港股基线步骤只读审计，不运行历史基线修复

代码修复后的 `qwl_weekly_revalidate(resume=false)` 仅在私有副本验收，不采集、不写正式数据库或检查点；只有用户另行主动授权恢复，且提供原检查点SHA时才允许 `resume=true, authorization="REVALIDATE_AFTER_REPAIR"`，真实阻断仍不能绕过，自动调度不得自行调用恢复模式

市场任务因qfq安全门禁阻断且相关合同已获用户授权修复时，只能通过 `qwl_weekly_resume_market_safety` 绑定原检查点SHA、原供应商快照SHA和固定60只离线重放SHA恢复；恢复仅把market重置为待采集，不将失败项标成成功，不写正式数据库，自动调度不得自行调用

## 输出契约

向用户报告：

```text
截至时间
股票池：60只（45 A + 15 HK）
执行的数据阶段
每阶段 cached / failed / timeout / empty / ineligible 数量
质量门禁 fatal / warn
覆盖审计状态
新增或更新文件
已知不可消除缺口
```

## 参考路由

- 数据源、接口和兜底：`references/01-data-sources.md`
- PIT、异常隔离和完整性校验：`references/02-pit-quality.md`
- MCP工具和标准操作流程：`references/03-operations.md`
- 明确不能补造的数据缺口：`references/04-known-gaps.md`
- 固定60只清单和存储结构：`references/05-universe-storage.md`
- 个人正式版周更、版本层和授权闸门：`references/06-weekly-publication.md`
