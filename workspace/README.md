# HPC 作业抢占与检查点恢复服务

实现调度器回收节点场景下的**抢占纪元签发、内容寻址检查点、全量校验释放、
同代际恢复与输出租约栅栏**，以及内容寻址存储的**可审计空间回收**（两阶段
计划 + 引用水位 + 代际锁定 + 崩溃续跑）。纯 Python 标准库，无外部依赖。

## 核心不变量

| # | 不变量 | 实现位置 |
|---|--------|----------|
| 1 | 回收前签发**抢占纪元**（单调 epoch/generation + HMAC 签名令牌）；重复抢占通知幂等，不轮换令牌 | `epochs.py`、`jobs.py::preempt` |
| 2 | 检查点分片为**内容寻址**（`sha256(payload)`），相同分片跨代际天然去重 | `storage.py::ContentStore` |
| 3 | 期望分片集合在开检查点时由「进程集合 × 每进程分片数」固定；提交时**逐个从 CAS 重读重算哈希**，缺片/损坏一律拒绝，检查点停留在 `PENDING`，绝不提前可用 | `checkpoints.py::commit` |
| 4 | **只有全部校验完成**（`COMPLETE`）才允许确认释放；不完整只能 `abandon`（`INCOMPLETE`/`ABORTED`） | `jobs.py::confirm_release/abort_epoch` |
| 5 | **恢复必须使用同一检查点代际**：恢复时再次全量重校验、核对进程集合、回传每个分片输入位点，校验通过才签发**新的输出租约** | `jobs.py::restore`、`leases.py` |
| 6 | 抢占瞬间吊销旧租约；旧进程迟到写入（旧 generation/已 REVOKED 令牌）返回 `412 FenceViolation`；输出按 `(generation, output_id)` 幂等去重 —— 无双重输出 | `leases.py::authorize`、`storage.py::OutputLog` |
| 7 | 节点失联：节点与 rank 标记 `LOST`，其分片不被接受、提交被阻塞；心跳恢复后 incarnation+1，补齐方可整体提交 | `jobs.py::mark_node_lost/heartbeat` |
| 8 | 新一轮抢占使旧的未完成检查点 `SUPERSEDED`；旧纪元令牌不能写新代际 | `checkpoints.py`、`jobs.py` |
| 9 | **结果提交授权失败统一处理**：缺失/伪造/超期/撤销/跨作业/旧代际凭证一律返回稳定错误（固定状态码+`reason`+固定消息，不回显凭证），并写入一条**顺序化安全事件**（全局单调 seq，不含凭证原文）；授权在写入前终止，重复并发请求不增加结果条目 | `leases.py::authorize`、`jobs.py::_deny` |
| 10 | **事件溯源持久化**：所有状态变更（含安全事件、输出条目、恢复报告、空间回收）顺序追加到 `data_dir/journal.jsonl`，blob 原子落盘；服务重启按 seq 重放重建，页面与验证报告仍可查询，旧令牌仍被栅栏，同密钥重新派生 | `persistence.py`、`jobs.py::_replay` |
| 11 | **可审计空间回收**：先生成带**引用水位**（refs seq）的回收计划，冻结全部候选（blob/大小/来源/原因/预计释放量/存储水位），执行前若引用关系发生过任何变化一律 `409 references_changed` 拒绝；支持**锁定保留代际**（pin）；执行持独占写锁、逐项重算可达集（`still_referenced`/`blob_missing` 跳过），上传/恢复/回收并发绝不删除仍可访问的数据；删除逐项落事件，崩溃重启后续跑/重试同一计划安全幂等 | `reclamation.py`、`jobs.py` |


## 状态机

```
作业:   RUNNING ──preempt──▶ PREEMPTING ──release(仅COMPLETE)──▶ RELEASED
                                  │                                   │
                                  └──abandon──▶ ABORTED              │
                                      ▲                              │
        RELEASED ──restore(同代际+重校验+新租约)──▶ RUNNING ◀─────────┘

检查点: PENDING ──全部分片重校验通过──▶ COMPLETE      (可重复 commit: 幂等)
            ├── 缺片/损坏: 提交拒绝, 保持 PENDING
            ├── 新纪元接管: SUPERSEDED
            └── 宽限期耗尽放弃: INCOMPLETE
```

## 运行

```bash
# 测试
python3 -m unittest discover -s tests -v        # 67 个测试, 含并发竞争、回收与重启持久化

# 端到端演示 (打印恢复验证报告 Markdown)
python3 demo.py
# 持久化演示 (状态/安全事件落盘, 重跑自动重放)
python3 demo.py --data-dir ./hpc-data
# 演示后继续提供 HTTP 服务, 浏览器打开节点进度页
python3 demo.py --serve --port 8080
#   http://127.0.0.1:8080/ui/jobs/climate-7

# 空服务 (纯内存, 重启不保留)
python3 -m hpc_cr            # 默认 127.0.0.1:8080
# 持久化服务 (事件日志 + blob + 自动生成 0600 签名密钥)
python3 -m hpc_cr --data-dir ./hpc-data
# 也可用环境变量 HPC_CR_SECRET 固定签名密钥 (重启后旧令牌才可继续验证)
```

## 作业 API

所有写操作为 `POST /api/jobs/{id}`，JSON body 用 `action` 区分；
凭证走请求头：`X-Epoch-Token`（抢占纪元令牌）、`X-Lease-Token`（输出租约令牌）。
二进制数据以 `data_b64`（标准 base64）传递。

| 方法/路径 | 说明 |
|---|---|
| `POST /api/jobs` | 创建作业（world_size、shards_per_rank、rank→node 放置） |
| `GET  /api/jobs` / `GET /api/jobs/{id}` | 作业列表 / 快照（纪元、宽限剩余、当前检查点、租约） |
| `action=preempt` | 签发抢占纪元；重复通知幂等，`force_new_epoch:true` 表示新一轮抢占 |
| `action=heartbeat` | 节点心跳（LOST→UP，进程 incarnation+1） |
| `action=mark_node_lost` | 标记节点失联，阻塞其分片 |
| `action=progress` | 上报 rank 输入位点 |
| `action=upload_shard` + `X-Epoch-Token` | 上传分片（rank/index/node/input_pos/data_b64），重复同内容幂等 |
| `action=commit_checkpoint` + `X-Epoch-Token` | 全量重校验提交；缺片/损坏返回 409 |
| `action=confirm_release` + `X-Epoch-Token` | 确认释放；非 COMPLETE 返回 409 |
| `action=abandon` | 宽限耗尽放弃，检查点 INCOMPLETE |
| `action=append_output` + `X-Lease-Token` | 提交输出；旧代际/吊销租约 412；同 output_id 幂等 |
| `action=restore` `{generation}` | 同代际恢复验证，返回恢复验证报告 + 新租约 |
| `GET  /api/jobs/{id}/progress.json` | 进度页聚合数据（节点/进程/分片矩阵/输出/审计） |
| `GET  /api/jobs/{id}/checkpoints` | 全部代际检查点 |
| `GET  /api/jobs/{id}/outputs` | 输出条目（按代际隔离） |
| `GET  /api/jobs/{id}/security-events` | **授权失败安全事件**（顺序化 seq/原因/绑定字段，不含凭证原文） |
| `GET  /api/jobs/{id}/restore/report` | 最近一次恢复验证报告（JSON，含截至当前的安全事件列表） |
| `action=pin_generation` `{generation,note?}` | **锁定保留代际**：解锁前该代际分片/输出一律可达，不被回收 |
| `action=unpin_generation` `{generation}` | 解除代际锁定 |
| `GET  /ui/jobs/{id}` | **节点进度页面**（2s 自动刷新，含安全事件表与空间回收区块） |

### 空间回收 API (`/api/reclamation`)

两阶段回收：**先出计划（只读，不删任何数据）→ 确认/执行**。

| 方法/路径 | 说明 |
|---|---|
| `GET  /api/reclamation` | 回收概览：引用水位、存储水位（总/可达/垃圾 blob 数与字节）、锁定列表、计划摘要列表 |
| `POST /api/reclamation/plans` `{min_bytes?,note?}` | 生成回收计划，返回全部候选（blob/size/kind/reason/来源作业代际/引用明细）、排除项、预计释放量、水位时存储统计、生效锁定 |
| `GET  /api/reclamation/plans` | 全部计划 |
| `GET  /api/reclamation/plans/{id}` | 计划详情：候选、**实际删除项**、**跳过项及原因**、最终结果、拒绝时的变更事件 |
| `POST /api/reclamation/plans/{id}/execute` | 按计划执行；水位不一致 → `409 reason=references_changed`，计划落 `REJECTED`；重复执行 `COMPLETED` 幂等 |
| `POST /api/reclamation/plans/{id}/cancel` | 取消未开始的计划（`PLANNED → CANCELLED`） |
| `GET  /ui/reclamation` | **全局回收审计页面**（5s 自动刷新：水位/锁定/计划/预计释放/实际删除/跳过/结果） |

**可达（保留）判定** —— 每个作业当前仍可访问的代际：

- `RUNNING`：当前输出生效代际（恢复代际；首次抢占前为 gen0）；
- `PREEMPTING`：正在构建的检查点代际 + 尚未释放的旧输出代际；
- `RELEASED`：唯一可恢复的 released 代际；
- `ABORTED`：无；
- 并上全部被 **pin 锁定**的代际（跨作业）。

不在任何可达代际引用集合内的 blob 按来源标为 `unreferenced_shard` /
`unreferenced_output` / `orphan_blob`（无任何元数据引用，如上传中途崩溃的
残片）。跨代际内容相同的分片共享同一 CAS blob，只要一处可达即整体保留。

**水位与并发**：计划冻结 `refs_seq`（事件日志中最近一次引用变更 seq；
抢占/分片上传/输出提交/释放/放弃/恢复/锁定变更均推进水位）。执行时：

1. 获取回收**写锁**（引用变更方持读锁，写者优先），排队等待在途上传/输出/
   恢复排空 —— 执行期引用关系物理冻结；
2. 再次校验水位，不一致直接拒绝并记录 `changed_events` 审计；
3. 对每个候选**重新**枚举可达集：仍被引用 → 跳过 `still_referenced`
   （纵深防御，附引用者证据），文件已不存在 → 跳过 `blob_missing`，
   其余才 `unlink`，逐项追加 `gc_blob_deleted`/`gc_blob_skipped` 事件，
   最后落 `gc_plan_completed`（结果 `completed` / `completed_with_skips`）。

**崩溃语义**：事件与物理删除的顺序保证最坏只丢"未删的文件"，不丢引用；
重启重放时对每个已确认删除事件执行幂等 `unlink`，`RUNNING` 计划再次
execute 即续跑（已删项不重复处理），`COMPLETED` 计划重放后重复执行幂等。

### 授权失败稳定错误 (`append_output` 等凭证接口)

错误响应固定为 `{error, reason, message}`，同一类拒绝永远得到同一三元组，
消息不随请求/令牌变化，不回显任何凭证原文：

| 场景 | HTTP | error | reason |
|---|---|---|---|
| 缺失凭证头 | 401 | invalid_token | `missing_token` |
| 伪造/篡改/格式损坏令牌 | 401 | invalid_token | `bad_signature` / `malformed_token` / `bad_token_version` / `undecodable_payload` |
| 令牌类型错用（epoch 充当 lease） | 401 | invalid_token | `wrong_token_type` |
| 已签发但租约 ID 不可识别 | 401 | invalid_token | `unknown_lease` |
| 旧代际凭证迟到写入 | 412 | fence_violation | `stale_generation` |
| 跨作业凭证 | 412 | fence_violation | `cross_job_token` |
| 抢占时已撤销租约 | 412 | fence_violation | `lease_revoked` |
| 超过租约/令牌有效期 | 412 | fence_violation | `lease_expired` |
| 纪元令牌跨作业/旧纪元 | 412 | fence_violation | `epoch_cross_job_or_epoch` |

每次拒绝同步写入一条 `security_authz_denied` 事件：包含全局单调 `seq`、
`credential`（`output_lease`/`epoch_token`）、`operation`、`reason`、
`http_status`、`expected_generation` 与仅可公开的绑定字段 `claimed`
（job/generation/lease id），**绝不包含令牌串或签名**。授权在校验通过前
终止，blob 与 OutputLog 不发生任何写入，故重复并发的无效/重复请求不会
增加结果条目（合法重复提交仍按 `(generation, output_id)` 幂等）。

其余错误映射：`400` 请求格式、`404` 对象不存在、
`409` 状态冲突（缺片提交/重复分片/状态机）。

## 恢复验证报告

`restore()` 与 `/restore/report` 返回结构化报告（`RestoreReport`），并可用
`report.to_markdown()` 渲染，包含：清单摘要、分片重算哈希结果（逐个列出失败
分片）、每个分片的**输入位点 seek 表**、旧/新进程放置与迁移 rank、新输出租约、
**安全事件表（授权拒绝证据，读取时补齐到最新）**、警告；任一检查项失败时
`success=false` 且作业保持 `RELEASED`，不会带着坏检查点运行。报告随事件日志
持久化，服务重启后仍可查询。

## 并发正确性

- 每作业一把 `RLock`，加锁顺序固定 Job → 子管理器；CAS 存储独立细锁。
- 空间回收独立维护一把**写者优先读写锁**：引用变更（抢占/分片上传/输出
  提交/释放/放弃/恢复/锁定）持读锁，回收执行持写锁；快照在 `rw.read()`
  下做前后水位比对，遇并发引用变更自动重试。作业锁仅在注册锁快照之后按
  job_id 顺序获取，不存在锁顺序倒置。
- 测试 `tests/test_concurrency_api.py` 覆盖：
  - 旧进程迟到写入与恢复后新进程并发提交相同 `output_id` → 旧写入 412、恰好一次落盘；
  - 8 线程并发重复 output / shard → 只有一次 delivered/stored；
  - 未补齐期间并发提交 6 次全部失败，补齐后才 COMPLETE；
  - 失联恢复与提交循环并发 → COMPLETE 最多出现一次且在补齐之后。
- 测试 `tests/test_authz_security.py` 覆盖：
  - 缺失/伪造/错类型/撤销/超期/跨作业/旧代际凭证的稳定错误分类与无回显；
  - 每次拒绝一条顺序化安全事件（seq 唯一递增、无凭证原文）；
  - 8 线程并发无效/混合请求不增加结果条目，合法重复恰好一次；
  - 落盘后新建管理器（模拟重启）：页面、`/security-events`、恢复验证报告
    （JSON 与 Markdown）仍可查询事件，输出/检查点/blob 完整且旧凭证仍被拒。
- 测试 `tests/test_reclamation.py` 覆盖（23 个）：
  - 计划只枚举不可达 blob（分片/输出/孤儿），含预计释放量、水位、存储统计；
  - 执行删除候选且可达代际完好、可再次恢复；重复执行幂等；
  - 计划后上传分片/提交输出/锁定变更 → `409 references_changed` 拒绝，
    `changed_events` 留档，新计划中后到的数据完好；
  - pin 锁定代际的分片与输出全部保留，pin 幂等、解锁后重新可回收；
  - 执行期 `blob_missing`/`still_referenced`（纵深防御，附引用者）逐项跳过，
    结果 `completed_with_skips`；`min_bytes` 小 blob 排除留档；
  - ABORTED 作业分片、孤儿 blob、跨代际 CAS 去重共享 blob 的保留；
  - 持久化目录下模拟执行中途崩溃 → 重启续跑同一计划，重放补删物理文件，
    锁定/计划/删除记录跨重启一致，COMPLETED 重试幂等；
  - 200 次并发上传 vs 20 轮回收、50 路恢复/输出并发 vs 回收：无死锁、
    可达 blob 从未被删；HTTP API 与全局/作业回收页面渲染。

## 代码结构

```
hpc_cr/
  security.py     # HMAC 签名令牌、规范化 JSON、sha256 CAS 原语
  persistence.py  # 顺序化事件日志 Journal、持久化 blob 后端、签名密钥加载
  epochs.py       # 调度器、抢占纪元 (epoch/generation/deadline)
  storage.py      # 内容寻址 blob 存储 (内存/磁盘) + 输出去重日志
  checkpoints.py  # 分片、检查点状态机、全量重校验提交、重放回填
  leases.py       # 输出租约、代际栅栏、授权失败稳定归类
  reclamation.py  # 空间回收: 读写锁栅栏、引用水位、代际锁定、两阶段计划/执行/续跑
  jobs.py         # Job/JobManager 聚合根、恢复验证报告、事件溯源/重放、回收编排
  api.py          # stdlib ThreadingHTTPServer JSON API
  ui.py           # 节点进度页面 + 全局回收审计页面 HTML 渲染
tests/            # 67 个单元/并发/HTTP/重启持久化测试 (含 23 个回收测试)
demo.py           # 端到端场景脚本 (可选 --data-dir 持久化)
```

## 持久化与重启语义

`data_dir` 布局：

- `journal.jsonl`：只追加事件日志，每行一个带全局单调 `seq` 的 JSON；
  写入 `flush + fsync`，重启按 seq 重放重建作业/检查点/租约/输出/审计/
  安全事件/**回收计划与代际锁定**（`gc_*` 事件；已确认删除的 blob 在
  重放时幂等补删，回收计划崩溃后续跑）。崩溃最多丢失未落盘的最后一行
  （该行对外也不可见）。
- `blobs/<sha256>`：分片与输出载荷，临时文件 fsync + 原子 rename。
- `signer.key`：首次启动自动生成的 HMAC 密钥（0600）；也可用
  `HPC_CR_SECRET` 覆盖。令牌自描述不入库，重放时以同密钥重新派生，
  因此旧令牌重启后仍能被验证或被栅栏拒绝。

不给定 `data_dir` 时全部退化为内存实现，接口行为一致。
