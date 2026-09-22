# Responses 语义降级探针与安全重放（Semantic Probe）

## 1. 背景：为什么现有容灾抓不到这类故障

某些聚合网关会把 Codex CLI 的 `/v1/responses` 请求路由到一个**不支持 Responses
工具协议**的降级渠道。表现为：

- HTTP 状态是 `200`，SSE 外壳完全合法；
- 模型看不到 `tools`，把工具调用当纯文本打印（例如 `to=functions.exec code=…`）；
- 客户端什么都没执行，于是反复重发同一轮，形成死循环。

因为状态码是 200，传输层熔断器、`auto_failover_enabled`、HTTP 失败重试全部失效。
唯一可靠的早期信号是**标识符指纹**，它在首字节之前就出现，因此可以安全重放。

> 结论：这类故障**必须依赖本探针**，不要指望打开 `auto_failover_enabled` 能解决。

## 2. 判定规则

### Tier A（可用作重放依据；必须出现在首字节之前）

| 位置 | 健康 | 降级 |
|---|---|---|
| `response.created` → `response.id` | `resp_` + 50 位小写 hex | `resp_` + 32 位小写 hex |
| `response.output_item.added` → `item.id` | `rs_`/`msg_`/`fc_`/`ctc_` + 长会话 id | `…_` + 32 位小写 hex |

- 全部**白名单匹配**，绝不写成「≠50 位就异常」。任何不匹配的形状一律 `Unknown`。
- `Unknown` ⇒ 立即停止缓冲并原样透传（宁可漏报，不可误杀）。
- 触发重放的条件：**首事件**是 SUSPECT 的 `response.created`，且在
  「首个 `response.output_item.added`／5 个事件／200ms」三者先到者之前，再收到
  ≥1 个**独立** SUSPECT 样本（如 `response_id_32hex` + `item_id_32hex`）。
- 只用事件到达或 ≤200ms 的短超时做窗口，没有固定 `sleep`。

### Tier B（仅审计/熔断加权，**永不触发重放**）

只有流结束后才能得到，因此**禁止**用于重放决策：

- `usage.input_tokens` 相对同会话上一请求骤降（`previous ≥ 10k` 且 `current < previous / 2`）；
- 请求带了 `tools` 但流里 0 个 `function_call` / `custom_tool_call` 项；
- 模型文本自述没有工具（`no tool` / `not seeing any definitions` / `没有工具` …）。

## 3. 重放规则

1. **仅当尚未向客户端写出任何字节**才允许重放。只要开始写客户端就绝不重放。
2. 缓冲窗口超时或判为 `Unknown` ⇒ 立即停止缓冲、正常透传，不给正常请求增加长尾。
   （窗口只作用于首个事件，健康流在第一个事件后即关闭探针。）
3. 第 1 次重放**原样发送**，保留 `prompt_cache_key` 以复用上游 prompt 缓存。
   若配置了第 3 次发送（`semantic_replay_max_attempts = 3`），才在第 2 次重放时
   给 `prompt_cache_key` 追加随机后缀以打破渠道亲和。
   *默认只有 2 次发送，因此默认不会扰动缓存键。*
4. 语义重放次数独立于 `max_retries`（后者是传输层/网络错误重试）。
5. 每次尝试都记录 `request_id`、判据、已发送字节（0）、尝试序号与最终结果。

## 4. 独立熔断

- 语义失败**独立计数**，与 HTTP 失败计数、Provider 健康度完全分离
  （HTTP 是 200，绝不能污染传输层熔断器）。
- 默认：5 分钟滑动窗口内 3 次语义失败 → 该目标冷却 60s。
- **仅在「重放预算用尽后仍降级」时计数**，一次误判不会熔断正常目标。
- key 形如 `app_type:provider_id:/v1/responses`（精确到 endpoint，去掉 query）。
- 多 Provider 时，被语义熔断的目标会被跳过，转入下一家；单 Provider 时不跳过，
  避免把「可用但降级」变成「完全不可用」。

## 5. 开关与默认值

配置列在 `proxy_config` 表（每个 app 一行），UI 也可读写。**默认先 dry-run**：

| 字段 | 默认 | 说明 |
|---|---|---|
| `semantic_probe_enabled` | `1` | 探针总开关。只检测 + 记录，不改流。 |
| `semantic_replay_enabled` | `0` | `0` = dry-run（只检测）；`1` = 允许中止 + 重放。 |
| `semantic_probe_window_ms` | `200` | 缓冲窗口，硬上限 200。 |
| `semantic_replay_max_attempts` | `2` | 单客户端请求的总发送次数（含原始），范围 1–3。 |
| `semantic_circuit_failure_threshold` | `3` | 5 分钟窗口内语义失败阈值。 |
| `semantic_circuit_timeout_seconds` | `60` | 语义熔断冷却秒数。 |

### 打开重放

```sql
-- 仅对 codex 打开重放；其它 app 保持 dry-run
UPDATE proxy_config SET semantic_replay_enabled = 1 WHERE app_type = 'codex';
```

或在前端代理设置里打开对应开关。修改后无需重启即可在下一次请求生效（配置每次请求读取）。

### 回滚

```sql
UPDATE proxy_config SET semantic_replay_enabled = 0;                 -- 退回 dry-run
-- 彻底关闭探针（连检测也不做）：
UPDATE proxy_config SET semantic_probe_enabled = 0;
```

代码回滚：本改动是「新增文件 + 少量 hook」，`git revert` 本次提交即可；数据库新增的
列/表对旧版本无副作用（旧版本忽略即可）。

## 6. 可观测性

### 日志（WARN）

```
[SEMA-001] Responses 语义降级(dry-run): provider=hejuapi-codex, endpoint=/responses, request_id=Some("resp_d96da…46c3"), evidence=["response_id_32hex", "item_id_32hex"], action=observe-only
[SEMA-001] Responses 语义降级: provider=hejuapi-codex, endpoint=/responses, request_id=Some("resp_…"), evidence=["response_id_32hex", "item_id_32hex"], action=replay
[SEMA-001] Responses 语义降级，安全重放 2/2: provider=hejuapi-codex, endpoint=/responses, perturbed_cache_key=false
[SEMA-004] 语义重放成功恢复: provider=hejuapi-codex, endpoint=/responses, sends=2
[SEMA-002] Responses 语义降级未被重放修复: provider=…, endpoint=…, request_id=…, evidence=[…], sends=2, circuit_opened=false
[SEMA-003] 语义熔断已打开，跳过 provider=… (codex:…:/responses)
```

### 落库

新表 `semantic_degradation_events`（每个降级事件一行，Tier A 的 dry-run / 重放 /
用尽 / 熔断跳过都会写；落库失败只记 warning，绝不影响代理请求）：

```sql
-- 按天复盘 burst
SELECT date(created_at, 'unixepoch') AS day, count(*)
FROM semantic_degradation_events
GROUP BY day ORDER BY day;

-- 看 09-18 / 09-21 两波
SELECT created_at, request_id, provider_id, evidence, attempts, outcome
FROM semantic_degradation_events
ORDER BY created_at DESC LIMIT 50;
```

### dry-run 样例（一条正常、一条检出）

- **正常流**：探针在首个事件判为 `KnownGood` 后立即关闭并原样透传，**不产生任何
  日志、不写审计行**。
- **检出流**：写一条 `outcome='dry_run'` 审计行 + 上面第一条 `[SEMA-001] … dry-run`
  WARN 日志，随后**照常把降级响应透传给客户端**（dry-run 不 abort、不重放）。

## 7. 测试

```bash
cd src-tauri
export PATH="$HOME/.cargo/bin:$PATH"
cargo test --lib semantic
```

覆盖：50/32/未知/空 id 四种形状、Two-sample 判定、事件上限与超时、Tier B 不触发
重放、健康流字节级透传、dry-run 落库、假上游 SSE 服务器上的完整重放循环
（恢复 / 用尽标记 / 第三次扰动缓存键）。

## 8. 已知限制

1. **Tier B 未接入热路径**：`analyze_tier_b` 已实现并有单测，但当前 SSE usage
   collector 只保留 `response.completed`/usage 事件，且 `RequestContext` 不持有原始
   请求体，无法在流结束时可靠拿到「请求带 tools」与上一轮 `input_tokens`。因此
   Tier B 目前只作为纯函数存在，未写审计行。Tier A 的检出/重放/熔断已完整。
2. **误判边界**：只有「`resp_`+32hex」且随后「item id +32hex」才触发。上游若换成
   新的降级 id 形状，会判为 `Unknown` 并漏报（这是有意的安全取舍）。
3. **延迟**：健康流不增加缓冲——首个事件即关闭探针；现有 `validate_responses_stream_start`
   本来就要等到首个产出事件。语义窗口仅在首事件为 suspect 时最多再等 200ms。
4. **缓存命中损失**：默认第一次重放保留 `prompt_cache_key`，不额外损失缓存；只有
   配置第 3 次发送时才扰动缓存键（会失效该次上游前缀缓存）。
5. **多会话并发**：探针是无状态纯函数，每个请求独立；熔断器按
   `app_type:provider_id:endpoint` 共享，一个坏会话连续触发才可能熔断整个目标。
6. **重放与故障转移的关系**：语义重放预算（2 次发送）独立于 provider 故障转移。
   若配置了多个 Provider 且首个用尽预算，仍会按 `max_retries` 故障转移到下一家
   （可在日志中按 provider 分别复盘）。
