# 高性能 Kafka 积压消息清空消费器 (kafka-drainer)

用于读取并丢弃指定 Topic 的消息，或直接推进指定消费组的提交位点。推进消费组位点不会删除 Broker 中的消息。

> **前置条件**：`assign` 和 `fast-forward` 要求整个目标消费组无活跃成员，包括消费其他 Topic 的成员。程序检查组状态，只接受无成员的 `Empty` / `Dead` 状态；检查不构成锁，运行期间须保持业务消费者停止。不能停组时，不能承诺“零 Rebalance 且推进活跃组位点”；`drain -mode group` 会加入消费组，需要评估 Rebalance 和订阅分配影响。客户端支持协议版本协商，但不等于已经验证所有 Broker 版本。

---

## 核心设计与生产风险防范

处理大规模积压前，需要区分本地读取进度和 Kafka 实际保存的消费组提交位点。

### 1. 避免消费组 Rebalance 踩踏死正常业务 Topic（核心隔离设计）
* **生产痛点**：当前消费组内已有其他 Topic 正在正常消费。如果新消费器使用标准 Kafka `Subscribe` 协议加入该消费组，会产生**非齐次订阅（Heterogeneous Subscription）**。新成员加入、网络抖动或拉取超时都会触发全局 Consumer Group Rebalance，导致正在正常消费核心业务的消费者发生 Stop-the-world 停摆，甚至陷入持续数十分钟的 Rebalance 风暴。
* **解决方案**：
  * **默认模式 `--mode=assign`**：独立拉取分区，通过管理客户端向目标 `group.id` 提交位点；不加入消费组不代表有权修改活跃组位点。
  * **提交验证**：逐分区检查提交响应，再从 Broker 回读位点。出现 `UNKNOWN_MEMBER_ID` 等错误时停止执行，不再将请求级成功冒充所有分区提交成功。
  * **备选模式 `--mode=group`**：加入标准消费组，提供 `-balancer=auto` 及显式分配策略选择；必须与现有组配置兼容。

### 2. 避免 Broker 磁盘 I/O 击穿与 PageCache 污染（冷读灾难防护）
* **生产痛点**：80 亿条历史积压消息绝大多数早已不在内存 PageCache 中，而是存放在 Broker 物理磁盘（甚至机械盘/共享网络存储）。暴力并发拉取会导致 Broker 磁盘读 IOPS 与带宽瞬间被打满（100% Util），并将正常生产写入所需的 PageCache 冲刷挤占，引发正在生产的业务产生严重的写入毛刺或超时失败。
* **解决方案**：
  * 内置**双维度平滑令牌桶限流器**：
    * `--max-msg-per-sec`：精准限制每秒拉取消息条数（QPS）。
    * `--max-bytes-per-sec`：精准限制每秒拉取网络流量（如 `50MB`、`100MB`）。
  * 默认单批次拉取参数优化（`1MB ~ 20MB`），避免巨型请求击垮 Broker 内存与连接缓冲区。

### 3. 两种清理动作决策（秒级对齐 vs 真实排空）
* **动作一：`--action=fast-forward`（秒级位点对齐，最推荐方案）**
  * 若业务确认这 80 亿条历史积压数据完全不需要处理，仅需消除 Lag。
  * 查询各分区 LEO 快照，提交位点并回读验证。跨分区提交不是原子事务，失败时可能已有部分分区成功。
  * 不拉取消息正文，但仍有元数据、提交和查询流量。生产者继续写入时，最终 lag 可以大于零；不承诺固定耗时或零副作用。
* **动作二：`--action=drain`（极速拉取并丢弃模式）**
  * 使用 `franz-go` 拉取消息，不执行业务处理，读取后丢弃。
  * 配合平滑限流与多 worker 并发，平稳且快速地推进消费位点。

### 4. Offset Commit 风暴抑制与优雅退出
* **抑制提交风暴**：不逐条提交，采用后台可配置时间间隔批量异步提交（默认 2 秒）。
* **优雅停机（Graceful Shutdown）**：assign 模式先等待所有 worker 停止，再提交最后处理位点并回读验证；提交失败以非零退出码报告，包括 Ctrl+C 后的失败。

### 5. 实时可观测性与生产看板
* **终端实时输出**：处理条数只代表已读取并丢弃的记录，不代表消费组位点提交成功。Group Lag 定期查询 Broker 的 LEO 和已提交位点，属于非原子的查询快照；查询失败、缺少提交位点或快照过期显示 `UNKNOWN`，不会显示清零。offset 差值不等于精确的存留消息条数。
* **Prometheus lag 语义**：`kafka_drain_remaining_lag_total` 为组提交位点 lag 快照，`-1` 表示未知或过期。
* **Prometheus 指标集成**：支持 `--metrics-addr=:9100`，暴露标准 Prometheus `/metrics` 接口供企业级监控采集。

---

## 命令行参数一览

所有参数均可通过命令行全部指定：

| 参数名 | 默认值 | 说明 |
| :--- | :--- | :--- |
| `-brokers` | (必填) | Kafka Broker 列表，逗号分隔，如 `10.0.0.1:9092,10.0.0.2:9092` |
| `-group` | (必填) | 目标消费组 ID，如 `my_consumer_group` |
| `-topics` | (必填) | 目标积压 Topic 列表，逗号分隔，如 `topic_a,topic_b` |
| `-action` | `drain` | 执行动作：`drain`（拉取并丢弃）或 `fast-forward`（秒级跳跃至最新 LEO） |
| `-mode` | `assign` | 分区模式：`assign`（静态分配，要求整个组无活跃成员）或 `group`（加入标准消费组，可能 Rebalance） |
| `-balancer` | `auto` | 消费组分配策略：`auto`、`range`（Kafka 2.2.1 默认）、`roundrobin`、`sticky`、`cooperative-sticky` |
| `-offset-reset` | `earliest` | 无历史提交位点时的重置策略：`earliest` 或 `latest` |
| `-max-msg-per-sec` | `0` | 全局消息 QPS 限速（条/秒，0 为不限制），保护 Broker 磁盘与 CPU |
| `-max-bytes-per-sec`| `0` | 全局网络带宽限速（支持 `50MB`, `100MB`, `1GB`，0 为不限制） |
| `-workers` | `0` | 并发 Worker 数量（0 为自适应，最多 4~8 个） |
| `-commit-interval` | `2s` | Offset 提交间隔（如 `1s`, `2s`, `5s`） |
| `-fetch-min-bytes` | `1MB` | 单次拉取最小字节数（如 `512KB`, `1MB`） |
| `-fetch-max-bytes` | `20MB` | 单次拉取最大字节数（如 `10MB`, `20MB`） |
| `-fetch-part-bytes`| `5MB` | 单分区单次拉取最大字节数（如 `2MB`, `5MB`） |
| `-stats-interval` | `3s` | 终端监控指标打印间隔（如 `1s`, `3s`, `5s`） |
| `-metrics-addr` | `""` | 可选 Prometheus metrics 监听地址（例如 `:9100`） |
| `-sasl-mech` | `""` | SASL 机制：`PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512` |
| `-sasl-user` | `""` | SASL 用户名 |
| `-sasl-pass` | `""` | SASL 密码 |
| `-tls` | `false`| 是否启用 TLS 连接 |
| `-tls-insecure-skip-verify` | `false` | 是否跳过 TLS 证书合法性校验 |

---

## 典型生产场景使用指南（以 kafka_2.12-2.2.1 为例）

### 场景一：停止消费组后跳过历史积压
如果 80 亿积压数据确实不需要任何业务处理：
```bash
./kafka-drainer \
  -brokers "10.0.0.1:9092,10.0.0.2:9092" \
  -group "prod_order_group" \
  -topics "unconsumed_topic_1,unconsumed_topic_2" \
  -action "fast-forward"
```
* **前提与结果**：先停止整个目标消费组的消费者并等待组变为 `Empty`，再执行。程序打印位点差值、提交并回读验证，随后重新查询组 lag；继续生产时可能出现新积压。

---

### 场景二：生产高峰期稳健限速排空（Drain 模式）
若需要真实流式消费推进 Offset，但处于业务高峰期，需要严格保护 Broker 磁盘与网络：
```bash
./kafka-drainer \
  -brokers "10.0.0.1:9092,10.0.0.2:9092" \
  -group "prod_order_group" \
  -topics "unconsumed_topic_1,unconsumed_topic_2" \
  -action "drain" \
  -mode "assign" \
  -max-msg-per-sec 100000 \
  -max-bytes-per-sec 50MB \
  -commit-interval 3s \
  -stats-interval 2s
```
* **效果**：
  * 使用 `assign` 模式前必须停止该组所有消费者，包括其他 Topic 的消费者。
  * 限速 10 万条/秒，带宽上限 50MB/s，平滑保护 Broker 磁盘与网络。

---

### 场景三：业务低峰期全速极速排空
在凌晨业务低峰期，放开限流以最大吞吐量快速清理：
```bash
./kafka-drainer \
  -brokers "10.0.0.1:9092,10.0.0.2:9092" \
  -group "prod_order_group" \
  -topics "unconsumed_topic_1,unconsumed_topic_2" \
  -action "drain" \
  -mode "assign" \
  -workers 8 \
  -fetch-max-bytes 30MB
```

---

### 场景四：带 SASL/SCRAM 认证与 Prometheus 监控
```bash
./kafka-drainer \
  -brokers "kafka-prod-01:9092,kafka-prod-02:9092" \
  -group "prod_order_group" \
  -topics "unconsumed_topic_1,unconsumed_topic_2" \
  -sasl-mech "SCRAM-SHA-512" \
  -sasl-user "admin" \
  -sasl-pass "secret123" \
  -metrics-addr ":9100"
```
* Prometheus 可直接抓取 `http://<ip>:9100/metrics`。

---

## 编译与打包

本目录已包含预编译的本地可执行文件及通用 Linux AMD64 静态二进制包：
* 本地运行：`./kafka-drainer`
* Linux 服务器部署：`./kafka-drainer-linux-amd64`

如需重新编译：
```bash
# 本地编译
make build

# 交叉编译 Linux 生产环境静态无依赖二进制
make build-linux

# 运行单元测试
make test
```
