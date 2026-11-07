# 本机端口登记表

新项目在本机启动服务前，**先扫端口再定端口**。撞端口会导致启动失败，或更糟——静默串号（请求打到别的服务上）。

## 快速使用

```bash
cd ~/Documents/GitHub/tdai-new

python3 tools/scan_ports.py                # 全量扫描 + 预留段提示
python3 tools/scan_ports.py --check 8420   # 单端口检查（退出码 0=空闲，1=占用）
python3 tools/scan_ports.py --free 8400 5  # 从 8400 起找 5 个连续空闲端口
python3 tools/scan_ports.py --json         # JSON 输出
```

脚本无外部依赖，macOS/Linux 通用。

---

## 长期占用端口

这些服务由 launchd 或常驻进程托管，**开机就在**，新项目必须避让。

| 端口 | 进程 | 归属 | 能否动 |
|---|---|---|---|
| 8420 | node | **TDAI MemoryCore** — Hermes 插件直连的记忆核心 | 勿占 |
| 8421 | node | **TDAI MemoryKnowledge** — 知识库 | 勿占 |
| 8123 | node | **TDAI MemoryPanel** — Web 控制台 | 勿占 |
| 8096 | node | **TDAI MemoryProxy** — 记忆代理入口（opencode / dsh 走这里） | 勿占 |
| 11434 | ollama | Ollama — 本地 LLM 服务（TDAI 的 LLM：qwen3.8:27b-mlx） | 勿占 |
| 11435 | ollama | Ollama — **TDAI 专用 embedding 实例**（bge-m3，与 11434 的 LLM 隔离） | 勿占 |
| 19333 | python | OpenViking — 记忆服务 | 勿占 |
| 5432 | postgres | PostgreSQL 17 | 勿占 |
| 1883 | mosquitto | MQTT broker | 别动 |
| 3906 | UlanziDeck | Ulanzi Studio 灯光控制台 | 别动 |
| 3283 | ARDAgent | macOS 远程管理（系统） | 系统，勿动 |

### 已装未启（占位不算占用，但要用先确认）

| 端口 | 服务 | 状态 |
|---|---|---|
| 6379 | Redis (homebrew) | **2026-11-07 已卸载**（TDAI 走 local state backend，MemoryProxy 的 `redis.enabled=false`） |
| 5672 / 15672 | RabbitMQ | **2026-11-07 已卸载**（连同 Erlang，零引用） |

---

## 动态端口（会变，不需要避让）

这些是应用运行时自行分配的，端口号每次可能不同：

```
49374  opencode serve          49432  llama-server
49503 / 49853  豆包浏览器       55364 / 65510  Ollama
58494  rapportd (系统)          59753  hermes 工具进程
60859  clash-verge             65376  DeepSeek Harness
65404  OpenCode.app
```

> 规律：macOS 给应用分配的动态端口通常落在 **49152–65535**。新项目若要固定端口，别选这个区间。

---

## 预留段规则

新项目按语义挑段，**不要用 3000 / 5173 / 8080 / 9000 这类默认值**（空着但撞车概率高）。

| 段 | 用途 | 现状 |
|---|---|---|
| 8400–8499 | TDAI 记忆系统内部 | 已占 8420, 8421 |
| 8100–8199 | TDAI Panel / Web UI | 已占 8123 |
| 8000–8099 | TDAI Proxy / 通用后端 | 已占 8096 |
| 18700–18999 | AI Agent 网关 | 全空 ✅ |

TDAI 内部服务定法：

| 服务 | 端口 |
|---|---|
| MemoryCore | 8420 |
| MemoryKnowledge | 8421 |
| MemoryPanel | 8123 |
| MemoryProxy | 8096 |

新增 TDAI 子服务从 **8422** 起顺延，不要跳号。

---

## 新增服务登记流程

1. 找空闲端口
   ```bash
   python3 tools/scan_ports.py --free 8400 5
   ```
2. 启动服务，确认端口确实被自己占用
   ```bash
   python3 tools/scan_ports.py --check <端口>
   ```
3. 登记到三处，缺一不可：
   - `tools/scan_ports.py` 的 `REGISTRY` 字典 → 让扫描器标出归属
   - 服务自身的配置文件（`~/.memory-tencentdb/*.yaml` 或项目内 config）
   - `~/Library/LaunchAgents/*.plist`（若用 launchd 托管）
4. launchd 的 `Label` 必须唯一：`com.<用户>.<项目>.<服务>`

---

## 撞端口排查

```bash
# 谁占着
lsof -nP -iTCP -sTCP:LISTEN | grep <端口>

# 完整命令行
ps -p <PID> -o command=
```

注意：

- macOS 上 `lsof -i :PORT` 需要 root 才能看全连接，`-iTCP -sTCP:LISTEN` 不需要
- 加 `-nP` 跳过 DNS 反查，扫描快很多
- 端口被占但找不到进程 → 检查 launchd：`launchctl list | grep <关键词>`

---

## 相关 launchd 服务清单

`~/Library/LaunchAgents/` 下的用户级服务：

```
ai.hermes.gateway                          Hermes 多 profile 网关
com.damien.tdai.memory-core                TDAI 记忆核心
com.damien.tdai.memory-knowledge          TDAI 知识库
com.damien.tdai.memory-panel               TDAI 控制台
com.damien.tdai.memory-proxy               TDAI 记忆代理
com.damien.openviking.server               OpenViking
com.damien.sync-comfyui-covers             ComfyUI 封面同步
com.ulanzi.UlanziDeck                      灯光台
homebrew.mxcl.{postgresql@14,postgresql@17,redis,rabbitmq,mosquitto,frpc}
pm2.damien                                 pm2 托管的 Node 进程
```

查询运行状态：

```bash
launchctl list | grep -iE "tdai|hermes|memory"
```
