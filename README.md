# 聚合口摘链演练（FastAPI 本地版）

在本地模拟“从聚合口摘掉一根成员链路”的值班操作：两条 **FIFO** 成员链路（A/B）
的送达延时与确认延时都可调，按流发送带编号的报文，申请摘链时设置等待时限和
缓冲上限，然后观察对端收包顺序、每包实际走的链路，以及本次摘链的最终结果。

## 规则（与演练行为一一对应）

- **两条链路都是严格 FIFO**：同一条链路同一时刻只处理一个报文，先送达再等确认，
  然后才取下一个。因此同一条流按接纳顺序送达，不丢不重。
- **对端确认**：摘链申请创建时，把成员链路上**当时已排队/在途**的受影响流报文
  记为一个快照。对端把快照中每个报文都收齐并回确认后，才把受影响流切到另一条链路。
- **等待期间**：待迁流的新包进入缓冲；缓冲总数达到上限就返回 `429` 拒收，**其他流
  照常发送**。被拒的编号没有被接纳，摘链结束后可以重发。
- **超时**：到达等待时限仍未收齐确认，本次摘链失败（`timed_out`），流不迁移，
  缓冲的报文按接纳顺序回到**原链路**。之后才到的确认会被记录，但**不会改变**本次结果。
- **一次只处理一个摘链申请**：已有 pending 申请时，第二个申请返回 `409`。
- 摘链成功后，成员链路从聚合组中摘除（`removed: true`），不能再在它上面建流；
  此时两条链路只剩一条，无法再摘，直到 `POST /api/reset` 复原。

## 启动

```bash
docker compose up --build -d
# 打开 http://localhost:8000   （端口可用 DRILL_PORT=9000 覆盖）
```

容器只有一个 worker（状态在内存里），健康检查打 `/healthz`。初始延时可通过
`.env`（参考 `.env.example`）或启动后 `PATCH /api/links/{A|B}` 调整。

不想用 Docker 时可以直接跑：

```bash
python3 -m pip install -r requirements.txt
python3 -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 1
```

## 可直接调用的演练请求

以下命令假设服务在 `http://localhost:8000`，按顺序执行即可复现一次成功摘链。
时间单位均为毫秒。

```bash
BASE=http://localhost:8000

# 0) 复位（清空流、申请和事件）
curl -s -X POST $BASE/api/reset

# 1) 把链路延时调小，方便观察（可选）
curl -s -X PATCH $BASE/api/links/A \
  -H 'Content-Type: application/json' \
  -d '{"delivery_delay_ms": 300, "ack_delay_ms": 200}'
curl -s -X PATCH $BASE/api/links/B \
  -H 'Content-Type: application/json' \
  -d '{"delivery_delay_ms": 150, "ack_delay_ms": 80}'

# 2) 建两条流：voice 走 A（待摘），video 走 B
curl -s -X POST $BASE/api/flows \
  -H 'Content-Type: application/json' -d '{"flow_id":"voice","link":"A"}'
curl -s -X POST $BASE/api/flows \
  -H 'Content-Type: application/json' -d '{"flow_id":"video","link":"B"}'

# 3) voice 先发两包，让它们排在 A 上
curl -s -X POST $BASE/api/flows/voice/packets \
  -H 'Content-Type: application/json' -d '{"seq":1,"payload":"v1"}'
curl -s -X POST $BASE/api/flows/voice/packets \
  -H 'Content-Type: application/json' -d '{"seq":2,"payload":"v2"}'

# 4) 申请摘 A：最多等 5s，缓冲上限 5 包；记下 removal_id
RM=$(curl -s -X POST $BASE/api/removals \
  -H 'Content-Type: application/json' \
  -d '{"member_link":"A","wait_ms":5000,"buffer_limit":5}' \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["removal_id"])')
echo "removal_id=$RM"

# 5) 等待期间给 voice 再发一包 -> 202，进入缓冲
curl -s -X POST $BASE/api/flows/voice/packets \
  -H 'Content-Type: application/json' -d '{"seq":3}'

# 6) 其他流不受影响
curl -s -X POST $BASE/api/flows/video/packets \
  -H 'Content-Type: application/json' -d '{"seq":1}'

# 7) 再申请一次摘链 -> 409（一次只处理一个）
curl -s -X POST $BASE/api/removals \
  -H 'Content-Type: application/json' \
  -d '{"member_link":"B","wait_ms":1000,"buffer_limit":1}'

# 8) 等对端确认（A 上两包依次 300+200ms）后查看结果：
#    status=succeeded, moved_flow_ids=["voice"], flushed_to="B", snapshot_acked=2
sleep 1.2
curl -s $BASE/api/removals/$RM | python3 -m json.tool

# 9) 对端收包顺序：1,2 走 A，3 走 B，按 1,2,3 送达
sleep 0.3
curl -s $BASE/api/flows/voice/received | python3 -m json.tool
```

超时场景：把第 4 步的 `wait_ms` 调成比 `delivery+ack` 小（例如 `50`），再发第 5 步。
结果会是 `timed_out`、`moved_flow_ids: []`、`flushed_to: "A"`，之后迟到的确认
只体现在 `late_confirmations_ignored`。

缓冲上限场景：把 `buffer_limit` 设成 `1`，等待期间发两包，第二包返回 `429`。
被拒的 seq 未接纳，摘链完成后可以重发。

### 一键跑完整演练脚本

```bash
# 本机已有服务时
python3 drills/run_drill.py --base-url http://localhost:8000
# 或让 Compose 起一个一次性客户端（跑完即退出）
docker compose --profile drill run --rm drill-client
# 只跑某个场景：--only success|timeout|buffer|after_removal
```

脚本只依赖标准库，四个场景全部通过时返回 0，否则返回 1 并列出失败项。

## 接口一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/healthz` | 健康检查 |
| GET | `/api/links` | 两条链路的延时、队列深度、收发计数、是否已摘除 |
| PATCH | `/api/links/{A\|B}` | 调整 `delivery_delay_ms` / `ack_delay_ms` |
| GET | `/api/flows` | 所有流及当前承载链路、已接纳/已送达计数 |
| POST | `/api/flows` | 建流 `{flow_id?, link}` |
| POST | `/api/flows/{id}/packets` | 发包 `{seq, payload?}`；正常入队 201，进缓冲 202，缓冲满 429，重复 seq 409 |
| GET | `/api/flows/{id}/received` | 对端收包顺序（序号、报文 ID、链路、时间） |
| GET | `/api/packets/{packet_id}` | 单个报文的状态与走过的链路 |
| POST | `/api/removals` | 申请摘链 `{member_link, wait_ms, buffer_limit}`；需等待 202，立即完成 200 |
| GET | `/api/removals` | 所有申请（倒序摘要） |
| GET | `/api/removals/{id}` | 单个申请详情：快照、确认进度、缓冲、拒收、迟到确认、事件时间线 |
| GET | `/api/state` | 链路 + 流 + 当前申请 + 最近事件 |
| POST | `/api/reset` | 清空并重建两条链路 |

交互界面在 `http://localhost:8000/`，包含链路延时调节、建流/发包、摘链申请、
对端收包顺序、事件日志，每 0.5s 自动刷新。

## 目录

```
app/main.py             FastAPI 路由与静态页挂载
app/engine.py           演练核心：FIFO 链路、流、报文、摘链状态机
app/config.py           从环境变量读取初始链路延时
app/static/index.html   值班界面（无外部依赖）
drills/run_drill.py     端到端演练脚本（仅标准库）
tests/test_drill.py     pytest 测试
Dockerfile / docker-compose.yml
```

## 测试

```bash
python3 -m pip install -r requirements-dev.txt
python3 -m pytest -q
```

## 已知边界（本地演练的取舍）

- 状态全部在内存，重启容器即清空；只适合演练，不作持久化。
- 每个成员链路用一条 `asyncio` 任务串行处理，模拟 FIFO；没有真实网络丢包/乱序。
- “对端确认”由服务内部模拟：快照报文全部被确认即视为对端收齐；没有独立的对端进程。
- 超时判定是精确的一次性定时器，需等待时长与链路延时同量级时，边界上偶发的
  先后顺序属于正常现象。
