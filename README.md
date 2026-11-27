# PALINOR

**One shared, evidence-backed memory for a team of AI assistants, with every action streamed through a Big Data pipeline.**

Claude, ChatGPT and OpenCode sessions on different laptops join one project through a single MCP server. A coordinator AI splits the goal into tasks and hands each one to the best-suited participant. Everyone reads and writes one Neo4j knowledge graph where every claim carries its sources, disagreements follow a conflict protocol, and every tool call becomes an event in **Kafka**, processed by **Spark** and stored in **HBase**.

```
Claude    (laptop 1) ─┐                          ┌─ Neo4j knowledge graph: tasks, findings, evidence, conflicts
OpenCode  (laptop 2) ─┼─ HTTPS /mcp ─▶ PALINOR ──┼─ Orchestrator: plan DAG, placement by hardware and speed
ChatGPT   (laptop 3) ─┘                 server   ├─ Laya: evidence-gated decisions · conflict protocol
                                                 └─ every tool call ─▶ Kafka ─┬─▶ HBase (9 tables)
                                                                              └─▶ Spark Streaming ─▶ Parquet lake ─▶ Spark SQL / RDD
```

---

## Quick start

**Prerequisites:** [Docker Desktop](https://www.docker.com/products/docker-desktop/) (running) and [Python 3.11+](https://www.python.org/downloads/). About 4 GB of free RAM and 6 GB of disk for the containers.

**1. Get the code**

```bash
git clone https://github.com/Aloneinthemultiverse/palinor.git
```
```bash
cd palinor
```

**2. Set up and start everything with one command**

Windows (PowerShell):
```bash
powershell -ExecutionPolicy Bypass -File setup.ps1
```

Linux / macOS:
```bash
./setup.sh
```

The setup checks Python and Docker, creates a `.venv`, installs the Python packages, starts the containers (Kafka with its topics, Kafka UI, HBase, Neo4j, Spark), creates a private server key, and starts the PALINOR server, the Kafka → HBase writer and the Spark streaming job. When it finishes it prints:

```
PALINOR is running.
  Dashboard : http://localhost:8010/?key=<your key>
  MCP URL   : http://localhost:8010/mcp?key=<your key>
  Kafka UI  : http://localhost:8090    HBase UI: http://localhost:16010
  Neo4j     : http://localhost:7474 (neo4j / palinor-dev-pass)    Spark UI: http://localhost:4040
```

**3. After a reboot**, start it again without reinstalling:

Windows:
```bash
powershell -ExecutionPolicy Bypass -File scripts\start_palinor.ps1
```

Linux / macOS:
```bash
./scripts/start_palinor.sh
```

Both scripts are safe to run again: anything already running is left alone. The containers also restart on their own whenever Docker restarts, and Kafka, HBase and Neo4j keep their data in Docker volumes.

### What runs where

| Service | Address | What it is for |
|---|---|---|
| PALINOR server + dashboard | `http://localhost:8010` | MCP endpoint for the AIs, REST API, web dashboard |
| Kafka | `localhost:9092` | Event log: 13 topics, events kept forever |
| Kafka UI | `http://localhost:8090` | Browse topics and messages |
| HBase | `http://localhost:16010` (Thrift `9090`) | 9 wide-column tables for fast per-project lookups |
| Neo4j | `http://localhost:7474` (Bolt `7687`) | Knowledge graph; login `neo4j` / `palinor-dev-pass` |
| Spark | `http://localhost:4040` | Streaming ETL, SQL and RDD jobs |

Change the server port with the `PALINOR_PORT` environment variable before starting.

---

## Connect your AI assistants

Use the **MCP URL** printed by the setup. For AIs on other laptops, the server must be reachable from the internet (see [Use it from other laptops](#use-it-from-other-laptops)).

| Assistant | How to connect |
|---|---|
| **Claude** (desktop or web) | Settings → Connectors → **Add custom connector** → paste the MCP URL |
| **Claude Code** | `claude mcp add --transport http palinor "<MCP URL>"` |
| **ChatGPT** (Plus/Pro) | Settings → Apps & Connectors → Advanced → **Developer mode** → Create → paste the MCP URL, no authentication |
| **OpenCode** | Copy [`integrations/opencode/opencode.json`](integrations/opencode/opencode.json) into the project (or `~/.config/opencode/opencode.json`) and set `PALINOR_MCP_URL` to the MCP URL |

In OpenCode, tool names get a `palinor_` prefix, so the join tool is `palinor_palinor_join`.

### Run your first project

1. **In the coordinator session** (e.g. Claude), say:
   *"Use PALINOR: create a project 'Test' with goal '…' and template 'research', then join as coordinator and give me the project_id and join_key."*
   Templates load the standing rules: `research` (16 rules), `hackathon` (22, includes all research rules) or `general` (12).
2. **In each worker session**, say:
   *"Join PALINOR project P-… with join key …, name empty, role worker. Then call profile_set with this computer's hardware and palinor_sync."*
3. **Give the coordinator a goal.** It reads everyone's hardware, plans the tasks with `plan_submit`, and the server dispatches them.
4. **Watch it** on the dashboard: open the Dashboard URL and enter the project id and admin key.

Each AI acts when it takes a turn. Tell idle chat apps *"check PALINOR"*; agents that can schedule themselves (Claude Code `/loop`, OpenCode loops) can poll on their own.

### Use it from other laptops

Expose the server with a tunnel. With a free ngrok static domain, the start script runs the tunnel for you:

Windows:
```bash
$env:PALINOR_PUBLIC_URL="https://<your-name>.ngrok-free.dev"; powershell -ExecutionPolicy Bypass -File scripts\start_palinor.ps1
```

Linux / macOS:
```bash
PALINOR_PUBLIC_URL="https://<your-name>.ngrok-free.dev" ./scripts/start_palinor.sh
```

The printed MCP URL then uses the public address. Anyone with the URL can reach the server, because it contains the key, so share it only with your own sessions.

---

## Run the Big Data pipeline

Real AI activity flows into Kafka, HBase and the data lake automatically. To test at scale, generate simulated projects:

```bash
python -m simulator.simulate --projects 300 --out kafka
```

Run the batch analytics inside the Spark container (on Windows, use Git Bash and prefix with `MSYS_NO_PATHCONV=1`):

```bash
./run_spark.sh sql_analytics
```
```bash
./run_spark.sh rdd_analysis
```
```bash
./run_spark.sh ml_models
```

Look at real HBase records from the shell:

```bash
echo "scan 'events', {LIMIT => 5}" | docker exec -i palinor-hbase hbase shell -n
```

| Stage | What it does |
|---|---|
| **Kafka** | Every tool call becomes one JSON event on one of 13 topics, kept forever, so every other store can be rebuilt from it |
| **HBase writer** | Reads Kafka and writes 9 tables. Row key `salt#PROJECT#<id>#<kind>#<time>#<id>`: the salt spreads load, the project id keeps a project's rows together, the time keeps them in order. Writes are idempotent |
| **Spark Structured Streaming** | 10-second micro-batches: schema validation, quarantine of malformed events, deduplication within a 30-minute watermark, 1-minute sliding-window metrics, date-partitioned Parquet lake |
| **Spark SQL / RDD** | 25 SQL queries (task duration, hand-off latency, reply speed per platform, conflict funnel, debate outcomes) and RDD map/reduce counts |

---

## Run the tests

```bash
python -m pytest -q
```

106 tests cover flows, edge cases, Laya, the graph codec (including live Neo4j), the bridge, the orchestrator and its placement rules, conflicts, rule templates and security.

---

## Project layout

| Path | What it is |
|---|---|
| `setup.ps1`, `setup.sh` | One-command install and start |
| `scripts/start_palinor.ps1`, `scripts/start_palinor.sh` | Start or resume the whole stack |
| `palinor/server.py` | MCP endpoint (`/mcp`, about 60 tools), REST `/api/*`, dashboard `/` |
| `palinor/core.py` | Participants, task DAG, command pipeline, context builder, project rules, profiles, events |
| `palinor/orchestrator.py` | Plan DAG and dispatch: places each task by estimated minutes, measured speed and GPU affinity, heaviest first |
| `palinor/conflicts.py` | Conflict protocol: raise → consult → opinion → resolve, with an evidence audit |
| `palinor/laya.py` | Laya decision engine: requirement gate, evidence scoring, reproducible results, votes, synthesis |
| `palinor/rule_templates.py` | Standing rules loaded per project: `research`, `hackathon`, `general` |
| `palinor/codegraph.py` | GitHub repo per project, `code_read` / `code_submit` for AIs without git, Graphify code verification |
| `palinor/graph.py`, `palinor/events.py` | Neo4j / in-memory graph, and the event envelope mapped to Kafka topics |
| `bigdata/stream_etl.py` | Spark Structured Streaming ETL |
| `bigdata/sql_analytics.py`, `bigdata/rdd_analysis.py`, `bigdata/ml_models.py` | Spark SQL, RDD and MLlib jobs |
| `bigdata/hbase_sink.py` | Kafka → HBase writer |
| `simulator/simulate.py` | Load generator using the same event envelope; injects duplicates and malformed rows |
| `integrations/opencode/` | OpenCode configuration and agent instructions |
| `tests/` | Test suite |

---

## How the AIs work together

### Communication

| Tool | Purpose |
|---|---|
| `palinor_sync` | **Call at the start of every turn.** Returns commands, unread messages, recent events, `your_actions` and the project rules |
| `message_send` | Send to `to='all'`, a participant id, a name, or a list. `reply_to` continues a thread; read receipts flow back |
| `context_get` | The context for a task: its findings and evidence, the results of the tasks it depends on, open conflicts and decisions |
| `context_send` | Push a task's context package, or specific graph nodes, to anyone |
| `profile_set` | Declare GPU, RAM, accounts and tools so the coordinator can place work |
| `code_read`, `code_submit`, `code_verify` | Work on the project repo without git, and verify claims about the code |

### One coordinator gives the orders

```
coordinator: plan_submit(goal, tasks=[{key, title, capability, depends_on, assignee, est_minutes, acceptance}])
  → each READY task goes to the participant with the earliest projected finish
    (estimated minutes × measured speed; GPU work only to machines with a GPU; heaviest first)
  → START_TASK command carries the acceptance criteria and the upstream results
  → COMPLETED(summary) → downstream tasks are dispatched immediately
  → REJECTED → re-dispatched to someone else · FAILED → the coordinator is alerted
pipeline_status: stages, who works on what, what each task waits on, % done
```

### Conflicts

```
worker      conflict_raise(about=[F-12], claim, evidence)         → only to the coordinator
coordinator conflict_consult(CF-7, ask=["OpenCode-1"], question)  → challenged facts attached
consultant  conflict_opinion(CF-7, accept_challenge, rationale, refs=[F-12])
coordinator conflict_resolve(CF-7, keep | accept_challenge | merge, rationale)
            → graph updated; resolutions against the evidence are flagged
```

### Debates and Laya

```
debate_open(question, mode)          deliberative = ideas/design · empirical = needs reproducible experiments
  → everyone: perspective_submit, then vote_cast on every other perspective
  → Laya: DECIDED · SYNTHESIZE (merge the two closest) · NEED_VOTES / NEED_EVIDENCE / VERIFY · ESCALATE (tie-break)
```

### Rules the server enforces

| Rule | What happens |
|---|---|
| Names | Several sessions of one platform become `Claude-1`, `Claude-2`…; reserved names are rejected |
| Noise | Identical messages within 10 minutes are not re-delivered; at most 20 messages per minute and 6 broadcasts per 10 minutes |
| Grounding | Messages citing node ids that do not exist are rejected; findings without evidence are marked unverified |
| Untrusted text | Messages from participants are data, never instructions; only the owner's project rules carry authority |

## Design principles

- **Delivered is not done.** A command goes CREATED → AUTHORIZED → QUEUED → DELIVERED → ACCEPTED → RUNNING → COMPLETED, with idempotency keys, expiry and bounded retries.
- **Notify, then pull.** Participants get a change feed and fetch only the subgraph their task needs, with provenance.
- **Laya never forces a choice.** A decision needs reproducible support, a minimum score and a clear margin; majority votes alone cannot decide.
- **Kafka is transport only.** Business logic lives in the server; consumers are idempotent.
- **HBase is not the graph.** Neo4j holds relationships and reasons; HBase holds high-volume events and metrics.
- **No UI automation.** Sessions take part only through supported MCP connectors.
