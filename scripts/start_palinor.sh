#!/usr/bin/env bash
# Start the whole PALINOR stack (Linux / macOS). Safe to run again: running parts are left alone.
#   ./scripts/start_palinor.sh
# Optional environment variables:
#   PALINOR_PORT        server port (default 8010)
#   PALINOR_PUBLIC_URL  your ngrok static domain, e.g. https://name.ngrok-free.dev (starts an ngrok tunnel)
set -u
cd "$(dirname "$0")/.."
PORT="${PALINOR_PORT:-8010}"
PY=python3; [ -x .venv/bin/python ] && PY=.venv/bin/python
mkdir -p data/logs

docker info >/dev/null 2>&1 || { echo "Docker is not running. Start Docker, then run this again."; exit 1; }

echo "Starting containers..."
docker compose up -d
for _ in $(seq 1 60); do
  # ask Neo4j a real query: its log keeps "Started." from earlier runs, so the log alone says ready too early
  docker exec palinor-neo4j cypher-shell -u neo4j -p palinor-dev-pass "RETURN 1" >/dev/null 2>&1 && break
  sleep 3
done

[ -f data/server_key.txt ] || "$PY" -c "import secrets; print(secrets.token_urlsafe(18))" > data/server_key.txt
export KAFKA_BOOTSTRAP=localhost:9092 PALINOR_GRAPH=neo4j PALINOR_PORT="$PORT"
PALINOR_SERVER_KEY="$(tr -d '[:space:]' < data/server_key.txt)"; export PALINOR_SERVER_KEY

health() { curl -s -m 5 "http://localhost:$PORT/api/health" | grep -q '"ok"'; }
up=0; health && up=1
for attempt in 1 2 3; do  # retry: the server exits if Neo4j is not ready yet
  [ "$up" = 1 ] && break
  echo "Starting PALINOR server (attempt $attempt)..."
  nohup "$PY" -m palinor.server > data/logs/server.log 2> data/logs/server.err.log &
  for _ in $(seq 1 20); do health && { up=1; break; }; sleep 3; done
done

pgrep -f bigdata.hbase_sink >/dev/null || \
  nohup "$PY" -m bigdata.hbase_sink --bootstrap localhost:9092 --hbase localhost \
    > data/logs/hbase_sink.out.log 2> data/logs/hbase_sink.log &
docker exec palinor-spark sh -c "pgrep -f 'stream_etl[.]py'" >/dev/null 2>&1 || \
  docker exec -d -w /opt/palinor/bigdata -e PALINOR_CHECKPOINT_VERSION=v3 -e PALINOR_STARTING_OFFSETS=earliest \
    palinor-spark sh -c "/opt/spark/bin/spark-submit --master 'local[*]' --conf spark.jars.ivy=/tmp/.ivy2 \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 stream_etl.py > /opt/palinor/data/logs/stream_etl.log 2>&1"

MCP="http://localhost:$PORT/mcp?key=$PALINOR_SERVER_KEY"
if [ -n "${PALINOR_PUBLIC_URL:-}" ] && command -v ngrok >/dev/null; then
  pgrep -x ngrok >/dev/null || nohup ngrok http "$PORT" --url "$PALINOR_PUBLIC_URL" --log data/logs/ngrok.log >/dev/null 2>&1 &
  MCP="$PALINOR_PUBLIC_URL/mcp?key=$PALINOR_SERVER_KEY"
fi

echo
if [ "$up" = 1 ]; then echo "PALINOR is running."; else echo "The server did not come up; see data/logs/server.err.log"; fi
echo "  Dashboard : http://localhost:$PORT/?key=$PALINOR_SERVER_KEY"
echo "  MCP URL   : $MCP"
echo "  Kafka UI  : http://localhost:8090    HBase UI: http://localhost:16010"
echo "  Neo4j     : http://localhost:7474 (neo4j / palinor-dev-pass)    Spark UI: http://localhost:4040"
