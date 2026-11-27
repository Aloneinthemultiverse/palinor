# Start the whole PALINOR stack (Windows). Safe to run again: running parts are left alone.
#   powershell -ExecutionPolicy Bypass -File scripts\start_palinor.ps1
# Optional environment variables:
#   PALINOR_PORT        server port (default 8010)
#   PALINOR_PUBLIC_URL  your ngrok static domain, e.g. https://name.ngrok-free.dev (starts an ngrok tunnel)
$ErrorActionPreference = "Continue"  # native commands write to stderr when a service is not up yet
$root = Split-Path $PSScriptRoot -Parent
Set-Location $root
$port = if ($env:PALINOR_PORT) { $env:PALINOR_PORT } else { "8010" }
$py = if (Test-Path ".venv\Scripts\python.exe") { (Resolve-Path ".venv\Scripts\python.exe").Path } else { "python" }
New-Item -ItemType Directory -Force data\logs | Out-Null

# 1. Docker Desktop
docker info --format '{{.ServerVersion}}' 2>$null | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Output "Starting Docker Desktop..."
    Start-Process "C:\Program Files\Docker\Docker\Docker Desktop.exe"
    for ($i = 0; $i -lt 72; $i++) {
        docker info --format '{{.ServerVersion}}' 2>$null | Out-Null
        if ($LASTEXITCODE -eq 0) { break }
        Start-Sleep 5
    }
}

# 2. Kafka (+ topics), Kafka UI, HBase, Neo4j, Spark
Write-Output "Starting containers..."
docker compose up -d
for ($i = 0; $i -lt 60; $i++) {
    # ask Neo4j a real query: its log keeps "Started." from earlier runs, so the log alone says ready too early
    docker exec palinor-neo4j cypher-shell -u neo4j -p palinor-dev-pass "RETURN 1" 2>$null | Out-Null
    if ($LASTEXITCODE -eq 0) { break }
    Start-Sleep 3
}

# 3. PALINOR server (the key protects /mcp; it is created once and kept in data/, which git ignores)
if (-not (Test-Path data\server_key.txt)) {
    & $py -c "import secrets; print(secrets.token_urlsafe(18))" | Out-File -Encoding ascii data\server_key.txt
}
$env:KAFKA_BOOTSTRAP = "localhost:9092"
$env:PALINOR_GRAPH = "neo4j"
$env:PALINOR_SERVER_KEY = (Get-Content data\server_key.txt -Raw).Trim()
$env:PALINOR_PORT = "$port"
$up = $false
try { $up = (Invoke-RestMethod "http://localhost:$port/api/health" -TimeoutSec 3).status -eq "ok" } catch {}
for ($attempt = 1; $attempt -le 3 -and -not $up; $attempt++) {  # retry: the server exits if Neo4j is not ready yet
    Write-Output "Starting PALINOR server (attempt $attempt)..."
    Start-Process $py -ArgumentList "-m", "palinor.server" -WorkingDirectory $root -WindowStyle Hidden `
        -RedirectStandardOutput data\logs\server.log -RedirectStandardError data\logs\server.err.log
    for ($i = 0; $i -lt 20; $i++) {
        try { if ((Invoke-RestMethod "http://localhost:$port/api/health" -TimeoutSec 5).status -eq "ok") { $up = $true; break } } catch {}
        Start-Sleep 3
    }
}

# 4. Streaming consumers: Kafka -> HBase writer, and Kafka -> Spark -> data lake
$sink = Get-CimInstance Win32_Process -Filter "Name='python.exe'" | Where-Object { $_.CommandLine -match 'bigdata\.hbase_sink' }
if (-not $sink) {
    Start-Process $py -ArgumentList "-m", "bigdata.hbase_sink", "--bootstrap", "localhost:9092", "--hbase", "localhost" `
        -WorkingDirectory $root -WindowStyle Hidden -RedirectStandardOutput data\logs\hbase_sink.out.log -RedirectStandardError data\logs\hbase_sink.log
}
$stream = docker exec palinor-spark sh -c "pgrep -f stream_etl" 2>$null
if (-not $stream) {
    docker exec -d -w /opt/palinor/bigdata -e PALINOR_CHECKPOINT_VERSION=v3 -e PALINOR_STARTING_OFFSETS=earliest `
        palinor-spark sh -c "/opt/spark/bin/spark-submit --master 'local[*]' --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 stream_etl.py > /opt/palinor/data/logs/stream_etl.log 2>&1"
}

# 5. Optional public URL for AIs on other machines
$mcp = "http://localhost:$port/mcp?key=$env:PALINOR_SERVER_KEY"
if ($env:PALINOR_PUBLIC_URL -and (Get-Command ngrok -ErrorAction SilentlyContinue)) {
    if (-not (Get-Process ngrok -ErrorAction SilentlyContinue)) {
        Start-Process ngrok -ArgumentList "http", "$port", "--url", $env:PALINOR_PUBLIC_URL, "--log", "data\logs\ngrok.log" -WindowStyle Hidden
    }
    $mcp = "$($env:PALINOR_PUBLIC_URL)/mcp?key=$env:PALINOR_SERVER_KEY"
}

Write-Output ""
if ($up) { Write-Output "PALINOR is running." } else { Write-Output "The server did not come up; see data\logs\server.err.log" }
Write-Output "  Dashboard : http://localhost:$port/?key=$env:PALINOR_SERVER_KEY"
Write-Output "  MCP URL   : $mcp"
Write-Output "  Kafka UI  : http://localhost:8090    HBase UI: http://localhost:16010"
Write-Output "  Neo4j     : http://localhost:7474 (neo4j / palinor-dev-pass)    Spark UI: http://localhost:4040"
