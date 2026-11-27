#!/usr/bin/env sh
# Run a PALINOR Spark job inside the spark container.
#   ./run_spark.sh stream_etl | rdd_analysis | sql_analytics | ml_models
# PALINOR_* tuning variables set on the host are passed through when set (unset ones are not):
#   PALINOR_CHECKPOINT_VERSION=v2 PALINOR_RESUME_FROM=_checkpoints ./run_spark.sh stream_etl
# (Git Bash on Windows: prefix with MSYS_NO_PATHCONV=1 so container paths are not rewritten.)
set -e
JOB="${1:?usage: run_spark.sh <stream_etl|rdd_analysis|sql_analytics|ml_models>}"
docker exec -w /opt/palinor/bigdata \
  -e PALINOR_CHECKPOINT_VERSION -e PALINOR_STARTING_OFFSETS -e PALINOR_RESUME_FROM \
  -e PALINOR_TOPIC_PARTITIONS -e PALINOR_TRIGGER -e PALINOR_MAX_OFFSETS -e PALINOR_SHUFFLE \
  palinor-spark /opt/spark/bin/spark-submit \
  --master "local[*]" \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  "$JOB.py"
