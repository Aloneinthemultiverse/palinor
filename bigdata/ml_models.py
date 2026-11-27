"""Spark MLlib: task-failure classifier + runtime regressor + worker/environment recommendation.

The recommendation table closes the loop: the execution scheduler reads
data/models/env_recommendation.json to pick the environment with the lowest
expected cost  E[runtime] / (1 - P[fail])  for each task type.

spark-submit bigdata/ml_models.py
"""
from __future__ import annotations

import json
import os

from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, RegressionEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import functions as F

from common import MODELS_PATH, load_events, spark_session

CATS = ["task_type", "environment", "platform"]
NUMS = ["complexity", "context_size", "num_dependencies"]


def features():
    idx = [StringIndexer(inputCol=c, outputCol=f"{c}_i", handleInvalid="keep") for c in CATS]
    ohe = OneHotEncoder(inputCols=[f"{c}_i" for c in CATS], outputCols=[f"{c}_v" for c in CATS])
    vec = VectorAssembler(inputCols=[f"{c}_v" for c in CATS] + NUMS, outputCol="features", handleInvalid="skip")
    return idx + [ohe, vec]


def main() -> None:
    spark = spark_session("ml")
    spark.sparkContext.setLogLevel("WARN")
    tasks = (load_events(spark)
             .filter(F.col("event_type").isin("TASK_COMPLETED", "TASK_FAILED"))
             .dropna(subset=CATS + NUMS)
             .withColumn("label", (F.col("event_type") == "TASK_FAILED").cast("double"))
             .withColumn("runtime_min", F.col("duration_ms") / 60000.0)
             .cache())
    n = tasks.count()
    print(f"\n=== PALINOR MLlib on {n:,} finished tasks ===")
    train, test = tasks.randomSplit([0.8, 0.2], seed=42)

    # 1) failure / retry-risk classifier
    clf = Pipeline(stages=features() + [RandomForestClassifier(labelCol="label", numTrees=60, maxDepth=8, seed=42)])
    clf_model = clf.fit(train)
    pred = clf_model.transform(test)
    auc = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC").evaluate(pred)
    base = test.agg(F.avg("label")).first()[0]
    print(f"[failure classifier] AUC = {auc:.3f}   (base failure rate {base:.1%})")

    # 2) runtime regressor (successful tasks only)
    ok_train, ok_test = train.filter("label = 0"), test.filter("label = 0")
    reg = Pipeline(stages=features() + [GBTRegressor(labelCol="runtime_min", maxIter=40, maxDepth=5, seed=42)])
    reg_model = reg.fit(ok_train)
    rp = reg_model.transform(ok_test)
    rmse = RegressionEvaluator(labelCol="runtime_min", metricName="rmse").evaluate(rp)
    mae = RegressionEvaluator(labelCol="runtime_min", metricName="mae").evaluate(rp)
    std = ok_test.agg(F.stddev("runtime_min")).first()[0]
    print(f"[runtime regressor]  RMSE = {rmse:.1f} min, MAE = {mae:.1f} min  (label stddev {std:.1f} min)")

    os.makedirs(MODELS_PATH, exist_ok=True)
    clf_model.write().overwrite().save(f"{MODELS_PATH}/failure_rf")
    reg_model.write().overwrite().save(f"{MODELS_PATH}/runtime_gbt")

    # 3) environment recommendation: score every (task_type, env) at median complexity/context
    med = tasks.approxQuantile(NUMS, [0.5], 0.01)
    task_types = [r[0] for r in tasks.select("task_type").distinct().collect()]
    envs = [r[0] for r in tasks.select("environment").distinct().collect()]
    grid = spark.createDataFrame(
        [(t, e, "claude", int(med[0][0]), int(med[1][0]), int(med[2][0])) for t in task_types for e in envs],
        CATS + NUMS)
    p_fail = F.udf(lambda v: float(v[1]), "double")
    scored = (clf_model.transform(grid).withColumn("p_fail", p_fail("probability"))
              .select(*CATS, "p_fail")
              .join(reg_model.transform(grid).select("task_type", "environment",
                                                     F.col("prediction").alias("runtime_min")),
                    ["task_type", "environment"])
              .withColumn("expected_cost", F.col("runtime_min") / (1 - F.col("p_fail"))))
    best = {}
    for r in scored.orderBy("task_type", "expected_cost").collect():
        best.setdefault(r.task_type, {"environment": r.environment, "p_fail": round(r.p_fail, 3),
                                      "runtime_min": round(r.runtime_min, 1)})
    with open(f"{MODELS_PATH}/env_recommendation.json", "w") as fh:
        json.dump({"auc": auc, "rmse_min": rmse, "recommendation": best}, fh, indent=2)
    print("[scheduler feedback] best environment per task type:")
    for t, b in sorted(best.items()):
        print(f"  {t:<15} → {b['environment']:<7} p_fail={b['p_fail']:.2f}  runtime≈{b['runtime_min']} min")
    spark.stop()


if __name__ == "__main__":
    main()
