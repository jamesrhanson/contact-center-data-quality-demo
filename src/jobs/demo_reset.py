# Databricks notebook source
# MAGIC %md
# MAGIC # demo_reset — cc_dq_demo_reset_1e53d4
# MAGIC Idempotent. Never edits pipeline code. Restores the starting state:
# MAGIC 1. cancel generator / pipeline-job runs  2. bad file back to `held_out/`, empty `rejected/`
# MAGIC 3. clear landing, write 7-day backfill + agents, recreate `dq_rules`
# MAGIC 4. full refresh both pipelines (dev only)  5. run pipeline job, start generator  6. print counts

# COMMAND ----------
import glob
import os
import shutil
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import UpdateInfoState

import datagen

for k in ["catalog", "schema", "volume_root", "bronze_reset_allowed", "ingestion_pipeline_id",
          "transformation_pipeline_id", "pipeline_job_id", "generator_job_id", "start_generator"]:
    dbutils.widgets.text(k, "")
p = {k: dbutils.widgets.get(k) for k in ["catalog", "schema", "volume_root", "bronze_reset_allowed",
     "ingestion_pipeline_id", "transformation_pipeline_id", "pipeline_job_id", "generator_job_id",
     "start_generator"]}
fq = f"{p['catalog']}.{p['schema']}"
root = p["volume_root"]
w = WorkspaceClient()

if p["bronze_reset_allowed"].lower() != "true":
    raise RuntimeError("demo_reset refuses to run: bronze_reset_allowed is false in this target "
                       "(full refresh would wipe bronze history).")

# COMMAND ----------
# 1. Cancel active runs
for job_id in [p["generator_job_id"], p["pipeline_job_id"]]:
    for r in w.jobs.list_runs(job_id=int(job_id), active_only=True):
        print(f"cancel run {r.run_id} of job {job_id}")
        w.jobs.cancel_run(r.run_id)
for pid in [p["ingestion_pipeline_id"], p["transformation_pipeline_id"]]:
    st = w.pipelines.get(pid)
    if st.state and st.state.value in ("RUNNING", "DEPLOYING", "STARTING"):
        w.pipelines.stop(pid)
time.sleep(10)

# COMMAND ----------
# 2. Bad batch back to held_out, empty rejected/
held = f"{root}/held_out/calls/calls_vendor_bad_batch.json"
found = sorted(glob.glob(f"{root}/landing/calls/calls_vendor_bad_batch*.json")
               + glob.glob(f"{root}/rejected/calls/*.json"))
os.makedirs(os.path.dirname(held), exist_ok=True)
for f in found:
    if os.path.basename(f).startswith("calls_vendor_bad_batch") and not os.path.exists(held):
        shutil.move(f, held)
if not os.path.exists(held):
    print("bad batch missing everywhere: regenerating", datagen.write_bad_batch(root))
shutil.rmtree(f"{root}/rejected", ignore_errors=True)
os.makedirs(f"{root}/rejected/calls", exist_ok=True)

# COMMAND ----------
# 3. Clear landing, write backfill + agents, recreate dq_rules
shutil.rmtree(f"{root}/landing", ignore_errors=True)
print("backfill calls rows:", datagen.write_backfill(root))

spark.sql(f"""CREATE OR REPLACE TABLE {fq}.dq_rules (
  rule_name STRING, dataset STRING, constraint_sql STRING, action STRING, owner STRING, description STRING)
  COMMENT 'Data quality rules as data. quality_scores_clean groups these by action (warn/drop/fail).'""")
rules = [
    ("valid_empathy_score", "empathy_score IS NULL OR empathy_score BETWEEN 1 AND 5", "Empathy score in 1-5"),
    ("valid_resolution_score", "resolution_score IS NULL OR resolution_score BETWEEN 1 AND 5", "Resolution score in 1-5"),
    ("valid_compliance_score", "compliance_score IS NULL OR compliance_score BETWEEN 1 AND 5", "Compliance score in 1-5"),
    ("empathy_score_not_null", "empathy_score IS NOT NULL", "Empathy score present"),
    ("resolution_score_not_null", "resolution_score IS NOT NULL", "Resolution score present"),
    ("compliance_score_not_null", "compliance_score IS NOT NULL", "Compliance score present"),
    ("valid_model_output_json", "try_parse_json(raw_model_output) IS NOT NULL", "Model output is valid JSON"),
]
spark.createDataFrame(
    [(n, "quality_scores", c, "drop", "qa-analytics@example.com", d) for n, c, d in rules],
    "rule_name STRING, dataset STRING, constraint_sql STRING, action STRING, owner STRING, description STRING",
).write.mode("append").saveAsTable(f"{fq}.dq_rules")

# COMMAND ----------
# 4. Full refresh both pipelines (dev only; guarded above)
def run_update(pid, full_refresh):
    uid = w.pipelines.start_update(pid, full_refresh=full_refresh).update_id
    while True:
        u = w.pipelines.get_update(pid, uid).update
        if u.state in (UpdateInfoState.COMPLETED, UpdateInfoState.FAILED, UpdateInfoState.CANCELED):
            print(pid, "full_refresh" if full_refresh else "update", u.state)
            if u.state != UpdateInfoState.COMPLETED:
                raise RuntimeError(f"pipeline {pid} update {uid} ended {u.state}")
            return
        time.sleep(15)

run_update(p["ingestion_pipeline_id"], True)
run_update(p["transformation_pipeline_id"], True)

# Monitoring views over the published event logs (event logs now exist)
sql_text = open(os.path.join(os.getcwd(), "..", "sql", "monitoring_views.sql")).read().replace("{fq}", fq)
for stmt in sql_text.split(";"):
    if "CREATE" in stmt:
        spark.sql(stmt)
print("monitoring views created: dq_expectation_metrics, dq_pipeline_failures")

# COMMAND ----------
# 5. Run the pipeline job once and wait, then start the generator (no wait)
run = w.jobs.run_now_and_wait(int(p["pipeline_job_id"]))
print("pipeline job:", run.state.result_state)
if p["start_generator"].lower() == "true":
    print("generator run:", w.jobs.run_now(int(p["generator_job_id"])).run_id)

# COMMAND ----------
# 6. Counts, invariants, expectation metrics
for t in ["calls_raw", "transcripts_raw", "quality_scores_raw", "agents", "calls_clean", "calls_quarantine",
          "transcripts_clean", "quality_scores_clean", "quality_scores_quarantine", "daily_call_checks",
          "daily_volume_checks",           "call_quality_daily", "dq_daily"]:
    print(f"{t:28s}", spark.table(f"{fq}.{t}").count())
spark.sql(f"""SELECT 'calls' ds, (SELECT count(*) FROM {fq}.calls_raw) raw, (SELECT count(*) FROM {fq}.calls_clean) clean,
  (SELECT count(*) FROM {fq}.calls_quarantine) quarantine
  UNION ALL SELECT 'quality_scores', (SELECT count(*) FROM {fq}.quality_scores_raw), (SELECT count(*) FROM {fq}.quality_scores_clean),
  (SELECT count(*) FROM {fq}.quality_scores_quarantine)""").withColumnRenamed("ds", "dataset").show()
print("rejected/:", os.listdir(f"{root}/rejected/calls"), " held_out:", os.listdir(f"{root}/held_out/calls"))
spark.sql(f"SELECT rule_name, action FROM {fq}.dq_rules ORDER BY rule_name").show()
