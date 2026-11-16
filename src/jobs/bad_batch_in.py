# Databricks notebook source
# MAGIC %md
# MAGIC # Move bad batch in — cc_dq_bad_batch_in_1e53d4
# MAGIC Moves `held_out/calls/calls_vendor_bad_batch.json` into `landing/calls/` with a fresh,
# MAGIC time-stamped name (a path Auto Loader has never seen).
# MAGIC Moving it to `rejected/calls/` during recovery is a **manual, stock** step.

# COMMAND ----------
import os
import shutil
from datetime import datetime, timezone

dbutils.widgets.text("volume_root", "")
root = dbutils.widgets.get("volume_root")
src = f"{root}/held_out/calls/calls_vendor_bad_batch.json"
if not os.path.exists(src):
    raise FileNotFoundError("bad batch already moved; run demo_reset")
stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
dst = f"{root}/landing/calls/calls_vendor_bad_batch_{stamp}.json"
shutil.move(src, dst)
print(f"moved bad batch -> {dst}")
dbutils.jobs.taskValues.set("bad_file", dst)
