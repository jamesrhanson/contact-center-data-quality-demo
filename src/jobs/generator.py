# Databricks notebook source
# MAGIC %md
# MAGIC # Live generator — cc_dq_generator_1e53d4
# MAGIC Every ~60 s writes one calls file plus the matching transcripts and quality_scores files
# MAGIC into `landing/`. Runs up to `max_minutes`, then exits. **Stop = cancel the run.**

# COMMAND ----------
import random
import time

import datagen

dbutils.widgets.text("volume_root", "")
dbutils.widgets.text("max_minutes", "120")
dbutils.widgets.text("interval_seconds", "60")
root = dbutils.widgets.get("volume_root")
max_minutes = float(dbutils.widgets.get("max_minutes"))
interval = float(dbutils.widgets.get("interval_seconds"))

rng = random.Random()
deadline = time.time() + max_minutes * 60
n = 0
while time.time() < deadline:
    t0 = time.time()
    rows = datagen.write_live_file(root, rng)
    n += 1
    print(f"file {n}: {rows} calls")
    time.sleep(max(1.0, interval - (time.time() - t0)))
print(f"done: {n} files")
