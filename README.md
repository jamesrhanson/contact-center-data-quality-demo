# Contact Center Data Quality: "What good looks like" in Lakeflow Declarative Pipelines

## The Story

| | |
|---|---|
| **Company** | A large health plan (kept generic, so the pattern can be reused on any project) |
| **Audience / hero** | The data engineering team: engineers who use PySpark notebooks today and are new to declarative pipelines |
| **Data** | Contact-center call data arriving all day: call metadata, call transcripts, and AI-generated quality scores for each call (all synthetic, no PHI, obviously fake values) |
| **Problem** | Bad records slip through today, and nobody can say how many were rejected or why |
| **Journey** | Rules declared next to the pipeline code → a deliberate **warn / drop / fail** choice per rule → a **quarantine** table that keeps and explains bad rows → a file that breaks the contract **FAILS at the door** → investigate, decide, set the file aside, and **restart only what failed** → **monitor** quality over time on a dashboard and a SQL Alert |
| **Resolution** | Every rejected row is counted, attributed to a named rule, and kept in a quarantine table you can query. A broken file stops `calls_raw` loudly: nothing from the bad file reaches `calls_raw`, and the other bronze tables keep loading. Recovery uses stock Lakeflow features (Repair run / Refresh failed tables), with no full refresh and no lost history |
| **Value** | "We can't say what got rejected" becomes "~4% of calls were quarantined today. Here's the rule and the row for each." |

---

## Overview

Every minute a generator job drops a new JSON file (a few hundred rows) for each of `calls`, `transcripts` and `quality_scores` into a Unity Catalog Volume landing folder. About 7 days of backfill are already there, so no table starts empty. Two Lakeflow Declarative Pipelines run as **two tasks in one job**:

- **Ingestion** lands the files as bronze streaming tables with Auto Loader (`read_files`).
- **Transformation** builds the silver, quarantine and gold tables. A failed transform therefore never blocks new files from landing.

Where each check lives is a teaching point in itself:

- **File contract checks go at bronze.** `calls_raw` has exactly one **FAIL** expectation: the required columns (`call_start_ts`, `call_end_ts`, `duration_seconds`) are present. Every other bronze rule only warns.
- **Row quality checks go at silver.** Each rule is declared next to the code that reads the data, and each one deliberately picks an action:
  - **WARN**: keep the row, count the violation.
  - **DROP**: remove the row, count it, and keep it in quarantine.
  - **FAIL**: stop the update. Reserved for incidents, not for rows.

For `quality_scores`, the rules are not in the code at all. They live in a Delta table, `dq_rules`, so changing a rule's action is an `UPDATE`, not a code change. Rows that break a DROP rule are not lost: they land in `calls_quarantine` / `quality_scores_quarantine` with a `failed_rules` array and a load timestamp. The event logs of both pipelines are published to Unity Catalog. A one-page **AI/BI dashboard** and a **SQL Alert** turn them into quality monitoring over time.

Then we break it on purpose. A held-out calls file from a "vendor", **missing its required timestamp and duration columns**, is moved into the landing folder. The ingestion pipeline runs in triggered mode, so only `calls_raw` fails on the contract rule: nothing from the bad file reaches `calls_raw`, and `transcripts_raw` and `quality_scores_raw` keep loading in the same update. We investigate, decide the file was generated incorrectly, move it to a `rejected/` folder as evidence, and restart **only what failed**, using **"Repair run"** on the job or **"Refresh failed tables"** on the pipeline, **without a full refresh**.

The whole thing ships as a Declarative Automation Bundle (formerly Databricks Asset Bundles) with `dev` / `staging` / `prod` targets, plus a `demo_reset` job that puts the demo back to the same clean starting state every time.

**Duration:** ~15 minutes CORE (Acts 1, 2, 3, 4, 6); +~10 minutes EXTENDED (Acts 5, 7).

---

## Key Numbers

Seeded defect rates, so every count on screen is predictable:

| Dataset | Seeded defect | Rate | Rule action |
|---|---|---|---|
| calls | null `call_id` | ~1% (normal files) | **DROP** + quarantine (`calls_clean`, `call_id_not_null`) |
| calls | `duration_seconds <= 0` | ~2% | **DROP** + quarantine |
| calls | `call_end_ts < call_start_ts` | ~1% | **DROP** + quarantine |
| calls | `queue` outside Pharmacy / Claims / Benefits / Enrollment / Billing | ~2% | **WARN** (allowed values) |
| calls | `agent_id` not in `agents` | ~1.5% | **WARN** (referential, left join) |
| calls | duplicate `call_id` | ~1% | **WARN** at table level (`daily_call_checks` MV: dupes per day = 0) |
| calls | `call_start_ts`, `call_end_ts`, `duration_seconds` missing | **0% in normal data** (never null); **100% of the bad batch** | **FAIL** at bronze (`calls_raw`, `required_columns_present`) |
| transcripts | under 20 words | ~3% | **DROP** (`word_count < 20`) |
| transcripts | empty | ~1% | **DROP** (word_count 0 < 20) |
| transcripts | contains a fake phone number or email pattern | ~5% | **WARN** (keep + count) |
| quality_scores | a score outside 1–5 | ~2% | **DROP** (from `dq_rules`) |
| quality_scores | a null score | ~1% | **DROP** (from `dq_rules`) |
| quality_scores | `raw_model_output` not valid JSON | ~1% | **DROP** (parse check, from `dq_rules`) |

What to expect on screen:

| Metric | Expected value |
|---|---|
| Backfill on a clean start | ~7 days of history (hourly backfill files, a few hundred rows each) |
| Live arrival rate | 1 file per dataset per ~minute, a few hundred rows per file |
| `calls_clean` rows dropped | ~4% (null id + duration + timestamps) |
| `calls_quarantine` share of calls | ~4%, the same rows: **`calls_clean` + `calls_quarantine` = `calls_raw`** |
| Calls kept in `calls_clean` with a warning | ~5.5% (queue + agent; WARN rows are **not** quarantined) |
| `transcripts_clean` rows dropped | ~4% · PII warnings ~5% |
| `quality_scores_quarantine` share | ~4% (range + null + invalid JSON): **clean + quarantine = raw** |
| SQL Alert | quarantine share > 5% in the last hour (normally ~4%, so quiet), **or** any failed update (ERROR event) |
| Bad batch | one calls file, a few hundred rows, required columns absent → **ingestion update FAILS** |

---

## Demo Walkthrough

**Frame:** *"Your team ingests contact-center data all day. Today, if someone asks 'how many records did we reject yesterday, and why?', nobody can answer. Here's what good looks like."*

### Act 1 [CORE]: Data is landing (2 min)
- Open the Volume: `landing/calls/`, `landing/transcripts/`, `landing/quality_scores/`, with a new JSON file each minute. Next to them: `held_out/` (the bad batch, not loaded) and `rejected/` (empty for now). The generator is its own job with start/stop control.
- Open the **pipeline job**: two tasks, `ingestion` → `transformation`. *"A failed transform never blocks landing new files."*
- Open the ingestion pipeline graph: bronze streaming tables `calls_raw`, `transcripts_raw`, `quality_scores_raw`. Point at the **one** FAIL rule in bronze, `required_columns_present` on `calls_raw`; everything else in bronze is warn-only. *"File contract checks go at the door. Row quality checks go at silver."*
- Teaching point: the `pipelines.reset.allowed` table property on bronze comes from a bundle variable. It is `true` in dev, so the demo can be fully reset, and `false` in staging and prod, to protect history. *"Bronze is your only copy of what arrived. Don't let a full refresh wipe it in prod."*

> *"This is Auto Loader doing incremental, exactly-once ingestion. You didn't write a checkpoint, a schema-inference block or a trigger loop. And Genie Code can help you draft the pipeline SQL and the expectations, against your real Unity Catalog tables."*

### Act 2 [CORE]: Rules next to the code: warn and drop at silver (3 min)
- Open the transformation pipeline SQL for `calls_clean`, and show the `CONSTRAINT … EXPECT … ON VIOLATION` lines:
  - **DROP**: `call_id IS NOT NULL`, `duration_seconds > 0` and `call_end_ts >= call_start_ts`.
  - **WARN**: `queue IN (...)` and the `agent_id` lookup.
- Open `transcripts_clean`: **DROP** `word_count < 20` and **WARN** on the phone/email pattern (keep the row, count the violation).
- Click a table in the pipeline graph and open the **Data quality** tab: passed / failed counts per expectation, per update.
- *"The action is a design decision per rule. Bad durations would corrupt averages, so drop them. An unknown queue is worth knowing about but not worth losing the call, so warn. And a stray null `call_id`? That's a row problem, so drop and quarantine it. FAIL is for incidents."*

### Act 3 [CORE]: Bad rows are kept and explainable (2 min)
- Query `calls_quarantine`: every row that broke a **DROP** rule, including the null `call_id`s, with a `failed_rules` array (e.g. `["call_id_not_null"]`, `["valid_duration","valid_timestamps"]`) and a load timestamp.
- Explain the pattern: quarantine is computed as **NOT (all DROP rules)** and split from the **same source** as `calls_clean`. WARN rows stay in clean only. So `calls_clean` + `calls_quarantine` accounts for every row in `calls_raw`: show the three counts side by side.
- `quality_scores_quarantine` works the same way for the scores.

### Act 4 [CORE]: A broken file FAILS at the door, then recover (6 min), the core beat
1. **Move the bad file in.** Run the "move bad batch in" control, which moves the held-out vendor file from `held_out/calls/` into `landing/calls/`. The file has a few hundred rows but no `call_start_ts`, `call_end_ts` or `duration_seconds` columns.
   - Run the pipeline job. The ingestion pipeline runs in triggered mode: **only `calls_raw` fails**, on its FAIL expectation `required_columns_present`, so the **ingestion task FAILS**. The **transformation task doesn't run** (its upstream failed).
   - **Nothing from the bad file reaches `calls_raw`; the other bronze tables keep loading.** `transcripts_raw` and `quality_scores_raw` complete in the same update. Silver and gold are untouched. Show `calls_raw`'s row count unchanged, and the other two bronze counts going up.
2. **Investigate.**
   - In the pipeline UI, open the failed update: the error names the expectation (`required_columns_present`), the table (`calls_raw`) and an example of a violating row.
   - Show the same thing in the published event log: the `ERROR` event for that update, with the rule, the table and the offending record.
3. **Decide: is it a one-off, or an ongoing change upstream?**
   - *"This is the human step. Before you change any rule, check with the source owner or vendor. If the feed changed for good, you update the contract. If the file was wrong, you reject it."*
   - In the demo, we decide **the vendor generated this file incorrectly**.
4. **Remove the file, but keep it.** Move it from `landing/calls/` to `rejected/calls/` in the same Volume (Catalog Explorer, or `databricks fs cp` then `databricks fs rm`: the CLI has no `fs mv`). Don't delete it: it's the evidence for the vendor.
5. **Restart only what failed. Do NOT full refresh.** Either:
   - in the job run, click **"Repair run"**, which re-runs the failed `ingestion` task and then `transformation`; or
   - in the ingestion pipeline UI, click **"Refresh failed tables"**, then run transformation.
   - Good files that landed while ingestion was down load normally on the retry.
   - Show both pipelines going green, with history and checkpoints intact: row counts continue from where they were, and the Expectations tab history is still there.
6. **If the vendor resends a corrected file**, it lands under a new name and loads like any other file.
7. *"Recovery used stock features only: look at the event log, set the file aside, Repair run. Full refresh is a dev-only reset tool, which is exactly why bronze has `pipelines.reset.allowed = false` outside dev."*

> **Build note on the retry (confirmed at validation, 2026-11-03).** With `ignoreMissingFiles` OFF, the Repair run **errored**: the retried micro-batch still lists the moved file and fails with `CloudFileNotFoundException`. So `calls_raw` now sets the stock Auto Loader / `read_files` option **`ignoreMissingFiles => 'true'`** (only on `calls_raw`, redeployed with no full refresh). With it, both Repair run and Refresh failed tables recovered cleanly. It stays in the code, so `demo_reset` never changes code.
>
> **Build note on retries.** Serverless pipelines auto-retry a failed update (`RETRY_ON_FAILURE`), even in dev. A contract FAIL is deterministic, so both pipelines set `pipelines.numUpdateRetryAttempts = 0`: the job stops at `ingestion` right away instead of retrying the same bad file.

**Talk track: other FAIL-worthy incidents (not built).** Fail when publishing wrong data is worse than publishing late data:
- **Empty or truncated feed**: a file with 3 rows instead of ~300, or a day with zero calls. "No data" looks the same as "no calls" on a dashboard.
- **A whole batch replayed**: yesterday's files re-sent would double call volume in gold.
- **Something like a real SSN in a transcript**: for a health plan, letting it reach silver is worse than stopping.

**Talk track: new or unexpected columns (not built).** `calls_raw` reads with a declared schema, so a **missing** column comes through as nulls and trips the contract rule. For **unexpected** columns:
- Auto Loader's **rescued data column** (`_rescued_data`) captures fields that don't match the schema, so nothing is silently lost.
- **Schema evolution** (`schemaEvolutionMode`, e.g. `addNewColumns`) lets new columns be added on purpose: the stream stops on a new column and picks it up on restart.

### Act 5 [EXTENDED]: Rules as data (2 min)
- Query `dq_rules`: `rule_name`, `dataset`, `constraint_sql`, `action`, `owner`, `description`.
- `quality_scores_clean` loads its rules from this table at pipeline start and **groups them by action**:
  - `warn` rules → `@dp.expect_all`;
  - `drop` rules → `@dp.expect_all_or_drop`;
  - `fail` rules → `@dp.expect_all_or_fail`.
  
  `quality_scores_quarantine` reads the same `drop` group, so the split stays consistent.
- Live: `UPDATE dq_rules SET action = 'warn' WHERE rule_name = 'valid_compliance_score'`, then run the transformation. That rule moves from the drop group to the warn group:
  - the rows are now kept in `quality_scores_clean`, and the violations are still counted (Expectations tab shows it as warn);
  - they no longer go to quarantine.
  
  Revert it afterwards.
- *"The owner of a rule can change its severity with a governed `UPDATE`. No pull request against pipeline code."*

### Act 6 [CORE]: Monitor quality over time (2 min, short)
- Open the **Contact Center Data Quality dashboard** (one page, built on the published event logs + quarantine tables):
  1. Rows in / passed / dropped / quarantined: today and the trend.
  2. Failures by expectation: a bar per rule, colored by warn / drop / fail. The FAIL rule shows as **failed updates** (from `ERROR` events), because a failed update records no expectation metrics.
  3. Failure rate over time by rule, with the 5% alert threshold line.
  4. Quarantine explorer: sample rows with `failed_rules`.
- Show the saved queries that extract expectation passed/failed counts per rule, per table, per update from the event log.
- Show the **SQL Alert**. It fires when either:
  - a quarantine table's share of rows in the last hour is above 5%; or
  - a pipeline update failed (an `ERROR` event in the event log).
  
  During Act 4 it fires on the failed ingestion update. *"A FAIL doesn't write expectation metrics for that update, so you alert on the failure itself."*
- Mention: **Unity Catalog data quality monitoring** can add anomaly detection for freshness and completeness across the schema.

### Act 7 [EXTENDED]: Extended checks (~8 min, built in, shown only with time)
- **Multiple rules as a group:** `quality_scores_clean` is the one **Python** table. Its `drop` group from `dq_rules` uses `@dp.expect_all_or_drop` over the three score ranges (1–5) and the not-null checks.
- **Referential check:** `calls_clean` **warns** when `agent_id` has no match in the `agents` lookup (~200 rows), via a left join.
- **Parse check:** drop `quality_scores` rows whose `raw_model_output` is not valid JSON (`try_parse_json` / `from_json` returning null). *"Validate AI output like any other input."*
- **Table-level check:** the materialized view `daily_call_checks` expects **duplicate `call_id`s per day = 0** and **the day has rows**. The seeded ~1% duplicates show up as a daily warn.
- **Volume check with standard deviations:** the materialized view `daily_volume_checks` counts calls per complete day and compares each day with the prior 7 days (**within 3 standard deviations**) and with the prior day (**within 2%**). Both are WARN. The 2% rule is there on purpose to show how a fixed band fires on normal swings while the standard-deviation band adapts to the data. For week over week, group by `date_trunc('WEEK', ...)` instead of by day.

### Closing
> *"Contract checks at the door, row checks at silver. A deliberate action per rule. Every dropped row kept and explained. A broken file that stops loudly and recovers with the tools you already have, no full refresh. A dashboard and an alert that tell you before your consumers do. That's what good looks like, and it's the same pattern on your next project."*

---

## Runbook (presenter + engineer)

All names carry the project suffix `_1e53d4`. Catalog and schema are bundle variables; pass your own (see `dab_instructions.md`).

**Volume layout:**

| Folder | Purpose |
|---|---|
| `landing/calls/`, `landing/transcripts/`, `landing/quality_scores/` | Auto Loader reads these |
| `held_out/calls/` | the bad batch; **never** read by the pipeline |
| `rejected/calls/` | files set aside after an incident, kept as evidence |

### Setup & deploy
The bundle is the single `databricks.yml` at the project root, with its sources in `src/`. The exact commands are in `dab_instructions.md`.
1. Install the Databricks CLI and authenticate to the target workspace (no URLs, identities or resource IDs are hard-coded in the bundle). Create the schema once (`databricks schemas create …`).
2. `databricks bundle validate -t dev`
3. `databricks bundle deploy -t dev`. This creates the Volume, both pipelines, the pipeline job, the generator job, `demo_reset`, the dashboard and the alert.
4. `databricks bundle run demo_reset -t dev`. This writes the 7-day backfill plus the `dq_rules` and `agents` tables, creates the monitoring views, and produces a clean starting state.

Targets: `dev` (development mode, `pipelines.reset.allowed = true` on bronze), `staging` and `prod` (`reset.allowed = false`). In `prod`, `run_as` is a service-principal variable and permissions go to a group variable.

### Start / stop the generator
- Start: `databricks bundle run generator -t dev` (or "Run now" on the generator job). It writes a new file per dataset about every minute.
- Stop: cancel the generator job run. Files already landed stay put.

### Trigger the bad batch
1. Run the "move bad batch in" control. It moves the held-out vendor file (a few hundred calls rows, missing `call_start_ts`, `call_end_ts`, `duration_seconds`) from `held_out/calls/` into `landing/calls/`. It is **not** loaded by default.
2. Run the pipeline job (or wait for the next scheduled run).
3. Expected:
   - the `ingestion` task FAILS: only `calls_raw` fails, on `required_columns_present`;
   - nothing from the bad file reaches `calls_raw`; `transcripts_raw` and `quality_scores_raw` keep loading;
   - `transformation` doesn't run;
   - the SQL Alert fires on the `ERROR` event.

### Recover (stock features only, no full refresh)
1. **Investigate:** open the failed update in the pipeline UI (rule, table, example row), and find the same `ERROR` event in the published event log.
2. **Decide:** one-off or ongoing change? Check with the source owner or vendor. In the demo: the file was generated incorrectly.
3. **Set the file aside:** move it out of landing, keeping it as evidence (don't delete it):
   ```bash
   # the CLI has no `fs mv`: copy, then remove (or use Catalog Explorer)
   databricks fs cp \
     dbfs:/Volumes/<catalog>/<schema>/<volume>/landing/calls/<bad_file>.json \
     dbfs:/Volumes/<catalog>/<schema>/<volume>/rejected/calls/<bad_file>.json
   databricks fs rm dbfs:/Volumes/<catalog>/<schema>/<volume>/landing/calls/<bad_file>.json
   ```
4. **Restart only what failed:**
   - **"Repair run"** on the pipeline job (re-runs `ingestion`, then `transformation`); **or**
   - **"Refresh failed tables"** on the ingestion pipeline, then run transformation.
5. **Confirm:**
   - both pipelines are green;
   - good files that landed during the outage are in `calls_raw`;
   - counts continue from where they were (no history lost, no full refresh).
6. **Corrected resend:** if the vendor sends a fixed file, it lands under a new name in `landing/calls/` and loads normally.

### Change a rule's action
```sql
UPDATE dq_rules SET action = 'warn'  -- warn | drop | fail
WHERE rule_name = 'valid_compliance_score';
```
Run the transformation pipeline. `quality_scores_clean` regroups the rules by action at the start of the update, so the new action applies on that update. Revert it the same way.

### Reset to a clean state before a rehearsal
`databricks bundle run demo_reset -t dev`. It is safe to run repeatedly, finishes in a few minutes, and **never changes pipeline code**. It:
1. stops the generator job;
2. moves the bad batch file back to `held_out/calls/`, from wherever it is (`landing/` or `rejected/`), and empties `rejected/`;
3. restores the landing folder to the starting backfill and `dq_rules` to its seeded actions;
4. full-refreshes both pipelines (allowed in dev only);
5. recreates the two monitoring views, runs the pipeline job once and restarts the generator;
6. prints row counts per table and expectation results, to confirm the clean starting state.

### Teardown
`databricks bundle destroy -t dev` removes the jobs, pipelines, dashboard and alert. Drop the schema separately if needed.

## Products Showcased

"Build" = a resource we provision. "Talk track" = a capability we narrate but don't build separately.

| Product | Mode | What it does in this demo |
|---|---|---|
| **Synthetic data generation** | Build | Generator job + reset notebook that land fake calls, transcripts, quality scores and the agents lookup as JSON in a UC Volume, with seeded defect rates and a held-out bad batch (missing required columns) |
| **Lakeflow Spark Declarative Pipelines** | Build | Two SQL pipelines (one Python table): bronze with one contract FAIL and otherwise warn-only rules; silver with warn / drop; quarantine with `failed_rules`; gold `call_quality_daily` + `dq_daily`; plus extended grouped, referential, parse and table-level checks |
| **Lakeflow Jobs** | Build | One job running ingestion → transformation as two tasks ("Repair run" re-runs only what failed), the generator job, and the `demo_reset` job |
| **Data quality** (expectations, event log, SQL Alert) | Build | Expectation metrics per rule/table/update, published to UC event log tables with saved queries; SQL Alert on quarantine share > 5% per hour and on failed updates. UC data quality monitoring is mentioned only |
| **AI/BI Dashboard** | Build | One page on the event logs + quarantine tables: rows in/passed/dropped/quarantined, failures by rule colored by action, failure rate vs. threshold, quarantine explorer |
| **Lakeflow Connect** | Talk track | Auto Loader (`read_files`) from the Volume landing folder into bronze: incremental, exactly-once, with a declared schema, rescued data column and schema evolution |
| **Genie Code** | Talk track | Can help draft the pipeline SQL and expectations against the real UC tables |
| **Unity Catalog** | Talk track | Governs the Volume, every table (including `dq_rules`, which owners update), the published event logs, and per-target catalog/schema + group permissions |
