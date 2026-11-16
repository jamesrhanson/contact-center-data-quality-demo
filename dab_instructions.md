# How to load the Contact Center Data Quality demo

This is a Declarative Automation Bundle (DAB). One deploy creates everything from the session: the Volume, the ingestion and transformation pipelines, four jobs, the dashboard and the alert. All data is synthetic (no PHI).

## What you need

- The Databricks CLI, version 0.283.0 or newer (`databricks --version`).
- A CLI profile for the workspace you want to use (`databricks auth login --host <workspace-url>`).
- A catalog you can create a schema in, and a SQL warehouse ID for the dashboard and alert.
- Serverless compute for pipelines and jobs enabled in that workspace.

## Deploy (dev)

Run these from this folder (where `databricks.yml` is). Replace the values in angle brackets.

```bash
# 1. Create the schema once (the bundle doesn't create it)
databricks schemas create <schema> <catalog>

# 2. Check and deploy
databricks bundle validate -t dev --var catalog=<catalog> --var schema=<schema> --var warehouse_id=<warehouse-id>
databricks bundle deploy   -t dev --var catalog=<catalog> --var schema=<schema> --var warehouse_id=<warehouse-id>

# 3. Load the starting data (7-day backfill, agents lookup, dq_rules), refresh both pipelines,
#    build the monitoring views and start the file generator. Takes about 8 minutes.
databricks bundle run demo_reset -t dev --var catalog=<catalog> --var schema=<schema> --var warehouse_id=<warehouse-id>
```

Tip: to avoid typing the `--var` flags every time, add `catalog`, `schema` and `warehouse_id` under `targets: dev: variables:` in `databricks.yml`.

In dev mode, jobs, pipelines and the dashboard get a `[dev <your user>]` prefix. Tables and the Volume keep their names.

## The four jobs

| Job | What it does |
|---|---|
| `cc_dq_generator_1e53d4` | Writes a new JSON file per dataset into `landing/` about every minute. It only writes files. Stops itself after 2 hours. |
| `cc_dq_pipeline_job_1e53d4` | Runs the ingestion pipeline, then the transformation pipeline. This is what loads the files. Its schedule is paused, so run it when you want fresh data. |
| `cc_dq_bad_batch_in_1e53d4` | Moves the broken calls file (missing required columns) from `held_out/calls/` into `landing/calls/`, adding a timestamp to its name. |
| `cc_dq_demo_reset_1e53d4` | Puts everything back to the starting state. Dev only; it refuses to run in staging or prod. |

## Try the failure and recovery

```bash
databricks bundle run bad_batch_in -t dev --var catalog=<catalog> --var schema=<schema> --var warehouse_id=<warehouse-id>
databricks bundle run pipeline_job -t dev --var catalog=<catalog> --var schema=<schema> --var warehouse_id=<warehouse-id>
# Ingestion fails on the required_columns_present rule; transformation doesn't run.
```

Then set the file aside. Use Catalog Explorer to move `calls_vendor_bad_batch_<timestamp>.json` from `landing/calls/` to `rejected/calls/`, or with the CLI (there's no `fs mv`):

```bash
databricks fs cp dbfs:/Volumes/<catalog>/<schema>/cc_files_1e53d4/landing/calls/calls_vendor_bad_batch_<timestamp>.json \
                 dbfs:/Volumes/<catalog>/<schema>/cc_files_1e53d4/rejected/calls/calls_vendor_bad_batch_<timestamp>.json
databricks fs rm dbfs:/Volumes/<catalog>/<schema>/cc_files_1e53d4/landing/calls/calls_vendor_bad_batch_<timestamp>.json
```

Finally, open the failed job run and click **Repair run**. No full refresh needed.

To start over at any point: `databricks bundle run demo_reset -t dev ...` (same `--var` flags).

## Staging and prod

The bundle also has `staging` and `prod` targets. In both, full refresh is blocked on bronze (`pipelines.reset.allowed = false`). Prod runs as a service principal and grants a group access:

```bash
databricks bundle deploy -t prod --var catalog=<catalog> --var schema=<schema> --var warehouse_id=<warehouse-id> \
  --var run_as_service_principal=<sp-application-id> --var permissions_group=<group>
```

## Turning the alert on (and back off)

The SQL alert `cc_dq_alert_1e53d4` ships **paused**. When it's on, it runs every 5 minutes, which keeps your SQL warehouse awake and costs money even when nobody is using the demo.

- **Turn it on:** redeploy with `--var alert_pause_status=UNPAUSED` (same `bundle deploy` command as above, plus that flag). You can also open the alert in the workspace (**Alerts** in the left menu) and unpause its schedule there.
- **Turn it back off when you're done:** redeploy without that flag (it goes back to `PAUSED`), or pause the schedule on the alert in the UI. Please don't forget this step, or the warehouse keeps waking up every 5 minutes.
- **Heads-up:** any `bundle deploy` resets the alert to whatever `alert_pause_status` says, so a plain redeploy pauses it again.
- To check it less often, set `--var alert_cron="<quartz cron>"` (for example `0 0 * * * ?` for hourly).

Nothing else in the demo runs on its own: the pipeline job's schedule is paused, the generator (started by `demo_reset`) stops itself after 2 hours, and the pipelines are triggered (not continuous).

## Saved queries

The analysis queries behind the dashboard are in `src/sql/queries/` (`{fq}` means `<catalog>.<schema>`). Open them in the SQL editor, or create them with `databricks queries create`.

## Remove everything

`databricks bundle destroy -t dev --var catalog=<catalog> --var schema=<schema> --var warehouse_id=<warehouse-id>`. This removes the jobs, pipelines, dashboard and alert. It keeps the schema, its tables and the event logs; drop the schema yourself if you want those gone.

## Where to start reading

- `README.md`: the story, the rules, and a walkthrough of each part of the demo.
- `src/pipelines/ingestion/bronze.sql`: bronze tables and the one FAIL rule.
- `src/pipelines/transformation/silver_calls_transcripts.sql`: warn and drop rules, the quarantine table, the lookup check and the table-level check.
- `src/pipelines/transformation/quality_scores.py`: rules loaded from the `dq_rules` table.
