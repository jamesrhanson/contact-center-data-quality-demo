"""Rules as data: quality_scores rules live in the governed dq_rules table.

The pipeline reads dq_rules when it starts and GROUPS THE RULES BY ACTION:
  warn -> expect_all, drop -> expect_all_or_drop, fail -> expect_all_or_fail.
Change a rule's action with an UPDATE on dq_rules; the next update picks it up. No code change.
The quarantine table uses the SAME drop group, so clean + quarantine = raw.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog")
SCHEMA = spark.conf.get("schema")
FQ = f"{CATALOG}.{SCHEMA}"


def load_rules(dataset):
    rows = (spark.read.table(f"{FQ}.dq_rules")
            .where(F.col("dataset") == dataset)
            .select("rule_name", "constraint_sql", "action").collect())
    groups = {"warn": {}, "drop": {}, "fail": {}}
    for r in rows:
        groups.setdefault(r.action.lower(), {})[r.rule_name] = r.constraint_sql
    return groups


RULES = load_rules("quality_scores")


def with_expectations(fn):
    # Empty groups are skipped.
    if RULES["warn"]:
        fn = dp.expect_all(RULES["warn"])(fn)
    if RULES["drop"]:
        fn = dp.expect_all_or_drop(RULES["drop"])(fn)
    if RULES["fail"]:
        fn = dp.expect_all_or_fail(RULES["fail"])(fn)
    return fn


@dp.table(name="quality_scores_clean",
          comment="AI QA scores validated by rules read from dq_rules (grouped by action).")
@with_expectations
def quality_scores_clean():
    return (spark.readStream.table(f"{FQ}.quality_scores_raw")
            .drop("_rescued_data"))


def _all_drop_rules_pass():
    if not RULES["drop"]:
        return F.lit(True)
    return F.expr(" AND ".join(f"({c})" for c in RULES["drop"].values()))


@dp.table(name="quality_scores_quarantine",
          comment="quality_scores rows that broke at least one DROP rule from dq_rules, with the reasons.")
def quality_scores_quarantine():
    failed = F.filter(
        F.array(*[F.when(~F.coalesce(F.expr(c), F.lit(False)), F.lit(n)) for n, c in RULES["drop"].items()]
                or [F.lit(None).cast("string")]),
        lambda x: x.isNotNull())
    return (spark.readStream.table(f"{FQ}.quality_scores_raw")
            .where(~F.coalesce(_all_drop_rules_pass(), F.lit(False)))
            .drop("_rescued_data")
            .withColumn("failed_rules", failed)
            .withColumn("_loaded_at", F.current_timestamp()))


# ---------------------------------------------------------------------------------------
# dq_daily (gold): per day x dataset x rule, plus a dataset-total row (rule_name='__all__').
# Per-rule failures are computed by evaluating each rule's predicate over the bronze rows,
# so WARN rules are counted too. Actions for quality_scores rules come from dq_rules.
# ---------------------------------------------------------------------------------------
CALL_RULES = {
    "call_id_not_null": ("call_id IS NOT NULL", "drop"),
    "valid_duration": ("duration_seconds > 0", "drop"),
    "valid_timestamps": ("call_end_ts >= call_start_ts", "drop"),
    "valid_queue": ("queue IN ('Pharmacy','Claims','Benefits','Enrollment','Billing')", "warn"),
    "known_agent": ("agent_matched", "warn"),
}
TRANSCRIPT_RULES = {
    "min_word_count": ("word_count >= 20", "drop"),
    "no_pii_pattern": ("NOT (transcript_text RLIKE '555-01[0-9]{2}' OR "
                       "transcript_text RLIKE '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\\\.[A-Za-z]{2,}')", "warn"),
}


def _rule_stats(df, dataset, rules, has_quarantine):
    drop_preds = [c for c, a in rules.values() if a == "drop"]
    all_drop = " AND ".join(f"coalesce(({c}), false)" for c in drop_preds) or "true"
    aggs = [F.count(F.lit(1)).alias("rows_in"),
            F.sum(F.expr(f"CASE WHEN {all_drop} THEN 1 ELSE 0 END")).alias("rows_clean")]
    for n, (c, _) in rules.items():
        aggs.append(F.sum(F.expr(f"CASE WHEN coalesce(({c}), false) THEN 0 ELSE 1 END")).alias(f"f__{n}"))
    g = df.groupBy("event_date").agg(*aggs)
    total = g.select(
        "event_date", F.lit(dataset).alias("dataset"), F.lit("__all__").alias("rule_name"),
        F.lit(None).cast("string").alias("action"), "rows_in",
        F.col("rows_clean").alias("rows_passed"), (F.col("rows_in") - F.col("rows_clean")).alias("rows_failed"),
        "rows_clean", (F.col("rows_in") - F.col("rows_clean")).alias("rows_dropped"),
        (F.col("rows_in") - F.col("rows_clean") if has_quarantine else F.lit(0).cast("bigint")).alias("rows_quarantined"))
    per_rule = [g.select(
        "event_date", F.lit(dataset).alias("dataset"), F.lit(n).alias("rule_name"), F.lit(a).alias("action"),
        "rows_in", (F.col("rows_in") - F.col(f"f__{n}")).alias("rows_passed"), F.col(f"f__{n}").alias("rows_failed"),
        F.lit(None).cast("bigint").alias("rows_clean"), F.lit(None).cast("bigint").alias("rows_dropped"),
        F.lit(None).cast("bigint").alias("rows_quarantined")) for n, (c, a) in rules.items()]
    out = total
    for p in per_rule:
        out = out.unionByName(p)
    return out


@dp.materialized_view(name="dq_daily",
                      comment="Daily data quality per dataset and rule; rule_name='__all__' holds dataset totals.")
def dq_daily():
    calls_raw = spark.read.table(f"{FQ}.calls_raw")
    agents = spark.read.table(f"{FQ}.agents").select("agent_id").distinct().withColumn("_m", F.lit(True))
    calls = (calls_raw.join(agents, "agent_id", "left")
             .withColumn("agent_matched", F.coalesce(F.col("_m"), F.lit(False)))
             .withColumn("event_date", F.to_date(F.coalesce("call_start_ts", "_ingested_at"))))
    call_dates = (calls_raw.where("call_id IS NOT NULL").groupBy("call_id")
                  .agg(F.min("call_start_ts").alias("_start")))
    transcripts = (spark.read.table(f"{FQ}.transcripts_raw").join(call_dates, "call_id", "left")
                   .withColumn("event_date", F.to_date(F.coalesce("_start", "_ingested_at"))))
    scores = (spark.read.table(f"{FQ}.quality_scores_raw")
              .withColumn("event_date", F.to_date(F.coalesce("scored_ts", "_ingested_at"))))
    score_rules = {n: (c, a) for a, grp in RULES.items() for n, c in grp.items()}
    return (_rule_stats(calls, "calls", CALL_RULES, True)
            .unionByName(_rule_stats(transcripts, "transcripts", TRANSCRIPT_RULES, False))
            .unionByName(_rule_stats(scores, "quality_scores", score_rules, True)))
