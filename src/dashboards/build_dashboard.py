"""Builds contact_center_dq.lvdash.json (bare table names; catalog/schema bound at deploy)."""
import json
Q = json.load(open("queries.json"))
WARN, DROP, FAIL, PASSED, QUAR = "#F2A900", "#E8590C", "#9B1C1C", "#2F9E44", "#7048E8"

def f(name, expr): return {"name": name, "expression": expr}
def q(ds, fields, disagg=False): return [{"name": "main_query", "query": {"datasetName": ds, "fields": fields, "disaggregated": disagg}}]
def frame(title, desc=None):
    fr = {"showTitle": True, "title": title}
    if desc: fr.update(description=desc, showDescription=True)
    return fr
def text(name, lines, x, y, w, h): return {"widget": {"name": name, "multilineTextboxSpec": {"lines": lines}}, "position": {"x": x, "y": y, "width": w, "height": h}}
def pos(x, y, w, h): return {"x": x, "y": y, "width": w, "height": h}

datasets = [{"name": k, "displayName": k, "queryLines": [v]} for k, v in Q.items()]
datasets[0]["columns"] = [
    {"displayName": "Passed share", "expression": "SUM(`rows_passed`) / SUM(`rows_in`)"},
    {"displayName": "Dropped share", "expression": "SUM(`rows_dropped`) / SUM(`rows_in`)"},
    {"displayName": "Quarantined share", "expression": "SUM(`rows_quarantined`) / SUM(CASE WHEN `dataset` IN ('calls', 'quality_scores') THEN `rows_in` END)"},
]
PCT = {"type": "number-percent", "decimalPlaces": {"type": "max", "places": 1}}
INT = {"type": "number", "decimalPlaces": {"type": "max", "places": 0}}

def counter(name, title, desc, field, expr, fmt):
    return {"widget": {"name": name,
        "queries": q("ds_daily", [f(field, expr), f("daily(event_date)", 'DATE_TRUNC("DAY", `event_date`)')]),
        "spec": {"version": 2, "widgetType": "counter",
                 "encodings": {"value": {"fieldName": field, "displayName": title, "format": fmt},
                               "period": {"fieldName": "daily(event_date)"}},
                 "frame": frame(title, desc)}}}

layout = [
    text("header", ["## Contact center data quality: what passed, what was stopped, and why\n", "\n",
         "Rules live next to the pipeline code; each one warns, drops (to a quarantine table) or fails the update. "
         "About 4% of calls and AI QA scores are quarantined. A broken vendor file stops the load and shows up under **Recent failed updates**."], 0, 0, 12, 2),
]
c = [counter("kpi-rows-in", "Rows in", "Rows that arrived in bronze, selected days and datasets", "sum(rows_in)", "SUM(`rows_in`)", INT),
     counter("kpi-passed", "Passed", "Share of rows that passed every drop rule", "measure(Passed share)", "MEASURE(`Passed share`)", PCT),
     counter("kpi-dropped", "Dropped", "Share of rows kept out of silver by a drop rule", "measure(Dropped share)", "MEASURE(`Dropped share`)", PCT),
     counter("kpi-quarantined", "Quarantined", "Share of calls + quality_scores rows kept, with reasons, in the quarantine tables", "measure(Quarantined share)", "MEASURE(`Quarantined share`)", PCT)]
for i, w in enumerate(c):
    w["position"] = pos(3 * i, 2, 3, 3); layout.append(w)

layout.append({"widget": {"name": "daily-stages",
    "queries": q("ds_daily_stages", [f("event_date", "`event_date`"), f("sum(rows)", "SUM(`rows`)"), f("stage", "`stage`")]),
    "spec": {"version": 3, "widgetType": "bar", "encodings": {
        "x": {"fieldName": "event_date", "scale": {"type": "temporal"}, "displayName": "Day"},
        "y": {"fieldName": "sum(rows)", "scale": {"type": "quantitative"}, "displayName": "Rows"},
        "color": {"fieldName": "stage", "displayName": "Outcome", "scale": {"type": "categorical",
                  "mappings": [{"value": "passed", "color": PASSED}, {"value": "dropped", "color": DROP}]}}},
        "frame": frame("Rows per day: passed vs dropped", "Dropped rows go to quarantine (calls, quality_scores) or are dropped (short transcripts)")}},
    "position": pos(0, 5, 6, 5)})

layout.append({"widget": {"name": "recent-failed-updates",
    "queries": q("ds_failures", [f(n, f"`{n}`") for n in ["event_ts", "pipeline", "expectation", "table_name", "example_record"]], True),
    "spec": {"version": 2, "widgetType": "table", "encodings": {"columns": [
        {"fieldName": "event_ts", "displayName": "When"}, {"fieldName": "pipeline", "displayName": "Pipeline"},
        {"fieldName": "expectation", "displayName": "Rule"}, {"fieldName": "table_name", "displayName": "Table"},
        {"fieldName": "example_record", "displayName": "Example record"}]},
        "frame": frame("Recent failed updates", "From the pipeline event logs. Empty until a FAIL rule stops an update (Act 4).")}},
    "position": pos(6, 5, 6, 5)})

layout.append({"widget": {"name": "failures-by-rule",
    "queries": q("ds_rule_failures", [f("sum(failures)", "SUM(`failures`)"), f("rule_name", "`rule_name`"), f("action", "`action`")]),
    "spec": {"version": 3, "widgetType": "bar", "encodings": {
        "x": {"fieldName": "sum(failures)", "scale": {"type": "quantitative"}, "displayName": "Failing rows (failed updates for FAIL)"},
        "y": {"fieldName": "rule_name", "scale": {"type": "categorical", "sort": {"by": "value"}}, "displayName": "Rule"},
        "color": {"fieldName": "action", "displayName": "Action", "scale": {"type": "categorical",
                  "mappings": [{"value": "warn", "color": WARN}, {"value": "drop", "color": DROP}, {"value": "fail", "color": FAIL}]}}},
        "frame": frame("Failures by rule, colored by action", "Warn keeps the row, drop quarantines it, fail stops the update. FAIL rules count failed updates.")}},
    "position": pos(0, 10, 6, 7)})

layout.append({"widget": {"name": "failure-rate-hourly",
    "queries": q("ds_rate_hourly", [f("hourly(hour)", 'DATE_TRUNC("HOUR", `hour`)'), f("max(failure_rate)", "MAX(`failure_rate`)"), f("rule", "`rule`")]),
    "spec": {"version": 3, "widgetType": "line", "encodings": {
        "x": {"fieldName": "hourly(hour)", "scale": {"type": "temporal"}, "displayName": "Hour"},
        "y": {"fieldName": "max(failure_rate)", "scale": {"type": "quantitative"}, "displayName": "Failure rate", "format": PCT},
        "color": {"fieldName": "rule", "scale": {"type": "categorical"}, "displayName": "Table.rule"}},
        "annotations": [{"type": "horizontal-line", "encodings": {"y": {"dataValue": "0.05", "dataType": "DOUBLE"},
                         "label": {"value": "5% alert threshold"}, "color": {"value": FAIL}}}],
        "frame": frame("Failure rate per rule per hour (top 8)", "From the expectation metrics in the event logs. The SQL alert fires above 5%.")}},
    "position": pos(6, 10, 6, 7)})

layout.append(text("quarantine-heading", ["## Quarantine explorer\n", "\n", "Every quarantined row with the rules it broke and the file it came from. Pick a rule to drill in."], 0, 17, 8, 2))
layout.append({"widget": {"name": "filter-rule",
    "queries": [{"name": "q_rule", "query": {"datasetName": "ds_quarantine", "fields": [f("rule_name", "`rule_name`")], "disaggregated": False}}],
    "spec": {"version": 2, "widgetType": "filter-multi-select",
             "encodings": {"fields": [{"fieldName": "rule_name", "displayName": "Rule", "queryName": "q_rule"}]},
             "frame": frame("Rule")}},
    "position": pos(8, 17, 4, 2)})
qcols = ["_loaded_at", "dataset", "call_id", "rule_name", "key_columns", "source_file"]
layout.append({"widget": {"name": "quarantine-table",
    "queries": q("ds_quarantine", [f(n, f"`{n}`") for n in qcols], True),
    "spec": {"version": 2, "widgetType": "table", "encodings": {"columns": [
        {"fieldName": n, "displayName": d} for n, d in zip(qcols, ["Loaded at", "Dataset", "call_id", "Failed rule", "Key columns", "Source file"])]},
        "frame": frame("Quarantined rows, newest first", "calls_quarantine + quality_scores_quarantine (latest 2,000 rows)")}},
    "position": pos(0, 19, 12, 8)})

DS = list(Q)
def gfilter(name, wtype, col, title, x, default=None):
    spec = {"version": 2, "widgetType": wtype,
            "encodings": {"fields": [{"fieldName": col, "displayName": title, "queryName": f"q_{col}_{d}"} for d in DS]},
            "frame": frame(title)}
    if default: spec["selection"] = default
    return {"widget": {"name": name,
        "queries": [{"name": f"q_{col}_{d}", "query": {"datasetName": d, "fields": [f(col, f"`{col}`")], "disaggregated": False}} for d in DS],
        "spec": spec}, "position": pos(x, 0, 2, 2)}

filters = [gfilter("filter-date", "filter-date-range-picker", "event_date", "Date range", 0,
                   {"defaultSelection": {"range": {"dataType": "DATE", "min": {"value": "now-7d/d"}, "max": {"value": "now/d"}}}}),
           gfilter("filter-dataset", "filter-multi-select", "dataset", "Dataset", 2)]

dash = {"datasets": datasets,
        "pages": [{"name": "overview", "displayName": "Data quality", "pageType": "PAGE_TYPE_CANVAS", "layoutVersion": "GRID_V1", "layout": layout},
                  {"name": "filters", "displayName": "Filters", "pageType": "PAGE_TYPE_GLOBAL_FILTERS", "layoutVersion": "GRID_V1", "layout": filters}],
        "uiSettings": {"theme": {
            "canvasBackgroundColor": {"light": "#F5F8FB", "dark": "#1F272D"},
            "widgetBackgroundColor": {"light": "#FFFFFF", "dark": "#11171C"},
            "widgetBorderColor": {"light": "#FFFFFF", "dark": "#11171C"},
            "fontColor": {"light": "#11171C", "dark": "#E8ECF0"},
            "selectionColor": {"light": "#2272B4", "dark": "#8ACAFF"},
            "visualizationColors": ["#1D5C96", "#F2A900", "#E8590C", "#7048E8", "#2F9E44", "#0C8599", "#C2255C", "#9B1C1C"],
            "widgetHeaderAlignment": "LEFT"}}}
json.dump(dash, open("contact_center_dq.lvdash.json", "w"), indent=1)
print("ok", len(layout), "widgets")
