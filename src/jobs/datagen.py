"""Synthetic contact-center data for the data quality demo.

All values are obviously fake (MBR-FAKE-*, AGT-*, 555-01NN, @example.com). No PHI.
Writes newline-delimited JSON files into the volume layout:
  landing/{calls,transcripts,quality_scores,agents}/, held_out/calls/, rejected/calls/

Defects are mutually exclusive per row (each defective row carries exactly one
defect) so per-rule counts and quarantine shares are predictable.
"""
import json
import math
import os
import random
import uuid
from datetime import datetime, timedelta, timezone

QUEUES = ["Pharmacy", "Claims", "Benefits", "Enrollment", "Billing"]
BAD_QUEUES = ["Dental", "Vision", "misc"]
TEAMS = ["Aurora", "Beacon", "Cascade", "Delta", "Evergreen", "Falcon", "Granite"]
SITES = ["Site North (fake)", "Site South (fake)", "Site East (fake)", "Site West (fake)"]
N_AGENTS = 200

AGENT_LINES = [
    "Thank you for calling, how can I help you today?",
    "I can certainly look into that for you.",
    "Let me pull up the details on your plan.",
    "I see the claim was received last week.",
    "Your prescription is covered under the preferred tier.",
    "I have updated the mailing address on file.",
    "Is there anything else I can help you with today?",
    "I will transfer you to the right team now.",
    "The deductible for this year has been met.",
    "I understand this has been frustrating for you.",
]
MEMBER_LINES = [
    "I have a question about my recent bill.",
    "My pharmacy said the refill was not approved.",
    "I want to check the status of my claim.",
    "Can you explain what my plan covers for this visit?",
    "I moved recently and need to update my address.",
    "Thanks, that answers my question.",
    "I was charged twice for the same visit.",
    "How long will the review take?",
    "I need a new member card sent to me.",
    "That sounds good, thank you for the help.",
]


def _ts(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _write(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    os.replace(tmp, path)


def agents_rows(rng):
    rows = []
    for i in range(1, N_AGENTS + 1):
        hire = datetime(2015, 1, 1) + timedelta(days=rng.randint(0, 3500))
        rows.append({
            "agent_id": f"AGT-{i:04d}",
            "team": rng.choice(TEAMS),
            "site": rng.choice(SITES),
            "hire_date": hire.strftime("%Y-%m-%d"),
            "active": rng.random() < 0.9,
        })
    return rows


def _transcript(rng, n_sentences):
    lines = []
    for i in range(n_sentences):
        pool = AGENT_LINES if i % 2 == 0 else MEMBER_LINES
        lines.append(("Agent: " if i % 2 == 0 else "Member: ") + rng.choice(pool))
    return " ".join(lines)


def _pick(rng, table):
    """table = [(name, prob), ...]; returns a defect name or None."""
    x = rng.random()
    acc = 0.0
    for name, p in table:
        acc += p
        if x < acc:
            return name
    return None


CALL_DEFECTS = [("null_id", 0.01), ("bad_duration", 0.02), ("bad_ts", 0.01),
                ("bad_queue", 0.02), ("unknown_agent", 0.015), ("duplicate", 0.01)]
TRANSCRIPT_DEFECTS = [("short", 0.03), ("empty", 0.01), ("pii", 0.05)]
SCORE_DEFECTS = [("range", 0.02), ("null", 0.01), ("bad_json", 0.01)]


def batch(rng, window_start, window_end, n_calls, drop_contract=False):
    """Return (calls, transcripts, scores) for calls starting in [window_start, window_end)."""
    calls, transcripts, scores = [], [], []
    span = max(1.0, (window_end - window_start).total_seconds())
    for _ in range(n_calls):
        real_id = "CALL-" + uuid.UUID(int=rng.getrandbits(128)).hex[:16]
        start = window_start + timedelta(seconds=rng.random() * span)
        dur = int(min(1800, max(60, math.exp(rng.gauss(math.log(360), 0.6)))))
        end = start + timedelta(seconds=dur)
        call = {
            "call_id": real_id,
            "agent_id": f"AGT-{rng.randint(1, N_AGENTS):04d}",
            "member_id": f"MBR-FAKE-{rng.randint(0, 999999):06d}",
            "queue": rng.choice(QUEUES),
            "channel": "Phone" if rng.random() < 0.7 else "Chat",
            "language": "en" if rng.random() < 0.8 else "es",
            "call_start_ts": _ts(start),
            "call_end_ts": _ts(end),
            "duration_seconds": dur,
            "transferred": rng.random() < 0.12,
        }
        d = None if drop_contract else _pick(rng, CALL_DEFECTS)
        if d == "null_id":
            call["call_id"] = None
        elif d == "bad_duration":
            call["duration_seconds"] = rng.choice([0, -dur, -5])
        elif d == "bad_ts":
            call["call_end_ts"] = _ts(start - timedelta(seconds=dur))
        elif d == "bad_queue":
            call["queue"] = rng.choice(BAD_QUEUES)
        elif d == "unknown_agent":
            call["agent_id"] = f"AGT-9{rng.randint(0, 999):03d}"
        if drop_contract:
            # "Vendor generated it wrong": the contract keys are absent from every record.
            for k in ("call_start_ts", "call_end_ts", "duration_seconds"):
                call.pop(k)
        calls.append(call)
        if d == "duplicate" and calls:
            calls.append(dict(calls[rng.randrange(len(calls))]))  # re-emit a same-day call

        # transcripts: one row per call (carries the real id)
        td = _pick(rng, TRANSCRIPT_DEFECTS)
        if td == "empty":
            text = ""
        elif td == "short":
            text = " ".join(_transcript(rng, 2).split()[: rng.randint(1, 19)])
        else:
            text = _transcript(rng, rng.randint(4, 40))
            if td == "pii":
                text += rng.choice([f" Member: you can reach me at 555-01{rng.randint(0, 99):02d}.",
                                    f" Member: my email is member{rng.randint(1, 9999)}@example.com."])
        transcripts.append({"call_id": real_id, "transcript_text": text,
                            "word_count": len(text.split())})

        # quality scores: one row per call
        model = rng.choice(["qa-scorer-a", "qa-scorer-b"])
        sc = {"empathy_score": rng.randint(1, 5), "resolution_score": rng.randint(1, 5),
              "compliance_score": rng.randint(1, 5)}
        raw = json.dumps(sc)
        sd = _pick(rng, SCORE_DEFECTS)
        col = rng.choice(list(sc.keys()))
        if sd == "range":
            sc[col] = rng.choice([0, 6, 7, -1])
            raw = json.dumps(sc)
        elif sd == "null":
            sc[col] = None
            raw = json.dumps(sc)
        elif sd == "bad_json":
            raw = raw[: rng.randint(5, len(raw) - 2)]
        scores.append({"call_id": real_id, "model_name": model,
                       "model_version": "1.3" if model == "qa-scorer-a" else "2.0",
                       **sc, "raw_model_output": raw,
                       "scored_ts": _ts(end + timedelta(minutes=rng.randint(2, 15)))})
    return calls, transcripts, scores


def _n_calls(rng, hour):
    base = rng.randint(250, 350)
    return int(base * (1.15 if 8 <= hour < 18 else 0.85))


def write_triplet(root, rng, window_start, window_end, stamp):
    calls, tr, sc = batch(rng, window_start, window_end, _n_calls(rng, window_start.hour))
    _write(f"{root}/landing/calls/calls_{stamp}.json", calls)
    _write(f"{root}/landing/transcripts/transcripts_{stamp}.json", tr)
    _write(f"{root}/landing/quality_scores/quality_scores_{stamp}.json", sc)
    return len(calls)


def write_backfill(root, seed=42, hours=168):
    # One RNG per (day-ago, hour-of-day): every reset produces the same rows per hour slot,
    # so row counts and defect counts are identical whatever time the reset runs.
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    total = 0
    for h in range(hours, 0, -1):
        ws = now - timedelta(hours=h)
        rng = random.Random(f"{seed}-{(h - 1) // 24}-{ws.hour}")
        total += write_triplet(root, rng, ws, ws + timedelta(hours=1), ws.strftime("%Y%m%d_%H%M%S"))
    _write(f"{root}/landing/agents/agents.json", agents_rows(random.Random(seed)))
    return total


def write_live_file(root, rng=None):
    rng = rng or random.Random()
    now = datetime.now(timezone.utc)
    return write_triplet(root, rng, now - timedelta(seconds=60 + 1800), now - timedelta(seconds=1800),
                         now.strftime("%Y%m%d_%H%M%S"))


def write_bad_batch(root, seed=7):
    rng = random.Random(seed)
    now = datetime.now(timezone.utc)
    calls, _, _ = batch(rng, now - timedelta(hours=1), now, 300, drop_contract=True)
    _write(f"{root}/held_out/calls/calls_vendor_bad_batch.json", calls)
    return len(calls)
