import os
import time
import requests
from datetime import datetime, timezone, timedelta
from collections import defaultdict

# ── CONFIG ─────────────────────────────────────────────────────

_raw_token = os.environ["SLACK_BOT_TOKEN"]
SLACK_TOKEN = "xoxb" + _raw_token[4:31] + "bFqMGfkmHBzvLRtU1It2ptnt"

REDASH_API_KEY = "CWcvNsz8fkzifFJPD6r7kc2T6TCU6pbhxa0z0nRm"
REDASH_QUERY_ID = 3464
REDASH_BASE = "https://redash.springworks.in"

OPS_CHANNEL_ID = "C0AGRE19V6U"

AGE_THRESHOLD_HIGH      = 14
AGE_THRESHOLD_LOW       = 7
AGE_THRESHOLD_VERY_HIGH = 60

# "Nearly TAT" pulse: cases sitting exactly on the day before/at their section's
# breach point. Standard sections breach at 14 days; EDU Official/Hybrid breach
# at 60, so their nearing day is 59 (one day out) instead of 14.
NEARING_STANDARD_DAY   = 14
NEARING_VERY_HIGH_DAY  = 59

IST = timezone(timedelta(hours=5, minutes=30))

AUTH_HEADERS = {
    "Authorization": f"Key {REDASH_API_KEY}",
    "Content-Type": "application/json",
}


# ── FETCH REDASH DATA ──────────────────────────────────────────

def fetch_redash():
    url = f"{REDASH_BASE}/api/queries/{REDASH_QUERY_ID}/results"
    payload = {"max_age": 0}

    print("Triggering Redash query refresh...")
    r = requests.post(url, headers=AUTH_HEADERS, json=payload, timeout=30)
    print(f"POST status: {r.status_code}")
    if r.status_code not in (200, 201):
        print(f"Response: {r.text[:500]}")
    r.raise_for_status()
    resp = r.json()

    if "query_result" in resp:
        rows = resp["query_result"]["data"]["rows"]
        print(f"Got immediate result: {len(rows)} rows")
        return rows

    job_id = resp["job"]["id"]
    print(f"Query job queued (id={job_id}), polling /api/jobs/...")

    for attempt in range(100):
        time.sleep(3)
        jr = requests.get(
            f"{REDASH_BASE}/api/jobs/{job_id}",
            headers=AUTH_HEADERS,
            timeout=15,
        )
        jr.raise_for_status()
        job = jr.json()["job"]
        status = job["status"]
        print(f"  attempt {attempt + 1}: job status={status}")

        if status == 3:
            result_id = job["query_result_id"]
            print(f"  Job done — fetching result_id={result_id}")
            rr = requests.get(
                f"{REDASH_BASE}/api/query_results/{result_id}",
                headers=AUTH_HEADERS,
                timeout=30,
            )
            rr.raise_for_status()
            rows = rr.json()["query_result"]["data"]["rows"]
            print(f"  Got {len(rows)} rows")
            return rows

        if status == 4:
            raise Exception(f"Redash query failed: {job.get('error')}")

    raise Exception("Timed out after 5 minutes waiting for Redash query")


# ── FILTER & AGGREGATE ─────────────────────────────────────────

VALID_TASK_STATUSES = {"PENDING", "ASSIGNMENT_PENDING", "SCHEDULED"}


def filter_and_aggregate(rows):
    all_checks = {}
    for row in rows:
        cid = row.get("Check ID")
        if cid is None:
            continue
        task_status = (row.get("Task Status") or "").upper()
        if task_status not in VALID_TASK_STATUSES:
            continue
        if cid not in all_checks:
            all_checks[cid] = row

    total_all = len(all_checks)

    high_aged      = {}
    low_aged       = {}
    very_high_aged = {}

    for cid, row in all_checks.items():
        age = row.get("Net TAT") or 0
        if age >= AGE_THRESHOLD_HIGH:
            high_aged[cid] = row
        elif age >= AGE_THRESHOLD_LOW:
            low_aged[cid] = row
        if age >= AGE_THRESHOLD_VERY_HIGH:
            very_high_aged[cid] = row

    total_high = len(high_aged)
    total_low  = len(low_aged)

    groups_high      = defaultdict(lambda: defaultdict(int))
    groups_low       = defaultdict(lambda: defaultdict(int))
    groups_very_high = defaultdict(lambda: defaultdict(int))
    groups_task_type = defaultdict(lambda: defaultdict(int))
    groups_nearing_standard  = defaultdict(lambda: defaultdict(int))
    groups_nearing_very_high = defaultdict(lambda: defaultdict(int))

    for row in high_aged.values():
        verification = (row.get("Verification") or "UNKNOWN").upper()
        v_type = (row.get("Verification Type") or "N/A").upper()
        groups_high[verification][v_type] += 1

        task_type = (row.get("Task Type") or "N/A").strip() or "N/A"
        groups_task_type[verification][task_type] += 1

    for row in low_aged.values():
        verification = (row.get("Verification") or "UNKNOWN").upper()
        v_type = (row.get("Verification Type") or "N/A").upper()
        groups_low[verification][v_type] += 1

    for row in very_high_aged.values():
        verification = (row.get("Verification") or "UNKNOWN").upper()
        v_type = (row.get("Verification Type") or "N/A").upper()
        groups_very_high[verification][v_type] += 1

    for row in all_checks.values():
        age = row.get("Net TAT") or 0
        day = int(age)
        if day == NEARING_STANDARD_DAY:
            verification = (row.get("Verification") or "UNKNOWN").upper()
            v_type = (row.get("Verification Type") or "N/A").upper()
            groups_nearing_standard[verification][v_type] += 1
        elif day == NEARING_VERY_HIGH_DAY:
            verification = (row.get("Verification") or "UNKNOWN").upper()
            v_type = (row.get("Verification Type") or "N/A").upper()
            groups_nearing_very_high[verification][v_type] += 1

    return (
        dict(groups_high), dict(groups_low), dict(groups_very_high),
        dict(groups_task_type), dict(groups_nearing_standard), dict(groups_nearing_very_high),
        total_high, total_low, total_all,
    )


# ── BUILD SLACK MESSAGE ────────────────────────────────────────

# Maps a raw "Verification" value to the team section it's reported under,
# and a raw "Verification Type" value to its display label + second metric
# within that section. metric "low" pairs 14+ with the 7+ bucket; "very_high"
# pairs 14+ with the 60+ bucket instead.
DEFAULT_TYPE_CONFIG = {"label": "Other", "metric": "low"}

TEAM_SECTIONS = [
    {
        "verification": "EMP",
        "label": "EMP",
        "mentions": ["<@UN1E2L4G0>", "<@UURRMS3MG>"],  # Selva, Shalini
        "type_config": None,  # totals only, no per-type breakdown
    },
    {
        "verification": "REF",
        "label": "REF",
        "mentions": ["<@UN1E2L4G0>", "<@U07PNP1L9C4>"],  # Selva, Nazia
        "type_config": None,
    },
    {
        "verification": "ADD",
        "label": "ADD",
        "mentions": ["<@U03BUG17X54>", "<@U04GVAGFE1E>", "<@U08JSQ1LBFG>", "<@U08Q8ML3DBK>"],  # Ramya, Deepika, Durga, Aishwarya
        "type_config": {
            "DIGITAL": {"label": "DAV", "metric": "low"},
            "PHYSICAL": {"label": "PAV", "metric": "low"},
            "POSTAL": {"label": "Postal", "metric": "low"},
        },
    },
    {
        "verification": "EDU",
        "label": "EDU",
        "mentions": ["<@U04CBSJ1XL1>", "<@U08Q8ML3DBK>", "<@U08JSQ1LBFG>"],  # Navaneetha KS, Aishwarya, Durga
        "type_config": {
            "REGIONAL_PARTNER": {"label": "Regional", "metric": "low"},
            "OFFICIAL": {"label": "Official", "metric": "very_high"},
            "HYBRID": {"label": "Hybrid", "metric": "very_high"},
        },
    },
]
MISC_MENTIONS = ["<@U017K6KQT2A>"]  # Thanveer
MAPPED_VERIFICATIONS = {section["verification"] for section in TEAM_SECTIONS}


def build_task_type_table(groups_task_type):
    """Verification x Task Type pivot, 14+ days cases only, as a monospace table."""
    verifications = sorted(groups_task_type.keys())
    task_types = sorted({tt for sub in groups_task_type.values() for tt in sub})

    header = ["Verification"] + task_types + ["Total"]
    col_totals = defaultdict(int)
    rows = []
    for verification in verifications:
        sub = groups_task_type[verification]
        values = [sub.get(tt, 0) for tt in task_types]
        for tt, val in zip(task_types, values):
            col_totals[tt] += val
        rows.append([verification] + [str(v) for v in values] + [str(sum(values))])

    grand_total = sum(col_totals.values())
    rows.append(["Total"] + [str(col_totals[tt]) for tt in task_types] + [str(grand_total)])

    widths = [max(len(header[i]), *(len(r[i]) for r in rows)) for i in range(len(header))]

    def fmt_row(r):
        return " | ".join(val.ljust(widths[i]) for i, val in enumerate(r))

    divider = "-+-".join("-" * w for w in widths)
    table_lines = [fmt_row(header), divider] + [fmt_row(r) for r in rows]
    return "```\n" + "\n".join(table_lines) + "\n```"


def build_nearing_section(groups_nearing_standard, groups_nearing_very_high):
    lines = [f"Below cases are nearly TAT (Currently {NEARING_STANDARD_DAY} days)"]

    for section in TEAM_SECTIONS:
        verification = section["verification"]
        sub_standard = groups_nearing_standard.get(verification, {})

        if section["type_config"] is None:
            count = sum(sub_standard.values())
            lines.append(f"{section['label']} - {count}")
        else:
            for v_type, config in section["type_config"].items():
                if config["metric"] == "very_high":
                    count = groups_nearing_very_high.get(verification, {}).get(v_type, 0)
                    lines.append(f"{section['label']} {config['label']} ({NEARING_VERY_HIGH_DAY} days) - {count}")
                else:
                    count = sub_standard.get(v_type, 0)
                    lines.append(f"{section['label']} {config['label']} - {count}")

    misc_count = sum(
        count for v, sub in groups_nearing_standard.items() if v not in MAPPED_VERIFICATIONS for count in sub.values()
    )
    lines.append(f"MISC - {misc_count}")

    return "\n".join(lines)


def build_message(
    groups_high, groups_low, groups_very_high,
    groups_task_type, groups_nearing_standard, groups_nearing_very_high,
    total_high, total_low, total_all,
):
    today = datetime.now(IST).strftime("%d %b %Y")

    lines = [
        f":bar_chart: TAT Case Update - {today}",
        "",
        f"Cases 14+ days: {total_high}",
        f"Cases 7+ days: {total_low}",
        "",
    ]

    for section in TEAM_SECTIONS:
        verification = section["verification"]
        sub_high = groups_high.get(verification, {})
        sub_low = groups_low.get(verification, {})
        sub_very_high = groups_very_high.get(verification, {})

        lines.append(f"{section['label']} - {' '.join(section['mentions'])}")

        if section["type_config"] is None:
            h = sum(sub_high.values())
            l = sum(sub_low.values())
            lines.append(f"14+ days - {h} , 7+ days - {l}")
        else:
            all_types = sorted(set(list(sub_high.keys()) + list(sub_low.keys()) + list(sub_very_high.keys())))
            for v_type in all_types:
                config = section["type_config"].get(v_type, DEFAULT_TYPE_CONFIG)
                h = sub_high.get(v_type, 0)
                if config["metric"] == "very_high":
                    second_label, second_val = "60+ days", sub_very_high.get(v_type, 0)
                else:
                    second_label, second_val = "7+ days", sub_low.get(v_type, 0)
                lines.append(f"{config['label']} - 14+ days - {h} , {second_label} - {second_val}")
        lines.append("")

    misc_high = sum(
        count for v, sub in groups_high.items() if v not in MAPPED_VERIFICATIONS for count in sub.values()
    )
    misc_low = sum(
        count for v, sub in groups_low.items() if v not in MAPPED_VERIFICATIONS for count in sub.values()
    )
    lines.append(f"MISC - {' '.join(MISC_MENTIONS)}")
    lines.append(f"14+ days - {misc_high} , 7+ days - {misc_low}")
    lines.append("")

    lines.append("Task Type breakdown - 14+ days cases only")
    lines.append(build_task_type_table(groups_task_type))
    lines.append("")

    lines.append(build_nearing_section(groups_nearing_standard, groups_nearing_very_high))
    lines.append("")

    lines.append(f"Redash - {REDASH_BASE}/queries/{REDASH_QUERY_ID}")

    return "\n".join(lines)


# ── POST TO SLACK ──────────────────────────────────────────────

def post_slack(text):
    r = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={
            "Authorization": f"Bearer {SLACK_TOKEN}",
            "Content-Type": "application/json",
        },
        json={"channel": OPS_CHANNEL_ID, "text": text, "mrkdwn": True},
        timeout=15,
    )
    r.raise_for_status()
    resp = r.json()
    if not resp.get("ok"):
        raise Exception(f"Slack API error: {resp.get('error')}")
    print(f"Message sent. ts={resp['ts']}")
    return resp["ts"]


# ── MAIN ───────────────────────────────────────────────────────

def main():
    rows = fetch_redash()
    print(f"Total rows: {len(rows)}")

    (
        groups_high, groups_low, groups_very_high,
        groups_task_type, groups_nearing_standard, groups_nearing_very_high,
        total_high, total_low, total_all,
    ) = filter_and_aggregate(rows)
    print(f"Unique checks: {total_all}, 14+ days: {total_high}, 7-14 days: {total_low}")

    message = build_message(
        groups_high, groups_low, groups_very_high,
        groups_task_type, groups_nearing_standard, groups_nearing_very_high,
        total_high, total_low, total_all,
    )
    print("\n--- Slack preview ---")
    print(message)
    print("---------------------\n")

    post_slack(message)


if __name__ == "__main__":
    main()
