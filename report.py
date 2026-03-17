import os
import time
import requests
from datetime import datetime, timezone, timedelta
from collections import defaultdict

# ── CONFIG ─────────────────────────────────────────────────────

_raw_token = os.environ["SLACK_BOT_TOKEN"]
SLACK_TOKEN = "xoxb" + _raw_token[4:]  # fix autocapitalisation of first letter

REDASH_API_KEY = "CWcvNsz8fkzifFJPD6r7kc2T6TCU6pbhxa0z0nRm"
REDASH_QUERY_ID = 1420
REDASH_BASE = "https://redash.springworks.in"

OPS_CHANNEL_ID = "CF0RH10M8"  # #sv-in-ops

AGE_THRESHOLD = 14  # days

IST = timezone(timedelta(hours=5, minutes=30))

PARAMETERS = {
    "CHECK Status": ["9", "12", "13", "4", "6", "2", "1", "0", "-2", "10"],
    "client_priority": ["ALL"],
    "task_status": ["PENDING", "ASSIGNMENT_PENDING", "SCHEDULED"],
    "task_type": ["ALL"],
}


# ── FETCH REDASH DATA ──────────────────────────────────────────

def fetch_redash():
    """
    POST to /api/queries/{id}/results with max_age=0 to trigger fresh execution.
    If result is cached, returns immediately.
    If a job is queued, re-POST with max_age=60 until result is ready.
    Works entirely with API key auth — no session cookie or /api/jobs polling needed.
    """
    headers = {
        "Authorization": f"Key {REDASH_API_KEY}",
        "Content-Type": "application/json",
    }

    url = f"{REDASH_BASE}/api/queries/{REDASH_QUERY_ID}/results"
    payload = {"parameters": PARAMETERS, "max_age": 0}

    print("Triggering Redash query refresh...")
    r = requests.post(url, headers=headers, json=payload, timeout=30)
    print(f"POST status: {r.status_code}")
    if r.status_code not in (200, 201):
        print(f"Response: {r.text[:500]}")
    r.raise_for_status()
    resp = r.json()

    # Immediate cached result
    if "query_result" in resp:
        rows = resp["query_result"]["data"]["rows"]
        print(f"Got immediate result: {len(rows)} rows")
        return rows

    # Job queued — re-POST with max_age=60 to get result when ready
    job_id = resp.get("job", {}).get("id", "unknown")
    print(f"Query job queued (id={job_id}), polling for result...")

    poll_payload = {**payload, "max_age": 60}

    for attempt in range(20):
        time.sleep(3)
        print(f"  Poll attempt {attempt + 1}/20...")
        r2 = requests.post(url, headers=headers, json=poll_payload, timeout=30)
        if r2.status_code not in (200, 201):
            print(f"  Poll status {r2.status_code}: {r2.text[:200]}")
            continue
        resp2 = r2.json()
        if "query_result" in resp2:
            rows = resp2["query_result"]["data"]["rows"]
            print(f"  Got result: {len(rows)} rows")
            return rows
        new_job = resp2.get("job", {})
        print(f"  Still running, job status={new_job.get('status')}")

    raise Exception("Timed out waiting for Redash query result after 60 seconds")


# ── FILTER & AGGREGATE ─────────────────────────────────────────

def filter_and_aggregate(rows):
    """
    Deduplicate by Check ID, filter Age >= 14 days,
    group by Verification x Verification Type.
    """
    all_checks = {}
    for row in rows:
        cid = row.get("Check ID")
        if cid is None:
            continue
        if cid not in all_checks:
            all_checks[cid] = row

    total_all = len(all_checks)

    aged = {
        cid: row
        for cid, row in all_checks.items()
        if (row.get("Age") or 0) >= AGE_THRESHOLD
    }
    total_aged = len(aged)

    groups = defaultdict(lambda: defaultdict(int))
    for row in aged.values():
        verification = (row.get("Verification") or "UNKNOWN").upper()
        v_type = (row.get("Verification Type") or "N/A").upper()
        groups[verification][v_type] += 1

    return dict(groups), total_aged, total_all


# ── BUILD SLACK MESSAGE ────────────────────────────────────────

def build_message(groups, total_aged, total_all):
    now = datetime.now(IST)
    today = now.strftime("%d %b %Y")

    lines = [
        f"*:bar_chart: Daily Case Update \u2014 {today}*",
        f"*Cases \u226514 days:* `{total_aged}` of `{total_all}` active checks",
        "",
    ]

    if not groups:
        lines.append("_No cases found with age \u226514 days._")
    else:
        col1, col2, col3 = 22, 20, 7
        header = f"{'Verification':<{col1}} {'Type':<{col2}} {'Count':>{col3}}"
        divider = "-" * (col1 + col2 + col3 + 2)

        table = [header, divider]
        grand_total = 0

        for verification in sorted(groups):
            sub = groups[verification]
            sub_total = sum(sub.values())
            grand_total += sub_total
            first = True
            for v_type in sorted(sub):
                count = sub[v_type]
                label = verification if first else ""
                table.append(f"{label:<{col1}} {v_type:<{col2}} {count:>{col3}}")
                first = False
            if len(sub) > 1:
                table.append(f"{'':>{col1}} {'Subtotal':<{col2}} {sub_total:>{col3}}")
            table.append("")

        table.append(divider)
        table.append(f"{'GRAND TOTAL':<{col1 + col2 + 1}} {grand_total:>{col3}}")

        lines.append("```")
        lines.extend(table)
        lines.append("```")

    lines.append("")
    lines.append(
        "CC: <!subteam^S08T66C76CS> \u2014 please review and share your updates. :pray:"
    )

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

    groups, total_aged, total_all = filter_and_aggregate(rows)
    print(f"Unique checks: {total_all}, aged >=14 days: {total_aged}")

    message = build_message(groups, total_aged, total_all)
    print("\n--- Slack preview ---")
    print(message)
    print("---------------------\n")

    post_slack(message)


if __name__ == "__main__":
    main()
