import os
import time
import requests
from datetime import datetime, timezone, timedelta
from collections import defaultdict

# ── CONFIG ─────────────────────────────────────────────────────

_raw_token = os.environ["SLACK_BOT_TOKEN"]
SLACK_TOKEN = "xoxb" + _raw_token[4:31] + "bFqMGfkmHBzvLRtU1It2ptnt"  # hardcode prefix+suffix; only numeric middle from secret

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

AUTH_HEADERS = {
    "Authorization": f"Key {REDASH_API_KEY}",
    "Content-Type": "application/json",
}


# ── FETCH REDASH DATA ──────────────────────────────────────────

def fetch_redash():
    """
    POST max_age=0 to trigger fresh execution.
    Poll /api/jobs/{id} until complete (up to 5 minutes).
    Then fetch result from /api/query_results/{id}.
    """
    url = f"{REDASH_BASE}/api/queries/{REDASH_QUERY_ID}/results"
    payload = {"parameters": PARAMETERS, "max_age": 0}

    print("Triggering Redash query refresh...")
    r = requests.post(url, headers=AUTH_HEADERS, json=payload, timeout=30)
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

    # Job queued — poll /api/jobs/{job_id} directly
    job_id = resp["job"]["id"]
    print(f"Query job queued (id={job_id}), polling /api/jobs/...")

    for attempt in range(100):  # up to ~5 minutes (100 x 3s)
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

        if status == 3:  # success
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

        if status == 4:  # failed
            raise Exception(f"Redash query failed: {job.get('error')}")

    raise Exception("Timed out after 5 minutes waiting for Redash query")


# ── FILTER & AGGREGATE ─────────────────────────────────────────

# Check Status codes from Redash URL
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
    today = datetime.now(IST).strftime("%d %b %Y")

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
