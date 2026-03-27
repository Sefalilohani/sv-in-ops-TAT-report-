import os
import time
import requests
from datetime import datetime, timezone, timedelta
from collections import defaultdict

# ── CONFIG ─────────────────────────────────────────────────────

_raw_token = os.environ["SLACK_BOT_TOKEN"]
SLACK_TOKEN = "xoxb" + _raw_token[4:31] + "bFqMGfkmHBzvLRtU1It2ptnt"

REDASH_API_KEY = "CWcvNsz8fkzifFJPD6r7kc2T6TCU6pbhxa0z0nRm"
REDASH_QUERY_ID = 1822
REDASH_BASE = "https://redash.springworks.in"

OPS_CHANNEL_ID = "CF0RH10M8"

AGE_THRESHOLD_HIGH = 14
AGE_THRESHOLD_LOW  = 7

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

    high_aged = {}
    low_aged  = {}

    for cid, row in all_checks.items():
        age = row.get("Net TAT") or 0
        if age >= AGE_THRESHOLD_HIGH:
            high_aged[cid] = row
        elif age >= AGE_THRESHOLD_LOW:
            low_aged[cid] = row

    total_high = len(high_aged)
    total_low  = len(low_aged)

    groups_high = defaultdict(lambda: defaultdict(int))
    groups_low  = defaultdict(lambda: defaultdict(int))

    for row in high_aged.values():
        verification = (row.get("Verification") or "UNKNOWN").upper()
        v_type = (row.get("Verification Type") or "N/A").upper()
        groups_high[verification][v_type] += 1

    for row in low_aged.values():
        verification = (row.get("Verification") or "UNKNOWN").upper()
        v_type = (row.get("Verification Type") or "N/A").upper()
        groups_low[verification][v_type] += 1

    return dict(groups_high), dict(groups_low), total_high, total_low, total_all


# ── BUILD SLACK MESSAGE ────────────────────────────────────────

def build_message(groups_high, groups_low, total_high, total_low, total_all):
    today = datetime.now(IST).strftime("%d %b %Y")

    lines = [
        f":bar_chart: *TAT Case Update - {today}*",
        f"*Cases 14+ days:* `{total_high}` | *Cases 7-14 days:* `{total_low}` | *Total active checks:* `{total_all}`",
        "",
    ]

    all_verifications = sorted(set(list(groups_high.keys()) + list(groups_low.keys())))

    if not all_verifications:
        lines.append("_No cases found with age >=7 days._")
    else:
        c1, c2, c3, c4 = 20, 20, 10, 10
        header  = f"{'Verification':<{c1}} {'Type':<{c2}} {'14+ days':>{c3}}  {'7-14 days':>{c4}}"
        divider = "-" * (c1 + c2 + c3 + c4 + 4)

        table = [header, divider]
        grand_high = 0
        grand_low  = 0

        for verification in all_verifications:
            sub_high = groups_high.get(verification, {})
            sub_low  = groups_low.get(verification, {})
            all_types = sorted(set(list(sub_high.keys()) + list(sub_low.keys())))

            sub_total_high = sum(sub_high.values())
            sub_total_low  = sum(sub_low.values())
            grand_high += sub_total_high
            grand_low  += sub_total_low

            first = True
            for v_type in all_types:
                h = sub_high.get(v_type, 0)
                l = sub_low.get(v_type, 0)
                label = verification if first else ""
                table.append(f"{label:<{c1}} {v_type:<{c2}} {h:>{c3}}  {l:>{c4}}")
                first = False

            if len(all_types) > 1:
                table.append(
                    f"{'':>{c1}} {'Subtotal':<{c2}} {sub_total_high:>{c3}}  {sub_total_low:>{c4}}"
                )
            table.append("")

        table.append(divider)
        table.append(
            f"{'GRAND TOTAL':<{c1 + c2 + 1}} {grand_high:>{c3}}  {grand_low:>{c4}}"
        )

        lines.append("```")
        lines.extend(table)
        lines.append("```")

    lines.append("")
    lines.append("<https://redash.springworks.in/queries/1822|View on Redash>")
    lines.append("")
    lines.append("<!subteam^S04K9859L64> Please review and share an update on 14+ days checks.")

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

    groups_high, groups_low, total_high, total_low, total_all = filter_and_aggregate(rows)
    print(f"Unique checks: {total_all}, 14+ days: {total_high}, 7-14 days: {total_low}")

    message = build_message(groups_high, groups_low, total_high, total_low, total_all)
    print("\n--- Slack preview ---")
    print(message)
    print("---------------------\n")

    post_slack(message)


if __name__ == "__main__":
    main()
