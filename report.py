import os
import requests
import time
from datetime import datetime
from collections import defaultdict

REDASH_URL = "https://redash.springworks.in"
REDASH_API_KEY = os.environ["REDASH_API_KEY"]
SLACK_BOT_TOKEN = os.environ["SLACK_BOT_TOKEN"]
SLACK_CHANNEL = os.environ.get("SLACK_CHANNEL", "CF0RH10M8")  # #sv-in-ops
QUERY_ID = 1420
AGE_THRESHOLD = 14  # days

PARAMETERS = {
    "CHECK Status": ["9", "12", "13", "4", "6", "2", "1", "0", "-2", "10"],
    "client_priority": ["ALL"],
    "task_status": ["PENDING", "ASSIGNMENT_PENDING", "SCHEDULED"],
    "task_type": ["ALL"],
}

HEADERS = {"Authorization": f"Key {REDASH_API_KEY}"}


def refresh_and_get_rows():
    """Trigger a fresh query execution and return all rows."""
    print("Triggering Redash query refresh...")
    resp = requests.post(
        f"{REDASH_URL}/api/queries/{QUERY_ID}/results",
        headers=HEADERS,
        json={"parameters": PARAMETERS, "max_age": 0},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()

    if "query_result" in data:
        return data["query_result"]["data"]["rows"]

    job_id = data["job"]["id"]
    return _poll_job(job_id)


def _poll_job(job_id):
    """Poll Redash job until complete, then return rows."""
    print(f"Polling job {job_id}...")
    for attempt in range(60):
        resp = requests.get(
            f"{REDASH_URL}/api/jobs/{job_id}", headers=HEADERS, timeout=15
        )
        resp.raise_for_status()
        job = resp.json()["job"]
        status = job["status"]

        if status == 3:  # success
            result_id = job["query_result_id"]
            print(f"Job done — result ID: {result_id}")
            return _fetch_result(result_id)
        elif status == 4:  # error
            raise RuntimeError(f"Redash query failed: {job.get('error')}")

        print(f"  attempt {attempt + 1}: status={status}, waiting...")
        time.sleep(3)

    raise TimeoutError("Timed out waiting for Redash query results")


def _fetch_result(result_id):
    resp = requests.get(
        f"{REDASH_URL}/api/query_results/{result_id}", headers=HEADERS, timeout=30
    )
    resp.raise_for_status()
    return resp.json()["query_result"]["data"]["rows"]


def filter_and_aggregate(rows):
    """
    Filter checks with Age >= 14 days (deduplicated by Check ID),
    then group by Verification x Verification Type.
    Returns: (grouped_counts dict, total_aged, total_all)
    """
    all_checks = {}
    for row in rows:
        cid = row.get("Check ID")
        if cid is None:
            continue
        if cid not in all_checks:
            all_checks[cid] = row

    total_all = len(all_checks)

    aged_checks = {
        cid: row
        for cid, row in all_checks.items()
        if (row.get("Age") or 0) >= AGE_THRESHOLD
    }
    total_aged = len(aged_checks)

    groups = defaultdict(lambda: defaultdict(int))
    for row in aged_checks.values():
        verification = (row.get("Verification") or "UNKNOWN").upper()
        v_type = (row.get("Verification Type") or "N/A").upper()
        groups[verification][v_type] += 1

    return dict(groups), total_aged, total_all


def build_slack_message(groups, total_aged, total_all):
    today = datetime.now().strftime("%d %b %Y")

    lines = [
        f"*:bar_chart: Daily Case Update — {today}*",
        f"*Cases \u226514 days:* `{total_aged}` of `{total_all}` active checks",
        "",
    ]

    if not groups:
        lines.append("_No cases found with age \u226514 days._")
    else:
        col1, col2, col3 = 22, 20, 7
        header = f"{'Verification':<{col1}} {'Type':<{col2}} {'Count':>{col3}}"
        divider = "-" * (col1 + col2 + col3 + 2)

        table_lines = [header, divider]
        grand_total = 0

        for verification in sorted(groups):
            sub = groups[verification]
            sub_total = sum(sub.values())
            grand_total += sub_total
            first = True
            for v_type in sorted(sub):
                count = sub[v_type]
                v_label = verification if first else ""
                table_lines.append(
                    f"{v_label:<{col1}} {v_type:<{col2}} {count:>{col3}}"
                )
                first = False
            if len(sub) > 1:
                table_lines.append(
                    f"{'':>{col1}} {'Subtotal':<{col2}} {sub_total:>{col3}}"
                )
            table_lines.append("")

        table_lines.append(divider)
        table_lines.append(
            f"{'GRAND TOTAL':<{col1 + col2 + 1}} {grand_total:>{col3}}"
        )

        lines.append("```")
        lines.extend(table_lines)
        lines.append("```")

    lines.append("")
    lines.append(
        "cc: *@svin-ops-teamspocs* — please review and share your updates. :pray:"
    )

    return "\n".join(lines)


def send_to_slack(message):
    resp = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={
            "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
            "Content-Type": "application/json",
        },
        json={
            "channel": SLACK_CHANNEL,
            "text": message,
            "mrkdwn": True,
        },
        timeout=15,
    )
    resp.raise_for_status()
    result = resp.json()
    if not result.get("ok"):
        raise RuntimeError(f"Slack API error: {result.get('error')}")
    print(f"Message sent. ts={result['ts']}")
    return result


def main():
    rows = refresh_and_get_rows()
    print(f"Total rows fetched: {len(rows)}")

    groups, total_aged, total_all = filter_and_aggregate(rows)
    print(f"Unique checks: {total_all} total, {total_aged} aged >=14 days")
    for v, sub in sorted(groups.items()):
        for vt, cnt in sorted(sub.items()):
            print(f"  {v} / {vt}: {cnt}")

    message = build_slack_message(groups, total_aged, total_all)
    print("\n--- Slack message preview ---")
    print(message)
    print("-----------------------------\n")

    send_to_slack(message)


if __name__ == "__main__":
    main()
