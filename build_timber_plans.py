#!/usr/bin/env python3
import json, os, sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from decimal import Decimal

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, BotoCoreError

REGION = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-west-2"
HD_TABLE = os.getenv("HD_TABLE", "hd_nums")
ATTACH_TABLE = os.getenv("ATTACH_TABLE", "caltrees_attachments")
ATTACH_PLAN_GSI = os.getenv("ATTACH_PLAN_GSI")  # e.g. "plan-index" if exists
S3_BUCKET = os.getenv("S3_BUCKET")
S3_KEY = os.getenv("S3_KEY", "data/timber_plans.json")

if not S3_BUCKET:
    print("ERROR: S3_BUCKET env var is required", file=sys.stderr)
    sys.exit(2)

session = boto3.Session(region_name=REGION)
ddb = session.resource("dynamodb", config=Config(retries={"max_attempts": 10, "mode": "adaptive"}))
s3 = session.client("s3", config=Config(retries={"max_attempts": 10, "mode": "adaptive"}))

# ---------------- Helpers ----------------

def parse_date(val: Any) -> Optional[datetime]:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val
    if isinstance(val, (int, float, Decimal)):
        n = float(val)
        # seconds vs ms
        if n > 10_000_000_000:
            n = n / 1000.0
        try:
            return datetime.fromtimestamp(n, tz=timezone.utc)
        except Exception:
            return None
    if isinstance(val, str):
        s = val.strip()
        # numeric string?
        try:
            n = float(s)
            if n > 10_000_000_000:
                n = n / 1000.0
            return datetime.fromtimestamp(n, tz=timezone.utc)
        except Exception:
            pass
        # ISO-ish
        try:
            if s.endswith("Z"):
                s = s.replace("Z", "+00:00")
            return datetime.fromisoformat(s)
        except Exception:
            return None
    return None


def iso(dt: Optional[datetime]) -> Optional[str]:
    if not dt:
        return None
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def scan_all(table_name: str, extra: Dict[str, Any] = None) -> List[Dict[str, Any]]:
    table = ddb.Table(table_name)
    items: List[Dict[str, Any]] = []
    kwargs: Dict[str, Any] = {}
    if extra:
        kwargs.update(extra)
    last_key = None
    while True:
        if last_key:
            kwargs["ExclusiveStartKey"] = last_key
        res = table.scan(**kwargs)
        items.extend(res.get("Items", []))
        last_key = res.get("LastEvaluatedKey")
        if not last_key:
            break
    return items


def query_plan_attachments(plans: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    by_plan: Dict[str, List[Dict[str, Any]]] = {p: [] for p in plans}
    if not ATTACH_PLAN_GSI:
        # Fall back: scan all then bucket by plan
        for a in scan_all(ATTACH_TABLE):
            p = a.get("plan")
            if p in by_plan:
                by_plan[p].append(a)
        return by_plan

    table = ddb.Table(ATTACH_TABLE)
    from boto3.dynamodb.conditions import Key
    for plan in plans:
        last_key = None
        q = {"IndexName": ATTACH_PLAN_GSI, "KeyConditionExpression": Key("plan").eq(plan)}
        while True:
            if last_key:
                q["ExclusiveStartKey"] = last_key
            res = table.query(**q)
            by_plan[plan].extend(res.get("Items", []))
            last_key = res.get("LastEvaluatedKey")
            if not last_key:
                break
    return by_plan


def compute_latest(attachments: List[Dict[str, Any]]) -> Optional[datetime]:
    best: Optional[datetime] = None
    for a in attachments:
        dt = parse_date(a.get("latest_update"))
        if dt and (best is None or dt > best):
            best = dt
    return best


def pick_doc_url(hd: Dict[str, Any], attachments: List[Dict[str, Any]]) -> Optional[str]:
    latest = None
    latest_dt: Optional[datetime] = None
    for a in attachments:
        dt = parse_date(a.get("latest_update"))
        if dt and (latest_dt is None or dt > latest_dt):
            latest_dt = dt
            latest = a
    if latest and latest.get("cap_url"):
        return latest["cap_url"]
    for a in attachments:
        if a.get("cap_url"):
            return a["cap_url"]
    if hd.get("cap_url"):
        return hd["cap_url"]
    return None


# ---- Make everything JSON-safe (Decimal, set, datetime, etc.) ----

def to_jsonable(x: Any) -> Any:
    if isinstance(x, Decimal):
        # keep integers as ints; others as floats
        try:
            if x == x.to_integral_value():
                return int(x)
        except Exception:
            pass
        return float(x)
    if isinstance(x, datetime):
        return iso(x)
    if isinstance(x, set):
        return [to_jsonable(v) for v in x]
    if isinstance(x, (list, tuple)):
        return [to_jsonable(v) for v in x]
    if isinstance(x, dict):
        return {k: to_jsonable(v) for k, v in x.items()}
    return x


def main():
    print(f"[{datetime.now(timezone.utc).isoformat()}] Build start", flush=True)

    # Pull hd_nums
    hd_items = scan_all(HD_TABLE)
    plans = [str(x.get("HD_NUM")) for x in hd_items if x.get("HD_NUM") is not None]

    # Attachments grouped by plan
    att_by_plan = query_plan_attachments(plans)

    # Build enriched rows
    out_rows: List[Dict[str, Any]] = []
    for h in hd_items:
        plan = str(h.get("HD_NUM"))
        attachments = att_by_plan.get(plan, [])
        latest_dt = compute_latest(attachments)
        out_rows.append({
            "HD_NUM": plan,
            "national_forests": h.get("national_forests"),
            "national_forest_overlap": h.get("national_forest_overlap"),
            "roadless_areas": h.get("roadless_areas"),
            "roadless_area_overlap": h.get("roadless_area_overlap"),
            "total_area": h.get("total_area"),
            "latest_attachment_update": iso(latest_dt),
            "doc_url": pick_doc_url(h, attachments),
            "_attachments": attachments,
        })

    payload = {
        "generated_at": iso(datetime.now(timezone.utc)),
        "source": {
            "region": REGION,
            "hd_table": HD_TABLE,
            "attachments_table": ATTACH_TABLE,
            "mode": "gsi-query" if ATTACH_PLAN_GSI else "scan-all",
        },
        "plans": out_rows,
    }

    clean_payload = to_jsonable(payload)
    body = json.dumps(clean_payload, ensure_ascii=False).encode("utf-8")

    try:
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=S3_KEY,
            Body=body,
            ContentType="application/json; charset=utf-8",
            CacheControl="max-age=60",
        )
    except (BotoCoreError, ClientError) as e:
        print("ERROR uploading JSON to S3:", e, file=sys.stderr)
        sys.exit(1)

    print(f"[{datetime.now(timezone.utc).isoformat()}] Wrote {len(body):,} bytes to s3://{S3_BUCKET}/{S3_KEY}")
    print("Done.")


if __name__ == "__main__":
    main()
