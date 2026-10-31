"""
SR Poster — Airflow DAG (ClickHouse + Slack)
============================================
Monitors payment success rates (SR) for a merchant portfolio and posts
hourly Slack digests so account managers get real-time visibility into
payment health — without needing to query a data warehouse themselves.

Architecture highlights
-----------------------
• Data source  : ClickHouse (columnar DB) via a read-only Airflow connection
• Schedule     : Every 2 hours at :15 (cron: 15 */2 * * *)
• Window       : Last 3 completed hours — 1h overlap acts as a recovery
                 buffer if a single run fails
• Timezone     : Explicit IST via toTimeZone() — independent of server TZ
• Query design : SR_QUERY and ERROR_CODE_QUERY run ONCE per DAG run for
                 ALL merchants combined (merchant_id IN (...)), then results
                 are grouped in Python.  This cuts ClickHouse query count
                 from  2 × N_merchants  down to  2 per run, staying within
                 the read-only user's hourly query quota.
• Retries      : Disabled (retries=0) — a failed run must not consume
                 additional quota via auto-retry.

Payment method segmentation
----------------------------
Each transaction is bucketed into one of nine categories based on card type
(debit / credit / prepaid), recurrence (one-time vs recurring, initial vs
auto), and geography (domestic vs international / Amex non-IN).  The SQL
CASE block encodes this logic directly in ClickHouse to avoid post-fetch
filtering in Python.

Airflow v3 notes
-----------------
• Imports from airflow.sdk / standard-provider paths
• schedule= kwarg replaces the removed schedule_interval=
• module-level tags_dct required for dags-v3 DAGs
• Runs on the worker (not a pod) — lib.slack.notify builds a
  SlackWebhookOperator and calls .execute() with a live task context;
  BaseHook.get_connection needs the metadata DB — both worker-only.

Author : arihant.bahuguna@gmail.com
"""

import logging
from datetime import datetime, timedelta
from functools import partial

import pytz
from airflow.sdk import DAG, BaseHook
from airflow.providers.standard.operators.python import PythonOperator

from lib import email, slack

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DAG_OWNER = "arihant.bahuguna@gmail.com"
DAG_ID    = "sr_poster_clickhouse"

logger = logging.getLogger(__name__)
IST    = pytz.timezone("Asia/Kolkata")

tags_dct = {
    "org.team":          "payments-platform",
    "org.business-unit": "merchant-ops",
}

# ---------------------------------------------------------------------------
# Merchant roster  (demo data — real IDs and names are omitted)
# ---------------------------------------------------------------------------
# Each entry maps a merchant to the Slack channel where their SR digest lands.
# thread_ts=None means a fresh top-level message each run; set a real ts to
# reply in a thread instead.

MERCHANTS = [
    # ── Tier-1 merchants (high-volume, dedicated channel) ──────────────────
    {"merchant_id": "DEMO_MID_001", "name": "Merchant Alpha",   "channel_id": "C_DEMO_CHANNEL_A", "thread_ts": None},
    {"merchant_id": "DEMO_MID_002", "name": "Merchant Beta",    "channel_id": "C_DEMO_CHANNEL_A", "thread_ts": None},
    {"merchant_id": "DEMO_MID_003", "name": "Merchant Gamma",   "channel_id": "C_DEMO_CHANNEL_A", "thread_ts": None},
    # ── Tier-2 merchants (shared monitoring channel) ────────────────────────
    {"merchant_id": "DEMO_MID_004", "name": "Merchant Delta",   "channel_id": "C_DEMO_CHANNEL_B", "thread_ts": None},
    {"merchant_id": "DEMO_MID_005", "name": "Merchant Epsilon", "channel_id": "C_DEMO_CHANNEL_B", "thread_ts": None},
    {"merchant_id": "DEMO_MID_006", "name": "Merchant Zeta",    "channel_id": "C_DEMO_CHANNEL_B", "thread_ts": None},
    # ── Pilot merchants (separate channel for closer monitoring) ────────────
    {"merchant_id": "DEMO_MID_007", "name": "Merchant Eta",     "channel_id": "C_DEMO_CHANNEL_C", "thread_ts": None},
    {"merchant_id": "DEMO_MID_008", "name": "Merchant Theta",   "channel_id": "C_DEMO_CHANNEL_C", "thread_ts": None},
]

MERCHANT_IDS = [m["merchant_id"] for m in MERCHANTS]


# ---------------------------------------------------------------------------
# ClickHouse client factory
# ---------------------------------------------------------------------------

def get_clickhouse_client():
    """
    Build a ClickHouse client from the Airflow connection store.

    Connection ID : clickhouse_readonly_default
    The connection holds host / port / login / password / schema so no
    credentials live in DAG code.
    """
    from clickhouse_driver import Client as CHClient

    conn = BaseHook.get_connection("clickhouse_readonly_default")
    logger.info(
        "Connecting to ClickHouse host=%s port=%s user=%s database=%s",
        conn.host, conn.port or 9000, conn.login, conn.schema or "default",
    )
    return CHClient(
        host=conn.host,
        port=conn.port or 9000,
        user=conn.login,
        password=conn.password,
        database=conn.schema or "default",
        connect_timeout=10,
        send_receive_timeout=300,
        sync_request_timeout=5,
    )


# ---------------------------------------------------------------------------
# SQL helpers
# ---------------------------------------------------------------------------

def _merchant_ids_sql(merchant_ids: list[str]) -> str:
    """Render a Python list as a ClickHouse SQL IN (...) literal."""
    escaped = [mid.replace("'", "\\'") for mid in merchant_ids]
    return ", ".join(f"'{mid}'" for mid in escaped)


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------
# Both queries accept a single {merchant_ids_sql} placeholder so they execute
# once for the full merchant cohort rather than once per merchant.

SR_QUERY = """
SELECT *
FROM (
    SELECT
        formatDateTime(
            toStartOfHour(toTimeZone(fromUnixTimestamp(created_at), 'Asia/Kolkata')),
            '%Y-%m-%d %H:00:00'
        )                                           AS hour_interval,
        merchant_id,
        -- Nine-way segmentation: card_type × recurrence × geography
        CASE
            WHEN method = 'card' AND lower(type) = 'debit'
                 AND recurring = 1 AND recurring_type IN ('initial', 'card_change')
                 AND (international = 1 OR (network = 'American Express' AND cards_country <> 'IN'))
                THEN 'Debit Card - Recurring - Initial - International'
            WHEN method = 'card' AND lower(type) = 'debit'
                 AND recurring = 1 AND recurring_type = 'auto'
                 AND (international = 1 OR (network = 'American Express' AND cards_country <> 'IN'))
                THEN 'Debit Card - Recurring - Auto - International'
            WHEN method = 'card' AND lower(type) = 'credit'
                 AND recurring = 1 AND recurring_type IN ('initial', 'card_change')
                 AND (international = 1 OR (network = 'American Express' AND cards_country <> 'IN'))
                THEN 'Credit Card - Recurring - Initial - International'
            WHEN method = 'card' AND lower(type) = 'credit'
                 AND recurring = 1 AND recurring_type = 'auto'
                 AND (international = 1 OR (network = 'American Express' AND cards_country <> 'IN'))
                THEN 'Credit Card - Recurring - Auto - International'
            WHEN method = 'card' AND lower(type) = 'prepaid'
                 AND recurring = 1 AND recurring_type IN ('initial', 'card_change')
                 AND (international = 1 OR (network = 'American Express' AND cards_country <> 'IN'))
                THEN 'Prepaid Card - Recurring - Initial - International'
            WHEN method = 'card' AND lower(type) = 'prepaid'
                 AND recurring = 1 AND recurring_type = 'auto'
                 AND (international = 1 OR (network = 'American Express' AND cards_country <> 'IN'))
                THEN 'Prepaid Card - Recurring - Auto - International'
            WHEN method = 'card' AND lower(type) = 'debit'
                 AND (international = 1 OR (network = 'American Express' AND cards_country <> 'IN'))
                THEN 'Debit Card - International'
            WHEN method = 'card' AND lower(type) = 'credit'
                 AND (international = 1 OR (network = 'American Express' AND cards_country <> 'IN'))
                THEN 'Credit Card - International'
            WHEN method = 'card' AND lower(type) = 'prepaid'
                 AND (international = 1 OR (network = 'American Express' AND cards_country <> 'IN'))
                THEN 'Prepaid Card - International'
            ELSE NULL
        END                                         AS method_drilled,
        count(id)                                   AS attempts,
        round(countIf(authorized_at > 0) * 100.0 / count(id), 2) AS sr
    FROM payments.transactions_view
    WHERE merchant_id IN ({merchant_ids_sql})
      -- Exclude known non-3DS block — not a real failure
      AND (internal_error_code IS NULL
           OR internal_error_code NOT IN ('BAD_REQUEST_NON_3DS_INTERNATIONAL_NOT_ALLOWED'))
      -- Rolling 3-hour window (IST-aware)
      AND created_at >= toUnixTimestamp(toStartOfHour(toTimeZone(now(), 'Asia/Kolkata'))) - (3 * 3600)
      AND created_at <  toUnixTimestamp(toStartOfHour(toTimeZone(now(), 'Asia/Kolkata')))
    GROUP BY 1, 2, 3
) AS t
WHERE method_drilled IS NOT NULL AND method_drilled != ''
ORDER BY merchant_id, hour_interval, attempts DESC
"""

ERROR_CODE_QUERY = """
SELECT
    merchant_id,
    hour_interval,
    method_drilled,
    internal_error_code,
    count(*)                                                      AS failure_count,
    -- Grab up to 5 sample payment IDs for quick investigation
    arrayStringConcat(arraySlice(groupArray(id), 1, 5), ',')      AS sample_ids
FROM (
    SELECT
        merchant_id,
        formatDateTime(
            toStartOfHour(toTimeZone(fromUnixTimestamp(created_at), 'Asia/Kolkata')),
            '%Y-%m-%d %H:00:00'
        )                                                         AS hour_interval,
        CASE
            WHEN method = 'card' AND lower(type) = 'debit'
                 AND recurring = 1 AND recurring_type IN ('initial', 'card_change')
                 AND (international = 1 OR (network = 'American Express' AND cards_country <> 'IN'))
                THEN 'Debit Card - Recurring - Initial - International'
            WHEN method = 'card' AND lower(type) = 'debit'
                 AND recurring = 1 AND recurring_type = 'auto'
                 AND (international = 1 OR (network = 'American Express' AND cards_country <> 'IN'))
                THEN 'Debit Card - Recurring - Auto - International'
            WHEN method = 'card' AND lower(type) = 'credit'
                 AND recurring = 1 AND recurring_type IN ('initial', 'card_change')
                 AND (international = 1 OR (network = 'American Express' AND cards_country <> 'IN'))
                THEN 'Credit Card - Recurring - Initial - International'
            WHEN method = 'card' AND lower(type) = 'credit'
                 AND recurring = 1 AND recurring_type = 'auto'
                 AND (international = 1 OR (network = 'American Express' AND cards_country <> 'IN'))
                THEN 'Credit Card - Recurring - Auto - International'
            WHEN method = 'card' AND lower(type) = 'prepaid'
                 AND recurring = 1 AND recurring_type IN ('initial', 'card_change')
                 AND (international = 1 OR (network = 'American Express' AND cards_country <> 'IN'))
                THEN 'Prepaid Card - Recurring - Initial - International'
            WHEN method = 'card' AND lower(type) = 'prepaid'
                 AND recurring = 1 AND recurring_type = 'auto'
                 AND (international = 1 OR (network = 'American Express' AND cards_country <> 'IN'))
                THEN 'Prepaid Card - Recurring - Auto - International'
            WHEN method = 'card' AND lower(type) = 'debit'
                 AND (international = 1 OR (network = 'American Express' AND cards_country <> 'IN'))
                THEN 'Debit Card - International'
            WHEN method = 'card' AND lower(type) = 'credit'
                 AND (international = 1 OR (network = 'American Express' AND cards_country <> 'IN'))
                THEN 'Credit Card - International'
            WHEN method = 'card' AND lower(type) = 'prepaid'
                 AND (international = 1 OR (network = 'American Express' AND cards_country <> 'IN'))
                THEN 'Prepaid Card - International'
            ELSE NULL
        END                                                       AS method_drilled,
        internal_error_code,
        id
    FROM payments.transactions_view
    WHERE merchant_id IN ({merchant_ids_sql})
      AND authorized_at = 0
      AND internal_error_code IS NOT NULL
      AND internal_error_code != ''
      AND internal_error_code NOT IN ('BAD_REQUEST_NON_3DS_INTERNATIONAL_NOT_ALLOWED')
      AND created_at >= toUnixTimestamp(toStartOfHour(toTimeZone(now(), 'Asia/Kolkata'))) - (3 * 3600)
      AND created_at <  toUnixTimestamp(toStartOfHour(toTimeZone(now(), 'Asia/Kolkata')))
) AS base
WHERE method_drilled IS NOT NULL AND method_drilled != ''
GROUP BY 1, 2, 3, 4
ORDER BY merchant_id, hour_interval, method_drilled, failure_count DESC
"""


# ---------------------------------------------------------------------------
# Data parsing helpers
# ---------------------------------------------------------------------------

def parse_sr_rows(ch_rows: list) -> list[dict]:
    """Convert raw ClickHouse tuple rows into SR metric dicts."""
    cols = ["hour_interval", "merchant_id", "method_drilled", "attempts", "sr"]
    return [dict(zip(cols, row)) for row in ch_rows]


def parse_error_rows(ch_rows: list) -> list[dict]:
    """Convert raw ClickHouse tuple rows into error-code dicts."""
    cols = ["merchant_id", "hour_interval", "method_drilled",
            "internal_error_code", "failure_count", "sample_ids"]
    return [dict(zip(cols, row)) for row in ch_rows]


def group_by_merchant(rows: list[dict], key: str = "merchant_id") -> dict:
    """Partition a flat list of dicts into {merchant_id: [rows]}."""
    grouped: dict[str, list] = {}
    for row in rows:
        grouped.setdefault(row[key], []).append(row)
    return grouped


def build_error_map(error_rows: list[dict]) -> dict:
    """
    Build a nested lookup for O(1) access during message formatting:
        {hour: {method: [(error_code, count, [sample_ids])]}}
    """
    error_map: dict = {}
    for row in error_rows:
        hour   = row["hour_interval"]
        method = row["method_drilled"]
        code   = row["internal_error_code"]
        count  = int(row["failure_count"])
        raw    = row.get("sample_ids") or ""
        ids    = [x.strip() for x in raw.split(",") if x.strip()]
        error_map.setdefault(hour, {}).setdefault(method, []).append((code, count, ids))
    return error_map


# ---------------------------------------------------------------------------
# Message formatting
# ---------------------------------------------------------------------------

def format_sr_message(merchant_name: str, sr_rows: list[dict],
                      error_map: dict, posted_at: str) -> str | None:
    """
    Render SR data + error breakdown into a Slack markdown message.

    Output example
    --------------
    📊 *Merchant Alpha - Payment SR Update*
    Posted at: 2026-01-15 14:15:00 IST

    *12:00 - 13:00*
      • Credit Card - International: 340 attempts | 94.12% SR
        ↳ GATEWAY_TIMEOUT (18 failures): pay_ABC1, pay_ABC2
      • Debit Card - International: 120 attempts | 97.50% SR
    """
    if not sr_rows:
        return None

    # Group rows by hour
    hours: dict[str, list] = {}
    for row in sr_rows:
        hours.setdefault(row["hour_interval"], []).append(row)

    lines = [f"📊 *{merchant_name} - Payment SR Update*",
             f"Posted at: {posted_at} IST\n"]

    for hour in sorted(hours, reverse=True):
        h = hour.split(" ")[1][:5]
        next_h = str((int(h.split(":")[0]) + 1) % 24).zfill(2)
        lines.append(f"*{h} - {next_h}:00*")

        for row in hours[hour]:
            attempts = int(row["attempts"])
            sr       = float(row["sr"])
            method   = row["method_drilled"]
            lines.append(f"  • {method}: {attempts} attempts | {sr}% SR")

            for code, count, ids in error_map.get(hour, {}).get(method, []):
                id_str = ", ".join(ids) if ids else ""
                suffix = f": {id_str}" if id_str else ""
                lines.append(f"    ↳ {code} ({count} failures){suffix}")

        lines.append("")

    return "\n".join(lines).strip()


# ---------------------------------------------------------------------------
# Per-merchant posting
# ---------------------------------------------------------------------------

def post_merchant_message(merchant: dict, sr_rows: list[dict],
                          error_rows: list[dict], posted_at: str, **kwargs) -> None:
    """Post the Slack digest for a single merchant (no DB calls here)."""
    name       = merchant["name"]
    channel_id = merchant["channel_id"]

    error_map = build_error_map(error_rows)
    message   = format_sr_message(name, sr_rows, error_map, posted_at)

    if not message:
        logger.info("No SR data for %s — skipping Slack post.", name)
        return

    logger.info("Posting to Slack channel %s for %s …", channel_id, name)
    slack.notify(f"SR Update - {name}", message, channel_id, [], kwargs)
    logger.info("Posted successfully for %s.", name)


# ---------------------------------------------------------------------------
# Main task callable
# ---------------------------------------------------------------------------

def run_sr_poster(**kwargs) -> None:
    """
    Core DAG task: fetch SR + error data ONCE for all merchants,
    then fan out to per-merchant Slack posts.

    Query optimisation
    ------------------
    Naïve approach   : 2 ClickHouse queries × N merchants = 2N queries/run
    Optimised (this) : 2 queries total — results grouped in Python.
    With N=26 merchants this is a 26× reduction, keeping us within the
    read-only user's hourly quota.
    """
    ids_sql = _merchant_ids_sql(MERCHANT_IDS)

    client = get_clickhouse_client()
    try:
        logger.info("Step 1/2 — Fetching SR data for all merchants …")
        sr_rows    = parse_sr_rows(client.execute(SR_QUERY.format(merchant_ids_sql=ids_sql)))
        logger.info("  → %d SR rows fetched", len(sr_rows))

        logger.info("Step 2/2 — Fetching error-code data for all merchants …")
        error_rows = parse_error_rows(client.execute(ERROR_CODE_QUERY.format(merchant_ids_sql=ids_sql)))
        logger.info("  → %d error rows fetched", len(error_rows))
    finally:
        client.disconnect()

    sr_by_mid    = group_by_merchant(sr_rows)
    error_by_mid = group_by_merchant(error_rows)
    posted_at    = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")

    for merchant in MERCHANTS:
        mid  = merchant["merchant_id"]
        name = merchant["name"]
        logger.info("Processing %s (%s)", name, mid)

        merchant_sr = sr_by_mid.get(mid, [])
        if not merchant_sr:
            logger.info("  No SR data — skipping.")
            continue

        try:
            post_merchant_message(merchant, merchant_sr, error_by_mid.get(mid, []), posted_at, **kwargs)
        except Exception:
            logger.exception("Slack post failed for %s", name)
            raise  # bubble up so Airflow marks the task failed


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

default_args = {
    "owner":             DAG_OWNER,
    "start_date":        datetime(2026, 1, 1),
    "on_failure_callback": partial(email.send_mail_slack, [DAG_OWNER]),
    "retries":           0,           # never auto-retry — saves quota
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule="15 */2 * * *",          # every 2 hours at :15
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=3),
    tags=["payments", "sr-monitoring", "observability"],
    doc_md=__doc__,
) as dag:

    post_sr_updates = PythonOperator(
        task_id="post_sr_updates",
        python_callable=run_sr_poster,
    )
