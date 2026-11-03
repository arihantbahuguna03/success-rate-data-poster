# Merchant Pulse — Payment Health Digest

An Airflow DAG that monitors payment transaction success rates across a merchant portfolio and posts structured Slack digests every hour, giving account teams real-time payment health visibility without needing direct data warehouse access.

> Built Aug 2024–Aug 2025 using the Anthropic SDK and Apache Airflow — before MCP, Claude Code, and modern agentic tooling existed in production-ready form. Raw API integration, no orchestration framework.

---

## The Problem

Merchant account teams needed to know when a merchant's payment success rates dropped — by payment type, by hour — without having to query ClickHouse themselves. Engineers couldn't give everyone DB access. Manual checks meant issues surfaced hours late. Account managers were reactive instead of proactive.

---

## What It Does

An Airflow DAG runs every hour and posts a structured digest to each merchant's dedicated Slack channel:

```
📊 Merchant Alpha — Payment Health Digest
Posted at: 2026-01-15 14:15:00 IST

*12:00 - 13:00*
  • Credit Card (Recurring - Initial): 340 attempts | 94.12% SR
    ↳ GATEWAY_TIMEOUT (18 failures): txn_abc1, txn_abc2, txn_abc3
  • Debit Card (Recurring - Auto): 120 attempts | 97.50% SR
  • Prepaid (One-time): 88 attempts | 91.00% SR
    ↳ ISSUER_DECLINE (7 failures): txn_xyz1

*11:00 - 12:00*
  • Credit Card (Recurring - Initial): 302 attempts | 96.35% SR
  ...
```

Account teams see: volume, success rate, top error codes, and sample transaction IDs — all per payment segment per hour. No SQL, no dashboards, no waiting.

---

## Architecture

```
Airflow Scheduler (cron: 15 * * * *)
    └── PythonOperator: run_merchant_pulse()
            │
            ├── ClickHouse (read-only connection)
            │     ├── Query 1: success rates  ─┐
            │     └── Query 2: failure codes  ─┘ (both run ONCE for all merchants)
            │
            ├── Python grouping
            │     └── group_by_merchant() → per-merchant data slices
            │
            └── Slack (per merchant)
                  └── format_digest_message() → post to channel
```

---

## Key Design Decisions

**1. 2 queries for N merchants, not 2N**

The naive approach runs two ClickHouse queries per merchant: one for success rates, one for failure breakdown. With 26 merchants, that's 52 queries per hour — exceeding the read-only user's hourly quota.

The optimised approach runs both queries once with `merchant_id IN (...)`, then groups results in Python. 52 → 2 queries per run. A 26x reduction.

```python
ids_sql = _merchant_ids_sql(MERCHANT_IDS)
sr_rows      = client.execute(SR_QUERY.format(merchant_ids_sql=ids_sql))
failure_rows = client.execute(FAILURE_QUERY.format(merchant_ids_sql=ids_sql))
# Group in Python — O(N) dict partition, not N round trips
sr_by_mid = group_by_merchant(sr_rows)
```

**2. 3-hour rolling window with 1-hour overlap**

Each run covers the last 3 completed hours, not just the last 1. If a single run fails (ClickHouse lag, Airflow worker restart), the next run's window recovers the missed hour. No data gaps without manual re-runs.

**3. retries=0 — intentional**

Auto-retry on failure would consume additional ClickHouse quota for what is likely a transient infra issue, not a data issue. Failures surface in Slack via `on_failure_callback` instead.

**4. Nine-way payment segmentation in SQL, not Python**

Segmenting by card type × recurrence × geography in the CASE block pushes filtering to ClickHouse (columnar, fast) rather than Python. Keeps result set small before it crosses the wire.

**5. IST timezone explicit in SQL**

`toTimeZone(fromUnixTimestamp(created_at), 'Asia/Kolkata')` in every timestamp expression makes results independent of the ClickHouse server's system timezone. Avoids hour-boundary bugs when the server is on UTC.

---

## Files

| File | Description |
|---|---|
| `merchant_pulse_dag.py` | Full DAG: ClickHouse client, data parsing, message formatting, Slack posting |
| `requirements.txt` | Dependencies |

**Note on SQL:** The payment method segmentation query and failure breakdown query contain proprietary business logic (segment taxonomy, error code exclusions). They are not included in this demo repo. The query interfaces (`_load_sr_query()`, `_load_failure_breakdown_query()`) are stubbed with descriptive docstrings — available on request.

---

## Configuration

**Airflow connections required:**
- `clickhouse_readonly_default` — ClickHouse host, port, user, password, schema
- `slack_default` — Slack webhook token

**Merchant roster:**
Edit the `MERCHANTS` list in `merchant_pulse_dag.py`. Each entry needs a `merchant_id`, display `name`, and `channel_id`.

---

## Stack

- Apache Airflow 2.9+ (Airflow v3 compatible)
- ClickHouse (`clickhouse-driver`)
- Slack SDK (`slack_sdk`)
- Python 3.11+
- pytz (explicit IST handling)

---

## What I'd Change Today

- Move to a vector DB for semantic error code grouping (similar errors cluster better than exact-match)
- Add a threshold-based alert layer: if SR drops >X% vs prior hour, skip the digest and fire a P1 alert immediately
- Parameterise the merchant roster in Airflow Variables instead of hardcoding — easier to add/remove merchants without a DAG redeploy
