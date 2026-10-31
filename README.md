# Merchant Pulse — Payment Health Monitor (Airflow + ClickHouse + Slack)

> Automated payment observability agent that fetches transaction success-rate
> data from ClickHouse and posts structured Slack digests every 2 hours —
> giving account teams real-time merchant health visibility without needing
> direct data warehouse access.

---

## What problem does this solve?

Account managers at a payments company need to know — continuously — whether
their merchants' international card payments are succeeding. Before this system,
they either:

- manually queried a data warehouse (slow, requires SQL knowledge), or  
- waited for merchants to complain (too late).

This DAG runs every 2 hours and posts a structured Slack digest per merchant,
broken down by payment method, showing attempt volume, success rate, and the
top failure error codes with sample payment IDs for instant investigation.

---

## Sample Slack output

```
📊 *Merchant Alpha - Payment SR Update*
Posted at: 2026-01-15 14:15:00 IST

*12:00 - 13:00*
  • Credit Card - Recurring - Auto - International: 340 attempts | 94.12% SR
    ↳ GATEWAY_TIMEOUT (18 failures): pay_ABC1, pay_ABC2, pay_ABC3
  • Debit Card - International: 120 attempts | 97.50% SR

*11:00 - 12:00*
  • Credit Card - International: 298 attempts | 96.98% SR
```

---

## Architecture

```
ClickHouse (columnar DB)
        │
        │  2 queries per run (see optimisation below)
        ▼
  Airflow PythonOperator  (Airflow v3, worker-side)
        │
        │  group by merchant_id in Python
        ▼
  Slack Webhook  ──►  per-merchant channel
```

### Key design decisions

| Decision | Rationale |
|---|---|
| **2 queries total** instead of `2 × N_merchants` | ClickHouse read-only user has an hourly query quota. With 26 merchants the naïve approach hits `52 queries/run`; this approach uses `2`. |
| **`retries=0`** | A retry after quota exhaustion makes things worse, not better. Fail fast; let on-call investigate. |
| **1-hour overlap buffer** | Window covers the last 3 completed hours. If one run fails, the next run's window still covers the missed hour. |
| **Explicit `toTimeZone(..., 'Asia/Kolkata')`** | ClickHouse server timezone is unreliable in cloud deployments. IST is baked into every timestamp operation. |
| **SQL CASE segmentation** | Nine-way card-type × recurrence × geography segmentation happens in ClickHouse (not Python), keeping the data transfer minimal. |
| **Worker-side execution** | `lib.slack.notify` builds a `SlackWebhookOperator` and calls `.execute()` with a live task context; `BaseHook.get_connection` needs the Airflow metadata DB. Both require the worker, not a Kubernetes pod. |

---

## Payment method segmentation

Each transaction is classified into one of 9 buckets:

| Card type | Recurrence | Geography | Bucket |
|---|---|---|---|
| Credit | Recurring – Initial | International | `Credit Card - Recurring - Initial - International` |
| Credit | Recurring – Auto | International | `Credit Card - Recurring - Auto - International` |
| Credit | One-time | International | `Credit Card - International` |
| Debit | Recurring – Initial | International | `Debit Card - Recurring - Initial - International` |
| Debit | Recurring – Auto | International | `Debit Card - Recurring - Auto - International` |
| Debit | One-time | International | `Debit Card - International` |
| Prepaid | Recurring – Initial | International | `Prepaid Card - Recurring - Initial - International` |
| Prepaid | Recurring – Auto | International | `Prepaid Card - Recurring - Auto - International` |
| Prepaid | One-time | International | `Prepaid Card - International` |

"International" includes Amex cards issued outside India regardless of the
`international` flag.

---

## Stack

| Component | Technology |
|---|---|
| Orchestration | Apache Airflow v3 |
| Data warehouse | ClickHouse |
| ClickHouse driver | `clickhouse-driver` |
| Alerting | Slack Webhooks (via Airflow `SlackWebhookOperator`) |
| Timezone handling | `pytz` |
| Language | Python 3.11+ |

---

## Configuration

All credentials live in the Airflow connection store — no secrets in DAG code.

| Airflow Connection ID | What it holds |
|---|---|
| `clickhouse_readonly_default` | ClickHouse host, port, read-only user credentials, database |

Slack webhook URLs are stored as Airflow connections and resolved by
`lib.slack.notify` at runtime.

---

## Running locally (development)

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Set up a local Airflow environment
export AIRFLOW_HOME=~/airflow
airflow db migrate
airflow connections add clickhouse_readonly_default \
    --conn-type generic \
    --conn-host localhost \
    --conn-port 9000 \
    --conn-login readonly_user \
    --conn-password your_password \
    --conn-schema default

# 3. Run the DAG task directly (dry-run)
airflow tasks test sr_poster_clickhouse post_sr_updates 2026-01-15
```

---

## File structure

```
sr-alert-bot/
├── sr_poster_dag.py      # Main Airflow DAG
├── requirements.txt      # Python dependencies
└── README.md             # This file
```

> **Note:** `lib/slack.py` and `lib/email.py` are internal helpers that wrap
> `SlackWebhookOperator` and Airflow email utilities. They are not included here
> as they contain organisation-specific routing logic.

---

## What I'd add next

- [ ] Prometheus metrics export (attempt volume + SR per method as gauges)
- [ ] Configurable SR threshold alerts (e.g. page on-call if SR drops below 90%)
- [ ] BigQuery sink for long-term trend analysis
- [ ] Unit tests for `build_error_map` and `format_sr_message`

---

*Built as part of a payments observability platform. Merchant IDs, channel IDs,
and internal table names have been anonymised.*
