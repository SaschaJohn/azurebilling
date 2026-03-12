# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Start PostgreSQL
docker compose up -d

# Activate virtualenv (Python 3.13 only — 3.14 can't build psycopg2-binary)
source .venv/bin/activate

# Run migrations
FLASK_APP=run.py flask db upgrade

# Start dev server (port 5000 is taken by macOS AirPlay)
FLASK_APP=run.py flask run --port 5001 --host 127.0.0.1

# Import a CSV from the command line
FLASK_APP=run.py flask import-file path/to/billing.csv

# Upload via HTTP
curl -X POST -F "file=@sample.csv" http://127.0.0.1:5001/imports/
```

## Architecture

This is a Flask + SQLAlchemy + PostgreSQL app that imports Azure billing CSVs into a star-schema data warehouse and provides a read-only web UI for browsing costs.

### Data model (star schema)
- **`fact_billing_line`** — central fact table (BIGSERIAL PK). One row per billing line. Deduplication via `row_hash CHAR(64) UNIQUE` (SHA-256 of raw CSV row).
- **12 dimension tables** (`dim_billing_account`, `dim_billing_profile`, `dim_invoice_section`, `dim_subscription`, `dim_reseller`, `dim_publisher`, `dim_product`, `dim_meter`, `dim_service`, `dim_resource_group`, `dim_invoice`, `dim_benefit`) — all have integer surrogate PKs.
- **`import_batch`** — UUID PK, tracks every CSV upload with status, row counts, and timestamps.
- Nullable FKs on fact: `meter_fk`, `reseller_fk`, `invoice_fk`, `benefit_fk`, `resource_group_fk` (common for SaaS/marketplace rows).

### Import pipeline (`app/services/`)
1. `csv_importer.py` — streams CSV in batches of 500 rows; calls `_process_row()` per row, then `_flush()` to bulk-insert with `ON CONFLICT DO NOTHING` on `row_hash`.
2. `dimension_cache.py` — `DimensionCache` warms from DB on init, then resolves every dimension FK with an in-memory dict; misses trigger a PostgreSQL `INSERT ... ON CONFLICT DO UPDATE ... RETURNING id` upsert.
3. `hash_utils.py` — computes the SHA-256 `row_hash` from the raw CSV row dict.

**Critical**: `on_conflict_do_update` requires a non-empty `set_`. For dimensions with only key columns (e.g., `DimReseller`, `DimService`), `_upsert()` falls back to a no-op update of the first conflict column.

### Controllers
Each controller is a Flask Blueprint with a URL prefix matching its name:
- `dashboard` (`/`) — aggregate cost summaries.
- `imports` (`/imports`) — CSV upload form + import history.
- `subscriptions`, `invoices`, `resources`, `meters` — paginated index + detail views with per-column filters and sortable columns.

**Pagination pattern**: Use `db.session.query(...).paginate()` (legacy SQLAlchemy style) for aggregate queries with `GROUP BY`. `db.paginate(select(...))` uses `.scalars()` internally and only returns the first column.

### Templates
Jinja2 with a `base.html` layout. Two custom globals:
- `fmt_cost` filter — formats Decimal as `kr. 0.0000`.
- `url_with_params(**overrides)` template global — merges overrides into current query string (used for sort/filter links).

### Migrations
Alembic config lives at `alembic/alembic.ini` (inside the `alembic/` dir, not the project root). Flask-Migrate is initialized with `directory='alembic'`. Migration versions are in `alembic/versions/`.

### Environment
Copy `.env.example` to `.env`. The only required variable is `DATABASE_URL` (PostgreSQL connection string). `run.py` loads `.env` via python-dotenv before creating the Flask app.
