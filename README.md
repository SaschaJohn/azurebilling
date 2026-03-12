# Azure Billing POC

A Flask + PostgreSQL web application that imports Azure billing CSVs into a star-schema data warehouse and provides a read-only web UI for browsing and analyzing costs.

---

## Tech Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| Language | Python | 3.13 (3.14 is incompatible — cannot build `psycopg2-binary`) |
| Web framework | Flask | 3.1.0 |
| ORM | SQLAlchemy | 2.0.36 |
| ORM integration | Flask-SQLAlchemy | 3.1.1 |
| Database migrations | Alembic + Flask-Migrate | 1.14.0 / 4.0.7 |
| Database driver | psycopg2-binary | 2.9.10 |
| Database | PostgreSQL | 17 (via Docker) |
| Environment | python-dotenv | 1.0.1 |
| Production server | gunicorn | 23.0.0 |
| Container | Docker Compose | — |
| Templates | Jinja2 | (bundled with Flask) |

---

## Repository Structure

```
azurebillingpoc/
│
├── docker-compose.yml          # PostgreSQL 17 service, port 5432, named volume pgdata
├── .env.example                # Environment variable template (copy to .env)
├── .gitignore
├── requirements.txt            # Python dependencies
├── run.py                      # FLASK_APP entry point; loads .env via python-dotenv
│
├── alembic/                    # Database migration configuration and versions
│   ├── alembic.ini             # Alembic config (MUST live here, not at project root)
│   ├── env.py                  # Flask-Migrate-compatible Alembic environment
│   ├── script.py.mako          # Migration file template
│   └── versions/
│       ├── 0001_create_dimensions.py   # All 12 dimension tables
│       ├── 0002_create_import_batch.py # Import audit/tracking table
│       └── 0003_create_fact_table.py   # Central fact table + indexes
│
└── app/                        # Main application package
    ├── __init__.py             # create_app() factory; blueprints, Jinja globals, middleware
    ├── config.py               # Flask config object (reads DATABASE_URL etc.)
    ├── extensions.py           # Shared db and migrate instances (avoids circular imports)
    │
    ├── models/                 # SQLAlchemy ORM model definitions
    │   ├── dimensions.py       # 12 dimension table models (DimBillingAccount, DimMeter, …)
    │   ├── fact.py             # FactBillingLine — central fact table model
    │   ├── import_batch.py     # ImportBatch — tracks every CSV upload
    │   └── exchange_rate.py    # ExchangeRate — DKK→X currency conversion rates
    │
    ├── services/               # Business logic and import pipeline
    │   ├── csv_importer.py     # Streams CSV in batches of 500; bulk-inserts with dedup
    │   ├── dimension_cache.py  # In-memory FK cache; upserts dims on cache miss
    │   └── hash_utils.py       # SHA-256 row_hash computation for deduplication
    │
    ├── controllers/            # Flask Blueprints — one file per URL prefix
    │   ├── __init__.py
    │   ├── dashboard.py        # GET /  — aggregate cost summary
    │   ├── imports.py          # GET/POST /imports/ — CSV upload + import history
    │   ├── subscriptions.py    # GET /subscriptions/ and /subscriptions/<id>
    │   ├── invoices.py         # GET /invoices/ and /invoices/<id>
    │   ├── resources.py        # GET /resources/ and /resources/<id>
    │   ├── meters.py           # GET /meters/ and /meters/<id>
    │   ├── storage.py          # GET /storage/ and /storage/<id> — storage account detail
    │   └── exchange_rates.py   # GET/POST /exchange-rates/ — manage currency rates
    │
    ├── templates/              # Jinja2 HTML templates
    │   ├── base.html           # Shared layout: navbar, currency selector, month filter
    │   ├── _pagination.html    # Reusable pagination partial
    │   ├── _line_items_table.html  # Reusable billing line items table
    │   ├── _macros.html        # Shared Jinja macros
    │   ├── dashboard/
    │   │   └── index.html
    │   ├── imports/
    │   │   ├── index.html      # Import history list
    │   │   └── upload.html     # File upload form with progress feedback
    │   ├── subscriptions/
    │   │   ├── index.html
    │   │   └── detail.html
    │   ├── invoices/
    │   │   ├── index.html
    │   │   └── detail.html
    │   ├── resources/
    │   │   ├── index.html
    │   │   └── detail.html
    │   ├── meters/
    │   │   ├── index.html
    │   │   └── detail.html
    │   ├── storage/
    │   │   ├── index.html
    │   │   └── detail.html     # Monthly cost pivot by meter
    │   └── exchange_rates/
    │       ├── index.html
    │       └── form.html
    │
    └── cli.py                  # Flask CLI command: `flask import-file <path>`
```

---

## Database Schema

The schema follows a **star schema** design: one central fact table surrounded by dimension lookup tables.

### Dimension Tables (12)

All dimension tables use integer surrogate PKs. They are populated on first-seen via `INSERT ... ON CONFLICT DO UPDATE ... RETURNING id` upserts, with an in-memory cache in front to avoid redundant DB round-trips.

| Table | Natural Key | Purpose |
|-------|------------|---------|
| `dim_billing_account` | `billing_account_id` | Azure billing account |
| `dim_billing_profile` | `billing_profile_id` | Billing profile within an account |
| `dim_invoice_section` | `invoice_section_id` | Invoice section within a profile |
| `dim_subscription` | `subscription_id` | Azure subscription |
| `dim_reseller` | `(name, mpn_id)` | Reseller / partner info |
| `dim_publisher` | `(publisher_type, publisher_id)` | Marketplace publisher |
| `dim_product` | `(product_id, product_order_id)` | Azure product / offer |
| `dim_meter` | `meter_id` | Billing meter (service unit) |
| `dim_service` | `(service_family, consumed_service)` | Azure service category |
| `dim_resource_group` | `resource_id` | Azure resource group |
| `dim_invoice` | `invoice_id` | Invoice document |
| `dim_benefit` | `(benefit_id, reservation_id)` | Reserved capacity / savings plans |

### Audit Table

`import_batch` — UUID PK. Tracks every CSV upload with `filename`, `started_at`, `finished_at`, `row_count`, `skipped_count`, `status`, and `error_msg`.

### Fact Table

`fact_billing_line` — BIGSERIAL PK. One row per CSV billing line. FKs reference all 12 dimension tables. Five FKs are nullable (`reseller_fk`, `meter_fk`, `resource_group_fk`, `invoice_fk`, `benefit_fk`) because those fields are empty for SaaS and marketplace rows.

Key columns:

| Column | Type | Notes |
|--------|------|-------|
| `row_hash` | `CHAR(64) UNIQUE` | SHA-256 of raw CSV row — used for deduplication |
| `charge_date` | `DATE NOT NULL` | Primary date for filtering |
| `billing_period_start/end_date` | `DATE` | Nullable |
| `cost_in_billing_currency` | `NUMERIC(28,10)` | Primary cost column |
| `additional_info`, `tags` | `JSONB` | GIN-indexed for flexible querying |

Indexes created by migration 0003:

```sql
idx_fact_charge_date                    -- charge_date
idx_fact_billing_period                 -- (billing_period_start_date, billing_period_end_date)
idx_fact_subscription_fk               -- subscription_fk
idx_fact_import_batch_id               -- import_batch_id
idx_fact_tags_gin          (GIN)       -- tags JSONB
idx_fact_additional_info_gin (GIN)     -- additional_info JSONB
```

### Exchange Rate Table

`exchange_rate` — stores DKK → foreign currency conversion rates keyed by `billing_month`. Used by the navbar currency selector to convert displayed costs at query time.

---

## Migrations

Migrations are managed with **Alembic** via **Flask-Migrate**. The `alembic/` directory acts as the migrations root.

> **Important:** `alembic.ini` lives inside `alembic/` (not the project root). Flask-Migrate is initialized with `directory='alembic'`, and `alembic.ini` uses `script_location = .` to resolve paths relative to itself.

Migration chain:

```
0001_create_dimensions.py    (down_revision: None)
        |
0002_create_import_batch.py  (down_revision: 0001)
        |
0003_create_fact_table.py    (down_revision: 0002)
```

Common commands:

```bash
# Apply all pending migrations
FLASK_APP=run.py flask db upgrade

# Roll back one step
FLASK_APP=run.py flask db downgrade

# Show current revision
FLASK_APP=run.py flask db current

# Generate a new migration (after changing models)
FLASK_APP=run.py flask db migrate -m "description"
```

---

## Import Pipeline

The CSV import pipeline lives in `app/services/` and is designed to handle large files efficiently without loading everything into memory.

```
CSV file (binary stream)
        |
        v
  csv_importer.py
  - Wraps stream in TextIOWrapper (UTF-8 BOM-safe)
  - Reads with csv.DictReader
  - Processes rows in BATCH_SIZE=500 chunks
        |
        v (per row)
  hash_utils.py
  - SHA-256 of the raw dict → row_hash
        |
        v
  dimension_cache.py  (DimensionCache)
  - Warms all dimension PKs from DB on init
  - Resolves each FK from in-memory dict
  - On cache miss: PostgreSQL INSERT ... ON CONFLICT DO UPDATE RETURNING id
        |
        v
  _flush() in csv_importer.py
  - Bulk INSERT via PostgreSQL dialect pg_insert()
  - ON CONFLICT DO NOTHING on row_hash (dedup)
  - result.rowcount → tracks new rows vs. duplicates
```

Re-uploading the same CSV results in `row_count=0` and all rows counted as `skipped_count`.

---

## Setup & Running

### Prerequisites

- Python 3.13
- Docker + Docker Compose

### First-time setup

```bash
# 1. Start PostgreSQL
docker compose up -d

# 2. Create and activate virtualenv
python3.13 -m venv .venv
source .venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure environment
cp .env.example .env
# Edit .env and set DATABASE_URL

# 5. Run migrations
FLASK_APP=run.py flask db upgrade

# 6. Start the dev server (port 5001 — port 5000 is taken by macOS AirPlay)
FLASK_APP=run.py flask run --port 5001 --host 127.0.0.1
```

### Importing billing data

```bash
# Via CLI
FLASK_APP=run.py flask import-file path/to/billing.csv

# Via HTTP upload
curl -X POST -F "file=@billing.csv" http://127.0.0.1:5001/imports/
```

### Environment variables

Copy `.env.example` to `.env`. Required variables:

| Variable | Example | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgresql://billing:billing@localhost:5432/azurebilling` | PostgreSQL connection string |

---

## Web UI

All views are read-only except the import upload and exchange rate management pages.

| URL | Description |
|-----|-------------|
| `/` | Dashboard — aggregate cost totals |
| `/imports/` | Import history; upload new CSV |
| `/subscriptions/` | Paginated subscription cost index |
| `/invoices/` | Paginated invoice index |
| `/resources/` | Paginated resource group index |
| `/meters/` | Paginated meter index |
| `/storage/` | Storage account cost overview |
| `/exchange-rates/` | Manage DKK → currency conversion rates |

The navbar exposes a **global month filter** (persisted in session) and a **currency selector** that converts all displayed costs on the fly using the stored exchange rates.

### Template globals

- `fmt_cost` filter — formats a `Decimal` value with currency symbol and exchange rate conversion, e.g. `kr. 1,234.56`.
- `url_with_params(**overrides)` — merges keyword overrides into the current query string; used by sort and filter links to preserve existing parameters.
