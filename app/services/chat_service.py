import re

from flask import current_app
from openai import AzureOpenAI
from sqlalchemy import text

from app.extensions import db

SCHEMA_CONTEXT = """You are a PostgreSQL expert. The database is an Azure billing star schema.

TABLES:
fact_billing_line (one row per billing line item)
  - id BIGSERIAL PK
  - charge_date DATE (not null)
  - cost_in_billing_currency NUMERIC  ← primary cost column
  - cost_in_usd NUMERIC
  - quantity NUMERIC
  - unit_of_measure TEXT
  - charge_type TEXT  (e.g. 'Usage', 'Purchase', 'Tax')
  - pricing_model TEXT  (e.g. 'OnDemand', 'Reservation', 'Savings Plan')
  - resource_location TEXT
  - service_info1 TEXT  (contains tier/sku details like 'Developer', 'Standard')
  - service_info2 TEXT
  - additional_info JSONB  (raw Azure JSON metadata)
  - tags JSONB
  -- FK columns: billing_account_fk, billing_profile_fk, invoice_section_fk,
  --             subscription_fk, reseller_fk (nullable), publisher_fk,
  --             product_fk, meter_fk (nullable), service_fk,
  --             resource_group_fk (nullable), invoice_fk (nullable),
  --             benefit_fk (nullable)

dim_subscription
  - id INT PK
  - subscription_id TEXT UNIQUE
  - subscription_name TEXT

dim_resource_group
  - id INT PK
  - resource_id TEXT UNIQUE  (the full Azure resource ID path)
  - resource_group_name TEXT
  - subscription_fk INT → dim_subscription.id

dim_service
  - id INT PK
  - service_family TEXT  (e.g. 'Compute', 'Networking', 'Storage', 'Databases')
  - consumed_service TEXT  (e.g. 'Microsoft.ApiManagement', 'Microsoft.Storage')

dim_meter
  - id INT PK
  - meter_id TEXT UNIQUE
  - meter_name TEXT  (e.g. 'Write Operations', 'Read Operations', 'Transactions')
  - meter_category TEXT  (e.g. 'Storage', 'API Management')
  - meter_sub_category TEXT
  - meter_region TEXT

dim_product
  - id INT PK
  - product_name TEXT
  - product_order_name TEXT

dim_publisher
  - id INT PK
  - publisher_name TEXT
  - publisher_type TEXT
  - publisher_id TEXT

dim_billing_account  - id INT PK, name TEXT
dim_billing_profile  - id INT PK, name TEXT
dim_invoice_section  - id INT PK, name TEXT
dim_invoice          - id INT PK, invoice_id TEXT
dim_benefit          - id INT PK, benefit_name TEXT, benefit_id TEXT
dim_reseller         - id INT PK, name TEXT, mpn_id TEXT

TIPS FOR AZURE BILLING QUERIES:
- "API Management" → dim_meter.meter_category = 'API Management' OR dim_service.consumed_service ILIKE '%ApiManagement%'
- Tiers/SKUs are often in fact_billing_line.service_info1 or dim_meter.meter_sub_category
- Storage write transactions → dim_meter.meter_name ILIKE '%Write%' AND dim_meter.meter_category ILIKE '%Storage%'
- Resource names: dim_resource_group.resource_id contains the full path; resource_group_name is the group name
- Always use SUM(cost_in_billing_currency) for costs
- Use DISTINCT or GROUP BY to count unique resources
- For "instances" of a service, count DISTINCT resource_group_fk or parse resource_id
- Join fact_billing_line to dimension tables using the _fk columns
"""


def _client():
    cfg = current_app.config
    return AzureOpenAI(
        azure_endpoint=cfg['AZURE_OPENAI_ENDPOINT'],
        api_key=cfg['AZURE_OPENAI_API_KEY'],
        api_version=cfg['AZURE_OPENAI_API_VERSION'],
    )


def _deployment():
    return current_app.config['AZURE_OPENAI_DEPLOYMENT']


def _month_clause(active_month) -> str:
    if active_month is None:
        return "all months (no date filter)"
    return f"the billing month {active_month.strftime('%Y-%m')} (filter: charge_date >= '{active_month.strftime('%Y-%m-01')}' AND charge_date < next month)"


def _generate_sql(question: str, active_month) -> str | None:
    month_hint = _month_clause(active_month)
    system = (
        SCHEMA_CONTEXT
        + f"\n\nThe user is currently viewing {month_hint}. "
        "Unless the user explicitly asks for all months, add an appropriate WHERE clause to filter by that month. "
        "Return ONLY a valid PostgreSQL SELECT query. No markdown, no explanation, no code fences."
    )
    response = _client().chat.completions.create(
        model=_deployment(),
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": question},
        ],
        temperature=0,
        max_completion_tokens=1000,
    )
    sql = response.choices[0].message.content.strip()
    # Strip accidental markdown fences
    sql = re.sub(r'^```(?:sql)?\s*', '', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\s*```$', '', sql)
    sql = sql.strip()
    return sql if sql else None


def _is_safe_sql(sql: str) -> bool:
    # Normalize: strip comments and excess whitespace, uppercase
    normalized = re.sub(r'--[^\n]*', ' ', sql)         # strip -- comments
    normalized = re.sub(r'/\*.*?\*/', ' ', normalized, flags=re.DOTALL)  # strip /* */ comments
    normalized = normalized.strip().upper()
    if not normalized.startswith('SELECT'):
        return False
    dangerous = re.compile(
        r'\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|GRANT|REVOKE|EXECUTE|EXEC|COPY|CALL)\b'
    )
    return not dangerous.search(normalized)


def _add_limit(sql: str, max_rows: int) -> str:
    # Append LIMIT if not already present
    normalized = sql.upper()
    if 'LIMIT' not in normalized:
        return sql.rstrip().rstrip(';') + f' LIMIT {max_rows}'
    return sql


def _execute_sql(sql: str, max_rows: int = 100):
    sql_with_limit = _add_limit(sql, max_rows)
    result = db.session.execute(text(sql_with_limit))
    columns = list(result.keys())
    rows = [dict(zip(columns, row)) for row in result.fetchall()]
    return rows, columns


def _format_answer(question: str, sql: str, rows: list, columns: list) -> str:
    if not rows:
        return "The query returned no results."

    # Serialize rows as a simple text table (cap at 50 for the prompt)
    preview = rows[:50]
    header = ' | '.join(str(c) for c in columns)
    lines = [header, '-' * len(header)]
    for row in preview:
        lines.append(' | '.join(str(v) for v in row.values()))
    table_text = '\n'.join(lines)
    if len(rows) > 50:
        table_text += f'\n... ({len(rows)} rows total, showing first 50)'

    system = (
        "You are a helpful Azure billing assistant. "
        "Given the user's question, the SQL that was executed, and the results, "
        "produce a clear, concise natural language answer. "
        "Format numbers with commas and 2 decimal places where relevant. "
        "Do not repeat the SQL in your answer."
    )
    user_content = (
        f"Question: {question}\n\n"
        f"SQL executed:\n{sql}\n\n"
        f"Results ({len(rows)} rows):\n{table_text}"
    )
    response = _client().chat.completions.create(
        model=_deployment(),
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user_content},
        ],
        temperature=0.3,
        max_completion_tokens=800,
    )
    return response.choices[0].message.content.strip()


def ask(question: str, active_month=None) -> dict:
    try:
        sql = _generate_sql(question, active_month)
    except Exception as e:
        return {"answer": f"Failed to generate SQL: {e}", "sql": None}

    if not sql:
        return {"answer": "I couldn't generate a valid SQL query for that question.", "sql": None}

    if not _is_safe_sql(sql):
        return {"answer": "I can only run SELECT queries. The generated query contained unsafe operations.", "sql": sql}

    try:
        rows, columns = _execute_sql(sql, max_rows=100)
    except Exception as e:
        return {"answer": f"The query failed to execute: {e}", "sql": sql}

    try:
        answer = _format_answer(question, sql, rows, columns)
    except Exception as e:
        return {"answer": f"Query returned {len(rows)} rows but formatting failed: {e}", "sql": sql, "row_count": len(rows)}

    return {"answer": answer, "sql": sql, "row_count": len(rows)}
