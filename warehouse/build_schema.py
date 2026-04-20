import sqlite3
import pandas as pd
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)s  %(message)s',
    handlers=[
        logging.FileHandler("etl/etl_run.log", mode='a'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

DB_PATH = "warehouse/bis_warehouse.db"

# ── DDL: Create all warehouse tables ──────────────────────────────────────────

DDL = """

-- ── Dimension: Date ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_date (
    date_key        INTEGER PRIMARY KEY,   -- YYYYMMDD integer key
    full_date       TEXT,
    day             INTEGER,
    month           INTEGER,
    month_name      TEXT,
    quarter_name    TEXT,                  -- Q1/Q2/Q3/Q4 (calendar)
    year            INTEGER,
    uk_fiscal_year  TEXT,                  -- e.g. FY2015/16
    uk_fiscal_quarter TEXT,               -- Q1=Apr-Jun ... Q4=Jan-Mar
    half_year       TEXT                   -- H1 (Apr-Sep) or H2 (Oct-Mar)
);

-- ── Dimension: Supplier ───────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_supplier (
    supplier_key        INTEGER PRIMARY KEY AUTOINCREMENT,
    supplier_name_raw   TEXT,
    supplier_name_clean TEXT,
    supplier_postcode   TEXT,
    supplier_type       TEXT
);

-- ── Dimension: Expense ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_expense (
    expense_key         INTEGER PRIMARY KEY AUTOINCREMENT,
    expense_type        TEXT,
    expense_area        TEXT,
    expenditure_type    TEXT
);

-- ── Dimension: Entity ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_entity (
    entity_key          INTEGER PRIMARY KEY AUTOINCREMENT,
    department          TEXT,
    entity_name         TEXT,
    entity_group        TEXT,
    entity_subgroup     TEXT
);

-- ── Fact: Transactions ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_transactions (
    transaction_key     INTEGER PRIMARY KEY AUTOINCREMENT,
    row_id              TEXT,
    date_key            INTEGER REFERENCES dim_date(date_key),
    supplier_key        INTEGER REFERENCES dim_supplier(supplier_key),
    expense_key         INTEGER REFERENCES dim_expense(expense_key),
    entity_key          INTEGER REFERENCES dim_entity(entity_key),
    transaction_number  TEXT,
    amount              REAL,
    is_refund           INTEGER,           -- 0/1 boolean
    description         TEXT,
    source_file         TEXT,
    load_timestamp      TEXT
);
"""

# ── Populate dim_date ──────────────────────────────────────────────────────────

def build_dim_date(conn):
    log.info("Building dim_date...")
    df = pd.read_sql(
        "SELECT DISTINCT payment_date, payment_year, payment_month, "
        "payment_month_name, uk_fiscal_year, uk_fiscal_quarter "
        "FROM stg_transactions WHERE payment_date IS NOT NULL",
        conn
    )
    df['full_date']   = pd.to_datetime(df['payment_date'])
    df['date_key']    = df['full_date'].dt.strftime('%Y%m%d').astype(int)
    df['day']         = df['full_date'].dt.day
    df['quarter_name']= df['full_date'].dt.quarter.apply(lambda q: f"Q{q}")
    df['half_year']   = df['payment_month'].apply(
        lambda m: 'H1' if m in [4,5,6,7,8,9] else 'H2'
    )
    df = df.rename(columns={
        'payment_year':       'year',
        'payment_month':      'month',
        'payment_month_name': 'month_name'
    })
    df = df[[
        'date_key','full_date','day','month','month_name',
        'quarter_name','year','uk_fiscal_year','uk_fiscal_quarter','half_year'
    ]].drop_duplicates(subset='date_key')

    df['full_date'] = df['full_date'].dt.strftime('%Y-%m-%d')
    df.to_sql('dim_date', conn, if_exists='append', index=False, chunksize=500)
    log.info(f"  dim_date populated: {len(df)} date records")

# ── Populate dim_supplier ─────────────────────────────────────────────────────

def build_dim_supplier(conn):
    log.info("Building dim_supplier...")
    df = pd.read_sql(
        """SELECT DISTINCT
               supplier_name      AS supplier_name_raw,
               supplier_name_clean,
               supplier_postcode,
               supplier_type
           FROM stg_transactions
           WHERE supplier_name IS NOT NULL""",
        conn
    )
    df.drop_duplicates(
        subset=['supplier_name_raw','supplier_postcode'], inplace=True
    )
    df.to_sql(
        'dim_supplier', conn, if_exists='append', index=False, chunksize=500
    )
    log.info(f"  dim_supplier populated: {len(df)} supplier records")

# ── Populate dim_expense ──────────────────────────────────────────────────────

def build_dim_expense(conn):
    log.info("Building dim_expense...")
    df = pd.read_sql(
        """SELECT DISTINCT
               expense_type,
               expense_area,
               expenditure_type
           FROM stg_transactions
           WHERE expense_type IS NOT NULL
              OR expense_area IS NOT NULL""",
        conn
    )
    df.drop_duplicates(
        subset=['expense_type','expense_area'], inplace=True
    )
    df.to_sql(
        'dim_expense', conn, if_exists='append', index=False, chunksize=500
    )
    log.info(f"  dim_expense populated: {len(df)} expense category records")

def build_dim_entity(conn):
    log.info("Building dim_entity...")
    df = pd.read_sql(
        """SELECT DISTINCT department, expense_area
           FROM stg_transactions
           WHERE expense_area IS NOT NULL""",
        conn
    )
    df.drop_duplicates(subset=['expense_area'], inplace=True)

    # Split "Group - Subgroup" pattern into two columns
    # e.g. "Business & Local Growth - Regional Growth"
    #       → group="Business & Local Growth", subgroup="Regional Growth"
    split = df['expense_area'].str.split(' - ', n=1, expand=True)
    df['entity_group']    = split[0].str.strip()
    df['entity_subgroup'] = split[1].str.strip() if 1 in split.columns else None
    df = df.rename(columns={'expense_area': 'entity_name'})
    df = df[['department','entity_name','entity_group','entity_subgroup']]

    df.to_sql(
        'dim_entity', conn, if_exists='append', index=False, chunksize=500
    )
    log.info(f"  dim_entity populated: {len(df)} entity/cost-center records")

# ── Populate fact_transactions ────────────────────────────────────────────────

def build_fact_transactions(conn):
    log.info("Building fact_transactions...")

    stg = pd.read_sql("SELECT * FROM stg_transactions", conn)
    log.info(f"  Loaded {len(stg):,} rows from staging")

    # Load dimension keys
    dim_date     = pd.read_sql(
        "SELECT date_key, full_date FROM dim_date", conn
    )
    dim_supplier = pd.read_sql(
        "SELECT supplier_key, supplier_name_raw, supplier_postcode FROM dim_supplier",
        conn
    )
    dim_expense  = pd.read_sql(
        "SELECT expense_key, expense_type, expense_area FROM dim_expense",
        conn
    )
    dim_entity   = pd.read_sql(
        "SELECT entity_key, entity_name FROM dim_entity",
        conn
    )

    # ── Join date key ──────────────────────────────────────────────────────────
    stg['payment_date_str'] = pd.to_datetime(
        stg['payment_date'], errors='coerce'
    ).dt.strftime('%Y-%m-%d')

    stg = stg.merge(
        dim_date.rename(columns={'full_date': 'payment_date_str'}),
        on='payment_date_str', how='left'
    )

    # ── Join supplier key ──────────────────────────────────────────────────────
    stg = stg.merge(
        dim_supplier,
        left_on=['supplier_name','supplier_postcode'],
        right_on=['supplier_name_raw','supplier_postcode'],
        how='left'
    )

    # ── Join expense key ───────────────────────────────────────────────────────
    stg = stg.merge(
        dim_expense,
        on=['expense_type','expense_area'],
        how='left'
    )

    # ── Join entity key ────────────────────────────────────────────────────────
    dim_entity_join = dim_entity[['entity_key','entity_name']].rename(
        columns={'entity_name': 'expense_area'}
    )
    stg = stg.merge(dim_entity_join, on='expense_area', how='left')


    # ── Build fact table ───────────────────────────────────────────────────────
    fact = stg[[
        'row_id', 'date_key', 'supplier_key', 'expense_key', 'entity_key',
        'transaction_number', 'amount', 'is_refund',
        'description', '_source_file', '_load_timestamp'
    ]].rename(columns={
        '_source_file':    'source_file',
        '_load_timestamp': 'load_timestamp'
    })

    fact['is_refund'] = fact['is_refund'].astype(int)

    # Check for unresolved keys
    for key_col in ['date_key','supplier_key','expense_key','entity_key']:
        nulls = fact[key_col].isnull().sum()
        if nulls > 0:
            log.warning(f"  {nulls} rows with NULL {key_col} — review join logic")

    fact.to_sql(
        'fact_transactions', conn,
        if_exists='append', index=False, chunksize=500
    )
    log.info(f"  fact_transactions populated: {len(fact):,} rows")

# ── Validation Query ──────────────────────────────────────────────────────────

def validate_warehouse(conn):
    log.info("\n" + "=" * 60)
    log.info("WAREHOUSE VALIDATION")
    log.info("=" * 60)

    checks = {
        "fact_transactions rows" : "SELECT COUNT(*) FROM fact_transactions",
        "dim_date rows"          : "SELECT COUNT(*) FROM dim_date",
        "dim_supplier rows"      : "SELECT COUNT(*) FROM dim_supplier",
        "dim_expense rows"       : "SELECT COUNT(*) FROM dim_expense",
        "dim_entity rows"        : "SELECT COUNT(*) FROM dim_entity",
        "Null date_keys"         : "SELECT COUNT(*) FROM fact_transactions WHERE date_key IS NULL",
        "Null supplier_keys"     : "SELECT COUNT(*) FROM fact_transactions WHERE supplier_key IS NULL",
        "Null expense_keys"      : "SELECT COUNT(*) FROM fact_transactions WHERE expense_key IS NULL",
        "Total refund rows"      : "SELECT COUNT(*) FROM fact_transactions WHERE is_refund = 1",
    }

    for label, query in checks.items():
        result = conn.execute(query).fetchone()[0]
        log.info(f"  {label:<30} {result:>10,}")

    log.info("\n  Top 5 suppliers by net spend:")
    top = pd.read_sql("""
        SELECT s.supplier_name_clean,
               ROUND(SUM(f.amount), 2) AS net_spend,
               COUNT(*) AS transactions
        FROM fact_transactions f
        JOIN dim_supplier s ON f.supplier_key = s.supplier_key
        GROUP BY s.supplier_name_clean
        ORDER BY net_spend DESC
        LIMIT 5
    """, conn)
    print(top.to_string(index=False))

    log.info("\n  Spend by UK Fiscal Quarter:")
    fiscal = pd.read_sql("""
        SELECT d.uk_fiscal_year,
               d.uk_fiscal_quarter,
               COUNT(*) AS transactions,
               ROUND(SUM(f.amount), 2) AS net_spend
        FROM fact_transactions f
        JOIN dim_date d ON f.date_key = d.date_key
        GROUP BY d.uk_fiscal_year, d.uk_fiscal_quarter
        ORDER BY d.uk_fiscal_year, d.uk_fiscal_quarter
    """, conn)
    print(fiscal.to_string(index=False))

# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    conn = sqlite3.connect(DB_PATH)

    log.info("=" * 60)
    log.info("WAREHOUSE SCHEMA BUILD STARTED")
    log.info("=" * 60)

    # Drop existing warehouse tables (safe re-run)
    for table in ['fact_transactions','dim_date',
                  'dim_supplier','dim_expense','dim_entity']:
        conn.execute(f"DROP TABLE IF EXISTS {table}")
    conn.commit()

    # Create schema
    conn.executescript(DDL)
    conn.commit()
    log.info("Schema created.")

    # Populate dimensions first, then fact
    build_dim_date(conn)
    build_dim_supplier(conn)
    build_dim_expense(conn)
    build_dim_entity(conn)
    conn.commit()

    build_fact_transactions(conn)
    conn.commit()

    validate_warehouse(conn)

    conn.close()
    log.info("\nWarehouse build complete.")

if __name__ == "__main__":
    run()