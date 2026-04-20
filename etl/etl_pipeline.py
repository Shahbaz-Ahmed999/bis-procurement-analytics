import pandas as pd
import os
import re
import sqlite3
import logging
from datetime import datetime

# ── Logging setup ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)s  %(message)s',
    handlers=[
        logging.FileHandler("etl/etl_run.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
RAW_DIR      = "data/raw"
PROCESSED_DIR = "data/processed"
DB_PATH      = "warehouse/bis_warehouse.db"

os.makedirs(PROCESSED_DIR, exist_ok=True)
os.makedirs("warehouse", exist_ok=True)

# ── Canonical column mapping ───────────────────────────────────────────────────
# Maps any variant found in source files → our standard name
COLUMN_MAP = {
    "department"          : "department",
    "entity"              : "entity",
    "date of payment"     : "payment_date",
    "expense type"        : "expense_type",
    "expense area"        : "expense_area",
    "supplier"            : "supplier_name",
    "transaction number"  : "transaction_number",
    "amount"              : "amount",
    "description"         : "description",
    "supplier post code"  : "supplier_postcode",
    "supplier type"       : "supplier_type",
    "contract number"     : "contract_number",
    "project code"        : "project_code",
    "expenditure type"    : "expenditure_type",
}

# ── EXTRACT ────────────────────────────────────────────────────────────────────
def extract(filepath: str, filename: str) -> pd.DataFrame:
    """Load a single raw CSV into a DataFrame with source metadata."""
    df = pd.read_csv(filepath, encoding='latin1', dtype=str)
    df['_source_file'] = filename
    df['_load_timestamp'] = datetime.now().isoformat()
    log.info(f"  Extracted {len(df):>5} rows from {filename}")
    return df

# ── TRANSFORM ──────────────────────────────────────────────────────────────────
def transform(df: pd.DataFrame, filename: str) -> pd.DataFrame:
    log.info(f"  Transforming {filename}...")
    original_rows = len(df)

    # 1. Normalize column names
    df.columns = [c.strip().lower() for c in df.columns]
    df.rename(columns=COLUMN_MAP, inplace=True)

    # 2. Drop rows where ALL core fields are null (blank/footer rows)
    core_cols = ['department', 'supplier_name', 'amount', 'payment_date']
    existing_core = [c for c in core_cols if c in df.columns]
    df.dropna(subset=existing_core, how='all', inplace=True)
    blank_rows_dropped = original_rows - len(df)
    if blank_rows_dropped > 0:
        log.info(f"    Dropped {blank_rows_dropped} blank/footer rows")

    # 3. Add any missing canonical columns as NaN
    for col in COLUMN_MAP.values():
        if col not in df.columns:
            df[col] = None
            log.info(f"    Added missing column: {col}")

    # 4. Clean and cast Amount
    df['amount'] = (
        df['amount']
        .astype(str)
        .str.replace(',', '', regex=False)
        .str.strip()
    )
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
    unparseable = df['amount'].isnull().sum()
    if unparseable > 0:
        log.warning(f"    {unparseable} rows with unparseable Amount — set to NaN")

    # 5. Parse dates (UK format dd/mm/yyyy)
    df['payment_date'] = pd.to_datetime(
        df['payment_date'], dayfirst=True, errors='coerce'
    )
    bad_dates = df['payment_date'].isnull().sum()
    if bad_dates > 0:
        log.warning(f"    {bad_dates} rows with unparseable dates")

    # 6. Derive date components
    df['payment_year']    = df['payment_date'].dt.year
    df['payment_month']   = df['payment_date'].dt.month
    df['payment_month_name'] = df['payment_date'].dt.strftime('%B')
    df['uk_fiscal_year']  = df['payment_date'].apply(get_fiscal_year)
    df['uk_fiscal_quarter'] = df['payment_date'].apply(get_fiscal_quarter)

    # 7. Flag refunds
    df['is_refund'] = df['amount'] < 0

    # 8. Normalize supplier names
    df['supplier_name_clean'] = df['supplier_name'].apply(normalize_supplier)

    # 9. Normalize expense type & area
    df['expense_type']  = df['expense_type'].str.strip().str.title()
    df['expense_area']  = df['expense_area'].str.strip()

    # 10. Create surrogate row key (no natural unique key exists)
    df.reset_index(drop=True, inplace=True)
    df['row_id'] = (
        filename.replace('.csv', '') + '_' + df.index.astype(str)
    )

    # 11. Keep only canonical + derived + metadata columns
    final_cols = [
        'row_id', 'department', 'entity',
        'payment_date', 'payment_year', 'payment_month',
        'payment_month_name', 'uk_fiscal_year', 'uk_fiscal_quarter',
        'expense_type', 'expense_area', 'expenditure_type',
        'supplier_name', 'supplier_name_clean',
        'supplier_postcode', 'supplier_type',
        'transaction_number', 'amount', 'is_refund',
        'description', 'contract_number', 'project_code',
        '_source_file', '_load_timestamp'
    ]
    df = df[[c for c in final_cols if c in df.columns]]

    log.info(f"    Final shape: {df.shape}")
    return df

# ── Helper Functions ───────────────────────────────────────────────────────────
def get_fiscal_year(date) -> str:
    """UK fiscal year: April–March. April 2015 → FY2015/16"""
    if pd.isnull(date):
        return None
    if date.month >= 4:
        return f"FY{date.year}/{str(date.year + 1)[-2:]}"
    else:
        return f"FY{date.year - 1}/{str(date.year)[-2:]}"

def get_fiscal_quarter(date) -> str:
    """UK fiscal quarters: Q1=Apr-Jun, Q2=Jul-Sep, Q3=Oct-Dec, Q4=Jan-Mar"""
    if pd.isnull(date):
        return None
    month = date.month
    if month in [4, 5, 6]:   return "Q1"
    if month in [7, 8, 9]:   return "Q2"
    if month in [10, 11, 12]: return "Q3"
    if month in [1, 2, 3]:   return "Q4"

def normalize_supplier(name: str) -> str:
    """Standardize supplier names for grouping."""
    if pd.isnull(name):
        return None
    name = str(name).strip().upper()
    # Remove common legal suffixes variation
    name = re.sub(r'\bLIMITED\b', 'LTD', name)
    name = re.sub(r'\bPLC\b',     'PLC', name)
    # Collapse multiple spaces
    name = re.sub(r'\s+', ' ', name)
    return name.strip()

# ── LOAD ───────────────────────────────────────────────────────────────────────
def load_to_sqlite(df: pd.DataFrame, conn: sqlite3.Connection):
    """Append cleaned data to the staging table in SQLite."""
    df.to_sql(
    'stg_transactions',
    conn,
    if_exists='append',
    index=False,
    chunksize=500
)

# ── PIPELINE ORCHESTRATOR ──────────────────────────────────────────────────────
def run_pipeline():
    log.info("=" * 60)
    log.info("BIS ETL PIPELINE STARTED")
    log.info("=" * 60)

    # Connect to SQLite warehouse
    conn = sqlite3.connect(DB_PATH)

    # Drop staging table if re-running
    conn.execute("DROP TABLE IF EXISTS stg_transactions")
    conn.commit()

    files = sorted(os.listdir(RAW_DIR))
    all_summaries = []

    for filename in files:
        if not filename.endswith('.csv'):
            continue

        log.info(f"\nProcessing: {filename}")
        filepath = os.path.join(RAW_DIR, filename)

        # Extract
        raw_df = extract(filepath, filename)

        # Transform
        clean_df = transform(raw_df, filename)

        # Load
        load_to_sqlite(clean_df, conn)
        log.info(f"  Loaded {len(clean_df)} rows to warehouse")

        # Track summary
        all_summaries.append({
            'file': filename,
            'raw_rows': len(raw_df),
            'clean_rows': len(clean_df),
            'dropped': len(raw_df) - len(clean_df),
            'refunds': int(clean_df['is_refund'].sum()),
            'null_amounts': int(clean_df['amount'].isnull().sum()),
            'total_net_spend': clean_df['amount'].sum()
        })

    conn.commit()

    # ── Post-load validation ───────────────────────────────────────────────────
    log.info("\n" + "=" * 60)
    log.info("PIPELINE COMPLETE — SUMMARY")
    log.info("=" * 60)

    summary_df = pd.DataFrame(all_summaries)
    print("\n" + summary_df.to_string(index=False))

    total = conn.execute("SELECT COUNT(*) FROM stg_transactions").fetchone()[0]
    net   = conn.execute(
        "SELECT SUM(amount) FROM stg_transactions WHERE is_refund = 0"
    ).fetchone()[0]
    refunds = conn.execute(
        "SELECT COUNT(*) FROM stg_transactions WHERE is_refund = 1"
    ).fetchone()[0]

    log.info(f"\nWarehouse row count : {total:,}")
    log.info(f"Total gross spend   : £{net:,.2f}")
    log.info(f"Total refund rows   : {refunds:,}")

    # Save summary CSV
    summary_path = os.path.join(PROCESSED_DIR, "etl_summary.csv")
    summary_df.to_csv(summary_path, index=False)
    log.info(f"\nSummary saved to: {summary_path}")

    conn.close()
    log.info("Database connection closed. Pipeline finished successfully.")

# ── Entry Point ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    run_pipeline()