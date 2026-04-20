import sqlite3
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s  %(levelname)s  %(message)s')
log = logging.getLogger(__name__)

DB_PATH = "warehouse/bis_warehouse.db"

VIEWS = {

"vw_monthly_spend_trend": """
CREATE VIEW IF NOT EXISTS vw_monthly_spend_trend AS
SELECT
    d.full_date,
    d.year,
    d.month,
    d.month_name,
    d.uk_fiscal_year,
    d.uk_fiscal_quarter,
    d.half_year,
    COUNT(*)                                                    AS transaction_count,
    SUM(CASE WHEN f.is_refund=0 THEN f.amount ELSE 0 END)      AS gross_spend,
    ABS(SUM(CASE WHEN f.is_refund=1 THEN f.amount ELSE 0 END)) AS total_refunds,
    SUM(f.amount)                                               AS net_spend,
    SUM(f.is_refund)                                            AS refund_count
FROM fact_transactions f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.full_date, d.year, d.month, d.month_name,
         d.uk_fiscal_year, d.uk_fiscal_quarter, d.half_year
""",

"vw_supplier_summary": """
CREATE VIEW IF NOT EXISTS vw_supplier_summary AS
WITH totals AS (
    SELECT SUM(CASE WHEN is_refund=0 THEN amount ELSE 0 END) AS grand_total
    FROM fact_transactions
)
SELECT
    s.supplier_key,
    s.supplier_name_clean,
    s.supplier_type,
    COUNT(CASE WHEN f.is_refund=0 THEN 1 END)              AS payment_count,
    COUNT(CASE WHEN f.is_refund=1 THEN 1 END)              AS refund_count,
    SUM(CASE WHEN f.is_refund=0 THEN f.amount ELSE 0 END)  AS gross_spend,
    ABS(SUM(CASE WHEN f.is_refund=1 THEN f.amount ELSE 0 END)) AS total_refunded,
    SUM(f.amount)                                           AS net_spend,
    ROUND(100.0 * SUM(CASE WHEN f.is_refund=0 THEN f.amount ELSE 0 END)
          / t.grand_total, 4)                               AS pct_of_total,
    RANK() OVER (ORDER BY
        SUM(CASE WHEN f.is_refund=0 THEN f.amount ELSE 0 END) DESC) AS spend_rank
FROM fact_transactions f
JOIN dim_supplier s ON f.supplier_key = s.supplier_key
CROSS JOIN totals t
GROUP BY s.supplier_key, s.supplier_name_clean, s.supplier_type, t.grand_total
""",

"vw_expense_summary": """
CREATE VIEW IF NOT EXISTS vw_expense_summary AS
SELECT
    e.expense_key,
    e.expense_type,
    e.expense_area,
    d.uk_fiscal_year,
    d.uk_fiscal_quarter,
    d.half_year,
    d.month,
    d.month_name,
    d.year,
    COUNT(CASE WHEN f.is_refund=0 THEN 1 END)              AS transaction_count,
    SUM(CASE WHEN f.is_refund=0 THEN f.amount ELSE 0 END)  AS gross_spend,
    ABS(SUM(CASE WHEN f.is_refund=1 THEN f.amount ELSE 0 END)) AS refunds,
    SUM(f.amount)                                           AS net_spend
FROM fact_transactions f
JOIN dim_expense e ON f.expense_key = e.expense_key
JOIN dim_date    d ON f.date_key    = d.date_key
GROUP BY e.expense_key, e.expense_type, e.expense_area,
         d.uk_fiscal_year, d.uk_fiscal_quarter,
         d.half_year, d.month, d.month_name, d.year
""",

"vw_entity_monthly": """
CREATE VIEW IF NOT EXISTS vw_entity_monthly AS
SELECT
    en.entity_key,
    en.entity_name,
    en.entity_group,
    en.entity_subgroup,
    d.year,
    d.month,
    d.month_name,
    d.uk_fiscal_year,
    d.uk_fiscal_quarter,
    d.half_year,
    COUNT(CASE WHEN f.is_refund=0 THEN 1 END)             AS transaction_count,
    SUM(CASE WHEN f.is_refund=0 THEN f.amount ELSE 0 END) AS gross_spend,
    SUM(f.amount)                                         AS net_spend,
    RANK() OVER (
        PARTITION BY d.year, d.month
        ORDER BY SUM(CASE WHEN f.is_refund=0
                          THEN f.amount ELSE 0 END) DESC
    )                                                     AS monthly_rank
FROM fact_transactions f
JOIN dim_entity en ON f.entity_key = en.entity_key
JOIN dim_date    d ON f.date_key   = d.date_key
GROUP BY en.entity_key, en.entity_name, en.entity_group,
         en.entity_subgroup, d.year, d.month, d.month_name,
         d.uk_fiscal_year, d.uk_fiscal_quarter, d.half_year
""",

"vw_refund_analysis": """
CREATE VIEW IF NOT EXISTS vw_refund_analysis AS
SELECT
    s.supplier_name_clean,
    s.supplier_type,
    e.expense_type,
    e.expense_area,
    d.month_name,
    d.month,
    d.year,
    d.uk_fiscal_quarter,
    f.amount,
    f.is_refund,
    f.description,
    f.transaction_number
FROM fact_transactions f
JOIN dim_supplier s ON f.supplier_key = s.supplier_key
JOIN dim_expense  e ON f.expense_key  = e.expense_key
JOIN dim_date     d ON f.date_key     = d.date_key
WHERE f.is_refund = 1
""",

"vw_executive_summary": """
CREATE VIEW IF NOT EXISTS vw_executive_summary AS
SELECT
    d.uk_fiscal_year,
    d.uk_fiscal_quarter,
    d.half_year,
    COUNT(*)                                                     AS total_transactions,
    COUNT(DISTINCT s.supplier_name_clean)                        AS unique_suppliers,
    COUNT(DISTINCT en.entity_name)                               AS active_cost_centers,
    SUM(CASE WHEN f.is_refund=0 THEN f.amount ELSE 0 END)       AS gross_spend,
    ABS(SUM(CASE WHEN f.is_refund=1 THEN f.amount ELSE 0 END))  AS total_refunds,
    SUM(f.amount)                                                AS net_spend,
    SUM(f.is_refund)                                             AS refund_count,
    ROUND(100.0 * SUM(f.is_refund) / COUNT(*), 2)               AS refund_rate_pct,
    AVG(CASE WHEN f.is_refund=0 THEN f.amount END)              AS avg_transaction_size
FROM fact_transactions f
JOIN dim_date     d  ON f.date_key     = d.date_key
JOIN dim_supplier s  ON f.supplier_key = s.supplier_key
JOIN dim_entity   en ON f.entity_key   = en.entity_key
GROUP BY d.uk_fiscal_year, d.uk_fiscal_quarter, d.half_year
"""
}

def create_views():
    conn = sqlite3.connect(DB_PATH)
    log.info("Creating reporting views...")

    for view_name, ddl in VIEWS.items():
        conn.execute(f"DROP VIEW IF EXISTS {view_name}")
        conn.execute(ddl)
        count = conn.execute(
            f"SELECT COUNT(*) FROM {view_name}"
        ).fetchone()[0]
        log.info(f"  ✓ {view_name} — {count:,} rows")

    conn.commit()
    conn.close()
    log.info("All views created successfully.")

if __name__ == "__main__":
    create_views()