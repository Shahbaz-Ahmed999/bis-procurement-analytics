import sqlite3
import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 120)
pd.set_option('display.float_format', lambda x: f'{x:,.2f}')

DB_PATH = "warehouse/bis_warehouse.db"
SQL_PATH = "sql/analytical_queries.sql"

conn = sqlite3.connect(DB_PATH)

# Split the SQL file into individual queries by the USE CASE headers
with open(SQL_PATH, 'r') as f:
    content = f.read()

# Extract individual SELECT statements
import re
queries = re.findall(
    r'--\s+(USE CASE \d+[A-Z]?.*?)\n(.*?)(?=\n-- (?:USE CASE|\={10})|$)',
    content, re.DOTALL
)

labels_and_sql = []
for match in re.finditer(
    r'-- ((?:USE CASE|EXECUTIVE).*?)\n-- .*?\n(SELECT.*?)(?=\n\n-- (?:USE CASE|\={10}|EXECUTIVE)|$)',
    content, re.DOTALL
):
    labels_and_sql.append((match.group(1).strip(), match.group(2).strip()))

# Simpler approach: run each numbered query block
query_blocks = {
    "1A — Top 25 Suppliers by Spend": """
        SELECT s.supplier_name_clean, s.supplier_type,
               COUNT(*) AS transaction_count,
               ROUND(SUM(f.amount),2) AS gross_spend,
               ROUND(100.0*SUM(f.amount)/SUM(SUM(f.amount)) OVER(),2) AS pct_of_total,
               RANK() OVER (ORDER BY SUM(f.amount) DESC) AS spend_rank
        FROM fact_transactions f
        JOIN dim_supplier s ON f.supplier_key = s.supplier_key
        WHERE f.is_refund = 0
        GROUP BY s.supplier_name_clean, s.supplier_type
        ORDER BY gross_spend DESC LIMIT 15
    """,
    "1B — Supplier Concentration (Cumulative %)": """
        WITH supplier_spend AS (
            SELECT s.supplier_name_clean, SUM(f.amount) AS total_spend
            FROM fact_transactions f
            JOIN dim_supplier s ON f.supplier_key = s.supplier_key
            WHERE f.is_refund = 0
            GROUP BY s.supplier_name_clean
        ), ranked AS (
            SELECT *, RANK() OVER (ORDER BY total_spend DESC) AS rnk,
                   SUM(total_spend) OVER () AS grand_total
            FROM supplier_spend
        )
        SELECT rnk, supplier_name_clean,
               ROUND(total_spend,2) AS spend,
               ROUND(100.0*total_spend/grand_total,2) AS pct_of_total,
               ROUND(100.0*SUM(total_spend) OVER (ORDER BY rnk)/grand_total,2) AS cumulative_pct
        FROM ranked WHERE rnk <= 10 ORDER BY rnk
    """,
    "2A — Top 20 Expense Types by Spend": """
        SELECT e.expense_type, COUNT(*) AS transaction_count,
               ROUND(SUM(f.amount),2) AS total_spend,
               ROUND(AVG(f.amount),2) AS avg_transaction,
               ROUND(100.0*SUM(f.amount)/SUM(SUM(f.amount)) OVER(),2) AS pct_of_total
        FROM fact_transactions f
        JOIN dim_expense e ON f.expense_key = e.expense_key
        WHERE f.is_refund = 0 AND e.expense_type IS NOT NULL
        GROUP BY e.expense_type
        ORDER BY total_spend DESC LIMIT 15
    """,
    "2B — H1 vs H2 Budget Pattern": """
        WITH hvh AS (
            SELECT e.expense_type,
                   SUM(CASE WHEN d.half_year='H1' THEN f.amount ELSE 0 END) AS h1,
                   SUM(CASE WHEN d.half_year='H2' THEN f.amount ELSE 0 END) AS h2
            FROM fact_transactions f
            JOIN dim_expense e ON f.expense_key = e.expense_key
            JOIN dim_date d ON f.date_key = d.date_key
            WHERE f.is_refund=0 AND e.expense_type IS NOT NULL
            GROUP BY e.expense_type
        )
        SELECT expense_type,
               ROUND(h1,2) AS h1_spend, ROUND(h2,2) AS h2_spend,
               ROUND(h2-h1,2) AS delta,
               CASE WHEN h2>h1*1.5 THEN 'H2 SPIKE'
                    WHEN h2>h1*1.2 THEN 'H2 High'
                    WHEN h2<h1*0.8 THEN 'H2 Low'
                    ELSE 'Balanced' END AS pattern
        FROM hvh WHERE (h1+h2)>0
        ORDER BY (h1+h2) DESC LIMIT 15
    """,
    "3A — Monthly Spend Trend with MoM Change": """
        WITH monthly AS (
            SELECT d.year, d.month, d.month_name, d.uk_fiscal_quarter,
                   COUNT(*) AS transactions, SUM(f.amount) AS total_spend
            FROM fact_transactions f
            JOIN dim_date d ON f.date_key = d.date_key
            WHERE f.amount IS NOT NULL
            GROUP BY d.year, d.month, d.month_name, d.uk_fiscal_quarter
        )
        SELECT year, month, month_name, uk_fiscal_quarter, transactions,
               ROUND(total_spend,2) AS total_spend,
               ROUND(total_spend - LAG(total_spend) OVER (ORDER BY year,month),2) AS mom_delta,
               ROUND(100.0*(total_spend-LAG(total_spend) OVER (ORDER BY year,month))
                     /NULLIF(LAG(total_spend) OVER (ORDER BY year,month),0),1) AS mom_pct
        FROM monthly ORDER BY year, month
    """,
    "4A — Supplier Refund Risk": """
        WITH t AS (
            SELECT s.supplier_name_clean,
                   SUM(CASE WHEN f.is_refund=0 THEN f.amount ELSE 0 END) AS gross,
                   SUM(CASE WHEN f.is_refund=1 THEN ABS(f.amount) ELSE 0 END) AS refunded,
                   COUNT(CASE WHEN f.is_refund=1 THEN 1 END) AS refund_count
            FROM fact_transactions f
            JOIN dim_supplier s ON f.supplier_key = s.supplier_key
            GROUP BY s.supplier_name_clean
        )
        SELECT supplier_name_clean,
               ROUND(gross,2) AS gross_spend,
               ROUND(refunded,2) AS total_refunded,
               refund_count,
               ROUND(100.0*refunded/NULLIF(gross,0),2) AS refund_rate_pct,
               CASE WHEN 100.0*refunded/NULLIF(gross,0)>10 THEN 'HIGH RISK'
                    WHEN 100.0*refunded/NULLIF(gross,0)>3  THEN 'MODERATE'
                    ELSE 'NORMAL' END AS risk_flag
        FROM t WHERE gross>0 AND refund_count>0
        ORDER BY refund_rate_pct DESC LIMIT 15
    """,
    "4B — Monthly Anomaly Detection": """
        WITH m AS (
            SELECT d.year, d.month, d.month_name, SUM(f.amount) AS spend
            FROM fact_transactions f JOIN dim_date d ON f.date_key=d.date_key
            WHERE f.is_refund=0 GROUP BY d.year, d.month, d.month_name
        ), s AS (SELECT AVG(spend) AS avg_s, 1.5*AVG(spend) AS threshold FROM m)
        SELECT m.year, m.month, m.month_name,
               ROUND(m.spend,2) AS monthly_spend,
               ROUND(s.avg_s,2) AS avg_spend,
               ROUND(100.0*(m.spend-s.avg_s)/s.avg_s,1) AS pct_vs_avg,
               CASE WHEN m.spend>s.threshold THEN 'SPIKE'
                    WHEN m.spend<s.avg_s*0.6 THEN 'LOW'
                    ELSE 'Normal' END AS flag
        FROM m CROSS JOIN s ORDER BY m.year, m.month
    """,
    "5 — Executive Summary by Fiscal Quarter": """
        SELECT d.uk_fiscal_year, d.uk_fiscal_quarter,
               COUNT(*) AS total_transactions,
               COUNT(DISTINCT s.supplier_name_clean) AS unique_suppliers,
               ROUND(SUM(CASE WHEN f.is_refund=0 THEN f.amount ELSE 0 END),2) AS gross_spend,
               ROUND(ABS(SUM(CASE WHEN f.is_refund=1 THEN f.amount ELSE 0 END)),2) AS refunds,
               ROUND(SUM(f.amount),2) AS net_spend,
               SUM(f.is_refund) AS refund_count,
               ROUND(100.0*SUM(f.is_refund)/COUNT(*),2) AS refund_rate_pct
        FROM fact_transactions f
        JOIN dim_date d ON f.date_key=d.date_key
        JOIN dim_supplier s ON f.supplier_key=s.supplier_key
        GROUP BY d.uk_fiscal_year, d.uk_fiscal_quarter
        ORDER BY d.uk_fiscal_year, d.uk_fiscal_quarter
    """
}

# Run all queries and print results
print("\n" + "=" * 80)
print("BIS PROCUREMENT — ANALYTICAL QUERY RESULTS")
print("=" * 80)

for title, query in query_blocks.items():
    print(f"\n{'─' * 80}")
    print(f"  QUERY {title}")
    print(f"{'─' * 80}")
    try:
        df = pd.read_sql(query.strip(), conn)
        print(df.to_string(index=False))
    except Exception as e:
        print(f"  ERROR: {e}")

conn.close()
print("\n" + "=" * 80)
print("All queries complete.")
print("=" * 80)