-- ============================================================
-- BIS PROCUREMENT ANALYTICS — ANALYTICAL QUERY LAYER
-- Warehouse: bis_warehouse.db (SQLite)
-- Author: [Your Name]
-- Description: Business-oriented procurement queries covering
--   supplier concentration, category efficiency, trend
--   analysis, refund patterns, and entity benchmarking.
-- ============================================================


-- ============================================================
-- USE CASE 1: SUPPLIER SPEND ANALYSIS
-- Business Question: Which suppliers are receiving the most
-- public money, and are we dangerously over-reliant on any?
-- ============================================================

-- 1A. Total net spend per supplier (full year, ranked)
SELECT
    s.supplier_name_clean,
    s.supplier_type,
    COUNT(*)                          AS transaction_count,
    SUM(f.amount)                     AS gross_spend,
    SUM(CASE WHEN f.is_refund = 1
             THEN f.amount ELSE 0 END) AS total_refunds,
    SUM(f.amount) - ABS(SUM(CASE WHEN f.is_refund = 1
             THEN f.amount ELSE 0 END)) AS net_spend,
    ROUND(
        100.0 * SUM(f.amount) /
        SUM(SUM(f.amount)) OVER (),
    2)                                AS pct_of_total_spend,
    RANK() OVER (ORDER BY SUM(f.amount) DESC) AS spend_rank
FROM fact_transactions f
JOIN dim_supplier s ON f.supplier_key = s.supplier_key
WHERE f.is_refund = 0
GROUP BY s.supplier_name_clean, s.supplier_type
ORDER BY gross_spend DESC
LIMIT 25;


-- 1B. Supplier concentration risk
-- Shows what % of total spend is held by top 1, 5, 10, 25 suppliers
WITH supplier_spend AS (
    SELECT
        s.supplier_name_clean,
        SUM(f.amount) AS total_spend
    FROM fact_transactions f
    JOIN dim_supplier s ON f.supplier_key = s.supplier_key
    WHERE f.is_refund = 0
    GROUP BY s.supplier_name_clean
),
ranked AS (
    SELECT *,
        RANK() OVER (ORDER BY total_spend DESC) AS rnk,
        SUM(total_spend) OVER ()                AS grand_total
    FROM supplier_spend
)
SELECT
    rnk                                      AS supplier_rank,
    supplier_name_clean,
    ROUND(total_spend, 2)                    AS spend,
    ROUND(100.0 * total_spend / grand_total, 2) AS pct_of_total,
    ROUND(100.0 * SUM(total_spend)
          OVER (ORDER BY rnk) / grand_total, 2) AS cumulative_pct
FROM ranked
WHERE rnk <= 25
ORDER BY rnk;


-- 1C. Quarterly spend per supplier (top 10 suppliers only)
WITH top_suppliers AS (
    SELECT s.supplier_name_clean
    FROM fact_transactions f
    JOIN dim_supplier s ON f.supplier_key = s.supplier_key
    WHERE f.is_refund = 0
    GROUP BY s.supplier_name_clean
    ORDER BY SUM(f.amount) DESC
    LIMIT 10
)
SELECT
    s.supplier_name_clean,
    d.uk_fiscal_quarter,
    COUNT(*)              AS transactions,
    ROUND(SUM(f.amount), 2) AS quarterly_spend
FROM fact_transactions f
JOIN dim_supplier s ON f.supplier_key = s.supplier_key
JOIN dim_date     d ON f.date_key     = d.date_key
WHERE s.supplier_name_clean IN (SELECT supplier_name_clean FROM top_suppliers)
  AND f.is_refund = 0
GROUP BY s.supplier_name_clean, d.uk_fiscal_quarter
ORDER BY s.supplier_name_clean, d.uk_fiscal_quarter;


-- ============================================================
-- USE CASE 2: CATEGORY-LEVEL BUDGET ANALYSIS
-- Business Question: Which expense categories are consuming
-- the most budget, and do they spike in H2 (end-of-year)?
-- ============================================================

-- 2A. Total spend by expense type, ranked
SELECT
    e.expense_type,
    COUNT(*)                AS transaction_count,
    ROUND(SUM(f.amount), 2) AS total_spend,
    ROUND(AVG(f.amount), 2) AS avg_transaction,
    ROUND(MAX(f.amount), 2) AS largest_transaction,
    ROUND(
        100.0 * SUM(f.amount) /
        SUM(SUM(f.amount)) OVER (),
    2)                      AS pct_of_total
FROM fact_transactions f
JOIN dim_expense e ON f.expense_key = e.expense_key
WHERE f.is_refund = 0
  AND e.expense_type IS NOT NULL
GROUP BY e.expense_type
ORDER BY total_spend DESC
LIMIT 20;


-- 2B. H1 vs H2 spend comparison by expense type
-- H1 = April–September (start of UK fiscal year)
-- H2 = October–March   (end of UK fiscal year)
-- A large H2 spike may signal end-of-year budget burning
WITH half_year_spend AS (
    SELECT
        e.expense_type,
        d.half_year,
        SUM(f.amount) AS spend
    FROM fact_transactions f
    JOIN dim_expense e ON f.expense_key = e.expense_key
    JOIN dim_date    d ON f.date_key    = d.date_key
    WHERE f.is_refund = 0
      AND e.expense_type IS NOT NULL
    GROUP BY e.expense_type, d.half_year
),
pivoted AS (
    SELECT
        expense_type,
        SUM(CASE WHEN half_year = 'H1' THEN spend ELSE 0 END) AS h1_spend,
        SUM(CASE WHEN half_year = 'H2' THEN spend ELSE 0 END) AS h2_spend
    FROM half_year_spend
    GROUP BY expense_type
)
SELECT
    expense_type,
    ROUND(h1_spend, 2)  AS h1_spend,
    ROUND(h2_spend, 2)  AS h2_spend,
    ROUND(h1_spend + h2_spend, 2) AS annual_spend,
    ROUND(h2_spend - h1_spend, 2) AS h2_vs_h1_delta,
    CASE
        WHEN h1_spend = 0 THEN NULL
        ELSE ROUND(100.0 * (h2_spend - h1_spend) / h1_spend, 1)
    END                 AS h2_growth_pct,
    CASE
        WHEN h2_spend > h1_spend * 1.5 THEN '⚠ High H2 Spike'
        WHEN h2_spend > h1_spend * 1.2 THEN '↑ Moderate H2 Increase'
        WHEN h2_spend < h1_spend * 0.8 THEN '↓ H2 Drop'
        ELSE '~ Balanced'
    END                 AS budget_pattern
FROM pivoted
WHERE (h1_spend + h2_spend) > 0
ORDER BY annual_spend DESC
LIMIT 20;


-- 2C. Spend by entity group (cost center parent)
SELECT
    en.entity_group,
    COUNT(DISTINCT en.entity_name) AS cost_centers,
    COUNT(*)                       AS transactions,
    ROUND(SUM(f.amount), 2)        AS total_spend,
    ROUND(AVG(f.amount), 2)        AS avg_per_transaction,
    ROUND(
        100.0 * SUM(f.amount) /
        SUM(SUM(f.amount)) OVER (),
    2)                             AS pct_of_total
FROM fact_transactions f
JOIN dim_entity en ON f.entity_key = en.entity_key
WHERE f.is_refund = 0
GROUP BY en.entity_group
ORDER BY total_spend DESC;


-- ============================================================
-- USE CASE 3: MONTHLY TRENDS & RANKING
-- Business Question: How does spending shift month to month?
-- Are there sudden spikes or drops that need investigation?
-- ============================================================

-- 3A. Monthly spend trend with month-on-month change
WITH monthly AS (
    SELECT
        d.uk_fiscal_year,
        d.uk_fiscal_quarter,
        d.month_name,
        d.month,
        d.year,
        COUNT(*)              AS transactions,
        SUM(f.amount)         AS total_spend,
        SUM(CASE WHEN f.is_refund = 1
                 THEN f.amount ELSE 0 END) AS refund_amount
    FROM fact_transactions f
    JOIN dim_date d ON f.date_key = d.date_key
    WHERE f.amount IS NOT NULL
    GROUP BY d.year, d.month, d.month_name,
             d.uk_fiscal_year, d.uk_fiscal_quarter
)
SELECT
    year,
    month,
    month_name,
    uk_fiscal_quarter,
    transactions,
    ROUND(total_spend, 2)    AS total_spend,
    ROUND(refund_amount, 2)  AS refund_amount,
    ROUND(
        total_spend - LAG(total_spend) OVER (ORDER BY year, month),
    2)                       AS mom_delta,
    ROUND(
        100.0 * (total_spend - LAG(total_spend) OVER (ORDER BY year, month))
        / NULLIF(LAG(total_spend) OVER (ORDER BY year, month), 0),
    1)                       AS mom_change_pct
FROM monthly
ORDER BY year, month;


-- 3B. Monthly ranking of expense areas by spend
-- Tracks which cost centers move up or down in rank each month
WITH monthly_entity_spend AS (
    SELECT
        d.year,
        d.month,
        d.month_name,
        en.entity_name,
        en.entity_group,
        SUM(f.amount) AS monthly_spend
    FROM fact_transactions f
    JOIN dim_date   d  ON f.date_key   = d.date_key
    JOIN dim_entity en ON f.entity_key = en.entity_key
    WHERE f.is_refund = 0
    GROUP BY d.year, d.month, d.month_name, en.entity_name, en.entity_group
),
ranked AS (
    SELECT *,
        RANK() OVER (
            PARTITION BY year, month
            ORDER BY monthly_spend DESC
        ) AS monthly_rank
    FROM monthly_entity_spend
)
SELECT
    year,
    month,
    month_name,
    monthly_rank,
    entity_group,
    entity_name,
    ROUND(monthly_spend, 2) AS monthly_spend,
    monthly_rank - LAG(monthly_rank) OVER (
        PARTITION BY entity_name
        ORDER BY year, month
    )                       AS rank_movement
FROM ranked
WHERE monthly_rank <= 10
ORDER BY year, month, monthly_rank;


-- ============================================================
-- USE CASE 4: REFUND & ANOMALY ANALYSIS
-- Business Question: Which suppliers and categories have the
-- highest refund rates? Do they indicate process failures?
-- ============================================================

-- 4A. Supplier refund rate analysis
WITH supplier_totals AS (
    SELECT
        s.supplier_name_clean,
        SUM(CASE WHEN f.is_refund = 0
                 THEN f.amount ELSE 0 END)       AS gross_spend,
        SUM(CASE WHEN f.is_refund = 1
                 THEN ABS(f.amount) ELSE 0 END)  AS total_refunded,
        COUNT(CASE WHEN f.is_refund = 0